import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const CHILD_ACK_TIMEOUT_MS = 10_000;

interface MeshLockLease {
  token: string;
  helper: import('child_process').ChildProcess;
  helperPid?: number;
  channelPath: string;
  childAcks: Map<number, { resolve: (birthId?: string) => void; reject: (error: Error) => void; timer: NodeJS.Timeout; timeout: NodeJS.Timeout }>;
  childBirthIds: Map<number, string>;
  detachedChildren: Set<number>;
  releasePromise?: Promise<boolean>;
}

export function buildPosixLockHelperCommand(lockPath: string, channelPath: string): { command: string; args: string[] } {
  const script = `use Fcntl qw(:flock);
use Time::HiRes qw(usleep);
open my $f, ">>", $ARGV[0] or die $!;
flock($f, LOCK_EX) or die $!;
$channel = $ARGV[1];
$|=1;
open my $ready, ">", "$channel.ready" or die $!;
print $ready "ready";
close $ready;
print "LOCKED\\n";
my @children;
my $release=0;
while(!$release){
  my $line=<STDIN>;
  if(!defined $line){$release=1;last}
  if($line=~/^RELEASE/){$release=1}
  elsif($line=~/^CHILD (\\d+)/){push @children,$1;open my $ack, ">", "$channel.child.$1" or die $!;print $ack "tracked";close $ack;print "CHILD-TRACKED $1\\n"}
  elsif($line=~/^EXIT (\\d+)/){my $pid=$1;@children=grep{$_!=$pid}@children}
}
while(@children){@children=grep{kill(0,$_)}@children;usleep(100000)}
unlink "$channel.ready";
unlink glob "$channel.child.*"`;
  return { command: 'perl', args: ['-e', script, lockPath, channelPath] };
}

/**
 * The mesh-wide merge lock path for a given bridge home.
 *
 * The real home (`~/.agent-mesh-bridge`) owns the one shared lock that
 * serializes merges across every process. Any other home — a test fixture, a
 * task's isolated storage — must use its own lock inside that directory:
 * otherwise every isolated fixture in a concurrently executed test file
 * contends for the single global lock and blocks for the full held duration.
 */
export function meshMergeLockPath(storageDir: string): string {
  const meshHome = path.join(os.homedir(), '.agent-mesh-bridge');
  let resolved: string | undefined;
  try {
    resolved = path.resolve(storageDir);
    if (resolved === path.resolve(meshHome)) {
      return path.join(os.homedir(), '.agent-mesh-bridge', 'merge.lock');
    }
  } catch {}
  return path.join(resolved || storageDir, 'merge.lock');
}

export class MeshFileLock {
  readonly lockPath: string;
  private activeLease?: MeshLockLease;
  constructor(lockPath = path.join(os.homedir(), '.agent-mesh-bridge', 'merge.lock')) {
    this.lockPath = lockPath;
  }

  get isHeld(): boolean {
    return Boolean(this.activeLease);
  }

  dispose(): void {
    if (this.activeLease) {
      const lease = this.activeLease;
      lease.releasePromise = this.waitForLeaseRelease(lease);
      this.signalRelease(lease);
      this.activeLease = undefined;
    }
  }

  /** Release the specific acquired lease and wait for its OS helper to exit. */
  public async releaseAndConfirm(
    release: (() => void) | undefined,
    timeoutMs: number = CHILD_ACK_TIMEOUT_MS,
  ): Promise<boolean> {
    if (!release) return true;
    const lease = this.activeLease;
    try { release(); } catch { return false; }
    if (!lease) return false;
    const pending = lease.releasePromise;
    if (!pending) return false;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), timeoutMs);
      timer.unref?.();
    });
    const confirmed = await Promise.race([pending, timeout]);
    if (timer) clearTimeout(timer);
    if (!confirmed) return false;
    try {
      const owner = JSON.parse(fs.readFileSync(`${this.lockPath}.owner.json`, 'utf8'));
      return owner?.token !== lease.token;
    } catch (error: any) {
      return error?.code === 'ENOENT';
    }
  }

  async acquire(onWait?: () => void, cancelled?: () => boolean, timeoutMs?: number): Promise<(() => void) | undefined> {
    if (cancelled?.()) return undefined;
    const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined;
    fs.mkdirSync(path.dirname(this.lockPath), { recursive: true });
    const token = crypto.randomUUID();
    const channelPath = `${this.lockPath}.${token}`;
    const child = this.startLockHelper(token, channelPath);
    const childAcks = new Map<number, { resolve: (birthId?: string) => void; reject: (error: Error) => void; timer: NodeJS.Timeout; timeout: NodeJS.Timeout }>();
    const childBirthIds = new Map<number, string>();
    const detachedChildren = new Set<number>();
    let output = '';
    let stdoutUtf8 = '';
    let stdoutUtf16 = '';
    let acquired = false;
    let readyPoll: NodeJS.Timeout;
    let readyTimeout: NodeJS.Timeout;
    let settleReady!: (error?: Error) => void;
    const ready = new Promise<void>((resolve, reject) => {
      let settled = false;
      settleReady = (error) => {
        if (settled) return;
        settled = true;
        clearInterval(readyPoll);
        clearTimeout(readyTimeout);
        if (error) reject(error); else resolve();
      };
      const checkReady = () => {
        if (process.platform === 'win32' && !acquired) {
          const errorPath = fs.existsSync(`${channelPath}.launch-error`) ? `${channelPath}.launch-error` :
            fs.existsSync(`${channelPath}.error`) ? `${channelPath}.error` : undefined;
          if (errorPath) {
            try { output += fs.readFileSync(errorPath, 'utf8'); fs.unlinkSync(errorPath); } catch {}
            settleReady(new Error(`Mesh lock helper failed before acquiring the OS lock: ${output}`));
            return;
          }
          if (fs.existsSync(`${channelPath}.done`)) {
            try { fs.unlinkSync(`${channelPath}.done`); } catch {}
            settleReady(new Error(`Mesh lock helper exited before acquiring the OS lock: ${output}`));
            return;
          }
          const helperPidPath = `${channelPath}.helper-pid`;
          const helperPid = Number(fs.existsSync(helperPidPath) ? fs.readFileSync(helperPidPath, 'utf8') : 0);
          if (helperPid) clearTimeout(readyTimeout);
          if (helperPid && !this.isPidAlive(helperPid)) {
            settleReady(new Error(`Mesh lock helper process ${helperPid} exited before acquiring the OS lock.`));
            return;
          }
        }
        const helperReady = fs.existsSync(`${channelPath}.ready`) || stdoutUtf8.includes('LOCKED') || stdoutUtf16.includes('LOCKED');
        if (acquired || !helperReady || (process.platform === 'win32' && !fs.existsSync(`${channelPath}.helper-pid`))) return;
        acquired = true;
        try { fs.unlinkSync(`${channelPath}.ready`); } catch {}
        settleReady();
      };
      child.stderr?.on('data', (chunk: Buffer) => { output += chunk.toString(); });
      child.stdout?.on('data', (chunk: Buffer) => {
        stdoutUtf8 += chunk.toString();
        stdoutUtf16 += chunk.toString('utf16le');
        output += chunk.toString();
        checkReady();
      });
      child.once('error', (error) => settleReady(error));
      child.once('close', (code) => {
        if (process.platform === 'win32') {
          if (acquired) return;
          if (fs.existsSync(`${channelPath}.helper-pid`)) return;
          try {
            const errorPath = `${channelPath}.launch-error`;
            if (fs.existsSync(errorPath)) output += fs.readFileSync(errorPath, 'utf8');
          } catch {}
          settleReady(new Error(`Mesh lock helper launcher exited before starting its worker (${code}): ${output}`));
          return;
        }
        try {
          const errorPath = `${channelPath}.error`;
          if (fs.existsSync(errorPath)) { output += fs.readFileSync(errorPath, 'utf8'); fs.unlinkSync(errorPath); }
        } catch {}
        const error = new Error(`Mesh lock helper exited before acquiring or tracking the OS lock (${code}): ${output}`);
        if (!acquired) settleReady(error);
        for (const [pid, waiter] of childAcks) {
          clearInterval(waiter.timer);
          clearTimeout(waiter.timeout);
          if (fs.existsSync(`${channelPath}.child.${pid}`)) waiter.resolve();
          else waiter.reject(error);
        }
        childAcks.clear();
      });
      readyPoll = setInterval(checkReady, 25);
      if (process.platform === 'win32') {
        readyTimeout = setTimeout(() => {
          if (!fs.existsSync(`${channelPath}.helper-pid`)) settleReady(new Error('Timed out starting the mesh lock helper.'));
        }, 15_000);
      }
    });
    void ready.catch(() => undefined);
    while (!acquired) {
      if (cancelled?.()) {
        settleReady(new Error('Mesh lock acquisition cancelled.'));
        await this.stopUnleasedHelper(child, channelPath);
        return undefined;
      }
      if (deadline !== undefined && Date.now() >= deadline) {
        settleReady(new Error('Mesh lock acquisition timed out.'));
        await this.stopUnleasedHelper(child, channelPath);
        return undefined;
      }
      let got: boolean;
      try { got = await Promise.race([ready.then(() => true), sleep(100).then(() => false)]); }
      catch (error) { await this.stopUnleasedHelper(child, channelPath); throw error; }
      if (got) break;
      onWait?.();
    }
    if (cancelled?.()) {
      settleReady(new Error('Mesh lock acquisition cancelled.'));
      await this.stopUnleasedHelper(child, channelPath);
      return undefined;
    }
    const ownerFile = `${this.lockPath}.owner.json`;
    const temp = `${ownerFile}.${process.pid}.${token}.tmp`;
    const helperPid = process.platform === 'win32' ? Number(fs.readFileSync(`${channelPath}.helper-pid`, 'utf8')) : child.pid;
    try {
      fs.writeFileSync(temp, JSON.stringify({ token, pid: process.pid, helperPid, acquiredAt: Date.now() }));
      fs.renameSync(temp, ownerFile);
    } catch (error) {
      try { fs.unlinkSync(temp); } catch {}
      if (process.platform === 'win32') {
        await this.stopUnleasedHelper(child, channelPath);
      } else {
        child.kill();
      }
      throw error;
    }
    const lease: MeshLockLease = { token, helper: child, helperPid, channelPath, childAcks, childBirthIds, detachedChildren };
    this.activeLease = lease;
    return () => {
      if (this.activeLease !== lease) return;
      try {
        const owner = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
        if (owner.token === token) fs.unlinkSync(ownerFile);
      } catch {}
      this.activeLease = undefined;
      lease.releasePromise = this.waitForLeaseRelease(lease);
      this.signalRelease(lease);
    };
  }

  attachChildPid(pid: number | undefined, cancelled?: () => boolean, expectedBirthId?: string): Promise<string | void> {
    const lease = this.activeLease;
    if (!pid || !lease) return Promise.resolve();
    if (cancelled?.()) return Promise.reject(new Error('Child registration cancelled because the merge task is no longer active.'));
    const registeredBirthId = lease.childBirthIds.get(pid);
    if (expectedBirthId && registeredBirthId === expectedBirthId && this.isPidAlive(lease.helperPid) &&
        !fs.existsSync(`${lease.channelPath}.error`) && !fs.existsSync(`${lease.channelPath}.done`)) {
      return Promise.resolve(registeredBirthId);
    }
    lease.detachedChildren.delete(pid);
    return new Promise<string | void>((resolve, reject) => {
      const ackPath = `${lease.channelPath}.child.${pid}`;
      const finish = (error?: Error, birthId?: string) => {
        const pending = lease.childAcks.get(pid);
        if (!pending) return;
        lease.childAcks.delete(pid);
        clearInterval(pending.timer);
        clearTimeout(pending.timeout);
        if (error) pending.reject(error);
        else {
          if (birthId && !lease.detachedChildren.has(pid)) lease.childBirthIds.set(pid, birthId);
          pending.resolve(birthId);
        }
      };
      const timer = setInterval(() => {
        if (fs.existsSync(ackPath)) {
          try {
            const ack = process.platform === 'win32' ? fs.readFileSync(ackPath, 'utf8') : 'missing';
            fs.unlinkSync(ackPath);
            const birthId = /^tracked:([0-9]+)$/.exec(ack)?.[1];
            if (!birthId && ack !== 'missing') throw new Error('Invalid mesh lock child acknowledgement.');
            finish(undefined, birthId);
          } catch (error) { finish(error as Error); }
          return;
        }
        const errorPath = `${lease.channelPath}.error`;
        if (fs.existsSync(errorPath)) {
          let message = 'Mesh lock helper failed while registering a child.';
          try { message += ` ${fs.readFileSync(errorPath, 'utf8')}`; fs.unlinkSync(errorPath); } catch {}
          finish(new Error(message));
          return;
        }
        if (fs.existsSync(`${lease.channelPath}.done`)) {
          try { fs.unlinkSync(`${lease.channelPath}.done`); } catch {}
          finish(new Error('Mesh lock helper exited before acknowledging the child.'));
          return;
        }
        if (cancelled?.()) {
          finish(new Error('Child registration cancelled because the merge task is no longer active.'));
          return;
        }
        const helperAlive = process.platform === 'win32'
          ? this.isPidAlive(lease.helperPid)
          : lease.helper.exitCode === null && lease.helper.signalCode === null;
        if (!helperAlive) finish(new Error('Mesh lock helper exited before acknowledging the child.'));
      }, 25);
      // Keep the caller alive until the helper acknowledges the tracked child.
      const timeout = setTimeout(
        () => finish(new Error(`Timed out waiting ${CHILD_ACK_TIMEOUT_MS}ms for the mesh lock helper to acknowledge child ${pid}.`)),
        CHILD_ACK_TIMEOUT_MS,
      );
      lease.childAcks.set(pid, { resolve, reject, timer, timeout });
      if (process.platform === 'win32') {
        try {
          const commandPath = `${lease.channelPath}.command.child.${pid}`;
          const tempPath = `${commandPath}.${crypto.randomUUID()}.tmp`;
          fs.writeFileSync(tempPath, 'CHILD');
          fs.renameSync(tempPath, commandPath);
        } catch (error) {
          finish(error as Error);
        }
      } else {
        try {
          lease.helper.stdin!.write(`CHILD ${pid}\n`, (error) => {
            if (error) finish(error);
          });
        } catch (error) {
          finish(error as Error);
        }
      }
    });
  }

  detachChildPid(pid: number | undefined): void {
    const lease = this.activeLease;
    if (!pid || !lease) return;
    lease.detachedChildren.add(pid);
    lease.childBirthIds.delete(pid);
    if (process.platform === 'win32') {
      try { fs.writeFileSync(`${lease.channelPath}.command.exit.${pid}`, 'EXIT'); } catch {}
    } else {
      try { lease.helper.stdin!.write(`EXIT ${pid}\n`); } catch {}
    }
  }

  private signalRelease(lease: MeshLockLease): void {
    if (process.platform === 'win32') {
      try { fs.writeFileSync(`${lease.channelPath}.release`, 'release'); } catch {}
    } else {
      try { lease.helper.stdin!.write('RELEASE\n'); lease.helper.stdin!.end(); } catch {}
    }
  }

  private waitForLeaseRelease(lease: MeshLockLease): Promise<boolean> {
    if (process.platform !== 'win32') {
      if (lease.helper.exitCode !== null || lease.helper.signalCode !== null) return Promise.resolve(true);
      return new Promise<boolean>((resolve) => {
        let settled = false;
        const finish = (confirmed: boolean) => {
          if (settled) return;
          settled = true;
          clearTimeout(timeout);
          lease.helper.removeListener('close', onClose);
          lease.helper.removeListener('error', onError);
          resolve(confirmed);
        };
        const onClose = () => finish(true);
        const onError = () => finish(false);
        const timeout = setTimeout(() => finish(false), CHILD_ACK_TIMEOUT_MS);
        timeout.unref?.();
        lease.helper.once('close', onClose);
        lease.helper.once('error', onError);
        if (lease.helper.exitCode !== null || lease.helper.signalCode !== null) finish(true);
      });
    }

    return new Promise<boolean>((resolve) => {
      let settled = false;
      const releasedPath = `${lease.channelPath}.released`;
      const finish = (confirmed: boolean) => {
        if (settled) return;
        settled = true;
        clearInterval(poll);
        clearTimeout(timeout);
        if (confirmed) try { fs.unlinkSync(releasedPath); } catch {}
        resolve(confirmed);
      };
      const check = () => {
        if (fs.existsSync(releasedPath)) finish(true);
      };
      const poll = setInterval(check, 25);
      poll.unref?.();
      const timeout = setTimeout(() => finish(false), CHILD_ACK_TIMEOUT_MS);
      timeout.unref?.();
      check();
    });
  }

  private startLockHelper(token: string, channelPath: string): import('child_process').ChildProcess {
    const mutexName = `AgentMeshMerge_${crypto.createHash('sha256').update(path.resolve(this.lockPath).toLowerCase()).digest('hex').slice(0, 32)}`;
    if (process.platform === 'win32') {
      const helperPath = `${this.lockPath}.${token}.mutex-helper.ps1`;
      const script = `$ErrorActionPreference = 'Stop'
$mutex = [System.Threading.Mutex]::new($false, $args[0])
$channel = $args[1]
$helperPath = $args[2]
$ownerPid = [int]$args[3]
$held = $false
$ownerStart = (Get-Process -Id $ownerPid -ErrorAction Stop).StartTime.ToUniversalTime().Ticks
$children = [System.Collections.Generic.Dictionary[int,long]]::new()
$childPrefix = [System.IO.Path]::GetFileName($channel + '.command.child.')
$exitPrefix = [System.IO.Path]::GetFileName($channel + '.command.exit.')
function Test-OwnerAlive {
  try { $owner = Get-Process -Id $ownerPid -ErrorAction Stop; return $owner.StartTime.ToUniversalTime().Ticks -eq $ownerStart } catch { return $false }
}
try {
  while (-not $held -and (Test-OwnerAlive) -and -not (Test-Path -LiteralPath ($channel + '.cancel'))) {
    try { $held = $mutex.WaitOne(100) } catch [System.Threading.AbandonedMutexException] { $held = $true }
  }
  if (-not $held -or (Test-Path -LiteralPath ($channel + '.cancel'))) { return }
  [System.IO.File]::WriteAllText($channel + '.ready', 'ready')
  $release = $false
  while (-not $release) {
    foreach ($command in (Get-ChildItem -Path ($channel + '.command.child.*') -ErrorAction SilentlyContinue)) {
      $pidValue = 0
      $pidText = $command.Name.Substring($childPrefix.Length)
      # Ignore temporary files until the writer atomically publishes the PID-only name.
      if (-not [int]::TryParse($pidText, [ref]$pidValue)) { continue }
      $ack = 'missing'
      try {
        $startedAt = (Get-Process -Id $pidValue -ErrorAction Stop).StartTime
        $children[$pidValue] = $startedAt.ToUniversalTime().Ticks
        $ack = 'tracked:' + $startedAt.ToFileTimeUtc().ToString([System.Globalization.CultureInfo]::InvariantCulture)
      } catch { }
      $ackPath = $channel + '.child.' + $pidValue
      $temporaryAck = $ackPath + '.tmp'
      [System.IO.File]::WriteAllText($temporaryAck, $ack)
      [System.IO.File]::Move($temporaryAck, $ackPath)
      Remove-Item -LiteralPath $command.FullName -Force -ErrorAction SilentlyContinue
    }
    foreach ($command in (Get-ChildItem -Path ($channel + '.command.exit.*') -ErrorAction SilentlyContinue)) {
      $pidValue = 0
      $pidText = $command.Name.Substring($exitPrefix.Length)
      if (-not [int]::TryParse($pidText, [ref]$pidValue)) { continue }
      $children.Remove($pidValue)
      Remove-Item -LiteralPath $command.FullName -Force -ErrorAction SilentlyContinue
    }
    $release = (Test-Path -LiteralPath ($channel + '.release')) -or (Test-Path -LiteralPath ($channel + '.cancel')) -or -not (Test-OwnerAlive)
    if (-not $release) { Start-Sleep -Milliseconds 25 }
  }
  while ($children.Count -gt 0) {
    foreach ($pidValue in @($children.Keys)) { try { $process = Get-Process -Id $pidValue -ErrorAction Stop; if ($process.StartTime.ToUniversalTime().Ticks -ne $children[$pidValue]) { $children.Remove($pidValue) } } catch { $children.Remove($pidValue) } }
    if ($children.Count -gt 0) { Start-Sleep -Milliseconds 100 }
  }
} catch {
  [System.IO.File]::WriteAllText($channel + '.error', ($_ | Out-String))
} finally {
  if ($held) { $mutex.ReleaseMutex() }
  $mutex.Dispose()
  Get-ChildItem -Path ($channel + '.*') -ErrorAction SilentlyContinue | Where-Object { $_.Name -notin @(([System.IO.Path]::GetFileName($channel + '.error')), ([System.IO.Path]::GetFileName($channel + '.done'))) } | Remove-Item -Force -ErrorAction SilentlyContinue
  [System.IO.File]::WriteAllText($channel + '.done', 'done')
  Remove-Item -LiteralPath ($channel + '.done') -Force -ErrorAction SilentlyContinue
  [System.IO.File]::WriteAllText($channel + '.released', 'released')
  Remove-Item -LiteralPath $helperPath -Force -ErrorAction SilentlyContinue
}
`;
      fs.writeFileSync(helperPath, script, 'utf8');
      const launcherPath = `${helperPath}.launch.ps1`;
      fs.writeFileSync(launcherPath, `$ErrorActionPreference = 'Stop'
$workerArgs = @('-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $args[0], $args[1], $args[2], $args[0], $args[3])
$command = 'powershell.exe ' + (($workerArgs | ForEach-Object { '"' + $_.Replace('"', '""') + '"' }) -join ' ')
try {
  $startup = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ ShowWindow = [uint16]0 }
  $result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $command; ProcessStartupInformation = $startup }
  if ($result.ReturnValue -ne 0) { throw "Win32_Process.Create returned $($result.ReturnValue)" }
  [System.IO.File]::WriteAllText($args[2] + '.helper-pid', [string]$result.ProcessId)
} catch {
  [System.IO.File]::WriteAllText($args[2] + '.launch-error', ($_ | Out-String))
  exit 1
}
`, 'utf8');
      const helper = spawn('powershell.exe', ['-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', launcherPath, helperPath, mutexName, channelPath, String(process.pid)], { stdio: 'ignore', windowsHide: true });
      helper.unref?.();
      helper.once('close', () => { try { fs.unlinkSync(launcherPath); } catch {} });
      return helper;
    }
    const { command, args } = buildPosixLockHelperCommand(this.lockPath, channelPath);
    const helper = spawn(command, args, { stdio: ['pipe', 'pipe', 'pipe'], detached: true }) as import('child_process').ChildProcessWithoutNullStreams;
    helper.unref?.();
    return helper;
  }

  private isPidAlive(pid?: number): boolean {
    if (!pid || pid <= 0) return false;
    try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException)?.code === 'EPERM'; }
  }

  private async stopUnleasedHelper(launcher: import('child_process').ChildProcess, channelPath: string): Promise<void> {
    if (process.platform !== 'win32') { launcher.kill(); return; }
    try { fs.writeFileSync(`${channelPath}.cancel`, 'cancel'); } catch {}
    const deadline = Date.now() + 1_000;
    while (Date.now() < deadline) {
      const observedPid = Number(fs.existsSync(`${channelPath}.helper-pid`) ? fs.readFileSync(`${channelPath}.helper-pid`, 'utf8') : 0);
      if (fs.existsSync(`${channelPath}.done`) || (observedPid && !this.isPidAlive(observedPid))) break;
      await sleep(25);
    }
    const helperPid = Number(fs.existsSync(`${channelPath}.helper-pid`) ? fs.readFileSync(`${channelPath}.helper-pid`, 'utf8') : 0);
    if (helperPid && this.isPidAlive(helperPid) && !fs.existsSync(`${channelPath}.done`)) {
      try { process.kill(helperPid, 'SIGKILL'); } catch {}
      const exitDeadline = Date.now() + 1_000;
      while (Date.now() < exitDeadline && this.isPidAlive(helperPid)) await sleep(25);
    }
    if (launcher.exitCode === null && launcher.signalCode === null) launcher.kill();
    try { fs.unlinkSync(`${channelPath}.done`); } catch {}
  }
}
