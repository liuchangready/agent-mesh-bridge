import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const CHILD_ACK_TIMEOUT_MS = 10_000;

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

export class MeshFileLock {
  readonly lockPath: string;
  private activeLease?: {
    token: string;
    helper: import('child_process').ChildProcess;
    helperPid?: number;
    channelPath: string;
    childAcks: Map<number, { resolve: () => void; reject: (error: Error) => void; timer: NodeJS.Timeout; timeout: NodeJS.Timeout }>;
  };
  constructor(lockPath = path.join(os.homedir(), '.agent-mesh-bridge', 'merge.lock')) {
    this.lockPath = lockPath;
  }

  get isHeld(): boolean {
    return Boolean(this.activeLease);
  }

  dispose(): void {
    if (this.activeLease) {
      const lease = this.activeLease;
      try {
        if (process.platform === 'win32') {
          fs.writeFileSync(`${lease.channelPath}.release`, 'release');
        } else {
          lease.helper.stdin?.write('RELEASE\n');
          lease.helper.stdin?.end();
        }
      } catch {}
      this.activeLease = undefined;
    }
  }

  async acquire(onWait?: () => void, cancelled?: () => boolean, timeoutMs?: number): Promise<(() => void) | undefined> {
    if (cancelled?.()) return undefined;
    const deadline = timeoutMs !== undefined ? Date.now() + timeoutMs : undefined;
    fs.mkdirSync(path.dirname(this.lockPath), { recursive: true });
    const token = crypto.randomUUID();
    const channelPath = `${this.lockPath}.${token}`;
    const child = this.startLockHelper(token, channelPath);
    const childAcks = new Map<number, { resolve: () => void; reject: (error: Error) => void; timer: NodeJS.Timeout; timeout: NodeJS.Timeout }>();
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
      readyPoll.unref?.();
      if (process.platform === 'win32') {
        readyTimeout = setTimeout(() => {
          if (!fs.existsSync(`${channelPath}.helper-pid`)) settleReady(new Error('Timed out starting the mesh lock helper.'));
        }, 15_000);
        readyTimeout.unref?.();
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
    const lease = { token, helper: child, helperPid, channelPath, childAcks };
    this.activeLease = lease;
    return () => {
      if (this.activeLease !== lease) return;
      try {
        const owner = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
        if (owner.token === token) fs.unlinkSync(ownerFile);
      } catch {}
      if (process.platform === 'win32') {
        try { fs.writeFileSync(`${lease.channelPath}.release`, 'release'); } catch {}
      } else {
        try { child.stdin!.write('RELEASE\n'); child.stdin!.end(); } catch {}
      }
      this.activeLease = undefined;
    };
  }

  attachChildPid(pid: number | undefined, cancelled?: () => boolean): Promise<void> {
    const lease = this.activeLease;
    if (!pid || !lease) return Promise.resolve();
    if (cancelled?.()) return Promise.reject(new Error('Child registration cancelled because the merge task is no longer active.'));
    return new Promise<void>((resolve, reject) => {
      const ackPath = `${lease.channelPath}.child.${pid}`;
      const finish = (error?: Error) => {
        const pending = lease.childAcks.get(pid);
        if (!pending) return;
        lease.childAcks.delete(pid);
        clearInterval(pending.timer);
        clearTimeout(pending.timeout);
        if (error) pending.reject(error); else pending.resolve();
      };
      const timer = setInterval(() => {
        if (fs.existsSync(ackPath)) {
          try { fs.unlinkSync(ackPath); } catch {}
          finish();
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
      timer.unref?.();
      const timeout = setTimeout(
        () => finish(new Error(`Timed out waiting ${CHILD_ACK_TIMEOUT_MS}ms for the mesh lock helper to acknowledge child ${pid}.`)),
        CHILD_ACK_TIMEOUT_MS,
      );
      timeout.unref?.();
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
    if (process.platform === 'win32') {
      try { fs.writeFileSync(`${lease.channelPath}.command.exit.${pid}`, 'EXIT'); } catch {}
    } else {
      try { lease.helper.stdin!.write(`EXIT ${pid}\n`); } catch {}
    }
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
      if ([int]::TryParse($pidText, [ref]$pidValue)) {
        try { $process = Get-Process -Id $pidValue -ErrorAction Stop; $children[$pidValue] = $process.StartTime.ToUniversalTime().Ticks } catch { }
        [System.IO.File]::WriteAllText(($channel + '.child.' + $pidValue), 'tracked')
      }
      Remove-Item -LiteralPath $command.FullName -Force -ErrorAction SilentlyContinue
    }
    foreach ($command in (Get-ChildItem -Path ($channel + '.command.exit.*') -ErrorAction SilentlyContinue)) {
      $pidValue = 0
      $pidText = $command.Name.Substring($exitPrefix.Length)
      if ([int]::TryParse($pidText, [ref]$pidValue)) { $children.Remove($pidValue) }
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
