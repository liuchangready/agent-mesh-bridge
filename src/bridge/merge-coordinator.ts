import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import { TaskRoleLock } from './task-role-lock';
import { TaskStore } from './task-store';
import { DelegationThread } from './types';
import { gitOutput, gitExecutable } from './git-worktree';
import { TaskExecutionRegistry } from './task-execution-registry';
import { killProcessTree } from './platform';
import { taskValidationDir } from './round-log';
import { resolveNodeCommand } from './deepseek-harness';
import { noteMergeFailure } from './merge-recovery';

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

  async acquire(onWait?: () => void, cancelled?: () => boolean): Promise<(() => void) | undefined> {
    if (cancelled?.()) return undefined;
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
      if (process.platform === 'win32') readyTimeout = setTimeout(() => {
        if (!fs.existsSync(`${channelPath}.helper-pid`)) settleReady(new Error('Timed out starting the mesh lock helper.'));
      }, 15_000);
    });
    void ready.catch(() => undefined);
    while (!acquired) {
      if (cancelled?.()) {
        settleReady(new Error('Mesh lock acquisition cancelled.'));
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
  Start-Sleep -Milliseconds 1000
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
  $result = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $command }
  if ($result.ReturnValue -ne 0) { throw "Win32_Process.Create returned $($result.ReturnValue)" }
  [System.IO.File]::WriteAllText($args[2] + '.helper-pid', [string]$result.ProcessId)
} catch {
  [System.IO.File]::WriteAllText($args[2] + '.launch-error', ($_ | Out-String))
  exit 1
}
`, 'utf8');
      const helper = spawn('powershell.exe', ['-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', launcherPath, helperPath, mutexName, channelPath, String(process.pid)], { stdio: 'ignore', windowsHide: true });
      helper.once('close', () => { try { fs.unlinkSync(launcherPath); } catch {} });
      return helper;
    }
    const { command, args } = buildPosixLockHelperCommand(this.lockPath, channelPath);
    return spawn(command, args, { stdio: ['pipe', 'pipe', 'pipe'], detached: true }) as import('child_process').ChildProcessWithoutNullStreams;
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

export interface MergeResult { status: 'merged' | 'no_changes' | 'conflict' | 'validation_failed' | 'stale' | 'blocked'; message: string; commit?: string; }

/** Where a repository declares its own combined-validation commands. */
export const VALIDATION_MANIFEST = path.join('.agent-mesh-bridge', 'validate.json');

export interface ValidationStep { command: string; args: string[]; label: string; needsShell: boolean; }

export type ValidationStrategy =
  | { ok: true; kind: 'package-scripts'; steps: ValidationStep[] }
  | { ok: true; kind: 'manifest'; steps: ValidationStep[] }
  | { ok: false; error: string };

/**
 * Resolve how to validate a merged tree.
 *
 * `package.json` scripts stay the primary contract because every Node project in
 * this mesh already carries them. A repository without one (Unity, Gradle, MSBuild)
 * declares its own commands in `.agent-mesh-bridge/validate.json` so the merge gate
 * stops refusing the whole project: refusing an approved Unity task forever is a
 * bridge capability gap, not a code defect. Everything else stays a hard refusal —
 * a missing strategy must never be read as "validation passed".
 */
export function readValidationStrategy(cwd: string): ValidationStrategy {
  const manifestPath = path.join(cwd, VALIDATION_MANIFEST);
  let pkg: any;
  try { pkg = JSON.parse(fs.readFileSync(path.join(cwd, 'package.json'), 'utf8')); }
  catch { pkg = undefined; }
  if (pkg) {
    const scripts = pkg.scripts || {};
    if (!scripts.compile || !scripts.test) {
      return { ok: false, error: 'Validation strategy must provide both package scripts "compile" and "test".' };
    }
    const steps: ValidationStep[] = [
      { command: scripts.compile, args: [], label: 'npm run compile', needsShell: true },
      { command: scripts.test, args: [], label: 'npm test', needsShell: true },
    ];
    return { ok: true, kind: 'package-scripts', steps };
  }
  if (!fs.existsSync(manifestPath)) {
    return {
      ok: false,
      error:
        'No supported validation strategy: package.json is missing or invalid and ' +
        `${VALIDATION_MANIFEST.split(path.sep).join('/')} was not found.`,
    };
  }
  let raw: any;
  try { raw = JSON.parse(fs.readFileSync(manifestPath, 'utf8')); }
  catch (error: any) {
    return { ok: false, error: `Validation manifest ${VALIDATION_MANIFEST} is not valid JSON: ${error?.message || error}` };
  }
  const declared = Array.isArray(raw?.commands) ? raw.commands : undefined;
  if (!declared || declared.length === 0) {
    return { ok: false, error: `Validation manifest ${VALIDATION_MANIFEST} must declare a non-empty "commands" array.` };
  }
  const steps: ValidationStep[] = [];
  for (const [index, entry] of declared.entries()) {
    if (typeof entry === 'string' && entry.trim()) {
      steps.push({ command: entry.trim(), args: [], label: `command ${index + 1}`, needsShell: true });
      continue;
    }
    if (!entry || typeof entry !== 'object' || typeof entry.command !== 'string' || !entry.command.trim()) {
      return { ok: false, error: `Validation manifest ${VALIDATION_MANIFEST} command ${index + 1} needs a non-empty "command".` };
    }
    const args = Array.isArray(entry.args) ? entry.args.map((value: unknown) => String(value)) : [];
    const label = typeof entry.label === 'string' && entry.label.trim() ? entry.label.trim() : `command ${index + 1}`;
    steps.push({ command: entry.command.trim(), args, label, needsShell: entry.shell === true });
  }
  return { ok: true, kind: 'manifest', steps };
}

/**
 * Directories that must be visible when a validation step shells out to Node.
 *
 * An extension host or MCP server can be launched from an environment that has
 * no `node` on PATH (a desktop app shipping its own runtime, a service manager,
 * a shortcut with a trimmed environment). The validation child inherits that
 * PATH, so `tsc` and `node --test` shims resolve `node` from the shell and fail
 * with "'node' is not recognized" even though dependencies are installed.
 *
 * Resolving the executable here and exposing its directory keeps the merge gate
 * independent of the environment the bridge process happened to inherit.
 */
export function validationNodeDirs(): string[] {
  const dirs: string[] = [];
  const resolved = resolveNodeCommand();
  if (resolved && (resolved.includes('/') || resolved.includes('\\'))) {
    dirs.push(path.dirname(resolved));
  }
  return dirs;
}

export class MergeCoordinator {
  private readonly executions: TaskExecutionRegistry;
  private readonly attemptLogs = new Map<string, { id: string; number: number; path: string; candidateTree?: string }>();

  constructor(
    private readonly store: TaskStore,
    private readonly storageDir: string,
    private readonly lock = new MeshFileLock(),
    private readonly requestReview?: (thread: DelegationThread) => void,
  ) {
    this.executions = new TaskExecutionRegistry(storageDir);
  }

  private async loggedGit(threadId: string, cwd: string, args: string[]): Promise<string> {
    const thread = this.store.getTask(threadId);
    if (!thread?.worktree?.approvedCommit || !this.isMergeStillAllowed(threadId, thread.worktree.approvedCommit)) throw new Error('Merge task was paused or deleted before Git could start.');
    const logPath = this.attemptLogs.get(threadId)?.path;
    if (logPath) fs.appendFileSync(logPath, `\n$ git ${args.join(' ')}\n`, 'utf8');
    const registration = this.executions.begin(threadId, thread?.currentRound || 0, 'merge-git', cwd);
    let child: ReturnType<typeof spawn>;
    try { child = spawn(gitExecutable(), args, { cwd, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] }); }
    catch (error) { this.executions.end(registration); throw error; }
    let stdout = ''; let stderr = ''; let trackingError: Error | undefined;
    try { this.executions.attachChildPid(child.pid, registration); }
    catch (error) { trackingError = error as Error; killProcessTree(child); }
    const record = (chunk: Buffer, isStdout: boolean) => {
      if (isStdout) stdout += chunk.toString();
      else stderr = (stderr + chunk.toString()).slice(-16_000);
      if (logPath) try { fs.appendFileSync(logPath, chunk); } catch (error) { trackingError = error as Error; killProcessTree(child); }
    };
    child.stdout!.on('data', (chunk: Buffer) => record(chunk, true));
    child.stderr!.on('data', (chunk: Buffer) => record(chunk, false));
    let resolveResult!: (result: { code: number | null; error?: Error }) => void;
    const completion = new Promise<{ code: number | null; error?: Error }>((resolve) => { resolveResult = resolve; });
    const timer = setTimeout(() => killProcessTree(child), 30_000);
    child.once('error', (error) => resolveResult({ code: null, error }));
    child.once('close', (code) => resolveResult({ code }));
    try {
      try { if (!trackingError) await this.lock.attachChildPid(child.pid, () => !this.isMergeStillAllowed(threadId, thread?.worktree?.approvedCommit || '')); }
      catch (error) { trackingError = error as Error; killProcessTree(child); }
      const result = await completion;
      if (logPath) fs.appendFileSync(logPath, `\n[exit ${result.code}]\n`, 'utf8');
      if (trackingError || result.error || result.code !== 0) throw new Error(`Git command failed (${args.join(' ')}): ${trackingError?.message || result.error?.message || stderr.trim() || `exit ${result.code}`}`);
      return stdout.trim();
    } finally {
      clearTimeout(timer); this.lock.detachChildPid(child.pid); this.executions.end(registration);
    }
  }

  /** Approval is tied to the exact clean task commit, then serialized with target read through durable ledger write. */
  async mergeApproved(threadId: string): Promise<MergeResult> {
    const thread = this.store.getTask(threadId);
    if (!thread?.worktree?.targetRef || !thread.worktree.approvedCommit) throw new Error(`Task ${threadId} has no approved isolated worktree commit.`);
    if (thread.status !== 'waiting_merge') throw new Error(`Task ${threadId} is not waiting to merge.`);
    const worktree = thread.worktree;
    const repoPath = worktree.deliveryTarget?.repositoryPath || worktree.repositoryPath || worktree.mainPath;
    const targetRef = worktree.deliveryTarget?.ref || worktree.targetRef!;
    const sourceCommit = worktree.approvedCommit!;
    const interruptionCount = thread.interruptions?.length || 0;
    const stillCurrent = () => this.isMergeStillAllowed(threadId, sourceCommit) &&
      (this.store.getTask(threadId)?.interruptions?.length || 0) === interruptionCount;
    await this.waitForReviewerExit(thread, stillCurrent);
    if (!stillCurrent()) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
    const mergeRole = new TaskRoleLock(this.storageDir);
    let roleLease = mergeRole.tryAcquire(threadId, 'reviewer', thread.reviewFlow === 'dedicated' ? thread.reviewerSessionId : undefined);
    while (!roleLease && stillCurrent()) {
      await sleep(250);
      if (stillCurrent()) roleLease = mergeRole.tryAcquire(threadId, 'reviewer', thread.reviewFlow === 'dedicated' ? thread.reviewerSessionId : undefined);
    }
    if (!roleLease) return { status: 'stale', message: 'Task was paused or deleted while waiting for its role.' };
    if (!stillCurrent()) { mergeRole.release(roleLease); return { status: 'stale', message: 'Task was paused before acquiring its role.' }; }
    let release: (() => void) | undefined;
    try { release = await this.lock.acquire(undefined, () => !this.isMergeStillAllowed(threadId, sourceCommit)); }
    catch (error) { mergeRole.release(roleLease); throw error; }
    if (!release) { mergeRole.release(roleLease); return { status: 'stale', message: 'Task was paused or deleted while waiting for local integration.' }; }
    let startedTargetCommit = '';
    try {
      // This is intentionally the first target read in the critical section.
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
      {
        const latest = this.store.getTask(threadId)!;
        const number = (latest.mergeHistory?.length || 0) + 1;
        const safeId = threadId.replace(/[^A-Za-z0-9_-]/g, '_');
        const dir = path.join(latest.artifactDir || path.join(this.storageDir, 'merge-logs', safeId), 'merge_log');
        fs.mkdirSync(dir, { recursive: true });
        const logPath = path.join(dir, `${safeId}_merge_${number}.log`);
        fs.writeFileSync(logPath, `Merge attempt ${number} for ${threadId}\n`, { encoding: 'utf8', flag: 'wx' });
        const id = crypto.randomUUID();
        this.attemptLogs.set(threadId, { id, number, path: logPath, candidateTree: worktree.approvedSnapshot?.tree });
        const updated = structuredClone(latest);
        updated.mergeHistory = [...(updated.mergeHistory || []), { id, number, at: new Date().toISOString(), phase: 'attempt', result: 'started',
          logPath, targetRef, sourceCommit: worktree.approvedSnapshot?.sourceCommit, candidateCommit: sourceCommit, candidateTree: worktree.approvedSnapshot?.tree,
          retryGeneration: updated.mergeControl?.retryGeneration || 0,
          decisionId: [...(updated.mergeHistory || [])].reverse().find((entry) => entry.phase === 'decision' && entry.result === 'answered')?.decisionId }];
        this.store.upsertTask(updated);
      }
      if (!fs.existsSync(worktree.path)) {
        const moved = (await this.loggedGit(threadId, repoPath, ['worktree', 'list', '--porcelain'])).split(/\r?\n\r?\n/)
          .map((value) => ({ branch: value.match(/^branch (.+)$/m)?.[1], worktree: value.match(/^worktree (.+)$/m)?.[1] }))
          .find((item) => item.branch === `refs/heads/${worktree.taskBranch}` && item.worktree && fs.existsSync(item.worktree));
        if (!moved?.worktree) {
          const reason = `Task worktree ${worktree.path} is missing and no registered worktree for ${worktree.taskBranch} was found.`;
          this.finishBlocked(threadId, reason, '', sourceCommit); return { status: 'blocked', message: reason };
        }
        const updated = structuredClone(this.store.getTask(threadId)!); updated.worktree!.path = moved.worktree; updated.workspace.executionPath = moved.worktree; this.store.upsertTask(updated);
        worktree.path = moved.worktree; thread.workspace.executionPath = moved.worktree;
      }
      const lockedHead = worktree.approvedSnapshot ? sourceCommit : await this.loggedGit(threadId, worktree.path, ['rev-parse', '--verify', 'HEAD^{commit}']);
      if (!worktree.approvedSnapshot && (lockedHead !== sourceCommit || await this.loggedGit(threadId, worktree.path, ['status', '--porcelain', '--untracked-files=all']))) {
        this.markNeedsReview(thread, `Approved commit ${sourceCommit} changed while waiting for integration (HEAD ${lockedHead}, dirty worktree present).`);
        return { status: 'stale', message: 'The approved task worktree changed while waiting; reviewer must inspect the new state.' };
      }
      const targetOldCommit = await this.loggedGit(threadId, repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
      startedTargetCommit = targetOldCommit;
      const attempt = this.attemptLogs.get(threadId);
      if (attempt) {
        attempt.candidateTree ||= await this.loggedGit(threadId, repoPath, ['rev-parse', '--verify', `${sourceCommit}^{tree}`]);
        const latest = this.store.getTask(threadId)!; const updated = structuredClone(latest);
        updated.mergeHistory = [...(updated.mergeHistory || []), { id: crypto.randomUUID(), number: (updated.mergeHistory?.length || 0) + 1,
          at: new Date().toISOString(), phase: 'target', result: 'read', attemptId: attempt.id, targetRef, targetOldCommit,
          candidateCommit: sourceCommit, candidateTree: attempt.candidateTree }];
        this.store.upsertTask(updated);
      }
      const txPath = this.transactionPath(threadId);
      const prior = this.readTransaction(txPath);
      let recovered = false;
      if (prior?.candidateCommit && prior.sourceCommit === sourceCommit &&
          (prior.phase === 'validated' || prior.phase === 'ref_updated' || prior.phase === 'ledger_written')) {
        if (prior.candidateCommit === targetOldCommit) recovered = true;
        else {
          try { await this.loggedGit(threadId, repoPath, ['merge-base', '--is-ancestor', prior.candidateCommit, targetOldCommit]); recovered = true; }
          catch { recovered = false; }
        }
      }
      if (recovered) {
        const sync = await this.syncCheckedOutTarget(repoPath, targetRef, prior.candidateCommit!, false, prior.targetOldCommit, threadId);
        if (!sync.ok) { this.finishBlocked(threadId, sync.error, prior.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
        const validationStatus = prior.validationPath ? await this.cleanupValidationWorktree(repoPath, threadId, prior.validationPath) : undefined;
        await this.finishMerged(threadId, sync.commit || prior.candidateCommit!, prior.targetOldCommit, sync.path,
          prior.validationPath ? this.validationRecord(prior.validationPath, prior.validationCommit, prior.validationResult, validationStatus) : undefined, prior.candidateCommit);
        prior.phase = 'ledger_written'; this.writeTransaction(txPath, prior);
        return { status: 'merged', message: 'Recovered completed merge transaction.', commit: sync.commit || prior.candidateCommit };
      }
      const resolutionPath = worktree.mergeResolutionSubmitted ? worktree.mergeResolutionPath : undefined;
      if (resolutionPath && (worktree.mergeResolutionSourceCommit || worktree.integration?.sourceCommit) !== sourceCommit) {
        const reason = 'The submitted resolution belongs to a different approved candidate.';
        this.finishBlocked(threadId, reason, worktree.integration?.targetOldCommit || '', sourceCommit);
        return { status: 'blocked', message: reason };
      }
      if (resolutionPath && worktree.integration?.targetOldCommit !== targetOldCommit) {
        const oldTarget = worktree.integration?.targetOldCommit;
        const prepared = prior?.phase === 'resolution_rebase_prepared' && prior.sourceCommit === sourceCommit && prior.validationPath === resolutionPath ? prior : undefined;
        if (!oldTarget || (prepared && prepared.targetOldCommit !== targetOldCommit)) {
          const reason = 'Target changed while a resolution update was in progress; the resolution workspace was preserved.';
          this.finishBlocked(threadId, reason, oldTarget || '', sourceCommit, undefined, resolutionPath);
          return { status: 'blocked', message: reason };
        }
        const head = await this.loggedGit(threadId, resolutionPath, ['rev-parse', '--verify', 'HEAD^{commit}']);
        let resolutionCommit: string = prepared?.resolutionCommit;
        if (!prepared) {
          if (head !== oldTarget || await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only', '--diff-filter=U']) ||
              await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only']) || await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--exclude-standard']) ||
              await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--ignored', '--exclude-standard'])) {
            const reason = 'Resolution workspace has unstaged, untracked, ignored, or unresolved files; it was preserved.';
            this.finishBlocked(threadId, reason, oldTarget, sourceCommit, undefined, resolutionPath);
            return { status: 'blocked', message: reason };
          }
          const tree = await this.loggedGit(threadId, resolutionPath, ['write-tree']);
          resolutionCommit = await this.loggedGit(threadId, resolutionPath, ['-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit-tree', tree, '-p', oldTarget, '-m', `Resolution ${threadId}`]);
          await this.loggedGit(threadId, repoPath, ['update-ref', `refs/mesh/resolutions/${threadId}`, resolutionCommit]);
          this.writeTransaction(txPath, { threadId, sourceCommit, targetOldCommit, validationPath: resolutionPath,
            phase: 'resolution_rebase_prepared', resolutionCommit, resolutionBaseCommit: oldTarget, at: new Date().toISOString() });
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted before updating the resolution.' };
        if (head === oldTarget) {
          if (await this.loggedGit(threadId, resolutionPath, ['write-tree']) !== await this.loggedGit(threadId, repoPath, ['rev-parse', `${resolutionCommit}^{tree}`]) ||
              await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only']) || await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--exclude-standard']) ||
              await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--ignored', '--exclude-standard'])) {
            const reason = 'The staged resolution changed after its recovery snapshot was saved; it was preserved.';
            this.finishBlocked(threadId, reason, oldTarget, sourceCommit, undefined, resolutionPath);
            return { status: 'blocked', message: reason };
          }
          await this.loggedGit(threadId, resolutionPath, ['reset', '--hard', targetOldCommit]);
        } else if (head !== targetOldCommit) {
          const reason = 'Resolution workspace HEAD changed during recovery; it was preserved.';
          this.finishBlocked(threadId, reason, oldTarget, sourceCommit, undefined, resolutionPath);
          return { status: 'blocked', message: reason };
        }
        let merging = false;
        try { await this.loggedGit(threadId, resolutionPath, ['rev-parse', '--verify', 'MERGE_HEAD']); merging = true; } catch {}
        if (!merging) {
          if (await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only']) || await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--exclude-standard']) ||
              await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--ignored', '--exclude-standard'])) {
            const reason = 'Resolution workspace gained unstaged, untracked, or ignored files during recovery; it was preserved.';
            this.finishBlocked(threadId, reason, oldTarget, sourceCommit, undefined, resolutionPath);
            return { status: 'blocked', message: reason };
          }
          try { await this.loggedGit(threadId, resolutionPath, ['merge', '--no-commit', '--no-ff', resolutionCommit]); }
          catch (error) {
            if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted during resolution merge.' };
            const conflicts = (await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only', '--diff-filter=U'])).split(/\r?\n/).filter(Boolean);
            const message = `Resolution conflicts with advanced target ${targetOldCommit}: ${conflicts.join(', ') || String(error)}`;
            this.finishMergeFailure(threadId, 'conflict', message, targetOldCommit, sourceCommit, conflicts, resolutionPath);
            return { status: 'conflict', message };
          }
        }
        const conflicts = (await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only', '--diff-filter=U'])).split(/\r?\n/).filter(Boolean);
        if (conflicts.length) {
          const message = `Resolution conflicts with advanced target ${targetOldCommit}: ${conflicts.join(', ')}`;
          this.finishMergeFailure(threadId, 'conflict', message, targetOldCommit, sourceCommit, conflicts, resolutionPath);
          return { status: 'conflict', message };
        }
        worktree.integration = { ...worktree.integration!, phase: 'waiting', targetOldCommit, sourceCommit, updatedAt: new Date().toISOString() };
        thread.updatedAt = worktree.integration.updatedAt;
        this.store.upsertTask(thread);
      }
      if (prior?.validationPath && prior.validationPath !== resolutionPath && !(worktree.supersededMergeResolutionPaths || []).includes(prior.validationPath)) await this.cleanupValidationWorktree(repoPath, threadId, prior.validationPath);
      const tx = { threadId, sourceCommit, targetOldCommit, phase: 'preparing', at: new Date().toISOString() } as any;
      this.writeTransaction(txPath, tx);
      const scratchRoot = thread.artifactDir
        ? taskValidationDir(thread.artifactDir)
        : path.join(this.storageDir, 'merge-worktrees');
      const scratch = resolutionPath || path.join(scratchRoot, `${threadId}-${crypto.randomUUID()}`);
      fs.mkdirSync(scratchRoot, { recursive: true });
      tx.validationPath = scratch; this.writeTransaction(txPath, tx);
      let added = !!resolutionPath;
      let validationCommit: string | undefined;
      let validationResult: 'passed' | 'failed' | 'cancelled' | undefined;
      let retainForResolution = !!resolutionPath;
      try {
        if (!resolutionPath) { added = true; await this.loggedGit(threadId, repoPath, ['worktree', 'add', '--detach', scratch, targetOldCommit]); }
        try { if (!resolutionPath) await this.loggedGit(threadId, scratch, ['merge', '--no-commit', '--no-ff', sourceCommit]); }
        catch (error) {
          if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted during merge.' }; }
          const conflicts = (await this.loggedGit(threadId, scratch, ['diff', '--name-only', '--diff-filter=U'])).split(/\r?\n/).filter(Boolean);
          const message = `Merge conflict against ${targetOldCommit}: ${conflicts.join(', ') || String(error)}`;
          retainForResolution = true;
          this.finishMergeFailure(threadId, 'conflict', message, targetOldCommit, sourceCommit, conflicts, retainForResolution ? scratch : undefined);
          return { status: 'conflict', message };
        }
        if (resolutionPath && (await this.loggedGit(threadId, scratch, ['rev-parse', '--verify', 'HEAD^{commit}']) !== targetOldCommit || await this.loggedGit(threadId, scratch, ['diff', '--name-only', '--diff-filter=U']) || await this.loggedGit(threadId, scratch, ['diff', '--name-only']) || await this.loggedGit(threadId, scratch, ['ls-files', '--others', '--exclude-standard']))) {
          this.finishMergeFailure(threadId, 'conflict', 'Resolution workspace changed or contains unstaged or unresolved files.', targetOldCommit, sourceCommit, undefined, scratch);
          return { status: 'conflict', message: 'Resolution workspace changed or contains unstaged or unresolved files.' };
        }
        const treeBeforeValidation = await this.loggedGit(threadId, scratch, ['write-tree']);
        const validation = await this.validate(scratch, repoPath, threadId, sourceCommit);
        validationResult = !this.isMergeStillAllowed(threadId, sourceCommit) ? 'cancelled' : validation.ok ? 'passed' : 'failed';
        tx.validationResult = validationResult; this.writeTransaction(txPath, tx);
        const validationAttempt = this.attemptLogs.get(threadId);
        const validationThread = this.store.getTask(threadId);
        if (validationAttempt && validationThread) {
          const updated = structuredClone(validationThread);
          updated.mergeHistory = [...(updated.mergeHistory || []), { id: crypto.randomUUID(), number: (updated.mergeHistory?.length || 0) + 1,
            at: new Date().toISOString(), phase: 'validation', result: validationResult, attemptId: validationAttempt.id,
            exitCode: validation.ok ? 0 : Number(validation.error.match(/exit\s+(-?\d+)/i)?.[1] || -1) }];
          this.store.upsertTask(updated);
        }
        if (!validation.ok) {
          if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted during combined validation.' }; }
          retainForResolution = true;
          this.finishMergeFailure(threadId, 'validation_failed', validation.error, targetOldCommit, sourceCommit, undefined, retainForResolution ? scratch : undefined);
          return { status: 'validation_failed', message: validation.error };
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted during combined validation.' }; }
        const validatedHead = worktree.approvedSnapshot ? sourceCommit : await this.loggedGit(threadId, worktree.path, ['rev-parse', '--verify', 'HEAD^{commit}']);
        if (!worktree.approvedSnapshot && (validatedHead !== sourceCommit || await this.loggedGit(threadId, worktree.path, ['status', '--porcelain', '--untracked-files=all']))) {
          this.markNeedsReview(thread, `Approved commit ${sourceCommit} changed during combined validation (HEAD ${validatedHead}, dirty worktree present).`);
          return { status: 'stale', message: 'The approved task worktree changed during validation; reviewer must inspect the new state.' };
        }
        const validationTree = await this.loggedGit(threadId, scratch, ['write-tree']);
        if (validationTree !== treeBeforeValidation || await this.loggedGit(threadId, scratch, ['diff', '--name-only'])) {
          const error = 'Combined validation changed the staged or tracked working tree; the tested tree cannot be safely integrated.';
          retainForResolution = true;
          this.finishMergeFailure(threadId, 'validation_failed', error, targetOldCommit, sourceCommit, undefined, retainForResolution ? scratch : undefined);
          return { status: 'validation_failed', message: error };
        }
        const candidateCommit = await this.loggedGit(threadId, scratch, ['-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit-tree', validationTree, '-p', targetOldCommit, '-p', sourceCommit, '-m', `Merge ${threadId}`]);
        if (await this.loggedGit(threadId, scratch, ['rev-parse', `${candidateCommit}^{tree}`]) !== validationTree || await this.loggedGit(threadId, scratch, ['write-tree']) !== validationTree) {
          throw new Error('The recorded validation commit does not match the combined tree that was validated.');
        }
        validationCommit = candidateCommit;
        tx.validationCommit = validationCommit;
        tx.phase = 'validated'; tx.candidateCommit = candidateCommit; this.writeTransaction(txPath, tx);
        retainForResolution = true;
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted before the validated integration could be written.' }; }
        const beforeLanding = await this.syncCheckedOutTarget(repoPath, targetRef, candidateCommit, true, targetOldCommit, threadId);
        if (!beforeLanding.ok) {
          this.finishBlocked(threadId, beforeLanding.error, targetOldCommit, sourceCommit, beforeLanding.path, retainForResolution ? scratch : undefined);
          return { status: 'blocked', message: beforeLanding.error };
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted before the target ref update.' }; }
        // update-ref's old-value argument prevents overwriting a ref changed outside this lock.
        await this.loggedGit(threadId, repoPath, ['update-ref', targetRef, candidateCommit, targetOldCommit]);
        tx.phase = 'ref_updated'; this.writeTransaction(txPath, tx);
        const synced = await this.syncCheckedOutTarget(repoPath, targetRef, candidateCommit, false, targetOldCommit, threadId);
        if (!synced.ok) { this.finishBlocked(threadId, synced.error, targetOldCommit, sourceCommit, synced.path, retainForResolution ? scratch : undefined); return { status: 'blocked', message: synced.error }; }
        const validationStatus = await this.removeValidationWorktree(repoPath, scratch, threadId) ? 'cleaned' : 'present'; added = validationStatus === 'present';
        await this.finishMerged(threadId, synced.commit || candidateCommit, targetOldCommit, synced.path, this.validationRecord(scratch, validationCommit, validationResult, validationStatus), candidateCommit);
        tx.phase = 'ledger_written'; this.writeTransaction(txPath, tx);
      return { status: 'merged', message: 'Merged and validated.', commit: synced.commit || candidateCommit };
      } finally {
        if (added && retainForResolution) {
          this.recordValidation(threadId, scratch, validationCommit, 'present', validationResult);
        } else if (added) {
          const validationStatus = await this.removeValidationWorktree(repoPath, scratch, threadId) ? 'cleaned' : 'present';
          this.recordValidation(threadId, scratch, validationCommit, validationStatus, validationResult);
        }
      }
    } catch (error) {
      // If update-ref succeeded but a later transaction/ledger write failed,
      // retain the landed fact and finish the same transaction before reporting.
      if (await this.hasLandedCandidate(threadId)) {
        const tx = this.readTransaction(this.transactionPath(threadId));
        if (tx?.candidateCommit) {
          const sync = await this.syncCheckedOutTarget(repoPath, targetRef, tx.candidateCommit, false, tx.targetOldCommit, threadId);
          if (!sync.ok) { this.finishBlocked(threadId, sync.error, tx.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
          const validationStatus = tx.validationPath ? await this.cleanupValidationWorktree(repoPath, threadId, tx.validationPath) : undefined;
          await this.finishMerged(threadId, sync.commit || tx.candidateCommit, tx.targetOldCommit, sync.path,
            tx.validationPath ? this.validationRecord(tx.validationPath, tx.validationCommit, tx.validationResult, validationStatus) : undefined, tx.candidateCommit);
          tx.phase = 'ledger_written';
          this.writeTransaction(this.transactionPath(threadId), tx);
          return { status: 'merged', message: 'Recovered target update after a durable ledger write failure.', commit: sync.commit || tx.candidateCommit };
        }
      }
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted during integration.' };
      const latest = this.store.getTask(threadId);
      if (latest?.status === 'waiting_merge' && latest.worktree?.deliveryTarget) {
        const reason = `Could not safely deliver to ${targetRef}: ${String(error)}`;
        // A prepared resolution is still based on its previous target even if
        // the checkout reset completed before the interruption.  The durable
        // transaction uses that base to replay the saved resolution safely.
        const resolutionBase = worktree.mergeResolutionSubmitted ? worktree.integration?.targetOldCommit : undefined;
        this.finishBlocked(threadId, reason, resolutionBase || startedTargetCommit || latest.worktree?.integration?.targetOldCommit || '', sourceCommit, undefined,
          latest.worktree.validation?.path);
        return { status: 'blocked', message: reason };
      }
      throw error;
    } finally {
      const attempt = this.attemptLogs.get(threadId);
      try {
        const latest = this.store.getTask(threadId);
        if (attempt && latest?.status === 'paused') {
          fs.appendFileSync(attempt.path, '\n[cancelled]\n', 'utf8');
          const updated = structuredClone(latest);
          updated.mergeHistory = [...(updated.mergeHistory || []), { id: crypto.randomUUID(), number: (updated.mergeHistory?.length || 0) + 1,
            at: new Date().toISOString(), phase: 'attempt', result: 'cancelled', attemptId: attempt.id }];
          this.store.upsertTask(updated);
        }
      } catch (error) { console.error(`[MergeCoordinator] Could not record cancellation for ${threadId}:`, error); }
      this.attemptLogs.delete(threadId); release(); mergeRole.release(roleLease);
    }
  }

  private async syncCheckedOutTarget(repoPath: string, targetRef: string, commit: string, preflight = false, previousCommit?: string, threadId?: string): Promise<{ ok: boolean; error: string; path?: string; commit?: string }> {
    const run = (cwd: string, args: string[]) => threadId ? this.loggedGit(threadId, cwd, args) : Promise.resolve(gitOutput(cwd, args));
    const head = (cwd: string) => run(cwd, ['rev-parse', '--verify', 'HEAD^{commit}']);
    const targetCommit = await run(repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
    const rows = (await run(repoPath, ['worktree', 'list', '--porcelain'])).split(/\r?\n\r?\n/);
    const row = rows.map((value) => ({ value, branch: value.match(/^branch (.+)$/m)?.[1], worktree: value.match(/^worktree (.+)$/m)?.[1] }))
      .find((item) => item.branch === targetRef);
    if (preflight) {
      if (!row?.worktree) return { ok: true, error: '', commit: targetCommit };
      if (!fs.existsSync(row.worktree)) return { ok: false, error: `Target branch ${targetRef} is registered at missing worktree ${row.worktree}; restore or remove that worktree registration, then retry merge.` };
      const dirty = await run(row.worktree, ['status', '--porcelain', '--untracked-files=all']);
      if (targetCommit !== previousCommit || await head(row.worktree) !== targetCommit || dirty) return { ok: false, path: row.worktree, error: `Target branch ${targetRef} changed or has uncommitted files in ${row.worktree}. Preserve or commit those changes, then retry merge; the bridge left them untouched.` };
      return { ok: true, error: '', path: row.worktree, commit: targetCommit };
    }
    if (targetCommit !== commit) {
      try { await run(repoPath, ['merge-base', '--is-ancestor', commit, targetCommit]); }
      catch { return { ok: false, path: row?.worktree, error: `Target branch ${targetRef} moved to unrelated commit ${targetCommit}; the bridge did not report this task as delivered.` }; }
    }
    if (!row?.worktree) return { ok: true, error: '', commit: targetCommit };
    if (!fs.existsSync(row.worktree)) return { ok: false, error: `Target branch ${targetRef} is registered at missing worktree ${row.worktree}; restore or remove that worktree registration, then retry merge.` };
    const dirty = await run(row.worktree, ['status', '--porcelain', '--untracked-files=all']);
    if (await head(row.worktree) === targetCommit && !dirty) return { ok: true, error: '', path: row.worktree, commit: targetCommit };
    const stagedTree = previousCommit && await run(row.worktree, ['write-tree']);
    const indexIsPriorTarget = !!previousCommit && stagedTree === await run(repoPath, ['rev-parse', `${previousCommit}^{tree}`]);
    const unstagedOrUntracked = dirty.split(/\r?\n/).filter(Boolean).some((line) => line.startsWith('??') || line[1] !== ' ');
    if (!previousCommit || !indexIsPriorTarget || unstagedOrUntracked) return { ok: false, path: row.worktree, error: `Target checkout ${row.worktree} no longer matches the pre-merge target; preserve or inspect its files, then retry. The bridge did not overwrite local changes.` };
    try {
      await run(row.worktree, ['read-tree', '-u', '-m', previousCommit, targetCommit]);
      const latestTarget = await run(repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
      if (latestTarget !== targetCommit || await head(row.worktree) !== latestTarget || await run(row.worktree, ['status', '--porcelain', '--untracked-files=all'])) {
        return { ok: false, path: row.worktree, error: `Target branch ${targetRef} changed while its checkout was synchronizing; inspect ${row.worktree} and retry. The task was not marked completed.` };
      }
    } catch (error) { return { ok: false, path: row.worktree, error: `Could not safely synchronize checked-out target ${targetRef} at ${row.worktree}: ${String(error)}. The bridge did not discard local files.` }; }
    return { ok: true, error: '', path: row.worktree, commit: targetCommit };
  }

  private recordValidation(threadId: string, validationPath: string, commit: string | undefined, status: 'passed' | 'failed' | 'cancelled' | 'cleaned' | 'present', result?: 'passed' | 'failed' | 'cancelled'): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    thread.worktree!.validation = { path: validationPath, commit, status, result, updatedAt: new Date().toISOString() };
    if (thread.status === 'completed' || thread.status === 'failed' || thread.status === 'review_failed') thread.finalSummary = undefined;
    thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
  }

  private async cleanupValidationWorktree(repoPath: string, threadId: string, validationPath: string): Promise<'cleaned' | 'present'> {
    const candidate = path.resolve(validationPath);
    const allowedRoots = [path.resolve(this.storageDir, 'merge-worktrees')];
    const thread = this.store.getTask(threadId);
    if (thread?.artifactDir) allowedRoots.push(path.resolve(taskValidationDir(thread.artifactDir)));
    const allowed = allowedRoots.some((root) => candidate.startsWith(`${root}${path.sep}`));
    if (!allowed || !path.basename(candidate).startsWith(`${threadId}-`)) return 'present';
    const status = await this.removeValidationWorktree(repoPath, candidate, threadId) ? 'cleaned' : 'present';
    this.recordValidation(threadId, candidate, undefined, status);
    return status;
  }

  private async removeValidationWorktree(repoPath: string, validationPath: string, threadId: string): Promise<boolean> {
    try {
      if (fs.existsSync(validationPath)) await this.loggedGit(threadId, repoPath, ['worktree', 'remove', '--force', validationPath]);
      const registered = (await this.loggedGit(threadId, repoPath, ['worktree', 'list', '--porcelain'])).split(/\r?\n/).some((line) => line === `worktree ${validationPath}`);
      return !fs.existsSync(validationPath) && !registered;
    } catch { return false; }
  }

  private validationRecord(validationPath: string, commit: string | undefined, result?: 'passed' | 'failed' | 'cancelled', status: 'cleaned' | 'present' = 'present'): NonNullable<DelegationThread['worktree']>['validation'] {
    return { path: validationPath, commit, status, result, updatedAt: new Date().toISOString() };
  }

  private finishBlocked(threadId: string, reason: string, targetOldCommit: string, sourceCommit: string, targetPath?: string, resolutionPath?: string): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    thread.status = 'waiting_merge';
    if (resolutionPath) { thread.worktree!.mergeResolutionPath = resolutionPath; thread.worktree!.mergeResolutionSourceCommit = sourceCommit; }
    thread.worktree!.integration = { ...thread.worktree!.integration, phase: 'blocked', error: reason, targetOldCommit, targetPath, sourceCommit, updatedAt: new Date().toISOString() };
    if (thread.worktree!.approvedCommit) {
      const code = /uncommitted|dirty|no longer matches/i.test(reason) ? 'DIRTY_CHECKOUT' : /update-ref|cannot lock ref|expected/i.test(reason) ? 'CAS_FAILED' :
        /permission|denied|unauthori[sz]ed/i.test(reason) ? 'PERMISSION' : /target.*(?:advanced|changed|moved)/i.test(reason) ? 'TARGET_MOVED' : 'BLOCKED';
      const budget = noteMergeFailure(thread, 'blocked', code, reason);
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number: (thread.mergeHistory?.length || 0) + 1,
        at: new Date().toISOString(), phase: 'blocked', result: 'failed', error: reason.slice(-5000), errorCode: code,
        targetRef: thread.worktree!.deliveryTarget?.ref || thread.worktree!.targetRef, targetOldCommit, candidateCommit: sourceCommit,
        candidateTree: this.attemptLogs.get(threadId)?.candidateTree, attemptId: this.attemptLogs.get(threadId)?.id, ...budget }];
    }
    thread.summary.lastOutputSnippet = reason; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
    if (thread.worktree!.approvedCommit && thread.status === 'waiting_merge') {
      const timer = setTimeout(() => {
        const latest = this.store.getTask(threadId);
        if (latest?.status === 'waiting_merge' && latest.worktree?.integration?.phase === 'blocked') this.requestReview?.(latest);
      }, 0);
      timer.unref();
    }
  }

  private async waitForReviewerExit(thread: DelegationThread, stillCurrent: () => boolean): Promise<void> {
    const roles = new TaskRoleLock(this.storageDir);
    while (true) {
      if (!stillCurrent()) return;
      const owner = roles.getOwner(thread.threadId);
      if (!owner || owner.role !== 'reviewer') return;
      try { process.kill(owner.childPid || owner.pid, 0); } catch { return; }
      await sleep(250);
    }
  }

  private async validate(cwd: string, repositoryPath: string, threadId: string, sourceCommit: string): Promise<{ ok: boolean; error: string }> {
    const strategy = readValidationStrategy(cwd);
    if (!strategy.ok) {
      const logPath = this.attemptLogs.get(threadId)?.path;
      if (logPath) fs.appendFileSync(logPath, `Validation strategy failed: ${strategy.error}\n`, 'utf8');
      return { ok: false, error: strategy.error };
    }
    const registration = this.executions.begin(threadId, this.store.getTask(threadId)?.currentRound || 0, 'merge-validation', cwd);
    const sourceModules = path.join(repositoryPath, 'node_modules');
    const scratchModules = path.join(cwd, 'node_modules');
    let linkedModules = false;
    try {
    if (strategy.kind === 'package-scripts' && fs.existsSync(sourceModules) && !fs.existsSync(scratchModules)) {
      try { fs.symlinkSync(sourceModules, scratchModules, process.platform === 'win32' ? 'junction' : 'dir'); linkedModules = true; }
      catch (error: any) { return { ok: false, error: `Could not expose the installed project dependencies to the temporary validation worktree: ${error?.message || error}` }; }
    }
    for (const step of strategy.steps) {
      const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === 'path') || 'PATH';
      const binPath = path.join(repositoryPath, 'node_modules', '.bin');
      const searchPath = [binPath, ...validationNodeDirs(), process.env[pathKey] || '']
        .filter((entry) => !!entry)
        .join(path.delimiter);
      const env = { ...process.env, [pathKey]: searchPath };
      const logPath = this.attemptLogs.get(threadId)?.path;
      if (logPath) fs.appendFileSync(logPath, `\n$ ${step.command} ${step.args.join(' ')}\n`, 'utf8');
      const child = spawn(step.command, step.args, { cwd, env, windowsHide: true, shell: step.needsShell });
      let output = '';
      let trackingError: Error | undefined;
      try { this.executions.attachChildPid(child.pid, registration); }
      catch (error) { trackingError = error as Error; killProcessTree(child); }
      let resolveResult!: (result: { code: number | null; output: string }) => void;
      const resultPromise = new Promise<{ code: number | null; output: string }>((resolve) => { resolveResult = resolve; });
      const record = (chunk: Buffer) => {
        output = (output + chunk.toString()).slice(-8192);
        if (logPath) try { fs.appendFileSync(logPath, chunk); } catch (error) { trackingError = error as Error; killProcessTree(child); }
      };
      child.stdout.on('data', record);
      child.stderr.on('data', record);
      const timer = setTimeout(() => {
        child.kill('SIGTERM');
        setTimeout(() => child.kill('SIGKILL'), 5_000).unref();
      }, 20 * 60 * 1000);
      child.on('close', (code) => { clearTimeout(timer); this.lock.detachChildPid(child.pid); resolveResult({ code: trackingError ? -1 : code, output: trackingError?.message || output }); });
      child.on('error', (error) => { clearTimeout(timer); this.lock.detachChildPid(child.pid); resolveResult({ code: -1, output: error.message }); });
      try { if (!trackingError) await this.lock.attachChildPid(child.pid, () => !this.isMergeStillAllowed(threadId, sourceCommit)); }
      catch (error) { trackingError = error as Error; killProcessTree(child); }
      const result = await resultPromise;
      if (logPath) fs.appendFileSync(logPath, `\n${step.label} ${result.code === 0 ? 'passed' : `failed (exit ${result.code})`}\n`, 'utf8');
      if (result.code !== 0) return { ok: false, error: `${step.label} failed (exit ${result.code}): ${result.output}` };
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { ok: false, error: 'Task was paused or deleted during combined validation.' };
    }
    return { ok: true, error: '' };
    } finally {
      if (linkedModules) { try { fs.unlinkSync(scratchModules); } catch {} }
      this.executions.end(registration);
    }
  }

  private isMergeStillAllowed(threadId: string, sourceCommit: string): boolean {
    const latest = this.store.getTask(threadId);
    return !!latest && latest.status === 'waiting_merge' && latest.worktree?.approvedCommit === sourceCommit;
  }

  private markNeedsReview(thread: DelegationThread, reason: string): void {
    thread = structuredClone(thread);
    thread.status = 'waiting_review'; thread.worktree!.approvedCommit = undefined;
    thread.worktree!.integration = { phase: 'ready', error: reason, updatedAt: new Date().toISOString() };
    thread.summary.lastOutputSnippet = reason; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
    const timer = setTimeout(() => {
      const latest = this.store.getTask(thread.threadId);
      if (latest?.status === 'waiting_review') this.requestReview?.(latest);
    }, 0);
    timer.unref();
  }

  private finishMergeFailure(threadId: string, phase: 'conflict' | 'validation_failed', error: string, targetOldCommit: string, sourceCommit: string, conflictFiles?: string[], resolutionPath?: string): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    thread.status = resolutionPath ? 'waiting_merge' : 'waiting_review';
    if (resolutionPath) { worktree.mergeResolutionPath = resolutionPath; worktree.mergeResolutionSubmitted = false; worktree.mergeResolutionSourceCommit = sourceCommit; }
    const at = new Date().toISOString();
    if (resolutionPath) {
      const number = (thread.mergeHistory?.length || 0) + 1;
      const safeId = threadId.replace(/[^A-Za-z0-9_-]/g, '_');
      const dir = path.join(thread.artifactDir || path.join(this.storageDir, 'merge-logs', safeId), 'merge_log');
      let logPath: string | undefined;
      try {
        fs.mkdirSync(dir, { recursive: true });
        logPath = path.join(dir, `${safeId}_merge_${number}.log`);
        fs.writeFileSync(logPath, `${phase}: ${error}\n`, 'utf8');
      } catch { logPath = undefined; }
      const code = phase === 'validation_failed' ? (error.match(/exit\s+(-?\d+)/i)?.[1] || 'VALIDATION_FAILED') : 'CONFLICT';
      const budget = noteMergeFailure(thread, phase, code, error, conflictFiles);
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number, at, phase, result: 'failed', ...budget, attemptId: this.attemptLogs.get(threadId)?.id,
        error: error.slice(-5000), errorCode: phase === 'conflict' ? 'CONFLICT' : 'VALIDATION_FAILED', logPath, exitCode: phase === 'conflict' ? 1 : Number(error.match(/exit\s+(-?\d+)/i)?.[1] || -1), targetRef: worktree.deliveryTarget?.ref || worktree.targetRef,
        targetOldCommit, candidateCommit: sourceCommit, candidateTree: this.attemptLogs.get(threadId)?.candidateTree || worktree.approvedSnapshot?.tree, conflictFiles }];
    }
    worktree.integration = { phase, error: error.slice(-5000), targetOldCommit, sourceCommit, conflictFiles, updatedAt: at };
    thread.summary.lastOutputSnippet = `${phase}: ${error.slice(-5000)}`; thread.updatedAt = at; this.store.upsertTask(thread);
    const file = this.transactionPath(threadId);
    this.writeTransaction(file, { threadId, sourceCommit, targetOldCommit, phase, error, conflictFiles, at: new Date().toISOString() });
    const timer = setTimeout(() => {
      const latest = this.store.getTask(threadId);
      if (latest && (latest.status === 'waiting_review' || (latest.status === 'waiting_merge' && latest.worktree?.mergeResolutionPath === resolutionPath))) this.requestReview?.(latest);
    }, 0);
    timer.unref();
  }

  private async finishMerged(threadId: string, commit: string, targetOldCommit?: string, targetPath?: string, validation?: NonNullable<DelegationThread['worktree']>['validation'], mergedCommit = commit): Promise<void> {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    if (thread.mergeControl) thread.mergeControl = { ...thread.mergeControl, revision: thread.mergeControl.revision + 1, failureFingerprint: undefined, consecutiveRetries: 0 };
    if (this.attemptLogs.has(threadId) && targetOldCommit && !(thread.mergeHistory || []).some((entry) => entry.mergedCommit === mergedCommit)) {
      const repo = worktree.deliveryTarget?.repositoryPath || worktree.repositoryPath || worktree.mainPath;
      const changedFiles = (await this.loggedGit(threadId, repo, ['diff', '--name-status', `${targetOldCommit}..${mergedCommit}`])).split(/\r?\n/).filter(Boolean);
      const number = (thread.mergeHistory?.length || 0) + 1;
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number, at: new Date().toISOString(),
        phase: 'merged', result: 'merged', targetRef: worktree.deliveryTarget?.ref || worktree.targetRef, targetOldCommit,
        targetCommit: commit, sourceCommit: worktree.approvedSnapshot?.sourceCommit, candidateCommit: worktree.approvedCommit, candidateTree: this.attemptLogs.get(threadId)?.candidateTree || worktree.approvedSnapshot?.tree, mergedCommit, changedFiles, exitCode: 0, attemptId: this.attemptLogs.get(threadId)?.id }];
    }
    thread.status = 'completed'; worktree.integration = { ...worktree.integration, phase: 'merged', targetOldCommit, targetCommit: commit, targetPath, mergedCommit, updatedAt: new Date().toISOString() };
    if (worktree.deliveryTarget) worktree.deliveryTarget.checkoutPath = targetPath;
    if (validation) worktree.validation = validation;
    worktree.mergePending = false; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
  }

  /** True when the target ref contains a candidate whose ledger write is pending. */
  public async hasLandedCandidate(threadId: string): Promise<boolean> {
    const tx = this.readTransaction(this.transactionPath(threadId));
    const thread = this.store.getTask(threadId);
    if (!tx?.candidateCommit || !thread?.worktree?.targetRef || !thread.worktree.repositoryPath) return false;
    try {
      const run = (args: string[]) => this.attemptLogs.has(threadId)
        ? this.loggedGit(threadId, thread.worktree!.repositoryPath!, args)
        : Promise.resolve(gitOutput(thread.worktree!.repositoryPath!, args));
      const head = await run(['rev-parse', '--verify', `${thread.worktree.targetRef}^{commit}`]);
      if (head === tx.candidateCommit) return true;
      try { await run(['merge-base', '--is-ancestor', tx.candidateCommit, head]); return true; } catch { return false; }
    } catch { return false; }
  }

  private transactionPath(threadId: string): string { return path.join(this.storageDir, 'merge-transactions', `${threadId}.json`); }
  private readTransaction(file: string): any { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } }
  private writeTransaction(file: string, value: unknown): void {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    const fd = fs.openSync(tmp, 'w');
    try { fs.writeFileSync(fd, JSON.stringify(value, null, 2)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    fs.renameSync(tmp, file);
    if (process.platform !== 'win32') { const dirFd = fs.openSync(path.dirname(file), 'r'); try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); } }
  }
}
