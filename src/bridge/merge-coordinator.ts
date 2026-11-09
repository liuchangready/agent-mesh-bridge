import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import { TaskRoleLock } from './task-role-lock';
import { TaskStore } from './task-store';
import { DelegationThread } from './types';
import { changedFiles, currentHead, gitOutput } from './git-worktree';
import { TaskExecutionRegistry } from './task-execution-registry';
import { killProcessTree } from './platform';
import { taskValidationDir } from './round-log';

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

export class MergeCoordinator {
  private readonly executions: TaskExecutionRegistry;

  constructor(
    private readonly store: TaskStore,
    private readonly storageDir: string,
    private readonly lock = new MeshFileLock(),
    private readonly requestReview?: (thread: DelegationThread) => void,
  ) {
    this.executions = new TaskExecutionRegistry(storageDir);
  }

  /** Approval is tied to the exact clean task commit, then serialized with target read through durable ledger write. */
  async mergeApproved(threadId: string): Promise<MergeResult> {
    const thread = this.store.getTask(threadId);
    if (!thread?.worktree?.targetRef || !thread.worktree.approvedCommit) throw new Error(`Task ${threadId} has no approved isolated worktree commit.`);
    if (thread.status !== 'waiting_merge') throw new Error(`Task ${threadId} is not waiting to merge.`);
    const worktree = thread.worktree;
    const repoPath = worktree.deliveryTarget?.repositoryPath || worktree.repositoryPath || worktree.mainPath;
    const targetRef = worktree.deliveryTarget?.ref || worktree.targetRef!;
    if (!fs.existsSync(worktree.path)) {
      try {
        const moved = gitOutput(repoPath, ['worktree', 'list', '--porcelain']).split(/\r?\n\r?\n/)
          .map((value) => ({ value, branch: value.match(/^branch (.+)$/m)?.[1], worktree: value.match(/^worktree (.+)$/m)?.[1] }))
          .find((item) => item.branch === `refs/heads/${worktree.taskBranch}` && item.worktree && fs.existsSync(item.worktree));
        if (moved?.worktree) {
          const updated = structuredClone(thread); updated.worktree!.path = moved.worktree; updated.workspacePath = moved.worktree; this.store.upsertTask(updated);
          worktree.path = moved.worktree;
        } else {
          const reason = `Task worktree ${worktree.path} is missing and no registered worktree for ${worktree.taskBranch} was found.`;
          this.finishBlocked(threadId, reason, '', worktree.approvedCommit!); return { status: 'blocked', message: reason };
        }
      } catch (error) {
        const reason = `Task worktree ${worktree.path} is missing or moved and could not be located: ${String(error)}`;
        this.finishBlocked(threadId, reason, '', worktree.approvedCommit!); return { status: 'blocked', message: reason };
      }
    }
    const sourceCommit = worktree.approvedCommit!;
    await this.waitForReviewerExit(thread, sourceCommit);
    if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
    const current = currentHead(worktree.path);
    if (current !== sourceCommit || changedFiles(worktree.path).length) {
      this.markNeedsReview(thread, `Approved commit ${sourceCommit} is stale: current HEAD is ${current} and/or the worktree has uncommitted changes.`);
      return { status: 'stale', message: 'The approved task worktree changed; reviewer must inspect it again.' };
    }

    const release = await this.lock.acquire(undefined, () => !this.isMergeStillAllowed(threadId, sourceCommit));
    if (!release) return { status: 'stale', message: 'Task was paused or deleted while waiting for local integration.' };
    try {
      // This is intentionally the first target read in the critical section.
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
      const lockedHead = currentHead(worktree.path);
      if (lockedHead !== sourceCommit || changedFiles(worktree.path).length) {
        this.markNeedsReview(thread, `Approved commit ${sourceCommit} changed while waiting for integration (HEAD ${lockedHead}, dirty worktree present).`);
        return { status: 'stale', message: 'The approved task worktree changed while waiting; reviewer must inspect the new state.' };
      }
      const targetOldCommit = gitOutput(repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
      const txPath = this.transactionPath(threadId);
      const prior = this.readTransaction(txPath);
      const recovered = prior?.candidateCommit && prior.sourceCommit === sourceCommit &&
        (prior.phase === 'validated' || prior.phase === 'ref_updated' || prior.phase === 'ledger_written') &&
        (prior.candidateCommit === targetOldCommit || this.isAncestor(repoPath, prior.candidateCommit, targetOldCommit));
      if (recovered) {
        const sync = this.syncCheckedOutTarget(repoPath, targetRef, prior.candidateCommit!, false, prior.targetOldCommit);
        if (!sync.ok) { this.finishBlocked(threadId, sync.error, prior.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
        const validationStatus = prior.validationPath ? this.cleanupValidationWorktree(repoPath, threadId, prior.validationPath) : undefined;
        this.finishMerged(threadId, sync.commit || prior.candidateCommit!, prior.targetOldCommit, sync.path,
          prior.validationPath ? this.validationRecord(prior.validationPath, prior.validationCommit, prior.validationResult, validationStatus) : undefined);
        prior.phase = 'ledger_written'; this.writeTransaction(txPath, prior);
        return { status: 'merged', message: 'Recovered completed merge transaction.', commit: sync.commit || prior.candidateCommit };
      }
      if (prior?.validationPath) this.cleanupValidationWorktree(repoPath, threadId, prior.validationPath);
      const tx = { threadId, sourceCommit, targetOldCommit, phase: 'preparing', at: new Date().toISOString() } as any;
      this.writeTransaction(txPath, tx);
      const scratchRoot = thread.artifactDir
        ? taskValidationDir(thread.artifactDir)
        : path.join(this.storageDir, 'merge-worktrees');
      const scratch = path.join(scratchRoot, `${threadId}-${crypto.randomUUID()}`);
      fs.mkdirSync(scratchRoot, { recursive: true });
      tx.validationPath = scratch; this.writeTransaction(txPath, tx);
      let added = false;
      let validationCommit: string | undefined;
      let validationResult: 'passed' | 'failed' | 'cancelled' | undefined;
      try {
        gitOutput(repoPath, ['worktree', 'add', '--detach', scratch, targetOldCommit]); added = true;
        try { gitOutput(scratch, ['merge', '--no-commit', '--no-ff', sourceCommit]); }
        catch (error) {
          const conflicts = gitOutput(scratch, ['diff', '--name-only', '--diff-filter=U']).split(/\r?\n/).filter(Boolean);
          const message = `Merge conflict against ${targetOldCommit}: ${conflicts.join(', ') || String(error)}`;
          this.finishMergeFailure(threadId, 'conflict', message, targetOldCommit, sourceCommit, conflicts);
          return { status: 'conflict', message };
        }
        const validation = await this.validate(scratch, repoPath, threadId, sourceCommit);
        validationResult = validation.ok ? 'passed' : 'failed';
        tx.validationResult = validationResult; this.writeTransaction(txPath, tx);
        if (!validation.ok) {
          if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted during combined validation.' }; }
          this.finishMergeFailure(threadId, 'validation_failed', validation.error, targetOldCommit, sourceCommit);
          return { status: 'validation_failed', message: validation.error };
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted during combined validation.' }; }
        const validatedHead = currentHead(worktree.path);
        if (validatedHead !== sourceCommit || changedFiles(worktree.path).length) {
          this.markNeedsReview(thread, `Approved commit ${sourceCommit} changed during combined validation (HEAD ${validatedHead}, dirty worktree present).`);
          return { status: 'stale', message: 'The approved task worktree changed during validation; reviewer must inspect the new state.' };
        }
        const validationTree = gitOutput(scratch, ['write-tree']);
        const candidateCommit = gitOutput(scratch, ['-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit-tree', validationTree, '-p', targetOldCommit, '-p', sourceCommit, '-m', `Merge ${threadId}`]);
        gitOutput(scratch, ['reset', '--hard', candidateCommit]);
        if (gitOutput(scratch, ['rev-parse', `${candidateCommit}^{tree}`]) !== validationTree || gitOutput(scratch, ['write-tree']) !== validationTree) {
          throw new Error('The recorded validation commit does not match the combined tree that was validated.');
        }
        validationCommit = candidateCommit;
        tx.validationCommit = validationCommit;
        tx.phase = 'validated'; tx.candidateCommit = candidateCommit; this.writeTransaction(txPath, tx);
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted before the validated integration could be written.' }; }
        const beforeLanding = this.syncCheckedOutTarget(repoPath, targetRef, candidateCommit, true, targetOldCommit);
        if (!beforeLanding.ok) {
          this.finishBlocked(threadId, beforeLanding.error, targetOldCommit, sourceCommit, beforeLanding.path);
          return { status: 'blocked', message: beforeLanding.error };
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted before the target ref update.' }; }
        // update-ref's old-value argument prevents overwriting a ref changed outside this lock.
        gitOutput(repoPath, ['update-ref', targetRef, candidateCommit, targetOldCommit]);
        tx.phase = 'ref_updated'; this.writeTransaction(txPath, tx);
        const synced = this.syncCheckedOutTarget(repoPath, targetRef, candidateCommit, false, targetOldCommit);
        if (!synced.ok) { this.finishBlocked(threadId, synced.error, targetOldCommit, sourceCommit, synced.path); return { status: 'blocked', message: synced.error }; }
        const validationStatus = this.removeValidationWorktree(repoPath, scratch) ? 'cleaned' : 'present'; added = validationStatus === 'present';
        this.finishMerged(threadId, synced.commit || candidateCommit, targetOldCommit, synced.path, this.validationRecord(scratch, validationCommit, validationResult, validationStatus));
        tx.phase = 'ledger_written'; this.writeTransaction(txPath, tx);
      return { status: 'merged', message: 'Merged and validated.', commit: synced.commit || candidateCommit };
      } finally {
        if (added) {
          const validationStatus = this.removeValidationWorktree(repoPath, scratch) ? 'cleaned' : 'present';
          this.recordValidation(threadId, scratch, validationCommit, validationStatus, validationResult);
        }
      }
    } catch (error) {
      // If update-ref succeeded but a later transaction/ledger write failed,
      // retain the landed fact and finish the same transaction before reporting.
      if (this.hasLandedCandidate(threadId)) {
        const tx = this.readTransaction(this.transactionPath(threadId));
        if (tx?.candidateCommit) {
          const sync = this.syncCheckedOutTarget(repoPath, targetRef, tx.candidateCommit, false, tx.targetOldCommit);
          if (!sync.ok) { this.finishBlocked(threadId, sync.error, tx.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
          const validationStatus = tx.validationPath ? this.cleanupValidationWorktree(repoPath, threadId, tx.validationPath) : undefined;
          this.finishMerged(threadId, sync.commit || tx.candidateCommit, tx.targetOldCommit, sync.path,
            tx.validationPath ? this.validationRecord(tx.validationPath, tx.validationCommit, tx.validationResult, validationStatus) : undefined);
          tx.phase = 'ledger_written';
          this.writeTransaction(this.transactionPath(threadId), tx);
          return { status: 'merged', message: 'Recovered target update after a durable ledger write failure.', commit: sync.commit || tx.candidateCommit };
        }
      }
      const latest = this.store.getTask(threadId);
      if (latest?.status === 'waiting_merge' && latest.worktree?.deliveryTarget) {
        const reason = `Could not safely deliver to ${targetRef}: ${String(error)}`;
        this.finishBlocked(threadId, reason, latest.worktree?.integration?.targetOldCommit || '', sourceCommit);
        return { status: 'blocked', message: reason };
      }
      throw error;
    } finally { release(); }
  }

  private syncCheckedOutTarget(repoPath: string, targetRef: string, commit: string, preflight = false, previousCommit?: string): { ok: boolean; error: string; path?: string; commit?: string } {
    const targetCommit = gitOutput(repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
    const rows = gitOutput(repoPath, ['worktree', 'list', '--porcelain']).split(/\r?\n\r?\n/);
    const row = rows.map((value) => ({ value, branch: value.match(/^branch (.+)$/m)?.[1], worktree: value.match(/^worktree (.+)$/m)?.[1] }))
      .find((item) => item.branch === targetRef);
    if (preflight) {
      if (!row?.worktree) return { ok: true, error: '', commit: targetCommit };
      if (!fs.existsSync(row.worktree)) return { ok: false, error: `Target branch ${targetRef} is registered at missing worktree ${row.worktree}; restore or remove that worktree registration, then retry merge.` };
      const dirty = gitOutput(row.worktree, ['status', '--porcelain', '--untracked-files=all']);
      if (targetCommit !== previousCommit || currentHead(row.worktree) !== targetCommit || dirty) return { ok: false, path: row.worktree, error: `Target branch ${targetRef} changed or has uncommitted files in ${row.worktree}. Preserve or commit those changes, then retry merge; the bridge left them untouched.` };
      return { ok: true, error: '', path: row.worktree, commit: targetCommit };
    }
    if (targetCommit !== commit && !this.isAncestor(repoPath, commit, targetCommit)) return { ok: false, path: row?.worktree, error: `Target branch ${targetRef} moved to unrelated commit ${targetCommit}; the bridge did not report this task as delivered.` };
    if (!row?.worktree) return { ok: true, error: '', commit: targetCommit };
    if (!fs.existsSync(row.worktree)) return { ok: false, error: `Target branch ${targetRef} is registered at missing worktree ${row.worktree}; restore or remove that worktree registration, then retry merge.` };
    const dirty = gitOutput(row.worktree, ['status', '--porcelain', '--untracked-files=all']);
    if (currentHead(row.worktree) === targetCommit && !dirty) return { ok: true, error: '', path: row.worktree, commit: targetCommit };
    const stagedTree = previousCommit && gitOutput(row.worktree, ['write-tree']);
    const indexIsPriorTarget = !!previousCommit && stagedTree === gitOutput(repoPath, ['rev-parse', `${previousCommit}^{tree}`]);
    const unstagedOrUntracked = dirty.split(/\r?\n/).filter(Boolean).some((line) => line.startsWith('??') || line[1] !== ' ');
    if (!previousCommit || !indexIsPriorTarget || unstagedOrUntracked) return { ok: false, path: row.worktree, error: `Target checkout ${row.worktree} no longer matches the pre-merge target; preserve or inspect its files, then retry. The bridge did not overwrite local changes.` };
    try {
      gitOutput(row.worktree, ['read-tree', '-u', '-m', previousCommit, targetCommit]);
      const latestTarget = gitOutput(repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
      if (latestTarget !== targetCommit || currentHead(row.worktree) !== latestTarget || gitOutput(row.worktree, ['status', '--porcelain', '--untracked-files=all'])) {
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

  private cleanupValidationWorktree(repoPath: string, threadId: string, validationPath: string): 'cleaned' | 'present' {
    const candidate = path.resolve(validationPath);
    const allowedRoots = [path.resolve(this.storageDir, 'merge-worktrees')];
    const thread = this.store.getTask(threadId);
    if (thread?.artifactDir) allowedRoots.push(path.resolve(taskValidationDir(thread.artifactDir)));
    const allowed = allowedRoots.some((root) => candidate.startsWith(`${root}${path.sep}`));
    if (!allowed || !path.basename(candidate).startsWith(`${threadId}-`)) return 'present';
    const status = this.removeValidationWorktree(repoPath, candidate) ? 'cleaned' : 'present';
    this.recordValidation(threadId, candidate, undefined, status);
    return status;
  }

  private removeValidationWorktree(repoPath: string, validationPath: string): boolean {
    try {
      if (fs.existsSync(validationPath)) gitOutput(repoPath, ['worktree', 'remove', '--force', validationPath]);
      const registered = gitOutput(repoPath, ['worktree', 'list', '--porcelain']).split(/\r?\n/).some((line) => line === `worktree ${validationPath}`);
      return !fs.existsSync(validationPath) && !registered;
    } catch { return false; }
  }

  private validationRecord(validationPath: string, commit: string | undefined, result?: 'passed' | 'failed' | 'cancelled', status: 'cleaned' | 'present' = 'present'): NonNullable<DelegationThread['worktree']>['validation'] {
    return { path: validationPath, commit, status, result, updatedAt: new Date().toISOString() };
  }

  private finishBlocked(threadId: string, reason: string, targetOldCommit: string, sourceCommit: string, targetPath?: string): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    thread.status = 'waiting_merge';
    thread.worktree!.integration = { ...thread.worktree!.integration, phase: 'blocked', error: reason, targetOldCommit, targetPath, sourceCommit, updatedAt: new Date().toISOString() };
    thread.summary.lastOutputSnippet = reason; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
  }

  private isAncestor(cwd: string, ancestor: string, commit: string): boolean {
    try { gitOutput(cwd, ['merge-base', '--is-ancestor', ancestor, commit]); return true; }
    catch { return false; }
  }

  private async waitForReviewerExit(thread: DelegationThread, sourceCommit: string): Promise<void> {
    const roles = new TaskRoleLock(this.storageDir);
    while (true) {
      if (!this.isMergeStillAllowed(thread.threadId, sourceCommit)) return;
      const owner = roles.getOwner(thread.threadId);
      if (!owner || owner.role !== 'reviewer') return;
      try { process.kill(owner.childPid || owner.pid, 0); } catch { return; }
      await sleep(250);
    }
  }

  private async validate(cwd: string, repositoryPath: string, threadId: string, sourceCommit: string): Promise<{ ok: boolean; error: string }> {
    let pkg: any;
    try { pkg = JSON.parse(fs.readFileSync(path.join(cwd, 'package.json'), 'utf8')); }
    catch { return { ok: false, error: 'No supported validation strategy: package.json is missing or invalid.' }; }
    const scripts = pkg.scripts || {};
    const commands: string[][] = [];
    if (scripts.compile) commands.push(['run', 'compile']);
    if (scripts.test) commands.push(['test']);
    if (!scripts.compile || !scripts.test) return { ok: false, error: 'Validation strategy must provide both package scripts "compile" and "test".' };
    const registration = this.executions.begin(threadId, this.store.getTask(threadId)?.currentRound || 0, 'merge-validation', cwd);
    const sourceModules = path.join(repositoryPath, 'node_modules');
    const scratchModules = path.join(cwd, 'node_modules');
    let linkedModules = false;
    try {
    if (fs.existsSync(sourceModules) && !fs.existsSync(scratchModules)) {
      try { fs.symlinkSync(sourceModules, scratchModules, process.platform === 'win32' ? 'junction' : 'dir'); linkedModules = true; }
      catch (error: any) { return { ok: false, error: `Could not expose the installed project dependencies to the temporary validation worktree: ${error?.message || error}` }; }
    }
    for (const args of commands) {
      const scriptName = args[0] === 'test' ? 'test' : args[1];
      const packageFile = JSON.parse(fs.readFileSync(path.join(cwd, 'package.json'), 'utf8'));
      const script = packageFile.scripts[scriptName];
      const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === 'path') || 'PATH';
      const binPath = path.join(repositoryPath, 'node_modules', '.bin');
      const env = { ...process.env, [pathKey]: `${binPath}${path.delimiter}${process.env[pathKey] || ''}` };
      const child = spawn(script, { cwd, env, windowsHide: true, shell: true });
      this.executions.attachChildPid(child.pid, registration);
      let output = '';
      let trackingError: Error | undefined;
      let resolveResult!: (result: { code: number | null; output: string }) => void;
      const resultPromise = new Promise<{ code: number | null; output: string }>((resolve) => { resolveResult = resolve; });
      child.stdout.on('data', (chunk) => { output += chunk.toString(); });
      child.stderr.on('data', (chunk) => { output += chunk.toString(); });
      const timer = setTimeout(() => {
        child.kill('SIGTERM');
        setTimeout(() => child.kill('SIGKILL'), 5_000).unref();
      }, 20 * 60 * 1000);
      child.on('close', (code) => { clearTimeout(timer); this.lock.detachChildPid(child.pid); resolveResult({ code: trackingError ? -1 : code, output: trackingError?.message || output }); });
      child.on('error', (error) => { clearTimeout(timer); this.lock.detachChildPid(child.pid); resolveResult({ code: -1, output: error.message }); });
      try { await this.lock.attachChildPid(child.pid, () => !this.isMergeStillAllowed(threadId, sourceCommit)); }
      catch (error) { trackingError = error as Error; killProcessTree(child); }
      const result = await resultPromise;
      if (result.code !== 0) return { ok: false, error: `npm ${args.join(' ')} failed (exit ${result.code}): ${result.output.slice(-5000)}` };
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

  private finishMergeFailure(threadId: string, phase: 'conflict' | 'validation_failed', error: string, targetOldCommit: string, sourceCommit: string, conflictFiles?: string[]): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    thread.status = 'waiting_review'; worktree.approvedCommit = undefined;
    worktree.integration = { phase, error, targetOldCommit, sourceCommit, conflictFiles, updatedAt: new Date().toISOString() };
    thread.summary.lastOutputSnippet = `${phase}: ${error}`; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
    const file = this.transactionPath(threadId);
    this.writeTransaction(file, { threadId, sourceCommit, targetOldCommit, phase, error, conflictFiles, at: new Date().toISOString() });
    const timer = setTimeout(() => {
      const latest = this.store.getTask(threadId);
      if (latest?.status === 'waiting_review') this.requestReview?.(latest);
    }, 0);
    timer.unref();
  }

  private finishMerged(threadId: string, commit: string, targetOldCommit?: string, targetPath?: string, validation?: NonNullable<DelegationThread['worktree']>['validation']): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    thread.status = 'completed'; worktree.integration = { ...worktree.integration, phase: 'merged', targetOldCommit, targetCommit: commit, targetPath, mergedCommit: commit, updatedAt: new Date().toISOString() };
    if (worktree.deliveryTarget) worktree.deliveryTarget.checkoutPath = targetPath;
    if (validation) worktree.validation = validation;
    worktree.mergePending = false; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
  }

  /** True when the target ref contains a candidate whose ledger write is pending. */
  public hasLandedCandidate(threadId: string): boolean {
    const tx = this.readTransaction(this.transactionPath(threadId));
    const thread = this.store.getTask(threadId);
    if (!tx?.candidateCommit || !thread?.worktree?.targetRef || !thread.worktree.repositoryPath) return false;
    try {
      const head = gitOutput(thread.worktree.repositoryPath, ['rev-parse', '--verify', `${thread.worktree.targetRef}^{commit}`]);
      return head === tx.candidateCommit || this.isAncestor(thread.worktree.repositoryPath, tx.candidateCommit, head);
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
