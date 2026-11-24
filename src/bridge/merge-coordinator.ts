import { spawnWindowsJob, startWindowsJob, windowsJobOf } from './windows-job';
import { taskArtifactBase } from './round-log';
import { requireDeliveryTarget } from './types';
import { assertDedicatedTask } from './types';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import { TaskRoleLock } from './task-role-lock';
import { TaskStore } from './task-store';
import { DelegationThread, isMergeReview } from './types';
import { gitOutput, gitExecutable } from './git-worktree';
import { TaskExecutionRegistry } from './task-execution-registry';
import { getProcessBirthId, killProcessTree } from './platform';
import { taskValidationDir } from './round-log';
import { resolveNodeCommand } from './deepseek-harness';
import { createMergeLog, noteMergeFailure } from './merge-recovery';

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const CHILD_ACK_TIMEOUT_MS = 10_000;

function childHasExited(child: ReturnType<typeof spawn>): boolean {
  // On Windows the open process handle prevents PID reuse until libuv
  // closes it and reports exit in the same event-loop callback.
  return child.exitCode !== null || child.signalCode !== null;
}

export { buildPosixLockHelperCommand, MeshFileLock } from './mesh-file-lock';
import { MeshFileLock, meshMergeLockPath } from './mesh-file-lock';

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
  private readonly lock: MeshFileLock;
  private readonly attemptLogs = new Map<string, { id: string; number: number; path: string; candidateTree?: string }>();
  private activeChildPidCallback?: (pid: number, childProcess?: ReturnType<typeof spawn>, childBirthId?: string) => void | Promise<void>;

  private spawnJobIfV2(threadId: string, command: string, args: string[], cwd: string, env?: NodeJS.ProcessEnv, shell = false) {
    const task = this.store.getTask(threadId);
    if (process.platform !== 'win32' || !this.activeChildPidCallback || task?.execution?.version !== 2 || !task.artifactDir) return;
    return spawnWindowsJob(command, args, { cwd, env, shell, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] },
      path.join(task.artifactDir, 'worker_log'), getProcessBirthId(process.pid) ?? '');
  }

  private spawnMergeChild(threadId: string, command: string, args: string[], cwd: string, env?: NodeJS.ProcessEnv, shell = false) {
    if (!this.activeChildPidCallback) {
      return { child: spawn(command, args, { cwd, env, shell, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] }) };
    }
    const jobChild = this.spawnJobIfV2(threadId, command, args, cwd, env, shell);
    if (jobChild) {
      return { child: jobChild, start: () => { startWindowsJob(jobChild); jobChild.stdin!.end(); } };
    }
    // Keep the real command behind the run binding and the OS lock helper ACK.
    // EOF without the start byte means the owner exited before admission.
    const script = `
      const { spawn } = require('node:child_process');
      process.stdin.once('data', data => {
        process.stdin.destroy();
        if (data[0] !== 49) return;
        const { command, args, shell } = JSON.parse(process.argv[1]);
        const child = spawn(command, args, { shell, windowsHide: true, stdio: ['ignore', 'inherit', 'inherit'] });
        child.on('error', error => console.error(error.message));
        child.on('close', code => { process.exitCode = code ?? 1; });
      });
    `;
    const node = process.versions.electron ? resolveNodeCommand() : process.execPath;
    const child = spawn(node, ['-e', script, JSON.stringify({ command, args, shell })],
      { cwd, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    child.stdin!.on('error', (error) => child.emit('error', error));
    return { child, start: () => child.stdin!.end('1') };
  }

  constructor(
    private readonly store: TaskStore,
    private readonly storageDir: string,
    lock?: MeshFileLock,
    private readonly requestReview?: (thread: DelegationThread) => void,
  ) {
    this.lock = lock || new MeshFileLock(meshMergeLockPath(storageDir));
    this.executions = new TaskExecutionRegistry(storageDir);
  }

  public getLock(): MeshFileLock {
    return this.lock;
  }

  private async loggedGit(threadId: string, cwd: string, args: string[], raw = false): Promise<string> {
    const thread = this.store.getTask(threadId);
    if (!thread?.worktree?.approvedCommit || !this.isMergeStillAllowed(threadId, thread.worktree.approvedCommit)) throw new Error('Merge task was paused or deleted before Git could start.');
    const logPath = this.attemptLogs.get(threadId)?.path;
    if (logPath) fs.appendFileSync(logPath, `\n$ git ${args.join(' ')}\n`, 'utf8');
    const registration = this.executions.begin(threadId, thread?.currentRound || 0, 'merge-git', cwd);
    let child: ReturnType<typeof spawn>;
    let start: (() => void) | undefined;
    try { ({ child, start } = this.spawnMergeChild(threadId, gitExecutable(), args, cwd)); }
    catch (error) { this.executions.end(registration); throw error; }
    const windows = process.platform === 'win32';
    let childBirthId = !windows && child.pid ? getProcessBirthId(child.pid) : undefined;
    let stdout = ''; let stderr = ''; let trackingError: Error | undefined;
    try { this.executions.attachChildPid(child.pid, registration); }
    catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
    const record = (chunk: Buffer, isStdout: boolean) => {
      if (isStdout) stdout += chunk.toString();
      else stderr = (stderr + chunk.toString()).slice(-16_000);
      if (logPath) try { fs.appendFileSync(logPath, chunk); } catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
    };
    child.stdout!.on('data', (chunk: Buffer) => record(chunk, true));
    child.stderr!.on('data', (chunk: Buffer) => record(chunk, false));
    let resolveResult!: (result: { code: number | null; error?: Error }) => void;
    const completion = new Promise<{ code: number | null; error?: Error }>((resolve) => { resolveResult = resolve; });
    // An `error` event reports a failed spawn/pipe, not that the process is gone:
    // resolving on it would end the registration and detach the lock while a
    // spawned tree is still running. Record it and settle on `close` only.
    let childError: Error | undefined;
    child.once('error', (error) => { childError = error; });
    child.once('close', (code) => resolveResult({ code, error: childError }));
    const timer = setTimeout(() => killProcessTree(child, childBirthId, true), 30_000);
    timer.unref?.();
    try {
      const cancelled = () => !this.isMergeStillAllowed(threadId, thread.worktree!.approvedCommit!);
      if (windows && !trackingError) {
        try {
          const birthId = await this.lock.attachChildPid(child.pid, cancelled);
          if (child.pid && !childHasExited(child)) {
            if (typeof birthId === 'string') childBirthId = birthId;
            else {
              // The OS may observe exit before Node delivers the close event.
              const closed = await Promise.race([completion.then(() => true), sleep(100).then(() => false)]);
              if (!closed) throw new Error('Mesh lock helper could not identify a Git child that had not closed.');
            }
          }
        } catch (error) {
          trackingError = error as Error;
          // Attachment failed before it could supply an identity. Query only for
          // stopping a live child, and only accept an identity from this launch.
          if (child.pid && !childHasExited(child)) {
            const birthId = getProcessBirthId(child.pid);
            if (birthId) killProcessTree(child, birthId, true);
          }
        }
      }
      // The child is already spawned: a rejected identity binding must stop this
      // subtree and be surfaced, never skip straight to the cleanup in `finally`.
      try { if (child.pid && !trackingError && (!windows || (childBirthId && !childHasExited(child)))) await this.activeChildPidCallback?.(child.pid, child, childBirthId); }
      catch (error: any) {
        trackingError = error as Error;
        if (error?.code !== 'CHILD_BINDING_IN_PROGRESS') killProcessTree(child, childBirthId, true);
      }
      try { if (!windows && !trackingError) await this.lock.attachChildPid(child.pid, cancelled); }
      catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
      if (start && !trackingError) start();
      else if (start) child.stdin!.end();
      const result = await completion;
      if (logPath) fs.appendFileSync(logPath, `\n[exit ${result.code}]\n`, 'utf8');
      if (trackingError || result.error || result.code !== 0) throw new Error(`Git command failed (${args.join(' ')}): ${trackingError?.message || result.error?.message || stderr.trim() || `exit ${result.code}`}`);
      return raw ? stdout : stdout.trim();
    } finally {
      clearTimeout(timer); this.lock.detachChildPid(child.pid); this.executions.end(registration);
    }
  }

  /** Approval is tied to the exact clean task commit, then serialized with target read through durable ledger write. */
  async mergeApproved(threadId: string): Promise<MergeResult> {
    for (;;) {
      const result = await this.mergeApprovedOnce(threadId);
      if (!('retryTarget' in result)) return result;
    }
  }

  async mergeApprovedAttempt(
    threadId: string,
    onChildPid?: (pid: number, childProcess?: ReturnType<typeof spawn>, childBirthId?: string) => void | Promise<void>,
    externalLease?: () => void,
  ): Promise<MergeResult> {
    this.activeChildPidCallback = onChildPid;
    try {
      const result = await this.mergeApprovedOnce(threadId, externalLease);
      if ('retryTarget' in result) {
        return { status: 'stale', message: result.message };
      }
      return result;
    } finally {
      this.activeChildPidCallback = undefined;
    }
  }

  private async mergeApprovedOnce(
    threadId: string,
    externalLease?: () => void,
  ): Promise<MergeResult | (MergeResult & { retryTarget: true })> {
    let thread = this.store.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread?.worktree?.targetRef || !thread.worktree.approvedCommit) throw new Error(`Task ${threadId} has no approved isolated worktree commit.`);
    requireDeliveryTarget(thread.worktree);
    if (isMergeReview(thread)) {
      thread = structuredClone(thread);
      thread.status = 'queued'; thread.queuePhase = 'merge';
      this.store.upsertTask(thread);
    }
    if (!(thread.status === 'queued' && thread.queuePhase === 'merge') && thread.status !== 'merging') throw new Error(`Task ${threadId} is not waiting to merge.`);
    const worktree = thread.worktree!;
    const repoPath = requireDeliveryTarget(worktree).repositoryPath;
    const targetRef = requireDeliveryTarget(worktree).ref;
    const sourceCommit = worktree.approvedCommit!;
    const interruptionCount = thread.interruptions?.length || 0;
    const stillCurrent = () => this.isMergeStillAllowed(threadId, sourceCommit) &&
      (this.store.getTask(threadId)?.interruptions?.length || 0) === interruptionCount;
    // A v2 caller holds the unique durable run through this merge. Reviewer
    // exit was already proven before the merge run was admitted, so do not
    // consult or create a legacy task-role lease on this production path.
    if (!externalLease) await this.waitForReviewerExit(thread, stillCurrent);
    if (!stillCurrent()) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
    const mergeRole = externalLease ? undefined : new TaskRoleLock(this.storageDir);
    let roleLease = mergeRole?.tryAcquire(threadId, 'reviewer', thread.reviewFlow === 'dedicated' ? thread.reviewerSessionId : undefined) || null;
    while (mergeRole && !roleLease && stillCurrent()) {
      await sleep(250);
      if (stillCurrent()) roleLease = mergeRole.tryAcquire(threadId, 'reviewer', thread.reviewFlow === 'dedicated' ? thread.reviewerSessionId : undefined);
    }
    if (mergeRole && !roleLease) return { status: 'stale', message: 'Task was paused or deleted while waiting for its role.' };
    if (!stillCurrent()) { if (roleLease) mergeRole?.release(roleLease); return { status: 'stale', message: 'Task was paused before acquiring its role.' }; }
    let release: (() => void) | undefined;
    if (externalLease) {
      release = () => {};
    } else {
      try { release = await this.lock.acquire(undefined, () => !this.isMergeStillAllowed(threadId, sourceCommit)); }
      catch (error) { if (roleLease) mergeRole?.release(roleLease); throw error; }
    }
    if (!release) { if (roleLease) mergeRole?.release(roleLease); return { status: 'stale', message: 'Task was paused or deleted while waiting for local integration.' }; }
    let startedTargetCommit = '';
    try {
      // This is intentionally the first target read in the critical section.
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted before integration started.' };
      {
        const latest = structuredClone(this.store.getTask(threadId)!);
        const number = (latest.mergeHistory?.length || 0) + 1;
        const safeId = threadId.replace(/[^A-Za-z0-9_-]/g, '_');
        const dir = path.join(taskArtifactBase(latest.threadId, this.storageDir, latest.artifactDir), 'merge_log');
        const { logPath, logFileNumber } = createMergeLog(dir, safeId, number, `Merge attempt ${number} for ${threadId}\n`);
        const id = crypto.randomUUID();
        this.attemptLogs.set(threadId, { id, number, path: logPath, candidateTree: worktree.approvedSnapshot?.tree });
        const updated = structuredClone(latest);
        updated.status = 'merging';
        updated.queuePhase = undefined;
        updated.updatedAt = new Date().toISOString();
        updated.mergeHistory = [...(updated.mergeHistory || []), { id, number, at: new Date().toISOString(), phase: 'attempt', result: 'started',
          logPath, logFileNumber, targetRef, sourceCommit: worktree.approvedSnapshot?.sourceCommit, candidateCommit: sourceCommit, candidateTree: worktree.approvedSnapshot?.tree,
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
      if (prior?.phase === 'resolution_rebase_prepared' && prior.sourceCommit === sourceCommit && worktree.mergeResolutionSubmitted &&
          prior.validationPath && prior.validationPath !== worktree.mergeResolutionPath) {
        const candidate = path.resolve(prior.validationPath);
        const root = path.resolve(thread.artifactDir ? taskValidationDir(thread.artifactDir) : path.join(this.storageDir, 'merge-worktrees'));
        const samePath = (a: string, b: string) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
        const registered = (await this.loggedGit(threadId, repoPath, ['worktree', 'list', '--porcelain'])).split(/\r?\n/)
          .some((line) => line.startsWith('worktree ') && samePath(path.resolve(line.slice(9)), candidate));
        if (samePath(path.dirname(candidate), root) && path.basename(candidate).startsWith(`${threadId}-`) && registered && fs.existsSync(candidate) &&
            await this.loggedGit(threadId, candidate, ['rev-parse', '--verify', 'HEAD^{commit}']) === prior.targetOldCommit &&
            !(await this.loggedGit(threadId, candidate, ['status', '--porcelain', '--untracked-files=all']))) {
          const latest = this.store.getTask(threadId)!; const updated = structuredClone(latest);
          if (updated.worktree!.mergeResolutionPath) updated.worktree!.supersededMergeResolutionPaths = [...new Set([...(updated.worktree!.supersededMergeResolutionPaths || []), updated.worktree!.mergeResolutionPath])];
          updated.worktree!.mergeResolutionPath = candidate;
          this.store.upsertTask(updated);
          worktree.mergeResolutionPath = candidate; worktree.supersededMergeResolutionPaths = updated.worktree!.supersededMergeResolutionPaths;
        }
      }
      let recovered = false;
      if (prior?.candidateCommit && prior.sourceCommit === sourceCommit &&
          ['validated', 'stash_creating', 'stash_created', 'ref_updated', 'checkout_synced', 'stash_restoring', 'stash_applied', 'stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery', 'stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(prior.phase)) {
        if (prior.candidateCommit === targetOldCommit) recovered = true;
        else {
          try { await this.loggedGit(threadId, repoPath, ['merge-base', '--is-ancestor', prior.candidateCommit, targetOldCommit]); recovered = true; }
          catch { recovered = false; }
        }
      }
      if (recovered) {
        const sync = ['checkout_synced', 'stash_restoring', 'stash_applied', 'stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery', 'stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(prior.phase)
          ? { ok: true, error: '', path: prior.checkoutPath, commit: targetOldCommit }
          : await this.syncCheckedOutTarget(repoPath, targetRef, prior.candidateCommit!, false, prior.targetOldCommit, threadId);
        if (!sync.ok) { this.finishBlocked(threadId, sync.error, prior.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
        if (!['stash_restoring', 'stash_applied', 'stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery', 'stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(prior.phase)) {
          prior.phase = 'checkout_synced'; this.writeTransaction(txPath, prior);
        }
        await this.restoreLandingStash(threadId, prior, txPath);
        if (prior.stashCleanupPending) return this.finishStashRecoveryPending(threadId, prior);
        const validationStatus = prior.validationPath ? await this.cleanupValidationWorktree(repoPath, threadId, prior.validationPath) : undefined;
        await this.finishMerged(threadId, sync.commit || prior.candidateCommit!, prior.targetOldCommit, sync.path,
          prior.validationPath ? this.validationRecord(prior.validationPath, prior.validationCommit, prior.validationResult, validationStatus) : undefined, prior.candidateCommit);
        prior.phase = 'ledger_written'; this.writeTransaction(txPath, prior);
        return { status: 'merged', message: 'Recovered completed merge transaction.', commit: sync.commit || prior.candidateCommit };
      }
      if (prior?.stashToken && prior.phase === 'stash_restore_conflict') {
        const reason = 'The previous landing stash needs manual recovery before another merge attempt.';
        this.finishBlocked(threadId, reason, prior.targetOldCommit, sourceCommit, prior.checkoutPath);
        return { status: 'blocked', message: reason };
      }
      if (prior?.stashToken && !['stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(prior.phase)) {
        const restored = await this.restoreLandingStash(threadId, prior, txPath);
        if (prior.stashCleanupPending) return this.finishStashRecoveryPending(threadId, prior);
        if (!restored) {
          const reason = 'The previous landing stash could not be safely restored; its checkout and stash were preserved.';
          this.finishBlocked(threadId, reason, prior.targetOldCommit, sourceCommit, prior.checkoutPath);
          return { status: 'blocked', message: reason };
        }
      }
      let resolutionPath = worktree.mergeResolutionSubmitted ? worktree.mergeResolutionPath : undefined;
      if (resolutionPath && (worktree.mergeResolutionSourceCommit || worktree.integration?.sourceCommit) !== sourceCommit) {
        const reason = 'The submitted resolution belongs to a different approved candidate.';
        this.finishBlocked(threadId, reason, worktree.integration?.targetOldCommit || '', sourceCommit);
        return { status: 'blocked', message: reason };
      }
      if (resolutionPath && worktree.integration?.targetOldCommit !== targetOldCommit) {
        const oldTarget = worktree.integration?.targetOldCommit;
        const prepared = prior?.phase === 'resolution_rebase_prepared' && prior.sourceCommit === sourceCommit && prior.validationPath === resolutionPath ? prior : undefined;
        if (!oldTarget) {
          const reason = 'Target changed while a resolution update was in progress; the resolution workspace was preserved.';
          this.finishBlocked(threadId, reason, oldTarget || '', sourceCommit, undefined, resolutionPath);
          return { status: 'blocked', message: reason };
        }
        let head = await this.loggedGit(threadId, resolutionPath, ['rev-parse', '--verify', 'HEAD^{commit}']);
        if (prepared && prepared.targetOldCommit !== targetOldCommit) {
          let mergeInProgress = false;
          try { await this.loggedGit(threadId, resolutionPath, ['rev-parse', '--verify', 'MERGE_HEAD']); mergeInProgress = true; } catch {}
          if (head === prepared.targetOldCommit &&
              !mergeInProgress &&
              !(await this.loggedGit(threadId, resolutionPath, ['diff', '--name-only'])) &&
              !(await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--exclude-standard'])) &&
              !(await this.loggedGit(threadId, resolutionPath, ['ls-files', '--others', '--ignored', '--exclude-standard'])) &&
              (await this.loggedGit(threadId, resolutionPath, ['write-tree'])) === (await this.loggedGit(threadId, repoPath, ['rev-parse', `${head}^{tree}`]))) {
            await this.loggedGit(threadId, resolutionPath, ['reset', '--hard', targetOldCommit]);
            head = targetOldCommit;
          } else if (head !== oldTarget || mergeInProgress) {
            const previousResolution = resolutionPath;
            const scratchRoot = thread.artifactDir ? taskValidationDir(thread.artifactDir) : path.join(this.storageDir, 'merge-worktrees');
            fs.mkdirSync(scratchRoot, { recursive: true });
            const nextResolution = path.join(scratchRoot, `${threadId}-${crypto.randomUUID()}`);
            await this.loggedGit(threadId, repoPath, ['worktree', 'add', '--detach', nextResolution, targetOldCommit]);
            prepared.validationPath = nextResolution; prepared.targetOldCommit = targetOldCommit; this.writeTransaction(txPath, prepared);
            const latest = this.store.getTask(threadId)!; const updated = structuredClone(latest);
            updated.worktree!.supersededMergeResolutionPaths = [...new Set([...(updated.worktree!.supersededMergeResolutionPaths || []), previousResolution])];
            updated.worktree!.mergeResolutionPath = nextResolution;
            this.store.upsertTask(updated);
            worktree.supersededMergeResolutionPaths = updated.worktree!.supersededMergeResolutionPaths;
            worktree.mergeResolutionPath = nextResolution; resolutionPath = nextResolution; head = targetOldCommit;
          }
          prepared.targetOldCommit = targetOldCommit; this.writeTransaction(txPath, prepared);
        }
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
            if (!conflicts.length) {
              const message = `Could not merge the saved resolution: ${String(error)}`;
              this.finishBlocked(threadId, message, oldTarget, sourceCommit, undefined, resolutionPath);
              return { status: 'blocked', message };
            }
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
          if (!conflicts.length) {
            const message = `Git could not start the merge: ${String(error)}`;
            retainForResolution = true;
            this.finishBlocked(threadId, message, targetOldCommit, sourceCommit, undefined, scratch);
            return { status: 'blocked', message };
          }
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
        if (beforeLanding.commit && beforeLanding.commit !== targetOldCommit) {
          const current = this.store.getTask(threadId);
          if (current) {
            const updated = structuredClone(current);
            updated.mergeHistory = [...(updated.mergeHistory || []), { id: crypto.randomUUID(), number: (updated.mergeHistory?.length || 0) + 1,
              at: new Date().toISOString(), phase: 'blocked', result: 'failed', errorCode: 'TARGET_MOVED',
              targetRef, targetOldCommit, targetCommit: beforeLanding.commit, candidateCommit: sourceCommit, attemptId: this.attemptLogs.get(threadId)?.id }];
            this.store.upsertTask(updated);
          }
          return { status: 'blocked', message: 'Target advanced; retrying integration.', retryTarget: true };
        }
        if (!beforeLanding.ok) {
          this.finishBlocked(threadId, beforeLanding.error, targetOldCommit, sourceCommit, beforeLanding.path, retainForResolution ? scratch : undefined);
          return { status: 'blocked', message: beforeLanding.error };
        }
        if (beforeLanding.path) {
          tx.checkoutPath = beforeLanding.path;
          try {
            await this.createLandingStash(threadId, tx, txPath);
            const ignored = (await this.loggedGit(threadId, beforeLanding.path, ['ls-files', '--others', '--ignored', '--exclude-standard', '--directory', '-z'], true)).split('\0').filter(Boolean);
            const incoming = (await this.loggedGit(threadId, repoPath, ['diff', '--name-only', '--diff-filter=AMRT', '-z', targetOldCommit, candidateCommit], true)).split('\0').filter(Boolean);
            const normalize = (value: string) => process.platform === 'win32' ? value.replace(/\/$/, '').toLowerCase() : value.replace(/\/$/, '');
            const blockers = ignored.filter((file) => incoming.some((name) => {
              const a = normalize(file); const b = normalize(name);
              return a === b || a.startsWith(`${b}/`) || b.startsWith(`${a}/`);
            }));
            if (blockers.length) throw new Error(`Ignored checkout files prevent synchronization: ${blockers.join(', ')}`);
            await this.loggedGit(threadId, beforeLanding.path, ['read-tree', '-n', '-u', '-m', targetOldCommit, candidateCommit]);
          } catch (error) {
            await this.restoreLandingStash(threadId, tx, txPath);
            if (tx.stashCleanupPending) return this.finishStashRecoveryPending(threadId, tx);
            throw error;
          }
        }
        if (!this.isMergeStillAllowed(threadId, sourceCommit)) { await this.restoreLandingStash(threadId, tx, txPath); if (tx.stashCleanupPending) return this.finishStashRecoveryPending(threadId, tx); validationResult = 'cancelled'; return { status: 'stale', message: 'Task was paused or deleted before the target ref update.' }; }
        // update-ref's old-value argument prevents overwriting a ref changed outside this lock.
        try {
          await this.loggedGit(threadId, repoPath, ['update-ref', targetRef, candidateCommit, targetOldCommit]);
        } catch (error) {
          const restored = await this.restoreLandingStash(threadId, tx, txPath);
          if (tx.stashCleanupPending) return this.finishStashRecoveryPending(threadId, tx);
          if (!restored) throw new Error(`Could not restore local changes after target update failed: ${String(error)}`);
          const latestTarget = await this.loggedGit(threadId, repoPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
          if (latestTarget === targetOldCommit) throw error;
          const current = this.store.getTask(threadId);
          if (current) {
            const updated = structuredClone(current);
            updated.mergeHistory = [...(updated.mergeHistory || []), { id: crypto.randomUUID(), number: (updated.mergeHistory?.length || 0) + 1,
              at: new Date().toISOString(), phase: 'blocked', result: 'failed', errorCode: 'CAS_FAILED',
              error: String(error).slice(-5000), targetRef, targetOldCommit, targetCommit: latestTarget,
              candidateCommit: sourceCommit, attemptId: this.attemptLogs.get(threadId)?.id }];
            this.store.upsertTask(updated);
          }
          return { status: 'blocked', message: 'Target advanced; retrying integration against its latest commit.', retryTarget: true };
        }
        tx.phase = 'ref_updated'; this.writeTransaction(txPath, tx);
        const synced = await this.syncCheckedOutTarget(repoPath, targetRef, candidateCommit, false, targetOldCommit, threadId);
        if (!synced.ok) { this.finishBlocked(threadId, synced.error, targetOldCommit, sourceCommit, synced.path, retainForResolution ? scratch : undefined); return { status: 'blocked', message: synced.error }; }
        tx.phase = 'checkout_synced'; this.writeTransaction(txPath, tx);
        await this.restoreLandingStash(threadId, tx, txPath);
        if (tx.stashCleanupPending) return this.finishStashRecoveryPending(threadId, tx);
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
      const failedAttempt = this.attemptLogs.get(threadId);
      if (failedAttempt) try { fs.appendFileSync(failedAttempt.path, `\n[error] ${String(error)}\n`, 'utf8'); } catch {}
      // If update-ref succeeded but a later transaction/ledger write failed,
      // retain the landed fact and finish the same transaction before reporting.
      if (await this.hasLandedCandidate(threadId)) {
        const tx = this.readTransaction(this.transactionPath(threadId));
        if (tx?.candidateCommit) {
          const sync = ['checkout_synced', 'stash_restoring', 'stash_applied', 'stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery', 'stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(tx.phase)
            ? { ok: true, error: '', path: tx.checkoutPath, commit: tx.candidateCommit }
            : await this.syncCheckedOutTarget(repoPath, targetRef, tx.candidateCommit, false, tx.targetOldCommit, this.isMergeStillAllowed(threadId, sourceCommit) ? threadId : undefined);
          if (!sync.ok) { this.finishBlocked(threadId, sync.error, tx.targetOldCommit, sourceCommit, sync.path); return { status: 'blocked', message: sync.error }; }
          if (!['stash_restoring', 'stash_applied', 'stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery', 'stash_restored', 'stash_restore_conflict', 'ledger_written'].includes(tx.phase)) { tx.phase = 'checkout_synced'; this.writeTransaction(this.transactionPath(threadId), tx); }
          await this.restoreLandingStash(threadId, tx, this.transactionPath(threadId));
          if (tx.stashCleanupPending) return this.finishStashRecoveryPending(threadId, tx);
          const validationStatus = tx.validationPath ? await this.cleanupValidationWorktree(repoPath, threadId, tx.validationPath) : undefined;
          await this.finishMerged(threadId, sync.commit || tx.candidateCommit, tx.targetOldCommit, sync.path,
            tx.validationPath ? this.validationRecord(tx.validationPath, tx.validationCommit, tx.validationResult, validationStatus) : undefined, tx.candidateCommit);
          tx.phase = 'ledger_written';
          this.writeTransaction(this.transactionPath(threadId), tx);
          return { status: 'merged', message: 'Recovered target update after a durable ledger write failure.', commit: sync.commit || tx.candidateCommit };
        }
      }
      if (!this.isMergeStillAllowed(threadId, sourceCommit)) return { status: 'stale', message: 'Task was paused or deleted during integration.' };
      if (/Could not durably persist task/i.test(String(error))) throw error;
      const latest = this.store.getTask(threadId);
      if (latest?.status === 'merging' && latest.worktree?.deliveryTarget) {
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
      this.attemptLogs.delete(threadId); release(); if (roleLease) mergeRole?.release(roleLease);
    }
  }

  private async createLandingStash(threadId: string, tx: any, file: string): Promise<void> {
    const cwd = tx.checkoutPath as string;
    if (!await this.loggedGit(threadId, cwd, ['status', '--porcelain', '--untracked-files=all'])) return;
    tx.stashBefore = (await this.loggedGit(threadId, cwd, ['for-each-ref', '--format=%(objectname)', 'refs/stash'])) || null;
    tx.stashToken = `mesh-landing-${threadId}-${crypto.randomUUID()}`;
    tx.phase = 'stash_creating'; this.writeTransaction(file, tx);
    await this.loggedGit(threadId, cwd, ['-c', 'core.autocrlf=false', 'stash', 'push', '--include-untracked', '-m', tx.stashToken]);
    const matches = this.findLandingStash(cwd, tx.stashToken);
    if (matches.length !== 1 || matches[0].sha === tx.stashBefore) throw new Error('Could not uniquely identify the newly created landing stash.');
    tx.stashSha = matches[0].sha; tx.phase = 'stash_created'; this.writeTransaction(file, tx);
    if (await this.loggedGit(threadId, cwd, ['status', '--porcelain', '--untracked-files=all'])) throw new Error('Target checkout is not clean after creating the landing stash.');
  }

  private findLandingStash(cwd: string, token: string): Array<{ ref: string; sha: string }> {
    return gitOutput(cwd, ['stash', 'list', '--format=%gd%x09%H%x09%gs']).split(/\r?\n/).filter(Boolean).map((line) => {
      const [ref, sha, ...message] = line.split('\t'); return { ref, sha, message: message.join('\t') };
    }).filter((entry) => entry.message.endsWith(`: ${token}`)).map(({ ref, sha }) => ({ ref, sha }));
  }

  private writeLockedGitFile(file: string, data: Buffer): void {
    const temporary = `${file}.mesh-${crypto.randomUUID()}.lock`;
    const mode = fs.statSync(file).mode & 0o777;
    try {
      const fd = fs.openSync(temporary, 'wx', mode);
      try { fs.writeFileSync(fd, data); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      fs.renameSync(temporary, file);
      if (process.platform !== 'win32') { const dir = fs.openSync(path.dirname(file), 'r'); try { fs.fsyncSync(dir); } finally { fs.closeSync(dir); } }
    } finally { try { fs.unlinkSync(temporary); } catch {} }
  }

  private async removeMultipleLandingStash(threadId: string, cwd: string, tx: any, file: string): Promise<boolean> {
    if (gitOutput(cwd, ['rev-parse', '--show-ref-format']) !== 'files') return false;
    const common = fs.realpathSync(gitOutput(cwd, ['rev-parse', '--path-format=absolute', '--git-common-dir']));
    const refPath = path.join(common, 'refs', 'stash');
    const logPath = path.join(common, 'logs', 'refs', 'stash');
    const samePath = (a: string, b: string) => process.platform === 'win32' ? a.toLowerCase() === b.toLowerCase() : a === b;
    if (!samePath(path.resolve(gitOutput(cwd, ['rev-parse', '--path-format=absolute', '--git-path', 'refs/stash'])), refPath) ||
        !samePath(path.resolve(gitOutput(cwd, ['rev-parse', '--path-format=absolute', '--git-path', 'logs/refs/stash'])), logPath)) return false;
    const parse = (data: Buffer, contiguous = true): Array<{ old: string; next: string; tail: string }> => {
      if (data.length > 2 * 1024 * 1024 || !data.length || data[data.length - 1] !== 10) throw new Error('Stash reflog is empty, malformed, or exceeds the cleanup limit.');
      const lines = data.toString('latin1').slice(0, -1).split('\n');
      if (lines.length > 10000) throw new Error('Stash reflog exceeds the cleanup entry limit.');
      const result = lines.map((line) => {
        const match = /^([0-9a-f]+) ([0-9a-f]+) ([^\n]*\t[^\n]*)$/i.exec(line);
        if (!match || match[1].length !== tx.stashSha.length || match[2].length !== tx.stashSha.length) throw new Error('Stash reflog contains an invalid entry.');
        return { old: match[1], next: match[2], tail: match[3] };
      });
      if (contiguous && (!/^0+$/.test(result[0].old) || result.some((entry, index) => index > 0 && entry.old !== result[index - 1].next))) throw new Error('Stash reflog history is not contiguous.');
      return result;
    };
    const encode = (entries: Array<{ old: string; next: string; tail: string }>) => Buffer.from(entries.map((entry) => `${entry.old} ${entry.next} ${entry.tail}\n`).join(''), 'latin1');
    const build = (head: string, before: Buffer) => {
      const entries = parse(before);
      if (entries[entries.length - 1].next !== head) throw new Error('Stash ref and reflog do not agree.');
      const indexes = entries.map((entry, index) => entry.next === tx.stashSha && entry.tail.endsWith(`: ${tx.stashToken}`) ? index : -1).filter((index) => index >= 0);
      if (indexes.length !== 1 || entries.length < 2) throw new Error('Landing stash identity is not unique in a multi-entry reflog.');
      const index = indexes[0]; const removed = entries[index]; entries.splice(index, 1);
      if (index < entries.length) entries[index].old = removed.old;
      return { after: encode(entries), afterRef: entries[entries.length - 1].next };
    };
    const prepare = (head: string, before: Buffer) => {
      const { after, afterRef } = build(head, before);
      tx.stashCleanup = { common, beforeRef: head, afterRef, beforeLog: before.toString('base64'), afterLog: after.toString('base64') };
      tx.phase = 'stash_cleanup_prepared'; this.writeTransaction(file, tx);
      return { after, afterRef: tx.stashCleanup.afterRef as string };
    };
    const apply = (head: string) => {
      if (!fs.lstatSync(refPath).isFile() || !fs.lstatSync(logPath).isFile()) throw new Error('Stash ref or reflog is not a regular file.');
      if (fs.statSync(logPath).size > 2 * 1024 * 1024) throw new Error('Stash reflog exceeds the cleanup size limit.');
      const refContents = fs.readFileSync(refPath, 'utf8');
      if (refContents !== `${head}\n` && refContents !== head) throw new Error('Stash ref changed while obtaining its lock.');
      let current = fs.readFileSync(logPath);
      let evidence = tx.stashCleanup;
      if (evidence && !samePath(evidence.common, common)) throw new Error('Stash cleanup repository identity changed.');
      if (evidence && (typeof evidence.beforeLog !== 'string' || typeof evidence.afterLog !== 'string' ||
          evidence.beforeLog.length > 2_796_208 || evidence.afterLog.length > 2_796_208)) throw new Error('Stash cleanup evidence is invalid or exceeds its size limit.');
      let before = evidence ? Buffer.from(evidence.beforeLog, 'base64') : undefined;
      let after = evidence ? Buffer.from(evidence.afterLog, 'base64') : undefined;
      if (!evidence || (before && current.length > before.length && current.subarray(0, before.length).equals(before))) {
        const next = prepare(head, current); evidence = tx.stashCleanup; before = Buffer.from(evidence.beforeLog, 'base64'); after = next.after;
      }
      if (!before || !after) throw new Error('Stash cleanup evidence is missing.');
      const expected = build(evidence.beforeRef, before);
      if (!after.equals(expected.after) || evidence.afterRef !== expected.afterRef) throw new Error('Stash cleanup evidence does not match its original reflog.');
      if (current.equals(before) && head === evidence.beforeRef) {
        this.writeLockedGitFile(logPath, after);
        tx.phase = 'stash_reflog_published'; this.writeTransaction(file, tx);
        current = after;
      }
      if (current.equals(after) && (head === evidence.beforeRef || head === evidence.afterRef)) {
        if (head !== evidence.afterRef) this.writeLockedGitFile(refPath, Buffer.from(`${evidence.afterRef}\n`, 'ascii'));
        tx.phase = 'stash_ref_published'; this.writeTransaction(file, tx);
        return true;
      }
      if (current.length > after.length && current.subarray(0, after.length).equals(after)) {
        const suffix = current.subarray(after.length); const appended = parse(Buffer.concat([Buffer.from(`${'0'.repeat(tx.stashSha.length)} ${evidence.afterRef} mesh <mesh@localhost> 0 +0000\tanchor\n`, 'ascii'), suffix]), false);
        if (appended[1]?.old === evidence.beforeRef && evidence.beforeRef !== evidence.afterRef) appended[1].old = evidence.afterRef;
        if (appended[1]?.old !== evidence.afterRef || appended[appended.length - 1].next !== head || appended.slice(1).some((entry) => entry.next === tx.stashSha && entry.tail.endsWith(`: ${tx.stashToken}`)) || appended.slice(2).some((entry, index) => entry.old !== appended[index + 1].next)) throw new Error('Stash reflog changed in an unrecognizable way after cleanup.');
        this.writeLockedGitFile(logPath, Buffer.concat([after, encode(appended.slice(1))]));
        tx.phase = 'stash_ref_published'; this.writeTransaction(file, tx);
        return true;
      }
      throw new Error('Stash cleanup state changed and cannot be safely reconciled.');
    };
    const lockPath = `${refPath}.lock`;
    const markerRoot = path.join(common, 'agent-mesh-bridge', 'stash-locks');
    const ownsLock = (markerPath: string, marker: string) => {
      const lock = fs.lstatSync(lockPath); const owner = fs.lstatSync(markerPath);
      return lock.isFile() && owner.isFile() && lock.dev === owner.dev && lock.ino === owner.ino && fs.readFileSync(lockPath, 'utf8') === marker;
    };
    const priorLock = tx.stashCleanupLock;
    let markerPath = ''; let marker = ''; let owned = false; let protectOnExit = false;
    if (priorLock) {
      const expectedMarker = path.join(markerRoot, priorLock.token || '');
      if (!/^[0-9a-f-]{36}$/i.test(priorLock.token || '') || !Number.isInteger(priorLock.ownerPid) || priorLock.ownerPid <= 0 || !samePath(priorLock.markerPath || '', expectedMarker)) throw new Error('Stash lock recovery metadata is invalid.');
      if (priorLock.ownerPid !== process.pid) {
        try {
          process.kill(priorLock.ownerPid, 0);
          // The global merge lock proves a live previous owner has handed off this protected recovery.
          if (tx.phase !== 'stash_cleanup_needs_recovery' || !tx.stashCleanupPending) throw new Error('The previous stash cleanup owner is still alive.');
        } catch (failure: any) { if (failure?.code !== 'ESRCH') throw failure; }
      }
      const priorMarker = `mesh-stash-lock ${priorLock.token}\n`;
      try {
        if (fs.existsSync(lockPath)) {
          let ownedByPrior = false;
          try { ownedByPrior = ownsLock(expectedMarker, priorMarker); } catch (failure: any) { if (failure?.code !== 'ENOENT') throw failure; }
          if (ownedByPrior) {
            markerPath = expectedMarker; marker = priorMarker; owned = true; protectOnExit = true;
            priorLock.ownerPid = process.pid; this.writeTransaction(file, tx);
          } else if (fs.readFileSync(lockPath, 'utf8') === priorMarker) throw new Error('Cannot prove ownership of the stale stash lock.');
        }
      } catch (failure: any) { if (failure?.code !== 'ENOENT') throw failure; }
      if (!owned) {
        try { if (fs.readFileSync(expectedMarker, 'utf8') === priorMarker) fs.unlinkSync(expectedMarker); } catch (failure: any) { if (failure?.code !== 'ENOENT') throw failure; }
        delete tx.stashCleanupLock; this.writeTransaction(file, tx);
      }
    }
    if (!owned) {
      const token = crypto.randomUUID();
      fs.mkdirSync(markerRoot, { recursive: true });
      markerPath = path.join(markerRoot, token); marker = `mesh-stash-lock ${token}\n`;
      const fd = fs.openSync(markerPath, 'wx', 0o600);
      try { fs.writeFileSync(fd, marker); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      tx.stashCleanupLock = { token, markerPath, ownerPid: process.pid };
      try { this.writeTransaction(file, tx); } catch (failure) { try { fs.unlinkSync(markerPath); } catch {} throw failure; }
    }
    const release = (): boolean => {
      if (owned) {
        try {
          if (fs.existsSync(lockPath)) {
            if (!ownsLock(markerPath, marker)) return false;
            fs.unlinkSync(lockPath);
          }
        } catch { return false; }
      }
      try { if (fs.readFileSync(markerPath, 'utf8') === marker) fs.unlinkSync(markerPath); } catch {}
      owned = false; return true;
    };
    const releaseOnExit = () => { if (!protectOnExit) release(); };
    process.once('exit', releaseOnExit);
    try {
      for (let attempt = 0; !owned && attempt < 100; attempt++) {
        try { fs.linkSync(markerPath, lockPath); owned = true; break; }
        catch (failure: any) {
          if (failure?.code !== 'EEXIST') throw failure;
          if (!this.isMergeStillAllowed(threadId, tx.sourceCommit)) return false;
          await sleep(100);
        }
      }
      if (!owned) return false;
      const head = gitOutput(cwd, ['for-each-ref', '--format=%(objectname)', 'refs/stash']);
      if (!/^[0-9a-f]{40,64}$/i.test(head)) return false;
      try {
        const result = apply(head);
        delete tx.stashCleanupPending; delete tx.stashRestoreError; protectOnExit = false;
        return result;
      } catch (failure) {
        const evidence = tx.stashCleanup;
        if (!evidence) throw failure;
        try {
          if (!samePath(evidence.common, common) || typeof evidence.beforeLog !== 'string' || typeof evidence.afterLog !== 'string' || evidence.beforeLog.length > 2_796_208 || evidence.afterLog.length > 2_796_208) throw new Error('Invalid stash cleanup evidence.');
          const before = Buffer.from(evidence.beforeLog, 'base64'); const after = Buffer.from(evidence.afterLog, 'base64');
          const expected = build(evidence.beforeRef, before);
          if (!after.equals(expected.after) || evidence.afterRef !== expected.afterRef) throw new Error('Stash cleanup evidence does not match its original reflog.');
          const actualLog = fs.readFileSync(logPath); const actualRef = fs.readFileSync(refPath, 'utf8').trim();
          if (actualLog.equals(after) && actualRef === evidence.afterRef) {
            tx.phase = 'stash_ref_published'; delete tx.stashCleanupPending; delete tx.stashRestoreError; this.writeTransaction(file, tx);
            protectOnExit = false; return true;
          }
          if (actualLog.equals(after) && actualRef === evidence.beforeRef && evidence.beforeRef !== evidence.afterRef) {
            protectOnExit = true;
            this.writeLockedGitFile(logPath, before);
            if (!fs.readFileSync(logPath).equals(before) || fs.readFileSync(refPath, 'utf8').trim() !== evidence.beforeRef) throw new Error('Stash reflog rollback could not be verified.');
            tx.phase = 'stash_cleanup_rolled_back'; tx.stashCleanupPending = true; tx.stashRestoreError = String(failure);
            this.writeTransaction(file, tx); protectOnExit = false; return false;
          }
          if (actualLog.equals(before) && actualRef === evidence.beforeRef) throw failure;
          throw new Error('Stash ref and reflog cannot be safely reconciled after publication failure.');
        } catch (recoveryFailure) {
          if (recoveryFailure === failure && !protectOnExit) throw failure;
          protectOnExit = true; tx.phase = 'stash_cleanup_needs_recovery'; tx.stashCleanupPending = true;
          tx.stashRestoreError = `${String(failure)}; rollback: ${String(recoveryFailure)}`;
          try { this.writeTransaction(file, tx); } catch {}
          return false;
        }
      }
    } finally {
      process.removeListener('exit', releaseOnExit);
      if (!protectOnExit) {
        if (!release()) { process.once('exit', releaseOnExit); this.writeTransaction(file, tx); throw new Error('Could not safely release the stash ref lock; recovery metadata was retained.'); }
        delete tx.stashCleanupLock; this.writeTransaction(file, tx);
      }
    }
  }

  private async removeSoleLandingStash(threadId: string, cwd: string, sha: string, token: string, recoverMissingLog = false): Promise<boolean> {
    if (!/^[0-9a-f]{40,64}$/i.test(sha)) throw new Error('Invalid landing stash SHA.');
    const registration = this.executions.begin(threadId, this.store.getTask(threadId)?.currentRound || 0, 'merge-git', cwd);
    let child: ReturnType<typeof spawn>;
    try { child = this.spawnJobIfV2(threadId, gitExecutable(), ['update-ref', '--stdin'], cwd) ??
      spawn(gitExecutable(), ['update-ref', '--stdin'], { cwd, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] }); }
    catch (failure) { this.executions.end(registration); throw failure; }
    const windows = process.platform === 'win32';
    let childBirthId = !windows && child.pid ? getProcessBirthId(child.pid) : undefined;
    let output = ''; let error = ''; let prepared = false; let committed = false;
    const completion = new Promise<boolean>((resolve) => {
      child.stdin!.on('error', (failure) => { error += failure.message; });
      child.stdout!.on('data', (chunk: Buffer) => {
        output += chunk.toString();
        if (!prepared && output.includes('prepare: ok')) {
          prepared = true;
          try {
            const entries = gitOutput(cwd, ['stash', 'list', '--format=%H%x09%gs']).split(/\r?\n/).filter(Boolean);
            const matches = this.findLandingStash(cwd, token);
            const format = gitOutput(cwd, ['rev-parse', '--show-ref-format']);
            const logPath = gitOutput(cwd, ['rev-parse', '--git-path', 'logs/refs/stash']);
            const absoluteLogPath = path.resolve(cwd, logPath);
            const hasLog = format === 'files' && fs.existsSync(absoluteLogPath);
            const rawLog = hasLog ? fs.readFileSync(absoluteLogPath, 'utf8') : '';
            const lines = rawLog.endsWith('\n') ? rawLog.slice(0, -1).split('\n') : [];
            const line = lines.length === 1 ? lines[0].match(/^([0-9a-f]+) ([0-9a-f]+) [^\n]*\t([^\n]*)$/i) : undefined;
            committed = entries.length === 1 && matches.length === 1 && matches[0].sha === sha &&
              !!line && line[1].length === sha.length && /^0+$/.test(line[1]) && line[2] === sha && line[3].endsWith(`: ${token}`);
            if (recoverMissingLog && format === 'files' && !hasLog && entries.length === 0 && matches.length === 0) committed = true;
            child.stdin!.end(committed ? 'commit\n' : 'abort\n');
          } catch (failure) { error = String(failure); child.stdin!.end('abort\n'); }
        }
      });
      child.stderr!.on('data', (chunk: Buffer) => { error += chunk.toString(); });
      // A failed helper start is not a completed removal: keep the outcome
      // pending until the process actually closes.
      child.once('error', (failure) => { error += failure.message; });
      child.once('close', (code) => resolve(code === 0 && committed && output.includes('commit: ok')));
    });
    const timer = setTimeout(() => killProcessTree(child, childBirthId, true), 30_000);
    timer.unref?.();
    try {
      if (!child.pid) throw new Error('Git stash cleanup process has no PID.');
      this.executions.attachChildPid(child.pid, registration);
      const birthId = await this.lock.attachChildPid(child.pid);
      if (windows) {
        if (typeof birthId !== 'string' || childHasExited(child)) throw new Error('Could not verify the Git stash cleanup child identity from the mesh lock helper.');
        childBirthId = birthId;
      }
      await this.activeChildPidCallback?.(child.pid, child, childBirthId);
      startWindowsJob(child);
      child.stdin!.write(`start\noption no-deref\ndelete refs/stash ${sha}\nprepare\n`);
      const removed = await completion;
      if (!removed && error) {
        const log = this.attemptLogs.get(threadId)?.path;
        if (log) fs.appendFileSync(log, `\n[local stash cleanup] ${error}\n`, 'utf8');
      }
      return removed;
    } catch (failure) { child.stdin!.end(); killProcessTree(child, childBirthId, true); await completion; throw failure; }
    finally { clearTimeout(timer); this.lock.detachChildPid(child.pid); this.executions.end(registration); }
  }

  private async restoreLandingStash(threadId: string, tx: any, file: string): Promise<boolean> {
    if (!tx.stashToken || tx.phase === 'stash_restored' || tx.phase === 'ledger_written') return true;
    if (tx.phase === 'stash_restore_conflict') return false;
    const cwd = tx.checkoutPath as string;
    const log = this.attemptLogs.get(threadId)?.path;
    let error = '';
    try {
      if (tx.phase === 'stash_restoring') throw new Error('Stash restoration was interrupted; its result is uncertain. The stash and checkout were left untouched.');
      const multiRecovery = ['stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'stash_cleanup_rolled_back', 'stash_cleanup_needs_recovery'].includes(tx.phase);
      const alreadyApplied = tx.phase === 'stash_applied' || tx.phase === 'stash_cleanup_started' || multiRecovery;
      const recoveringCleanup = tx.phase === 'stash_cleanup_started';
      const matches = multiRecovery ? [] : this.findLandingStash(cwd, tx.stashToken);
      if (!alreadyApplied) {
        if (matches.length !== 1 || (tx.stashSha && matches[0].sha !== tx.stashSha) || matches[0].sha === tx.stashBefore) throw new Error('The landing stash cannot be uniquely identified.');
        tx.stashSha = matches[0].sha;
        tx.phase = 'stash_restoring'; this.writeTransaction(file, tx);
        if (gitOutput(cwd, ['rev-parse', '--verify', `${matches[0].ref}^{commit}`]) !== tx.stashSha) throw new Error('The landing stash moved before restoration.');
        if (log) fs.appendFileSync(log, `\n[local stash restore] ${matches[0].ref} ${tx.stashSha}\n`, 'utf8');
        gitOutput(cwd, ['-c', 'core.autocrlf=false', 'stash', 'apply', '--index', tx.stashSha]);
        tx.phase = 'stash_applied'; this.writeTransaction(file, tx);
      }
      if (!multiRecovery) { tx.phase = 'stash_cleanup_started'; this.writeTransaction(file, tx); }
      const currentStashRef = recoveringCleanup && matches.length === 0
        ? gitOutput(cwd, ['for-each-ref', '--format=%(objectname)', 'refs/stash']) : undefined;
      let removed = recoveringCleanup && matches.length === 0 && !currentStashRef;
      const uncertain = recoveringCleanup && matches.length === 0 && !!currentStashRef && currentStashRef !== tx.stashSha;
      if (uncertain) {
        tx.stashCleanupUncertain = true;
        tx.stashRestoreError = 'The stash reflog changed after cleanup started; the cleanup outcome is uncertain and other entries were left untouched.';
      }
      try {
        if (multiRecovery) removed = await this.removeMultipleLandingStash(threadId, cwd, tx, file);
        else if (!removed && !uncertain) {
          removed = await this.removeSoleLandingStash(threadId, cwd, tx.stashSha, tx.stashToken, recoveringCleanup);
          if (!removed && (!recoveringCleanup || matches.length === 1)) removed = await this.removeMultipleLandingStash(threadId, cwd, tx, file);
        }
      } catch (failure) {
        tx.stashRestoreError = `Stash cleanup failed: ${String(failure)}`;
        if (tx.stashCleanup && !tx.stashCleanupPending) {
          try { removed = await this.removeMultipleLandingStash(threadId, cwd, tx, file); }
          catch (recoveryFailure) { tx.stashCleanupUncertain = true; tx.stashRestoreError += `; recovery: ${String(recoveryFailure)}`; }
        }
      }
      if (multiRecovery && !removed && !tx.stashCleanupPending) {
        tx.phase = 'stash_cleanup_needs_recovery'; tx.stashCleanupPending = true;
        tx.stashRestoreError ||= 'Stash metadata recovery could not be confirmed; its journal was retained.';
        this.writeTransaction(file, tx);
      }
      if (tx.stashCleanupPending) return false;
      tx.stashRetained = !removed;
      if (removed) { delete tx.stashRestoreError; delete tx.stashCleanupUncertain; }
      if (!removed && !tx.stashRestoreError) tx.stashRestoreError = 'The restored stash was retained because the reflog contains other entries or its identity could not be verified under the Git ref lock.';
      tx.phase = 'stash_restored'; this.writeTransaction(file, tx);
      if (log) fs.appendFileSync(log, removed ? '[local stash restore] completed; stash removed\n' : `[local stash restore] applied; ${tx.stashSha} retained: ${tx.stashRestoreError}\n`, 'utf8');
      return true;
    } catch (failure) {
      if (tx.stashCleanupPending) throw failure;
      if (tx.phase === 'stash_restored' && !tx.stashRetained) throw failure;
      error = String(failure);
    }
    tx.phase = 'stash_restore_conflict'; tx.stashRestoreError = error; this.writeTransaction(file, tx);
    if (log) fs.appendFileSync(log, `\n[local stash restore] ${error}\n`, 'utf8');
    return false;
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
      if (targetCommit !== previousCommit || await head(row.worktree) !== targetCommit) return { ok: false, path: row.worktree, commit: targetCommit, error: `Target branch ${targetRef} changed before landing.` };
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
    if (thread.status === 'completed' || thread.status === 'failed') thread.finalSummary = undefined;
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

  private finishStashRecoveryPending(threadId: string, tx: any): MergeResult {
    const current = this.store.getTask(threadId);
    const message = `Stash metadata cleanup requires recovery (${tx.phase}): ${tx.stashRestoreError || 'publication was interrupted'}`;
    if (!current?.worktree) return { status: 'blocked', message };
    const thread = structuredClone(current); const worktree = thread.worktree!; const at = new Date().toISOString();
    let landedTarget: string | undefined;
    try {
      const repo = requireDeliveryTarget(worktree).repositoryPath;
      const ref = requireDeliveryTarget(worktree).ref;
      const target = gitOutput(repo, ['rev-parse', '--verify', `${ref}^{commit}`]);
      if (target !== tx.candidateCommit) gitOutput(repo, ['merge-base', '--is-ancestor', tx.candidateCommit, target]);
      landedTarget = target;
    } catch {}
    if (thread.status !== 'paused') thread.status = 'reviewing';
    worktree.integration = { ...worktree.integration, phase: 'blocked', sourceCommit: tx.sourceCommit, candidateCommit: tx.candidateCommit,
      targetOldCommit: tx.targetOldCommit, targetPath: tx.checkoutPath, stashRecoveryPending: true, stashRecoveryLockHeld: tx.phase === 'stash_cleanup_needs_recovery',
      ...(landedTarget ? { targetCommit: landedTarget, mergedCommit: tx.candidateCommit } : {}),
      stashRestore: { status: 'stash_cleanup_uncertain', sha: tx.stashSha, error: tx.stashRestoreError }, error: message, updatedAt: at };
    thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number: (thread.mergeHistory?.length || 0) + 1,
      at, phase: 'stash_cleanup', result: 'pending', error: message, errorCode: 'STASH_CLEANUP_PENDING', attemptId: this.attemptLogs.get(threadId)?.id }];
    thread.summary.lastOutputSnippet = message; thread.updatedAt = at; this.store.upsertTask(thread);
    return landedTarget ? { status: 'merged', message: `The merge is landed, but ${message}`, commit: tx.candidateCommit } : { status: 'blocked', message };
  }

  private finishBlocked(threadId: string, reason: string, targetOldCommit: string, sourceCommit: string, targetPath?: string, resolutionPath?: string): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    thread.status = 'reviewing';
    if (resolutionPath) { thread.worktree!.mergeResolutionPath = resolutionPath; thread.worktree!.mergeResolutionSourceCommit = sourceCommit; }
    thread.worktree!.integration = { ...thread.worktree!.integration, phase: 'blocked', error: reason, targetOldCommit, targetPath, sourceCommit, updatedAt: new Date().toISOString() };
    if (thread.worktree!.approvedCommit) {
      const code = /uncommitted|dirty|no longer matches/i.test(reason) ? 'DIRTY_CHECKOUT' : /update-ref|cannot lock ref|expected/i.test(reason) ? 'CAS_FAILED' :
        /permission|denied|unauthori[sz]ed/i.test(reason) ? 'PERMISSION' : /target.*(?:advanced|changed|moved)/i.test(reason) ? 'TARGET_MOVED' : 'BLOCKED';
      const budget = noteMergeFailure(thread, 'blocked', code, reason);
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number: (thread.mergeHistory?.length || 0) + 1,
        at: new Date().toISOString(), phase: 'blocked', result: 'failed', error: reason.slice(-5000), errorCode: code,
        targetRef: requireDeliveryTarget(thread.worktree!).ref, targetOldCommit, candidateCommit: sourceCommit,
        candidateTree: this.attemptLogs.get(threadId)?.candidateTree, attemptId: this.attemptLogs.get(threadId)?.id, ...budget }];
    }
    thread.summary.lastOutputSnippet = reason; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
    if (thread.worktree!.approvedCommit && thread.status === 'reviewing') {
      const timer = setTimeout(() => {
        const latest = this.store.getTask(threadId);
        if (latest?.status === 'reviewing' && latest.worktree?.integration?.phase === 'blocked') this.requestReview?.(latest);
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
      const { child, start } = this.spawnMergeChild(threadId, step.command, step.args, cwd, env, step.needsShell);
      const windows = process.platform === 'win32';
      let childBirthId = !windows && child.pid ? getProcessBirthId(child.pid) : undefined;
      let output = '';
      let trackingError: Error | undefined;
      try { this.executions.attachChildPid(child.pid, registration); }
      catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
      let resolveResult!: (result: { code: number | null; output: string }) => void;
      const resultPromise = new Promise<{ code: number | null; output: string }>((resolve) => { resolveResult = resolve; });
      const record = (chunk: Buffer) => {
        output = (output + chunk.toString()).slice(-8192);
        if (logPath) try { fs.appendFileSync(logPath, chunk); } catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
      };
      child.stdout!.on('data', record);
      child.stderr!.on('data', record);
      const timer = setTimeout(() => {
        if (windowsJobOf(child)) {
          killProcessTree(child, childBirthId, true);
          return;
        }
        child.kill('SIGTERM');
        setTimeout(() => child.kill('SIGKILL'), 5_000).unref();
      }, 20 * 60 * 1000);
      timer.unref?.();
      child.on('close', (code) => { clearTimeout(timer); resolveResult({ code, output }); });
      // Spawn/pipe failure is not proof of exit: stop the tree and let `close`
      // settle the result so the lock is not detached under a live process.
      child.on('error', (error) => {
        if (!trackingError) trackingError = error;
        killProcessTree(child, childBirthId, true);
      });
      const cancelled = () => !this.isMergeStillAllowed(threadId, sourceCommit);
      if (windows && !trackingError) {
        try {
          const birthId = await this.lock.attachChildPid(child.pid, cancelled);
          if (child.pid && !childHasExited(child)) {
            if (typeof birthId === 'string') childBirthId = birthId;
            else if (!await Promise.race([resultPromise.then(() => true), sleep(100).then(() => false)])) {
              throw new Error('Mesh lock helper could not identify a validation child that had not closed.');
            }
          }
        } catch (error) {
          trackingError = error as Error;
          if (child.pid && !childHasExited(child)) {
            childBirthId = getProcessBirthId(child.pid);
            killProcessTree(child, childBirthId, true);
          }
        }
      }
      try { if (!windows && !trackingError) await this.lock.attachChildPid(child.pid, cancelled); }
      catch (error) { trackingError = error as Error; killProcessTree(child, childBirthId, true); }
      try {
        if (child.pid && !trackingError && (!windows || (childBirthId && !childHasExited(child)))) {
          await this.activeChildPidCallback?.(child.pid, child, childBirthId);
        }
      }
      catch (error: any) {
        trackingError = error as Error;
        if (error?.code !== 'CHILD_BINDING_IN_PROGRESS') killProcessTree(child, childBirthId, true);
      }
      if (start && !trackingError) start();
      else if (start) child.stdin!.end();
      const result = await resultPromise;
      this.lock.detachChildPid(child.pid);
      const code = trackingError ? -1 : result.code;
      if (logPath) fs.appendFileSync(logPath, `\n${step.label} ${code === 0 ? 'passed' : `failed (exit ${code})`}\n`, 'utf8');
      if (code !== 0) return { ok: false, error: `${step.label} failed (exit ${code}): ${trackingError?.message || result.output}` };
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
    return !!latest && (latest.status === 'merging' || (latest.status === 'queued' && latest.queuePhase === 'merge')) && latest.worktree?.approvedCommit === sourceCommit;
  }

  private markNeedsReview(thread: DelegationThread, reason: string): void {
    thread = structuredClone(thread);
    thread.status = 'reviewing'; thread.worktree!.approvedCommit = undefined;
    thread.worktree!.integration = { phase: 'ready', error: reason, updatedAt: new Date().toISOString() };
    thread.summary.lastOutputSnippet = reason; thread.updatedAt = new Date().toISOString(); this.store.upsertTask(thread);
    const timer = setTimeout(() => {
      const latest = this.store.getTask(thread.threadId);
      if (latest?.status === 'reviewing') this.requestReview?.(latest);
    }, 0);
    timer.unref();
  }

  private finishMergeFailure(threadId: string, phase: 'conflict' | 'validation_failed', error: string, targetOldCommit: string, sourceCommit: string, conflictFiles?: string[], resolutionPath?: string): void {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    thread.status = 'reviewing';
    if (resolutionPath) { worktree.mergeResolutionPath = resolutionPath; worktree.mergeResolutionSubmitted = false; worktree.mergeResolutionSourceCommit = sourceCommit; }
    const at = new Date().toISOString();
    {
      const number = (thread.mergeHistory?.length || 0) + 1;
      const safeId = threadId.replace(/[^A-Za-z0-9_-]/g, '_');
      const dir = path.join(taskArtifactBase(thread.threadId, this.storageDir, thread.artifactDir), 'merge_log');
      let logPath: string | undefined; let logFileNumber: number | undefined;
      try {
        ({ logPath, logFileNumber } = createMergeLog(dir, safeId, number, `${phase}: ${error}\n`));
      } catch { logPath = undefined; }
      const code = phase === 'validation_failed' ? (error.match(/exit\s+(-?\d+)/i)?.[1] || 'VALIDATION_FAILED') : 'CONFLICT';
      const budget = noteMergeFailure(thread, phase, code, error, conflictFiles);
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number, at, phase, result: 'failed', ...budget, attemptId: this.attemptLogs.get(threadId)?.id,
        error: error.slice(-5000), errorCode: phase === 'conflict' ? 'CONFLICT' : 'VALIDATION_FAILED', logPath, logFileNumber, exitCode: phase === 'conflict' ? 1 : Number(error.match(/exit\s+(-?\d+)/i)?.[1] || -1), targetRef: requireDeliveryTarget(worktree).ref,
        targetOldCommit, candidateCommit: sourceCommit, candidateTree: this.attemptLogs.get(threadId)?.candidateTree || worktree.approvedSnapshot?.tree, conflictFiles }];
    }
    worktree.integration = { phase, error: error.slice(-5000), targetOldCommit, sourceCommit, conflictFiles, updatedAt: at };
    thread.summary.lastOutputSnippet = `${phase}: ${error.slice(-5000)}`; thread.updatedAt = at; this.store.upsertTask(thread);
    const file = this.transactionPath(threadId);
    this.writeTransaction(file, { threadId, sourceCommit, targetOldCommit, phase, error, conflictFiles, at: new Date().toISOString() });
    const timer = setTimeout(() => {
      const latest = this.store.getTask(threadId);
      if (latest && (latest.status === 'reviewing' || (latest.status === 'merging' && latest.worktree?.mergeResolutionPath === resolutionPath))) this.requestReview?.(latest);
    }, 0);
    timer.unref();
  }

  private async finishMerged(threadId: string, commit: string, targetOldCommit?: string, targetPath?: string, validation?: NonNullable<DelegationThread['worktree']>['validation'], mergedCommit = commit): Promise<void> {
    const current = this.store.getTask(threadId); if (!current?.worktree) return;
    const thread = structuredClone(current);
    const worktree = thread.worktree!;
    const tx = this.readTransaction(this.transactionPath(threadId));
    const stashRestore = tx?.stashToken ? { status: tx.stashCleanupUncertain ? 'stash_cleanup_uncertain' : tx.stashRetained && tx.phase === 'stash_restored' ? 'stash_restored_retained' : tx.phase, sha: tx.stashSha, error: tx.stashRestoreError } : undefined;
    if (thread.mergeControl) thread.mergeControl = { ...thread.mergeControl, revision: thread.mergeControl.revision + 1, failureFingerprint: undefined, consecutiveRetries: 0 };
    if (this.attemptLogs.has(threadId) && targetOldCommit && !(thread.mergeHistory || []).some((entry) => entry.mergedCommit === mergedCommit)) {
      const repo = requireDeliveryTarget(worktree).repositoryPath;
      const changedFiles = (this.isMergeStillAllowed(threadId, worktree.approvedCommit || '')
        ? await this.loggedGit(threadId, repo, ['diff', '--name-status', `${targetOldCommit}..${mergedCommit}`])
        : gitOutput(repo, ['diff', '--name-status', `${targetOldCommit}..${mergedCommit}`])).split(/\r?\n/).filter(Boolean);
      const number = (thread.mergeHistory?.length || 0) + 1;
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number, at: new Date().toISOString(),
        phase: 'merged', result: 'merged', targetRef: requireDeliveryTarget(worktree).ref, targetOldCommit,
        targetCommit: commit, sourceCommit: worktree.approvedSnapshot?.sourceCommit, candidateCommit: worktree.approvedCommit, candidateTree: this.attemptLogs.get(threadId)?.candidateTree || worktree.approvedSnapshot?.tree, mergedCommit, changedFiles, exitCode: 0, attemptId: this.attemptLogs.get(threadId)?.id }];
    }
    if (stashRestore && !(thread.mergeHistory || []).some((entry) => entry.phase === 'stash_restore' && entry.attemptId === this.attemptLogs.get(threadId)?.id)) {
      thread.mergeHistory = [...(thread.mergeHistory || []), { id: crypto.randomUUID(), number: (thread.mergeHistory?.length || 0) + 1,
        at: new Date().toISOString(), phase: 'stash_restore', result: stashRestore.status, error: stashRestore.error, attemptId: this.attemptLogs.get(threadId)?.id }];
    }
    thread.status = 'completed'; worktree.integration = { ...worktree.integration, phase: 'merged', targetOldCommit, targetCommit: commit, targetPath, mergedCommit, stashRecoveryPending: false, stashRecoveryLockHeld: false, stashRestore, updatedAt: new Date().toISOString() };
    if (worktree.deliveryTarget) worktree.deliveryTarget.checkoutPath = targetPath;
    if (validation) worktree.validation = validation;
    worktree.mergePending = false; thread.updatedAt = new Date().toISOString();
    if (thread.execution?.version === 2) {
      const expected = thread.execution;
      const expectedRunId = expected.run?.id;
      if (!expectedRunId || expected.phase !== 'merge') throw new Error(`Cannot finalize merged task ${threadId} without its v2 merge run.`);
      for (let attempt = 0; attempt < 3; attempt++) {
        const fresh = this.store.reloadTask(threadId);
        if (fresh?.execution?.version !== 2 || fresh.execution.run?.id !== expectedRunId ||
            fresh.execution.operationId !== expected.operationId || fresh.execution.generation !== expected.generation ||
            fresh.execution.phase !== 'merge' || fresh.status !== 'merging' || fresh.cleanup) {
          throw new Error(`Cannot finalize merged task ${threadId}: v2 merge identity changed.`);
        }
        try {
          await this.store.transitionExecution(threadId, fresh.execution.revision, (current) => {
            if (current.execution?.version !== 2 || current.execution.run?.id !== expectedRunId ||
                current.execution.operationId !== expected.operationId || current.execution.generation !== expected.generation ||
                current.execution.phase !== 'merge' || current.status !== 'merging' || current.cleanup) {
              throw new Error(`Cannot finalize merged task ${threadId}: v2 merge identity changed.`);
            }
            current.status = 'completed';
            current.mergeControl = thread.mergeControl;
            current.mergeHistory = thread.mergeHistory;
            const latestWorktree = current.worktree!;
            latestWorktree.integration = { ...latestWorktree.integration!, phase: 'merged', targetOldCommit, targetCommit: commit, targetPath, mergedCommit,
              stashRecoveryPending: false, stashRecoveryLockHeld: false, stashRestore, updatedAt: thread.updatedAt };
            if (latestWorktree.deliveryTarget) latestWorktree.deliveryTarget.checkoutPath = targetPath;
            if (validation) latestWorktree.validation = validation;
            latestWorktree.mergePending = false;
          });
          break;
        } catch (error: any) {
          if (attempt === 2 || !String(error?.message || error).includes('revision mismatch')) throw error;
        }
      }
    } else this.store.upsertTask(thread);
  }

  /** True when the target ref contains a candidate whose ledger write is pending. */
  public async hasLandedCandidate(threadId: string): Promise<boolean> {
    const tx = this.readTransaction(this.transactionPath(threadId));
    const thread = this.store.getTask(threadId);
    if (!tx?.candidateCommit || !thread?.worktree?.deliveryTarget) return false;
    try {
      const run = (args: string[]) => Promise.resolve(gitOutput(thread.worktree!.repositoryPath!, args));
      const head = await run(['rev-parse', '--verify', `${thread.worktree.deliveryTarget.ref}^{commit}`]);
      if (head === tx.candidateCommit) return true;
      try { await run(['merge-base', '--is-ancestor', tx.candidateCommit, head]); return true; } catch { return false; }
    } catch { return false; }
  }

  private transactionPath(threadId: string): string { return path.join(this.storageDir, 'merge-transactions', `${threadId}.json`); }
  private readTransaction(file: string): any { try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return undefined; } }
  private writeTransaction(file: string, value: unknown): void {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
    try {
      const fd = fs.openSync(tmp, 'wx', 0o600);
      try { fs.writeFileSync(fd, JSON.stringify(value, null, 2)); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      fs.renameSync(tmp, file);
      if (process.platform !== 'win32') { const dirFd = fs.openSync(path.dirname(file), 'r'); try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); } }
    } finally { try { fs.unlinkSync(tmp); } catch {} }
  }
}
