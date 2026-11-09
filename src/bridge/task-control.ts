import * as fs from 'fs';
import * as path from 'path';
import { AutonomousRunner } from './autonomous-runner';
import { CancellationRegistry } from './cancellation-registry';
import { TaskExecutionRegistry } from './task-execution-registry';
import {
  NativeSessionCleanupResult,
  cleanupThreadNativeSessions,
  describeNativeSessionCleanup,
} from './native-session-cleanup';
import { TaskStore } from './task-store';
import { removeTaskArtifacts } from './task-summary';
import { removeTaskDataDir } from './task-data-dir';
import { removeTaskWorktree } from './git-worktree';
import { changedFiles, currentHead } from './git-worktree';
import { gitOutput } from './git-worktree';
import { MeshFileLock } from './merge-coordinator';
import { TaskRoleLock } from './task-role-lock';
import { killPidTree } from './platform';
import {
  DelegationRound,
  DelegationThread,
  TaskInterruption,
  appendInterruption,
  findInFlightRound,
  isMergePending,
  isIsolatedTaskWorktree,
} from './types';

export interface TaskControlResult {
  threadId: string;
  status: 'PAUSED' | 'DELETED';
  reason: string;
  message: string;
  /**
   * Receiver-side native conversations removed along with a deleted ledger
   * entry. Absent when the task had no resolvable native session.
   */
  nativeSessions?: NativeSessionCleanupResult;
}

/**
 * Pause/delete control plane shared by the VSCode commands and the MCP server.
 *
 * The method may run in a process that does not own the agent child. It always
 * persists the intended state first, then publishes a cancellation request that
 * the owning runner polls and acts on. The owning runner will additionally
 * finalize the round with the same interruption reason.
 */
export class TaskControlService {
  private readonly taskStore: TaskStore;
  private readonly runner?: AutonomousRunner;
  private readonly cancellationRegistry: CancellationRegistry;
  private readonly executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private readonly sessionCleanup: (thread: DelegationThread) => NativeSessionCleanupResult;
  private readonly mergeLock: MeshFileLock;

  constructor(
    taskStore: TaskStore,
    runner?: AutonomousRunner,
    cancellationRegistry?: CancellationRegistry,
    storageDir?: string,
    sessionCleanup?: (thread: DelegationThread) => NativeSessionCleanupResult,
    mergeLock?: MeshFileLock,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.storageDir = storageDir || path.dirname(taskStore.getStoragePath());
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry(this.storageDir);
    this.executionRegistry = new TaskExecutionRegistry(this.storageDir);
    this.sessionCleanup = sessionCleanup || ((thread) => cleanupThreadNativeSessions(thread));
    this.mergeLock = mergeLock || new MeshFileLock();
  }

  /**
   * Pause a queued/running/reworking task. The current round is marked
   * `paused` and keeps the original prompt so it can be resumed later.
   */
  public pauseTask(threadId: string, reason: string, by: string = 'user'): TaskControlResult {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (thread.status === 'completed') {
      throw new Error(`Cannot pause task ${threadId}: it is already completed.`);
    }
    if (thread.status === 'failed') {
      throw new Error(
        `Cannot pause task ${threadId}: it already failed. Use resume/retry instead.`,
      );
    }
    if (thread.status === 'waiting_review') {
      throw new Error(
        `Cannot pause task ${threadId}: it is waiting for the caller's review, not executing.`,
      );
    }
    if (thread.status === 'review_failed') {
      throw new Error(
        `Cannot pause task ${threadId}: its review already failed (terminal). ` +
          `Use re-review / rework / retry instead.`,
      );
    }

    const normalizedReason = (reason || '').trim() || 'Paused by user';
    const now = new Date().toISOString();
    const interruption: TaskInterruption = {
      kind: 'paused',
      reason: normalizedReason,
      at: now,
      by,
    };

    const round = this.resolveCurrentRound(thread);
    const pausingMerge = thread.status === 'waiting_merge';
    if (thread.status === 'paused') {
      // Idempotent, but still publish cancellation and stop this task's child.
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
      // Registry records are observational; kill only this task's child. Its
      // compatibility reader also checks old execution.lock/leases records.
      this.executionRegistry.killOrphanChild(threadId, true);
      return {
        threadId,
        status: 'PAUSED',
        reason: thread.interruption?.reason || normalizedReason,
        message: `Task ${threadId} is already paused (${thread.interruption?.reason || normalizedReason}).`,
      };
    }

    thread.status = 'paused';
    thread.updatedAt = now;
    thread.summary.lastOutputSnippet = `Paused: ${normalizedReason}`;
    appendInterruption(thread, interruption);
    if (round && !pausingMerge) {
      round.status = 'paused';
      round.interruption = interruption;
    }

    this.taskStore.upsertTask(thread);

    // Publish the stop signal for whichever process owns the child.
    this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
    this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
    this.executionRegistry.killOrphanChild(threadId, true);

    return {
      threadId,
      status: 'PAUSED',
      reason: normalizedReason,
      message:
        `Task ${threadId} paused. Interruption reason: ${normalizedReason}. ` +
        `The round is preserved and can be resumed later.`,
    };
  }

  /**
   * Permanently delete a task. Active children are stopped first; the ledger
   * entry is removed and a tombstone records the deletion reason.
   * Local integration, not MR metadata, decides whether unmerged task output
   * can be discarded without an explicit force.
   */
  public async deleteTask(
    threadId: string,
    reason: string,
    by: string = 'user',
    force: boolean = false,
  ): Promise<TaskControlResult> {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    const hasUnmergedOutput = (candidate: DelegationThread): boolean => !!candidate.worktree && (() => {
      try {
        const worktree = candidate.worktree!;
        const head = currentHead(worktree.path);
        const dirty = changedFiles(worktree.path).length > 0;
        const phase = worktree.integration?.phase;
        if ((phase === 'merged' || phase === 'no_changes') && !dirty) {
          const expected = phase === 'merged' ? worktree.approvedCommit : worktree.baseCommit;
          if (expected && head === expected) return false;
          // A ref may have landed before its ledger write. The transaction is
          // trusted only when the target contains that exact candidate and the
          // task worktree still matches its approved source.
          const txPath = path.join(this.storageDir, 'merge-transactions', `${candidate.threadId}.json`);
          const tx = JSON.parse(fs.readFileSync(txPath, 'utf8'));
          const repo = worktree.repositoryPath || worktree.mainPath;
          const target = gitOutput(repo, ['rev-parse', '--verify', `${worktree.targetRef}^{commit}`]);
          const integrated = tx.candidateCommit && (target === tx.candidateCommit || gitOutput(repo, ['merge-base', '--is-ancestor', tx.candidateCommit, target]) === '');
          if (integrated && head === tx.sourceCommit) return false;
        }
        return dirty || (!!worktree.baseCommit && head !== worktree.baseCommit) ||
          !!worktree.approvedCommit || isMergePending(thread);
      } catch { return true; }
    })();
    const worktreeHasUnmergedOutput = hasUnmergedOutput(thread);
    if (worktreeHasUnmergedOutput && !force) {
      throw new Error(
        `Refusing to delete task ${threadId}: its approved work has not been integrated locally. ` +
          `Wait for integration or pass force: true to discard the unmerged worktree output.`,
      );
    }

    const normalizedReason = (reason || '').trim() || 'Deleted by user';
    const active =
      thread.status === 'running' ||
      thread.status === 'reworking' ||
      thread.status === 'queued' ||
      thread.status === 'paused' ||
      thread.status === 'waiting_merge' ||
      !!findInFlightRound(thread) ||
      !!new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid ||
      this.executionRegistry.listOwners().some((owner) => owner.threadId === threadId && !!owner.childPid);

    if (active) {
      this.cancellationRegistry.request(threadId, 'delete', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'delete', normalizedReason, by);
      this.executionRegistry.killOrphanChild(threadId, true);
      const reviewerChild = new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid;
      if (reviewerChild) { try { killPidTree(reviewerChild); } catch {} }
      thread.status = 'paused';
      thread.updatedAt = new Date().toISOString();
      thread.summary.lastOutputSnippet = `Deleting: ${normalizedReason}`;
      try { this.taskStore.upsertTask(thread); }
      catch (error) { throw new Error(`Could not persist deletion intent for ${threadId}: ${(error as Error).message}`, { cause: error }); }
      await this.waitForThreadChildren(threadId);
    }

    // Serialize final cleanup with ref updates and their durable ledger write.
    if (isIsolatedTaskWorktree(thread.worktree)) {
      const release = await this.mergeLock.acquire();
      if (!release) throw new Error(`Deletion of ${threadId} was cancelled while waiting for integration to stop.`);
      try {
        this.taskStore.loadFromDisk();
        const latest = this.taskStore.getTask(threadId);
        if (latest?.status === 'completed' && latest.worktree?.integration?.phase === 'merged') {
          thread.status = latest.status;
          thread.worktree = latest.worktree;
        }
        if (latest && hasUnmergedOutput(latest) && !force) {
          throw new Error(`Refusing to delete task ${threadId}: its approved work has not been integrated locally. Wait for integration or pass force: true to discard the unmerged worktree output.`);
        }
        this.appendTombstone(thread, normalizedReason, by, undefined, force);
        this.taskStore.deleteTask(threadId);
      } finally { release(); }
    } else {
      this.appendTombstone(thread, normalizedReason, by, undefined, force);
      this.taskStore.deleteTask(threadId);
    }

    // Everything the bridge itself stored for this task: per-round logs, output
    // archives, reviewer logs and a pending bootstrap token. The ledger entry is
    // already gone, so a cleanup failure can never resurrect the task.
    removeTaskArtifacts(threadId, this.storageDir, {
      keepArchive: false,
      targetAgent: thread.targetAgent,
      initiatorAgent: thread.initiatorAgent,
      artifactDir: thread.artifactDir,
    });
    if (thread.worktree) removeTaskWorktree(thread.worktree);
    // The Git worktree is now unregistered; the unified task directory itself
    // (worktree/, logs, Archive/, bootstrap credential) can go as a unit.
    if (thread.artifactDir) removeTaskDataDir(thread.artifactDir, true);

    // Clean up the receiver's native conversation(s) only after the ledger entry
    // is gone: a cleanup failure must never leave a "deleted but still listed"
    // task behind. The cleaner itself never throws, but guard the call site too.
    const nativeSessions = this.cleanupNativeSessions(thread);
    this.appendTombstone(thread, normalizedReason, by, nativeSessions, force);

    const cleanupNote = describeNativeSessionCleanup(nativeSessions);
    return {
      threadId,
      status: 'DELETED',
      reason: normalizedReason,
      message:
        `Task ${threadId} deleted. Reason: ${normalizedReason}.` +
        (cleanupNote ? ` Cleaned: ${cleanupNote}.` : ''),
      nativeSessions,
    };
  }

  private async waitForThreadChildren(threadId: string): Promise<void> {
    const roles = new TaskRoleLock(this.storageDir);
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      this.executionRegistry.killOrphanChild(threadId, true);
      const executionLive = this.executionRegistry.listOwners().some((owner) => owner.threadId === threadId);
      const roleChild = roles.getOwner(threadId)?.childPid;
      let roleLive = false;
      if (roleChild) {
        try { process.kill(roleChild, 0); roleLive = true; } catch (error) { roleLive = (error as NodeJS.ErrnoException).code !== 'ESRCH'; }
      }
      if (!executionLive && !roleLive) return;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error(`Timed out waiting for task ${threadId} processes to exit; its worktree and ledger were retained.`);
  }

  /** Run the injected cleaner, swallowing anything it might still throw. */
  private cleanupNativeSessions(thread: DelegationThread): NativeSessionCleanupResult | undefined {
    try {
      const result = this.sessionCleanup(thread);
      return result.entries.length > 0 ? result : undefined;
    } catch (e: any) {
      console.warn('[TaskControlService] Native session cleanup failed:', e?.message || e);
      return undefined;
    }
  }

  private resolveCurrentRound(thread: DelegationThread): DelegationRound | undefined {
    const inFlight = findInFlightRound(thread);
    if (inFlight) return inFlight;
    return thread.rounds && thread.rounds.length > 0
      ? thread.rounds[thread.rounds.length - 1]
      : undefined;
  }

  private appendTombstone(
    thread: DelegationThread,
    reason: string,
    by: string,
    nativeSessions?: NativeSessionCleanupResult,
    forced: boolean = false,
  ): void {
    try {
      fs.mkdirSync(this.storageDir, { recursive: true });
      const record = {
        threadId: thread.threadId,
        title: thread.title,
        initiatorAgent: thread.initiatorAgent,
        targetAgent: thread.targetAgent,
        statusAtDeletion: thread.status,
        reason,
        by,
        // A forced deletion bypassed the merge gate: the audit trail must show
        // that the worktree output was discarded deliberately, not by accident.
        ...(forced ? { forced: true } : {}),
        at: new Date().toISOString(),
        nativeSessions: nativeSessions
          ? {
              removedFiles: nativeSessions.removedCount,
              sessions: nativeSessions.entries.map((entry) => ({
                agent: entry.agent,
                sessionId: entry.sessionId,
                found: entry.found,
                removed: entry.removed,
                pruned: entry.pruned,
                ...(entry.error ? { error: entry.error } : {}),
              })),
            }
          : undefined,
      };
      const file = path.join(this.storageDir, 'deleted_tasks.log');
      const prior = fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean) : [];
      const retained = prior.filter((line) => { try { return JSON.parse(line).threadId !== thread.threadId; } catch { return true; } });
      const temp = `${file}.${process.pid}.${Date.now()}.tmp`;
      const fd = fs.openSync(temp, 'w');
      try { fs.writeFileSync(fd, `${retained.join('\n')}${retained.length ? '\n' : ''}${JSON.stringify(record)}\n`, 'utf8'); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      fs.renameSync(temp, file);
      if (process.platform !== 'win32') { const dirFd = fs.openSync(this.storageDir, 'r'); try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); } }
    } catch (error) {
      throw new Error(`Could not persist deletion tombstone for ${thread.threadId}: ${(error as Error)?.message || error}`, { cause: error });
    }
  }
}
