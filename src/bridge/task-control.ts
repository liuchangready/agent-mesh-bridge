import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AutonomousRunner } from './autonomous-runner';
import { CancellationRegistry } from './cancellation-registry';
import { ExecutionGate } from './execution-gate';
import {
  NativeSessionCleanupResult,
  cleanupThreadNativeSessions,
  describeNativeSessionCleanup,
} from './native-session-cleanup';
import { TaskStore } from './task-store';
import {
  DelegationRound,
  DelegationThread,
  TaskInterruption,
  appendInterruption,
  findInFlightRound,
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
  private readonly executionGate: ExecutionGate;
  private readonly storageDir: string;
  private readonly sessionCleanup: (thread: DelegationThread) => NativeSessionCleanupResult;

  constructor(
    taskStore: TaskStore,
    runner?: AutonomousRunner,
    cancellationRegistry?: CancellationRegistry,
    storageDir?: string,
    sessionCleanup?: (thread: DelegationThread) => NativeSessionCleanupResult,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry(storageDir);
    this.storageDir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.executionGate = new ExecutionGate(this.storageDir);
    this.sessionCleanup = sessionCleanup || ((thread) => cleanupThreadNativeSessions(thread));
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

    const normalizedReason = (reason || '').trim() || 'Paused by user';
    const now = new Date().toISOString();
    const interruption: TaskInterruption = {
      kind: 'paused',
      reason: normalizedReason,
      at: now,
      by,
    };

    const round = this.resolveCurrentRound(thread);
    if (thread.status === 'paused') {
      // Idempotent, but still publish cancellation in case a stale child lives.
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
      // A host restart drops the in-memory child handle; the execution lease is
      // the only place the real agent PID survives.
      this.executionGate.killOrphanChild(threadId, true);
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
    if (round) {
      round.status = 'paused';
      round.interruption = interruption;
    }

    this.taskStore.upsertTask(thread);

    // Publish the stop signal for whichever process owns the child.
    this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
    this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
    this.executionGate.killOrphanChild(threadId);

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
   * entry is removed and a tombstone (with the reason) is appended to
   * `deleted_tasks.log` so the stop reason is not silently lost.
   *
   * The receiver's native conversations are removed as well (best effort: a
   * conversation that no longer exists is not an error). The caller's own
   * session is never touched — it is shared with later tasks.
   *
   * Merge gate: a task that ran in a temporary worktree and was completed
   * without a recorded merge request is refused by default — deleting it would
   * throw away work that only exists in the throwaway worktree. The caller must
   * record the MR first (the normal path) or pass `force: true` deliberately.
   */
  public deleteTask(
    threadId: string,
    reason: string,
    by: string = 'user',
    force: boolean = false,
  ): TaskControlResult {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (thread.worktree && !thread.mergeRequest && !force) {
      throw new Error(
        `Refusing to delete task ${threadId}: its worktree ${thread.worktree.path} ` +
          `has not been merged back to ${thread.worktree.mainPath} yet. ` +
          `Record the merge request first (record_merge_request with url/branch/commit), ` +
          `or pass force: true to delete anyway — the worktree output would be lost.`,
      );
    }

    const normalizedReason = (reason || '').trim() || 'Deleted by user';
    const active =
      thread.status === 'running' ||
      thread.status === 'reworking' ||
      thread.status === 'queued' ||
      thread.status === 'paused' ||
      !!findInFlightRound(thread);

    if (active) {
      this.cancellationRegistry.request(threadId, 'delete', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'delete', normalizedReason, by);
      this.executionGate.killOrphanChild(threadId, true);
    }

    this.taskStore.deleteTask(threadId);

    // Clean up the receiver's native conversation(s) only after the ledger entry
    // is gone: a cleanup failure must never leave a "deleted but still listed"
    // task behind. The cleaner itself never throws, but guard the call site too.
    const nativeSessions = this.cleanupNativeSessions(thread);
    this.appendTombstone(thread, normalizedReason, by, nativeSessions);

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
      fs.appendFileSync(
        path.join(this.storageDir, 'deleted_tasks.log'),
        JSON.stringify(record) + '\n',
        'utf8',
      );
    } catch {}
  }
}
