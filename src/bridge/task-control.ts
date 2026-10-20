import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AutonomousRunner } from './autonomous-runner';
import { CancellationRegistry } from './cancellation-registry';
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
  private readonly storageDir: string;

  constructor(
    taskStore: TaskStore,
    runner?: AutonomousRunner,
    cancellationRegistry?: CancellationRegistry,
    storageDir?: string,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry(storageDir);
    this.storageDir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
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
      throw new Error(`Cannot pause task ${threadId}: it already failed. Use resume/retry instead.`);
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
   */
  public deleteTask(threadId: string, reason: string, by: string = 'user'): TaskControlResult {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
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
    }

    this.taskStore.deleteTask(threadId);
    // Logs are intentionally kept for audit; the tombstone records why the
    // ledger entry disappeared.
    this.appendTombstone(thread, normalizedReason, by);

    return {
      threadId,
      status: 'DELETED',
      reason: normalizedReason,
      message: `Task ${threadId} deleted. Reason: ${normalizedReason}.`,
    };
  }

  private resolveCurrentRound(thread: DelegationThread): DelegationRound | undefined {
    const inFlight = findInFlightRound(thread);
    if (inFlight) return inFlight;
    return thread.rounds && thread.rounds.length > 0
      ? thread.rounds[thread.rounds.length - 1]
      : undefined;
  }

  private appendTombstone(thread: DelegationThread, reason: string, by: string): void {
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
      };
      fs.appendFileSync(
        path.join(this.storageDir, 'deleted_tasks.log'),
        JSON.stringify(record) + '\n',
        'utf8',
      );
    } catch {}
  }
}
