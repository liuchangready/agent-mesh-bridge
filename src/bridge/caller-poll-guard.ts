/**
 * Keeps a caller from sitting in a polling loop after it dispatches a task.
 *
 * The bridge is asynchronous by contract (AGENTS.md §4): `dispatch_task` returns
 * immediately, the caller ends its turn, and the bridge resumes the caller's
 * native session when the round finishes. In practice agents (Codex notably)
 * keep the turn alive and poll `get_thread_details` / `get_recent_logs` every
 * minute to "watch" the task, burning the user's tokens for information they
 * will receive anyway by callback.
 *
 * Telling them to stop is not enough — the tool reply is the strongest lever the
 * bridge has, so this guard makes repeated reads *uninformative* rather than
 * erroring (an error would break legitimate checks, e.g. a user asking "how is
 * it going?"):
 *
 *  - the first read of a self-dispatched thread at a given progress returns the
 *    full payload;
 *  - an identical second read at the SAME progress returns a short status-only
 *    payload plus the stop instruction;
 *  - any real progress (new round, status change, round finishing) restores the
 *    full payload, so a genuine post-callback read is never starved.
 *
 * Threads the process did not dispatch (e.g. a reviewer inspecting someone
 * else's task) are never affected.
 */

import { DelegationThread } from './types';

/** A cheap fingerprint of "has anything actually happened since last check". */
export function progressSignature(thread: DelegationThread): string {
  const rounds = thread.rounds || [];
  const last = rounds.length > 0 ? rounds[rounds.length - 1] : undefined;
  return [
    thread.status,
    thread.currentRound ?? 0,
    rounds.length,
    last?.round ?? '',
    last?.status ?? '',
    last?.durationMs ?? 0,
    thread.targetAgent,
    thread.targetModel ?? '',
  ].join('|');
}

export interface PollVerdict {
  /** `repeat` means: same thread, same progress, already read once. */
  verdict: 'allow' | 'repeat';
  /** How many times this exact progress state has been read in this process. */
  reads: number;
}

export const STOP_POLLING_INSTRUCTION =
  'STOP POLLING. dispatch_task is asynchronous: end your turn and wait for the bridge to wake ' +
  'your session when the round finishes (success and failure both wake you). Do not call ' +
  'get_thread_details / get_recent_logs / list_recent_tasks to watch progress — this thread has ' +
  'not changed since your last check, so you already have all the information there is.';

export class CallerPollGuard {
  private readonly dispatched = new Map<
    string,
    { signature: string; views: Map<string, number>; at: number }
  >();

  /** Record that this process dispatched the task (starts guarding it). */
  public noteDispatch(threadId: string, signature: string, now: number = Date.now()): void {
    this.dispatched.set(threadId, { signature, views: new Map(), at: now });
  }

  /**
   * Inspect a read of a thread.
   *
   * Reads are counted per *view* (`details`, `logs`, ...): a single inspection
   * that reads details and then logs is legitimate and must not be starved, while
   * a second read of the same view at the same progress is the polling loop.
   */
  public check(threadId: string, signature: string, view: string = 'details'): PollVerdict {
    const entry = this.dispatched.get(threadId);
    if (!entry) return { verdict: 'allow', reads: 0 };

    if (entry.signature !== signature) {
      // Real progress: every view may read the full payload again.
      entry.signature = signature;
      entry.views.clear();
    }

    const reads = (entry.views.get(view) ?? 0) + 1;
    entry.views.set(view, reads);
    return { verdict: reads > 1 ? 'repeat' : 'allow', reads };
  }

  /** The thread is finished with as far as guarding is concerned (approved/deleted). */
  public forget(threadId: string): void {
    this.dispatched.delete(threadId);
  }

  public isGuarded(threadId: string): boolean {
    return this.dispatched.has(threadId);
  }

  /** Short status-only payload for a repeated poll. */
  public static repeatPayload(thread: DelegationThread, reads: number): Record<string, unknown> {
    const rounds = thread.rounds || [];
    const last = rounds.length > 0 ? rounds[rounds.length - 1] : undefined;
    return {
      threadId: thread.threadId,
      status: thread.status,
      currentRound: thread.currentRound,
      lastRound: last ? { round: last.round, status: last.status } : undefined,
      targetAgent: thread.targetAgent,
      targetModel: thread.targetModel,
      repeatPoll: true,
      pollsAtSameProgress: reads,
      nextAction: 'END_TURN_AND_WAIT_FOR_CALLBACK',
      stopInstruction: STOP_POLLING_INSTRUCTION,
      message:
        `No progress since your previous check (poll #${reads} at the same state). ` +
        `You will be woken automatically when the round finishes. End your turn now.`,
    };
  }
}
