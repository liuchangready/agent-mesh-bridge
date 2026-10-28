import { EventEmitter } from 'events';
import type { TaskStore } from './task-store';
import type { CallbackManager } from './callback-manager';
import {
  REVIEW_STALL_TIMEOUT_MS,
  REVIEW_WAKE_MAX_AUTO_RETRIES,
  buildReviewWakeState,
  isAutoRetryDue,
  noteAutoRetryLaunched,
  planAutoRetry,
  describeReviewWake,
} from './review-wake';
import { DelegationRound, DelegationThread, RateLimitInfo, ReviewWakeState } from './types';
import { TaskRoleLock } from './task-role-lock';

export interface ReviewWakeSchedulerOptions {
  now?: () => number;
  maxTimerMs?: number;
  /** How long a silent `waiting_review` may last before it counts as stalled. */
  stallTimeoutMs?: number;
  /** Watchdog cadence; mostly exists so tests do not wait a minute. */
  watchdogIntervalMs?: number;
  /** True while a reviewer process holds this task's role lock. */
  isReviewerActive?: (threadId: string) => boolean;
}

/**
 * Automatic recovery for stalled reviews.
 *
 * When a reviewer wake-up fails because the caller is rate-limited / out of
 * quota, `CallbackManager.finalizeWake` writes the failure (and the provider's
 * reset time) onto the thread. This scheduler is the other half: at the planned
 * moment it re-wakes the same caller in the SAME native session, for the SAME
 * thread and round — never a new task, never a new session.
 *
 * It also runs a watchdog for the stalls nobody reported: a wake whose process
 * vanished with the extension host, or a caller CLI that exits 0 without doing
 * anything. A task in `waiting_review` with no live `reviewer` role lock and no
 * progress for `stallTimeoutMs` is recorded as a timeout so it becomes visible
 * (and retryable) instead of blocking the workspace queue forever.
 *
 * Design constraints borrowed from the bridge invariants:
 *  - best-effort: a failure here must never block the reviewer wake path, so
 *    every ledger access is wrapped;
 *  - the plan lives in the ledger (`thread.reviewWake.autoRetryAt`), so a
 *    VS Code restart resumes it instead of silently dropping the retry;
 *  - a retry only happens while the task is still `waiting_review`; any other
 *    status (approved, reworked, paused, deleted) drops the plan.
 */
export class ReviewWakeScheduler extends EventEmitter {
  private timer: NodeJS.Timeout | null = null;
  private watchdogTimer: NodeJS.Timeout | null = null;
  private disposed = false;
  private readonly now: () => number;
  private readonly maxTimerMs: number;
  private readonly watchdogIntervalMs: number;
  private readonly isReviewerActive: (threadId: string) => boolean;
  private stallTimeoutMs: number;

  constructor(
    private readonly taskStore: TaskStore,
    private readonly callbackManager: CallbackManager,
    options: ReviewWakeSchedulerOptions = {},
  ) {
    super();
    this.now = options.now || (() => Date.now());
    // setTimeout overflows past ~24.8 days; a quota reset is never that far out,
    // but clamping keeps a corrupt timestamp from scheduling an instant loop.
    this.maxTimerMs = options.maxTimerMs ?? 6 * 60 * 60 * 1000;
    this.stallTimeoutMs = options.stallTimeoutMs ?? REVIEW_STALL_TIMEOUT_MS;
    this.watchdogIntervalMs = options.watchdogIntervalMs ?? 60_000;
    this.isReviewerActive =
      options.isReviewerActive ??
      ((threadId: string) => {
        try {
          // getOwner() only reports a LIVE owner, so a killed extension host or
          // a crashed caller shows up as "no reviewer" once its heartbeat stops.
          return !!new TaskRoleLock().getOwner(threadId);
        } catch {
          return false;
        }
      });
  }

  public start(): void {
    this.disposed = false;
    this.reschedule();
    this.startWatchdog();
  }

  public dispose(): void {
    this.disposed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.watchdogTimer) {
      clearTimeout(this.watchdogTimer);
      this.watchdogTimer = null;
    }
  }

  /** Change the stall threshold (settings) without recreating the scheduler. */
  public setStallTimeoutMs(ms: number): void {
    if (!Number.isFinite(ms) || ms < 60_000) return;
    this.stallTimeoutMs = ms;
  }

  public getStallTimeoutMs(): number {
    return this.stallTimeoutMs;
  }

  private startWatchdog(): void {
    if (this.disposed) return;
    if (this.watchdogTimer) clearTimeout(this.watchdogTimer);
    this.watchdogTimer = setTimeout(() => {
      this.watchdogTimer = null;
      void this.runWatchdog().finally(() => this.startWatchdog());
    }, this.watchdogIntervalMs);
    this.watchdogTimer.unref?.();
  }

  /** Re-read the ledger and re-arm the timer (called on any task change). */
  public reschedule(): void {
    if (this.disposed) return;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    let nextAt = Number.POSITIVE_INFINITY;
    for (const thread of this.candidates()) {
      const at = thread.reviewWake?.autoRetryAt ? Date.parse(thread.reviewWake.autoRetryAt) : NaN;
      if (!Number.isFinite(at)) continue;
      nextAt = Math.min(nextAt, at);
    }
    if (!Number.isFinite(nextAt)) return;

    const delay = Math.min(this.maxTimerMs, Math.max(0, nextAt - this.now()));
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.runDue();
    }, delay);
    this.timer.unref?.();
  }

  /**
   * Execute every retry that is due right now.
   *
   * Exposed (and awaitable) for tests; the timer path calls it too. Never throws:
   * a scheduler failure must not take the extension host down with it.
   */
  public async runDue(): Promise<number> {
    if (this.disposed) return 0;
    const now = this.now();
    let launched = 0;

    for (const thread of this.candidates()) {
      const state = thread.reviewWake;
      if (!state) continue;

      try {
        // The record only describes the review of one round. After a rework the
        // task briefly returns to `waiting_review` for a NEW round, and the old
        // failure must not leak into it (nor burn its retry budget).
        if (state.round !== thread.currentRound) {
          this.clear(thread);
          continue;
        }
        // Out of automatic attempts: surface "manual action required" instead of
        // re-arming a timer that can never fire.
        if ((state.autoRetryAttempts || 0) >= REVIEW_WAKE_MAX_AUTO_RETRIES) {
          if (!state.gaveUp) this.persist(thread, planAutoRetry(state, now));
          continue;
        }
        if (!isAutoRetryDue(state, now)) continue;

        // Still cooling down? Nothing will succeed before the reset: move the
        // plan instead of burning an attempt on a guaranteed 429.
        const info = this.rateLimitOf(thread);
        const resetAt = info?.resetsAt ? Date.parse(info.resetsAt) : NaN;
        if (Number.isFinite(resetAt) && resetAt > now) {
          this.persist(thread, planAutoRetry({ ...state, quotaResetsAt: info?.resetsAt }, now));
          continue;
        }

        const round = this.reviewRound(thread);
        if (!round) {
          this.clear(thread);
          continue;
        }

        const planned = noteAutoRetryLaunched({ ...state, quotaResetsAt: undefined }, now);
        this.persist(thread, planned);
        const accepted = await Promise.resolve(this.callbackManager.notifyCaller(thread, round));
        if (accepted) {
          launched++;
          this.emit('retryLaunched', { thread, round, state: planned });
        }
      } catch (err) {
        console.warn(
          `[ReviewWakeScheduler] Retry for ${thread.threadId} failed:`,
          err instanceof Error ? err.message : err,
        );
      }
    }

    this.reschedule();
    return launched;
  }

  /**
   * Detect reviews that died without reporting anything.
   *
   * A `waiting_review` task holds its workspace queue slot forever when the
   * reviewer process disappears without an exit event (extension host crash) or
   * exits cleanly without acting. Neither path has a recorded failure, so the
   * re-review action would stay hidden and nobody would know why the queue
   * stopped. The reviewer role lock is the liveness signal: it is held for as
   * long as a caller process runs, and goes stale on its own when that process
   * dies.
   *
   * Exposed (and awaitable) for tests; the watchdog timer calls it in production.
   *
   * @returns how many stalls were newly recorded.
   */
  public async runWatchdog(): Promise<number> {
    if (this.disposed) return 0;
    const now = this.now();
    let tasks: DelegationThread[];
    try {
      this.taskStore.loadFromDisk();
      tasks = this.taskStore.getAllTasks();
    } catch {
      return 0;
    }

    const stalled: DelegationThread[] = [];
    for (const thread of tasks) {
      try {
        if (thread.status !== 'waiting_review') continue;
        // Parked on a dependency on purpose: no stall, no retry timer.
        if (thread.reviewBlocked) continue;
        const state = thread.reviewWake;
        if (state) {
          // A record from an older round is stale, not "review in progress".
          if (state.round !== thread.currentRound) this.clear(thread);
          continue;
        }
        if (this.isReviewerActive(thread.threadId)) continue;
        if (this.reviewAgeMs(thread, now) < this.stallTimeoutMs) continue;

        const minutes = Math.max(1, Math.round(this.stallTimeoutMs / 60_000));
        const next = buildReviewWakeState({
          round: thread.currentRound || 1,
          agent: thread.initiatorAgent,
          kind: 'timeout',
          error: `检视无进展：已等待超过 ${minutes} 分钟且没有活跃的检视进程（reviewer timeout）`,
          now,
        });
        this.persist(thread, next);
        stalled.push(thread);
      } catch (err) {
        console.warn(
          `[ReviewWakeScheduler] Watchdog skipped ${thread.threadId}:`,
          err instanceof Error ? err.message : err,
        );
      }
    }

    if (stalled.length > 0) {
      this.emit('reviewStalled', { threads: stalled });
      this.reschedule();
    }
    return stalled.length;
  }

  /** Milliseconds since the round under review finished (or the task changed). */
  private reviewAgeMs(thread: DelegationThread, now: number): number {
    const round = this.reviewRound(thread);
    const finishedAt = round ? Date.parse(round.timestamp) + (round.durationMs || 0) : Number.NaN;
    const updatedAt = Date.parse(thread.updatedAt);
    const reference = Math.max(
      Number.isFinite(finishedAt) ? finishedAt : 0,
      Number.isFinite(updatedAt) ? updatedAt : 0,
    );
    return reference > 0 ? Math.max(0, now - reference) : 0;
  }

  /** Threads whose review is stalled and whose plan is still active. */
  private candidates(): DelegationThread[] {    try {
      this.taskStore.loadFromDisk();
      return this.taskStore
        .getAllTasks()
        .filter(
          (t) =>
            t.status === 'waiting_review' &&
            !!t.reviewWake &&
            t.reviewWake.gaveUp !== true &&
            // A review parked on a dependency is not stalled: its wake is
            // driven by the dependency's completion, not by a timer.
            !t.reviewBlocked,
        );
    } catch {
      return [];
    }
  }

  private rateLimitOf(thread: DelegationThread): RateLimitInfo | undefined {
    try {
      return this.callbackManager.getRateLimitTracker().getRateLimitInfo(thread.initiatorAgent);
    } catch {
      return undefined;
    }
  }

  /** The round the reviewer still has to inspect. */
  private reviewRound(thread: DelegationThread): DelegationRound | undefined {
    const target = thread.reviewWake?.round ?? thread.currentRound;
    return (
      (thread.rounds || []).find((r) => r.round === target && r.status === 'completed') ||
      [...(thread.rounds || [])].reverse().find((r) => r.status === 'completed')
    );
  }

  private persist(thread: DelegationThread, state: ReviewWakeState): void {
    try {
      thread.reviewWake = state;
      thread.updatedAt = new Date().toISOString();
      this.taskStore.upsertTask(thread);
      this.emit('planChanged', { threadId: thread.threadId, state });
    } catch (err) {
      console.warn(`[ReviewWakeScheduler] Could not persist plan for ${thread.threadId}:`, err);
    }
  }

  private clear(thread: DelegationThread): void {
    if (!thread.reviewWake) return;
    thread.reviewWake = undefined;
    thread.updatedAt = new Date().toISOString();
    try {
      this.taskStore.upsertTask(thread);
    } catch {}
  }

  /** Human-readable reason for the last stall (UI convenience). */
  public static describe(thread: DelegationThread): string | undefined {
    return thread.reviewWake ? describeReviewWake(thread.reviewWake) : undefined;
  }
}
