import { EventEmitter } from 'events';
import * as crypto from 'crypto';
import type { TaskStore } from './task-store';
import type { CallbackManager } from './callback-manager';
import {
  REVIEW_STALL_TIMEOUT_MS,
  REVIEW_WAKE_MAX_AUTO_RETRIES,
  applyReviewFailure,
  buildReviewWakeState,
  isAutoRetryDue,
  noteAutoRetryLaunched,
  planAutoRetry,
  describeReviewWake,
  reviewBlockDependencyId,
} from './review-wake';
import {
  DelegationRound,
  DelegationThread,
  RateLimitInfo,
  ReviewFailure,
  ReviewWakeState,
  latestCompletedWorkRound,
  isReviewerWakeEligible,
  isMergeReview,
  appendInterruption,
} from './types';
import { noteMergeFailure } from './merge-recovery';
import { TaskRoleLock } from './task-role-lock';
import { callerSessionKey } from './caller-wake-queue';

export interface ReviewWakeSchedulerOptions {
  now?: () => number;
  maxTimerMs?: number;
  /** How long a silent `reviewing` may last before it counts as stalled. */
  stallTimeoutMs?: number;
  /** Watchdog cadence; mostly exists so tests do not wait a minute. */
  watchdogIntervalMs?: number;
  /** True while a reviewer process holds this task's role lock. */
  isReviewerActive?: (threadId: string) => boolean;
  /** Production hosts set this to make historical wake state read-only. */
  requireV2Execution?: boolean;
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
 * anything. A task in `reviewing` with no live `reviewer` role lock and no
 * progress for `stallTimeoutMs` is recorded as a timeout so it becomes visible
 * (and retryable) instead of blocking the workspace queue forever.
 *
 * Design constraints borrowed from the bridge invariants:
 *  - best-effort: a failure here must never block the reviewer wake path, so
 *    every ledger access is wrapped;
 *  - the plan lives in the ledger (`thread.reviewWake.autoRetryAt`), so a
 *    VS Code restart resumes it instead of silently dropping the retry;
 *  - a retry only happens while the task is still `reviewing`; any other
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
  private readonly requireV2Execution: boolean;

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
    this.requireV2Execution = options.requireV2Execution === true;
    this.watchdogIntervalMs = options.watchdogIntervalMs ?? 60_000;
    this.isReviewerActive =
      options.isReviewerActive ??
      ((threadId: string) => {
        const execution = this.taskStore.getTask(threadId)?.execution;
        if (execution?.version === 2) return Boolean(execution.run);
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
        if (isMergeReview(thread)) {
          if (!state.autoRetryAt || state.gaveUp || Date.parse(state.autoRetryAt) > now) continue;
          const quota = this.rateLimitOf(thread);
          const reset = quota?.resetsAt ? Date.parse(quota.resetsAt) : NaN;
          if (Number.isFinite(reset) && reset > now) {
            this.persist(thread, planAutoRetry({ ...state, quotaResetsAt: quota!.resetsAt, autoRetryAttempts: 0 }, now));
            continue;
          }
          const round = this.reviewRound(thread);
          if (!round) continue;
          const accepted = await Promise.resolve(this.callbackManager.notifyCaller(thread, round));
          const latest = this.taskStore.getTask(thread.threadId);
          if ((latest && isMergeReview(latest)) && latest.reviewWake?.lastAttemptAt === state.lastAttemptAt) {
            latest.reviewWake = { ...state, autoRetryAt: accepted ? undefined : new Date(now + 60_000).toISOString() };
            this.taskStore.upsertTask(latest);
          }
          if (accepted) launched++;
          continue;
        }
        // The record only describes the review of one round. After a rework the
        // task briefly returns to `reviewing` for a NEW round, and the old
        // failure must not leak into it (nor burn its retry budget).
        if (state.round !== thread.currentRound) {
          this.clear(thread);
          continue;
        }
        // Out of automatic attempts: surface "manual action required" instead of
        // re-arming a timer that can never fire.
        if ((state.autoRetryAttempts || 0) >= REVIEW_WAKE_MAX_AUTO_RETRIES) {
          const planned = state.gaveUp ? state : planAutoRetry(state, now);
          this.persist(thread, planned, {
            kind: 'retries_exhausted',
            reason:
              `检视唤醒自动重试已耗尽（${planned.attempts} 次失败）：` +
              `${planned.lastError || '未知原因'}`,
          });
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
   * A `reviewing` task holds its workspace queue slot forever when the
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
      if (thread.reviewFlow !== 'dedicated') continue;
      if (this.requireV2Execution && thread.execution?.version !== 2) continue;
      try {
        if (isMergeReview(thread) && isReviewerWakeEligible(thread)) {
          if (this.isReviewerActive(thread.threadId)) continue;
          if (thread.reviewBlocked || thread.worktree?.integration?.stashRecoveryPending) continue;
          const history = thread.mergeHistory || [];
          const startedIndex = history.map(event => event.phase === 'reviewer' && event.result === 'started' && !!event.attemptId).lastIndexOf(true);
          const started = startedIndex < 0 ? undefined : history[startedIndex];
          const handoffAfterStart = started && history.slice(startedIndex + 1).some(event =>
            (event.phase === 'decision' && event.result === 'answered') ||
            (['conflict', 'validation_failed', 'blocked', 'snapshot', 'coordinator'].includes(event.phase) && event.result === 'failed'));
          const finished = started && history.some(event => event.phase === 'reviewer' && event.attemptId === started.attemptId && event.result !== 'started');
          if (!started || finished || handoffAfterStart) {
            if ((thread.reviewWake?.autoRetryAt && !thread.reviewWake.gaveUp) || this.reviewAgeMs(thread, now) < this.stallTimeoutMs) continue;
            const lastDecision = [...history].reverse().find(event => event.phase === 'decision' && event.result === 'answered');
            const decisionAfterWake = !!lastDecision && Date.parse(lastDecision.at) >= Date.parse(thread.reviewWake?.lastAttemptAt || '');
            if (thread.reviewWake?.gaveUp && !decisionAfterWake) {
              applyReviewFailure(thread, { kind: 'retries_exhausted', reason: thread.reviewWake.lastError || 'Merge reviewer retries were exhausted.', round: thread.currentRound || 1, by: 'watchdog', wake: thread.reviewWake });
              this.taskStore.upsertTask(thread);
              stalled.push(thread);
              continue;
            }
            const round = this.reviewRound(thread);
            if (!round) {
              applyReviewFailure(thread, { kind: 'retries_exhausted', reason: 'Cannot resume merge review: no completed work round is available.', round: thread.currentRound || 1, by: 'watchdog' });
              this.taskStore.upsertTask(thread);
            } else {
              const state: ReviewWakeState = { kind: 'pending', round: round.round, agent: thread.initiatorAgent,
                attempts: thread.reviewWake?.attempts || 0, autoRetryAttempts: thread.reviewWake?.autoRetryAttempts || 0,
                lastAttemptAt: new Date(now).toISOString(), lastError: '', autoRetryAt: new Date(now).toISOString() };
              this.persist(thread, state);
            }
            stalled.push(thread);
            continue;
          }
          const startedAt = Date.parse(started.at);
          if (!Number.isFinite(startedAt) || now - startedAt < this.stallTimeoutMs) continue;
          const reason = 'Reviewer process disappeared before reporting completion';
          const interruption = { kind: 'watchdog' as const, reason, at: new Date(now).toISOString(), by: 'watchdog' };
          appendInterruption(thread, interruption);
          const budget = noteMergeFailure(thread, 'reviewer', 'WATCHDOG', reason);
          const state = buildReviewWakeState({ round: thread.currentRound || 1, agent: thread.initiatorAgent, kind: 'timeout', error: reason, now });
          if (budget.consecutiveRetries >= 6) { state.autoRetryAt = undefined; state.gaveUp = true; }
          thread.reviewWake = state;
          thread.mergeHistory = [...history, { id: crypto.randomUUID(), attemptId: started.attemptId,
            number: history.length + 1, at: interruption.at, phase: 'reviewer', result: 'failed', error: reason,
            errorCode: 'WATCHDOG', interruption, ...budget }];
          thread.updatedAt = interruption.at;
          this.taskStore.upsertTask(thread);
          stalled.push(thread);
          continue;
        }
        if (thread.status !== 'reviewing') continue;
        if (this.isReviewerActive(thread.threadId)) continue;
        const state = thread.reviewWake;
        if (state) {
          // A record from an older round is stale, not "review in progress".
          if (state.round !== thread.currentRound) {
            this.clear(thread);
            continue;
          }
          // The retry budget is only exhausted once its last launched reviewer
          // has exited. A successful tool handoff updates the task directly;
          // otherwise the callback or this watchdog records the final failure.
          // Heal ledgers written before `review_failed` existed: a wake plan that
          // already gave up means no reviewer is coming. Rows used to stay
          // `reviewing` forever in that shape (the old watchdog skipped any
          // thread that already had a record), which keeps occupying the queue.
          if (state.gaveUp) {
            const healed = applyReviewFailure(thread, {
              kind: (state.autoRetryAttempts || 0) > 0 ? 'retries_exhausted' : 'unresolvable_park',
              reason:
                `检视未取得结论（历史记录迁移）：${state.lastError || '未知原因'}`,
              round: state.round,
              by: 'watchdog',
              wake: state,
            });
            if (healed) {
              try {
                thread.updatedAt = new Date().toISOString();
                this.taskStore.upsertTask(thread);
                stalled.push(thread);
              } catch (err) {
                console.warn(
                  `[ReviewWakeScheduler] Could not heal gave-up review for ${thread.threadId}:`,
                  err,
                );
              }
            }
          } else if ((state.autoRetryAttempts || 0) >= REVIEW_WAKE_MAX_AUTO_RETRIES) {
            this.persist(thread, { ...state, gaveUp: true, autoRetryAt: undefined }, {
              kind: 'retries_exhausted',
              reason:
                `Review produced no verdict after ${state.attempts} failed attempts: ${state.lastError || 'unknown reason'}`,
            });
          }
          continue;
        }
        // A park is only healthy when it names a dependency that still exists
        // and has not finished yet: the dependency driver wakes the caller when
        // that thread reaches a terminal state.
        //
        // Two shapes can never be released by the mesh, and both used to
        // deadlock silently (this watchdog skipped every `reviewBlocked`
        // thread, and the driver needs a resolvable id):
        //   - `blocked: true` with NO `waitingForThreadId` ("waiting for an
        //     external dependency" — there is nothing to watch);
        //   - `waitingForThreadId` naming a thread that is gone from the ledger
        //     (deleted dependency).
        // Convert exactly those into a terminal stall: visible in the tree and
        // detail panel, never auto-retried (a re-wake would just re-block), and
        // with the dead park cleared so "Re-Review" is allowed to run.
        const dependencyId = thread.reviewBlocked ? reviewBlockDependencyId(thread) : undefined;
        const parkedWithoutDependency = !!thread.reviewBlocked && !dependencyId;
        const parkedOnDeletedDependency =
          !!dependencyId && !this.taskStore.getTask(dependencyId);
        const unresolvablePark = parkedWithoutDependency || parkedOnDeletedDependency;
        if (thread.reviewBlocked && !unresolvablePark) continue;
        if (this.reviewAgeMs(thread, now) < this.stallTimeoutMs) continue;

        const minutes = Math.max(1, Math.round(this.stallTimeoutMs / 60_000));
        const next = buildReviewWakeState({
          round: thread.currentRound || 1,
          agent: thread.initiatorAgent,
          kind: 'timeout',
          error: parkedWithoutDependency
            ? `已挂起但未记录 waitingForThreadId：没有可等待的依赖线程，桥接无法自动恢复；` +
              `已转为可人工重新检视的停滞（parked without dependency）`
            : parkedOnDeletedDependency
              ? `已挂起等待 ${dependencyId}，但该依赖线程已不在账本中，桥接无法自动恢复；` +
                `已转为可人工重新检视的停滞（parked on a deleted dependency）`
              : `检视无进展：已等待超过 ${minutes} 分钟且没有活跃的检视进程（reviewer timeout）`,
          now,
        });
        // Clear the park is what makes `notifyCaller` accept the manual
        // re-review. Capture the caller's own reason BEFORE clearing: the park
        // reason is the most useful part of the failure record, and it used to
        // be lost when the dead park was removed.
        const parkReason = thread.reviewBlocked?.reason?.trim();
        if (unresolvablePark) {
          next.autoRetryAt = undefined;
          next.gaveUp = true;
          thread.reviewBlocked = undefined;
        }
        this.persist(
          thread,
          next,
          unresolvablePark
            ? {
                kind: 'unresolvable_park',
                reason: `检视挂起在桥接无法观测的依赖上${
                  parkedOnDeletedDependency ? `（依赖线程 ${dependencyId} 已不存在）` : ''
                }${parkReason ? `：${parkReason}` : '：检视方未记录 waitingForThreadId'}`,
              }
            : undefined,
        );
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
            (!this.requireV2Execution || t.execution?.version === 2) &&
            isReviewerWakeEligible(t) &&
            !!t.reviewWake &&
            t.reviewWake.gaveUp !== true &&
            // A review parked on a dependency is not stalled: its wake is
            // driven by the dependency's completion, not by a timer.
            !t.reviewBlocked &&
            // A reviewer is already running this review. Firing the planned
            // retry now would spawn a second process on the SAME native
            // session, and codex rejects that with "no rollout found".
            !this.isReviewerActive(t.threadId) &&
            // Sibling tasks can share one caller session (the same codex
            // thread). While any of them has a live reviewer, this session is
            // busy: resuming it again is rejected by the CLI.
            !this.isCallerSessionBusy(t),
        );
    } catch {
      return [];
    }
  }

  /**
   * True when a live reviewer of a SIBLING task owns the same caller session.
   *
   * The wake queue is keyed per process; two tasks whose callerSessionId spells
   * the same codex thread differently (`rollout-…-uuid` vs bare `uuid`) used to
   * get separate slots and both resumed it, which codex rejects with
   * "no rollout found for thread id". The role lock is the cross-process truth:
   * if a sibling thread still holds a live reviewer role for that session, this
   * task must wait for it to finish.
   */
  private isCallerSessionBusy(thread: DelegationThread): boolean {
    if (thread.execution?.version === 2) return false;
    const key = callerSessionKey(thread.callerSessionId || '');
    if (!key) return false;
    try {
      const lock = new TaskRoleLock();
      for (const other of this.taskStore.getAllTasks()) {
        if (other.threadId === thread.threadId) continue;
        if (callerSessionKey(other.callerSessionId || '') !== key) continue;
        if (lock.getOwner(other.threadId)?.role === 'reviewer') return true;
      }
    } catch {
      // Best effort: a lock read failure must not block the retry path.
    }
    return false;
  }

  private rateLimitOf(thread: DelegationThread): RateLimitInfo | undefined {
    try {
      return this.callbackManager.getRateLimitTracker().getRateLimitInfo(thread.initiatorAgent);
    } catch {
      return undefined;
    }
  }

  /**
   * The round the reviewer still has to inspect.
   *
   * Always the LATEST completed work round. A stalled `reviewWake.round` is a
   * historical record of when the retry plan was armed; a rework may have
   * completed since then, and reviewing that older round would both log into the
   * wrong `<round>.reviewer.log` and hand the reviewer a stale report.
   */
  private reviewRound(thread: DelegationThread): DelegationRound | undefined {
    return latestCompletedWorkRound(thread);
  }

  private persist(
    thread: DelegationThread,
    state: ReviewWakeState,
    failure?: { kind: ReviewFailure['kind']; reason: string },
  ): void {
    try {
      thread.reviewWake = state;
      if (state.gaveUp) {
        // No automatic attempt left: the review did not succeed and will not
        // retry itself. Make that a first-class terminal status so the queue
        // advances and the cause stays visible, instead of parking the task in
        // `reviewing` forever.
        applyReviewFailure(thread, {
          kind: failure?.kind || 'retries_exhausted',
          reason:
            failure?.reason ||
            `检视未能完成（${state.lastError || '达到自动重试上限'}）`,
          round: state.round,
          by: failure?.kind === 'unresolvable_park' ? 'watchdog' : 'scheduler',
          wake: state,
        });
      } else {
        thread.updatedAt = new Date().toISOString();
      }
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
