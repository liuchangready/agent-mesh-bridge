import { AgentId, RateLimitInfo, ReviewWakeState } from './types';

/**
 * Reviewer wake-up failures and their automatic recovery.
 *
 * A delegated round ending in `waiting_review` only means the *receiver* is
 * done. The reviewer (the caller agent) is woken by spawning its CLI in its own
 * native session; that process can fail before it ever calls `approve_task` /
 * `request_rework` — the common case being a 429 or an exhausted quota. The
 * thread would then stay `waiting_review` forever and, because `waiting_review`
 * is an unfinished task, it would keep blocking its workspace queue.
 *
 * This module keeps the decision inputs pure (no timers, no IO) so both the
 * callback manager and the scheduler agree on one policy and it stays testable.
 */

/** Automatic re-wakes allowed for one stalled review before giving up. */
export const REVIEW_WAKE_MAX_AUTO_RETRIES = 5;

/** Wait this long after the provider's reset time before trying again. */
export const REVIEW_WAKE_RESET_GRACE_MS = 20_000;

/** Never retry sooner than this, even when the reported reset is in the past. */
export const REVIEW_WAKE_MIN_DELAY_MS = 30_000;

/**
 * How long a task may sit in `waiting_review` with no live reviewer process
 * before the review is declared stalled.
 *
 * Every wake takes the `reviewer` role lock for the lifetime of the caller
 * process, so "waiting_review + no live lock + older than this" means the review
 * died without reporting anything (extension host crash, silent CLI exit, or a
 * caller session that no longer exists). Ten minutes is well past the usual
 * 30-60s review, and the user can raise it in the settings.
 */
export const REVIEW_STALL_TIMEOUT_MS = 10 * 60_000;

/** Backoff used when the failure carried no usable reset time. */
export const REVIEW_WAKE_BACKOFF_MS = [
  60_000,
  5 * 60_000,
  15 * 60_000,
  30 * 60_000,
  60 * 60_000,
];

export interface WakeFailureInput {
  round: number;
  agent: AgentId;
  /** Distinguishes a failed wake from a watchdog-detected stall. */
  kind?: 'wake_failed' | 'timeout';
  /** The caller process' own output (stdout+stderr, tail). */
  output?: string;
  /** Spawn-level error message when the process never started. */
  error?: string;
  exitCode?: number;
  /** Previous state, so attempts accumulate across failures. */
  previous?: ReviewWakeState;
  /** Set when the output looked like a 429 / quota signal. */
  quota?: RateLimitInfo;
  now?: number;
}

/** First meaningful line of a process output, bounded for the ledger. */
export function summarizeWakeError(input: {
  output?: string;
  error?: string;
  exitCode?: number;
}): string {
  // NUL bytes survive neither terminals nor JSON logs; drop them before slicing.
  const text = (input.error || input.output || '').split('\u0000').join('');
  const line = text
    .split(/[\r\n]+/)
    .map((l) => l.trim())
    .filter((l) => l.length > 0)
    .slice(-1)[0];
  if (line) return line.slice(0, 300);
  return typeof input.exitCode === 'number'
    ? `Caller process exited with code ${input.exitCode} and produced no output`
    : 'Caller process failed to start';
}

/**
 * Build the state recorded on the thread after a failed (or quota-blocked) wake.
 *
 * `autoRetryAt` is planned here as well: when the provider told us when the
 * quota comes back we wait for that moment, otherwise we back off.
 */
export function buildReviewWakeState(input: WakeFailureInput): ReviewWakeState {
  const now = input.now ?? Date.now();
  const previous = input.previous;
  const state: ReviewWakeState = {
    kind: input.kind || 'wake_failed',
    round: input.round,
    attempts: (previous?.attempts || 0) + 1,
    lastAttemptAt: new Date(now).toISOString(),
    lastError: summarizeWakeError(input),
    agent: input.agent,
    exitCode: input.exitCode,
    quotaExhausted: input.quota ? true : previous?.quotaExhausted === true,
    quotaResetsAt: input.quota?.resetsAt ?? previous?.quotaResetsAt,
    quotaResetsAtFormatted: input.quota?.resetsAtFormatted ?? previous?.quotaResetsAtFormatted,
    quotaReason: input.quota?.reason ?? previous?.quotaReason,
    autoRetryAttempts: previous?.autoRetryAttempts || 0,
  };
  return planAutoRetry(state, now);
}

/**
 * Decide when the next automatic attempt happens.
 *
 * Two regimes:
 *  - quota reset known → wake shortly after the reset (the provider told us the
 *    only moment a retry can possibly succeed);
 *  - otherwise → exponential backoff, and give up after
 *    `REVIEW_WAKE_MAX_AUTO_RETRIES` so a permanently broken caller cannot spin.
 *
 * A quota wait does NOT consume an attempt: nothing was tried yet.
 */
export function planAutoRetry(
  state: ReviewWakeState,
  nowMs: number = Date.now(),
): ReviewWakeState {
  const used = state.autoRetryAttempts || 0;
  if (used >= REVIEW_WAKE_MAX_AUTO_RETRIES) {
    return { ...state, autoRetryAt: undefined, autoRetryAttempts: used, gaveUp: true };
  }

  const resetAt = state.quotaResetsAt ? Date.parse(state.quotaResetsAt) : Number.NaN;
  if (Number.isFinite(resetAt)) {
    const target = Math.max(nowMs + REVIEW_WAKE_MIN_DELAY_MS, resetAt + REVIEW_WAKE_RESET_GRACE_MS);
    return {
      ...state,
      autoRetryAt: new Date(target).toISOString(),
      autoRetryAttempts: used,
      gaveUp: false,
    };
  }

  const backoff = REVIEW_WAKE_BACKOFF_MS[Math.min(used, REVIEW_WAKE_BACKOFF_MS.length - 1)];
  return {
    ...state,
    autoRetryAt: new Date(nowMs + Math.max(REVIEW_WAKE_MIN_DELAY_MS, backoff)).toISOString(),
    autoRetryAttempts: used,
    gaveUp: false,
  };
}

/** Record that an automatic attempt was launched (consumes one retry). */
export function noteAutoRetryLaunched(
  state: ReviewWakeState,
  nowMs: number = Date.now(),
): ReviewWakeState {
  const used = (state.autoRetryAttempts || 0) + 1;
  const next = planAutoRetry({ ...state, autoRetryAttempts: used }, nowMs);
  return { ...next, autoRetryAttempts: used };
}

/** Is this state still waiting for its automatic attempt? */
export function isAutoRetryDue(state: ReviewWakeState | undefined, nowMs: number): boolean {
  if (!state || state.gaveUp) return false;
  if ((state.autoRetryAttempts || 0) >= REVIEW_WAKE_MAX_AUTO_RETRIES) return false;
  if (!state.autoRetryAt) return false;
  const at = Date.parse(state.autoRetryAt);
  return Number.isFinite(at) && at <= nowMs;
}

/** Human-readable one-liner used by the task tree and the detail panel. */
export function describeReviewWake(state: ReviewWakeState): string {
  const parts: string[] = [];
  if (state.quotaExhausted) {
    parts.push(
      state.quotaResetsAtFormatted
        ? `配额不足，预计 ${state.quotaResetsAtFormatted} 恢复`
        : '配额不足',
    );
  } else if (state.kind === 'timeout') {
    parts.push('检视卡住（无活跃检视进程）');
  } else {
    parts.push('检视唤醒失败');
  }
  if (state.exitCode !== undefined) parts.push(`退出码 ${state.exitCode}`);
  if (state.lastError) parts.push(state.lastError);
  if (state.gaveUp) {
    parts.push('自动重试已停止，请手动重新检视');
  } else if (state.autoRetryAt) {
    const at = new Date(state.autoRetryAt);
    parts.push(`将于 ${at.toLocaleTimeString()} 自动重新检视`);
  }
  return parts.join(' · ');
}
