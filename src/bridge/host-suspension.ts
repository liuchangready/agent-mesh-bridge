import { performance } from 'perf_hooks';

/**
 * Host suspension guard: tells "the agent went silent" apart from "the host was
 * not running".
 *
 * Closing the lid of a Windows laptop puts the machine into Modern Standby /
 * sleep / hibernate. The extension host AND the agent child are suspended
 * together, but the wall clock keeps moving, so after the resume a round looks
 * like it produced no output for 80+ minutes and gets judged `STREAM_STALLED`
 * even though it had actually finished its work. The very same false positive
 * appears when the OS freezes the extension host (desktop activity moderator),
 * when the debugger pauses the process, or when the machine is swapped out.
 *
 * The guard runs a short heartbeat on a MONOTONIC clock (immune to wall-clock
 * jumps such as NTP corrections) and records every interval where the heartbeat
 * itself was late by more than `thresholdMs`. A late heartbeat is proof that
 * this process was not scheduled, so no chunk could have been read during that
 * window. Consumers subtract those windows from their "silence" / "output gap"
 * measurements instead of blaming the agent.
 */

/** Monotonic milliseconds for duration math (unaffected by wall-clock changes). */
export function monotonicNowMs(): number {
  return performance.now();
}

/** An interval during which the host process was not scheduled. */
export interface SuspensionWindow {
  /** Monotonic start (ms, same clock as {@link monotonicNowMs}). */
  startMs: number;
  /** Monotonic end. */
  endMs: number;
  durationMs: number;
}

export interface HostSuspensionOptions {
  /** Heartbeat interval in ms. Default 1s. */
  tickMs?: number;
  /**
   * How late a heartbeat must be before the lateness is attributed to a host
   * suspension rather than ordinary event-loop jitter. Default 30s: normal
   * load spikes stay far below it, any real sleep/hibernate overshoots it.
   */
  thresholdMs?: number;
  /** Injectable monotonic clock, for tests. */
  monotonicNow?: () => number;
}

/**
 * Detects host suspension windows for one process run.
 *
 * Usage: `start()` right before the child is spawned, call `sync()` from any
 * callback that may run after a resume (chunk arrival, watchdog tick), then
 * `suspendedWithin(from, to)` to discount that time from a measured gap.
 */
export class HostSuspensionGuard {
  private readonly tickMs: number;
  private readonly thresholdMs: number;
  private readonly now: () => number;
  private timer: NodeJS.Timeout | null = null;
  private started = false;
  private lastTick = 0;
  private windows: SuspensionWindow[] = [];

  constructor(options: HostSuspensionOptions = {}) {
    this.tickMs = Math.max(100, options.tickMs ?? 1_000);
    // A heartbeat that is only a couple of ticks late is ordinary event-loop
    // jitter; require a clearly suspended process.
    this.thresholdMs = Math.max(this.tickMs * 2, options.thresholdMs ?? 30_000);
    this.now = options.monotonicNow ?? monotonicNowMs;
  }

  start(): void {
    if (this.started) return;
    this.started = true;
    this.lastTick = this.now();
    this.timer = setInterval(() => this.sync(), this.tickMs);
    // A heartbeat must never be the reason a Node process stays alive.
    this.timer.unref?.();
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.started = false;
  }

  /**
   * Closes the current heartbeat interval, recording a suspension window when
   * it is overdue. Safe to call at any time and from any callback: callers that
   * may run before the overdue heartbeat tick (a chunk arriving right after a
   * resume, for instance) must sync first, or the window would be missed.
   */
  sync(): void {
    if (!this.started) return;
    const now = this.now();
    const late = now - this.lastTick;
    if (late > this.thresholdMs) {
      this.windows.push({ startMs: this.lastTick, endMs: now, durationMs: late });
    }
    this.lastTick = now;
  }

  /** Windows detected so far, in chronological order. */
  get suspensionWindows(): readonly SuspensionWindow[] {
    return this.windows;
  }

  get suspensionCount(): number {
    return this.windows.length;
  }

  /** Milliseconds of `[fromMs, toMs]` that lie inside a detected suspension. */
  suspendedWithin(fromMs: number, toMs: number): number {
    if (!(toMs > fromMs)) return 0;
    let total = 0;
    for (const window of this.windows) {
      const overlap = Math.min(toMs, window.endMs) - Math.max(fromMs, window.startMs);
      if (overlap > 0) total += overlap;
    }
    return total;
  }

  /** Total suspended time recorded so far. */
  totalSuspendedMs(): number {
    return this.windows.reduce((sum, window) => sum + window.durationMs, 0);
  }
}
