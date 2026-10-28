import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import { HostSuspensionGuard, monotonicNowMs } from '../bridge/host-suspension';

describe('HostSuspensionGuard', () => {
  test('monotonic clock is available and does not jump backwards', () => {
    const first = monotonicNowMs();
    const second = monotonicNowMs();
    assert.ok(Number.isFinite(first));
    assert.ok(second >= first);
  });

  test('an on-time heartbeat records no suspension', () => {
    let now = 10_000;
    const guard = new HostSuspensionGuard({
      tickMs: 100,
      thresholdMs: 1_000,
      monotonicNow: () => now,
    });
    guard.start();
    for (let i = 0; i < 10; i++) {
      now += 500;
      guard.sync();
    }
    assert.strictEqual(guard.suspensionCount, 0);
    assert.strictEqual(guard.totalSuspendedMs(), 0);
    assert.strictEqual(guard.suspendedWithin(10_000, now), 0);
    guard.stop();
  });

  test('a late heartbeat becomes an excluded window (the closed-lid case)', () => {
    let now = 1_000;
    const guard = new HostSuspensionGuard({
      tickMs: 100,
      thresholdMs: 30_000,
      monotonicNow: () => now,
    });
    guard.start();
    now += 100;
    guard.sync();
    assert.strictEqual(guard.suspensionCount, 0);

    // The machine sleeps for 83 minutes: the next heartbeat is that late.
    const sleepMs = Math.round(4_990_859);
    now += sleepMs;
    guard.sync();

    assert.strictEqual(guard.suspensionCount, 1);
    assert.strictEqual(guard.totalSuspendedMs(), sleepMs);

    // The whole interval is attributable to the host, and the chunk that ends
    // it (a raw 83-minute gap) contributes nothing to the agent's own gap.
    const lastChunkAt = now - sleepMs;
    assert.strictEqual(guard.suspendedWithin(lastChunkAt, now), sleepMs);
    assert.strictEqual(Math.max(0, now - lastChunkAt - guard.suspendedWithin(lastChunkAt, now)), 0);

    // Only the genuinely active part counts.
    now += 5_000;
    assert.strictEqual(guard.suspendedWithin(now - 5_000, now), 0);
    guard.stop();
  });

  test('overlap math is clamped to the requested interval', () => {
    let now = 1_000;
    const guard = new HostSuspensionGuard({
      tickMs: 100,
      thresholdMs: 1_000,
      monotonicNow: () => now,
    });
    guard.start();
    now = 6_000; // 5s suspension: window [1000, 6000]
    guard.sync();

    assert.strictEqual(guard.suspendedWithin(1_000, 6_000), 5_000);
    assert.strictEqual(guard.suspendedWithin(3_000, 8_000), 3_000);
    assert.strictEqual(guard.suspendedWithin(7_000, 9_000), 0);
    assert.strictEqual(guard.suspendedWithin(6_000, 6_000), 0);
    guard.stop();
  });

  test('a blocked event loop is detected as host suspension', async () => {
    const guard = new HostSuspensionGuard({ tickMs: 50, thresholdMs: 200 });
    guard.start();
    const blockedFrom = Date.now();
    // No timers can run here: this is what a frozen/suspended host looks like.
    while (Date.now() - blockedFrom < 400) {
      /* spin */
    }
    await new Promise((resolve) => setTimeout(resolve, 120));

    assert.ok(guard.suspensionCount >= 1, 'the overdue heartbeat must be detected');
    assert.ok(guard.totalSuspendedMs() >= 250, `recorded ${guard.totalSuspendedMs()}ms`);
    guard.stop();
  });

  test('stop() is idempotent and a stopped guard stops recording', () => {
    let now = 1_000;
    const guard = new HostSuspensionGuard({
      tickMs: 100,
      thresholdMs: 1_000,
      monotonicNow: () => now,
    });
    guard.start();
    guard.stop();
    guard.stop();
    now += 60_000;
    guard.sync();
    assert.strictEqual(guard.suspensionCount, 0);
  });
});
