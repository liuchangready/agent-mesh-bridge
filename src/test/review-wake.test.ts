import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-review-wake-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { RateLimitTracker } from '../bridge/rate-limit-tracker';
import { CallbackManager } from '../bridge/callback-manager';
import { ReviewWakeScheduler, ReviewWakeSchedulerOptions } from '../bridge/review-wake-scheduler';
import {
  REVIEW_WAKE_MAX_AUTO_RETRIES,
  REVIEW_WAKE_RESET_GRACE_MS,
  buildReviewWakeState,
} from '../bridge/review-wake';
import { DelegationRound, DelegationThread } from '../bridge/types';

describe('Review wake failure policy', () => {
  const now = Date.parse('2026-10-23T12:00:00.000Z');

  test('waits for the provider reset time when quota exhaustion is reported', () => {
    const resetsAt = new Date(now + 10 * 60_000).toISOString();
    const state = buildReviewWakeState({
      round: 2,
      agent: 'codex',
      output: '{"status":429,"message":"slow down"}',
      exitCode: 1,
      quota: { isExhausted: true, resetsAt, reason: '429 Rate Limit Exceeded', lastUpdated: new Date(now).toISOString() },
      now,
    });

    assert.strictEqual(state.quotaExhausted, true);
    assert.strictEqual(state.quotaResetsAt, resetsAt);
    assert.strictEqual(Date.parse(state.autoRetryAt!), now + 10 * 60_000 + REVIEW_WAKE_RESET_GRACE_MS);
    assert.strictEqual(state.gaveUp, false);
  });

  test('backs off when the failure carries no reset time', () => {
    const state = buildReviewWakeState({
      round: 1,
      agent: 'codex',
      output: 'Error: session not found',
      exitCode: 1,
      now,
    });

    assert.strictEqual(state.quotaExhausted, false);
    assert.strictEqual(Date.parse(state.autoRetryAt!) - now, 60_000);
    assert.match(state.lastError, /session not found/);
  });

  test('stops scheduling after the automatic retry budget is spent', () => {
    const state = buildReviewWakeState({
      round: 1,
      agent: 'codex',
      output: 'boom',
      exitCode: 1,
      previous: {
        round: 1,
        attempts: 9,
        lastAttemptAt: new Date(now).toISOString(),
        lastError: 'boom',
        autoRetryAttempts: REVIEW_WAKE_MAX_AUTO_RETRIES,
      },
      now,
    });

    assert.strictEqual(state.gaveUp, true);
    assert.strictEqual(state.autoRetryAt, undefined);
  });
});

describe('CallbackManager records stalled reviewer wakes', () => {
  const setup = () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-review-wake-'));
    const tracker = new RateLimitTracker(dir);
    const detector = new AgentDetector(undefined, tracker);
    const store = new TaskStore(dir);
    const manager = new CallbackManager(detector, dir);
    manager.setTaskStore(store);
    return { dir, tracker, store, manager };
  };

  const makeThread = (overrides: Partial<DelegationThread> = {}): DelegationThread => {
    const round: DelegationRound = {
      round: 1,
      type: 'initial',
      prompt: 'implement the lobby service',
      status: 'completed',
      durationMs: 1_000,
      timestamp: '2026-10-23T11:00:00.000Z',
      output: 'delivered',
    };
    return {
      threadId: 'thread_review_wake_1',
      title: 'Lobby service',
      initiatorAgent: 'codex',
      targetAgent: 'deepseek_harness',
      status: 'waiting_review',
      currentRound: 1,
      createdAt: '2026-10-23T10:00:00.000Z',
      updatedAt: '2026-10-23T11:00:00.000Z',
      receiverSessionId: 'session-receiver-1',
      receiverSessionCaptured: true,
      callerSessionId: 'caller-session-1',
      workspacePath: dirWorkspace(),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 1_000 },
      rounds: [round],
      ...overrides,
    };
  };

  const dirWorkspace = () => {
    // Any stable path works; the ledger does not touch it here.
    return path.join(os.tmpdir(), 'agent-bridge-review-wake-ws');
  };

  test('a 429 that still exits 0 is a failed review, not a delivered one', () => {
    const { store, manager, tracker } = setup();
    const thread = makeThread();
    store.upsertTask(thread);

    const state = manager.finalizeWake(thread, thread.rounds[0], 'caller-session-1', {
      exitCode: 0,
      output: 'stream error: 429 Too Many Requests, retry after 120 seconds',
    });

    assert.ok(state, 'the quota signal must be recorded');
    assert.strictEqual(state!.quotaExhausted, true);
    assert.ok(state!.quotaResetsAt, 'reset time is captured for the scheduler');
    assert.ok(tracker.isExhausted('codex'), 'the caller agent is marked quota-exhausted');

    const stored = store.getTask(thread.threadId)!;
    assert.ok(stored.reviewWake, 'the stall is visible in the ledger');
    assert.strictEqual(stored.reviewWake!.round, 1);
    assert.strictEqual(stored.status, 'waiting_review');
    store.dispose();
  });

  test('a rework starts a fresh review budget instead of inheriting the old one', () => {
    const { store, manager } = setup();
    const thread = makeThread({ currentRound: 2 });
    thread.reviewWake = {
      round: 1,
      attempts: 5,
      lastAttemptAt: new Date().toISOString(),
      lastError: 'old failure',
      autoRetryAttempts: REVIEW_WAKE_MAX_AUTO_RETRIES,
      gaveUp: true,
    };
    const round2: DelegationRound = {
      ...thread.rounds[0],
      round: 2,
      type: 'rework',
      timestamp: '2026-10-23T12:00:00.000Z',
    };
    thread.rounds.push(round2);
    store.upsertTask(thread);

    const state = manager.finalizeWake(thread, round2, 'caller-session-1', {
      exitCode: 1,
      output: 'transport error',
    });

    assert.ok(state);
    assert.strictEqual(state!.round, 2);
    assert.strictEqual(state!.attempts, 1, 'attempts belong to one review, not the task');
    assert.strictEqual(state!.autoRetryAttempts, 0);
    assert.strictEqual(state!.gaveUp, false);
    store.dispose();
  });

  test('a clean exit clears the stall record', () => {
    const { store, manager } = setup();
    const thread = makeThread();
    store.upsertTask(thread);

    manager.finalizeWake(thread, thread.rounds[0], 'caller-session-1', {
      exitCode: 1,
      output: 'transport error',
    });
    assert.ok(store.getTask(thread.threadId)!.reviewWake);

    const cleared = manager.finalizeWake(
      store.getTask(thread.threadId)!,
      thread.rounds[0],
      'caller-session-1',
      { exitCode: 0, output: 'review complete: approved' },
    );

    assert.strictEqual(cleared, undefined);
    assert.strictEqual(store.getTask(thread.threadId)!.reviewWake, undefined);
    store.dispose();
  });
});

describe('ReviewWakeScheduler', () => {
  const makeThread = (state: Partial<DelegationThread['reviewWake']> = {}): DelegationThread => ({
    threadId: 'thread_review_wake_sched',
    title: 'Lobby service',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'waiting_review',
    currentRound: 1,
    createdAt: '2026-10-23T10:00:00.000Z',
    updatedAt: '2026-10-23T11:00:00.000Z',
    receiverSessionId: 'session-receiver-1',
    receiverSessionCaptured: true,
    callerSessionId: 'caller-session-1',
    workspacePath: os.tmpdir(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 1_000 },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'implement',
        status: 'completed',
        durationMs: 1_000,
        timestamp: '2026-10-23T11:00:00.000Z',
        output: 'delivered',
      },
    ],
    reviewWake: {
      round: 1,
      attempts: 1,
      lastAttemptAt: '2026-10-23T11:01:00.000Z',
      lastError: '429 Too Many Requests',
      quotaExhausted: true,
      autoRetryAt: '2026-10-23T11:01:30.000Z',
      autoRetryAttempts: 0,
      ...state,
    },
  });

  const setup = (nowMs: number, extra: Partial<ReviewWakeSchedulerOptions> = {}) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-review-sched-'));
    const tracker = new RateLimitTracker(dir);
    const store = new TaskStore(dir);
    const launches: Array<{ threadId: string; round: number }> = [];
    const fakeCallback = {
      notifyCaller: async (thread: DelegationThread, round: DelegationRound) => {
        launches.push({ threadId: thread.threadId, round: round.round });
        return true;
      },
      getRateLimitTracker: () => tracker,
    } as unknown as CallbackManager;
    let clock = nowMs;
    const scheduler = new ReviewWakeScheduler(store, fakeCallback, {
      now: () => clock,
      ...extra,
    });
    return {
      dir,
      tracker,
      store,
      scheduler,
      launches,
      advance: (ms: number) => {
        clock += ms;
      },
    };
  };

  test('re-wakes the same session once the plan is due, and consumes one attempt', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs);
    store.upsertTask(makeThread({ autoRetryAt: new Date(nowMs - 1_000).toISOString() }));

    const launched = await scheduler.runDue();

    assert.strictEqual(launched, 1);
    assert.deepStrictEqual(launches, [{ threadId: 'thread_review_wake_sched', round: 1 }]);
    const stored = store.getTask('thread_review_wake_sched')!;
    assert.strictEqual(stored.status, 'waiting_review');
    assert.strictEqual(stored.reviewWake!.autoRetryAttempts, 1);
    assert.ok(Date.parse(stored.reviewWake!.autoRetryAt!) > nowMs);
    scheduler.dispose();
    store.dispose();
  });

  test('does not retry while the quota is still cooling down', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, tracker, scheduler, launches } = setup(nowMs);
    tracker.recordExhaustion('codex', 'retry after 600 seconds');
    store.upsertTask(makeThread({ autoRetryAt: new Date(nowMs - 1_000).toISOString() }));

    const launched = await scheduler.runDue();

    assert.strictEqual(launched, 0);
    assert.strictEqual(launches.length, 0);
    const stored = store.getTask('thread_review_wake_sched')!;
    const info = tracker.getRateLimitInfo('codex')!;
    assert.ok(
      Date.parse(stored.reviewWake!.autoRetryAt!) >= Date.parse(info.resetsAt!) - 1_000,
      'the plan moves to the reset window instead of burning an attempt',
    );
    assert.strictEqual(stored.reviewWake!.autoRetryAttempts, 0);
    scheduler.dispose();
    store.dispose();
  });

  test('marks the plan as given up once attempts are exhausted', async () => {    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs);
    store.upsertTask(
      makeThread({
        autoRetryAt: new Date(nowMs - 1_000).toISOString(),
        autoRetryAttempts: REVIEW_WAKE_MAX_AUTO_RETRIES,
      }),
    );

    const launched = await scheduler.runDue();

    assert.strictEqual(launched, 0);
    assert.strictEqual(launches.length, 0);
    assert.strictEqual(store.getTask('thread_review_wake_sched')!.reviewWake!.gaveUp, true);
    scheduler.dispose();
    store.dispose();
  });

  test('drops a failure record that belongs to an older round', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs);
    const thread = makeThread({ autoRetryAt: new Date(nowMs - 1_000).toISOString() });
    // A rework moved the task to round 2; the round-1 failure is stale.
    thread.currentRound = 2;
    thread.reviewWake!.round = 1;
    store.upsertTask(thread);

    const launched = await scheduler.runDue();

    assert.strictEqual(launched, 0);
    assert.strictEqual(launches.length, 0);
    assert.strictEqual(store.getTask('thread_review_wake_sched')!.reviewWake, undefined);
    scheduler.dispose();
    store.dispose();
  });

  describe('stall watchdog', () => {
    const staleThread = (nowMs: number) => {
      const thread = makeThread();
      // A healthy review in progress: no failure record, just an old round.
      delete thread.reviewWake;
      thread.rounds[0].timestamp = new Date(nowMs - 20 * 60_000).toISOString();
      thread.rounds[0].durationMs = 0;
      thread.updatedAt = new Date(nowMs - 20 * 60_000).toISOString();
      return thread;
    };

    test('records a silent review as stalled and then retries it', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler, launches, advance } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      store.upsertTask(staleThread(nowMs));
      let stalledEvents = 0;
      scheduler.on('reviewStalled', () => stalledEvents++);

      assert.strictEqual(await scheduler.runWatchdog(), 1);

      const stalled = store.getTask('thread_review_wake_sched')!;
      assert.strictEqual(stalled.status, 'waiting_review');
      assert.strictEqual(stalled.reviewWake!.kind, 'timeout');
      assert.match(stalled.reviewWake!.lastError, /reviewer timeout/);
      assert.ok(stalled.reviewWake!.autoRetryAt, 'a retry must be planned');
      assert.strictEqual(stalledEvents, 1);

      advance(61_000);
      assert.strictEqual(await scheduler.runDue(), 1);
      assert.deepStrictEqual(launches, [{ threadId: 'thread_review_wake_sched', round: 1 }]);
      scheduler.dispose();
      store.dispose();
    });

    test('leaves a review alone while its reviewer process is alive', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => true,
      });
      store.upsertTask(staleThread(nowMs));

      assert.strictEqual(await scheduler.runWatchdog(), 0);
      assert.strictEqual(store.getTask('thread_review_wake_sched')!.reviewWake, undefined);
      scheduler.dispose();
      store.dispose();
    });

    test('leaves a review parked on a live dependency alone', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      // The dependency the review is parked on still exists and is unfinished:
      // its own completion is what wakes the caller, not a timer here.
      const dependency = makeThread();
      dependency.threadId = 'thread_live_dependency';
      dependency.status = 'running';
      dependency.rounds[0].status = 'running';
      delete dependency.reviewWake;
      store.upsertTask(dependency);

      const thread = staleThread(nowMs);
      thread.reviewBlocked = {
        waitingForThreadId: 'thread_live_dependency',
        at: new Date(nowMs).toISOString(),
        by: 'caller',
      };
      store.upsertTask(thread);

      assert.strictEqual(await scheduler.runWatchdog(), 0);
      const parked = store.getTask('thread_review_wake_sched')!;
      assert.strictEqual(parked.reviewWake, undefined);
      assert.strictEqual(parked.reviewBlocked?.waitingForThreadId, 'thread_live_dependency');
      scheduler.dispose();
      store.dispose();
    });

    test('turns a park without a dependency id into a visible, terminal stall', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler, launches, advance } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      const thread = staleThread(nowMs);
      // What `request_rework(blocked: true)` records when the caller names no
      // sibling: "waiting for an external dependency" — unresolvable by design.
      thread.reviewBlocked = {
        reason: 'blocked on the full batch',
        at: new Date(nowMs).toISOString(),
        by: 'caller',
      };
      store.upsertTask(thread);

      assert.strictEqual(await scheduler.runWatchdog(), 1);

      const stalled = store.getTask('thread_review_wake_sched')!;
      assert.strictEqual(stalled.reviewWake!.kind, 'timeout');
      assert.match(stalled.reviewWake!.lastError, /waitingForThreadId/);
      assert.strictEqual(stalled.reviewWake!.gaveUp, true, 'no auto retry for a dead park');
      assert.strictEqual(stalled.reviewWake!.autoRetryAt, undefined);
      assert.strictEqual(stalled.reviewBlocked, undefined, 'the dead park must be cleared');

      // Terminal: the scheduler must not keep waking a caller that will re-block.
      advance(61_000);
      assert.strictEqual(await scheduler.runDue(), 0);
      assert.deepStrictEqual(launches, []);
      scheduler.dispose();
      store.dispose();
    });

    test('turns a park on a deleted dependency into a visible, terminal stall', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      const thread = staleThread(nowMs);
      thread.reviewBlocked = {
        waitingForThreadId: 'thread_deleted_dependency',
        at: new Date(nowMs).toISOString(),
        by: 'caller',
      };
      store.upsertTask(thread);

      assert.strictEqual(await scheduler.runWatchdog(), 1);

      const stalled = store.getTask('thread_review_wake_sched')!;
      assert.match(stalled.reviewWake!.lastError, /已不在账本/);
      assert.strictEqual(stalled.reviewWake!.gaveUp, true);
      assert.strictEqual(stalled.reviewBlocked, undefined);
      scheduler.dispose();
      store.dispose();
    });

    test('does not fire before the stall threshold', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      const thread = staleThread(nowMs);
      // Completed one minute ago: a review is simply still running.
      thread.rounds[0].timestamp = new Date(nowMs - 60_000).toISOString();
      thread.updatedAt = new Date(nowMs - 60_000).toISOString();
      store.upsertTask(thread);

      assert.strictEqual(await scheduler.runWatchdog(), 0);
      assert.strictEqual(store.getTask('thread_review_wake_sched')!.reviewWake, undefined);
      scheduler.dispose();
      store.dispose();
    });
  });
});
