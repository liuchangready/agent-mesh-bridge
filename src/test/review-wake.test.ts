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
  applyReviewFailure,
  buildReviewWakeState,
  reopenFailedReview,
} from '../bridge/review-wake';
import { DelegationRound, DelegationThread, isTaskUnfinished } from '../bridge/types';
import { TaskRoleLock } from '../bridge/task-role-lock';

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

  test('retries against the newest completed work round, never an approval round', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs);
    const thread = makeThread({
      round: 6,
      autoRetryAt: new Date(nowMs - 1_000).toISOString(),
    });
    thread.currentRound = 6;
    thread.rounds = [
      {
        round: 1,
        type: 'initial',
        prompt: 'implement',
        status: 'completed',
        durationMs: 1_000,
        timestamp: '2026-10-23T10:10:00.000Z',
        output: 'round 1',
      },
      {
        round: 5,
        type: 'rework',
        prompt: 'round 5',
        status: 'completed',
        durationMs: 1_000,
        timestamp: '2026-10-23T11:00:00.000Z',
        output: 'round 5',
      },
      {
        round: 6,
        type: 'approval',
        prompt: 'approved',
        status: 'completed',
        durationMs: 0,
        timestamp: '2026-10-23T11:30:00.000Z',
      },
    ];
    store.upsertTask(thread);

    try {
      await scheduler.runDue();

      assert.deepStrictEqual(
        launches,
        [{ threadId: 'thread_review_wake_sched', round: 5 }],
        'an approval round is bookkeeping and must never be reviewed',
      );
    } finally {
      scheduler.dispose();
      store.dispose();
    }
  });

  test('does not fire a planned retry while a reviewer is already running', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs, { isReviewerActive: () => true });
    store.upsertTask(makeThread({ autoRetryAt: new Date(nowMs - 1_000).toISOString() }));

    // A live reviewer owns the caller session; a second `resume` of the same
    // native thread is rejected by codex ("no rollout found").
    const launched = await scheduler.runDue();

    assert.strictEqual(launched, 0);
    assert.strictEqual(launches.length, 0);
    scheduler.dispose();
    store.dispose();
  });

  test('does not retry while a sibling task is reviewing the same caller session', async () => {
    const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
    const { store, scheduler, launches } = setup(nowMs, { isReviewerActive: () => false });
    const sharedUuid = '11111111-2222-3333-4444-555555555555';

    // A sibling task spells the same codex thread by its rollout file name.
    const sibling = makeThread();
    sibling.threadId = 'thread_sibling_same_session';
    sibling.callerSessionId = `rollout-2026-10-23T10-00-00-${sharedUuid}`;
    delete sibling.reviewWake;
    store.upsertTask(sibling);

    // The retried task spells it as the bare thread id.
    const retried = makeThread({ autoRetryAt: new Date(nowMs - 1_000).toISOString() });
    retried.threadId = 'thread_review_wake_sched';
    retried.callerSessionId = sharedUuid;
    store.upsertTask(retried);

    const lock = new TaskRoleLock();
    const lease = lock.tryAcquire(sibling.threadId, 'reviewer');
    assert.ok(lease, 'the sibling holds the reviewer role for that session');
    try {
      const launched = await scheduler.runDue();
      assert.strictEqual(launched, 0, 'the busy caller session must not be resumed twice');
      assert.strictEqual(launches.length, 0);
    } finally {
      lock.release(lease);
    }

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
    const stored = store.getTask('thread_review_wake_sched')!;
    assert.strictEqual(stored.reviewWake!.gaveUp, true);
    // Exhausted retries are a terminal review failure, not a silent wait.
    assert.strictEqual(stored.status, 'review_failed');
    assert.strictEqual(stored.reviewFailure?.kind, 'retries_exhausted');
    assert.match(stored.reviewFailure?.reason || '', /429 Too Many Requests/);
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
      // `review_failed` is the visible terminal status, and the caller's own
      // parked reason must survive in the failure record.
      assert.strictEqual(stalled.status, 'review_failed');
      assert.strictEqual(stalled.reviewFailure?.kind, 'unresolvable_park');
      assert.match(stalled.reviewFailure?.reason || '', /blocked on the full batch/);

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
      assert.strictEqual(stalled.status, 'review_failed');
      assert.strictEqual(stalled.reviewFailure?.kind, 'unresolvable_park');
      assert.match(stalled.reviewFailure?.reason || '', /thread_deleted_dependency/);
      scheduler.dispose();
      store.dispose();
    });

    test('heals a pre-existing gave-up wake into the terminal review_failed status', async () => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const { store, scheduler } = setup(nowMs, {
        stallTimeoutMs: 10 * 60_000,
        isReviewerActive: () => false,
      });
      const thread = staleThread(nowMs);
      // Exactly the ledger shape written before `review_failed` existed: the
      // review already gave up, but the task is still `waiting_review` forever.
      thread.reviewWake = {
        round: 1,
        attempts: 1,
        lastAttemptAt: new Date(nowMs - 30 * 60_000).toISOString(),
        lastError: '已挂起但未记录 waitingForThreadId：没有可等待的依赖线程',
        gaveUp: true,
        autoRetryAttempts: 0,
      };
      store.upsertTask(thread);

      assert.strictEqual(await scheduler.runWatchdog(), 1);

      const healed = store.getTask('thread_review_wake_sched')!;
      assert.strictEqual(healed.status, 'review_failed');
      assert.strictEqual(healed.reviewFailure?.kind, 'unresolvable_park');
      assert.match(healed.reviewFailure?.reason || '', /waitingForThreadId/);
      // Idempotent: once terminal, the watchdog does not touch it again.
      assert.strictEqual(await scheduler.runWatchdog(), 0);
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

describe('review_failed lifecycle status', () => {
  const baseThread = (): DelegationThread => ({
    threadId: 'thread_review_failed_1',
    title: 'Review failed task',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'waiting_review',
    currentRound: 3,
    createdAt: '2026-10-23T10:00:00.000Z',
    updatedAt: '2026-10-23T11:00:00.000Z',
    receiverSessionId: 'session-receiver-1',
    callerSessionId: 'caller-session-1',
    workspacePath: os.tmpdir(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 1_000 },
    rounds: [],
  });

  test('records a non-empty cause and releases the task queue slot', () => {
    const thread = baseThread();
    assert.strictEqual(isTaskUnfinished(thread), true, 'waiting_review still owns the slot');

    const changed = applyReviewFailure(thread, {
      kind: 'unresolvable_park',
      reason: '等待外部 Client 控制投影修复\n（没有可等待的 thread）',
      by: 'watchdog',
    });

    assert.strictEqual(changed, true);
    assert.strictEqual(thread.status, 'review_failed');
    assert.strictEqual(thread.reviewFailure?.kind, 'unresolvable_park');
    // The reason is normalized (newlines folded), never dropped.
    assert.match(thread.reviewFailure?.reason || '', /外部 Client 控制投影修复/);
    assert.doesNotMatch(thread.reviewFailure?.reason || '', /\n/);
    assert.strictEqual(thread.reviewFailure?.round, 3);
    assert.match(thread.summary.lastOutputSnippet, /Review failed/);
    assert.strictEqual(isTaskUnfinished(thread), false, 'terminal tasks release the slot');
  });

  test('never downgrades a task that is no longer waiting for review', () => {
    for (const status of ['completed', 'reworking', 'paused', 'queued'] as const) {
      const thread = baseThread();
      thread.status = status;
      assert.strictEqual(
        applyReviewFailure(thread, { kind: 'retries_exhausted', reason: 'late failure' }),
        false,
        `a stale wake failure must not overwrite ${status}`,
      );
      assert.strictEqual(thread.status, status);
      assert.strictEqual(thread.reviewFailure, undefined);
    }
  });

  test('re-opening returns to waiting_review and keeps the failure as history', () => {
    const thread = baseThread();
    applyReviewFailure(thread, { kind: 'retries_exhausted', reason: '唤醒重试耗尽' });

    assert.strictEqual(reopenFailedReview(thread), true);
    assert.strictEqual(thread.status, 'waiting_review');
    assert.strictEqual(thread.reviewWake, undefined);
    assert.strictEqual(thread.reviewFailure?.reason, '唤醒重试耗尽', 'audit history is kept');
    assert.strictEqual(isTaskUnfinished(thread), true, 'a re-opened review owns the slot again');
    assert.strictEqual(reopenFailedReview(thread), false, 'idempotent for non-failed tasks');
  });
});
