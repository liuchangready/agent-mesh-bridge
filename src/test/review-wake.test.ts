import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
const childProcess = require('child_process') as typeof import('child_process');

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
import { NativeSessionCompactor } from '../bridge/native-session-compactor';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';

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

  test('a dedicated reviewer wake does not launch after prepare pauses an unknown native operation', async () => {
    const { dir, store, manager } = setup();
    const sessionId = '4ad2498c-215f-4b92-a172-2af983c691f1';
    const thread = makeThread({
      threadId: 'thread_unknown_native_review', reviewFlow: 'dedicated',
      reviewBootstrap: { phase: 'confirmed', attempts: 1 },
      reviewerSessionId: sessionId, reviewerSessionCaptured: true, callerSessionId: sessionId,
      nativeSessionCompaction: { reviewer: { sessionId, lastOperationId: 'unknown-op', revision: 2, turnsSinceCompaction: 0 } },
    });
    thread.rounds[0].nativeSessionCompactions = [{
      id: 'unknown-op', role: 'reviewer', agent: 'codex', sessionId, reason: 'turn_budget',
      phase: 'unknown', revision: 2, startedAt: new Date().toISOString(), recoveryPath: 'unused', logPath: 'unused',
    } as any];
    store.upsertTask(thread);
    let launches = 0;
    (manager as any).launchCaller = () => { launches++; return true; };
    const stored = store.getTask(thread.threadId)!;
    assert.equal(await manager.notifyCaller(stored, stored.rounds[0]), false);
    assert.equal(store.getTask(thread.threadId)!.status, 'paused');
    assert.equal(launches, 0);
    assert.equal(new TaskExecutionRegistry(dir).getOwner(thread.threadId), undefined);
    store.dispose();
  });

  test('a reviewer tail process keeps the native session lease after its role is handed off', async () => {
    const { store, manager } = setup();
    const sessionId = '4ad2498c-215f-4b92-a172-2af983c691f1';
    const first = makeThread({ threadId: 'thread_live_reviewer_tail_1', reviewFlow: 'dedicated',
      reviewBootstrap: { phase: 'confirmed', attempts: 1 }, reviewerSessionId: sessionId,
      reviewerSessionCaptured: true, callerSessionId: sessionId, reviewerModel: 'gpt-6.1-pro' });
    const second = makeThread({ ...first, threadId: 'thread_live_reviewer_tail_2' });
    store.upsertTask(first); store.upsertTask(second);
    let launches = 0;
    let onChildPid: ((pid: number | undefined) => void) | undefined;
    let release: (() => void) | undefined;
    (manager as any).launchCaller = (...args: any[]) => {
      launches++;
      release = args[5];
      onChildPid = args[6];
      onChildPid?.(process.pid);
      return true;
    };
    const storedFirst = store.getTask(first.threadId)!;
    assert.equal(await manager.notifyCaller(storedFirst, storedFirst.rounds[0]), true);
    // Simulate request_rework handing off the reviewer role while the CLI still
    // has a live tail process; only process exit releases the session writer.
    new TaskRoleLock(path.dirname(store.getStoragePath())).release(new TaskRoleLock(path.dirname(store.getStoragePath())).getOwner(first.threadId)!);
    const storedSecond = store.getTask(second.threadId)!;
    assert.equal(await manager.notifyCaller(storedSecond, storedSecond.rounds[0]), false);
    assert.equal(launches, 1);
    assert.equal((manager as any).wakeQueue.pendingCount(sessionId), 1);
    (manager as any).wakeQueue.discard(sessionId, second.threadId);
    onChildPid?.(undefined);
    release?.();
    store.dispose();
  });

  test('reviewer launch binding and exit cleanup failures kill or fail safely without escaping callbacks', async () => {
    const originalSpawn = childProcess.spawn;
    try {
      for (const failure of ['bind', 'clear'] as const) {
        const { store, manager } = setup();
        const sessionId = '4ad2498c-215f-4b92-a172-2af983c691f1';
        const tempWorkspace = fs.mkdtempSync(path.join(os.tmpdir(), 'reviewer-child-workspace-'));
        const thread = makeThread({ threadId: `thread_reviewer_pid_${failure}`, initiatorAgent: 'antigravity',
          reviewFlow: 'dedicated', reviewerSessionId: sessionId, reviewerSessionCaptured: true, workspacePath: tempWorkspace });
        store.upsertTask(thread);
        let launchedPid: number | undefined;
        let actualClose = false;
        let killed = false;
        (childProcess as any).spawn = (_command: string, _args: string[], options: any) => {
          const script = failure === 'bind'
            ? 'process.stdout.write("temporary reviewer child\\n");setInterval(()=>{},1000)'
            : 'process.stdout.write("temporary reviewer child\\n");process.exit(0)';
          const child: any = originalSpawn(process.execPath, ['-e', script], options);
          launchedPid = child.pid;
          const kill = child.kill.bind(child);
          child.kill = (...args: any[]) => { killed = true; return kill(...args); };
          child.once('close', () => { actualClose = true; });
          return child;
        };
        let finish!: () => void;
        const finished = new Promise<void>((resolve) => { finish = resolve; });
        let releaseCalls = 0;
        const onChildPid = (pid: number | undefined) => {
          if (pid) {
            if (failure === 'bind') throw new Error('PID bind rejected');
          } else if (failure === 'clear') throw new Error('PID clear rejected');
        };
        let started = false;
        try { started = (manager as any).launchCaller(thread, sessionId, 'review prompt', thread.rounds[0], 'review', () => {
          releaseCalls++;
          finish();
          if (failure === 'clear') throw new Error('release rejected');
        }, (pid: number | undefined) => {
          onChildPid(pid);
          if (!pid && failure === 'clear') finish();
        }); } finally { await finished; }
        try {
          assert.equal(started, true);
          assert.ok(launchedPid);
          assert.equal(actualClose, true, 'cleanup/finalization follows the child close event');
          assert.equal(killed, failure === 'bind', 'a child whose PID could not be durably bound is stopped');
          assert.equal(releaseCalls, 1);
          const saved = store.getTask(thread.threadId)!;
          assert.ok(saved.reviewWake, 'ownership failures become a recorded failed wake');
          assert.match(saved.reviewWake!.lastError, /Native child ownership failed/);
          assert.notEqual(saved.status, 'completed');
        } finally { store.dispose(); fs.rmSync(tempWorkspace, { recursive: true, force: true }); }
      }
    } finally { (childProcess as any).spawn = originalSpawn; }
  });

  test('dedicated reviewer launch rechecks cancellation, deletion, retargeting, and missing session lease', async () => {
    const originalPrepare = NativeSessionCompactor.prototype.prepareBeforeResume;
    try {
      for (const mode of ['no-lease', 'cancel', 'delete', 'retarget'] as const) {
        const { dir, store, manager } = setup();
        const sessionId = '4ad2498c-215f-4b92-a172-2af983c691f1';
        const thread = makeThread({ threadId: `thread_prepare_${mode}`, reviewFlow: 'dedicated',
          reviewBootstrap: { phase: 'confirmed', attempts: 1 }, reviewerSessionId: sessionId,
          reviewerSessionCaptured: true, callerSessionId: sessionId, reviewerModel: 'gpt-6.1-pro' });
        store.upsertTask(thread);
        NativeSessionCompactor.prototype.prepareBeforeResume = async function (threadId: string) {
          if (mode === 'cancel') new CancellationRegistry(dir).request(threadId, 'pause', 'test cancellation');
          if (mode === 'delete') store.deleteTask(threadId);
          if (mode === 'retarget') {
            const fresh = store.getTask(threadId)!;
            fresh.reviewerSessionId = '9203c3bf-62c6-4aef-a661-2e164d39c10e';
            fresh.callerSessionId = fresh.reviewerSessionId;
            store.upsertTask(fresh);
          }
          return {};
        } as any;
        let launches = 0;
        (manager as any).launchCaller = () => { launches++; return true; };
        const candidate = store.getTask(thread.threadId)!;
        assert.equal(await manager.notifyCaller(candidate, candidate.rounds[0]), false, mode);
        assert.equal(launches, 0, mode);
        assert.equal(new TaskRoleLock(dir).getOwner(thread.threadId), undefined, `role lease released: ${mode}`);
        assert.equal(new TaskExecutionRegistry(dir).getOwner(thread.threadId), undefined, `execution registration released: ${mode}`);
        assert.equal((manager as any).wakeQueue.isBusy(sessionId), false, `wake slot released: ${mode}`);
        store.dispose();
      }
    } finally { NativeSessionCompactor.prototype.prepareBeforeResume = originalPrepare; }
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

  test('a clean exit without a ledger verdict is a retryable no-decision', () => {
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
      { exitCode: 0, output: 'review complete: approved, no permission needed' },
    );

    assert.ok(cleared);
    assert.match(cleared!.lastError, /REVIEW_NO_DECISION/);
    assert.strictEqual(cleared!.attempts, 2);
    assert.strictEqual(store.getTask(thread.threadId)!.status, 'waiting_review');
    assert.strictEqual(store.getTask(thread.threadId)!.reviewWake!.attempts, 2);
    store.dispose();
  });

  test('clears the round wake only after a recorded approval, rework, or park', () => {
    const { store, manager } = setup();
    const wake = {
      round: 1,
      attempts: 2,
      lastAttemptAt: new Date().toISOString(),
      lastError: 'prior timeout',
      autoRetryAttempts: 1,
    };

    const approvedThread = makeThread({ threadId: 'thread_wake_approved', reviewWake: wake });
    store.upsertTask(approvedThread);
    const approved = store.getTask(approvedThread.threadId)!;
    approved.status = 'completed';
    store.upsertTask(approved);
    manager.finalizeWake(approvedThread, approvedThread.rounds[0], 'caller-session-1', {
      exitCode: 1,
      output: 'late error',
    });
    assert.strictEqual(store.getTask(approvedThread.threadId)!.status, 'completed');
    assert.strictEqual(store.getTask(approvedThread.threadId)!.reviewWake, undefined);

    const reworkThread = makeThread({ threadId: 'thread_wake_rework', reviewWake: wake });
    reworkThread.status = 'waiting_review';
    reworkThread.currentRound = 2;
    reworkThread.rounds.push({ ...reworkThread.rounds[0], round: 2, type: 'rework' });
    reworkThread.reviewWake = {
      round: 2,
      attempts: 1,
      lastAttemptAt: new Date().toISOString(),
      lastError: 'new round failure',
      autoRetryAttempts: 0,
    };
    store.upsertTask(reworkThread);
    manager.finalizeWake(reworkThread, reworkThread.rounds[0], 'caller-session-1', {
      exitCode: 0,
      output: 'approved',
    });
    assert.strictEqual(store.getTask(reworkThread.threadId)!.reviewWake!.round, 2, 'late callback keeps the new round plan');

    const parkedThread = makeThread({ threadId: 'thread_wake_park' });
    parkedThread.reviewBlocked = {
      waitingForThreadId: 'thread_wake_park_dependency',
      reason: 'waiting for dependency',
      at: new Date().toISOString(),
      by: 'caller',
    };
    const dependency = makeThread({ threadId: 'thread_wake_park_dependency', status: 'running' });
    dependency.rounds[0].status = 'running';
    store.upsertTask(dependency);
    store.upsertTask(parkedThread);
    assert.strictEqual(
      manager.finalizeWake(parkedThread, parkedThread.rounds[0], 'caller-session-1', {
        exitCode: 0,
        output: 'parked',
      }),
      undefined,
    );
    const parked = store.getTask(parkedThread.threadId)!;
    assert.strictEqual(parked.currentRound, 1, 'parking does not create a round');
    assert.strictEqual(parked.reviewBlocked?.waitingForThreadId, dependency.threadId);
    assert.strictEqual(parked.reviewWake, undefined);
    store.dispose();
  });

  test('does not rewrite paused, archived, deleted, or failure-notification tasks', () => {
    const { store, manager } = setup();
    const thread = makeThread();
    thread.reviewWake = {
      round: 1,
      attempts: 3,
      lastAttemptAt: new Date().toISOString(),
      lastError: 'keep this record',
      autoRetryAttempts: 2,
    };
    store.upsertTask(thread);

    const paused = store.getTask(thread.threadId)!;
    paused.status = 'paused';
    store.upsertTask(paused);
    manager.finalizeWake(thread, thread.rounds[0], 'caller-session-1', { exitCode: 0 }, 'review');
    assert.strictEqual(store.getTask(thread.threadId)!.status, 'paused');
    assert.strictEqual(store.getTask(thread.threadId)!.reviewWake!.lastError, 'keep this record');

    const archived = store.getTask(thread.threadId)!;
    archived.status = 'completed';
    archived.archivedAt = new Date().toISOString();
    store.upsertTask(archived);
    manager.finalizeWake(thread, thread.rounds[0], 'caller-session-1', { exitCode: 1 }, 'review');
    assert.strictEqual(store.getTask(thread.threadId)!.status, 'completed');

    store.deleteTask(thread.threadId);
    assert.strictEqual(manager.finalizeWake(thread, thread.rounds[0], 'caller-session-1', { exitCode: 1 }), undefined);
    assert.strictEqual(store.getTask(thread.threadId), undefined);

    const failed = makeThread({ threadId: 'thread_review_wake_failure_notice', status: 'failed', reviewWake: thread.reviewWake });
    store.upsertTask(failed);
    manager.finalizeWake(failed, failed.rounds[0], 'caller-session-1', { exitCode: 0 }, 'failure');
    assert.strictEqual(store.getTask(failed.threadId)!.reviewWake!.lastError, 'keep this record');
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

  test('keeps the fifth launch in flight and exhausts only after its recorded outcome', async () => {
    const run = async (verdict: 'none' | 'approve') => {
      const nowMs = Date.parse('2026-10-23T12:00:00.000Z');
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-review-fifth-'));
      const tracker = new RateLimitTracker(dir);
      const store = new TaskStore(dir);
      const manager = new CallbackManager(new AgentDetector(undefined, tracker), dir);
      manager.setTaskStore(store);
      let now = nowMs;
      let active = false;
      let calls = 0;
      let finishWake: (() => void) | undefined;
      (manager as any).launchCaller = (...args: any[]) => {
        calls++;
        active = true;
        finishWake = args[5];
        if (calls === REVIEW_WAKE_MAX_AUTO_RETRIES && verdict === 'approve') {
          const approved = store.getTask('thread_review_wake_sched')!;
          approved.status = 'completed';
          store.upsertTask(approved);
          finishWake?.();
          active = false;
        }
        return true;
      };
      const scheduler = new ReviewWakeScheduler(store, manager, {
        now: () => now,
        isReviewerActive: () => active,
        stallTimeoutMs: 1,
      });
      const thread = makeThread({ autoRetryAt: new Date(now - 1_000).toISOString() });
      thread.callerSessionId = '11111111-1111-4111-8111-111111111111';
      thread.updatedAt = new Date(now - 60 * 60_000).toISOString();
      thread.rounds[0].timestamp = thread.updatedAt;
      store.upsertTask(thread);

      for (let attempt = 1; attempt <= REVIEW_WAKE_MAX_AUTO_RETRIES; attempt++) {
        if (attempt > 1) {
          const due = store.getTask(thread.threadId)!;
          due.reviewWake!.autoRetryAt = new Date(now - 1_000).toISOString();
          store.upsertTask(due);
        }
        assert.strictEqual(await scheduler.runDue(), 1);
        const launched = store.getTask(thread.threadId)!;
        assert.strictEqual(launched.status, verdict === 'approve' && attempt === 5 ? 'completed' : 'waiting_review');
        assert.strictEqual(launched.reviewWake!.autoRetryAttempts, attempt);
        if (attempt < 5) {
          finishWake?.();
          finishWake = undefined;
          active = false;
          manager.finalizeWake(launched, launched.rounds[0], 'caller-session-1', {
            exitCode: 1,
            output: `retry ${attempt} failed`,
          });
        }
        now += 1_000;
      }

      const fifth = store.getTask(thread.threadId)!;
      assert.strictEqual(calls, 5);
      assert.strictEqual(fifth.reviewWake!.gaveUp, false, 'the fifth retry is still in flight');
      assert.strictEqual(await scheduler.runWatchdog(), 0, 'an active final reviewer must not be failed early');
      assert.strictEqual(await scheduler.runDue(), 0, 'there is no sixth automatic launch');

      if (verdict === 'none') {
        finishWake?.();
        finishWake = undefined;
        active = false;
        const result = manager.finalizeWake(fifth, fifth.rounds[0], 'caller-session-1', {
          exitCode: 0,
          output: 'approved',
        });
        assert.ok(result);
        assert.match(result!.lastError, /REVIEW_NO_DECISION/);
        const failed = store.getTask(thread.threadId)!;
        assert.strictEqual(failed.status, 'review_failed');
        assert.strictEqual(failed.reviewWake!.autoRetryAttempts, 5);
        assert.strictEqual(failed.reviewWake!.gaveUp, true);
        assert.strictEqual(await scheduler.runDue(), 0);
        assert.strictEqual(calls, 5);
      } else {
        const approved = store.getTask(thread.threadId)!;
        assert.strictEqual(approved.status, 'completed');
        manager.finalizeWake(fifth, fifth.rounds[0], 'caller-session-1', {
          exitCode: 0,
          output: 'approved',
        });
        assert.strictEqual(store.getTask(thread.threadId)!.reviewWake, undefined);
        manager.finalizeWake(fifth, fifth.rounds[0], 'caller-session-1', {
          exitCode: 1,
          output: 'late failure',
        });
        assert.strictEqual(store.getTask(thread.threadId)!.status, 'completed');
        assert.strictEqual(store.getTask(thread.threadId)!.reviewFailure, undefined);
      }
      scheduler.dispose();
      store.dispose();
    };

    await run('none');
    await run('approve');
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
