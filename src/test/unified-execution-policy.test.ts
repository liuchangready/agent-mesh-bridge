import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as os from 'node:os';

import {
  DEFAULT_EXECUTION_BUDGET_MS,
  AUTO_RETRY_BACKOFFS_MS,
  MAX_AUTO_RETRIES,
} from '../bridge/execution-controller';
import { DelegationThread, ExecutionControl } from '../bridge/types';
import { getProcessBirthId } from '../bridge/platform';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import { ReworkController } from '../bridge/rework-controller';
import { testWorkspace } from './workspace-fixture';
import {
  FastInMemoryLock,
  createMockRunner,
  createThread,
  setupFixture,
} from './unified-execution-fixture';

describe('Unified Execution Policy Tests', () => {
  test('v2 scan makes one merge lock attempt in durable waiting order', async (t) => {
    let now = Date.now();
    const fixture = await setupFixture({ useFastLock: true, wallClockNow: () => now });
    try {
      const waiting = (id: string, waitedMs: number) => {
        const task = createThread(id, 'merge');
        task.execution!.version = 2;
        task.execution!.queue = { reason: 'merge_lock', enteredAt: new Date(now - waitedMs).toISOString() };
        return task;
      };
      const newer = waiting('thread_merge_newer', 500);
      const first = waiting('thread_merge_first', 1_000);
      const blocked = waiting('thread_merge_dependency', 3_000);
      blocked.reviewBlocked = { waitingForThreadId: 'thread_external_dependency', at: blocked.createdAt, by: 'reviewer' };
      const exhausted = waiting('thread_merge_exhausted', 2_000);
      exhausted.execution!.budget.spentActiveMs = exhausted.execution!.budget.limitMs;
      const delayed = waiting('thread_merge_delayed', 4_000);
      delayed.execution!.queue!.notBefore = new Date(now + 120_000).toISOString();
      for (const task of [newer, first, blocked, exhausted, delayed]) fixture.store.upsertTask(task);

      const timers = (fixture.controller as unknown as { backoffTimers: Map<string, NodeJS.Timeout> }).backoffTimers;
      const timer = setTimeout(() => {}, 120_000);
      timer.unref();
      timers.set(delayed.threadId, timer);

      const attempted: string[] = [];
      const lock = (fixture.controller as unknown as { mergeLock: MeshFileLock }).mergeLock;
      t.mock.method(lock, 'acquire', async () => {
        const claims = fixture.store.getAllTasks().filter(task => task.execution?.phase === 'merge' && task.execution.run);
        assert.equal(claims.length, 1);
        assert.equal(claims[0].status, 'queued', 'merging is published only after lock acquisition');
        attempted.push(claims[0].threadId);
        return undefined;
      });

      assert.equal((await fixture.controller.startExecution(blocked.threadId, 'merge')).reason, 'dependency_pending');
      assert.equal((await fixture.controller.startExecution(delayed.threadId, 'merge')).reason, 'not_before');
      assert.equal(timers.get(delayed.threadId), timer, 'an early wake keeps the deadline notification');
      await Promise.all([fixture.controller.scanAndDrain(), fixture.controller.scanAndDrain()]);
      now += 60_000;
      await fixture.controller.scanAndDrain();
      assert.deepEqual(attempted, [first.threadId, first.threadId]);
      const held = fixture.store.getTask(first.threadId)!;
      assert.equal(held.execution!.queue?.enteredAt, first.execution!.queue?.enteredAt);
      assert.equal(held.execution!.attempt, first.execution!.attempt);
      assert.equal(fixture.store.getTask(newer.threadId)?.execution?.revision, newer.execution!.revision);
      await fixture.store.transitionExecution(held.threadId, held.execution!.revision, task => {
        task.execution!.run = { id: 'merge-handshake', step: 'starting', state: 'starting',
          startedAt: new Date(now).toISOString(), owner: { pid: process.pid, birthId: getProcessBirthId(process.pid)! } };
      });
      await fixture.controller.scanAndDrain();
      assert.deepEqual(attempted, [first.threadId, first.threadId], 'a live merge handshake keeps peers from contending');
    } finally {
      await fixture.cleanup();
    }
  });

  test('v2 Agent capacity uses durable runs and preserves queued handoffs', async (t) => {
    const fixture = await setupFixture({ useFastLock: true });
    let finish!: () => void;
    const held = new Promise<void>((resolve) => { finish = resolve; });
    try {
      saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 2, maxConcurrentPerAgent: 1 } }, fixture.dir);
      const task = (id: string, agent: DelegationThread['targetAgent']) => {
        const thread = createThread(id);
        thread.targetAgent = agent;
        thread.execution!.version = 2;
        thread.receiverSessionCaptured = false;
        return thread;
      };
      const occupied = task('thread_slot_live', 'claude_code');
      occupied.status = 'running';
      occupied.execution!.run = { id: 'live-slot', role: 'developer', step: 'starting', state: 'starting',
        startedAt: occupied.createdAt, owner: { pid: process.pid, birthId: getProcessBirthId(process.pid)! } };
      const expired = task('thread_slot_exited', 'codex');
      expired.status = 'running';
      expired.execution!.run = { id: 'exited-slot', role: 'developer', step: 'starting', state: 'starting',
        startedAt: expired.createdAt, owner: { pid: 99_999_995, birthId: 'exited-fixture' } };
      const blocked = task('thread_slot_peer', 'claude_code');
      blocked.execution!.next = { phase: 'develop', round: 1, sourceRunId: 'previous-review' };
      blocked.execution!.budget.spentActiveMs = 12_345;
      blocked.execution!.autoRetriesUsed = 1;
      const independent = task('thread_slot_other_agent', 'codex');
      const overflow = task('thread_slot_overflow', 'deepseek_harness');
      for (const thread of [occupied, expired, blocked, independent, overflow]) fixture.store.upsertTask(thread);
      const legacy = fixture.registry.begin('historical-observation', 1, 'codex');
      t.after(() => fixture.registry.end(legacy));
      t.mock.method(fixture.runner, 'execute', async () => {
        await held;
        return { exitCode: 1, error: 'controlled stop' };
      });

      assert.equal((await fixture.controller.startExecution(blocked.threadId)).reason, 'capacity_limit_agent');
      const queued = fixture.store.getTask(blocked.threadId)!;
      assert.equal(queued.execution!.queue?.reason, 'capacity');
      assert.deepEqual(queued.execution!.next, blocked.execution!.next);
      assert.deepEqual(queued.execution!.budget, blocked.execution!.budget);
      assert.equal(queued.execution!.autoRetriesUsed, 1);
      assert.equal((await fixture.controller.startExecution(independent.threadId)).started, true);
      assert.equal((await fixture.controller.startExecution(overflow.threadId)).reason, 'capacity_limit');
      assert.equal((await fixture.controller.startExecution(blocked.threadId)).started, false);
      const stillQueued = fixture.store.getTask(blocked.threadId)?.execution;
      assert.equal(stillQueued?.revision, queued.execution!.revision, 'still blocked does not write again');
      assert.deepEqual(stillQueued?.queue, queued.execution!.queue);
    } finally {
      finish();
      await fixture.cleanup();
    }
  });

  test('v2 scheduler wakes a dependency-blocked review after its sibling settles', async (t) => {
    const fixture = await setupFixture({ useFastLock: true, standingScanIntervalMs: 60_000 });
    const rework = new ReworkController(fixture.store, fixture.callbackManager);
    try {
      const dependency = createThread('thread_review_dependency');
      dependency.execution!.version = 2;
      dependency.status = 'running';
      const review = createThread('thread_review_parked', 'review');
      review.execution!.version = 2;
      review.reviewerSessionCaptured = false;
      review.status = 'reviewing';
      review.rounds[0].status = 'completed';
      fixture.store.upsertTask(dependency);
      fixture.store.upsertTask(review);
      const wake = t.mock.method(fixture.callbackManager, 'launchReviewer');

      await assert.rejects(rework.blockReview({ threadId: review.threadId, waitingForThreadId: 'missing' }), /not found/);
      await assert.rejects(rework.blockReview({ threadId: review.threadId, waitingForThreadId: review.threadId }), /cycle/);
      await rework.blockReview({ threadId: review.threadId, waitingForThreadId: dependency.threadId });
      const parked = fixture.store.reloadTask(review.threadId)!;
      assert.equal(parked.reviewBlocked?.waitingForThreadId, dependency.threadId);
      assert.equal(parked.execution?.queue?.reason, 'dependency');
      assert.equal((await fixture.controller.startExecution(review.threadId)).reason, 'dependency_pending');
      await fixture.controller.scanAndDrain();
      assert.equal(wake.mock.callCount(), 0);
      await fixture.store.transitionExecution(dependency.threadId, dependency.execution!.revision, (task) => {
        task.status = 'failed';
      });

      const deadline = Date.now() + 5_000;
      while (!wake.mock.callCount() && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.equal(wake.mock.callCount(), 1);
      assert.equal(fixture.store.getTask(review.threadId)?.reviewBlocked, undefined);
    } finally {
      rework.dispose();
      await fixture.cleanup();
    }
  });

  test('v2 scheduler reports a missing persisted dependency without starting the parked review', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const review = createThread('thread_review_lost_dependency', 'review');
      review.status = 'reviewing';
      review.rounds[0].status = 'completed';
      review.execution!.version = 2;
      review.execution!.queue = { reason: 'dependency' };
      review.reviewBlocked = { waitingForThreadId: 'thread_removed', at: review.createdAt, by: 'reviewer' };
      fixture.store.upsertTask(review);

      await fixture.controller.scanAndDrain();
      const deadline = Date.now() + 5_000;
      let task = fixture.store.getTask(review.threadId);
      while (task?.status !== 'failed' && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 20));
        task = fixture.store.getTask(review.threadId);
      }
      assert.equal(task?.status, 'failed');
      assert.equal(task?.reviewFailure?.kind, 'unresolvable_park');
      assert.match(task?.reviewFailure?.reason || '', /thread_removed/);
      assert.equal(task?.execution?.run, undefined);
      assert.equal(task?.rounds[0].status, 'completed');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 2: Active budget with monotonic tracking; output does not extend budget
  test('Scenario 2: Active time budget is enforced and output does not extend deadline', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s2', 'develop');
      // Set small budget limit: 50ms
      thread.execution!.budget.limitMs = 50;
      fixture.addRunnableThread(thread);

      // Runner hangs longer than budget
      fixture.runner.setOnExecute(async () => {
        await new Promise((r) => setTimeout(r, 100));
        return { exitCode: 0, stdout: 'stream chunk', extractedOutput: 'chunk' };
      });

      const res = await fixture.controller.startExecution('thread_s2', 'develop');
      assert.strictEqual(res.started, true);

      // Wait for execution worker to hit timeout
      const deadline2 = Date.now() + 15_000;
      let fresh = fixture.store.getTask('thread_s2');
      while (fresh?.status !== 'failed' && Date.now() < deadline2) {
        await new Promise((r) => setTimeout(r, 20));
        fresh = fixture.store.getTask('thread_s2');
      }
      assert.strictEqual(fresh?.status, 'failed');
      assert.strictEqual(fresh?.failure?.code, 'TIMEOUT');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 3: Max 2 auto-retries with backoffs 5s / 30s; fingerprint change does not reset count
  test('Scenario 3: Transient error auto-retries max 2 times; error message change preserves retry count', async () => {
    let virtualTime = 1_700_000_000_000;
    const fixture = await setupFixture({
      useFastLock: true,
      retryBackoffsMs: AUTO_RETRY_BACKOFFS_MS,
      wallClockNow: () => virtualTime,
    });
    try {
      const thread = createThread('thread_s3', 'develop');
      fixture.addRunnableThread(thread);

      // 1st attempt: transient failure
      fixture.runner.setNextResult({
        exitCode: 1,
        error: 'connection error: server reset',
        assessment: {
          success: false,
          errorCode: 'NONZERO_EXIT',
          exitCode: 1,
          stream: { chunkCount: 0, stdoutBytes: 0, stderrBytes: 20, hasActualOutput: false, continuous: false, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60000 },
        },
      });

      await fixture.controller.startExecution('thread_s3', 'develop');
      const deadline3_1 = Date.now() + 15_000;
      let fresh = fixture.store.getTask('thread_s3');
      while ((fresh?.status !== 'queued' || fresh?.execution?.autoRetriesUsed !== 1) && Date.now() < deadline3_1) {
        await new Promise((r) => setTimeout(r, 20));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'queued');
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 1);
      assert.strictEqual(fresh?.execution?.queue?.reason, 'backoff');
      assert.ok(fresh?.execution?.queue?.notBefore);

      // Negative assertion: Before 5 seconds elapsed, scanAndDrain will not start the task
      virtualTime += 4_000;
      await fixture.controller.scanAndDrain();
      fresh = fixture.store.getTask('thread_s3');
      assert.strictEqual(fresh?.status, 'queued');
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 1);

      // 2nd attempt with DIFFERENT error fingerprint
      fixture.runner.setNextResult({
        exitCode: 1,
        error: 'network error: socket hang up',
        assessment: {
          success: false,
          errorCode: 'NONZERO_EXIT',
          exitCode: 1,
          stream: { chunkCount: 0, stdoutBytes: 0, stderrBytes: 20, hasActualOutput: false, continuous: false, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60000 },
        },
      });

      // Advance virtual time past the 5s backoff and trigger via scanAndDrain
      virtualTime += 1_000;
      await fixture.controller.scanAndDrain();

      const deadline3_2 = Date.now() + 15_000;
      fresh = fixture.store.getTask('thread_s3');
      while ((fresh?.status !== 'queued' || fresh?.execution?.autoRetriesUsed !== 2) && Date.now() < deadline3_2) {
        await new Promise((r) => setTimeout(r, 20));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'queued');
      // Fingerprint change did NOT reset retry count; it incremented to 2!
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 2);
      assert.strictEqual(fresh?.execution?.queue?.reason, 'backoff');

      // Negative assertion: Before 30 seconds elapsed, scanAndDrain will not start the task
      virtualTime += 25_000;
      await fixture.controller.scanAndDrain();
      fresh = fixture.store.getTask('thread_s3');
      assert.strictEqual(fresh?.status, 'queued');
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 2);

      // 3rd attempt: retries exhausted -> enters terminal failed
      fixture.runner.setNextResult({
        exitCode: 1,
        error: 'connection error again',
        assessment: {
          success: false,
          errorCode: 'NONZERO_EXIT',
          exitCode: 1,
          stream: { chunkCount: 0, stdoutBytes: 0, stderrBytes: 20, hasActualOutput: false, continuous: false, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60000 },
        },
      });

      // Advance virtual time past the 30s backoff and trigger via scanAndDrain
      virtualTime += 5_000;
      await fixture.controller.scanAndDrain();

      const deadline3_3 = Date.now() + 15_000;
      fresh = fixture.store.getTask('thread_s3');
      while (fresh?.status !== 'failed' && Date.now() < deadline3_3) {
        await new Promise((r) => setTimeout(r, 20));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'failed');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 4: Quota wait does not consume autoRetries; unknown quota enters failed(RATE_LIMITED)
  test('Scenario 4: Known quota wait sets queued(quota) without consuming retries; unknown reset enters failed(RATE_LIMITED)', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s4', 'develop');
      thread.execution!.run = {
        id: 'run_test_quota',
        owner: { pid: process.pid, birthId: String(process.pid) },
        step: 'executing',
        state: 'active',
        startedAt: new Date().toISOString(),
      };
      fixture.addRunnableThread(thread);

      // Call handlePhaseOutcome directly with quotaResetsAt
      const runId = 'run_test_quota';
      await (fixture.controller as any).handlePhaseOutcome('thread_s4', 'develop', runId, {
        ok: false,
        code: 'RATE_LIMITED',
        reason: 'Rate limit exceeded',
        quotaResetsAt: new Date(Date.now() + 60_000).toISOString(),
        transient: true,
      });

      let fresh = fixture.store.getTask('thread_s4');
      assert.strictEqual(fresh?.status, 'queued');
      assert.strictEqual(fresh?.execution?.queue?.reason, 'quota');
      // Did NOT consume an auto retry!
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 0);

      // Reset run for 2nd test
      const rev = fresh!.execution!.revision;
      await fixture.store.transitionExecution('thread_s4', rev, (t) => {
        t.execution!.run = {
          id: 'run_test_quota_2',
          owner: { pid: process.pid, birthId: String(process.pid) },
          step: 'executing',
          state: 'active',
          startedAt: new Date().toISOString(),
        };
      });

      // Rate limit without known reset time
      await (fixture.controller as any).handlePhaseOutcome('thread_s4', 'develop', 'run_test_quota_2', {
        ok: false,
        code: 'RATE_LIMITED',
        reason: 'Unknown rate limit',
        transient: false,
      });

      fresh = fixture.store.getTask('thread_s4');
      assert.strictEqual(fresh?.status, 'failed');
      assert.strictEqual(fresh?.failure?.code, 'RATE_LIMITED');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 6: Git sub-steps with empty child don't fail as long as owner alive
  test('Scenario 6: Between Git commands child is temporarily undefined, probeRun returns alive via owner', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s6', 'merge');
      fixture.addRunnableThread(thread);

      const run: ExecutionControl['run'] = {
        id: 'run_6',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        child: undefined, // Between child steps
        step: 'git_checkout',
        state: 'active',
        startedAt: new Date().toISOString(),
      };

      const probe = fixture.controller.probeRun('thread_s6', run);
      assert.strictEqual(probe, 'alive');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 8: Reviewer tool verdict and trailing CLI exit: only advances once; exit without verdict enters NO_VERDICT
  test('Scenario 8: Reviewer exit without tool verdict enters NO_VERDICT failure', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s8', 'review');
      thread.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      fixture.addRunnableThread(thread);

      // Reviewer exits 0 with some output but NEVER called approve_task or request_rework
      fixture.callbackManager.launchReviewer = (async () => ({
        success: true,
        exitCode: 0,
      })) as any;

      const adapterResult = await (fixture.controller as any).executeReviewAdapter('thread_s8', 'run_s8');
      assert.strictEqual(adapterResult.ok, false);
      assert.strictEqual(adapterResult.code, 'NO_VERDICT');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 10: Task decision across develop, review, merge releases locks and preserves budget on answer
  test('Scenario 10: requestTaskDecision transitions to wait_decision and submitTaskDecision resumes with budget preserved', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s10', 'merge');
      thread.execution!.budget.spentActiveMs = 1500;
      fixture.addRunnableThread(thread);

      const decision = await fixture.controller.requestTaskDecision('thread_s10', {
        question: 'Resolve merge conflict in file A?',
        resumePoint: 'retry_merge',
      });

      assert.strictEqual(decision.question, 'Resolve merge conflict in file A?');
      assert.strictEqual(decision.resumePoint, 'retry_merge');

      let fresh = fixture.store.getTask('thread_s10');
      assert.strictEqual(fresh?.status, 'wait_decision');
      assert.strictEqual(fresh?.decision?.id, decision.id);

      // Mock merge coordinator to return success when resumed
      (fixture.controller as any).mergeCoordinator.mergeApprovedAttempt = async () => {
        return { status: 'merged', message: 'OK' };
      };

      // Answering the decision resumes execution immediately and preserves budget!
      await fixture.controller.submitTaskDecision('thread_s10', decision.id, 'Accept incoming changes');

      fresh = fixture.store.getTask('thread_s10');
      assert.strictEqual(fresh?.decisions?.find((d) => d.id === decision.id)?.answer, 'Accept incoming changes');
      assert.strictEqual(fresh?.decision?.answer, 'Accept incoming changes');
      // Preserved spent active budget!
      assert.strictEqual(fresh?.execution?.budget.spentActiveMs, 1500);

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 11: Write failure before spawn prevents spawn; recovered CAS does not re-merge
  test('Scenario 11: Write failure before spawn aborts execution without spawning child', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s11', 'develop');
      fixture.addRunnableThread(thread);

      // Simulate transaction failure before spawn (e.g. disk or revision error)
      fixture.store.transitionExecution = async () => {
        throw new Error('Revision conflict or lock acquisition error');
      };

      const res = await fixture.controller.startExecution('thread_s11', 'develop');
      assert.strictEqual(res.started, false);
      assert.match(res.reason || '', /Revision conflict or lock acquisition error/);

      // No child was spawned!
      assert.strictEqual(fixture.runner.executions.length, 0);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 12: Manual retry starts new generation; resume preserves budget
  test('Scenario 12: Manual retry resets autoRetriesUsed and starts new generation; resume preserves budget', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    let finishRunner: (() => void) | undefined;
    try {
      const thread = createThread('thread_s12', 'develop');
      thread.execution!.generation = 1;
      thread.execution!.autoRetriesUsed = 2;
      thread.execution!.budget.spentActiveMs = 5000;
      fixture.addRunnableThread(thread);

      const runnerBarrier = new Promise<void>((r) => { finishRunner = r; });
      fixture.runner.setOnExecute(async () => {
        await runnerBarrier;
        return { exitCode: 1, error: 'Invalid command arguments' };
      });

      // Resume preserves generation, retries, and spent budget
      await fixture.controller.resumeOperation('thread_s12');
      let fresh = fixture.store.getTask('thread_s12');
      assert.strictEqual(fresh?.execution?.generation, 1);
      assert.strictEqual(fresh?.execution?.budget.spentActiveMs, 5000);

      // A resume keeps its live run protected until the phase stops.
      await assert.rejects(fixture.controller.retryOperation('thread_s12'), /not retryable/);
      finishRunner!();
      const deadline = Date.now() + 5_000;
      while (fixture.store.getTask('thread_s12')?.status !== 'failed' && Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      assert.strictEqual(fixture.store.getTask('thread_s12')?.status, 'failed');

      // Manual retry increments generation and resets retries and spent budget.
      const retryBarrier = new Promise<void>(resolve => { finishRunner = resolve; });
      fixture.runner.setOnExecute(async () => { await retryBarrier; });
      await fixture.controller.retryOperation('thread_s12');
      fresh = fixture.store.getTask('thread_s12');
      assert.strictEqual(fresh?.execution?.generation, 2);
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 0);
      assert.strictEqual(fresh?.execution?.budget.spentActiveMs, 0);

      finishRunner!();
    } finally {
      finishRunner?.();
      await fixture.cleanup();
    }
  });

  // Scenario 14: Legacy tasks without execution record are preserved without synthetic execution
  test('Scenario 14: Legacy tasks without execution field retain legacy status semantics', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const now = new Date().toISOString();
      const legacyThread: DelegationThread = {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + ('thread_legacy_14')),
    callerSessionId: ('reviewer-' + ('thread_legacy_14')),
        threadId: 'thread_legacy_14',
        title: 'Legacy Task',
        initiatorAgent: 'codex',
        targetAgent: 'claude_code',
        status: 'reviewing',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'legacy-sess',
        workspace: testWorkspace(os.tmpdir()),
        summary: { lastPromptSnippet: 'leg', lastOutputSnippet: 'leg', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'initial', prompt: 'legacy', status: 'completed', durationMs: 0, timestamp: now }],
      };

      fixture.store.upsertTask(legacyThread);

      const readBack = fixture.store.getTask('thread_legacy_14');
      assert.strictEqual(readBack?.status, 'reviewing');
      assert.strictEqual(readBack?.execution, undefined);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 16: retryOperation resets generation/failure and resumes; resumeOperation clears paused state
  test('Scenario 16: retryOperation and resumeOperation restore state to queued/running and clear failures', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const task = createThread('thread_s16', 'develop');
      task.status = 'failed';
      task.failure = { stage: 'execution', reason: 'syntax error', code: 'ERROR', at: new Date().toISOString(), round: 1 };
      task.execution = {
        version: 2,
        revision: 1,
        operationId: 'op_s16',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 3,
        autoRetriesUsed: 2,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 5000, maxAutoRetries: 2 },
      };
      fixture.addRunnableThread(task);

      fixture.runner.setNextResult({
        exitCode: 0,
        stdout: 'recovered',
        extractedOutput: 'recovered code',
        assessment: {
          success: true,
          errorCode: 'OK',
          exitCode: 0,
          stream: { chunkCount: 1, stdoutBytes: 14, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 10, gapThresholdMs: 60000 },
        },
      });

      const updated = await fixture.controller.retryOperation('thread_s16');
      assert.strictEqual(updated.failure, undefined);
      assert.strictEqual(updated.execution?.generation, 2);
      assert.strictEqual(updated.execution?.autoRetriesUsed, 0);
      assert.strictEqual(updated.execution?.budget.spentActiveMs, 0);

      const fresh = fixture.store.getTask('thread_s16');
      assert.notStrictEqual(fresh?.status, 'failed');

      // Test pause and resume
      await fixture.controller.pauseExecution('thread_s16', 'user pause');
      const paused = fixture.store.getTask('thread_s16');
      assert.strictEqual(paused?.status, 'paused');
      assert.strictEqual(paused?.interruption?.kind, 'paused');

      fixture.runner.setNextResult({
        exitCode: 0,
        stdout: 'resumed',
        extractedOutput: 'resumed code',
        assessment: {
          success: true,
          errorCode: 'OK',
          exitCode: 0,
          stream: { chunkCount: 1, stdoutBytes: 12, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 10, gapThresholdMs: 60000 },
        },
      });

      const resumed = await fixture.controller.resumeOperation('thread_s16');
      assert.notStrictEqual(resumed.status, 'paused');
      assert.strictEqual(resumed.interruption, undefined);

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 17: User decision submission injects answer into prompt, clears decision, and advances status
  test('Scenario 17: submitTaskDecision injects user answer, clears decision, and resumes execution', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const task = createThread('thread_s17', 'develop');
      fixture.addRunnableThread(task);

      const decision = await fixture.controller.requestTaskDecision('thread_s17', {
        question: 'Should we use TypeScript or JavaScript?',
        resumePoint: 'continue_develop',
      });
      assert.ok(decision.id);
      const waiting = fixture.store.getTask('thread_s17');
      assert.strictEqual(waiting?.status, 'wait_decision');
      assert.strictEqual(waiting?.decision?.id, decision.id);

      fixture.runner.setNextResult({
        exitCode: 0,
        stdout: 'built TS',
        extractedOutput: 'TypeScript implementation',
        assessment: {
          success: true,
          errorCode: 'OK',
          exitCode: 0,
          stream: { chunkCount: 1, stdoutBytes: 10, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 10, gapThresholdMs: 60000 },
        },
      });

      const answered = await fixture.controller.submitTaskDecision('thread_s17', decision.id, 'Use TypeScript');
      assert.notStrictEqual(answered.status, 'wait_decision');
      assert.strictEqual(answered.decision?.answer, 'Use TypeScript');
      assert.strictEqual(answered.decisions?.[0].answer, 'Use TypeScript');
      assert.ok(answered.rounds[0].prompt.includes('[User Decision Answer]:\nUse TypeScript'));

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 18: Anti-stale outcome ignores outdated runId / generation without corrupting task state
  test('Scenario 18: Anti-stale protection prevents older run callbacks from overwriting new run state', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const task = createThread('thread_s18', 'develop');
      fixture.addRunnableThread(task);

      await fixture.controller.startExecution('thread_s18', 'develop');
      const running = fixture.store.getTask('thread_s18');
      const currentRunId = running?.execution?.run?.id;
      assert.ok(currentRunId);

      // Attempt to report outcome for an old fake runId
      await (fixture.controller as any).handlePhaseOutcome('thread_s18', 'develop', 'stale-run-999', {
        ok: false,
        code: 'STALE_ERROR',
        reason: 'Should be ignored',
      });

      const afterStale = fixture.store.getTask('thread_s18');
      assert.strictEqual(afterStale?.execution?.run?.id, currentRunId);
      assert.notStrictEqual(afterStale?.status, 'failed');

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 24: Decisions persist a durable handoff before execution, while direct resume can start immediately.
  test('Scenario 24: a decision queues its handoff before launch and local resume starts directly', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s24', 'develop');
      fixture.addRunnableThread(thread);

      // 1. Request decision -> wait_decision
      const dec = await fixture.controller.requestTaskDecision('thread_s24', {
        question: 'Proceed?',
        resumePoint: 'continue_develop',
      });
      assert.strictEqual(fixture.store.getTask('thread_s24')?.status, 'wait_decision');

      // Track all statuses written to disk
      const recordedStatuses: string[] = [];
      const origPersist = (fixture.store as any).persistThread.bind(fixture.store);
      (fixture.store as any).persistThread = (t: DelegationThread) => {
        recordedStatuses.push(t.status);
        return origPersist(t);
      };

      // 2. Submit the decision and consume the persisted handoff in the resident controller.
      await fixture.controller.submitTaskDecision('thread_s24', dec.id, 'yes proceed');
      const afterSubmit = fixture.store.getTask('thread_s24');
      assert.strictEqual(afterSubmit?.status, 'running');
      assert.strictEqual(recordedStatuses[0], 'queued', `Missing durable handoff in: ${recordedStatuses.join(', ')}`);

      // 3. Pause task
      await fixture.controller.pauseExecution('thread_s24', 'user pause');
      assert.strictEqual(fixture.store.getTask('thread_s24')?.status, 'paused');

      recordedStatuses.length = 0;
      // 4. Resume task: must transition directly to running when free
      await fixture.controller.resumeOperation('thread_s24');
      const afterResume = fixture.store.getTask('thread_s24');
      assert.strictEqual(afterResume?.status, 'running');
      assert.ok(!recordedStatuses.includes('queued'), `Unexpected queued state in: ${recordedStatuses.join(', ')}`);

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });
});
