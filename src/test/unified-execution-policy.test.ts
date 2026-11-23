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
import { testWorkspace } from './workspace-fixture';
import {
  FastInMemoryLock,
  createMockRunner,
  createThread,
  setupFixture,
} from './unified-execution-fixture';

describe('Unified Execution Policy Tests', () => {
  test('v1 develop preserves the runner-captured child birth identity', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    let finishRun!: () => void;
    const held = new Promise<void>((resolve) => { finishRun = resolve; });
    try {
      const threadId = 'thread_v1_captured_identity';
      fixture.store.upsertTask(createThread(threadId, 'develop'));
      const execute = fixture.runner.execute.bind(fixture.runner);
      fixture.runner.execute = (options) => execute({
        ...options,
        onChildPid: (pid: number) => options.onChildPid(pid, undefined, 'runner-captured-birth-id'),
      });
      fixture.runner.setOnExecute(async () => { await held; });

      assert.equal((await fixture.controller.startExecution(threadId, 'develop')).started, true);
      const deadline = Date.now() + 5_000;
      let child = fixture.store.getTask(threadId)?.execution?.run?.child;
      while (!child && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        child = fixture.store.getTask(threadId)?.execution?.run?.child;
      }
      assert.deepEqual(child, { pid: 777777, birthId: 'runner-captured-birth-id' });
    } finally {
      finishRun();
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
      fixture.store.upsertTask(thread);

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
      fixture.store.upsertTask(thread);

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
      fixture.store.upsertTask(thread);

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
      fixture.store.upsertTask(thread);

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
      fixture.store.upsertTask(thread);

      // Reviewer exits 0 with some output but NEVER called approve_task or request_rework
      fixture.callbackManager.launchCaller = (async () => ({
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
      fixture.store.upsertTask(thread);

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
      fixture.store.upsertTask(thread);

      // Simulate transaction failure before spawn (e.g. disk or revision error)
      fixture.store.transitionExecution = async () => {
        throw new Error('Revision conflict or lock acquisition error');
      };

      const res = await fixture.controller.startExecution('thread_s11', 'develop');
      assert.strictEqual(res.started, false);
      assert.ok(res.reason?.includes('transition_failed'));

      // No child was spawned!
      assert.strictEqual(fixture.runner.executions.length, 0);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 12: Manual retry starts new generation; resume preserves budget
  test('Scenario 12: Manual retry resets autoRetriesUsed and starts new generation; resume preserves budget', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s12', 'develop');
      thread.execution!.generation = 1;
      thread.execution!.autoRetriesUsed = 2;
      thread.execution!.budget.spentActiveMs = 5000;
      fixture.store.upsertTask(thread);

      let finishRunner: () => void;
      const runnerBarrier = new Promise<void>((r) => { finishRunner = r; });
      fixture.runner.setOnExecute(async () => {
        await runnerBarrier;
        return { exitCode: 0, stdout: 'ok', extractedOutput: 'ok' };
      });

      // Resume preserves generation, retries, and spent budget
      await fixture.controller.resumeOperation('thread_s12');
      let fresh = fixture.store.getTask('thread_s12');
      assert.strictEqual(fresh?.execution?.generation, 1);
      assert.strictEqual(fresh?.execution?.budget.spentActiveMs, 5000);

      // Manual retry increments generation and resets retries and spent budget
      await fixture.controller.retryOperation('thread_s12');
      fresh = fixture.store.getTask('thread_s12');
      assert.strictEqual(fresh?.execution?.generation, 2);
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 0);
      assert.strictEqual(fresh?.execution?.budget.spentActiveMs, 0);

      finishRunner!();
    } finally {
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
        version: 1,
        revision: 1,
        operationId: 'op_s16',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 3,
        autoRetriesUsed: 2,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 5000, maxAutoRetries: 2 },
      };
      fixture.store.upsertTask(task);

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
      fixture.store.upsertTask(task);

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
      fixture.store.upsertTask(task);

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

  // Scenario 24: Direct execution without intermediate queued step when resources are free
  test('Scenario 24: retryOperation, resumeOperation, and submitTaskDecision start directly without queued state', async () => {
    const fixture = await setupFixture({ useFastLock: true });
    try {
      const thread = createThread('thread_s24', 'develop');
      fixture.store.upsertTask(thread);

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

      // 2. Submit decision: must transition directly to running when free
      await fixture.controller.submitTaskDecision('thread_s24', dec.id, 'yes proceed');
      const afterSubmit = fixture.store.getTask('thread_s24');
      assert.strictEqual(afterSubmit?.status, 'running');
      // Verify 'queued' was never written during the direct submit transition
      assert.ok(!recordedStatuses.includes('queued'), `Unexpected queued state in: ${recordedStatuses.join(', ')}`);

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
