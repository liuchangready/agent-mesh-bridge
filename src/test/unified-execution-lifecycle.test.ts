import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import { spawn, execFileSync } from 'node:child_process';

import { TaskStore } from '../bridge/task-store';
import {
  ExecutionController,
  DEFAULT_EXECUTION_BUDGET_MS,
  AUTO_RETRY_BACKOFFS_MS,
  MAX_AUTO_RETRIES,
} from '../bridge/execution-controller';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { NativeSessionLock } from '../bridge/native-session-lock';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { MergeCoordinator } from '../bridge/merge-coordinator';
import { ReworkController } from '../bridge/rework-controller';
import { TaskControlService } from '../bridge/task-control';
import {
  DelegationRound,
  DelegationThread,
  ExecutionControl,
  ExecutionPhase,
  TaskDecisionResumePoint,
} from '../bridge/types';
import {
  getProcessBirthId,
  setProcessBirthIdProvider,
  probeProcessIdentity,
  confirmProcessTreeExit,
  setKillPidTreeProvider,
  setProcessProbeProvider,
  setConfirmProcessTreeExitProvider,
} from '../bridge/platform';
import { testWorkspace } from './workspace-fixture';

function createMockRunner() {
  const executions: any[] = [];
  let nextResult: Partial<RunResult> | undefined;
  let onExecute: ((options: any) => Promise<Partial<RunResult> | void>) | undefined;

  return {
    executions,
    setNextResult: (res: Partial<RunResult>) => { nextResult = res; },
    setOnExecute: (fn: (options: any) => Promise<Partial<RunResult> | void>) => { onExecute = fn; },
    clearPendingCancellation: (_id: string) => undefined,
    cancelThread: (_id: string, _action: string, _reason: string) => true,
    isTransientError: (result: RunResult) => {
      const err = (result.error || '').toLowerCase();
      return err.includes('connection error') || err.includes('timeout') || err.includes('network');
    },
    isRateLimitOrQuotaError: (text: string) => /rate[-_\s]*limit|quota/i.test(text),
    execute: async (options: any): Promise<RunResult> => {
      executions.push(options);
      if (options.onChildPid) {
        options.onChildPid(777777);
      }
      if (onExecute) {
        const custom = await onExecute(options);
        if (custom) {
          return {
            exitCode: custom.exitCode ?? 0,
            stdout: custom.stdout ?? 'output',
            stderr: custom.stderr ?? '',
            extractedOutput: custom.extractedOutput ?? 'output',
            durationMs: custom.durationMs ?? 10,
            error: custom.error,
            assessment: custom.assessment ?? {
              success: (custom.exitCode ?? 0) === 0,
              errorCode: (custom.exitCode ?? 0) === 0 ? 'OK' : 'NONZERO_EXIT',
              exitCode: custom.exitCode ?? 0,
              stream: {
                chunkCount: 1,
                stdoutBytes: 10,
                stderrBytes: 0,
                hasActualOutput: true,
                continuous: true,
                gapExceeded: false,
                maxGapMs: 0,
                gapThresholdMs: 60000,
              },
            },
          };
        }
      }
      const res = nextResult || {};
      const exitCode = res.exitCode ?? 0;
      return {
        exitCode,
        stdout: res.stdout ?? 'output',
        stderr: res.stderr ?? '',
        extractedOutput: res.extractedOutput ?? 'extracted output',
        durationMs: res.durationMs ?? 10,
        error: res.error,
        assessment: res.assessment ?? {
          success: exitCode === 0,
          errorCode: exitCode === 0 ? 'OK' : 'NONZERO_EXIT',
          exitCode,
          stream: {
            chunkCount: 1,
            stdoutBytes: 10,
            stderrBytes: 0,
            hasActualOutput: true,
            continuous: true,
            gapExceeded: false,
            maxGapMs: 0,
            gapThresholdMs: 60000,
          },
        },
      };
    },
  };
}

function createThread(id: string, phase: ExecutionPhase = 'develop'): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: id,
    title: `Task ${id}`,
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'queued',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: `session-${id}`,
    reviewerSessionId: `rev-session-${id}`,
    callerSessionId: `rev-session-${id}`,
    reviewerSessionCaptured: true,
    workspace: testWorkspace(os.tmpdir()),
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    execution: {
      version: 1,
      revision: 1,
      operationId: `op_${id}`,
      phase,
      round: 1,
      generation: 1,
      attempt: 0,
      autoRetriesUsed: 0,
      budget: {
        limitMs: DEFAULT_EXECUTION_BUDGET_MS,
        spentActiveMs: 0,
        maxAutoRetries: MAX_AUTO_RETRIES,
      },
    },
    summary: {
      lastPromptSnippet: 'test',
      lastOutputSnippet: 'test',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'test prompt',
        status: 'running',
        durationMs: 0,
        timestamp: now,
      },
    ],
  };
}

async function setupFixture() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'unified-exec-test-'));
  const store = new TaskStore(dir);
  const runner = createMockRunner();
  const detector = new AgentDetector();
  const callbackManager = new CallbackManager(detector, dir);
  callbackManager.setTaskStore(store);
  callbackManager.setAutonomousRunner(runner as any);
  callbackManager.launchCaller = (async () => ({ success: true, exitCode: 0 })) as any;

  const roleLock = new TaskRoleLock(dir);
  const sessionLock = new NativeSessionLock(dir);
  const registry = new TaskExecutionRegistry(dir);
  const mergeLock = new MeshFileLock(path.join(dir, 'merge.lock'));

  const controller = new ExecutionController(
    store,
    runner as any,
    callbackManager,
    undefined,
    roleLock,
    sessionLock,
    registry,
    { storageDir: dir, retryBackoffsMs: [10_000, 30_000] },
  );

  return {
    dir,
    store,
    runner,
    detector,
    callbackManager,
    roleLock,
    sessionLock,
    registry,
    mergeLock,
    controller,
    cleanup: async () => {
      await controller.waitForActiveExecutions(3000);
      controller.dispose();
      roleLock.dispose();
      registry.dispose();
      store.dispose();
      mergeLock.dispose();
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    },
  };
}

describe('Unified Execution Lifecycle (docs/UNIFIED_EXECUTION_LIFECYCLE_DESIGN.md)', () => {
  // Scenario 1: Unified start/stop/outcome classification for develop, review, merge
  test('Scenario 1: develop, review, and merge share execution control record and transition through controller', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s1', 'develop');
      fixture.store.upsertTask(thread);

      let finishDevelop: () => void;
      const barrier = new Promise<void>((r) => { finishDevelop = r; });
      fixture.runner.setOnExecute(async () => {
        await barrier;
        return { exitCode: 0, stdout: 'ok', extractedOutput: 'ok' };
      });

      let finishReview: () => void;
      const reviewBarrier = new Promise<void>((r) => { finishReview = r; });
      fixture.callbackManager.launchCaller = (async () => {
        await reviewBarrier;
        return { success: true, exitCode: 0 };
      }) as any;

      const res = await fixture.controller.startExecution('thread_s1', 'develop');
      assert.strictEqual(res.started, true);

      // Verify thread transitions to running
      const updated = fixture.store.getTask('thread_s1');
      assert.strictEqual(updated?.status, 'running');
      assert.strictEqual(updated?.execution?.phase, 'develop');
      assert.strictEqual(updated?.execution?.attempt, 1);
      assert.strictEqual(updated?.execution?.version, 1);
      assert.ok(updated?.execution?.run?.id);
      assert.strictEqual(updated?.execution?.run?.owner.pid, process.pid);

      // Finish develop -> advances to review seamlessly
      finishDevelop!();
      const deadline1 = Date.now() + 15_000;
      let postDevelop = fixture.store.getTask('thread_s1');
      while (postDevelop?.status !== 'reviewing' && Date.now() < deadline1) {
        await new Promise((r) => setTimeout(r, 100));
        postDevelop = fixture.store.getTask('thread_s1');
      }
      assert.strictEqual(postDevelop?.status, 'reviewing');
      assert.strictEqual(postDevelop?.execution?.phase, 'review');

      finishReview!();
      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 2: Active budget with monotonic tracking; output does not extend budget
  test('Scenario 2: Active time budget is enforced and output does not extend deadline', async () => {
    const fixture = await setupFixture();
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
        await new Promise((r) => setTimeout(r, 100));
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
    const fixture = await setupFixture();
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
        await new Promise((r) => setTimeout(r, 100));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'queued');
      assert.strictEqual(fresh?.execution?.autoRetriesUsed, 1);
      assert.strictEqual(fresh?.execution?.queue?.reason, 'backoff');
      assert.ok(fresh?.execution?.queue?.notBefore);

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

      // Clear queue notBefore to run immediately
      const rev1 = fixture.store.getTask('thread_s3')!.execution!.revision;
      await fixture.store.transitionExecution('thread_s3', rev1, (t) => {
        t.execution!.queue = undefined;
      });

      await fixture.controller.startExecution('thread_s3', 'develop');
      const deadline3_2 = Date.now() + 15_000;
      fresh = fixture.store.getTask('thread_s3');
      while ((fresh?.status !== 'queued' || fresh?.execution?.autoRetriesUsed !== 2) && Date.now() < deadline3_2) {
        await new Promise((r) => setTimeout(r, 100));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'queued');
      // Fingerprint change did NOT reset retry count; it incremented to 2!
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
      const rev2 = fixture.store.getTask('thread_s3')!.execution!.revision;
      await fixture.store.transitionExecution('thread_s3', rev2, (t) => {
        t.execution!.queue = undefined;
      });

      await fixture.controller.startExecution('thread_s3', 'develop');
      const deadline3_3 = Date.now() + 15_000;
      fresh = fixture.store.getTask('thread_s3');
      while (fresh?.status !== 'failed' && Date.now() < deadline3_3) {
        await new Promise((r) => setTimeout(r, 100));
        fresh = fixture.store.getTask('thread_s3');
      }
      assert.strictEqual(fresh?.status, 'failed');
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 4: Quota wait does not consume autoRetries; unknown quota enters failed(RATE_LIMITED)
  test('Scenario 4: Known quota wait sets queued(quota) without consuming retries; unknown reset enters failed(RATE_LIMITED)', async () => {
    const fixture = await setupFixture();
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

  // Scenario 5: Two tasks approved simultaneously: only one enters merging, other queued(merge)
  test('Scenario 5: Two approved tasks serialize on global merge lock; only holder is merging', async () => {
    const fixture = await setupFixture();
    try {
      const taskA = createThread('thread_s5_a', 'merge');
      const taskB = createThread('thread_s5_b', 'merge');
      fixture.store.upsertTask(taskA);
      fixture.store.upsertTask(taskB);

      // Simulate merge lock held by Task A
      const releaseA = await fixture.mergeLock.acquire();
      assert.ok(releaseA);

      // Task B tries to start merge while lock is held
      const resB = await fixture.controller.startExecution('thread_s5_b', 'merge');
      assert.strictEqual(resB.started, false);
      assert.strictEqual(resB.reason, 'merge_lock_busy');

      const freshB = fixture.store.getTask('thread_s5_b');
      assert.strictEqual(freshB?.status, 'queued');
      assert.strictEqual(freshB?.execution?.phase, 'merge');
      assert.strictEqual(freshB?.execution?.queue?.reason, 'merge_lock');

      // Release Task A's lock
      releaseA();

      // Now Task B can acquire and start
      const resB2 = await fixture.controller.startExecution('thread_s5_b', 'merge');
      assert.strictEqual(resB2.started, true);
      const freshB2 = fixture.store.getTask('thread_s5_b');
      assert.strictEqual(freshB2?.status, 'merging');

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 6: Git sub-steps with empty child don't fail as long as owner alive
  test('Scenario 6: Between Git commands child is temporarily undefined, probeRun returns alive via owner', async () => {
    const fixture = await setupFixture();
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

  // Scenario 7: Process identity probing with birthId prevents PID reuse
  test('Scenario 7: Mismatched birthId identifies exited process despite reused PID', async () => {
    const fixture = await setupFixture();
    try {
      // Invalidate birthId provider to mock PID reuse
      setProcessBirthIdProvider((pid) => (pid === 99999 ? 'birth_new_process' : 'birth_original'));

      const probe = probeProcessIdentity(99999, 'birth_old_crashed_process');
      assert.strictEqual(probe, 'exited');
    } finally {
      setProcessBirthIdProvider(undefined);
      await fixture.cleanup();
    }
  });

  // Scenario 8: Reviewer tool verdict and trailing CLI exit: only advances once; exit without verdict enters NO_VERDICT
  test('Scenario 8: Reviewer exit without tool verdict enters NO_VERDICT failure', async () => {
    const fixture = await setupFixture();
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

  // Scenario 9: TaskStore transitionExecution with CAS revision check prevents stale overwrite
  test('Scenario 9: transitionExecution throws on revision conflict and protects execution state', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s9', 'develop');
      fixture.store.upsertTask(thread);

      // Transition with expected revision 1 -> succeeds, increments to 2
      await fixture.store.transitionExecution('thread_s9', 1, (t) => {
        t.execution!.attempt = 1;
      });

      // Stale writer with expected revision 1 -> rejected!
      await assert.rejects(async () => {
        await fixture.store.transitionExecution('thread_s9', 1, (t) => {
          t.execution!.attempt = 99;
        });
      }, /Execution revision mismatch/);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 10: Task decision across develop, review, merge releases locks and preserves budget on answer
  test('Scenario 10: requestTaskDecision transitions to wait_decision and submitTaskDecision resumes with budget preserved', async () => {
    const fixture = await setupFixture();
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
    const fixture = await setupFixture();
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
    const fixture = await setupFixture();
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

  // Scenario 13: Token usage components, sessions, snapshots, targets preserved
  test('Scenario 13: TaskStore upsertTask preserves execution record and does not overwrite with stale copies', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s13', 'develop');
      fixture.store.upsertTask(thread);

      // Update execution in store
      await fixture.store.transitionExecution('thread_s13', 1, (t) => {
        t.execution!.attempt = 3;
        t.execution!.revision = 2;
      });

      // Older task copy lacking latest execution
      const staleCopy = structuredClone(thread);
      staleCopy.execution!.attempt = 1;
      staleCopy.execution!.revision = 1;

      // Upserting stale copy preserves the newer execution record
      fixture.store.upsertTask(staleCopy);

      const fresh = fixture.store.getTask('thread_s13');
      assert.strictEqual(fresh?.execution?.attempt, 3);
      assert.strictEqual(fresh?.execution?.revision, 2);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 14: Legacy tasks without execution record are preserved without synthetic execution
  test('Scenario 14: Legacy tasks without execution field retain legacy status semantics', async () => {
    const fixture = await setupFixture();
    try {
      const now = new Date().toISOString();
      const legacyThread: DelegationThread = {
        threadId: 'thread_legacy_14',
        title: 'Legacy Task',
        initiatorAgent: 'codex',
        targetAgent: 'claude_code',
        status: 'waiting_review',
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
      assert.strictEqual(readBack?.status, 'waiting_review');
      assert.strictEqual(readBack?.execution, undefined);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 15: Seamless phase handoff without mandatory queued state; concurrent independent reviews
  test('Scenario 15: Phase handoff advances directly when resources free; independent reviews are concurrent', async () => {
    const fixture = await setupFixture();
    try {
      const task1 = createThread('thread_s15_1', 'review');
      task1.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      const task2 = createThread('thread_s15_2', 'review');
      task2.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      fixture.store.upsertTask(task1);
      fixture.store.upsertTask(task2);

      let finishReview: () => void;
      const reviewBarrier = new Promise<void>((r) => { finishReview = r; });
      fixture.callbackManager.launchCaller = (async () => {
        await reviewBarrier;
        return { success: true, exitCode: 0 };
      }) as any;

      // Start review on Task 1
      const res1 = await fixture.controller.startExecution('thread_s15_1', 'review');
      assert.strictEqual(res1.started, true);
      const fresh1 = fixture.store.getTask('thread_s15_1');
      assert.strictEqual(fresh1?.status, 'reviewing');

      // Start review on Task 2: reviews do NOT globally serialize on each other!
      const res2 = await fixture.controller.startExecution('thread_s15_2', 'review');
      assert.strictEqual(res2.started, true);
      const fresh2 = fixture.store.getTask('thread_s15_2');
      assert.strictEqual(fresh2?.status, 'reviewing');

      finishReview!();
      await fixture.controller.waitForActiveExecutions(3000);
      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 16: retryOperation resets generation/failure and resumes; resumeOperation clears paused state
  test('Scenario 16: retryOperation and resumeOperation restore state to queued/running and clear failures', async () => {
    const fixture = await setupFixture();
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
    const fixture = await setupFixture();
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
    const fixture = await setupFixture();
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

  // Scenario 19: MeshFileLock child lifetime: helper holds OS mutex until attached child exits
  test('Scenario 19: MeshFileLock helper holds OS lock until attached child terminates', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-test-'));
    const lockPath = path.join(tmpDir, 'test-merge.lock');
    const lockA = new MeshFileLock(lockPath);
    const lockB = new MeshFileLock(lockPath);

    let childProc: any;
    try {
      const releaseA = await lockA.acquire();
      assert.ok(releaseA, 'lockA should acquire');

      // Spawn a child process that sleeps
      childProc = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 4000)'], { stdio: 'ignore' });
      await lockA.attachChildPid(childProc.pid);

      // Release A: helper should NOT release OS mutex yet because childProc is still alive!
      releaseA();

      // lockB tries to acquire with 600ms timeout -> must time out!
      const releaseB = await lockB.acquire(undefined, undefined, 600);
      assert.strictEqual(releaseB, undefined, 'lockB must not acquire while childProc is still alive');

      // Now kill the child process
      try { process.kill(childProc.pid, 'SIGKILL'); } catch {}

      // Wait a moment for helper to observe child exit
      await new Promise((r) => setTimeout(r, 400));

      // Now lockB should be able to acquire!
      const releaseBAfter = await lockB.acquire(undefined, undefined, 4000);
      assert.ok(releaseBAfter, 'lockB should acquire after childProc terminates');
      releaseBAfter();
    } finally {
      if (childProc?.pid) {
        try { process.kill(childProc.pid, 'SIGKILL'); } catch {}
      }
      lockA.dispose();
      lockB.dispose();
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 20: Alive run identity preservation and stale callback rejection with empty run
  test('Scenario 20: Alive run identity preservation and stale callback rejection with empty run', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s20', 'develop');
      // Set run pointing to process.pid with real birthId
      const realBirthId = getProcessBirthId(process.pid);
      thread.execution!.run = {
        id: 'run_alive_1',
        owner: { pid: process.pid, birthId: realBirthId },
        step: 'executing',
        state: 'active',
        startedAt: new Date().toISOString(),
      };
      fixture.store.upsertTask(thread);

      // Calling startExecution when run is alive must return previous_child_alive
      const startRes = await fixture.controller.startExecution('thread_s20', 'develop');
      assert.strictEqual(startRes.started, false);
      assert.strictEqual(startRes.reason, 'previous_child_alive');

      // Verify execution.run is STILL present and NOT wiped!
      const fresh = fixture.store.getTask('thread_s20');
      assert.strictEqual(fresh?.execution?.run?.id, 'run_alive_1');

      // Now test: task has generation=2, operationId='new-operation', status='queued', run=undefined, spentActiveMs=5000
      const rev = fresh!.execution!.revision;
      await fixture.store.transitionExecution('thread_s20', rev, (t) => {
        t.status = 'queued';
        t.execution!.generation = 2;
        t.execution!.operationId = 'new-operation';
        t.execution!.run = undefined;
        t.execution!.budget.spentActiveMs = 5000;
      });

      // Call handlePhaseOutcome with old runId and generation=1
      await (fixture.controller as any).handlePhaseOutcome(
        'thread_s20',
        'develop',
        'old-generation-run',
        { ok: true },
        { generation: 1, operationId: 'old-op' },
      );

      // Verify phase was NOT changed to review, spentActiveMs was NOT reset to 0, status is STILL queued!
      const afterStale = fixture.store.getTask('thread_s20');
      assert.strictEqual(afterStale?.status, 'queued');
      assert.strictEqual(afterStale?.execution?.phase, 'develop');
      assert.strictEqual(afterStale?.execution?.budget.spentActiveMs, 5000);
      assert.strictEqual(afterStale?.execution?.generation, 2);
    } finally {
      await fixture.cleanup();

    }
  });

  // Scenario 21: MergeCoordinator lock sharing and external lease avoids self-deadlock
  test('Scenario 21: MergeCoordinator lock sharing and external lease avoids self-deadlock', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s21', 'merge');
      thread.status = 'waiting_merge';
      thread.worktree = {
        path: thread.workspace.executionPath,
        mainPath: thread.workspace.rootPath,
        repositoryPath: thread.workspace.rootPath,
        baseCommit: 'commit_base',
        headCommit: 'commit_head',
        approvedCommit: 'commit_approved',
        targetRef: 'refs/heads/master',
        branch: 'mesh/task/thread_s21',
      };
      fixture.store.upsertTask(thread);

      // In controller, mergeLock is shared from mergeCoordinator.getLock()
      assert.strictEqual(
        (fixture.controller as any).mergeLock,
        (fixture.controller as any).mergeCoordinator.getLock(),
        'ExecutionController and MergeCoordinator must share identical MeshFileLock instance'
      );

      // Acquire lock with lease
      const release = await (fixture.controller as any).mergeLock.acquire();
      assert.ok(release, 'Merge lock acquired');

      // executeMergeAdapter with external lease must not deadlock on lock acquisition
      let mockGitCalled = false;
      (fixture.controller as any).mergeCoordinator.mergeApprovedAttempt = async (
        id: string,
        onPid?: any,
        extLease?: any,
      ) => {
        mockGitCalled = true;
        assert.ok(extLease, 'externalLease must be passed to mergeApprovedAttempt');
        return { status: 'merged', message: 'OK' };
      };

      const outcome = await (fixture.controller as any).executeMergeAdapter(
        'thread_s21',
        'run_merge_1',
        undefined,
        undefined,
        release,
      );
      assert.strictEqual(outcome.ok, true);
      assert.strictEqual(mockGitCalled, true);

      release();
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 22: TaskStore CAS Lock (transitionExecution with MeshFileLock, immune to mtime backdating)
  test('Scenario 22: TaskStore CAS Lock mutual exclusion and immune to mtime backdating', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s22', 'develop');
      fixture.store.upsertTask(thread);

      const store2 = new TaskStore(path.dirname(fixture.store.getStoragePath()));
      const lockDir = path.join(fixture.dir, 'execution-locks');
      fs.mkdirSync(lockDir, { recursive: true });
      const lockFile = path.join(lockDir, 'thread_s22.lock');

      // Acquire lock on thread_s22 via store1
      let finishStore1: () => void;
      const barrier = new Promise<void>((r) => { finishStore1 = r; });
      const p1 = fixture.store.transitionExecution('thread_s22', 1, (t) => {
        t.summary.lastOutputSnippet = 'store1-writing';
      });

      await p1;

      // Simulate mtime backdating by 11 seconds on lock file
      if (fs.existsSync(lockFile)) {
        const past = new Date(Date.now() - 11_000);
        fs.utimesSync(lockFile, past, past);
      }

      // transitionExecution on store2 with expected revision 1 must be rejected due to revision mismatch (already bumped to 2)
      await assert.rejects(
        () => store2.transitionExecution('thread_s22', 1, (t) => {
          t.summary.lastOutputSnippet = 'store2-invalid';
        }),
        /revision mismatch/i,
      );

      // transitionExecution on store2 with revision 2 succeeds cleanly
      const p2 = await store2.transitionExecution('thread_s22', 2, (t) => {
        t.summary.lastOutputSnippet = 'store2-valid';
      });
      assert.strictEqual(p2.summary.lastOutputSnippet, 'store2-valid');
      assert.strictEqual(p2.execution?.revision, 3);
      store2.dispose();
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 23: Crash recovery scanner (scanAndDrain sets failed(OWNER_LOST) without auto-retry, stops orphan child)
  test('Scenario 23: Crash recovery sets failed(OWNER_LOST) without auto-retry and stops orphan child', async () => {
    const fixture = await setupFixture();
    try {
      // Spawn an orphan child
      const child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {
        stdio: 'ignore',
      });
      assert.ok(child.pid);
      const childBirthId = getProcessBirthId(child.pid);

      const thread = createThread('thread_s23', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s23',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-1',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          child: { pid: child.pid, birthId: childBirthId },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Verify inspectRunProcesses sees dead owner and alive child
      const inspect = fixture.controller.inspectRunProcesses(thread.execution!.run);
      assert.strictEqual(inspect.ownerStatus, 'exited');
      assert.strictEqual(inspect.childStatus, 'alive');

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Verify task entered failed(OWNER_LOST) without auto-retrying
      const recovered = fixture.store.getTask('thread_s23');
      assert.strictEqual(recovered?.status, 'failed');
      assert.strictEqual(recovered?.failure?.code, 'OWNER_LOST');
      assert.strictEqual(recovered?.execution?.autoRetriesUsed, 0); // MUST NOT auto-retry!
      assert.strictEqual(recovered?.execution?.run, undefined);

      // Verify child process was stopped
      const childProbe = probeProcessIdentity(child.pid, childBirthId);
      assert.strictEqual(childProbe, 'exited');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 24: Direct execution without intermediate queued step when resources are free
  test('Scenario 24: retryOperation, resumeOperation, and submitTaskDecision start directly without queued state', async () => {
    const fixture = await setupFixture();
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

  // Scenario 25: Stdio MCP decision termination order (persists decision via CAS first, skips self-kill)
  test('Scenario 25: Stdio MCP decision request persists first and skips self-kill', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s25', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s25',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 0, maxAutoRetries: 2 },
        run: {
          id: 'stdio-run-1',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          // Child PID is process.pid (simulating stdio MCP server inside child tree)
          child: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Calling requestTaskDecision from within child process tree must NOT kill self
      const decision = await fixture.controller.requestTaskDecision('thread_s25', {
        question: 'Need clarification?',
        resumePoint: 'continue_develop',
      });

      assert.ok(decision.id);
      assert.strictEqual(decision.question, 'Need clarification?');

      // Verify decision and wait_decision were persisted via CAS first
      const saved = fixture.store.getTask('thread_s25');
      assert.strictEqual(saved?.status, 'wait_decision');
      assert.strictEqual(saved?.decision?.id, decision.id);
      assert.strictEqual(saved?.decision?.question, 'Need clarification?');
      // Current process is still alive and didn't kill itself
      assert.strictEqual(probeProcessIdentity(process.pid), 'alive');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 26: Recursive descendant process tree exit confirmation with birthId
  test('Scenario 26: confirmProcessTreeExit tracks descendant tree and verifies exit with birthId', async () => {
    // Spawn child that spawns grandchild
    const child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {
      stdio: 'ignore',
    });
    assert.ok(child.pid);
    const birthId = getProcessBirthId(child.pid);

    try {
      // While alive, confirmProcessTreeExit with short timeout returns false
      const aliveCheck = await confirmProcessTreeExit(child.pid, 200, birthId);
      assert.strictEqual(aliveCheck, false);

      // Kill child
      try { child.kill('SIGKILL'); } catch {}

      // Once killed, confirmProcessTreeExit returns true
      const deadCheck = await confirmProcessTreeExit(child.pid, 5000, birthId);
      assert.strictEqual(deadCheck, true);
    } finally {
      try { child.kill('SIGKILL'); } catch {}
    }
  });

  // Scenario 27: Crash recovery candidate ref alone must NOT complete task; requires completed response & Git landed merge
  test('Scenario 27: Crash recovery candidate ref alone must NOT mark completed', async () => {
    const fixture = await setupFixture();
    const gitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-recovery-test-'));
    try {
      // Init real git repo
      execFileSync('git', ['init', '-b', 'master'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.name', 'test'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.email', 'test@test.com'], { cwd: gitDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'base');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'base commit'], { cwd: gitDir, stdio: 'ignore' });
      const baseSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      // Create a candidate ref in git
      execFileSync('git', ['update-ref', 'refs/mesh/candidates/thread_s27', baseSha], { cwd: gitDir, stdio: 'ignore' });

      // Case A: develop phase, round is still 'running', assessment undefined, mergePending=true, owner is dead
      const thread = createThread('thread_s27', 'develop');
      thread.status = 'running';
      thread.workspace = testWorkspace(gitDir);
      thread.worktree = {
        path: gitDir,
        mainPath: gitDir,
        baseCommit: baseSha,
        headCommit: baseSha,
        mergePending: true,
        integration: { phase: 'ready', updatedAt: new Date().toISOString() },
      };
      thread.rounds = [
        {
          round: 1,
          type: 'initial',
          prompt: 'develop prompt',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
          // Note: NO assessment!
        },
      ];
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s27',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-develop',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Task MUST NOT become completed! It must be failed(OWNER_LOST)!
      const afterDrain = fixture.store.getTask('thread_s27');
      assert.strictEqual(afterDrain?.status, 'failed');
      assert.strictEqual(afterDrain?.failure?.code, 'OWNER_LOST');
      assert.strictEqual(afterDrain?.worktree?.integration?.phase, 'ready'); // Preserved workspace

      // Case B: develop phase, BUT round has completed with valid assessment
      const threadB = createThread('thread_s27_b', 'develop');
      threadB.status = 'running';
      threadB.workspace = testWorkspace(gitDir);
      threadB.worktree = {
        path: gitDir,
        mainPath: gitDir,
        baseCommit: baseSha,
        headCommit: baseSha,
      };
      threadB.rounds = [
        {
          round: 1,
          type: 'initial',
          prompt: 'develop prompt',
          status: 'completed',
          durationMs: 100,
          timestamp: new Date().toISOString(),
          assessment: {
            success: true,
            exitCode: 0,
            errorCode: 'OK',
            warnings: [],
            stream: {
              chunkCount: 1,
              stdoutBytes: 10,
              stderrBytes: 0,
              firstOutputAtMs: 1,
              lastOutputAtMs: 2,
              maxGapMs: 0,
              rawMaxGapMs: 0,
              excludedSuspensionMs: 0,
              suspensionCount: 0,
              gapThresholdMs: 60000,
              hasActualOutput: true,
              continuous: true,
              gapExceeded: false,
            },
          },
        },
      ];
      execFileSync('git', ['update-ref', 'refs/mesh/candidates/thread_s27_b', baseSha], { cwd: gitDir, stdio: 'ignore' });
      threadB.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s27_b',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-develop-b',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadB);

      await fixture.controller.scanAndDrain();

      // Successful develop with response evidence hands off to REVIEW, NEVER completed!
      const afterDrainB = fixture.store.getTask('thread_s27_b');
      assert.strictEqual(afterDrainB?.status, 'waiting_review');
      assert.strictEqual(afterDrainB?.execution?.phase, 'review');
    } finally {
      await fixture.cleanup();
      try { fs.rmSync(gitDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 28: Stop unconfirmed retains run protection, prevents concurrent retry/resume, and pauseExecution with unknown identity does not clear run
  test('Scenario 28: Stop unconfirmed retains run protection and blocks concurrent retry/resume', async () => {
    const fixture = await setupFixture();
    const killedPids: number[] = [];
    setKillPidTreeProvider((pid) => { killedPids.push(pid); });
    setProcessProbeProvider((pid, birthId) => {
      // Injected: alive probe for mock child PIDs (99999998, 99999997, 99999996), exited for dead mock owner PID 99999999
      if (pid === 99999998 || pid === 99999997 || pid === 99999996) {
        if (birthId && birthId !== 'birth-99999996') {
          return 'exited';
        }
        return 'alive';
      }
      if (pid === 99999999) {
        return 'exited';
      }
      // For real OS processes (e.g. process.pid): delegate to genuine OS probe logic
      try {
        process.kill(pid, 0);
      } catch {
        return 'exited';
      }
      if (!birthId) return 'alive';
      const actual = getProcessBirthId(pid);
      if (!actual) return 'unknown';
      return actual === birthId ? 'alive' : 'exited';
    });
    setConfirmProcessTreeExitProvider(async () => true);

    try {
      // 1. Unconfirmed stop in scanAndDrain (child probe is alive, but missing birthId: MUST NOT KILL!)
      const thread = createThread('thread_s28', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28',
          owner: { pid: 99999999, birthId: 'dead-owner' },
          child: { pid: 99999998, birthId: undefined as any }, // Missing birthId -> unconfirmed identity!
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Assert killPidTree was NEVER called because birthId is missing!
      assert.strictEqual(killedPids.length, 0, 'Must NOT kill child process without verified birthId');

      const afterDrain = fixture.store.getTask('thread_s28');
      // Must NOT be cleared or failed without protection!
      assert.strictEqual(afterDrain?.execution?.run?.state, 'stopping');
      assert.strictEqual(afterDrain?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // Attempt concurrent retryOperation: must throw and refuse to run!
      await assert.rejects(
        async () => {
          await fixture.controller.retryOperation('thread_s28', 'retry');
        },
        /still stopping or unconfirmed/,
      );

      // Attempt concurrent resumeOperation: must throw and refuse to run!
      await assert.rejects(
        async () => {
          await fixture.controller.resumeOperation('thread_s28');
        },
        /still stopping or unconfirmed/,
      );

      // 2. pauseExecution with alive probe but missing child birthId: must NOT kill and must NOT clear run
      killedPids.length = 0;
      const threadPause = createThread('thread_s28_pause', 'develop');
      threadPause.status = 'running';
      threadPause.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_p',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-p',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          child: { pid: 99999997, birthId: undefined as any }, // Missing birthId
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadPause);

      const paused = await fixture.controller.pauseExecution('thread_s28_pause');
      assert.strictEqual(killedPids.length, 0, 'pauseExecution must NOT kill child process without verified birthId');
      assert.strictEqual(paused.execution?.run?.state, 'stopping');
      assert.ok(paused.execution?.run); // run is NOT undefined!
      assert.strictEqual(paused.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // 3. Production collection regression via updateRunChild with undefined birthId:
      // When onChildPid/getProcessBirthId returns undefined, updateRunChild must write birthId=undefined (NO String(pid) fallback!)
      // Subsequently identity reading recovers and child process is still alive:
      // pauseExecution must NOT treat old process as exited or clear run!
      const mockChildPid = 99999996;
      killedPids.length = 0;
      const threadCollect = createThread('thread_s28_collect', 'develop');
      threadCollect.status = 'running';
      threadCollect.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_c',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-c',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadCollect);

      // Call updateRunChild simulating onChildPid with undefined birthId
      await fixture.controller.updateRunChild('thread_s28_collect', 'run-s28-c', mockChildPid, undefined);

      // Verify task on disk has birthId=undefined (NOT fake String(pid)!)
      fixture.store.loadFromDisk();
      const taskAfterCollect = fixture.store.getTask('thread_s28_collect');
      assert.strictEqual(taskAfterCollect?.execution?.run?.child?.pid, mockChildPid);
      assert.strictEqual(taskAfterCollect?.execution?.run?.child?.birthId, undefined);

      // Identity reading recovers in system
      setProcessBirthIdProvider((pid) => (pid === mockChildPid ? 'birth-99999996' : undefined));

      // Calling pauseExecution must NOT consider child exited, must NOT kill, and must NOT clear run
      const pausedCollect = await fixture.controller.pauseExecution('thread_s28_collect');
      assert.strictEqual(killedPids.length, 0, 'Must NOT kill child process without verified birthId');
      assert.ok(pausedCollect.execution?.run, 'Run must NOT be cleared when stop is unconfirmed');
      assert.strictEqual(pausedCollect.execution?.run?.state, 'stopping');
      assert.strictEqual(pausedCollect.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');
      assert.notStrictEqual(pausedCollect.status, 'paused', 'Status must NOT transition to clean paused');

      // Attempting resume/retry must be blocked
      await assert.rejects(
        async () => { await fixture.controller.resumeOperation('thread_s28_collect'); },
        /still stopping or unconfirmed/,
      );
      await assert.rejects(
        async () => { await fixture.controller.retryOperation('thread_s28_collect'); },
        /still stopping or unconfirmed/,
      );

      // 4. Scanner regression with updateRunChild having undefined birthId and child alive:
      killedPids.length = 0;
      const threadScanCollect = createThread('thread_s28_scan_collect', 'develop');
      threadScanCollect.status = 'running';
      threadScanCollect.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_sc',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-sc',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadScanCollect);

      await fixture.controller.updateRunChild('thread_s28_scan_collect', 'run-s28-sc', mockChildPid, undefined);
      fixture.store.loadFromDisk();
      const taskScanBefore = fixture.store.getTask('thread_s28_scan_collect');
      assert.ok(taskScanBefore?.execution?.run);
      assert.strictEqual(taskScanBefore?.execution?.run?.child?.pid, mockChildPid);
      assert.strictEqual(taskScanBefore?.execution?.run?.child?.birthId, undefined);

      // Now simulate owner crash
      await fixture.store.transitionExecution(
        'thread_s28_scan_collect',
        taskScanBefore.execution!.revision,
        (t) => {
          if (t.execution?.run) {
            t.execution.run.owner = { pid: 99999999, birthId: 'dead-owner' };
          }
        },
      );

      await fixture.controller.scanAndDrain();
      assert.strictEqual(killedPids.length, 0, 'scanAndDrain must NOT kill child process without verified birthId');
      fixture.store.loadFromDisk();
      const taskScanAfter = fixture.store.getTask('thread_s28_scan_collect');
      assert.ok(taskScanAfter?.execution?.run, 'scanAndDrain must NOT clear run without verified birthId');
      assert.strictEqual(taskScanAfter?.execution?.run?.state, 'stopping');
      assert.strictEqual(taskScanAfter?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // 5. Owner collection failure: owner.birthId is undefined, owner is alive
      const threadOwnerFail = createThread('thread_s28_owner_fail', 'develop');
      threadOwnerFail.status = 'running';
      threadOwnerFail.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_of',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-of',
          owner: { pid: process.pid, birthId: undefined as any }, // Owner collection failure
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadOwnerFail);

      // inspectRunProcesses must return ownerStatus='unknown' (NEVER 'exited')
      const inspectOwner = fixture.controller.inspectRunProcesses(threadOwnerFail.execution?.run);
      assert.strictEqual(inspectOwner.ownerStatus, 'unknown', 'Missing owner birthId must be unknown, not exited');

      // scanAndDrain must NOT mark owner lost or clear run
      await fixture.controller.scanAndDrain();
      fixture.store.loadFromDisk();
      const afterOwnerScan = fixture.store.getTask('thread_s28_owner_fail');
      assert.strictEqual(afterOwnerScan?.status, 'running', 'Task must NOT enter failed(OWNER_LOST)');
      assert.ok(afterOwnerScan?.execution?.run, 'Run must NOT be cleared when owner identity is unknown');
    } finally {
      setKillPidTreeProvider(undefined);
      setProcessProbeProvider(undefined);
      setConfirmProcessTreeExitProvider(undefined);
      setProcessBirthIdProvider(undefined);
      await fixture.cleanup();
    }
  });

  // Scenario 29: Separate logical verdict from physical execution exit: approve/rework retains Reviewer run until CLI exits
  test('Scenario 29: approveTask retains Reviewer run until CLI exits and non-zero tail exit does not revoke verdict', async () => {
    const fixture = await setupFixture();
    const reworkController = new ReworkController(
      fixture.store,
      fixture.runner as any,
      fixture.callbackManager,
      fixture.detector,
      fixture.registry,
    );
    const gitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-s29-test-'));

    try {
      execFileSync('git', ['init', '-b', 'master'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.name', 'test'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.email', 'test@test.com'], { cwd: gitDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'base');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'base commit'], { cwd: gitDir, stdio: 'ignore' });
      const baseSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'modified by developer');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'dev commit'], { cwd: gitDir, stdio: 'ignore' });
      const headSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      const thread = createThread('thread_s29', 'review');
      thread.workspace = testWorkspace(gitDir);
      thread.worktree = {
        path: gitDir,
        mainPath: gitDir,
        repositoryPath: gitDir,
        commonDir: path.join(gitDir, '.git'),
        targetRef: 'refs/heads/master',
        taskBranch: 'mesh/task/thread_s29',
        baseCommit: baseSha,
        headCommit: headSha,
      };
      thread.rounds[0].status = 'completed'; // developer round completed
      fixture.store.upsertTask(thread);

      let releaseCliBarrier: (res: { success: boolean; exitCode: number }) => void;
      const cliBarrier = new Promise<{ success: boolean; exitCode: number }>((resolve) => {
        releaseCliBarrier = resolve;
      });

      // Mock launchCaller representing a live Reviewer CLI
      fixture.callbackManager.launchCaller = (async (_t: any, _r: any, onChildPid?: (pid: number) => void) => {
        if (onChildPid) onChildPid(process.pid);
        return await cliBarrier;
      }) as any;

      // Start review phase
      const startRes = await fixture.controller.startExecution('thread_s29', 'review');
      assert.strictEqual(startRes.started, true);

      // Verify task is reviewing with an active run
      const reviewingTask = fixture.store.getTask('thread_s29');
      assert.strictEqual(reviewingTask?.status, 'reviewing');
      assert.strictEqual(reviewingTask?.execution?.phase, 'review');
      const activeRunId = reviewingTask?.execution?.run?.id;
      assert.ok(activeRunId);

      // Now inside the Reviewer CLI turn, call approveTask (simulating MCP tool call)
      const approveRes = await reworkController.approveTask({
        threadId: 'thread_s29',
        remarks: 'Code looks good',
      });
      assert.strictEqual(approveRes.status, 'WAITING_MERGE');

      // CRITICAL ASSERTION: While CLI is still held at barrier:
      // 1. The Reviewer run is STILL active in execution!
      // 2. The phase is STILL 'review'!
      // 3. The next phase ('merge') has NOT started yet!
      const midTask = fixture.store.getTask('thread_s29');
      assert.strictEqual(midTask?.execution?.run?.id, activeRunId);
      assert.strictEqual(midTask?.execution?.phase, 'review');

      // Now release the CLI barrier with NON-ZERO exit code (simulating tail exit error)
      releaseCliBarrier!({ success: false, exitCode: 1 });

      // Wait for ExecutionController to consume the verdict
      const deadline = Date.now() + 30_000;
      let finalTask = fixture.store.getTask('thread_s29');
      while (finalTask?.execution?.phase !== 'merge' && Date.now() < deadline) {
        await new Promise((r) => setTimeout(r, 100));
        fixture.store.loadFromDisk();
        finalTask = fixture.store.getTask('thread_s29');
      }

      // Assert: The non-zero tail exit did NOT revoke the persistent approval!
      // Phase transitioned to 'merge' and old reviewer run was cleanly released!
      assert.strictEqual(finalTask?.execution?.phase, 'merge');
      assert.ok(finalTask?.worktree?.approvedCommit);
    } finally {
      await fixture.cleanup();
      try { fs.rmSync(gitDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 30: Controlled CAS concurrency on rework and approve rejects stale generation/run/status without wiping out new generation
  test('Scenario 30: Controlled CAS concurrency on rework and approve rejects stale generation/run/status without wiping out new generation', async () => {
    const fixture = await setupFixture();
    const reworkController = new ReworkController(
      fixture.store,
      fixture.runner as any,
      fixture.callbackManager,
      fixture.detector,
      fixture.registry,
    );
    try {
      // 1. requestRework: Intercept first CAS attempt, inject generation 2 with active run
      const thread1 = createThread('thread_s30_rework', 'develop');
      thread1.status = 'waiting_review';
      thread1.rounds[0].status = 'completed';
      thread1.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_1',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(thread1);

      let injectedReworkRace = false;
      const originalTransition = fixture.store.transitionExecution.bind(fixture.store);
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_rework' && !injectedReworkRace) {
          injectedReworkRace = true;
          // Concurrent writer commits generation 2 with active run!
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.generation = 2;
              t.execution.operationId = 'op_s30_gen2';
              t.execution.run = {
                id: 'new-generation-run',
                owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
                state: 'active',
                step: 'executing',
                startedAt: new Date().toISOString(),
              };
            }
            t.status = 'reviewing';
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // requestRework should reject and NOT wipe out generation 2
      await assert.rejects(
        async () => {
          await reworkController.requestRework({
            threadId: 'thread_s30_rework',
            feedback: 'Rework needed for scenario 30',
          });
        },
        /Cannot request rework: execution operation changed concurrently|Cannot request rework: execution generation changed concurrently/,
      );

      // Verify generation 2 and its active run were PRESERVED!
      fixture.store.loadFromDisk();
      const afterReworkConflict = fixture.store.getTask('thread_s30_rework');
      assert.strictEqual(afterReworkConflict?.execution?.generation, 2);
      assert.strictEqual(afterReworkConflict?.execution?.run?.id, 'new-generation-run');
      assert.strictEqual(afterReworkConflict?.status, 'reviewing');

      // 2. approveTask: Intercept first CAS attempt, inject generation 2 with active run
      const thread2 = createThread('thread_s30_approve', 'review');
      thread2.status = 'waiting_review';
      thread2.rounds[0].status = 'completed';
      thread2.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_2',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(thread2);

      let injectedApproveRace = false;
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_approve' && !injectedApproveRace) {
          injectedApproveRace = true;
          // Concurrent writer commits generation 2 with active run!
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.generation = 2;
              t.execution.operationId = 'op_s30_approve_gen2';
              t.execution.run = {
                id: 'new-gen-run-approve',
                owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
                state: 'active',
                step: 'executing',
                startedAt: new Date().toISOString(),
              };
            }
            t.status = 'reviewing';
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // approveTask should reject and NOT wipe out generation 2
      await assert.rejects(
        async () => {
          await reworkController.approveTask({
            threadId: 'thread_s30_approve',
            remarks: 'Approving scenario 30',
          });
        },
        /Cannot approve task: execution operation changed concurrently|Cannot approve task: execution generation changed concurrently/,
      );

      fixture.store.loadFromDisk();
      const afterApproveConflict = fixture.store.getTask('thread_s30_approve');
      assert.strictEqual(afterApproveConflict?.execution?.generation, 2);
      assert.strictEqual(afterApproveConflict?.execution?.run?.id, 'new-gen-run-approve');
      assert.strictEqual(afterApproveConflict?.status, 'reviewing');

      // 3. Cross with pause: task paused concurrently
      const threadPause = createThread('thread_s30_pause', 'review');
      threadPause.status = 'paused';
      threadPause.rounds[0].status = 'completed';
      threadPause.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_pause',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(threadPause);

      await assert.rejects(
        async () => {
          await reworkController.requestRework({ threadId: 'thread_s30_pause', feedback: 'test pause' });
        },
        /paused/,
      );
      await assert.rejects(
        async () => {
          await reworkController.approveTask({ threadId: 'thread_s30_pause', remarks: 'test pause' });
        },
        /paused/,
      );

      // 4. Cross with wait_decision: task in wait_decision
      const threadDecision = createThread('thread_s30_decision', 'review');
      threadDecision.status = 'wait_decision';
      threadDecision.rounds[0].status = 'completed';
      threadDecision.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_dec',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(threadDecision);

      await assert.rejects(
        async () => {
          await reworkController.requestRework({ threadId: 'thread_s30_decision', feedback: 'test dec' });
        },
        /wait_decision/,
      );
      await assert.rejects(
        async () => {
          await reworkController.approveTask({ threadId: 'thread_s30_decision', remarks: 'test dec' });
        },
        /wait_decision/,
      );

      // 5. Unrelated checkpoint retry: same generation and run, but revision bumped by checkpoint
      fixture.store.transitionExecution = originalTransition;

      const threadCheckpoint = createThread('thread_s30_cp', 'review');
      threadCheckpoint.status = 'waiting_review';
      threadCheckpoint.rounds[0].status = 'completed';
      threadCheckpoint.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_cp',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-cp',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadCheckpoint);

      let injectedCheckpoint = false;
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_cp' && !injectedCheckpoint) {
          injectedCheckpoint = true;
          // Unrelated checkpoint: budget updated, revision bumped to 2
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.budget.spentActiveMs = 2000;
            }
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // requestRework should succeed on retry because operation/generation/run identity matched!
      const reworkRes = await reworkController.requestRework({
        threadId: 'thread_s30_cp',
        feedback: 'Valid rework on retry',
      });
      assert.strictEqual(reworkRes.status, 'REWORK_DISPATCHED');
      const taskAfterCp = fixture.store.getTask('thread_s30_cp');
      // Has active run was preserved because reviewer is still running!
      assert.strictEqual(taskAfterCp?.execution?.run?.id, 'run-cp');
      assert.strictEqual(taskAfterCp?.currentRound, 2);
    } finally {
      await fixture.cleanup();
    }
  });
});
