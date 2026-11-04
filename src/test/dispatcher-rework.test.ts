import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// Isolate the supervisor heartbeat and execution lock from concurrently running
// test files. `TaskSupervisor.isSupervisorActive()` and `ExecutionGate` both read
// the home directory.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-dispatch-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';
import { ExecutionGate } from '../bridge/execution-gate';
import { getBridgeHome, invalidateConfiguredModelCache, saveModelDefaults } from '../bridge/model-catalog';
import { clearReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { describeModelRouting } from '../bridge/types';

describe('AsyncDispatcher and ReworkController Tests', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let detector: AgentDetector;
  let resolver: ActiveAgentResolver;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let dispatcher: AsyncDispatcher;
  let reworkController: ReworkController;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-dispatch-'));
    taskStore = new TaskStore(tempDir);
    detector = new AgentDetector();
    resolver = new ActiveAgentResolver(detector);
    runner = new AutonomousRunner(detector);
    runner.execute = async () => {
      await new Promise((r) => setTimeout(r, 50));
      return {
        exitCode: 0,
        stdout: 'Mock execution finished successfully',
        stderr: '',
        extractedOutput: 'Mock execution finished successfully',
        durationMs: 100,
      };
    };
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const gate = new ExecutionGate(tempDir);
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, gate);
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  /**
   * Wait until the mock receiver process has actually released its developer
   * role. Role enforcement (new) intentionally blocks the next role while the
   * previous round is still executing.
   */
  const waitForStatus = async (threadId: string, status: string, timeoutMs = 5000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (taskStore.getTask(threadId)?.status === status) return;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    throw new Error(
      `Timed out waiting for ${threadId} to reach ${status}; current=${
        taskStore.getTask(threadId)?.status
      }`,
    );
  };

  test('should dispatch task asynchronously and create thread with round 1', async () => {
    const res = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Optimize SQL Query',
      prompt: 'Add composite index on (tenant_id, created_at)',
      callerSessionId: 'caller-session-001',
    });

    assert.strictEqual(res.status, 'DISPATCHED');
    assert.strictEqual(res.targetAgent, 'codex');
    assert.ok(res.threadId.startsWith('thread_'));
    assert.ok(res.receiverSessionId);

    const stored = taskStore.getTask(res.threadId);
    assert.ok(stored);
    assert.strictEqual(stored.title, 'Optimize SQL Query');
    assert.strictEqual(stored.currentRound, 1);
    assert.strictEqual(stored.status, 'running');
    assert.strictEqual(stored.receiverSessionId, res.receiverSessionId);
    assert.strictEqual(stored.rounds.length, 1);
    assert.strictEqual(stored.rounds[0].type, 'initial');
    // No model pinned: the agent's configured default applies at launch time.
    assert.strictEqual(stored.targetModel, undefined);
    assert.strictEqual(stored.initiatorModel, undefined);
    assert.strictEqual(stored.initiatorModelSource, 'none');
  });

  test('refuses a same-agent dispatch while the legacy review flow is active', async () => {
    await assert.rejects(
      () =>
        dispatcher.dispatchTask({
          targetAgent: 'codex',
          initiatorAgent: 'codex',
          title: 'Self dispatch',
          prompt: 'do the work',
        }),
      /self-dispatch requires the dedicated reviewer flow/i,
    );
    // Rejected before the thread exists: it must not occupy the queue.
    assert.strictEqual(taskStore.getAllTasks().length, 0);
  });

  test('refuses a same-agent dispatch detected from the caller environment (legacy)', async () => {
    const previous = process.env.MESH_CALLER_AGENT;
    process.env.MESH_CALLER_AGENT = 'deepseek_harness';
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({
            targetAgent: 'deepseek_harness',
            title: 'Self dispatch via env',
            prompt: 'do the work',
          }),
        /self-dispatch requires the dedicated reviewer flow/i,
      );
      assert.strictEqual(taskStore.getAllTasks().length, 0);
    } finally {
      if (previous === undefined) delete process.env.MESH_CALLER_AGENT;
      else process.env.MESH_CALLER_AGENT = previous;
    }
  });

  test('dedicated flow prepares a reviewer bootstrap instead of starting the developer', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, {
      initiatorModels: { codex: 'gpt-6-pro' },
    });
    invalidateConfiguredModelCache();
    let executed = 0;
    runner.execute = async () => {
      executed += 1;
      return {
        exitCode: 0,
        stdout: 'x',
        stderr: '',
        extractedOutput: 'x',
        durationMs: 1,
      };
    };
    try {
      const res = await dispatcher.dispatchTask({
        targetAgent: 'codex',
        initiatorAgent: 'codex',
        title: 'Self review with two models',
        prompt: 'do the work',
        model: 'gpt-6-flash',
        reviewFlow: 'dedicated',
      });
      assert.strictEqual(res.status, 'REVIEWER_STARTING');
      const stored = taskStore.getTask(res.threadId)!;
      assert.strictEqual(stored.reviewFlow, 'dedicated');
      assert.strictEqual(stored.reviewerModel, 'gpt-6-pro');
      assert.strictEqual(stored.targetModel, 'gpt-6-flash');
      assert.strictEqual(stored.reviewBootstrap?.phase, 'pending');
      assert.strictEqual(stored.status, 'queued');
      assert.strictEqual(stored.callerSessionId, undefined, 'caller is the reviewer, not the requester');
      assert.strictEqual(stored.requesterAgent, 'codex');
      // The developer must not run before the reviewer dispatches.
      await new Promise((r) => setTimeout(r, 80));
      assert.strictEqual(executed, 0);
      clearReviewerBootstrapToken(res.threadId, home);
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('dedicated same-agent dispatch rejects an identical reviewer/executor model', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, {
      initiatorModels: { codex: 'gpt-6-flash' },
    });
    invalidateConfiguredModelCache();
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({
            targetAgent: 'codex',
            initiatorAgent: 'codex',
            title: 'Same model self review',
            prompt: 'do the work',
            model: 'gpt-6-flash',
            reviewFlow: 'dedicated',
          }),
        /same model/i,
      );
      assert.strictEqual(taskStore.getAllTasks().length, 0);
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('dedicated same-agent dispatch requires a configured reviewer model', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, { initiatorModels: {} });
    invalidateConfiguredModelCache();
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({
            targetAgent: 'codex',
            initiatorAgent: 'codex',
            title: 'Unconfigured reviewer',
            prompt: 'do the work',
            model: 'gpt-6-flash',
            reviewFlow: 'dedicated',
          }),
        /configure a sender-side reviewer model/i,
      );
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('pins the model chosen at dispatch on the task and the runner call', async () => {
    const received: any[] = [];
    runner.execute = async (options: any) => {
      received.push(options);
      await new Promise((r) => setTimeout(r, 20));
      return {
        exitCode: 0,
        stdout: 'Mock execution finished successfully',
        stderr: '',
        extractedOutput: 'Mock execution finished successfully',
        durationMs: 20,
      };
    };
    // The user confirmed the switch, so the non-default model is applied.
    dispatcher.setModelApprovalHandler(async (request) => ({
      id: request.id,
      status: 'approved',
      at: new Date().toISOString(),
      by: 'user',
    }));

    const res = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Pin a specific model',
      prompt: 'Implement the parser',
      model: '  gpt-6-sol  ',
      modelRequestedByUser: true,
      initiatorModel: 'gpt-6.1-sol',
    });
    await waitForStatus(res.threadId, 'waiting_review');

    const stored = taskStore.getTask(res.threadId)!;
    assert.strictEqual(stored.targetModel, 'gpt-6-sol');
    assert.strictEqual(stored.modelApproval?.status, 'approved');
    // Both ends of the routing are persisted, so the ledger can show them.
    assert.strictEqual(stored.initiatorModel, 'gpt-6.1-sol');
    assert.strictEqual(stored.initiatorModelSource, 'explicit');
    assert.match(res.message, /model routing/i);
    assert.strictEqual(received.length, 1);
    assert.strictEqual(received[0].model, 'gpt-6-sol');
    assert.strictEqual(received[0].agent, 'codex');
  });

  test('records the sender-side default separately from the receiver-side default', async () => {
    const bridgeHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-two-ends-'));
    const previousHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = bridgeHome;
    // Two different agents, each with its own side of the defaults: Antigravity
    // reviews as `gemini-3-pro` while Codex executes as `gpt-6-flash`.
    saveModelDefaults({ codex: 'gpt-6-flash' }, bridgeHome, {
      initiatorModels: { antigravity: 'gemini-3-pro' },
    });
    invalidateConfiguredModelCache();

    try {
      const res = await dispatcher.dispatchTask({
        targetAgent: 'codex',
        title: 'One dispatch, two model defaults',
        prompt: 'Implement the parser',
        allowNoScope: true,
        callerSessionId: 'caller-session-two-ends',
        initiatorAgent: 'antigravity',
      });
      await waitForStatus(res.threadId, 'waiting_review');

      const stored = taskStore.getTask(res.threadId)!;
      assert.strictEqual(stored.targetModel, 'gpt-6-flash', 'receiver uses its own default');
      assert.strictEqual(stored.initiatorModel, 'gemini-3-pro', 'sender is reported on its own default');
      assert.strictEqual(stored.initiatorModelSource, 'sender-configured');
      assert.match(describeModelRouting(stored), /gemini-3-pro/);
      assert.match(describeModelRouting(stored), /gpt-6-flash/);
    } finally {
      if (previousHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = previousHome;
      invalidateConfiguredModelCache();
      fs.rmSync(bridgeHome, { recursive: true, force: true });
    }
  });

  test('ignores a non-default model the caller did not declare as a user request', async () => {
    const received: any[] = [];
    runner.execute = async (options: any) => {
      received.push(options);
      await new Promise((r) => setTimeout(r, 20));
      return {
        exitCode: 0,
        stdout: 'Mock execution finished successfully',
        stderr: '',
        extractedOutput: 'Mock execution finished successfully',
        durationMs: 20,
      };
    };
    dispatcher.setModelApprovalHandler(async () => {
      throw new Error('a guessed model must never reach the user');
    });

    const res = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Do not guess a model',
      prompt: 'Implement the parser',
      model: 'gpt-6-luna',
    });
    await waitForStatus(res.threadId, 'waiting_review');

    const stored = taskStore.getTask(res.threadId)!;
    // No configured default in this isolated home, so nothing is pinned.
    assert.strictEqual(stored.targetModel, undefined);
    assert.match(String(stored.modelNotice || ''), /ignored/);
    assert.match(res.message, /ignored/);
    assert.strictEqual(received[0].model, undefined);
  });

  test('a declined model switch aborts the dispatch before anything is queued', async () => {
    dispatcher.setModelApprovalHandler(async (request) => ({
      id: request.id,
      status: 'cancelled',
      at: new Date().toISOString(),
      by: 'user',
    }));

    await assert.rejects(
      () =>
        dispatcher.dispatchTask({
          targetAgent: 'codex',
          title: 'Declined switch',
          prompt: 'Implement the parser',
          model: 'gpt-6-luna',
          modelRequestedByUser: true,
        }),
      (err: any) => err?.code === 'MODEL_APPROVAL_DENIED',
    );

    assert.strictEqual(taskStore.count(), 0);
  });

  test('should treat exit 0 with no actual output as a failed response', async () => {
    runner.execute = async () => ({
      exitCode: 0,
      stdout: '{"type":"turn.started"}\n{"type":"turn.completed"}',
      stderr: '',
      extractedOutput: '',
      durationMs: 10,
      error:
        'Process exited with code 0 but the stream contained no actual final answer (only status/metadata or no output).',
      assessment: {
        success: false,
        errorCode: 'EMPTY_OUTPUT',
        exitCode: 0,
        stream: {
          chunkCount: 2,
          stdoutBytes: 48,
          stderrBytes: 0,
          firstOutputAtMs: 5,
          lastOutputAtMs: 8,
          maxGapMs: 3,
          gapThresholdMs: 180_000,
          hasActualOutput: false,
          continuous: true,
        },
        reason: 'no actual output',
      },
    });

    const res = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Empty output probe',
      prompt: 'return nothing',
    });
    await waitForStatus(res.threadId, 'failed');

    const thread = taskStore.getTask(res.threadId)!;
    assert.strictEqual(thread.status, 'failed');
    assert.strictEqual(thread.rounds[0].status, 'failed');
    assert.strictEqual(thread.rounds[0].assessment?.errorCode, 'EMPTY_OUTPUT');
    assert.match(thread.rounds[0].error || '', /no actual|actual final answer/i);
  });

  test('refuses a rework that would move a legacy task onto its own reviewer', async () => {
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'claude_code',
      title: 'Rework onto the reviewer',
      prompt: 'do the work',
      initiatorAgent: 'codex',
    });
    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    await assert.rejects(
      () =>
        reworkController.requestRework({
          threadId: dispatchRes.threadId,
          feedback: 'and now do it yourself',
          targetAgent: 'codex',
        }),
      /legacy review flow/i,
    );

    // Rejected before a new round exists: still one round, still in review.
    const thread = taskStore.getTask(dispatchRes.threadId)!;
    assert.strictEqual(thread.rounds.length, 1);
    assert.strictEqual(thread.status, 'waiting_review');
    assert.strictEqual(thread.targetAgent, 'claude_code');
  });

  test('should request rework strictly reusing same receiverSessionId and incrementing round', async () => {
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'claude_code',
      title: 'Implement OAuth PKCE',
      prompt: 'Implement PKCE flow in auth.ts',
    });

    const originalSessionId = dispatchRes.receiverSessionId;
    assert.ok(originalSessionId);

    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    const reworkRes = await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Code verifier check is missing SHA256 hashing. Please fix.',
    });

    assert.strictEqual(reworkRes.status, 'REWORK_DISPATCHED');
    assert.strictEqual(reworkRes.round, 2);
    // CRITICAL DESIGN INVARIANT: Receiver session MUST be identical!
    assert.strictEqual(reworkRes.receiverSessionId, originalSessionId);

    const thread = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread);
    assert.strictEqual(thread.currentRound, 2);
    assert.strictEqual(thread.status, 'reworking');
    assert.strictEqual(thread.receiverSessionId, originalSessionId);
    assert.strictEqual(thread.rounds.length, 2);
    assert.strictEqual(thread.rounds[1].type, 'rework');
    assert.strictEqual(
      thread.rounds[1].prompt,
      'Code verifier check is missing SHA256 hashing. Please fix.',
    );
  });

  test('request_rework can move the task to another agent on a fresh session', async () => {
    const received: any[] = [];
    runner.execute = async (options: any) => {
      received.push(options);
      await new Promise((r) => setTimeout(r, 20));
      return {
        exitCode: 0,
        stdout: 'Mock execution finished successfully',
        stderr: '',
        extractedOutput: 'Mock execution finished successfully',
        durationMs: 20,
      };
    };

    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'claude_code',
      title: 'Cross-agent rework',
      prompt: 'Implement the flow',
    });
    const originalSessionId = dispatchRes.receiverSessionId;
    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    const reworkRes = await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Also cover the cancel path.',
      targetAgent: 'codex',
    });

    assert.strictEqual(reworkRes.status, 'REWORK_DISPATCHED');
    assert.strictEqual(reworkRes.targetAgent, 'codex');
    // A different agent cannot resume the previous CLI session.
    assert.notStrictEqual(reworkRes.receiverSessionId, originalSessionId);
    assert.ok(reworkRes.retarget, 'the switch must be recorded on the rework result');

    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    const thread = taskStore.getTask(dispatchRes.threadId)!;
    assert.strictEqual(thread.targetAgent, 'codex');
    assert.strictEqual(thread.retargets?.length, 1);
    const switchedRun = received.find((options) => options.agent === 'codex');
    assert.ok(switchedRun, 'the rework round must execute on the new agent');
    assert.strictEqual(switchedRun.isRework, false);
  });

  test('should approve task and mark it completed', async () => {
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Format code according to styleguide',
      prompt: 'Run prettier and eslint',
    });

    // A thread may only be approved once nothing is in flight.
    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    const approveRes = reworkController.approveTask({
      threadId: dispatchRes.threadId,
      remarks: 'All lint checks passed.',
    });

    assert.strictEqual(approveRes.status, 'COMPLETED');

    const thread = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread);
    assert.strictEqual(thread.status, 'completed');
  });

  test('should reject invalid callerSessionId (agent literals) and fallback properly', async () => {
    // 1. Dispatching with invalid callerSessionId "claude_code"
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Invalid callerSessionId test',
      prompt: 'Check caller session sanitization',
      callerSessionId: 'claude_code', // Invalid! Should be ignored
    });

    const thread1 = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread1);
    assert.notStrictEqual(thread1.callerSessionId, 'claude_code');

    // 2. Requesting rework with invalid callerSessionId "antigravity"
    await waitForStatus(dispatchRes.threadId, 'waiting_review');
    await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Fix something',
      callerSessionId: 'antigravity', // Invalid!
    });

    const thread2 = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread2);
    assert.notStrictEqual(thread2.callerSessionId, 'antigravity');

    // 3. Requesting rework with a valid UUID
    await waitForStatus(dispatchRes.threadId, 'waiting_review');
    const validUuid = '12345678-abcd-ef01-2345-6789abcdef01';
    await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Fix something else',
      callerSessionId: validUuid,
    });

    const thread3 = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread3);
    assert.strictEqual(thread3.callerSessionId, validUuid);
  });

  test('should retry failed round preserving original round number and prompt', async () => {
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Fix edge case in parser',
      prompt: 'Handle trailing comma in JSON',
    });

    // Request rework (round 1 must be finished first)
    await waitForStatus(dispatchRes.threadId, 'waiting_review');
    await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Handle comments in JSON',
    });
    await waitForStatus(dispatchRes.threadId, 'waiting_review');

    const thread = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread);
    assert.strictEqual(thread.rounds.length, 2);

    // Simulate failure on round 2
    taskStore.updateRound(
      thread.threadId,
      2,
      { status: 'failed', error: 'Network timeout', durationMs: 5000 },
      'failed',
    );

    const failedThread = taskStore.getTask(dispatchRes.threadId);
    assert.ok(failedThread);
    assert.strictEqual(failedThread.status, 'failed');
    assert.strictEqual(failedThread.rounds[1].status, 'failed');

    // Retry failed round
    const retriedRound = await reworkController.retryFailedRound(dispatchRes.threadId);
    assert.strictEqual(retriedRound.round, 2);
    assert.strictEqual(retriedRound.status, 'running');
    assert.strictEqual(retriedRound.prompt, 'Handle comments in JSON');

    const recoveredThread = taskStore.getTask(dispatchRes.threadId);
    assert.ok(recoveredThread);
    assert.strictEqual(recoveredThread.status, 'reworking');
    assert.strictEqual(recoveredThread.rounds.length, 2); // No duplicate round created!
    assert.strictEqual(recoveredThread.rounds[1].status, 'running');
  });
});
