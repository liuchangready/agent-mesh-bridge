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
  const waitForStatus = async (threadId: string, status: string, timeoutMs = 2000) => {
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

    const res = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Pin a specific model',
      prompt: 'Implement the parser',
      model: '  gpt-6-sol  ',
      initiatorModel: 'gpt-6.1-sol',
    });
    await waitForStatus(res.threadId, 'waiting_review');

    const stored = taskStore.getTask(res.threadId)!;
    assert.strictEqual(stored.targetModel, 'gpt-6-sol');
    // Both ends of the routing are persisted, so the ledger can show them.
    assert.strictEqual(stored.initiatorModel, 'gpt-6.1-sol');
    assert.strictEqual(stored.initiatorModelSource, 'explicit');
    assert.match(res.message, /model routing/i);
    assert.strictEqual(received.length, 1);
    assert.strictEqual(received[0].model, 'gpt-6-sol');
    assert.strictEqual(received[0].agent, 'codex');
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
