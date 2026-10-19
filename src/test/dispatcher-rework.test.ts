import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';

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
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager);
    reworkController = new ReworkController(taskStore, runner, callbackManager);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

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
  });

  test('should request rework strictly reusing same receiverSessionId and incrementing round', async () => {
    const dispatchRes = await dispatcher.dispatchTask({
      targetAgent: 'claude_code',
      title: 'Implement OAuth PKCE',
      prompt: 'Implement PKCE flow in auth.ts',
    });

    const originalSessionId = dispatchRes.receiverSessionId;
    assert.ok(originalSessionId);

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
    await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Fix something',
      callerSessionId: 'antigravity', // Invalid!
    });

    const thread2 = taskStore.getTask(dispatchRes.threadId);
    assert.ok(thread2);
    assert.notStrictEqual(thread2.callerSessionId, 'antigravity');

    // 3. Requesting rework with a valid UUID
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

    // Request rework
    await reworkController.requestRework({
      threadId: dispatchRes.threadId,
      feedback: 'Handle comments in JSON',
    });

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
