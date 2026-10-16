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
    callbackManager = new CallbackManager(detector);
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
});
