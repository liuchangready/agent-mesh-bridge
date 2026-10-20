import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// Keep `TaskSupervisor.isSupervisorActive()` from observing another test file's
// heartbeat while exercising the fallback queue pump.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-gate-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { ExecutionGate } from '../bridge/execution-gate';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';

describe('ExecutionGate - cross-process single flight', () => {
  let tempDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-gate-'));
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('only one live lease may exist at a time', () => {
    const first = new ExecutionGate(tempDir);
    const second = new ExecutionGate(tempDir);

    const lease = first.tryAcquire('thread_a', 1);
    assert.ok(lease);
    assert.strictEqual(second.tryAcquire('thread_b', 1), null, 'second task must be refused');
    assert.strictEqual(second.isBusy(), true);
    assert.strictEqual(second.getOwner()?.threadId, 'thread_a');

    first.release(lease!);
    assert.strictEqual(second.isBusy(), false);

    const next = second.tryAcquire('thread_b', 1);
    assert.ok(next);
    second.release(next!);
  });

  test('a stale lock from a dead process is reclaimed', () => {
    const gate = new ExecutionGate(tempDir);
    const lockPath = gate.getLockFilePath();
    const staleAt = Date.now() - 5 * 60_000;
    fs.writeFileSync(
      lockPath,
      JSON.stringify({
        token: 'dead-token',
        pid: 999_999,
        threadId: 'thread_dead',
        round: 1,
        acquiredAt: staleAt,
        updatedAt: staleAt,
      }),
      'utf8',
    );

    assert.strictEqual(gate.isBusy(), false);
    const lease = gate.tryAcquire('thread_live', 1);
    assert.ok(lease, 'stale lease should be reclaimable');
    assert.strictEqual(gate.getOwner()?.threadId, 'thread_live');
    gate.release(lease!);
  });
});

describe('Global serialization - dispatcher fallback queue', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let dispatcher: AsyncDispatcher;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-global-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    const resolver = new ActiveAgentResolver(detector);
    runner = new AutonomousRunner(detector);
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    dispatcher = new AsyncDispatcher(
      taskStore,
      resolver,
      runner,
      callbackManager,
      detector,
      new ExecutionGate(tempDir),
    );
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('starts one task at a time and drains the next automatically', async () => {
    let releaseA: () => void = () => {};
    const aGate = new Promise<void>((resolve) => {
      releaseA = resolve;
    });

    let inFlight = 0;
    let maxInFlight = 0;

    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      try {
        if (options.title === 'Task A') {
          await aGate;
        } else {
          await new Promise((r) => setTimeout(r, 10));
        }
        return {
          exitCode: 0,
          stdout: 'done',
          stderr: '',
          extractedOutput: 'done',
          durationMs: 10,
        };
      } finally {
        inFlight--;
      }
    };

    const resA = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Task A',
      prompt: 'first',
      expectedFiles: ['src/a.ts'],
    });
    assert.strictEqual(resA.status, 'DISPATCHED');

    const resB = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Task B',
      prompt: 'second',
      expectedFiles: ['src/b.ts'],
    });
    assert.strictEqual(resB.status, 'QUEUED', 'a disjoint second task is still globally queued');
    assert.strictEqual(taskStore.getTask(resB.threadId)?.status, 'queued');

    releaseA();

    const waitFor = async (threadId: string, status: string, timeoutMs = 3_000) => {
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        if (taskStore.getTask(threadId)?.status === status) return;
        await new Promise((r) => setTimeout(r, 25));
      }
    };

    await waitFor(resA.threadId, 'waiting_review');
    assert.strictEqual(taskStore.getTask(resA.threadId)?.status, 'waiting_review');
    assert.strictEqual(
      taskStore.getTask(resB.threadId)?.status,
      'queued',
      'B must wait even though A is only waiting_review, not completed',
    );

    // Task-level queue: only A's completion releases B.
    taskStore.updateRound(resA.threadId, 1, { status: 'completed' }, 'completed');
    await waitFor(resB.threadId, 'waiting_review');

    assert.strictEqual(taskStore.getTask(resB.threadId)?.status, 'waiting_review');
    assert.strictEqual(maxInFlight, 1, 'the fallback pump must not overlap rounds');
  });
});
