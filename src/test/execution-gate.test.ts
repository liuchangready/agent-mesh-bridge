import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as child_process from 'child_process';
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

  test('records and clears the real agent child on the live lease', () => {
    const gate = new ExecutionGate(tempDir);
    const lease = gate.tryAcquire('thread_child', 1);
    assert.ok(lease);

    gate.attachChildPid(4242);
    const withChild = JSON.parse(fs.readFileSync(gate.getLockFilePath(), 'utf8'));
    assert.strictEqual(withChild.childPid, 4242);

    gate.attachChildPid(undefined);
    const cleared = JSON.parse(fs.readFileSync(gate.getLockFilePath(), 'utf8'));
    assert.strictEqual(cleared.childPid, undefined);

    gate.release(lease!);
  });

  test('killOrphanChild leaves a live host and other tasks alone', () => {
    const gate = new ExecutionGate(tempDir, { staleAfterMs: 100, orphanGraceMs: 500 });
    const child = child_process.spawn(
      process.execPath,
      ['-e', 'setInterval(() => {}, 1000)'],
      { stdio: 'ignore', windowsHide: true },
    );

    try {
      fs.writeFileSync(
        gate.getLockFilePath(),
        JSON.stringify({
          token: 'live-token',
          pid: process.pid, // this test process is the live host
          childPid: child.pid,
          threadId: 'thread_live_host',
          round: 1,
          acquiredAt: Date.now(),
          updatedAt: Date.now(),
        }),
        'utf8',
      );

      assert.strictEqual(
        gate.killOrphanChild('thread_live_host'),
        false,
        'a live host handles its own child through the cancellation file',
      );
      assert.strictEqual(gate.killOrphanChild('thread_other'), false);

      // With force=true, the child is killed even if host is alive
      assert.strictEqual(
        gate.killOrphanChild('thread_live_host', true),
        true,
        'force=true kills active child even when host is alive',
      );
      const ownerAfter = JSON.parse(fs.readFileSync(gate.getLockFilePath(), 'utf8'));
      assert.strictEqual(ownerAfter.childPid, undefined, 'childPid is cleared from lock file');
    } finally {
      try {
        child.kill('SIGKILL');
      } catch {}
    }
  });

  test('an orphaned agent child keeps the gate busy and is killed before takeover', async () => {
    const child = child_process.spawn(
      process.execPath,
      ['-e', 'setInterval(() => {}, 1000)'],
      { stdio: 'ignore', windowsHide: true },
    );
    let childExited = false;
    child.once('exit', () => {
      childExited = true;
    });

    try {
      assert.ok(child.pid);
      const gate = new ExecutionGate(tempDir, { staleAfterMs: 100, orphanGraceMs: 500 });
      const now = Date.now();
      fs.writeFileSync(
        gate.getLockFilePath(),
        JSON.stringify({
          token: 'orphan-token',
          pid: 999_999, // the host that spawned the agent is gone
          childPid: child.pid, // the agent itself is still alive
          threadId: 'thread_orphan',
          round: 1,
          acquiredAt: now - 200,
          updatedAt: now - 200, // stale heartbeat, still inside the orphan grace
        }),
        'utf8',
      );

      // Inside the grace window a surviving agent still owns the slot: starting
      // the next round now would put two agents on the same workspace.
      assert.strictEqual(gate.tryAcquire('thread_next', 1), null);
      assert.strictEqual(gate.getOwner()?.threadId, 'thread_orphan');

      // Past the grace window the orphan is killed, then the slot is taken over.
      await new Promise((resolve) => setTimeout(resolve, 550));
      const lease = gate.tryAcquire('thread_next', 1);
      assert.ok(lease, 'the gate must be reclaimable once the orphan is past its grace');

      await new Promise((resolve) => setTimeout(resolve, 400));
      assert.strictEqual(childExited, true, 'the orphaned agent process must be killed');
      gate.release(lease!);
    } finally {
      if (!childExited) {
        try {
          child.kill('SIGKILL');
        } catch {}
      }
    }
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
    callbackManager.notifyCallerOfFailure = async () => false;
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
