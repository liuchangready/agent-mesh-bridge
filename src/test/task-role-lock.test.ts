import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-role-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { ExecutionGate } from '../bridge/execution-gate';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { DelegationThread } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_role_1',
    title: 'Role lock probe',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'running',
    currentRound: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    receiverSessionId: 'session-role-0001',
    callerSessionId: 'rollout-role-0001',
    workspacePath: os.tmpdir(),
    summary: {
      lastPromptSnippet: 'do the work',
      lastOutputSnippet: 'Executing...',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'do the work',
        status: 'running',
        durationMs: 0,
        timestamp: new Date().toISOString(),
      },
    ],
    ...overrides,
  };
}

describe('TaskRoleLock - one role per task', () => {
  let tempDir: string;
  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-role-'));
  });
  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('reviewer and developer cannot hold the same task at once', () => {
    const first = new TaskRoleLock(tempDir);
    const second = new TaskRoleLock(tempDir);

    const reviewer = first.tryAcquire('thread_role_1', 'reviewer');
    assert.ok(reviewer);
    assert.strictEqual(second.tryAcquire('thread_role_1', 'developer'), null);
    assert.strictEqual(second.getOwner('thread_role_1')?.role, 'reviewer');
    assert.strictEqual(second.releaseIfRole('thread_role_1', 'developer'), false);

    // The reviewer submits its decision: hand off to developer.
    assert.strictEqual(second.releaseIfRole('thread_role_1', 'reviewer'), true);
    const developer = second.tryAcquire('thread_role_1', 'developer');
    assert.ok(developer);
    assert.strictEqual(second.getOwner('thread_role_1')?.role, 'developer');
    second.release(developer!);
  });

  test('a detached reviewer child keeps the role alive after its launcher stops', () => {
    const lock = new TaskRoleLock(tempDir);
    const file = lock.getLockFilePath('thread_child');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const write = (owner: Record<string, unknown>) =>
      fs.writeFileSync(file, JSON.stringify(owner), 'utf8');

    // The wake launches the reviewer detached: the launcher exits (dead pid,
    // heartbeat long gone) while the caller CLI keeps reviewing. The role must
    // stay valid, otherwise the review's own approve/rework calls are refused.
    write({
      token: 'child-token',
      threadId: 'thread_child',
      role: 'reviewer',
      pid: 999_999,
      childPid: process.pid,
      acquiredAt: Date.now() - 10 * 60_000,
      updatedAt: Date.now() - 10 * 60_000,
    });

    const observer = new TaskRoleLock(tempDir);
    const owner = observer.getOwner('thread_child');
    assert.strictEqual(owner?.role, 'reviewer');
    assert.ok(observer.describe(owner).includes('reviewer child'));

    // Both processes gone: the role is dead again and can be reclaimed.
    write({
      token: 'child-token',
      threadId: 'thread_child',
      role: 'reviewer',
      pid: 999_999,
      childPid: 999_998,
      acquiredAt: Date.now() - 10 * 60_000,
      updatedAt: Date.now() - 10 * 60_000,
    });
    assert.strictEqual(observer.getOwner('thread_child'), undefined);
  });

  test('attachChild binds the role to the spawned caller process', () => {
    const lock = new TaskRoleLock(tempDir);
    const lease = lock.tryAcquire('thread_attach', 'reviewer');
    assert.ok(lease);

    assert.strictEqual(lock.attachChild(lease, process.pid), true);
    assert.strictEqual(lock.getOwner('thread_attach')?.childPid, process.pid);

    // A foreign lease token must not be able to rebind someone else's role.
    assert.strictEqual(
      lock.attachChild({ token: 'not-the-owner', threadId: 'thread_attach', role: 'reviewer' }, 4242),
      false,
    );
    lock.release(lease!);
  });

  test('a stale role lock is reclaimed', () => {
    const lock = new TaskRoleLock(tempDir);
    const file = lock.getLockFilePath('thread_stale');
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const staleAt = Date.now() - 5 * 60_000;
    fs.writeFileSync(
      file,
      JSON.stringify({
        token: 'stale-token',
        threadId: 'thread_stale',
        role: 'reviewer',
        pid: 999_999,
        acquiredAt: staleAt,
        updatedAt: staleAt,
      }),
      'utf8',
    );

    assert.strictEqual(lock.getOwner('thread_stale'), undefined);
    const lease = lock.tryAcquire('thread_stale', 'developer');
    assert.ok(lease);
    assert.strictEqual(lock.getOwner('thread_stale')?.role, 'developer');
    lock.release(lease!);
  });
});

describe('Task role enforcement in execution', () => {
  test('developer waits while reviewer owns the task, then starts after handoff', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-role-exec-'));
    const store = new TaskStore(tempDir);
    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    let executions = 0;
    runner.execute = async () => {
      executions++;
      return {
        exitCode: 0,
        stdout: 'done',
        stderr: '',
        extractedOutput: 'done',
        durationMs: 5,
      };
    };
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const supervisor = new TaskSupervisor(
      store,
      runner,
      callbackManager,
      detector,
      new ExecutionGate(tempDir),
    );

    const reviewer = new TaskRoleLock().tryAcquire('thread_role_1', 'reviewer');
    assert.ok(reviewer);

    try {
      const thread = makeThread();
      store.upsertTask(thread);
      await supervisor.executeThreadRound(thread, thread.rounds[0]);

      assert.strictEqual(executions, 0, 'developer must not start during reviewer role');
      assert.strictEqual(store.getTask('thread_role_1')?.status, 'queued');

      new TaskRoleLock().release(reviewer!);
      supervisor.checkPendingTasks();

      const deadline = Date.now() + 2_000;
      while (Date.now() < deadline) {
        if (store.getTask('thread_role_1')?.status === 'waiting_review') break;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }

      assert.strictEqual(executions, 1, 'developer starts after reviewer handoff');
      assert.strictEqual(store.getTask('thread_role_1')?.status, 'waiting_review');
    } finally {
      supervisor.dispose();
      store.dispose();
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch {}
    }
  });

  test('a second reviewer wake-up is refused while one is active', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-role-cb-'));
    const store = new TaskStore(tempDir);
    const thread = makeThread({
      status: 'waiting_review',
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'do the work',
          status: 'completed',
          durationMs: 10,
          timestamp: new Date().toISOString(),
          output: 'report',
        },
      ],
    });
    store.upsertTask(thread);

    const reviewer = new TaskRoleLock().tryAcquire(thread.threadId, 'reviewer');
    assert.ok(reviewer);

    try {
      const callbackManager = new CallbackManager(new AgentDetector());
      const accepted = await callbackManager.notifyCaller(thread, thread.rounds[0]);
      assert.strictEqual(accepted, false, 'duplicate reviewer must be rejected');
    } finally {
      new TaskRoleLock().release(reviewer!);
      store.dispose();
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch {}
    }
  });
});
