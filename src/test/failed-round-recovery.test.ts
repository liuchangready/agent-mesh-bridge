import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { createThread, setupFixture } from './unified-execution-fixture';
import { ReworkController } from '../bridge/rework-controller';
import type { DelegationThread } from '../bridge/types';

const reason = 'The task role lease is not held during native-session preparation.';

function failedThread(): DelegationThread {
  const thread = createThread('thread_failed_preparation');
  thread.status = 'failed';
  thread.currentRound = thread.execution!.round = 2;
  thread.receiverSessionId = '01a1f86c-f294-7df3-858b-5f286aaa0ffd';
  thread.rounds[0].status = 'completed';
  thread.rounds.push({ round: 2, type: 'rework', prompt: 'Continue the original revision',
    status: 'running', durationMs: 0, timestamp: thread.updatedAt });
  thread.failure = { stage: 'execution', code: 'EXECUTION_ERROR', reason, at: thread.updatedAt, round: 2 };
  return thread;
}

test('a preparation exception finalizes the current failed work round', async () => {
  const f = await setupFixture({ useFastLock: true });
  try {
    const thread = createThread('thread_preparation_throw');
    f.store.upsertTask(thread);
    f.runner.setOnExecute(async () => { throw new Error(reason); });
    await f.controller.startExecution(thread.threadId);
    await f.controller.waitForActiveExecutions();
    const failed = f.store.getTask(thread.threadId)!;
    assert.equal(failed.status, 'failed');
    assert.equal(failed.rounds[0].status, 'failed');
    assert.equal(failed.rounds[0].error, reason);
    assert.equal(failed.execution?.run, undefined);
    assert.equal(failed.receiverSessionId, thread.receiverSessionId);
  } finally { await f.cleanup(); }
});

test('retry reconciles only the idle preparation failure, preserving round, session and prompt', async () => {
  const f = await setupFixture({ useFastLock: true });
  const rework = new ReworkController(f.store, f.runner as any, f.callbackManager, f.detector, f.registry);
  const originalHome = { HOME: process.env.HOME, USERPROFILE: process.env.USERPROFILE };
  try {
    // Supervisor discovery uses the home directory, so do not consult production.
    process.env.HOME = process.env.USERPROFILE = f.dir;
    const thread = failedThread();
    f.store.upsertTask(thread);
    let launch: any[] | undefined;
    (rework as any).executeReworkBackground = async (...args: any[]) => { launch = args; };
    const round = await rework.retryFailedRound(thread.threadId);
    assert.equal(round.round, 2);
    assert.equal(round.prompt, thread.rounds[1].prompt);
    const retried = f.store.getTask(thread.threadId)!;
    assert.equal(retried.receiverSessionId, thread.receiverSessionId);
    assert.equal(retried.reviewerSessionId, thread.reviewerSessionId);
    assert.equal(retried.execution?.version, 1);
    assert.equal(retried.rounds.length, 2);
    assert.equal(retried.status, 'running');
    assert.equal(launch?.[1], 2);
    assert.equal(launch?.[3], true);
  } finally {
    for (const key of ['HOME', 'USERPROFILE'] as const) {
      if (originalHome[key] === undefined) delete process.env[key]; else process.env[key] = originalHome[key];
    }
    rework.dispose(); await f.cleanup();
  }
});

for (const roundStatus of ['running', 'failed'] as const) test(`the unified retry used by MCP restores a ${roundStatus} round in its original session`, async () => {
  const f = await setupFixture({ useFastLock: true });
  let release!: () => void;
  const barrier = new Promise<void>(resolve => { release = resolve; });
  try {
    const thread = failedThread();
    thread.rounds[1].status = roundStatus;
    f.store.upsertTask(thread);
    f.runner.setOnExecute(async () => { await barrier; throw new Error('Controlled end after retry admission'); });
    await f.controller.retryOperation(thread.threadId);
    const retried = f.store.getTask(thread.threadId)!;
    assert.equal(retried.status, 'running');
    assert.equal(retried.currentRound, 2);
    assert.equal(retried.rounds[1].status, 'running');
    assert.equal(retried.rounds.length, 2);
    assert.equal(retried.rounds[1].prompt, thread.rounds[1].prompt);
    assert.equal(retried.receiverSessionId, thread.receiverSessionId);
    assert.equal(retried.execution?.version, 1);
    assert.equal(retried.execution?.generation, 2);
    assert.equal(f.runner.executions.length, 1);
  } finally { release(); await f.controller.waitForActiveExecutions(); await f.cleanup(); }
});

for (const obstruction of ['run', 'cleanup', 'unknown-failure', 'wrong-round', 'v2', 'role', 'session', 'registration', 'cas'] as const) {
  test(`failed-round recovery refuses ${obstruction} without changing evidence or starting work`, async () => {
    const f = await setupFixture({ useFastLock: true });
    const rework = new ReworkController(f.store, f.runner as any, f.callbackManager, f.detector, f.registry);
    let roleLease: ReturnType<typeof f.roleLock.tryAcquire> | undefined;
    let sessionLease: ReturnType<typeof f.sessionLock.acquire>;
    try {
      const thread = failedThread();
      if (obstruction === 'run') thread.execution!.run = { id: 'uncertain-run', owner: { pid: process.pid },
        state: 'stopping', step: 'executing', startedAt: thread.createdAt };
      if (obstruction === 'cleanup') thread.cleanup = { action: 'delete', stage: 'in_progress', token: 'cleanup',
        owner: { pid: process.pid }, requestedAt: thread.createdAt } as any;
      if (obstruction === 'unknown-failure') thread.failure!.reason = 'Uncertain work after a host crash';
      if (obstruction === 'wrong-round') thread.failure!.round = 1;
      if (obstruction === 'v2') thread.execution!.version = 2;
      f.store.upsertTask(thread);
      if (obstruction === 'role') roleLease = f.roleLock.tryAcquire(thread.threadId, 'developer');
      if (obstruction === 'session') sessionLease = f.sessionLock.acquire(thread.targetAgent, thread.receiverSessionId);
      if (obstruction === 'registration') f.registry.begin(thread.threadId, 2, thread.targetAgent);
      if (obstruction === 'cas') f.store.transitionExecution = async () => { throw new Error('Execution revision mismatch'); };
      const before = f.store.getTask(thread.threadId);
      await assert.rejects(rework.retryFailedRound(thread.threadId));
      assert.deepEqual(f.store.getTask(thread.threadId), before);
      assert.equal(f.runner.executions.length, 0);
    } finally {
      if (roleLease) f.roleLock.release(roleLease);
      if (sessionLease) f.sessionLock.release(sessionLease);
      rework.dispose(); await f.cleanup();
    }
  });
}
