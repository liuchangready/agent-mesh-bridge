import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { createThread, setupFixture } from './unified-execution-fixture';

const reason = 'The task role lease is not held during native-session preparation.';

function failedThread(): ReturnType<typeof createThread> {
  const thread = createThread('thread_failed_retry_v2');
  thread.status = 'failed';
  thread.currentRound = thread.execution!.round = 2;
  thread.rounds[0].status = 'completed';
  thread.rounds.push({ round: 2, type: 'rework', prompt: 'Continue the original revision',
    status: 'failed', durationMs: 1, timestamp: thread.updatedAt });
  thread.failure = { stage: 'execution', code: 'EXECUTION_ERROR', reason, at: thread.updatedAt, round: 2 };
  return thread;
}

test('a preparation exception finalizes the current failed work round', async () => {
  const f = await setupFixture({ useFastLock: true });
  try {
    const thread = createThread('thread_preparation_throw');
    f.addRunnableThread(thread);
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

for (const roundStatus of ['running', 'failed'] as const) test(`the unified retry used by MCP restores a ${roundStatus} round in its original session`, async () => {
  const f = await setupFixture({ useFastLock: true });
  let release!: () => void;
  const barrier = new Promise<void>(resolve => { release = resolve; });
  try {
    const thread = failedThread();
    thread.rounds[1].status = roundStatus;
    f.addRunnableThread(thread);
    f.runner.setOnExecute(async () => { await barrier; throw new Error('Controlled end after retry admission'); });
    await f.controller.retryOperation(thread.threadId);
    const retried = f.store.getTask(thread.threadId)!;
    assert.equal(retried.status, 'running');
    assert.equal(retried.currentRound, 2);
    assert.equal(retried.rounds[1].status, 'running');
    assert.equal(retried.rounds.length, 2);
    assert.equal(retried.rounds[1].prompt, thread.rounds[1].prompt);
    assert.equal(retried.receiverSessionId, thread.receiverSessionId);
    assert.equal(retried.execution?.version, 2);
    assert.equal(retried.execution?.generation, 2);
    assert.equal(f.runner.executions.length, 1);
  } finally { release(); await f.controller.waitForActiveExecutions(); await f.cleanup(); }
});
