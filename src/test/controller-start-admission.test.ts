import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { createThread, setupFixture } from './unified-execution-fixture';
import { testWorkspace } from './workspace-fixture';

test('concurrent local starts share the winning V2 run', async () => {
  const fixture = await setupFixture();
  let release!: () => void;
  const barrier = new Promise<void>(resolve => { release = resolve; });
  fixture.runner.setOnExecute(async () => { await barrier; return { exitCode: 1, stderr: 'controlled end' }; });
  try {
    const thread = createThread('thread_start_admission');
    fixture.addRunnableThread(thread);
    const [first, second] = await Promise.all([
      fixture.controller.startExecution(thread.threadId), fixture.controller.startExecution(thread.threadId),
    ]);
    assert.equal(first.started, true);
    assert.deepEqual(second, first);
    assert.equal(fixture.runner.executions.length, 1);
    assert.ok(first.run?.id);
    assert.equal(fixture.store.getTask(thread.threadId)?.execution?.run?.id, first.run.id);
    assert.equal(fixture.roleLock.getOwner(thread.threadId), undefined);
  } finally {
    release();
    await fixture.controller.waitForActiveExecutions();
    await fixture.cleanup();
  }
});

test('a landed merge clears only its matching run after the coordinator has recorded completion', async () => {
  const fixture = await setupFixture();
  try {
    const thread = createThread('thread_merge_finalization', 'merge');
    thread.workspace = testWorkspace(fixture.dir);
    thread.status = 'completed';
    thread.execution!.run = { id: 'own-run', owner: { pid: process.pid }, state: 'active', step: 'merging', startedAt: thread.createdAt };
    thread.worktree = { path: fixture.dir, mainPath: fixture.dir,
      integration: { phase: 'merged', updatedAt: thread.createdAt } };
    fixture.store.upsertTask(thread);
    await (fixture.controller as any).handlePhaseOutcome(thread.threadId, 'merge', 'old-run', { ok: true });
    assert.equal(fixture.store.getTask(thread.threadId)?.execution?.run?.id, 'own-run');
    await (fixture.controller as any).handlePhaseOutcome(thread.threadId, 'merge', 'own-run', { ok: true });
    assert.equal(fixture.store.getTask(thread.threadId)?.status, 'completed');
    assert.equal(fixture.store.getTask(thread.threadId)?.execution?.run, undefined);
  } finally { await fixture.cleanup(); }
});
