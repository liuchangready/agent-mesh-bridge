import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { assertDedicatedTask, requireDeliveryTarget } from '../bridge/types';
import { resolveReviewFlow } from '../bridge/reviewer-bootstrap';
import { createThread, setupFixture } from './unified-execution-fixture';

test('new dispatch always uses dedicated review and rejects the retired flow', () => {
  assert.equal(resolveReviewFlow(), 'dedicated');
  assert.equal(resolveReviewFlow('dedicated'), 'dedicated');
  assert.throws(() => resolveReviewFlow('legacy'), /LEGACY_REVIEW_FLOW_DISABLED/);
});

test('historical review records cannot obtain execution admission', () => {
  for (const reviewFlow of [undefined, 'legacy'] as const) {
    assert.throws(() => assertDedicatedTask({ threadId: 'old-task', reviewFlow }), /LEGACY_REVIEW_FLOW_DISABLED/);
  }
  assert.doesNotThrow(() => assertDedicatedTask({ threadId: 'current-task', reviewFlow: 'dedicated' }));
});

test('historical target refs never become implicit delivery targets', () => {
  assert.throws(() => requireDeliveryTarget({ path: 'task', mainPath: 'repo', targetRef: 'refs/heads/mesh/integration/old' }), /MISSING_DELIVERY_TARGET/);
  const deliveryTarget = { branch: 'master', ref: 'refs/heads/master', repositoryPath: 'repo' };
  assert.equal(requireDeliveryTarget({ path: 'task', mainPath: 'repo', deliveryTarget }), deliveryTarget);
});

test('the controller refuses historical review before claiming a run', async () => {
  const fixture = await setupFixture();
  try {
    for (const version of [1, 2] as const) {
      for (const reviewFlow of [undefined, 'legacy'] as const) {
        const thread = createThread(`historical_${version}_${reviewFlow}`);
        thread.reviewFlow = reviewFlow;
        thread.execution!.version = version;
        fixture.store.upsertTask(thread);
        const execution = structuredClone(fixture.store.getTask(thread.threadId)!.execution);
        const expected = { started: false, reason: 'LEGACY_REVIEW_FLOW_DISABLED' };
        assert.deepEqual(await fixture.controller.startExecution(thread.threadId), expected);
        assert.deepEqual(fixture.store.getTask(thread.threadId)!.execution, execution);
      }
    }
    assert.equal(fixture.runner.executions.length, 0);
  } finally {
    await fixture.cleanup();
  }
});

test('historical and unknown execution records cannot start, resume, retry, or open a decision', async () => {
  const fixture = await setupFixture();
  try {
    for (const version of [undefined, 1, 99]) {
      const thread = createThread('historical_run_' + version);
      if (version === undefined) thread.execution = undefined;
      else (thread.execution! as { version: number }).version = version;
      fixture.store.upsertTask(thread);
      const before = fixture.store.reloadTask(thread.threadId);
      const reason = version === undefined ? 'NO_EXECUTION' : `VERSION_${version}`;
      assert.deepEqual(await fixture.controller.startExecution(thread.threadId), {
        started: false, reason: `HISTORICAL_TASK_READ_ONLY:${reason}`,
      });
      await assert.rejects(fixture.controller.resumeOperation(thread.threadId), /HISTORICAL_TASK_READ_ONLY/);
      await assert.rejects(fixture.controller.retryOperation(thread.threadId), /HISTORICAL_TASK_READ_ONLY/);
      await assert.rejects(fixture.controller.requestTaskDecision(thread.threadId, {
        question: 'Should this run resume?', resumePoint: 'continue_develop',
      }), /HISTORICAL_TASK_READ_ONLY/);
      await assert.rejects(fixture.controller.submitTaskDecision(thread.threadId, 'past-decision', 'Yes'), /HISTORICAL_TASK_READ_ONLY/);
      await fixture.controller.scanAndDrain();
      assert.deepEqual(fixture.store.reloadTask(thread.threadId), before);
      assert.equal(fixture.roleLock.getOwner(thread.threadId), undefined);
    }
    assert.equal(fixture.runner.executions.length, 0);
    assert.deepEqual(fixture.registry.listOwners(), []);
  } finally {
    await fixture.cleanup();
  }
});
