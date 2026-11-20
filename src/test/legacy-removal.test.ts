import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { assertDedicatedTask, requireDeliveryTarget } from '../bridge/types';
import { resolveReviewFlow } from '../bridge/reviewer-bootstrap';
import { createThread, setupFixture } from './unified-execution-fixture';

test('new dispatch never falls back to requester review with absent or stale policy', () => {
  assert.equal(resolveReviewFlow(), 'dedicated');
  assert.equal(resolveReviewFlow(undefined, 'legacy'), 'dedicated');
  assert.equal(resolveReviewFlow('dedicated', 'legacy'), 'dedicated');
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

test('both controller entry points refuse historical review before claiming a run', async () => {
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
        assert.deepEqual(await fixture.controller.startExecutionV2(thread.threadId), expected);
        assert.deepEqual(fixture.store.getTask(thread.threadId)!.execution, execution);
      }
    }
    assert.equal(fixture.runner.executions.length, 0);
  } finally {
    await fixture.cleanup();
  }
});
