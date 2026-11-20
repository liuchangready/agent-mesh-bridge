import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { assertDedicatedTask, requireDeliveryTarget } from '../bridge/types';
import { resolveReviewFlow } from '../bridge/reviewer-bootstrap';

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
