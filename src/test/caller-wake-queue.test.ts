import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CallerWakeQueue, callerSessionKey } from '../bridge/caller-wake-queue';

function lockPathFor(root: string, callerSessionId: string): string {
  const key = crypto
    .createHash('sha1')
    .update(callerSessionKey(callerSessionId))
    .digest('hex')
    .slice(0, 16);
  return path.join(root, 'wakes', key, 'active.json');
}

describe('CallerWakeQueue - one wake at a time per caller session', () => {
  test('treats a codex rollout name and its bare thread id as one wake slot', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root);
    const rollout = 'rollout-2026-10-27T19-40-56-01a17d29-d40d-74c3-831d-878dd4edc0e5';
    const bare = '01a17d29-d40d-74c3-831d-878dd4edc0e5';
    try {
      assert.strictEqual(
        queue.tryAcquire(rollout),
        true,
        'the rollout spelling wins the slot first',
      );
      // The same native thread under its bare UUID must NOT get a second slot:
      // a concurrent `codex exec resume` fails with "no rollout found".
      assert.strictEqual(queue.isBusy(bare), true);
      assert.strictEqual(queue.tryAcquire(bare), false, 'one native thread, one wake at a time');

      queue.release(bare);
      assert.strictEqual(queue.isBusy(rollout), false);
      assert.strictEqual(queue.tryAcquire(bare), true);
      queue.release(rollout);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('serializes wakes for the same caller and keeps different callers independent', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root);
    try {
      assert.strictEqual(queue.tryAcquire('sess-a'), true);
      assert.strictEqual(queue.tryAcquire('sess-a'), false, 'one turn per caller session');
      assert.strictEqual(queue.isBusy('sess-a'), true);
      assert.strictEqual(queue.tryAcquire('sess-b'), true, 'another caller is independent');

      queue.release('sess-a');
      assert.strictEqual(queue.isBusy('sess-a'), false);
      assert.strictEqual(queue.tryAcquire('sess-a'), true);
      queue.release('sess-a');
      queue.release('sess-b');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('queued wakes drain in FIFO order and survive a restart', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root);
    try {
      queue.enqueue('sess-a', { threadId: 't2', round: 1, kind: 'review', enqueuedAt: 200 });
      queue.enqueue('sess-a', { threadId: 't1', round: 3, kind: 'failure', enqueuedAt: 100 });

      // A new queue instance (bridge restarted) still sees the backlog.
      const reopened = new CallerWakeQueue(root);
      assert.strictEqual(reopened.pendingCount('sess-a'), 2);
      assert.strictEqual(reopened.dequeue('sess-a')?.threadId, 't1');
      assert.strictEqual(reopened.dequeue('sess-a')?.threadId, 't2');
      assert.strictEqual(reopened.dequeue('sess-a'), undefined);
      assert.strictEqual(reopened.pendingCount('sess-a'), 0);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('a stale lock whose owner process is gone is reclaimed', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root, { staleAfterMs: 50 });
    try {
      const lockPath = lockPathFor(root, 'sess-a');
      fs.mkdirSync(path.dirname(lockPath), { recursive: true });
      fs.writeFileSync(
        lockPath,
        JSON.stringify({ pid: 999_999, at: Date.now() - 60_000 }),
        'utf8',
      );
      assert.strictEqual(queue.tryAcquire('sess-a'), true, 'dead owner must not block forever');
      queue.release('sess-a');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('a fresh lock owned by a live process still blocks', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root);
    try {
      const lockPath = lockPathFor(root, 'sess-a');
      fs.mkdirSync(path.dirname(lockPath), { recursive: true });
      fs.writeFileSync(lockPath, JSON.stringify({ pid: process.ppid, at: Date.now() }), 'utf8');
      assert.strictEqual(queue.tryAcquire('sess-a'), false);
      assert.strictEqual(queue.isBusy('sess-a'), true);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('discard drops queued wakes for a thread that no longer needs review', () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'wake-queue-'));
    const queue = new CallerWakeQueue(root);
    try {
      queue.enqueue('sess-a', { threadId: 't1', round: 1, kind: 'review', enqueuedAt: 1 });
      queue.enqueue('sess-a', { threadId: 't2', round: 1, kind: 'review', enqueuedAt: 2 });
      assert.strictEqual(queue.discard('sess-a', 't1'), 1);
      assert.strictEqual(queue.pendingCount('sess-a'), 1);
      assert.strictEqual(queue.dequeue('sess-a')?.threadId, 't2');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
