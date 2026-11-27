import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-role-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskRoleLock } from '../bridge/task-role-lock';

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
    first.release(reviewer);
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

  test('re-acquiring the role this process already holds returns the live lease', () => {
    // A phase handoff re-enters startExecution while this turn still owns the
    // role. Treating that as "busy" parked the task in queued(role) while the
    // agent kept running, so the reported status never matched reality.
    const lock = new TaskRoleLock();
    const first = lock.tryAcquire('thread_role_reentrant', 'reviewer');
    assert.ok(first, 'first acquisition must succeed');

    try {
      const again = lock.tryAcquire('thread_role_reentrant', 'reviewer');
      assert.ok(again, 're-entrant acquisition must not report the role as busy');
      assert.strictEqual(again!.token, first!.token, 'the live lease must be returned unchanged');

      const conflicting = lock.tryAcquire('thread_role_reentrant', 'developer');
      assert.strictEqual(conflicting, null, 'a different role must still be refused');
    } finally {
      lock.release(first!);
    }
  });
