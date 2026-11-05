import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';

const dirs: string[] = [];
function tempDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-execution-registry-'));
  dirs.push(dir);
  return dir;
}

describe('TaskExecutionRegistry - observation without mutual exclusion', () => {
  test('same-workspace registrations coexist and legacy lock files do not block them', () => {
    const storage = tempDir();
    const ws = path.join(storage, 'workspace');
    fs.mkdirSync(path.join(storage, 'leases'), { recursive: true });
    const old = { token: 'old', pid: process.pid, threadId: 'legacy', round: 1, acquiredAt: Date.now(), updatedAt: Date.now(), workspace: ws };
    fs.writeFileSync(path.join(storage, 'execution.lock'), JSON.stringify(old));
    fs.writeFileSync(path.join(storage, 'leases', 'old.lock'), JSON.stringify(old));

    const a = new TaskExecutionRegistry(storage);
    const b = new TaskExecutionRegistry(storage);
    const first = a.begin('thread_a', 1, 'codex', ws);
    const second = b.begin('thread_b', 1, 'claude_code', ws);

    assert.notStrictEqual(first.token, second.token);
    assert.deepStrictEqual(
      a.listOwners().map((owner) => owner.threadId).sort(),
      ['thread_a', 'thread_b'],
      'legacy records must not count against other threads or enabled concurrency limits',
    );
    assert.strictEqual(a.getOwner('legacy')?.threadId, 'legacy', 'legacy lock remains readable for same-thread recovery');
    assert.strictEqual(fs.existsSync(path.join(storage, 'execution.lock')), true, 'legacy files are ignored, not rewritten');
    a.end(first);
    b.end(second);
    a.dispose();
    b.dispose();
  });

  test('end only removes the matching thread registration', () => {
    const storage = tempDir();
    const registry = new TaskExecutionRegistry(storage);
    const first = registry.begin('thread_a', 1, 'codex');
    const second = registry.begin('thread_b', 1, 'agy');
    registry.end(first);
    assert.deepStrictEqual(registry.listOwners().map((owner) => owner.threadId), ['thread_b']);
    registry.end(second);
    registry.dispose();
  });

  test('orphan cancellation targets the registered thread PID only', async () => {
    const storage = tempDir();
    const registry = new TaskExecutionRegistry(storage, { orphanGraceMs: 0 });
    const child = child_process.spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {
      stdio: 'ignore',
      windowsHide: true,
    });
    assert.ok(child.pid);
    const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
    const target = registry.begin('thread_target', 1, 'codex');
    const other = registry.begin('thread_other', 1, 'claude_code');
    try {
      registry.attachChildPid(child.pid!, target);
      registry.killOrphanChild('thread_other', true);
      assert.strictEqual(child.exitCode, null, 'cancelling an unrelated thread must not kill this child');
      registry.killOrphanChild('thread_target', true);
      await exited;
      assert.notStrictEqual(child.exitCode, null);
    } finally {
      if (child.exitCode === null) child.kill('SIGKILL');
      registry.end(target);
      registry.end(other);
      registry.dispose();
    }
  });
});

test('temp execution-registry directories are removed', () => {
  while (dirs.length > 0) {
    fs.rmSync(dirs.pop()!, { recursive: true, force: true });
  }
});
