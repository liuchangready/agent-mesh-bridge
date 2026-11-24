import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { TaskStore } from '../bridge/task-store';
import { createThread } from './unified-execution-fixture';

for (const persistent of [false, true]) {
  test(`atomic task publication preserves the old record on ${persistent ? 'persistent' : 'temporary'} replacement denial`, () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'amb-atomic-'));
    const store = new TaskStore(root);
    const thread = createThread('thread_atomic_publish');
    thread.execution!.version = 1;
    store.upsertTask(thread);
    const target = path.join(root, 'threads', `${thread.threadId}.json`);
    const before = fs.readFileSync(target, 'utf8');
    const rawFs = fs;
    const original = rawFs.renameSync;
    let attempts = 0;
    (rawFs as any).renameSync = (source: string, destination: string) => {
      if (destination === target && (persistent || attempts === 0)) {
        attempts++;
        throw Object.assign(new Error('controlled replacement denial'), { code: 'EPERM' });
      }
      return original(source, destination);
    };
    try {
      thread.title = 'published exactly once';
      if (!persistent && process.platform === 'win32') {
        store.upsertTask(thread);
        assert.equal(attempts, 1);
        assert.equal(JSON.parse(fs.readFileSync(target, 'utf8')).title, thread.title);
      } else {
        assert.throws(() => store.upsertTask(thread), /controlled replacement denial/);
        assert.equal(fs.readFileSync(target, 'utf8'), before);
        assert.equal(attempts, process.platform === 'win32' ? 20 : 1);
      }
      assert.equal(fs.readdirSync(path.dirname(target)).some(name => name.includes('.tmp.')), false);
    } finally {
      (rawFs as any).renameSync = original;
      store.dispose();
      assert.ok(fs.realpathSync.native(root).startsWith(fs.realpathSync.native(os.tmpdir()) + path.sep));
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
}
