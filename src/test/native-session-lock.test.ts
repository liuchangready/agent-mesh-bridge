import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawnSync } from 'node:child_process';
import { afterEach, test } from 'node:test';
import { NativeSessionLock } from '../bridge/native-session-lock';

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
function tempRoot(): string { const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'native-lock-')); roots.push(dir); return dir; }

test('Codex UUID and rollout name share one session write lease', () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const uuid = '01a17d29-d40d-74c3-831d-878dd4edc0e5';
  const lease = lock.acquire('codex', uuid)!;
  assert.ok(lease);
  assert.equal(lock.acquire('codex', `rollout-2026-10-27T19-40-56-${uuid}`), undefined);
  lock.release(lease);
  assert.ok(lock.acquire('codex', uuid));
});

test('session write lease excludes a separate bridge process', () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const lease = lock.acquire('codex', 'thread-test')!;
  const modulePath = path.resolve(__dirname, '../bridge/native-session-lock.js');
  const child = spawnSync(process.execPath, ['-e', `const {NativeSessionLock}=require(${JSON.stringify(modulePath)}); const lock=new NativeSessionLock(${JSON.stringify(root)}); process.stdout.write(lock.acquire('codex','thread-test')?'acquired':'busy');`], { encoding: 'utf8' });
  assert.equal(child.status, 0, child.stderr);
  assert.equal(child.stdout, 'busy');
  lock.release(lease);
});

test('a live owner is never reclaimed from a stale heartbeat', () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const lease = lock.acquire('codex', 'thread-test')!;
  const owner = JSON.parse(fs.readFileSync(lease.path, 'utf8'));
  owner.updatedAt = 0;
  fs.writeFileSync(lease.path, JSON.stringify(owner));
  assert.equal(lock.acquire('codex', 'thread-test'), undefined);
});
