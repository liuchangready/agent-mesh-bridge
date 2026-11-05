import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
import { afterEach, test } from 'node:test';
import { NativeSessionLock } from '../bridge/native-session-lock';

const roots: string[] = [];
afterEach(() => { for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
function tempRoot(): string { const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'native-lock-')); roots.push(dir); return dir; }

async function competeAtBarrier(root: string, sessionId: string, count = 2): Promise<string[]> {
  const modulePath = path.resolve(__dirname, '../bridge/native-session-lock.js');
  const script = `const {NativeSessionLock}=require(${JSON.stringify(modulePath)}); process.stdin.once('data',()=>{process.stdin.pause(); const l=new NativeSessionLock(${JSON.stringify(root)}); const x=l.acquire('codex',${JSON.stringify(sessionId)}); process.stdout.write(x?'acquired':'busy'); if(x) setTimeout(()=>{},2000);}); process.stdin.resume();`;
  const children = Array.from({ length: count }, () => spawn(process.execPath, ['-e', script], { stdio: ['pipe', 'pipe', 'pipe'] }));
  const results = children.map((child) => new Promise<string>((resolve, reject) => {
    let output = ''; let error = '';
    child.stdout.on('data', (chunk) => { output += chunk.toString(); });
    child.stderr.on('data', (chunk) => { error += chunk.toString(); });
    child.once('error', reject);
    child.once('close', (code) => code === 0 ? resolve(output.trim()) : reject(new Error(error || `child exit ${code}`)));
  }));
  for (const child of children) child.stdin.end('go');
  return Promise.all(results);
}

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

test('concurrent stale-owner recovery grants only one lease across processes', async () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const oldLease = lock.acquire('codex', 'stale-race')!;
  lock.release(oldLease);
  fs.writeFileSync(oldLease.path, JSON.stringify({ ...oldLease.owner, pid: 2_147_000_000, childPid: undefined }));
  fs.writeFileSync(`${oldLease.path}.reaper`, JSON.stringify({ token: 'dead-reaper', pid: 2_147_000_000 }));
  const results = await competeAtBarrier(root, 'stale-race', 8);
  assert.equal(results.filter((value) => value === 'acquired').length, 1, results.join(','));
});

test('simultaneous first acquisition publishes exactly one owner', async () => {
  const results = await competeAtBarrier(tempRoot(), 'first-publish');
  assert.deepEqual(results.sort(), ['acquired', 'busy']);
});

test('an unreadable owner publication is never mistaken for a dead session owner', async () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const old = lock.acquire('codex', 'publish-window')!;
  lock.release(old);
  const script = `const fs=require('fs'); const fd=fs.openSync(${JSON.stringify(old.path)},'wx',0o600); process.stdout.write('opened\\n'); process.stdin.resume(); process.stdin.on('data',()=>{fs.writeSync(fd,JSON.stringify({token:'publisher',pid:process.pid,agent:'codex',sessionId:'publish-window',updatedAt:Date.now()})); process.stdout.write('published\\n');}); setTimeout(()=>{},5000);`;
  const child = spawn(process.execPath, ['-e', script], { stdio: ['pipe', 'pipe', 'pipe'] });
  let output = '';
  child.stdout.on('data', (chunk) => { output += chunk.toString(); });
  try {
    const waitFor = async (text: string) => {
      const until = Date.now() + 3_000;
      while (!output.includes(text) && Date.now() < until) await new Promise((resolve) => setTimeout(resolve, 10));
      assert.ok(output.includes(text), `child did not publish ${text}: ${output}`);
    };
    await waitFor('opened');
    assert.equal(lock.acquire('codex', 'publish-window'), undefined);
    child.stdin.write('publish');
    await waitFor('published');
    assert.equal(lock.acquire('codex', 'publish-window'), undefined);
  } finally {
    child.kill();
    await new Promise<void>((resolve) => child.once('close', () => resolve()));
  }
});

test('a live pending owner is restored after a crash between owner replacement steps', () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const lease = lock.acquire('codex', 'pending-owner-recovery')!;
  lock.release(lease);
  const pending = `${lease.path}.pending.${lease.token}`;
  fs.writeFileSync(pending, JSON.stringify({ ...lease.owner, childPid: process.pid, updatedAt: Date.now() }), { flag: 'wx' });
  // Simulate a crash after removing the prior owner path but before linking the
  // complete replacement into place. The pending record is the recovery proof.
  assert.equal(fs.existsSync(lease.path), false);
  assert.equal(lock.acquire('codex', 'pending-owner-recovery'), undefined);
  const restored = JSON.parse(fs.readFileSync(lease.path, 'utf8'));
  assert.equal(restored.token, lease.token);
  assert.equal(restored.childPid, process.pid);
  assert.equal(fs.existsSync(pending), true, 'the live recovery proof remains until its owner updates it');
});

test('attachChild replaces the complete owner record atomically while another process reads it', async () => {
  const root = tempRoot();
  const lock = new NativeSessionLock(root);
  const lease = lock.acquire('codex', 'attach-window')!;
  const modulePath = path.resolve(__dirname, '../bridge/native-session-lock.js');
  const script = `const {NativeSessionLock}=require(${JSON.stringify(modulePath)}); const l=new NativeSessionLock(${JSON.stringify(root)}); const lease=JSON.parse(process.argv[1]); for(let i=0;i<20;i++) l.attachChild(lease, process.pid); process.stdout.write('done');`;
  const child = spawn(process.execPath, ['-e', script, JSON.stringify(lease)], { stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  let errors = '';
  child.stdout.on('data', (chunk) => { output += chunk.toString(); });
  child.stderr.on('data', (chunk) => { errors += chunk.toString(); });
  const childClosed = new Promise<void>((resolve) => child.once('close', () => resolve()));
  const deadline = Date.now() + 5_000;
  while (!output.includes('done') && Date.now() < deadline) {
    try { const owner = JSON.parse(fs.readFileSync(lease.path, 'utf8')); assert.equal(owner.token, lease.token); }
    catch (error) { child.kill(); throw error; }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  assert.ok(output.includes('done'), `child did not finish owner updates: ${output}${errors}`);
  const owner = JSON.parse(fs.readFileSync(lease.path, 'utf8'));
  assert.equal(owner.token, lease.token);
  assert.equal(owner.childPid, child.pid);
  let closeTimedOut = false;
  let timer: NodeJS.Timeout | undefined;
  await Promise.race([childClosed, new Promise<void>((resolve) => { timer = setTimeout(() => { closeTimedOut = true; resolve(); }, 5_000); })]);
  if (timer) clearTimeout(timer);
  if (closeTimedOut) child.kill();
  assert.notEqual(child.exitCode, null, 'the child should exit naturally after the atomic updates');
  lock.release(lease);
});
