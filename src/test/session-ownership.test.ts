import * as assert from 'node:assert/strict';
import * as crypto from 'node:crypto';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import * as childProcess from 'node:child_process';
import { describe, it } from 'node:test';
import { SessionOwnershipStore, nativeSessionNamespace } from '../bridge/session-ownership';

describe('static native session ownership', () => {
  const makeDir = () => fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-session-owner-'));
  const input = (threadId: string, sessionId = '123e4567-e89b-12d3-a456-426614174000') => ({
    agent: 'codex' as const, namespace: 'C:/isolated/codex-home', sessionId,
    threadId, role: 'reviewer' as const, sessionEpoch: 1,
  });

  it('claims idempotently for the same task and conflicts across tasks, including rollout aliases', () => {
    const dir = makeDir();
    try {
      const a = new SessionOwnershipStore(dir);
      const first = a.claim(input('thread-a'));
      assert.equal(first.ok, true);
      assert.equal(a.claim(input('thread-a', 'rollout-2026-10-27T19-40-56-123e4567-e89b-12d3-a456-426614174000')).ok, true);
      const other = new SessionOwnershipStore(dir).claim(input('thread-b'));
      assert.equal(other.ok, false);
      if (!other.ok) assert.equal(other.code, 'SESSION_OWNERSHIP_CONFLICT');
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('rejects a damaged published binding as unverified', () => {
    const dir = makeDir();
    try {
      const inputValue = input('thread-a');
      const store = new SessionOwnershipStore(dir);
      const published = store.claim(inputValue);
      assert.equal(published.ok, true);
      if (!published.ok) return;
      fs.writeFileSync(path.join(dir, 'session-bindings', `${published.record.key}.json`), '{broken');
      const result = store.claim(inputValue);
      assert.equal(result.ok, false);
      if (!result.ok) assert.equal(result.code, 'SESSION_OWNERSHIP_UNVERIFIED');
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('recomputes the published key from every stored identity field', () => {
    const dir = makeDir();
    try {
      const store = new SessionOwnershipStore(dir);
      const value = input('thread-a');
      const claimed = store.claim(value);
      assert.equal(claimed.ok, true);
      if (!claimed.ok) return;
      const file = path.join(dir, 'session-bindings', `${claimed.record.key}.json`);
      const record = JSON.parse(fs.readFileSync(file, 'utf8'));
      record.sessionId = 'different-session-id';
      fs.writeFileSync(file, JSON.stringify(record));
      const result = store.claim(value);
      assert.equal(result.ok, false);
      if (!result.ok) assert.equal(result.code, 'SESSION_OWNERSHIP_UNVERIFIED');
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('normalizes a real native directory and its junction to the same namespace', { skip: process.platform !== 'win32' }, () => {
    const dir = makeDir();
    try {
      const real = path.join(dir, 'codex-home');
      const junction = path.join(dir, 'codex-junction');
      fs.mkdirSync(real);
      fs.symlinkSync(real, junction, 'junction');
      const realNamespace = nativeSessionNamespace('codex', { homeDir: dir, env: { CODEX_HOME: real } });
      const linkedNamespace = nativeSessionNamespace('codex', { homeDir: dir, env: { CODEX_HOME: junction } });
      assert.equal(realNamespace, linkedNamespace);
      const store = new SessionOwnershipStore(path.join(dir, 'bridge'));
      assert.equal(store.claim({ ...input('thread-a'), namespace: realNamespace }).ok, true);
      const second = store.claim({ ...input('thread-b'), namespace: linkedNamespace });
      assert.equal(second.ok, false);
      if (!second.ok) assert.equal(second.code, 'SESSION_OWNERSHIP_CONFLICT');
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('does not share ownership across native storage namespaces', () => {
    const dir = makeDir();
    try {
      const store = new SessionOwnershipStore(dir);
      assert.equal(store.claim(input('thread-a')).ok, true);
      assert.equal(store.claim({ ...input('thread-b'), namespace: 'C:/other/codex-home' }).ok, true);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('keeps revocation durable and idempotent for the same owner after cleanup retries', () => {
    const dir = makeDir();
    try {
      const store = new SessionOwnershipStore(dir);
      const value = input('thread-a');
      assert.equal(store.claim(value).ok, true);
      const owner = { threadId: value.threadId, role: value.role, sessionEpoch: value.sessionEpoch };
      assert.equal(store.revoke(value.agent, value.namespace, value.sessionId, owner).ok, true);
      const retried = store.revoke(value.agent, value.namespace, value.sessionId, owner);
      assert.equal(retried.ok, true);
      if (retried.ok) assert.equal(retried.existing, true);
      const wrongOwner = store.revoke(value.agent, value.namespace, value.sessionId, { ...owner, sessionEpoch: 2 });
      assert.equal(wrongOwner.ok, false);
      if (!wrongOwner.ok) assert.equal(wrongOwner.code, 'SESSION_OWNERSHIP_CONFLICT');
      const claimAgain = store.claim({ ...value, threadId: 'thread-b' });
      assert.equal(claimAgain.ok, false);
      if (!claimAgain.ok) assert.equal(claimAgain.code, 'SESSION_OWNERSHIP_CONFLICT');
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });

  it('uses atomic no-replace publication when two real processes claim one session', async () => {
    const dir = makeDir();
    const gate = path.join(dir, 'go');
    try {
      const modulePath = path.resolve(__dirname, '../bridge/session-ownership');
      const script = [
        `const fs=require('fs'); const {SessionOwnershipStore}=require(${JSON.stringify(modulePath)});`,
        `const [root,gate,threadId]=process.argv.slice(1);`,
        `while(!fs.existsSync(gate)) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,5);`,
        `const result=new SessionOwnershipStore(root).claim({agent:'codex',namespace:'C:/isolated/codex-home',sessionId:'123e4567-e89b-12d3-a456-426614174000',threadId,role:'reviewer',sessionEpoch:1});`,
        `process.stdout.write(JSON.stringify(result));`,
      ].join(' ');
      const launch = (threadId: string) => childProcess.spawn(process.execPath, ['-e', script, dir, gate, threadId], {
        windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
      });
      const children = [launch('thread-a'), launch('thread-b')];
      const output = children.map((child) => new Promise<string>((resolve, reject) => {
        let text = '';
        child.stdout!.on('data', (chunk) => { text += chunk.toString(); });
        child.once('error', reject);
        child.once('close', (code) => code === 0 ? resolve(text) : reject(new Error(`claim child exited ${code}: ${text}`)));
      }));
      await new Promise((resolve) => setTimeout(resolve, 100));
      fs.writeFileSync(gate, 'go');
      const results = (await Promise.all(output)).map((text) => JSON.parse(text));
      assert.equal(results.filter((result) => result.ok).length, 1);
      assert.equal(results.filter((result) => !result.ok && result.code === 'SESSION_OWNERSHIP_CONFLICT').length, 1);
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
  });
});
