import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';

test('test runner prioritizes long files, discovers nested tests and propagates failures', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-test-runner-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const tests = path.join(root, 'out', 'test');
  fs.mkdirSync(path.join(tests, 'nested'), { recursive: true });
  const passing = (name: string) => `require('node:test').test('${name}', () => {});`;
  fs.writeFileSync(path.join(tests, 'a-fast.test.js'), passing('alphabetically first'));
  fs.writeFileSync(path.join(tests, 'git-worktree-stash.test.js'), passing('long file first'));
  fs.writeFileSync(path.join(tests, 'nested', 'nested.test.js'), passing('nested test discovered'));
  fs.writeFileSync(path.join(tests, 'ignored.js'), "throw new Error('must not run');");
  const runner = path.resolve(__dirname, '../../scripts/test.cjs');
  const env = { ...process.env };
  // The subprocess starts a separate test run, rather than recursing into this worker.
  delete env.NODE_TEST_CONTEXT;
  const execute = () => spawnSync(process.execPath, [runner], {
    cwd: root, env, encoding: 'utf8', windowsHide: true, timeout: 30_000,
  });
  const passed = execute();
  assert.equal(passed.status, 0, passed.stderr || passed.stdout);
  assert.ok(passed.stdout.includes('nested test discovered'));
  assert.ok(passed.stdout.indexOf('long file first') >= 0);
  assert.ok(passed.stdout.indexOf('long file first') < passed.stdout.indexOf('alphabetically first'));

  fs.writeFileSync(path.join(tests, 'failure.test.js'), "require('node:test').test('intentional failure', () => { throw new Error('expected failure'); });");
  const failed = execute();
  assert.equal(failed.status, 1, failed.stderr || failed.stdout);
  assert.ok(failed.stdout.includes('intentional failure'));

  const empty = path.join(root, 'empty');
  fs.mkdirSync(path.join(empty, 'out', 'test'), { recursive: true });
  const missing = spawnSync(process.execPath, [runner], { cwd: empty, env, encoding: 'utf8', windowsHide: true, timeout: 30_000 });
  assert.equal(missing.status, 1, missing.stderr || missing.stdout);
  assert.ok(missing.stderr.includes('No compiled tests found'));
});
