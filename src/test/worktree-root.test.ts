import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import {
  readBridgePolicy,
  saveBridgePolicy,
  worktreeRoot,
} from '../bridge/bridge-policy';
import { createTaskWorktree, resolveGitRepository } from '../bridge/git-worktree';

const gitExe =
  process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');

function git(cwd: string, ...args: string[]): string {
  const result = spawnSync(gitExe, args, { cwd, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  return result.stdout.trim();
}

function initRepo(root: string): void {
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-b', 'main');
  git(root, 'config', 'user.name', 'Mesh Test');
  git(root, 'config', 'user.email', 'mesh-test@example.test');
  fs.writeFileSync(path.join(root, 'readme.txt'), 'base\n', 'utf8');
  git(root, 'add', '-A');
  git(root, 'commit', '-m', 'base');
}

function isolatedDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function removeTree(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
}

test('an unconfigured bridge keeps the historical worktrees location', (t) => {
  const storage = isolatedDir('mesh-wtroot-default-');
  t.after(() => removeTree(storage));

  assert.strictEqual(worktreeRoot(storage), path.join(storage, 'worktrees'));
});

test('an absolute configured root is used verbatim', (t) => {
  const storage = isolatedDir('mesh-wtroot-abs-');
  t.after(() => removeTree(storage));
  const configured = isolatedDir('mesh-wtroot-target-');
  t.after(() => removeTree(configured));

  saveBridgePolicy({ worktreeRoot: configured }, storage);

  assert.strictEqual(worktreeRoot(storage), path.normalize(configured));
});

test('a relative configured root resolves against the bridge home, not the cwd', (t) => {
  const storage = isolatedDir('mesh-wtroot-rel-');
  t.after(() => removeTree(storage));

  saveBridgePolicy({ worktreeRoot: 'mesh-worktrees' }, storage);

  const resolved = worktreeRoot(storage);
  assert.strictEqual(resolved, path.resolve(storage, 'mesh-worktrees'));
  assert.ok(path.isAbsolute(resolved));
});

test('clearing the setting restores the default root', (t) => {
  const storage = isolatedDir('mesh-wtroot-clear-');
  t.after(() => removeTree(storage));

  saveBridgePolicy({ worktreeRoot: path.join(storage, 'somewhere') }, storage);
  assert.notStrictEqual(worktreeRoot(storage), path.join(storage, 'worktrees'));

  saveBridgePolicy({ worktreeRoot: '   ' }, storage);

  assert.strictEqual(readBridgePolicy(storage).worktreeRoot, undefined);
  assert.strictEqual(worktreeRoot(storage), path.join(storage, 'worktrees'));
});

test('saving the worktree root does not disturb the other mirrored policy switches', (t) => {
  const storage = isolatedDir('mesh-wtroot-preserve-');
  t.after(() => removeTree(storage));

  saveBridgePolicy(
    { contextCompaction: false, reviewFlow: 'legacy', concurrency: { enabled: true, maxConcurrentTasks: 4 } },
    storage,
  );
  saveBridgePolicy({ worktreeRoot: path.join(storage, 'wt') }, storage);

  const policy = readBridgePolicy(storage);
  assert.strictEqual(policy.worktreeRoot, path.join(storage, 'wt'));
  assert.strictEqual(policy.contextCompaction, false);
  assert.strictEqual(policy.reviewFlow, 'legacy');
  assert.strictEqual(policy.concurrency?.enabled, true);
  assert.strictEqual(policy.concurrency?.maxConcurrentTasks, 4);
});

test('a new task worktree is actually created under the configured root', (t) => {
  const root = isolatedDir('mesh-wtroot-create-');
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  const configuredRoot = path.join(root, 'elsewhere', 'worktrees');
  t.after(() => removeTree(root));

  initRepo(repository);
  saveBridgePolicy({ worktreeRoot: configuredRoot }, storage);

  const repo = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(repo, 'thread_wtroot', storage, worktreeRoot(storage));

  assert.strictEqual(path.resolve(worktree.path), path.resolve(configuredRoot, 'thread_wtroot'));
  assert.ok(
    !path.resolve(worktree.path).startsWith(path.resolve(storage, 'worktrees')),
    'the worktree must not land in the default root when a root is configured',
  );
  assert.strictEqual(fs.readFileSync(path.join(worktree.path, 'readme.txt'), 'utf8').replace(/\r\n/g, '\n'), 'base\n');
  assert.strictEqual(git(worktree.path, 'rev-parse', '--abbrev-ref', 'HEAD'), 'mesh/task/thread_wtroot');
});

test('omitting the root keeps creating worktrees under storage', (t) => {
  const root = isolatedDir('mesh-wtroot-omit-');
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  t.after(() => removeTree(root));

  initRepo(repository);

  const repo = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(repo, 'thread_wtroot_omit', storage);

  assert.strictEqual(path.resolve(worktree.path), path.resolve(storage, 'worktrees', 'thread_wtroot_omit'));
});
