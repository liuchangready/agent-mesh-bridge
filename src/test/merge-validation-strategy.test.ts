import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import {
  MergeCoordinator,
  MeshFileLock,
  VALIDATION_MANIFEST,
  readValidationStrategy,
} from '../bridge/merge-coordinator';
import {
  commitTaskChanges,
  createTaskWorktree,
  resolveGitRepository,
} from '../bridge/git-worktree';
import { TaskStore } from '../bridge/task-store';
import type { DelegationThread, WorktreeInfo } from '../bridge/types';

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

function thread(threadId: string, worktree: WorktreeInfo): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId,
    title: threadId,
    initiatorAgent: 'codex',
    targetAgent: 'codex',
    status: 'waiting_merge',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: `session-${threadId}`,
    workspacePath: worktree.path,
    summary: { lastPromptSnippet: 'review', lastOutputSnippet: 'approved', totalDurationMs: 1 },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'change one file',
        output: 'done',
        durationMs: 1,
        status: 'completed',
        timestamp: now,
      },
    ],
    worktree: {
      ...worktree,
      approvedCommit: worktree.headCommit,
      integration: {
        phase: 'waiting',
        sourceCommit: worktree.headCommit,
        updatedAt: now,
      },
    },
  };
}

function isolatedDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function removeTree(dir: string): void {
  fs.rmSync(dir, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 });
}

test('package.json scripts remain the primary validation strategy', (t) => {
  const dir = isolatedDir('mesh-strategy-pkg-');
  t.after(() => removeTree(dir));
  fs.writeFileSync(
    path.join(dir, 'package.json'),
    JSON.stringify({ scripts: { compile: 'tsc -p .', test: 'node --test' } }),
    'utf8',
  );
  fs.mkdirSync(path.join(dir, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(
    path.join(dir, VALIDATION_MANIFEST),
    JSON.stringify({ commands: ['echo should-not-run'] }),
    'utf8',
  );

  const strategy = readValidationStrategy(dir);
  assert.strictEqual(strategy.ok, true);
  assert.strictEqual(strategy.ok && strategy.kind, 'package-scripts');
  assert.deepStrictEqual(
    strategy.ok ? strategy.steps.map((step) => step.label) : [],
    ['npm run compile', 'npm test'],
  );
});

test('a package.json without compile and test is refused even when a manifest exists', (t) => {
  const dir = isolatedDir('mesh-strategy-partial-');
  t.after(() => removeTree(dir));
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ scripts: { test: 'x' } }), 'utf8');
  fs.mkdirSync(path.join(dir, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(path.join(dir, VALIDATION_MANIFEST), JSON.stringify({ commands: ['true'] }), 'utf8');

  const strategy = readValidationStrategy(dir);
  assert.strictEqual(strategy.ok, false);
  assert.match(strategy.ok ? '' : strategy.error, /compile/);
});

test('a repository with neither package.json nor manifest is refused', (t) => {
  const dir = isolatedDir('mesh-strategy-none-');
  t.after(() => removeTree(dir));
  fs.writeFileSync(path.join(dir, 'build.gradle'), '// no node here\n', 'utf8');

  const strategy = readValidationStrategy(dir);
  assert.strictEqual(strategy.ok, false);
  assert.match(strategy.ok ? '' : strategy.error, /No supported validation strategy/);
  assert.match(strategy.ok ? '' : strategy.error, /package\.json/);
});

test('a malformed or empty manifest is refused instead of silently passing', (t) => {
  const dir = isolatedDir('mesh-strategy-bad-');
  t.after(() => removeTree(dir));
  fs.mkdirSync(path.join(dir, '.agent-mesh-bridge'), { recursive: true });

  fs.writeFileSync(path.join(dir, VALIDATION_MANIFEST), '{ not json', 'utf8');
  const broken = readValidationStrategy(dir);
  assert.strictEqual(broken.ok, false);
  assert.match(broken.ok ? '' : broken.error, /not valid JSON/);

  fs.writeFileSync(path.join(dir, VALIDATION_MANIFEST), JSON.stringify({ commands: [] }), 'utf8');
  const empty = readValidationStrategy(dir);
  assert.strictEqual(empty.ok, false);
  assert.match(empty.ok ? '' : empty.error, /non-empty "commands"/);
});

test('an entry without a command is refused', (t) => {
  const dir = isolatedDir('mesh-strategy-entry-');
  t.after(() => removeTree(dir));
  fs.mkdirSync(path.join(dir, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(
    path.join(dir, VALIDATION_MANIFEST),
    JSON.stringify({ commands: [{ args: ['x'] }] }),
    'utf8',
  );

  const strategy = readValidationStrategy(dir);
  assert.strictEqual(strategy.ok, false);
  assert.match(strategy.ok ? '' : strategy.error, /needs a non-empty "command"/);
});

test('manifest strings and object entries both resolve to executable steps', (t) => {
  const dir = isolatedDir('mesh-strategy-shape-');
  t.after(() => removeTree(dir));
  fs.mkdirSync(path.join(dir, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(
    path.join(dir, VALIDATION_MANIFEST),
    JSON.stringify({
      commands: ['npm --version', { command: process.execPath, args: ['-e', '0'], label: 'node probe' }],
    }),
    'utf8',
  );

  const strategy = readValidationStrategy(dir);
  assert.strictEqual(strategy.ok, true);
  assert.strictEqual(strategy.ok && strategy.kind, 'manifest');
  if (!strategy.ok) return;
  assert.deepStrictEqual(strategy.steps[0], {
    command: 'npm --version',
    args: [],
    label: 'command 1',
    needsShell: true,
  });
  assert.deepStrictEqual(strategy.steps[1], {
    command: process.execPath,
    args: ['-e', '0'],
    label: 'node probe',
    needsShell: false,
  });
});

test('a Unity-style repository without package.json integrates through its manifest', async (t) => {
  const root = isolatedDir('mesh-manifest-merge-');
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  initRepo(repository);
  const repo = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(repo, 'thread_manifest_merge', storage);

  fs.mkdirSync(path.join(worktree.path, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(
    path.join(worktree.path, '.agent-mesh-bridge', 'validate.json'),
    JSON.stringify({
      commands: [{ command: process.execPath, args: ['validate.js'], label: 'gradle-style compile' }],
    }),
    'utf8',
  );
  fs.writeFileSync(
    path.join(worktree.path, 'validate.js'),
    "require('fs').writeFileSync('validated.txt','ok');\n",
    'utf8',
  );
  fs.writeFileSync(path.join(worktree.path, 'readme.txt'), 'changed\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved change');

  const store = new TaskStore(storage);
  t.after(() => {
    store.dispose();
    removeTree(root);
  });
  store.upsertTask(thread('thread_manifest_merge', worktree));
  const coordinator = new MergeCoordinator(
    store,
    storage,
    new MeshFileLock(path.join(root, 'merge.lock')),
  );

  const result = await coordinator.mergeApproved('thread_manifest_merge');
  assert.strictEqual(result.status, 'merged', result.message);
  assert.strictEqual(
    git(repository, 'show', `${worktree.targetRef!}:readme.txt`).replace(/\r\n/g, '\n'),
    'changed',
  );
});

test('a manifest whose command fails reports the failure and leaves the target unchanged', async (t) => {
  const root = isolatedDir('mesh-manifest-fail-');
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  initRepo(repository);
  const repo = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(repo, 'thread_manifest_fail', storage);
  const targetBefore = git(repository, 'rev-parse', worktree.targetRef!);

  fs.mkdirSync(path.join(worktree.path, '.agent-mesh-bridge'), { recursive: true });
  fs.writeFileSync(
    path.join(worktree.path, '.agent-mesh-bridge', 'validate.json'),
    JSON.stringify({
      commands: [{ command: process.execPath, args: ['-e', 'process.exit(9)'], label: 'failing check' }],
    }),
    'utf8',
  );
  fs.writeFileSync(path.join(worktree.path, 'readme.txt'), 'changed\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved change');

  const store = new TaskStore(storage);
  t.after(() => {
    store.dispose();
    removeTree(root);
  });
  store.upsertTask(thread('thread_manifest_fail', worktree));
  const coordinator = new MergeCoordinator(
    store,
    storage,
    new MeshFileLock(path.join(root, 'merge.lock')),
  );

  const result = await coordinator.mergeApproved('thread_manifest_fail');
  assert.strictEqual(result.status, 'validation_failed', result.message);
  assert.match(result.message, /failing check failed \(exit 9\)/);
  assert.strictEqual(git(repository, 'rev-parse', worktree.targetRef!), targetBefore);
  assert.strictEqual(
    store.getTask('thread_manifest_fail')!.worktree!.integration!.phase,
    'validation_failed',
  );
});

