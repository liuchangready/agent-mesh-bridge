import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import {
  readBridgePolicy,
  saveBridgePolicy,
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

test('new worktrees use only the task data root', t => {
  const storage=isolatedDir('mesh-task-root-');t.after(()=>removeTree(storage));
  const repoPath=path.join(storage,'repo');initRepo(repoPath);
  const repo=resolveGitRepository(repoPath,storage);
  saveBridgePolicy({taskDataRoot:path.join(storage,'task-data')},storage);
  const worktree=createTaskWorktree(repo,'thread_new_root',storage);
  assert.equal(worktree.path,path.join(storage,'task-data','thread_new_root','worktree'));
  assert.equal(worktree.deliveryTarget?.ref,'refs/heads/main');
});

test('stale worktreeRoot is discarded without changing recorded worktrees', t => {
  const storage=isolatedDir('mesh-stale-root-');t.after(()=>removeTree(storage));
  fs.writeFileSync(path.join(storage,'bridge-policy.json'),JSON.stringify({worktreeRoot:'old',taskDataRoot:'tasks'}));
  const policy=saveBridgePolicy({},storage);
  assert.equal((policy as any).worktreeRoot,undefined);
  assert.equal(policy.taskDataRoot,'tasks');
});

test('standalone Git resolution uses the mirrored Git path and explains a missing executable', t => {
  const storage = isolatedDir('mesh-git-config-');
  t.after(() => removeTree(storage));
  const repository = path.join(storage, 'repo');
  initRepo(repository);
  const binary = path.isAbsolute(gitExe) ? gitExe : spawnSync(
    process.platform === 'win32' ? 'where.exe' : 'which', [gitExe], { encoding: 'utf8', windowsHide: true },
  ).stdout.trim().split(/\r?\n/)[0];
  assert.ok(path.isAbsolute(binary));

  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
    !/^(PATH|MESH_GIT_PATH|GIT_EXECUTABLE|GIT_CONFIG_COUNT)$/i.test(key)));
  const modulePath = path.resolve(__dirname, '../bridge/git-worktree.js');
  const run = (extraEnv: NodeJS.ProcessEnv = {}) => spawnSync(process.execPath, ['-e', `
    const { resolveGitRepository, resolveTemplateWorkspaceRoot } = require(process.argv[1]);
    const [repository, storage] = process.argv.slice(2);
    const repo = resolveGitRepository(repository, storage);
    const workspace = resolveTemplateWorkspaceRoot(repository, storage);
    console.log(JSON.stringify({ ref: repo.targetRef, workspace }));
  `, modulePath, repository, storage], { env: { ...env, PATH: '', ...extraEnv }, encoding: 'utf8', windowsHide: true });

  const missing = run();
  assert.notEqual(missing.status, 0);
  assert.match(missing.stderr, /GIT_EXECUTABLE_UNAVAILABLE.*git\.path.*MESH_GIT_PATH/);
  assert.doesNotMatch(missing.stderr, /not a usable Git checkout/);

  saveBridgePolicy({ gitPath: [path.join(storage, 'missing-git'), binary] }, storage);
  const configured = run();
  assert.equal(configured.status, 0, configured.stderr);
  assert.equal(JSON.parse(configured.stdout).ref, 'refs/heads/main');
  assert.equal(JSON.parse(configured.stdout).workspace, fs.realpathSync.native(repository));

  const refused = run({ GIT_TEST_ASSUME_DIFFERENT_OWNER: '1', GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: path.join(storage, 'empty-gitconfig') });
  assert.notEqual(refused.status, 0);
  assert.match(refused.stderr, /GIT_REPOSITORY_OWNERSHIP/);
  assert.doesNotMatch(refused.stderr, /GIT_EXECUTABLE_UNAVAILABLE|not a usable Git checkout/);

  saveBridgePolicy({ gitPath: [] }, storage);
  assert.equal(readBridgePolicy(storage).gitPath, undefined);
});
