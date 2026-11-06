import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { TaskStore } from '../bridge/task-store';
import { TaskControlService } from '../bridge/task-control';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { MeshFileLock } from '../bridge/merge-coordinator';
import { DelegationThread, pendingMerges } from '../bridge/types';
import { createTaskWorktree, resolveGitRepository } from '../bridge/git-worktree';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-wt-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;

function git(cwd: string, ...args: string[]): string {
  const result = spawnSync(gitExe, args, { cwd, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  return result.stdout.trim();
}

function initRepo(root: string): void {
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-b', 'feature/caller');
  git(root, 'config', 'user.name', 'Mesh Test');
  git(root, 'config', 'user.email', 'mesh-test@example.test');
  fs.writeFileSync(path.join(root, 'tracked.txt'), 'committed input\n');
  git(root, 'add', '-A');
  git(root, 'commit', '-m', 'base');
}

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: 'thread_merge_gate', title: 'Merge gate', initiatorAgent: 'codex', targetAgent: 'claude_code',
    status: 'completed', currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session-1',
    workspacePath: os.tmpdir(), summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 1 }, rounds: [],
    ...overrides,
  };
}

test('dispatch creates a separate task branch from committed target state and ignores caller dirt', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-worktree-dispatch-'));
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  initRepo(repository);
  const base = git(repository, 'rev-parse', 'HEAD');
  fs.writeFileSync(path.join(repository, 'tracked.txt'), 'uncommitted caller edit\n');
  const store = new TaskStore(storage);
  const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({ exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 });
  const callback = new CallbackManager(detector, storage);
  callback.notifyCaller = async () => true;
  callback.notifyCallerOfFailure = async () => false;
  const registry = new TaskExecutionRegistry(storage);
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  t.after(() => { (dispatcher as unknown as { queueDrainer?: { dispose(): void } }).queueDrainer?.dispose(); registry.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });

  const result = await dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Isolated task', prompt: 'inspect the committed file', workspacePath: repository });
  const task = store.getTask(result.threadId)!;
  assert.notStrictEqual(path.resolve(task.workspacePath), path.resolve(repository));
  assert.strictEqual(task.worktree?.baseCommit, base);
  assert.strictEqual(task.worktree?.headCommit, base);
  assert.equal(task.worktree?.targetRef, 'refs/heads/feature/caller');
  assert.equal(task.worktree?.deliveryTarget?.branch, 'feature/caller');
  assert.strictEqual(fs.readFileSync(path.join(task.workspacePath, 'tracked.txt'), 'utf8').replace(/\r\n/g, '\n'), 'committed input\n');
  assert.strictEqual(git(repository, 'rev-parse', 'feature/caller'), base);
  assert.strictEqual(fs.readFileSync(path.join(repository, 'tracked.txt'), 'utf8').replace(/\r\n/g, '\n'), 'uncommitted caller edit\n');
});

test('explicit delivery branch is pinned and task worktrees pass it to child tasks', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-target-inherit-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  git(repository, 'branch', 'release');
  const releaseHead = git(repository, 'rev-parse', 'release');
  const first = createTaskWorktree(resolveGitRepository(repository, storage, 'release'), 'thread_target_parent', storage);
  const childRepo = resolveGitRepository(first.path, storage, 'feature/caller');
  const child = createTaskWorktree(childRepo, 'thread_target_child', storage);
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.equal(first.deliveryTarget?.branch, 'release');
  assert.equal(first.baseCommit, releaseHead);
  assert.equal(child.deliveryTarget?.branch, 'release', 'marker inheritance takes precedence over a later caller target');
  assert.equal(child.targetRef, 'refs/heads/release');
  assert.equal(child.baseCommit, releaseHead);
});

test('detached source requires an explicit existing local delivery branch', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-detached-target-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  git(repository, 'checkout', '--detach', 'HEAD');
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  assert.throws(() => resolveGitRepository(repository, storage), /detached HEAD|targetBranch/i);
  assert.equal(resolveGitRepository(repository, storage, 'feature/caller').deliveryTarget?.branch, 'feature/caller');
  assert.throws(() => resolveGitRepository(repository, storage, 'missing'), /does not exist/i);
});

test('new dispatch creates its worktree while final integration validation holds the mesh lock', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-dispatch-during-merge-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const store = new TaskStore(storage); const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({ exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 });
  const callback = new CallbackManager(detector, storage); callback.notifyCaller = async () => true; callback.notifyCallerOfFailure = async () => false;
  const registry = new TaskExecutionRegistry(storage); const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  const lock = new MeshFileLock(); const release = await lock.acquire(); assert.ok(release);
  t.after(() => { release(); (dispatcher as unknown as { queueDrainer?: { dispose(): void } }).queueDrainer?.dispose(); registry.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  const result = await Promise.race([
    dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'parallel dispatch', prompt: 'start independent task', workspacePath: repository }),
    new Promise<never>((_, reject) => { const timer = setTimeout(() => reject(new Error('dispatch blocked on integration lock')), 3_000); timer.unref(); }),
  ]);
  const created = store.getTask(result.threadId)!;
  assert.ok(created.worktree?.path);
  assert.notEqual(path.resolve(created.worktree!.path), path.resolve(repository));
  assert.equal(fs.existsSync(created.worktree!.path), true);
  release();
});

test('non-Git source paths fail instead of dispatching in a shared directory', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-no-git-'));
  fs.writeFileSync(path.join(root, '.git'), 'invalid git directory marker');
  const store = new TaskStore(path.join(root, 'ledger'));
  const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector);
  const callback = new CallbackManager(detector, path.join(root, 'ledger'));
  const registry = new TaskExecutionRegistry(path.join(root, 'ledger'));
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  t.after(() => { (dispatcher as unknown as { queueDrainer?: { dispose(): void } }).queueDrainer?.dispose(); registry.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  await assert.rejects(dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Must fail', prompt: 'do work', workspacePath: root }), /Git|repository|HEAD/i);
  assert.strictEqual(store.getAllTasks().length, 0);
});

test('local merge state, not MR metadata, controls preservation and deletion', async () => {
  const storage = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-merge-gate-'));
  const store = new TaskStore(storage);
  const control = new TaskControlService(store);
  try {
    const thread = makeThread({ worktree: {
      path: path.join(storage, 'worktree'), mainPath: path.join(storage, 'repo'), repositoryPath: path.join(storage, 'repo'),
      commonDir: path.join(storage, 'repo', '.git'), targetRef: 'refs/heads/mesh/integration/test', taskBranch: 'mesh/task/thread_merge_gate',
      branch: 'mesh/task/thread_merge_gate', baseCommit: 'base', headCommit: 'approved', approvedCommit: 'approved',
      integration: { phase: 'ready', updatedAt: new Date().toISOString() }, mergePending: true,
    } });
    store.upsertTask(thread);
    assert.deepStrictEqual(pendingMerges(store.getAllTasks()).map((task) => task.threadId), ['thread_merge_gate']);
    store.recordMergeRequest('thread_merge_gate', {
      url: 'https://example.test/mr/1', branch: 'mesh/integration/test', commit: 'approved', remote: 'origin',
      status: 'synced', summary: 'remote record only', updatedAt: new Date().toISOString(), by: 'codex',
    });
    assert.strictEqual(pendingMerges(store.getAllTasks()).length, 1, 'an MR URL cannot claim that local integration succeeded');
  await assert.rejects(() => control.deleteTask('thread_merge_gate', 'cleanup', 'test', false), /not been integrated locally/i);
  await control.deleteTask('thread_merge_gate', 'discard explicitly', 'test', true);
    assert.strictEqual(store.getTask('thread_merge_gate'), undefined);
  } finally {
    store.dispose();
    fs.rmSync(storage, { recursive: true, force: true });
  }
});
