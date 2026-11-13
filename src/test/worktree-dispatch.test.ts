import { testWorkspace } from './workspace-fixture';
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
import { WorkspaceRegistry } from '../bridge/workspace';

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
    workspace: testWorkspace(os.tmpdir()), summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 1 }, rounds: [],
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

  const result = await dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Isolated task', prompt: 'inspect the committed file', workspaceRoot: repository });
  const task = store.getTask(result.threadId)!;
  assert.equal(task.workspace.executionPath, task.worktree?.path);
  assert.equal(task.workspace.rootPath, fs.realpathSync.native(repository));
  assert.notStrictEqual(path.resolve(task.workspace.executionPath), path.resolve(repository));
  assert.strictEqual(task.worktree?.baseCommit, base);
  assert.strictEqual(task.worktree?.headCommit, base);
  assert.equal(task.worktree?.targetRef, 'refs/heads/feature/caller');
  assert.equal(task.worktree?.deliveryTarget?.branch, 'feature/caller');
  assert.strictEqual(fs.readFileSync(path.join(task.workspace.executionPath, 'tracked.txt'), 'utf8').replace(/\r\n/g, '\n'), 'committed input\n');
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

test('ordinary linked checkouts group with their main workspace without changing the delivery branch', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-linked-workspace-'));
  const repository = path.join(root, 'repo'); const linked = path.join(root, 'linked');
  const alias = path.join(root, 'alias'); const clone = path.join(root, 'clone'); const clone2 = path.join(root, 'clone2');
  const storage = path.join(root, 'ledger'); initRepo(repository);
  git(repository, 'worktree', 'add', '-b', 'linked-branch', linked);
  fs.writeFileSync(path.join(linked, 'tracked.txt'), 'linked branch commit\n');
  git(linked, 'add', 'tracked.txt'); git(linked, 'commit', '-m', 'linked branch change');
  const linkedHead = git(linked, 'rev-parse', 'HEAD');
  const mainHead = git(repository, 'rev-parse', 'HEAD');
  assert.notEqual(linkedHead, mainHead);
  fs.mkdirSync(path.join(linked, 'subdir'));
  fs.symlinkSync(linked, alias, process.platform === 'win32' ? 'junction' : 'dir');
  git(root, 'clone', repository, clone);
  git(root, 'clone', repository, clone2);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector); const callback = new CallbackManager(detector, storage);
  const executions = new TaskExecutionRegistry(storage);
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, executions);
  t.after(() => { (dispatcher as any).queueDrainer?.dispose(); executions.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 50 }); });
  const workspaces = new WorkspaceRegistry(storage);
  const mainId = workspaces.register(repository).id;
  const send = (workspaceRoot: string, targetBranch?: string) => dispatcher.dispatchTask({
    workspaceRoot, targetBranch, targetAgent: 'claude_code', initiatorAgent: 'codex',
    title: 'Linked workspace', prompt: 'run', reviewFlow: 'dedicated',
  });
  for (const input of [linked, alias, path.join(alias, 'subdir')]) {
    const result = await send(input);
    const task = store.getTask(result.threadId)!;
    assert.equal(result.workspace.id, mainId);
    assert.equal(result.workspace.rootPath, fs.realpathSync.native(repository));
    assert.equal(task.worktree?.deliveryTarget?.branch, 'linked-branch');
    assert.equal(task.worktree?.targetRef, 'refs/heads/linked-branch');
    assert.equal(task.worktree?.baseCommit, linkedHead);
    assert.equal(task.workspace.executionPath, task.worktree?.path);
    assert.notEqual(task.worktree?.path, fs.realpathSync.native(linked));
  }
  const explicit = await send(linked, 'feature/caller');
  assert.equal(store.getTask(explicit.threadId)!.worktree?.deliveryTarget?.branch, 'feature/caller');
  assert.equal(store.getTask(explicit.threadId)!.worktree?.baseCommit, mainHead);
  assert.equal(explicit.workspace.id, mainId);
  const both = await dispatcher.dispatchTask({ workspaceRoot: linked, workspaceId: mainId,
    targetAgent: 'claude_code', initiatorAgent: 'codex', title: 'Linked id and root', prompt: 'run', reviewFlow: 'dedicated' });
  assert.equal(both.workspace.id, mainId);
  assert.equal(store.getTask(both.threadId)!.worktree?.deliveryTarget?.branch, 'linked-branch');
  const independent = await send(clone);
  assert.notEqual(independent.workspace.id, mainId);
  assert.equal(independent.workspace.rootPath, fs.realpathSync.native(clone));
  const independent2 = await send(clone2);
  assert.notEqual(independent2.workspace.id, independent.workspace.id);
  assert.notEqual(independent2.workspace.id, mainId);
  assert.equal(independent2.workspace.rootPath, fs.realpathSync.native(clone2));
  assert.equal(workspaces.list().length, 3, 'linked checkouts do not create separate workspace records');
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
    dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'parallel dispatch', prompt: 'start independent task', workspaceRoot: repository }),
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
  await assert.rejects(dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Must fail', prompt: 'do work', workspaceRoot: root }), /Git|repository|HEAD/i);
  await assert.rejects(dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Old API', prompt: 'do work', workspacePath: root } as any), /workspacePath is not supported/);
  await assert.rejects(dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'Missing', prompt: 'do work' }), /WORKSPACE_REQUIRED/);
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
    thread.workspace.executionPath = thread.worktree!.path;
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
