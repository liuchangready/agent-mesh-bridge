import { testWorkspace } from './workspace-fixture';
import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import * as zlib from 'zlib';
import { readBridgePolicy, saveBridgePolicy } from '../bridge/bridge-policy';
import {
  ensureTaskDataDir,
  resolveTaskDataRoot,
  taskArtifactDir,
} from '../bridge/task-data-dir';
import { createTaskWorktree, resolveGitRepository } from '../bridge/git-worktree';
import {
  roundExecutionLogPath,
  taskArchiveDir,
  taskModelOverlayDir,
  taskReviewerLogDir,
  taskWorkerLogDir,
  taskWorktreeDir,
} from '../bridge/round-log';
import { reviewerLogPath } from '../bridge/reviewer-log';
import { archiveCompletedTaskLogs, removeTaskArtifacts } from '../bridge/task-summary';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { TaskControlService } from '../bridge/task-control';
import { MeshFileLock } from '../bridge/merge-coordinator';

const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'git.exe' : 'git');

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

function tarContains(tarGzPath: string, fragment: string): boolean {
  return zlib.gunzipSync(fs.readFileSync(tarGzPath)).toString('utf8').includes(fragment);
}

test('task data root defaults to <storage>/tasks and resolves relative values against the bridge home', (t) => {
  const storage = isolatedDir('mesh-taskdata-root-');
  t.after(() => removeTree(storage));

  assert.strictEqual(resolveTaskDataRoot(storage), path.join(storage, 'tasks'));

  saveBridgePolicy({ taskDataRoot: 'task-data' }, storage);
  assert.strictEqual(resolveTaskDataRoot(storage), path.resolve(storage, 'task-data'));
  assert.strictEqual(readBridgePolicy(storage).taskDataRoot, 'task-data');

  const absolute = isolatedDir('mesh-taskdata-abs-');
  t.after(() => removeTree(absolute));
  saveBridgePolicy({ taskDataRoot: absolute }, storage);
  assert.strictEqual(resolveTaskDataRoot(storage), path.normalize(absolute));

  // Clearing returns to the default; a legacy worktreeRoot must not leak into it.
  saveBridgePolicy({ taskDataRoot: '   ', worktreeRoot: path.join(storage, 'legacy-worktrees') }, storage);
  assert.strictEqual(readBridgePolicy(storage).taskDataRoot, undefined);
  assert.strictEqual(resolveTaskDataRoot(storage), path.join(storage, 'tasks'));
});

test('a task directory is created with the standard layout and the worktree lives inside it', (t) => {
  const storage = isolatedDir('mesh-taskdata-layout-');
  const repo = path.join(storage, 'repo');
  initRepo(repo);
  t.after(() => removeTree(storage));

  const threadId = 'thread_layout_1';
  const dirs = ensureTaskDataDir(threadId, storage);
  assert.strictEqual(dirs.artifactDir, taskArtifactDir(threadId, storage));
  for (const dir of [
    dirs.artifactDir,
    dirs.workerLogDir,
    dirs.reviewerLogDir,
    dirs.archiveDir,
    dirs.bootstrapDir,
    dirs.modelOverlayDir,
  ]) {
    assert.strictEqual(fs.existsSync(dir), true, dir);
  }
  assert.strictEqual(taskWorktreeDir(dirs.artifactDir), path.join(dirs.artifactDir, 'worktree'));
  assert.strictEqual(taskWorkerLogDir(dirs.artifactDir), path.join(dirs.artifactDir, 'worker_log'));
  assert.strictEqual(taskReviewerLogDir(dirs.artifactDir), path.join(dirs.artifactDir, 'reviewer_log'));
  assert.strictEqual(taskArchiveDir(dirs.artifactDir), path.join(dirs.artifactDir, 'Archive'));

  const worktree = createTaskWorktree(
    resolveGitRepository(repo, storage),
    threadId,
    storage,
    undefined,
    dirs.artifactDir,
  );
  assert.strictEqual(path.resolve(worktree.path), path.resolve(dirs.artifactDir, 'worktree'));
  assert.strictEqual(fs.readFileSync(path.join(worktree.path, 'readme.txt'), 'utf8').replace(/\r\n/g, '\n'), 'base\n');
  assert.strictEqual(fs.existsSync(path.join(worktree.path, '.agent-mesh-task.json')), true);
  assert.strictEqual(
    roundExecutionLogPath(threadId, 1, undefined, 'codex', dirs.artifactDir),
    path.join(dirs.workerLogDir, `${threadId}_round_1.log`),
  );
  assert.strictEqual(
    reviewerLogPath(threadId, 1, undefined, 'codex', dirs.artifactDir),
    path.join(dirs.reviewerLogDir, `${threadId}_round_1.reviewer.log`),
  );
});

test('archive and cleanup stay inside the task directory and never delete the archive prematurely', (t) => {
  const storage = isolatedDir('mesh-taskdata-archive-');
  t.after(() => removeTree(storage));
  const threadId = 'thread_archive_boundary';
  const dirs = ensureTaskDataDir(threadId, storage);

  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.log`), 'worker bytes', 'utf8');
  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.output.md`), 'full output', 'utf8');
  fs.writeFileSync(path.join(dirs.reviewerLogDir, `${threadId}_round_1.reviewer.log`), 'review bytes', 'utf8');
  fs.writeFileSync(path.join(dirs.reviewerLogDir, `${threadId}_bootstrap.log`), 'bootstrap bytes', 'utf8');
  fs.writeFileSync(path.join(dirs.bootstrapDir, `${threadId}.json`), '{"token":"secret"}', 'utf8');
  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.native_op.recovery.json`), '{"recovery":true}', 'utf8');
  const overlayFile = path.join(dirs.modelOverlayDir, 'deepseek-task.yml');
  fs.writeFileSync(overlayFile, 'model: task-overlay\n', 'utf8');

  const thread = {
    threadId,
    title: 'Unified task data',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'completed',
    currentRound: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    receiverSessionId: '',
    workspace: testWorkspace(path.join(dirs.worktreeDir)),
    artifactDir: dirs.artifactDir,
    taskDataDir: dirs.artifactDir,
    rounds: [],
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
  } as unknown as DelegationThread;

  const archived = archiveCompletedTaskLogs(thread, storage);
  assert.deepStrictEqual(archived.tarGzPaths, [path.join(dirs.archiveDir, `${threadId}_archive.tar.gz`)]);
  assert.deepStrictEqual(archived.jsonPaths, [path.join(dirs.archiveDir, `${threadId}_archive.json`)]);
  assert.strictEqual(fs.existsSync(archived.tarGzPaths[0]), true);
  assert.strictEqual(tarContains(archived.tarGzPaths[0], `worker_log/${threadId}_round_1.log`), true);
  assert.strictEqual(tarContains(archived.tarGzPaths[0], `reviewer_log/${threadId}_round_1.reviewer.log`), true);
  assert.strictEqual(tarContains(archived.tarGzPaths[0], `worker_log/${threadId}_round_1.native_op.recovery.json`), true);
  assert.strictEqual(tarContains(archived.tarGzPaths[0], 'model-overlays/deepseek-task.yml'), true);
  assert.strictEqual(tarContains(archived.tarGzPaths[0], 'summary.json'), true);

  removeTaskArtifacts(threadId, storage, {
    keepArchive: true,
    targetAgent: thread.targetAgent,
    initiatorAgent: thread.initiatorAgent,
    artifactDir: dirs.artifactDir,
  });
  assert.strictEqual(fs.existsSync(path.join(dirs.workerLogDir, `${threadId}_round_1.log`)), false);
  assert.strictEqual(fs.existsSync(path.join(dirs.reviewerLogDir, `${threadId}_round_1.reviewer.log`)), false);
  assert.strictEqual(fs.existsSync(path.join(dirs.bootstrapDir, `${threadId}.json`)), false);
  assert.strictEqual(fs.existsSync(overlayFile), false);
  assert.strictEqual(fs.existsSync(archived.tarGzPaths[0]), true);
  assert.strictEqual(fs.existsSync(archived.jsonPaths[0]), true);

  removeTaskArtifacts(threadId, storage, {
    keepArchive: false,
    artifactDir: dirs.artifactDir,
  });
  assert.strictEqual(fs.existsSync(archived.tarGzPaths[0]), false);
  assert.strictEqual(fs.existsSync(archived.jsonPaths[0]), false);
});

test('TaskStore.deleteTask removes a task directory and its bootstrap credential', (t) => {
  const storage = isolatedDir('mesh-taskdata-delete-');
  t.after(() => removeTree(storage));
  const store = new TaskStore(storage);
  t.after(() => store.dispose());
  const threadId = 'thread_delete_unified';
  const dirs = ensureTaskDataDir(threadId, storage);
  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.log`), 'worker', 'utf8');
  fs.writeFileSync(path.join(dirs.bootstrapDir, `${threadId}.json`), '{"token":"secret"}', 'utf8');

  const now = new Date().toISOString();
  store.upsertTask({
    threadId,
    title: 'Delete unified',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'failed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: '',
    workspace: testWorkspace(path.join(storage, 'workspace')),
    artifactDir: dirs.artifactDir,
    taskDataDir: dirs.artifactDir,
    rounds: [],
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
  });

  assert.strictEqual(store.deleteTask(threadId), true);
  assert.strictEqual(fs.existsSync(dirs.artifactDir), false);
});

test('delete_task removes the unified directory including its worktree', async (t) => {
  const storage = isolatedDir('mesh-taskdata-delete-wt-');
  const repo = path.join(storage, 'repo');
  initRepo(repo);
  t.after(() => removeTree(storage));
  const store = new TaskStore(storage);
  t.after(() => store.dispose());
  const threadId = 'thread_delete_worktree';
  const dirs = ensureTaskDataDir(threadId, storage);
  const worktree = createTaskWorktree(
    resolveGitRepository(repo, storage),
    threadId,
    storage,
    undefined,
    dirs.artifactDir,
  );
  fs.writeFileSync(path.join(worktree.path, 'readme.txt'), 'changed\n', 'utf8');
  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.log`), 'worker', 'utf8');
  fs.writeFileSync(path.join(dirs.modelOverlayDir, 'deepseek-delete.yml'), 'model: delete-overlay\n', 'utf8');

  const now = new Date().toISOString();
  store.upsertTask({
    threadId,
    title: 'Delete unified worktree',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'failed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: '',
    workspace: testWorkspace(worktree.path),
    artifactDir: dirs.artifactDir,
    taskDataDir: dirs.artifactDir,
    worktree: { ...worktree, headCommit: worktree.baseCommit },
    rounds: [],
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
  });

  const control = new TaskControlService(
    store,
    undefined,
    undefined,
    storage,
    () => ({ entries: [], removedCount: 0 }),
    new MeshFileLock(path.join(storage, 'delete.lock')),
  );
  const result = await control.deleteTask(threadId, 'test cleanup', 'test', true);
  assert.strictEqual(result.status, 'DELETED');
  assert.strictEqual(fs.existsSync(dirs.artifactDir), false);
  assert.strictEqual(fs.existsSync(dirs.modelOverlayDir), false);
  assert.strictEqual(fs.existsSync(worktree.path), false);
  assert.strictEqual(store.getTask(threadId), undefined);
});

test('legacy threads without artifactDir keep using per-agent log directories', (t) => {
  const storage = isolatedDir('mesh-taskdata-legacy-');
  t.after(() => removeTree(storage));
  const threadId = 'thread_legacy_paths';
  const logDir = path.join(storage, 'custom-logs');
  saveBridgePolicy({ logDirectories: { default: logDir } }, storage);

  const legacyPath = roundExecutionLogPath(threadId, 2, storage, 'codex');
  assert.strictEqual(legacyPath, path.join(logDir, 'worker_log', `${threadId}_round_2.log`));
  assert.strictEqual(fs.existsSync(taskArtifactDir(threadId, storage)), false);
});

test('TaskStore.archiveTask keeps overlays, rounds and sessions when an overlay read fails, then retries', (t) => {
  const storage = isolatedDir('mesh-taskdata-archive-eio-');
  const store = new TaskStore(storage);
  const fsModule: any = require('fs');
  const originalReadFileSync = fsModule.readFileSync;
  t.after(() => {
    fsModule.readFileSync = originalReadFileSync;
    store.dispose();
    removeTree(storage);
  });

  const threadId = 'thread_archive_overlay_eio';
  const dirs = ensureTaskDataDir(threadId, storage);
  const overlayFile = path.join(dirs.modelOverlayDir, 'model.yml');
  fs.writeFileSync(overlayFile, 'model: overlay-eio\n', 'utf8');
  fs.writeFileSync(path.join(dirs.workerLogDir, `${threadId}_round_1.log`), 'worker', 'utf8');
  const now = new Date().toISOString();
  store.upsertTask({
    threadId,
    title: 'Archive overlay EIO',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'completed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'receiver-session-eio',
    workspace: testWorkspace(path.join(dirs.worktreeDir)),
    artifactDir: dirs.artifactDir,
    taskDataDir: dirs.artifactDir,
    rounds: [{
      round: 1,
      type: 'initial',
      prompt: 'do the work',
      output: 'done',
      durationMs: 1,
      status: 'completed',
      timestamp: now,
    }],
    summary: { lastPromptSnippet: 'do the work', lastOutputSnippet: 'done', totalDurationMs: 1 },
  });

  let injectOverlayEio = true;
  fsModule.readFileSync = function (file: any, ...args: any[]) {
    if (injectOverlayEio && path.resolve(String(file)) === path.resolve(overlayFile)) {
      const error: any = new Error('injected overlay EIO');
      error.code = 'EIO';
      throw error;
    }
    return originalReadFileSync.call(this, file, ...args);
  };

  const failed = store.archiveTask(threadId);
  assert.strictEqual(failed, undefined, 'archive must fail instead of reporting success');
  const afterFailure = store.getTask(threadId)!;
  assert.strictEqual(afterFailure.archivedAt, undefined, 'failed archive must not mark the task archived');
  assert.strictEqual(afterFailure.rounds.length, 1, 'failed archive must retain rounds');
  assert.strictEqual(afterFailure.receiverSessionId, 'receiver-session-eio', 'failed archive must retain the session');
  assert.strictEqual(fs.existsSync(overlayFile), true, 'failed archive must retain the unarchived overlay');
  assert.strictEqual(fs.existsSync(path.join(dirs.archiveDir, `${threadId}_archive.tar.gz`)), false);

  // Remove the fault and retry: the same task must archive completely.
  injectOverlayEio = false;
  const archived = store.archiveTask(threadId);
  assert.ok(archived);
  assert.ok(archived!.archivedAt);
  const tarPath = path.join(dirs.archiveDir, `${threadId}_archive.tar.gz`);
  const jsonPath = path.join(dirs.archiveDir, `${threadId}_archive.json`);
  assert.strictEqual(fs.existsSync(tarPath), true);
  assert.strictEqual(tarContains(tarPath, 'model-overlays/model.yml'), true);
  const manifest = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
  assert.ok(
    manifest.archivedLogFiles.includes('model-overlays/model.yml'),
    'manifest must list the archived overlay',
  );
  assert.strictEqual(fs.existsSync(overlayFile), false, 'a successful archive may prune the active overlay');
  assert.strictEqual(archived!.rounds.length, 0);
});

test('dispatch pins the absolute task directory and writes its round log there', async (t) => {
  const storage = isolatedDir('mesh-taskdata-dispatch-');
  const repo = path.join(storage, 'repo');
  initRepo(repo);
  const originalCwd = process.cwd();
  process.chdir(repo);
  const store = new TaskStore(storage);
  const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({
    exitCode: 0,
    stdout: 'mock output',
    stderr: '',
    extractedOutput: 'mock output',
    durationMs: 1,
  });
  const callbackManager = new CallbackManager(detector, storage);
  callbackManager.notifyCaller = async () => true;
  callbackManager.notifyCallerOfFailure = async () => false;
  const dispatcher = new AsyncDispatcher(
    store,
    new ActiveAgentResolver(detector),
    runner,
    callbackManager,
    detector,
    new TaskExecutionRegistry(storage),
  );
  const originalSupervisorProbe = TaskSupervisor.isSupervisorActive;
  (TaskSupervisor as any).isSupervisorActive = () => false;
  t.after(() => {
    (TaskSupervisor as any).isSupervisorActive = originalSupervisorProbe;
    process.chdir(originalCwd);
    store.dispose();
    removeTree(storage);
  });

  const result = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
    targetAgent: 'codex',
    initiatorAgent: 'antigravity',
    title: 'Unified dispatch layout',
    prompt: 'do the work',
    reviewFlow: 'legacy',
  });
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline && store.getTask(result.threadId)?.status !== 'waiting_review') {
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  const thread = store.getTask(result.threadId)!;
  assert.strictEqual(thread.status, 'waiting_review');
  assert.strictEqual(thread.artifactDir, path.join(storage, 'tasks', result.threadId));
  assert.strictEqual(thread.taskDataDir, thread.artifactDir);
  assert.strictEqual(path.resolve(thread.worktree!.path), path.join(thread.artifactDir!, 'worktree'));
  assert.strictEqual(fs.existsSync(path.join(thread.artifactDir!, 'worktree', 'readme.txt')), true);
  assert.strictEqual(fs.existsSync(path.join(thread.artifactDir!, 'worker_log', `${result.threadId}_round_1.log`)), true);
  // No bridge log may be written inside the Git worktree content.
  assert.strictEqual(fs.existsSync(path.join(thread.worktree!.path, 'worker_log')), false);
});

