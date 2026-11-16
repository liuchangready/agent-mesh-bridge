import { testWorkspace } from './workspace-fixture';
import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput, removeTaskWorktree, snapshotTaskWorktree } from '../bridge/git-worktree';
import { buildPosixLockHelperCommand, MergeCoordinator, MeshFileLock, validationNodeDirs } from '../bridge/merge-coordinator';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread, WorktreeInfo } from '../bridge/types';
import { TaskControlService } from '../bridge/task-control';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { ReworkController } from '../bridge/rework-controller';

const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
function git(cwd: string, ...args: string[]): string {
  const result = spawnSync(gitExe, args, { cwd, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  return result.stdout.trim();
}

/**
 * Compare two paths that are known to exist using their filesystem real path.
 *
 * Git returns the canonical on-disk case on Windows; path.resolve() does not
 * canonicalize existing directory case, so a TEMP env like `C:\Windows\TEMP`
 * would not compare equal to the real `C:\Windows\Temp`. realpathSync.native
 * keeps POSIX comparisons case-sensitive while normalizing Windows casing.
 */
function assertSameExistingPath(actual: string, expected: string): void {
  assert.equal(fs.realpathSync.native(actual), fs.realpathSync.native(expected));
}

function initRepo(root: string): void {
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-b', 'feature/caller');
  git(root, 'config', 'user.name', 'Mesh Test');
  git(root, 'config', 'user.email', 'mesh-test@example.test');
  fs.writeFileSync(path.join(root, 'shared.txt'), 'alpha\nmiddle\nbeta\n', 'utf8');
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({
    scripts: { compile: 'node validate.js compile', test: 'node validate.js test' },
  }), 'utf8');
  fs.writeFileSync(path.join(root, 'validate.js'), `const fs=require('fs'),path=require('path');\nconst base=process.env.MESH_VALIDATION_SIGNAL_DIR;const id=process.cwd().includes('thread_merge_A')?'thread_merge_A':process.cwd().includes('thread_merge_B')?'thread_merge_B':'';\nif(id){fs.writeFileSync(path.join(base,id+'.entered'),'yes');if(process.env.MESH_VALIDATION_HOLD_THREAD===id)while(!fs.existsSync(path.join(base,id+'.release')))Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,20);}\nif(process.env.MESH_VALIDATION_FAIL==='1')process.exit(6);\nif(process.argv[2]==='test'){const text=fs.readFileSync(path.join(process.cwd(),'shared.txt'),'utf8');if(!text.includes('A')&&!text.includes('B'))process.exit(2);}\n`, 'utf8');
  git(root, 'add', '-A');
  git(root, 'commit', '-m', 'base');
}

function thread(threadId: string, worktree: WorktreeInfo): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code',
    status: 'waiting_merge', currentRound: 1, createdAt: now, updatedAt: now,
    receiverSessionId: `session-${threadId}`, workspace: testWorkspace(worktree.path),
    summary: { lastPromptSnippet: 'review', lastOutputSnippet: 'approved', totalDurationMs: 1 },
    rounds: [{ round: 1, type: 'initial', prompt: 'change one line', output: 'done', durationMs: 1, status: 'completed', timestamp: now }],
    worktree: { ...worktree, approvedCommit: worktree.headCommit, integration: { phase: 'waiting', sourceCommit: worktree.headCommit, updatedAt: now } },
  };
}

async function waitFor(file: string, attempts = 300): Promise<void> {
  for (let n = 0; n < attempts; n++) {
    if (fs.existsSync(file)) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${file}`);
}

test('candidate snapshots preserve the worker index and exclude ignored and later changes', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snapshot-'));
  const repository = path.join(root, 'repo');
  initRepo(repository);
  const worktree = createTaskWorktree(resolveGitRepository(repository, path.join(root, 'ledger')), 'thread_snapshot', path.join(root, 'ledger'));
  t.after(() => {
    removeTaskWorktree(worktree);
    const writable = (dir: string): void => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        const file = path.join(dir, entry.name);
        if (entry.isDirectory()) writable(file); else fs.chmodSync(file, 0o666);
      }
    };
    writable(root);
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  fs.writeFileSync(path.join(worktree.path, '.gitignore'), 'ignored.txt\n');
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'staged\n');
  git(worktree.path, 'add', 'shared.txt');
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'final\n');
  fs.writeFileSync(path.join(worktree.path, 'new.txt'), 'new\n');
  fs.writeFileSync(path.join(worktree.path, 'ignored.txt'), 'ignored\n');
  fs.unlinkSync(path.join(worktree.path, 'validate.js'));
  const status = git(worktree.path, 'status', '--porcelain');
  const head = git(worktree.path, 'rev-parse', 'HEAD');
  const candidate = snapshotTaskWorktree(worktree);
  assert.equal(git(worktree.path, 'rev-parse', 'HEAD'), head);
  assert.equal(git(worktree.path, 'status', '--porcelain'), status);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'later\n');
  assert.equal(git(worktree.path, 'show', `${candidate.commit}:shared.txt`), 'final');
  assert.equal(git(worktree.path, 'show', `${candidate.commit}:new.txt`), 'new');
  assert.equal(git(worktree.path, 'ls-tree', '--name-only', candidate.commit, 'ignored.txt', 'validate.js'), '');
  assert.equal(git(worktree.path, 'rev-parse', `${candidate.commit}^{tree}`), candidate.tree);
});

test('approval integrates the frozen dirty candidate after later worker edits', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snapshot-approval-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_approval_snapshot', storage);
  const store = new TaskStore(storage);
  const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_approval_snapshot', worktree);
  item.status = 'waiting_review';
  item.worktree!.approvedCommit = undefined;
  item.worktree!.integration = { phase: 'ready', updatedAt: new Date().toISOString() };
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  const before = git(worktree.path, 'status', '--porcelain');
  const head = git(worktree.path, 'rev-parse', 'HEAD');
  assert.equal(controller.approveTask({ threadId: item.threadId }).status, 'WAITING_MERGE');
  assert.equal(git(worktree.path, 'status', '--porcelain'), before);
  assert.equal(git(worktree.path, 'rev-parse', 'HEAD'), head);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'later change\n');
  const result = await controller.mergeApproved(item.threadId);
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A approved\nmiddle\nbeta');
  assert.equal(fs.readFileSync(path.join(worktree.path, 'shared.txt'), 'utf8'), 'later change\n');
  const log = fs.readFileSync(store.getTask(item.threadId)!.mergeHistory!.find((entry) => entry.phase === 'attempt')!.logPath!, 'utf8');
  assert.match(log, /\$ git merge --no-commit --no-ff/);
  assert.match(log, /compile passed/); assert.match(log, /test passed/);
});

test('CAS competition retries against the advanced target without waking a reviewer', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-cas-retry-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_cas_retry', storage);
  const store = new TaskStore(storage);
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_cas_retry', worktree);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  const snapshot = snapshotTaskWorktree(worktree);
  item.worktree!.approvedCommit = snapshot.commit; item.worktree!.approvedSnapshot = snapshot;
  fs.writeFileSync(path.join(repository, 'local.txt'), 'local');
  store.upsertTask(item);
  let wakes = 0;
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(storage, 'merge.lock')), () => { wakes++; });
  const original = (coordinator as any).loggedGit.bind(coordinator);
  let advanced = false;
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    if (!advanced && args[0] === 'update-ref' && args[1] === target.targetRef) {
      advanced = true;
      fs.writeFileSync(path.join(repository, 'target.txt'), 'concurrent change\n');
      git(repository, 'add', 'target.txt'); git(repository, 'commit', '-m', 'concurrent update');
    }
    return original(id, cwd, args);
  };
  const result = await coordinator.mergeApproved(item.threadId);
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:target.txt`), 'concurrent change');
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A approved\nmiddle\nbeta');
  assert.equal(fs.readFileSync(path.join(repository, 'local.txt'), 'utf8'), 'local');
  assert.ok(store.getTask(item.threadId)!.mergeHistory!.some((entry) => entry.errorCode === 'CAS_FAILED'));
  assert.equal(wakes, 0);
});

test('snapshot failure records a blocked merge and requests a reviewer wake', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snapshot-failure-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const worktree = createTaskWorktree(resolveGitRepository(repository, storage), 'thread_snapshot_failure', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const callback = new CallbackManager(detector, storage);
  let woken = false; callback.notifyCaller = async () => { woken = true; return true; };
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_snapshot_failure', worktree);
  item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  removeTaskWorktree(worktree);
  assert.equal(controller.approveTask({ threadId: item.threadId }).status, 'WAITING_MERGE');
  await new Promise((resolve) => setTimeout(resolve, 20));
  const saved = store.getTask(item.threadId)!;
  assert.equal(saved.status, 'waiting_merge');
  assert.equal(saved.worktree!.integration!.phase, 'blocked');
  assert.equal(saved.mergeHistory![0].phase, 'snapshot');
  assert.match(fs.readFileSync(saved.mergeHistory![0].logPath!, 'utf8'), /Could not create approved snapshot/);
  assert.equal(woken, true);
});

test('conflicting approved snapshot keeps the merge workspace and approval for review', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snapshot-conflict-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_snapshot_conflict', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_snapshot_conflict', worktree);
  item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  const candidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  const result = await controller.mergeApproved(item.threadId);
  const saved = store.getTask(item.threadId)!;
  assert.equal(result.status, 'conflict');
  assert.equal(saved.status, 'waiting_merge');
  assert.equal(saved.worktree!.approvedCommit, candidate);
  assert.deepEqual(saved.worktree!.integration!.conflictFiles, ['shared.txt']);
  assert.ok(fs.existsSync(saved.worktree!.mergeResolutionPath!));
  assert.match(fs.readFileSync(path.join(saved.worktree!.mergeResolutionPath!, 'shared.txt'), 'utf8'), /<<<<<<< /);
  const resolution = saved.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A resolved\nmiddle\nbeta\n');
  git(resolution, 'add', 'shared.txt');
  assert.equal(controller.submitMergeResolution(item.threadId).status, 'WAITING_MERGE');
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A resolved\nmiddle\nbeta');
  assert.equal(git(repository, 'rev-parse', `${merged.commit}^1`), saved.worktree!.integration!.targetOldCommit);
});

test('failed combined validation retains the approved snapshot and resolution workspace', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-snapshot-validation-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_snapshot_validation', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { delete process.env.MESH_VALIDATION_FAIL; controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_snapshot_validation', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  fs.writeFileSync(path.join(worktree.path, 'validate.js'), `if(process.env.MESH_VALIDATION_FAIL==='1'){process.stdout.write('START-OUTPUT-'+ 'x'.repeat(25000)+'-END-OUTPUT');process.exitCode=6;}`);
  controller.approveTask({ threadId: item.threadId });
  const candidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  process.env.MESH_VALIDATION_FAIL = '1';
  const failed = await controller.mergeApproved(item.threadId); delete process.env.MESH_VALIDATION_FAIL;
  const saved = store.getTask(item.threadId)!;
  assert.equal(failed.status, 'validation_failed', failed.message); assert.equal(saved.status, 'waiting_merge');
  assert.equal(saved.worktree!.approvedCommit, candidate);
  assert.ok(fs.existsSync(saved.worktree!.mergeResolutionPath!));
  const mergeLog = fs.readFileSync(saved.mergeHistory![0].logPath!, 'utf8');
  assert.match(mergeLog, /failed \(exit 6\)/);
  assert.match(mergeLog, /\$ git merge --no-commit --no-ff/);
  assert.ok(mergeLog.includes('START-OUTPUT-') && mergeLog.includes('-END-OUTPUT'));
  controller.submitMergeResolution(item.threadId);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
});

test('six repeated merge retries enter merge_failed and retry_merge starts a new generation', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-budget-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_merge_budget', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_merge_budget', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A change\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  const coordinator = (controller as any).mergeCoordinator;
  coordinator.validate = async () => ({ ok: false, error: 'test failed (exit 2): same failure' });
  for (let n = 0; n <= 6; n++) {
    if (n) controller.submitMergeResolution(item.threadId);
    assert.equal((await controller.mergeApproved(item.threadId)).status, 'validation_failed');
    const saved = store.getTask(item.threadId)!;
    assert.equal(saved.mergeControl!.consecutiveRetries, n);
    assert.equal(saved.status, n === 6 ? 'merge_failed' : 'waiting_merge');
  }
  const before = store.getTask(item.threadId)!;
  const pathBefore = before.worktree!.mergeResolutionPath; const sessionBefore = before.receiverSessionId;
  assert.throws(() => controller.submitMergeResolution(item.threadId), /awaiting submission/);
  assert.throws(() => controller.approveTask({ threadId: item.threadId }), /merge_failed/);
  const retried = controller.retryMerge(item.threadId);
  const after = store.getTask(item.threadId)!;
  assert.equal(retried.retryGeneration, 1); assert.equal(after.status, 'waiting_merge');
  assert.equal(after.worktree!.mergeResolutionPath, pathBefore); assert.equal(after.receiverSessionId, sessionBefore);
  assert.ok(after.mergeHistory!.length > before.mergeHistory!.length);
});

test('deleting an unmerged snapshot unregisters its retained resolution worktree', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-delete-resolution-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_delete_resolution', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_delete_resolution', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(path.join(root, 'merge.lock')));
  await control.deleteTask(item.threadId, 'discard candidate', 'test', true);
  assert.equal(fs.existsSync(resolution), false);
  assert.equal(git(repository, 'worktree', 'list', '--porcelain').replace(/\\/g, '/').includes(resolution.replace(/\\/g, '/')), false);
});

test('a target that advances during conflict resolution is merged with the staged fix', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-target-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_advance', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_advance', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A resolved\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next\n'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance again');
  const head = git(repository, 'rev-parse', target.targetRef);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'rev-parse', `${merged.commit}^1`), head);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A resolved\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:next.txt`), 'next');
  assert.equal(store.getTask(item.threadId)!.status, 'completed');
});

test('rework and reapproval bind a new snapshot instead of the old submitted resolution', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-reapprove-resolution-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_reapprove_resolution', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  runner.execute = async () => {
    fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'alpha\nmiddle\nbeta\n');
    fs.writeFileSync(path.join(worktree.path, 'fresh.txt'), 'new candidate\n');
    return { exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 };
  };
  const controller = new ReworkController(store, runner, new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_reapprove_resolution', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  item.receiverSessionCaptured = true; store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'A target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A old resolution\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  const oldCandidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  const result = await controller.requestRework({ threadId: item.threadId, feedback: 'Change the candidate to add fresh.txt instead.' });
  assert.equal(result.receiverSessionId, item.receiverSessionId);
  for (let n = 0; n < 200 && store.getTask(item.threadId)?.status !== 'waiting_review'; n++) await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(store.getTask(item.threadId)?.status, 'waiting_review');
  controller.approveTask({ threadId: item.threadId });
  const approved = store.getTask(item.threadId)!.worktree!;
  assert.notEqual(approved.approvedCommit, oldCandidate);
  assert.equal(approved.mergeResolutionSubmitted, false);
  assert.equal(approved.mergeResolutionPath, undefined);
  assert.ok(approved.supersededMergeResolutionPaths?.includes(resolution));
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A target\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:fresh.txt`), 'new candidate');
  assert.equal(git(resolution, 'show', ':shared.txt'), 'A old resolution\nmiddle\nbeta');
});

test('a second conflict after target advancement keeps the same resolution workspace for another review', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-reconflict-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_reconflict', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_reconflict', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A first fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target again\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target again');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  assert.equal(store.getTask(item.threadId)!.worktree!.mergeResolutionPath, resolution);
  assert.match(fs.readFileSync(path.join(resolution, 'shared.txt'), 'utf8'), /<<<<<<< /);
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A final fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A final fix\nmiddle\nbeta');
});

test('a saved resolution survives a coordinator failure before switching to the advanced target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-recovery-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_recovery', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_recovery', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next\n'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');
  const coordinator = (controller as any).mergeCoordinator;
  const original = coordinator.writeTransaction.bind(coordinator);
  coordinator.writeTransaction = (file: string, tx: any) => { original(file, tx); if (tx.phase === 'resolution_rebase_prepared') throw new Error('injected interruption'); };
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'blocked');
  coordinator.writeTransaction = original;
  assert.equal(git(resolution, 'show', ':shared.txt'), 'A saved fix\nmiddle\nbeta');
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  controller.submitMergeResolution(item.threadId);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A saved fix\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:next.txt`), 'next');
  assert.equal(git(repository, 'show', `${target.targetRef}:again.txt`), 'again');
});

test('a saved resolution can recover when switching the merge workspace is interrupted', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-reset-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_reset', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_reset', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next\n'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');
  const coordinator = (controller as any).mergeCoordinator;
  const original = coordinator.loggedGit.bind(coordinator);
  coordinator.loggedGit = async (threadId: string, cwd: string, args: string[]) => {
    const value = await original(threadId, cwd, args);
    if (cwd === resolution && args[0] === 'reset') throw new Error('injected after reset');
    return value;
  };
  try { assert.equal((await controller.mergeApproved(item.threadId)).status, 'blocked'); }
  finally { coordinator.loggedGit = original; }
  assert.equal(store.getTask(item.threadId)!.worktree!.mergeResolutionSubmitted, true);
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A saved fix\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:next.txt`), 'next');
  assert.equal(git(repository, 'show', `${target.targetRef}:again.txt`), 'again');
});

test('an interrupted resolution merge is replayed in a fresh workspace when the target advances again', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-replay-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_resolution_replay', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_replay', worktree); item.status = 'waiting_review'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');
  const coordinator = (controller as any).mergeCoordinator; const original = coordinator.loggedGit.bind(coordinator);
  coordinator.loggedGit = async (id: string, cwd: string, args: string[]) => {
    const value = await original(id, cwd, args);
    if (cwd === resolution && args[0] === 'merge') throw new Error('injected after resolution merge');
    return value;
  };
  try { assert.equal((await controller.mergeApproved(item.threadId)).status, 'blocked'); } finally { coordinator.loggedGit = original; }
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  const write = coordinator.writeTransaction.bind(coordinator); let interrupted = false;
  coordinator.writeTransaction = (file: string, tx: any) => {
    write(file, tx);
    if (!interrupted && tx.phase === 'resolution_rebase_prepared' && tx.validationPath !== resolution) { interrupted = true; throw new Error('injected after replacement workspace transaction'); }
  };
  try { assert.equal((await controller.mergeApproved(item.threadId)).status, 'blocked'); } finally { coordinator.writeTransaction = write; }
  assert.equal(interrupted, true);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A saved fix\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:again.txt`), 'again');
  assert.ok(store.getTask(item.threadId)!.worktree!.supersededMergeResolutionPaths!.includes(resolution));
});

async function waitForExit(child: ReturnType<typeof spawn>): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => child.once('exit', () => resolve()));
}

async function waitForProcessExit(pid: number, attempts = 300): Promise<void> {
  for (let n = 0; n < attempts; n++) {
    try { process.kill(pid, 0); } catch { return; }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for helper process ${pid} to exit`);
}

test('POSIX helper command passes its channel path and acknowledges child registration', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-perl-lock-smoke-'));
  const lockPath = path.join(root, 'merge.lock');
  const channelPath = path.join(root, 'merge.channel');
  const { command, args } = buildPosixLockHelperCommand(lockPath, channelPath);
  assert.equal(command, 'perl');
  assert.deepEqual(args.slice(2), [lockPath, channelPath]);
  assert.match(args[1], /\$channel = \$ARGV\[1\]/);
  const configuredPerl = process.env.MESH_PERL_PATH || (process.platform === 'win32' && fs.existsSync('D:/Git/usr/bin/perl.exe') ? 'D:/Git/usr/bin/perl.exe' : command);
  const helper = spawn(configuredPerl, args, { stdio: ['pipe', 'ignore', 'pipe'], windowsHide: true });
  t.after(async () => {
    if (helper.exitCode === null && helper.signalCode === null) helper.kill('SIGKILL');
    await waitForExit(helper);
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  await waitFor(`${channelPath}.ready`);
  helper.stdin!.write(`CHILD ${process.pid}\n`);
  await waitFor(`${channelPath}.child.${process.pid}`);
  helper.stdin!.write(`EXIT ${process.pid}\nRELEASE\n`);
  helper.stdin!.end();
  await waitForExit(helper);
  assert.equal(helper.exitCode, 0);
});

test('two tasks edit the same file in independent worktrees and integrate serially from the lock-protected latest target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-git-merge-'));
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  const signals = path.join(root, 'signals');
  fs.mkdirSync(signals);
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const base = target.baseCommit;
  const a = createTaskWorktree(target, 'thread_merge_A', storage);
  const b = createTaskWorktree(target, 'thread_merge_B', storage);
  assert.notStrictEqual(a.path, b.path);
  assert.notStrictEqual(a.taskBranch, b.taskBranch);
  assert.strictEqual(a.targetRef, b.targetRef);
  assert.strictEqual(a.baseCommit, base);
  assert.strictEqual(b.baseCommit, base);
  assert.strictEqual(fs.readFileSync(path.join(a.path, 'shared.txt'), 'utf8').replace(/\r\n/g, '\n'), 'alpha\nmiddle\nbeta\n');

  fs.writeFileSync(path.join(a.path, 'shared.txt'), 'A\nmiddle\nbeta\n', 'utf8');
  fs.writeFileSync(path.join(b.path, 'shared.txt'), 'alpha\nmiddle\nB\n', 'utf8');
  a.headCommit = commitTaskChanges(a, 'approved A');
  b.headCommit = commitTaskChanges(b, 'approved B');
  assert.notStrictEqual(a.headCommit, b.headCommit);

  const store = new TaskStore(storage);
  t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  store.upsertTask(thread('thread_merge_A', a));
  store.upsertTask(thread('thread_merge_B', b));
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const mergeA = coordinator.mergeApproved('thread_merge_A');
  await waitFor(path.join(signals, 'thread_merge_A.entered'));
  assert.equal(new TaskRoleLock(storage).tryAcquire('thread_merge_A', 'reviewer'), null, 'reviewer cannot overlap merge validation');

  // A is inside its real combined verification while holding the mesh mutex.
  // A third task must still be admitted and receive its own worktree.
  const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({ exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 });
  const callback = new CallbackManager(detector, storage); const registry = new TaskExecutionRegistry(storage);
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  const dispatchResult = await Promise.race([
    dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'dispatch during validation', prompt: 'independent task', workspaceRoot: repository }),
    new Promise<never>((_, reject) => { const timer = setTimeout(() => reject(new Error('new dispatch waited on final integration lock')), 3_000); timer.unref(); }),
  ]);
  const admitted = store.getTask(dispatchResult.threadId)!;
  assert.notEqual(path.resolve(admitted.worktree!.path), path.resolve(a.path));
  assert.notEqual(path.resolve(admitted.worktree!.path), path.resolve(b.path));
  registry.dispose(); (dispatcher as any).queueDrainer?.dispose();

  const mergeB = coordinator.mergeApproved('thread_merge_B');
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.strictEqual(fs.existsSync(path.join(signals, 'thread_merge_B.entered')), false, 'B must not reach combined validation while A owns the global lock');
  assert.strictEqual(fs.existsSync(path.join(storage, 'merge-transactions', 'thread_merge_B.json')), false, 'B must not read/persist the target before acquiring the lock');
  fs.writeFileSync(path.join(signals, 'thread_merge_A.release'), 'go');
  const resultA = await mergeA;
  assert.strictEqual(resultA.status, 'merged');
  const commitA = resultA.commit!;

  await waitFor(path.join(signals, 'thread_merge_B.entered'));
  process.env.MESH_VALIDATION_HOLD_THREAD = '';
  const resultB = await mergeB;
  assert.strictEqual(resultB.status, 'merged', resultB.message);
  assert.strictEqual(store.getTask('thread_merge_B')!.worktree!.integration!.targetOldCommit, commitA);
  const finalRef = gitOutput(repository, ['rev-parse', target.targetRef]);
  assert.strictEqual(finalRef, resultB.commit);
  const combined = git(repository, 'show', `${target.targetRef}:shared.txt`);
  assert.strictEqual(combined, 'A\nmiddle\nB');
  assert.strictEqual(git(repository, 'rev-parse', 'feature/caller'), resultB.commit, 'the pinned real delivery branch advances');
  assert.strictEqual(fs.readFileSync(path.join(repository, 'shared.txt'), 'utf8').replace(/\r\n/g, '\n'), 'A\nmiddle\nB\n');
  assert.equal(git(repository, 'status', '--porcelain'), '', 'the checked-out target directory is synchronized and clean');
  assertSameExistingPath(
    store.getTask('thread_merge_B')!.worktree!.integration!.targetPath!,
    repository,
  );
  assert.equal(store.getTask('thread_merge_B')!.finalSummary?.gitDelivery?.targetBranch, 'feature/caller');
  assert.equal(store.getTask('thread_merge_B')!.finalSummary?.gitDelivery?.validationStatus, 'cleaned');
  const validationCommit = store.getTask('thread_merge_B')!.worktree!.validation!.commit!;
  assert.equal(validationCommit, resultB.commit);
  assert.equal(git(repository, 'show', `${validationCommit}:shared.txt`), 'A\nmiddle\nB');
  assert.equal(fs.existsSync(store.getTask('thread_merge_B')!.worktree!.validation!.path), false);
});

test('a dirty checked-out delivery target restores staged, unstaged, deleted and untracked files', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-dirty-delivery-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_dirty_delivery', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A\nmiddle\nbeta\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved change');
  fs.writeFileSync(path.join(repository, 'staged.txt'), 'staged\n'); git(repository, 'add', 'staged.txt');
  fs.appendFileSync(path.join(repository, 'staged.txt'), 'unstaged\n');
  fs.unlinkSync(path.join(repository, 'validate.js'));
  const userFile = path.join(repository, 'keep-me.txt'); fs.writeFileSync(userFile, 'uncommitted user data\n');
  const statusBefore = git(repository, 'status', '--porcelain');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_dirty_delivery', worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_dirty_delivery');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'status', '--porcelain'), statusBefore);
  assert.equal(git(repository, 'show', ':staged.txt'), 'staged');
  assert.equal(fs.readFileSync(path.join(repository, 'staged.txt'), 'utf8').replace(/\r\n/g, '\n'), 'staged\nunstaged\n');
  assert.equal(git(repository, 'ls-tree', '--name-only', result.commit!, 'staged.txt', 'keep-me.txt'), '');
  assert.equal(git(repository, 'ls-tree', '--name-only', result.commit!, 'validate.js'), 'validate.js');
  assert.equal(fs.readFileSync(userFile, 'utf8'), 'uncommitted user data\n');
  const saved = store.getTask('thread_dirty_delivery')!;
  assert.equal(saved.status, 'completed');
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_restored');
  assert.equal(git(repository, 'stash', 'list'), '', 'Successful landing must remove its own temporary stash');
  assert.equal(saved.worktree!.validation!.status, 'cleaned');
  assert.ok(saved.worktree!.validation!.commit);
  assert.equal(fs.existsSync(saved.worktree!.validation!.path), false);
});

test('stash restoration conflict leaves the merge completed and preserves both stashes', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-conflict-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_conflict', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old stash\n'); git(repository, 'stash', 'push', '-u', '-m', 'existing');
  const oldStash = git(repository, 'rev-parse', 'refs/stash');
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'user change\nmiddle\nbeta\n');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_conflict', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_stash_conflict');
  assert.equal(result.status, 'merged', result.message);
  const saved = store.getTask('thread_stash_conflict')!;
  assert.equal(saved.status, 'completed');
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_restore_conflict');
  assert.equal(saved.finalSummary!.gitDelivery!.stashRestore!.status, 'stash_restore_conflict');
  const stashes = git(repository, 'stash', 'list', '--format=%H');
  assert.ok(stashes.includes(oldStash));
  assert.ok(stashes.includes(saved.worktree!.integration!.stashRestore!.sha!));
  assert.equal(git(repository, 'show', `${result.commit}:shared.txt`), 'A task\nmiddle\nbeta');
});

test('target advancement before preflight restarts merge and validation', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-preflight-advance-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_preflight_advance', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'task');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_preflight_advance', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  let wakes = 0; const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')), () => { wakes++; });
  const sync = (coordinator as any).syncCheckedOutTarget.bind(coordinator); let advanced = false;
  (coordinator as any).syncCheckedOutTarget = async (...args: any[]) => {
    if (args[3] && !advanced) { advanced = true; fs.writeFileSync(path.join(repository, 'advance.txt'), 'advance'); git(repository, 'add', 'advance.txt'); git(repository, 'commit', '-m', 'advance'); }
    return sync(...args);
  };
  const result = await coordinator.mergeApproved('thread_preflight_advance');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'show', `${result.commit}:advance.txt`), 'advance');
  assert.equal(wakes, 0);
  assert.ok(store.getTask('thread_preflight_advance')!.mergeHistory!.filter((e) => e.phase === 'validation' && e.result === 'passed').length >= 2);
});

test('landing restores its own stash when another stash is added after it', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-order-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_order', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const oldSha = git(repository, 'rev-parse', 'refs/stash');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_order', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const original = (coordinator as any).loggedGit.bind(coordinator); let concurrentSha = '';
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    const result = await original(id, cwd, args);
    if (args.includes('push') && args.includes('--include-untracked')) {
      fs.writeFileSync(path.join(repository, 'other.txt'), 'other'); git(repository, 'stash', 'push', '-u', '-m', 'concurrent');
      concurrentSha = git(repository, 'rev-parse', 'refs/stash');
    }
    return result;
  };
  const result = await coordinator.mergeApproved('thread_stash_order');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  const stashes = git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/);
  assert.deepEqual(stashes, [concurrentSha, oldSha]);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), concurrentSha);
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H%x09%gs').split(/\r?\n/), [
    `${concurrentSha}\tOn feature/caller: concurrent`, `${oldSha}\tOn feature/caller: old`,
  ]);
});

test('sole stash cleanup verifies its identity while Git holds the ref lock', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-clean-lock-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_clean_lock', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_clean_lock', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const find = (coordinator as any).findLandingStash.bind(coordinator); let attempted = false;
  (coordinator as any).findLandingStash = (cwd: string, token: string) => {
    if (!attempted && fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock'))) {
      attempted = true;
      const sha = git(repository, 'rev-parse', 'refs/stash');
      const raced = spawnSync(gitExe, ['stash', 'store', '-m', 'concurrent', sha], { cwd: repository, encoding: 'utf8', windowsHide: true });
      assert.notEqual(raced.status, 0);
    }
    return find(cwd, token);
  };
  const result = await coordinator.mergeApproved('thread_stash_clean_lock');
  assert.equal(result.status, 'merged', result.message); assert.equal(attempted, true);
  assert.equal(git(repository, 'stash', 'list'), '');
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('cleanup failure retains the exact stash and does not fail a landed merge', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-clean-failure-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_clean_failure', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_clean_failure', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  (coordinator as any).removeSoleLandingStash = async () => { throw new Error('injected cleanup failure'); };
  const result = await coordinator.mergeApproved('thread_stash_clean_failure');
  assert.equal(result.status, 'merged', result.message);
  const saved = store.getTask('thread_stash_clean_failure')!;
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_restored_retained');
  assert.match(saved.worktree!.integration!.stashRestore!.error!, /injected cleanup failure/);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), saved.worktree!.integration!.stashRestore!.sha);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('stash cleanup never deletes a branch if refs/stash becomes symbolic', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-symref-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_symref', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_symref', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const cleanup = (coordinator as any).removeSoleLandingStash.bind(coordinator); let sha = '';
  (coordinator as any).removeSoleLandingStash = async (...args: any[]) => {
    sha = args[2]; git(repository, 'update-ref', 'refs/heads/decoy', sha); git(repository, 'symbolic-ref', 'refs/stash', 'refs/heads/decoy');
    return cleanup(...args);
  };
  const result = await coordinator.mergeApproved('thread_stash_symref');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'rev-parse', 'refs/heads/decoy'), sha);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('stash identity drift before cleanup preserves the new entry', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-clean-drift-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_clean_drift', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'other.txt'), 'other'); git(repository, 'stash', 'push', '-u', '-m', 'other');
  const other = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_clean_drift', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const cleanup = (coordinator as any).removeSoleLandingStash.bind(coordinator);
  (coordinator as any).removeSoleLandingStash = async (...args: any[]) => { git(repository, 'stash', 'store', '-m', 'racing stash', other); return cleanup(...args); };
  const result = await coordinator.mergeApproved('thread_stash_clean_drift');
  assert.equal(result.status, 'merged', result.message);
  const saved = store.getTask('thread_stash_clean_drift')!;
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_restored');
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), [other]);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('landing removes its top stash while preserving an older stash', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-old-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_old', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_old', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_stash_old');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), old);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('a held Git stash ref lock prevents cleanup without deleting any entry', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-lock-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_lock_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_lock_fail', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const lockPath = path.join(repository, '.git', 'refs', 'stash.lock');
  (coordinator as any).removeSoleLandingStash = async () => { fs.writeFileSync(lockPath, '', { flag: 'wx' }); return false; };
  const result = await coordinator.mergeApproved('thread_stash_lock_fail');
  fs.unlinkSync(lockPath);
  assert.equal(result.status, 'merged', result.message);
  const saved = store.getTask('thread_stash_lock_fail')!;
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_restored_retained');
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), [saved.worktree!.integration!.stashRestore!.sha, old]);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('a failed reflog publication preserves all stash entries and reports the cleanup failure', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-log-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_log_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_log_fail', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  (coordinator as any).writeLockedGitFile = () => { throw new Error('injected reflog publication failure'); };
  const result = await coordinator.mergeApproved('thread_stash_log_fail');
  assert.equal(result.status, 'merged', result.message);
  const saved = store.getTask('thread_stash_log_fail')!;
  assert.equal(saved.worktree!.integration!.stashRestore!.status, 'stash_cleanup_uncertain');
  assert.match(saved.worktree!.integration!.stashRestore!.error!, /injected reflog publication failure/);
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), [saved.worktree!.integration!.stashRestore!.sha, old]);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

for (const failRollback of [false, true]) test(`ref publication failure preserves recoverable stash metadata, rollback failure=${failRollback}`, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-ref-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const id = 'thread_stash_ref_fail'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash');
  fs.writeFileSync(path.join(repository, 'later-stash.txt'), 'later'); git(repository, 'stash', 'push', '-u', '-m', 'later');
  const later = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
  fs.writeFileSync(path.join(repository, 'staged.txt'), 'staged\n'); git(repository, 'add', 'staged.txt');
  fs.appendFileSync(path.join(repository, 'staged.txt'), 'unstaged\n'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const beforeStatus = git(repository, 'status', '--porcelain');
  const store = new TaskStore(storage); store.upsertTask(thread(id, worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const publish = (coordinator as any).writeLockedGitFile.bind(coordinator); let logWrites = 0; let originalLog: Buffer | undefined;
  (coordinator as any).writeLockedGitFile = (file: string, data: Buffer) => {
    if (file.includes(`${path.sep}logs${path.sep}`)) {
      logWrites++; if (logWrites === 1) originalLog = fs.readFileSync(file);
      if (failRollback && logWrites >= 2) throw new Error('injected rollback failure');
      return publish(file, data);
    }
    throw new Error('injected ref publication failure');
  };
  const first = await coordinator.mergeApproved(id);
  assert.equal(first.status, 'merged');
  const landed = git(repository, 'rev-parse', target.targetRef);
  assert.notEqual(landed, target.baseCommit);
  assert.equal(store.getTask(id)!.worktree!.integration!.mergedCommit, landed);
  assert.equal(store.getTask(id)!.worktree!.integration!.stashRecoveryPending, true);
  assert.equal(git(repository, 'status', '--porcelain'), beforeStatus);
  assert.equal(git(repository, 'show', ':staged.txt'), 'staged');
  assert.equal(fs.readFileSync(path.join(repository, 'staged.txt'), 'utf8').replace(/\r\n/g, '\n'), 'staged\nunstaged\n');
  assert.equal(git(repository, 'show', `${old}^3:old.txt`), 'old');
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  const txFile = path.join(storage, 'merge-transactions', `${id}.json`);
  const tx = JSON.parse(fs.readFileSync(txFile, 'utf8'));
  assert.ok(tx.stashCleanup); assert.ok(tx.stashSha);
  if (failRollback) {
    assert.equal(tx.phase, 'stash_cleanup_needs_recovery');
    assert.ok(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')));
    assert.ok(tx.stashCleanupLock);
    const racing = spawnSync(gitExe, ['stash', 'store', '-m', 'racing', later], { cwd: repository, encoding: 'utf8', windowsHide: true });
    assert.notEqual(racing.status, 0);
    const control = new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(path.join(root, 'merge.lock')));
    await assert.rejects(() => control.deleteTask(id, 'delete', 'test', true), /stash metadata recovery/i);
  } else {
    assert.equal(tx.phase, 'stash_cleanup_rolled_back');
    assert.equal(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')), false);
    assert.deepEqual(fs.readFileSync(path.join(repository, '.git', 'logs', 'refs', 'stash')), originalLog);
    const list = git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/);
    assert.deepEqual(list, [tx.stashSha, old]);
    assert.equal(git(repository, 'rev-parse', 'refs/stash'), list[0]);
  }
  (coordinator as any).writeLockedGitFile = (file: string, data: Buffer) => {
    if (failRollback) assert.ok(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')));
    return publish(file, data);
  };
  if (!failRollback) git(repository, 'stash', 'store', '-m', 'later', later);
  fs.writeFileSync(path.join(repository, 'after-failure.txt'), 'new user edit');
  const recoveryStatus = git(repository, 'status', '--porcelain');
  if (failRollback) {
    const script = `const fs=require('fs'),path=require('path'),{TaskStore}=require(process.argv[1]),{MergeCoordinator,MeshFileLock}=require(process.argv[2]);
      const s=new TaskStore(process.argv[3]), c=new MergeCoordinator(s,process.argv[3],new MeshFileLock(process.argv[4]));
      const publish=c.writeLockedGitFile.bind(c);c.writeLockedGitFile=(file,data)=>{if(!fs.existsSync(path.join(process.argv[5],'.git','refs','stash.lock')))throw Error('recovery lost the stash lock');return publish(file,data)};
      c.mergeApproved('thread_stash_ref_fail').then(r=>{if(r.status!=='merged')throw Error(r.message);s.dispose()}).catch(e=>{console.error(e);process.exitCode=1});`;
    const child = spawnSync(process.execPath, ['-e', script, require.resolve('../bridge/task-store'), require.resolve('../bridge/merge-coordinator'), storage, path.join(root, 'merge.lock'), repository], { encoding: 'utf8', timeout: 60000, windowsHide: true });
    assert.equal(child.status, 0, child.stderr); store.loadFromDisk();
    assert.equal(store.getTask(id)!.status, 'completed');
  } else {
    const recovered = await coordinator.mergeApproved(id);
    assert.equal(recovered.status, 'merged', recovered.message);
  }
  assert.equal(git(repository, 'rev-parse', target.targetRef), landed);
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), failRollback ? [old] : [later, old]);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), failRollback ? old : later);
  assert.equal(git(repository, 'status', '--porcelain'), recoveryStatus);
  assert.equal(git(repository, 'show', ':staged.txt'), 'staged');
  assert.equal(fs.readFileSync(path.join(repository, 'staged.txt'), 'utf8').replace(/\r\n/g, '\n'), 'staged\nunstaged\n');
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')), false);
});

test('a retained inconsistent stash lock survives process exit and is adopted by the same transaction', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-protected-restart-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const id = 'thread_stash_protected_restart'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const initial = new TaskStore(storage); initial.upsertTask(thread(id, worktree)); initial.dispose();
  t.after(() => { try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const script = `const path=require('path'),{TaskStore}=require(process.argv[1]),{MergeCoordinator,MeshFileLock}=require(process.argv[2]);
    const s=new TaskStore(process.argv[3]), c=new MergeCoordinator(s,process.argv[3],new MeshFileLock(process.argv[4]));
    const publish=c.writeLockedGitFile.bind(c);let logs=0;c.writeLockedGitFile=(file,data)=>{if(file.includes(path.sep+'logs'+path.sep)){if(++logs>1)throw Error('rollback failure');return publish(file,data)}throw Error('ref failure')};
    c.mergeApproved('thread_stash_protected_restart').then(r=>process.exit(r.status==='merged'?71:2)).catch(e=>{console.error(e);process.exit(3)});`;
  const child = spawnSync(process.execPath, ['-e', script, require.resolve('../bridge/task-store'), require.resolve('../bridge/merge-coordinator'), storage, path.join(root, 'merge.lock')], { encoding: 'utf8', timeout: 60000, windowsHide: true });
  assert.equal(child.status, 71, child.stderr);
  const lockPath = path.join(repository, '.git', 'refs', 'stash.lock'); assert.ok(fs.existsSync(lockPath));
  const txFile = path.join(storage, 'merge-transactions', `${id}.json`);
  const tx = JSON.parse(fs.readFileSync(txFile, 'utf8')); assert.equal(tx.phase, 'stash_cleanup_needs_recovery'); assert.ok(tx.stashCleanupLock);
  const landed = git(repository, 'rev-parse', target.targetRef);
  fs.writeFileSync(path.join(repository, 'after-failure.txt'), 'new user edit');
  const recoveryStatus = git(repository, 'status', '--porcelain');
  const store = new TaskStore(storage); t.after(() => store.dispose());
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved(id);
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'rev-parse', target.targetRef), landed);
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
  assert.equal(git(repository, 'status', '--porcelain'), recoveryStatus);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(fs.existsSync(lockPath), false);
});

test('an error after ref publication recognizes the already consistent stash state', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-ref-after-write-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const id = 'thread_stash_ref_after_write'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread(id, worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const publish = (coordinator as any).writeLockedGitFile.bind(coordinator); let injected = false;
  (coordinator as any).writeLockedGitFile = (file: string, data: Buffer) => {
    publish(file, data);
    if (!injected && !file.includes(`${path.sep}logs${path.sep}`)) { injected = true; throw new Error('injected after ref publication'); }
  };
  const result = await coordinator.mergeApproved(id);
  assert.equal(result.status, 'merged', result.message); assert.equal(injected, true);
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), old);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(store.getTask(id)!.worktree!.integration!.stashRestore!.status, 'stash_restored');
});

test('pausing during multi-stash publication preserves the ref lock until cleanup is consistent', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-pause-publish-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_pause_publish', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_pause_publish', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const publish = (coordinator as any).writeLockedGitFile.bind(coordinator); let paused = false;
  (coordinator as any).writeLockedGitFile = (file: string, data: Buffer) => {
    publish(file, data);
    if (!paused && file.includes(`${path.sep}logs${path.sep}`)) {
      paused = true; const task = structuredClone(store.getTask('thread_stash_pause_publish')!); task.status = 'paused'; store.upsertTask(task);
      assert.ok(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')));
    }
  };
  const result = await coordinator.mergeApproved('thread_stash_pause_publish');
  assert.equal(result.status, 'merged', result.message); assert.equal(paused, true);
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
  assert.equal(git(repository, 'rev-parse', 'refs/stash'), old);
  assert.equal(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')), false);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('a transient failure releasing the stash lock retains enough evidence to recover', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-unlock-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_unlock_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_unlock_fail', worktree));
  const mutableFs = createRequire(__filename)('fs') as { unlinkSync: typeof fs.unlinkSync };
  const unlink = mutableFs.unlinkSync; const lockPath = path.join(repository, '.git', 'refs', 'stash.lock'); let failed = false;
  t.after(() => { mutableFs.unlinkSync = unlink; store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  mutableFs.unlinkSync = ((file: fs.PathLike) => {
    const normalize = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
    if (!failed && normalize(String(file)) === normalize(lockPath)) { failed = true; throw new Error('injected lock release failure'); }
    return unlink(file);
  }) as typeof fs.unlinkSync;
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_stash_unlock_fail');
  assert.equal(result.status, 'merged', result.message); assert.equal(failed, true);
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
  assert.equal(fs.existsSync(lockPath), false);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('stash cleanup retries identification when the ref moves before its lock is acquired', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-lock-drift-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_lock_drift', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
  const old = git(repository, 'rev-parse', 'refs/stash');
  fs.writeFileSync(path.join(repository, 'extra.txt'), 'extra'); git(repository, 'stash', 'push', '-u', '-m', 'extra');
  const extra = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_lock_drift', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const coordinator = new MergeCoordinator(store, storage, lock); const multi = (coordinator as any).removeMultipleLandingStash.bind(coordinator);
  let moved = false;
  (coordinator as any).removeMultipleLandingStash = async (...args: any[]) => {
    if (!moved) {
      moved = true;
      const head = git(repository, 'rev-parse', 'refs/stash');
      const racer = spawn(gitExe, ['update-ref', '-m', 'late', '--stdin'], { cwd: repository, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
      let output = ''; let errors = '';
      const ready = new Promise<void>((resolve, reject) => {
        racer.stdout!.on('data', (chunk: Buffer) => { output += chunk.toString(); if (output.includes('prepare: ok')) resolve(); });
        racer.stderr!.on('data', (chunk: Buffer) => { errors += chunk.toString(); });
        racer.once('error', reject); racer.once('close', (code) => { if (!output.includes('prepare: ok')) reject(new Error(`Racing ref transaction failed (${code}): ${errors}`)); });
      });
      racer.stdin!.write(`start\nupdate refs/stash ${extra} ${head}\nprepare\n`);
      await ready;
      setTimeout(() => racer.stdin!.end('commit\n'), 500);
    }
    return multi(...args);
  };
  const result = await coordinator.mergeApproved('thread_stash_lock_drift');
  assert.equal(result.status, 'merged', result.message); assert.equal(moved, true);
  assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), [extra, old]);
});

for (const phase of ['stash_cleanup_started', 'stash_cleanup_prepared', 'stash_reflog_published', 'stash_ref_published', 'before_stash_reflog_published', 'before_stash_ref_published', 'hard_stash_cleanup_prepared', 'hard_stash_reflog_published', 'hard_stash_ref_published']) {
  for (const addConcurrent of phase.startsWith('hard_') ? [false] : [false, true]) test(`multi-stash cleanup recovers ${phase}, concurrent=${addConcurrent}`, async (t) => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-multi-crash-'));
    const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
    const id = 'thread_stash_multi_crash'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
    fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
    fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old');
    const old = git(repository, 'rev-parse', 'refs/stash');
    fs.writeFileSync(path.join(repository, 'extra.txt'), 'extra'); git(repository, 'stash', 'push', '-u', '-m', 'extra');
    const extra = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
    fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
    const store = new TaskStore(storage); store.upsertTask(thread(id, worktree));
    t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
    const script = `const {TaskStore}=require(process.argv[1]),{MergeCoordinator,MeshFileLock}=require(process.argv[2]);
      const s=new TaskStore(process.argv[3]), c=new MergeCoordinator(s,process.argv[3],new MeshFileLock(process.argv[4]));
      const write=c.writeTransaction.bind(c);c.writeTransaction=(file,tx)=>{const p=process.argv[5];if(p.startsWith('before_')&&tx.phase===p.slice(7))process.exit(71);write(file,tx);if(p.startsWith('hard_')&&tx.phase===p.slice(5))process.kill(process.pid,'SIGKILL');if(tx.phase===p)process.exit(71)};
      c.mergeApproved('thread_stash_multi_crash').catch(e=>{console.error(e);process.exit(2)});`;
    const child = spawnSync(process.execPath, ['-e', script, require.resolve('../bridge/task-store'), require.resolve('../bridge/merge-coordinator'), storage, path.join(root, 'merge.lock'), phase], { encoding: 'utf8', timeout: 60000, windowsHide: true });
    if (phase.startsWith('hard_')) assert.notEqual(child.status, 0, child.stderr); else assert.equal(child.status, 71, child.stderr);
    const landed = git(repository, 'rev-parse', target.targetRef);
    if (addConcurrent) {
      let stored = false;
      for (let n = 0; n < 100 && !stored; n++) { try { git(repository, 'stash', 'store', '-m', 'late', extra); stored = true; } catch { await new Promise((resolve) => setTimeout(resolve, 50)); } }
      assert.equal(stored, true);
    }
    const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved(id);
    assert.equal(result.status, 'merged', result.message);
    assert.equal(git(repository, 'rev-parse', target.targetRef), landed);
    assert.deepEqual(git(repository, 'stash', 'list', '--format=%H').split(/\r?\n/), addConcurrent ? [extra, old] : [old]);
    assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
    assert.equal(store.getTask(id)!.worktree!.integration!.stashRestore!.status, 'stash_restored');
    assert.equal(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')), false);
    const markers = path.join(repository, '.git', 'agent-mesh-bridge', 'stash-locks');
    if (fs.existsSync(markers)) assert.deepEqual(fs.readdirSync(markers), []);
  });
}

test('stash creation failure never advances the target or discards local files', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-failure-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_failure', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine'); const before = git(repository, 'rev-parse', target.targetRef);
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_failure', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const original = (coordinator as any).loggedGit.bind(coordinator);
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    if (args.includes('push') && args.includes('--include-untracked')) throw new Error('injected stash failure');
    return original(id, cwd, args);
  };
  assert.equal((await coordinator.mergeApproved('thread_stash_failure')).status, 'blocked');
  assert.equal(git(repository, 'rev-parse', target.targetRef), before);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('an unconfirmed new stash is restored without advancing the target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-unconfirmed-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_unconfirmed', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine'); const before = git(repository, 'rev-parse', target.targetRef);
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_unconfirmed', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const find = (coordinator as any).findLandingStash.bind(coordinator); let first = true;
  (coordinator as any).findLandingStash = (...args: any[]) => { if (first) { first = false; return []; } return find(...args); };
  assert.equal((await coordinator.mergeApproved('thread_stash_unconfirmed')).status, 'blocked');
  assert.equal(git(repository, 'rev-parse', target.targetRef), before);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
});

test('ignored checkout file that blocks synchronization is preserved before ref update', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-ignored-block-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  fs.writeFileSync(path.join(repository, '.gitignore'), 'ignored.txt\n'); git(repository, 'add', '.gitignore'); git(repository, 'commit', '-m', 'ignore');
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_ignored_block', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n');
  fs.writeFileSync(path.join(worktree.path, 'ignored.txt'), 'task file'); git(worktree.path, 'add', '-f', 'ignored.txt');
  worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'ignored.txt'), 'user ignored file');
  const before = git(repository, 'rev-parse', target.targetRef);
  const store = new TaskStore(storage); store.upsertTask(thread('thread_ignored_block', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_ignored_block');
  assert.equal(result.status, 'blocked');
  assert.match(result.message, /ignored\.txt/);
  assert.equal(git(repository, 'rev-parse', target.targetRef), before);
  assert.equal(fs.readFileSync(path.join(repository, 'ignored.txt'), 'utf8'), 'user ignored file');
});

test('pause immediately after ref update finishes the existing landing and restores the stash', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-post-ref-pause-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_post_ref_pause', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_post_ref_pause', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const original = (coordinator as any).loggedGit.bind(coordinator); let landed = '';
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    const result = await original(id, cwd, args);
    if (args[0] === 'update-ref' && args[1] === target.targetRef) {
      landed = args[2]; const paused = structuredClone(store.getTask(id)!); paused.status = 'paused'; store.upsertTask(paused);
    }
    return result;
  };
  const result = await coordinator.mergeApproved('thread_post_ref_pause');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'rev-parse', target.targetRef), landed);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(store.getTask('thread_post_ref_pause')!.status, 'completed');
});

for (const existingStash of [false, true]) test(`pause after stashing but before ref update restores local changes, existing=${existingStash}`, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-pre-ref-pause-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_pre_ref_pause', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  let old = '';
  if (existingStash) { fs.writeFileSync(path.join(repository, 'old.txt'), 'old'); git(repository, 'stash', 'push', '-u', '-m', 'old'); old = git(repository, 'rev-parse', 'refs/stash'); }
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine'); const before = git(repository, 'rev-parse', target.targetRef);
  const store = new TaskStore(storage); store.upsertTask(thread('thread_pre_ref_pause', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const original = (coordinator as any).loggedGit.bind(coordinator);
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    const result = await original(id, cwd, args);
    if (args[0] === 'read-tree' && args.includes('-n')) { const paused = structuredClone(store.getTask(id)!); paused.status = 'paused'; store.upsertTask(paused); }
    return result;
  };
  assert.equal((await coordinator.mergeApproved('thread_pre_ref_pause')).status, 'stale');
  assert.equal(git(repository, 'rev-parse', target.targetRef), before);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(git(repository, 'stash', 'list', '--format=%H'), old);
});

test('an uncertain post-apply transaction preserves the checkout without applying the stash twice', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-pop-window-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_pop_window', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_pop_window', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const write = (coordinator as any).writeTransaction.bind(coordinator); let injected = false;
  (coordinator as any).writeTransaction = (file: string, tx: any) => {
    if (tx.phase === 'stash_applied' && !injected) { injected = true; throw new Error('injected post-apply persistence failure'); }
    return write(file, tx);
  };
  const result = await coordinator.mergeApproved('thread_pop_window');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  assert.equal(store.getTask('thread_pop_window')!.worktree!.integration!.stashRestore!.status, 'stash_restore_conflict');
  assert.match(git(repository, 'stash', 'list'), /mesh-landing-thread_pop_window/);
});

for (const crashPhase of ['stash_created', 'ref_updated', 'checkout_synced', 'before_stash_applied', 'stash_applied', 'after_reflog_deleted', 'before_stash_restored', 'before_stash_restored_concurrent']) test(`a process crash after ${crashPhase} recovers local files and landing`, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-ref-crash-stash-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_ref_crash_stash', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  let concurrentStash = '';
  if (crashPhase === 'before_stash_restored_concurrent') {
    fs.writeFileSync(path.join(repository, 'other.txt'), 'other'); git(repository, 'stash', 'push', '-u', '-m', 'other');
    concurrentStash = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
  }
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_ref_crash_stash', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const script = `const {TaskStore}=require(process.argv[1]),{MergeCoordinator,MeshFileLock}=require(process.argv[2]);
    const s=new TaskStore(process.argv[3]), c=new MergeCoordinator(s,process.argv[3],new MeshFileLock(process.argv[4]));
    if(process.argv[5]==='after_reflog_deleted')c.removeSoleLandingStash=async(id,cwd)=>{require('fs').unlinkSync(require('path').join(cwd,'.git','logs','refs','stash'));process.exit(71)};
    const write=c.writeTransaction.bind(c);c.writeTransaction=(file,tx)=>{if((process.argv[5].startsWith('before_stash_restored')&&tx.phase==='stash_restored')||(process.argv[5]==='before_stash_applied'&&tx.phase==='stash_applied'))process.exit(71);write(file,tx);if(tx.phase===process.argv[5])process.exit(71)};
    c.mergeApproved('thread_ref_crash_stash').catch(e=>{console.error(e);process.exit(2)});`;
  const child = spawnSync(process.execPath, ['-e', script, require.resolve('../bridge/task-store'), require.resolve('../bridge/merge-coordinator'), storage, path.join(root, 'merge.lock'), crashPhase], { encoding: 'utf8', timeout: 60000, windowsHide: true });
  assert.equal(child.status, 71, child.stderr);
  const landed = git(repository, 'rev-parse', target.targetRef);
  if (concurrentStash) git(repository, 'stash', 'store', '-m', 'concurrent', concurrentStash);
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const result = await coordinator.mergeApproved('thread_ref_crash_stash');
  assert.equal(result.status, 'merged', result.message);
  if (crashPhase !== 'stash_created') assert.equal(git(repository, 'rev-parse', target.targetRef), landed);
  else assert.notEqual(git(repository, 'rev-parse', target.targetRef), landed);
  assert.equal(fs.readFileSync(path.join(repository, 'mine.txt'), 'utf8'), 'mine');
  if (crashPhase === 'before_stash_applied') {
    assert.equal(store.getTask('thread_ref_crash_stash')!.worktree!.integration!.stashRestore!.status, 'stash_restore_conflict');
    assert.match(git(repository, 'stash', 'list'), /mesh-landing-thread_ref_crash_stash/);
  } else if (concurrentStash) {
    assert.equal(store.getTask('thread_ref_crash_stash')!.worktree!.integration!.stashRestore!.status, 'stash_cleanup_uncertain');
    assert.equal(git(repository, 'rev-parse', 'refs/stash'), concurrentStash);
  } else {
    assert.equal(store.getTask('thread_ref_crash_stash')!.worktree!.integration!.stashRestore!.status, 'stash_restored');
    assert.equal(git(repository, 'stash', 'list'), '');
    assert.equal(git(repository, 'for-each-ref', '--format=%(objectname)', 'refs/stash'), '');
  }
});

test('delivery remains pinned after the user checks out a different branch', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-pinned-target-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  git(repository, 'branch', 'other');
  const target = resolveGitRepository(repository, storage, 'feature/caller'); const base = target.baseCommit;
  const worktree = createTaskWorktree(target, 'thread_pinned_target', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A\nmiddle\nbeta\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved pinned change');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_pinned_target', worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  git(repository, 'checkout', 'other');
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_pinned_target');
  assert.equal(result.status, 'merged');
  assert.equal(git(repository, 'rev-parse', 'feature/caller'), result.commit);
  assert.equal(git(repository, 'rev-parse', 'other'), base);
  assert.equal(store.getTask('thread_pinned_target')!.worktree!.integration!.targetPath, undefined);
  assert.equal(store.getTask('thread_pinned_target')!.worktree!.deliveryTarget!.checkoutPath, undefined);
});

test('an unchecked delivery target that is externally rewound after landing stays blocked', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-unchecked-target-rewind-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const base = git(repository, 'rev-parse', 'HEAD');
  git(repository, 'branch', 'master', base); git(repository, 'checkout', '-b', 'user-branch');
  const target = resolveGitRepository(repository, storage, 'master');
  const worktree = createTaskWorktree(target, 'thread_unchecked_target_rewind', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task change\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved task change');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_unchecked_target_rewind', worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const sync = (coordinator as unknown as { syncCheckedOutTarget: (repo: string, ref: string, commit: string, preflight: boolean, previous?: string) => { ok: boolean; error: string; path?: string; commit?: string } }).syncCheckedOutTarget;
  let rewound = false;
  (coordinator as unknown as { syncCheckedOutTarget: typeof sync }).syncCheckedOutTarget = function (repo, ref, commit, preflight, previous) {
    if (!preflight && !rewound) { git(repo, 'update-ref', ref, base, commit); rewound = true; }
    return sync.call(this, repo, ref, commit, preflight, previous);
  };

  const result = await coordinator.mergeApproved('thread_unchecked_target_rewind');
  assert.equal(rewound, true);
  assert.equal(result.status, 'blocked');
  assert.match(result.message, /unrelated commit/i);
  assert.equal(git(repository, 'rev-parse', 'refs/heads/master'), base);
  assert.equal(git(repository, 'show', 'refs/heads/master:shared.txt'), 'alpha\nmiddle\nbeta');
  const saved = store.getTask('thread_unchecked_target_rewind')!;
  assert.equal(saved.status, 'waiting_merge');
  assert.equal(saved.worktree!.integration!.phase, 'blocked');
  assert.notEqual(saved.status, 'completed');
});

test('legacy records without deliveryTarget keep their recorded targetRef semantics', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-legacy-target-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const base = git(repository, 'rev-parse', 'HEAD'); const legacyRef = 'refs/heads/mesh/integration/legacy';
  git(repository, 'update-ref', legacyRef, base);
  const worktree = createTaskWorktree(resolveGitRepository(repository, storage), 'thread_legacy_target', storage);
  worktree.targetRef = legacyRef; worktree.deliveryTarget = undefined;
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A legacy result\nmiddle\nbeta\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved legacy change');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_legacy_target', worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_legacy_target');
  assert.equal(result.status, 'merged');
  assert.equal(git(repository, 'rev-parse', legacyRef), result.commit);
  assert.equal(git(repository, 'rev-parse', 'feature/caller'), base);
});

test('the mesh lock serializes processes, preserves a live owner, and recovers after a process crash', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-file-lock-'));
  const lockPath = path.join(root, 'merge.lock');
  const acquired = path.join(root, 'child-acquired');
  const stop = path.join(root, 'stop-child');
  const lock = new MeshFileLock(lockPath);
  const release = await lock.acquire();
  assert.ok(release);
  const script = `const fs=require('fs');const {MeshFileLock}=require(process.argv[1]);new MeshFileLock(process.argv[2]).acquire().then(release=>{fs.writeFileSync(process.argv[3],'yes');setInterval(()=>{if(fs.existsSync(process.argv[4])){release();process.exit(0)}},20)})`;
  const child = spawn(process.execPath, ['-e', script, path.resolve(__dirname, '../bridge/merge-coordinator.js'), lockPath, acquired, stop], { stdio: 'ignore' });
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.strictEqual(fs.existsSync(acquired), false, 'the second process waits while the first owns the lock');
  release();
  await waitFor(acquired);
  assert.strictEqual(JSON.parse(fs.readFileSync(`${lockPath}.owner.json`, 'utf8')).pid, child.pid);
  child.kill();
  await new Promise<void>((resolve) => child.once('exit', () => resolve()));
  const recoveredRelease = await new MeshFileLock(lockPath).acquire();
  assert.ok(recoveredRelease, 'a dead owner is safely reclaimed');
  recoveredRelease();
  fs.rmSync(root, { recursive: true, force: true });
});

test('three processes contend after an owner crash without overlapping lock ownership', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-contenders-'));
  const lockPath = path.join(root, 'merge.lock');
  const ownerReady = path.join(root, 'owner-ready');
  const childPidFile = path.join(root, 'protected-child.pid');
  const active = path.join(root, 'critical-section');
  const violation = path.join(root, 'overlap');
  const doneA = path.join(root, 'done-a');
  const doneB = path.join(root, 'done-b');
  let ownerProcess: ReturnType<typeof spawn> | undefined;
  const contenders: ReturnType<typeof spawn>[] = [];
  t.after(async () => {
    for (const child of [ownerProcess, ...contenders]) if (child && child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await Promise.all([ownerProcess, ...contenders].filter((child): child is ReturnType<typeof spawn> => !!child).map(waitForExit));
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
  });
  const ownerCode = `const fs=require('fs'),cp=require('child_process'),{MeshFileLock}=require(process.argv[1]);const lock=new MeshFileLock(process.argv[2]);lock.acquire().then(async()=>{const child=cp.spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'});await lock.attachChildPid(child.pid);fs.writeFileSync(process.argv[3],'1');fs.writeFileSync(process.argv[4],String(child.pid));setInterval(()=>{},1000)})`;
  const contenderCode = `const fs=require('fs'),{MeshFileLock}=require(process.argv[1]);(async()=>{for(let i=0;i<3;i++){const release=await new MeshFileLock(process.argv[2]).acquire();try{try{fs.writeFileSync(process.argv[3],'1',{flag:'wx'})}catch{fs.writeFileSync(process.argv[4],'overlap')}await new Promise(r=>setTimeout(r,50));try{fs.unlinkSync(process.argv[3])}catch{}}finally{release()}}fs.writeFileSync(process.argv[5],'done')})().catch(e=>{console.error(e);process.exit(1)})`;
  const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
  const owner = ownerProcess = spawn(process.execPath, ['-e', ownerCode, helper, lockPath, ownerReady, childPidFile], { stdio: 'ignore' });
  await waitFor(ownerReady);
  const a = spawn(process.execPath, ['-e', contenderCode, helper, lockPath, active, violation, doneA], { stdio: 'ignore' });
  const b = spawn(process.execPath, ['-e', contenderCode, helper, lockPath, active, violation, doneB], { stdio: 'ignore' });
  contenders.push(a, b);
  await new Promise((resolve) => setTimeout(resolve, 100));
  owner.kill();
  await new Promise<void>((resolve) => owner.once('exit', () => resolve()));
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(fs.existsSync(doneA) || fs.existsSync(doneB), false, 'the surviving child keeps the OS lock after its parent crashes');
  try { process.kill(Number(fs.readFileSync(childPidFile, 'utf8')), 'SIGKILL'); } catch {}
  await Promise.all([waitFor(doneA, 2_000), waitFor(doneB, 2_000)]);
  a.kill('SIGKILL'); b.kill('SIGKILL');
  await Promise.all([waitForExit(a), waitForExit(b)]);
  assert.equal(fs.existsSync(violation), false, 'the OS mutex admits only one successor at a time');
});

test('one lock instance keeps child tracking bound to each acquired lease across overlapping waits', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-reused-instance-'));
  const lockPath = path.join(root, 'merge.lock');
  const aReady = path.join(root, 'a-ready');
  const releaseA = path.join(root, 'release-a');
  const bAcquired = path.join(root, 'b-acquired');
  const cAcquired = path.join(root, 'c-acquired');
  const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
  const keepAlive = () => spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
  const protectedForA = keepAlive();
  const protectedForB = keepAlive();
  const ownerCode = `const fs=require('fs'),{MeshFileLock}=require(process.argv[1]);(async()=>{const lock=new MeshFileLock(process.argv[2]);const release=await lock.acquire();await lock.attachChildPid(Number(process.argv[3]));const waiting=lock.acquire();fs.writeFileSync(process.argv[4],'ready');const poll=setInterval(()=>{if(fs.existsSync(process.argv[5])){clearInterval(poll);release();waiting.then(async()=>{await lock.attachChildPid(Number(process.argv[6]));const owner=JSON.parse(fs.readFileSync(process.argv[2]+'.owner.json','utf8'));fs.writeFileSync(process.argv[7],String(owner.helperPid));process.exit(0)})}},10)})().catch(e=>{console.error(e);process.exit(1)})`;
  const owner = spawn(process.execPath, ['-e', ownerCode, helper, lockPath, String(protectedForA.pid), aReady, releaseA, String(protectedForB.pid), bAcquired], { stdio: ['ignore', 'ignore', 'pipe'] });
  let ownerError = '';
  owner.stderr?.on('data', (chunk: Buffer) => { ownerError += chunk.toString(); });
  const contender: { process?: ReturnType<typeof spawn> } = {};
  t.after(async () => {
    for (const child of [protectedForA, protectedForB, owner, contender.process]) {
      if (child && child.exitCode === null) child.kill('SIGKILL');
    }
    await Promise.all([protectedForA, protectedForB, owner, ...(contender.process ? [contender.process] : [])].map(waitForExit));
    fs.rmSync(root, { recursive: true, force: true });
  });

  try { await waitFor(aReady, 800); } catch (error) { throw new Error(`${String(error)} ${ownerError}`, { cause: error }); }
  assert.equal(fs.existsSync(bAcquired), false, 'the second acquisition must wait while lease A is held');
  fs.writeFileSync(releaseA, 'release');
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(fs.existsSync(bAcquired), false, 'A registered its child on A and its helper retains the mutex');
  protectedForA.kill('SIGKILL');
  await waitForExit(protectedForA);
  await waitFor(bAcquired);
  await waitForExit(owner);
  const helperPid = Number(fs.readFileSync(bAcquired, 'utf8'));
  const pidAlive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };
  assert.equal(owner.exitCode, 0, 'B controller exited after the helper acknowledged CHILD registration');
  assert.equal(pidAlive(protectedForB.pid!), true, 'the protected process must still be alive after B exits');
  assert.equal(pidAlive(helperPid), true, 'the detached lock helper must survive its controller process');

  const contenderStarted = path.join(root, 'c-started');
  const contenderWaiting = path.join(root, 'c-waiting');
  const contenderCode = `const fs=require('fs'),{MeshFileLock}=require(process.argv[1]);let marked=false;new MeshFileLock(process.argv[2]).acquire(()=>{if(!marked){marked=true;fs.writeFileSync(process.argv[3],'started')}fs.writeFileSync(process.argv[4],'waiting')}).then(release=>{fs.writeFileSync(process.argv[5],'acquired');release()}).catch(e=>{console.error(e);process.exit(1)})`;
  contender.process = spawn(process.execPath, ['-e', contenderCode, helper, lockPath, contenderStarted, contenderWaiting, cAcquired], { stdio: 'ignore' });
  await waitFor(contenderWaiting);
  assert.equal(pidAlive(helperPid), true, 'B helper remains alive while the successor is actively waiting');
  assert.equal(pidAlive(protectedForB.pid!), true);
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(fs.existsSync(cAcquired), false, 'the waiting successor cannot acquire while B protected child lives');
  protectedForB.kill('SIGKILL');
  await waitForExit(protectedForB);
  await waitFor(cAcquired, 1200);
  await waitForExit(contender.process);
});

test('immediate and post-spawn lock cancellation settle in an independent Node process', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-immediate-cancel-'));
  try {
    const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
    const script = `const fs=require('fs'),path=require('path'),{MeshFileLock}=require(process.argv[1]);(async()=>{const firstPath=process.argv[2];const first=await new MeshFileLock(firstPath).acquire(undefined,()=>true);if(first!==undefined)throw new Error('immediate cancellation acquired the lock');if(fs.readdirSync(path.dirname(firstPath)).some(name=>name.includes('.mutex-helper.ps1')))throw new Error('immediate cancellation spawned a helper');console.log('CANCEL RESULT:undefined');let checks=0;const second=await new MeshFileLock(process.argv[3]).acquire(undefined,()=>++checks>1);if(second!==undefined)throw new Error('post-spawn cancellation acquired the lock');console.log('RACE CANCEL RESULT:undefined');console.log('PROCESS SURVIVED')})().catch(e=>{console.error(e);process.exitCode=1})`;
    const firstPath = path.join(root, 'immediate.lock');
    const secondPath = path.join(root, 'race.lock');
    const result = spawnSync(process.execPath, ['-e', script, helper, firstPath, secondPath], { encoding: 'utf8', timeout: 10_000, windowsHide: true });
    assert.equal(result.status, 0, result.stderr || result.error?.message);
    assert.match(result.stdout, /CANCEL RESULT:undefined/);
    assert.match(result.stdout, /RACE CANCEL RESULT:undefined/);
    assert.match(result.stdout, /PROCESS SURVIVED/);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('cancelling a started lock waiter settles readiness and lets its Node process exit naturally', { skip: process.platform !== 'win32' }, () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-cancel-ready-cleanup-'));
  try {
    const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
    const script = `const fs=require('fs'),path=require('path'),{Atomics,Int32Array,SharedArrayBuffer}=global,{MeshFileLock}=require(process.argv[1]);const lockPath=process.argv[2],dir=path.dirname(lockPath);(async()=>{const lock=new MeshFileLock(lockPath),release=await lock.acquire();let cancelled=false;const waiting=lock.acquire(undefined,()=>cancelled);const deadline=Date.now()+8000;while(fs.readdirSync(dir).filter(n=>n.endsWith('.helper-pid')).length<2){if(Date.now()>deadline)throw new Error('waiter helper did not start');await new Promise(r=>setTimeout(r,10))}cancelled=true;if(await waiting!==undefined)throw new Error('cancelled waiter acquired the lock');Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,1600);release();console.log('CANCEL SETTLED');console.log('END OF SCRIPT; ACTIVE RESOURCES:'+JSON.stringify(process.getActiveResourcesInfo()))})().catch(e=>{console.error(e);process.exitCode=1})`;
    const result = spawnSync(process.execPath, ['-e', script, helper, path.join(root, 'merge.lock')], { encoding: 'utf8', timeout: 12_000, windowsHide: true });
    assert.equal(result.status, 0, result.error?.message || result.stderr);
    assert.match(result.stdout, /CANCEL SETTLED/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('POSIX waiter may wait longer than the helper startup budget for the global lock', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-posix-lock-long-wait-'));
  try {
    const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
    const perl = process.env.MESH_PERL_PATH || (fs.existsSync('D:/Git/usr/bin/perl.exe') ? 'D:/Git/usr/bin/perl.exe' : 'perl');
    const script = `const path=require('path'),{MeshFileLock}=require(process.argv[1]);Object.defineProperty(process,'platform',{value:'linux'});const pathKey=Object.keys(process.env).find(k=>k.toLowerCase()==='path')||'PATH';process.env[pathKey]=path.dirname(process.argv[3])+path.delimiter+process.env[pathKey];const sleep=ms=>new Promise(r=>setTimeout(r,ms));(async()=>{const lockPath=process.argv[2],first=await new MeshFileLock(lockPath).acquire(),started=Date.now(),waiting=new MeshFileLock(lockPath).acquire();await sleep(15500);first();const second=await waiting;if(!second)throw new Error('waiter cancelled');console.log('POSIX WAIT ACQUIRED AFTER '+(Date.now()-started)+'ms');second()})().catch(e=>{console.error(e);process.exitCode=1})`;
    const result = spawnSync(process.execPath, ['-e', script, helper, path.join(root, 'merge.lock'), perl], { encoding: 'utf8', timeout: 30_000, windowsHide: true, env: { ...process.env, MESH_PERL_PATH: perl } });
    assert.equal(result.status, 0, result.error?.message || result.stderr);
    assert.match(result.stdout, /POSIX WAIT ACQUIRED AFTER 15\d{3}ms/);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('child registration times out, rejects, and removes its pending waiter', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-ack-timeout-'));
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const release = await lock.acquire();
  assert.ok(release);
  const lease = (lock as unknown as { activeLease: { channelPath: string; childAcks: Map<number, unknown> } }).activeLease;
  const channelPath = lease.channelPath;
  const ackPath = `${channelPath}.child.${process.pid}`;
  const mutableFs = createRequire(__filename)('fs') as { existsSync: typeof fs.existsSync };
  const realExistsSync = mutableFs.existsSync;
  mutableFs.existsSync = ((file: fs.PathLike) => String(file) === ackPath ? false : realExistsSync(file)) as typeof fs.existsSync;
  t.after(async () => { mutableFs.existsSync = realExistsSync; lock.detachChildPid(process.pid); release(); fs.rmSync(root, { recursive: true, force: true }); });
  await assert.rejects(lock.attachChildPid(process.pid), /Timed out waiting 10000ms/);
  assert.equal(lease.childAcks.size, 0);
});

test('cancelling a same-instance lock waiter leaves the current owner lease intact', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-cancel-same-instance-'));
  const lockPath = path.join(root, 'merge.lock');
  const acquiredFile = path.join(root, 'successor-acquired');
  const lock = new MeshFileLock(lockPath);
  const release = await lock.acquire();
  assert.ok(release);
  const protectedChild = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
  const successorCode = `const fs=require('fs'),{MeshFileLock}=require(process.argv[1]);new MeshFileLock(process.argv[2]).acquire().then(release=>{fs.writeFileSync(process.argv[3],'yes');release()})`;
  const helper = path.resolve(__dirname, '../bridge/merge-coordinator.js');
  const successor = spawn(process.execPath, ['-e', successorCode, helper, lockPath, acquiredFile], { stdio: 'ignore' });
  let cancelled = false;
  const waiting = lock.acquire(undefined, () => cancelled);
  t.after(async () => {
    if (protectedChild.exitCode === null) protectedChild.kill('SIGKILL');
    if (successor.exitCode === null) successor.kill('SIGKILL');
    await Promise.all([protectedChild, successor].map(waitForExit));
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });

  await new Promise((resolve) => setTimeout(resolve, 150));
  cancelled = true;
  assert.equal(await waiting, undefined, 'the waiting helper is cancelled without acquiring the mutex');
  await lock.attachChildPid(protectedChild.pid);
  release();
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(fs.existsSync(acquiredFile), false, 'cancelling the waiter did not clear the active lease child tracking');
  protectedChild.kill('SIGKILL');
  await waitForExit(protectedChild);
  await waitFor(acquiredFile);
  await waitForExit(successor);
});

test('pause cancels an approved merge while it is waiting for the global lock', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-cancel-lock-wait-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_cancel_lock_wait', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A waiting\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'approved');
  const store = new TaskStore(storage);
  store.upsertTask(thread('thread_cancel_lock_wait', worktree));
  const lockPath = path.join(root, 'merge.lock'); const lockOwner = new MeshFileLock(lockPath); const release = await lockOwner.acquire(); assert.ok(release);
  const helperPid = (lockOwner as unknown as { activeLease: { helperPid: number } }).activeLease.helperPid;
  t.after(async () => { store.dispose(); release(); await waitForProcessExit(helperPid); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(lockPath));
  const pending = coordinator.mergeApproved('thread_cancel_lock_wait');
  await new Promise((resolve) => setTimeout(resolve, 200));
  new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(lockPath)).pauseTask('thread_cancel_lock_wait', 'cancel lock wait', 'test');
  assert.equal((await pending).status, 'stale');
  assert.equal(fs.existsSync(path.join(storage, 'merge-transactions', 'thread_cancel_lock_wait.json')), false);
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  release();
});

test('recovery syncs the actual latest checkout and reports validation cleanup truthfully', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-recovery-latest-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const source = createTaskWorktree(target, 'thread_recovery_latest', storage);
  fs.writeFileSync(path.join(source.path, 'shared.txt'), 'task change\nmiddle\nbeta\n');
  source.headCommit = commitTaskChanges(source, 'approved task change');
  const validationPath = path.join(storage, 'merge-worktrees', 'thread_recovery_latest-crash');
  fs.mkdirSync(path.dirname(validationPath), { recursive: true });
  git(repository, 'worktree', 'add', '--detach', validationPath, target.baseCommit);
  git(validationPath, 'merge', '--no-commit', '--no-ff', source.headCommit!);
  const validationTree = git(validationPath, 'write-tree');
  const candidate = git(repository, '-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit-tree', validationTree, '-p', target.baseCommit, '-p', source.headCommit!, '-m', 'Merge recovery task');
  git(repository, 'update-ref', target.targetRef, candidate, target.baseCommit);
  git(repository, 'reset', '--hard', candidate);
  fs.writeFileSync(path.join(repository, 'later.txt'), 'later external commit\n');
  git(repository, 'add', 'later.txt'); git(repository, 'commit', '-m', 'later target change');
  const latest = git(repository, 'rev-parse', 'HEAD');
  git(repository, 'read-tree', '-u', '--reset', target.baseCommit);
  assert.match(git(repository, 'status', '--porcelain'), /later\.txt/, 'fixture represents a ref update that outran its checkout/index sync');

  const store = new TaskStore(storage); t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  store.upsertTask(thread('thread_recovery_latest', source));
  const transactionRoot = path.join(storage, 'merge-transactions'); fs.mkdirSync(transactionRoot, { recursive: true });
  fs.writeFileSync(path.join(transactionRoot, 'thread_recovery_latest.json'), JSON.stringify({
    threadId: 'thread_recovery_latest', sourceCommit: source.headCommit, targetOldCommit: target.baseCommit,
    phase: 'ref_updated', candidateCommit: candidate, validationPath, validationCommit: candidate, validationResult: 'passed',
  }));
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const removed = (coordinator as any).removeValidationWorktree;
  const result = await coordinator.mergeApproved('thread_recovery_latest');
  assert.equal(result.status, 'merged', result.message);
  assert.equal(git(repository, 'rev-parse', target.targetRef), latest);
  assert.equal(git(repository, 'rev-parse', 'HEAD'), latest);
  assert.equal(fs.readFileSync(path.join(repository, 'later.txt'), 'utf8').replace(/\r\n/g, '\n'), 'later external commit\n');
  assert.equal(fs.readFileSync(path.join(repository, 'shared.txt'), 'utf8').replace(/\r\n/g, '\n'), 'task change\nmiddle\nbeta\n');
  assert.equal(git(repository, 'status', '--porcelain'), '');
  const task = store.getTask('thread_recovery_latest')!;
  assert.equal(task.worktree!.validation!.commit, candidate);
  assert.equal(git(repository, 'show', `${task.worktree!.validation!.commit}:shared.txt`), 'task change\nmiddle\nbeta');
  assert.equal(task.worktree!.validation!.status, 'cleaned');
  assert.equal(fs.existsSync(validationPath), false);
  assert.equal(git(repository, 'worktree', 'list', '--porcelain').includes(validationPath), false);

  git(repository, 'worktree', 'add', '--detach', validationPath, target.baseCommit);
  (coordinator as any).removeValidationWorktree = async () => false;
  await (coordinator as any).cleanupValidationWorktree(repository, 'thread_recovery_latest', validationPath);
  assert.equal(store.getTask('thread_recovery_latest')!.worktree!.validation!.status, 'present');
  assert.equal(fs.existsSync(validationPath), true);
  assert.match(store.getTask('thread_recovery_latest')!.finalSummary?.gitDelivery?.validationStatus || '', /present/);
  (coordinator as any).removeValidationWorktree = removed.bind(coordinator);
  git(repository, 'worktree', 'remove', '--force', validationPath);
});

test('an approved task cannot integrate until its reviewer process releases the task role', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-reviewer-exit-'));
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_reviewer_exit', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A reviewed\nmiddle\nbeta\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved');
  const store = new TaskStore(storage);
  let helperPid: number | undefined;
  t.after(async () => { store.dispose(); if (helperPid) await waitForProcessExit(helperPid); fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }); });
  store.upsertTask(thread('thread_reviewer_exit', worktree));
  const roles = new TaskRoleLock(storage);
  const lease = roles.tryAcquire('thread_reviewer_exit', 'reviewer');
  assert.ok(lease);
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const attachChildPid = lock.attachChildPid.bind(lock);
  lock.attachChildPid = async (pid, cancelled) => {
    helperPid = (lock as unknown as { activeLease: { helperPid: number } }).activeLease.helperPid;
    return attachChildPid(pid, cancelled);
  };
  const coordinator = new MergeCoordinator(store, storage, lock);
  const pending = coordinator.mergeApproved('thread_reviewer_exit');
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.strictEqual(fs.existsSync(path.join(storage, 'merge-transactions', 'thread_reviewer_exit.json')), false);
  assert.strictEqual(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  roles.release(lease!);
  assert.strictEqual((await pending).status, 'merged');
});

test('a dead reviewer launcher does not permit merge while its native child lives', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-reviewer-child-live-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_reviewer_child_live', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved');
  const store = new TaskStore(storage); t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }); });
  store.upsertTask(thread('thread_reviewer_child_live', worktree));
  const roles = new TaskRoleLock(storage); const lease = roles.tryAcquire('thread_reviewer_child_live', 'reviewer'); assert.ok(lease);
  const liveReviewer = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
  roles.attachChild(lease, liveReviewer.pid);
  const roleFile = roles.getLockFilePath('thread_reviewer_child_live');
  const owner = JSON.parse(fs.readFileSync(roleFile, 'utf8')); owner.pid = 2147483000; owner.updatedAt = 1; fs.writeFileSync(roleFile, JSON.stringify(owner));
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const pending = coordinator.mergeApproved('thread_reviewer_child_live');
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(fs.existsSync(path.join(storage, 'merge-transactions', 'thread_reviewer_child_live.json')), false);
  liveReviewer.kill(); await new Promise<void>((resolve) => liveReviewer.once('exit', () => resolve()));
  const merged = await pending;
  assert.equal(merged.status, 'merged', store.getTask('thread_reviewer_child_live')?.worktree?.integration?.error || merged.message);
});

test('dirty approved work and failed combined validation never update the target ref', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-failure-'));
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  const signals = path.join(root, 'signals');
  fs.mkdirSync(signals);
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const store = new TaskStore(storage);
  t.after(() => {
    for (const id of ['thread_validation_fail', 'thread_dirty_approval']) {
      const saved = store.getTask(id)?.worktree;
      for (const candidate of [saved?.mergeResolutionPath, saved?.path]) {
        if (candidate && path.resolve(candidate).startsWith(path.resolve(root) + path.sep) && fs.existsSync(candidate)) {
          try { git(repository, 'worktree', 'remove', '--force', candidate); } catch {}
        }
      }
    }
    store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 });
  });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));

  const failedWorktree = createTaskWorktree(target, 'thread_validation_fail', storage);
  fs.writeFileSync(path.join(failedWorktree.path, 'shared.txt'), 'failed validation\nmiddle\nbeta\n', 'utf8');
  failedWorktree.headCommit = commitTaskChanges(failedWorktree, 'approved');
  store.upsertTask(thread('thread_validation_fail', failedWorktree));
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_FAIL = '1';
  const failed = await coordinator.mergeApproved('thread_validation_fail');
  delete process.env.MESH_VALIDATION_FAIL;
  assert.strictEqual(failed.status, 'validation_failed');
  assert.strictEqual(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  const failedValidation = store.getTask('thread_validation_fail')!.worktree!.validation!;
  assert.equal(failedValidation.result, 'failed');
  assert.equal(failedValidation.commit, undefined, 'a failed validation must not claim the old HEAD as verified');
  assert.equal(failedValidation.status === 'cleaned', !fs.existsSync(failedValidation.path));

  const dirtyWorktree = createTaskWorktree(target, 'thread_dirty_approval', storage);
  fs.writeFileSync(path.join(dirtyWorktree.path, 'shared.txt'), 'approved\nmiddle\nbeta\n', 'utf8');
  dirtyWorktree.headCommit = commitTaskChanges(dirtyWorktree, 'approved clean candidate');
  store.upsertTask(thread('thread_dirty_approval', dirtyWorktree));
  fs.writeFileSync(path.join(dirtyWorktree.path, 'shared.txt'), 'changed after approval\n', 'utf8');
  const dirty = await coordinator.mergeApproved('thread_dirty_approval');
  assert.strictEqual(dirty.status, 'stale');
  assert.strictEqual(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
});

test('a dead lock worker rejects child registration, stops validation, and leaves the target unchanged', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-dead-lock-worker-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const signals = path.join(root, 'signals');
  fs.mkdirSync(signals); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_merge_A', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A worker failed\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved with dead helper');
  const store = new TaskStore(storage);
  t.after(() => { store.dispose(); delete process.env.MESH_VALIDATION_SIGNAL_DIR; delete process.env.MESH_VALIDATION_HOLD_THREAD; fs.rmSync(root, { recursive: true, force: true }); });
  store.upsertTask(thread('thread_merge_A', worktree));
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const originalAttach = lock.attachChildPid.bind(lock);
  let validationPid: number | undefined;
  lock.attachChildPid = async (pid, cancelled) => {
    if (new TaskExecutionRegistry(storage).getOwner('thread_merge_A')?.agent !== 'merge-validation') return originalAttach(pid, cancelled);
    validationPid = pid;
    await waitFor(path.join(signals, 'thread_merge_A.entered'));
    const helperPid = JSON.parse(fs.readFileSync(`${lock.lockPath}.owner.json`, 'utf8')).helperPid as number;
    process.kill(helperPid, 'SIGKILL');
    await originalAttach(pid, cancelled);
  };
  const coordinator = new MergeCoordinator(store, storage, lock);
  const resultPromise = coordinator.mergeApproved('thread_merge_A');
  await waitFor(path.join(signals, 'thread_merge_A.entered'));
  const result = await resultPromise;
  assert.equal(result.status, 'validation_failed');
  assert.match(result.message, /helper exited|acknowledge|timed out/i);
  assert.ok(validationPid);
  try { process.kill(validationPid!, 0); assert.fail('validation process must be terminated after child tracking fails'); }
  catch (error) { assert.equal((error as NodeJS.ErrnoException)?.code, 'ESRCH'); }
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  assert.equal(store.getTask('thread_merge_A')?.status, 'waiting_merge');
  assert.equal(new TaskExecutionRegistry(storage).getOwner('thread_merge_A'), undefined);
});

test('pausing while child registration is pending ends validation without integrating', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-cancel-lock-child-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const signals = path.join(root, 'signals');
  fs.mkdirSync(signals); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_merge_A', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A paused during registration\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved before registration pause');
  const store = new TaskStore(storage);
  let helperPid: number | undefined;
  t.after(async () => {
    store.dispose(); delete process.env.MESH_VALIDATION_SIGNAL_DIR; delete process.env.MESH_VALIDATION_HOLD_THREAD;
    if (helperPid) {
      for (let n = 0; n < 200; n++) {
        try { process.kill(helperPid, 0); } catch { break; }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    fs.rmSync(root, { recursive: true, force: true });
  });
  store.upsertTask(thread('thread_merge_A', worktree));
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const originalAttach = lock.attachChildPid.bind(lock);
  let validationPid: number | undefined;
  lock.attachChildPid = async (pid, cancelled) => {
    validationPid = pid;
    helperPid = (lock as unknown as { activeLease: { helperPid: number } }).activeLease.helperPid;
    const current = store.getTask('thread_merge_A')!;
    store.upsertTask({ ...current, status: 'paused' });
    await originalAttach(pid, cancelled);
  };
  const result = await new MergeCoordinator(store, storage, lock).mergeApproved('thread_merge_A');
  assert.equal(result.status, 'stale');
  assert.ok(validationPid);
  try { process.kill(validationPid!, 0); assert.fail('validation process must stop after pause'); }
  catch (error) { assert.equal((error as NodeJS.ErrnoException)?.code, 'ESRCH'); }
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  const saved = store.getTask('thread_merge_A')!;
  assert.ok(saved.mergeHistory?.some((entry) => entry.result === 'cancelled'));
  assert.match(fs.readFileSync(saved.mergeHistory!.find((entry) => entry.phase === 'attempt')!.logPath!, 'utf8'), /\[cancelled\]/);
});

test('pause stops combined validation and forced delete waits for it before cleaning the worktree', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-cancel-merge-validation-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const signals = path.join(root, 'signals');
  fs.mkdirSync(signals); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const store = new TaskStore(storage);
  t.after(() => { store.dispose(); delete process.env.MESH_VALIDATION_SIGNAL_DIR; delete process.env.MESH_VALIDATION_HOLD_THREAD; fs.rmSync(root, { recursive: true, force: true }); });
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  const lock = new MeshFileLock(path.join(root, 'merge.lock')); const coordinator = new MergeCoordinator(store, storage, lock);
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, lock);
  const a = createTaskWorktree(target, 'thread_merge_A', storage); fs.writeFileSync(path.join(a.path, 'shared.txt'), 'A paused\nmiddle\nbeta\n'); a.headCommit = commitTaskChanges(a, 'A');
  store.upsertTask(thread('thread_merge_A', a)); process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const mergeA = coordinator.mergeApproved('thread_merge_A'); await waitFor(path.join(signals, 'thread_merge_A.entered'));
  control.pauseTask('thread_merge_A', 'stop validation', 'test');
  assert.equal((await mergeA).status, 'stale');
  const cancelledValidation = store.getTask('thread_merge_A')!.worktree!.validation!;
  assert.equal(cancelledValidation.result, 'cancelled');
  assert.equal(cancelledValidation.commit, undefined, 'cancelled validation must not claim the old HEAD');
  assert.equal(cancelledValidation.status === 'cleaned', !fs.existsSync(cancelledValidation.path));
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  assert.ok(fs.existsSync(a.path), 'pause retains the task worktree');

  const b = createTaskWorktree(target, 'thread_merge_B', storage); fs.writeFileSync(path.join(b.path, 'shared.txt'), 'alpha\nmiddle\nB cancelled\n'); b.headCommit = commitTaskChanges(b, 'B');
  store.upsertTask(thread('thread_merge_B', b)); process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_B';
  const mergeB = coordinator.mergeApproved('thread_merge_B'); await waitFor(path.join(signals, 'thread_merge_B.entered'));
  const deleting = control.deleteTask('thread_merge_B', 'discard failed validation', 'test', true);
  assert.equal((await mergeB).status, 'stale');
  assert.equal((await deleting).status, 'DELETED');
  assert.equal(fs.existsSync(b.path), false, 'force delete waits for validation exit before removing the worktree');
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
});

test('pause and delete terminate a running Git process tree and record cancelled merge attempts', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-cancel-long-git-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository); const target = resolveGitRepository(repository, storage); const store = new TaskStore(storage);
  const lock = new MeshFileLock(path.join(root, 'merge.lock')); let wakes = 0;
  const coordinator = new MergeCoordinator(store, storage, lock, () => { wakes++; });
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, lock);
  const childProcess = require('child_process') as typeof import('child_process');
  const originalSpawn = childProcess.spawn; const pids: number[] = [];
  const helper = path.join(root, 'hold-git.js'); const signal = path.join(root, 'git-running.json');
  fs.writeFileSync(helper, `const cp=require('child_process'),fs=require('fs'); const child=cp.spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore'}); fs.writeFileSync(process.env.MESH_GIT_HOLD_SIGNAL,JSON.stringify({pid:process.pid,child:child.pid})); setInterval(()=>{},1000);`);
  t.after(() => { (childProcess as any).spawn = originalSpawn; delete process.env.MESH_GIT_HOLD_SIGNAL; store.dispose();
    for (const pid of pids) try { process.kill(pid, 'SIGKILL'); } catch {}
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }); });
  for (const action of ['pause', 'delete'] as const) {
    const id = `thread_git_${action}`; const source = createTaskWorktree(target, id, storage);
    fs.writeFileSync(path.join(source.path, 'shared.txt'), `A ${action}\nmiddle\nbeta\n`);
    source.headCommit = commitTaskChanges(source, action); store.upsertTask(thread(id, source));
    let cancelled = false; store.on('changed', (tasks: DelegationThread[]) => {
      if (tasks.find((item) => item.threadId === id)?.mergeHistory?.some((entry) => entry.result === 'cancelled')) cancelled = true;
    });
    process.env.MESH_GIT_HOLD_SIGNAL = signal; try { fs.unlinkSync(signal); } catch {}
    let intercepted = false;
    (childProcess as any).spawn = (command: string, args: string[], options: any) => {
      if (!intercepted && command === gitExe) {
        intercepted = true;
        return originalSpawn(command, ['-c', `alias.meshhold=!node "${helper.replace(/\\/g, '/')}"`, 'meshhold'], options);
      }
      return originalSpawn(command, args, options);
    };
    const merge = coordinator.mergeApproved(id); await waitFor(signal, 600);
    const held = JSON.parse(fs.readFileSync(signal, 'utf8')) as { pid: number; child: number };
    const gitPid = new TaskExecutionRegistry(storage).getOwner(id)!.childPid!; pids.push(gitPid, held.pid, held.child);
    const deletion = action === 'delete' ? control.deleteTask(id, 'discard', 'test', true) : undefined;
    if (action === 'pause') control.pauseTask(id, 'pause', 'test');
    assert.equal((await merge).status, 'stale');
    if (deletion) await deletion;
    for (const pid of [gitPid, held.pid, held.child]) await waitForProcessExit(pid);
    assert.equal(cancelled, true); assert.equal(wakes, 0);
    if (action === 'pause') {
      const saved = store.getTask(id)!; assert.equal(saved.status, 'paused');
      assert.equal(saved.mergeControl?.consecutiveRetries, undefined);
      assert.match(fs.readFileSync(saved.mergeHistory!.find((entry) => entry.phase === 'attempt')!.logPath!, 'utf8'), /\[cancelled\]/);
    } else assert.equal(store.getTask(id), undefined);
    assert.equal(git(repository, 'rev-parse', target.targetRef), target.baseCommit);
    (childProcess as any).spawn = originalSpawn;
  }
});

test('a merge waiting for a reviewer cannot resume after a rapid pause and resume', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-pause-merge-waiter-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const source = createTaskWorktree(target, 'thread_waiter_pause', storage);
  fs.writeFileSync(path.join(source.path, 'shared.txt'), 'A waiter\nmiddle\nbeta\n'); source.headCommit = commitTaskChanges(source, 'candidate');
  const store = new TaskStore(storage); const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  const controller = new ReworkController(store, runner, new CallbackManager(detector, storage), detector);
  const roles = new TaskRoleLock(storage); const role = roles.tryAcquire('thread_waiter_pause', 'reviewer')!;
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const control = new TaskControlService(store, runner, undefined, storage);
  t.after(() => { roles.release(role); controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 20, retryDelay: 100 }); });
  store.upsertTask(thread('thread_waiter_pause', source));
  const oldMerge = coordinator.mergeApproved('thread_waiter_pause');
  control.pauseTask('thread_waiter_pause', 'pause', 'test'); roles.release(role);
  await controller.resumePausedTask('thread_waiter_pause');
  assert.equal((await oldMerge).status, 'stale');
  assert.equal(git(repository, 'rev-parse', target.targetRef), target.baseCommit);
  assert.equal(store.getTask('thread_waiter_pause')!.status, 'waiting_merge');
  assert.equal(store.getTask('thread_waiter_pause')!.mergeHistory, undefined);
});

test('delete protects running dirty output, force waits and cleans, and clean merged output deletes without force', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-delete-worktree-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const store = new TaskStore(storage);
  t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(path.join(root, 'merge.lock')));
  const running = createTaskWorktree(target, 'thread_delete_running_dirty', storage);
  fs.writeFileSync(path.join(running.path, 'shared.txt'), 'dirty running output\n');
  store.upsertTask({ ...thread('thread_delete_running_dirty', running), status: 'running' });
  await assert.rejects(() => control.deleteTask('thread_delete_running_dirty', 'delete', 'test', false), /not been integrated locally/i);
  assert.ok(fs.existsSync(running.path));
  await control.deleteTask('thread_delete_running_dirty', 'force discard', 'test', true);
  assert.equal(fs.existsSync(running.path), false);

  const merged = createTaskWorktree(target, 'thread_delete_merged_clean', storage);
  fs.writeFileSync(path.join(merged.path, 'shared.txt'), 'integrated output\n');
  merged.headCommit = commitTaskChanges(merged, 'integrated source');
  merged.approvedCommit = merged.headCommit;
  gitOutput(repository, ['update-ref', target.targetRef, merged.headCommit, target.baseCommit]);
  merged.integration = { phase: 'merged', mergedCommit: merged.headCommit, updatedAt: new Date().toISOString() };
  store.upsertTask({ ...thread('thread_delete_merged_clean', merged), status: 'completed', worktree: { ...merged, integration: { phase: 'merged', mergedCommit: merged.headCommit, updatedAt: new Date().toISOString() }, mergePending: false } });
  const dirtyAfterMerge = path.join(merged.path, 'shared.txt');
  fs.writeFileSync(dirtyAfterMerge, 'new unmerged change\n');
  await assert.rejects(() => control.deleteTask('thread_delete_merged_clean', 'delete', 'test', false), /not been integrated locally/i);
  gitOutput(merged.path, ['checkout', '--', 'shared.txt']);
  const result = await control.deleteTask('thread_delete_merged_clean', 'delete merged', 'test', false);
  assert.equal(result.status, 'DELETED');
});

test('ref-updated transaction recovers durable ledger failure and ledger_written cannot remerge or regress target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-ledger-crash-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger');
  initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_ledger_crash', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A durable candidate\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved durable candidate');
  const store = new TaskStore(storage); const helperPids = new Set<number>();
  t.after(async () => {
    store.dispose();
    for (const pid of helperPids) for (let n = 0; n < 200; n++) {
      try { process.kill(pid, 0); } catch { break; }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    // The last recovery helper can still be removing its channel files.
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  store.upsertTask(thread('thread_ledger_crash', worktree));
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const attachChildPid = lock.attachChildPid.bind(lock);
  lock.attachChildPid = async (pid, cancelled) => {
    const helperPid = (lock as unknown as { activeLease: { helperPid: number } }).activeLease.helperPid;
    if (helperPid) helperPids.add(helperPid);
    return attachChildPid(pid, cancelled);
  };
  const coordinator = new MergeCoordinator(store, storage, lock);
  const mutableFs = createRequire(__filename)('fs') as { renameSync: typeof fs.renameSync };
  const realRename = mutableFs.renameSync;
  const taskFile = path.join(storage, 'threads', 'thread_ledger_crash.json');
  const txFile = path.join(storage, 'merge-transactions', 'thread_ledger_crash.json');
  mutableFs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(to) === taskFile && fs.existsSync(txFile) && ['ref_updated', 'checkout_synced'].includes(JSON.parse(fs.readFileSync(txFile, 'utf8')).phase)) { const error: NodeJS.ErrnoException = new Error('injected EIO'); error.code = 'EIO'; throw error; }
    return realRename(from, to);
  }) as typeof fs.renameSync;
  try { await assert.rejects(() => coordinator.mergeApproved('thread_ledger_crash'), /EIO|durably persist/i); }
  finally { mutableFs.renameSync = realRename; }
  assert.equal(store.getTask('thread_ledger_crash')?.status, 'waiting_merge', 'failed persistence must roll in-memory state back too');
  const tx = JSON.parse(fs.readFileSync(txFile, 'utf8'));
  assert.equal(tx.phase, 'checkout_synced');
  const landed = gitOutput(repository, ['rev-parse', target.targetRef]);
  assert.equal(landed, tx.candidateCommit);
  assert.equal(JSON.parse(fs.readFileSync(taskFile, 'utf8')).status, 'waiting_merge');
  store.dispose();

  const recoveredStore = new TaskStore(storage);
  const recovered = await new MergeCoordinator(recoveredStore, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_ledger_crash');
  assert.equal(recovered.status, 'merged');
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), landed, 'recovery must not create a second merge or move target');
  assert.equal(recoveredStore.getTask('thread_ledger_crash')?.worktree?.integration?.phase, 'merged');
  recoveredStore.dispose();

  const stale = JSON.parse(fs.readFileSync(taskFile, 'utf8')) as DelegationThread;
  stale.status = 'waiting_merge'; stale.updatedAt = new Date(Date.now() + 10_000).toISOString();
  stale.worktree!.integration = { phase: 'waiting', updatedAt: stale.updatedAt };
  fs.writeFileSync(taskFile, JSON.stringify(stale, null, 2));
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), JSON.stringify([stale], null, 2));
  const staleStore = new TaskStore(storage);
  const idempotent = await new MergeCoordinator(staleStore, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_ledger_crash');
  assert.equal(idempotent.status, 'merged');
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), landed, 'ledger_written plus stale disk copy must remain idempotent');
  assert.equal(staleStore.getTask('thread_ledger_crash')?.worktree?.integration?.phase, 'merged');
  staleStore.dispose();
});

test('combined validation resolves node even when the bridge environment has no node on PATH', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-node-path-'));
  const repository = path.join(root, 'repo');
  const storage = path.join(root, 'ledger');
  initRepo(repository);
  const repo = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(repo, 'thread_node_path', storage);

  fs.writeFileSync(
    path.join(worktree.path, 'package.json'),
    JSON.stringify({ scripts: { compile: 'node validate.js', test: 'node validate.js' } }),
    'utf8',
  );
  fs.writeFileSync(path.join(worktree.path, 'readme.txt'), 'changed\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved change');

  const store = new TaskStore(storage);
  const pathKey = Object.keys(process.env).find((key) => key.toLowerCase() === 'path') || 'PATH';
  const previousPath = process.env[pathKey];
  const previousNode = process.env.DSH_BRIDGE_NODE;
  t.after(() => {
    if (previousPath === undefined) delete process.env[pathKey];
    else process.env[pathKey] = previousPath;
    if (previousNode === undefined) delete process.env.DSH_BRIDGE_NODE;
    else process.env.DSH_BRIDGE_NODE = previousNode;
    store.dispose();
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });

  // Pin the runtime, then drop every node directory from PATH: a bare `node`
  // lookup in the validation child can only succeed if the merge gate exposes
  // the resolved runtime itself.
  process.env.DSH_BRIDGE_NODE = process.execPath;
  const nodeDir = path.dirname(process.execPath);
  process.env[pathKey] = (previousPath || '')
    .split(path.delimiter)
    .filter((entry) => !!entry && path.resolve(entry) !== path.resolve(nodeDir))
    .join(path.delimiter);
  assert.deepEqual(validationNodeDirs(), [nodeDir]);

  store.upsertTask(thread('thread_node_path', worktree));
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const result = await coordinator.mergeApproved('thread_node_path');

  assert.equal(result.status, 'merged', result.message);
  assert.equal(gitOutput(repository, ['show', `${worktree.targetRef!}:readme.txt`]).replace(/\r\n/g, '\n'), 'changed');
});
