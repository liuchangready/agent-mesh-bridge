import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput, removeTaskWorktree, snapshotTaskWorktree } from '../bridge/git-worktree';
import * as gitWorktree from '../bridge/git-worktree';
import { MergeCoordinator, MeshFileLock, validationNodeDirs } from '../bridge/merge-coordinator';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { TaskControlService } from '../bridge/task-control';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { ReworkController } from '../bridge/rework-controller';
import { git, assertSameExistingPath, initRepo, thread, waitFor, waitForValidationStart } from './git-worktree-fixture';

function reviewStub(detector: AgentDetector, storage: string): CallbackManager {
  const manager = new CallbackManager(detector, storage);
  manager.notifyCaller = async () => true;
  manager.notifyCallerOfFailure = async () => false;
  return manager;
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
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_approval_snapshot', worktree);
  item.status = 'reviewing';
  item.worktree!.approvedCommit = undefined;
  item.worktree!.integration = { phase: 'ready', updatedAt: new Date().toISOString() };
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  const before = git(worktree.path, 'status', '--porcelain');
  const head = git(worktree.path, 'rev-parse', 'HEAD');
  assert.equal(controller.approveTask({ threadId: item.threadId }).status, 'QUEUED');
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
  item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  removeTaskWorktree(worktree);
  assert.equal(controller.approveTask({ threadId: item.threadId }).status, 'REVIEWING');
  await new Promise((resolve) => setTimeout(resolve, 20));
  const saved = store.getTask(item.threadId)!;
  assert.equal(saved.status, 'reviewing');
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
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_snapshot_conflict', worktree);
  item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  const candidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  const result = await controller.mergeApproved(item.threadId);
  const saved = store.getTask(item.threadId)!;
  assert.equal(result.status, 'conflict');
  assert.equal(saved.status, 'reviewing');
  assert.equal(saved.worktree!.approvedCommit, candidate);
  assert.deepEqual(saved.worktree!.integration!.conflictFiles, ['shared.txt']);
  assert.ok(fs.existsSync(saved.worktree!.mergeResolutionPath!));
  assert.match(fs.readFileSync(path.join(saved.worktree!.mergeResolutionPath!, 'shared.txt'), 'utf8'), /<<<<<<< /);
  const resolution = saved.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A resolved\nmiddle\nbeta\n');
  git(resolution, 'add', 'shared.txt');
  assert.equal((await controller.submitMergeResolution(item.threadId)).status, 'QUEUED');
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
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { delete process.env.MESH_VALIDATION_FAIL; controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_snapshot_validation', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A approved\nmiddle\nbeta\n');
  fs.writeFileSync(path.join(worktree.path, 'validate.js'), `if(process.env.MESH_VALIDATION_FAIL==='1'){process.stdout.write('START-OUTPUT-'+ 'x'.repeat(25000)+'-END-OUTPUT');process.exitCode=6;}`);
  controller.approveTask({ threadId: item.threadId });
  const candidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  process.env.MESH_VALIDATION_FAIL = '1';
  const failed = await controller.mergeApproved(item.threadId); delete process.env.MESH_VALIDATION_FAIL;
  const saved = store.getTask(item.threadId)!;
  assert.equal(failed.status, 'validation_failed', failed.message); assert.equal(saved.status, 'reviewing');
  assert.equal(saved.worktree!.approvedCommit, candidate);
  assert.ok(fs.existsSync(saved.worktree!.mergeResolutionPath!));
  const mergeLog = fs.readFileSync(saved.mergeHistory![0].logPath!, 'utf8');
  assert.match(mergeLog, /failed \(exit 6\)/);
  assert.match(mergeLog, /\$ git merge --no-commit --no-ff/);
  assert.ok(mergeLog.includes('START-OUTPUT-') && mergeLog.includes('-END-OUTPUT'));
  await controller.submitMergeResolution(item.threadId);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
});

test('the last merge retry enters failed and retry_merge starts a new generation', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-budget-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_merge_budget', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_merge_budget', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A change\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  const coordinator = (controller as any).mergeCoordinator;
  coordinator.validate = async () => ({ ok: false, error: 'test failed (exit 2): same failure' });
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'validation_failed');
  const firstFailure = store.getTask(item.threadId)!;
  assert.equal(firstFailure.mergeControl!.consecutiveRetries, 0);
  assert.equal(firstFailure.status, 'reviewing');
  // merge-recovery.test.ts covers every counter transition; exercise only the real Git boundaries here.
  firstFailure.mergeControl!.consecutiveRetries = 5;
  firstFailure.mergeControl!.revision++;
  store.upsertTask(firstFailure);
  await controller.submitMergeResolution(item.threadId);
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'validation_failed');
  const exhausted = store.getTask(item.threadId)!;
  assert.equal(exhausted.mergeControl!.consecutiveRetries, 6);
  assert.equal(exhausted.status, 'failed');
  assert.equal(exhausted.failure?.stage, 'merge');
  assert.ok(exhausted.failure.reason);
  assert.ok((exhausted.failureRevision || 0) > 0);
  const before = store.getTask(item.threadId)!;
  const pathBefore = before.worktree!.mergeResolutionPath; const sessionBefore = before.receiverSessionId;
  await assert.rejects(() => controller.submitMergeResolution(item.threadId), /awaiting submission/);
  assert.throws(() => controller.approveTask({ threadId: item.threadId }), /failed/);
  const retried = controller.retryMerge(item.threadId);
  const after = store.getTask(item.threadId)!;
  assert.equal(retried.retryGeneration, 1); assert.equal(after.status, 'reviewing');
  assert.equal(after.worktree!.mergeResolutionPath, pathBefore); assert.equal(after.receiverSessionId, sessionBefore);
  assert.ok(after.mergeHistory!.length > before.mergeHistory!.length);
});

test('deleting an unmerged snapshot unregisters its retained resolution worktree', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-delete-resolution-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_delete_resolution', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_delete_resolution', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
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
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_advance', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A resolved\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  await controller.submitMergeResolution(item.threadId);
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
  const controller = new ReworkController(store, runner, reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_reapprove_resolution', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  item.receiverSessionCaptured = true; store.upsertTask(item);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'A target\nmiddle\nbeta\n');
  git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'advance target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A old resolution\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  const oldCandidate = store.getTask(item.threadId)!.worktree!.approvedCommit;
  const result = await controller.requestRework({ threadId: item.threadId, feedback: 'Change the candidate to add fresh.txt instead.' });
  assert.equal(result.receiverSessionId, item.receiverSessionId);
  for (let n = 0; n < 200 && store.getTask(item.threadId)?.status !== 'reviewing'; n++) await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(store.getTask(item.threadId)?.status, 'reviewing');
  assert.equal(store.getTask(item.threadId)?.worktree?.integration?.phase, 'ready');
  assert.equal(store.getTask(item.threadId)?.worktree?.approvedCommit, undefined);
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
  const controller = new ReworkController(store, new AutonomousRunner(detector), reviewStub(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_reconflict', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n');
  controller.approveTask({ threadId: item.threadId });
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A first fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  await controller.submitMergeResolution(item.threadId);
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target again\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target again');
  assert.equal((await controller.mergeApproved(item.threadId)).status, 'conflict');
  assert.equal(store.getTask(item.threadId)!.worktree!.mergeResolutionPath, resolution);
  assert.match(fs.readFileSync(path.join(resolution, 'shared.txt'), 'utf8'), /<<<<<<< /);
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A final fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  await controller.submitMergeResolution(item.threadId);
  const merged = await controller.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A final fix\nmiddle\nbeta');
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
  let mergeA: Promise<any> | undefined;
  t.after(() => {
    // Always release the test-owned validation child, including when dispatch
    // fails before reaching the normal release point below.
    try { fs.writeFileSync(path.join(signals, 'thread_merge_A.release'), 'cleanup'); } catch {}
    return Promise.race([
      mergeA?.catch(() => undefined) || Promise.resolve(),
      new Promise((resolve) => setTimeout(resolve, 5_000)),
    ]).finally(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  });
  store.upsertTask(thread('thread_merge_A', a));
  store.upsertTask(thread('thread_merge_B', b));
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const previousSignalDir = process.env.MESH_VALIDATION_SIGNAL_DIR;
  const previousHoldThread = process.env.MESH_VALIDATION_HOLD_THREAD;
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const mergePromiseA = coordinator.mergeApproved('thread_merge_A');
  mergeA = mergePromiseA;
  let mergeB: ReturnType<typeof coordinator.mergeApproved> | undefined;
  const releaseA = () => fs.writeFileSync(path.join(signals, 'thread_merge_A.release'), 'go');
  try {
  await waitForValidationStart(path.join(signals, 'thread_merge_A.entered'), mergePromiseA);
  assert.equal(new TaskRoleLock(storage).tryAcquire('thread_merge_A', 'reviewer'), null, 'reviewer cannot overlap merge validation');

  // A is inside its real combined verification while holding the mesh mutex.
  // A third task must still be admitted and receive its own worktree.
  const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({ exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 });
  const callback = new CallbackManager(detector, storage); const registry = new TaskExecutionRegistry(storage);
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  const originalCreateTaskWorktree = gitWorktree.createTaskWorktree;
  let announceWorktreeStart!: () => void;
  const worktreeStart = new Promise<void>((resolve) => { announceWorktreeStart = resolve; });
  (gitWorktree as any).createTaskWorktree = (...args: Parameters<typeof createTaskWorktree>) => {
    announceWorktreeStart();
    return originalCreateTaskWorktree(...args);
  };
  let dispatchResult;
  try {
    const dispatch = dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'dispatch during validation', prompt: 'independent task', workspaceRoot: repository });
    await worktreeStart;
    dispatchResult = await dispatch;
  } finally {
    (gitWorktree as any).createTaskWorktree = originalCreateTaskWorktree;
  }
  const admitted = store.getTask(dispatchResult.threadId)!;
  assert.notEqual(path.resolve(admitted.worktree!.path), path.resolve(a.path));
  assert.notEqual(path.resolve(admitted.worktree!.path), path.resolve(b.path));
  registry.dispose(); (dispatcher as any).queueDrainer?.dispose();

  mergeB = coordinator.mergeApproved('thread_merge_B');
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(store.getTask('thread_merge_A')?.status, 'merging');
  assert.equal(store.getTask('thread_merge_B')?.status, 'queued');
  assert.equal(store.getTask('thread_merge_B')?.queuePhase, 'merge');
  assert.strictEqual(fs.existsSync(path.join(signals, 'thread_merge_B.entered')), false, 'B must not reach combined validation while A owns the global lock');
  assert.strictEqual(fs.existsSync(path.join(storage, 'merge-transactions', 'thread_merge_B.json')), false, 'B must not read/persist the target before acquiring the lock');
  releaseA();
  const resultA = await mergeA;
  assert.strictEqual(resultA.status, 'merged');
  const commitA = resultA.commit!;

  await waitForValidationStart(path.join(signals, 'thread_merge_B.entered'), mergeB);
  assert.equal(store.getTask('thread_merge_B')?.status, 'merging');
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
  } finally {
    releaseA();
    process.env.MESH_VALIDATION_HOLD_THREAD = '';
    const mergesSettled = Promise.allSettled([mergeA, ...(mergeB ? [mergeB] : [])]);
    let cleanupTimer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([mergesSettled, new Promise<void>((resolve) => { cleanupTimer = setTimeout(resolve, 60_000); })]);
    } finally { if (cleanupTimer) clearTimeout(cleanupTimer); }
    if (previousSignalDir === undefined) delete process.env.MESH_VALIDATION_SIGNAL_DIR;
    else process.env.MESH_VALIDATION_SIGNAL_DIR = previousSignalDir;
    if (previousHoldThread === undefined) delete process.env.MESH_VALIDATION_HOLD_THREAD;
    else process.env.MESH_VALIDATION_HOLD_THREAD = previousHoldThread;
  }
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

test('historical records without deliveryTarget refuse Git execution', async (t) => {
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
  await assert.rejects(new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_legacy_target'), /MISSING_DELIVERY_TARGET/);
  assert.equal(git(repository, 'rev-parse', legacyRef), base);
  assert.equal(git(repository, 'rev-parse', 'feature/caller'), base);
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
