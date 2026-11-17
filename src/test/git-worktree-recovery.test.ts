import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput, removeTaskWorktree } from '../bridge/git-worktree';
import { MergeCoordinator, MeshFileLock } from '../bridge/merge-coordinator';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { ReworkController } from '../bridge/rework-controller';
import { git, initRepo, thread } from './git-worktree-fixture';

test('a saved resolution survives a coordinator failure before switching to the advanced target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-recovery-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_recovery', storage);
  const store = new TaskStore(storage); const detector = new AgentDetector();
  const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector);
  t.after(() => { controller.dispose(); store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_recovery', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
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
  const item = thread('thread_resolution_reset', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
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
  const item = thread('thread_resolution_replay', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
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
  assert.equal(saved.status, 'reviewing');
  assert.equal(saved.worktree!.integration!.phase, 'blocked');
  assert.notEqual(saved.status, 'completed');
});

test('host recovery requeues an interrupted merge until the global lock is available', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-restart-lock-wait-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_restart_wait', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A restart\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved');
  const store = new TaskStore(storage); const interrupted = thread('thread_restart_wait', worktree);
  interrupted.status = 'merging'; interrupted.queuePhase = undefined; store.upsertTask(interrupted);
  const release = await new MeshFileLock().acquire(); assert.ok(release);
  const detector = new AgentDetector(); const controller = new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector, storage), detector, undefined, true);
  t.after(() => { release(); controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  assert.equal(store.getTask(interrupted.threadId)?.status, 'queued');
  assert.equal(store.getTask(interrupted.threadId)?.queuePhase, 'merge');
  release();
  for (let n = 0; n < 500 && store.getTask(interrupted.threadId)?.status !== 'completed'; n++) await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(store.getTask(interrupted.threadId)?.status, 'completed');
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
  assert.equal(store.getTask('thread_ledger_crash')?.status, 'merging', 'failed persistence must roll in-memory state back too');
  const tx = JSON.parse(fs.readFileSync(txFile, 'utf8'));
  assert.equal(tx.phase, 'checkout_synced');
  const landed = gitOutput(repository, ['rev-parse', target.targetRef]);
  assert.equal(landed, tx.candidateCommit);
  assert.equal(JSON.parse(fs.readFileSync(taskFile, 'utf8')).status, 'merging');
  store.dispose();

  const recoveredStore = new TaskStore(storage);
  const recovered = await new MergeCoordinator(recoveredStore, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_ledger_crash');
  assert.equal(recovered.status, 'merged');
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), landed, 'recovery must not create a second merge or move target');
  assert.equal(recoveredStore.getTask('thread_ledger_crash')?.worktree?.integration?.phase, 'merged');
  recoveredStore.dispose();

  const stale = JSON.parse(fs.readFileSync(taskFile, 'utf8')) as DelegationThread;
  stale.status = 'merging'; stale.updatedAt = new Date(Date.now() + 10_000).toISOString();
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
