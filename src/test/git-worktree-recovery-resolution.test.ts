import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { resolveGitRepository, createTaskWorktree } from '../bridge/git-worktree';
import { TaskStore } from '../bridge/task-store';
import { MergeCoordinator } from '../bridge/merge-coordinator';
import { git, initRepo, thread, storeApprovedGitSnapshot, storeQueuedGitResolution } from './git-worktree-fixture';

test('a saved resolution survives a coordinator failure before switching to the advanced target', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-recovery-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_resolution_recovery', storage);
  const store = new TaskStore(storage);
  const coordinator = new MergeCoordinator(store, storage);
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_recovery', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); storeApprovedGitSnapshot(store, item);
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  storeQueuedGitResolution(store, item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next\n'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');

  const original = (coordinator as any).writeTransaction.bind(coordinator);
  (coordinator as any).writeTransaction = (file: string, tx: any) => { original(file, tx); if (tx.phase === 'resolution_rebase_prepared') throw new Error('injected interruption'); };
  assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'blocked');
  (coordinator as any).writeTransaction = original;
  assert.equal(git(resolution, 'show', ':shared.txt'), 'A saved fix\nmiddle\nbeta');
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  storeQueuedGitResolution(store, item.threadId);
  const merged = await coordinator.mergeApproved(item.threadId);
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
  const store = new TaskStore(storage);
  const coordinator = new MergeCoordinator(store, storage);
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_reset', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); storeApprovedGitSnapshot(store, item);
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  storeQueuedGitResolution(store, item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next\n'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');

  const original = (coordinator as any).loggedGit.bind(coordinator);
  (coordinator as any).loggedGit = async (threadId: string, cwd: string, args: string[]) => {
    const value = await original(threadId, cwd, args);
    if (cwd === resolution && args[0] === 'reset') throw new Error('injected after reset');
    return value;
  };
  try { assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'blocked'); }
  finally { (coordinator as any).loggedGit = original; }
  assert.equal(store.getTask(item.threadId)!.worktree!.mergeResolutionSubmitted, true);
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  const merged = await coordinator.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A saved fix\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:next.txt`), 'next');
  assert.equal(git(repository, 'show', `${target.targetRef}:again.txt`), 'again');
});

test('an interrupted resolution merge is replayed in a fresh workspace when the target advances again', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resolution-replay-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_resolution_replay', storage);
  const store = new TaskStore(storage);
  const coordinator = new MergeCoordinator(store, storage);
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const item = thread('thread_resolution_replay', worktree); item.status = 'reviewing'; item.worktree!.approvedCommit = undefined;
  store.upsertTask(item); fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'worker\nmiddle\nbeta\n'); storeApprovedGitSnapshot(store, item);
  fs.writeFileSync(path.join(repository, 'shared.txt'), 'target\nmiddle\nbeta\n'); git(repository, 'add', 'shared.txt'); git(repository, 'commit', '-m', 'target');
  assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'conflict');
  const resolution = store.getTask(item.threadId)!.worktree!.mergeResolutionPath!;
  fs.writeFileSync(path.join(resolution, 'shared.txt'), 'A saved fix\nmiddle\nbeta\n'); git(resolution, 'add', 'shared.txt');
  storeQueuedGitResolution(store, item.threadId);
  fs.writeFileSync(path.join(repository, 'next.txt'), 'next'); git(repository, 'add', 'next.txt'); git(repository, 'commit', '-m', 'advance');
 const original = (coordinator as any).loggedGit.bind(coordinator);
  (coordinator as any).loggedGit = async (id: string, cwd: string, args: string[]) => {
    const value = await original(id, cwd, args);
    if (cwd === resolution && args[0] === 'merge') throw new Error('injected after resolution merge');
    return value;
  };
  try { assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'blocked'); } finally { (coordinator as any).loggedGit = original; }
  fs.writeFileSync(path.join(repository, 'again.txt'), 'again'); git(repository, 'add', 'again.txt'); git(repository, 'commit', '-m', 'advance again');
  const write = (coordinator as any).writeTransaction.bind(coordinator); let interrupted = false;
  (coordinator as any).writeTransaction = (file: string, tx: any) => {
    write(file, tx);
    if (!interrupted && tx.phase === 'resolution_rebase_prepared' && tx.validationPath !== resolution) { interrupted = true; throw new Error('injected after replacement workspace transaction'); }
  };
  try { assert.equal((await coordinator.mergeApproved(item.threadId)).status, 'blocked'); } finally { (coordinator as any).writeTransaction = write; }
  assert.equal(interrupted, true);
  const merged = await coordinator.mergeApproved(item.threadId);
  assert.equal(merged.status, 'merged', merged.message);
  assert.equal(git(repository, 'show', `${target.targetRef}:shared.txt`), 'A saved fix\nmiddle\nbeta');
  assert.equal(git(repository, 'show', `${target.targetRef}:again.txt`), 'again');
  assert.ok(store.getTask(item.threadId)!.worktree!.supersededMergeResolutionPaths!.includes(resolution));
});
