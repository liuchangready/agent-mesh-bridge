import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, removeTaskWorktree } from '../bridge/git-worktree';
import { MergeCoordinator, MeshFileLock } from '../bridge/merge-coordinator';
import { TaskStore } from '../bridge/task-store';
import { TaskControlService } from '../bridge/task-control';
import { gitExe, git, initRepo, initMultiStashRepo, thread } from './git-worktree-fixture';

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

test('landing restores its own stash when another stash is added after it', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-order-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old: oldSha } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_order', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_old', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_lock_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_stash_lock_fail', worktree));
  t.after(() => { store.dispose(); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  const lockPath = path.join(repository, '.git', 'refs', 'stash.lock');
  (coordinator as any).removeSoleLandingStash = async () => { fs.writeFileSync(lockPath, '', { flag: 'wx' }); return false; };
  const multi = (coordinator as any).removeMultipleLandingStash.bind(coordinator);
  const mutableFs = createRequire(__filename)('fs') as typeof fs;
  const link = mutableFs.linkSync;
  let attempts = 0;
  t.mock.method(mutableFs, 'linkSync', (source: fs.PathLike, target: fs.PathLike) => {
    if (String(target) === lockPath) attempts++;
    return link(source, target);
  });
  (coordinator as any).removeMultipleLandingStash = async (...args: any[]) => {
    // Advance only the retry stage; process startup, Git and lock ownership stay real.
    t.mock.timers.enable({ apis: ['setTimeout'] });
    try {
      let settled = false;
      const pending = multi(...args).then((value: boolean) => { settled = true; return value; });
      for (let n = 0; n < 99; n++) { t.mock.timers.tick(100); await Promise.resolve(); }
      assert.equal(attempts, 100);
      t.mock.timers.tick(99); await Promise.resolve();
      assert.equal(settled, false, 'cleanup still waits at 9999ms');
      t.mock.timers.tick(1);
      const removed = await pending;
      assert.equal(removed, false, 'the retry budget expires at 10000ms');
      assert.equal(attempts, 100, 'the retry budget does not allow an extra attempt');
      assert.equal(fs.existsSync(lockPath), true, 'the held ref lock is preserved');
      return removed;
    } finally { t.mock.timers.reset(); }
  };
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_log_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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

for (const [failRollback, deleteBeforeRecovery] of [[false, false], [true, false], [true, true]]) test(
  `ref publication failure preserves recoverable stash metadata, rollback failure=${failRollback}, delete=${deleteBeforeRecovery}`, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-ref-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const id = `thread_stash_ref_fail_${failRollback ? 'rollback' : 'ref'}`; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'later-stash.txt'), 'later'); git(repository, 'stash', 'push', '-u', '-m', 'later');
  const later = git(repository, 'rev-parse', 'refs/stash'); git(repository, 'stash', 'drop');
  fs.writeFileSync(path.join(repository, 'staged.txt'), 'staged\n'); git(repository, 'add', 'staged.txt');
  fs.appendFileSync(path.join(repository, 'staged.txt'), 'unstaged\n'); fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
  const beforeStatus = git(repository, 'status', '--porcelain');
  const store = new TaskStore(storage); store.upsertTask(thread(id, worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); try { fs.rmSync(root, { recursive: true, force: true }); } catch {} });
  const coordinator = new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock')));
  let targetRefUpdates = 0;
  const loggedGit = (coordinator as any).loggedGit.bind(coordinator);
  (coordinator as any).loggedGit = async (threadId: string, cwd: string, args: string[]) => {
    if (args[0] === 'update-ref' && args[1] === target.targetRef) targetRefUpdates++;
    return loggedGit(threadId, cwd, args);
  };
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
  assert.equal(targetRefUpdates, 1, 'the approved commit is published once before stash metadata recovery');
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
    if (deleteBeforeRecovery) {
      const control = new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(path.join(root, 'merge.lock')));
      const deleted = await control.deleteTask(id, 'delete', 'test', true);
      assert.equal(deleted.status, 'DELETED');
      assert.match(deleted.message, /stash recovery metadata remains pending/i);
      assert.equal(store.getTask(id), undefined);
      assert.ok(fs.existsSync(worktree.path));
      assert.ok(fs.existsSync(path.join(repository, '.git', 'refs', 'stash.lock')));
      assert.deepEqual(JSON.parse(fs.readFileSync(txFile, 'utf8')), tx);
      return;
    }
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
      c.mergeApproved('${id}').then(r=>{if(r.status!=='merged')throw Error(r.message);s.dispose()}).catch(e=>{console.error(e);process.exitCode=1});`;
    const child = spawnSync(process.execPath, ['-e', script, require.resolve('../bridge/task-store'), require.resolve('../bridge/merge-coordinator'), storage, path.join(root, 'merge.lock'), repository], { encoding: 'utf8', timeout: 60000, windowsHide: true });
    assert.equal(child.status, 0, child.stderr); store.loadFromDisk();
    assert.equal(store.getTask(id)!.status, 'completed');
  } else {
    const recovered = await coordinator.mergeApproved(id);
    assert.equal(recovered.status, 'merged', recovered.message);
    assert.equal(targetRefUpdates, 1, 'stash recovery reuses the landed commit without a second target ref update');
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

