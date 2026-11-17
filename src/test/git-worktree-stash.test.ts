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

for (const failRollback of [false, true]) test(`ref publication failure preserves recoverable stash metadata, rollback failure=${failRollback}`, async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-ref-fail-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const id = 'thread_stash_ref_fail'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const id = 'thread_stash_protected_restart'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const id = 'thread_stash_ref_after_write'; const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_pause_publish', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_unlock_fail', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
  fs.writeFileSync(path.join(repository, 'mine.txt'), 'mine');
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
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const { old, extra } = initMultiStashRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_stash_lock_drift', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A task\nmiddle\nbeta\n'); worktree.headCommit = commitTaskChanges(worktree, 'task');
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
