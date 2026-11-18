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
