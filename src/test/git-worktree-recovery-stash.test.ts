import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges } from '../bridge/git-worktree';
import { MergeCoordinator, MeshFileLock } from '../bridge/merge-coordinator';
import { TaskStore } from '../bridge/task-store';
import { git, initRepo, thread } from './git-worktree-fixture';

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
