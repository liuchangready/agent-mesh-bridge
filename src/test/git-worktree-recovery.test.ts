import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput, removeTaskWorktree } from '../bridge/git-worktree';
import { MergeCoordinator, MeshFileLock } from '../bridge/merge-coordinator';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';
import { git, initRepo, thread } from './git-worktree-fixture';

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
