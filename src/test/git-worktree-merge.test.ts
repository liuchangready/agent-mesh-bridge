import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput, removeTaskWorktree } from '../bridge/git-worktree';
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
    receiverSessionId: `session-${threadId}`, workspacePath: worktree.path,
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

  // A is inside its real combined verification while holding the mesh mutex.
  // A third task must still be admitted and receive its own worktree.
  const detector = new AgentDetector(); const runner = new AutonomousRunner(detector);
  runner.execute = async () => ({ exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 });
  const callback = new CallbackManager(detector, storage); const registry = new TaskExecutionRegistry(storage);
  const dispatcher = new AsyncDispatcher(store, new ActiveAgentResolver(detector), runner, callback, detector, registry);
  const dispatchResult = await Promise.race([
    dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'dispatch during validation', prompt: 'independent task', workspacePath: repository }),
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

test('a dirty checked-out delivery target blocks safely and preserves user files', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-dirty-delivery-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_dirty_delivery', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A\nmiddle\nbeta\n', 'utf8');
  worktree.headCommit = commitTaskChanges(worktree, 'approved change');
  const originalTarget = git(repository, 'rev-parse', target.targetRef);
  const userFile = path.join(repository, 'keep-me.txt'); fs.writeFileSync(userFile, 'uncommitted user data\n');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_dirty_delivery', worktree));
  t.after(() => { store.dispose(); removeTaskWorktree(worktree); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const result = await new MergeCoordinator(store, storage, new MeshFileLock(path.join(root, 'merge.lock'))).mergeApproved('thread_dirty_delivery');
  assert.equal(result.status, 'blocked');
  assert.equal(git(repository, 'rev-parse', target.targetRef), originalTarget);
  assert.equal(fs.readFileSync(userFile, 'utf8'), 'uncommitted user data\n');
  const saved = store.getTask('thread_dirty_delivery')!;
  assert.equal(saved.status, 'waiting_merge');
  assert.equal(saved.worktree!.integration!.phase, 'blocked');
  assert.match(saved.worktree!.integration!.error || '', /uncommitted files/i);
  assert.equal(saved.worktree!.validation!.status, 'cleaned');
  assert.ok(saved.worktree!.validation!.commit);
  assert.equal(fs.existsSync(saved.worktree!.validation!.path), false);
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
    fs.rmSync(root, { recursive: true, force: true });
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
  const removed = (coordinator as unknown as { removeValidationWorktree: (repo: string, validation: string) => boolean }).removeValidationWorktree;
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
  (coordinator as unknown as { removeValidationWorktree: (repo: string, validation: string) => boolean }).removeValidationWorktree = () => false;
  (coordinator as unknown as { cleanupValidationWorktree: (repo: string, thread: string, validation: string) => void }).cleanupValidationWorktree(repository, 'thread_recovery_latest', validationPath);
  assert.equal(store.getTask('thread_recovery_latest')!.worktree!.validation!.status, 'present');
  assert.equal(fs.existsSync(validationPath), true);
  assert.match(store.getTask('thread_recovery_latest')!.finalSummary?.gitDelivery?.validationStatus || '', /present/);
  (coordinator as unknown as { removeValidationWorktree: typeof removed }).removeValidationWorktree = removed.bind(coordinator);
  assert.equal((coordinator as unknown as { removeValidationWorktree: (repo: string, validation: string) => boolean }).removeValidationWorktree(repository, validationPath), true);
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
  const store = new TaskStore(storage); t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
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
  t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
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
    validationPid = pid;
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
  assert.equal(store.getTask('thread_merge_A')?.status, 'waiting_review');
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
  mutableFs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(to) === taskFile) { const error: NodeJS.ErrnoException = new Error('injected EIO'); error.code = 'EIO'; throw error; }
    return realRename(from, to);
  }) as typeof fs.renameSync;
  try { await assert.rejects(() => coordinator.mergeApproved('thread_ledger_crash'), /EIO|durably persist/i); }
  finally { mutableFs.renameSync = realRename; }
  assert.equal(store.getTask('thread_ledger_crash')?.status, 'waiting_merge', 'failed persistence must roll in-memory state back too');
  const txFile = path.join(storage, 'merge-transactions', 'thread_ledger_crash.json');
  const tx = JSON.parse(fs.readFileSync(txFile, 'utf8'));
  assert.equal(tx.phase, 'ref_updated');
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
