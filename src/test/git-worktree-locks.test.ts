import { test, type TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';
import { createRequire } from 'module';
import { resolveGitRepository, createTaskWorktree, commitTaskChanges, gitOutput } from '../bridge/git-worktree';
import { buildPosixLockHelperCommand, MergeCoordinator, MeshFileLock, type MergeResult } from '../bridge/merge-coordinator';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';
import { TaskControlService } from '../bridge/task-control';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { ReworkController } from '../bridge/rework-controller';
import { getProcessBirthId } from '../bridge/platform';
import { gitExe, git, initRepo, thread, waitFor, waitForExit, waitForProcessExit, waitForValidationStart } from './git-worktree-fixture';

function trackValidationMerges(
  t: TestContext, root: string, signals: string, store: TaskStore, control: TaskControlService,
) {
  const active = new Map<string, Promise<MergeResult>>();
  t.after(async () => {
    let timeout: NodeJS.Timeout | undefined;
    try {
      const outstanding = [...active];
      for (const [threadId] of outstanding) {
        const status = store.getTask(threadId)?.status;
        try {
          if (status === 'queued' || status === 'merging') control.pauseTask(threadId, 'Validation fixture teardown', 'test');
        } finally {
          fs.writeFileSync(path.join(signals, `${threadId}.release`), 'release');
        }
      }
      if (outstanding.length) {
        await Promise.race([
          Promise.allSettled(outstanding.map(([, merge]) => merge)),
          new Promise<never>((_, reject) => {
            timeout = setTimeout(() => reject(new Error(`Merge cleanup timed out; fixture retained at ${root}`)), 60_000);
          }),
        ]);
      }
    } finally {
      clearTimeout(timeout);
      store.dispose();
      delete process.env.MESH_VALIDATION_SIGNAL_DIR;
      delete process.env.MESH_VALIDATION_HOLD_THREAD;
    }
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  return (threadId: string, merge: Promise<MergeResult>) => {
    active.set(threadId, merge);
    const done = () => { active.delete(threadId); };
    void merge.then(done, done);
    return merge;
  };
}

test('unpublished child command files survive helper scans without losing child protection', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-unpublished-'));
  const lockPath = path.join(root, 'merge.lock');
  const lock = new MeshFileLock(lockPath);
  const contender = new MeshFileLock(lockPath);
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
  let helperPid: number | undefined;
  t.after(async () => {
    child.kill('SIGKILL');
    await waitForExit(child);
    lock.dispose(); contender.dispose();
    if (helperPid) await waitForProcessExit(helperPid);
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
  });
  const release = await lock.acquire();
  assert.ok(release);
  const lease = (lock as any).activeLease;
  helperPid = lease.helperPid ?? lease.helper.pid;
  const childTemp = `${lease.channelPath}.command.child.${child.pid}.unpublished.tmp`;
  const exitTemp = `${lease.channelPath}.command.exit.${child.pid}.unpublished.tmp`;
  fs.writeFileSync(childTemp, 'CHILD');
  fs.writeFileSync(exitTemp, 'EXIT');
  const birthId = (await lock.attachChildPid(child.pid)) || '';
  if (process.platform === 'win32') {
    assert.match(birthId || '', /^\d+$/);
    assert.equal(birthId, getProcessBirthId(child.pid!));
    assert.equal(await lock.attachChildPid(child.pid, undefined, birthId), birthId, 'the same lease reuses its exact verified child ACK');
  }
  await new Promise((resolve) => setTimeout(resolve, 100));
  assert.ok(fs.existsSync(childTemp), 'a writer must retain its temporary file until atomic publication');
  assert.ok(fs.existsSync(exitTemp), 'an unpublished exit command must be ignored');
  release();
  const early = await contender.acquire(undefined, undefined, 300);
  assert.equal(early, undefined, 'the live registered child must still protect the lock');
  child.kill('SIGKILL');
  await waitForExit(child);
  const afterExit = await contender.acquire(undefined, undefined, 5000);
  assert.ok(afterExit, 'the lock becomes available after the registered child exits');
  afterExit();
});

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
    fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
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
    const script = `
      const path = require('path'), { mock } = require('node:test'), { MeshFileLock } = require(process.argv[1]);
      Object.defineProperty(process, 'platform', { value: 'linux' });
      const pathKey = Object.keys(process.env).find(k => k.toLowerCase() === 'path') || 'PATH';
      process.env[pathKey] = path.dirname(process.argv[3]) + path.delimiter + process.env[pathKey];
      (async () => {
        const lockPath = process.argv[2], first = await new MeshFileLock(lockPath).acquire();
        mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
        const started = Date.now();
        let acquired = false;
        const waiting = new MeshFileLock(lockPath).acquire().then(release => { acquired = !!release; return release; });
        mock.timers.tick(15500);
        await new Promise(resolve => setImmediate(resolve));
        if (acquired) throw new Error('waiter bypassed the held OS lock');
        first();
        const second = await waiting;
        if (!second) throw new Error('waiter cancelled');
        console.log('POSIX WAIT ACQUIRED AFTER ' + (Date.now() - started) + 'ms');
        second();
        mock.timers.reset();
      })().catch(e => { console.error(e); process.exitCode = 1; });
    `;
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
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const rejected = assert.rejects(lock.attachChildPid(process.pid), /Timed out waiting 10000ms/);
  t.mock.timers.tick(9_999);
  assert.equal(lease.childAcks.size, 1, 'the registration remains pending before its deadline');
  t.mock.timers.tick(1);
  await rejected;
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

test('forced deletion cancels a queued merge before the global lock is released', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-delete-lock-wait-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repository);
  const target = resolveGitRepository(repository, storage); const worktree = createTaskWorktree(target, 'thread_delete_lock_wait', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A waiting\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved');
  const store = new TaskStore(storage); store.upsertTask(thread('thread_delete_lock_wait', worktree));
  const lockPath = path.join(root, 'merge.lock'); const owner = new MeshFileLock(lockPath); const release = await owner.acquire(); assert.ok(release);
  t.after(() => { release(); store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  const pending = new MergeCoordinator(store, storage, new MeshFileLock(lockPath)).mergeApproved('thread_delete_lock_wait');
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.equal(store.getTask('thread_delete_lock_wait')?.status, 'queued');
  const deleting = new TaskControlService(store, undefined, undefined, storage, undefined, new MeshFileLock(lockPath))
    .deleteTask('thread_delete_lock_wait', 'discard', 'test', true);
  assert.equal((await pending).status, 'stale');
  release(); await deleting;
  assert.equal(store.getTask('thread_delete_lock_wait'), undefined);
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
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

test('a dead lock worker rejects child registration, stops validation, and leaves the target unchanged', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-dead-lock-worker-'));
  const repository = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); const signals = path.join(root, 'signals');
  fs.mkdirSync(signals); initRepo(repository);
  const target = resolveGitRepository(repository, storage);
  const worktree = createTaskWorktree(target, 'thread_merge_A', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'A worker failed\nmiddle\nbeta\n');
  worktree.headCommit = commitTaskChanges(worktree, 'approved with dead helper');
  const store = new TaskStore(storage);
  store.upsertTask(thread('thread_merge_A', worktree));
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const lock = new MeshFileLock(path.join(root, 'merge.lock'));
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, lock);
  const track = trackValidationMerges(t, root, signals, store, control);
  const originalAttach = lock.attachChildPid.bind(lock);
  let validationPid: number | undefined;
  let started: Promise<void>;
  lock.attachChildPid = async (pid, cancelled) => {
    if (new TaskExecutionRegistry(storage).getOwner('thread_merge_A')?.agent !== 'merge-validation') return originalAttach(pid, cancelled);
    validationPid = pid;
    await started;
    const helperPid = JSON.parse(fs.readFileSync(`${lock.lockPath}.owner.json`, 'utf8')).helperPid as number;
    process.kill(helperPid, 'SIGKILL');
    await originalAttach(pid, cancelled);
  };
  const coordinator = new MergeCoordinator(store, storage, lock);
  const resultPromise = track('thread_merge_A', coordinator.mergeApproved('thread_merge_A'));
  started = waitForValidationStart(path.join(signals, 'thread_merge_A.entered'), resultPromise);
  await started;
  const result = await resultPromise;
  assert.equal(result.status, 'validation_failed');
  assert.match(result.message, /helper exited|acknowledge|timed out/i);
  assert.ok(validationPid);
  try { process.kill(validationPid!, 0); assert.fail('validation process must be terminated after child tracking fails'); }
  catch (error) { assert.equal((error as NodeJS.ErrnoException)?.code, 'ESRCH'); }
  assert.equal(gitOutput(repository, ['rev-parse', target.targetRef]), target.baseCommit);
  assert.equal(store.getTask('thread_merge_A')?.status, 'reviewing');
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
  process.env.MESH_VALIDATION_SIGNAL_DIR = signals;
  const lock = new MeshFileLock(path.join(root, 'merge.lock')); const coordinator = new MergeCoordinator(store, storage, lock);
  const control = new TaskControlService(store, undefined, undefined, storage, undefined, lock);
  const track = trackValidationMerges(t, root, signals, store, control);
  const a = createTaskWorktree(target, 'thread_merge_A', storage); fs.writeFileSync(path.join(a.path, 'shared.txt'), 'A paused\nmiddle\nbeta\n'); a.headCommit = commitTaskChanges(a, 'A');
  store.upsertTask(thread('thread_merge_A', a)); process.env.MESH_VALIDATION_HOLD_THREAD = 'thread_merge_A';
  const mergeA = track('thread_merge_A', coordinator.mergeApproved('thread_merge_A'));
  await waitForValidationStart(path.join(signals, 'thread_merge_A.entered'), mergeA);
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
  const mergeB = track('thread_merge_B', coordinator.mergeApproved('thread_merge_B'));
  await waitForValidationStart(path.join(signals, 'thread_merge_B.entered'), mergeB);
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
  assert.equal(store.getTask('thread_waiter_pause')!.status, 'queued');
  assert.equal(store.getTask('thread_waiter_pause')!.mergeHistory, undefined);
});
