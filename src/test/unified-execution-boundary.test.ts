import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { spawn } from 'node:child_process';

import { TaskStore } from '../bridge/task-store';
import { ExecutionController } from '../bridge/execution-controller';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskControlService } from '../bridge/task-control';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { ReworkController } from '../bridge/rework-controller';
import { waitFor, waitForExit } from './git-worktree-fixture';
import { DelegationThread } from '../bridge/types';
import {
  getProcessBirthId,
  getProcessBirthIdAsync,
  setProcessBirthIdProvider,
  probeProcessIdentity,
  confirmProcessTreeExit,
} from '../bridge/platform';
import { captureChildDescendants, registerRunChildV2 } from '../bridge/process-binding';
import { SessionOwnershipStore } from '../bridge/session-ownership';
import {
  createThread,
  setupFixture,
} from './unified-execution-fixture';

async function waitForText(read: () => string | undefined, timeoutMs: number): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error('Timed out waiting for controlled child output');
}

describe('Unified Execution Boundary Tests', () => {
  test('v2 pause retains an unbound spawn-intent run and reports pending stop', async () => {
    const fixture = await setupFixture();
    let lateChild: ReturnType<typeof spawn> | undefined;
    try {
      const thread = createThread('thread_v2_pause_spawn_window', 'develop');
      thread.status = 'running';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 4, operationId: 'pause-spawn-op', generation: 3,
        run: {
          id: 'pause-spawn-run', role: 'developer',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          step: 'spawning', state: 'starting', spawnIntentAt: new Date().toISOString(), startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);
      const service = new TaskControlService(fixture.store, fixture.runner as any,
        new CancellationRegistry(fixture.dir), fixture.dir);
      const result = await service.pauseTask(thread.threadId, 'pause at spawn registration', 'test');
      assert.equal(result.status, 'STOPPING', 'the public control result reports unresolved stop evidence');
      const paused = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(paused.execution?.run?.id, 'pause-spawn-run');
      assert.equal(paused.execution?.run?.state, 'stopping');
      assert.equal(paused.status, 'running', 'pause is not reported complete while spawn exit is unconfirmed');
      assert.equal(paused.execution?.generation, 3);
      await assert.rejects(fixture.controller.resumeOperation(thread.threadId), /previous run exit is not confirmed|state running is not resumable/);
      lateChild = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { lateChild!.once('error', reject); lateChild!.once('spawn', resolve); });
      const lateBirthId = getProcessBirthId(lateChild.pid!);
      assert.ok(lateBirthId);
      const late = await registerRunChildV2({
        taskStore: fixture.store,
        threadId: thread.threadId,
        identity: { runId: 'pause-spawn-run', operationId: 'pause-spawn-op', generation: 3, phase: 'develop', round: 1 },
        childPid: lateChild.pid!, childBirthId: lateBirthId, childProcess: lateChild,
      });
      assert.equal(late.success, false, 'late spawn binding cannot revive a stopping run');
      assert.equal(fixture.store.reloadTask(thread.threadId)?.execution?.run?.id, 'pause-spawn-run');
      await assert.rejects(service.deleteTask(thread.threadId, 'concurrent delete at spawn window', 'test', true), /has not confirmed complete process-tree and resource exit/);
      const deleting = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(deleting.execution?.run?.id, 'pause-spawn-run');
      assert.equal(deleting.cleanup?.action, 'delete');
      assert.ok(deleting.cleanup?.token);
    } finally {
      if (lateChild && lateChild.exitCode === null && lateChild.signalCode === null) {
        const closed = new Promise<void>((resolve) => lateChild!.once('close', () => resolve()));
        try { lateChild.kill('SIGKILL'); } catch {}
        await closed;
      }
      await fixture.cleanup();
    }
  });

  test('v2 pause never signals a legacy-registry-only unrelated PID', async () => {
    const fixture = await setupFixture();
    let unrelated: ReturnType<typeof spawn> | undefined;
    try {
      const thread = createThread('thread_v2_pause_legacy_registry', 'develop');
      thread.status = 'queued';
      thread.execution = { ...thread.execution!, version: 2, revision: 2, operationId: 'no-run-op', run: undefined };
      fixture.store.upsertTask(thread);
      unrelated = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { unrelated!.once('error', reject); unrelated!.once('spawn', resolve); });
      const registration = fixture.registry.begin(thread.threadId, 1, 'legacy-observer');
      fixture.registry.attachChildPid(unrelated.pid!, registration);

      const service = new TaskControlService(fixture.store, fixture.runner as any, new CancellationRegistry(fixture.dir), fixture.dir);
      const result = await service.pauseTask(thread.threadId, 'pause queued v2', 'test');
      assert.equal(result.status, 'PAUSED');
      assert.equal(fixture.store.reloadTask(thread.threadId)?.status, 'paused');
      assert.equal(unrelated.exitCode, null, 'an observational legacy registry entry is not stop authority');
      assert.equal(unrelated.signalCode, null);
      assert.equal(fixture.registry.listOwners().some((owner) => owner.threadId === thread.threadId), true);

      const deletable = createThread('thread_v2_delete_legacy_registry', 'develop');
      deletable.status = 'completed';
      deletable.execution = { ...deletable.execution!, version: 2, revision: 2, operationId: 'delete-no-run-op', run: undefined };
      fixture.store.upsertTask(deletable);
      const deleteRegistration = fixture.registry.begin(deletable.threadId, 1, 'legacy-observer');
      fixture.registry.attachChildPid(unrelated.pid!, deleteRegistration);
      const deleted = await service.deleteTask(deletable.threadId, 'delete historical v2 ledger', 'test', true);
      assert.equal(deleted.status, 'DELETED');
      assert.equal(unrelated.exitCode, null, 'delete also ignores legacy registry-only PID observations for v2');
    } finally {
      if (unrelated && unrelated.exitCode === null && unrelated.signalCode === null) {
        const closed = new Promise<void>((resolve) => unrelated!.once('close', () => resolve()));
        try { unrelated.kill('SIGKILL'); } catch {}
        await closed;
      }
      await fixture.cleanup();
    }
  });

  test('v2 pause and delete retain cleanup while a captured descendant outlives the root', async () => {
    const fixture = await setupFixture();
    let root: ReturnType<typeof spawn> | undefined;
    let descendant: number | undefined;
    let owner: ReturnType<typeof spawn> | undefined;
    const cleanupProcesses = async () => {
      for (const child of [root, owner]) {
        if (child && child.exitCode === null && child.signalCode === null) {
          const closed = new Promise<void>((resolve) => child.once('close', () => resolve()));
          try { child.kill('SIGKILL'); } catch {}
          await closed;
        }
      }
      if (descendant && probeProcessIdentity(descendant) !== 'exited') {
        try { process.kill(descendant, 'SIGKILL'); } catch {}
      }
    };
    try {
      root = spawn(process.execPath, ['-e',
        "const {spawn}=require('child_process');const c=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{detached:true,stdio:'ignore',windowsHide:true});console.log(c.pid);setTimeout(()=>process.exit(0),800);",
      ], { stdio: ['ignore', 'pipe', 'ignore'], windowsHide: true });
      await new Promise<void>((resolve, reject) => { root!.once('error', reject); root!.once('spawn', resolve); });
      const rootPid = root.pid!;
      const rootBirthId = getProcessBirthId(rootPid)!;
      let stdout = '';
      root.stdout!.on('data', (chunk) => { stdout += chunk.toString(); });
      await waitForText(() => stdout.trim().split(/\s+/).at(-1), 5000);
      descendant = Number(stdout.trim().split(/\s+/).at(-1));
      assert.ok(descendant && getProcessBirthId(descendant));
      const descendantBirthId = getProcessBirthId(descendant!);
      assert.ok(descendantBirthId, 'the root emitted the spawned descendant identity');
      const knownDescendants = [{ pid: descendant!, birthId: descendantBirthId }];
      const rootClosed = new Promise<void>((resolve) => root!.once('close', () => resolve()));
      await rootClosed;
      assert.equal(probeProcessIdentity(descendant!), 'alive', 'the detached descendant remains alive after its captured root exits');

      owner = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { owner!.once('error', reject); owner!.once('spawn', resolve); });
      const ownerBirthId = getProcessBirthId(owner.pid!)!;
      const thread = createThread('thread_v2_pause_orphan_descendant', 'develop');
      thread.status = 'running';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 4, operationId: 'pause-descendant-op', generation: 2,
        run: {
          id: 'pause-descendant-run', role: 'developer', owner: { pid: owner.pid!, birthId: ownerBirthId },
          child: { pid: rootPid, birthId: rootBirthId },
          subtrees: [{ root: { pid: rootPid, birthId: rootBirthId }, descendants: knownDescendants }],
          step: 'executing', state: 'active', spawnIntentAt: new Date().toISOString(),
          registeredAt: new Date().toISOString(), startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);
      const service = new TaskControlService(fixture.store, fixture.runner as any,
        new CancellationRegistry(fixture.dir), fixture.dir);
      const result = await service.pauseTask(thread.threadId, 'pause orphaned descendant', 'test');
      assert.equal(result.status, 'STOPPING');
      const paused = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(paused.execution?.run?.id, 'pause-descendant-run');
      assert.equal(paused.execution?.run?.state, 'stopping');
      assert.equal(probeProcessIdentity(descendant!), 'exited', 'only the durably captured descendant is stopped');

      const cleanupDir = path.join(fixture.dir, 'must-remain');
      fs.mkdirSync(cleanupDir, { recursive: true });
      fs.writeFileSync(path.join(cleanupDir, 'evidence.txt'), 'keep until run settled');
      await assert.rejects(service.deleteTask(thread.threadId, 'delete with unconfirmed owner', 'test', true), /has not confirmed complete process-tree and resource exit/);
      const retained = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(retained.execution?.run?.id, 'pause-descendant-run');
      assert.equal(retained.cleanup?.action, 'delete');
      assert.ok(retained.cleanup?.token);
      const cleanupToken = retained.cleanup!.token;
      assert.equal(fs.existsSync(path.join(cleanupDir, 'evidence.txt')), true, 'cleanup assets remain while the run is protected');

      const ownerClosed = new Promise<void>((resolve) => owner!.once('close', () => resolve()));
      owner.kill('SIGKILL');
      await ownerClosed;
      const deleted = await service.deleteTask(thread.threadId, 'retry the same published deletion', 'test', true);
      assert.equal(deleted.status, 'DELETED');
      assert.equal(fixture.store.reloadTask(thread.threadId), undefined);
      const tombstones = fs.readFileSync(path.join(fixture.dir, 'deleted_tasks.log'), 'utf8');
      assert.ok(tombstones.includes(cleanupToken), 'retry commits the original cleanup token after exit proof');
    } finally {
      await cleanupProcesses();
      await fixture.cleanup();
    }
  });

  test('v2 retry and resume retain a live run; direct start respects future and unverifiable notBefore', async () => {
    const fixture = await setupFixture({ wallClockNow: () => Date.now() });
    let child: ReturnType<typeof spawn> | undefined;
    try {
      child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { child!.once('error', reject); child!.once('spawn', resolve); });
      const childPid = child.pid!;
      const childBirthId = getProcessBirthId(childPid);
      assert.ok(childBirthId);
      const thread = createThread('thread_v2_live_retry', 'develop');
      thread.status = 'failed';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 10, operationId: 'live-op', phase: 'develop', round: 1,
        generation: 4, attempt: 2,
        run: {
          id: 'live-run', role: 'developer', owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          child: { pid: childPid, birthId: childBirthId },
          subtrees: [{ root: { pid: childPid, birthId: childBirthId }, descendants: [] }],
          state: 'active', step: 'executing', startedAt: new Date().toISOString(),
          spawnIntentAt: new Date().toISOString(), registeredAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);
      const beforeCalls = fixture.runner.executions.length;
      await assert.rejects(fixture.controller.retryOperation(thread.threadId), /previous run exit is not confirmed/);
      const afterRetry = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(afterRetry.execution?.run?.id, 'live-run');
      assert.equal(afterRetry.execution?.generation, 4);
      assert.equal(fixture.runner.executions.length, beforeCalls);

      const paused = createThread('thread_v2_live_resume', 'develop');
      paused.status = 'paused';
      paused.execution = { ...thread.execution!, operationId: 'live-resume-op', run: { ...thread.execution!.run!, id: 'live-resume-run' } };
      fixture.store.upsertTask(paused);
      await assert.rejects(fixture.controller.resumeOperation(paused.threadId), /previous run exit is not confirmed/);
      assert.equal(fixture.store.reloadTask(paused.threadId)?.execution?.run?.id, 'live-resume-run');
      assert.equal(fixture.store.reloadTask(paused.threadId)?.execution?.generation, 4);
      assert.equal(fixture.runner.executions.length, beforeCalls);

      for (const [id, notBefore] of [
        ['thread_v2_future_queue', new Date(Date.now() + 600_000).toISOString()],
        ['thread_v2_unknown_queue', 'not-a-date'],
      ] as const) {
        const queued = createThread(id, 'develop');
        queued.execution = {
          ...queued.execution!, version: 2, revision: 1, operationId: `${id}-op`,
          queue: { reason: id.includes('unknown') ? 'backoff' : 'quota', notBefore },
          next: { phase: 'develop', round: 1 },
        };
        fixture.store.upsertTask(queued);
        const [result] = await Promise.all([
          fixture.controller.startExecution(id, 'develop'),
          fixture.controller.scanAndDrain(),
        ]);
        assert.equal(result.started, false);
        assert.equal(result.reason, id.includes('unknown') ? 'not_before_unverifiable' : 'not_before');
        const persisted = fixture.store.reloadTask(id)!;
        assert.equal(persisted.status, 'queued');
        assert.equal(persisted.execution?.queue?.notBefore, notBefore);
        assert.equal(persisted.execution?.attempt, 0);
      }
      assert.equal(fixture.runner.executions.length, beforeCalls, 'rejected controls do not launch a replacement child');
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) {
        const closed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
        try { child.kill('SIGKILL'); } catch {}
        await closed;
      }
      await fixture.cleanup();
    }
  });

  test('orphaned v2 run consumes a matching durable next once after owner and child identities exit', async () => {
    const fixture = await setupFixture();
    const secondStore = new TaskStore(fixture.dir);
    const secondRegistry = new TaskExecutionRegistry(fixture.dir);
    const secondController = new ExecutionController(
      secondStore, fixture.runner as any, fixture.callbackManager, undefined, secondRegistry, { storageDir: fixture.dir, standingScanIntervalMs: 0 } as any,
    );
    let developerChildren = 0;
    let child: ReturnType<typeof spawn> | undefined;
    try {
      const thread = createThread('thread_v2_orphan_handoff', 'develop');
      thread.status = 'running';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 10, operationId: 'old-develop-op', phase: 'develop', round: 1,
        run: {
          id: 'orphan-source-run', role: 'developer', owner: { pid: 99_999_999, birthId: 'owner-dead' },
          child: { pid: 99_999_998, birthId: 'child-dead' }, state: 'active', step: 'executing',
          subtrees: [{ root: { pid: 99_999_998, birthId: 'child-dead' }, descendants: [] }],
          startedAt: new Date().toISOString(), registeredAt: new Date().toISOString(),
        },
        next: { phase: 'develop', round: 1, sourceRunId: 'orphan-source-run' },
      };
      assert.ok(new SessionOwnershipStore(fixture.dir).claim({
        agent: thread.targetAgent, sessionId: thread.receiverSessionId, threadId: thread.threadId,
        role: 'developer', sessionEpoch: 1,
      }).ok, 'the captured worker session has a durable owner');
      fixture.store.upsertTask(thread);
      (fixture.runner as any).execute = async (options: any) => {
        developerChildren++;
        child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
        await new Promise<void>((resolve, reject) => { child!.once('error', reject); child!.once('spawn', resolve); });
        const pid = child!.pid!;
        const birthId = getProcessBirthId(pid);
        assert.ok(birthId, 'new child has spawn-time birthId');
        await options.onChildPid(pid, child, birthId);
        const closed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
        child!.kill('SIGKILL');
        await closed;
        await options.onChildPid(undefined);
        return { exitCode: 0, stdout: 'developed', stderr: '', extractedOutput: 'developed', durationMs: 1,
          actualSessionId: thread.receiverSessionId,
          assessment: { success: true, exitCode: 0, stream: { hasActualOutput: true, chunkCount: 1, stdoutBytes: 9, stderrBytes: 0, continuous: true, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60_000 } } };
      };

      const [first, competing] = await Promise.all([
        fixture.controller.startExecution(thread.threadId, 'develop'),
        secondController.startExecution(thread.threadId, 'develop'),
      ]);
      assert.ok(first.started || competing.started, 'one controller claims the orphaned handoff');
      await fixture.controller.waitForActiveExecutions(15_000);
      await secondController.waitForActiveExecutions(15_000);
      const recovered = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(developerChildren, 1, 'matching next is consumed once');
      assert.notEqual(recovered.execution?.run?.id, 'orphan-source-run');
      assert.notEqual(recovered.execution?.next?.sourceRunId, 'orphan-source-run',
        'the recovered phase handoff does not survive as an unconsumed source run');

      const uncertain = createThread('thread_v2_unknown_owner', 'develop');
      uncertain.status = 'running';
      uncertain.execution = {
        ...uncertain.execution!, version: 2, revision: 20, operationId: 'unknown-op', phase: 'develop', round: 1,
        run: {
          id: 'unknown-owner-run', role: 'developer', owner: { pid: 99_999_997 },
          state: 'active', step: 'executing', startedAt: new Date().toISOString(),
        },
        next: { phase: 'develop', round: 1, sourceRunId: 'unknown-owner-run' },
      };
      fixture.store.upsertTask(uncertain);
      const refused = await secondController.startExecution(uncertain.threadId, 'develop');
      assert.equal(refused.started, false);
      assert.equal(refused.reason, 'run_busy');
      assert.equal(fixture.store.reloadTask(uncertain.threadId)?.execution?.run?.id, 'unknown-owner-run',
        'unknown owner identity retains the existing run protection');
    } finally {
      if (child && child.exitCode === null && child.signalCode === null) {
        const closed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
        try { child.kill('SIGKILL'); } catch {}
        await closed;
      }
      secondController.dispose();
      secondRegistry.dispose();
      secondStore.dispose();
      await fixture.cleanup();
    }
  });

  test('v2 callback wake reads the fixed reviewer session and launches only through the current run', async () => {
    const fixture = await setupFixture();
    try {
      const task = createThread('thread_v2_callback_wake', 'review');
      task.status = 'reviewing';
      task.rounds[0].status = 'completed';
      task.execution!.version = 2;
      assert.ok(new SessionOwnershipStore(fixture.dir).claim({
        agent: task.initiatorAgent, sessionId: task.reviewerSessionId!, threadId: task.threadId,
        role: 'reviewer', sessionEpoch: 1,
      }).ok);
      fixture.store.upsertTask(task);
      let startedRun: string | undefined;
      fixture.callbackManager.launchCaller = (async (_task: unknown, _round: unknown, _onChildPid: unknown, identity?: { runId: string }) => {
        startedRun = identity?.runId;
        assert.equal(startedRun, fixture.store.reloadTask(task.threadId)?.execution?.run?.id);
        assert.ok(startedRun);
        return { success: false, error: 'controlled reviewer returned without verdict' };
      }) as typeof fixture.callbackManager.launchCaller;
      assert.equal(await fixture.callbackManager.notifyCaller(task, task.rounds[0]), true);
      await fixture.controller.waitForActiveExecutions(5_000);
      assert.ok(startedRun);
      assert.equal(fixture.roleLock.getOwner(task.threadId), undefined);
    } finally {
      await fixture.cleanup();
    }
  });

  test('returned native maintenance keeps cleanup blocked until its persistent descendant tree exits', async () => {
    const fixture = await setupFixture();
    const secondStore = new TaskStore(fixture.dir);
    const secondRegistry = new TaskExecutionRegistry(fixture.dir);
    const secondController = new ExecutionController(
      secondStore, fixture.runner as any, fixture.callbackManager, undefined, secondRegistry, { storageDir: fixture.dir, standingScanIntervalMs: 0 } as any,
    );
    const pidFile = path.join(fixture.dir, 'maintenance-grandchild.pid');
    const stopFile = path.join(fixture.dir, 'maintenance-root.stop');
    let root: ReturnType<typeof spawn> | undefined;
    let known: Array<{ pid: number; birthId?: string }> = [];
    let cleanupToken: string | undefined;
    try {
      const thread = createThread('thread_v2_returned_native_maintenance', 'develop');
      thread.status = 'paused';
      const threadOwnerBirth = getProcessBirthId(process.pid);
      assert.ok(threadOwnerBirth);
      const childCode = `const fs=require('fs'),cp=require('child_process');const p=cp.spawn(process.execPath,['-e',${JSON.stringify('setInterval(()=>{},1000)')}],{detached:true,stdio:'ignore'});p.unref();fs.writeFileSync(${JSON.stringify(pidFile)},String(p.pid));const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(stopFile)})){clearInterval(t);process.exit(0)}},10);`;
      root = spawn(process.execPath, ['-e', childCode], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { root!.once('error', reject); root!.once('spawn', resolve); });
      const rootPid = root.pid!;
      const rootBirth = await getProcessBirthIdAsync(rootPid);
      assert.ok(rootBirth);
      const pidDeadline = Date.now() + 8_000;
      while (!fs.existsSync(pidFile) && Date.now() < pidDeadline) await new Promise((resolve) => setTimeout(resolve, 10));
      assert.ok(fs.existsSync(pidFile), 'controlled child created a detached descendant');
      const descendantPid = Number(fs.readFileSync(pidFile, 'utf8'));
      const descendantBirth = await getProcessBirthIdAsync(descendantPid);
      assert.ok(descendantBirth);
      const descendants = await captureChildDescendants(rootPid, rootBirth);
      assert.ok(descendants.some((item) => item.pid === descendantPid && item.birthId === descendantBirth));
      known = [{ pid: rootPid, birthId: rootBirth }, ...descendants];
      thread.execution = {
        ...thread.execution!, version: 2, revision: 2, operationId: 'native-maintenance-op', phase: 'develop', round: 1,
        generation: 8, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 12_000, maxAutoRetries: 2 },
        run: {
          id: 'returned-native-maintenance-run', purpose: 'native_maintenance', role: 'developer',
          owner: { pid: process.pid, birthId: threadOwnerBirth }, child: { pid: rootPid, birthId: rootBirth },
          subtrees: [{ root: { pid: rootPid, birthId: rootBirth }, descendants, sampledAt: new Date().toISOString() }],
          step: 'native_maintenance', state: 'stopping', unconfirmed: true, settlementPending: true,
          spawnIntentAt: new Date().toISOString(), registeredAt: new Date().toISOString(), startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);
      const marked = await fixture.store.markTaskCleanup(thread.threadId, 'delete', { pid: process.pid, birthId: threadOwnerBirth });
      cleanupToken = marked.token;
      const rootClosed = new Promise<void>((resolve) => root!.once('close', () => resolve()));
      fs.writeFileSync(stopFile, 'exit');
      await rootClosed;
      assert.equal(probeProcessIdentity(descendantPid, descendantBirth), 'alive');
      await secondController.scanAndDrain();
      assert.equal(secondStore.reloadTask(thread.threadId)?.execution?.run?.id, 'returned-native-maintenance-run',
        'a live detached descendant retains the maintenance run after the adapter returned');
      await assert.rejects(fixture.store.commitTaskCleanup(thread.threadId, cleanupToken, 'delete'), /execution run .* is still recorded/);

      for (const identity of known) if (identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'alive') {
        try { process.kill(identity.pid); } catch {}
      }
      const exitDeadline = Date.now() + 8_000;
      while (known.some((identity) => !identity.birthId || probeProcessIdentity(identity.pid, identity.birthId) !== 'exited') && Date.now() < exitDeadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.ok(known.every((identity) => !!identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'exited'));
      await secondController.scanAndDrain();
      assert.equal(secondStore.reloadTask(thread.threadId)?.execution?.run, undefined, 'a second controller settles the returned run after full tree exit');
      assert.equal(await secondStore.commitTaskCleanup(thread.threadId, cleanupToken, 'delete'), true,
        'cleanup can commit only after the scanner persisted full-tree exit settlement');
    } finally {
      if (root?.pid && root.exitCode === null && root.signalCode === null) { try { root.kill(); } catch {} }
      for (const identity of known) if (identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'alive') {
        try { process.kill(identity.pid); } catch {}
      }
      secondController.dispose(); secondRegistry.dispose(); secondStore.dispose();
      await fixture.cleanup();
    }
  });

  // Scenario 7: Process identity probing with birthId prevents PID reuse
  test('Scenario 7: Mismatched birthId identifies exited process despite reused PID', async () => {
    const fixture = await setupFixture();
    try {
      // Invalidate birthId provider to mock PID reuse
      setProcessBirthIdProvider((pid) => (pid === 99999 ? 'birth_new_process' : 'birth_original'));

      const probe = probeProcessIdentity(99999, 'birth_old_crashed_process');
      assert.strictEqual(probe, 'exited');
    } finally {
      setProcessBirthIdProvider(undefined);
      await fixture.cleanup();
    }
  });

  // Scenario 9: TaskStore transitionExecution cross-process CAS competition on same revision
  test('Scenario 9: cross-process transitionExecution CAS race: only one succeeds, loser cannot overwrite, 3rd instance verifies', async () => {
    const fixture = await setupFixture();
    const barrierDir = fs.mkdtempSync(path.join(fixture.dir, 's9-barrier-'));
    const readyFile = path.join(barrierDir, 'child_ready');
    const goFile = path.join(barrierDir, 'go');
    const childResultFile = path.join(barrierDir, 'child_result.json');
    let child: ReturnType<typeof spawn> | undefined;

    try {
      const thread = createThread('thread_s9', 'develop');
      fixture.store.upsertTask(thread);

      // Child process competing for same revision 1 on same thread
      const taskStorePath = require.resolve('../bridge/task-store');
      const childCode = `
        const fs = require('fs');
        const { TaskStore } = require(${JSON.stringify(taskStorePath)});
        (async () => {
          try {
            const store = new TaskStore(${JSON.stringify(fixture.dir)});
            fs.writeFileSync(${JSON.stringify(readyFile)}, 'ready');
            while (!fs.existsSync(${JSON.stringify(goFile)})) {
              await new Promise((r) => setTimeout(r, 10));
            }
            try {
              const res = await store.transitionExecution('thread_s9', 1, (t) => {
                t.summary.lastOutputSnippet = 'child_winner';
              });
              fs.writeFileSync(${JSON.stringify(childResultFile)}, JSON.stringify({ ok: true, revision: res.execution?.revision }));
              store.dispose();
              process.exit(0);
            } catch (err) {
              fs.writeFileSync(${JSON.stringify(childResultFile)}, JSON.stringify({ ok: false, error: err.message }));
              store.dispose();
              process.exit(1);
            }
          } catch (topErr) {
            fs.writeFileSync(${JSON.stringify(childResultFile)}, JSON.stringify({ ok: false, error: topErr.message }));
            process.exit(2);
          }
        })();
      `;

      child = spawn(process.execPath, ['-e', childCode], {
        windowsHide: true,
        timeout: 30_000,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      child.stdout!.resume();
      child.stderr!.pipe(process.stderr);

      // Wait until child is ready
      await waitFor(readyFile, 1000);

      // Signal 'go' to both child and parent concurrently
      fs.writeFileSync(goFile, 'go');

      // Parent concurrently attempts CAS transition on revision 1
      let parentOk = false;
      let parentErr = '';
      try {
        await fixture.store.transitionExecution('thread_s9', 1, (t) => {
          t.summary.lastOutputSnippet = 'parent_winner';
        });
        parentOk = true;
      } catch (err: any) {
        parentErr = err.message;
      }

      // Wait for child to exit
      // The child may exit while the parent's CAS is still acquiring its lock.
      await waitForExit(child);
      assert.strictEqual(child.exitCode, parentOk ? 1 : 0);

      const childRes = JSON.parse(fs.readFileSync(childResultFile, 'utf8'));

      // Exactly one must have succeeded
      assert.strictEqual(
        parentOk !== childRes.ok,
        true,
        `Expected exactly one process to win CAS race. Parent: ${parentOk} (${parentErr}), Child: ${childRes.ok} (${childRes.error})`,
      );

      // The loser MUST have failed with revision mismatch
      const loserErr = parentOk ? childRes.error : parentErr;
      assert.match(loserErr, /revision mismatch/i);

      // Third independent TaskStore instance reloads from disk and verifies winning state
      const store3 = new TaskStore(fixture.dir);
      store3.ensureUpToDate();
      const finalTask = store3.getTask('thread_s9')!;
      assert.strictEqual(finalTask.execution?.revision, 2);
      const expectedWinner = parentOk ? 'parent_winner' : 'child_winner';
      assert.strictEqual(finalTask.summary.lastOutputSnippet, expectedWinner);
      store3.dispose();
    } finally {
      if (child) {
        child.kill();
        await waitForExit(child);
      }
      await fixture.cleanup();
      try { fs.rmSync(barrierDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 13: Token usage components, sessions, snapshots, targets preserved
  test('Scenario 13: TaskStore upsertTask preserves execution record and does not overwrite with stale copies', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s13', 'develop');
      fixture.store.upsertTask(thread);

      // Update execution in store
      await fixture.store.transitionExecution('thread_s13', 1, (t) => {
        t.execution!.attempt = 3;
        t.execution!.revision = 2;
      });

      // Older task copy lacking latest execution
      const staleCopy = structuredClone(thread);
      staleCopy.execution!.attempt = 1;
      staleCopy.execution!.revision = 1;

      // Upserting stale copy preserves the newer execution record
      fixture.store.upsertTask(staleCopy);

      const fresh = fixture.store.getTask('thread_s13');
      assert.strictEqual(fresh?.execution?.attempt, 3);
      assert.strictEqual(fresh?.execution?.revision, 2);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 20: Alive run identity preservation and stale callback rejection with empty run
  test('Scenario 20: Alive run identity preservation and stale callback rejection with empty run', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s20', 'develop');
      // Set run pointing to process.pid with real birthId
      const realBirthId = getProcessBirthId(process.pid);
      thread.execution!.run = {
        id: 'run_alive_1',
        owner: { pid: process.pid, birthId: realBirthId },
        step: 'executing',
        state: 'active',
        startedAt: new Date().toISOString(),
      };
      fixture.store.upsertTask(thread);

      // Calling startExecution must preserve the active run.
      const startRes = await fixture.controller.startExecution('thread_s20', 'develop');
      assert.strictEqual(startRes.started, true);
      assert.strictEqual(startRes.reason, 'already_running');
      assert.strictEqual(fixture.runner.executions.length, 0);

      // Verify execution.run is STILL present and NOT wiped!
      const fresh = fixture.store.getTask('thread_s20');
      assert.strictEqual(fresh?.execution?.run?.id, 'run_alive_1');

      // Now test: task has generation=2, operationId='new-operation', status='queued', run=undefined, spentActiveMs=5000
      const rev = fresh!.execution!.revision;
      await fixture.store.transitionExecution('thread_s20', rev, (t) => {
        t.status = 'queued';
        t.execution!.generation = 2;
        t.execution!.operationId = 'new-operation';
        t.execution!.run = undefined;
        t.execution!.budget.spentActiveMs = 5000;
      });

      // Call handlePhaseOutcome with old runId and generation=1
      await (fixture.controller as any).handlePhaseOutcome(
        'thread_s20',
        'develop',
        'old-generation-run',
        { ok: true },
        { generation: 1, operationId: 'old-op' },
      );

      // Verify phase was NOT changed to review, spentActiveMs was NOT reset to 0, status is STILL queued!
      const afterStale = fixture.store.getTask('thread_s20');
      assert.strictEqual(afterStale?.status, 'queued');
      assert.strictEqual(afterStale?.execution?.phase, 'develop');
      assert.strictEqual(afterStale?.execution?.budget.spentActiveMs, 5000);
      assert.strictEqual(afterStale?.execution?.generation, 2);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 21: MergeCoordinator lock sharing and external lease avoids self-deadlock
  test('Scenario 21: MergeCoordinator lock sharing and external lease avoids self-deadlock', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s21', 'merge');
      thread.status = 'queued';
      thread.queuePhase = 'merge';
      thread.worktree = {
        path: thread.workspace.executionPath,
        mainPath: thread.workspace.rootPath,
        repositoryPath: thread.workspace.rootPath,
        baseCommit: 'commit_base',
        headCommit: 'commit_head',
        approvedCommit: 'commit_approved',
        targetRef: 'refs/heads/master',
        branch: 'mesh/task/thread_s21',
      };
      fixture.store.upsertTask(thread);

      // In controller, mergeLock is shared from mergeCoordinator.getLock()
      assert.strictEqual(
        (fixture.controller as any).mergeLock,
        (fixture.controller as any).mergeCoordinator.getLock(),
        'ExecutionController and MergeCoordinator must share identical MeshFileLock instance'
      );

      // Acquire lock with lease
      const release = await (fixture.controller as any).mergeLock.acquire();
      assert.ok(release, 'Merge lock acquired');

      // executeMergeAdapter with external lease must not deadlock on lock acquisition
      let mockGitCalled = false;
      (fixture.controller as any).mergeCoordinator.mergeApprovedAttempt = async (
        id: string,
        onPid?: any,
        extLease?: any,
      ) => {
        mockGitCalled = true;
        assert.ok(extLease, 'externalLease must be passed to mergeApprovedAttempt');
        return { status: 'merged', message: 'OK' };
      };

      const outcome = await (fixture.controller as any).executeMergeAdapter(
        'thread_s21',
        'run_merge_1',
        undefined,
        undefined,
        release,
      );
      assert.strictEqual(outcome.ok, true);
      assert.strictEqual(mockGitCalled, true);

      release();
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 22: TaskStore CAS Lock mutual exclusion and immune to mtime backdating while held
  test('Scenario 22: TaskStore CAS Lock mutual exclusion and immune to mtime backdating while held', async () => {
    const fixture = await setupFixture();
    const barrierDir = fs.mkdtempSync(path.join(fixture.dir, 's22-barrier-'));
    const inLockFile = path.join(barrierDir, 'in_lock');
    const releaseLockFile = path.join(barrierDir, 'release_lock');
    let holderChild: ReturnType<typeof spawn> | undefined;

    try {
      const thread = createThread('thread_s22', 'develop');
      fixture.store.upsertTask(thread);

      const lockDir = path.join(fixture.dir, 'execution-locks');
      fs.mkdirSync(lockDir, { recursive: true });
      const lockFile = path.join(lockDir, 'thread_s22.lock');

      // Process 1: Holder that holds execution lock, backdates mtime while held, and ensures competitor cannot steal
      const taskStorePath = require.resolve('../bridge/task-store');
      const holderCode = `
        const fs = require('fs');
        const cp = require('child_process');
        const { TaskStore } = require(${JSON.stringify(taskStorePath)});
        const store = new TaskStore(${JSON.stringify(fixture.dir)});
        store.transitionExecution('thread_s22', 1, (t) => {
          fs.writeFileSync(${JSON.stringify(inLockFile)}, 'held');
          const start = Date.now();
          while (!fs.existsSync(${JSON.stringify(releaseLockFile)}) && Date.now() - start < 15000) {
            cp.spawnSync(process.execPath, ['-e', 'setTimeout(()=>{}, 30)'], { timeout: 100 });
          }
          t.summary.lastOutputSnippet = 'holder_winner';
        }).then(
          () => { store.dispose(); process.exit(0); },
          (err) => { console.error(err); store.dispose(); process.exit(1); }
        );
      `;

      holderChild = spawn(process.execPath, ['-e', holderCode], {
        windowsHide: true,
        timeout: 30_000,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      holderChild.stdout!.resume();
      holderChild.stderr!.pipe(process.stderr);

      // Wait until holder is confirmed inside transitionExecution holding the lock
      await waitFor(inLockFile, 1000);

      // Simulate clock drift / malicious mtime backdating by 30 seconds into the past while lock is actively held
      const past = new Date(Date.now() - 30_000);
      if (fs.existsSync(lockDir)) {
        for (const f of fs.readdirSync(lockDir)) {
          const full = path.join(lockDir, f);
          try { fs.utimesSync(full, past, past); } catch {}
        }
      }

      // Process 2: Attempt concurrent transitionExecution on expected revision 1 while holder is STILL holding the lock
      const store2 = new TaskStore(fixture.dir);
      let store2FailedWithMismatch = false;

      // Start store2 transition in background promise
      const p2 = (async () => {
        try {
          await store2.transitionExecution('thread_s22', 1, (t) => {
            t.summary.lastOutputSnippet = 'competitor_stolen';
          });
        } catch (err: any) {
          if (/revision mismatch/i.test(err.message)) {
            store2FailedWithMismatch = true;
          }
        }
      })();

      // Wait a moment (200ms) to ensure store2 is blocked waiting for lock and HAS NOT stolen the lock
      await new Promise((r) => setTimeout(r, 200));

      // Now release holder child
      fs.writeFileSync(releaseLockFile, 'release');

      // Wait for holder to finish
      await waitForExit(holderChild);
      assert.strictEqual(holderChild.exitCode, 0);

      // Wait for store2 to complete its attempt
      await p2;

      // store2 MUST have failed with revision mismatch because holder advanced revision to 2
      assert.strictEqual(
        store2FailedWithMismatch,
        true,
        'Competitor must fail with revision mismatch and must NOT steal the held lock when mtime is backdated',
      );

      // Third independent instance reloads from disk to verify winning state
      const store3 = new TaskStore(fixture.dir);
      store3.ensureUpToDate();
      const loadedTask = store3.getTask('thread_s22')!;
      assert.strictEqual(loadedTask.execution?.revision, 2);
      assert.strictEqual(loadedTask.summary.lastOutputSnippet, 'holder_winner');
      store3.dispose();

      // Now store2 can perform transition on revision 2 cleanly
      const p2Valid = await store2.transitionExecution('thread_s22', 2, (t) => {
        t.summary.lastOutputSnippet = 'store2_valid';
      });
      assert.strictEqual(p2Valid.summary.lastOutputSnippet, 'store2_valid');
      assert.strictEqual(p2Valid.execution?.revision, 3);
      store2.dispose();
    } finally {
      if (holderChild) {
        holderChild.kill();
        await waitForExit(holderChild);
      }
      await fixture.cleanup();
      try { fs.rmSync(barrierDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 25: Stdio MCP decision termination order (persists decision via CAS first, skips self-kill)
  test('Scenario 25: Stdio MCP decision request persists first and skips self-kill', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s25', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 2,
        revision: 1,
        operationId: 'op_s25',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 0, maxAutoRetries: 2 },
        run: {
          id: 'stdio-run-1',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          // Child PID is process.pid (simulating stdio MCP server inside child tree)
          child: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Calling requestTaskDecision from within child process tree must NOT kill self
      const decision = await fixture.controller.requestTaskDecision('thread_s25', {
        question: 'Need clarification?',
        resumePoint: 'continue_develop',
      });

      assert.ok(decision.id);
      assert.strictEqual(decision.question, 'Need clarification?');

      // Verify decision and wait_decision were persisted via CAS first
      const saved = fixture.store.getTask('thread_s25');
      assert.strictEqual(saved?.status, 'wait_decision');
      assert.strictEqual(saved?.decision?.id, decision.id);
      assert.strictEqual(saved?.decision?.question, 'Need clarification?');
      // Current process is still alive and didn't kill itself
      assert.strictEqual(probeProcessIdentity(process.pid), 'alive');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 26: Recursive descendant process tree exit confirmation with birthId
  test('Scenario 26: confirmProcessTreeExit tracks descendant tree and verifies exit with birthId', async () => {
    // Spawn child that spawns grandchild
    const child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {
      stdio: 'ignore',
    });
    assert.ok(child.pid);
    const birthId = getProcessBirthId(child.pid);

    try {
      // While alive, confirmProcessTreeExit with short timeout returns false
      const aliveCheck = await confirmProcessTreeExit(child.pid, 200, birthId);
      assert.strictEqual(aliveCheck, false);

      // Kill child
      try { child.kill('SIGKILL'); } catch {}

      // Once killed, confirmProcessTreeExit returns true
      const deadCheck = await confirmProcessTreeExit(child.pid, 5000, birthId);
      assert.strictEqual(deadCheck, true);
    } finally {
      try { child.kill('SIGKILL'); } catch {}
    }
  });

  // Scenario 30: Controlled CAS concurrency on rework and approve rejects stale generation/run/status without wiping out new generation
  test('Scenario 30: Controlled CAS concurrency on rework and approve rejects stale generation/run/status without wiping out new generation', async () => {
    const fixture = await setupFixture();
    const reworkController = new ReworkController(
      fixture.store,
      fixture.runner as any,
      fixture.callbackManager,
      fixture.detector,
      fixture.registry,
    );
    try {
      // 1. requestRework: Intercept first CAS attempt, inject generation 2 with active run
      const thread1 = createThread('thread_s30_rework', 'develop');
      thread1.status = 'reviewing';
      thread1.rounds[0].status = 'completed';
      thread1.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_1',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(thread1);

      let injectedReworkRace = false;
      const originalTransition = fixture.store.transitionExecution.bind(fixture.store);
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_rework' && !injectedReworkRace) {
          injectedReworkRace = true;
          // Concurrent writer commits generation 2 with active run!
          const revision = fixture.store.getTask(threadId)!.execution!.revision;
          await originalTransition(threadId, revision, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.generation = 2;
              t.execution.operationId = 'op_s30_gen2';
              t.execution.run = {
                id: 'new-generation-run',
                owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
                state: 'active',
                step: 'executing',
                startedAt: new Date().toISOString(),
              };
            }
            t.status = 'reviewing';
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // requestRework should reject and NOT wipe out generation 2
      await assert.rejects(
        async () => {
          await reworkController.requestRework({
            threadId: 'thread_s30_rework',
            feedback: 'Rework needed for scenario 30',
          });
        },
        /Cannot request rework: execution operation changed concurrently|Cannot request rework: execution generation changed concurrently/,
      );

      // Verify generation 2 and its active run were PRESERVED!
      fixture.store.loadFromDisk();
      const afterReworkConflict = fixture.store.getTask('thread_s30_rework');
      assert.strictEqual(afterReworkConflict?.execution?.generation, 2);
      assert.strictEqual(afterReworkConflict?.execution?.run?.id, 'new-generation-run');
      assert.strictEqual(afterReworkConflict?.status, 'reviewing');

      // 2. approveTask: Intercept first CAS attempt, inject generation 2 with active run
      const thread2 = createThread('thread_s30_approve', 'review');
      thread2.status = 'reviewing';
      thread2.rounds[0].status = 'completed';
      thread2.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_2',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(thread2);

      let injectedApproveRace = false;
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_approve' && !injectedApproveRace) {
          injectedApproveRace = true;
          // Concurrent writer commits generation 2 with active run!
          const revision = fixture.store.getTask(threadId)!.execution!.revision;
          await originalTransition(threadId, revision, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.generation = 2;
              t.execution.operationId = 'op_s30_approve_gen2';
              t.execution.run = {
                id: 'new-gen-run-approve',
                owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
                state: 'active',
                step: 'executing',
                startedAt: new Date().toISOString(),
              };
            }
            t.status = 'reviewing';
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // approveTask should reject and NOT wipe out generation 2
      await assert.rejects(
        async () => {
          await reworkController.approveTask({
            threadId: 'thread_s30_approve',
            remarks: 'Approving scenario 30',
          });
        },
        /Cannot approve task: execution operation changed concurrently|Cannot approve task: execution generation changed concurrently/,
      );

      fixture.store.loadFromDisk();
      const afterApproveConflict = fixture.store.getTask('thread_s30_approve');
      assert.strictEqual(afterApproveConflict?.execution?.generation, 2);
      assert.strictEqual(afterApproveConflict?.execution?.run?.id, 'new-gen-run-approve');
      assert.strictEqual(afterApproveConflict?.status, 'reviewing');

      // 3. Cross with pause: task paused concurrently
      const threadPause = createThread('thread_s30_pause', 'review');
      threadPause.status = 'paused';
      threadPause.rounds[0].status = 'completed';
      threadPause.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_pause',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(threadPause);

      await assert.rejects(
        async () => {
          await reworkController.requestRework({ threadId: 'thread_s30_pause', feedback: 'test pause' });
        },
        /paused/,
      );
      await assert.rejects(
        async () => {
          await reworkController.approveTask({ threadId: 'thread_s30_pause', remarks: 'test pause' });
        },
        /paused/,
      );

      // 4. Cross with wait_decision: task in wait_decision
      const threadDecision = createThread('thread_s30_decision', 'review');
      threadDecision.status = 'wait_decision';
      threadDecision.rounds[0].status = 'completed';
      threadDecision.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_dec',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: undefined,
      };
      fixture.store.upsertTask(threadDecision);

      await assert.rejects(
        async () => {
          await reworkController.requestRework({ threadId: 'thread_s30_decision', feedback: 'test dec' });
        },
        /wait_decision/,
      );
      await assert.rejects(
        async () => {
          await reworkController.approveTask({ threadId: 'thread_s30_decision', remarks: 'test dec' });
        },
        /wait_decision/,
      );

      // 5. An unrelated checkpoint may advance the revision while keeping the same decision identity
      fixture.store.transitionExecution = originalTransition;

      const threadCheckpoint = createThread('thread_s30_cp', 'review');
      threadCheckpoint.status = 'reviewing';
      threadCheckpoint.rounds[0].status = 'completed';
      threadCheckpoint.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s30_cp',
        phase: 'review',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-cp',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadCheckpoint);

      let injectedCheckpoint = false;
      let decisionAttempts = 0;
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_cp') decisionAttempts++;
        if (threadId === 'thread_s30_cp' && !injectedCheckpoint) {
          injectedCheckpoint = true;
          // Unrelated checkpoint: budget updated, revision bumped to 2
          const revision = fixture.store.getTask(threadId)!.execution!.revision;
          await originalTransition(threadId, revision, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.budget.spentActiveMs = 2000;
            }
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // The decision should use the latest record and preserve the checkpoint.
      const reworkRes = await reworkController.requestRework({
        threadId: 'thread_s30_cp',
        feedback: 'Valid rework after a budget checkpoint',
      });
      assert.strictEqual(reworkRes.status, 'REWORK_DISPATCHED');
      const taskAfterCp = fixture.store.getTask('thread_s30_cp');
      // Has active run was preserved because reviewer is still running!
      assert.strictEqual(taskAfterCp?.execution?.run?.id, 'run-cp');
      assert.strictEqual(taskAfterCp?.currentRound, 2);
      assert.strictEqual(taskAfterCp?.execution?.budget.spentActiveMs, 2000);
      assert.strictEqual(decisionAttempts, 1);

      const v2 = createThread('thread_s30_v2', 'review');
      v2.execution!.version = 2;
      fixture.store.upsertTask(v2);
      await assert.rejects(
        fixture.store.transitionExecution(v2.threadId, 'current-legacy-review', () => assert.fail('must reject v2 before mutation')),
        /requires eligible legacy execution/,
      );
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 31: recovery CAS cannot clear a concurrently started generation
  test('Scenario 31: the scheduler never attempts to replace a historical generation', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s31', 'develop');
      thread.execution!.version = 1;
      thread.status = 'running';
      fixture.store.upsertTask(thread);
      const before = fixture.store.reloadTask(thread.threadId);
      await fixture.controller.scanAndDrain();
      assert.deepEqual(fixture.store.reloadTask(thread.threadId), before);
      assert.equal(fixture.runner.executions.length, 0);
    } finally {
      await fixture.cleanup();
    }
  });
});
