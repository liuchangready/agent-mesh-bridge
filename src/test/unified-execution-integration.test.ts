import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';

import { MeshFileLock, MeshLockAcquisitionError } from '../bridge/mesh-file-lock';
import { ReworkController } from '../bridge/rework-controller';
import { DEFAULT_EXECUTION_BUDGET_MS, MAX_AUTO_RETRIES } from '../bridge/execution-controller';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { NativeSessionLock } from '../bridge/native-session-lock';
import {
  bindSpawnedChildIdentity,
  getProcessBirthId,
  setProcessBirthIdProvider,
  probeProcessIdentity,
  setKillPidTreeProvider,
  setProcessProbeProvider,
} from '../bridge/platform';
import { testWorkspace } from './workspace-fixture';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { readRecentTaskLogs, roundExecutionLogPath } from '../bridge/round-log';
import { SessionOwnershipStore, nativeSessionNamespace } from '../bridge/session-ownership';
import {
  createThread,
  setupFixture,
} from './unified-execution-fixture';

describe('Unified Execution Integration Tests', () => {
  test('v2 rework runs through AutonomousRunner with its durable run instead of Bridge role/session locks', async () => {
    const fixture = await setupFixture();
    const legacyLock = new NativeSessionLock(fixture.dir);
    let legacyLease: ReturnType<NativeSessionLock['acquire']> | undefined;
    let businessCalls = 0;
    const runnerEvents: string[] = [];
    const childIdentities: Array<{ pid: number; birthId: string }> = [];
    try {
      const thread = createThread('thread_v2_actual_runner_rework', 'develop');
      thread.status = 'queued';
      thread.currentRound = 2;
      thread.artifactDir = path.join(fixture.dir, 'task-artifacts', thread.threadId);
      thread.receiverSessionId = 'captured-receiver-session-v2-rework';
      thread.receiverSessionCaptured = true;
      thread.nativeSessionAssignments = {
        developer: { namespace: nativeSessionNamespace(thread.targetAgent), sessionEpoch: 4 },
      };
      const pinnedNamespace = thread.nativeSessionAssignments.developer!.namespace;
      thread.execution = {
        version: 2, revision: 1, operationId: 'op_v2_actual_runner_rework', phase: 'develop', round: 2,
        generation: 3, attempt: 0, autoRetriesUsed: 0,
        budget: { limitMs: DEFAULT_EXECUTION_BUDGET_MS, spentActiveMs: 0, maxAutoRetries: MAX_AUTO_RETRIES },
      } as any;
      thread.rounds = [{ round: 2, type: 'rework', prompt: 'continue this same session', status: 'running', durationMs: 0, timestamp: new Date().toISOString() }];
      fixture.store.upsertTask(thread);
      const binding = new SessionOwnershipStore(fixture.dir).claim({
        agent: thread.targetAgent, namespace: pinnedNamespace,
        sessionId: thread.receiverSessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 4,
      });
      assert.equal(binding.ok, true);

      // Simulate a stale pre-v2 Bridge lease. The v2 run must not wait on it or
      // rewrite/release it; the static binding and run CAS are authoritative.
      legacyLease = legacyLock.acquire(thread.targetAgent, thread.receiverSessionId);
      assert.ok(legacyLease);
      const legacyLeaseBytes = fs.readFileSync(legacyLease.path, 'utf8');

      const actualRunner = new AutonomousRunner(fixture.detector, undefined, undefined, fixture.store);
      const realExecute = actualRunner.execute.bind(actualRunner);
      (actualRunner as any).execute = async (options: any) => {
        runnerEvents.push('runner.execute:start');
        try { const result = await realExecute(options); runnerEvents.push(`runner.execute:done:${result.exitCode}`); return result; }
        catch (error: any) { runnerEvents.push(`runner.execute:error:${error?.message || error}`); throw error; }
      };
      (actualRunner as any).executeOnce = async (options: any) => {
        businessCalls++;
        runnerEvents.push('executeOnce:start');
        const guard = options.preSpawnGuard?.();
        assert.equal(guard?.ok, true);
        const child = spawn(process.execPath, ['-e', "process.stdin.once('data',()=>{process.stdout.write('controlled rework output');process.exit(0)})"], {
          stdio: ['pipe', 'ignore', 'ignore'], windowsHide: true,
        });
        await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve); });
        const childExit = new Promise<number>((resolve, reject) => {
          child.once('error', reject);
          child.once('close', (code) => resolve(code ?? -1));
        });
        const pid = child.pid!;
        const birthId = getProcessBirthId(pid);
        assert.ok(birthId);
        childIdentities.push({ pid, birthId });
        bindSpawnedChildIdentity(child, birthId);
        await options.onChildPid(pid, child, birthId);
        runnerEvents.push('child:bound');
        await options.onNativeSessionId?.(thread.receiverSessionId, pid);
        options.onStdoutChunk?.('controlled rework output\n');
        options.onStderrChunk?.('controlled rework stderr\n');
        const liveLogs = readRecentTaskLogs(fixture.store, thread.threadId);
        child.stdin!.end('finish');
        const exitCode = await childExit;
        await options.onChildPid(undefined, child, birthId);
        runnerEvents.push('child:closed');
        assert.equal(liveLogs.logFilePath, roundExecutionLogPath(thread.threadId, 2, fixture.dir, undefined, thread.artifactDir));
        assert.equal(liveLogs.isRunning, true);
        assert.equal(liveLogs.logs, 'controlled rework output\ncontrolled rework stderr');
        return {
          exitCode, stdout: 'controlled rework output', stderr: '', extractedOutput: 'controlled rework output',
          actualSessionId: thread.receiverSessionId, durationMs: 1,
          assessment: { success: exitCode === 0, errorCode: exitCode === 0 ? 'OK' : 'NONZERO_EXIT', exitCode,
            stream: { chunkCount: 1, stdoutBytes: 24, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60_000 } },
        };
      };
      (fixture.controller as any).runner = actualRunner;

      const started = await fixture.controller.startExecution(thread.threadId, 'develop');
      assert.equal(started.started, true, JSON.stringify(started));
      const originalRunId = started.run?.id || fixture.store.reloadTask(thread.threadId)?.execution?.run?.id;
      const deadline = Date.now() + 10_000;
      let fresh = fixture.store.getTask(thread.threadId);
      while (fresh?.rounds.find((round) => round.round === 2)?.status !== 'completed' && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        fixture.store.loadFromDisk();
        fresh = fixture.store.getTask(thread.threadId);
      }
      assert.equal(businessCalls, 1, 'the real AutonomousRunner.execute path reached the controlled CLI');
      assert.ok(childIdentities.length > 0, JSON.stringify({ runnerEvents, status: fresh?.status, execution: fresh?.execution, round: fresh?.rounds.find((round) => round.round === 2) }));
      assert.ok(childIdentities.every(({ pid, birthId }) => probeProcessIdentity(pid, birthId) === 'exited'), 'the controlled CLI process identity is confirmed exited');
      assert.equal(fresh?.rounds.find((round) => round.round === 2)?.status, 'completed', JSON.stringify({ runnerEvents, status: fresh?.status, execution: fresh?.execution, round: fresh?.rounds.find((round) => round.round === 2) }));
      assert.equal(fresh?.receiverSessionId, thread.receiverSessionId);
      assert.equal(fresh?.execution?.generation, 3);
      if (fresh?.execution?.run?.id === originalRunId) {
        const retainedRun = fresh.execution.run;
        assert.equal(retainedRun?.child?.pid, childIdentities[0].pid,
          'if subtree exit acknowledgement is pending, the original run still retains the exact child identity');
      }
      assert.equal(new TaskRoleLock(fixture.dir).getOwner(thread.threadId), undefined, 'v2 did not create a Bridge role lock');
      const nativeSessionDir = path.join(fixture.dir, 'native-sessions');
      if (fs.existsSync(nativeSessionDir)) assert.deepEqual(fs.readdirSync(nativeSessionDir), [path.basename(legacyLease!.path)], 'v2 did not create another Bridge native-session lease');
      assert.equal(fs.readFileSync(legacyLease.path, 'utf8'), legacyLeaseBytes, 'v2 left the pre-existing legacy lease untouched');
    } finally {
      if (legacyLease) legacyLock.release(legacyLease);
      await fixture.cleanup();
    }
  });

  test('v2 runner rejects a successful CLI result when its actual session cannot be durably claimed', async () => {
    const fixture = await setupFixture();
    let childPid: number | undefined;
    let childBirthId: string | undefined;
    try {
      const thread = createThread('thread_v2_runner_session_claim_failure', 'develop');
      thread.status = 'queued';
      thread.receiverSessionId = 'captured-session-owner';
      // A known preassigned ID may be claimed before first capture. The CLI's
      // actual ID below conflicts with another task and must not be accepted.
      thread.receiverSessionCaptured = false;
      thread.nativeSessionAssignments = {
        developer: { namespace: nativeSessionNamespace(thread.targetAgent), sessionEpoch: 2 },
      };
      thread.execution = {
        version: 2, revision: 1, operationId: 'session-claim-failure-op', phase: 'develop', round: 1,
        generation: 1, attempt: 0, autoRetriesUsed: 0,
        budget: { limitMs: DEFAULT_EXECUTION_BUDGET_MS, spentActiveMs: 0, maxAutoRetries: MAX_AUTO_RETRIES },
      } as any;
      thread.rounds = [{ round: 1, type: 'initial', prompt: 'run', status: 'running', durationMs: 0, timestamp: new Date().toISOString() }];
      fixture.store.upsertTask(thread);
      const ownership = new SessionOwnershipStore(fixture.dir);
      assert.equal(ownership.claim({ agent: thread.targetAgent, namespace: thread.nativeSessionAssignments!.developer!.namespace,
        sessionId: thread.receiverSessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 2 }).ok, true);
      assert.equal(ownership.claim({ agent: thread.targetAgent, namespace: thread.nativeSessionAssignments!.developer!.namespace,
        sessionId: 'session-owned-by-another-thread', threadId: 'thread_existing_session_owner', role: 'developer', sessionEpoch: 1 }).ok, true);

      const actualRunner = new AutonomousRunner(fixture.detector, undefined, undefined, fixture.store);
      (actualRunner as any).executeOnce = async (options: any) => {
        const guard = options.preSpawnGuard?.();
        assert.equal(guard?.ok, true);
        const child = spawn(process.execPath, ['-e', "process.stdin.once('data',()=>{process.stdout.write('valid output');process.exit(0)})"], {
          stdio: ['pipe', 'ignore', 'ignore'], windowsHide: true,
        });
        await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve); });
        childPid = child.pid;
        childBirthId = getProcessBirthId(childPid!);
        assert.ok(childBirthId);
        bindSpawnedChildIdentity(child, childBirthId!);
        await options.onChildPid(childPid, child, childBirthId);
        await options.onNativeSessionId?.('session-owned-by-another-thread', childPid);
        child.stdin!.end('finish');
        const exitCode = await new Promise<number>((resolve, reject) => {
          child.once('error', reject);
          child.once('close', (code) => resolve(code ?? -1));
        });
        await options.onChildPid(undefined, child, childBirthId);
        return {
          exitCode, stdout: 'valid output', stderr: '', extractedOutput: 'valid output',
          actualSessionId: 'session-owned-by-another-thread', durationMs: 1,
          assessment: { success: true, errorCode: 'OK', exitCode,
            stream: { chunkCount: 1, stdoutBytes: 12, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60_000 } },
        };
      };
      (fixture.controller as any).runner = actualRunner;

      const started = await fixture.controller.startExecution(thread.threadId, 'develop');
      assert.equal(started.started, true, JSON.stringify(started));
      const deadline = Date.now() + 10_000;
      let fresh = fixture.store.reloadTask(thread.threadId)!;
      while (fresh.rounds[0].status !== 'failed' && Date.now() < deadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
        fresh = fixture.store.reloadTask(thread.threadId)!;
      }
      assert.equal(fresh.rounds[0].status, 'failed', JSON.stringify({ status: fresh.status, execution: fresh.execution, round: fresh.rounds[0] }));
      assert.equal(fresh.rounds[0].assessment?.success, false);
      assert.equal(fresh.rounds[0].assessment?.errorCode, 'SESSION_OWNERSHIP_CONFLICT');
      assert.equal(fresh.receiverSessionId, thread.receiverSessionId, 'a conflicting real CLI session never replaces the pinned session');
      assert.ok(childPid && childBirthId && probeProcessIdentity(childPid, childBirthId) === 'exited', 'rejected child is confirmed exited');
    } finally {
      if (childPid && childBirthId && probeProcessIdentity(childPid, childBirthId) !== 'exited') {
        try { process.kill(childPid); } catch {}
      }
      await fixture.cleanup();
    }
  });

  test('v2 reviewer wake uses AutonomousRunner run identity and ignores stale Bridge session leases', async () => {
    const fixture = await setupFixture();
    const legacyLock = new NativeSessionLock(fixture.dir);
    let legacyLease: ReturnType<NativeSessionLock['acquire']> | undefined;
    let childIdentity: { pid: number; birthId: string } | undefined;
    const events: string[] = [];
    let resolveReviewerChildClose!: () => void;
    const reviewerChildClose = new Promise<void>((resolve) => { resolveReviewerChildClose = resolve; });
    try {
      const thread = createThread('thread_v2_actual_runner_reviewer_wake', 'review');
      thread.status = 'reviewing';
      thread.reviewerSessionId = thread.callerSessionId = 'captured-reviewer-session';
      thread.reviewerSessionCaptured = true;
      thread.nativeSessionAssignments = {
        reviewer: { namespace: nativeSessionNamespace(thread.initiatorAgent), sessionEpoch: 7 },
      };
      thread.rounds[0].status = 'completed';
      thread.execution = {
        version: 2, revision: 1, operationId: 'actual-reviewer-run-op', phase: 'review', round: 1,
        generation: 4, attempt: 0, autoRetriesUsed: 0,
        budget: { limitMs: DEFAULT_EXECUTION_BUDGET_MS, spentActiveMs: 0, maxAutoRetries: MAX_AUTO_RETRIES },
      } as any;
      fixture.store.upsertTask(thread);
      assert.equal(new SessionOwnershipStore(fixture.dir).claim({
        agent: thread.initiatorAgent,
        namespace: thread.nativeSessionAssignments.reviewer!.namespace,
        sessionId: thread.reviewerSessionId,
        threadId: thread.threadId,
        role: 'reviewer',
        sessionEpoch: 7,
      }).ok, true);
      legacyLease = legacyLock.acquire(thread.initiatorAgent, thread.reviewerSessionId);
      assert.ok(legacyLease);
      const legacyLeaseBytes = fs.readFileSync(legacyLease.path, 'utf8');

      // Remove the fixture's legacy direct-spawn mock. V2 review must use the
      // real CallbackManager -> AutonomousRunner path and only replace the
      // lowest-level CLI adapter with a controlled OS child.
      delete (fixture.callbackManager as any).launchCaller;
      const actualRunner = new AutonomousRunner(fixture.detector, undefined, undefined, fixture.store);
      fixture.callbackManager.setAutonomousRunner(actualRunner);
      (fixture.controller as any).runner = actualRunner;
      (actualRunner as any).executeOnce = async (options: any) => {
        events.push(`executeOnce:${options.nativeSessionRole}:${options.role}`);
        assert.equal(options.nativeSessionRole, 'reviewer');
        assert.equal(options.receiverSessionId, thread.reviewerSessionId);
        assert.equal(options.executionIdentity?.role, 'reviewer');
        assert.equal(options.executionIdentity?.phase, 'review');
        assert.ok(options.executionIdentity?.runId);
        assert.equal(options.preSpawnGuard?.().ok, true);
        const child = spawn(process.execPath, ['-e', "process.stdin.once('data',()=>{process.stdout.write('controlled reviewer output');process.exit(0)})"], {
          stdio: ['pipe', 'ignore', 'ignore'], windowsHide: true,
        });
        await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve); });
        const pid = child.pid!;
        const birthId = getProcessBirthId(pid);
        assert.ok(birthId);
        childIdentity = { pid, birthId };
        bindSpawnedChildIdentity(child, birthId);
        const childExit = new Promise<number>((resolve, reject) => {
          child.once('error', reject);
          child.once('close', (code) => resolve(code ?? -1));
        });
        await options.onChildPid(pid, child, birthId);
        events.push('reviewer-child-bound');
        child.stdin!.end('finish');
        const exitCode = await childExit;
        await options.onChildPid(undefined, child, birthId);
        events.push('reviewer-child-close-confirmed');
        resolveReviewerChildClose();
        return { exitCode, stdout: 'controlled reviewer output', stderr: '', extractedOutput: 'controlled reviewer output',
          durationMs: 1, assessment: { success: exitCode === 0, errorCode: exitCode === 0 ? 'OK' : 'NONZERO_EXIT', exitCode,
            stream: { chunkCount: 1, stdoutBytes: 27, stderrBytes: 0, hasActualOutput: true, continuous: true, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60_000 } } };
      };

      const started = await fixture.controller.startExecution(thread.threadId, 'review');
      assert.equal(started.started, true, JSON.stringify(started));
      let closeTimer: NodeJS.Timeout | undefined;
      await Promise.race([reviewerChildClose, new Promise<void>((_, reject) => {
        closeTimer = setTimeout(() => reject(new Error('Timed out waiting for the controlled reviewer child close event.')), 10_000);
      })]);
      if (closeTimer) clearTimeout(closeTimer);
      const fresh = fixture.store.reloadTask(thread.threadId)!;
      assert.deepEqual(events, ['executeOnce:reviewer:reviewer', 'reviewer-child-bound', 'reviewer-child-close-confirmed'], JSON.stringify({ status: fresh.status, execution: fresh.execution }));
      if (fresh.execution?.run) {
        assert.equal(fresh.execution.run.child?.pid, childIdentity!.pid,
          'a run retained for exit uncertainty still points at its exact reviewer child');
      } else {
        assert.equal(probeProcessIdentity(childIdentity!.pid, childIdentity!.birthId), 'exited',
          'the reviewer run was released only after the exact child exited');
      }
      assert.equal(fresh.execution?.generation, 4);
      assert.equal(new TaskRoleLock(fixture.dir).getOwner(thread.threadId), undefined, 'v2 reviewer wake did not create a Bridge role lease');
      const nativeSessionDir = path.join(fixture.dir, 'native-sessions');
      if (fs.existsSync(nativeSessionDir)) assert.deepEqual(fs.readdirSync(nativeSessionDir), [path.basename(legacyLease!.path)]);
      assert.equal(fs.readFileSync(legacyLease.path, 'utf8'), legacyLeaseBytes, 'v2 reviewer wake did not rewrite the stale native-session lease');
      assert.equal(probeProcessIdentity(childIdentity!.pid, childIdentity!.birthId), 'exited');
    } finally {
      if (legacyLease) legacyLock.release(legacyLease);
      if (childIdentity && probeProcessIdentity(childIdentity.pid, childIdentity.birthId) !== 'exited') {
        try { process.kill(childIdentity.pid); } catch {}
      }
      await fixture.cleanup();
    }
  });
  // Scenario 1: Unified start/stop/outcome classification for develop, review, merge
  test('Scenario 1: develop, review, and merge share execution control record and transition through controller', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s1', 'develop');
      fixture.addRunnableThread(thread);

      let finishDevelop: () => void;
      const barrier = new Promise<void>((r) => { finishDevelop = r; });
      fixture.runner.setOnExecute(async () => {
        await barrier;
        return { exitCode: 0, stdout: 'ok', extractedOutput: 'ok' };
      });

      let finishReview: () => void;
      const reviewBarrier = new Promise<void>((r) => { finishReview = r; });
      fixture.callbackManager.launchCaller = (async () => {
        await reviewBarrier;
        return { success: true, exitCode: 0 };
      }) as any;

      const res = await fixture.controller.startExecution('thread_s1', 'develop');
      assert.strictEqual(res.started, true);

      // Verify thread transitions to running
      const updated = fixture.store.getTask('thread_s1');
      assert.strictEqual(updated?.status, 'running');
      assert.strictEqual(updated?.execution?.phase, 'develop');
      assert.strictEqual(updated?.execution?.attempt, 1);
      assert.strictEqual(updated?.execution?.version, 2);
      assert.ok(updated?.execution?.run?.id);
      assert.strictEqual(updated?.execution?.run?.owner.pid, process.pid);

      // Finish develop -> advances to review seamlessly
      finishDevelop!();
      const deadline1 = Date.now() + 60_000;
      let postDevelop = fixture.store.getTask('thread_s1');
      while (postDevelop?.status !== 'reviewing' && Date.now() < deadline1) {
        await fixture.controller.scanAndDrain();
        await new Promise((r) => setTimeout(r, 50));
        postDevelop = fixture.store.getTask('thread_s1');
      }
      assert.strictEqual(postDevelop?.status, 'reviewing');
      assert.strictEqual(postDevelop?.execution?.phase, 'review');

      finishReview!();
      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 5: Two tasks approved simultaneously: only one enters merging, other queued(merge)
  test('Scenario 5: Two approved tasks serialize on global merge lock; only holder is merging', async () => {
    const fixture = await setupFixture();
    try {
      const taskA = createThread('thread_s5_a', 'merge');
      const taskB = createThread('thread_s5_b', 'merge');
      fixture.addRunnableThread(taskA);
      fixture.addRunnableThread(taskB);

      // Simulate merge lock held by Task A
      const releaseA = await fixture.mergeLock.acquire();
      assert.ok(releaseA);

      // Task B tries to start merge while lock is held
      const resB = await fixture.controller.startExecution('thread_s5_b', 'merge');
      assert.strictEqual(resB.started, false);
      assert.strictEqual(resB.reason, 'merge_lock_busy');

      const freshB = fixture.store.getTask('thread_s5_b');
      assert.strictEqual(freshB?.status, 'queued');
      assert.strictEqual(freshB?.execution?.phase, 'merge');
      assert.strictEqual(freshB?.execution?.queue?.reason, 'merge_lock');

      // Release Task A's lock
      releaseA();

      // Now Task B can acquire and start
      const resB2 = await fixture.controller.startExecution('thread_s5_b', 'merge');
      assert.strictEqual(resB2.started, true);
      const freshB2 = fixture.store.getTask('thread_s5_b');
      assert.strictEqual(freshB2?.status, 'merging');

      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  test('v2 merge lock contention stays queued, bounded, and preserves the durable handoff', async () => {
    const fixture = await setupFixture();
    let owner: ReturnType<typeof spawn> | undefined;
    try {
      const thread = createThread('thread_v2_merge_lock_busy', 'merge');
      thread.status = 'queued';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 3, operationId: 'approved-merge-op', phase: 'merge', round: 1,
        generation: 2, attempt: 0,
        next: { phase: 'merge', round: 1, sourceRunId: 'approval-source-run' },
      };
      fixture.store.upsertTask(thread);
      const ownerScript = `const fs=require('node:fs');const {MeshFileLock}=require(${JSON.stringify(require.resolve('../bridge/mesh-file-lock'))});
        const lock=new MeshFileLock(${JSON.stringify(fixture.mergeLock.lockPath)});
        lock.acquire().then(release=>{if(!release){process.exitCode=2;return;}fs.unlinkSync(${JSON.stringify(`${fixture.mergeLock.lockPath}.owner.json`)});process.stdout.write('held\\n');process.stdin.once('data',()=>{process.stdin.pause();release();process.stdout.write('released\\n');setImmediate(()=>process.exit(0));});});`;
      const lockOwner = spawn(process.execPath, ['-e', ownerScript], { stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
      owner = lockOwner;
      let ownerOutput = '';
      lockOwner.stdout!.on('data', (chunk) => { ownerOutput += chunk.toString(); });
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`Lock owner did not acquire OS lock: ${ownerOutput}`)), 15_000);
        const check = () => { if (ownerOutput.includes('held' + String.fromCharCode(10))) { clearTimeout(timer); resolve(); } };
        lockOwner.stdout!.on('data', check);
        lockOwner.once('error', (error) => { clearTimeout(timer); reject(error); });
        lockOwner.once('close', (code) => { if (!ownerOutput.includes('held' + String.fromCharCode(10))) { clearTimeout(timer); reject(new Error(`Lock owner exited ${code}: ${ownerOutput}`)); } });
        check();
      });
      const mergeLock = (fixture.controller as any).mergeLock;
      const originalStartLockHelper = mergeLock.startLockHelper.bind(mergeLock);
      const waiterChannels: string[] = [];
      mergeLock.startLockHelper = (token: string, channel: string, tryOnly: boolean) => {
        waiterChannels.push(channel);
        return originalStartLockHelper(token, channel, tryOnly);
      };
      const result = await fixture.controller.startExecution(thread.threadId, 'merge');
      mergeLock.startLockHelper = originalStartLockHelper;
      assert.equal(result.started, false);
      assert.equal(result.reason, 'merge_lock_busy');
      assert.equal(waiterChannels.length, 1, 'one real bounded try-lock helper performs the contention handshake');
      const released = fs.readFileSync(`${waiterChannels[0]}.released`, 'utf8').trim().split(':');
      assert.ok(Number(released[0]) > 0 && released[1], 'busy helper persists its exact OS-release acknowledgement');
      assert.equal(probeProcessIdentity(Number(released[0]), released[1]), 'exited', 'the busy helper is gone before admission returns');
      const queued = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(queued.status, 'queued', 'a merge is never published before OS lock acquisition');
      assert.equal(queued.execution?.run, undefined);
      assert.equal(queued.execution?.queue?.reason, 'merge_lock');
      assert.equal(queued.execution?.next?.sourceRunId, 'approval-source-run', 'failed admission preserves the durable handoff');
      owner.stdin!.end('release\n');
      await new Promise<void>((resolve) => owner!.once('close', () => resolve()));

      const originalAcquire = mergeLock.acquire.bind(mergeLock);
      let helperAttempts = 0;
      mergeLock.acquire = async () => { helperAttempts++; throw new Error('controlled helper launch failure'); };
      try {
        const helperFailure = await fixture.controller.startExecution(thread.threadId, 'merge');
        assert.equal(helperFailure.started, false);
        assert.equal(helperFailure.reason, 'merge_lock_error');
        const retained = fixture.store.reloadTask(thread.threadId)!;
        assert.equal(retained.status, 'queued');
        assert.equal(retained.execution?.queue?.reason, 'merge_lock_error');
        assert.equal(retained.execution?.lastOutcome?.code, 'MERGE_LOCK_HELPER_ERROR');
        assert.equal(retained.execution?.next?.sourceRunId, 'approval-source-run');
        await fixture.controller.scanAndDrain();
        const blocked = await fixture.controller.startExecution(thread.threadId, 'merge');
        assert.equal(blocked.started, false);
        assert.equal(blocked.reason, 'merge_lock_error_requires_intervention');
        assert.equal(helperAttempts, 1, 'helper error remains blocked rather than replaying on scan or direct start');
      } finally {
        mergeLock.acquire = originalAcquire;
      }
    } finally {
      if (owner && owner.exitCode === null && owner.signalCode === null) {
        const closed = new Promise<void>((resolve) => owner!.once('close', () => resolve()));
        try { owner.stdin!.write('release\n'); owner.stdin!.end(); } catch {}
        await closed;
      }
      await fixture.cleanup();
    }
  });

  test('v2 merge helper with unconfirmed exit retains run and durable next until exact helper exit is proven', async () => {
    const fixture = await setupFixture();
    let helper: ReturnType<typeof spawn> | undefined;
    try {
      const thread = createThread('thread_v2_merge_helper_unconfirmed', 'merge');
      thread.status = 'queued';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 3, operationId: 'approved-merge-unconfirmed', phase: 'merge', round: 1,
        generation: 5, attempt: 0,
        next: { phase: 'merge', round: 1, sourceRunId: 'approval-source-unconfirmed' },
      };
      fixture.store.upsertTask(thread);
      helper = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { windowsHide: true, stdio: 'ignore' });
      await new Promise<void>((resolve, reject) => { helper!.once('error', reject); helper!.once('spawn', resolve); });
      const birthId = getProcessBirthId(helper.pid!);
      assert.ok(birthId);
      const lock = (fixture.controller as any).mergeLock;
      const originalAcquire = lock.acquire.bind(lock);
      let attempts = 0;
      lock.acquire = async () => {
        attempts++;
        throw new MeshLockAcquisitionError('Busy merge lock helper exit/release acknowledgement is unverified.', 'unconfirmed', {
          pid: helper!.pid!, birthId, channelPath: path.join(fixture.dir, 'uncertain-helper-channel'),
        });
      };
      try {
        const first = await fixture.controller.startExecution(thread.threadId, 'merge');
        assert.equal(first.started, false);
        assert.equal(first.reason, 'merge_lock_unconfirmed');
        let saved = fixture.store.reloadTask(thread.threadId)!;
        assert.equal(saved.execution?.run?.state, 'stopping');
        assert.equal(saved.execution?.run?.unconfirmed, true);
        assert.equal(saved.execution?.run?.mergeLockHelper?.pid, helper.pid);
        assert.equal(saved.execution?.run?.mergeLockHelper?.birthId, birthId);
        assert.equal(saved.execution?.next?.sourceRunId, 'approval-source-unconfirmed');
        await fixture.controller.scanAndDrain();
        const blocked = await fixture.controller.startExecution(thread.threadId, 'merge');
        assert.equal(blocked.started, false);
        assert.equal(blocked.reason, 'merge_lock_unconfirmed');
        assert.equal(attempts, 1, 'unconfirmed helper is protected from scan replay');

        const closed = new Promise<void>((resolve) => helper!.once('close', () => resolve()));
        helper.kill();
        await closed;
        await fixture.controller.scanAndDrain();
        saved = fixture.store.reloadTask(thread.threadId)!;
        assert.equal(saved.execution?.run, undefined, 'only exact helper exit proof settles the protected run');
        assert.equal(saved.execution?.next?.sourceRunId, 'approval-source-unconfirmed');
        assert.equal(saved.execution?.queue?.reason, 'merge_lock_error');
        const settled = await fixture.controller.startExecution(thread.threadId, 'merge');
        assert.equal(settled.started, false);
        assert.equal(settled.reason, 'merge_lock_error_requires_intervention');
        assert.equal(attempts, 1);
      } finally {
        lock.acquire = originalAcquire;
      }
    } finally {
      if (helper && helper.exitCode === null && helper.signalCode === null) {
        const closed = new Promise<void>((resolve) => helper!.once('close', () => resolve()));
        helper.kill(); await closed;
      }
      await fixture.cleanup();
    }
  });

  test('confirmed merge helper cannot settle a business run until its persisted root and descendants exit', async () => {
    const fixture = await setupFixture();
    let owner: ReturnType<typeof spawn> | undefined;
    let root: ReturnType<typeof spawn> | undefined;
    let descendantIdentity: { pid: number; birthId: string } | undefined;
    try {
      const thread = createThread('thread_merge_helper_child_tree', 'merge');
      thread.status = 'merging';
      thread.execution = {
        ...thread.execution!, version: 2, revision: 3, operationId: 'merge-child-tree-op', phase: 'merge', round: 1,
        generation: 7, attempt: 1,
        next: { phase: 'merge', round: 1, sourceRunId: 'merge-child-tree-source' },
      };
      fixture.store.upsertTask(thread);

      owner = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      root = spawn(process.execPath, ['-e', `const{spawn}=require('node:child_process');const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'ignore',windowsHide:true,detached:true});child.unref();process.stdout.write('DESC:'+child.pid+'\\n');setInterval(()=>{},1000)`], { stdio: ['ignore', 'pipe', 'ignore'], windowsHide: true });
      const descendantPid = await new Promise<number>((resolve, reject) => {
        let output = '';
        const timer = setTimeout(() => reject(new Error(`controlled business root did not publish descendant PID: ${output}`)), 5_000);
        root!.stdout!.on('data', (chunk) => {
          output += chunk.toString();
          const match = output.match(/DESC:(\d+)/);
          if (match) { clearTimeout(timer); resolve(Number(match[1])); }
        });
        root!.once('error', (error) => { clearTimeout(timer); reject(error); });
      });
      const descendant = { pid: descendantPid, birthId: getProcessBirthId(descendantPid)! };
      descendantIdentity = descendant;
      await new Promise<void>((resolve) => setTimeout(resolve, 30));
      const rootIdentity = { pid: root.pid!, birthId: getProcessBirthId(root.pid!)! };
      const ownerIdentity = { pid: owner.pid!, birthId: getProcessBirthId(owner.pid!)! };
      assert.ok(rootIdentity.birthId && ownerIdentity.birthId && descendant.birthId);
      const release = await fixture.mergeLock.acquire();
      assert.ok(release);
      const helper = fixture.mergeLock.getActiveHelperEvidence();
      assert.ok(helper?.pid && helper.birthId);
      assert.equal(await fixture.mergeLock.releaseAndConfirm(release), true, 'helper OS release is acknowledged before recovery scans');

      await fixture.store.transitionExecution(thread.threadId, fixture.store.getTask(thread.threadId)!.execution!.revision, (task) => {
        const control = task.execution!;
        control.run = {
          id: 'merge-child-tree-run', owner: ownerIdentity, child: rootIdentity,
          subtrees: [{ root: rootIdentity, descendants: [descendant] }],
          step: 'executing', state: 'stopping', startedAt: new Date().toISOString(), role: 'reviewer',
          spawnIntentAt: new Date().toISOString(), registeredAt: new Date().toISOString(), unconfirmed: true,
          mergeLockHelper: { ...helper!, reason: 'release ack delayed', observedAt: new Date().toISOString() },
        };
        control.lastOutcome = { code: 'PROCESS_STOP_UNCONFIRMED', reason: 'controlled helper release interruption', at: new Date().toISOString() };
        control.queue = { reason: 'merge_lock_error', enteredAt: new Date().toISOString() };
        task.status = 'queued';
      });

      const scanInSecondControl = async () => {
        const scanner = spawn(process.execPath, [
          path.resolve(__dirname, 'fixtures/two-process-runner.js'),
          `--storage-dir=${fixture.dir}`, `--thread-id=${thread.threadId}`, '--worker-id=merge-recovery', '--scan-only',
        ], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
        let stdout = '';
        let stderr = '';
        scanner.stdout!.on('data', (chunk) => { stdout += chunk.toString(); });
        scanner.stderr!.on('data', (chunk) => { stderr += chunk.toString(); });
        const code = await new Promise<number | null>((resolve, reject) => {
          scanner.once('error', reject);
          scanner.once('close', resolve);
        });
        assert.equal(code, 0, stderr);
        const line = stdout.split(/\r?\n/).find((candidate) => candidate.startsWith('RESULT:'));
        assert.ok(line, `scan controller did not publish its result: ${stdout} ${stderr}`);
        return JSON.parse(line!.slice('RESULT:'.length)) as { runId?: string; revision?: number };
      };

      assert.equal((await scanInSecondControl()).runId, 'merge-child-tree-run', 'second control process retains a live business root');
      assert.equal(probeProcessIdentity(rootIdentity.pid, rootIdentity.birthId), 'alive');
      assert.equal(probeProcessIdentity(descendant.pid, descendant.birthId), 'alive');

      const rootClosed = new Promise<void>((resolve) => root!.once('close', () => resolve()));
      const ownerClosed = new Promise<void>((resolve) => owner!.once('close', () => resolve()));
      root.kill(); owner.kill();
      await Promise.all([rootClosed, ownerClosed]);
      root = undefined; owner = undefined;
      assert.equal(probeProcessIdentity(descendant.pid, descendant.birthId), 'alive', 'captured descendant survives root and owner exit');
      assert.equal((await scanInSecondControl()).runId, 'merge-child-tree-run', 'second control process retains the run while a captured descendant lives');

      {
        process.kill(descendant.pid, 'SIGKILL');
        const deadline = Date.now() + 5_000;
        while (probeProcessIdentity(descendant.pid, descendant.birthId) !== 'exited' && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 25));
        }
      }
      assert.equal(probeProcessIdentity(descendant.pid, descendant.birthId), 'exited');
      await scanInSecondControl();
      const settled = fixture.store.reloadTask(thread.threadId)!;
      assert.equal(settled.execution?.run, undefined, 'all durable identities plus helper ACK permit one settlement');
      assert.equal(settled.execution?.next?.sourceRunId, 'merge-child-tree-source');
      const revision = settled.execution!.revision;
      await scanInSecondControl();
      assert.equal(fixture.store.reloadTask(thread.threadId)?.execution?.revision, revision, 'repeat scan cannot settle the same run twice');
    } finally {
      for (const child of [root, owner]) {
        if (child && child.exitCode === null && child.signalCode === null) {
          const closed = new Promise<void>((resolve) => child.once('close', () => resolve()));
          try { child.kill(); } catch {}
          await closed;
        }
      }
      if (descendantIdentity && probeProcessIdentity(descendantIdentity.pid, descendantIdentity.birthId) !== 'exited') {
        try { process.kill(descendantIdentity.pid, 'SIGKILL'); } catch {}
      }
      await fixture.cleanup();
    }
  });

  // Scenario 15: Seamless phase handoff without mandatory queued state; concurrent independent reviews
  test('Scenario 15: Phase handoff advances directly when resources free; independent reviews are concurrent', async () => {
    const fixture = await setupFixture();
    try {
      const task1 = createThread('thread_s15_1', 'review');
      task1.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      const task2 = createThread('thread_s15_2', 'review');
      task2.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      fixture.addRunnableThread(task1);
      fixture.addRunnableThread(task2);

      let finishReview: () => void;
      const reviewBarrier = new Promise<void>((r) => { finishReview = r; });
      fixture.callbackManager.launchCaller = (async () => {
        await reviewBarrier;
        return { success: true, exitCode: 0 };
      }) as any;

      // Start review on Task 1
      const res1 = await fixture.controller.startExecution('thread_s15_1', 'review');
      assert.strictEqual(res1.started, true);
      const fresh1 = fixture.store.getTask('thread_s15_1');
      assert.strictEqual(fresh1?.status, 'reviewing');

      // Start review on Task 2: reviews do NOT globally serialize on each other!
      const res2 = await fixture.controller.startExecution('thread_s15_2', 'review');
      assert.strictEqual(res2.started, true);
      const fresh2 = fixture.store.getTask('thread_s15_2');
      assert.strictEqual(fresh2?.status, 'reviewing');

      finishReview!();
      await fixture.controller.waitForActiveExecutions(3000);
      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 19: MeshFileLock child lifetime: helper holds OS mutex until attached child exits
  test('Scenario 19: MeshFileLock helper holds OS lock until attached child terminates', async () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-lock-test-'));
    const lockPath = path.join(tmpDir, 'test-merge.lock');
    const lockA = new MeshFileLock(lockPath);
    const lockB = new MeshFileLock(lockPath);

    let childProc: any;
    try {
      const releaseA = await lockA.acquire();
      assert.ok(releaseA, 'lockA should acquire');

      // Spawn a child process that sleeps
      childProc = spawn(process.execPath, ['-e', 'setTimeout(() => {}, 4000)'], { stdio: 'ignore' });
      await lockA.attachChildPid(childProc.pid);

      // Release A: helper should NOT release OS mutex yet because childProc is still alive!
      releaseA();

      // lockB tries to acquire with 600ms timeout -> must time out!
      const releaseB = await lockB.acquire(undefined, undefined, 600);
      assert.strictEqual(releaseB, undefined, 'lockB must not acquire while childProc is still alive');

      // Now kill the child process
      try { process.kill(childProc.pid, 'SIGKILL'); } catch {}

      // Wait a moment for helper to observe child exit
      await new Promise((r) => setTimeout(r, 400));

      // Now lockB should be able to acquire!
      const releaseBAfter = await lockB.acquire(undefined, undefined, 12_000);
      assert.ok(releaseBAfter, 'lockB should acquire after childProc terminates');
      releaseBAfter();
    } finally {
      if (childProc?.pid) {
        try { process.kill(childProc.pid, 'SIGKILL'); } catch {}
      }
      lockA.dispose();
      lockB.dispose();
      try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 23: A passive scan leaves historical ownership and files untouched
  test('Scenario 23: V1 orphan is left untouched by the V2 scheduler', async () => {
    const fixture = await setupFixture();
    let child: ReturnType<typeof spawn> | undefined;
    try {
      child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {
        stdio: 'ignore',
      });
      assert.ok(child.pid);
      const childBirthId = getProcessBirthId(child.pid);

      const thread = createThread('thread_s23', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s23',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-1',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          child: { pid: child.pid, birthId: childBirthId },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Verify inspectRunProcesses sees dead owner and alive child
      const inspect = fixture.controller.inspectRunProcesses(thread.execution!.run);
      assert.strictEqual(inspect.ownerStatus, 'exited');
      assert.strictEqual(inspect.childStatus, 'alive');

      const snapshot = fixture.store.reloadTask(thread.threadId);
      await fixture.controller.scanAndDrain();
      assert.deepEqual(fixture.store.reloadTask(thread.threadId), snapshot);
      assert.strictEqual(probeProcessIdentity(child.pid, childBirthId), 'alive');
      assert.equal(fixture.runner.executions.length, 0);
    } finally {
      child?.kill();
      await fixture.cleanup();
    }
  });

  test('pausing a historical run cannot stop a child without its recorded birth identity', async () => {
    const fixture = await setupFixture();
    const childPid = 99_999_996;
    const owner = { pid: process.pid, birthId: getProcessBirthId(process.pid) };
    setProcessProbeProvider(pid => pid === childPid || pid === owner.pid ? 'alive' : 'unknown');
    setProcessBirthIdProvider(pid => pid === childPid ? 'current-process-birth' : undefined);
    setKillPidTreeProvider(pid => assert.fail(`Must not signal unverified PID ${pid}`));
    try {
      const thread = createThread('thread_historical_pause');
      thread.status = 'running';
      thread.execution!.version = 1;
      thread.execution!.run = {
        id: 'historical-run',
        owner,
        child: { pid: childPid },
        state: 'active',
        step: 'executing',
        startedAt: thread.createdAt,
      };
      fixture.store.upsertTask(thread);

      const runWithoutOwnerBirth = { ...thread.execution!.run, owner: { pid: owner.pid } };
      assert.equal(fixture.controller.inspectRunProcesses(runWithoutOwnerBirth).ownerStatus, 'unknown');

      await fixture.controller.pauseExecution(thread.threadId);
      const paused = fixture.store.reloadTask(thread.threadId);
      assert.equal(paused?.status, 'running');
      assert.equal(paused?.execution?.run?.id, 'historical-run');
      assert.equal(paused?.execution?.run?.state, 'stopping');
      assert.deepEqual(paused?.execution?.run?.child, { pid: childPid });
      assert.equal(paused?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');
    } finally {
      setKillPidTreeProvider(undefined);
      setProcessProbeProvider(undefined);
      setProcessBirthIdProvider(undefined);
      await fixture.cleanup();
    }
  });

  // Scenario 29: Separate logical verdict from physical execution exit: approve/rework retains Reviewer run until CLI exits
  test('Scenario 29: approveTask retains Reviewer run until CLI exits and non-zero tail exit does not revoke verdict', async () => {
    const fixture = await setupFixture();
    const reworkController = new ReworkController(
      fixture.store,
      fixture.runner as any,
      fixture.callbackManager,
      fixture.detector,
      fixture.registry,
    );
    const gitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-s29-test-'));

    try {
      execFileSync('git', ['init', '-b', 'master'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.name', 'test'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.email', 'test@test.com'], { cwd: gitDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'base');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'base commit'], { cwd: gitDir, stdio: 'ignore' });
      const baseSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'modified by developer');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'dev commit'], { cwd: gitDir, stdio: 'ignore' });
      const headSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      const thread = createThread('thread_s29', 'review');
      thread.workspace = testWorkspace(gitDir);
      thread.worktree = {
        path: gitDir,
        mainPath: gitDir,
        repositoryPath: gitDir,
        commonDir: path.join(gitDir, '.git'),
        targetRef: 'refs/heads/master',
        taskBranch: 'mesh/task/thread_s29',
        baseCommit: baseSha,
        headCommit: headSha,
      };
      thread.rounds[0].status = 'completed'; // developer round completed
      fixture.addRunnableThread(thread);

      let releaseCliBarrier: (res: { success: boolean; exitCode: number }) => void;
      const cliBarrier = new Promise<{ success: boolean; exitCode: number }>((resolve) => {
        releaseCliBarrier = resolve;
      });

      // Mock launchCaller representing a live Reviewer CLI
      fixture.callbackManager.launchCaller = (async () => await cliBarrier) as any;

      // Start review phase
      const startRes = await fixture.controller.startExecution('thread_s29', 'review');
      assert.strictEqual(startRes.started, true);

      // Verify task is reviewing with an active run
      const reviewingTask = fixture.store.getTask('thread_s29');
      assert.strictEqual(reviewingTask?.status, 'reviewing');
      assert.strictEqual(reviewingTask?.execution?.phase, 'review');
      const activeRunId = reviewingTask?.execution?.run?.id;
      assert.ok(activeRunId);

      // Now inside the Reviewer CLI turn, call approveTask (simulating MCP tool call)
      const approveRes = await reworkController.approveTask({
        threadId: 'thread_s29',
        remarks: 'Code looks good',
      });
      assert.strictEqual(approveRes.status, 'QUEUED');

      // CRITICAL ASSERTION: While CLI is still held at barrier:
      // 1. The Reviewer run is STILL active in execution!
      // 2. The phase is STILL 'review'!
      // 3. The next phase ('merge') has NOT started yet!
      const midTask = fixture.store.getTask('thread_s29');
      assert.strictEqual(midTask?.execution?.run?.id, activeRunId);
      assert.strictEqual(midTask?.execution?.phase, 'review');

      // Now release the CLI barrier with NON-ZERO exit code (simulating tail exit error)
      releaseCliBarrier!({ success: false, exitCode: 1 });

      // Wait for ExecutionController to consume the verdict
      const deadline = Date.now() + 60_000;
      let finalTask = fixture.store.getTask('thread_s29');
      while (finalTask?.execution?.phase !== 'merge' && Date.now() < deadline) {
        await fixture.controller.scanAndDrain();
        await new Promise((r) => setTimeout(r, 100));
        fixture.store.loadFromDisk();
        finalTask = fixture.store.getTask('thread_s29');
      }

      // Assert: The non-zero tail exit did NOT revoke the persistent approval!
      // Phase transitioned to 'merge' and old reviewer run was cleanly released!
      assert.strictEqual(finalTask?.execution?.phase, 'merge');
      assert.ok(finalTask?.worktree?.approvedCommit);
    } finally {
      await fixture.cleanup();
      try { fs.rmSync(gitDir, { recursive: true, force: true }); } catch {}
    }
  });
});
