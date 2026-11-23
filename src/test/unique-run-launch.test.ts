import * as assert from 'node:assert';
import * as child_process from 'node:child_process';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { test, describe } from 'node:test';

import { TaskStore } from '../bridge/task-store';
import {
  ExecutionController,
} from '../bridge/execution-controller';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { NativeSessionLock } from '../bridge/native-session-lock';
import { nativeSessionNamespace, SessionOwnershipStore } from '../bridge/session-ownership';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import { WindowsTaskLock } from '../bridge/windows-process-helper';
import {
  DelegationThread,
  ExecutionControl,
  ExecutionPhase,
  ExecutionRun,
} from '../bridge/types';
import {
  bindSpawnedChildIdentity,
  confirmProcessTreeExit,
  getProcessBirthId,
  getProcessBirthIds,
  IS_WINDOWS,
  killPidTree,
  killProcessTree,
  probeProcessIdentity,
  setConfirmProcessTreeExitProvider,
  setProcessProbeProvider,
} from '../bridge/platform';
import {
  assessRunLaunchEvidence,
  captureChildDescendants,
  recordLaunchRecoveryFailure,
  registerRunChildV2,
  stopVerifiedChildTree,
  verifySubtreeOwnership,
  RunIdentity,
} from '../bridge/process-binding';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { validateV2ExecutionEligibility } from '../bridge/execution-gate';
import { AgentDetector } from '../bridge/agent-detector';
import { testWorkspace } from './workspace-fixture';

function createTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `amb-test-${prefix}-`));
}

function cleanTempDir(dir: string): void {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
}

function waitForFile(file: string, timeoutMs = 20_000): Promise<void> {
  if (fs.existsSync(file)) return Promise.resolve();
  return new Promise((resolve, reject) => {
    let watcher: fs.FSWatcher | undefined;
    const finish = (error?: Error) => {
      clearTimeout(timer);
      watcher?.close();
      error ? reject(error) : resolve();
    };
    const check = () => { if (fs.existsSync(file)) finish(); };
    const timer = setTimeout(() => finish(new Error(`Timed out waiting for barrier file ${file}`)), timeoutMs);
    watcher = fs.watch(path.dirname(file), check);
    check();
  });
}

function createV2Thread(
  threadId: string,
  storageDir: string,
  phase: ExecutionPhase = 'develop',
  overrides?: Partial<DelegationThread>,
): DelegationThread {
  const now = new Date().toISOString();
  const thread: DelegationThread = {
    threadId,
    title: `Test ${threadId}`,
    initiatorAgent: 'claude_code',
    targetAgent: 'codex',
    status: phase === 'develop' ? 'queued' : phase === 'review' ? 'reviewing' : 'queued',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: `session-${threadId}`,
    receiverSessionCaptured: true,
    reviewerSessionId: `rev-session-${threadId}`,
    callerSessionId: `rev-session-${threadId}`,
    reviewerSessionCaptured: true,
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    summary: {
      lastPromptSnippet: 'Build feature',
      lastOutputSnippet: '',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'Build feature',
        status: 'running',
        durationMs: 0,
        timestamp: now,
      },
    ],
    workspace: testWorkspace(storageDir),
    execution: {
      version: 2,
      revision: 1,
      operationId: `op_${threadId}_1`,
      phase,
      round: 1,
      generation: 1,
      attempt: 0,
      autoRetriesUsed: 0,
      budget: {
        limitMs: 120 * 60 * 1000,
        spentActiveMs: 0,
        maxAutoRetries: 2,
      },
    },
    ...overrides,
  };
  const ownership = new SessionOwnershipStore(storageDir);
  if (thread.reviewerSessionCaptured && thread.reviewerSessionId) {
    ownership.claim({
      agent: thread.initiatorAgent,
      namespace: thread.nativeSessionAssignments?.reviewer?.namespace || nativeSessionNamespace(thread.initiatorAgent),
      sessionId: thread.reviewerSessionId,
      threadId: thread.threadId,
      role: 'reviewer',
      sessionEpoch: thread.nativeSessionAssignments?.reviewer?.sessionEpoch || 1,
    });
  }
  if (thread.receiverSessionCaptured && thread.receiverSessionId) {
    ownership.claim({
      agent: thread.targetAgent,
      namespace: thread.nativeSessionAssignments?.developer?.namespace || nativeSessionNamespace(thread.targetAgent),
      sessionId: thread.receiverSessionId,
      threadId: thread.threadId,
      role: 'developer',
      sessionEpoch: thread.nativeSessionAssignments?.developer?.sessionEpoch || 1,
    });
  }
  return thread;
}

describe('Task 02: Unique Run Acquisition & Launch Registration', () => {
  test('Windows descendant birth IDs use one batch request to the persistent helper', { skip: !IS_WINDOWS }, async (t) => {
    const helper = require('../bridge/windows-process-helper') as typeof import('../bridge/windows-process-helper');
    const batch = t.mock.method(helper, 'requestWindowsProcessHelper', async () => ({ 11: '1001', 22: '2002' }));
    assert.deepEqual([...(await getProcessBirthIds([11, 22, 11, 0]))], [[11, '1001'], [22, '2002']]);
    assert.equal(batch.mock.callCount(), 1);
    assert.deepEqual(batch.mock.calls[0].arguments, [{ operation: 'birthIds', pids: [11, 22] }]);
  });


  // -------------------------------------------------------------------------
  // Acceptance Criterion 1: Two real control processes concurrent competition
  // -------------------------------------------------------------------------
  test('Two real OS control processes concurrently start same task: exactly one wins run and spawns child', async () => {
    const storageDir = createTempDir('real-concurrency');
    const threadId = 'thread_real_concurrency_p1';
    const barrierFile = path.join(storageDir, 'start.barrier');
    const holdFile = path.join(storageDir, 'release-child');
    const registeredFile = path.join(storageDir, 'child-registered');
    const spawnedFile = path.join(storageDir, 'child-spawned');
    const spawnedBirthFile = path.join(storageDir, 'child-spawned-birth');
    const workers: child_process.ChildProcess[] = [];

    try {
      const store = new TaskStore(storageDir);
      const initialThread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(initialThread);

      const workerScript = path.resolve(__dirname, 'fixtures/two-process-runner.js');

      // Spawn two real OS child processes running the worker script
      const proc1 = child_process.spawn(process.execPath, [
        workerScript,
        `--storage-dir=${storageDir}`,
        `--thread-id=${threadId}`,
        '--worker-id=worker_1',
        `--barrier-file=${barrierFile}`,
        `--hold-file=${holdFile}`,
        `--registered-file=${registeredFile}`,
        `--spawned-file=${spawnedFile}`,
        `--spawned-birth-file=${spawnedBirthFile}`,
      ], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
      workers.push(proc1);

      const proc2 = child_process.spawn(process.execPath, [
        workerScript,
        `--storage-dir=${storageDir}`,
        `--thread-id=${threadId}`,
        '--worker-id=worker_2',
        `--barrier-file=${barrierFile}`,
        `--hold-file=${holdFile}`,
        `--registered-file=${registeredFile}`,
        `--spawned-file=${spawnedFile}`,
        `--spawned-birth-file=${spawnedBirthFile}`,
      ], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
      workers.push(proc2);

      let out1 = '';
      let out2 = '';
      let err1 = '';
      let err2 = '';
      proc1.stdout?.on('data', (d) => { out1 += d.toString(); });
      proc2.stdout?.on('data', (d) => { out2 += d.toString(); });
      proc1.stderr?.on('data', (d) => { err1 += d.toString(); });
      proc2.stderr?.on('data', (d) => { err2 += d.toString(); });
      const proc1Close = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
        proc1.once('close', (code, signal) => resolve({ code, signal }));
      });
      const proc2Close = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) => {
        proc2.once('close', (code, signal) => resolve({ code, signal }));
      });

      // Event barriers keep both real control processes poised at startExecution.
      const ready1 = `${barrierFile}.worker_1.ready`;
      const ready2 = `${barrierFile}.worker_2.ready`;
      await Promise.all([waitForFile(ready1, 60_000), waitForFile(ready2, 60_000)]);

      // Release barrier so both invoke startExecution simultaneously.
      fs.writeFileSync(barrierFile, 'start', 'utf8');
      try {
        await waitForFile(registeredFile, 60_000);
      } catch (error: any) {
        store.ensureUpToDate();
        let events = '';
        try { events = fs.readFileSync(`${registeredFile}.events`, 'utf8'); } catch {}
        throw new Error(`${error?.message || error}; events=${events}; worker1 stdout=${out1}; stderr=${err1}; worker2 stdout=${out2}; stderr=${err2}; ledger=${JSON.stringify(store.getTask(threadId)?.execution)}`);
      }
      store.ensureUpToDate();
      assert.strictEqual(store.getTask(threadId)?.execution?.run?.state, 'active', 'winner registration must finish before its child is released');
      fs.writeFileSync(holdFile, 'release', 'utf8');

      // Wait for both processes to complete.
      const [exit1, exit2] = await Promise.all([proc1Close, proc2Close]);
      assert.strictEqual(exit1.code, 0, `Worker 1 exit ${JSON.stringify(exit1)}; stdout=${out1}; stderr=${err1}`);
      assert.strictEqual(exit2.code, 0, `Worker 2 exit ${JSON.stringify(exit2)}; stdout=${out2}; stderr=${err2}`);

      const parseResult = (output: string) => {
        const line = output.split('\n').find((l) => l.startsWith('RESULT:'));
        if (!line) return null;
        try {
          return JSON.parse(line.slice(7));
        } catch {
          return null;
        }
      };

      const res1 = parseResult(out1);
      const res2 = parseResult(out2);

      assert.ok(res1, `Worker 1 failed to emit result: stdout=${out1}; stderr=${err1}`);
      assert.ok(res2, `Worker 2 failed to emit result: stdout=${out2}; stderr=${err2}`);

      // Exactly ONE must have started: true, and ONE must have started: false
      const startedCount = (res1.started ? 1 : 0) + (res2.started ? 1 : 0);
      assert.strictEqual(startedCount, 1, `Expected exactly 1 winner, got ${startedCount}`);

      // Exactly ONE subprocess was spawned across both controllers
      const totalSpawns = (res1.spawnCount || 0) + (res2.spawnCount || 0);
      assert.strictEqual(totalSpawns, 1, `Expected exactly 1 spawn across processes, got ${totalSpawns}`);

      // Losing worker must have reason 'run_busy'
      const loser = res1.started ? res2 : res1;
      assert.strictEqual(loser.started, false);
      assert.strictEqual(loser.reason, 'run_busy');
      assert.strictEqual(loser.spawnCount, 0);

      // Verify authoritative task state on disk
      store.ensureUpToDate();
      const onDisk = store.getTask(threadId);
      assert.ok(onDisk);
      assert.strictEqual(onDisk.execution?.version, 2);
      assert.strictEqual(onDisk.execution?.attempt, 1, 'Attempt must be exactly 1; loser must not consume attempt');
      // No role files created under v2
      const roleFile = path.join(storageDir, 'roles', `${threadId}.json`);
      assert.strictEqual(fs.existsSync(roleFile), false, 'v2 must not create independent role files');
    } finally {
      try { fs.writeFileSync(holdFile, 'release', 'utf8'); } catch {}
      let cleanupTimer: NodeJS.Timeout | undefined;
      const workersClosed = Promise.all(workers.map((worker) => new Promise<void>((resolve) => {
        if (worker.exitCode !== null || worker.signalCode !== null) return resolve();
        worker.once('close', () => resolve());
      })));
      await Promise.race([
        workersClosed,
        new Promise<void>((resolve) => { cleanupTimer = setTimeout(resolve, 15_000); }),
      ]);
      if (cleanupTimer) clearTimeout(cleanupTimer);
      for (const worker of workers) {
        if (worker.exitCode === null && worker.signalCode === null) try { worker.kill(); } catch {}
      }
      let forcedCloseTimer: NodeJS.Timeout | undefined;
      await Promise.race([
        Promise.all(workers.map((worker) => new Promise<void>((resolve) => {
          if (worker.exitCode !== null || worker.signalCode !== null) return resolve();
          worker.once('close', () => resolve());
        }))),
        new Promise<void>((resolve) => { forcedCloseTimer = setTimeout(resolve, 5_000); }),
      ]);
      if (forcedCloseTimer) clearTimeout(forcedCloseTimer);
      try {
        if (fs.existsSync(spawnedFile) && fs.existsSync(spawnedBirthFile)) {
          const pid = Number(fs.readFileSync(spawnedFile, 'utf8').trim());
          const birthId = fs.readFileSync(spawnedBirthFile, 'utf8').trim();
          if (Number.isSafeInteger(pid) && pid > 0 && birthId && probeProcessIdentity(pid, birthId) !== 'exited') {
            await stopVerifiedChildTree(pid, true, undefined, birthId, 5_000, [], true);
          }
        }
      } catch {}
      cleanTempDir(storageDir);
    }
  });

  test('two real control processes recover one exited owner with a durable review decision and next', async () => {
    const storageDir = createTempDir('orphan-next-processes');
    const threadId = 'thread_orphan_next_processes';
    const barrierFile = path.join(storageDir, 'recovery.start');
    const holdFile = path.join(storageDir, 'recovery.release');
    const registeredFile = path.join(storageDir, 'recovery.child-registered');
    const spawnedFile = path.join(storageDir, 'recovery.child-pid');
    const spawnedBirthFile = path.join(storageDir, 'recovery.child-birth');
    const workers: child_process.ChildProcess[] = [];
    const store = new TaskStore(storageDir);

    try {
      const thread = createV2Thread(threadId, storageDir, 'review');
      const now = new Date().toISOString();
      thread.status = 'running';
      thread.execution = {
        ...thread.execution!,
        revision: 10,
        operationId: 'review-operation-before-crash',
        phase: 'review',
        round: 1,
        run: {
          id: 'review-run-before-crash',
          role: 'reviewer',
          owner: { pid: 99_999_999, birthId: 'dead-review-owner' },
          child: { pid: 99_999_998, birthId: 'dead-review-child' },
          subtrees: [{
            root: { pid: 99_999_998, birthId: 'dead-review-child' },
            descendants: [],
          }],
          state: 'active',
          step: 'executing',
          startedAt: now,
          registeredAt: now,
        },
        next: { phase: 'develop', round: 2, sourceRunId: 'review-run-before-crash' },
      };
      thread.currentRound = 1;
      const decision = {
        id: 'durable-rework-decision',
        operationId: 'review-operation-before-crash',
        generation: 1,
        phase: 'review' as const,
        round: 1,
        question: 'Make a correction',
        resumePoint: 'continue_develop' as const,
        createdAt: now,
        answer: 'rework requested',
        answeredAt: now,
        answeredBy: 'reviewer-session',
      };
      thread.decision = decision;
      thread.decisions = [decision];
      store.upsertTask(thread);
      assert.equal(probeProcessIdentity(99_999_999, 'dead-review-owner'), 'exited');
      assert.equal(probeProcessIdentity(99_999_998, 'dead-review-child'), 'exited');

      const workerScript = path.resolve(__dirname, 'fixtures/two-process-runner.js');
      const launchWorker = (workerId: string) => {
        const worker = child_process.spawn(process.execPath, [
          workerScript,
          `--storage-dir=${storageDir}`,
          `--thread-id=${threadId}`,
          `--worker-id=${workerId}`,
          '--phase=develop',
          `--barrier-file=${barrierFile}`,
          `--hold-file=${holdFile}`,
          `--registered-file=${registeredFile}`,
          `--spawned-file=${spawnedFile}`,
          `--spawned-birth-file=${spawnedBirthFile}`,
        ], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
        workers.push(worker);
        return worker;
      };
      const first = launchWorker('recovery_1');
      const second = launchWorker('recovery_2');
      let firstOut = ''; let secondOut = ''; let firstErr = ''; let secondErr = '';
      first.stdout?.on('data', (data) => { firstOut += data.toString(); });
      second.stdout?.on('data', (data) => { secondOut += data.toString(); });
      first.stderr?.on('data', (data) => { firstErr += data.toString(); });
      second.stderr?.on('data', (data) => { secondErr += data.toString(); });
      const firstClosed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
        first.once('close', (code, signal) => resolve({ code, signal })));
      const secondClosed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((resolve) =>
        second.once('close', (code, signal) => resolve({ code, signal })));

      await Promise.all([
        waitForFile(`${barrierFile}.recovery_1.ready`, 60_000),
        waitForFile(`${barrierFile}.recovery_2.ready`, 60_000),
      ]);
      const controlIdentities = [first, second].map((worker) => ({
        pid: worker.pid!, birthId: getProcessBirthId(worker.pid!),
      }));
      assert.ok(controlIdentities.every((identity) => identity.birthId), 'both control processes have captured PID+birthId');
      fs.writeFileSync(barrierFile, 'start', 'utf8');
      try {
        await waitForFile(registeredFile, 60_000);
      } catch (error: any) {
        store.ensureUpToDate();
        let events = '';
        try { events = fs.readFileSync(`${registeredFile}.events`, 'utf8'); } catch {}
        throw new Error(`${error?.message || error}; events=${events}; first stdout=${firstOut}; stderr=${firstErr}; second stdout=${secondOut}; stderr=${secondErr}; ledger=${JSON.stringify(store.getTask(threadId)?.execution)}`);
      }

      const recovering = store.reloadTask(threadId)!;
      const run = recovering.execution?.run;
      const childPid = Number(fs.readFileSync(spawnedFile, 'utf8'));
      const childBirthId = fs.readFileSync(spawnedBirthFile, 'utf8');
      assert.equal(recovering.execution?.phase, 'develop');
      assert.equal(recovering.execution?.round, 2);
      assert.equal(recovering.execution?.operationId === 'review-operation-before-crash', false);
      assert.equal(run?.id === 'review-run-before-crash', false);
      assert.equal(run?.role, 'developer');
      assert.equal(run?.child?.pid, childPid);
      assert.equal(run?.child?.birthId, childBirthId);
      assert.equal(run?.state, 'active', 'child binding ACK is durable before it is released');
      assert.equal(recovering.execution?.next, undefined, 'durable next is consumed with the new starting run');
      assert.equal(recovering.decision?.id, decision.id, 'persisted reviewer decision survives recovery');
      assert.equal(recovering.decisions?.at(-1)?.id, decision.id);
      assert.equal(probeProcessIdentity(childPid, childBirthId), 'alive');
      const winnerOwner = controlIdentities.find((identity) => identity.pid === run?.owner?.pid);
      assert.ok(winnerOwner, 'recovered run belongs to one of the real control processes');
      assert.equal(run?.owner?.birthId, winnerOwner.birthId);
      assert.equal(controlIdentities.filter((identity) => identity.pid === run?.owner?.pid).length, 1);

      const stale = await store.reportRoundEvidence(threadId, 2, { output: 'stale pre-crash review callback' }, {
        operationId: 'review-operation-before-crash', generation: 1, runId: 'review-run-before-crash',
      });
      assert.equal(stale, undefined, 'old source operation cannot write evidence into the recovered developer run');
      assert.notEqual(store.reloadTask(threadId)?.rounds.find((round) => round.round === 2)?.output, 'stale pre-crash review callback');

      fs.writeFileSync(holdFile, 'release', 'utf8');
      const [firstExit, secondExit] = await Promise.all([firstClosed, secondClosed]);
      assert.equal(firstExit.code, 0, `first controller failed: ${firstErr}; ${firstOut}`);
      assert.equal(secondExit.code, 0, `second controller failed: ${secondErr}; ${secondOut}`);
      const parse = (output: string) => {
        const line = output.split('\n').find((value) => value.startsWith('RESULT:'));
        return line ? JSON.parse(line.slice(7)) : undefined;
      };
      const results = [parse(firstOut), parse(secondOut)];
      assert.ok(results.every(Boolean), `both controls must report outcomes: ${firstOut} ${secondOut}`);
      assert.equal(results.filter((result) => result.started).length, 1);
      assert.equal(results.reduce((sum, result) => sum + result.spawnCount, 0), 1);
      assert.ok(results.some((result) => result.reason === 'run_busy'));
      assert.equal(probeProcessIdentity(childPid, childBirthId), 'exited', 'the registered child is confirmed exited after release');
      const finished = store.reloadTask(threadId)!;
      assert.equal(finished.execution?.next, undefined);
      assert.equal(finished.decision?.id, decision.id);
    } finally {
      try { fs.writeFileSync(holdFile, 'release', 'utf8'); } catch {}
      for (const worker of workers) if (worker.exitCode === null && worker.signalCode === null) try { worker.kill(); } catch {}
      const workersClosed = Promise.all(workers.map((worker) => new Promise<void>((resolve) => {
        if (worker.exitCode !== null || worker.signalCode !== null) return resolve();
        worker.once('close', () => resolve());
      })));
      let closeTimer: NodeJS.Timeout | undefined;
      const closed = await Promise.race([
        workersClosed.then(() => true),
        new Promise<boolean>((resolve) => { closeTimer = setTimeout(() => resolve(false), 15_000); }),
      ]);
      if (closeTimer) clearTimeout(closeTimer);
      if (!closed) {
        for (const worker of workers) if (worker.exitCode === null && worker.signalCode === null) try { worker.kill(); } catch {}
        let finalCloseTimer: NodeJS.Timeout | undefined;
        await Promise.race([
          workersClosed,
          new Promise<void>((resolve) => { finalCloseTimer = setTimeout(resolve, 5_000); }),
        ]);
        if (finalCloseTimer) clearTimeout(finalCloseTimer);
      }
      try {
        if (fs.existsSync(spawnedFile) && fs.existsSync(spawnedBirthFile)) {
          const pid = Number(fs.readFileSync(spawnedFile, 'utf8'));
          const birthId = fs.readFileSync(spawnedBirthFile, 'utf8').trim();
          if (pid > 0 && birthId && probeProcessIdentity(pid, birthId) !== 'exited') {
            await stopVerifiedChildTree(pid, true, undefined, birthId, 5_000, [], true);
          }
        }
      } catch {}
      store.dispose();
      cleanTempDir(storageDir);
    }
  });

  test('two real controls retain durable next while a captured grandchild survives exited owner and child', async () => {
    const storageDir = createTempDir('orphan-next-live-grandchild');
    const threadId = 'thread_orphan_next_live_grandchild';
    const barrierFile = path.join(storageDir, 'grandchild-recovery.start');
    const holdFile = path.join(storageDir, 'grandchild-recovery.release');
    const registeredFile = path.join(storageDir, 'grandchild-recovery.registered');
    const spawnedFile = path.join(storageDir, 'grandchild-recovery.pid');
    const spawnedBirthFile = path.join(storageDir, 'grandchild-recovery.birth');
    const descendantFile = path.join(storageDir, 'grandchild.pid');
    const workers: child_process.ChildProcess[] = [];
    const store = new TaskStore(storageDir);
    let oldOwner: child_process.ChildProcess | undefined;
    let oldChildPid: number | undefined;
    let oldChildBirthId: string | undefined;
    let grandchildPid: number | undefined;
    let grandchildBirthId: string | undefined;

    try {
      const grandchildScript = 'setInterval(()=>{},1000)';
      const oldChildScript = `const cp=require('child_process');const fs=require('fs');const d=cp.spawn(process.execPath,['-e',${JSON.stringify(grandchildScript)}],{detached:true,windowsHide:true,stdio:'ignore'});d.unref();fs.writeFileSync(${JSON.stringify(descendantFile)},String(d.pid));setInterval(()=>{},1000);`;
      oldOwner = child_process.spawn(process.execPath, ['-e', `const cp=require('child_process');const c=cp.spawn(process.execPath,['-e',${JSON.stringify(oldChildScript)}],{detached:true,windowsHide:true,stdio:'ignore'});c.unref();process.stdout.write(String(c.pid));setInterval(()=>{},1000);`], {
        windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'],
      });
      let childText = '';
      oldOwner.stdout?.on('data', (chunk) => { childText += chunk.toString(); });
      await waitForFile(descendantFile, 30_000);
      const actualChildPidDeadline = Date.now() + 10_000;
      while (!childText.trim() && Date.now() < actualChildPidDeadline) await new Promise((resolve) => setTimeout(resolve, 10));
      const actualChildPid = Number(childText.trim());
      oldChildPid = actualChildPid;
      grandchildPid = Number(fs.readFileSync(descendantFile, 'utf8'));
      const ownerPid = oldOwner.pid!;
      const ownerBirthId = getProcessBirthId(ownerPid);
      const childBirthId = getProcessBirthId(actualChildPid);
      oldChildBirthId = childBirthId;
      grandchildBirthId = getProcessBirthId(grandchildPid);
      assert.ok(ownerBirthId && childBirthId && grandchildBirthId, 'old owner, root child, and captured grandchild identities are observable');
      // Stop the recorded owner and child roots while keeping the detached grandchild alive.
      try { oldOwner.kill(); } catch {}
      await new Promise<void>((resolve) => oldOwner!.once('close', () => resolve()));
      // The child root is detached; terminate only that exact PID after its birthId was captured.
      try { process.kill(actualChildPid, 'SIGKILL'); } catch {}
      const childExitedDeadline = Date.now() + 10_000;
      while (probeProcessIdentity(actualChildPid, childBirthId) !== 'exited' && Date.now() < childExitedDeadline) await new Promise((resolve) => setTimeout(resolve, 20));
      assert.equal(probeProcessIdentity(ownerPid, ownerBirthId), 'exited');
      assert.equal(probeProcessIdentity(actualChildPid, childBirthId), 'exited');
      assert.equal(probeProcessIdentity(grandchildPid, grandchildBirthId), 'alive');

      const thread = createV2Thread(threadId, storageDir, 'review');
      const now = new Date().toISOString();
      thread.status = 'running';
      thread.execution = {
        ...thread.execution!,
        revision: 10,
        operationId: 'review-operation-grandchild',
        phase: 'review',
        round: 1,
        run: {
          id: 'review-run-grandchild', role: 'reviewer',
          owner: { pid: ownerPid, birthId: ownerBirthId },
          child: { pid: actualChildPid, birthId: childBirthId },
          subtrees: [{ root: { pid: actualChildPid, birthId: childBirthId }, descendants: [{ pid: grandchildPid, birthId: grandchildBirthId }] }],
          state: 'active', step: 'executing', startedAt: now, registeredAt: now,
        },
        next: { phase: 'develop', round: 2, sourceRunId: 'review-run-grandchild' },
      };
      thread.currentRound = 1;
      const decision = {
        id: 'durable-rework-grandchild', operationId: 'review-operation-grandchild', generation: 1,
        phase: 'review' as const, round: 1, question: 'Make a correction', resumePoint: 'continue_develop' as const,
        createdAt: now, answer: 'rework requested', answeredAt: now, answeredBy: 'reviewer-session',
      };
      thread.decision = decision;
      thread.decisions = [decision];
      store.upsertTask(thread);

      const workerScript = path.resolve(__dirname, 'fixtures/two-process-runner.js');
      const launchWorker = (workerId: string) => {
        const worker = child_process.spawn(process.execPath, [
          workerScript, `--storage-dir=${storageDir}`, `--thread-id=${threadId}`, `--worker-id=${workerId}`,
          '--phase=develop', `--barrier-file=${barrierFile}`, `--hold-file=${holdFile}`,
          `--registered-file=${registeredFile}`, `--spawned-file=${spawnedFile}`, `--spawned-birth-file=${spawnedBirthFile}`,
        ], { stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
        workers.push(worker);
        return worker;
      };
      const first = launchWorker('grandchild_1');
      const second = launchWorker('grandchild_2');
      let firstOut = ''; let secondOut = '';
      first.stdout?.on('data', (chunk) => { firstOut += chunk.toString(); });
      second.stdout?.on('data', (chunk) => { secondOut += chunk.toString(); });
      const closed = (worker: child_process.ChildProcess) => new Promise<number | null>((resolve) => worker.once('close', resolve));
      const firstClosed = closed(first); const secondClosed = closed(second);
      await Promise.all([waitForFile(`${barrierFile}.grandchild_1.ready`, 30_000), waitForFile(`${barrierFile}.grandchild_2.ready`, 30_000)]);
      fs.writeFileSync(barrierFile, 'start', 'utf8');
      const [firstCode, secondCode] = await Promise.all([firstClosed, secondClosed]);
      assert.equal(firstCode, 0, firstOut);
      assert.equal(secondCode, 0, secondOut);
      const parse = (output: string) => {
        const line = output.split('\n').find((value) => value.startsWith('RESULT:'));
        return line ? JSON.parse(line.slice(7)) : undefined;
      };
      const results = [parse(firstOut), parse(secondOut)];
      assert.ok(results.every(Boolean), `both controls report: ${firstOut} ${secondOut}`);
      assert.ok(results.every((result) => !result.started && result.reason === 'run_busy' && result.spawnCount === 0));
      const retained = store.reloadTask(threadId)!;
      assert.equal(retained.execution?.run?.id, 'review-run-grandchild');
      assert.equal(retained.execution?.next?.sourceRunId, 'review-run-grandchild');
      assert.equal(retained.decision?.id, decision.id);
      assert.equal(probeProcessIdentity(grandchildPid, grandchildBirthId), 'alive');
    } finally {
      try { fs.writeFileSync(holdFile, 'release', 'utf8'); } catch {}
      for (const worker of workers) if (worker.exitCode === null && worker.signalCode === null) try { worker.kill(); } catch {}
      await Promise.all(workers.map((worker) => new Promise<void>((resolve) => {
        if (worker.exitCode !== null || worker.signalCode !== null) return resolve();
        worker.once('close', () => resolve());
      })));
      if (grandchildPid && grandchildBirthId) {
        try { await killPidTree(grandchildPid); } catch {}
      }
      if (oldChildPid && oldChildBirthId && probeProcessIdentity(oldChildPid, oldChildBirthId) !== 'exited') {
        try { process.kill(oldChildPid, 'SIGKILL'); } catch {}
      }
      if (oldOwner && oldOwner.exitCode === null && oldOwner.signalCode === null) try { oldOwner.kill(); } catch {}
      store.dispose();
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 1 (Negative evidence): starting, active, stopping block
  // -------------------------------------------------------------------------
  test('Negative evidence: run in starting state blocks alternative run without incrementing attempt', async () => {
    const storageDir = createTempDir('run-starting-block');
    const threadId = 'thread_starting_block';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_other_owner',
        role: 'developer',
        owner: { pid: 999998, birthId: 'birth_other' },
        state: 'starting',
        step: 'starting',
        startedAt: new Date().toISOString(),
      };
      thread.execution!.attempt = 1;
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const mockRunner: any = { execute: async () => assert.fail('Should not spawn') };
      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'run_busy');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.attempt, 1, 'Attempt must not be incremented on rejected start');
      assert.strictEqual(fresh?.execution?.run?.id, 'run_other_owner', 'Run must not be replaced');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Negative evidence: run in active state blocks alternative run without incrementing attempt', async () => {
    const storageDir = createTempDir('run-active-block');
    const threadId = 'thread_active_block';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_active_existing',
        role: 'developer',
        owner: { pid: 999998, birthId: 'birth_other' },
        child: { pid: 999997, birthId: 'birth_child' },
        state: 'active',
        step: 'executing',
        startedAt: new Date().toISOString(),
      };
      thread.execution!.attempt = 1;
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const mockRunner: any = { execute: async () => assert.fail('Should not spawn') };
      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'run_busy');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.attempt, 1);
      assert.strictEqual(fresh?.execution?.run?.id, 'run_active_existing');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Negative evidence: run in stopping state blocks alternative run without incrementing attempt', async () => {
    const storageDir = createTempDir('run-stopping-block');
    const threadId = 'thread_stopping_block';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_stopping_existing',
        role: 'developer',
        owner: { pid: 999998, birthId: 'birth_other' },
        child: { pid: 999997, birthId: 'birth_child' },
        state: 'stopping',
        step: 'stopping',
        startedAt: new Date().toISOString(),
      };
      thread.execution!.attempt = 1;
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const mockRunner: any = { execute: async () => assert.fail('Should not spawn') };
      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'run_busy');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.attempt, 1);
      assert.strictEqual(fresh?.execution?.run?.id, 'run_stopping_existing');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Duplicate request from same owner process returns already_running without extra attempt or budget reset', async () => {
    const storageDir = createTempDir('duplicate-owner-request');
    const threadId = 'thread_dup_request';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      const ownerPid = process.pid;
      const ownerBirthId = getProcessBirthId(ownerPid) || 'owner_birth';
      thread.execution!.run = {
        id: 'run_same_owner',
        role: 'developer',
        owner: { pid: ownerPid, birthId: ownerBirthId },
        state: 'starting',
        step: 'starting',
        startedAt: new Date().toISOString(),
      };
      thread.execution!.attempt = 1;
      thread.execution!.budget.spentActiveMs = 15000;
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const mockRunner: any = { execute: async () => assert.fail('Should not spawn') };
      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, true, `${JSON.stringify(result)} ${JSON.stringify(store.getTask(threadId)?.failure)}`);
      assert.strictEqual(result.reason, 'already_running');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.attempt, 1, 'Attempt must not be incremented on duplicate start');
      assert.strictEqual(fresh?.execution?.budget?.spentActiveMs, 15000, 'Budget must not be reset');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 1 & 4: Stale callbacks & identity validation
  // -------------------------------------------------------------------------
  test('Stale callbacks: child registration with mismatched runId, operationId, generation, or round is rejected', async (t) => {
    const storageDir = createTempDir('stale-callbacks');
    const threadId = 'thread_stale_callbacks';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_active_correct',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      const spawnChild = () => {
        // Keep the real process alive until the binding check reaches the CAS assertion.
        const c = child_process.spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], {
          windowsHide: true,
          stdio: 'ignore',
        });
        const closed = new Promise<void>((resolve) => c.once('close', () => resolve()));
        t.after(async () => {
          if (c.exitCode === null && c.signalCode === null) c.kill();
          await closed;
        });
        assert.ok(c.pid, 'the test child must spawn');
        const birthId = getProcessBirthId(c.pid);
        assert.ok(birthId, 'the test child must have a real birth identity');
        return { child: c, pid: c.pid, birthId };
      };

      // Stale runId
      const c1 = spawnChild();
      const staleRunRes = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'stale_run_id',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: c1.pid,
        childBirthId: c1.birthId,
        childProcess: c1.child,
      });
      assert.strictEqual(staleRunRes.success, false);
      assert.ok(staleRunRes.error?.includes('Run ID mismatch'));

      // Stale operationId
      const c2 = spawnChild();
      const staleOpRes = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_active_correct',
          operationId: 'stale_op_id',
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: c2.pid,
        childBirthId: c2.birthId,
        childProcess: c2.child,
      });
      assert.strictEqual(staleOpRes.success, false);
      assert.ok(staleOpRes.error?.includes('OperationId mismatch'));

      // Stale generation
      const c3 = spawnChild();
      const staleGenRes = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_active_correct',
          operationId: thread.execution!.operationId,
          generation: 999,
          phase: 'develop',
          round: 1,
        },
        childPid: c3.pid,
        childBirthId: c3.birthId,
        childProcess: c3.child,
      });
      assert.strictEqual(staleGenRes.success, false);
      assert.ok(staleGenRes.error?.includes('Generation mismatch'));

      // Stale round
      const c4 = spawnChild();
      const staleRoundRes = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_active_correct',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 999,
        },
        childPid: c4.pid,
        childBirthId: c4.birthId,
        childProcess: c4.child,
      });
      assert.strictEqual(staleRoundRes.success, false);
      assert.ok(staleRoundRes.error?.includes('Round mismatch'));
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('controller rejects a different child while preserving the first binding owner', async () => {
    const storageDir = createTempDir('different-child-binding');
    const threadId = 'thread_different_child_binding';
    let childA: child_process.ChildProcess | undefined;
    let childB: child_process.ChildProcess | undefined;
    let pidA: number | undefined;
    let pidB: number | undefined;
    let birthA: string | undefined;
    let birthB: string | undefined;
    let releaseAck!: () => void;
    const ackBarrier = new Promise<void>((resolve) => { releaseAck = resolve; });
    let resolveAckReached!: () => void;
    const ackReached = new Promise<void>((resolve) => { resolveAckReached = resolve; });
    let resolveBAttempted!: () => void;
    const bAttempted = new Promise<void>((resolve) => { resolveBAttempted = resolve; });
    let resolveARegistered!: () => void;
    const aRegistered = new Promise<void>((resolve) => { resolveARegistered = resolve; });
    let bError: any;
    let attachCalls = 0;
    let unbindCalls = 0;
    let phaseOutcomeCount = 0;
    let resolvePhaseOutcome!: () => void;
    const phaseOutcome = new Promise<void>((resolve) => { resolvePhaseOutcome = resolve; });
    let resolveUnconfirmed!: () => void;
    const unconfirmed = new Promise<void>((resolve) => { resolveUnconfirmed = resolve; });

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);
      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const originalAttachChild = registry.attachChildPid.bind(registry);
      (registry as any).attachChildPid = async (pid: number | undefined, registration: any) => {
        if (pid === undefined) unbindCalls++;
        else if (pid === pidA) {
          attachCalls++;
          resolveAckReached();
          await ackBarrier;
        }
        return originalAttachChild(pid, registration);
      };
      const originalTransition = store.transitionExecution.bind(store);
      store.transitionExecution = async (...args: any[]) => {
        const result = await (originalTransition as any)(...args);
        if (result?.execution?.run?.state === 'stopping' && result.execution.run.unconfirmed) resolveUnconfirmed();
        return result;
      };

      const runner: any = {
        clearPendingCancellation: () => undefined,
        cancelThread: () => true,
        isTransientError: () => false,
        isRateLimitOrQuotaError: () => false,
        execute: async (opts: any) => {
          childA = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
            windowsHide: true,
            stdio: 'ignore',
          });
          childB = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
            windowsHide: true,
            stdio: 'ignore',
          });
          pidA = childA.pid!;
          pidB = childB.pid!;
          birthA = getProcessBirthId(pidA);
          birthB = getProcessBirthId(pidB);
          assert.ok(birthA && birthB);
          bindSpawnedChildIdentity(childA, birthA);
          bindSpawnedChildIdentity(childB, birthB);
          const closedA = new Promise<void>((resolve) => childA!.once('close', () => resolve()));
          const registrationA = opts.onChildPid(pidA, childA, birthA);
          await ackReached;

          try {
            await opts.onChildPid(pidB, childB, birthB);
          } catch (error) {
            bError = error;
          } finally {
            resolveBAttempted();
          }

          await registrationA;
          resolveARegistered();
          childA!.kill();
          await closedA;
          await opts.onChildPid(undefined);
          return {
            exitCode: -1,
            stdout: 'controlled failure after first registration',
            stderr: '',
            extractedOutput: 'controlled failure after first registration',
            durationMs: 1,
            assessment: { success: false, errorCode: 'NONZERO_EXIT', exitCode: -1, stream: { hasActualOutput: true } },
          };
        },
      };

      setConfirmProcessTreeExitProvider(async () => true);
      const controller = new ExecutionController(
        store,
        runner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0, streamSyncIntervalMs: 60_000 },
      );
      (controller as any).handlePhaseOutcome = async () => {
        phaseOutcomeCount++;
        resolvePhaseOutcome();
      };

      const started = await controller.startExecution(threadId);
      assert.strictEqual(started.started, true);
      await ackReached;
      store.ensureUpToDate();
      const beforeB = store.getTask(threadId);
      const bindingRevision = beforeB?.execution?.revision;
      assert.strictEqual(beforeB?.execution?.run?.child?.pid, pidA);
      assert.strictEqual(beforeB?.execution?.run?.step, 'binding');

      await bAttempted;
      assert.ok(bError, 'the different-PID callback is rejected instead of borrowing A\'s promise');
      assert.strictEqual(bError.code, 'CHILD_BINDING_IN_PROGRESS');
      assert.strictEqual(attachCalls, 1, 'only A may complete the controller process-binding ACK');
      assert.strictEqual(unbindCalls, 0, 'B cannot clear A\'s process binding');
      assert.strictEqual(probeProcessIdentity(pidA!, birthA), 'alive', 'A remains alive and owned by its original binding');
      assert.strictEqual(probeProcessIdentity(pidB!, birthB), 'exited', 'B is stopped using its own captured identity');
      store.ensureUpToDate();
      const afterB = store.getTask(threadId);
      assert.strictEqual(afterB?.execution?.revision, bindingRevision, 'B cannot mutate A\'s ledger revision');
      assert.strictEqual(afterB?.execution?.run?.child?.pid, pidA);
      assert.strictEqual(afterB?.execution?.run?.step, 'binding');

      releaseAck();
      await aRegistered;
      store.ensureUpToDate();
      const afterA = store.getTask(threadId);
      assert.strictEqual(afterA?.execution?.run?.child?.pid, pidA);
      assert.strictEqual(afterA?.execution?.run?.state, 'active');
      assert.strictEqual(afterA?.execution?.run?.step, 'executing');
      assert.ok(afterA?.execution?.run?.registeredAt);

      const settledAs = await Promise.race([
        phaseOutcome.then(() => 'outcome' as const),
        unconfirmed.then(() => 'unconfirmed' as const),
      ]);
      if (settledAs === 'unconfirmed') {
        store.ensureUpToDate();
        const finalTask = store.getTask(threadId);
        assert.strictEqual(finalTask?.execution?.run?.state, 'stopping');
        assert.strictEqual(finalTask?.execution?.run?.unconfirmed, true);
        assert.strictEqual(phaseOutcomeCount, 0);
      } else {
        assert.strictEqual(phaseOutcomeCount, 1, 'only the original ACK owner may settle the phase outcome');
      }
      controller.dispose();
    } finally {
      releaseAck();
      setConfirmProcessTreeExitProvider(undefined);
      try { childA?.kill(); } catch {}
      try { childB?.kill(); } catch {}
      if (pidB && birthB && probeProcessIdentity(pidB, birthB) === 'alive') {
        try { process.kill(pidB, 'SIGKILL'); } catch {}
      }
      if (pidA && birthA && probeProcessIdentity(pidA, birthA) === 'alive') {
        try { process.kill(pidA, 'SIGKILL'); } catch {}
      }
      cleanTempDir(storageDir);
    }
  });

  test('Windows task helper retains exclusivity while serving other mutexes', { skip: process.platform !== 'win32' }, async () => {
    const storageDir = createTempDir('windows-task-lock');
    const first = new WindowsTaskLock(path.join(storageDir, 'first.lock'));
    const same = new WindowsTaskLock(path.join(storageDir, 'first.lock'));
    const other = new WindowsTaskLock(path.join(storageDir, 'other.lock'));
    try {
      const releaseFirst = await first.acquire();
      assert.ok(releaseFirst);
      const releaseOther = await other.acquire();
      assert.ok(releaseOther);
      await releaseOther();
      assert.strictEqual(await same.acquire(undefined, undefined, 100), undefined);
      await releaseFirst();
      const releaseSame = await same.acquire();
      assert.ok(releaseSame);
      await releaseSame();
    } finally {
      first.dispose();
      same.dispose();
      other.dispose();
      cleanTempDir(storageDir);
    }
  });

  test('duplicate child registration shares the single binding ACK owner under a real CAS barrier', async () => {
    const storageDir = createTempDir('duplicate-child-binding');
    const threadId = 'thread_duplicate_child_binding';
    let child: child_process.ChildProcess | undefined;
    let releaseAck!: () => void;
    const ackBarrier = new Promise<void>((resolve) => { releaseAck = resolve; });
    let resolveBindingPersisted!: () => void;
    const bindingPersisted = new Promise<void>((resolve) => { resolveBindingPersisted = resolve; });
    let firstAckCount = 0;
    let secondAckCount = 0;

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.status = 'running';
      thread.execution!.run = {
        id: 'run_duplicate_child_binding',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        spawnIntentAt: new Date().toISOString(),
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);
      const originalTransition = store.transitionExecution.bind(store);
      store.transitionExecution = async (...args: any[]) => {
        const result = await (originalTransition as any)(...args);
        if (result?.execution?.run?.id === 'run_duplicate_child_binding' && result.execution.run.step === 'binding') {
          resolveBindingPersisted();
        }
        return result;
      };

      child = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const childPid = child.pid!;
      const childBirthId = getProcessBirthId(childPid);
      assert.ok(childBirthId);
      const childClosed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
      const identity = {
        runId: 'run_duplicate_child_binding',
        operationId: thread.execution!.operationId,
        generation: thread.execution!.generation,
        phase: 'develop' as const,
        round: 1,
      };
      const baseOptions = {
        taskStore: store,
        threadId,
        identity,
        childPid,
        childBirthId,
        childProcess: child,
      };

      const firstRegistration = registerRunChildV2({
        ...baseOptions,
        attachLeases: async () => {
          firstAckCount++;
          await ackBarrier;
        },
      });
      await bindingPersisted;

      store.ensureUpToDate();
      const beforeDuplicate = store.getTask(threadId);
      const bindingRevision = beforeDuplicate?.execution?.revision;
      assert.strictEqual(beforeDuplicate?.execution?.run?.state, 'starting');
      assert.strictEqual(beforeDuplicate?.execution?.run?.step, 'binding');
      assert.strictEqual(beforeDuplicate?.execution?.run?.registeredAt, undefined);

      const duplicate = await registerRunChildV2({
        ...baseOptions,
        attachLeases: async () => { secondAckCount++; },
      });
      assert.strictEqual(duplicate.success, false);
      assert.strictEqual(duplicate.inProgress, true);
      assert.strictEqual(secondAckCount, 0, 'duplicate callback must not issue a second lease ACK');
      store.ensureUpToDate();
      const stillBinding = store.getTask(threadId);
      assert.strictEqual(stillBinding?.execution?.revision, bindingRevision, 'in-progress duplicate must not write a new revision');
      assert.strictEqual(stillBinding?.execution?.run?.state, 'starting');
      assert.strictEqual(stillBinding?.execution?.run?.step, 'binding');
      assert.strictEqual(stillBinding?.execution?.run?.registeredAt, undefined);
      assert.strictEqual(probeProcessIdentity(childPid, childBirthId), 'alive', 'duplicate registration must not stop the owner child');

      releaseAck();
      const firstResult = await firstRegistration;
      assert.strictEqual(firstResult.success, true, 'only the original ACK owner commits registration');
      assert.strictEqual(firstAckCount, 1);
      store.ensureUpToDate();
      const active = store.getTask(threadId);
      assert.strictEqual(active?.execution?.run?.state, 'active');
      assert.strictEqual(active?.execution?.run?.step, 'executing');
      assert.ok(active?.execution?.run?.registeredAt);

      await store.transitionExecution(threadId, active!.execution!.revision, (current) => {
        current.status = 'completed';
      });
      store.ensureUpToDate();
      const terminal = store.getTask(threadId);
      const terminalRevision = terminal?.execution?.revision;
      const lateDuplicate = await registerRunChildV2({
        ...baseOptions,
        attachLeases: async () => { secondAckCount++; },
      });
      assert.strictEqual(lateDuplicate.success, true);
      assert.strictEqual(lateDuplicate.alreadyRegistered, true);
      assert.strictEqual(secondAckCount, 0);
      store.ensureUpToDate();
      assert.strictEqual(store.getTask(threadId)?.status, 'completed');
      assert.strictEqual(store.getTask(threadId)?.execution?.revision, terminalRevision, 'late duplicate cannot rewrite terminal state');
      assert.strictEqual(probeProcessIdentity(childPid, childBirthId), 'alive');
      child.kill();
      await childClosed;
    } finally {
      releaseAck();
      try { child?.kill(); } catch {}
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 2: Pre-spawn invalidation (cancellation or cleanup)
  // -------------------------------------------------------------------------
  test('Pre-spawn invalidation: paused state before spawn aborts launch and releases resources', async () => {
    const storageDir = createTempDir('pause-before-spawn');
    const threadId = 'thread_pause_pre_spawn';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      let spawnAttempted = false;

      const mockRunner: any = {
        execute: async () => {
          spawnAttempted = true;
          return { exitCode: 0, stdout: '', stderr: '', extractedOutput: '', durationMs: 1 };
        },
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // Simulate a concurrent pause right after starting run acquisition by patching transitionExecution
      const origTransition = store.transitionExecution.bind(store);
      let acquiredStartingRun = false;

      store.transitionExecution = async (tId: string, rev: number, change: any) => {
        const res = await origTransition(tId, rev, change);
        if (!acquiredStartingRun && res.execution?.run?.state === 'starting') {
          acquiredStartingRun = true;
          // Concurrent pause occurs right after starting run acquired
          const rev2 = res.execution.revision;
          await origTransition(tId, rev2, (item) => {
            item.status = 'paused';
            item.interruption = {
              kind: 'paused',
              reason: 'User paused',
              at: new Date().toISOString(),
              by: 'user',
            };
          });
        }
        return res;
      };

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'paused');
      assert.strictEqual(spawnAttempted, false, 'Process spawn must NOT be attempted after pause');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.status, 'paused');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Pre-spawn invalidation: cleanup reservation before spawn aborts launch', async () => {
    const storageDir = createTempDir('cleanup-before-spawn');
    const threadId = 'thread_cleanup_pre_spawn';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      let spawnAttempted = false;

      const mockRunner: any = {
        execute: async () => {
          spawnAttempted = true;
          return { exitCode: 0, stdout: '', stderr: '', extractedOutput: '', durationMs: 1 };
        },
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const origTransition = store.transitionExecution.bind(store);
      let acquiredStartingRun = false;

      store.transitionExecution = async (tId: string, rev: number, change: any) => {
        const res = await origTransition(tId, rev, change);
        if (!acquiredStartingRun && res.execution?.run?.state === 'starting') {
          acquiredStartingRun = true;
          // Mark task cleanup right after starting run acquired
          await store.markTaskCleanup(tId, 'delete', { pid: process.pid, birthId: 'cleaner_birth' });
        }
        return res;
      };

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'cleanup_in_progress');
      assert.strictEqual(spawnAttempted, false, 'Process spawn must NOT be attempted after cleanup reservation');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 2: Delayed registration & early child exit
  // -------------------------------------------------------------------------
  test('Early child exit: fast exiting child does not skip registration handshake; settles after registration', async () => {
    const storageDir = createTempDir('early-exit');
    const threadId = 'thread_early_exit';
    let releaseRegistrationAck: (() => void) | undefined;
    let controlledChild: child_process.ChildProcess | undefined;
    let executionController: ExecutionController | undefined;

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      let registeredPid: number | undefined;
      let workerFinished = false;
      let childClosedFirst = false;
      const registrationAckBarrier = new Promise<void>((r) => { releaseRegistrationAck = r; });
      let resolveRegistrationWaiting!: () => void;
      const registrationWaiting = new Promise<void>((resolve) => { resolveRegistrationWaiting = resolve; });
      let resolveChildClosed!: () => void;
      const childClosed = new Promise<void>((resolve) => { resolveChildClosed = resolve; });
      let resolveWorkerFinished!: () => void;
      const workerFinishedPromise = new Promise<void>((resolve) => { resolveWorkerFinished = resolve; });
      let resolveUnbindAck!: () => void;
      const unbindAck = new Promise<void>((resolve) => { resolveUnbindAck = resolve; });
      let resolvePhaseOutcome!: () => void;
      const phaseOutcome = new Promise<void>((resolve) => { resolvePhaseOutcome = resolve; });
      const originalAttachChildPid = registry.attachChildPid.bind(registry);
      (registry as any).attachChildPid = async (pid: number | undefined, registration: any) => {
        if (pid === undefined) {
          resolveUnbindAck();
          return originalAttachChildPid(pid, registration);
        }
        resolveRegistrationWaiting();
        await registrationAckBarrier;
        return originalAttachChildPid(pid, registration);
      };

      const mockRunner: any = {
        clearPendingCancellation: () => undefined,
        cancelThread: () => true,
        isTransientError: () => false,
        isRateLimitOrQuotaError: () => false,
        execute: async (opts: any) => {
          // Close the real child while the durable run-child registration ACK is held.
          const child = child_process.spawn(
            process.execPath,
            ['-e', 'setInterval(() => {}, 1000)'],
            { windowsHide: true, stdio: 'ignore' },
          );
          controlledChild = child;
          registeredPid = child.pid;
          const closePromise = new Promise<void>((resolve) => child.once('close', () => resolve()));
          const childBirthId = child.pid ? getProcessBirthId(child.pid) : undefined;
          bindSpawnedChildIdentity(child, childBirthId);
          const registration = opts.onChildPid(child.pid, child, childBirthId);
          await closePromise;
          childClosedFirst = true;
          resolveChildClosed();
          await registration;

          workerFinished = true;
          resolveWorkerFinished();
          return {
            exitCode: -1,
            stdout: 'done',
            stderr: '',
            extractedOutput: 'done',
            durationMs: 400,
            error: 'Controlled child exited before execution completed',
            assessment: {
              success: false,
              errorCode: 'NONZERO_EXIT',
              exitCode: -1,
              stream: {
                chunkCount: 1,
                stdoutBytes: 4,
                stderrBytes: 0,
                hasActualOutput: true,
                continuous: true,
                gapExceeded: false,
                maxGapMs: 0,
                gapThresholdMs: 60000,
              },
            },
          };
        },
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );
      executionController = controller;
      const originalHandlePhaseOutcome = (controller as any).handlePhaseOutcome.bind(controller);
      (controller as any).handlePhaseOutcome = async (...args: any[]) => {
        try { await originalHandlePhaseOutcome(...args); }
        finally { resolvePhaseOutcome(); }
      };

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, true, `${JSON.stringify(result)} ${JSON.stringify(store.getTask(threadId)?.failure)}`);

      // Wait until the persisted binding reaches its explicit run-child ACK barrier;
      // the child remains alive here, so startup timing cannot race registration.
      await registrationWaiting;
      store.ensureUpToDate();
      const binding = store.getTask(threadId);
      assert.ok(binding?.execution?.run?.child?.pid, 'Child PID must be durably recorded before ACK');
      assert.strictEqual(binding?.execution?.run?.step, 'binding');
      assert.strictEqual(binding?.execution?.attempt, 1);

      controlledChild!.kill();
      await childClosed;
      assert.strictEqual(childClosedFirst, true, 'Real child process must close first');
      assert.strictEqual(workerFinished, false, 'Worker must NOT be settled while registration ACK barrier is held');

      // Now release registration ACK; the runner can settle only after the close
      // callback's registration promise and controller's unbind ACK complete.
      releaseRegistrationAck!();
      await workerFinishedPromise;
      assert.strictEqual(workerFinished, true, 'Worker must settle after registration ACK is released');
      await unbindAck;
      await phaseOutcome;
      assert.ok(registeredPid, 'The real child PID was captured');
      store.ensureUpToDate();
      assert.strictEqual(store.getTask(threadId)?.status, 'failed', 'the closed child produces a settled failed round');
      controller.dispose();
    } finally {
      releaseRegistrationAck?.();
      try { controlledChild?.kill(); } catch {}
      executionController?.dispose();
      cleanTempDir(storageDir);
    }
  });

  test('V2 leaves stale Bridge session leases untouched and retains runs when merge release is unconfirmed', async () => {
    const runReleaseFailure = async (kind: 'session' | 'merge') => {
      const storageDir = createTempDir(`release-unconfirmed-${kind}`);
      const threadId = `thread_release_unconfirmed_${kind}`;
      try {
        const store = new TaskStore(storageDir);
        const thread = createV2Thread(threadId, storageDir, kind === 'merge' ? 'merge' : 'develop');
        if (kind === 'merge') thread.queuePhase = 'merge';
        store.upsertTask(thread);

        const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
        const roleLock = new TaskRoleLock(storageDir);
        const sessionLock = new NativeSessionLock(storageDir);
        let staleSessionLease: ReturnType<NativeSessionLock['acquire']> | undefined;
        let staleSessionLeaseBytes: string | undefined;
        let releaseAttempted = false;
        let releaseRequested = false;
        let outcomeCount = 0;
        let resolveUnconfirmed!: () => void;
        const unconfirmed = new Promise<void>((resolve) => { resolveUnconfirmed = resolve; });
        const originalTransition = store.transitionExecution.bind(store);
        store.transitionExecution = async (...args: any[]) => {
          const result = await (originalTransition as any)(...args);
          if (result?.execution?.run?.state === 'stopping' && result.execution.run.unconfirmed) {
            resolveUnconfirmed();
          }
          return result;
        };

        const runner: any = {
          execute: async () => ({
            exitCode: 0,
            stdout: 'completed',
            stderr: '',
            extractedOutput: 'completed',
            durationMs: 1,
          }),
        };
        const fakeMergeLock: any = {
          acquire: async () => () => { releaseRequested = true; },
          getActiveHelperEvidence: () => ({ pid: process.pid, birthId: getProcessBirthId(process.pid), channelPath: path.join(storageDir, 'fake-helper-channel') }),
          attachChildPid: async () => undefined,
          dispose: () => undefined,
          releaseAndConfirm: async (release: (() => void) | undefined) => {
            releaseAttempted = true;
            release?.();
            return false;
          },
        };
        const mergeCoordinator: any = { getLock: () => fakeMergeLock };
        const controller = new ExecutionController(
          store,
          runner,
          {} as any,
          kind === 'merge' ? mergeCoordinator : undefined,
          roleLock,
          sessionLock,
          registry,
          { storageDir, standingScanIntervalMs: 0 },
        );
        if (kind === 'session') {
          staleSessionLease = sessionLock.acquire(thread.targetAgent, thread.receiverSessionId!);
          assert.ok(staleSessionLease);
          staleSessionLeaseBytes = fs.readFileSync(staleSessionLease.path, 'utf8');
          (sessionLock as any).release = () => { releaseAttempted = true; return false; };
        } else {
          (controller as any).executeMergeAdapter = async () => ({ ok: true });
        }
        (controller as any).handlePhaseOutcome = async () => { outcomeCount++; };

        const started = await controller.startExecution(threadId, kind === 'merge' ? 'merge' : undefined);
        assert.strictEqual(started.started, true);
        if (kind === 'session') {
          const deadline = Date.now() + 10_000;
          while (outcomeCount === 0 && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
          assert.strictEqual(outcomeCount, 1, 'the v2 developer run completes without consulting a Bridge session lease');
          assert.strictEqual(releaseAttempted, false, 'v2 must not release or rewrite a legacy session lease');
          assert.strictEqual(fs.readFileSync(staleSessionLease!.path, 'utf8'), staleSessionLeaseBytes);
          assert.strictEqual(roleLock.getOwner(threadId), undefined, 'v2 must not create a Bridge role lock');
          controller.dispose();
          return;
        }
        await unconfirmed;
        store.ensureUpToDate();
        const stopped = store.getTask(threadId);
        assert.strictEqual(stopped?.execution?.run?.state, 'stopping');
        assert.strictEqual(stopped?.execution?.run?.unconfirmed, true);
        assert.strictEqual(outcomeCount, 0, `${kind} release failure must suppress phase outcome`);
        assert.strictEqual(releaseAttempted, true);
        if (kind === 'merge') assert.strictEqual(releaseRequested, true);
        assert.ok(registry.getOwner(threadId), 'execution registration remains until release is confirmed');
        controller.dispose();
      } finally {
        cleanTempDir(storageDir);
      }
    };

    await runReleaseFailure('session');
    await runReleaseFailure('merge');
  });

  test('MeshFileLock release confirmation waits for the OS helper to release its lease', async () => {
    const root = createTempDir('merge-lock-release-confirmed');
    try {
      const lock = new MeshFileLock(path.join(root, 'merge.lock'));
      const release = await lock.acquire(undefined, undefined, 10_000);
      assert.ok(release, 'the OS-backed merge lock is acquired');
      assert.strictEqual(lock.isHeld, true);
      assert.strictEqual(await lock.releaseAndConfirm(release, 10_000), true);
      assert.strictEqual(lock.isHeld, false);
      assert.strictEqual(fs.existsSync(`${lock.lockPath}.owner.json`), false);
    } finally {
      cleanTempDir(root);
    }
  });

  test('Windows process birth IDs keep the legacy PowerShell FILETIME format for live child records', { skip: process.platform !== 'win32' }, async () => {
    const child = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 30000)'], {
      windowsHide: true,
      stdio: 'ignore',
    });
    try {
      assert.ok(child.pid);
      const legacyBirthId = child_process.execFileSync('powershell.exe', [
        '-NoProfile',
        '-NonInteractive',
        '-Command',
        `$p = Get-Process -Id ${child.pid} -ErrorAction SilentlyContinue; if ($p) { $p.StartTime.ToFileTimeUtc() }`,
      ], { encoding: 'utf8', windowsHide: true }).trim();
      const observedBirthId = getProcessBirthId(child.pid!);
      assert.match(legacyBirthId, /^\d+$/);
      assert.strictEqual(observedBirthId, legacyBirthId);
      assert.strictEqual(probeProcessIdentity(child.pid!, legacyBirthId), 'alive');
    } finally {
      try { child.kill(); } catch {}
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 2: Child identity unverifiable (missing birthId)
  // -------------------------------------------------------------------------
  test('Child identity unverifiable: missing spawn birthId is not repaired from the current PID', async () => {
    const storageDir = createTempDir('unverifiable-identity');
    const threadId = 'thread_unverifiable_id';
    let child: child_process.ChildProcess | undefined;

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_active_test',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      // Spawn a controlled live process. With no spawn-time birthId, registration
      // must fail closed instead of adopting its current OS identity.
      child = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const pid = child.pid!;

      // Attempt registration with empty childBirthId
      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_active_test',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: pid,
        childBirthId: '',
        childProcess: child,
      });

      assert.strictEqual(res.success, false);
      assert.ok(res.error?.includes('CHILD_IDENTITY_UNVERIFIABLE'));

      assert.strictEqual(res.unconfirmed, true, 'a missing immutable birthId leaves the run protected');
      assert.strictEqual(probeProcessIdentity(pid), 'alive', 'missing birthId must not authorize a PID-only stop');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.run?.child, undefined, 'Must not downgrade to PID-only registration');
      assert.strictEqual(fresh?.execution?.run?.unconfirmed, true);
    } finally {
      if (child) {
        try { child.kill(); } catch {}
        if (child.exitCode === null && child.signalCode === null) {
          await new Promise<void>((resolve) => child!.once('close', () => resolve()));
        }
      }
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 2 & 3: Crash window & launch safety assessment
  // -------------------------------------------------------------------------
  test('Crash window: provably not spawned is classified safe to clean', async () => {
    const run: ExecutionRun = {
      id: 'run_provably_not_spawned',
      role: 'developer',
      owner: { pid: 999999, birthId: 'dead_owner_birth' },
      state: 'starting',
      step: 'starting', // Before spawn intent
      startedAt: new Date().toISOString(),
    };

    // Owner is exited
    const assessment = assessRunLaunchEvidence(run, (_pid) => 'exited');
    assert.strictEqual(assessment.status, 'PROVABLY_NOT_SPAWNED');
    assert.strictEqual(assessment.safeToClean, true);
  });

  test('Crash window: owner exited during spawning is classified as POSSIBLY_SPAWNED_UNCONFIRMED and unsafe to clean', async () => {
    const storageDir = createTempDir('spawning-crash-window');
    const threadId = 'thread_spawning_crash';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_crashed_while_spawning',
        role: 'developer',
        owner: { pid: 999999, birthId: 'dead_owner_birth' },
        state: 'starting',
        step: 'spawning', // Spawn intent was recorded, but registration never completed!
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      // Owner is exited
      const assessment = assessRunLaunchEvidence(thread.execution!.run, (_pid) => 'exited');
      assert.strictEqual(assessment.status, 'POSSIBLY_SPAWNED_UNCONFIRMED');
      assert.strictEqual(assessment.safeToClean, false, 'Unproven child status must NOT be deemed safe to clean');

      // Record visible recovery failure and retain stopping protection
      await recordLaunchRecoveryFailure(
        store,
        threadId,
        'run_crashed_while_spawning',
        'Owner died while spawning before child registration completed',
      );

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.status, 'failed');
      assert.strictEqual(fresh?.failure?.code, 'LAUNCH_UNCONFIRMED');
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping', 'Run must be retained in stopping state to prevent duplicate spawn');
      assert.strictEqual(fresh?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Cannot overwrite an active in-flight child process with a new child', async () => {
    const storageDir = createTempDir('no-child-overwrite');
    const threadId = 'thread_no_overwrite';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');

      // Spawn an active child process
      const liveChild = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 30000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const livePid = liveChild.pid!;
      const liveBirthId = getProcessBirthId(livePid);

      thread.execution!.run = {
        id: 'run_with_live_child',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        child: { pid: livePid, birthId: liveBirthId },
        state: 'active',
        step: 'executing',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      // Spawn second child and attempt to register it on same run
      const secondChild = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 30000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const secondPid = secondChild.pid!;
      const secondBirthId = getProcessBirthId(secondPid);

      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_with_live_child',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: secondPid,
        childBirthId: secondBirthId,
        childProcess: secondChild,
      });

      assert.strictEqual(res.success, false);
      assert.strictEqual(res.inProgress, true, 'a second child cannot acquire the existing run binding');
      assert.match(res.error || '', /already owns child|already owned by this run/);

      try { liveChild.kill(); } catch {}
      try { secondChild.kill(); } catch {}
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Acceptance Criterion 3: Production default task (v1) isolation
  // -------------------------------------------------------------------------
  test('Production tasks on execution.version=1 do not use v2 path', async () => {
    const storageDir = createTempDir('v1-isolation');
    const threadId = 'thread_v1_isolation';

    try {
      const store = new TaskStore(storageDir);
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId,
        title: 'v1 Task',
        initiatorAgent: 'claude_code',
        targetAgent: 'codex',
        status: 'queued',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: `session-${threadId}`,
        reviewerSessionId: `rev-session-${threadId}`,
        callerSessionId: `rev-session-${threadId}`,
        reviewerSessionCaptured: true,
        reviewFlow: 'dedicated',
        reviewBootstrap: { phase: 'confirmed', attempts: 1 },
        summary: {
          lastPromptSnippet: 'v1 work',
          lastOutputSnippet: '',
          totalDurationMs: 0,
        },
        rounds: [{ round: 1, type: 'initial', prompt: 'v1 work', status: 'running', durationMs: 0, timestamp: now }],
        workspace: testWorkspace(storageDir),
        execution: {
          version: 1,
          revision: 1,
          operationId: 'op_v1_1',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 0,
          autoRetriesUsed: 0,
          budget: { limitMs: 60000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };
      store.upsertTask(thread);

      // Verify eligibility check explicitly rejects v1 for v2 execution
      const el = validateV2ExecutionEligibility(thread);
      assert.strictEqual(el.eligible, false);
      assert.strictEqual(el.code, 'LEGACY_VERSION_1');

      // Verify ExecutionController startExecution uses v1 path (takes roleLock)
      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      let executedV1 = false;

      const mockRunner: any = {
        execute: async (opts: any) => {
          executedV1 = true;
          if (opts.onChildPid) opts.onChildPid(process.pid);
          return { exitCode: 0, stdout: 'v1_done', stderr: '', extractedOutput: 'v1_done', durationMs: 10 };
        },
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, true);

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.version, 1, 'v1 task must NOT be upgraded to v2');
      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // -------------------------------------------------------------------------
  // Round 2 Regression Tests: Defect 1 to Defect 5
  // -------------------------------------------------------------------------

  // Defect 1: assessRunLaunchEvidence stopping run without child must return safeToClean=false
  test('Defect 1: assessRunLaunchEvidence returns safeToClean=false for stopping run with spawnIntentAt and exited owner and no child', () => {
    const run: ExecutionRun = {
      id: 'run_stopping_spawning',
      role: 'developer',
      owner: { pid: 999999, birthId: 'dead_owner' },
      state: 'stopping',
      step: 'spawning',
      spawnIntentAt: new Date().toISOString(),
      startedAt: new Date().toISOString(),
    };

    const assessment = assessRunLaunchEvidence(run, (_pid) => 'exited');
    assert.strictEqual(assessment.status, 'STOPPING_UNCONFIRMED');
    assert.strictEqual(assessment.safeToClean, false, 'Stopping run with spawnIntentAt and no child must be unsafe to clean');

    // Run marked with unconfirmed=true
    const unconfirmedRun: ExecutionRun = {
      ...run,
      unconfirmed: true,
    };
    const unconfirmedAssessment = assessRunLaunchEvidence(unconfirmedRun, (_pid) => 'exited');
    assert.strictEqual(unconfirmedAssessment.safeToClean, false, 'Unconfirmed run must never be safe to clean');
  });

  // Defect 2: Subtree verification protects foreign processes and marks unconfirmed
  test('Defect 2: Foreign PID (unverified subtree) is not killed, retains stopping with unconfirmed: true', async () => {
    const storageDir = createTempDir('foreign-pid-protection');
    const threadId = 'thread_foreign_pid';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_foreign_test',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      // Attempt registration with a foreign PID (e.g. PID 4 or 1 which is not our child)
      // verifySubtreeOwnership will return false
      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_foreign_test',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: 9999999, // Unverified foreign PID
      });

      assert.strictEqual(res.success, false);
      assert.strictEqual(res.unconfirmed, true);
      assert.ok(res.error?.includes('CHILD_IDENTITY_UNVERIFIABLE'));

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.status, 'failed');
      assert.strictEqual(fresh?.failure?.code, 'LAUNCH_UNCONFIRMED');
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping');
      assert.strictEqual(fresh?.execution?.run?.unconfirmed, true);
      assert.strictEqual(fresh?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Defect 2: PID/birthId mismatch does not stop a live replacement root or its child', async () => {
    const storageDir = createTempDir('mismatched-birthid');
    const threadId = 'thread_mismatched_birthid';
    let replacementRoot: child_process.ChildProcess | undefined;
    let replacementChildPid: number | undefined;

    try {
      const oldRoot = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      await new Promise<void>((resolve) => oldRoot.once('spawn', () => resolve()));
      const capturedBirthId = getProcessBirthId(oldRoot.pid!);
      assert.ok(capturedBirthId);
      const oldRootClosed = new Promise<void>((resolve) => oldRoot.once('close', () => resolve()));
      oldRoot.kill();
      await oldRootClosed;

      const grandchildScript = 'setInterval(()=>{},1000)';
      const replacementScript = `
        const cp = require('child_process');
        const descendant = cp.spawn(process.execPath, ['-e', ${JSON.stringify(grandchildScript)}], { windowsHide: true, stdio: 'ignore', detached: true });
        descendant.unref();
        process.stdout.write(String(descendant.pid));
        setInterval(()=>{},1000);
      `;
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_mismatch_test',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      replacementRoot = child_process.spawn(process.execPath, ['-e', replacementScript], {
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'ignore'],
      });
      let childText = '';
      let resolveChildPid!: () => void;
      const childPidReady = new Promise<void>((resolve) => { resolveChildPid = resolve; });
      replacementRoot.stdout?.on('data', (chunk) => {
        childText += chunk.toString();
        if (childText.trim()) resolveChildPid();
      });
      await childPidReady;
      replacementChildPid = Number(childText.trim());
      assert.ok(replacementChildPid > 0);
      const replacementBirthId = getProcessBirthId(replacementRoot.pid!);
      const replacementChildBirthId = getProcessBirthId(replacementChildPid);
      assert.ok(replacementBirthId && replacementChildBirthId);
      bindSpawnedChildIdentity(replacementRoot, replacementBirthId);
      assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, capturedBirthId), 'exited');
      assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, replacementBirthId), 'alive');
      assert.strictEqual(probeProcessIdentity(replacementChildPid, replacementChildBirthId), 'alive');

      const originalKill = process.kill.bind(process);
      const attemptedSigkills: number[] = [];
      process.kill = ((pid: number, signal?: number | NodeJS.Signals) => {
        if (signal === 'SIGKILL') {
          attemptedSigkills.push(pid);
          return true;
        }
        return originalKill(pid, signal as any);
      }) as typeof process.kill;
      let res: Awaited<ReturnType<typeof registerRunChildV2>>;
      try {
        res = await registerRunChildV2({
          taskStore: store,
          threadId,
          identity: {
            runId: 'run_mismatch_test',
            operationId: thread.execution!.operationId,
            generation: thread.execution!.generation,
            phase: 'develop',
            round: 1,
          },
          childPid: replacementRoot.pid!,
          childBirthId: capturedBirthId,
          childProcess: replacementRoot,
          childDescendants: [],
        });
      } finally {
        process.kill = originalKill as typeof process.kill;
      }

      assert.strictEqual(res!.success, false);
      assert.strictEqual(res!.unconfirmed, true);
      assert.ok(res!.error?.includes('PID_BIRTHID_MISMATCH'));
      assert.deepStrictEqual(attemptedSigkills, [], 'mismatched birthId cannot authorize replacement descendants');
      assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, replacementBirthId), 'alive');
      assert.strictEqual(probeProcessIdentity(replacementChildPid, replacementChildBirthId), 'alive');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.status, 'failed');
      assert.strictEqual(fresh?.failure?.code, 'LAUNCH_UNCONFIRMED');
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping');
      assert.strictEqual(fresh?.execution?.run?.unconfirmed, true);
    } finally {
      try { replacementRoot?.kill(); } catch {}
      if (replacementChildPid) try { process.kill(replacementChildPid, 'SIGKILL'); } catch {}
      cleanTempDir(storageDir);
    }
  });

  // Defect 3: Lease attachment rejection directionally stops child and marks stopping with unconfirmed
  test('Defect 3: Lease attachment rejection directionally stops child and marks stopping with unconfirmed', async () => {
    const storageDir = createTempDir('lease-rejection');
    const threadId = 'thread_lease_rejection';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_lease_test',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      const child = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 5000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const pid = child.pid!;
      const childClosed = new Promise<void>((resolve) => child.once('close', () => resolve()));
      const realBirthId = getProcessBirthId(pid);
      setProcessProbeProvider((candidatePid, expectedBirthId) => {
        const actualBirthId = candidatePid === pid ? realBirthId : getProcessBirthId(candidatePid);
        if (!actualBirthId) {
          try { process.kill(candidatePid, 0); return 'unknown'; }
          catch (error: any) { return error?.code === 'EPERM' ? 'unknown' : 'exited'; }
        }
        if (expectedBirthId && expectedBirthId !== actualBirthId) return 'exited';
        try { process.kill(candidatePid, 0); return 'alive'; }
        catch (error: any) { return error?.code === 'EPERM' ? 'unknown' : 'exited'; }
      });
      setConfirmProcessTreeExitProvider(async () => true);

      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_lease_test',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: pid,
        childBirthId: realBirthId,
        childProcess: child,
        attachLeases: async (_p) => {
          throw new Error('Lock helper IPC connection refused (simulated lease ACK failure)');
        },
      });

      assert.strictEqual(res.success, false);
      assert.ok(res.error?.includes('LEASE_ATTACHMENT_FAILED'));

      // Verified child must have been stopped before the failure is settled.
      await childClosed;
      assert.strictEqual(probeProcessIdentity(pid), 'exited');

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping');
      assert.strictEqual(fresh?.interruption?.reason, 'LEASE_ATTACHMENT_FAILED');
    } finally {
      setProcessProbeProvider(undefined);
      setConfirmProcessTreeExitProvider(undefined);
      cleanTempDir(storageDir);
    }
  });

  // Defect 4: CAS conflict cannot claim new generation
  test('Defect 4: CAS conflict cannot claim new generation', async () => {
    const storageDir = createTempDir('generation-mismatch');
    const threadId = 'thread_gen_mismatch';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.generation = 1;
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      const mockRunner: any = {
        execute: async () => ({ exitCode: 0, stdout: '', stderr: '', extractedOutput: '', durationMs: 0 }),
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // Now update the task in the store to generation 2 before starting with generation 1
      store.transitionExecution(threadId, thread.execution!.revision, (t) => {
        t.execution!.generation = 2;
      });

      // Call startExecution specifying generation 1
      const res = await controller.startExecution(threadId, 'develop', { generation: 1 });
      assert.strictEqual(res.started, false);
      assert.ok(res.reason?.includes('GENERATION_MISMATCH'), `Expected GENERATION_MISMATCH, got: ${res.reason}`);

      // Verify task on disk was not claimed or corrupted
      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.generation, 2);
      assert.strictEqual(fresh?.execution?.run, undefined);
      assert.strictEqual(fresh?.execution?.attempt, 0, 'Attempt count must not be incremented on rejection');

      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // Defect 4: Pre-spawn invalidation when task is paused after spawn intent
  test('Defect 4: Pre-spawn invalidation when task is paused or marked cleanup after spawn intent', async () => {
    const storageDir = createTempDir('pre-spawn-invalidation');
    const threadId = 'thread_pre_spawn_inv';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      let runnerSpawned = false;
      const mockRunner: any = {
        execute: async () => {
          runnerSpawned = true;
          return { exitCode: 0, stdout: '', stderr: '', extractedOutput: '', durationMs: 0 };
        },
      };

      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // Start execution; right after startExecution persists spawn intent, pause the task
      // Simulate pause happening right after spawn intent:
      await store.transitionExecution(threadId, store.getTask(threadId)!.execution!.revision, (t) => {
        t.execution!.run = {
          id: 'run_pre_spawn_test',
          role: 'developer',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'starting',
          step: 'spawning',
          spawnIntentAt: new Date().toISOString(),
          startedAt: new Date().toISOString(),
        };
        t.status = 'paused'; // Task paused!
      });

      // Now run develop adapter directly or through controller
      const adapterRes = await (controller as any).executeDevelopAdapter(
        threadId,
        'run_pre_spawn_test',
        undefined,
        () => {},
        { operationId: thread.execution!.operationId, generation: 1, round: 1 },
      );

      assert.strictEqual(adapterRes.ok, false);
      assert.strictEqual(adapterRes.code, 'INVALIDATED_BEFORE_SPAWN');
      assert.strictEqual(runnerSpawned, false, 'Runner must not spawn child process when invalidated');

      // Verify task status remained paused
      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.status, 'paused');

      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // Defect 4: Late child registration cannot revive stopping run
  test('Defect 4: Late child registration cannot revive stopping or failed run', async () => {
    const storageDir = createTempDir('no-stopping-revival');
    const threadId = 'thread_no_revival';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.execution!.run = {
        id: 'run_already_stopping',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'stopping',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      thread.status = 'failed';
      store.upsertTask(thread);

      const child = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 5000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const pid = child.pid!;
      const realBirthId = getProcessBirthId(pid);

      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_already_stopping',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: pid,
        childBirthId: realBirthId,
        childProcess: child,
      });

      assert.strictEqual(res.success, false);

      // Verify run is still stopping and status is still failed
      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping');
      assert.strictEqual(fresh?.status, 'failed');
      assert.notStrictEqual(fresh?.execution?.run?.state, 'active');

      try { child.kill(); } catch {}
    } finally {
      cleanTempDir(storageDir);
    }
  });

  // Defect 5: probeRun on v1 tasks retains legacy behavior where exited owner with no child returns exited
  test('Defect 5: probeRun on v1 tasks retains legacy behavior where exited owner with no child returns exited', () => {
    const storageDir = createTempDir('v1-proberun');
    const threadId = 'thread_v1_probe';

    try {
      const store = new TaskStore(storageDir);
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId,
        title: 'v1 Probe Task',
        initiatorAgent: 'claude_code',
        targetAgent: 'codex',
        receiverSessionId: 'sess_v1_probe',
        status: 'queued',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [],
        workspace: testWorkspace(storageDir),
        execution: {
          version: 1, // Legacy v1
          revision: 1,
          operationId: 'op_v1_probe',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 0,
          autoRetriesUsed: 0,
          budget: { limitMs: 60000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      const controller = new ExecutionController(
        store,
        {} as any,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // A legacy v1 run where owner PID 999999 has exited and child is undefined
      const v1Run: ExecutionRun = {
        id: 'run_v1_exited',
        role: 'developer',
        owner: { pid: 999999, birthId: 'dead' },
        state: 'starting',
        step: 'starting',
        startedAt: now,
      };

      const probeResult = controller.probeRun(threadId, v1Run);
      assert.strictEqual(probeResult, 'exited', 'v1 task must retain legacy exited verdict');

      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Defect 1: scanAndDrain routes orphan v2 runs through launch recovery and retains unconfirmed identity', async () => {
    const storageDir = createTempDir('v2-scan-isolation');
    const threadId = 'thread_v2_scan_isolation';

    try {
      const deadOwner = child_process.spawn(process.execPath, ['-e', 'setTimeout(() => {}, 60000)'], {
        windowsHide: true, stdio: 'ignore',
      });
      await new Promise<void>((resolve, reject) => {
        deadOwner.once('error', reject);
        deadOwner.once('spawn', () => resolve());
      });
      const deadOwnerPid = deadOwner.pid!;
      let deadOwnerBirthId = getProcessBirthId(deadOwnerPid);
      for (let i = 0; !deadOwnerBirthId && i < 20; i++) {
        await new Promise((resolve) => setTimeout(resolve, 25));
        deadOwnerBirthId = getProcessBirthId(deadOwnerPid);
      }
      assert.ok(deadOwnerBirthId, 'test owner must have a verifiable birth identity');
      const deadOwnerClosed = new Promise<void>((resolve) => deadOwner.once('close', () => resolve()));
      deadOwner.kill();
      await deadOwnerClosed;

      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      const now = new Date().toISOString();
      thread.status = 'running';
      thread.execution!.run = {
        id: 'run_crashed_owner',
        role: 'developer',
        owner: { pid: deadOwnerPid, birthId: deadOwnerBirthId },
        state: 'starting',
        step: 'spawning',
        spawnIntentAt: now,
        startedAt: now,
      };
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      const controller = new ExecutionController(
        store,
        {} as any,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // The scanner uses the v2 recovery classifier; an indeterminate spawn is
      // made visible and retained rather than erased or replaced.
      await controller.scanAndDrain();

      store.ensureUpToDate();
      let current = store.getTask(threadId);
      assert.strictEqual(current?.status, 'failed', 'orphan spawn intent must become a visible failure');
      assert.strictEqual(current?.failure?.code, 'LAUNCH_UNCONFIRMED');
      assert.strictEqual(current?.execution?.run?.id, 'run_crashed_owner', 'scanAndDrain must preserve uncertain run identity');
      assert.strictEqual(current?.execution?.run?.state, 'stopping');

      // Explicit entry agrees and cannot replace the uncertain run.
      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'terminal');

      store.ensureUpToDate();
      current = store.getTask(threadId);
      assert.strictEqual(current?.status, 'failed', 'Task must remain visibly failed');
      assert.strictEqual(current?.failure?.code, 'LAUNCH_UNCONFIRMED', 'Failure code must be LAUNCH_UNCONFIRMED');
      assert.strictEqual(current?.execution?.run?.state, 'stopping', 'Run state must be stopping');
      assert.strictEqual(current?.execution?.run?.unconfirmed, true, 'Run must be marked unconfirmed');
      assert.strictEqual(current?.execution?.run?.id, 'run_crashed_owner', 'Run must be preserved');

      // 3. Subsequent attempts to start alternative run continue to be rejected
      const retryResult = await controller.startExecution(threadId, undefined, { allowStates: ['failed'] });
      assert.strictEqual(retryResult.started, false);
      assert.strictEqual(retryResult.reason, 'run_busy');

      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('captured v2 session without static ownership evidence is rejected before spawn', async () => {
    const storageDir = createTempDir('captured-session-no-binding');
    const threadId = 'thread_captured_session_no_binding';
    let spawnAttempted = false;
    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.receiverSessionCaptured = true;
      fs.rmSync(path.join(storageDir, 'session-bindings'), { recursive: true, force: true });
      thread.nativeSessionAssignments = {
        developer: { namespace: nativeSessionNamespace('codex'), sessionEpoch: 4 },
      };
      store.upsertTask(thread);
      const runner: any = { execute: async () => { spawnAttempted = true; return { exitCode: 0 }; } };
      const controller = new ExecutionController(
        store, runner, {} as any, undefined,
        new TaskRoleLock(storageDir), new NativeSessionLock(storageDir),
        new TaskExecutionRegistry(storageDir), { storageDir, standingScanIntervalMs: 0 },
      );

      const result = await controller.startExecution(threadId, 'develop');
      assert.equal(result.started, false);
      assert.equal(result.reason, 'SESSION_OWNERSHIP_UNVERIFIED');
      assert.equal(spawnAttempted, false);
      assert.equal(fs.existsSync(path.join(storageDir, 'session-bindings')), false, 'verification must not create an implicit claim');
      assert.equal(store.getTask(threadId)?.receiverSessionCaptured, true);
      controller.dispose();
      store.dispose();
    } finally { cleanTempDir(storageDir); }
  });

  test('Defect 1: Real control process crash between spawn and registration leaves run stopping/unconfirmed and blocks replacement', async () => {
    const storageDir = createTempDir('control-crash-window');
    const threadId = 'thread_control_crash';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);

      // Spawn a real short-lived external control process that starts the task and exits immediately after spawn intent
      const taskStoreModulePath = path.resolve(__dirname, '../bridge/task-store').replace(/\\/g, '/');
      const script = `
        const { TaskStore } = require(${JSON.stringify(taskStoreModulePath)});
        const store = new TaskStore(${JSON.stringify(storageDir)});
        store.ensureUpToDate();
        const t = store.getTask(${JSON.stringify(threadId)});
        const rev = t.execution.revision;
        store.transitionExecution(${JSON.stringify(threadId)}, rev, (task) => {
          task.status = 'running';
          task.execution.run = {
            id: 'run_simulated_crashed_control',
            role: 'developer',
            owner: { pid: process.pid, birthId: String(Date.now()) },
            state: 'starting',
            step: 'spawning',
            spawnIntentAt: new Date().toISOString(),
            startedAt: new Date().toISOString(),
          };
        }).then(() => process.exit(0)).catch((err) => { console.error(err); process.exit(1); });
      `;

      let helperErr = '';
      const helper = child_process.spawn(process.execPath, ['-e', script], {
        windowsHide: true,
        stdio: ['ignore', 'ignore', 'pipe'],
      });
      helper.stderr?.on('data', (d) => { helperErr += d.toString(); });
      const helperExitCode = await new Promise<number | null>((resolve) => helper.on('close', resolve));
      assert.strictEqual(helperExitCode, 0, `Helper control process must exit cleanly: ${helperErr}`);

      // The external control process has exited now!
      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);

      const controller = new ExecutionController(
        store,
        {} as any,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      // Subsequent call to startExecution on this task:
      const result = await controller.startExecution(threadId);
      assert.strictEqual(result.started, false);
      assert.strictEqual(result.reason, 'run_busy');

      store.ensureUpToDate();
      const current = store.getTask(threadId);
      assert.strictEqual(current?.status, 'failed');
      assert.strictEqual(current?.failure?.code, 'LAUNCH_UNCONFIRMED');
      assert.strictEqual(current?.execution?.run?.id, 'run_simulated_crashed_control');
      assert.strictEqual(current?.execution?.run?.state, 'stopping');
      assert.strictEqual(current?.execution?.run?.unconfirmed, true);

      controller.dispose();
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Defect 2: verifySubtreeOwnership rejects host PID and sibling tasks under same host', async () => {
    // 1. Host PID rejection
    assert.strictEqual(verifySubtreeOwnership(process.pid), false, 'Host PID must be strictly rejected');

    // 2. Sibling tasks under same host
    const childA = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 5000)'], {
      windowsHide: true,
      stdio: 'ignore',
    });
    const childB = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 5000)'], {
      windowsHide: true,
      stdio: 'ignore',
    });

    try {
      const pidA = childA.pid!;
      const pidB = childB.pid!;

      // childA belongs to childA handle
      assert.strictEqual(verifySubtreeOwnership(pidA, childA), true);
      // childB does NOT belong to childA handle
      assert.strictEqual(verifySubtreeOwnership(pidB, childA), false);
      // childA does NOT belong to childB handle
      assert.strictEqual(verifySubtreeOwnership(pidA, childB), false);
    } finally {
      childA.kill();
      childB.kill();
    }
  });

  test('Defect 2: stopVerifiedChildTree pre-collects multi-generation descendant tree and stops all', async () => {
    const root = createTempDir('stop-child-tree');
    const readyFile = path.join(root, 'grandchild-ready');
    const grandchildScript = `setTimeout(()=>{}, 30000)`;
    const parentScript = `
      const cp = require('child_process');
      const fs = require('fs');
      const g = cp.spawn(process.execPath, ['-e', ${JSON.stringify(grandchildScript)}], { windowsHide: true, stdio: 'ignore' });
      fs.writeFileSync(${JSON.stringify(readyFile)}, String(g.pid));
      process.stdout.write(String(g.pid));
      setTimeout(()=>{}, 30000);
    `;
    const parent = child_process.spawn(process.execPath, ['-e', parentScript], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });

    let grandchildPidStr = '';
    let grandchildPid: number | undefined;
    let resolvePidOutput!: () => void;
    const pidOutput = new Promise<void>((resolve) => { resolvePidOutput = resolve; });
    parent.stdout?.on('data', (d) => {
      grandchildPidStr += d.toString();
      if (grandchildPidStr.trim()) resolvePidOutput();
    });

    try {
      await Promise.all([waitForFile(readyFile), pidOutput]);
      const parentPid = parent.pid!;
      const pid = parseInt(grandchildPidStr.trim(), 10);
      grandchildPid = pid;
      assert.ok(pid > 0, `Grandchild must have been spawned; got "${grandchildPidStr}"`);

      const parentBirthId = getProcessBirthId(parentPid);
      const capturedDescendants = await captureChildDescendants(parentPid);
      assert.ok(capturedDescendants.some((identity) => identity.pid === pid), 'the grandchild identity is captured before the root exits');
      const knownBirthIds = new Map<number, string | undefined>([
        [parentPid, parentBirthId],
        ...capturedDescendants.map((identity) => [identity.pid, identity.birthId] as [number, string | undefined]),
      ]);
      let grandchildWasProbed = false;
      setProcessProbeProvider((candidatePid, expectedBirthId) => {
        if (candidatePid === pid) grandchildWasProbed = true;
        const actualBirthId = knownBirthIds.has(candidatePid)
          ? knownBirthIds.get(candidatePid)
          : getProcessBirthId(candidatePid);
        if (!actualBirthId) {
          try { process.kill(candidatePid, 0); return 'unknown'; }
          catch (error: any) { return error?.code === 'EPERM' ? 'unknown' : 'exited'; }
        }
        if (expectedBirthId && expectedBirthId !== actualBirthId) return 'exited';
        try { process.kill(candidatePid, 0); return 'alive'; }
        catch (error: any) { return error?.code === 'EPERM' ? 'unknown' : 'exited'; }
      });
      let treeConfirmationCalled = false;
      setConfirmProcessTreeExitProvider(async (confirmedRoot) => {
        assert.strictEqual(confirmedRoot, parentPid);
        treeConfirmationCalled = true;
        return true;
      });

      const stopped = await stopVerifiedChildTree(
        parentPid,
        true,
        parent,
        parentBirthId,
        20_000,
        capturedDescendants,
      );

      assert.strictEqual(stopped, true, 'stopVerifiedChildTree must confirm every captured process identity');
      assert.strictEqual(grandchildWasProbed, true, 'a root-only exit signal cannot confirm the captured grandchild');
      assert.strictEqual(treeConfirmationCalled, true, 'the full subtree check runs after captured identities are confirmed');
      assert.strictEqual(probeProcessIdentity(parentPid, parentBirthId), 'exited', 'Parent must be exited');
      assert.strictEqual(probeProcessIdentity(pid, knownBirthIds.get(pid)), 'exited', 'Grandchild must be stopped as well');
    } finally {
      setProcessProbeProvider(undefined);
      setConfirmProcessTreeExitProvider(undefined);
      try { parent.kill(); } catch {}
      if (grandchildPid) try { killPidTree(grandchildPid); } catch {}
      cleanTempDir(root);
    }
  });

  test('Stale root PID never expands to a replacement subtree; captured reparented descendants remain stoppable', async () => {
    const oldRoot = child_process.spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], {
      windowsHide: true,
      stdio: 'ignore',
    });
    await new Promise<void>((resolve) => oldRoot.once('spawn', () => resolve()));
    const oldRootBirthId = getProcessBirthId(oldRoot.pid!);
    assert.ok(oldRootBirthId);
    const oldRootClosed = new Promise<void>((resolve) => oldRoot.once('close', () => resolve()));
    oldRoot.kill();
    await oldRootClosed;
    assert.strictEqual(verifySubtreeOwnership(oldRoot.pid!, oldRoot), false, 'a closed ChildProcess handle cannot claim fresh subtree ownership');

    const grandchildScript = 'setInterval(()=>{},1000)';
    const rootScript = `
      const cp = require('child_process');
      const child = cp.spawn(process.execPath, ['-e', ${JSON.stringify(grandchildScript)}], { windowsHide: true, stdio: 'ignore', detached: true });
      child.unref();
      process.stdout.write(String(child.pid));
      setInterval(()=>{},1000);
    `;
    const replacementRoot = child_process.spawn(process.execPath, ['-e', rootScript], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    const replacementRootClosed = new Promise<void>((resolve) => replacementRoot.once('close', () => resolve()));
    let replacementChildText = '';
    let resolveReplacementChild!: () => void;
    const replacementChildOutput = new Promise<void>((resolve) => { resolveReplacementChild = resolve; });
    replacementRoot.stdout?.on('data', (chunk) => {
      replacementChildText += chunk.toString();
      if (replacementChildText.trim()) resolveReplacementChild();
    });

    let replacementChildPid: number | undefined;
    try {
      await replacementChildOutput;
      replacementChildPid = Number(replacementChildText.trim());
      assert.ok(replacementChildPid > 0);
      const replacementBirthId = getProcessBirthId(replacementRoot.pid!);
      const childBirthId = getProcessBirthId(replacementChildPid);
      assert.ok(replacementBirthId && childBirthId);
      assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, oldRootBirthId), 'exited');
      assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, replacementBirthId), 'alive');

      const originalKill = process.kill.bind(process);
      const attemptedKills: number[] = [];
      process.kill = ((pid: number, signal?: number | NodeJS.Signals) => {
        if (signal === 'SIGKILL') {
          attemptedKills.push(pid);
          return true;
        }
        return originalKill(pid, signal as any);
      }) as typeof process.kill;
      try {
        killProcessTree(replacementRoot, oldRootBirthId);
        const staleRootStop = await stopVerifiedChildTree(
          replacementRoot.pid!, true, undefined, oldRootBirthId, 300,
        );
        assert.strictEqual(staleRootStop, true, 'the old root is already gone; no replacement subtree should be adopted');
        assert.deepStrictEqual(attemptedKills, [], 'no PID-only kill may target the replacement root or its child');
        assert.strictEqual(probeProcessIdentity(replacementRoot.pid!, replacementBirthId), 'alive');
        assert.strictEqual(probeProcessIdentity(replacementChildPid, childBirthId), 'alive');
      } finally {
        process.kill = originalKill as typeof process.kill;
      }
    } finally {
      try { replacementRoot.kill(); } catch {}
      if (replacementChildPid) try { process.kill(replacementChildPid, 'SIGKILL'); } catch {}
      await replacementRootClosed;
    }

    const reparentingRoot = child_process.spawn(process.execPath, ['-e', rootScript], {
      windowsHide: true,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
    let reparentedChildText = '';
    let resolveReparentedChild!: () => void;
    const reparentedChildOutput = new Promise<void>((resolve) => { resolveReparentedChild = resolve; });
    reparentingRoot.stdout?.on('data', (chunk) => {
      reparentedChildText += chunk.toString();
      if (reparentedChildText.trim()) resolveReparentedChild();
    });
    let reparentedChildPid: number | undefined;
    try {
      await reparentedChildOutput;
      reparentedChildPid = Number(reparentedChildText.trim());
      assert.ok(reparentedChildPid > 0);
      const rootBirthId = getProcessBirthId(reparentingRoot.pid!);
      const descendantBirthId = getProcessBirthId(reparentedChildPid);
      assert.ok(rootBirthId && descendantBirthId);
      assert.strictEqual(verifySubtreeOwnership(reparentedChildPid, reparentingRoot), true);
      const knownDescendant = [{ pid: reparentedChildPid, birthId: descendantBirthId }];

      const rootClosed = new Promise<void>((resolve) => reparentingRoot.once('close', () => resolve()));
      reparentingRoot.kill();
      await rootClosed;
      assert.strictEqual(probeProcessIdentity(reparentingRoot.pid!, rootBirthId), 'exited');
      assert.strictEqual(probeProcessIdentity(reparentedChildPid, descendantBirthId), 'alive');

      const stopped = await stopVerifiedChildTree(
        reparentingRoot.pid!, true, undefined, rootBirthId, 1_000, knownDescendant,
      );
      assert.strictEqual(stopped, true, 'a previously captured full identity remains stoppable after reparenting');
      assert.strictEqual(probeProcessIdentity(reparentedChildPid, descendantBirthId), 'exited');
    } finally {
      try { reparentingRoot.kill(); } catch {}
      if (reparentedChildPid) try { process.kill(reparentedChildPid, 'SIGKILL'); } catch {}
    }
  });

  test('Defect 3: Task transitioned to wait_decision during lease ACK wait preserves wait_decision status', async () => {
    const storageDir = createTempDir('wait-decision-race');
    const threadId = 'thread_wait_decision_race';

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.status = 'running';
      thread.execution!.run = {
        id: 'run_wait_race',
        role: 'developer',
        owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
        state: 'starting',
        step: 'spawning',
        startedAt: new Date().toISOString(),
      };
      store.upsertTask(thread);

      const child = child_process.spawn(process.execPath, ['-e', 'setTimeout(()=>{}, 5000)'], {
        windowsHide: true,
        stdio: 'ignore',
      });
      const pid = child.pid!;
      const birthId = getProcessBirthId(pid);

      const res = await registerRunChildV2({
        taskStore: store,
        threadId,
        identity: {
          runId: 'run_wait_race',
          operationId: thread.execution!.operationId,
          generation: thread.execution!.generation,
          phase: 'develop',
          round: 1,
        },
        childPid: pid,
        childBirthId: birthId,
        childProcess: child,
        attachLeases: async () => {
          // Simulate user decision requested during lease wait: status becomes wait_decision!
          store.ensureUpToDate();
          const t = store.getTask(threadId);
          const rev = t?.execution?.revision ?? 0;
          await store.transitionExecution(threadId, rev, (item) => {
            item.status = 'wait_decision';
          });
        },
      });

      assert.strictEqual(res.success, false);
      assert.ok(res.error?.includes('REGISTRATION_COMMIT_FAILED'));

      store.ensureUpToDate();
      const fresh = store.getTask(threadId);
      // MUST NOT be overwritten to running or failed(LAUNCH_UNCONFIRMED)
      assert.strictEqual(fresh?.status, 'wait_decision', 'Status must remain wait_decision');
      assert.strictEqual(fresh?.failure, undefined, 'Failure must not be set on wait_decision task');
      assert.strictEqual(fresh?.execution?.run?.state, 'stopping', 'Run state must be stopping');

      await new Promise((r) => setTimeout(r, 200));
      assert.strictEqual(probeProcessIdentity(pid), 'exited', 'Child process must be stopped');
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Defect 3: AutonomousRunner with controlled child command verifies early exit and binding timing', async () => {
    const storageDir = createTempDir('runner-controlled-child');
    const threadId = 'thread_runner_controlled';

    try {
      const runner = new AutonomousRunner(new AgentDetector());

      let onChildPidCalledWithPid = false;
      let onChildPidCalledWithUndefined = false;
      let receivedChildProcess = false;
      let releaseRegistrationAck: () => void;
      const registrationAckBarrier = new Promise<void>((r) => { releaseRegistrationAck = r; });
      let childSawClose = false;
      let resolveChildClose!: () => void;
      const childCloseSignal = new Promise<void>((resolve) => { resolveChildClose = resolve; });
      let runnerSettled = false;

      const script = `
        process.stdin.on('data', (d) => {
          process.stdout.write('ACK:' + d.toString());
          process.exit(0);
        });
      `;

      const runnerPromise = runner.execute({
        agent: 'claude_code',
        prompt: 'test prompt',
        taskThreadId: threadId,
        isRework: false,
        receiverSessionId: 'sess_runner_test',
        customCommand: {
          command: process.execPath,
          args: ['-e', script],
          stdinPayload: 'hello from test\n',
        },
        onChildPid: async (pid, childProc) => {
          if (pid) {
            onChildPidCalledWithPid = true;
            if (childProc && childProc.pid === pid) {
              receivedChildProcess = true;
              childProc.once('close', () => {
                childSawClose = true;
                resolveChildClose();
              });
            }
            // Suspend registration ACK before child close is handled
            await registrationAckBarrier;
          } else {
            onChildPidCalledWithUndefined = true;
          }
        },
      });

      runnerPromise.then(() => { runnerSettled = true; });

      // Wait on the real child close event; the registration promise is still held.
      await childCloseSignal;
      assert.strictEqual(childSawClose, true, 'Child process must close first');
      assert.strictEqual(runnerSettled, false, 'AutonomousRunner worker must not settle while registration ACK is held');

      // Now release registration ACK!
      releaseRegistrationAck!();

      const result = await runnerPromise;
      assert.strictEqual(runnerSettled, true, 'Worker must settle after registration ACK is released');
      assert.strictEqual(onChildPidCalledWithPid, true, 'onChildPid must be called with PID');
      assert.strictEqual(receivedChildProcess, true, 'onChildPid must receive ChildProcess handle');
      assert.strictEqual(onChildPidCalledWithUndefined, true, 'onChildPid must be called with undefined on exit');
      assert.strictEqual(result.exitCode, 0);
      assert.ok(result.stdout.includes('ACK:hello from test'));
    } finally {
      cleanTempDir(storageDir);
    }
  });

  test('Defect 1: Two control processes concurrency: loser arrives during winner starting/binding window, gets run_busy, and leaves winner intact without false interruption', async () => {
    const storageDir = createTempDir('concurrent-start-binding-window');
    const threadId = 'thread_concurrent_binding';
    const fixture = path.resolve(__dirname, 'fixtures/two-process-runner.js');
    const holdFile = path.join(storageDir, 'release-winner');
    const spawnedFile = path.join(storageDir, 'winner-spawned');
    const spawnedBirthFile = path.join(storageDir, 'winner-spawned-birth');
    const registeredFile = path.join(storageDir, 'winner-registered');
    const registrationGateFile = path.join(storageDir, 'release-registration');
    let store: TaskStore | undefined;
    let winner: child_process.ChildProcess | undefined;
    let loser: child_process.ChildProcess | undefined;
    let winnerExitPromise: Promise<number> | undefined;
    let loserExitPromise: Promise<number> | undefined;
    let winnerOutput = '';
    let loserOutput = '';
    const waitBounded = async <T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> => {
      let timer: NodeJS.Timeout | undefined;
      try {
        return await Promise.race([
          promise,
          new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new Error(message)), timeoutMs); }),
        ]);
      } finally { if (timer) clearTimeout(timer); }
    };
    try {
      store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      store.upsertTask(thread);
      const launch = (workerId: string, extra: string[] = []) => child_process.spawn(process.execPath, [
        fixture,
        `--storage-dir=${storageDir}`,
        `--thread-id=${threadId}`,
        `--worker-id=${workerId}`,
        ...extra,
      ], { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
      const winnerChild = launch('winner', [
        `--hold-file=${holdFile}`,
        `--spawned-file=${spawnedFile}`,
        `--spawned-birth-file=${spawnedBirthFile}`,
        `--registered-file=${registeredFile}`,
        `--registration-gate-file=${registrationGateFile}`,
      ]);
      winner = winnerChild;
      winnerChild.stdout?.on('data', (data) => { winnerOutput += data.toString(); });
      winnerChild.stderr?.on('data', (data) => { winnerOutput += data.toString(); });
      winnerExitPromise = new Promise<number>((resolve, reject) => {
        winnerChild.once('error', reject);
        winnerChild.once('close', (code) => resolve(code ?? -1));
      });
      await waitForFile(spawnedFile, 60_000);
      await waitForFile(`${registrationGateFile}.entered`, 60_000);

      store.ensureUpToDate();
      const beforeLoser = store.getTask(threadId);
      assert.strictEqual(beforeLoser?.execution?.run?.state, 'starting');
      assert.strictEqual(beforeLoser?.execution?.run?.step, 'spawning');
      const winnerRunId = beforeLoser?.execution?.run?.id;
      const winnerRevision = beforeLoser?.execution?.revision;
      const winnerAttempt = beforeLoser?.execution?.attempt;

      const loserChild = launch('loser');
      loser = loserChild;
      loserChild.stdout?.on('data', (data) => { loserOutput += data.toString(); });
      loserChild.stderr?.on('data', (data) => { loserOutput += data.toString(); });
      loserExitPromise = new Promise<number>((resolve, reject) => {
        loserChild.once('error', reject);
        loserChild.once('close', (code) => resolve(code ?? -1));
      });
      const loserExit = await waitBounded(loserExitPromise, 60_000, 'Loser control process did not exit');
      assert.strictEqual(loserExit, 0, loserOutput);
      const loserResult = JSON.parse(loserOutput.split('\n').find((line) => line.startsWith('RESULT:'))!.slice(7));
      assert.strictEqual(loserResult.started, false);
      assert.strictEqual(loserResult.reason, 'run_busy');

      store.ensureUpToDate();
      const afterLoser = store.getTask(threadId);
      assert.strictEqual(afterLoser?.status, 'running');
      assert.strictEqual(afterLoser?.failure, undefined);
      assert.strictEqual(afterLoser?.execution?.run?.id, winnerRunId);
      assert.strictEqual(afterLoser?.execution?.run?.state, 'starting');
      assert.strictEqual(afterLoser?.execution?.revision, winnerRevision);
      assert.strictEqual(afterLoser?.execution?.attempt, winnerAttempt, 'a competing start must not consume an attempt');
      assert.strictEqual(afterLoser?.interruption, undefined);

      // Hold the winner after spawning but before registering the child so the loser observes `starting`.
      fs.writeFileSync(registrationGateFile, 'release', 'utf8');
      // Let the child remain alive until the registration callback and lease ACK
      // have completed. Releasing the worker's child before this point races the
      // Windows FILETIME lookup and turns a healthy start into a false early exit.
      await waitForFile(registeredFile, 60_000);
      store.ensureUpToDate();
      const afterRegistration = store.getTask(threadId);
      assert.strictEqual(afterRegistration?.execution?.run?.state, 'active');
      assert.ok(afterRegistration?.execution?.run?.child?.pid);
      assert.strictEqual(afterRegistration?.failure, undefined);
      assert.strictEqual(afterRegistration?.interruption, undefined);

      fs.writeFileSync(holdFile, 'release', 'utf8');
      const winnerExit = await waitBounded(winnerExitPromise, 60_000, 'Winner control process did not exit');
      assert.strictEqual(winnerExit, 0, winnerOutput);
    } finally {
      try { fs.writeFileSync(registrationGateFile, 'release', 'utf8'); } catch {}
      try { fs.writeFileSync(holdFile, 'release', 'utf8'); } catch {}
      try {
        if (fs.existsSync(spawnedFile) && fs.existsSync(spawnedBirthFile)) {
          const pid = Number(fs.readFileSync(spawnedFile, 'utf8').trim());
          const birthId = fs.readFileSync(spawnedBirthFile, 'utf8').trim();
          if (Number.isSafeInteger(pid) && pid > 0 && birthId && probeProcessIdentity(pid, birthId) !== 'exited') {
            await stopVerifiedChildTree(pid, true, undefined, birthId, 5_000, [], true);
          }
        }
      } catch {}
      if (winnerExitPromise) {
        try { await waitBounded(winnerExitPromise, 10_000, 'Winner did not settle after fixture barriers were released'); }
        catch {
          if (winner && winner.exitCode === null && winner.signalCode === null) try { winner.kill(); } catch {}
          try { await waitBounded(winnerExitPromise, 5_000, 'Winner control process remained after cleanup'); } catch {}
        }
      }
      if (loser && loserExitPromise) {
        try { await waitBounded(loserExitPromise, 5_000, 'Loser control process remained after cleanup'); }
        catch { if (loser.exitCode === null && loser.signalCode === null) try { loser.kill(); } catch {} }
      }
      store?.dispose();
      cleanTempDir(storageDir);
    }
  });

  test('Defect 3: Child subtree plus pending run binding ACK and unconfirmed exit keeps durable ownership', async () => {
    const storageDir = createTempDir('subtree-unconfirmed-lease');
    const threadId = 'thread_subtree_unconfirmed';
    const readyFile = path.join(storageDir, 'grandchild-ready');
    let parentProc: child_process.ChildProcess | undefined;
    let grandchildPid: number | undefined;
    let releaseRegistrationAck!: () => void;
    const registrationAck = new Promise<void>((resolve) => { releaseRegistrationAck = resolve; });
    let resolveBindingWaiting!: () => void;
    const bindingWaiting = new Promise<void>((resolve) => { resolveBindingWaiting = resolve; });
    let resolveChildClosed!: () => void;
    const childClosed = new Promise<void>((resolve) => { resolveChildClosed = resolve; });
    let workerFinished = false;
    let resolveRunnerFinished!: () => void;
    const runnerFinished = new Promise<void>((resolve) => { resolveRunnerFinished = resolve; });
    let resolveUnconfirmedPersisted!: () => void;
    const unconfirmedPersisted = new Promise<void>((resolve) => { resolveUnconfirmedPersisted = resolve; });
    let unbindCalled = false;

    try {
      const store = new TaskStore(storageDir);
      const thread = createV2Thread(threadId, storageDir, 'develop');
      thread.receiverSessionCaptured = true;
      thread.nativeSessionAssignments = {
        developer: { namespace: nativeSessionNamespace('codex'), sessionEpoch: 1 },
      };
      const ownership = new SessionOwnershipStore(storageDir);
      const claimed = ownership.claim({
        agent: 'codex',
        namespace: thread.nativeSessionAssignments!.developer!.namespace,
        sessionId: thread.receiverSessionId!,
        threadId,
        role: 'developer',
        sessionEpoch: 1,
      });
      assert.equal(claimed.ok, true);
      store.upsertTask(thread);

      const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
      const roleLock = new TaskRoleLock(storageDir);
      const sessionLock = new NativeSessionLock(storageDir);
      const originalAttachChild = registry.attachChildPid.bind(registry);
      (registry as any).attachChildPid = async (pid: number | undefined, registration: any) => {
        if (pid === undefined) unbindCalled = true;
        else {
          resolveBindingWaiting();
          await registrationAck;
        }
        return originalAttachChild(pid, registration);
      };
      const originalTransition = store.transitionExecution.bind(store);
      store.transitionExecution = async (...args: any[]) => {
        const result = await (originalTransition as any)(...args);
        if (result?.execution?.run?.state === 'stopping' && result.execution.run.unconfirmed) {
          resolveUnconfirmedPersisted();
        }
        return result;
      };

      const grandchildScript = `setInterval(()=>{},1000)`;
      const parentScript = `
        const cp = require('child_process');
        const fs = require('fs');
        const g = cp.spawn(process.execPath, ['-e', ${JSON.stringify(grandchildScript)}], { windowsHide: true, stdio: 'ignore' });
        fs.writeFileSync(${JSON.stringify(readyFile)}, String(g.pid));
        process.stdout.write(String(g.pid));
        setInterval(()=>{},1000);
      `;
      const mockRunner: any = {
        clearPendingCancellation: () => undefined,
        cancelThread: () => true,
        isTransientError: () => false,
        isRateLimitOrQuotaError: () => false,
        execute: async (opts: any) => {
          const child = child_process.spawn(process.execPath, ['-e', parentScript], {
            windowsHide: true,
            stdio: ['ignore', 'pipe', 'ignore'],
          });
          parentProc = child;
          let grandchildPidText = '';
          let resolveGrandchildPid!: () => void;
          const grandchildReady = new Promise<void>((resolve) => { resolveGrandchildPid = resolve; });
          child.stdout?.on('data', (data) => {
            grandchildPidText += data.toString();
            if (grandchildPidText.trim()) resolveGrandchildPid();
          });
          const close = new Promise<void>((resolve) => child.once('close', () => resolve()));
          await Promise.all([waitForFile(readyFile), grandchildReady]);
          grandchildPid = parseInt(grandchildPidText.trim(), 10);
          const childPid = child.pid;
          if (!childPid) throw new Error('The controlled child process did not receive a PID.');
          const childBirthId = getProcessBirthId(childPid);
          bindSpawnedChildIdentity(child, childBirthId);

          const registration = opts.onChildPid(childPid, child, childBirthId);
          await bindingWaiting;
          // The run child binding and descendant identities are being durably
          // registered. Close only the root while its binding ACK is blocked; the grandchild
          // is deliberately reparented and must still be tracked and stopped.
          child.kill();
          await close;
          resolveChildClosed();
          await registration;
          await opts.onChildPid(undefined);
          workerFinished = true;
          resolveRunnerFinished();
          throw new Error('Simulated runner failure after the child close');
        },
      };

      setConfirmProcessTreeExitProvider(async () => false);
      const controller = new ExecutionController(
        store,
        mockRunner,
        {} as any,
        undefined,
        roleLock,
        sessionLock,
        registry,
        { storageDir, standingScanIntervalMs: 0 },
      );

      try {
        const started = await controller.startExecution(threadId);
        assert.strictEqual(started.started, true);

        await bindingWaiting;
        store.ensureUpToDate();
        const binding = store.getTask(threadId);
        assert.ok(binding?.execution?.run?.child?.pid, 'the root PID is persisted before the run binding ACK');
        assert.strictEqual(binding?.execution?.run?.step, 'binding');

        await childClosed;
        assert.strictEqual(probeProcessIdentity(parentProc!.pid!), 'exited', 'the controlled root closed before ACK release');
        assert.strictEqual(workerFinished, false, 'worker cannot settle while the run binding ACK is held');

        releaseRegistrationAck();
        await runnerFinished;
        await unconfirmedPersisted;

        store.ensureUpToDate();
        const task = store.getTask(threadId);
        assert.strictEqual(task?.execution?.run?.state, 'stopping');
        assert.strictEqual(task?.execution?.run?.unconfirmed, true);
        assert.strictEqual(unbindCalled, false, 'run binding is not released before subtree exit is confirmed');
        assert.ok(grandchildPid, 'the detached descendant PID was captured');
        assert.strictEqual(probeProcessIdentity(grandchildPid!), 'exited', 'the captured descendant was stopped independently of the root');

        const otherThreadClaim = ownership.claim({
          agent: 'codex',
          namespace: thread.nativeSessionAssignments!.developer!.namespace,
          sessionId: thread.receiverSessionId!,
          threadId: `${threadId}_other`,
          role: 'developer',
          sessionEpoch: 1,
        });
        assert.strictEqual(otherThreadClaim.ok, false, 'static ownership prevents another thread from claiming the session');
        assert.strictEqual(otherThreadClaim.code, 'SESSION_OWNERSHIP_CONFLICT');
      } finally {
        releaseRegistrationAck();
        setConfirmProcessTreeExitProvider(undefined);
        controller.dispose();
        if (grandchildPid) try { killPidTree(grandchildPid); } catch {}
        if (parentProc?.pid) try { killPidTree(parentProc.pid); } catch {}
      }
    } finally {
      setConfirmProcessTreeExitProvider(undefined);
      cleanTempDir(storageDir);
    }
  });

});
