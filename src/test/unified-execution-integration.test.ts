import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, execFileSync } from 'node:child_process';

import { MeshFileLock } from '../bridge/mesh-file-lock';
import { ReworkController } from '../bridge/rework-controller';
import {
  getProcessBirthId,
  setProcessBirthIdProvider,
  probeProcessIdentity,
  setKillPidTreeProvider,
  setProcessProbeProvider,
  setConfirmProcessTreeExitProvider,
} from '../bridge/platform';
import { testWorkspace } from './workspace-fixture';
import {
  createThread,
  setupFixture,
} from './unified-execution-fixture';

describe('Unified Execution Integration Tests', () => {
  // Scenario 1: Unified start/stop/outcome classification for develop, review, merge
  test('Scenario 1: develop, review, and merge share execution control record and transition through controller', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s1', 'develop');
      fixture.store.upsertTask(thread);

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
      assert.strictEqual(updated?.execution?.version, 1);
      assert.ok(updated?.execution?.run?.id);
      assert.strictEqual(updated?.execution?.run?.owner.pid, process.pid);

      // Finish develop -> advances to review seamlessly
      finishDevelop!();
      const deadline1 = Date.now() + 60_000;
      let postDevelop = fixture.store.getTask('thread_s1');
      while (postDevelop?.status !== 'reviewing' && Date.now() < deadline1) {
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
      fixture.store.upsertTask(taskA);
      fixture.store.upsertTask(taskB);

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

  // Scenario 15: Seamless phase handoff without mandatory queued state; concurrent independent reviews
  test('Scenario 15: Phase handoff advances directly when resources free; independent reviews are concurrent', async () => {
    const fixture = await setupFixture();
    try {
      const task1 = createThread('thread_s15_1', 'review');
      task1.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      const task2 = createThread('thread_s15_2', 'review');
      task2.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      fixture.store.upsertTask(task1);
      fixture.store.upsertTask(task2);

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

  // Scenario 23: Crash recovery scanner (scanAndDrain sets failed(OWNER_LOST) without auto-retry, stops orphan child)
  test('Scenario 23: Crash recovery sets failed(OWNER_LOST) without auto-retry and stops orphan child', async () => {
    const fixture = await setupFixture();
    try {
      // Spawn an orphan child
      const child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {
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

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Verify task entered failed(OWNER_LOST) without auto-retrying
      const recovered = fixture.store.getTask('thread_s23');
      assert.strictEqual(recovered?.status, 'failed');
      assert.strictEqual(recovered?.failure?.code, 'OWNER_LOST');
      assert.strictEqual(recovered?.execution?.autoRetriesUsed, 0); // MUST NOT auto-retry!
      assert.strictEqual(recovered?.execution?.run, undefined);

      // Verify child process was stopped
      const childProbe = probeProcessIdentity(child.pid, childBirthId);
      assert.strictEqual(childProbe, 'exited');
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 27: Crash recovery candidate ref alone must NOT complete task; requires completed response & Git landed merge
  test('Scenario 27: Crash recovery candidate ref alone must NOT mark completed', async () => {
    const fixture = await setupFixture();
    const gitDir = fs.mkdtempSync(path.join(os.tmpdir(), 'git-recovery-test-'));
    try {
      // Init real git repo
      execFileSync('git', ['init', '-b', 'master'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.name', 'test'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['config', 'user.email', 'test@test.com'], { cwd: gitDir, stdio: 'ignore' });
      fs.writeFileSync(path.join(gitDir, 'file.txt'), 'base');
      execFileSync('git', ['add', '.'], { cwd: gitDir, stdio: 'ignore' });
      execFileSync('git', ['commit', '-m', 'base commit'], { cwd: gitDir, stdio: 'ignore' });
      const baseSha = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: gitDir, encoding: 'utf8' }).trim();

      // Create a candidate ref in git
      execFileSync('git', ['update-ref', 'refs/mesh/candidates/thread_s27', baseSha], { cwd: gitDir, stdio: 'ignore' });

      // Case A: develop phase, round is still 'running', assessment undefined, mergePending=true, owner is dead
      const thread = createThread('thread_s27', 'develop');
      thread.status = 'running';
      thread.workspace = testWorkspace(gitDir);
      thread.worktree = {
        path: gitDir,
        mainPath: gitDir,
        baseCommit: baseSha,
        headCommit: baseSha,
        mergePending: true,
        integration: { phase: 'ready', updatedAt: new Date().toISOString() },
      };
      thread.rounds = [
        {
          round: 1,
          type: 'initial',
          prompt: 'develop prompt',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
          // Note: NO assessment!
        },
      ];
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s27',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-develop',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Task MUST NOT become completed! It must be failed(OWNER_LOST)!
      const afterDrain = fixture.store.getTask('thread_s27');
      assert.strictEqual(afterDrain?.status, 'failed');
      assert.strictEqual(afterDrain?.failure?.code, 'OWNER_LOST');
      assert.strictEqual(afterDrain?.worktree?.integration?.phase, 'ready'); // Preserved workspace

      // Case B: develop phase, BUT round has completed with valid assessment
      const threadB = createThread('thread_s27_b', 'develop');
      threadB.status = 'running';
      threadB.workspace = testWorkspace(gitDir);
      threadB.worktree = {
        path: gitDir,
        mainPath: gitDir,
        baseCommit: baseSha,
        headCommit: baseSha,
      };
      threadB.rounds = [
        {
          round: 1,
          type: 'initial',
          prompt: 'develop prompt',
          status: 'completed',
          durationMs: 100,
          timestamp: new Date().toISOString(),
          assessment: {
            success: true,
            exitCode: 0,
            errorCode: 'OK',
            warnings: [],
            stream: {
              chunkCount: 1,
              stdoutBytes: 10,
              stderrBytes: 0,
              firstOutputAtMs: 1,
              lastOutputAtMs: 2,
              maxGapMs: 0,
              rawMaxGapMs: 0,
              excludedSuspensionMs: 0,
              suspensionCount: 0,
              gapThresholdMs: 60000,
              hasActualOutput: true,
              continuous: true,
              gapExceeded: false,
            },
          },
        },
      ];
      execFileSync('git', ['update-ref', 'refs/mesh/candidates/thread_s27_b', baseSha], { cwd: gitDir, stdio: 'ignore' });
      threadB.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s27_b',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'crashed-run-develop-b',
          owner: { pid: 99999999, birthId: 'dead-birth' },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadB);

      await fixture.controller.scanAndDrain();

      // Successful develop with response evidence hands off to REVIEW, NEVER completed!
      const afterDrainB = fixture.store.getTask('thread_s27_b');
      assert.strictEqual(afterDrainB?.status, 'reviewing');
      assert.strictEqual(afterDrainB?.execution?.phase, 'review');

      // Wait for any background execution to settle before cleanup
      await fixture.controller.waitForActiveExecutions(3000);
    } finally {
      await fixture.cleanup();
      try { fs.rmSync(gitDir, { recursive: true, force: true }); } catch {}
    }
  });

  // Scenario 28: Stop unconfirmed retains run protection, prevents concurrent retry/resume, and pauseExecution with unknown identity does not clear run
  test('Scenario 28: Stop unconfirmed retains run protection and blocks concurrent retry/resume', async () => {
    const fixture = await setupFixture();
    const killedPids: number[] = [];
    setKillPidTreeProvider((pid) => { killedPids.push(pid); });
    setProcessProbeProvider((pid, birthId) => {
      // Injected: alive probe for mock child PIDs (99999998, 99999997, 99999996), exited for dead mock owner PID 99999999
      if (pid === 99999998 || pid === 99999997 || pid === 99999996) {
        if (birthId && birthId !== 'birth-99999996') {
          return 'exited';
        }
        return 'alive';
      }
      if (pid === 99999999) {
        return 'exited';
      }
      // For real OS processes (e.g. process.pid): delegate to genuine OS probe logic
      try {
        process.kill(pid, 0);
      } catch {
        return 'exited';
      }
      if (!birthId) return 'alive';
      const actual = getProcessBirthId(pid);
      if (!actual) return 'unknown';
      return actual === birthId ? 'alive' : 'exited';
    });
    setConfirmProcessTreeExitProvider(async () => true);

    try {
      // 1. Unconfirmed stop in scanAndDrain (child probe is alive, but missing birthId: MUST NOT KILL!)
      const thread = createThread('thread_s28', 'develop');
      thread.status = 'running';
      thread.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28',
          owner: { pid: 99999999, birthId: 'dead-owner' },
          child: { pid: 99999998, birthId: undefined as any }, // Missing birthId -> unconfirmed identity!
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(thread);

      // Run scanAndDrain
      await fixture.controller.scanAndDrain();

      // Assert killPidTree was NEVER called because birthId is missing!
      assert.strictEqual(killedPids.length, 0, 'Must NOT kill child process without verified birthId');

      const afterDrain = fixture.store.getTask('thread_s28');
      // Must NOT be cleared or failed without protection!
      assert.strictEqual(afterDrain?.execution?.run?.state, 'stopping');
      assert.strictEqual(afterDrain?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // Attempt concurrent retryOperation: must throw and refuse to run!
      await assert.rejects(
        async () => {
          await fixture.controller.retryOperation('thread_s28', 'retry');
        },
        /still stopping or unconfirmed/,
      );

      // Attempt concurrent resumeOperation: must throw and refuse to run!
      await assert.rejects(
        async () => {
          await fixture.controller.resumeOperation('thread_s28');
        },
        /still stopping or unconfirmed/,
      );

      // 2. pauseExecution with alive probe but missing child birthId: must NOT kill and must NOT clear run
      killedPids.length = 0;
      const threadPause = createThread('thread_s28_pause', 'develop');
      threadPause.status = 'running';
      threadPause.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_p',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-p',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          child: { pid: 99999997, birthId: undefined as any }, // Missing birthId
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadPause);

      const paused = await fixture.controller.pauseExecution('thread_s28_pause');
      assert.strictEqual(killedPids.length, 0, 'pauseExecution must NOT kill child process without verified birthId');
      assert.strictEqual(paused.execution?.run?.state, 'stopping');
      assert.ok(paused.execution?.run); // run is NOT undefined!
      assert.strictEqual(paused.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // 3. Production collection regression via updateRunChild with undefined birthId:
      const mockChildPid = 99999996;
      killedPids.length = 0;
      const threadCollect = createThread('thread_s28_collect', 'develop');
      threadCollect.status = 'running';
      threadCollect.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_c',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-c',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadCollect);

      // Call updateRunChild simulating onChildPid with undefined birthId
      await fixture.controller.updateRunChild('thread_s28_collect', 'run-s28-c', mockChildPid, undefined);

      // Verify task on disk has birthId=undefined (NOT fake String(pid)!)
      fixture.store.loadFromDisk();
      const taskAfterCollect = fixture.store.getTask('thread_s28_collect');
      assert.strictEqual(taskAfterCollect?.execution?.run?.child?.pid, mockChildPid);
      assert.strictEqual(taskAfterCollect?.execution?.run?.child?.birthId, undefined);

      // Identity reading recovers in system
      setProcessBirthIdProvider((pid) => (pid === mockChildPid ? 'birth-99999996' : undefined));

      // Calling pauseExecution must NOT consider child exited, must NOT kill, and must NOT clear run
      const pausedCollect = await fixture.controller.pauseExecution('thread_s28_collect');
      assert.strictEqual(killedPids.length, 0, 'Must NOT kill child process without verified birthId');
      assert.ok(pausedCollect.execution?.run, 'Run must NOT be cleared when stop is unconfirmed');
      assert.strictEqual(pausedCollect.execution?.run?.state, 'stopping');
      assert.strictEqual(pausedCollect.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');
      assert.notStrictEqual(pausedCollect.status, 'paused', 'Status must NOT transition to clean paused');

      // Attempting resume/retry must be blocked
      await assert.rejects(
        async () => { await fixture.controller.resumeOperation('thread_s28_collect'); },
        /still stopping or unconfirmed/,
      );
      await assert.rejects(
        async () => { await fixture.controller.retryOperation('thread_s28_collect'); },
        /still stopping or unconfirmed/,
      );

      // 4. Scanner regression with updateRunChild having undefined birthId and child alive:
      killedPids.length = 0;
      const threadScanCollect = createThread('thread_s28_scan_collect', 'develop');
      threadScanCollect.status = 'running';
      threadScanCollect.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_sc',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-sc',
          owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadScanCollect);

      await fixture.controller.updateRunChild('thread_s28_scan_collect', 'run-s28-sc', mockChildPid, undefined);
      fixture.store.loadFromDisk();
      const taskScanBefore = fixture.store.getTask('thread_s28_scan_collect');
      assert.ok(taskScanBefore?.execution?.run);
      assert.strictEqual(taskScanBefore?.execution?.run?.child?.pid, mockChildPid);
      assert.strictEqual(taskScanBefore?.execution?.run?.child?.birthId, undefined);

      // Now simulate owner crash
      await fixture.store.transitionExecution(
        'thread_s28_scan_collect',
        taskScanBefore.execution!.revision,
        (t) => {
          if (t.execution?.run) {
            t.execution.run.owner = { pid: 99999999, birthId: 'dead-owner' };
          }
        },
      );

      await fixture.controller.scanAndDrain();
      assert.strictEqual(killedPids.length, 0, 'scanAndDrain must NOT kill child process without verified birthId');
      fixture.store.loadFromDisk();
      const taskScanAfter = fixture.store.getTask('thread_s28_scan_collect');
      assert.ok(taskScanAfter?.execution?.run, 'scanAndDrain must NOT clear run without verified birthId');
      assert.strictEqual(taskScanAfter?.execution?.run?.state, 'stopping');
      assert.strictEqual(taskScanAfter?.interruption?.reason, 'PROCESS_STOP_UNCONFIRMED');

      // 5. Owner collection failure: owner.birthId is undefined, owner is alive
      const threadOwnerFail = createThread('thread_s28_owner_fail', 'develop');
      threadOwnerFail.status = 'running';
      threadOwnerFail.execution = {
        version: 1,
        revision: 1,
        operationId: 'op_s28_of',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 1000, maxAutoRetries: 2 },
        run: {
          id: 'run-s28-of',
          owner: { pid: process.pid, birthId: undefined as any }, // Owner collection failure
          state: 'active',
          step: 'executing',
          startedAt: new Date().toISOString(),
        },
      };
      fixture.store.upsertTask(threadOwnerFail);

      // inspectRunProcesses must return ownerStatus='unknown' (NEVER 'exited')
      const inspectOwner = fixture.controller.inspectRunProcesses(threadOwnerFail.execution?.run);
      assert.strictEqual(inspectOwner.ownerStatus, 'unknown', 'Missing owner birthId must be unknown, not exited');

      // scanAndDrain must NOT mark owner lost or clear run
      await fixture.controller.scanAndDrain();
      fixture.store.loadFromDisk();
      const afterOwnerScan = fixture.store.getTask('thread_s28_owner_fail');
      assert.strictEqual(afterOwnerScan?.status, 'running', 'Task must NOT enter failed(OWNER_LOST)');
      assert.ok(afterOwnerScan?.execution?.run, 'Run must NOT be cleared when owner identity is unknown');
    } finally {
      setKillPidTreeProvider(undefined);
      setProcessProbeProvider(undefined);
      setConfirmProcessTreeExitProvider(undefined);
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
      fixture.store.upsertTask(thread);

      let releaseCliBarrier: (res: { success: boolean; exitCode: number }) => void;
      const cliBarrier = new Promise<{ success: boolean; exitCode: number }>((resolve) => {
        releaseCliBarrier = resolve;
      });

      // Mock launchCaller representing a live Reviewer CLI
      fixture.callbackManager.launchCaller = (async (_t: any, _r: any, onChildPid?: (pid: number) => void) => {
        if (onChildPid) onChildPid(process.pid);
        return await cliBarrier;
      }) as any;

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
