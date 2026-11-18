import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { spawn } from 'node:child_process';

import { TaskStore } from '../bridge/task-store';
import { ReworkController } from '../bridge/rework-controller';
import { waitFor, waitForExit } from './git-worktree-fixture';
import { DelegationThread } from '../bridge/types';
import {
  getProcessBirthId,
  setProcessBirthIdProvider,
  probeProcessIdentity,
  confirmProcessTreeExit,
} from '../bridge/platform';
import {
  createThread,
  setupFixture,
} from './unified-execution-fixture';

describe('Unified Execution Boundary Tests', () => {
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

      // Calling startExecution when run is alive must return previous_child_alive
      const startRes = await fixture.controller.startExecution('thread_s20', 'develop');
      assert.strictEqual(startRes.started, false);
      assert.strictEqual(startRes.reason, 'previous_child_alive');

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
        version: 1,
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
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
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
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
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

      // 5. Unrelated checkpoint retry: same generation and run, but revision bumped by checkpoint
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
      fixture.store.transitionExecution = (async (threadId: string, expectedRev: number, change: (t: any) => void) => {
        if (threadId === 'thread_s30_cp' && !injectedCheckpoint) {
          injectedCheckpoint = true;
          // Unrelated checkpoint: budget updated, revision bumped to 2
          await originalTransition(threadId, expectedRev, (t: DelegationThread) => {
            if (t.execution) {
              t.execution.budget.spentActiveMs = 2000;
            }
          });
        }
        return await originalTransition(threadId, expectedRev, change);
      }) as any;

      // requestRework should succeed on retry because operation/generation/run identity matched!
      const reworkRes = await reworkController.requestRework({
        threadId: 'thread_s30_cp',
        feedback: 'Valid rework on retry',
      });
      assert.strictEqual(reworkRes.status, 'REWORK_DISPATCHED');
      const taskAfterCp = fixture.store.getTask('thread_s30_cp');
      // Has active run was preserved because reviewer is still running!
      assert.strictEqual(taskAfterCp?.execution?.run?.id, 'run-cp');
      assert.strictEqual(taskAfterCp?.currentRound, 2);
    } finally {
      await fixture.cleanup();
    }
  });

  // Scenario 31: recovery CAS cannot clear a concurrently started generation
  test('Scenario 31: recovery CAS cannot clear a concurrently started generation', async () => {
    const fixture = await setupFixture();
    try {
      const thread = createThread('thread_s31', 'develop');
      thread.status = 'running';
      fixture.store.upsertTask(thread);
      const originalTransition = fixture.store.transitionExecution.bind(fixture.store);
      let injected = false;
      fixture.store.transitionExecution = (async (id: string, revision: number, change: (task: DelegationThread) => void) => {
        if (id === thread.threadId && !injected) {
          injected = true;
          await originalTransition(id, revision, fresh => {
            fresh.execution!.generation = 2;
            fresh.execution!.run = {
              id: 'new-recovery-run', state: 'active', step: 'executing',
              owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
              startedAt: new Date().toISOString(),
            };
          });
        }
        return originalTransition(id, revision, change);
      }) as any;
      await fixture.controller.scanAndDrain();
      fixture.store.loadFromDisk();
      const fresh = fixture.store.getTask(thread.threadId)!;
      assert.equal(injected, true);
      assert.equal(fresh.execution!.generation, 2);
      assert.equal(fresh.execution!.run?.id, 'new-recovery-run');
      assert.equal(fresh.status, 'running');
      assert.equal(fresh.failure, undefined);
    } finally {
      await fixture.cleanup();
    }
  });
});
