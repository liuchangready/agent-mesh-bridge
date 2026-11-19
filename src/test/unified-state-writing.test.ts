import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';
import { TaskStore } from '../bridge/task-store';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import {
  DelegationThread,
  ExecutionControl,
  TokenUsageMetrics,
} from '../bridge/types';
import {
  validateV2ExecutionEligibility,
} from '../bridge/execution-gate';
import { createTarGzBuffer, archiveCompletedTaskLogs } from '../bridge/task-summary';

describe('Unified State Writing (Task 01) Tests', () => {
  let tempDir: string;
  let taskStore: TaskStore;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-v2-write-test-'));
    taskStore = new TaskStore(tempDir);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  describe('Acceptance A: Persistence & Reload Compatibility', () => {
    test('reads legacy task without execution: does not invent execution, version, or budget', () => {
      const now = new Date().toISOString();
      const legacyThread: DelegationThread = {
        threadId: 'thread_legacy_read',
        title: 'Legacy Task Without Execution',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'legacy-receiver-session-123',
        callerSessionId: 'legacy-caller-session-456',
        workspace: testWorkspace(tempDir),
        summary: {
          lastPromptSnippet: 'Do legacy work',
          lastOutputSnippet: 'Working',
          totalDurationMs: 1500,
        },
        rounds: [
          {
            round: 1,
            type: 'rework',
            prompt: 'Do legacy work',
            output: 'Legacy output',
            durationMs: 1500,
            status: 'completed',
            timestamp: now,
          },
        ],
      };

      taskStore.upsertTask(legacyThread);
      assert.strictEqual(taskStore.count(), 1);

      // Reload with fresh TaskStore reading raw disk JSON
      const freshStore = new TaskStore(tempDir);
      try {
        const loaded = freshStore.getTask('thread_legacy_read');
        assert.ok(loaded);
        assert.strictEqual(loaded.execution, undefined, 'Legacy task must NOT have synthetic execution');
        assert.strictEqual(loaded.receiverSessionId, 'legacy-receiver-session-123');
        assert.strictEqual(loaded.callerSessionId, 'legacy-caller-session-456');
        assert.strictEqual(loaded.currentRound, 1);
        assert.strictEqual(loaded.rounds.length, 1);
      } finally {
        freshStore.dispose();
      }
    });

    test('reads v1 task: retains version=1, budget, sessions, rounds without upgrading to v2', () => {
      const now = new Date().toISOString();
      const v1Execution: ExecutionControl = {
        version: 1,
        revision: 3,
        operationId: 'op_v1_develop_1',
        phase: 'develop',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: {
          limitMs: 7200000,
          spentActiveMs: 45000,
          maxAutoRetries: 2,
        },
      };

      const v1Thread: DelegationThread = {
        threadId: 'thread_v1_read',
        title: 'V1 Task Thread',
        initiatorAgent: 'codex',
        targetAgent: 'claude_code',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'v1-session-id',
        workspace: testWorkspace(tempDir),
        execution: v1Execution,
        summary: {
          lastPromptSnippet: 'V1 prompt',
          lastOutputSnippet: 'V1 output',
          totalDurationMs: 45000,
        },
        rounds: [
          {
            round: 1,
            type: 'rework',
            prompt: 'V1 prompt',
            output: 'V1 output',
            durationMs: 45000,
            status: 'completed',
            timestamp: now,
          },
        ],
      };

      taskStore.upsertTask(v1Thread);

      const freshStore = new TaskStore(tempDir);
      try {
        const loaded = freshStore.getTask('thread_v1_read');
        assert.ok(loaded);
        assert.ok(loaded.execution);
        assert.strictEqual(loaded.execution.version, 1, 'Version 1 must remain version 1');
        assert.strictEqual(loaded.execution.revision, 3);
        assert.strictEqual(loaded.execution.operationId, 'op_v1_develop_1');
        assert.strictEqual(loaded.execution.budget.spentActiveMs, 45000);
      } finally {
        freshStore.dispose();
      }
    });

    test('reads v2 task: retains version=2, execution.run.role, next, queue.enteredAt, cleanup marker', () => {
      const now = new Date().toISOString();
      const v2Execution: ExecutionControl = {
        version: 2,
        revision: 4,
        operationId: 'op_v2_review_1',
        phase: 'review',
        round: 2,
        generation: 2,
        attempt: 1,
        autoRetriesUsed: 0,
        queue: {
          reason: 'session',
          enteredAt: now,
        },
        run: {
          id: 'run_v2_review_uuid',
          owner: { pid: process.pid, birthId: 'birth-v2-1' },
          child: { pid: process.pid + 10, birthId: 'birth-v2-child' },
          step: 'active_review',
          state: 'active',
          startedAt: now,
          role: 'reviewer',
        },
        next: {
          phase: 'merge',
          round: 2,
          purpose: 'merge_resolution',
          sourceRunId: 'run_v2_review_uuid',
        },
        budget: {
          limitMs: 7200000,
          spentActiveMs: 90000,
          maxAutoRetries: 2,
        },
      };

      const v2Thread: DelegationThread = {
        threadId: 'thread_v2_read',
        title: 'V2 Task Thread With Next and Cleanup',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'reviewing',
        currentRound: 2,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'v2-receiver-session',
        callerSessionId: 'v2-reviewer-session',
        workspace: testWorkspace(tempDir),
        execution: v2Execution,
        cleanup: {
          token: 'cleanup_token_test',
          action: 'archive',
          owner: { pid: process.pid },
          startedAt: now,
          stage: 'in_progress',
        },
        summary: {
          lastPromptSnippet: 'V2 review prompt',
          lastOutputSnippet: 'Review output',
          totalDurationMs: 90000,
        },
        rounds: [
          {
            round: 1,
            type: 'rework',
            prompt: 'V2 dev prompt',
            output: 'Dev output',
            durationMs: 40000,
            status: 'completed',
            timestamp: now,
          },
          {
            round: 2,
            type: 'rework',
            prompt: 'V2 review prompt',
            output: 'Review output',
            durationMs: 50000,
            status: 'completed',
            timestamp: now,
          },
        ],
      };

      taskStore.upsertTask(v2Thread);

      const freshStore = new TaskStore(tempDir);
      try {
        const loaded = freshStore.getTask('thread_v2_read');
        assert.ok(loaded);
        assert.ok(loaded.execution);
        assert.strictEqual(loaded.execution.version, 2, 'Execution version must be 2');
        assert.strictEqual(loaded.execution.run?.role, 'reviewer', 'Execution run must retain role=reviewer');
        assert.strictEqual(loaded.execution.queue?.enteredAt, now);
        assert.deepStrictEqual(loaded.execution.next, {
          phase: 'merge',
          round: 2,
          purpose: 'merge_resolution',
          sourceRunId: 'run_v2_review_uuid',
        });
        assert.ok(loaded.cleanup);
        assert.strictEqual(loaded.cleanup.token, 'cleanup_token_test');
        assert.strictEqual(loaded.cleanup.action, 'archive');
      } finally {
        freshStore.dispose();
      }
    });
  });

  describe('Acceptance B: Same-Revision Control State Protection & Evidence Durability', () => {
    test('ordinary upsertTask with identical revision cannot overwrite CAS-committed run, status, decision, cleanup, failure', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_cas_protect',
        title: 'CAS Protection Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'queued',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_init',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Develop', status: 'running', durationMs: 0, timestamp: now }],
      };

      taskStore.upsertTask(thread);

      // Transition execution via CAS to revision 2: set active run, status running, decision
      await taskStore.transitionExecution('thread_cas_protect', 1, (t) => {
        t.status = 'running';
        t.execution!.run = {
          id: 'run_cas_active',
          owner: { pid: process.pid },
          step: 'active_dev',
          state: 'active',
          startedAt: new Date().toISOString(),
          role: 'developer',
        };
        t.decision = {
          id: 'decision_cas_1',
          operationId: 'op_init',
          generation: 1,
          phase: 'develop',
          round: 1,
          question: 'Which strategy?',
          resumePoint: 'continue_develop',
          createdAt: new Date().toISOString(),
        };
        t.cleanup = {
          token: 'token_cas_clean',
          action: 'archive',
          owner: { pid: process.pid },
          startedAt: new Date().toISOString(),
          stage: 'in_progress',
        };
      });

      // Verify CAS state is durable on disk
      const afterCas = taskStore.getTask('thread_cas_protect');
      assert.strictEqual(afterCas?.execution?.revision, 2);
      assert.strictEqual(afterCas?.status, 'running');
      assert.strictEqual(afterCas?.execution?.run?.id, 'run_cas_active');
      assert.strictEqual(afterCas?.decision?.id, 'decision_cas_1');
      assert.strictEqual(afterCas?.cleanup?.token, 'token_cas_clean');

      // Now create a stale snapshot with IDENTICAL revision 2, but where run is cleared, status is queued, decision cleared
      const staleSnapshot = structuredClone(afterCas!);
      staleSnapshot.execution!.revision = 2; // Same revision!
      staleSnapshot.execution!.run = undefined; // Stale writer didn't have the run!
      staleSnapshot.status = 'queued'; // Stale writer thought it was queued!
      delete staleSnapshot.decision; // Stale writer didn't know about decision!
      delete staleSnapshot.cleanup; // Stale writer didn't know about cleanup!

      // Attempt ordinary upsertTask with stale snapshot at SAME revision
      taskStore.upsertTask(staleSnapshot);

      // Verify that CAS committed fields were NOT overwritten!
      const afterStaleUpsert = taskStore.getTask('thread_cas_protect');
      assert.strictEqual(afterStaleUpsert?.execution?.revision, 2);
      assert.strictEqual(afterStaleUpsert?.status, 'running', 'Status must stay running, not overwritten to queued');
      assert.strictEqual(
        afterStaleUpsert?.execution?.run?.id,
        'run_cas_active',
        'Run must not be cleared by stale upsertTask',
      );
      assert.strictEqual(
        afterStaleUpsert?.decision?.id,
        'decision_cas_1',
        'Decision must not be erased by stale upsertTask',
      );
      assert.strictEqual(
        afterStaleUpsert?.cleanup?.token,
        'token_cas_clean',
        'Cleanup marker must not be erased by stale upsertTask',
      );

      // Reload from disk with fresh TaskStore to ensure persistence wasn't corrupted
      const reloadedStore = new TaskStore(tempDir);
      try {
        const diskCopy = reloadedStore.getTask('thread_cas_protect');
        assert.strictEqual(diskCopy?.status, 'running');
        assert.strictEqual(diskCopy?.execution?.run?.id, 'run_cas_active');
        assert.strictEqual(diskCopy?.decision?.id, 'decision_cas_1');
        assert.strictEqual(diskCopy?.cleanup?.token, 'token_cas_clean');
      } finally {
        reloadedStore.dispose();
      }
    });

    test('ordinary upsertTask does not lose unrelated round evidence, tokenUsage, or compressed round', async () => {
      const now = new Date().toISOString();
      const tokenUsage: TokenUsageMetrics = {
        uncachedInputTokens: 100,
        cacheReadTokens: 50,
        cacheWriteTokens: 25,
        promptTokens: 175,
        outputTokens: 80,
        totalTokens: 255,
        source: 'native_reported',
        agent: 'codex',
        model: 'gpt-5-codex',
        available: true,
        collectedAt: now,
      };

      const thread: DelegationThread = {
        threadId: 'thread_evidence_durability',
        title: 'Evidence Durability Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_dev',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [
          {
            round: 1,
            type: 'rework',
            prompt: 'Develop code',
            output: 'Compact Summary Card',
            isCompacted: true,
            compactSummary: {
              headline: 'Fix crash',
              touchedFiles: ['main.ts'],
              resolvedIssues: ['null pointer'],
              longTermConstraints: ['keep null checks'],
              metrics: {
                originalChars: 1000,
                compactedChars: 200,
                compressionRatio: 0.2,
                compactedAt: now,
                source: 'agent_protocol_envelope',
              },
            },
            status: 'completed',
            durationMs: 5000,
            timestamp: now,
            tokenUsage,
            touchedFiles: ['main.ts'],
          },
        ],
      };

      taskStore.upsertTask(thread);

      // Now incoming update with raw output and new files, but without compaction flag
      const incoming = structuredClone(thread);
      incoming.rounds[0].output = 'Raw large output string that should not un-compact';
      incoming.rounds[0].isCompacted = false;
      incoming.rounds[0].touchedFiles = ['main.ts', 'helper.ts'];

      taskStore.upsertTask(incoming);

      const updated = taskStore.getTask('thread_evidence_durability');
      assert.strictEqual(updated?.rounds[0].isCompacted, true, 'Compacted state must be preserved');
      assert.strictEqual(updated?.rounds[0].output, 'Compact Summary Card');
      assert.deepStrictEqual(updated?.rounds[0].touchedFiles, ['main.ts'], 'Original touched files preserved against uncoordinated upsert');
      assert.strictEqual(updated?.rounds[0].tokenUsage?.totalTokens, 255, 'Token usage preserved');
    });

    test('updateRound with newThreadStatus cannot mutate thread.status on v2 task', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_v2_status_protect',
        title: 'Status Protect Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_dev',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
          run: {
            id: 'run_dev_1',
            owner: { pid: process.pid },
            role: 'developer',
            step: 'executing',
            state: 'active',
            startedAt: now,
          },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Develop', status: 'running', durationMs: 0, timestamp: now }],
      };

      taskStore.upsertTask(thread);

      // Calling updateRound with newThreadStatus: 'failed' and valid evidence identity
      await taskStore.updateRound(
        'thread_v2_status_protect',
        1,
        { status: 'failed', error: 'execution error' },
        'failed',
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op_dev', generation: 1, runId: 'run_dev_1' },
      );

      const afterUpdate = taskStore.getTask('thread_v2_status_protect');
      assert.strictEqual(afterUpdate?.status, 'running', 'Thread status on v2 must NOT be changed by updateRound');
      assert.strictEqual(afterUpdate?.rounds[0].status, 'failed', 'Round-level status may be updated');
      assert.strictEqual(afterUpdate?.rounds[0].error, 'execution error');
    });

    test('ordinary upsertTask cannot advance currentRound on v2 task', () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_v2_round_protect',
        title: 'Round Protect Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_dev',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Develop', status: 'running', durationMs: 0, timestamp: now }],
      };
      taskStore.upsertTask(thread);

      // Incoming stale snapshot tries to claim currentRound is 5
      const mutated = structuredClone(thread);
      mutated.currentRound = 5;

      taskStore.upsertTask(mutated);

      const after = taskStore.getTask('thread_v2_round_protect');
      assert.strictEqual(after?.currentRound, 1, 'v2 currentRound must be preserved from prior execution state');
    });
  });

  describe('Acceptance C: Identity Validation for Evidence Updates', () => {
    let v2Thread: DelegationThread;

    beforeEach(() => {
      const now = new Date().toISOString();
      v2Thread = {
        threadId: 'thread_identity_val',
        title: 'Identity Validation Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_generation_2',
          phase: 'develop',
          round: 1,
          generation: 2,
          attempt: 1,
          autoRetriesUsed: 0,
          run: {
            id: 'run_active_gen_2',
            owner: { pid: process.pid },
            step: 'running',
            state: 'active',
            startedAt: now,
            role: 'developer',
          },
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [
          {
            round: 1,
            type: 'rework',
            prompt: 'Initial prompt',
            output: 'Old initial output',
            status: 'running',
            durationMs: 1000,
            timestamp: now,
          },
        ],
      };
      taskStore.upsertTask(v2Thread);
    });

    test('reportRoundEvidence rejects stale operationId for v2 task', async () => {
      const result = await taskStore.reportRoundEvidence(
        'thread_identity_val',
        1,
        { output: 'Hacked output from stale op' },
        { operationId: 'op_generation_OLD', generation: 2, runId: 'run_active_gen_2' },
      );

      assert.strictEqual(result, undefined, 'Stale operationId must be rejected');
      const thread = taskStore.getTask('thread_identity_val');
      assert.strictEqual(thread?.rounds[0].output, 'Old initial output', 'Output must not be modified');
    });

    test('reportRoundEvidence rejects stale generation for v2 task', async () => {
      const result = await taskStore.reportRoundEvidence(
        'thread_identity_val',
        1,
        { output: 'Hacked output from gen 1' },
        { operationId: 'op_generation_2', generation: 1, runId: 'run_active_gen_2' },
      );

      assert.strictEqual(result, undefined, 'Stale generation must be rejected');
      const thread = taskStore.getTask('thread_identity_val');
      assert.strictEqual(thread?.rounds[0].output, 'Old initial output', 'Output must not be modified');
    });

    test('reportRoundEvidence rejects stale runId for v2 task', async () => {
      const result = await taskStore.reportRoundEvidence(
        'thread_identity_val',
        1,
        { output: 'Hacked output from old run' },
        { operationId: 'op_generation_2', generation: 2, runId: 'run_old_id' },
      );

      assert.strictEqual(result, undefined, 'Stale runId must be rejected');
      const thread = taskStore.getTask('thread_identity_val');
      assert.strictEqual(thread?.rounds[0].output, 'Old initial output', 'Output must not be modified');
    });

    test('reportRoundEvidence accepts matching credentials and updates only allowed round fields', async () => {
      const result = await taskStore.reportRoundEvidence(
        'thread_identity_val',
        1,
        {
          output: 'Legitimate new output',
          durationMs: 4500,
          touchedFiles: ['src/app.ts'],
          status: 'completed',
        },
        { operationId: 'op_generation_2', generation: 2, runId: 'run_active_gen_2' },
      );

      assert.ok(result, 'Matching identity credentials must be accepted');
      const thread = taskStore.getTask('thread_identity_val');
      assert.strictEqual(thread?.rounds[0].output, 'Legitimate new output');
      assert.strictEqual(thread?.rounds[0].durationMs, 4500);
      assert.deepStrictEqual(thread?.rounds[0].touchedFiles, ['src/app.ts']);
      assert.strictEqual(thread?.rounds[0].status, 'completed');

      // Control fields must remain pristine
      assert.strictEqual(thread?.status, 'running', 'Thread status must not be altered');
      assert.strictEqual(thread?.execution?.revision, 1, 'Revision must not be bumped by evidence update');
      assert.strictEqual(thread?.execution?.run?.id, 'run_active_gen_2');
    });
  });

  describe('Acceptance D: Cleanup Reservation Protocol, Race Prevention & Tombstone Durability', () => {
    test('markTaskCleanup reserves cleanup token and increments revision under lock', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_clean_reserve',
        title: 'Cleanup Reservation Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_done',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 5000, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 5000 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Work', output: 'Done', status: 'completed', durationMs: 5000, timestamp: now }],
      };
      taskStore.upsertTask(thread);

      const reservation = await taskStore.markTaskCleanup('thread_clean_reserve', 'archive', { pid: process.pid });
      assert.ok(reservation.token);
      assert.strictEqual(reservation.thread.cleanup?.token, reservation.token);
      assert.strictEqual(reservation.thread.cleanup?.action, 'archive');
      assert.strictEqual(reservation.thread.execution?.revision, 2, 'Revision must be incremented on cleanup reservation');

      // Verify persistence on disk
      const diskCopy = new TaskStore(tempDir);
      try {
        const loaded = diskCopy.getTask('thread_clean_reserve');
        assert.strictEqual(loaded?.cleanup?.token, reservation.token);
        assert.strictEqual(loaded?.execution?.revision, 2);
      } finally {
        diskCopy.dispose();
      }
    });

    test('concurrent cleaner with mismatched token is rejected on commitTaskCleanup', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_clean_mismatch',
        title: 'Cleanup Token Mismatch Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_done',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 5000, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 5000 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Work', output: 'Done', status: 'completed', durationMs: 5000, timestamp: now }],
      };
      taskStore.upsertTask(thread);

      const { token: validToken } = await taskStore.markTaskCleanup('thread_clean_mismatch', 'archive', { pid: process.pid });

      // Stale cleaner attempts to commit with bad token
      await assert.rejects(
        () => taskStore.commitTaskCleanup('thread_clean_mismatch', 'token_stale_or_alien', 'archive'),
        /cleanup token mismatch/i,
      );

      // Verify cleanup reservation is still intact on disk
      const loaded = taskStore.getTask('thread_clean_mismatch');
      assert.strictEqual(loaded?.cleanup?.token, validToken, 'Original token must be retained');
    });

    test('in-progress cleanup blocks transitionExecution', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_clean_blocks_transition',
        title: 'Cleanup Blocks Transition Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_done',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 5000, maxAutoRetries: 2 },
        },
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 5000 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Work', output: 'Done', status: 'completed', durationMs: 5000, timestamp: now }],
      };
      taskStore.upsertTask(thread);

      await taskStore.markTaskCleanup('thread_clean_blocks_transition', 'archive', { pid: process.pid });

      // Attempt to transition execution while cleanup reservation is active
      await assert.rejects(
        () => taskStore.transitionExecution('thread_clean_blocks_transition', 2, (t) => {
          t.status = 'running';
        }),
        /task is reserved for cleanup/i,
      );
    });

    test('commitTaskCleanup for delete writes tombstone and unlinks file; subsequent upsertTask cannot revive it', async () => {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'thread_delete_durability',
        title: 'Delete Durability Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'failed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Fail', status: 'failed', durationMs: 0, timestamp: now }],
      };
      taskStore.upsertTask(thread);

      const cachedThreadSnapshot = structuredClone(taskStore.getTask('thread_delete_durability')!);

      const { token } = await taskStore.markTaskCleanup('thread_delete_durability', 'delete', { pid: process.pid });

      // Commit deletion
      const committed = await taskStore.commitTaskCleanup('thread_delete_durability', token, 'delete', {
        tombstone: { reason: 'User requested purge', by: 'tester' },
      });
      assert.strictEqual(committed, true);

      // Verify task is gone from memory and file unlinked
      assert.strictEqual(taskStore.getTask('thread_delete_durability'), undefined);
      const filePath = path.join(tempDir, 'threads', 'thread_delete_durability.json');
      assert.strictEqual(fs.existsSync(filePath), false, 'Thread file must be unlinked');

      // Verify tombstone in deleted_tasks.log
      assert.strictEqual(taskStore.isTaskTombstoned('thread_delete_durability'), true);
      const logContent = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8');
      assert.ok(logContent.includes('thread_delete_durability'));
      assert.ok(logContent.includes(token));

      // Attempt to revive via stale upsertTask
      taskStore.upsertTask(cachedThreadSnapshot);

      // Verify task does NOT revive
      assert.strictEqual(taskStore.getTask('thread_delete_durability'), undefined);
      assert.strictEqual(fs.existsSync(filePath), false, 'Thread file must NOT be recreated by stale upsertTask');
    });

    test('legacy task cleanup preserves legacy format without creating execution or upgrading to v2', async () => {
      const now = new Date().toISOString();
      const legacyThread: DelegationThread = {
        threadId: 'thread_legacy_cleanup_safe',
        title: 'Legacy Cleanup Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Done', status: 'completed', durationMs: 0, timestamp: now }],
      };
      taskStore.upsertTask(legacyThread);

      const { token } = await taskStore.markTaskCleanup('thread_legacy_cleanup_safe', 'archive', { pid: process.pid });

      const marked = taskStore.getTask('thread_legacy_cleanup_safe');
      assert.ok(marked?.cleanup);
      assert.strictEqual(marked?.execution, undefined, 'Legacy task must NOT gain execution record during cleanup marking');

      await taskStore.abortTaskCleanup('thread_legacy_cleanup_safe', token);

      const aborted = taskStore.getTask('thread_legacy_cleanup_safe');
      assert.strictEqual(aborted?.cleanup, undefined);
      assert.strictEqual(aborted?.execution, undefined, 'Legacy task must NOT gain execution record after aborting cleanup');
    });

    test('abortTaskCleanup on v2 task clears cleanup marker and increments execution revision', async () => {
      const now = new Date().toISOString();
      const v2Thread: DelegationThread = {
        threadId: 'thread_v2_abort_cleanup',
        title: 'V2 Abort Cleanup Test',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 'session_target',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: 'Done', status: 'completed', durationMs: 0, timestamp: now }],
        execution: {
          version: 2,
          revision: 5,
          operationId: 'op_abort',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };
      taskStore.upsertTask(v2Thread);

      // markTaskCleanup increments revision from 5 to 6
      const { token } = await taskStore.markTaskCleanup('thread_v2_abort_cleanup', 'archive', { pid: process.pid });
      const marked = taskStore.getTask('thread_v2_abort_cleanup');
      assert.strictEqual(marked?.execution?.revision, 6);
      assert.ok(marked?.cleanup);

      // abortTaskCleanup increments revision from 6 to 7 and deletes cleanup marker
      const aborted = await taskStore.abortTaskCleanup('thread_v2_abort_cleanup', token);
      assert.strictEqual(aborted, true);

      const afterAbort = taskStore.getTask('thread_v2_abort_cleanup');
      assert.strictEqual(afterAbort?.cleanup, undefined);
      assert.strictEqual(afterAbort?.execution?.revision, 7);
    });
  });

  describe('Acceptance E: V2 Execution Eligibility Validation', () => {
    const now = new Date().toISOString();

    test('validateV2ExecutionEligibility rejects tasks with no execution, v1, and unknown versions', () => {
      // 1. null / undefined
      const nullRes = validateV2ExecutionEligibility(null);
      assert.strictEqual(nullRes.eligible, false);
      assert.strictEqual(nullRes.code, 'INVALID_STRUCTURE');

      // 2. Legacy task (no execution)
      const legacyTask: DelegationThread = {
        threadId: 'thread_leg',
        title: 'Leg',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 's',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [],
      };
      const legRes = validateV2ExecutionEligibility(legacyTask);
      assert.strictEqual(legRes.eligible, false);
      assert.strictEqual(legRes.code, 'NO_EXECUTION');

      // 3. V1 task
      const v1Task: DelegationThread = {
        ...structuredClone(legacyTask),
        execution: {
          version: 1,
          revision: 1,
          operationId: 'op_1',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };
      const v1Res = validateV2ExecutionEligibility(v1Task);
      assert.strictEqual(v1Res.eligible, false);
      assert.strictEqual(v1Res.code, 'LEGACY_VERSION_1');

      // 4. Unknown version (e.g. 99)
      const unknownVerTask: DelegationThread = {
        ...structuredClone(legacyTask),
        execution: {
          version: 99 as any,
          revision: 1,
          operationId: 'op_1',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };
      const unkRes = validateV2ExecutionEligibility(unknownVerTask);
      assert.strictEqual(unkRes.eligible, false);
      assert.strictEqual(unkRes.code, 'UNKNOWN_VERSION');
    });

    test('validateV2ExecutionEligibility rejects cleanup in progress, archived, and completed states', () => {
      const validV2: DelegationThread = {
        threadId: 'thread_v2_elig',
        title: 'V2',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 's',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [],
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_v2',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };

      // In progress cleanup
      const cleaning = structuredClone(validV2);
      cleaning.cleanup = { token: 't1', action: 'archive', owner: { pid: 1 }, startedAt: now, stage: 'in_progress' };
      const cleanRes = validateV2ExecutionEligibility(cleaning);
      assert.strictEqual(cleanRes.eligible, false);
      assert.strictEqual(cleanRes.code, 'CLEANUP_IN_PROGRESS');

      // Archived
      const archived = structuredClone(validV2);
      archived.archivedAt = now;
      const archRes = validateV2ExecutionEligibility(archived);
      assert.strictEqual(archRes.eligible, false);
      assert.strictEqual(archRes.code, 'ARCHIVED');

      // Completed
      const completed = structuredClone(validV2);
      completed.status = 'completed';
      const compRes = validateV2ExecutionEligibility(completed);
      assert.strictEqual(compRes.eligible, false);
      assert.strictEqual(compRes.code, 'TERMINAL_STATUS');
    });

    test('validateV2ExecutionEligibility accepts valid v2 task', () => {
      const validV2: DelegationThread = {
        threadId: 'thread_v2_valid',
        title: 'Valid V2',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 's',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [],
        execution: {
          version: 2,
          revision: 1,
          operationId: 'op_v2',
          phase: 'develop',
          round: 1,
          generation: 1,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
      };

      const res = validateV2ExecutionEligibility(validV2);
      assert.strictEqual(res.eligible, true);
      if (res.eligible) {
        assert.strictEqual(res.version, 2);
      }
    });

    test('historical queries listTasks/getTask/canArchiveTask are not blocked for legacy or v1 tasks', () => {
      const legacy: DelegationThread = {
        threadId: 'thread_hist_legacy',
        title: 'Historical Legacy',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'completed',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: 's',
        workspace: testWorkspace(tempDir),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [{ round: 1, type: 'rework', prompt: '', output: 'Done', status: 'completed', durationMs: 100, timestamp: now }],
      };
      taskStore.upsertTask(legacy);

      // getTask works
      assert.ok(taskStore.getTask('thread_hist_legacy'));

      // listTasks works
      const list = taskStore.listTasks(10);
      assert.ok(list.some((t) => t.threadId === 'thread_hist_legacy'));

      // checkExecutionEligibility reports false
      const elig = taskStore.validateExecutionEligibility('thread_hist_legacy');
      assert.strictEqual(elig.eligible, false);
      assert.strictEqual(elig.code, 'NO_EXECUTION');
    });
  });

  describe('Review Boundaries & Invariant Regressions (P1 Tests)', () => {
    function setupReviewFixture(s: TaskStore, dir: string): { disk: () => any } {
      const now = new Date().toISOString();
      const thread: DelegationThread = {
        threadId: 'review_test',
        title: 'Review fixture',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: '',
        workspace: testWorkspace(dir),
        summary: {
          lastPromptSnippet: 'original',
          lastOutputSnippet: '',
          totalDurationMs: 0,
        },
        rounds: [
          {
            round: 1,
            type: 'initial',
            prompt: 'original',
            status: 'running',
            durationMs: 0,
            timestamp: now,
          },
        ],
        execution: {
          version: 2,
          revision: 0,
          operationId: 'op-current',
          phase: 'develop',
          round: 1,
          generation: 2,
          attempt: 1,
          autoRetriesUsed: 0,
          budget: { limitMs: 7200000, spentActiveMs: 0, maxAutoRetries: 2 },
          run: {
            id: 'run-current',
            owner: { pid: process.pid },
            role: 'developer',
            step: 'executing',
            state: 'active',
            startedAt: now,
          },
        },
      };
      s.upsertTask(thread);
      const disk = () => JSON.parse(fs.readFileSync(path.join(dir, 'threads', 'review_test.json'), 'utf8'));
      return { disk };
    }

    test('v2 evidence requires complete execution identity: missing or partial identity is rejected', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      // Omitted identity
      taskStore.reportRoundEvidence('review_test', 1, { output: 'unidentified stale report' });
      assert.strictEqual(disk().rounds[0].output, undefined);

      // Partial identity (missing runId)
      taskStore.reportRoundEvidence('review_test', 1, { output: 'partial-identity-report' }, {
        operationId: 'op-current',
        generation: 2,
      } as any);
      assert.strictEqual(disk().rounds[0].output, undefined);
    });

    test('v2 ordinary upsert cannot replace round prompt or append next round', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const stale = structuredClone(taskStore.getTask('review_test')!);
      stale.rounds[0].prompt = 'replaced by ordinary writer';
      stale.rounds.push({
        round: 2,
        type: 'rework',
        prompt: 'unauthorized next round',
        status: 'completed',
        durationMs: 0,
        timestamp: new Date().toISOString(),
      });
      taskStore.upsertTask(stale);
      assert.strictEqual(disk().rounds[0].prompt, 'original');
      assert.strictEqual(disk().rounds.length, 1);
    });

    test('v2 updateRound cannot replace round identity, type, prompt, or timestamp', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      taskStore.updateRound(
        'review_test',
        1,
        { round: 99, prompt: 'replaced', type: 'approval' } as any,
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(disk().rounds.length, 1);
      assert.strictEqual(disk().rounds[0].round, 1);
      assert.strictEqual(disk().rounds[0].prompt, 'original');
    });

    test('saveToDisk cannot persist mutable cached v2 control fields', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const cached = taskStore.getTask('review_test')!;
      cached.execution!.run!.id = 'forged-run';
      cached.status = 'paused';
      taskStore.saveToDisk();
      assert.strictEqual(disk().execution.run.id, 'run-current');
      assert.strictEqual(disk().status, 'running');
    });

    test('deleteTask without token cannot steal another cleanup reservation', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.markTaskCleanup('review_test', 'delete', { pid: process.pid }, { token: 'owned-by-cleaner-A' });
      assert.strictEqual(taskStore.deleteTask('review_test'), false);
      assert.strictEqual(disk().cleanup.token, 'owned-by-cleaner-A');
    });

    test('cleanup commit must match reserved action', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      await assert.rejects(
        taskStore.commitTaskCleanup('review_test', token, 'delete'),
        /action|reservation|mismatch/i,
      );
      assert.strictEqual(disk().cleanup.action, 'archive');
    });

    test('real child report cannot race through held task CAS lock and get lost', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const modulePath = path.join(__dirname, '../bridge/task-store.js');
      const childCode = `const {TaskStore}=require(${JSON.stringify(modulePath)}); const s=new TaskStore(${JSON.stringify(tempDir)}); try{s.reportRoundEvidence('review_test',1,{output:'durable-child-evidence'},{operationId:'op-current',generation:2,runId:'run-current'});} finally{s.dispose();}`;
      const child = spawn(process.execPath, ['-e', childCode], { stdio: ['ignore', 'pipe', 'pipe'] });
      let stderr = '';
      child.stderr?.on('data', (d) => (stderr += d));
      const childExit = new Promise<number>((resolve, reject) => {
        child.on('exit', (code) =>
          code === 0 ? resolve(0) : reject(new Error(`Child exited with ${code}: ${stderr}`)),
        );
      });
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'paused';
      });
      await childExit;
      assert.strictEqual(disk().status, 'paused');
      assert.strictEqual(disk().rounds[0].output, 'durable-child-evidence');
    });

    test('upsert cannot append a round even if currentRound remains pinned', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const copy = structuredClone(taskStore.getTask('review_test')!);
      copy.rounds.push({
        round: 2,
        type: 'rework',
        prompt: 'unauthorized',
        status: 'completed',
        durationMs: 0,
        timestamp: new Date().toISOString(),
      });
      taskStore.upsertTask(copy);
      assert.strictEqual(disk().rounds.length, 1);
    });

    test('same TaskStore concurrent CAS allows only one winner at revision zero', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const outcomes = await Promise.allSettled([
        taskStore.transitionExecution('review_test', 0, (t) => {
          t.status = 'paused';
        }),
        taskStore.transitionExecution('review_test', 0, (t) => {
          t.status = 'reviewing';
        }),
      ]);
      assert.strictEqual(outcomes.filter((x) => x.status === 'fulfilled').length, 1);
      assert.strictEqual(disk().execution.revision, 1);
    });

    test('ordinary upsert cannot submit unsigned v2 output and round status', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const t = taskStore.getTask('review_test')!;
      t.rounds[0].output = 'unsigned-output';
      t.rounds[0].status = 'completed';
      taskStore.upsertTask(t);
      assert.strictEqual(disk().rounds[0].output, undefined);
      assert.strictEqual(disk().rounds[0].status, 'running');
    });

    test('getAllTasks mutable cache cannot bypass identity through saveToDisk', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const t = taskStore.getAllTasks()[0];
      t.rounds[0].output = 'unsigned-from-cache';
      t.rounds[0].status = 'completed';
      taskStore.saveToDisk();
      assert.strictEqual(disk().rounds[0].output, undefined);
      assert.strictEqual(disk().rounds[0].status, 'running');
    });

    test('saveToDisk cannot erase v2 rounds from mutable cache', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      taskStore.getAllTasks()[0].rounds = [];
      taskStore.saveToDisk();
      assert.strictEqual(disk().rounds.length, 1);
    });

    test('direct deleteTask cannot consume archive reservation token', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      assert.strictEqual(taskStore.deleteTask('review_test', token), false);
      assert.strictEqual(disk().cleanup.action, 'archive');
    });

    test('legacy archive mutation cannot manufacture v2 execution', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
        delete (t as any).execution;
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive', {
          archiveMutation: (t) => {
            t.execution = { version: 2, revision: 0 } as any;
            (t as any).archivedAt = new Date().toISOString();
          },
        });
      } catch {}
      assert.strictEqual(disk().execution, undefined);
      assert.strictEqual(disk().archivedAt, undefined);
    });

    test('current execution credentials do not authorize modifying prior round', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.rounds.unshift({ ...t.rounds[0], round: 0, status: 'completed', output: 'historical evidence' });
      });
      taskStore.reportRoundEvidence(
        'review_test',
        0,
        { output: 'wrong-round-report' },
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(disk().rounds[0].output, 'historical evidence');
    });

    test('failed durable tombstone prevents physical deletion', () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      fs.mkdirSync(path.join(tempDir, 'deleted_tasks.log'));
      assert.throws(() => taskStore.deleteTask('review_test'));
      assert.strictEqual(disk().threadId, 'review_test');
      fs.rmdirSync(path.join(tempDir, 'deleted_tasks.log'));
    });

    test('compaction flag cannot authorize arbitrary historical evidence edits', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.rounds.unshift({ ...t.rounds[0], round: 0, status: 'completed', output: 'historical evidence' });
      });
      taskStore.updateRound(
        'review_test',
        0,
        { output: 'rewritten history', error: 'forged error', status: 'failed', isCompacted: true },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(disk().rounds[0].output, 'historical evidence');
      assert.strictEqual(disk().rounds[0].status, 'completed');
    });

    test('archive commit cannot discard history without durable archive', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      let refused = false;
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive');
      } catch {
        refused = true;
      }
      const hasArchive = fs.readdirSync(tempDir, { recursive: true }).some((x) => String(x).endsWith('.tar.gz'));
      assert.ok(refused || hasArchive || disk().rounds.length === 1, 'history discarded without any archive');
    });

    for (const [label, action] of [
      [
        'updateRound',
        `s.updateRound('review_test',1,{output:'lock-bypass'},undefined,undefined,undefined,undefined,undefined,{operationId:'op-current',generation:2,runId:'run-current'})`,
      ],
      [
        'recordMergeRequest',
        `s.recordMergeRequest('review_test',{status:'synced',url:'https://example.test/mr/1',updatedAt:new Date().toISOString()})`,
      ],
      ['deleteTask', `s.deleteTask('review_test')`],
    ]) {
      test(`${label} must respect the same real OS task lock`, async () => {
        setupReviewFixture(taskStore, tempDir);
        const lock = new MeshFileLock(path.join(tempDir, 'execution-locks', 'review_test.lock'));
        const release = await lock.acquire(undefined, undefined, 15000);
        assert.ok(release);
        const ready = path.join(tempDir, `ready-${label}`);
        const done = path.join(tempDir, `done-${label}`);
        const mod = path.join(__dirname, '../bridge/task-store.js');
        const code = `const fs=require('fs');const {TaskStore}=require(${JSON.stringify(mod)});const s=new TaskStore(${JSON.stringify(tempDir)});(async()=>{try{fs.writeFileSync(${JSON.stringify(ready)},'ready');const result=await ${action};fs.writeFileSync(${JSON.stringify(done)},result?'committed':'refused');}catch(e){fs.writeFileSync(${JSON.stringify(done)},'refused:'+e.message);}finally{s.dispose();}})();`;
        let childExit: Promise<number>;
        let observed: string;
        try {
          const child = spawn(process.execPath, ['-e', code], { stdio: ['ignore', 'pipe', 'pipe'] });
          childExit = new Promise<number>((resolve, reject) => {
            child.on('error', reject);
            child.on('exit', (code) => resolve(code ?? 0));
          });
          const deadline = Date.now() + 10000;
          while (!fs.existsSync(ready) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 10));
          assert.ok(fs.existsSync(ready));
          const waitUntil = Date.now() + 500;
          while (!fs.existsSync(done) && Date.now() < waitUntil) await new Promise((r) => setTimeout(r, 10));
          observed = fs.existsSync(done) ? fs.readFileSync(done, 'utf8') : 'waiting';
        } finally {
          release();
          lock.dispose();
        }
        assert.strictEqual(await childExit, 0);
        assert.notStrictEqual(observed, 'committed');
      });
    }

    test('missing archive must leave cleanup retriable without archivedAt', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive');
      } catch {}
      assert.strictEqual(disk().archivedAt, undefined);
      assert.strictEqual(disk().cleanup.token, token);
    });

    test('directory passed as tarGzPaths cannot authorize dropping rounds', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive', { tarGzPaths: [tempDir] });
      } catch {}
      assert.strictEqual(disk().rounds.length, 1);
      assert.strictEqual(disk().archivedAt, undefined);
    });

    test('historical compaction needs successful trigger and durable original output', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.rounds.unshift({ ...t.rounds[0], round: 0, status: 'completed', output: 'original historical output' });
      });
      taskStore.updateRound(
        'review_test',
        0,
        { isCompacted: true, output: 'card without original archive' },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(disk().rounds[0].output, 'original historical output');
      assert.strictEqual(disk().rounds[0].isCompacted, undefined);
    });

    test('archive commit with valid durable .tar.gz file succeeds, sets archivedAt, clears rounds and reloads cleanly', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const artifactDir = path.join(tempDir, 'task-artifacts');
      const worker = path.join(artifactDir, 'worker_log');
      fs.mkdirSync(worker, { recursive: true });
      fs.writeFileSync(path.join(worker, 'review_test_round_1.log'), 'real worker evidence');

      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
        t.artifactDir = artifactDir;
        t.taskDataDir = artifactDir;
        t.rounds[0].output = 'completed output';
      });

      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      const currentThread = taskStore.getTask('review_test')!;
      const { tarGzPaths } = archiveCompletedTaskLogs(currentThread, tempDir);

      const commitResult = await taskStore.commitTaskCleanup('review_test', token, 'archive', { tarGzPaths });
      assert.strictEqual(commitResult, true);
      assert.ok(disk().archivedAt);
      assert.strictEqual(disk().rounds.length, 0);

      // Reload fresh from disk
      const freshStore = new TaskStore(tempDir);
      try {
        const reloaded = freshStore.getTask('review_test');
        assert.ok(reloaded?.archivedAt);
        assert.strictEqual(reloaded?.rounds.length, 0);
      } finally {
        freshStore.dispose();
      }
    });

    test('historical compaction with valid trigger assessment and durable raw output file succeeds and reloads cleanly', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.rounds.unshift({ ...t.rounds[0], round: 0, status: 'completed', output: 'original historical output' });
        t.rounds[1].assessment = { success: true, exitCode: 0 } as any;
      });
      const rawOutputFilePath = path.join(tempDir, 'round_0.output.md');
      fs.writeFileSync(rawOutputFilePath, 'original historical output');

      const res = await taskStore.updateRound(
        'review_test',
        0,
        { isCompacted: true, output: 'compact capsule', rawOutputFilePath },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.ok(res);
      assert.strictEqual(disk().rounds[0].output, 'compact capsule');
      assert.strictEqual(disk().rounds[0].isCompacted, true);

      // Reload fresh from disk
      const freshStore = new TaskStore(tempDir);
      try {
        const reloaded = freshStore.getTask('review_test');
        assert.strictEqual(reloaded?.rounds[0].output, 'compact capsule');
        assert.strictEqual(reloaded?.rounds[0].isCompacted, true);
      } finally {
        freshStore.dispose();
      }
    });

    test('v2 updateRound and recordMergeRequest succeed and reload cleanly when lock is uncontented', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const updateRes = await taskStore.updateRound(
        'review_test',
        1,
        { output: 'successful-evidence-output' },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.ok(updateRes);
      assert.strictEqual(disk().rounds[0].output, 'successful-evidence-output');

      const mrRes = await taskStore.recordMergeRequest('review_test', {
        status: 'synced',
        url: 'https://example.test/mr/123',
        updatedAt: new Date().toISOString(),
      });
      assert.ok(mrRes);
      assert.strictEqual(disk().mergeRequest?.url, 'https://example.test/mr/123');

      // Reload fresh from disk
      const freshStore = new TaskStore(tempDir);
      try {
        const reloaded = freshStore.getTask('review_test');
        assert.strictEqual(reloaded?.rounds[0].output, 'successful-evidence-output');
        assert.strictEqual(reloaded?.mergeRequest?.url, 'https://example.test/mr/123');
      } finally {
        freshStore.dispose();
      }
    });

    test('stale memory cannot replace fresh on-disk v2 execution on upsert', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const stale = taskStore.getTask('review_test')!;
      const threadDir = path.join(tempDir, 'threads');
      const stamp = fs.statSync(threadDir);
      const other = new TaskStore(tempDir);
      try {
        await other.transitionExecution('review_test', 0, (t) => {
          t.execution!.run!.id = 'run-new';
          t.execution!.generation = 3;
          t.status = 'paused';
        });
      } finally {
        other.dispose();
      }
      fs.utimesSync(threadDir, stamp.atime, stamp.mtime);
      stale.title = 'ordinary metadata update';
      taskStore.upsertTask(stale);
      assert.strictEqual(disk().execution.revision, 1);
      assert.strictEqual(disk().execution.run.id, 'run-new');
      assert.strictEqual(disk().execution.generation, 3);
    });

    test('archiveMutation cannot discard rounds without prepared archive', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive', {
          archiveMutation: (t) => {
            t.rounds = [];
          },
        });
      } catch {}
      assert.strictEqual(disk().rounds.length, 1);
      assert.strictEqual(disk().archivedAt, undefined);
    });

    test('nonempty garbage tar.gz cannot authorize dropping task history', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
      });
      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      const fake = path.join(tempDir, 'fake.tar.gz');
      fs.writeFileSync(fake, 'not a gzip or tar archive');
      try {
        await taskStore.commitTaskCleanup('review_test', token, 'archive', { tarGzPaths: [fake] });
      } catch {}
      assert.strictEqual(disk().rounds.length, 1);
      assert.strictEqual(disk().archivedAt, undefined);
    });

    test('compaction original archive must contain the original output', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.rounds[0].assessment = { success: true } as any;
        t.rounds.unshift({ ...t.rounds[0], round: 0, status: 'completed', output: 'precious original evidence' });
      });
      const wrong = path.join(tempDir, 'wrong-original.txt');
      fs.writeFileSync(wrong, 'unrelated contents');
      await taskStore.updateRound(
        'review_test',
        0,
        { isCompacted: true, output: 'card', rawOutputFilePath: wrong },
        undefined,
        undefined,
        undefined,
        undefined,
        undefined,
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(disk().rounds[0].output, 'precious original evidence');
      assert.strictEqual(disk().rounds[0].isCompacted, undefined);
    });

    test('ordinary upsert cannot write existing v2 task while real OS lock is held', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const lock = new MeshFileLock(path.join(tempDir, 'execution-locks', 'review_test.lock'));
      const release = await lock.acquire(undefined, undefined, 15000);
      assert.ok(release);
      let pending: any;
      let wroteWhileHeld = false;
      try {
        const t = taskStore.getTask('review_test')!;
        t.title = 'metadata-written-through-held-lock';
        try {
          pending = taskStore.upsertTask(t);
        } catch {}
        wroteWhileHeld = disk().title === 'metadata-written-through-held-lock';
      } finally {
        release();
        lock.dispose();
      }
      await pending;
      assert.strictEqual(wroteWhileHeld, false, 'ordinary upsert bypassed the task OS lock and renamed a full snapshot');
    });

    test('valid gzip tar with matching ID but missing worker logs must not finalize archive', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const artifactDir = path.join(tempDir, 'task-artifacts');
      const worker = path.join(artifactDir, 'worker_log');
      fs.mkdirSync(worker, { recursive: true });
      fs.writeFileSync(path.join(worker, 'review_test_round_1.log'), 'complete worker evidence that must be in archive');

      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
        t.artifactDir = artifactDir;
        t.taskDataDir = artifactDir;
        t.rounds[0].output = 'completed worker output';
      });

      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      const archivePath = path.join(tempDir, 'incomplete.tar.gz');
      fs.writeFileSync(
        archivePath,
        createTarGzBuffer([
          {
            name: 'summary.json',
            content: JSON.stringify({ threadId: 'review_test', archivedLogFiles: [] }),
          },
        ]),
      );

      const result = await taskStore.commitTaskCleanup('review_test', token, 'archive', { tarGzPaths: [archivePath] });
      assert.strictEqual(result, false, 'summary-only package lost task snapshot / current worker logs');
      assert.strictEqual(disk().archivedAt, undefined);
      assert.strictEqual(disk().rounds.length, 1);
      assert.strictEqual(disk().cleanup.token, token);
    });

    test('legacy cleanup reservation cannot be consumed through unlocked archiveTask', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
        delete t.execution;
      });

      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      const lock = new MeshFileLock(path.join(tempDir, 'execution-locks', 'review_test.lock'));
      const release = await lock.acquire(undefined, undefined, 15000);
      assert.ok(release);

      let result: any;
      let committedWhileHeld: boolean;
      try {
        result = taskStore.archiveTask('review_test', token);
        committedWhileHeld = Boolean(disk().archivedAt);
      } finally {
        release();
        lock.dispose();
      }

      await result;
      assert.strictEqual(committedWhileHeld, false, 'legacy cleanup state committed while its task OS lock was held');
    });

    test('archive built by real helper must be rejected after same-length worker log rewrite', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const artifactDir = path.join(tempDir, 'task-artifacts');
      const workerDir = path.join(artifactDir, 'worker_log');
      fs.mkdirSync(workerDir, { recursive: true });
      const log = path.join(workerDir, 'review_test_round_1.log');
      const original = 'original evidence from worker';
      fs.writeFileSync(log, original);

      await taskStore.transitionExecution('review_test', 0, (t) => {
        t.status = 'completed';
        t.artifactDir = artifactDir;
        t.taskDataDir = artifactDir;
        t.rounds[0].output = 'completed worker output';
      });

      const { token } = await taskStore.markTaskCleanup('review_test', 'archive', { pid: process.pid });
      const currentTask = taskStore.getTask('review_test')!;
      const prepared = archiveCompletedTaskLogs(currentTask, tempDir);

      // A retry/late writer replaces a log with different bytes, without changing its size.
      fs.writeFileSync(log, 'X'.repeat(Buffer.byteLength(original)));

      const result = await taskStore.commitTaskCleanup('review_test', token, 'archive', {
        tarGzPaths: prepared.tarGzPaths,
      });

      assert.strictEqual(
        result,
        false,
        'archive contained old bytes while same-sized current worker evidence was not archived',
      );
      assert.strictEqual(disk().archivedAt, undefined);
      assert.strictEqual(disk().cleanup.token, token);
      assert.strictEqual(disk().rounds.length, 1);
    });

    test('valid new run evidence is checked against disk even when this store caches previous run', async () => {
      const { disk } = setupReviewFixture(taskStore, tempDir);
      const old = taskStore.getTask('review_test')!;
      assert.strictEqual(old.execution?.run?.id, 'run-current');

      const other = new TaskStore(tempDir);
      try {
        await other.transitionExecution('review_test', 0, (t) => {
          t.execution!.operationId = 'op-next';
          t.execution!.generation = 3;
          t.execution!.run!.id = 'run-next';
        });
      } finally {
        other.dispose();
      }

      assert.strictEqual(disk().execution.run.id, 'run-next');
      const result = await taskStore.reportRoundEvidence(
        'review_test',
        1,
        { output: 'valid new run report' },
        { operationId: 'op-next', generation: 3, runId: 'run-next' },
      );
      assert.ok(result, 'current disk-valid execution identity was rejected by an old in-memory cache');
      assert.strictEqual(disk().rounds[0].output, 'valid new run report');

      const staleResult = await taskStore.reportRoundEvidence(
        'review_test',
        1,
        { output: 'stale run report' },
        { operationId: 'op-current', generation: 2, runId: 'run-current' },
      );
      assert.strictEqual(staleResult, undefined, 'stale run credentials were not rejected by disk check');
      assert.strictEqual(disk().rounds[0].output, 'valid new run report');
    });
  });
});
