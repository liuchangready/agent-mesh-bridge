import { testWorkspace } from './workspace-fixture';
import { enterTaskFailure } from '../bridge/types';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore, normalizeThread } from '../bridge/task-store';
import { ReworkController } from '../bridge/rework-controller';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { formatTaskTreeStatus } from '../ui/task-status-formatter';
import { NativeSessionCompactor } from '../bridge/native-session-compactor';
import {
  DelegationThread,
  DelegationRound,
  findInFlightRound,
} from '../bridge/types';

describe('Running and Reworking Status Merge Tests', () => {
  let tmpDir: string;
  let taskStore: TaskStore;
  let reworkController: ReworkController;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-status-test-'));
    taskStore = new TaskStore(tmpDir);
    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    runner.execute = async () => {
      await new Promise((r) => setTimeout(r, 100));
      return {
        exitCode: 0,
        stdout: 'mock execution output',
        stderr: '',
        extractedOutput: 'mock execution output',
        durationMs: 50,
        assessment: {
          success: true,
          errorCode: 'OK',
          exitCode: 0,
          warnings: [],
          stream: {
            chunkCount: 1,
            stdoutBytes: 21,
            stderrBytes: 0,
            firstOutputAtMs: 1,
            lastOutputAtMs: 2,
            maxGapMs: 1,
            rawMaxGapMs: 1,
            excludedSuspensionMs: 0,
            suspensionCount: 0,
            gapThresholdMs: 3000,
            hasActualOutput: true,
            continuous: true,
            gapExceeded: false,
          },
        },
      };
    };
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    const gate = new TaskExecutionRegistry(tmpDir);
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
  });

  afterEach(() => {
    taskStore.dispose();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  test('read-time normalization: legacy status "reworking" on disk loads as "running" with all metadata preserved', () => {
    const threadId = 'thread_legacy_reworking_001';
    const threadsDir = path.join(tmpDir, 'threads');
    fs.mkdirSync(threadsDir, { recursive: true });
    const legacyThreadJson = {
      threadId,
      title: 'Legacy Reworking Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'reworking',
      currentRound: 2,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:15:00.000Z',
      receiverSessionId: 'native-session-legacy-1234',
      callerSessionId: 'caller-session-legacy-5678',
      workspace: testWorkspace(tmpDir),
      worktree: {
        path: tmpDir,
        taskBranch: 'mesh/task/thread_legacy_reworking_001',
        baseCommit: 'commit-base-001',
        headCommit: 'commit-head-002',
      },
      summary: {
        lastPromptSnippet: 'Fix hashing logic',
        lastOutputSnippet: 'Executing rework in background...',
        totalDurationMs: 12000,
      },
      interruption: {
        kind: 'paused',
        reason: 'User paused earlier',
        at: '2026-10-20T10:10:00.000Z',
        by: 'user',
      },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Initial request',
          status: 'completed',
          durationMs: 5000,
          output: 'Round 1 output',
          timestamp: '2026-10-20T10:00:00.000Z',
        },
        {
          round: 2,
          type: 'rework',
          prompt: 'Fix hashing logic',
          status: 'running',
          durationMs: 7000,
          output: 'Partial rework output',
          timestamp: '2026-10-20T10:10:00.000Z',
          tokenUsage: {
            promptTokens: 100,
            outputTokens: 50,
            totalTokens: 150,
          },
        },
      ],
    };

    // Write legacy JSON directly to disk
    const filePath = path.join(threadsDir, `${threadId}.json`);
    fs.writeFileSync(filePath, JSON.stringify(legacyThreadJson, null, 2), 'utf8');

    // Read through taskStore
    taskStore.loadFromDisk();
    const loaded = taskStore.getTask(threadId);

    assert.ok(loaded);
    assert.strictEqual(loaded.status, 'running', 'Legacy reworking status must be normalized to running');
    assert.strictEqual(loaded.currentRound, 2);
    assert.strictEqual(loaded.receiverSessionId, 'native-session-legacy-1234');
    assert.strictEqual(loaded.callerSessionId, 'caller-session-legacy-5678');
    assert.strictEqual(loaded.rounds.length, 2);
    assert.strictEqual(loaded.rounds[0].type, 'initial');
    assert.strictEqual(loaded.rounds[1].type, 'rework');
    assert.strictEqual(loaded.rounds[1].status, 'running');
    assert.strictEqual(loaded.rounds[1].output, 'Partial rework output');
    assert.deepStrictEqual(loaded.rounds[1].tokenUsage, {
      promptTokens: 100,
      outputTokens: 50,
      totalTokens: 150,
    });
    assert.strictEqual(loaded.worktree?.taskBranch, 'mesh/task/thread_legacy_reworking_001');
    assert.strictEqual(loaded.interruption?.reason, 'User paused earlier');

    // On-disk file is only rewritten on next normal upsert
    taskStore.upsertTask(loaded);
    const onDiskAfterUpsert = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    assert.strictEqual(onDiskAfterUpsert.status, 'running');
  });

  test('statusFilter compatibility alias: "reworking" filters tasks in running status with active rework round', () => {
    const threadInitial: DelegationThread = {
      threadId: 'thread_initial_run',
      title: 'Initial Running Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'running',
      currentRound: 1,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:00:00.000Z',
      receiverSessionId: 'sess-init',
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Do initial work',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
      ],
    };

    const threadRework: DelegationThread = {
      threadId: 'thread_rework_run',
      title: 'Rework Running Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'running',
      currentRound: 2,
      createdAt: '2026-10-20T10:05:00.000Z',
      updatedAt: '2026-10-20T10:05:00.000Z',
      receiverSessionId: 'sess-rework',
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Do initial work',
          status: 'completed',
          durationMs: 1000,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
        {
          round: 2,
          type: 'rework',
          prompt: 'Do rework',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:05:00.000Z',
        },
      ],
    };

    taskStore.upsertTask(threadInitial);
    taskStore.upsertTask(threadRework);

    // Filtering by 'running' returns both
    const runningTasks = taskStore.listTasks(20, 'running');
    assert.strictEqual(runningTasks.length, 2);

    // Filtering by 'active' returns both
    const activeTasks = taskStore.listTasks(20, 'active');
    assert.strictEqual(activeTasks.length, 2);

    // Filtering by compatibility alias 'reworking' only returns threadRework
    const reworkTasks = taskStore.listTasks(20, 'reworking');
    assert.strictEqual(reworkTasks.length, 1);
    assert.strictEqual(reworkTasks[0].threadId, 'thread_rework_run');
    assert.strictEqual(reworkTasks[0].status, 'running');
    assert.strictEqual(findInFlightRound(reworkTasks[0])?.type, 'rework');
  });

  test('rework creation and retry/resume lifecycle writes status "running"', async () => {
    const threadId = 'thread_lifecycle_test';
    const initialThread: DelegationThread = {
      threadId,
      title: 'Lifecycle Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'waiting_review',
      currentRound: 1,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:00:00.000Z',
      receiverSessionId: 'session-stable-001',
      receiverSessionCaptured: true,
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Implement feature A',
          status: 'completed',
          durationMs: 3000,
          output: 'Feature A done',
          timestamp: '2026-10-20T10:00:00.000Z',
          touchedFiles: ['featureA.ts'],
        },
      ],
    };
    taskStore.upsertTask(initialThread);

    // 1. Request rework
    const reworkResult = await reworkController.requestRework({
      threadId,
      feedback: 'Please add unit tests for feature A',
    });
    assert.strictEqual(reworkResult.status, 'REWORK_DISPATCHED');
    assert.strictEqual(reworkResult.round, 2);

    const reworkThread = taskStore.getTask(threadId)!;
    assert.strictEqual(reworkThread.status, 'running', 'Rework round must set task status to running');
    assert.strictEqual(reworkThread.rounds.length, 2);
    assert.strictEqual(reworkThread.rounds[1].type, 'rework');
    assert.strictEqual(reworkThread.rounds[1].status, 'running');
    assert.strictEqual(reworkThread.receiverSessionId, 'session-stable-001', 'Must reuse native session');

    // Simulate failure of round 2
    reworkThread.rounds[1].status = 'failed';
    reworkThread.rounds[1].error = 'Simulated timeout';
    enterTaskFailure(reworkThread, { stage: 'execution', reason: 'Simulated timeout', at: new Date().toISOString(), round: 2 });
    taskStore.upsertTask(reworkThread);

    // 2. Retry failed round 2
    const retriedRound = await reworkController.retryFailedRound(threadId);
    assert.strictEqual(retriedRound.round, 2);
    assert.strictEqual(retriedRound.type, 'rework');
    assert.strictEqual(retriedRound.status, 'running');

    const retriedThread = taskStore.getTask(threadId)!;
    assert.strictEqual(retriedThread.status, 'running', 'Retried round sets thread status to running');
    assert.strictEqual(retriedThread.rounds.length, 2, 'Must not duplicate round on retry');
    assert.strictEqual(retriedThread.rounds[1].type, 'rework');
    assert.strictEqual(retriedThread.receiverSessionId, 'session-stable-001');

    // Simulate pause of round 2
    retriedThread.rounds[1].status = 'paused';
    retriedThread.status = 'paused';
    taskStore.upsertTask(retriedThread);

    // 3. Resume paused round 2
    const resumedRound = await reworkController.resumePausedTask(threadId);
    assert.strictEqual(resumedRound.round, 2);
    assert.strictEqual(resumedRound.type, 'rework');
    assert.strictEqual(resumedRound.status, 'running');

    const resumedThread = taskStore.getTask(threadId)!;
    assert.strictEqual(resumedThread.status, 'running', 'Resumed task sets status to running');
    assert.strictEqual(resumedThread.rounds.length, 2);
    assert.strictEqual(resumedThread.rounds[1].type, 'rework');
    assert.strictEqual(resumedThread.receiverSessionId, 'session-stable-001');
  });

  test('UI task tree status formatter: differentiates Running vs Reworking based on active round type', () => {
    const threadInitial: DelegationThread = {
      threadId: 'tree_init',
      title: 'Initial Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'running',
      currentRound: 1,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:00:00.000Z',
      receiverSessionId: 'sess-init',
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Initial',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
      ],
    };

    const threadRework: DelegationThread = {
      threadId: 'tree_rework',
      title: 'Rework Task',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'running',
      currentRound: 2,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:00:00.000Z',
      receiverSessionId: 'sess-rework',
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Initial',
          status: 'completed',
          durationMs: 1000,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
        {
          round: 2,
          type: 'rework',
          prompt: 'Rework',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
      ],
    };

    const itemInit = formatTaskTreeStatus(threadInitial);
    assert.strictEqual(itemInit.statusLabel, 'Running (Round 1)');
    assert.strictEqual(itemInit.iconName, 'sync~spin');

    const itemRework = formatTaskTreeStatus(threadRework);
    assert.strictEqual(itemRework.statusLabel, 'Reworking (Round 2)');
    assert.strictEqual(itemRework.iconName, 'sync~spin');

    // Idle running thread with no active round displays fallback Running
    const threadNoActiveRound: DelegationThread = {
      ...threadRework,
      threadId: 'tree_idle',
      rounds: threadRework.rounds.map((r) => ({ ...r, status: 'completed' })),
    };
    const itemIdle = formatTaskTreeStatus(threadNoActiveRound);
    assert.strictEqual(itemIdle.statusLabel, 'Running (Round 2)');
  });

  test('NativeSessionCompactor business success acknowledges rework under running status', () => {
    const compactor = new NativeSessionCompactor(taskStore, new AgentDetector());
    const threadId = 'thread_compactor_test';

    const thread: DelegationThread = {
      threadId,
      title: 'Compactor Thread',
      initiatorAgent: 'codex',
      targetAgent: 'antigravity',
      status: 'running',
      currentRound: 2,
      createdAt: '2026-10-20T10:00:00.000Z',
      updatedAt: '2026-10-20T10:00:00.000Z',
      receiverSessionId: 'receiver-sess-1',
      reviewerSessionId: 'reviewer-sess-1',
      workspace: testWorkspace(tmpDir),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Initial',
          status: 'completed',
          durationMs: 1000,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
        {
          round: 2,
          type: 'rework',
          prompt: 'Rework',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
      ],
    };
    taskStore.upsertTask(thread);

    // Reviewer business success when task transitioned to rework (status: 'running', inFlight.type: 'rework')
    compactor.recordBusinessSuccess(threadId, 'reviewer', 'reviewer-sess-1', 'rev-turn-1');
    const updated = taskStore.getTask(threadId)!;
    assert.strictEqual(
      updated.nativeSessionCompaction?.reviewer?.turnsSinceCompaction,
      1,
      'Reviewer business turn should be recorded when task is in rework execution',
    );

    // But if status is running with initial round, reviewer business success must be refused
    const threadInit: DelegationThread = {
      ...thread,
      threadId: 'thread_compactor_init',
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Initial',
          status: 'running',
          durationMs: 0,
          timestamp: '2026-10-20T10:00:00.000Z',
        },
      ],
    };
    taskStore.upsertTask(threadInit);
    compactor.recordBusinessSuccess('thread_compactor_init', 'reviewer', 'reviewer-sess-1', 'rev-turn-2');
    const updatedInit = taskStore.getTask('thread_compactor_init')!;
    assert.strictEqual(
      updatedInit.nativeSessionCompaction?.reviewer?.turnsSinceCompaction,
      undefined,
      'Reviewer business turn must NOT be recorded during initial developer execution',
    );
  });
});
