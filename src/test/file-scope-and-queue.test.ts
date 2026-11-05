import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// Keep this file's supervisor heartbeat and execution gate out of the real
// ~/.agent-mesh-bridge, so concurrently running test files cannot observe a
// "supervisor is active" or "a task is already running" state from here.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-queue-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import {
  normalizeWorkspacePath,
  areFileScopesConflicting,
  doPathsConflict,
  findFileConflicts,
  validateExpectedFiles,
} from '../bridge/file-scope';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { DelegationThread } from '../bridge/types';

describe('File Scope and Conflict Queueing Tests', () => {
  describe('Path Normalization and Conflict Detection', () => {
    test('should normalize relative and workspace absolute paths uniformly', () => {
      // Workspace paths are fixtures, not this machine's layout: the point is
      // that a backslash absolute path and a relative one normalize alike.
      const ws = path.join(os.tmpdir(), 'majiang');
      const p1 = `${ws.replace(/\//g, '\\')}\\presentation\\src\\main\\MenuScreen.kt`;
      const p2 = 'presentation/src/main/MenuScreen.kt';
      const p3 = './presentation/src/main/MenuScreen.kt';

      assert.strictEqual(
        normalizeWorkspacePath(p1, ws),
        normalizeWorkspacePath(p2, ws),
      );
      assert.strictEqual(
        normalizeWorkspacePath(p2, ws),
        normalizeWorkspacePath(p3, ws),
      );
    });

    test('should detect exact file matches as conflicting', () => {
      const f1 = 'presentation/menu/MenuScreen.kt';
      const f2 = 'presentation\\menu\\MenuScreen.kt';
      assert.strictEqual(areFileScopesConflicting([f1], [f2]), true);
    });

    test('should distinguish distinct sibling files as non-conflicting', () => {
      const f1 = 'presentation/menu/MenuScreen.kt';
      const f2 = 'presentation/render/TableBoardRenderer.kt';
      assert.strictEqual(areFileScopesConflicting([f1], [f2]), false);
    });

    test('should reject directory scopes instead of locking a subtree', () => {
      const workspace = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-scope-dir-'));
      fs.mkdirSync(path.join(workspace, 'presentation', 'menu'), { recursive: true });
      try {
        const noSlash = validateExpectedFiles(['presentation/menu'], workspace);
        assert.strictEqual(noSlash.ok, false);
        assert.match(noSlash.errors.join(' '), /directory/i);

        const trailingSlash = validateExpectedFiles(['presentation/menu/'], workspace);
        assert.strictEqual(trailingSlash.ok, false);
        assert.match(trailingSlash.errors.join(' '), /separator|directory/i);

        // Even if legacy data still contains a directory, it no longer locks a
        // file underneath it.
        assert.strictEqual(
          areFileScopesConflicting(
            ['presentation/menu/'],
            ['presentation/menu/subviews/MainMenuView.kt'],
          ),
          false,
        );
      } finally {
        fs.rmSync(workspace, { recursive: true, force: true });
      }
    });

    test('should reject glob patterns because they widen the lock scope', () => {
      const globScope = 'presentation/menu/**';
      const validation = validateExpectedFiles([globScope]);
      assert.strictEqual(validation.ok, false);
      assert.match(validation.errors.join(' '), /glob|wildcard/i);
      assert.strictEqual(
        areFileScopesConflicting([globScope], ['presentation/menu/subviews/MainMenuView.kt']),
        false,
      );
    });

    test('should accept concrete files and only exact paths conflict', () => {
      const validation = validateExpectedFiles([
        'presentation/menu/MenuScreen.kt',
        './presentation/render/TableBoardRenderer.kt',
        'Dockerfile',
      ]);
      assert.strictEqual(validation.ok, true, validation.errors.join(' '));
      assert.strictEqual(validation.files.length, 3);

      assert.strictEqual(
        doPathsConflict('presentation/menu/MenuScreen.kt', 'presentation/menu/MenuScreen.kt'),
        true,
      );
      assert.strictEqual(
        doPathsConflict('presentation/menu', 'presentation/menu/MenuScreen.kt'),
        false,
      );
      assert.strictEqual(
        areFileScopesConflicting(
          ['presentation/menu/MenuScreen.kt'],
          ['presentation/menu/MenuScreen.kt'],
        ),
        true,
      );
    });

    test('should find conflicts against active threads', () => {
      const activeThread: DelegationThread = {
        threadId: 'thread-active-1',
        title: 'Active Task',
        initiatorAgent: 'claude_code',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        receiverSessionId: 'sess-1',
        workspacePath: path.join(os.tmpdir(), 'majiang'),
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
        summary: {
          lastPromptSnippet: 'test',
          lastOutputSnippet: 'test',
          totalDurationMs: 0,
        },
        rounds: [],
      };

      const candidateConflicting: DelegationThread = {
        ...activeThread,
        threadId: 'thread-candidate-2',
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
      };

      const candidateDistinct: DelegationThread = {
        ...activeThread,
        threadId: 'thread-candidate-3',
        expectedFiles: ['presentation/render/TableBoardRenderer.kt'],
      };

      const conflict1 = findFileConflicts(candidateConflicting, [activeThread]);
      assert.strictEqual(conflict1.hasConflict, true);
      assert.strictEqual(conflict1.conflictingThread?.threadId, 'thread-active-1');

      const conflict2 = findFileConflicts(candidateDistinct, [activeThread]);
      assert.strictEqual(conflict2.hasConflict, false);
    });
  });

  describe('AsyncDispatcher and TaskSupervisor Conflict Queueing & 30s Stream Sync', () => {
    let tempDir: string;
    let taskStore: TaskStore;
    let detector: AgentDetector;
    let resolver: ActiveAgentResolver;
    let runner: AutonomousRunner;
    let callbackManager: CallbackManager;
    let supervisor: TaskSupervisor;
    let dispatcher: AsyncDispatcher;

    beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-queue-'));
      taskStore = new TaskStore(tempDir);
      detector = new AgentDetector();
      resolver = new ActiveAgentResolver(detector);
      runner = new AutonomousRunner(detector);

      callbackManager = new CallbackManager(detector);
      callbackManager.notifyCaller = async () => true;
      callbackManager.notifyCallerOfFailure = async () => false;

      // Both components share the same lock file, which is scoped to this
      // test's temp directory instead of the real home directory.
      const gate = new TaskExecutionRegistry(tempDir);
      supervisor = new TaskSupervisor(taskStore, runner, callbackManager, detector, gate);
      // Fast stream sync interval for testing (30ms instead of 30,000ms)
      supervisor.setStreamSyncIntervalMs(30);

      dispatcher = new AsyncDispatcher(
        taskStore,
        resolver,
        runner,
        callbackManager,
        detector,
        gate,
      );
      dispatcher.setStreamSyncIntervalMs(30);
    });

    afterEach(() => {
      supervisor.dispose();
      taskStore.dispose();
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch {}
    });

    test('should reject folder/glob expectedFiles before creating a task', async () => {
      fs.mkdirSync(path.join(tempDir, 'src'), { recursive: true });

      await assert.rejects(
        () =>
          dispatcher.dispatchTask({
            targetAgent: 'antigravity',
            title: 'Folder scope',
            prompt: 'edit the folder',
            workspacePath: tempDir,
            expectedFiles: ['src'],
          }),
        /Invalid expectedFiles|directory/i,
      );

      await assert.rejects(
        () =>
          dispatcher.dispatchTask({
            targetAgent: 'antigravity',
            title: 'Glob scope',
            prompt: 'edit the glob',
            workspacePath: tempDir,
            expectedFiles: ['src/**'],
          }),
        /Invalid expectedFiles|glob|wildcard/i,
      );

      assert.strictEqual(taskStore.count(), 0, 'invalid scopes must not create a ledger entry');
    });

    test('scope conflicts queue only overlapping work while disjoint work runs', async () => {
      let releaseAll: () => void = () => {};
      const held = new Promise<void>((resolve) => { releaseAll = resolve; });
      const startOrder: string[] = [];
      let inFlight = 0;
      let maxInFlight = 0;

      runner.execute = async (options: RunOptions): Promise<RunResult> => {
        startOrder.push(options.title || 'unnamed');
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        try {
          await held;
          return {
            exitCode: 0,
            stdout: `Finished ${options.title}`,
            stderr: '',
            extractedOutput: `Finished ${options.title}`,
            durationMs: 50,
          };
        } finally {
          inFlight--;
        }
      };

      const taskA = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'Task A (MenuScreen)',
        prompt: 'Refactor MenuScreen.kt',
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
      });
      assert.strictEqual(taskA.status, 'DISPATCHED');

      const conflict = await dispatcher.dispatchTask({
        targetAgent: 'codex',
        title: 'Task B (MenuScreen conflict)',
        prompt: 'Change the same file',
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
      });
      assert.strictEqual(conflict.status, 'QUEUED');

      const disjoint = await dispatcher.dispatchTask({
        targetAgent: 'claude_code',
        title: 'Task C (disjoint)',
        prompt: 'Extract TableBoardRenderer.kt',
        expectedFiles: ['presentation/render/TableBoardRenderer.kt'],
      });
      assert.strictEqual(disjoint.status, 'DISPATCHED', disjoint.message);

      const waitForStarts = async (count: number) => {
        const deadline = Date.now() + 2_000;
        while (Date.now() < deadline && startOrder.length < count) await new Promise((r) => setTimeout(r, 10));
        assert.strictEqual(startOrder.length, count);
      };
      await waitForStarts(2);
      assert.deepStrictEqual(startOrder.slice().sort(), ['Task A (MenuScreen)', 'Task C (disjoint)']);
      assert.strictEqual(maxInFlight, 2, 'same-workspace disjoint tasks must overlap in the runner');

      releaseAll();
      const waitForStatus = async (threadId: string, status: string) => {
        const deadline = Date.now() + 3_000;
        while (Date.now() < deadline && taskStore.getTask(threadId)?.status !== status) {
          await new Promise((r) => setTimeout(r, 20));
        }
        assert.strictEqual(taskStore.getTask(threadId)?.status, status);
      };
      await Promise.all([
        waitForStatus(taskA.threadId, 'waiting_review'),
        waitForStatus(disjoint.threadId, 'waiting_review'),
      ]);
      assert.strictEqual(taskStore.getTask(conflict.threadId)?.status, 'queued');
      assert.deepStrictEqual(startOrder.length, 2, 'the waiting-review owner keeps only the overlapping candidate queued');
    });
    test('should sync live output stream periodically (30s stream sync)', async () => {
      let releaseExecution: () => void = () => {};
      let chunkSeen = false;
      const executionHeld = new Promise<void>((resolve) => {
        releaseExecution = resolve;
      });
      runner.execute = async (options: RunOptions): Promise<RunResult> => {
        options.onStdoutChunk?.('Streaming chunk 1: modifying files...\n');
        chunkSeen = true;
        await executionHeld;
        return {
          exitCode: 0,
          stdout: 'Streaming chunk 1\nStreaming chunk 2\nStreaming chunk 3\nTask completed',
          stderr: '',
          extractedOutput: 'Task completed successfully',
          durationMs: 150,
        };
      };

      const res = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'Streaming Test Task',
        prompt: 'Run long task with streaming',
        expectedFiles: ['service/Worker.kt'],
      });

      const waitUntil = async (predicate: () => boolean, description: string, timeoutMs = 2_000) => {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
          if (predicate()) return;
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        assert.ok(predicate(), `timed out waiting for ${description}`);
      };

      try {
        await waitUntil(() => chunkSeen, 'the runner to emit a streaming chunk');
        await waitUntil(() => {
          const round = taskStore.getTask(res.threadId)?.rounds[0];
          return round?.status === 'running' && !!round.output?.includes('Streaming chunk');
        }, 'the live chunk to be synchronized while the round is running');

        const midTask = taskStore.getTask(res.threadId);
        assert.ok(midTask);
        assert.strictEqual(midTask.status, 'running');
        assert.strictEqual(midTask.rounds[0].status, 'running');
        assert.ok(midTask.rounds[0].output?.includes('Streaming chunk'));
      } finally {
        releaseExecution();
        await waitUntil(() => taskStore.getTask(res.threadId)?.status === 'waiting_review', 'round completion').catch(
          () => {},
        );
      }

      const finalTask = taskStore.getTask(res.threadId);
      assert.strictEqual(finalTask?.status, 'waiting_review');
      assert.strictEqual(finalTask?.rounds[0].status, 'completed');
    });

    test('should return recent 1000 lines when log exceeds 1000 lines', async () => {
      let taskResolve: () => void = () => {};
      const taskPromise = new Promise<void>((resolve) => {
        taskResolve = resolve;
      });

      // Prepare 1500 lines of output
      const totalLineCount = 1500;
      const generatedLines: string[] = [];
      for (let i = 1; i <= totalLineCount; i++) {
        generatedLines.push(`[LOG LINE ${i}] processing data chunk ${i}`);
      }
      const rawLogText = generatedLines.join('\n');

      runner.execute = async (options: RunOptions): Promise<RunResult> => {
        if (options.onStdoutChunk) {
          options.onStdoutChunk(rawLogText + '\n');
        }
        await taskPromise;
        return {
          exitCode: 0,
          stdout: rawLogText,
          stderr: '',
          extractedOutput: rawLogText,
          durationMs: 50,
        };
      };

      const res = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'High Volume Log Task',
        prompt: 'Emit 1500 lines',
        expectedFiles: ['service/HeavyLog.kt'],
      });

      // Wait a bit for the runner to start and emit chunks
      await new Promise((r) => setTimeout(r, 50));

      // 1. While task is running, check getRecentLogs
      const runningLogs = supervisor.getRecentLogs(res.threadId, 1000);
      assert.strictEqual(runningLogs.isRunning, true);
      assert.ok(runningLogs.totalLines >= totalLineCount);
      assert.strictEqual(runningLogs.returnedLines, 1000);
      assert.ok(runningLogs.logs.includes('[LOG LINE 1500]'));
      // Since it's the tail 1000 lines, [LOG LINE 1] should not be present
      assert.ok(!runningLogs.logs.includes('[LOG LINE 1] processing data chunk 1'));

      // 2. Complete the task
      taskResolve();
      await new Promise((r) => setTimeout(r, 100));

      // 3. After completion, check getRecentLogs fallback from store / disk
      const completedLogs = TaskSupervisor.getRecentLogs(res.threadId, 500, taskStore);
      assert.strictEqual(completedLogs.isRunning, false);
      assert.strictEqual(completedLogs.returnedLines, 500);
      assert.ok(completedLogs.logs.includes('[LOG LINE 1500]'));
      assert.ok(!completedLogs.logs.includes('[LOG LINE 900]'));
    });
  });
});
