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
import { ExecutionGate } from '../bridge/execution-gate';
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
      const gate = new ExecutionGate(tempDir);
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

    test('should queue whole tasks until the current task is completed (task-level FIFO)', async () => {
      let taskAResolve: () => void = () => {};
      const taskAPromise = new Promise<void>((resolve) => {
        taskAResolve = resolve;
      });

      const startOrder: string[] = [];
      let inFlight = 0;
      let maxInFlight = 0;

      runner.execute = async (options: RunOptions): Promise<RunResult> => {
        startOrder.push(options.title || 'unnamed');
        inFlight++;
        maxInFlight = Math.max(maxInFlight, inFlight);
        try {
          if (options.title?.includes('Task A')) {
            await taskAPromise;
          } else {
            await new Promise((r) => setTimeout(r, 20));
          }
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

      // 1. Dispatch Task A with MenuScreen.kt
      const resA = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'Task A (MenuScreen)',
        prompt: 'Refactor MenuScreen.kt',
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
      });
      assert.strictEqual(resA.status, 'DISPATCHED');

      // 2. Task B conflicts on files AND must wait for A to finish.
      const resB = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'Task B (MenuScreen Conflicting)',
        prompt: 'Add shortcut to MenuScreen.kt',
        expectedFiles: ['presentation/menu/MenuScreen.kt'],
      });
      assert.strictEqual(resB.status, 'QUEUED');
      const threadB = taskStore.getTask(resB.threadId);
      assert.strictEqual(threadB?.status, 'queued');
      assert.match(
        threadB?.summary.lastOutputSnippet || '',
        // Either wording is accurate: the task is queued behind Task A, and the
        // scope gate names the same owner plus the overlapping file.
        /one at a time|Waiting for (conflicting )?task|Queued \(scope\)/,
      );

      // 3. Task C touches disjoint files, but it must STILL be queued: the rule
      // is one task at a time, not merely "no overlapping file edits".
      const resC = await dispatcher.dispatchTask({
        targetAgent: 'antigravity',
        title: 'Task C (TableBoardRenderer Disjoint)',
        prompt: 'Extract TableBoardRenderer.kt',
        expectedFiles: ['presentation/render/TableBoardRenderer.kt'],
      });
      assert.strictEqual(resC.status, 'QUEUED');
      assert.strictEqual(taskStore.getTask(resC.threadId)?.status, 'queued');

      // 4. Release Task A. A goes to waiting_review, but B and C must remain
      // queued until A is actually completed/approved.
      taskAResolve();

      const waitFor = async (threadId: string, status: string, timeoutMs = 3_000) => {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
          if (taskStore.getTask(threadId)?.status === status) return;
          await new Promise((r) => setTimeout(r, 25));
        }
      };

      await waitFor(resA.threadId, 'waiting_review');
      assert.strictEqual(taskStore.getTask(resA.threadId)?.status, 'waiting_review');
      assert.strictEqual(taskStore.getTask(resB.threadId)?.status, 'queued');
      assert.strictEqual(taskStore.getTask(resC.threadId)?.status, 'queued');
      assert.deepStrictEqual(startOrder, ['Task A (MenuScreen)']);

      // 5. Complete A and收口 its MR -> B may start. A's uncommitted edits on
      // MenuScreen.kt keep owning that file until the merge request is recorded,
      // so approval alone must NOT release B onto the same file.
      taskStore.updateRound(resA.threadId, 1, { status: 'completed' }, 'completed');
      supervisor.checkPendingTasks();
      await new Promise((r) => setTimeout(r, 120));
      assert.strictEqual(
        taskStore.getTask(resB.threadId)?.status,
        'queued',
        'an accepted but unmerged task keeps owning its files',
      );

      taskStore.recordMergeRequest(resA.threadId, {
        url: 'https://example.com/mr/1',
        branch: 'feature/menu',
        commit: 'abc1234',
        remote: 'origin',
        status: 'synced',
        summary: 'MenuScreen refactor merged',
        updatedAt: new Date().toISOString(),
        by: 'codex',
      });
      supervisor.checkPendingTasks();
      await waitFor(resB.threadId, 'waiting_review');
      assert.strictEqual(taskStore.getTask(resB.threadId)?.status, 'waiting_review');
      assert.strictEqual(taskStore.getTask(resC.threadId)?.status, 'queued');
      assert.deepStrictEqual(startOrder, [
        'Task A (MenuScreen)',
        'Task B (MenuScreen Conflicting)',
      ]);

      // 6. Complete B -> C may start.
      taskStore.updateRound(resB.threadId, 1, { status: 'completed' }, 'completed');
      supervisor.checkPendingTasks();
      await waitFor(resC.threadId, 'waiting_review');
      assert.strictEqual(taskStore.getTask(resC.threadId)?.status, 'waiting_review');
      assert.deepStrictEqual(startOrder, [
        'Task A (MenuScreen)',
        'Task B (MenuScreen Conflicting)',
        'Task C (TableBoardRenderer Disjoint)',
      ]);
      assert.strictEqual(maxInFlight, 1, 'no two agent processes may run concurrently');
    });

    test('should sync live output stream periodically (30s stream sync)', async () => {
      runner.execute = async (options: RunOptions): Promise<RunResult> => {
        // Emit chunks over time
        for (let i = 1; i <= 3; i++) {
          await new Promise((r) => setTimeout(r, 40));
          if (options.onStdoutChunk) {
            options.onStdoutChunk(`Streaming chunk ${i}: modifying files...\n`);
          }
        }
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

      // Wait enough for the 30ms stream sync to trigger at least once while executing
      await new Promise((r) => setTimeout(r, 90));

      const midTask = taskStore.getTask(res.threadId);
      assert.ok(midTask);
      const midRound = midTask.rounds[0];
      // Live output should have synced partial streamed text before final completion
      assert.ok(midRound.output && midRound.output.includes('Streaming chunk'));

      // Wait for complete
      await new Promise((r) => setTimeout(r, 120));
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

