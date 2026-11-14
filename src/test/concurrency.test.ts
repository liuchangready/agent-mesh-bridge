import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-concurrency-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;

function initGitRepo(root: string): void {
  const run = (...args: string[]) => {
    const result = spawnSync(gitExe, args, { cwd: root, encoding: 'utf8', windowsHide: true });
    if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  };
  run('init', '-b', 'test/source');
  run('config', 'user.name', 'Mesh Test');
  run('config', 'user.email', 'mesh-test@example.test');
  run('commit', '--allow-empty', '-m', 'base');
}

import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import {
  DEFAULT_CONCURRENCY_POLICY,
  concurrencyPolicy,
  readBridgePolicy,
  saveBridgePolicy,
} from '../bridge/bridge-policy';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { ReworkController } from '../bridge/rework-controller';
import { QueueDrainer } from '../bridge/queue-drainer';
import { DelegationThread, pendingMerges } from '../bridge/types';

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: 'thread_merge_1',
    title: 'Merge gate probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'completed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'receiver-session-0001',
    workspace: testWorkspace(os.tmpdir()),
    summary: { lastPromptSnippet: 'work', lastOutputSnippet: 'done', totalDurationMs: 1 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', output: 'done', status: 'completed', durationMs: 1, timestamp: now }],
    ...overrides,
  };
}

const temporaryDirs: string[] = [];
function trackedTempDir(prefix: string): string {
  const dir = makeTempDir(prefix);
  temporaryDirs.push(dir);
  return dir;
}
afterEach(() => {
  while (temporaryDirs.length) fs.rmSync(temporaryDirs.pop()!, { recursive: true, force: true });
});

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(predicate(), 'condition did not become true before timeout');
}

function success(title?: string): RunResult {
  return { exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 };
}

describe('bridge policy - optional best-effort concurrency limits', () => {
  let root: string;
  beforeEach(() => { root = trackedTempDir('agent-bridge-policy-'); });

  test('disabled means no configured cap; getter shape and historical enabled semantics stay stable', () => {
    assert.deepStrictEqual(concurrencyPolicy(root), DEFAULT_CONCURRENCY_POLICY);
    assert.strictEqual(concurrencyPolicy(root).enabled, false);
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 });
    saveBridgePolicy({ concurrency: { maxConcurrentTasks: 4 } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 4, maxConcurrentPerAgent: 2 });
    assert.strictEqual(readBridgePolicy(root).concurrency?.enabled, true);
  });

  test('invalid limits preserve prior values and only explicit true enables them', () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } }, root);
    saveBridgePolicy({ concurrency: { maxConcurrentTasks: -5, maxConcurrentPerAgent: Number.NaN } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 });
    saveBridgePolicy({ concurrency: { enabled: false } }, root);
    assert.strictEqual(concurrencyPolicy(root).enabled, false);
  });
});

describe('AsyncDispatcher - independent task admission', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let registry: TaskExecutionRegistry;
  let dispatcher: AsyncDispatcher;
  let workspace: string;
  let release: () => void;
  let started: string[];
  let active: number;
  let maxActive: number;

  beforeEach(() => {
    tempDir = trackedTempDir('agent-bridge-dispatch-concurrency-');
    workspace = path.join(tempDir, 'workspace');
    fs.mkdirSync(workspace, { recursive: true });
    initGitRepo(workspace);
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    registry = new TaskExecutionRegistry(tempDir);
    runner = new AutonomousRunner(detector);
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    let releaseAll: () => void = () => {};
    const held = new Promise<void>((resolve) => { releaseAll = resolve; });
    release = releaseAll;
    started = [];
    active = 0;
    maxActive = 0;
    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      started.push(options.title || 'unnamed');
      active++;
      maxActive = Math.max(maxActive, active);
      try { await held; return success(options.title); }
      finally { active--; }
    };
    dispatcher = new AsyncDispatcher(taskStore, new ActiveAgentResolver(detector), runner, callbackManager, detector, registry);
  });

  afterEach(async () => {
    release();
    await new Promise((resolve) => setTimeout(resolve, 150));
    (dispatcher as any).queueDrainer?.dispose?.();
    registry.dispose();
    taskStore.dispose();
  });

  test('same-file tasks in one repository run concurrently in isolated worktrees', async () => {
    saveBridgePolicy({ concurrency: { enabled: false } });
    const first = await dispatcher.dispatchTask({ targetAgent: 'codex', title: 'A', prompt: 'edit a', workspaceRoot: workspace });
    assert.strictEqual(first.status, 'DISPATCHED');
    await waitFor(() => started.length === 1);

    const conflict = await dispatcher.dispatchTask({ targetAgent: 'antigravity', title: 'B same file', prompt: 'edit a too', workspaceRoot: workspace });
    assert.strictEqual(conflict.status, 'DISPATCHED');
    await waitFor(() => started.length === 2);

    const disjoint = await dispatcher.dispatchTask({ targetAgent: 'claude_code', title: 'C disjoint', prompt: 'edit c', workspaceRoot: workspace });
    assert.strictEqual(disjoint.status, 'DISPATCHED', disjoint.message);
    await waitFor(() => started.length === 3);
    assert.strictEqual(maxActive, 3, 'the runner promises overlap in wall time, not merely in dispatcher return statuses');
    assert.deepStrictEqual(started.slice().sort(), ['A', 'B same file', 'C disjoint']);
    assert.notStrictEqual(taskStore.getTask(first.threadId)?.worktree?.path, taskStore.getTask(conflict.threadId)?.worktree?.path);
  });

  test('live stdout chunks are persisted while a round is still running', async () => {
    dispatcher.setStreamSyncIntervalMs(30);
    let releaseTask: () => void = () => {};
    const held = new Promise<void>((resolve) => { releaseTask = resolve; });
    let chunkSeen = false;
    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      options.onStdoutChunk?.('Streaming chunk 1: editing files...\n');
      chunkSeen = true;
      await held;
      return { exitCode: 0, stdout: 'Streaming chunk 1\nStreaming chunk 2\nTask completed', stderr: '', extractedOutput: 'Task completed successfully', durationMs: 150 };
    };
    const task = await dispatcher.dispatchTask({ targetAgent: 'antigravity', title: 'Streaming', prompt: 'stream output', workspaceRoot: workspace });
    try {
      await waitFor(() => chunkSeen);
      await waitFor(() => !!taskStore.getTask(task.threadId)?.rounds[0].output?.includes('Streaming chunk'));
      assert.strictEqual(taskStore.getTask(task.threadId)?.status, 'running');
    } finally {
      releaseTask();
      await waitFor(() => taskStore.getTask(task.threadId)?.status === 'waiting_review').catch(() => {});
    }
    assert.strictEqual(taskStore.getTask(task.threadId)?.rounds[0].status, 'completed');
  });

  test('recent execution logs return the requested tail while running and after completion', async () => {
    const detector = new AgentDetector();
    const supervisor = new TaskSupervisor(taskStore, runner, callbackManager, detector, registry);
    supervisor.setStreamSyncIntervalMs(30);
    let releaseTask: () => void = () => {};
    const held = new Promise<void>((resolve) => { releaseTask = resolve; });
    let logChunkSeen = false;
    const totalLineCount = 1_500;
    const rawLogText = Array.from({ length: totalLineCount }, (_, index) => `[LOG LINE ${index + 1}] processing data chunk ${index + 1}`).join('\n');
    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      options.onStdoutChunk?.(`${rawLogText}\n`);
      logChunkSeen = true;
      await held;
      return { exitCode: 0, stdout: rawLogText, stderr: '', extractedOutput: rawLogText, durationMs: 50 };
    };
    const task = await dispatcher.dispatchTask({ targetAgent: 'antigravity', title: 'High volume log', prompt: 'emit 1500 lines', workspaceRoot: workspace });
    supervisor.checkPendingTasks();
    try {
      await waitFor(() => logChunkSeen);
      const running = supervisor.getRecentLogs(task.threadId, 1_000);
      assert.strictEqual(running.isRunning, true);
      assert.ok(running.totalLines >= totalLineCount);
      assert.strictEqual(running.returnedLines, 1_000);
      assert.ok(running.logs.includes('[LOG LINE 1500]'));
      assert.ok(!running.logs.includes('[LOG LINE 1] processing data chunk 1'));
    } finally {
      releaseTask();
      await waitFor(() => taskStore.getTask(task.threadId)?.status === 'waiting_review');
      supervisor.dispose();
    }
    const completed = TaskSupervisor.getRecentLogs(task.threadId, 500, taskStore);
    assert.strictEqual(completed.isRunning, false);
    assert.strictEqual(completed.returnedLines, 500);
    assert.ok(completed.logs.includes('[LOG LINE 1500]'));
    assert.ok(!completed.logs.includes('[LOG LINE 900]'));
  });

  test('an old execution.lock record does not block a new independent task', async () => {
    const legacy = { token: 'legacy', pid: process.pid, threadId: 'old-thread', round: 1, acquiredAt: Date.now(), updatedAt: Date.now() };
    fs.writeFileSync(path.join(tempDir, 'execution.lock'), JSON.stringify(legacy));
    saveBridgePolicy({ concurrency: { enabled: false } });
    const task = await dispatcher.dispatchTask({ targetAgent: 'codex', title: 'Independent', prompt: 'edit', workspaceRoot: workspace });
    assert.strictEqual(task.status, 'DISPATCHED');
    await waitFor(() => started.length === 1);
    assert.strictEqual(taskStore.getTask(task.threadId)?.status, 'running');
    assert.ok(registry.getOwner(task.threadId));
  });

  test('enabled limits queue only after observed registrations reach the cap', async () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 1, maxConcurrentPerAgent: 2 } });
    const first = await dispatcher.dispatchTask({ targetAgent: 'codex', title: 'Limited A', prompt: 'a', workspaceRoot: workspace });
    assert.strictEqual(first.status, 'DISPATCHED');
    await waitFor(() => started.length === 1);
    const second = await dispatcher.dispatchTask({ targetAgent: 'antigravity', title: 'Limited B', prompt: 'b', workspaceRoot: workspace });
    assert.strictEqual(second.status, 'QUEUED');
    assert.match(second.message, /limit reached/i);
  });

  test('a per-agent cap skips only that agent and still starts a different agent candidate', async () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 1 } });
    const first = await dispatcher.dispatchTask({ targetAgent: 'codex', title: 'Codex active', prompt: 'a', workspaceRoot: workspace });
    assert.strictEqual(first.status, 'DISPATCHED');
    await waitFor(() => started.length === 1);

    const sameAgent = await dispatcher.dispatchTask({ targetAgent: 'codex', title: 'Codex queued', prompt: 'b', workspaceRoot: workspace });
    assert.strictEqual(sameAgent.status, 'QUEUED');
    assert.match(sameAgent.message, /agent codex/i);

    const otherAgent = await dispatcher.dispatchTask({ targetAgent: 'antigravity', title: 'Other agent', prompt: 'c', workspaceRoot: workspace });
    assert.strictEqual(otherAgent.status, 'DISPATCHED', otherAgent.message);
    await waitFor(() => started.length === 2);
    assert.deepStrictEqual(started.slice().sort(), ['Codex active', 'Other agent']);
    assert.strictEqual(maxActive, 2);
  });

  test('delayed queue marking cannot resurrect deleted or overwrite paused/completed tasks', () => {
    saveBridgePolicy({ concurrency: { enabled: false } });
    const supervisor = new TaskSupervisor(taskStore, runner, callbackManager, undefined, registry);
    const rework = new ReworkController(taskStore, runner, callbackManager, undefined, registry);
    const markers: Array<(stale: DelegationThread) => void> = [
      (stale) => (dispatcher as any).markThreadQueued(stale, undefined, 'stale cap result'),
      (stale) => (supervisor as any).markQueued(stale, undefined, 'stale scheduler result'),
      (stale) => (rework as any).markQueued(stale, undefined, 'stale scheduler result'),
    ];

    try {
      for (const [markerIndex, markQueued] of markers.entries()) {
        for (const terminalState of ['paused', 'completed', 'deleted'] as const) {
          const threadId = `thread_delayed_queue_${markerIndex}_${terminalState}`;
          const stale = makePendingThread(threadId, workspace, new Date().toISOString());
          taskStore.upsertTask(stale);
          if (terminalState === 'deleted') taskStore.deleteTask(threadId);
          else {
            const latest = taskStore.getTask(threadId)!;
            latest.status = terminalState;
            taskStore.upsertTask(latest);
          }

          markQueued(stale);
          const latest = taskStore.getTask(threadId);
          if (terminalState === 'deleted') assert.strictEqual(latest, undefined, `${threadId} must stay deleted`);
          else assert.strictEqual(latest?.status, terminalState, `${threadId} must retain its newer lifecycle state`);
        }
      }
    } finally {
      (rework as any).queueDrainer?.dispose?.();
      supervisor.dispose();
    }
  });
});

function makePendingThread(threadId: string, workspacePath: string, createdAt: string): DelegationThread {
  return makeThread({
    threadId, title: threadId, status: 'running', workspace: testWorkspace(workspacePath), createdAt, updatedAt: createdAt,
    currentRound: 1,
    rounds: [{ round: 1, type: 'initial', prompt: 'work ' + threadId, status: 'running', durationMs: 0, timestamp: createdAt }],
  });
}

describe('QueueDrainer - scan every runnable candidate', () => {
  test('paused, waiting review, blocked and bootstrap-pending tasks do not hide later candidates', async () => {
    const dir = trackedTempDir('agent-bridge-drainer-');
    const store = new TaskStore(dir);
    const registry = new TaskExecutionRegistry(dir);
    const workspace = path.join(dir, 'ws');
    const old = '2026-11-01T00:00:00.000Z';
    store.upsertTask(makePendingThread('paused', workspace, old) as any);
    store.upsertTask(makePendingThread('review', workspace, old) as any);
    store.upsertTask(makePendingThread('blocked', workspace, old) as any);
    store.upsertTask(makePendingThread('bootstrap', workspace, old) as any);
    store.upsertTask(makePendingThread('ready', workspace, old) as any);
    const paused = store.getTask('paused')!; paused.status = 'paused'; store.upsertTask(paused);
    const review = store.getTask('review')!; review.status = 'waiting_review'; store.upsertTask(review);
    const blocked = store.getTask('blocked')!; blocked.status = 'waiting_review'; blocked.reviewBlocked = { reason: 'dependency', at: old } as any; store.upsertTask(blocked);
    const bootstrap = store.getTask('bootstrap')!; bootstrap.reviewFlow = 'dedicated'; bootstrap.reviewBootstrap = { phase: 'pending', attempts: 0 }; store.upsertTask(bootstrap);

    const started: string[] = [];
    const drainer = new QueueDrainer(store, registry, () => false, async (target) => {
      started.push(target.thread.threadId);
      registry.begin(target.thread.threadId, target.round.round, target.thread.targetAgent, target.thread.workspace.executionPath);
    });
    await drainer.drain();
    assert.deepStrictEqual(started, ['ready']);
    drainer.dispose(); registry.dispose(); store.dispose();
  });

  test('disabled limits allow all disjoint same-workspace candidates; enabled limits are best-effort admission', async () => {
    const dir = trackedTempDir('agent-bridge-drainer-cap-');
    const store = new TaskStore(dir);
    const registry = new TaskExecutionRegistry(dir);
    const ws = path.join(dir, 'ws');
    const when = '2026-11-01T00:00:00.000Z';
    store.upsertTask(makePendingThread('a', ws, when));
    store.upsertTask(makePendingThread('b', ws, when));
    saveBridgePolicy({ concurrency: { enabled: false } });
    const started: string[] = [];
    const drainer = new QueueDrainer(store, registry, () => false, async (target) => {
      started.push(target.thread.threadId);
      registry.begin(target.thread.threadId, target.round.round, target.thread.targetAgent, ws);
    });
    await drainer.drain();
    assert.deepStrictEqual(started, ['a', 'b']);
    drainer.dispose(); registry.dispose(); store.dispose();
  });
});

describe('TaskSupervisor - same-workspace overlap and same-thread exclusion', () => {
  test('two disjoint same-workspace tasks run at once; duplicate scheduling starts one thread once', async () => {
    saveBridgePolicy({ concurrency: { enabled: false } });
    const dir = trackedTempDir('agent-bridge-supervisor-overlap-');
    const store = new TaskStore(dir);
    const registry = new TaskExecutionRegistry(dir);
    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    let release: () => void = () => {};
    const barrier = new Promise<void>((resolve) => { release = resolve; });
    const starts: string[] = [];
    runner.execute = async (options: RunOptions) => { starts.push(options.taskThreadId || ''); await barrier; return success(options.title); };
    const callback = new CallbackManager(detector);
    callback.notifyCaller = async () => true;
    callback.notifyCallerOfFailure = async () => false;
    const supervisor = new TaskSupervisor(store, runner, callback, detector, registry);
    const ws = path.join(dir, 'ws');
    const when = new Date().toISOString();
    const a = makePendingThread('thread_a', ws, when);
    const b = makePendingThread('thread_b', ws, when);
    store.upsertTask(a); store.upsertTask(b);
    supervisor.checkPendingTasks();
    await waitFor(() => starts.length === 2);
    assert.deepStrictEqual(starts.slice().sort(), ['thread_a', 'thread_b']);
    assert.strictEqual(registry.listOwners().length, 2);

    const same = makePendingThread('thread_same', ws, when);
    store.upsertTask(same);
    const sameRound = same.rounds[0];
    const sameRuns = [
      supervisor.executeThreadRound(same, sameRound),
      supervisor.executeThreadRound(same, sameRound),
    ];
    await waitFor(() => starts.filter((id) => id === 'thread_same').length === 1);
    assert.strictEqual(starts.filter((id) => id === 'thread_same').length, 1, 'role/round admission rejects duplicate schedule');
    assert.strictEqual(store.getTask('thread_same')?.status, 'running', 'losing admission must not rewrite it as queued');

    release();
    await Promise.all(sameRuns);
    await new Promise((resolve) => setTimeout(resolve, 100));
    supervisor.dispose(); registry.dispose(); store.dispose();
  });
});

describe('merge gate - pendingMerges', () => {
  test('local integration state controls pending results, regardless of MR metadata', () => {
    const older = makeThread({ threadId: 'thread_old', createdAt: '2026-10-01T00:00:00.000Z', worktree: { path: 'D:/wt/old', mainPath: 'D:/repo', integration: { phase: 'ready', updatedAt: '2026-10-01T00:00:00.000Z' } } });
    const newer = makeThread({ threadId: 'thread_new', createdAt: '2026-10-02T00:00:00.000Z', worktree: { path: 'D:/wt/new', mainPath: 'D:/repo', integration: { phase: 'ready', updatedAt: '2026-10-02T00:00:00.000Z' } }, mergeRequest: { url: 'https://example.com/mr/1', status: 'synced', updatedAt: '2026-10-02T00:00:00.000Z' } });
    const merged = makeThread({ threadId: 'thread_merged', createdAt: '2026-10-03T00:00:00.000Z', worktree: { path: 'D:/wt/merged', mainPath: 'D:/repo', mergePending: true, integration: { phase: 'merged', updatedAt: '2026-10-03T00:00:00.000Z' } } });
    const noWorktree = makeThread({ threadId: 'thread_plain', createdAt: '2026-10-04T00:00:00.000Z' });
    const stillRunning = makeThread({ threadId: 'thread_running', createdAt: '2026-10-05T00:00:00.000Z', status: 'running', worktree: { path: 'D:/wt/running', mainPath: 'D:/repo' } });
    assert.deepStrictEqual(pendingMerges([newer, noWorktree, merged, older, stillRunning]).map((task) => task.threadId), ['thread_old', 'thread_new']);
  });
});
