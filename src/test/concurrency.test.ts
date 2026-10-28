import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// The mirrored policy and the supervisor heartbeat both live under the home
// directory. Node's test runner executes each test FILE in its own process, so
// redirecting the home here keeps this file's "concurrency is enabled" policy
// from leaking into the other, concurrently running test files.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-concurrency-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { ExecutionGate, normalizeWorkspaceKey } from '../bridge/execution-gate';
import {
  DEFAULT_CONCURRENCY_POLICY,
  concurrencyPolicy,
  readBridgePolicy,
  saveBridgePolicy,
} from '../bridge/bridge-policy';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { TaskControlService } from '../bridge/task-control';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { QueueDrainer } from '../bridge/queue-drainer';
import { DelegationThread, pendingMerges } from '../bridge/types';

/** Minimal valid thread; tests override only what they exercise. */
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
    workspacePath: os.tmpdir(),
    summary: { lastPromptSnippet: 'work', lastOutputSnippet: 'done', totalDurationMs: 1 },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'work',
        output: 'done',
        status: 'completed',
        durationMs: 1,
        timestamp: now,
      },
    ],
    ...overrides,
  };
}

const temporaryDirs: string[] = [];

function makeTempDir(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  temporaryDirs.push(dir);
  return dir;
}

afterEach(() => {
  while (temporaryDirs.length > 0) {
    const dir = temporaryDirs.pop()!;
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {}
  }
});

describe('ExecutionGate - one lease per workspace', () => {
  test('two different workspaces may execute at the same time', () => {
    const storageDir = makeTempDir('agent-bridge-lease-');
    const gate = new ExecutionGate(storageDir);
    const workspaceA = path.join(storageDir, 'workspace-a');
    const workspaceB = path.join(storageDir, 'workspace-b');

    const leaseA = gate.tryAcquire('thread_a', 1, workspaceA, 'codex');
    const leaseB = gate.tryAcquire('thread_b', 1, workspaceB, 'antigravity');

    assert.ok(leaseA, 'workspace A must get a lease');
    assert.ok(leaseB, 'workspace B must get its own lease');
    assert.notStrictEqual(
      leaseA!.lockPath,
      leaseB!.lockPath,
      'each workspace must own a distinct lock file',
    );
    assert.match(leaseA!.lockPath.replace(/\\/g, '/'), /\/leases\/[0-9a-f]{16}\.lock$/);
    assert.strictEqual(gate.countActive(), 2, 'two live leases');

    const owners = gate.listOwners();
    assert.strictEqual(owners.length, 2);
    assert.deepStrictEqual(
      owners.map((owner) => owner.workspace).sort(),
      [normalizeWorkspaceKey(workspaceA), normalizeWorkspaceKey(workspaceB)].sort(),
    );
    assert.deepStrictEqual(
      owners.map((owner) => owner.agent).sort(),
      ['antigravity', 'codex'],
    );

    // A second lease for the SAME workspace is refused, whoever asks.
    const sameWorkspace = new ExecutionGate(storageDir);
    assert.strictEqual(
      sameWorkspace.tryAcquire('thread_c', 1, workspaceA),
      null,
      'one task per workspace',
    );

    // Releasing one lease leaves the other one held.
    gate.release(leaseA!);
    assert.strictEqual(gate.isBusy(normalizeWorkspaceKey(workspaceA)), false);
    assert.strictEqual(gate.isBusy(normalizeWorkspaceKey(workspaceB)), true);
    assert.strictEqual(gate.countActive(), 1);
    assert.strictEqual(gate.listOwners()[0].threadId, 'thread_b');

    gate.release(leaseB!);
    assert.strictEqual(gate.countActive(), 0);
  });

  test('without a workspace key the mesh-wide single flight is unchanged', () => {
    const storageDir = makeTempDir('agent-bridge-lease-legacy-');
    const first = new ExecutionGate(storageDir);
    const second = new ExecutionGate(storageDir);

    const lease = first.tryAcquire('thread_legacy', 1);
    assert.ok(lease);
    assert.strictEqual(lease!.lockPath, first.getLockFilePath(), 'legacy lock file');
    assert.strictEqual(lease!.workspace, undefined);

    assert.strictEqual(
      second.tryAcquire('thread_legacy_2', 1),
      null,
      'a second workspace-less acquire is refused exactly as before',
    );
    assert.strictEqual(second.isBusy(), true);
    assert.strictEqual(second.getOwner()?.threadId, 'thread_legacy');
    assert.strictEqual(first.countActive(), 1);

    first.release(lease!);
    assert.strictEqual(second.isBusy(), false);
    assert.strictEqual(second.countActive(), 0);
    assert.ok(second.tryAcquire('thread_legacy_2', 1), 'the slot is free again');
    second.release();
    assert.strictEqual(second.countActive(), 0, 'release() without a lease clears this process');
  });

  test('attachChildPid targets the given lease and the orphan scan covers every lease', () => {
    const storageDir = makeTempDir('agent-bridge-lease-child-');
    const gate = new ExecutionGate(storageDir, { staleAfterMs: 60_000 });
    const leaseA = gate.tryAcquire('thread_a', 1, path.join(storageDir, 'ws-a'));
    const leaseB = gate.tryAcquire('thread_b', 1, path.join(storageDir, 'ws-b'));
    assert.ok(leaseA && leaseB);

    // Ambiguous without a lease (two are held), explicit with one.
    gate.attachChildPid(4242);
    assert.strictEqual(gate.getRawOwner(leaseA!.workspace)?.childPid, undefined);
    gate.attachChildPid(4242, leaseA!);
    assert.strictEqual(gate.getRawOwner(leaseA!.workspace)?.childPid, 4242);
    assert.strictEqual(gate.getRawOwner(leaseB!.workspace)?.childPid, undefined);

    gate.release(leaseA!);
    gate.release(leaseB!);
  });
});

describe('bridge policy - concurrency is opt-in', () => {
  let policyRoot: string;

  beforeEach(() => {
    policyRoot = makeTempDir('agent-bridge-policy-');
  });

  test('defaults to disabled with 2 mesh slots and 1 slot per agent', () => {
    assert.deepStrictEqual(concurrencyPolicy(policyRoot), {
      enabled: false,
      maxConcurrentTasks: 2,
      maxConcurrentPerAgent: 1,
    });
    assert.deepStrictEqual(concurrencyPolicy(policyRoot), DEFAULT_CONCURRENCY_POLICY);
  });

  test('persists and reads the three fields back', () => {
    saveBridgePolicy(
      { concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } },
      policyRoot,
    );
    assert.deepStrictEqual(concurrencyPolicy(policyRoot), {
      enabled: true,
      maxConcurrentTasks: 3,
      maxConcurrentPerAgent: 2,
    });
    const raw = readBridgePolicy(policyRoot);
    assert.strictEqual(raw.concurrency?.enabled, true);

    // A partial patch keeps the untouched fields.
    saveBridgePolicy({ concurrency: { maxConcurrentTasks: 4 } }, policyRoot);
    assert.deepStrictEqual(concurrencyPolicy(policyRoot), {
      enabled: true,
      maxConcurrentTasks: 4,
      maxConcurrentPerAgent: 2,
    });
  });

  test('negative and non-finite slot counts are ignored, zero clamps to one', () => {
    saveBridgePolicy(
      { concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } },
      policyRoot,
    );

    saveBridgePolicy(
      { concurrency: { maxConcurrentTasks: -5, maxConcurrentPerAgent: Number.NaN } },
      policyRoot,
    );
    assert.deepStrictEqual(concurrencyPolicy(policyRoot), {
      enabled: true,
      maxConcurrentTasks: 3,
      maxConcurrentPerAgent: 2,
    });

    saveBridgePolicy({ concurrency: { maxConcurrentPerAgent: 0 } }, policyRoot);
    assert.strictEqual(
      concurrencyPolicy(policyRoot).maxConcurrentPerAgent,
      1,
      'a slot count can never be zero',
    );
  });

  test('only an explicit true enables concurrency', () => {
    saveBridgePolicy({ concurrency: { enabled: false } }, policyRoot);
    assert.strictEqual(concurrencyPolicy(policyRoot).enabled, false);
    saveBridgePolicy({ concurrency: {} }, policyRoot);
    assert.strictEqual(concurrencyPolicy(policyRoot).enabled, false);
  });
});

describe('AsyncDispatcher - workspace slots', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let gate: ExecutionGate;
  let dispatcher: AsyncDispatcher;
  let workspaceA: string;
  let workspaceB: string;
  let releaseRounds: () => void;

  beforeEach(() => {
    tempDir = makeTempDir('agent-bridge-dispatch-concurrency-');
    workspaceA = path.join(tempDir, 'workspace-a');
    workspaceB = path.join(tempDir, 'workspace-b');
    fs.mkdirSync(workspaceA, { recursive: true });
    fs.mkdirSync(workspaceB, { recursive: true });

    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    gate = new ExecutionGate(tempDir);
    runner = new AutonomousRunner(detector);
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;

    // Every round waits for the test: the assertions are about which tasks the
    // dispatcher starts, not about what the agents do.
    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    releaseRounds = release;
    runner.execute = async (): Promise<RunResult> => {
      await held;
      return {
        exitCode: 0,
        stdout: 'done',
        stderr: '',
        extractedOutput: 'done',
        durationMs: 1,
      };
    };

    dispatcher = new AsyncDispatcher(
      taskStore,
      new ActiveAgentResolver(detector),
      runner,
      callbackManager,
      detector,
      gate,
    );
  });

  afterEach(async () => {
    // Let the held rounds finish so their timers/intervals cannot outlive the
    // test, then drop the storage directory.
    releaseRounds();
    await new Promise((resolve) => setTimeout(resolve, 150));
    taskStore.dispose();
  });

  test('concurrency disabled: a second task is queued even in another workspace', async () => {
    saveBridgePolicy({ concurrency: { enabled: false } });

    const first = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Workspace A task',
      prompt: 'edit a',
      workspacePath: workspaceA,
      expectedFiles: ['src/a.ts'],
    });
    assert.strictEqual(first.status, 'DISPATCHED');

    const second = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Workspace B task',
      prompt: 'edit b',
      workspacePath: workspaceB,
      expectedFiles: ['src/b.ts'],
    });
    assert.strictEqual(second.status, 'QUEUED', 'single flight is mesh-wide, not per workspace');
    assert.strictEqual(taskStore.getTask(second.threadId)?.status, 'queued');
    assert.strictEqual(gate.countActive(), 1, 'only one lease may exist');
    assert.match(second.message, /strictly one at a time/);
  });

  test('concurrency enabled: different workspaces dispatch together, same agent still caps', async () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 2 } });

    const first = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Workspace A task',
      prompt: 'edit a',
      workspacePath: workspaceA,
      expectedFiles: ['src/a.ts'],
    });
    assert.strictEqual(first.status, 'DISPATCHED');
    assert.strictEqual(gate.countActive(), 1);

    const second = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Workspace B task',
      prompt: 'edit b',
      workspacePath: workspaceB,
      expectedFiles: ['src/b.ts'],
    });
    assert.strictEqual(second.status, 'DISPATCHED', second.message);
    assert.strictEqual(gate.countActive(), 2);
    assert.strictEqual(taskStore.getTask(first.threadId)?.status, 'running');
    assert.strictEqual(taskStore.getTask(second.threadId)?.status, 'running');

    // A third workspace is free, but `codex` already occupies its only slot.
    const workspaceC = path.join(tempDir, 'workspace-c');
    fs.mkdirSync(workspaceC, { recursive: true });
    const third = await dispatcher.dispatchTask({
      targetAgent: 'codex',
      title: 'Workspace C task',
      prompt: 'edit c',
      workspacePath: workspaceC,
      expectedFiles: ['src/c.ts'],
    });
    assert.strictEqual(third.status, 'QUEUED');
    assert.strictEqual(taskStore.getTask(third.threadId)?.status, 'queued');
    assert.match(third.message, /agent codex/i);
    assert.match(third.message, /concurrent slots/i);
  });
});

describe('merge gate - pendingMerges', () => {
  test('only completed worktree tasks without a merge request, oldest first', () => {
    const older = makeThread({
      threadId: 'thread_old',
      createdAt: '2026-10-01T00:00:00.000Z',
      worktree: { path: 'D:/wt/old', mainPath: 'D:/repo' },
    });
    const newer = makeThread({
      threadId: 'thread_new',
      createdAt: '2026-10-02T00:00:00.000Z',
      worktree: { path: 'D:/wt/new', mainPath: 'D:/repo' },
    });
    const merged = makeThread({
      threadId: 'thread_merged',
      createdAt: '2026-10-03T00:00:00.000Z',
      worktree: { path: 'D:/wt/merged', mainPath: 'D:/repo', mergePending: true },
      mergeRequest: {
        url: 'https://example.com/mr/1',
        status: 'synced',
        updatedAt: '2026-10-03T00:00:00.000Z',
      },
    });
    const noWorktree = makeThread({
      threadId: 'thread_plain',
      createdAt: '2026-10-04T00:00:00.000Z',
    });
    const stillRunning = makeThread({
      threadId: 'thread_running',
      createdAt: '2026-10-05T00:00:00.000Z',
      status: 'running',
      worktree: { path: 'D:/wt/running', mainPath: 'D:/repo' },
    });

    const pending = pendingMerges([newer, noWorktree, merged, older, stillRunning]);
    assert.deepStrictEqual(
      pending.map((task) => task.threadId),
      ['thread_old', 'thread_new'],
      'a merge request, a missing worktree and a non-completed task are all excluded',
    );
  });
});

describe('merge gate - delete refuses an unmerged worktree', () => {
  let tempDir: string;
  let taskStore: TaskStore;

  beforeEach(() => {
    tempDir = makeTempDir('agent-bridge-merge-gate-');
    taskStore = new TaskStore(tempDir);
  });

  afterEach(() => {
    taskStore.dispose();
  });

  const control = () =>
    new TaskControlService(taskStore, undefined, new CancellationRegistry(tempDir), tempDir);

  const worktreeThread = () =>
    makeThread({
      threadId: 'thread_worktree',
      workspacePath: tempDir,
      worktree: { path: path.join(tempDir, 'wt'), mainPath: tempDir, branch: 'feat/x' },
    });

  test('completed worktree task raises mergePending and is refused until forced', () => {
    taskStore.upsertTask(worktreeThread());
    const stored = taskStore.getTask('thread_worktree')!;
    assert.strictEqual(stored.worktree?.mergePending, true, 'completion raises the merge gate');
    assert.strictEqual(pendingMerges(taskStore.getAllTasks()).length, 1);

    assert.throws(
      () => control().deleteTask('thread_worktree', 'cleanup', 'user'),
      /worktree/i,
      'an unmerged worktree must not be deleted silently',
    );
    assert.ok(taskStore.getTask('thread_worktree'), 'the refused deletion keeps the task');
    assert.strictEqual(
      fs.existsSync(path.join(tempDir, 'deleted_tasks.log')),
      false,
      'a refused deletion writes no tombstone',
    );

    const forced = control().deleteTask('thread_worktree', 'discard', 'user', true);
    assert.strictEqual(forced.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_worktree'), undefined);
    const tombstone = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8');
    assert.match(tombstone, /thread_worktree/);
  });

  test('recording the merge request clears the gate and allows a normal delete', () => {
    taskStore.upsertTask(worktreeThread());
    assert.strictEqual(pendingMerges(taskStore.getAllTasks()).length, 1);

    taskStore.recordMergeRequest('thread_worktree', {
      url: 'https://example.com/mr/7',
      branch: 'feat/x',
      commit: 'abcdef1',
      remote: 'origin',
      status: 'synced',
      updatedAt: new Date().toISOString(),
      by: 'codex',
    });

    const afterMerge = taskStore.getTask('thread_worktree')!;
    assert.strictEqual(afterMerge.worktree?.mergePending, false);
    assert.strictEqual(pendingMerges(taskStore.getAllTasks()).length, 0);

    const result = control().deleteTask('thread_worktree', 'merged and closed', 'user');
    assert.strictEqual(result.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_worktree'), undefined);
  });

  test('a normal (non-worktree) task is still deleted without force', () => {
    taskStore.upsertTask(makeThread({ threadId: 'thread_plain', workspacePath: tempDir }));
    const result = control().deleteTask('thread_plain', 'duplicate', 'user');
    assert.strictEqual(result.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_plain'), undefined);
  });
});

/** A queued/running task with one round in flight, created `ageMs` ago. */
function makePendingThread(
  threadId: string,
  title: string,
  workspacePath: string,
  ageMs: number,
  status: 'queued' | 'running' = 'queued',
): DelegationThread {
  const createdAt = new Date(Date.now() - ageMs).toISOString();
  return makeThread({
    threadId,
    title,
    status,
    workspacePath,
    createdAt,
    updatedAt: createdAt,
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: `work for ${threadId}`,
        status: 'running',
        durationMs: 0,
        timestamp: createdAt,
      },
    ],
  });
}

describe('QueueDrainer - one queue per workspace', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let gate: ExecutionGate;
  let workspaceA: string;
  let workspaceB: string;
  let workspaceC: string;

  beforeEach(() => {
    tempDir = makeTempDir('agent-bridge-drainer-');
    taskStore = new TaskStore(tempDir);
    gate = new ExecutionGate(tempDir);
    workspaceA = path.join(tempDir, 'ws-a');
    workspaceB = path.join(tempDir, 'ws-b');
    workspaceC = path.join(tempDir, 'ws-c');
    for (const dir of [workspaceA, workspaceB, workspaceC]) {
      fs.mkdirSync(dir, { recursive: true });
    }

    taskStore.upsertTask(makePendingThread('thread_a1', 'A1', workspaceA, 3000));
    taskStore.upsertTask(makePendingThread('thread_a2', 'A2', workspaceA, 1000));
    taskStore.upsertTask(makePendingThread('thread_b1', 'B1', workspaceB, 2000));
    taskStore.upsertTask(makePendingThread('thread_c1', 'C1', workspaceC, 500));
  });

  afterEach(() => {
    gate.release();
    taskStore.dispose();
  });

  test('disabled: exactly the single global head starts', async () => {
    saveBridgePolicy({ concurrency: { enabled: false } });
    const started: string[] = [];
    const drainer = new QueueDrainer(taskStore, gate, () => false, async (target) => {
      started.push(target.thread.threadId);
    });

    await drainer.drain();
    assert.deepStrictEqual(started, ['thread_a1'], 'one task at a time, mesh-wide');
  });

  test('enabled: per-workspace heads start up to the mesh-wide cap', async () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 2 } });
    const started: string[] = [];
    const drainer = new QueueDrainer(taskStore, gate, () => false, async (target) => {
      started.push(target.thread.threadId);
      gate.tryAcquire(
        target.thread.threadId,
        target.round.round,
        normalizeWorkspaceKey(target.thread.workspacePath),
        target.thread.targetAgent,
      );
    });

    await drainer.drain();
    assert.deepStrictEqual(
      started,
      ['thread_a1', 'thread_b1'],
      'oldest head of each workspace, stopping at the mesh-wide cap',
    );
    assert.strictEqual(gate.countActive(), 2);

    // Tasks that already hold their workspace lease must never be re-entered:
    // raising the cap starts the remaining workspace, not A/B again.
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3 } });
    await drainer.drain();
    assert.deepStrictEqual(started, ['thread_a1', 'thread_b1', 'thread_c1']);
    assert.strictEqual(gate.countActive(), 3);
  });
});

describe('TaskSupervisor - concurrency partitions the queue by workspace', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let gate: ExecutionGate;
  let supervisor: TaskSupervisor;
  let workspaceA: string;
  let workspaceB: string;
  let releaseRounds: () => void;

  beforeEach(() => {
    tempDir = makeTempDir('agent-bridge-supervisor-concurrency-');
    taskStore = new TaskStore(tempDir);
    gate = new ExecutionGate(tempDir);
    workspaceA = path.join(tempDir, 'ws-a');
    workspaceB = path.join(tempDir, 'ws-b');
    fs.mkdirSync(workspaceA, { recursive: true });
    fs.mkdirSync(workspaceB, { recursive: true });

    let release: () => void = () => {};
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    releaseRounds = release;

    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    (runner as any).execute = async (): Promise<RunResult> => {
      await held;
      return {
        exitCode: 0,
        stdout: 'done',
        stderr: '',
        extractedOutput: 'done',
        durationMs: 1,
      };
    };
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    supervisor = new TaskSupervisor(taskStore, runner, callbackManager, detector, gate);
  });

  afterEach(async () => {
    releaseRounds();
    await new Promise((resolve) => setTimeout(resolve, 150));
    supervisor.dispose();
    gate.release();
    taskStore.dispose();
  });

  test('two workspaces run at once; a younger same-workspace task stays queued', () => {
    // Both tasks execute on the same agent, so the per-agent cap is raised too;
    // what is under test here is the per-workspace partition.
    saveBridgePolicy({
      concurrency: { enabled: true, maxConcurrentTasks: 2, maxConcurrentPerAgent: 2 },
    });

    taskStore.upsertTask(makePendingThread('thread_a1', 'A1', workspaceA, 3000, 'running'));
    taskStore.upsertTask(makePendingThread('thread_b1', 'B1', workspaceB, 2000, 'running'));
    taskStore.upsertTask(makePendingThread('thread_a2', 'A2', workspaceA, 1000, 'running'));

    supervisor.checkPendingTasks();

    assert.strictEqual(supervisor.isExecuting('thread_a1'), true);
    assert.strictEqual(supervisor.isExecuting('thread_b1'), true);
    assert.strictEqual(supervisor.isExecuting('thread_a2'), false);
    assert.strictEqual(gate.countActive(), 2, 'one lease per workspace');
    assert.strictEqual(taskStore.getTask('thread_a2')?.status, 'queued');
  });
});
