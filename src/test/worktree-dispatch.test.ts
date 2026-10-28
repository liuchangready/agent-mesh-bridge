import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

// Keep this file's wake queue / cancellation registry out of the real home.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-wt-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ExecutionGate } from '../bridge/execution-gate';
import { TaskControlService } from '../bridge/task-control';
import { DelegationThread, pendingMerges } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const base: DelegationThread = {
    threadId: 'thread_wt_1',
    title: 'Worktree task',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'completed',
    currentRound: 1,
    createdAt: '2026-10-23T06:00:00.000Z',
    updatedAt: '2026-10-23T06:10:00.000Z',
    receiverSessionId: 'sess-1',
    workspacePath: 'D:/majiang-wt/p1',
    summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 0 },
    rounds: [],
    worktree: {
      path: 'D:/majiang-wt/p1',
      mainPath: 'D:/majiang',
      branch: 'wt/p1',
      baseCommit: 'abc1234',
      mergePending: false,
    },
  };
  return { ...base, ...overrides } as DelegationThread;
}

const MERGE_REQUEST = {
  url: 'https://example.com/pr/9',
  branch: 'wt/p1',
  commit: 'def5678',
  remote: 'origin',
  status: 'synced' as const,
  summary: 'merged',
  updatedAt: '2026-10-23T06:20:00.000Z',
  by: 'codex',
};

describe('Worktree dispatch declaration', () => {
  let tempDir: string;
  let store: TaskStore;
  let dispatcher: AsyncDispatcher;
  let runner: AutonomousRunner;
  let wtDir: string;
  let mainDir: string;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-wt-store-'));
    mainDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-main-'));
    wtDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-wt-'));
    store = new TaskStore(tempDir);
    const detector = new AgentDetector();
    const resolver = new ActiveAgentResolver(detector);
    runner = new AutonomousRunner(detector);
    runner.execute = async () => ({
      exitCode: 0,
      stdout: 'ok',
      stderr: '',
      extractedOutput: 'done',
      durationMs: 1,
    });
    const callbackManager = new CallbackManager(detector, tempDir);
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const gate = new ExecutionGate(tempDir);
    dispatcher = new AsyncDispatcher(store, resolver, runner, callbackManager, detector, gate);
  });

  afterEach(() => {
    store.dispose();
    for (const dir of [tempDir, mainDir, wtDir]) {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    }
  });

  test('records the merge-back obligation when a worktree is declared', async () => {
    const res = await dispatcher.dispatchTask({
      targetAgent: 'antigravity',
      title: 'Worktree work',
      prompt: 'edit files in the temporary worktree',
      workspacePath: wtDir,
      worktree: { mainPath: mainDir, branch: 'wt/x', baseCommit: 'abc1234' },
      expectedFiles: ['src/Feature.kt'],
    });

    const thread = store.getTask(res.threadId)!;
    assert.ok(thread.worktree, 'worktree metadata must be persisted');
    assert.strictEqual(path.resolve(thread.worktree!.path), path.resolve(wtDir));
    assert.strictEqual(path.resolve(thread.worktree!.mainPath), path.resolve(mainDir));
    assert.strictEqual(thread.worktree!.branch, 'wt/x');
    assert.strictEqual(thread.worktree!.baseCommit, 'abc1234');
    assert.strictEqual(thread.worktree!.mergePending, false, 'not merged yet, not pending yet');
  });

  test('rejects a worktree declaration whose main path is the workspace itself', async () => {
    await assert.rejects(
      () =>
        dispatcher.dispatchTask({
          targetAgent: 'antigravity',
          title: 'Bad worktree',
          prompt: 'p',
          workspacePath: wtDir,
          worktree: { mainPath: wtDir },
          expectedFiles: ['src/Feature.kt'],
        }),
      /must differ from workspacePath/i,
    );
    assert.strictEqual(store.getAllTasks().length, 0, 'nothing may be persisted on a bad spec');
  });

  test('rejects a worktree declaration without mainPath', async () => {
    await assert.rejects(
      () =>
        dispatcher.dispatchTask({
          targetAgent: 'antigravity',
          title: 'Bad worktree',
          prompt: 'p',
          workspacePath: wtDir,
          worktree: { mainPath: '   ' },
          expectedFiles: ['src/Feature.kt'],
        }),
      /mainPath is required/i,
    );
    assert.strictEqual(store.getAllTasks().length, 0);
  });
});

describe('Merge gate - temporary worktree output must not be lost', () => {
  let tempDir: string;
  let store: TaskStore;
  let control: TaskControlService;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-merge-'));
    store = new TaskStore(tempDir);
    control = new TaskControlService(store);
  });

  afterEach(() => {
    store.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('a completed worktree task is marked mergePending and listed in merge order', () => {
    store.upsertTask(makeThread({ threadId: 'thread_late', createdAt: '2026-10-23T08:00:00.000Z' }));
    store.upsertTask(makeThread({ threadId: 'thread_early', createdAt: '2026-10-23T06:00:00.000Z' }));
    store.upsertTask(makeThread({ threadId: 'thread_plain', createdAt: '2026-10-23T05:00:00.000Z', worktree: undefined }));

    assert.strictEqual(store.getTask('thread_early')!.worktree?.mergePending, true);
    assert.strictEqual(store.getTask('thread_late')!.worktree?.mergePending, true);

    const pending = pendingMerges(store.getAllTasks());
    assert.deepStrictEqual(
      pending.map((t) => t.threadId),
      ['thread_early', 'thread_late'],
      'merge order follows createdAt, and non-worktree tasks never appear',
    );
  });

  test('delete is refused until the merge request is recorded', () => {
    store.upsertTask(makeThread());

    assert.throws(
      () => control.deleteTask('thread_wt_1', 'cleanup', 'test', false),
      /has not been merged|merge back/i,
      'unmerged worktree output must not be deletable',
    );
    assert.ok(store.getTask('thread_wt_1'), 'the task must still exist after a refused delete');

    store.recordMergeRequest('thread_wt_1', MERGE_REQUEST);
    assert.strictEqual(
      store.getTask('thread_wt_1')!.worktree?.mergePending,
      false,
      'recording the MR clears the pending flag',
    );
    assert.deepStrictEqual(pendingMerges(store.getAllTasks()), []);

    control.deleteTask('thread_wt_1', 'cleanup', 'test', false);
    assert.strictEqual(store.getTask('thread_wt_1'), undefined);
  });

  test('force:true still allows an explicit discard', () => {
    store.upsertTask(makeThread({ threadId: 'thread_force' }));
    control.deleteTask('thread_force', 'discard on purpose', 'test', true);
    assert.strictEqual(store.getTask('thread_force'), undefined);
  });

  test('a non-worktree task deletes without a merge request', () => {
    store.upsertTask(makeThread({ threadId: 'thread_plain', worktree: undefined }));
    control.deleteTask('thread_plain', 'cleanup', 'test', false);
    assert.strictEqual(store.getTask('thread_plain'), undefined);
  });
});
