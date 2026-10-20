import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-control-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { ExecutionGate } from '../bridge/execution-gate';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskControlService } from '../bridge/task-control';
import { ReworkController } from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_control_1',
    title: 'Pause/delete probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'running',
    currentRound: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    receiverSessionId: 'receiver-session-0001',
    workspacePath: os.tmpdir(),
    summary: {
      lastPromptSnippet: 'do the work',
      lastOutputSnippet: 'Executing...',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'do the work',
        status: 'running',
        durationMs: 10,
        timestamp: new Date().toISOString(),
      },
    ],
    ...overrides,
  };
}

describe('TaskControlService - pause/delete reasons', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let runner: AutonomousRunner;
  let canceled: Array<{ threadId: string; action: string; reason: string }>;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-control-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    runner = new AutonomousRunner(detector, undefined, new CancellationRegistry(tempDir));
    canceled = [];
    runner.cancelThread = (threadId, action, reason) => {
      canceled.push({ threadId, action, reason });
      return true;
    };
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('pausing a running task records the interruption reason and stops the child', () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );

    const result = control.pauseTask('thread_control_1', 'switch to a hotfix', 'user');
    assert.strictEqual(result.status, 'PAUSED');

    const thread = taskStore.getTask('thread_control_1')!;
    assert.strictEqual(thread.status, 'paused');
    assert.strictEqual(thread.rounds[0].status, 'paused');
    assert.strictEqual(thread.interruption?.kind, 'paused');
    assert.strictEqual(thread.interruption?.reason, 'switch to a hotfix');
    assert.strictEqual(thread.interruptions?.length, 1);
    assert.strictEqual(thread.rounds[0].interruption?.reason, 'switch to a hotfix');

    assert.deepStrictEqual(canceled, [
      { threadId: 'thread_control_1', action: 'pause', reason: 'switch to a hotfix' },
    ]);
    const request = new CancellationRegistry(tempDir).peek('thread_control_1');
    assert.strictEqual(request?.action, 'pause');
    assert.strictEqual(request?.reason, 'switch to a hotfix');
  });

  test('deleting a task removes it, stops the child, and writes a tombstone reason', () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );

    const result = control.deleteTask('thread_control_1', 'duplicate request', 'user');
    assert.strictEqual(result.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_control_1'), undefined);
    assert.deepStrictEqual(canceled, [
      { threadId: 'thread_control_1', action: 'delete', reason: 'duplicate request' },
    ]);

    const tombstone = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim();
    const record = JSON.parse(tombstone);
    assert.strictEqual(record.threadId, 'thread_control_1');
    assert.strictEqual(record.reason, 'duplicate request');
  });

  test('resuming a paused task keeps history and clears the current interruption', async () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );
    control.pauseTask('thread_control_1', 'pause for inspection', 'user');

    const callbackManager = new CallbackManager(new AgentDetector());
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const reworkController = new ReworkController(
      taskStore,
      runner,
      callbackManager,
      new AgentDetector(),
      new ExecutionGate(tempDir),
    );

    let executedPrompt = '';
    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      executedPrompt = options.prompt;
      return {
        exitCode: 0,
        stdout: 'resumed',
        stderr: '',
        extractedOutput: 'resumed',
        durationMs: 5,
      };
    };

    const round = await reworkController.resumePausedTask('thread_control_1');
    assert.strictEqual(round.status, 'running');

    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      if (taskStore.getTask('thread_control_1')?.status === 'waiting_review') break;
      await new Promise((r) => setTimeout(r, 20));
    }

    const thread = taskStore.getTask('thread_control_1')!;
    assert.strictEqual(executedPrompt, 'do the work');
    assert.strictEqual(thread.status, 'waiting_review');
    assert.strictEqual(thread.interruption, undefined, 'current interruption cleared');
    assert.strictEqual(thread.interruptions?.[0]?.reason, 'pause for inspection', 'history kept');
  });
});
