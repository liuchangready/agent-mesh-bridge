import { testWorkspace } from './workspace-fixture';
import { test, describe } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { AgentDetector } from '../bridge/agent-detector';
import { CallbackManager } from '../bridge/callback-manager';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { ReworkController } from '../bridge/rework-controller';
import { readReviewerBootstrapToken, writeReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { TaskControlService } from '../bridge/task-control';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskStore } from '../bridge/task-store';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { DelegationThread } from '../bridge/types';

function makeDedicatedTask(
  threadId: string,
  phase: 'pending' | 'launching' | 'confirmed',
  overrides: Partial<DelegationThread> = {},
): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId,
    title: 'Reviewer bootstrap resume lifecycle',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'queued',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'receiver-session-original',
    workspace: testWorkspace(os.tmpdir()),
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase, attempts: 1, launchedAt: now },
    summary: {
      lastPromptSnippet: 'Implement the task',
      lastOutputSnippet: 'Reviewer bootstrap pending',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'Implement the task',
        status: 'running',
        durationMs: 0,
        timestamp: now,
      },
    ],
    ...overrides,
  };
}

function createFakeRunner() {
  const calls: Array<Record<string, any>> = [];
  return {
    calls,
    clearPendingCancellation: (_threadId: string) => undefined,
    cancelThread: (_threadId: string, _action: string, _reason: string) => true,
    execute: async (options: Record<string, any>) => {
      calls.push(options);
      return {
        exitCode: 0,
        stdout: 'completed output',
        stderr: '',
        extractedOutput: 'completed output',
        durationMs: 1,
      };
    },
  };
}

async function withFixture(
  run: (fixture: {
    dir: string;
    store: TaskStore;
    registry: TaskExecutionRegistry;
    controller: ReworkController;
    runner: ReturnType<typeof createFakeRunner>;
  }) => Promise<void>,
): Promise<void> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-independent-lifecycle-'));
  const workspace = path.join(dir, 'workspace');
  fs.mkdirSync(workspace, { recursive: true });
  const store = new TaskStore(dir);
  const registry = new TaskExecutionRegistry(dir);
  const detector = new AgentDetector();
  const runner = createFakeRunner();
  const callbackManager = new CallbackManager(detector);
  callbackManager.notifyCaller = async () => true;
  callbackManager.notifyCallerOfFailure = async () => true;
  const controller = new ReworkController(
    store,
    runner as any,
    callbackManager,
    detector,
    registry,
  );

  try {
    await run({ dir, store, registry, controller, runner });
  } finally {
    (controller as any).queueDrainer.dispose();
    registry.dispose();
    store.dispose();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

describe('dedicated reviewer bootstrap pause/resume lifecycle', () => {
  test('resuming a paused pending/launching bootstrap preserves its token and queues bootstrap only', async () => {
    await withFixture(async ({ dir, store, controller, runner }) => {
      const id = 'thread_bootstrap_pending_resume';
      const token = 'one-shot-token-kept-across-pause';
      store.upsertTask(makeDedicatedTask(id, 'launching', {
        workspace: testWorkspace(path.join(dir, 'workspace')),
      }));
      assert.equal(writeReviewerBootstrapToken({
        threadId: id,
        token,
        agent: 'codex',
        createdAt: new Date().toISOString(),
      }, dir), true);

      const cancellation = new CancellationRegistry(dir);
      const taskControl = new TaskControlService(store, runner as any, cancellation, dir);
      assert.equal(taskControl.pauseTask(id, 'pause before reviewer confirmation').status, 'PAUSED');

      const originalSupervisorState = TaskSupervisor.isSupervisorActive;
      (TaskSupervisor as any).isSupervisorActive = () => true;
      try {
        await controller.resumePausedTask(id);
      } finally {
        (TaskSupervisor as any).isSupervisorActive = originalSupervisorState;
      }

      const resumed = store.getTask(id)!;
      assert.equal(resumed.status, 'queued');
      assert.equal(resumed.reviewBootstrap?.phase, 'pending');
      assert.equal(resumed.rounds[0].status, 'running');
      assert.equal(readReviewerBootstrapToken(id, dir)?.token, token);
      assert.equal(runner.calls.length, 0, 'developer runner must stay behind reviewer bootstrap');
      assert.equal(store.getAllTasks().length, 1, 'resume must not create another task');
    });
  });

  test('a consumed token without a captured reviewer session fails visibly and cannot relaunch', async () => {
    await withFixture(async ({ dir, store, controller, runner }) => {
      const id = 'thread_bootstrap_missing_session';
      store.upsertTask(makeDedicatedTask(id, 'confirmed', {
        workspace: testWorkspace(path.join(dir, 'workspace')),
        reviewerSessionCaptured: false,
      }));
      // No token file models the one-shot confirmation token already being consumed.
      assert.equal(readReviewerBootstrapToken(id, dir), undefined);

      const taskControl = new TaskControlService(
        store,
        runner as any,
        new CancellationRegistry(dir),
        dir,
      );
      taskControl.pauseTask(id, 'pause before reviewer session capture');

      const originalSupervisorState = TaskSupervisor.isSupervisorActive;
      (TaskSupervisor as any).isSupervisorActive = () => true;
      try {
        await controller.resumePausedTask(id);
      } finally {
        (TaskSupervisor as any).isSupervisorActive = originalSupervisorState;
      }

      const failed = store.getTask(id)!;
      assert.equal(failed.status, 'failed');
      assert.equal(failed.reviewBootstrap?.phase, 'failed');
      assert.match(failed.reviewBootstrap?.lastError || '', /session was never captured/i);
      assert.equal(failed.rounds[0].status, 'failed');
      assert.match(failed.rounds[0].error || '', /session was never captured/i);
      assert.equal(runner.calls.length, 0, 'resume must not start developer or a replacement reviewer');
      assert.equal(readReviewerBootstrapToken(id, dir), undefined);
      assert.equal(store.getAllTasks().length, 1);
    });
  });

  test('a captured reviewer handoff resumes the existing receiver session in the same task', async () => {
    await withFixture(async ({ dir, store, registry, controller, runner }) => {
      const id = 'thread_bootstrap_captured_resume';
      const receiverSessionId = 'receiver-session-captured-before-pause';
      store.upsertTask(makeDedicatedTask(id, 'confirmed', {
        status: 'running',
        receiverSessionId,
        reviewerSessionCaptured: true,
        reviewerSessionId: 'reviewer-session-captured',
        callerSessionId: 'reviewer-session-captured',
        workspace: testWorkspace(path.join(dir, 'workspace')),
      }));

      const taskControl = new TaskControlService(
        store,
        runner as any,
        new CancellationRegistry(dir),
        dir,
      );
      taskControl.pauseTask(id, 'pause developer after reviewer handoff');

      let signalStarted!: () => void;
      const started = new Promise<void>((resolve) => { signalStarted = resolve; });
      let finishExecution!: (result: any) => void;
      (runner as any).execute = (options: Record<string, any>) => {
        runner.calls.push(options);
        signalStarted();
        return new Promise((resolve) => { finishExecution = resolve; });
      };

      const originalSupervisorState = TaskSupervisor.isSupervisorActive;
      (TaskSupervisor as any).isSupervisorActive = () => false;
      try {
        await controller.resumePausedTask(id);
        await started;
        assert.equal(runner.calls.length, 1);
        assert.equal(runner.calls[0].receiverSessionId, receiverSessionId);
        assert.equal(store.getAllTasks().length, 1, 'resume must reuse the existing task');
        finishExecution({
          exitCode: 0,
          stdout: 'resumed output',
          stderr: '',
          extractedOutput: 'resumed output',
          durationMs: 1,
        });
        for (let attempt = 0; attempt < 50 && registry.getOwner(id); attempt++) {
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
      } finally {
        (TaskSupervisor as any).isSupervisorActive = originalSupervisorState;
        if (finishExecution) {
          finishExecution({ exitCode: -1, stdout: '', stderr: '', extractedOutput: '', durationMs: 0 });
        }
      }

      const resumed = store.getTask(id)!;
      assert.equal(resumed.receiverSessionId, receiverSessionId);
      assert.equal(resumed.rounds.length, 1);
    });
  });
});
