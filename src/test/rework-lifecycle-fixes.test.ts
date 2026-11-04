import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { AgentDetector } from '../bridge/agent-detector';
import { CallbackManager } from '../bridge/callback-manager';
import { ReworkController } from '../bridge/rework-controller';
import { readReviewerBootstrapToken, writeReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { TaskStore } from '../bridge/task-store';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { DelegationRound, DelegationThread, isReviewerBootstrapPending } from '../bridge/types';

const now = '2026-11-05T00:00:00.000Z';
const reviewerSessionId = 'reviewer-session-0123';

function persisted<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function round(
  n: number,
  status: DelegationRound['status'],
  type: DelegationRound['type'] = 'rework',
): DelegationRound {
  return {
    round: n,
    type,
    prompt: `prompt ${n}`,
    output: status === 'failed' ? `partial output ${n}` : `output ${n}`,
    status,
    durationMs: 1,
    timestamp: now,
  };
}

function task(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_lifecycle_001',
    title: 'Lifecycle regression',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'failed',
    currentRound: 2,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'receiver-session-0123',
    receiverSessionCaptured: true,
    callerSessionId: reviewerSessionId,
    reviewerSessionId,
    reviewerSessionCaptured: true,
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    workspacePath: os.tmpdir(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [round(1, 'completed', 'initial'), round(2, 'failed')],
    ...overrides,
  };
}

async function withController(
  callback: (fixture: { dir: string; store: TaskStore; controller: ReworkController }) => Promise<void>,
): Promise<void> {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-rework-lifecycle-'));
  const store = new TaskStore(dir);
  const detector = new AgentDetector();
  const runner = {
    clearPendingCancellation: () => undefined,
    execute: async () => ({
      exitCode: 0,
      stdout: 'output',
      stderr: '',
      extractedOutput: 'output',
      durationMs: 1,
    }),
  };
  const callbackManager = new CallbackManager(detector);
  callbackManager.notifyCaller = async () => true;
  callbackManager.notifyCallerOfFailure = async () => true;
  const controller = new ReworkController(store, runner as any, callbackManager, detector);
  try {
    await callback({ dir, store, controller });
  } finally {
    (controller as any).queueDrainer.dispose();
    store.dispose();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('dedicated bootstrap readiness requires a confirmed captured canonical reviewer session', () => {
  const failed = task({ reviewBootstrap: { phase: 'failed', attempts: 1 } });
  const mismatched = task({ callerSessionId: 'some-other-session' });
  assert.equal(isReviewerBootstrapPending(failed), true);
  assert.equal(isReviewerBootstrapPending(mismatched), true);
  assert.equal(isReviewerBootstrapPending(task()), false);
});

test('failed bootstrap retry rejects before retarget or ledger mutation', async () => {
  await withController(async ({ store, controller }) => {
    const failed = task({
      status: 'failed',
      reviewBootstrap: { phase: 'failed', attempts: 1, lastError: 'bootstrap failed' },
      callerSessionId: undefined,
      reviewerSessionId: undefined,
      reviewerSessionCaptured: false,
    });
    store.upsertTask(failed);
    const before = persisted(store.getTask(failed.threadId));

    await assert.rejects(
      controller.retryFailedRound(failed.threadId, { targetAgent: 'antigravity' }),
      /dedicated reviewer bootstrap is failed/i,
    );

    assert.deepEqual(persisted(store.getTask(failed.threadId)), before);
  });
});

test('completed and archived tasks cannot be retried or have output cleared', async () => {
  await withController(async ({ store, controller }) => {
    for (const completed of [
      task({ status: 'completed' }),
      task({ status: 'failed', archivedAt: now, rounds: [] }),
    ]) {
      store.upsertTask(completed);
      const before = persisted(store.getTask(completed.threadId));
      await assert.rejects(controller.retryFailedRound(completed.threadId), /completed or archived/i);
      assert.deepEqual(persisted(store.getTask(completed.threadId)), before);
    }
  });
});

test('waiting review cannot retry an older failure behind a newer successful round', async () => {
  await withController(async ({ store, controller }) => {
    const waiting = task({
      status: 'waiting_review',
      currentRound: 3,
      rounds: [round(1, 'failed', 'initial'), round(2, 'completed'), round(3, 'completed')],
    });
    store.upsertTask(waiting);
    const before = persisted(store.getTask(waiting.threadId));

    await assert.rejects(controller.retryFailedRound(waiting.threadId), /while it is waiting_review/i);

    assert.deepEqual(persisted(store.getTask(waiting.threadId)), before);
  });
});

test('latest failed execution round retries in place and retains its receiver session', async () => {
  await withController(async ({ store, controller }) => {
    const failed = task();
    store.upsertTask(failed);
    const originalReceiverSession = failed.receiverSessionId;
    const originalRound = failed.rounds[1];
    const supervisorActive = TaskSupervisor.isSupervisorActive;
    (TaskSupervisor as any).isSupervisorActive = () => true;
    try {
      const retried = await controller.retryFailedRound(failed.threadId);
      assert.equal(retried.round, 2);
      assert.equal(retried.status, 'running');
      assert.equal(store.getTask(failed.threadId)?.receiverSessionId, originalReceiverSession);
      assert.equal(store.getTask(failed.threadId)?.rounds[1].prompt, originalRound.prompt);
      assert.equal(store.getTask(failed.threadId)?.rounds[1].output, undefined);
    } finally {
      (TaskSupervisor as any).isSupervisorActive = supervisorActive;
    }
  });
});

test('retry does not revive a task completed during executor approval', async () => {
  await withController(async ({ store, controller }) => {
    const failed = task();
    store.upsertTask(failed);
    let beginApproval!: () => void;
    let finishRetarget!: () => void;
    const approvalStarted = new Promise<void>((resolve) => { beginApproval = resolve; });
    const approvalGate = new Promise<void>((resolve) => { finishRetarget = resolve; });
    const retargetService = (controller as any).retargetService;
    retargetService.retargetTask = async () => {
      beginApproval();
      await approvalGate;
      return { status: 'RETARGETED' };
    };

    const retrying = controller.retryFailedRound(failed.threadId, { targetAgent: 'antigravity' });
    await approvalStarted;
    const completed = store.getTask(failed.threadId)!;
    completed.status = 'completed';
    store.upsertTask(completed);
    finishRetarget();

    await assert.rejects(retrying, /became completed or archived during retarget/i);
    const stored = store.getTask(failed.threadId)!;
    assert.equal(stored.status, 'completed');
    assert.equal(stored.rounds[1].status, 'failed');
    assert.equal(stored.rounds[1].output, 'partial output 2');
  });
});

test('dedicated rework cannot rebind reviewer session from params or detector', async () => {
  await withController(async ({ store, controller }) => {
    const ready = task({ status: 'waiting_review', currentRound: 1, rounds: [round(1, 'completed', 'initial')] });
    store.upsertTask(ready);
    const before = persisted(store.getTask(ready.threadId));

    await assert.rejects(
      controller.requestRework({
        threadId: ready.threadId,
        feedback: 'please fix this',
        callerSessionId: 'attacker-session-9999',
      }),
      /session is fixed/i,
    );

    assert.deepEqual(persisted(store.getTask(ready.threadId)), before);
  });
});

test('paused pending bootstrap resumes with its existing token and no developer run', async () => {
  await withController(async ({ dir, store, controller }) => {
    const paused = task({
      status: 'paused',
      currentRound: 1,
      reviewBootstrap: { phase: 'launching', attempts: 1 },
      callerSessionId: undefined,
      reviewerSessionId: undefined,
      reviewerSessionCaptured: false,
      rounds: [round(1, 'paused', 'initial')],
    });
    store.upsertTask(paused);
    const token = 'existing-one-shot-token';
    assert.equal(writeReviewerBootstrapToken({
      threadId: paused.threadId,
      token,
      agent: 'codex',
      createdAt: now,
    }, dir), true);

    const supervisorActive = TaskSupervisor.isSupervisorActive;
    (TaskSupervisor as any).isSupervisorActive = () => true;
    try {
      await controller.resumePausedTask(paused.threadId);
    } finally {
      (TaskSupervisor as any).isSupervisorActive = supervisorActive;
    }

    const resumed = store.getTask(paused.threadId)!;
    assert.equal(resumed.status, 'queued');
    assert.equal(resumed.reviewBootstrap?.phase, 'pending');
    assert.equal(resumed.rounds[0].status, 'running');
    assert.equal(readReviewerBootstrapToken(paused.threadId, dir)?.token, token);
    assert.equal(resumed.reviewerSessionId, undefined);
    assert.equal(store.getAllTasks().length, 1);
  });
});
