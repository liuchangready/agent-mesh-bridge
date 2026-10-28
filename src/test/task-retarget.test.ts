import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { TaskStore } from '../bridge/task-store';
import { TaskRetargetService, latestRetarget } from '../bridge/task-retarget';
import { invalidateConfiguredModelCache, saveModelDefaults } from '../bridge/model-catalog';
import { DelegationThread, DelegationRound, shouldResumeNativeSession } from '../bridge/types';
import { ModelApprovalDecision } from '../bridge/model-approval';

describe('shouldResumeNativeSession after an executor switch', () => {
  const reworkRound: DelegationRound = {
    round: 2,
    type: 'rework',
    prompt: 'continue',
    status: 'running',
    durationMs: 0,
    timestamp: new Date().toISOString(),
  };
  const resetRecord = {
    at: new Date().toISOString(),
    by: 'user',
    fromAgent: 'deepseek_harness' as const,
    toAgent: 'claude_code' as const,
    previousReceiverSessionId: 'session-old',
    sessionReset: true,
  };

  test('the first round after an agent switch starts a brand-new session', () => {
    const thread = makeThread({
      receiverSessionId: 'session-new',
      receiverSessionCaptured: false,
      retargets: [resetRecord],
    });
    assert.strictEqual(shouldResumeNativeSession(thread, reworkRound), false);
  });

  test('once the new session was captured, later rounds resume it', () => {
    // Regression: returning false forever made the runner reserve an existing
    // session id with `--session-id`, and Claude Code answers
    // "Session ID <id> is already in use." — the round dies in one second.
    const thread = makeThread({
      receiverSessionId: 'session-new',
      receiverSessionCaptured: true,
      retargets: [resetRecord],
    });
    assert.strictEqual(shouldResumeNativeSession(thread, reworkRound), true);
  });

  test('a model-only switch keeps the existing session', () => {
    const thread = makeThread({
      receiverSessionId: 'session-old',
      receiverSessionCaptured: true,
      retargets: [{ ...resetRecord, sessionReset: false, previousReceiverSessionId: undefined }],
    });
    assert.strictEqual(shouldResumeNativeSession(thread, reworkRound), true);
  });

  test('an initial round still needs a captured session to resume', () => {
    const initialRound: DelegationRound = { ...reworkRound, type: 'initial' };
    const thread = makeThread({ receiverSessionCaptured: false, retargets: [] });
    assert.strictEqual(shouldResumeNativeSession(thread, initialRound), false);
  });
});

const CONFIGURED = 'deepseek/deepseek-v4.1-flash';
const OTHER = 'deepseek/deepseek-v4-pro';
const CODEX_DEFAULT = 'gpt-6.1-sol';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  const round: DelegationRound = {
    round: 1,
    type: 'initial',
    prompt: 'implement the shuffle cheat',
    status: 'completed',
    durationMs: 1,
    timestamp: now,
  };
  return {
    threadId: 'thread_retarget_1',
    title: '统一码牌千术',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'waiting_review',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'session-old',
    receiverSessionCaptured: true,
    callerSessionId: 'caller-1',
    workspacePath: process.cwd(),
    expectedFiles: ['engine/src/main/kotlin/Foo.kt'],
    summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 1 },
    rounds: [round],
    ...overrides,
  };
}

function decision(status: ModelApprovalDecision['status']): (r: any) => Promise<ModelApprovalDecision> {
  return async (request: any) => ({
    id: request.id,
    status,
    at: new Date().toISOString(),
    by: 'user',
  });
}

describe('TaskRetargetService - switching a task executor', () => {
  let tempDir: string;
  let bridgeHome: string;
  let taskStore: TaskStore;
  let service: TaskRetargetService;
  let previousBridgeHome: string | undefined;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-retarget-'));
    bridgeHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-retarget-home-'));
    previousBridgeHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = bridgeHome;
    invalidateConfiguredModelCache();
    // DSH keeps the Flash default; Codex has its own default model.
    saveModelDefaults(
      { deepseek_harness: CONFIGURED, codex: CODEX_DEFAULT },
      bridgeHome,
    );
    taskStore = new TaskStore(tempDir);
    service = new TaskRetargetService(taskStore);
  });

  afterEach(() => {
    taskStore.dispose();
    if (previousBridgeHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = previousBridgeHome;
    invalidateConfiguredModelCache();
    for (const dir of [tempDir, bridgeHome]) {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    }
  });

  test('switching the agent drops the native session and keeps the task', async () => {
    taskStore.upsertTask(makeThread());

    const res = await service.retargetTask({
      threadId: 'thread_retarget_1',
      targetAgent: 'codex',
      reason: 'Pro is rate limited',
      by: 'codex',
    });

    assert.strictEqual(res.status, 'RETARGETED');
    assert.strictEqual(res.targetAgent, 'codex');
    assert.strictEqual(res.previousAgent, 'deepseek_harness');
    assert.strictEqual(res.sessionReset, true);
    // No model given -> the new agent's configured default.
    assert.strictEqual(res.targetModel, CODEX_DEFAULT);

    const stored = taskStore.getTask('thread_retarget_1')!;
    assert.strictEqual(stored.targetAgent, 'codex');
    assert.strictEqual(stored.targetModel, CODEX_DEFAULT);
    assert.notStrictEqual(stored.receiverSessionId, 'session-old');
    assert.strictEqual(stored.receiverSessionCaptured, false);
    // History and scope survive the switch.
    assert.strictEqual(stored.rounds.length, 1);
    assert.strictEqual(stored.status, 'waiting_review');
    assert.deepStrictEqual(stored.expectedFiles, ['engine/src/main/kotlin/Foo.kt']);

    const record = latestRetarget(stored)!;
    assert.strictEqual(record.fromAgent, 'deepseek_harness');
    assert.strictEqual(record.toAgent, 'codex');
    assert.strictEqual(record.previousReceiverSessionId, 'session-old');
    assert.strictEqual(record.sessionReset, true);
    assert.strictEqual(record.reason, 'Pro is rate limited');
  });

  test('a model-only switch approved by the user keeps the native session', async () => {
    taskStore.upsertTask(makeThread());
    service.setModelApprovalHandler(decision('approved'));

    const res = await service.retargetTask({
      threadId: 'thread_retarget_1',
      model: OTHER,
      modelRequestedByUser: true,
    });

    assert.strictEqual(res.status, 'RETARGETED');
    assert.strictEqual(res.targetModel, OTHER);
    assert.strictEqual(res.sessionReset, false);
    assert.strictEqual(res.modelApproval?.status, 'approved');

    const stored = taskStore.getTask('thread_retarget_1')!;
    assert.strictEqual(stored.targetAgent, 'deepseek_harness');
    assert.strictEqual(stored.receiverSessionId, 'session-old');
    assert.strictEqual(stored.receiverSessionCaptured, true);
    assert.strictEqual(stored.modelApproval?.status, 'approved');
  });

  test('a model the caller did not attribute to the user is ignored', async () => {
    taskStore.upsertTask(makeThread({ targetModel: OTHER }));
    service.setModelApprovalHandler(async () => {
      throw new Error('a guessed model must not reach the user');
    });

    const res = await service.retargetTask({
      threadId: 'thread_retarget_1',
      model: 'gpt-6-luna',
    });

    assert.strictEqual(res.status, 'UNCHANGED');
    assert.strictEqual(res.targetModel, OTHER);
    assert.match(String(res.modelNotice || ''), /ignored/);
  });

  test('the user choosing the default moves the task onto the configured default', async () => {
    taskStore.upsertTask(makeThread({ targetModel: OTHER }));
    service.setModelApprovalHandler(decision('use_default'));

    const res = await service.retargetTask({
      threadId: 'thread_retarget_1',
      model: 'gpt-6.1-sol',
      modelRequestedByUser: true,
    });

    // "Use the default model" means exactly that: the configured default, not
    // the non-default model the task happened to be pinned to.
    assert.strictEqual(res.status, 'RETARGETED');
    assert.strictEqual(res.targetModel, CONFIGURED);
    assert.strictEqual(res.modelApproval?.status, 'use_default');
    assert.strictEqual(taskStore.getTask('thread_retarget_1')!.targetModel, CONFIGURED);
  });

  test('a declined switch aborts before touching the thread', async () => {
    taskStore.upsertTask(makeThread({ targetModel: OTHER }));
    service.setModelApprovalHandler(decision('cancelled'));

    await assert.rejects(
      () =>
        service.retargetTask({
          threadId: 'thread_retarget_1',
          targetAgent: 'codex',
          model: 'gpt-6-luna',
          modelRequestedByUser: true,
        }),
      (err: any) => err?.code === 'MODEL_APPROVAL_DENIED',
    );

    const stored = taskStore.getTask('thread_retarget_1')!;
    assert.strictEqual(stored.targetAgent, 'deepseek_harness');
    assert.strictEqual(stored.targetModel, OTHER);
    assert.strictEqual(stored.receiverSessionId, 'session-old');
    assert.strictEqual(stored.receiverSessionCaptured, true);
    assert.strictEqual(stored.retargets, undefined);
  });

  test('refuses while a round is still in flight', async () => {
    taskStore.upsertTask(
      makeThread({
        status: 'running',
        rounds: [
          {
            round: 1,
            type: 'initial',
            prompt: 'p',
            status: 'running',
            durationMs: 0,
            timestamp: new Date().toISOString(),
          },
        ],
      }),
    );

    await assert.rejects(
      () => service.retargetTask({ threadId: 'thread_retarget_1', targetAgent: 'codex' }),
      /in flight/i,
    );
  });

  test('refuses on a completed task and on an unknown thread', async () => {
    taskStore.upsertTask(makeThread({ status: 'completed' }));
    await assert.rejects(
      () => service.retargetTask({ threadId: 'thread_retarget_1', targetAgent: 'codex' }),
      /completed/i,
    );
    await assert.rejects(
      () => service.retargetTask({ threadId: 'thread_missing', targetAgent: 'codex' }),
      /not found/i,
    );
  });

  test('queued and paused tasks are switchable, and the queue is released', async () => {
    for (const status of ['queued', 'paused'] as const) {
      const threadId = `thread_${status}`;
      taskStore.upsertTask(makeThread({ threadId, status }));
      const res = await service.retargetTask({
        threadId,
        targetAgent: 'claude_code',
        reason: 'move off DeepSeek',
      });
      assert.strictEqual(res.status, 'RETARGETED');
      const stored = taskStore.getTask(threadId)!;
      // The task keeps its parked status; only the executor changed.
      assert.strictEqual(stored.status, status);
      assert.strictEqual(stored.targetAgent, 'claude_code');
      assert.ok(stored.retargets && stored.retargets.length === 1);
    }
  });
});
