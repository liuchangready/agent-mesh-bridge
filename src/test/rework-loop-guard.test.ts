import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-loop-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { CallbackManager } from '../bridge/callback-manager';
import {
  ReworkController,
  assertNotNoopRework,
  promptSimilarity,
} from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: 'rollout-loop-0001',
    threadId: 'thread_loop_1',
    title: 'no-op rework guard',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'reviewing',
    currentRound: 1,
    createdAt: '2026-10-23T10:00:00.000Z',
    updatedAt: '2026-10-23T10:00:00.000Z',
    receiverSessionId: 'session-loop-0001',
    callerSessionId: 'rollout-loop-0001',
    workspace: testWorkspace(os.tmpdir()),
    summary: {
      lastPromptSnippet: 'do the work',
      lastOutputSnippet: 'done',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'rework',
        prompt: '第3轮caller独立复验：本任务范围 PASS，无新增缺陷，仅同批 PG 依赖未交付。',
        status: 'completed',
        durationMs: 20_000,
        timestamp: '2026-10-23T10:00:00.000Z',
        touchedFiles: [],
      },
    ],
    ...overrides,
  };
}

describe('no-op rework guard', () => {
  test('rejects a rework that repeats the previous instruction after an empty round', () => {
    const thread = makeThread();
    const repeated = '第4轮caller独立复验：本任务范围 PASS，无新增缺陷，仅同批 PG 依赖未交付。';

    assert.ok(promptSimilarity(repeated, thread.rounds[0].prompt!) > 0.9);
    assert.throws(() => assertNotNoopRework(thread, repeated), /REWORK_NOOP_LOOP/);
  });

  test('allows the same words when the previous round did change files', () => {
    const thread = makeThread();
    thread.rounds[0].touchedFiles = ['src/a.ts'];
    assert.doesNotThrow(() => assertNotNoopRework(thread, thread.rounds[0].prompt!));
  });

  test('allows genuinely new feedback after an empty round', () => {
    const thread = makeThread();
    const fresh =
      '编译报错：TableActor.cs:120 未解析 DeadlineArbiter；请补 using 并补一条失败前缀回归测试。';
    assert.doesNotThrow(() => assertNotNoopRework(thread, fresh));
  });

  test('legacy rounds without touchedFiles are never treated as no-ops', () => {
    const thread = makeThread();
    delete thread.rounds[0].touchedFiles;
    assert.doesNotThrow(() => assertNotNoopRework(thread, thread.rounds[0].prompt!));
  });
});

describe('review block and terminal state', () => {
  let tempDir: string;
  let store: TaskStore;
  let controller: ReworkController;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-loop-'));
    store = new TaskStore(tempDir);
    const detector = new AgentDetector();
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyReviewer = async () => true;
    controller = new ReworkController(store, callbackManager);
  });

  afterEach(() => {
    store.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('historical tasks cannot be blocked from review through the retired control path', async () => {
    store.upsertTask(makeThread({ status: 'running' }));
    await assert.rejects(
      () => controller.blockReview({ threadId: 'thread_loop_1', reason: 'x' }),
      /HISTORICAL_TASK_READ_ONLY/,
    );
  });

  test('a completed v2 task cannot be resurrected by a late rework', async () => {
    const thread = makeThread({ status: 'completed', execution: {
      version: 2, revision: 1, operationId: 'completed-review', phase: 'review', round: 1,
      generation: 1, attempt: 0, autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    } });
    store.upsertTask(thread);
    const before = store.reloadTask(thread.threadId);
    await assert.rejects(
      () => controller.requestRework({ threadId: 'thread_loop_1', feedback: 'again' }),
      /TERMINAL_STATUS.*completed/,
    );
    assert.deepEqual(store.reloadTask(thread.threadId), before);
  });
});
