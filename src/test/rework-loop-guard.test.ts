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
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { ExecutionGate } from '../bridge/execution-gate';
import {
  ReworkController,
  assertNotNoopRework,
  promptSimilarity,
} from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_loop_1',
    title: 'no-op rework guard',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'waiting_review',
    currentRound: 1,
    createdAt: '2026-10-23T10:00:00.000Z',
    updatedAt: '2026-10-23T10:00:00.000Z',
    receiverSessionId: 'session-loop-0001',
    callerSessionId: 'rollout-loop-0001',
    workspacePath: os.tmpdir(),
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
    const runner = new AutonomousRunner(detector);
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    controller = new ReworkController(store, runner, callbackManager, detector, new ExecutionGate(tempDir));
  });

  afterEach(() => {
    store.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('blockReview parks the review without creating a round', () => {
    store.upsertTask(makeThread());
    const before = store.getTask('thread_loop_1')!.rounds.length;

    const result = controller.blockReview({
      threadId: 'thread_loop_1',
      reason: 'waiting for the sibling PostgreSQL task',
      waitingForThreadId: 'thread_pg_1',
    });

    const parked = store.getTask('thread_loop_1')!;
    assert.strictEqual(parked.status, 'waiting_review');
    assert.strictEqual(parked.rounds.length, before, 'a blocked review must not add a round');
    assert.strictEqual(parked.reviewWake, undefined, 'no retry timer for a parked review');
    assert.strictEqual(parked.reviewBlocked?.waitingForThreadId, 'thread_pg_1');
    assert.match(result.message, /no rework round was created/);
  });

  test('blockReview refuses a task that is not waiting for review', () => {
    store.upsertTask(makeThread({ status: 'reworking' }));
    assert.throws(
      () => controller.blockReview({ threadId: 'thread_loop_1', reason: 'x' }),
      /not "waiting_review"/,
    );
  });

  test('a completed task cannot be resurrected by a late rework', async () => {
    store.upsertTask(makeThread({ status: 'completed' }));
    await assert.rejects(
      () => controller.requestRework({ threadId: 'thread_loop_1', feedback: 'again' }),
      /already approved and archived/,
    );
  });
});
