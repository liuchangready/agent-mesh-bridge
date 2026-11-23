import { testWorkspace } from './workspace-fixture';
import { test } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  archiveThreadData,
  buildFinalSummary,
  ensureFinalSummary,
  removeTaskArtifacts,
  selectTasksToArchive,
} from '../bridge/task-summary';
import { TaskStore } from '../bridge/task-store';
import { TaskArchiveService } from '../bridge/task-archive';
import { DelegationThread } from '../bridge/types';
import { NativeSessionLock } from '../bridge/native-session-lock';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_summary_1',
    title: 'Fix the parser',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'reviewing',
    currentRound: 2,
    createdAt: '2026-11-01T00:00:00.000Z',
    updatedAt: '2026-11-01T00:10:00.000Z',
    receiverSessionId: 'receiver-session-1',
    callerSessionId: 'caller-session-1',
    workspace: testWorkspace(path.join(os.tmpdir(), 'bridge-workspace-summary')),
    summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 10 },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'Implement the parser',
        output: 'I changed the tokenizer.',
        durationMs: 1000,
        status: 'completed',
        timestamp: '2026-11-01T00:01:00.000Z',
        touchedFiles: ['src/parser.ts'],
        tokenUsage: {
          uncachedInputTokens: 10,
          cacheReadTokens: 0,
          cacheWriteTokens: 0,
          promptTokens: 10,
          outputTokens: 5,
          totalTokens: 15,
          agent: 'claude_code',
          source: 'native_reported',
          available: true,
          collectedAt: '2026-11-01T00:01:00.000Z',
        },
      },
      {
        round: 2,
        type: 'approval',
        prompt: 'Approved: parser and tests look correct.',
        output: 'Task successfully approved and verified.',
        durationMs: 0,
        status: 'completed',
        timestamp: '2026-11-01T00:10:00.000Z',
      },
    ],
    targetModel: 'sonnet',
    initiatorModel: 'gpt-6.1-sol',
    ...overrides,
  };
}

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

test('final summary - completed task keeps verdict, files and MR', () => {
  const thread = makeThread({
    status: 'completed',
    mergeRequest: {
      url: 'https://example.test/mr/1',
      status: 'created',
      updatedAt: '2026-11-01T00:11:00.000Z',
    },
  });
  const summary = buildFinalSummary(thread);
  assert.strictEqual(summary.kind, 'completed');
  assert.match(summary.verdict || '', /parser and tests/);
  assert.deepStrictEqual(summary.changedFiles, ['src/parser.ts']);
  assert.strictEqual(summary.mergeRequest?.url, 'https://example.test/mr/1');
  assert.strictEqual(summary.rounds, 1, 'approval rounds are bookkeeping, not work');
  assert.strictEqual(summary.totalTokens, 15);
  assert.strictEqual(summary.reviewerAgent, 'codex');
  assert.strictEqual(summary.executorAgent, 'claude_code');
});

test('final summary - failed task records the terminal error', () => {
  const thread = makeThread({
    status: 'failed',
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'Implement the parser',
        output: 'partial',
        error: 'NONZERO_EXIT (exit code 1)',
        durationMs: 100,
        status: 'failed',
        timestamp: '2026-11-01T00:01:00.000Z',
      },
    ],
  });
  const summary = buildFinalSummary(thread);
  assert.strictEqual(summary.kind, 'failed');
  assert.match(summary.error || '', /NONZERO_EXIT/);
  assert.match(summary.summary, /任务失败/);
});

test('final summary - review_failed records why no verdict was reached', () => {
  const thread = makeThread({
    status: 'failed',
    reviewFailure: {
      kind: 'unresolvable_park',
      reason: '检视挂起在桥接无法观测的依赖上：等待外部 Client 控制投影修复',
      at: '2026-11-01T00:20:00.000Z',
      round: 1,
      by: 'watchdog',
      agent: 'codex',
    },
  });
  thread.failure = { stage: 'review', reason: thread.reviewFailure!.reason, at: thread.reviewFailure!.at };
  const summary = buildFinalSummary(thread);
  assert.strictEqual(summary.kind, 'failed');
  assert.match(summary.error || '', /外部 Client 控制投影修复/);
  assert.match(summary.verdict || '', /外部 Client 控制投影修复/);
  assert.match(summary.summary, /检视失败/);
  assert.strictEqual(summary.rounds, 1);
});

test('final summary - only terminal tasks get one', () => {
  const running = makeThread({ status: 'running' });
  assert.strictEqual(ensureFinalSummary(running), false);
  assert.strictEqual(running.finalSummary, undefined);

  const done = makeThread({ status: 'completed' });
  assert.strictEqual(ensureFinalSummary(done), true);
  assert.ok(done.finalSummary);
  assert.strictEqual(ensureFinalSummary(done), false, 'idempotent');

  const failedReview = makeThread({
    status: 'failed',
    reviewFailure: {
      kind: 'retries_exhausted',
      reason: '检视唤醒自动重试已耗尽（6 次失败）：429 Too Many Requests',
      at: '2026-11-01T00:20:00.000Z',
      round: 1,
    },
  });
  failedReview.failure = { stage: 'review', reason: failedReview.reviewFailure!.reason, at: failedReview.reviewFailure!.at };
  assert.strictEqual(ensureFinalSummary(failedReview), true);
  assert.strictEqual(failedReview.finalSummary?.kind, 'failed');
});

test('archive - keeps only the digest and metadata', () => {
  const dir = makeTempDir('bridge-archive-data-');
  const thread = makeThread({ status: 'completed', reviewFlow: 'dedicated' });
  thread.receiverSessionCaptured = true;
  thread.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
  assert.strictEqual(archiveThreadData(thread), true);

  assert.ok(thread.archivedAt);
  assert.deepStrictEqual(thread.rounds, []);
  assert.strictEqual(thread.callerSessionId, undefined);
  assert.strictEqual(thread.receiverSessionId, '');
  assert.strictEqual(thread.reviewBootstrap, undefined);
  assert.ok(thread.finalSummary);
  assert.match(thread.finalSummary!.summary, /审阅通过/);
  assert.strictEqual(thread.title, 'Fix the parser');
  assert.strictEqual(thread.targetModel, 'sonnet');
  assert.strictEqual(archiveThreadData(thread), false, 'already archived');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('archive policy - newest N stay hot and only terminal tasks are eligible', () => {
  const tasks: DelegationThread[] = [];
  for (let i = 0; i < 5; i++) {
    tasks.push(
      makeThread({
        threadId: `thread_${i}`,
        createdAt: new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString(),
        status: i === 4 ? 'running' : i % 2 === 0 ? 'completed' : 'failed',
      }),
    );
  }
  const selected = selectTasksToArchive(tasks, 2);
  assert.deepStrictEqual(
    selected.map((t) => t.threadId).sort(),
    ['thread_0', 'thread_1', 'thread_2'],
  );
});

test('TaskStore.archiveTask strips rounds, logs and persists the digest', () => {
  const dir = makeTempDir('bridge-archive-store-');
  const store = new TaskStore(dir);
  const thread = makeThread({ status: 'completed' });
  thread.artifactDir = path.join(dir, 'tasks', thread.threadId);
  store.upsertTask(thread);
  assert.ok(store.getTask(thread.threadId)?.finalSummary);

  const logsDir = path.join(dir, 'tasks', 'thread_summary_1', 'worker_log');
  fs.mkdirSync(logsDir, { recursive: true });
  const logFile = path.join(logsDir, `${thread.threadId}_round_1.log`);
  fs.writeFileSync(logFile, 'raw stdio');
  const reviewerDir = path.join(thread.artifactDir!, 'reviewer_log');
  fs.mkdirSync(reviewerDir, { recursive: true });
  const reviewerLog = path.join(reviewerDir, `${thread.threadId}_round_1.reviewer.log`);
  fs.writeFileSync(reviewerLog, 'reviewer stdio');

  const archived = store.archiveTask(thread.threadId);
  assert.ok(archived?.archivedAt);
  assert.deepStrictEqual(archived?.rounds, []);
  assert.ok(archived?.finalSummary);
  assert.strictEqual(fs.existsSync(logFile), false);
  assert.strictEqual(fs.existsSync(reviewerLog), false);

  const reloaded = new TaskStore(dir);
  assert.ok(reloaded.getTask(thread.threadId)?.archivedAt, 'archive survives a reload');
  assert.deepStrictEqual(reloaded.getTask(thread.threadId)?.rounds, []);
  store.dispose();
  reloaded.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('TaskStore does not archive while a native session write lease is active', () => {
  const dir = makeTempDir('bridge-archive-active-native-');
  const store = new TaskStore(dir);
  const sessionId = '01a1acc2-877d-71e3-9bf1-5e668c1f2183';
  const thread = makeThread({ status: 'completed', receiverSessionId: sessionId, receiverSessionCaptured: true });
  thread.artifactDir = path.join(dir, 'tasks', thread.threadId);
  store.upsertTask(thread);
  const lock = new NativeSessionLock(dir);
  const lease = lock.acquire(thread.targetAgent, sessionId)!;
  assert.equal(store.archiveTask(thread.threadId), undefined);
  assert.equal(store.getTask(thread.threadId)?.archivedAt, undefined);
  lock.release(lease);
  assert.ok(store.archiveTask(thread.threadId)?.archivedAt);
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('TaskArchiveService archives terminal tasks beyond the retention window', async () => {
  const dir = makeTempDir('bridge-archive-service-');
  const store = new TaskStore(dir);
  for (let i = 0; i < 6; i++) {
    store.upsertTask(
      makeThread({
        threadId: `thread_service_${i}`,
        artifactDir: path.join(dir, 'tasks', `thread_service_${i}`),
        createdAt: new Date(Date.UTC(2026, 0, 1, 0, i)).toISOString(),
        status: 'completed',
      }),
    );
  }
  const service = new TaskArchiveService(store, { keepRecent: 3 });
  const archived = await service.run();
  assert.deepStrictEqual(archived.sort(), [
    'thread_service_0',
    'thread_service_1',
    'thread_service_2',
  ]);
  const hot = store.getAllTasks().filter((t) => !t.archivedAt);
  assert.strictEqual(hot.length, 3);
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('TaskArchiveService uses a durable cleanup reservation for completed v2 tasks', async () => {
  const dir = makeTempDir('bridge-archive-v2-');
  const threadId = 'thread_archive_v2';
  const artifactDir = path.join(dir, 'tasks', threadId);
  fs.mkdirSync(path.join(artifactDir, 'worker_log'), { recursive: true });
  fs.writeFileSync(path.join(artifactDir, 'worker_log', `${threadId}_round_1.log`), 'worker output');
  const store = new TaskStore(dir);
  store.upsertTask(makeThread({
    threadId,
    artifactDir,
    status: 'completed',
    execution: {
      version: 2,
      revision: 1,
      operationId: 'merge_1',
      phase: 'merge',
      round: 1,
      generation: 1,
      attempt: 1,
      autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    },
  }));

  const archived = await new TaskArchiveService(store, { keepRecent: 0 }).run();
  assert.deepStrictEqual(archived, [threadId]);
  const result = store.getTask(threadId)!;
  assert.ok(result.archivedAt);
  assert.deepStrictEqual(result.rounds, []);
  assert.equal(result.execution?.version, 2);
  assert.equal(result.cleanup, undefined);
  assert.ok(fs.existsSync(path.join(artifactDir, 'Archive', `${threadId}_archive.tar.gz`)));
  assert.ok(fs.existsSync(path.join(artifactDir, 'Archive', `${threadId}_archive.json`)));
  assert.equal(fs.existsSync(path.join(artifactDir, 'worker_log', `${threadId}_round_1.log`)), false);
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('removeTaskArtifacts removes only that task bridge files', () => {
  const dir = makeTempDir('bridge-archive-artifacts-');
  const logsDir = path.join(dir, 'tasks', 'thread_a', 'worker_log');
  fs.mkdirSync(logsDir, { recursive: true });
  fs.writeFileSync(path.join(logsDir, 'thread_a_round_1.log'), 'a');
  fs.writeFileSync(path.join(logsDir, 'thread_a_round_1.output.md'), 'a');
  fs.writeFileSync(path.join(logsDir, 'thread_b_round_1.log'), 'b');
  const removed = removeTaskArtifacts('thread_a', dir, { artifactDir: path.dirname(logsDir) });
  assert.strictEqual(removed, 2);
  assert.strictEqual(fs.existsSync(path.join(logsDir, 'thread_b_round_1.log')), true);
  fs.rmSync(dir, { recursive: true, force: true });
});
