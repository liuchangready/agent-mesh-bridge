import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { TaskStore, MAX_STORED_TASKS } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';

describe('TaskStore Tests', () => {
  let tempDir: string;
  let taskStore: TaskStore;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-test-'));
    taskStore = new TaskStore(tempDir);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('should initialize with empty store when file does not exist', () => {
    assert.strictEqual(taskStore.count(), 0);
    assert.deepStrictEqual(taskStore.listTasks(), []);
  });

  test('should upsert, retrieve and persist task thread', () => {
    const thread: DelegationThread = {
      threadId: 'test_thread_1',
      title: 'Fix Buffer Overrun',
      initiatorAgent: 'antigravity',
      targetAgent: 'codex',
      status: 'running',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: 'uuid-1234',
      workspacePath: tempDir,
      summary: {
        lastPromptSnippet: 'Fix the bug',
        lastOutputSnippet: 'Running...',
        totalDurationMs: 0,
      },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Fix the bug',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
        },
      ],
    };

    taskStore.upsertTask(thread);
    assert.strictEqual(taskStore.count(), 1);

    const retrieved = taskStore.getTask('test_thread_1');
    assert.ok(retrieved);
    assert.strictEqual(retrieved.title, 'Fix Buffer Overrun');
    assert.strictEqual(retrieved.receiverSessionId, 'uuid-1234');

    // Create another TaskStore instance pointing to same directory to verify persistence
    const secondStore = new TaskStore(tempDir);
    assert.strictEqual(secondStore.count(), 1);
    const persisted = secondStore.getTask('test_thread_1');
    assert.ok(persisted);
    assert.strictEqual(persisted.title, 'Fix Buffer Overrun');
    secondStore.dispose();
  });

  test('should update round and refresh summary accurately', () => {
    const thread: DelegationThread = {
      threadId: 'thread_update_test',
      title: 'Implement TCP decode',
      initiatorAgent: 'antigravity',
      targetAgent: 'claude_code',
      status: 'running',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: 'sess-abc',
      workspacePath: tempDir,
      summary: {
        lastPromptSnippet: 'Original prompt',
        lastOutputSnippet: 'Running',
        totalDurationMs: 0,
      },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'Original prompt',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
        },
      ],
    };

    taskStore.upsertTask(thread);

    taskStore.updateRound(
      'thread_update_test',
      1,
      {
        output: 'Successfully implemented TCP decoder with 10 unit tests.',
        durationMs: 4200,
        status: 'completed',
      },
      'waiting_review',
    );

    const updated = taskStore.getTask('thread_update_test');
    assert.ok(updated);
    assert.strictEqual(updated.status, 'waiting_review');
    assert.strictEqual(updated.rounds[0].status, 'completed');
    assert.strictEqual(updated.rounds[0].durationMs, 4200);
    assert.ok(updated.summary.lastOutputSnippet.includes('Successfully implemented TCP decoder'));
    assert.strictEqual(updated.summary.totalDurationMs, 4200);
  });

  test('should persist the caller merge-request handoff', () => {
    const thread: DelegationThread = {
      threadId: 'thread_mr_test',
      title: 'MR handoff',
      initiatorAgent: 'codex',
      targetAgent: 'deepseek_harness',
      status: 'completed',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: 'sess-mr',
      workspacePath: tempDir,
      summary: {
        lastPromptSnippet: 'do work',
        lastOutputSnippet: 'approved',
        totalDurationMs: 10,
      },
      rounds: [],
    };
    taskStore.upsertTask(thread);

    const updated = taskStore.recordMergeRequest('thread_mr_test', {
      url: 'https://example.com/mr/7',
      branch: 'feat/mr',
      commit: 'abc123',
      remote: 'origin',
      status: 'synced',
      updatedAt: new Date().toISOString(),
    });
    assert.ok(updated);
    assert.strictEqual(updated!.mergeRequest?.url, 'https://example.com/mr/7');
    assert.strictEqual(taskStore.getTask('thread_mr_test')?.mergeRequest?.commit, 'abc123');

    const secondStore = new TaskStore(tempDir);
    assert.strictEqual(
      secondStore.getTask('thread_mr_test')?.mergeRequest?.branch,
      'feat/mr',
      'MR info must survive persistence',
    );
    secondStore.dispose();
  });

  test('should enforce strict 1,000 tasks cap with LRU truncation', () => {
    // Generate 1050 tasks
    const baseTime = Date.now();
    for (let i = 0; i < 1050; i++) {
      const timeIso = new Date(baseTime + i * 1000).toISOString();
      const t: DelegationThread = {
        threadId: `thread_${i}`,
        title: `Task #${i}`,
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: timeIso,
        updatedAt: timeIso,
        receiverSessionId: `sess_${i}`,
        workspacePath: tempDir,
        summary: {
          lastPromptSnippet: `Prompt ${i}`,
          lastOutputSnippet: `Output ${i}`,
          totalDurationMs: 1000,
        },
        rounds: [],
      };
      taskStore.upsertTask(t);
    }

    assert.strictEqual(taskStore.count(), MAX_STORED_TASKS);
    assert.strictEqual(taskStore.count(), 1000);

    // Oldest 50 (thread_0 to thread_49) should have been truncated
    assert.strictEqual(taskStore.getTask('thread_0'), undefined);
    assert.strictEqual(taskStore.getTask('thread_49'), undefined);
    // Newest tasks should exist
    assert.ok(taskStore.getTask('thread_1049'));
    assert.ok(taskStore.getTask('thread_50'));
  });
});
