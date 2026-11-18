import { testWorkspace } from './workspace-fixture';
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
      workspace: testWorkspace(tempDir),
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

  test('preserves task template snapshot when an old copy is saved after reload', () => {
    const now = new Date().toISOString();
    const snapshot = { id: 'programming', version: '1', label: 'Programming', summary: 'Code', workerInstructions: 'worker original', reviewerInstructions: 'reviewer original', sourceFileName: 'programming.md', selectedAt: now };
    const thread: DelegationThread = { threadId: 'thread_template_snapshot', title: 'Template', initiatorAgent: 'codex', targetAgent: 'claude_code', status: 'queued', currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session', workspace: testWorkspace(tempDir), rounds: [], summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 }, taskTemplate: snapshot };
    taskStore.upsertTask(thread);
    const stale = structuredClone(thread);
    delete stale.taskTemplate;
    const second = new TaskStore(tempDir);
    try {
      assert.deepStrictEqual(second.getTask(thread.threadId)?.taskTemplate, snapshot);
      second.upsertTask(stale);
      second.loadFromDisk();
      assert.deepStrictEqual(second.getTask(thread.threadId)?.taskTemplate, snapshot);
      const changed = structuredClone(stale); changed.taskTemplate = { ...snapshot, workerInstructions: 'drift', reviewerInstructions: 'drift' };
      second.upsertTask(changed);
      second.loadFromDisk();
      assert.deepStrictEqual(second.getTask(thread.threadId)?.taskTemplate, snapshot);
    } finally { second.dispose(); }
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
      workspace: testWorkspace(tempDir),
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
      'reviewing',
    );

    const updated = taskStore.getTask('thread_update_test');
    assert.ok(updated);
    assert.strictEqual(updated.status, 'reviewing');
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
      workspace: testWorkspace(tempDir),
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

  test('newer merge failure revokes approval and stale snapshots cannot restore it', () => {
    const now = new Date(Date.now() - 10_000).toISOString();
    const thread: DelegationThread = {
      threadId: 'thread_merge_failure', title: 'merge failure', initiatorAgent: 'codex', targetAgent: 'claude_code',
      status: 'merging', currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session', workspace: testWorkspace(tempDir),
      summary: { lastPromptSnippet: 'review', lastOutputSnippet: 'approved', totalDurationMs: 1 }, rounds: [],
      worktree: { path: tempDir, mainPath: tempDir, repositoryPath: tempDir, commonDir: tempDir, targetRef: 'refs/mesh/batch', taskBranch: 'mesh/task/x', baseCommit: 'base', headCommit: 'approved', approvedCommit: 'approved', integration: { phase: 'waiting', updatedAt: now } },
    };
    taskStore.upsertTask(thread);
    const stale = JSON.parse(JSON.stringify(thread)) as DelegationThread;
    const latest = taskStore.getTask(thread.threadId)!;
    latest.status = 'reviewing';
    latest.worktree!.approvedCommit = undefined;
    latest.worktree!.integration = { phase: 'validation_failed', error: 'combined test failed', updatedAt: new Date().toISOString() };
    taskStore.upsertTask(latest);
    taskStore.upsertTask(stale);
    const persisted = taskStore.getTask(thread.threadId)!;
    assert.equal(persisted.status, 'reviewing');
    assert.equal(persisted.worktree!.approvedCommit, undefined);
    assert.equal(persisted.worktree!.integration!.phase, 'validation_failed');
    assert.equal(persisted.worktree!.integration!.error, 'combined test failed');
  });

  test('should enforce strict 1,000 tasks cap with LRU truncation preserving active and unmerged worktree tasks', () => {
    // Pre-seed oldest active task + oldest unmerged worktree task + 993 completed tasks (total 995)
    const baseTime = Date.now();
    const threadsDir = path.join(tempDir, 'threads');
    fs.mkdirSync(threadsDir, { recursive: true });

    // 1. Oldest active task (timestamp -100s)
    const oldestActiveTime = new Date(baseTime - 100_000).toISOString();
    const oldestActiveTask: DelegationThread = {
      threadId: 'thread_oldest_active',
      title: 'Oldest Active Task',
      initiatorAgent: 'antigravity',
      targetAgent: 'codex',
      status: 'running',
      currentRound: 1,
      createdAt: oldestActiveTime,
      updatedAt: oldestActiveTime,
      receiverSessionId: 'sess_oldest_active',
      workspace: testWorkspace(tempDir),
      summary: { lastPromptSnippet: 'oldest active', lastOutputSnippet: 'oldest active', totalDurationMs: 0 },
      rounds: [],
    };
    fs.writeFileSync(path.join(threadsDir, 'thread_oldest_active.json'), JSON.stringify(oldestActiveTask, null, 2), 'utf8');

    // 2. Oldest unmerged worktree task (timestamp -90s, completed but integration.phase = 'waiting')
    const oldestUnmergedTime = new Date(baseTime - 90_000).toISOString();
    const oldestUnmergedPath = path.join(tempDir, 'worktree_oldest');
    const oldestUnmergedTask: DelegationThread = {
      threadId: 'thread_oldest_unmerged',
      title: 'Oldest Unmerged Task',
      initiatorAgent: 'antigravity',
      targetAgent: 'codex',
      status: 'completed',
      currentRound: 1,
      createdAt: oldestUnmergedTime,
      updatedAt: oldestUnmergedTime,
      receiverSessionId: 'sess_oldest_unmerged',
      workspace: testWorkspace(oldestUnmergedPath),
      worktree: {
        path: oldestUnmergedPath,
        mainPath: tempDir,
        repositoryPath: tempDir,
        commonDir: tempDir,
        targetRef: 'refs/heads/master',
        taskBranch: 'mesh/task/thread_oldest_unmerged',
        baseCommit: 'base_sha',
        headCommit: 'head_sha',
        integration: { phase: 'waiting', updatedAt: oldestUnmergedTime },
      },
      summary: { lastPromptSnippet: 'oldest unmerged', lastOutputSnippet: 'oldest unmerged', totalDurationMs: 0 },
      rounds: [],
    };
    fs.writeFileSync(path.join(threadsDir, 'thread_oldest_unmerged.json'), JSON.stringify(oldestUnmergedTask, null, 2), 'utf8');

    // 3. 993 completed tasks (thread_0 to thread_992, newer timestamps baseTime + i * 1000)
    for (let i = 0; i < 993; i++) {
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
        workspace: testWorkspace(tempDir),
        summary: {
          lastPromptSnippet: `Prompt ${i}`,
          lastOutputSnippet: `Output ${i}`,
          totalDurationMs: 1000,
        },
        rounds: [],
      };
      fs.writeFileSync(path.join(threadsDir, `thread_${i}.json`), JSON.stringify(t, null, 2), 'utf8');
    }

    // Load pre-seeded tasks into store (total 2 + 993 = 995)
    taskStore.loadFromDisk();
    assert.strictEqual(taskStore.count(), 995);

    // Upsert 55 tasks through real public upsertTask API (from 993 to 1047, total 995 + 55 = 1050)
    for (let i = 993; i < 1048; i++) {
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
        workspace: testWorkspace(tempDir),
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

    // Oldest active and unmerged worktree tasks MUST NEVER BE EVICTED despite being the oldest
    assert.ok(taskStore.getTask('thread_oldest_active'), 'Oldest active task must be preserved');
    assert.ok(taskStore.getTask('thread_oldest_unmerged'), 'Oldest unmerged worktree task must be preserved');

    // 50 oldest evictable completed tasks (thread_0 to thread_49) should have been evicted
    assert.strictEqual(taskStore.getTask('thread_0'), undefined, 'thread_0 should be evicted');
    assert.strictEqual(taskStore.getTask('thread_49'), undefined, 'thread_49 should be evicted');

    // Retained completed tasks
    assert.ok(taskStore.getTask('thread_50'), 'thread_50 should be retained');
    assert.ok(taskStore.getTask('thread_1047'), 'thread_1047 should be retained');

    // Verify durable reload persistence with a fresh TaskStore instance
    const reloaded = new TaskStore(tempDir);
    assert.strictEqual(reloaded.count(), MAX_STORED_TASKS);
    assert.ok(reloaded.getTask('thread_oldest_active'));
    assert.ok(reloaded.getTask('thread_oldest_unmerged'));
    assert.strictEqual(reloaded.getTask('thread_0'), undefined);
    assert.strictEqual(reloaded.getTask('thread_49'), undefined);
    assert.ok(reloaded.getTask('thread_50'));
    assert.ok(reloaded.getTask('thread_1047'));
    reloaded.dispose();
  });

  test('should reject new tasks and preserve ledger when 1,000 non-evictable tasks fill ledger', () => {
    const fullDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-full-'));
    try {
      const threadsDir = path.join(fullDir, 'threads');
      fs.mkdirSync(threadsDir, { recursive: true });
      const baseTime = Date.now();

      // Create 1,000 non-evictable tasks:
      // - 500 active/running tasks
      // - 500 completed tasks with unmerged worktrees (integration.phase = 'waiting')
      for (let i = 0; i < 500; i++) {
        const timeIso = new Date(baseTime + i * 1000).toISOString();
        const activeTask: DelegationThread = {
          threadId: `thread_active_${i}`,
          title: `Active #${i}`,
          initiatorAgent: 'antigravity',
          targetAgent: 'codex',
          status: 'running',
          currentRound: 1,
          createdAt: timeIso,
          updatedAt: timeIso,
          receiverSessionId: `sess_active_${i}`,
          workspace: testWorkspace(fullDir),
          summary: { lastPromptSnippet: 'active', lastOutputSnippet: 'active', totalDurationMs: 0 },
          rounds: [],
        };
        fs.writeFileSync(path.join(threadsDir, `thread_active_${i}.json`), JSON.stringify(activeTask, null, 2), 'utf8');
      }

      for (let i = 500; i < 1000; i++) {
        const timeIso = new Date(baseTime + i * 1000).toISOString();
        const wtPath = path.join(fullDir, `worktree_${i}`);
        const unmergedTask: DelegationThread = {
          threadId: `thread_unmerged_${i}`,
          title: `Unmerged #${i}`,
          initiatorAgent: 'antigravity',
          targetAgent: 'codex',
          status: 'completed',
          currentRound: 1,
          createdAt: timeIso,
          updatedAt: timeIso,
          receiverSessionId: `sess_unmerged_${i}`,
          workspace: testWorkspace(wtPath),
          worktree: {
            path: wtPath,
            mainPath: fullDir,
            repositoryPath: fullDir,
            commonDir: fullDir,
            targetRef: 'refs/heads/master',
            taskBranch: `mesh/task/thread_unmerged_${i}`,
            baseCommit: 'base_sha',
            headCommit: 'head_sha',
            integration: { phase: 'waiting', updatedAt: timeIso },
          },
          summary: { lastPromptSnippet: 'unmerged', lastOutputSnippet: 'unmerged', totalDurationMs: 0 },
          rounds: [],
        };
        fs.writeFileSync(path.join(threadsDir, `thread_unmerged_${i}.json`), JSON.stringify(unmergedTask, null, 2), 'utf8');
      }

      const fullStore = new TaskStore(fullDir);
      assert.strictEqual(fullStore.count(), 1000);
      assert.strictEqual(fullStore.canAcceptTask(), false, 'canAcceptTask must return false when all 1,000 tasks are non-evictable');

      // Attempting to upsert a new task must throw and refuse
      const overflowTask: DelegationThread = {
        threadId: 'thread_overflow',
        title: 'Overflow Task',
        initiatorAgent: 'antigravity',
        targetAgent: 'codex',
        status: 'completed',
        currentRound: 1,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        receiverSessionId: 'sess_overflow',
        workspace: testWorkspace(fullDir),
        summary: { lastPromptSnippet: 'overflow', lastOutputSnippet: 'overflow', totalDurationMs: 0 },
        rounds: [],
      };

      assert.throws(() => {
        fullStore.upsertTask(overflowTask);
      }, /ledger is full/i, 'upsertTask must throw when ledger is full of non-evictable tasks');

      // Ledger must not be corrupted and must not exceed 1000
      assert.strictEqual(fullStore.count(), 1000);
      assert.strictEqual(fullStore.getTask('thread_overflow'), undefined);

      // Updating an existing task must still succeed
      const existing = fullStore.getTask('thread_active_0')!;
      assert.ok(existing);
      existing.summary.lastOutputSnippet = 'updated snippet';
      fullStore.upsertTask(existing);
      assert.strictEqual(fullStore.getTask('thread_active_0')?.summary.lastOutputSnippet, 'updated snippet');
      assert.strictEqual(fullStore.count(), 1000);

      // Fresh reload verifies disk integrity
      const reloaded = new TaskStore(fullDir);
      assert.strictEqual(reloaded.count(), 1000);
      assert.strictEqual(reloaded.canAcceptTask(), false);
      assert.strictEqual(reloaded.getTask('thread_overflow'), undefined);
      assert.strictEqual(reloaded.getTask('thread_active_0')?.summary.lastOutputSnippet, 'updated snippet');
      reloaded.dispose();
      fullStore.dispose();
    } finally {
      try {
        fs.rmSync(fullDir, { recursive: true, force: true });
      } catch {}
    }
  });
});
