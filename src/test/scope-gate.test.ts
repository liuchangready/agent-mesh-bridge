import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as os from 'os';
import * as path from 'path';

import { describeScopeConflict, findScopeConflicts, knownTaskFiles } from '../bridge/file-scope';
import { DelegationThread } from '../bridge/types';

const NOW = Date.parse('2026-10-20T12:00:00Z');
const RECENT = new Date(NOW - 60_000).toISOString();
const ANCIENT = new Date(NOW - 3 * 24 * 60 * 60 * 1000).toISOString();

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_a',
    title: 'Task A',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'queued',
    currentRound: 1,
    createdAt: RECENT,
    updatedAt: RECENT,
    receiverSessionId: 'session-a',
    callerSessionId: 'caller-a',
    workspacePath: path.join(os.tmpdir(), 'bridge-workspace-repo'),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [],
    ...overrides,
  };
}

const completedRound = (touched: string[]) => ({
  round: 1,
  type: 'initial' as const,
  prompt: 'work',
  status: 'completed' as const,
  durationMs: 1,
  timestamp: RECENT,
  touchedFiles: touched,
});

describe('file-scope gate - one file belongs to one task', () => {
  test('blocks a candidate that declares a file another open task declared', () => {
    const candidate = makeThread({ threadId: 'thread_b', expectedFiles: ['src/shared/Foo.kt'] });
    const owner = makeThread({
      threadId: 'thread_a',
      status: 'waiting_review',
      expectedFiles: ['src/shared/Foo.kt'],
    });

    const conflict = findScopeConflicts(candidate, [owner], NOW);
    assert.ok(conflict);
    assert.strictEqual(conflict!.threadId, 'thread_a');
    assert.deepStrictEqual(conflict!.files, ['src/shared/Foo.kt']);
    assert.match(describeScopeConflict(conflict!), /thread_a/);
  });

  test('blocks on files a task actually touched, even without a declared scope', () => {
    const candidate = makeThread({
      threadId: 'thread_b',
      expectedFiles: ['src/shared/Bar.kt'],
    });
    const owner = makeThread({
      threadId: 'thread_a',
      status: 'completed',
      expectedFiles: [],
      rounds: [completedRound(['src/shared/Bar.kt'])],
    });

    const conflict = findScopeConflicts(candidate, [owner], NOW);
    assert.ok(conflict, 'measured touchedFiles must protect files the owner never declared');
    assert.strictEqual(conflict!.threadId, 'thread_a');
  });

  test('releases the files once the owner is收口 with a merge request', () => {
    const candidate = makeThread({ threadId: 'thread_b', expectedFiles: ['src/shared/Foo.kt'] });
    const owner = makeThread({
      threadId: 'thread_a',
      status: 'completed',
      expectedFiles: ['src/shared/Foo.kt'],
      mergeRequest: {
        status: 'synced',
        url: 'https://example.com/mr/1',
        updatedAt: RECENT,
        by: 'codex',
      } as any,
    });

    assert.strictEqual(findScopeConflicts(candidate, [owner], NOW), undefined);
  });

  test('a failed task and an ancient unmerged task do not block forever', () => {
    const candidate = makeThread({ threadId: 'thread_b', expectedFiles: ['src/shared/Foo.kt'] });
    const failed = makeThread({
      threadId: 'thread_failed',
      status: 'failed',
      expectedFiles: ['src/shared/Foo.kt'],
    });
    const ancient = makeThread({
      threadId: 'thread_old',
      status: 'completed',
      expectedFiles: ['src/shared/Foo.kt'],
      updatedAt: ANCIENT,
    });

    assert.strictEqual(findScopeConflicts(candidate, [failed], NOW), undefined);
    assert.strictEqual(findScopeConflicts(candidate, [ancient], NOW), undefined);
  });

  test('same task, override flag and undeclared candidates are never blocked', () => {
    const owner = makeThread({ threadId: 'thread_a', expectedFiles: ['src/shared/Foo.kt'] });

    assert.strictEqual(
      findScopeConflicts(
        makeThread({ threadId: 'thread_a', expectedFiles: ['src/shared/Foo.kt'] }),
        [owner],
        NOW,
      ),
      undefined,
    );
    assert.strictEqual(
      findScopeConflicts(
        makeThread({
          threadId: 'thread_b',
          expectedFiles: ['src/shared/Foo.kt'],
          scopeOverride: true,
        }),
        [owner],
        NOW,
      ),
      undefined,
    );
    assert.strictEqual(
      findScopeConflicts(makeThread({ threadId: 'thread_b', expectedFiles: [] }), [owner], NOW),
      undefined,
      'an undeclared candidate cannot be checked up front; its own touchedFiles protect later tasks',
    );
  });

  test('knownTaskFiles unions the declared scope with every round measurement', () => {
    const thread = makeThread({
      expectedFiles: ['src/a.kt'],
      rounds: [
        completedRound(['src/a.kt', 'src/b.kt']),
        {
          round: 2,
          type: 'rework',
          prompt: 'more',
          status: 'running',
          durationMs: 0,
          timestamp: RECENT,
          touchedFiles: ['src/c.kt'],
        },
      ],
    });

    assert.deepStrictEqual(knownTaskFiles(thread).sort(), ['src/a.kt', 'src/b.kt', 'src/c.kt']);
  });
});
