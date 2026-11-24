import { testWorkspace } from './workspace-fixture';
import { test } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-bootstrap-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import {
  buildReviewerBootstrapPrompt,
  clearReviewerBootstrapToken,
  findReviewerBootstrapByHostPid,
  findSolePendingBootstrap,
  readReviewerBootstrapToken,
  recordReviewerBootstrapHostPid,
  writeReviewerBootstrapToken,
} from '../bridge/reviewer-bootstrap';
import { TaskStore } from '../bridge/task-store';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import { DelegationThread, isReviewerBootstrapPending } from '../bridge/types';
import { ensureTaskDataDir } from '../bridge/task-data-dir';

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function makeDedicatedThread(threadId: string, overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId,
    title: 'Fix the parser',
    initiatorAgent: 'codex',
    targetAgent: 'codex',
    status: 'queued',
    currentRound: 1,
    createdAt: '2026-11-01T00:00:00.000Z',
    updatedAt: '2026-11-01T00:00:00.000Z',
    receiverSessionId: 'receiver-session-1',
    workspace: testWorkspace(makeTempDir('bridge-bootstrap-ws-')),
    reviewFlow: 'dedicated',
    reviewerModel: 'gpt-6-pro',
    targetModel: 'gpt-6-flash',
    reviewBootstrap: { phase: 'pending', attempts: 0 },
    summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 0 },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'do the work',
        status: 'running',
        durationMs: 0,
        timestamp: '2026-11-01T00:00:00.000Z',
      },
    ],
    ...overrides,
  };
}

test('bootstrap token round-trips and clears', () => {
  const dir = makeTempDir('bridge-bootstrap-token-');
  assert.strictEqual(
    writeReviewerBootstrapToken(
      { threadId: 'thread_t', token: 'secret', agent: 'codex', createdAt: 'now' },
      dir,
    ),
    true,
  );
  assert.strictEqual(readReviewerBootstrapToken('thread_t', dir)?.token, 'secret');
  clearReviewerBootstrapToken('thread_t', dir);
  assert.strictEqual(readReviewerBootstrapToken('thread_t', dir), undefined);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a bootstrap is identifiable by the launched reviewer pid, without any env', () => {
  const dir = makeTempDir('bridge-bootstrap-pid-');
  const createdAt = new Date().toISOString();
  writeReviewerBootstrapToken(
    { threadId: 'thread_pid', token: 'secret-pid', agent: 'codex', createdAt },
    dir,
  );
  // No pid yet: nothing can be matched.
  assert.strictEqual(findReviewerBootstrapByHostPid(4242, dir), undefined);

  recordReviewerBootstrapHostPid('thread_pid', 4242, dir);
  assert.strictEqual(readReviewerBootstrapToken('thread_pid', dir)?.hostPid, 4242);
  assert.strictEqual(findReviewerBootstrapByHostPid(4242, dir)?.threadId, 'thread_pid');
  // Another process must not inherit the record.
  assert.strictEqual(findReviewerBootstrapByHostPid(9999, dir), undefined);
  // A pid-less lookup never matches.
  assert.strictEqual(findReviewerBootstrapByHostPid(undefined, dir), undefined);

  clearReviewerBootstrapToken('thread_pid', dir);
  assert.strictEqual(findReviewerBootstrapByHostPid(4242, dir), undefined);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('pid lookup follows the ledger artifactDir after taskDataRoot changes, and keeps legacy records', () => {
  const dir = makeTempDir('bridge-bootstrap-pid-root-switch-');
  const store = new TaskStore(dir);
  try {
    saveBridgePolicy({ taskDataRoot: 'old-root' }, dir);
    const threadId = 'thread_pinned_bootstrap';
    const dirs = ensureTaskDataDir(threadId, dir);
    const thread = makeDedicatedThread(threadId, {
      artifactDir: dirs.artifactDir,
      taskDataDir: dirs.artifactDir,
      reviewBootstrap: { phase: 'launching', attempts: 1 },
    });
    store.upsertTask(thread);
    writeReviewerBootstrapToken(
      {
        threadId,
        token: 'probe',
        agent: 'codex',
        createdAt: new Date().toISOString(),
        hostPid: 4242,
      },
      dir,
      dirs.artifactDir,
    );

    // Rebuild the reading side after the policy points at a different root.
    saveBridgePolicy({ taskDataRoot: 'new-root' }, dir);
    const reader = new TaskStore(dir);
    try {
      assert.strictEqual(
        findReviewerBootstrapByHostPid(4242, dir, reader)?.threadId,
        threadId,
        'a policy change must not hide the credential stored under the pinned artifactDir',
      );
    } finally {
      reader.dispose();
    }

    // Legacy/global records keep the pre-unified lookup path.
    writeReviewerBootstrapToken(
      {
        threadId: 'thread_legacy_pid',
        token: 'legacy-probe',
        agent: 'codex',
        createdAt: new Date().toISOString(),
        hostPid: 4343,
      },
      dir,
    );
    assert.strictEqual(
      findReviewerBootstrapByHostPid(4343, dir, store)?.threadId,
      'thread_legacy_pid',
    );
  } finally {
    store.dispose();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('a stale launch record cannot be claimed by a reused pid', () => {
  const dir = makeTempDir('bridge-bootstrap-stale-');
  writeReviewerBootstrapToken(
    {
      threadId: 'thread_stale',
      token: 'secret-stale',
      agent: 'codex',
      createdAt: new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(),
      hostPid: 5151,
    },
    dir,
  );
  assert.strictEqual(findReviewerBootstrapByHostPid(5151, dir), undefined);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a failed reviewer bootstrap is terminal, not "still starting"', () => {
  const pending = makeDedicatedThread('thread_phase_pending');
  const confirmed = makeDedicatedThread('thread_phase_confirmed');
  confirmed.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
  const captured = makeDedicatedThread('thread_phase_captured');
  captured.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
  captured.reviewerSessionCaptured = true;
  captured.reviewerSessionId = 'reviewer-session-captured';
  captured.callerSessionId = 'reviewer-session-captured';
  const failed = makeDedicatedThread('thread_phase_failed', { status: 'failed' });
  failed.reviewBootstrap = { phase: 'failed', attempts: 1, lastError: 'boom' };
  const legacy = makeDedicatedThread('thread_phase_legacy');
  legacy.reviewFlow = 'legacy';
  legacy.reviewBootstrap = undefined;

  assert.strictEqual(isReviewerBootstrapPending(pending), true);
  assert.strictEqual(isReviewerBootstrapPending(confirmed), true, 'dispatch confirmation alone must not start developer work');
  assert.strictEqual(isReviewerBootstrapPending(captured), false, 'the captured reviewer session completes the handoff');
  // A `failed` bootstrap can never be confirmed (its one-shot token is gone and
  // dispatch_task refuses it), so calling it "pending" left the supervisor
  // flipping the task running <-> queued on every pump, forever.
  assert.strictEqual(isReviewerBootstrapPending(failed), true);
  assert.strictEqual(isReviewerBootstrapPending(legacy), false);
});

test('the bootstrap prompt carries the one-time credential', () => {
  const thread = makeDedicatedThread('thread_prompt_credential');
  const prompt = buildReviewerBootstrapPrompt(thread, 'tok-abc-123');
  assert.match(prompt, /"reviewBootstrap"/);
  assert.ok(prompt.includes(`"threadId": "${thread.threadId}"`), prompt);
  assert.ok(prompt.includes(`"token": "tok-abc-123"`), prompt);
  assert.match(prompt, /Do NOT pass/);
});

test('the last-resort finder answers only when one bootstrap can be meant', () => {
  const dir = makeTempDir('bridge-bootstrap-sole-');
  const store = new TaskStore(dir);
  const first = makeDedicatedThread('thread_sole_a');
  store.upsertTask(first);
  writeReviewerBootstrapToken(
    { threadId: first.threadId, token: 't-a', agent: 'codex', createdAt: new Date().toISOString() },
    dir,
  );

  assert.strictEqual(findSolePendingBootstrap({ agent: 'codex', taskStore: store }, dir)?.threadId, first.threadId);
  assert.strictEqual(
    findSolePendingBootstrap(
      { agent: 'codex', workspacePath: first.workspace.executionPath, taskStore: store },
      dir,
    )?.threadId,
    first.threadId,
  );
  // A different agent must never inherit another agent's reviewer turn.
  assert.strictEqual(findSolePendingBootstrap({ agent: 'claude_code', taskStore: store }, dir), undefined);

  const second = makeDedicatedThread('thread_sole_b');
  store.upsertTask(second);
  writeReviewerBootstrapToken(
    { threadId: second.threadId, token: 't-b', agent: 'codex', createdAt: new Date().toISOString() },
    dir,
  );
  // Two pending bootstraps for one agent: ambiguous, so no claim.
  assert.strictEqual(findSolePendingBootstrap({ agent: 'codex', taskStore: store }, dir), undefined);
  // An exact workspace match still identifies the right one.
  assert.strictEqual(
    findSolePendingBootstrap(
      { agent: 'codex', workspacePath: second.workspace.executionPath, taskStore: store },
      dir,
    )?.threadId,
    second.threadId,
  );

  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('bootstrap includes only the reviewer template snapshot', () => {
  const template = { id: 't', version: '1', label: 'T', summary: 'S', workerInstructions: 'WORKER SECRET', reviewerInstructions: 'REVIEWER REQUIREMENT', sourceFileName: 't.md', selectedAt: 'now' };
  const prompt = buildReviewerBootstrapPrompt(makeDedicatedThread('thread_template_bootstrap', { taskTemplate: template }), 'token');
  assert.ok(prompt.includes(template.reviewerInstructions)); assert.ok(!prompt.includes(template.workerInstructions));
  assert.equal(prompt.split(template.reviewerInstructions).length - 1, 1);
  assert.ok(!buildReviewerBootstrapPrompt(makeDedicatedThread('thread_legacy'), 'token').includes(template.reviewerInstructions));
});
