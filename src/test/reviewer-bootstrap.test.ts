import { test } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-bootstrap-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import {
  ReviewerBootstrapService,
  buildReviewerBootstrapPrompt,
  clearReviewerBootstrapToken,
  findReviewerBootstrapByHostPid,
  findSolePendingBootstrap,
  readReviewerBootstrapToken,
  recordReviewerBootstrapHostPid,
  writeReviewerBootstrapToken,
} from '../bridge/reviewer-bootstrap';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { ExecutionGate } from '../bridge/execution-gate';
import { DelegationThread, isReviewerBootstrapPending } from '../bridge/types';
import { saveBridgePolicy } from '../bridge/bridge-policy';

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
    workspacePath: makeTempDir('bridge-bootstrap-ws-'),
    reviewFlow: 'dedicated',
    requesterSessionId: 'requester-session',
    reviewerModel: 'gpt-6-pro',
    targetModel: 'gpt-6-flash',
    reviewBootstrap: { phase: 'pending', attempts: 0 },
    expectedFiles: [],
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

function makeService(
  dir: string,
  store: TaskStore,
  runner: AutonomousRunner,
  onSettled?: (threadId: string) => void,
): ReviewerBootstrapService {
  return new ReviewerBootstrapService({
    taskStore: store,
    runner,
    detector: new AgentDetector(),
    executionGate: new ExecutionGate(dir),
    storageDir: dir,
    timeoutMs: 1000,
    onSettled,
  });
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

test('the launched reviewer pid is written before the turn runs', async () => {
  const dir = makeTempDir('bridge-bootstrap-childpid-');
  const store = new TaskStore(dir);
  const thread = makeDedicatedThread('thread_child_pid');
  store.upsertTask(thread);
  writeReviewerBootstrapToken(
    { threadId: thread.threadId, token: 'token-child', agent: 'codex', createdAt: new Date().toISOString() },
    dir,
  );

  const runner = new AutonomousRunner(new AgentDetector(), 5_000);
  runner.execute = async (options: any) => {
    options.onChildPid?.(7788);
    // The reviewer's dispatch_task call arrives through the bridge server.
    const fresh = store.getTask(thread.threadId)!;
    fresh.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
    store.upsertTask(fresh);
    return {
      exitCode: 0,
      stdout: 'dispatched',
      stderr: '',
      extractedOutput: 'dispatched',
      durationMs: 5,
      actualSessionId: 'reviewer-session-child',
    };
  };
  const service = makeService(dir, store, runner);
  await service.tick();
  service.stop();

  assert.strictEqual(store.getTask(thread.threadId)?.reviewBootstrap?.phase, 'confirmed');
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('pending dedicated task is bootstrapped by a fresh reviewer session', async () => {
  const dir = makeTempDir('bridge-bootstrap-ok-');
  const store = new TaskStore(dir);
  const thread = makeDedicatedThread('thread_bootstrap_ok');
  store.upsertTask(thread);
  writeReviewerBootstrapToken(
    { threadId: thread.threadId, token: 'token-1', agent: 'codex', createdAt: 'now' },
    dir,
  );

  const runner = new AutonomousRunner(new AgentDetector(), 5_000);
  const calls: any[] = [];
  runner.execute = async (options: any) => {
    calls.push(options);
    // Simulate the reviewer's dispatch_task call confirming the token.
    const fresh = store.getTask(thread.threadId)!;
    fresh.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
    store.upsertTask(fresh);
    return {
      exitCode: 0,
      stdout: 'dispatched',
      stderr: '',
      extractedOutput: 'dispatched',
      durationMs: 10,
      actualSessionId: 'reviewer-session-9',
    };
  };

  let settled = 0;
  const service = makeService(dir, store, runner, () => {
    settled += 1;
  });
  await service.tick();
  service.stop();

  assert.strictEqual(calls.length, 1);
  const options = calls[0];
  assert.strictEqual(options.agent, 'codex');
  assert.strictEqual(options.model, 'gpt-6-pro');
  assert.strictEqual(options.markAsReceiver, false);
  assert.strictEqual(options.extraEnv.MESH_REVIEW_BOOTSTRAP_TOKEN, 'token-1');
  assert.strictEqual(options.extraEnv.MESH_REVIEW_THREAD_ID, thread.threadId);

  const stored = store.getTask(thread.threadId)!;
  assert.strictEqual(stored.reviewBootstrap?.phase, 'confirmed');
  assert.strictEqual(stored.callerSessionId, 'reviewer-session-9');
  assert.strictEqual(stored.reviewerSessionId, 'reviewer-session-9');
  assert.strictEqual(stored.reviewerSessionCaptured, true);
  assert.strictEqual(stored.status, 'queued');
  assert.strictEqual(readReviewerBootstrapToken(thread.threadId, dir), undefined);
  assert.strictEqual(settled, 1);

  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a reviewer turn that never dispatches fails the task without starting a developer', async () => {
  const dir = makeTempDir('bridge-bootstrap-fail-');
  const store = new TaskStore(dir);
  const thread = makeDedicatedThread('thread_bootstrap_fail');
  store.upsertTask(thread);
  writeReviewerBootstrapToken(
    { threadId: thread.threadId, token: 'token-2', agent: 'codex', createdAt: 'now' },
    dir,
  );

  const runner = new AutonomousRunner(new AgentDetector(), 5_000);
  runner.execute = async () => ({
    exitCode: 0,
    stdout: 'I did not call anything',
    stderr: '',
    extractedOutput: 'I did not call anything',
    durationMs: 10,
  });
  const service = makeService(dir, store, runner);
  await service.tick();
  service.stop();

  const stored = store.getTask(thread.threadId)!;
  assert.strictEqual(stored.status, 'failed');
  assert.strictEqual(stored.reviewBootstrap?.phase, 'failed');
  assert.match(stored.reviewBootstrap?.lastError || '', /without calling dispatch_task/i);
  assert.strictEqual(stored.rounds[0].status, 'failed');
  assert.match(stored.finalSummary?.summary || '', /任务失败/);
  assert.strictEqual(stored.callerSessionId, undefined);

  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('a failed reviewer bootstrap is terminal, not "still starting"', () => {
  const pending = makeDedicatedThread('thread_phase_pending');
  const confirmed = makeDedicatedThread('thread_phase_confirmed');
  confirmed.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
  const failed = makeDedicatedThread('thread_phase_failed', { status: 'failed' });
  failed.reviewBootstrap = { phase: 'failed', attempts: 1, lastError: 'boom' };
  const legacy = makeDedicatedThread('thread_phase_legacy');
  legacy.reviewFlow = 'legacy';
  legacy.reviewBootstrap = undefined;

  assert.strictEqual(isReviewerBootstrapPending(pending), true);
  assert.strictEqual(isReviewerBootstrapPending(confirmed), false);
  // A `failed` bootstrap can never be confirmed (its one-shot token is gone and
  // dispatch_task refuses it), so calling it "pending" left the supervisor
  // flipping the task running <-> queued on every pump, forever.
  assert.strictEqual(isReviewerBootstrapPending(failed), false);
  assert.strictEqual(isReviewerBootstrapPending(legacy), false);
});

test('FIFO: an older unfinished task blocks the reviewer bootstrap', async () => {
  const dir = makeTempDir('bridge-bootstrap-fifo-');
  const store = new TaskStore(dir);
  const older = makeDedicatedThread('thread_older', {
    status: 'waiting_review',
    createdAt: '2026-10-31T23:00:00.000Z',
  });
  older.reviewFlow = 'legacy';
  older.reviewBootstrap = undefined;
  store.upsertTask(older);
  const younger = makeDedicatedThread('thread_younger', {
    createdAt: '2026-11-01T00:00:00.000Z',
  });
  store.upsertTask(younger);
  writeReviewerBootstrapToken(
    { threadId: younger.threadId, token: 'token-3', agent: 'codex', createdAt: 'now' },
    dir,
  );

  const runner = new AutonomousRunner(new AgentDetector(), 5_000);
  let executed = 0;
  runner.execute = async () => {
    executed += 1;
    return { exitCode: 0, stdout: '', stderr: '', extractedOutput: '', durationMs: 1 };
  };
  const service = makeService(dir, store, runner);
  await service.tick();
  service.stop();

  assert.strictEqual(executed, 0, 'the queue head must be dispatched first');
  assert.strictEqual(store.getTask(younger.threadId)?.reviewBootstrap?.phase, 'pending');
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('concurrency: a running task in another workspace does not block a pending bootstrap', async () => {
  const dir = makeTempDir('bridge-bootstrap-concurrent-');
  saveBridgePolicy(
    { concurrency: { enabled: true, maxConcurrentTasks: 4, maxConcurrentPerAgent: 3 } },
    dir,
  );
  const store = new TaskStore(dir);
  const otherWorkspace = makeTempDir('bridge-bootstrap-other-ws-');
  const older = makeDedicatedThread('thread_other_ws', {
    status: 'reworking',
    createdAt: '2026-10-31T23:00:00.000Z',
    workspacePath: otherWorkspace,
  });
  older.reviewFlow = 'legacy';
  older.reviewBootstrap = undefined;
  store.upsertTask(older);

  const target = makeDedicatedThread('thread_bootstrap_concurrent', {
    createdAt: '2026-11-01T00:00:00.000Z',
  });
  store.upsertTask(target);
  writeReviewerBootstrapToken(
    { threadId: target.threadId, token: 'token-4', agent: 'codex', createdAt: 'now' },
    dir,
  );

  const runner = new AutonomousRunner(new AgentDetector(), 5_000);
  const calls: any[] = [];
  runner.execute = async (options: any) => {
    calls.push(options);
    const fresh = store.getTask(target.threadId)!;
    fresh.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
    store.upsertTask(fresh);
    return {
      exitCode: 0,
      stdout: 'dispatched',
      stderr: '',
      extractedOutput: 'dispatched',
      durationMs: 1,
      actualSessionId: 'reviewer-concurrent',
    };
  };
  const service = makeService(dir, store, runner);
  await service.tick();
  service.stop();

  assert.strictEqual(calls.length, 1, 'the target workspace has its own execution slot');
  assert.strictEqual(store.getTask(target.threadId)?.reviewBootstrap?.phase, 'confirmed');
  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
  fs.rmSync(otherWorkspace, { recursive: true, force: true });
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
      { agent: 'codex', workspacePath: first.workspacePath, taskStore: store },
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
      { agent: 'codex', workspacePath: second.workspacePath, taskStore: store },
      dir,
    )?.threadId,
    second.threadId,
  );

  store.dispose();
  fs.rmSync(dir, { recursive: true, force: true });
});
