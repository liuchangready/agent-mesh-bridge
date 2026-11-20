import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { DelegationThread, enterTaskFailure, clearTaskFailure, isMergePending } from '../bridge/types';
import { TaskStore, normalizeThread } from '../bridge/task-store';
import { archiveThreadData, selectTasksToArchive, ensureFinalSummary } from '../bridge/task-summary';
import { progressSignature } from '../bridge/caller-poll-guard';
import { testWorkspace } from './workspace-fixture';
import { CallbackManager } from '../bridge/callback-manager';
import { AgentDetector } from '../bridge/agent-detector';

function task(root: string): DelegationThread {
  const at = '2026-01-01T00:00:00.000Z';
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + ('thread_failure_revision')),
    callerSessionId: ('reviewer-' + ('thread_failure_revision')), threadId: 'thread_failure_revision', title: 'test', initiatorAgent: 'codex', targetAgent: 'claude_code',
    status: 'running', currentRound: 1, createdAt: at, updatedAt: at, receiverSessionId: 'receiver',
    artifactDir: path.join(root, 'tasks', 'thread_failure_revision'),
    workspace: testWorkspace(root), summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', status: 'running', durationMs: 0, timestamp: at }] };
}

test('legacy failures and summaries normalize without inventing events', () => {
  const root = os.tmpdir();
  for (const [old, stage] of [['review_failed', 'review'], ['merge_failed', 'merge'], ['failed', 'execution']] as const) {
    const item = task(root); (item as any).status = old;
    item.reviewFailure = { kind: 'retries_exhausted', reason: 'old review', at: item.updatedAt, round: 1 };
    normalizeThread(item);
    assert.equal(item.status, 'failed'); assert.equal(item.failure?.stage, stage);
    assert.equal(item.failureRevision, undefined); assert.equal(item.updatedAt, item.createdAt);
    assert.equal(item.rounds.length, 1);
  }
  const bootstrap = task(root); bootstrap.status = 'failed'; bootstrap.reviewBootstrap = { phase: 'failed', attempts: 1, lastError: 'token missing' };
  normalizeThread(bootstrap); assert.equal(bootstrap.failure?.stage, 'bootstrap'); assert.equal(bootstrap.failure?.reason, 'token missing');
  const explicit = task(root); (explicit as any).status = 'merge_failed';
  explicit.failure = { stage: 'review', reason: 'stale reason', at: explicit.createdAt };
  explicit.mergeHistory = [{ id: '1', number: 1, at: explicit.updatedAt, phase: 'merge', result: 'failed', error: 'merge error' }];
  normalizeThread(explicit); assert.equal(explicit.failure?.stage, 'merge'); assert.equal(explicit.failure?.reason, 'merge error');
  const current = task(root); current.status = 'failed'; current.failure = { stage: 'review', reason: 'current', at: current.updatedAt };
  current.reviewFailure = { kind: 'retries_exhausted', reason: 'historical', at: current.updatedAt, round: 1 };
  normalizeThread(current); assert.equal(current.failure?.reason, 'current'); assert.equal(current.failure?.stage, 'review');
  const legacy = task(root); (legacy as any).status = 'reworking'; normalizeThread(legacy); assert.equal(legacy.status, 'running');
  (legacy as any).finalSummary = { kind: 'review_failed', at: legacy.createdAt, summary: 'saved' };
  legacy.archivedAt = legacy.updatedAt; legacy.rounds = []; normalizeThread(legacy);
  assert.equal(legacy.finalSummary?.kind, 'failed'); assert.equal(legacy.finalSummary?.failureStage, 'review'); assert.equal(legacy.finalSummary?.summary, 'saved');
});

test('legacy review and merge states normalize according to the persisted integration phase', () => {
  const review = task(os.tmpdir()); (review as any).status = 'waiting_review';
  normalizeThread(review); assert.equal(review.status, 'reviewing');
  for (const [phase, status] of [['waiting', 'queued'], ['merging', 'merging'], ['conflict', 'reviewing']] as const) {
    const item = task(os.tmpdir()); (item as any).status = 'waiting_merge';
    item.worktree = { path: os.tmpdir(), mainPath: os.tmpdir(), integration: { phase, updatedAt: item.updatedAt } };
    item.reviewFlow = 'legacy'; item.callerSessionId = 'original-session';
    item.worktree.deliveryTarget = { branch: 'master', ref: 'refs/heads/master', repositoryPath: os.tmpdir(), baseCommit: 'base' };
    normalizeThread(item); assert.equal(item.status, status);
    assert.equal(item.reviewFlow, 'legacy'); assert.equal(item.callerSessionId, 'original-session');
    assert.equal(item.worktree.deliveryTarget?.ref, 'refs/heads/master');
    if (phase === 'waiting') assert.equal(item.queuePhase, 'merge');
  }
});

test('failure revisions protect persisted failure and recovered state from stale writers and callbacks', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-failure-revision-'));
  const store = new TaskStore(root);
  try {
    const initial = task(root); store.upsertTask(initial);
    const stale = structuredClone(initial);
    const failed = structuredClone(initial);
    enterTaskFailure(failed, { stage: 'execution', reason: 'execution error', at: failed.updatedAt, round: 1 });
    store.upsertTask(failed);
    store.upsertTask(stale); assert.equal(store.getTask(initial.threadId)?.failure?.reason, 'execution error');
    const conflict = structuredClone(store.getTask(initial.threadId)!);
    conflict.failure = { stage: 'merge', reason: 'stale merge', at: conflict.updatedAt };
    store.upsertTask(conflict); assert.equal(store.getTask(initial.threadId)?.failure?.stage, 'execution');
    const oldFailure = structuredClone(store.getTask(initial.threadId)!);
    const recovered = structuredClone(oldFailure); clearTaskFailure(recovered); recovered.status = 'running';
    recovered.rounds[0].timestamp = '2026-01-02T00:00:00.000Z'; store.upsertTask(recovered);
    store.upsertTask(oldFailure); assert.equal(store.getTask(initial.threadId)?.status, 'running');
    assert.equal(store.getTask(initial.threadId)?.failureRevision, 2);
    assert.equal(store.updateRound(initial.threadId, 1, { status: 'failed', error: 'late' }, 'failed', undefined, undefined, undefined,
      { failureRevision: 0, roundTimestamp: initial.rounds[0].timestamp }), undefined);
    assert.equal(store.getTask(initial.threadId)?.status, 'running');
    const next = structuredClone(store.getTask(initial.threadId)!); next.currentRound = 2;
    next.rounds.push({ ...next.rounds[0], round: 2 }); store.upsertTask(next);
    store.updateRound(initial.threadId, 1, { status: 'failed', error: 'old round' }, 'failed');
    assert.equal(store.getTask(initial.threadId)?.status, 'running');
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('merge failures cannot be archived even without worktree metadata and filters preserve stage', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-failure-retention-')); const store = new TaskStore(root);
  try {
    const item = task(root); enterTaskFailure(item, { stage: 'merge', reason: 'conflict', at: item.updatedAt }); store.upsertTask(item);
    assert.equal(archiveThreadData(structuredClone(item)), false); assert.deepEqual(selectTasksToArchive([item], 0), []);
    assert.equal(store.archiveTask(item.threadId), undefined);
    const incomplete = structuredClone(item); incomplete.worktree = { path: '', mainPath: root };
    assert.equal(isMergePending(incomplete), true); assert.equal(archiveThreadData(incomplete), false);
    assert.equal(store.listTasks(10, 'active').length, 1); assert.equal(store.listTasks(10, 'merge_failed').length, 1);
    assert.equal(store.listTasks(10, 'review_failed').length, 0); assert.equal(store.listTasks(10, 'failed')[0].status, 'failed');
    assert.equal(ensureFinalSummary(item), false); const signature = progressSignature(item);
    assert.equal(item.finalSummary?.error, 'conflict');
    const revision = item.failureRevision!; clearTaskFailure(item); item.status = 'merging';
    enterTaskFailure(item, { stage: 'merge', reason: 'second failure', at: item.updatedAt }); ensureFinalSummary(item);
    assert.ok(item.failureRevision! > revision); assert.notEqual(progressSignature(item), signature); assert.equal(item.finalSummary?.error, 'second failure');
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('a delayed review outcome cannot overwrite a later failure generation', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stale-review-')); const store = new TaskStore(root);
  try {
    const item = task(root); item.status = 'reviewing'; item.rounds[0].status = 'completed';
    store.upsertTask(item); const wakeSnapshot = structuredClone(item);
    const newer = structuredClone(item); enterTaskFailure(newer, { stage: 'review', reason: 'first review', at: newer.updatedAt, round: 1 });
    clearTaskFailure(newer); newer.status = 'reviewing'; store.upsertTask(newer);
    const callback = new CallbackManager(new AgentDetector(), root); callback.setTaskStore(store);
    callback.finalizeWake(wakeSnapshot, wakeSnapshot.rounds[0], 'reviewer', { exitCode: 1, error: 'late review' });
    const saved = store.getTask(item.threadId)!;
    assert.equal(saved.status, 'reviewing'); assert.equal(saved.failureRevision, 2); assert.equal(saved.reviewWake, undefined);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('an old writer cannot restore rounds after archival', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-archived-stale-')); const store = new TaskStore(root);
  try {
    const item = task(root); item.status = 'completed'; item.rounds[0].status = 'completed'; store.upsertTask(item);
    const stale = structuredClone(item);
    assert.ok(store.archiveTask(item.threadId));
    store.upsertTask(stale);
    const saved = store.getTask(item.threadId)!;
    assert.ok(saved.archivedAt); assert.deepEqual(saved.rounds, []);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('ordinary execution-failure notifications reject other failure stages', async () => {
  const callback = new CallbackManager(new AgentDetector(), os.tmpdir());
  for (const stage of ['bootstrap', 'review', 'merge'] as const) {
    const item = task(os.tmpdir()); enterTaskFailure(item, { stage, reason: 'failure', at: item.updatedAt, round: 1 });
    assert.equal(await callback.notifyCallerOfFailure(item, 1), false);
    assert.equal(item.failureNotifications, undefined);
  }
});

test('queued failure wake from an old revision cannot wake a new failure', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stale-wake-')); const store = new TaskStore(root);
  try {
    const item = task(root); enterTaskFailure(item, { stage: 'execution', reason: 'first', at: item.updatedAt, round: 1 });
    clearTaskFailure(item); item.status = 'running'; enterTaskFailure(item, { stage: 'execution', reason: 'second', at: item.updatedAt, round: 1 });
    store.upsertTask(item); const callback = new CallbackManager(new AgentDetector(), root); callback.setTaskStore(store);
    let calls = 0; callback.notifyCallerOfFailure = async () => { calls++; return true; };
    (callback as any).wakeQueue.enqueue('reviewer', { threadId: item.threadId, round: 1, kind: 'failure', failureRevision: 1, enqueuedAt: Date.now() });
    callback.drainNextWake('reviewer'); assert.equal(calls, 0); assert.equal(store.getTask(item.threadId)?.failure?.reason, 'second');
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('repeated reporting of the same failure does not create a new revision', () => {
  const item = task(os.tmpdir());
  enterTaskFailure(item, { stage: 'execution', reason: 'failed', at: item.createdAt, round: 1 });
  enterTaskFailure(item, { stage: 'execution', reason: 'failed', at: item.updatedAt + 'later', round: 1 });
  assert.equal(item.failureRevision, 1); assert.equal(item.failure?.at, item.createdAt);
});

test('persisted local merge wins over a stale failure even with a higher control revision', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merged-priority-')); const store = new TaskStore(root);
  try {
    const item = task(root);
    item.worktree = { path: root, mainPath: root, baseCommit: 'base', headCommit: 'head', integration: { phase: 'waiting', updatedAt: item.updatedAt } };
    item.status = 'merging'; store.upsertTask(item);
    const stale = structuredClone(item);
    const merged = structuredClone(item); merged.status = 'completed'; merged.worktree!.integration = { phase: 'merged', updatedAt: item.updatedAt };
    store.upsertTask(merged);
    enterTaskFailure(stale, { stage: 'merge', reason: 'late failure', at: stale.updatedAt });
    stale.mergeControl = { revision: 10, retryGeneration: 0, consecutiveRetries: 6 };
    store.upsertTask(stale);
    const saved = store.getTask(item.threadId)!;
    assert.equal(saved.status, 'completed'); assert.equal(saved.worktree?.integration?.phase, 'merged'); assert.equal(saved.failure, undefined);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});
