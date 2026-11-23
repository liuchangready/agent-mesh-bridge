import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { testWorkspace } from './workspace-fixture';
import { DelegationThread } from '../bridge/types';
import { failureFingerprint, noteMergeFailure, resetMergeBudget } from '../bridge/merge-recovery';
import { TaskStore } from '../bridge/task-store';
import { ReworkController } from '../bridge/rework-controller';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { archiveCompletedTaskLogs, isTerminalThreadStatus, removeTaskArtifacts, selectTasksToArchive } from '../bridge/task-summary';
import * as zlib from 'node:zlib';
import { ReviewWakeScheduler } from '../bridge/review-wake-scheduler';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskControlService } from '../bridge/task-control';

function task(root: string): DelegationThread {
  const now = new Date().toISOString();
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: 'reviewer-session', threadId: 'thread_merge_decision', title: 'merge', initiatorAgent: 'codex', targetAgent: 'claude_code',
    status: 'reviewing', currentRound: 1, createdAt: now, updatedAt: now,
    receiverSessionId: 'receiver-session', callerSessionId: 'reviewer-session', workspace: testWorkspace(root),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', output: 'done', status: 'completed', durationMs: 0, timestamp: now }],
    worktree: { path: root, mainPath: root, approvedCommit: 'candidate', approvedSnapshot: { commit: 'candidate', tree: 'tree', sourceCommit: 'source' },
      integration: { phase: 'blocked', error: 'conflict', updatedAt: now } } };
}

test('merge failures count the sixth retry, reset on a different fingerprint and on a new generation', () => {
  const thread = task(os.tmpdir());
  for (let i = 0; i <= 5; i++) {
    const result = noteMergeFailure(thread, 'conflict', 'CONFLICT', 'same failure', ['a.txt']);
    assert.equal(result.consecutiveRetries, i); assert.equal(thread.status, 'reviewing');
  }
  assert.equal(noteMergeFailure(thread, 'conflict', 'CONFLICT', 'same failure', ['a.txt']).consecutiveRetries, 6);
  assert.equal(thread.status, 'failed'); assert.equal(thread.failure?.stage, 'merge'); assert.equal(thread.failure?.reason, 'same failure'); assert.equal(thread.failureRevision, 1); assert.equal(isTerminalThreadStatus(thread.status), true);
  resetMergeBudget(thread); thread.status = 'reviewing';
  assert.equal(noteMergeFailure(thread, 'conflict', 'CONFLICT', 'same failure', ['a.txt']).consecutiveRetries, 0);
  assert.equal(noteMergeFailure(thread, 'conflict', 'CONFLICT', 'same failure', ['b.txt']).consecutiveRetries, 0);
  assert.equal(thread.mergeControl!.retryGeneration, 1);
});

test('failure fingerprints ignore temporary paths and object ids but retain the failing paths', () => {
  const first = failureFingerprint('conflict', 'CONFLICT', `failed /tmp/one/file at ${'a'.repeat(40)}`, ['shared.txt']);
  const second = failureFingerprint('conflict', 'CONFLICT', `failed /tmp/two/file at ${'b'.repeat(40)}`, ['shared.txt']);
  assert.equal(first, second);
  assert.notEqual(first, failureFingerprint('conflict', 'CONFLICT', `failed /tmp/two/file at ${'b'.repeat(40)}`, ['other.txt']));
});

test('merge decisions append immutable question and answer records and wake the same session', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-decision-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  const sessions: string[] = []; callback.notifyCaller = async (thread) => { sessions.push(thread.callerSessionId!); return true; };
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  t.after(() => { controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  store.upsertTask(task(root));
  const stale = structuredClone(store.getTask('thread_merge_decision')!);
  const first = await controller.requestMergeDecision('thread_merge_decision', 'Which fix?', 'context', 'choose A', ['a.txt']);
  assert.equal(store.getTask(first.threadId)!.status, 'wait_decision');
  stale.updatedAt = new Date(Date.now() + 60_000).toISOString(); store.upsertTask(stale);
  assert.equal(store.getTask(first.threadId)!.status, 'wait_decision');
  const savedQuestion = JSON.stringify(store.getTask(first.threadId)!.mergeHistory![0]);
  controller.submitMergeDecision(first.threadId, 'Use A\nand retain B');
  const second = await controller.requestMergeDecision(first.threadId, 'Which test?', 'more context', 'run integration');
  controller.submitMergeDecision(first.threadId, 'Run it');
  await new Promise((resolve) => setTimeout(resolve, 25));
  const stored = store.getTask(first.threadId)!;
  assert.equal(stored.status, 'reviewing'); assert.equal(stored.callerSessionId, 'reviewer-session');
  assert.equal(JSON.stringify(stored.mergeHistory![0]), savedQuestion);
  assert.equal(stored.mergeHistory!.length, 4); assert.notEqual(first.decisionId, second.decisionId);
  assert.deepEqual(sessions, ['reviewer-session', 'reviewer-session']);
  assert.equal(stored.currentRound, 1); assert.equal(stored.rounds.length, 1);
});

test('a review decision resumes the same reviewer without creating a development round', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-review-decision-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  const sessions: string[] = []; callback.notifyCaller = async (thread) => { sessions.push(thread.callerSessionId!); return true; };
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  t.after(() => { controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  const item = task(root); item.worktree = undefined; store.upsertTask(item);
  controller.requestMergeDecision(item.threadId, 'Which approach?', 'review context', 'A');
  assert.equal(store.getTask(item.threadId)?.mergeControl?.decisionStage, 'review');
  controller.submitMergeDecision(item.threadId, 'A');
  await new Promise((resolve) => setTimeout(resolve, 25));
  assert.equal(store.getTask(item.threadId)?.status, 'reviewing');
  assert.equal(store.getTask(item.threadId)?.rounds.length, 1);
  assert.deepEqual(sessions, ['reviewer-session']);
});

test('retry_merge preserves the task identities and history while starting a new budget', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-retry-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  callback.notifyCaller = async () => true;
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  const thread = task(root); thread.status = 'failed'; thread.failure = { stage: 'merge', reason: 'conflict', at: thread.updatedAt }; thread.failureRevision = 1;
  thread.mergeControl = { revision: 7, retryGeneration: 2, consecutiveRetries: 6, failureFingerprint: 'old' };
  thread.mergeHistory = [{ id: 'old', number: 1, at: thread.updatedAt, phase: 'conflict', result: 'failed' }];
  store.upsertTask(thread);
  const result = controller.retryMerge(thread.threadId); const saved = store.getTask(thread.threadId)!;
  assert.equal(result.retryGeneration, 3); assert.equal(saved.status, 'reviewing');
  assert.equal(saved.receiverSessionId, thread.receiverSessionId); assert.equal(saved.callerSessionId, thread.callerSessionId);
  assert.equal(saved.worktree!.approvedCommit, 'candidate'); assert.equal(saved.mergeHistory!.length, 2);
  controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true });
});

test('pending stash metadata recovery survives stale writes and pause/resume without waking a reviewer', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-stash-recovery-retry-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  let wakes = 0; callback.notifyCaller = async () => { wakes++; return true; };
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  const thread = task(root); store.upsertTask(thread); const stale = structuredClone(thread);
  thread.worktree!.integration!.stashRecoveryPending = true; thread.worktree!.integration!.stashRecoveryLockHeld = true; store.upsertTask(thread);
  stale.updatedAt = new Date(Date.now() + 60_000).toISOString(); stale.worktree!.integration!.updatedAt = stale.updatedAt;
  stale.worktree!.approvedCommit = 'stale-candidate'; store.upsertTask(stale);
  assert.equal(store.getTask(thread.threadId)!.worktree!.integration!.stashRecoveryLockHeld, true);
  assert.equal(store.getTask(thread.threadId)!.worktree!.approvedCommit, 'candidate');
  await assert.rejects(() => controller.requestRework({ threadId: thread.threadId, feedback: 'change it' }), /stash metadata recovery/i);
  assert.throws(() => controller.approveTask({ threadId: thread.threadId }), /stash metadata recovery/i);
  await assert.rejects(() => controller.requestMergeDecision(thread.threadId, 'Choose?', '', ''), /stash metadata recovery/i);
  const control = new TaskControlService(store, undefined, undefined, root);
  assert.equal((await control.pauseTask(thread.threadId, 'pause', 'test')).status, 'PAUSED');
  await controller.resumePausedTask(thread.threadId);
  assert.equal(store.getTask(thread.threadId)!.status, 'reviewing');
  assert.equal(store.getTask(thread.threadId)!.worktree!.integration!.stashRecoveryLockHeld, true);
  const saved = store.getTask(thread.threadId)!;
  assert.equal(wakes, 0);
  assert.equal(saved.worktree!.integration!.phase, 'blocked'); assert.equal(saved.worktree!.integration!.stashRecoveryPending, true);
  assert.equal(saved.callerSessionId, thread.callerSessionId); assert.equal(saved.receiverSessionId, thread.receiverSessionId);
  controller.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true });
});

test('actual merge reviewer failures consume the budget, but waiting for a wake does not', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-review-budget-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  callback.setTaskStore(store); const initial = task(root); store.upsertTask(initial);
  t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  for (let n = 0; n <= 5; n++) {
    const current = store.getTask(initial.threadId)!;
    callback.finalizeWake(current, current.rounds[0], current.callerSessionId!, { exitCode: 1, output: 'process failed' });
    assert.equal(store.getTask(initial.threadId)!.mergeControl!.consecutiveRetries, n);
    assert.equal(store.getTask(initial.threadId)!.status, 'reviewing');
  }
  const pending = store.getTask(initial.threadId)!;
  pending.reviewWake!.autoRetryAt = new Date(1).toISOString(); store.upsertTask(pending);
  callback.notifyCaller = async () => false;
  const scheduler = new ReviewWakeScheduler(store, callback, { now: () => 2_000, isReviewerActive: () => false });
  assert.equal(await scheduler.runDue(), 0);
  assert.equal(store.getTask(initial.threadId)!.mergeControl!.consecutiveRetries, 5);
  const current = store.getTask(initial.threadId)!;
  callback.finalizeWake(current, current.rounds[0], current.callerSessionId!, { exitCode: 1, output: 'process failed' });
  assert.equal(store.getTask(initial.threadId)!.status, 'failed'); assert.equal(store.getTask(initial.threadId)!.failure?.stage, 'merge'); assert.equal(store.getTask(initial.threadId)!.failure?.reason, 'process failed'); assert.equal(store.getTask(initial.threadId)!.failureRevision, 1);
  assert.equal(store.getTask(initial.threadId)!.mergeControl!.consecutiveRetries, 6);
  assert.equal(await scheduler.runDue(), 0); scheduler.dispose();
});

test('quota deferral does not consume a merge retry', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-quota-'));
  const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root);
  const thread = task(root); thread.mergeControl = { revision: 1, retryGeneration: 0, consecutiveRetries: 2, failureFingerprint: 'fingerprint' };
  thread.reviewWake = { round: 1, attempts: 3, lastAttemptAt: new Date(1).toISOString(), lastError: 'quota', autoRetryAt: new Date(2).toISOString() };
  store.upsertTask(thread); let launches = 0;
  callback.notifyCaller = async () => { launches++; return true; };
  (callback as any).getRateLimitTracker = () => ({ getRateLimitInfo: () => ({ isExhausted: true, resetsAt: new Date(600_000).toISOString() }) });
  const scheduler = new ReviewWakeScheduler(store, callback, { now: () => 10_000, isReviewerActive: () => false });
  t.after(() => { scheduler.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  assert.equal(await scheduler.runDue(), 0); assert.equal(launches, 0);
  const saved = store.getTask(thread.threadId)!;
  assert.equal(saved.mergeControl!.consecutiveRetries, 2);
  assert.ok(Date.parse(saved.reviewWake!.autoRetryAt!) > 600_000);
});

test('watchdog counts a disappeared launched merge reviewer once, but not an unlaunched wake', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-lost-reviewer-'));
  const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root);
  const thread = task(root); store.upsertTask(thread);
  const scheduler = new ReviewWakeScheduler(store, callback, { now: () => 100_000, stallTimeoutMs: 10_000, isReviewerActive: () => false });
  t.after(() => { scheduler.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  assert.equal(await scheduler.runWatchdog(), 0);
  const launched = store.getTask(thread.threadId)!;
  launched.mergeHistory = [{ id: 'launch', attemptId: 'launch', number: 1, at: new Date(1).toISOString(), phase: 'reviewer', result: 'started' }];
  store.upsertTask(launched);
  assert.equal(await scheduler.runWatchdog(), 1);
  const saved = store.getTask(thread.threadId)!;
  assert.equal(saved.mergeControl!.consecutiveRetries, 0);
  assert.equal(saved.mergeHistory!.at(-1)!.errorCode, 'WATCHDOG');
  assert.equal(saved.mergeHistory!.at(-1)!.attemptId, 'launch');
  assert.equal(saved.interruption!.kind, 'watchdog');
  assert.equal(await scheduler.runWatchdog(), 0);
});

test('restart schedules unlaunched merge reviews without consuming the merge retry budget', async () => {
  for (const scenario of ['conflict', 'validation_failed', 'blocked', 'legacy_conflict', 'completed_attempt', 'old_launch_before_handoff', 'old_gave_up_before_decision'] as const) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-review-restart-'));
    const initialStore = new TaskStore(root); const thread = task(root);
    const at = new Date(1).toISOString();
    thread.createdAt = at; thread.updatedAt = at; thread.rounds[0].timestamp = at;
    thread.reviewFlow = 'dedicated'; thread.reviewerSessionId = 'reviewer-session'; thread.reviewerSessionCaptured = true;
    thread.worktree!.mergePending = true;
    thread.worktree!.integration = { phase: scenario === 'validation_failed' ? 'validation_failed' : scenario === 'blocked' ? 'blocked' : 'conflict', updatedAt: at };
    thread.mergeControl = { revision: 1, retryGeneration: 0, consecutiveRetries: 3, failureFingerprint: 'existing' };
    const event = { id: 'conflict', number: 1, at, phase: thread.worktree!.integration.phase, result: 'failed' };
    thread.mergeHistory = [event];
    if (scenario === 'completed_attempt' || scenario === 'old_launch_before_handoff') {
      thread.mergeHistory = scenario === 'completed_attempt'
        ? [event, { id: 'launch', number: 2, at, phase: 'reviewer', result: 'started', attemptId: 'attempt' },
          { id: 'done', number: 3, at, phase: 'reviewer', result: 'completed', attemptId: 'attempt' }]
        : [{ id: 'launch', number: 1, at, phase: 'reviewer', result: 'started', attemptId: 'attempt' }, { ...event, number: 2 }];
      thread.reviewWake = { round: 1, attempts: 2, lastAttemptAt: at, lastError: 'previous', autoRetryAttempts: 1 };
    }
    if (scenario === 'old_gave_up_before_decision') {
      thread.mergeHistory = [event, { id: 'answer', decisionId: 'question', number: 2, at, phase: 'decision', result: 'answered', answer: 'A' }];
      thread.reviewWake = { round: 1, attempts: 2, lastAttemptAt: at, lastError: 'old failure', autoRetryAttempts: 1, gaveUp: true };
    }
    initialStore.upsertTask(thread); initialStore.dispose();
    if (scenario === 'legacy_conflict') {
      const file = path.join(root, 'threads', `${thread.threadId}.json`);
      const legacy = JSON.parse(fs.readFileSync(file, 'utf8')); legacy.status = 'waiting_merge';
      fs.writeFileSync(file, JSON.stringify(legacy));
    }
    const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root);
    let now = 3_600_000; let active = true; let calls = 0; const sessions: string[] = [];
    callback.notifyCaller = async (item) => { calls++; sessions.push(item.reviewerSessionId!); if (calls === 1) return false; active = true; return true; };
    const scheduler = new ReviewWakeScheduler(store, callback, { now: () => now, stallTimeoutMs: 10_000, isReviewerActive: () => active });
    try {
      assert.equal(await scheduler.runWatchdog(), 0, `${scenario}: an active reviewer must not be duplicated`);
      active = false;
      assert.equal(await scheduler.runWatchdog(), 1, scenario);
      const planned = store.getTask(thread.threadId)!;
      assert.equal(planned.status, 'reviewing'); assert.equal(planned.reviewWake?.kind, 'pending');
      assert.equal(planned.reviewWake?.attempts, thread.reviewWake?.attempts || 0);
      assert.equal(planned.reviewWake?.autoRetryAttempts, thread.reviewWake?.autoRetryAttempts || 0);
      assert.equal(planned.mergeControl?.consecutiveRetries, 3);
      assert.equal(await scheduler.runDue(), 0); assert.equal(calls, 1);
      assert.equal(store.getTask(thread.threadId)?.mergeControl?.consecutiveRetries, 3);
      now += 60_001;
      assert.equal(await scheduler.runDue(), 1); assert.deepEqual(sessions, ['reviewer-session', 'reviewer-session']);
      assert.equal(await scheduler.runWatchdog(), 0); assert.equal(await scheduler.runDue(), 0);
      const saved = store.getTask(thread.threadId)!;
      assert.equal(saved.worktree?.approvedCommit, 'candidate'); assert.equal(saved.worktree?.path, root);
      assert.equal(saved.receiverSessionId, thread.receiverSessionId); assert.equal(saved.callerSessionId, thread.callerSessionId);
      assert.equal(saved.mergeControl?.consecutiveRetries, 3); assert.equal(saved.rounds.length, 1);
      assert.equal(saved.interruptions?.length || 0, 0);
    } finally { scheduler.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
  }
});

test('an answered merge decision is recovered after a crash before the reviewer starts', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-decision-restart-'));
  const store = new TaskStore(root); const detector = new AgentDetector(); const callback = new CallbackManager(detector, root);
  callback.notifyCaller = async () => false;
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  const thread = task(root); thread.reviewFlow = 'dedicated'; thread.reviewerSessionId = 'reviewer-session'; thread.reviewerSessionCaptured = true;
  thread.reviewWake = { round: 1, attempts: 2, autoRetryAttempts: 1, lastAttemptAt: new Date(1).toISOString(), lastError: 'old' };
  store.upsertTask(thread);
  controller.requestMergeDecision(thread.threadId, 'Which fix?', 'context', 'A');
  controller.submitMergeDecision(thread.threadId, 'Choose A');
  assert.equal(store.getTask(thread.threadId)?.reviewWake, undefined);
  await new Promise(resolve => setTimeout(resolve, 20));
  controller.dispose(); store.dispose();
  const reopened = new TaskStore(root); const newCallback = new CallbackManager(detector, root); const sessions: string[] = [];
  newCallback.notifyCaller = async (item) => { sessions.push(item.reviewerSessionId!); return true; };
  const scheduler = new ReviewWakeScheduler(reopened, newCallback, { now: () => Date.now() + 3_600_000, stallTimeoutMs: 10_000, isReviewerActive: () => false });
  try {
    assert.equal(await scheduler.runWatchdog(), 1); assert.equal(await scheduler.runDue(), 1);
    assert.deepEqual(sessions, ['reviewer-session']);
    const saved = reopened.getTask(thread.threadId)!;
    assert.equal(saved.status, 'reviewing'); assert.equal(saved.worktree?.approvedCommit, 'candidate');
    assert.equal(saved.mergeHistory?.find(e => e.result === 'answered')?.answer, 'Choose A');
    assert.equal(saved.mergeControl?.consecutiveRetries, 0); assert.equal(saved.rounds.length, 1);
  } finally { scheduler.dispose(); reopened.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('a wake accepted before a second crash is recovered without charging an unstarted attempt', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-wake-restart-'));
  let store = new TaskStore(root); const thread = task(root); const old = new Date(1).toISOString();
  thread.createdAt = old; thread.updatedAt = old; thread.rounds[0].timestamp = old; thread.worktree!.integration!.updatedAt = old;
  store.upsertTask(thread); const callback = new CallbackManager(new AgentDetector(), root); let wakes = 0;
  callback.notifyCaller = async () => { wakes++; return true; };
  let now = Date.now();
  let scheduler = new ReviewWakeScheduler(store, callback, { now: () => now, stallTimeoutMs: 10_000, isReviewerActive: () => false });
  try {
    assert.equal(await scheduler.runWatchdog(), 1); assert.equal(await scheduler.runDue(), 1);
    assert.equal(store.getTask(thread.threadId)?.reviewWake?.autoRetryAt, undefined);
    scheduler.dispose(); store.dispose();
    store = new TaskStore(root); now = Date.now() + 11_000;
    scheduler = new ReviewWakeScheduler(store, callback, { now: () => now, stallTimeoutMs: 10_000, isReviewerActive: () => false });
    assert.equal(await scheduler.runWatchdog(), 1); assert.equal(await scheduler.runDue(), 1);
    assert.equal(wakes, 2);
    assert.equal(store.getTask(thread.threadId)?.reviewWake?.attempts, 0);
    assert.equal(store.getTask(thread.threadId)?.mergeControl?.consecutiveRetries, undefined);
  } finally { scheduler.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('an unrecoverable merge review is failed with a recorded review reason', async () => {
  for (const mode of ['missing_round', 'gave_up'] as const) {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-review-unrecoverable-'));
    const store = new TaskStore(root); const thread = task(root); const old = new Date(1).toISOString();
    thread.createdAt = old; thread.updatedAt = old; thread.worktree!.integration!.updatedAt = old;
    if (mode === 'missing_round') thread.rounds = [];
    else { thread.rounds[0].timestamp = old; thread.reviewWake = { round: 1, attempts: 3, lastAttemptAt: old, lastError: 'review unavailable', gaveUp: true }; }
    store.upsertTask(thread);
    const scheduler = new ReviewWakeScheduler(store, new CallbackManager(new AgentDetector(), root), { now: () => 3_600_000, stallTimeoutMs: 10_000, isReviewerActive: () => false });
    try {
      assert.equal(await scheduler.runWatchdog(), 1);
      const saved = store.getTask(thread.threadId)!;
      assert.equal(saved.status, 'failed'); assert.equal(saved.failure?.stage, 'review');
      assert.equal(saved.reviewFailure?.reason, saved.failure?.reason);
      assert.equal(saved.worktree?.approvedCommit, 'candidate');
    } finally { scheduler.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
  }
});

test('merge reviewer warnings after handoff and user pauses do not consume retry budget', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-warning-'));
  const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root); callback.setTaskStore(store);
  try {
    const original = task(root); store.upsertTask(original);
    const handedOff = structuredClone(original); handedOff.status = 'queued'; handedOff.queuePhase = 'merge'; handedOff.worktree!.integration!.phase = 'waiting';
    handedOff.worktree!.integration!.updatedAt = new Date(Date.now() + 100).toISOString(); store.upsertTask(handedOff);
    const assessment = new AutonomousRunner(new AgentDetector()).evaluateResponse({ exitCode: 0, stdout: 'done', stderr: '', chunkCount: 2,
      stdoutBytes: 4, stderrBytes: 0, maxGapMs: 200, gapThresholdMs: 100 });
    assert.equal(assessment.success, true); assert.equal(assessment.warnings?.[0].code, 'STREAM_GAP_EXCEEDED');
    callback.finalizeWake(original, original.rounds[0], original.callerSessionId!, { exitCode: 0, output: 'done', assessment });
    assert.equal(store.getTask(original.threadId)!.mergeControl, undefined);
    const paused = structuredClone(store.getTask(original.threadId)!); paused.status = 'paused'; store.upsertTask(paused);
    callback.finalizeWake(original, original.rounds[0], original.callerSessionId!, { exitCode: 1, output: 'stopped by user' });
    assert.equal(store.getTask(original.threadId)!.mergeControl, undefined);
    assert.equal(store.getTask(original.threadId)!.status, 'paused');
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('empty reviewer output after a merge handoff blocks integration and records assessment', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-empty-review-'));
  const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root); callback.setTaskStore(store);
  try {
    const original = task(root); store.upsertTask(original);
    const handedOff = structuredClone(original); handedOff.status = 'queued'; handedOff.queuePhase = 'merge'; handedOff.worktree!.integration!.phase = 'waiting';
    handedOff.worktree!.integration!.updatedAt = new Date(Date.now() + 100).toISOString(); store.upsertTask(handedOff);
    const assessment = new AutonomousRunner(new AgentDetector()).evaluateResponse({ exitCode: 0, stdout: '{"type":"status"}', stderr: '',
      chunkCount: 1, stdoutBytes: 17, stderrBytes: 0, maxGapMs: 0, gapThresholdMs: 100 });
    assert.equal(assessment.errorCode, 'EMPTY_OUTPUT');
    callback.finalizeWake(original, original.rounds[0], original.callerSessionId!, { exitCode: 0, output: '{"type":"status"}', assessment });
    const saved = store.getTask(original.threadId)!;
    assert.equal(saved.status, 'reviewing'); assert.equal(saved.worktree!.integration!.phase, 'blocked');
    assert.equal(saved.mergeHistory!.at(-1)!.assessment!.errorCode, 'EMPTY_OUTPUT');
    assert.equal(saved.mergeControl!.consecutiveRetries, 0);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('reviewer watchdog interruption is recorded without changing the worker round', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-watchdog-'));
  const store = new TaskStore(root); const callback = new CallbackManager(new AgentDetector(), root); callback.setTaskStore(store);
  try {
    const original = task(root); store.upsertTask(original);
    const interruption = { kind: 'watchdog' as const, reason: 'reviewer stalled', at: new Date().toISOString(), by: 'callback' };
    const assessment = new AutonomousRunner(new AgentDetector()).evaluateResponse({ exitCode: -1, stdout: '', stderr: '', chunkCount: 0,
      stdoutBytes: 0, stderrBytes: 0, maxGapMs: 0, gapThresholdMs: 100, interruption });
    callback.finalizeWake(original, original.rounds[0], original.callerSessionId!, { exitCode: -1, assessment, interruption });
    const saved = store.getTask(original.threadId)!;
    assert.equal(saved.interruptions?.at(-1)?.kind, 'watchdog');
    assert.equal(saved.mergeHistory?.at(-1)?.assessment?.errorCode, 'WATCHDOG');
    assert.equal(saved.rounds[0].status, 'completed'); assert.equal(saved.rounds[0].interruption, undefined);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('nonterminal merge states remain unfinished and are not selected for archive', () => {
  const root = os.tmpdir();
  for (const status of ['merging', 'wait_decision', 'failed'] as const) {
    const thread = task(root); thread.status = status;
    if (status === 'failed') thread.failure = { stage: 'merge', reason: 'conflict', at: thread.updatedAt };
    assert.equal(isTerminalThreadStatus(status), status === 'failed');
    assert.deepEqual(selectTasksToArchive([thread], 0), []);
  }
});

test('a ledger full of merge waiting states has no capacity eviction candidate', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-capacity-')); const store = new TaskStore(root);
  try {
    const states = ['merging', 'wait_decision', 'failed'] as const;
    const rows = new Map<string, DelegationThread>();
    for (let n = 0; n < 1000; n++) { const item = task(root); item.threadId = `thread_capacity_${n}`; item.status = states[n % states.length]; if (item.status === 'failed') item.failure = {stage: 'merge', reason: 'conflict', at: item.updatedAt}; rows.set(item.threadId, item); }
    (store as any).tasks = rows; (store as any).lastDirMtimeMs = Number.POSITIVE_INFINITY;
    assert.equal(store.canAcceptTask(), false);
  } finally { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('merge Git and validation registrations do not consume agent concurrency slots', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-slots-')); const registry = new TaskExecutionRegistry(root);
  try {
    const git = registry.begin('git-task', 1, 'merge-git');
    const validation = registry.begin('validation-task', 1, 'merge-validation');
    const reviewer = registry.begin('reviewer-task', 1, 'merge-reviewer');
    const developer = registry.begin('developer-task', 1, 'codex');
    assert.equal(registry.listOwners().length, 4);
    assert.deepEqual(registry.listAgentOwners().map((owner) => owner.threadId), ['developer-task']);
    registry.end(git); registry.end(validation); registry.end(reviewer); registry.end(developer);
  } finally { registry.dispose(); fs.rmSync(root, { recursive: true, force: true }); }
});

test('task archival includes merge history and complete merge log before cleanup', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-archive-'));
  try {
    const thread = task(root); thread.status = 'completed'; thread.worktree = undefined; thread.artifactDir = path.join(root, 'task');
    const logPath = path.join(thread.artifactDir, 'merge_log', `${thread.threadId}_merge_1.log`);
    fs.mkdirSync(path.dirname(logPath), { recursive: true }); fs.writeFileSync(logPath, 'full merge output');
    thread.mergeHistory = [{ id: 'attempt', number: 1, at: thread.updatedAt, phase: 'attempt', result: 'started', logPath }];
    const archive = archiveCompletedTaskLogs(thread, root);
    const content = zlib.gunzipSync(fs.readFileSync(archive.tarGzPaths[0])).toString('utf8');
    assert.match(content, /full merge output/); assert.match(content, /mergeHistory/);
    removeTaskArtifacts(thread.threadId, root, { artifactDir: thread.artifactDir, keepArchive: true });
    assert.equal(fs.existsSync(logPath), false); assert.equal(fs.existsSync(archive.tarGzPaths[0]), true);
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
