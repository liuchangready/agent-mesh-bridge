import { test, describe } from 'node:test';
import * as assert from 'node:assert';

import {
  CallerPollGuard,
  STOP_POLLING_INSTRUCTION,
  progressSignature,
} from '../bridge/caller-poll-guard';
import { DelegationThread, DelegationRound } from '../bridge/types';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  const round: DelegationRound = {
    round: 1,
    type: 'initial',
    prompt: 'p',
    status: 'running',
    durationMs: 0,
    timestamp: now,
  };
  return {
    threadId: 'thread_poll_1',
    title: 'poll guard',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'running',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'session-1',
    workspacePath: process.cwd(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [round],
    ...overrides,
  };
}

describe('progressSignature', () => {
  test('changes when the round actually progresses', () => {
    const base = makeThread();
    const sameProgress: DelegationThread = makeThread({
      updatedAt: new Date(Date.now() + 5000).toISOString(),
      summary: { lastPromptSnippet: 'x', lastOutputSnippet: 'y', totalDurationMs: 10 },
    });
    // Cosmetic fields (timestamps, snippets) must not look like progress.
    assert.strictEqual(progressSignature(base), progressSignature(sameProgress));

    const finished = makeThread();
    finished.rounds[0].status = 'completed';
    assert.notStrictEqual(progressSignature(base), progressSignature(finished));

    const moreRounds = makeThread();
    moreRounds.rounds.push({ ...moreRounds.rounds[0], round: 2, status: 'running' });
    assert.notStrictEqual(progressSignature(base), progressSignature(moreRounds));
  });
});

describe('CallerPollGuard', () => {
  test('allows the first read, starves an identical second read', () => {
    const guard = new CallerPollGuard();
    const thread = makeThread();
    const sig = progressSignature(thread);
    guard.noteDispatch(thread.threadId, sig);

    assert.deepStrictEqual(guard.check(thread.threadId, sig), { verdict: 'allow', reads: 1 });
    assert.deepStrictEqual(guard.check(thread.threadId, sig), { verdict: 'repeat', reads: 2 });
    assert.deepStrictEqual(guard.check(thread.threadId, sig), { verdict: 'repeat', reads: 3 });
  });

  test('counts reads per view, so one inspection may read details and logs', () => {
    const guard = new CallerPollGuard();
    const thread = makeThread();
    const sig = progressSignature(thread);
    guard.noteDispatch(thread.threadId, sig);

    // details #1 and logs #1 belong to the same inspection: both are allowed.
    assert.strictEqual(guard.check(thread.threadId, sig, 'details').verdict, 'allow');
    assert.strictEqual(guard.check(thread.threadId, sig, 'logs').verdict, 'allow');
    // Repeating either tool at the same progress is the polling loop.
    assert.strictEqual(guard.check(thread.threadId, sig, 'details').verdict, 'repeat');
    assert.strictEqual(guard.check(thread.threadId, sig, 'logs').verdict, 'repeat');
  });

  test('real progress restores the full payload', () => {
    const guard = new CallerPollGuard();
    const thread = makeThread();
    guard.noteDispatch(thread.threadId, progressSignature(thread));

    guard.check(thread.threadId, progressSignature(thread));
    assert.strictEqual(guard.check(thread.threadId, progressSignature(thread)).verdict, 'repeat');

    const progressed = makeThread();
    progressed.rounds[0].status = 'completed';
    progressed.status = 'waiting_review';
    const res = guard.check(thread.threadId, progressSignature(progressed));
    assert.strictEqual(res.verdict, 'allow');
    assert.strictEqual(res.reads, 1);
  });

  test('never guards threads this process did not dispatch', () => {
    const guard = new CallerPollGuard();
    const sig = progressSignature(makeThread());
    assert.strictEqual(guard.check('thread_someone_else', sig).verdict, 'allow');
    assert.strictEqual(guard.isGuarded('thread_someone_else'), false);
  });

  test('forget stops guarding a finished task', () => {
    const guard = new CallerPollGuard();
    const thread = makeThread();
    guard.noteDispatch(thread.threadId, progressSignature(thread));
    guard.forget(thread.threadId);
    assert.strictEqual(guard.isGuarded(thread.threadId), false);
    assert.strictEqual(guard.check(thread.threadId, progressSignature(thread)).verdict, 'allow');
  });

  test('the repeat payload carries a machine-readable stop directive', () => {
    const payload = CallerPollGuard.repeatPayload(makeThread(), 2) as Record<string, any>;
    assert.strictEqual(payload.repeatPoll, true);
    assert.strictEqual(payload.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
    assert.strictEqual(payload.stopInstruction, STOP_POLLING_INSTRUCTION);
    assert.strictEqual(payload.status, 'running');
    assert.match(payload.message, /No progress since your previous check/);
    // Deliberately small: no rounds, prompts, outputs or logs.
    assert.strictEqual(payload.rounds, undefined);
    assert.strictEqual(payload.recentLogs, undefined);
    assert.deepStrictEqual(payload.lastRound, { round: 1, status: 'running' });
  });
});
