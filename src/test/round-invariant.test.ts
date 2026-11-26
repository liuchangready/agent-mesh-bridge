import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

// The supervisor heartbeat writes a global lock under os.homedir(). Node's test
// runner executes each test FILE in its own process, so redirecting the home
// directory here keeps this file from making `TaskSupervisor.isSupervisorActive()`
// true for the other, concurrently running test files.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import {
  DelegationThread,
  DelegationRound,
  findInFlightRound,
  getInFlightRounds,
  nextRoundNumber,
} from '../bridge/types';

function makeRound(
  round: number,
  status: DelegationRound['status'],
  type: DelegationRound['type'] = 'rework',
): DelegationRound {
  return {
    round,
    type,
    prompt: `prompt for round ${round}`,
    status,
    durationMs: 0,
    timestamp: '2026-10-19T00:00:00.000Z',
  };
}

function makeThread(rounds: DelegationRound[], currentRound: number): DelegationThread {
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + ('thread_test_0001')),
    callerSessionId: ('reviewer-' + ('thread_test_0001')),
    threadId: 'thread_test_0001',
    title: 'Round invariant probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'running',
    currentRound,
    createdAt: '2026-10-19T00:00:00.000Z',
    updatedAt: '2026-10-19T00:00:00.000Z',
    receiverSessionId: 'receiver-session-0001',
    workspace: testWorkspace(os.tmpdir()),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds,
  };
}

describe('Round invariant - pure helpers', () => {
  test('findInFlightRound returns undefined when the thread is idle', () => {
    const thread = makeThread([makeRound(1, 'completed'), makeRound(2, 'failed')], 2);
    assert.strictEqual(getInFlightRounds(thread).length, 0);
    assert.strictEqual(findInFlightRound(thread), undefined);
  });

  test('findInFlightRound returns the single running round', () => {
    const running = makeRound(2, 'running');
    const thread = makeThread([makeRound(1, 'completed'), running], 2);
    assert.strictEqual(findInFlightRound(thread), running);
  });

  test('findInFlightRound picks the highest round when several are running', () => {
    const thread = makeThread(
      [makeRound(3, 'running'), makeRound(4, 'completed'), makeRound(5, 'running')],
      5,
    );
    assert.strictEqual(findInFlightRound(thread)?.round, 5);
  });

  test('findInFlightRound keeps the newest entry when round numbers tie', () => {
    const first = makeRound(4, 'running');
    const second = makeRound(4, 'running');
    const thread = makeThread([first, second], 4);
    assert.strictEqual(findInFlightRound(thread), second);
  });

  test('nextRoundNumber is monotonic and never reuses an existing number', () => {
    assert.strictEqual(nextRoundNumber(makeThread([makeRound(1, 'completed')], 1)), 2);
    // A retry rewound currentRound to 3 while rounds 1..5 already exist.
    const rewound = makeThread(
      [
        makeRound(1, 'completed'),
        makeRound(2, 'completed'),
        makeRound(3, 'failed'),
        makeRound(4, 'failed'),
        makeRound(5, 'failed'),
      ],
      3,
    );
    assert.strictEqual(nextRoundNumber(rewound), 6);
  });
});

describe('Round invariant - TaskStore refuses a second running round', () => {
  let tempDir: string;
  let taskStore: TaskStore;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-store-inv-'));
    taskStore = new TaskStore(tempDir);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('updateRound will not append a running round while one is in flight', () => {
    taskStore.upsertTask(makeThread([makeRound(1, 'running', 'initial')], 1));

    const result = taskStore.updateRound('thread_test_0001', 2, {
      prompt: 'sneaky second round',
      status: 'running',
      type: 'rework',
    });

    assert.strictEqual(result, undefined);
    const thread = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(thread.rounds.length, 1, 'no second round appended');
    assert.strictEqual(getInFlightRounds(thread).length, 1);
  });

  test('updateRound updates the newest entry when round numbers are duplicated', () => {
    // Legacy ledger shape: round 4 appears twice and only the later one is live.
    const stale = makeRound(4, 'failed');
    const live = makeRound(4, 'running');
    taskStore.upsertTask(makeThread([stale, live], 4));

    taskStore.updateRound('thread_test_0001', 4, { output: 'live output', status: 'completed' });

    const thread = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(thread.rounds[0].output, undefined, 'stale duplicate untouched');
    assert.strictEqual(thread.rounds[1].output, 'live output', 'live duplicate updated');
  });
});
