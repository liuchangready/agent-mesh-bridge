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
import { TaskSupervisor } from '../bridge/task-supervisor';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { ReworkController } from '../bridge/rework-controller';
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
    threadId: 'thread_test_0001',
    title: 'Round invariant probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'reworking',
    currentRound,
    createdAt: '2026-10-19T00:00:00.000Z',
    updatedAt: '2026-10-19T00:00:00.000Z',
    receiverSessionId: 'receiver-session-0001',
    workspacePath: os.tmpdir(),
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

describe('Round invariant - ReworkController refuses to overlap rounds', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let reworkController: ReworkController;
  let runner: AutonomousRunner;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-invariant-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    runner = new AutonomousRunner(detector);
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    // Never spawn a real caller CLI from a unit test.
    callbackManager.notifyCallerOfFailure = async () => false;
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector);
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('requestRework throws while a round is still running', async () => {
    taskStore.upsertTask(makeThread([makeRound(1, 'running', 'initial')], 1));

    await assert.rejects(
      () => reworkController.requestRework({ threadId: 'thread_test_0001', feedback: 'more work' }),
      (err: Error) => {
        assert.match(err.message, /still running/);
        assert.match(err.message, /one round in flight/);
        return true;
      },
    );

    // Nothing was minted.
    const thread = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(thread.rounds.length, 1);
    assert.strictEqual(getInFlightRounds(thread).length, 1);
  });

  test('requestRework succeeds once the previous round is finalized', async () => {
    taskStore.upsertTask(makeThread([makeRound(1, 'completed', 'initial')], 1));

    const result = await reworkController.requestRework({
      threadId: 'thread_test_0001',
      feedback: 'polish it',
    });

    assert.strictEqual(result.round, 2);
    const thread = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(getInFlightRounds(thread).length, 1, 'exactly one round in flight');
    assert.strictEqual(findInFlightRound(thread)?.round, 2);
  });

  test('requestRework numbers monotonically after a retry rewound currentRound', async () => {
    // Ledger after retryFailedRound(): rounds 1..5 exist but currentRound is 3.
    taskStore.upsertTask(
      makeThread(
        [
          makeRound(1, 'completed', 'initial'),
          makeRound(2, 'completed'),
          makeRound(3, 'failed'),
          makeRound(4, 'failed'),
          makeRound(5, 'failed'),
        ],
        3,
      ),
    );

    const result = await reworkController.requestRework({
      threadId: 'thread_test_0001',
      feedback: 'one more pass',
    });

    assert.strictEqual(result.round, 6, 'must not reuse an existing round number');
    const thread = taskStore.getTask('thread_test_0001')!;
    const numbers = thread.rounds.map((r) => r.round);
    assert.strictEqual(new Set(numbers).size, numbers.length, 'no duplicate round numbers');
  });

  test('retryFailedRound throws while another round is running', async () => {
    taskStore.upsertTask(
      makeThread([makeRound(1, 'failed', 'initial'), makeRound(2, 'running')], 2),
    );

    await assert.rejects(
      () => reworkController.retryFailedRound('thread_test_0001'),
      (err: Error) => {
        assert.match(err.message, /still running/);
        return true;
      },
    );
  });

  test('approveTask throws while a round is still running', () => {
    taskStore.upsertTask(makeThread([makeRound(1, 'running', 'initial')], 1));

    assert.throws(
      () => reworkController.approveTask({ threadId: 'thread_test_0001', remarks: 'looks good' }),
      (err: Error) => {
        assert.match(err.message, /still running/);
        return true;
      },
    );

    const thread = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(thread.status, 'reworking', 'must not be marked completed');
    assert.strictEqual(thread.rounds.length, 1, 'no approval round appended');
  });

  test('approveTask is idempotent once the thread is completed', () => {
    taskStore.upsertTask(makeThread([makeRound(1, 'completed', 'initial')], 1));

    const first = reworkController.approveTask({ threadId: 'thread_test_0001', remarks: 'pass' });
    assert.strictEqual(first.status, 'COMPLETED');
    const afterFirst = taskStore.getTask('thread_test_0001')!;
    const roundsAfterFirst = afterFirst.rounds.length;
    assert.strictEqual(afterFirst.status, 'completed');

    const second = reworkController.approveTask({
      threadId: 'thread_test_0001',
      remarks: 'pass again',
    });
    assert.strictEqual(second.status, 'COMPLETED');
    const afterSecond = taskStore.getTask('thread_test_0001')!;
    assert.strictEqual(
      afterSecond.rounds.length,
      roundsAfterFirst,
      'a repeated approval must not append another round',
    );
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

describe('Round invariant - supervisor repairs and guards', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let supervisor: TaskSupervisor;
  let runner: AutonomousRunner;
  let executedPrompts: string[];

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-sup-inv-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    runner = new AutonomousRunner(detector);
    executedPrompts = [];
    runner.execute = async (options) => {
      executedPrompts.push(options.prompt);
      return {
        exitCode: 0,
        stdout: 'ok',
        stderr: '',
        extractedOutput: 'ok',
        durationMs: 1,
      };
    };
    const callbackManager = new CallbackManager(detector);
    supervisor = new TaskSupervisor(taskStore, runner, callbackManager, detector);
  });

  afterEach(() => {
    supervisor.dispose();
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('normalizeRoundIntegrity keeps the highest running round and finalizes the rest', () => {
    // Mirrors the real D-07 ledger: rounds 3, 4 and 5 all marked running, with
    // round 3 and round 4 each duplicated.
    const thread = makeThread(
      [
        makeRound(1, 'failed', 'initial'),
        makeRound(2, 'completed'),
        makeRound(3, 'running'),
        makeRound(4, 'completed'),
        makeRound(3, 'running'),
        makeRound(4, 'running'),
        makeRound(5, 'running'),
      ],
      5,
    );
    // Repair before persisting, so the supervisor's own file watcher cannot
    // race this call and repair the ledger first.
    const repaired = supervisor.normalizeRoundIntegrity(thread);
    assert.strictEqual(repaired, true, 'corrupt ledger should be repaired');

    const running = getInFlightRounds(thread);
    assert.strictEqual(running.length, 1, 'exactly one round may stay in flight');
    assert.strictEqual(running[0].round, 5, 'the highest round wins');

    const repairedRounds = thread.rounds.filter((r) =>
      (r.error || '').includes('already in flight'),
    );
    assert.strictEqual(repairedRounds.length, 3, 'the three overlapping rounds were finalized');
    assert.ok(
      repairedRounds.every((r) => r.status === 'failed'),
      'each repaired round is finalized as failed',
    );
    assert.strictEqual(thread.rounds[0].error, undefined, 'pre-existing failures are untouched');
    assert.strictEqual(thread.rounds[3].status, 'completed', 'untouched rounds keep their status');

    // Repair is persisted.
    taskStore.loadFromDisk();
    assert.strictEqual(getInFlightRounds(taskStore.getTask('thread_test_0001')!).length, 1);
  });

  test('normalizeRoundIntegrity is a no-op for a healthy ledger', () => {
    const thread = makeThread([makeRound(1, 'completed', 'initial'), makeRound(2, 'running')], 2);
    taskStore.upsertTask(thread);
    assert.strictEqual(supervisor.normalizeRoundIntegrity(thread), false);
  });

  test('executeThreadRound refuses to start a round while another is in flight', async () => {
    const thread = makeThread([makeRound(3, 'running'), makeRound(5, 'running')], 5);
    taskStore.upsertTask(thread);

    // Ask it to run round 3 while round 5 is still in flight.
    await supervisor.executeThreadRound(thread, thread.rounds[0]);

    // The supervisor's own file-watch may legitimately launch the live round (5);
    // round 3 must never run alongside it.
    assert.ok(
      !executedPrompts.some((p) => p.includes('prompt for round 3')),
      `round 3 must not start while round 5 is in flight (executed: ${executedPrompts.join(' | ') || 'none'})`,
    );
  });
});
