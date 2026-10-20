import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-cb-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { CallbackManager } from '../bridge/callback-manager';
import { AgentDetector } from '../bridge/agent-detector';
import { DelegationRound, DelegationThread } from '../bridge/types';

describe('CallbackManager failure wake-ups', () => {
  const failedRound = (overrides: Partial<DelegationRound> = {}): DelegationRound => ({
    round: 2,
    type: 'rework',
    prompt: 'Implement the sleeve bloom extension',
    status: 'failed',
    durationMs: 510_176,
    timestamp: '2026-10-20T04:30:26.662Z',
    error: 'dsh: TRANSPORT: Connection error.',
    assessment: {
      success: false,
      errorCode: 'NONZERO_EXIT',
      exitCode: 1,
      stream: {
        chunkCount: 28,
        stdoutBytes: 29_827,
        stderrBytes: 34,
        maxGapMs: 118_233,
        gapThresholdMs: 600_000,
        hasActualOutput: true,
        continuous: true,
      },
      reason: 'dsh: TRANSPORT: Connection error.',
    },
    ...overrides,
  });

  const makeThread = (overrides: Partial<DelegationThread> = {}): DelegationThread => ({
    threadId: 'thread_cb_1',
    title: 'Implement e68faed extension',
    initiatorAgent: 'codex',
    targetAgent: 'deepseek_harness',
    status: 'failed',
    currentRound: 2,
    createdAt: '2026-10-20T03:17:38.487Z',
    updatedAt: '2026-10-20T04:39:07.796Z',
    receiverSessionId: 'session-8b5dc091-3d68-43d9-bd08-05b383c4b972',
    receiverSessionCaptured: true,
    callerSessionId: 'caller-codex-session-1',
    workspacePath: process.cwd(),
    expectedFiles: [],
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 857_740 },
    rounds: [
      { ...failedRound(), round: 1, type: 'initial' },
      failedRound(),
    ],
    ...overrides,
  });

  test('wakes the caller with the retry_task instruction and records the round once', async () => {
    const manager = new CallbackManager(new AgentDetector());
    let captured: { sessionId: string; prompt: string; round: DelegationRound } | undefined;
    (manager as any).launchCaller = (
      _thread: DelegationThread,
      sessionId: string,
      prompt: string,
      round: DelegationRound,
    ) => {
      captured = { sessionId, prompt, round };
      return true;
    };

    const thread = makeThread();
    const notified = await manager.notifyCallerOfFailure(thread, 2, {
      error: 'dsh: TRANSPORT: Connection error.',
      exitCode: 1,
      assessment: { errorCode: 'NONZERO_EXIT' },
    });

    assert.strictEqual(notified, true);
    assert.deepStrictEqual(thread.failureNotifications, [2]);
    assert.ok(captured);
    assert.strictEqual(captured!.sessionId, 'caller-codex-session-1');
    assert.strictEqual(captured!.round.round, 2);
    assert.match(captured!.prompt, /retry_task\(threadId="thread_cb_1"\)/);
    assert.match(captured!.prompt, /request_rework/);
    assert.match(captured!.prompt, /不要.*原样/s);
    assert.match(captured!.prompt, /Connection error/);
    assert.match(captured!.prompt, /session-8b5dc091/);
  });

  test('does not wake the caller twice for the same failed round', async () => {
    const manager = new CallbackManager(new AgentDetector());
    let launches = 0;
    (manager as any).launchCaller = () => {
      launches++;
      return true;
    };

    const thread = makeThread();
    assert.strictEqual(await manager.notifyCallerOfFailure(thread, 2), true);
    assert.strictEqual(await manager.notifyCallerOfFailure(thread, 2), false);
    assert.strictEqual(launches, 1);
  });

  test('stays silent for paused tasks and after three failure wake-ups', async () => {
    const manager = new CallbackManager(new AgentDetector());
    let launches = 0;
    (manager as any).launchCaller = () => {
      launches++;
      return true;
    };

    const paused = makeThread({ status: 'paused' });
    assert.strictEqual(await manager.notifyCallerOfFailure(paused, 2), false);
    assert.strictEqual(launches, 0);

    const exhausted = makeThread({ failureNotifications: [1, 2, 3] });
    exhausted.rounds.push(failedRound({ round: 4 }));
    assert.strictEqual(await manager.notifyCallerOfFailure(exhausted, 4), false);
    assert.strictEqual(launches, 0);
  });

  test('tells the caller when the round merely repeats an earlier instruction', async () => {
    const manager = new CallbackManager(new AgentDetector());
    let prompt = '';
    (manager as any).launchCaller = (_t: DelegationThread, _s: string, p: string) => {
      prompt = p;
      return true;
    };

    const thread = makeThread();
    thread.rounds[1] = failedRound({ repeatOfRound: 1 });
    assert.strictEqual(await manager.notifyCallerOfFailure(thread, 2), true);
    assert.match(prompt, /repeatOfRound=1/);
    assert.match(prompt, /retry_task/);
  });
});
