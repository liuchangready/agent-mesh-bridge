import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';

// Patch the real CommonJS module: `import * as child_process` only exposes
// getters, so the spawned-module object is the one that can be replaced.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const childProcessModule: any = require('child_process');

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-reviewer-capture-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { CallbackManager } from '../bridge/callback-manager';
import { AgentDetector } from '../bridge/agent-detector';
import { DelegationRound, DelegationThread } from '../bridge/types';
import { readReviewerLogTail, reviewerLogPath } from '../bridge/reviewer-log';

interface FakeChild extends EventEmitter {
  pid: number;
  stdout: EventEmitter;
  stderr: EventEmitter;
  unref: () => void;
}

function makeFakeChild(): FakeChild {
  const child = new EventEmitter() as FakeChild;
  child.pid = 4242;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.unref = () => {};
  return child;
}

const completedRound: DelegationRound = {
  round: 1,
  type: 'initial',
  prompt: 'Implement the composition',
  status: 'completed',
  durationMs: 1000,
  timestamp: '2026-10-20T04:30:26.662Z',
  output: 'receiver report',
};

const makeThread = (): DelegationThread => ({
  threadId: 'thread_capture_1',
  title: 'Capture reviewer stream',
  initiatorAgent: 'codex',
  targetAgent: 'claude_code',
  status: 'waiting_review',
  currentRound: 1,
  createdAt: '2026-10-20T03:17:38.487Z',
  updatedAt: '2026-10-20T04:39:07.796Z',
  receiverSessionId: 'session-8b5dc091-3d68-43d9-bd08-05b383c4b972',
  receiverSessionCaptured: true,
  callerSessionId: 'caller-codex-session-1',
  workspacePath: process.cwd(),
  expectedFiles: ['src/example.ts'],
  summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 1000 },
  rounds: [completedRound],
});

describe('Reviewer stream capture', () => {
  test('tees the caller stdout/stderr into the per-round reviewer log', async () => {
    const originalSpawn = childProcessModule.spawn;
    const spawned: FakeChild[] = [];
    childProcessModule.spawn = () => {
      const child = makeFakeChild();
      spawned.push(child);
      return child;
    };

    try {
      const detector = new AgentDetector();
      // Never resolve a real CLI: the test only needs the spawn call itself.
      (detector as any).resolveExecutable = () => 'fake-codex';
      const manager = new CallbackManager(detector);
      const thread = makeThread();

      const launched = await manager.notifyCaller(thread, thread.rounds[0]);
      assert.strictEqual(launched, true);
      assert.strictEqual(spawned.length, 1);

      const child = spawned[0];
      child.stdout.emit('data', Buffer.from('REVIEWER-STDOUT line\n'));
      child.stderr.emit('data', Buffer.from('REVIEWER-STDERR line\n'));
      child.emit('exit', 0);

      const file = reviewerLogPath(thread.threadId, 1);
      assert.strictEqual(fs.existsSync(file), true, `expected reviewer log at ${file}`);

      const tail = readReviewerLogTail(thread.threadId, 1, { maxBytes: 64 * 1024 });
      assert.ok(tail);
      assert.match(tail!.content, /wake stream=reviewer/);
      assert.match(tail!.content, /agent=codex/);
      assert.match(tail!.content, /kind=review/);
      assert.match(tail!.content, /REVIEWER-STDOUT line/);
      assert.match(tail!.content, /REVIEWER-STDERR line/);
      assert.match(tail!.content, /\[exit\] code=0/);
    } finally {
      childProcessModule.spawn = originalSpawn;
      fs.rmSync(isolatedHome, { recursive: true, force: true });
    }
  });

  test('resumes codex with the rollout name when the ledger holds a bare thread UUID', async () => {
    const uuid = '01a17d29-d40d-74c3-831d-878dd4edc0e5';
    const rollout = `rollout-2026-10-27T19-40-56-${uuid}`;
    const sessionDir = path.join(isolatedHome, '.codex', 'sessions', '2026', '10', '27');
    fs.mkdirSync(sessionDir, { recursive: true });
    fs.writeFileSync(path.join(sessionDir, `${rollout}.jsonl`), '{"type":"session_meta"}\n');

    const originalSpawn = childProcessModule.spawn;
    const calls: any[][] = [];
    childProcessModule.spawn = (...args: any[]) => {
      calls.push(args);
      return makeFakeChild();
    };

    try {
      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      // What the JSONL stream captured: the bare thread id, which codex
      // rejects with "no rollout found for thread id".
      thread.callerSessionId = uuid;

      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);

      assert.strictEqual(calls.length, 1);
      const argv = calls[0][1] as string[];
      const resumeIndex = argv.indexOf('resume');
      assert.ok(resumeIndex >= 0);
      assert.strictEqual(argv[resumeIndex + 1], rollout, 'resume must use the rollout name');
      assert.strictEqual(thread.callerSessionId, rollout, 'the ledger copy is healed in place');
    } finally {
      childProcessModule.spawn = originalSpawn;
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }
  });
});
