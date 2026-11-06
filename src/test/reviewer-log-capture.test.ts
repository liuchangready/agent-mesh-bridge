import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';

// Patch the real CommonJS module: `import * as child_process` only exposes
// getters, so the spawned-module object is the one that can be replaced.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const childProcessModule: any = require('child_process');

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-reviewer-capture-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { CallbackManager } from '../bridge/callback-manager';
import { AgentDetector } from '../bridge/agent-detector';
import { rememberCodexResumeSupport } from '../bridge/codex-session';
import { getBridgeHome, invalidateConfiguredModelCache, saveModelDefaults } from '../bridge/model-catalog';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { DelegationRound, DelegationThread } from '../bridge/types';
import { readReviewerLogTail, reviewerLogPath, statReviewerLog } from '../bridge/reviewer-log';

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
      child.emit('close', 0);

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

  test('resumes codex with the canonical thread UUID by default', async () => {
    const uuid = '01a17d29-d40d-74c3-831d-878dd4edc0e5';

    const originalSpawn = childProcessModule.spawn;
    const calls: any[][] = [];
    childProcessModule.spawn = (...args: any[]) => {
      calls.push(args);
      return makeFakeChild();
    };

    try {
      // Bundled thin client: needs --no-daemon and resolves by UUID.
      rememberCodexResumeSupport('fake-codex-uuid', { noDaemon: true, uuidIds: true });

      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex-uuid';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      thread.threadId = 'thread_capture_uuid';
      thread.callerSessionId = uuid;

      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);

      assert.strictEqual(calls.length, 1);
      const argv = calls[0][1] as string[];
      const resumeIndex = argv.indexOf('resume');
      assert.ok(resumeIndex >= 0);
      assert.strictEqual(argv[resumeIndex + 1], uuid, 'modern CLI resolves by thread UUID');
      assert.ok(argv.includes('--no-daemon'), 'the thin client is told to skip the daemon');
      assert.ok(
        argv.indexOf('--no-daemon') < argv.indexOf('exec'),
        '--no-daemon is a global option and must precede exec',
      );
      assert.strictEqual(thread.callerSessionId, uuid, 'the ledger keeps the canonical UUID');
    } finally {
      childProcessModule.spawn = originalSpawn;
    }
  });

  test('uses the rollout name for a legacy CLI but still stores the UUID', async () => {
    // A different thread from the UUID test: the wake queue is keyed by the
    // canonical caller session, and the fake child never exits to release it.
    const uuid = '02b28e3a-51e1-4854-942a-989ee5fe1f96';
    const rollout = `rollout-2026-10-27T19-41-02-${uuid}`;
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
      // Legacy build: `resume --help` talks about the rollout record.
      rememberCodexResumeSupport('fake-codex-legacy', { noDaemon: false, uuidIds: false });
      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex-legacy';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      thread.threadId = 'thread_capture_legacy';
      // A legacy ledger may still hold the rollout spelling.
      thread.callerSessionId = rollout;

      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);

      assert.strictEqual(calls.length, 1);
      const argv = calls[0][1] as string[];
      const resumeIndex = argv.indexOf('resume');
      assert.ok(resumeIndex >= 0);
      assert.strictEqual(argv[resumeIndex + 1], rollout, 'legacy CLI gets the rollout name');
      assert.ok(!argv.includes('--no-daemon'), 'a standalone CLI must not receive the flag');
      assert.strictEqual(thread.callerSessionId, uuid, 'the ledger is healed to the canonical UUID');
    } finally {
      childProcessModule.spawn = originalSpawn;
      fs.rmSync(sessionDir, { recursive: true, force: true });
    }
  });

  test('wakes the caller on its sender-side default model, not the receiver default', async () => {
    const home = getBridgeHome();
    // The review wake must run on the separately configured sender-side model,
    // never on the receiver-side default the task executes with.
    saveModelDefaults({ codex: 'gpt-6-sol' }, home, {
      initiatorModels: { codex: 'gpt-6.1-pro' },
    });
    invalidateConfiguredModelCache();

    const originalSpawn = childProcessModule.spawn;
    const calls: any[][] = [];
    childProcessModule.spawn = (...args: any[]) => {
      calls.push(args);
      return makeFakeChild();
    };

    try {
      rememberCodexResumeSupport('fake-codex-sender', { noDaemon: false, uuidIds: true });
      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex-sender';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      thread.threadId = 'thread_capture_sender_model';
      thread.callerSessionId = '03c39f4b-62f2-4965-a53b-a9aef6ef2fa7';

      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);

      assert.strictEqual(calls.length, 1);
      const argv = calls[0][1] as string[];
      const configIndex = argv.indexOf('-c');
      assert.ok(configIndex >= 0, argv.join(' '));
      assert.strictEqual(argv[configIndex + 1], 'model="gpt-6.1-pro"');
      assert.ok(
        configIndex < argv.indexOf('exec'),
        `the model override is a global option and must precede exec: ${argv.join(' ')}`,
      );
      assert.ok(
        !argv.includes('model="gpt-6-sol"'),
        'the receiver-side default must not leak into the reviewer wake',
      );
    } finally {
      childProcessModule.spawn = originalSpawn;
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('dedicated review resumes the reviewer conversation and binds the lock to it', async () => {
    const reviewerSession = '05e4a9c1-73f3-4d86-b64c-b1cf7fd3a08b';
    const originalSpawn = childProcessModule.spawn;
    const calls: any[][] = [];
    childProcessModule.spawn = (...args: any[]) => {
      calls.push(args);
      return makeFakeChild();
    };

    try {
      rememberCodexResumeSupport('fake-codex-dedicated', { noDaemon: false, uuidIds: true });
      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex-dedicated';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      thread.threadId = 'thread_capture_dedicated_review';
      thread.reviewFlow = 'dedicated';
      thread.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
      thread.reviewerSessionCaptured = true;
      thread.reviewerSessionId = reviewerSession;
      thread.callerSessionId = reviewerSession;

      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);
      assert.strictEqual(calls.length, 1);
      const argv = calls[0][1] as string[];
      const resumeIndex = argv.indexOf('resume');
      assert.ok(resumeIndex >= 0, argv.join(' '));
      assert.strictEqual(argv[resumeIndex + 1], reviewerSession);
      const spawnOptions = calls[0][2] as any;
      assert.strictEqual(spawnOptions.env.MESH_REVIEW_SESSION_ID, reviewerSession);
      assert.strictEqual(spawnOptions.env.MESH_REVIEW_THREAD_ID, thread.threadId);
      assert.strictEqual(spawnOptions.env.MESH_TASK_RECEIVER, undefined);

      const owner = new TaskRoleLock().getOwner(thread.threadId);
      assert.ok(owner, 'the reviewer role must be held');
      assert.strictEqual(owner?.conversationId, reviewerSession);
    } finally {
      childProcessModule.spawn = originalSpawn;
      new TaskRoleLock().releaseIfRole('thread_capture_dedicated_review', 'reviewer');
    }
  });

  test('a stale wake round is normalised to the newest completed work round', async () => {
    const originalSpawn = childProcessModule.spawn;
    childProcessModule.spawn = () => makeFakeChild();
    try {
      rememberCodexResumeSupport('fake-codex-round-clamp', { noDaemon: false, uuidIds: true });
      const detector = new AgentDetector();
      (detector as any).resolveExecutable = () => 'fake-codex-round-clamp';
      const manager = new CallbackManager(detector);
      const thread = makeThread();
      thread.threadId = 'thread_capture_round_clamp';
      thread.callerSessionId = '06f5b0d2-84f4-4e97-a75d-c2d0a8e4b19c';
      thread.currentRound = 5;
      const madeRound = (round: number): DelegationRound => ({
        round,
        type: 'rework',
        prompt: `round ${round}`,
        output: `delivered ${round}`,
        status: 'completed',
        durationMs: 1,
        timestamp: '2026-10-20T04:30:26.662Z',
      });
      thread.rounds = [madeRound(4), madeRound(5)];

      // The caller/retry still points at round 4; the log must be written for 5.
      assert.strictEqual(await manager.notifyCaller(thread, thread.rounds[0]), true);
      assert.strictEqual(statReviewerLog(thread.threadId, 5).exists, true);
      assert.strictEqual(statReviewerLog(thread.threadId, 4).exists, false);
      assert.strictEqual(fs.existsSync(reviewerLogPath(thread.threadId, 5)), true);
    } finally {
      childProcessModule.spawn = originalSpawn;
      new TaskRoleLock().releaseIfRole('thread_capture_round_clamp', 'reviewer');
    }
  });
});
