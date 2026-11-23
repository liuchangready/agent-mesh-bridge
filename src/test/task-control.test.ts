import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn } from 'child_process';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-control-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskControlService } from '../bridge/task-control';
import { ReworkController } from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { SessionOwnershipStore, nativeSessionNamespace } from '../bridge/session-ownership';
import { getProcessBirthId } from '../bridge/platform';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + ('thread_control_1')),
    callerSessionId: ('reviewer-' + ('thread_control_1')),
    threadId: 'thread_control_1',
    title: 'Pause/delete probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'running',
    currentRound: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    receiverSessionId: 'receiver-session-0001',
    workspace: testWorkspace(os.tmpdir()),
    summary: {
      lastPromptSnippet: 'do the work',
      lastOutputSnippet: 'Executing...',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'do the work',
        status: 'running',
        durationMs: 10,
        timestamp: new Date().toISOString(),
      },
    ],
    ...overrides,
  };
}

describe('TaskControlService - pause/delete reasons', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let runner: AutonomousRunner;
  let canceled: Array<{ threadId: string; action: string; reason: string }>;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-control-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    runner = new AutonomousRunner(detector, undefined, new CancellationRegistry(tempDir));
    canceled = [];
    runner.cancelThread = (threadId, action, reason) => {
      canceled.push({ threadId, action, reason });
      return true;
    };
  });

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('pausing a running task records the interruption reason and stops the child', async () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );

    const result = await control.pauseTask('thread_control_1', 'switch to a hotfix', 'user');
    assert.strictEqual(result.status, 'PAUSED');

    const thread = taskStore.getTask('thread_control_1')!;
    assert.strictEqual(thread.status, 'paused');
    assert.strictEqual(thread.rounds[0].status, 'paused');
    assert.strictEqual(thread.interruption?.kind, 'paused');
    assert.strictEqual(thread.interruption?.reason, 'switch to a hotfix');
    assert.strictEqual(thread.interruptions?.length, 1);
    assert.strictEqual(thread.rounds[0].interruption?.reason, 'switch to a hotfix');

    assert.deepStrictEqual(canceled, [
      { threadId: 'thread_control_1', action: 'pause', reason: 'switch to a hotfix' },
    ]);
    const request = new CancellationRegistry(tempDir).peek('thread_control_1');
    assert.strictEqual(request?.action, 'pause');
    assert.strictEqual(request?.reason, 'switch to a hotfix');
  });

  test('deleting a task removes it, stops the child, and writes a tombstone reason', async () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );

    const result = await control.deleteTask('thread_control_1', 'duplicate request', 'user');
    assert.strictEqual(result.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_control_1'), undefined);
    assert.deepStrictEqual(canceled, [
      { threadId: 'thread_control_1', action: 'delete', reason: 'duplicate request' },
    ]);

    const record = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line)).filter((entry) => entry.threadId === 'thread_control_1').slice(-1)[0];
    assert.strictEqual(record.threadId, 'thread_control_1');
    assert.strictEqual(record.reason, 'duplicate request');
    // A normal delete is not a forced one: the tombstone must not claim otherwise.
    assert.strictEqual(record.forced, undefined);
  });

  test('independent TaskStore processes append concurrent tombstone facts without losing either', async () => {
    const storage = path.join(tempDir, 'concurrent-tombstones');
    const modulePath = path.join(process.cwd(), 'out', 'bridge', 'task-store.js');
    const code = [
      `const { TaskStore } = require(${JSON.stringify(modulePath)});`,
      `const store = new TaskStore(process.argv[1]);`,
      `process.send('ready');`,
      `process.on('message', (message) => { if (message === 'append') { store.appendTombstone({threadId:'delete-from-child',cleanupToken:'child-token'}); process.send('done'); store.dispose(); process.disconnect(); } });`,
    ].join('');
    const child = spawn(process.execPath, ['-e', code, storage], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'], windowsHide: true });
    const ready = new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('message', (message) => message === 'ready' && resolve()); });
    const done = new Promise<void>((resolve, reject) => { child.once('error', reject); child.on('message', (message) => message === 'done' && resolve()); });
    await ready;
    const parentStore = new TaskStore(storage);
    parentStore.appendTombstone({ threadId: 'delete-from-parent', cleanupToken: 'parent-token' });
    child.send('append');
    await done;
    const lines = fs.readFileSync(path.join(storage, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line));
    assert.deepStrictEqual(lines.map((record) => record.cleanupToken).sort(), ['child-token', 'parent-token']);
    parentStore.dispose();
  });

  test('v2 deletion reserves and commits cleanup before removing the ledger row', async () => {
    const threadId = 'thread_control_v2_cleanup';
    taskStore.upsertTask(makeThread({
      threadId,
      status: 'completed',
      reviewerSessionCaptured: false,
      rounds: [{
        round: 1, type: 'initial', prompt: 'done', output: 'done', status: 'completed',
        durationMs: 1, timestamp: new Date().toISOString(),
      }],
      execution: {
        version: 2,
        revision: 1,
        operationId: 'merge_1',
        phase: 'merge',
        round: 1,
        generation: 1,
        attempt: 1,
        autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
      },
    }));
    assert.equal(taskStore.getTask(threadId)?.execution?.version, 2);
    let cleanupMarks = 0;
    const originalMark = taskStore.markTaskCleanup.bind(taskStore);
    taskStore.markTaskCleanup = async (...args: any[]) => { cleanupMarks++; return (originalMark as any)(...args); };
    const originalCommit = taskStore.commitTaskCleanup.bind(taskStore);
    taskStore.commitTaskCleanup = async (...args: any[]) => { assert.ok(taskStore.getTask(threadId)?.cleanup); const result = await (originalCommit as any)(...args); assert.equal(result, true); return result; };
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const result = await control.deleteTask(threadId, 'cleanup proof', 'user');
    assert.equal(cleanupMarks, 1);
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(threadId), undefined);
    const tombstones = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8');
    const records = tombstones.trim().split(/\r?\n/).map((line) => JSON.parse(line));
    const record = records.find((entry: any) => entry.threadId === threadId && entry.cleanupToken);
    assert.ok(record, tombstones);
    assert.match(record.cleanupToken, /^clean_/);
  });

  test('v2 delete retains native transcript when static ownership evidence is missing', async () => {
    const threadId = 'thread_control_v2_unverified_native';
    const sessionId = '01a16bb5-4139-7231-910b-3bed102caa6e';
    const nativeHome = path.join(tempDir, 'native');
    const priorCodexHome = process.env.CODEX_HOME;
    process.env.CODEX_HOME = nativeHome;
    try {
      const transcript = path.join(nativeHome, 'sessions', '2026', '10', '24', `rollout-2026-10-24T10-00-00-${sessionId}.jsonl`);
      fs.mkdirSync(path.dirname(transcript), { recursive: true });
      fs.writeFileSync(transcript, '{"id":"protected"}\n');
      taskStore.upsertTask(makeThread({
        threadId, status: 'completed', targetAgent: 'codex', receiverSessionId: sessionId,
        receiverSessionCaptured: true,
        nativeSessionAssignments: { developer: { namespace: nativeSessionNamespace('codex'), sessionEpoch: 0 } },
        execution: {
          version: 2, revision: 1, operationId: 'op_delete_unverified', phase: 'merge', round: 1,
          generation: 1, attempt: 1, autoRetriesUsed: 0,
          budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
        },
        rounds: [{ round: 1, type: 'initial', prompt: 'done', output: 'done', status: 'completed', durationMs: 1, timestamp: new Date().toISOString() }],
      }));
      const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
      const result = await control.deleteTask(threadId, 'verify before native delete', 'user');
      assert.equal(result.status, 'DELETED');
      assert.equal(fs.existsSync(transcript), true, 'unverified native data must be retained');
      assert.ok(result.nativeSessions?.entries.some((entry) => entry.sessionId === sessionId && /SESSION_OWNERSHIP_UNVERIFIED/.test(entry.error || '')));
      const records = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line));
      assert.ok(records.some((record) => record.threadId === threadId && record.nativeSessions?.sessions?.some((entry: any) => entry.sessionId === sessionId && /SESSION_OWNERSHIP_UNVERIFIED/.test(entry.error || ''))));
      assert.equal(new SessionOwnershipStore(tempDir).verify({
        agent: 'codex', namespace: nativeSessionNamespace('codex'), sessionId, threadId,
        role: 'developer', sessionEpoch: 0,
      }).ok, false);
    } finally {
      if (priorCodexHome === undefined) delete process.env.CODEX_HOME;
      else process.env.CODEX_HOME = priorCodexHome;
    }
  });

  test('default v2 delete cleans only the verified Antigravity CLI namespace', async () => {
    const threadId = 'thread_control_v2_antigravity_namespace';
    const sessionId = 'aa9fd1da-01b7-4bd7-a8df-8aae3b0aee44';
    const nativeHome = path.join(tempDir, 'antigravity-home');
    const oldHome = process.env.HOME;
    const oldUserProfile = process.env.USERPROFILE;
    process.env.HOME = nativeHome;
    process.env.USERPROFILE = nativeHome;
    try {
      const namespace = nativeSessionNamespace('antigravity');
      const cliDb = path.join(nativeHome, '.gemini', 'antigravity-cli', 'conversations', `${sessionId}.db`);
      const desktopDb = path.join(nativeHome, '.gemini', 'antigravity', 'conversations', `${sessionId}.db`);
      fs.mkdirSync(path.dirname(cliDb), { recursive: true });
      fs.mkdirSync(path.dirname(desktopDb), { recursive: true });
      fs.writeFileSync(cliDb, 'cli owned');
      fs.writeFileSync(desktopDb, 'desktop owned');
      const ownership = new SessionOwnershipStore(tempDir);
      assert.equal(ownership.claim({ agent: 'antigravity', namespace, sessionId, threadId, role: 'developer', sessionEpoch: 1 }).ok, true);
      taskStore.upsertTask(makeThread({
        threadId, status: 'completed', targetAgent: 'antigravity', receiverSessionId: sessionId,
        receiverSessionCaptured: true, reviewerSessionId: undefined as any, reviewerSessionCaptured: false,
        callerSessionId: 'requester-never-clean',
        nativeSessionAssignments: { developer: { namespace, sessionEpoch: 1 } },
        execution: { version: 2, revision: 1, operationId: 'op_agy_delete', phase: 'merge', round: 1,
          generation: 1, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
        rounds: [{ round: 1, type: 'initial', prompt: 'done', output: 'done', status: 'completed', durationMs: 1, timestamp: new Date().toISOString() }],
      }));
      const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
      const result = await control.deleteTask(threadId, 'namespace test', 'user');
      assert.equal(fs.existsSync(cliDb), false);
      assert.equal(fs.existsSync(desktopDb), true, 'same ID under desktop storage is outside the pinned CLI namespace');
      assert.equal(ownership.verify({ agent: 'antigravity', namespace, sessionId, threadId, role: 'developer', sessionEpoch: 1 }).ok, false, 'successful cleanup durably revokes the binding');
      assert.ok(!result.nativeSessions?.entries.some((entry) => entry.sessionId === 'requester-never-clean'));
    } finally {
      if (oldHome === undefined) delete process.env.HOME; else process.env.HOME = oldHome;
      if (oldUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = oldUserProfile;
    }
  });

  test('v2 delete retains Antigravity assets when the pinned CLI namespace changes', async () => {
    const threadId = 'thread_control_v2_antigravity_namespace_changed';
    const sessionId = '17ab8d70-08e6-42d0-b709-591f80f8db8b';
    const pinnedHome = path.join(tempDir, 'antigravity-pinned-home');
    const changedHome = path.join(tempDir, 'antigravity-changed-home');
    const oldHome = process.env.HOME;
    const oldUserProfile = process.env.USERPROFILE;
    process.env.HOME = pinnedHome;
    process.env.USERPROFILE = pinnedHome;
    try {
      const namespace = nativeSessionNamespace('antigravity');
      const cliDb = path.join(pinnedHome, '.gemini', 'antigravity-cli', 'conversations', `${sessionId}.db`);
      fs.mkdirSync(path.dirname(cliDb), { recursive: true });
      fs.writeFileSync(cliDb, 'must be retained after namespace drift');
      const ownership = new SessionOwnershipStore(tempDir);
      assert.equal(ownership.claim({ agent: 'antigravity', namespace, sessionId, threadId, role: 'developer', sessionEpoch: 3 }).ok, true);
      taskStore.upsertTask(makeThread({
        threadId, status: 'completed', targetAgent: 'antigravity', receiverSessionId: sessionId,
        receiverSessionCaptured: true, callerSessionId: 'requester-never-clean',
        nativeSessionAssignments: { developer: { namespace, sessionEpoch: 3 } },
        execution: { version: 2, revision: 1, operationId: 'op_agy_namespace_drift', phase: 'merge', round: 1,
          generation: 1, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
        rounds: [{ round: 1, type: 'initial', prompt: 'done', output: 'done', status: 'completed', durationMs: 1, timestamp: new Date().toISOString() }],
      }));
      process.env.HOME = changedHome;
      process.env.USERPROFILE = changedHome;
      const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
      const result = await control.deleteTask(threadId, 'namespace drift', 'user');
      assert.equal(fs.existsSync(cliDb), true, 'the old pinned native root cannot be guessed after configuration drift');
      assert.ok(result.nativeSessions?.entries.some((entry) => entry.sessionId === sessionId && /namespace differs/.test(entry.error || '')));
      const events = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line));
      assert.ok(events.some((event) => event.threadId === threadId && event.nativeSessions?.sessions?.some((entry: any) => entry.sessionId === sessionId && /namespace differs/.test(entry.error || ''))));
    } finally {
      if (oldHome === undefined) delete process.env.HOME; else process.env.HOME = oldHome;
      if (oldUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = oldUserProfile;
    }
  });

  test('v2 delete resumes a durable pending tombstone once under the original cleaner identity', async () => {
    const threadId = 'thread_control_v2_tombstone_retry';
    const artifactDir = path.join(tempDir, 'task-data', threadId);
    const asset = path.join(artifactDir, 'worker_log', 'round.log');
    fs.mkdirSync(path.dirname(asset), { recursive: true });
    fs.writeFileSync(asset, 'durable cleanup fixture');
    const owner = { pid: process.pid, birthId: getProcessBirthId(process.pid)! };
    taskStore.upsertTask(makeThread({
      threadId, status: 'completed', execution: {
        version: 2, revision: 1, operationId: 'op_retry_tombstone', phase: 'merge', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
      },
    }));
    const reservation = await taskStore.markTaskCleanup(threadId, 'delete', owner);
    const thread = reservation.thread;
    const manifest = {
      token: reservation.token, owner, stage: 'pending', reason: 'crash window', by: 'user', forced: false,
      thread: { threadId, targetAgent: thread.targetAgent, initiatorAgent: thread.initiatorAgent,
        reviewFlow: thread.reviewFlow, receiverSessionId: '', receiverSessionCaptured: false,
        reviewerSessionCaptured: false, artifactDir, worktree: undefined },
    };
    await taskStore.commitTaskCleanup(threadId, reservation.token, 'delete', {
      tombstone: { reason: 'crash window', by: 'user' }, cleanupManifest: manifest,
    });
    assert.equal(fs.existsSync(asset), true, 'tombstone precedes physical cleanup');
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const retried = await control.deleteTask(threadId, 'retry', 'user');
    assert.equal(retried.status, 'DELETED');
    assert.equal(fs.existsSync(artifactDir), false);
    const records = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line));
    assert.equal(records.filter((record) => record.threadId === threadId && record.cleanupComplete === true).length, 1);
    assert.equal(records.filter((record) => record.threadId === threadId).slice(-1)[0].cleanupManifest.stage, 'complete');
    const duplicate = await control.deleteTask(threadId, 'retry again', 'user');
    assert.match(duplicate.message, /cleanup is complete/);
    assert.equal(fs.existsSync(artifactDir), false);
    assert.equal(fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).filter((line) => JSON.parse(line).threadId === threadId && JSON.parse(line).cleanupComplete === true).length, 1);
  });

  test('deleting a task also records the receiver conversation cleanup', async () => {
    taskStore.upsertTask(makeThread());
    const cleanedThreads: string[] = [];
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
      (thread) => {
        cleanedThreads.push(thread.threadId);
        return {
          entries: [
            {
              agent: 'claude_code',
              sessionId: 'receiver-session-0001',
              found: true,
              removed: ['/home/u/.claude/projects/x/receiver-session-0001.jsonl'],
              pruned: [],
            },
          ],
          removedCount: 1,
        };
      },
    );

    const result = await control.deleteTask('thread_control_1', 'duplicate request', 'user');

    assert.deepStrictEqual(cleanedThreads, ['thread_control_1']);
    assert.ok(result.nativeSessions, 'the cleanup outcome is returned to the caller');
    assert.strictEqual(result.nativeSessions?.removedCount, 1);
    assert.match(result.message, /native conversations: 1 cleaned/);

    const record = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line)).filter((entry) => entry.threadId === 'thread_control_1').slice(-1)[0];
    assert.strictEqual(record.nativeSessions.removedFiles, 1);
    assert.strictEqual(record.nativeSessions.sessions[0].agent, 'claude_code');
    assert.strictEqual(record.nativeSessions.sessions[0].sessionId, 'receiver-session-0001');
    assert.strictEqual(record.nativeSessions.sessions[0].found, true);
  });

  test('a failing native conversation cleanup never blocks the deletion', async () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
      () => {
        throw new Error('session store is locked');
      },
    );

    const result = await control.deleteTask('thread_control_1', 'cleanup failed on purpose', 'user');

    assert.strictEqual(result.status, 'DELETED');
    assert.strictEqual(taskStore.getTask('thread_control_1'), undefined);
    assert.strictEqual(result.nativeSessions, undefined);
    const record = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line)).filter((entry) => entry.threadId === 'thread_control_1').slice(-1)[0];
    assert.strictEqual(record.reason, 'cleanup failed on purpose');
  });

  test('deleting a task stops and waits for its live reviewer child before removing it', async () => {
    const thread = makeThread({ status: 'reviewing', rounds: [{ round: 1, type: 'initial', prompt: 'p', output: 'o', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }] });
    taskStore.upsertTask(thread);
    const roles = new TaskRoleLock(tempDir); const lease = roles.tryAcquire(thread.threadId, 'reviewer'); assert.ok(lease);
    const reviewer = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
    roles.attachChild(lease, reviewer.pid);
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const deleting = control.deleteTask(thread.threadId, 'remove reviewed task', 'user', true);
    await new Promise<void>((resolve) => reviewer.once('exit', () => resolve()));
    const result = await deleting;
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(thread.threadId), undefined);
  });

  test('forced deletion retains the ledger until the registered developer process exits', async () => {
    const thread = makeThread({ status: 'running' }); taskStore.upsertTask(thread);
    const registry = new TaskExecutionRegistry(tempDir);
    const registration = registry.begin(thread.threadId, 1, 'test', tempDir);
    const developer = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore' });
    registry.attachChildPid(developer.pid, registration);
    developer.once('exit', () => registry.end(registration));
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const deleting = control.deleteTask(thread.threadId, 'discard active run', 'user', true);
    assert.equal(taskStore.getTask(thread.threadId)?.status, 'paused', 'deletion intent is durable while stopping the child');
    await new Promise<void>((resolve) => developer.once('exit', () => resolve()));
    assert.equal((await deleting).status, 'DELETED');
    assert.equal(taskStore.getTask(thread.threadId), undefined);
    registry.dispose();
  });

  test('resuming a paused task keeps history and clears the current interruption', async () => {
    taskStore.upsertTask(makeThread());
    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );
    control.pauseTask('thread_control_1', 'pause for inspection', 'user');

    const callbackManager = new CallbackManager(new AgentDetector());
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const reworkController = new ReworkController(
      taskStore,
      runner,
      callbackManager,
      new AgentDetector(),
      new TaskExecutionRegistry(tempDir),
    );

    let executedPrompt = '';
    runner.execute = async (options: RunOptions): Promise<RunResult> => {
      executedPrompt = options.prompt;
      return {
        exitCode: 0,
        stdout: 'resumed',
        stderr: '',
        extractedOutput: 'resumed',
        durationMs: 5,
      };
    };

    const round = await reworkController.resumePausedTask('thread_control_1');
    assert.strictEqual(round.status, 'running');
    assert.strictEqual(round.interruption, undefined, 'resumed round clears its pause marker');

    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      if (taskStore.getTask('thread_control_1')?.status === 'reviewing') break;
      await new Promise((r) => setTimeout(r, 20));
    }

    const thread = taskStore.getTask('thread_control_1')!;
    assert.strictEqual(executedPrompt, 'do the work');
    assert.strictEqual(thread.status, 'reviewing');
    assert.strictEqual(thread.interruption, undefined, 'current interruption cleared');
    assert.strictEqual(thread.rounds[0].interruption, undefined, 'persisted round clears its pause marker');
    assert.strictEqual(thread.interruptions?.[0]?.reason, 'pause for inspection', 'history kept');
  });

  test('resuming clears the pause the runner remembered while nothing was running', async () => {
    taskStore.upsertTask(makeThread({ status: 'queued' }));
    // Use the real runner: pausing a task that never spawned a child leaves the
    // pause remembered in memory, where no settling round will consume it.
    runner.cancelThread = AutonomousRunner.prototype.cancelThread;

    const control = new TaskControlService(
      taskStore,
      runner,
      new CancellationRegistry(tempDir),
      tempDir,
    );
    control.pauseTask('thread_control_1', 'pause before start', 'user');
    assert.ok(
      runner.checkPendingCancellation('thread_control_1'),
      'the pause stays pending for a task that never spawned a child',
    );

    const callbackManager = new CallbackManager(new AgentDetector());
    callbackManager.notifyCaller = async () => true;
    callbackManager.notifyCallerOfFailure = async () => false;
    const reworkController = new ReworkController(
      taskStore,
      runner,
      callbackManager,
      new AgentDetector(),
      new TaskExecutionRegistry(tempDir),
    );
    runner.execute = async (): Promise<RunResult> => ({
      exitCode: 0,
      stdout: 'resumed',
      stderr: '',
      extractedOutput: 'resumed',
      durationMs: 5,
    });

    await reworkController.resumePausedTask('thread_control_1');

    assert.strictEqual(
      runner.checkPendingCancellation('thread_control_1'),
      undefined,
      'a resumed task must not be cancelled on sight by the pause it was resumed from',
    );
  });
});
