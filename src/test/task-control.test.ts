import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, execFileSync } from 'child_process';
const mutableFs = require('fs') as typeof fs;

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
import { MeshFileLock } from '../bridge/merge-coordinator';
import { inspectShutdownPreflight, loadShutdownPreflightSnapshot } from '../bridge/shutdown-preflight';
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

  test('pausing a V2 wait-decision handoff without a controller durably cancels next and queue', async () => {
    const threadId = 'thread_control_v2_pause_handoff';
    taskStore.upsertTask(makeThread({
      threadId, status: 'wait_decision',
      execution: {
        version: 2, revision: 2, operationId: 'op_pause_handoff', phase: 'review', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0,
        next: { phase: 'develop', round: 2, sourceRunId: 'review-run' },
        queue: { reason: 'backoff', notBefore: new Date(Date.now() + 60_000).toISOString() },
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
      },
    }));
    const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
      .pauseTask(threadId, 'cancel pending decision handoff', 'user');
    const paused = taskStore.getTask(threadId)!;
    assert.equal(result.status, 'PAUSED');
    assert.equal(paused.status, 'paused');
    assert.equal(paused.execution?.next, undefined);
    assert.equal(paused.execution?.queue, undefined);
    assert.equal(paused.interruption?.reason, 'cancel pending decision handoff');
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

  test('v2 tombstones never copy round conversation at reservation, claim, completion, or repeat delete', async () => {
    const threadId = 'thread_control_v2_tombstone_minimal';
    const markers = ['PROMPT_SECRET_MARKER_A', 'OUTPUT_SECRET_MARKER_A', 'PROMPT_SECRET_MARKER_B', 'OUTPUT_SECRET_MARKER_B'];
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', execution: {
      version: 2, revision: 1, operationId: 'op_minimal_tombstone', phase: 'merge', round: 2,
      generation: 9, attempt: 1, autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    }, rounds: [
      { round: 1, type: 'initial', prompt: markers[0], output: markers[1], status: 'completed', durationMs: 1, timestamp: new Date().toISOString() },
      { round: 2, type: 'rework', prompt: markers[2], output: markers[3], status: 'completed', durationMs: 1, timestamp: new Date().toISOString() },
    ] }));
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    assert.equal((await control.deleteTask(threadId, 'remove conversation', 'user')).status, 'DELETED');
    const records = () => fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/)
      .map((line) => JSON.parse(line)).filter((entry) => entry.threadId === threadId);
    for (const record of records()) {
      const encoded = JSON.stringify(record);
      for (const marker of markers) assert.equal(encoded.includes(marker), false, `tombstone leaked ${marker}`);
      assert.equal(record.cleanupManifest?.thread?.rounds, undefined);
      assert.equal(record.cleanupManifest?.thread?.finalSummary, undefined);
    }
    assert.ok(records().some((record) => record.cleanupManifest?.stage === 'running'), 'claim state must also be minimal');
    assert.ok(records().some((record) => record.cleanupManifest?.stage === 'complete'), 'complete state must also be minimal');
    await control.deleteTask(threadId, 'repeat', 'user');
    for (const record of records()) for (const marker of markers) assert.equal(JSON.stringify(record).includes(marker), false);
  });

  test('ledger_written transaction proves fixed-ref integration and permits exact worktree cleanup', async () => {
    const threadId = 'thread_control_v2_ledger_written';
    const repo = path.join(tempDir, 'ledger-repo');
    const worktreePath = path.join(tempDir, 'worktrees', threadId);
    fs.mkdirSync(repo, { recursive: true });
    const git = (args: string[], cwd = repo) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
    git(['init', '-b', 'master']); git(['config', 'user.email', 'test@example.invalid']); git(['config', 'user.name', 'Test']);
    fs.writeFileSync(path.join(repo, 'base.txt'), 'base'); git(['add', 'base.txt']); git(['commit', '-m', 'base']);
    const baseCommit = git(['rev-parse', 'HEAD']);
    fs.mkdirSync(path.dirname(worktreePath), { recursive: true });
    git(['worktree', 'add', '-b', `mesh/task/${threadId}`, worktreePath, baseCommit]);
    fs.writeFileSync(path.join(worktreePath, 'change.txt'), 'approved'); git(['add', 'change.txt'], worktreePath); git(['commit', '-m', 'approved'], worktreePath);
    const approved = git(['rev-parse', 'HEAD'], worktreePath);
    git(['update-ref', 'refs/heads/master', approved]);
    const artifactDir = path.join(tempDir, 'tasks', threadId); fs.mkdirSync(path.join(artifactDir, 'worker_log'), { recursive: true });
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', artifactDir, artifactRoot: path.dirname(artifactDir),
      workspace: { ...testWorkspace(os.tmpdir()), executionPath: worktreePath },
      worktree: { path: worktreePath, mainPath: repo, repositoryPath: repo, commonDir: path.join(repo, '.git'), targetRef: 'refs/heads/master',
        deliveryTarget: { branch: 'master', ref: 'refs/heads/master', repositoryPath: repo }, taskBranch: `mesh/task/${threadId}`,
        baseCommit, approvedCommit: approved, integration: { phase: 'merged', sourceCommit: approved, targetOldCommit: baseCommit,
          candidateCommit: approved, mergedCommit: approved, updatedAt: new Date().toISOString() } },
      execution: { version: 2, revision: 1, operationId: 'op_ledger', phase: 'merge', round: 1, generation: 1, attempt: 1, autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
    }));
    const transactionDir = path.join(tempDir, 'merge-transactions'); fs.mkdirSync(transactionDir, { recursive: true });
    fs.writeFileSync(path.join(transactionDir, `${threadId}.json`), JSON.stringify({ threadId, sourceCommit: approved,
      targetOldCommit: baseCommit, candidateCommit: approved, phase: 'ledger_written', at: new Date().toISOString() }));
    const lockPath = path.join(tempDir, 'merge.lock');
    const mergeLock = new MeshFileLock(lockPath);
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir, undefined, mergeLock);
    const originalSafeWorktree = (control as any).isSafeDeletionWorktree.bind(control);
    (control as any).isSafeDeletionWorktree = () => { throw new Error('injected failure after merge lock acquisition'); };
    const interrupted = await control.deleteTask(threadId, 'retry after cleaner fault', 'user');
    assert.equal(interrupted.status, 'DELETED'); assert.match(interrupted.message, /injected failure after merge lock acquisition/);
    const afterFaultRelease = await new MeshFileLock(lockPath).acquire(undefined, undefined, 500, true);
    assert.ok(afterFaultRelease, 'cleaner finally releases the exact merge lock after an exception'); afterFaultRelease();
    (control as any).isSafeDeletionWorktree = originalSafeWorktree;

    const transactionPath = path.join(transactionDir, `${threadId}.json`);
    const originalReadFileSync = mutableFs.readFileSync;
    let transactionReads = 0;
    (mutableFs as any).readFileSync = function (file: any, ...args: any[]) {
      if (path.resolve(String(file)) === path.resolve(transactionPath)) {
        transactionReads++;
        if (transactionReads === 2) throw Object.assign(new Error('injected journal read EIO'), { code: 'EIO' });
      }
      return originalReadFileSync.call(this, file, ...args);
    };
    let result: Awaited<ReturnType<TaskControlService['deleteTask']>>;
    try { result = await control.deleteTask(threadId, 'remove integrated task', 'user'); }
    finally { (mutableFs as any).readFileSync = originalReadFileSync; }
    assert.equal(transactionReads, 1, 'all checks reuse the single verified transaction snapshot');
    assert.equal(result.status, 'DELETED');
    const afterCleanupRelease = await new MeshFileLock(lockPath).acquire(undefined, undefined, 500, true);
    assert.ok(afterCleanupRelease, 'another lock instance can acquire after cleanup'); afterCleanupRelease();
    assert.equal(fs.existsSync(worktreePath), false);
    assert.throws(() => git(['show-ref', '--verify', `refs/heads/mesh/task/${threadId}`]));
    const preflight = inspectShutdownPreflight({ ...loadShutdownPreflightSnapshot(tempDir), expectedVersion: 'test', requiredComponents: [] });
    assert.equal(preflight.blockers.some((blocker) => blocker.threadId === threadId), false);
  });

  test('ledger_written never discards post-approval tracked, untracked, or task-branch commits on ordinary delete', async () => {
    for (const mutation of ['tracked', 'untracked', 'local-commit'] as const) {
      const threadId = `thread_control_v2_ledger_dirty_${mutation.replace('-', '_')}`;
      const repo = path.join(tempDir, `ledger-dirty-${mutation}`);
      const worktreePath = path.join(tempDir, 'worktrees', threadId);
      fs.mkdirSync(repo, { recursive: true });
      const git = (args: string[], cwd = repo) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
      git(['init', '-b', 'master']); git(['config', 'user.email', 'test@example.invalid']); git(['config', 'user.name', 'Test']);
      fs.writeFileSync(path.join(repo, 'tracked.txt'), 'base'); git(['add', 'tracked.txt']); git(['commit', '-m', 'base']);
      const baseCommit = git(['rev-parse', 'HEAD']); fs.mkdirSync(path.dirname(worktreePath), { recursive: true });
      git(['worktree', 'add', '-b', `mesh/task/${threadId}`, worktreePath, baseCommit]);
      fs.writeFileSync(path.join(worktreePath, 'approved.txt'), 'approved'); git(['add', 'approved.txt'], worktreePath); git(['commit', '-m', 'approved'], worktreePath);
      const approved = git(['rev-parse', 'HEAD'], worktreePath); git(['update-ref', 'refs/heads/master', approved]);
      if (mutation === 'tracked') fs.writeFileSync(path.join(worktreePath, 'tracked.txt'), 'changed after approval');
      if (mutation === 'untracked') fs.writeFileSync(path.join(worktreePath, 'untracked.txt'), 'created after approval');
      if (mutation === 'local-commit') {
        fs.writeFileSync(path.join(worktreePath, 'local-only.txt'), 'not on master'); git(['add', 'local-only.txt'], worktreePath); git(['commit', '-m', 'local only'], worktreePath);
      }
      if (mutation !== 'local-commit') assert.match(git(['status', '--porcelain', '--untracked-files=all'], worktreePath), mutation === 'tracked' ? /M/ : /\?\?/);
      const artifactDir = path.join(tempDir, 'tasks', threadId); fs.mkdirSync(path.join(artifactDir, 'worker_log'), { recursive: true });
      taskStore.upsertTask(makeThread({ threadId, status: 'completed', artifactDir, artifactRoot: path.dirname(artifactDir),
        workspace: { ...testWorkspace(os.tmpdir()), executionPath: worktreePath },
        worktree: { path: worktreePath, mainPath: repo, repositoryPath: repo, commonDir: path.join(repo, '.git'), targetRef: 'refs/heads/master',
          deliveryTarget: { branch: 'master', ref: 'refs/heads/master', repositoryPath: repo }, taskBranch: `mesh/task/${threadId}`,
          baseCommit, approvedCommit: approved, integration: { phase: 'merged', sourceCommit: approved, targetOldCommit: baseCommit,
            candidateCommit: approved, mergedCommit: approved, updatedAt: new Date().toISOString() } },
        execution: { version: 2, revision: 1, operationId: `op_${mutation}`, phase: 'merge', round: 1, generation: 1, attempt: 1, autoRetriesUsed: 0,
          budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
      }));
      const transactionDir = path.join(tempDir, 'merge-transactions'); fs.mkdirSync(transactionDir, { recursive: true });
      fs.writeFileSync(path.join(transactionDir, `${threadId}.json`), JSON.stringify({ threadId, sourceCommit: approved,
        targetOldCommit: baseCommit, candidateCommit: approved, phase: 'ledger_written', at: new Date().toISOString() }));
      const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
        .deleteTask(threadId, `ordinary delete ${mutation}`, 'user', false);
      assert.equal(result.status, 'DELETED'); assert.equal(fs.existsSync(worktreePath), true, `${mutation}: worktree retained`);
      assert.match(result.message, /retained/i, `${mutation}: result warns about preserved output`);
      assert.equal(git(['for-each-ref', '--format=%(refname)', `refs/heads/mesh/task/${threadId}`]), `refs/heads/mesh/task/${threadId}`);
      if (mutation === 'untracked') assert.equal(fs.readFileSync(path.join(worktreePath, 'untracked.txt'), 'utf8'), 'created after approval');
      if (mutation === 'local-commit') assert.equal(git(['rev-parse', 'HEAD'], worktreePath) !== approved, true);
    }
  });

  test('retry completes task-data cleanup after exact Git worktree removal already succeeded', async () => {
    const threadId = 'thread_control_v2_removed_worktree_retry';
    const repo = path.join(tempDir, 'retry-repo'); const worktreePath = path.join(tempDir, 'worktrees', threadId);
    fs.mkdirSync(repo, { recursive: true });
    const git = (args: string[], cwd = repo) => execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
    git(['init', '-b', 'master']); git(['config', 'user.email', 'test@example.invalid']); git(['config', 'user.name', 'Test']);
    fs.writeFileSync(path.join(repo, 'base.txt'), 'base'); git(['add', 'base.txt']); git(['commit', '-m', 'base']);
    const baseCommit = git(['rev-parse', 'HEAD']); fs.mkdirSync(path.dirname(worktreePath), { recursive: true });
    git(['worktree', 'add', '-b', `mesh/task/${threadId}`, worktreePath, baseCommit]);
    const artifactDir = path.join(tempDir, 'tasks', threadId); const blocker = path.join(artifactDir, 'unknown-child');
    fs.mkdirSync(artifactDir, { recursive: true }); fs.writeFileSync(blocker, 'retain until repaired');
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', artifactDir, artifactRoot: path.dirname(artifactDir),
      workspace: { ...testWorkspace(os.tmpdir()), executionPath: worktreePath },
      worktree: { path: worktreePath, mainPath: repo, repositoryPath: repo, commonDir: path.join(repo, '.git'), targetRef: 'refs/heads/master',
        deliveryTarget: { branch: 'master', ref: 'refs/heads/master', repositoryPath: repo }, taskBranch: `mesh/task/${threadId}`,
        baseCommit, integration: { phase: 'no_changes', updatedAt: new Date().toISOString() } },
      execution: { version: 2, revision: 1, operationId: 'op_retry_removed', phase: 'merge', round: 1, generation: 1, attempt: 1, autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
    }));
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const first = await control.deleteTask(threadId, 'first cleanup pass', 'user');
    assert.equal(first.status, 'DELETED'); assert.equal(fs.existsSync(worktreePath), false, first.message); assert.equal(fs.existsSync(blocker), true);
    fs.unlinkSync(blocker);
    const retry = await control.deleteTask(threadId, 'retry cleanup', 'user');
    assert.equal(retry.status, 'DELETED'); assert.equal(fs.existsSync(artifactDir), false, `${retry.message}; entries=${fs.existsSync(artifactDir) ? fs.readdirSync(artifactDir).join(',') : ''}`);
  });

  test('historical stop timeout deletes logically but retains logs, worktree evidence, and requester session', async () => {
    const threadId = 'thread_control_legacy_stop_unknown';
    const artifactDir = path.join(tempDir, 'tasks', threadId); const log = path.join(artifactDir, 'worker_log', `${threadId}_round_1.log`);
    const preservedWorktreeAsset = path.join(artifactDir, 'worktree', 'uncommitted.txt');
    fs.mkdirSync(path.dirname(log), { recursive: true }); fs.mkdirSync(path.dirname(preservedWorktreeAsset), { recursive: true });
    fs.writeFileSync(log, 'preserve legacy output'); fs.writeFileSync(preservedWorktreeAsset, 'preserve worktree');
    const thread = makeThread({ threadId, status: 'running', callerSessionId: 'requester-session-must-survive',
      artifactDir, artifactRoot: path.dirname(artifactDir), execution: undefined });
    taskStore.upsertTask(thread);
    let cleanedSessions = false;
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir, () => { cleanedSessions = true; return { entries: [], removedCount: 0 }; });
    (control as any).waitForThreadChildren = async () => { throw new Error('injected stop timeout'); };
    const result = await control.deleteTask(threadId, 'legacy stop uncertain', 'user');
    assert.equal(result.status, 'DELETED'); assert.match(result.message, /stop remains unconfirmed|retained/i);
    assert.equal(taskStore.getTask(threadId), undefined); assert.equal(fs.existsSync(log), true);
    assert.equal(fs.existsSync(preservedWorktreeAsset), true); assert.equal(cleanedSessions, false);
    const latest = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/).map((line) => JSON.parse(line))
      .filter((entry) => entry.threadId === threadId).slice(-1)[0];
    assert.equal(latest.cleanupManifest.legacyStopPending, true);
    assert.equal(latest.cleanupManifest.legacyStopIdentityUnknown, true);
    const report = inspectShutdownPreflight({ ...loadShutdownPreflightSnapshot(tempDir), expectedVersion: 'test', requiredComponents: [] });
    assert.equal(report.ready, false); assert.ok(report.blockers.some((blocker) => blocker.threadId === threadId));
    assert.equal(latest.cleanupManifest.thread.callerSessionId, undefined, 'requester session is never copied into cleanup manifest');
  });

  test('v2 delete commits logically with an unconfirmed run and keeps its exact identity in the tombstone', async () => {
    const threadId = 'thread_control_v2_unknown_run_delete';
    const run = {
      id: 'unknown-run-identity', owner: { pid: 991122, birthId: 'old-owner-birth' },
      child: { pid: 991123, birthId: 'old-child-birth' }, step: 'running', state: 'stopping' as const,
      unconfirmed: true, startedAt: new Date().toISOString(), spawnIntentAt: new Date().toISOString(),
    };
    taskStore.upsertTask(makeThread({
      threadId, status: 'failed', execution: {
        version: 2, revision: 3, operationId: 'op_unknown_delete', phase: 'develop', round: 1,
        generation: 7, attempt: 1, autoRetriesUsed: 0, run,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
      },
    }));
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    const result = await control.deleteTask(threadId, 'remove failed run', 'user');
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(threadId), undefined);
    assert.match(result.message, /no verified .*exit proof|resources.*isolated/i);
    const record = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/)
      .map((line) => JSON.parse(line)).filter((entry) => entry.threadId === threadId).slice(-1)[0];
    assert.equal(record.cleanupManifest.retainResources, true);
    assert.deepEqual(record.cleanupManifest.execution.run, run);
    const stale = makeThread({ threadId, status: 'running' });
    taskStore.upsertTask(stale);
    assert.equal(taskStore.getTask(threadId), undefined, 'a late worker write cannot revive the tombstone');
  });

  test('logical deletion with an unknown spawn remains a shutdown blocker and repeat delete does not clean it', async () => {
    const threadId = 'thread_control_v2_deleted_spawn_preflight';
    const run = { id: 'unknown-spawn', owner: { pid: 991222, birthId: 'owner-unknown' },
      child: undefined, step: 'spawn', state: 'stopping' as const, unconfirmed: true,
      spawnIntentAt: new Date().toISOString(), startedAt: new Date().toISOString() };
    taskStore.upsertTask(makeThread({ threadId, status: 'failed', execution: {
      version: 2, revision: 1, operationId: 'op_deleted_spawn', phase: 'develop', round: 1,
      generation: 4, attempt: 1, autoRetriesUsed: 0, run,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    } }));
    const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
    assert.equal((await control.deleteTask(threadId, 'remove unknown spawn', 'user')).status, 'DELETED');
    const snapshot = loadShutdownPreflightSnapshot(tempDir);
    const report = inspectShutdownPreflight({ ...snapshot, expectedVersion: 'test', requiredComponents: [] });
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((blocker) => blocker.code === 'DELETED_RUN_PROTECTED' && blocker.threadId === threadId));
    assert.ok(report.blockers.some((blocker) => blocker.code === 'DELETED_CLEANUP_PENDING' && blocker.threadId === threadId));
    const repeated = await control.deleteTask(threadId, 'repeat delete', 'user');
    assert.equal(repeated.status, 'DELETED');
    assert.match(repeated.message, /no verified .*exit proof|resources remain isolated/i);
  });

  test('ordinary delete preserves unproven worktree output and still removes the task from the ledger', async () => {
    const threadId = 'thread_control_v2_unproven_output';
    const worktreePath = path.join(tempDir, 'unproven-worktree');
    fs.mkdirSync(worktreePath, { recursive: true });
    const workspace = { ...testWorkspace(os.tmpdir()), executionPath: worktreePath };
    taskStore.upsertTask(makeThread({
      threadId, status: 'failed', workspace,
      worktree: {
        path: worktreePath, mainPath: path.join(tempDir, 'missing-repository'),
        repositoryPath: path.join(tempDir, 'missing-repository'), commonDir: path.join(tempDir, 'missing-repository', '.git'),
        targetRef: 'refs/heads/master', taskBranch: `mesh/task/${threadId}`, baseCommit: 'base-sha',
        deliveryTarget: { branch: 'master', ref: 'refs/heads/master', repositoryPath: path.join(tempDir, 'missing-repository') },
        integration: { phase: 'ready', updatedAt: new Date().toISOString() },
      },
      execution: {
        version: 2, revision: 1, operationId: 'op_unproven_output', phase: 'merge', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
      },
    }));
    const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
      .deleteTask(threadId, 'forget task but retain output', 'user');
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(threadId), undefined);
    assert.equal(fs.existsSync(worktreePath), true);
    assert.match(result.message, /retained/i);
    const record = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/)
      .map((line) => JSON.parse(line)).filter((entry) => entry.threadId === threadId).slice(-1)[0];
    assert.equal(record.cleanupManifest.retainResources, true);
    assert.equal(record.cleanupManifest.thread.worktree.path, worktreePath);
  });

  test('force delete still retains assets when the persisted artifact path is outside its authorized root', async () => {
    const threadId = 'thread_control_v2_path_guard';
    const outside = path.join(tempDir, 'outside', threadId);
    const sentinel = path.join(outside, 'worker_log', `${threadId}_round_1.log`);
    fs.mkdirSync(path.dirname(sentinel), { recursive: true });
    fs.writeFileSync(sentinel, 'must survive');
    taskStore.upsertTask(makeThread({
      threadId, status: 'completed', artifactDir: outside, artifactRoot: path.join(tempDir, 'authorized'),
      rounds: [{ round: 1, type: 'initial', prompt: 'done', output: 'done', status: 'completed', durationMs: 1, timestamp: new Date().toISOString() }],
      execution: { version: 2, revision: 1, operationId: 'op_path_guard', phase: 'merge', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
    }));
    const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
      .deleteTask(threadId, 'force cannot bypass path ownership', 'user', true);
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(threadId), undefined);
    assert.equal(fs.readFileSync(sentinel, 'utf8'), 'must survive');
    assert.match(result.message, /paths could not be verified/i);
  });

  test('a post-tombstone aggregate failure reports deletion success with a residual warning', async () => {
    const threadId = 'thread_control_v2_post_tombstone_warning';
    taskStore.upsertTask(makeThread({
      threadId, status: 'completed',
      execution: { version: 2, revision: 1, operationId: 'op_post_tombstone', phase: 'merge', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
    }));
    const originalMark = taskStore.markTaskCleanup.bind(taskStore);
    taskStore.markTaskCleanup = async (...args: any[]) => {
      const reservation = await (originalMark as any)(...args);
      (taskStore as any).writeAggregate = () => { throw new Error('aggregate disk error'); };
      return reservation;
    };
    const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
      .deleteTask(threadId, 'tombstone is the commit boundary', 'user');
    assert.equal(result.status, 'DELETED');
    assert.equal(taskStore.getTask(threadId), undefined);
    assert.match(result.message, /aggregate update failed/i);
  });

  test('a failed tombstone write remains a storage error and leaves the task visible', async () => {
    const threadId = 'thread_control_v2_tombstone_write_failure';
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', execution: {
      version: 2, revision: 1, operationId: 'op_tombstone_failure', phase: 'merge', round: 1,
      generation: 1, attempt: 1, autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    } }));
    const originalAppend = taskStore.appendTombstone.bind(taskStore);
    (taskStore as any).appendTombstone = () => { throw new Error('injected tombstone fsync failure'); };
    try {
      const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
      await assert.rejects(control.deleteTask(threadId, 'storage fault', 'test'), /injected tombstone fsync failure/);
      assert.equal(taskStore.reloadTask(threadId)?.status, 'completed', 'no successful deletion is reported without a durable tombstone');
      assert.equal(taskStore.isTaskTombstoned(threadId), false);
    } finally { (taskStore as any).appendTombstone = originalAppend; }
  });

  test('post-tombstone cleanup exceptions remain DELETED and mark resources for retry', async () => {
    const threadId = 'thread_control_v2_cleanup_exception';
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', execution: {
      version: 2, revision: 1, operationId: 'op_cleanup_exception', phase: 'merge', round: 1,
      generation: 1, attempt: 1, autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    } }));
    const originalClaim = taskStore.claimTombstoneCleanup.bind(taskStore);
    taskStore.claimTombstoneCleanup = async () => { throw new Error('injected cleaner claim failure'); };
    try {
      const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
        .deleteTask(threadId, 'post-commit cleanup error', 'test');
      assert.equal(result.status, 'DELETED');
      assert.match(result.message, /Physical cleanup remains pending: injected cleaner claim failure/i);
      const latest = fs.readFileSync(path.join(tempDir, 'deleted_tasks.log'), 'utf8').trim().split(/\r?\n/)
        .map((line) => JSON.parse(line)).filter((entry) => entry.threadId === threadId).slice(-1)[0];
      assert.equal(latest.cleanupManifest.retainResources, true);
    } finally { taskStore.claimTombstoneCleanup = originalClaim; }
  });

  test('delete retains worktree behind a junction in its authorized parent chain', async (t) => {
    const threadId = 'thread_control_v2_worktree_junction';
    const externalRoot = path.join(tempDir, 'external-worktrees');
    const worktreesRoot = path.join(tempDir, 'worktrees');
    const worktreePath = path.join(worktreesRoot, threadId);
    fs.mkdirSync(externalRoot, { recursive: true });
    fs.mkdirSync(tempDir, { recursive: true });
    try { fs.symlinkSync(externalRoot, worktreesRoot, 'junction'); }
    catch (error) { t.skip(`directory junction unavailable: ${(error as Error).message}`); return; }
    fs.mkdirSync(path.join(externalRoot, threadId), { recursive: true });
    const repositoryPath = path.join(tempDir, 'missing-repository');
    taskStore.upsertTask(makeThread({ threadId, status: 'completed',
      workspace: { ...testWorkspace(os.tmpdir()), executionPath: worktreePath },
      worktree: { path: worktreePath, mainPath: repositoryPath, repositoryPath,
        commonDir: path.join(repositoryPath, '.git'), targetRef: 'refs/heads/master',
        taskBranch: `mesh/task/${threadId}`, baseCommit: 'base-sha',
        deliveryTarget: { branch: 'master', ref: 'refs/heads/master', repositoryPath },
        integration: { phase: 'no_changes', updatedAt: new Date().toISOString() } },
      execution: { version: 2, revision: 1, operationId: 'op_worktree_junction', phase: 'merge', round: 1,
        generation: 1, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 } },
    }));
    const result = await new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir)
      .deleteTask(threadId, 'junction path must be retained', 'test', true);
    assert.equal(result.status, 'DELETED');
    assert.equal(fs.existsSync(path.join(externalRoot, threadId)), true);
    assert.match(result.message, /Git worktree or branch ownership is unverified/i);
  });

  test('a tombstoned file that cannot be unlinked cannot accept a late execution transition', async () => {
    const threadId = 'thread_control_v2_tombstone_unlink_failure';
    taskStore.upsertTask(makeThread({ threadId, status: 'completed', execution: {
      version: 2, revision: 1, operationId: 'op_unlink_failure', phase: 'merge', round: 1,
      generation: 1, attempt: 1, autoRetriesUsed: 0,
      budget: { limitMs: 120_000, spentActiveMs: 0, maxAutoRetries: 2 },
    } }));
    const taskFile = path.join(tempDir, 'threads', `${threadId}.json`);
    const originalUnlink = mutableFs.unlinkSync;
    mutableFs.unlinkSync = (file: fs.PathLike) => {
      if (path.resolve(String(file)) === path.resolve(taskFile)) throw new Error('injected task unlink failure');
      return originalUnlink.call(mutableFs, file);
    };
    try {
      const control = new TaskControlService(taskStore, runner, new CancellationRegistry(tempDir), tempDir);
      const result = await control.deleteTask(threadId, 'unlink fault', 'test');
      assert.equal(result.status, 'DELETED');
      assert.match(result.message, /task file unlink failed/i);
      assert.equal(taskStore.reloadTask(threadId), undefined);
      const before = fs.readFileSync(taskFile, 'utf8');
      await assert.rejects(taskStore.transitionExecution(threadId, 1, (task) => { task.status = 'running'; }), /durable deletion tombstone/);
      assert.equal(fs.readFileSync(taskFile, 'utf8'), before, 'late writers cannot revive the still-present ledger file');
    } finally { mutableFs.unlinkSync = originalUnlink; }
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
    const asset = path.join(artifactDir, 'worker_log', `${threadId}_round.log`);
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
        reviewerSessionCaptured: false, artifactDir, artifactRoot: path.dirname(artifactDir), worktree: undefined },
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

  test('historical paused tasks cannot resume through the retired controller path', async () => {
    const thread = makeThread({ status: 'paused' });
    taskStore.upsertTask(thread);
    const callbackManager = new CallbackManager(new AgentDetector());
    const reworkController = new ReworkController(taskStore, runner, callbackManager, new AgentDetector());
    await assert.rejects(reworkController.resumePausedTask(thread.threadId), /HISTORICAL_TASK_READ_ONLY/);
    assert.deepEqual(taskStore.getTask(thread.threadId), thread);
    reworkController.dispose();
  });
});