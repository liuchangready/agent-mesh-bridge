import { testWorkspace } from './workspace-fixture';
import { test, after } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { createRequire } from 'module';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { createTaskWorktree, resolveGitRepository, commitTaskChanges, gitOutput } from '../bridge/git-worktree';
import { ReworkController } from '../bridge/rework-controller';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';
import { MCPServer } from '../bridge/mcp-server';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AsyncDispatcher } from '../bridge/async-dispatcher';

// Nested test merges must not contend with an outer production integration lock.
const residentHome = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resident-home-'));
const mutableOs = createRequire(__filename)('os') as { homedir: typeof os.homedir };
const originalHomedir = mutableOs.homedir;
mutableOs.homedir = () => residentHome;
after(() => {
  mutableOs.homedir = originalHomedir;
  fs.rmSync(residentHome, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
});

const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;

function git(cwd: string, ...args: string[]): string {
  const result = spawnSync(gitExe, args, { cwd, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  return result.stdout.trim();
}

function initRepo(root: string): void {
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-b', 'caller'); git(root, 'config', 'user.name', 'Mesh Test'); git(root, 'config', 'user.email', 'mesh@test.invalid');
  fs.writeFileSync(path.join(root, 'shared.txt'), 'base\n');
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ scripts: { compile: 'node validate.js compile', test: 'node validate.js test' } }));
  fs.writeFileSync(path.join(root, 'validate.js'), `const fs=require('fs');if(process.argv[2]==='test'&&!fs.readFileSync('shared.txt','utf8').includes('changed'))process.exit(2)`);
  git(root, 'add', '-A'); git(root, 'commit', '-m', 'base');
}

function makeThread(threadId: string, worktree: ReturnType<typeof createTaskWorktree>): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code', status: 'waiting_review', currentRound: 1,
    createdAt: now, updatedAt: now, receiverSessionId: `session-${threadId}`, workspace: testWorkspace(worktree.path),
    summary: { lastPromptSnippet: 'review', lastOutputSnippet: 'done', totalDurationMs: 1 },
    rounds: [{ round: 1, type: 'initial', prompt: 'change', output: 'changed file', durationMs: 1, status: 'completed', timestamp: now }],
    worktree: { ...worktree, headCommit: worktree.headCommit, integration: { phase: 'ready', updatedAt: now } },
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) { if (predicate()) return; await new Promise((resolve) => setTimeout(resolve, 50)); }
  throw new Error('Timed out waiting for resident merge host');
}

function makeController(store: TaskStore, resident = false) {
  const detector = new AgentDetector();
  return new ReworkController(store, new AutonomousRunner(detector), new CallbackManager(detector), detector, undefined, resident);
}

test('resident host observes a short-lived MCP approval from disk and merges without restart or polling', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resident-merge-'));
  const repo = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repo);
  const target = resolveGitRepository(repo, storage); const worktree = createTaskWorktree(target, 'thread_resident_merge', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'changed\n'); worktree.headCommit = commitTaskChanges(worktree, 'task change');
  const hostStore = new TaskStore(storage); const mcpStore = new TaskStore(storage);
  const host = makeController(hostStore, true); const shortMcp = makeController(mcpStore);
  t.after(() => { host.dispose(); hostStore.dispose(); mcpStore.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  mcpStore.upsertTask(makeThread('thread_resident_merge', worktree));
  assert.equal(shortMcp.approveTask({ threadId: 'thread_resident_merge' }).status, 'WAITING_MERGE');
  await waitFor(() => hostStore.getTask('thread_resident_merge')?.status === 'completed', 30_000);
  assert.equal(gitOutput(repo, ['rev-parse', target.targetRef]), hostStore.getTask('thread_resident_merge')?.worktree?.integration?.mergedCommit);
});

test('pre-ref merge infrastructure failure returns a concrete error to the original reviewer', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-review-error-'));
  const repo = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repo);
  const target = resolveGitRepository(repo, storage); const worktree = createTaskWorktree(target, 'thread_merge_review_error', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'changed\n'); worktree.headCommit = commitTaskChanges(worktree, 'task change');
  worktree.targetRef = 'refs/heads/missing-target'; worktree.deliveryTarget = undefined;
  const store = new TaskStore(storage); const detector = new AgentDetector(); const callback = new CallbackManager(detector);
  const notified: DelegationThread[] = []; callback.notifyCaller = async (thread) => { notified.push(thread); return true; };
  const controller = new ReworkController(store, new AutonomousRunner(detector), callback, detector);
  t.after(() => { store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  const original = makeThread('thread_merge_review_error', worktree); original.callerSessionId = 'reviewer-session-original';
  store.upsertTask({ ...original, status: 'waiting_merge', worktree: { ...worktree, approvedCommit: worktree.headCommit, integration: { phase: 'waiting', updatedAt: new Date().toISOString() } } });
  controller.resumePendingMerges();
  await waitFor(() => notified.length === 1);
  const latest = store.getTask('thread_merge_review_error')!;
  assert.equal(latest.status, 'waiting_merge');
  assert.equal(latest.worktree?.approvedCommit, worktree.headCommit);
  assert.match(latest.worktree?.integration?.error || '', /Git|fatal|unknown revision|not a valid object/i);
  assert.equal(notified[0].threadId, 'thread_merge_review_error');
  assert.equal(notified[0].callerSessionId, original.callerSessionId);
  assert.ok(latest.mergeHistory?.some((entry) => entry.phase === 'coordinator' && entry.result === 'failed' && /Git|fatal|revision|object/i.test(entry.error || '')));
});

test('a landed ref with repeated ledger EIO is not retried in a loop and recovers after host restart', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resident-ledger-recovery-'));
  const repo = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repo);
  const target = resolveGitRepository(repo, storage); const worktree = createTaskWorktree(target, 'thread_resident_ledger_eio', storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'changed\n'); worktree.headCommit = commitTaskChanges(worktree, 'task change');
  const task = makeThread('thread_resident_ledger_eio', worktree); task.status = 'waiting_merge'; task.worktree!.approvedCommit = worktree.headCommit;
  task.worktree!.integration = { phase: 'waiting', updatedAt: new Date().toISOString() };
  const store = new TaskStore(storage); store.upsertTask(task);
  let host: ReworkController;
  const mutableFs = createRequire(__filename)('fs') as { renameSync: typeof fs.renameSync };
  const realRename = mutableFs.renameSync; const taskFile = path.join(storage, 'threads', 'thread_resident_ledger_eio.json');
  const txFile = path.join(storage, 'merge-transactions', 'thread_resident_ledger_eio.json'); let writeFailures = 0;
  mutableFs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(to) === taskFile && fs.existsSync(txFile) && JSON.parse(fs.readFileSync(txFile, 'utf8')).phase === 'ref_updated') { writeFailures++; const error: NodeJS.ErrnoException = new Error('injected EIO'); error.code = 'EIO'; throw error; }
    return realRename(from, to);
  }) as typeof fs.renameSync;
  host = makeController(store, true);
  t.after(() => { mutableFs.renameSync = realRename; host.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true }); });
  await waitFor(() => gitOutput(repo, ['rev-parse', target.targetRef]) !== target.baseCommit);
  const landed = gitOutput(repo, ['rev-parse', target.targetRef]);
  await waitFor(() => writeFailures > 0);
  await new Promise((resolve) => setTimeout(resolve, 1_000));
  const failuresAfterSettlement = writeFailures;
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  assert.equal(writeFailures, failuresAfterSettlement, 'the landed transaction is parked instead of retrying each poll');
  assert.equal(gitOutput(repo, ['rev-parse', target.targetRef]), landed);
  assert.equal(JSON.parse(fs.readFileSync(txFile, 'utf8')).phase, 'ref_updated');
  assert.equal(store.getTask('thread_resident_ledger_eio')?.status, 'waiting_merge');
  assert.equal(JSON.parse(fs.readFileSync(taskFile, 'utf8')).status, 'waiting_merge');

  mutableFs.renameSync = realRename;
  host.dispose(); store.dispose();
  const restartedStore = new TaskStore(storage); host = makeController(restartedStore, true);
  t.after(() => { host.dispose(); restartedStore.dispose(); });
  await waitFor(() => restartedStore.getTask('thread_resident_ledger_eio')?.status === 'completed');
  assert.equal(gitOutput(repo, ['rev-parse', target.targetRef]), restartedStore.getTask('thread_resident_ledger_eio')?.worktree?.integration?.mergedCommit);
  assert.equal(gitOutput(repo, ['rev-parse', target.targetRef]), landed, 'recovery preserves the landed commit');
});

test('an initial ledger EIO is logged and backed off until the same host can recover', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-resident-pre-ref-eio-'));
  const repo = path.join(root, 'repo'); const storage = path.join(root, 'ledger'); initRepo(repo);
  const target = resolveGitRepository(repo, storage); const id = 'thread_resident_pre_ref_eio';
  const worktree = createTaskWorktree(target, id, storage);
  fs.writeFileSync(path.join(worktree.path, 'shared.txt'), 'changed\n'); worktree.headCommit = commitTaskChanges(worktree, 'task change');
  const task = makeThread(id, worktree); task.status = 'waiting_merge'; task.worktree!.approvedCommit = worktree.headCommit;
  task.worktree!.integration = { phase: 'waiting', updatedAt: new Date().toISOString() };
  const store = new TaskStore(storage); store.upsertTask(task);
  const detector = new AgentDetector(); const callback = new CallbackManager(detector); let wakes = 0;
  callback.notifyCaller = async () => { wakes++; return true; };
  const mutableFs = createRequire(__filename)('fs') as { renameSync: typeof fs.renameSync };
  const realRename = mutableFs.renameSync; const taskFile = path.join(storage, 'threads', `${id}.json`); let failures = 0;
  mutableFs.renameSync = ((from: fs.PathLike, to: fs.PathLike) => {
    if (String(to) === taskFile) { failures++; const error: NodeJS.ErrnoException = new Error('injected EIO'); error.code = 'EIO'; throw error; }
    return realRename(from, to);
  }) as typeof fs.renameSync;
  const host = new ReworkController(store, new AutonomousRunner(detector), callback, detector, undefined, true);
  t.after(() => { mutableFs.renameSync = realRename; host.dispose(); store.dispose(); fs.rmSync(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); });
  await waitFor(() => failures >= 2);
  const failuresAfterSettlement = failures;
  await new Promise((resolve) => setTimeout(resolve, 1_500));
  assert.equal(failures, failuresAfterSettlement, 'one-second host polls must not retry a failed ledger write');
  assert.equal(wakes, 0); assert.equal(gitOutput(repo, ['rev-parse', target.targetRef]), target.baseCommit);
  assert.equal(store.getTask(id)?.status, 'waiting_merge');
  const logs = path.join(storage, 'merge-logs', id, 'merge_log');
  const originalLog = path.join(logs, `${id}_merge_1.log`);
  const originalContent = fs.readFileSync(originalLog, 'utf8'); assert.match(originalContent, /EIO/i);
  mutableFs.renameSync = realRename;
  await waitFor(() => store.getTask(id)?.status === 'completed', 30_000);
  assert.notEqual(gitOutput(repo, ['rev-parse', target.targetRef]), target.baseCommit);
  assert.equal(store.getTask(id)?.threadId, id);
  const event = store.getTask(id)!.mergeHistory!.find((entry) => entry.phase === 'attempt' && entry.result === 'started')!;
  assert.equal(event.number, 1); assert.equal(event.logFileNumber, 2);
  const mcp = new MCPServer({} as AsyncDispatcher, host, store, detector, new ActiveAgentResolver(detector), new AutonomousRunner(detector));
  const first = await mcp.handleToolCall('get_merge_log', { threadId: id, number: event.number, limit: 16 });
  const second = await mcp.handleToolCall('get_merge_log', { threadId: id, number: event.number, offset: first.nextOffset, limit: 16 });
  const savedLog = fs.readFileSync(event.logPath!);
  assert.equal(first.content, savedLog.subarray(0, 16).toString('utf8')); assert.equal(first.nextOffset, 16);
  assert.equal(second.content, savedLog.subarray(16, 32).toString('utf8')); assert.equal(second.nextOffset, 32);
  assert.equal(second.size, savedLog.length); assert.match(savedLog.toString('utf8'), /\$ git/);
  let fullLog = first.content + second.content; let offset = second.nextOffset;
  while (offset < second.size) {
    const page = await mcp.handleToolCall('get_merge_log', { threadId: id, number: event.number, offset, limit: 64 });
    assert.ok(page.nextOffset > offset); fullLog += page.content; offset = page.nextOffset;
  }
  assert.equal(fullLog, savedLog.toString('utf8'));
  assert.equal(fs.readFileSync(originalLog, 'utf8'), originalContent);
});
