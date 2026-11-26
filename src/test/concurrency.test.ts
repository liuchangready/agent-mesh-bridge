import { testWorkspace, confirmTestReviewer } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-concurrency-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;

function initGitRepo(root: string): void {
  const run = (...args: string[]) => {
    const result = spawnSync(gitExe, args, { cwd: root, encoding: 'utf8', windowsHide: true });
    if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  };
  run('init', '-b', 'test/source');
  run('config', 'user.name', 'Mesh Test');
  run('config', 'user.email', 'mesh-test@example.test');
  run('commit', '--allow-empty', '-m', 'base');
}

import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import {
  DEFAULT_CONCURRENCY_POLICY,
  concurrencyPolicy,
  readBridgePolicy,
  saveBridgePolicy,
} from '../bridge/bridge-policy';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner, RunOptions, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';
import { DelegationThread, pendingMerges } from '../bridge/types';
import { readRecentTaskLogs, roundExecutionLogPath } from '../bridge/round-log';
import { createThread } from './unified-execution-fixture';
import { getProcessBirthId } from '../bridge/platform';

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + ('thread_merge_1')),
    callerSessionId: ('reviewer-' + ('thread_merge_1')),
    threadId: 'thread_merge_1',
    title: 'Merge gate probe',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'completed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'receiver-session-0001',
    workspace: testWorkspace(os.tmpdir()),
    summary: { lastPromptSnippet: 'work', lastOutputSnippet: 'done', totalDurationMs: 1 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', output: 'done', status: 'completed', durationMs: 1, timestamp: now }],
    ...overrides,
  };
}

const temporaryDirs: string[] = [];
function trackedTempDir(prefix: string): string {
  const dir = makeTempDir(prefix);
  temporaryDirs.push(dir);
  return dir;
}
afterEach(() => {
  while (temporaryDirs.length) fs.rmSync(temporaryDirs.pop()!, { recursive: true, force: true });
});

async function waitFor(predicate: () => boolean, timeoutMs = 2_000): Promise<void> {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.ok(predicate(), 'condition did not become true before timeout');
}

function success(title?: string): RunResult {
  return { exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1 };
}

describe('bridge policy - optional best-effort concurrency limits', () => {
  let root: string;
  beforeEach(() => { root = trackedTempDir('agent-bridge-policy-'); });

  test('disabled means no configured cap; getter shape and historical enabled semantics stay stable', () => {
    assert.deepStrictEqual(concurrencyPolicy(root), DEFAULT_CONCURRENCY_POLICY);
    assert.strictEqual(concurrencyPolicy(root).enabled, false);
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 });
    saveBridgePolicy({ concurrency: { maxConcurrentTasks: 4 } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 4, maxConcurrentPerAgent: 2 });
    assert.strictEqual(readBridgePolicy(root).concurrency?.enabled, true);
  });

  test('invalid limits preserve prior values and only explicit true enables them', () => {
    saveBridgePolicy({ concurrency: { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 } }, root);
    saveBridgePolicy({ concurrency: { maxConcurrentTasks: -5, maxConcurrentPerAgent: Number.NaN } }, root);
    assert.deepStrictEqual(concurrencyPolicy(root), { enabled: true, maxConcurrentTasks: 3, maxConcurrentPerAgent: 2 });
    saveBridgePolicy({ concurrency: { enabled: false } }, root);
    assert.strictEqual(concurrencyPolicy(root).enabled, false);
  });
});

describe('merge gate - pendingMerges', () => {
  test('local integration state controls pending results, regardless of MR metadata', () => {
    const older = makeThread({ threadId: 'thread_old', createdAt: '2026-10-01T00:00:00.000Z', worktree: { path: 'D:/wt/old', mainPath: 'D:/repo', integration: { phase: 'ready', updatedAt: '2026-10-01T00:00:00.000Z' } } });
    const newer = makeThread({ threadId: 'thread_new', createdAt: '2026-10-02T00:00:00.000Z', worktree: { path: 'D:/wt/new', mainPath: 'D:/repo', integration: { phase: 'ready', updatedAt: '2026-10-02T00:00:00.000Z' } }, mergeRequest: { url: 'https://example.com/mr/1', status: 'synced', updatedAt: '2026-10-02T00:00:00.000Z' } });
    const merged = makeThread({ threadId: 'thread_merged', createdAt: '2026-10-03T00:00:00.000Z', worktree: { path: 'D:/wt/merged', mainPath: 'D:/repo', mergePending: true, integration: { phase: 'merged', updatedAt: '2026-10-03T00:00:00.000Z' } } });
    const noWorktree = makeThread({ threadId: 'thread_plain', createdAt: '2026-10-04T00:00:00.000Z' });
    const stillRunning = makeThread({ threadId: 'thread_running', createdAt: '2026-10-05T00:00:00.000Z', status: 'running', worktree: { path: 'D:/wt/running', mainPath: 'D:/repo' } });
    assert.deepStrictEqual(pendingMerges([newer, noWorktree, merged, older, stillRunning]).map((task) => task.threadId), ['thread_old', 'thread_new']);
  });
});
