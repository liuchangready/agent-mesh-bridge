import { test, TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawnSync } from 'node:child_process';
import type { DelegationThread } from '../bridge/types';
import { MockMcpClient, MockScenario } from './fixtures/mock-agent-protocol';

function git(cwd: string, ...args: string[]): string {
  const executable = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:/Git/cmd/git.exe' : 'git');
  const env: NodeJS.ProcessEnv = { GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: path.resolve(cwd, '..', 'home', '.gitconfig') };
  for (const key of ['SystemRoot', 'WINDIR', 'ComSpec', 'PATHEXT', 'PATH', 'TEMP', 'TMP']) env[key] = process.env[key];
  const result = spawnSync(executable, args, { cwd, env, encoding: 'utf8', windowsHide: true });
  assert.equal(result.status, 0, result.stderr);
  return result.stdout.trim();
}

function trace(root: string): any[] {
  return fs.readdirSync(path.join(root, 'events')).map(name =>
    JSON.parse(fs.readFileSync(path.join(root, 'events', name), 'utf8')),
  ).sort((a, b) => BigInt(a.at) < BigInt(b.at) ? -1 : BigInt(a.at) > BigInt(b.at) ? 1 : 0);
}

/** Observe durable task files; do not keep a caller turn alive by polling MCP. */
async function waitForTask(root: string, threadId: string, predicate: (task: DelegationThread) => boolean, timeoutMs = 360_000): Promise<DelegationThread> {
  const dir = path.join(root, 'home', '.agent-mesh-bridge', 'threads');
  const file = path.join(dir, `${threadId}.json`);
  return new Promise((resolve, reject) => {
    let latest: DelegationThread | undefined;
    const finish = (error?: Error) => {
      clearTimeout(timer);
      watcher.close();
      if (error) reject(error); else resolve(latest!);
    };
    const check = () => {
      try {
        latest = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (predicate(latest!)) finish();
        else if (latest?.status === 'failed' && !latest.execution?.run) {
          finish(new Error(`Unexpected failure: ${JSON.stringify(latest.failure)}; evidence=${root}`));
        }
      } catch { /* Atomic replacement can briefly hide the file. */ }
    };
    const watcher = fs.watch(dir, check);
    // Real Windows OS-lock helpers and concurrent Git suites can exceed three minutes.
    const timer = setTimeout(() => finish(new Error(`Task timed out; evidence=${root}; status=${latest?.status}; run=${JSON.stringify(latest?.execution)}`)), timeoutMs);
    check();
  });
}

async function fixture(t: TestContext, scenario: MockScenario) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'amb-e2e-'));
  const repo = path.join(root, 'repo');
  const home = path.join(root, 'home');
  const storage = path.join(home, '.agent-mesh-bridge');
  for (const dir of [repo, storage, path.join(root, 'events'), path.join(root, 'tasks')]) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(root, 'mock-only.json'), JSON.stringify(scenario));
  fs.writeFileSync(path.join(home, '.gitconfig'), '');
  fs.writeFileSync(path.join(storage, 'model-config.json'), JSON.stringify({
    models: { codex: 'mock-worker' }, initiatorModels: { codex: 'mock-reviewer' }, requireModelApproval: true,
  }));
  fs.writeFileSync(path.join(storage, 'bridge-policy.json'), JSON.stringify({
    reviewFlow: 'dedicated', taskDataRoot: path.join(root, 'tasks'), contextCompaction: false,
    nativeSessionCompaction: { mode: 'off' }, concurrency: { enabled: false },
  }));
  fs.writeFileSync(path.join(repo, 'result.txt'), 'base\n');
  fs.writeFileSync(path.join(repo, '.gitignore'), '.agent_bridge/\n');
  fs.mkdirSync(path.join(repo, '.agent-mesh-bridge'));
  fs.writeFileSync(path.join(repo, '.agent-mesh-bridge', 'validate.json'), JSON.stringify({
    commands: [
      { command: process.execPath, args: ['validate.cjs', 'compile'] },
      { command: process.execPath, args: ['validate.cjs', 'test'] },
    ],
  }));
  fs.writeFileSync(path.join(repo, 'validate.cjs'), [
    "const fs=require('fs'), assert=require('assert/strict');",
    `assert.equal(fs.readFileSync('result.txt','utf8'),${JSON.stringify(`round ${scenario.rounds}\n`)});`,
    `fs.appendFileSync(${JSON.stringify(path.join(root, 'validation.jsonl'))},JSON.stringify({step:process.argv[2],cwd:process.cwd()})+String.fromCharCode(10));`,
  ].join('\n'));
  git(repo, 'init', '-b', 'master');
  git(repo, 'config', 'user.name', 'Mock Agent');
  git(repo, 'config', 'user.email', 'mock@example.test');
  git(repo, 'config', 'core.autocrlf', 'false');
  git(repo, 'add', '.');
  git(repo, 'commit', '-m', 'isolated fixture base');
  const base = git(repo, 'rev-parse', 'HEAD');

  // Explicit allowlist: real provider credentials, CLI paths, homes and mesh
  // identities cannot enter a fixture from the developer's environment.
  const env: NodeJS.ProcessEnv = {};
  for (const key of ['SystemRoot', 'WINDIR', 'ComSpec', 'PATHEXT', 'PATH', 'TEMP', 'TMP']) env[key] = process.env[key];
  Object.assign(env, {
    HOME: home, USERPROFILE: home, APPDATA: path.join(home, 'AppData', 'Roaming'),
    LOCALAPPDATA: path.join(home, 'AppData', 'Local'), CODEX_HOME: path.join(home, '.codex'),
    AGENT_MESH_BRIDGE_HOME: storage, MESH_MOCK_ROOT: root, MESH_CALLER_AGENT: 'codex',
    GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: path.join(home, '.gitconfig'),
    MESH_GIT_PATH: process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:/Git/cmd/git.exe' : 'git'),
  });
  const client = new MockMcpClient(env, repo);
  const state: { threadId?: string; passed: boolean } = { passed: false };
  t.after(async () => {
    try {
      if (!state.passed && state.threadId) {
        try { await client.tool('pause_task', { threadId: state.threadId, reason: 'Fixture teardown' }); } catch {}
      }
      await client.close();
      const events = trace(root);
      for (const start of events.filter(entry => entry.kind === 'process.start')) {
        assert.ok(events.some(entry => entry.kind === 'process.close' && entry.launchId === start.launchId),
          `Mock child did not close: ${start.childPid}; evidence=${root}`);
      }
      if (state.passed) {
        const resolved = fs.realpathSync.native(root);
        const temp = fs.realpathSync.native(os.tmpdir()) + path.sep;
        assert.ok(resolved.startsWith(temp) && path.basename(resolved).startsWith('amb-e2e-'));
        fs.rmSync(resolved, { recursive: true, force: true });
      } else t.diagnostic(`Mock E2E evidence retained: ${root}\n${client.stderr}`);
    } catch (error) { t.diagnostic(`Mock E2E cleanup evidence: ${root}`); throw error; }
  });
  await client.initialize();
  const templates = await client.tool('list_task_templates', { workspaceRoot: repo });
  assert.ok(templates.templates.some((entry: any) => entry.id === 'programming'));
  const dispatch = await client.tool('dispatch_task', {
    workspaceRoot: repo, targetBranch: 'master', targetAgent: 'codex', initiatorAgent: 'codex',
    title: 'Scripted multi-round acceptance', prompt: 'Write result.txt for each requested revision', templateId: 'programming',
  });
  state.threadId = dispatch.threadId;
  assert.equal(dispatch.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
  assert.equal(dispatch.status, 'REVIEWER_STARTING');
  return { root, repo, base, client, threadId: state.threadId!, finish: () => { state.passed = true; } };
}

test('mock agents: real MCP bootstrap, three worker/reviewer rounds and validated Git delivery', { timeout: 420_000 }, async t => {
  const f = await fixture(t, { rounds: 3, reviewerTailMs: 200 });
  const task = await waitForTask(f.root, f.threadId, task => task.status === 'completed' && !task.execution?.run);
  const events = trace(f.root);
  const agents = events.filter(entry => entry.kind === 'agent.start');
  const workers = agents.filter(entry => entry.role === 'worker');
  const reviewers = agents.filter(entry => entry.role === 'reviewer');
  assert.equal(agents.filter(entry => entry.role === 'bootstrap').length, 1);
  assert.equal(events.filter(entry => entry.kind === 'bootstrap.confirmed').length, 1);
  assert.deepEqual(workers.map(entry => entry.round), [1, 2, 3]);
  assert.deepEqual(workers.map(entry => entry.resume), [false, true, true]);
  assert.equal(new Set(workers.map(entry => entry.sessionId)).size, 1);
  assert.equal(reviewers.length, 3);
  assert.ok(reviewers.every(entry => entry.resume && entry.sessionId === task.reviewerSessionId));
  assert.notEqual(task.reviewerSessionId, task.receiverSessionId);
  assert.ok(workers.every(entry => entry.model === 'model="mock-worker"'));
  assert.ok(reviewers.every(entry => entry.model === 'model="mock-reviewer"'));
  assert.equal(events.filter(entry => entry.kind === 'receiver.guards').length, 3);
  assert.deepEqual(events.filter(entry => entry.kind === 'reviewer.verdict').map(entry => entry.tool),
    ['request_rework', 'request_rework', 'approve_task']);
  for (const agent of workers.slice(1)) {
    const precedingReviewer = reviewers[workers.indexOf(agent) - 1];
    const closed = events.find(entry => entry.kind === 'process.close' && entry.launchId === precedingReviewer.launchId);
    assert.ok(closed && BigInt(closed.at) < BigInt(agent.at), 'Next worker started before the reviewer process closed');
  }
  assert.deepEqual(task.rounds.filter(round => round.type !== 'approval').map(round => round.round), [1, 2, 3]);
  assert.ok(task.rounds.filter(round => round.type !== 'approval').every(round => round.assessment?.success));
  assert.equal(task.worktree?.integration?.phase, 'merged');
  assert.equal(task.worktree?.validation?.result, 'passed');
  assert.ok(task.finalSummary);
  assert.equal(fs.readFileSync(path.join(f.repo, 'result.txt'), 'utf8'), 'round 3\n');
  assert.notEqual(git(f.repo, 'rev-parse', 'HEAD'), f.base);
  assert.equal(git(f.repo, 'status', '--porcelain'), '');
  const validation = fs.readFileSync(path.join(f.root, 'validation.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  assert.deepEqual(validation.map(entry => entry.step), ['compile', 'test']);
  assert.ok(validation.every(entry => entry.cwd !== f.repo), 'Validation must use the combined validation worktree');
  assert.equal(fs.readdirSync(path.join(f.root, 'home', '.agent-mesh-bridge', 'threads')).filter(name => name.endsWith('.json')).length, 1);
  const summary = await f.client.tool('get_task_summary', { threadId: f.threadId });
  assert.equal(summary.status, 'completed');
  assert.ok(summary.finalSummary);
  f.finish();
});

for (const fault of ['empty-worker', 'nonzero-worker', 'no-verdict', 'bad-bootstrap-token'] as const) {
  test(`mock agents fail closed: ${fault}`, { timeout: 180_000 }, async t => {
    const f = await fixture(t, { rounds: 1, fault });
    const task = await waitForTask(f.root, f.threadId, task => task.status === 'failed' && !task.execution?.run, 120_000);
    const events = trace(f.root);
    assert.equal(git(f.repo, 'rev-parse', 'HEAD'), f.base, 'Failed task must never advance the delivery target');
    assert.equal(fs.existsSync(path.join(f.root, 'validation.jsonl')), false);
    assert.equal(events.filter(entry => entry.kind === 'reviewer.verdict').length, 0);
    if (fault === 'bad-bootstrap-token') {
      assert.equal(task.reviewBootstrap?.phase, 'failed');
      assert.equal(events.filter(entry => entry.kind === 'agent.start' && entry.role === 'worker').length, 0);
    } else if (fault === 'no-verdict') {
      assert.equal(task.failure?.stage, 'review');
    } else {
      const round = task.rounds.find(round => round.type !== 'approval')!;
      assert.equal(round.assessment?.success, false);
      assert.equal(round.assessment?.errorCode, fault === 'empty-worker' ? 'EMPTY_OUTPUT' : 'NONZERO_EXIT');
      assert.equal(events.filter(entry => entry.kind === 'agent.start' && entry.role === 'reviewer').length, 0);
    }
    f.finish();
  });
}
