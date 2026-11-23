import { test, TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawnSync } from 'node:child_process';
import type { DelegationThread } from '../bridge/types';
import { MockMcpClient, MockScenario } from './fixtures/mock-agent-protocol';
import { getProcessBirthId, probeProcessIdentity } from '../bridge/platform';

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

async function waitForFile(file: string, timeoutMs = 120_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!fs.existsSync(file) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
  assert.ok(fs.existsSync(file), `Timed out waiting for ${file}`);
}

async function waitForEvent(root: string, kind: string, predicate: (entry: any) => boolean = () => true): Promise<any> {
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    const found = trace(root).find(entry => entry.kind === kind && predicate(entry));
    if (found) return found;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${kind}; evidence=${root}`);
}

async function fixture(t: TestContext, scenario: MockScenario) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'amb-e2e-'));
  const repo = path.join(root, 'repo');
  const home = path.join(root, 'home');
  const storage = path.join(home, '.agent-mesh-bridge');
  for (const dir of [repo, storage, path.join(root, 'events'), path.join(root, 'tasks')]) fs.mkdirSync(dir, { recursive: true });
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify({
    schemaVersion: 1, cutoverApproved: true,
    components: [
      { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js'), completeProcessInventory: true, previousProcesses: [] },
      { component: 'standalone-mcp', version, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs'), completeProcessInventory: true, previousProcesses: [] },
    ],
  }));
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
    ...(scenario.pauseAt === 'merge' ? [
      `const gate=${JSON.stringify(path.join(root, 'merge'))}; if(!fs.existsSync(gate+'.pause-marker')){fs.writeFileSync(gate+'.pause-marker','held');fs.writeFileSync(gate+'.ready',JSON.stringify({pid:process.pid,at:process.hrtime.bigint().toString()}));const until=Date.now()+240000;while(!fs.existsSync(gate+'.release')&&Date.now()<until)Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,20);if(!fs.existsSync(gate+'.release'))throw Error('merge gate timed out');}`,
    ] : []),
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
    NODE_OPTIONS: `${process.env.NODE_OPTIONS || ''} --require ${path.resolve(__dirname, '../../src/test/fixtures/mock-agent-spawn.cjs')}`.trim(),
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
        fs.rmSync(resolved, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
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
  const task = await waitForTask(
    f.root,
    f.threadId,
    task => task.status === 'completed' && task.worktree?.integration?.phase === 'merged' && !task.execution?.run,
    80_000,
  );
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

for (const phase of ['bootstrap', 'develop', 'review', 'merge'] as const) {
  test(`mock v2 pause/resume: same task, sessions and delivery through ${phase}`, { timeout: 360_000 }, async t => {
    const rounds = phase === 'develop' ? 2 : 1;
    const pauseRound = phase === 'develop' ? 2 : undefined;
    const f = await fixture(t, { rounds, pauseAt: phase, pauseRound });
    const mergeReady = path.join(f.root, 'merge.ready');
    let heldValidation: { pid: number; birthId?: string } | undefined;
    let mergeGateAt = '';
    let agentsBeforeMergeResume = 0;
    let firstRunId: string;
    let pausedTask: DelegationThread;
    let gateEvent: any;

    if (phase === 'merge') {
      await waitForFile(mergeReady);
      const gate = JSON.parse(fs.readFileSync(mergeReady, 'utf8'));
      mergeGateAt = gate.at;
      agentsBeforeMergeResume = trace(f.root).filter(entry => entry.kind === 'agent.start').length;
      heldValidation = { pid: gate.pid, birthId: getProcessBirthId(gate.pid) };
      assert.ok(heldValidation.birthId, 'the held validation child must have captured process identity');
      pausedTask = await waitForTask(f.root, f.threadId, task => task.status === 'merging' && task.execution?.run?.state === 'active' && task.execution.phase === 'merge');
    } else {
      const expectedRole = phase === 'develop' ? 'worker' : phase === 'review' ? 'reviewer' : 'bootstrap';
      gateEvent = await waitForEvent(f.root, 'agent.pause-ready', entry => entry.role === expectedRole &&
        (pauseRound === undefined || entry.round === pauseRound));
      pausedTask = await waitForTask(f.root, f.threadId, task => task.execution?.run?.state === 'active' &&
        (phase === 'develop' ? task.execution.phase === 'develop' && task.execution.round === pauseRound :
          phase === 'bootstrap' ? task.execution.phase === 'review' : task.execution.phase === 'review'));
    }
    assert.equal(pausedTask.execution?.version, 2, 'the task must use admitted production v2 execution');
    assert.ok(pausedTask.execution.run?.id);
    firstRunId = pausedTask.execution.run.id;
    assert.equal(pausedTask.threadId, f.threadId);
    const sameWorktree = pausedTask.worktree?.path;
    const receiverSession = pausedTask.receiverSessionCaptured ? pausedTask.receiverSessionId : undefined;
    const reviewerSession = pausedTask.reviewerSessionCaptured ? pausedTask.reviewerSessionId : undefined;
    if (phase === 'develop') assert.ok(receiverSession, 'round 2 starts after the developer session was captured');
    if (phase === 'review') assert.ok(reviewerSession, 'review starts after bootstrap captured the reviewer session');

    const pausedByMcp = await f.client.tool('pause_task', { threadId: f.threadId, reason: `E2E pause at ${phase}` });
    assert.equal(pausedByMcp.status, 'PAUSED', JSON.stringify(pausedByMcp));
    pausedTask = await waitForTask(f.root, f.threadId, task => task.status === 'paused' && !task.execution?.run);
    if (heldValidation) assert.equal(probeProcessIdentity(heldValidation.pid, heldValidation.birthId), 'exited', 'pause must stop the held validation child');
    if (gateEvent?.launchId) {
      await waitForEvent(f.root, 'process.close', entry => entry.launchId === gateEvent.launchId);
    }

    const resumedRun = waitForTask(f.root, f.threadId, task => !!task.execution?.run && task.execution.run.id !== firstRunId, 60_000);
    const resumeResult = await f.client.tool('resume_task', { threadId: f.threadId });
    assert.ok(resumeResult, 'resume_task must respond through real MCP');
    const resumedTask = await resumedRun;
    assert.equal(resumedTask.execution?.version, 2);
    assert.notEqual(resumedTask.execution?.run?.id, firstRunId, 'resume must use a unique v2 run ID');
    assert.equal(resumedTask.execution?.phase, pausedTask.execution?.phase);
    assert.equal(resumedTask.execution?.round, pausedTask.execution?.round);
    const finalTask = await waitForTask(f.root, f.threadId, task => task.status === 'completed' && task.worktree?.integration?.phase === 'merged' && !task.execution?.run, 300_000);
    assert.equal(finalTask.threadId, f.threadId);
    assert.equal(finalTask.worktree?.path, sameWorktree);
    assert.equal(finalTask.worktree?.deliveryTarget?.ref, 'refs/heads/master');
    assert.equal(finalTask.execution?.version, 2);
    if (receiverSession) assert.equal(finalTask.receiverSessionId, receiverSession);
    if (reviewerSession) assert.equal(finalTask.reviewerSessionId, reviewerSession);
    const events = trace(f.root);
    if (phase === 'bootstrap') {
      if (pausedTask.reviewerSessionCaptured) {
        assert.ok(events.filter(entry => entry.role === 'bootstrap').every(entry => entry.sessionId === pausedTask.reviewerSessionId));
      } else {
        const tokens = events.filter(entry => entry.kind === 'bootstrap.prompt').map(entry => entry.bootstrapToken);
        assert.ok(tokens.length >= 2 && tokens.every(token => token === tokens[0]), 'uncaptured bootstrap retries must carry the original credential token');
      }
    }
    if (phase === 'develop') assert.ok(events.filter(entry => entry.kind === 'agent.start' && entry.role === 'worker').every(entry => entry.sessionId === receiverSession));
    if (phase === 'review') assert.ok(events.filter(entry => entry.kind === 'agent.start' && entry.role === 'reviewer').every(entry => entry.sessionId === reviewerSession));
    if (phase === 'merge') {
      assert.equal(events.filter(entry => entry.kind === 'agent.start').length, agentsBeforeMergeResume, 'merge resume must not launch another agent');
      assert.ok(reviewerSession && receiverSession && finalTask.reviewerSessionId === reviewerSession && finalTask.receiverSessionId === receiverSession);
      const lastReviewer = events.filter(entry => entry.kind === 'process.close' && entry.threadId === f.threadId &&
        events.some(start => start.kind === 'agent.start' && start.role === 'reviewer' && start.launchId === entry.launchId))
        .sort((a, b) => BigInt(a.at) < BigInt(b.at) ? -1 : 1).at(-1);
      assert.ok(lastReviewer, 'final reviewer process must close before merge starts');
      assert.ok(BigInt(lastReviewer.at) < BigInt(mergeGateAt), 'reviewer must exit before merge consumes its handoff');
    }
    assert.equal(git(f.repo, 'rev-parse', 'HEAD'), finalTask.worktree?.integration?.targetCommit);
    assert.equal(fs.readFileSync(path.join(f.repo, 'result.txt'), 'utf8'), `round ${rounds}\n`);
    assert.equal(git(f.repo, 'status', '--porcelain'), '');
    f.finish();
  });
}

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
