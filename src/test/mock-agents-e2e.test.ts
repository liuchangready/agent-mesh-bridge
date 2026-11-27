import { test, TestContext } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';
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

async function fixture(t: TestContext, scenario: MockScenario, delayResident = false) {
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
    ...(scenario.pauseAt?.merge ? [
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
  let resident: MockMcpClient | undefined;
  let clientClosed = false;
  const state: { threadId?: string; passed: boolean } = { passed: false };
  t.after(async () => {
    try {
      if (!state.passed && state.threadId) {
        try { await (clientClosed ? resident : client)?.tool('pause_task', { threadId: state.threadId, reason: 'Fixture teardown' }); } catch {}
      }
      if (!clientClosed) await client.close();
      await resident?.close();
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
  const brief = {
    workspaceRoot: repo, targetBranch: 'master', targetAgent: 'codex', initiatorAgent: 'codex',
    title: 'Scripted multi-round acceptance', prompt: 'Write result.txt for each requested revision', templateId: 'programming',
  };
  if (delayResident) {
    await assert.rejects(client.tool('dispatch_task', brief), /RESIDENT_EXECUTION_REQUIRED/);
    await assert.rejects(client.tool('invoke_agent', { targetAgent: 'codex', prompt: 'Must not start' }), /RESIDENT_EXECUTION_REQUIRED/);
    assert.deepEqual(fs.readdirSync(path.join(storage, 'threads')), []);
  }
  const residentGate = path.join(root, 'resident-scan');
  const residentScript = [
    "const fs = require('node:fs');",
    'const [pkg, storage, gate] = process.argv.slice(1);',
    "const { createBridgeHost } = require(pkg + '/bin/agent-bridge-mcp.cjs');",
    "const { WindowWorkspace } = require(pkg + '/out/bridge/workspace');",
    "const { ExecutionController } = require(pkg + '/out/bridge/execution-controller');",
    'const host = createBridgeHost();',
    "if (!host.startupAdmission.ready) throw Error(JSON.stringify(host.startupAdmission));",
    'host.executionController.dispose();',
    'const controller = new ExecutionController(host.taskStore, host.runner, host.callbackManager, host.reworkController.getMergeCoordinator(), host.registry, { standingScanIntervalMs: gate ? 0 : 500 });',
    'const window = new WindowWorkspace(storage, [process.cwd()]);',
    'window.advertiseExecutionOwner();',
    "process.on('exit', () => window.dispose());",
    'setInterval(() => window.publish(), 10000).unref();',
    'if (gate) setInterval(() => { if (fs.existsSync(gate)) void controller.scanAndDrain(); }, 100).unref();',
    'host.server.startStdio();',
  ].join('\n');
  resident = new MockMcpClient(env, repo, ['-e', residentScript, packageRoot, storage, delayResident ? residentGate : '']);
  await resident.initialize();
  const dispatch = await client.tool('dispatch_task', brief);
  state.threadId = dispatch.threadId;
  assert.equal(dispatch.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
  assert.equal(dispatch.status, 'QUEUED');
  return { root, repo, base, client, resident, residentPid: resident.child.pid, residentGate, threadId: state.threadId!,
    closeSubmitter: async () => { await client.close(); clientClosed = true; },
    finish: () => { state.passed = true; } };
}

test('resident Bridge takes over after submitter exits and delivers three rounds', { timeout: 420_000 }, async t => {
  const f = await fixture(t, {
    rounds: 3, reviewerTailMs: 200, bootstrapProcessChurn: process.platform === 'win32', pauseAt: { bootstrap: true },
  }, true);
  const queued = await waitForTask(f.root, f.threadId, task => task.workspace?.dispatchAccepted === true);
  assert.equal(queued.status, 'queued');
  assert.equal(queued.execution?.run, undefined);
  assert.equal(trace(f.root).filter(entry => entry.kind === 'agent.start').length, 0);
  await f.closeSubmitter();
  fs.writeFileSync(f.residentGate, '');
  const running = await waitForTask(f.root, f.threadId, task => task.execution?.run?.state === 'active');
  assert.equal(running.execution?.run?.owner.pid, f.residentPid);
  assert.equal(running.execution?.purpose, 'bootstrap');
  // Let the controlled waiting agent proceed through bootstrap and delivery.
  const gate = await waitForEvent(f.root, 'agent.pause-ready', entry => entry.role === 'bootstrap');
  fs.writeFileSync(path.join(f.root, 'bootstrap-' + gate.round + '.release'), '');
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
  if (process.platform === 'win32') {
    assert.ok(workers.every(entry => entry.jobId), 'every Windows developer run must register a Job');
    assert.equal(new Set(workers.map(entry => entry.jobId)).size, workers.length);
    const churn = events.find(entry => entry.kind === 'bootstrap.process-churn');
    assert.ok(churn?.backgroundPid);
    assert.equal(probeProcessIdentity(churn.backgroundPid), 'exited', 'background descendant outlived bootstrap Job');
  }
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
  for (const round of [1, 2, 3]) {
    const log = await f.resident.tool('get_reviewer_log', { threadId: f.threadId, round });
    assert.equal(log.available, true);
    assert.match(log.content, /"type":"thread.started"/);
  }
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
  const summary = await f.resident.tool('get_task_summary', { threadId: f.threadId });
  assert.equal(summary.status, 'completed');
  assert.ok(summary.finalSummary);
  f.finish();
});

test('Windows v2 receiver cannot forward over stdio after the CLI scrubs receiver markers', {
  skip: process.platform !== 'win32', timeout: 180_000,
}, async t => {
  const f = await fixture(t, { rounds: 1, scrubReceiverMcpEnv: true });
  const task = await waitForTask(f.root, f.threadId, task => task.status === 'completed' && !task.execution?.run, 120_000);
  const events = trace(f.root);
  assert.ok(events.some(entry => entry.kind === 'receiver.guards' && entry.threadId === f.threadId));
  assert.ok(events.some(entry => entry.kind === 'bootstrap.confirmed' && entry.threadId === f.threadId));
  assert.ok(events.some(entry => entry.kind === 'reviewer.verdict' && entry.tool === 'approve_task'));
  assert.equal(task.worktree?.integration?.phase, 'merged');
  const ledger = path.join(f.root, 'home', '.agent-mesh-bridge', 'threads');
  assert.deepEqual(fs.readdirSync(ledger).filter(name => name.endsWith('.json')), [`${f.threadId}.json`]);
  f.finish();
});

test('mock v2 pause/resume: same task, sessions and delivery through all four phases', { timeout: 360_000 }, async t => {
  const rounds = 2;
  const pauseAt = { bootstrap: true, develop: rounds, review: rounds, merge: true } as const;
  const f = await fixture(t, { rounds, pauseAt });
  const initialTask = await waitForTask(f.root, f.threadId, task => Boolean(task.worktree?.path));
  const worktree = initialTask.worktree?.path;
  assert.ok(worktree);
  let agentsBeforeMerge = 0;

  for (const phase of ['bootstrap', 'develop', 'review', 'merge'] as const) {
    const executionPhase = phase === 'bootstrap' ? 'review' : phase;
    const role = phase === 'develop' ? 'worker' : phase === 'review' ? 'reviewer' : phase;
    const pauseRound = pauseAt[phase];
    let heldValidation: { pid: number; birthId: string } | undefined;
    let gateEvent: any;

    if (phase === 'merge') {
      const mergeReady = path.join(f.root, 'merge.ready');
      await waitForFile(mergeReady);
      const gate = JSON.parse(fs.readFileSync(mergeReady, 'utf8'));
      const birthId = getProcessBirthId(gate.pid);
      assert.ok(birthId, 'the held validation child must have captured process identity');
      heldValidation = { pid: gate.pid, birthId };
      const events = trace(f.root);
      const agents = events.filter(entry => entry.kind === 'agent.start');
      agentsBeforeMerge = agents.length;
      const lastReviewer = agents.filter(entry => entry.role === 'reviewer').at(-1);
      assert.ok(lastReviewer);
      const closed = events.find(entry => entry.kind === 'process.close' && entry.launchId === lastReviewer.launchId);
      assert.ok(closed && BigInt(closed.at) < BigInt(gate.at), 'reviewer must exit before merge consumes its handoff');
    } else {
      gateEvent = await waitForEvent(f.root, 'agent.pause-ready', entry => entry.threadId === f.threadId && entry.role === role &&
        (pauseRound === true || entry.round === pauseRound));
    }

    const running = await waitForTask(f.root, f.threadId, task => task.execution?.run?.state === 'active' &&
      task.execution.phase === executionPhase && (gateEvent ? task.execution.run.id === gateEvent.runId : task.status === 'merging'));
    assert.equal(running.execution?.version, 2, `${phase} must use admitted production v2 execution`);
    assert.ok(running.execution.run?.id);
    const oldRunId = running.execution.run.id;
    const round = running.execution.round;
    if (typeof pauseRound === 'number') assert.equal(round, pauseRound, `${phase} must pause in the selected round`);
    assert.equal(running.threadId, f.threadId);
    assert.equal(running.worktree?.path, worktree);

    const pausedByMcp = await f.client.tool('pause_task', { threadId: f.threadId, reason: `E2E pause at ${phase}` });
    assert.ok(['PAUSED', 'STOPPING'].includes(pausedByMcp.status), JSON.stringify(pausedByMcp));
    const paused = await waitForTask(f.root, f.threadId, task => task.status === 'paused' && !task.execution?.run);
    assert.equal(paused.execution?.phase, executionPhase, `pause must preserve ${phase}`);
    assert.equal(paused.execution?.round, round, 'pause must preserve the round');
    if (heldValidation) assert.equal(probeProcessIdentity(heldValidation.pid, heldValidation.birthId), 'exited', 'pause must stop the held validation child');
    if (gateEvent) {
      assert.ok(gateEvent.launchId);
      await waitForEvent(f.root, 'process.close', entry => entry.launchId === gateEvent.launchId);
    }

    const [resumed, resumeResult] = await Promise.all([
      waitForTask(f.root, f.threadId, task => Boolean(task.execution?.run && task.execution.run.id !== oldRunId), 60_000),
      f.client.tool('resume_task', { threadId: f.threadId }),
    ]);
    assert.ok(resumeResult, `resume_task must respond through real MCP during ${phase}`);
    assert.equal(resumed.execution?.version, 2);
    assert.ok(resumed.execution.run?.id);
    assert.notEqual(resumed.execution.run.id, oldRunId, 'resume must use a unique v2 run ID');
    assert.equal(resumed.execution.phase, executionPhase);
    assert.equal(resumed.execution.round, round);
    assert.equal(resumed.execution.purpose, running.execution.purpose);
    assert.equal(resumed.threadId, f.threadId);
    assert.equal(resumed.worktree?.path, worktree);

    if (phase === 'merge') continue;
    const agent = await waitForEvent(f.root, 'agent.start', entry => entry.threadId === f.threadId &&
      entry.runId === resumed.execution?.run?.id && entry.role === role);
    if (phase === 'bootstrap') {
      if (paused.reviewerSessionCaptured) {
        assert.equal(agent.sessionId, paused.reviewerSessionId);
      } else {
        const prompt = await waitForEvent(f.root, 'bootstrap.prompt', entry => entry.launchId === agent.launchId);
        assert.ok(gateEvent.bootstrapToken);
        assert.equal(prompt.bootstrapToken, gateEvent.bootstrapToken, 'uncaptured bootstrap retries must carry the original credential token');
      }
    } else if (phase === 'develop') {
      assert.ok(paused.receiverSessionCaptured && paused.receiverSessionId, 'round 2 starts with the captured developer session');
      assert.equal(agent.sessionId, paused.receiverSessionId);
    } else {
      assert.ok(paused.reviewerSessionCaptured && paused.reviewerSessionId, 'review starts with the captured reviewer session');
      assert.equal(agent.sessionId, paused.reviewerSessionId);
      const verdict = await waitForEvent(f.root, 'reviewer.verdict', entry => entry.launchId === agent.launchId);
      assert.equal(verdict.tool, 'approve_task');
    }
  }

  const finalTask = await waitForTask(f.root, f.threadId, task => task.status === 'completed' && task.worktree?.integration?.phase === 'merged' && !task.execution?.run, 300_000);
  assert.equal(finalTask.threadId, f.threadId);
  assert.equal(finalTask.worktree?.path, worktree);
  assert.equal(finalTask.worktree?.deliveryTarget?.ref, 'refs/heads/master');
  assert.equal(finalTask.execution?.version, 2);
  assert.equal(finalTask.worktree?.validation?.result, 'passed');
  const agents = trace(f.root).filter(entry => entry.kind === 'agent.start');
  assert.equal(agents.length, agentsBeforeMerge, 'merge resume must not launch another agent');
  assert.ok(finalTask.receiverSessionId && finalTask.reviewerSessionId);
  assert.ok(agents.filter(entry => entry.role === 'worker').every(entry => entry.sessionId === finalTask.receiverSessionId));
  assert.ok(agents.filter(entry => entry.role === 'reviewer').every(entry => entry.sessionId === finalTask.reviewerSessionId));
  assert.equal(git(f.repo, 'rev-parse', 'HEAD'), finalTask.worktree?.integration?.targetCommit);
  assert.equal(fs.readFileSync(path.join(f.repo, 'result.txt'), 'utf8'), `round ${rounds}\n`);
  assert.equal(git(f.repo, 'status', '--porcelain'), '');
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


test('Windows Job emits an exit receipt when the controller process exits', { skip: process.platform !== 'win32', timeout: 20_000 }, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'amb-job-owner-exit-'));
  const platform = path.resolve(__dirname, '../bridge/platform.js');
  const jobs = path.resolve(__dirname, '../bridge/windows-job.js');
  const work = [
    "const child = require('node:child_process').spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });",
    "child.unref();",
    "require('node:fs').writeFileSync(process.argv[1], JSON.stringify([process.pid, child.pid]));",
    "setInterval(() => {}, 1000);",
  ].join('\n');
  const ownerScript = [
    "const fs = require('node:fs'), path = require('node:path');",
    "const [platform, jobs, root, work] = process.argv.slice(1);",
    "const { getProcessBirthId } = require(platform);",
    "const { spawnWindowsJob, startWindowsJob, windowsJobOf } = require(jobs);",
    "const pids = path.join(root, 'pids.json');",
    "const child = spawnWindowsJob(process.execPath, ['-e', work, pids], { stdio: 'ignore', windowsHide: true }, root, getProcessBirthId(process.pid));",
    "fs.writeFileSync(path.join(root, 'job.json'), JSON.stringify(windowsJobOf(child)));",
    "startWindowsJob(child);",
    "setInterval(() => { if (fs.existsSync(pids)) process.exit(0); }, 20);",
    "setTimeout(() => process.exit(1), 10000).unref();",
  ].join('\n');
  const owner = spawn(process.execPath, ['-e', ownerScript, platform, jobs, root, work], { stdio: 'ignore', windowsHide: true });
  const code = await new Promise<number | null>((resolve, reject) => {
    owner.once('error', reject);
    owner.once('close', (exitCode) => resolve(exitCode));
  });
  assert.equal(code, 0, root);
  const job = JSON.parse(fs.readFileSync(path.join(root, 'job.json'), 'utf8'));
  await waitForFile(job.prefix + '.done', 8_000);
  const pids: number[] = JSON.parse(fs.readFileSync(path.join(root, 'pids.json'), 'utf8'));
  assert.equal(pids.length, 2);
  assert.ok(pids.every(pid => probeProcessIdentity(pid) === 'exited'), root);
  const realRoot = fs.realpathSync.native(root);
  const temp = fs.realpathSync.native(os.tmpdir()) + path.sep;
  assert.ok(realRoot.startsWith(temp) && path.basename(realRoot).startsWith('amb-job-owner-exit-'));
  fs.rmSync(realRoot, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
});
