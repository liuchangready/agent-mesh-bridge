import { strict as assert } from 'assert';
import { test } from 'node:test';
import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { claimGroupAgentRuns, cleanupGroupAgentTaskArtifacts, groupAgentRunState, GroupAgentMailboxClient, hasGroupAgentCancel, publishGroupAgentFailure, publishGroupAgentPid, publishGroupAgentRun, publishGroupAgentResult } from '../bridge/group-agent-mailbox';
import { saveGroupAgentConfig, writeGroupAgentHost } from '../bridge/group-agent-registry';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread, GroupAgentBindingSnapshot } from '../bridge/types';
import { GroupAgentRuntime, GroupAgentRuntimeError } from '../copilot/group-agent-runtime';

test('independent runner process exchanges one pinned Group run through the durable mailbox', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-process-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-mailbox-one', generation: 7, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
  const request = {
    threadId: 'thread_mailbox_process', round: 3, role: 'developer' as const, agent: 'copilot_group:11111111-2222-4333-8444-555555555555' as const,
    sessionId: 'session-mailbox-0003', worktree: root, prompt: 'resume with latest request', modelRef,
    bindingSnapshot: { agentId: 'copilot_group:11111111-2222-4333-8444-555555555555' as const, revision: 9, source: 'vscode-lm' as const, modelRef },
    isRework: true, timeoutMs: 10000,
  };
  const script = `const m=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-mailbox.js'))}); const r=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-registry.js'))}); const h=r.loadGroupAgentHost(); const c=new m.GroupAgentMailboxClient(); c.execute(${JSON.stringify(request)},h,new AbortController().signal).then(v=>process.stdout.write(JSON.stringify(v))).catch(e=>{process.stderr.write(e.message);process.exitCode=1});`;
  writeGroupAgentHost(host, root);
  const child = childProcess.spawn(process.execPath, ['-e', script], { cwd: process.cwd(), env: { ...process.env, AGENT_MESH_BRIDGE_HOME: root }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
  let stdout = '';
  let stderr = '';
  child.stdout.setEncoding('utf8').on('data', (chunk) => stdout += chunk);
  child.stderr.setEncoding('utf8').on('data', (chunk) => stderr += chunk);
  try {
    let claimed: ReturnType<typeof claimGroupAgentRuns> = [];
    const deadline = Date.now() + 5000;
    while (!claimed.length && Date.now() < deadline) {
      claimed = claimGroupAgentRuns(host, root);
      if (child.exitCode !== null) break;
      if (!claimed.length) await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(claimed.length, 1, `host should atomically claim the runner request once (${stderr})`);
    assert.deepEqual(claimGroupAgentRuns(host, root), [], 'duplicate scan must not execute the run twice');
    assert.equal(claimed[0].threadId, request.threadId);
    assert.equal(claimed[0].round, request.round);
    assert.deepEqual(claimed[0].modelRef, modelRef);
    fs.writeFileSync(path.join(root, 'group-agent-mailbox', 'requests', `${claimed[0].runId}.json`), JSON.stringify(claimed[0]));
    assert.deepEqual(claimGroupAgentRuns(host, root), [], 'a redelivered claimed run must not be accepted twice');
    assert.equal(groupAgentRunState(claimed[0].runId, root), 'running');
    publishGroupAgentResult(claimed[0].runId, host.generation, { text: 'same task resumed', toolCallsCompleted: 2, modelRequests: 3 });
    assert.equal(publishGroupAgentRun({ ...request, runId: claimed[0].runId }, host, root), claimed[0].runId);
    assert.deepEqual(claimGroupAgentRuns(host, root), [], 'terminal runs must not be replayed after a broker restart');
    assert.equal(groupAgentRunState(claimed[0].runId, root), 'terminal');
    const restart = childProcess.spawnSync(process.execPath, ['-e', `const m=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-mailbox.js'))}); const r=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-registry.js'))}); const h=r.loadGroupAgentHost(); process.stdout.write(JSON.stringify({claimed:m.claimGroupAgentRuns(h),state:m.groupAgentRunState(${JSON.stringify(claimed[0].runId)})}));`],
      { cwd: process.cwd(), env: { ...process.env, AGENT_MESH_BRIDGE_HOME: root }, encoding: 'utf8', windowsHide: true });
    assert.equal(restart.status, 0, restart.stderr);
    assert.deepEqual(JSON.parse(restart.stdout), { claimed: [], state: 'terminal' });
    const exitCode = await new Promise<number | null>((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', resolve);
    });
    assert.equal(exitCode, 0, stderr);
    assert.deepEqual(JSON.parse(stdout), { text: 'same task resumed', toolCallsCompleted: 2, modelRequests: 3 });
  } finally {
    if (child.exitCode === null) child.kill();
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('two broker processes racing for one request produce one durable run claim', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-claim-race-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-claim-race', generation: 5, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const agent = 'copilot_group:11111111-2222-4333-8444-55555555555e' as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-race', id: 'model-race' };
  writeGroupAgentHost(host, root);
  try {
    const runId = publishGroupAgentRun({ threadId: 'thread_claim_race', round: 1, role: 'developer', agent,
      sessionId: 'session-claim-race', worktree: root, prompt: 'race', modelRef,
      bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm', modelRef }, isRework: false, timeoutMs: 5000 }, host, root);
    const script = `const m=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-mailbox.js'))}); const r=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-registry.js'))}); process.stdout.write(JSON.stringify(m.claimGroupAgentRuns(r.loadGroupAgentHost()).map(x=>x.runId)));`;
    const launch = () => childProcess.spawn(process.execPath, ['-e', script], { cwd: process.cwd(), env: { ...process.env, AGENT_MESH_BRIDGE_HOME: root }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    const children = [launch(), launch()];
    const results = await Promise.all(children.map((child) => new Promise<string>((resolve, reject) => {
      let stdout = ''; let stderr = '';
      child.stdout.setEncoding('utf8').on('data', (chunk) => stdout += chunk);
      child.stderr.setEncoding('utf8').on('data', (chunk) => stderr += chunk);
      child.once('error', reject);
      child.once('exit', (code) => code === 0 ? resolve(stdout) : reject(new Error(stderr)));
    })));
    assert.deepEqual(results.map((result) => JSON.parse(result)).flat(), [runId]);
    assert.equal(groupAgentRunState(runId, root), 'running');
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('concurrent publishers and result writers cannot replace an existing run or final response', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-publish-race-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-publish-race', generation: 6, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const agent = 'copilot_group:11111111-2222-4333-8444-55555555555f' as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-publish', id: 'model-publish' };
  const request = { runId: 'bbbbbbbb-cccc-4ddd-8eee-ffffffffffff', threadId: 'thread_publish_race', round: 1,
    role: 'developer' as const, agent, sessionId: 'session-publish-race', worktree: root, prompt: 'race', modelRef,
    bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm' as const, modelRef }, isRework: false, timeoutMs: 5000 };
  writeGroupAgentHost(host, root);
  try {
    const mailboxPath = JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-mailbox.js'));
    const runScript = `const m=require(${mailboxPath});const r=require(${JSON.stringify(path.resolve(__dirname, '../bridge/group-agent-registry.js'))});m.publishGroupAgentRun(${JSON.stringify(request)},r.loadGroupAgentHost());`;
    const launch = (script: string) => childProcess.spawn(process.execPath, ['-e', script], { cwd: process.cwd(), env: { ...process.env, AGENT_MESH_BRIDGE_HOME: root }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    const finish = (child: childProcess.ChildProcess) => new Promise<void>((resolve, reject) => {
      let stderr = ''; child.stderr?.setEncoding('utf8').on('data', (chunk) => stderr += chunk);
      child.once('error', reject); child.once('exit', (code) => code === 0 ? resolve() : reject(new Error(stderr)));
    });
    const publishers = [launch(runScript), launch(runScript)];
    await Promise.all(publishers.map(finish));
    const claimed = claimGroupAgentRuns(host, root);
    assert.equal(claimed.length, 1);
    const resultScript = `const m=require(${mailboxPath});m.publishGroupAgentResult(${JSON.stringify(request.runId)},${host.generation},{text:'one final',toolCallsCompleted:1,modelRequests:1});`;
    await Promise.all([finish(launch(resultScript)), finish(launch(resultScript))]);
    const response = JSON.parse(fs.readFileSync(path.join(root, 'group-agent-mailbox', 'responses', `${request.runId}.json`), 'utf8'));
    assert.equal(response.result.text, 'one final');
    assert.equal(groupAgentRunState(request.runId, root), 'terminal');
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('mailbox client rejects a structurally inconsistent native usage result', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-invalid-result-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-invalid-result', generation: 8, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const agent = 'copilot_group:11111111-2222-4333-8444-55555555555a' as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
  const request = { threadId: 'thread_mailbox_bad_usage', round: 1, role: 'developer' as const, agent,
    sessionId: 'session-bad-usage', worktree: root, prompt: 'run', modelRef,
    bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm' as const, modelRef }, isRework: false, timeoutMs: 5000 };
  writeGroupAgentHost(host, root);
  try {
    const execution = new GroupAgentMailboxClient(root).execute(request, host, new AbortController().signal);
    const deadline = Date.now() + 2000;
    let claimed: ReturnType<typeof claimGroupAgentRuns> = [];
    while (!claimed.length && Date.now() < deadline) {
      claimed = claimGroupAgentRuns(host, root);
      if (!claimed.length) await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(claimed.length, 1);
    fs.writeFileSync(path.join(root, 'group-agent-mailbox', 'responses', `${claimed[0].runId}.json`), JSON.stringify({ version: 1,
      runId: claimed[0].runId, hostGeneration: host.generation, sequence: 1, completedAt: Date.now(), result: { text: 'done', toolCallsCompleted: 0,
        tokenUsage: { uncachedInputTokens: 5, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 1, outputTokens: 2, totalTokens: 3, agent, available: true, partial: false, source: 'native_reported' } } }));
    await assert.rejects(execution, /no valid result/);
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('an aborted unclaimed mailbox run is atomically cancelled and settles without a broker', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-no-broker-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-no-broker', generation: 3, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const agent = 'copilot_group:11111111-2222-4333-8444-55555555555b' as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
  const request = { threadId: 'thread_mailbox_no_broker', round: 1, role: 'developer' as const, agent,
    sessionId: 'session-no-broker', worktree: root, prompt: 'run', modelRef,
    bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm' as const, modelRef }, isRework: false, timeoutMs: 5000 };
  writeGroupAgentHost(host, root);
  const aborter = new AbortController();
  try {
    const execution = new GroupAgentMailboxClient(root).execute(request, host, aborter.signal);
    aborter.abort(new Error('Task paused.'));
    await assert.rejects(execution, /Task paused/);
    assert.equal(groupAgentRunState(fs.readdirSync(path.join(root, 'group-agent-mailbox', 'states'))[0], root), 'terminal');
    assert.deepEqual(fs.readdirSync(path.join(root, 'group-agent-mailbox', 'requests')), []);
    assert.equal(fs.readdirSync(path.join(root, 'group-agent-mailbox', 'cancelled')).length, 1);
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('claimed mailbox cancellation waits for child shutdown and forwards failure usage across host generations', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-running-cancel-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const host = { version: 1 as const, hostInstanceId: 'host-running-old', generation: 12, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const agent = 'copilot_group:11111111-2222-4333-8444-55555555555c' as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
  const request = { runId: 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee', threadId: 'thread_mailbox_running_cancel', round: 1,
    role: 'developer' as const, agent, sessionId: 'session-running-cancel', worktree: root, prompt: 'run', modelRef,
    bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm' as const, modelRef }, isRework: false, timeoutMs: 5000 };
  writeGroupAgentHost(host, root);
  const aborter = new AbortController();
  let child: childProcess.ChildProcess | undefined;
  try {
    const execution = new GroupAgentMailboxClient(root).execute(request, host, aborter.signal);
    const deadline = Date.now() + 2000;
    let claimed: ReturnType<typeof claimGroupAgentRuns> = [];
    while (!claimed.length && Date.now() < deadline) {
      claimed = claimGroupAgentRuns(host, root);
      if (!claimed.length) await new Promise((resolve) => setTimeout(resolve, 10));
    }
    assert.equal(claimed.length, 1);
    child = childProcess.spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore', windowsHide: true });
    await new Promise<void>((resolve, reject) => { child!.once('spawn', resolve); child!.once('error', reject); });
    publishGroupAgentPid(request.runId, 1, child.pid, root);
    writeGroupAgentHost({ ...host, hostInstanceId: 'host-running-new', generation: 13, updatedAt: Date.now() }, root);
    await new Promise((resolve) => setTimeout(resolve, 150));
    assert.equal(hasGroupAgentCancel(request.runId, root), true);
    const settled = execution.then(() => true, () => true);
    assert.equal(await Promise.race([settled, new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 50))]), false,
      'generation change must not settle while the old child is still running');
    child.kill();
    await new Promise<void>((resolve) => child!.once('exit', () => resolve()));
    publishGroupAgentFailure(request.runId, host.generation, {
      message: 'host executor stopped after cancellation', modelRequests: 2,
      tokenUsage: { uncachedInputTokens: 4, cacheReadTokens: 1, cacheWriteTokens: 0, promptTokens: 5, outputTokens: 2, totalTokens: 7,
        agent, model: modelRef.id, source: 'native_reported', available: true, partial: true, attempts: 2, collectedAt: new Date().toISOString() },
      interruption: { kind: 'paused', reason: 'Host generation changed.', at: new Date().toISOString(), by: 'test-host' },
      sessionId: request.sessionId,
    }, root);
    await assert.rejects(execution, (error: any) => error.modelRequests === 2 && error.tokenUsage?.totalTokens === 7 && error.interruption?.kind === 'paused');
    assert.equal(groupAgentRunState(request.runId, root), 'terminal');
  } finally {
    if (child && child.exitCode === null) child.kill();
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('task artifact cleanup removes only Bridge-owned Group sessions and terminal mailbox records', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-cleanup-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const uuid = '11111111-2222-4333-8444-55555555555d';
  const agent = `copilot_group:${uuid}` as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-cleanup', id: 'model-cleanup' };
  const host = { version: 1 as const, hostInstanceId: 'host-cleanup', generation: 2, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const thread = { threadId: 'thread_group_cleanup', title: 'cleanup', initiatorAgent: agent, targetAgent: agent,
    reviewFlow: 'dedicated' as const, status: 'completed' as const, currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    requesterSessionId: 'native-requester-session', receiverSessionId: 'group-developer-session', callerSessionId: 'group-reviewer-session',
    reviewerSessionId: 'group-reviewer-session', workspacePath: root, rounds: [], summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 } } as unknown as DelegationThread;
  const bridgeSessions = path.join(root, 'group-agent-sessions');
  fs.mkdirSync(bridgeSessions);
  for (const id of ['group-developer-session', 'group-reviewer-session', 'native-requester-session'])
    fs.writeFileSync(path.join(bridgeSessions, `${id}.jsonl`), 'owned history');
  for (const [id, ownerAgentId] of [['group-developer-session', agent], ['group-reviewer-session', agent]] as const)
    fs.writeFileSync(path.join(bridgeSessions, `${id}.jsonl.owner.json`), JSON.stringify({ version: 1, transport: 'bridge-agent', ownerAgentId, threadId: thread.threadId, workspace: root }));
  writeGroupAgentHost(host, root);
  const request = { threadId: thread.threadId, round: 1, role: 'developer' as const, agent,
    sessionId: thread.receiverSessionId, worktree: root, prompt: 'work', modelRef,
    bindingSnapshot: { agentId: agent, revision: 1, source: 'vscode-lm' as const, modelRef }, isRework: false, timeoutMs: 5000 };
  try {
    const runId = publishGroupAgentRun(request, host, root);
    assert.equal(claimGroupAgentRuns(host, root).length, 1);
    publishGroupAgentResult(runId, host.generation, { text: 'finished', toolCallsCompleted: 0, modelRequests: 1 });
    cleanupGroupAgentTaskArtifacts(thread, root);
    assert.equal(fs.existsSync(path.join(bridgeSessions, 'group-developer-session.jsonl')), false);
    assert.equal(fs.existsSync(path.join(bridgeSessions, 'group-reviewer-session.jsonl')), false);
    assert.equal(fs.existsSync(path.join(bridgeSessions, 'native-requester-session.jsonl')), true);
    assert.equal(groupAgentRunState(runId, root), undefined);
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('an independent AutonomousRunner without a VS Code executor uses mailbox and the pinned task snapshot', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-runner-mailbox-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const groupUuid = '11111111-2222-4333-8444-555555555556';
  const agent = `copilot_group:${groupUuid}` as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-mailbox', id: 'model-pinned' };
  const bindingSnapshot: GroupAgentBindingSnapshot = { agentId: agent, revision: 4, source: 'vscode-lm', modelRef };
  const host = { version: 1 as const, hostInstanceId: 'host-runner-mailbox', generation: 11, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const store = new TaskStore(root);
  const threadId = 'thread_runner_mailbox';
  const thread: DelegationThread = {
    threadId, title: 'Mailbox task', initiatorAgent: 'codex', targetAgent: agent,
    status: 'running', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    receiverSessionId: 'session-runner-mailbox', workspacePath: root, targetModelRef: modelRef,
    targetGroupBindingSnapshot: bindingSnapshot,
    worktree: { path: root, mainPath: root, baseCommit: 'base' },
    summary: { lastPromptSnippet: 'work', lastOutputSnippet: 'running', totalDurationMs: 0 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', durationMs: 0, status: 'running', timestamp: new Date().toISOString() }],
  };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Mailbox group', source: 'vscode-lm',
    enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false, revision: 4, receiverModel: modelRef }] }, root);
  writeGroupAgentHost(host, root);
  store.upsertTask(thread);
  const lease = new TaskRoleLock(root).tryAcquire(threadId, 'developer');
  assert.ok(lease);
  const runner = new AutonomousRunner({} as any, 5000, new CancellationRegistry(root), store);
  try {
    const execution = runner.execute({ agent, prompt: 'apply the pinned task prompt', isRework: false,
      receiverSessionId: thread.receiverSessionId, cwd: root, taskThreadId: threadId, taskRound: 1,
      modelRef, role: 'developer', timeoutMs: 3000 });
    let claimed: ReturnType<typeof claimGroupAgentRuns> = [];
    const deadline = Date.now() + 2000;
    while (!claimed.length && Date.now() < deadline) {
      claimed = claimGroupAgentRuns(host, root);
      if (!claimed.length) await new Promise((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].bindingSnapshot.revision, 4);
    assert.equal(claimed[0].threadId, threadId);
    assert.equal(claimed[0].round, 1);
    publishGroupAgentResult(claimed[0].runId, host.generation, { text: 'mailbox result', toolCallsCompleted: 1, modelRequests: 2 });
    const result = await execution;
    assert.equal(result.assessment?.success, true);
    assert.equal(result.transport, 'bridge-agent');
    assert.equal(result.actualSessionId, thread.receiverSessionId);
    assert.equal(result.extractedOutput, 'mailbox result');
  } finally {
    new TaskRoleLock(root).release(lease);
    store.dispose();
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('host broker validates the real task lease and runs structured tools in its pinned Git worktree', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-host-broker-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  childProcess.execFileSync('git', ['init', '-b', 'main'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.name', 'Bridge Test'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.email', 'bridge@example.invalid'], { cwd: root, stdio: 'ignore' });
  fs.writeFileSync(path.join(root, 'README.md'), 'base\n');
  childProcess.execFileSync('git', ['add', 'README.md'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['commit', '-m', 'base'], { cwd: root, stdio: 'ignore' });
  const groupUuid = '11111111-2222-4333-8444-555555555557';
  const agent = `copilot_group:${groupUuid}` as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-host', id: 'model-host' };
  const bindingSnapshot: GroupAgentBindingSnapshot = { agentId: agent, revision: 2, source: 'vscode-lm', modelRef };
  const host = { version: 1 as const, hostInstanceId: 'host-broker-one', generation: 12, pid: process.pid,
    workspaceUri: `file://${root}`, repositoryCommonDir: path.join(root, '.git'), trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  const store = new TaskStore(root);
  const threadId = 'thread_host_broker';
  const thread: DelegationThread = {
    threadId, title: 'Broker task', initiatorAgent: 'codex', targetAgent: agent,
    status: 'running', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    receiverSessionId: 'session-host-broker', workspacePath: root, targetModelRef: modelRef,
    targetGroupBindingSnapshot: bindingSnapshot,
    worktree: { path: root, mainPath: root, baseCommit: childProcess.execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim() },
    summary: { lastPromptSnippet: 'work', lastOutputSnippet: 'running', totalDurationMs: 0 },
    rounds: [{ round: 1, type: 'initial', prompt: 'work', durationMs: 0, status: 'running', timestamp: new Date().toISOString() }],
  };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Host group', source: 'vscode-lm',
    enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false, revision: 2, receiverModel: modelRef,
    models: [modelRef], modelProbes: [{ role: 'receiver', modelRef, revision: 2, hostGeneration: host.generation }] }] }, root);
  writeGroupAgentHost(host, root);
  store.upsertTask(thread);
  const lease = new TaskRoleLock(root).tryAcquire(threadId, 'developer');
  assert.ok(lease);

  const Module = require('module');
  const originalLoad = Module._load;
  const callbacks: Array<() => void> = [];
  class CancellationTokenSource {
    private cancelled = false;
    token = { get isCancellationRequested() { return (this as any).source.cancelled; }, source: this,
      onCancellationRequested: (callback: () => void) => { callbacks.push(callback); return { dispose() {} }; } };
    cancel() { this.cancelled = true; for (const callback of callbacks.splice(0)) callback(); }
    dispose() {}
  }
  Module._load = function (request: string, parent: unknown, isMain: boolean) {
    return request === 'vscode' ? { CancellationTokenSource } : originalLoad.call(this, request, parent, isMain);
  };
  let broker: { start(intervalMs?: number): void; dispose(): void } | undefined;
  try {
    const { GroupAgentHostBroker } = require('../copilot/group-agent-host-broker');
    let failAfterUsage = false;
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async (_messages, tools) => {
      if (failAfterUsage) return { text: '', finishReason: 'tool_calls', toolCalls: [
        { id: 'fail-after-usage', name: 'read_file', arguments: { path: 'missing-after-usage.txt' } },
      ], usage: { uncachedInputTokens: 4, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 4, outputTokens: 1, totalTokens: 5 } };
      if (tools.length && !fs.existsSync(path.join(root, 'from-agent.txt'))) return {
        text: '', finishReason: 'tool_calls', toolCalls: [
          { id: 'write-file', name: 'write_file', arguments: { path: 'from-agent.txt', content: 'written by bridge runtime\n' } },
          { id: 'run-test', name: 'run_command', arguments: { command: 'node -e "require(\'fs\').writeFileSync(\'command-test.txt\', \'passed\')"' } },
        ],
        usage: { uncachedInputTokens: 5, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 5, outputTokens: 2, totalTokens: 7 },
      };
      return { text: 'work completed', finishReason: 'stop', toolCalls: [],
        usage: { uncachedInputTokens: 3, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 3, outputTokens: 2, totalTokens: 5 } };
    } });
    const models = {
      getAgentBinding: () => ({ id: agent, source: 'vscode-lm', enabled: true, toolsAuthorized: true, revision: 2, receiverReady: true, initiatorReady: false, receiverModel: modelRef }),
      async runGroupAgent(_group: string, _model: unknown, sessionId: string, cwd: string, prompt: string, token: any, onPid: (pid?: number) => void, role: 'developer' | 'reviewer' | 'bootstrap') {
        const controller = new AbortController();
        const sub = token.onCancellationRequested(() => controller.abort());
        try {
          const result = await runtime.run(sessionId, cwd, prompt, controller.signal, role);
          const usage = result.usage;
          return { ...result, ...(usage ? { tokenUsage: { ...usage, agent, model: modelRef.id, source: 'native_reported' as const, available: true, partial: false, attempts: result.modelRequests, collectedAt: new Date().toISOString() } } : {}) };
        }
        catch (error) {
          if (error instanceof GroupAgentRuntimeError) throw Object.assign(new Error(error.message), {
            modelRequests: error.modelRequests,
            tokenUsage: { ...(error.usage || { uncachedInputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 0, outputTokens: 0, totalTokens: 0 }),
              agent, model: modelRef.id, source: error.usage ? 'native_reported' as const : 'unavailable' as const,
              available: Boolean(error.usage), partial: true, attempts: error.modelRequests, collectedAt: new Date().toISOString() },
          });
          throw error;
        }
        finally { sub.dispose(); }
      },
    };
    broker = new GroupAgentHostBroker({ hostInstanceId: host.hostInstanceId, taskStore: store, models: models as any, storageDir: root });
    broker!.start(20);
    const runner = new AutonomousRunner({} as any, 5000, new CancellationRegistry(root), store);
    const result = await runner.execute({ agent, prompt: 'implement test change', isRework: false, receiverSessionId: thread.receiverSessionId,
      cwd: root, taskThreadId: threadId, taskRound: 1, modelRef, role: 'developer', timeoutMs: 3000 });
    assert.equal(result.assessment?.success, true);
    assert.equal(result.tokenUsage?.promptTokens, 8);
    if (result.assessment?.transport !== 'bridge-agent') throw new Error('Expected a Bridge Agent assessment.');
    const successfulRunId = result.assessment.runId;
    assert.ok(successfulRunId);
    assert.equal(groupAgentRunState(successfulRunId!, root), 'terminal');
    assert.equal(fs.readFileSync(path.join(root, 'from-agent.txt'), 'utf8'), 'written by bridge runtime\n');
    assert.equal(fs.readFileSync(path.join(root, 'command-test.txt'), 'utf8'), 'passed');
    assert.equal(childProcess.execFileSync('git', ['status', '--short'], { cwd: root, encoding: 'utf8' }).includes('from-agent.txt'), true);

    failAfterUsage = true;
    const failed = await runner.execute({ agent, prompt: 'resume and diagnose the missing file', isRework: true, receiverSessionId: thread.receiverSessionId,
      cwd: root, taskThreadId: threadId, taskRound: 1, modelRef, role: 'developer', timeoutMs: 3000, previousTokenUsage: result.tokenUsage });
    assert.equal(failed.assessment?.success, false);
    if (failed.assessment?.transport !== 'bridge-agent') throw new Error('Expected a Bridge Agent failure assessment.');
    assert.equal(failed.tokenUsage?.totalTokens, 17, 'the resumed round keeps prior usage and adds the failed mailbox attempt');
    const failedRequest = JSON.parse(fs.readFileSync(path.join(root, 'group-agent-mailbox', 'claimed', `${failed.assessment.runId}.json`), 'utf8'));
    assert.equal(failedRequest.runId, failed.assessment.runId, 'round assessment uses the mailbox request runId');
    const failureRecord = JSON.parse(fs.readFileSync(path.join(root, 'group-agent-mailbox', 'responses', `${failed.assessment.runId}.json`), 'utf8'));
    assert.equal(failureRecord.failure.tokenUsage.totalTokens, 5);
    assert.equal(failureRecord.failure.sessionId, thread.receiverSessionId);
  } finally {
    broker?.dispose();
    Module._load = originalLoad;
    new TaskRoleLock(root).release(lease);
    store.dispose();
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('independent default dispatch completes Group bootstrap, tool work, same-session rework, approval, and merge', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-vertical-'));
  const storage = path.join(root, '.agent-mesh-bridge');
  const repo = path.join(root, 'repo');
  fs.mkdirSync(repo);
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  const oldUserProfile = process.env.USERPROFILE;
  const oldHomeEnv = process.env.HOME;
  const oldPath = process.env.PATH;
  const oldCallerAgent = process.env.MESH_CALLER_AGENT;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  process.env.USERPROFILE = root;
  process.env.HOME = root;
  childProcess.execFileSync('git', ['init', '-b', 'main'], { cwd: repo, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.name', 'Bridge Test'], { cwd: repo, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.email', 'bridge@example.invalid'], { cwd: repo, stdio: 'ignore' });
  fs.writeFileSync(path.join(repo, 'package.json'), JSON.stringify({ scripts: {
    compile: 'node -e "process.exit(0)"', test: 'node -e "process.exit(0)"',
  } }));
  fs.writeFileSync(path.join(repo, 'README.md'), 'base\n');
  childProcess.execFileSync('git', ['add', '.'], { cwd: repo, stdio: 'ignore' });
  childProcess.execFileSync('git', ['commit', '-m', 'base'], { cwd: repo, stdio: 'ignore' });

  const groupUuid = '11111111-2222-4333-8444-555555555558';
  const agent = `copilot_group:${groupUuid}` as const;
  const developerGroupUuid = '11111111-2222-4333-8444-555555555560';
  const developerAgent = `copilot_group:${developerGroupUuid}` as const;
  const receiverModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'developer-model' };
  const approvedReceiverModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'approved-developer-model' };
  const secondGroupModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'second-group-developer' };
  const initiatorModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'reviewer-model' };
  const host = { version: 1 as const, hostInstanceId: 'host-vertical', generation: 22, pid: process.pid,
    workspaceUri: `file://${repo}`, repositoryCommonDir: path.join(repo, '.git'), trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), defaultAgent: agent, defaultReviewer: agent, agents: [{
    id: agent, groupId: groupUuid, name: 'Vertical Group', source: 'vscode-lm', enabled: true, toolsAuthorized: true,
    receiverReady: true, initiatorReady: true, revision: 3, receiverModel, initiatorModel,
    models: [receiverModel, approvedReceiverModel, initiatorModel],
    modelProbes: [{ role: 'receiver', modelRef: receiverModel, revision: 3, hostGeneration: host.generation }, { role: 'receiver', modelRef: approvedReceiverModel, revision: 3, hostGeneration: host.generation }, { role: 'initiator', modelRef: initiatorModel, revision: 3, hostGeneration: host.generation }],
  }, {
    id: developerAgent, groupId: developerGroupUuid, name: 'Second Developer Group', source: 'vscode-lm', enabled: true, toolsAuthorized: true,
    receiverReady: true, initiatorReady: false, revision: 3, receiverModel: secondGroupModel, models: [secondGroupModel],
    modelProbes: [{ role: 'receiver', modelRef: secondGroupModel, revision: 3, hostGeneration: host.generation }],
  }] }, root);
  require('../bridge/bridge-policy').saveBridgePolicy({ reviewFlow: 'dedicated' }, path.join(root, '.agent-mesh-bridge'));
  writeGroupAgentHost(host, root);

  const store = new TaskStore(storage);
  const detector = new (require('../bridge/agent-detector').AgentDetector)();
  const fakeCliDir = path.join(root, 'fake-codex');
  fs.mkdirSync(fakeCliDir);
  const fakeClaude = process.platform === 'win32' ? path.join(fakeCliDir, 'claude.cmd') : path.join(fakeCliDir, 'claude');
  const fakeClaudeJs = path.join(fakeCliDir, 'fake-claude.js');
  const fakeClaudeReviewer = process.platform === 'win32' ? path.join(fakeCliDir, 'claude-reviewer.cmd') : path.join(fakeCliDir, 'claude-reviewer');
  const fakeClaudeReviewerJs = path.join(fakeCliDir, 'fake-claude-reviewer.js');
  fs.writeFileSync(fakeClaudeJs, `const fs=require('fs');const args=process.argv.slice(2);const resume=args.indexOf('--resume');const session=resume>=0?args[resume+1]:'fake-claude-session-001';const first=!fs.existsSync('result.md');fs.writeFileSync('result.md',first?'first pass\\n':'reworked\\n');fs.writeFileSync('command-check.md','passed');process.stdout.write(JSON.stringify({type:'system',subtype:'init',session_id:session})+'\\n');process.stdout.write(JSON.stringify({type:'assistant',message:{content:[{type:'text',text:'CLI implemented and tested the requested file.'}]}})+'\\n');process.stdout.write(JSON.stringify({type:'result',result:'CLI completed the task.',usage:{input_tokens:3,output_tokens:2}})+'\\n');`);
  if (process.platform === 'win32') fs.writeFileSync(fakeClaude, `@echo off\r\nnode "%~dp0fake-claude.js" %*\r\n`);
  else { fs.writeFileSync(fakeClaude, `#!/usr/bin/env node\nrequire(${JSON.stringify(fakeClaudeJs)});\n`); fs.chmodSync(fakeClaude, 0o755); }
  const projectRoot = path.resolve(__dirname, '..');
  const claudeReviewerSource = `const fs=require('fs'),path=require('path');const args=process.argv.slice(2);const resume=args.indexOf('--resume');const session=resume>=0?args[resume+1]:args[args.indexOf('--session-id')+1];const bootstrap=process.env.MESH_REVIEW_BOOTSTRAP==='1';let prompt='';(async()=>{const bridge=${JSON.stringify(projectRoot)};const home=process.env.AGENT_MESH_BRIDGE_HOME;const {TaskStore}=require(path.join(bridge,'bridge/task-store.js'));const {AgentDetector}=require(path.join(bridge,'bridge/agent-detector.js'));const {ActiveAgentResolver}=require(path.join(bridge,'bridge/active-agent-resolver.js'));const {AutonomousRunner}=require(path.join(bridge,'bridge/autonomous-runner.js'));const {CallbackManager}=require(path.join(bridge,'bridge/callback-manager.js'));const {AsyncDispatcher}=require(path.join(bridge,'bridge/async-dispatcher.js'));const {MCPServer}=require(path.join(bridge,'bridge/mcp-server.js'));const {ReworkController}=require(path.join(bridge,'bridge/rework-controller.js'));const {TaskExecutionRegistry}=require(path.join(bridge,'bridge/task-execution-registry.js'));const d=new AgentDetector(),s=new TaskStore(path.join(home,'.agent-mesh-bridge')),run=new AutonomousRunner(d,60000,undefined,s),cb=new CallbackManager(d);cb.setTaskStore(s);cb.setAutonomousRunner(run);const resolver=new ActiveAgentResolver(d),registry=new TaskExecutionRegistry(path.join(home,'.agent-mesh-bridge')),rw=new ReworkController(s,run,cb,d,registry),dispatcher=new AsyncDispatcher(s,resolver,run,cb,d,registry),mcp=new MCPServer(dispatcher,rw,s,d,resolver,run);try{if(bootstrap){const token=process.env.MESH_REVIEW_BOOTSTRAP_TOKEN;const thread=process.env.MESH_REVIEW_THREAD_ID;if(!token||!thread)throw Error('Bootstrap credential missing from environment');await mcp.handleToolCall('dispatch_task',{reviewBootstrap:{threadId:thread,token}});fs.writeFileSync(path.join(home,'fake-cli-session-id'),session);}else{const threadId=process.env.MESH_TASK_THREAD_ID||process.env.MESH_REVIEW_THREAD_ID;const task=s.getTask(threadId);if(!task?.worktree?.path)throw Error('Task worktree missing');const content=fs.readFileSync(path.join(task.worktree.path,'result.txt'),'utf8');const counter=path.join(home,'fake-cli-review-count');let count=0;try{count=Number(fs.readFileSync(counter,'utf8'))}catch{}count++;fs.writeFileSync(counter,String(count));if(content.includes('reworked'))await mcp.handleToolCall('approve_task',{threadId,remarks:'CLI reviewer approved',callerSessionId:session});else await mcp.handleToolCall('request_rework',{threadId,feedback:'Update result.txt to say reworked.',callerSessionId:session});}fs.appendFileSync(path.join(home,'fake-cli-review-sessions'),session+String.fromCharCode(10));process.stdout.write(JSON.stringify({type:'system',subtype:'init',session_id:session})+'\\n');process.stdout.write(JSON.stringify({type:'assistant',message:{content:[{type:'text',text:'CLI reviewer processed the task.'}]}})+'\\n');process.stdout.write(JSON.stringify({type:'result',result:'CLI reviewer processed the task.',usage:{input_tokens:2,output_tokens:1}})+'\\n');process.exit(0);}catch(e){process.stderr.write(e.stack||String(e));process.exitCode=1}finally{rw.dispose();cb.dispose?.();s.dispose()}})();`;
  fs.writeFileSync(fakeClaudeReviewerJs, claudeReviewerSource);
  if (process.platform === 'win32') fs.writeFileSync(fakeClaudeReviewer, `@echo off\r\nnode "%~dp0fake-claude-reviewer.js" %*\r\n`);
  else { fs.writeFileSync(fakeClaudeReviewer, `#!/usr/bin/env node\nrequire(${JSON.stringify(fakeClaudeReviewerJs)});\n`); fs.chmodSync(fakeClaudeReviewer, 0o755); }
  process.env.PATH = `${fakeCliDir}${path.delimiter}${oldPath || ''}`;
  process.env.MESH_CALLER_AGENT = 'codex';
  detector.setCustomPath('claude_code', fakeClaude);
  const { AutonomousRunner } = require('../bridge/autonomous-runner');
  const { CallbackManager } = require('../bridge/callback-manager');
  const { TaskExecutionRegistry } = require('../bridge/task-execution-registry');
  const { TaskSupervisor } = require('../bridge/task-supervisor');
  const { ReviewerBootstrapService, confirmReviewerBootstrapTask } = require('../bridge/reviewer-bootstrap');
  const { ReworkController } = require('../bridge/rework-controller');
  const { CancellationRegistry } = require('../bridge/cancellation-registry');
  const { GroupAgentRuntime } = require('../copilot/group-agent-runtime');
  const registry = new TaskExecutionRegistry(storage);
  const runner = new AutonomousRunner(detector, 60_000, new CancellationRegistry(storage), store);
  const callback = new CallbackManager(detector, root);
  callback.setTaskStore(store);
  callback.setAutonomousRunner(runner);
  const supervisor = new TaskSupervisor(store, runner, callback, detector, registry);
  const rework = new ReworkController(store, runner, callback, detector, registry);
  let developerTurns = 0;
  let reviewerTurns = 0;
  let callerReviewMode = false;
  const sessionsByRole: Record<string, string[]> = { developer: [], reviewer: [], bootstrap: [] };
  const fakeModels = {
    getAgentBinding: (groupId: string) => groupId === developerGroupUuid
      ? ({ id: developerAgent, source: 'vscode-lm', enabled: true, toolsAuthorized: true, revision: 3, receiverReady: true, initiatorReady: false, receiverModel: secondGroupModel })
      : ({ id: agent, source: 'vscode-lm', enabled: true, toolsAuthorized: true, revision: 3, receiverReady: true, initiatorReady: true, receiverModel, initiatorModel }),
    async runGroupAgent(groupId: string, modelRef: any, sessionId: string, cwd: string, prompt: string,
      token: any, onPid: (pid?: number) => void, role: 'developer' | 'reviewer' | 'bootstrap', context: any) {
      sessionsByRole[role].push(sessionId);
      const expectedModel = role === 'developer' ? groupId === developerGroupUuid ? secondGroupModel : callerReviewMode ? receiverModel : approvedReceiverModel : initiatorModel;
      assert.deepEqual(modelRef, expectedModel, 'the actual model request must use the explicitly approved/pinned role reference');
      const control = async (name: string, input: Record<string, unknown>) => {
        if (role === 'bootstrap') {
          const thread = confirmReviewerBootstrapTask(store, String(input.threadId), { token: String(input.token) }, storage);
          return JSON.stringify({ threadId: thread.threadId, status: 'DISPATCHED' });
        }
        const threadId = String(input.threadId);
        if (role !== 'reviewer' || threadId !== context.threadId || store.getTask(threadId)?.reviewerSessionId !== sessionId)
          throw new Error('Reviewer session/role does not own task.');
        const owner = new TaskRoleLock(storage).getOwner(threadId);
        if (owner?.role !== 'reviewer' || owner.conversationId !== sessionId) throw new Error('Reviewer lease mismatch.');
        if (name === 'request_rework') return JSON.stringify(await rework.requestRework({ threadId, feedback: String(input.feedback), callerSessionId: sessionId }));
        if (name === 'approve_task') {
          const result = rework.approveTask({ threadId, remarks: String(input.remarks || 'approved') });
          rework.resumePendingMerges();
          return JSON.stringify(result);
        }
        throw new Error(`Unexpected control ${name}`);
      };
      const runtime = new GroupAgentRuntime({ bridgeHome: root, onChildPid: onPid, controlTool: control,
        request: async (messages: any[], tools: any[]) => {
          const latest = messages.at(-1)?.content || prompt;
          const call = (id: string, name: string, args: unknown) => ({ text: '', finishReason: 'tool_calls',
            toolCalls: [{ id, name, arguments: args }], usage: { uncachedInputTokens: 2, cacheReadTokens: 0,
              cacheWriteTokens: 0, promptTokens: 2, outputTokens: 1, totalTokens: 3 } });
          const final = (text: string) => ({ text, finishReason: 'stop', toolCalls: [], usage: {
            uncachedInputTokens: 2, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 2,
            outputTokens: 1, totalTokens: 3 } });
          if (role === 'bootstrap') {
            const tokenMatch = prompt.match(/token["']?\s*[:=]\s*["']([0-9a-f-]{36})/i);
            const taskMatch = prompt.match(/thread(?:Id)?["']?\s*[:=]\s*["'](thread_[A-Za-z0-9_-]+)/i);
            if (!tokenMatch || !taskMatch) throw new Error(`Could not read bootstrap credential from prompt: ${prompt.slice(0, 1000)}`);
            return call('bootstrap-confirm', 'dispatch_task', { threadId: taskMatch[1], token: tokenMatch[1] });
          }
          if (role === 'developer') {
            if (messages.at(-1)?.role === 'tool') return final('Developer completed requested work.');
            developerTurns++;
            if (developerTurns === 1) return { text: '', finishReason: 'tool_calls', toolCalls: [
              { id: 'dev-write', name: 'write_file', arguments: { path: 'result.txt', content: 'first pass\n' } },
              { id: 'dev-command', name: 'run_command', arguments: { command: 'node -e "require(\'fs\').writeFileSync(\'command-check.txt\', \'passed\')"' } },
            ], usage: { uncachedInputTokens: 2, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 2, outputTokens: 1, totalTokens: 3 } };
            return call('dev-rework', 'write_file', { path: 'result.txt', content: 'reworked\n' });
          }
          if (role === 'reviewer') {
            if (messages.at(-1)?.role === 'tool') {
              if (messages.at(-1)?.toolName === 'read_file') {
                return reviewerTurns === 1
                  ? call('review-rework', 'request_rework', { threadId: hostThreadId, feedback: `Update ${store.getTask(hostThreadId)?.targetAgent === 'claude_code' ? 'result.md' : 'result.txt'} to say reworked.` })
                  : call('review-approve', 'approve_task', { threadId: hostThreadId, remarks: 'Reviewed and approved.' });
              }
            }
            reviewerTurns++;
            return call(`review-read-${reviewerTurns}`, 'read_file', { path: store.getTask(hostThreadId)?.targetAgent === 'claude_code' ? 'result.md' : 'result.txt' });
          }
          return final('unexpected role');
        },
      });
      // The thread id is fixed after the independent dispatcher responds.
      hostThreadId = context.threadId;
      const result = await runtime.run(sessionId, cwd, prompt, new AbortController().signal, role);
      return { ...result, ...(result.usage ? { tokenUsage: { ...result.usage, agent, model: (modelRef as any).id,
        source: 'native_reported', available: true, partial: false, attempts: result.modelRequests,
        collectedAt: new Date().toISOString() } } : {}) };
    },
  };
  let hostThreadId = '';
  const Module = require('module');
  const originalLoad = Module._load;
  class CancellationTokenSource {
    private cancelled = false;
    token = { get isCancellationRequested() { return (this as any).source.cancelled; }, source: this,
      onCancellationRequested: (_callback: () => void) => ({ dispose() {} }) };
    cancel() { this.cancelled = true; }
    dispose() {}
  }
  Module._load = function (request: string, parent: unknown, isMain: boolean) {
    return request === 'vscode' ? { CancellationTokenSource } : originalLoad.call(this, request, parent, isMain);
  };
  let broker: { start(intervalMs?: number): void; dispose(): void } | undefined;
  let bootstrap: InstanceType<typeof ReviewerBootstrapService> | undefined;
  let heartbeat: NodeJS.Timeout | undefined;
  try {
    const { GroupAgentHostBroker } = require('../copilot/group-agent-host-broker');
    broker = new GroupAgentHostBroker({ hostInstanceId: host.hostInstanceId, taskStore: store, models: fakeModels as any, storageDir: storage });
    broker!.start(10);
    heartbeat = setInterval(() => writeGroupAgentHost({ ...host, updatedAt: Date.now() }, root), 500);
    bootstrap = new ReviewerBootstrapService({ taskStore: store, runner, detector, executionRegistry: registry,
      storageDir: storage, timeoutMs: 10_000, onSettled: () => supervisor.checkPendingTasks() });
    bootstrap.start(30);

    const buildDispatchScript = (requestedModel?: typeof approvedReceiverModel, targetAgent = 'default') => `const {TaskStore}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/task-store.js'))});` +
      `const {AgentDetector}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/agent-detector.js'))});` +
      `const {ActiveAgentResolver}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/active-agent-resolver.js'))});` +
      `const {AutonomousRunner}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/autonomous-runner.js'))});` +
      `const {CallbackManager}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/callback-manager.js'))});` +
      `const {AsyncDispatcher}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/async-dispatcher.js'))});` +
      `const {MCPServer}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/mcp-server.js'))});` +
      `const {ReworkController}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/rework-controller.js'))});` +
      `const {TaskExecutionRegistry}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/task-execution-registry.js'))});` +
      `const d=new AgentDetector(),s=new TaskStore(${JSON.stringify(storage)}),r=new ActiveAgentResolver(d);` +
      `const run=new AutonomousRunner(d,60000,undefined,s),c=new CallbackManager(d),g=new TaskExecutionRegistry(${JSON.stringify(storage)});` +
      `const x=new AsyncDispatcher(s,r,run,c,d,g),rw=new ReworkController(s,run,c,d,g);` +
      `x.setModelApprovalHandler(async request=>({id:request.id,status:'approved',at:new Date().toISOString(),by:'test-user'}));` +
      `const m=new MCPServer(x,rw,s,d,r,run);m.handleToolCall('dispatch_task',{initiatorAgent:${JSON.stringify(process.env.MESH_CALLER_AGENT || 'codex')},targetAgent:${JSON.stringify(targetAgent)},title:'Group full cycle',prompt:'Create and validate result.txt',workspacePath:${JSON.stringify(repo)},modelRef:${JSON.stringify(requestedModel)},modelRequestedByUser:${Boolean(requestedModel)}})` +
      `.then(v=>process.stdout.write(JSON.stringify(v))).catch(e=>{process.stderr.write(e.stack||e);process.exitCode=1}).finally(()=>s.dispose());`;
    const runIndependentDispatch = async (requestedModel?: typeof approvedReceiverModel, targetAgent = 'default') => {
    const child = childProcess.spawn(process.execPath, ['-e', buildDispatchScript(requestedModel, targetAgent)], { cwd: process.cwd(), env: { ...process.env,
      AGENT_MESH_BRIDGE_HOME: root, HOME: root, USERPROFILE: root }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    let out = ''; let err = '';
    child.stdout.setEncoding('utf8').on('data', (chunk) => out += chunk);
    child.stderr.setEncoding('utf8').on('data', (chunk) => err += chunk);
    const childCode = await new Promise<number | null>((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
    assert.equal(childCode, 0, err);
    return JSON.parse(out);
    };
    const dispatch = await runIndependentDispatch(approvedReceiverModel);
    hostThreadId = dispatch.threadId;
    assert.equal(dispatch.targetAgent, agent);
    assert.equal(dispatch.modelRouting?.initiatorAgent, agent, 'the configured Group reviewer is distinct from the CLI requester');
    assert.equal(store.getTask(hostThreadId)?.initiatorAgent, agent, 'default reviewer must be the Group Agent');
    assert.equal(store.getTask(hostThreadId)?.requesterAgent, 'codex');
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && store.getTask(hostThreadId)?.status !== 'completed')
      await new Promise((resolve) => setTimeout(resolve, 50));
    const finished = store.getTask(hostThreadId)!;
    assert.equal(finished.status, 'completed', JSON.stringify({ status: finished.status, snippet: finished.summary.lastOutputSnippet,
      rounds: finished.rounds.map((r) => ({ round: r.round, type: r.type, status: r.status, error: r.error })) }));
    assert.equal(finished.worktree?.integration?.phase, 'merged');
    assert.deepEqual(finished.targetModelRef, approvedReceiverModel);
    assert.equal(finished.modelApproval?.status, 'approved', 'the public MCP request and user approval are persisted on the task');
    assert.equal(finished.reviewerModelRef?.id, initiatorModel.id);
    assert.equal(finished.reviewerSessionId, finished.callerSessionId);
    assert.equal(finished.receiverSessionId, dispatch.receiverSessionId);
    assert.equal(new Set(sessionsByRole.developer).size, 1, 'developer rework must reuse its original Bridge session');
    assert.equal(new Set(sessionsByRole.reviewer).size, 1, 'reviewer wake must reuse its bootstrap Bridge session');
    assert.ok(finished.rounds.some((r) => r.type === 'rework'));
    assert.equal(fs.readFileSync(path.join(finished.worktree!.path, 'result.txt'), 'utf8'), 'reworked\n');
    assert.equal(fs.readFileSync(path.join(finished.worktree!.path, 'command-check.txt'), 'utf8'), 'passed');

    // Switch only the default receiver for a second real mailbox cycle: the
    // dedicated reviewer remains Group A while developer work runs in Group B.
    developerTurns = 0; reviewerTurns = 0;
    const groupConfig = require('../bridge/group-agent-registry').loadGroupAgentConfig(root);
    require('../bridge/group-agent-registry').saveGroupAgentConfig({ ...groupConfig, defaultAgent: developerAgent, updatedAt: Date.now() }, root);
    const crossDispatch = await runIndependentDispatch();
    const crossDeadline = Date.now() + 15_000;
    while (Date.now() < crossDeadline && store.getTask(crossDispatch.threadId)?.status !== 'completed') await new Promise((resolve) => setTimeout(resolve, 50));
    const crossFinished = store.getTask(crossDispatch.threadId)!;
    assert.equal(crossFinished.status, 'completed', JSON.stringify({ status: crossFinished.status, rounds: crossFinished.rounds.map((round) => ({ status: round.status, error: round.error })) }));
    assert.equal(crossFinished.initiatorAgent, agent);
    assert.equal(crossFinished.targetAgent, developerAgent);
    assert.equal(crossFinished.worktree?.integration?.phase, 'merged');
    assert.equal(crossFinished.rounds.some((round) => round.type === 'rework'), true);
    assert.equal(fs.readFileSync(path.join(crossFinished.worktree!.path, 'result.txt'), 'utf8'), 'reworked\n');
    assert.equal(sessionsByRole.developer.length, 4, 'both Group receivers retain one session each across their rework');
    assert.equal(sessionsByRole.reviewer.length, 4, 'the Group sender resumes each dedicated reviewer session');

    developerTurns = 0; reviewerTurns = 0;
    const groupCliDispatch = await runIndependentDispatch(undefined, 'claude_code');
    const cliDeadline = Date.now() + 15_000;
    while (Date.now() < cliDeadline && store.getTask(groupCliDispatch.threadId)?.status !== 'completed') await new Promise((resolve) => setTimeout(resolve, 50));
    const groupCliFinished = store.getTask(groupCliDispatch.threadId)!;
    assert.equal(groupCliFinished.status, 'completed', JSON.stringify({ status: groupCliFinished.status, rounds: groupCliFinished.rounds.map((round) => ({ status: round.status, error: round.error, assessment: round.assessment })), files: fs.readdirSync(groupCliFinished.worktree!.path), git: childProcess.execFileSync('git', ['status', '--short'], { cwd: groupCliFinished.worktree!.path, encoding: 'utf8' }) }));
    assert.equal(groupCliFinished.initiatorAgent, agent, 'the Group sender remains the dedicated reviewer for a CLI receiver');
    assert.equal(groupCliFinished.targetAgent, 'claude_code');
    assert.equal(groupCliFinished.worktree?.integration?.phase, 'merged');
    assert.equal(groupCliFinished.rounds.some((round) => round.type === 'rework'), true);
    assert.equal(groupCliFinished.receiverSessionId, 'fake-claude-session-001', 'CLI rework resumes its original native session id');
    assert.equal(fs.readFileSync(path.join(groupCliFinished.worktree!.path, 'result.md'), 'utf8'), 'reworked\n');

    // Caller mode uses a real CLI process for bootstrap and both review turns.
    const callerConfig = require('../bridge/group-agent-registry').loadGroupAgentConfig(root);
    require('../bridge/group-agent-registry').saveGroupAgentConfig({ ...callerConfig, defaultAgent: agent, defaultReviewer: 'caller', updatedAt: Date.now() }, root);
    process.env.MESH_CALLER_AGENT = 'claude_code';
    detector.setCustomPath('claude_code', fakeClaudeReviewer);
    callerReviewMode = true;
    const cliReviewDispatch = await runIndependentDispatch();
    const cliReviewerDeadline = Date.now() + 20_000;
    while (Date.now() < cliReviewerDeadline && store.getTask(cliReviewDispatch.threadId)?.status !== 'completed') {
      if (store.getTask(cliReviewDispatch.threadId)?.status === 'waiting_merge') rework.resumePendingMerges();
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const cliReviewFinished = store.getTask(cliReviewDispatch.threadId)!;
    assert.equal(cliReviewFinished.requesterAgent, 'claude_code');
    assert.equal(cliReviewFinished.initiatorAgent, 'claude_code', 'defaultReviewer=caller must keep the actual CLI reviewer');
    assert.equal(cliReviewFinished.targetAgent, agent);
    assert.equal(cliReviewFinished.status, 'completed', JSON.stringify({ status: cliReviewFinished.status, reviewBootstrap: cliReviewFinished.reviewBootstrap,
      rounds: cliReviewFinished.rounds.map((round) => ({ round: round.round, type: round.type, status: round.status, error: round.error, assessment: round.assessment, interruption: round.interruption })) }));
    assert.equal(cliReviewFinished.reviewBootstrap?.phase, 'confirmed');
    const reviewerSessionIds = fs.readFileSync(path.join(root, 'fake-cli-review-sessions'), 'utf8').trim().split('\n');
    assert.equal(cliReviewFinished.reviewerSessionId, reviewerSessionIds[0]);
    assert.equal(new Set(reviewerSessionIds).size, 1, 'bootstrap, rework review and approval all reused one native CLI session');
    assert.equal(cliReviewFinished.worktree?.integration?.phase, 'merged');
    assert.equal(cliReviewFinished.rounds.some((round) => round.type === 'rework'), true);
    assert.equal(fs.readFileSync(path.join(cliReviewFinished.worktree!.path, 'result.txt'), 'utf8'), 'reworked\n');
    assert.equal(fs.readFileSync(path.join(root, 'fake-cli-review-count'), 'utf8'), '2', 'the CLI reviewer used its same captured session for rework and approval');
  } finally {
    bootstrap?.stop(); broker?.dispose(); supervisor.dispose(); rework.dispose(); callback.dispose?.(); store.dispose();
    if (heartbeat) clearInterval(heartbeat);
    Module._load = originalLoad;
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    if (oldUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = oldUserProfile;
    if (oldHomeEnv === undefined) delete process.env.HOME; else process.env.HOME = oldHomeEnv;
    if (oldPath === undefined) delete process.env.PATH; else process.env.PATH = oldPath;
    if (oldCallerAgent === undefined) delete process.env.MESH_CALLER_AGENT; else process.env.MESH_CALLER_AGENT = oldCallerAgent;
    try {
      const worktrees = childProcess.execFileSync('git', ['worktree', 'list', '--porcelain'], { cwd: repo, encoding: 'utf8' });
      for (const line of worktrees.split(/\r?\n/)) {
        if (!line.startsWith('worktree ')) continue;
        const worktree = line.slice('worktree '.length);
        const relative = path.relative(root, worktree);
        if (relative.startsWith(`${path.join('.agent-mesh-bridge', 'worktrees')}${path.sep}`))
          childProcess.execFileSync('git', ['worktree', 'remove', '--force', worktree], { cwd: repo, stdio: 'ignore' });
      }
    } catch {}
    for (let attempt = 0; ; attempt++) {
      try { fs.rmSync(root, { recursive: true, force: true, maxRetries: 30, retryDelay: 100 }); break; }
      catch (error) {
        if (attempt >= 4 || !['ENOTEMPTY', 'EBUSY', 'EPERM'].includes((error as NodeJS.ErrnoException).code || '')) throw error;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  }
});

test('a lost Group host pauses the existing task and resumes it with the same session after a new generation returns', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-host-recovery-'));
  const storage = path.join(root, '.agent-mesh-bridge');
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  childProcess.execFileSync('git', ['init', '-b', 'main'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.name', 'Bridge Test'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['config', 'user.email', 'bridge@example.invalid'], { cwd: root, stdio: 'ignore' });
  fs.writeFileSync(path.join(root, 'README.md'), 'base\n');
  childProcess.execFileSync('git', ['add', '.'], { cwd: root, stdio: 'ignore' });
  childProcess.execFileSync('git', ['commit', '-m', 'base'], { cwd: root, stdio: 'ignore' });
  const groupUuid = '11111111-2222-4333-8444-555555555559';
  const agent = `copilot_group:${groupUuid}` as const;
  const modelRef = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'recovery-model' };
  const snapshot: GroupAgentBindingSnapshot = { agentId: agent, revision: 6, source: 'vscode-lm', modelRef };
  const host = { version: 1 as const, hostInstanceId: 'host-recovery', generation: 1, pid: process.pid,
    workspaceUri: `file://${root}`, repositoryCommonDir: path.join(root, '.git'), trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Recovery group',
    source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
    revision: 6, receiverModel: modelRef, models: [modelRef], modelProbes: [{ role: 'receiver', modelRef, revision: 6, hostGeneration: host.generation }] }] }, root);
  writeGroupAgentHost(host, root);
  const store = new TaskStore(storage);
  const threadId = 'thread_group_host_recovery';
  const sessionId = 'session-group-recovery';
  const base = childProcess.execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
  store.upsertTask({ threadId, title: 'Recover Group task', initiatorAgent: 'codex', targetAgent: agent,
    status: 'running', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
    receiverSessionId: sessionId, workspacePath: root, targetModel: modelRef.id, targetModelRef: modelRef,
    targetGroupBindingSnapshot: snapshot, worktree: { path: root, mainPath: root, baseCommit: base },
    summary: { lastPromptSnippet: 'resume task', lastOutputSnippet: 'running', totalDurationMs: 0 },
    rounds: [{ round: 1, type: 'initial', prompt: 'resume task', status: 'running', durationMs: 0, timestamp: new Date().toISOString() }] });
  const detector = new (require('../bridge/agent-detector').AgentDetector)();
  const { AutonomousRunner } = require('../bridge/autonomous-runner');
  const { CallbackManager } = require('../bridge/callback-manager');
  const { ReworkController } = require('../bridge/rework-controller');
  const { TaskExecutionRegistry } = require('../bridge/task-execution-registry');
  const { TaskSupervisor } = require('../bridge/task-supervisor');
  const { CancellationRegistry } = require('../bridge/cancellation-registry');
  const { GroupAgentRuntime } = require('../copilot/group-agent-runtime');
  const runner = new AutonomousRunner(detector, 10_000, new CancellationRegistry(storage), store);
  const callback = new CallbackManager(detector, storage);
  callback.setTaskStore(store); callback.setAutonomousRunner(runner);
  const registry = new TaskExecutionRegistry(storage);
  const supervisor = new TaskSupervisor(store, runner, callback, detector, registry);
  const rework = new ReworkController(store, runner, callback, detector, registry);
  const lease = new TaskRoleLock(storage).tryAcquire(threadId, 'developer');
  assert.ok(lease);
  let broker: { start(intervalMs?: number): void; dispose(): void } | undefined;
  const Module = require('module'); const originalLoad = Module._load;
  class CancellationTokenSource {
    private cancelled = false;
    token = { get isCancellationRequested() { return (this as any).source.cancelled; }, source: this,
      onCancellationRequested: (callback: () => void) => { this.callback = callback; return { dispose() {} }; } };
    callback?: () => void;
    cancel() { this.cancelled = true; this.callback?.(); }
    dispose() {}
  }
  Module._load = function (request: string, parent: unknown, isMain: boolean) {
    return request === 'vscode' ? { CancellationTokenSource } : originalLoad.call(this, request, parent, isMain);
  };
  try {
    const first = runner.execute({ agent, prompt: 'resume task', isRework: false, receiverSessionId: sessionId,
      cwd: root, taskThreadId: threadId, taskRound: 1, modelRef, role: 'developer', timeoutMs: 5000 });
    const deadline = Date.now() + 3000;
    while (Date.now() < deadline && !fs.existsSync(path.join(root, 'group-agent-mailbox', 'requests')))
      await new Promise((resolve) => setTimeout(resolve, 10));
    const requestsDir = path.join(root, 'group-agent-mailbox', 'requests');
    while (Date.now() < deadline && (!fs.existsSync(requestsDir) || !fs.readdirSync(requestsDir).length))
      await new Promise((resolve) => setTimeout(resolve, 10));
    assert.ok(fs.existsSync(requestsDir) && fs.readdirSync(requestsDir).length, 'runner should publish to the mailbox before host loss');
    writeGroupAgentHost({ ...host, generation: 2, hostInstanceId: 'host-recovery-returned', updatedAt: Date.now() }, root);
    saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Recovery group',
      source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
      revision: 6, receiverModel: modelRef, models: [modelRef], modelProbes: [{ role: 'receiver', modelRef, revision: 6, hostGeneration: 2 }] }] }, root);
    const interrupted = await first;
    assert.equal(interrupted.interruption?.kind, 'paused');
    assert.equal(interrupted.actualSessionId, sessionId);
    const paused = store.updateRound(threadId, 1, { status: 'paused', error: interrupted.error,
      assessment: interrupted.assessment, interruption: interrupted.interruption }, 'paused', sessionId,
      undefined, interrupted.interruption)!;
    assert.equal(paused.status, 'paused');
    new TaskRoleLock(storage).release(lease);

    const { GroupAgentHostBroker } = require('../copilot/group-agent-host-broker');
    const fakeModels = {
      getAgentBinding: () => ({ id: agent, source: 'vscode-lm', enabled: true, toolsAuthorized: true,
        revision: 6, receiverReady: true, initiatorReady: false, receiverModel: modelRef }),
      async runGroupAgent(_groupId: string, _model: unknown, resumedSession: string, cwd: string, prompt: string,
        token: any, onPid: (pid?: number) => void, role: 'developer' | 'reviewer' | 'bootstrap') {
        assert.equal(resumedSession, sessionId); assert.equal(role, 'developer');
        const runtime = new GroupAgentRuntime({ bridgeHome: root, onChildPid: onPid,
          request: async () => ({ text: 'completed after host restart', finishReason: 'stop', toolCalls: [] }) });
        const result = await runtime.run(resumedSession, cwd, prompt, new AbortController().signal, role);
        return { ...result, tokenUsage: { uncachedInputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0,
          promptTokens: 0, outputTokens: 0, totalTokens: 0, agent, model: modelRef.id, source: 'unavailable',
          available: false, partial: true, attempts: 1, collectedAt: new Date().toISOString() } };
      },
    };
    broker = new GroupAgentHostBroker({ hostInstanceId: 'host-recovery-returned', taskStore: store, models: fakeModels as any, storageDir: storage });
    broker!.start(10);
    await rework.resumePausedTask(threadId);
    const resumedDeadline = Date.now() + 10_000;
    while (Date.now() < resumedDeadline && store.getTask(threadId)?.status !== 'waiting_review')
      await new Promise((resolve) => setTimeout(resolve, 25));
    const resumed = store.getTask(threadId)!;
    assert.equal(resumed.status, 'waiting_review');
    assert.equal(resumed.receiverSessionId, sessionId, 'host recovery must retain the original Group session');
    assert.equal(resumed.rounds[0].status, 'completed');
  } finally {
    broker?.dispose(); supervisor.dispose(); rework.dispose(); callback.dispose?.(); store.dispose();
    new TaskRoleLock(storage).release(lease);
    Module._load = originalLoad;
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});
