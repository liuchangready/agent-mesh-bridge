import { strict as assert } from 'assert';
import { test } from 'node:test';
import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { claimGroupAgentRuns, GroupAgentMailboxClient, publishGroupAgentResult } from '../bridge/group-agent-mailbox';
import { saveGroupAgentConfig, writeGroupAgentHost } from '../bridge/group-agent-registry';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread, GroupAgentBindingSnapshot } from '../bridge/types';
import { GroupAgentRuntime } from '../copilot/group-agent-runtime';

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
    publishGroupAgentResult(claimed[0].runId, host.generation, { text: 'same task resumed', toolCallsCompleted: 2, modelRequests: 3 });
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
    publishGroupAgentResult(claimed[0].runId, host.generation, { text: 'done', toolCallsCompleted: 0,
      tokenUsage: { uncachedInputTokens: 5, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 1, outputTokens: 2, totalTokens: 3, agent, available: true, partial: false, source: 'native_reported' } as any });
    await assert.rejects(execution, /no valid result/);
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
    enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false, revision: 2, receiverModel: modelRef }] }, root);
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
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async (_messages, tools) => {
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
    assert.equal(fs.readFileSync(path.join(root, 'from-agent.txt'), 'utf8'), 'written by bridge runtime\n');
    assert.equal(fs.readFileSync(path.join(root, 'command-test.txt'), 'utf8'), 'passed');
    assert.equal(childProcess.execFileSync('git', ['status', '--short'], { cwd: root, encoding: 'utf8' }).includes('from-agent.txt'), true);
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
  const receiverModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'developer-model' };
  const initiatorModel = { source: 'vscode-lm' as const, vendor: 'fake-host', id: 'reviewer-model' };
  const host = { version: 1 as const, hostInstanceId: 'host-vertical', generation: 22, pid: process.pid,
    workspaceUri: `file://${repo}`, repositoryCommonDir: path.join(repo, '.git'), trusted: true as const, localDesktop: true as const, updatedAt: Date.now() };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), defaultAgent: agent, defaultReviewer: agent, agents: [{
    id: agent, groupId: groupUuid, name: 'Vertical Group', source: 'vscode-lm', enabled: true, toolsAuthorized: true,
    receiverReady: true, initiatorReady: true, revision: 3, receiverModel, initiatorModel,
  }] }, root);
  require('../bridge/bridge-policy').saveBridgePolicy({ reviewFlow: 'dedicated' }, path.join(root, '.agent-mesh-bridge'));
  writeGroupAgentHost(host, root);

  const store = new TaskStore(storage);
  const detector = new (require('../bridge/agent-detector').AgentDetector)();
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
  const sessionsByRole: Record<string, string[]> = { developer: [], reviewer: [], bootstrap: [] };
  const fakeModels = {
    getAgentBinding: () => ({ id: agent, source: 'vscode-lm', enabled: true, toolsAuthorized: true,
      revision: 3, receiverReady: true, initiatorReady: true, receiverModel, initiatorModel }),
    async runGroupAgent(_groupId: string, modelRef: unknown, sessionId: string, cwd: string, prompt: string,
      token: any, onPid: (pid?: number) => void, role: 'developer' | 'reviewer' | 'bootstrap', context: any) {
      sessionsByRole[role].push(sessionId);
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
                  ? call('review-rework', 'request_rework', { threadId: hostThreadId, feedback: 'Update result.txt to say reworked.' })
                  : call('review-approve', 'approve_task', { threadId: hostThreadId, remarks: 'Reviewed and approved.' });
              }
            }
            reviewerTurns++;
            return call(`review-read-${reviewerTurns}`, 'read_file', { path: 'result.txt' });
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

    const script = `const {TaskStore}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/task-store.js'))});` +
      `const {AgentDetector}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/agent-detector.js'))});` +
      `const {ActiveAgentResolver}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/active-agent-resolver.js'))});` +
      `const {AutonomousRunner}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/autonomous-runner.js'))});` +
      `const {CallbackManager}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/callback-manager.js'))});` +
      `const {AsyncDispatcher}=require(${JSON.stringify(path.resolve(__dirname, '../bridge/async-dispatcher.js'))});` +
      `const d=new AgentDetector(),s=new TaskStore(${JSON.stringify(storage)}),r=new ActiveAgentResolver(d);` +
      `r.setManualOverride(${JSON.stringify(agent)});const run=new AutonomousRunner(d,60000,undefined,s),c=new CallbackManager(d);` +
      `const x=new AsyncDispatcher(s,r,run,c,d);x.dispatchTask({targetAgent:'default',title:'Group full cycle',prompt:'Create and validate result.txt',workspacePath:${JSON.stringify(repo)}})` +
      `.then(v=>process.stdout.write(JSON.stringify(v))).catch(e=>{process.stderr.write(e.stack||e);process.exitCode=1}).finally(()=>s.dispose());`;
    const child = childProcess.spawn(process.execPath, ['-e', script], { cwd: process.cwd(), env: { ...process.env,
      AGENT_MESH_BRIDGE_HOME: root, HOME: root, USERPROFILE: root }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
    let out = ''; let err = '';
    child.stdout.setEncoding('utf8').on('data', (chunk) => out += chunk);
    child.stderr.setEncoding('utf8').on('data', (chunk) => err += chunk);
    const childCode = await new Promise<number | null>((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
    assert.equal(childCode, 0, err);
    const dispatch = JSON.parse(out);
    hostThreadId = dispatch.threadId;
    assert.equal(dispatch.targetAgent, agent);
    assert.equal(store.getTask(hostThreadId)?.initiatorAgent, agent, 'default reviewer must be the Group Agent');
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && store.getTask(hostThreadId)?.status !== 'completed')
      await new Promise((resolve) => setTimeout(resolve, 50));
    const finished = store.getTask(hostThreadId)!;
    assert.equal(finished.status, 'completed', JSON.stringify({ status: finished.status, snippet: finished.summary.lastOutputSnippet,
      rounds: finished.rounds.map((r) => ({ round: r.round, type: r.type, status: r.status, error: r.error })) }));
    assert.equal(finished.worktree?.integration?.phase, 'merged');
    assert.equal(finished.targetModelRef?.id, receiverModel.id);
    assert.equal(finished.reviewerModelRef?.id, initiatorModel.id);
    assert.equal(finished.reviewerSessionId, finished.callerSessionId);
    assert.equal(finished.receiverSessionId, dispatch.receiverSessionId);
    assert.equal(new Set(sessionsByRole.developer).size, 1, 'developer rework must reuse its original Bridge session');
    assert.equal(new Set(sessionsByRole.reviewer).size, 1, 'reviewer wake must reuse its bootstrap Bridge session');
    assert.ok(finished.rounds.some((r) => r.type === 'rework'));
    assert.equal(fs.readFileSync(path.join(finished.worktree!.path, 'result.txt'), 'utf8'), 'reworked\n');
    assert.equal(fs.readFileSync(path.join(finished.worktree!.path, 'command-check.txt'), 'utf8'), 'passed');
  } finally {
    bootstrap?.stop(); broker?.dispose(); supervisor.dispose(); rework.dispose(); callback.dispose?.(); store.dispose();
    if (heartbeat) clearInterval(heartbeat);
    Module._load = originalLoad;
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME; else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    if (oldUserProfile === undefined) delete process.env.USERPROFILE; else process.env.USERPROFILE = oldUserProfile;
    if (oldHomeEnv === undefined) delete process.env.HOME; else process.env.HOME = oldHomeEnv;
    fs.rmSync(root, { recursive: true, force: true });
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
    revision: 6, receiverModel: modelRef }] }, root);
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
