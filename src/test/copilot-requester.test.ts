import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { execFileSync } from 'child_process';

function initRepository(root: string): void {
  execFileSync('git', ['init', root], { windowsHide: true, stdio: 'pipe' });
  execFileSync('git', ['-C', root, '-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '--allow-empty', '-m', 'Initial'], { windowsHide: true, stdio: 'pipe' });
}

import { assertCanExecute, EXECUTABLE_AGENTS } from '../bridge/agent-capabilities';
import { MCPServer } from '../bridge/mcp-server';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { TaskStore } from '../bridge/task-store';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { claimCopilotTerminalNotification } from '../copilot/copilot-notifications';
import { isCopilotRequester, projectCopilotTaskDetails } from '../copilot/copilot-projection';

test('Copilot is never an executable transport', () => {
  for (const agent of EXECUTABLE_AGENTS) assert.doesNotThrow(() => assertCanExecute(agent));
  assert.throws(() => assertCanExecute('vscode_chat'), /requester-only/);
});

test('runner refuses Copilot before attempting CLI fallback', async () => {
  const runner = new AutonomousRunner({ resolveExecutable: () => 'vscode_internal' } as any);
  await assert.rejects(runner.execute({ agent: 'vscode_chat', prompt: 'no', isRework: false, receiverSessionId: 'unused' }), /requester-only/);
});

test('Copilot MCP facade exposes only requester and bounded read tools', () => {
  const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true }) as MCPServer;
  const names = server.getToolDefinitions().map((tool: any) => tool.name);
  assert.deepEqual(names, [
    'submit_mesh_task', 'list_recent_tasks', 'get_thread_details',
    'get_task_summary', 'get_task_token_usage', 'list_agent_models', 'query_agent_mesh_status',
  ]);
  const submit = server.getToolDefinitions().find((tool: any) => tool.name === 'submit_mesh_task');
  assert.ok(submit.inputSchema.required.includes('submissionId'));
  assert.equal('callerSessionId' in submit.inputSchema.properties, false);
  assert.equal('initiatorAgent' in submit.inputSchema.properties, false);
});

test('Copilot MCP facade rejects hidden control calls even when sent directly', async () => {
  const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true }) as MCPServer;
  await assert.rejects(server.handleToolCall('approve_task', {}), /not available in the Copilot requester profile/);
  await assert.rejects(server.handleToolCall('dispatch_task', {}), /not available in the Copilot requester profile/);
});

test('receiver cannot call submit_mesh_task directly in either MCP profile', async () => {
  const old = { receiver: process.env.MESH_TASK_RECEIVER, profile: process.env.MESH_MCP_PROFILE };
  let calls = 0;
  try {
    process.env.MESH_TASK_RECEIVER = '1';
    for (const profile of ['mesh-agent', 'copilot-requester']) {
      process.env.MESH_MCP_PROFILE = profile;
      const server = Object.assign(Object.create(MCPServer.prototype), {
        copilotProfile: profile === 'copilot-requester', dispatcher: { dispatchTask: () => { calls++; } },
      }) as MCPServer;
      await assert.rejects(server.handleToolCall('submit_mesh_task', { title: 'x' }), /receiver of delegated task/);
    }
    assert.equal(calls, 0);
  } finally {
    if (old.receiver === undefined) delete process.env.MESH_TASK_RECEIVER; else process.env.MESH_TASK_RECEIVER = old.receiver;
    if (old.profile === undefined) delete process.env.MESH_MCP_PROFILE; else process.env.MESH_MCP_PROFILE = old.profile;
  }
});

test('Copilot raw-history flags and unknown query arguments are rejected at direct call boundary', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-args-'));
  const file = path.join(root, 'host.json');
  const oldEnv = { profile: process.env.MESH_MCP_PROFILE, binding: process.env.MESH_COPILOT_BINDING_FILE, workspace: process.env.MESH_COPILOT_WORKSPACE_PATH, uri: process.env.MESH_COPILOT_WORKSPACE_URI, authority: process.env.MESH_COPILOT_WORKSPACE_AUTHORITY, host: process.env.MESH_COPILOT_HOST_ID, reviewer: process.env.MESH_COPILOT_REVIEWER };
  try {
    process.env.MESH_MCP_PROFILE = 'copilot-requester';
    process.env.MESH_COPILOT_BINDING_FILE = file;
    process.env.MESH_COPILOT_WORKSPACE_PATH = root;
    process.env.MESH_COPILOT_WORKSPACE_URI = `file://${root.replace(/\\/g, '/')}`;
    process.env.MESH_COPILOT_WORKSPACE_AUTHORITY = '';
    process.env.MESH_COPILOT_HOST_ID = 'host';
    process.env.MESH_COPILOT_REVIEWER = 'claude_code';
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: root }));
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, taskStore: { getTask: () => ({ requesterBinding: { workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI } }) } }) as MCPServer;
    await assert.rejects(server.handleToolCall('get_thread_details', { threadId: 'allowed', includeFullHistory: true }), /Argument includeFullHistory/);
    await assert.rejects(server.handleToolCall('get_thread_details', { threadId: 'allowed', includeRawLogs: true }), /Argument includeRawLogs/);
    await assert.rejects(server.handleToolCall('get_task_summary', { threadId: 'allowed', callerSessionId: 'forged' }), /Argument callerSessionId/);
    const schema = server.getToolDefinitions().find((tool: any) => tool.name === 'get_thread_details')!.inputSchema;
    assert.deepEqual(Object.keys(schema.properties), ['threadId']);
  } finally {
    process.env.MESH_MCP_PROFILE = oldEnv.profile || '';
    process.env.MESH_COPILOT_BINDING_FILE = oldEnv.binding || '';
    process.env.MESH_COPILOT_WORKSPACE_PATH = oldEnv.workspace || '';
    process.env.MESH_COPILOT_WORKSPACE_URI = oldEnv.uri || '';
    process.env.MESH_COPILOT_WORKSPACE_AUTHORITY = oldEnv.authority || '';
    process.env.MESH_COPILOT_HOST_ID = oldEnv.host || '';
    process.env.MESH_COPILOT_REVIEWER = oldEnv.reviewer || '';
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Copilot task projection caps recent evidence and never exposes internal session or reviewer fields', () => {
  const rounds = Array.from({ length: 8 }, (_, i) => ({ round: i + 1, type: 'rework', status: 'completed', timestamp: 'now', durationMs: 1, prompt: 'p'.repeat(12_000), output: 'o'.repeat(20_000), touchedFiles: [] }));
  const projected = projectCopilotTaskDetails({ threadId: 't', title: 'task', status: 'running', currentRound: 8, createdAt: 'now', updatedAt: 'now', requesterAgent: 'vscode_chat', initiatorAgent: 'claude_code', targetAgent: 'codex', rounds, reviewerSessionId: 'private', worktree: { path: 'private' } } as any, { byAgent: {}, byModel: Object.fromEntries(Array.from({ length: 30 }, (_, i) => [`model-${i}`, i])), totalRounds: 8 } as any);
  assert.equal(projected.rounds.length, 4);
  assert.equal(projected.rounds.at(-1)?.prompt.length, 8000);
  assert.equal(projected.rounds.at(-1)?.output.length, 8000);
  assert.ok(projected.truncationHint);
  assert.equal('reviewerSessionId' in projected, false);
  assert.equal('worktree' in projected, false);
  assert.equal(Object.keys(projected.tokenUsage.byModel).length, 20);
  assert.equal(projected.tokenUsage.byModelTruncated, true);
});

test('task summary keeps Copilot requester model unknown while preserving reviewer identity', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-summary-'));
  const file = path.join(root, 'host.json');
  const old = { profile: process.env.MESH_MCP_PROFILE, binding: process.env.MESH_COPILOT_BINDING_FILE, workspace: process.env.MESH_COPILOT_WORKSPACE_PATH, uri: process.env.MESH_COPILOT_WORKSPACE_URI, authority: process.env.MESH_COPILOT_WORKSPACE_AUTHORITY, host: process.env.MESH_COPILOT_HOST_ID, reviewer: process.env.MESH_COPILOT_REVIEWER };
  try {
    Object.assign(process.env, { MESH_MCP_PROFILE: 'copilot-requester', MESH_COPILOT_BINDING_FILE: file, MESH_COPILOT_WORKSPACE_PATH: root, MESH_COPILOT_WORKSPACE_URI: `file://${root.replace(/\\/g, '/')}`, MESH_COPILOT_WORKSPACE_AUTHORITY: '', MESH_COPILOT_HOST_ID: 'host', MESH_COPILOT_REVIEWER: 'claude_code' });
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: root }));
    const task = { threadId: 't', title: 'task', status: 'running', createdAt: 'now', updatedAt: 'now', requesterAgent: 'vscode_chat', initiatorAgent: 'claude_code', initiatorModel: 'reviewer-model', reviewerModel: 'reviewer-model', targetAgent: 'codex', requesterBinding: { kind: 'vscode-copilot', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI } };
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, taskStore: { getTask: () => task } }) as MCPServer;
    const result = await server.handleToolCall('get_task_summary', { threadId: 't' });
    assert.equal(result.requesterAgent, 'vscode_chat');
    assert.equal(result.reviewerAgent, 'claude_code');
    assert.equal(result.requesterModel, null);
    assert.equal(result.requesterModelSource, 'none');
    assert.equal(result.reviewerModel, 'reviewer-model');
    assert.equal(isCopilotRequester(task as any), true);
    assert.equal(isCopilotRequester({ requesterAgent: 'claude_code' } as any), false);
  } finally {
    for (const [key, value] of Object.entries({ MESH_MCP_PROFILE: old.profile, MESH_COPILOT_BINDING_FILE: old.binding, MESH_COPILOT_WORKSPACE_PATH: old.workspace, MESH_COPILOT_WORKSPACE_URI: old.uri, MESH_COPILOT_WORKSPACE_AUTHORITY: old.authority, MESH_COPILOT_HOST_ID: old.host, MESH_COPILOT_REVIEWER: old.reviewer })) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Copilot list_agent_models uses the public targetAgent argument and returns only that agent', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-model-schema-'));
  const file = path.join(root, 'host.json');
  const old = { profile: process.env.MESH_MCP_PROFILE, binding: process.env.MESH_COPILOT_BINDING_FILE, workspace: process.env.MESH_COPILOT_WORKSPACE_PATH, uri: process.env.MESH_COPILOT_WORKSPACE_URI, authority: process.env.MESH_COPILOT_WORKSPACE_AUTHORITY, host: process.env.MESH_COPILOT_HOST_ID, reviewer: process.env.MESH_COPILOT_REVIEWER };
  try {
    Object.assign(process.env, { MESH_MCP_PROFILE: 'copilot-requester', MESH_COPILOT_BINDING_FILE: file, MESH_COPILOT_WORKSPACE_PATH: root, MESH_COPILOT_WORKSPACE_URI: `file://${root.replace(/\\/g, '/')}`, MESH_COPILOT_WORKSPACE_AUTHORITY: '', MESH_COPILOT_HOST_ID: 'host', MESH_COPILOT_REVIEWER: 'claude_code' });
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: root }));
    const catalogs: any = Object.fromEntries(['codex', 'claude_code', 'antigravity', 'deepseek_harness'].map((agent) => [agent, { models: [], source: 'mock', updatedAt: 'now' }]));
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, modelCatalog: { getAll: async () => catalogs } }) as MCPServer;
    const tool = server.getToolDefinitions().find((entry: any) => entry.name === 'list_agent_models') as any;
    assert.ok(tool.inputSchema.properties.targetAgent);
    assert.equal(tool.inputSchema.properties.agent, undefined);
    const result = await server.handleToolCall('list_agent_models', { targetAgent: 'codex' });
    assert.deepEqual(Object.keys(result.agents), ['codex']);
  } finally {
    for (const [key, value] of Object.entries({ MESH_MCP_PROFILE: old.profile, MESH_COPILOT_BINDING_FILE: old.binding, MESH_COPILOT_WORKSPACE_PATH: old.workspace, MESH_COPILOT_WORKSPACE_URI: old.uri, MESH_COPILOT_WORKSPACE_AUTHORITY: old.authority, MESH_COPILOT_HOST_ID: old.host, MESH_COPILOT_REVIEWER: old.reviewer })) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Copilot token usage facade bounds rounds and preserves aggregate totals including omitted history', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-usage-'));
  const file = path.join(root, 'host.json');
  const old = { profile: process.env.MESH_MCP_PROFILE, binding: process.env.MESH_COPILOT_BINDING_FILE, workspace: process.env.MESH_COPILOT_WORKSPACE_PATH, uri: process.env.MESH_COPILOT_WORKSPACE_URI, authority: process.env.MESH_COPILOT_WORKSPACE_AUTHORITY, host: process.env.MESH_COPILOT_HOST_ID, reviewer: process.env.MESH_COPILOT_REVIEWER };
  try {
    Object.assign(process.env, { MESH_MCP_PROFILE: 'copilot-requester', MESH_COPILOT_BINDING_FILE: file, MESH_COPILOT_WORKSPACE_PATH: root, MESH_COPILOT_WORKSPACE_URI: `file://${root.replace(/\\/g, '/')}`, MESH_COPILOT_WORKSPACE_AUTHORITY: '', MESH_COPILOT_HOST_ID: 'host', MESH_COPILOT_REVIEWER: 'claude_code' });
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: root }));
    const rounds = Array.from({ length: 1000 }, (_, i) => ({ round: i + 1, type: 'rework', status: 'completed', durationMs: 1, executorAgent: 'codex', executorModel: 'model', tokenUsage: { uncachedInputTokens: 1, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 1, outputTokens: 1, totalTokens: 2, agent: 'codex', model: 'model', source: 'native_reported', available: true, collectedAt: 'now', secretBlob: 'must not escape' } }));
    const task: any = { threadId: 't', title: 'task', status: 'completed', requesterAgent: 'vscode_chat', requesterBinding: { kind: 'vscode-copilot', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI }, rounds, privateLedgerField: 'must not escape' };
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, taskStore: { getTask: () => task } }) as MCPServer;
    const all = await server.handleToolCall('get_task_token_usage', { threadId: 't' });
    assert.equal(all.rounds.length, 4);
    assert.equal(all.omittedRoundCount, 996);
    assert.equal(all.roundsTruncated, true);
    assert.equal(all.tokenUsage.totalTokens, 2000);
    assert.equal(all.tokenUsage.measuredRounds, 1000);
    assert.equal(all.rounds[0].round, 997);
    assert.equal('secretBlob' in all.rounds[0].tokenUsage, false);
    assert.equal('privateLedgerField' in all, false);

    const single = await server.handleToolCall('get_task_token_usage', { threadId: 't', round: 1 });
    assert.equal(single.round, 1);
    assert.equal(single.tokenUsage.totalTokens, 2);
    assert.equal(single.taskTotal.totalTokens, 2000);
    assert.equal(single.taskTotal.measuredRounds, 1000);
    assert.equal('secretBlob' in single.tokenUsage, false);
    assert.equal('rounds' in single, false);
  } finally {
    for (const [key, value] of Object.entries({ MESH_MCP_PROFILE: old.profile, MESH_COPILOT_BINDING_FILE: old.binding, MESH_COPILOT_WORKSPACE_PATH: old.workspace, MESH_COPILOT_WORKSPACE_URI: old.uri, MESH_COPILOT_WORKSPACE_AUTHORITY: old.authority, MESH_COPILOT_HOST_ID: old.host, MESH_COPILOT_REVIEWER: old.reviewer })) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Copilot task list calls need no threadId and only returns workspace bound rows with a bounded limit', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-list-'));
  const file = path.join(root, 'host.json');
  const old = { profile: process.env.MESH_MCP_PROFILE, binding: process.env.MESH_COPILOT_BINDING_FILE, workspace: process.env.MESH_COPILOT_WORKSPACE_PATH, uri: process.env.MESH_COPILOT_WORKSPACE_URI, authority: process.env.MESH_COPILOT_WORKSPACE_AUTHORITY, host: process.env.MESH_COPILOT_HOST_ID, reviewer: process.env.MESH_COPILOT_REVIEWER };
  try {
    Object.assign(process.env, { MESH_MCP_PROFILE: 'copilot-requester', MESH_COPILOT_BINDING_FILE: file, MESH_COPILOT_WORKSPACE_PATH: root, MESH_COPILOT_WORKSPACE_URI: `file://${root.replace(/\\/g, '/')}`, MESH_COPILOT_WORKSPACE_AUTHORITY: '', MESH_COPILOT_HOST_ID: 'host', MESH_COPILOT_REVIEWER: 'claude_code' });
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: root }));
    const rows = [
      { threadId: 'mine', requesterBinding: { workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI }, rounds: [] },
      { threadId: 'other', requesterBinding: { workspaceUri: 'file:///other' }, rounds: [] },
    ];
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, taskStore: { listTasks: (n: number) => { assert.equal(n, 1000); return rows; } } }) as MCPServer;
    const result = await server.handleToolCall('list_recent_tasks', { limit: 500 });
    assert.deepEqual(result.map((row: any) => row.threadId), ['mine']);
    await assert.rejects(server.handleToolCall('list_recent_tasks', { limit: 0 }), /positive integer/);
  } finally {
    for (const [key, value] of Object.entries({ MESH_MCP_PROFILE: old.profile, MESH_COPILOT_BINDING_FILE: old.binding, MESH_COPILOT_WORKSPACE_PATH: old.workspace, MESH_COPILOT_WORKSPACE_URI: old.uri, MESH_COPILOT_WORKSPACE_AUTHORITY: old.authority, MESH_COPILOT_HOST_ID: old.host, MESH_COPILOT_REVIEWER: old.reviewer })) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Copilot terminal notification is host-bound and deduplicated per thread and terminal state', () => {
  const seen = new Set<string>();
  const task = { threadId: 't', status: 'completed', requesterAgent: 'vscode_chat', requesterBinding: { kind: 'vscode-copilot', hostInstanceId: 'host-a', workspaceUri: 'file:///same' } } as any;
  assert.equal(claimCopilotTerminalNotification(task, 'host-b', seen), false);
  assert.equal(claimCopilotTerminalNotification(task, 'host-a', seen), true);
  assert.equal(claimCopilotTerminalNotification(task, 'host-a', seen), false);
  assert.equal(seen.size, 1);
  assert.equal(claimCopilotTerminalNotification({ ...task, requesterBinding: { ...task.requesterBinding, hostInstanceId: 'host-c', workspaceUri: 'file:///other' } }, 'host-a', seen), false);
});

test('requester source and submission retry key are present in the first persisted task record', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-ledger-'));
  initRepository(root);
  const store = new TaskStore(root);
  const wasActive = TaskSupervisor.isSupervisorActive;
  TaskSupervisor.isSupervisorActive = (() => true) as typeof TaskSupervisor.isSupervisorActive;
  try {
    const dispatcher = new AsyncDispatcher(store, { resolveActiveAgent: async () => ({ agent: 'codex', mode: 'auto', reason: 'test' }) } as any, {} as any, {} as any);
    const result = await dispatcher.dispatchTask({
      targetAgent: 'codex', initiatorAgent: 'claude_code', title: 'copilot source test',
      prompt: 'brief', workspacePath: root,
      requesterContext: {
        agent: 'vscode_chat', submissionId: 'retry-key-0001', fingerprint: 'sha256-test',
        binding: { kind: 'vscode-copilot', hostInstanceId: 'host-test', workspaceUri: 'file:///workspace', nativeWakeSupported: false, notificationDelivery: 'vscode-ui' },
      },
    });
    store.loadFromDisk();
    const persisted = store.getTask(result.threadId)!;
    assert.equal(persisted.requesterAgent, 'vscode_chat');
    assert.equal(persisted.requesterModelSource, 'none');
    assert.equal(persisted.requesterSessionId, undefined);
    assert.equal(persisted.initiatorAgent, 'claude_code');
    assert.equal(persisted.reviewFlow, 'dedicated');
    assert.equal(persisted.reviewBootstrap?.phase, 'pending');
    assert.equal(persisted.requesterSubmissionId, 'retry-key-0001');
    assert.equal(persisted.requesterSubmissionFingerprint, 'sha256-test');
  } finally {
    TaskSupervisor.isSupervisorActive = wasActive;
    store.dispose();
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('requester profile refuses an untrusted or stale host binding before status queries', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-host-'));
  const file = path.join(root, 'host.json');
  const prior = Object.fromEntries(['MESH_COPILOT_BINDING_FILE', 'MESH_COPILOT_WORKSPACE_PATH', 'MESH_COPILOT_WORKSPACE_URI', 'MESH_COPILOT_HOST_ID'].map((key) => [key, process.env[key]]));
  try {
    fs.writeFileSync(file, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: false, remoteName: undefined, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'host', workspaceUri: 'file:///workspace', workspacePath: root }));
    process.env.MESH_COPILOT_BINDING_FILE = file;
    process.env.MESH_COPILOT_WORKSPACE_PATH = root;
    process.env.MESH_COPILOT_WORKSPACE_URI = 'file:///workspace';
    process.env.MESH_COPILOT_HOST_ID = 'host';
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true }) as MCPServer;
    await assert.rejects(server.handleToolCall('query_agent_mesh_status', {}), /trusted local desktop workspace/);
  } finally {
    for (const [key, value] of Object.entries(prior)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('provider config and workspace events revoke an already connected facade', async () => {
  const Module = require('module') as any;
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-provider-events-'));
  const listeners: Record<string, Array<(event?: any) => void>> = { folders: [], trust: [], config: [], definitions: [] };
  const subscriptions: Array<{ dispose(): void }> = [];
  const workspaceUri = (folder: string) => ({ scheme: 'file', authority: '', fsPath: folder, toString: () => `file://${folder.replace(/\\/g, '/')}` });
  let folders = [{ uri: workspaceUri(root) }];
  let enabled = true;
  let reviewer = 'claude_code';
  let provider: any;
  let definition: any;
  const event = (key: string) => (listener: (event?: any) => void) => { listeners[key].push(listener); return { dispose() {} }; };
  const vscodeMock: any = {
    workspace: {
      get workspaceFolders() { return folders; }, isTrusted: true,
      getConfiguration: () => ({ get: (key: string, fallback?: any) => key === 'enabled' ? enabled : key === 'reviewer' ? reviewer : fallback }),
      onDidChangeWorkspaceFolders: event('folders'), onDidGrantWorkspaceTrust: event('trust'), onDidChangeConfiguration: event('config'),
    },
    env: { remoteName: undefined, uiKind: 1 }, UIKind: { Web: 2 },
    EventEmitter: class { event = event('definitions'); fire() { for (const cb of listeners.definitions) cb(); } dispose() {} },
    lm: { registerMcpServerDefinitionProvider: (_id: string, value: any) => { provider = value; return { dispose() {} }; } },
    McpStdioServerDefinition: class { constructor(public label: string, public command: string, public args: string[], public env: any, public version: string) {} cwd: any; },
    extensions: { getExtension: () => ({ extensionPath: root }) },
    window: { showWarningMessage: () => Promise.resolve(undefined) },
  };
  const originalLoad = (Module as any)._load;
  const originalNode = process.env.MESH_NODE_EXECUTABLE;
  const oldEnv = Object.fromEntries(['MESH_MCP_PROFILE', 'MESH_COPILOT_BINDING_FILE', 'MESH_COPILOT_WORKSPACE_PATH', 'MESH_COPILOT_WORKSPACE_URI', 'MESH_COPILOT_WORKSPACE_AUTHORITY', 'MESH_COPILOT_HOST_ID', 'MESH_COPILOT_REVIEWER'].map((key) => [key, process.env[key]]));
  const context = { globalStorageUri: { fsPath: root }, subscriptions } as any;
  try {
    (Module as any)._load = function(request: string, parent: any, isMain: boolean) { return request === 'vscode' ? vscodeMock : originalLoad.call(this, request, parent, isMain); };
    process.env.MESH_NODE_EXECUTABLE = 'node-mock';
    const providerModule = require('../copilot/copilot-mcp-provider');
    providerModule.registerCopilotMcpProvider(context);
    const [definitionValue] = await provider.provideMcpServerDefinitions();
    definition = definitionValue;
    process.env.MESH_MCP_PROFILE = definition.env.MESH_MCP_PROFILE;
    for (const [key, envKey] of [['MESH_COPILOT_BINDING_FILE', 'MESH_COPILOT_BINDING_FILE'], ['MESH_COPILOT_WORKSPACE_PATH', 'MESH_COPILOT_WORKSPACE_PATH'], ['MESH_COPILOT_WORKSPACE_URI', 'MESH_COPILOT_WORKSPACE_URI'], ['MESH_COPILOT_WORKSPACE_AUTHORITY', 'MESH_COPILOT_WORKSPACE_AUTHORITY'], ['MESH_COPILOT_HOST_ID', 'MESH_COPILOT_HOST_ID'], ['MESH_COPILOT_REVIEWER', 'MESH_COPILOT_REVIEWER']]) process.env[key] = definition.env[envKey];
    const server = Object.assign(Object.create(MCPServer.prototype), { copilotProfile: true, taskStore: { listTasks: () => [] } }) as MCPServer;
    assert.deepEqual(await server.handleToolCall('list_recent_tasks', {}), []);

    enabled = false;
    for (const cb of listeners.config) cb({ affectsConfiguration: () => true });
    await assert.rejects(server.handleToolCall('list_recent_tasks', {}), /live trusted local desktop workspace/);
    enabled = true;
    reviewer = 'codex';
    for (const cb of listeners.config) cb({ affectsConfiguration: () => true });
    await assert.rejects(server.handleToolCall('list_recent_tasks', {}), /live trusted local desktop workspace/);
    reviewer = 'claude_code';
    folders = [...folders, { uri: workspaceUri(path.join(root, 'second')) }];
    for (const cb of listeners.folders) cb();
    assert.deepEqual(await provider.provideMcpServerDefinitions(), []);
    await assert.rejects(server.handleToolCall('list_recent_tasks', {}), /live trusted local desktop workspace/);
  } finally {
    (Module as any)._load = originalLoad;
    if (originalNode === undefined) delete process.env.MESH_NODE_EXECUTABLE; else process.env.MESH_NODE_EXECUTABLE = originalNode;
    for (const sub of subscriptions) sub.dispose();
    for (const [key, value] of Object.entries(oldEnv)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('same submission ID is serialized across requester MCP processes', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-lock-'));
  const server = Object.assign(Object.create(MCPServer.prototype), {
    taskStore: { getStoragePath: () => path.join(root, 'delegation_threads.json') },
  }) as any;
  try {
    const first = await server.acquireCopilotSubmissionLock('workspace\0same-id');
    const waiting = server.acquireCopilotSubmissionLock('workspace\0same-id');
    let acquired = false;
    void waiting.then(() => { acquired = true; });
    await new Promise((resolve) => setTimeout(resolve, 20));
    assert.equal(acquired, false);
    first();
    const second = await waiting;
    assert.equal(acquired, true);
    second();
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('real MCP facade submission persists once, retries by submissionId, and recovers after failed admission', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-copilot-facade-store-'));
  const workspace = path.join(root, 'workspace');
  fs.mkdirSync(workspace);
  initRepository(workspace);
  const bindingFile = path.join(root, 'host.json');
  const keys = ['MESH_MCP_PROFILE', 'MESH_COPILOT_BINDING_FILE', 'MESH_COPILOT_WORKSPACE_PATH', 'MESH_COPILOT_WORKSPACE_URI', 'MESH_COPILOT_WORKSPACE_AUTHORITY', 'MESH_COPILOT_HOST_ID', 'MESH_COPILOT_REVIEWER', 'MESH_TASK_RECEIVER', 'MESH_TASK_THREAD_ID', 'MESH_TASK_ROUND'];
  const oldEnv = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const store = new TaskStore(path.join(root, 'store'));
  const wasActive = TaskSupervisor.isSupervisorActive;
  let executorCalls = 0;
  const runner = { execute: async () => { executorCalls++; throw new Error('executor must not run before reviewer bootstrap'); } };
  try {
    Object.assign(process.env, {
      MESH_MCP_PROFILE: 'copilot-requester', MESH_COPILOT_BINDING_FILE: bindingFile,
      MESH_COPILOT_WORKSPACE_PATH: workspace, MESH_COPILOT_WORKSPACE_URI: `file://${workspace.replace(/\\/g, '/')}`,
      MESH_COPILOT_WORKSPACE_AUTHORITY: '', MESH_COPILOT_HOST_ID: 'facade-host', MESH_COPILOT_REVIEWER: 'claude_code',
    });
    const writeBinding = () => fs.writeFileSync(bindingFile, JSON.stringify({ enabled: true, workspaceCount: 1, reviewer: 'claude_code', trusted: true, remoteName: undefined, uiKind: 'desktop', updatedAt: Date.now(), hostInstanceId: 'facade-host', workspaceUri: process.env.MESH_COPILOT_WORKSPACE_URI, workspaceAuthority: '', workspacePath: workspace }));
    writeBinding();
    const dispatcher = new AsyncDispatcher(store, { resolveActiveAgent: async () => ({ agent: 'codex', mode: 'auto', reason: 'mock' }) } as any, runner as any, {} as any);
    const server = new MCPServer(dispatcher, {} as any, store, {} as any, {} as any, runner as any);
    const args = (submissionId: string) => ({ submissionId, title: 'same request', prompt: 'same prompt', targetAgent: 'codex' });

    TaskSupervisor.isSupervisorActive = (() => false) as typeof TaskSupervisor.isSupervisorActive;
    await assert.rejects(server.handleToolCall('submit_mesh_task', args('recover-key-0001')), /reviewer host to be active/);
    assert.equal(store.getAllTasks().length, 0);

    TaskSupervisor.isSupervisorActive = (() => true) as typeof TaskSupervisor.isSupervisorActive;
    const first = await server.handleToolCall('submit_mesh_task', args('recover-key-0001'));
    const retry = await server.handleToolCall('submit_mesh_task', args('recover-key-0001'));
    const independent = await server.handleToolCall('submit_mesh_task', args('independent-key-1'));
    assert.equal(first.threadId, retry.threadId);
    assert.notEqual(first.threadId, independent.threadId);
    assert.equal(store.getAllTasks().length, 2);
    for (const task of store.getAllTasks()) {
      assert.equal(task.requesterAgent, 'vscode_chat');
      assert.equal(task.requesterSessionId, undefined);
      assert.equal(task.reviewFlow, 'dedicated');
      assert.equal(task.reviewBootstrap?.phase, 'pending');
    }
    store.loadFromDisk();
    assert.equal(store.getAllTasks().length, 2, 'submission dedupe and requester metadata survive TaskStore reload');
    assert.equal(executorCalls, 0);
  } finally {
    TaskSupervisor.isSupervisorActive = wasActive;
    store.dispose();
    for (const [key, value] of Object.entries(oldEnv)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; }
    fs.rmSync(root, { recursive: true, force: true });
  }
});
