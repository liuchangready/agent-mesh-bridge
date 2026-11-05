import { strict as assert } from 'assert';
import { test } from 'node:test';
import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { GroupAgentRuntime, GroupAgentRuntimeError, GroupTurnResponse } from '../copilot/group-agent-runtime';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { saveGroupAgentConfig, writeGroupAgentHost } from '../bridge/group-agent-registry';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { assertGroupModelReady } from '../bridge/agent-capabilities';

function git(cwd: string, ...args: string[]): string {
  return childProcess.execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

test('GroupAgentRuntime runs structured write and command calls in one real Git worktree and resumes its append-only session', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-agent-'));
    const repo = path.join(root, 'repo');
    const worktree = path.join(root, 'task-worktree');
    const bridgeHome = path.join(root, 'bridge-home');
    fs.mkdirSync(repo);
    git(repo, 'init', '-b', 'main');
    git(repo, 'config', 'user.name', 'Bridge Test');
    git(repo, 'config', 'user.email', 'bridge-test@example.invalid');
    fs.writeFileSync(path.join(repo, 'README.md'), 'base\n');
    git(repo, 'add', 'README.md');
    git(repo, 'commit', '-m', 'base');
    git(repo, 'worktree', 'add', '-b', 'mesh/task/test', worktree, 'HEAD');

    const replies: GroupTurnResponse[] = [
      { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'write-1', name: 'write_file', arguments: { path: 'task.txt', content: 'from group agent\n' } }] },
      { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'command-1', name: 'run_command', arguments: { command: process.platform === 'win32' ? 'Set-Content verified.txt yes' : 'printf yes > verified.txt' } }] },
      { text: 'Implemented and verified the change.', finishReason: 'stop', toolCalls: [] },
    ];
    const runtime = new GroupAgentRuntime({
      bridgeHome,
      request: async (_messages, tools, signal) => {
        assert.equal(signal.aborted, false);
        assert.ok(tools.some((tool) => tool.name === 'run_command'));
        const reply = replies.shift();
        if (!reply) throw new Error('Unexpected model request.');
        return reply;
      },
    });
    try {
      const answer = await runtime.run('session_test_123', worktree, 'Implement the task and run a check.', undefined, 'developer', { agentId: 'copilot_group:11111111-2222-4333-8444-555555555555', threadId: 'thread_owner_test' });
      assert.equal(answer.text, 'Implemented and verified the change.');
      assert.equal(answer.toolCallsCompleted, 2);
      assert.equal(fs.readFileSync(path.join(worktree, 'task.txt'), 'utf8'), 'from group agent\n');
      assert.equal(fs.readFileSync(path.join(worktree, 'verified.txt'), 'utf8').trim(), 'yes');
      assert.equal(fs.existsSync(path.join(repo, 'task.txt')), false);
      assert.equal(git(worktree, 'status', '--porcelain'), '?? task.txt\n?? verified.txt');
      const history = runtime.sessions.read('session_test_123');
      assert.ok(history.some((event) => event.role === 'tool' && event.content.includes('Wrote task.txt')));
      assert.ok(history.some((event) => event.role === 'tool' && event.content.includes('[exit 0]')));
      assert.ok(fs.readFileSync(path.join(bridgeHome, 'group-agent-sessions', 'session_test_123.jsonl'), 'utf8').includes('"role":"intent"'));
      assert.deepEqual(runtime.sessions.owner('session_test_123'), { ownerAgentId: 'copilot_group:11111111-2222-4333-8444-555555555555', threadId: 'thread_owner_test', workspace: worktree, transport: 'bridge-agent' });
    } finally {
      try { git(repo, 'worktree', 'remove', '--force', worktree); } catch {}
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime refuses to replay a tool call whose persisted intent has no result', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-session-'));
    let requests = 0;
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async () => { requests++; return { text: 'unexpected', toolCalls: [], finishReason: 'stop' }; } });
    fs.mkdirSync(path.join(root, 'workspace'));
    runtime.sessions.append('session_unknown_1', { role: 'intent', callId: 'side-effect-1', name: 'run_command', arguments: { command: 'touch file' } });
    try {
      await assert.rejects(runtime.run('session_unknown_1', path.join(root, 'workspace'), 'resume'));
      assert.equal(requests, 0);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime waits for the real command child to close before reporting cancellation', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-cancel-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    const childPids: Array<number | undefined> = [];
    const runtime = new GroupAgentRuntime({
      bridgeHome: root,
      onChildPid: (pid) => childPids.push(pid),
      request: async () => ({
        text: '', finishReason: 'tool_calls', toolCalls: [{
          id: 'long-command', name: 'run_command',
          arguments: { command: process.platform === 'win32' ? 'Start-Sleep -Seconds 30' : 'sleep 30' },
        }],
      }),
    });
    const aborter = new AbortController();
    const running = runtime.run('session_cancel_123', cwd, 'run and cancel', aborter.signal);
    const timer = setTimeout(() => aborter.abort(), 200);
    try {
      await assert.rejects(running, /cancelled/i);
      assert.ok(childPids.some((pid) => typeof pid === 'number'));
      assert.equal(childPids.at(-1), undefined);
    } finally {
      clearTimeout(timer);
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime includes the latest same-session rework instruction after more than 500 events', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-long-session-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    let received: string | undefined;
    const runtime = new GroupAgentRuntime({
      bridgeHome: root,
      request: async (messages) => {
        received = messages.at(-1)?.content;
        return { text: 'rework applied', toolCalls: [], finishReason: 'stop' };
      },
    });
    for (let index = 0; index < 505; index++)
      runtime.sessions.append('session_long_123', { role: 'user', content: `old-${index}` });
    try {
      const result = await runtime.run('session_long_123', cwd, 'LATEST_REWORK');
      assert.equal(result.text, 'rework applied');
      assert.equal(received, 'LATEST_REWORK');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime discards a late model final after pause or delete cancellation', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-late-model-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    let requestStarted!: () => void;
    let finishRequest!: (result: GroupTurnResponse) => void;
    const started = new Promise<void>((resolve) => (requestStarted = resolve));
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async () => {
      requestStarted();
      return new Promise<GroupTurnResponse>((resolve) => (finishRequest = resolve));
    } });
    const aborter = new AbortController();
    const run = runtime.run('session_late_123', cwd, 'do not write after pause', aborter.signal);
    try {
      await started;
      aborter.abort(new Error('task deleted'));
      finishRequest({ text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'late-write', name: 'write_file', arguments: { path: 'late.txt', content: 'must not exist' } }] });
      await assert.rejects(run, /cancelled/i);
      assert.equal(fs.existsSync(path.join(cwd, 'late.txt')), false);
      const history = runtime.sessions.read('session_late_123');
      assert.equal(history.filter((event) => event.role === 'assistant').length, 0);
      assert.equal(fs.readFileSync(path.join(root, 'group-agent-sessions', 'session_late_123.jsonl'), 'utf8').includes('late-write'), false);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime exposes only reviewer or bootstrap control tools and stops after a handoff', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-control-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    const calls: string[] = [];
    const reviewer = new GroupAgentRuntime({ bridgeHome: root, controlTool: async (name, input) => {
      calls.push(`${name}:${String(input.threadId)}`);
      return '{"status":"REWORK_QUEUED"}';
    }, request: async (_messages, tools) => {
      assert.ok(tools.some((tool) => tool.name === 'request_rework'));
      assert.ok(tools.some((tool) => tool.name === 'approve_task'));
      assert.equal(tools.some((tool) => tool.name === 'write_file' || tool.name === 'run_command' || tool.name === 'dispatch_task'), false);
      return { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'review-1', name: 'request_rework', arguments: { threadId: 'thread_control_1', feedback: 'Fix the edge case.' } }] };
    } });
    const bootstrap = new GroupAgentRuntime({ bridgeHome: root, controlTool: async (name, input) => {
      calls.push(`${name}:${String(input.threadId)}:${String(input.token)}`);
      return '{"status":"DISPATCHED"}';
    }, request: async (_messages, tools) => {
      assert.deepEqual(tools.map((tool) => tool.name), ['dispatch_task']);
      return { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'bootstrap-1', name: 'dispatch_task', arguments: { reviewBootstrap: { threadId: 'thread_control_1', token: 'one-time-token' } } }] };
    } });
    try {
      const reviewed = await reviewer.run('session_review_123', cwd, 'Review the developer result.', undefined, 'reviewer');
      const bootstrapped = await bootstrap.run('session_bootstrap_1', cwd, 'Call dispatch_task once with the bootstrap token.', undefined, 'bootstrap');
      assert.match(reviewed.text, /REWORK_QUEUED/);
      assert.match(bootstrapped.text, /DISPATCHED/);
      assert.equal(reviewed.modelRequests, 1);
      assert.equal(bootstrapped.modelRequests, 1);
      assert.deepEqual(calls, ['request_rework:thread_control_1', 'dispatch_task:thread_control_1:one-time-token']);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime rejects hidden and malformed tools against the active role before intent or side effects', async () => {
    const cases: Array<{ role: 'developer' | 'receiver' | 'reviewer' | 'bootstrap'; name: string; args: Record<string, unknown>; pattern: RegExp }> = [
      { role: 'reviewer', name: 'write_file', args: { path: 'reviewer-wrote.txt', content: 'unexpected' }, pattern: /not permitted/ },
      { role: 'reviewer', name: 'run_command', args: { command: 'echo unexpected > reviewer-command.txt' }, pattern: /not permitted/ },
      { role: 'bootstrap', name: 'read_file', args: { path: 'README.md' }, pattern: /not permitted/ },
      { role: 'receiver', name: 'approve_task', args: { threadId: 'thread-x' }, pattern: /not permitted/ },
      { role: 'receiver', name: 'dispatch_task', args: { reviewBootstrap: { threadId: 'thread-x', token: 'secret' } }, pattern: /not permitted/ },
      { role: 'reviewer', name: 'request_rework', args: { threadId: 'thread-x', feedback: 'bad', forwardTo: 'codex' }, pattern: /invalid arguments/i },
    ];
    for (const [index, entry] of cases.entries()) {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-hidden-tool-'));
      const cwd = path.join(root, 'workspace');
      fs.mkdirSync(cwd);
      let controls = 0;
      const runtimeRole = entry.role === 'receiver' ? 'developer' : entry.role;
      const runtime = new GroupAgentRuntime({ bridgeHome: root, controlTool: async () => { controls++; return 'controlled'; }, request: async (_messages, tools) => {
        assert.equal(tools.some((tool) => tool.name === entry.name), runtimeRole === 'reviewer' && ['request_rework', 'approve_task'].includes(entry.name));
        return { text: '', finishReason: 'tool_calls', toolCalls: [{ id: `hidden-${index}`, name: entry.name, arguments: entry.args }] };
      } });
      try {
        await assert.rejects(runtime.run(`session_hidden_${index}`, cwd, 'run', undefined, runtimeRole), entry.pattern);
        assert.equal(controls, 0);
        assert.equal(fs.existsSync(path.join(cwd, 'reviewer-wrote.txt')), false);
        assert.equal(fs.existsSync(path.join(cwd, 'reviewer-command.txt')), false);
        assert.equal(runtime.sessions.read(`session_hidden_${index}`, 0, 20).some((event: any) => event.role === 'intent'), false);
      } finally { fs.rmSync(root, { recursive: true, force: true }); }
    }
});

test('GroupAgentRuntime errors retain usage already returned by earlier model turns', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-error-usage-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async () => ({ text: '', finishReason: 'tool_calls',
      toolCalls: [{ id: 'bad-tool', name: 'hidden_unknown', arguments: {} }],
      usage: { uncachedInputTokens: 8, cacheReadTokens: 2, cacheWriteTokens: 1, promptTokens: 11, outputTokens: 4, totalTokens: 15, nativeTotalTokens: 16 } }) });
    try {
      await assert.rejects(runtime.run('session_usage_failure', cwd, 'do work'), (error: unknown) => {
        assert.ok(error instanceof GroupAgentRuntimeError);
        assert.equal(error.modelRequests, 1);
        assert.equal(error.usageRequests, 1);
        assert.equal(error.usage?.promptTokens, 11);
        assert.equal(error.usage?.nativeTotalTokens, 16);
        return true;
      });
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('Group Agent receiver admission uses the developer role and does not require sender readiness', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-admission-'));
    const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = root;
    const groupUuid = '11111111-2222-4333-8444-555555555555';
    const agent = `copilot_group:${groupUuid}` as const;
    const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
    saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{
      id: agent, groupId: groupUuid, name: 'Group', source: 'vscode-lm', enabled: true,
      toolsAuthorized: true, receiverReady: true, initiatorReady: false, revision: 1, receiverModel: modelRef,
    }] }, root);
    writeGroupAgentHost({ version: 1, hostInstanceId: 'host-one', generation: 1, pid: process.pid,
      workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
    const runner = new AutonomousRunner({} as any, 1000);
    runner.setGroupAgentExecutor(async () => ({ text: 'developer result', toolCallsCompleted: 1,
      tokenUsage: { uncachedInputTokens: 7, cacheReadTokens: 2, cacheWriteTokens: 0, promptTokens: 9,
        outputTokens: 3, totalTokens: 12, agent, model: modelRef.id, source: 'native_reported', available: true, attempts: 1, collectedAt: new Date().toISOString() } }));
    try {
      const result = await runner.execute({ agent, prompt: 'develop', isRework: false, receiverSessionId: 'group-session-1',
        cwd: root, modelRef, role: 'developer', timeoutMs: 500, previousTokenUsage: {
          uncachedInputTokens: 5, cacheReadTokens: 1, cacheWriteTokens: 0, promptTokens: 6, outputTokens: 2, totalTokens: 8,
          agent, model: modelRef.id, source: 'native_reported', available: true, attempts: 1, collectedAt: new Date().toISOString(),
        } });
      assert.equal(result.assessment?.success, true);
      assert.equal(result.tokenUsage?.promptTokens, 15);
      assert.equal(result.tokenUsage?.outputTokens, 5);
      assert.equal(result.tokenUsage?.attempts, 2);
    } finally {
      if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('an approved but unprobed Group member model is still rejected at receiver admission', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-model-admission-'));
  const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
  process.env.AGENT_MESH_BRIDGE_HOME = root;
  const groupUuid = '11111111-2222-4333-8444-555555555556';
  const agent = `copilot_group:${groupUuid}` as const;
  const ready = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'ready-model' };
  const unprobed = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'other-member' };
  saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Group',
    source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false, revision: 1,
      receiverModel: ready, models: [ready, unprobed], modelProbes: [{ role: 'receiver', modelRef: ready, revision: 1, hostGeneration: 1 }] }] }, root);
  writeGroupAgentHost({ version: 1, hostInstanceId: 'host-capability', generation: 1, pid: process.pid,
    workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
  try {
    assertGroupModelReady(agent, 'receiver', ready);
    assert.throws(() => assertGroupModelReady(agent, 'receiver', unprobed), /no current receiver structured-tool probe/);
    assert.throws(() => assertGroupModelReady(agent, 'receiver', { ...ready, vendor: 'another-vendor' }), /no current receiver structured-tool probe/);
  } finally {
    if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('Group Agent runner waits for executor shutdown on timeout and rejects a late final result', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-timeout-'));
    const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = root;
    const groupUuid = '11111111-2222-4333-8444-555555555556';
    const agent = `copilot_group:${groupUuid}` as const;
    const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
    saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Group',
      source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
      revision: 1, receiverModel: modelRef }] }, root);
    writeGroupAgentHost({ version: 1, hostInstanceId: 'host-one', generation: 1, pid: process.pid,
      workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
    const runner = new AutonomousRunner({} as any, 1000);
    let executorStopped = false;
    runner.setGroupAgentExecutor((_options, signal) => new Promise((resolve) => {
      signal.addEventListener('abort', () => setTimeout(() => { executorStopped = true; resolve({ text: 'late final', toolCallsCompleted: 0 }); }, 70), { once: true });
    }));
    try {
      const result = await runner.execute({ agent, prompt: 'develop', isRework: false, receiverSessionId: 'group-session-2',
        cwd: root, modelRef, role: 'developer', timeoutMs: 20 });
      assert.equal(executorStopped, true);
      assert.equal(result.assessment?.success, false);
      assert.equal(result.assessment?.errorCode, 'TIMEOUT');
      assert.equal(result.extractedOutput, '');
    } finally {
      if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('Group Agent runner consumes cross-process pause before accepting an executor result', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-mailbox-cancel-'));
    const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = root;
    const groupUuid = '11111111-2222-4333-8444-555555555557';
    const agent = `copilot_group:${groupUuid}` as const;
    const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
    saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Group',
      source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
      revision: 1, receiverModel: modelRef }] }, root);
    writeGroupAgentHost({ version: 1, hostInstanceId: 'host-one', generation: 1, pid: process.pid,
      workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
    const runner = new AutonomousRunner({} as any, 1000, new CancellationRegistry(root));
    let signal!: AbortSignal;
    let started!: () => void;
    const entered = new Promise<void>((resolve) => started = resolve);
    runner.setGroupAgentExecutor((_options, runSignal) => new Promise((resolve) => {
      signal = runSignal;
      started();
      runSignal.addEventListener('abort', () => setTimeout(() => resolve({ text: 'late result', toolCallsCompleted: 0 }), 50), { once: true });
    }));
    try {
      const execution = runner.execute({ agent, prompt: 'develop', isRework: false, receiverSessionId: 'group-session-3',
        cwd: root, modelRef, role: 'developer', timeoutMs: 1000, taskThreadId: 'group-task-cancel' });
      await entered;
      assert.equal(signal.aborted, false);
      new CancellationRegistry(root).request('group-task-cancel', 'pause', 'paused from another process', 'test');
      const result = await execution;
      assert.equal(signal.aborted, true);
      assert.equal(result.assessment?.success, false);
      assert.equal(result.assessment?.errorCode, 'PAUSED');
      assert.equal(result.extractedOutput, '');
      assert.equal(new CancellationRegistry(root).peek('group-task-cancel'), undefined);
    } finally {
      if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('Group Agent runner rejects pause/delete already present before invoking the executor', async () => {
    for (const action of ['pause', 'delete'] as const) {
      const root = fs.mkdtempSync(path.join(os.tmpdir(), `mesh-group-pre-${action}-`));
      const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
      process.env.AGENT_MESH_BRIDGE_HOME = root;
      const groupUuid = '11111111-2222-4333-8444-555555555558';
      const agent = `copilot_group:${groupUuid}` as const;
      const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
      saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Group',
        source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
        revision: 1, receiverModel: modelRef }] }, root);
      writeGroupAgentHost({ version: 1, hostInstanceId: 'host-one', generation: 1, pid: process.pid,
        workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
      const registry = new CancellationRegistry(root);
      registry.request(`task_precancel_${action}`, action, `${action} before run`, 'test');
      const runner = new AutonomousRunner({} as any, 1000, registry);
      let executorCalls = 0;
      runner.setGroupAgentExecutor(async () => { executorCalls++; return { text: 'done', toolCallsCompleted: 0 }; });
      try {
        const result = await runner.execute({ agent, prompt: 'develop', isRework: false, receiverSessionId: `group-pre-${action}`,
          cwd: root, modelRef, role: 'developer', timeoutMs: 500, taskThreadId: `task_precancel_${action}` });
        assert.equal(executorCalls, 0);
        assert.equal(result.assessment?.success, false);
        assert.equal(result.interruption?.kind, action === 'pause' ? 'paused' : 'deleted');
        assert.equal(result.assessment?.errorCode, action === 'pause' ? 'PAUSED' : 'DELETED');
      } finally {
        if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
        else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
        fs.rmSync(root, { recursive: true, force: true });
      }
    }
});

test('Group Agent runner adds native usage from a failed execution to the previous round spend', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-resume-usage-'));
    const oldHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = root;
    const groupUuid = '11111111-2222-4333-8444-555555555559';
    const agent = `copilot_group:${groupUuid}` as const;
    const modelRef = { source: 'vscode-lm' as const, vendor: 'vendor-a', id: 'model-a' };
    saveGroupAgentConfig({ version: 1, updatedAt: Date.now(), agents: [{ id: agent, groupId: groupUuid, name: 'Group',
      source: 'vscode-lm', enabled: true, toolsAuthorized: true, receiverReady: true, initiatorReady: false,
      revision: 1, receiverModel: modelRef }] }, root);
    writeGroupAgentHost({ version: 1, hostInstanceId: 'host-one', generation: 1, pid: process.pid,
      workspaceUri: 'file:///workspace', repositoryCommonDir: root, trusted: true, localDesktop: true, updatedAt: Date.now() }, root);
    const runner = new AutonomousRunner({} as any, 1000);
    runner.setGroupAgentExecutor(async () => {
      throw Object.assign(new Error('tool failed after model reply'), { modelRequests: 2, tokenUsage: {
        uncachedInputTokens: 8, cacheReadTokens: 3, cacheWriteTokens: 0, promptTokens: 11, outputTokens: 4, totalTokens: 15,
        agent, model: modelRef.id, source: 'native_reported' as const, available: true, partial: true, attempts: 2, collectedAt: new Date().toISOString(),
      } });
    });
    const previousTokenUsage = { uncachedInputTokens: 5, cacheReadTokens: 2, cacheWriteTokens: 0, promptTokens: 7, outputTokens: 3, totalTokens: 10,
      agent, model: modelRef.id, source: 'native_reported' as const, available: true, partial: false, attempts: 1, collectedAt: new Date().toISOString() };
    try {
      const result = await runner.execute({ agent, prompt: 'resume', isRework: true, receiverSessionId: 'group-session-usage',
        cwd: root, modelRef, role: 'developer', timeoutMs: 500, previousTokenUsage });
      assert.equal(result.assessment?.success, false);
      assert.equal(result.tokenUsage?.promptTokens, 18);
      assert.equal(result.tokenUsage?.outputTokens, 7);
      assert.equal(result.tokenUsage?.attempts, 3);
      assert.equal(result.tokenUsage?.partial, true);
    } finally {
      if (oldHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = oldHome;
      fs.rmSync(root, { recursive: true, force: true });
    }
});
