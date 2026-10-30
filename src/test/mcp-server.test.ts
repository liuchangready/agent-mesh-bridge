import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-mcp-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';
import { MCPServer } from '../bridge/mcp-server';
import { ExecutionGate } from '../bridge/execution-gate';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { roundOutputArchivePath } from '../bridge/round-log';
import { DelegationRound } from '../bridge/types';

describe('MCPServer Protocol Tests', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let detector: AgentDetector;
  let resolver: ActiveAgentResolver;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let dispatcher: AsyncDispatcher;
  let reworkController: ReworkController;
  let mcpServer: MCPServer;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-mcp-test-'));
    taskStore = new TaskStore(tempDir);
    detector = new AgentDetector();
    resolver = new ActiveAgentResolver(detector);
    runner = new AutonomousRunner(detector);
    // Never spawn a real agent from a protocol test.
    runner.execute = async () => ({
      exitCode: 0,
      stdout: 'ok',
      stderr: '',
      extractedOutput: 'ok',
      durationMs: 1,
    });
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    // Never spawn a real caller CLI from a protocol test.
    callbackManager.notifyCallerOfFailure = async () => false;
    const gate = new ExecutionGate(tempDir);
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, gate);
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
    mcpServer = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner);
  });

  const waitForStatus = async (threadId: string, status: string, timeoutMs = 3000) => {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (taskStore.getTask(threadId)?.status === status) return;
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    throw new Error(
      `Timed out waiting for ${threadId} to reach ${status}; current=${
        taskStore.getTask(threadId)?.status
      }`,
    );
  };

  afterEach(() => {
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('should handle initialize request with proper capabilities', async () => {
    const initReq = {
      jsonrpc: '2.0',
      id: 1,
      method: 'initialize',
      params: { clientInfo: { name: 'test-client', version: '1.0' } },
    };

    const res = await mcpServer.handleRpcMessage(initReq);
    assert.strictEqual(res.jsonrpc, '2.0');
    assert.strictEqual(res.id, 1);
    assert.strictEqual(res.result.serverInfo.name, 'agent-mesh-bridge');
    assert.ok(res.result.capabilities.tools);
  });

  test('should handle tools/list and return all 18 specified tools', async () => {
    const listReq = {
      jsonrpc: '2.0',
      id: 2,
      method: 'tools/list',
      params: {},
    };

    const res = await mcpServer.handleRpcMessage(listReq);
    assert.strictEqual(res.id, 2);
    const tools = res.result.tools;
    assert.ok(Array.isArray(tools));
    const toolNames = tools.map((t: any) => t.name);

    assert.ok(toolNames.includes('dispatch_task'));
    assert.ok(toolNames.includes('request_rework'));
    assert.ok(toolNames.includes('approve_task'));
    assert.ok(toolNames.includes('pause_task'));
    assert.ok(toolNames.includes('resume_task'));
    assert.ok(toolNames.includes('retry_task'));
    assert.ok(toolNames.includes('retarget_task'));
    assert.ok(toolNames.includes('allow_scope_overlap'));
    assert.ok(toolNames.includes('delete_task'));
    assert.ok(toolNames.includes('record_merge_request'));
    assert.ok(toolNames.includes('list_recent_tasks'));
    assert.ok(toolNames.includes('get_thread_details'));
    assert.ok(toolNames.includes('invoke_agent'));
    assert.ok(toolNames.includes('query_agent_mesh_status'));
    assert.ok(toolNames.includes('get_recent_logs'));
    assert.ok(toolNames.includes('get_round_raw_log'));
    assert.ok(toolNames.includes('get_task_token_usage'));
    assert.ok(toolNames.includes('get_reviewer_log'));
    assert.ok(toolNames.includes('list_agent_models'));
    assert.strictEqual(toolNames.length, 19);
  });

  test('serves the reviewer side of a round and reports a missing log honestly', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 61,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => JSON.parse(res.result.content[0].text);

    const dispatched = payload(
      await call('dispatch_task', {
        targetAgent: 'claude_code',
        title: 'Reviewer output',
        prompt: 'do the work',
        allowNoScope: true,
      }),
    );
    await waitForStatus(dispatched.threadId, 'waiting_review');

    // No wake has been captured for this task yet.
    const missing = payload(await call('get_reviewer_log', { threadId: dispatched.threadId }));
    assert.strictEqual(missing.available, false);
    assert.deepStrictEqual(missing.roundsWithReviewerLog, []);
    assert.match(missing.message, /No reviewer log/);

    // A captured reviewer stream must be served with the same paging contract.
    const logsDir = path.join(tempDir, 'logs');
    fs.mkdirSync(logsDir, { recursive: true });
    fs.writeFileSync(
      path.join(logsDir, `${dispatched.threadId}_round_1.reviewer.log`),
      '--- wake ---\n[reviewer] EditMode 82/82\n[reviewer] verdict pending\n',
    );

    const page = payload(
      await call('get_reviewer_log', { threadId: dispatched.threadId, round: 1 }),
    );
    assert.strictEqual(page.available, true);
    assert.match(page.content, /EditMode 82\/82/);
    assert.match(page.content, /verdict pending/);
    assert.strictEqual(page.reviewerAgent, taskStore.getTask(dispatched.threadId)!.initiatorAgent);
    assert.deepStrictEqual(page.roundsWithReviewerLog, [1]);

    // Bounded reads keep the tail and flag the truncation.
    const tail = payload(
      await call('get_reviewer_log', {
        threadId: dispatched.threadId,
        round: 1,
        maxBytes: 20,
      }),
    );
    assert.strictEqual(tail.truncated, true);
    assert.match(tail.hint, /offsetBytes=/);
  });

  test('tells the caller to stop and wait, and starves repeated polls', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 60,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => JSON.parse(res.result.content[0].text);

    const dispatched = payload(
      await call('dispatch_task', {
        targetAgent: 'codex',
        title: 'Stop polling',
        prompt: 'do the work',
        allowNoScope: true,
      }),
    );
    // The dispatch reply itself must carry the directive, before anything else.
    assert.strictEqual(dispatched.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
    assert.match(dispatched.message, /^nextAction=END_TURN_AND_WAIT_FOR_CALLBACK/);
    assert.match(dispatched.stopInstruction, /End your turn immediately/);

    await waitForStatus(dispatched.threadId, 'waiting_review');

    const first = payload(await call('get_thread_details', { threadId: dispatched.threadId }));
    assert.strictEqual(first.repeatPoll, undefined);
    assert.ok(first.rounds, 'the first read is a real payload');

    const second = payload(await call('get_thread_details', { threadId: dispatched.threadId }));
    assert.strictEqual(second.repeatPoll, true);
    assert.strictEqual(second.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
    assert.match(second.stopInstruction, /STOP POLLING/);
    assert.strictEqual(second.rounds, undefined, 'a repeated poll is starved of details');

    // Log watching is the same loop wearing a different hat: the first read is
    // still served (one inspection = details + logs), the second is starved.
    assert.strictEqual(
      payload(await call('get_recent_logs', { threadId: dispatched.threadId })).repeatPoll,
      undefined,
    );
    assert.strictEqual(
      payload(await call('get_recent_logs', { threadId: dispatched.threadId })).repeatPoll,
      true,
    );

    // Real progress unblocks the full payload: the callback/resume path works.
    const stored = taskStore.getTask(dispatched.threadId)!;
    stored.currentRound = 2;
    taskStore.upsertTask(stored);
    const afterProgress = payload(await call('get_thread_details', { threadId: dispatched.threadId }));
    assert.strictEqual(afterProgress.repeatPoll, undefined);
    assert.strictEqual(afterProgress.threadId, dispatched.threadId);
  });

  test('get_thread_details condenses older rounds and get_round_raw_log pages the archive', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 61,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => JSON.parse(res.result.content[0].text);

    const threadId = 'thread_capsule_projection';
    const longOutput = 'A'.repeat(9_000);
    const now = new Date().toISOString();
    const round = (
      roundNumber: number,
      type: 'initial' | 'rework',
      output: string,
    ): DelegationRound => ({
      round: roundNumber,
      type,
      prompt: `prompt ${roundNumber}`,
      output,
      durationMs: 1,
      status: 'completed',
      timestamp: now,
    });

    taskStore.upsertTask({
      threadId,
      title: 'capsule projection',
      initiatorAgent: 'codex',
      targetAgent: 'claude_code',
      status: 'waiting_review',
      currentRound: 3,
      createdAt: now,
      updatedAt: now,
      receiverSessionId: 'session-abcdef12',
      workspacePath: tempDir,
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 3 },
      rounds: [
        round(1, 'initial', longOutput),
        round(2, 'rework', longOutput),
        round(3, 'rework', 'latest output'),
      ],
    });

    // Default view: latest verbatim, older oversized rounds excerpted, no logs.
    const details = payload(await call('get_thread_details', { threadId }));
    assert.strictEqual(details.rounds[2].output, 'latest output');
    assert.strictEqual(details.rounds[0].outputTruncated, true);
    assert.ok(details.rounds[0].output.length < longOutput.length);
    assert.match(details.rounds[0].recallHint, /get_round_raw_log/);
    assert.strictEqual(details.recentLogs, undefined, 'log dump is opt-in');
    assert.ok(details.fullHistoryHint);

    // Opting in still returns the unbounded history.
    const full = payload(await call('get_thread_details', { threadId, includeRawLogs: true }));
    assert.strictEqual(full.rounds[0].output.length, longOutput.length);
    assert.ok(typeof full.recentLogs === 'string');

    // Paged raw log: the archive is served in bounded windows.
    const archive = roundOutputArchivePath(threadId, 1, tempDir);
    fs.mkdirSync(path.dirname(archive), { recursive: true });
    fs.writeFileSync(archive, longOutput, 'utf8');

    const first = payload(
      await call('get_round_raw_log', { threadId, round: 1, maxBytes: 100 }),
    );
    assert.strictEqual(first.available, true);
    assert.strictEqual(first.content.length, 100);
    assert.strictEqual(first.truncated, true);
    assert.strictEqual(first.totalBytes, 9_000);

    const second = payload(
      await call('get_round_raw_log', {
        threadId,
        round: 1,
        offsetBytes: first.nextOffset,
        maxBytes: 100,
      }),
    );
    assert.strictEqual(second.startOffset, 100);
    assert.strictEqual(second.content.length, 100);

    // `maxBytes` is clamped, never trusted as-is.
    const clamped = payload(
      await call('get_round_raw_log', { threadId, round: 1, maxBytes: 10_000_000 }),
    );
    assert.ok(clamped.content.length <= 200_000);
  });

  test('get_task_token_usage reports task rollup and single-round detail', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 62,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => JSON.parse(res.result.content[0].text);

    const threadId = 'thread_token_usage';
    const now = new Date().toISOString();
    const usage = (
      total: number,
      prompt: number,
      output: number,
    ): import('../bridge/types').TokenUsageMetrics => ({
      uncachedInputTokens: prompt - 10,
      cacheReadTokens: 10,
      cacheWriteTokens: 0,
      promptTokens: prompt,
      outputTokens: output,
      totalTokens: total,
      agent: 'codex',
      model: 'gpt-6-sol',
      source: 'native_reported',
      available: true,
      attempts: 1,
      collectedAt: now,
    });

    taskStore.upsertTask({
      threadId,
      title: 'token usage',
      initiatorAgent: 'claude_code',
      targetAgent: 'codex',
      status: 'waiting_review',
      currentRound: 2,
      createdAt: now,
      updatedAt: now,
      receiverSessionId: 'session-token-01',
      workspacePath: tempDir,
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 2 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'p1',
          output: 'o1',
          durationMs: 1,
          status: 'completed',
          timestamp: now,
          executorAgent: 'codex',
          executorModel: 'gpt-6-sol',
          tokenUsage: usage(1_200, 1_000, 200),
        },
        {
          round: 2,
          type: 'rework',
          prompt: 'p2',
          output: 'o2',
          durationMs: 1,
          status: 'completed',
          timestamp: now,
          executorAgent: 'codex',
          executorModel: 'gpt-6-sol',
          tokenUsage: usage(800, 600, 200),
        },
      ],
    });

    const whole = payload(await call('get_task_token_usage', { threadId }));
    assert.strictEqual(whole.tokenUsage.totalTokens, 2_000);
    assert.strictEqual(whole.tokenUsage.totalPromptTokens, 1_600);
    assert.strictEqual(whole.tokenUsage.totalOutputTokens, 400);
    assert.strictEqual(whole.tokenUsage.measuredRounds, 2);
    assert.strictEqual(whole.tokenUsage.byAgent.codex, 2_000);
    assert.strictEqual(whole.rounds.length, 2);
    assert.strictEqual(whole.rounds[0].tokenUsage.totalTokens, 1_200);

    const single = payload(await call('get_task_token_usage', { threadId, round: 1 }));
    assert.strictEqual(single.round, 1);
    assert.strictEqual(single.tokenUsage.totalTokens, 1_200);
    assert.strictEqual(single.taskTotal.totalTokens, 2_000);

    // The details payload also carries the derived rollup.
    const details = payload(await call('get_thread_details', { threadId }));
    assert.strictEqual(details.tokenUsage.totalTokens, 2_000);
  });

  test('should execute dispatch_task, get_thread_details, request_rework, and approve_task via tools/call', async () => {
    // 1. Dispatch
    const dispatchCall = {
      jsonrpc: '2.0',
      id: 10,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: {
          targetAgent: 'codex',
          title: 'Refactor Cache Layer',
          prompt: 'Use LRU cache with 500 items max',
          expectedFiles: ['src/cache/lru.ts'],
          callerSessionId: 'caller_mcp_1',
        },
      },
    };

    const dispatchRes = await mcpServer.handleRpcMessage(dispatchCall);
    assert.strictEqual(dispatchRes.id, 10);
    const dispatchData = JSON.parse(dispatchRes.result.content[0].text);
    assert.strictEqual(dispatchData.status, 'DISPATCHED');
    const threadId = dispatchData.threadId;
    const receiverSessionId = dispatchData.receiverSessionId;
    assert.ok(threadId);

    // 2. Get Thread Details
    const detailsCall = {
      jsonrpc: '2.0',
      id: 11,
      method: 'tools/call',
      params: {
        name: 'get_thread_details',
        arguments: { threadId },
      },
    };
    const detailsRes = await mcpServer.handleRpcMessage(detailsCall);
    assert.strictEqual(detailsRes.id, 11);
    const detailsData = JSON.parse(detailsRes.result.content[0].text);
    assert.strictEqual(detailsData.threadId, threadId);
    assert.strictEqual(detailsData.currentRound, 1);

    // 3. Request Rework.
    // Rounds are strictly sequential, so the receiver's round 1 must reach a
    // terminal state before a new round may be started.
    await waitForStatus(threadId, 'waiting_review');
    const reviewer1 = new TaskRoleLock().tryAcquire(threadId, 'reviewer');
    assert.ok(reviewer1, 'reviewer role must be available after the developer handoff');

    const reworkCall = {
      jsonrpc: '2.0',
      id: 12,
      method: 'tools/call',
      params: {
        name: 'request_rework',
        arguments: {
          threadId,
          feedback: 'Please make TTL expiration configurable',
        },
      },
    };
    const reworkRes = await mcpServer.handleRpcMessage(reworkCall);
    assert.strictEqual(reworkRes.id, 12);
    const reworkData = JSON.parse(reworkRes.result.content[0].text);
    assert.strictEqual(reworkData.status, 'REWORK_DISPATCHED');
    assert.strictEqual(reworkData.round, 2);
    assert.strictEqual(reworkData.receiverSessionId, receiverSessionId);

    // 4. Approve Task.
    // The rework round started above must reach a terminal state first.
    await waitForStatus(threadId, 'waiting_review');
    const reviewer2 = new TaskRoleLock().tryAcquire(threadId, 'reviewer');
    assert.ok(reviewer2, 'reviewer role must be available after the rework handoff');

    const approveCall = {
      jsonrpc: '2.0',
      id: 13,
      method: 'tools/call',
      params: {
        name: 'approve_task',
        arguments: {
          threadId,
          remarks: 'TTL configurable cache verified',
        },
      },
    };
    const approveRes = await mcpServer.handleRpcMessage(approveCall);
    assert.strictEqual(approveRes.id, 13);
    const approveData = JSON.parse(approveRes.result.content[0].text);
    assert.strictEqual(approveData.status, 'COMPLETED');

    // 5. Record the caller's MR handoff.
    const mrCall = {
      jsonrpc: '2.0',
      id: 14,
      method: 'tools/call',
      params: {
        name: 'record_merge_request',
        arguments: {
          threadId,
          url: 'https://example.com/mr/42',
          branch: 'feat/d07',
          commit: 'abcdef123456',
          remote: 'origin',
          status: 'synced',
          summary: 'D-07 MR',
        },
      },
    };
    const mrRes = await mcpServer.handleRpcMessage(mrCall);
    assert.strictEqual(mrRes.id, 14);
    const mrData = JSON.parse(mrRes.result.content[0].text);
    assert.strictEqual(mrData.status, 'MR_RECORDED');
    assert.strictEqual(mrData.mergeRequest.url, 'https://example.com/mr/42');
    const storedMr = taskStore.getTask(threadId)?.mergeRequest;
    assert.strictEqual(storedMr?.url, 'https://example.com/mr/42');
    assert.strictEqual(storedMr?.branch, 'feat/d07');
    assert.strictEqual(storedMr?.commit, 'abcdef123456');

    // 6. List Tasks
    const listCall = {
      jsonrpc: '2.0',
      id: 15,
      method: 'tools/call',
      params: {
        name: 'list_recent_tasks',
        arguments: { limit: 10 },
      },
    };
    const listRes = await mcpServer.handleRpcMessage(listCall);
    assert.strictEqual(listRes.id, 15);
    const listData = JSON.parse(listRes.result.content[0].text);
    assert.ok(Array.isArray(listData));
    assert.strictEqual(listData.length, 1);
    assert.strictEqual(listData[0].threadId, threadId);

    // 6. Get Recent Logs
    const logsCall = {
      jsonrpc: '2.0',
      id: 15,
      method: 'tools/call',
      params: {
        name: 'get_recent_logs',
        arguments: { threadId, maxLines: 1000 },
      },
    };
    const logsRes = await mcpServer.handleRpcMessage(logsCall);
    assert.strictEqual(logsRes.id, 15);
    const logsData = JSON.parse(logsRes.result.content[0].text);
    assert.strictEqual(logsData.threadId, threadId);
    assert.ok(typeof logsData.totalLines === 'number');
    assert.ok(typeof logsData.recentLogs === 'string');
  });

  test('should hide forwarding tools and reject them while acting as a task receiver', async () => {
    const savedReceiver = process.env.MESH_TASK_RECEIVER;
    const savedThread = process.env.MESH_TASK_THREAD_ID;
    try {
      process.env.MESH_TASK_RECEIVER = '1';
      process.env.MESH_TASK_THREAD_ID = 'thread_receiver_1';

      // The model should not even see new-work tools.
      const toolNames = mcpServer.getToolDefinitions().map((tool: any) => tool.name);
      assert.ok(!toolNames.includes('dispatch_task'));
      assert.ok(!toolNames.includes('invoke_agent'));
      assert.ok(toolNames.includes('request_rework'));
      assert.ok(toolNames.includes('approve_task'));

      // And direct JSON-RPC calls are rejected even if the client cached them.
      const dispatchRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 30,
        method: 'tools/call',
        params: {
          name: 'dispatch_task',
          arguments: { targetAgent: 'codex', title: 'nested', prompt: 'do more work' },
        },
      });
      assert.match(dispatchRes.error.message, /receiver/i);
      assert.match(dispatchRes.error.message, /forward/i);

      const invokeRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 31,
        method: 'tools/call',
        params: {
          name: 'invoke_agent',
          arguments: { targetAgent: 'codex', prompt: 'nested query' },
        },
      });
      assert.match(invokeRes.error.message, /receiver/i);

      // A receiver must not drive the review loop of the task it is executing.
      const reworkRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 32,
        method: 'tools/call',
        params: {
          name: 'request_rework',
          arguments: { threadId: 'thread_receiver_1', feedback: 'again' },
        },
      });
      assert.match(reworkRes.error.message, /receiver/i);

      // Nor pause/delete the task it is currently executing.
      const pauseRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 33,
        method: 'tools/call',
        params: {
          name: 'pause_task',
          arguments: { threadId: 'thread_receiver_1', reason: 'self pause' },
        },
      });
      assert.match(pauseRes.error.message, /receiver/i);

      const deleteRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 34,
        method: 'tools/call',
        params: {
          name: 'delete_task',
          arguments: { threadId: 'thread_receiver_1', reason: 'self delete' },
        },
      });
      assert.match(deleteRes.error.message, /receiver/i);

      // Nor move its own task to another agent/model.
      const retargetRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 36,
        method: 'tools/call',
        params: {
          name: 'retarget_task',
          arguments: { threadId: 'thread_receiver_1', targetAgent: 'codex' },
        },
      });
      assert.match(retargetRes.error.message, /receiver/i);

      const mrRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 35,
        method: 'tools/call',
        params: {
          name: 'record_merge_request',
          arguments: {
            threadId: 'thread_receiver_1',
            url: 'https://example.com/mr/1',
          },
        },
      });
      assert.match(mrRes.error.message, /receiver/i);
    } finally {
      if (savedReceiver === undefined) delete process.env.MESH_TASK_RECEIVER;
      else process.env.MESH_TASK_RECEIVER = savedReceiver;
      if (savedThread === undefined) delete process.env.MESH_TASK_THREAD_ID;
      else process.env.MESH_TASK_THREAD_ID = savedThread;
    }
  });

  test('should execute query_agent_mesh_status and return mesh info', async () => {
    const statusCall = {
      jsonrpc: '2.0',
      id: 20,
      method: 'tools/call',
      params: {
        name: 'query_agent_mesh_status',
        arguments: {},
      },
    };

    const res = await mcpServer.handleRpcMessage(statusCall);
    assert.strictEqual(res.id, 20);
    const data = JSON.parse(res.result.content[0].text);
    assert.ok(data.activeAgent);
    assert.ok(data.agents.antigravity);
    assert.ok(data.agents.claude_code);
    assert.ok(data.agents.codex);
  });

  test('retry_task re-runs the failed round in place instead of creating a duplicate round', async () => {
    let shouldFail = true;
    runner.execute = async () =>
      shouldFail
        ? {
            exitCode: 1,
            stdout: '',
            stderr: 'dsh: TRANSPORT: Connection error.',
            extractedOutput: '',
            durationMs: 5,
            error: 'dsh: TRANSPORT: Connection error.',
          }
        : {
            exitCode: 0,
            stdout: 'ok',
            stderr: '',
            extractedOutput: 'ok',
            durationMs: 5,
            actualSessionId: 'session-retry-0001',
          };

    const dispatchRes = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 40,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: {
          targetAgent: 'codex',
          title: 'Retry in place',
          prompt: 'Implement the extension exactly as designed',
          expectedFiles: ['src/extension/sleeve.ts'],
          callerSessionId: 'caller_mcp_retry',
        },
      },
    });
    const threadId = JSON.parse(dispatchRes.result.content[0].text).threadId;
    await waitForStatus(threadId, 'failed', 5000);

    const before = taskStore.getTask(threadId)!;
    const originalPrompt = before.rounds[0].prompt;
    assert.strictEqual(before.rounds.length, 1);

    shouldFail = false;
    const retryRes = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 41,
      method: 'tools/call',
      params: { name: 'retry_task', arguments: { threadId } },
    });
    const retryData = JSON.parse(retryRes.result.content[0].text);
    assert.strictEqual(retryData.status, 'RETRYING');
    assert.strictEqual(retryData.round, 1, 'retry must reuse the failed round number');

    await waitForStatus(threadId, 'waiting_review', 5000);
    const after = taskStore.getTask(threadId)!;
    assert.strictEqual(after.rounds.length, 1, 'retry must not append a second round');
    assert.strictEqual(after.rounds[0].prompt, originalPrompt);
  });

  test('request_rework flags a round whose instruction repeats an earlier one', async () => {
    const prompt = 'Implement the sleeve bloom extension as designed';
    const dispatchRes = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 50,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: {
          targetAgent: 'codex',
          title: 'Repeat instruction',
          prompt,
          expectedFiles: ['src/extension/repeat.ts'],
          callerSessionId: 'caller_mcp_repeat',
        },
      },
    });
    const threadId = JSON.parse(dispatchRes.result.content[0].text).threadId;
    await waitForStatus(threadId, 'waiting_review');

    new TaskRoleLock().tryAcquire(threadId, 'reviewer');
    const reworkRes = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 51,
      method: 'tools/call',
      params: { name: 'request_rework', arguments: { threadId, feedback: prompt } },
    });
    assert.ok(reworkRes.result);

    const thread = taskStore.getTask(threadId)!;
    const reworkRound = thread.rounds.find((r) => r.round === 2);
    assert.ok(reworkRound);
    assert.strictEqual(
      reworkRound!.repeatOfRound,
      1,
      'a rework that re-sends the original prompt must be marked as repeating round 1',
    );
  });

  test('requires a declared scope unless the caller explicitly opts out', async () => {
    const noScope = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 60,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: { targetAgent: 'codex', title: 'No scope', prompt: 'do work' },
      },
    });
    assert.ok(noScope.error, 'a dispatch without expectedFiles must be rejected');
    assert.match(noScope.error.message, /expectedFiles/);

    const optedOut = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 61,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: {
          targetAgent: 'codex',
          title: 'Docs only',
          prompt: 'answer a question',
          allowNoScope: true,
        },
      },
    });
    assert.ok(optedOut.result, 'allowNoScope must be accepted');
    assert.strictEqual(JSON.parse(optedOut.result.content[0].text).status, 'DISPATCHED');
  });

  test('blocks a task whose declared files another open task already owns', async () => {
    // A task that was accepted but never收口 (no merge request) still owns its
    // files: the next task would overwrite the uncommitted work.
    const now = new Date().toISOString();
    // The gate only lets an EARLIER task hold files, and equal timestamps are
    // broken by threadId, which would make this assertion depend on the id the
    // dispatcher happens to mint. Seed the owner in the past so ordering is
    // deterministic.
    const earlier = new Date(Date.now() - 60_000).toISOString();
    taskStore.upsertTask({
      threadId: 'thread_seeded_owner',
      title: 'Earlier overlapping task',
      initiatorAgent: 'codex',
      targetAgent: 'deepseek_harness',
      status: 'completed',
      currentRound: 1,
      createdAt: earlier,
      updatedAt: now,
      receiverSessionId: 'session-seeded-owner',
      callerSessionId: 'caller-seeded',
      workspacePath: process.cwd(),
      expectedFiles: ['engine/src/shared/TableAuthority.kt'],
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'touch TableAuthority',
          status: 'completed',
          durationMs: 1,
          timestamp: now,
        },
      ],
    });

    const blocked = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 70,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: {
          targetAgent: 'codex',
          title: 'Overlapping follow-up',
          prompt: 'also touch TableAuthority',
          expectedFiles: ['engine/src/shared/TableAuthority.kt'],
        },
      },
    });
    const data = JSON.parse(blocked.result.content[0].text);
    assert.strictEqual(data.status, 'QUEUED');
    assert.match(data.message, /scope/i);
    assert.match(data.message, /thread_seeded_owner/);

    const thread = taskStore.getTask(data.threadId)!;
    assert.deepStrictEqual(thread.scopeBlockedBy?.files, ['engine/src/shared/TableAuthority.kt']);

    const override = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 71,
      method: 'tools/call',
      params: { name: 'allow_scope_overlap', arguments: { threadId: data.threadId, reason: 'reviewed' } },
    });
    assert.strictEqual(JSON.parse(override.result.content[0].text).status, 'SCOPE_OVERRIDE');
    const after = taskStore.getTask(data.threadId)!;
    assert.strictEqual(after.scopeOverride, true);
    assert.strictEqual(after.scopeBlockedBy, undefined);
  });
});
