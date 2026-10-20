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

  test('should handle tools/list and return all 13 specified tools', async () => {
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
    assert.ok(toolNames.includes('delete_task'));
    assert.ok(toolNames.includes('record_merge_request'));
    assert.ok(toolNames.includes('list_recent_tasks'));
    assert.ok(toolNames.includes('get_thread_details'));
    assert.ok(toolNames.includes('invoke_agent'));
    assert.ok(toolNames.includes('query_agent_mesh_status'));
    assert.ok(toolNames.includes('get_recent_logs'));
    assert.ok(toolNames.includes('list_agent_models'));
    assert.strictEqual(toolNames.length, 13);
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
});
