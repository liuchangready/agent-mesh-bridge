import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';
import { MCPServer } from '../bridge/mcp-server';

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
    callbackManager = new CallbackManager(detector);
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager);
    reworkController = new ReworkController(taskStore, runner, callbackManager);
    mcpServer = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner);
  });

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

  test('should handle tools/list and return all 7 specified tools', async () => {
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
    assert.ok(toolNames.includes('list_recent_tasks'));
    assert.ok(toolNames.includes('get_thread_details'));
    assert.ok(toolNames.includes('invoke_agent'));
    assert.ok(toolNames.includes('query_agent_mesh_status'));
    assert.strictEqual(toolNames.length, 7);
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

    // 3. Request Rework
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

    // 4. Approve Task
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

    // 5. List Tasks
    const listCall = {
      jsonrpc: '2.0',
      id: 14,
      method: 'tools/call',
      params: {
        name: 'list_recent_tasks',
        arguments: { limit: 10 },
      },
    };
    const listRes = await mcpServer.handleRpcMessage(listCall);
    assert.strictEqual(listRes.id, 14);
    const listData = JSON.parse(listRes.result.content[0].text);
    assert.ok(Array.isArray(listData));
    assert.strictEqual(listData.length, 1);
    assert.strictEqual(listData[0].threadId, threadId);
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
