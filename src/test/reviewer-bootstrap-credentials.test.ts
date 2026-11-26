import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-bootstrap-cred-home-'));
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
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { getBridgeHome } from '../bridge/model-catalog';
import { makeBootstrapExecution } from './unified-execution-fixture';
import {
  clearReviewerBootstrapToken,
  confirmReviewerBootstrapTask,
  readReviewerBootstrapToken,
  writeReviewerBootstrapToken,
} from '../bridge/reviewer-bootstrap';

/**
 * Which channel identifies the dedicated reviewer.
 *
 * A real run failed with "the reviewer turn ended without calling dispatch_task"
 * although the reviewer DID call dispatch_task with `{}`: its host stripped
 * MESH_REVIEW_* and its bridge server's parent pid matched no launch record.
 * These tests pin the two channels that survive such a host.
 */
describe('dedicated reviewer bootstrap identity', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let mcpServer: MCPServer;

  const saved = ['MESH_REVIEW_BOOTSTRAP', 'MESH_REVIEW_BOOTSTRAP_TOKEN', 'MESH_REVIEW_THREAD_ID', 'MESH_TASK_RECEIVER', 'MESH_TASK_THREAD_ID', 'MESH_TASK_ROUND'];

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-bootstrap-cred-'));
    taskStore = new TaskStore(tempDir);
    const detector = new AgentDetector();
    const resolver = new ActiveAgentResolver(detector);
    const runner = new AutonomousRunner(detector);
    runner.execute = async () => ({
      exitCode: 0,
      stdout: 'ok',
      stderr: '',
      extractedOutput: 'ok',
      durationMs: 1,
    });
    const callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    const gate = new TaskExecutionRegistry(tempDir);
    const dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, gate);
    const reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
    mcpServer = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner, true);

    // No inherited bootstrap environment: exactly what a closed-env host does.
    for (const name of saved) delete process.env[name];
    process.env.MESH_CALLER_AGENT = 'codex';
  });

  function prepareTask(threadId: string, token: string, historical = false): void {
    taskStore.upsertTask({
      threadId,
      title: 'Prepared task behind a closed-env host',
      initiatorAgent: 'codex',
      targetAgent: 'deepseek_harness',
      status: 'queued',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: `receiver-${threadId}`,
      workspace: testWorkspace(tempDir),
      reviewFlow: 'dedicated',
      reviewerModel: 'gpt-6-pro',
      targetModel: 'flash',
      reviewBootstrap: { phase: 'pending', attempts: 0 },
      execution: historical ? undefined : makeBootstrapExecution(threadId),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [],
    });
    // Deliberately no hostPid: the pid channel can never match this record.
    assert.strictEqual(
      writeReviewerBootstrapToken(
        { threadId, token, agent: 'codex', createdAt: new Date().toISOString() },
        getBridgeHome(),
      ),
      true,
    );
  }

  async function callDispatch(args: any) {
    return mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 1,
      method: 'tools/call',
      params: { name: 'dispatch_task', arguments: args },
    });
  }

  test('the prompt credential confirms the prepared task', async () => {
    const threadId = 'thread_prompt_credential';
    prepareTask(threadId, 'token-from-prompt');
    try {
      const res = await callDispatch({ reviewBootstrap: { threadId, token: 'token-from-prompt' } });
      assert.ok(res.result, JSON.stringify(res));
      const payload = JSON.parse(res.result.content[0].text);
      assert.strictEqual(payload.status, 'DISPATCHED');
      assert.strictEqual(payload.threadId, threadId);
      assert.strictEqual(taskStore.getAllTasks().length, 1, 'no second task was created');
      assert.strictEqual(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'confirmed');
      assert.strictEqual(readReviewerBootstrapToken(threadId, getBridgeHome()), undefined);
    } finally {
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('a brief-less call claims the only pending bootstrap of its agent', async () => {
    const threadId = 'thread_sole_pending';
    prepareTask(threadId, 'sole-token');
    try {
      const res = await callDispatch({});
      assert.ok(res.result, JSON.stringify(res));
      const payload = JSON.parse(res.result.content[0].text);
      assert.strictEqual(payload.status, 'DISPATCHED');
      assert.strictEqual(payload.threadId, threadId);
      assert.strictEqual(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'confirmed');
    } finally {
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('a wrong prompt token cannot claim the task', async () => {
    const threadId = 'thread_wrong_token';
    prepareTask(threadId, 'right-token');
    try {
      const res = await callDispatch({ reviewBootstrap: { threadId, token: 'wrong-token' } });
      assert.ok(res.error, JSON.stringify(res));
      assert.match(String(res.error.message), /token mismatch/i);
      assert.strictEqual(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'pending');
    } finally {
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('the v2 MCP entry refuses a historical bootstrap even with its original credential', async () => {
    const threadId = 'thread_historical_bootstrap';
    const token = 'retired-bootstrap-token';
    prepareTask(threadId, token, true);
    try {
      const response = await callDispatch({ reviewBootstrap: { threadId, token } });
      assert.match(response.error?.message || '', /HISTORICAL_TASK_READ_ONLY/);
      assert.equal(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'pending');
      assert.equal(readReviewerBootstrapToken(threadId, getBridgeHome())?.token, token);
    } finally {
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('a paused task cannot be confirmed by a delayed bootstrap and keeps its token', async () => {
    const threadId = 'thread_paused_bootstrap';
    prepareTask(threadId, 'token-before-pause');
    await taskStore.transitionExecution(threadId, 1, task => { task.status = 'paused'; });
    try {
      const res = await callDispatch({
        reviewBootstrap: { threadId, token: 'token-before-pause' },
      });
      assert.ok(res.error, JSON.stringify(res));
      assert.strictEqual(taskStore.getTask(threadId)?.status, 'paused');
      assert.strictEqual(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'pending');
      assert.strictEqual(readReviewerBootstrapToken(threadId, getBridgeHome())?.token, 'token-before-pause');
    } finally {
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('a v2 bootstrap CAS failure keeps its credential reusable until confirmation commits', async () => {
    const threadId = 'thread_bootstrap_cas_retry';
    prepareTask(threadId, 'cas-retry-token');
    const transition = taskStore.transitionExecution.bind(taskStore);
    (taskStore as any).transitionExecution = async () => { throw new Error('Execution revision mismatch'); };
    try {
      await assert.rejects(confirmReviewerBootstrapTask(taskStore, threadId, { token: 'cas-retry-token' }, getBridgeHome()), /revision mismatch/i);
      assert.equal(readReviewerBootstrapToken(threadId, getBridgeHome())?.token, 'cas-retry-token');
      assert.equal(taskStore.getTask(threadId)?.reviewBootstrap?.phase, 'pending');
      (taskStore as any).transitionExecution = transition;
      const confirmed = await confirmReviewerBootstrapTask(taskStore, threadId, { token: 'cas-retry-token' }, getBridgeHome());
      assert.equal(confirmed.reviewBootstrap?.phase, 'confirmed');
      assert.equal(readReviewerBootstrapToken(threadId, getBridgeHome()), undefined);
    } finally {
      (taskStore as any).transitionExecution = transition;
      clearReviewerBootstrapToken(threadId, getBridgeHome());
      taskStore.dispose();
    }
  });

  test('without a pending bootstrap a brief-less call is still an ordinary error', async () => {
    try {
      const noGitWorkspace = path.join(isolatedHome, 'no-git-workspace');
      fs.mkdirSync(noGitWorkspace, { recursive: true });
      fs.writeFileSync(path.join(noGitWorkspace, '.git'), 'invalid git directory marker');
      const res = await callDispatch({ targetAgent: 'claude_code', title: 'No Git repository', prompt: 'must fail', workspaceRoot: noGitWorkspace });
      assert.ok(res.error, JSON.stringify(res));
      assert.match(String(res.error.message), /Git|repository|HEAD/i);
    } finally {
      taskStore.dispose();
    }
  });
});
