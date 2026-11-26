import { testWorkspace } from './workspace-fixture';
import { test, describe, beforeEach, afterEach, after } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawnSync } from 'child_process';

// Isolate model defaults and execution locks from concurrently running tests.
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-dispatch-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;
let originalCwd = process.cwd();

function initGitRepo(root: string): void {
  const run = (...args: string[]) => {
    const result = spawnSync(gitExe, args, { cwd: root, encoding: 'utf8', windowsHide: true });
    if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  };
  run('init', '-b', 'test/source');
  run('config', 'user.name', 'Mesh Test');
  run('config', 'user.email', 'mesh-test@example.test');
  run('commit', '--allow-empty', '-m', 'base');
}

import { TaskStore } from '../bridge/task-store';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import { AsyncDispatcher } from '../bridge/async-dispatcher';
import { ReworkController } from '../bridge/rework-controller';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { NativeSessionLock } from '../bridge/native-session-lock';
import { getBridgeHome, invalidateConfiguredModelCache, saveModelDefaults } from '../bridge/model-catalog';
import { clearReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { describeModelRouting } from '../bridge/types';

const inheritedCallerAgent = process.env.MESH_CALLER_AGENT;
delete process.env.MESH_CALLER_AGENT;
after(() => {
  if (inheritedCallerAgent === undefined) delete process.env.MESH_CALLER_AGENT;
  else process.env.MESH_CALLER_AGENT = inheritedCallerAgent;
});

describe('AsyncDispatcher and ReworkController Tests', () => {
  let tempDir: string;
  let taskStore: TaskStore;
  let detector: AgentDetector;
  let resolver: ActiveAgentResolver;
  let runner: AutonomousRunner;
  let callbackManager: CallbackManager;
  let dispatcher: AsyncDispatcher;
  let reworkController: ReworkController;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-dispatch-'));
    const workspace = path.join(tempDir, 'repo');
    fs.mkdirSync(workspace, { recursive: true });
    initGitRepo(workspace);
    originalCwd = process.cwd();
    process.chdir(workspace);
    taskStore = new TaskStore(tempDir);
    detector = new AgentDetector();
    resolver = new ActiveAgentResolver(detector);
    runner = new AutonomousRunner(detector);
    runner.execute = async () => {
      await new Promise((r) => setTimeout(r, 50));
      return {
        exitCode: 0,
        stdout: 'Mock execution finished successfully',
        stderr: '',
        extractedOutput: 'Mock execution finished successfully',
        durationMs: 100,
      };
    };
    callbackManager = new CallbackManager(detector);
    callbackManager.notifyCaller = async () => true;
    const gate = new TaskExecutionRegistry(tempDir);
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, gate);
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
  });

  afterEach(() => {
    process.chdir(originalCwd);
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  /**
   * Wait until the mock receiver process has actually released its developer
   * role. Role enforcement (new) intentionally blocks the next role while the
   * previous round is still executing.
   */
  const dispatchReady = async (params: any) => {
    const target = params.targetAgent;
    const result = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(), ...params, reviewFlow: 'dedicated',
      initiatorAgent: params.initiatorAgent || (target === 'antigravity' ? 'codex' : 'antigravity') });
    return result;
  };

  test('should dispatch task asynchronously and create thread with round 1', async () => {
    const res = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
      targetAgent: 'codex',
      title: 'Optimize SQL Query',
      prompt: 'Add composite index on (tenant_id, created_at)',
      callerSessionId: 'caller-session-001',
    });

    assert.strictEqual(res.status, 'REVIEWER_STARTING');
    assert.strictEqual(res.targetAgent, 'codex');
    assert.ok(res.threadId.startsWith('thread_'));
    assert.ok(res.receiverSessionId);

    const stored = taskStore.getTask(res.threadId);
    assert.ok(stored);
    assert.strictEqual(stored.title, 'Optimize SQL Query');
    assert.strictEqual(stored.currentRound, 1);
    assert.strictEqual(stored.status, 'queued');
    assert.strictEqual(stored.reviewFlow, 'dedicated');
    assert.strictEqual(stored.receiverSessionId, res.receiverSessionId);
    assert.strictEqual(stored.rounds.length, 1);
    assert.strictEqual(stored.rounds[0].type, 'initial');
    // No model pinned: the agent's configured default applies at launch time.
    assert.strictEqual(stored.targetModel, undefined);
    assert.strictEqual(stored.initiatorModel, undefined);
    assert.strictEqual(stored.initiatorModelSource, 'none');
  });

  test('default auto in the host mirror keeps resolver and CLI defaults working', async () => {
    const previous = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = tempDir;
    try {
      resolver.setManualOverride('codex');
      const result = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(), targetAgent: 'default', title: 'Auto default', prompt: 'run' });
      assert.equal(result.targetAgent, 'codex');
      const explicitCli = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(), targetAgent: 'claude_code', title: 'Explicit CLI', prompt: 'run' });
      assert.equal(explicitCli.targetAgent, 'claude_code');
    } finally {
      if (previous === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = previous;
    }
  });

  test('refuses a same-agent dispatch while the legacy review flow is active', async () => {
    await assert.rejects(
      () =>
        dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
          targetAgent: 'codex',
          initiatorAgent: 'codex',
          title: 'Self dispatch',
          prompt: 'do the work',
        }),
      /configure a sender-side reviewer model/i,
    );
    // Rejected before the thread exists: it must not occupy the queue.
    assert.strictEqual(taskStore.getAllTasks().length, 0);
  });

  test('refuses a same-agent dispatch detected from the caller environment (legacy)', async () => {
    const previous = process.env.MESH_CALLER_AGENT;
    process.env.MESH_CALLER_AGENT = 'deepseek_harness';
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
            targetAgent: 'deepseek_harness',
            title: 'Self dispatch via env',
            prompt: 'do the work',
          }),
        /configure a sender-side reviewer model/i,
      );
      assert.strictEqual(taskStore.getAllTasks().length, 0);
    } finally {
      if (previous === undefined) delete process.env.MESH_CALLER_AGENT;
      else process.env.MESH_CALLER_AGENT = previous;
    }
  });

  test('dedicated flow prepares a reviewer bootstrap instead of starting the developer', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, {
      initiatorModels: { codex: 'gpt-6-pro' },
    });
    invalidateConfiguredModelCache();
    let executed = 0;
    runner.execute = async () => {
      executed += 1;
      return {
        exitCode: 0,
        stdout: 'x',
        stderr: '',
        extractedOutput: 'x',
        durationMs: 1,
      };
    };
    try {
      const res = await dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
        targetAgent: 'codex',
        initiatorAgent: 'codex',
        title: 'Self review with two models',
        prompt: 'do the work',
        model: 'gpt-6-flash',
        reviewFlow: 'dedicated',
      });
      assert.strictEqual(res.status, 'REVIEWER_STARTING');
      const stored = taskStore.getTask(res.threadId)!;
      assert.strictEqual(stored.reviewFlow, 'dedicated');
      assert.strictEqual(stored.reviewerModel, 'gpt-6-pro');
      assert.strictEqual(stored.targetModel, 'gpt-6-flash');
      assert.strictEqual(stored.reviewBootstrap?.phase, 'pending');
      assert.strictEqual(stored.status, 'queued');
      assert.strictEqual(stored.callerSessionId, undefined, 'caller is the reviewer, not the requester');
      // The developer must not run before the reviewer dispatches.
      await new Promise((r) => setTimeout(r, 80));
      assert.strictEqual(executed, 0);
      clearReviewerBootstrapToken(res.threadId, home);
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('dedicated same-agent dispatch rejects an identical reviewer/executor model', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, {
      initiatorModels: { codex: 'gpt-6-flash' },
    });
    invalidateConfiguredModelCache();
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
            targetAgent: 'codex',
            initiatorAgent: 'codex',
            title: 'Same model self review',
            prompt: 'do the work',
            model: 'gpt-6-flash',
            reviewFlow: 'dedicated',
          }),
        /same model/i,
      );
      assert.strictEqual(taskStore.getAllTasks().length, 0);
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('dedicated same-agent dispatch requires a configured reviewer model', async () => {
    const home = getBridgeHome();
    saveModelDefaults({ codex: 'gpt-6-flash' }, home, { initiatorModels: {} });
    invalidateConfiguredModelCache();
    try {
      await assert.rejects(
        () =>
          dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
            targetAgent: 'codex',
            initiatorAgent: 'codex',
            title: 'Unconfigured reviewer',
            prompt: 'do the work',
            model: 'gpt-6-flash',
            reviewFlow: 'dedicated',
          }),
        /configure a sender-side reviewer model/i,
      );
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('pins the user-approved model at V2 dispatch without starting a developer before bootstrap', async () => {
    // The user confirmed the switch, so the non-default model is applied.
    dispatcher.setModelApprovalHandler(async (request) => ({
      id: request.id,
      status: 'approved',
      at: new Date().toISOString(),
      by: 'user',
    }));

    const res = await dispatchReady({ workspaceRoot: process.cwd(),
      targetAgent: 'codex',
      title: 'Pin a specific model',
      prompt: 'Implement the parser',
      model: '  gpt-6-sol  ',
      modelRequestedByUser: true,
      initiatorModel: 'gpt-6.1-sol',
    });
    const stored = taskStore.getTask(res.threadId)!;
    assert.strictEqual(stored.targetModel, 'gpt-6-sol');
    assert.strictEqual(stored.modelApproval?.status, 'approved');
    // Both ends of the routing are persisted, so the ledger can show them.
    assert.strictEqual(stored.initiatorModel, 'gpt-6.1-sol');
    assert.strictEqual(stored.initiatorModelSource, 'explicit');
    assert.match(res.message, /model routing/i);
    assert.equal(stored.execution?.version, 2);
    assert.equal(stored.status, 'queued');
    assert.equal(stored.reviewBootstrap?.phase, 'pending');
  });

  test('records the sender-side default separately from the receiver-side default', async () => {
    const bridgeHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-two-ends-'));
    const previousHome = process.env.AGENT_MESH_BRIDGE_HOME;
    process.env.AGENT_MESH_BRIDGE_HOME = bridgeHome;
    // Two different agents, each with its own side of the defaults: Antigravity
    // reviews as `gemini-3-pro` while Codex executes as `gpt-6-flash`.
    saveModelDefaults({ codex: 'gpt-6-flash' }, bridgeHome, {
      initiatorModels: { antigravity: 'gemini-3-pro' },
    });
    invalidateConfiguredModelCache();

    try {
      const res = await dispatchReady({ workspaceRoot: process.cwd(),
        targetAgent: 'codex',
        title: 'One dispatch, two model defaults',
        prompt: 'Implement the parser',
        callerSessionId: 'caller-session-two-ends',
        initiatorAgent: 'antigravity',
      });
      const stored = taskStore.getTask(res.threadId)!;
      assert.strictEqual(stored.targetModel, 'gpt-6-flash', 'receiver uses its own default');
      assert.strictEqual(stored.initiatorModel, 'gemini-3-pro', 'sender is reported on its own default');
      assert.strictEqual(stored.initiatorModelSource, 'sender-configured');
      assert.match(describeModelRouting(stored), /gemini-3-pro/);
      assert.match(describeModelRouting(stored), /gpt-6-flash/);
    } finally {
      if (previousHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
      else process.env.AGENT_MESH_BRIDGE_HOME = previousHome;
      invalidateConfiguredModelCache();
      fs.rmSync(bridgeHome, { recursive: true, force: true });
    }
  });

  test('ignores a non-default model the caller did not declare as a user request', async () => {
    dispatcher.setModelApprovalHandler(async () => {
      throw new Error('a guessed model must never reach the user');
    });

    const res = await dispatchReady({ workspaceRoot: process.cwd(),
      targetAgent: 'codex',
      title: 'Do not guess a model',
      prompt: 'Implement the parser',
      model: 'gpt-6-luna',
    });
    const stored = taskStore.getTask(res.threadId)!;
    // No configured default in this isolated home, so nothing is pinned.
    assert.strictEqual(stored.targetModel, undefined);
    assert.match(String(stored.modelNotice || ''), /ignored/);
    assert.match(res.message, /ignored/);
    assert.equal(stored.execution?.version, 2);
    assert.equal(stored.status, 'queued');
  });

  test('a declined model switch aborts the dispatch before anything is queued', async () => {
    dispatcher.setModelApprovalHandler(async (request) => ({
      id: request.id,
      status: 'cancelled',
      at: new Date().toISOString(),
      by: 'user',
    }));

    await assert.rejects(
      () =>
        dispatcher.dispatchTask({ workspaceRoot: process.cwd(),
          targetAgent: 'codex',
          title: 'Declined switch',
          prompt: 'Implement the parser',
          model: 'gpt-6-luna',
          modelRequestedByUser: true,
        }),
      (err: any) => err?.code === 'MODEL_APPROVAL_DENIED',
    );

    assert.strictEqual(taskStore.count(), 0);
  });
});
