import { testWorkspace, confirmTestReviewer } from './workspace-fixture';
import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as crypto from 'crypto';
import { spawn, spawnSync } from 'child_process';

const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-mcp-home-'));
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
import { MCPServer } from '../bridge/mcp-server';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { ExecutionController } from '../bridge/execution-controller';
import { getProcessBirthId, probeProcessIdentity } from '../bridge/platform';
import { registerRunChildV2 } from '../bridge/process-binding';
import { meshMergeLockPath } from '../bridge/mesh-file-lock';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { roundOutputArchivePath } from '../bridge/round-log';
import { reviewerLogPath } from '../bridge/reviewer-log';
import {
  getBridgeHome,
  invalidateConfiguredModelCache,
  saveModelDefaults,
} from '../bridge/model-catalog';
import {
  clearReviewerBootstrapToken,
  readReviewerBootstrapToken,
  writeReviewerBootstrapToken,
} from '../bridge/reviewer-bootstrap';
import { DelegationRound } from '../bridge/types';
import { WindowWorkspace, WorkspaceRegistry, physicalDirectory } from '../bridge/workspace';

const { createBridgeHost } = require('../../bin/agent-bridge-mcp.cjs') as { createBridgeHost: (options?: any) => any };

function configureCodexReviewerFixture(): void {
  const home = getBridgeHome();
  saveModelDefaults({ codex: 'gpt-6-flash' }, home, { initiatorModels: { codex: 'gpt-6-pro' } });
  invalidateConfiguredModelCache();
}

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
  let savedReceiverEnv: string | undefined;
  let savedThreadIdEnv: string | undefined;

  beforeEach(() => {
    savedReceiverEnv = process.env.MESH_TASK_RECEIVER;
    savedThreadIdEnv = process.env.MESH_TASK_THREAD_ID;
    delete process.env.MESH_TASK_RECEIVER;
    delete process.env.MESH_TASK_THREAD_ID;
    saveModelDefaults({}, getBridgeHome(), { initiatorModels: {} });
    invalidateConfiguredModelCache();
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-mcp-test-'));
    const workspace = path.join(tempDir, 'repo');
    fs.mkdirSync(workspace, { recursive: true });
    initGitRepo(workspace);
    originalCwd = process.cwd();
    process.chdir(workspace);
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
    const gate = new TaskExecutionRegistry(tempDir);
    dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, gate);
    reworkController = new ReworkController(taskStore, runner, callbackManager, detector, gate);
    mcpServer = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner);
  });

  const waitForStatus = async (threadId: string, status: string, timeoutMs = 3000) => {
    const pending = taskStore.getTask(threadId);
    if (pending?.reviewBootstrap?.phase === 'pending') {
      const task = confirmTestReviewer(pending);
      taskStore.upsertTask(task);
      clearReviewerBootstrapToken(task.threadId, tempDir, task.artifactDir);
      void (dispatcher as any).executeBackgroundRound(task, 1, task.rounds[0].prompt, false);
    }
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
    if (savedReceiverEnv !== undefined) {
      process.env.MESH_TASK_RECEIVER = savedReceiverEnv;
    } else {
      delete process.env.MESH_TASK_RECEIVER;
    }
    if (savedThreadIdEnv !== undefined) {
      process.env.MESH_TASK_THREAD_ID = savedThreadIdEnv;
    } else {
      delete process.env.MESH_TASK_THREAD_ID;
    }
    process.chdir(originalCwd);
    taskStore.dispose();
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('independent MCP host dispatches v2 through bootstrap, rework, approval, and merge after child exit', async () => {
    (dispatcher as any).queueDrainer?.dispose();
    reworkController.dispose();
    taskStore.dispose();
    const taskLockTails = new Map<string, Promise<void>>();
    taskStore = new TaskStore(tempDir, { lockFactory: (lockPath) => ({
      acquire: async () => {
        const key = path.resolve(lockPath);
        const previous = taskLockTails.get(key) || Promise.resolve();
        let releaseNext!: () => void;
        const next = new Promise<void>((resolve) => { releaseNext = resolve; });
        taskLockTails.set(key, next);
        await previous;
        let released = false;
        return () => {
          if (released) return;
          released = true;
          releaseNext();
          if (taskLockTails.get(key) === next) taskLockTails.delete(key);
        };
      },
      dispose: () => undefined,
    }) });
    const registry = new TaskExecutionRegistry(tempDir);
    callbackManager.setTaskStore(taskStore);
    callbackManager.setAutonomousRunner(runner);
    const host = createBridgeHost({ taskStore, detector, resolver, runner, callbackManager, registry,
      executionControllerOptions: { storageDir: tempDir, standingScanIntervalMs: 0, streamSyncIntervalMs: 60_000 } });
    dispatcher = host.dispatcher;
    reworkController = host.reworkController;
    mcpServer = host.server;
    const controller: ExecutionController = host.executionController;
    const mergeActiveMarker = path.join(tempDir, 'merge-helper-active');
    const mergeReleaseMarker = path.join(tempDir, 'merge-helper-release');
    const validationDir = path.join(process.cwd(), '.agent-mesh-bridge');
    fs.mkdirSync(validationDir, { recursive: true });
    fs.writeFileSync(path.join(validationDir, 'validate.json'), JSON.stringify({
      commands: [{ command: process.execPath, args: ['-e', `const fs=require('fs');const p=${JSON.stringify(mergeActiveMarker)};const r=${JSON.stringify(mergeReleaseMarker)};fs.writeFileSync(p,'active');const t=setInterval(()=>{if(fs.existsSync(r)){clearInterval(t);try{fs.unlinkSync(p)}catch{};process.exit(0)}},20)`], label: 'fixture validation' }],
    }), 'utf8');
    const fixtureValidationCommit = spawnSync(gitExe, ['add', '.agent-mesh-bridge/validate.json'], { cwd: process.cwd(), encoding: 'utf8', windowsHide: true });
    assert.equal(fixtureValidationCommit.status, 0, fixtureValidationCommit.stderr);
    const fixtureBaselineCommit = spawnSync(gitExe, ['commit', '-m', 'declare isolated merge validation'], { cwd: process.cwd(), encoding: 'utf8', windowsHide: true });
    assert.equal(fixtureBaselineCommit.status, 0, fixtureBaselineCommit.stderr);
    const previousReviewSession = process.env.MESH_REVIEW_SESSION_ID;
    let bootstrapStage = 'not_started';
    const processEvents: Array<{ at: number; event: string; pid?: number; detail?: string }> = [];
    const recordProcessEvent = (event: string, pid?: number, detail?: string) => processEvents.push({ at: Date.now(), event, pid, detail });
    const originalTransitionExecution = taskStore.transitionExecution.bind(taskStore);
    taskStore.transitionExecution = (async (threadId: string, expectedRevision: number, change: (thread: any) => void) => {
      const startedAt = Date.now();
      recordProcessEvent('task_cas_begin', undefined, `rev=${expectedRevision}`);
      try {
        const result = await originalTransitionExecution(threadId, expectedRevision, change);
        recordProcessEvent('task_cas_ack', undefined, `rev=${result.execution?.revision};elapsed=${Date.now() - startedAt}`);
        return result;
      } catch (error: any) {
        recordProcessEvent('task_cas_rejected', undefined, `rev=${expectedRevision};elapsed=${Date.now() - startedAt};error=${error?.message || error}`);
        throw error;
      }
    }) as any;
    const controlledChildren = new Set<{ child: any; pid: number; birthId: string; closed: Promise<void>; isClosed: () => boolean }>();
    const rpc = async (name: string, args: any) => {
      const response = await mcpServer.handleRpcMessage({ jsonrpc: '2.0', id: crypto.randomUUID(), method: 'tools/call', params: { name, arguments: args } });
      if (response?.error) throw new Error(response.error.message);
      if (response?.result?.isError) throw new Error(response.result.content?.[0]?.text || `${name} failed`);
      return JSON.parse(response.result.content[0].text);
    };
    const waitFor = async (predicate: () => boolean, label: string) => {
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
        taskStore.loadFromDisk();
        if (predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      throw new Error(`Timed out waiting for ${label}; state=${JSON.stringify({ calls: { bootstrapSeen, bootstrapStage, developerCalls, reviewCalls }, processEvents, tasks: taskStore.getAllTasks().map((task) => ({ id: task.threadId, status: task.status, phase: task.execution?.phase, run: task.execution?.run, next: task.execution?.next, failure: task.failure, bootstrap: task.reviewBootstrap })) })}`);
    };
    const controlledChild = async (onChildPid?: (pid: number | undefined, child?: any, birthId?: string) => void, duringAlive?: (pid: number) => Promise<void>) => {
      const child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: ['ignore', 'ignore', 'ignore'], windowsHide: true });
      let isClosed = false;
      let resolveClosed!: () => void;
      const closed = new Promise<void>((resolve) => { resolveClosed = resolve; });
      child.once('close', () => { isClosed = true; recordProcessEvent('child_close', child.pid); resolveClosed(); });
      const childRecord: { child: any; pid: number; birthId: string; closed: Promise<void>; isClosed: () => boolean } = { child, pid: 0, birthId: '', closed, isClosed: () => isClosed };
      try {
        await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve); });
        const pid = child.pid;
        const birthId = pid ? getProcessBirthId(pid) : undefined;
        if (!pid || !birthId) throw new Error('Controlled subprocess identity was not observable after spawn.');
        childRecord.pid = pid;
        childRecord.birthId = birthId;
        controlledChildren.add(childRecord);
        recordProcessEvent('child_spawned', pid, birthId);
        recordProcessEvent('child_binding_begin', pid, birthId);
        await onChildPid?.(pid, child, birthId);
        recordProcessEvent('child_binding_ack', pid, birthId);
        await duringAlive?.(pid);
        recordProcessEvent('child_stop_requested', pid, birthId);
        if (!isClosed) child.kill('SIGKILL');
        await Promise.race([closed, new Promise<never>((_resolve, reject) => setTimeout(() => reject(new Error(`Controlled child ${pid}/${birthId} did not emit close.`)), 5000))]);
        recordProcessEvent('child_close_confirmed', pid, `${birthId};probe=${probeProcessIdentity(pid, birthId)}`);
        if (probeProcessIdentity(pid, birthId) !== 'exited') throw new Error(`Controlled child ${pid}/${birthId} emitted close but its identity is not confirmed exited.`);
        recordProcessEvent('child_exit_callback_begin', pid, birthId);
        await onChildPid?.(undefined);
        recordProcessEvent('child_exit_callback_ack', pid, birthId);
      } finally {
        if (!isClosed) {
          recordProcessEvent('child_failure_cleanup', child.pid);
          try { child.kill('SIGKILL'); } catch {}
          await Promise.race([closed, new Promise((resolve) => setTimeout(resolve, 5000))]);
        }
        if (isClosed) controlledChildren.delete(childRecord);
      }
    };
    let bootstrapSeen = false;
    let developerCalls = 0;
    let reviewCalls = 0;
    let requesterSessionUsed = false;
    try {
      runner.execute = async (options: any) => {
        if (!bootstrapSeen) {
          bootstrapSeen = true;
          bootstrapStage = 'loading_token';
          const task = taskStore.getTask(options.taskThreadId)!;
          const credential = readReviewerBootstrapToken(task.threadId, tempDir, task.artifactDir)!;
          bootstrapStage = 'child_started';
          await controlledChild(options.onChildPid, async () => { bootstrapStage = 'confirming'; await rpc('dispatch_task', { reviewBootstrap: { threadId: task.threadId, token: credential.token } }); bootstrapStage = 'confirmed'; });
          bootstrapStage = 'child_exited';
          return { exitCode: 0, stdout: 'reviewer ready', stderr: '', extractedOutput: 'reviewer ready', actualSessionId: 'reviewer-session-fixed', durationMs: 1 };
        }
        bootstrapStage = 'business_runner';
        developerCalls++;
        recordProcessEvent('developer_runner_start', undefined, `round=${developerCalls}`);
        fs.writeFileSync(path.join(options.cwd, `round-${developerCalls}.txt`), `work ${developerCalls}\n`);
        await controlledChild(options.onChildPid);
        return {
          exitCode: 0, stdout: `developer output ${developerCalls}`, stderr: '',
          extractedOutput: `developer output ${developerCalls}`, actualSessionId: 'developer-session-fixed', durationMs: 1,
          assessment: { success: true, exitCode: 0, stream: { hasActualOutput: true, chunkCount: 1, stdoutBytes: 12, stderrBytes: 0, continuous: true, gapExceeded: false, maxGapMs: 0, gapThresholdMs: 60_000 } },
        } as any;
      };
      callbackManager.launchCaller = (async (thread: any, _round: any, onChildPid: any) => {
        reviewCalls++;
        recordProcessEvent('reviewer_runner_start', undefined, `review=${reviewCalls}`);
        const reviewer = thread.reviewerSessionId;
        assert.equal(reviewer, 'reviewer-session-fixed');
        assert.notEqual(reviewer, thread.requesterSessionId);
        requesterSessionUsed ||= reviewer === thread.callerSessionId && reviewer !== 'reviewer-session-fixed';
        process.env.MESH_REVIEW_SESSION_ID = reviewer;
        const doReview = async (pid: number) => {
          if (reviewCalls === 1) {
            await rpc('request_rework', { threadId: thread.threadId, feedback: 'add a second change' });
            recordProcessEvent('rework_decision_persisted', pid);
          const handed = taskStore.getTask(thread.threadId)!;
          assert.equal(handed.execution?.next?.phase, 'develop', 'review decision and next phase are durable together');
          assert.equal(handed.execution?.run?.role, 'reviewer', 'reviewer run remains until its controlled child exits');
          assert.equal(developerCalls, 1, 'next is not consumed while the reviewer child is alive');
          } else {
            await rpc('approve_task', { threadId: thread.threadId, remarks: 'accepted' });
            recordProcessEvent('approve_decision_persisted', pid);
          const handed = taskStore.getTask(thread.threadId)!;
          assert.equal(handed.execution?.next?.phase, 'merge', 'approval and merge handoff are durable together');
          assert.equal(handed.execution?.run?.role, 'reviewer', 'approved reviewer run remains until its child exits');
          }
        };
        await controlledChild(onChildPid, doReview);
        return { success: false, exitCode: 17, error: 'controlled reviewer tail exits nonzero after durable decision' };
      }) as any;

      const dispatched = await rpc('dispatch_task', {
        workspaceRoot: process.cwd(), targetAgent: 'claude_code', initiatorAgent: 'codex',
        title: 'Independent MCP v2 lifecycle', prompt: 'Implement the fixture change',
      });
      const threadId = dispatched.threadId;
      assert.equal(taskStore.getTask(threadId)?.execution?.version, 2, 'formal independent MCP dispatch creates v2');
      const targetAdvance = spawnSync(gitExe, ['commit', '--allow-empty', '-m', 'advance delivery target for merge fixture'], { cwd: process.cwd(), encoding: 'utf8', windowsHide: true });
      assert.equal(targetAdvance.status, 0, targetAdvance.stderr || 'could not advance temporary delivery target');
      await waitFor(() => developerCalls >= 1, 'first developer launch after bootstrap exit ACK');
      await waitFor(() => reviewCalls >= 1, 'reviewer launch after first developer exit ACK');
      await waitFor(() => processEvents.some((event) => event.event === 'rework_decision_persisted'), 'durable rework decision');
      const firstReworkPid = processEvents.find((event) => event.event === 'rework_decision_persisted')?.pid;
      assert.ok(firstReworkPid, 'the rework decision must be tied to its real reviewer child');
      await waitFor(() => processEvents.some((event) => event.event === 'child_exit_callback_ack' && event.pid === firstReworkPid), 'reviewer child close and run exit ACK');
      await waitFor(() => developerCalls >= 2, 'next consumption after reviewer exit ACK');
      await waitFor(() => reviewCalls >= 2, 'second review');
      await waitFor(() => fs.existsSync(mergeActiveMarker), 'merge helper child to hold the global merge lock');
      recordProcessEvent('merge_helper_active');
      const competingMergeLock = new MeshFileLock(meshMergeLockPath(tempDir));
      const stolenMergeLock = await competingMergeLock.acquire(undefined, undefined, 250);
      assert.equal(stolenMergeLock, undefined, 'the global merge lock remains unavailable while the real Git hook child is alive');
      competingMergeLock.dispose();
      fs.writeFileSync(mergeReleaseMarker, 'release', 'utf8');
      recordProcessEvent('merge_helper_release_requested');
      await waitFor(() => taskStore.getTask(threadId)?.status === 'completed', 'merge completion');
      assert.equal(fs.existsSync(mergeActiveMarker), false, 'the merge hook child exited before lock release was acknowledged');
      const completed = taskStore.getTask(threadId)!;
      assert.equal(completed.reviewerSessionId, 'reviewer-session-fixed');
      assert.equal(completed.callerSessionId, 'reviewer-session-fixed');
      assert.equal(requesterSessionUsed, false);
      assert.equal(developerCalls, 2);
      assert.equal(reviewCalls, 2);
      const reworkIndex = processEvents.findIndex((event) => event.event === 'rework_decision_persisted');
      const reworkPid = processEvents[reworkIndex]?.pid;
      const reworkExitAckIndex = processEvents.findIndex((event) => event.event === 'child_exit_callback_ack' && event.pid === reworkPid);
      const secondDeveloperIndex = processEvents.findIndex((event) => event.event === 'developer_runner_start' && event.detail === 'round=2');
      assert.ok(reworkIndex >= 0 && reworkIndex < reworkExitAckIndex && reworkExitAckIndex < secondDeveloperIndex,
        `rework was not consumed after confirmed child close/exit callback: ${JSON.stringify(processEvents)}`);
      const mergeLock = new MeshFileLock(meshMergeLockPath(tempDir));
      const release = await mergeLock.acquire(undefined, undefined, 10_000);
      assert.ok(release, 'global merge lock helper acknowledged release');
      release();
      mergeLock.dispose();
    } finally {
      try { fs.writeFileSync(mergeReleaseMarker, 'failure cleanup release', 'utf8'); } catch {}
      const mergeExitDeadline = Date.now() + 5000;
      while (fs.existsSync(mergeActiveMarker) && Date.now() < mergeExitDeadline) {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      for (const child of [...controlledChildren]) {
        if (!child.isClosed()) {
          recordProcessEvent('outer_failure_cleanup', child.pid, child.birthId);
          try { child.child.kill('SIGKILL'); } catch {}
        }
        await Promise.race([child.closed, new Promise((resolve) => setTimeout(resolve, 5000))]);
        controlledChildren.delete(child);
      }
      if (previousReviewSession === undefined) delete process.env.MESH_REVIEW_SESSION_ID;
      else process.env.MESH_REVIEW_SESSION_ID = previousReviewSession;
      await controller.waitForActiveExecutions(5000);
      controller.dispose();
      registry.dispose();
    }
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

  test('standalone stdio reads historical history but refuses retry without changing ledger files', async () => {
    const isolatedUserHome = path.join(tempDir, 'stdio-user-home');
    const bridgeHome = path.join(isolatedUserHome, '.agent-mesh-bridge');
    const fixtureStore = new TaskStore(bridgeHome);
    const historical = [
      { threadId: 'thread_stdio_completed_v1_history', version: 1 },
      { threadId: 'thread_stdio_completed_no_execution_history', version: undefined },
      { threadId: 'thread_stdio_completed_unknown_version_history', version: 99 },
      { threadId: 'thread_stdio_completed_v1_live_ledger_owner', version: 1, liveOwner: true },
    ];
    const heldLegacyOwnerPid = process.pid;
    const heldLegacyBirthId = getProcessBirthId(heldLegacyOwnerPid);
    assert.ok(heldLegacyBirthId, 'live historical owner must have an exact PID/birthId');
    for (const fixture of historical) fixtureStore.upsertTask({
      threadId: fixture.threadId,
      title: `Completed historical fixture ${fixture.threadId}`,
      initiatorAgent: 'codex', targetAgent: 'claude_code', reviewFlow: 'dedicated', status: 'completed', currentRound: 1,
      createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      receiverSessionId: 'historical-session-never-resumed', workspace: testWorkspace(tempDir),
      rounds: [{ round: 1, type: 'initial', prompt: 'historical work', status: 'completed', durationMs: 1, timestamp: new Date().toISOString(), output: 'done' }],
      summary: { lastPromptSnippet: 'historical work', lastOutputSnippet: 'done', totalDurationMs: 1 },
      ...(fixture.version === undefined ? {} : { execution: {
        version: fixture.version, revision: 1, operationId: 'legacy_operation', phase: 'develop', round: 1,
        generation: 1, attempt: 0, autoRetriesUsed: 0,
        budget: { limitMs: 120_000, spentActiveMs: 1, maxAutoRetries: 2 },
        ...(fixture.liveOwner ? { run: { id: 'legacy-live-run', owner: { pid: heldLegacyOwnerPid, birthId: heldLegacyBirthId },
          state: 'stopping', step: 'develop', startedAt: new Date().toISOString() }, next: { phase: 'review', round: 2, sourceRunId: 'legacy-live-run' } } : {}),
      } }),
    } as any);
    fixtureStore.dispose();
    const before = new Map(historical.map(({ threadId }) => {
      const file = path.join(bridgeHome, 'threads', `${threadId}.json`);
      return [threadId, fs.readFileSync(file)] as const;
    }));
    const packageRoot = path.resolve(__dirname, '../..');
    const packageVersion = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
    fs.writeFileSync(path.join(bridgeHome, 'v2-runtime-components.json'), JSON.stringify({
      schemaVersion: 1, cutoverApproved: true,
      components: [
        { component: 'extension-host', version: packageVersion, packageRoot, path: path.join(packageRoot, 'out', 'extension.js'), completeProcessInventory: true, previousProcesses: [] },
        { component: 'standalone-mcp', version: packageVersion, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs'), completeProcessInventory: true, previousProcesses: [] },
      ],
    }));
    const snapshotFiles = (root: string): Map<string, Buffer> => {
      const files = new Map<string, Buffer>();
      const visit = (directory: string) => {
        for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
          const absolute = path.join(directory, entry.name);
          if (entry.isDirectory()) visit(absolute);
          else if (entry.isFile()) files.set(path.relative(root, absolute).replace(/\\/g, '/'), fs.readFileSync(absolute));
        }
      };
      visit(root);
      return files;
    };
    const serverEntry = path.resolve(__dirname, '../../bin/agent-bridge-mcp.cjs');
    const child = spawn(process.execPath, [serverEntry, '--caller', 'codex'], {
      cwd: process.cwd(), windowsHide: true,
      env: { ...process.env, AGENT_MESH_BRIDGE_HOME: bridgeHome, USERPROFILE: isolatedUserHome, HOME: isolatedUserHome },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let output = '';
    let stderr = '';
    let closed = false;
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => { output += chunk; });
    child.stderr.on('data', (chunk) => { stderr += chunk; });
    const waitResponse = async (id: number): Promise<any> => {
      const deadline = Date.now() + 10_000;
      while (Date.now() < deadline) {
        const lines = output.split(/\r?\n/);
        for (const line of lines) {
          if (!line.trim()) continue;
          try {
            const message = JSON.parse(line);
            if (message.id === id) return message;
          } catch {}
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      throw new Error(`stdio response ${id} timed out; stdout=${output}; stderr=${stderr}`);
    };
    const request = async (id: number, method: string, params: any = {}) => {
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
      return waitResponse(id);
    };
    try {
      const initialized = await request(1, 'initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'history-gate-test', version: '1' } });
      assert.equal(initialized.result?.serverInfo?.name, 'agent-mesh-bridge', JSON.stringify(initialized));
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`);
      let requestId = 2;
      const retryIds: string[] = [];
      for (const { threadId, version } of historical) {
        const summary = await request(requestId++, 'tools/call', { name: 'get_task_summary', arguments: { threadId } });
        assert.ok(summary.result?.content?.[0]?.text, `stdio summary failed: ${JSON.stringify(summary)}; stderr=${stderr}`);
        assert.equal(JSON.parse(summary.result.content[0].text).status, 'completed', 'history reads remain available over actual stdio');
        retryIds.push(threadId);
      }
      const beforeControlCalls = snapshotFiles(bridgeHome);
      for (const [index, threadId] of retryIds.entries()) {
        const { version } = historical[index];
        const retry = await request(requestId++, 'tools/call', { name: 'retry_task', arguments: { threadId } });
        const reason = version === 1 ? 'LEGACY_VERSION_1' : version === undefined ? 'NO_EXECUTION' : 'UNKNOWN_VERSION';
        assert.match(retry.error?.message || '', new RegExp(`HISTORICAL_TASK_READ_ONLY.*${reason}`));
      }
      const afterControlCalls = snapshotFiles(bridgeHome);
      assert.deepEqual([...afterControlCalls.keys()].sort(), [...beforeControlCalls.keys()].sort(), 'historical retry must not create or remove control/session files');
      for (const [relative, bytes] of beforeControlCalls) {
        assert.deepEqual(afterControlCalls.get(relative), bytes, `${relative} changed after rejecting historical execution`);
      }
      for (const [threadId, bytes] of before) {
        assert.deepEqual(fs.readFileSync(path.join(bridgeHome, 'threads', `${threadId}.json`)), bytes,
          `${threadId} retry must not alter generation, execution, rounds, or any ledger bytes`);
      }
      const dispatch = await request(requestId++, 'tools/call', { name: 'dispatch_task', arguments: {
        targetAgent: 'claude_code', title: 'blocked before v2 activation', prompt: 'must not spawn', workspaceRoot: process.cwd(),
      } });
      assert.match(dispatch.error?.message || '', /V2_STARTUP_BLOCKED.*(RUN_RECORD_REMAINS|RUNTIME_OLD_PROCESS_ALIVE|HISTORICAL_TASK_NOT_DRAINED)/);
      assert.deepEqual(snapshotFiles(bridgeHome), beforeControlCalls, 'failed runtime admission must not add a task, run, session, or control record');
    } finally {
      child.stdin.end();
      const exited = await Promise.race([
        new Promise<boolean>((resolve) => child.once('close', () => { closed = true; resolve(true); })),
        new Promise<boolean>((resolve) => setTimeout(() => resolve(false), 2_000)),
      ]);
      if (!exited) {
        child.kill();
        await new Promise<void>((resolve) => child.once('close', () => { closed = true; resolve(); }));
      }
      assert.equal(closed, true, 'standalone stdio process must exit after stdin closes');
    }
  });

  test('two standalone stdio hosts recover admission for the exact live v2 merge helper without acquiring another lock', async () => {
    const isolatedHome = path.join(tempDir, 'stdio-v2-live-merge-home');
    const bridgeHome = path.join(isolatedHome, '.agent-mesh-bridge');
    const threadId = 'thread_stdio_v2_live_merge';
    const token = 'stdio-live-merge-token';
    const channelPath = path.join(bridgeHome, `merge.lock.${token}`);
    const owner = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
    const helper = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
    const validation = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
    assert.ok(owner.pid && helper.pid && validation.pid);
    const ownerBirth = getProcessBirthId(owner.pid!);
    const helperBirth = getProcessBirthId(helper.pid!);
    const validationBirth = getProcessBirthId(validation.pid!);
    assert.ok(ownerBirth && helperBirth && validationBirth);
    fs.mkdirSync(path.join(bridgeHome, 'threads'), { recursive: true });
    const task = {
      threadId, title: 'Active merge recovery fixture', initiatorAgent: 'codex', targetAgent: 'claude_code',
      status: 'merging', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      reviewFlow: 'dedicated', workspace: testWorkspace(process.cwd()), rounds: [], summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      execution: { version: 2, revision: 1, operationId: 'merge-operation', phase: 'merge', round: 1, generation: 1,
        attempt: 0, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 10, maxAutoRetries: 2 },
        run: { id: 'live-merge-run', owner: { pid: owner.pid, birthId: ownerBirth }, child: { pid: validation.pid, birthId: validationBirth },
          subtrees: [{ root: { pid: validation.pid, birthId: validationBirth }, descendants: [] }],
          mergeLockHelper: { pid: helper.pid, birthId: helperBirth, channelPath, reason: 'held for restart recovery', observedAt: new Date().toISOString() },
          state: 'active', step: 'merge', startedAt: new Date().toISOString() },
      },
    } as any;
    fs.writeFileSync(path.join(bridgeHome, 'threads', `${threadId}.json`), JSON.stringify(task));
    fs.writeFileSync(path.join(bridgeHome, 'delegation_threads.json'), JSON.stringify([task]));
    const packageRoot = path.resolve(__dirname, '../..');
    const packageVersion = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
    fs.writeFileSync(path.join(bridgeHome, 'v2-runtime-components.json'), JSON.stringify({ schemaVersion: 1, cutoverApproved: true,
      components: [
        { component: 'extension-host', version: packageVersion, packageRoot, path: path.join(packageRoot, 'out', 'extension.js'), completeProcessInventory: true, previousProcesses: [] },
        { component: 'standalone-mcp', version: packageVersion, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs'), completeProcessInventory: true, previousProcesses: [] },
      ] }));
    fs.writeFileSync(`${channelPath}.helper-pid`, String(helper.pid));
    fs.writeFileSync(`${channelPath}.helper-birth`, helperBirth);
    fs.writeFileSync(`${channelPath}.child.${validation.pid}`, 'tracked');
    fs.writeFileSync(path.join(bridgeHome, 'merge.lock.owner.json'), JSON.stringify({ token, pid: owner.pid, birthId: ownerBirth,
      helperPid: helper.pid, helperBirthId: helperBirth, acquiredAt: Date.now() }));

    const beforeTask = fs.readFileSync(path.join(bridgeHome, 'threads', `${threadId}.json`));
    const beforeOwner = fs.readFileSync(path.join(bridgeHome, 'merge.lock.owner.json'));
    const beforeChannels = fs.readdirSync(bridgeHome).filter((name) => name.startsWith('merge.lock.')).sort();
    const entry = path.resolve(__dirname, '../../bin/agent-bridge-mcp.cjs');
    const startHost = () => spawn(process.execPath, [entry, '--caller', 'codex'], { cwd: process.cwd(), windowsHide: true,
      env: { ...process.env, AGENT_MESH_BRIDGE_HOME: bridgeHome, USERPROFILE: isolatedHome, HOME: isolatedHome, MESH_GIT_PATH: gitExe },
      stdio: ['pipe', 'pipe', 'pipe'] });
    const hosts = [startHost(), startHost()];
    const connectAndRead = async (host: ReturnType<typeof spawn>) => {
      let stdout = ''; let stderr = '';
      host.stdout!.setEncoding('utf8'); host.stderr!.setEncoding('utf8');
      host.stdout!.on('data', (chunk) => { stdout += chunk; }); host.stderr!.on('data', (chunk) => { stderr += chunk; });
      const response = async (id: number) => {
        const deadline = Date.now() + 15_000;
        while (Date.now() < deadline) {
          for (const line of stdout.split(/\r?\n/)) {
            try { const message = JSON.parse(line); if (message.id === id) return message; } catch {}
          }
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        throw new Error(`stdio ${id} timed out; stdout=${stdout}; stderr=${stderr}`);
      };
      host.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'live-v2-restart', version: '1' } } })}\n`);
      const initialized = await response(1);
      assert.equal(initialized.result?.serverInfo?.name, 'agent-mesh-bridge', JSON.stringify(initialized));
      host.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`);
      host.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'get_task_summary', arguments: { threadId } } })}\n`);
      const summary = await response(2);
      assert.ok(summary.result?.content?.[0]?.text, JSON.stringify(summary));
      assert.equal(JSON.parse(summary.result.content[0].text).status, 'merging');
      host.stdin!.end();
      await Promise.race([new Promise<void>((resolve) => host.once('close', () => resolve())), new Promise<void>((resolve) => setTimeout(resolve, 5_000))]);
      if (host.exitCode === null && host.signalCode === null) host.kill();
    };
    try {
      await Promise.all(hosts.map(connectAndRead));
      assert.deepEqual(fs.readFileSync(path.join(bridgeHome, 'threads', `${threadId}.json`)), beforeTask,
        'restart scanners must leave the live merge run untouched until owner/helper/child exit evidence changes');
      assert.deepEqual(fs.readFileSync(path.join(bridgeHome, 'merge.lock.owner.json')), beforeOwner,
        'restart must not replace or duplicate the current OS helper owner');
      assert.deepEqual(fs.readdirSync(bridgeHome).filter((name) => name.startsWith('merge.lock.')).sort(), beforeChannels,
        'parallel stdio restarts must not create another helper channel while the exact v2 run owns the merge');
    } finally {
      for (const host of hosts) { try { if (host.exitCode === null && host.signalCode === null) host.kill(); } catch {} }
      const stop = async (child: ReturnType<typeof spawn>) => {
        if (child.exitCode !== null || child.signalCode !== null) return;
        await new Promise<void>((resolve) => { child.once('close', () => resolve()); try { child.kill(); } catch { resolve(); } });
      };
      await Promise.all([stop(owner), stop(helper), stop(validation)]);
    }
  });

  test('client roots resolve only a unique live window and its manual default', async () => {
    const window = new WindowWorkspace(tempDir, [process.cwd()]);
    const registry = new WorkspaceRegistry(tempDir);
    const selected = registry.register(process.cwd(), 'user');
    window.setDefault(selected.id);
    try {
      (mcpServer as any).clientRoots = Promise.resolve([physicalDirectory(process.cwd())]);
      const nonGit = path.join(tempDir, 'not-git'); fs.mkdirSync(nonGit);
      const invalid = registry.register(nonGit, 'user'); window.setDefault(invalid.id);
      await assert.rejects(mcpServer.handleToolCall('dispatch_task', { targetAgent: 'claude_code', title: 'failed', prompt: 'run' }), /Git|repository/);
      assert.equal((mcpServer as any).workspaceContext, undefined);
      assert.equal(window.context.defaultWorkspaceId, invalid.id);
      window.setDefault(selected.id);
      assert.deepEqual(await mcpServer.handleToolCall('list_recent_tasks', { scope: 'current' }), []);
      const result = await mcpServer.handleToolCall('dispatch_task', { targetAgent: 'claude_code', title: 'root context', prompt: 'run' });
      assert.equal(result.workspace.id, selected.id);
      assert.equal(result.workspace.source, 'window-default');
      assert.equal(result.workspace.origin.hostInstanceId, window.context.hostInstanceId);
      const second = new WindowWorkspace(tempDir, [process.cwd()]);
      try {
        (mcpServer as any).workspaceContext = undefined;
        await assert.rejects(mcpServer.handleToolCall('dispatch_task', { targetAgent: 'claude_code', title: 'ambiguous', prompt: 'run' }), /WORKSPACE_AMBIGUOUS/);
      } finally { second.dispose(); }
    } finally { window.dispose(); }
  });

  test('linked workspace ID, root and bound context list the dispatch workspace', async () => {
    const main = process.cwd();
    const linked = path.join(tempDir, 'linked');
    const git = spawnSync(gitExe, ['worktree', 'add', '-b', 'linked-test', linked], { cwd: main, encoding: 'utf8', windowsHide: true });
    assert.equal(git.status, 0, git.stderr);
    const registry = new WorkspaceRegistry(tempDir);
    const linkedId = registry.register(linked).id;
    const mainId = registry.register(main).id;
    const byId = await mcpServer.handleToolCall('list_task_templates', { workspaceId: linkedId });
    const byRoot = await mcpServer.handleToolCall('list_task_templates', { workspaceRoot: linked });
    (mcpServer as any).workspaceContext = { id: linkedId };
    const inherited = await mcpServer.handleToolCall('list_task_templates', {});
    assert.equal(byId.workspaceId, mainId);
    assert.equal(byRoot.workspaceId, mainId);
    assert.equal(inherited.workspaceId, mainId);
    assert.equal(fs.existsSync(path.join(linked, '.agent_bridge')), false);
    const count = taskStore.listTasks(1000, 'all').length;
    await assert.rejects(mcpServer.handleToolCall('dispatch_task', { workspaceId: linkedId, targetAgent: 'claude_code', initiatorAgent: 'codex', title: 'invalid template', prompt: 'work', templateId: 'missing' }), /TASK_TEMPLATE_INVALID_OR_NOT_FOUND/);
    assert.equal(taskStore.listTasks(1000, 'all').length, count);
    const template = byId.templates.find((t: any) => t.id === 'programming');
    assert.ok(template);
    assert.equal(template.workerInstructions, undefined);
    const realDispatch = dispatcher.dispatchTask.bind(dispatcher);
    dispatcher.dispatchTask = params => realDispatch({ ...params, reviewFlow: 'dedicated' });
    const result = await mcpServer.handleToolCall('dispatch_task', { workspaceId: linkedId, targetAgent: 'claude_code', initiatorAgent: 'codex', title: 'linked template', prompt: 'work', templateId: template.id });
    assert.equal(result.workspace.id, mainId);
    assert.equal(taskStore.getTask(result.threadId)?.taskTemplate?.id, template.id);
  });

  test('explicit cross-workspace dispatch keeps caller window on first and later calls', async () => {
    const rootA = process.cwd();
    const rootB = path.join(tempDir, 'repo-b'); fs.mkdirSync(rootB); initGitRepo(rootB);
    const registry = new WorkspaceRegistry(tempDir);
    const workspaceA = registry.register(rootA, 'user');
    const workspaceB = registry.register(rootB, 'user');
    const windowA = new WindowWorkspace(tempDir, [rootA]);
    const windowB = new WindowWorkspace(tempDir, [rootB]);
    windowA.setDefault(workspaceA.id);
    const realDispatch = dispatcher.dispatchTask.bind(dispatcher);
    dispatcher.dispatchTask = (params) => realDispatch({ ...params, reviewFlow: 'dedicated' });
    try {
      (mcpServer as any).clientRoots = Promise.resolve([physicalDirectory(rootA)]);
      const send = (workspaceRoot?: string) => mcpServer.handleToolCall('dispatch_task', {
        targetAgent: 'claude_code', initiatorAgent: 'codex', title: 'Window origin', prompt: 'run', ...(workspaceRoot ? { workspaceRoot } : {}),
      });
      const first = await send(rootB);
      assert.equal(first.workspace.id, workspaceB.id);
      assert.equal(first.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      assert.equal(windowB.adopt(first.workspace), false);
      assert.equal(windowB.context.defaultWorkspaceId, undefined);
      const next = await send();
      assert.equal(next.workspace.id, workspaceA.id);
      assert.equal(next.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      const again = await send(rootB);
      assert.equal(again.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      assert.equal(windowB.adopt(again.workspace), false);
      const byId = await mcpServer.handleToolCall('dispatch_task', { workspaceId: workspaceB.id,
        targetAgent: 'claude_code', initiatorAgent: 'codex', title: 'Workspace by id', prompt: 'run' });
      assert.equal(byId.workspace.id, workspaceB.id);
      assert.equal(byId.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      assert.equal(windowB.adopt(byId.workspace), false);

      (mcpServer as any).workspaceContext = undefined;
      const invoked = await mcpServer.handleToolCall('invoke_antigravity', { workspaceRoot: rootB, prompt: 'run' });
      assert.equal(invoked.workspace.id, workspaceB.id);
      assert.equal(invoked.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      assert.equal(windowB.adopt(invoked.workspace), false);

      (mcpServer as any).workspaceContext = { id: workspaceB.id };
      const lateRoots = await send(rootB);
      assert.equal(lateRoots.workspace.origin.hostInstanceId, windowA.context.hostInstanceId);
      assert.equal(windowB.adopt(lateRoots.workspace), false);

      const otherA = new WindowWorkspace(tempDir, [rootA]);
      try {
        (mcpServer as any).workspaceContext = undefined;
        const ambiguous = await send(rootB);
        assert.equal(ambiguous.workspace.origin, undefined);
        assert.equal(windowB.adopt(ambiguous.workspace), false);
      } finally { otherA.dispose(); }
    } finally { windowA.dispose(); windowB.dispose(); }
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
    assert.ok(toolNames.includes('delete_task'));
    assert.ok(toolNames.includes('record_merge_request'));
    assert.ok(toolNames.includes('list_recent_tasks'));
    assert.ok(toolNames.includes('list_workspaces'));
    assert.ok(toolNames.includes('get_thread_details'));
    assert.ok(toolNames.includes('invoke_agent'));
    assert.ok(toolNames.includes('query_agent_mesh_status'));
    assert.ok(toolNames.includes('get_recent_logs'));
    assert.ok(toolNames.includes('get_round_raw_log'));
    assert.ok(toolNames.includes('get_task_token_usage'));
    assert.ok(toolNames.includes('get_reviewer_log'));
    assert.ok(toolNames.includes('list_agent_models'));
    assert.ok(toolNames.includes('get_task_summary'));
    const dispatch = tools.find((t: any) => t.name === 'dispatch_task');
    assert.equal(dispatch.inputSchema.properties.targetBranch.type, 'string');
    assert.equal(dispatch.inputSchema.properties.workspaceRoot.type, 'string');
    assert.ok(!Object.prototype.hasOwnProperty.call(dispatch.inputSchema.properties, 'workspacePath'));
    assert.ok(!Object.prototype.hasOwnProperty.call(dispatch.inputSchema.properties, 'expectedFiles'));
    assert.ok(!Object.prototype.hasOwnProperty.call(dispatch.inputSchema.properties, 'allowNoScope'));
    assert.ok(toolNames.includes('submit_merge_resolution'));
    assert.ok(toolNames.includes('get_merge_log'));
    assert.ok(toolNames.includes('submit_merge_decision'));
    assert.ok(toolNames.includes('list_task_templates'));
    assert.ok(toolNames.includes('request_task_decision'));
    assert.ok(toolNames.includes('submit_task_decision'));
    assert.strictEqual(toolNames.length, 28);
  });

  test('list_agent_models reports the receiver and sender defaults separately', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 62,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => {
      assert.ok(res?.result?.content?.[0]?.text, `MCP call failed: ${JSON.stringify(res)}`);
      return JSON.parse(res.result.content[0].text);
    };

    const home = getBridgeHome();
    // Sender and receiver defaults are independent per agent (self-dispatch is
    // rejected, so this only pins how each side is reported).
    saveModelDefaults({ deepseek_harness: 'deepseek-v4.1-flash' }, home, {
      initiatorModels: { deepseek_harness: 'deepseek-v4.1-pro' },
    });
    invalidateConfiguredModelCache();
    // Skip live CLI/provider discovery: the payload contract is what this test pins.
    (mcpServer as any).modelCatalog = {
      getAll: async () => ({
        deepseek_harness: {
          agent: 'deepseek_harness',
          models: [
            { id: 'deepseek-v4.1-pro' },
            { id: 'deepseek-v4.1-flash' },
          ],
          source: 'config',
          updatedAt: new Date().toISOString(),
        },
      }),
    };

    try {
      const result = payload(await call('list_agent_models', { targetAgent: 'deepseek_harness' }));
      const dsh = result.agents.deepseek_harness;
      assert.strictEqual(dsh.configuredModel, 'deepseek-v4.1-flash');
      assert.strictEqual(dsh.initiatorModel, 'deepseek-v4.1-pro');
      // The receiver default still leads the catalogue and is tagged in place.
      assert.strictEqual(dsh.models[0].id, 'deepseek-v4.1-flash');
      assert.match(dsh.models[0].hint, /configured default/);
      assert.match(result.hint, /sender-side default/);
    } finally {
      saveModelDefaults({}, home, { initiatorModels: {} });
      invalidateConfiguredModelCache();
    }
  });

  test('get_task_summary survives archival with the read-only digest', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 63,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => JSON.parse(res.result.content[0].text);

    const dispatched = payload(
      await call('dispatch_task', { workspaceRoot: process.cwd(),
        targetAgent: 'claude_code',
        title: 'Summarise me',
        prompt: 'do the work',
      }),
    );
    await waitForStatus(dispatched.threadId, 'reviewing');
    // A review decision requires the reviewer role, exactly like a real wake.
    const lease = new TaskRoleLock().tryAcquire(dispatched.threadId, 'reviewer');
    assert.ok(lease);
    payload(await call('approve_task', { threadId: dispatched.threadId, remarks: 'LGTM' }));

    const before = payload(await call('get_task_summary', { threadId: dispatched.threadId }));
    assert.strictEqual(before.archived, false);
    assert.strictEqual(before.finalSummary.kind, 'completed');
    assert.match(before.finalSummary.verdict || '', /LGTM/);

    // Archiving drops the rounds but keeps the digest queryable.
    assert.ok(taskStore.archiveTask(dispatched.threadId));
    const after = payload(await call('get_task_summary', { threadId: dispatched.threadId }));
    assert.strictEqual(after.archived, true);
    assert.strictEqual(after.finalSummary.kind, 'completed');
    assert.match(after.finalSummary.summary, /审阅通过/);

    const details = payload(await call('get_thread_details', { threadId: dispatched.threadId }));
    assert.strictEqual(details.archived, true);
    assert.deepStrictEqual(details.rounds, []);
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
      await call('dispatch_task', { workspaceRoot: process.cwd(),
        targetAgent: 'claude_code',
        title: 'Reviewer output',
        prompt: 'do the work',
      }),
    );
    await waitForStatus(dispatched.threadId, 'reviewing');

    // No wake has been captured for this task yet.
    const missing = payload(await call('get_reviewer_log', { threadId: dispatched.threadId }));
    assert.strictEqual(missing.available, false);
    assert.deepStrictEqual(missing.roundsWithReviewerLog, []);
    assert.match(missing.message, /No reviewer log/);

    // A captured reviewer stream must be served with the same paging contract.
    // New tasks keep it inside their unified task directory; the helper falls
    // back to the legacy logs/ location when no artifactDir is recorded.
    const threadForLog = taskStore.getTask(dispatched.threadId)!;
    const reviewerFile = reviewerLogPath(
      dispatched.threadId,
      1,
      tempDir,
      threadForLog.initiatorAgent,
      threadForLog.artifactDir,
    );
    fs.mkdirSync(path.dirname(reviewerFile), { recursive: true });
    fs.writeFileSync(
      reviewerFile,
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
    configureCodexReviewerFixture();
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 60,
        method: 'tools/call',
        params: { name, arguments: args },
      });
    const payload = (res: any) => {
      assert.ok(res?.result?.content?.[0]?.text, `MCP call failed: ${JSON.stringify(res)}`);
      return JSON.parse(res.result.content[0].text);
    };

    const dispatched = payload(
      await call('dispatch_task', { workspaceRoot: process.cwd(),
        targetAgent: 'codex',
        title: 'Stop polling',
        prompt: 'do the work',
      }),
    );
    // The dispatch reply itself must carry the directive, before anything else.
    assert.strictEqual(dispatched.nextAction, 'END_TURN_AND_WAIT_FOR_CALLBACK');
    assert.match(dispatched.message, /^nextAction=END_TURN_AND_WAIT_FOR_CALLBACK/);
    assert.match(dispatched.stopInstruction, /End your turn immediately/);

    await waitForStatus(dispatched.threadId, 'reviewing');

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
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (threadId)),
    callerSessionId: ('reviewer-' + (threadId)),
      threadId,
      title: 'capsule projection',
      initiatorAgent: 'codex',
      targetAgent: 'claude_code',
      status: 'reviewing',
      currentRound: 3,
      createdAt: now,
      updatedAt: now,
      receiverSessionId: 'session-abcdef12',
      workspace: testWorkspace(tempDir),
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

  test('get_merge_log reads bounded pages from the task-owned log', async () => {
    const threadId = 'thread_merge_log_page'; const now = new Date().toISOString();
    const logPath = path.join(tempDir, 'tasks', threadId, 'merge_log', `${threadId}_merge_1.log`);
    fs.mkdirSync(path.dirname(logPath), { recursive: true }); fs.writeFileSync(logPath, 'abcdefghij');
    taskStore.upsertTask({
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (threadId)),
    callerSessionId: ('reviewer-' + (threadId)), threadId, title: 'merge log', initiatorAgent: 'codex', targetAgent: 'claude_code',
      status: 'reviewing', currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session-abcdef12',
      workspace: testWorkspace(tempDir), summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 }, rounds: [],
      mergeHistory: [{ id: 'one', number: 1, at: now, phase: 'conflict', result: 'failed', logPath }] });
    const res = await mcpServer.handleRpcMessage({ jsonrpc: '2.0', id: 800, method: 'tools/call',
      params: { name: 'get_merge_log', arguments: { threadId, number: 1, offset: 2, limit: 4 } } });
    const page = JSON.parse(res.result.content[0].text);
    assert.equal(page.content, 'cdef'); assert.equal(page.nextOffset, 6); assert.equal(page.size, 10);
  });

  test('get_merge_log resolves decision and legacy file numbers but rejects paths outside the task log', async () => {
    const threadId = 'thread_merge_log_mapping'; const now = new Date().toISOString();
    const dir = path.join(tempDir, 'tasks', threadId, 'merge_log'); fs.mkdirSync(dir, { recursive: true });
    const logPath = path.join(dir, `${threadId}_merge_2.log`); fs.writeFileSync(logPath, 'abcdefghij');
    const decisionLog = path.join(dir, `${threadId}_merge_9.log`); fs.writeFileSync(decisionLog, 'decision context');
    const outside = path.join(tempDir, `${threadId}_merge_2.log`); fs.writeFileSync(outside, 'private');
    taskStore.upsertTask({
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (threadId)),
    callerSessionId: ('reviewer-' + (threadId)), threadId, title: 'merge log', initiatorAgent: 'codex', targetAgent: 'claude_code',
      status: 'reviewing', currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session-abcdef12',
      workspace: testWorkspace(tempDir), summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 }, rounds: [],
      mergeHistory: [
        { id: 'legacy', number: 1, at: now, phase: 'attempt', result: 'started', logPath },
        { id: 'decision', number: 2, at: now, phase: 'decision', result: 'requested', logPath, logNumber: 1 },
        { id: 'outside', number: 3, at: now, phase: 'attempt', result: 'started', logPath: outside, logFileNumber: 2 },
        { id: 'mismatch', number: 4, at: now, phase: 'attempt', result: 'started', logPath, logFileNumber: 1 },
        { id: 'new', number: 5, at: now, phase: 'attempt', result: 'started', logPath: decisionLog, logFileNumber: 9 },
      ] });
    const legacy = await mcpServer.handleToolCall('get_merge_log', { threadId, number: 1, offset: 2, limit: 4 });
    const decision = await mcpServer.handleToolCall('get_merge_log', { threadId, number: 2, offset: 2, limit: 4 });
    assert.equal(legacy.content, 'cdef'); assert.equal(decision.content, 'cdef');
    await assert.rejects(mcpServer.handleToolCall('get_merge_log', { threadId, number: 3 }), /Invalid merge log path/);
    await assert.rejects(mcpServer.handleToolCall('get_merge_log', { threadId, number: 4 }), /Invalid merge log path/);
    await reworkController.requestMergeDecision(threadId, 'Which fix?', 'background', 'suggestion', [], decisionLog);
    const question = taskStore.getTask(threadId)!.mergeHistory!.at(-1)!;
    assert.equal(question.logNumber, 5); assert.equal(question.logFileNumber, 9);
    const context = await mcpServer.handleToolCall('get_merge_log', { threadId, number: question.number, limit: 8 });
    assert.equal(context.content, 'decision');
  });

  test('merge_failed rejects ordinary controls and retry_merge keeps task identity', async () => {
    const threadId = 'thread_merge_failed_control'; const now = new Date().toISOString();
    taskStore.upsertTask({
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: 'reviewer-session', threadId, title: 'merge failure', initiatorAgent: 'codex', targetAgent: 'claude_code', status: 'failed', failure: { stage: 'merge', reason: 'conflict', at: now }, failureRevision: 1,
      currentRound: 1, createdAt: now, updatedAt: now, receiverSessionId: 'session-abcdef12', callerSessionId: 'reviewer-session',
      workspace: testWorkspace(tempDir), summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 }, rounds: [],
      mergeControl: { revision: 1, retryGeneration: 0, consecutiveRetries: 6, failureFingerprint: 'failure' } });
    for (const name of ['approve_task', 'request_rework', 'retry_task', 'retarget_task', 'pause_task']) {
      await assert.rejects(() => mcpServer.handleToolCall(name, { threadId }), /only permits retry_merge or delete_task/);
    }
    const result = await mcpServer.handleToolCall('retry_merge', { threadId });
    assert.equal(result.status, 'reviewing');
    assert.equal(taskStore.getTask(threadId)!.receiverSessionId, 'session-abcdef12');
    assert.equal(taskStore.getTask(threadId)!.mergeControl!.retryGeneration, 1);
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
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (threadId)),
    callerSessionId: ('reviewer-' + (threadId)),
      threadId,
      title: 'token usage',
      initiatorAgent: 'claude_code',
      targetAgent: 'codex',
      status: 'reviewing',
      currentRound: 2,
      createdAt: now,
      updatedAt: now,
      receiverSessionId: 'session-token-01',
      workspace: testWorkspace(tempDir),
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

  test('disposing the current execution controller clears its process singleton', () => {
    const controller = new ExecutionController(taskStore, runner, callbackManager,
      reworkController.getMergeCoordinator(), undefined, undefined, undefined,
      { storageDir: tempDir, standingScanIntervalMs: 0 } as any);
    assert.strictEqual(ExecutionController.getInstance(), controller);
    controller.dispose();
    assert.strictEqual(ExecutionController.getInstance(), undefined);
  });

  test('should execute dispatch_task, get_thread_details, request_rework, and approve_task via tools/call', async () => {
    configureCodexReviewerFixture();
    // 1. Dispatch
    const dispatchCall = {
      jsonrpc: '2.0',
      id: 10,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: { workspaceRoot: process.cwd(),
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
    assert.strictEqual(dispatchData.status, 'REVIEWER_STARTING');
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
    await waitForStatus(threadId, 'reviewing');
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
    await waitForStatus(threadId, 'reviewing');
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
      assert.ok(!toolNames.includes('list_task_templates'));
      await assert.rejects(mcpServer.handleToolCall('list_task_templates', {}), /receiver/i);
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
          arguments: { workspaceRoot: process.cwd(), targetAgent: 'codex', title: 'nested', prompt: 'do more work' },
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

  test('should detect a receiver by the pid of the launched task process when the host scrubs the environment', async () => {
    // Codex hands its MCP servers ONLY the `env` table from config.toml, and
    // DeepSeek Harness scrubs inherited variables, so MESH_TASK_RECEIVER never
    // arrives. The bridge therefore also matches the pid of the CLI it launched.
    // Before this channel existed a receiver could silently dispatch a nested
    // task, which produced chains of tasks each delegating the same blocked brief.
    const savedReceiver = process.env.MESH_TASK_RECEIVER;
    const savedThread = process.env.MESH_TASK_THREAD_ID;
    const savedRound = process.env.MESH_TASK_ROUND;
    const registry = new TaskExecutionRegistry(tempDir);
    const originalPpid = Object.getOwnPropertyDescriptor(process, 'ppid');
    let registration: ReturnType<TaskExecutionRegistry['begin']> | undefined;
    try {
      delete process.env.MESH_TASK_RECEIVER;
      delete process.env.MESH_TASK_THREAD_ID;
      delete process.env.MESH_TASK_ROUND;

      const threadId = 'thread_receiver_by_pid';
      registration = registry.begin(threadId, 2, 'codex', tempDir, 'developer');
      // The MCP server the agent spawns is a child of the CLI the bridge
      // launched, so its ppid is the recorded child pid.
      registry.attachChildPid(4321, registration);
      Object.defineProperty(process, 'ppid', { value: 4321, configurable: true });

      const toolNames = mcpServer.getToolDefinitions().map((tool: any) => tool.name);
      assert.ok(!toolNames.includes('dispatch_task'), 'forwarding tools must stay hidden');
      assert.ok(toolNames.includes('approve_task'));

      const dispatchRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 40,
        method: 'tools/call',
        params: {
          name: 'dispatch_task',
          arguments: { workspaceRoot: process.cwd(), targetAgent: 'codex', title: 'nested', prompt: 'delegate again' },
        },
      });
      assert.match(dispatchRes.error.message, /receiver/i);
      assert.match(dispatchRes.error.message, /thread_receiver_by_pid/);

      // The receiver must not drive its own task's review loop either.
      const reworkRes = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 41,
        method: 'tools/call',
        params: {
          name: 'request_rework',
          arguments: { threadId, feedback: 'again' },
        },
      });
      assert.match(reworkRes.error.message, /receiver|not its initiator/i);
    } finally {
      if (originalPpid) Object.defineProperty(process, 'ppid', originalPpid);
      if (registration) registry.end(registration);
      registry.dispose();
      if (savedReceiver === undefined) delete process.env.MESH_TASK_RECEIVER;
      else process.env.MESH_TASK_RECEIVER = savedReceiver;
      if (savedThread === undefined) delete process.env.MESH_TASK_THREAD_ID;
      else process.env.MESH_TASK_THREAD_ID = savedThread;
      if (savedRound === undefined) delete process.env.MESH_TASK_ROUND;
      else process.env.MESH_TASK_ROUND = savedRound;
    }
  });

  test('should not treat an unrelated process as a receiver when no execution record matches its pid', async () => {
    const savedReceiver = process.env.MESH_TASK_RECEIVER;
    const originalPpid = Object.getOwnPropertyDescriptor(process, 'ppid');
    try {
      delete process.env.MESH_TASK_RECEIVER;
      Object.defineProperty(process, 'ppid', { value: 987654, configurable: true });

      const toolNames = mcpServer.getToolDefinitions().map((tool: any) => tool.name);
      assert.ok(toolNames.includes('dispatch_task'), 'a normal caller keeps the forwarding tools');
    } finally {
      if (originalPpid) Object.defineProperty(process, 'ppid', originalPpid);
      if (savedReceiver === undefined) delete process.env.MESH_TASK_RECEIVER;
      else process.env.MESH_TASK_RECEIVER = savedReceiver;
    }
  });

  test('should never treat a reviewer or bootstrap round as a task receiver', async () => {
    // A reviewer round is registered against the SAME thread and also binds the
    // reviewer CLI pid. If the pid channel matched those records, the conversation
    // whose job is to confirm the task would lose `dispatch_task`.
    const savedReceiver = process.env.MESH_TASK_RECEIVER;
    const registry = new TaskExecutionRegistry(tempDir);
    const originalPpid = Object.getOwnPropertyDescriptor(process, 'ppid');
    const registrations: Array<ReturnType<TaskExecutionRegistry['begin']>> = [];
    try {
      delete process.env.MESH_TASK_RECEIVER;
      for (const [role, pid, threadId] of [
        ['reviewer', 5101, 'thread_reviewer_role'],
        ['bootstrap', 5102, 'thread_bootstrap_role'],
        ['maintenance', 5103, 'thread_maintenance_role'],
      ] as const) {
        const registration = registry.begin(threadId, 1, 'codex', tempDir, role);
        registry.attachChildPid(pid, registration);
        registrations.push(registration);
        Object.defineProperty(process, 'ppid', { value: pid, configurable: true });
        assert.strictEqual(registry.findByChildPid(pid), undefined, `${role} must not resolve as a receiver`);
      }
      Object.defineProperty(process, 'ppid', { value: 5101, configurable: true });
      const toolNames = mcpServer.getToolDefinitions().map((tool: any) => tool.name);
      assert.ok(toolNames.includes('dispatch_task'), 'a reviewer keeps its forwarding tools');
    } finally {
      if (originalPpid) Object.defineProperty(process, 'ppid', originalPpid);
      for (const registration of registrations) registry.end(registration);
      registry.dispose();
      if (savedReceiver === undefined) delete process.env.MESH_TASK_RECEIVER;
      else process.env.MESH_TASK_RECEIVER = savedReceiver;
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
    configureCodexReviewerFixture();
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
        arguments: { workspaceRoot: process.cwd(),
          targetAgent: 'codex',
          title: 'Retry in place',
          prompt: 'Implement the extension exactly as designed',
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

    await waitForStatus(threadId, 'reviewing', 5000);
    const after = taskStore.getTask(threadId)!;
    assert.strictEqual(after.rounds.length, 1, 'retry must not append a second round');
    assert.strictEqual(after.rounds[0].prompt, originalPrompt);
  });

  test('MCP v2 retry_task and resume_task reject a live child without clearing its run or generation', async () => {
    const controller = new ExecutionController(taskStore, runner, callbackManager,
      reworkController.getMergeCoordinator(), undefined, undefined, undefined,
      { storageDir: tempDir, standingScanIntervalMs: 0 } as any);
    let child: ReturnType<typeof spawn> | undefined;
    const oldExecute = runner.execute;
    let executions = 0;
    runner.execute = async (...args: any[]) => { executions++; return oldExecute.apply(runner, args as any); };
    try {
      child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      await new Promise<void>((resolve, reject) => { child!.once('error', reject); child!.once('spawn', resolve); });
      const pid = child.pid!;
      const birthId = getProcessBirthId(pid);
      assert.ok(birthId);
      const makeV2 = (threadId: string, status: 'failed' | 'paused') => ({
        threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code',
        status, currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
        receiverSessionId: `receiver-${threadId}`, receiverSessionCaptured: true,
        reviewerSessionId: `reviewer-${threadId}`, reviewFlow: 'dedicated',
        workspace: testWorkspace(process.cwd()),
        reviewBootstrap: { phase: 'confirmed', attempts: 1 }, rounds: [],
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        execution: {
          version: 2, revision: 1, operationId: `op-${threadId}`, phase: 'develop', round: 1,
          generation: 7, attempt: 1, autoRetriesUsed: 0,
          budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 25, maxAutoRetries: 2 },
          run: {
            id: `run-${threadId}`, role: 'developer', owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
            child: { pid, birthId }, subtrees: [{ root: { pid, birthId }, descendants: [] }],
            step: 'executing', state: 'active', startedAt: new Date().toISOString(),
            spawnIntentAt: new Date().toISOString(), registeredAt: new Date().toISOString(),
          },
        },
      });
      const retryThread: any = makeV2('mcp-v2-live-retry', 'failed');
      const resumeThread: any = makeV2('mcp-v2-live-resume', 'paused');
      taskStore.upsertTask(retryThread);
      taskStore.upsertTask(resumeThread);

      for (const [name, threadId] of [['retry_task', retryThread.threadId], ['resume_task', resumeThread.threadId]] as const) {
        const response = await mcpServer.handleRpcMessage({
          jsonrpc: '2.0', id: 801, method: 'tools/call', params: { name, arguments: { threadId } },
        });
        assert.equal(response.error?.code, -32000, `${name} reports an actionable rejection`);
        assert.match(response.error?.message || '', /previous run exit is not confirmed/);
        const after = taskStore.reloadTask(threadId)!;
        assert.equal(after.execution?.run?.id, `run-${threadId}`);
        assert.equal(after.execution?.generation, 7);
      }
      assert.equal(executions, 0, 'neither control path starts a replacement runner');
    } finally {
      runner.execute = oldExecute;
      controller.dispose();
      if (child && child.exitCode === null && child.signalCode === null) {
        const closed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
        try { child.kill('SIGKILL'); } catch {}
        await closed;
      }
    }
  });

  test('MCP decision keeps a v2 spawn-intent run until owner/child exit acknowledgement; answer is durable next', async () => {
    const controller = new ExecutionController(taskStore, runner, callbackManager,
      reworkController.getMergeCoordinator(), undefined, undefined, undefined,
      { storageDir: tempDir, standingScanIntervalMs: 0 } as any);
    const oldReceiver = process.env.MESH_TASK_RECEIVER;
    const oldThread = process.env.MESH_TASK_THREAD_ID;
    const children: ReturnType<typeof spawn>[] = [];
    const ownerBirthId = getProcessBirthId(process.pid);
    assert.ok(ownerBirthId);
    const makeRun = (threadId: string, child?: { pid: number; birthId: string }) => ({
      threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code',
      status: 'running', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      receiverSessionId: `receiver-${threadId}`, receiverSessionCaptured: true,
      reviewerSessionId: `reviewer-${threadId}`, reviewFlow: 'dedicated',
      reviewBootstrap: { phase: 'confirmed', attempts: 1 }, workspace: testWorkspace(process.cwd()),
      rounds: [{ round: 1, type: 'initial', prompt: 'work', status: 'running', durationMs: 0, timestamp: new Date().toISOString() }],
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      execution: {
        version: 2, revision: 1, operationId: `op-${threadId}`, phase: 'develop', round: 1,
        generation: 3, attempt: 1, autoRetriesUsed: 0,
        budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 10, maxAutoRetries: 2 },
        run: {
          id: `run-${threadId}`, role: 'developer', owner: { pid: process.pid, birthId: ownerBirthId },
          ...(child ? { child: { pid: child.pid, birthId: child.birthId }, subtrees: [{ root: child, descendants: [] }], registeredAt: new Date().toISOString() } : {}),
          step: child ? 'spawning' : 'spawning', state: 'starting', spawnIntentAt: new Date().toISOString(), startedAt: new Date().toISOString(),
        },
      },
    });
    try {
      process.env.MESH_TASK_RECEIVER = '1';
      const noBindingId = 'mcp-v2-decision-no-binding';
      process.env.MESH_TASK_THREAD_ID = noBindingId;
      taskStore.upsertTask(makeRun(noBindingId) as any);
      const request = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0', id: 821, method: 'tools/call',
        params: { name: 'request_task_decision', arguments: { threadId: noBindingId, question: 'Need direction', resumePoint: 'continue_develop' } },
      });
      assert.equal(request.error, undefined);
      let persisted = taskStore.reloadTask(noBindingId)!;
      assert.equal(persisted.status, 'wait_decision');
      assert.equal(persisted.execution?.run?.id, `run-${noBindingId}`);
      assert.equal(persisted.execution?.run?.state, 'stopping');
      assert.equal(persisted.execution?.run?.unconfirmed, true, 'spawn intent without a bound child is not treated as no-spawn proof');

      const noBindingDecisionId = persisted.decision!.id;
      const answerScript = `const {TaskStore}=require(${JSON.stringify(require.resolve('../bridge/task-store'))});
        const {ExecutionController}=require(${JSON.stringify(require.resolve('../bridge/execution-controller'))});
        const store=new TaskStore(${JSON.stringify(tempDir)});
        const controller=new ExecutionController(store,{execute:async()=>{throw new Error('unexpected spawn')}},{},{},undefined,undefined,undefined,{storageDir:${JSON.stringify(tempDir)},standingScanIntervalMs:0});
        controller.submitTaskDecision(${JSON.stringify(noBindingId)},${JSON.stringify(noBindingDecisionId)},'Proceed carefully').then(()=>{process.stdout.write('answered');controller.dispose();store.dispose()}).catch(e=>{process.stderr.write(String(e.stack||e));controller.dispose();store.dispose();process.exitCode=1});`;
      const answerProcess = spawnSync(process.execPath, ['-e', answerScript], { encoding: 'utf8', windowsHide: true });
      assert.equal(answerProcess.status, 0, answerProcess.stderr);
      assert.equal(answerProcess.stdout, 'answered', 'a separate real control process may durably answer without clearing the uncertain run');
      persisted = taskStore.reloadTask(noBindingId)!;
      assert.equal(persisted.execution?.run?.id, `run-${noBindingId}`);
      assert.equal(persisted.execution?.next?.sourceRunId, `run-${noBindingId}`);
      assert.equal(persisted.status, 'wait_decision', 'answer is durable while previous run remains protected');

      const child = spawn(process.execPath, ['-e', 'setInterval(()=>{},1000)'], { stdio: 'ignore', windowsHide: true });
      children.push(child);
      await new Promise<void>((resolve, reject) => { child.once('error', reject); child.once('spawn', resolve); });
      const childBirthId = getProcessBirthId(child.pid!);
      assert.ok(childBirthId);
      const childThreadId = 'mcp-v2-decision-bound-child';
      taskStore.upsertTask(makeRun(childThreadId, { pid: child.pid!, birthId: childBirthId }) as any);
      process.env.MESH_TASK_THREAD_ID = childThreadId;
      const childRequest = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0', id: 822, method: 'tools/call',
        params: { name: 'request_task_decision', arguments: { threadId: childThreadId, question: 'Confirm stop', resumePoint: 'continue_develop' } },
      });
      assert.equal(childRequest.error, undefined);
      assert.equal(probeProcessIdentity(child.pid!, childBirthId), 'exited', 'controlled child was stopped and positively observed exited');
      const childState = taskStore.reloadTask(childThreadId)!;
      assert.equal(childState.execution?.run?.id, `run-${childThreadId}`, 'controller-owner exit/release evidence is still required');
      assert.equal(childState.execution?.run?.unconfirmed, true);
      const late = await registerRunChildV2({
        taskStore, threadId: childThreadId,
        identity: { runId: `run-${childThreadId}`, operationId: childState.execution!.operationId, generation: 3, phase: 'develop', round: 1 },
        childPid: child.pid!, childBirthId,
      });
      assert.equal(late.success, false, 'late callback cannot rebind a stopping decision run');
      assert.equal(taskStore.reloadTask(childThreadId)?.execution?.run?.id, `run-${childThreadId}`);
    } finally {
      for (const child of children) {
        if (child.exitCode === null && child.signalCode === null) {
          const closed = new Promise<void>((resolve) => child.once('close', () => resolve()));
          try { child.kill('SIGKILL'); } catch {}
          await closed;
        }
      }
      if (oldReceiver === undefined) delete process.env.MESH_TASK_RECEIVER; else process.env.MESH_TASK_RECEIVER = oldReceiver;
      if (oldThread === undefined) delete process.env.MESH_TASK_THREAD_ID; else process.env.MESH_TASK_THREAD_ID = oldThread;
      controller.dispose();
    }
  });

  test('v2 decision cannot replace a first review-run rework handoff', async () => {
    const controller = new ExecutionController(taskStore, runner, callbackManager,
      reworkController.getMergeCoordinator(), undefined, undefined, undefined,
      { storageDir: tempDir, standingScanIntervalMs: 0 } as any);
    const ownerBirthId = getProcessBirthId(process.pid)!;
    const createReview = (threadId: string, pendingDecision = false) => {
      const runId = `review-run-${threadId}`;
      const decision = pendingDecision ? {
        id: `decision-${threadId}`, sourceRunId: runId, operationId: `op-${threadId}`, generation: 4,
        phase: 'review', round: 1, question: 'change or approve?', resumePoint: 'continue_review',
        createdAt: new Date().toISOString(),
      } : undefined;
      return {
        threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code',
        status: pendingDecision ? 'wait_decision' : 'reviewing', currentRound: 1,
        createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
        reviewerSessionId: `reviewer-${threadId}`, reviewerSessionCaptured: true, reviewFlow: 'dedicated',
        reviewBootstrap: { phase: 'confirmed', attempts: 1 }, workspace: testWorkspace(process.cwd()),
        rounds: [{ round: 1, type: 'initial', prompt: 'review work', status: 'completed', durationMs: 1, timestamp: new Date().toISOString() }],
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 1 },
        ...(decision ? { decision, decisions: [decision] } : {}),
        execution: {
          version: 2, revision: 1, operationId: `op-${threadId}`, phase: 'review', round: 1,
          generation: 4, attempt: 0, autoRetriesUsed: 0,
          budget: { limitMs: 120 * 60 * 1000, spentActiveMs: 0, maxAutoRetries: 2 },
          next: { phase: 'develop', round: 2, sourceRunId: runId },
          run: { id: runId, role: 'reviewer', owner: { pid: process.pid, birthId: ownerBirthId },
            state: pendingDecision ? 'stopping' : 'active', step: 'executing', startedAt: new Date().toISOString(), registeredAt: new Date().toISOString() },
        },
      };
    };
    try {
      const requestId = 'v2-decision-existing-rework';
      taskStore.upsertTask(createReview(requestId) as any);
      await assert.rejects(
        controller.requestTaskDecision(requestId, { question: 'Continue reviewing?', resumePoint: 'continue_review' }),
        /already has a durable handoff/,
      );
      let saved = taskStore.reloadTask(requestId)!;
      assert.deepEqual(saved.execution?.next, { phase: 'develop', round: 2, sourceRunId: `review-run-${requestId}` });
      assert.equal(saved.status, 'reviewing');
      assert.equal(saved.decision, undefined);

      const submitId = 'v2-submit-existing-rework';
      taskStore.upsertTask(createReview(submitId, true) as any);
      await assert.rejects(controller.submitTaskDecision(submitId, `decision-${submitId}`, 'continue review'), /cannot replace an existing durable handoff/);
      saved = taskStore.reloadTask(submitId)!;
      assert.deepEqual(saved.execution?.next, { phase: 'develop', round: 2, sourceRunId: `review-run-${submitId}` });
      assert.equal(saved.execution?.run?.id, `review-run-${submitId}`);
      assert.equal(saved.decision?.answer, undefined);
    } finally {
      controller.dispose();
    }
  });

  test('request_rework flags a round whose instruction repeats an earlier one', async () => {
    configureCodexReviewerFixture();
    const prompt = 'Implement the sleeve bloom extension as designed';
    const dispatchRes = await mcpServer.handleRpcMessage({
      jsonrpc: '2.0',
      id: 50,
      method: 'tools/call',
      params: {
        name: 'dispatch_task',
        arguments: { workspaceRoot: process.cwd(),
          targetAgent: 'codex',
          title: 'Repeat instruction',
          prompt,
          callerSessionId: 'caller_mcp_repeat',
        },
      },
    });
    const threadId = JSON.parse(dispatchRes.result.content[0].text).threadId;
    await waitForStatus(threadId, 'reviewing');

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

  test('rejects self-dispatch instead of parking a task that can never be reviewed', async () => {
    const call = async (name: string, args: any) =>
      mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 72,
        method: 'tools/call',
        params: { name, arguments: args },
      });

    // A caller naming itself as the receiver used to create a task whose review
    // callback could never be delivered (the caller session is busy running the
    // round). It must be an error, and nothing may reach the ledger.
    const rejected = await call('dispatch_task', { workspaceRoot: process.cwd(),
      targetAgent: 'codex',
      initiatorAgent: 'codex',
      title: 'Self dispatch',
      prompt: 'do the work',
    });
    assert.ok(rejected.error, 'a self-dispatch must be an error, not a queued task');
    assert.match(
      String(rejected.error.message),
      /configure a sender-side reviewer model/,
    );
    assert.strictEqual(taskStore.getAllTasks().length, 0);

    // The synchronous call follows the same rule, using the caller identity the
    // bridge injects into each agent's MCP process.
    const previous = process.env.MESH_CALLER_AGENT;
    process.env.MESH_CALLER_AGENT = 'codex';
    try {
      const invoked = await call('invoke_agent', { targetAgent: 'codex', prompt: 'say hi' });
      assert.ok(invoked.error);
      assert.match(String(invoked.error.message), /cannot send work to itself/);
    } finally {
      if (previous === undefined) delete process.env.MESH_CALLER_AGENT;
      else process.env.MESH_CALLER_AGENT = previous;
    }
});
  test('reviewer bootstrap confirms the prepared task instead of creating a new one', async () => {
    const home = getBridgeHome();
    const threadId = 'thread_bootstrap_confirm';
    const token = 'bootstrap-token-1';
    taskStore.upsertTask({
      threadId,
      title: 'Prepared task',
      initiatorAgent: 'codex',
      targetAgent: 'claude_code',
      status: 'queued',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: 'receiver-bootstrap-1',
      workspace: testWorkspace(tempDir),
      reviewFlow: 'dedicated',
      reviewerModel: 'gpt-6-pro',
      targetModel: 'sonnet',
      reviewBootstrap: { phase: 'pending', attempts: 0 },
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'do it',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
        },
      ],
    });
    assert.strictEqual(
      writeReviewerBootstrapToken(
        { threadId, token, agent: 'codex', createdAt: new Date().toISOString() },
        home,
      ),
      true,
    );

    const prevBootstrap = process.env.MESH_REVIEW_BOOTSTRAP;
    const prevToken = process.env.MESH_REVIEW_BOOTSTRAP_TOKEN;
    const prevThread = process.env.MESH_REVIEW_THREAD_ID;
    const prevCaller = process.env.MESH_CALLER_AGENT;
    process.env.MESH_REVIEW_BOOTSTRAP = '1';
    process.env.MESH_REVIEW_BOOTSTRAP_TOKEN = token;
    process.env.MESH_REVIEW_THREAD_ID = threadId;
    process.env.MESH_CALLER_AGENT = 'codex';
    try {
      const res = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 90,
        method: 'tools/call',
        params: { name: 'dispatch_task', arguments: {} },
      });
      assert.ok(res.result, JSON.stringify(res));
      const payload = JSON.parse(res.result.content[0].text);
      assert.strictEqual(payload.status, 'DISPATCHED');
      assert.strictEqual(payload.threadId, threadId);
      assert.strictEqual(taskStore.getAllTasks().length, 1, 'no second task was created');

      const stored = taskStore.getTask(threadId)!;
      assert.strictEqual(stored.reviewBootstrap?.phase, 'confirmed');
      assert.strictEqual(stored.status, 'queued');
      assert.strictEqual(readReviewerBootstrapToken(threadId, home), undefined);
    } finally {
      if (prevBootstrap === undefined) delete process.env.MESH_REVIEW_BOOTSTRAP;
      else process.env.MESH_REVIEW_BOOTSTRAP = prevBootstrap;
      if (prevToken === undefined) delete process.env.MESH_REVIEW_BOOTSTRAP_TOKEN;
      else process.env.MESH_REVIEW_BOOTSTRAP_TOKEN = prevToken;
      if (prevThread === undefined) delete process.env.MESH_REVIEW_THREAD_ID;
      else process.env.MESH_REVIEW_THREAD_ID = prevThread;
      if (prevCaller === undefined) delete process.env.MESH_CALLER_AGENT;
      else process.env.MESH_CALLER_AGENT = prevCaller;
      clearReviewerBootstrapToken(threadId, home);
    }
  });

  test('reviewer bootstrap still confirms when the host scrubs the MCP server environment', async () => {
    // Codex passes ONLY the `env` table from config.toml to the MCP servers it
    // spawns, and DeepSeek Harness scrubs inherited variables: MESH_REVIEW_*
    // never arrives. The launched reviewer pid must identify the process instead.
    const home = getBridgeHome();
    const threadId = 'thread_bootstrap_host_pid';
    taskStore.upsertTask({
      threadId,
      title: 'Prepared task behind a closed-env host',
      initiatorAgent: 'codex',
      targetAgent: 'claude_code',
      status: 'queued',
      currentRound: 1,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      receiverSessionId: 'receiver-bootstrap-pid',
      workspace: testWorkspace(tempDir),
      reviewFlow: 'dedicated',
      reviewerModel: 'gpt-6-pro',
      targetModel: 'sonnet',
      reviewBootstrap: { phase: 'pending', attempts: 0 },
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [
        {
          round: 1,
          type: 'initial',
          prompt: 'do it',
          status: 'running',
          durationMs: 0,
          timestamp: new Date().toISOString(),
        },
      ],
    });
    assert.strictEqual(
      writeReviewerBootstrapToken(
        {
          threadId,
          token: 'bootstrap-token-pid',
          agent: 'codex',
          createdAt: new Date().toISOString(),
          hostPid: process.ppid,
        },
        home,
      ),
      true,
    );

    const saved = ['MESH_REVIEW_BOOTSTRAP', 'MESH_REVIEW_BOOTSTRAP_TOKEN', 'MESH_REVIEW_THREAD_ID'].map(
      (name) => [name, process.env[name]] as [string, string | undefined],
    );
    for (const [name] of saved) delete process.env[name];
    const prevCaller = process.env.MESH_CALLER_AGENT;
    process.env.MESH_CALLER_AGENT = 'codex';
    try {
      const res = await mcpServer.handleRpcMessage({
        jsonrpc: '2.0',
        id: 91,
        method: 'tools/call',
        params: { name: 'dispatch_task', arguments: {} },
      });
      assert.ok(res.result, JSON.stringify(res));
      const payload = JSON.parse(res.result.content[0].text);
      assert.strictEqual(payload.status, 'DISPATCHED');
      assert.strictEqual(payload.threadId, threadId);
      assert.strictEqual(taskStore.getAllTasks().length, 1, 'no second task was created');

      const stored = taskStore.getTask(threadId)!;
      assert.strictEqual(stored.reviewBootstrap?.phase, 'confirmed');
      assert.strictEqual(stored.status, 'queued');
      assert.strictEqual(readReviewerBootstrapToken(threadId, home), undefined);
    } finally {
      for (const [name, value] of saved) {
        if (value === undefined) delete process.env[name];
        else process.env[name] = value;
      }
      if (prevCaller === undefined) delete process.env.MESH_CALLER_AGENT;
      else process.env.MESH_CALLER_AGENT = prevCaller;
      clearReviewerBootstrapToken(threadId, home);
    }
  });

});
