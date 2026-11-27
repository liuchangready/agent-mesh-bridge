#!/usr/bin/env node

/**
 * Agent Mesh Bridge - MCP Stdio Server Entrypoint
 */

const path = require('path');
const fs = require('fs');
const os = require('os');
const { isMainThread, parentPort, workerData } = require('node:worker_threads');

const profileIndex = process.argv.indexOf('--profile');
if (profileIndex !== -1) {
  const profile = process.argv[profileIndex + 1];
  if (profile !== 'mesh-agent') {
    console.error('[agent-bridge-mcp] Unknown server profile.');
    process.exit(2);
  }
  process.env.MESH_MCP_PROFILE = profile;
}

// Locate compiled bridge modules
const outBridgeDir = path.resolve(__dirname, '..', 'out', 'bridge');

if (!fs.existsSync(outBridgeDir)) {
  console.error('[agent-bridge-mcp] Error: Compiled output not found at ' + outBridgeDir);
  console.error(
    '[agent-bridge-mcp] Please run "npm run compile" in ' + path.resolve(__dirname, '..'),
  );
  process.exit(1);
}

// Check for --caller argument to identify initiating agent
const callerIdx = process.argv.indexOf('--caller');
if (callerIdx !== -1 && process.argv[callerIdx + 1]) {
  process.env.MESH_CALLER_AGENT = process.argv[callerIdx + 1];
}

function readRuntimeAdmission(storageDir) {
  const { inspectV2RuntimeActivation, prepareRuntimePreflightSnapshot } = require(path.join(outBridgeDir, 'shutdown-preflight'));
  const packageRoot = path.resolve(__dirname, '..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  return inspectV2RuntimeActivation({ ...prepareRuntimePreflightSnapshot(storageDir), storageDir, components: [
    { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
    { component: 'standalone-mcp', version, packageRoot, path: path.resolve(__dirname, 'agent-bridge-mcp.cjs') },
  ] });
}

function createBridgeHost(options = {}) {
  const { TaskStore } = require(path.join(outBridgeDir, 'task-store'));
  const { AgentDetector } = require(path.join(outBridgeDir, 'agent-detector'));
  const { ActiveAgentResolver } = require(path.join(outBridgeDir, 'active-agent-resolver'));
  const { AutonomousRunner } = require(path.join(outBridgeDir, 'autonomous-runner'));
  const { CallbackManager } = require(path.join(outBridgeDir, 'callback-manager'));
  const { AsyncDispatcher } = require(path.join(outBridgeDir, 'async-dispatcher'));
  const { ReworkController } = require(path.join(outBridgeDir, 'rework-controller'));
  const { ExecutionController } = require(path.join(outBridgeDir, 'execution-controller'));
  const { MCPServer } = require(path.join(outBridgeDir, 'mcp-server'));
  const taskStore = options.taskStore || TaskStore.getInstance();
  const startupAdmission = options.v2StartupAdmission || readRuntimeAdmission(path.dirname(taskStore.getStoragePath()));
  const detector = options.detector || new AgentDetector();
  const resolver = options.resolver || new ActiveAgentResolver(detector);
  const runner = options.runner || new AutonomousRunner(detector, undefined, undefined, taskStore);
  const callbackManager = options.callbackManager || new CallbackManager(detector);
  callbackManager.setTaskStore(taskStore);
  callbackManager.setAutonomousRunner(runner);
  const registry = options.registry;
  const reworkController = new ReworkController(taskStore, callbackManager, startupAdmission.ready);
  const executionController = new ExecutionController(
    taskStore,
    runner,
    callbackManager,
    reworkController.getMergeCoordinator ? reworkController.getMergeCoordinator() : undefined,
    registry,
    { ...(options.executionControllerOptions || {}), ownsRuns: false, admissionEnabled: startupAdmission.ready },
  );
  const dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, registry,
    { admitted: startupAdmission.ready });
  const reason = startupAdmission.ready ? undefined :
    (startupAdmission.blockers || []).slice(0, 5).map((item) => `${item.code}: ${item.evidence}`).join(' | ') ||
    'runtime component preflight has not passed.';
  const server = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner, reason, true);
  return { taskStore, detector, resolver, runner, callbackManager, registry, dispatcher, reworkController, executionController, server, startupAdmission };
}

function start() {
  const storageDir = path.join(os.homedir(), '.agent-mesh-bridge');
  if (isMainThread) {
    const { startMcpTransport } = require(path.join(outBridgeDir, 'standalone-mcp-transport'));
    void startMcpTransport(path.resolve(__dirname, '..'), storageDir).catch(err => {
      console.error('[agent-bridge-mcp] Fatal server startup error:', err);
      process.exit(1);
    });
    return;
  }

  const { readMcpBuild } = require(path.join(outBridgeDir, 'standalone-mcp-status'));
  const build = readMcpBuild(path.resolve(__dirname, '..'));
  if (build.fingerprint !== workerData.fingerprint) throw new Error('MCP build changed while loading');
  const startupAdmission = readRuntimeAdmission(storageDir);
  if (!startupAdmission.ready && !workerData.replacement) {
    console.error('[agent-bridge-mcp] V2 dispatch and recovery are blocked by cutover preflight:', startupAdmission.blockers);
  }
  const { server } = createBridgeHost({ v2StartupAdmission: startupAdmission });
  if (workerData.clientState) server.restoreClientState(workerData.clientState);
  const handle = (request, send) => server.handleStdioMessage(request, message => {
    if (request?.method === 'initialize' && message.result?.serverInfo) {
      message.result.serverInfo.version = build.version;
    }
    send(message);
  });
  void (async () => {
    for (const request of workerData.handshake) await handle(request, () => {});
    parentPort.on('message', async ({ key, request }) => {
      if (request === undefined) {
        parentPort.postMessage({ key, clientState: server.getClientState() });
        return;
      }
      try {
        const messages = [];
        await handle(request, message => messages.push(message));
        parentPort.postMessage({ key, messages });
      } catch (error) {
        parentPort.postMessage({ key, messages: [{ jsonrpc: '2.0', id: request?.id ?? null,
          error: { code: -32603, message: error.message || 'Internal MCP server error' } }] });
      }
    });
    parentPort.postMessage({ admission: { ready: startupAdmission.ready, blockers: startupAdmission.blockers.slice(0, 30) } });
  })().catch(error => { throw error; });
}

module.exports = { createBridgeHost };
if (require.main === module) start();
