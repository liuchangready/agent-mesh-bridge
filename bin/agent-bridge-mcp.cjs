#!/usr/bin/env node

/**
 * Agent Mesh Bridge - MCP Stdio Server Entrypoint
 */

const path = require('path');
const fs = require('fs');
const os = require('os');

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

const { TaskStore } = require(path.join(outBridgeDir, 'task-store'));
const { AgentDetector } = require(path.join(outBridgeDir, 'agent-detector'));
const { ActiveAgentResolver } = require(path.join(outBridgeDir, 'active-agent-resolver'));
const { AutonomousRunner } = require(path.join(outBridgeDir, 'autonomous-runner'));
const { CallbackManager } = require(path.join(outBridgeDir, 'callback-manager'));
const { AsyncDispatcher } = require(path.join(outBridgeDir, 'async-dispatcher'));
const { ReworkController } = require(path.join(outBridgeDir, 'rework-controller'));
const { ExecutionController } = require(path.join(outBridgeDir, 'execution-controller'));
const { MCPServer } = require(path.join(outBridgeDir, 'mcp-server'));
const { inspectV2RuntimeActivation, loadShutdownPreflightSnapshot } = require(path.join(outBridgeDir, 'shutdown-preflight'));

function createBridgeHost(options = {}) {
  const startupAdmission = options.v2StartupAdmission || { ready: true, blockers: [] };
  const taskStore = options.taskStore || TaskStore.getInstance();
  const detector = options.detector || new AgentDetector();
  const resolver = options.resolver || new ActiveAgentResolver(detector);
  const runner = options.runner || new AutonomousRunner(detector, undefined, undefined, taskStore);
  const callbackManager = options.callbackManager || new CallbackManager(detector);
  callbackManager.setTaskStore(taskStore);
  callbackManager.setAutonomousRunner(runner);
  const registry = options.registry;
  const dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector, registry);
  const reworkController = new ReworkController(taskStore, runner, callbackManager, detector, registry, false, true, startupAdmission.ready);
  const executionController = new ExecutionController(
    taskStore,
    runner,
    callbackManager,
    reworkController.getMergeCoordinator ? reworkController.getMergeCoordinator() : undefined,
    undefined,
    undefined,
    registry,
    { ...(options.executionControllerOptions || {}), requireV2Execution: true,
      ...(!startupAdmission.ready ? { standingScanIntervalMs: 0 } : {}) },
  );
  // Admission is checked before enabling dispatch or the durable recovery scan.
  if (startupAdmission.ready) dispatcher.enableV2Dispatch();
  const reason = startupAdmission.ready ? undefined : (startupAdmission.blockers || []).slice(0, 5).map((item) => `${item.code}: ${item.evidence}`).join(' | ');
  const server = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner, true, reason);
  return { taskStore, detector, resolver, runner, callbackManager, registry, dispatcher, reworkController, executionController, server, startupAdmission };
}

function start() {
  try {
    const packageRoot = path.resolve(__dirname, '..');
    const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
    const storageDir = path.join(os.homedir(), '.agent-mesh-bridge');
    const snapshot = loadShutdownPreflightSnapshot(storageDir);
    const startupAdmission = inspectV2RuntimeActivation({ ...snapshot, storageDir, components: [
      { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
      { component: 'standalone-mcp', version, packageRoot, path: path.resolve(__dirname, 'agent-bridge-mcp.cjs') },
    ] });
    if (!startupAdmission.ready) console.error('[agent-bridge-mcp] V2 dispatch and recovery are blocked by cutover preflight:', startupAdmission.blockers);
    createBridgeHost({ v2StartupAdmission: startupAdmission }).server.startStdio();
  } catch (err) {
    console.error('[agent-bridge-mcp] Fatal server startup error:', err);
    process.exit(1);
  }
}

module.exports = { createBridgeHost };
if (require.main === module) start();
