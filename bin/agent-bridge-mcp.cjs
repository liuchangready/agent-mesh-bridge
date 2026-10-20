#!/usr/bin/env node

/**
 * Agent Mesh Bridge - MCP Stdio Server Entrypoint
 */

const path = require('path');
const fs = require('fs');

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
const { MCPServer } = require(path.join(outBridgeDir, 'mcp-server'));

try {
  const taskStore = TaskStore.getInstance();
  const detector = new AgentDetector();
  const resolver = new ActiveAgentResolver(detector);
  const runner = new AutonomousRunner(detector, undefined, undefined, taskStore);
  const callbackManager = new CallbackManager(detector);
  const dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector);
  const reworkController = new ReworkController(taskStore, runner, callbackManager, detector);

  const server = new MCPServer(dispatcher, reworkController, taskStore, detector, resolver, runner);

  server.startStdio();
} catch (err) {
  console.error('[agent-bridge-mcp] Fatal server startup error:', err);
  process.exit(1);
}
