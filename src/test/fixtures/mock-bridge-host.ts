import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import childProcess from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fixtureRoot, event } from './mock-agent-protocol';
import { TaskStore } from '../../bridge/task-store';
import { AgentDetector } from '../../bridge/agent-detector';
import { ActiveAgentResolver } from '../../bridge/active-agent-resolver';
import { AutonomousRunner } from '../../bridge/autonomous-runner';
import { CallbackManager } from '../../bridge/callback-manager';
import { AsyncDispatcher } from '../../bridge/async-dispatcher';
import { ReworkController } from '../../bridge/rework-controller';
import { ExecutionController } from '../../bridge/execution-controller';
import { MCPServer } from '../../bridge/mcp-server';
import { TaskExecutionRegistry } from '../../bridge/task-execution-registry';

const root = fixtureRoot();
const storage = path.join(root, 'home', '.agent-mesh-bridge');
if (path.resolve(process.env.AGENT_MESH_BRIDGE_HOME || '') !== path.resolve(storage)) {
  throw new Error('Refusing a non-fixture bridge home');
}
if (path.resolve(os.homedir(), '.agent-mesh-bridge') !== path.resolve(storage)) throw new Error('Default home escaped the fixture');
console.log = (...args: unknown[]) => console.error(...args);
console.info = console.log;

// The only substitution is the external Codex program. Keep its production
// argv/env, process lifetime, streams, assessment, callbacks and MCP unchanged.
const originalSpawn = childProcess.spawn;
(childProcess as any).spawn = (command: string, args: string[], options: any) => {
  if (command === process.execPath && args.includes('exec') && args.includes('--ask-for-approval')) {
    const launchId = randomUUID();
    const child = originalSpawn(process.execPath, [path.join(__dirname, 'mock-agent.js'), ...args],
      { ...options, env: { ...options.env, MESH_MOCK_LAUNCH_ID: launchId } });
    const details = { launchId, childPid: child.pid, threadId: options.env?.MESH_TASK_THREAD_ID || options.env?.MESH_REVIEW_THREAD_ID };
    event(root, 'process.start', details);
    child.once('close', (code, signal) => event(root, 'process.close', { ...details, code, signal }));
    return child;
  }
  return originalSpawn(command, args, options);
};

const store = new TaskStore(storage);
store.on('changed', tasks => {
  for (const task of tasks) event(root, 'ledger.change', { threadId: task.threadId, status: task.status,
    revision: task.execution?.revision, phase: task.execution?.phase, runId: task.execution?.run?.id });
});
const detector = new AgentDetector();
detector.resolveExecutable = agent => agent === 'codex' ? process.execPath : null;
const resolver = new ActiveAgentResolver(detector);
const runner = new AutonomousRunner(detector, undefined, undefined, store);
const callbacks = new CallbackManager(detector, storage);
callbacks.setTaskStore(store);
callbacks.setAutonomousRunner(runner);
const registry = new TaskExecutionRegistry(storage);
const dispatcher = new AsyncDispatcher(store, resolver, runner, callbacks, detector, registry);
const rework = new ReworkController(store, runner, callbacks, detector, registry);
const controller = new ExecutionController(store, runner, callbacks, rework.getMergeCoordinator(),
  undefined, undefined, registry, { storageDir: storage, standingScanIntervalMs: 0 });
const server = new MCPServer(dispatcher, rework, store, detector, resolver, runner);
server.startStdio();
event(root, 'host.start', { receiver: process.env.MESH_TASK_RECEIVER === '1', home: os.homedir(),
  roles: fs.existsSync(path.join(storage, 'roles')) ? fs.readdirSync(path.join(storage, 'roles')).map(name => {
    try { return JSON.parse(fs.readFileSync(path.join(storage, 'roles', name), 'utf8')); } catch { return name; }
  }) : [] });
process.stdin.once('end', () => {
  controller.dispose();
  registry.dispose();
  store.dispose();
  event(root, 'host.close');
  // Each host owns a bounded test connection. No production service is stopped.
  process.exit(0);
});
