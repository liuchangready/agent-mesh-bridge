const path = require('node:path');
const crypto = require('node:crypto');
const childProcess = require('node:child_process');

const root = process.env.MESH_MOCK_ROOT;
if (!root || !require('node:fs').existsSync(path.join(root, 'mock-only.json'))) {
  throw new Error('Mock agents require an explicitly isolated test root');
}
const originalSpawn = childProcess.spawn;
const detectorModule = require(path.resolve(__dirname, '../../../out/bridge/agent-detector.js'));
detectorModule.AgentDetector.prototype.resolveExecutable = agent => agent === 'codex' ? process.execPath : null;
function event(kind, details) {
  const fs = require('node:fs');
  const at = process.hrtime.bigint().toString();
  fs.writeFileSync(path.join(root, 'events', `${at}-${process.pid}-${crypto.randomUUID()}.json`),
    JSON.stringify({ kind, at, pid: process.pid, ...details }));
}
childProcess.spawn = function(command, args, options) {
  const isJob = command.endsWith('agent-bridge-windows-job.exe') && args[4] === process.execPath;
  if ((command === process.execPath || isJob) && args.includes('exec') && args.includes('--ask-for-approval')) {
    const launchId = crypto.randomUUID();
    const agent = path.resolve(__dirname, '../../../out/test/fixtures/mock-agent.js');
    const wrappedArgs = isJob ? [...args.slice(0, 5), agent, ...args.slice(5)] : [agent, ...args];
    const child = originalSpawn(command, wrappedArgs, {
      ...options, env: { ...options.env, MESH_MOCK_LAUNCH_ID: launchId },
    });
    const details = { launchId, childPid: child.pid, threadId: options.env?.MESH_TASK_THREAD_ID || options.env?.MESH_REVIEW_THREAD_ID };
    event('process.start', details);
    child.once('close', (code, signal) => event('process.close', { ...details, code, signal }));
    return child;
  }
  return originalSpawn.call(this, command, args, options);
};
