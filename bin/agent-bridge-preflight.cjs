#!/usr/bin/env node
/* Read-only cutover preflight. This command never constructs TaskStore or
 * TaskExecutionRegistry and never creates/updates coordination files. */
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const bridgeDir = path.resolve(__dirname, '..', 'out', 'bridge');
if (!fs.existsSync(bridgeDir)) {
  process.stderr.write('[agent-bridge-preflight] Compiled bridge output is missing; run the package compile first.\n');
  process.exit(2);
}
const { inspectShutdownPreflight, inspectV2RuntimeActivation, loadShutdownPreflightSnapshot } = require(path.join(bridgeDir, 'shutdown-preflight'));
const packagePath = path.resolve(__dirname, '..', 'package.json');
const expectedVersion = JSON.parse(fs.readFileSync(packagePath, 'utf8')).version;
const storageDir = path.join(os.homedir(), '.agent-mesh-bridge');
const packageRoot = path.resolve(__dirname, '..');
const startupAdmission = process.argv.includes('--startup-admission');
const snapshot = loadShutdownPreflightSnapshot(storageDir);
if (startupAdmission) {
  const components = [
    { component: 'extension-host', version: expectedVersion, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
    { component: 'standalone-mcp', version: expectedVersion, packageRoot, path: path.resolve(__dirname, 'agent-bridge-mcp.cjs') },
  ];
  const report = inspectV2RuntimeActivation({ ...snapshot, storageDir, components });
  process.stdout.write(`${JSON.stringify({ storageDir, taskCount: snapshot.tasks.length, processOwnerRecords: snapshot.owners.length, startupAdmission: report }, null, 2)}\n`);
  process.exitCode = report.ready ? 0 : 2;
  return;
}
const components = [{ component: 'standalone-mcp', version: expectedVersion, source: 'this package' }];
for (let index = 2; index < process.argv.length; index += 1) {
  if (process.argv[index] !== '--component' || !process.argv[index + 1]) continue;
  const [component, version] = process.argv[++index].split('=', 2);
  components.push({ component, version, source: 'operator supplied process inventory' });
}
const report = inspectShutdownPreflight({
  ...snapshot,
  storageDir,
  expectedVersion,
  components,
  requiredComponents: ['extension-host', 'standalone-mcp'],
});
process.stdout.write(`${JSON.stringify({ storageDir, taskCount: snapshot.tasks.length, processOwnerRecords: snapshot.owners.length, ...report }, null, 2)}\n`);
process.exitCode = report.ready ? 0 : 2;
