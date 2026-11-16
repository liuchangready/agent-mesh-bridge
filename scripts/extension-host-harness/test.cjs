'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vscode = require('vscode');

exports.run = async () => {
  const extensionId = process.env.MESH_SMOKE_EXTENSION_ID;
  const expectedVersion = process.env.MESH_SMOKE_EXPECTED_VERSION;
  const extensionsDir = path.resolve(process.env.MESH_SMOKE_EXTENSIONS_DIR);
  const extension = vscode.extensions.getExtension(extensionId);

  assert.ok(extension, `VSIX extension ${extensionId} was not found in the Extension Host`);
  assert.ok(
    extension.packageJSON.activationEvents?.includes('onStartupFinished'),
    'installed extension no longer activates after startup',
  );
  if (!extension.isActive) await extension.activate();
  assert.equal(extension.isActive, true, 'installed extension failed to activate in the Extension Host');
  assert.equal(extension.packageJSON.version, expectedVersion, 'loaded VSIX version does not match package.json');

  const relativeExtensionPath = path.relative(extensionsDir, path.resolve(extension.extensionPath));
  assert.ok(
    relativeExtensionPath && !relativeExtensionPath.startsWith('..') && !path.isAbsolute(relativeExtensionPath),
    `Extension loaded outside the isolated extensions directory: ${extension.extensionPath}`,
  );
  assert.ok(fs.existsSync(path.join(extension.extensionPath, 'out', 'extension.js')));

  const activityBar = extension.packageJSON.contributes?.viewsContainers?.activitybar || [];
  assert.ok(
    activityBar.some((container) => container.id === 'agent-mesh-bridge-container'),
    'Agent Mesh Bridge activity bar container is missing from the installed VSIX',
  );
  const views = extension.packageJSON.contributes?.views?.['agent-mesh-bridge-container'] || [];
  for (const viewId of ['agent-mesh-bridge.agent-nodes', 'agent-mesh-bridge.active-tasks']) {
    assert.ok(views.some((view) => view.id === viewId), `Expected contributed view is missing: ${viewId}`);
  }

  const commands = new Set(await vscode.commands.getCommands(true));
  for (const command of [
    'agent-mesh-bridge.refreshAgentStatus',
    'agent-mesh-bridge.refreshTasks',
    'agent-mesh-bridge.syncMcpConfig',
  ]) {
    assert.ok(commands.has(command), `Expected command was not registered: ${command}`);
  }

  console.log(`PASS: ${extensionId}@${expectedVersion} loaded from the packaged VSIX and activated`);
};
