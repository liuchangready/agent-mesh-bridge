'use strict';

const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const {
  downloadAndUnzipVSCode,
  resolveCliArgsFromVSCodeExecutablePath,
  runTests,
} = require('@vscode/test-electron');

const root = path.resolve(__dirname, '..');
const manifest = require(path.join(root, 'package.json'));
const version = process.env.VSCODE_TEST_VERSION || 'stable';
const extensionId = `${manifest.publisher}.${manifest.name}`;
const vsixPath = path.join(root, `${manifest.name}-${manifest.version}.vsix`);
const harnessPath = path.join(__dirname, 'extension-host-harness');

function assertExitOk(result, description) {
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`${description} exited with code ${result.status ?? 'unknown'}`);
  }
}

async function main() {
  try {
    await fs.access(vsixPath);
  } catch {
    throw new Error(`VSIX not found: ${vsixPath}. Run npm run package first.`);
  }

  const smokeCache = path.join(root, '.vscode-test');
  await fs.mkdir(smokeCache, { recursive: true });
  const tempRoot = await fs.mkdtemp(path.join(smokeCache, 'host-smoke-'));
  const tempBase = `${path.resolve(smokeCache)}${path.sep}`;
  const resolvedTempRoot = path.resolve(tempRoot);
  if (!resolvedTempRoot.startsWith(tempBase)) {
    throw new Error(`Refusing to use a smoke-test directory outside the project test cache: ${tempRoot}`);
  }
  const homeDir = path.join(tempRoot, 'home');
  const userDataDir = path.join(tempRoot, 'user-data');
  const extensionsDir = path.join(tempRoot, 'extensions');
  const bridgeHome = path.join(tempRoot, 'bridge-home');
  const osTemp = path.join(tempRoot, 'os-temp');
  const appData = path.join(homeDir, 'AppData', 'Roaming');
  const localAppData = path.join(homeDir, 'AppData', 'Local');
  const previousEnv = Object.fromEntries(
    [
      'USERPROFILE', 'HOME', 'APPDATA', 'LOCALAPPDATA', 'AGENT_MESH_BRIDGE_HOME', 'CODEX_HOME',
      'ELECTRON_RUN_AS_NODE', 'VSCODE_NODE_COMPILE_CACHE_KIND', 'VSCODE_NODE_COMPILE_CACHE_ROOT',
      'TEMP', 'TMP', 'PATH',
    ].map((key) => [key, process.env[key]]),
  );

  try {
    await Promise.all([
      fs.mkdir(path.join(userDataDir, 'User'), { recursive: true }),
      fs.mkdir(extensionsDir, { recursive: true }),
      fs.mkdir(bridgeHome, { recursive: true }),
      fs.mkdir(osTemp, { recursive: true }),
      fs.mkdir(appData, { recursive: true }),
      fs.mkdir(localAppData, { recursive: true }),
    ]);
    await fs.writeFile(
      path.join(userDataDir, 'User', 'settings.json'),
      JSON.stringify({ 'agentMeshBridge.autoSyncMcp': false }, null, 2),
    );

    // Activation writes bridge state and probes user CLI locations; keep both inside this temp profile.
    process.env.USERPROFILE = homeDir;
    process.env.HOME = homeDir;
    process.env.APPDATA = appData;
    process.env.LOCALAPPDATA = localAppData;
    process.env.AGENT_MESH_BRIDGE_HOME = bridgeHome;
    process.env.CODEX_HOME = path.join(homeDir, '.codex');
    process.env.TEMP = osTemp;
    process.env.TMP = osTemp;
    const systemRoot = process.env.SystemRoot || 'C:\\Windows';
    process.env.PATH = process.platform === 'win32'
      ? [path.join(systemRoot, 'System32'), systemRoot].join(path.delimiter)
      : ['/usr/bin', '/bin'].join(path.delimiter);
    delete process.env.ELECTRON_RUN_AS_NODE;
    delete process.env.VSCODE_NODE_COMPILE_CACHE_KIND;
    delete process.env.VSCODE_NODE_COMPILE_CACHE_ROOT;

    const vscodeExecutablePath = await downloadAndUnzipVSCode(version);
    const [cliPath] = resolveCliArgsFromVSCodeExecutablePath(vscodeExecutablePath, {
      reuseMachineInstall: true,
    });
    const installResult = spawnSync(
      cliPath,
      [
        '--user-data-dir', userDataDir,
        '--extensions-dir', extensionsDir,
        '--install-extension', vsixPath,
        '--force',
      ],
      { stdio: 'inherit', shell: process.platform === 'win32', windowsHide: true },
    );
    assertExitOk(installResult, 'VSIX installation');

    console.log(`Starting VS Code ${version} Extension Host for ${extensionId}@${manifest.version}`);
    await runTests({
      vscodeExecutablePath,
      extensionDevelopmentPath: harnessPath,
      extensionTestsPath: path.join(harnessPath, 'test.cjs'),
      launchArgs: [
        '--user-data-dir', userDataDir,
        '--extensions-dir', extensionsDir,
        '--disable-workspace-trust',
      ],
      extensionTestsEnv: {
        MESH_SMOKE_EXTENSION_ID: extensionId,
        MESH_SMOKE_EXPECTED_VERSION: manifest.version,
        MESH_SMOKE_EXTENSIONS_DIR: extensionsDir,
      },
    });
  } finally {
    for (const [key, value] of Object.entries(previousEnv)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    await fs.rm(tempRoot, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
