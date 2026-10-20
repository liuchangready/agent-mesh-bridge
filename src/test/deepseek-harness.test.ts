import { test, describe, after } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  DSH_PROVIDER_ENV_HINT,
  DSH_PROVIDER_ID_HINT,
  buildDshHeadlessCommand,
  buildDshMcpPatchBlock,
  buildDshProviderPatchBlock,
  checkDshAuth,
  detectDshActiveSession,
  encodeWorkspaceSlug,
  isDshProviderUsable,
  mergeManagedPatchBlock,
  readDshConfiguredProvider,
  removeManagedPatchBlock,
  resolveDshInstall,
  resolveDshProviderSettings,
} from '../bridge/deepseek-harness';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { AgentDetector } from '../bridge/agent-detector';
import { isValidSessionId } from '../bridge/types';

const tempRoots: string[] = [];

function makeTempDir(prefix: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), prefix));
  tempRoots.push(dir);
  return dir;
}

function writeFile(filePath: string, content = ''): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content, 'utf8');
}

after(() => {
  for (const dir of tempRoots) {
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {}
  }
});

describe('DeepSeek Harness - workspace slug encoding', () => {
  test('should collapse separator runs into a single dash', () => {
    if (process.platform !== 'win32') return;
    assert.strictEqual(encodeWorkspaceSlug('D:\\agent_bridge'), '--D-agent_bridge--');
    assert.strictEqual(
      encodeWorkspaceSlug('C:\\Users\\sncdc\\Documents\\deepseek-harness\\default workspace'),
      '--C-Users-sncdc-Documents-deepseek-harness-default-workspace--',
    );
  });
});

describe('DeepSeek Harness - launcher resolution', () => {
  test('should return null for a missing launcher', () => {
    assert.strictEqual(
      resolveDshInstall(path.join(os.tmpdir(), 'definitely-missing-dsh.cmd')),
      null,
    );
  });

  test('should derive the install root from the profile launcher path', () => {
    const root = makeTempDir('dsh-install-');
    const launcher = path.join(root, 'resources', 'runtime', 'cli', 'bin', 'dsh.cmd');
    writeFile(launcher, '@echo off');

    const install = resolveDshInstall(launcher);
    assert.ok(install, 'launcher should resolve');
    assert.strictEqual(install!.root, root);
    assert.strictEqual(install!.launcherPath, launcher);
    // Without the Electron payload present, no direct-spawn strategy is chosen.
    assert.strictEqual(install!.execPath, undefined);
    assert.strictEqual(install!.cliPath, undefined);
  });

  test('should prefer the Electron binary when the packaged layout is present', () => {
    if (process.platform !== 'win32') return;
    const root = makeTempDir('dsh-electron-');
    const launcher = path.join(root, 'resources', 'runtime', 'cli', 'bin', 'dsh.cmd');
    writeFile(launcher, '@echo off');
    writeFile(path.join(root, 'DeepSeek Harness.exe'));
    writeFile(path.join(root, 'resources', 'app.asar'));

    const install = resolveDshInstall(launcher);
    assert.ok(install?.execPath, 'expected the Electron executable to be selected');
    assert.strictEqual(install!.execPath, path.join(root, 'DeepSeek Harness.exe'));
    assert.ok(
      install!.cliPath?.endsWith(path.join('@deepseek-ai', 'dsh-desktop-host', 'lib', 'cli.js')),
      `unexpected cli path: ${install!.cliPath}`,
    );
  });
});

describe('DeepSeek Harness - headless command contract', () => {
  test('should omit --session-id for a fresh task and close stdin safely', () => {
    if (process.platform !== 'win32') return;
    const root = makeTempDir('dsh-cmd-init-');
    const launcher = path.join(root, 'resources', 'runtime', 'cli', 'bin', 'dsh.cmd');
    writeFile(launcher, '@echo off');
    writeFile(path.join(root, 'DeepSeek Harness.exe'));
    writeFile(path.join(root, 'resources', 'app.asar'));

    const built = buildDshHeadlessCommand(launcher, {
      prompt: 'Implement the parser',
      json: true,
    });

    assert.ok(built);
    // `--session-id` ADOPTS an existing session and rejects unknown ids, so a
    // fresh task must never pass one.
    assert.ok(!built!.args.includes('--session-id'));
    assert.ok(built!.args.includes('headless'));
    assert.ok(built!.args.includes('--json'));
    assert.strictEqual(built!.args[built!.args.length - 1], 'Implement the parser');
    assert.strictEqual(built!.env.ELECTRON_RUN_AS_NODE, '1');
    // Unattended DSH runs must not inherit the headless profile's default
    // `workspace-write` + `approval: ask` policy.
    assert.strictEqual(built!.env.DSH_PERMISSION_MODE, 'danger-full-access');
    assert.strictEqual(built!.useShell, false);
  });

  test('should pass --session-id when resuming a rework round', () => {
    if (process.platform !== 'win32') return;
    const root = makeTempDir('dsh-cmd-rework-');
    const launcher = path.join(root, 'resources', 'runtime', 'cli', 'bin', 'dsh.cmd');
    writeFile(launcher, '@echo off');
    writeFile(path.join(root, 'DeepSeek Harness.exe'));
    writeFile(path.join(root, 'resources', 'app.asar'));

    const sessionId = 'session-4966a066-ed0c-461d-803d-89d897edae28';
    const built = buildDshHeadlessCommand(launcher, {
      prompt: 'Fix the failing test',
      sessionId,
      json: true,
    });

    assert.ok(built);
    const idx = built!.args.indexOf('--session-id');
    assert.ok(idx >= 0, 'expected --session-id');
    assert.strictEqual(built!.args[idx + 1], sessionId);
  });
});

describe('DeepSeek Harness - native session detection', () => {
  test('should prefer the session recorded for the requested workspace', () => {
    const home = makeTempDir('dsh-home-');
    const previous = process.env.DSH_HOME;
    process.env.DSH_HOME = home;
    try {
      const workspace = path.resolve('D:\\agent_bridge');
      const slug = encodeWorkspaceSlug(workspace);

      const own = path.join(home, 'sessions', slug, 'session-11111111-1111-1111-1111-111111111111');
      writeFile(path.join(own, 'session.v4.jsonl.zstd'), 'x');

      // A different workspace holding a *newer* session must not win.
      const other = path.join(
        home,
        'sessions',
        '--C-other--',
        'session-22222222-2222-2222-2222-222222222222',
      );
      writeFile(path.join(other, 'session.v4.jsonl.zstd'), 'x');
      const future = new Date(Date.now() + 60_000);
      fs.utimesSync(path.join(other, 'session.v4.jsonl.zstd'), future, future);

      assert.strictEqual(
        detectDshActiveSession(workspace),
        'session-11111111-1111-1111-1111-111111111111',
      );

      // Exclusion (used to avoid waking the receiver's own session).
      assert.strictEqual(
        detectDshActiveSession(workspace, ['session-11111111-1111-1111-1111-111111111111']),
        'session-22222222-2222-2222-2222-222222222222',
      );
    } finally {
      if (previous === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previous;
    }
  });

  test('should return undefined when no session store exists', () => {
    const home = makeTempDir('dsh-home-empty-');
    const previous = process.env.DSH_HOME;
    process.env.DSH_HOME = home;
    try {
      assert.strictEqual(detectDshActiveSession('D:\\agent_bridge'), undefined);
    } finally {
      if (previous === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previous;
    }
  });
});

describe('DeepSeek Harness - profile patch management', () => {
  test('should emit a single-server stdio MCP registration block', () => {
    const block = buildDshMcpPatchBlock('D:\\tools\\agent-bridge-mcp.cjs', 'node');
    assert.ok(block.includes("name: '@deepseek-ai/dsh-mcp-client'"));
    assert.ok(block.includes('transport: stdio'));
    // Each dsh-mcp-client instance connects to exactly one server.
    assert.ok(block.includes('serverName: agent-mesh-bridge'));
    assert.ok(block.includes("'D:/tools/agent-bridge-mcp.cjs'"));
    assert.ok(block.includes("'--caller'"));
    assert.ok(block.includes('MESH_CALLER_AGENT: deepseek_harness'));
  });

  test('should replace a bare empty-array document rather than append a second root', () => {
    const merged = mergeManagedPatchBlock('# comment\n\n[]\n', 'BLOCK');
    assert.ok(merged.includes('BLOCK'));
    assert.ok(!/^\s*\[\s*\]\s*$/m.test(merged), 'the empty document must be consumed');
  });

  test('should preserve user patch entries', () => {
    const existing = '- insert:\n    - id: user-entry\n      name: some-plugin\n';
    const merged = mergeManagedPatchBlock(existing, 'BLOCK');
    assert.ok(merged.includes('user-entry'));
    assert.ok(merged.includes('BLOCK'));
    assert.ok(merged.indexOf('user-entry') < merged.indexOf('BLOCK'));
  });

  test('should be idempotent across repeated syncs', () => {
    const block = buildDshMcpPatchBlock('D:\\tools\\agent-bridge-mcp.cjs', 'node');
    let content = '# comment\n\n[]\n';
    content = mergeManagedPatchBlock(content, block);
    content = mergeManagedPatchBlock(content, block);
    content = mergeManagedPatchBlock(content, block);
    assert.strictEqual(content.split('agent-mesh-bridge-mcp').length - 1, 1);
  });

  test('should replace a stale managed block with the current one', () => {
    const stale = buildDshMcpPatchBlock('D:\\old\\agent-bridge-mcp.cjs', 'old-node');
    const current = buildDshMcpPatchBlock('D:\\new\\agent-bridge-mcp.cjs', 'new-node');

    let content = mergeManagedPatchBlock('[]\n', stale);
    assert.ok(content.includes("'D:/old/agent-bridge-mcp.cjs'"));

    content = mergeManagedPatchBlock(content, current);
    assert.ok(content.includes("'D:/new/agent-bridge-mcp.cjs'"));
    assert.ok(!content.includes("'D:/old/agent-bridge-mcp.cjs'"));
    assert.ok(!content.includes('old-node'));
  });
});

describe('DeepSeek Harness - run-output parsing', () => {
  const runner = new AutonomousRunner(new AgentDetector());

  test('should capture the native session id from the first run event', () => {
    const stream = [
      '{"type":"session","sessionId":"session-807deafa-091d-4122-a287-ade252e863ab","cwd":"D:\\\\agent_bridge"}',
      '{"type":"status","phase":"turn_start","turn":1}',
      '{"type":"final","text":"PONG"}',
    ].join('\n');

    assert.strictEqual(
      runner.extractSessionId(stream),
      'session-807deafa-091d-4122-a287-ade252e863ab',
    );
  });

  test('should extract the final answer from the run-event stream', () => {
    const stream = [
      '{"type":"session","sessionId":"session-1"}',
      '{"type":"status","phase":"step_start","turn":1,"step":1}',
      '{"type":"final","text":"PONG"}',
    ].join('\n');

    assert.strictEqual(runner.extractCleanOutput(stream), 'PONG');
  });
});

describe('DeepSeek Harness - session id validation', () => {
  test('should accept native DSH session ids and reject agent identifiers', () => {
    assert.ok(isValidSessionId('session-4966a066-ed0c-461d-803d-89d897edae28'));
    assert.ok(!isValidSessionId('deepseek_harness'));
    assert.ok(!isValidSessionId('dsh'));
    assert.ok(!isValidSessionId('harness'));
  });
});

// The managed-block markers are the on-disk contract for `cordis.patch.yml`.
const MCP_MARKERS = {
  begin: '# >>> agent-mesh-bridge (managed) >>>',
  end: '# <<< agent-mesh-bridge (managed) <<<',
};
const PROVIDER_MARKERS = {
  begin: '# >>> agent-mesh-bridge (provider) >>>',
  end: '# <<< agent-mesh-bridge (provider) <<<',
};

describe('DeepSeek Harness - configurable model provider', () => {
  const provider = {
    provider: 'commandcode',
    api: 'openai-completions',
    baseUrl: 'https://api.commandcode.ai/provider/v1',
    model: 'deepseek/deepseek-v4.1-flash',
    apiKeyEnv: 'COMMANDCODE_API_KEY',
  };

  test('should require every field DSH needs to boot the route', () => {
    assert.ok(isDshProviderUsable(provider));
    assert.ok(!isDshProviderUsable(null));
    assert.ok(!isDshProviderUsable({ ...provider, provider: '  ' }));
    assert.ok(!isDshProviderUsable({ ...provider, baseUrl: '' }));
    assert.ok(!isDshProviderUsable({ ...provider, apiKeyEnv: '' }));
  });

  test('should render the provider block without ever embedding the key', () => {
    const block = buildDshProviderPatchBlock(provider);
    assert.ok(block.includes('- id: agent-default-model'));
    assert.ok(block.includes('provider: commandcode'));
    assert.ok(block.includes("model: 'deepseek/deepseek-v4.1-flash'"));
    assert.ok(block.includes('- id: llm-pi-ai'));
    assert.ok(block.includes("baseURL: 'https://api.commandcode.ai/provider/v1'"));
    // The profile only names the env var; the secret is injected at spawn time.
    assert.ok(block.includes("apiKeyEnv: 'COMMANDCODE_API_KEY'"));
    assert.ok(!block.includes('Bearer'));
  });

  test('should keep the MCP and provider blocks independent', () => {
    const mcp = buildDshMcpPatchBlock('D:\\tools\\agent-bridge-mcp.cjs', 'node');
    const prov = buildDshProviderPatchBlock(provider);

    let text = mergeManagedPatchBlock('[]\n', mcp, MCP_MARKERS);
    text = mergeManagedPatchBlock(text, prov, PROVIDER_MARKERS);
    assert.ok(text.includes('agent-mesh-bridge-mcp'));
    assert.ok(text.includes('commandcode'));

    // Re-syncing one block must not disturb the other.
    text = mergeManagedPatchBlock(text, mcp, MCP_MARKERS);
    assert.strictEqual(text.split('agent-mesh-bridge-mcp').length - 1, 1);
    assert.strictEqual(text.split('provider: commandcode').length - 1, 1);

    // Clearing the provider removes only its own block.
    text = removeManagedPatchBlock(text, PROVIDER_MARKERS);
    assert.ok(!text.includes('commandcode'));
    assert.ok(text.includes('agent-mesh-bridge-mcp'));
  });

  test('should report the configured provider as the login route', () => {
    const saved = {
      hintEnv: process.env[DSH_PROVIDER_ENV_HINT],
      hintId: process.env[DSH_PROVIDER_ID_HINT],
      key: process.env.COMMANDCODE_API_KEY,
      deepseek: process.env.DEEPSEEK_API_KEY,
      dshKey: process.env.DSH_API_KEY,
    };
    const restore = (name: string, value: string | undefined) => {
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    };
    try {
      delete process.env.DEEPSEEK_API_KEY;
      delete process.env.DSH_API_KEY;

      // Configured, but the key is not there yet.
      process.env[DSH_PROVIDER_ENV_HINT] = 'COMMANDCODE_API_KEY';
      process.env[DSH_PROVIDER_ID_HINT] = 'commandcode';
      delete process.env.COMMANDCODE_API_KEY;
      const missing = checkDshAuth();
      assert.strictEqual(missing.authenticated, false);
      assert.match(missing.details, /commandcode/);
      assert.match(missing.details, /COMMANDCODE_API_KEY/);

      // Key supplied through the declared variable.
      process.env.COMMANDCODE_API_KEY = 'test-key-value';
      const ok = checkDshAuth();
      assert.strictEqual(ok.authenticated, true);
      assert.match(ok.details, /commandcode/);
    } finally {
      restore(DSH_PROVIDER_ENV_HINT, saved.hintEnv);
      restore(DSH_PROVIDER_ID_HINT, saved.hintId);
      restore('COMMANDCODE_API_KEY', saved.key);
      restore('DEEPSEEK_API_KEY', saved.deepseek);
      restore('DSH_API_KEY', saved.dshKey);
    }
  });

  test('should resolve the configured route from disk, for any process', () => {
    const home = makeTempDir('dsh-provider-home-');
    const previousDshHome = process.env.DSH_HOME;
    const previousHint = process.env[DSH_PROVIDER_ENV_HINT];
    const previousKey = process.env.COMMANDCODE_API_KEY;
    const previousDeepseek = process.env.DEEPSEEK_API_KEY;
    try {
      process.env.DSH_HOME = home;
      delete process.env[DSH_PROVIDER_ENV_HINT];
      delete process.env.COMMANDCODE_API_KEY;
      delete process.env.DEEPSEEK_API_KEY;

      // No profile patch yet.
      assert.strictEqual(readDshConfiguredProvider(), undefined);

      writeFile(
        path.join(home, 'profiles', 'headless', 'cordis.patch.yml'),
        mergeManagedPatchBlock('[]\n', buildDshProviderPatchBlock(provider), PROVIDER_MARKERS),
      );

      // A sibling process (an agent's own MCP server) must reach the same answer
      // without any in-process hint.
      const fromDisk = readDshConfiguredProvider();
      assert.ok(fromDisk, 'provider should be read back from the profile patch');
      assert.strictEqual(fromDisk!.provider, 'commandcode');
      assert.strictEqual(fromDisk!.baseUrl, 'https://api.commandcode.ai/provider/v1');
      assert.strictEqual(fromDisk!.model, 'deepseek/deepseek-v4.1-flash');
      assert.strictEqual(fromDisk!.apiKeyEnv, 'COMMANDCODE_API_KEY');
      assert.strictEqual(fromDisk!.api, 'openai-completions');

      // Key missing in this process: report the real, actionable cause.
      const missing = checkDshAuth();
      assert.strictEqual(missing.authenticated, false);
      assert.match(missing.details, /commandcode/);
      assert.match(missing.details, /COMMANDCODE_API_KEY/);
      assert.ok(
        !missing.details.includes('桌面端登录凭据'),
        'must not fall through to the misleading desktop-credential message',
      );

      // Once the declared variable is present, the route is usable.
      process.env.COMMANDCODE_API_KEY = 'k';
      assert.strictEqual(checkDshAuth().authenticated, true);
    } finally {
      if (previousDshHome === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = previousDshHome;
      if (previousHint === undefined) delete process.env[DSH_PROVIDER_ENV_HINT];
      else process.env[DSH_PROVIDER_ENV_HINT] = previousHint;
      if (previousKey === undefined) delete process.env.COMMANDCODE_API_KEY;
      else process.env.COMMANDCODE_API_KEY = previousKey;
      if (previousDeepseek === undefined) delete process.env.DEEPSEEK_API_KEY;
      else process.env.DEEPSEEK_API_KEY = previousDeepseek;
    }
  });

  test('should authenticate official route with DEEPSEEK_API_KEY when no custom provider is configured', () => {
    const saved = {
      dshHome: process.env.DSH_HOME,
      hintEnv: process.env[DSH_PROVIDER_ENV_HINT],
      hintId: process.env[DSH_PROVIDER_ID_HINT],
      deepseek: process.env.DEEPSEEK_API_KEY,
      dshKey: process.env.DSH_API_KEY,
    };
    try {
      const emptyHome = makeTempDir('dsh-empty-home-');
      process.env.DSH_HOME = emptyHome;
      delete process.env[DSH_PROVIDER_ENV_HINT];
      delete process.env[DSH_PROVIDER_ID_HINT];
      delete process.env.DSH_API_KEY;
      delete process.env.DEEPSEEK_API_KEY;

      const unauth = checkDshAuth();
      assert.strictEqual(unauth.authenticated, false);

      process.env.DEEPSEEK_API_KEY = 'sk-official-test-key';
      const auth = checkDshAuth();
      assert.strictEqual(auth.authenticated, true);
      assert.match(auth.details, /DEEPSEEK_API_KEY/);
    } finally {
      if (saved.dshHome === undefined) delete process.env.DSH_HOME;
      else process.env.DSH_HOME = saved.dshHome;
      if (saved.hintEnv === undefined) delete process.env[DSH_PROVIDER_ENV_HINT];
      else process.env[DSH_PROVIDER_ENV_HINT] = saved.hintEnv;
      if (saved.hintId === undefined) delete process.env[DSH_PROVIDER_ID_HINT];
      else process.env[DSH_PROVIDER_ID_HINT] = saved.hintId;
      if (saved.deepseek === undefined) delete process.env.DEEPSEEK_API_KEY;
      else process.env.DEEPSEEK_API_KEY = saved.deepseek;
      if (saved.dshKey === undefined) delete process.env.DSH_API_KEY;
      else process.env.DSH_API_KEY = saved.dshKey;
    }
  });
});

describe('DeepSeek Harness - model route resolution', () => {
  test('every setting blank keeps the shipped deepseek-official route', () => {
    const resolved = resolveDshProviderSettings({});
    assert.strictEqual(resolved.provider, '');
    assert.strictEqual(resolved.baseUrl, '');
    assert.strictEqual(resolved.apiKeyEnv, 'DEEPSEEK_API_KEY');
    assert.strictEqual(
      isDshProviderUsable(resolved),
      false,
      'a route with no URL must not be written into the headless profile',
    );
  });

  test('a URL without a provider derives one, plus its key variable and model', () => {
    const custom = resolveDshProviderSettings({ baseUrl: 'https://api.siliconflow.cn/v1/' });
    assert.strictEqual(custom.provider, 'custom');
    assert.strictEqual(custom.apiKeyEnv, 'DEEPSEEK_API_KEY');
    assert.strictEqual(custom.model, 'deepseek-chat');
    assert.strictEqual(custom.api, 'openai-completions');

    const commandcode = resolveDshProviderSettings({
      baseUrl: 'https://api.commandcode.ai/provider/v1',
    });
    assert.strictEqual(commandcode.provider, 'commandcode');
    assert.strictEqual(commandcode.apiKeyEnv, 'COMMANDCODE_API_KEY');
  });

  test('a named legacy provider with no URL keeps its shipped endpoint and model', () => {
    const resolved = resolveDshProviderSettings({ provider: 'commandcode' });
    assert.strictEqual(resolved.baseUrl, 'https://api.commandcode.ai/provider/v1');
    assert.strictEqual(resolved.model, 'deepseek/deepseek-v4.1-flash');
    assert.strictEqual(resolved.apiKeyEnv, 'COMMANDCODE_API_KEY');
    assert.strictEqual(
      isDshProviderUsable(resolved),
      true,
      'a pre-existing commandcode route must not silently fall back to the official endpoint',
    );

    // The same pairing holds when the user wrote the URL out explicitly.
    const explicitUrl = resolveDshProviderSettings({
      provider: 'commandcode',
      baseUrl: 'https://api.commandcode.ai/provider/v1',
    });
    assert.strictEqual(explicitUrl.model, 'deepseek/deepseek-v4.1-flash');
    assert.strictEqual(explicitUrl.apiKeyEnv, 'COMMANDCODE_API_KEY');
  });

  test('explicit values win over every derived one', () => {
    const resolved = resolveDshProviderSettings({
      provider: 'commandcode',
      baseUrl: 'https://gateway.example/v1',
      model: 'my-model',
      apiKeyEnv: 'MY_KEY',
      api: 'anthropic-messages',
    });
    assert.strictEqual(resolved.provider, 'commandcode');
    assert.strictEqual(resolved.baseUrl, 'https://gateway.example/v1');
    assert.strictEqual(resolved.model, 'my-model');
    assert.strictEqual(resolved.apiKeyEnv, 'MY_KEY');
    assert.strictEqual(resolved.api, 'anthropic-messages');
  });

  test('an unknown named provider without a URL stays unroutable rather than guessing', () => {
    const resolved = resolveDshProviderSettings({ provider: 'some-gateway' });
    assert.strictEqual(resolved.baseUrl, '');
    assert.strictEqual(isDshProviderUsable(resolved), false);
  });
});
