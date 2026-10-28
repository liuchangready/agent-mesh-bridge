import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { ConfigSyncService } from '../config/config-sync';

/**
 * Every agent must end up with exactly ONE MCP registration: Codex surfaces one
 * namespace per registered server, so a duplicate shows the model a second,
 * identical copy of all eight tools and spawns a second server process.
 */
describe('ConfigSyncService - single MCP registration per agent', () => {
  let home: string;
  let previousDshHome: string | undefined;

  beforeEach(() => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-sync-'));
    // Keep the DeepSeek Harness sync away from the real ~/.dsh.
    previousDshHome = process.env.DSH_HOME;
    process.env.DSH_HOME = path.join(home, '.dsh');
  });

  afterEach(() => {
    if (previousDshHome === undefined) delete process.env.DSH_HOME;
    else process.env.DSH_HOME = previousDshHome;
    try {
      fs.rmSync(home, { recursive: true, force: true });
    } catch {}
  });

  test('registers one Codex server and cleans up the legacy duplicate', async () => {
    // Seed exactly what an older version of this extension used to write.
    const codexDir = path.join(home, '.codex');
    fs.mkdirSync(codexDir, { recursive: true });
    fs.writeFileSync(
      path.join(codexDir, 'config.toml'),
      [
        'model = "gpt-6-sol"',
        '[mcp_servers.cross-agent-bridge]',
        'command = "node"',
        'args = ["stale.cjs", "--caller", "codex"]',
        '',
        "[projects.'/tmp/bridge-workspace']",
        'trust_level = "trusted"',
        '',
      ].join('\n'),
      'utf8',
    );
    fs.writeFileSync(
      path.join(codexDir, 'config.json'),
      JSON.stringify({ mcpServers: { 'cross-agent-bridge': { command: 'node' } } }, null, 2),
      'utf8',
    );

    const sync = new ConfigSyncService(path.join(os.tmpdir(), 'bridge-tools', 'agent-bridge-mcp.cjs'), null, home);
    await sync.syncAll();

    const toml = fs.readFileSync(path.join(codexDir, 'config.toml'), 'utf8');
    assert.strictEqual(
      toml.split('[mcp_servers.agent-mesh-bridge]').length - 1,
      1,
      'exactly one agent-mesh-bridge block',
    );
    assert.ok(!toml.includes('cross-agent-bridge'), 'legacy duplicate removed');
    assert.ok(toml.includes('model = "gpt-6-sol"'), 'unrelated config preserved');
    assert.ok(toml.includes('trust_level = "trusted"'), 'project trust preserved');

    const json = JSON.parse(fs.readFileSync(path.join(codexDir, 'config.json'), 'utf8'));
    assert.deepStrictEqual(Object.keys(json.mcpServers), ['agent-mesh-bridge']);

    // Re-syncing must not reintroduce a duplicate.
    await sync.syncAll();
    const tomlAgain = fs.readFileSync(path.join(codexDir, 'config.toml'), 'utf8');
    assert.strictEqual(tomlAgain.split('[mcp_servers.agent-mesh-bridge]').length - 1, 1);
    assert.ok(!tomlAgain.includes('cross-agent-bridge'));
  });

  test('registers one server for Claude Code and Antigravity too', async () => {
    const sync = new ConfigSyncService(path.join(os.tmpdir(), 'bridge-tools', 'agent-bridge-mcp.cjs'), null, home);
    await sync.syncAll();

    const claude = JSON.parse(fs.readFileSync(path.join(home, '.claude.json'), 'utf8'));
    assert.deepStrictEqual(Object.keys(claude.mcpServers), ['agent-mesh-bridge']);

    const antigravity = JSON.parse(
      fs.readFileSync(path.join(home, '.gemini', 'antigravity', 'mcp_config.json'), 'utf8'),
    );
    assert.deepStrictEqual(Object.keys(antigravity.mcpServers), ['agent-mesh-bridge']);
  });

  test('hands the provider key to every bridge MCP process explicitly', async () => {
    // An agent spawns its own bridge MCP process, so ambient environment
    // inheritance is unreliable; the key must be declared on the registration.
    const provider = {
      provider: 'commandcode',
      api: 'openai-completions',
      baseUrl: 'https://api.commandcode.ai/provider/v1',
      model: 'deepseek/deepseek-v4.1-flash',
      apiKeyEnv: 'COMMANDCODE_API_KEY',
    };
    const dshPatchDir = path.join(home, '.dsh', 'profiles', 'headless');
    fs.mkdirSync(dshPatchDir, { recursive: true });
    fs.writeFileSync(path.join(dshPatchDir, 'cordis.patch.yml'), '[]\n', 'utf8');

    const sync = new ConfigSyncService(path.join(os.tmpdir(), 'bridge-tools', 'agent-bridge-mcp.cjs'), provider, home);
    sync.setAgentMcpEnv({ COMMANDCODE_API_KEY: 'secret-key-value' });
    await sync.syncAll();

    const toml = fs.readFileSync(path.join(home, '.codex', 'config.toml'), 'utf8');
    assert.ok(
      toml.includes('COMMANDCODE_API_KEY = "secret-key-value"'),
      'codex TOML carries the key',
    );

    const claude = JSON.parse(fs.readFileSync(path.join(home, '.claude.json'), 'utf8'));
    assert.strictEqual(
      claude.mcpServers['agent-mesh-bridge'].env.COMMANDCODE_API_KEY,
      'secret-key-value',
    );

    const antigravity = JSON.parse(
      fs.readFileSync(path.join(home, '.gemini', 'antigravity', 'mcp_config.json'), 'utf8'),
    );
    assert.strictEqual(
      antigravity.mcpServers['agent-mesh-bridge'].env.COMMANDCODE_API_KEY,
      'secret-key-value',
    );

    const dshPatch = fs.readFileSync(path.join(dshPatchDir, 'cordis.patch.yml'), 'utf8');
    assert.ok(dshPatch.includes('COMMANDCODE_API_KEY'), 'dsh MCP registration carries the key');
    // The model-route block must keep naming the env var only, never the secret.
    const providerBlock = dshPatch.slice(dshPatch.indexOf('(provider)'));
    assert.ok(!providerBlock.includes('secret-key-value'), 'provider block must not embed the key');
  });
});
