import { test } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import {
  ModelCatalog,
  extractJsonObject,
  getBridgeHome,
  invalidateConfiguredModelCache,
  loadModelDefaults,
  normalizeModelId,
  parseAgyModels,
  parseCodexModels,
  parseModelListPayload,
  resolveConfiguredModel,
  resolveInitiatorModel,
  saveModelDefaults,
  writeDshModelOverlay,
} from '../bridge/model-catalog';
import { DelegationThread, describeModelRouting, getModelRouting } from '../bridge/types';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { AgentDetector } from '../bridge/agent-detector';
import { buildDshProviderPatchBlock } from '../bridge/deepseek-harness';

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), prefix));
}

function withEnv<T>(vars: Record<string, string | undefined>, fn: () => T): T {
  const previous: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(vars)) {
    previous[key] = process.env[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return fn();
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    invalidateConfiguredModelCache();
  }
}

async function withEnvAsync<T>(
  vars: Record<string, string | undefined>,
  fn: () => Promise<T>,
): Promise<T> {
  const previous: Record<string, string | undefined> = {};
  for (const [key, value] of Object.entries(vars)) {
    previous[key] = process.env[key];
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  try {
    return await fn();
  } finally {
    for (const [key, value] of Object.entries(previous)) {
      if (value === undefined) delete process.env[key];
      else process.env[key] = value;
    }
    invalidateConfiguredModelCache();
  }
}

// ---------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------

test('Model catalog - Antigravity output parser', () => {
  const stdout = [
    'Fetching available models...',
    'gemini-3.8-flash-high\tGemini 3.8 Flash (High)',
    'claude-sonnet-4-6\tClaude Sonnet 4.6 (Thinking)',
    'gpt-oss-120b-medium\tGPT-OSS 120B (Medium)',
    '',
  ].join('\r\n');

  const models = parseAgyModels(stdout);
  assert.strictEqual(models.length, 3);
  assert.deepStrictEqual(models[0], {
    id: 'gemini-3.8-flash-high',
    label: 'Gemini 3.8 Flash (High)',
  });
  assert.strictEqual(models[1].id, 'claude-sonnet-4-6');
  // The banner must never be mistaken for a model id.
  assert.ok(!models.some((m) => /fetching/i.test(m.id)));
});

test('Model catalog - Codex catalog parser tolerates leading noise', () => {
  const stdout = [
    'warning: something harmless',
    JSON.stringify({
      models: [
        { slug: 'gpt-6-sol', display_name: 'GPT-6-Sol', visibility: 'list' },
        { slug: 'gpt-reserve', display_name: 'GPT-Reserve', visibility: 'hide' },
        { slug: 'gpt-6-sol', display_name: 'duplicate' },
      ],
    }),
  ].join('\n');

  const models = parseCodexModels(stdout);
  assert.deepStrictEqual(
    models.map((m) => m.id),
    ['gpt-6-sol', 'gpt-reserve'],
  );
  assert.strictEqual(models[0].label, 'GPT-6-Sol');
  assert.strictEqual(models[1].hint, 'hidden');
});

test('Model catalog - OpenAI/Anthropic model list parser', () => {
  const anthropic = parseModelListPayload({
    data: [
      { id: 'glm-5.2', display_name: 'GLM-5.2', type: 'model' },
      { id: 'glm-4.7', display_name: 'GLM-4.7', type: 'model' },
    ],
  });
  assert.deepStrictEqual(
    anthropic.map((m) => m.id),
    ['glm-5.2', 'glm-4.7'],
  );
  assert.strictEqual(anthropic[0].label, 'GLM-5.2');

  const openai = parseModelListPayload({ data: [{ id: 'deepseek/deepseek-v4.1-flash' }] });
  assert.deepStrictEqual(openai.map((m) => m.id), ['deepseek/deepseek-v4.1-flash']);

  assert.deepStrictEqual(parseModelListPayload({}), []);
});

test('Model catalog - JSON extraction rejects non-JSON payloads', () => {
  assert.strictEqual(extractJsonObject('no json here'), undefined);
  assert.deepStrictEqual(extractJsonObject('junk {"a":1} tail'), { a: 1 });
});

test('Model catalog - model ids are trimmed and empty means unset', () => {
  assert.strictEqual(normalizeModelId('  gpt-6-sol '), 'gpt-6-sol');
  assert.strictEqual(normalizeModelId('   '), undefined);
  assert.strictEqual(normalizeModelId(undefined), undefined);
});

// ---------------------------------------------------------------------------
// Persisted defaults
// ---------------------------------------------------------------------------

test('Model catalog - defaults round-trip and drop empty values', () => {
  const dir = makeTempDir('mesh-model-config-');
  saveModelDefaults(
    {
      codex: ' gpt-6-sol ',
      claude_code: 'sonnet',
      antigravity: '',
      deepseek_harness: '   ',
    },
    dir,
  );

  const loaded = loadModelDefaults(dir);
  assert.deepStrictEqual(loaded, { codex: 'gpt-6-sol', claude_code: 'sonnet' });
  assert.strictEqual(resolveConfiguredModel('codex', dir), 'gpt-6-sol');
  assert.strictEqual(resolveConfiguredModel('antigravity', dir), undefined);

  // A corrupt file must degrade to "no defaults" instead of throwing.
  fs.writeFileSync(path.join(dir, 'model-config.json'), '{not json', 'utf8');
  assert.deepStrictEqual(loadModelDefaults(dir), {});
});

test('Model catalog - resolveConfiguredModel memoizes the saved snapshot', () => {
  const dir = makeTempDir('mesh-model-cache-');
  saveModelDefaults({ codex: 'gpt-6-sol' }, dir);
  assert.strictEqual(resolveConfiguredModel('codex', dir), 'gpt-6-sol');

  saveModelDefaults({ codex: 'gpt-6-luna' }, dir);
  // Still the memoized value until the cache is dropped...
  assert.strictEqual(resolveConfiguredModel('codex', dir), 'gpt-6-sol');
  invalidateConfiguredModelCache();
  // ...then the new value is visible.
  assert.strictEqual(resolveConfiguredModel('codex', dir), 'gpt-6-luna');
});

test('Model catalog - bridge home can be redirected for isolation', () => {
  const dir = makeTempDir('mesh-bridge-home-');
  withEnv({ AGENT_MESH_BRIDGE_HOME: dir }, () => {
    assert.strictEqual(getBridgeHome(), dir);
  });
  assert.ok(fs.existsSync(dir));
});

// ---------------------------------------------------------------------------
// Discovery fallbacks (no network, no real CLIs)
// ---------------------------------------------------------------------------

test('Model catalog - missing executables fall back without throwing', async () => {
  const dir = makeTempDir('mesh-catalog-fallback-');
  const detector = new AgentDetector({}, undefined);
  // Force "not installed" for the two CLI-probed agents.
  detector.resolveExecutable = (() => null) as any;

  await withEnvAsync({ AGENT_MESH_BRIDGE_HOME: dir }, async () => {
    const catalog = new ModelCatalog(detector, { storageDir: dir, timeoutMs: 50 });
    const antigravity = await catalog.discover('antigravity');
    assert.deepStrictEqual(antigravity.models, []);
    assert.match(String(antigravity.error), /not found/i);

    const codex = await catalog.discover('codex');
    // Falls back to the local models cache / config.toml, never to a crash.
    assert.ok(Array.isArray(codex.models));
  });
});

test('Model catalog - cached catalogue is reused between instances', async () => {
  const dir = makeTempDir('mesh-catalog-cache-');
  fs.writeFileSync(
    path.join(dir, 'models-cache.json'),
    JSON.stringify({
      codex: {
        models: [{ id: 'gpt-6-sol', label: 'GPT-6-Sol' }],
        source: 'cli',
        updatedAt: new Date().toISOString(),
      },
    }),
    'utf8',
  );

  const detector = new AgentDetector({}, undefined);
  detector.resolveExecutable = (() => null) as any;
  const catalog = new ModelCatalog(detector, { storageDir: dir, timeoutMs: 50 });

  const model = await catalog.get('codex');
  assert.deepStrictEqual(model.models.map((m) => m.id), ['gpt-6-sol']);
  assert.strictEqual(model.source, 'cli');
});

// ---------------------------------------------------------------------------
// Command construction
// ---------------------------------------------------------------------------

function makeRunner(): { runner: AutonomousRunner; root: string } {
  const root = makeTempDir('mesh-runner-');
  const files: Record<string, string> = {
    antigravity: path.join(root, 'agy.exe'),
    codex: path.join(root, 'codex.exe'),
    claude_code: path.join(root, 'claude.cmd'),
    deepseek_harness: path.join(root, 'dsh-missing'),
  };
  for (const [agent, file] of Object.entries(files)) {
    if (agent === 'deepseek_harness') continue; // configured per-test
    fs.writeFileSync(file, '', 'utf8');
  }

  const detector = new AgentDetector({
    antigravity: files.antigravity,
    codex: files.codex,
    claude_code: files.claude_code,
  });
  const runner = new AutonomousRunner(detector);
  return { runner, root };
}

test('Model catalog - explicit model reaches every agent CLI', () => {
  const { runner } = makeRunner();

  const codex = runner.buildCommand({
    agent: 'codex',
    prompt: 'ship it',
    isRework: false,
    receiverSessionId: 'session-1',
    model: 'gpt-6-sol',
  });
  const codexConfigIndex = codex.args.indexOf('-c');
  assert.ok(codexConfigIndex >= 0, codex.args.join(' '));
  assert.strictEqual(codex.args[codexConfigIndex + 1], 'model="gpt-6-sol"');
  // The global config override must precede the subcommand.
  assert.ok(codexConfigIndex < codex.args.indexOf('exec'), codex.args.join(' '));

  const codexRework = runner.buildCommand({
    agent: 'codex',
    prompt: 'again',
    isRework: true,
    receiverSessionId: 'session-1',
    model: 'gpt-6-sol',
  });
  assert.deepStrictEqual(codexRework.args.slice(0, 2), ['-c', 'model="gpt-6-sol"']);
  assert.ok(codexRework.args.includes('resume'));

  const claude = runner.buildCommand({
    agent: 'claude_code',
    prompt: 'ship it',
    isRework: false,
    receiverSessionId: '11111111-1111-1111-1111-111111111111',
    model: 'sonnet',
  });
  const claudeModelIndex = claude.args.indexOf('--model');
  assert.ok(claudeModelIndex >= 0, claude.args.join(' '));
  assert.strictEqual(claude.args[claudeModelIndex + 1], 'sonnet');

  const agy = runner.buildCommand({
    agent: 'antigravity',
    prompt: 'ship it',
    isRework: false,
    receiverSessionId: 'conv-1',
    model: 'gemini-3.8-flash-high',
  });
  const agyModelIndex = agy.args.indexOf('--model');
  assert.ok(agyModelIndex >= 0, agy.args.join(' '));
  assert.strictEqual(agy.args[agyModelIndex + 1], 'gemini-3.8-flash-high');
});

test('Model catalog - configured default is used when no model is pinned', () => {
  const { runner } = makeRunner();
  runner.setModelResolver((agent) => (agent === 'codex' ? 'gpt-6-luna' : undefined));

  const codex = runner.buildCommand({
    agent: 'codex',
    prompt: 'ship it',
    isRework: false,
    receiverSessionId: 'session-1',
  });
  assert.ok(codex.args.includes('model="gpt-6-luna"'), codex.args.join(' '));

  const antigravity = runner.buildCommand({
    agent: 'antigravity',
    prompt: 'ship it',
    isRework: false,
    receiverSessionId: 'conv-1',
  });
  assert.ok(!antigravity.args.includes('--model'), 'no configured default means no flag');

  // An explicit model always wins over the configured default.
  assert.strictEqual(runner.resolveModel('codex', 'gpt-6-sol'), 'gpt-6-sol');
  assert.strictEqual(runner.resolveModel('codex'), 'gpt-6-luna');
});

test('Model catalog - DSH runs get a per-run overlay patch', () => {
  const root = makeTempDir('mesh-dsh-');
  const bridgeHome = path.join(root, 'bridge');
  const dshHome = path.join(root, 'dsh');
  const launcher = path.join(root, 'resources', 'runtime', 'cli', 'bin', 'dsh.cmd');
  fs.mkdirSync(path.dirname(launcher), { recursive: true });
  fs.writeFileSync(launcher, '@echo off\r\n', 'utf8');

  const profileDir = path.join(dshHome, 'profiles', 'headless');
  fs.mkdirSync(profileDir, { recursive: true });
  const providerBlock = buildDshProviderPatchBlock({
    provider: 'commandcode',
    api: 'openai-completions',
    baseUrl: 'https://api.commandcode.ai/provider/v1',
    model: 'deepseek/deepseek-v4.1-flash',
    apiKeyEnv: 'COMMANDCODE_API_KEY',
  });
  const profilePatch = path.join(profileDir, 'cordis.patch.yml');
  fs.writeFileSync(profilePatch, `${providerBlock}\n`, 'utf8');
  const before = fs.readFileSync(profilePatch, 'utf8');

  withEnv({ AGENT_MESH_BRIDGE_HOME: bridgeHome, DSH_HOME: dshHome }, () => {
    const detector = new AgentDetector({ deepseek_harness: launcher });
    const runner = new AutonomousRunner(detector);
    const built = runner.buildCommand({
      agent: 'deepseek_harness',
      prompt: 'ship it',
      isRework: false,
      receiverSessionId: 'session-1',
      model: 'deepseek/deepseek-v4.1-flash-fast',
    });

    const patchIndex = built.args.indexOf('--patch');
    assert.ok(patchIndex >= 0, built.args.join(' '));
    const overlayPath = built.args[patchIndex + 1];
    assert.ok(fs.existsSync(overlayPath), `overlay missing: ${overlayPath}`);
    assert.ok(overlayPath.startsWith(bridgeHome), overlayPath);

    const overlay = fs.readFileSync(overlayPath, 'utf8');
    assert.ok(overlay.includes("model: 'deepseek/deepseek-v4.1-flash-fast'"), overlay);
    assert.ok(overlay.includes('provider: commandcode'), overlay);
    assert.ok(overlay.includes("apiKeyEnv: 'COMMANDCODE_API_KEY'"), overlay);

    // The user's own profile patch is never rewritten by a per-task override.
    assert.strictEqual(fs.readFileSync(profilePatch, 'utf8'), before);
  });
});

test('Model catalog - DSH refuses an overlay when no provider route exists', () => {
  const root = makeTempDir('mesh-dsh-noroute-');
  withEnv(
    {
      AGENT_MESH_BRIDGE_HOME: path.join(root, 'bridge'),
      DSH_HOME: path.join(root, 'dsh-empty'),
    },
    () => {
      assert.strictEqual(writeDshModelOverlay('deepseek-chat'), undefined);
    },
  );
});

// ---------------------------------------------------------------------------
// Sender -> receiver model routing recorded on every task
// ---------------------------------------------------------------------------

test('Model routing - sender model resolution precedence', () => {
  const dir = makeTempDir('mesh-routing-');
  saveModelDefaults({ codex: 'gpt-6-sol' }, dir);

  withEnv({ CODEX_MODEL: 'gpt-6.1-sol' }, () => {
    // An explicit value always wins over the environment and configuration.
    assert.deepStrictEqual(resolveInitiatorModel('codex', 'gpt-6-astra', dir), {
      model: 'gpt-6-astra',
      source: 'explicit',
    });
    // Then the environment advertisement of the live session.
    assert.deepStrictEqual(resolveInitiatorModel('codex', undefined, dir), {
      model: 'gpt-6.1-sol',
      source: 'env',
    });
  });

  // Without an env hint it falls back to the configured default...
  withEnv({ CODEX_MODEL: undefined }, () => {
    invalidateConfiguredModelCache();
    assert.deepStrictEqual(resolveInitiatorModel('codex', undefined, dir), {
      model: 'gpt-6-sol',
      source: 'configured',
    });
    // ...and reports "none" when nothing is known at all.
    assert.deepStrictEqual(resolveInitiatorModel('antigravity', undefined, dir), { source: 'none' });
  });
});

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  return {
    threadId: 'thread_test',
    title: 'Test task',
    initiatorAgent: 'codex',
    targetAgent: 'antigravity',
    status: 'running',
    currentRound: 1,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    receiverSessionId: 'session-x',
    workspacePath: 'D:\\agent_bridge',
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds: [],
    ...overrides,
  };
}

test('Model routing - both ends are summarized for tasks and messages', () => {
  const thread = makeThread({
    initiatorModel: 'gpt-6.1-sol',
    initiatorModelSource: 'explicit',
    targetModel: 'gemini-3.8-flash-high',
  });

  const routing = getModelRouting(thread);
  assert.strictEqual(routing.initiatorModel, 'gpt-6.1-sol');
  assert.strictEqual(routing.targetModel, 'gemini-3.8-flash-high');
  assert.strictEqual(routing.initiatorModelSource, 'explicit');
  assert.match(routing.text, /sender codex: gpt-6\.1-sol/);
  assert.match(routing.text, /receiver antigravity: gemini-3\.8-flash-high/);

  const uiText = describeModelRouting(thread);
  assert.match(uiText, /发送 codex\(gpt-6\.1-sol\)/);
  assert.match(uiText, /接收 antigravity\(gemini-3\.8-flash-high\)/);

  // Unknown models are rendered as the agent default, never as an empty field.
  const unknown = describeModelRouting(makeThread());
  assert.match(unknown, /发送 codex\(Agent 默认\)/);
  assert.match(unknown, /接收 antigravity\(Agent 默认\)/);
});
