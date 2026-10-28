/**
 * Per-agent model discovery, configuration and default resolution.
 *
 * Every mesh member exposes its model catalogue differently:
 *
 *  - Antigravity (`agy`)   : `agy models` prints `<id>\t<display name>` lines.
 *  - OpenAI Codex (`codex`): `codex debug models` renders the raw catalog as JSON.
 *  - Claude Code (`claude`): no list subcommand; the Anthropic-compatible
 *    `/v1/models` endpoint of the configured gateway is the authority, with the
 *    documented aliases (`sonnet`/`opus`/`haiku`) as an offline fallback.
 *  - DeepSeek Harness      : no `--model` flag; the model is part of the
 *    `headless` profile's provider route, so the OpenAI-compatible `/models`
 *    endpoint of that provider is queried and the route itself is the fallback.
 *
 * The extension host runs discovery (it owns the VS Code settings), persists the
 * result plus the configured defaults under `~/.agent-mesh-bridge/`, and every
 * standalone MCP server process reads that snapshot. That keeps `dispatch_task`
 * defaulting to the same model the user picked in the UI without giving the MCP
 * process a VS Code dependency.
 */

import * as child_process from 'child_process';
import * as fs from 'fs';
import * as http from 'http';
import * as https from 'https';
import * as os from 'os';
import * as path from 'path';
import { AgentId, ModelResolutionSource } from './types';
import { AgentDetector } from './agent-detector';
import { getDshHome, readDshConfiguredProvider } from './deepseek-harness';
import { needsShellWrapper } from './platform';

/** Bridge storage root; overridable so tests never touch the real home directory. */
export function getBridgeHome(): string {
  const override = process.env.AGENT_MESH_BRIDGE_HOME;
  if (override && override.trim()) return path.resolve(override.trim());
  return path.join(os.homedir(), '.agent-mesh-bridge');
}

/** Snapshot of the user's per-agent default models, shared with MCP processes. */
export const MODEL_CONFIG_FILENAME = 'model-config.json';

/** Cached discovery results (one entry per agent). */
export const MODELS_CACHE_FILENAME = 'models-cache.json';

/** Discovery results older than this are refreshed on demand. */
export const DEFAULT_CATALOG_TTL_MS = 6 * 60 * 60 * 1000;

/** Where a catalogue entry came from, so the UI can explain itself. */
export type ModelSource = 'cli' | 'api' | 'config' | 'builtin';

export interface ModelOption {
  /** Exact id handed to the agent CLI. */
  id: string;
  /** Human-readable name, when the source provides one. */
  label?: string;
  /** Extra context shown in the picker (e.g. "hidden", "alias"). */
  hint?: string;
}

export interface AgentModelCatalog {
  agent: AgentId;
  models: ModelOption[];
  source: ModelSource;
  updatedAt: string;
  /** Populated when discovery fell back because the probe failed. */
  error?: string;
}

export type ModelDefaults = Partial<Record<AgentId, string>>;

interface ModelConfigFile {
  updatedAt?: string;
  models?: ModelDefaults;
  /**
   * Whether pinning a receiver model other than the configured default requires
   * the user's click consent in VS Code. Absent/false-y means "not configured";
   * only an explicit `false` turns the gate off.
   */
  requireModelApproval?: boolean;
}

/** Raw snapshot reader; missing/corrupt file -> undefined. */
function readModelConfigFile(dir: string): ModelConfigFile | undefined {
  try {
    const file = path.join(dir, MODEL_CONFIG_FILENAME);
    if (!fs.existsSync(file)) return undefined;
    return JSON.parse(fs.readFileSync(file, 'utf8')) as ModelConfigFile;
  } catch {
    return undefined;
  }
}

/**
 * Whether a non-default receiver model must be confirmed by the user.
 *
 * Defaults to `true`: switching an agent away from the model the user configured
 * is an explicit decision the user makes, not one a caller agent may make on
 * their behalf.
 */
export function loadModelApprovalRequired(dir: string = getBridgeHome()): boolean {
  const parsed = readModelConfigFile(dir);
  return parsed?.requireModelApproval !== false;
}

/** Trim a model id and treat an empty value as "not configured". */
export function normalizeModelId(value?: string | null): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/** Read the mirrored per-agent defaults (missing/corrupt file -> empty map). */
export function loadModelDefaults(dir: string = getBridgeHome()): ModelDefaults {
  const file = path.join(dir, MODEL_CONFIG_FILENAME);
  try {
    if (!fs.existsSync(file)) return {};
    const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as ModelConfigFile;
    const result: ModelDefaults = {};
    for (const [agent, model] of Object.entries(parsed.models || {})) {
      const normalized = normalizeModelId(model);
      if (normalized) result[agent as AgentId] = normalized;
    }
    return result;
  } catch {
    return {};
  }
}

/** Persist the per-agent defaults for every MCP process to read. */
export function saveModelDefaults(
  models: ModelDefaults,
  dir: string = getBridgeHome(),
  options: { requireModelApproval?: boolean } = {},
): void {
  const file = path.join(dir, MODEL_CONFIG_FILENAME);
  const clean: ModelDefaults = {};
  for (const [agent, model] of Object.entries(models)) {
    const normalized = normalizeModelId(model);
    if (normalized) clean[agent as AgentId] = normalized;
  }
  try {
    fs.mkdirSync(dir, { recursive: true });
    const payload: ModelConfigFile = { updatedAt: new Date().toISOString(), models: clean };
    // Keep the approval policy sticky: callers that only mirror the model
    // settings must not silently reset it.
    const nextPolicy =
      options.requireModelApproval !== undefined
        ? options.requireModelApproval
        : readModelConfigFile(dir)?.requireModelApproval;
    if (nextPolicy !== undefined) payload.requireModelApproval = nextPolicy;
    fs.writeFileSync(file, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  } catch (e: any) {
    console.warn(`[ModelCatalog] Could not persist model defaults: ${e?.message || e}`);
  }
}

/**
 * Put the configured default model first and tag it in place.
 *
 * `list_agent_models` used to bury `configuredModel` after the whole probe list,
 * so a caller skimming the (provider-ordered) catalogue saw whichever id came
 * first — `deepseek/deepseek-v4-pro` before the configured Flash route — and
 * pinned it. Marking the default in the list itself removes that trap.
 */
export function annotateConfiguredModel(
  models: ModelOption[],
  configured?: string | null,
): ModelOption[] {
  const target = normalizeModelId(configured);
  if (!target) return models.map((m) => ({ ...m }));

  const tagged = 'configured default';
  const found = models.find((m) => m.id === target);
  const head: ModelOption = found
    ? { ...found, hint: found.hint ? `${found.hint} · ${tagged}` : tagged }
    : { id: target, hint: `${tagged} (not returned by the probe)` };

  const rest = models.filter((m) => m.id !== target).map((m) => ({ ...m }));
  return [head, ...rest];
}

const configuredCache = new Map<string, { value?: string; at: number }>();
const CONFIGURED_CACHE_TTL_MS = 2000;

/**
 * The default model configured for an agent, or `undefined` when the agent
 * should use whatever model it ships with.
 */
export function resolveConfiguredModel(
  agent: AgentId,
  dir: string = getBridgeHome(),
): string | undefined {
  const key = `${dir}::${agent}`;
  const cached = configuredCache.get(key);
  if (cached && Date.now() - cached.at < CONFIGURED_CACHE_TTL_MS) return cached.value;
  const value = loadModelDefaults(dir)[agent];
  configuredCache.set(key, { value, at: Date.now() });
  return value;
}

/** Drop the memoized configured-model lookup (used after saving defaults). */
export function invalidateConfiguredModelCache(): void {
  configuredCache.clear();
}

/**
 * Environment variables a running agent may use to advertise the model of the
 * session the bridge MCP server is embedded in. They are hints only: an agent
 * that does not export its model simply reports "configured/none".
 */
const INITIATOR_MODEL_ENV: Partial<Record<AgentId, string[]>> = {
  antigravity: ['AGY_MODEL', 'GEMINI_MODEL'],
  codex: ['CODEX_MODEL', 'OPENAI_MODEL'],
  claude_code: ['ANTHROPIC_MODEL', 'CLAUDE_MODEL'],
  deepseek_harness: ['DSH_MODEL', 'DEEPSEEK_MODEL'],
};

/**
 * Best-effort model of the *sender* agent for a task's routing badge.
 *
 * Precedence: explicit argument (the caller knows best) -> environment hint ->
 * the sender's configured default. The source is returned so the UI can be
 * honest about a value that was merely inferred.
 */
export function resolveInitiatorModel(
  agent: AgentId,
  explicit?: string,
  dir: string = getBridgeHome(),
): { model?: string; source: ModelResolutionSource } {
  const normalized = normalizeModelId(explicit);
  if (normalized) return { model: normalized, source: 'explicit' };

  for (const name of INITIATOR_MODEL_ENV[agent] || []) {
    const fromEnv = normalizeModelId(process.env[name]);
    if (fromEnv) return { model: fromEnv, source: 'env' };
  }

  const configured = resolveConfiguredModel(agent, dir);
  if (configured) return { model: configured, source: 'configured' };
  return { source: 'none' };
}

// ---------------------------------------------------------------------------
// Output parsers (exported so they can be exercised without invoking a CLI)
// ---------------------------------------------------------------------------

/**
 * Parse `agy models` output.
 *
 * Observed shape:
 *   Fetching available models...
 *   gemini-3.8-flash-high<TAB>Gemini 3.8 Flash (High)
 */
export function parseAgyModels(stdout: string): ModelOption[] {
  const options: ModelOption[] = [];
  const seen = new Set<string>();
  for (const rawLine of (stdout || '').split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || /^fetching\b/i.test(line)) continue;
    const parts = line.split(/\t+|\s{2,}/).map((p) => p.trim()).filter(Boolean);
    const id = parts[0];
    if (!id || /\s/.test(id)) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    options.push({ id, label: parts.slice(1).join(' ') || undefined });
  }
  return options;
}

/** Parse the JSON rendered by `codex debug models`. */
export function parseCodexModels(stdout: string): ModelOption[] {
  const parsed = extractJsonObject(stdout);
  if (!parsed) return [];
  const list = Array.isArray(parsed.models)
    ? parsed.models
    : Array.isArray(parsed.data)
      ? parsed.data
      : [];
  const options: ModelOption[] = [];
  const seen = new Set<string>();
  for (const entry of list as any[]) {
    const id = normalizeModelId(entry?.slug || entry?.id);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    options.push({
      id,
      label: typeof entry?.display_name === 'string' ? entry.display_name : undefined,
      hint: entry?.visibility === 'hide' ? 'hidden' : undefined,
    });
  }
  return options;
}

/** Parse an Anthropic/OpenAI-compatible `{ data: [{ id, display_name? }] }` body. */
export function parseModelListPayload(payload: any): ModelOption[] {
  const list = Array.isArray(payload?.data)
    ? payload.data
    : Array.isArray(payload?.models)
      ? payload.models
      : [];
  const options: ModelOption[] = [];
  const seen = new Set<string>();
  for (const entry of list) {
    const id = normalizeModelId(typeof entry === 'string' ? entry : entry?.id || entry?.slug);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const label =
      typeof entry === 'object' && typeof entry?.display_name === 'string'
        ? entry.display_name
        : undefined;
    options.push({ id, label });
  }
  return options;
}

/**
 * Locate the JSON object inside CLI output that may be preceded by warnings or
 * banners. Returns `undefined` when no balanced object can be parsed.
 */
export function extractJsonObject(raw: string): any | undefined {
  const text = raw || '';
  const start = text.indexOf('{');
  if (start === -1) return undefined;
  const end = text.lastIndexOf('}');
  if (end <= start) return undefined;
  try {
    return JSON.parse(text.slice(start, end + 1));
  } catch {
    return undefined;
  }
}

/** Directory the DSH overlay patches are written to. */
export function getDshOverlayDir(): string {
  return path.join(getBridgeHome(), 'model-overlays');
}

/**
 * Ensure a DSH overlay patch exists for `model` and return its path.
 *
 * Returns `undefined` when no provider route is configured: DSH's overlay
 * replaces the whole `agent-default-model` config, so writing only a model would
 * silently drop the provider (verified with `dsh headless --dump-config`).
 */
export function writeDshModelOverlay(model: string): string | undefined {
  const provider = readDshConfiguredProvider();
  if (!provider) return undefined;

  const dir = getDshOverlayDir();
  const safe = model.replace(/[^A-Za-z0-9._-]/g, '_').slice(0, 80);
  const file = path.join(dir, `${safe}.yml`);
  const body = [
    '# Generated by agent-mesh-bridge for one bridge-launched `dsh headless` run.',
    '# Overrides the model route for this run only; the profile patch is untouched.',
    '- id: agent-default-model',
    '  config:',
    `    provider: ${provider.provider.trim()}`,
    `    model: '${model.replace(/'/g, "''")}'`,
    '- id: llm-pi-ai',
    '  config:',
    '    providers:',
    `      ${provider.provider.trim()}:`,
    `        displayName: '${provider.provider.trim()}'`,
    `        api: ${provider.api.trim() || 'openai-completions'}`,
    `        baseURL: '${provider.baseUrl.trim().replace(/'/g, "''")}'`,
    `        apiKeyEnv: '${provider.apiKeyEnv.trim()}'`,
    '        models:',
    `          - id: '${model.replace(/'/g, "''")}'`,
    `            name: '${model.replace(/'/g, "''")}'`,
    '            contextWindow: 1000000',
    '            maxTokens: 32768',
    '            input:',
    '              - text',
    '',
  ].join('\n');

  try {
    fs.mkdirSync(dir, { recursive: true });
    const existing = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : undefined;
    if (existing !== body) fs.writeFileSync(file, body, 'utf8');
    return file;
  } catch (e: any) {
    console.warn(`[ModelCatalog] Could not write DSH model overlay: ${e?.message || e}`);
    return undefined;
  }
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

export interface ModelCatalogOptions {
  /** Cache TTL for in-memory and on-disk catalogues. */
  ttlMs?: number;
  /** Storage root, defaulting to the bridge home. */
  storageDir?: string;
  /** Per-probe timeout. */
  timeoutMs?: number;
}

interface StoredCatalogs {
  [agent: string]: { models: ModelOption[]; source: ModelSource; updatedAt: string; error?: string };
}

export class ModelCatalog {
  private detector: AgentDetector;
  private ttlMs: number;
  private storageDir: string;
  private timeoutMs: number;
  private memory = new Map<AgentId, { catalog: AgentModelCatalog; at: number }>();

  constructor(detector: AgentDetector, options: ModelCatalogOptions = {}) {
    this.detector = detector;
    this.ttlMs = options.ttlMs ?? DEFAULT_CATALOG_TTL_MS;
    this.storageDir = options.storageDir ?? getBridgeHome();
    this.timeoutMs = options.timeoutMs ?? 20000;
  }

  public async get(agent: AgentId, forceRefresh: boolean = false): Promise<AgentModelCatalog> {
    if (agent === 'vscode_chat') {
      return {
        agent,
        models: [{ id: 'builtin', label: 'VSCode Builtin Model' }],
        source: 'builtin',
        updatedAt: new Date().toISOString(),
      };
    }

    const memoryHit = this.memory.get(agent);
    if (!forceRefresh && memoryHit && Date.now() - memoryHit.at < this.ttlMs) {
      return memoryHit.catalog;
    }

    if (!forceRefresh) {
      const stored = this.readCache()[agent];
      if (stored && Date.now() - new Date(stored.updatedAt).getTime() < this.ttlMs) {
        const catalog: AgentModelCatalog = { agent, ...stored };
        this.memory.set(agent, { catalog, at: Date.now() });
        return catalog;
      }
    }

    const catalog = await this.discover(agent);
    this.memory.set(agent, { catalog, at: Date.now() });
    if (catalog.models.length > 0) this.writeCache(agent, catalog);
    return catalog;
  }

  public async getAll(forceRefresh: boolean = false): Promise<Record<AgentId, AgentModelCatalog>> {
    const agents: AgentId[] = ['antigravity', 'codex', 'claude_code', 'deepseek_harness'];
    const entries = await Promise.all(agents.map(async (a) => [a, await this.get(a, forceRefresh)] as const));
    return Object.fromEntries(entries) as Record<AgentId, AgentModelCatalog>;
  }

  /** Probe one agent without consulting any cache. */
  public async discover(agent: AgentId): Promise<AgentModelCatalog> {
    const now = new Date().toISOString();
    try {
      switch (agent) {
        case 'antigravity':
          return { agent, ...(await this.probeAntigravity()), updatedAt: now };
        case 'codex':
          return { agent, ...(await this.probeCodex()), updatedAt: now };
        case 'claude_code':
          return { agent, ...(await this.probeClaude()), updatedAt: now };
        case 'deepseek_harness':
          return { agent, ...(await this.probeDsh()), updatedAt: now };
        default:
          return { agent, models: [], source: 'builtin', updatedAt: now, error: 'Unsupported agent' };
      }
    } catch (e: any) {
      return { agent, models: [], source: 'builtin', updatedAt: now, error: e?.message || String(e) };
    }
  }

  private async probeAntigravity(): Promise<Omit<AgentModelCatalog, 'agent' | 'updatedAt'>> {
    const exec = this.detector.resolveExecutable('antigravity');
    if (!exec) {
      return { models: [], source: 'cli', error: 'Antigravity executable not found' };
    }
    const probe = await runCli(exec, ['models'], this.timeoutMs);
    const models = parseAgyModels(probe.stdout);
    if (models.length > 0) return { models, source: 'cli' };
    return {
      models: [],
      source: 'cli',
      error: probe.stderr.trim() || 'agy models returned no parsable output',
    };
  }

  private async probeCodex(): Promise<Omit<AgentModelCatalog, 'agent' | 'updatedAt'>> {
    const exec = this.detector.resolveExecutable('codex');
    if (exec) {
      const probe = await runCli(exec, ['debug', 'models'], this.timeoutMs);
      const models = parseCodexModels(probe.stdout);
      if (models.length > 0) return { models, source: 'cli' };
    }

    // Fallback 1: Codex's own on-disk catalogue cache.
    const cached = readCodexModelsCache();
    if (cached.length > 0) {
      return {
        models: cached,
        source: 'config',
        error: exec ? '`codex debug models` failed; using the local models cache' : undefined,
      };
    }

    // Fallback 2: whatever model its config already names.
    const configured = readTomlString(path.join(os.homedir(), '.codex', 'config.toml'), 'model');
    if (configured) {
      return { models: [{ id: configured }], source: 'config' };
    }

    return {
      models: [],
      source: 'cli',
      error: exec ? 'Could not read the Codex model catalogue' : 'Codex executable not found',
    };
  }

  private async probeClaude(): Promise<Omit<AgentModelCatalog, 'agent' | 'updatedAt'>> {
    const settings = readClaudeSettings();
    const baseUrl = normalizeModelId(
      settings?.env?.ANTHROPIC_BASE_URL || process.env.ANTHROPIC_BASE_URL,
    );
    const token = normalizeModelId(
      settings?.env?.ANTHROPIC_AUTH_TOKEN ||
        settings?.env?.ANTHROPIC_API_KEY ||
        process.env.ANTHROPIC_AUTH_TOKEN ||
        process.env.ANTHROPIC_API_KEY,
    );

    let error: string | undefined;
    if (baseUrl) {
      const url = /\/v1\/?$/.test(baseUrl) ? `${baseUrl.replace(/\/+$/, '')}/models` : `${baseUrl.replace(/\/+$/, '')}/v1/models`;
      try {
        const payload = await httpGetJson(
          url,
          {
            authorization: token ? `Bearer ${token}` : undefined,
            'x-api-key': token,
            'anthropic-version': '2023-06-01',
          },
          Math.min(this.timeoutMs, 15000),
        );
        const models = parseModelListPayload(payload);
        if (models.length > 0) return { models, source: 'api' };
        error = 'The model endpoint returned no models';
      } catch (e: any) {
        error = `Model endpoint unreachable: ${e?.message || e}`;
      }
    }

    // Offline fallback: the documented aliases plus the gateway alias mapping
    // (a gateway install commonly pins e.g. sonnet -> glm-5.2 in settings.json).
    const models = claudeFallbackModels(settings);
    return { models, source: 'builtin', error };
  }

  private async probeDsh(): Promise<Omit<AgentModelCatalog, 'agent' | 'updatedAt'>> {
    const provider = readDshConfiguredProvider();
    const baseUrl = normalizeModelId(provider?.baseUrl || process.env.DSH_BASE_URL);
    const apiKeyEnv = normalizeModelId(provider?.apiKeyEnv);
    const token = apiKeyEnv
      ? normalizeModelId(process.env[apiKeyEnv])
      : normalizeModelId(process.env.DEEPSEEK_API_KEY);

    if (baseUrl) {
      const url = `${baseUrl.replace(/\/+$/, '')}/models`;
      try {
        const payload = await httpGetJson(
          url,
          { authorization: token ? `Bearer ${token}` : undefined, 'x-api-key': token },
          Math.min(this.timeoutMs, 15000),
        );
        const models = parseModelListPayload(payload);
        if (models.length > 0) return { models, source: 'api' };
      } catch (e: any) {
        // Fall through to the configured route below.
        return {
          models: dshFallbackModels(provider?.model),
          source: 'config',
          error: `Model endpoint unreachable: ${e?.message || e}`,
        };
      }
    }

    return { models: dshFallbackModels(provider?.model), source: 'config' };
  }

  private cacheFile(): string {
    return path.join(this.storageDir, MODELS_CACHE_FILENAME);
  }

  private readCache(): StoredCatalogs {
    try {
      const file = this.cacheFile();
      if (!fs.existsSync(file)) return {};
      return JSON.parse(fs.readFileSync(file, 'utf8')) as StoredCatalogs;
    } catch {
      return {};
    }
  }

  private writeCache(agent: AgentId, catalog: AgentModelCatalog): void {
    try {
      const all = this.readCache();
      all[agent] = {
        models: catalog.models,
        source: catalog.source,
        updatedAt: catalog.updatedAt,
        error: catalog.error,
      };
      fs.mkdirSync(this.storageDir, { recursive: true });
      fs.writeFileSync(this.cacheFile(), `${JSON.stringify(all, null, 2)}\n`, 'utf8');
    } catch (e: any) {
      console.warn(`[ModelCatalog] Could not persist the model cache: ${e?.message || e}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

interface CliResult {
  stdout: string;
  stderr: string;
  code: number;
}

/** Run a short-lived CLI probe with stdin closed (never let it block on input). */
export function runCli(command: string, args: string[], timeoutMs: number): Promise<CliResult> {
  return new Promise((resolve) => {
    const wrap = needsShellWrapper(command);
    const spawnCommand = wrap ? 'cmd.exe' : command;
    const spawnArgs = wrap ? ['/c', command, ...args] : args;

    let stdout = '';
    let stderr = '';
    let settled = false;
    const finish = (code: number) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve({ stdout, stderr, code });
    };

    let proc: child_process.ChildProcess;
    try {
      proc = child_process.spawn(spawnCommand, spawnArgs, {
        windowsHide: true,
        stdio: ['pipe', 'pipe', 'pipe'],
        env: { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1', CI: '1' },
      });
    } catch (e: any) {
      resolve({ stdout: '', stderr: String(e?.message || e), code: -1 });
      return;
    }

    const timer = setTimeout(() => {
      try {
        proc.kill();
      } catch {}
      finish(-1);
    }, timeoutMs);

    try {
      proc.stdin?.end();
    } catch {}

    proc.stdout?.on('data', (d) => {
      stdout += d.toString();
    });
    proc.stderr?.on('data', (d) => {
      stderr += d.toString();
    });
    proc.on('error', (e) => {
      stderr += String(e?.message || e);
      finish(-1);
    });
    proc.on('close', (code) => finish(code ?? 0));
  });
}

/** Minimal JSON GET; only used for model listings, with a response size cap. */
export function httpGetJson(
  url: string,
  headers: Record<string, string | undefined>,
  timeoutMs: number,
  redirectsLeft: number = 3,
): Promise<any> {
  return new Promise((resolve, reject) => {
    let parsedUrl: URL;
    try {
      parsedUrl = new URL(url);
    } catch {
      reject(new Error(`Invalid URL: ${url}`));
      return;
    }

    const client = parsedUrl.protocol === 'http:' ? http : https;
    const request = client.request(
      {
        protocol: parsedUrl.protocol,
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || undefined,
        path: `${parsedUrl.pathname}${parsedUrl.search}`,
        method: 'GET',
        headers: Object.fromEntries(
          Object.entries(headers).filter(([, v]) => typeof v === 'string' && v.length > 0),
        ) as Record<string, string>,
        timeout: timeoutMs,
      },
      (res) => {
        const status = res.statusCode || 0;
        if (status >= 300 && status < 400 && res.headers.location) {
          res.resume();
          if (redirectsLeft <= 0) {
            reject(new Error(`Too many redirects for ${url}`));
            return;
          }
          const next = new URL(res.headers.location, parsedUrl).toString();
          httpGetJson(next, headers, timeoutMs, redirectsLeft - 1).then(resolve, reject);
          return;
        }
        if (status < 200 || status >= 300) {
          res.resume();
          reject(new Error(`HTTP ${status}`));
          return;
        }

        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk: string) => {
          body += chunk;
          if (body.length > 8 * 1024 * 1024) {
            request.destroy(new Error('Model listing response too large'));
          }
        });
        res.on('end', () => {
          try {
            resolve(JSON.parse(body));
          } catch {
            reject(new Error('Model endpoint did not return JSON'));
          }
        });
      },
    );

    request.on('timeout', () => request.destroy(new Error('Request timed out')));
    request.on('error', (e) => reject(e));
    request.end();
  });
}

/** Read Codex's cached catalogue (`~/.codex/models_cache.json`). */
function readCodexModelsCache(): ModelOption[] {
  try {
    const file = path.join(os.homedir(), '.codex', 'models_cache.json');
    if (!fs.existsSync(file)) return [];
    return parseCodexModels(fs.readFileSync(file, 'utf8'));
  } catch {
    return [];
  }
}

/** Read a top-level string assignment from a TOML file. */
function readTomlString(file: string, key: string): string | undefined {
  try {
    if (!fs.existsSync(file)) return undefined;
    const text = fs.readFileSync(file, 'utf8');
    const match = text.match(new RegExp(`^\\s*${key}\\s*=\\s*"([^"]*)"`, 'm'));
    return normalizeModelId(match?.[1]);
  } catch {
    return undefined;
  }
}

interface ClaudeSettings {
  model?: string;
  env?: Record<string, string>;
}

function readClaudeSettings(): ClaudeSettings | undefined {
  try {
    const file = path.join(os.homedir(), '.claude', 'settings.json');
    if (!fs.existsSync(file)) return undefined;
    return JSON.parse(fs.readFileSync(file, 'utf8')) as ClaudeSettings;
  } catch {
    return undefined;
  }
}

/** Aliases Claude Code documents plus any gateway alias mapping it pins. */
function claudeFallbackModels(settings?: ClaudeSettings): ModelOption[] {
  const options: ModelOption[] = [];
  const seen = new Set<string>();
  const push = (id: string | undefined, label?: string, hint?: string) => {
    const normalized = normalizeModelId(id);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    options.push({ id: normalized, label, hint });
  };

  push(settings?.model, 'configured default', 'settings.json');
  for (const alias of ['sonnet', 'opus', 'haiku']) {
    push(alias, `Claude ${alias[0].toUpperCase()}${alias.slice(1)} (alias)`, 'alias');
  }

  const env = settings?.env || {};
  const mappings: Array<[string, string]> = [
    ['ANTHROPIC_DEFAULT_SONNET_MODEL', 'sonnet'],
    ['ANTHROPIC_DEFAULT_OPUS_MODEL', 'opus'],
    ['ANTHROPIC_DEFAULT_HAIKU_MODEL', 'haiku'],
  ];
  for (const [envName, alias] of mappings) {
    const value = normalizeModelId(env[envName]) || normalizeModelId(process.env[envName]);
    if (value) push(value, `${alias} -> ${value}`, 'gateway mapping');
  }
  return options;
}

/** DSH's provider route plus the official ids it can always resolve. */
function dshFallbackModels(configured?: string): ModelOption[] {
  const options: ModelOption[] = [];
  const seen = new Set<string>();
  const push = (id: string | undefined, hint?: string) => {
    const normalized = normalizeModelId(id);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    options.push({ id: normalized, hint });
  };
  push(configured, 'profile route');
  if (!configured) {
    push('deepseek-chat', 'official');
    push('deepseek-reasoner', 'official');
  }
  return options;
}

/** Exposed for diagnostics: the DSH home this process would read. */
export function describeDshHome(): string {
  return getDshHome();
}
