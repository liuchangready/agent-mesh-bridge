/**
 * DeepSeek Harness (`dsh`) integration surface.
 *
 * Unlike the other mesh members, DSH is an Electron desktop application that
 * exposes a CLI through a profile launcher (`resources/runtime/cli/bin/dsh.cmd`)
 * rather than a standalone binary. This module owns every DSH-specific detail:
 *
 *  - install discovery and launcher/CLI resolution,
 *  - the non-interactive `headless` invocation contract,
 *  - native session discovery for automatic caller wake-up,
 *  - MCP server registration into the profile patch layers.
 *
 * Key CLI contract (verified against dsh 0.2.1-rc.2):
 *   dsh headless --json "<task>"                          -> answer one task and exit
 *   dsh headless --json --session-id <id> "<follow-up>"   -> resume a persisted Session
 *
 * `--session-id` adopts an *existing* session and errors on an unknown id, so a
 * fresh DSH task cannot be given a pre-minted id the way Claude's `--session-id`
 * works. The native id is instead emitted as the first JSON stream event
 * (`{"type":"session","sessionId":"session-<uuid>",...}`) and adopted by the
 * dispatcher for subsequent rework rounds.
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';

/** Canonical mesh id for DeepSeek Harness. */
export const DSH_AGENT_ID = 'deepseek_harness';

/** MCP namespace registered inside DSH (must match /^[A-Za-z0-9_-]{1,32}$/). */
export const DSH_MCP_SERVER_NAME = 'agent-mesh-bridge';

/** Marker delimiting the MCP block this extension owns inside `cordis.patch.yml`. */
const MANAGED_BEGIN = '# >>> agent-mesh-bridge (managed) >>>';
const MANAGED_END = '# <<< agent-mesh-bridge (managed) <<<';

/** Marker delimiting the model-provider block owned by this extension. */
const PROVIDER_BEGIN = '# >>> agent-mesh-bridge (provider) >>>';
const PROVIDER_END = '# <<< agent-mesh-bridge (provider) <<<';

/**
 * Profile that bridge-launched (`dsh headless`) runs boot.
 *
 * Only this profile gets the model-provider override: desktop/web keep using
 * the signed-in account so the user's own app behaviour is untouched.
 */
const HEADLESS_PROFILE = 'headless';

interface ManagedMarkers {
  begin: string;
  end: string;
}

const MCP_MARKERS: ManagedMarkers = { begin: MANAGED_BEGIN, end: MANAGED_END };
const PROVIDER_MARKERS: ManagedMarkers = { begin: PROVIDER_BEGIN, end: PROVIDER_END };

/**
 * Env var through which the extension host tells this module which credential
 * variable the configured DSH provider resolves its key from. Keeps the probe
 * free of any VS Code dependency.
 */
export const DSH_PROVIDER_ENV_HINT = 'DSH_BRIDGE_PROVIDER_ENV';

/** Env var carrying the configured provider id, for human-readable status text. */
export const DSH_PROVIDER_ID_HINT = 'DSH_BRIDGE_PROVIDER_ID';

/**
 * A model provider registered into the DSH `headless` profile.
 *
 * DSH's `headless` profile ships with the `deepseek-official` API-key route
 * only, so any other endpoint (a gateway, a proxy, a reseller) has to be
 * declared as a provider in the profile's patch layer.
 */
export interface DshProviderConfig {
  /** Provider id referenced by `agent-default-model` (e.g. `commandcode`). */
  provider: string;
  /** Wire protocol understood by the endpoint. */
  api: string;
  /** Provider base URL. */
  baseUrl: string;
  /** Model id to make the default (e.g. `deepseek/deepseek-v4.1-flash`). */
  model: string;
  /** Environment variable DSH reads the key from. */
  apiKeyEnv: string;
}

/** True when a provider config names everything DSH needs to boot the route. */
export function isDshProviderUsable(provider?: Partial<DshProviderConfig> | null): boolean {
  return !!(
    provider &&
    provider.provider &&
    provider.provider.trim() &&
    provider.baseUrl &&
    provider.baseUrl.trim() &&
    provider.model &&
    provider.model.trim() &&
    provider.apiKeyEnv &&
    provider.apiKeyEnv.trim()
  );
}

/** Relative path from an install root to the profile launcher. */
const LAUNCHER_RELATIVE = path.join('resources', 'runtime', 'cli', 'bin');

/** Relative path from an install root to the desktop-host CLI entry. */
const CLI_RELATIVE = path.join(
  'resources',
  'app.asar',
  'dsh',
  'node_modules',
  '@deepseek-ai',
  'dsh-desktop-host',
  'lib',
  'cli.js',
);

export interface DshInstall {
  /** Directory the application was installed into. */
  root: string;
  /** Resolved profile launcher (`dsh.cmd` / `dsh`). */
  launcherPath: string;
  /**
   * Preferred spawn target. When `execPath` + `cliPath` are present the CLI is
   * launched through the Electron binary with `ELECTRON_RUN_AS_NODE=1`, which
   * avoids `cmd.exe` batch quoting entirely and gives us a real child process
   * whose stdin can be closed deterministically.
   */
  execPath?: string;
  cliPath?: string;
}

/** Absolute `$DSH_HOME` (defaults to `~/.dsh`). */
export function getDshHome(): string {
  const configured = process.env.DSH_HOME;
  if (configured && configured.trim()) {
    return path.resolve(configured.trim());
  }
  return path.join(os.homedir(), '.dsh');
}

/** Directory holding one folder per bootable profile. */
export function getDshProfilesDir(): string {
  return path.join(getDshHome(), 'profiles');
}

/** Directory holding per-workspace native session stores. */
export function getDshSessionsDir(): string {
  return path.join(getDshHome(), 'sessions');
}

/**
 * Encode a workspace path into DSH's session-store directory name.
 *
 * Runs of disallowed characters collapse into a single `-`, and the result is
 * wrapped in `--`. Observed mappings:
 *   `D:\agent_bridge`              -> `--D-agent_bridge--`
 *   `C:\Users\x\Documents\a b`     -> `--C-Users-x-Documents-a-b--`
 */
export function encodeWorkspaceSlug(workspacePath: string): string {
  const resolved = path.resolve(workspacePath);
  const body = resolved.replace(/[^A-Za-z0-9_-]+/g, '-');
  return `--${body}--`;
}

/**
 * Discover candidate `dsh` launchers, cheapest and most explicit sources first.
 */
export function getDshCandidatePaths(customPath?: string): string[] {
  const candidates: string[] = [];
  const isWindows = process.platform === 'win32';

  if (customPath && customPath.trim()) {
    candidates.push(customPath.trim());
  }

  // Explicit overrides for non-standard installs.
  for (const envName of ['DSH_CLI_PATH', 'DSH_LAUNCHER_PATH']) {
    const value = process.env[envName];
    if (value && value.trim()) candidates.push(value.trim());
  }

  const launcherName = isWindows ? 'dsh.cmd' : 'dsh';
  const home = os.homedir();

  const appendInstallRoot = (root: string) => {
    candidates.push(path.join(root, LAUNCHER_RELATIVE, launcherName));
  };

  if (isWindows) {
    const localAppData = process.env.LOCALAPPDATA || path.join(home, 'AppData', 'Local');
    const programFiles = process.env.ProgramFiles || 'C:\\Program Files';
    const programFilesX86 = process.env['ProgramFiles(x86)'] || 'C:\\Program Files (x86)';
    appendInstallRoot(path.join(localAppData, 'Programs', 'DeepSeek Harness'));
    appendInstallRoot(path.join(programFiles, 'DeepSeek Harness'));
    appendInstallRoot(path.join(programFilesX86, 'DeepSeek Harness'));
  } else if (process.platform === 'darwin') {
    candidates.push('/Applications/DeepSeek Harness.app/Contents/Resources/runtime/cli/bin/dsh');
    candidates.push(
      path.join(
        home,
        'Applications',
        'DeepSeek Harness.app',
        'Contents',
        'Resources',
        'runtime',
        'cli',
        'bin',
        'dsh',
      ),
    );
  } else {
    candidates.push('/opt/DeepSeek Harness/resources/runtime/cli/bin/dsh');
    candidates.push('/usr/lib/deepseek-harness/resources/runtime/cli/bin/dsh');
  }

  // Portable-style installs (`D:\harness`, `D:\DeepSeek Harness`, ...): scan the
  // root of every mounted fixed drive for a directory named after the product.
  if (isWindows) {
    for (const drive of 'CDEFGHIJKLMNOPQRSTUVWXYZ') {
      const driveRoot = `${drive}:\\`;
      let entries: fs.Dirent[];
      try {
        if (!fs.existsSync(driveRoot)) continue;
        entries = fs.readdirSync(driveRoot, { withFileTypes: true });
      } catch {
        continue;
      }
      for (const entry of entries) {
        if (!entry.isDirectory() || !/harness/i.test(entry.name)) continue;
        appendInstallRoot(path.join(driveRoot, entry.name));
      }
    }
  }

  const pathBin = lookupInPath(launcherName);
  if (pathBin) candidates.push(pathBin);

  return Array.from(new Set(candidates));
}

/**
 * Resolve a launcher path into a concrete spawn strategy.
 * Returns `null` when the launcher itself does not exist.
 */
export function resolveDshInstall(launcherPath: string): DshInstall | null {
  try {
    if (!fs.existsSync(launcherPath)) return null;
  } catch {
    return null;
  }

  const base = path.basename(launcherPath).toLowerCase();
  const isLauncherScript = base === 'dsh' || base === 'dsh.cmd' || base === 'dsh.bat';

  // `<root>/resources/runtime/cli/bin/dsh[.cmd]` -> `<root>`
  const root = isLauncherScript
    ? path.resolve(path.dirname(launcherPath), '..', '..', '..', '..')
    : path.dirname(launcherPath);

  const install: DshInstall = { root, launcherPath };

  // Prefer the Electron binary: it avoids cmd.exe batch quoting entirely and
  // gives the supervisor a directly killable process. The CLI entry lives
  // inside `app.asar`, which only DSH's own patched `fs` can stat, so the
  // archive itself is what we verify here; `dsh.cmd` hardcodes the same
  // member path, making it a stable contract of the distribution.
  if (process.platform === 'win32') {
    const exe = path.join(root, 'DeepSeek Harness.exe');
    const asar = path.join(root, 'resources', 'app.asar');
    try {
      if (fs.existsSync(exe) && fs.existsSync(asar)) {
        install.execPath = exe;
        install.cliPath = path.join(root, CLI_RELATIVE);
      }
    } catch {}
  } else if (isLauncherScript) {
    // POSIX launchers are directly executable.
    install.execPath = launcherPath;
  }

  return install;
}

export interface DshHeadlessOptions {
  prompt: string;
  /** Resume the persisted session instead of starting a new one. */
  sessionId?: string;
  /** Emit newline-delimited run events (needed to capture the session id). */
  json?: boolean;
  /**
   * Extra patch layer overriding the model route for this run only.
   *
   * `dsh headless` has no `--model` flag: the model is part of the profile's
   * `agent-default-model` route. Passing a generated overlay keeps the user's
   * profile untouched while still letting one task pick a different model.
   * Verified against the launcher: `dsh headless --patch <file> --dump-config`
   * applies the overlay after the profile layer.
   */
  modelOverlayPath?: string;
}

/**
 * Build the non-interactive `dsh headless` invocation.
 *
 * Returns `null` when no usable spawn strategy could be derived from the given
 * launcher path, letting the caller fall back to the generic path.
 */
export function buildDshHeadlessCommand(
  launcherPath: string,
  options: DshHeadlessOptions,
): { command: string; args: string[]; useShell: boolean; env: Record<string, string> } | null {
  const install = resolveDshInstall(launcherPath);
  if (!install) return null;

  const appArgs: string[] = ['headless'];
  // Launcher options (`--patch`) parse after the profile name; passing them
  // before it makes the launcher reject the invocation.
  if (options.modelOverlayPath) {
    appArgs.push('--patch', options.modelOverlayPath);
  }
  if (options.json !== false) appArgs.push('--json');
  // `--session-id` adopts a persisted Session; never pass it for a fresh task.
  if (options.sessionId) appArgs.push('--session-id', options.sessionId);
  appArgs.push(options.prompt);

  // `dsh-headless` derives both `sandbox-policy.mode` and `user-approval.policy`
  // from this variable (see the composed profile: `danger-full-access` ->
  // approval `never`). Bridge-launched runs are unattended, so the default
  // `workspace-write` + `ask` would leave a delegated task waiting forever on a
  // prompt. This also covers callback wake-ups, which use the same builder.
  const permissionEnv = { DSH_PERMISSION_MODE: 'danger-full-access' };

  if (install.execPath && install.cliPath) {
    return {
      command: install.execPath,
      args: ['--expose-internals', install.cliPath, ...appArgs],
      useShell: false,
      env: { ELECTRON_RUN_AS_NODE: '1', ...permissionEnv },
    };
  }

  // Fallback: run the launcher through the shell. Batch launchers require
  // cmd.exe on Windows; POSIX launchers execute directly.
  if (process.platform === 'win32') {
    return {
      command: 'cmd.exe',
      args: ['/c', install.launcherPath, ...appArgs],
      useShell: false,
      env: permissionEnv,
    };
  }

  return {
    command: install.launcherPath,
    args: appArgs,
    useShell: false,
    env: permissionEnv,
  };
}

/**
 * Find the most recently active native DSH session for a workspace.
 *
 * Native layout: `<dshHome>/sessions/--<slug>--/session-<uuid>/session.v4.jsonl.zstd`
 * The session id is the `session-<uuid>` directory name.
 */
export function detectDshActiveSession(
  workspacePath?: string,
  excludeSessionIds: string[] = [],
): string | undefined {
  const sessionsRoot = getDshSessionsDir();
  if (!fs.existsSync(sessionsRoot)) return undefined;

  const excluded = new Set(excludeSessionIds);
  const candidates: Array<{ sessionId: string; mtime: number }> = [];

  const collectFrom = (workspaceDir: string) => {
    let sessionDirs: fs.Dirent[];
    try {
      sessionDirs = fs.readdirSync(workspaceDir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of sessionDirs) {
      if (!entry.isDirectory() || !entry.name.startsWith('session-')) continue;
      if (excluded.has(entry.name)) continue;
      const dirPath = path.join(workspaceDir, entry.name);
      let mtime: number;
      try {
        const storeFile = path.join(dirPath, 'session.v4.jsonl.zstd');
        mtime = fs.existsSync(storeFile)
          ? fs.statSync(storeFile).mtimeMs
          : fs.statSync(dirPath).mtimeMs;
      } catch {
        continue;
      }
      candidates.push({ sessionId: entry.name, mtime });
    }
  };

  // Prefer sessions recorded for this exact workspace.
  let matchedWorkspace = false;
  if (workspacePath) {
    const slug = encodeWorkspaceSlug(workspacePath);
    let slugDir = path.join(sessionsRoot, slug);
    if (!fs.existsSync(slugDir)) {
      // Drive-letter / path casing can differ from the recorded directory name.
      try {
        const match = fs
          .readdirSync(sessionsRoot, { withFileTypes: true })
          .find((e) => e.isDirectory() && e.name.toLowerCase() === slug.toLowerCase());
        if (match) slugDir = path.join(sessionsRoot, match.name);
      } catch {}
    }
    if (fs.existsSync(slugDir)) {
      collectFrom(slugDir);
      matchedWorkspace = candidates.length > 0;
    }
  }

  // Fall back to any workspace so auto-detection still works when the caller's
  // session store was created under an unexpected slug.
  if (!matchedWorkspace) {
    try {
      for (const entry of fs.readdirSync(sessionsRoot, { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        collectFrom(path.join(sessionsRoot, entry.name));
      }
    } catch {}
  }

  if (candidates.length === 0) return undefined;
  candidates.sort((a, b) => b.mtime - a.mtime);
  return candidates[0].sessionId;
}

/**
 * Read the model route currently declared for `dsh headless`.
 *
 * The declaration lives in the profile patch, NOT in process state: a status
 * query may be answered by any process (the extension host, an agent's own MCP
 * server, ...), so only on-disk configuration gives every caller the same,
 * truthful answer.
 */
export function readDshConfiguredProvider(): DshProviderConfig | undefined {
  try {
    const patchPath = path.join(getDshProfilesDir(), HEADLESS_PROFILE, 'cordis.patch.yml');
    if (!fs.existsSync(patchPath)) return undefined;

    const text = fs.readFileSync(patchPath, 'utf8');
    const begin = text.indexOf(PROVIDER_MARKERS.begin);
    if (begin === -1) return undefined;
    const end = text.indexOf(PROVIDER_MARKERS.end, begin);
    if (end === -1) return undefined;
    const block = text.slice(begin, end);

    const pick = (pattern: RegExp): string => {
      const m = block.match(pattern);
      if (!m) return '';
      // Our own writer single-quotes values; YAML escapes a quote by doubling it.
      return m[1]
        .trim()
        .replace(/^'(.*)'$/s, '$1')
        .replace(/''/g, "'");
    };

    const candidate: DshProviderConfig = {
      provider: pick(/-\s*id:\s*agent-default-model\b[\s\S]*?\n\s+provider:\s*([^\r\n]+)/),
      // `\n\s+model:` cannot match `models:` because of the literal colon.
      model: pick(/\n\s+model:\s*([^\r\n]+)/),
      api: pick(/\n\s+api:\s*([^\r\n]+)/),
      baseUrl: pick(/\n\s+baseURL:\s*([^\r\n]+)/),
      apiKeyEnv: pick(/\n\s+apiKeyEnv:\s*([^\r\n]+)/),
    };

    return isDshProviderUsable(candidate) ? candidate : undefined;
  } catch {
    return undefined;
  }
}

/**
 * Report whether DSH can be driven non-interactively.
 *
 * The `headless` profile disables the local credentials service and the account
 * platform, so a desktop account login alone is NOT sufficient for
 * bridge-launched runs. What counts is that the credential the *effective*
 * model route needs is present:
 *
 *  1. the shipped `deepseek-official` route reads `DEEPSEEK_API_KEY`;
 *  2. a provider declared in the profile (see {@link readDshConfiguredProvider})
 *     reads the env var that declaration names.
 */
export function checkDshAuth(): { authenticated: boolean; details: string } {
  if (process.env.DEEPSEEK_API_KEY && process.env.DEEPSEEK_API_KEY.trim()) {
    return { authenticated: true, details: '已登录 (DEEPSEEK_API_KEY 环境变量)' };
  }
  if (process.env.DSH_API_KEY && process.env.DSH_API_KEY.trim()) {
    return { authenticated: true, details: '已登录 (DSH_API_KEY 环境变量)' };
  }

  // Disk is authoritative; the env hints only cover a not-yet-synced host.
  const configured = readDshConfiguredProvider();
  const providerEnv = (configured?.apiKeyEnv || process.env[DSH_PROVIDER_ENV_HINT] || '').trim();
  const providerId = (configured?.provider || process.env[DSH_PROVIDER_ID_HINT] || 'custom').trim();

  if (providerEnv) {
    const key = process.env[providerEnv];
    if (key && key.trim()) {
      return {
        authenticated: true,
        details: `已登录 (provider: ${providerId} · 环境变量 ${providerEnv})`,
      };
    }
    return {
      authenticated: false,
      details:
        `已配置 provider ${providerId}，但当前进程缺少密钥（请设置环境变量 ${providerEnv}，` +
        `或在插件中填写 API Key 后完全重启 VS Code）`,
    };
  }

  const credentialsPath = path.join(getDshHome(), '.credentials.yaml');
  if (fs.existsSync(credentialsPath)) {
    return {
      authenticated: false,
      details: '缺少 DEEPSEEK_API_KEY（桌面端登录凭据不适用于 headless 运行）',
    };
  }

  return { authenticated: false, details: '未找到 DEEPSEEK_API_KEY 或 DSH 凭据' };
}

/**
 * Read the DSH CLI version using the same launch strategy used for real runs.
 *
 * Going through the Electron binary avoids `cmd.exe` batch handling, which is
 * unreliable for this launcher.
 *
 * The probe boots a whole Electron process, and callers (status bar, tree view,
 * `probeAll`) refresh frequently, so results are memoized: a successful read is
 * kept for the process lifetime because an install's version does not change,
 * while a failure is retried after a short cooldown.
 */
const versionCache = new Map<string, { value?: string; at: number }>();
const VERSION_FAILURE_TTL_MS = 60_000;

export function queryDshVersion(launcherPath: string): Promise<string | undefined> {
  const cached = versionCache.get(launcherPath);
  if (cached && (cached.value !== undefined || Date.now() - cached.at < VERSION_FAILURE_TTL_MS)) {
    return Promise.resolve(cached.value);
  }

  return new Promise((resolve) => {
    const finish = (value?: string) => {
      versionCache.set(launcherPath, { value, at: Date.now() });
      resolve(value);
    };

    const install = resolveDshInstall(launcherPath);
    if (!install) {
      finish(undefined);
      return;
    }

    let command: string;
    let args: string[];
    let env: NodeJS.ProcessEnv = process.env;

    if (install.execPath && install.cliPath) {
      command = install.execPath;
      args = ['--expose-internals', install.cliPath, '--version'];
      env = { ...process.env, ELECTRON_RUN_AS_NODE: '1' };
    } else if (process.platform === 'win32') {
      command = 'cmd.exe';
      args = ['/c', install.launcherPath, '--version'];
    } else {
      command = install.launcherPath;
      args = ['--version'];
    }

    try {
      const proc = execFile(command, args, { timeout: 15000, env }, (error, stdout) => {
        finish(error ? undefined : stdout.trim().split('\n')[0] || undefined);
      });
      if (proc && proc.stdin) {
        proc.stdin.end();
      }
    } catch {
      finish(undefined);
    }
  });
}

/** Resolve a node executable for spawning the bridge MCP server. */
export function resolveNodeCommand(): string {
  const override = process.env.DSH_BRIDGE_NODE;
  if (override && override.trim()) return override.trim();
  const found = lookupInPath(process.platform === 'win32' ? 'node.exe' : 'node');
  return found || 'node';
}

/**
 * Render the managed MCP registration block for a DSH profile patch file.
 *
 * @param extraEnv - additional variables handed to the bridge MCP process. The
 *   configured provider key travels this way: DSH scrubs credential-shaped names
 *   out of the *inherited* environment, but an explicit `env` entry is merged
 *   afterwards and survives.
 */
export function buildDshMcpPatchBlock(
  mcpScriptPath: string,
  nodeCommand: string,
  extraEnv: Record<string, string> = {},
): string {
  const quote = (value: string) => `'${value.replace(/'/g, "''")}'`;
  const script = mcpScriptPath.replace(/\\/g, '/');
  const node = nodeCommand.replace(/\\/g, '/');

  const envLines = [
    `          MESH_CALLER_AGENT: ${DSH_AGENT_ID}`,
    ...Object.entries(extraEnv).map(([key, value]) => `          ${key}: ${quote(value)}`),
  ];

  return [
    MANAGED_BEGIN,
    '- insert:',
    '    - id: agent-mesh-bridge-mcp',
    "      name: '@deepseek-ai/dsh-mcp-client'",
    '      config:',
    '        transport: stdio',
    `        serverName: ${DSH_MCP_SERVER_NAME}`,
    `        command: ${quote(node)}`,
    '        args:',
    `          - ${quote(script)}`,
    "          - '--caller'",
    `          - '${DSH_AGENT_ID}'`,
    '        env:',
    ...envLines,
    '        failOnStartupError: false',
    MANAGED_END,
  ].join('\n');
}

/**
 * Render the model-provider block for the DSH `headless` profile.
 *
 * The key is deliberately NOT written here: the profile only names the
 * environment variable DSH should read (`apiKeyEnv`), and the extension host
 * injects the actual secret into the child process environment at spawn time.
 */
export function buildDshProviderPatchBlock(provider: DshProviderConfig): string {
  const quote = (value: string) => `'${value.replace(/'/g, "''")}'`;
  const id = provider.provider.trim();
  const model = provider.model.trim();
  const displayName = id.charAt(0).toUpperCase() + id.slice(1);

  return [
    PROVIDER_BEGIN,
    '# Model route for bridge-launched `dsh headless` runs (the desktop profile',
    '# keeps using the signed-in account).',
    '- id: agent-default-model',
    '  config:',
    `    provider: ${id}`,
    `    model: ${quote(model)}`,
    '- id: llm-pi-ai',
    '  config:',
    '    providers:',
    `      ${id}:`,
    `        displayName: ${quote(displayName)}`,
    `        api: ${provider.api.trim() || 'openai-completions'}`,
    `        baseURL: ${quote(provider.baseUrl.trim())}`,
    `        apiKeyEnv: ${quote(provider.apiKeyEnv.trim())}`,
    '        models:',
    `          - id: ${quote(model)}`,
    `            name: ${quote(model)}`,
    '            contextWindow: 1000000',
    '            maxTokens: 32768',
    '            input:',
    '              - text',
    PROVIDER_END,
  ].join('\n');
}

/**
 * Strip a previously written managed block, leaving the rest of the document
 * untouched. Idempotent: a file without the markers is returned unchanged.
 */
export function removeManagedPatchBlock(
  existing: string,
  markers: ManagedMarkers = MCP_MARKERS,
): string {
  const text = existing ?? '';
  const begin = text.indexOf(markers.begin);
  if (begin === -1) return text;
  const end = text.indexOf(markers.end);
  return end === -1
    ? text.slice(0, begin)
    : text.slice(0, begin) + text.slice(end + markers.end.length);
}

/**
 * Merge the MCP registration block into an existing patch file without
 * disturbing the user's own patch entries.
 *
 * The file must remain a top-level YAML list. The shipped default body is `[]`,
 * which is replaced in place; otherwise the block is appended as a new list item.
 */
export function mergeManagedPatchBlock(
  existing: string,
  block: string,
  markers: ManagedMarkers = MCP_MARKERS,
): string {
  const text = removeManagedPatchBlock(existing, markers);

  // Replace a bare empty-array document rather than appending a second root.
  const emptyDoc = /^\s*\[\s*\]\s*$/m;
  if (emptyDoc.test(text)) {
    return `${text.replace(emptyDoc, block).trimEnd()}\n`;
  }

  const trimmed = text.trim();
  if (!trimmed) {
    return `${block}\n`;
  }

  return `${text.trimEnd()}\n\n${block}\n`;
}

export interface DshMcpSyncResult {
  synced: string[];
  errors: string[];
}

/**
 * Register the bridge MCP server into every DSH profile patch layer, and the
 * configured model provider into the `headless` profile.
 *
 * Each `dsh-mcp-client` instance connects to exactly one MCP server, so the
 * registration is a profile patch entry rather than a `mcpServers` JSON file.
 *
 * @param provider - custom model route for `dsh headless`; omit/empty to leave
 *   DSH on its shipped `deepseek-official` route (and remove any stale override).
 * @param extraMcpEnv - extra variables for the bridge MCP process (the provider
 *   key), so the registration works regardless of the host's environment.
 */
export function syncDshMcpConfigs(
  mcpScriptPath: string,
  nodeCommand: string,
  provider?: DshProviderConfig | null,
  extraMcpEnv: Record<string, string> = {},
): DshMcpSyncResult {
  const synced: string[] = [];
  const errors: string[] = [];

  const profilesDir = getDshProfilesDir();
  if (!fs.existsSync(profilesDir)) {
    return { synced, errors };
  }

  let profileDirs: fs.Dirent[];
  try {
    profileDirs = fs.readdirSync(profilesDir, { withFileTypes: true });
  } catch (e: any) {
    return { synced, errors: [`DeepSeek Harness: ${e?.message || e}`] };
  }

  const mcpBlock = buildDshMcpPatchBlock(mcpScriptPath, nodeCommand, extraMcpEnv);
  const useProvider = isDshProviderUsable(provider);
  const providerBlock = useProvider ? buildDshProviderPatchBlock(provider!) : '';
  const providerId = useProvider ? provider!.provider.trim() : '';

  for (const entry of profileDirs) {
    if (!entry.isDirectory()) continue;
    const patchPath = path.join(profilesDir, entry.name, 'cordis.patch.yml');
    try {
      let existing = '';
      if (fs.existsSync(patchPath)) {
        existing = fs.readFileSync(patchPath, 'utf8');
      }

      // MCP registration goes into every profile.
      let next = mergeManagedPatchBlock(existing, mcpBlock, MCP_MARKERS);

      // The model-provider override is scoped to the headless profile.
      if (entry.name === HEADLESS_PROFILE) {
        next = useProvider
          ? mergeManagedPatchBlock(next, providerBlock, PROVIDER_MARKERS)
          : removeManagedPatchBlock(next, PROVIDER_MARKERS);
      }

      fs.writeFileSync(patchPath, next, 'utf8');
      synced.push(
        entry.name === HEADLESS_PROFILE && useProvider
          ? `DeepSeek Harness (${entry.name}, provider: ${providerId})`
          : `DeepSeek Harness (${entry.name})`,
      );
    } catch (e: any) {
      errors.push(`DeepSeek Harness (${entry.name}): ${e?.message || e}`);
    }
  }

  return { synced, errors };
}

/** PATH lookup shared by discovery and node resolution. */
function lookupInPath(command: string): string | null {
  const envPath = process.env.PATH || '';
  const delimiter = process.platform === 'win32' ? ';' : ':';
  for (const dir of envPath.split(delimiter)) {
    if (!dir) continue;
    const full = path.join(dir, command);
    try {
      if (fs.existsSync(full)) return full;
    } catch {}
  }
  return null;
}
