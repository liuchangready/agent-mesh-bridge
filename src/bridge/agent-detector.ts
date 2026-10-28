import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';
import { AgentId, AgentInfo, AgentStatusType } from './types';
import { RateLimitTracker } from './rate-limit-tracker';
import {
  IS_WINDOWS,
  binaryCandidates,
  bundledCliRelativePaths,
  isExecutableFile,
  lookupInPath,
  needsShellWrapper,
  vscodeExtensionDirs,
} from './platform';
import {
  checkDshAuth,
  detectDshActiveSession,
  getDshCandidatePaths,
  queryDshVersion,
} from './deepseek-harness';

export class AgentDetector {
  private customPaths: Partial<Record<AgentId, string>> = {};
  private rateLimitTracker: RateLimitTracker;
  private cache: Partial<Record<AgentId, { info: AgentInfo; timestamp: number }>> = {};
  private readonly CACHE_TTL_MS = 5000; // 5s cache

  constructor(customPaths?: Partial<Record<AgentId, string>>, rateLimitTracker?: RateLimitTracker) {
    if (customPaths) {
      this.customPaths = customPaths;
    }
    this.rateLimitTracker = rateLimitTracker || RateLimitTracker.getInstance();
  }

  public setCustomPath(agent: AgentId, execPath: string): void {
    this.customPaths[agent] = execPath;
    delete this.cache[agent];
  }

  public getRateLimitTracker(): RateLimitTracker {
    return this.rateLimitTracker;
  }

  /**
   * Find candidate paths for Antigravity (agy).
   */
  public getAntigravityCandidatePaths(): string[] {
    const candidates: string[] = [];
    if (this.customPaths.antigravity && fs.existsSync(this.customPaths.antigravity)) {
      candidates.push(this.customPaths.antigravity);
    }

    const home = os.homedir();
    if (IS_WINDOWS) {
      candidates.push(path.join(home, '.gemini', 'bin', 'agy.exe'));
      candidates.push(path.join(home, 'AppData', 'Local', 'Programs', 'Antigravity', 'agy.exe'));
    } else {
      candidates.push(path.join(home, '.gemini', 'bin', 'agy'));
      candidates.push(path.join(home, '.local', 'bin', 'agy'));
      candidates.push('/usr/local/bin/agy');
    }

    for (const name of binaryCandidates('agy')) {
      const pathBin = this.lookupInPath(name);
      if (pathBin) candidates.push(pathBin);
    }

    return Array.from(new Set(candidates));
  }

  /**
   * Find candidate paths for Claude Code (claude).
   */
  public getClaudeCandidatePaths(): string[] {
    const candidates: string[] = [];
    if (this.customPaths.claude_code && fs.existsSync(this.customPaths.claude_code)) {
      candidates.push(this.customPaths.claude_code);
    }

    const home = os.homedir();
    if (IS_WINDOWS) {
      const appData = process.env.APPDATA || path.join(home, 'AppData', 'Roaming');
      candidates.push(
        path.join(appData, 'npm', 'node_modules', '@anthropic-ai', 'claude-code', 'bin', 'claude.exe'),
      );
      candidates.push(path.join(appData, 'npm', 'claude.cmd'));
      candidates.push(path.join(appData, 'npm', 'claude.ps1'));
      candidates.push(path.join(home, '.claude', 'bin', 'claude.cmd'));
    } else {
      candidates.push(path.join(home, '.npm-global', 'bin', 'claude'));
      candidates.push(path.join(home, '.local', 'bin', 'claude'));
      candidates.push('/usr/local/bin/claude');
      candidates.push('/usr/bin/claude');
      candidates.push(path.join(home, '.claude', 'bin', 'claude'));
    }

    for (const name of binaryCandidates('claude')) {
      const pathBin = this.lookupInPath(name);
      if (pathBin) candidates.push(pathBin);
    }

    return Array.from(new Set(candidates));
  }

  /**
   * Find candidate paths for OpenAI Codex (codex).
   *
   * The extension-bundled binary is looked up in every VS Code extension root
   * that exists here — a remote workspace keeps its extensions under
   * `~/.vscode-server`, not `~/.vscode`.
   */
  public getCodexCandidatePaths(): string[] {
    const candidates: string[] = [];
    if (this.customPaths.codex && fs.existsSync(this.customPaths.codex)) {
      candidates.push(this.customPaths.codex);
    }

    const home = os.homedir();
    const cliRelativePaths = bundledCliRelativePaths('codex');
    for (const extensionsDir of vscodeExtensionDirs(home)) {
      if (!fs.existsSync(extensionsDir)) continue;
      try {
        const dirs = fs.readdirSync(extensionsDir);
        const codexDirs = dirs.filter(
          (d) => d.startsWith('openai.chatgpt-') || d.startsWith('openai.codex-'),
        );
        for (const d of codexDirs) {
          for (const relative of cliRelativePaths) {
            candidates.push(path.join(extensionsDir, d, relative));
          }
        }
      } catch {}
    }

    if (IS_WINDOWS) {
      candidates.push(path.join(home, '.codex', 'bin', 'codex.exe'));
      candidates.push(path.join(home, 'AppData', 'Local', 'Programs', 'Codex', 'codex.exe'));
    } else {
      candidates.push(path.join(home, '.codex', 'bin', 'codex'));
      candidates.push(path.join(home, '.local', 'bin', 'codex'));
      candidates.push('/usr/local/bin/codex');
    }

    for (const name of binaryCandidates('codex')) {
      const pathBin = this.lookupInPath(name);
      if (pathBin) candidates.push(pathBin);
    }

    return Array.from(new Set(candidates));
  }

  /**
   * Find candidate paths for DeepSeek Harness (`dsh`), an Electron desktop app
   * whose CLI is exposed through a profile launcher.
   */
  public getDeepSeekHarnessCandidatePaths(): string[] {
    return getDshCandidatePaths(this.customPaths.deepseek_harness);
  }

  /**
   * Resolve best executable path for given agent.
   */
  public resolveExecutable(agent: AgentId): string | null {
    let candidates: string[] = [];
    switch (agent) {
      case 'antigravity':
        candidates = this.getAntigravityCandidatePaths();
        break;
      case 'claude_code':
        candidates = this.getClaudeCandidatePaths();
        break;
      case 'codex':
        candidates = this.getCodexCandidatePaths();
        break;
      case 'deepseek_harness':
        candidates = this.getDeepSeekHarnessCandidatePaths();
        break;
      case 'vscode_chat':
        return 'vscode_internal';
    }

    // Only a path that can actually be launched here counts as installed: on
    // POSIX a present-but-not-executable shim still has to be reported offline.
    for (const p of candidates) {
      if (isExecutableFile(p)) return p;
    }
    return null;
  }

  /**
   * Probe agent info including version, authentication status, and rate limit exhaustion with recovery time.
   */
  public async probeAgent(agent: AgentId, forceRefresh: boolean = false): Promise<AgentInfo> {
    const cached = this.cache[agent];
    if (!forceRefresh && cached && Date.now() - cached.timestamp < this.CACHE_TTL_MS) {
      return cached.info;
    }

    const names: Record<AgentId, string> = {
      antigravity: 'Google Antigravity',
      claude_code: 'Anthropic Claude Code',
      codex: 'OpenAI Codex',
      deepseek_harness: 'DeepSeek Harness',
      vscode_chat: 'VSCode Copilot/Chat',
    };

    if (agent === 'vscode_chat') {
      const info: AgentInfo = {
        id: 'vscode_chat',
        name: names.vscode_chat,
        status: 'online',
        available: true,
        executablePath: 'builtin',
        authDetails: 'Builtin VSCode Session',
        statusDetails: 'Ready',
        lastChecked: new Date().toISOString(),
      };
      this.cache[agent] = { info, timestamp: Date.now() };
      return info;
    }

    const execPath = this.resolveExecutable(agent);
    const rateLimit = this.rateLimitTracker.getRateLimitInfo(agent);

    // 1. Check if executable exists (Offline check)
    if (!execPath) {
      const info: AgentInfo = {
        id: agent,
        name: names[agent],
        status: 'offline',
        available: false,
        statusDetails: '未安装或无法找到可执行文件 (Offline / Missing executable)',
        rateLimit,
        lastChecked: new Date().toISOString(),
      };
      this.cache[agent] = { info, timestamp: Date.now() };
      return info;
    }

    // 2. Check version
    const version =
      agent === 'deepseek_harness'
        ? await queryDshVersion(execPath)
        : await this.queryVersion(execPath);

    // 3. Check Authentication
    const auth = this.checkAuthentication(agent);

    // 4. Check Rate Limit Exhaustion
    let status: AgentStatusType = 'online';
    let statusDetails = '正常就绪 (Ready)';

    if (rateLimit && rateLimit.isExhausted) {
      status = 'exhausted';
      const resets = rateLimit.resetsAtFormatted || '未知时间';
      statusDetails = `配额耗尽 / 限流中 (恢复时间: ${resets})`;
    } else if (!auth.authenticated) {
      status = 'unauthenticated';
      statusDetails = `未登录 (${auth.details})`;
    }

    const info: AgentInfo = {
      id: agent,
      name: names[agent],
      status,
      available: true,
      executablePath: execPath,
      version: version || 'installed',
      authDetails: auth.details,
      statusDetails,
      rateLimit,
      lastChecked: new Date().toISOString(),
    };

    this.cache[agent] = { info, timestamp: Date.now() };
    return info;
  }

  /**
   * Probe all agents concurrently.
   */
  public async probeAll(forceRefresh: boolean = false): Promise<Record<AgentId, AgentInfo>> {
    const agents: AgentId[] = ['antigravity', 'codex', 'claude_code', 'deepseek_harness'];
    const results = await Promise.all(agents.map((a) => this.probeAgent(a, forceRefresh)));
    const map: Partial<Record<AgentId, AgentInfo>> = {};
    for (const r of results) {
      map[r.id] = r;
    }
    return map as Record<AgentId, AgentInfo>;
  }

  private checkAuthentication(agent: AgentId): { authenticated: boolean; details: string } {
    const home = os.homedir();

    if (agent === 'antigravity') {
      const pbtxt = path.join(home, '.gemini', 'antigravity', 'antigravity_state.pbtxt');
      const cliPbtxt = path.join(home, '.gemini', 'antigravity-cli', 'jetski_state.pbtxt');
      if (fs.existsSync(pbtxt)) {
        try {
          const content = fs.readFileSync(pbtxt, 'utf8');
          if (
            content.includes('installation_uuid') ||
            content.includes('AGENT_ONBOARDING_STATE_COMPLETED')
          ) {
            return { authenticated: true, details: '已登录 (Antigravity 会话就绪)' };
          }
        } catch {}
      }
      if (fs.existsSync(cliPbtxt) || process.env.ANTIGRAVITY_APP || process.env.GEMINI_API_KEY) {
        return { authenticated: true, details: '已登录 (CLI / 环境变量)' };
      }
      return { authenticated: false, details: '缺失 Antigravity 登录会话' };
    }

    if (agent === 'codex') {
      const authPath = path.join(home, '.codex', 'auth.json');
      if (fs.existsSync(authPath)) {
        try {
          const data = JSON.parse(fs.readFileSync(authPath, 'utf8'));
          if (
            data.OPENAI_API_KEY ||
            (data.tokens && Object.keys(data.tokens).length > 0) ||
            data.access_token
          ) {
            return { authenticated: true, details: '已登录 (Codex API Key / 凭据)' };
          }
        } catch {}
      }
      if (process.env.OPENAI_API_KEY || process.env.CODEX_API_KEY) {
        return { authenticated: true, details: '已登录 (环境变量 API Key)' };
      }
      return { authenticated: false, details: '未找到 OpenAI API Key 或 Token' };
    }

    if (agent === 'claude_code') {
      const settingsPath = path.join(home, '.claude', 'settings.json');
      if (fs.existsSync(settingsPath)) {
        try {
          const data = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
          if (data.env && (data.env.ANTHROPIC_AUTH_TOKEN || data.env.ANTHROPIC_API_KEY)) {
            return { authenticated: true, details: '已登录 (Claude Auth Token)' };
          }
        } catch {}
      }

      const claudeJsonPath = path.join(home, '.claude.json');
      if (fs.existsSync(claudeJsonPath)) {
        try {
          const data = JSON.parse(fs.readFileSync(claudeJsonPath, 'utf8'));
          if (data.primaryApiKey || data.oauthAccount || data.sessionKey) {
            return { authenticated: true, details: '已登录 (Claude OAuth/Key)' };
          }
        } catch {}
      }

      if (process.env.ANTHROPIC_API_KEY || process.env.ANTHROPIC_AUTH_TOKEN) {
        return { authenticated: true, details: '已登录 (环境变量 Token)' };
      }

      return { authenticated: false, details: '未配置 Anthropic 访问令牌' };
    }

    if (agent === 'deepseek_harness') {
      return checkDshAuth();
    }

    return { authenticated: true, details: 'Default' };
  }

  private queryVersion(execPath: string): Promise<string | undefined> {
    return new Promise((resolve) => {
      try {
        // Windows `.cmd` / `.bat` launchers (Claude Code, DeepSeek Harness) are
        // batch scripts and must be executed through cmd.exe. POSIX launchers
        // run directly.
        const wrap = needsShellWrapper(execPath);
        const command = wrap ? 'cmd.exe' : execPath;
        const args = wrap ? ['/c', execPath, '--version'] : ['--version'];

        const proc = execFile(command, args, { timeout: 8000 }, (error, stdout) => {
          if (error) {
            resolve(undefined);
          } else {
            resolve(stdout.trim().split('\n')[0]);
          }
        });
        if (proc && proc.stdin) {
          proc.stdin.end();
        }
      } catch {
        resolve(undefined);
      }
    });
  }

  private lookupInPath(command: string): string | null {
    return lookupInPath(command);
  }

  /**
   * Automatically detect the most recent active session ID for an agent in a given workspace.
   */
  public detectActiveSession(
    agent: AgentId,
    workspacePath?: string,
    excludeSessionIds: string[] = [],
  ): string | undefined {
    const home = os.homedir();
    const excludeSet = new Set(excludeSessionIds);

    try {
      if (agent === 'claude_code') {
        const projectsDir = path.join(home, '.claude', 'projects');
        if (!fs.existsSync(projectsDir)) return undefined;

        let targetDirs: string[] = [];
        if (workspacePath) {
          const resolvedWs = path.resolve(workspacePath);
          const baseName = path.basename(resolvedWs).toLowerCase();
          try {
            const projectEntries = fs.readdirSync(projectsDir);
            for (const entry of projectEntries) {
              const lower = entry.toLowerCase();
              if (lower.includes(baseName)) {
                targetDirs.push(path.join(projectsDir, entry));
              }
            }
          } catch {}
        }

        if (targetDirs.length === 0) {
          try {
            const allEntries = fs.readdirSync(projectsDir);
            targetDirs = allEntries.map((e) => path.join(projectsDir, e));
          } catch {}
        }

        const allFiles: { file: string; mtime: number }[] = [];
        for (const dir of targetDirs) {
          try {
            if (!fs.statSync(dir).isDirectory()) continue;
            const files = fs.readdirSync(dir);
            for (const f of files) {
              if (f.endsWith('.jsonl')) {
                const fullPath = path.join(dir, f);
                const stat = fs.statSync(fullPath);
                allFiles.push({ file: f, mtime: stat.mtimeMs });
              }
            }
          } catch {}
        }

        allFiles.sort((a, b) => b.mtime - a.mtime);
        for (const item of allFiles) {
          const sessionId = path.basename(item.file, '.jsonl');
          if (!excludeSet.has(sessionId)) {
            return sessionId;
          }
        }
      }

      if (agent === 'antigravity') {
        const convDir = path.join(home, '.gemini', 'antigravity-cli', 'conversations');
        if (fs.existsSync(convDir)) {
          const files = fs.readdirSync(convDir);
          const dbFiles: { file: string; mtime: number }[] = [];
          for (const f of files) {
            if (f.endsWith('.db') && !f.endsWith('.db-wal') && !f.endsWith('.db-shm')) {
              try {
                const stat = fs.statSync(path.join(convDir, f));
                dbFiles.push({ file: f, mtime: stat.mtimeMs });
              } catch {}
            }
          }
          dbFiles.sort((a, b) => b.mtime - a.mtime);
          for (const item of dbFiles) {
            const sessionId = path.basename(item.file, '.db');
            if (!excludeSet.has(sessionId)) {
              return sessionId;
            }
          }
        }
      }

      if (agent === 'codex') {
        const sessionsDir = path.join(home, '.codex', 'sessions');
        if (fs.existsSync(sessionsDir)) {
          const files = this.findRecentFilesRecursive(sessionsDir, 3);
          files.sort((a, b) => b.mtime - a.mtime);
          for (const item of files) {
            const base = path.basename(item.file, path.extname(item.file));
            if (!excludeSet.has(base)) {
              return base;
            }
          }
        }
      }

      if (agent === 'deepseek_harness') {
        return detectDshActiveSession(workspacePath, excludeSessionIds);
      }
    } catch (e) {
      console.warn(`[AgentDetector] Error detecting active session for ${agent}:`, e);
    }

    return undefined;
  }

  private findRecentFilesRecursive(
    dir: string,
    maxDepth: number,
    currentDepth: number = 0,
  ): { file: string; mtime: number }[] {
    const results: { file: string; mtime: number }[] = [];
    if (currentDepth > maxDepth || !fs.existsSync(dir)) return results;
    try {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          results.push(...this.findRecentFilesRecursive(fullPath, maxDepth, currentDepth + 1));
        } else if (
          entry.isFile() &&
          (entry.name.endsWith('.jsonl') || entry.name.endsWith('.json'))
        ) {
          try {
            const stat = fs.statSync(fullPath);
            results.push({ file: fullPath, mtime: stat.mtimeMs });
          } catch {}
        }
      }
    } catch {}
    return results;
  }
}
