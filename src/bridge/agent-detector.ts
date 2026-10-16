import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { execFile } from 'child_process';
import { AgentId, AgentInfo } from './types';

export class AgentDetector {
  private customPaths: Partial<Record<AgentId, string>> = {};

  constructor(customPaths?: Partial<Record<AgentId, string>>) {
    if (customPaths) {
      this.customPaths = customPaths;
    }
  }

  public setCustomPath(agent: AgentId, execPath: string): void {
    this.customPaths[agent] = execPath;
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
    // Default gemini bin paths
    if (process.platform === 'win32') {
      candidates.push(path.join(home, '.gemini', 'bin', 'agy.exe'));
      candidates.push(path.join(home, 'AppData', 'Local', 'Programs', 'Antigravity', 'agy.exe'));
    } else {
      candidates.push(path.join(home, '.gemini', 'bin', 'agy'));
      candidates.push('/usr/local/bin/agy');
    }

    // Try finding in PATH
    const pathBin = this.lookupInPath(process.platform === 'win32' ? 'agy.exe' : 'agy');
    if (pathBin) candidates.push(pathBin);

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
    if (process.platform === 'win32') {
      const appData = process.env.APPDATA || path.join(home, 'AppData', 'Roaming');
      candidates.push(path.join(appData, 'npm', 'claude.cmd'));
      candidates.push(path.join(appData, 'npm', 'claude.ps1'));
      candidates.push(path.join(home, '.claude', 'bin', 'claude.cmd'));
    } else {
      candidates.push(path.join(home, '.npm-global', 'bin', 'claude'));
      candidates.push('/usr/local/bin/claude');
      candidates.push(path.join(home, '.claude', 'bin', 'claude'));
    }

    const pathBin = this.lookupInPath(process.platform === 'win32' ? 'claude.cmd' : 'claude');
    if (pathBin) candidates.push(pathBin);

    return Array.from(new Set(candidates));
  }

  /**
   * Find candidate paths for OpenAI Codex (codex).
   */
  public getCodexCandidatePaths(): string[] {
    const candidates: string[] = [];
    if (this.customPaths.codex && fs.existsSync(this.customPaths.codex)) {
      candidates.push(this.customPaths.codex);
    }

    const home = os.homedir();
    if (process.platform === 'win32') {
      // Look in VSCode extensions folder for openai.chatgpt-*
      const extensionsDir = path.join(home, '.vscode', 'extensions');
      if (fs.existsSync(extensionsDir)) {
        try {
          const dirs = fs.readdirSync(extensionsDir);
          const codexDirs = dirs.filter(
            (d) => d.startsWith('openai.chatgpt-') || d.startsWith('openai.codex-'),
          );
          for (const d of codexDirs) {
            candidates.push(path.join(extensionsDir, d, 'bin', 'windows-x86_64', 'codex.exe'));
            candidates.push(path.join(extensionsDir, d, 'bin', 'codex.exe'));
          }
        } catch {}
      }
      candidates.push(path.join(home, '.codex', 'bin', 'codex.exe'));
      candidates.push(path.join(home, 'AppData', 'Local', 'Programs', 'Codex', 'codex.exe'));
    } else {
      const extensionsDir = path.join(home, '.vscode', 'extensions');
      if (fs.existsSync(extensionsDir)) {
        try {
          const dirs = fs.readdirSync(extensionsDir);
          const codexDirs = dirs.filter(
            (d) => d.startsWith('openai.chatgpt-') || d.startsWith('openai.codex-'),
          );
          for (const d of codexDirs) {
            candidates.push(path.join(extensionsDir, d, 'bin', 'linux-x86_64', 'codex'));
            candidates.push(path.join(extensionsDir, d, 'bin', 'darwin-arm64', 'codex'));
            candidates.push(path.join(extensionsDir, d, 'bin', 'codex'));
          }
        } catch {}
      }
      candidates.push(path.join(home, '.codex', 'bin', 'codex'));
      candidates.push('/usr/local/bin/codex');
    }

    const pathBin = this.lookupInPath(process.platform === 'win32' ? 'codex.exe' : 'codex');
    if (pathBin) candidates.push(pathBin);

    return Array.from(new Set(candidates));
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
      case 'vscode_chat':
        return 'vscode_internal';
    }

    for (const p of candidates) {
      try {
        if (fs.existsSync(p)) {
          return p;
        }
      } catch {}
    }
    return null;
  }

  /**
   * Probe agent info including version if possible.
   */
  public async probeAgent(agent: AgentId): Promise<AgentInfo> {
    const names: Record<AgentId, string> = {
      antigravity: 'Google Antigravity',
      claude_code: 'Anthropic Claude Code',
      codex: 'OpenAI Codex',
      vscode_chat: 'VSCode Copilot/Chat',
    };

    if (agent === 'vscode_chat') {
      return {
        id: 'vscode_chat',
        name: names.vscode_chat,
        available: true,
        executablePath: 'builtin',
      };
    }

    const execPath = this.resolveExecutable(agent);
    if (!execPath) {
      return {
        id: agent,
        name: names[agent],
        available: false,
        statusDetails: 'Executable not found in standard paths or PATH',
      };
    }

    const version = await this.queryVersion(execPath);
    return {
      id: agent,
      name: names[agent],
      available: true,
      executablePath: execPath,
      version: version || 'installed',
      statusDetails: 'Ready',
    };
  }

  private queryVersion(execPath: string): Promise<string | undefined> {
    return new Promise((resolve) => {
      try {
        const proc = execFile(execPath, ['--version'], { timeout: 3000 }, (error, stdout) => {
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

  /**
   * Search PATH for command.
   */
  private lookupInPath(command: string): string | null {
    const envPath = process.env.PATH || '';
    const delimiter = process.platform === 'win32' ? ';' : ':';
    const dirs = envPath.split(delimiter);

    for (const dir of dirs) {
      const full = path.join(dir, command);
      try {
        if (fs.existsSync(full)) {
          return full;
        }
      } catch {}
    }
    return null;
  }
}
