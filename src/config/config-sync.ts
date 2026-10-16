import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

export class ConfigSyncService {
  private mcpScriptPath: string;

  constructor(customMcpScriptPath?: string) {
    this.mcpScriptPath =
      customMcpScriptPath || path.resolve(__dirname, '..', '..', 'bin', 'agent-bridge-mcp.cjs');
  }

  /**
   * Sync MCP server registration across Antigravity, Claude Code, and Codex.
   */
  public async syncAll(): Promise<{ success: boolean; synced: string[]; errors: string[] }> {
    const synced: string[] = [];
    const errors: string[] = [];

    // 1. Sync Claude Code
    try {
      const claudeSynced = this.syncClaudeConfig();
      if (claudeSynced) synced.push('Claude Code');
    } catch (e: any) {
      errors.push(`Claude Code: ${e.message}`);
    }

    // 2. Sync OpenAI Codex
    try {
      const codexSynced = this.syncCodexConfig();
      if (codexSynced) synced.push('OpenAI Codex');
    } catch (e: any) {
      errors.push(`OpenAI Codex: ${e.message}`);
    }

    // 3. Sync Antigravity
    try {
      const antiSynced = this.syncAntigravityConfig();
      if (antiSynced) synced.push('Google Antigravity');
    } catch (e: any) {
      errors.push(`Google Antigravity: ${e.message}`);
    }

    return {
      success: errors.length === 0,
      synced,
      errors,
    };
  }

  private syncClaudeConfig(): boolean {
    const home = os.homedir();
    const configPaths = [
      path.join(home, '.claude', 'claude_desktop_config.json'),
      path.join(home, '.claude.json'),
    ];

    let updated = false;
    for (const confPath of configPaths) {
      const dir = path.dirname(confPath);
      if (!fs.existsSync(dir)) {
        try {
          fs.mkdirSync(dir, { recursive: true });
        } catch {}
      }

      let config: any = {};
      if (fs.existsSync(confPath)) {
        try {
          config = JSON.parse(fs.readFileSync(confPath, 'utf8'));
        } catch {
          config = {};
        }
      }

      if (!config.mcpServers) {
        config.mcpServers = {};
      }

      config.mcpServers['agent-mesh-bridge'] = {
        command: 'node',
        args: [this.mcpScriptPath],
      };

      try {
        fs.writeFileSync(confPath, JSON.stringify(config, null, 2), 'utf8');
        updated = true;
      } catch {}
    }

    return updated;
  }

  private syncCodexConfig(): boolean {
    const home = os.homedir();
    const confPath = path.join(home, '.codex', 'config.json');
    const dir = path.dirname(confPath);

    if (!fs.existsSync(dir)) {
      try {
        fs.mkdirSync(dir, { recursive: true });
      } catch {}
    }

    let config: any = {};
    if (fs.existsSync(confPath)) {
      try {
        config = JSON.parse(fs.readFileSync(confPath, 'utf8'));
      } catch {
        config = {};
      }
    }

    if (!config.mcpServers) {
      config.mcpServers = {};
    }

    config.mcpServers['agent-mesh-bridge'] = {
      command: 'node',
      args: [this.mcpScriptPath],
    };

    fs.writeFileSync(confPath, JSON.stringify(config, null, 2), 'utf8');
    return true;
  }

  private syncAntigravityConfig(): boolean {
    const home = os.homedir();
    const mcpConfigPath = path.join(home, '.gemini', 'antigravity', 'mcp_config.json');
    const dir = path.dirname(mcpConfigPath);

    if (!fs.existsSync(dir)) {
      try {
        fs.mkdirSync(dir, { recursive: true });
      } catch {}
    }

    let config: any = {};
    if (fs.existsSync(mcpConfigPath)) {
      try {
        config = JSON.parse(fs.readFileSync(mcpConfigPath, 'utf8'));
      } catch {
        config = {};
      }
    }

    if (!config.mcpServers) {
      config.mcpServers = {};
    }

    config.mcpServers['agent-mesh-bridge'] = {
      command: 'node',
      args: [this.mcpScriptPath],
    };

    fs.writeFileSync(mcpConfigPath, JSON.stringify(config, null, 2), 'utf8');
    return true;
  }
}
