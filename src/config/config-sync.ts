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
        args: [this.mcpScriptPath, '--caller', 'claude_code'],
        env: {
          MESH_CALLER_AGENT: 'claude_code',
        },
      };

      try {
        fs.writeFileSync(confPath, JSON.stringify(config, null, 2), 'utf8');
        updated = true;
      } catch {}
    }

    // Sync permissions into ~/.claude/settings.json
    try {
      const settingsPath = path.join(home, '.claude', 'settings.json');
      if (fs.existsSync(settingsPath)) {
        const settings = JSON.parse(fs.readFileSync(settingsPath, 'utf8'));
        if (!settings.permissions) settings.permissions = {};
        if (!Array.isArray(settings.permissions.allow)) settings.permissions.allow = [];

        const requiredPermissions = [
          'mcp__agent-mesh-bridge__*',
          'mcp__agent-mesh-bridge__dispatch_task',
          'mcp__agent-mesh-bridge__request_rework',
          'mcp__agent-mesh-bridge__approve_task',
          'mcp__agent-mesh-bridge__get_thread_details',
          'mcp__agent-mesh-bridge__list_recent_tasks',
          'mcp__agent-mesh-bridge__invoke_agent',
          'mcp__agent-mesh-bridge__query_agent_mesh_status',
          'Bash(*)',
          'Bash(./gradlew *)',
          'Bash(gradlew *)',
          'Bash(git *)',
        ];

        let permsChanged = false;
        for (const perm of requiredPermissions) {
          if (!settings.permissions.allow.includes(perm)) {
            settings.permissions.allow.unshift(perm);
            permsChanged = true;
          }
        }

        if (permsChanged) {
          fs.writeFileSync(settingsPath, JSON.stringify(settings, null, 2), 'utf8');
          updated = true;
        }
      }
    } catch {}

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
      args: [this.mcpScriptPath, '--caller', 'codex'],
      env: {
        MESH_CALLER_AGENT: 'codex',
      },
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
      args: [this.mcpScriptPath, '--caller', 'antigravity'],
      env: {
        MESH_CALLER_AGENT: 'antigravity',
      },
    };

    fs.writeFileSync(mcpConfigPath, JSON.stringify(config, null, 2), 'utf8');
    return true;
  }
}
