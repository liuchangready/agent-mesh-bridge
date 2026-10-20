import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  DshProviderConfig,
  resolveNodeCommand,
  syncDshMcpConfigs,
} from '../bridge/deepseek-harness';

export class ConfigSyncService {
  private mcpScriptPath: string;
  private dshProvider: DshProviderConfig | null;
  private homeDir: string;
  /** Extra variables handed to every registered bridge MCP process. */
  private agentMcpEnv: Record<string, string> = {};

  constructor(
    customMcpScriptPath?: string,
    dshProvider?: DshProviderConfig | null,
    homeDir?: string,
  ) {
    this.mcpScriptPath =
      customMcpScriptPath || path.resolve(__dirname, '..', '..', 'bin', 'agent-bridge-mcp.cjs');
    this.dshProvider = dshProvider ?? null;
    this.homeDir = homeDir || os.homedir();
  }

  /**
   * Update the model route written into the DSH `headless` profile.
   * Pass `null` to fall back to DSH's shipped `deepseek-official` route.
   */
  public setDshProvider(provider: DshProviderConfig | null): void {
    this.dshProvider = provider;
  }

  /**
   * Variables passed explicitly to each agent's bridge MCP process.
   *
   * An agent spawns that process itself, so relying on the ambient environment
   * is unreliable (a Windows `setx` does not reach programs launched from an
   * already-running Explorer). Declaring the provider key here makes the
   * registration self-contained and independent of process inheritance.
   */
  public setAgentMcpEnv(env: Record<string, string>): void {
    this.agentMcpEnv = { ...env };
  }

  /** TOML inline table for the Codex MCP `env` key, with escaped values. */
  private buildTomlEnv(entries: Record<string, string>): string {
    const pairs = Object.entries(entries).map(
      ([key, value]) => `${key} = "${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`,
    );
    return `{ ${pairs.join(', ')} }`;
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

    // 4. Sync DeepSeek Harness (profile patch layers)
    try {
      const dsh = this.syncDeepSeekHarnessConfig();
      synced.push(...dsh.synced);
      errors.push(...dsh.errors);
    } catch (e: any) {
      errors.push(`DeepSeek Harness: ${e.message}`);
    }

    return {
      success: errors.length === 0,
      synced,
      errors,
    };
  }

  /**
   * Register the bridge MCP server with every DeepSeek Harness profile.
   *
   * DSH has no `mcpServers` JSON file: each `dsh-mcp-client` plugin instance
   * connects to exactly one server and is declared in the profile's
   * `cordis.patch.yml` patch layer, so the registration is a YAML insert.
   */
  private syncDeepSeekHarnessConfig(): { synced: string[]; errors: string[] } {
    return syncDshMcpConfigs(
      this.mcpScriptPath,
      resolveNodeCommand(),
      this.dshProvider,
      this.agentMcpEnv,
    );
  }

  private syncClaudeConfig(): boolean {
    const home = this.homeDir;
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
          ...this.agentMcpEnv,
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
    const home = this.homeDir;
    const tomlPath = path.join(home, '.codex', 'config.toml');
    const dir = path.dirname(tomlPath);

    if (!fs.existsSync(dir)) {
      try {
        fs.mkdirSync(dir, { recursive: true });
      } catch {}
    }

    // 1. Update config.toml (Codex CLI strictly reads config.toml, not config.json)
    let tomlContent = '';
    if (fs.existsSync(tomlPath)) {
      try {
        tomlContent = fs.readFileSync(tomlPath, 'utf8');
      } catch {
        tomlContent = '';
      }
    }

    const normalizedScriptPath = this.mcpScriptPath.replace(/\\/g, '/');

    // Remove stale blocks, then register exactly ONE server. Codex surfaces one
    // MCP namespace per registered server, so a duplicate registration shows up
    // to the model as a second, identical copy of every tool (and spawns a
    // second server process).
    const cleanToml = tomlContent
      .replace(/\[mcp_servers\.agent-mesh-bridge\][\s\S]*?(?=(\n\[|$))/g, '')
      .replace(/\[mcp_servers\.cross-agent-bridge\][\s\S]*?(?=(\n\[|$))/g, '')
      .trim();

    const codexEnv = this.buildTomlEnv({
      MESH_CALLER_AGENT: 'codex',
      ...this.agentMcpEnv,
    });
    const mcpBlock = `\n\n[mcp_servers.agent-mesh-bridge]\ncommand = "node"\nargs = ["${normalizedScriptPath}", "--caller", "codex"]\nenv = ${codexEnv}\n`;

    fs.writeFileSync(tomlPath, cleanToml + mcpBlock, 'utf8');

    // 2. Also write to config.json for compatibility
    const jsonPath = path.join(home, '.codex', 'config.json');
    try {
      let config: any = {};
      if (fs.existsSync(jsonPath)) {
        try {
          config = JSON.parse(fs.readFileSync(jsonPath, 'utf8'));
        } catch {
          config = {};
        }
      }
      if (!config.mcpServers) config.mcpServers = {};
      config.mcpServers['agent-mesh-bridge'] = {
        command: 'node',
        args: [this.mcpScriptPath, '--caller', 'codex'],
        env: { MESH_CALLER_AGENT: 'codex', ...this.agentMcpEnv },
      };
      // Drop the legacy duplicate so upgrades clean themselves up.
      delete config.mcpServers['cross-agent-bridge'];
      fs.writeFileSync(jsonPath, JSON.stringify(config, null, 2), 'utf8');
    } catch {}

    return true;
  }

  private syncAntigravityConfig(): boolean {
    const home = this.homeDir;
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
        ...this.agentMcpEnv,
      },
    };

    fs.writeFileSync(mcpConfigPath, JSON.stringify(config, null, 2), 'utf8');
    return true;
  }
}
