import * as fs from 'fs';
import * as path from 'path';
import * as vscode from 'vscode';
import { execFileSync } from 'child_process';
import { lookupInPath } from '../bridge/platform';
import { getBridgeHome } from '../bridge/model-catalog';
import { writeGroupAgentHost, GROUP_AGENT_HOST_FILENAME } from '../bridge/group-agent-registry';

export const COPILOT_MCP_PROVIDER_ID = 'agent-mesh-bridge.copilot';

export function registerCopilotMcpProvider(context: vscode.ExtensionContext): string {
  const hostInstanceId = require('crypto').randomUUID();
  const hostGeneration = Date.now();
  const heartbeatFile = path.join(context.globalStorageUri.fsPath, `copilot-host-${hostInstanceId}.json`);
  let timer: NodeJS.Timeout | undefined;

  const writeHeartbeat = () => {
    const folder = vscode.workspace.workspaceFolders?.[0];
    const workspaceUri = folder?.uri;
    const config = vscode.workspace.getConfiguration('agentMeshBridge.copilot');
    const reviewer = config.get<string>('reviewer') || '';
    const payload = {
      hostInstanceId,
      workspaceUri: workspaceUri?.toString(),
      workspaceAuthority: workspaceUri?.authority,
      workspacePath: workspaceUri?.scheme === 'file' ? workspaceUri.fsPath : undefined,
      remoteName: vscode.env.remoteName,
      uiKind: vscode.env.uiKind === vscode.UIKind.Web ? 'web' : 'desktop',
      trusted: vscode.workspace.isTrusted,
      enabled: config.get<boolean>('enabled', false),
      workspaceCount: vscode.workspace.workspaceFolders?.length || 0,
      reviewer,
      updatedAt: Date.now(),
    };
    try {
      fs.mkdirSync(path.dirname(heartbeatFile), { recursive: true });
      const tmp = `${heartbeatFile}.${process.pid}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(payload), 'utf8');
      fs.renameSync(tmp, heartbeatFile);
      const bridgeHome = getBridgeHome();
      const groupHostFile = path.join(bridgeHome, GROUP_AGENT_HOST_FILENAME);
      if (workspaceUri?.scheme === 'file' && vscode.workspace.isTrusted && vscode.env.uiKind !== vscode.UIKind.Web && !vscode.env.remoteName && vscode.workspace.workspaceFolders?.length === 1) {
        try {
          const commonDirRaw = execFileSync('git', ['-C', workspaceUri.fsPath, 'rev-parse', '--git-common-dir'], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] }).trim();
          const commonDir = path.resolve(workspaceUri.fsPath, commonDirRaw);
          if (commonDir && fs.existsSync(commonDir)) writeGroupAgentHost({
            version: 1, hostInstanceId, generation: hostGeneration, pid: process.pid,
            workspaceUri: workspaceUri.toString(), repositoryCommonDir: fs.realpathSync(commonDir),
            trusted: true, localDesktop: true, updatedAt: Date.now(),
          }, bridgeHome);
        } catch {
          try {
            const existing = JSON.parse(fs.readFileSync(groupHostFile, 'utf8'));
            if (existing.hostInstanceId === hostInstanceId) fs.unlinkSync(groupHostFile);
          } catch {}
        }
      } else {
        try {
          const existing = JSON.parse(fs.readFileSync(groupHostFile, 'utf8'));
          if (existing.hostInstanceId === hostInstanceId) fs.unlinkSync(groupHostFile);
        } catch {}
      }
    } catch (error) {
      console.warn('[Agent Mesh Bridge] Could not refresh Copilot MCP host binding:', error);
    }
  };
  writeHeartbeat();
  timer = setInterval(writeHeartbeat, 5000);
  context.subscriptions.push({ dispose: () => {
    if (timer) clearInterval(timer);
    try { fs.unlinkSync(heartbeatFile); } catch { /* already removed */ }
  } });

  const emitter = new vscode.EventEmitter<void>();
  let unsupportedEnvironmentWarned = false;
  context.subscriptions.push(emitter);
  context.subscriptions.push(vscode.workspace.onDidChangeWorkspaceFolders(() => {
    writeHeartbeat();
    emitter.fire();
  }));
  context.subscriptions.push(vscode.workspace.onDidGrantWorkspaceTrust(() => {
    writeHeartbeat();
    emitter.fire();
  }));
  context.subscriptions.push(vscode.workspace.onDidChangeConfiguration((event) => {
    if (event.affectsConfiguration('agentMeshBridge.copilot')) {
      writeHeartbeat();
      emitter.fire();
    }
  }));

  context.subscriptions.push(vscode.lm.registerMcpServerDefinitionProvider(
    COPILOT_MCP_PROVIDER_ID,
    {
      onDidChangeMcpServerDefinitions: emitter.event,
      provideMcpServerDefinitions: async () => {
        const config = vscode.workspace.getConfiguration('agentMeshBridge.copilot');
        if (!config.get<boolean>('enabled', false)) return [];
        const workspace = vscode.workspace.workspaceFolders?.[0]?.uri;
        if (!vscode.workspace.isTrusted || workspace?.scheme !== 'file' || vscode.env.remoteName || vscode.env.uiKind === vscode.UIKind.Web || vscode.workspace.workspaceFolders?.length !== 1) {
          if (!unsupportedEnvironmentWarned) {
            unsupportedEnvironmentWarned = true;
            void vscode.window.showWarningMessage('Copilot requester is disabled here. It requires one trusted local desktop workspace; multi-root, Remote, WSL and Web workspaces are not supported.');
          }
          return [];
        }
        const reviewer = config.get<string>('reviewer') || '';
        if (!['codex', 'claude_code', 'antigravity', 'deepseek_harness'].includes(reviewer)) return [];
        const root = vscode.extensions.getExtension('agent-mesh.agent-mesh-bridge')?.extensionPath;
        if (!root) return [];
        const node = process.env.MESH_NODE_EXECUTABLE || lookupInPath('node');
        if (!node) {
          void vscode.window.showWarningMessage('Agent Mesh Bridge Copilot requester is unavailable: install Node.js and ensure node is on PATH.');
          return [];
        }
        const server = new vscode.McpStdioServerDefinition(
          'Agent Mesh Bridge · Copilot requester',
          node,
          [path.join(root, 'bin', 'agent-bridge-mcp.cjs'), '--profile', 'copilot-requester'],
          {
            MESH_MCP_PROFILE: 'copilot-requester',
            MESH_COPILOT_HOST_ID: hostInstanceId,
            MESH_COPILOT_WORKSPACE_URI: workspace.toString(),
            MESH_COPILOT_WORKSPACE_AUTHORITY: workspace.authority,
            MESH_COPILOT_WORKSPACE_PATH: workspace.fsPath,
            MESH_COPILOT_BINDING_FILE: heartbeatFile,
            MESH_COPILOT_REVIEWER: reviewer,
          },
          '1',
        );
        server.cwd = workspace;
        return [server];
      },
      resolveMcpServerDefinition: async (server) => server,
    },
  ));
  return hostInstanceId;
}
