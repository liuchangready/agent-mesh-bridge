import * as vscode from 'vscode';
import { TaskStore } from './bridge/task-store';
import { AgentDetector } from './bridge/agent-detector';
import { ActiveAgentResolver } from './bridge/active-agent-resolver';
import { AutonomousRunner } from './bridge/autonomous-runner';
import { CallbackManager } from './bridge/callback-manager';
import { AsyncDispatcher } from './bridge/async-dispatcher';
import { ReworkController } from './bridge/rework-controller';
import { ConfigSyncService } from './config/config-sync';
import { StatusBarController } from './ui/status-bar';
import { TaskTreeProvider, TaskTreeItem } from './ui/task-tree-provider';
import { TaskDetailPanel } from './ui/detail-panel';
import { AgentId } from './bridge/types';

export function activate(context: vscode.ExtensionContext) {
  console.log('[Agent Mesh Bridge] Activating extension...');

  // 1. Read configuration
  const config = vscode.workspace.getConfiguration('agentMeshBridge');
  const customPaths: Partial<Record<AgentId, string>> = {};
  const agyPath = config.get<string>('antigravityPath');
  const claudePath = config.get<string>('claudeCodePath');
  const codexPath = config.get<string>('codexPath');
  if (agyPath) customPaths.antigravity = agyPath;
  if (claudePath) customPaths.claude_code = claudePath;
  if (codexPath) customPaths.codex = codexPath;

  // 2. Initialize Core Services
  const taskStore = TaskStore.getInstance();
  const detector = new AgentDetector(customPaths);
  const resolver = new ActiveAgentResolver(detector);
  const runner = new AutonomousRunner(detector);
  const callbackManager = new CallbackManager(detector);
  const dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager);
  const reworkController = new ReworkController(taskStore, runner, callbackManager);
  const configSync = new ConfigSyncService();

  // Apply defaultAgent config if manual
  const defaultAgentCfg = config.get<string>('defaultAgent', 'auto');
  if (defaultAgentCfg && defaultAgentCfg !== 'auto') {
    resolver.setManualOverride(defaultAgentCfg as AgentId);
  }

  // 3. Status Bar Controller
  const statusBar = new StatusBarController(resolver);
  context.subscriptions.push(statusBar.getStatusBarItem());

  // 4. Tree Views
  const activeTreeProvider = new TaskTreeProvider(taskStore, 'active');
  const recentTreeProvider = new TaskTreeProvider(taskStore, 'all');

  const activeTreeView = vscode.window.createTreeView('agent-mesh-bridge.active-tasks', {
    treeDataProvider: activeTreeProvider,
    showCollapseAll: true,
  });
  const recentTreeView = vscode.window.createTreeView('agent-mesh-bridge.recent-tasks', {
    treeDataProvider: recentTreeProvider,
    showCollapseAll: true,
  });

  context.subscriptions.push(activeTreeView, recentTreeView);

  // 5. Notify in VSCode when tasks complete
  callbackManager.on('taskCompleted', ({ thread, round }) => {
    const isSuccess = round.status === 'completed';
    const msg = isSuccess
      ? `[Agent Bridge] ${thread.targetAgent} completed Round ${round.round} for "${thread.title}".`
      : `[Agent Bridge] ${thread.targetAgent} failed Round ${round.round} for "${thread.title}".`;

    vscode.window.showInformationMessage(msg, 'View Details').then((choice) => {
      if (choice === 'View Details') {
        TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, thread.threadId);
      }
    });

    statusBar.update();
  });

  // 6. Register Commands
  context.subscriptions.push(
    vscode.commands.registerCommand('agent-mesh-bridge.refreshTasks', () => {
      taskStore.loadFromDisk();
      activeTreeProvider.refresh();
      recentTreeProvider.refresh();
      statusBar.update();
      vscode.window.showInformationMessage('Agent Mesh Bridge: Tasks refreshed.');
    }),

    vscode.commands.registerCommand('agent-mesh-bridge.selectActiveAgent', async () => {
      await statusBar.promptUserToSelect();
    }),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.openTaskDetail',
      (itemOrThreadId: TaskTreeItem | string) => {
        const threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (threadId) {
          TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
        } else {
          vscode.window.showErrorMessage('No task selected to view.');
        }
      },
    ),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.approveTask',
      async (item?: TaskTreeItem) => {
        const threadId = item?.thread?.threadId;
        if (!threadId) {
          const input = await vscode.window.showInputBox({ prompt: 'Enter Thread ID to approve:' });
          if (!input) return;
          try {
            reworkController.approveTask({ threadId: input });
            vscode.window.showInformationMessage(`Task ${input} approved.`);
          } catch (e: any) {
            vscode.window.showErrorMessage(e.message);
          }
          return;
        }

        try {
          reworkController.approveTask({ threadId });
          vscode.window.showInformationMessage(`Task ${threadId} approved.`);
        } catch (e: any) {
          vscode.window.showErrorMessage(e.message);
        }
      },
    ),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.requestRework',
      async (item?: TaskTreeItem) => {
        let threadId = item?.thread?.threadId;
        if (!threadId) {
          const input = await vscode.window.showInputBox({ prompt: 'Enter Thread ID for rework:' });
          if (!input) return;
          threadId = input;
        }

        const feedback = await vscode.window.showInputBox({
          prompt: 'Enter rework feedback for the agent:',
          placeHolder: 'Detail required fixes or additional instructions...',
        });
        if (!feedback) return;

        try {
          await reworkController.requestRework({ threadId, feedback });
          vscode.window.showInformationMessage(`Rework requested for ${threadId} in same session.`);
        } catch (e: any) {
          vscode.window.showErrorMessage(e.message);
        }
      },
    ),

    vscode.commands.registerCommand('agent-mesh-bridge.syncMcpConfig', async () => {
      const result = await configSync.syncAll();
      if (result.success) {
        vscode.window.showInformationMessage(
          `Agent Mesh Bridge: Synced MCP configs to ${result.synced.join(', ')}.`,
        );
      } else {
        vscode.window.showWarningMessage(
          `Agent Mesh Bridge: Synced with warnings. Synced: ${result.synced.join(', ') || 'None'}. Errors: ${result.errors.join('; ')}`,
        );
      }
    }),
  );

  // 7. Auto Sync MCP if enabled
  if (config.get<boolean>('autoSyncMcp', true)) {
    configSync
      .syncAll()
      .then((res) => {
        if (res.synced.length > 0) {
          console.log(`[Agent Mesh Bridge] Auto-synced MCP config to: ${res.synced.join(', ')}`);
        }
      })
      .catch((err) => {
        console.warn('[Agent Mesh Bridge] Auto MCP sync failed:', err);
      });
  }

  console.log('[Agent Mesh Bridge] Activated successfully.');
}

export function deactivate() {
  const store = TaskStore.getInstance();
  store.dispose();
  console.log('[Agent Mesh Bridge] Deactivated.');
}
