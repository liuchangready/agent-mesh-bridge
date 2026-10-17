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
import { AgentNodesProvider, AgentNodeItem } from './ui/agent-nodes-provider';
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
  const timeoutMinutes = config.get<number>('taskTimeoutMinutes', 120);
  const runner = new AutonomousRunner(detector, timeoutMinutes * 60 * 1000);
  const callbackManager = new CallbackManager(detector);
  const _dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector);
  const reworkController = new ReworkController(taskStore, runner, callbackManager, detector);
  const configSync = new ConfigSyncService();

  // Apply defaultAgent config if manual
  const defaultAgentCfg = config.get<string>('defaultAgent', 'auto');
  if (defaultAgentCfg && defaultAgentCfg !== 'auto') {
    resolver.setManualOverride(defaultAgentCfg as AgentId);
  }

  // 3. Status Bar Controller
  const statusBar = new StatusBarController(resolver, detector);
  context.subscriptions.push(statusBar.getStatusBarItem());

  // 4. Tree Views
  const agentNodesProvider = new AgentNodesProvider(detector, resolver);
  const activeTreeProvider = new TaskTreeProvider(taskStore, 'active');
  const recentTreeProvider = new TaskTreeProvider(taskStore, 'all');

  const agentNodesTreeView = vscode.window.createTreeView('agent-mesh-bridge.agent-nodes', {
    treeDataProvider: agentNodesProvider,
    showCollapseAll: false,
  });
  const activeTreeView = vscode.window.createTreeView('agent-mesh-bridge.active-tasks', {
    treeDataProvider: activeTreeProvider,
    showCollapseAll: true,
  });
  const recentTreeView = vscode.window.createTreeView('agent-mesh-bridge.recent-tasks', {
    treeDataProvider: recentTreeProvider,
    showCollapseAll: true,
  });

  context.subscriptions.push(agentNodesTreeView, activeTreeView, recentTreeView);

  // 5. Notify in VSCode when tasks complete
  callbackManager.on('taskCompleted', async ({ thread, round }) => {
    const isSuccess = round.status === 'completed';
    const statusText = isSuccess ? '已完成' : '执行失败';
    const msg = `[Agent Bridge] ${thread.targetAgent} ${statusText} 第 ${round.round} 轮: "${thread.title}".`;

    const actions = ['查看详情 (View Details)'];
    if (isSuccess) {
      actions.push('验收通过 (Approve)');
      actions.push('唤醒下发者 (Wake Up Caller)');
    } else {
      actions.push('恢复继续 (Resume)');
    }

    vscode.window.showInformationMessage(msg, ...actions).then(async (choice) => {
      if (choice === '查看详情 (View Details)') {
        TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, thread.threadId);
      } else if (choice === '验收通过 (Approve)') {
        vscode.commands.executeCommand('agent-mesh-bridge.approveTask', { thread });
      } else if (choice === '唤醒下发者 (Wake Up Caller)') {
        vscode.commands.executeCommand('agent-mesh-bridge.wakeUpCaller', thread.threadId);
      } else if (choice === '恢复继续 (Resume)') {
        vscode.commands.executeCommand('agent-mesh-bridge.resumeTask', thread.threadId);
      }
    });

    statusBar.update();
    agentNodesProvider.refresh(false);
  });

  // 6. Register Commands
  context.subscriptions.push(
    vscode.commands.registerCommand('agent-mesh-bridge.refreshAgentStatus', async () => {
      await agentNodesProvider.refresh(true);
      await statusBar.update();
      vscode.window.showInformationMessage('Agent Mesh Bridge: 节点状态与限流信息已刷新。');
    }),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.clearAgentExhaustion',
      async (item?: AgentNodeItem) => {
        if (item && item.info) {
          detector.getRateLimitTracker().clearExhaustion(item.info.id);
          await agentNodesProvider.refresh(true);
          await statusBar.update();
          vscode.window.showInformationMessage(`已清除 ${item.info.name} 的耗尽状态标记。`);
        } else {
          const pick = await vscode.window.showQuickPick(['antigravity', 'codex', 'claude_code'], {
            placeHolder: '选择要清除耗尽状态的 Agent',
          });
          if (pick) {
            detector.getRateLimitTracker().clearExhaustion(pick as AgentId);
            await agentNodesProvider.refresh(true);
            await statusBar.update();
            vscode.window.showInformationMessage(`已清除 ${pick} 的耗尽状态标记。`);
          }
        }
      },
    ),

    vscode.commands.registerCommand('agent-mesh-bridge.refreshTasks', () => {
      taskStore.loadFromDisk();
      agentNodesProvider.refresh(false);
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

    vscode.commands.registerCommand(
      'agent-mesh-bridge.resumeTask',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          const input = await vscode.window.showInputBox({
            prompt: '输入要恢复/继续执行的 Thread ID:',
          });
          if (!input) return;
          threadId = input.trim();
        }

        const thread = taskStore.getTask(threadId);
        if (!thread) {
          vscode.window.showErrorMessage(`未找到任务: ${threadId}`);
          return;
        }

        const feedback = await vscode.window.showInputBox({
          prompt: `恢复继续任务 [${thread.title}]：`,
          value: '检测到任务中断，请在当前原生会话中继续执行未完成的开发与交付。',
          placeHolder: '可输入补充要求或直接回车继续...',
        });
        if (!feedback) return;

        try {
          await reworkController.requestRework({ threadId, feedback: feedback.trim() });
          vscode.window.showInformationMessage(
            `已成功恢复任务 ${threadId}，并在原生会话中继续执行。`,
          );
          TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
        } catch (e: any) {
          vscode.window.showErrorMessage(`恢复任务失败: ${e.message}`);
        }
      },
    ),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.wakeUpCaller',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          const input = await vscode.window.showInputBox({
            prompt: '输入要唤醒下发者的 Thread ID:',
          });
          if (!input) return;
          threadId = input.trim();
        }

        const thread = taskStore.getTask(threadId);
        if (!thread) {
          vscode.window.showErrorMessage(`未找到任务: ${threadId}`);
          return;
        }

        const lastRound = thread.rounds[thread.rounds.length - 1];
        if (!lastRound) {
          vscode.window.showErrorMessage(`任务无有效轮次: ${threadId}`);
          return;
        }

        // If callerSessionId is missing, allow user to input or confirm detected
        if (!thread.callerSessionId) {
          const detected = detector.detectActiveSession(
            thread.initiatorAgent,
            thread.workspacePath,
            [thread.receiverSessionId],
          );
          const input = await vscode.window.showInputBox({
            prompt: `请输入下发者 [${thread.initiatorAgent}] 的会话 ID:`,
            value: detected || '',
            placeHolder: '例如 UUID 会话 ID',
          });
          if (!input) return;
          thread.callerSessionId = input.trim();
          taskStore.upsertTask(thread);
        }

        const success = await callbackManager.notifyCaller(thread, lastRound);
        if (success) {
          vscode.window.showInformationMessage(
            `已成功向下发者 ${thread.initiatorAgent} (会话: ${thread.callerSessionId}) 触发检视回调通知！`,
          );
        } else {
          vscode.window.showWarningMessage(
            `唤醒下发者可能未成功，请检查 ${thread.initiatorAgent} 运行环境与会话有效性。`,
          );
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
