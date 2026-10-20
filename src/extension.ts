import * as vscode from 'vscode';
import * as path from 'path';
import * as os from 'os';
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
import { TaskSupervisor } from './bridge/task-supervisor';
import { TaskControlService } from './bridge/task-control';
import {
  DSH_PROVIDER_ENV_HINT,
  DSH_PROVIDER_ID_HINT,
  DshProviderConfig,
  isDshProviderUsable,
  readDshConfiguredProvider,
} from './bridge/deepseek-harness';
import {
  ModelCatalog,
  ModelDefaults,
  invalidateConfiguredModelCache,
  normalizeModelId,
  resolveConfiguredModel,
  saveModelDefaults,
} from './bridge/model-catalog';

/** Settings key holding the default model of each agent. */
const MODEL_SETTING_KEYS: Record<AgentId, string> = {
  antigravity: 'models.antigravity',
  codex: 'models.codex',
  claude_code: 'models.claudeCode',
  deepseek_harness: 'models.deepseekHarness',
  vscode_chat: 'models.vscodeChat',
};

const AGENT_LABELS: Record<AgentId, string> = {
  antigravity: 'Google Antigravity',
  codex: 'OpenAI Codex',
  claude_code: 'Anthropic Claude Code',
  deepseek_harness: 'DeepSeek Harness',
  vscode_chat: 'VSCode Chat',
};

export function activate(context: vscode.ExtensionContext) {
  console.log('[Agent Mesh Bridge] Activating extension...');

  // 1. Read configuration
  const config = vscode.workspace.getConfiguration('agentMeshBridge');
  const customPaths: Partial<Record<AgentId, string>> = {};
  const agyPath = config.get<string>('antigravityPath');
  const claudePath = config.get<string>('claudeCodePath');
  const codexPath = config.get<string>('codexPath');
  const dshPath = config.get<string>('deepseekHarnessPath');
  if (agyPath) customPaths.antigravity = agyPath;
  if (claudePath) customPaths.claude_code = claudePath;
  if (codexPath) customPaths.codex = codexPath;
  if (dshPath) customPaths.deepseek_harness = dshPath;

  /**
   * Resolve the DeepSeek Harness model route used by bridge-launched
   * (`dsh headless`) runs.
   *
   * DSH's headless profile disables the account/credentials plugins, so an
   * endpoint other than the shipped one must be declared in the profile and its
   * key must reach the DSH process. The key is exported into this host's
   * environment *and* handed to every agent's bridge MCP process as an explicit
   * `env` entry (see ConfigSyncService.setAgentMcpEnv) — an agent spawns that
   * process itself, so ambient environment inheritance is not reliable.
   */
  let dshApiKey = '';
  const applyDshProviderSettings = (): DshProviderConfig | null => {
    const dshCfg = vscode.workspace.getConfiguration('agentMeshBridge.deepseekHarness');
    const candidate: DshProviderConfig = {
      provider: (dshCfg.get<string>('provider') || '').trim(),
      api: (dshCfg.get<string>('api') || '').trim() || 'openai-completions',
      baseUrl: (dshCfg.get<string>('baseUrl') || '').trim(),
      model: (dshCfg.get<string>('model') || '').trim(),
      apiKeyEnv: (dshCfg.get<string>('apiKeyEnv') || '').trim() || 'COMMANDCODE_API_KEY',
    };
    const apiKey = (dshCfg.get<string>('apiKey') || '').trim();
    dshApiKey = apiKey;

    if (!isDshProviderUsable(candidate)) {
      delete process.env[DSH_PROVIDER_ENV_HINT];
      delete process.env[DSH_PROVIDER_ID_HINT];
      return null;
    }

    if (apiKey) {
      process.env[candidate.apiKeyEnv] = apiKey;
    }
    process.env[DSH_PROVIDER_ENV_HINT] = candidate.apiKeyEnv;
    process.env[DSH_PROVIDER_ID_HINT] = candidate.provider;
    return candidate;
  };

  /** Variables every registered bridge MCP process receives. */
  const agentMcpEnvFor = (provider: DshProviderConfig | null): Record<string, string> =>
    provider && dshApiKey ? { [provider.apiKeyEnv]: dshApiKey } : {};

  const dshProvider = applyDshProviderSettings();

  // 2. Initialize Core Services
  const taskStore = TaskStore.getInstance();
  const detector = new AgentDetector(customPaths);
  const resolver = new ActiveAgentResolver(detector);
  const modelCatalog = new ModelCatalog(detector);

  /**
   * Mirror the per-agent model settings into `~/.agent-mesh-bridge/model-config.json`.
   *
   * The MCP server runs in its own process (spawned by each agent, without VS
   * Code APIs), so the settings are not directly visible to it. This snapshot is
   * what `dispatch_task` uses when the caller does not pin a model.
   */
  const readModelSettings = (): ModelDefaults => {
    const modelCfg = vscode.workspace.getConfiguration('agentMeshBridge');
    const defaults: ModelDefaults = {};
    for (const agent of ['antigravity', 'codex', 'claude_code', 'deepseek_harness'] as AgentId[]) {
      const value = normalizeModelId(modelCfg.get<string>(MODEL_SETTING_KEYS[agent]));
      if (value) defaults[agent] = value;
    }
    return defaults;
  };

  const applyModelSettings = (): ModelDefaults => {
    const defaults = readModelSettings();
    saveModelDefaults(defaults);
    invalidateConfiguredModelCache();
    return defaults;
  };

  applyModelSettings();
  const timeoutMinutes = config.get<number>('taskTimeoutMinutes', 120);
  const runner = new AutonomousRunner(detector, timeoutMinutes * 60 * 1000);
  const callbackManager = new CallbackManager(detector);
  const _dispatcher = new AsyncDispatcher(taskStore, resolver, runner, callbackManager, detector);
  const reworkController = new ReworkController(taskStore, runner, callbackManager, detector);
  const taskControl = new TaskControlService(taskStore, runner);
  const configSync = new ConfigSyncService(undefined, dshProvider);
  configSync.setAgentMcpEnv(agentMcpEnvFor(dshProvider));
  const supervisor = new TaskSupervisor(taskStore, runner, callbackManager, detector);
  context.subscriptions.push({ dispose: () => supervisor.dispose() });

  supervisor.on('taskStarted', ({ thread, round }) => {
    statusBar.update();
    activeTreeProvider.refresh();
    recentTreeProvider.refresh();
    vscode.window.showInformationMessage(
      `[Agent Bridge] 开始执行【${thread.title}】第 ${round.round} 轮 (${thread.targetAgent})...`,
    );
  });

  supervisor.on('taskCompleted', () => {
    statusBar.update();
    activeTreeProvider.refresh();
    recentTreeProvider.refresh();
  });

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

  // Track completed tasks to announce when a task transitions to completed
  const knownCompletedThreadIds = new Set<string>();
  for (const t of taskStore.getAllTasks()) {
    if (t.status === 'completed') {
      knownCompletedThreadIds.add(t.threadId);
    }
  }

  // Auto-refresh all tree views and notify when tasks complete or change on disk
  taskStore.on('changed', (allTasks: any[]) => {
    activeTreeProvider.refresh();
    recentTreeProvider.refresh();
    statusBar.update();
    agentNodesProvider.refresh(false);

    const taskList = Array.isArray(allTasks) ? allTasks : taskStore.getAllTasks();
    for (const t of taskList) {
      if (t.status === 'completed' && !knownCompletedThreadIds.has(t.threadId)) {
        knownCompletedThreadIds.add(t.threadId);
        const lastRound = t.rounds && t.rounds[t.rounds.length - 1];
        const snippet = lastRound && lastRound.prompt ? `\n验收备注: ${lastRound.prompt.slice(0, 100)}` : '';
        vscode.window.showInformationMessage(
          `🎉 [Agent Bridge] 任务【${t.title}】已由 ${t.initiatorAgent} 验收通过并正式归档！${snippet}`,
          '查看详情 (View Details)',
        ).then((choice) => {
          if (choice === '查看详情 (View Details)') {
            TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, t.threadId);
          }
        });
      }
    }
  });

  // 5. Notify in VSCode when tasks complete a round
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

  callbackManager.on('callbackFailed', async ({ thread, round, error }) => {
    vscode.window
      .showWarningMessage(
        `⚠️ [Agent Bridge] 自动唤醒下发者 ${thread.initiatorAgent} 失败: ${error}。`,
        '手动唤醒 (Wake Up)',
        '查看日志 (View Log)',
      )
      .then((choice) => {
        if (choice === '手动唤醒 (Wake Up)') {
          vscode.commands.executeCommand('agent-mesh-bridge.wakeUpCaller', thread.threadId);
        } else if (choice === '查看日志 (View Log)') {
          const logPath = path.join(os.homedir(), '.agent-mesh-bridge', 'callbacks.log');
          vscode.workspace
            .openTextDocument(logPath)
            .then((doc) => vscode.window.showTextDocument(doc));
        }
      });
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
          const pick = await vscode.window.showQuickPick(
            ['antigravity', 'codex', 'claude_code', 'deepseek_harness'],
            {
              placeHolder: '选择要清除耗尽状态的 Agent',
            },
          );
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
      'agent-mesh-bridge.printRecentLogs',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          taskStore.loadFromDisk();
          const tasks = taskStore.getAllTasks();
          if (tasks.length === 0) {
            vscode.window.showInformationMessage('当前暂无任何任务。');
            return;
          }

          const picks = tasks.map((t) => {
            const isRunning = supervisor.isExecuting(t.threadId);
            const prefix = isRunning ? '⚡ [运行中] ' : t.status === 'queued' ? '⏳ [排队中] ' : '';
            return {
              label: `${prefix}${t.title}`,
              description: `[${t.initiatorAgent} ➔ ${t.targetAgent}] · ${t.status}`,
              detail: `Thread ID: ${t.threadId}`,
              threadId: t.threadId,
            };
          });

          const selected = await vscode.window.showQuickPick(picks, {
            placeHolder: '选择要查看/打印最近1000行实时日志的任务:',
          });
          if (!selected) return;
          threadId = selected.threadId;
        }

        taskStore.loadFromDisk();
        const thread = taskStore.getTask(threadId);
        const title = thread ? thread.title : threadId;

        const logInfo = TaskSupervisor.getRecentLogs(threadId, 1000, taskStore);

        const channel = vscode.window.createOutputChannel(`Agent Bridge: ${title.slice(0, 20)} Logs`);
        context.subscriptions.push(channel);

        channel.clear();
        channel.appendLine('='.repeat(80));
        channel.appendLine(`[Agent Mesh Bridge] 最近 1,000 行实时日志 (Thread: ${threadId})`);
        channel.appendLine(`任务标题: ${title}`);
        channel.appendLine(
          `运行状态: ${logInfo.isRunning ? '⚡ 正在运行中 (Running)' : `已停止 (${thread?.status || 'unknown'})`}`,
        );
        channel.appendLine(
          `总输出行数: ${logInfo.totalLines} 行 | 本次打印: 最近 ${logInfo.returnedLines} 行`,
        );
        if (logInfo.logFilePath) {
          channel.appendLine(`本地日志源文件: ${logInfo.logFilePath}`);
        }
        channel.appendLine('='.repeat(80));
        channel.appendLine('');

        if (logInfo.logs && logInfo.logs.trim()) {
          channel.appendLine(logInfo.logs);
        } else {
          channel.appendLine('(当前暂无实时输出日志)');
        }

        channel.show(true);

        vscode.window.showInformationMessage(
          `已在输出面板展示【${title}】最近 1000 行实时日志（共 ${logInfo.returnedLines} 行）。`,
        );
      },
    ),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.pauseTask',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          const input = await vscode.window.showInputBox({
            prompt: '输入要暂停的 Thread ID:',
          });
          if (!input) return;
          threadId = input.trim();
        }

        const reason = await vscode.window.showInputBox({
          prompt: `请输入暂停【${threadId}】的中断原因:`,
          value: '用户手动暂停',
          placeHolder: '例如：需要先处理更高优先级任务',
        });
        if (reason === undefined) return;

        try {
          const result = taskControl.pauseTask(threadId, reason, 'user');
          vscode.window.showInformationMessage(result.message);
          taskStore.loadFromDisk();
          activeTreeProvider.refresh();
          recentTreeProvider.refresh();
          statusBar.update();
          TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
        } catch (e: any) {
          vscode.window.showErrorMessage(`暂停任务失败: ${e.message}`);
        }
      },
    ),

    vscode.commands.registerCommand(
      'agent-mesh-bridge.deleteTask',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          const input = await vscode.window.showInputBox({
            prompt: '输入要删除的 Thread ID:',
          });
          if (!input) return;
          threadId = input.trim();
        }

        taskStore.loadFromDisk();
        const thread = taskStore.getTask(threadId);
        const title = thread ? thread.title : threadId;
        const confirm = await vscode.window.showWarningMessage(
          `确定要永久删除任务【${title}】吗？如果它正在运行，会先终止 Agent 进程；删除后不可恢复。`,
          { modal: true },
          '删除',
          '取消',
        );
        if (confirm !== '删除') return;

        const reason = await vscode.window.showInputBox({
          prompt: `请输入删除【${title}】的原因（会写入删除墓碑日志）:`,
          value: '用户手动删除',
          placeHolder: '例如：任务重复 / 需求取消',
        });
        if (reason === undefined) return;

        try {
          const result = taskControl.deleteTask(threadId, reason, 'user');
          vscode.window.showInformationMessage(result.message);
          taskStore.loadFromDisk();
          activeTreeProvider.refresh();
          recentTreeProvider.refresh();
          statusBar.update();
        } catch (e: any) {
          vscode.window.showErrorMessage(`删除任务失败: ${e.message}`);
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

        taskStore.loadFromDisk();
        const thread = taskStore.getTask(threadId);
        if (!thread) {
          vscode.window.showErrorMessage(`未找到任务: ${threadId}`);
          return;
        }

        if (thread.status === 'paused' || (thread.rounds || []).some((r) => r.status === 'paused')) {
          try {
            const round = await reworkController.resumePausedTask(threadId);
            vscode.window.showInformationMessage(
              `已恢复暂停任务 ${threadId} 第 ${round.round} 轮，正在同一原生会话中继续执行。`,
            );
            TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
          } catch (e: any) {
            vscode.window.showErrorMessage(`恢复暂停任务失败: ${e.message}`);
          }
          return;
        }

        const runningRounds = (thread.rounds || []).filter((r) => r.status === 'running');
        const targetRound =
          thread.rounds.find((r) => r.round === thread.currentRound && r.status === 'running') ||
          runningRounds[runningRounds.length - 1];

        // If currently in running/reworking and has a round waiting to execute
        if (targetRound && targetRound.prompt) {
          supervisor.executeThreadRound(thread, targetRound);
          vscode.window.showInformationMessage(
            `已恢复执行任务 ${threadId} 第 ${targetRound.round} 轮！`,
          );
          TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
          return;
        }

        const lastRound = thread.rounds && thread.rounds[thread.rounds.length - 1];
        if (lastRound && lastRound.status === 'failed' && lastRound.prompt) {
          const choice = await vscode.window.showQuickPick(
            [
              {
                label: `$(refresh) 重新执行失败轮次 (第 ${lastRound.round} 轮)`,
                description: '保留原有一致指令与上下文，重新执行此轮',
                action: 'retry',
              },
              {
                label: `$(plus) 追加新的一轮返修 (第 ${(thread.currentRound || 1) + 1} 轮)`,
                description: '输入新的反馈要求，追加后续轮次',
                action: 'new_round',
              },
            ],
            { placeHolder: `任务第 ${lastRound.round} 轮执行中断/失败，请选择恢复方式:` },
          );

          if (!choice) return;

          if (choice.action === 'retry') {
            await reworkController.retryFailedRound(threadId);
            vscode.window.showInformationMessage(
              `正在重新执行任务 ${threadId} 第 ${lastRound.round} 轮...`,
            );
            TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
            return;
          }
        }

        const defaultValue =
          lastRound && lastRound.prompt
            ? lastRound.prompt
            : '检测到任务中断，请在当前原生会话中继续执行未完成的开发与交付。';

        const feedback = await vscode.window.showInputBox({
          prompt: `恢复继续任务 [${thread.title}]：`,
          value: defaultValue,
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
      'agent-mesh-bridge.retryFailedRound',
      async (itemOrThreadId?: TaskTreeItem | string) => {
        let threadId =
          typeof itemOrThreadId === 'string' ? itemOrThreadId : itemOrThreadId?.thread?.threadId;

        if (!threadId) {
          const input = await vscode.window.showInputBox({
            prompt: '输入要重试失败轮次的 Thread ID:',
          });
          if (!input) return;
          threadId = input.trim();
        }

        try {
          const retried = await reworkController.retryFailedRound(threadId);
          vscode.window.showInformationMessage(
            `正在重新执行任务 ${threadId} 第 ${retried.round} 轮...`,
          );
          TaskDetailPanel.render(context.extensionUri, taskStore, reworkController, threadId);
        } catch (e: any) {
          vscode.window.showErrorMessage(`重试失败轮次失败: ${e.message}`);
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

        taskStore.loadFromDisk();
        const thread = taskStore.getTask(threadId);
        if (!thread) {
          vscode.window.showErrorMessage(`未找到任务: ${threadId}`);
          return;
        }

        const lastRound =
          [...thread.rounds].reverse().find((r) => r.status === 'completed') ||
          thread.rounds[thread.rounds.length - 1];
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

    vscode.commands.registerCommand(
      'agent-mesh-bridge.selectAgentModel',
      async (item?: AgentNodeItem) => {
        const targets: AgentId[] = ['antigravity', 'codex', 'claude_code', 'deepseek_harness'];

        let agent = item?.info?.id;
        if (!agent || agent === 'vscode_chat') {
          const pick = await vscode.window.showQuickPick(
            targets.map((a) => ({
              label: AGENT_LABELS[a],
              description: resolveConfiguredModel(a) || 'Agent 默认模型',
              agent: a,
            })),
            { placeHolder: '选择要配置默认模型的 Agent' },
          );
          if (!pick) return;
          agent = pick.agent;
        }
        const targetAgent: AgentId = agent;
        const modelCfg = vscode.workspace.getConfiguration('agentMeshBridge');

        for (;;) {
          const catalog = await vscode.window.withProgress(
            {
              location: vscode.ProgressLocation.Notification,
              title: `Agent Bridge: 正在探测 ${AGENT_LABELS[targetAgent]} 可用模型...`,
            },
            () => modelCatalog.get(targetAgent, false),
          );

          if (catalog.error) {
            vscode.window.showWarningMessage(
              `探测 ${AGENT_LABELS[targetAgent]} 模型不完整: ${catalog.error}`,
            );
          }

          const current = resolveConfiguredModel(targetAgent);
          const items: any[] = [
            {
              label: '$(check) 使用 Agent 默认模型',
              description: current ? `当前: ${current}（选择后清除）` : '当前已使用 Agent 默认',
              action: 'clear',
            },
            ...catalog.models.map((m) => ({
              label: current === m.id ? `$(check) ${m.label || m.id}` : m.label || m.id,
              description: m.id,
              detail: m.hint ? `标记: ${m.hint}` : undefined,
              action: 'set',
              model: m.id,
            })),
            {
              label: '$(refresh) 重新探测可用模型',
              description: `来源: ${catalog.source} · 共 ${catalog.models.length} 个`,
              action: 'refresh',
            },
            {
              label: '$(edit) 手动输入模型 ID',
              description: '适用于列表外的自定义模型',
              action: 'manual',
            },
          ];

          const selected = await vscode.window.showQuickPick(items, {
            placeHolder: `${AGENT_LABELS[targetAgent]} 默认模型 · 来源: ${catalog.source}`,
            matchOnDescription: true,
          });
          if (!selected) return;

          if (selected.action === 'refresh') {
            await modelCatalog.get(targetAgent, true);
            continue;
          }

          let model: string | undefined;
          if (selected.action === 'manual') {
            const input = await vscode.window.showInputBox({
              prompt: `输入 ${AGENT_LABELS[targetAgent]} 要使用的模型 ID`,
              value: current || '',
              placeHolder: '例如 gpt-6-sol / sonnet / gemini-3.8-flash-high',
            });
            if (input === undefined) continue;
            model = normalizeModelId(input);
            if (!model) {
              vscode.window.showWarningMessage('模型 ID 不能为空。');
              continue;
            }
          } else if (selected.action === 'clear') {
            model = undefined;
          } else {
            model = selected.model;
          }

          if (targetAgent === 'deepseek_harness' && model && !readDshConfiguredProvider()) {
            vscode.window.showWarningMessage(
              'DeepSeek Harness 需先配置 provider 路由（agentMeshBridge.deepseekHarness.provider / baseUrl / apiKey）才能按任务切换模型。',
            );
          }

          await modelCfg.update(
            MODEL_SETTING_KEYS[targetAgent],
            model ?? '',
            vscode.ConfigurationTarget.Global,
          );
          applyModelSettings();
          agentNodesProvider.refresh(false);

          vscode.window.showInformationMessage(
            model
              ? `Agent Bridge: ${AGENT_LABELS[targetAgent]} 默认模型已设为 ${model}，后续下发任务将使用该模型。`
              : `Agent Bridge: 已清除 ${AGENT_LABELS[targetAgent]} 的模型配置，恢复使用 Agent 默认模型。`,
          );
          return;
        }
      },
    ),

    vscode.commands.registerCommand('agent-mesh-bridge.configureDeepSeekHarness', async () => {
      await vscode.commands.executeCommand(
        'workbench.action.openSettings',
        'agentMeshBridge.deepseekHarness',
      );
    }),
  );

  // Keep the mirrored model defaults in sync with the settings. The picker
  // writes the setting, which lands here; hand-edited settings land here too.
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration((e) => {
      if (!e.affectsConfiguration('agentMeshBridge.models')) return;
      applyModelSettings();
      agentNodesProvider.refresh(false);
    }),
  );

  // Re-apply the DeepSeek Harness model route whenever its settings change, so
  // the profile patch and the reported login state follow the configuration.
  context.subscriptions.push(
    vscode.workspace.onDidChangeConfiguration(async (e) => {
      if (!e.affectsConfiguration('agentMeshBridge.deepseekHarness')) return;
      const provider = applyDshProviderSettings();
      configSync.setDshProvider(provider);
      configSync.setAgentMcpEnv(agentMcpEnvFor(provider));
      await configSync.syncAll();
      await agentNodesProvider.refresh(true);
      await statusBar.update();
      vscode.window.showInformationMessage(
        provider
          ? `Agent Mesh Bridge: DeepSeek Harness 模型路由已更新 (provider: ${provider.provider})。`
          : 'Agent Mesh Bridge: DeepSeek Harness 已恢复为官方 provider 路由。',
      );
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
