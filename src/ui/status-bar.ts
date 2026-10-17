import * as vscode from 'vscode';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AgentDetector } from '../bridge/agent-detector';
import { AgentId } from '../bridge/types';

export class StatusBarController {
  private statusBarItem: vscode.StatusBarItem;
  private resolver: ActiveAgentResolver;
  private detector: AgentDetector;

  constructor(resolver: ActiveAgentResolver, detector: AgentDetector) {
    this.resolver = resolver;
    this.detector = detector;
    this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    this.statusBarItem.command = 'agent-mesh-bridge.selectActiveAgent';
    this.update();
  }

  public getStatusBarItem(): vscode.StatusBarItem {
    return this.statusBarItem;
  }

  public async update(): Promise<void> {
    const resolved = await this.resolver.resolveActiveAgent();
    const allAgents = await this.detector.probeAll(false);
    const activeInfo =
      allAgents[resolved.agent] || (await this.detector.probeAgent(resolved.agent));

    const agentLabels: Record<AgentId, string> = {
      antigravity: 'Antigravity',
      claude_code: 'Claude Code',
      codex: 'Codex',
      vscode_chat: 'VSCode Chat',
    };

    let statusSymbol = '$(pass-filled)';
    if (activeInfo.status === 'exhausted') statusSymbol = '$(warning)';
    else if (activeInfo.status === 'unauthenticated') statusSymbol = '$(key)';
    else if (activeInfo.status === 'offline') statusSymbol = '$(circle-slash)';

    const label = agentLabels[resolved.agent] || resolved.agent;
    const modeTag = resolved.mode === 'manual' ? 'Manual' : 'Auto';
    const exhaustedTag = activeInfo.status === 'exhausted' ? ' [耗尽]' : '';

    this.statusBarItem.text = `${statusSymbol} Agent Bridge: ${label}${exhaustedTag} (${modeTag})`;

    // Rich Markdown Tooltip
    const md = new vscode.MarkdownString();
    md.isTrusted = true;
    md.appendMarkdown(`### Agent Mesh Bridge 节点状态\n\n`);
    md.appendMarkdown(`**当前选路**: \`${label}\` (${resolved.reason})\n\n---\n\n`);

    const agentList: AgentId[] = ['antigravity', 'codex', 'claude_code'];
    for (const id of agentList) {
      const info = allAgents[id];
      if (!info) continue;
      let icon = '🟢';
      let statusZh = '在线';
      if (info.status === 'exhausted') {
        icon = '⏳';
        statusZh = `耗尽 (恢复时间: ${info.rateLimit?.resetsAtFormatted || '计算中'})`;
      } else if (info.status === 'unauthenticated') {
        icon = '🟡';
        statusZh = `未登录 (${info.authDetails || '缺失凭证'})`;
      } else if (info.status === 'offline') {
        icon = '🔴';
        statusZh = '离线 (未找到程序)';
      }
      md.appendMarkdown(`- ${icon} **${info.name}**: ${statusZh}\n`);
    }

    md.appendMarkdown(`\n---\n*点击切换目标 Agent 或手动刷新*`);
    this.statusBarItem.tooltip = md;
    this.statusBarItem.show();
  }

  public async promptUserToSelect(): Promise<void> {
    const current = await this.resolver.resolveActiveAgent();
    const agents = await this.detector.probeAll(true);

    const formatAgentStatus = (id: AgentId) => {
      const info = agents[id];
      if (!info) return '';
      if (info.status === 'online') return '🟢 在线 (Ready)';
      if (info.status === 'exhausted')
        return `⏳ 耗尽 (恢复: ${info.rateLimit?.resetsAtFormatted || '待定'})`;
      if (info.status === 'unauthenticated') return '🟡 未登录';
      return '🔴 离线';
    };

    const items: vscode.QuickPickItem[] = [
      {
        label: `$(sync) 自动选路 / Auto-Detect (当前: ${current.agent})`,
        description: current.reason,
        detail: '根据当前 IDE 宿主、扩展活跃度和会话文件自动选路',
      },
      {
        label: '$(symbol-event) Google Antigravity (agy)',
        description: formatAgentStatus('antigravity'),
        detail: `状态: ${agents.antigravity?.statusDetails || ''}`,
      },
      {
        label: '$(symbol-event) OpenAI Codex (codex)',
        description: formatAgentStatus('codex'),
        detail: `状态: ${agents.codex?.statusDetails || ''}`,
      },
      {
        label: '$(symbol-event) Anthropic Claude Code (claude)',
        description: formatAgentStatus('claude_code'),
        detail: `状态: ${agents.claude_code?.statusDetails || ''}`,
      },
      {
        label: '$(refresh) 刷新所有 Agent 节点状态 (Refresh Status)',
        description: '重新探测各 Agent 可执行文件、登录态与限流恢复时间',
      },
    ];

    const pick = await vscode.window.showQuickPick(items, {
      placeHolder: '选择目标 Agent 或刷新节点状态',
    });

    if (!pick) return;

    if (pick.label.includes('刷新所有 Agent')) {
      await this.detector.probeAll(true);
      await this.update();
      vscode.window.showInformationMessage('Agent Mesh Bridge: 节点状态探测已刷新。');
      return;
    }

    if (pick.label.includes('自动选路')) {
      this.resolver.setManualOverride(null);
      vscode.window.showInformationMessage('Agent Mesh Bridge: 已切换为自动选路模式。');
    } else if (pick.label.includes('Google Antigravity')) {
      this.resolver.setManualOverride('antigravity');
      vscode.window.showInformationMessage('Agent Mesh Bridge: 目标已锁定为 Google Antigravity。');
    } else if (pick.label.includes('OpenAI Codex')) {
      this.resolver.setManualOverride('codex');
      vscode.window.showInformationMessage('Agent Mesh Bridge: 目标已锁定为 OpenAI Codex。');
    } else if (pick.label.includes('Anthropic Claude Code')) {
      this.resolver.setManualOverride('claude_code');
      vscode.window.showInformationMessage(
        'Agent Mesh Bridge: 目标已锁定为 Anthropic Claude Code。',
      );
    }

    await this.update();
  }

  public dispose(): void {
    this.statusBarItem.dispose();
  }
}
