import * as vscode from 'vscode';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AgentId } from '../bridge/types';

export class StatusBarController {
  private statusBarItem: vscode.StatusBarItem;
  private resolver: ActiveAgentResolver;

  constructor(resolver: ActiveAgentResolver) {
    this.resolver = resolver;
    this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 100);
    this.statusBarItem.command = 'agent-mesh-bridge.selectActiveAgent';
    this.statusBarItem.tooltip = 'Click to switch Agent Mesh Bridge target agent';
    this.update();
  }

  public getStatusBarItem(): vscode.StatusBarItem {
    return this.statusBarItem;
  }

  public async update(): Promise<void> {
    const resolved = await this.resolver.resolveActiveAgent();
    const agentLabels: Record<AgentId, string> = {
      antigravity: 'Antigravity',
      claude_code: 'Claude Code',
      codex: 'Codex',
      vscode_chat: 'VSCode Chat',
    };

    const label = agentLabels[resolved.agent] || resolved.agent;
    const modeTag = resolved.mode === 'manual' ? 'Manual' : 'Auto';
    this.statusBarItem.text = `$(hubot) Agent Bridge: ${label} (${modeTag})`;
    this.statusBarItem.show();
  }

  public async promptUserToSelect(): Promise<void> {
    const current = await this.resolver.resolveActiveAgent();
    const items: vscode.QuickPickItem[] = [
      {
        label: `$(sync) Auto-Detect (Current: ${current.agent})`,
        description: current.reason,
        detail:
          'Automatically detects agent based on IDE host, active extension, and recent sessions.',
      },
      {
        label: '$(symbol-event) Google Antigravity (agy)',
        description: 'Force route tasks to Antigravity',
      },
      {
        label: '$(symbol-event) OpenAI Codex (codex)',
        description: 'Force route tasks to OpenAI Codex',
      },
      {
        label: '$(symbol-event) Anthropic Claude Code (claude)',
        description: 'Force route tasks to Claude Code',
      },
    ];

    const pick = await vscode.window.showQuickPick(items, {
      placeHolder: 'Select active target agent for delegation bridge',
    });

    if (!pick) return;

    if (pick.label.includes('Auto-Detect')) {
      this.resolver.setManualOverride(null);
      vscode.window.showInformationMessage('Agent Mesh Bridge: Set to Auto-Detect mode.');
    } else if (pick.label.includes('Antigravity')) {
      this.resolver.setManualOverride('antigravity');
      vscode.window.showInformationMessage(
        'Agent Mesh Bridge: Forced target to Google Antigravity.',
      );
    } else if (pick.label.includes('OpenAI Codex')) {
      this.resolver.setManualOverride('codex');
      vscode.window.showInformationMessage('Agent Mesh Bridge: Forced target to OpenAI Codex.');
    } else if (pick.label.includes('Claude Code')) {
      this.resolver.setManualOverride('claude_code');
      vscode.window.showInformationMessage('Agent Mesh Bridge: Forced target to Claude Code.');
    }

    await this.update();
  }

  public dispose(): void {
    this.statusBarItem.dispose();
  }
}
