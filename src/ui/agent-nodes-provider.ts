import * as vscode from 'vscode';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AgentId, AgentInfo, AgentStatusType } from '../bridge/types';

export class AgentNodesProvider implements vscode.TreeDataProvider<AgentNodeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<AgentNodeItem | undefined | void> =
    new vscode.EventEmitter<AgentNodeItem | undefined | void>();
  readonly onDidChangeTreeData: vscode.Event<AgentNodeItem | undefined | void> =
    this._onDidChangeTreeData.event;

  private detector: AgentDetector;
  private resolver: ActiveAgentResolver;
  private cachedAgents: Record<AgentId, AgentInfo> | null = null;

  constructor(detector: AgentDetector, resolver: ActiveAgentResolver) {
    this.detector = detector;
    this.resolver = resolver;
  }

  public async refresh(forceProbe: boolean = true): Promise<void> {
    if (forceProbe) {
      this.cachedAgents = await this.detector.probeAll(true);
    }
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: AgentNodeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: AgentNodeItem): Promise<AgentNodeItem[]> {
    if (element) {
      return element.subItems || [];
    }

    if (!this.cachedAgents) {
      this.cachedAgents = await this.detector.probeAll(false);
    }

    const activeResolution = await this.resolver.resolveActiveAgent();
    const agentsList: AgentId[] = ['antigravity', 'codex', 'claude_code'];

    const items: AgentNodeItem[] = [];
    for (const agentId of agentsList) {
      const info = this.cachedAgents[agentId] || (await this.detector.probeAgent(agentId));
      const isActive = activeResolution.agent === agentId;
      items.push(this.createAgentNodeItem(info, isActive));
    }

    return items;
  }

  private createAgentNodeItem(info: AgentInfo, isActive: boolean): AgentNodeItem {
    const activeBadge = isActive ? ' [当前活跃]' : '';
    let statusText = '';
    let iconName = 'circle-filled';
    let iconColor: vscode.ThemeColor | undefined = undefined;

    switch (info.status) {
      case 'online':
        statusText = `🟢 在线 · ${info.version || '就绪'}`;
        iconName = 'pass-filled';
        iconColor = new vscode.ThemeColor('testing.iconPassed');
        break;
      case 'exhausted': {
        const resetInfo = info.rateLimit?.resetsAtFormatted
          ? ` · 恢复: ${info.rateLimit.resetsAtFormatted}`
          : '';
        statusText = `⏳ 耗尽${resetInfo}`;
        iconName = 'warning';
        iconColor = new vscode.ThemeColor('problemsWarningIcon.foreground');
        break;
      }
      case 'unauthenticated':
        statusText = `🟡 未登录 (${info.authDetails || '缺失凭证'})`;
        iconName = 'key';
        iconColor = new vscode.ThemeColor('charts.yellow');
        break;
      case 'offline':
        statusText = `🔴 离线 (未检测到可执行文件)`;
        iconName = 'error';
        iconColor = new vscode.ThemeColor('testing.iconFailed');
        break;
    }

    const item = new AgentNodeItem(
      `${info.name}${activeBadge}`,
      vscode.TreeItemCollapsibleState.Expanded,
      info,
    );

    item.description = statusText;
    const themeIcon = new vscode.ThemeIcon(iconName, iconColor);
    item.iconPath = themeIcon;
    item.contextValue = `agent_node_${info.status}`;

    // Sub-items
    const subItems: AgentNodeItem[] = [];

    // 1. Status detail
    const statusSub = new AgentNodeItem(
      `状态: ${this.translateStatus(info.status)}`,
      vscode.TreeItemCollapsibleState.None,
    );
    statusSub.description = info.statusDetails || '';
    statusSub.iconPath = new vscode.ThemeIcon('pulse');
    subItems.push(statusSub);

    // 2. Rate limit / Exhaustion recovery time (if exhausted or has rateLimit info)
    if (info.rateLimit && (info.status === 'exhausted' || info.rateLimit.isExhausted)) {
      const resetSub = new AgentNodeItem(
        `恢复时间: ${info.rateLimit.resetsAtFormatted || '计算中...'}`,
        vscode.TreeItemCollapsibleState.None,
      );
      resetSub.description = info.rateLimit.reason || '配额耗尽 / 限流中';
      resetSub.iconPath = new vscode.ThemeIcon('watch');
      subItems.push(resetSub);
    }

    // 3. Auth Details
    const authSub = new AgentNodeItem(
      `认证: ${info.authDetails || '未配置'}`,
      vscode.TreeItemCollapsibleState.None,
    );
    authSub.iconPath = new vscode.ThemeIcon('shield');
    subItems.push(authSub);

    // 4. Executable path
    if (info.executablePath) {
      const pathSub = new AgentNodeItem(
        `路径: ${info.executablePath}`,
        vscode.TreeItemCollapsibleState.None,
      );
      pathSub.iconPath = new vscode.ThemeIcon('file-code');
      subItems.push(pathSub);
    }

    // 5. Version
    if (info.version) {
      const verSub = new AgentNodeItem(
        `版本: ${info.version}`,
        vscode.TreeItemCollapsibleState.None,
      );
      verSub.iconPath = new vscode.ThemeIcon('tag');
      subItems.push(verSub);
    }

    item.subItems = subItems;
    return item;
  }

  private translateStatus(status: AgentStatusType): string {
    switch (status) {
      case 'online':
        return '在线 (Online)';
      case 'exhausted':
        return '耗尽 (Rate Limit Exhausted)';
      case 'unauthenticated':
        return '未登录 (Unauthenticated)';
      case 'offline':
        return '离线 (Offline)';
    }
  }
}

export class AgentNodeItem extends vscode.TreeItem {
  public subItems?: AgentNodeItem[];
  public info?: AgentInfo;

  constructor(label: string, collapsibleState: vscode.TreeItemCollapsibleState, info?: AgentInfo) {
    super(label, collapsibleState);
    this.info = info;
  }
}
