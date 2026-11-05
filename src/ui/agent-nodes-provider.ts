import * as vscode from 'vscode';
import { AgentDetector } from '../bridge/agent-detector';
import { ActiveAgentResolver } from '../bridge/active-agent-resolver';
import { AgentId, AgentInfo, AgentStatusType } from '../bridge/types';
import { resolveConfiguredModel } from '../bridge/model-catalog';
import { CopilotModelService } from '../copilot/model-service';
import { CopilotModelGroup } from '../copilot/model-groups';

export class AgentNodesProvider implements vscode.TreeDataProvider<AgentNodeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<AgentNodeItem | undefined | void> =
    new vscode.EventEmitter<AgentNodeItem | undefined | void>();
  readonly onDidChangeTreeData: vscode.Event<AgentNodeItem | undefined | void> =
    this._onDidChangeTreeData.event;

  private detector: AgentDetector;
  private resolver: ActiveAgentResolver;
  private cachedAgents: Record<AgentId, AgentInfo> | null = null;
  private copilotModels?: CopilotModelService;

  constructor(detector: AgentDetector, resolver: ActiveAgentResolver, copilotModels?: CopilotModelService) {
    this.detector = detector;
    this.resolver = resolver;
    this.copilotModels = copilotModels;
    copilotModels?.onDidChange(() => this._onDidChangeTreeData.fire());
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
    const agentsList: AgentId[] = ['antigravity', 'codex', 'claude_code', 'deepseek_harness', 'vscode_chat'];

    const items: AgentNodeItem[] = [];
    for (const agentId of agentsList) {
      const info = this.cachedAgents[agentId] || (await this.detector.probeAgent(agentId));
      const isActive = activeResolution.agent === agentId;
      items.push(info.id === 'vscode_chat' && this.copilotModels
        ? await this.createCopilotNodeItem(info, isActive)
        : this.createAgentNodeItem(info, isActive));
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

    if (info.id === 'vscode_chat') {
      statusText = 'Requester only · Copilot login/model unknown';
      iconName = 'info';
      iconColor = new vscode.ThemeColor('descriptionForeground');
    }
    const item = new AgentNodeItem(
      `${info.name}${activeBadge}`,
      vscode.TreeItemCollapsibleState.Expanded,
      info,
    );

    item.description = info.id === 'vscode_chat' ? 'Requester only · Copilot login/model unknown' : statusText;
    const themeIcon = new vscode.ThemeIcon(iconName, iconColor);
    item.iconPath = themeIcon;
    item.contextValue = info.id === 'vscode_chat' ? 'agent_node_requester_only' : `agent_node_${info.status}`;

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

    // 1b. Configured models for this agent (click either side to change it).
    //     Receiver and sender defaults are independent: the same agent may run
    //     on one model while executing delegated work and another while
    //     dispatching/reviewing (notably when it sends a task to itself).
    if (info.id !== 'vscode_chat') {
    const receiverModel = resolveConfiguredModel(info.id, undefined, 'receiver');
    const receiverSub = new AgentNodeItem(
      `接收模型: ${receiverModel || 'Agent 默认'}`,
      vscode.TreeItemCollapsibleState.None,
    );
    receiverSub.description = '被派发任务时执行使用 · 点击选择';
    receiverSub.iconPath = new vscode.ThemeIcon('arrow-down');
    receiverSub.contextValue = 'agent_model';
    receiverSub.command = {
      command: 'agent-mesh-bridge.selectAgentModel',
      title: 'Select Receiver Model',
      arguments: [item, 'receiver'],
    };
    subItems.push(receiverSub);

    const initiatorModel = resolveConfiguredModel(info.id, undefined, 'initiator');
    const initiatorSub = new AgentNodeItem(
      `下发模型: ${initiatorModel || 'Agent 默认'}`,
      vscode.TreeItemCollapsibleState.None,
    );
    initiatorSub.description = '作为下发者/检视者运行时使用 · 点击选择';
    initiatorSub.iconPath = new vscode.ThemeIcon('arrow-up');
    initiatorSub.contextValue = 'agent_model';
    initiatorSub.command = {
      command: 'agent-mesh-bridge.selectAgentModel',
      title: 'Select Sender Model',
      arguments: [item, 'initiator'],
    };
    subItems.push(initiatorSub);
    } else {
      const capability = new AgentNodeItem('Requester only · not an executor or reviewer', vscode.TreeItemCollapsibleState.None);
      capability.description = 'GitHub Copilot sign-in and current model are unknown to the Bridge.';
      capability.iconPath = new vscode.ThemeIcon('info');
      subItems.push(capability);
    }

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
    if (info.id === 'deepseek_harness') {
      authSub.description = '点击配置 URL 与 API 密钥';
      authSub.command = {
        command: 'agent-mesh-bridge.configureDeepSeekHarness',
        title: 'Configure DeepSeek Harness',
      };
    }
    subItems.push(authSub);

    // 3b. Endpoint for DeepSeek Harness
    if (info.id === 'deepseek_harness') {
      const dshCfg = vscode.workspace.getConfiguration('agentMeshBridge.deepseekHarness');
      const currentUrl = (dshCfg.get<string>('baseUrl') || '').trim();
      const endpointSub = new AgentNodeItem(
        `接口: ${currentUrl || '官方默认 (https://api.deepseek.com)'}`,
        vscode.TreeItemCollapsibleState.None,
      );
      endpointSub.description = '点击修改 URL 与密钥';
      endpointSub.iconPath = new vscode.ThemeIcon('link');
      endpointSub.command = {
        command: 'agent-mesh-bridge.configureDeepSeekHarness',
        title: 'Configure DeepSeek Harness',
      };
      subItems.push(endpointSub);
    }

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

  private async createCopilotNodeItem(info: AgentInfo, isActive: boolean): Promise<AgentNodeItem> {
    const config = vscode.workspace.getConfiguration('agentMeshBridge.copilot');
    const enabled = config.get<boolean>('enabled', false);
    const reviewer = config.get<string>('reviewer', '') || 'not selected';
    const activeBadge = isActive ? ' [active]' : '';
    const item = new AgentNodeItem(`${info.name}${activeBadge}`, vscode.TreeItemCollapsibleState.Expanded, info);
    item.description = `Requester: ${enabled ? 'enabled' : 'disabled'} · model access: unknown`;
    item.iconPath = new vscode.ThemeIcon('hubot', new vscode.ThemeColor('descriptionForeground'));
    item.contextValue = 'agent_node_copilot';
    const children: AgentNodeItem[] = [];
    const requester = new AgentNodeItem(`Requester: ${enabled ? 'enabled' : 'disabled'}`, vscode.TreeItemCollapsibleState.None);
    requester.description = `Reviewer: ${reviewer} · this is independent of model calls`;
    requester.iconPath = new vscode.ThemeIcon('comment-discussion');
    children.push(requester);
    const catalogState = this.copilotModels!.getHostState();
    const catalog = new AgentNodeItem(`Host model catalog: ${catalogState}`, vscode.TreeItemCollapsibleState.None);
    catalog.description = catalogState === 'available' ? `${this.copilotModels!.getHostModels().length} models` : catalogState === 'not-refreshed' ? 'Refresh to ask VS Code for available models' : '';
    catalog.iconPath = new vscode.ThemeIcon('library');
    children.push(catalog);
    const auth = new AgentNodeItem('Copilot login and quota: unknown', vscode.TreeItemCollapsibleState.None);
    auth.description = 'Bridge has no reliable independent signal for native Copilot login or quota.';
    auth.iconPath = new vscode.ThemeIcon('question');
    children.push(auth);
    const lastCall = new AgentNodeItem(`Last model call: ${this.copilotModels!.getLastCall()}`, vscode.TreeItemCollapsibleState.None);
    lastCall.iconPath = new vscode.ThemeIcon('history');
    children.push(lastCall);

    if (catalogState === 'available') {
      const byVendor = new Map<string, vscode.LanguageModelChat[]>();
      for (const model of this.copilotModels!.getHostModels()) {
        const vendor = model.vendor || 'unknown';
        byVendor.set(vendor, [...(byVendor.get(vendor) || []), model]);
      }
      for (const [vendor, models] of byVendor) {
        const vendorNode = new AgentNodeItem(`Host models · ${vendor}`, vscode.TreeItemCollapsibleState.Collapsed);
        vendorNode.subItems = models.map((model) => {
          const child = new AgentNodeItem(model.name || model.id, vscode.TreeItemCollapsibleState.None);
          child.description = `${vendor} · ${model.id}`;
          child.iconPath = new vscode.ThemeIcon('symbol-misc');
          return child;
        });
        children.push(vendorNode);
      }
    }

    let groups: CopilotModelGroup[];
    try { groups = this.copilotModels!.groups.getAll(); }
    catch (error) {
      const invalid = new AgentNodeItem('Custom groups: unavailable', vscode.TreeItemCollapsibleState.None);
      invalid.description = error instanceof Error ? error.message : 'Stored group data is invalid; existing data was preserved.';
      invalid.iconPath = new vscode.ThemeIcon('warning');
      children.push(invalid);
      groups = [];
    }
    for (const group of groups) {
      const groupNode = new AgentNodeItem(group.name, vscode.TreeItemCollapsibleState.Collapsed);
      const keyStatus = group.source === 'openai-compatible' ? await this.copilotModels!.getKeyStatus(group) : undefined;
      groupNode.description = group.source === 'vscode-lm'
        ? `VS Code models · ${group.models.length} bound${group.vendor ? ` · ${group.vendor}` : ''}`
        : `Custom API · ${group.models.length} models · key ${keyStatus}`;
      groupNode.iconPath = new vscode.ThemeIcon(group.source === 'vscode-lm' ? 'library' : 'cloud');
      groupNode.contextValue = 'copilot_model_group';
      groupNode.command = { command: 'agent-mesh-bridge.manageCopilotModelGroup', title: 'Manage Model Group', arguments: [group] };
      if (group.source === 'vscode-lm') {
        groupNode.subItems = group.models.map((ref) => {
          const child = new AgentNodeItem(ref.id, vscode.TreeItemCollapsibleState.None);
          child.description = ref.vendor;
          child.contextValue = 'copilot_model';
          child.iconPath = new vscode.ThemeIcon('symbol-misc');
          child.command = { command: 'agent-mesh-bridge.callCopilotModel', title: 'Call Model', arguments: [group, ref.id] };
          return child;
        });
      } else {
        groupNode.subItems = group.models.map((model) => {
          const child = new AgentNodeItem(model.name || model.id, vscode.TreeItemCollapsibleState.None);
          child.description = model.id;
          child.contextValue = 'copilot_model';
          child.iconPath = new vscode.ThemeIcon('symbol-misc');
          child.command = { command: 'agent-mesh-bridge.callCopilotModel', title: 'Call Model', arguments: [group, model.id] };
          return child;
        });
      }
      children.push(groupNode);
    }
    const actions = [
      ['Refresh host model catalog', 'agent-mesh-bridge.refreshCopilotModels', 'refresh'],
      ['Add model group', 'agent-mesh-bridge.addCopilotModelGroup', 'add'],
      ['Import group names from JSONC', 'agent-mesh-bridge.importCopilotModelGroups', 'symbol-file'],
    ] as const;
    for (const [label, command, icon] of actions) {
      const child = new AgentNodeItem(label, vscode.TreeItemCollapsibleState.None);
      child.iconPath = new vscode.ThemeIcon(icon);
      child.command = { command, title: label };
      children.push(child);
    }
    item.subItems = children;
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
