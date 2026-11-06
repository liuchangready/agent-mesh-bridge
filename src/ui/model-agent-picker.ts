import * as vscode from 'vscode';
import { AgentId, GroupModelRef } from '../bridge/types';
import { assertGroupModelReady } from '../bridge/agent-capabilities';
import { ModelRole } from '../bridge/model-catalog';
import { CopilotModelGroup } from '../copilot/model-groups';
import { CopilotModelService } from '../copilot/model-service';

export interface ModelAgentChoice extends vscode.QuickPickItem {
  agent: AgentId;
  group?: CopilotModelGroup;
  action?: 'configure-group';
  hostVendor?: string;
}

export async function pickGroupReceiverModel(
  service: CopilotModelService,
  group: CopilotModelGroup,
  current?: GroupModelRef,
): Promise<{ modelRef?: GroupModelRef } | undefined> {
  if (!service.getAgentBinding(group.id)?.receiverReady) {
    await vscode.commands.executeCommand('agent-mesh-bridge.configureCopilotGroupAgent', group, 'receiver');
  }
  const binding = service.getAgentBinding(group.id);
  if (!binding?.enabled || !binding.toolsAuthorized || !binding.receiverReady || !binding.receiverModel) return;
  const same = (left: GroupModelRef, right: GroupModelRef) => left.source === right.source && left.id === right.id &&
    (left.source === 'vscode-lm' ? right.source === 'vscode-lm' && left.vendor === right.vendor :
      right.source === 'openai-compatible' && left.origin === right.origin && left.bindingRevision === right.bindingRevision);
  const models = [binding.receiverModel, ...(binding.models || []).filter(ref => !same(ref, binding.receiverModel!))];
  const choices: Array<vscode.QuickPickItem & { modelRef?: GroupModelRef }> = models.map(ref => ({
    label: ref.id,
    description: `${ref.source === 'vscode-lm' ? ref.vendor : ref.origin}${same(ref, binding.receiverModel!) ? ' · 配置默认' : ''}`,
    detail: '选择后检查工具能力；保留该分组的默认模型配置',
    modelRef: ref,
  }));
  if (current) choices.unshift({ label: `保持当前模型 ${current.id}`, description: 'keep' });
  const selected = await vscode.window.showQuickPick(choices, { placeHolder: `为 ${group.name} 选择接收模型`, matchOnDescription: true });
  if (!selected) return;
  if (!selected.modelRef) return {};
  const modelRef = selected.modelRef;
  try { assertGroupModelReady(binding.id, 'receiver', modelRef); }
  catch {
    await vscode.window.withProgress({ location: vscode.ProgressLocation.Notification, title: `检查 ${group.name} · ${modelRef.id} 的工具能力` },
      (_progress, token) => service.probeToolCalling(group.id,
        modelRef.source === 'vscode-lm' ? { id: modelRef.id, vendor: modelRef.vendor } : modelRef.id, token, 'receiver'));
    assertGroupModelReady(binding.id, 'receiver', modelRef);
  }
  return { modelRef };
}

export async function pickModelAgent(
  service: CopilotModelService,
  cliChoices: ModelAgentChoice[],
  role: ModelRole,
  placeHolder: string,
): Promise<ModelAgentChoice | undefined> {
  if (service.getHostState() === 'not-refreshed') await service.refreshHostModels();
  const savedGroups = service.groups.getAll();
  const groups = savedGroups.map((group): ModelAgentChoice => {
    const binding = service.getAgentBinding(group.id);
    const model = role === 'initiator' ? binding?.initiatorModel : binding?.receiverModel;
    return {
      label: `$(library) ${group.name}`,
      description: `Copilot 分组 · ${model ? model.source === 'vscode-lm' ? `${model.vendor} / ${model.id}` : model.id : '未配置模型'}`,
      detail: `${group.source === 'vscode-lm' ? 'VS Code 宿主模型' : '自定义 API'} · ${group.models.length} 个成员 · ${binding?.enabled ? '已启用' : '选择后可配置并授权'}`,
      agent: service.getGroupAgentId(group.id),
      group,
    };
  });
  const hostGroups: ModelAgentChoice[] = [...new Set(service.getHostModels().map(model => model.vendor))]
    .filter(vendor => vendor && !savedGroups.some(group => group.source === 'vscode-lm' && group.vendor === vendor && group.models.length))
    .map(vendor => ({
      label: `$(library) ${vendor}`,
      description: 'VS Code 已发现的模型分组 · 直接选择模型',
      agent: 'vscode_chat', hostVendor: vendor,
    }));
  const selected = await vscode.window.showQuickPick([...cliChoices, ...groups, ...hostGroups, {
    label: '$(add) 添加 / 配置 Copilot 模型分组',
    description: '选择宿主模型或自定义 API；先配置分组，再用于任务',
    agent: 'vscode_chat' as const,
    action: 'configure-group' as const,
  }], {
    placeHolder,
    matchOnDescription: true,
    matchOnDetail: true,
  });
  if (selected?.action === 'configure-group') {
    await vscode.commands.executeCommand('agent-mesh-bridge.configureCopilotGroupAgent', undefined, role);
    return;
  }
  const vendor = selected?.hostVendor || (selected?.group?.source === 'vscode-lm' && !selected.group.models.length ? selected.group.vendor : undefined);
  if (selected && vendor) {
    const group = await service.bindHostModelGroup(vendor);
    return { ...selected, agent: service.getGroupAgentId(group.id), group };
  }
  return selected;
}
