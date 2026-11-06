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
  const groups = service.groups.getAll().map((group): ModelAgentChoice => {
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
  const selected = await vscode.window.showQuickPick([...cliChoices, ...groups, {
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
  return selected;
}
