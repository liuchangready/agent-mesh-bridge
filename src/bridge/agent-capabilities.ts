import { AgentId, CliAgentId, GroupModelRef } from './types';
import { isGroupAgentId, loadGroupAgentConfig, loadGroupAgentHost } from './group-agent-registry';

export const EXECUTABLE_AGENTS = [
  'antigravity',
  'codex',
  'claude_code',
  'deepseek_harness',
] as const;
export type ExecutableAgentId = (typeof EXECUTABLE_AGENTS)[number];
export type AgentTaskRole = 'receiver' | 'initiator';

export function assertGroupModelReady(agent: AgentId, role: AgentTaskRole, modelRef: GroupModelRef): void {
  if (!isGroupAgentId(agent)) throw new Error(`Structured modelRef is only valid for a Group Agent, not ${agent}.`);
  const binding = loadGroupAgentConfig()?.agents.find((entry) => entry.id === agent);
  const host = loadGroupAgentHost();
  const same = (left: GroupModelRef, right: GroupModelRef) => left.source === right.source && left.id === right.id &&
    (left.source === 'vscode-lm' ? right.source === 'vscode-lm' && left.vendor === right.vendor :
      right.source === 'openai-compatible' && left.origin === right.origin && left.bindingRevision === right.bindingRevision);
  const member = binding?.models?.some((model) => same(model, modelRef));
  const probed = binding?.modelProbes?.some((probe) => probe.role === role && probe.revision === binding.revision &&
    probe.hostGeneration === host?.generation && same(probe.modelRef, modelRef));
  if (!binding?.enabled || !binding.toolsAuthorized || !member || !probed || !host)
    throw new Error(`Group Agent ${agent} model ${modelRef.source === 'vscode-lm' ? `${modelRef.vendor}/${modelRef.id}` : `${modelRef.id} (${modelRef.origin})`} has no current ${role} structured-tool probe and authorization. Select it for that role and complete the explicit capability check first.`);
}

export function canExecute(agent: AgentId): agent is ExecutableAgentId {
  return (EXECUTABLE_AGENTS as readonly string[]).includes(agent);
}

export function assertCanExecute(agent: AgentId, action = 'execute tasks', role: AgentTaskRole = 'receiver'): asserts agent is ExecutableAgentId {
  if (canExecute(agent)) return;
  if (isGroupAgentId(agent)) {
    const config = loadGroupAgentConfig();
    const binding = config?.agents.find((entry) => entry.id === agent);
    const host = loadGroupAgentHost();
    const initiator = role === 'initiator';
    const modelReady = initiator ? binding?.initiatorModel : binding?.receiverModel;
    const roleReady = initiator ? binding?.initiatorReady : binding?.receiverReady;
    if (binding?.enabled && binding.toolsAuthorized && modelReady && roleReady && host) return;
    throw new Error(
      `Copilot Group Agent ${agent} is not ready to ${action}: enable and authorize the group, configure a role-specific model, and keep its trusted local VS Code host online.`,
    );
  }
  if (!(agent as string) || !(EXECUTABLE_AGENTS as readonly string[]).includes(agent)) {
    throw new Error(`${agent} is requester-only in this release and cannot ${action}.`);
  }
}
