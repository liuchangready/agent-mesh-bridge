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
  const configured = role === 'receiver' ? binding?.receiverModel : binding?.initiatorModel;
  const ready = role === 'receiver' ? binding?.receiverReady : binding?.initiatorReady;
  const same = configured?.source === modelRef.source && configured.id === modelRef.id &&
    (configured.source === 'vscode-lm'
      ? modelRef.source === 'vscode-lm' && configured.vendor === modelRef.vendor
      : modelRef.source === 'openai-compatible' && configured.origin === modelRef.origin && configured.bindingRevision === modelRef.bindingRevision);
  if (!binding?.enabled || !binding.toolsAuthorized || !ready || !same || !loadGroupAgentHost())
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
