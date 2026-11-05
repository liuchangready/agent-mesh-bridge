import { AgentId, CliAgentId } from './types';
import { isGroupAgentId, loadGroupAgentConfig, loadGroupAgentHost } from './group-agent-registry';

export const EXECUTABLE_AGENTS = [
  'antigravity',
  'codex',
  'claude_code',
  'deepseek_harness',
] as const;
export type ExecutableAgentId = (typeof EXECUTABLE_AGENTS)[number];

export function canExecute(agent: AgentId): agent is ExecutableAgentId {
  return (EXECUTABLE_AGENTS as readonly string[]).includes(agent);
}

export function assertCanExecute(agent: AgentId, action = 'execute tasks'): asserts agent is ExecutableAgentId {
  if (canExecute(agent)) return;
  if (isGroupAgentId(agent)) {
    const config = loadGroupAgentConfig();
    const binding = config?.agents.find((entry) => entry.id === agent);
    const host = loadGroupAgentHost();
    const reviewer = /review|initiator|sender/i.test(action);
    const modelReady = reviewer ? binding?.initiatorModel : binding?.receiverModel;
    const roleReady = reviewer ? binding?.initiatorReady : binding?.receiverReady;
    if (binding?.enabled && binding.toolsAuthorized && modelReady && roleReady && host) return;
    throw new Error(
      `Copilot Group Agent ${agent} is not ready to ${action}: enable and authorize the group, configure a role-specific model, and keep its trusted local VS Code host online.`,
    );
  }
  if (!(agent as string) || !(EXECUTABLE_AGENTS as readonly string[]).includes(agent)) {
    throw new Error(`${agent} is requester-only in this release and cannot ${action}.`);
  }
}
