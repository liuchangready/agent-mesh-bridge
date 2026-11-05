import { AgentId } from './types';

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
  if (!canExecute(agent)) {
    throw new Error(`${agent} is requester-only in this release and cannot ${action}.`);
  }
}
