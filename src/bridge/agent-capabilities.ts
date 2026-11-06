import { AgentId } from './types';

const CLI_AGENTS = new Set<string>(['antigravity', 'codex', 'claude_code', 'deepseek_harness']);

export function assertCanExecute(agent: AgentId | string, action: string, _role?: string): asserts agent is AgentId {
  if (!CLI_AGENTS.has(agent)) throw new Error(`Agent "${agent}" is no longer supported and cannot ${action}.`);
}
