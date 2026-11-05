import * as fs from 'fs';
import * as path from 'path';
import { getBridgeHome } from './model-catalog';
import { GroupModelRef } from './types';

export const GROUP_AGENT_CONFIG_FILENAME = 'group-agent-config.json';
export const GROUP_AGENT_HOST_FILENAME = 'group-agent-host.json';
const MAX_CONFIG_BYTES = 256 * 1024;

export interface GroupAgentBinding {
  id: `copilot_group:${string}`;
  groupId: string;
  name: string;
  source: 'vscode-lm' | 'openai-compatible';
  baseUrl?: string;
  enabled: boolean;
  toolsAuthorized: boolean;
  receiverReady: boolean;
  initiatorReady: boolean;
  revision: number;
  receiverModel?: GroupModelRef;
  initiatorModel?: GroupModelRef;
}

export interface GroupAgentConfig {
  version: 1;
  updatedAt: number;
  defaultAgent?: string;
  defaultReviewer?: string;
  agents: GroupAgentBinding[];
}

export interface GroupAgentHost {
  version: 1;
  hostInstanceId: string;
  generation: number;
  pid: number;
  workspaceUri: string;
  repositoryCommonDir: string;
  trusted: true;
  localDesktop: true;
  updatedAt: number;
}

export function isGroupAgentId(value: unknown): value is `copilot_group:${string}` {
  return typeof value === 'string' && /^copilot_group:[0-9a-f-]{36}$/i.test(value);
}

export function loadGroupAgentConfig(dir = getBridgeHome()): GroupAgentConfig | undefined {
  try {
    const file = path.join(dir, GROUP_AGENT_CONFIG_FILENAME);
    const stat = fs.statSync(file);
    if (stat.size > MAX_CONFIG_BYTES) return undefined;
    const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as GroupAgentConfig;
    if (parsed?.version !== 1 || !Array.isArray(parsed.agents) || parsed.agents.length > 50)
      return undefined;
    const ids = new Set<string>();
    for (const agent of parsed.agents) {
      if (
        !isGroupAgentId(agent?.id) ||
        agent.id !== `copilot_group:${agent.groupId}` ||
        typeof agent.name !== 'string' ||
        !['vscode-lm', 'openai-compatible'].includes(agent.source) ||
        !Number.isInteger(agent.revision) ||
        typeof agent.receiverReady !== 'boolean' || typeof agent.initiatorReady !== 'boolean' ||
        ids.has(agent.id)
      ) return undefined;
      ids.add(agent.id);
    }
    return parsed;
  } catch {
    return undefined;
  }
}

export function loadGroupAgentHost(dir = getBridgeHome()): GroupAgentHost | undefined {
  try {
    const parsed = JSON.parse(
      fs.readFileSync(path.join(dir, GROUP_AGENT_HOST_FILENAME), 'utf8'),
    ) as GroupAgentHost;
    if (
      parsed?.version !== 1 || !parsed.hostInstanceId || !Number.isInteger(parsed.generation) ||
      !Number.isInteger(parsed.pid) || !parsed.workspaceUri || !parsed.repositoryCommonDir ||
      parsed.trusted !== true || parsed.localDesktop !== true ||
      Date.now() - parsed.updatedAt > 15_000
    ) return undefined;
    return parsed;
  } catch {
    return undefined;
  }
}

export function saveGroupAgentConfig(config: GroupAgentConfig, dir = getBridgeHome()): void {
  const file = path.join(dir, GROUP_AGENT_CONFIG_FILENAME);
  fs.mkdirSync(dir, { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(config, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 });
  fs.renameSync(temporary, file);
}

export function writeGroupAgentHost(host: GroupAgentHost, dir = getBridgeHome()): void {
  const file = path.join(dir, GROUP_AGENT_HOST_FILENAME);
  fs.mkdirSync(dir, { recursive: true });
  const temporary = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temporary, `${JSON.stringify(host, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 });
  fs.renameSync(temporary, file);
}
