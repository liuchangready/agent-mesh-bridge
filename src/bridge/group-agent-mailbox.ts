import * as fs from 'fs';
import * as path from 'path';
import { randomUUID } from 'crypto';
import { getBridgeHome } from './model-catalog';
import { GroupAgentHost, isGroupAgentId, loadGroupAgentHost } from './group-agent-registry';
import { GroupAgentBindingSnapshot, GroupModelRef, TokenUsageMetrics } from './types';

const MAX_REQUEST_BYTES = 128 * 1024;
const MAX_RESPONSE_BYTES = 1024 * 1024;
const ROOT_NAME = 'group-agent-mailbox';

export interface GroupAgentMailboxRequest {
  version: 1;
  runId: string;
  threadId: string;
  round: number;
  role: 'developer' | 'reviewer' | 'bootstrap';
  agent: `copilot_group:${string}`;
  sessionId: string;
  worktree: string;
  prompt: string;
  modelRef: GroupModelRef;
  bindingSnapshot: GroupAgentBindingSnapshot;
  hostInstanceId: string;
  hostGeneration: number;
  isRework: boolean;
  timeoutMs: number;
  extraEnv?: Record<string, string>;
  previousTokenUsage?: TokenUsageMetrics;
  createdAt: number;
}

export interface GroupAgentMailboxResult {
  text: string;
  toolCallsCompleted: number;
  modelRequests?: number;
  tokenUsage?: TokenUsageMetrics;
}

function directories(root = getBridgeHome()) {
  const base = path.join(root, ROOT_NAME);
  return { base, requests: path.join(base, 'requests'), claimed: path.join(base, 'claimed'), responses: path.join(base, 'responses'), cancel: path.join(base, 'cancel'), pids: path.join(base, 'pids') };
}

function ensure(root?: string) {
  const dirs = directories(root);
  for (const dir of Object.values(dirs)) fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  return dirs;
}

function atomicWrite(file: string, value: unknown, exclusive = false): void {
  const raw = `${JSON.stringify(value)}\n`;
  const temp = `${file}.${process.pid}.${randomUUID()}.tmp`;
  fs.writeFileSync(temp, raw, { encoding: 'utf8', mode: 0o600, flag: 'wx' });
  try {
    if (exclusive && fs.existsSync(file)) throw new Error('Mailbox record already exists.');
    fs.renameSync(temp, file);
  } catch (error) {
    try { fs.unlinkSync(temp); } catch {}
    throw error;
  }
}

function readJson<T>(file: string, limit: number): T {
  const stat = fs.statSync(file);
  if (!stat.isFile() || stat.size > limit) throw new Error('Mailbox record exceeds its size limit.');
  return JSON.parse(fs.readFileSync(file, 'utf8')) as T;
}

function safeRunId(value: string): string {
  if (!/^[0-9a-f-]{36}$/i.test(value)) throw new Error('Invalid Group Agent run id.');
  return value;
}

function validModelRef(value: unknown): value is GroupModelRef {
  if (!value || typeof value !== 'object') return false;
  const ref = value as Record<string, unknown>;
  return ref.source === 'vscode-lm'
    ? typeof ref.vendor === 'string' && ref.vendor.length > 0 && ref.vendor.length <= 100 && typeof ref.id === 'string' && ref.id.length > 0 && ref.id.length <= 256
    : ref.source === 'openai-compatible' && typeof ref.id === 'string' && ref.id.length > 0 && ref.id.length <= 256 && typeof ref.origin === 'string' && ref.origin.length <= 2048 && Number.isInteger(ref.bindingRevision);
}

function validResult(value: unknown): value is GroupAgentMailboxResult {
  if (!value || typeof value !== 'object') return false;
  const result = value as GroupAgentMailboxResult;
  if (typeof result.text !== 'string' || Buffer.byteLength(result.text) > MAX_RESPONSE_BYTES || !Number.isInteger(result.toolCallsCompleted) || result.toolCallsCompleted < 0 || result.toolCallsCompleted > 10_000) return false;
  if (result.modelRequests !== undefined && (!Number.isInteger(result.modelRequests) || result.modelRequests < 1 || result.modelRequests > 10_000)) return false;
  if (result.tokenUsage !== undefined) {
    const usage = result.tokenUsage;
    const counts = [usage.uncachedInputTokens, usage.cacheReadTokens, usage.cacheWriteTokens, usage.promptTokens, usage.outputTokens, usage.totalTokens];
    if (counts.some((count) => !Number.isSafeInteger(count) || count < 0) || usage.promptTokens !== usage.uncachedInputTokens + usage.cacheReadTokens + usage.cacheWriteTokens || usage.totalTokens !== usage.promptTokens + usage.outputTokens || typeof usage.available !== 'boolean' || typeof usage.partial !== 'boolean' || !['native_reported', 'estimated', 'partial', 'unavailable'].includes(usage.source)) return false;
  }
  return true;
}

export function publishGroupAgentRun(request: Omit<GroupAgentMailboxRequest, 'version' | 'runId' | 'hostInstanceId' | 'hostGeneration' | 'createdAt'>, host: GroupAgentHost, root?: string): string {
  if (!isGroupAgentId(request.agent) || !['developer', 'reviewer', 'bootstrap'].includes(request.role) || !Number.isInteger(request.round) || request.round < (request.role === 'bootstrap' ? 0 : 1) || request.bindingSnapshot?.agentId !== request.agent || !Number.isInteger(request.bindingSnapshot.revision) || !validModelRef(request.modelRef) || JSON.stringify(request.bindingSnapshot.modelRef) !== JSON.stringify(request.modelRef) || request.prompt.length > 32_000 || !path.isAbsolute(request.worktree) || !request.sessionId || request.sessionId.length > 128 || !Number.isFinite(request.timeoutMs) || request.timeoutMs < 1_000 || request.timeoutMs > 24 * 60 * 60 * 1000 || !Number.isInteger(host.generation) || !host.hostInstanceId)
    throw new Error('Invalid Group Agent mailbox request.');
  const runId = randomUUID();
  const dirs = ensure(root);
  const full: GroupAgentMailboxRequest = { version: 1, ...request, runId, hostInstanceId: host.hostInstanceId, hostGeneration: host.generation, createdAt: Date.now() };
  const raw = Buffer.byteLength(JSON.stringify(full));
  if (raw > MAX_REQUEST_BYTES) throw new Error('Group Agent mailbox request exceeds its size limit.');
  atomicWrite(path.join(dirs.requests, `${runId}.json`), full, true);
  return runId;
}

export function claimGroupAgentRuns(host: GroupAgentHost, root?: string): GroupAgentMailboxRequest[] {
  const dirs = ensure(root);
  const claimed: GroupAgentMailboxRequest[] = [];
  for (const name of fs.readdirSync(dirs.requests).filter((entry) => /^[0-9a-f-]{36}\.json$/i.test(entry))) {
    const source = path.join(dirs.requests, name);
    const target = path.join(dirs.claimed, name);
    try { fs.renameSync(source, target); } catch { continue; }
    const runId = name.slice(0, -5);
    try {
      const request = readJson<GroupAgentMailboxRequest>(target, MAX_REQUEST_BYTES);
      if (request.version !== 1 || request.runId !== runId || request.hostInstanceId !== host.hostInstanceId || request.hostGeneration !== host.generation || !isGroupAgentId(request.agent) || !['developer', 'reviewer', 'bootstrap'].includes(request.role) || !Number.isInteger(request.round) || request.round < (request.role === 'bootstrap' ? 0 : 1) || typeof request.prompt !== 'string' || request.prompt.length > 32_000 || !request.sessionId || request.sessionId.length > 128 || !path.isAbsolute(request.worktree) || !Number.isFinite(request.timeoutMs) || request.timeoutMs < 1_000 || request.timeoutMs > 24 * 60 * 60 * 1000 || request.bindingSnapshot?.agentId !== request.agent || !Number.isInteger(request.bindingSnapshot.revision) || !validModelRef(request.modelRef) || JSON.stringify(request.bindingSnapshot.modelRef) !== JSON.stringify(request.modelRef)) {
        publishGroupAgentResult(runId, host.generation, undefined, 'Invalid or stale mailbox request.', root);
        continue;
      }
      claimed.push(request);
    } catch (error) {
      console.warn(`[GroupAgentMailbox] Rejected malformed request ${name}:`, error);
      try { publishGroupAgentResult(runId, host.generation, undefined, 'Malformed mailbox request.', root); } catch {}
    }
  }
  return claimed;
}

export function publishGroupAgentResult(runIdValue: string, hostGeneration: number, result?: GroupAgentMailboxResult, error?: string, root?: string): void {
  const runId = safeRunId(runIdValue);
  const dirs = ensure(root);
  const file = path.join(dirs.responses, `${runId}.json`);
  const record = { version: 1, runId, hostGeneration, sequence: 1, completedAt: Date.now(), ...(result ? { result } : {}), ...(error ? { error: error.slice(0, 4000) } : {}) };
  if (Buffer.byteLength(JSON.stringify(record)) > MAX_RESPONSE_BYTES) throw new Error('Group Agent mailbox response exceeds its size limit.');
  if (!fs.existsSync(file)) atomicWrite(file, record, true);
}

export function publishGroupAgentCancel(runIdValue: string, reason: string, root?: string): void {
  const runId = safeRunId(runIdValue);
  const dirs = ensure(root);
  const file = path.join(dirs.cancel, `${runId}.json`);
  if (!fs.existsSync(file)) atomicWrite(file, { version: 1, runId, reason: reason.slice(0, 1000), at: Date.now() }, true);
}

export function hasGroupAgentCancel(runIdValue: string, root?: string): boolean {
  const runId = safeRunId(runIdValue);
  try {
    const value = readJson<{ version: number; runId: string }>(path.join(ensure(root).cancel, `${runId}.json`), 4096);
    return value.version === 1 && value.runId === runId;
  } catch { return false; }
}

export function publishGroupAgentPid(runIdValue: string, sequence: number, pid: number | undefined, root?: string): void {
  const runId = safeRunId(runIdValue);
  atomicWrite(path.join(ensure(root).pids, `${runId}.json`), { version: 1, runId, sequence, pid: pid || null, at: Date.now() });
}

export class GroupAgentMailboxClient {
  constructor(private readonly root = getBridgeHome()) {}

  async execute(request: Omit<GroupAgentMailboxRequest, 'version' | 'runId' | 'hostInstanceId' | 'hostGeneration' | 'createdAt'>, host: GroupAgentHost, signal: AbortSignal, onChildPid?: (pid: number | undefined) => void): Promise<GroupAgentMailboxResult> {
    const runId = publishGroupAgentRun(request, host, this.root);
    const dirs = ensure(this.root);
    let pidSequence = 0;
    try {
      while (true) {
        if (signal.aborted) publishGroupAgentCancel(runId, String(signal.reason || 'cancelled'), this.root);
        if (loadGroupAgentHost(this.root)?.hostInstanceId !== host.hostInstanceId || loadGroupAgentHost(this.root)?.generation !== host.generation)
          throw new Error('The Group Agent host generation changed; resume this task in its original session.');
        const pidFile = path.join(dirs.pids, `${runId}.json`);
        try {
          const pid = readJson<{ sequence: number; pid: number | null }>(pidFile, 4096);
          if (pid.sequence > pidSequence) { pidSequence = pid.sequence; onChildPid?.(pid.pid || undefined); }
        } catch {}
        const responseFile = path.join(dirs.responses, `${runId}.json`);
        try {
          const response = readJson<{ version: number; runId: string; hostGeneration: number; sequence: number; completedAt: number; result?: GroupAgentMailboxResult; error?: string }>(responseFile, MAX_RESPONSE_BYTES);
          if (response.version !== 1 || response.runId !== runId || response.hostGeneration !== host.generation || response.sequence !== 1 || !Number.isFinite(response.completedAt))
            throw new Error('The Group Agent mailbox response was stale or malformed.');
          if (response.error) throw new Error(response.error);
          if (!validResult(response.result)) throw new Error('The Group Agent mailbox response has no valid result.');
          return response.result;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') { /* wait for host response */ }
          else throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    } finally {
      try { fs.unlinkSync(path.join(dirs.pids, `${runId}.json`)); } catch {}
      try { fs.unlinkSync(path.join(dirs.cancel, `${runId}.json`)); } catch {}
      try { fs.unlinkSync(path.join(dirs.responses, `${runId}.json`)); } catch {}
      try { fs.unlinkSync(path.join(dirs.claimed, `${runId}.json`)); } catch {}
    }
  }
}
