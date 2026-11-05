import * as fs from 'fs';
import * as path from 'path';
import { randomUUID } from 'crypto';
import { getBridgeHome } from './model-catalog';
import { GroupAgentHost, isGroupAgentId, loadGroupAgentHost } from './group-agent-registry';
import { DelegationThread, GroupAgentBindingSnapshot, GroupModelRef, TokenUsageMetrics } from './types';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';

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

export interface GroupAgentMailboxFailure {
  message: string;
  modelRequests?: number;
  tokenUsage?: TokenUsageMetrics;
  interruption?: { kind: 'paused' | 'deleted' | 'timeout' | 'stopped'; reason: string; at: string; by?: string };
  sessionId?: string;
  toolCallsCompleted?: number;
}

export interface GroupAgentMailboxResult {
  text: string;
  toolCallsCompleted: number;
  modelRequests?: number;
  tokenUsage?: TokenUsageMetrics;
}

function directories(root = getBridgeHome()) {
  const base = path.join(root, ROOT_NAME);
  return { base, requests: path.join(base, 'requests'), claimed: path.join(base, 'claimed'), cancelled: path.join(base, 'cancelled'), responses: path.join(base, 'responses'), cancel: path.join(base, 'cancel'), pids: path.join(base, 'pids'), states: path.join(base, 'states') };
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
    if (exclusive) {
      // Linking a fully-written same-directory temp file publishes it atomically and never replaces an existing run record.
      fs.linkSync(temp, file);
      fs.unlinkSync(temp);
    } else fs.renameSync(temp, file);
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

function validFailure(value: unknown): value is GroupAgentMailboxFailure {
  if (!value || typeof value !== 'object') return false;
  const failure = value as GroupAgentMailboxFailure;
  if (typeof failure.message !== 'string' || !failure.message || failure.message.length > 4000) return false;
  if (failure.modelRequests !== undefined && (!Number.isInteger(failure.modelRequests) || failure.modelRequests < 0 || failure.modelRequests > 10_000)) return false;
  if (failure.toolCallsCompleted !== undefined && (!Number.isInteger(failure.toolCallsCompleted) || failure.toolCallsCompleted < 0 || failure.toolCallsCompleted > 10_000)) return false;
  if (failure.tokenUsage !== undefined && !validResult({ text: '', toolCallsCompleted: 0, tokenUsage: failure.tokenUsage })) return false;
  if (failure.interruption !== undefined && (!['paused', 'deleted', 'timeout', 'stopped'].includes(failure.interruption.kind) || typeof failure.interruption.reason !== 'string' || typeof failure.interruption.at !== 'string')) return false;
  return true;
}

function writeRunState(runId: string, state: 'accepted' | 'running' | 'terminal' | 'unknown', root?: string): void {
  const dir = path.join(ensure(root).states, runId);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, `${state}.json`);
  if (!fs.existsSync(file)) {
    try { atomicWrite(file, { version: 1, runId, state, at: Date.now() }, true); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const existing = readJson<{ version: number; runId: string; state: string }>(file, 4096);
      if (existing.version !== 1 || existing.runId !== runId || existing.state !== state) throw error;
    }
  }
}

export function groupAgentRunState(runId: string, root?: string): 'accepted' | 'running' | 'terminal' | 'unknown' | undefined {
  const dir = path.join(ensure(root).states, safeRunId(runId));
  for (const state of ['unknown', 'terminal', 'running', 'accepted'] as const) {
    try {
      const record = readJson<{ version: number; runId: string; state: string }>(path.join(dir, `${state}.json`), 4096);
      if (record.version === 1 && record.runId === runId && record.state === state) return state;
    } catch {}
  }
  return undefined;
}

function runFingerprint(request: Omit<GroupAgentMailboxRequest, 'version' | 'hostInstanceId' | 'hostGeneration' | 'createdAt'>): string {
  const { runId: _runId, ...payload } = request;
  return require('crypto').createHash('sha256').update(JSON.stringify(payload)).digest('hex');
}

export function publishGroupAgentRun(request: Omit<GroupAgentMailboxRequest, 'version' | 'runId' | 'hostInstanceId' | 'hostGeneration' | 'createdAt'> & { runId?: string }, host: GroupAgentHost, root?: string): string {
  if (!isGroupAgentId(request.agent) || !['developer', 'reviewer', 'bootstrap'].includes(request.role) || !Number.isInteger(request.round) || request.round < (request.role === 'bootstrap' ? 0 : 1) || request.bindingSnapshot?.agentId !== request.agent || !Number.isInteger(request.bindingSnapshot.revision) || !validModelRef(request.modelRef) || JSON.stringify(request.bindingSnapshot.modelRef) !== JSON.stringify(request.modelRef) || request.prompt.length > 32_000 || !path.isAbsolute(request.worktree) || !request.sessionId || request.sessionId.length > 128 || !Number.isFinite(request.timeoutMs) || request.timeoutMs < 1_000 || request.timeoutMs > 24 * 60 * 60 * 1000 || !Number.isInteger(host.generation) || !host.hostInstanceId)
    throw new Error('Invalid Group Agent mailbox request.');
  const runId = safeRunId(request.runId || randomUUID());
  const dirs = ensure(root);
  const { runId: _runId, ...payload } = request;
  const full: GroupAgentMailboxRequest = { version: 1, ...payload, runId, hostInstanceId: host.hostInstanceId, hostGeneration: host.generation, createdAt: Date.now() };
  const raw = Buffer.byteLength(JSON.stringify(full));
  if (raw > MAX_REQUEST_BYTES) throw new Error('Group Agent mailbox request exceeds its size limit.');
  const stateDir = path.join(dirs.states, runId);
  fs.mkdirSync(stateDir, { recursive: true, mode: 0o700 });
  const stateFile = path.join(stateDir, 'accepted.json');
  const fingerprintFile = path.join(stateDir, 'fingerprint.json');
  try {
    atomicWrite(fingerprintFile, { version: 1, runId, fingerprint: runFingerprint({ ...payload, runId }) }, true);
    atomicWrite(stateFile, { version: 1, runId, state: 'accepted', at: Date.now() }, true);
    atomicWrite(path.join(dirs.requests, `${runId}.json`), full, true);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    const saved = readJson<{ fingerprint: string }>(fingerprintFile, 4096);
    if (saved.fingerprint !== runFingerprint({ ...payload, runId })) throw new Error('Group Agent run id was reused with a different request.');
  }
  if (!groupAgentRunState(runId, root)) writeRunState(runId, 'accepted', root);
  if (groupAgentRunState(runId, root) === 'accepted' && !fs.existsSync(path.join(dirs.requests, `${runId}.json`))) {
    if (!fs.existsSync(path.join(dirs.claimed, `${runId}.json`))) atomicWrite(path.join(dirs.requests, `${runId}.json`), full, true);
  }
  return runId;
}

export function claimGroupAgentRuns(host: GroupAgentHost, root?: string): GroupAgentMailboxRequest[] {
  const dirs = ensure(root);
  const claimed: GroupAgentMailboxRequest[] = [];
  for (const name of fs.readdirSync(dirs.requests).filter((entry) => /^[0-9a-f-]{36}\.json$/i.test(entry))) {
    const source = path.join(dirs.requests, name);
    const target = path.join(dirs.claimed, name);
    const runId = name.slice(0, -5);
    if (groupAgentRunState(runId, root) !== 'accepted') {
      try { fs.unlinkSync(source); } catch {}
      continue;
    }
    try {
      fs.linkSync(source, target);
      fs.unlinkSync(source);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') continue;
      continue;
    }
    writeRunState(runId, 'running', root);
    let request: GroupAgentMailboxRequest | undefined;
    try {
      request = readJson<GroupAgentMailboxRequest>(target, MAX_REQUEST_BYTES);
      if (request.version !== 1 || request.runId !== runId || request.hostInstanceId !== host.hostInstanceId || request.hostGeneration !== host.generation || !isGroupAgentId(request.agent) || !['developer', 'reviewer', 'bootstrap'].includes(request.role) || !Number.isInteger(request.round) || request.round < (request.role === 'bootstrap' ? 0 : 1) || typeof request.prompt !== 'string' || request.prompt.length > 32_000 || !request.sessionId || request.sessionId.length > 128 || !path.isAbsolute(request.worktree) || !Number.isFinite(request.timeoutMs) || request.timeoutMs < 1_000 || request.timeoutMs > 24 * 60 * 60 * 1000 || request.bindingSnapshot?.agentId !== request.agent || !Number.isInteger(request.bindingSnapshot.revision) || !validModelRef(request.modelRef) || JSON.stringify(request.bindingSnapshot.modelRef) !== JSON.stringify(request.modelRef)) {
        publishGroupAgentFailure(runId, Number.isInteger(request.hostGeneration) ? request.hostGeneration : host.generation, { message: 'Invalid or stale mailbox request.', interruption: { kind: 'paused', reason: 'The request was rejected before execution.', at: new Date().toISOString(), by: 'group-agent-mailbox' } }, root);
        continue;
      }
      claimed.push(request);
    } catch (error) {
      console.warn(`[GroupAgentMailbox] Rejected malformed request ${name}:`, error);
      const responseGeneration = Number.isInteger(request?.hostGeneration) ? request!.hostGeneration : host.generation;
      try { publishGroupAgentFailure(runId, responseGeneration, { message: 'Malformed or stale mailbox request.', interruption: { kind: 'paused', reason: 'The request was rejected before execution.', at: new Date().toISOString(), by: 'group-agent-mailbox' } }, root); } catch {}
    }
  }
  return claimed;
}

export function publishGroupAgentResult(runIdValue: string, hostGeneration: number, result?: GroupAgentMailboxResult, error?: string, root?: string): void {
  const runId = safeRunId(runIdValue);
  const dirs = ensure(root);
  const file = path.join(dirs.responses, `${runId}.json`);
  if (result && !validResult(result)) throw new Error('Invalid Group Agent mailbox result.');
  const record = { version: 1, runId, hostGeneration, sequence: 1, completedAt: Date.now(), ...(result ? { result } : {}), ...(error ? { error: error.slice(0, 4000) } : {}) };
  if (Buffer.byteLength(JSON.stringify(record)) > MAX_RESPONSE_BYTES) throw new Error('Group Agent mailbox response exceeds its size limit.');
  writeMailboxResponse(file, record);
  writeRunState(runId, 'terminal', root);
}

export function publishGroupAgentFailure(runIdValue: string, hostGeneration: number, failure: GroupAgentMailboxFailure, root?: string): void {
  const runId = safeRunId(runIdValue);
  const dirs = ensure(root);
  if (!validFailure(failure)) throw new Error('Invalid Group Agent mailbox failure result.');
  const record = { version: 1, runId, hostGeneration, sequence: 1, completedAt: Date.now(), failure };
  if (Buffer.byteLength(JSON.stringify(record)) > MAX_RESPONSE_BYTES) throw new Error('Group Agent mailbox response exceeds its size limit.');
  const file = path.join(dirs.responses, `${runId}.json`);
  writeMailboxResponse(file, record);
  writeRunState(runId, 'terminal', root);
}

function writeMailboxResponse(file: string, record: Record<string, unknown>): void {
  try { atomicWrite(file, record, true); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
    const existing = readJson<Record<string, unknown>>(file, MAX_RESPONSE_BYTES);
    const candidate = { ...record, completedAt: existing.completedAt };
    if (!(require('util').isDeepStrictEqual(existing, candidate)))
      throw new Error('A conflicting final result already exists for this Group Agent run.');
  }
}

export function cancelUnclaimedGroupAgentRun(runIdValue: string, reason: string, root?: string): boolean {
  const runId = safeRunId(runIdValue);
  const dirs = ensure(root);
  const request = path.join(dirs.requests, `${runId}.json`);
  const cancelled = path.join(dirs.cancelled, `${runId}.json`);
  try { fs.renameSync(request, cancelled); } catch { return false; }
  publishGroupAgentFailure(runId, Number(readJson<GroupAgentMailboxRequest>(cancelled, MAX_REQUEST_BYTES).hostGeneration), {
    message: reason.slice(0, 4000),
    interruption: { kind: /delete/i.test(reason) ? 'deleted' : 'paused', reason: reason.slice(0, 1000), at: new Date().toISOString(), by: 'mailbox-client' },
  }, root);
  return true;
}

export function markGroupAgentRunUnknown(runIdValue: string, reason: string, root?: string): void {
  const runId = safeRunId(runIdValue);
  const dir = path.join(ensure(root).states, runId);
  fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
  const file = path.join(dir, 'unknown.json');
  if (!fs.existsSync(file)) atomicWrite(file, { version: 1, runId, state: 'unknown', reason: reason.slice(0, 1000), at: Date.now() }, true);
}

export function cleanupGroupAgentTaskArtifacts(thread: DelegationThread, root = getBridgeHome()): void {
  const sessions: Array<{ id: string; agent: string }> = [];
  if (isGroupAgentId(thread.targetAgent)) sessions.push({ id: thread.receiverSessionId, agent: thread.targetAgent });
  if (thread.reviewFlow === 'dedicated' && isGroupAgentId(thread.initiatorAgent)) {
    const reviewerSession = thread.reviewerSessionId || thread.callerSessionId;
    if (reviewerSession && reviewerSession !== thread.requesterSessionId) sessions.push({ id: reviewerSession, agent: thread.initiatorAgent });
  }
  for (const session of sessions) cleanupOwnedGroupAgentSession(session.id, thread.threadId, session.agent, root);
  const dirs = ensure(root);
  const runIds = new Set<string>();
  for (const dir of [dirs.requests, dirs.claimed, dirs.cancelled]) {
    for (const name of fs.readdirSync(dir).filter((entry) => /^[0-9a-f-]{36}\.json$/i.test(entry))) {
      try {
        const request = readJson<GroupAgentMailboxRequest>(path.join(dir, name), MAX_REQUEST_BYTES);
        if (request.threadId === thread.threadId) runIds.add(request.runId);
      } catch {}
    }
  }
  for (const runId of runIds) {
    const state = groupAgentRunState(runId, root);
    if (state === 'accepted') cancelUnclaimedGroupAgentRun(runId, 'Task was deleted.', root);
    if (!['terminal', 'accepted'].includes(state || '')) continue;
    for (const dir of [dirs.requests, dirs.claimed, dirs.cancelled, dirs.responses, dirs.cancel, dirs.pids, dirs.states]) {
      const target = dir === dirs.states ? path.join(dir, runId) : dir === dirs.pids ? path.join(dir, runId) : path.join(dir, `${runId}.json`);
      try { fs.rmSync(target, { recursive: true, force: true }); } catch {}
    }
  }
}

export function cleanupOwnedGroupAgentSession(sessionId: string, threadId: string, agentId: string, root = getBridgeHome()): boolean {
  if (!/^[A-Za-z0-9_-]{8,128}$/.test(sessionId)) return false;
  const base = path.join(root, 'group-agent-sessions', `${sessionId}.jsonl`);
  const ownerFile = `${base}.owner.json`;
  try {
    const owner = readJson<{ version: number; transport: string; threadId: string; ownerAgentId: string }>(ownerFile, 4096);
    if (owner.version !== 1 || owner.transport !== 'bridge-agent' || owner.threadId !== threadId || owner.ownerAgentId !== agentId) return false;
    fs.rmSync(base, { force: true });
    fs.rmSync(ownerFile, { force: true });
    return true;
  } catch { return false; }
}

export function hasUnsettledGroupAgentRun(threadId: string, sessionId: string, exceptRunId?: string, root?: string): boolean {
  const dirs = ensure(root);
  for (const name of fs.readdirSync(dirs.claimed).filter((entry) => /^[0-9a-f-]{36}\.json$/i.test(entry))) {
    const runId = name.slice(0, -5);
    if (runId === exceptRunId || !['running', 'unknown'].includes(groupAgentRunState(runId, root) || '')) continue;
    try {
      const request = readJson<GroupAgentMailboxRequest>(path.join(dirs.claimed, name), MAX_REQUEST_BYTES);
      if (request.threadId === threadId && request.sessionId === sessionId) return true;
    } catch {}
  }
  return false;
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
  if (!Number.isInteger(sequence) || sequence < 1) throw new Error('Invalid Group Agent PID sequence.');
  const directory = path.join(ensure(root).pids, runId);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  atomicWrite(path.join(directory, `${String(sequence).padStart(8, '0')}.json`), { version: 1, runId, sequence, pid: pid || null, at: Date.now() }, true);
}

export class GroupAgentMailboxClient {
  constructor(private readonly root = getBridgeHome()) {}

  async execute(request: Omit<GroupAgentMailboxRequest, 'version' | 'runId' | 'hostInstanceId' | 'hostGeneration' | 'createdAt'> & { runId?: string }, host: GroupAgentHost, signal: AbortSignal, onChildPid?: (pid: number | undefined) => void): Promise<GroupAgentMailboxResult> {
    const runId = publishGroupAgentRun(request, host, this.root);
    const dirs = ensure(this.root);
    let pidSequence = 0;
    let stoppingAt: number | undefined;
    const suspension = new HostSuspensionGuard({ tickMs: 100 });
    suspension.start();
    try {
    while (true) {
        suspension.sync();
        const state = groupAgentRunState(runId, this.root);
        if (state === 'unknown') throw Object.assign(new Error('The Group Agent run stopped with an unknown outcome; inspect the worktree before retrying.'), { code: 'GROUP_AGENT_OUTCOME_UNKNOWN', interruption: { kind: 'paused', reason: 'The previous Group Agent executor could not confirm shutdown.', at: new Date().toISOString(), by: 'group-agent-mailbox' } });
        const currentHost = loadGroupAgentHost(this.root);
        const generationChanged = !currentHost || currentHost.hostInstanceId !== host.hostInstanceId || currentHost.generation !== host.generation;
        if (signal.aborted || generationChanged) {
          const reason = generationChanged ? 'The Group Agent host generation changed; resume this task after the prior run has stopped.' : String(signal.reason || 'Group Agent run cancelled.');
          if (cancelUnclaimedGroupAgentRun(runId, reason, this.root)) throw Object.assign(new Error(reason), { interruption: { kind: /delete/i.test(reason) ? 'deleted' : 'paused', reason, at: new Date().toISOString(), by: 'group-agent-mailbox' } });
          publishGroupAgentCancel(runId, reason, this.root);
          const activeNow = monotonicNowMs() - suspension.totalSuspendedMs();
          stoppingAt ??= activeNow;
          if (activeNow - stoppingAt >= 15_000) {
            markGroupAgentRunUnknown(runId, 'Cancellation was not acknowledged by the host executor.', this.root);
            throw Object.assign(new Error('Group Agent cancellation was not acknowledged; outcome is unknown and this session cannot be resumed safely.'), { code: 'GROUP_AGENT_OUTCOME_UNKNOWN', interruption: { kind: 'paused', reason: 'Executor stop was not confirmed.', at: new Date().toISOString(), by: 'group-agent-mailbox' } });
          }
        }
        if (!generationChanged || state === 'terminal' || state === 'running') {
        const pidDirectory = path.join(dirs.pids, runId);
        try {
          const records = fs.readdirSync(pidDirectory).filter((entry) => /^\d{8}\.json$/.test(entry)).sort();
          if (records.length) {
            const pid = readJson<{ sequence: number; pid: number | null }>(path.join(pidDirectory, records[records.length - 1]), 4096);
            if (pid.sequence > pidSequence) { pidSequence = pid.sequence; onChildPid?.(pid.pid || undefined); }
          }
        } catch {}
        const responseFile = path.join(dirs.responses, `${runId}.json`);
        try {
          const response = readJson<{ version: number; runId: string; hostGeneration: number; sequence: number; completedAt: number; result?: GroupAgentMailboxResult; error?: string; failure?: GroupAgentMailboxFailure }>(responseFile, MAX_RESPONSE_BYTES);
          if (response.version !== 1 || response.runId !== runId || response.hostGeneration !== host.generation || response.sequence !== 1 || !Number.isFinite(response.completedAt))
            throw new Error('The Group Agent mailbox response was stale or malformed.');
          try { fs.rmSync(pidDirectory, { recursive: true, force: true }); } catch {}
          if (response.failure) {
            const failure = response.failure;
            if (!validFailure(failure)) throw new Error('The Group Agent mailbox failure was malformed.');
            const interruption = failure.interruption || (generationChanged ? { kind: 'paused' as const, reason: 'Host generation changed before the mailbox failure was accepted.', at: new Date().toISOString(), by: 'group-agent-mailbox' } : undefined);
            const error = Object.assign(new Error(failure.message), { modelRequests: failure.modelRequests, tokenUsage: failure.tokenUsage, interruption, sessionId: failure.sessionId, toolCallsCompleted: failure.toolCallsCompleted });
            throw error;
          }
          if (response.error) throw new Error(response.error);
          if (!validResult(response.result)) throw new Error('The Group Agent mailbox response has no valid result.');
          if (generationChanged) throw Object.assign(new Error('A late result from the previous Group Agent host generation was rejected.'), { code: 'GROUP_AGENT_STALE_GENERATION', interruption: { kind: 'paused', reason: 'Host generation changed before the result could be accepted.', at: new Date().toISOString(), by: 'group-agent-mailbox' } });
          if (signal.aborted) throw Object.assign(new Error(String(signal.reason || 'Group Agent run cancelled before result acceptance.')), { interruption: { kind: 'paused', reason: String(signal.reason || 'Group Agent run cancelled before result acceptance.'), at: new Date().toISOString(), by: 'mailbox-client' } });
          return response.result;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') { /* wait for host response */ }
          else throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }
    } finally {
      suspension.stop();
    }
  }
}
