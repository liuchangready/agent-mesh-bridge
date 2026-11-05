import * as childProcess from 'child_process';
import * as readline from 'readline';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';
import { TokenUsageComponent } from './types';

export interface CodexCompactionRequest {
  executable: string;
  sessionId: string;
  cwd: string;
  model: string;
  operationId: string;
  role: 'developer' | 'reviewer';
  timeoutMs?: number;
  cancelled?: () => boolean;
  onChildPid?: (pid: number | undefined) => void;
}

export interface CodexCompactionResult {
  threadId: string;
  nativeTurnId: string;
  model: string;
  provider: string;
  processExitCode: number;
  usage?: TokenUsageComponent;
}

interface RpcMessage { id?: number | string; method?: string; result?: any; error?: { code?: number; message?: string }; params?: any; }

/** A private stdio app-server client. It never starts a model-authored turn. */
export class CodexNativeCompaction {
  public async compact(request: CodexCompactionRequest): Promise<CodexCompactionResult> {
    const threadId = request.sessionId.trim();
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(threadId)) {
      throw new Error('Codex compaction requires a canonical thread UUID.');
    }
    if (!request.cwd || !request.model) throw new Error('Codex compaction requires the existing worktree and resolved model.');
    const timeoutMs = request.timeoutMs ?? 120_000;
    const child = childProcess.spawn(request.executable, ['app-server', '--listen', 'stdio://', '-c', 'mcp_servers={}', '-c', 'hooks={}'], {
      cwd: request.cwd,
      windowsHide: true,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env },
    });
    if (!child.stdin || !child.stdout) throw new Error('Codex app-server stdio is unavailable.');
    request.onChildPid?.(child.pid);
    const guard = new HostSuspensionGuard();
    guard.start();
    let nextId = 0;
    const notifications: RpcMessage[] = [];
    const waiters = new Map<string, { resolve: (message: RpcMessage) => void; reject: (error: Error) => void }>();
    const lineReader = readline.createInterface({ input: child.stdout });
    const rejectServerRequests = (message: RpcMessage): void => {
      if (message.id === undefined || !message.method) return;
      child.stdin?.write(`${JSON.stringify({ jsonrpc: '2.0', id: message.id, error: { code: -32000, message: 'Native compaction does not accept approvals or tool requests.' } })}\n`);
      child.kill();
    };
    lineReader.on('line', (line) => {
      let message: RpcMessage;
      try { message = JSON.parse(line); } catch { return; }
      if (message.id !== undefined && waiters.has(String(message.id))) {
        const waiter = waiters.get(String(message.id))!;
        waiters.delete(String(message.id));
        waiter.resolve(message);
      } else if (message.method && message.id === undefined) {
        notifications.push(message);
      } else {
        rejectServerRequests(message);
      }
    });
    child.on('error', (error) => { for (const waiter of waiters.values()) waiter.reject(error); waiters.clear(); });
    const rpc = async (method: string, params: unknown): Promise<RpcMessage> => {
      if (request.cancelled?.()) throw new Error('Native compaction cancelled.');
      const id = ++nextId;
      const started = monotonicNowMs();
      child.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
      while (monotonicNowMs() - started - guard.suspendedWithin(started, monotonicNowMs()) < timeoutMs) {
        if (request.cancelled?.()) throw new Error('Native compaction cancelled.');
        const response = await new Promise<RpcMessage | undefined>((resolve, reject) => {
          const timer = setTimeout(() => resolve(undefined), 100);
          waiters.set(String(id), { resolve: (message) => { clearTimeout(timer); resolve(message); }, reject });
        });
        if (!response) continue;
        if (response.error) throw new Error(response.error.message || `${method} failed (${response.error.code ?? 'unknown'}).`);
        return response;
      }
      throw new Error(`${method} timed out after ${timeoutMs} active milliseconds.`);
    };
    const waitForTurn = async (from: number): Promise<any> => {
      const started = monotonicNowMs();
      while (monotonicNowMs() - started - guard.suspendedWithin(started, monotonicNowMs()) < timeoutMs) {
        if (request.cancelled?.()) throw new Error('Native compaction cancelled.');
        const completed = notifications.slice(from).find((message) => message.method === 'turn/completed' && message.params?.threadId === threadId);
        if (completed) return completed.params.turn;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      throw new Error('Codex did not complete the native compaction turn.');
    };
    const close = async (): Promise<number> => {
      lineReader.close();
      if (child.exitCode !== null) return child.exitCode ?? -1;
      child.stdin?.end();
      const exited = await new Promise<number>((resolve) => {
        const timer = setTimeout(() => { child.kill(); resolve(-1); }, 5_000);
        child.once('exit', (code) => { clearTimeout(timer); resolve(code ?? -1); });
      });
      return exited;
    };

    try {
      await rpc('initialize', { clientInfo: { name: 'agent-mesh-bridge', title: 'Agent Mesh Bridge', version: '1.0.0' }, capabilities: null });
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'initialized', params: {} })}\n`);
      const resumed = await rpc('thread/resume', {
        threadId, cwd: request.cwd, model: request.model, approvalPolicy: 'never', sandbox: 'read-only', excludeTurns: true,
        config: { mcp_servers: {}, hooks: {} },
      });
      const thread = resumed.result?.thread;
      if (!thread || thread.id !== threadId || thread.cwd !== request.cwd || thread.model !== request.model) {
        throw new Error('Codex did not resume the same thread, worktree, and requested model.');
      }
      if (thread.status?.type !== 'idle') {
        throw new Error(`Codex thread is not confirmed idle (${thread.status?.type || 'unknown'}).`);
      }
      const before = notifications.length;
      await rpc('thread/compact/start', { threadId });
      const turn = await waitForTurn(before);
      if (turn?.status !== 'completed' || !turn.id) throw new Error(`Codex compaction turn ended with status ${turn?.status || 'unknown'}.`);
      const item = notifications.slice(before).find((message) =>
        message.method === 'item/completed' && message.params?.threadId === threadId &&
        message.params?.turnId === turn.id && message.params?.item?.type === 'contextCompaction');
      if (!item) throw new Error('Codex compaction turn completed without its matching contextCompaction item.');
      const usageEvent = notifications.slice(before).find((message) =>
        message.method === 'thread/tokenUsage/updated' && message.params?.threadId === threadId &&
        message.params?.turnId === turn.id && message.params?.tokenUsage?.last);
      const usage = usageEvent ? codexCompactionUsage(usageEvent.params.tokenUsage.last, request) : undefined;
      const exitCode = await close();
      if (exitCode !== 0) throw new Error(`Codex app-server exited ${exitCode} after reporting compaction completion.`);
      await this.verifyPersisted(request, turn.id, thread.modelProvider);
      return { threadId, nativeTurnId: turn.id, model: thread.model, provider: thread.modelProvider, processExitCode: exitCode, usage };
    } catch (error) {
      try { child.kill(); } catch {}
      try { await close(); } catch {}
      request.onChildPid?.(undefined);
      throw error;
    } finally {
      guard.stop();
    }
  }

  private async verifyPersisted(request: CodexCompactionRequest, turnId: string, provider: string): Promise<void> {
    const child = childProcess.spawn(request.executable, ['app-server', '--listen', 'stdio://', '-c', 'mcp_servers={}', '-c', 'hooks={}'], {
      cwd: request.cwd, windowsHide: true, stdio: ['pipe', 'pipe', 'ignore'], env: { ...process.env },
    });
    if (!child.stdin || !child.stdout) throw new Error('Codex persistence verifier stdio is unavailable.');
    request.onChildPid?.(child.pid);
    const reader = readline.createInterface({ input: child.stdout });
    const waiters = new Map<string, { resolve: (value: RpcMessage) => void; reject: (error: Error) => void }>();
    let id = 0;
    reader.on('line', (line) => {
      let message: RpcMessage;
      try { message = JSON.parse(line); } catch { return; }
      if (message.id !== undefined) {
        const waiter = waiters.get(String(message.id));
        if (waiter) { waiters.delete(String(message.id)); waiter.resolve(message); }
      }
    });
    const rpc = (method: string, params: unknown): Promise<RpcMessage> => new Promise((resolve, reject) => {
      const requestId = ++id;
      const timer = setTimeout(() => { waiters.delete(String(requestId)); reject(new Error(`Codex persistence check timed out at ${method}.`)); }, 30_000);
      waiters.set(String(requestId), { resolve: (message) => { clearTimeout(timer); resolve(message); }, reject });
      child.stdin!.write(`${JSON.stringify({ jsonrpc: '2.0', id: requestId, method, params })}\n`);
    });
    try {
      await rpc('initialize', { clientInfo: { name: 'agent-mesh-bridge', title: 'Agent Mesh Bridge', version: '1.0.0' }, capabilities: null });
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'initialized', params: {} })}\n`);
      const response = await rpc('thread/resume', {
        threadId: request.sessionId.trim(), cwd: request.cwd, model: request.model,
        approvalPolicy: 'never', sandbox: 'read-only', excludeTurns: false,
        config: { mcp_servers: {}, hooks: {} },
      });
      if (response.error) throw new Error(response.error.message || 'Codex could not reload the compacted session.');
      const thread = response.result?.thread;
      const persisted = thread?.turns?.some((turn: any) => turn.id === turnId &&
        turn.status === 'completed' && turn.items?.some((item: any) => item.type === 'contextCompaction'));
      if (thread?.id !== request.sessionId.trim() || thread?.cwd !== request.cwd ||
          thread?.model !== request.model || thread?.modelProvider !== provider || !persisted) {
        throw new Error('Codex did not reload the same model, provider, worktree, and completed compaction item.');
      }
      reader.close();
      child.stdin.end();
      const exitCode = await new Promise<number>((resolve) => {
        const timer = setTimeout(() => { child.kill(); resolve(-1); }, 5_000);
        child.once('exit', (code) => { clearTimeout(timer); resolve(code ?? -1); });
      });
      if (exitCode !== 0) throw new Error(`Codex persistence verifier exited ${exitCode}.`);
    } catch (error) {
      try { child.kill(); } catch {}
      await new Promise<void>((resolve) => {
        if (child.exitCode !== null) return resolve();
        child.once('exit', () => resolve());
      });
      throw error;
    } finally {
      request.onChildPid?.(undefined);
    }
  }
}

function codexCompactionUsage(value: any, request: CodexCompactionRequest): TokenUsageComponent | undefined {
  const input = value?.inputTokens;
  const cached = value?.cachedInputTokens;
  const cacheWrite = value?.cacheWriteInputTokens;
  const output = value?.outputTokens;
  if (![input, cached, cacheWrite, output].every((counter) => Number.isFinite(counter) && counter >= 0) || cached + cacheWrite > input) return undefined;
  return {
    id: request.operationId, kind: 'native_compaction', role: request.role, agent: 'codex', model: request.model,
    uncachedInputTokens: input - cached - cacheWrite,
    cacheReadTokens: cached,
    cacheWriteTokens: cacheWrite,
    outputTokens: output,
    reasoningTokens: Number.isFinite(value?.reasoningOutputTokens) ? value.reasoningOutputTokens : undefined,
    nativeTotalTokens: Number.isFinite(value?.totalTokens) ? value.totalTokens : undefined,
    source: 'native_reported', available: true,
  };
}
