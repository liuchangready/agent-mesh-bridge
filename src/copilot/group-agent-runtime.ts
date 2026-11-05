import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { killPidTree } from '../bridge/platform';
import type { TokenUsageMetrics } from '../bridge/types';

export type GroupProviderUsage = Pick<TokenUsageMetrics,
  'uncachedInputTokens' | 'cacheReadTokens' | 'cacheWriteTokens' | 'promptTokens' | 'outputTokens' | 'totalTokens'> &
  Pick<Partial<TokenUsageMetrics>, 'nativeTotalTokens'>;

export type GroupMessage = {
  role: 'user' | 'assistant' | 'tool';
  content: string;
  toolCalls?: Array<{ id: string; name: string; arguments: Record<string, unknown> }>;
  toolCallId?: string;
  toolName?: string;
};

export interface GroupTool {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
}

export interface GroupTurnResponse {
  text: string;
  toolCalls: Array<{ id: string; name: string; arguments: Record<string, unknown> }>;
  finishReason: 'stop' | 'tool_calls';
  usage?: GroupProviderUsage;
}

export interface GroupAgentRunResult {
  text: string;
  toolCallsCompleted: number;
  modelRequests: number;
  usageRequests: number;
  usage?: GroupProviderUsage;
  tokenUsage?: TokenUsageMetrics;
}

export interface GroupAgentRuntimeOptions {
  bridgeHome: string;
  request: (messages: GroupMessage[], tools: GroupTool[], signal: AbortSignal) => Promise<GroupTurnResponse>;
  onChildPid?: (pid: number | undefined) => void;
  controlTool?: (name: string, input: Record<string, unknown>) => Promise<string>;
  commandTimeoutMs?: number;
  maxToolOutputBytes?: number;
}

const MAX_TURNS = 24;
const MAX_MESSAGE_BYTES = 2 * 1024 * 1024;
const MAX_SESSION_BYTES = 32 * 1024 * 1024;
const MAX_TOOL_OUTPUT = 256 * 1024;
const MAX_CONTEXT_MESSAGES = 500;
const MAX_CONTEXT_BYTES = 2 * 1024 * 1024;
function redactToolOutput(value: string): string {
  return value
    .replace(/(\b[A-Z0-9_]*(?:API[_-]?KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)[A-Z0-9_]*\s*[:=]\s*)([^\r\n]+)/gim, '$1[redacted]')
    .replace(/\bBearer\s+[A-Za-z0-9._~+\/-]+=*/gi, 'Bearer [redacted]')
    .replace(/\bsk-[A-Za-z0-9_-]{20,}\b/g, '[redacted]');
}
const GROUP_TOOLS: GroupTool[] = [
  { name: 'list_files', description: 'List files in the task worktree.', inputSchema: { type: 'object', properties: { path: { type: 'string' } } } },
  { name: 'read_file', description: 'Read a UTF-8 file in the task worktree.', inputSchema: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] } },
  { name: 'search_files', description: 'Search text in files in the task worktree.', inputSchema: { type: 'object', properties: { query: { type: 'string' } }, required: ['query'] } },
  { name: 'write_file', description: 'Write a UTF-8 file in the task worktree.', inputSchema: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' } }, required: ['path', 'content'] } },
  { name: 'run_command', description: 'Run a command in the task worktree.', inputSchema: { type: 'object', properties: { command: { type: 'string' } }, required: ['command'] } },
];
const REVIEWER_TOOLS: GroupTool[] = [
  ...GROUP_TOOLS.filter((tool) => ['list_files', 'read_file', 'search_files'].includes(tool.name)),
  { name: 'request_rework', description: 'Return the task to its developer with review feedback.', inputSchema: { type: 'object', properties: { threadId: { type: 'string' }, feedback: { type: 'string' } }, required: ['threadId', 'feedback'], additionalProperties: false } },
  { name: 'approve_task', description: 'Approve the reviewed task through the Bridge task service.', inputSchema: { type: 'object', properties: { threadId: { type: 'string' }, remarks: { type: 'string' } }, required: ['threadId'], additionalProperties: false } },
];
const BOOTSTRAP_TOOLS: GroupTool[] = [{
  name: 'dispatch_task', description: 'Confirm the one prepared task using its one-time reviewer bootstrap token.',
  inputSchema: { type: 'object', anyOf: [
    { properties: { threadId: { type: 'string' }, token: { type: 'string' } }, required: ['threadId', 'token'], additionalProperties: false },
    { properties: { reviewBootstrap: { type: 'object', properties: { threadId: { type: 'string' }, token: { type: 'string' } }, required: ['threadId', 'token'], additionalProperties: false } }, required: ['reviewBootstrap'], additionalProperties: false },
  ] },
}];

export class GroupAgentSessionStore {
  private readonly directory: string;
  constructor(bridgeHome: string) {
    this.directory = path.join(bridgeHome, 'group-agent-sessions');
    fs.mkdirSync(this.directory, { recursive: true });
  }

  append(sessionId: string, event: GroupMessage | { role: 'intent' | 'result'; callId: string; name: string; arguments?: unknown; content?: string }): void {
    const file = this.file(sessionId);
    const line = `${JSON.stringify({ at: new Date().toISOString(), ...event })}\n`;
    if (Buffer.byteLength(line) > MAX_MESSAGE_BYTES) throw new Error('Group Agent session event exceeds the size limit.');
    let size = 0;
    try { size = fs.statSync(file).size; } catch {}
    if (size + Buffer.byteLength(line) > MAX_SESSION_BYTES) throw new Error('Group Agent session reached its storage limit.');
    fs.appendFileSync(file, line, { encoding: 'utf8', mode: 0o600 });
  }

  read(sessionId: string, offset = 0, limit = 100): GroupMessage[] {
    if (!Number.isInteger(offset) || offset < 0 || !Number.isInteger(limit) || limit < 1 || limit > 500)
      throw new Error('Invalid Group Agent session page.');
    const events = fs.readFileSync(this.file(sessionId), 'utf8').split('\n').filter(Boolean);
    return events.slice(offset, offset + limit).map((line) => JSON.parse(line) as GroupMessage);
  }

  hasUnresolvedIntent(sessionId: string): boolean {
    try {
      const events = fs.readFileSync(this.file(sessionId), 'utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line));
      const pending = new Set<string>();
      for (const event of events) {
        if (event.role === 'intent' && typeof event.callId === 'string') pending.add(event.callId);
        if (event.role === 'tool' && typeof event.toolCallId === 'string') pending.delete(event.toolCallId);
      }
      return pending.size > 0;
    } catch { return false; }
  }

  private file(sessionId: string): string {
    if (!/^[A-Za-z0-9_-]{8,128}$/.test(sessionId)) throw new Error('Invalid Bridge Agent session id.');
    return path.join(this.directory, `${sessionId}.jsonl`);
  }
}

export class GroupAgentRuntime {
  readonly sessions: GroupAgentSessionStore;
  private readonly options: GroupAgentRuntimeOptions;
  private readonly active = new Map<string, AbortController>();

  constructor(options: GroupAgentRuntimeOptions) {
    this.options = options;
    this.sessions = new GroupAgentSessionStore(options.bridgeHome);
  }

  async run(sessionId: string, cwd: string, prompt: string, signal?: AbortSignal, role: 'developer' | 'reviewer' | 'bootstrap' = 'developer'): Promise<GroupAgentRunResult> {
    if (this.active.has(sessionId)) throw new Error('This Group Agent session already has an active run.');
    const root = fs.realpathSync(cwd);
    const controller = new AbortController();
    const abort = () => controller.abort(signal?.reason);
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    this.active.set(sessionId, controller);
    try {
      if (this.sessions.hasUnresolvedIntent(sessionId))
        throw new Error('A previous tool call has an unknown outcome; inspect the task worktree before resuming this session.');
      this.sessions.append(sessionId, { role: 'user', content: prompt });
      const messages = this.readMessages(sessionId);
      const completedCalls = new Set<string>();
      const intents = new Set<string>();
      let usage: GroupProviderUsage | undefined;
      let modelRequests = 0;
      let usageRequests = 0;
      const tools = role === 'bootstrap' ? BOOTSTRAP_TOOLS : role === 'reviewer' ? REVIEWER_TOOLS : GROUP_TOOLS;
      for (let turn = 0; turn < MAX_TURNS; turn++) {
        if (controller.signal.aborted) throw new Error('Group Agent run cancelled.');
        const response = await this.options.request(messages, tools, controller.signal);
        if (controller.signal.aborted) throw new Error('Group Agent run cancelled.');
        modelRequests++;
        if (response.usage) {
          usageRequests++;
          usage = usage ? {
            uncachedInputTokens: usage.uncachedInputTokens + response.usage.uncachedInputTokens,
            cacheReadTokens: usage.cacheReadTokens + response.usage.cacheReadTokens,
            cacheWriteTokens: usage.cacheWriteTokens + response.usage.cacheWriteTokens,
            promptTokens: usage.promptTokens + response.usage.promptTokens,
            outputTokens: usage.outputTokens + response.usage.outputTokens,
            totalTokens: usage.totalTokens + response.usage.totalTokens,
            ...((usage.nativeTotalTokens !== undefined || response.usage.nativeTotalTokens !== undefined)
              ? { nativeTotalTokens: (usage.nativeTotalTokens || 0) + (response.usage.nativeTotalTokens || 0) } : {}),
          } : { ...response.usage };
        }
        const calls = response.toolCalls || [];
        this.sessions.append(sessionId, { role: 'assistant', content: response.text || '', ...(calls.length ? { toolCalls: calls } : {}) });
        messages.push({ role: 'assistant', content: response.text || '', ...(calls.length ? { toolCalls: calls } : {}) });
        if (response.finishReason === 'stop') {
          if (!response.text.trim()) throw new Error('Group Agent returned an empty final answer.');
          return { text: response.text, toolCallsCompleted: completedCalls.size, modelRequests, usageRequests, ...(usage ? { usage } : {}) };
        }
        if (!calls.length) throw new Error('Group Agent requested tools without structured tool calls.');
        for (const call of calls) {
          if (!call.id || completedCalls.has(call.id) || intents.has(call.id))
            throw new Error('Group Agent returned a duplicate tool call id.');
          if (Buffer.byteLength(JSON.stringify(call.arguments)) > 32 * 1024)
            throw new Error('Group Agent tool arguments exceed 32 KB.');
          intents.add(call.id);
          this.sessions.append(sessionId, { role: 'intent', callId: call.id, name: call.name, arguments: call.arguments });
          const isControl = role === 'bootstrap' || (role === 'reviewer' && ['request_rework', 'approve_task'].includes(call.name));
          const controlInput = call.name === 'dispatch_task' && call.arguments.reviewBootstrap && typeof call.arguments.reviewBootstrap === 'object'
            ? call.arguments.reviewBootstrap as Record<string, unknown>
            : call.arguments;
          const result = redactToolOutput(isControl
            ? await this.options.controlTool?.(call.name, controlInput) ?? (() => { throw new Error('Group reviewer control service is unavailable.'); })()
            : await this.executeTool(root, call.name, call.arguments, controller));
          this.sessions.append(sessionId, { role: 'tool', toolCallId: call.id, toolName: call.name, content: result });
          messages.push({ role: 'tool', toolCallId: call.id, toolName: call.name, content: result });
          completedCalls.add(call.id);
          if (isControl) return { text: result, toolCallsCompleted: completedCalls.size, modelRequests, usageRequests, ...(usage ? { usage } : {}) };
        }
      }
      throw new Error('Group Agent exceeded the tool-call turn limit.');
    } finally {
      signal?.removeEventListener('abort', abort);
      this.active.delete(sessionId);
    }
  }

  cancel(sessionId: string): void {
    this.active.get(sessionId)?.abort(new Error('Task cancelled.'));
  }

  private readMessages(sessionId: string): GroupMessage[] {
    const all: GroupMessage[] = [];
    for (let offset = 0; ; offset += 500) {
      const page = this.sessions.read(sessionId, offset, 500);
      all.push(...page.filter((event) => ['user', 'assistant', 'tool'].includes(event.role)));
      if (page.length < 500) break;
    }
    const turns: GroupMessage[][] = [];
    for (const message of all) {
      if (message.role === 'user') turns.push([]);
      turns.at(-1)?.push(message);
    }
    let selected: GroupMessage[] = [];
    let size = 0;
    for (let index = turns.length - 1; index >= 0; index--) {
      const turn = turns[index];
      const turnSize = turn.reduce((total, message) => total + Buffer.byteLength(JSON.stringify(message)), 0);
      if (selected.length && (selected.length + turn.length > MAX_CONTEXT_MESSAGES || size + turnSize > MAX_CONTEXT_BYTES)) break;
      if (turn.length > MAX_CONTEXT_MESSAGES || turnSize > MAX_CONTEXT_BYTES)
        throw new Error('The latest complete Group Agent conversation turn exceeds the context limit.');
      selected = turn.concat(selected);
      size += turnSize;
    }
    return selected;
  }

  private async executeTool(root: string, name: string, input: Record<string, unknown>, controller: AbortController): Promise<string> {
    const maxOutput = this.options.maxToolOutputBytes ?? MAX_TOOL_OUTPUT;
    if (name === 'run_command') {
      if (typeof input.command !== 'string' || !input.command.trim() || input.command.length > 16_000)
        throw new Error('Invalid run_command input.');
      return this.runCommand(root, input.command, controller, maxOutput);
    }
    if (name === 'search_files') {
      if (typeof input.query !== 'string' || !input.query || input.query.length > 500) throw new Error('Invalid search query.');
      const query = input.query;
      const files = this.listFiles(root);
      const matches: string[] = [];
      for (const file of files) {
        if (matches.join('\n').length > maxOutput) break;
        try {
          const text = fs.readFileSync(path.join(root, file), 'utf8');
          text.split(/\r?\n/).forEach((line, index) => { if (line.includes(query)) matches.push(`${file}:${index + 1}:${line}`); });
        } catch {}
      }
      return matches.join('\n').slice(0, maxOutput) || 'No matches.';
    }
    if (name === 'list_files') return this.listFiles(input.path ? this.resolvePath(root, this.stringArg(input.path, 'path')) : root).slice(0, 2000).join('\n');
    if (name === 'read_file') {
      const file = this.resolvePath(root, this.stringArg(input.path, 'path'));
      const content = fs.readFileSync(file, 'utf8');
      return Buffer.byteLength(content) > maxOutput ? content.slice(0, maxOutput) : content;
    }
    if (name === 'write_file') {
      const file = this.resolvePath(root, this.stringArg(input.path, 'path'));
      const content = this.stringArg(input.content, 'content');
      if (Buffer.byteLength(content) > maxOutput) throw new Error('write_file content exceeds the size limit.');
      fs.mkdirSync(path.dirname(file), { recursive: true });
      fs.writeFileSync(file, content, { encoding: 'utf8', mode: 0o600 });
      return `Wrote ${path.relative(root, file)} (${Buffer.byteLength(content)} bytes).`;
    }
    throw new Error(`Unknown Group Agent tool: ${name}`);
  }

  private resolvePath(root: string, value: string): string {
    const candidate = path.resolve(root, value);
    const relative = path.relative(root, candidate);
    if (relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('Tool path escapes the task worktree.');
    let parent = path.dirname(candidate);
    while (!fs.existsSync(parent) && parent !== root) parent = path.dirname(parent);
    const realParent = fs.realpathSync(parent);
    const realRelative = path.relative(root, realParent);
    if (realRelative.startsWith('..') || path.isAbsolute(realRelative)) throw new Error('Tool path resolves outside the task worktree.');
    if (fs.existsSync(candidate)) {
      const realCandidate = fs.realpathSync(candidate);
      const candidateRelative = path.relative(root, realCandidate);
      if (candidateRelative.startsWith('..') || path.isAbsolute(candidateRelative)) throw new Error('Tool path resolves outside the task worktree.');
    }
    return candidate;
  }

  private listFiles(directory: string): string[] {
    const out: string[] = [];
    const visit = (current: string) => {
      for (const entry of fs.readdirSync(current, { withFileTypes: true })) {
        if (entry.name === '.git' || entry.name === 'node_modules' || entry.name === '.agent-mesh-bridge') continue;
        const file = path.join(current, entry.name);
        if (entry.isDirectory()) visit(file);
        else if (entry.isFile()) out.push(path.relative(directory, file));
        if (out.length >= 5000) return;
      }
    };
    visit(directory);
    return out;
  }

  private stringArg(value: unknown, name: string): string {
    if (typeof value !== 'string' || Buffer.byteLength(value) > MAX_MESSAGE_BYTES) throw new Error(`Invalid ${name}.`);
    return value;
  }

  private runCommand(cwd: string, command: string, controller: AbortController, limit: number): Promise<string> {
    return new Promise((resolve, reject) => {
      const shell = process.platform === 'win32' ? 'powershell.exe' : '/bin/sh';
      const args = process.platform === 'win32' ? ['-NoProfile', '-NonInteractive', '-Command', command] : ['-lc', command];
      const safeEnv = Object.fromEntries(Object.entries(process.env).filter(([key]) => !/(?:API[_-]?KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)/i.test(key)));
      const child = childProcess.spawn(shell, args, { cwd, env: { ...safeEnv, MESH_TASK_RECEIVER: '1' }, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true });
      let output = '';
      let bytes = 0;
      let settled = false;
      let terminationError: Error | undefined;
      const finish = (error?: Error, code?: number | null) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        controller.signal.removeEventListener('abort', abort);
        this.options.onChildPid?.(undefined);
        if (error) reject(error);
        else resolve(`${output}\n[exit ${code ?? 'unknown'}]`);
      };
      const terminate = (error: Error) => {
        if (terminationError) return;
        terminationError = error;
        if (child.pid) killPidTree(child.pid);
      };
      const abort = () => terminate(new Error('Command cancelled.'));
      const timeout = setTimeout(() => terminate(new Error('Command timed out.')), this.options.commandTimeoutMs ?? 120_000);
      controller.signal.addEventListener('abort', abort, { once: true });
      if (controller.signal.aborted) abort();
      if (child.pid) this.options.onChildPid?.(child.pid);
      const collect = (chunk: Buffer) => {
        bytes += chunk.length;
        output += chunk.toString('utf8');
        if (bytes > limit) terminate(new Error('Command output exceeded the size limit.'));
      };
      child.stdout?.on('data', collect);
      child.stderr?.on('data', collect);
      child.once('error', (error) => finish(error));
      child.once('close', (code) => finish(terminationError, code));
    });
  }
}
