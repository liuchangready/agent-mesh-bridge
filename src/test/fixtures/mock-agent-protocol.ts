import * as fs from 'node:fs';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import { spawn, ChildProcessWithoutNullStreams } from 'node:child_process';
import { createInterface } from 'node:readline';

export interface MockScenario {
  rounds: number;
  fault?: 'empty-worker' | 'nonzero-worker' | 'no-verdict' | 'bad-bootstrap-token';
  reviewerTailMs?: number;
  bootstrapProcessChurn?: boolean;
  pauseAt?: 'bootstrap' | 'develop' | 'review' | 'merge';
  pauseRound?: number;
}

export function fixtureRoot(): string {
  const root = process.env.MESH_MOCK_ROOT;
  if (!root || !fs.existsSync(path.join(root, 'mock-only.json'))) {
    throw new Error('Mock agents require an explicitly isolated test root');
  }
  return root;
}

export function event(root: string, kind: string, details: Record<string, unknown> = {}): void {
  const at = process.hrtime.bigint().toString();
  fs.writeFileSync(path.join(root, 'events', `${at}-${process.pid}-${crypto.randomUUID()}.json`),
    JSON.stringify({ kind, at, pid: process.pid, ...details }));
}

/** Real JSON-RPC over stdio. No dispatch, verdict or ledger mutation is mocked. */
export class MockMcpClient {
  readonly child: ChildProcessWithoutNullStreams;
  stderr = '';
  private nextId = 0;
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  private closed: Promise<void>;

  constructor(env: NodeJS.ProcessEnv, cwd: string, entry = [path.resolve(__dirname, '../../../bin/agent-bridge-mcp.cjs')]) {
    this.child = spawn(process.execPath, entry, {
      cwd, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'],
    });
    this.child.stderr.on('data', chunk => {
      this.stderr = (this.stderr + chunk).slice(-32_000);
      if (env.MESH_MOCK_ROOT) fs.appendFileSync(path.join(env.MESH_MOCK_ROOT, `host-${this.child.pid}.stderr.log`), chunk);
    });
    const lines = createInterface({ input: this.child.stdout });
    lines.on('line', line => {
      let response: any;
      try { response = JSON.parse(line); }
      catch { this.rejectAll(new Error(`Non-JSON MCP stdout: ${line}`)); return; }
      const request = this.pending.get(response.id);
      if (!request) return;
      this.pending.delete(response.id);
      clearTimeout(request.timer);
      if (response.error) request.reject(new Error(response.error.message));
      else request.resolve(response.result);
    });
    this.child.once('error', error => this.rejectAll(error));
    this.child.stdin.on('error', error => this.rejectAll(error));
    this.closed = new Promise(resolve => this.child.once('close', () => {
      lines.close();
      this.rejectAll(new Error(`MCP host exited: ${this.stderr}`));
      resolve();
    }));
  }

  private rejectAll(error: Error): void {
    for (const request of this.pending.values()) {
      clearTimeout(request.timer);
      request.reject(error);
    }
    this.pending.clear();
  }

  async rpc(method: string, params: unknown = {}): Promise<any> {
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`MCP timeout: ${method}; ${this.stderr}`));
      }, 30_000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n', error => {
        if (error) this.rejectAll(error);
      });
    });
  }

  async initialize(): Promise<void> {
    await this.rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'mock-agent', version: '1' } });
    this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  }

  async tool(name: string, args: Record<string, unknown> = {}): Promise<any> {
    const result = await this.rpc('tools/call', { name, arguments: args });
    if (result.isError) throw new Error(JSON.stringify(result.content));
    const text = result.content?.find((item: any) => item.type === 'text')?.text;
    if (typeof text !== 'string') throw new Error(`Missing MCP tool payload: ${name}`);
    return JSON.parse(text);
  }

  async close(): Promise<void> {
    this.child.stdin.end();
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([this.closed, new Promise<never>((_, reject) => {
        timer = setTimeout(() => { this.child.kill(); reject(new Error(`Mock MCP shutdown timed out: ${this.stderr}`)); }, 15_000);
      })]);
      if (this.child.exitCode !== 0) throw new Error(`Mock MCP host failed: ${this.stderr}`);
    } finally { if (timer) clearTimeout(timer); }
  }
}
