import { randomBytes } from 'crypto';
import * as fs from 'fs';
import * as net from 'net';
import * as path from 'path';
import * as readline from 'readline';
import { Worker } from 'worker_threads';
import { compareVersions } from './build-staleness';
import {
  McpBuild, McpControlReply, McpInstance, McpStatus, mcpControlAddress,
  mcpRestartBlockers, mcpStatusDirectory, readMcpBuild,
} from './standalone-mcp-status';

interface RuntimeReply {
  key?: number;
  admission?: McpInstance['admission'];
  clientState?: unknown;
  messages?: unknown[];
}

function waitForWorker(
  worker: Worker, accept: (reply: RuntimeReply) => boolean, timeout: number,
): Promise<RuntimeReply> {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      worker.off('message', received);
      worker.off('exit', exited);
      worker.off('error', failed);
    };
    const failed = (error: Error) => { cleanup(); reject(error); };
    const exited = (code: number) => failed(new Error(`MCP 运行实例提前退出（${code}）`));
    const received = (reply: RuntimeReply) => {
      if (!accept(reply)) return;
      cleanup();
      resolve(reply);
    };
    const timer = setTimeout(() => failed(new Error('MCP 运行实例响应超时')), timeout);
    worker.on('message', received);
    worker.once('exit', exited);
    worker.once('error', failed);
  });
}

/** Keep the agent-owned pipes open while an idle runtime is replaced. */
export async function startMcpTransport(packageRoot: string, storageDir: string): Promise<void> {
  const startedAt = new Date().toISOString();
  const original = readMcpBuild(packageRoot);
  const workers = new Set<Worker>();
  const launch = (build: McpBuild, handshake: unknown[], replacement = false, clientState?: unknown) => {
    const worker = new Worker(build.entry, {
      workerData: { fingerprint: build.fingerprint, handshake, replacement, clientState },
      stdout: true,
    });
    // All protocol output travels over the worker message channel.
    worker.stdout.pipe(process.stderr, { end: false });
    workers.add(worker);
    worker.once('exit', () => workers.delete(worker));
    return worker;
  };
  let active = launch(original, []);
  const initial = await waitForWorker(active, reply => !!reply.admission, 25_000);
  let instance: McpInstance = {
    pid: process.pid, parentPid: process.ppid, caller: process.env.MESH_CALLER_AGENT,
    profile: process.env.MESH_MCP_PROFILE, node: process.version, transportStartedAt: startedAt,
    launcher: original.entry, loadedAt: new Date().toISOString(), build: original, admission: initial.admission!,
  };
  let sequence = 0;
  let restarting = false;
  let closed = false;
  let initialize: any;
  let initialized: any;
  let roots: any;
  const pending = new Set<number>();
  const rl = readline.createInterface({ input: process.stdin, terminal: false });
  const write = (message: unknown) => process.stdout.write(JSON.stringify(message) + '\n');

  const token = randomBytes(24).toString('hex');
  const address = mcpControlAddress(token);
  const directory = mcpStatusDirectory(storageDir);
  const marker = path.join(directory, `${process.pid}.json`);
  const temporary = `${marker}.${token}.tmp`;
  const publish = () => {
    try {
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      fs.writeFileSync(temporary, JSON.stringify({ instance, token }), { mode: 0o600 });
      fs.renameSync(temporary, marker);
    } catch (error) {
      console.error('[agent-bridge-mcp] Could not publish status:', error);
      try { fs.rmSync(temporary, { force: true }); } catch {}
    }
  };
  const status = (restartBlockers?: string[]): McpStatus => ({
    ...instance, pending: pending.size, restarting, restartBlockers,
  });
  const blockers = () => pending.size ? [`还有 ${pending.size} 个 MCP 请求正在处理，请等它们结束`] : mcpRestartBlockers(storageDir);
  const shutdown = () => {
    if (!closed || pending.size) return;
    for (const worker of workers) void worker.terminate();
  };
  const attach = (worker: Worker) => {
    worker.on('message', ({ key, messages }: RuntimeReply) => {
      if (key === undefined || !messages || !pending.delete(key)) return;
      for (const message of messages) write(message);
      shutdown();
    });
    worker.on('error', error => console.error('[agent-bridge-mcp] Runtime error:', error));
    worker.once('exit', () => {
      if (active !== worker || closed) return;
      process.exitCode = 1;
      pending.clear();
      process.stdin.pause();
      rl.close();
    });
  };
  attach(active);

  const restart = async (requested: McpBuild): Promise<McpControlReply> => {
    if (restarting) return { error: '此 MCP 实例正在重启' };
    restarting = true;
    let candidate: Worker | undefined;
    try {
      const reasons = blockers();
      if (reasons.length) throw new Error(reasons.join('\n'));
      const build = readMcpBuild(requested.packageRoot);
      if (build.fingerprint !== requested.fingerprint || build.entry !== requested.entry) {
        throw new Error('磁盘构建已变化，请刷新状态后重试');
      }
      if (compareVersions(build.version, instance.build.version) < 0) throw new Error('所选构建早于当前运行版本');
      const revision = sequence;
      const checkpoint = waitForWorker(active, reply => reply.key === 0 && 'clientState' in reply, 2_000);
      active.postMessage({ key: 0 });
      const { clientState } = await checkpoint;
      candidate = launch(build, [initialize, initialized, roots].filter(Boolean), true, clientState);
      const { admission } = await waitForWorker(candidate, reply => !!reply.admission, 22_000);
      if (closed || sequence !== revision || pending.size) throw new Error('MCP 客户端在重启准备期间有活动，请稍后重试');
      if (instance.admission.ready && !admission!.ready) {
        throw new Error('新构建未通过 v2 启动准入\n' + admission!.blockers.map(item => item.evidence).join('\n'));
      }
      const finalReasons = blockers();
      if (finalReasons.length) throw new Error(finalReasons.join('\n'));
      if (readMcpBuild(build.packageRoot).fingerprint !== build.fingerprint) throw new Error('新构建在读取期间发生变化，请重新检查');
      const previous = active;
      active = candidate;
      attach(active);
      candidate = undefined;
      instance = { ...instance, build, admission: admission!, loadedAt: new Date().toISOString() };
      publish();
      if (initialized) write({ jsonrpc: '2.0', method: 'notifications/tools/list_changed' });
      await previous.terminate();
      return { status: { ...status(), restarting: false } };
    } catch (error) {
      return { error: error instanceof Error ? error.message : String(error) };
    } finally {
      if (candidate) await candidate.terminate();
      restarting = false;
    }
  };

  const control = net.createServer(socket => {
    socket.on('error', () => {});
    socket.setEncoding('utf8');
    socket.setTimeout(2_000, () => socket.destroy());
    let input = '';
    const receive = (chunk: string) => {
      input += chunk;
      if (input.length > 32_768) { socket.destroy(); return; }
      if (!input.includes('\n')) return;
      socket.off('data', receive);
      socket.setTimeout(0);
      void (async () => {
        let reply: McpControlReply;
        try {
          const request = JSON.parse(input);
          if (request.token !== token || closed) { socket.destroy(); return; }
          if (request.action === 'status') reply = { status: status() };
          else if (request.action === 'inspect') reply = { status: status(restarting ? ['此 MCP 实例正在重启'] : blockers()) };
          else if (request.action === 'restart') reply = await restart(request.build);
          else reply = { error: '未知的 MCP 控制请求' };
        } catch (error) {
          reply = { error: error instanceof Error ? error.message : String(error) };
        }
        socket.end(JSON.stringify(reply));
      })();
    };
    socket.on('data', receive);
  });
  control.on('error', error => console.error('[agent-bridge-mcp] Status endpoint unavailable:', error));
  control.listen(address, () => {
    try {
      if (process.platform !== 'win32') fs.chmodSync(address, 0o600);
      publish();
    } catch (error) {
      console.error('[agent-bridge-mcp] Could not secure status endpoint:', error);
      control.close();
    }
  });
  rl.on('line', line => {
    if (!line.trim()) return;
    let request: any;
    try { request = JSON.parse(line); }
    catch {
      write({ jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } });
      return;
    }
    if (request?.method === 'initialize') initialize = request;
    if (request?.method === 'notifications/initialized') initialized = request;
    if (request?.id === 'mesh-roots' && !request.method) roots = request;
    const key = ++sequence;
    pending.add(key);
    active.postMessage({ key, request });
  });
  rl.once('close', () => {
    closed = true;
    control.close();
    try { fs.rmSync(marker, { force: true }); } catch {}
    shutdown();
  });
}
