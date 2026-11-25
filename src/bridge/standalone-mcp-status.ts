import { createHash } from 'crypto';
import { execFile } from 'child_process';
import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { promisify } from 'util';
import { compareVersions } from './build-staleness';
import { getProcessBirthIds, probeProcessIdentity } from './platform';
import { loadShutdownPreflightSnapshot } from './shutdown-preflight';
import type { DelegationThread, ProcessIdentity } from './types';

export interface McpBuild {
  packageRoot: string;
  entry: string;
  version: string;
  fingerprint: string;
  builtAtMs: number;
}

export interface McpInstance {
  pid: number;
  parentPid: number;
  caller?: string;
  profile?: string;
  node: string;
  transportStartedAt: string;
  loadedAt: string;
  launcher: string;
  build: McpBuild;
  admission: { ready: boolean; blockers: Array<{ code: string; evidence: string; threadId?: string }> };
}

export interface McpStatus extends McpInstance {
  pending: number;
  restarting: boolean;
  restartBlockers?: string[];
}

export interface McpRecord {
  instance: McpInstance;
  token: string;
}

export type McpControlRequest =
  | { action: 'status' | 'inspect' }
  | { action: 'restart'; build: McpBuild };

export type McpControlReply = { status: McpStatus } | { error: string };

export function readMcpBuild(packageRoot: string): McpBuild {
  packageRoot = fs.realpathSync(packageRoot);
  const manifest = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8'));
  if (manifest.name !== 'agent-mesh-bridge' || manifest.publisher !== 'agent-mesh' ||
      typeof manifest.version !== 'string' || !/^\d+\.\d+\.\d+(?:[-+][\w.-]+)?$/.test(manifest.version)) {
    throw new Error(`无效的 Agent Mesh Bridge 安装目录 ${packageRoot}`);
  }
  const entry = path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs');
  const bridge = path.join(packageRoot, 'out', 'bridge');
  const hash = createHash('sha256');
  let builtAtMs = 0;
  const addFile = (file: string) => {
    builtAtMs = Math.max(builtAtMs, fs.statSync(file).mtimeMs);
    hash.update(JSON.stringify([path.relative(packageRoot, file).replace(/\\/g, '/'),
      createHash('sha256').update(fs.readFileSync(file)).digest('hex')]));
  };
  const visit = (dir: string) => {
    for (const file of fs.readdirSync(dir, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const absolute = path.join(dir, file.name);
      if (file.isDirectory()) visit(absolute);
      else if (file.name.endsWith('.js')) addFile(absolute);
    }
  };
  hash.update(manifest.version);
  addFile(entry);
  visit(bridge);
  if (!fs.existsSync(path.join(bridge, 'standalone-mcp-transport.js'))) {
    throw new Error(`缺少可重启的 MCP 编译产物 ${bridge}`);
  }
  return { packageRoot, entry, version: manifest.version, fingerprint: hash.digest('hex'), builtAtMs };
}

export function mcpBuildIsStale(loaded: McpBuild, available: McpBuild): boolean {
  const order = compareVersions(available.version, loaded.version);
  return order > 0 || (order === 0 && loaded.fingerprint !== available.fingerprint);
}

export function availableMcpBuilds(extensionPath: string, extensionId: string, runningRoots: string[]): McpBuild[] {
  const roots = [extensionPath, ...runningRoots];
  if (path.basename(extensionPath).startsWith(`${extensionId}-`)) {
    try {
      const directory = path.dirname(extensionPath);
      for (const name of fs.readdirSync(directory)) {
        if (name.startsWith(`${extensionId}-`)) roots.push(path.join(directory, name));
      }
    } catch {}
  }
  return [...new Set(roots)].flatMap(root => {
    try { return [readMcpBuild(root)]; } catch { return []; }
  }).sort((left, right) => compareVersions(right.version, left.version) || right.builtAtMs - left.builtAtMs);
}

export function mcpStatusDirectory(storageDir: string): string {
  return path.join(storageDir, 'standalone-mcp');
}

export function mcpControlAddress(token: string): string {
  if (!/^[0-9a-f]{48}$/.test(token)) throw new Error('无效的 MCP 实例凭证');
  const id = createHash('sha256').update(token).digest('hex').slice(0, 32);
  return process.platform === 'win32'
    ? `\\\\.\\pipe\\agent-bridge-mcp-${id}`
    : path.join(os.tmpdir(), `agent-bridge-mcp-${id}.sock`);
}

export function readMcpRecords(storageDir: string): McpRecord[] {
  const dir = mcpStatusDirectory(storageDir);
  let names: string[];
  try { names = fs.readdirSync(dir); }
  catch (error: any) { if (error?.code === 'ENOENT') return []; throw error; }
  return names.filter(name => /^\d+\.json$/.test(name)).flatMap(name => {
    const pid = Number.parseInt(name, 10);
    if (probeProcessIdentity(pid) === 'exited') return [];
    try {
      const record = JSON.parse(fs.readFileSync(path.join(dir, name), 'utf8')) as McpRecord;
      if (record?.instance?.pid === pid && record.instance.build?.fingerprint && /^[0-9a-f]{48}$/.test(record.token)) {
        return [record];
      }
    } catch (error: any) {
      if (error?.code !== 'ENOENT') throw error;
    }
    return [];
  });
}

export function requestMcpControl(record: McpRecord, request: McpControlRequest): Promise<McpControlReply> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(mcpControlAddress(record.token));
    let reply = '';
    socket.setEncoding('utf8');
    socket.setTimeout(request.action === 'restart' ? 35_000 : request.action === 'inspect' ? 12_000 : 2_000);
    socket.once('connect', () => socket.write(JSON.stringify({ ...request, token: record.token }) + '\n'));
    socket.on('data', chunk => {
      reply += chunk;
      if (reply.length > 1024 * 1024) socket.destroy(new Error('MCP 状态响应过大'));
    });
    socket.once('timeout', () => socket.destroy(new Error('MCP 控制端点响应超时')));
    socket.once('error', reject);
    socket.once('close', () => reject(new Error('MCP 控制端点已断开')));
    socket.once('end', () => {
      try { resolve(JSON.parse(reply) as McpControlReply); }
      catch { reject(new Error('MCP 控制端点未返回有效响应')); }
    });
  });
}

export interface UnmanagedMcpProcess { pid: number; parentPid: number; launcher: string }

interface ProcessRow {
  pid: number;
  parentPid: number;
  created?: string;
  launcher?: string;
}

async function readProcessTable(): Promise<ProcessRow[]> {
  const run = promisify(execFile);
  const options = { windowsHide: true, timeout: 6_000, maxBuffer: 4 * 1024 * 1024 };
  if (process.platform === 'win32') {
    const script = String.raw`
      $ErrorActionPreference = 'Stop'; [Console]::OutputEncoding = [Text.Encoding]::UTF8
      $rows = @(Get-CimInstance Win32_Process -Property Name,CommandLine,ProcessId,ParentProcessId,CreationDate | ForEach-Object {
        $launcher = if ($_.Name -eq 'node.exe' -and $_.CommandLine -match '^\s*(?:"[^"]+"|\S+)\s+(?:"([^"]*[/\\]agent-bridge-mcp\.cjs)"|([^\s"]*[/\\]agent-bridge-mcp\.cjs))(?=\s|$)') {
          $Matches[1] + $Matches[2]
        }
        $created = if ($_.CreationDate) { $_.CreationDate.ToFileTimeUtc().ToString('D19') }
        [pscustomobject]@{ pid = $_.ProcessId; parentPid = $_.ParentProcessId; created = $created; launcher = $launcher }
      })
      ConvertTo-Json -InputObject $rows -Compress`;
    const { stdout } = await run('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', script], options);
    return JSON.parse(stdout);
  }
  const { stdout } = await run('ps', ['-eo', 'pid=,ppid=,comm=,args='], options);
  return stdout.split('\n').flatMap(line => {
    const match = /^\s*(\d+)\s+(\d+)\s+(\S+)\s+(.*)$/.exec(line);
    if (!match) return [];
    const [, pid, parentPid, command, args] = match;
    const launcher = /(?:^|\/)node$/.test(command)
      ? /^\S+\s+((?:\/|\.\.?\/)(?:.*?\/)?agent-bridge-mcp\.cjs)(?=\s|$)/.exec(args)?.[1]
      : undefined;
    return [{ pid: Number(pid), parentPid: Number(parentPid), launcher }];
  });
}

/** Show only stdio instances that belong to a recorded V2 task process tree. */
export async function discoverTaskMcpProcesses(tasks: readonly Pick<DelegationThread, 'execution'>[]): Promise<UnmanagedMcpProcess[]> {
  const recorded = new Map<number, Set<string>>();
  const add = (identity?: ProcessIdentity) => {
    if (!identity?.birthId) return;
    const births = recorded.get(identity.pid) ?? new Set<string>();
    births.add(identity.birthId);
    recorded.set(identity.pid, births);
  };
  for (const { execution } of tasks) {
    const run = execution?.version === 2 ? execution.run : undefined;
    if (!run) continue;
    add(run.child);
    for (const subtree of run.subtrees ?? []) {
      add(subtree.root);
      for (const descendant of subtree.descendants) add(descendant);
    }
  }
  if (!recorded.size) return [];

  const rows = await readProcessTable();
  const byPid = new Map(rows.map(row => [row.pid, row]));
  const births = await getProcessBirthIds([...recorded.keys()].filter(pid => byPid.has(pid)));
  const owned = new Set([...births].filter(([pid, birth]) => recorded.get(pid)?.has(birth)).map(([pid]) => pid));
  return rows.flatMap(row => {
    if (typeof row.launcher !== 'string' || !row.launcher) return [];
    const seen = new Set<number>();
    for (let current: ProcessRow | undefined = row; current && !seen.has(current.pid);) {
      if (owned.has(current.pid)) return [{ pid: row.pid, parentPid: row.parentPid, launcher: row.launcher }];
      seen.add(current.pid);
      const parent: ProcessRow | undefined = byPid.get(current.parentPid);
      // On Windows a surviving child can still report the PID of a parent that has since been recycled.
      if (process.platform === 'win32' && (typeof current.created !== 'string' || typeof parent?.created !== 'string' ||
          current.created < parent.created)) break;
      current = parent;
    }
    return [];
  });
}

/** A stdio runtime swap leaves the resident execution host and all task records in place. */
export function mcpRestartBlockers(storageDir: string): string[] {
  const snapshot = loadShutdownPreflightSnapshot(storageDir);
  // A fresh data root has no ledger yet. Admission still makes its own decision.
  const empty = snapshot.tasks.length === 0 &&
    !fs.existsSync(path.join(storageDir, 'delegation_threads.json'));
  const blockers = snapshot.readErrors
    .filter(error => !(empty && /^(threads\/|delegation_threads\.json): ENOENT/.test(error)))
    .map(error => `无法核验任务账本和进程记录 ${error}`);
  for (const task of snapshot.tasks) {
    const run = task.execution?.run;
    const integration = task.worktree?.integration;
    let reason: string | undefined;
    if (task.cleanup) reason = '任务正在清理或清理结果待确认';
    else if (run) reason = `运行尚未结算（${run.step} / ${run.state}）`;
    else if (integration?.phase === 'merging' || integration?.stashRecoveryPending || integration?.stashRecoveryLockHeld) {
      reason = 'Git 合入或恢复尚未结算';
    }
    else if (task.rounds?.some(round => round.nativeSessionCompactions?.some(operation =>
      operation.phase === 'prepared' || operation.phase === 'running'))) reason = '原生会话维护尚未结算';
    else if (task.status === 'running' || task.status === 'merging' || task.rounds?.some(round => round.status === 'running')) {
      reason = `仍有进行中的任务或轮次（${task.status}）`;
    } else if (task.execution?.version !== 2 && !['completed', 'failed'].includes(task.status)) {
      reason = `旧执行版本的任务尚未收尾（${task.status}）`;
    } else if (task.reviewBootstrap?.phase === 'launching') reason = '专用审阅者正在启动';
    if (reason) blockers.push(`${task.threadId} · ${reason}`);
  }
  for (const { path: transaction, phase } of snapshot.pendingMergeTransactions) {
    blockers.push(`Git 合入事务待结算 ${transaction}（${phase || '未知阶段'}）`);
  }
  for (const record of snapshot.pendingCoordination) {
    if (/^(bootstrap\/|wakes\/)/.test(record)) blockers.push(`旧版协调记录尚未结算 ${record}`);
  }
  for (const owner of snapshot.owners) {
    const active = [owner.pid, owner.childPid, owner.helperPid].some(pid => pid && probeProcessIdentity(pid) !== 'exited');
    if (active || owner.helperChildPids?.length || (!owner.pid && !owner.childPid && !owner.helperPid) ||
        (owner.source.includes('merge') && (!owner.helperPid || !owner.helperBirthId))) {
      blockers.push(`存在活动或无法核验的执行记录 ${owner.source}`);
    }
  }
  return blockers;
}
