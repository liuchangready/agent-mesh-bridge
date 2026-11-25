import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { createInterface } from 'node:readline';
import { test } from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import {
  availableMcpBuilds, discoverTaskMcpProcesses, McpControlReply, McpStatus, mcpBuildIsStale, mcpRestartBlockers,
  readMcpBuild, readMcpRecords, requestMcpControl,
} from '../bridge/standalone-mcp-status';
import { getProcessBirthIds } from '../bridge/platform';
import { createThread } from './unified-execution-fixture';

function status(reply: McpControlReply): McpStatus {
  assert.ok('status' in reply, 'error' in reply ? reply.error : 'missing status');
  return reply.status;
}

function fixtureBuild(root: string, version: string, tag: string) {
  const source = path.resolve(__dirname, '../..');
  fs.mkdirSync(path.join(root, 'bin'), { recursive: true });
  fs.cpSync(path.join(source, 'out', 'bridge'), path.join(root, 'out', 'bridge'), { recursive: true });
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({ name: 'agent-mesh-bridge', publisher: 'agent-mesh', version }));
  const bin = fs.readFileSync(path.join(source, 'bin', 'agent-bridge-mcp.cjs'), 'utf8');
  const customized = bin.replace('send(message);', `if (request?.method === 'ping') message.result.fixtureBuild = ${JSON.stringify(tag)};
    send(message);`).replace('const messages = [];', `const messages = [];
        if (request.params?.delayMs) await new Promise(resolve => setTimeout(resolve, request.params.delayMs));`);
  fs.writeFileSync(path.join(root, 'bin', 'agent-bridge-mcp.cjs'), customized +
    (tag === 'broken' ? '\nif (!isMainThread) throw new Error("Broken build fixture");\n' : ''));
  return readMcpBuild(root);
}

test('standalone MCP reports and swaps real builds without replacing the stdio connection', { timeout: 30_000 }, async t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-mcp-restart-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const home = path.join(root, 'home');
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(storage, { recursive: true });
  const extensionId = 'agent-mesh.agent-mesh-bridge';
  const initial = fixtureBuild(path.join(root, `${extensionId}-1.2.37`), '1.2.37', 'old');
  const child = spawn(process.execPath, [initial.entry, '--caller', 'codex'], {
    env: { ...process.env, HOME: home, USERPROFILE: home, AGENT_MESH_BRIDGE_HOME: storage },
    windowsHide: true,
    stdio: 'pipe',
  });
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  const exit = once(child, 'close');
  const lines = createInterface({ input: child.stdout });
  const messages: any[] = [];
  lines.on('line', line => { messages.push(JSON.parse(line)); });
  const waitFor = async <T>(read: () => T | undefined): Promise<T> => {
    const deadline = Date.now() + 8_000;
    while (Date.now() < deadline) {
      const value = read();
      if (value !== undefined) return value;
      await delay(20);
    }
    throw new Error(`MCP did not respond. stderr=${stderr}`);
  };
  const send = (message: any) => child.stdin.write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n');
  const response = (id: number | string) => waitFor(() => messages.find(message => message.id === id));
  try {
    send({ method: 'initialize', id: 1, params: { capabilities: { roots: {} } } });
    assert.equal((await response(1)).result.serverInfo.version, '1.2.37');
    send({ method: 'notifications/initialized' });
    assert.equal((await response('mesh-roots')).method, 'roots/list');
    send({ id: 'mesh-roots', result: { roots: [] } });
    const record = await waitFor(() => readMcpRecords(storage).find(row => row.instance.pid === child.pid));
    assert.equal(status(await requestMcpControl(record, { action: 'status' })).caller, 'codex');
    assert.equal(status(await requestMcpControl(record, { action: 'status' })).admission.ready, false);

    const sameVersion = fixtureBuild(initial.packageRoot, '1.2.37', 'rebuilt');
    assert.equal(mcpBuildIsStale(record.instance.build, sameVersion), true);
    const newest = fixtureBuild(path.join(root, `${extensionId}-1.2.38`), '1.2.38', 'new');
    assert.equal(availableMcpBuilds(initial.packageRoot, extensionId, [])[0]?.entry, newest.entry);

    send({ id: 2, method: 'ping', params: { delayMs: 900 } });
    let busy = false;
    for (let i = 0; i < 25; i++) {
      busy = status(await requestMcpControl(record, { action: 'status' })).pending > 0;
      if (busy) break;
      await delay(20);
    }
    assert.equal(busy, true);
    const blocked = await requestMcpControl(record, { action: 'restart', build: newest });
    assert.match('error' in blocked ? blocked.error : '', /请求正在处理/);
    assert.equal((await response(2)).result.fixtureBuild, 'old');

    const broken = fixtureBuild(path.join(root, 'broken'), '1.2.39', 'broken');
    const failed = await requestMcpControl(record, { action: 'restart', build: broken });
    assert.match('error' in failed ? failed.error : '', /Broken build fixture/);
    assert.equal(status(await requestMcpControl(record, { action: 'status' })).build.fingerprint, initial.fingerprint);
    send({ id: 3, method: 'ping' });
    assert.equal((await response(3)).result.fixtureBuild, 'old');

    const restarted = status(await requestMcpControl(record, { action: 'restart', build: newest }));
    assert.equal(restarted.build.fingerprint, newest.fingerprint);
    assert.equal(restarted.pid, child.pid);
    assert.equal(restarted.launcher, initial.entry);
    assert.equal(restarted.admission.ready, false);
    await waitFor(() => messages.find(message => message.method === 'notifications/tools/list_changed'));
    assert.equal(messages.filter(message => message.method === 'roots/list').length, 1);
    send({ id: 4, method: 'ping' });
    assert.equal((await response(4)).result.fixtureBuild, 'new');

    // Exercise the production gate with exact evidence in the isolated data root.
    const extension = path.join(newest.packageRoot, 'out', 'extension.js');
    fs.writeFileSync(extension, '');
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
    fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify({
      schemaVersion: 1, cutoverApproved: true, components: [
        { component: 'extension-host', path: extension },
        { component: 'standalone-mcp', path: newest.entry },
      ].map(component => ({ ...component, version: newest.version, packageRoot: newest.packageRoot,
        completeProcessInventory: true, previousProcesses: [] })),
    }));
    assert.equal(status(await requestMcpControl(record, { action: 'restart', build: newest })).admission.ready, true);
    const unapproved = fixtureBuild(path.join(root, 'unapproved'), '1.2.40', 'unapproved');
    const denied = await requestMcpControl(record, { action: 'restart', build: unapproved });
    assert.match('error' in denied ? denied.error : '', /新构建未通过 v2 启动准入/);
    assert.equal(status(await requestMcpControl(record, { action: 'status' })).build.fingerprint, newest.fingerprint);
    send({ id: 5, method: 'ping' });
    assert.equal((await response(5)).result.fixtureBuild, 'new');
  } finally {
    child.stdin.end();
    await Promise.race([exit, delay(3_000, undefined, { ref: false })]);
    if (child.exitCode === null && child.signalCode === null) {
      child.kill();
      await exit;
    }
  }
  assert.equal(child.exitCode, 0);
  assert.deepEqual(readMcpRecords(storage), []);
});

test('MCP status lists only processes owned by a V2 run', { timeout: 20_000 }, async t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-task-mcp-status-'));
  const entry = path.join(root, 'agent-bridge-mcp.cjs');
  fs.writeFileSync(entry, 'process.stdin.resume();');
  const ordinary = spawn(process.execPath, [entry]);
  const worker = spawn(process.execPath, ['-e', [
    "const { spawn } = require('node:child_process');",
    "const child = spawn(process.execPath, [process.argv[1]]);",
    "child.once('spawn', () => console.log(child.pid));",
    "process.stdin.once('end', () => child.stdin.end());",
    'process.stdin.resume();',
  ].join('\n'), entry]);
  const closed = Promise.all([once(ordinary, 'close'), once(worker, 'close')]);
  t.after(async () => {
    ordinary.stdin.end();
    worker.stdin.end();
    await closed;
    fs.rmSync(root, { recursive: true, force: true });
  });
  const lines = createInterface({ input: worker.stdout });
  const [line] = await once(lines, 'line');
  const mcpPid = Number(line);
  const workerPid = worker.pid!;
  const births = await getProcessBirthIds([workerPid, mcpPid]);
  assert.ok(ordinary.pid && births.get(workerPid) && births.get(mcpPid));

  const task = createThread('thread_owned_mcp_status', 'review');
  const run = task.execution!.run = {
    id: 'review-run', owner: { pid: process.pid },
    child: { pid: workerPid, birthId: births.get(workerPid) },
    state: 'active', step: 'executing', startedAt: task.createdAt,
  };
  assert.deepEqual((await discoverTaskMcpProcesses([task])).map(row => row.pid), [mcpPid]);

  run.child.birthId = 'stale-identity';
  assert.deepEqual(await discoverTaskMcpProcesses([task]), []);

  task.execution!.run.subtrees = [{ root: run.child, descendants: [{ pid: mcpPid, birthId: births.get(mcpPid) }] }];
  assert.deepEqual((await discoverTaskMcpProcesses([task])).map(row => row.pid), [mcpPid]);
});

test('MCP restart policy distinguishes idle v2 tasks from active, legacy and inconsistent ledger states', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-mcp-restart-policy-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const threads = path.join(root, 'threads');
  fs.mkdirSync(threads);
  assert.deepEqual(mcpRestartBlockers(root), []);
  fs.writeFileSync(path.join(root, 'delegation_threads.json'), '[]');
  const write = (threadId: string, fields: object) => {
    fs.writeFileSync(path.join(threads, `${threadId}.json`), JSON.stringify({
      threadId, status: 'paused', execution: { version: 2 }, ...fields,
    }));
  };
  write('paused_v2', {});
  write('queued_v2', { status: 'queued' });
  write('decision_v2', { status: 'wait_decision' });
  assert.deepEqual(mcpRestartBlockers(root), []);
  write('active_review', { status: 'reviewing', execution: { version: 2, run: { step: 'review', state: 'stopping' } } });
  write('active_merge', { status: 'merging' });
  write('cleanup', { status: 'completed', cleanup: { action: 'archive' } });
  write('legacy_paused', { execution: { version: 1 } });
  fs.writeFileSync(path.join(threads, 'corrupt.json'), '{');
  const blockers = mcpRestartBlockers(root).join('\n');
  for (const id of ['active_review', 'active_merge', 'cleanup', 'legacy_paused', 'corrupt.json']) assert.match(blockers, new RegExp(id));
  assert.doesNotMatch(blockers, /paused_v2|queued_v2|decision_v2/);
});
