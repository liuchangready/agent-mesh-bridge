import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import { WorkspaceRegistry, WorkspaceResolver, WindowWorkspace, findUniqueWindow, physicalDirectory, samePhysicalFile, groupWorkspaceTasks } from '../bridge/workspace';
import { getTaskExecutionPath, DelegationThread } from '../bridge/types';
import { testWorkspace } from './workspace-fixture';
import { TaskStore } from '../bridge/task-store';

test('workspace identity resolves aliases and rejects invalid directories', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-ws-'));
  try {
    const root = path.join(temp, 'project'); fs.mkdirSync(root);
    const link = path.join(temp, 'alias'); fs.symlinkSync(root, link, process.platform === 'win32' ? 'junction' : 'dir');
    const registry = new WorkspaceRegistry(path.join(temp, 'data'));
    const resolver = new WorkspaceResolver(registry);
    assert.equal(registry.register(root).id, registry.register(link).id);
    assert.throws(() => resolver.resolve(), /WORKSPACE_REQUIRED/);
    assert.throws(() => resolver.resolve('ws_' + 'a'.repeat(64)), /WORKSPACE_NOT_FOUND/);
    assert.throws(() => resolver.resolve(registry.register(root).id, temp), /WORKSPACE_MISMATCH/);
    if (process.platform === 'win32' && fs.existsSync(root.toUpperCase()) && samePhysicalFile(root, root.toUpperCase())) {
      assert.equal(registry.register(root.toUpperCase()).id, registry.register(root).id);
    }
    assert.throws(() => physicalDirectory('relative'), /WORKSPACE_PATH_INVALID/);
    assert.throws(() => physicalDirectory(path.join(temp, 'missing')), /WORKSPACE_PATH_INVALID/);
    if (process.platform !== 'win32') {
      const other = path.join(temp, 'PROJECT'); fs.mkdirSync(other);
      assert.notEqual(registry.register(root).id, registry.register(other).id);
    }
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

test('physical file identity distinguishes projects and detects hard links', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-scope-'));
  try {
    const a = path.join(temp, 'a'), b = path.join(temp, 'b'); fs.mkdirSync(a); fs.mkdirSync(b);
    const first = path.join(a, 'same.ts'), second = path.join(b, 'same.ts'), alias = path.join(b, 'alias.ts');
    fs.writeFileSync(first, 'a'); fs.writeFileSync(second, 'b'); fs.linkSync(first, alias);
    assert.equal(samePhysicalFile(first, second), false);
    assert.equal(samePhysicalFile(first, alias), true);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

test('concurrent processes register the same workspace once', async () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-ws-race-'));
  try {
    const modulePath = require.resolve('../bridge/workspace');
    const script = `const W=require(process.argv[1]); console.log(new W.WorkspaceRegistry(process.argv[2]).register(process.argv[3]).id)`;
    const run = () => new Promise<string>((resolve, reject) => {
      const child = spawn(process.execPath, ['-e', script, modulePath, path.join(temp, 'store'), temp]);
      let out = '', err = ''; child.stdout.on('data', (v) => out += v); child.stderr.on('data', (v) => err += v);
      child.on('error', reject); child.on('close', (code) => code ? reject(new Error(err)) : resolve(out.trim()));
    });
    const ids = await Promise.all(Array.from({ length: 8 }, run));
    assert.equal(new Set(ids).size, 1);
    assert.equal(new WorkspaceRegistry(path.join(temp, 'store')).list().length, 1);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

test('window adoption is local, requires successful dispatch, and never replaces a default', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-window-'));
  try {
    const first = new WindowWorkspace(temp, [temp]);
    const binding = { ...testWorkspace(temp), origin: { hostInstanceId: first.context.hostInstanceId, generation: first.context.generation } };
    assert.equal(findUniqueWindow(temp, physicalDirectory(temp))?.hostInstanceId, first.context.hostInstanceId);
    assert.equal(first.adopt(binding), false);
    const second = new WindowWorkspace(temp, [temp]);
    assert.equal(findUniqueWindow(temp, physicalDirectory(temp)), undefined);
    assert.equal(second.adopt({ ...binding, dispatchAccepted: true }), false);
    assert.equal(first.adopt({ ...binding, dispatchAccepted: true }), true);
    assert.equal(first.adopt({ ...binding, id: 'another', dispatchAccepted: true }), false);
    assert.equal(second.context.defaultWorkspaceId, undefined);
    first.dispose(); second.dispose();
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

test('execution path must agree with the isolated worktree', () => {
  const thread = { threadId: 't', workspace: testWorkspace('/one'), worktree: { path: '/two' } } as DelegationThread;
  assert.throws(() => getTaskExecutionPath(thread), /Invalid execution path/);
  thread.workspace.executionPath = '/two';
  assert.equal(getTaskExecutionPath(thread), '/two');
});

test('ledger refuses a missing or changed binding and filters before limiting', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-ws-store-'));
  const store = new TaskStore(temp);
  try {
    const make = (id: string, workspaceId: string, time: string) => ({
      threadId: id, title: id, initiatorAgent: 'codex', targetAgent: 'claude_code', status: 'queued',
      currentRound: 1, createdAt: time, updatedAt: time, receiverSessionId: 'session',
      workspace: { ...testWorkspace(temp), id: workspaceId },
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 }, rounds: [],
    } as DelegationThread);
    const first = make('thread_a', 'one', '2026-01-01T00:00:00Z');
    store.upsertTask(first);
    store.upsertTask(make('thread_b', 'two', '2026-01-02T00:00:00Z'));
    assert.equal(store.listTasks(1, 'all', 'one')[0].threadId, 'thread_a');
    assert.deepEqual(groupWorkspaceTasks(store.listTasks(10)).map((items) => items[0].workspace.id), ['two', 'one']);
    const changed = structuredClone(first); changed.workspace.id = 'two';
    assert.throws(() => store.upsertTask(changed), /binding cannot be changed/);
    const legacy = { ...make('thread_c', 'one', '2026-01-03T00:00:00Z'), workspace: undefined } as unknown as DelegationThread;
    assert.throws(() => store.upsertTask(legacy), /requires a valid workspace binding/);
  } finally { store.dispose(); fs.rmSync(temp, { recursive: true, force: true }); }
});

test('legacy aggregate and task files are not loaded or migrated', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-old-ledger-'));
  try {
    const old = { threadId: 'thread_legacy', workspacePath: temp };
    fs.writeFileSync(path.join(temp, 'delegation_threads.json'), JSON.stringify([old]));
    const aggregateOnly = new TaskStore(temp);
    try {
      assert.equal(aggregateOnly.getAllTasks().length, 0);
      assert.equal(fs.existsSync(path.join(temp, 'threads', 'thread_legacy.json')), false);
    } finally { aggregateOnly.dispose(); }
    fs.writeFileSync(path.join(temp, 'threads', 'thread_legacy.json'), JSON.stringify(old));
    const store = new TaskStore(temp);
    try {
      assert.equal(store.getAllTasks().length, 0);
      assert.deepEqual(JSON.parse(fs.readFileSync(path.join(temp, 'threads', 'thread_legacy.json'), 'utf8')), old);
    } finally { store.dispose(); }
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});
