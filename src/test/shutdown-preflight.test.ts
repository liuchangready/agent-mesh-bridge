import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawnSync, execFileSync } from 'node:child_process';
import { inspectShutdownPreflight, inspectV2RuntimeActivation, loadShutdownPreflightSnapshot } from '../bridge/shutdown-preflight';
import { writeReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { getProcessBirthId, probeProcessIdentity } from '../bridge/platform';
import { MeshFileLock } from '../bridge/mesh-file-lock';

const mutableFs = require('node:fs') as typeof fs;

const matchingVersions = [
  { component: 'extension-host', version: '1.2.36', source: 'test inventory' },
  { component: 'standalone-mcp', version: '1.2.36', source: 'test inventory' },
];

function snapshotTree(root: string): Map<string, string> {
  const result = new Map<string, string>();
  const visit = (dir: string) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      const relative = path.relative(root, file).replace(/\\/g, '/');
      result.set(relative, entry.isDirectory() ? '<dir>' : fs.readFileSync(file).toString('base64'));
      if (entry.isDirectory()) visit(file);
    }
  };
  visit(root);
  return result;
}

function startHeldNode(): ReturnType<typeof import('node:child_process').spawn> {
  const { spawn } = require('node:child_process');
  return spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], { stdio: 'ignore', windowsHide: true });
}

async function stopChild(child: ReturnType<typeof startHeldNode>): Promise<void> {
  if (!child.pid || child.exitCode !== null || child.signalCode !== null) return;
  const closed = new Promise<void>((resolve) => child.once('close', () => resolve()));
  try { child.kill(); } catch {}
  await Promise.race([closed, new Promise<void>((resolve) => setTimeout(resolve, 5000))]);
}

function runPreflightCli(home: string, version = '1.2.36') {
  const entry = path.resolve(__dirname, '../../bin/agent-bridge-preflight.cjs');
  return spawnSync(process.execPath, [entry, '--component', `extension-host=${version}`], {
    encoding: 'utf8', windowsHide: true,
    env: { ...process.env, USERPROFILE: home, HOME: home },
  });
}

function runStartupPreflightCli(home: string) {
  const entry = path.resolve(__dirname, '../../bin/agent-bridge-preflight.cjs');
  return spawnSync(process.execPath, [entry, '--startup-admission'], {
    encoding: 'utf8', windowsHide: true,
    env: { ...process.env, USERPROFILE: home, HOME: home },
  });
}

test('shutdown preflight is read-only and reports paused, decisions, pending wake, unfinished work, and mixed hosts', () => {
  const storage = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-shutdown-preflight-'));
  try {
    const threadsDir = path.join(storage, 'threads');
    fs.mkdirSync(threadsDir);
    fs.writeFileSync(path.join(threadsDir, 'fixture.json'), JSON.stringify({ threadId: 'fixture', status: 'paused', execution: { version: 1 } }));
    const before = fs.readFileSync(path.join(threadsDir, 'fixture.json'));
    const snapshot = loadShutdownPreflightSnapshot(storage);
    const report = inspectShutdownPreflight({
      ...snapshot,
      expectedVersion: '1.2.36',
      components: [matchingVersions[0], { ...matchingVersions[1], version: '1.2.35' }],
    });
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item) => item.code === 'PAUSED_TASK' && item.threadId === 'fixture'));
    assert.ok(report.blockers.some((item) => item.code === 'UNDELIVERED_TASK' && item.threadId === 'fixture'));
    assert.ok(report.blockers.some((item) => item.code === 'MIXED_HOST_VERSIONS'));
    assert.deepEqual(fs.readFileSync(path.join(threadsDir, 'fixture.json')), before);
    assert.deepEqual(fs.readdirSync(storage), ['threads'], 'inspection must not create coordination or registry directories');
  } finally { fs.rmSync(storage, { recursive: true, force: true }); }
});

test('shutdown preflight identifies wait decision, pending wake, live terminal subtree, and unknown process identity', () => {
  const base = { threadId: 'sample', status: 'completed', createdAt: '', updatedAt: '', rounds: [], summary: {} } as any;
  const tasks = [
    { ...base, threadId: 'decision', status: 'wait_decision' },
    { ...base, threadId: 'wake', reviewWake: { round: 1, attempts: 1, lastAttemptAt: '', lastError: '', autoRetryAt: new Date().toISOString() } },
    { ...base, threadId: 'live', execution: { version: 2, run: {
      id: 'run-live', state: 'stopping', step: 'merge', owner: { pid: 10, birthId: 'owner' },
      child: { pid: 11, birthId: 'child' }, subtrees: [{ root: { pid: 11, birthId: 'child' }, descendants: [{ pid: 12, birthId: 'grandchild' }] }],
    } } },
    { ...base, threadId: 'unknown', execution: { version: 2, run: { id: 'run-unknown', state: 'stopping', step: 'develop', owner: { pid: 13 } } } },
  ];
  const report = inspectShutdownPreflight({
    tasks,
    expectedVersion: '1.2.36',
    components: matchingVersions,
    probe: (pid) => pid === 10 || pid === 11 || pid === 12 ? 'alive' : 'unknown',
  });
  assert.ok(report.blockers.some((item) => item.code === 'WAITING_FOR_DECISION'));
  assert.ok(report.blockers.some((item) => item.code === 'PENDING_WAKE'));
  assert.ok(report.blockers.some((item) => item.code === 'PROCESS_STILL_ALIVE' && item.threadId === 'live'));
  assert.ok(report.blockers.some((item) => item.code === 'PROCESS_IDENTITY_UNVERIFIED' && item.threadId === 'unknown'));
  assert.ok(report.blockers.some((item) => item.code === 'RUN_RECORD_REMAINS' && item.threadId === 'live'));
});

test('shutdown preflight fails closed when component version evidence is missing', () => {
  const report = inspectShutdownPreflight({ tasks: [], expectedVersion: '1.2.36', components: [] });
  assert.equal(report.ready, false);
  assert.deepEqual(report.blockers.map((item) => item.evidence).sort(), [
    'extension-host version is missing or ambiguous.',
    'standalone-mcp version is missing or ambiguous.',
  ]);
});

test('shutdown preflight reports old pending wake, bootstrap credential, and Bridge lease files', () => {
  const storage = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-shutdown-coordination-'));
  try {
    fs.mkdirSync(path.join(storage, 'threads'));
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
    const pending = path.join(storage, 'wakes', 'caller', 'pending'); fs.mkdirSync(pending, { recursive: true });
    fs.writeFileSync(path.join(pending, 'review.json'), '{}');
    fs.mkdirSync(path.join(storage, 'bootstrap'));
    fs.writeFileSync(path.join(storage, 'bootstrap', 'thread.json'), '{}');
    fs.mkdirSync(path.join(storage, 'roles'));
    fs.writeFileSync(path.join(storage, 'roles', 'thread.json'), JSON.stringify({ pid: 23 }));
    const report = inspectShutdownPreflight({
      ...loadShutdownPreflightSnapshot(storage), expectedVersion: '1.2.36', components: matchingVersions,
      probe: () => 'unknown',
    });
    assert.ok(report.blockers.some((item) => item.code === 'LEGACY_COORDINATION_REMAINS' && item.evidence.includes('wakes/caller/pending/review.json')));
    assert.ok(report.blockers.some((item) => item.code === 'LEGACY_COORDINATION_REMAINS' && item.evidence.includes('bootstrap/thread.json')));
    assert.ok(report.blockers.some((item) => item.code === 'LEGACY_COORDINATION_REMAINS' && item.evidence.includes('roles/thread.json')));
    assert.ok(report.blockers.some((item) => item.code === 'PROCESS_IDENTITY_UNVERIFIED' && item.evidence.includes('roles/thread.json')));
  } finally { fs.rmSync(storage, { recursive: true, force: true }); }
});

test('standalone preflight CLI is read-only and emits blocking evidence for missing ledger and host inventory', () => {
  const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-cli-'));
  const entry = path.resolve(__dirname, '../../bin/agent-bridge-preflight.cjs');
  try {
    const result = spawnSync(process.execPath, [entry, '--component', 'extension-host=1.2.36'], {
      encoding: 'utf8', windowsHide: true,
      env: { ...process.env, USERPROFILE: isolatedHome, HOME: isolatedHome },
    });
    assert.equal(result.status, 2, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'PREFLIGHT_READ_ERROR' && item.evidence.includes('threads/')));
    assert.ok(report.blockers.some((item: any) => item.code === 'PREFLIGHT_READ_ERROR' && item.evidence.includes('delegation_threads.json')));
    assert.equal(fs.readdirSync(isolatedHome).length, 0, 'preflight must not create Bridge state under the inspected home');
  } finally { fs.rmSync(isolatedHome, { recursive: true, force: true }); }
});

test('standalone preflight CLI blocks an exited lock owner while its exact merge helper is alive', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-merge-helper-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const ownerProcess = startHeldNode();
  const ownerPid = ownerProcess.pid!;
  const ownerBirthId = getProcessBirthId(ownerPid)!;
  await stopChild(ownerProcess);
  const helper = startHeldNode();
  const helperPid = helper.pid!;
  const helperBirthId = getProcessBirthId(helperPid)!;
  const token = '12345678-1234-4123-8123-123456789abc';
  const channel = path.join(storage, `merge.lock.${token}`);
  fs.writeFileSync(`${channel}.helper-pid`, String(helperPid));
  fs.writeFileSync(`${channel}.helper-birth`, helperBirthId);
  fs.writeFileSync(path.join(storage, 'merge.lock.owner.json'), JSON.stringify({
    token, pid: ownerPid, birthId: ownerBirthId, helperPid, helperBirthId, acquiredAt: Date.now(),
  }));
  try {
    const before = snapshotTree(storage);
    const result = runPreflightCli(home);
    assert.equal(result.status, 2, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_HELPER_STILL_ALIVE' &&
      item.evidence.includes(`${helperPid}/${helperBirthId}`)));
    assert.deepEqual(snapshotTree(storage), before, 'preflight must not alter lock evidence or bridge files');
  } finally {
    await stopChild(helper);
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('standalone preflight blocks unpublished helper channels, missing helper birth IDs and retained validation child ACKs', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-orphan-helper-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const helper = startHeldNode();
  const child = startHeldNode();
  const helperPid = helper.pid!;
  const helperBirthId = getProcessBirthId(helperPid)!;
  const childPid = child.pid!;
  const token = 'abcdefab-cdef-4abc-8def-abcdefabcdef';
  const channel = path.join(storage, `merge.lock.${token}`);
  // The helper channel has no published owner. A child ACK remains after a
  // helper crash; because the ACK schema has no birthId, it is a blocker even
  // when a separate process currently proves the PID is alive.
  fs.writeFileSync(`${channel}.helper-pid`, String(helperPid));
  fs.writeFileSync(`${channel}.helper-birth`, helperBirthId);
  fs.writeFileSync(`${channel}.child.${childPid}`, 'tracked');
  const secondToken = 'bbbbbbbb-cccc-4ddd-8eee-ffffffffffff';
  const secondChannel = path.join(storage, `merge.lock.${secondToken}`);
  fs.writeFileSync(`${secondChannel}.helper-pid`, String(helperPid));
  fs.writeFileSync(path.join(storage, `merge.lock.${secondToken}.mutex-helper.ps1`), '# unpublished launch helper');
  const thirdToken = 'cccccccc-dddd-4eee-8fff-000000000001';
  fs.writeFileSync(path.join(storage, `merge.lock.owner.json.${process.pid}.${thirdToken}.tmp`), '{"incomplete":');
  try {
    const result = runPreflightCli(home);
    assert.equal(result.status, 2, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_LOCK_CHILD_UNVERIFIED' && item.evidence.includes(String(childPid))));
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_HELPER_STILL_ALIVE'));
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_HELPER_IDENTITY_UNVERIFIED'),
      'helper channel without a birth ID must fail closed');
  } finally {
    await Promise.all([stopChild(helper), stopChild(child)]);
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('standalone preflight blocks the real MeshFileLock helper before owner publication', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-real-helper-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const lock = new MeshFileLock(path.join(storage, 'merge.lock'));
  const originalRename = mutableFs.renameSync;
  const ownerFile = `${lock.lockPath}.owner.json`;
  let reportResult: ReturnType<typeof runPreflightCli> | undefined;
  mutableFs.renameSync = function (source: fs.PathLike, destination: fs.PathLike, ...args: any[]) {
    if (!reportResult && String(destination) === ownerFile) {
      // The real helper already owns the OS lock, but the atomic owner rename
      // has not happened. Run the production CLI inside that exact window.
      reportResult = runPreflightCli(home);
    }
    return (originalRename as any).call(mutableFs, source, destination, ...args);
  };
  let release: (() => void) | undefined;
  try {
    release = await lock.acquire();
    assert.ok(release);
    assert.ok(reportResult, 'hook observed the real owner-publication window');
    assert.equal(reportResult!.status, 2, reportResult!.stdout || reportResult!.stderr);
    const report = JSON.parse(reportResult!.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_HELPER_STILL_ALIVE'));
  } finally {
    mutableFs.renameSync = originalRename;
    if (release) await lock.releaseAndConfirm(release);
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('standalone preflight blocks a released helper channel that still records a validation child', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-helper-child-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const owner = startHeldNode();
  const helper = startHeldNode();
  const child = startHeldNode();
  const ownerPid = owner.pid!; const ownerBirthId = getProcessBirthId(ownerPid)!;
  const helperPid = helper.pid!; const helperBirthId = getProcessBirthId(helperPid)!;
  const childPid = child.pid!;
  await Promise.all([stopChild(owner), stopChild(helper)]);
  const token = 'dddddddd-eeee-4fff-8000-111111111111';
  const channel = path.join(storage, `merge.lock.${token}`);
  fs.writeFileSync(`${channel}.helper-pid`, String(helperPid));
  fs.writeFileSync(`${channel}.helper-birth`, helperBirthId);
  fs.writeFileSync(`${channel}.released`, `${helperPid}:${helperBirthId}`);
  fs.writeFileSync(`${channel}.child.${childPid}`, 'tracked');
  fs.writeFileSync(path.join(storage, 'merge.lock.owner.json'), JSON.stringify({ token, pid: ownerPid, birthId: ownerBirthId,
    helperPid, helperBirthId, acquiredAt: Date.now() }));
  try {
    const result = runPreflightCli(home);
    assert.equal(result.status, 2, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'MERGE_LOCK_CHILD_UNVERIFIED' && item.evidence.includes(String(childPid))));
  } finally {
    await stopChild(child);
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('preflight treats a reused PID as the exited recorded helper only with its exact release acknowledgement', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-pid-reuse-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const oldOwner = startHeldNode();
  const oldOwnerPid = oldOwner.pid!;
  const oldOwnerBirthId = getProcessBirthId(oldOwnerPid)!;
  await stopChild(oldOwner);
  const reusedPid = process.pid;
  const actualBirthId = getProcessBirthId(reusedPid)!;
  const oldHelperBirthId = `${actualBirthId}0`;
  assert.notEqual(oldHelperBirthId, actualBirthId);
  assert.equal(probeProcessIdentity(reusedPid, oldHelperBirthId), 'exited', 'the PID is live but belongs to another process instance');
  const token = 'eeeeeeee-ffff-4000-8111-222222222222';
  const channel = path.join(storage, `merge.lock.${token}`);
  fs.writeFileSync(`${channel}.helper-pid`, String(reusedPid));
  fs.writeFileSync(`${channel}.helper-birth`, oldHelperBirthId);
  fs.writeFileSync(`${channel}.released`, `${reusedPid}:${oldHelperBirthId}`);
  fs.writeFileSync(path.join(storage, 'merge.lock.owner.json'), JSON.stringify({ token, pid: oldOwnerPid, birthId: oldOwnerBirthId,
    helperPid: reusedPid, helperBirthId: oldHelperBirthId, acquiredAt: Date.now() }));
  try {
    const result = runPreflightCli(home);
    assert.equal(result.status, 0, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, true);
    assert.deepEqual(report.blockers, []);
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});

test('standalone preflight sees a real task-local bootstrap credential on terminal v1 history and is byte-read-only', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-task-bootstrap-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const threads = path.join(storage, 'threads');
  const artifactDir = path.join(storage, 'tasks', 'terminal-v1');
  fs.mkdirSync(threads, { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const task = {
    threadId: 'terminal-v1', status: 'completed', reviewFlow: 'dedicated', artifactDir,
    createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(), execution: { version: 1 },
    rounds: [], summary: {},
  };
  fs.writeFileSync(path.join(threads, `${task.threadId}.json`), JSON.stringify(task));
  assert.equal(writeReviewerBootstrapToken({ threadId: task.threadId, token: 'fresh-bootstrap-token', agent: 'codex', createdAt: new Date().toISOString() }, storage, artifactDir), true);
  try {
    const before = snapshotTree(storage);
    const result = runPreflightCli(home);
    assert.equal(result.status, 2, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item: any) => item.code === 'LEGACY_COORDINATION_REMAINS' &&
      item.evidence.includes('tasks/terminal-v1/bootstrap/terminal-v1.json')));
    assert.deepEqual(snapshotTree(storage), before, 'preflight must preserve bytes, paths, and token files');
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});

test('preflight blocks terminal rows that retain in-flight rounds, unanswered decisions, or Git recovery transactions', () => {
  const storage = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-recovery-'));
  try {
    fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
    fs.mkdirSync(path.join(storage, 'merge-transactions'));
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), JSON.stringify([{ threadId: 'terminal-live-round' }]));
    fs.writeFileSync(path.join(storage, 'merge-transactions', 'terminal-live-round.json'), JSON.stringify({
      threadId: 'terminal-live-round', phase: 'stash_cleanup_started', stashCleanupPending: true,
    }));
    fs.writeFileSync(path.join(storage, 'threads', 'terminal-live-round.json'), JSON.stringify({
      threadId: 'terminal-live-round', status: 'completed', rounds: [{ round: 4, type: 'review', status: 'running' }],
      decision: { id: 'decision-live', phase: 'review' }, execution: { version: 1 }, summary: {},
    }));
    const snapshot = loadShutdownPreflightSnapshot(storage);
    const report = inspectShutdownPreflight({ ...snapshot, expectedVersion: '1.2.36', components: matchingVersions });
    assert.ok(report.blockers.some((item) => item.code === 'IN_FLIGHT_ROUND'));
    assert.ok(report.blockers.some((item) => item.code === 'PENDING_DECISION'));
    assert.ok(report.blockers.some((item) => item.code === 'GIT_RECOVERY_PENDING'));
  } finally { fs.rmSync(storage, { recursive: true, force: true }); }
});

test('standalone preflight has a green, read-only CLI path when all required evidence is clean', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-preflight-green-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  try {
    const before = snapshotTree(storage);
    const result = runPreflightCli(home);
    assert.equal(result.status, 0, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.ready, true);
    assert.deepEqual(report.blockers, []);
    assert.deepEqual(snapshotTree(storage), before, 'green preflight is also read-only');
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});

function runtimeInventory(packageRoot: string, version: string): any {
  return {
    schemaVersion: 1,
    cutoverApproved: true,
    components: [
      { component: 'extension-host', version, packageRoot,
        path: path.join(packageRoot, 'out', 'extension.js'), completeProcessInventory: true, previousProcesses: [] },
      { component: 'standalone-mcp', version, packageRoot,
        path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs'), completeProcessInventory: true, previousProcesses: [] },
    ],
  };
}

test('runtime activation admits valid v2 recovery but requires exact component path/version and exited old process evidence', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-runtime-activation-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  const components = [
    { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
    { component: 'standalone-mcp', version, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs') },
  ];
  const writeInventory = (value: unknown) => fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify(value));
  try {
    const missing = inspectV2RuntimeActivation({ ...loadShutdownPreflightSnapshot(storage), storageDir: storage, components });
    assert.equal(missing.ready, false);
    assert.ok(missing.blockers.some((item) => item.code === 'RUNTIME_COMPONENT_INVENTORY_UNVERIFIED'));

    writeInventory(runtimeInventory(packageRoot, version));
    const v2Task = { threadId: 'v2-daily-recovery', status: 'running', execution: { version: 2, run: {
      id: 'run-survives-v2-restart', owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) }, state: 'active', step: 'develop',
    }, next: { phase: 'review', round: 1, sourceRunId: 'run-survives-v2-restart' } }, rounds: [], summary: {} } as any;
    const v2RoleSource = 'roles/v2-daily-recovery.json';
    const valid = inspectV2RuntimeActivation({ tasks: [v2Task], owners: [
      { pid: process.pid, birthId: getProcessBirthId(process.pid), threadId: v2Task.threadId, source: v2RoleSource },
    ], readErrors: [], pendingCoordination: [v2RoleSource],
      pendingMergeTransactions: [{ threadId: v2Task.threadId, path: 'merge-transactions/v2-daily-recovery.json', phase: 'stash_created' }],
      storageDir: storage, components });
    assert.equal(valid.ready, true, JSON.stringify(valid.blockers));

    const oldV1 = inspectV2RuntimeActivation({ tasks: [{ threadId: 'active-v1', status: 'running', execution: { version: 1 }, rounds: [], summary: {} } as any],
      owners: [], readErrors: [], pendingCoordination: [], pendingMergeTransactions: [], storageDir: storage, components });
    assert.ok(oldV1.blockers.some((item) => item.code === 'HISTORICAL_TASK_NOT_DRAINED'));

    const mismatch = runtimeInventory(packageRoot, version);
    mismatch.components[1].path = path.join(packageRoot, 'bin', 'different-mcp.cjs');
    writeInventory(mismatch);
    const pathBlocked = inspectV2RuntimeActivation({ tasks: [], owners: [], readErrors: [], pendingCoordination: [],
      pendingMergeTransactions: [], storageDir: storage, components });
    assert.ok(pathBlocked.blockers.some((item) => item.code === 'RUNTIME_COMPONENT_PATH_MISMATCH'));

    const old = startHeldNode();
    const previous = { pid: old.pid!, birthId: getProcessBirthId(old.pid!)!, version: '1.2.35', path: 'C:\\old-agent-bridge\\out\\extension.js' };
    const liveInventory = runtimeInventory(packageRoot, version);
    liveInventory.components[0].previousProcesses = [previous];
    writeInventory(liveInventory);
    const liveBlocked = inspectV2RuntimeActivation({ tasks: [], owners: [], readErrors: [], pendingCoordination: [],
      pendingMergeTransactions: [], storageDir: storage, components });
    assert.ok(liveBlocked.blockers.some((item) => item.code === 'RUNTIME_OLD_PROCESS_ALIVE'));
    await stopChild(old);
    const exited = inspectV2RuntimeActivation({ tasks: [], owners: [], readErrors: [], pendingCoordination: [],
      pendingMergeTransactions: [], storageDir: storage, components });
    assert.equal(exited.ready, true, JSON.stringify(exited.blockers));
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});

test('runtime admission inspects terminal legacy run identities, next and cleanup even without registry files', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-runtime-legacy-run-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  const components = [
    { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
    { component: 'standalone-mcp', version, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs') },
  ];
  const owner = startHeldNode();
  const child = startHeldNode();
  const token = 'legacy-live-run';
  fs.mkdirSync(storage, { recursive: true });
  fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify(runtimeInventory(packageRoot, version)));
  try {
    const task = { threadId: 'completed-v1-still-live', status: 'completed', execution: {
      version: 1, next: { phase: 'review', round: 2, sourceRunId: 'old-run' },
      run: { id: 'old-run', owner: { pid: owner.pid, birthId: getProcessBirthId(owner.pid!) },
        child: { pid: child.pid, birthId: getProcessBirthId(child.pid!) },
        subtrees: [{ root: { pid: child.pid, birthId: getProcessBirthId(child.pid!) }, descendants: [] }], state: 'stopping', step: 'review' },
    }, cleanup: { token, action: 'delete', owner: { pid: owner.pid, birthId: getProcessBirthId(owner.pid!) }, startedAt: new Date().toISOString() },
    rounds: [], summary: {} } as any;
    const report = inspectV2RuntimeActivation({ tasks: [task], owners: [], readErrors: [], pendingCoordination: [],
      pendingMergeTransactions: [], storageDir: storage, components });
    assert.equal(report.ready, false);
    assert.ok(report.blockers.some((item) => item.code === 'RUNTIME_OLD_PROCESS_ALIVE' && item.threadId === task.threadId));
    assert.ok(report.blockers.some((item) => item.code === 'RUN_RECORD_REMAINS' && item.threadId === task.threadId));
    assert.ok(report.blockers.some((item) => item.code === 'PENDING_HANDOFF' && item.threadId === task.threadId));
    assert.ok(report.blockers.some((item) => item.code === 'CLEANUP_IN_PROGRESS' && item.threadId === task.threadId));
    const unknown = structuredClone(task);
    unknown.execution.run.owner.birthId = 'not-the-current-birth-id';
    const unknownReport = inspectV2RuntimeActivation({ tasks: [unknown], owners: [], readErrors: [], pendingCoordination: [],
      pendingMergeTransactions: [], storageDir: storage, components, probe: (pid) => pid === owner.pid ? 'unknown' : probeProcessIdentity(pid) });
    assert.ok(unknownReport.blockers.some((item) => item.code === 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED'));
  } finally { void Promise.all([stopChild(owner), stopChild(child)]); fs.rmSync(home, { recursive: true, force: true }); }
});

test('runtime admission recognizes a completed legacy merge only when its fixed ref contains the approved commit', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-runtime-legacy-merge-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const repository = path.join(home, 'repo');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  const git = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
  const runGit = (...args: string[]) => execFileSync(git, ['-C', repository, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true }).trim();
  fs.mkdirSync(repository, { recursive: true });
  const previousGit = process.env.MESH_GIT_PATH;
  process.env.MESH_GIT_PATH = git;
  try {
    runGit('init', '-b', 'main');
    runGit('config', 'user.name', 'Preflight Test');
    runGit('config', 'user.email', 'preflight@example.test');
    fs.writeFileSync(path.join(repository, 'base.txt'), 'base\n');
    runGit('add', 'base.txt'); runGit('commit', '-m', 'base');
    const oldTarget = runGit('rev-parse', 'HEAD');
    runGit('checkout', '-b', 'mesh/task/legacy-delivered');
    fs.writeFileSync(path.join(repository, 'delivered.txt'), 'delivered\n');
    runGit('add', 'delivered.txt'); runGit('commit', '-m', 'approved candidate');
    const approved = runGit('rev-parse', 'HEAD');
    runGit('checkout', 'main'); runGit('merge', '--no-ff', 'mesh/task/legacy-delivered', '-m', 'merge approved candidate');
    const merged = runGit('rev-parse', 'HEAD');
    const storageThreads = path.join(storage, 'threads'); fs.mkdirSync(storageThreads, { recursive: true });
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
    fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify(runtimeInventory(packageRoot, version)));
    const task: any = { threadId: 'completed-v1-merged', status: 'completed', execution: { version: 1 }, rounds: [], summary: {},
      worktree: { path: repository, mainPath: repository, repositoryPath: repository, targetRef: 'refs/heads/main',
        taskBranch: 'mesh/task/legacy-delivered', baseCommit: oldTarget, approvedCommit: approved, mergePending: false,
        integration: { phase: 'merged', targetOldCommit: oldTarget, targetCommit: merged, mergedCommit: merged,
          stashRecoveryPending: false, stashRecoveryLockHeld: false, updatedAt: new Date().toISOString() } } };
    fs.writeFileSync(path.join(storageThreads, `${task.threadId}.json`), JSON.stringify(task));
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), JSON.stringify([task]));
    const before = snapshotTree(storage);
    const accepted = runStartupPreflightCli(home);
    assert.equal(accepted.status, 0, accepted.stdout || accepted.stderr);
    assert.equal(JSON.parse(accepted.stdout).startupAdmission.ready, true);
    assert.deepEqual(snapshotTree(storage), before, 'startup admission must be read-only after a verified real merge');

    task.threadId = 'completed-v1-unmerged-approval';
    task.worktree.integration = { phase: 'ready', updatedAt: new Date().toISOString() };
    fs.writeFileSync(path.join(storageThreads, `${task.threadId}.json`), JSON.stringify(task));
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), JSON.stringify([JSON.parse(fs.readFileSync(path.join(storageThreads, 'completed-v1-merged.json'), 'utf8')), task]));
    const blocked = runStartupPreflightCli(home);
    assert.equal(blocked.status, 2, blocked.stdout || blocked.stderr);
    assert.ok(JSON.parse(blocked.stdout).startupAdmission.blockers.some((item: any) => item.code === 'HISTORICAL_TASK_NOT_DRAINED'));
  } finally {
    if (previousGit === undefined) delete process.env.MESH_GIT_PATH; else process.env.MESH_GIT_PATH = previousGit;
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('runtime admission associates a live v2 merge owner and helper with its exact run and channel', async () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-runtime-v2-helper-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  const ownerProcess = startHeldNode();
  const helperProcess = startHeldNode();
  const validationProcess = startHeldNode();
  const token = 'exact-v2-merge-token';
  const channelPath = path.join(storage, `merge.lock.${token}`);
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify(runtimeInventory(packageRoot, version)));
  const writeTaskAndOwner = (helperPid: number, helperBirthId: string) => {
    const task = { threadId: 'v2-active-merge-helper', status: 'merging', execution: { version: 2, phase: 'merge',
      run: { id: 'v2-merge-run', owner: { pid: ownerProcess.pid, birthId: getProcessBirthId(ownerProcess.pid!) },
        child: { pid: validationProcess.pid, birthId: getProcessBirthId(validationProcess.pid!) },
        subtrees: [{ root: { pid: validationProcess.pid, birthId: getProcessBirthId(validationProcess.pid!) }, descendants: [] }],
        mergeLockHelper: { pid: helperPid, birthId: helperBirthId, channelPath, reason: 'test held lock', observedAt: new Date().toISOString() },
        state: 'active', step: 'merge' } }, rounds: [], summary: {} };
    fs.writeFileSync(path.join(storage, 'threads', `${task.threadId}.json`), JSON.stringify(task));
    fs.writeFileSync(path.join(storage, 'delegation_threads.json'), JSON.stringify([task]));
  };
  try {
    const helperBirthId = getProcessBirthId(helperProcess.pid!)!;
    fs.writeFileSync(`${channelPath}.helper-pid`, String(helperProcess.pid));
    fs.writeFileSync(`${channelPath}.helper-birth`, helperBirthId);
    fs.writeFileSync(`${channelPath}.child.${validationProcess.pid}`, 'tracked');
    fs.writeFileSync(path.join(storage, 'merge.lock.owner.json'), JSON.stringify({ token, pid: ownerProcess.pid,
      birthId: getProcessBirthId(ownerProcess.pid!), helperPid: helperProcess.pid, helperBirthId, acquiredAt: Date.now() }));
    writeTaskAndOwner(helperProcess.pid!, helperBirthId);
    const snapshot = loadShutdownPreflightSnapshot(storage);
    const components = [
      { component: 'extension-host', version, packageRoot, path: path.join(packageRoot, 'out', 'extension.js') },
      { component: 'standalone-mcp', version, packageRoot, path: path.join(packageRoot, 'bin', 'agent-bridge-mcp.cjs') },
    ];
    const liveV2 = inspectV2RuntimeActivation({ ...snapshot, storageDir: storage, components });
    assert.equal(liveV2.ready, true, JSON.stringify({ blockers: liveV2.blockers, owners: snapshot.owners, task: snapshot.tasks[0] }));
    const startupCli = runStartupPreflightCli(home);
    assert.equal(startupCli.status, 0, startupCli.stdout || startupCli.stderr);
    assert.equal(JSON.parse(startupCli.stdout).startupAdmission.ready, true);
    const mismatched = JSON.parse(fs.readFileSync(path.join(storage, 'threads', 'v2-active-merge-helper.json'), 'utf8'));
    mismatched.execution.run.mergeLockHelper.pid = helperProcess.pid! + 1000;
    fs.writeFileSync(path.join(storage, 'threads', `${mismatched.threadId}.json`), JSON.stringify(mismatched));
    const notAssociated = inspectV2RuntimeActivation({ ...loadShutdownPreflightSnapshot(storage), storageDir: storage, components });
    assert.equal(notAssociated.ready, false);
    assert.ok(notAssociated.blockers.some((item) => item.code === 'RUNTIME_OLD_PROCESS_ALIVE' || item.code === 'MERGE_HELPER_STILL_ALIVE'));
    const mismatchCli = runStartupPreflightCli(home);
    assert.equal(mismatchCli.status, 2, mismatchCli.stdout || mismatchCli.stderr);
  } finally {
    await Promise.all([stopChild(ownerProcess), stopChild(helperProcess), stopChild(validationProcess)]);
    fs.rmSync(home, { recursive: true, force: true });
  }
});

test('preflight CLI exposes runtime activation mode and requires inventory without modifying it', () => {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-startup-preflight-cli-'));
  const storage = path.join(home, '.agent-mesh-bridge');
  const packageRoot = path.resolve(__dirname, '../..');
  const version = JSON.parse(fs.readFileSync(path.join(packageRoot, 'package.json'), 'utf8')).version;
  fs.mkdirSync(path.join(storage, 'threads'), { recursive: true });
  fs.writeFileSync(path.join(storage, 'delegation_threads.json'), '[]');
  fs.writeFileSync(path.join(storage, 'v2-runtime-components.json'), JSON.stringify(runtimeInventory(packageRoot, version)));
  try {
    const before = snapshotTree(storage);
    const entry = path.resolve(__dirname, '../../bin/agent-bridge-preflight.cjs');
    const result = spawnSync(process.execPath, [entry, '--startup-admission'], {
      encoding: 'utf8', windowsHide: true, env: { ...process.env, USERPROFILE: home, HOME: home },
    });
    assert.equal(result.status, 0, result.stdout || result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.startupAdmission.ready, true);
    assert.deepEqual(report.startupAdmission.blockers, []);
    assert.deepEqual(snapshotTree(storage), before, 'startup admission CLI must remain read-only');
  } finally { fs.rmSync(home, { recursive: true, force: true }); }
});
