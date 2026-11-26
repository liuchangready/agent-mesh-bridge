'use strict';

// Operator recovery for explicitly selected Windows developer runs without a Job receipt.
const assert = require('node:assert/strict');
const { execFileSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { getProcessBirthId, probeProcessIdentity } = require('../out/bridge/platform');
const { TaskControlService } = require('../out/bridge/task-control');
const { TaskStore } = require('../out/bridge/task-store');
const { appendInterruption, findInFlightRound } = require('../out/bridge/types');

const validThreadId = id => typeof id === 'string' && /^thread_\d{8}_\d{6}_[0-9a-f]{4}$/.test(id);
const completeIdentity = identity => Number.isSafeInteger(identity?.pid) && identity.pid > 0 &&
  typeof identity.birthId === 'string' && identity.birthId.length > 0;
const sameIdentity = (left, right) => left?.pid === right?.pid && left?.birthId === right?.birthId;

function windowsBoot() {
  assert.equal(process.platform, 'win32', 'This recovery requires Windows boot evidence.');
  const powershell = path.join(process.env.SystemRoot || 'C:\\Windows', 'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe');
  const query = [
    "$os = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop",
    "$machine = Get-ItemPropertyValue -LiteralPath 'HKLM:\\SOFTWARE\\Microsoft\\Cryptography' -Name MachineGuid -ErrorAction Stop",
    "$event = Get-WinEvent -FilterHashtable @{LogName='System';ProviderName='Microsoft-Windows-Kernel-General';Id=12} -MaxEvents 1 -ErrorAction Stop",
    "[pscustomobject]@{ machine = $machine; bootedAt = $os.LastBootUpTime.ToUniversalTime().ToString('o'); eventId = $event.RecordId; eventAt = $event.TimeCreated.ToUniversalTime().ToString('o') } | ConvertTo-Json -Compress",
  ].join('\n');
  return JSON.parse(execFileSync(powershell, ['-NoProfile', '-NonInteractive', '-Command', query], {
    encoding: 'utf8', timeout: 8_000, windowsHide: true,
  }));
}

function validateNewBoot(plan, currentBoot) {
  assert.ok(typeof plan.boot.machine === 'string' && plan.boot.machine, 'Missing machine identity.');
  assert.equal(currentBoot.machine, plan.boot.machine, 'Recovery plan is for a different Windows installation.');
  assert.ok(Number.isSafeInteger(plan.boot.eventId) && Number.isSafeInteger(currentBoot.eventId) &&
    Date.parse(plan.boot.bootedAt) < plan.preparedAt && Date.parse(plan.boot.eventAt) < plan.preparedAt &&
    Date.parse(currentBoot.bootedAt) > plan.preparedAt && Date.parse(currentBoot.eventAt) > plan.preparedAt &&
    currentBoot.eventId > plan.boot.eventId,
    'A complete Windows restart after preparing this plan is required.');
}

function taskPaths(task) {
  const artifactDir = task.artifactDir;
  assert.ok(typeof artifactDir === 'string' && path.isAbsolute(artifactDir) && path.basename(artifactDir) === task.threadId,
    `${task.threadId}: expected a task-owned artifact directory.`);
  const realParent = fs.realpathSync(path.dirname(artifactDir));
  assert.equal(path.resolve(artifactDir).toLowerCase(), path.join(realParent, task.threadId).toLowerCase(),
    `${task.threadId}: unexpected artifact parent path.`);
  const artifact = fs.lstatSync(artifactDir, { throwIfNoEntry: false });
  assert.ok(!artifact || (artifact.isDirectory() && !artifact.isSymbolicLink()), `${task.threadId}: unsafe artifact directory.`);
  const worktree = task.worktree;
  if (worktree) {
    assert.ok(worktree.path === path.join(artifactDir, 'worktree') && worktree.taskBranch === `mesh/task/${task.threadId}` &&
      !worktree.mergeResolutionPath && !worktree.supersededMergeResolutionPaths?.length,
      `${task.threadId}: unexpected worktree or merge resolution.`);
  }
  return { artifactDir, worktree: worktree?.path ?? null, repository: worktree?.mainPath ?? null };
}

function runSnapshot(task) {
  const execution = task.execution;
  const run = execution?.run;
  assert.ok(execution?.version === 2 && execution.phase === 'develop' && !execution.next &&
    run?.state === 'stopping' && run.step === 'executing' && !run.mergeLockHelper &&
    !run.subtrees?.some(tree => tree.windowsJob) && !task.worktree?.integration?.stashRecoveryLockHeld,
  `${task.threadId}: expected a stopping developer run without a Job or pending handoff.`);
  assert.ok(completeIdentity(run.owner) && completeIdentity(run.child), `${task.threadId}: incomplete run root identity.`);
  assert.ok(run.id && execution.operationId && Number.isSafeInteger(execution.generation), `${task.threadId}: incomplete run revision identity.`);
  return {
    threadId: task.threadId,
    runId: run.id,
    operationId: execution.operationId,
    generation: execution.generation,
    startedAt: run.startedAt,
    owner: run.owner,
    child: run.child,
    ...taskPaths(task),
  };
}

function preparePlan(storage, filename, ids) {
  const boot = windowsBoot();
  const preparedAt = Date.now();
  const tasks = ids.map(id => {
    const task = JSON.parse(fs.readFileSync(path.join(storage, 'threads', `${id}.json`), 'utf8'));
    assert.equal(task.threadId, id);
    const entry = runSnapshot(task);
    assert.ok(Date.parse(boot.bootedAt) < Date.parse(entry.startedAt) && Date.parse(entry.startedAt) < preparedAt,
      `${id}: run is not from the recorded Windows boot.`);
    for (const process of [entry.owner, entry.child]) {
      assert.equal(probeProcessIdentity(process.pid, process.birthId), 'exited', `${id}: recorded root or owner has not exited.`);
    }
    return entry;
  });
  fs.writeFileSync(filename, JSON.stringify({ version: 2, storage, boot, preparedAt, tasks }, null, 2), { flag: 'wx' });
  console.log(`Prepared ${tasks.length} recovery record(s): ${filename}`);
}

async function settleForDeletion(store, entry, owner, probe = probeProcessIdentity) {
  const task = store.reloadTask(entry.threadId);
  if (!task) {
    const tombstones = fs.readFileSync(path.join(path.dirname(store.getStoragePath()), 'deleted_tasks.log'), 'utf8');
    const latest = tombstones.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)).findLast(record => record.threadId === entry.threadId);
    assert.equal(latest?.by, 'operator-recovery', `${entry.threadId}: task record is absent without its recovery tombstone.`);
    const previous = taskPaths(latest.cleanupManifest.thread);
    assert.ok(previous.artifactDir === entry.artifactDir && previous.worktree === entry.worktree && previous.repository === entry.repository,
      `${entry.threadId}: tombstone paths do not match the recovery plan.`);
    return;
  }
  const proof = { runId: entry.runId, operationId: entry.operationId, generation: entry.generation };
  const matchesProof = value => value?.runId === proof.runId && value?.operationId === proof.operationId &&
    value?.generation === proof.generation;
  const cleanup = task.cleanup;
  if (cleanup) {
    assert.ok(cleanup.action === 'delete' && cleanup.stage === 'in_progress' && matchesProof(cleanup) &&
      completeIdentity(cleanup.owner), `${entry.threadId}: conflicting cleanup reservation.`);
    if (!sameIdentity(cleanup.owner, owner)) {
      assert.equal(probe(cleanup.owner.pid, cleanup.owner.birthId), 'exited', `${entry.threadId}: cleanup owner has not exited.`);
    }
  }
  const token = cleanup?.token || randomUUID();
  await store.transitionExecution(entry.threadId, task.execution.revision, current => {
    const execution = current.execution;
    assert.ok(execution?.version === 2 && execution.phase === 'develop' && !execution.next &&
      execution.operationId === proof.operationId && execution.generation === proof.generation &&
      !current.worktree?.integration?.stashRecoveryLockHeld, `${entry.threadId}: recovery target changed.`);
    const locations = taskPaths(current);
    assert.ok(locations.artifactDir === entry.artifactDir && locations.worktree === entry.worktree &&
      locations.repository === entry.repository, `${entry.threadId}: recovery target paths changed.`);
    assert.deepEqual(current.cleanup, cleanup, `${entry.threadId}: cleanup reservation changed.`);
    if (execution.run) assert.deepEqual(runSnapshot(current), entry, `${entry.threadId}: run does not match the prepared snapshot.`);
    else assert.ok(matchesProof(cleanup?.runExitProof), `${entry.threadId}: run disappeared without matching exit proof.`);

    const at = new Date().toISOString();
    current.cleanup = { ...cleanup, ...proof, token, owner, action: 'delete', stage: 'in_progress', startedAt: cleanup?.startedAt ?? at };
    if (execution.run) {
      execution.run = undefined;
      execution.queue = undefined;
      appendInterruption(current, { kind: 'paused', at, reason: 'Recovered after verified Windows reboot', by: 'operator-recovery' });
      current.status = 'paused';
      const round = findInFlightRound(current);
      if (round) { round.status = 'paused'; round.interruption = current.interruption; }
    }
  }, token, proof);
}

async function main(args) {
  const [mode, filename, ...ids] = args;
  assert.ok(filename && (mode === 'prepare' || mode === 'apply'),
    'Usage: recover-stopping-runs.cjs prepare <plan.json> <threadId>... | apply <plan.json>');
  const storage = fs.realpathSync(path.join(os.homedir(), '.agent-mesh-bridge'));
  if (mode === 'prepare') {
    assert.ok(ids.length && ids.every(validThreadId) && new Set(ids).size === ids.length, 'Provide unique task IDs to recover and delete.');
    preparePlan(storage, filename, ids);
    return;
  }
  assert.equal(ids.length, 0, 'Apply uses only the task IDs in the prepared plan.');
  const plan = JSON.parse(fs.readFileSync(filename, 'utf8'));
  assert.ok(plan.version === 2 && storage === plan.storage && Array.isArray(plan.tasks) && plan.tasks.length &&
    plan.tasks.every(entry => validThreadId(entry.threadId)) && new Set(plan.tasks.map(entry => entry.threadId)).size === plan.tasks.length,
  'Invalid or mismatched recovery plan.');
  validateNewBoot(plan, windowsBoot());
  const owner = { pid: process.pid, birthId: getProcessBirthId(process.pid) };
  assert.ok(completeIdentity(owner), 'Cannot verify this cleanup process identity.');
  const store = new TaskStore(storage);
  try {
    const controls = new TaskControlService(store);
    const failed = [];
    for (const entry of plan.tasks) {
      try {
        await settleForDeletion(store, entry, owner);
        const result = await controls.deleteTask(entry.threadId, 'Operator-approved cleanup after verified Windows reboot', 'operator-recovery', true);
        assert.ok(!fs.existsSync(entry.artifactDir), `${entry.threadId}: task directory remains on disk.`);
        console.log(`${entry.threadId}: ${result.status}. ${result.message}`);
      } catch (error) {
        failed.push(entry.threadId);
        console.error(`${entry.threadId}: ${error.message}`);
      }
    }
    assert.equal(failed.length, 0, `Unfinished tasks: ${failed.join(', ')}`);
  } finally {
    store.dispose();
  }
}

module.exports = { runSnapshot, settleForDeletion, validateNewBoot };
if (require.main === module) main(process.argv.slice(2)).catch(error => {
  console.error(error.message);
  process.exitCode = 1;
});
