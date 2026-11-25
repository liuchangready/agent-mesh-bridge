import * as fs from 'fs';
import * as path from 'path';
import { execFileSync } from 'child_process';
import { DelegationThread } from './types';
import { probeProcessIdentity } from './platform';
import { gitExecutable } from './git-worktree';

export interface PreflightComponentVersion {
  component: string;
  version?: string;
  source?: string;
}

export interface CutoverBlocker {
  code: string;
  threadId?: string;
  evidence: string;
}

export interface ShutdownPreflightReport {
  ready: boolean;
  checkedAt: string;
  expectedVersion: string;
  components: PreflightComponentVersion[];
  blockers: CutoverBlocker[];
}

export interface RuntimeComponentExpectation {
  component: string;
  version: string;
  path: string;
  packageRoot: string;
}

export interface RuntimeActivationReport {
  ready: boolean;
  inventoryPath: string;
  blockers: CutoverBlocker[];
  components: RuntimeComponentExpectation[];
}

interface PreflightOwnerRecord {
  pid?: number;
  birthId?: string;
  childPid?: number;
  childBirthId?: string;
  helperPid?: number;
  helperBirthId?: string;
  helperChannelPath?: string;
  helperReleaseAck?: string;
  helperChildPids?: number[];
  mergeToken?: string;
  channelHelperPid?: number;
  channelHelperBirthId?: string;
  threadId?: string;
  source: string;
}

const REVIEWER_BOOTSTRAP_TTL_MS = 30 * 60 * 1000;

function safeThreadName(threadId: string): string {
  return String(threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');
}

function readText(file: string): string | undefined {
  try { return fs.readFileSync(file, 'utf8'); }
  catch (error: any) { if (error?.code === 'ENOENT') return undefined; throw error; }
}

/** Read task and legacy execution records without constructing TaskStore or
 * TaskExecutionRegistry (both are allowed to create directories/watchers). */
export function loadShutdownPreflightSnapshot(storageDir: string): {
  tasks: DelegationThread[];
  tombstones: any[];
  owners: PreflightOwnerRecord[];
  readErrors: string[];
  pendingCoordination: string[];
  pendingMergeTransactions: Array<{ threadId?: string; path: string; phase?: string }>;
} {
  const tasks: DelegationThread[] = [];
  const tombstones: any[] = [];
  const owners: PreflightOwnerRecord[] = [];
  const readErrors: string[] = [];
  const pendingCoordination: string[] = [];
  const pendingMergeTransactions: Array<{ threadId?: string; path: string; phase?: string }> = [];
  try {
    const content = readText(path.join(storageDir, 'deleted_tasks.log'));
    if (content) for (const line of content.split(/\r?\n/).filter(Boolean)) {
      try { tombstones.push(JSON.parse(line)); }
      catch (error: any) { readErrors.push(`deleted_tasks.log: ${error?.message || error}`); }
    }
  } catch (error: any) { readErrors.push(`deleted_tasks.log: ${error?.message || error}`); }
  const tombstonedIds = new Set(tombstones.map((entry) => entry?.threadId).filter((id): id is string => typeof id === 'string'));
  const threadsDir = path.join(storageDir, 'threads');
  let taskFiles: string[] = [];
  try { taskFiles = fs.readdirSync(threadsDir).filter((name) => name.endsWith('.json')); }
  catch (error: any) { readErrors.push(`threads/: ${error?.message || error}`); }
  for (const name of taskFiles) {
    const file = path.join(threadsDir, name);
    try {
      const item = JSON.parse(fs.readFileSync(file, 'utf8'));
      if (!item || typeof item.threadId !== 'string') throw new Error('missing threadId');
      if (tombstonedIds.has(item.threadId)) continue;
      tasks.push(item as DelegationThread);
    } catch (error: any) { readErrors.push(`${path.relative(storageDir, file)}: ${error?.message || error}`); }
  }
  const threadIds = new Set(tasks.map((task) => task.threadId));
  const aggregatePath = path.join(storageDir, 'delegation_threads.json');
  try {
    const aggregate = JSON.parse(fs.readFileSync(aggregatePath, 'utf8'));
    if (!Array.isArray(aggregate)) throw new Error('aggregate is not an array');
    for (const item of aggregate) {
      if (typeof item?.threadId === 'string' && tombstonedIds.has(item.threadId)) continue;
      if (typeof item?.threadId !== 'string' || !threadIds.has(item.threadId)) {
        readErrors.push(`delegation_threads.json: aggregate entry ${String(item?.threadId)} has no matching per-thread record.`);
      }
    }
  } catch (error: any) { readErrors.push(`delegation_threads.json: ${error?.message || error}`); }
  for (const directory of ['executions', 'leases']) {
    const root = path.join(storageDir, directory);
    let names: string[] = [];
    try { names = fs.readdirSync(root).filter((name) => name.endsWith('.json') || name.endsWith('.lock')); }
    catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`${directory}/: ${error?.message || error}`); }
    for (const name of names) {
      const file = path.join(root, name);
      try {
        const item = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (item && typeof item === 'object') owners.push({
          pid: Number.isSafeInteger(item.pid) ? item.pid : undefined,
          birthId: typeof item.birthId === 'string' ? item.birthId : undefined,
          childPid: Number.isSafeInteger(item.childPid) ? item.childPid : undefined,
          childBirthId: typeof item.childBirthId === 'string' ? item.childBirthId : undefined,
          threadId: typeof item.threadId === 'string' ? item.threadId : undefined,
          source: path.relative(storageDir, file).replace(/\\/g, '/'),
        });
      } catch (error: any) { readErrors.push(`${path.relative(storageDir, file)}: ${error?.message || error}`); }
    }
  }
  const legacyExecutionOwner = path.join(storageDir, 'execution.lock');
  try {
    const item = JSON.parse(fs.readFileSync(legacyExecutionOwner, 'utf8'));
    owners.push({ pid: item.pid, birthId: item.birthId, childPid: item.childPid, childBirthId: item.childBirthId,
      threadId: item.threadId, source: 'execution.lock' });
  } catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`execution.lock: ${error?.message || error}`); }
  const mergeLockPath = path.join(storageDir, 'merge.lock');
  const mergeOwner = `${mergeLockPath}.owner.json`;
  const readMergeChannel = (channelPath: string, source: string, owner?: any) => {
    const helperPidText = readText(`${channelPath}.helper-pid`)?.trim();
    const helperBirthId = readText(`${channelPath}.helper-birth`)?.trim();
    const channelHelperPid = helperPidText && /^\d+$/.test(helperPidText) ? Number(helperPidText) : undefined;
    const releaseAck = readText(`${channelPath}.released`)?.trim();
    const childPids: number[] = [];
    const dir = path.dirname(channelPath);
    const prefix = `${path.basename(channelPath)}.child.`;
    try {
      for (const name of fs.readdirSync(dir)) {
        if (!name.startsWith(prefix)) continue;
        const pid = Number(name.slice(prefix.length));
        if (Number.isSafeInteger(pid) && pid > 0) childPids.push(pid);
      }
    } catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`${path.relative(storageDir, dir)}: ${error?.message || error}`); }
    return {
      ...(owner && Number.isSafeInteger(owner.pid) ? { pid: owner.pid } : {}),
      ...(owner && typeof owner.birthId === 'string' ? { birthId: owner.birthId } : {}),
      ...(owner && Number.isSafeInteger(owner.childPid) ? { childPid: owner.childPid } : {}),
      ...(owner && typeof owner.childBirthId === 'string' ? { childBirthId: owner.childBirthId } : {}),
      ...(owner && Number.isSafeInteger(owner.helperPid) ? { helperPid: owner.helperPid } :
        helperPidText && /^\d+$/.test(helperPidText) ? { helperPid: Number(helperPidText) } : {}),
      ...(owner && typeof owner.helperBirthId === 'string' ? { helperBirthId: owner.helperBirthId } :
        helperBirthId ? { helperBirthId } : {}),
      ...(channelHelperPid ? { channelHelperPid } : {}),
      ...(helperBirthId ? { channelHelperBirthId: helperBirthId } : {}),
      helperChannelPath: channelPath,
      helperReleaseAck: releaseAck,
      helperChildPids: childPids,
      ...(path.basename(channelPath).match(/^merge\.lock\.([A-Za-z0-9-]{8,128})$/)?.[1]
        ? { mergeToken: path.basename(channelPath).match(/^merge\.lock\.([A-Za-z0-9-]{8,128})$/)![1] } : {}),
      ...(owner && typeof owner.threadId === 'string' ? { threadId: owner.threadId } : {}),
      source,
    } as PreflightOwnerRecord;
  };
  const mergeOwnerSeen = new Set<string>();
  try {
    const item = JSON.parse(fs.readFileSync(mergeOwner, 'utf8'));
    if (!item || typeof item !== 'object' || typeof item.token !== 'string' || !/^[A-Za-z0-9-]{8,128}$/.test(item.token)) {
      throw new Error('merge owner record has no valid token');
    }
    const channelPath = `${mergeLockPath}.${item.token}`;
    mergeOwnerSeen.add(item.token);
    owners.push(readMergeChannel(channelPath, 'merge.lock.owner.json', item));
  } catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`merge.lock.owner.json: ${error?.message || error}`); }
  // A helper can be spawned before it publishes merge.lock.owner.json. Inspect
  // its actual per-acquisition channel files and launch scripts as well; the
  // absence of the owner file is not proof that the OS mutex was never taken.
  let rootEntries: string[] = [];
  try { rootEntries = fs.readdirSync(storageDir); }
  catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`storage root: ${error?.message || error}`); }
  const channelTokens = new Set<string>();
  for (const name of rootEntries) {
    const match = name.match(/^merge\.lock\.([A-Za-z0-9-]{8,128})\.(?:helper-pid|helper-birth|released|ready|busy|done|error|launch-error|child\.\d+|command\.child\.\d+|command\.exit\.\d+)$/);
    if (match) channelTokens.add(match[1]);
    const scriptMatch = name.match(/^merge\.lock\.([A-Za-z0-9-]{8,128})\.mutex-helper\.ps1(?:\.launch\.ps1)?$/);
    if (scriptMatch) channelTokens.add(scriptMatch[1]);
    if (/^merge\.lock\.owner\.json\..+\.tmp$/.test(name)) {
      // The owner rename window is deliberately a blocker: the helper may
      // already own the OS lock while no durable owner has been published.
      const temp = path.join(storageDir, name);
      let pid: number | undefined;
      const ownerMatch = name.match(/^merge\.lock\.owner\.json\.(\d+)\.([A-Za-z0-9-]{8,128})\.tmp$/);
      if (ownerMatch) { pid = Number(ownerMatch[1]); channelTokens.add(ownerMatch[2]); }
      owners.push({ pid, source: `merge owner publication ${path.basename(temp)}` });
    }
  }
  for (const token of channelTokens) {
    if (mergeOwnerSeen.has(token)) continue;
    owners.push(readMergeChannel(`${mergeLockPath}.${token}`, `unpublished merge helper channel ${token}`));
  }
  const transactionDir = path.join(storageDir, 'merge-transactions');
  let transactionFiles: string[] = [];
  try { transactionFiles = fs.readdirSync(transactionDir).filter((name) => name.endsWith('.json')); }
  catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`merge-transactions/: ${error?.message || error}`); }
  for (const name of transactionFiles) {
    const file = path.join(transactionDir, name);
    try {
      const item = JSON.parse(fs.readFileSync(file, 'utf8'));
      const completed = item?.phase === 'ledger_written' && !item.stashCleanupPending && !item.stashCleanupLock && !item.stashRestoreError;
      if (!completed) pendingMergeTransactions.push({ threadId: typeof item?.threadId === 'string' ? item.threadId : undefined,
        path: path.relative(storageDir, file).replace(/\\/g, '/'), phase: typeof item?.phase === 'string' ? item.phase : undefined });
    } catch (error: any) { readErrors.push(`${path.relative(storageDir, file)}: ${error?.message || error}`); }
  }
  // Old independent wake/bootstrap intent is not imported by the v2 engine.
  // It must be consumed or explicitly dispositioned before cutover. Read the
  // records without constructing their services (which would create dirs).
  const collectFiles = (directory: string, predicate: (relative: string) => boolean) => {
    const root = path.join(storageDir, directory);
    const visit = (absolute: string) => {
      let entries: fs.Dirent[];
      try { entries = fs.readdirSync(absolute, { withFileTypes: true }); }
      catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`${path.relative(storageDir, absolute)}: ${error?.message || error}`); return; }
      for (const entry of entries) {
        const file = path.join(absolute, entry.name);
        const relative = path.relative(storageDir, file).replace(/\\/g, '/');
        if (entry.isDirectory()) visit(file);
        else if (entry.isFile() && predicate(relative)) pendingCoordination.push(relative);
      }
    };
    visit(root);
  };
  collectFiles('wakes', (relative) => relative.includes('/pending/') && relative.endsWith('.json'));
  collectFiles('bootstrap', (relative) => relative.endsWith('.json'));
  const now = Date.now();
  for (const task of tasks) {
    const dir = task.artifactDir ? path.join(task.artifactDir, 'bootstrap') : undefined;
    if (!dir) continue;
    let names: string[] = [];
    try { names = fs.readdirSync(dir).filter((name) => name.endsWith('.json')); }
    catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`${dir}: ${error?.message || error}`); }
    for (const name of names) {
      const file = path.join(dir, name);
      try {
        const record = JSON.parse(fs.readFileSync(file, 'utf8'));
        const created = Date.parse(record?.createdAt || '');
        if (!record || record.threadId !== task.threadId || typeof record.token !== 'string' || !record.token ||
            !Number.isFinite(created)) throw new Error('invalid task bootstrap credential schema');
        if (created <= now && now - created <= REVIEWER_BOOTSTRAP_TTL_MS) {
          pendingCoordination.push(path.relative(storageDir, file).replace(/\\/g, '/'));
        }
      } catch (error: any) { readErrors.push(`${path.relative(storageDir, file)}: ${error?.message || error}`); }
    }
  }
  // Role/session leases must be drained, but are never used as new-version
  // admission. Unknown schemas fail closed instead of being silently ignored.
  for (const directory of ['roles', 'native-sessions']) {
    const root = path.join(storageDir, directory);
    let names: string[] = [];
    try { names = fs.readdirSync(root).filter((name) => directory === 'native-sessions' || name.endsWith('.json') || name.endsWith('.lock')); }
    catch (error: any) { if (error?.code !== 'ENOENT') readErrors.push(`${directory}/: ${error?.message || error}`); }
    for (const name of names) {
      const file = path.join(root, name);
      try {
        const item = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (item && typeof item === 'object') owners.push({
          pid: Number.isSafeInteger(item.pid) ? item.pid : undefined,
          birthId: typeof item.birthId === 'string' ? item.birthId : undefined,
          childPid: Number.isSafeInteger(item.childPid) ? item.childPid : undefined,
          childBirthId: typeof item.childBirthId === 'string' ? item.childBirthId : undefined,
          threadId: typeof item.threadId === 'string' ? item.threadId : undefined,
          source: path.relative(storageDir, file).replace(/\\/g, '/'),
        });
        pendingCoordination.push(path.relative(storageDir, file).replace(/\\/g, '/'));
      } catch (error: any) { readErrors.push(`${path.relative(storageDir, file)}: ${error?.message || error}`); }
    }
  }
  return { tasks, tombstones, owners, readErrors, pendingCoordination, pendingMergeTransactions };
}

function historicalDeliveryProblem(task: DelegationThread): string | undefined {
  const worktree = task.worktree as any;
  if (!worktree) return undefined;
  const integration = worktree.integration || {};
  if (worktree.mergePending || integration.stashCleanupPending || integration.stashRecoveryLockHeld || integration.recoveryRequired) {
    return 'Git delivery or stash recovery remains pending.';
  }
  if (integration.phase === 'no_changes') return undefined;
  if (integration.phase !== 'merged') return `Git integration phase is ${integration.phase || 'unknown'}.`;

  const target = worktree.deliveryTarget;
  const ref = target?.ref || worktree.targetRef;
  const repositoryPath = target?.repositoryPath || worktree.repositoryPath || worktree.mainPath;
  const approvedCommit = worktree.approvedCommit || [...(task.mergeHistory || [])].reverse()
    .find((entry: any) => entry?.phase === 'merged' && typeof entry.candidateCommit === 'string')?.candidateCommit;
  if (typeof ref !== 'string' || !/^refs\/heads\/[A-Za-z0-9._/-]+$/.test(ref) || ref.includes('..') || ref.includes('@{') ||
      typeof repositoryPath !== 'string' || !path.isAbsolute(repositoryPath) ||
      typeof approvedCommit !== 'string' || !/^[a-f0-9]{40,64}$/i.test(approvedCommit)) {
    return 'Merged status lacks a pinned local branch, repository path, or approved commit proof.';
  }
  try {
    const git = gitExecutable();
    const run = (args: string[]) => execFileSync(git, ['-C', repositoryPath, ...args], { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true }).trim();
    const targetCommit = run(['rev-parse', '--verify', `${ref}^{commit}`]);
    const ancestors = [approvedCommit, integration.targetCommit, integration.mergedCommit].filter((value): value is string => typeof value === 'string');
    for (const commit of ancestors) run(['merge-base', '--is-ancestor', commit, targetCommit]);
  } catch (error: any) {
    return `Pinned delivery ref does not prove the approved merge is present: ${error?.stderr?.toString?.().trim() || error?.message || error}`;
  }
  return undefined;
}

/** Pure, read-only cutover decision. An omitted/unverifiable identity or
 * component version is a blocker, never an implicit green result. */
export function inspectShutdownPreflight(input: {
  tasks: DelegationThread[];
  tombstones?: any[];
  owners?: Array<PreflightOwnerRecord>;
  readErrors?: string[];
  pendingCoordination?: string[];
  pendingMergeTransactions?: Array<{ threadId?: string; path: string; phase?: string }>;
  expectedVersion: string;
  components?: PreflightComponentVersion[];
  requiredComponents?: string[];
  probe?: (pid: number, birthId?: string) => 'alive' | 'exited' | 'unknown';
}): ShutdownPreflightReport {
  const blockers: CutoverBlocker[] = [];
  const components = input.components || [];
  const required = input.requiredComponents || ['extension-host', 'standalone-mcp'];
  for (const name of required) {
    const matches = components.filter((entry) => entry.component === name);
    if (matches.length !== 1 || !matches[0].version) {
      blockers.push({ code: 'HOST_VERSION_UNVERIFIED', evidence: `${name} version is missing or ambiguous.` });
    } else if (matches[0].version !== input.expectedVersion) {
      blockers.push({ code: 'MIXED_HOST_VERSIONS', evidence: `${name}=${matches[0].version}, expected ${input.expectedVersion}${matches[0].source ? ` (${matches[0].source})` : ''}.` });
    }
  }
  for (const error of input.readErrors || []) blockers.push({ code: 'PREFLIGHT_READ_ERROR', evidence: error });
  for (const record of input.pendingCoordination || []) blockers.push({ code: 'LEGACY_COORDINATION_REMAINS', evidence: `${record} must be consumed or safely dispositioned by the old runtime before cutover.` });
  const probe = input.probe || probeProcessIdentity;
  const inspectIdentity = (pid: number | undefined, birthId: string | undefined, source: string, threadId?: string) => {
    if (!pid) return;
    if (!birthId) {
      blockers.push({ code: 'PROCESS_IDENTITY_UNVERIFIED', threadId, evidence: `${source} has PID ${pid} without birthId.` });
      return;
    }
    const state = probe(pid, birthId);
    if (state !== 'exited') blockers.push({ code: state === 'alive' ? 'PROCESS_STILL_ALIVE' : 'PROCESS_IDENTITY_UNVERIFIED', threadId,
      evidence: `${source} PID ${pid}/${birthId} is ${state}.` });
  };
  for (const owner of input.owners || []) {
    inspectIdentity(owner.pid, owner.birthId, owner.source || 'execution owner', owner.threadId);
    inspectIdentity(owner.childPid, owner.childBirthId, `${owner.source || 'execution owner'} child`, owner.threadId);
    inspectIdentity(owner.helperPid, owner.helperBirthId, `${owner.source || 'merge lock'} helper`, owner.threadId);
    for (const pid of owner.helperChildPids || []) blockers.push({ code: 'MERGE_LOCK_CHILD_UNVERIFIED', threadId: owner.threadId,
      evidence: `${owner.source || 'merge lock'} helper channel retains child PID ${pid} without durable birthId/exit evidence.` });
    if (owner.source?.includes('merge') || owner.helperChannelPath) {
      if (!owner.helperPid || !owner.helperBirthId) blockers.push({ code: 'MERGE_HELPER_IDENTITY_UNVERIFIED', threadId: owner.threadId,
        evidence: `${owner.source || 'merge lock'} has no exact helper PID/birthId.` });
      else {
        const helperState = probe(owner.helperPid, owner.helperBirthId);
        const ack = owner.helperReleaseAck?.split(':');
        const exactAck = !!ack && Number(ack[0]) === owner.helperPid && ack[1] === owner.helperBirthId;
        const released = helperState === 'exited' && (process.platform !== 'win32' || exactAck);
        if (helperState !== 'exited' || !released) blockers.push({
          code: helperState === 'alive' ? 'MERGE_HELPER_STILL_ALIVE' : 'MERGE_HELPER_RELEASE_UNVERIFIED', threadId: owner.threadId,
          evidence: `${owner.source || 'merge lock'} helper PID ${owner.helperPid}/${owner.helperBirthId} is ${helperState}; exact release acknowledgement ${exactAck ? 'present' : 'missing or mismatched'}.`,
        });
      }
    }
  }
  for (const thread of input.tasks) {
    if (thread.status === 'paused') blockers.push({ code: 'PAUSED_TASK', threadId: thread.threadId, evidence: 'Task is paused and requires old-version resume or explicit disposition.' });
    if (thread.status === 'wait_decision') blockers.push({ code: 'WAITING_FOR_DECISION', threadId: thread.threadId, evidence: 'Task has a pending user decision.' });
    if (!['completed'].includes(thread.status)) blockers.push({ code: 'UNDELIVERED_TASK', threadId: thread.threadId, evidence: `Task status is ${thread.status}; delivery/terminal disposition is incomplete.` });
    if (thread.reviewWake && !thread.reviewWake.gaveUp) blockers.push({ code: 'PENDING_WAKE', threadId: thread.threadId,
      evidence: `Reviewer wake for round ${thread.reviewWake.round} is pending${thread.reviewWake.autoRetryAt ? ` until ${thread.reviewWake.autoRetryAt}` : ''}.` });
    if (thread.worktree?.approvedCommit && !['merged', 'no_changes'].includes(thread.worktree.integration?.phase || '')) {
      blockers.push({ code: 'UNDELIVERED_APPROVAL', threadId: thread.threadId,
        evidence: `Approved commit ${thread.worktree.approvedCommit} is not recorded as merged or no_changes.` });
    }
    const deliveryProblem = thread.status === 'completed' ? historicalDeliveryProblem(thread) : undefined;
    if (deliveryProblem) blockers.push({ code: 'UNDELIVERED_TASK', threadId: thread.threadId, evidence: deliveryProblem });
    if (thread.execution?.next) blockers.push({ code: 'PENDING_HANDOFF', threadId: thread.threadId,
      evidence: `Durable ${thread.execution.next.phase} handoff from ${thread.execution.next.sourceRunId || 'unknown source run'} is unconsumed.` });
    const inFlight = (thread.rounds || []).filter((round) => round.status === 'running');
    for (const round of inFlight) blockers.push({ code: 'IN_FLIGHT_ROUND', threadId: thread.threadId,
      evidence: `Round ${round.round} (${round.type}) is still recorded running, regardless of thread terminal status.` });
    if (thread.decision && !thread.decision.answer) blockers.push({ code: 'PENDING_DECISION', threadId: thread.threadId,
      evidence: `Decision ${thread.decision.id} for ${thread.decision.phase} has no durable answer.` });
    if (thread.execution?.queue) blockers.push({ code: 'PENDING_RETRY_OR_QUEUE', threadId: thread.threadId,
      evidence: `Execution is queued for ${thread.execution.queue.reason}${thread.execution.queue.notBefore ? ` until ${thread.execution.queue.notBefore}` : ''}.` });
    if (thread.reviewBootstrap && ['pending', 'launching'].includes(thread.reviewBootstrap.phase)) blockers.push({ code: 'PENDING_BOOTSTRAP', threadId: thread.threadId,
      evidence: `Reviewer bootstrap is ${thread.reviewBootstrap.phase}.` });
    const run = thread.execution?.run;
    if (run) {
      if (!run.owner?.pid || !run.owner.birthId) blockers.push({ code: 'PROCESS_IDENTITY_UNVERIFIED', threadId: thread.threadId,
        evidence: `Run ${run.id} has no complete controller PID/birthId.` });
      inspectIdentity(run.owner?.pid, run.owner?.birthId, 'run owner', thread.threadId);
      inspectIdentity(run.child?.pid, run.child?.birthId, 'run child', thread.threadId);
      inspectIdentity(run.mergeLockHelper?.pid, run.mergeLockHelper?.birthId, 'merge lock helper', thread.threadId);
      for (const tree of run.subtrees || []) {
        inspectIdentity(tree.root.pid, tree.root.birthId, 'captured subtree root', thread.threadId);
        for (const child of tree.descendants) inspectIdentity(child.pid, child.birthId, 'captured descendant', thread.threadId);
      }
      blockers.push({ code: 'RUN_RECORD_REMAINS', threadId: thread.threadId,
        evidence: `execution.version=${thread.execution?.version ?? 'unknown'} run ${run.id} remains recorded (${run.state}/${run.step}).` });
    }
    if (thread.cleanup) blockers.push({ code: 'CLEANUP_IN_PROGRESS', threadId: thread.threadId, evidence: `${thread.cleanup.action} cleanup token ${thread.cleanup.token} remains active.` });
    const worktree = thread.worktree as any;
    if (worktree?.integration?.stashCleanupPending || worktree?.integration?.stashRecoveryLockHeld || worktree?.integration?.recoveryRequired) {
      blockers.push({ code: 'GIT_RECOVERY_PENDING', threadId: thread.threadId,
        evidence: `Task worktree integration recovery remains pending (${worktree.integration.stashRecoveryLockHeld ? 'stash recovery lock held' : 'cleanup/recovery marker'}).` });
    }
    if (thread.execution && thread.execution.version !== 2 && thread.status !== 'completed') blockers.push({ code: 'LEGACY_TASK_NOT_DRAINED', threadId: thread.threadId,
      evidence: `execution.version=${thread.execution.version} is historical but task is not terminal.` });
  }
  const latestTombstones = new Map<string, any>();
  for (const item of input.tombstones || []) if (typeof item?.threadId === 'string') latestTombstones.set(item.threadId, item);
  for (const [threadId, tombstone] of latestTombstones) {
    const manifest = tombstone.cleanupManifest;
    if (!manifest || manifest.stage === 'complete' || tombstone.cleanupComplete) continue;
    const run = manifest.execution?.run;
    const proof = manifest.runExitProof;
    if (run && (!proof || proof.runId !== run.id || proof.operationId !== manifest.execution?.operationId || proof.generation !== manifest.execution?.generation)) {
      blockers.push({ code: 'DELETED_RUN_PROTECTED', threadId,
        evidence: `Deleted task retains run ${run.id} without matching process-tree and resource exit acknowledgement.` });
      inspectIdentity(run.owner?.pid, run.owner?.birthId, 'deleted run owner', threadId);
      inspectIdentity(run.child?.pid, run.child?.birthId, 'deleted run child', threadId);
      for (const tree of run.subtrees || []) {
        inspectIdentity(tree.root?.pid, tree.root?.birthId, 'deleted captured subtree root', threadId);
        for (const child of tree.descendants || []) inspectIdentity(child.pid, child.birthId, 'deleted captured descendant', threadId);
        if (tree.windowsJob) blockers.push({ code: 'DELETED_JOB_RECEIPT_RETAINED', threadId,
          evidence: `Deleted run ${run.id} retains Windows Job receipt ${tree.windowsJob.id}.` });
      }
      if (run.mergeLockHelper) inspectIdentity(run.mergeLockHelper.pid, run.mergeLockHelper.birthId, 'deleted merge helper', threadId);
    }
    if (manifest.retainResources) blockers.push({ code: 'DELETED_RESOURCES_RETAINED', threadId,
      evidence: `Deleted task resources remain retained under cleanup token ${manifest.token || tombstone.cleanupToken || 'unknown'}.` });
    blockers.push({ code: 'DELETED_CLEANUP_PENDING', threadId,
      evidence: `Deleted task cleanup token ${manifest.token || tombstone.cleanupToken || 'unknown'} is ${manifest.stage || 'pending'}.` });
  }
  const mergeTransactions = input.pendingMergeTransactions || [];
  for (const transaction of mergeTransactions) blockers.push({ code: 'GIT_RECOVERY_PENDING', threadId: transaction.threadId,
    evidence: `${transaction.path} is in phase ${transaction.phase || 'unknown'} and requires merge recovery.` });
  return { ready: blockers.length === 0, checkedAt: new Date().toISOString(), expectedVersion: input.expectedVersion, components, blockers };
}

/**
 * Admission check run before any v2 scanner or dispatcher is enabled. The
 * operator-created inventory is immutable evidence from the cutover preflight;
 * it records the exact installed entrypoints and every previously observed
 * host process. Current v2 ledger runs are deliberately allowed through this
 * gate so the ordinary run recovery classifier can resume safely after a v2
 * restart.
 */
export function inspectV2RuntimeActivation(input: {
  storageDir: string;
  tasks: DelegationThread[];
  tombstones?: any[];
  owners?: PreflightOwnerRecord[];
  readErrors?: string[];
  pendingCoordination?: string[];
  pendingMergeTransactions?: Array<{ threadId?: string; path: string; phase?: string }>;
  components: RuntimeComponentExpectation[];
  probe?: (pid: number, birthId?: string) => 'alive' | 'exited' | 'unknown';
}): RuntimeActivationReport {
  const inventoryPath = path.join(input.storageDir, 'v2-runtime-components.json');
  const blockers: CutoverBlocker[] = [];
  const probe = input.probe || probeProcessIdentity;
  let inventory: any;
  try { inventory = JSON.parse(fs.readFileSync(inventoryPath, 'utf8')); }
  catch (error: any) { blockers.push({ code: 'RUNTIME_COMPONENT_INVENTORY_UNVERIFIED', evidence: `${path.basename(inventoryPath)}: ${error?.message || error}` }); }
  if (!inventory || inventory.schemaVersion !== 1 || inventory.cutoverApproved !== true || !Array.isArray(inventory.components)) {
    if (!blockers.some((item) => item.code === 'RUNTIME_COMPONENT_INVENTORY_UNVERIFIED')) blockers.push({ code: 'RUNTIME_COMPONENT_INVENTORY_UNVERIFIED', evidence: 'Runtime inventory is missing, incomplete, or has no explicit cutover approval.' });
  }
  const canonical = (value: string): string | undefined => {
    try { const resolved = fs.realpathSync(value); return process.platform === 'win32' ? resolved.toLowerCase() : resolved; }
    catch { return undefined; }
  };
  const inventoryByName = new Map<string, any>();
  for (const entry of Array.isArray(inventory?.components) ? inventory.components : []) {
    if (!entry || typeof entry.component !== 'string' || inventoryByName.has(entry.component)) {
      blockers.push({ code: 'RUNTIME_COMPONENT_INVENTORY_UNVERIFIED', evidence: 'Runtime inventory contains an invalid or duplicate component entry.' });
      continue;
    }
    inventoryByName.set(entry.component, entry);
  }
  for (const expected of input.components) {
    const entry = inventoryByName.get(expected.component);
    if (!entry) {
      blockers.push({ code: 'RUNTIME_COMPONENT_UNVERIFIED', evidence: `${expected.component} inventory entry is missing.` });
      continue;
    }
    const actualEntryPath = canonical(expected.path);
    const inventoryEntryPath = typeof entry.path === 'string' ? canonical(entry.path) : undefined;
    const actualRoot = canonical(expected.packageRoot);
    const inventoryRoot = typeof entry.packageRoot === 'string' ? canonical(entry.packageRoot) : undefined;
    let actualPackageVersion: string | undefined;
    try { actualPackageVersion = JSON.parse(fs.readFileSync(path.join(expected.packageRoot, 'package.json'), 'utf8')).version; }
    catch (error: any) { blockers.push({ code: 'RUNTIME_COMPONENT_UNVERIFIED', evidence: `${expected.component} package metadata cannot be read: ${error?.message || error}` }); }
    if (!actualEntryPath || !inventoryEntryPath || actualEntryPath !== inventoryEntryPath || !actualRoot || !inventoryRoot || actualRoot !== inventoryRoot) {
      blockers.push({ code: 'RUNTIME_COMPONENT_PATH_MISMATCH', evidence: `${expected.component} inventory path does not match the loaded entrypoint/package (${expected.path}).` });
    }
    if (entry.version !== expected.version || actualPackageVersion !== expected.version) {
      blockers.push({ code: 'RUNTIME_COMPONENT_VERSION_MISMATCH', evidence: `${expected.component} inventory=${String(entry.version)}, loaded=${expected.version}, package=${String(actualPackageVersion)}.` });
    }
    if (entry.completeProcessInventory !== true || !Array.isArray(entry.previousProcesses)) {
      blockers.push({ code: 'RUNTIME_PROCESS_INVENTORY_UNVERIFIED', evidence: `${expected.component} has no complete prior-process inventory.` });
    } else {
      for (const previous of entry.previousProcesses) {
        if (!Number.isSafeInteger(previous?.pid) || typeof previous?.birthId !== 'string' || !previous.birthId ||
            typeof previous?.version !== 'string' || typeof previous?.path !== 'string' || !path.isAbsolute(previous.path)) {
          blockers.push({ code: 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED', evidence: `${expected.component} has an incomplete prior process record.` });
          continue;
        }
        const state = probe(previous.pid, previous.birthId);
        if (state !== 'exited') blockers.push({ code: state === 'alive' ? 'RUNTIME_OLD_PROCESS_ALIVE' : 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED',
          evidence: `${expected.component} prior ${previous.version} process ${previous.pid}/${previous.birthId} at ${previous.path} is ${state}.` });
      }
    }
  }
  const expectedNames = new Set(input.components.map((item) => item.component));
  for (const name of inventoryByName.keys()) {
    if (!expectedNames.has(name)) blockers.push({ code: 'RUNTIME_COMPONENT_UNVERIFIED', evidence: `Runtime inventory contains unverified component ${name}.` });
  }
  for (const error of input.readErrors || []) blockers.push({ code: 'PREFLIGHT_READ_ERROR', evidence: error });
  const v2BootstrapPaths = new Set(input.tasks.filter((task) => task.execution?.version === 2 && task.artifactDir)
    .map((task) => path.resolve(task.artifactDir!, 'bootstrap', `${safeThreadName(task.threadId)}.json`)));
  const v2TasksById = new Map(input.tasks.filter((task) => task.execution?.version === 2).map((task) => [task.threadId, task]));
  const protectedV2OwnerSources = new Set<string>();
  for (const owner of input.owners || []) {
    const task = owner.threadId ? v2TasksById.get(owner.threadId) : [...v2TasksById.values()].find((candidate) => {
      const candidateRun = candidate.execution?.run;
      return !!candidateRun && candidateRun.owner.pid === owner.pid && candidateRun.owner.birthId === owner.birthId &&
        candidateRun.mergeLockHelper?.channelPath === owner.helperChannelPath;
    });
    const run = task?.execution?.run;
    if (!run) continue;
    const identities = [run.owner, run.child, ...(run.subtrees || []).flatMap((tree) => [tree.root, ...tree.descendants])]
      .filter((item): item is NonNullable<typeof item> => !!item);
    const matches = (pid?: number, birthId?: string) => !pid || identities.some((identity) => identity.pid === pid &&
      !!birthId && identity.birthId === birthId);
    const source = owner.source || '';
    const leaseOwner = /^(roles|native-sessions|executions|leases)\//.test(source) && !!owner.pid && !!owner.birthId && matches(owner.pid, owner.birthId) &&
      (!owner.childPid || !!owner.childBirthId && matches(owner.childPid, owner.childBirthId));
    const helper = run.mergeLockHelper;
    const exactChannel = !!helper && !!owner.mergeToken && !!owner.helperChannelPath &&
      path.resolve(owner.helperChannelPath) === path.resolve(input.storageDir, `merge.lock.${owner.mergeToken}`) &&
      path.resolve(owner.helperChannelPath) === path.resolve(helper.channelPath);
    const exactMergeOwner = source === 'merge.lock.owner.json' && task?.status === 'merging' && task.execution?.phase === 'merge' &&
      ['starting', 'active', 'stopping'].includes(run.state) && owner.pid === run.owner.pid && owner.birthId === run.owner.birthId &&
      !!helper && owner.helperPid === helper.pid && owner.helperBirthId === helper.birthId &&
      owner.channelHelperPid === helper.pid && owner.channelHelperBirthId === helper.birthId && exactChannel &&
      (!owner.childPid || matches(owner.childPid, owner.childBirthId)) &&
      (!owner.helperChildPids?.length || owner.helperChildPids.every((pid) => identities.some((identity) => identity.pid === pid)));
    if (leaseOwner || exactMergeOwner) protectedV2OwnerSources.add(source);
  }
  for (const coordination of input.pendingCoordination || []) {
    const absolute = path.resolve(input.storageDir, coordination);
    if (v2BootstrapPaths.has(absolute)) continue;
    const normalized = coordination.replace(/\\/g, '/');
    if (protectedV2OwnerSources.has(normalized)) continue;
    const roleMatch = normalized.match(/^roles\/([^/]+)\.(?:json|lock)$/);
    if (roleMatch) {
      const threadId = path.basename(roleMatch[1], path.extname(roleMatch[1]));
      if (protectedV2OwnerSources.has(`roles/${safeThreadName(threadId)}.json`) || protectedV2OwnerSources.has(`roles/${safeThreadName(threadId)}.lock`)) continue;
    }
    blockers.push({ code: 'LEGACY_COORDINATION_REMAINS', evidence: `${coordination} must be consumed or safely dispositioned before v2 activation.` });
  }
  const inspectRuntimeIdentity = (pid: number | undefined, birthId: string | undefined, label: string, threadId: string) => {
    if (!pid) return;
    const state = birthId ? probe(pid, birthId) : 'unknown';
    if (state !== 'exited') blockers.push({ code: state === 'alive' ? 'RUNTIME_OLD_PROCESS_ALIVE' : 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED', threadId,
      evidence: `${label} PID ${pid}/${birthId || 'missing birthId'} is ${state}.` });
  };
  for (const owner of input.owners || []) {
    const source = owner.source || 'process owner';
    if (protectedV2OwnerSources.has(source)) continue;
    const check = (pid: number | undefined, birthId: string | undefined, label: string) => {
      if (!pid) return;
      const state = birthId ? probe(pid, birthId) : 'unknown';
      if (state !== 'exited') blockers.push({ code: state === 'alive' ? 'RUNTIME_OLD_PROCESS_ALIVE' : 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED',
        threadId: owner.threadId, evidence: `${source} ${label} PID ${pid}/${birthId || 'missing birthId'} is ${state}.` });
    };
    check(owner.pid, owner.birthId, 'owner');
    check(owner.childPid, owner.childBirthId, 'child');
    check(owner.helperPid, owner.helperBirthId, 'merge helper');
    for (const pid of owner.helperChildPids || []) blockers.push({ code: 'MERGE_LOCK_CHILD_UNVERIFIED', threadId: owner.threadId,
      evidence: `${source} retains child PID ${pid} without durable birthId/exit evidence.` });
    if (source.includes('merge') && owner.helperPid && owner.helperBirthId) {
      const state = probe(owner.helperPid, owner.helperBirthId);
      const ack = owner.helperReleaseAck?.split(':');
      const exactAck = !!ack && Number(ack[0]) === owner.helperPid && ack[1] === owner.helperBirthId;
      if (state !== 'exited' || (process.platform === 'win32' && !exactAck)) blockers.push({
        code: state === 'alive' ? 'MERGE_HELPER_STILL_ALIVE' : 'MERGE_HELPER_RELEASE_UNVERIFIED', threadId: owner.threadId,
        evidence: `${source} helper release is not confirmed (identity=${state}, exact ACK=${exactAck}).`,
      });
    }
  }
  for (const task of input.tasks) {
    if (task.execution?.version === 2) continue;
    const run = task.execution?.run;
    if (run) {
      if (!run.owner?.pid || !run.owner.birthId) blockers.push({ code: 'RUNTIME_PROCESS_IDENTITY_UNVERIFIED', threadId: task.threadId,
        evidence: `Historical run ${run.id} has no complete controller PID/birthId.` });
      inspectRuntimeIdentity(run.owner?.pid, run.owner?.birthId, 'Historical run owner', task.threadId);
      inspectRuntimeIdentity(run.child?.pid, run.child?.birthId, 'Historical run child', task.threadId);
      inspectRuntimeIdentity(run.mergeLockHelper?.pid, run.mergeLockHelper?.birthId, 'Historical merge helper', task.threadId);
      for (const tree of run.subtrees || []) {
        inspectRuntimeIdentity(tree.root.pid, tree.root.birthId, 'Historical subtree root', task.threadId);
        for (const child of tree.descendants) inspectRuntimeIdentity(child.pid, child.birthId, 'Historical descendant', task.threadId);
      }
      blockers.push({ code: 'RUN_RECORD_REMAINS', threadId: task.threadId,
        evidence: `Historical execution.version=${task.execution?.version ?? 'unknown'} run ${run.id} remains recorded (${run.state}/${run.step}).` });
    }
    if (task.execution?.next) blockers.push({ code: 'PENDING_HANDOFF', threadId: task.threadId,
      evidence: `Historical ${task.execution.next.phase} handoff from ${task.execution.next.sourceRunId || 'unknown source run'} remains unconsumed.` });
    if (task.cleanup) blockers.push({ code: 'CLEANUP_IN_PROGRESS', threadId: task.threadId,
      evidence: `Historical ${task.cleanup.action} cleanup token ${task.cleanup.token} remains active.` });
    const deliveryProblem = historicalDeliveryProblem(task);
    if (task.status !== 'completed' || (task.rounds || []).some((round) => round.status === 'running') ||
        (task.decision && !task.decision.answer) || (task.execution?.queue) ||
        (task.reviewWake && !task.reviewWake.gaveUp) ||
        (task.reviewBootstrap && ['pending', 'launching'].includes(task.reviewBootstrap.phase)) || deliveryProblem || task.failure?.stage === 'merge') {
      blockers.push({ code: 'HISTORICAL_TASK_NOT_DRAINED', threadId: task.threadId,
        evidence: `Historical task retains active status, undelivered Git work, or in-flight decision, retry, wake, bootstrap, or round state (${task.status})${deliveryProblem ? `: ${deliveryProblem}` : '.'}` });
    }
  }
  const v2TaskIds = new Set(input.tasks.filter((task) => task.execution?.version === 2).map((task) => task.threadId));
  for (const transaction of input.pendingMergeTransactions || []) {
    if (transaction.threadId && v2TaskIds.has(transaction.threadId)) continue;
    blockers.push({ code: 'GIT_RECOVERY_PENDING', threadId: transaction.threadId,
      evidence: `${transaction.path} is in phase ${transaction.phase || 'unknown'} and requires legacy recovery.` });
  }
  const latestDeleted = new Map<string, any>();
  for (const tombstone of input.tombstones || []) if (typeof tombstone?.threadId === 'string') latestDeleted.set(tombstone.threadId, tombstone);
  for (const [threadId, tombstone] of latestDeleted) {
    const manifest = tombstone.cleanupManifest;
    if (!manifest || manifest.stage === 'complete' || tombstone.cleanupComplete) continue;
    const run = manifest.execution?.run;
    const proof = manifest.runExitProof;
    if (run && (!proof || proof.runId !== run.id || proof.operationId !== manifest.execution?.operationId || proof.generation !== manifest.execution?.generation)) {
      blockers.push({ code: 'DELETED_RUN_PROTECTED', threadId,
        evidence: `Deleted task run ${run.id} has no matching process-tree/Job exit acknowledgement.` });
      inspectRuntimeIdentity(run.owner?.pid, run.owner?.birthId, 'Deleted run owner', threadId);
      inspectRuntimeIdentity(run.child?.pid, run.child?.birthId, 'Deleted run child', threadId);
      for (const tree of run.subtrees || []) {
        inspectRuntimeIdentity(tree.root?.pid, tree.root?.birthId, 'Deleted subtree root', threadId);
        for (const child of tree.descendants || []) inspectRuntimeIdentity(child.pid, child.birthId, 'Deleted subtree descendant', threadId);
        if (tree.windowsJob) blockers.push({ code: 'DELETED_JOB_RECEIPT_RETAINED', threadId,
          evidence: `Deleted task retains Windows Job receipt ${tree.windowsJob.id}.` });
      }
      if (run.mergeLockHelper) inspectRuntimeIdentity(run.mergeLockHelper.pid, run.mergeLockHelper.birthId, 'Deleted merge helper', threadId);
    }
    if (manifest.retainResources) blockers.push({ code: 'DELETED_RESOURCES_RETAINED', threadId,
      evidence: `Deleted task resources remain protected by token ${manifest.token || tombstone.cleanupToken || 'unknown'}.` });
    blockers.push({ code: 'DELETED_CLEANUP_PENDING', threadId,
      evidence: `Deleted task cleanup is ${manifest.stage || 'pending'} under token ${manifest.token || tombstone.cleanupToken || 'unknown'}.` });
  }
  return { ready: blockers.length === 0, inventoryPath, blockers, components: input.components };
}
