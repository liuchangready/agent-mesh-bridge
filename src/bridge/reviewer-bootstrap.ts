import { getTaskExecutionPath } from './types';
/**
 * Dedicated reviewer bootstrap (plan B).
 *
 * New tasks do not hand the developer round straight to the executor. The
 * bridge first launches a FRESH conversation of the reviewer agent, whose only
 * job in that turn is to call `dispatch_task` with the one-time token from its
 * environment. Confirming the token is what actually starts the developer.
 *
 * Why this exists:
 *  - the reviewer no longer resumes the requester's busy conversation, so the
 *    same agent may dispatch to itself without hitting a native writer lock;
 *  - the reviewer conversation is idle while the developer works, so resuming it
 *    for review is safe;
 *  - the role/writer lock can be bound to that conversation id instead of the
 *    requester session.
 *
 * Legacy tasks never enter this path.
 */

import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { TaskStore } from './task-store';
import { requireV2ExecutionEligibility } from './execution-gate';
import { bootstrapDir } from './task-summary';
import { taskBootstrapDir, bridgeHomeDir } from './round-log';
import { resolveTaskDataRoot } from './task-data-dir';
import {
  DelegationThread,
  ReviewFlow,
  isReviewerBootstrapPending,
} from './types';

/** One-time record handed to the bootstrap reviewer process. */
export interface ReviewerBootstrapToken {
  threadId: string;
  token: string;
  agent: string;
  createdAt: string;
  /**
   * PID of the reviewer CLI process the bridge launched for this bootstrap.
   *
   * Filled in as soon as the child exists. It is the environment-independent
   * half of the handshake: some hosts (Codex, DeepSeek Harness) spawn their MCP
   * servers with a closed/scrubbed environment, so `MESH_REVIEW_*` never
   * reaches the bridge server, but the server IS a direct child of this pid.
   */
  hostPid?: number;
}

/**
 * How long a launch record stays usable for pid matching.
 *
 * A bootstrap turn is bounded by a few minutes; the slack only covers machines
 * that were suspended mid-launch. Anything older is stale state from a crash,
 * and pid reuse must not be able to confirm it.
 */
const BOOTSTRAP_HOST_TTL_MS = 30 * 60 * 1000;

/** The one tool a bootstrap turn exists to call. */
const REVIEWER_BOOTSTRAP_TOOL = 'dispatch_task';

export function reviewerBootstrapTokenPath(
  threadId: string,
  root?: string,
  artifactDir?: string,
): string {
  const safe = String(threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');
  // New tasks keep the one-shot credential inside their unified task dir so
  // archiving/deleting the task also removes the credential. Legacy tasks keep
  // the historical global bootstrap/ location.
  if (artifactDir) return path.join(taskBootstrapDir(artifactDir), `${safe}.json`);
  return path.join(bootstrapDir(root), `${safe}.json`);
}

export function writeReviewerBootstrapToken(
  record: ReviewerBootstrapToken,
  root?: string,
  artifactDir?: string,
): boolean {
  try {
    const file = reviewerBootstrapTokenPath(record.threadId, root, artifactDir);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, JSON.stringify(record), {
      encoding: 'utf8',
      mode: 0o600,
    });
    return true;
  } catch (e: any) {
    console.warn(`[ReviewerBootstrap] Could not write token: ${e?.message || e}`);
    return false;
  }
}

export function readReviewerBootstrapToken(
  threadId: string,
  root?: string,
  artifactDir?: string,
): ReviewerBootstrapToken | undefined {
  try {
    const parsed = JSON.parse(
      fs.readFileSync(reviewerBootstrapTokenPath(threadId, root, artifactDir), 'utf8'),
    );
    return parsed && typeof parsed.token === 'string' ? (parsed as ReviewerBootstrapToken) : undefined;
  } catch {
    return undefined;
  }
}

export function clearReviewerBootstrapToken(
  threadId: string,
  root?: string,
  artifactDir?: string,
): void {
  try {
    fs.unlinkSync(reviewerBootstrapTokenPath(threadId, root, artifactDir));
  } catch {}
}

/**
 * Attach the launched reviewer CLI pid to an existing token record.
 *
 * Called from the runner's `onChildPid` callback, i.e. before the model has a
 * chance to call `dispatch_task`, so the pid is always available to the fallback
 * path below.
 */
export function recordReviewerBootstrapHostPid(
  threadId: string,
  hostPid: number,
  root?: string,
  artifactDir?: string,
): void {
  if (!hostPid || !Number.isFinite(hostPid)) return;
  const record = readReviewerBootstrapToken(threadId, root, artifactDir);
  if (!record) return;
  writeReviewerBootstrapToken({ ...record, hostPid }, root, artifactDir);
}

function bootstrapRecordFiles(root?: string, taskStore?: TaskStore): string[] {
  const files = new Set<string>();
  const addDir = (dir: string) => {
    try {
      for (const name of fs.readdirSync(dir)) {
        if (name.endsWith('.json')) files.add(path.join(dir, name));
      }
    } catch {}
  };
  // Legacy tasks keep the global bootstrap/ location.
  addDir(bootstrapDir(root));
  // New tasks own `<artifactDir>/bootstrap/<threadId>.json`. Scan the current
  // root for the pid fallback, and the ledger-recorded dirs when a store is
  // available (a policy change must not hide a pending credential).
  try {
    const tasksRoot = resolveTaskDataRoot(bridgeHomeDir(root), root);
    for (const entry of fs.readdirSync(tasksRoot, { withFileTypes: true })) {
      if (entry.isDirectory()) addDir(path.join(tasksRoot, entry.name, 'bootstrap'));
    }
  } catch {}
  for (const thread of taskStore?.getAllTasks() || []) {
    if (thread.artifactDir) addDir(taskBootstrapDir(thread.artifactDir));
  }
  return [...files];
}

/**
 * Find the bootstrap that belongs to the process which launched this MCP server.
 *
 * Why this exists: `MESH_REVIEW_BOOTSTRAP*` is only visible to a host that
 * inherits its environment. Codex passes a closed environment to the MCP servers
 * it spawns (only the `env` table from `config.toml`), and DeepSeek Harness
 * scrubs inherited variables, so the environment handshake simply never fires
 * there. The bridge server, however, is still a direct child of the reviewer CLI
 * the bridge itself launched — matching that pid identifies the same process
 * without any cooperation from the host.
 */
export function findReviewerBootstrapByHostPid(
  hostPid: number | undefined,
  root?: string,
  taskStore?: TaskStore,
): ReviewerBootstrapToken | undefined {
  if (!hostPid || !Number.isFinite(hostPid)) return undefined;

  const now = Date.now();
  let found: ReviewerBootstrapToken | undefined;
  for (const file of bootstrapRecordFiles(root, taskStore)) {
    let record: ReviewerBootstrapToken | undefined;
    try {
      record = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      continue;
    }
    if (!record || record.hostPid !== hostPid || typeof record.token !== 'string') continue;
    const created = Date.parse(record.createdAt || '');
    if (!Number.isFinite(created) || now - created > BOOTSTRAP_HOST_TTL_MS) continue;
    // One reviewer process owns one bootstrap; the newest record wins if a
    // crashed launch left an older record behind.
    if (!found || created > Date.parse(found.createdAt || '')) found = record;
  }
  return found;
}

/** A stale policy snapshot must never resume the requester for new work. */
export function resolveReviewFlow(override?: ReviewFlow): 'dedicated' {
  if (override !== undefined && override !== 'dedicated') {
    throw new Error('LEGACY_REVIEW_FLOW_DISABLED: New tasks require a dedicated reviewer.');
  }
  return 'dedicated';
}

/**
 * Find the only pending bootstrap a reviewer process could be claiming.
 *
 * Last-resort channel for hosts that neither forward `MESH_REVIEW_*` nor let the
 * bridge server see its real parent pid (a brokered MCP server is a child of the
 * broker, not of the CLI the bridge launched). A call without a task brief can
 * only be a reviewer, so a single pending bootstrap for the same agent — and
 * preferentially the same working directory — is unambiguous. Anything less
 * certain returns undefined so the caller keeps its normal (invalid) dispatch
 * behaviour instead of confirming the wrong task.
 */
export function findSolePendingBootstrap(
  query: { agent?: string; workspacePath?: string; taskStore: TaskStore },
  root?: string,
): ReviewerBootstrapToken | undefined {
  const agent = (query.agent || '').trim();
  if (!agent) return undefined;

  const now = Date.now();
  const candidates: ReviewerBootstrapToken[] = [];
  for (const file of bootstrapRecordFiles(root, query.taskStore)) {
    let record: ReviewerBootstrapToken | undefined;
    try {
      record = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {
      continue;
    }
    if (!record || record.agent !== agent || typeof record.token !== 'string') continue;
    const created = Date.parse(record.createdAt || '');
    if (!Number.isFinite(created) || now - created > BOOTSTRAP_HOST_TTL_MS) continue;
    const thread = query.taskStore.getTask(record.threadId);
    if (
      !thread || thread.initiatorAgent !== agent ||
      thread.reviewBootstrap?.phase === 'failed' || !isReviewerBootstrapPending(thread)
    ) continue;
    candidates.push(record);
  }

  const inWorkspace = query.workspacePath
    ? candidates.filter((record) => {
        const thread = query.taskStore.getTask(record.threadId);
        return !!thread && sameWorkspace(getTaskExecutionPath(thread), query.workspacePath as string);
      })
    : [];
  if (inWorkspace.length === 1) return inWorkspace[0];
  if (inWorkspace.length > 1) return undefined;
  return candidates.length === 1 ? candidates[0] : undefined;
}

function sameWorkspace(a?: string, b?: string): boolean {
  if (!a || !b) return false;
  try {
    const norm = (value: string) => path.resolve(value).replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase();
    return norm(a) === norm(b);
  } catch {
    return false;
  }
}

/**
 * Builds the bootstrap prompt: one tool call, nothing else.
 *
 * The one-time credential travels IN the prompt as well as in the environment:
 * a host may strip the environment (Codex, DeepSeek Harness) and may also hide
 * the real parent pid, and a reviewer that sends no arguments then loses its
 * prepared task ("the reviewer turn ended without calling dispatch_task").
 */
export function buildReviewerBootstrapPrompt(thread: DelegationThread, token?: string): string {
  const credential = token
    ? `{"reviewBootstrap": {"threadId": "${thread.threadId}", "token": "${token}"}}`
    : `{"reviewBootstrap": {"threadId": "${thread.threadId}", "token": "<one-time token from your environment>"}}`;
  return [
    '[Agent Mesh Bridge — dedicated reviewer bootstrap]',
    `You are the dedicated reviewer for task "${thread.title}" (thread ${thread.threadId}).`,
    '',
    'Your ONLY action in this turn: call the MCP tool `dispatch_task` exactly once, with these arguments:',
    credential,
    ...(thread.taskTemplate ? ['', '[Task template reviewer instructions]', thread.taskTemplate.reviewerInstructions] : []),
    '',
    'The bridge validates that token against the pending task and fills in the brief itself. Do NOT pass',
    'title or prompt, do NOT explore the repository, do NOT modify files, and do NOT start any',
    'other task.',
    '',
    'If `dispatch_task` is not in your tool list yet, the bridge MCP server is still starting: this is a',
    'launch race, not a missing feature. In that case do NOT end the turn and do NOT report the tool as',
    'unavailable. Wait a few seconds (for example by listing your tools again, or a short sleep) and retry',
    '— up to ~60 seconds. Only if it is still absent after that may you say so.',
    '',
    'After the tool returns, end your turn immediately. Do not poll for progress; the bridge will',
    'resume THIS conversation when the developer report is ready for review.',
  ].join('\n');
}

/** Consume the exact one-time reviewer credential and persist its handoff once. */
export async function confirmReviewerBootstrapTask(
  taskStore: TaskStore,
  threadId: string,
  credential: { token?: string; hostPid?: number },
  root?: string,
): Promise<DelegationThread> {
  taskStore.loadFromDisk();
  const thread = taskStore.getTask(threadId);
  requireV2ExecutionEligibility(thread, 'confirm reviewer bootstrap');
  const isConfirmableRun = (task: DelegationThread) => {
    const execution = task.execution;
    return task.reviewFlow === 'dedicated' && !task.cleanup && !task.archivedAt &&
      (task.status === 'queued' || task.status === 'reviewing') && execution?.version === 2 &&
      execution.phase === 'review' && execution.purpose === 'bootstrap' &&
      execution.run?.role === 'reviewer' && execution.run.state !== 'stopping' && !execution.run.unconfirmed;
  };
  if (!isConfirmableRun(thread)) throw new Error(`Reviewer bootstrap for ${threadId} has no active v2 run.`);
  if (thread.reviewBootstrap?.phase === 'confirmed') throw new Error(`Thread ${threadId} was already dispatched by its reviewer.`);
  if (thread.reviewBootstrap?.phase === 'failed') throw new Error(`Thread ${threadId} failed its reviewer bootstrap; it cannot be dispatched anymore.`);
  const record = readReviewerBootstrapToken(threadId, root, thread.artifactDir);
  if (!record) throw new Error(`Reviewer bootstrap token for ${threadId} is missing or was already consumed.`);
  if (credential.token && record.token !== credential.token)
    throw new Error(`Reviewer bootstrap token mismatch for ${threadId}.`);
  if (!credential.token && credential.hostPid && record.hostPid !== credential.hostPid)
    throw new Error(`Reviewer bootstrap host process mismatch for ${threadId}.`);
  if (!credential.token && !credential.hostPid)
    throw new Error(`Reviewer bootstrap credential is missing for ${threadId}.`);
  if (record.agent !== thread.initiatorAgent) throw new Error(`Reviewer bootstrap agent mismatch for ${threadId}.`);
  const source = thread.execution!;
  const matchesSource = (task: DelegationThread) => isConfirmableRun(task) &&
    task.execution!.run?.id === source.run!.id && task.execution!.operationId === source.operationId &&
    task.execution!.generation === source.generation;
  let confirmed: DelegationThread | undefined;
  for (let attempt = 0; attempt < 6; attempt++) {
    const latest = taskStore.reloadTask(threadId);
    if (!latest || !matchesSource(latest)) {
      throw new Error(`Reviewer bootstrap run changed before confirmation for ${threadId}; its token was not consumed.`);
    }
    if (latest.reviewBootstrap?.phase === 'failed') {
      throw new Error(`Thread ${threadId} failed its reviewer bootstrap; it cannot be dispatched anymore.`);
    }
    if (latest.reviewBootstrap?.phase === 'confirmed') {
      confirmed = latest;
      break;
    }
    try {
      confirmed = await taskStore.transitionExecution(threadId, latest.execution!.revision, (fresh) => {
        if (!matchesSource(fresh)) throw new Error(`Reviewer bootstrap run changed before confirmation for ${threadId}.`);
        fresh.reviewBootstrap = { ...(fresh.reviewBootstrap || { attempts: 0 }), phase: 'confirmed' };
        fresh.summary.lastOutputSnippet = 'Reviewer dispatched the task; waiting for the bootstrap process to persist its native session.';
      });
      break;
    } catch (error: any) {
      if (!String(error?.message || error).includes('revision mismatch') || attempt === 5) throw error;
    }
  }
  if (!confirmed) throw new Error(`Reviewer bootstrap confirmation did not commit for ${threadId}.`);
  // The credential is consumed only after the confirmation is durable.
  clearReviewerBootstrapToken(threadId, root, confirmed.artifactDir);
  return confirmed;
}
