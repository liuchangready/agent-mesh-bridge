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
import * as os from 'os';
import { AutonomousRunner } from './autonomous-runner';
import { AgentDetector } from './agent-detector';
import { concurrencyPolicy } from './bridge-policy';
import { ExecutionGate, normalizeWorkspaceKey } from './execution-gate';
import { TaskStore } from './task-store';
import { bootstrapDir } from './task-summary';
import { reviewerBootstrapLogPath } from './reviewer-log';
import {
  DelegationThread,
  ReviewFlow,
  appendInterruption,
  findQueueHead,
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

export function reviewerBootstrapTokenPath(threadId: string, root?: string): string {
  const safe = String(threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');
  return path.join(bootstrapDir(root), `${safe}.json`);
}

export function writeReviewerBootstrapToken(
  record: ReviewerBootstrapToken,
  root?: string,
): boolean {
  try {
    fs.mkdirSync(bootstrapDir(root), { recursive: true });
    fs.writeFileSync(reviewerBootstrapTokenPath(record.threadId, root), JSON.stringify(record), {
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
): ReviewerBootstrapToken | undefined {
  try {
    const parsed = JSON.parse(
      fs.readFileSync(reviewerBootstrapTokenPath(threadId, root), 'utf8'),
    );
    return parsed && typeof parsed.token === 'string' ? (parsed as ReviewerBootstrapToken) : undefined;
  } catch {
    return undefined;
  }
}

export function clearReviewerBootstrapToken(threadId: string, root?: string): void {
  try {
    fs.unlinkSync(reviewerBootstrapTokenPath(threadId, root));
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
): void {
  if (!hostPid || !Number.isFinite(hostPid)) return;
  const record = readReviewerBootstrapToken(threadId, root);
  if (!record) return;
  writeReviewerBootstrapToken({ ...record, hostPid }, root);
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
): ReviewerBootstrapToken | undefined {
  if (!hostPid || !Number.isFinite(hostPid)) return undefined;
  let names: string[];
  try {
    names = fs.readdirSync(bootstrapDir(root));
  } catch {
    return undefined;
  }

  const now = Date.now();
  let found: ReviewerBootstrapToken | undefined;
  for (const name of names) {
    if (!name.endsWith('.json')) continue;
    let record: ReviewerBootstrapToken | undefined;
    try {
      record = JSON.parse(fs.readFileSync(path.join(bootstrapDir(root), name), 'utf8'));
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

/**
 * Review topology for a NEW task.
 *
 * Dedicated requires a long-lived process to launch the reviewer bootstrap; the
 * mirrored policy says the extension-host supervisor is available. A standalone
 * MCP server (or a test without the policy snapshot) stays legacy instead of
 * creating a task that could never be dispatched.
 */
export function resolveReviewFlow(
  override?: ReviewFlow,
  policyFlow?: ReviewFlow,
): ReviewFlow {
  if (override === 'legacy' || override === 'dedicated') return override;
  return policyFlow === 'dedicated' ? 'dedicated' : 'legacy';
}

export interface ReviewerBootstrapDeps {
  taskStore: TaskStore;
  runner: AutonomousRunner;
  detector: AgentDetector;
  executionGate: ExecutionGate;
  storageDir?: string;
  /** Overall budget for one bootstrap turn. */
  timeoutMs?: number;
  /** Called after a bootstrap settles so the queue can advance. */
  onSettled?: (threadId: string) => void;
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
  let names: string[];
  try {
    names = fs.readdirSync(bootstrapDir(root));
  } catch {
    return undefined;
  }

  const now = Date.now();
  const candidates: ReviewerBootstrapToken[] = [];
  for (const name of names) {
    if (!name.endsWith('.json')) continue;
    let record: ReviewerBootstrapToken | undefined;
    try {
      record = JSON.parse(fs.readFileSync(path.join(bootstrapDir(root), name), 'utf8'));
    } catch {
      continue;
    }
    if (!record || record.agent !== agent || typeof record.token !== 'string') continue;
    const created = Date.parse(record.createdAt || '');
    if (!Number.isFinite(created) || now - created > BOOTSTRAP_HOST_TTL_MS) continue;
    const thread = query.taskStore.getTask(record.threadId);
    if (!thread || thread.initiatorAgent !== agent || !isReviewerBootstrapPending(thread)) continue;
    candidates.push(record);
  }

  const inWorkspace = query.workspacePath
    ? candidates.filter((record) => {
        const thread = query.taskStore.getTask(record.threadId);
        return !!thread && sameWorkspace(thread.workspacePath, query.workspacePath as string);
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
    '',
    'The bridge validates that token against the pending task and fills in the brief itself. Do NOT pass',
    'title/prompt/expectedFiles, do NOT explore the repository, do NOT modify files, and do NOT start any',
    'other task.',
    '',
    'After the tool returns, end your turn immediately. Do not poll for progress; the bridge will',
    'resume THIS conversation when the developer report is ready for review.',
  ].join('\n');
}

/**
 * Launches and supervises the one bootstrap turn per dedicated task.
 *
 * It respects the task-level FIFO (only the queue head is bootstrapped) and the
 * execution gate (the bootstrap turn is a real agent process).
 */
export class ReviewerBootstrapService {
  private deps: ReviewerBootstrapDeps;
  private timer: NodeJS.Timeout | null = null;
  private launching = new Set<string>();
  private disposed = false;

  constructor(deps: ReviewerBootstrapDeps) {
    this.deps = deps;
  }

  public start(intervalMs: number = 1500): void {
    if (this.timer) return;
    this.timer = setInterval(() => void this.tick(), Math.max(250, intervalMs));
    this.timer.unref?.();
    void this.tick();
  }

  public stop(): void {
    this.disposed = true;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * One pass: bootstrap the head of each startable workspace.
   *
   * With bounded concurrency enabled, a pending task in workspace B must not be
   * blocked by an older task running in workspace A (the queue is per
   * workspace). Without concurrency, the historical single global head applies.
   */
  public async tick(): Promise<void> {
    if (this.disposed) return;
    const store = this.deps.taskStore;
    store.loadFromDisk();
    const tasks = store.getAllTasks();
    const policy = concurrencyPolicy(this.deps.storageDir);

    const pending = tasks
      .filter((thread) => isReviewerBootstrapPending(thread))
      .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());

    for (const candidate of pending) {
      if (candidate.reviewBootstrap?.phase === 'failed') continue;
      if (this.launching.has(candidate.threadId)) continue;
      if ((candidate.reviewBootstrap?.attempts || 0) >= 3) {
        this.fail(
          candidate,
          candidate.reviewBootstrap?.lastError || 'reviewer bootstrap attempts exhausted',
        );
        continue;
      }

      if (policy.enabled) {
        const workspaceKey = normalizeWorkspaceKey(candidate.workspacePath);
        const peers = tasks.filter(
          (task) => normalizeWorkspaceKey(task.workspacePath) === workspaceKey,
        );
        const head = findQueueHead(peers);
        if (head?.threadId !== candidate.threadId) continue;
        // Another task from this workspace already owns its execution slot.
        if (this.deps.executionGate.getOwner(workspaceKey)) continue;
      } else {
        // Strict single-flight: only the global queue head may start, and only
        // while the one execution slot is free.
        const head = findQueueHead(tasks);
        if (head?.threadId !== candidate.threadId) return;
        if (this.deps.executionGate.isBusy()) return;
      }

      this.launching.add(candidate.threadId);
      try {
        await this.launch(candidate);
      } catch (e: any) {
        console.warn(`[ReviewerBootstrap] Failed for ${candidate.threadId}: ${e?.message || e}`);
      } finally {
        this.launching.delete(candidate.threadId);
      }
      // One bootstrap turn per tick; the next tick handles the next workspace.
      return;
    }
  }

  /** Launch the fresh reviewer conversation and wait for its bootstrap turn. */
  private async launch(thread: DelegationThread): Promise<void> {
    const store = this.deps.taskStore;
    const root = this.deps.storageDir;
    const tokenRecord = readReviewerBootstrapToken(thread.threadId, root);
    if (!tokenRecord) {
      this.fail(thread, 'Reviewer bootstrap token is missing (the task was created before the bootstrap service raced it?).');
      return;
    }

    const preassigned = crypto.randomUUID();
    const policy = concurrencyPolicy(root);
    const workspaceKey = policy.enabled ? normalizeWorkspaceKey(thread.workspacePath) : undefined;
    const lease = this.deps.executionGate.tryAcquire(
      thread.threadId,
      0,
      workspaceKey,
      thread.initiatorAgent,
    );
    if (!lease) return; // another process owns the slot; retry on the next tick

    const attempt = (thread.reviewBootstrap?.attempts || 0) + 1;
    thread.reviewBootstrap = {
      ...(thread.reviewBootstrap || { phase: 'pending' }),
      phase: 'launching',
      attempts: attempt,
      launchedAt: new Date().toISOString(),
    };
    thread.summary.lastOutputSnippet = 'Starting the dedicated reviewer conversation...';
    store.upsertTask(thread);

    const logFile = reviewerBootstrapLogPath(thread.threadId, root, thread.initiatorAgent);
    try {
      fs.mkdirSync(path.dirname(logFile), { recursive: true });
      fs.writeFileSync(logFile, '', 'utf8');
    } catch {}

    let result;
    let runError: any;
    try {
      result = await this.deps.runner.execute({
        agent: thread.initiatorAgent,
        prompt: buildReviewerBootstrapPrompt(thread, tokenRecord.token),
        isRework: false,
        receiverSessionId: preassigned,
        title: `Reviewer bootstrap: ${thread.title}`,
        cwd: thread.workspacePath,
        taskThreadId: thread.threadId,
        taskRound: 0,
        model: thread.reviewerModel,
        markAsReceiver: false,
        timeoutMs: this.deps.timeoutMs ?? 3 * 60 * 1000,
        silenceTimeoutMs: 2 * 60 * 1000,
        extraEnv: {
          MESH_REVIEW_BOOTSTRAP: '1',
          MESH_REVIEW_BOOTSTRAP_TOKEN: tokenRecord.token,
          MESH_REVIEW_THREAD_ID: thread.threadId,
          MESH_CALLER_AGENT: thread.initiatorAgent,
        },
        onChildPid: (pid) => {
          this.deps.executionGate.attachChildPid(pid, lease);
          // Environment-independent half of the handshake: the bridge server
          // that this reviewer CLI spawns can identify itself by this pid even
          // when the host scrubs the inherited environment.
          if (pid) recordReviewerBootstrapHostPid(thread.threadId, pid, root);
        },
        onStdoutChunk: (chunk: string) => {
          try {
            fs.appendFileSync(logFile, chunk, 'utf8');
          } catch {}
        },
        onStderrChunk: (chunk: string) => {
          try {
            fs.appendFileSync(logFile, chunk, 'utf8');
          } catch {}
        },
      });
    } catch (e: any) {
      runError = e;
    } finally {
      this.deps.executionGate.release(lease);
    }

    if (!result) {
      const failedAttempt = store.getTask(thread.threadId) || thread;
      failedAttempt.reviewBootstrap = {
        ...(failedAttempt.reviewBootstrap || { phase: 'pending', attempts: attempt }),
        phase: attempt >= 3 ? 'failed' : 'pending',
        lastError: `reviewer launch failed: ${runError?.message || runError}`,
      };
      store.upsertTask(failedAttempt);
      if (attempt >= 3) this.fail(failedAttempt, `reviewer launch failed: ${runError?.message || runError}`);
      this.deps.onSettled?.(thread.threadId);
      return;
    }

    const fresh = store.getTask(thread.threadId);
    if (!fresh) {
      clearReviewerBootstrapToken(thread.threadId, root);
      this.deps.onSettled?.(thread.threadId);
      return;
    }

    if (fresh.reviewBootstrap?.phase === 'confirmed') {
      const sessionId = result.actualSessionId || preassigned;
      fresh.reviewerSessionId = sessionId;
      fresh.reviewerSessionCaptured = true;
      fresh.callerSessionId = sessionId;
      fresh.reviewBootstrap = {
        ...fresh.reviewBootstrap,
        phase: 'confirmed',
        reviewerSessionId: sessionId,
      };
      fresh.status = 'queued';
      fresh.summary.lastOutputSnippet =
        'Reviewer conversation dispatched the task; waiting for the execution slot.';
      clearReviewerBootstrapToken(fresh.threadId, root);
      store.upsertTask(fresh);
    } else {
      const detail =
        result.error ||
        (result.exitCode === 0
          ? `the reviewer turn ended without calling dispatch_task (reviewer log: ${logFile})`
          : `reviewer process exited with code ${result.exitCode} (reviewer log: ${logFile})`);
      this.fail(fresh, detail);
    }
    this.deps.onSettled?.(thread.threadId);
  }

  /** Mark a bootstrap as failed without touching the developer's work. */
  private fail(thread: DelegationThread, detail: string): void {
    const fresh = this.deps.taskStore.getTask(thread.threadId) || thread;
    const at = new Date().toISOString();
    fresh.reviewBootstrap = {
      ...(fresh.reviewBootstrap || { phase: 'pending', attempts: 0 }),
      phase: 'failed',
      lastError: detail,
    };
    fresh.status = 'failed';
    fresh.summary.lastOutputSnippet = `[Reviewer bootstrap failed] ${detail.slice(0, 140)}`;
    const round = fresh.rounds && fresh.rounds.length > 0
      ? fresh.rounds[fresh.rounds.length - 1]
      : undefined;
    if (round && round.status === 'running') {
      round.status = 'failed';
      round.error = `Reviewer bootstrap failed: ${detail}`;
    }
    appendInterruption(fresh, {
      kind: 'stopped',
      reason: `Reviewer bootstrap failed: ${detail}`,
      at,
      by: 'bridge',
    });
    clearReviewerBootstrapToken(fresh.threadId, this.deps.storageDir);
    this.deps.taskStore.upsertTask(fresh);
  }
}
