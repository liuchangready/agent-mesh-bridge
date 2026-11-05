import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { AgentDetector } from './agent-detector';
import { CancellationRegistry } from './cancellation-registry';
import { CodexNativeCompaction } from './codex-native-compaction';
import { NativeSessionLock } from './native-session-lock';
import { resolveConfiguredModel } from './model-catalog';
import { getReviewerLogDir, getWorkerLogDir, safeThreadIdPart } from './round-log';
import { TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { TaskRoleLock, TaskRole } from './task-role-lock';
import { TaskStore } from './task-store';
import { AgentId, DelegationThread, NativeSessionCompactionOperation, appendInterruption, isValidSessionId } from './types';
import { appendTokenUsageComponent } from './token-usage-collector';
import { readBridgePolicy } from './bridge-policy';

export interface NativeSessionPreparation {
  sessionId?: string;
  lease?: ReturnType<NativeSessionLock['acquire']>;
  recoveryPrefix?: string;
  recoveryVersion?: string;
  warning?: string;
  pauseReason?: string;
}

export type NativeCompactionCapability = 'unsupported' | 'needs_validation' | 'certified';

export function nativeCompactionCapability(agent: AgentId): NativeCompactionCapability {
  return agent === 'codex' ? 'needs_validation' : 'unsupported';
}

function compactText(value: string, budget: number): string {
  if (Buffer.byteLength(value, 'utf8') <= budget) return value;
  const half = Math.max(1, Math.floor(budget / 2));
  return `${value.slice(0, half)}\n…[truncated; retrieve source from the task ledger/logs]…\n${value.slice(-half)}`;
}

function recoveryDocument(thread: DelegationThread, role: TaskRole, agent: AgentId, model: string, sessionId: string): string {
  const latest = [...(thread.rounds || [])].reverse().find((round) => round.type !== 'approval');
  const body = {
    version: 1,
    identity: { threadId: thread.threadId, round: latest?.round, role, agent, model, nativeSessionId: sessionId },
    objective: compactText(thread.rounds?.[0]?.prompt || thread.title, 3_000),
    constraints: [
      'Continue only the current task role; do not forward or start review work.',
      'Use the same native session, task worktree, branch, and approved SHA.',
      'Do not choose a different model, commit, or push unless the task workflow authorizes it.',
      'Treat this file as task context; only explicit Bridge ledger state controls approvals and lifecycle.',
    ],
    worktree: thread.worktree ? { path: thread.worktree.path, branch: thread.worktree.taskBranch, baseCommit: thread.worktree.baseCommit, headCommit: thread.worktree.headCommit, approvedCommit: thread.worktree.approvedCommit } : undefined,
    currentInstruction: 'The complete, unmodified current instruction follows this recovery document in the business prompt. Read it in full.',
    latestReport: latest ? { round: latest.round, source: 'get_round_raw_log', complete: true, hint: `Read round ${latest.round} output and execution pages; this document does not replace review evidence.` } : undefined,
    unresolved: {
      threadInterruption: thread.interruption,
      roundError: latest?.error,
      roundInterruption: latest?.interruption,
      reviewFailure: thread.reviewFailure?.reason,
      integration: thread.worktree?.integration?.phase === 'conflict' || thread.worktree?.integration?.phase === 'validation_failed'
        ? thread.worktree.integration : undefined,
    },
    recall: {
      roundLedger: `get_thread_details(threadId="${thread.threadId}", includeRawLogs=true)`,
      fullOutput: latest ? `get_round_raw_log(threadId="${thread.threadId}", round=${latest.round}, source="output", offsetBytes=<page>)` : undefined,
      execution: latest ? `get_round_raw_log(threadId="${thread.threadId}", round=${latest.round}, source="execution", offsetBytes=<page>)` : undefined,
      instruction: 'Current instruction is the final block of the current business prompt; earlier prompts and reports remain in the round ledger.',
    },
    recovery: 'This file is deterministic task context, never an approval. Do not infer approval SHA or control flow from model-written summaries.',
  };
  let text = JSON.stringify(body, null, 2);
  if (Buffer.byteLength(text, 'utf8') > 12 * 1024) {
    body.objective = compactText(body.objective, 1_000);
    text = JSON.stringify(body, null, 2);
  }
  if (Buffer.byteLength(text, 'utf8') > 12 * 1024) throw new Error('Required task constraints do not fit in the recovery document budget.');
  return text;
}

export class NativeSessionCompactor {
  private readonly store: TaskStore;
  private readonly detector: AgentDetector;
  private readonly codex = new CodexNativeCompaction();

  constructor(store: TaskStore, detector: AgentDetector) {
    this.store = store;
    this.detector = detector;
  }

  /** Acquire the session write lease and supply any still-undelivered recovery document. */
  public async prepareBeforeResume(threadId: string, role: TaskRole, model?: string, reviewerBootstrap = false): Promise<NativeSessionPreparation> {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    let thread = this.store.getTask(threadId);
    if (!thread || thread.archivedAt) throw new Error('Task disappeared before native-session preparation.');
    if (role === 'reviewer' && thread.reviewFlow !== 'dedicated') return {};
    const agent = role === 'developer' ? thread.targetAgent : thread.initiatorAgent;
    const captured = role === 'developer' ? thread.receiverSessionCaptured : thread.reviewerSessionCaptured;
    const sessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    if (!captured || !isValidSessionId(sessionId || '')) return {};
    const lease = new NativeSessionLock(root).acquire(agent, sessionId!);
    if (!lease) throw new Error(`Native session ${sessionId} is already owned by another Bridge process.`);
    const cancellation = new CancellationRegistry(root).peek(threadId);
    this.store.loadFromDisk();
    const fresh = this.store.getTask(threadId);
    const freshAgent = fresh && (role === 'developer' ? fresh.targetAgent : fresh.initiatorAgent);
    const freshSession = fresh && (role === 'developer' ? fresh.receiverSessionId : fresh.reviewerSessionId);
    const freshModel = fresh && (role === 'developer' ? fresh.targetModel : fresh.reviewerModel);
    const worktreeChanged = JSON.stringify(fresh?.worktree && {
      path: fresh.worktree.path, branch: fresh.worktree.taskBranch, base: fresh.worktree.baseCommit,
      head: fresh.worktree.headCommit, approved: fresh.worktree.approvedCommit,
    }) !== JSON.stringify(thread.worktree && {
      path: thread.worktree.path, branch: thread.worktree.taskBranch, base: thread.worktree.baseCommit,
      head: thread.worktree.headCommit, approved: thread.worktree.approvedCommit,
    });
    if (!fresh || fresh.archivedAt || freshAgent !== agent || freshSession !== sessionId ||
        freshModel !== model || worktreeChanged || cancellation ||
        (role === 'developer' && !['running', 'reworking', 'queued'].includes(fresh.status)) ||
        (role === 'reviewer' && fresh.status !== 'waiting_review' && !(reviewerBootstrap && fresh.status === 'queued' && fresh.reviewBootstrap?.phase === 'launching'))) {
      new NativeSessionLock(root).release(lease);
      throw new Error('Task state, cancellation, model, worktree, or native session changed before resume preparation.');
    }
    thread = fresh;
    let cursor = thread.nativeSessionCompaction?.[role];
    const currentCursor = cursor;
    if (currentCursor && currentCursor.sessionId === sessionId && currentCursor.lastOperationId) {
      const round = [...(thread.rounds || [])].reverse().find((entry) =>
        entry.nativeSessionCompactions?.some((operation) => operation.id === currentCursor.lastOperationId));
      const operation = round?.nativeSessionCompactions?.find((entry) => entry.id === currentCursor.lastOperationId);
      if (operation && (operation.phase === 'prepared' || operation.phase === 'running')) {
        operation.phase = 'unknown';
        operation.errorCode = 'HOST_RECOVERED_WITH_UNCONFIRMED_OPERATION';
        operation.error = 'The previous Bridge process ended before recording a confirmed native completion; the compression request will not be replayed.';
        operation.finishedAt = new Date().toISOString();
        operation.revision++;
        cursor = { ...currentCursor, revision: (currentCursor.revision || 0) + 1 };
        thread.nativeSessionCompaction = { ...thread.nativeSessionCompaction, [role]: cursor };
        this.store.upsertTask(thread);
        const expectedLogDir = role === 'reviewer' ? getReviewerLogDir(agent, root) : getWorkerLogDir(agent, root);
        if (path.dirname(path.resolve(operation.logPath)) === path.resolve(expectedLogDir)) {
          try { fs.appendFileSync(operation.logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: operation.id, phase: operation.phase, errorCode: operation.errorCode })}\n`, 'utf8'); } catch {}
        }
      }
    }
    let recoveryPrefix: string | undefined;
    const latestCursor = cursor;
    const latestOperation = latestCursor && latestCursor.sessionId === sessionId && latestCursor.lastOperationId
      ? (thread.rounds || []).flatMap((round) => round.nativeSessionCompactions || []).find((entry) => entry.id === latestCursor.lastOperationId)
      : undefined;
    if (latestOperation && ['unknown', 'interrupted'].includes(latestOperation.phase)) {
      const reason = `Native session ${sessionId} cannot be resumed safely: operation ${latestOperation.id} has unconfirmed state and no validated read-only recovery probe is available.`;
      appendInterruption(thread, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' });
      thread.status = 'paused';
      this.store.upsertTask(thread);
      new NativeSessionLock(root).release(lease);
      return { sessionId, pauseReason: reason };
    }
    if (cursor && cursor.sessionId === sessionId && cursor.recoveryVersion && cursor.recoveryVersion !== cursor.recoveryDeliveredVersion) {
      const operation = (thread.rounds || []).flatMap((round) => round.nativeSessionCompactions || [])
        .find((entry) => entry.id === cursor.recoveryVersion && entry.sessionId === sessionId);
      if (!operation) { new NativeSessionLock(root).release(lease); throw new Error('Pending native-session recovery record is missing.'); }
      const logDir = role === 'reviewer' ? getReviewerLogDir(agent, root) : getWorkerLogDir(agent, root);
      const recoveryPath = path.resolve(operation.recoveryPath);
      if (path.dirname(recoveryPath) !== path.resolve(logDir)) { new NativeSessionLock(root).release(lease); throw new Error('Recovery document path is outside its agent log directory.'); }
      try { recoveryPrefix = fs.readFileSync(recoveryPath, 'utf8'); }
      catch (error) { new NativeSessionLock(root).release(lease); throw error; }
    }
    const mode = readBridgePolicy(root).nativeSessionCompaction?.mode || 'off';
    const turns = cursor && cursor.sessionId === sessionId ? cursor.turnsSinceCompaction : 0;
    const warning = mode === 'auto' && turns >= 4
      ? `${agent} native compaction is ${nativeCompactionCapability(agent)}; automatic compaction was skipped.`
      : undefined;
    return { sessionId, lease, recoveryPrefix, recoveryVersion: recoveryPrefix ? cursor?.recoveryVersion : undefined, warning };
  }

  /** Count a completed business turn and acknowledge only the recovery version it actually carried. */
  public recordBusinessSuccess(threadId: string, role: TaskRole, sessionId: string, turnId: string, recoveryVersion?: string): void {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    const thread = this.store.getTask(threadId);
    if (!thread || thread.archivedAt) return;
    const boundSessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    const successfulStatus = role === 'developer'
      ? ['running', 'reworking', 'queued'].includes(thread.status)
      : ['reworking', 'waiting_merge', 'completed'].includes(thread.status);
    if (boundSessionId !== sessionId || !successfulStatus || new CancellationRegistry(root).peek(threadId)) return;
    const old = thread.nativeSessionCompaction?.[role];
    const cursor = old?.sessionId === sessionId ? old : { sessionId, turnsSinceCompaction: 0 };
    const seen = 'recentBusinessTurnIds' in cursor ? cursor.recentBusinessTurnIds || [] : [];
    if (seen.includes(turnId)) return;
    const next = [...seen, turnId].slice(-64);
    thread.nativeSessionCompaction = {
      ...thread.nativeSessionCompaction,
      [role]: {
        ...cursor,
        revision: (cursor.revision || 0) + 1,
        recentBusinessTurnIds: next,
        turnsSinceCompaction: cursor.turnsSinceCompaction + 1,
        ...(recoveryVersion && cursor.recoveryVersion === recoveryVersion ? { recoveryDeliveredVersion: recoveryVersion } : {}),
      },
    };
    this.store.upsertTask(thread);
  }

  /** Explicit, paused-task action. It never starts the next business round. */
  public async compactPausedTask(threadId: string, role: TaskRole): Promise<NativeSessionCompactionOperation> {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    let thread = this.store.getTask(threadId);
    if (!thread || thread.archivedAt || thread.status !== 'paused') throw new Error('Native compaction is available only for an unarchived paused task.');
    if (role === 'reviewer' && thread.reviewFlow !== 'dedicated') throw new Error('Legacy caller/requester sessions are not managed by Bridge compaction.');
    const agent = role === 'developer' ? thread.targetAgent : thread.initiatorAgent;
    const sessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    const captured = role === 'developer' ? thread.receiverSessionCaptured : thread.reviewerSessionCaptured;
    if (!captured || !isValidSessionId(sessionId || '')) throw new Error(`The ${role} native session is not captured.`);
    const nativeSessionId = sessionId!;
    if (agent !== 'codex') throw new Error(`${agent} is ${nativeCompactionCapability(agent)} for Bridge initiated compaction.`);
    const model = role === 'developer'
      ? thread.targetModel || resolveConfiguredModel(agent)
      : thread.reviewerModel || thread.initiatorModel || resolveConfiguredModel(agent, undefined, 'initiator');
    if (!model) throw new Error(`No resolved ${role} model is available; refusing to guess.`);
    let round = [...(thread.rounds || [])].reverse().find((entry) => entry.type !== 'approval');
    if (!round) throw new Error('The task has no business round to attach the maintenance record to.');

    const roleLock = new TaskRoleLock(root);
    const roleLease = roleLock.tryAcquire(threadId, role, role === 'reviewer' ? nativeSessionId : undefined);
    if (!roleLease) throw new Error('Another developer or reviewer process owns this task role.');
    const sessionLock = new NativeSessionLock(root);
    const sessionLease = sessionLock.acquire(agent, nativeSessionId);
    if (!sessionLease) { roleLock.release(roleLease); throw new Error('Another process owns this native session.'); }
    const executions = new TaskExecutionRegistry(root);
    let registration: TaskExecutionRegistration | undefined;
    let nativeChildPid: number | undefined;
    const cancellation = new CancellationRegistry(root);
    const baselineCancellation = cancellation.peek(threadId);
    const isNewCancellation = () => {
      const current = cancellation.peek(threadId);
      return !!current && JSON.stringify(current) !== JSON.stringify(baselineCancellation);
    };
    let resourceFinalized = false;
    let resourcesReleased = false;
    const releaseResources = () => {
      if (resourcesReleased || nativeChildPid && processAlive(nativeChildPid)) return;
      resourcesReleased = true;
      if (registration) executions.end(registration);
      sessionLock.release(sessionLease);
      roleLock.release(roleLease);
      executions.dispose();
    };
    try {
      const pending = cancellation.peek(threadId);
      if (pending?.action === 'delete') throw new Error('Task deletion is in progress.');
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      if (!fresh || fresh.status !== 'paused' || fresh.archivedAt ||
          (role === 'developer' ? fresh.receiverSessionId : fresh.reviewerSessionId) !== nativeSessionId) {
        throw new Error('Task state or native session changed before maintenance acquired ownership.');
      }
      thread = fresh;
      const freshRound = [...(thread.rounds || [])].reverse().find((entry) => entry.type !== 'approval');
      if (!freshRound) throw new Error('The task has no business round to attach the maintenance record to.');
      round = freshRound;
      registration = executions.begin(threadId, freshRound.round, agent, thread.worktree?.path || thread.workspacePath);
    } catch (error) {
      executions.dispose(); sessionLock.release(sessionLease); roleLock.release(roleLease); throw error;
    }
    const id = crypto.randomUUID();
    const logDir = role === 'reviewer' ? getReviewerLogDir(agent, root) : getWorkerLogDir(agent, root);
    const base = `${safeThreadIdPart(threadId)}_round_${round.round}.native_${id}`;
    const logPath = path.join(logDir, role === 'reviewer' ? `${base}.reviewer.log` : `${base}.log`);
    const recoveryPath = path.join(logDir, `${base}.recovery.json`);
    const operation: NativeSessionCompactionOperation = {
      id, role: role as 'developer' | 'reviewer', agent, sessionId: nativeSessionId, model, reason: 'manual',
      phase: 'prepared', revision: 1, startedAt: new Date().toISOString(), logPath, recoveryPath,
    };
    try {
      fs.writeFileSync(recoveryPath, recoveryDocument(thread, role, agent, model, nativeSessionId), { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      fs.writeFileSync(logPath, `${JSON.stringify({ at: new Date().toISOString(), operationId: id, phase: 'prepared', agent, model })}\n`, { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      round.nativeSessionCompactions = [...(round.nativeSessionCompactions || []), operation];
      const oldCursor = thread.nativeSessionCompaction?.[role];
      thread.nativeSessionCompaction = {
        ...thread.nativeSessionCompaction,
        [role]: { revision: (oldCursor?.revision || 0) + 1, sessionId: nativeSessionId, lastOperationId: id, turnsSinceCompaction: 0, recoveryVersion: id },
      };
      this.store.upsertTask(thread);

      const executable = this.detector.resolveExecutable('codex');
      if (!executable) throw new Error('Codex executable is unavailable.');
      operation.phase = 'running';
      operation.revision++;
      this.store.upsertTask(thread);
      const result = await this.codex.compact({
        executable, sessionId: nativeSessionId, cwd: thread.worktree?.path || thread.workspacePath, model, operationId: id, role: role as 'developer' | 'reviewer',
        cancelled: isNewCancellation,
        onChildPid: (pid) => {
          nativeChildPid = pid;
          executions.attachChildPid(pid, registration);
          if (pid) { sessionLock.attachChild(sessionLease, pid); roleLock.attachChild(roleLease, pid); }
          else if (resourceFinalized) releaseResources();
        },
      });
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      if (!fresh || fresh.archivedAt || fresh.status !== 'paused' || isNewCancellation() ||
          (role === 'developer' ? fresh.receiverSessionId : fresh.reviewerSessionId) !== nativeSessionId ||
          roleLock.getOwner(threadId)?.token !== roleLease.token) {
        operation.phase = 'unknown';
        operation.errorCode = 'TASK_CHANGED_DURING_MAINTENANCE';
        operation.revision++;
        return operation;
      }
      operation.phase = 'succeeded';
      operation.nativeTurnId = result.nativeTurnId;
      operation.finishedAt = new Date().toISOString();
      operation.revision++;
      const freshRound = [...fresh.rounds].reverse().find((entry) => entry.round === round.round);
      if (freshRound) {
        freshRound.nativeSessionCompactions = [...(freshRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
        if (result.usage) {
          result.usage.id = id;
          freshRound.tokenUsage = appendTokenUsageComponent(freshRound.tokenUsage, result.usage, `legacy-business-round-${round.round}`);
        }
      }
      this.store.upsertTask(fresh);
      fs.appendFileSync(logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, nativeTurnId: operation.nativeTurnId, usage: result.usage || { available: false } })}\n`, 'utf8');
      return operation;
    } catch (error: any) {
      operation.phase = isNewCancellation() ? 'interrupted' : 'unknown';
      operation.errorCode = operation.phase === 'interrupted' ? 'CANCELLED' : 'NATIVE_COMPACTION_UNCONFIRMED';
      operation.error = String(error?.message || error).slice(0, 500);
      operation.finishedAt = new Date().toISOString();
      operation.revision++;
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      const targetRound = fresh && !fresh.archivedAt
        ? [...(fresh.rounds || [])].reverse().find((entry) => entry.round === round.round)
        : undefined;
      if (fresh && targetRound) {
        targetRound.nativeSessionCompactions = [...(targetRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
        this.store.upsertTask(fresh);
      }
      try { fs.appendFileSync(logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, errorCode: operation.errorCode, error: operation.error })}\n`, 'utf8'); } catch {}
      return operation;
    } finally {
      resourceFinalized = true;
      releaseResources();
    }
  }
}

function processAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error: any) { return error?.code === 'EPERM'; }
}
