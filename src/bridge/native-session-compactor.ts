import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import { AgentDetector } from './agent-detector';
import { CancellationRegistry } from './cancellation-registry';
import { CodexNativeCompaction } from './codex-native-compaction';
import { NativeSessionLock } from './native-session-lock';
import { resolveConfiguredModel } from './model-catalog';
import { getReviewerLogDir, getWorkerLogDir, safeThreadIdPart } from './round-log';
import { TaskExecutionRegistry } from './task-execution-registry';
import { TaskRoleLock, TaskRole } from './task-role-lock';
import { TaskStore } from './task-store';
import { AgentId, DelegationThread, NativeSessionCompactionOperation, isValidSessionId } from './types';
import { appendTokenUsageComponent } from './token-usage-collector';

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
    currentInstruction: compactText(latest?.prompt || '', 2_500),
    latestReport: compactText(latest?.output || '', 3_500),
    recovery: 'For omitted history, read the round ledger and paged worker/reviewer logs; do not infer an approval from this recovery document.',
  };
  let text = JSON.stringify(body, null, 2);
  if (Buffer.byteLength(text, 'utf8') > 12 * 1024) {
    body.latestReport = compactText(body.latestReport, 1_000);
    body.objective = compactText(body.objective, 1_000);
    body.currentInstruction = compactText(body.currentInstruction, 1_000);
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
    let registration;
    try {
      const pending = new CancellationRegistry(root).peek(threadId);
      if (pending?.action === 'delete') throw new Error('Task deletion is in progress.');
      if (pending?.action === 'pause') new CancellationRegistry(root).clear(threadId);
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
      thread.nativeSessionCompaction = {
        ...thread.nativeSessionCompaction,
        [role]: { sessionId: nativeSessionId, lastOperationId: id, turnsSinceCompaction: 0, recoveryVersion: id },
      };
      this.store.upsertTask(thread);

      const executable = this.detector.resolveExecutable('codex');
      if (!executable) throw new Error('Codex executable is unavailable.');
      operation.phase = 'running';
      operation.revision++;
      this.store.upsertTask(thread);
      const cancellation = new CancellationRegistry(root);
      const result = await this.codex.compact({
        executable, sessionId: nativeSessionId, cwd: thread.worktree?.path || thread.workspacePath, model, operationId: id, role: role as 'developer' | 'reviewer',
        cancelled: () => !!cancellation.peek(threadId),
        onChildPid: (pid) => {
          executions.attachChildPid(pid, registration);
          if (pid) { sessionLock.attachChild(sessionLease, pid); roleLock.attachChild(roleLease, pid); }
        },
      });
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      if (!fresh || fresh.archivedAt || fresh.status !== 'paused' ||
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
      operation.phase = cancellationPeek(root, threadId) ? 'interrupted' : 'unknown';
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
      if (registration) executions.end(registration);
      sessionLock.release(sessionLease);
      roleLock.release(roleLease);
      executions.dispose();
    }
  }
}

function cancellationPeek(root: string, threadId: string): boolean {
  return !!new CancellationRegistry(root).peek(threadId);
}
