import { windowsJobFinished, windowsJobOf, type WindowsJobBinding } from './windows-job';
import { assertDedicatedTask } from './types';
import { getTaskExecutionPath, isReviewerWakeEligible, isMergeReview } from './types';
import * as crypto from 'crypto';
import { spawnSync } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AgentDetector } from './agent-detector';
import { CancellationRegistry } from './cancellation-registry';
import { CodexNativeCompaction, MAX_NATIVE_COMPACTION_ACTIVE_MS, MAX_NATIVE_RECOVERY_PROBE_ACTIVE_MS } from './codex-native-compaction';
import { NativeSessionLock, NativeSessionLease } from './native-session-lock';
import { resolveConfiguredModel } from './model-catalog';
import { taskArtifactBase, safeThreadIdPart, taskReviewerLogDir, taskWorkerLogDir } from './round-log';
import { TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { TaskRoleLock, TaskRole } from './task-role-lock';
import { TaskStore } from './task-store';
import { AgentId, DelegationThread, ExecutionSubtreeEvidence, NativeSessionCompactionOperation, ProcessIdentity, TokenUsageComponent, appendInterruption, isValidSessionId, findInFlightRound } from './types';
import { ExecutionRun } from './types';
import { appendTokenUsageComponent } from './token-usage-collector';
import { readBridgePolicy } from './bridge-policy';
import { SessionOwnershipStore, nativeSessionNamespace } from './session-ownership';
import { getProcessBirthId } from './platform';
import { captureChildDescendants } from './process-binding';
import { probeProcessIdentity } from './platform';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';

export interface NativeSessionPreparation {
  sessionId?: string;
  lease?: ReturnType<NativeSessionLock['acquire']>;
  executionLease?: { registry: TaskExecutionRegistry; registration: TaskExecutionRegistration; owned: boolean };
  recoveryPrefix?: string;
  recoveryVersion?: string;
  warning?: string;
  pauseReason?: string;
}

export interface NativeRunPreparationContext {
  runId: string;
  operationId: string;
  generation: number;
  phase: 'develop' | 'review' | 'merge';
  round: number;
  role: 'developer' | 'reviewer';
  onChildPid?: (pid: number | undefined, childProcess?: import('child_process').ChildProcess, childBirthId?: string) => void | Promise<void>;
}

export type NativeCompactionCapability = 'unsupported' | 'needs_validation' | 'certified';

export interface NativeCompactionCapabilityIdentity {
  agent: AgentId;
  executablePath: string;
  executableVersion: string;
  providerRoute: string;
  providerId: string;
  model: string;
}

export function resolveCodexProviderIdentity(
  config: string | undefined,
  env: Record<string, string | undefined>,
): Pick<NativeCompactionCapabilityIdentity, 'providerRoute' | 'providerId'> | undefined {
  const baseUrl = env.OPENAI_BASE_URL || env.CODEX_BASE_URL;
  if (baseUrl) return undefined;
  if (config === undefined) return undefined;
  const lines = config.split(/\r?\n/).map((line) => line.trim());
  let section = '';
  let providerId: string | undefined;
  let providerBaseUrl: string | undefined;
  let hasCustomProviders = false;
  for (const line of lines) {
    if (!line || line.startsWith('#')) continue;
    if (line.startsWith('[')) {
      if (/^\[profiles?(?:\.|\])/.test(line)) return undefined;
      if (line.startsWith('[model_providers.')) {
        const table = line.match(/^\[model_providers\.([A-Za-z0-9_-]+)\]\s*(?:#.*)?$/);
        if (!table) return undefined;
        section = table[1]; hasCustomProviders = true; continue;
      }
      if (line.startsWith('[model_providers]')) return undefined;
      if (/model_providers/i.test(line)) return undefined;
      section = '!other';
      continue;
    }
    const key = line.match(/^(model_provider|base_url)\s*=/)?.[1];
    if (!key) {
      // Only a deliberately small TOML subset is understood here. If a route
      // key is present in another legal spelling, do not certify a default.
      if (/(?:model_provider|base_url)/i.test(line)) return undefined;
      continue;
    }
    const value = line.match(/^(model_provider|base_url)\s*=\s*"([^"\\]*)"\s*(?:#.*)?$/);
    if (!value) return undefined;
    if (key === 'model_provider') {
      if (section !== '') return undefined;
      providerId = value[2];
    } else if (section === '!other') {
      return undefined;
    } else if (section !== '!other' && section !== '') {
      if (providerId && section === providerId) providerBaseUrl = value[2];
    } else if (section === '') {
      return undefined;
    }
  }
  if (!providerId) {
    return hasCustomProviders ? undefined : { providerId: 'openai', providerRoute: 'subscription-default' };
  }
  if (providerId !== 'openai' && !providerBaseUrl) return undefined;
  return { providerId, providerRoute: providerBaseUrl ? `${providerId}|${providerBaseUrl}` : providerId };
}

export type NativeCompactionCapabilityResolver = (
  identity: NativeCompactionCapabilityIdentity,
) => NativeCompactionCapability;

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
      role === 'reviewer'
        ? 'Perform the assigned review and record its verdict through the Bridge workflow; do not forward or alter the approved delivery SHA.'
        : 'Continue the assigned implementation or rework; do not forward or perform reviewer-only decisions.',
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

/**
 * Bridge log directory for a role, preferring the task's recorded unified dir.
 *
 * Only the task directory is used; agent-wide directories are never scanned.
 */
function roleLogDir(
  thread: DelegationThread,
  role: TaskRole,
  agent: AgentId,
  root?: string,
): string {
  const dir = taskArtifactBase(thread.threadId, root, thread.artifactDir);
  const logDir = role === 'reviewer' ? taskReviewerLogDir(dir) : taskWorkerLogDir(dir);
  fs.mkdirSync(logDir, { recursive: true });
  return role === 'reviewer' ? taskReviewerLogDir(dir) : taskWorkerLogDir(dir);
}

function emptyMaintenanceUsage(id: string, role: TaskRole, agent: AgentId, model: string): TokenUsageComponent {
  return { id, kind: 'native_compaction', role: role as 'developer' | 'reviewer', agent, model,
    uncachedInputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 0,
    source: 'unavailable', available: false, partial: true };
}

export class NativeSessionCompactor {
  private readonly store: TaskStore;
  private readonly detector: AgentDetector;
  private readonly codex = new CodexNativeCompaction();
  private readonly capabilityResolver: NativeCompactionCapabilityResolver;
  private readonly capabilityCache = new Map<string, NativeCompactionCapability>();

  constructor(store: TaskStore, detector: AgentDetector, capabilityResolver?: NativeCompactionCapabilityResolver) {
    this.store = store;
    this.detector = detector;
    this.capabilityResolver = capabilityResolver || ((identity) => this.readCapabilityRecord(identity));
  }

  private readCapabilityRecord(identity: NativeCompactionCapabilityIdentity): NativeCompactionCapability {
    const root = path.dirname(this.store.getStoragePath());
    const key = JSON.stringify(identity);
    const cached = this.capabilityCache.get(key);
    if (cached) return cached;
    let status: NativeCompactionCapability = 'needs_validation';
    try {
      const entries = JSON.parse(fs.readFileSync(path.join(root, 'native-session-capabilities.json'), 'utf8')) as Array<NativeCompactionCapabilityIdentity & { status?: NativeCompactionCapability }>;
      status = entries.find((entry) => entry.agent === identity.agent && entry.executablePath === identity.executablePath &&
        entry.executableVersion === identity.executableVersion && entry.providerRoute === identity.providerRoute &&
        entry.providerId === identity.providerId && entry.model === identity.model)?.status || 'needs_validation';
    } catch {}
    this.capabilityCache.set(key, status);
    return status;
  }

  private recordMaintenanceUsage(
    threadId: string, roundNumber: number, operationId: string, role: TaskRole, agent: AgentId, model: string,
    measured?: TokenUsageComponent,
  ): void {
    this.store.loadFromDisk();
    const thread = this.store.getTask(threadId);
    const round = thread && !thread.archivedAt && thread.rounds.find((entry) => entry.round === roundNumber);
    if (!round) return;
    const usage = measured || emptyMaintenanceUsage(operationId, role, agent, model);
    usage.id = operationId;
    round.tokenUsage = appendTokenUsageComponent(round.tokenUsage, usage, `legacy-business-round-${roundNumber}`);
    this.store.upsertTask(thread);
  }

  private capabilityIdentity(agent: AgentId, model: string): NativeCompactionCapabilityIdentity | undefined {
    if (agent !== 'codex') return undefined;
    const executablePath = this.detector.resolveExecutable('codex');
    if (!executablePath) return undefined;
    const versionResult = spawnSync(executablePath, ['--version'], { encoding: 'utf8', timeout: 3_000, windowsHide: true });
    const executableVersion = `${versionResult.stdout || ''}${versionResult.stderr || ''}`.trim().split(/\r?\n/, 1)[0];
    if (versionResult.status !== 0 || !executableVersion) return undefined;
    let codexHome = process.env.CODEX_HOME || path.join(os.homedir(), '.codex');
    if (!path.isAbsolute(codexHome)) return undefined;
    let config: string | undefined;
    try { config = fs.readFileSync(path.join(codexHome, 'config.toml'), 'utf8'); } catch {}
    const provider = resolveCodexProviderIdentity(config, process.env);
    if (!provider) return undefined;
    let actualPath = path.resolve(executablePath);
    try { actualPath = fs.realpathSync.native(executablePath); } catch {}
    return { agent, executablePath: actualPath, executableVersion, ...provider, model };
  }

  /** Acquire the session write lease and supply any still-undelivered recovery document. */
  public async prepareBeforeResume(threadId: string, role: TaskRole, model?: string, reviewerBootstrap = false, existingLease?: NativeSessionLease, runContext?: NativeRunPreparationContext): Promise<NativeSessionPreparation> {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    let thread = this.store.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread || thread.archivedAt) throw new Error('Task disappeared before native-session preparation.');

    const agent = role === 'developer' ? thread.targetAgent : thread.initiatorAgent;
    const captured = role === 'developer' ? thread.receiverSessionCaptured : thread.reviewerSessionCaptured;
    const sessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    if (!captured || !isValidSessionId(sessionId || '')) return {};
    const isV2Run = Boolean(runContext);
    if (isV2Run) {
      const assignment = thread.nativeSessionAssignments?.[role];
      if (!assignment || assignment.namespace !== nativeSessionNamespace(agent) || !Number.isSafeInteger(assignment.sessionEpoch)) {
        throw new Error('SESSION_OWNERSHIP_UNVERIFIED: Native preparation has no pinned namespace and session epoch.');
      }
      const ownership = new SessionOwnershipStore(root).verify({
        agent, namespace: assignment.namespace, sessionId: sessionId!, threadId, role,
        sessionEpoch: assignment.sessionEpoch,
      });
      if (!ownership.ok) throw new Error(`${ownership.code}: ${ownership.reason}`);
      const run = thread.execution?.run;
      if (thread.execution?.version !== 2 || !run || run.id !== runContext!.runId ||
          thread.execution.operationId !== runContext!.operationId || thread.execution.generation !== runContext!.generation ||
          thread.execution.phase !== runContext!.phase || thread.execution.round !== runContext!.round ||
          run.role !== runContext!.role || run.state === 'stopping' || run.unconfirmed || thread.cleanup || thread.execution.next) {
        throw new Error('V2 native preparation is not covered by the current protected run.');
      }
    }
    // V2 has no Bridge session lease: the pinned static ownership record and
    // the unique durable run/subtree are the only admission and lifetime proof.
    const lease = isV2Run ? undefined : existingLease || new NativeSessionLock(root).acquire(agent, sessionId!);
    if (!isV2Run && !lease) throw new Error(`Native session ${sessionId} is already owned by another Bridge process.`);
    let executionLease: NativeSessionPreparation['executionLease'];
    let keepLease = Boolean(existingLease);
    try {
    const roleLock = isV2Run ? undefined : new TaskRoleLock(root);
    const roleOwnerAtStart = roleLock?.getOwner(threadId);
    const roleLease = roleOwnerAtStart ? {
      token: roleOwnerAtStart.token,
      threadId,
      role,
      ...(roleOwnerAtStart.conversationId ? { conversationId: roleOwnerAtStart.conversationId } : {}),
    } : undefined;
    if (!isV2Run && (!roleOwnerAtStart || roleOwnerAtStart.role !== role)) {
      if (!existingLease && lease) new NativeSessionLock(root).release(lease);
      throw new Error('The task role lease is not held during native-session preparation.');
    }
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
        (role === 'developer' && !['running', 'queued'].includes(fresh.status)) ||
        (role === 'reviewer' && !isReviewerWakeEligible(fresh) && !(reviewerBootstrap && fresh.status === 'queued' &&
          ['pending', 'launching'].includes(fresh.reviewBootstrap?.phase || '')))) {
      if (!existingLease && lease) new NativeSessionLock(root).release(lease);
      throw new Error('Task state, cancellation, model, worktree, or native session changed before resume preparation.');
    }
    thread = fresh;
    const ensureExecutionLease = (): NativeSessionPreparation['executionLease'] => {
      if (executionLease) return executionLease;
      // The v2 controller already owns process registration through the run's
      // child/subtree handshake; do not create a second process lease.
      if (isV2Run) return undefined;
      const registry = new TaskExecutionRegistry(root);
      const owner = registry.getOwner(threadId);
      if (owner) {
        executionLease = { registry, registration: { token: owner.token, threadId, round: owner.round, recordPath: owner.recordPath }, owned: false };
      } else {
        if (isV2Run) throw new Error('V2 native preparation has no matching execution registry entry.');
        const currentRound = [...(thread!.rounds || [])].reverse().find((entry) => entry.type !== 'approval');
        executionLease = {
          registry,
          registration: registry.begin(
            threadId,
            currentRound?.round || thread!.currentRound || 1,
            role === 'reviewer' && isMergeReview(thread!) ? 'merge-reviewer' : agent,
            getTaskExecutionPath(thread!),
            'maintenance',
          ),
          owned: true,
        };
      }
      return executionLease;
    };
    const releaseExecutionLease = () => {
      if (!executionLease) return;
      if (executionLease.owned) executionLease.registry.end(executionLease.registration);
      executionLease.registry.dispose();
      executionLease = undefined;
    };
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
        const expectedLogDir = roleLogDir(thread, role, agent, root);
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
    let warning: string | undefined;
    let recoveredUnknownOperation = false;
    if (latestOperation && ['unknown', 'interrupted'].includes(latestOperation.phase)) {
      const alreadyVerified = cursor?.recoveryVerifiedOperationId === latestOperation.id;
      const alreadyDelivered = cursor?.recoveryVersion === latestOperation.id && cursor.recoveryDeliveredVersion === latestOperation.id;
      if (alreadyVerified) {
        if (!alreadyDelivered) {
          recoveredUnknownOperation = true;
          warning = 'The unknown native operation was already verified read-only; its pending recovery document will be carried without another probe or compaction.';
        }
      } else {
        const activeExecution = ensureExecutionLease();
        let recovered = false;
        const nativeRun = snapshotNativeRun(thread);
        try {
          const executable = agent === 'codex' ? this.detector.resolveExecutable('codex') : undefined;
          if (nativeRun) await setNativeRunMaintenanceStep(this.store, threadId, nativeRun, true);
          recovered = !!executable && !!latestOperation.model && await this.codex.probeResume({
            executable, sessionId: sessionId!, cwd: getTaskExecutionPath(thread),
            model: latestOperation.model || model || '', operationId: latestOperation.id,
            role, cancelled: () => !!new CancellationRegistry(root).peek(threadId),
            jobDirectory: (isV2Run || nativeRun) && thread.artifactDir ? path.join(thread.artifactDir, 'worker_log') : undefined,
            onChildPid: async (pid, birthId, child) => {
              if (isV2Run) await runContext!.onChildPid?.(pid, child, birthId);
              else if (lease) new NativeSessionLock(root).attachChild(lease, pid, pid ? () => !!new CancellationRegistry(root).peek(threadId) : undefined);
              if (activeExecution) activeExecution.registry.attachChildPid(pid, activeExecution.registration);
              if (!isV2Run && roleLease) roleLock!.attachChild(roleLease, pid);
              if (nativeRun && !isV2Run) await bindNativeRunChild(this.store, threadId, nativeRun, pid, birthId, windowsJobOf(child));
            },
          });
        } catch (error: any) {
          warning = `Same-ID read-only recovery probe failed: ${String(error?.message || error).slice(0, 300)}`;
        } finally {
          if (nativeRun) {
            this.store.loadFromDisk();
            const latest = this.store.getTask(threadId);
            if (latest?.execution?.run?.id === nativeRun.id && !latest.execution.run.child && !latest.execution.run.unconfirmed) {
              try { await setNativeRunMaintenanceStep(this.store, threadId, nativeRun, false); } catch {}
            }
          }
        }
        if (!recovered) {
          releaseExecutionLease();
          const reason = `Native session ${sessionId} could not be proven healthy after operation ${latestOperation.id}; compression was not replayed.`;
          appendInterruption(thread, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' });
          thread.status = 'paused';
          this.store.upsertTask(thread);
          if (lease) new NativeSessionLock(root).release(lease);
          return { sessionId, pauseReason: reason };
        }
        if (cursor) {
          cursor = {
            ...cursor,
            recoveryVersion: cursor.recoveryVersion || latestOperation.id,
            recoveryVerifiedOperationId: latestOperation.id,
            ...(alreadyDelivered ? {} : {
              turnsSinceCompaction: 0,
              autoSuppressedOperationId: latestOperation.id,
              turnCounterResetId: latestOperation.id,
            }),
            revision: (cursor.revision || 0) + 1,
          };
          if (latestOperation.errorCode !== 'UNKNOWN_OPERATION_RECOVERED_READONLY') {
            latestOperation.errorCode = 'UNKNOWN_OPERATION_RECOVERED_READONLY';
            latestOperation.error = 'Same-ID read-only recovery succeeded. Automatic compaction is suppressed until the carried recovery document is delivered by a successful business turn.';
            latestOperation.revision++;
          }
          thread.nativeSessionCompaction = { ...thread.nativeSessionCompaction, [role]: cursor };
          this.store.upsertTask(thread);
        }
        if (!alreadyDelivered) {
          recoveredUnknownOperation = true;
          warning = 'Unknown native operation was recovered read-only; the pending recovery document will be carried and compaction is suppressed.';
        }
      }
    }
    const mode = readBridgePolicy(root).nativeSessionCompaction?.mode || 'off';
    const turns = cursor && cursor.sessionId === sessionId ? cursor.turnsSinceCompaction : 0;
    if (mode === 'auto' && turns >= 4 && !warning && !recoveredUnknownOperation &&
        (!cursor?.autoSuppressedOperationId || cursor.autoSuppressedOperationId !== cursor.lastOperationId)) {
      if (cursor?.autoDisabledReason) {
        warning = `Automatic compaction is disabled for this session: ${cursor.autoDisabledReason}.`;
      } else {
        const identity = model ? this.capabilityIdentity(agent, model) : undefined;
        const capability = identity ? this.capabilityResolver(identity) : 'needs_validation';
        if (capability === 'certified' && identity) {
          ensureExecutionLease();
          const result = await this.compactAutomatically(thread, role, agent, model!, sessionId!, lease, cursor || { sessionId: sessionId!, turnsSinceCompaction: 0 }, identity, runContext);
          if (result.pauseReason) {
            releaseExecutionLease();
            if (!existingLease && lease) new NativeSessionLock(root).release(lease);
            return { sessionId, pauseReason: result.pauseReason };
          }
          warning = result.warning;
          this.store.loadFromDisk();
          thread = this.store.getTask(threadId) || thread;
          cursor = thread.nativeSessionCompaction?.[role];
        } else {
          warning = `${agent} native compaction is ${capability}; automatic compaction was skipped (executable/model/provider identity is not certified).`;
        }
      }
    }
    if (cursor && cursor.sessionId === sessionId && cursor.recoveryVersion && cursor.recoveryVersion !== cursor.recoveryDeliveredVersion) {
      const operation = (thread.rounds || []).flatMap((round) => round.nativeSessionCompactions || [])
        .find((entry) => entry.id === cursor.recoveryVersion && entry.sessionId === sessionId);
      if (!operation) {
        releaseExecutionLease();
        const reason = 'Pending native-session recovery record is missing; refusing to resume without task context.';
        appendInterruption(thread, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' });
        thread.status = 'paused'; this.store.upsertTask(thread); if (!existingLease && lease) new NativeSessionLock(root).release(lease);
        return { sessionId, pauseReason: reason };
      }
      const logDir = roleLogDir(thread, role, agent, root);
      const recoveryPath = path.resolve(operation.recoveryPath);
      if (path.dirname(recoveryPath) !== path.resolve(logDir)) {
        releaseExecutionLease();
        const reason = 'Pending native-session recovery path is outside the task agent log directory.';
        appendInterruption(thread, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' });
        thread.status = 'paused'; this.store.upsertTask(thread); if (!existingLease && lease) new NativeSessionLock(root).release(lease);
        return { sessionId, pauseReason: reason };
      }
      try { recoveryPrefix = fs.readFileSync(recoveryPath, 'utf8'); }
      catch (error: any) {
        releaseExecutionLease();
        const reason = `Pending native-session recovery document is unavailable: ${String(error?.message || error).slice(0, 300)}`;
        appendInterruption(thread, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' });
        thread.status = 'paused'; this.store.upsertTask(thread); if (!existingLease && lease) new NativeSessionLock(root).release(lease);
        return { sessionId, pauseReason: reason };
      }
    }
    this.store.loadFromDisk();
    const latestThread = this.store.getTask(threadId);
    const latestAgent = latestThread && (role === 'developer' ? latestThread.targetAgent : latestThread.initiatorAgent);
    const latestSession = latestThread && (role === 'developer' ? latestThread.receiverSessionId : latestThread.reviewerSessionId);
    const latestModel = latestThread && (role === 'developer' ? latestThread.targetModel : latestThread.reviewerModel);
    const latestWorktree = JSON.stringify(latestThread?.worktree && {
      path: latestThread.worktree.path, branch: latestThread.worktree.taskBranch, base: latestThread.worktree.baseCommit,
      head: latestThread.worktree.headCommit, approved: latestThread.worktree.approvedCommit,
    });
    const originalWorktree = JSON.stringify(thread.worktree && {
      path: thread.worktree.path, branch: thread.worktree.taskBranch, base: thread.worktree.baseCommit,
      head: thread.worktree.headCommit, approved: thread.worktree.approvedCommit,
    });
    const statusAllowed = latestThread && (role === 'developer'
      ? ['running', 'queued'].includes(latestThread.status)
      : isReviewerWakeEligible(latestThread) || reviewerBootstrap && latestThread.status === 'queued' &&
        ['pending', 'launching'].includes(latestThread.reviewBootstrap?.phase || ''));
    if (!latestThread || latestThread.archivedAt || !statusAllowed || latestAgent !== agent || latestSession !== sessionId ||
        latestModel !== model || latestWorktree !== originalWorktree || new CancellationRegistry(root).peek(threadId) ||
        (!isV2Run && new TaskRoleLock(root).getOwner(threadId)?.token !== roleOwnerAtStart?.token) ||
        (isV2Run && (latestThread?.execution?.run?.id !== runContext!.runId || latestThread.execution.operationId !== runContext!.operationId ||
          latestThread.execution.generation !== runContext!.generation || latestThread.cleanup || latestThread.execution.next))) {
      if (!existingLease && lease) new NativeSessionLock(root).release(lease);
      releaseExecutionLease();
      return { sessionId, pauseReason: 'Task, role, model, session, worktree, or cancellation state changed during preparation.' };
    }
    keepLease = true;
    return { sessionId, lease, executionLease, recoveryPrefix, recoveryVersion: recoveryPrefix ? cursor?.recoveryVersion : undefined, warning };
    } catch (error) {
      if (executionLease?.owned) executionLease.registry.end(executionLease.registration);
      executionLease?.registry.dispose();
      if (!keepLease && !existingLease && lease) new NativeSessionLock(root).release(lease);
      throw error;
    }
  }

  private async compactAutomatically(
    thread: DelegationThread,
    role: TaskRole,
    agent: AgentId,
    model: string,
    sessionId: string,
    lease: ReturnType<NativeSessionLock['acquire']> | undefined,
    cursor: NonNullable<DelegationThread['nativeSessionCompaction']>[TaskRole],
    identity: NativeCompactionCapabilityIdentity,
    runContext?: NativeRunPreparationContext,
  ): Promise<{ warning?: string; pauseReason?: string }> {
    const root = path.dirname(this.store.getStoragePath());
    const v2Run = snapshotNativeRun(thread);
    const initialBudget = v2Run ? remainingMaintenanceBudget(thread) : undefined;
    if (v2Run && (!initialBudget || initialBudget.remainingMs <= 0)) {
      if (v2Run) {
        try { await setNativeRunMaintenanceStep(this.store, thread.threadId, v2Run, false); } catch {}
      }
      return { warning: 'Automatic compaction skipped because no active execution budget remains.' };
    }
    const maintenanceTimeoutMs = Math.min(MAX_NATIVE_COMPACTION_ACTIVE_MS, initialBudget?.remainingMs ?? MAX_NATIVE_COMPACTION_ACTIVE_MS);
    const roleLock = v2Run ? undefined : new TaskRoleLock(root);
    const roleOwner = roleLock?.getOwner(thread.threadId);
    if (!v2Run && (!roleOwner || roleOwner.role !== role)) return { warning: 'Automatic compaction skipped because the task role lease is not held.' };
    const cancellation = new CancellationRegistry(root);
    if (cancellation.peek(thread.threadId)) return { warning: 'Automatic compaction skipped because cancellation is pending.' };
    if (v2Run) {
      await setNativeRunMaintenanceStep(this.store, thread.threadId, v2Run, true);
      this.store.loadFromDisk();
      thread = this.store.getTask(thread.threadId) || thread;
    }
    const latest = [...(thread.rounds || [])].reverse().find((round) => round.type !== 'approval');
    if (!latest) return { warning: 'Automatic compaction skipped because no business round is available.' };
    const id = crypto.randomUUID();
    const logDir = roleLogDir(thread, role, agent, root);
    const base = `${safeThreadIdPart(thread.threadId)}_round_${latest.round}.native_${id}`;
    const operation: NativeSessionCompactionOperation = {
      id, role: role as 'developer' | 'reviewer', agent, sessionId, model, reason: 'turn_budget',
      phase: 'prepared', revision: 1, startedAt: new Date().toISOString(),
      logPath: path.join(logDir, role === 'reviewer' ? `${base}.reviewer.log` : `${base}.log`),
      recoveryPath: path.join(logDir, `${base}.recovery.json`),
    };
    const freshCursor = { ...cursor, sessionId, revision: (cursor?.revision || 0) + 1, lastOperationId: id, recoveryVersion: id };
    try {
      fs.writeFileSync(operation.recoveryPath, recoveryDocument(thread, role, agent, model, sessionId), { flag: 'wx', mode: 0o600 });
      fs.writeFileSync(operation.logPath, `${JSON.stringify({ at: operation.startedAt, operationId: id, phase: 'prepared', reason: operation.reason })}\n`, { flag: 'wx', mode: 0o600 });
      latest.nativeSessionCompactions = [...(latest.nativeSessionCompactions || []), operation];
      thread.nativeSessionCompaction = { ...thread.nativeSessionCompaction, [role]: freshCursor };
      this.store.upsertTask(thread);
    } catch (error: any) {
      return { warning: `Automatic compaction could not persist its recovery record: ${String(error?.message || error)}` };
    }
    const executable = this.detector.resolveExecutable('codex');
    if (!executable) return { warning: 'Automatic compaction skipped because Codex executable disappeared.' };
    const registry = runContext ? undefined : new TaskExecutionRegistry(root);
    const executionOwner = registry?.getOwner(thread.threadId);
    const registration = executionOwner ? { token: executionOwner.token, threadId: executionOwner.threadId, round: executionOwner.round, recordPath: executionOwner.recordPath } : undefined;
    const bindChild = async (pid: number | undefined, birthId?: string, child?: import('child_process').ChildProcess) => {
      if (runContext) await runContext.onChildPid?.(pid, child, birthId);
      else if (lease) new NativeSessionLock(root).attachChild(lease, pid, pid ? () => !!cancellation.peek(thread.threadId) : undefined);
      if (!runContext && registration && registry) registry.attachChildPid(pid, registration);
      if (!runContext && roleOwner) roleLock!.attachChild({ token: roleOwner.token, threadId: thread.threadId, role: role as 'developer' | 'reviewer' }, pid);
      if (v2Run && !runContext) await bindNativeRunChild(this.store, thread.threadId, v2Run, pid, birthId, windowsJobOf(child));
    };
    const originalStatus = thread.status;
    operation.phase = 'running'; operation.revision++;
    this.store.upsertTask(thread);
    try {
      const result = await this.codex.compact({
        executable, sessionId, cwd: getTaskExecutionPath(thread), model, operationId: id, role: role as 'developer' | 'reviewer',
        timeoutMs: maintenanceTimeoutMs,
        expectedProviderId: identity.providerId,
        cancelled: () => !!cancellation.peek(thread.threadId),
        onUsage: (usage) => this.recordMaintenanceUsage(thread.threadId, latest.round, id, role, agent, model, usage),
        jobDirectory: (runContext || v2Run) && thread.artifactDir ? path.join(thread.artifactDir, 'worker_log') : undefined,
        onChildPid: bindChild,
      });
      this.store.loadFromDisk();
      const current = this.store.getTask(thread.threadId);
      const currentOwner = roleLock?.getOwner(thread.threadId);
      const currentAgent = current && (role === 'developer' ? current.targetAgent : current.initiatorAgent);
      const currentSession = current && (role === 'developer' ? current.receiverSessionId : current.reviewerSessionId);
      const currentModel = current && (role === 'developer' ? current.targetModel : current.reviewerModel);
      if (!current || current.archivedAt || current.status !== originalStatus || cancellation.peek(thread.threadId) ||
          currentAgent !== agent || currentSession !== sessionId || currentModel !== model ||
          (!runContext && currentOwner?.token !== roleOwner?.token) ||
          (runContext && (current.execution?.run?.id !== runContext.runId || current.execution.operationId !== runContext.operationId ||
            current.execution.generation !== runContext.generation || current.cleanup || current.execution.next))) {
        throw new Error('Task state changed while automatic maintenance was active.');
      }
      operation.phase = 'succeeded'; operation.nativeTurnId = result.nativeTurnId; operation.finishedAt = new Date().toISOString(); operation.revision++;
      const currentRound = current.rounds.find((round) => round.round === latest.round);
      if (currentRound) {
        currentRound.nativeSessionCompactions = [...(currentRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
        const usage = result.usage || {
          id, kind: 'native_compaction' as const, role: role as 'developer' | 'reviewer', agent: 'codex' as const, model,
          uncachedInputTokens: 0, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 0,
          source: 'unavailable' as const, available: false, partial: true,
        };
        usage.id = id;
        if (!result.usage) { usage.available = false; usage.partial = true; }
        currentRound.tokenUsage = appendTokenUsageComponent(currentRound.tokenUsage, usage, `legacy-business-round-${latest.round}`);
      }
      current.nativeSessionCompaction = {
        ...current.nativeSessionCompaction,
        [role]: { ...freshCursor, turnsSinceCompaction: 0, turnCounterResetId: id, revision: freshCursor.revision + 1 },
      };
      this.store.upsertTask(current);
      fs.appendFileSync(operation.logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, nativeTurnId: operation.nativeTurnId, usage: result.usage || { available: false, partial: true } })}\n`);
      return {};
    } catch (error: any) {
      this.recordMaintenanceUsage(thread.threadId, latest.round, id, role, agent, model);
      operation.phase = cancellation.peek(thread.threadId) ? 'interrupted' : 'unknown';
      operation.errorCode = operation.phase === 'interrupted' ? 'CANCELLED' : 'NATIVE_COMPACTION_UNCONFIRMED';
      operation.error = String(error?.message || error).slice(0, 500); operation.finishedAt = new Date().toISOString(); operation.revision++;
      this.store.loadFromDisk();
      const current = this.store.getTask(thread.threadId);
      const currentRound = current && !current.archivedAt && current.rounds.find((round) => round.round === latest.round);
      if (currentRound) {
        currentRound.nativeSessionCompactions = [...(currentRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
        current.nativeSessionCompaction = { ...current.nativeSessionCompaction, [role]: { ...freshCursor, revision: freshCursor.revision + 1 } };
        this.store.upsertTask(current);
      }
      try { fs.appendFileSync(operation.logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, errorCode: operation.errorCode, error: operation.error })}\n`); } catch {}
      if (operation.phase === 'interrupted') return { pauseReason: 'Automatic compaction was cancelled; the task must remain paused.' };
      try {
        this.store.loadFromDisk();
        const probeThread = this.store.getTask(thread.threadId) || thread;
        const probeBudget = v2Run ? remainingMaintenanceBudget(probeThread) : undefined;
        if (v2Run && (!probeBudget || probeBudget.remainingMs <= 0)) throw new Error('NATIVE_MAINTENANCE_BUDGET_EXHAUSTED: No active budget remains for recovery probe.');
        const probeTimeoutMs = Math.min(MAX_NATIVE_RECOVERY_PROBE_ACTIVE_MS, probeBudget?.remainingMs ?? MAX_NATIVE_RECOVERY_PROBE_ACTIVE_MS);
        const healthy = await this.codex.probeResume({ executable, sessionId, cwd: getTaskExecutionPath(thread), model, operationId: id, role: role as 'developer' | 'reviewer', timeoutMs: probeTimeoutMs, cancelled: () => !!cancellation.peek(thread.threadId), jobDirectory: (runContext || v2Run) && thread.artifactDir ? path.join(thread.artifactDir, 'worker_log') : undefined, onChildPid: bindChild });
        if (healthy) return { warning: 'Automatic compaction outcome is unknown; same-ID read-only recovery succeeded and the recovery document remains pending.' };
      } catch {}
      const reason = `Native session ${sessionId} could not be proven healthy after automatic operation ${id}; compression was not replayed.`;
      if (current) { appendInterruption(current, { kind: 'paused', reason, at: new Date().toISOString(), by: 'native_session_compaction' }); current.status = 'paused'; this.store.upsertTask(current); }
      return { pauseReason: reason };
    } finally {
      this.store.loadFromDisk();
      const latest = this.store.getTask(thread.threadId);
      if (v2Run && latest?.execution?.run?.id === v2Run.id && !latest.execution.run.child && !latest.execution.run.unconfirmed) {
        try { await setNativeRunMaintenanceStep(this.store, thread.threadId, v2Run, false); } catch {}
      }
      registry?.dispose();
    }
  }

  /** Count a completed business turn and acknowledge only the recovery version it actually carried. */
  public recordNativeAutoCompactionEvents(threadId: string, role: TaskRole, sessionId: string, stdout: string): void {
    const events: string[] = [];
    for (const line of String(stdout || '').split(/\r?\n/)) {
      try {
        const event = JSON.parse(line);
        const params = event.params || event;
        const item = params.item;
        const eventThread = params.threadId || params.thread_id;
        const turnId = params.turnId || params.turn_id;
        if ((event.method === 'item/completed' || event.type === 'item.completed') && item?.type === 'contextCompaction' &&
            typeof item.id === 'string' && typeof turnId === 'string' && eventThread === sessionId) {
          events.push(`${sessionId}:${turnId}:${item.id}`);
        }
      } catch {}
    }
    if (!events.length) return;
    this.store.loadFromDisk();
    const thread = this.store.getTask(threadId);
    if (!thread || thread.archivedAt) return;
    const boundSessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    if (boundSessionId !== sessionId) return;
    const old = thread.nativeSessionCompaction?.[role];
    const cursor = old?.sessionId === sessionId ? old : { sessionId, turnsSinceCompaction: 0 };
    const seen = new Set(cursor.recentNativeAutoCompactionEventIds || []);
    const fresh: string[] = [];
    for (const id of events) if (!seen.has(id)) { seen.add(id); fresh.push(id); }
    if (!fresh.length) return;
    const precedingOperation = cursor.lastOperationId
      ? (thread.rounds || []).flatMap((round) => round.nativeSessionCompactions || []).find((operation) => operation.id === cursor.lastOperationId)
      : undefined;
    const ineffective = precedingOperation?.phase === 'succeeded' && cursor.turnsSinceCompaction < 2;
    thread.nativeSessionCompaction = {
      ...thread.nativeSessionCompaction,
      [role]: {
        ...cursor,
        revision: (cursor.revision || 0) + 1,
        recentNativeAutoCompactionEventIds: [...seen].slice(-64),
        turnsSinceCompaction: 0,
        turnCounterResetId: fresh[fresh.length - 1],
        ...(ineffective ? { autoDisabledReason: 'ineffective: native compaction recurred within two business turns' } : {}),
      },
    };
    this.store.upsertTask(thread);
  }

  /** Count a completed business turn and acknowledge only the recovery version it actually carried. */
  public recordBusinessSuccess(threadId: string, role: TaskRole, sessionId: string, turnId: string, recoveryVersion?: string): void {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    const thread = this.store.getTask(threadId);
    if (!thread || thread.archivedAt) return;
    const boundSessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    const successfulStatus = role === 'developer'
      ? ['running', 'queued'].includes(thread.status)
      : ['merging', 'completed', 'wait_decision'].includes(thread.status) || (thread.status === 'queued' && thread.queuePhase === 'merge') || (['running', 'queued'].includes(thread.status) && findInFlightRound(thread)?.type === 'rework');
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
        ...(recoveryVersion && cursor.recoveryVersion === recoveryVersion && cursor.autoSuppressedOperationId === recoveryVersion
          ? { autoSuppressedOperationId: undefined }
          : {}),
      },
    };
    this.store.upsertTask(thread);
  }

  /** Explicit, paused-task action. It never starts the next business round. */
  public async compactPausedTask(threadId: string, role: TaskRole): Promise<NativeSessionCompactionOperation> {
    const root = path.dirname(this.store.getStoragePath());
    this.store.loadFromDisk();
    let thread = this.store.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread || thread.archivedAt || thread.status !== 'paused') throw new Error('Native compaction is available only for an unarchived paused task.');
    if (role === 'reviewer' && thread.reviewFlow !== 'dedicated') throw new Error('Legacy caller/requester sessions are not managed by Bridge compaction.');
    const agent = role === 'developer' ? thread.targetAgent : thread.initiatorAgent;
    const boundAgent = agent;
    const boundModel = role === 'developer' ? thread.targetModel : thread.reviewerModel;
    const boundWorktree = JSON.stringify(thread.worktree && {
      path: thread.worktree.path, branch: thread.worktree.taskBranch, base: thread.worktree.baseCommit,
      head: thread.worktree.headCommit, approved: thread.worktree.approvedCommit,
    });
    const sessionId = role === 'developer' ? thread.receiverSessionId : thread.reviewerSessionId;
    const captured = role === 'developer' ? thread.receiverSessionCaptured : thread.reviewerSessionCaptured;
    if (!captured || !isValidSessionId(sessionId || '')) throw new Error(`The ${role} native session is not captured.`);
    const nativeSessionId = sessionId!;
    // A captured ledger ID is not proof of ownership. Verify the durable static
    // binding before capability probes, leases, recovery files, or native RPC.
    const assignment = thread.nativeSessionAssignments?.[role];
    if (!assignment || !Number.isSafeInteger(assignment.sessionEpoch) || assignment.sessionEpoch < 1 ||
        assignment.namespace !== nativeSessionNamespace(agent)) {
      throw new Error('SESSION_OWNERSHIP_UNVERIFIED: Captured native session has no matching pinned namespace and epoch.');
    }
    const ownership = new SessionOwnershipStore(root).verify({
      agent, namespace: assignment.namespace, sessionId: nativeSessionId, threadId, role,
      sessionEpoch: assignment.sessionEpoch,
    });
    if (!ownership.ok) throw new Error(`${ownership.code}: ${ownership.reason}`);
    if (agent !== 'codex') throw new Error(`${agent} is ${nativeCompactionCapability(agent)} for Bridge initiated compaction.`);
    const model = role === 'developer'
      ? thread.targetModel || resolveConfiguredModel(agent)
      : thread.reviewerModel || thread.initiatorModel || resolveConfiguredModel(agent, undefined, 'initiator');
    if (!model) throw new Error(`No resolved ${role} model is available; refusing to guess.`);
    const initialBudget = remainingMaintenanceBudget(thread);
    if (!initialBudget || initialBudget.remainingMs <= 0) {
      throw new Error('NATIVE_MAINTENANCE_BUDGET_EXHAUSTED: No active execution budget remains for native maintenance.');
    }
    let round = [...(thread.rounds || [])].reverse().find((entry) => entry.type !== 'approval');
    if (!round) throw new Error('The task has no business round to attach the maintenance record to.');

    const executions = new TaskExecutionRegistry(root);
    let registration: TaskExecutionRegistration | undefined;
    let nativeChildPid: number | undefined;
    let nativeChildBirthId: string | undefined;
    let nativeSubtreeTimer: NodeJS.Timeout | undefined;
    let nativeSubtreeTail: Promise<void> = Promise.resolve();
    let nativeRunUnconfirmed = false;
    let nativeExitConfirmed = false;
    let nativeSpawnNotStarted = false;
    let maintenanceTimeoutMs = 0;
    let admittedBudgetLimitMs = 0;
    let admittedBudgetSpentMs = 0;
    let maintenanceGuard: HostSuspensionGuard | undefined;
    let maintenanceStartedAt: number | undefined;
    let maintenanceActiveMs = 0;
    const cancellation = new CancellationRegistry(root);
    const id = crypto.randomUUID();
    const baselineCancellation = cancellation.peek(threadId);
    const isNewCancellation = () => {
      const current = cancellation.peek(threadId);
      return !!current && JSON.stringify(current) !== JSON.stringify(baselineCancellation);
    };
    const finishMaintenanceClock = () => {
      if (maintenanceStartedAt === undefined) return maintenanceActiveMs;
      const guard = maintenanceGuard;
      const now = monotonicNowMs();
      guard?.sync();
      maintenanceActiveMs += Math.max(0, now - maintenanceStartedAt - (guard?.suspendedWithin(maintenanceStartedAt, now) || 0));
      guard?.stop();
      maintenanceStartedAt = undefined;
      return maintenanceActiveMs;
    };
    const remainingMaintenanceTimeMs = () => {
      if (maintenanceStartedAt === undefined) return maintenanceTimeoutMs;
      const now = monotonicNowMs();
      maintenanceGuard?.sync();
      const elapsed = Math.max(0, now - maintenanceStartedAt - (maintenanceGuard?.suspendedWithin(maintenanceStartedAt, now) || 0));
      return Math.max(0, maintenanceTimeoutMs - elapsed);
    };
    let resourceFinalized = false;
    let resourcesReleased = false;
    const releaseResources = () => {
      if (resourcesReleased || nativeRunUnconfirmed || nativeChildPid && processAlive(nativeChildPid)) return;
      resourcesReleased = true;
      if (registration) executions.end(registration);
      executions.dispose();
    };
    try {
      const pending = cancellation.peek(threadId);
      if (pending?.action === 'delete') throw new Error('Task deletion is in progress.');
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      if (!fresh || fresh.status !== 'paused' || fresh.archivedAt ||
          (role === 'developer' ? fresh.receiverSessionId : fresh.reviewerSessionId) !== nativeSessionId ||
          (role === 'developer' ? fresh.targetAgent : fresh.initiatorAgent) !== boundAgent ||
          (role === 'developer' ? fresh.targetModel : fresh.reviewerModel) !== boundModel ||
          JSON.stringify(fresh.worktree && {
            path: fresh.worktree.path, branch: fresh.worktree.taskBranch, base: fresh.worktree.baseCommit,
            head: fresh.worktree.headCommit, approved: fresh.worktree.approvedCommit,
          }) !== boundWorktree) {
        throw new Error('Task state or native session changed before maintenance acquired ownership.');
      }
      thread = fresh;
      const freshRound = [...(thread.rounds || [])].reverse().find((entry) => entry.type !== 'approval');
      if (!freshRound) throw new Error('The task has no business round to attach the maintenance record to.');
      round = freshRound;
      const ownerBirthId = getProcessBirthId(process.pid);
      if (!ownerBirthId) throw new Error('SESSION_OWNERSHIP_UNVERIFIED: Maintenance controller process identity is unavailable.');
      const revision = fresh.execution?.revision ?? 0;
      const now = new Date().toISOString();
      thread = await this.store.transitionExecution(threadId, revision, (current) => {
        if (current.status !== 'paused' || current.archivedAt || current.execution?.version !== 2 ||
            current.execution.run || current.execution.next || current.cleanup ||
            current.execution.operationId !== fresh.execution?.operationId ||
            current.execution.generation !== fresh.execution?.generation ||
            JSON.stringify(current.nativeSessionAssignments?.[role]) !== JSON.stringify(assignment)) {
          throw new Error('Maintenance admission changed: run, handoff, cleanup, or session assignment is present.');
        }
        const budget = remainingMaintenanceBudget(current);
        if (!budget || budget.remainingMs <= 0) {
          throw new Error('NATIVE_MAINTENANCE_BUDGET_EXHAUSTED: No active execution budget remains for native maintenance.');
        }
        admittedBudgetLimitMs = budget.limitMs;
        admittedBudgetSpentMs = budget.spentActiveMs;
        maintenanceTimeoutMs = Math.min(MAX_NATIVE_COMPACTION_ACTIVE_MS, budget.remainingMs);
        current.execution = {
          ...current.execution,
          run: {
            id, // replaced below once generated; kept unique for the admission CAS
            purpose: 'native_maintenance', role,
            owner: { pid: process.pid, birthId: ownerBirthId },
            step: 'native_maintenance', state: 'active', startedAt: now,
          },
        };
      });
      registration = executions.begin(
        threadId,
        freshRound.round,
        agent,
        getTaskExecutionPath(thread),
        'maintenance',
      );
      maintenanceGuard = new HostSuspensionGuard();
      maintenanceGuard.start();
      maintenanceStartedAt = monotonicNowMs();
    } catch (error) {
      if (!registration) {
        try {
          this.store.loadFromDisk();
          const current = this.store.getTask(threadId);
          if (current?.execution?.run?.id === id && current.execution.run.purpose === 'native_maintenance' &&
              !current.execution.run.spawnIntentAt && !current.execution.run.child && !current.cleanup) {
            await this.store.transitionExecution(threadId, current.execution.revision, (latest) => {
              if (latest.execution?.run?.id === id && latest.execution.run.purpose === 'native_maintenance' &&
                  !latest.execution.run.spawnIntentAt && !latest.execution.run.child && !latest.cleanup) latest.execution.run = undefined;
            });
          }
        } catch {}
      }
      executions.dispose(); throw error;
    }
    const admittedOperationId = thread.execution!.operationId;
    const admittedGeneration = thread.execution!.generation;
    const logDir = roleLogDir(thread, role, agent, root);
    const base = `${safeThreadIdPart(threadId)}_round_${round.round}.native_${id}`;
    const logPath = path.join(logDir, role === 'reviewer' ? `${base}.reviewer.log` : `${base}.log`);
    const recoveryPath = path.join(logDir, `${base}.recovery.json`);
    const operation: NativeSessionCompactionOperation = {
      id, role: role as 'developer' | 'reviewer', agent, sessionId: nativeSessionId, model, reason: 'manual',
      phase: 'prepared', revision: 1, startedAt: new Date().toISOString(), logPath, recoveryPath,
    };
    try {
      const identity = this.capabilityIdentity(agent, model);
      if (!identity) throw new Error('Codex provider route could not be safely resolved; refusing native compaction.');
      fs.writeFileSync(recoveryPath, recoveryDocument(thread, role, agent, model, nativeSessionId), { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      fs.writeFileSync(logPath, `${JSON.stringify({ at: new Date().toISOString(), operationId: id, phase: 'prepared', agent, model })}\n`, { encoding: 'utf8', mode: 0o600, flag: 'wx' });
      thread = await this.store.transitionExecution(threadId, thread.execution!.revision, (current) => {
        if (current.status !== 'paused' || current.execution?.run?.id !== id || current.execution.run.purpose !== 'native_maintenance' || current.cleanup) {
          throw new Error('Native maintenance preparation lost its protected run.');
        }
        const targetRound = [...(current.rounds || [])].reverse().find((entry) => entry.round === round!.round);
        if (!targetRound) throw new Error('The business round disappeared during maintenance admission.');
        targetRound.nativeSessionCompactions = [...(targetRound.nativeSessionCompactions || []), operation];
        const oldCursor = current.nativeSessionCompaction?.[role];
        current.nativeSessionCompaction = {
          ...current.nativeSessionCompaction,
          [role]: { revision: (oldCursor?.revision || 0) + 1, sessionId: nativeSessionId, lastOperationId: id, turnsSinceCompaction: 0, recoveryVersion: id, turnCounterResetId: id },
        };
      });

      const executable = this.detector.resolveExecutable('codex');
      if (!executable) throw new Error('Codex executable is unavailable.');
      operation.phase = 'running';
      operation.revision++;
      thread = await this.store.transitionExecution(threadId, thread.execution!.revision, (current) => {
        if (current.execution?.run?.id !== id || current.execution.run.purpose !== 'native_maintenance' || current.status !== 'paused' || current.cleanup) {
          throw new Error('Native maintenance lost its protected run before RPC admission.');
        }
        const targetRound = [...(current.rounds || [])].reverse().find((entry) => entry.round === round!.round);
        const target = targetRound?.nativeSessionCompactions?.find((item) => item.id === id);
        if (!target) throw new Error('Native maintenance operation record disappeared before RPC admission.');
        target.phase = 'running'; target.revision = operation.revision;
      });
      thread = await this.store.transitionExecution(threadId, thread.execution!.revision, (current) => {
        const activeRun = current.execution?.run;
        if (current.status !== 'paused' || !current.execution || !activeRun || activeRun.id !== id ||
            activeRun.purpose !== 'native_maintenance' || activeRun.unconfirmed || current.cleanup || current.execution.next) {
          throw new Error('Native maintenance lost run admission before spawn intent.');
        }
        activeRun.state = 'starting';
        activeRun.spawnIntentAt = new Date().toISOString();
      });
      const rpcTimeoutMs = remainingMaintenanceTimeMs();
      if (rpcTimeoutMs <= 0) throw new Error('NATIVE_MAINTENANCE_BUDGET_EXHAUSTED: Setup consumed the remaining active budget.');
      const result = await this.codex.compact({
        executable, sessionId: nativeSessionId, cwd: getTaskExecutionPath(thread), model, operationId: id, role: role as 'developer' | 'reviewer',
        timeoutMs: rpcTimeoutMs,
        expectedProviderId: identity.providerId,
        cancelled: isNewCancellation,
        onUsage: (usage) => this.recordMaintenanceUsage(threadId, round!.round, id, role, agent, model!, usage),
        onSpawnNotStarted: async () => {
          nativeSpawnNotStarted = true;
          this.store.loadFromDisk();
          const latest = this.store.getTask(threadId);
          if (!latest?.execution || latest.execution.run?.id !== id) throw new Error('Native maintenance spawn refusal lost its run.');
          await this.store.transitionExecution(threadId, latest.execution.revision, (current) => {
            const activeRun = current.execution?.run;
            if (!current.execution || !activeRun || activeRun.id !== id || activeRun.purpose !== 'native_maintenance' ||
                activeRun.child || activeRun.unconfirmed) throw new Error('Cannot clear spawn intent after a child was registered.');
            activeRun.spawnIntentAt = undefined;
            activeRun.state = 'active';
          }, latest.cleanup?.token);
        },
        jobDirectory: thread.artifactDir ? path.join(thread.artifactDir, 'worker_log') : undefined,
        onChildPid: async (pid, birthId, child) => {
          executions.attachChildPid(pid, registration);
          if (!pid && nativeSubtreeTimer) { clearInterval(nativeSubtreeTimer); nativeSubtreeTimer = undefined; }
          if (!pid) await nativeSubtreeTail;
          this.store.loadFromDisk();
          const current = this.store.getTask(threadId);
          const run = current?.execution?.run;
          if (!current?.execution || run?.id !== id || run.purpose !== 'native_maintenance' ||
              current.execution.operationId !== admittedOperationId ||
              current.execution.generation !== admittedGeneration) {
            throw new Error('Native maintenance run identity changed while registering a child.');
          }
          const cleanupToken = current.cleanup?.token;
          const runExitProof = !pid && nativeChildPid && subtreeEvidenceExit(mergeSubtreeEvidence(run?.subtrees,
            nativeChildBirthId ? [{ root: { pid: nativeChildPid, birthId: nativeChildBirthId }, descendants: [] }] : [])) === 'exited'
            ? { runId: id, operationId: admittedOperationId, generation: admittedGeneration }
            : undefined;
          const windowsJob = windowsJobOf(child);
          const sampledDescendants = pid && birthId && !windowsJob ? await captureChildDescendants(pid, birthId) : [];
          await this.store.transitionExecution(threadId, current.execution.revision, (fresh) => {
            const activeRun = fresh.execution?.run;
            if (!fresh.execution || !activeRun || activeRun.id !== id || activeRun.purpose !== 'native_maintenance' ||
                fresh.execution.operationId !== admittedOperationId ||
                fresh.execution.generation !== admittedGeneration || (fresh.cleanup && fresh.cleanup.token !== cleanupToken)) {
              throw new Error('Native maintenance child registration lost its run or cleanup guard.');
            }
            if (pid) {
              if (!birthId || probeProcessIdentity(pid, birthId) !== 'alive') {
                activeRun.unconfirmed = true; activeRun.state = 'stopping'; nativeRunUnconfirmed = true;
              }
              nativeChildPid = pid; nativeChildBirthId = birthId;
              const descendants = sampledDescendants;
              if (descendants.some((item) => !item.birthId)) {
                activeRun.unconfirmed = true; activeRun.state = 'stopping'; nativeRunUnconfirmed = true;
              }
              const priorSubtrees = activeRun.subtrees || [];
              const priorExit = subtreeEvidenceExit(priorSubtrees);
              const priorSettled = !activeRun.child && (!priorSubtrees.length || priorExit === 'exited');
              const additions = birthId
                ? [{ root: { pid, birthId }, descendants, sampledAt: new Date().toISOString(), windowsJob }]
                : [{ root: { pid }, descendants, sampledAt: new Date().toISOString() }];
              activeRun.subtrees = mergeSubtreeEvidence(priorSettled ? undefined : priorSubtrees, additions);
              if (!priorSettled) {
                activeRun.unconfirmed = true;
                activeRun.state = 'stopping';
                nativeRunUnconfirmed = true;
              }
              activeRun.child = { pid, birthId };
              if (!nativeRunUnconfirmed) activeRun.state = 'active';
              activeRun.registeredAt = new Date().toISOString();
              activeRun.step = 'native_maintenance';
            } else {
              const currentSubtrees = mergeSubtreeEvidence(activeRun.subtrees, nativeChildPid && nativeChildBirthId
                ? [{ root: { pid: nativeChildPid, birthId: nativeChildBirthId }, descendants: [] }]
                : []);
              // The adapter reports this exact ChildProcess close, but a reparented
              // descendant can outlive it. Every immutable identity must probe exited.
              const allExited = subtreeEvidenceExit(currentSubtrees) === 'exited';
              if (nativeChildPid && !allExited) {
                activeRun.subtrees = currentSubtrees;
                activeRun.unconfirmed = true; activeRun.state = 'stopping'; nativeRunUnconfirmed = true;
                return;
              }
              if (nativeChildPid && allExited) {
                // A previous close notification may have arrived while a
                // detached descendant was still alive. Re-evaluate from the
                // immutable durable identities; once each exact PID/birthId is
                // confirmed exited, the earlier uncertainty is resolved.
                nativeRunUnconfirmed = false;
                nativeExitConfirmed = true;
                activeRun.unconfirmed = undefined;
                activeRun.state = 'active';
              }
              if (nativeChildPid && allExited && subtreeEvidenceExit(mergeSubtreeEvidence(activeRun.subtrees, currentSubtrees)) !== 'exited') {
                activeRun.subtrees = currentSubtrees;
                activeRun.unconfirmed = true; activeRun.state = 'stopping'; nativeRunUnconfirmed = true; nativeExitConfirmed = false;
                return;
              }
              nativeChildPid = undefined; nativeChildBirthId = undefined;
              activeRun.child = undefined;
              activeRun.subtrees = [];
              activeRun.registeredAt = undefined;
              activeRun.spawnIntentAt = undefined;
              if (resourceFinalized && nativeExitConfirmed && !nativeRunUnconfirmed) {
                fresh.execution!.run = undefined;
              }
            }
          }, cleanupToken, runExitProof);
          if (pid && nativeChildPid === pid && !nativeRunUnconfirmed && !nativeSubtreeTimer && !windowsJob) {
            nativeSubtreeTimer = setInterval(() => {
              nativeSubtreeTail = nativeSubtreeTail.then(async () => {
                if (nativeChildPid !== pid || !nativeChildBirthId) return;
                const descendants = await captureChildDescendants(pid, nativeChildBirthId);
                if (descendants.some((item) => !item.birthId)) nativeRunUnconfirmed = true;
                this.store.loadFromDisk();
                const latest = this.store.getTask(threadId);
                if (!latest?.execution || latest.execution.run?.id !== id) { nativeRunUnconfirmed = true; return; }
                await this.store.transitionExecution(threadId, latest.execution.revision, (fresh) => {
                  const activeRun = fresh.execution?.run;
                  if (!fresh.execution || !activeRun || activeRun.id !== id || activeRun.purpose !== 'native_maintenance' || fresh.cleanup) {
                    nativeRunUnconfirmed = true; return;
                  }
                  activeRun.subtrees = mergeSubtreeEvidence(activeRun.subtrees, [
                    { root: { pid, birthId: nativeChildBirthId! }, descendants, sampledAt: new Date().toISOString() },
                  ]);
                  if (nativeRunUnconfirmed) { activeRun.unconfirmed = true; activeRun.state = 'stopping'; }
                });
              }).catch(() => { nativeRunUnconfirmed = true; });
            }, 150);
            nativeSubtreeTimer.unref();
          }
          if (nativeRunUnconfirmed) throw new Error('Native maintenance child identity or subtree exit could not be confirmed.');
          if (!pid && resourceFinalized) releaseResources();
        },
      });
      finishMaintenanceClock();
      operation.activeDurationMs = maintenanceActiveMs;
      this.store.loadFromDisk();
      const fresh = this.store.getTask(threadId);
      if (!fresh || fresh.archivedAt || fresh.status !== 'paused' || isNewCancellation() ||
          (role === 'developer' ? fresh.receiverSessionId : fresh.reviewerSessionId) !== nativeSessionId ||
          (role === 'developer' ? fresh.targetAgent : fresh.initiatorAgent) !== boundAgent ||
          (role === 'developer' ? fresh.targetModel : fresh.reviewerModel) !== boundModel ||
          JSON.stringify(fresh.worktree && {
            path: fresh.worktree.path, branch: fresh.worktree.taskBranch, base: fresh.worktree.baseCommit,
            head: fresh.worktree.headCommit, approved: fresh.worktree.approvedCommit,
          }) !== boundWorktree || fresh.execution?.run?.id !== id) {
        operation.phase = 'unknown';
        operation.errorCode = 'TASK_CHANGED_DURING_MAINTENANCE';
        operation.revision++;
        return operation;
      }
      operation.phase = 'succeeded';
      operation.nativeTurnId = result.nativeTurnId;
      operation.finishedAt = new Date().toISOString();
      operation.revision++;
      const usage = result.usage || emptyMaintenanceUsage(id, role, agent, model);
      usage.id = id;
      await this.store.transitionExecution(threadId, fresh.execution!.revision, (current) => {
        if (current.status !== 'paused' || current.execution?.run?.id !== id ||
            current.execution.run.purpose !== 'native_maintenance' || current.execution.operationId !== fresh.execution!.operationId ||
            current.execution.generation !== fresh.execution!.generation || current.execution.run.spawnIntentAt || current.execution.run.child ||
            current.execution.run.unconfirmed || current.execution.next || current.cleanup || isNewCancellation()) {
          throw new Error('Native maintenance settlement lost its run, operation, or paused task guard.');
        }
        if (current.execution.budget.limitMs !== admittedBudgetLimitMs || current.execution.budget.spentActiveMs !== admittedBudgetSpentMs) {
          throw new Error('Native maintenance budget changed during protected maintenance.');
        }
        current.execution.budget.spentActiveMs += maintenanceActiveMs;
        const freshRound = [...(current.rounds || [])].reverse().find((entry) => entry.round === round.round);
        if (freshRound) {
          freshRound.nativeSessionCompactions = [...(freshRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
          freshRound.tokenUsage = appendTokenUsageComponent(freshRound.tokenUsage, usage, `legacy-business-round-${round.round}`);
        }
        current.execution.run = undefined;
      });
      fs.appendFileSync(logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, nativeTurnId: operation.nativeTurnId, usage: result.usage || { available: false } })}\n`, 'utf8');
      return operation;
    } catch (error: any) {
      finishMaintenanceClock();
      operation.activeDurationMs = maintenanceActiveMs;
      this.recordMaintenanceUsage(threadId, round!.round, id, role, agent, model!);
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
        try {
          const cleanupProof = fresh.cleanup && nativeExitConfirmed && !nativeRunUnconfirmed
            ? { runId: id, operationId: admittedOperationId, generation: admittedGeneration }
            : undefined;
          await this.store.transitionExecution(threadId, fresh.execution?.revision ?? 0, (current) => {
            const activeRun = current.execution?.run;
            if (!current.execution || !activeRun || activeRun.id !== id || activeRun.purpose !== 'native_maintenance') return;
            const currentRound = [...(current.rounds || [])].reverse().find((entry) => entry.round === round!.round);
            if (currentRound) currentRound.nativeSessionCompactions = [...(currentRound.nativeSessionCompactions || []).filter((item) => item.id !== id), operation];
            if (current.execution.budget.limitMs === admittedBudgetLimitMs && current.execution.budget.spentActiveMs === admittedBudgetSpentMs) {
              current.execution.budget.spentActiveMs += maintenanceActiveMs;
            }
            if (nativeChildPid || nativeRunUnconfirmed || activeRun.spawnIntentAt) {
              activeRun.state = 'stopping'; activeRun.unconfirmed = true;
              activeRun.settlementPending = true;
            }
            else current.execution.run = undefined;
          }, fresh.cleanup?.token, cleanupProof);
        } catch { /* A failed CAS leaves the durable run protected for recovery. */ }
      }
      try { fs.appendFileSync(logPath, `${JSON.stringify({ at: operation.finishedAt, operationId: id, phase: operation.phase, errorCode: operation.errorCode, error: operation.error })}\n`, 'utf8'); } catch {}
      return operation;
    } finally {
      if (nativeSubtreeTimer) clearInterval(nativeSubtreeTimer);
      try { await nativeSubtreeTail; } catch { nativeRunUnconfirmed = true; }
      if (maintenanceGuard) maintenanceGuard.stop();
      resourceFinalized = true;
      releaseResources();
    }
  }
}

interface NativeRunSnapshot { id: string; operationId: string; generation: number; priorStep: string; }

type ProcessEvidenceExit = 'exited' | 'alive' | 'unknown';

function processEvidenceKey(identity: ProcessIdentity): string {
  return `${identity.pid}:${identity.birthId || 'unknown'}`;
}

/** Preserve every identity ever observed for this run until every identity is proven exited. */
function mergeSubtreeEvidence(
  existing: ExecutionSubtreeEvidence[] | undefined,
  additions: ExecutionSubtreeEvidence[],
): ExecutionSubtreeEvidence[] {
  const merged = new Map<string, ExecutionSubtreeEvidence>();
  for (const evidence of [...(existing || []), ...additions]) {
    const key = processEvidenceKey(evidence.root);
    const current = merged.get(key) || { root: { ...evidence.root }, descendants: [] };
    const descendants = new Map(current.descendants.map((identity) => [processEvidenceKey(identity), { ...identity }] as const));
    for (const identity of evidence.descendants) descendants.set(processEvidenceKey(identity), { ...identity });
    current.descendants = [...descendants.values()];
    if (evidence.windowsJob) current.windowsJob = evidence.windowsJob;
    if (evidence.sampledAt && (!current.sampledAt || evidence.sampledAt > current.sampledAt)) current.sampledAt = evidence.sampledAt;
    merged.set(key, current);
  }
  return [...merged.values()];
}

function processEvidenceExit(identities: ProcessIdentity[]): ProcessEvidenceExit {
  if (!identities.length || identities.some((identity) => !identity.birthId)) return 'unknown';
  let unknown = false;
  for (const identity of identities) {
    const status = probeProcessIdentity(identity.pid, identity.birthId);
    if (status === 'alive') return 'alive';
    if (status === 'unknown') unknown = true;
  }
  return unknown ? 'unknown' : 'exited';
}

function subtreeEvidenceExit(subtrees: ExecutionSubtreeEvidence[] | undefined): ProcessEvidenceExit {
  if (!subtrees?.length) return 'exited';
  if (subtrees.some(tree => tree.windowsJob && !windowsJobFinished(tree.windowsJob))) return 'unknown';
  const identities = subtrees.flatMap((tree) => [tree.root, ...tree.descendants]);
  return processEvidenceExit(identities);
}

function snapshotNativeRun(thread: DelegationThread): NativeRunSnapshot | undefined {
  const control = thread.execution;
  const run = control?.run;
  if (control?.version !== 2 || !run || (run.state !== 'active' && run.state !== 'starting') || run.unconfirmed) return undefined;
  return { id: run.id, operationId: control.operationId, generation: control.generation, priorStep: run.step };
}

async function setNativeRunMaintenanceStep(store: TaskStore, threadId: string, expected: NativeRunSnapshot, enabled: boolean): Promise<void> {
  store.loadFromDisk();
  const fresh = store.getTask(threadId);
  if (!fresh?.execution || fresh.execution.run?.id !== expected.id) throw new Error('Native maintenance lost its v2 run.');
  await store.transitionExecution(threadId, fresh.execution.revision, (current) => {
    const control = current.execution;
    const run = control?.run;
    if (!control || control.version !== 2 || !run || run.id !== expected.id || (run.state !== 'active' && run.state !== 'starting') || run.unconfirmed ||
        control.operationId !== expected.operationId || control.generation !== expected.generation || control.next || current.cleanup) {
      throw new Error('Native maintenance step no longer matches the active run.');
    }
    run.step = enabled ? 'native_maintenance' : expected.priorStep;
  });
}

async function bindNativeRunChild(
  store: TaskStore,
  threadId: string,
  expected: NativeRunSnapshot,
  pid: number | undefined,
  birthId?: string,
  windowsJob?: WindowsJobBinding,
): Promise<void> {
  store.loadFromDisk();
  const fresh = store.getTask(threadId);
  if (!fresh?.execution || fresh.execution.run?.id !== expected.id) throw new Error('Native maintenance child has no matching v2 run.');
  let descendants: Awaited<ReturnType<typeof captureChildDescendants>> = [];
  if (pid) {
    if (!birthId || probeProcessIdentity(pid, birthId) !== 'alive') throw new Error('Native maintenance child identity is unverified.');
    if (!windowsJob) descendants = await captureChildDescendants(pid, birthId);
  }
  let bindingUnconfirmed = false;
  await store.transitionExecution(threadId, fresh.execution.revision, (current) => {
    const control = current.execution;
    const run = control?.run;
    if (!control || control.version !== 2 || !run || run.id !== expected.id || (run.state !== 'active' && run.state !== 'starting') || run.unconfirmed ||
        control.operationId !== expected.operationId || control.generation !== expected.generation || control.next || current.cleanup) {
      throw new Error('Native maintenance child registration lost its run identity.');
    }
    if (pid && birthId) {
      const priorSubtrees = run.subtrees || [];
      const priorExit = subtreeEvidenceExit(priorSubtrees);
      const priorSettled = !run.child && (!priorSubtrees.length || priorExit === 'exited');
      run.subtrees = mergeSubtreeEvidence(priorSettled ? undefined : priorSubtrees, [
        { root: { pid, birthId }, descendants, sampledAt: new Date().toISOString(), windowsJob },
      ]);
      run.child = { pid, birthId };
      run.registeredAt = new Date().toISOString();
      run.step = 'native_maintenance';
      if (!priorSettled || descendants.some((item) => !item.birthId)) {
        run.state = 'stopping';
        run.unconfirmed = true;
        bindingUnconfirmed = true;
      }
    } else {
      const subtrees = mergeSubtreeEvidence(run.subtrees, run.child?.birthId
        ? [{ root: { ...run.child }, descendants: [] }]
        : []);
      const exited = subtreeEvidenceExit(subtrees) === 'exited';
      if ((run.child || subtrees.length) && (!exited || run.unconfirmed)) {
        run.subtrees = subtrees;
        run.state = 'stopping'; run.unconfirmed = true; bindingUnconfirmed = true;
        return;
      }
      run.child = undefined; run.subtrees = undefined; run.registeredAt = undefined; run.step = expected.priorStep;
    }
  });
  if (bindingUnconfirmed) throw new Error('Native maintenance subtree identity or exit is unconfirmed.');
  if (!pid) {
    store.loadFromDisk();
    const latest = store.getTask(threadId)?.execution?.run;
    if (latest?.id === expected.id && latest.unconfirmed) throw new Error('Native maintenance process subtree exit is unconfirmed.');
  }
}

function processAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error: any) { return error?.code === 'EPERM'; }
}

function remainingMaintenanceBudget(thread: DelegationThread): { limitMs: number; spentActiveMs: number; remainingMs: number } | undefined {
  const budget = thread.execution?.budget;
  if (!budget || !Number.isFinite(budget.limitMs) || !Number.isFinite(budget.spentActiveMs) ||
      budget.limitMs < 0 || budget.spentActiveMs < 0) return undefined;
  return {
    limitMs: budget.limitMs,
    spentActiveMs: budget.spentActiveMs,
    remainingMs: Math.max(0, budget.limitMs - budget.spentActiveMs),
  };
}
