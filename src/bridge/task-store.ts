import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as zlib from 'zlib';
import { EventEmitter } from 'events';
import {
  DelegationThread,
  DelegationRound,
  ThreadStatus,
  RoundStatus,
  ResponseAssessment,
  ProcessIdentity,
  TaskCleanupState,
  TaskInterruption,
  MergeRequestInfo,
  TokenUsageMetrics,
  TokenUsageComponent,
  appendInterruption,
  isMergePending,
  isMergeReview,
  findInFlightRound,
  enterTaskFailure,
} from './types';
import {
  archiveCompletedTaskLogs,
  collectExpectedArchiveLogFiles,
  archiveThreadData,
  buildFinalSummary,
  ensureFinalSummary,
  isTerminalThreadStatus,
  canArchiveTask,
  removeTaskArtifacts,
} from './task-summary';
import { cleanupThreadNativeSessions, collectThreadSessionRefs } from './native-session-cleanup';
import { NativeSessionLock, NativeSessionLease } from './native-session-lock';
import { MeshFileLock } from './mesh-file-lock';
import { WindowsTaskLock } from './windows-process-helper';
import { probeProcessIdentity } from './platform';
import {
  validateV2ExecutionEligibility,
  ExecutionEligibilityResult,
  ExecutionEligibilityCode,
} from './execution-gate';

export {
  validateV2ExecutionEligibility,
  ExecutionEligibilityResult,
  ExecutionEligibilityCode,
};

export const MAX_STORED_TASKS = 1000;

function cleanupExecutionProjection(execution?: DelegationThread['execution']): DelegationThread['execution'] | undefined {
  if (!execution) return undefined;
  return {
    version: execution.version, revision: execution.revision, operationId: execution.operationId,
    phase: execution.phase, round: execution.round, purpose: execution.purpose,
    generation: execution.generation, run: execution.run ? structuredClone(execution.run) : undefined,
  } as DelegationThread['execution'];
}

function cleanupThreadProjection(thread: DelegationThread): Record<string, unknown> {
  const worktree = thread.worktree;
  return {
    threadId: thread.threadId, targetAgent: thread.targetAgent, initiatorAgent: thread.initiatorAgent,
    reviewFlow: thread.reviewFlow, receiverSessionId: thread.receiverSessionId,
    receiverSessionCaptured: thread.receiverSessionCaptured, reviewerSessionId: thread.reviewerSessionId,
    reviewerSessionCaptured: thread.reviewerSessionCaptured, nativeSessionAssignments: thread.nativeSessionAssignments,
    retargets: (thread.retargets || []).map(({ fromAgent, toAgent, previousReceiverSessionId }) => ({ fromAgent, toAgent, previousReceiverSessionId })),
    artifactDir: thread.artifactDir, artifactRoot: thread.artifactRoot,
    worktree: worktree ? {
      path: worktree.path, mainPath: worktree.mainPath, commonDir: worktree.commonDir,
      repositoryPath: worktree.repositoryPath, targetRef: worktree.targetRef, deliveryTarget: worktree.deliveryTarget,
      taskBranch: worktree.taskBranch, headCommit: worktree.headCommit, approvedCommit: worktree.approvedCommit,
      approvedSnapshot: worktree.approvedSnapshot, mergeResolutionPath: worktree.mergeResolutionPath,
      supersededMergeResolutionPaths: worktree.supersededMergeResolutionPaths, branch: worktree.branch,
      baseCommit: worktree.baseCommit, mergePending: worktree.mergePending,
      integration: worktree.integration ? {
        phase: worktree.integration.phase, sourceCommit: worktree.integration.sourceCommit,
        targetOldCommit: worktree.integration.targetOldCommit, targetCommit: worktree.integration.targetCommit,
        targetPath: worktree.integration.targetPath, candidateCommit: worktree.integration.candidateCommit,
        mergedCommit: worktree.integration.mergedCommit, stashRecoveryPending: worktree.integration.stashRecoveryPending,
        stashRecoveryLockHeld: worktree.integration.stashRecoveryLockHeld,
        stashRestore: worktree.integration.stashRestore ? { status: worktree.integration.stashRestore.status, sha: worktree.integration.stashRestore.sha } : undefined,
      } : undefined,
    } : undefined,
  };
}

export type ExecutionLockFactory = (lockPath: string) => {
  acquire(onWait?: () => void, cancelled?: () => boolean, timeoutMs?: number): Promise<(() => void | Promise<void>) | undefined>;
  dispose(): void;
};

function canEvictForCapacity(task: DelegationThread): boolean {
  if (task.status === 'completed') {
    return !task.worktree || task.worktree.integration?.phase === 'merged' || task.worktree.integration?.phase === 'no_changes';
  }
  if (task.status !== 'failed') return false;
  if (task.failure?.stage === 'merge' || task.failure?.stage === 'review') return false;
  const worktree = task.worktree;
  return !worktree || (
    !worktree.approvedCommit &&
    worktree.integration?.phase !== 'ready' &&
    worktree.integration?.phase !== 'waiting' &&
    worktree.integration?.phase !== 'merging' &&
    worktree.integration?.phase !== 'conflict' &&
    worktree.integration?.phase !== 'validation_failed' &&
    !!worktree.headCommit && !!worktree.baseCommit && worktree.headCommit === worktree.baseCommit
  );
}

/**
 * Normalize legacy thread status in-place.
 * Historical ledgers may carry `status: 'reworking'`; normalize it to `'running'`
 * on read while preserving all rounds, sessions, worktree, token usage, and interruptions.
 */
export function normalizeThread(thread: DelegationThread): DelegationThread {
  if (!thread) return thread;
  const oldStatus = thread.status as string;
  if (oldStatus === 'waiting_review') thread.status = 'reviewing';
  if (oldStatus === 'waiting_merge') {
    const phase = thread.worktree?.integration?.phase;
    if (phase === 'waiting' || phase === 'ready' || !phase) {
      thread.status = 'queued';
      thread.queuePhase = 'merge';
    } else if (['blocked', 'conflict', 'validation_failed'].includes(phase)) {
      thread.status = 'reviewing';
      if (thread.worktree) thread.worktree.mergePending = true;
    } else {
      thread.status = 'merging';
    }
  }
  if (oldStatus === 'reworking') {
    thread.status = 'running';
  }
  if (thread.status !== 'queued' && thread.status !== 'paused') thread.queuePhase = undefined;
  // Controller records already carry explicit failure and execution identities.
  if (thread.execution) return thread;
  if (oldStatus === 'review_failed' || oldStatus === 'merge_failed' || oldStatus === 'failed') {
    const validStage = thread.failure?.stage && ['bootstrap', 'execution', 'review', 'merge'].includes(thread.failure.stage)
      ? thread.failure.stage : undefined;
    const stage = oldStatus === 'review_failed' ? 'review' : oldStatus === 'merge_failed' ? 'merge' :
      validStage || (thread.reviewBootstrap?.phase === 'failed' ? 'bootstrap' : 'execution');
    const work = [...(thread.rounds || [])].reverse().find(r => r.type !== 'approval');
    const merge = [...(thread.mergeHistory || [])].reverse().find(e => e.result === 'failed');
    const reason = oldStatus === 'review_failed' ? thread.reviewFailure?.reason : oldStatus === 'merge_failed' ? merge?.error :
      thread.failure?.reason || (stage === 'bootstrap' ? thread.reviewBootstrap?.lastError : stage === 'review' ? thread.reviewFailure?.reason :
        stage === 'merge' ? merge?.error : work?.error || work?.interruption?.reason);
    thread.status = 'failed';
    thread.failure = { ...(oldStatus === 'failed' ? thread.failure : undefined), stage,
      reason: reason?.trim() || '旧任务失败，原因未记录',
      at: (oldStatus === 'failed' && thread.failure?.at) || (stage === 'review' ? thread.reviewFailure?.at : stage === 'merge' ? merge?.at : undefined) || thread.updatedAt || thread.createdAt };
  }
  if ((thread.finalSummary?.kind as string) === 'review_failed' && thread.finalSummary) {
    thread.finalSummary.kind = 'failed';
    thread.finalSummary.failureStage = 'review';
  }
  return thread;
}

/**
 * Pick the better of two measurements of the same round.
 *
 * Token usage only ever grows (retries and resumed rounds add up), so the
 * larger native number wins. A native measurement always beats an estimate:
 * an estimate is a guess, and a stale guess must not overwrite a real number.
 */
function preferTokenUsage(
  existing: TokenUsageMetrics | undefined,
  incoming: TokenUsageMetrics | undefined,
  legacyId: string,
): TokenUsageMetrics | undefined {
  if (!existing) return incoming;
  if (!incoming) return existing;
  if (existing.components?.length || incoming.components?.length) {
    const components = new Map<string, TokenUsageComponent>();
    for (const item of existing.components || [legacyUsageComponent(existing, legacyId)]) components.set(item.id, item);
    for (const item of incoming.components || [legacyUsageComponent(incoming, legacyId)]) {
      const current = components.get(item.id);
      components.set(item.id, current ? preferUsageComponent(current, item) : item);
    }
    const values = [...components.values()];
    const measured = values.filter((item) => item.available);
    const uncachedInputTokens = measured.reduce((sum, item) => sum + item.uncachedInputTokens, 0);
    const cacheReadTokens = measured.reduce((sum, item) => sum + item.cacheReadTokens, 0);
    const cacheWriteTokens = measured.reduce((sum, item) => sum + item.cacheWriteTokens, 0);
    const outputTokens = measured.reduce((sum, item) => sum + item.outputTokens, 0);
    const costs = measured.filter((item) => item.costUsd !== undefined);
    const reasoningTokens = measured.some((item) => item.reasoningTokens !== undefined)
      ? measured.reduce((sum, item) => sum + (item.reasoningTokens || 0), 0) : undefined;
    const nativeTotalTokens = measured.some((item) => item.nativeTotalTokens !== undefined)
      ? measured.reduce((sum, item) => sum + (item.nativeTotalTokens || 0), 0) : undefined;
    return {
      ...existing,
      uncachedInputTokens, cacheReadTokens, cacheWriteTokens,
      promptTokens: uncachedInputTokens + cacheReadTokens + cacheWriteTokens,
      outputTokens, totalTokens: uncachedInputTokens + cacheReadTokens + cacheWriteTokens + outputTokens,
      reasoningTokens, nativeTotalTokens,
      source: measured.length && measured.every((item) => item.source === 'native_reported') ? 'native_reported' : measured.length ? 'estimated' : 'unavailable',
      agent: existing.agent, model: existing.model,
      costUsd: costs.length ? costs.reduce((sum, item) => sum + (item.costUsd || 0), 0) : undefined,
      costSource: costs.length ? 'native' : undefined,
      available: measured.length > 0,
      partial: values.some((item) => item.partial) || measured.length !== values.length || undefined,
      attempts: values.length,
      components: values,
    };
  }
  const existingIsNative = existing.source === 'native_reported';
  const incomingIsNative = incoming.source === 'native_reported';
  if (existingIsNative !== incomingIsNative) return existingIsNative ? existing : incoming;
  return (incoming.totalTokens ?? 0) >= (existing.totalTokens ?? 0) ? incoming : existing;
}

function legacyUsageComponent(usage: TokenUsageMetrics, id: string): TokenUsageComponent {
  return {
    id, kind: 'business', role: 'developer', agent: usage.agent,
    model: usage.model, uncachedInputTokens: usage.uncachedInputTokens,
    cacheReadTokens: usage.cacheReadTokens, cacheWriteTokens: usage.cacheWriteTokens,
    outputTokens: usage.outputTokens, reasoningTokens: usage.reasoningTokens,
    nativeTotalTokens: usage.nativeTotalTokens, costUsd: usage.costUsd,
    costSource: usage.costSource, source: usage.source, available: usage.available, partial: usage.partial,
  };
}

function preferUsageComponent(existing: TokenUsageComponent, incoming: TokenUsageComponent): TokenUsageComponent {
  if (!existing.available) return incoming.available ? incoming : existing;
  if (!incoming.available) return existing;
  const total = (value: TokenUsageComponent) => value.uncachedInputTokens + value.cacheReadTokens + value.cacheWriteTokens + value.outputTokens;
  if (total(incoming) < total(existing)) return existing;
  return {
    ...incoming,
    nativeTotalTokens: Math.max(existing.nativeTotalTokens || 0, incoming.nativeTotalTokens || 0) || undefined,
    costUsd: Math.max(existing.costUsd || 0, incoming.costUsd || 0) || undefined,
    costSource: incoming.costUsd !== undefined || existing.costUsd !== undefined ? 'native' : undefined,
  };
}

/**
 * A stale writer must not resurrect a round that a later process condensed,
 * and must not drop the durable execution metadata of a round.
 *
 * Every process keeps its own in-memory copy of the ledger and `upsertTask`
 * prefers the incoming rounds. Without this guard, a supervisor pass holding a
 * copy from before compaction would write the bulky output back and silently
 * undo the compression (and, over time, keep the ledger large forever). The
 * same staleness would otherwise erase the token accounting of a finished
 * round.
 */
function mergeRoundPreferringCompaction(
  incomingExisting: DelegationRound | undefined,
  incoming: DelegationRound,
): DelegationRound {
  const merged: DelegationRound = { ...incoming };

  // Durable execution metadata: never lose it to an older copy.
  merged.tokenUsage = preferTokenUsage(
    incomingExisting?.tokenUsage,
    incoming.tokenUsage,
    `legacy-business-round-${incoming.round}`,
  );
  if (merged.executorAgent === undefined && incomingExisting?.executorAgent !== undefined) {
    merged.executorAgent = incomingExisting.executorAgent;
  }
  if (merged.executorModel === undefined && incomingExisting?.executorModel !== undefined) {
    merged.executorModel = incomingExisting.executorModel;
  }
  if (merged.assessment === undefined && incomingExisting?.assessment !== undefined) {
    merged.assessment = incomingExisting.assessment;
  }
  if (merged.touchedFiles === undefined && incomingExisting?.touchedFiles !== undefined) {
    merged.touchedFiles = incomingExisting.touchedFiles;
  }
  merged.nativeSessionCompactions = mergeNativeCompactions(
    incomingExisting?.nativeSessionCompactions,
    incoming.nativeSessionCompactions,
  );

  if (!incomingExisting?.isCompacted || incoming.isCompacted) return merged;
  return {
    ...merged,
    // Keep the newer status/error/duration from `incoming`, but never lose the
    // condensed payload or its archive pointer.
    output: incomingExisting.output,
    isCompacted: true,
    compactSummary: incomingExisting.compactSummary,
    rawOutputFilePath: incomingExisting.rawOutputFilePath,
    outputBytes: incomingExisting.outputBytes,
  };
}

function mergeNativeCompactions(
  prior: DelegationRound['nativeSessionCompactions'],
  incoming: DelegationRound['nativeSessionCompactions'],
): DelegationRound['nativeSessionCompactions'] {
  const byId = new Map((prior || []).map((operation) => [operation.id, operation]));
  for (const operation of incoming || []) {
    const current = byId.get(operation.id);
    if (!current || operation.revision > current.revision || operation.revision === current.revision && current.phase === 'running' && operation.phase !== 'running') {
      byId.set(operation.id, operation);
    }
  }
  return byId.size ? [...byId.values()] : undefined;
}

function mergeNativeCursors(
  prior: DelegationThread['nativeSessionCompaction'],
  incoming: DelegationThread['nativeSessionCompaction'],
  boundSessions: { developer?: string; reviewer?: string },
): DelegationThread['nativeSessionCompaction'] {
  const merged: NonNullable<DelegationThread['nativeSessionCompaction']> = {};
  for (const role of ['developer', 'reviewer'] as const) {
    const oldCursor = prior?.[role];
    const nextCursor = incoming?.[role];
    const bound = boundSessions[role];
    const oldValid = oldCursor && bound && oldCursor.sessionId === bound ? oldCursor : undefined;
    const nextValid = nextCursor && bound && nextCursor.sessionId === bound ? nextCursor : undefined;
    if (!oldValid) { if (nextValid) merged[role] = nextValid; continue; }
    if (!nextValid) { merged[role] = oldValid; continue; }
    if (oldValid.sessionId !== nextValid.sessionId) {
      merged[role] = nextValid.revision && nextValid.revision > (oldValid.revision || 0) ? nextValid : oldValid;
      continue;
    }
    const chosen = (nextValid.revision || 0) >= (oldValid.revision || 0) ? nextValid : oldValid;
    const other = chosen === nextValid ? oldValid : nextValid;
    const sameRecovery = chosen.recoveryVersion === other.recoveryVersion;
    const sameOperation = chosen.lastOperationId === other.lastOperationId;
    const recoveryDelivered = sameRecovery &&
      (oldValid.recoveryDeliveredVersion === chosen.recoveryVersion || nextValid.recoveryDeliveredVersion === chosen.recoveryVersion);
    merged[role] = {
        ...chosen,
        recentBusinessTurnIds: Array.from(new Set([...(oldValid.recentBusinessTurnIds || []), ...(nextValid.recentBusinessTurnIds || [])])).slice(-64),
        recentNativeAutoCompactionEventIds: Array.from(new Set([...(oldValid.recentNativeAutoCompactionEventIds || []), ...(nextValid.recentNativeAutoCompactionEventIds || [])])).slice(-64),
        turnsSinceCompaction: chosen.turnCounterResetId !== other.turnCounterResetId
          ? chosen.turnsSinceCompaction
          : Math.max(oldValid.turnsSinceCompaction, nextValid.turnsSinceCompaction),
        turnCounterResetId: chosen.turnCounterResetId,
        recoveryVersion: chosen.recoveryVersion || other.recoveryVersion,
        recoveryDeliveredVersion: sameRecovery
          ? (recoveryDelivered ? chosen.recoveryVersion : undefined)
          : chosen.recoveryDeliveredVersion,
        recoveryVerifiedOperationId: sameOperation
          ? chosen.recoveryVerifiedOperationId || other.recoveryVerifiedOperationId
          : chosen.recoveryVerifiedOperationId,
        autoSuppressedOperationId: recoveryDelivered
          ? undefined
          : chosen.autoSuppressedOperationId || other.autoSuppressedOperationId,
        autoDisabledReason: chosen.autoDisabledReason || other.autoDisabledReason,
      };
  }
  return Object.keys(merged).length ? merged : undefined;
}

function bindingRank(thread: DelegationThread, role: 'developer' | 'reviewer'): [number, number, number] {
  const retargets = role === 'developer' ? (thread.retargets || []).length : 0;
  const captured = role === 'developer' ? thread.receiverSessionCaptured : thread.reviewerSessionCaptured;
  return [retargets, captured ? 1 : 0, Date.parse(thread.updatedAt) || 0];
}

function preferBinding(current: DelegationThread, prior: DelegationThread): void {
  for (const role of ['developer', 'reviewer'] as const) {
    const a = bindingRank(current, role);
    const b = bindingRank(prior, role);
    const priorWins = b.some((value, index) => value !== a[index] && value > a[index] && b.slice(0, index).every((x, i) => x === a[i]));
    if (!priorWins) continue;
    if (role === 'developer') {
      current.targetAgent = prior.targetAgent;
      current.targetModel = prior.targetModel;
      current.receiverSessionId = prior.receiverSessionId;
      current.receiverSessionCaptured = prior.receiverSessionCaptured;
      current.retargets = prior.retargets;
    } else {
      current.reviewerSessionId = prior.reviewerSessionId;
      current.reviewerSessionCaptured = prior.reviewerSessionCaptured;
    }
  }
}

function parseTarEntries(tarBuf: Buffer): Map<string, Buffer> {
  const entries = new Map<string, Buffer>();
  let offset = 0;
  while (offset + 512 <= tarBuf.length) {
    const header = tarBuf.subarray(offset, offset + 512);
    let allZeros = true;
    for (let i = 0; i < 512; i++) {
      if (header[i] !== 0) {
        allZeros = false;
        break;
      }
    }
    if (allZeros) break;

    const nullIdx = header.indexOf(0);
    const rawName = header.subarray(0, nullIdx >= 0 && nullIdx < 100 ? nullIdx : 100).toString('utf8').trim();
    if (!rawName) break;

    const sizeStr = header.subarray(124, 136).toString('ascii').replace(/\0/g, ' ').trim();
    const size = parseInt(sizeStr, 8);
    if (isNaN(size) || size < 0) break;

    offset += 512;
    if (offset + size > tarBuf.length) break;
    const content = tarBuf.subarray(offset, offset + size);
    entries.set(rawName, content);
    offset += Math.ceil(size / 512) * 512;
  }
  return entries;
}

function validateDurableArchiveFiles(
  paths: string[] | undefined,
  thread: DelegationThread,
  root?: string,
): boolean {
  if (!paths || !Array.isArray(paths) || paths.length === 0) return false;
  const expectedFiles = collectExpectedArchiveLogFiles(thread, root);

  for (const p of paths) {
    if (!p || typeof p !== 'string') return false;
    if (!fs.existsSync(p)) return false;
    try {
      const stat = fs.statSync(p);
      if (!stat.isFile() || stat.size === 0) return false;
      const lower = p.toLowerCase();
      if (!lower.endsWith('.tar.gz') && !lower.endsWith('.tgz')) return false;

      const rawBuf = fs.readFileSync(p);
      if (rawBuf.length < 10) return false;
      if (rawBuf[0] !== 0x1f || rawBuf[1] !== 0x8b) return false;

      const unzipped = zlib.gunzipSync(rawBuf);
      if (!unzipped || unzipped.length < 512) return false;

      const entries = parseTarEntries(unzipped);
      const summaryBuf = entries.get('summary.json');
      if (!summaryBuf) return false;

      const summary = JSON.parse(summaryBuf.toString('utf8'));
      if (!summary || typeof summary !== 'object') return false;
      if (summary.threadId !== thread.threadId) return false;
      if (summary.status !== thread.status) return false;
      if (thread.artifactDir && summary.artifactDir && summary.artifactDir !== thread.artifactDir) return false;
      if (!Array.isArray(summary.archivedLogFiles)) return false;

      // Every file listed in summary.archivedLogFiles must exist in tar entries
      for (const logFile of summary.archivedLogFiles) {
        if (!entries.has(logFile)) return false;
      }

      // Every expected log file on disk must exist in tar entries and in summary.archivedLogFiles,
      // and its contents must match current on-disk bytes exactly
      for (const [entryName, expectedInfo] of expectedFiles) {
        const entryBuf = entries.get(entryName);
        if (!entryBuf) return false;
        if (entryBuf.length !== expectedInfo.size) return false;
        if (!summary.archivedLogFiles.includes(entryName)) return false;
        try {
          const currentDiskBytes = fs.readFileSync(expectedInfo.fullPath);
          if (!entryBuf.equals(currentDiskBytes)) return false;
        } catch {
          return false;
        }
      }

      // If accompanying .json file exists next to .tar.gz (standard archive format from archiveCompletedTaskLogs),
      // verify that its manifest also matches
      const companionJson = p.replace(/\.tar\.gz$/i, '.json').replace(/\.tgz$/i, '.json');
      if (companionJson !== p && fs.existsSync(companionJson)) {
        try {
          const companionStat = fs.statSync(companionJson);
          if (companionStat.isFile() && companionStat.size > 0) {
            const companionManifest = JSON.parse(fs.readFileSync(companionJson, 'utf8'));
            if (
              companionManifest.threadId !== thread.threadId ||
              companionManifest.status !== thread.status
            ) {
              return false;
            }
          }
        } catch {}
      }
    } catch {
      return false;
    }
  }
  return true;
}

/** Directory holding one JSON file per thread (the authoritative ledger). */
export const THREADS_DIR_NAME = 'threads';

export class TaskStore extends EventEmitter {
  private static instance: TaskStore | null = null;
  private storageDir: string;
  /** Aggregate snapshot, kept only as a derived read model. */
  private storageFile: string;
  /** Per-thread authoritative files: parallel writers touch different files. */
  private threadsDir: string;
  private tasks: Map<string, DelegationThread> = new Map();
  private readonly cleanupCommitWarnings = new Map<string, string[]>();
  /** mtime per thread file, so a sync only re-parses what actually changed. */
  private knownMtimes: Map<string, number> = new Map();
  private isWatching: boolean = false;
  private watchDebounceTimer: NodeJS.Timeout | null = null;
  private lastDirMtimeMs: number = 0;
  private readonly lockFactory?: ExecutionLockFactory;
  private readonly taskExecutionQueues = new Map<string, Promise<any>>();
  private readonly activeLocks = new Set<any>();

  constructor(customStorageDir?: string, options?: { lockFactory?: ExecutionLockFactory }) {
    super();
    this.storageDir = customStorageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.storageFile = path.join(this.storageDir, 'delegation_threads.json');
    this.threadsDir = path.join(this.storageDir, THREADS_DIR_NAME);
    this.lockFactory = options?.lockFactory;
    this.ensureStorageDir();
    this.loadFromDisk();
    this.startWatchFile();
  }

  public static getInstance(customStorageDir?: string): TaskStore {
    if (!TaskStore.instance) {
      TaskStore.instance = new TaskStore(customStorageDir);
    }
    return TaskStore.instance;
  }

  public getStoragePath(): string {
    return this.storageFile;
  }

  private ensureStorageDir(): void {
    if (!fs.existsSync(this.storageDir)) {
      try {
        fs.mkdirSync(this.storageDir, { recursive: true });
      } catch (err) {
        console.error(`[TaskStore] Failed to create storage dir: ${this.storageDir}`, err);
      }
    }
  }

  private threadFilePath(threadId: string): string {
    return path.join(this.threadsDir, `${threadId.replace(/[^A-Za-z0-9_-]/g, '_')}.json`);
  }

  private fileMtimeMs(file: string): number {
    try {
      return fs.statSync(file).mtimeMs;
    } catch {
      return 0;
    }
  }

  private ensureThreadsDir(): void {
    try {
      fs.mkdirSync(this.threadsDir, { recursive: true });
    } catch {}
  }

  /**
   * Load the ledger.
   *
   * Per-thread files are authoritative: two processes updating two different
   * tasks write two different files, so neither can overwrite the other's
   * rounds. The aggregate snapshot is never an input or migration source.
   */
  public loadFromDisk(): void {
    this.tasks.clear();
    this.knownMtimes.clear();

    this.syncFromThreads(true);
  }

  /**
   * Re-parse only the thread files whose mtime changed, and drop vanished ones.
   *
   * Returns the thread ids whose in-memory entry actually changed, so a caller
   * refreshing a tree can repaint just those rows instead of the whole view.
   */
  private syncFromThreads(force: boolean): string[] {
    this.ensureThreadsDir();
    const changed: string[] = [];
    let names: string[];
    try {
      names = fs.readdirSync(this.threadsDir).filter((n) => n.endsWith('.json'));
    } catch {
      return changed;
    }
    const seen = new Set(names);
    for (const name of names) {
      const file = path.join(this.threadsDir, name);
      const mtime = this.fileMtimeMs(file);
      if (!force && this.knownMtimes.get(name) === mtime) continue;
      const id = name.slice(0, -'.json'.length);
      const existed = this.tasks.has(id);
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (parsed && parsed.threadId && this.isTaskTombstoned(parsed.threadId)) {
          this.tasks.delete(parsed.threadId);
          this.knownMtimes.set(name, mtime);
          try { fs.unlinkSync(file); } catch {}
          if (existed) changed.push(id);
        } else if (parsed && parsed.threadId && parsed.workspace?.id && parsed.workspace?.executionPath &&
            (!parsed.worktree || parsed.worktree.path === parsed.workspace.executionPath)) {
          this.tasks.set(parsed.threadId, normalizeThread(parsed as DelegationThread));
          this.knownMtimes.set(name, mtime);
          changed.push(id);
        } else {
          this.tasks.delete(id);
          this.knownMtimes.set(name, mtime);
          if (existed) changed.push(id);
        }
      } catch {
        // Writes go through rename, so a half-written file cannot happen; a
        // corrupt one must still not take the whole ledger down.
      }
    }
    for (const name of Array.from(this.knownMtimes.keys())) {
      if (seen.has(name)) continue;
      const id = name.slice(0, -'.json'.length);
      this.tasks.delete(id);
      this.knownMtimes.delete(name);
      changed.push(id);
    }
    this.lastDirMtimeMs = this.fileMtimeMs(this.threadsDir);
    return changed;
  }

  /**
   * Ensures in-memory cache reflects any external file modifications on disk.
   *
   * Returns the thread ids that were re-read, or an empty array when nothing
   * changed on disk.
   */
  public ensureUpToDate(): string[] {
    const dirMtime = this.fileMtimeMs(this.threadsDir);
    if (dirMtime <= this.lastDirMtimeMs) return [];
    return this.syncFromThreads(false);
  }

  /** Atomic single-thread write: `rename` is the only step readers can observe. */
  private persistThread(thread: DelegationThread): void {
    if (!thread || !thread.threadId) return;
    if (this.isTaskTombstoned(thread.threadId)) {
      throw new Error(`Cannot persist task ${thread.threadId}: a durable deletion tombstone already exists.`);
    }
    this.ensureThreadsDir();
    const file = this.threadFilePath(thread.threadId);
    const temp = `${file}.tmp.${process.pid}.${Date.now()}`;
    try {
      const fd = fs.openSync(temp, 'w');
      try { fs.writeFileSync(fd, JSON.stringify(thread, null, 2), 'utf8'); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      for (let attempt = 0; ; attempt++) {
        try { fs.renameSync(temp, file); break; }
        catch (error: any) {
          // Windows readers/scanners can briefly deny replacement. Preserve the
          // old file and retry only the same atomic publish, never the mutation.
          if (process.platform !== 'win32' || !['EPERM', 'EACCES', 'EBUSY'].includes(error?.code) || attempt >= 19) throw error;
          Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
        }
      }
      if (process.platform !== 'win32') {
        const dirFd = fs.openSync(path.dirname(file), 'r');
        try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
      }
      this.knownMtimes.set(path.basename(file), this.fileMtimeMs(file));
    } catch (err) {
      try {
        if (fs.existsSync(temp)) fs.unlinkSync(temp);
      } catch {}
      console.error(`[TaskStore] Failed to persist thread ${thread.threadId}:`, err);
      throw new Error(`Could not durably persist task ${thread.threadId}: ${(err as Error)?.message || err}`, { cause: err });
    }
  }

  /**
   * Enforce the hard 1,000-task cap, oldest-updated first.
   *
   * The per-thread files must be removed together with the in-memory entry,
   * otherwise a later sync would resurrect the evicted task.
   */
  private enforceCapacity(): void {
    if (this.tasks.size <= MAX_STORED_TASKS) return;
    const sorted = Array.from(this.tasks.values()).sort((a, b) => {
      const timeA = new Date(a.updatedAt || a.createdAt).getTime();
      const timeB = new Date(b.updatedAt || b.createdAt).getTime();
      return timeA - timeB;
    });
    const evictable = sorted.filter(canEvictForCapacity);
    for (const evicted of evictable.slice(0, this.tasks.size - MAX_STORED_TASKS)) {
      this.tasks.delete(evicted.threadId);
      try {
        fs.unlinkSync(this.threadFilePath(evicted.threadId));
      } catch {}
      this.knownMtimes.delete(path.basename(this.threadFilePath(evicted.threadId)));
    }
    if (this.tasks.size > MAX_STORED_TASKS) {
      console.error(`[TaskStore] Ledger capacity reached (${MAX_STORED_TASKS}); retained all non-evictable tasks.`);
    }
  }

  public canAcceptTask(): boolean {
    this.ensureUpToDate();
    if (this.tasks.size < MAX_STORED_TASKS) return true;
    return Array.from(this.tasks.values()).some(canEvictForCapacity);
  }

  /**
   * Best-effort aggregate snapshot.
   *
   * Readers treat the per-thread files as truth, so losing a race here only
   * makes this cache one write stale; it can never lose a round.
   */
  private writeAggregate(): void {
    this.ensureStorageDir();
    const sorted = Array.from(this.tasks.values()).sort((a, b) => {
      const timeA = new Date(a.updatedAt || a.createdAt).getTime();
      const timeB = new Date(b.updatedAt || b.createdAt).getTime();
      return timeB - timeA;
    });
    const truncated = sorted.slice(0, MAX_STORED_TASKS);
    const tempFile = `${this.storageFile}.tmp.${process.pid}.${Date.now()}`;
    try {
      fs.writeFileSync(tempFile, JSON.stringify(truncated, null, 2), 'utf8');
      fs.renameSync(tempFile, this.storageFile);
    } catch (err) {
      console.error(`[TaskStore] Failed to write aggregate ${this.storageFile}:`, err);
      try {
        if (fs.existsSync(tempFile)) fs.unlinkSync(tempFile);
      } catch {}
    }
  }

  /**
   * Save all tasks to disk atomically with hard 1,000 tasks truncation.
   */
  public saveToDisk(): void {
    this.ensureUpToDate();
    this.ensureThreadsDir();
    const saved: string[] = [];
    for (const thread of this.tasks.values()) {
      if (thread.execution?.version === 2 || thread.cleanup) {
        // v2 task state is authoritative on disk under the per-task execution lock.
        // Active cleanups (v1 or v2) must not be overwritten by legacy saveToDisk.
        continue;
      }
      const file = this.threadFilePath(thread.threadId);
      if (fs.existsSync(file)) {
        try {
          const onDisk = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          if (onDisk.execution?.version === 2 || onDisk.cleanup) {
            continue;
          }
        } catch (err) {
          throw new Error(`Corrupted task file on disk for ${thread.threadId}: ${(err as Error)?.message || err}`);
        }
      }
      this.persistThread(thread);
      saved.push(thread.threadId);
    }
    this.enforceCapacity();
    this.writeAggregate();
    if (saved.length) this.emit('changed', Array.from(this.tasks.values()), saved);
  }

  /**
   * File watcher with debounce to detect external changes (e.g. MCP daemon update).
   *
   * `fs.watchFile` is a stat poller, so the interval is the background cost of
   * noticing another process's ledger write. A 30s fallback still catches a
   * missed change without paying a per-second stat while a task is running;
   * callers that need current data call {@link ensureUpToDate} directly.
   */
  private startWatchFile(): void {
    if (this.isWatching) return;
    try {
      const onChange = () => {
        if (this.watchDebounceTimer) {
          clearTimeout(this.watchDebounceTimer);
        }
        this.watchDebounceTimer = setTimeout(() => {
          const changed = this.ensureUpToDate();
          // An empty list means nothing changed on disk; do not repaint.
          if (changed.length) this.emit('changed', Array.from(this.tasks.values()), changed);
        }, 300);
        this.watchDebounceTimer.unref?.();
      };
      this.ensureThreadsDir();
      const watcher = fs.watchFile(this.threadsDir, { interval: 30_000 }, (curr, prev) => {
        if (curr.mtimeMs !== prev.mtimeMs) onChange();
      });
      (watcher as any)?.unref?.();
      this.isWatching = true;
    } catch (err) {
      console.warn(`[TaskStore] Failed to setup watchFile:`, err);
    }
  }

  public dispose(): void {
    if (this.isWatching) {
      try {
        fs.unwatchFile(this.threadsDir);
      } catch {}
      this.isWatching = false;
    }
    if (this.watchDebounceTimer) {
      clearTimeout(this.watchDebounceTimer);
      this.watchDebounceTimer = null;
    }
    for (const lock of this.activeLocks) {
      try {
        lock.dispose();
      } catch {}
    }
    this.activeLocks.clear();
  }

  /**
   * Retrieve a specific task thread by ID.
   */
  public getTask(threadId: string): DelegationThread | undefined {
    this.ensureUpToDate();
    if (this.isTaskTombstoned(threadId)) {
      return undefined;
    }
    const task = this.tasks.get(threadId);
    return task ? structuredClone(task) : undefined;
  }

  /** Read one task file directly after a cross-process CAS miss, independent of directory mtime granularity. */
  public reloadTask(threadId: string): DelegationThread | undefined {
    if (this.isTaskTombstoned(threadId)) {
      this.tasks.delete(threadId);
      return undefined;
    }
    const file = this.threadFilePath(threadId);
    if (!fs.existsSync(file)) {
      this.tasks.delete(threadId);
      return undefined;
    }
    try {
      const thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')) as DelegationThread);
      this.tasks.set(threadId, thread);
      this.knownMtimes.set(path.basename(file), this.fileMtimeMs(file));
      return structuredClone(thread);
    } catch (error: any) {
      throw new Error(`Corrupted task file on disk for ${threadId}: ${error?.message || error}`);
    }
  }

  private taskLockFilePath(threadId: string): string {
    const lockDir = path.join(this.storageDir, 'execution-locks');
    fs.mkdirSync(lockDir, { recursive: true });
    return path.join(lockDir, `${threadId.replace(/[^A-Za-z0-9_-]/g, '_')}.lock`);
  }

  /**
   * Sole gateway for acquiring the per-task execution OS lock.
   * Both Windows helpers and legacy writers contend for the same OS mutex.
   */
  private async withExecutionLock<T>(threadId: string, fn: () => T | Promise<T>): Promise<T> {
    const prev = this.taskExecutionQueues.get(threadId) || Promise.resolve();
    let releaseQueue: () => void;
    const next = new Promise<void>((resolve) => { releaseQueue = resolve; });
    this.taskExecutionQueues.set(threadId, next);

    await prev;
    try {
      const lockFile = this.taskLockFilePath(threadId);
      const lock = this.lockFactory?.(lockFile) ??
        (process.platform === 'win32' ? new WindowsTaskLock(lockFile) : new MeshFileLock(lockFile));
      this.activeLocks.add(lock);
      try {
        const release = await lock.acquire(undefined, undefined, 15_000);
        if (!release) throw new Error(`Timed out acquiring task execution lock for ${threadId}: ${lockFile}`);
        try {
          return await fn();
        } finally {
          await release();
        }
      } finally {
        this.activeLocks.delete(lock);
        lock.dispose();
      }
    } finally {
      releaseQueue!();
      if (this.taskExecutionQueues.get(threadId) === next) {
        this.taskExecutionQueues.delete(threadId);
      }
    }
  }

  /**
   * Merge authoritative round evidence from disk to prevent clobbering concurrent child reports.
   */
  private mergeAuthoritativeRounds(target: DelegationThread, diskRounds?: DelegationRound[]): void {
    if (!diskRounds || !diskRounds.length) return;
    const diskMap = new Map<number, DelegationRound>();
    for (const r of diskRounds) diskMap.set(r.round, r);
    for (const r of target.rounds) {
      const diskRound = diskMap.get(r.round);
      if (diskRound) {
        if (diskRound.output !== undefined) r.output = diskRound.output;
        if (diskRound.error !== undefined) r.error = diskRound.error;
        if (diskRound.durationMs && !r.durationMs) r.durationMs = diskRound.durationMs;
        if (diskRound.assessment && !r.assessment) r.assessment = diskRound.assessment;
        if (diskRound.touchedFiles?.length && !r.touchedFiles?.length) r.touchedFiles = diskRound.touchedFiles;
        r.tokenUsage = preferTokenUsage(r.tokenUsage, diskRound.tokenUsage, `v2-merge-round-${r.round}`);
        if (diskRound.executorAgent && !r.executorAgent) r.executorAgent = diskRound.executorAgent;
        if (diskRound.executorModel && !r.executorModel) r.executorModel = diskRound.executorModel;
        if (diskRound.nativeSessionCompactions && !r.nativeSessionCompactions) r.nativeSessionCompactions = diskRound.nativeSessionCompactions;
        if (diskRound.rawOutputFilePath && !r.rawOutputFilePath) r.rawOutputFilePath = diskRound.rawOutputFilePath;
        if (diskRound.isCompacted !== undefined && r.isCompacted === undefined) r.isCompacted = diskRound.isCompacted;
        if (diskRound.compactSummary && !r.compactSummary) r.compactSummary = diskRound.compactSummary;
        if (diskRound.outputBytes && !r.outputBytes) r.outputBytes = diskRound.outputBytes;
      }
    }
  }

  /**
   * Sole gateway for protected execution control, lifecycle, and decision transitions.
   * Runs inside a short OS file lock, reads fresh state from disk, validates expected revision,
   * applies change, increments revision, and atomically writes.
   */
  public async transitionExecution(
    threadId: string,
    expectedRevision: number,
    change: (thread: DelegationThread) => void,
    cleanupToken?: string,
    runExitProof?: { runId: string; operationId: string; generation: number },
  ): Promise<DelegationThread> {
    return this.withExecutionLock(threadId, async () => {
      if (this.isTaskTombstoned(threadId)) {
        this.tasks.delete(threadId);
        throw new Error(`Cannot transition execution for ${threadId}: task has a durable deletion tombstone.`);
      }
      this.ensureUpToDate();
      let thread = this.tasks.get(threadId);
      const file = this.threadFilePath(threadId);
      if (fs.existsSync(file)) {
        try {
          thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        } catch (err) {
          throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
        }
      }
      if (!thread) {
        throw new Error(`Task thread not found: ${threadId}`);
      }
      if (thread.cleanup && thread.cleanup.stage === 'in_progress' && cleanupToken !== thread.cleanup.token) {
        throw new Error(
          `Cannot transition execution for ${threadId}: task is reserved for cleanup (${thread.cleanup.action}) by token ${thread.cleanup.token}`,
        );
      }
      const currentRev = thread.execution?.revision ?? 0;
      if (currentRev !== expectedRevision) {
        throw new Error(
          `Execution revision mismatch for task ${threadId}: expected revision ${expectedRevision}, but found ${currentRev}`,
        );
      }
      const priorRun = thread.execution?.run;
      change(thread);
      if (cleanupToken && priorRun && !thread.execution?.run) {
        const cleanup = thread.cleanup;
        if (!runExitProof || runExitProof.runId !== priorRun.id ||
            runExitProof.operationId !== thread.execution?.operationId ||
            runExitProof.generation !== thread.execution?.generation ||
            cleanup?.runId !== priorRun.id || cleanup.operationId !== runExitProof.operationId || cleanup.generation !== runExitProof.generation) {
          throw new Error(`Cannot settle cleanup run ${priorRun.id} without matching identity and exit proof.`);
        }
        cleanup.runExitProof = { ...runExitProof, confirmedAt: new Date().toISOString() };
      }
      if (thread.execution) {
        thread.execution.revision = currentRev + 1;
      }
      thread.updatedAt = new Date().toISOString();

      ensureFinalSummary(thread);
      this.persistThread(thread);
      this.tasks.set(thread.threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
      return thread;
    });
  }

  /**
   * Acquire a cleanup reservation for a task under the task's short OS lock.
   * Sets thread.cleanup with the operation identity and returns the reservation token.
   * Blocks concurrent recovery, retry, or resume.
   * Safe for both v2 and legacy tasks; does NOT create or upgrade execution.version.
   */
  public async markTaskCleanup(
    threadId: string,
    action: 'archive' | 'delete',
    owner: ProcessIdentity,
    options?: {
      token?: string;
      force?: boolean;
      allowExisting?: boolean;
    },
  ): Promise<{ token: string; thread: DelegationThread }> {
    return this.withExecutionLock(threadId, async () => {
      this.ensureUpToDate();
      let thread = this.tasks.get(threadId);
      const file = this.threadFilePath(threadId);
      if (fs.existsSync(file)) {
        try {
          thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        } catch (err) {
          throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
        }
      }
      if (!thread) {
        throw new Error(`Task thread not found: ${threadId}`);
      }
      if (this.isTaskTombstoned(threadId)) {
        throw new Error(`Task ${threadId} has already been deleted and cannot be cleaned up`);
      }
      if (thread.cleanup) {
        if (action === 'delete' && thread.cleanup.action === 'delete' && thread.cleanup.stage === 'in_progress') {
          return { token: thread.cleanup.token, thread };
        }
        const sameOwner = !!owner.birthId && !!thread.cleanup.owner.birthId &&
          thread.cleanup.action === action && thread.cleanup.owner.pid === owner.pid &&
          thread.cleanup.owner.birthId === owner.birthId;
        if (sameOwner && thread.cleanup.stage === 'in_progress' &&
            (!options?.token || options.token === thread.cleanup.token)) {
          return { token: thread.cleanup.token, thread };
        }
        if (!(action === 'delete' && options?.allowExisting)) {
          throw new Error(
            `Task ${threadId} is already reserved for cleanup (${thread.cleanup.action}) by token ${thread.cleanup.token}`,
          );
        }
      }
      const previousCleanup = thread.cleanup && thread.cleanup.action !== action
        ? structuredClone(thread.cleanup) as unknown as Record<string, unknown>
        : thread.cleanup?.previousCleanup;
      if (action === 'archive' && !canArchiveTask(thread)) {
        throw new Error(`Task ${threadId} cannot be archived in its current status: ${thread.status}`);
      }
      if (action === 'archive' && thread.execution?.run) {
        throw new Error(`Task ${threadId} cannot be archived while execution run ${thread.execution.run.id} is ${thread.execution.run.state}`);
      }
      const token = options?.token || `clean_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
      thread.cleanup = {
        token,
        action,
        owner,
        startedAt: new Date().toISOString(),
        stage: 'in_progress',
        operationId: thread.execution?.operationId,
        generation: thread.execution?.generation,
        runId: thread.execution?.run?.id,
        ...(previousCleanup ? { previousCleanup } : {}),
      };
      if (thread.execution) {
        thread.execution.revision += 1;
      }
      thread.updatedAt = new Date().toISOString();
      this.persistThread(thread);
      this.tasks.set(thread.threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
      return { token, thread };
    });
  }

  /**
   * Finalize a cleanup operation under the task's short OS lock.
   * Verifies the cleanup token matches the reservation on disk.
   * If action is 'archive', commits the archived thread state and clears the cleanup reservation.
   * If action is 'delete', writes the tombstone and permanently unlinks the thread file.
   * Stale or mismatched cleaners are rejected.
   */
  public async commitTaskCleanup(
    threadId: string,
    token: string,
    action: 'archive' | 'delete',
    options?: {
      archiveMutation?: (thread: DelegationThread) => void;
      tarGzPaths?: string[];
      tombstone?: {
        reason?: string;
        by?: string;
        forced?: boolean;
        nativeSessions?: any;
      };
      cleanupManifest?: Record<string, unknown>;
    },
  ): Promise<boolean> {
    this.ensureUpToDate();
    let thread = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (fs.existsSync(file)) {
      try {
        thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
      } catch (err) {
        throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
      }
    }
    if (!thread) {
      if (action === 'delete') return true;
      throw new Error(`Task thread not found: ${threadId}`);
    }
    if (thread.cleanup?.token !== token) {
      throw new Error(
        `Cannot commit cleanup for ${threadId}: cleanup token mismatch (expected ${token}, found ${thread.cleanup?.token ?? 'none'})`,
      );
    }
    if (thread.cleanup?.action !== action) {
      throw new Error(
        `Cannot commit cleanup for ${threadId}: cleanup action mismatch (reserved for ${thread.cleanup?.action}, but committing ${action})`,
      );
    }

    if (action === 'archive') {
      // Validate complete durable archive files OUTSIDE the short execution lock
      if (!validateDurableArchiveFiles(options?.tarGzPaths, thread, this.storageDir)) {
        console.warn(
          `[TaskStore] Cannot commit archive for ${threadId}: archive preparation missing or invalid archive files on disk`,
        );
        return false;
      }

      const snapshotRevision = thread.execution?.revision;
      const snapshotOperationId = thread.execution?.operationId;
      const snapshotUpdatedAt = thread.updatedAt;

      return this.withExecutionLock(threadId, async () => {
        this.ensureUpToDate();
        let current = this.tasks.get(threadId);
        if (fs.existsSync(file)) {
          try {
            current = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          } catch (err) {
            throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
          }
        }
        if (!current) {
          throw new Error(`Task thread not found: ${threadId}`);
        }
        if (current.cleanup?.token !== token || current.cleanup?.action !== action) {
          console.warn(`[TaskStore] Cannot commit archive for ${threadId}: cleanup token/action mismatch under lock`);
          return false;
        }
        if (current.execution?.run) {
          throw new Error(`Cannot commit deletion for ${threadId}: execution run ${current.execution.run.id} is still recorded.`);
        }
        if (
          current.execution &&
          (current.execution.revision !== snapshotRevision ||
            current.execution.operationId !== snapshotOperationId)
        ) {
          console.warn(`[TaskStore] Cannot commit archive for ${threadId}: execution revision/operation changed concurrently`);
          return false;
        }
        if (current.updatedAt !== snapshotUpdatedAt) {
          console.warn(`[TaskStore] Cannot commit archive for ${threadId}: task modified concurrently`);
          return false;
        }

        const priorExecution = current.execution ? structuredClone(current.execution) : undefined;
        const priorStatus = current.status;
        const priorDecision = current.decision ? structuredClone(current.decision) : undefined;
        const priorDecisions = current.decisions ? structuredClone(current.decisions) : undefined;

        archiveThreadData(current);

        // Strictly protect control fields from archiveMutation
        current.status = priorStatus;
        current.decision = priorDecision;
        current.decisions = priorDecisions;
        if (!priorExecution) {
          delete current.execution;
        } else {
          current.execution = priorExecution;
          current.execution.revision += 1;
        }
        current.archivedAt = current.archivedAt || new Date().toISOString();
        delete current.cleanup;
        current.updatedAt = new Date().toISOString();
        ensureFinalSummary(current);
        this.persistThread(current);
        this.tasks.set(threadId, current);
        this.writeAggregate();
        this.emit('changed', Array.from(this.tasks.values()), [threadId]);
        return true;
      });
    }

    if (action === 'delete') {
      return this.withExecutionLock(threadId, async () => {
        const postTombstoneWarnings: string[] = [];
        this.ensureUpToDate();
        let current = this.tasks.get(threadId);
        if (fs.existsSync(file)) {
          try {
            current = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          } catch (err) {
            throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
          }
        }
        if (!current) {
          if (this.isTaskTombstoned(threadId)) return true;
          throw new Error(`Cannot commit deletion for ${threadId}: task is missing and no durable tombstone exists.`);
        }
        if (current.cleanup?.token !== token || current.cleanup?.action !== action) {
          throw new Error(
            `Cannot commit cleanup for ${threadId}: cleanup token mismatch (expected ${token}, found ${current.cleanup?.token ?? 'none'})`,
          );
        }
        // Logical deletion is the durable tombstone commit. A live or unknown
        // run remains represented in the tombstone manifest and protects its
        // resources; it must not keep the task visible in the active ledger.
        const record = {
          threadId: current.threadId,
          title: current.title,
          initiatorAgent: current.initiatorAgent,
          targetAgent: current.targetAgent,
          statusAtDeletion: current.status,
          reason: options?.tombstone?.reason || 'deleted',
          by: options?.tombstone?.by || 'system',
          ...(options?.tombstone?.forced ? { forced: true } : {}),
          at: new Date().toISOString(),
          cleanupToken: token,
          nativeSessions: options?.tombstone?.nativeSessions,
          cleanupManifest: options?.cleanupManifest || {
            token, stage: 'pending', reason: options?.tombstone?.reason || 'deleted',
            by: options?.tombstone?.by || 'system', retainResources: !!current.execution?.run,
            execution: cleanupExecutionProjection(current.execution),
            thread: cleanupThreadProjection(current),
          },
        };
        record.cleanupManifest = {
          ...(record.cleanupManifest || {}),
          token,
          execution: cleanupExecutionProjection(current.execution) || record.cleanupManifest?.execution,
          retainResources: !!current.execution?.run || record.cleanupManifest?.retainResources === true,
        };
        this.appendTombstone(record);
        this.tasks.delete(threadId);
        const taskFile = this.threadFilePath(threadId);
        if (fs.existsSync(taskFile)) {
          try { fs.unlinkSync(taskFile); }
          catch (error) { postTombstoneWarnings.push(`Task file unlink failed: ${(error as Error).message}`); }
        }
        this.knownMtimes.delete(path.basename(taskFile));
        try { this.writeAggregate(); }
        catch (error) { postTombstoneWarnings.push(`Aggregate update failed: ${(error as Error).message}`); }
        if (postTombstoneWarnings.length) {
          this.cleanupCommitWarnings.set(token, postTombstoneWarnings);
          for (const warning of postTombstoneWarnings) console.warn(`[TaskStore] ${threadId} deletion committed with residue: ${warning}`);
        }
        try { this.emit('changed', Array.from(this.tasks.values()), [threadId]); }
        catch (error) {
          postTombstoneWarnings.push(`Task notification failed: ${(error as Error).message}`);
          this.cleanupCommitWarnings.set(token, postTombstoneWarnings);
        }
        return true;
      });
    }
    return false;
  }

  public getCleanupCommitWarnings(token: string): string[] {
    return [...(this.cleanupCommitWarnings.get(token) || [])];
  }

  /** Clear a run that never reached spawn when cleanup won admission immediately after start CAS. */
  public async releaseUnspawnedRunForCleanup(threadId: string, runId: string): Promise<boolean> {
    return this.withExecutionLock(threadId, async () => {
      const file = this.threadFilePath(threadId);
      if (!fs.existsSync(file)) return false;
      const thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')) as DelegationThread);
      const run = thread.execution?.run;
      if (!thread.cleanup || !thread.cleanup.operationId || !Number.isSafeInteger(thread.cleanup.generation) ||
          run?.id !== runId || run.state !== 'starting' || run.step === 'spawning' || run.spawnIntentAt) return false;
      thread.execution!.run = undefined;
      thread.cleanup!.runExitProof = { runId, operationId: thread.cleanup!.operationId!, generation: thread.cleanup!.generation!, confirmedAt: new Date().toISOString() };
      thread.execution!.attempt = Math.max(0, thread.execution!.attempt - 1);
      thread.execution!.revision += 1;
      thread.updatedAt = new Date().toISOString();
      this.persistThread(thread);
      this.tasks.set(threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [threadId]);
      return true;
    });
  }

  /**
   * Abort an in-progress cleanup reservation if the token matches.
   * Clears thread.cleanup under the short OS lock.
   */
  public async abortTaskCleanup(threadId: string, token: string): Promise<boolean> {
    return this.withExecutionLock(threadId, async () => {
      this.ensureUpToDate();
      let thread = this.tasks.get(threadId);
      const file = this.threadFilePath(threadId);
      if (fs.existsSync(file)) {
        try {
          thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        } catch {}
      }
      if (!thread) return false;
      if (thread.cleanup?.token !== token) return false;
      delete thread.cleanup;
      if (thread.execution) {
        thread.execution.revision += 1;
      }
      thread.updatedAt = new Date().toISOString();
      this.persistThread(thread);
      this.tasks.set(threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [threadId]);
      return true;
    });
  }

  /**
   * Check whether a task meets execution eligibility under v2.
   */
  public validateExecutionEligibility(threadOrId: string | DelegationThread): ExecutionEligibilityResult {
    const thread = typeof threadOrId === 'string' ? this.getTask(threadOrId) : threadOrId;
    return validateV2ExecutionEligibility(thread);
  }

  /**
   * Check if a task has an entry in deleted_tasks.log.
   */
  public isTaskTombstoned(threadId: string): boolean {
    const file = path.join(this.storageDir, 'deleted_tasks.log');
    if (!fs.existsSync(file)) return false;
    try {
      const content = fs.readFileSync(file, 'utf8');
      return content.split(/\r?\n/).some((line) => {
        try {
          return JSON.parse(line).threadId === threadId;
        } catch {
          return false;
        }
      });
    } catch {
      return false;
    }
  }

  /**
   * Atomically append a deletion tombstone to deleted_tasks.log without racing with concurrent deletions.
   * Throws on write or fsync failure to ensure task JSON files are never deleted without durable tombstone.
   */
  public appendTombstone(record: any): void {
    const file = path.join(this.storageDir, 'deleted_tasks.log');
    const dir = path.dirname(file);
    fs.mkdirSync(dir, { recursive: true });
    const existed = fs.existsSync(file);
    const fd = fs.openSync(file, 'a');
    try {
      const line = Buffer.from(`${JSON.stringify(record)}\n`, 'utf8');
      const written = fs.writeSync(fd, line, 0, line.length);
      if (written !== line.length) throw new Error(`Short tombstone append: wrote ${written}/${line.length} bytes.`);
      fs.fsyncSync(fd);
    } finally {
      try {
        fs.closeSync(fd);
      } catch {}
    }
    if (!existed) {
      try {
        const dirFd = fs.openSync(dir, 'r');
        try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
      } catch {
        // Directory fsync is unsupported on some platforms; the file record is
        // still fsynced and append-only, so no concurrent writer can erase it.
      }
    }
  }

  /** Atomically claim the physical-cleanup phase of a durable v2 tombstone. */
  public async claimTombstoneCleanup(threadId: string, token: string, owner: ProcessIdentity): Promise<'claimed' | 'busy' | 'complete' | 'missing'> {
    const observed = this.readCleanupTombstone(threadId, token);
    const observedManifest = observed?.cleanupManifest;
    const observedOwner = observedManifest?.owner as ProcessIdentity | undefined;
    const priorExited = observedManifest?.stage === 'running' && !!observedOwner?.pid && !!observedOwner.birthId &&
      probeProcessIdentity(observedOwner.pid, observedOwner.birthId) === 'exited';
    return this.withExecutionLock(threadId, async () => {
      const latest = this.readCleanupTombstone(threadId, token);
      if (!latest?.cleanupManifest) return 'missing';
      if (latest.cleanupManifest.stage === 'complete' || latest.cleanupComplete) return 'complete';
      if (latest.cleanupManifest.stage === 'running') {
        const prior = latest.cleanupManifest.owner as ProcessIdentity | undefined;
        if (!priorExited || prior?.pid !== observedOwner?.pid || prior?.birthId !== observedOwner?.birthId) return 'busy';
      }
      this.appendTombstone({ ...latest, at: new Date().toISOString(), cleanupManifest: { ...latest.cleanupManifest, stage: 'running', owner } });
      return 'claimed';
    });
  }

  /** Mark a claimed tombstone cleanup complete or retryable after a failed attempt. */
  public async finishTombstoneCleanup(threadId: string, token: string, owner: ProcessIdentity, complete: boolean, nativeSessions?: unknown): Promise<boolean> {
    return this.withExecutionLock(threadId, async () => {
      const latest = this.readCleanupTombstone(threadId, token);
      const manifest = latest?.cleanupManifest;
      const currentOwner = manifest?.owner as ProcessIdentity | undefined;
      if (!latest || manifest?.stage !== 'running' || currentOwner?.pid !== owner.pid || currentOwner.birthId !== owner.birthId) return false;
      if (complete) {
        const taskFile = this.threadFilePath(threadId);
        if (fs.existsSync(taskFile)) {
          try { fs.unlinkSync(taskFile); }
          catch (error) {
            console.warn(`[TaskStore] Tombstoned task file unlink remains pending for ${threadId}: ${(error as Error).message}`);
            return false;
          }
        }
      }
      const durableNativeSessions = nativeSessions && typeof nativeSessions === 'object' && 'entries' in nativeSessions
        ? {
            removedFiles: Number((nativeSessions as any).removedCount) || 0,
            sessions: Array.isArray((nativeSessions as any).entries)
              ? (nativeSessions as any).entries.map((entry: any) => ({
                  agent: entry.agent, sessionId: entry.sessionId, found: entry.found,
                  removed: entry.removed || [], pruned: entry.pruned || [],
                  ...(entry.error ? { error: entry.error } : {}),
                }))
              : [],
          }
        : nativeSessions;
      this.appendTombstone({ ...latest, at: new Date().toISOString(), cleanupComplete: complete,
        nativeSessions: durableNativeSessions ?? latest.nativeSessions,
        cleanupManifest: { ...manifest, stage: complete ? 'complete' : 'pending', retainResources: complete ? false : true } });
      return true;
    });
  }

  public async recordTombstoneRunExitProof(
    threadId: string,
    token: string,
    proof: { runId: string; operationId: string; generation: number; confirmedAt: string },
  ): Promise<boolean> {
    return this.withExecutionLock(threadId, async () => {
      const latest = this.readCleanupTombstone(threadId, token);
      const manifest = latest?.cleanupManifest;
      const run = manifest?.execution?.run;
      if (!latest || !manifest || run?.id !== proof.runId ||
          manifest.execution?.operationId !== proof.operationId || manifest.execution?.generation !== proof.generation) return false;
      this.appendTombstone({ ...latest, at: new Date().toISOString(), cleanupManifest: {
        ...manifest, runExitProof: proof, retainResources: false,
      } });
      return true;
    });
  }

  public async updateTombstoneCleanupManifest(
    threadId: string,
    token: string,
    patch: Record<string, unknown>,
  ): Promise<boolean> {
    return this.withExecutionLock(threadId, async () => {
      const latest = this.readCleanupTombstone(threadId, token);
      if (!latest?.cleanupManifest || latest.cleanupManifest.stage === 'complete') return false;
      this.appendTombstone({ ...latest, at: new Date().toISOString(), cleanupManifest: {
        ...latest.cleanupManifest, ...patch,
      } });
      return true;
    });
  }

  private readCleanupTombstone(threadId: string, token: string): any | undefined {
    const file = path.join(this.storageDir, 'deleted_tasks.log');
    if (!fs.existsSync(file)) return undefined;
    try {
      return fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line))
        .filter((record) => record.threadId === threadId && record.cleanupToken === token).slice(-1)[0];
    } catch { return undefined; }
  }

  /**
   * Upsert a task thread.
   */
  public upsertTask(thread: DelegationThread): void {
    if (thread) normalizeThread(thread);
    const incomingMergeRevision = thread.mergeControl?.revision || 0;
    if (!thread.workspace?.id || !thread.workspace.executionPath ||
        (thread.worktree && thread.workspace.executionPath !== thread.worktree.path)) {
      throw new Error('Task requires a valid workspace binding matching its worktree');
    }
    this.ensureUpToDate();
    const priorMemory = this.tasks.get(thread.threadId);
    const rollbackSnapshot = priorMemory ? structuredClone(priorMemory) : undefined;
    if (this.isTaskTombstoned(thread.threadId)) {
      return;
    }
      // Merge against the on-disk copy too: another process may have appended a
      // round between our last read and this write. Per-thread files make that
      // the only shared surface left, and it is one file per task.
      let onDisk: DelegationThread | undefined;
      const file = this.threadFilePath(thread.threadId);
      if (fs.existsSync(file)) {
        try {
          onDisk = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        } catch (err) {
          throw new Error(`Corrupted task file on disk for ${thread.threadId}: ${(err as Error)?.message || err}`);
        }
      }
      if (onDisk?.execution?.version === 2) {
        // Existing v2 tasks cannot be overwritten via ordinary upsertTask.
        // Control state changes must go through transitionExecution; round evidence must go through reportRoundEvidence.
        // Refresh memory cache from authoritative on-disk state and return immediately without writing to disk.
        this.tasks.set(thread.threadId, onDisk);
        return;
      }
      if (onDisk?.cleanup) {
        // An active cleanup reservation on disk cannot be overwritten by ordinary upsertTask.
        this.tasks.set(thread.threadId, onDisk);
        return;
      }
      const inMemory = this.tasks.get(thread.threadId);
      if (!inMemory && !onDisk && !this.canAcceptTask()) {
        throw new Error('The task ledger is full and every stored task contains active, unmerged, or otherwise non-evictable work. Merge or explicitly delete retained tasks before dispatching another.');
      }
      if (onDisk?.archivedAt && !thread.archivedAt) {
        this.tasks.set(thread.threadId, onDisk);
        return;
      }
      if (onDisk && !onDisk.cleanup && inMemory?.cleanup) {
        delete inMemory.cleanup;
      }
      if ((onDisk || inMemory) && onDisk !== thread) {
        const authority = onDisk ? [onDisk] : ([inMemory].filter(Boolean) as DelegationThread[]);
        for (const prior of authority) {
          if (!thread.workspace || !prior.workspace || thread.workspace.id !== prior.workspace.id ||
              thread.workspace.rootPath !== prior.workspace.rootPath ||
              thread.workspace.environmentId !== prior.workspace.environmentId ||
              thread.workspace.canonicalRootKey !== prior.workspace.canonicalRootKey ||
              thread.workspace.nameAtDispatch !== prior.workspace.nameAtDispatch ||
              thread.workspace.source !== prior.workspace.source ||
              JSON.stringify(thread.workspace.origin) !== JSON.stringify(prior.workspace.origin)) {
            throw new Error('Task workspace binding cannot be changed');
          }
          if (prior.workspace.dispatchAccepted) thread.workspace.dispatchAccepted = true;
          if (thread.workspace.executionPath !== prior.workspace.executionPath &&
              (!thread.worktree || thread.workspace.executionPath !== thread.worktree.path ||
               !prior.worktree || fs.existsSync(prior.worktree.path) || !fs.existsSync(thread.worktree.path))) {
            throw new Error('Task execution path cannot be changed');
          }
        }
        for (const prior of authority) preferBinding(thread, prior);
        thread.taskTemplate = structuredClone(onDisk?.taskTemplate || inMemory?.taskTemplate || thread.taskTemplate);
        const mergeEvents = new Map<string, NonNullable<DelegationThread['mergeHistory']>[number]>();
        for (const source of [onDisk, inMemory, thread]) {
          for (const event of source?.mergeHistory || []) if (!mergeEvents.has(event.id)) mergeEvents.set(event.id, event);
        }
        if (mergeEvents.size) thread.mergeHistory = [...mergeEvents.values()].sort((a, b) => a.number - b.number || a.at.localeCompare(b.at));
        for (const prior of authority) {
          if (prior.cleanup?.action === 'delete') {
            return;
          }
          if (prior.cleanup) {
            thread.cleanup = structuredClone(prior.cleanup);
            thread.status = prior.status;
          }
          if (prior.mergeControl && (!thread.mergeControl || prior.mergeControl.revision > thread.mergeControl.revision)) {
            thread.mergeControl = structuredClone(prior.mergeControl);
            thread.status = prior.status;
          }
          if (prior.execution) {
            if (prior.execution.version === 2) {
              // Under v2: ordinary upsertTask CANNOT overwrite execution, status, decision(s), cleanup, failure, failureRevision, queuePhase, currentRound
              // Even if incoming revision is identical or higher!
              thread.execution = structuredClone(prior.execution);
              thread.status = prior.status;
              thread.decision = structuredClone(prior.decision);
              thread.decisions = structuredClone(prior.decisions);
              thread.cleanup = structuredClone(prior.cleanup);
              thread.failure = structuredClone(prior.failure);
              thread.failureRevision = prior.failureRevision;
              thread.queuePhase = prior.queuePhase;
              thread.currentRound = prior.currentRound;
              thread.receiverSessionId = prior.receiverSessionId;
              if (prior.receiverSessionCaptured !== undefined) {
                thread.receiverSessionCaptured = prior.receiverSessionCaptured;
              }
              if (prior.reviewerSessionCaptured !== undefined) {
                thread.reviewerSessionCaptured = prior.reviewerSessionCaptured;
              }
              thread.callerSessionId = prior.callerSessionId;
              thread.reviewerSessionId = prior.reviewerSessionId;
            } else if (!thread.execution || prior.execution.revision > thread.execution.revision) {
              // Ordinary stale upsertTask must not overwrite protected execution, lifecycle status, decisions, or failure!
              thread.execution = structuredClone(prior.execution);
              thread.status = prior.status;
              if (prior.failure) thread.failure = structuredClone(prior.failure);
              if (prior.decision) thread.decision = structuredClone(prior.decision);
              if (prior.decisions?.length) thread.decisions = structuredClone(prior.decisions);
              if (prior.cleanup) thread.cleanup = structuredClone(prior.cleanup);
            }
          }
          if (prior.decision && (!thread.decision || prior.decision.id !== thread.decision.id)) {
            if (!thread.decision || prior.status === 'wait_decision') {
              thread.decision = structuredClone(prior.decision);
            }
          }
          if (prior.decisions?.length) {
            const existingIds = new Set((thread.decisions || []).map((d) => d.id));
            const mergedDecisions = [...(thread.decisions || [])];
            for (const d of prior.decisions) {
              if (!existingIds.has(d.id)) {
                mergedDecisions.push(structuredClone(d));
                existingIds.add(d.id);
              }
            }
            thread.decisions = mergedDecisions;
          }
        }
        thread.nativeSessionCompaction = mergeNativeCursors(
          mergeNativeCursors(inMemory?.nativeSessionCompaction, onDisk?.nativeSessionCompaction, {
            developer: thread.receiverSessionId,
            reviewer: thread.reviewerSessionId,
          }),
          thread.nativeSessionCompaction,
          { developer: thread.receiverSessionId, reviewer: thread.reviewerSessionId },
        );

        const v2Prior = authority.find((entry) => entry.execution?.version === 2);
        if (v2Prior) {
          // V2 ROUND CONTROL & EVIDENCE PROTECTION
          // Existing v2 rounds are authoritative on disk/prior: ordinary upsertTask CANNOT submit unsigned round evidence, cannot append new rounds, cannot change round prompt/status/output!
          const incomingRoundsMap = new Map((thread.rounds || []).map((r) => [r.round, r]));
          thread.rounds = v2Prior.rounds.map((r) => {
            const cloned = structuredClone(r);
            const inc = incomingRoundsMap.get(r.round);
            if (inc?.touchedFiles && inc.touchedFiles.length > 0) {
              const combined = new Set([...(cloned.touchedFiles || []), ...inc.touchedFiles]);
              cloned.touchedFiles = Array.from(combined);
            }
            return cloned;
          });
          thread.currentRound = v2Prior.currentRound;
          thread.status = v2Prior.status;
        } else {
          // Merge rounds from every known copy for v1/legacy
          const priorRounds = new Map<number, DelegationRound>();
          for (const source of [inMemory, onDisk]) {
            for (const r of source?.rounds || []) {
              priorRounds.set(r.round, r);
            }
          }
          const merged: DelegationRound[] = [];
          const seen = new Set<number>();
          for (const r of thread.rounds || []) {
            merged.push(mergeRoundPreferringCompaction(priorRounds.get(r.round), r));
            seen.add(r.round);
          }
          for (const source of [inMemory, onDisk]) {
            for (const r of source?.rounds || []) {
              if (seen.has(r.round)) continue;
              merged.push(r);
              seen.add(r.round);
            }
          }
          thread.rounds = merged.sort((a, b) => a.round - b.round);
          if (thread.worktree?.integration?.phase === 'merged') { thread.status = 'completed'; thread.failure = undefined; }
          const highestKnown = Math.max(
            onDisk?.currentRound || 0,
            inMemory?.currentRound || 0,
            thread.currentRound || 0,
          );
          if (highestKnown > 0) thread.currentRound = highestKnown;
        }
        // The unified task directory is immutable once assigned: a stale writer
        // (or a policy reload) must not strip it and make this task fall back to
        // agent-wide legacy log directories.
        thread.artifactDir = thread.artifactDir || inMemory?.artifactDir || onDisk?.artifactDir;
        thread.taskDataDir = thread.taskDataDir || inMemory?.taskDataDir || onDisk?.taskDataDir;
        if (thread.artifactDir && !thread.taskDataDir) thread.taskDataDir = thread.artifactDir;
        const authoritative = [inMemory, onDisk].filter(Boolean) as DelegationThread[];
        for (const prior of authoritative) {
          if (!prior.worktree || !thread.worktree) continue;
          const priorValidation = prior.worktree.validation;
          const incomingValidation = thread.worktree.validation;
          thread.worktree = {
            ...thread.worktree,
            deliveryTarget: thread.worktree.deliveryTarget || prior.worktree.deliveryTarget,
            validation: priorValidation && (!incomingValidation || Date.parse(priorValidation.updatedAt) > Date.parse(incomingValidation.updatedAt))
              ? priorValidation : incomingValidation,
          };
          const priorPhase = prior.worktree.integration?.phase;
          const incomingPhase = thread.worktree.integration?.phase;
          if (prior.worktree.integration?.stashRecoveryPending && !thread.worktree.integration?.stashRecoveryPending &&
              !(incomingPhase === 'merged' && thread.worktree.integration?.stashRecoveryPending === false)) {
            const validation = thread.worktree.validation;
            thread.worktree = { ...thread.worktree, ...prior.worktree, validation: validation || prior.worktree.validation };
            thread.status = prior.status;
            continue;
          }
          if (incomingPhase === 'merged') {
            // Local integration is monotonic even when a stale writer carries a
            // later wall-clock timestamp (for example after clock skew).
            thread.status = 'completed';
            delete thread.failure;
            delete thread.failureRevision;
            continue;
          }
          const priorAt = new Date(prior.worktree.integration?.updatedAt || 0).getTime();
          const nextAt = new Date(thread.worktree.integration?.updatedAt || 0).getTime();
          // A cleared approval is an explicit transition, not a missing field to
          // fill from a stale snapshot. Only newer snapshots may replace it.
          if (priorPhase === 'merged' || priorAt > nextAt) {
            thread.worktree = {
              ...thread.worktree,
              ...prior.worktree,
              integration: prior.worktree.integration || thread.worktree.integration,
              headCommit: prior.worktree.headCommit || thread.worktree.headCommit,
              approvedCommit: priorAt > nextAt
                ? prior.worktree.approvedCommit
                : thread.worktree.approvedCommit,
            };
          }
          if (priorPhase === 'merged') {
            thread.status = 'completed';
            delete thread.failure;
            delete thread.failureRevision;
          } else if (priorAt > nextAt) thread.status = prior.status;
          const superseded = [...new Set([...(prior.worktree.supersededMergeResolutionPaths || []), ...(thread.worktree.supersededMergeResolutionPaths || [])])];
          if (superseded.length) thread.worktree.supersededMergeResolutionPaths = superseded;
        }
        const newestControl = authoritative.filter((entry) => (entry.mergeControl?.revision || 0) > incomingMergeRevision)
          .sort((a, b) => (b.mergeControl?.revision || 0) - (a.mergeControl?.revision || 0))[0];
        if (newestControl?.mergeControl) { thread.mergeControl = structuredClone(newestControl.mergeControl); thread.status = newestControl.status; }
        const persisted = onDisk || inMemory;
        if (!thread.execution && persisted && ((persisted.failureRevision || 0) > (thread.failureRevision || 0) ||
            ((persisted.failureRevision || 0) === (thread.failureRevision || 0) &&
             ((persisted.status !== thread.status && (persisted.status === 'failed' || thread.status === 'failed')) ||
              (persisted.status === 'failed' && JSON.stringify(persisted.failure) !== JSON.stringify(thread.failure)))))) {
          thread.failureRevision = persisted.failureRevision;
          thread.failure = persisted.failure;
          thread.status = persisted.status;
        }
        if (thread.worktree?.integration?.phase === 'merged') {
          thread.status = 'completed';
          delete thread.failure;
          delete thread.failureRevision;
        }
        if (!v2Prior) {
          const cleanupPrior = authority.find((entry) => !!entry.cleanup);
          if (cleanupPrior?.cleanup) {
            thread.cleanup = structuredClone(cleanupPrior.cleanup);
            thread.status = cleanupPrior.status;
          }
        }
      }
      thread.updatedAt = thread.updatedAt || new Date().toISOString();
      if (isMergePending(thread) && thread.worktree) {
        thread.worktree.mergePending = true;
      }
      ensureFinalSummary(thread);
      this.tasks.set(thread.threadId, thread);
      try { this.persistThread(thread); }
      catch (error) {
        if (rollbackSnapshot) this.tasks.set(thread.threadId, rollbackSnapshot);
        else this.tasks.delete(thread.threadId);
        throw error;
      }
      this.enforceCapacity();
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
  }

  /**
   * Update round inside a thread and optionally transition thread status.
   */
  public updateRound(
    threadId: string,
    roundNumber: number,
    roundUpdate: Partial<DelegationRound>,
    newThreadStatus?: ThreadStatus,
    receiverSessionId?: string,
    callerSessionId?: string,
    interruption?: TaskInterruption,
    expected?: {
      failureRevision?: number;
      roundTimestamp?: string;
      operationId?: string;
      generation?: number;
      runId?: string;
      receiverSessionCaptured?: boolean;
    },
    identity?: {
      operationId?: string;
      generation?: number;
      runId?: string;
    },
  ): DelegationThread | undefined;
  public updateRound(
    threadId: string,
    roundNumber: number,
    roundUpdate: Partial<DelegationRound>,
    newThreadStatus?: ThreadStatus,
    receiverSessionId?: string,
    callerSessionId?: string,
    interruption?: TaskInterruption,
    expected?: any,
    identity?: any,
  ): any {
    if (this.isTaskTombstoned(threadId)) return undefined;
    this.ensureUpToDate();
    let thread = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (fs.existsSync(file)) {
      try {
        thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
      } catch (err) {
        throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
      }
    }
    if (!thread) return undefined;

    if (thread.cleanup && thread.cleanup.stage === 'in_progress') {
      console.warn(`[TaskStore] Refusing to update round ${roundNumber} on ${threadId}: task is reserved for cleanup`);
      return undefined;
    }

    if (expected?.failureRevision !== undefined || expected?.roundTimestamp !== undefined) {
      const round = [...thread.rounds].reverse().find((r) => r.round === roundNumber);
      if (
        thread.archivedAt ||
        (expected.failureRevision !== undefined && (thread.failureRevision || 0) !== expected.failureRevision) ||
        thread.currentRound !== roundNumber ||
        !round ||
        (expected.roundTimestamp !== undefined && round.timestamp !== expected.roundTimestamp) ||
        (thread.status !== 'running' && thread.status !== 'paused')
      )
        return undefined;
    }

    const activeIdentity =
      identity ||
      (expected && (expected.operationId || expected.runId || expected.generation !== undefined)
        ? {
            operationId: expected.operationId,
            generation: expected.generation,
            runId: expected.runId,
          }
        : undefined);

    if (thread.execution?.version === 2) {
      // V2 STRICT IDENTITY VALIDATION
      if (
        !activeIdentity ||
        !activeIdentity.operationId ||
        typeof activeIdentity.generation !== 'number' ||
        !activeIdentity.runId
      ) {
        console.warn(
          `[TaskStore] Refusing round update for ${threadId}: missing or invalid execution identity for v2 task`,
        );
        return undefined;
      }

      // V2 ROUND NUMBER BINDING: credentials must match current execution round
      if (roundNumber !== thread.execution.round) {
        if (roundNumber >= thread.execution.round) {
          console.warn(`[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: latest round cannot be compacted`);
          return undefined;
        }
        const isCompaction = roundUpdate.isCompacted === true || roundUpdate.compactSummary !== undefined;
        if (!isCompaction) {
          console.warn(
            `[TaskStore] Refusing round update for ${threadId}: credentials for round ${thread.execution.round} cannot modify historical round ${roundNumber}`,
          );
          return undefined;
        }

        // Compaction validation (Rule 20 / Issue 3):
        const targetRound = thread.rounds.find((r) => r.round === roundNumber);
        if (!targetRound) return undefined;

        if (targetRound.status !== 'completed' && targetRound.status !== 'failed') {
          console.warn(`[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: round is not terminal (${targetRound.status})`);
          return undefined;
        }

        const triggeringRound = thread.rounds.find((r) => r.round === thread.execution!.round);
        const latestAssessedRound = [...thread.rounds]
          .reverse()
          .find((r) => r.round >= roundNumber && r.assessment?.success !== undefined);
        const triggerSuccessful =
          triggeringRound?.assessment?.success === true ||
          latestAssessedRound?.assessment?.success === true;
        if (!triggerSuccessful) {
          console.warn(
            `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: no successful trigger round assessment found`,
          );
          return undefined;
        }

        if (
          roundUpdate.status !== undefined ||
          roundUpdate.error !== undefined ||
          roundUpdate.interruption !== undefined ||
          roundUpdate.prompt !== undefined ||
          roundUpdate.type !== undefined ||
          roundUpdate.round !== undefined ||
          roundUpdate.timestamp !== undefined
        ) {
          console.warn(
            `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: compaction update contains forbidden control/evidence fields`,
          );
          return undefined;
        }

        if (!roundUpdate.rawOutputFilePath || typeof roundUpdate.rawOutputFilePath !== 'string') {
          console.warn(
            `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: rawOutputFilePath must be provided`,
          );
          return undefined;
        }
        if (!fs.existsSync(roundUpdate.rawOutputFilePath)) {
          console.warn(
            `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: rawOutputFilePath does not exist on disk`,
          );
          return undefined;
        }
        try {
          const stat = fs.statSync(roundUpdate.rawOutputFilePath);
          if (!stat.isFile() || stat.size === 0) {
            console.warn(
              `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: rawOutputFilePath is not a non-empty regular file`,
            );
            return undefined;
          }
          const rawContent = fs.readFileSync(roundUpdate.rawOutputFilePath, 'utf8');
          if (targetRound.output && rawContent !== targetRound.output && !rawContent.includes(targetRound.output)) {
            console.warn(
              `[TaskStore] Refusing compaction for ${threadId} round ${roundNumber}: rawOutputFilePath does not match original target round output`,
            );
            return undefined;
          }
        } catch {
          return undefined;
        }
      }

      // Check if there are any allowed updates to apply
      const hasAllowedUpdates =
        roundUpdate.output !== undefined ||
        roundUpdate.error !== undefined ||
        roundUpdate.durationMs !== undefined ||
        roundUpdate.status !== undefined ||
        roundUpdate.touchedFiles !== undefined ||
        roundUpdate.assessment !== undefined ||
        roundUpdate.tokenUsage !== undefined ||
        roundUpdate.executorAgent !== undefined ||
        roundUpdate.executorModel !== undefined ||
        roundUpdate.nativeSessionCompactions !== undefined ||
        roundUpdate.rawOutputFilePath !== undefined ||
        roundUpdate.compactSummary !== undefined ||
        roundUpdate.isCompacted !== undefined ||
        roundUpdate.outputBytes !== undefined ||
        interruption !== undefined ||
        Boolean(receiverSessionId && receiverSessionId !== thread.receiverSessionId) ||
        Boolean(callerSessionId && callerSessionId !== thread.callerSessionId) ||
        Boolean(expected?.receiverSessionCaptured !== undefined && expected.receiverSessionCaptured !== thread.receiverSessionCaptured);

      if (!hasAllowedUpdates && roundNumber === thread.execution.round) {
        return thread;
      }

      return this.withExecutionLock(threadId, async () => {
        this.ensureUpToDate();
        let current = this.tasks.get(threadId);
        if (fs.existsSync(file)) {
          try {
            current = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          } catch (err) {
            throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
          }
        }
        if (!current || current.execution?.version !== 2) return undefined;

        if (
          activeIdentity.operationId !== current.execution.operationId ||
          activeIdentity.generation !== current.execution.generation ||
          activeIdentity.runId !== current.execution.run?.id
        ) {
          return undefined;
        }

        if (roundNumber !== current.execution.round) {
          if (roundNumber >= current.execution.round) return undefined;
          const isCompaction = roundUpdate.isCompacted === true || roundUpdate.compactSummary !== undefined;
          if (!isCompaction) return undefined;
          const target = current.rounds.find((r) => r.round === roundNumber);
          if (!target) return undefined;
          if (target.status !== 'completed' && target.status !== 'failed') return undefined;
          const triggeringRound = current.rounds.find((r) => r.round === current.execution!.round);
          const latestAssessedRound = [...current.rounds]
            .reverse()
            .find((r) => r.round >= roundNumber && r.assessment?.success !== undefined);
          const triggerSuccessful =
            triggeringRound?.assessment?.success === true ||
            latestAssessedRound?.assessment?.success === true;
          if (!triggerSuccessful) return undefined;
          if (!roundUpdate.rawOutputFilePath || !fs.existsSync(roundUpdate.rawOutputFilePath)) return undefined;
          try {
            const stat = fs.statSync(roundUpdate.rawOutputFilePath);
            if (!stat.isFile() || stat.size === 0) return undefined;
            const rawContent = fs.readFileSync(roundUpdate.rawOutputFilePath, 'utf8');
            if (target.output && rawContent !== target.output && !rawContent.includes(target.output)) return undefined;
          } catch {
            return undefined;
          }

          if (roundUpdate.output !== undefined) target.output = roundUpdate.output;
          target.isCompacted = true;
          if (roundUpdate.compactSummary) target.compactSummary = roundUpdate.compactSummary;
          if (roundUpdate.rawOutputFilePath) target.rawOutputFilePath = roundUpdate.rawOutputFilePath;
          if (roundUpdate.outputBytes) target.outputBytes = roundUpdate.outputBytes;
          current.updatedAt = new Date().toISOString();
          ensureFinalSummary(current);
          this.persistThread(current);
          this.tasks.set(threadId, current);
          this.writeAggregate();
          this.emit('changed', Array.from(this.tasks.values()), [threadId]);
          return current;
        }

        const targetRound = current.rounds.find((r) => r.round === roundNumber);
        if (!targetRound) return undefined;

        if (targetRound.status === 'paused' && roundUpdate.status === 'running') {
          return current;
        }

        if (roundUpdate.output !== undefined && !targetRound.isCompacted) targetRound.output = roundUpdate.output;
        if (roundUpdate.error !== undefined) targetRound.error = roundUpdate.error;
        if (roundUpdate.durationMs !== undefined) targetRound.durationMs = roundUpdate.durationMs;
        if (roundUpdate.status !== undefined) targetRound.status = roundUpdate.status;
        if (roundUpdate.touchedFiles !== undefined) targetRound.touchedFiles = roundUpdate.touchedFiles;
        if (roundUpdate.assessment !== undefined) targetRound.assessment = roundUpdate.assessment;
        if (roundUpdate.tokenUsage !== undefined) {
          targetRound.tokenUsage = preferTokenUsage(targetRound.tokenUsage, roundUpdate.tokenUsage, `v2-round-${roundNumber}`);
        }
        if (roundUpdate.executorAgent !== undefined) targetRound.executorAgent = roundUpdate.executorAgent;
        if (roundUpdate.executorModel !== undefined) targetRound.executorModel = roundUpdate.executorModel;
        if (roundUpdate.nativeSessionCompactions !== undefined) targetRound.nativeSessionCompactions = roundUpdate.nativeSessionCompactions;
        if (roundUpdate.rawOutputFilePath !== undefined) targetRound.rawOutputFilePath = roundUpdate.rawOutputFilePath;
        if (roundUpdate.compactSummary !== undefined) targetRound.compactSummary = roundUpdate.compactSummary;
        if (roundUpdate.isCompacted !== undefined) targetRound.isCompacted = roundUpdate.isCompacted;
        if (roundUpdate.outputBytes !== undefined) targetRound.outputBytes = roundUpdate.outputBytes;
        if (interruption) {
          targetRound.interruption = interruption;
          appendInterruption(current, interruption);
        }

        if (receiverSessionId) {
          current.receiverSessionId = receiverSessionId;
        }
        if (expected?.receiverSessionCaptured !== undefined) {
          current.receiverSessionCaptured = expected.receiverSessionCaptured;
        } else if (receiverSessionId && receiverSessionId !== current.receiverSessionId) {
          current.receiverSessionCaptured = true;
        }
        if (callerSessionId && callerSessionId !== current.callerSessionId) {
          current.callerSessionId = callerSessionId;
        }

        const latestRound = current.rounds[current.rounds.length - 1];
        if (latestRound) {
          current.summary.lastPromptSnippet = (latestRound.prompt || '').slice(0, 100);
          if (latestRound.output) {
            current.summary.lastOutputSnippet = latestRound.output.slice(-140);
          } else if (latestRound.error) {
            current.summary.lastOutputSnippet = `[Error] ${latestRound.error.slice(0, 140)}`;
          }
        }
        current.summary.totalDurationMs = current.rounds.reduce(
          (acc, cur) => acc + (cur.durationMs || 0),
          0,
        );
        current.updatedAt = new Date().toISOString();

        ensureFinalSummary(current);
        this.persistThread(current);
        this.tasks.set(current.threadId, current);
        this.writeAggregate();
        this.emit('changed', Array.from(this.tasks.values()), [current.threadId]);
        return current;
      });
    }

      if (receiverSessionId) {
        thread.receiverSessionId = receiverSessionId;
      }
      if (expected?.receiverSessionCaptured !== undefined) {
        thread.receiverSessionCaptured = expected.receiverSessionCaptured;
      } else if (receiverSessionId && receiverSessionId !== thread.receiverSessionId) {
        thread.receiverSessionCaptured = true;
      }
      if (callerSessionId) {
        thread.callerSessionId = callerSessionId;
      }

      // Target the NEWEST entry for this round number: a ledger written before the
      // one-round-in-flight invariant may hold duplicate numbers, and the live,
      // most recently appended entry is the one that must be updated.
      let roundIndex = -1;
      for (let i = thread.rounds.length - 1; i >= 0; i--) {
        if (thread.rounds[i].round === roundNumber) {
          roundIndex = i;
          break;
        }
      }
      if (roundIndex >= 0) {
        // Live-progress syncs must never revive a round that the control plane
        // just paused. Resume explicitly mutates the round and upserts instead.
        if (
          thread.rounds[roundIndex].status === 'paused' &&
          roundUpdate.status === 'running'
        ) {
          return thread;
        }
        thread.rounds[roundIndex] = {
          ...thread.rounds[roundIndex],
          ...roundUpdate,
          ...(interruption ? { interruption } : {}),
        };
      } else if (roundUpdate.prompt) {
        // Append if not found. Refuse to append a NEW running round while another
        // round is already in flight: that would break the one-round-in-flight
        // invariant and let two agent processes drive one workspace at once.
        const wouldAddRunningRound = (roundUpdate.status || 'running') === 'running';
        if (wouldAddRunningRound && thread.rounds.some((r) => r.status === 'running')) {
          console.warn(
            `[TaskStore] Refusing to append running round ${roundNumber} to ${threadId}: ` +
              `a round is already in flight.`,
          );
          return undefined;
        }
        thread.rounds.push({
          round: roundNumber,
          type: roundUpdate.type || 'rework',
          prompt: roundUpdate.prompt || '',
          output: roundUpdate.output,
          error: roundUpdate.error,
          durationMs: roundUpdate.durationMs || 0,
          status: roundUpdate.status || 'running',
          timestamp: roundUpdate.timestamp || new Date().toISOString(),
          interruption,
          ...(roundUpdate.touchedFiles ? { touchedFiles: roundUpdate.touchedFiles } : {}),
          ...(roundUpdate.assessment ? { assessment: roundUpdate.assessment } : {}),
          ...(roundUpdate.tokenUsage ? { tokenUsage: roundUpdate.tokenUsage } : {}),
          ...(roundUpdate.executorAgent ? { executorAgent: roundUpdate.executorAgent } : {}),
          ...(roundUpdate.executorModel ? { executorModel: roundUpdate.executorModel } : {}),
          ...(roundUpdate.nativeSessionCompactions ? { nativeSessionCompactions: roundUpdate.nativeSessionCompactions } : {}),
        });
      }

      if (interruption) {
        appendInterruption(thread, interruption);
      }

      if (newThreadStatus) {
        const isLatest = !thread.currentRound || roundNumber >= thread.currentRound;
        if (isLatest) {
          if (newThreadStatus === 'failed') {
            enterTaskFailure(thread, { stage: 'execution', reason: roundUpdate.error || interruption?.reason || 'Execution failed',
              at: new Date().toISOString(), round: roundNumber, code: roundUpdate.assessment?.errorCode });
          } else {
            thread.status = newThreadStatus;
          }
        }
      }

      // Refresh summary
      const latestRound = thread.rounds[thread.rounds.length - 1];
      if (latestRound) {
        thread.summary.lastPromptSnippet = (latestRound.prompt || '').slice(0, 100);
        if (latestRound.output) {
          thread.summary.lastOutputSnippet = latestRound.output.slice(0, 150);
        } else if (latestRound.error) {
          thread.summary.lastOutputSnippet = `[Error] ${latestRound.error.slice(0, 140)}`;
        }
      }
      thread.summary.totalDurationMs = thread.rounds.reduce(
        (acc, cur) => acc + (cur.durationMs || 0),
        0,
      );
      thread.updatedAt = new Date().toISOString();

      this.persistThread(thread);
      this.tasks.set(thread.threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
      return thread;
  }

  /**
   * Field-by-field round evidence and output update with identity validation.
   * Acquires the task OS execution lock, verifies identity, binds strictly to execution.round,
   * updates evidence fields on disk and in memory atomically.
   */
  public async reportRoundEvidence(
    threadId: string,
    roundNumber: number,
    evidence: {
      output?: string;
      error?: string;
      durationMs?: number;
      status?: RoundStatus;
      touchedFiles?: string[];
      assessment?: ResponseAssessment;
      tokenUsage?: TokenUsageMetrics;
      executorAgent?: import('./types').AgentId;
      executorModel?: string;
      nativeSessionCompactions?: import('./types').NativeSessionCompactionOperation[];
      rawOutputFilePath?: string;
      compactSummary?: import('./types').CompactedContextSummary;
      isCompacted?: boolean;
      outputBytes?: number;
      interruption?: TaskInterruption;
    },
    identity?: {
      operationId?: string;
      generation?: number;
      runId?: string;
    },
  ): Promise<DelegationThread | undefined> {
    let cached = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (fs.existsSync(file)) {
      try {
        cached = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        this.tasks.set(threadId, cached);
      } catch {}
    }
    if (cached && cached.execution?.version === 2) {
      if (
        !identity?.operationId ||
        typeof identity.generation !== 'number' ||
        !identity.runId ||
        roundNumber !== cached.execution.round
      ) {
        console.warn(
          `[TaskStore] Refusing round evidence for ${threadId} round ${roundNumber}: ` +
            `caller execution credentials do not match active v2 execution`,
        );
        return undefined;
      }
    }

    return this.withExecutionLock(threadId, async () => {
      if (this.isTaskTombstoned(threadId)) {
        this.tasks.delete(threadId);
        return undefined;
      }
      this.ensureUpToDate();
      let thread = this.tasks.get(threadId);
      const file = this.threadFilePath(threadId);
      if (fs.existsSync(file)) {
        try {
          thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
        } catch (err) {
          throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
        }
      }
      if (!thread) {
        return undefined;
      }

      if (thread.execution?.version !== 2) {
        // legacy / v1
        const targetRound = thread.rounds.find((r) => r.round === roundNumber);
        if (!targetRound) return undefined;
        this.applyRoundEvidence(targetRound, evidence);
        this.persistThread(thread);
        this.tasks.set(thread.threadId, thread);
        this.writeAggregate();
        this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
        return thread;
      }

      // V2 IDENTITY VALIDATION
      if (!identity || !identity.operationId || typeof identity.generation !== 'number' || !identity.runId) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: missing complete execution identity for v2 task`);
        return undefined;
      }
      if (thread.execution.operationId !== identity.operationId) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: operationId mismatch (expected ${thread.execution.operationId}, found ${identity.operationId})`);
        return undefined;
      }
      if (thread.execution.generation !== identity.generation) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: generation mismatch (expected ${thread.execution.generation}, found ${identity.generation})`);
        return undefined;
      }
      if (thread.execution.run?.id !== identity.runId) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: runId mismatch (expected ${thread.execution.run?.id}, found ${identity.runId})`);
        return undefined;
      }

      // Strict round binding: current execution credentials can ONLY update the active execution round!
      if (roundNumber !== thread.execution.round) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: credentials for round ${thread.execution.round} cannot modify historical round ${roundNumber}`);
        return undefined;
      }

      const targetRound = thread.rounds.find((r) => r.round === roundNumber);
      if (!targetRound) {
        console.warn(`[TaskStore] Refusing round update for ${threadId}: round ${roundNumber} does not exist`);
        return undefined;
      }

      // Apply evidence fields ONLY to the target round
      this.applyRoundEvidence(targetRound, evidence);
      if (evidence.interruption) {
        targetRound.interruption = evidence.interruption;
        appendInterruption(thread, evidence.interruption);
      }
      thread.updatedAt = new Date().toISOString();

      ensureFinalSummary(thread);
      this.persistThread(thread);
      this.tasks.set(thread.threadId, thread);
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
      return thread;
    });
  }

  private applyRoundEvidence(
    targetRound: DelegationRound,
    evidence: {
      output?: string;
      error?: string;
      durationMs?: number;
      status?: RoundStatus;
      touchedFiles?: string[];
      assessment?: ResponseAssessment;
      tokenUsage?: TokenUsageMetrics;
      executorAgent?: import('./types').AgentId;
      executorModel?: string;
      nativeSessionCompactions?: import('./types').NativeSessionCompactionOperation[];
    },
  ): void {
    if (evidence.output !== undefined && !targetRound.isCompacted) {
      targetRound.output = evidence.output;
    }
    if (evidence.error !== undefined) {
      targetRound.error = evidence.error;
    }
    if (evidence.status !== undefined) {
      targetRound.status = evidence.status;
    }
    if (typeof evidence.durationMs === 'number') {
      targetRound.durationMs = evidence.durationMs;
    }
    if (evidence.touchedFiles?.length) {
      const combined = new Set([...(targetRound.touchedFiles || []), ...evidence.touchedFiles]);
      targetRound.touchedFiles = Array.from(combined);
    }
    if (evidence.tokenUsage) {
      targetRound.tokenUsage = preferTokenUsage(
        targetRound.tokenUsage,
        evidence.tokenUsage,
        `evidence-round-${targetRound.round}`,
      );
    }
    if (evidence.executorAgent) {
      targetRound.executorAgent = evidence.executorAgent;
    }
    if (evidence.executorModel) {
      targetRound.executorModel = evidence.executorModel;
    }
    if (evidence.assessment) {
      targetRound.assessment = evidence.assessment;
    }
    if (evidence.nativeSessionCompactions) {
      targetRound.nativeSessionCompactions = evidence.nativeSessionCompactions;
    }
  }

  /**
   * List tasks with pagination and optional filter.
   */
  public listTasks(limit: number = 20, statusFilter?: string, workspaceId?: string): DelegationThread[] {
    this.ensureUpToDate();
    const safeLimit = Math.min(Math.max(1, limit), MAX_STORED_TASKS);
    let all = Array.from(this.tasks.values()).sort((a, b) => {
      const timeA = new Date(a.updatedAt || a.createdAt).getTime();
      const timeB = new Date(b.updatedAt || b.createdAt).getTime();
      return timeB - timeA;
    });

    if (workspaceId) all = all.filter((task) => task.workspace.id === workspaceId);
    if (statusFilter && statusFilter !== 'all') {
      if (statusFilter === 'active') {
        all = all.filter(
          (t) =>
            t.status === 'running' ||
            t.status === 'reviewing' ||
            t.status === 'queued' ||
            t.status === 'merging' ||
            t.status === 'wait_decision' ||
            (t.status === 'failed' && t.failure?.stage === 'merge') ||
            t.status === 'paused',
        );
      } else if (statusFilter === 'review_failed' || statusFilter === 'merge_failed') {
        const stage = statusFilter === 'review_failed' ? 'review' : 'merge';
        all = all.filter(t => t.status === 'failed' && t.failure?.stage === stage);
      } else if (statusFilter === 'reworking') {
        // Compatibility alias: match tasks normalized to 'running' whose in-flight round is rework
        all = all.filter((t) => t.status === 'running' && findInFlightRound(t)?.type === 'rework');
      } else if (statusFilter === 'waiting_review') {
        all = all.filter((t) => t.status === 'reviewing');
      } else if (statusFilter === 'waiting_merge') {
        all = all.filter((t) => t.status === 'merging' || (t.status === 'queued' && t.queuePhase === 'merge') || isMergeReview(t));
      } else {
        all = all.filter((t) => t.status === statusFilter);
      }
    }

    return all.slice(0, safeLimit).map((t) => structuredClone(t));
  }

  /**
   * Record the caller's merge-request handoff for a task/batch.
   *
   * The caller agent performs the actual git/MR operations; this method only
   * persists the result so the UI and ledger can audit it.
   */
  public recordMergeRequest(
    threadId: string,
    mergeRequest: MergeRequestInfo,
  ): DelegationThread | undefined;
  public recordMergeRequest(
    threadId: string,
    mergeRequest: MergeRequestInfo,
  ): any {
    this.ensureUpToDate();
    let thread = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (!thread && fs.existsSync(file)) {
      try {
        thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
      } catch (err) {
        throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
      }
    }
    if (!thread && !fs.existsSync(file)) return undefined;

    if (thread?.execution?.version === 2) {
      return this.withExecutionLock(threadId, async () => {
        this.ensureUpToDate();
        let current = this.tasks.get(threadId);
        if (fs.existsSync(file)) {
          try {
            current = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          } catch (err) {
            throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
          }
        }
        if (!current) return undefined;
        current.mergeRequest = {
          ...current.mergeRequest,
          ...mergeRequest,
          updatedAt: mergeRequest.updatedAt || new Date().toISOString(),
        };
        current.updatedAt = new Date().toISOString();
        current.summary.lastOutputSnippet = mergeRequest.url
          ? `MR: ${mergeRequest.url}`
          : mergeRequest.commit
            ? `MR commit: ${mergeRequest.commit}`
            : `MR: ${mergeRequest.status}`;
        if (current.finalSummary) {
          if (current.archivedAt) {
            current.finalSummary = {
              ...current.finalSummary,
              mergeRequest: current.mergeRequest,
            };
          } else {
            const rebuilt = buildFinalSummary(current, current.mergeRequest);
            rebuilt.at = current.finalSummary.at;
            current.finalSummary = rebuilt;
          }
        }
        this.persistThread(current);
        this.tasks.set(current.threadId, current);
        this.writeAggregate();
        this.emit('changed', Array.from(this.tasks.values()), [current.threadId]);
        return current;
      });
    }

    if (!thread) return undefined;

    thread.mergeRequest = {
      ...thread.mergeRequest,
      ...mergeRequest,
      updatedAt: mergeRequest.updatedAt || new Date().toISOString(),
    };
    thread.updatedAt = new Date().toISOString();
    thread.summary.lastOutputSnippet = mergeRequest.url
      ? `MR: ${mergeRequest.url}`
      : mergeRequest.commit
        ? `MR commit: ${mergeRequest.commit}`
        : `MR: ${mergeRequest.status}`;
    if (thread.finalSummary) {
      if (thread.archivedAt) {
        thread.finalSummary = {
          ...thread.finalSummary,
          mergeRequest: thread.mergeRequest,
        };
      } else {
        const rebuilt = buildFinalSummary(thread, thread.mergeRequest);
        rebuilt.at = thread.finalSummary.at;
        thread.finalSummary = rebuilt;
      }
    }
    this.persistThread(thread);
    this.tasks.set(thread.threadId, thread);
    this.writeAggregate();
    this.emit('changed', Array.from(this.tasks.values()), [thread.threadId]);
    return thread;
  }

  /**
   * Archive one terminal task in place.
   *
   * Keeps the task row, model/agent metadata, MR and final digest; archives
   * task-owned histories before dropping rounds and session ids.
   */
  public archiveTask(threadId: string, cleanupToken?: string): DelegationThread | undefined {
    this.ensureUpToDate();
    let source = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (fs.existsSync(file)) {
      try {
        source = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
      } catch {}
    }
    if (source?.execution?.version === 2) {
      console.warn(
        `[TaskStore] Refusing legacy archiveTask for v2 task ${threadId}: v2 tasks must use markTaskCleanup/commitTaskCleanup`,
      );
      return undefined;
    }
    if (source?.cleanup) {
      console.warn(
        `[TaskStore] Refusing legacy archiveTask on ${threadId}: task is reserved for cleanup (${source.cleanup.action}) by ${source.cleanup.token}; must use commitTaskCleanup under OS lock`,
      );
      return undefined;
    }
    const thread = source && JSON.parse(JSON.stringify(source)) as DelegationThread;
    if (!thread || !canArchiveTask(thread)) return undefined;
    // Do not discard the source until a complete archive and archived ledger row are durable.
    const nativeLock = new NativeSessionLock(this.storageDir);
    const leases: NativeSessionLease[] = [];
    for (const { agent, sessionId } of collectThreadSessionRefs(thread)) {
      const lease = nativeLock.acquire(agent, sessionId);
      if (!lease) {
        for (const held of leases) nativeLock.release(held);
        return undefined;
      }
      leases.push(lease);
    }
    try {
      try {
        const archivedLogs = archiveCompletedTaskLogs(thread, this.storageDir);
        if (!archivedLogs.tarGzPaths.length) return undefined;
      } catch (error) {
        console.warn(`[TaskStore] Could not archive ${threadId}; retaining task data:`, error);
        return undefined;
      }
      if (!archiveThreadData(thread)) return undefined;
      delete thread.cleanup;
      if (thread.execution) {
        thread.execution.revision += 1;
      }
      this.persistThread(thread);
      this.tasks.set(threadId, thread);
      try { cleanupThreadNativeSessions(source!); } catch {}
      removeTaskArtifacts(threadId, this.storageDir, {
        keepArchive: true,
        targetAgent: thread.targetAgent,
        initiatorAgent: thread.initiatorAgent,
        artifactDir: thread.artifactDir,
      });
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()), [threadId]);
      return thread;
    } finally {
      for (const lease of leases) nativeLock.release(lease);
    }
  }

  /**
   * Permanently remove a task from the ledger.
   *
   * @returns true when an entry existed and was removed.
   */
  public deleteTask(threadId: string, cleanupToken?: string, options?: { preserveAssets?: boolean }): boolean;
  public deleteTask(threadId: string, cleanupToken?: string, options?: { preserveAssets?: boolean }): any {
    const tombstonePath = path.join(this.storageDir, 'deleted_tasks.log');
    if (fs.existsSync(tombstonePath) && fs.statSync(tombstonePath).isDirectory()) {
      throw new Error(`EISDIR: illegal operation on a directory, open '${tombstonePath}'`);
    }
    this.ensureUpToDate();
    let thread = this.tasks.get(threadId);
    const file = this.threadFilePath(threadId);
    if (!thread && fs.existsSync(file)) {
      try {
        thread = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
      } catch (err) {
        throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
      }
    }
    if (thread?.cleanup) {
      console.warn(
        `[TaskStore] Refusing legacy deleteTask on ${threadId}: task is reserved for cleanup (${thread.cleanup.action}) by ${thread.cleanup.token}; must use commitTaskCleanup under OS lock`,
      );
      return false;
    }
    if (thread?.execution?.version === 2) {
      return this.withExecutionLock(threadId, async () => {
        this.ensureUpToDate();
        let current = this.tasks.get(threadId);
        if (fs.existsSync(file)) {
          try {
            current = normalizeThread(JSON.parse(fs.readFileSync(file, 'utf8')));
          } catch (err) {
            throw new Error(`Corrupted task file on disk for ${threadId}: ${(err as Error)?.message || err}`);
          }
        }
        if (current?.cleanup && (!cleanupToken || current.cleanup.token !== cleanupToken || current.cleanup.action !== 'delete')) {
          console.warn(`[TaskStore] Refusing deleteTask on ${threadId}: task is reserved for cleanup (${current.cleanup.action}) by ${current.cleanup.token}`);
          return false;
        }
        const existed = Boolean(this.tasks.has(threadId) || fs.existsSync(file));
        if (existed) {
          this.appendTombstone({
            threadId,
            title: current?.title,
            initiatorAgent: current?.initiatorAgent,
            targetAgent: current?.targetAgent,
            statusAtDeletion: current?.status,
            reason: 'Direct task store deletion',
            by: 'system',
            at: new Date().toISOString(),
            cleanupToken: cleanupToken || current?.cleanup?.token,
            cleanupManifest: current ? {
              token: cleanupToken || current.cleanup?.token,
              stage: 'pending', retainResources: true, execution: current.execution ? {
                version: current.execution.version, revision: current.execution.revision,
                operationId: current.execution.operationId, phase: current.execution.phase,
                round: current.execution.round, purpose: current.execution.purpose,
                generation: current.execution.generation,
                run: current.execution.run,
              } : undefined,
              thread: {
                threadId: current.threadId, targetAgent: current.targetAgent,
                initiatorAgent: current.initiatorAgent, reviewFlow: current.reviewFlow,
                receiverSessionId: current.receiverSessionId, receiverSessionCaptured: current.receiverSessionCaptured,
                reviewerSessionId: current.reviewerSessionId, reviewerSessionCaptured: current.reviewerSessionCaptured,
                nativeSessionAssignments: current.nativeSessionAssignments, artifactDir: current.artifactDir,
                artifactRoot: current.artifactRoot,
                retargets: (current.retargets || []).map(({ fromAgent, toAgent, previousReceiverSessionId }) => ({ fromAgent, toAgent, previousReceiverSessionId })),
                worktree: current.worktree ? {
                  path: current.worktree.path, mainPath: current.worktree.mainPath, commonDir: current.worktree.commonDir,
                  repositoryPath: current.worktree.repositoryPath, targetRef: current.worktree.targetRef,
                  deliveryTarget: current.worktree.deliveryTarget, taskBranch: current.worktree.taskBranch,
                  headCommit: current.worktree.headCommit, approvedCommit: current.worktree.approvedCommit,
                  approvedSnapshot: current.worktree.approvedSnapshot, mergeResolutionPath: current.worktree.mergeResolutionPath,
                  supersededMergeResolutionPaths: current.worktree.supersededMergeResolutionPaths,
                  branch: current.worktree.branch, baseCommit: current.worktree.baseCommit, mergePending: current.worktree.mergePending,
                  integration: current.worktree.integration ? {
                    phase: current.worktree.integration.phase, sourceCommit: current.worktree.integration.sourceCommit,
                    targetOldCommit: current.worktree.integration.targetOldCommit, targetCommit: current.worktree.integration.targetCommit,
                    targetPath: current.worktree.integration.targetPath, candidateCommit: current.worktree.integration.candidateCommit,
                    mergedCommit: current.worktree.integration.mergedCommit, stashRecoveryPending: current.worktree.integration.stashRecoveryPending,
                    stashRecoveryLockHeld: current.worktree.integration.stashRecoveryLockHeld,
                    stashRestore: current.worktree.integration.stashRestore ? { status: current.worktree.integration.stashRestore.status, sha: current.worktree.integration.stashRestore.sha } : undefined,
                  } : undefined,
                } : undefined,
              },
            } : undefined,
          });
          this.tasks.delete(threadId);
          if (fs.existsSync(file)) {
            try { fs.unlinkSync(file); } catch (error) { console.warn(`[TaskStore] Tombstoned task file could not be unlinked for ${threadId}: ${(error as Error).message}`); }
          }
          this.knownMtimes.delete(path.basename(file));
          try { this.writeAggregate(); } catch (error) { console.warn(`[TaskStore] Tombstoned task aggregate update failed for ${threadId}: ${(error as Error).message}`); }
          try { this.emit('changed', Array.from(this.tasks.values()), [threadId]); }
          catch (error) { console.warn(`[TaskStore] Tombstoned task notification failed for ${threadId}: ${(error as Error).message}`); }
        }
        return existed;
      });
    }

    const existed = Boolean(this.tasks.has(threadId) || fs.existsSync(file));
    if (existed) {
      // Must append durable tombstone before unlinking; throws on failure
      if (!this.isTaskTombstoned(threadId)) {
        this.appendTombstone({
          threadId,
          title: thread?.title,
          initiatorAgent: thread?.initiatorAgent,
          targetAgent: thread?.targetAgent,
          statusAtDeletion: thread?.status,
          reason: 'Direct task store deletion',
          by: 'system',
          at: new Date().toISOString(),
          cleanupToken: cleanupToken || thread?.cleanup?.token,
        });
      }
      this.tasks.delete(threadId);
      if (fs.existsSync(file)) {
        try { fs.unlinkSync(file); } catch (error) { console.warn(`[TaskStore] Tombstoned task file could not be unlinked for ${threadId}: ${(error as Error).message}`); }
      }
      this.knownMtimes.delete(path.basename(file));
      if (!options?.preserveAssets) {
        try {
          removeTaskArtifacts(threadId, this.storageDir, {
            keepArchive: false, targetAgent: thread?.targetAgent,
            initiatorAgent: thread?.initiatorAgent, artifactDir: thread?.artifactDir,
          });
        } catch (error) { console.warn(`[TaskStore] Bridge artifact cleanup deferred for ${threadId}: ${(error as Error).message}`); }
      }
      try { this.writeAggregate(); } catch (error) { console.warn(`[TaskStore] Tombstoned task aggregate update failed for ${threadId}: ${(error as Error).message}`); }
      try { this.emit('changed', Array.from(this.tasks.values()), [threadId]); }
      catch (error) { console.warn(`[TaskStore] Tombstoned task notification failed for ${threadId}: ${(error as Error).message}`); }
    }
    return existed;
  }

  /**
   * Return all stored tasks in memory.
   */
  public getAllTasks(): DelegationThread[] {
    this.ensureUpToDate();
    return Array.from(this.tasks.values()).map((t) => structuredClone(t));
  }

  /** Project only the process identities needed to recognize v2 developer callers. */
  public listDeveloperRunProcesses(): { threadId: string; round: number; process: ProcessIdentity }[] {
    this.ensureUpToDate();
    const developers: { threadId: string; round: number; process: ProcessIdentity }[] = [];
    for (const { threadId, execution } of this.tasks.values()) {
      const run = execution?.run;
      if (execution?.version !== 2 || run?.role !== 'developer') continue;
      const processes = [run.child, ...(run.subtrees ?? []).flatMap(tree => [tree.root, ...tree.descendants])];
      for (const identity of processes) {
        if (identity?.birthId) developers.push({ threadId, round: execution.round, process: { ...identity } });
      }
    }
    return developers;
  }

  /**
   * Count of stored tasks.
   */
  public count(): number {
    this.ensureUpToDate();
    return this.tasks.size;
  }
}
