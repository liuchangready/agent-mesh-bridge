import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import {
  DelegationThread,
  DelegationRound,
  ThreadStatus,
  TaskInterruption,
  MergeRequestInfo,
  TokenUsageMetrics,
  TokenUsageComponent,
  appendInterruption,
  isMergePending,
} from './types';
import {
  archiveCompletedTaskLogs,
  archiveThreadData,
  buildFinalSummary,
  ensureFinalSummary,
  isTerminalThreadStatus,
  removeTaskArtifacts,
} from './task-summary';
import { cleanupThreadNativeSessions } from './native-session-cleanup';
import { NativeSessionLock, NativeSessionLease } from './native-session-lock';

export const MAX_STORED_TASKS = 1000;

function canEvictForCapacity(task: DelegationThread): boolean {
  if (task.status === 'completed') {
    return !task.worktree || task.worktree.integration?.phase === 'merged' || task.worktree.integration?.phase === 'no_changes';
  }
  if (task.status !== 'failed') return false;
  const worktree = task.worktree;
  return !worktree || (
    !worktree.approvedCommit &&
    worktree.integration?.phase !== 'ready' &&
    worktree.integration?.phase !== 'waiting' &&
    worktree.integration?.phase !== 'merging' &&
    worktree.integration?.phase !== 'conflict' &&
    worktree.integration?.phase !== 'validation_failed' &&
    (!worktree.headCommit || worktree.headCommit === worktree.baseCommit)
  );
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
          ? (oldValid.recoveryDeliveredVersion === chosen.recoveryVersion || nextValid.recoveryDeliveredVersion === chosen.recoveryVersion
            ? chosen.recoveryVersion
            : undefined)
          : chosen.recoveryDeliveredVersion,
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

/** Directory holding one JSON file per thread (the authoritative ledger). */
export const THREADS_DIR_NAME = 'threads';

export class TaskStore extends EventEmitter {
  private static instance: TaskStore | null = null;
  private storageDir: string;
  /** Aggregate snapshot, kept only as a derived read model / legacy fallback. */
  private storageFile: string;
  /** Per-thread authoritative files: parallel writers touch different files. */
  private threadsDir: string;
  private tasks: Map<string, DelegationThread> = new Map();
  /** mtime per thread file, so a sync only re-parses what actually changed. */
  private knownMtimes: Map<string, number> = new Map();
  private isWatching: boolean = false;
  private watchDebounceTimer: NodeJS.Timeout | null = null;
  private lastAggMtimeMs: number = 0;
  private lastDirMtimeMs: number = 0;

  constructor(customStorageDir?: string) {
    super();
    this.storageDir = customStorageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.storageFile = path.join(this.storageDir, 'delegation_threads.json');
    this.threadsDir = path.join(this.storageDir, THREADS_DIR_NAME);
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
   * rounds. A legacy single-file ledger is migrated in place on first load.
   */
  public loadFromDisk(): void {
    this.tasks.clear();
    this.knownMtimes.clear();

    if (fs.existsSync(this.threadsDir)) {
      this.syncFromThreads(true);
      this.lastAggMtimeMs = this.fileMtimeMs(this.storageFile);
      return;
    }

    if (!fs.existsSync(this.storageFile)) {
      this.lastAggMtimeMs = 0;
      return;
    }

    try {
      const data = fs.readFileSync(this.storageFile, 'utf8');
      const rawList: DelegationThread[] = data.trim() ? JSON.parse(data) : [];
      if (Array.isArray(rawList)) {
        this.ensureThreadsDir();
        for (const thread of rawList) {
          if (!thread || !thread.threadId) continue;
          this.tasks.set(thread.threadId, thread);
          this.persistThread(thread);
        }
      }
      this.lastAggMtimeMs = this.fileMtimeMs(this.storageFile);
    } catch (err) {
      console.error(`[TaskStore] Failed to parse ${this.storageFile}:`, err);
    }
  }

  /** Re-parse only the thread files whose mtime changed, and drop vanished ones. */
  private syncFromThreads(force: boolean): void {
    this.ensureThreadsDir();
    let names: string[];
    try {
      names = fs.readdirSync(this.threadsDir).filter((n) => n.endsWith('.json'));
    } catch {
      return;
    }
    const seen = new Set(names);
    for (const name of names) {
      const file = path.join(this.threadsDir, name);
      const mtime = this.fileMtimeMs(file);
      if (!force && this.knownMtimes.get(name) === mtime) continue;
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (parsed && parsed.threadId) {
          this.tasks.set(parsed.threadId, parsed as DelegationThread);
          this.knownMtimes.set(name, mtime);
        }
      } catch {
        // Writes go through rename, so a half-written file cannot happen; a
        // corrupt one must still not take the whole ledger down.
      }
    }
    for (const name of Array.from(this.knownMtimes.keys())) {
      if (seen.has(name)) continue;
      this.tasks.delete(name.slice(0, -'.json'.length));
      this.knownMtimes.delete(name);
    }
    this.lastDirMtimeMs = this.fileMtimeMs(this.threadsDir);
  }

  /**
   * Fold writes made by a not-yet-upgraded process that only knows the
   * aggregate file, so a rolling upgrade does not lose in-flight rounds.
   */
  private mergeFromAggregate(): void {
    try {
      const data = fs.readFileSync(this.storageFile, 'utf8');
      if (!data.trim()) return;
      const list = JSON.parse(data);
      if (!Array.isArray(list)) return;
      for (const thread of list as DelegationThread[]) {
        if (!thread || !thread.threadId) continue;
        const current = this.tasks.get(thread.threadId);
        const incomingAt = new Date(thread.updatedAt || thread.createdAt || 0).getTime();
        const currentAt = current
          ? new Date(current.updatedAt || current.createdAt || 0).getTime()
          : -1;
        if (incomingAt > currentAt) {
          this.tasks.set(thread.threadId, thread);
          this.persistThread(thread);
        }
      }
    } catch {}
  }

  /**
   * Ensures in-memory cache reflects any external file modifications on disk.
   */
  public ensureUpToDate(): void {
    const dirMtime = this.fileMtimeMs(this.threadsDir);
    const aggMtime = this.fileMtimeMs(this.storageFile);
    if (dirMtime <= this.lastDirMtimeMs && aggMtime <= this.lastAggMtimeMs) return;
    this.syncFromThreads(false);
    if (aggMtime > this.lastAggMtimeMs) {
      this.mergeFromAggregate();
      this.lastAggMtimeMs = aggMtime;
    }
  }

  /** Atomic single-thread write: `rename` is the only step readers can observe. */
  private persistThread(thread: DelegationThread): void {
    if (!thread || !thread.threadId) return;
    this.ensureThreadsDir();
    const file = this.threadFilePath(thread.threadId);
    const temp = `${file}.tmp.${process.pid}.${Date.now()}`;
    try {
      const fd = fs.openSync(temp, 'w');
      try { fs.writeFileSync(fd, JSON.stringify(thread, null, 2), 'utf8'); fs.fsyncSync(fd); }
      finally { fs.closeSync(fd); }
      fs.renameSync(temp, file);
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
      this.lastAggMtimeMs = this.fileMtimeMs(this.storageFile);
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
    this.ensureThreadsDir();
    for (const thread of this.tasks.values()) {
      this.persistThread(thread);
    }
    this.enforceCapacity();
    this.writeAggregate();
    this.emit('changed', Array.from(this.tasks.values()));
  }

  /**
   * File watcher with debounce to detect external changes (e.g. MCP daemon update).
   */
  private startWatchFile(): void {
    if (this.isWatching) return;
    try {
      const onChange = () => {
        if (this.watchDebounceTimer) {
          clearTimeout(this.watchDebounceTimer);
        }
        this.watchDebounceTimer = setTimeout(() => {
          this.ensureUpToDate();
          this.emit('changed', Array.from(this.tasks.values()));
        }, 300);
      };
      // The aggregate is kept for compatibility, but the per-thread directory is
      // the real source of truth: another process can write a thread file and
      // die before refreshing the aggregate, so both must be watched.
      fs.watchFile(this.storageFile, { interval: 1000 }, (curr, prev) => {
        if (curr.mtimeMs !== prev.mtimeMs) onChange();
      });
      this.ensureThreadsDir();
      fs.watchFile(this.threadsDir, { interval: 1000 }, (curr, prev) => {
        if (curr.mtimeMs !== prev.mtimeMs) onChange();
      });
      this.isWatching = true;
    } catch (err) {
      console.warn(`[TaskStore] Failed to setup watchFile:`, err);
    }
  }

  public dispose(): void {
    if (this.isWatching) {
      try {
        fs.unwatchFile(this.storageFile);
        fs.unwatchFile(this.threadsDir);
      } catch {}
      this.isWatching = false;
    }
    if (this.watchDebounceTimer) {
      clearTimeout(this.watchDebounceTimer);
      this.watchDebounceTimer = null;
    }
  }

  /**
   * Retrieve a specific task thread by ID.
   */
  public getTask(threadId: string): DelegationThread | undefined {
    this.ensureUpToDate();
    return this.tasks.get(threadId);
  }

  /**
   * Upsert a task thread.
   */
  public upsertTask(thread: DelegationThread): void {
    this.ensureUpToDate();
    const priorMemory = this.tasks.get(thread.threadId);
    const rollbackSnapshot = priorMemory ? structuredClone(priorMemory) : undefined;
    try {
      const tombstones = fs.readFileSync(path.join(this.storageDir, 'deleted_tasks.log'), 'utf8');
      if (tombstones.split(/\r?\n/).some((line) => {
        try { return JSON.parse(line).threadId === thread.threadId; } catch { return false; }
      })) return;
    } catch {}
    // Merge against the on-disk copy too: another process may have appended a
    // round between our last read and this write. Per-thread files make that
    // the only shared surface left, and it is one file per task.
    let onDisk: DelegationThread | undefined;
    try {
      const file = this.threadFilePath(thread.threadId);
      if (fs.existsSync(file)) onDisk = JSON.parse(fs.readFileSync(file, 'utf8'));
    } catch {}
    const inMemory = this.tasks.get(thread.threadId);
    if ((onDisk || inMemory) && onDisk !== thread) {
      const authority = [inMemory, onDisk].filter(Boolean) as DelegationThread[];
      for (const prior of authority) preferBinding(thread, prior);
      thread.nativeSessionCompaction = mergeNativeCursors(
        mergeNativeCursors(inMemory?.nativeSessionCompaction, onDisk?.nativeSessionCompaction, {
          developer: thread.receiverSessionId,
          reviewer: thread.reviewerSessionId,
        }),
        thread.nativeSessionCompaction,
        { developer: thread.receiverSessionId, reviewer: thread.reviewerSessionId },
      );
      // Merge rounds from every known copy to prevent losing rounds added by
      // another process or by an earlier in-memory state. Duplicate round
      // numbers are preserved inside one copy (legacy ledgers may hold them and
      // `updateRound` targets the newest entry) — only rounds that are entirely
      // missing from the incoming copy are appended.
      const priorRounds = new Map<number, DelegationRound>();
      for (const source of [inMemory, onDisk]) {
        for (const r of source?.rounds || []) {
          // Last occurrence wins, matching `updateRound`'s "newest entry" rule.
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
      const authoritative = [inMemory, onDisk].filter(Boolean) as DelegationThread[];
      for (const prior of authoritative) {
        if (!prior.worktree || !thread.worktree) continue;
        const priorPhase = prior.worktree.integration?.phase;
        const incomingPhase = thread.worktree.integration?.phase;
        if (incomingPhase === 'merged') {
          // Local integration is monotonic even when a stale writer carries a
          // later wall-clock timestamp (for example after clock skew).
          thread.status = 'completed';
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
        if (priorPhase === 'merged') thread.status = 'completed';
        else if (priorAt > nextAt) thread.status = prior.status;
      }
      const highestKnown = Math.max(
        onDisk?.currentRound || 0,
        inMemory?.currentRound || 0,
        thread.currentRound || 0,
      );
      if (highestKnown > 0) thread.currentRound = highestKnown;
    }
    thread.updatedAt = thread.updatedAt || new Date().toISOString();
    // Merge gate: a completed task that ran in a temporary worktree and has no
    // recorded merge request keeps its output out of the main line. Every
    // completion path funnels through here, so this is the one place that has to
    // raise the flag (the inverse is cleared by `recordMergeRequest`).
    if (isMergePending(thread) && thread.worktree) {
      thread.worktree.mergePending = true;
    }
    // Terminal tasks carry a deterministic read-only digest. It is what survives
    // archival and what `get_task_summary` answers long after rounds are gone.
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
    // Same contract as the old single-file save: every mutation notifies the
    // supervisor/UI so a freshly dispatched task starts without waiting a poll.
    this.emit('changed', Array.from(this.tasks.values()));
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
  ): DelegationThread | undefined {
    this.ensureUpToDate();
    const thread = this.tasks.get(threadId);
    if (!thread) return undefined;

    if (receiverSessionId) {
      thread.receiverSessionId = receiverSessionId;
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
        // Whitelisted explicitly: this branch builds the round from scratch, so
        // anything not listed here would be silently dropped.
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
      // Only transition thread status if this round is the latest round or newer,
      // or if explicitly closing task
      const isLatest = !thread.currentRound || roundNumber >= thread.currentRound;
      if (isLatest || newThreadStatus === 'completed' || newThreadStatus === 'failed') {
        thread.status = newThreadStatus;
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

    this.upsertTask(thread);
    return thread;
  }

  /**
   * List tasks with pagination and optional filter.
   */
  public listTasks(limit: number = 20, statusFilter?: string): DelegationThread[] {
    this.ensureUpToDate();
    const safeLimit = Math.min(Math.max(1, limit), MAX_STORED_TASKS);
    let all = Array.from(this.tasks.values()).sort((a, b) => {
      const timeA = new Date(a.updatedAt || a.createdAt).getTime();
      const timeB = new Date(b.updatedAt || b.createdAt).getTime();
      return timeB - timeA;
    });

    if (statusFilter && statusFilter !== 'all') {
      if (statusFilter === 'active') {
        all = all.filter(
          (t) =>
            t.status === 'running' ||
            t.status === 'waiting_review' ||
            t.status === 'reworking' ||
            t.status === 'queued' ||
            t.status === 'paused',
        );
      } else {
        all = all.filter((t) => t.status === statusFilter);
      }
    }

    return all.slice(0, safeLimit);
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
  ): DelegationThread | undefined {
    this.ensureUpToDate();
    const thread = this.tasks.get(threadId);
    if (!thread) return undefined;

    thread.mergeRequest = {
      ...thread.mergeRequest,
      ...mergeRequest,
      updatedAt: mergeRequest.updatedAt || new Date().toISOString(),
    };
    // MR metadata is remote delivery only; local integration is tracked separately.
    thread.updatedAt = new Date().toISOString();
    thread.summary.lastOutputSnippet = mergeRequest.url
      ? `MR: ${mergeRequest.url}`
      : mergeRequest.commit
        ? `MR commit: ${mergeRequest.commit}`
        : `MR: ${mergeRequest.status}`;
    // A task may be accepted before its MR is recorded: refresh the digest so the
    // archived summary carries the MR pointer too. An ARCHIVED task has no rounds
    // left, so rebuilding would erase the digest — merge the pointer instead.
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
    this.saveToDisk();
    return thread;
  }

  /**
   * Archive one terminal task in place.
   *
   * Keeps the task row, model/agent metadata, MR and the final digest; drops the
   * rounds, session ids and per-round logs. Native CLI histories are left alone
   * (that is `delete_task`'s job).
   */
  public archiveTask(threadId: string): DelegationThread | undefined {
    this.ensureUpToDate();
    const thread = this.tasks.get(threadId);
    if (!thread || thread.archivedAt || !isTerminalThreadStatus(thread.status)) return undefined;
    const nativeLock = new NativeSessionLock(this.storageDir);
    const leases: NativeSessionLease[] = [];
    const sessions = [
      thread.receiverSessionCaptured ? [thread.targetAgent, thread.receiverSessionId] as const : undefined,
      thread.reviewFlow === 'dedicated' && thread.reviewerSessionCaptured ? [thread.initiatorAgent, thread.reviewerSessionId] as const : undefined,
    ].filter((entry): entry is readonly [DelegationThread['targetAgent'], string | undefined] => !!entry && !!entry[1]);
    for (const [agent, sessionId] of sessions) {
      const lease = nativeLock.acquire(agent, sessionId!);
      if (!lease) {
        for (const held of leases) nativeLock.release(held);
        return undefined;
      }
      leases.push(lease);
    }
    try {
    // Read the session ids BEFORE they are stripped: archival is the "keep only
    // the summary" boundary, so the native conversations and bridge logs are
    // removed now. Best-effort: a missing conversation is not an error.
    try {
      cleanupThreadNativeSessions(thread);
    } catch {}
    try {
      archiveCompletedTaskLogs(thread, this.storageDir);
    } catch {}
    if (!archiveThreadData(thread)) return undefined;
    this.tasks.set(threadId, thread);
    this.persistThread(thread);
    removeTaskArtifacts(threadId, this.storageDir, {
      keepArchive: true,
      targetAgent: thread.targetAgent,
      initiatorAgent: thread.initiatorAgent,
    });
    this.writeAggregate();
    this.emit('changed', Array.from(this.tasks.values()));
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
  public deleteTask(threadId: string): boolean {
    this.ensureUpToDate();
    const thread = this.tasks.get(threadId);
    const existed = this.tasks.delete(threadId);
    if (existed) {
      try {
        fs.unlinkSync(this.threadFilePath(threadId));
      } catch {}
      removeTaskArtifacts(threadId, this.storageDir, {
        keepArchive: false,
        targetAgent: thread?.targetAgent,
        initiatorAgent: thread?.initiatorAgent,
      });
      this.knownMtimes.delete(path.basename(this.threadFilePath(threadId)));
      this.writeAggregate();
      this.emit('changed', Array.from(this.tasks.values()));
    }
    return existed;
  }

  /**
   * Return all stored tasks in memory.
   */
  public getAllTasks(): DelegationThread[] {
    this.ensureUpToDate();
    return Array.from(this.tasks.values());
  }

  /**
   * Count of stored tasks.
   */
  public count(): number {
    this.ensureUpToDate();
    return this.tasks.size;
  }
}
