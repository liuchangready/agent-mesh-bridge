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
  appendInterruption,
} from './types';

export const MAX_STORED_TASKS = 1000;

export class TaskStore extends EventEmitter {
  private static instance: TaskStore | null = null;
  private storageDir: string;
  private storageFile: string;
  private tasks: Map<string, DelegationThread> = new Map();
  private isWatching: boolean = false;
  private watchDebounceTimer: NodeJS.Timeout | null = null;
  private lastMtimeMs: number = 0;

  constructor(customStorageDir?: string) {
    super();
    this.storageDir = customStorageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.storageFile = path.join(this.storageDir, 'delegation_threads.json');
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

  /**
   * Load threads from disk into memory.
   */
  public loadFromDisk(): void {
    if (!fs.existsSync(this.storageFile)) {
      this.tasks.clear();
      this.lastMtimeMs = 0;
      return;
    }

    try {
      const stat = fs.statSync(this.storageFile);
      const data = fs.readFileSync(this.storageFile, 'utf8');
      if (!data.trim()) {
        this.tasks.clear();
        this.lastMtimeMs = stat.mtimeMs;
        return;
      }
      const rawList: DelegationThread[] = JSON.parse(data);
      if (Array.isArray(rawList)) {
        this.tasks.clear();
        for (const thread of rawList) {
          if (thread && thread.threadId) {
            this.tasks.set(thread.threadId, thread);
          }
        }
      }
      this.lastMtimeMs = stat.mtimeMs;
    } catch (err) {
      console.error(`[TaskStore] Failed to parse ${this.storageFile}:`, err);
    }
  }

  /**
   * Ensures in-memory cache reflects any external file modifications on disk.
   */
  public ensureUpToDate(): void {
    if (!fs.existsSync(this.storageFile)) return;
    try {
      const stat = fs.statSync(this.storageFile);
      if (stat.mtimeMs > this.lastMtimeMs) {
        this.loadFromDisk();
      }
    } catch {}
  }

  /**
   * Save all tasks to disk atomically with hard 1,000 tasks truncation.
   */
  public saveToDisk(): void {
    this.ensureStorageDir();

    // Sort by updatedAt descending
    const sorted = Array.from(this.tasks.values()).sort((a, b) => {
      const timeA = new Date(a.updatedAt || a.createdAt).getTime();
      const timeB = new Date(b.updatedAt || b.createdAt).getTime();
      return timeB - timeA;
    });

    // Enforce 1,000 tasks cap
    const truncated = sorted.slice(0, MAX_STORED_TASKS);

    // Keep memory map in sync with truncated list
    if (truncated.length < this.tasks.size) {
      this.tasks.clear();
      for (const t of truncated) {
        this.tasks.set(t.threadId, t);
      }
    }

    const tempFile = `${this.storageFile}.tmp.${Date.now()}`;
    try {
      fs.writeFileSync(tempFile, JSON.stringify(truncated, null, 2), 'utf8');
      fs.renameSync(tempFile, this.storageFile);
      try {
        this.lastMtimeMs = fs.statSync(this.storageFile).mtimeMs;
      } catch {}
      this.emit('changed', truncated);
    } catch (err) {
      console.error(`[TaskStore] Failed to write atomically to ${this.storageFile}:`, err);
      try {
        if (fs.existsSync(tempFile)) {
          fs.unlinkSync(tempFile);
        }
      } catch {}
    }
  }

  /**
   * File watcher with debounce to detect external changes (e.g. MCP daemon update).
   */
  private startWatchFile(): void {
    if (this.isWatching) return;
    try {
      fs.watchFile(this.storageFile, { interval: 1000 }, (curr, prev) => {
        if (curr.mtimeMs !== prev.mtimeMs) {
          if (this.watchDebounceTimer) {
            clearTimeout(this.watchDebounceTimer);
          }
          this.watchDebounceTimer = setTimeout(() => {
            this.loadFromDisk();
            this.emit('changed', Array.from(this.tasks.values()));
          }, 300);
        }
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
    const existing = this.tasks.get(thread.threadId);
    if (existing && existing !== thread) {
      // Merge rounds from existing to prevent losing rounds added concurrently by another process
      const roundsMap = new Map<number, DelegationRound>();
      for (const r of existing.rounds || []) {
        roundsMap.set(r.round, r);
      }
      for (const r of thread.rounds || []) {
        roundsMap.set(r.round, r);
      }
      thread.rounds = Array.from(roundsMap.values()).sort((a, b) => a.round - b.round);
      if (
        existing.currentRound &&
        (!thread.currentRound || existing.currentRound > thread.currentRound)
      ) {
        thread.currentRound = existing.currentRound;
      }
    }
    thread.updatedAt = thread.updatedAt || new Date().toISOString();
    this.tasks.set(thread.threadId, thread);
    this.saveToDisk();
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
    thread.updatedAt = new Date().toISOString();
    thread.summary.lastOutputSnippet = mergeRequest.url
      ? `MR: ${mergeRequest.url}`
      : mergeRequest.commit
        ? `MR commit: ${mergeRequest.commit}`
        : `MR: ${mergeRequest.status}`;
    this.saveToDisk();
    return thread;
  }

  /**
   * Permanently remove a task from the ledger.
   *
   * @returns true when an entry existed and was removed.
   */
  public deleteTask(threadId: string): boolean {
    this.ensureUpToDate();
    const existed = this.tasks.delete(threadId);
    if (existed) {
      this.saveToDisk();
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
