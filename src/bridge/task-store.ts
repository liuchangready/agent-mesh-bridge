import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { DelegationThread, DelegationRound, ThreadStatus } from './types';

export const MAX_STORED_TASKS = 1000;

export class TaskStore extends EventEmitter {
  private static instance: TaskStore | null = null;
  private storageDir: string;
  private storageFile: string;
  private tasks: Map<string, DelegationThread> = new Map();
  private isWatching: boolean = false;
  private watchDebounceTimer: NodeJS.Timeout | null = null;

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
      return;
    }

    try {
      const data = fs.readFileSync(this.storageFile, 'utf8');
      if (!data.trim()) {
        this.tasks.clear();
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
    } catch (err) {
      console.error(`[TaskStore] Failed to parse ${this.storageFile}:`, err);
    }
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
    return this.tasks.get(threadId);
  }

  /**
   * Upsert a task thread.
   */
  public upsertTask(thread: DelegationThread): void {
    thread.updatedAt = new Date().toISOString();
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
  ): DelegationThread | undefined {
    const thread = this.tasks.get(threadId);
    if (!thread) return undefined;

    const roundIndex = thread.rounds.findIndex((r) => r.round === roundNumber);
    if (roundIndex >= 0) {
      thread.rounds[roundIndex] = {
        ...thread.rounds[roundIndex],
        ...roundUpdate,
      };
    } else if (roundUpdate.prompt) {
      // Append if not found
      thread.rounds.push({
        round: roundNumber,
        type: roundUpdate.type || 'rework',
        prompt: roundUpdate.prompt || '',
        output: roundUpdate.output,
        error: roundUpdate.error,
        durationMs: roundUpdate.durationMs || 0,
        status: roundUpdate.status || 'running',
        timestamp: roundUpdate.timestamp || new Date().toISOString(),
      });
    }

    if (newThreadStatus) {
      thread.status = newThreadStatus;
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
            t.status === 'running' || t.status === 'waiting_review' || t.status === 'reworking',
        );
      } else {
        all = all.filter((t) => t.status === statusFilter);
      }
    }

    return all.slice(0, safeLimit);
  }

  /**
   * Count of stored tasks.
   */
  public count(): number {
    return this.tasks.size;
  }
}
