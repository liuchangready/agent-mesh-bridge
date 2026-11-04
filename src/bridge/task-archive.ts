/**
 * Ledger retention: keep the newest N tasks hot, archive older terminal ones.
 *
 * Archival is the lightweight half of cleanup: the summary survives, the
 * conversational payload does not. Deletion is still the explicit full purge.
 *
 * The service runs in the extension host (the only long-lived bridge process),
 * on activation, on ledger changes and on a slow timer. Standalone MCP servers
 * do not archive on their own; they always see whatever the host persisted.
 */

import { TaskStore } from './task-store';
import { DEFAULT_ARCHIVE_KEEP_RECENT, selectTasksToArchive } from './task-summary';

export interface TaskArchiveOptions {
  /** How many newest tasks stay unarchived. Defaults to 20. */
  keepRecent?: number;
  /** Broker hook for diagnostics/tests. */
  onArchived?: (threadId: string) => void;
}

export class TaskArchiveService {
  private readonly taskStore: TaskStore;
  private readonly options: TaskArchiveOptions;

  constructor(taskStore: TaskStore, options: TaskArchiveOptions = {}) {
    this.taskStore = taskStore;
    this.options = options;
  }

  /** Archive every eligible task; returns the thread ids that were archived. */
  public run(keepRecent?: number): string[] {
    const keep = keepRecent ?? this.options.keepRecent ?? DEFAULT_ARCHIVE_KEEP_RECENT;
    this.taskStore.loadFromDisk();
    const selected = selectTasksToArchive(this.taskStore.getAllTasks(), keep);
    const archived: string[] = [];
    for (const thread of selected) {
      const result = this.taskStore.archiveTask(thread.threadId);
      if (!result) continue;
      archived.push(result.threadId);
      try {
        this.options.onArchived?.(result.threadId);
      } catch {}
    }
    return archived;
  }
}
