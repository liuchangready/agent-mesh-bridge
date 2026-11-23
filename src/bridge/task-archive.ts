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
import { DEFAULT_ARCHIVE_KEEP_RECENT, archiveCompletedTaskLogs, removeTaskArtifacts, selectTasksToArchive } from './task-summary';
import { getProcessBirthId } from './platform';
import * as path from 'path';
import { SessionOwnershipStore, nativeSessionNamespace } from './session-ownership';
import { collectThreadSessionRefs } from './native-session-cleanup';

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
  public async run(keepRecent?: number): Promise<string[]> {
    const keep = keepRecent ?? this.options.keepRecent ?? DEFAULT_ARCHIVE_KEEP_RECENT;
    this.taskStore.loadFromDisk();
    const selected = selectTasksToArchive(this.taskStore.getAllTasks(), keep);
    const archived: string[] = [];
    for (const thread of selected) {
      let archivedThread = false;
      if (thread.execution?.version === 2) {
        if (thread.execution.run) {
          console.warn(`[TaskArchiveService] Retaining ${thread.threadId}: execution run ${thread.execution.run.id} has not settled.`);
          continue;
        }
        const ownership = new SessionOwnershipStore(path.dirname(this.taskStore.getStoragePath()));
        const unverified = collectThreadSessionRefs(thread).find((ref) => {
          const reviewer = ref.agent === thread.initiatorAgent && ref.sessionId === thread.reviewerSessionId;
          const role = reviewer ? 'reviewer' : 'developer';
          const captured = reviewer ? thread.reviewerSessionCaptured === true : thread.receiverSessionCaptured === true;
          if (!captured) return false;
          const assignment = thread.nativeSessionAssignments?.[role];
          if (!assignment || assignment.namespace !== nativeSessionNamespace(ref.agent)) return true;
          return !ownership.verify({ agent: ref.agent, namespace: assignment.namespace, sessionId: ref.sessionId,
            threadId: thread.threadId, role, sessionEpoch: assignment.sessionEpoch }).ok;
        });
        if (unverified) {
          console.warn(`[TaskArchiveService] Retaining ${thread.threadId}: native session ownership is unverified for ${unverified.agent}/${unverified.sessionId}.`);
          continue;
        }
        let reservation: { token: string; thread: typeof thread } | undefined;
        try {
          reservation = await this.taskStore.markTaskCleanup(thread.threadId, 'archive', {
            pid: process.pid,
            birthId: getProcessBirthId(process.pid),
          });
          if (reservation.thread.execution?.run) {
            await this.taskStore.abortTaskCleanup(thread.threadId, reservation.token);
            continue;
          }
          const storageRoot = path.dirname(this.taskStore.getStoragePath());
          const files = archiveCompletedTaskLogs(reservation.thread, storageRoot).tarGzPaths;
          archivedThread = await this.taskStore.commitTaskCleanup(thread.threadId, reservation.token, 'archive', {
            tarGzPaths: files,
          });
          if (archivedThread) {
            removeTaskArtifacts(thread.threadId, storageRoot, {
              keepArchive: true,
              targetAgent: thread.targetAgent,
              initiatorAgent: thread.initiatorAgent,
              artifactDir: thread.artifactDir,
            });
          }
        } catch (error) {
          if (reservation) await this.taskStore.abortTaskCleanup(thread.threadId, reservation.token).catch(() => false);
          console.warn(`[TaskArchiveService] Could not archive ${thread.threadId}: ${(error as Error)?.message || error}`);
          continue;
        }
      } else {
        archivedThread = !!this.taskStore.archiveTask(thread.threadId);
      }
      if (!archivedThread) continue;
      archived.push(thread.threadId);
      try {
        this.options.onArchived?.(thread.threadId);
      } catch {}
    }
    return archived;
  }
}
