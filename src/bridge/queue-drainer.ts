import { TaskExecutionRegistry } from './task-execution-registry';
import { TaskStore } from './task-store';
import { concurrencyPolicy } from './bridge-policy';
import { DelegationRound, DelegationThread, findInFlightRound, isReviewerBootstrapPending } from './types';

export interface QueuedExecution {
  thread: DelegationThread;
  round: DelegationRound;
}

/**
 * Fallback scheduler used when no persistent `TaskSupervisor` is running.
 * It scans every runnable round; a review/paused/bootstrap-blocked task never hides
 * a later disjoint task. TaskRoleLock is the only per-thread admission gate.
 */
export class QueueDrainer {
  private timer: NodeJS.Timeout | null = null;
  private timerDueAt = 0;

  constructor(
    private taskStore: TaskStore,
    private executionRegistry: TaskExecutionRegistry,
    private isExternalExecutorActive: () => boolean,
    private execute: (target: QueuedExecution) => Promise<void>,
  ) {}

  public schedule(delayMs: number = 1_000): void {
    if (this.isExternalExecutorActive()) return;

    const dueAt = Date.now() + Math.max(10, delayMs);
    // A finishing task asks for a fast drain (50ms) while a queued dispatch may
    // already have scheduled a slower one (1.5s). Keep the earliest deadline.
    if (this.timer && this.timerDueAt <= dueAt) return;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    this.timerDueAt = dueAt;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.timerDueAt = 0;
      void this.drain();
    }, Math.max(10, delayMs));
    this.timer.unref?.();
  }

  public async drain(): Promise<void> {
    if (this.isExternalExecutorActive()) return;
    const policy = concurrencyPolicy();
    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();
    const candidates = allTasks
      .filter((thread) => {
        if (!thread || !thread.threadId || thread.status === 'paused' || thread.status === 'waiting_review') {
          return false;
        }
        if (thread.reviewBlocked || isReviewerBootstrapPending(thread)) return false;
        return thread.status === 'queued' || thread.status === 'running' || thread.status === 'reworking';
      })
      .map((thread) => ({ thread, round: findInFlightRound(thread) }))
      .filter((target): target is QueuedExecution => Boolean(target.round?.prompt))
      .sort((a, b) => new Date(a.thread.createdAt).getTime() - new Date(b.thread.createdAt).getTime());

    let deferred = false;

    for (const target of candidates) {
      if (this.executionRegistry.getOwner(target.thread.threadId)) {
        deferred = true;
        continue;
      }
      const owners = this.executionRegistry.listOwners();
      const activeByAgent = new Map<string, number>();
      for (const owner of owners) {
        if (owner.agent) activeByAgent.set(owner.agent, (activeByAgent.get(owner.agent) || 0) + 1);
      }
      if (policy.enabled && owners.length >= policy.maxConcurrentTasks) {
        deferred = true;
        continue;
      }
      const agentCount = activeByAgent.get(target.thread.targetAgent) || 0;
      if (policy.enabled && agentCount >= policy.maxConcurrentPerAgent) {
        deferred = true;
        continue;
      }

      // execute() acquires the task's role and writes the registry record before
      // its first await. Do not await a round here: launch all admissible work.
      void this.execute(target).catch((err) => {
        console.error(
          `[QueueDrainer] Failed to execute task ${target.thread.threadId} round ${target.round.round}:`,
          err,
        );
      });
    }

    if (deferred) this.schedule(3_000);
  }

  public dispose(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.timerDueAt = 0;
  }
}
