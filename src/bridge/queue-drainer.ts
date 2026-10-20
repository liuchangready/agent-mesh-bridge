import { ExecutionGate } from './execution-gate';
import { TaskStore } from './task-store';
import { DelegationRound, DelegationThread, findInFlightRound, findQueueHead } from './types';

export interface QueuedExecution {
  thread: DelegationThread;
  round: DelegationRound;
}

/**
 * Fallback queue pump used when no persistent `TaskSupervisor` is running.
 *
 * The VSCode extension host normally owns the queue (it polls every few
 * seconds). An agent's standalone MCP server, however, may be the only bridge
 * process alive; after it finishes a round it must pick up the next queued task
 * itself, otherwise a task queued behind the global single-flight gate would
 * wait forever.
 *
 * Every process may pump, but only one can actually execute: `ExecutionGate`
 * decides the winner and the losers simply leave the thread queued.
 */
export class QueueDrainer {
  private timer: NodeJS.Timeout | null = null;
  private timerDueAt = 0;

  constructor(
    private taskStore: TaskStore,
    private executionGate: ExecutionGate,
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
    if (this.executionGate.isBusy()) {
      // The owner may crash; keep polling so a stale lease is eventually
      // reclaimed and the queued task is not orphaned.
      this.schedule(3_000);
      return;
    }

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();
    const head = findQueueHead(allTasks);

    // No unfinished task: the mesh is idle.
    if (!head) return;

    // The oldest unfinished task owns the single global task slot. If it is
    // waiting for review or paused, no younger task may start.
    const round = findInFlightRound(head);
    if (
      !round ||
      !round.prompt ||
      head.status === 'waiting_review' ||
      head.status === 'paused'
    ) {
      this.schedule(10_000);
      return;
    }

    const next: QueuedExecution = { thread: head, round };
    try {
      await this.execute(next);
    } catch (err) {
      console.error(
        `[QueueDrainer] Failed to execute queued task ${next.thread.threadId} round ${next.round.round}:`,
        err,
      );
    }
  }

  public dispose(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.timerDueAt = 0;
  }
}
