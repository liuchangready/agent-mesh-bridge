import { ExecutionGate, normalizeWorkspaceKey } from './execution-gate';
import { TaskStore } from './task-store';
import { concurrencyPolicy } from './bridge-policy';
import { DelegationRound, DelegationThread, findInFlightRound, findQueueHead } from './types';

export interface QueuedExecution {
  thread: DelegationThread;
  round: DelegationRound;
}

/**
 * One queue per workspace.
 *
 * Concurrency slots are per workspace, so the queue head must be chosen per
 * workspace too: a blocked head (waiting_review/paused) only blocks the tasks
 * that share its workspace. With concurrency off the caller uses the historical
 * single global queue instead, so grouping is never consulted there.
 */
export function groupTasksByWorkspace(
  tasks: DelegationThread[],
): Map<string, DelegationThread[]> {
  const groups = new Map<string, DelegationThread[]>();
  for (const task of tasks) {
    if (!task || !task.threadId) continue;
    // An empty path (legacy ledger) groups with other pathless tasks, so they
    // still serialize instead of each claiming its own slot.
    const key = normalizeWorkspaceKey(task.workspacePath);
    const group = groups.get(key);
    if (group) group.push(task);
    else groups.set(key, [task]);
  }
  return groups;
}

/**
 * Fallback queue pump used when no persistent `TaskSupervisor` is running.
 *
 * The VSCode extension host normally owns the queue (it polls every few
 * seconds). An agent's standalone MCP server, however, may be the only bridge
 * process alive; after it finishes a round it must pick up the next queued task
 * itself, otherwise a task queued behind the execution gate would wait forever.
 *
 * Every process may pump, but only one can actually execute a given slot:
 * `ExecutionGate` decides the winner and the losers simply leave the thread
 * queued. With concurrency enabled the pump starts one task per workspace, up to
 * the configured mesh-wide limit; with it disabled it starts exactly one.
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

    const policy = concurrencyPolicy();
    if (!policy.enabled) {
      // Strict single-flight: byte-for-byte the historical behaviour.
      await this.drainSerial();
      return;
    }
    await this.drainConcurrent(policy.maxConcurrentTasks);
  }

  /** One global slot, one global queue head: the pre-concurrency pump. */
  private async drainSerial(): Promise<void> {
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

  /**
   * Bounded concurrency: start the oldest startable head of each workspace, in
   * `createdAt` order, until the mesh-wide slot count is reached.
   */
  private async drainConcurrent(maxConcurrentTasks: number): Promise<void> {
    if (this.executionGate.countActive() >= maxConcurrentTasks) {
      // Every slot is taken; the owner releases it and reschedules this pump.
      this.schedule(3_000);
      return;
    }

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();
    const groups = groupTasksByWorkspace(allTasks);

    const starts: QueuedExecution[] = [];
    let blocked = false;
    for (const group of groups.values()) {
      const head = findQueueHead(group);
      if (!head) continue;
      const workspaceKey = normalizeWorkspaceKey(head.workspacePath);
      // This workspace is already executing (here or in another process). The
      // lease is held for the whole round, so re-entering it would let the pump
      // fight the running round for its own role lock.
      if (this.executionGate.getOwner(workspaceKey)) continue;
      const round = findInFlightRound(head);
      if (
        !round ||
        !round.prompt ||
        head.status === 'waiting_review' ||
        head.status === 'paused'
      ) {
        // Only the tasks of this workspace wait behind it.
        blocked = true;
        continue;
      }
      starts.push({ thread: head, round });
    }

    if (starts.length === 0) {
      if (blocked) this.schedule(10_000);
      return;
    }

    starts.sort(
      (a, b) => new Date(a.thread.createdAt).getTime() - new Date(b.thread.createdAt).getTime(),
    );

    for (const target of starts) {
      if (this.executionGate.countActive() >= maxConcurrentTasks) break;
      // Never await here: awaiting one round would serialize exactly the tasks
      // concurrency exists to overlap. Each executor reschedules this pump when
      // it finishes; a lost race simply leaves the thread queued.
      void this.execute(target).catch((err) => {
        console.error(
          `[QueueDrainer] Failed to execute queued task ${target.thread.threadId} round ${target.round.round}:`,
          err,
        );
      });
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
