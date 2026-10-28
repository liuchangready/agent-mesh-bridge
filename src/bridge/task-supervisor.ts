import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { TaskStore } from './task-store';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { AgentDetector } from './agent-detector';
import {
  DelegationThread,
  DelegationRound,
  ThreadStatus,
  findInFlightRound,
  findQueueHead,
  getInFlightRounds,
  isUserInterruption,
  shouldResumeNativeSession,
} from './types';
import {
  findFileConflicts,
  findScopeConflicts,
  describeScopeConflict,
  ScopeConflict,
} from './file-scope';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { ExecutionGate, ExecutionOwner, normalizeWorkspaceKey } from './execution-gate';
import { ConcurrencyPolicy, concurrencyPolicy } from './bridge-policy';
import { QueueDrainer, QueuedExecution, groupTasksByWorkspace } from './queue-drainer';
import { TaskRoleLock } from './task-role-lock';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';
import {
  findLatestExecutionLog,
  readTextFilePaged,
  roundExecutionLogPath,
} from './round-log';

/** `get_recent_logs` reads a bounded tail, not a whole multi-megabyte log. */
const RECENT_LOG_EXCERPT_BYTES = 512 * 1024;

function readLogExcerpt(filePath: string): string {
  try {
    const stat = fs.statSync(filePath);
    const startOffset = Math.max(0, stat.size - RECENT_LOG_EXCERPT_BYTES);
    return readTextFilePaged(filePath, {
      offsetBytes: startOffset,
      maxBytes: RECENT_LOG_EXCERPT_BYTES,
    }).content;
  } catch {
    return '';
  }
}

export class TaskSupervisor extends EventEmitter {
  private static instance: TaskSupervisor | undefined;
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private activeTasks: Map<string, Promise<void>> = new Map();
  private liveLogs: Map<string, string> = new Map();
  private lockFilePath: string;
  private logDir: string;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private pollInterval: NodeJS.Timeout | null = null;
  private isDisposed: boolean = false;
  private streamSyncIntervalMs: number = 30000;
  private executionGate: ExecutionGate;
  /**
   * Re-entrancy guard: `taskStore.upsertTask()` emits `changed` synchronously,
   * which calls back into `checkPendingTasks()`. Without this flag that nested
   * pass can try to start the very same task that the outer pass is starting.
   */
  private startingThreadId: string | null = null;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    executionGate?: ExecutionGate,
  ) {
    super();
    TaskSupervisor.instance = this;
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.executionGate = executionGate || new ExecutionGate();

    const storageDir = path.join(os.homedir(), '.agent-mesh-bridge');
    if (!fs.existsSync(storageDir)) {
      try {
        fs.mkdirSync(storageDir, { recursive: true });
      } catch {}
    }
    this.lockFilePath = path.join(storageDir, 'supervisor.json');

    this.logDir = path.join(storageDir, 'logs');
    if (!fs.existsSync(this.logDir)) {
      try {
        fs.mkdirSync(this.logDir, { recursive: true });
      } catch {}
    }

    this.startHeartbeat();
    this.startWatching();
  }

  public static getInstance(): TaskSupervisor | undefined {
    return TaskSupervisor.instance;
  }

  public setStreamSyncIntervalMs(ms: number): void {
    this.streamSyncIntervalMs = ms;
  }

  /**
   * Checks if an external supervisor instance (e.g. VS Code extension host) is actively running.
   */
  public static isSupervisorActive(): boolean {
    const lockFile = path.join(os.homedir(), '.agent-mesh-bridge', 'supervisor.json');
    if (!fs.existsSync(lockFile)) return false;

    try {
      const data = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
      if (!data || !data.updatedAt || !data.pid) return false;

      // Check if heartbeat is fresher than 20 seconds
      const isFresh = Date.now() - data.updatedAt < 20000;
      if (!isFresh) return false;

      // Check if process with that pid is alive
      try {
        process.kill(data.pid, 0);
        return true;
      } catch {
        return false;
      }
    } catch {
      return false;
    }
  }

  private startHeartbeat(): void {
    const updateLock = () => {
      if (this.isDisposed) return;
      try {
        fs.writeFileSync(
          this.lockFilePath,
          JSON.stringify({ pid: process.pid, updatedAt: Date.now() }, null, 2),
          'utf8',
        );
      } catch {}
    };

    updateLock();
    this.heartbeatInterval = setInterval(updateLock, 5000);
  }

  private startWatching(): void {
    // Listen to TaskStore changes
    this.taskStore.on('changed', () => {
      this.checkPendingTasks();
    });

    // Periodic watchdog poll (every 3s) to ensure fast autonomous reaction
    this.pollInterval = setInterval(() => {
      this.checkPendingTasks();
    }, 3000);

    // Initial check on startup
    setTimeout(() => {
      this.checkPendingTasks();
    }, 1000);
  }

  /**
   * Enforce the one-round-in-flight invariant on persisted state.
   *
   * Ledgers written before this invariant existed can hold several `running`
   * rounds at once (and two entries sharing a round number). The highest
   * numbered running round is the live one; every other `running` entry is
   * finalized as failed so it can never be launched alongside it — which is what
   * made two agent processes drive one workspace concurrently.
   *
   * @param thread - thread loaded from the store, mutated in place when repaired.
   * @returns whether the ledger was modified.
   */
  public normalizeRoundIntegrity(thread: DelegationThread): boolean {
    const running = getInFlightRounds(thread);
    if (running.length <= 1) return false;

    const live = findInFlightRound(thread);
    if (!live) return false;

    const finalized: number[] = [];
    for (const round of thread.rounds) {
      if (round === live || round.status !== 'running') continue;
      round.status = 'failed';
      round.error =
        `Finalized by supervisor: round ${live.round} of this task was already in flight, ` +
        `and a task may only have one round running at a time.`;
      finalized.push(round.round);
    }

    if (finalized.length === 0) return false;

    this.taskStore.upsertTask(thread);
    console.warn(
      `[TaskSupervisor] Repaired ${finalized.length} overlapping running round(s) in ` +
        `${thread.threadId}: kept round ${live.round}, finalized [${finalized.join(', ')}].`,
    );
    return true;
  }

  /**
   * Queue pump: decides which task round may execute next.
   *
   * Concurrency is opt-in. With it disabled this is the historical strict FIFO
   * pump: exactly one round may run mesh-wide, so it starts at most one thread
   * and marks every other candidate queued. With it enabled the mesh is
   * partitioned by workspace and up to `maxConcurrentTasks` workspaces may run
   * at once (see `scheduleConcurrent`); the per-workspace lease inside
   * `executeThreadRound` is still the final authority.
   */
  public checkPendingTasks(): void {
    if (this.isDisposed || this.startingThreadId) return;

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();

    // If an active task was deleted from disk while executing, proactively abort its runner.
    // If it was marked paused, stop the child process to preserve the round.
    for (const activeThreadId of Array.from(this.activeTasks.keys())) {
      const live = this.taskStore.getTask(activeThreadId);
      if (!live) {
        console.warn(
          `[TaskSupervisor] Active task ${activeThreadId} was deleted from store; aborting runner.`,
        );
        this.runner.cancelThread(activeThreadId, 'delete', 'Task was deleted from store', 'supervisor');
      } else if (live.status === 'paused') {
        this.runner.cancelThread(
          activeThreadId,
          'pause',
          live.interruption?.reason || 'Task was paused in store',
          'supervisor',
        );
      }
    }

    // Orphaned execution leases: a lock whose task is gone must not hold a slot.
    // Every lease is scanned (the legacy mesh-wide lock and each per-workspace
    // one), so a deleted task cannot block one workspace forever.
    for (const lockOwner of this.executionGate.listOwners()) {
      if (this.taskStore.getTask(lockOwner.threadId)) continue;
      console.warn(
        `[TaskSupervisor] Lock owner task ${lockOwner.threadId} no longer exists in store; clearing orphaned gate.`,
      );
      this.executionGate.killOrphanChild(lockOwner.threadId, true);
      if (lockOwner.pid === process.pid && !this.activeTasks.has(lockOwner.threadId)) {
        // Release exactly this lease: with concurrency on, releasing every lease
        // here would hand another, perfectly healthy task's slot away.
        this.executionGate.release({
          token: lockOwner.token,
          threadId: lockOwner.threadId,
          round: lockOwner.round,
          lockPath: lockOwner.lockPath || this.executionGate.getLockFilePath(),
          workspace: lockOwner.workspace,
        });
      }
    }

    const policy = concurrencyPolicy();
    if (policy.enabled) {
      this.scheduleConcurrent(allTasks, policy);
      return;
    }
    this.scheduleSerial(allTasks);
  }

  /**
   * Legacy, strict single-flight pump.
   *
   * Only ONE delegated round may execute across the whole mesh. This method
   * therefore either starts exactly one thread and returns, or (when something
   * is already in flight) marks every other candidate as queued. It never
   * launches two rounds in one pass. This is the behaviour whenever concurrency
   * is not explicitly enabled.
   */
  private scheduleSerial(allTasks: DelegationThread[]): void {
    // Task-level queue head: the oldest unfinished task owns the single global
    // slot. A waiting_review/paused head blocks every younger task until it is
    // completed (or deleted/retried).
    const queueHead = findQueueHead(allTasks);
    let queueHeadBlockReason: string | undefined;
    if (queueHead) {
      if (this.activeTasks.has(queueHead.threadId)) {
        queueHeadBlockReason = `Queued: Waiting for task ${queueHead.threadId} to finish; tasks execute one whole task at a time.`;
      } else {
        const headRound = findInFlightRound(queueHead);
        if (
          !headRound ||
          queueHead.status === 'waiting_review' ||
          queueHead.status === 'paused'
        ) {
          queueHeadBlockReason =
            `Queued: Waiting for task ${queueHead.threadId} (${queueHead.status}) to complete ` +
            `before another task runs; tasks execute one whole task at a time.`;
        }
      }
    }

    // Candidate tasks that need execution:
    // status is running, reworking, or queued, OR has running rounds.
    let candidateTasks = allTasks
      .filter((thread) => {
        if (this.activeTasks.has(thread.threadId)) {
          return false;
        }
        // A paused task must only restart through the explicit resume path.
        if (thread.status === 'paused') {
          return false;
        }
        const hasRunningRound = (thread.rounds || []).some(
          (r: DelegationRound) => r.status === 'running',
        );
        return (
          thread.status === 'running' ||
          thread.status === 'reworking' ||
          thread.status === 'queued' ||
          hasRunningRound
        );
      })
      // Sort candidate tasks by createdAt ascending (FIFO queue order)
      .sort((a, b) => {
        const timeA = new Date(a.createdAt).getTime();
        const timeB = new Date(b.createdAt).getTime();
        return timeA - timeB;
      });

    // Heal ledgers written before the one-round-in-flight invariant existed.
    for (const thread of candidateTasks) {
      this.normalizeRoundIntegrity(thread);
    }

    // A waiting_review/paused head (or an active head elsewhere) means no
    // younger task may start.
    if (queueHeadBlockReason) {
      for (const thread of candidateTasks) {
        if (queueHead && thread.threadId === queueHead.threadId) continue;
        this.markQueued(thread, undefined, queueHeadBlockReason);
      }
      return;
    }

    // Only the queue head is eligible, even if several candidates have rounds.
    if (queueHead) {
      candidateTasks = candidateTasks.filter((thread) => thread.threadId === queueHead.threadId);
    }

    const owner = this.executionGate.getOwner();
    const somethingRunning = this.activeTasks.size > 0 || !!owner;

    if (somethingRunning) {
      // Strict serialization: nothing else starts. The owner (whether in this
      // process or another one) will pump the queue when it finishes.
      for (const thread of candidateTasks) {
        if (this.activeTasks.has(thread.threadId)) continue;
        if (owner && owner.threadId === thread.threadId) continue;
        if (!findInFlightRound(thread)) continue;
        this.markQueued(thread, owner);
      }
      return;
    }

    // Free gate: start exactly one task, then stop.
    for (const thread of candidateTasks) {
      const targetRound = findInFlightRound(thread);
      if (!targetRound || !targetRound.prompt) {
        continue;
      }

      // Batch-level scope gate: single flight only stops *simultaneous* edits.
      // This stops a task from starting on files another open task already
      // changed (declared scope or measured touchedFiles) and overwriting them.
      const scopeConflict = findScopeConflicts(thread, allTasks);
      if (scopeConflict) {
        this.markQueued(thread, undefined, describeScopeConflict(scopeConflict), scopeConflict);
        continue;
      }

      // File-scope check is now normally moot (nothing else is executing), but
      // keep it for ledgers/threads that were already in flight when the global
      // gate was introduced.
      const executingThreads = allTasks.filter((t) => this.activeTasks.has(t.threadId));
      const conflict = findFileConflicts(thread, executingThreads);
      if (conflict.hasConflict && conflict.conflictingThread) {
        this.markQueued(
          thread,
          undefined,
          `Queued: Waiting for task ${conflict.conflictingThread.threadId} to release the same file(s) [${(conflict.conflictingFiles || []).join(', ')}]`,
        );
        continue;
      }

      this.startingThreadId = thread.threadId;
      try {
        if (thread.status === 'queued') {
          const restoredStatus: ThreadStatus =
            targetRound.type === 'rework' ? 'reworking' : 'running';
          thread.status = restoredStatus;
          thread.scopeBlockedBy = undefined;
          thread.summary.lastOutputSnippet = 'Executing in background...';
          this.taskStore.upsertTask(thread);
        }

        console.log(
          `[TaskSupervisor] Starting task ${thread.threadId} (Round ${targetRound.round}, status: ${thread.status}); all other tasks stay queued.`,
        );
        this.executeThreadRound(thread, targetRound);
      } finally {
        this.startingThreadId = null;
      }
      return;
    }
  }

  /**
   * Bounded-concurrency pump (only used when the policy enables it).
   *
   * Each workspace has its own queue head, so a `waiting_review`/`paused` head
   * only blocks the tasks that share its workspace. Startable heads are ordered
   * by `createdAt` and started until the mesh-wide slot cap is reached; the
   * per-workspace lease independently refuses a second task of the same
   * workspace, so this pass can never start two rounds in one workspace.
   */
  private scheduleConcurrent(allTasks: DelegationThread[], policy: ConcurrencyPolicy): void {
    const candidateTasks = allTasks
      .filter((thread) => {
        if (this.activeTasks.has(thread.threadId)) return false;
        // A paused task must only restart through the explicit resume path.
        if (thread.status === 'paused') return false;
        const hasRunningRound = (thread.rounds || []).some(
          (r: DelegationRound) => r.status === 'running',
        );
        return (
          thread.status === 'running' ||
          thread.status === 'reworking' ||
          thread.status === 'queued' ||
          hasRunningRound
        );
      })
      .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());

    // Heal ledgers written before the one-round-in-flight invariant existed.
    for (const thread of candidateTasks) {
      this.normalizeRoundIntegrity(thread);
    }

    const groups = groupTasksByWorkspace(allTasks);
    const startable: QueuedExecution[] = [];
    const blockedReasons = new Map<string, string>();

    for (const [key, group] of groups) {
      const head = findQueueHead(group);
      if (!head) continue;
      const headRound = findInFlightRound(head);
      if (
        !headRound ||
        !headRound.prompt ||
        head.status === 'waiting_review' ||
        head.status === 'paused'
      ) {
        blockedReasons.set(
          key,
          `Queued: Waiting for task ${head.threadId} (${head.status}) to complete in workspace ` +
            `${head.workspacePath}; only one task per workspace may run at a time.`,
        );
        continue;
      }
      if (this.activeTasks.has(head.threadId)) continue;
      startable.push({ thread: head, round: headRound });
    }

    // Everything that is not a startable head stays queued, naming its blocker.
    for (const thread of candidateTasks) {
      if (startable.some((entry) => entry.thread.threadId === thread.threadId)) continue;
      const workspaceKey = normalizeWorkspaceKey(thread.workspacePath);
      const owner = this.executionGate.getOwner(workspaceKey);
      if (owner && owner.threadId === thread.threadId) continue;
      if (!findInFlightRound(thread)) continue;
      const reason =
        blockedReasons.get(workspaceKey) ||
        // Keep this text stable: a number that moves on every pass would rewrite
        // the ledger of an already-queued task on every poll.
        `Queued: waiting for a free execution slot for workspace ${thread.workspacePath}.`;
      this.markQueued(thread, owner, reason);
    }

    startable.sort(
      (a, b) => new Date(a.thread.createdAt).getTime() - new Date(b.thread.createdAt).getTime(),
    );

    for (const { thread, round } of startable) {
      const workspaceKey = normalizeWorkspaceKey(thread.workspacePath);
      // Already executing (this process or another one): leave it alone.
      const currentOwner = this.executionGate.getOwner(workspaceKey);
      if (currentOwner && currentOwner.threadId === thread.threadId) continue;

      const activeSlots = this.executionGate.countActive();
      if (activeSlots >= policy.maxConcurrentTasks) {
        this.markQueued(
          thread,
          undefined,
          `Queued: global concurrency limit reached (${activeSlots}/${policy.maxConcurrentTasks}); ` +
            `waiting for a free execution slot.`,
        );
        continue;
      }

      const agentSlots = this.executionGate
        .listOwners()
        .filter((owner) => owner.agent === thread.targetAgent).length;
      if (agentSlots >= policy.maxConcurrentPerAgent) {
        this.markQueued(
          thread,
          undefined,
          `Queued: agent ${thread.targetAgent} already occupies ${agentSlots}/` +
            `${policy.maxConcurrentPerAgent} of its concurrent slots; waiting for one of its ` +
            `running tasks to finish.`,
        );
        continue;
      }

      // Batch-level scope gate: the per-workspace slot only stops *simultaneous*
      // edits in one workspace, not the same file being rewritten in sequence.
      const scopeConflict = findScopeConflicts(thread, allTasks);
      if (scopeConflict) {
        this.markQueued(thread, undefined, describeScopeConflict(scopeConflict), scopeConflict);
        continue;
      }

      const executingThreads = allTasks.filter((t) => this.activeTasks.has(t.threadId));
      const conflict = findFileConflicts(thread, executingThreads);
      if (conflict.hasConflict && conflict.conflictingThread) {
        this.markQueued(
          thread,
          undefined,
          `Queued: Waiting for task ${conflict.conflictingThread.threadId} to release the same file(s) [${(conflict.conflictingFiles || []).join(', ')}]`,
        );
        continue;
      }

      this.startingThreadId = thread.threadId;
      try {
        if (thread.status === 'queued') {
          const restoredStatus: ThreadStatus =
            round.type === 'rework' ? 'reworking' : 'running';
          thread.status = restoredStatus;
          thread.scopeBlockedBy = undefined;
          thread.summary.lastOutputSnippet = 'Executing in background...';
          this.taskStore.upsertTask(thread);
        }

        console.log(
          `[TaskSupervisor] Starting task ${thread.threadId} (Round ${round.round}, status: ${thread.status}) ` +
            `in workspace ${workspaceKey}; other workspaces may still start.`,
        );
        this.executeThreadRound(thread, round);
      } finally {
        this.startingThreadId = null;
      }
    }
  }

  /** Why no slot could be taken when no same-workspace owner explains it. */
  private describeConcurrencyBlocked(
    targetAgent: string,
    policy: ConcurrencyPolicy,
  ): string | undefined {
    if (!policy.enabled) return undefined;
    const owners = this.executionGate.listOwners();
    if (owners.length >= policy.maxConcurrentTasks) {
      return (
        `Queued: global concurrency limit reached (${owners.length}/${policy.maxConcurrentTasks}); ` +
        `waiting for a free execution slot.`
      );
    }
    const sameAgent = owners.filter((owner) => owner.agent === targetAgent).length;
    if (sameAgent >= policy.maxConcurrentPerAgent) {
      return (
        `Queued: agent ${targetAgent} already occupies ${sameAgent}/${policy.maxConcurrentPerAgent} ` +
        `of its concurrent slots; waiting for one of its running tasks to finish.`
      );
    }
    return undefined;
  }

  private markQueued(
    thread: DelegationThread,
    owner?: ExecutionOwner,
    detail?: string,
    scopeConflict?: ScopeConflict,
  ): void {
    const snippet =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
        : 'Queued: Waiting for the current task to finish; tasks execute strictly one at a time.');

    const nextBlock = scopeConflict
      ? { threadId: scopeConflict.threadId, title: scopeConflict.title, files: scopeConflict.files }
      : undefined;
    const blockChanged =
      JSON.stringify(thread.scopeBlockedBy || null) !== JSON.stringify(nextBlock || null);
    thread.scopeBlockedBy = nextBlock;
    if (thread.status === 'queued' && thread.summary.lastOutputSnippet === snippet && !blockChanged) {
      return;
    }

    thread.status = 'queued';
    thread.summary.lastOutputSnippet = snippet;
    this.taskStore.upsertTask(thread);
  }

  /**
   * Executes a thread round autonomously within the persistent supervisor process,
   * syncing live streamed output to taskStore every 30 seconds.
   */
  public executeThreadRound(thread: DelegationThread, round: DelegationRound): Promise<void> {
    if (this.activeTasks.has(thread.threadId)) {
      return this.activeTasks.get(thread.threadId)!;
    }

    // Last line of defence: never launch a round while a DIFFERENT round of the
    // same task is still in flight. `activeTasks` only covers this process, so
    // the persisted state is re-read before starting.
    const stored = this.taskStore.getTask(thread.threadId);
    if (!stored) {
      console.warn(
        `[TaskSupervisor] Task ${thread.threadId} was deleted before round ${round.round} started.`,
      );
      return Promise.resolve();
    }
    const fresh = stored;
    const inFlight = findInFlightRound(fresh);
    if (inFlight && inFlight.round !== round.round) {
      console.warn(
        `[TaskSupervisor] Refusing to start round ${round.round} for ${thread.threadId}: ` +
          `round ${inFlight.round} is still in flight.`,
      );
      return Promise.resolve();
    }

    // Per-task role exclusivity: developer only after reviewer handoff.
    const roleLock = new TaskRoleLock();
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) {
      const owner = roleLock.getOwner(thread.threadId);
      this.markQueued(
        fresh,
        undefined,
        owner?.role === 'reviewer'
          ? 'Queued: Reviewer is still active; developer starts after review handoff.'
          : `Queued: ${roleLock.describe(owner)} is active; a task may only have one role at a time.`,
      );
      return Promise.resolve();
    }

    // Cross-process execution slot: with concurrency off this is the single
    // mesh-wide lease; with it on, the lease belongs to this task's workspace, so
    // two tasks of the SAME workspace can never overlap no matter who asks.
    const gatePolicy = concurrencyPolicy();
    const workspaceKey = gatePolicy.enabled
      ? normalizeWorkspaceKey(thread.workspacePath)
      : undefined;
    const lease = this.executionGate.tryAcquire(
      thread.threadId,
      round.round,
      workspaceKey,
      thread.targetAgent,
    );
    if (!lease) {
      roleLock.release(roleLease);
      const owner = this.executionGate.getOwner(workspaceKey);
      if (owner?.threadId !== thread.threadId) {
        this.markQueued(
          fresh,
          owner,
          this.describeConcurrencyBlocked(thread.targetAgent, gatePolicy),
        );
      }
      return Promise.resolve();
    }

    const promise = (async () => {
      try {
        const isRework = shouldResumeNativeSession(fresh, round);
        const roundNumber = round.round;

      console.log(
        `[TaskSupervisor] Executing ${thread.threadId} Round ${roundNumber} on ${thread.targetAgent} (isRework: ${isRework})...`,
      );

      this.emit('taskStarted', { thread, round });

      let streamedOutput = '';
      const startTime = Date.now();
      let streamTimer: NodeJS.Timeout | null = null;

      // Every 30 seconds, sync the live output stream and duration to taskStore
      const syncIntervalMs = this.streamSyncIntervalMs;
      streamTimer = setInterval(() => {
        if (this.isDisposed) {
          if (streamTimer) clearInterval(streamTimer);
          return;
        }

        const elapsedMs = Date.now() - startTime;
        const cleanSnippet = this.runner.extractCleanOutput(streamedOutput) || streamedOutput;
        const lines = cleanSnippet.trim().split('\n').filter(Boolean);
        const latestPreview =
          lines.length > 0 ? lines[lines.length - 1].slice(0, 150) : 'Executing in background...';

        console.log(
          `[TaskSupervisor] 30s stream sync for ${thread.threadId} (Round ${roundNumber}): ${Math.round(elapsedMs / 1000)}s elapsed`,
        );

        this.taskStore.updateRound(
          thread.threadId,
          roundNumber,
          {
            output: cleanSnippet,
            durationMs: elapsedMs,
            status: 'running',
          },
        );

        this.emit('taskProgress', {
          threadId: thread.threadId,
          roundNumber,
          durationMs: elapsedMs,
          latestPreview,
        });
      }, syncIntervalMs);

      const logFilePath = roundExecutionLogPath(thread.threadId, roundNumber);
      try {
        fs.writeFileSync(logFilePath, '', 'utf8');
      } catch {}
      this.liveLogs.set(thread.threadId, '');

      let result: RunResult;
      // Baseline before the agent starts: the diff afterwards attributes exactly
      // which files this round changed.
      const scopeBefore = captureWorkspaceSnapshot(thread.workspacePath);
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: isRework ? appendCompactionProtocol(round.prompt, roundNumber) : round.prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
          model: thread.targetModel,
          previousTokenUsage: readStoredRoundUsage(this.taskStore, thread.threadId, roundNumber),
          onChildPid: (pid) => this.executionGate.attachChildPid(pid, lease),
          onStdoutChunk: (chunk: string) => {
            streamedOutput += chunk;
            const cur = this.liveLogs.get(thread.threadId) || '';
            this.liveLogs.set(thread.threadId, cur + chunk);
            try {
              fs.appendFileSync(logFilePath, chunk, 'utf8');
            } catch {}
          },
          onStderrChunk: (chunk: string) => {
            streamedOutput += chunk;
            const cur = this.liveLogs.get(thread.threadId) || '';
            this.liveLogs.set(thread.threadId, cur + chunk);
            try {
              fs.appendFileSync(logFilePath, chunk, 'utf8');
            } catch {}
          },
        });
      } catch (err: any) {
        result = {
          exitCode: -1,
          stdout: '',
          stderr: String(err),
          extractedOutput: '',
          durationMs: 0,
          error: `Execution error: ${err.message || err}`,
        };
      } finally {
        if (streamTimer) {
          clearInterval(streamTimer);
          streamTimer = null;
        }
      }

      const touchedFiles = diffWorkspaceSnapshots(
        scopeBefore,
        captureWorkspaceSnapshot(thread.workspacePath),
      );

      // Re-read latest task from disk to avoid overwriting newer state
      this.taskStore.loadFromDisk();
      const currentThread = this.taskStore.getTask(thread.threadId);
      if (!currentThread) {
        console.warn(
          `[TaskSupervisor] Task ${thread.threadId} was deleted while round ${roundNumber} was executing.`,
        );
        return;
      }

      // A pause may have landed after the child exited but before this handler
      // ran; the control plane's state wins over a "successful" result.
      const pausedByControl = currentThread.status === 'paused';
      const interruption = result.interruption || (pausedByControl ? currentThread.interruption : undefined);
      const isPaused = pausedByControl || interruption?.kind === 'paused';
      const responseOk = result.assessment
        ? result.assessment.success
        : result.exitCode === 0;
      const isSuccess = !interruption && !pausedByControl && responseOk;
      const finalRoundStatus = isPaused ? 'paused' : isSuccess ? 'completed' : 'failed';
      const finalThreadStatus = isPaused ? 'paused' : isSuccess ? 'waiting_review' : 'failed';
      const nativeSessionId = result.actualSessionId || currentThread.receiverSessionId;

      if (result.actualSessionId && result.actualSessionId !== currentThread.receiverSessionId) {
        console.log(
          `[TaskSupervisor] Updating receiverSessionId from ${currentThread.receiverSessionId} to ${result.actualSessionId}`,
        );
        currentThread.receiverSessionId = result.actualSessionId;
      }
      if (
        result.actualSessionId ||
        (thread.targetAgent === 'claude_code' && round.type === 'initial')
      ) {
        currentThread.receiverSessionCaptured = true;
      }

      console.log(
        `[TaskSupervisor] ${thread.threadId} Round ${roundNumber} completed with exitCode ${result.exitCode}. Updating store...`,
      );

      const isLatestRound = !currentThread.currentRound || roundNumber >= currentThread.currentRound;
      const threadStatusToApply = isLatestRound ? finalThreadStatus : currentThread.status;

      const updatedThread = this.taskStore.updateRound(
        thread.threadId,
        roundNumber,
        {
          output: result.extractedOutput || result.stdout,
          error: result.error,
          durationMs: result.durationMs,
          status: finalRoundStatus,
          assessment: result.assessment,
          touchedFiles: touchedFiles.length > 0 ? touchedFiles : undefined,
          // Execution metadata: who ran this round, on which model, and what
          // it cost. It must survive the lagged compaction.
          executorAgent: currentThread.targetAgent,
          executorModel: result.model || currentThread.targetModel,
          tokenUsage: result.tokenUsage,
        },
        threadStatusToApply,
        nativeSessionId,
        undefined,
        interruption,
      );

        if (updatedThread && this.taskStore.getTask(updatedThread.threadId)) {
          const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
          if (completedRound) {
            this.emit('taskCompleted', { thread: updatedThread, round: completedRound, result });

            if (!interruption && isSuccess && isLatestRound) {
              console.log(
                `[TaskSupervisor] Round ${roundNumber} successful and is latest. Notifying caller ${updatedThread.initiatorAgent}...`,
              );
              // Lagged compaction of the previous round, best-effort, before the
              // developer role is released to the reviewer.
              compactRoundBestEffort(this.taskStore, {
                threadId: updatedThread.threadId,
                currentRoundNumber: roundNumber,
                currentOutput: result.extractedOutput || result.stdout,
              });
              // The developer role ends when the round is finalized, before the
              // reviewer starts. Keep one role active per task.
              roleLock.release(roleLease);
              await this.callbackManager.notifyCaller(updatedThread, completedRound);
            } else if (isLatestRound && !isSuccess && !isUserInterruption(interruption)) {
              console.log(
                `[TaskSupervisor] Round ${roundNumber} failed. Notifying caller ${updatedThread.initiatorAgent} so it can retry_task...`,
              );
              roleLock.release(roleLease);
              const notified = await this.callbackManager.notifyCallerOfFailure(
                updatedThread,
                roundNumber,
                result,
              );
              // Persist the dedupe marker so a restart cannot wake the caller twice.
              if (notified) this.taskStore.upsertTask(updatedThread);
            }
          }
        }
      } finally {
        // Release only after the round is finalized, then pump the next task.
        this.activeTasks.delete(thread.threadId);
        this.executionGate.release(lease);
        roleLock.release(roleLease);
        setTimeout(() => {
          this.checkPendingTasks();
        }, 50);
      }
    })();

    this.activeTasks.set(thread.threadId, promise);
    return promise;
  }

  public isExecuting(threadId: string): boolean {
    return this.activeTasks.has(threadId);
  }

  /**
   * Retrieves recent log lines (default 1,000 lines) for a running or past task.
   */
  public getRecentLogs(
    threadId: string,
    maxLines: number = 1000,
  ): {
    threadId: string;
    isRunning: boolean;
    totalLines: number;
    returnedLines: number;
    logs: string;
    recentLogs: string;
    logFilePath?: string;
  } {
    const isRunning = this.activeTasks.has(threadId);
    let rawOutput = this.liveLogs.get(threadId);
    let resolvedFilePath: string | undefined = undefined;

    // Compare round numbers numerically: a plain name sort puts round_9 after
    // round_10, which used to return the wrong round for long-lived tasks.
    const latest = findLatestExecutionLog(threadId, path.dirname(this.taskStore.getStoragePath()))
      || findLatestExecutionLog(threadId);
    if (latest) resolvedFilePath = latest.path;

    if ((!rawOutput || !rawOutput.trim()) && resolvedFilePath) {
      rawOutput = readLogExcerpt(resolvedFilePath);
    }

    if (!rawOutput || !rawOutput.trim()) {
      this.taskStore.loadFromDisk();
      const task = this.taskStore.getTask(threadId);
      if (task && task.rounds && task.rounds.length > 0) {
        const lastRound = task.rounds[task.rounds.length - 1];
        rawOutput = lastRound.output || lastRound.error || '';
      }
    }

    const allText = rawOutput || '';
    const allLines = allText.split(/\r?\n/);
    const tailLines = allLines.length > maxLines ? allLines.slice(-maxLines) : allLines;

    return {
      threadId,
      isRunning,
      totalLines: allLines.length,
      returnedLines: tailLines.length,
      logs: tailLines.join('\n'),
      recentLogs: tailLines.join('\n'),
      logFilePath: resolvedFilePath,
    };
  }

  /**
   * Static helper to retrieve recent logs, falling back to disk and task store if supervisor is not active.
   */
  public static getRecentLogs(
    threadId: string,
    maxLines: number = 1000,
    taskStore?: TaskStore,
  ): {
    threadId: string;
    isRunning: boolean;
    totalLines: number;
    returnedLines: number;
    logs: string;
    recentLogs: string;
    logFilePath?: string;
  } {
    const supervisor = TaskSupervisor.instance;
    if (supervisor) {
      return supervisor.getRecentLogs(threadId, maxLines);
    }

    let rawOutput = '';
    let resolvedFilePath: string | undefined = undefined;

    // The dispatcher/rework controller write under the store root, the
    // supervisor under the default home; accept both, newest round wins.
    const storeRoot = taskStore ? path.dirname(taskStore.getStoragePath()) : undefined;
    const candidates = [storeRoot, undefined]
      .map((root) => findLatestExecutionLog(threadId, root))
      .filter((entry): entry is NonNullable<typeof entry> => Boolean(entry));
    if (candidates.length > 0) {
      const latest = candidates.reduce((a, b) => (b.round > a.round ? b : a));
      resolvedFilePath = latest.path;
      rawOutput = readLogExcerpt(latest.path);
    }

    if (!rawOutput) {
      const store = taskStore || TaskStore.getInstance();
      store.loadFromDisk();
      const task = store.getTask(threadId);
      if (task && task.rounds && task.rounds.length > 0) {
        const lastRound = task.rounds[task.rounds.length - 1];
        rawOutput = lastRound.output || lastRound.error || '';
      }
    }

    const allLines = (rawOutput || '').split(/\r?\n/);
    const tailLines = allLines.length > maxLines ? allLines.slice(-maxLines) : allLines;

    return {
      threadId,
      isRunning: false,
      totalLines: allLines.length,
      returnedLines: tailLines.length,
      logs: tailLines.join('\n'),
      recentLogs: tailLines.join('\n'),
      logFilePath: resolvedFilePath,
    };
  }

  public dispose(): void {
    if (TaskSupervisor.instance === this) {
      TaskSupervisor.instance = undefined;
    }
    this.isDisposed = true;
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    try {
      if (fs.existsSync(this.lockFilePath)) {
        fs.unlinkSync(this.lockFilePath);
      }
    } catch {}
  }
}