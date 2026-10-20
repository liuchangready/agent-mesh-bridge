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
  shouldResumeNativeSession,
} from './types';
import { findFileConflicts } from './file-scope';
import { ExecutionGate, ExecutionOwner } from './execution-gate';
import { TaskRoleLock } from './task-role-lock';

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
   * Global, strict FIFO pump.
   *
   * Only ONE delegated round may execute across the whole mesh. This method
   * therefore either starts exactly one thread and returns, or (when something
   * is already in flight) marks every other candidate as queued. It never
   * launches two rounds in one pass.
   */
  public checkPendingTasks(): void {
    if (this.isDisposed || this.startingThreadId) return;

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();

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

  private markQueued(thread: DelegationThread, owner?: ExecutionOwner, detail?: string): void {
    const snippet =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
        : 'Queued: Waiting for the current task to finish; tasks execute strictly one at a time.');

    if (thread.status === 'queued' && thread.summary.lastOutputSnippet === snippet) return;

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

    // Cross-process single-flight gate: refuse to spawn a second agent process
    // while any other round is executing, no matter which process owns it.
    const lease = this.executionGate.tryAcquire(thread.threadId, round.round);
    if (!lease) {
      roleLock.release(roleLease);
      const owner = this.executionGate.getOwner();
      if (owner?.threadId !== thread.threadId) {
        this.markQueued(fresh, owner);
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

      const logFilePath = path.join(this.logDir, `${thread.threadId}_round_${roundNumber}.log`);
      try {
        fs.writeFileSync(logFilePath, '', 'utf8');
      } catch {}
      this.liveLogs.set(thread.threadId, '');

      let result: RunResult;
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: round.prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
          model: thread.targetModel,
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
              // The developer role ends when the round is finalized, before the
              // reviewer starts. Keep one role active per task.
              roleLock.release(roleLease);
              await this.callbackManager.notifyCaller(updatedThread, completedRound);
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

    if (fs.existsSync(this.logDir)) {
      try {
        const files = fs
          .readdirSync(this.logDir)
          .filter((f) => f.startsWith(threadId))
          .sort();
        if (files.length > 0) {
          resolvedFilePath = path.join(this.logDir, files[files.length - 1]);
        }
      } catch {}
    }

    if ((!rawOutput || !rawOutput.trim()) && resolvedFilePath && fs.existsSync(resolvedFilePath)) {
      try {
        rawOutput = fs.readFileSync(resolvedFilePath, 'utf8');
      } catch {}
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

    const logDir = path.join(os.homedir(), '.agent-mesh-bridge', 'logs');
    let rawOutput = '';
    let resolvedFilePath: string | undefined = undefined;

    if (fs.existsSync(logDir)) {
      try {
        const files = fs
          .readdirSync(logDir)
          .filter((f) => f.startsWith(threadId))
          .sort();
        if (files.length > 0) {
          resolvedFilePath = path.join(logDir, files[files.length - 1]);
          rawOutput = fs.readFileSync(resolvedFilePath, 'utf8');
        }
      } catch {}
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
