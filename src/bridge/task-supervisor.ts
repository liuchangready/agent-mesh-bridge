import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { TaskStore } from './task-store';
import { assertCanExecute } from './agent-capabilities';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { AgentDetector } from './agent-detector';
import {
  DelegationThread,
  DelegationRound,
  isIsolatedTaskWorktree,
  findInFlightRound,
  getInFlightRounds,
  isReviewerBootstrapPending,
  isUserInterruption,
  shouldResumeNativeSession,
} from './types';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { TaskExecutionOwner, TaskExecutionRegistration, TaskExecutionRegistry } from './task-execution-registry';
import { ConcurrencyPolicy, concurrencyPolicy } from './bridge-policy';
import { TaskRoleLock } from './task-role-lock';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';
import {
  findLatestExecutionLog,
  readTextFilePaged,
  roundExecutionLogPath,
} from './round-log';
import { commitTaskChanges, changedFiles, currentHead } from './git-worktree';

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
  private executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
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
    executionRegistry?: TaskExecutionRegistry,
  ) {
    super();
    TaskSupervisor.instance = this;
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.storageDir = path.dirname(taskStore.getStoragePath());
    this.executionRegistry = executionRegistry || new TaskExecutionRegistry(this.storageDir);
    const storageDir = this.storageDir;
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
   * Every process scans all runnable task rounds. TaskRoleLock protects a task's
   * reviewer/developer handoff; the registry tracks PIDs and may feed optional
   * best-effort count limits, but neither imposes workspace or FIFO exclusion.
   */
  public checkPendingTasks(): void {
    if (this.isDisposed || this.startingThreadId) return;

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();

    // A deleted or paused task must stop its local runner promptly.
    for (const activeThreadId of Array.from(this.activeTasks.keys())) {
      const live = this.taskStore.getTask(activeThreadId);
      if (!live) {
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

    // Registry records are observations for PID recovery, never execution locks.
    for (const owner of this.executionRegistry.listOwners()) {
      if (this.taskStore.getTask(owner.threadId)) continue;
      this.executionRegistry.killOrphanChild(owner.threadId, true);
      if (owner.pid === process.pid && !this.activeTasks.has(owner.threadId)) {
        this.executionRegistry.end({
          token: owner.token,
          threadId: owner.threadId,
          round: owner.round,
          recordPath: owner.recordPath,
        });
      }
    }

    this.scheduleReadyTasks(allTasks, concurrencyPolicy());
  }

  /** Scan every ready round; a parked or blocked task never blocks a peer. */
  private scheduleReadyTasks(allTasks: DelegationThread[], policy: ConcurrencyPolicy): void {
    const candidates = allTasks
      .filter((thread) => {
        if (!thread || !thread.threadId || this.activeTasks.has(thread.threadId)) return false;
        if (thread.status === 'paused' || thread.status === 'waiting_review') return false;
        if (thread.reviewBlocked || isReviewerBootstrapPending(thread)) return false;
        return thread.status === 'running' || thread.status === 'reworking' || thread.status === 'queued';
      })
      .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());

    for (const thread of candidates) {
      this.normalizeRoundIntegrity(thread);
      const round = findInFlightRound(thread);
      if (!round || !round.prompt) continue;

      if (this.executionRegistry.getOwner(thread.threadId)) continue;
      const roleLock = new TaskRoleLock(this.storageDir);
      const roleOwner = roleLock.getOwner(thread.threadId);
      if (roleOwner) continue;

      if (isReviewerBootstrapPending(thread)) continue;
      if (policy.enabled) {
        const owners = this.executionRegistry.listOwners();
        const agentSlots = owners.filter((owner) => owner.agent === thread.targetAgent).length;
        if (owners.length >= policy.maxConcurrentTasks || agentSlots >= policy.maxConcurrentPerAgent) {
          this.markQueued(thread, undefined, this.describeConcurrencyBlocked(thread.targetAgent, policy));
          continue;
        }
      }

      // executeThreadRound takes the role and records the PID synchronously before yielding to the runner.
      this.startingThreadId = thread.threadId;
      try {
        void this.executeThreadRound(thread, round);
      } catch (error: any) {
        console.warn(`[TaskSupervisor] Could not start ${thread.threadId} round ${round.round}: ${error?.message || error}`);
      } finally {
        this.startingThreadId = null;
      }
    }
  }

  private describeConcurrencyBlocked(targetAgent: string, policy: ConcurrencyPolicy): string {
    const owners = this.executionRegistry.listOwners();
    const sameAgent = owners.filter((owner) => owner.agent === targetAgent).length;
    if (owners.length >= policy.maxConcurrentTasks) {
      return `Queued: active execution limit reached (${owners.length}/${policy.maxConcurrentTasks}); waiting for a free execution record.`;
    }
    return `Queued: agent ${targetAgent} has ${sameAgent}/${policy.maxConcurrentPerAgent} active executions; waiting for one to finish.`;
  }

  private markQueued(
    thread: DelegationThread,
    owner?: TaskExecutionOwner,
    detail?: string,
  ): void {
    if (this.executionRegistry.getOwner(thread.threadId)) return;
    const roleOwner = new TaskRoleLock(this.storageDir).getOwner(thread.threadId);
    if (roleOwner) return;
    this.taskStore.loadFromDisk();
    const fresh = this.taskStore.getTask(thread.threadId);
    const candidateRound = findInFlightRound(thread);
    const freshRound = fresh ? findInFlightRound(fresh) : undefined;
    if (
      !fresh ||
      (fresh.status !== 'queued' && fresh.status !== 'running' && fresh.status !== 'reworking') ||
      fresh.targetAgent !== thread.targetAgent ||
      fresh.reviewBlocked ||
      isReviewerBootstrapPending(fresh) ||
      !candidateRound || freshRound?.round !== candidateRound.round || freshRound.prompt !== candidateRound.prompt
    ) return;
    const message = detail || (owner
      ? `Queued: waiting for task ${owner.threadId} round ${owner.round}.`
      : 'Queued: waiting for an execution slot.');
    if (fresh.status === 'queued' && fresh.summary.lastOutputSnippet === message) return;
    fresh.status = 'queued';
    fresh.summary.lastOutputSnippet = message;
    this.taskStore.upsertTask(fresh);
  }
  public executeThreadRound(thread: DelegationThread, round: DelegationRound): Promise<void> {
    if (this.activeTasks.has(thread.threadId)) {
      return this.activeTasks.get(thread.threadId)!;
    }
    if (this.executionRegistry.getOwner(thread.threadId)) return Promise.resolve();

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
    try {
      assertCanExecute(stored.targetAgent, 'run a scheduled task round');
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      if (stored.status === 'queued' || stored.status === 'running' || stored.status === 'reworking') {
        const pendingRound = findInFlightRound(stored);
        if (pendingRound) {
          pendingRound.status = 'failed';
          pendingRound.error = reason;
        }
        stored.status = 'failed';
        stored.summary.lastOutputSnippet = reason;
        this.taskStore.upsertTask(stored);
        console.warn(`[TaskSupervisor] Refusing unsupported agent for task ${stored.threadId}: ${reason}`);
      }
      return Promise.resolve();
    }
    let fresh = stored;
    // Dedicated review: the reviewer conversation must dispatch first.
    if (isReviewerBootstrapPending(fresh)) {
      return Promise.resolve();
    }
    const inFlight = findInFlightRound(fresh);
    if (
      fresh.status === 'paused' || fresh.status === 'waiting_review' ||
      fresh.status === 'completed' || fresh.status === 'failed' || fresh.status === 'review_failed' ||
      fresh.targetAgent !== thread.targetAgent || !inFlight || inFlight.round !== round.round ||
      inFlight.status !== 'running' || inFlight.prompt !== round.prompt
    ) {
      console.warn(
        `[TaskSupervisor] Refusing stale round ${round.round} for ${thread.threadId}; task state changed before admission.`,
      );
      return Promise.resolve();
    }

    // Per-task role exclusivity: developer only after reviewer handoff.
    const roleLock = new TaskRoleLock(this.storageDir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) return Promise.resolve();

    // Re-read after role acquisition so a pause/approval/retarget during the
    // scan cannot revive a stale candidate.
    this.taskStore.loadFromDisk();
    const latest = this.taskStore.getTask(thread.threadId);
    const latestRound = latest?.rounds.filter((entry) => entry.round === round.round).slice(-1)[0];
    if (
      !latest || latest.status === 'paused' || latest.status === 'waiting_review' ||
      latest.status === 'completed' || latest.status === 'failed' || latest.status === 'review_failed' ||
      latest.targetAgent !== thread.targetAgent || !latestRound || latestRound.status !== 'running' ||
      latestRound.prompt !== round.prompt || findInFlightRound(latest)?.round !== round.round ||
      isReviewerBootstrapPending(latest)
    ) {
      roleLock.release(roleLease);
      return Promise.resolve();
    }
    fresh = latest;

    const gatePolicy = concurrencyPolicy();
    if (gatePolicy.enabled) {
      const owners = this.executionRegistry.listOwners();
      const agentSlots = owners.filter((owner) => owner.agent === fresh.targetAgent).length;
      if (owners.length >= gatePolicy.maxConcurrentTasks || agentSlots >= gatePolicy.maxConcurrentPerAgent) {
        roleLock.release(roleLease);
        this.markQueued(fresh, undefined, this.describeConcurrencyBlocked(fresh.targetAgent, gatePolicy));
        return Promise.resolve();
      }
    }

    let registration: TaskExecutionRegistration;
    try {
      registration = this.executionRegistry.begin(
        fresh.threadId,
        round.round,
        fresh.targetAgent,
        fresh.workspacePath,
      );
    } catch (error: any) {
      roleLock.release(roleLease);
      latest.status = 'failed';
      latestRound.status = 'failed';
      latestRound.error = `Could not register task execution: ${error?.message || error}`;
      latest.updatedAt = new Date().toISOString();
      this.taskStore.upsertTask(latest);
      return Promise.resolve();
    }

    try {
      latest.status = round.type === 'rework' ? 'reworking' : 'running';
      latest.summary.lastOutputSnippet = 'Executing in background...';
      this.taskStore.upsertTask(latest);
    } catch (error) {
      this.executionRegistry.end(registration);
      roleLock.release(roleLease);
      throw error;
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

      const logFilePath = roundExecutionLogPath(
        thread.threadId,
        roundNumber,
        undefined,
        thread.targetAgent,
        thread.artifactDir,
      );
      try {
        fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        fs.writeFileSync(logFilePath, '', 'utf8');
      } catch {}
      this.liveLogs.set(thread.threadId, '');

      let result: RunResult;
      // Baseline before the agent starts: the diff afterwards attributes exactly
      // which files this round changed.
      const workspaceBefore = captureWorkspaceSnapshot(thread.workspacePath);
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: isRework ? appendCompactionProtocol(round.prompt, roundNumber) : round.prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          artifactDir: thread.artifactDir,
          taskRound: roundNumber,
          model: thread.targetModel,
          role: 'developer',
          previousTokenUsage: readStoredRoundUsage(this.taskStore, thread.threadId, roundNumber),
          onChildPid: (pid) => {
            this.executionRegistry.attachChildPid(pid, registration);
            roleLock.attachChild(roleLease, pid);
          },
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
        workspaceBefore,
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
      let isSuccess = !interruption && !pausedByControl && responseOk;
      if (isSuccess && isIsolatedTaskWorktree(currentThread.worktree)) {
        try {
          currentThread.worktree.headCommit = commitTaskChanges(currentThread.worktree, `Agent work ${currentThread.threadId} round ${roundNumber}`);
          currentThread.worktree.integration = { phase: currentThread.worktree.headCommit === currentThread.worktree.baseCommit ? 'no_changes' : 'ready', sourceCommit: currentThread.worktree.headCommit, updatedAt: new Date().toISOString() };
        } catch (error: any) {
          result.error = `Could not create the task delivery commit: ${error?.message || error}`;
          result.exitCode = 1;
          isSuccess = false;
        }
      }
      if (!isSuccess && isIsolatedTaskWorktree(currentThread.worktree)) {
        try {
          const head = currentHead(currentThread.worktree.path);
          if (head !== currentThread.worktree.baseCommit || changedFiles(currentThread.worktree.path).length) {
            currentThread.worktree.headCommit = head;
            currentThread.worktree.integration = { phase: 'ready', sourceCommit: head, updatedAt: new Date().toISOString() };
          }
        } catch {}
      }
      const finalRoundStatus = isPaused ? 'paused' : isSuccess ? 'completed' : 'failed';
      const finalThreadStatus = isPaused ? 'paused' : isSuccess ? 'waiting_review' : 'failed';
      const nativeSessionId = result.actualSessionId || currentThread.receiverSessionId;

      if (result.actualSessionId && result.actualSessionId !== currentThread.receiverSessionId) {
        console.log(
          `[TaskSupervisor] Updating receiverSessionId from ${currentThread.receiverSessionId} to ${result.actualSessionId}`,
        );
        currentThread.receiverSessionId = result.actualSessionId;
      }
      if (result.actualSessionId) {
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
        this.executionRegistry.end(registration);
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
    const artifactDir = this.taskStore.getTask(threadId)?.artifactDir;
    const latest = artifactDir
      ? findLatestExecutionLog(threadId, undefined, undefined, artifactDir)
      : findLatestExecutionLog(threadId, path.dirname(this.taskStore.getStoragePath()))
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
    // supervisor under the default home; a unified task directory is
    // self-contained and takes precedence so no agent-wide scan is needed.
    const store = taskStore || TaskStore.getInstance();
    store.loadFromDisk();
    const storeRoot = path.dirname(store.getStoragePath());
    const artifactDir = store.getTask(threadId)?.artifactDir;
    const candidates = (artifactDir
      ? [findLatestExecutionLog(threadId, undefined, undefined, artifactDir)]
      : [storeRoot, undefined].map((root) => findLatestExecutionLog(threadId, root))
    ).filter((entry): entry is NonNullable<typeof entry> => Boolean(entry));
    if (candidates.length > 0) {
      const latest = candidates.reduce((a, b) => (b.round > a.round ? b : a));
      resolvedFilePath = latest.path;
      rawOutput = readLogExcerpt(latest.path);
    }

    if (!rawOutput) {
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
