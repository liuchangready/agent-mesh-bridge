import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type { ChildProcess } from 'child_process';
import {
  AgentId,
  DelegationRound,
  DelegationThread,
  ExecutionControl,
  ExecutionPhase,
  ExecutionRun,
  FailureStage,
  NextAction,
  ProcessIdentity,
  ResponseAssessment,
  TaskDecisionRecord,
  TaskDecisionResumePoint,
  TaskInterruption,
  ThreadStatus,
  appendInterruption,
  clearTaskFailure,
  enterTaskFailure,
  findInFlightRound,
  isReviewerBootstrapPending,
  isReviewerWakeEligible,
  isTaskUnfinished,
  isUserInterruption,
  nextRoundNumber,
  shouldResumeNativeSession,
} from './types';
import { TaskStore } from './task-store';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { MergeCoordinator, MergeResult } from './merge-coordinator';
import { TaskRoleLock } from './task-role-lock';
import { NativeSessionLock, NativeSessionLease } from './native-session-lock';
import { TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { MeshFileLock } from './mesh-file-lock';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';
import {
  confirmProcessTreeExit,
  getProcessBirthId,
  isProcessDescendant,
  killPidTree,
  probeProcessIdentity,
} from './platform';
import { gitOutput } from './git-worktree';
import { ConcurrencyPolicy, concurrencyPolicy } from './bridge-policy';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import {
  assessRunLaunchEvidence,
  captureChildDescendants,
  isUnconfirmedLaunchAfterOwnerExit,
  registerRunChildV2,
  recordLaunchRecoveryFailure,
  stopVerifiedChildTree,
  verifySubtreeOwnership,
  RunIdentity,
} from './process-binding';
import { validateV2ExecutionEligibility } from './execution-gate';

export const DEFAULT_EXECUTION_BUDGET_MS = 120 * 60 * 1000; // 120 minutes
export const AUTO_RETRY_BACKOFFS_MS = [5_000, 30_000];
export const MAX_AUTO_RETRIES = 2;
export const PROCESS_STOP_TIMEOUT_MS = 10_000;

export interface ExecutionControllerOptions {
  storageDir?: string;
  defaultBudgetLimitMs?: number;
  streamSyncIntervalMs?: number;
  retryBackoffsMs?: number[];
  monotonicNow?: () => number;
  wallClockNow?: () => number;
  standingScanIntervalMs?: number;
}

export class ExecutionController {
  private static instance: ExecutionController | undefined;
  private readonly taskStore: TaskStore;
  private readonly runner: AutonomousRunner;
  private readonly callbackManager: CallbackManager;
  private readonly mergeCoordinator: MergeCoordinator;
  private readonly roleLock: TaskRoleLock;
  private readonly sessionLock: NativeSessionLock;
  private readonly executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private readonly defaultBudgetLimitMs: number;
  private readonly checkpointIntervalMs: number;
  private readonly retryBackoffsMs: number[];
  private readonly monotonicNow: () => number;
  private readonly wallClockNow: () => number;
  private readonly mergeLock: MeshFileLock;
  private readonly activeExecutions = new Map<string, { abort: () => void; activeSinceMs: number }>();
  private readonly backoffTimers = new Map<string, NodeJS.Timeout>();
  private readonly threadTransitionChains = new Map<string, Promise<any>>();
  private standingScanTimer: NodeJS.Timeout | undefined;
  private isDisposed = false;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    mergeCoordinator?: MergeCoordinator,
    roleLock?: TaskRoleLock,
    sessionLock?: NativeSessionLock,
    executionRegistry?: TaskExecutionRegistry,
    options?: ExecutionControllerOptions,
  ) {
    ExecutionController.instance = this;
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.storageDir = options?.storageDir || path.dirname(taskStore.getStoragePath());
    this.defaultBudgetLimitMs = options?.defaultBudgetLimitMs ?? DEFAULT_EXECUTION_BUDGET_MS;
    this.checkpointIntervalMs = options?.streamSyncIntervalMs ?? 5_000;
    this.retryBackoffsMs = options?.retryBackoffsMs ?? AUTO_RETRY_BACKOFFS_MS;
    this.monotonicNow = options?.monotonicNow ?? monotonicNowMs;
    this.wallClockNow = options?.wallClockNow ?? (() => Date.now());
    this.roleLock = roleLock || new TaskRoleLock(this.storageDir);
    this.sessionLock = sessionLock || new NativeSessionLock(this.storageDir);
    this.executionRegistry = executionRegistry || new TaskExecutionRegistry(this.storageDir);
    this.mergeLock =
      mergeCoordinator?.getLock?.() ||
      new MeshFileLock(path.join(this.storageDir, 'merge.lock'));
    this.mergeCoordinator =
      mergeCoordinator ||
      new MergeCoordinator(taskStore, this.storageDir, this.mergeLock, (thread) => {
        const reviewRound = [...(thread.rounds || [])].reverse().find((r) => r.type !== 'approval');
        if (reviewRound) void this.callbackManager.notifyCaller(thread, reviewRound);
      });

    const scanInterval = options?.standingScanIntervalMs !== undefined
      ? options.standingScanIntervalMs
      : 5_000;
    if (scanInterval > 0) {
      this.standingScanTimer = setInterval(() => {
        if (!this.isDisposed) {
          void this.scanAndDrain().catch(() => {});
        }
      }, scanInterval);
      this.standingScanTimer.unref?.();
    }
  }

  public static getInstance(): ExecutionController | undefined {
    return ExecutionController.instance;
  }

  public getStorageDir(): string {
    return this.storageDir;
  }

  /**
   * Probe whether owner and child processes recorded for a run are still alive.
   */
  public inspectRunProcesses(run?: ExecutionControl['run']): {
    ownerStatus: 'alive' | 'exited' | 'unknown' | 'none';
    childStatus: 'alive' | 'exited' | 'unknown' | 'none';
  } {
    if (!run) {
      return { ownerStatus: 'none', childStatus: 'none' };
    }
    const ownerStatus = run.owner?.pid
      ? (run.owner.birthId ? probeProcessIdentity(run.owner.pid, run.owner.birthId) : 'unknown')
      : 'none';
    const childStatus = run.child?.pid
      ? (run.child.birthId ? probeProcessIdentity(run.child.pid, run.child.birthId) : 'unknown')
      : 'none';
    return { ownerStatus, childStatus };
  }

  /**
   * Probe whether a recorded run is still alive.
   */
  public probeRun(
    threadId: string,
    run?: ExecutionControl['run'],
  ): 'alive' | 'exited' | 'unknown' {
    if (!run) return 'exited';
    const { ownerStatus, childStatus } = this.inspectRunProcesses(run);
    if (ownerStatus === 'alive' || childStatus === 'alive') {
      return 'alive';
    }
    const thread = this.taskStore.getTask(threadId);
    if (thread?.execution?.version === 2) {
      const assessment = assessRunLaunchEvidence(run);
      if (!assessment.safeToClean) {
        return 'unknown';
      }
    }
    if (ownerStatus === 'unknown' || childStatus === 'unknown') {
      return 'unknown';
    }
    return 'exited';
  }

  private async transitionTask(
    threadId: string,
    change: (thread: DelegationThread) => void,
  ): Promise<DelegationThread> {
    const prev = this.threadTransitionChains.get(threadId) || Promise.resolve();
    const next = prev.catch(() => {}).then(async () => {
      for (let attempt = 0; attempt < 3; attempt++) {
        this.taskStore.ensureUpToDate();
        const currentRev = this.taskStore.getTask(threadId)?.execution?.revision ?? 0;
        try {
          return await this.taskStore.transitionExecution(threadId, currentRev, change);
        } catch (err: any) {
          if (err?.message?.includes('revision mismatch') && attempt < 2) {
            continue;
          }
          throw err;
        }
      }
      throw new Error(`Failed to transition task ${threadId} after retries`);
    });
    this.threadTransitionChains.set(threadId, next);
    return next;
  }

  /**
   * Primary entry point to start or resume a phase execution for a task.
   */
  public async startExecution(
    threadId: string,
    targetPhase?: ExecutionPhase,
    options?: {
      purpose?: 'bootstrap' | 'merge_resolution';
      operationId?: string;
      generation?: number;
      allowStates?: ThreadStatus[];
    },
  ): Promise<{ started: boolean; reason?: string; run?: ExecutionRun }> {
    if (this.isDisposed) return { started: false, reason: 'disposed' };
    const pendingBackoff = this.backoffTimers.get(threadId);
    if (pendingBackoff) {
      clearTimeout(pendingBackoff);
      this.backoffTimers.delete(threadId);
    }
    this.taskStore.ensureUpToDate();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) return { started: false, reason: 'not_found' };

    if (thread.execution?.version === 2) {
      return this.startExecutionV2(threadId, targetPhase, options);
    }

    const allowStates = options?.allowStates;
    const isAllowed = (s: ThreadStatus) => allowStates?.includes(s);

    if (thread.status === 'completed' || (thread.status === 'failed' && !isAllowed('failed'))) {
      return { started: false, reason: 'terminal' };
    }
    if (thread.status === 'paused' && !isAllowed('paused')) {
      return { started: false, reason: 'paused' };
    }
    if (thread.status === 'wait_decision' && !isAllowed('wait_decision')) {
      return { started: false, reason: 'wait_decision' };
    }

    // Budget exhaustion pre-check
    const limitMs = thread.execution?.budget?.limitMs ?? this.defaultBudgetLimitMs;
    const spentActiveMs = thread.execution?.budget?.spentActiveMs ?? 0;
    if (spentActiveMs >= limitMs) {
      return { started: false, reason: 'budget_exhausted' };
    }

    // Determine target phase
    const phase: ExecutionPhase =
      targetPhase ||
      thread.execution?.phase ||
      (thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge')
        ? 'merge'
        : thread.status === 'reviewing'
          ? 'review'
          : 'develop');

    // Prevent re-entrance if already running this exact task in this owner process
    if (this.activeExecutions.has(threadId)) {
      return { started: true, reason: 'already_running' };
    }

    // Check if an existing recorded run is still alive or unknown
    if (thread.execution?.run) {
      const probe = this.probeRun(threadId, thread.execution.run);
      if (probe === 'alive' || probe === 'unknown') {
        return { started: false, reason: 'previous_child_alive' };
      }
    }

    // Check dedicated reviewer bootstrap constraint before developer round
    if (phase === 'develop' && isReviewerBootstrapPending(thread)) {
      await this.setQueuedState(threadId, phase, 'role');
      return { started: false, reason: 'reviewer_bootstrap_pending' };
    }

    // Try to acquire prerequisites based on phase
    const policy = concurrencyPolicy();
    let roleLease: ReturnType<TaskRoleLock['tryAcquire']> = null;
    let sessionLease: NativeSessionLease | undefined;
    let mergeRelease: (() => void) | undefined;

    if (phase === 'develop') {
      roleLease = this.roleLock.tryAcquire(threadId, 'developer');
      if (!roleLease) {
        await this.setQueuedState(threadId, phase, 'role');
        return { started: false, reason: 'role_busy' };
      }
      if (thread.receiverSessionId) {
        sessionLease = this.sessionLock.acquire(thread.targetAgent, thread.receiverSessionId);
        if (!sessionLease) {
          this.roleLock.release(roleLease);
          await this.setQueuedState(threadId, phase, 'session');
          return { started: false, reason: 'session_busy' };
        }
      }
      if (policy.enabled) {
        const owners = this.executionRegistry.listAgentOwners();
        if (owners.length >= policy.maxConcurrentTasks) {
          this.cleanupAcquired(roleLease, sessionLease, mergeRelease);
          await this.setQueuedState(threadId, phase, 'capacity');
          return { started: false, reason: 'capacity_limit' };
        }
        const perAgent = owners.filter((o) => o.agent === thread!.targetAgent).length;
        if (perAgent >= policy.maxConcurrentPerAgent) {
          this.cleanupAcquired(roleLease, sessionLease, mergeRelease);
          await this.setQueuedState(threadId, phase, 'capacity');
          return { started: false, reason: 'capacity_limit_agent' };
        }
      }
    } else if (phase === 'review') {
      const reviewerSession = thread.reviewerSessionId || thread.callerSessionId;
      roleLease = this.roleLock.tryAcquire(threadId, 'reviewer', reviewerSession);
      if (!roleLease) {
        await this.setQueuedState(threadId, phase, 'role');
        return { started: false, reason: 'role_busy' };
      }
      if (reviewerSession) {
        sessionLease = this.sessionLock.acquire(thread.initiatorAgent, reviewerSession);
        if (!sessionLease) {
          this.roleLock.release(roleLease);
          await this.setQueuedState(threadId, phase, 'session');
          return { started: false, reason: 'session_busy' };
        }
      }
      if (policy.enabled) {
        const owners = this.executionRegistry.listAgentOwners();
        if (owners.length >= policy.maxConcurrentTasks) {
          this.cleanupAcquired(roleLease, sessionLease, mergeRelease);
          await this.setQueuedState(threadId, phase, 'capacity');
          return { started: false, reason: 'capacity_limit' };
        }
      }
    } else if (phase === 'merge') {
      // Check if merge lock is currently held by someone else before acquiring
      const ownerFile = `${this.mergeLock.lockPath}.owner.json`;
      if (fs.existsSync(ownerFile)) {
        try {
          const owner = JSON.parse(fs.readFileSync(ownerFile, 'utf8'));
          if (owner?.pid && (owner.birthId ? probeProcessIdentity(owner.pid, owner.birthId) === 'alive' : probeProcessIdentity(owner.pid) === 'alive')) {
            await this.setQueuedState(threadId, phase, 'merge_lock');
            return { started: false, reason: 'merge_lock_busy' };
          }
        } catch {}
      }
      try {
        mergeRelease = await this.mergeLock.acquire();
      } catch (err) {
        await this.setQueuedState(threadId, phase, 'merge_lock');
        return { started: false, reason: 'merge_lock_busy' };
      }
      if (!mergeRelease) {
        await this.setQueuedState(threadId, phase, 'merge_lock');
        return { started: false, reason: 'merge_lock_busy' };
      }
    }

    // We have acquired the required exclusive resources.
    // Atomically commit execution start in TaskStore with write-lock validation.
    const runId = crypto.randomUUID();
    const ownerIdentity: ProcessIdentity = {
      pid: process.pid,
      birthId: getProcessBirthId(process.pid),
    };

    let operationId: string = '';
    let generation: number = 1;

    try {
      thread = await this.transitionTask(threadId, (t) => {
        if (options?.allowStates ? !options.allowStates.includes(t.status) : (t.status === 'completed' || t.status === 'failed' || t.status === 'paused' || t.status === 'wait_decision')) {
          throw new Error(`Cannot start execution in state ${t.status}`);
        }
        if (t.execution?.run && (t.execution.run.state === 'starting' || t.execution.run.state === 'active')) {
          const p = this.probeRun(threadId, t.execution.run);
          if (p === 'alive' || p === 'unknown') {
            throw new Error('Concurrent start conflict: previous run alive');
          }
        }

        const existing = t.execution;
        generation = options?.generation ?? existing?.generation ?? 1;
        operationId =
          options?.operationId ||
          existing?.operationId ||
          `${phase}_${t.currentRound || 1}_${this.wallClockNow()}`;
        const currentAttempt = (existing?.attempt || 0) + 1;
        const autoRetriesUsed = existing?.autoRetriesUsed || 0;
        const curLimitMs = existing?.budget?.limitMs ?? this.defaultBudgetLimitMs;
        const curSpentActiveMs = existing?.budget?.spentActiveMs ?? 0;

        t.execution = {
          version: 1,
          revision: (existing?.revision || 0) + 1,
          operationId,
          phase,
          round: t.currentRound || 1,
          purpose: options?.purpose ?? existing?.purpose,
          generation,
          attempt: currentAttempt,
          autoRetriesUsed,
          budget: {
            limitMs: curLimitMs,
            spentActiveMs: curSpentActiveMs,
            maxAutoRetries: MAX_AUTO_RETRIES,
          },
          run: {
            id: runId,
            owner: ownerIdentity,
            step: 'starting',
            state: 'starting',
            startedAt: new Date().toISOString(),
          },
          queue: undefined,
        };
        t.status =
          phase === 'develop'
            ? 'running'
            : phase === 'review'
              ? 'reviewing'
              : 'merging';
        clearTaskFailure(t);
      });
    } catch (err) {
      this.cleanupAcquired(roleLease, sessionLease, mergeRelease);
      return { started: false, reason: `transition_failed: ${(err as Error)?.message || err}` };
    }

    // Register with execution registry for observation
    const registration = this.executionRegistry.begin(
      threadId,
      thread.currentRound || 1,
      phase === 'develop'
        ? thread.targetAgent
        : phase === 'review'
          ? thread.initiatorAgent
          : 'merge-git',
      thread.workspace?.executionPath,
      phase === 'develop' ? 'developer' : phase === 'review' ? 'reviewer' : undefined,
    );

    // Launch background worker for phase
    void this.executePhaseWorker({
      threadId,
      phase,
      runId,
      operationId,
      generation,
      ownerIdentity,
      roleLease,
      sessionLease,
      mergeRelease,
      registration,
      purpose: options?.purpose,
    }).catch((err) => {
      if (!this.isDisposed && this.taskStore.getTask(threadId)) {
        console.error(`[ExecutionController] executePhaseWorker unhandled error for ${threadId}:`, err);
      }
    });

    return { started: true };
  }

  /**
   * Primary entry point for v2 tasks: acquires unique run under short task write lock (TaskStore.transitionExecution),
   * verifies eligibility, cancellation, cleanup marker, valid round/phase, generation/operationId.
   * If run is occupied by starting, active, or stopping, refuses replacement run without mutating attempt or budget.
   * After acquiring starting run, acquires necessary resources, checks pre-spawn invalidation, records spawn intent,
   * and launches background worker with confirmed child binding handshake.
   */
  public async startExecutionV2(
    threadId: string,
    targetPhase?: ExecutionPhase,
    options?: {
      purpose?: 'bootstrap' | 'merge_resolution';
      operationId?: string;
      generation?: number;
      allowStates?: ThreadStatus[];
    },
  ): Promise<{ started: boolean; reason?: string; run?: ExecutionRun }> {
    if (this.isDisposed) return { started: false, reason: 'disposed' };
    const pendingBackoff = this.backoffTimers.get(threadId);
    if (pendingBackoff) {
      clearTimeout(pendingBackoff);
      this.backoffTimers.delete(threadId);
    }
    this.taskStore.ensureUpToDate();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) return { started: false, reason: 'not_found' };

    const eligibility = validateV2ExecutionEligibility(thread);
    if (!eligibility.eligible) {
      return { started: false, reason: eligibility.code };
    }

    const allowStates = options?.allowStates;
    const isAllowed = (s: ThreadStatus) => allowStates?.includes(s);

    if (thread.status === 'completed' || (thread.status === 'failed' && !isAllowed('failed'))) {
      return { started: false, reason: 'terminal' };
    }
    if (thread.status === 'paused' && !isAllowed('paused')) {
      return { started: false, reason: 'paused' };
    }
    if (thread.status === 'wait_decision' && !isAllowed('wait_decision')) {
      return { started: false, reason: 'wait_decision' };
    }
    if (thread.cleanup) {
      return { started: false, reason: 'cleanup_in_progress' };
    }

    // Budget exhaustion pre-check
    const limitMs = thread.execution?.budget?.limitMs ?? this.defaultBudgetLimitMs;
    const spentActiveMs = thread.execution?.budget?.spentActiveMs ?? 0;
    if (spentActiveMs >= limitMs) {
      return { started: false, reason: 'budget_exhausted' };
    }

    // Determine target phase and role
    const phase: ExecutionPhase =
      targetPhase ||
      thread.execution?.phase ||
      (thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge')
        ? 'merge'
        : thread.status === 'reviewing'
          ? 'review'
          : 'develop');
    const role: 'developer' | 'reviewer' = phase === 'develop' ? 'developer' : 'reviewer';

    // Dedicated reviewer bootstrap check
    if (phase === 'develop' && isReviewerBootstrapPending(thread)) {
      return { started: false, reason: 'reviewer_bootstrap_pending' };
    }

    // Check owner identity
    const ownerPid = process.pid;
    const ownerBirthId = getProcessBirthId(ownerPid);
    if (!ownerBirthId) {
      return { started: false, reason: 'OWNER_IDENTITY_UNVERIFIABLE' };
    }

    // Pre-check for existing uncleaned run
    const existingRun = thread.execution?.run;
    if (existingRun) {
      const isSameOwner =
        existingRun.owner?.pid === ownerPid &&
        existingRun.owner?.birthId === ownerBirthId;
      if (!isSameOwner) {
        // A competing controller only reports occupancy while the persisted owner
        // is alive. Recovery is allowed only for a launched-but-unregistered run
        // whose exact owner identity is definitively gone; safeToClean=false by
        // itself also describes a live owner in its normal starting/binding window.
        const recoverableLaunch = isUnconfirmedLaunchAfterOwnerExit(existingRun);
        if (recoverableLaunch && thread.failure?.code !== 'LAUNCH_UNCONFIRMED') {
          const evidence = `Owner ${existingRun.owner?.pid}/${existingRun.owner?.birthId} exited after spawn intent before child registration`;
          await recordLaunchRecoveryFailure(this.taskStore, threadId, existingRun.id, evidence, {
            run: existingRun,
            generation: thread.execution!.generation,
            operationId: thread.execution!.operationId,
            phase: thread.execution!.phase,
            round: thread.execution!.round,
          });
        }
        return { started: false, reason: 'run_busy', run: this.taskStore.getTask(threadId)?.execution?.run ?? existingRun };
      }
    }

    // Capture initial identity to ensure authoritative CAS matching
    const initialGeneration = options?.generation ?? thread.execution?.generation ?? 1;
    const initialOperationId = options?.operationId ?? thread.execution?.operationId;
    const initialPhase = phase;
    const initialRound = thread.execution?.round ?? thread.currentRound ?? 1;

    // Unique run acquisition under task short lock
    const runId = crypto.randomUUID();
    let isDuplicateRequest = false;
    let runBusyError: string | undefined;
    let finalRunId: string = runId;
    let finalGeneration = initialGeneration;
    let finalOperationId =
      initialOperationId ||
      `${phase}_${initialRound}_${this.wallClockNow()}`;
    let finalRound = initialRound;

    for (let attempt = 0; attempt < 3; attempt++) {
      this.taskStore.ensureUpToDate();
      const current = this.taskStore.getTask(threadId);
      if (!current) return { started: false, reason: 'not_found' };
      const rev = current.execution?.revision ?? 0;

      try {
        await this.taskStore.transitionExecution(threadId, rev, (t) => {
          const el = validateV2ExecutionEligibility(t);
          if (!el.eligible) throw new Error(`INELIGIBLE:${el.code}`);
          if (t.status === 'completed' || (t.status === 'failed' && !isAllowed('failed'))) {
            throw new Error('TERMINAL');
          }
          if (t.status === 'paused' && !isAllowed('paused')) {
            throw new Error('PAUSED');
          }
          if (t.status === 'wait_decision' && !isAllowed('wait_decision')) {
            throw new Error('WAIT_DECISION');
          }
          if (t.cleanup) {
            throw new Error('CLEANUP_IN_PROGRESS');
          }
          if (t.execution?.generation !== initialGeneration) {
            throw new Error(`GENERATION_MISMATCH: expected ${initialGeneration}, found ${t.execution?.generation}`);
          }
          if (initialOperationId && t.execution?.operationId !== initialOperationId) {
            throw new Error(`OPERATION_MISMATCH: expected ${initialOperationId}, found ${t.execution?.operationId}`);
          }
          if (t.execution?.phase && t.execution.phase !== initialPhase) {
            throw new Error(`PHASE_MISMATCH: expected ${initialPhase}, found ${t.execution.phase}`);
          }
          if (t.execution?.round && t.execution.round !== initialRound) {
            throw new Error(`ROUND_MISMATCH: expected ${initialRound}, found ${t.execution.round}`);
          }

          // Check for uncleaned run
          if (t.execution?.run) {
            if (
              t.execution.run.owner?.pid === ownerPid &&
              t.execution.run.owner?.birthId === ownerBirthId
            ) {
              isDuplicateRequest = true;
              finalRunId = t.execution.run.id;
              finalGeneration = t.execution.generation;
              finalOperationId = t.execution.operationId;
              finalRound = t.execution.round;
              return;
            }
            throw new Error(`RUN_BUSY:${t.execution.run.id}:${t.execution.run.state}`);
          }

          finalGeneration = initialGeneration;
          finalOperationId =
            initialOperationId ||
            `${phase}_${initialRound}_${this.wallClockNow()}`;
          finalRound = initialRound;
          const currentAttempt = (t.execution?.attempt || 0) + 1;
          const autoRetriesUsed = t.execution?.autoRetriesUsed || 0;
          const curLimitMs = t.execution?.budget?.limitMs ?? this.defaultBudgetLimitMs;
          const curSpentActiveMs = t.execution?.budget?.spentActiveMs ?? 0;

          t.execution = {
            version: 2,
            revision: rev + 1,
            operationId: finalOperationId,
            phase,
            round: finalRound,
            purpose: options?.purpose ?? t.execution?.purpose,
            generation: finalGeneration,
            attempt: currentAttempt,
            autoRetriesUsed,
            budget: {
              limitMs: curLimitMs,
              spentActiveMs: curSpentActiveMs,
              maxAutoRetries: MAX_AUTO_RETRIES,
            },
            run: {
              id: runId,
              role,
              owner: { pid: ownerPid, birthId: ownerBirthId },
              step: 'starting',
              state: 'starting',
              startedAt: new Date().toISOString(),
            },
            queue: undefined,
            next: t.execution?.next,
          };
          t.status =
            phase === 'develop'
              ? 'running'
              : phase === 'review'
                ? 'reviewing'
                : 'merging';
          clearTaskFailure(t);
        });
        break;
      } catch (err: any) {
        const msg = String(err?.message || err);
        if (msg.includes('RUN_BUSY')) {
          runBusyError = msg;
          break;
        }
        if (msg.includes('revision mismatch') && attempt < 2) {
          continue;
        }
        return { started: false, reason: msg };
      }
    }

    if (runBusyError) {
      const currentTask = this.taskStore.getTask(threadId);
      const currentExecution = currentTask?.execution;
      const currentRun = currentExecution?.run;
      if (
        currentRun &&
        currentExecution &&
        currentTask?.failure?.code !== 'LAUNCH_UNCONFIRMED' &&
        isUnconfirmedLaunchAfterOwnerExit(currentRun)
      ) {
        const evidence = `Owner ${currentRun.owner?.pid}/${currentRun.owner?.birthId} exited after spawn intent before child registration`;
        await recordLaunchRecoveryFailure(this.taskStore, threadId, currentRun.id, evidence, {
          run: currentRun,
          generation: currentExecution.generation,
          operationId: currentExecution.operationId,
          phase: currentExecution.phase,
          round: currentExecution.round,
        });
      }
      return { started: false, reason: 'run_busy', run: this.taskStore.getTask(threadId)?.execution?.run };
    }
    if (isDuplicateRequest) {
      return { started: true, reason: 'already_running', run: this.taskStore.getTask(threadId)?.execution?.run };
    }

    // Acquire prerequisites based on phase
    let sessionLease: NativeSessionLease | undefined;
    let mergeRelease: (() => void) | undefined;

    if (phase === 'develop') {
      if (thread.receiverSessionId) {
        sessionLease = this.sessionLock.acquire(thread.targetAgent, thread.receiverSessionId);
        if (!sessionLease) {
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'session');
          return { started: false, reason: 'session_busy' };
        }
      }
    } else if (phase === 'review') {
      const reviewerSession = thread.reviewerSessionId || thread.callerSessionId;
      if (reviewerSession) {
        sessionLease = this.sessionLock.acquire(thread.initiatorAgent, reviewerSession);
        if (!sessionLease) {
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'session');
          return { started: false, reason: 'session_busy' };
        }
      }
    } else if (phase === 'merge') {
      try {
        mergeRelease = await this.mergeLock.acquire();
      } catch {
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock');
        return { started: false, reason: 'merge_lock_busy' };
      }
      if (!mergeRelease) {
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock');
        return { started: false, reason: 'merge_lock_busy' };
      }
    }

    // Pre-spawn invalidation check: cancellation, pause, delete, cleanup, or identity replacement
    this.taskStore.ensureUpToDate();
    const check = this.taskStore.getTask(threadId);
    if (
      !check ||
      check.status === 'paused' ||
      check.cleanup ||
      check.archivedAt ||
      check.execution?.run?.id !== finalRunId
    ) {
      this.cleanupAcquired(null, sessionLease, mergeRelease);
      if (check?.execution?.run?.id === finalRunId && check.status !== 'paused' && !check.cleanup) {
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn');
      }
      return {
        started: false,
        reason: check?.cleanup ? 'cleanup_in_progress' : check?.status === 'paused' ? 'paused' : 'invalidated_before_spawn',
      };
    }

    // Persist spawn intent
    try {
      const rev = check.execution!.revision;
      await this.taskStore.transitionExecution(threadId, rev, (t) => {
        if (
          t.execution?.run?.id !== finalRunId ||
          t.execution.generation !== finalGeneration ||
          t.execution.operationId !== finalOperationId
        ) {
          throw new Error('STALE_SPAWN_INTENT');
        }
        if (t.status === 'paused' || t.cleanup) {
          throw new Error('INVALIDATED_BEFORE_SPAWN');
        }
        t.execution.run.step = 'spawning';
        t.execution.run.spawnIntentAt = new Date().toISOString();
      });
    } catch (spawnIntentErr: any) {
      this.cleanupAcquired(null, sessionLease, mergeRelease);
      return { started: false, reason: `spawn_intent_failed: ${spawnIntentErr?.message || spawnIntentErr}` };
    }

    // Register with execution registry for observation
    const registration = this.executionRegistry.begin(
      threadId,
      thread.currentRound || 1,
      phase === 'develop'
        ? thread.targetAgent
        : phase === 'review'
          ? thread.initiatorAgent
          : 'merge-git',
      thread.workspace?.executionPath,
      phase === 'develop' ? 'developer' : phase === 'review' ? 'reviewer' : undefined,
    );

    // Launch background worker for v2
    void this.executePhaseWorkerV2({
      threadId,
      phase,
      runId: finalRunId,
      operationId: finalOperationId,
      generation: finalGeneration,
      round: finalRound,
      ownerIdentity: { pid: ownerPid, birthId: ownerBirthId },
      role,
      sessionLease,
      mergeRelease,
      registration,
      purpose: options?.purpose,
    }).catch((err) => {
      if (!this.isDisposed && this.taskStore.getTask(threadId)) {
        console.error(`[ExecutionController] executePhaseWorkerV2 unhandled error for ${threadId}:`, err);
      }
    });

    return { started: true, run: this.taskStore.getTask(threadId)?.execution?.run };
  }

  private async releaseStartingRunV2(
    threadId: string,
    runId: string,
    operationId: string,
    generation: number,
    reason: 'session' | 'merge_lock' | 'capacity' | 'invalidated_before_spawn',
  ): Promise<void> {
    for (let attempt = 0; attempt < 3; attempt++) {
      this.taskStore.ensureUpToDate();
      const current = this.taskStore.getTask(threadId);
      if (!current) break;
      const rev = current.execution?.revision ?? 0;
      try {
        await this.taskStore.transitionExecution(threadId, rev, (t) => {
          if (
            t.execution?.run?.id === runId &&
            t.execution.generation === generation &&
            t.execution.operationId === operationId
          ) {
            // Restore attempt if released before spawn
            if (t.execution.attempt > 0) {
              t.execution.attempt -= 1;
            }
            if (t.execution.run.state !== 'stopping' && !t.execution.run.unconfirmed) {
              t.execution.run = undefined;
            }
            const isTerminalOrPaused =
              t.status === 'paused' ||
              t.status === 'completed' ||
              t.status === 'failed' ||
              t.status === 'wait_decision' ||
              Boolean(t.cleanup);
            if (!isTerminalOrPaused && reason !== 'invalidated_before_spawn') {
              t.status = 'queued';
              t.execution.queue = {
                reason: reason === 'session' ? 'session' : reason === 'merge_lock' ? 'merge_lock' : 'capacity',
                enteredAt: new Date().toISOString(),
              };
            }
          }
        });
        break;
      } catch (err: any) {
        if (String(err?.message || err).includes('revision mismatch') && attempt < 2) {
          continue;
        }
        break;
      }
    }
  }

  private async executePhaseWorkerV2(params: {
    threadId: string;
    phase: ExecutionPhase;
    runId: string;
    operationId: string;
    generation: number;
    round: number;
    ownerIdentity: ProcessIdentity;
    role: 'developer' | 'reviewer';
    sessionLease: NativeSessionLease | undefined;
    mergeRelease: (() => void) | undefined;
    registration: TaskExecutionRegistration;
    purpose?: 'bootstrap' | 'merge_resolution';
  }): Promise<void> {
    const { threadId, phase, runId, operationId, generation, round, role, registration } = params;
    let { sessionLease, mergeRelease } = params;
    let aborted = false;
    let abortReason = '';
    const abortFn = () => {
      aborted = true;
    };
    this.activeExecutions.set(threadId, { abort: abortFn, activeSinceMs: this.monotonicNow() });

    const guard = new HostSuspensionGuard({ monotonicNow: this.monotonicNow });
    guard.start();
    let lastMonotonic = this.monotonicNow();
    let accumulatedSpentMs = 0;
    type TrackedChild = ProcessIdentity & {
      process?: ChildProcess;
      descendants: ProcessIdentity[];
      belongsToSubtree: boolean;
      stopConfirmed: boolean;
    };
    const trackedChildren = new Map<string, TrackedChild>();
    let registrationUnconfirmed = false;
    let stopTrackedInFlight: Promise<boolean> | undefined;
    const processIdentityKey = (identity: ProcessIdentity) => `${identity.pid}:${identity.birthId ?? 'unknown'}`;
    const refreshDescendants = (tracked: TrackedChild) => {
      if (!tracked.belongsToSubtree || !tracked.birthId || probeProcessIdentity(tracked.pid, tracked.birthId) !== 'alive') return;
      const known = new Map(tracked.descendants.map((identity) => [processIdentityKey(identity), identity]));
      for (const identity of captureChildDescendants(tracked.pid, tracked.birthId)) {
        known.set(processIdentityKey(identity), identity);
      }
      tracked.descendants.splice(0, tracked.descendants.length, ...known.values());
    };
    const stopTrackedChildren = async (): Promise<boolean> => {
      if (stopTrackedInFlight) return stopTrackedInFlight;
      const operation = (async () => {
        let allStopped = trackedChildren.size > 0 || !registrationUnconfirmed;
        for (const tracked of trackedChildren.values()) {
          if (tracked.stopConfirmed) continue;
          if (!tracked.belongsToSubtree) {
            allStopped = false;
            continue;
          }
          refreshDescendants(tracked);
          let stopped = false;
          try {
            stopped = await stopVerifiedChildTree(
              tracked.pid,
              true,
              tracked.process,
              tracked.birthId,
              PROCESS_STOP_TIMEOUT_MS,
              tracked.descendants,
            );
          } catch {}
          if (stopped) tracked.stopConfirmed = true;
          else allStopped = false;
        }
        return allStopped;
      })();
      stopTrackedInFlight = operation;
      try {
        return await operation;
      } finally {
        if (stopTrackedInFlight === operation) stopTrackedInFlight = undefined;
      }
    };
    const checkpointTimer = setInterval(async () => {
      if (aborted || this.isDisposed) return;
      for (const tracked of trackedChildren.values()) refreshDescendants(tracked);
      const now = this.monotonicNow();
      guard.sync();
      const suspended = guard.suspendedWithin(lastMonotonic, now);
      const delta = Math.max(0, (now - lastMonotonic) - suspended);
      lastMonotonic = now;
      accumulatedSpentMs += delta;

      try {
        const t = this.taskStore.getTask(threadId);
        if (
          t?.execution &&
          t.execution.run?.id === runId &&
          t.execution.generation === generation &&
          t.execution.operationId === operationId
        ) {
          const newSpent = (t.execution.budget.spentActiveMs || 0) + accumulatedSpentMs;
          accumulatedSpentMs = 0;
          const rev = t.execution.revision;
          await this.taskStore.transitionExecution(threadId, rev, (item) => {
            if (
              item.execution?.run?.id === runId &&
              item.execution.generation === generation &&
              item.execution.operationId === operationId
            ) {
              item.execution.budget.spentActiveMs = newSpent;
            }
          });
          if (newSpent >= t.execution.budget.limitMs) {
            aborted = true;
            abortReason = 'TIMEOUT: Active execution budget exceeded (120m)';
            if (trackedChildren.size) {
              void stopTrackedChildren().then((stopped) => {
                if (!stopped) registrationUnconfirmed = true;
              });
            }
          }
        }
      } catch {}
    }, this.checkpointIntervalMs);
    checkpointTimer.unref?.();

    let phaseResult:
      | { ok: true; verdict?: string }
      | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean }
      | undefined;

    const childBindingPromises = new Map<string, Promise<void>>();
    const bindChildPid = async (
      pid: number,
      childProcess: ChildProcess | undefined,
      childBirthId: string | undefined,
      trackedKey: string,
    ): Promise<void> => {
      // Verify that this is the runner's actual child and capture its stable
      // identity before gathering any subtree evidence. A rejected candidate
      // must never contribute PIDs to the stop set.
      const candidateBirthId = childBirthId?.trim() || undefined;
      const belongsToSubtree = verifySubtreeOwnership(pid, childProcess, undefined, candidateBirthId);
      const descendants = belongsToSubtree && candidateBirthId
        ? captureChildDescendants(pid, candidateBirthId)
        : [];
      const tracked: TrackedChild = {
        pid,
        birthId: candidateBirthId,
        process: childProcess,
        descendants,
        belongsToSubtree,
        stopConfirmed: false,
      };
      trackedChildren.set(trackedKey, tracked);

      const reg = await registerRunChildV2({
        taskStore: this.taskStore,
        threadId,
        identity: {
          runId,
          operationId,
          generation,
          phase,
          round,
          role,
        },
        childPid: pid,
        childBirthId: candidateBirthId,
        childDescendants: tracked.descendants,
        childProcess,
        attachLeases: async (p) => {
          if (sessionLease) this.sessionLock.attachChild(sessionLease, p);
          if (mergeRelease) await this.mergeLock.attachChildPid(p);
          this.executionRegistry.attachChildPid(p, registration);
        },
      });

      if (reg.alreadyRegistered) {
        if (trackedChildren.get(trackedKey) === tracked) trackedChildren.delete(trackedKey);
        return;
      }
      if (reg.inProgress) {
        const currentChild = this.taskStore.getTask(threadId)?.execution?.run?.child;
        const sameOwnedChild = currentChild?.pid === tracked.pid && currentChild.birthId === tracked.birthId;
        if (sameOwnedChild) {
          if (trackedChildren.get(trackedKey) === tracked) trackedChildren.delete(trackedKey);
        } else if (tracked.belongsToSubtree && tracked.birthId) {
          let stopped = false;
          try {
            stopped = await stopVerifiedChildTree(
              tracked.pid,
              true,
              tracked.process,
              tracked.birthId,
              PROCESS_STOP_TIMEOUT_MS,
              tracked.descendants,
            );
          } catch {}
          if (stopped) tracked.stopConfirmed = true;
          else registrationUnconfirmed = true;
        } else {
          registrationUnconfirmed = true;
        }
        const error: any = new Error(reg.error || 'Child binding already has an owner');
        error.code = 'CHILD_BINDING_IN_PROGRESS';
        throw error;
      }
      if (!reg.success) {
        registrationUnconfirmed ||= Boolean(reg.unconfirmed);
        throw new Error(reg.error || 'Child process registration failed');
      }
      tracked.birthId = candidateBirthId;
    };

    const confirmedOnChildPid = (
      pid: number | undefined,
      childProcess?: ChildProcess,
      childBirthId?: string,
    ): Promise<void> => {
      if (pid === undefined) {
        // A close notification is not a binding operation. Final cleanup owns
        // stopping and unbinding after the original registration promise settles.
        return Promise.resolve();
      }
      const key = `${pid}:${childBirthId?.trim() || 'missing'}`;
      const existing = childBindingPromises.get(key);
      if (existing) return existing;
      // Only a duplicate with the same immutable identity may share this ACK
      // Promise. A different PID/birthId must take its own rejection/stop path.
      const pending = bindChildPid(pid, childProcess, childBirthId, key);
      childBindingPromises.set(key, pending);
      void pending.catch(() => undefined);
      return pending;
    };

    try {
      if (phase === 'develop') {
        phaseResult = await this.executeDevelopAdapter(
          threadId,
          runId,
          sessionLease,
          confirmedOnChildPid,
          { operationId, generation, round },
        );
      } else if (phase === 'review') {
        phaseResult = await this.executeReviewAdapter(
          threadId,
          runId,
          params.purpose,
          confirmedOnChildPid,
          { operationId, generation, round },
        );
      } else if (phase === 'merge') {
        phaseResult = await this.executeMergeAdapter(
          threadId,
          runId,
          params.purpose,
          confirmedOnChildPid,
          mergeRelease,
          { operationId, generation, round },
        );
      }

      guard.sync();
      const postNow = this.monotonicNow();
      const suspendedPost = guard.suspendedWithin(lastMonotonic, postNow);
      const delta = Math.max(0, (postNow - lastMonotonic) - suspendedPost);
      accumulatedSpentMs += delta;
      try {
        const t = this.taskStore.getTask(threadId);
        if (
          t?.execution &&
          t.execution.run?.id === runId &&
          t.execution.generation === generation &&
          t.execution.operationId === operationId
        ) {
          const finalSpent = (t.execution.budget.spentActiveMs || 0) + accumulatedSpentMs;
          accumulatedSpentMs = 0;
          const rev = t.execution.revision;
          await this.taskStore.transitionExecution(threadId, rev, (item) => {
            if (
              item.execution?.run?.id === runId &&
              item.execution.generation === generation &&
              item.execution.operationId === operationId
            ) {
              item.execution.budget.spentActiveMs = finalSpent;
            }
          });
          if (finalSpent >= t.execution.budget.limitMs) {
            aborted = true;
            abortReason = 'TIMEOUT: Active execution budget exceeded (120m)';
          }
        }
      } catch {}
    } catch (err: any) {
      phaseResult = {
        ok: false,
        code: 'EXECUTION_ERROR',
        reason: (err as Error)?.message || String(err),
      };
    } finally {
      clearInterval(checkpointTimer);
      guard.stop();

      let unconfirmedStop = registrationUnconfirmed && trackedChildren.size === 0;
      if (!unconfirmedStop && trackedChildren.size > 0) {
        try {
          unconfirmedStop = !(await stopTrackedChildren());
        } catch {
          unconfirmedStop = true;
        }
      }
      if (!unconfirmedStop) {
        try {
          // Unbind the session and clear registry PID only after every captured
          // process identity has been confirmed stopped. MeshFileLock's PID-less
          // attach is a no-op; its helper exit is the actual release ACK.
          if (sessionLease && !this.sessionLock.attachChild(sessionLease, undefined)) {
            throw new Error('Native-session child unbinding was not acknowledged.');
          }
          this.executionRegistry.attachChildPid(undefined, registration);
        } catch {
          unconfirmedStop = true;
        }
      }

      if (!unconfirmedStop && mergeRelease) {
        try {
          const mergeReleased = await this.mergeLock.releaseAndConfirm(mergeRelease, PROCESS_STOP_TIMEOUT_MS);
          if (!mergeReleased) unconfirmedStop = true;
          else mergeRelease = undefined;
        } catch {
          unconfirmedStop = true;
        }
      }
      if (!unconfirmedStop && sessionLease) {
        try {
          const sessionReleased = this.sessionLock.release(sessionLease);
          if (!sessionReleased) unconfirmedStop = true;
          else sessionLease = undefined;
        } catch {
          unconfirmedStop = true;
        }
      }

      if (unconfirmedStop) {
        try {
          const rev = this.taskStore.getTask(threadId)?.execution?.revision ?? 0;
          await this.taskStore.transitionExecution(threadId, rev, (t) => {
            if (t.execution?.run?.id === runId) {
              t.execution.run.state = 'stopping';
              t.execution.run.unconfirmed = true;
              appendInterruption(t, {
                kind: 'error',
                reason: 'PROCESS_STOP_UNCONFIRMED',
                at: new Date().toISOString(),
                by: 'execution_controller',
              });
            }
          });
        } catch {}
        return;
      }

      this.activeExecutions.delete(threadId);

      try {
        this.executionRegistry.end(registration);
      } catch {}
    }

    if (aborted && abortReason.includes('TIMEOUT')) {
      phaseResult = {
        ok: false,
        code: 'TIMEOUT',
        reason: abortReason,
        transient: false,
      };
    }

    if (!this.isDisposed && phaseResult) {
      try {
        await this.handlePhaseOutcome(threadId, phase, runId, phaseResult, { operationId, generation, round });
      } catch (err) {
        if (!this.isDisposed && this.taskStore.getTask(threadId)) {
          console.error(`[ExecutionController] handlePhaseOutcome error for ${threadId}:`, err);
        }
      }
    }
  }

  private cleanupAcquired(
    roleLease: ReturnType<TaskRoleLock['tryAcquire']>,
    sessionLease: NativeSessionLease | undefined,
    mergeRelease: (() => void) | undefined,
  ): void {
    if (roleLease) this.roleLock.release(roleLease);
    if (sessionLease) this.sessionLock.release(sessionLease);
    if (mergeRelease) mergeRelease();
  }

  private async setQueuedState(
    threadId: string,
    phase: ExecutionPhase,
    reason: 'role' | 'session' | 'merge_lock' | 'capacity' | 'backoff' | 'quota',
    notBefore?: string,
  ): Promise<void> {
    try {
      await this.transitionTask(threadId, (t) => {
        const existing = t.execution;
        let retainedRun = existing?.run?.state === 'stopping' ? existing.run : undefined;
        if (!retainedRun && existing?.run) {
          const probe = this.probeRun(threadId, existing.run);
          if (probe === 'alive' || probe === 'unknown') {
            retainedRun = existing.run;
          }
        }
        t.execution = {
          version: 1,
          revision: (existing?.revision || 0) + 1,
          operationId: existing?.operationId || `${phase}_${t.currentRound || 1}_${this.wallClockNow()}`,
          phase,
          round: t.currentRound || 1,
          generation: existing?.generation || 1,
          attempt: existing?.attempt || 0,
          autoRetriesUsed: existing?.autoRetriesUsed || 0,
          budget: existing?.budget || {
            limitMs: this.defaultBudgetLimitMs,
            spentActiveMs: 0,
            maxAutoRetries: MAX_AUTO_RETRIES,
          },
          run: retainedRun,
          queue: { reason, notBefore },
        };
        t.status = 'queued';
      });
    } catch {}
  }

  /**
   * Internal worker executing a phase attempt under unified budget and observation.
   */
  private async executePhaseWorker(params: {
    threadId: string;
    phase: ExecutionPhase;
    runId: string;
    operationId?: string;
    generation?: number;
    ownerIdentity: ProcessIdentity;
    roleLease: ReturnType<TaskRoleLock['tryAcquire']>;
    sessionLease: NativeSessionLease | undefined;
    mergeRelease: (() => void) | undefined;
    registration: TaskExecutionRegistration;
    purpose?: 'bootstrap' | 'merge_resolution';
  }): Promise<void> {
    const { threadId, phase, runId, operationId, generation, registration } = params;
    let { roleLease, sessionLease, mergeRelease } = params;
    let aborted = false;
    let abortReason = '';
    const abortFn = () => {
      aborted = true;
    };
    this.activeExecutions.set(threadId, { abort: abortFn, activeSinceMs: this.monotonicNow() });

    const guard = new HostSuspensionGuard({ monotonicNow: this.monotonicNow });
    guard.start();
    let childPid: number | undefined;
    let childBirthId: string | undefined;

    // Checkpoint timer: updates active spent time every 5 seconds
    let lastMonotonic = this.monotonicNow();
    let accumulatedSpentMs = 0;
    const checkpointTimer = setInterval(async () => {
      if (aborted || this.isDisposed) return;
      const now = this.monotonicNow();
      guard.sync();
      const suspended = guard.suspendedWithin(lastMonotonic, now);
      const delta = Math.max(0, (now - lastMonotonic) - suspended);
      lastMonotonic = now;
      accumulatedSpentMs += delta;

      // Update checkpoint on task
      try {
        const t = this.taskStore.getTask(threadId);
        if (t?.execution && t.execution.run?.id === runId) {
          const newSpent = (t.execution.budget.spentActiveMs || 0) + accumulatedSpentMs;
          accumulatedSpentMs = 0;
          await this.transitionTask(threadId, (item) => {
            if (item.execution?.run?.id === runId) {
              item.execution.budget.spentActiveMs = newSpent;
            }
          });
          // Check timeout
          if (newSpent >= t.execution.budget.limitMs) {
            aborted = true;
            abortReason = 'TIMEOUT: Active execution budget exceeded (120m)';
            if (childPid && childBirthId && probeProcessIdentity(childPid, childBirthId) === 'alive') {
              killPidTree(childPid);
            }
          }
        }
      } catch {}
    }, this.checkpointIntervalMs);
    checkpointTimer.unref?.();

    let phaseResult:
      | { ok: true; verdict?: string }
      | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean }
      | undefined;

    try {
      // Execute the adapter for this phase
      if (phase === 'develop') {
        phaseResult = await this.executeDevelopAdapter(
          threadId,
          runId,
          sessionLease,
          (pid) => {
            childPid = pid;
            if (pid) {
              childBirthId = getProcessBirthId(pid);
              this.executionRegistry.attachChildPid(pid, registration);
              // Native-session preparation checks the child binding on the role lease.
              this.roleLock.attachChild(roleLease, pid);
              void this.updateRunChild(threadId, runId, pid, childBirthId);
            } else {
              this.executionRegistry.attachChildPid(undefined, registration);
              this.roleLock.attachChild(roleLease, undefined);
            }
          },
          { operationId, generation },
        );
      } else if (phase === 'review') {
        phaseResult = await this.executeReviewAdapter(threadId, runId, params.purpose, (pid) => {
          childPid = pid;
          if (pid) {
            childBirthId = getProcessBirthId(pid);
            this.executionRegistry.attachChildPid(pid, registration);
            this.roleLock.attachChild(roleLease, pid);
            void this.updateRunChild(threadId, runId, pid, childBirthId);
          } else {
            this.executionRegistry.attachChildPid(undefined, registration);
            this.roleLock.attachChild(roleLease, undefined);
          }
        });
      } else if (phase === 'merge') {
        phaseResult = await this.executeMergeAdapter(
          threadId,
          runId,
          params.purpose,
          (pid) => {
            childPid = pid;
            if (pid) {
              childBirthId = getProcessBirthId(pid);
              this.executionRegistry.attachChildPid(pid, registration);
              void this.mergeLock.attachChildPid(pid);
              void this.updateRunChild(threadId, runId, pid, childBirthId);
            } else {
              this.executionRegistry.attachChildPid(undefined, registration);
              void this.mergeLock.attachChildPid(undefined);
            }
          },
          mergeRelease,
        );
      }

      // Check spent active time post-execution
      guard.sync();
      const postNow = this.monotonicNow();
      const suspendedPost = guard.suspendedWithin(lastMonotonic, postNow);
      const delta = Math.max(0, (postNow - lastMonotonic) - suspendedPost);
      accumulatedSpentMs += delta;
      try {
        const t = this.taskStore.getTask(threadId);
        if (t?.execution && t.execution.run?.id === runId) {
          const finalSpent = (t.execution.budget.spentActiveMs || 0) + accumulatedSpentMs;
          accumulatedSpentMs = 0;
          await this.transitionTask(threadId, (item) => {
            if (item.execution?.run?.id === runId) {
              item.execution.budget.spentActiveMs = finalSpent;
            }
          });
          if (finalSpent >= t.execution.budget.limitMs) {
            aborted = true;
            abortReason = 'TIMEOUT: Active execution budget exceeded (120m)';
          }
        }
      } catch {}
    } catch (err) {
      phaseResult = {
        ok: false,
        code: 'EXECUTION_ERROR',
        reason: (err as Error)?.message || String(err),
      };
    } finally {
      clearInterval(checkpointTimer);
      guard.stop();

      let unconfirmedStop = false;
      if (childPid && childPid !== process.pid) {
        try {
          const isAlive = probeProcessIdentity(childPid) !== 'exited';
          if (isAlive) {
            if (!childBirthId) {
              unconfirmedStop = true;
            } else {
              const probe = probeProcessIdentity(childPid, childBirthId);
              if (probe === 'alive') {
                killPidTree(childPid);
                const exited = await confirmProcessTreeExit(childPid, PROCESS_STOP_TIMEOUT_MS, childBirthId);
                if (!exited) {
                  unconfirmedStop = true;
                }
              } else if (probe === 'unknown') {
                unconfirmedStop = true;
              }
            }
          }
        } catch {
          unconfirmedStop = true;
        }
      }

      if (unconfirmedStop) {
        try {
          await this.transitionTask(threadId, (t) => {
            appendInterruption(t, {
              kind: 'error',
              reason: 'PROCESS_STOP_UNCONFIRMED',
              at: new Date().toISOString(),
              by: 'execution_controller',
            });
            if (t.execution?.run) {
              t.execution.run.state = 'stopping';
            }
          });
        } catch {}
        // Do NOT release locks; retain stopping state and active execution
        return;
      }

      this.activeExecutions.delete(threadId);

      // End execution registry entry
      try {
        this.executionRegistry.end(registration);
      } catch {}

      // Release locks
      if (roleLease) {
        try {
          this.roleLock.release(roleLease);
        } catch {}
        roleLease = null;
      }
      if (sessionLease) {
        try {
          this.sessionLock.release(sessionLease);
        } catch {}
        sessionLease = undefined;
      }
      if (mergeRelease) {
        try {
          mergeRelease();
        } catch {}
        mergeRelease = undefined;
      }
    }

    if (aborted && abortReason.includes('TIMEOUT')) {
      phaseResult = {
        ok: false,
        code: 'TIMEOUT',
        reason: abortReason,
        transient: false,
      };
    }

    // Process outcome & classify NextAction
    if (!this.isDisposed && phaseResult) {
      try {
        await this.handlePhaseOutcome(threadId, phase, runId, phaseResult, { operationId, generation });
      } catch (err) {
        if (!this.isDisposed && this.taskStore.getTask(threadId)) {
          console.error(`[ExecutionController] handlePhaseOutcome error for ${threadId}:`, err);
        }
      }
    }
  }

  public async updateRunChild(
    threadId: string,
    runId: string,
    childPid: number,
    childBirthId?: string,
  ): Promise<void> {
    try {
      await this.transitionTask(threadId, (t) => {
        if (t.execution?.run?.id === runId) {
          t.execution.run.child = {
            pid: childPid,
            birthId: childBirthId,
          };
          t.execution.run.state = 'active';
          t.execution.run.step = 'executing';
        }
      });
    } catch {}
  }

  /**
    * Adapter for phase: develop
    */
  private async executeDevelopAdapter(
    threadId: string,
    runId: string,
    sessionLease: NativeSessionLease | undefined,
    onChildPid: (pid: number | undefined, childProcess?: ChildProcess, childBirthId?: string) => void | Promise<void>,
    identity?: { operationId?: string; generation?: number; round?: number },
  ): Promise<
    | { ok: true }
    | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean }
  > {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) return { ok: false, code: 'NOT_FOUND', reason: 'Task not found' };

    // Pre-spawn invalidation check: pause, cleanup, cancellation, stopping, or identity replacement
    if (thread.execution?.version === 2 && identity) {
      if (
        !thread.execution?.run ||
        thread.execution.run.id !== runId ||
        thread.execution.run.state === 'stopping' ||
        thread.execution.run.unconfirmed ||
        thread.status === 'paused' ||
        thread.status === 'completed' ||
        thread.status === 'failed' ||
        Boolean(thread.cleanup) ||
        (identity?.generation !== undefined && thread.execution.generation !== identity.generation) ||
        (identity?.operationId !== undefined && thread.execution.operationId !== identity.operationId) ||
        (identity?.round !== undefined && thread.execution.round !== identity.round)
      ) {
        return {
          ok: false,
          code: 'INVALIDATED_BEFORE_SPAWN',
          reason: thread.cleanup
            ? `Task reserved for cleanup (${thread.cleanup.action})`
            : thread.status === 'paused'
              ? 'Task was paused before spawn'
              : 'Execution identity invalidated before spawn',
        };
      }
    }

    const round = findInFlightRound(thread) || thread.rounds[thread.rounds.length - 1];
    if (!round) return { ok: false, code: 'NO_ROUND', reason: 'No round to execute' };

    const isRework = shouldResumeNativeSession(thread, round);
    let promptText = round.prompt;
    if (isRework) {
      promptText = appendCompactionProtocol(promptText, round.round);
    }

    const limitMs = thread.execution?.budget?.limitMs ?? this.defaultBudgetLimitMs;
    const spentActiveMs = thread.execution?.budget?.spentActiveMs ?? 0;
    const remainingMs = Math.max(1000, limitMs - spentActiveMs);

    // Run execution with runner maxRetries = 0 (controller owns retries)
    const result = await this.runner.execute({
      taskThreadId: threadId,
      agent: thread.targetAgent,
      prompt: promptText,
      receiverSessionId: thread.receiverSessionId || '',
      isRework,
      cwd: thread.workspace?.executionPath,
      model: thread.targetModel,
      taskRound: round.round,
      timeoutMs: remainingMs,
      previousTokenUsage: round.tokenUsage,
      sessionLease,
      maxRetries: 0,
      onChildPid: (pid, childProc, childBirthId) => {
        return onChildPid(pid, childProc, childBirthId);
      },
      preSpawnGuard: () => {
        this.taskStore.ensureUpToDate();
        const t = this.taskStore.getTask(threadId);
        if (!t || !t.execution?.run || t.execution.run.id !== runId) {
          return { ok: false, reason: 'Execution run invalidated before spawn' };
        }
        if (
          t.status !== 'running' ||
          Boolean(t.cleanup) ||
          t.execution.run.state === 'stopping' ||
          t.execution.run.unconfirmed ||
          (identity?.generation !== undefined && t.execution.generation !== identity.generation) ||
          (identity?.operationId !== undefined && t.execution.operationId !== identity.operationId) ||
          (identity?.round !== undefined && t.execution.round !== identity.round)
        ) {
          return {
            ok: false,
            reason: t.cleanup
              ? `Task reserved for cleanup (${t.cleanup.action})`
              : t.status === 'paused'
                ? 'Task was paused before spawn'
                : t.status === 'wait_decision'
                  ? 'Task entered wait_decision before spawn'
                  : 'Execution identity invalidated before spawn',
          };
        }
        return { ok: true };
      },
    });

    // Check assessment
    const assessment = result.assessment;
    const actualOutput = result.extractedOutput || '';
    const success = Boolean(assessment?.success && assessment.stream?.hasActualOutput);

    // Save outputs on task
    await this.transitionTask(threadId, (t) => {
      if (!t.execution?.run || t.execution.run.id !== runId) {
        throw new Error(`Stale develop run write rejected: expected run ${t.execution?.run?.id}, got ${runId}`);
      }
      if (t.execution.run.state === 'stopping' || t.execution.run.unconfirmed) {
        throw new Error(`Stopping develop run write rejected for ${runId}`);
      }
      if (identity?.generation !== undefined && t.execution.generation !== identity.generation) {
        throw new Error(`Stale develop generation write rejected: expected ${t.execution.generation}, got ${identity.generation}`);
      }
      if (identity?.operationId !== undefined && t.execution.operationId !== identity.operationId) {
        throw new Error(`Stale develop operationId write rejected: expected ${t.execution.operationId}, got ${identity.operationId}`);
      }
      if (identity?.round !== undefined && t.execution.round !== identity.round) {
        throw new Error(`Stale develop round write rejected: expected ${t.execution.round}, got ${identity.round}`);
      }
      const activeRound = t.rounds.find((r) => r.round === round.round) || t.rounds[t.rounds.length - 1];
      if (activeRound) {
        activeRound.output = actualOutput;
        activeRound.durationMs = result.durationMs || 0;
        activeRound.status = success ? 'completed' : 'failed';
        activeRound.assessment = assessment;
        activeRound.tokenUsage = result.tokenUsage;
        activeRound.executorAgent = t.targetAgent;
        activeRound.executorModel = t.targetModel;
      }
      if (result.actualSessionId) {
        t.receiverSessionId = result.actualSessionId;
        t.receiverSessionCaptured = true;
      }
      if (t.execution?.run?.id === runId) {
        t.execution.lastOutcome = {
          code: assessment?.errorCode || (success ? 'OK' : 'EXECUTION_FAILED'),
          reason: result.error || (success ? 'Success' : 'Execution failed'),
          at: new Date().toISOString(),
        };
      }
    });

    if (success) {
      return { ok: true };
    }

    const errorText = `${result.error || ''}\n${result.stderr || ''}`;
    let quotaResetsAt: string | undefined;
    const isRateLimit =
      (typeof this.runner.isRateLimitOrQuotaError === 'function' && this.runner.isRateLimitOrQuotaError(errorText)) ||
      assessment?.errorCode === 'RATE_LIMITED';
    if (isRateLimit) {
      const info = (this.runner as any).detector?.getRateLimitTracker?.()?.getRateLimit?.(thread.targetAgent);
      if (info?.resetsAt) {
        quotaResetsAt = info.resetsAt;
      }
    }

    const errorCode = assessment?.errorCode || (result.exitCode !== 0 ? 'NONZERO_EXIT' : 'EMPTY_OUTPUT');
    const isTransient = typeof this.runner.isTransientError === 'function' ? this.runner.isTransientError(result) : false;
    const nonTransientCodes = new Set(['EMPTY_OUTPUT', 'TIMEOUT', 'UNKNOWN']);

    return {
      ok: false,
      code: errorCode,
      reason: result.error || `Execution failed with ${errorCode}`,
      stage: 'execution',
      quotaResetsAt,
      transient: isTransient && !nonTransientCodes.has(errorCode),
    };
  }

  /**
   * Adapter for phase: review
   */
  private async executeReviewAdapter(
    threadId: string,
    runId: string,
    purpose?: 'bootstrap' | 'merge_resolution',
    onChildPid?: (pid: number | undefined, childProcess?: ChildProcess, childBirthId?: string) => void | Promise<void>,
    identity?: { operationId?: string; generation?: number; round?: number },
  ): Promise<
    | { ok: true; verdict?: 'bootstrap' | 'approve' | 'rework' | 'wait_decision' | 'completed' }
    | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean }
  > {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) return { ok: false, code: 'NOT_FOUND', reason: 'Task not found' };

    // Pre-spawn invalidation check
    if (thread.execution?.version === 2 && identity) {
      if (
        !thread.execution?.run ||
        thread.execution.run.id !== runId ||
        thread.execution.run.state === 'stopping' ||
        thread.execution.run.unconfirmed ||
        thread.status === 'paused' ||
        thread.status === 'completed' ||
        thread.status === 'failed' ||
        Boolean(thread.cleanup) ||
        (identity?.generation !== undefined && thread.execution.generation !== identity.generation) ||
        (identity?.operationId !== undefined && thread.execution.operationId !== identity.operationId) ||
        (identity?.round !== undefined && thread.execution.round !== identity.round)
      ) {
        return {
          ok: false,
          code: 'INVALIDATED_BEFORE_SPAWN',
          reason: thread.cleanup
            ? `Task reserved for cleanup (${thread.cleanup.action})`
            : thread.status === 'paused'
              ? 'Task was paused before spawn'
              : 'Execution identity invalidated before spawn',
        };
      }
    }

    // Check if dedicated reviewer bootstrap
    if (purpose === 'bootstrap' || (thread.reviewFlow === 'dedicated' && thread.reviewBootstrap?.phase !== 'confirmed')) {
      const bootstrapOutcome = await this.callbackManager.launchReviewerBootstrap(thread, onChildPid);
      if (bootstrapOutcome.confirmed) {
        return { ok: true, verdict: 'bootstrap' };
      }
      return {
        ok: false,
        code: 'BOOTSTRAP_FAILED',
        reason: bootstrapOutcome.error || 'Reviewer bootstrap failed to confirm dispatch_task',
        stage: 'bootstrap',
        transient: bootstrapOutcome.transient,
      };
    }

    // Normal review turn
    const baselineApprovedCommit = thread.worktree?.approvedCommit;
    const baselineRoundsCount = thread.rounds.length;
    const baselineDecisionsCount = thread.decisions?.length || 0;

    const reviewRound = [...(thread.rounds || [])].reverse().find((r) => r.type !== 'approval') || thread.rounds[0];
    const wakeResult = await this.callbackManager.launchCaller(thread, reviewRound, onChildPid);

    // Re-read task to observe durable verdict
    const freshThread = this.taskStore.getTask(threadId);
    if (!freshThread) return { ok: false, code: 'NOT_FOUND', reason: 'Task disappeared' };

    if (freshThread.worktree?.approvedCommit && freshThread.worktree.approvedCommit !== baselineApprovedCommit) {
      return { ok: true, verdict: 'approve' };
    }
    if (freshThread.rounds.length > baselineRoundsCount || freshThread.status === 'running') {
      return { ok: true, verdict: 'rework' };
    }
    if ((freshThread.decisions?.length || 0) > baselineDecisionsCount || freshThread.status === 'wait_decision') {
      return { ok: true, verdict: 'wait_decision' };
    }
    if (freshThread.status === 'merging' || (freshThread.status === 'queued' && freshThread.queuePhase === 'merge')) {
      return { ok: true, verdict: 'approve' };
    }
    if (freshThread.status === 'completed') {
      return { ok: true, verdict: 'completed' };
    }
    if (freshThread.reviewBlocked !== undefined) {
      return {
        ok: false,
        code: 'REVIEW_BLOCKED',
        reason: 'Reviewer blocked task',
        stage: 'review',
        transient: false,
      };
    }

    // Process exited with no verdict tool called -> NO_VERDICT
    return {
      ok: false,
      code: 'NO_VERDICT',
      reason:
        wakeResult.error ||
        'Reviewer turn ended without calling approve_task, request_rework, or request_task_decision',
      stage: 'review',
      transient: false,
    };
  }

  /**
   * Adapter for phase: merge
   */
  private async executeMergeAdapter(
    threadId: string,
    runId: string,
    purpose?: 'bootstrap' | 'merge_resolution',
    onChildPid?: (pid: number | undefined, childProcess?: ChildProcess, childBirthId?: string) => void | Promise<void>,
    mergeRelease?: () => void,
    identity?: { operationId?: string; generation?: number; round?: number },
  ): Promise<
    | { ok: true }
    | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean }
  > {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) return { ok: false, code: 'NOT_FOUND', reason: 'Task not found' };

    // Pre-spawn invalidation check
    if (thread.execution?.version === 2 && identity) {
      if (
        !thread.execution?.run ||
        thread.execution.run.id !== runId ||
        thread.execution.run.state === 'stopping' ||
        thread.execution.run.unconfirmed ||
        thread.status === 'paused' ||
        thread.status === 'completed' ||
        thread.status === 'failed' ||
        Boolean(thread.cleanup) ||
        (identity?.generation !== undefined && thread.execution.generation !== identity.generation) ||
        (identity?.operationId !== undefined && thread.execution.operationId !== identity.operationId) ||
        (identity?.round !== undefined && thread.execution.round !== identity.round)
      ) {
        return {
          ok: false,
          code: 'INVALIDATED_BEFORE_SPAWN',
          reason: thread.cleanup
            ? `Task reserved for cleanup (${thread.cleanup.action})`
            : thread.status === 'paused'
              ? 'Task was paused before spawn'
              : 'Execution identity invalidated before spawn',
        };
      }
    }

    try {
      const mergeResult = await this.mergeCoordinator.mergeApprovedAttempt(threadId, onChildPid, mergeRelease);
      if (mergeResult.status === 'merged' || mergeResult.status === 'no_changes') {
        return { ok: true };
      }

      if (mergeResult.status === 'stale' || mergeResult.status === 'conflict') {
        return {
          ok: false,
          code: 'CAS_CONFLICT',
          reason: mergeResult.message || 'Target advanced or conflict detected',
          stage: 'merge',
          transient: true,
        };
      }

      return {
        ok: false,
        code: mergeResult.status.toUpperCase(),
        reason: mergeResult.message || `Merge failed with status ${mergeResult.status}`,
        stage: 'merge',
        transient: false,
      };
    } catch (err: any) {
      if (await this.mergeCoordinator.hasLandedCandidate(threadId)) {
        for (let retry = 0; retry < 3; retry++) {
          try {
            await this.transitionTask(threadId, (t) => {
              t.status = 'completed';
              if (t.worktree?.integration) {
                t.worktree.integration.phase = 'merged';
              }
            });
            return { ok: true };
          } catch {
            await new Promise((r) => setTimeout(r, 100));
          }
        }
      }
      return {
        ok: false,
        code: 'MERGE_ERROR',
        reason: (err as Error)?.message || String(err),
        stage: 'merge',
        transient: false,
      };
    }
  }

  /**
   * Unified outcome handler and classifier.
   */
  private async handlePhaseOutcome(
    threadId: string,
    phase: ExecutionPhase,
    runId: string,
    result:
      | { ok: true; verdict?: string }
      | { ok: false; code: string; reason: string; stage?: FailureStage; quotaResetsAt?: string; transient?: boolean },
    identity?: { operationId?: string; generation?: number; round?: number },
  ): Promise<void> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread || !thread.execution) return;

    // Check anti-stale protection: must strictly match current runId, generation, and operationId
    if (!thread.execution.run || thread.execution.run.id !== runId) {
      console.warn(`[ExecutionController] Stale phase outcome ignored for ${threadId}: expected run ${thread.execution.run?.id}, got ${runId}`);
      return;
    }
    if (thread.execution.run.state === 'stopping' || thread.execution.run.unconfirmed || thread.failure?.code === 'LAUNCH_UNCONFIRMED') {
      console.warn(`[ExecutionController] Phase outcome ignored for stopping/unconfirmed run ${runId}`);
      return;
    }
    if (identity?.generation !== undefined && thread.execution.generation !== identity.generation) {
      console.warn(`[ExecutionController] Stale callback generation mismatch for ${threadId}: expected ${thread.execution.generation}, got ${identity.generation}`);
      return;
    }
    if (identity?.operationId !== undefined && thread.execution.operationId !== identity.operationId) {
      console.warn(`[ExecutionController] Stale callback operationId mismatch for ${threadId}: expected ${thread.execution.operationId}, got ${identity.operationId}`);
      return;
    }
    if (identity?.round !== undefined && thread.execution.round !== identity.round) {
      console.warn(`[ExecutionController] Stale callback round mismatch for ${threadId}: expected ${thread.execution.round}, got ${identity.round}`);
      return;
    }
    if (thread.execution.phase !== phase) {
      console.warn(`[ExecutionController] Stale callback phase mismatch for ${threadId}: expected phase ${thread.execution.phase}, got ${phase}`);
      return;
    }

    // Check if task has already been paused, completed, or wait_decision during the run
    if (thread.status === 'paused' || thread.status === 'wait_decision' || thread.cleanup) {
      return;
    }

    const validateRunMatch = (t: DelegationThread) => {
      if (!t.execution?.run || t.execution.run.id !== runId) {
        throw new Error(`Stale run write aborted for ${threadId}: expected run ${t.execution?.run?.id}, got ${runId}`);
      }
      if (t.execution.run.state === 'stopping' || t.execution.run.unconfirmed || t.failure?.code === 'LAUNCH_UNCONFIRMED') {
        throw new Error(`Cannot apply phase outcome to stopping or unconfirmed run ${runId}`);
      }
      if (t.cleanup) {
        throw new Error(`Task is marked for cleanup (${t.cleanup.action})`);
      }
      if (t.status === 'paused' || t.status === 'completed' || t.status === 'failed') {
        throw new Error(`Cannot apply phase outcome to task in ${t.status} status`);
      }
      if (identity?.generation !== undefined && t.execution.generation !== identity.generation) {
        throw new Error(`Stale generation write aborted for ${threadId}: expected ${t.execution.generation}, got ${identity.generation}`);
      }
      if (identity?.operationId !== undefined && t.execution.operationId !== identity.operationId) {
        throw new Error(`Stale operationId write aborted for ${threadId}: expected ${t.execution.operationId}, got ${identity.operationId}`);
      }
      if (identity?.round !== undefined && t.execution.round !== identity.round) {
        throw new Error(`Stale round write aborted for ${threadId}: expected ${t.execution.round}, got ${identity.round}`);
      }
      if (t.execution.phase !== phase) {
        throw new Error(`Stale phase write aborted for ${threadId}: expected phase ${t.execution.phase}, got ${phase}`);
      }
    };

    if (result.ok) {
      // SUCCESS in this phase: Advance immediately to next phase!
      if (phase === 'develop') {
        // Develop succeeded -> advance directly to review!
        await this.transitionTask(threadId, (t) => {
          validateRunMatch(t);
          t.execution!.phase = 'review';
          t.execution!.run = undefined;
          t.execution!.attempt = 0;
          t.execution!.autoRetriesUsed = 0;
          t.execution!.budget.spentActiveMs = 0;
        });
        const reviewStartRes = await this.startExecution(threadId, 'review');
        if (!reviewStartRes.started) {
          console.error(`[ExecutionController] Failed to auto-start review phase for ${threadId}:`, reviewStartRes);
        }
      } else if (phase === 'review') {
        const verdict = (result as { ok: true; verdict?: string }).verdict;
        if (verdict === 'bootstrap' || (thread.reviewBootstrap?.phase === 'confirmed' && !thread.worktree?.approvedCommit && thread.rounds.length === 1 && thread.rounds[0].status !== 'completed')) {
          // Dedicated reviewer bootstrap confirmed -> advance directly to develop!
          await this.transitionTask(threadId, (t) => {
            validateRunMatch(t);
            t.execution!.phase = 'develop';
            t.execution!.purpose = undefined;
            t.execution!.run = undefined;
            t.execution!.attempt = 0;
            t.execution!.autoRetriesUsed = 0;
            t.execution!.budget.spentActiveMs = 0;
          });
          void this.startExecution(threadId, 'develop');
        } else if (verdict === 'approve' || thread.worktree?.approvedCommit) {
          // Approved -> advance directly to merge!
          await this.transitionTask(threadId, (t) => {
            validateRunMatch(t);
            t.execution!.phase = 'merge';
            t.execution!.run = undefined;
            t.execution!.attempt = 0;
            t.execution!.autoRetriesUsed = 0;
            t.execution!.budget.spentActiveMs = 0;
          });
          void this.startExecution(threadId, 'merge');
        } else if (verdict === 'rework' || thread.status === 'running') {
          // Rework requested -> advance directly to develop!
          await this.transitionTask(threadId, (t) => {
            validateRunMatch(t);
            t.execution!.phase = 'develop';
            t.execution!.run = undefined;
            t.execution!.attempt = 0;
            t.execution!.autoRetriesUsed = 0;
            t.execution!.budget.spentActiveMs = 0;
          });
          void this.startExecution(threadId, 'develop');
        } else if (verdict === 'wait_decision') {
          // Stays in wait_decision
        } else if (verdict === 'completed') {
          await this.transitionTask(threadId, (t) => {
            validateRunMatch(t);
            t.status = 'completed';
            t.execution!.run = undefined;
          });
        }
      } else if (phase === 'merge') {
        // Merge succeeded -> completed!
        await this.transitionTask(threadId, (t) => {
          validateRunMatch(t);
          t.status = 'completed';
          t.execution!.run = undefined;
        });
      }
      return;
    }

    // FAILURE in this phase:
    const { code, reason, stage, quotaResetsAt, transient } = result;
    const autoRetriesUsed = thread.execution.autoRetriesUsed || 0;
    const canRetry =
      Boolean(transient) &&
      autoRetriesUsed < thread.execution.budget.maxAutoRetries &&
      thread.execution.budget.spentActiveMs < thread.execution.budget.limitMs;

    if (quotaResetsAt) {
      // Known quota reset time: queue until reset without consuming retry count
      const resetTime = Date.parse(quotaResetsAt);
      const notBefore = new Date(Math.max(this.wallClockNow() + 30_000, resetTime + 20_000)).toISOString();
      await this.setQueuedState(threadId, phase, 'quota', notBefore);
      return;
    }

    if (canRetry) {
      const nextUsed = autoRetriesUsed + 1;
      const backoffMs = this.retryBackoffsMs[Math.min(nextUsed - 1, this.retryBackoffsMs.length - 1)];
      const notBefore = new Date(this.wallClockNow() + backoffMs).toISOString();

      await this.transitionTask(threadId, (t) => {
        validateRunMatch(t);
        t.execution!.autoRetriesUsed = nextUsed;
        t.execution!.run = undefined;
        t.execution!.queue = { reason: 'backoff', notBefore };
        t.execution!.lastOutcome = { code, reason, at: new Date().toISOString() };
        t.status = 'queued';
      });

      // Schedule wake after backoff
      const timer = setTimeout(() => {
        this.backoffTimers.delete(threadId);
        void this.startExecution(threadId, phase);
      }, backoffMs);
      timer.unref?.();
      this.backoffTimers.set(threadId, timer);
      return;
    }

    // Terminal failure for this attempt / operation
    const failureStage: FailureStage = stage || (phase === 'merge' ? 'merge' : phase === 'review' ? 'review' : 'execution');
    await this.transitionTask(threadId, (t) => {
      validateRunMatch(t);
      if (code === 'REVIEW_BLOCKED' || t.reviewBlocked) {
        if (t.execution) {
          t.execution.run = undefined;
        }
        t.status = 'reviewing';
        return;
      }
      enterTaskFailure(t, {
        stage: failureStage,
        reason: reason || code,
        code,
        at: new Date().toISOString(),
        round: t.currentRound || 1,
      });
      if (t.execution) {
        t.execution.run = undefined;
        t.execution.lastOutcome = { code, reason, at: new Date().toISOString() };
      }
      t.status = 'failed';
    });
  }

  /**
   * Request a task decision (wait_decision) across develop, review, or merge.
   */
  public async requestTaskDecision(
    threadId: string,
    params: {
      question: string;
      background?: string;
      suggestion?: string;
      resumePoint: TaskDecisionResumePoint;
      conflictFiles?: string[];
      logPath?: string;
    },
  ): Promise<TaskDecisionRecord> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    const decisionId = crypto.randomUUID();
    const phase: ExecutionPhase = thread.execution?.phase || 'develop';
    const decision: TaskDecisionRecord = {
      id: decisionId,
      operationId: thread.execution?.operationId || `op_${this.wallClockNow()}`,
      generation: thread.execution?.generation || 1,
      phase,
      round: thread.currentRound || 1,
      question: params.question,
      background: params.background,
      suggestion: params.suggestion,
      resumePoint: params.resumePoint,
      conflictFiles: params.conflictFiles,
      logPath: params.logPath,
      createdAt: new Date().toISOString(),
    };

    // CAS FIRST: Persist decision and wait_decision before stopping any process
    await this.transitionTask(threadId, (t) => {
      t.decision = decision;
      t.decisions = [...(t.decisions || []), decision];
      t.status = 'wait_decision';
      if (t.execution?.run) {
        t.execution.run.state = 'stopping';
      }
    });

    const childPid = thread.execution?.run?.child?.pid;
    const childBirthId = thread.execution?.run?.child?.birthId;
    const isInsideChildTree = childPid ? isProcessDescendant(process.pid, childPid) : false;

    // If called from within child process tree (stdio MCP server invoked by agent CLI),
    // do NOT kill self/parent tree; return decision JSON-RPC response so CLI turn ends cleanly.
    if (!isInsideChildTree) {
      const activeExecution = this.activeExecutions.get(threadId);
      if (activeExecution) {
        activeExecution.abort();
      }

      if (childPid) {
        if (!childBirthId) {
          // Missing birthId: CANNOT authorize kill or identity check!
          await this.transitionTask(threadId, (t) => {
            appendInterruption(t, {
              kind: 'error',
              reason: 'PROCESS_STOP_UNCONFIRMED',
              at: new Date().toISOString(),
              by: 'decision',
            });
            if (t.execution?.run) t.execution.run.state = 'stopping';
          });
          return decision;
        }
        const probe = probeProcessIdentity(childPid, childBirthId);
        if (probe === 'alive') {
          killPidTree(childPid);
          const exited = await confirmProcessTreeExit(childPid, PROCESS_STOP_TIMEOUT_MS, childBirthId);
          if (!exited) {
            await this.transitionTask(threadId, (t) => {
              appendInterruption(t, {
                kind: 'error',
                reason: 'PROCESS_STOP_UNCONFIRMED',
                at: new Date().toISOString(),
                by: 'decision',
              });
              if (t.execution?.run) t.execution.run.state = 'stopping';
            });
            return decision;
          }
        } else if (probe === 'unknown') {
          // Unknown process identity must retain stopping and retain leases
          await this.transitionTask(threadId, (t) => {
            appendInterruption(t, {
              kind: 'error',
              reason: 'PROCESS_STOP_UNCONFIRMED',
              at: new Date().toISOString(),
              by: 'decision',
            });
            if (t.execution?.run) t.execution.run.state = 'stopping';
          });
          return decision;
        }
      }

      await this.transitionTask(threadId, (t) => {
        if (t.execution) {
          t.execution.run = undefined;
        }
      });
    }

    return decision;
  }

  /**
   * Submit an answer to a task decision and immediately resume execution.
   */
  public async submitTaskDecision(
    threadId: string,
    decisionId: string,
    answer: string,
    by = 'user',
  ): Promise<DelegationThread> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);
    if (thread.status !== 'wait_decision' || !thread.decision || thread.decision.id !== decisionId) {
      throw new Error(`Task ${threadId} has no pending decision matching ID ${decisionId}`);
    }
    if (thread.decision.answer) {
      throw new Error(`Decision ${decisionId} for task ${threadId} was already answered`);
    }

    if (thread.execution?.run?.state === 'stopping') {
      const probe = this.probeRun(threadId, thread.execution.run);
      if (probe === 'alive' || probe === 'unknown') {
        throw new Error(`Cannot submit decision for task ${threadId}: previous process is still stopping`);
      }
    }

    const resumePoint = thread.decision.resumePoint;
    const targetPhase: ExecutionPhase =
      resumePoint === 'retry_merge' || resumePoint === 'continue_merge_resolution'
        ? 'merge'
        : resumePoint === 'continue_review'
          ? 'review'
          : 'develop';

    const updated = await this.transitionTask(threadId, (t) => {
      if (!t.decision || t.decision.id !== decisionId || t.decision.answer) {
        throw new Error(`Decision ${decisionId} cannot be consumed: already answered or mismatched`);
      }
      const answeredRecord: TaskDecisionRecord = {
        ...t.decision,
        answer,
        answeredAt: new Date().toISOString(),
        answeredBy: by,
      };
      if (t.decisions) {
        const idx = t.decisions.findIndex((d) => d.id === decisionId);
        if (idx >= 0) t.decisions[idx] = answeredRecord;
        else t.decisions.push(answeredRecord);
      } else {
        t.decisions = [answeredRecord];
      }
      t.decision = answeredRecord; // Kept as answered record so t.decision.answer is present

      if (targetPhase === 'develop') {
        const inFlight = findInFlightRound(t) || t.rounds[t.rounds.length - 1];
        if (inFlight) {
          inFlight.prompt += `\n\n[User Decision Answer]:\n${answer}\n[Decision ID: ${decisionId}]`;
        }
      } else if (targetPhase === 'review') {
        const inFlight = t.rounds[t.rounds.length - 1];
        if (inFlight) {
          inFlight.prompt += `\n\n[User Decision Answer]:\n${answer}\n[Decision ID: ${decisionId}]`;
        }
      } else if (targetPhase === 'merge') {
        if (t.worktree) {
          t.summary.lastOutputSnippet = `[Decision Answer (${decisionId})]: ${answer.slice(0, 100)}`;
        }
      }

      if (t.execution) {
        t.execution.phase = targetPhase;
        t.execution.run = undefined;
        t.execution.queue = undefined;
      }
    });

    // Immediately attempt to start the preserved phase directly without mandatory queued state
    await this.startExecution(threadId, targetPhase, {
      allowStates: ['wait_decision', 'queued'],
    });
    return this.taskStore.getTask(threadId) || updated;
  }

  /**
   * Manual retry: creates a new generation, resets autoRetriesUsed and spentActiveMs.
   */
  public async retryOperation(threadId: string, by = 'user'): Promise<DelegationThread> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    if (thread.execution?.run?.state === 'stopping' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
      const probe = this.probeRun(threadId, thread.execution?.run);
      if (probe === 'alive' || probe === 'unknown' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
        throw new Error(`Cannot retry task ${threadId}: previous process is still stopping or unconfirmed`);
      }
    }

    const phase: ExecutionPhase = thread.execution?.phase || 'develop';
    const nextGen = (thread.execution?.generation || 1) + 1;
    const updated = await this.transitionTask(threadId, (t) => {
      clearTaskFailure(t);
      if (t.execution) {
        t.execution.generation = nextGen;
        t.execution.attempt = 0;
        t.execution.autoRetriesUsed = 0;
        t.execution.budget.spentActiveMs = 0;
        t.execution.run = undefined;
        t.execution.queue = undefined;
      }
    });

    await this.startExecution(threadId, phase, {
      allowStates: ['failed', 'queued'],
      generation: nextGen,
    });
    return this.taskStore.getTask(threadId) || updated;
  }

  /**
   * Manual resume: keeps current generation and remaining budget.
   */
  public async resumeOperation(threadId: string, by = 'user'): Promise<DelegationThread> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    if (thread.execution?.run?.state === 'stopping' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
      const probe = this.probeRun(threadId, thread.execution?.run);
      if (probe === 'alive' || probe === 'unknown' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
        throw new Error(`Cannot resume task ${threadId}: previous process is still stopping or unconfirmed`);
      }
    }

    const phase: ExecutionPhase = thread.execution?.phase || 'develop';
    const updated = await this.transitionTask(threadId, (t) => {
      if (t.interruption?.kind === 'paused') {
        t.interruption = undefined;
      }
      if (t.execution) {
        t.execution.run = undefined;
        t.execution.queue = undefined;
      }
    });

    await this.startExecution(threadId, phase, {
      allowStates: ['paused', 'queued'],
    });
    return this.taskStore.getTask(threadId) || updated;
  }

  /**
   * Pause execution: cancels current execution, confirms process tree stop, releases locks.
   */
  public async pauseExecution(
    threadId: string,
    reason = 'Paused by user',
    by = 'user',
  ): Promise<DelegationThread> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    const active = this.activeExecutions.get(threadId);
    if (active) active.abort();

    if (thread.execution?.run?.child?.pid) {
      const childPid = thread.execution.run.child.pid;
      const expectedBirth = thread.execution.run.child.birthId;
      const isAlive = probeProcessIdentity(childPid) !== 'exited';
      if (isAlive) {
        if (!expectedBirth) {
          // Missing birthId: CANNOT authorize kill or identity check!
          const updated = await this.transitionTask(threadId, (t) => {
            appendInterruption(t, {
              kind: 'error',
              reason: 'PROCESS_STOP_UNCONFIRMED',
              at: new Date().toISOString(),
              by,
            });
            if (t.execution?.run) t.execution.run.state = 'stopping';
          });
          return updated;
        }
        const probe = probeProcessIdentity(childPid, expectedBirth);
        if (probe === 'alive') {
          killPidTree(childPid);
          const exited = await confirmProcessTreeExit(childPid, PROCESS_STOP_TIMEOUT_MS, expectedBirth);
          if (!exited) {
            const updated = await this.transitionTask(threadId, (t) => {
              appendInterruption(t, {
                kind: 'error',
                reason: 'PROCESS_STOP_UNCONFIRMED',
                at: new Date().toISOString(),
                by,
              });
              if (t.execution?.run) t.execution.run.state = 'stopping';
            });
            return updated;
          }
        } else if (probe === 'unknown') {
          const updated = await this.transitionTask(threadId, (t) => {
            appendInterruption(t, {
              kind: 'error',
              reason: 'PROCESS_STOP_UNCONFIRMED',
              at: new Date().toISOString(),
              by,
            });
            if (t.execution?.run) t.execution.run.state = 'stopping';
          });
          return updated;
        }
      }
    }

    const updated = await this.transitionTask(threadId, (t) => {
      appendInterruption(t, {
        kind: 'paused',
        reason,
        at: new Date().toISOString(),
        by,
      });
      if (t.execution) {
        t.execution.run = undefined;
        t.execution.queue = undefined;
      }
      t.status = 'paused';
    });

    return updated;
  }

  private hasGitCandidate(thread: DelegationThread): boolean {
    if (!thread.worktree?.path) return false;
    try {
      const main = thread.worktree.mainPath || thread.worktree.path;
      const ref = gitOutput(main, ['rev-parse', '--verify', `refs/mesh/candidates/${thread.threadId}`]);
      return Boolean(ref && ref.trim());
    } catch {
      return false;
    }
  }

  /**
   * Scans all tasks with execution records, drains queued phases, and recovers crashed or orphaned tasks.
   */
  public async scanAndDrain(): Promise<void> {
    if (this.isDisposed) return;
    this.taskStore.ensureUpToDate();
    const tasks = this.taskStore.getAllTasks();

    for (const thread of tasks) {
      if (!thread.execution || thread.execution.version === 2) continue; // Only manage unified execution tasks! V2 tasks isolated from old scanAndDrain
      const status = thread.status;
      const threadId = thread.threadId;

      if (status === 'queued') {
        if (thread.execution.queue?.notBefore) {
          const notBeforeTime = Date.parse(thread.execution.queue.notBefore);
          if (Number.isFinite(notBeforeTime) && this.wallClockNow() < notBeforeTime) {
            continue;
          }
        }
        void this.startExecution(threadId, thread.execution.phase);
      } else if (status === 'running' || status === 'reviewing' || status === 'merging') {
        const run = thread.execution.run;
        const generation = thread.execution.generation;
        const operationId = thread.execution.operationId;
        const phase = thread.execution.phase;
        const transitionRun = (change: (task: DelegationThread) => void) =>
          this.transitionTask(threadId, (fresh) => {
            if (fresh.status !== status || fresh.execution?.generation !== generation ||
                fresh.execution.operationId !== operationId || fresh.execution.phase !== phase ||
                fresh.execution.run?.id !== run?.id) return;
            change(fresh);
          });
        const inspection = this.inspectRunProcesses(run);

        if (inspection.ownerStatus === 'alive') {
          // Owner process is alive and active; nothing for scanner to recover
          continue;
        }
        if (inspection.ownerStatus === 'unknown') {
          // Indeterminate process identity; do not guess-kill
          continue;
        }

        // Owner process died (ownerStatus === 'exited' or 'none')
        // Check child status and confirm termination
        const childPid = run?.child?.pid;
        const childBirthId = run?.child?.birthId;

        if (childPid && childPid > 0) {
          const isAlive = probeProcessIdentity(childPid) !== 'exited';
          if (isAlive) {
            if (!childBirthId) {
              // Missing birthId: CANNOT authorize kill or identity check!
              await transitionRun( (t) => {
                appendInterruption(t, {
                  kind: 'error',
                  reason: 'PROCESS_STOP_UNCONFIRMED',
                  at: new Date().toISOString(),
                  by: 'system',
                });
                if (t.execution?.run) {
                  t.execution.run.state = 'stopping';
                }
              });
              continue;
            }

            const childProbe = probeProcessIdentity(childPid, childBirthId);
            if (childProbe === 'alive') {
              killPidTree(childPid);
              const exited = await confirmProcessTreeExit(childPid, PROCESS_STOP_TIMEOUT_MS, childBirthId);
              if (!exited) {
                // Termination could not be confirmed: keep run.state = 'stopping' and record interruption.
                // Do NOT clear run, do NOT advance or fail, to prevent concurrent replacement execution.
                await transitionRun( (t) => {
                  appendInterruption(t, {
                    kind: 'error',
                    reason: 'PROCESS_STOP_UNCONFIRMED',
                    at: new Date().toISOString(),
                    by: 'system',
                  });
                  if (t.execution?.run) {
                    t.execution.run.state = 'stopping';
                  }
                });
                continue;
              }
            } else if (childProbe === 'unknown') {
              // Indeterminate process state: unconfirmed stop.
              await transitionRun( (t) => {
                appendInterruption(t, {
                  kind: 'error',
                  reason: 'PROCESS_STOP_UNCONFIRMED',
                  at: new Date().toISOString(),
                  by: 'system',
                });
                if (t.execution?.run) {
                  t.execution.run.state = 'stopping';
                }
              });
              continue;
            }
          }
        }

        // Child process is confirmed dead. Now evaluate evidence for current phase.
        if (thread.execution.phase === 'merge') {
          // Merge phase: completed ONLY if candidate actually landed on real target branch,
          // or explicit no_changes
          const landed = await this.mergeCoordinator.hasLandedCandidate(threadId);
          if (landed && thread.worktree?.approvedCommit) {
            await transitionRun( (t) => {
              t.status = 'completed';
              if (t.worktree?.integration) {
                t.worktree.integration.phase = 'merged';
                t.worktree.integration.updatedAt = new Date().toISOString();
              }
              if (t.execution) t.execution.run = undefined;
            });
            continue;
          }
          if (thread.worktree?.integration?.phase === 'no_changes') {
            await transitionRun( (t) => {
              t.status = 'completed';
              if (t.execution) t.execution.run = undefined;
            });
            continue;
          }
        } else if (thread.execution.phase === 'review') {
          // Review phase: advance ONLY if durable verdict was persisted before owner died
          if (thread.worktree?.approvedCommit) {
            await transitionRun( (t) => {
              if (t.execution) {
                t.execution.phase = 'merge';
                t.execution.run = undefined;
              }
              t.status = 'queued';
              t.queuePhase = 'merge';
            });
            void this.startExecution(threadId, 'merge');
            continue;
          }
          const hasReworkRound = thread.rounds.some(
            (r) => r.type === 'rework' && r.round === (thread.currentRound || 1),
          );
          if (hasReworkRound || thread.status === 'running') {
            await transitionRun( (t) => {
              if (t.execution) {
                t.execution.phase = 'develop';
                t.execution.run = undefined;
              }
              t.status = 'running';
            });
            void this.startExecution(threadId, 'develop');
            continue;
          }
        } else if (thread.execution.phase === 'develop') {
          // Develop phase: NEVER transition directly to completed!
          // Can ONLY hand off to review, and ONLY if current round has valid persistent completed response evidence!
          const activeRoundNum = thread.execution.round || thread.currentRound || 1;
          const currentRound = thread.rounds.find((r) => r.round === activeRoundNum);
          const hasCandidate = this.hasGitCandidate(thread);

          const hasValidCompletedResponse =
            Boolean(currentRound) &&
            currentRound!.status === 'completed' &&
            currentRound!.assessment?.success === true &&
            currentRound!.assessment?.exitCode === 0 &&
            currentRound!.assessment?.stream?.hasActualOutput !== false &&
            hasCandidate;

          if (hasValidCompletedResponse) {
            await transitionRun( (t) => {
              if (t.execution) {
                t.execution.phase = 'review';
                t.execution.run = undefined;
              }
              t.status = 'reviewing';
            });
            void this.startExecution(threadId, 'review');
            continue;
          }
        }

        // When owner died without completion evidence, mark failed(OWNER_LOST) and DO NOT auto-retry!
        await transitionRun( (t) => {
          enterTaskFailure(t, {
            stage: t.execution?.phase === 'merge' ? 'merge' : t.execution?.phase === 'review' ? 'review' : 'execution',
            reason: 'Owner process died without completion evidence',
            at: new Date().toISOString(),
            code: 'OWNER_LOST',
          });
          if (t.execution) {
            t.execution.run = undefined;
            t.execution.lastOutcome = {
              code: 'OWNER_LOST',
              reason: 'Owner process died without completion evidence',
              at: new Date().toISOString(),
            };
          }
        });
      }
    }
  }

  public async waitForActiveExecutions(timeoutMs = 5000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.activeExecutions.size > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 50));
    }
  }

  public dispose(): void {
    this.isDisposed = true;
    if (this.standingScanTimer) {
      clearInterval(this.standingScanTimer);
      this.standingScanTimer = undefined;
    }
    for (const [id, active] of this.activeExecutions) {
      active.abort();
    }
    this.activeExecutions.clear();
    for (const timer of this.backoffTimers.values()) {
      clearTimeout(timer);
    }
    this.backoffTimers.clear();
    this.mergeLock.dispose();
  }
}
