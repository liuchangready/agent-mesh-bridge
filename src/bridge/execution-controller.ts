import { windowsJobFinished, windowsJobOf } from './windows-job';
import { assertDedicatedTask } from './types';
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
  ExecutionNext,
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
  isMergeReview,
  isReviewerBootstrapPending,
  isReviewerWakeEligible,
  isTaskUnfinished,
  isUserInterruption,
  nextRoundNumber,
  shouldResumeNativeSession,
} from './types';
import { TaskStore } from './task-store';
import { applyReviewFailure, reopenFailedReview, reviewBlockDependencyId, reviewDependencyIssue } from './review-wake';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { MergeCoordinator, MergeResult } from './merge-coordinator';
import { TaskRoleLock } from './task-role-lock';
import { NativeSessionLock, NativeSessionLease } from './native-session-lock';
import { TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { MeshFileLock, MeshLockAcquisitionError } from './mesh-file-lock';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';
import {
  confirmProcessTreeExit,
  getProcessBirthId,
  isProcessDescendant,
  killPidTree,
  probeProcessIdentity,
  probeProcessIdentityAsync,
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
  verifyCapturedSubtreeOwnership,
  RunIdentity,
} from './process-binding';
import { requireV2ExecutionEligibility, validateV2ExecutionEligibility } from './execution-gate';
import { CancellationRegistry } from './cancellation-registry';
import { nativeSessionNamespace, SessionOwnershipStore } from './session-ownership';

export const DEFAULT_EXECUTION_BUDGET_MS = 120 * 60 * 1000; // 120 minutes
export const AUTO_RETRY_BACKOFFS_MS = [5_000, 30_000];
export const MAX_AUTO_RETRIES = 2;
export const PROCESS_STOP_TIMEOUT_MS = 10_000;

interface StartExecutionOptions {
  purpose?: 'bootstrap' | 'merge_resolution';
  operationId?: string;
  generation?: number;
  allowStates?: ThreadStatus[];
  onMergeAdmission?: () => void;
}

function v2SlotAgent(task: DelegationThread): AgentId | undefined {
  const execution = task.execution;
  if (execution?.version !== 2 || !execution.run) return undefined;
  if (execution.run.purpose === 'native_maintenance') {
    return execution.run.role === 'reviewer' ? task.initiatorAgent : task.targetAgent;
  }
  if (execution.phase === 'develop') return task.targetAgent;
  if (execution.phase === 'review') return task.initiatorAgent;
  return undefined;
}

function mergeQueueTime(task: DelegationThread): number {
  const enteredAt = Date.parse(task.execution?.queue?.enteredAt ?? '');
  const createdAt = Date.parse(task.createdAt);
  return Number.isFinite(enteredAt) ? enteredAt : Number.isFinite(createdAt) ? createdAt : Number.MAX_SAFE_INTEGER;
}

export interface ExecutionControllerOptions {
  storageDir?: string;
  /** Production hosts set this after v2 dispatch is wired; fixture/legacy controllers may inspect v1 behavior. */
  requireV2Execution?: boolean;
  admissionEnabled?: boolean;
  ownsRuns?: boolean;
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
  private roleLockInstance?: TaskRoleLock;
  private sessionLockInstance?: NativeSessionLock;
  private readonly sessionOwnership: SessionOwnershipStore;
  private readonly executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private readonly defaultBudgetLimitMs: number;
  private readonly requireV2Execution: boolean;
  private readonly admissionEnabled: boolean;
  public readonly ownsRuns: boolean;
  private readonly checkpointIntervalMs: number;
  private readonly retryBackoffsMs: number[];
  private readonly monotonicNow: () => number;
  private readonly wallClockNow: () => number;
  private readonly mergeLock: MeshFileLock;
  private readonly activeExecutions = new Map<string, { abort: () => void; activeSinceMs: number; settled?: Promise<void> }>();
  private readonly startingExecutions = new Map<string, Promise<{ started: boolean; reason?: string; run?: ExecutionRun }>>();
  private readonly backoffTimers = new Map<string, NodeJS.Timeout>();
  private readonly threadTransitionChains = new Map<string, Promise<any>>();
  private standingScanTimer: NodeJS.Timeout | undefined;
  private drainInFlight?: Promise<void>;
  private releaseWake?: NodeJS.Immediate;
  private isDisposed = false;

  /** Legacy v1 locks are constructed only if a v1 path actually needs them. */
  private get roleLock(): TaskRoleLock {
    return this.roleLockInstance || (this.roleLockInstance = new TaskRoleLock(this.storageDir));
  }

  private get sessionLock(): NativeSessionLock {
    return this.sessionLockInstance || (this.sessionLockInstance = new NativeSessionLock(this.storageDir));
  }

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
    this.requireV2Execution = options?.requireV2Execution === true;
    this.admissionEnabled = options?.admissionEnabled !== false;
    this.ownsRuns = options?.ownsRuns !== false;
    this.checkpointIntervalMs = options?.streamSyncIntervalMs ?? 5_000;
    this.retryBackoffsMs = options?.retryBackoffsMs ?? AUTO_RETRY_BACKOFFS_MS;
    this.monotonicNow = options?.monotonicNow ?? monotonicNowMs;
    this.wallClockNow = options?.wallClockNow ?? (() => Date.now());
    this.roleLockInstance = roleLock;
    this.sessionLockInstance = sessionLock;
    this.sessionOwnership = new SessionOwnershipStore(this.storageDir);
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
    if (this.admissionEnabled && this.ownsRuns && scanInterval > 0) {
      this.standingScanTimer = setInterval(() => {
        if (!this.isDisposed) {
          void this.scanAndDrain().catch(() => {});
        }
      }, scanInterval);
      this.standingScanTimer.unref?.();
      this.taskStore.on('changed', this.onDependencyChanged);
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
    subtreeStatus: 'alive' | 'exited' | 'unknown' | 'none';
  } {
    if (!run) {
      return { ownerStatus: 'none', childStatus: 'none', subtreeStatus: 'none' };
    }
    const ownerStatus = run.owner?.pid
      ? (run.owner.birthId ? probeProcessIdentity(run.owner.pid, run.owner.birthId) : 'unknown')
      : 'none';
    const childStatus = run.child?.pid
      ? (run.child.birthId ? probeProcessIdentity(run.child.pid, run.child.birthId) : 'unknown')
      : 'none';
    let subtreeStatus: 'alive' | 'exited' | 'unknown' | 'none' = 'none';
    if (run.child?.pid && !run.subtrees) {
      // Older v2 records never persisted descendant identities. A dead root
      // cannot prove its detached descendants exited, so retain protection.
      subtreeStatus = 'unknown';
    } else if (run.subtrees?.length) {
      let sawUnknown = false;
      let sawAlive = false;
      for (const subtree of run.subtrees) {
        if (subtree.windowsJob && !windowsJobFinished(subtree.windowsJob)) sawUnknown = true;
        for (const identity of [subtree.root, ...subtree.descendants]) {
          if (!identity?.pid || !identity.birthId) {
            sawUnknown = true;
            continue;
          }
          const state = probeProcessIdentity(identity.pid, identity.birthId);
          if (state === 'alive') sawAlive = true;
          else if (state === 'unknown') sawUnknown = true;
        }
      }
      subtreeStatus = sawAlive ? 'alive' : sawUnknown ? 'unknown' : 'exited';
    }
    return { ownerStatus, childStatus, subtreeStatus };
  }

  /**
   * Probe whether a recorded run is still alive.
   */
  public probeRun(
    threadId: string,
    run?: ExecutionControl['run'],
  ): 'alive' | 'exited' | 'unknown' {
    if (!run) return 'exited';
    const { ownerStatus, childStatus, subtreeStatus } = this.inspectRunProcesses(run);
    if (ownerStatus === 'alive' || childStatus === 'alive' || subtreeStatus === 'alive') {
      return 'alive';
    }
    const thread = this.taskStore.getTask(threadId);
    if (thread?.execution?.version === 2) {
      const assessment = assessRunLaunchEvidence(run);
      if (!assessment.safeToClean) {
        return 'unknown';
      }
    }
    if (ownerStatus === 'unknown' || childStatus === 'unknown' || subtreeStatus === 'unknown') {
      return 'unknown';
    }
    return 'exited';
  }

  private async transitionTask(
    threadId: string,
    change: (thread: DelegationThread) => void,
    cleanupToken?: string,
    runExitProof?: { runId: string; operationId: string; generation: number },
  ): Promise<DelegationThread> {
    const prev = this.threadTransitionChains.get(threadId) || Promise.resolve();
    const next = prev.catch(() => {}).then(async () => {
      for (let attempt = 0; attempt < 3; attempt++) {
        this.taskStore.ensureUpToDate();
        const currentRev = this.taskStore.getTask(threadId)?.execution?.revision ?? 0;
        try {
          return await this.taskStore.transitionExecution(threadId, currentRev, change, cleanupToken, runExitProof);
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
    options?: StartExecutionOptions,
  ): Promise<{ started: boolean; reason?: string; run?: ExecutionRun }> {
    if (!this.admissionEnabled) return { started: false, reason: 'V2_STARTUP_BLOCKED' };
    if (!this.ownsRuns) return { started: false, reason: 'RESIDENT_EXECUTION_REQUIRED' };
    const pending = this.startingExecutions.get(threadId);
    if (pending) return pending;
    // V1 acquires resources before CAS. Concurrent local callers must not share
    // a reentrant role lease and let the losing caller release the winner's.
    const starting = this.startExecutionAttempt(threadId, targetPhase, options);
    this.startingExecutions.set(threadId, starting);
    try { return await starting; }
    finally { if (this.startingExecutions.get(threadId) === starting) this.startingExecutions.delete(threadId); }
  }

  public async wakeReviewerV2(threadId: string, round: number, unblock = false): Promise<boolean> {
    if (!this.admissionEnabled || this.isDisposed) return false;
    const canWake = (task: DelegationThread) =>
      task.execution?.version === 2 && task.reviewFlow === 'dedicated' &&
      task.execution.phase === 'review' && task.execution.purpose !== 'bootstrap' &&
      task.execution.round === round && !task.execution.run && !task.execution.next &&
      !task.archivedAt && !task.cleanup && (unblock || !task.reviewBlocked) &&
      (task.status === 'reviewing' || (task.status === 'failed' && task.failure?.stage === 'review'));
    let task = this.taskStore.reloadTask(threadId);
    if (!task || !canWake(task)) return false;
    if (task.status === 'failed' || task.reviewBlocked) {
      try {
        task = await this.transitionTask(threadId, (fresh) => {
          if (!canWake(fresh)) throw new Error('Review is no longer available for wake.');
          reopenFailedReview(fresh);
          if (unblock) {
            fresh.reviewBlocked = undefined;
            if (fresh.execution?.queue?.reason === 'dependency') fresh.execution.queue = undefined;
          }
        });
      } catch { return false; }
    }
    return (await this.startExecution(threadId, 'review', {
      operationId: task.execution!.operationId, generation: task.execution!.generation, allowStates: ['reviewing'],
    })).started;
  }

  private v2DependencyResult(task: DelegationThread, phase: ExecutionPhase): 'completed' | 'failed' | undefined {
    const dependencyId = reviewBlockDependencyId(task);
    if (phase !== 'review' || !dependencyId || dependencyId === task.threadId) return undefined;
    const dependency = this.taskStore.getTask(dependencyId);
    if (dependency?.execution?.run) return undefined;
    return dependency?.status === 'completed' || dependency?.status === 'failed' ? dependency.status : undefined;
  }

  private async failInvalidV2Dependency(thread: DelegationThread): Promise<void> {
    await this.taskStore.transitionExecution(thread.threadId, thread.execution!.revision, task => {
      const execution = task.execution;
      if (execution?.version !== 2 || execution.phase !== 'review' || execution.run || execution.next ||
          task.cleanup || !['reviewing', 'queued'].includes(task.status)) {
        throw new Error('The parked review changed.');
      }
      const reason = reviewDependencyIssue(task.threadId, reviewBlockDependencyId(task), id => this.taskStore.getTask(id));
      if (!reason) throw new Error('The dependency is available again.');
      task.status = 'reviewing';
      task.reviewBlocked = undefined;
      if (execution.queue?.reason === 'dependency') execution.queue = undefined;
      applyReviewFailure(task, { kind: 'unresolvable_park', reason, by: 'execution_controller' });
    });
  }

  private v2StartBlocker(task: DelegationThread, phase: ExecutionPhase): string | undefined {
    if (phase === 'review' && task.execution?.purpose === 'bootstrap' && task.workspace?.dispatchAccepted === false) {
      return 'dispatch_not_accepted';
    }
    const queue = task.execution?.queue;
    if (queue?.notBefore !== undefined) {
      const notBefore = Date.parse(queue.notBefore);
      if (!Number.isFinite(notBefore)) return 'not_before_unverifiable';
      if (this.wallClockNow() < notBefore) return 'not_before';
    }
    if (queue?.reason === 'merge_lock_error') return 'merge_lock_error_requires_intervention';
    if ((task.reviewBlocked || queue?.reason === 'dependency') && !this.v2DependencyResult(task, phase)) {
      return 'dependency_pending';
    }
    if (phase === 'develop' && isReviewerBootstrapPending(task)) return 'reviewer_bootstrap_pending';
    return undefined;
  }

  private async startExecutionAttempt(
    threadId: string,
    targetPhase?: ExecutionPhase,
    options?: StartExecutionOptions,
  ): Promise<{ started: boolean; reason?: string; run?: ExecutionRun }> {
    if (this.isDisposed) return { started: false, reason: 'disposed' };
    this.taskStore.ensureUpToDate();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) return { started: false, reason: 'not_found' };
    if (thread.reviewFlow !== 'dedicated') return { started: false, reason: 'LEGACY_REVIEW_FLOW_DISABLED' };

    if (this.requireV2Execution && thread.execution?.version !== 2) {
      return { started: false, reason: `HISTORICAL_TASK_READ_ONLY:${thread.execution ? `VERSION_${String((thread.execution as any).version)}` : 'NO_EXECUTION'}` };
    }

    if (thread.execution?.version === 2) {
      return this.startExecutionV2(threadId, targetPhase, options);
    }
    const pendingBackoff = this.backoffTimers.get(threadId);
    if (pendingBackoff) {
      clearTimeout(pendingBackoff);
      this.backoffTimers.delete(threadId);
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
      if (thread.receiverSessionCaptured && thread.receiverSessionId) {
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
              ? t.execution.purpose === 'bootstrap' ? 'queued' : 'reviewing'
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
  private async startExecutionV2(
    threadId: string,
    targetPhase?: ExecutionPhase,
    options?: StartExecutionOptions,
  ): Promise<{ started: boolean; reason?: string; run?: ExecutionRun }> {
    if (!this.admissionEnabled) return { started: false, reason: 'V2_STARTUP_BLOCKED' };
    if (this.isDisposed) return { started: false, reason: 'disposed' };
    this.taskStore.ensureUpToDate();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) return { started: false, reason: 'not_found' };

    if (thread.reviewFlow !== 'dedicated') return { started: false, reason: 'LEGACY_REVIEW_FLOW_DISABLED' };
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
    if (thread.execution?.run?.mergeLockHelper) {
      const sourceRun = thread.execution.run;
      if (sourceRun.state !== 'stopping' || !sourceRun.unconfirmed) {
        return { started: false, reason: 'run_busy', run: sourceRun };
      }
      const helper = sourceRun.mergeLockHelper;
      if (!helper) return { started: false, reason: 'run_busy', run: sourceRun };
      const confirmed = this.mergeLock.confirmHelperExit({ pid: helper.pid, birthId: helper.birthId, channelPath: helper.channelPath });
      if (!confirmed) return { started: false, reason: 'merge_lock_unconfirmed', run: sourceRun };
      const sourceExecution = thread.execution;
      const sourceNext = sourceExecution.next ? structuredClone(sourceExecution.next) : undefined;
      const sourceQueue = sourceExecution.queue ? structuredClone(sourceExecution.queue) : undefined;
      const sourceOutcome = sourceExecution.lastOutcome ? structuredClone(sourceExecution.lastOutcome) : undefined;
      const sourceInterruption = thread.interruption ? structuredClone(thread.interruption) : undefined;
      const hasNoBusinessSpawn = sourceRun.step === 'merge_lock_release_unconfirmed' &&
        !sourceRun.spawnIntentAt && !sourceRun.registeredAt && !sourceRun.child &&
        (!sourceRun.subtrees || sourceRun.subtrees.length === 0);
      const processEvidence = this.inspectRunProcesses(sourceRun);
      const hasCompleteExitedBusinessTree = Boolean(
        sourceRun.spawnIntentAt && sourceRun.registeredAt && sourceRun.child?.pid && sourceRun.child.birthId &&
        Array.isArray(sourceRun.subtrees) &&
        sourceRun.subtrees.some((subtree) => subtree.root.pid === sourceRun.child!.pid &&
          subtree.root.birthId === sourceRun.child!.birthId) &&
        processEvidence.ownerStatus === 'exited' && processEvidence.childStatus === 'exited' &&
        processEvidence.subtreeStatus === 'exited'
      );
      // A confirmed lock-helper exit only proves the global mutex helper is gone.
      // If business spawn began, retain the run until the controller, registered
      // root, and its durable descendant snapshot are all positively exited.
      if (this.activeExecutions.has(threadId) || (!hasNoBusinessSpawn && !hasCompleteExitedBusinessTree)) {
        return { started: false, reason: 'merge_run_exit_unconfirmed', run: sourceRun };
      }
      try {
        await this.transitionTask(threadId, (task) => {
          const control = task.execution;
          const run = control?.run;
          if (task.cleanup || !run || control?.version !== 2 || control.operationId !== sourceExecution.operationId ||
              control.generation !== sourceExecution.generation || control.phase !== sourceExecution.phase ||
              control.run?.id !== sourceRun.id || JSON.stringify(control.next) !== JSON.stringify(sourceNext) ||
              JSON.stringify(control.queue) !== JSON.stringify(sourceQueue) ||
              JSON.stringify(control.lastOutcome) !== JSON.stringify(sourceOutcome) ||
              JSON.stringify(task.interruption) !== JSON.stringify(sourceInterruption) ||
              JSON.stringify(run) !== JSON.stringify(sourceRun) ||
              run.mergeLockHelper?.channelPath !== helper.channelPath || run.mergeLockHelper.pid !== helper.pid ||
              run.mergeLockHelper.birthId !== helper.birthId ||
              !this.mergeLock.confirmHelperExit({ pid: helper.pid, birthId: helper.birthId, channelPath: helper.channelPath })) {
            throw new Error('Merge helper identity changed before confirmed settlement.');
          }
          if (this.activeExecutions.has(threadId)) throw new Error('The local execution worker has not acknowledged settlement.');
          if (!hasNoBusinessSpawn) {
            const evidence = this.inspectRunProcesses(run);
            if (!run.spawnIntentAt || !run.registeredAt || !run.child?.pid || !run.child.birthId ||
                !Array.isArray(run.subtrees) ||
                !run.subtrees.some((subtree) => subtree.root.pid === run.child!.pid && subtree.root.birthId === run.child!.birthId) ||
                evidence.ownerStatus !== 'exited' || evidence.childStatus !== 'exited' || evidence.subtreeStatus !== 'exited') {
              throw new Error('The complete merge business process tree has not been confirmed exited.');
            }
          }
          task.execution!.run = undefined;
          task.execution!.queue = { reason: 'merge_lock_error', enteredAt: new Date().toISOString() };
          task.execution!.lastOutcome = { code: 'MERGE_LOCK_HELPER_EXIT_CONFIRMED', reason: helper.reason, at: new Date().toISOString() };
          if (task.status !== 'paused') task.status = 'queued';
        });
      } catch {
        return { started: false, reason: 'merge_lock_unconfirmed', run: this.taskStore.getTask(threadId)?.execution?.run };
      }
      return { started: false, reason: 'merge_lock_error_requires_intervention' };
    }
    // A persisted next handoff is consumed only by the CAS that claims the target run.
    const pendingNext = thread.execution?.next;
    // Determine target phase and role
    const phase: ExecutionPhase =
      pendingNext?.phase ||
      targetPhase ||
      thread.execution?.phase ||
      (thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge')
        ? 'merge'
        : thread.status === 'reviewing'
          ? 'review'
          : 'develop');
    const role: 'developer' | 'reviewer' = phase === 'develop' ? 'developer' : 'reviewer';

    if (options?.onMergeAdmission && phase !== 'merge') return { started: false, reason: 'candidate_changed' };
    const blocker = this.v2StartBlocker(thread, phase);
    if (blocker) return { started: false, reason: blocker };

    const elapsedBackoff = this.backoffTimers.get(threadId);
    if (elapsedBackoff) {
      clearTimeout(elapsedBackoff);
      this.backoffTimers.delete(threadId);
    }
    const limitMs = thread.execution?.budget?.limitMs ?? this.defaultBudgetLimitMs;
    const spentActiveMs = thread.execution?.budget?.spentActiveMs ?? 0;
    if (spentActiveMs >= limitMs) return { started: false, reason: 'budget_exhausted' };

    // Check owner identity
    const ownerPid = process.pid;
    const ownerBirthId = getProcessBirthId(ownerPid);
    if (!ownerBirthId) {
      return { started: false, reason: 'OWNER_IDENTITY_UNVERIFIABLE' };
    }

    // Pre-check for existing uncleaned run
    const existingRun = thread.execution?.run;
    let recoverableRunId: string | undefined;
    if (existingRun) {
      const isSameOwner =
        existingRun.owner?.pid === ownerPid &&
        existingRun.owner?.birthId === ownerBirthId;
      if (!isSameOwner) {
        // A persisted decision may leave a durable next handoff with the source
        // run still recorded if its controller crashes before settling. Reuse it
        // only when the source, owner, registered child, and launch evidence are
        // all exact and both process identities are positively exited.
        const next = thread.execution?.next;
        const { ownerStatus: staleOwnerStatus, childStatus: staleChildStatus, subtreeStatus: staleSubtreeStatus } = this.inspectRunProcesses(existingRun);
        const launchEvidence = assessRunLaunchEvidence(existingRun);
        // Registered children require an explicit exited probe; a missing child
        // is acceptable only when the launch evidence proves no child was spawned.
        const recoverableHandoff = Boolean(
          next && next.sourceRunId === existingRun.id &&
          staleOwnerStatus === 'exited' &&
          (existingRun.child?.pid ? staleChildStatus === 'exited' : staleChildStatus === 'none') &&
          (existingRun.child?.pid ? staleSubtreeStatus === 'exited' : staleSubtreeStatus === 'none') &&
          launchEvidence.safeToClean && !existingRun.unconfirmed && !thread.cleanup,
        );
        if (recoverableHandoff) recoverableRunId = existingRun.id;

        // A competing controller only reports occupancy while the persisted owner
        // is alive, except for the fully evidenced durable handoff above.
        const recoverableLaunch = isUnconfirmedLaunchAfterOwnerExit(existingRun);
        if (!recoverableHandoff && recoverableLaunch && thread.failure?.code !== 'LAUNCH_UNCONFIRMED') {
          const evidence = `Owner ${existingRun.owner?.pid}/${existingRun.owner?.birthId} exited after spawn intent before child registration`;
          await recordLaunchRecoveryFailure(this.taskStore, threadId, existingRun.id, evidence, {
            run: existingRun,
            generation: thread.execution!.generation,
            operationId: thread.execution!.operationId,
            phase: thread.execution!.phase,
            round: thread.execution!.round,
          });
        }
        if (!recoverableHandoff) return { started: false, reason: 'run_busy', run: this.taskStore.getTask(threadId)?.execution?.run ?? existingRun };
      }
    }

    // Capture initial identity to ensure authoritative CAS matching
    const initialGeneration = options?.generation ?? thread.execution?.generation ?? 1;
    const initialOperationId = pendingNext ? undefined : options?.operationId ?? thread.execution?.operationId;
    const initialPhase = phase;
    const initialRound = pendingNext?.round ?? thread.execution?.round ?? thread.currentRound ?? 1;
    const policy = phase === 'merge' ? undefined : concurrencyPolicy(this.storageDir);
    const exitedRuns = new Map<string, number>();
    if (policy?.enabled) {
      // Probe before the short write lock. Newly published runs still count
      // when the capacity check re-reads the ledger inside the CAS.
      await Promise.all(this.taskStore.getAllTasks().map(async task => {
        const run = task.execution?.run;
        if (task.threadId === threadId || !v2SlotAgent(task) || !run?.owner?.birthId || run.unconfirmed ||
            await this.probeRunOwner(run.owner) !== 'exited') return;
        if (this.probeRun(task.threadId, run) === 'exited') exitedRuns.set(task.threadId, task.execution!.revision);
      }));
    }

    // Unique run acquisition under task short lock
    const runId = crypto.randomUUID();
    let isDuplicateRequest = false;
    let capacityBlocker: 'capacity_limit' | 'capacity_limit_agent' | undefined;
    let runBusyError: string | undefined;
    let finalRunId: string = runId;
    let finalGeneration = initialGeneration;
    let finalOperationId =
      initialOperationId ||
      `${phase}_${initialRound}_${this.wallClockNow()}`;
    let finalRound = initialRound;

    for (let attempt = 0; attempt < 3; attempt++) {
      const current = this.taskStore.reloadTask(threadId);
      if (!current) return { started: false, reason: 'not_found' };
      const rev = current.execution?.revision ?? 0;

      try {
        thread = await this.taskStore.transitionExecution(threadId, rev, (t) => {
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
          const currentBlocker = this.v2StartBlocker(t, phase);
          if (currentBlocker) throw new Error(currentBlocker);
          if (t.execution?.generation !== initialGeneration) {
            throw new Error(`GENERATION_MISMATCH: expected ${initialGeneration}, found ${t.execution?.generation}`);
          }
          if (initialOperationId && t.execution?.operationId !== initialOperationId) {
            throw new Error(`OPERATION_MISMATCH: expected ${initialOperationId}, found ${t.execution?.operationId}`);
          }
          if (!t.execution?.next && t.execution?.phase && t.execution.phase !== initialPhase) {
            throw new Error(`PHASE_MISMATCH: expected ${initialPhase}, found ${t.execution.phase}`);
          }
          if (!t.execution?.next && t.execution?.round && t.execution.round !== initialRound) {
            throw new Error(`ROUND_MISMATCH: expected ${initialRound}, found ${t.execution.round}`);
          }
          if (t.execution?.next && (t.execution.next.phase !== initialPhase || t.execution.next.round !== initialRound)) {
            throw new Error('NEXT_MISMATCH: persisted handoff no longer matches the requested phase.');
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
            const stale = t.execution.run;
            const handoff = t.execution.next;
            const { ownerStatus: ownerNow, childStatus: childNow, subtreeStatus: subtreeNow } = this.inspectRunProcesses(stale);
            const evidenceNow = assessRunLaunchEvidence(stale);
            if (
              !recoverableRunId || stale.id !== recoverableRunId ||
              !handoff || handoff.sourceRunId !== stale.id ||
              ownerNow !== 'exited' || (stale.child?.pid ? childNow !== 'exited' : childNow !== 'none') ||
              (stale.child?.pid ? subtreeNow !== 'exited' : subtreeNow !== 'none') ||
              !evidenceNow.safeToClean || stale.unconfirmed || t.cleanup
            ) {
              throw new Error(`RUN_BUSY:${stale.id}:${stale.state}`);
            }
          }

          if (policy?.enabled) {
            const occupied = this.taskStore.getAllTasks()
              .filter(task => task.threadId !== threadId && task.execution?.revision !== exitedRuns.get(task.threadId))
              .map(v2SlotAgent).filter(agent => agent !== undefined);
            const agent = phase === 'develop' ? t.targetAgent : t.initiatorAgent;
            capacityBlocker = occupied.length >= policy.maxConcurrentTasks ? 'capacity_limit'
              : occupied.filter(owner => owner === agent).length >= policy.maxConcurrentPerAgent ? 'capacity_limit_agent'
              : undefined;
            if (capacityBlocker) {
              if (t.status === 'queued' && !t.execution!.run && t.execution!.queue?.reason === 'capacity') {
                throw new Error(capacityBlocker);
              }
              t.execution!.run = undefined;
              t.execution!.queue = { reason: 'capacity', enteredAt: this.queueEnteredAt(t.execution!) };
              t.status = 'queued';
              clearTaskFailure(t);
              return;
            }
          }

          const dependencyResult = this.v2DependencyResult(t, phase);
          if (dependencyResult) {
            t.summary.lastOutputSnippet = `Dependency ${t.reviewBlocked!.waitingForThreadId} ${dependencyResult}; review resumed.`;
            t.reviewBlocked = undefined;
          }
          finalGeneration = initialGeneration;
          finalOperationId = pendingNext
            ? `${phase}_${initialRound}_${this.wallClockNow()}`
            : initialOperationId || `${phase}_${initialRound}_${this.wallClockNow()}`;
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
            purpose: options?.purpose ?? t.execution?.next?.purpose ?? t.execution?.purpose,
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
            queue: t.execution?.queue?.reason === 'dependency' ? undefined : t.execution?.queue,
            next: undefined,
          };
          if (phase !== 'merge') {
            const assignmentAgent = phase === 'develop' ? t.targetAgent : t.initiatorAgent;
            const assignment = t.nativeSessionAssignments?.[role];
            t.nativeSessionAssignments = {
              ...(t.nativeSessionAssignments || {}),
              [role]: assignment || { namespace: nativeSessionNamespace(assignmentAgent), sessionEpoch: 1 },
            };
          }
          t.status =
            phase === 'develop'
              ? 'running'
              : phase === 'review'
                ? t.execution.purpose === 'bootstrap' ? 'queued' : 'reviewing'
                : 'queued';
          clearTaskFailure(t);
        });
        if (capacityBlocker) return { started: false, reason: capacityBlocker };
        break;
      } catch (err: any) {
        const msg = String(err?.message || err);
        if (msg.includes('RUN_BUSY')) {
          runBusyError = msg;
          break;
        }
        if (msg.includes('revision mismatch')) {
          const latest = this.taskStore.reloadTask(threadId);
          if (latest?.execution?.run) {
            const latestExecution = latest.execution;
            const latestRun = latestExecution.run!;
            const sameOwnerDuplicate =
              latestRun.owner?.pid === ownerPid &&
              latestRun.owner?.birthId === ownerBirthId &&
              latestExecution.generation === initialGeneration &&
              latestExecution.phase === initialPhase &&
              latestExecution.round === initialRound &&
              (!initialOperationId || latestExecution.operationId === initialOperationId) &&
              (!latestExecution.next ||
                (latestExecution.next.phase === initialPhase && latestExecution.next.round === initialRound));
            if (sameOwnerDuplicate) {
              // A same-process standing scan may win the short-write CAS just
              // before an explicit start call. Treat its exact run as the
              // duplicate operation; never launch a second subtree.
              isDuplicateRequest = true;
              finalRunId = latestRun.id;
              finalGeneration = latestExecution.generation;
              finalOperationId = latestExecution.operationId;
              finalRound = latestExecution.round;
              break;
            }
            runBusyError = `RUN_BUSY:${latestRun.id}:${latestRun.state}`;
            break;
          }
          if (attempt < 2) continue;
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

    // Pin the native namespace and statically claim any known session before spawn.
    // This record is assignment evidence only; the run above is the only execution owner.
    this.taskStore.ensureUpToDate();
    const admissionCheck = this.taskStore.getTask(threadId);
    if (
      !admissionCheck ||
      admissionCheck.status === 'paused' ||
      admissionCheck.cleanup ||
      admissionCheck.archivedAt ||
      admissionCheck.execution?.run?.id !== finalRunId
    ) {
      if (admissionCheck?.execution?.run?.id === finalRunId && !admissionCheck.cleanup) {
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn', pendingNext);
      }
      return {
        started: false,
        reason: admissionCheck?.cleanup ? 'cleanup_in_progress' : admissionCheck?.status === 'paused' ? 'paused' : 'invalidated_before_spawn',
      };
    }

    let sessionLease: NativeSessionLease | undefined;
    let mergeRelease: (() => void) | undefined;

    if (phase === 'develop') {
      if (thread.receiverSessionId) {
        const ownership = await this.claimSessionForRun(thread, 'developer', thread.targetAgent, thread.receiverSessionId);
        if (!ownership.ok) {
          const latest = this.taskStore.reloadTask(threadId);
          if (latest?.cleanup) {
            await this.taskStore.releaseUnspawnedRunForCleanup(threadId, finalRunId);
            return { started: false, reason: 'cleanup_in_progress' };
          }
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn', pendingNext);
          await this.failSessionOwnership(threadId, ownership.code, ownership.reason);
          return { started: false, reason: ownership.code };
        }
      }
    } else if (phase === 'review') {
      const reviewerSession = thread.reviewerSessionId || thread.callerSessionId;
      if (reviewerSession) {
        const ownership = await this.claimSessionForRun(thread, 'reviewer', thread.initiatorAgent, reviewerSession);
        if (!ownership.ok) {
          const latest = this.taskStore.reloadTask(threadId);
          if (latest?.cleanup) {
            await this.taskStore.releaseUnspawnedRunForCleanup(threadId, finalRunId);
            return { started: false, reason: 'cleanup_in_progress' };
          }
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn', pendingNext);
          await this.failSessionOwnership(threadId, ownership.code, ownership.reason);
          return { started: false, reason: ownership.code };
        }
      }
    } else if (phase === 'merge') {
      options?.onMergeAdmission?.();
      const ownerFile = `${this.mergeLock.lockPath}.owner.json`;
      let ownerText: string | undefined;
      try {
        ownerText = fs.readFileSync(ownerFile, 'utf8');
      } catch (error: any) {
        if (error?.code !== 'ENOENT') {
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock_error', pendingNext);
          await this.recordMergeLockError(threadId, 'Merge lock owner record could not be read.');
          return { started: false, reason: 'merge_lock_error' };
        }
      }
      if (ownerText) {
        try {
          const owner = JSON.parse(ownerText);
          // An in-process lease is authoritative for this controller. A
          // cross-process owner record is only a hint when both process
          // identities are positively verified; otherwise the OS try-lock
          // handshake below decides busy vs helper failure.
          if (this.mergeLock.hasLocalOwner()) {
            await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock', pendingNext);
            return { started: false, reason: 'merge_lock_busy' };
          }
          if (owner?.pid && owner?.birthId && owner?.helperPid && owner?.helperBirthId) {
            const ownerState = probeProcessIdentity(owner.pid, owner.birthId);
            const helperState = probeProcessIdentity(owner.helperPid, owner.helperBirthId);
            if (ownerState === 'alive' && helperState === 'alive') {
              await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock', pendingNext);
              return { started: false, reason: 'merge_lock_busy' };
            }
            if (ownerState === 'unknown' || helperState === 'unknown') {
              await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock_error', pendingNext);
              await this.recordMergeLockError(threadId, 'Merge lock owner or helper process identity is unverifiable.');
              return { started: false, reason: 'merge_lock_error' };
            }
          }
        } catch {
          await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock_error', pendingNext);
          await this.recordMergeLockError(threadId, 'Merge lock owner record is unreadable.');
          return { started: false, reason: 'merge_lock_error' };
        }
      }
      try {
        // A merge run may be claimed as starting while the bounded OS-lock
        // handshake runs, but it is never advertised as merging until the
        // helper has confirmed acquisition.
        mergeRelease = await this.mergeLock.acquire(undefined, () => this.isDisposed, 2_000, true);
      } catch (error: any) {
        if (error instanceof MeshLockAcquisitionError && error.certainty === 'unconfirmed') {
          await this.markMergeHelperUnconfirmed(threadId, finalRunId, finalOperationId, finalGeneration, pendingNext, error.helper, error.message);
          return { started: false, reason: 'merge_lock_unconfirmed' };
        }
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock_error', pendingNext);
        await this.recordMergeLockError(threadId, String(error?.message || error));
        return { started: false, reason: 'merge_lock_error' };
      }
      if (!mergeRelease) {
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'merge_lock', pendingNext);
        return { started: false, reason: 'merge_lock_busy' };
      }
      const acquiredHelper = this.mergeLock.getActiveHelperEvidence?.();
      try {
        await this.transitionTask(threadId, (task) => {
          if (task.execution?.run?.id !== finalRunId || task.execution.run.state !== 'starting' || task.cleanup ||
              !acquiredHelper?.pid || !acquiredHelper.birthId) {
            throw new Error('MERGE_RUN_INVALIDATED_BEFORE_LOCK');
          }
          task.execution.run.mergeLockHelper = { ...acquiredHelper, reason: 'OS lock acquired; awaiting phase completion', observedAt: new Date().toISOString() };
          task.status = 'merging';
        });
      } catch {
        const released = await this.mergeLock.releaseAndConfirm(mergeRelease, PROCESS_STOP_TIMEOUT_MS);
        mergeRelease = undefined;
        if (!released) {
          await this.markMergeHelperUnconfirmed(threadId, finalRunId, finalOperationId, finalGeneration, pendingNext,
            acquiredHelper || { channelPath: this.mergeLock.lockPath }, 'Merge run was invalidated after helper acquisition but helper release was not acknowledged.');
          return { started: false, reason: 'merge_lock_unconfirmed' };
        }
        await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn', pendingNext);
        return { started: false, reason: 'invalidated_before_spawn' };
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
        const helper = mergeRelease ? this.mergeLock.getActiveHelperEvidence?.() : undefined;
      let mergeReleased = true;
      if (mergeRelease) mergeReleased = await this.mergeLock.releaseAndConfirm(mergeRelease, PROCESS_STOP_TIMEOUT_MS);
      this.cleanupAcquired(null, sessionLease, undefined);
      if (!mergeReleased && check?.execution?.run?.id === finalRunId) {
        await this.markMergeHelperUnconfirmed(threadId, finalRunId, finalOperationId, finalGeneration, pendingNext,
          helper || { channelPath: this.mergeLock.lockPath }, 'Pre-spawn invalidation could not confirm merge helper release.');
        return { started: false, reason: 'merge_lock_unconfirmed' };
      }
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
        t.execution.queue = undefined;
      });
    } catch (spawnIntentErr: any) {
      const helper = mergeRelease ? this.mergeLock.getActiveHelperEvidence() : undefined;
      let mergeReleased = true;
      if (mergeRelease) mergeReleased = await this.mergeLock.releaseAndConfirm(mergeRelease, PROCESS_STOP_TIMEOUT_MS);
      this.cleanupAcquired(null, sessionLease, undefined);
      if (!mergeReleased) {
        await this.markMergeHelperUnconfirmed(threadId, finalRunId, finalOperationId, finalGeneration, pendingNext,
          helper || { channelPath: this.mergeLock.lockPath }, `Spawn intent persistence failed and merge helper release was not confirmed: ${spawnIntentErr?.message || spawnIntentErr}`);
        return { started: false, reason: 'merge_lock_unconfirmed' };
      }
      await this.releaseStartingRunV2(threadId, finalRunId, finalOperationId, finalGeneration, 'invalidated_before_spawn', pendingNext);
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
      ownerBirthId,
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

  private async claimSessionForRun(
    thread: DelegationThread,
    role: 'developer' | 'reviewer',
    agent: AgentId,
    sessionId: string,
  ): Promise<{ ok: true } | { ok: false; code: 'SESSION_OWNERSHIP_CONFLICT' | 'SESSION_OWNERSHIP_UNVERIFIED'; reason: string }> {
    let fresh = this.taskStore.reloadTask(thread.threadId);
    if (!fresh?.execution || fresh.execution.version !== 2) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Task is not an eligible v2 execution record.' };
    }
    const expectedRun = thread.execution?.run;
    if (!expectedRun?.id || fresh.execution.run?.id !== expectedRun.id ||
        fresh.execution.operationId !== thread.execution?.operationId ||
        fresh.execution.generation !== thread.execution?.generation ||
        fresh.execution.run.state === 'stopping' || fresh.execution.run.unconfirmed) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Session claim is not bound to the current confirmed run identity.' };
    }
    let assignment = fresh.nativeSessionAssignments?.[role];
    if (!assignment) {
      const namespace = nativeSessionNamespace(agent);
      const expectedRunId = fresh.execution.run?.id;
      const expectedGeneration = fresh.execution.generation;
      try {
        fresh = await this.taskStore.transitionExecution(thread.threadId, fresh.execution.revision, (task) => {
          if (!task.execution || task.execution.run?.id !== expectedRunId || task.execution.generation !== expectedGeneration) {
            throw new Error('Run changed while pinning native session namespace.');
          }
          task.nativeSessionAssignments = { ...(task.nativeSessionAssignments || {}), [role]: { namespace, sessionEpoch: 1 } };
        });
        assignment = fresh.nativeSessionAssignments?.[role];
      } catch (error: any) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: error?.message || String(error) };
      }
    }
    if (!assignment?.namespace || !Number.isSafeInteger(assignment.sessionEpoch)) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: `Missing pinned ${role} native namespace or session epoch.` };
    }
    const actualNamespace = nativeSessionNamespace(agent);
    if (assignment.namespace !== actualNamespace) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: `Pinned native namespace no longer matches ${agent}'s CLI storage root.` };
    }
    const ownerInput = {
      agent, namespace: assignment.namespace, sessionId, threadId: thread.threadId, role,
      sessionEpoch: assignment.sessionEpoch,
    };
    const captured = role === 'developer' ? fresh.receiverSessionCaptured === true : fresh.reviewerSessionCaptured === true;
    const result = captured ? this.sessionOwnership.verify(ownerInput) : this.sessionOwnership.claim(ownerInput);
    return result.ok ? { ok: true } : { ok: false, code: result.code, reason: result.reason };
  }

  private async failSessionOwnership(
    threadId: string,
    code: 'SESSION_OWNERSHIP_CONFLICT' | 'SESSION_OWNERSHIP_UNVERIFIED',
    reason: string,
  ): Promise<void> {
    const current = this.taskStore.getTask(threadId);
    if (!current?.execution || current.execution.version !== 2) return;
    try {
      await this.taskStore.transitionExecution(threadId, current.execution.revision, (task) => {
        if (task.execution?.run) return;
        task.status = 'failed';
        task.failureRevision = (task.failureRevision || 0) + 1;
        task.failure = { stage: 'execution', code, reason, at: new Date().toISOString(), round: task.currentRound };
        task.execution!.lastOutcome = { code, reason, at: new Date().toISOString() };
      });
    } catch {}
  }

  private async releaseStartingRunV2(
    threadId: string,
    runId: string,
    operationId: string,
    generation: number,
    reason: 'session' | 'merge_lock' | 'merge_lock_error' | 'capacity' | 'invalidated_before_spawn',
    handoff?: ExecutionNext,
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
            if (handoff && !t.execution.next) t.execution.next = handoff;
            const isTerminalOrPaused =
              t.status === 'paused' ||
              t.status === 'completed' ||
              t.status === 'failed' ||
              t.status === 'wait_decision' ||
              Boolean(t.cleanup);
            if (!isTerminalOrPaused && reason !== 'invalidated_before_spawn') {
              t.status = 'queued';
              t.execution.queue = {
                reason: reason === 'session' ? 'session' : reason === 'merge_lock' || reason === 'merge_lock_error' ? reason : 'capacity',
                enteredAt: this.queueEnteredAt(t.execution),
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

  private queueEnteredAt(execution: ExecutionControl): string {
    const enteredAt = execution.queue?.enteredAt;
    return enteredAt && Number.isFinite(Date.parse(enteredAt)) ? enteredAt : new Date(this.wallClockNow()).toISOString();
  }

  private async recordMergeLockError(threadId: string, reason: string): Promise<void> {
    try {
      await this.transitionTask(threadId, (task) => {
        if (task.execution?.version === 2 && task.status === 'queued' && task.execution.queue?.reason === 'merge_lock_error') {
          task.execution.lastOutcome = { code: 'MERGE_LOCK_HELPER_ERROR', reason, at: new Date().toISOString() };
        }
      });
    } catch {}
  }

  private async markMergeHelperUnconfirmed(
    threadId: string,
    runId: string,
    operationId: string,
    generation: number,
    handoff: ExecutionNext | undefined,
    helper: { pid?: number; birthId?: string; channelPath: string },
    reason: string,
  ): Promise<void> {
    try {
      await this.transitionTask(threadId, (task) => {
        const control = task.execution;
        const run = control?.run;
        if (control?.version !== 2 || control.operationId !== operationId || control.generation !== generation ||
            run?.id !== runId || task.cleanup) return;
        run.state = 'stopping';
        run.step = 'merge_lock_release_unconfirmed';
        run.unconfirmed = true;
        run.mergeLockHelper = { ...helper, reason, observedAt: new Date().toISOString() };
        if (handoff && !control.next) control.next = handoff;
        control.queue = { reason: 'merge_lock_error', enteredAt: new Date().toISOString() };
        control.lastOutcome = { code: 'MERGE_LOCK_HELPER_UNCONFIRMED', reason, at: new Date().toISOString() };
        if (task.status !== 'paused') task.status = 'queued';
        appendInterruption(task, { kind: 'error', reason: 'MERGE_LOCK_HELPER_UNCONFIRMED', at: new Date().toISOString(), by: 'execution_controller' });
      });
    } catch {}
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
    let unconfirmedMergeHelper: { pid?: number; birthId?: string; channelPath: string } | undefined;
    type TrackedChild = ProcessIdentity & {
      process?: ChildProcess;
      descendants: ProcessIdentity[];
      belongsToSubtree: boolean;
      stopConfirmed: boolean;
    };
    const trackedChildren = new Map<string, TrackedChild>();
    let registrationUnconfirmed = false;
    let stopTrackedInFlight: Promise<boolean> | undefined;
    let stopTrackedChildren: () => Promise<boolean> = async () => false;
    let settleRun!: () => void;
    const settled = new Promise<void>((resolve) => { settleRun = resolve; });
    const abortFn = () => {
      aborted = true;
      if (trackedChildren.size) {
        void stopTrackedChildren().then((stopped) => {
          if (!stopped) registrationUnconfirmed = true;
        });
      }
    };
    this.activeExecutions.set(threadId, { abort: abortFn, activeSinceMs: this.monotonicNow(), settled });

    const guard = new HostSuspensionGuard({ monotonicNow: this.monotonicNow });
    guard.start();
    let lastMonotonic = this.monotonicNow();
    let accumulatedSpentMs = 0;
    const processIdentityKey = (identity: ProcessIdentity) => `${identity.pid}:${identity.birthId ?? 'unknown'}`;
    const refreshDescendants = async (tracked: TrackedChild) => {
      if (!tracked.belongsToSubtree || !tracked.birthId || windowsJobOf(tracked.process)) return;
      const captured = await captureChildDescendants(tracked.pid, tracked.birthId);
      const known = new Map(tracked.descendants.map((identity) => [processIdentityKey(identity), identity]));
      for (const identity of captured) {
        known.set(processIdentityKey(identity), identity);
      }
      tracked.descendants.splice(0, tracked.descendants.length, ...known.values());
    };
    stopTrackedChildren = async (): Promise<boolean> => {
      if (stopTrackedInFlight) return stopTrackedInFlight;
      const operation = (async () => {
        let allStopped = trackedChildren.size > 0 || !registrationUnconfirmed;
        for (const tracked of trackedChildren.values()) {
          if (tracked.stopConfirmed) continue;
          if (!tracked.belongsToSubtree) {
            allStopped = false;
            continue;
          }
          await refreshDescendants(tracked);
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
      for (const tracked of trackedChildren.values()) await refreshDescendants(tracked);
      if (aborted || this.isDisposed) return;
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
              const subtrees = item.execution.run.subtrees || (item.execution.run.subtrees = []);
              for (const tracked of trackedChildren.values()) {
                if (!tracked.birthId) continue;
                const rootKey = `${tracked.pid}:${tracked.birthId}`;
                let subtree = subtrees.find((candidate) => `${candidate.root.pid}:${candidate.root.birthId || 'unknown'}` === rootKey);
                if (!subtree) {
                  subtree = { root: { pid: tracked.pid, birthId: tracked.birthId }, descendants: [] };
                  subtrees.push(subtree);
                }
                const descendants = new Map(
                  subtree.descendants.map((identity) => [`${identity.pid}:${identity.birthId || 'unknown'}`, identity] as const),
                );
                for (const identity of tracked.descendants) {
                  descendants.set(`${identity.pid}:${identity.birthId || 'unknown'}`, structuredClone(identity));
                }
                subtree.descendants = [...descendants.values()];
              }
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
      const tracked: TrackedChild = {
        pid,
        birthId: candidateBirthId,
        process: childProcess,
        descendants: [],
        belongsToSubtree: false,
        stopConfirmed: false,
      };
      trackedChildren.set(trackedKey, tracked);
      const belongsToSubtree = await verifyCapturedSubtreeOwnership(pid, childProcess, candidateBirthId);
      if (belongsToSubtree && candidateBirthId && !windowsJobOf(childProcess)) {
        tracked.descendants = await captureChildDescendants(pid, candidateBirthId);
      }
      tracked.belongsToSubtree = belongsToSubtree;

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
          if (sessionLease) await this.sessionLock.attachChild(sessionLease, p);
          if (mergeRelease) await this.mergeLock.attachChildPid(p, undefined, candidateBirthId);
          await this.executionRegistry.attachChildPid(p, registration, candidateBirthId);
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
          { runId, operationId, generation, round },
        );
      } else if (phase === 'review') {
        phaseResult = await this.executeReviewAdapter(
          threadId,
          runId,
          params.purpose,
          confirmedOnChildPid,
          { runId, operationId, generation, round },
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
          const helper = this.mergeLock.getActiveHelperEvidence();
          const mergeReleased = await this.mergeLock.releaseAndConfirm(mergeRelease, PROCESS_STOP_TIMEOUT_MS);
          if (!mergeReleased) { unconfirmedStop = true; unconfirmedMergeHelper = helper; }
          else mergeRelease = undefined;
        } catch {
          unconfirmedStop = true;
          unconfirmedMergeHelper = this.mergeLock.getActiveHelperEvidence();
        }
      }
      // V2 session exclusivity is the static binding plus the unique run and
      // its durable process subtree. Bridge NativeSessionLock is legacy-only.

      if (unconfirmedStop) {
        try {
          const rev = this.taskStore.getTask(threadId)?.execution?.revision ?? 0;
          await this.taskStore.transitionExecution(threadId, rev, (t) => {
            if (t.execution?.run?.id === runId) {
              t.execution.run.state = 'stopping';
              t.execution.run.unconfirmed = true;
              if (unconfirmedMergeHelper) {
                t.execution.run.mergeLockHelper = {
                  ...unconfirmedMergeHelper,
                  reason: 'Merge helper release acknowledgement was not confirmed after phase execution.',
                  observedAt: new Date().toISOString(),
                };
              }
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

      // The run owner can attest exit even when pause or delete came from another process.
      try {
        const cleanup = this.taskStore.reloadTask(threadId)?.cleanup;
        if (!cleanup || (cleanup.action === 'delete' && cleanup.runId === runId &&
            cleanup.operationId === operationId && cleanup.generation === generation)) {
          const settled = await this.transitionTask(threadId, (task) => {
            const control = task.execution;
            const run = control?.run;
            if (control?.version !== 2 || run?.id !== runId || control.generation !== generation ||
                control.operationId !== operationId || run.state !== 'stopping' || run.unconfirmed ||
                run.owner.pid !== params.ownerIdentity.pid || run.owner.birthId !== params.ownerIdentity.birthId ||
                task.cleanup?.token !== cleanup?.token) return;
            if (task.status === 'wait_decision' && !cleanup) {
              control.run = undefined;
              if (task.decision?.answer && control.next?.sourceRunId === runId) {
                control.phase = control.next.phase;
                control.purpose = control.next.purpose;
                task.status = 'queued';
              }
            } else if (task.interruption?.kind === 'paused' && task.status !== 'completed') {
              control.run = undefined;
              control.queue = undefined;
              task.status = 'paused';
              const pausedRound = findInFlightRound(task);
              if (pausedRound) {
                pausedRound.status = 'paused';
                pausedRound.interruption = task.interruption;
              }
            }
          }, cleanup?.token, cleanup ? { runId, operationId, generation } : undefined);
          if (settled.execution?.run?.id !== runId) phaseResult = undefined;
        }
      } catch {}
      settleRun();
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
    this.wakeQueuedTasks();
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
    reason: 'role' | 'session' | 'merge_lock' | 'merge_lock_error' | 'capacity' | 'backoff' | 'quota',
    notBefore?: string,
  ): Promise<void> {
    const current = this.taskStore.getTask(threadId);
    if (current?.execution?.version === 2) {
      await this.transitionTask(threadId, (task) => {
        const control = task.execution;
        if (!control || control.version !== 2) return;
        const sourceRunId = control.run?.id;
        control.next = { phase, round: control.round, purpose: control.purpose, sourceRunId };
        control.phase = phase;
        control.run = undefined;
        control.queue = { reason: reason === 'role' || reason === 'session' ? 'capacity' : reason, notBefore, enteredAt: new Date().toISOString() };
        task.status = 'queued';
      });
      return;
    }
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
          (pid, _child, reportedBirthId) => {
            childPid = pid;
            if (pid) {
              childBirthId = reportedBirthId || getProcessBirthId(pid);
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
        phaseResult = await this.executeReviewAdapter(threadId, runId, params.purpose, (pid, _child, reportedBirthId) => {
          childPid = pid;
          if (pid) {
            childBirthId = reportedBirthId || getProcessBirthId(pid);
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
          (pid, _child, reportedBirthId) => {
            childPid = pid;
            if (pid) {
              childBirthId = reportedBirthId || getProcessBirthId(pid);
              this.executionRegistry.attachChildPid(pid, registration);
              void this.updateRunChild(threadId, runId, pid, childBirthId);
            } else {
              this.executionRegistry.attachChildPid(undefined, registration);
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
    identity?: { runId?: string; operationId?: string; generation?: number; round?: number },
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
      artifactDir: thread.artifactDir,
      model: thread.targetModel,
      taskRound: round.round,
      executionIdentity: thread.execution?.version === 2 ? {
        runId,
        operationId: identity?.operationId || thread.execution.operationId,
        generation: identity?.generation ?? thread.execution.generation,
        phase: 'develop',
        round: identity?.round ?? round.round,
        role: 'developer',
      } : undefined,
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

    let ownershipFailure: { code: 'SESSION_OWNERSHIP_CONFLICT' | 'SESSION_OWNERSHIP_UNVERIFIED'; reason: string } | undefined;
    // Static session ownership only applies to v2 tasks. Legacy dedicated/v1
    // tasks must keep their historical first-turn ID capture semantics.
    if (thread.execution?.version === 2 && result.actualSessionId && result.actualSessionId !== thread.receiverSessionId) {
      const ownership = await this.claimSessionForRun(thread, 'developer', thread.targetAgent, result.actualSessionId);
      if (!ownership.ok) {
        ownershipFailure = ownership;
        result.actualSessionId = undefined;
      }
    }

    // Check assessment
    const assessment = result.assessment;
    const actualOutput = result.extractedOutput || '';
    const success = !ownershipFailure && Boolean(assessment?.success && assessment.stream?.hasActualOutput);

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
        if (ownershipFailure) activeRound.error = `${ownershipFailure.code}: ${ownershipFailure.reason}`;
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
          code: ownershipFailure?.code || assessment?.errorCode || (success ? 'OK' : 'EXECUTION_FAILED'),
          reason: ownershipFailure?.reason || result.error || (success ? 'Success' : 'Execution failed'),
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

    const errorCode = ownershipFailure?.code || assessment?.errorCode || (result.exitCode !== 0 ? 'NONZERO_EXIT' : 'EMPTY_OUTPUT');
    const isTransient = !ownershipFailure && (typeof this.runner.isTransientError === 'function' ? this.runner.isTransientError(result) : false);
    const nonTransientCodes = new Set(['EMPTY_OUTPUT', 'TIMEOUT', 'UNKNOWN']);

    return {
      ok: false,
      code: errorCode,
      reason: ownershipFailure?.reason || result.error || `Execution failed with ${errorCode}`,
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
    identity?: { runId?: string; operationId?: string; generation?: number; round?: number },
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
      const bootstrapOutcome = await this.callbackManager.launchReviewerBootstrap(thread, onChildPid,
        thread.execution?.version === 2 ? {
          runId: identity?.runId || thread.execution.run?.id || runId,
          operationId: identity?.operationId || thread.execution.operationId,
          generation: identity?.generation ?? thread.execution.generation,
          phase: 'review',
          round: identity?.round ?? thread.execution.round,
          role: 'reviewer',
        } : undefined);
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
    const reviewerIdentity = thread.execution?.version === 2 ? {
      runId,
      operationId: identity?.operationId || thread.execution.operationId,
      generation: identity?.generation ?? thread.execution.generation,
      phase: 'review' as const,
      round: identity?.round ?? thread.execution.round,
      role: 'reviewer' as const,
    } : undefined;
    const wakeResult = await this.callbackManager.launchCaller(thread, reviewRound, onChildPid, reviewerIdentity);

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
      const finalizedMerge = phase === 'merge' && result.ok && t.status === 'completed' &&
        (t.worktree?.integration?.phase === 'merged' || t.worktree?.integration?.phase === 'no_changes');
      if (t.status === 'paused' || (t.status === 'completed' && !finalizedMerge) || t.status === 'failed') {
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

    const recordNext = (
      t: DelegationThread,
      nextPhase: ExecutionPhase,
      round: number,
      purpose?: 'bootstrap' | 'merge_resolution',
    ) => {
      validateRunMatch(t);
      t.execution!.next = { phase: nextPhase, round, purpose, sourceRunId: runId };
      t.execution!.phase = nextPhase;
      t.execution!.round = round;
      t.execution!.purpose = purpose;
      t.execution!.run = undefined;
      t.execution!.queue = undefined;
      t.execution!.attempt = 0;
      t.execution!.autoRetriesUsed = 0;
      t.execution!.budget.spentActiveMs = 0;
      t.status = 'queued';
    };

    if (result.ok) {
      // SUCCESS in this phase: Advance immediately to next phase!
      if (phase === 'develop') {
        // Develop succeeded -> advance directly to review!
        await this.transitionTask(threadId, (t) => {
          if (t.execution?.version === 2) recordNext(t, 'review', t.execution.round);
          else {
            validateRunMatch(t);
            t.execution!.phase = 'review';
            t.execution!.run = undefined;
            t.execution!.attempt = 0;
            t.execution!.autoRetriesUsed = 0;
            t.execution!.budget.spentActiveMs = 0;
          }
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
            if (t.execution?.version === 2) recordNext(t, 'develop', t.execution.round);
            else {
              validateRunMatch(t);
              t.execution!.phase = 'develop';
              t.execution!.purpose = undefined;
              t.execution!.run = undefined;
              t.execution!.attempt = 0;
              t.execution!.autoRetriesUsed = 0;
              t.execution!.budget.spentActiveMs = 0;
            }
          });
          void this.startExecution(threadId, 'develop');
        } else if (verdict === 'approve' || (thread.execution.version !== 2 && thread.worktree?.approvedCommit)) {
          // Approved -> advance directly to merge!
          await this.transitionTask(threadId, (t) => {
            if (t.execution?.version === 2) recordNext(t, 'merge', t.execution.round);
            else {
              validateRunMatch(t);
              t.execution!.phase = 'merge';
              t.execution!.run = undefined;
              t.execution!.attempt = 0;
              t.execution!.autoRetriesUsed = 0;
              t.execution!.budget.spentActiveMs = 0;
            }
          });
          void this.startExecution(threadId, 'merge');
        } else if (verdict === 'rework' || thread.status === 'running') {
          // Rework requested -> advance directly to develop!
          await this.transitionTask(threadId, (t) => {
            if (t.execution?.version === 2) recordNext(t, 'develop', t.currentRound || t.execution.round);
            else {
              validateRunMatch(t);
              t.execution!.phase = 'develop';
              t.execution!.run = undefined;
              t.execution!.attempt = 0;
              t.execution!.autoRetriesUsed = 0;
              t.execution!.budget.spentActiveMs = 0;
            }
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
    if (phase === 'merge' && thread.execution.version === 2 && thread.status === 'reviewing' &&
        !thread.worktree?.integration?.stashRecoveryPending) {
      await this.transitionTask(threadId, (t) => {
        if (t.status !== 'reviewing' || t.worktree?.integration?.stashRecoveryPending) {
          throw new Error('Merge review is no longer available.');
        }
        recordNext(t, 'review', t.execution!.round, isMergeReview(t) ? 'merge_resolution' : undefined);
      });
      void this.startExecution(threadId, 'review');
      return;
    }
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
        if (t.execution!.version === 2) {
          t.execution!.next = { phase, round: t.execution!.round, purpose: t.execution!.purpose, sourceRunId: runId };
        }
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
      if (failureStage === 'execution') {
        const failedRound = t.rounds.find(round => round.round === (t.execution?.round || t.currentRound));
        if (failedRound?.status === 'running') {
          failedRound.status = 'failed';
          failedRound.error = reason || code;
        }
      }
      if (failureStage === 'bootstrap') {
        t.reviewBootstrap = { ...(t.reviewBootstrap || { attempts: 0 }), phase: 'failed', lastError: reason || code };
        const placeholder = t.rounds.find(round => round.round === t.currentRound);
        if (placeholder?.status === 'running') { placeholder.status = 'failed'; placeholder.error = reason || code; }
      }
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
    if (thread) assertDedicatedTask(thread);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    const decisionId = crypto.randomUUID();
    const phase: ExecutionPhase = thread.execution?.phase || 'develop';
    const decision: TaskDecisionRecord = {
      id: decisionId,
      sourceRunId: thread.execution?.version === 2 ? thread.execution.run?.id : undefined,
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
    const capturedV2 = thread.execution?.version === 2 ? {
      operationId: thread.execution.operationId,
      generation: thread.execution.generation,
      phase: thread.execution.phase,
      runId: thread.execution.run?.id,
    } : undefined;

    // CAS FIRST: Persist decision and wait_decision before stopping any process
    await this.transitionTask(threadId, (t) => {
      if (capturedV2) {
        if (t.cleanup || t.execution?.version !== 2 || t.execution.operationId !== capturedV2.operationId ||
            t.execution.generation !== capturedV2.generation || t.execution.phase !== capturedV2.phase ||
            t.execution.run?.id !== capturedV2.runId) {
          throw new Error(`Task ${threadId} changed execution identity before persisting its decision.`);
        }
        if (capturedV2.runId && t.execution.next) {
          throw new Error(`Task ${threadId} already has a durable handoff from run ${capturedV2.runId}; another decision cannot replace it.`);
        }
        if (capturedV2.runId && (t.decision?.sourceRunId === capturedV2.runId ||
            (t.decisions || []).some((entry) => entry.sourceRunId === capturedV2.runId))) {
          throw new Error(`Task ${threadId} already has a decision reserved for run ${capturedV2.runId}.`);
        }
        if (t.decision && !t.decision.answer) throw new Error(`Task ${threadId} already has a pending decision.`);
      }
      t.decision = decision;
      t.decisions = [...(t.decisions || []), decision];
      t.status = 'wait_decision';
      if (t.execution?.run) {
        t.execution.run.state = 'stopping';
      }
    });

    if (capturedV2?.runId) {
      const run = thread.execution?.run;
      if (!run || run.id !== capturedV2.runId) return decision;
      const childPid = run.child?.pid;
      const childBirthId = run.child?.birthId;
      const isInsideChildTree = childPid ? isProcessDescendant(process.pid, childPid) : false;
      const active = this.activeExecutions.get(threadId);
      active?.abort();
      if (isInsideChildTree) return decision;

      if (childPid) {
        if (!childBirthId) {
          await this.markDecisionStopUnconfirmed(threadId, capturedV2, 'Decision cannot stop a registered child without its birthId.');
          return decision;
        }
        const descendants = (run.subtrees || []).flatMap((subtree) => subtree.descendants || []);
        let stopped = false;
        try {
          const job = run.subtrees?.find(tree => tree.root.pid === childPid && tree.root.birthId === childBirthId)?.windowsJob;
          stopped = await stopVerifiedChildTree(childPid, true, undefined, childBirthId, PROCESS_STOP_TIMEOUT_MS, descendants, true, job);
        } catch {}
        if (!stopped) {
          await this.markDecisionStopUnconfirmed(threadId, capturedV2, 'Decision could not confirm exit of the full captured child tree.');
          return decision;
        }
      } else if (!assessRunLaunchEvidence(run).safeToClean) {
        await this.markDecisionStopUnconfirmed(threadId, capturedV2, 'Decision arrived after spawn intent, but no bound child or safe no-spawn proof is available.');
        return decision;
      }

      if (active?.settled) {
        let timer: NodeJS.Timeout | undefined;
        const settled = await Promise.race([
          active.settled.then(() => true),
          new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(false), PROCESS_STOP_TIMEOUT_MS); timer.unref?.(); }),
        ]);
        if (timer) clearTimeout(timer);
        if (!settled) {
          await this.markDecisionStopUnconfirmed(threadId, capturedV2, 'Decision is waiting for execution resource release acknowledgement.');
          return decision;
        }
      } else {
        const { ownerStatus, childStatus, subtreeStatus } = this.inspectRunProcesses(run);
        if (ownerStatus !== 'exited' || (childPid && childStatus !== 'exited') || (childPid && subtreeStatus !== 'exited')) {
          await this.markDecisionStopUnconfirmed(threadId, capturedV2, 'Decision is durable, but the previous controller or child tree has not acknowledged exit.');
          return decision;
        }
      }

      await this.transitionTask(threadId, (task) => {
        const execution = task.execution;
        if (execution?.version !== 2 || execution.operationId !== capturedV2.operationId ||
            execution.generation !== capturedV2.generation || execution.phase !== capturedV2.phase ||
            execution.run?.id !== capturedV2.runId || task.decision?.id !== decision.id || task.cleanup) {
          throw new Error(`Decision ${decision.id} lost its run identity before exit acknowledgement.`);
        }
        execution.run = undefined;
      });
      return decision;
    }

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

  private async markDecisionStopUnconfirmed(
    threadId: string,
    identity: { operationId: string; generation: number; phase: ExecutionPhase; runId?: string },
    reason: string,
  ): Promise<void> {
    try {
      await this.transitionTask(threadId, (task) => {
        const execution = task.execution;
        if (execution?.version !== 2 || execution.operationId !== identity.operationId ||
            execution.generation !== identity.generation || execution.phase !== identity.phase ||
            execution.run?.id !== identity.runId || task.cleanup) return;
        appendInterruption(task, {
          kind: 'error', reason: 'PROCESS_STOP_UNCONFIRMED', at: new Date().toISOString(), by: 'decision',
        });
        if (execution.run) {
          execution.run.state = 'stopping';
          execution.run.unconfirmed = true;
          execution.lastOutcome = { code: 'PROCESS_STOP_UNCONFIRMED', reason, at: new Date().toISOString() };
        }
      });
    } catch {}
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
    if (thread) assertDedicatedTask(thread);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);
    if (thread.status !== 'wait_decision' || !thread.decision || thread.decision.id !== decisionId) {
      throw new Error(`Task ${threadId} has no pending decision matching ID ${decisionId}`);
    }
    if (thread.decision.answer) {
      throw new Error(`Decision ${decisionId} for task ${threadId} was already answered`);
    }

    if (thread.execution?.version !== 2 && thread.execution?.run?.state === 'stopping') {
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

    if (thread.execution?.version === 2) {
      const captured = {
        operationId: thread.execution.operationId,
        generation: thread.execution.generation,
        phase: thread.execution.phase,
        runId: thread.execution.run?.id,
      };
      const updated = await this.transitionTask(threadId, (task) => {
        const control = task.execution;
        if (task.cleanup || task.status !== 'wait_decision' || !task.decision || task.decision.id !== decisionId || task.decision.answer ||
            control?.version !== 2 || control.operationId !== captured.operationId || control.generation !== captured.generation ||
            control.phase !== captured.phase || (control.run?.id && control.run.id !== captured.runId) ||
            task.decision.sourceRunId !== captured.runId) {
          throw new Error(`Decision ${decisionId} changed execution identity before its answer could be committed.`);
        }
        if (control.next) {
          throw new Error(`Decision ${decisionId} cannot replace an existing durable handoff from run ${control.next.sourceRunId || 'unknown'}.`);
        }
        const answeredRecord: TaskDecisionRecord = {
          ...task.decision, answer, answeredAt: new Date().toISOString(), answeredBy: by,
        };
        if (task.decisions) {
          const index = task.decisions.findIndex((entry) => entry.id === decisionId);
          if (index >= 0) task.decisions[index] = answeredRecord;
          else task.decisions.push(answeredRecord);
        } else task.decisions = [answeredRecord];
        task.decision = answeredRecord;
        if (targetPhase === 'develop') {
          const inFlight = findInFlightRound(task) || task.rounds[task.rounds.length - 1];
          if (inFlight) inFlight.prompt += `\n\n[User Decision Answer]:\n${answer}\n[Decision ID: ${decisionId}]`;
        } else if (targetPhase === 'review') {
          const inFlight = task.rounds[task.rounds.length - 1];
          if (inFlight) inFlight.prompt += `\n\n[User Decision Answer]:\n${answer}\n[Decision ID: ${decisionId}]`;
        } else if (targetPhase === 'merge' && task.worktree) {
          task.summary.lastOutputSnippet = `[Decision Answer (${decisionId})]: ${answer.slice(0, 100)}`;
        }
        control.next = {
          phase: targetPhase,
          round: control.round,
          purpose: control.purpose,
          sourceRunId: control.run?.id,
        };
        if (control.run) {
          control.run.state = 'stopping';
          task.status = 'wait_decision';
        } else {
          control.phase = targetPhase;
          control.queue = undefined;
          task.status = 'queued';
        }
      });

      if (updated.execution?.run) return updated;
      await this.startExecution(threadId, targetPhase, { allowStates: ['queued'] });
      return this.taskStore.getTask(threadId) || updated;
    }

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
    if (!this.admissionEnabled) throw new Error('V2_STARTUP_BLOCKED: runtime component preflight has not passed.');
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);
    if (this.requireV2Execution) requireV2ExecutionEligibility(thread, 'retry this task');

    if (thread.execution?.version === 2) {
      const captured = thread.execution;
      const updated = await this.transitionTask(threadId, (t) => {
        const e = t.execution;
        if (!e || e.version !== 2 || e.generation !== captured.generation || e.operationId !== captured.operationId || e.phase !== captured.phase) {
          throw new Error(`Cannot retry task ${threadId}: execution identity changed`);
        }
        if (t.cleanup || e.next) throw new Error(`Cannot retry task ${threadId}: cleanup or handoff is pending`);
        if (t.status !== 'failed' && t.status !== 'queued') throw new Error(`Cannot retry task ${threadId}: state ${t.status} is not retryable`);
        if (e.queue?.notBefore) {
          const notBefore = Date.parse(e.queue.notBefore);
          if (!Number.isFinite(notBefore) || this.wallClockNow() < notBefore) throw new Error(`Cannot retry task ${threadId}: queue delay has not elapsed`);
        }
        if (e.run) {
          const { ownerStatus, childStatus, subtreeStatus } = this.inspectRunProcesses(e.run);
          if (e.run.state === 'starting' || e.run.state === 'active' || e.run.state === 'stopping' || e.run.unconfirmed ||
              ownerStatus !== 'exited' || (e.run.child?.pid ? childStatus !== 'exited' : childStatus !== 'none') ||
              (e.run.child?.pid ? subtreeStatus !== 'exited' : subtreeStatus !== 'none') || !assessRunLaunchEvidence(e.run).safeToClean) {
            throw new Error(`Cannot retry task ${threadId}: previous run exit is not confirmed`);
          }
        }
        clearTaskFailure(t);
        e.generation += 1;
        e.attempt = 0;
        e.autoRetriesUsed = 0;
        e.budget.spentActiveMs = 0;
        e.run = undefined;
        e.queue = undefined;
        if (!this.ownsRuns) t.status = 'queued';
      });
      const nextGen = updated.execution?.generation ?? captured.generation + 1;
      await this.startExecution(threadId, captured.phase, { allowStates: ['failed', 'queued'], generation: nextGen });
      return this.taskStore.getTask(threadId) || updated;
    }

    if (thread.execution?.run?.state === 'stopping' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
      const probe = this.probeRun(threadId, thread.execution?.run);
      if (probe === 'alive' || probe === 'unknown' || thread.interruption?.reason === 'PROCESS_STOP_UNCONFIRMED') {
        throw new Error(`Cannot retry task ${threadId}: previous process is still stopping or unconfirmed`);
      }
    }

    const phase: ExecutionPhase = thread.execution?.phase || 'develop';
    const nextGen = (thread.execution?.generation || 1) + 1;
    const updated = await this.transitionTask(threadId, (t) => {
      if (phase === 'develop') {
        const round = t.rounds.find(item => item.round === t.currentRound && item.type !== 'approval');
        if (!round) throw new Error('Cannot retry execution: current work round is missing.');
        round.status = 'running';
        round.error = undefined;
      }
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
    if (!this.admissionEnabled) throw new Error('V2_STARTUP_BLOCKED: runtime component preflight has not passed.');
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);
    if (this.requireV2Execution) requireV2ExecutionEligibility(thread, 'resume this task');

    if (thread.execution?.version === 2) {
      const captured = thread.execution;
      const updated = await this.transitionTask(threadId, (t) => {
        const e = t.execution;
        if (!e || e.version !== 2 || e.generation !== captured.generation || e.operationId !== captured.operationId || e.phase !== captured.phase) {
          throw new Error(`Cannot resume task ${threadId}: execution identity changed`);
        }
        if (t.cleanup || e.next) throw new Error(`Cannot resume task ${threadId}: cleanup or handoff is pending`);
        if (t.status !== 'paused' && t.status !== 'queued') throw new Error(`Cannot resume task ${threadId}: state ${t.status} is not resumable`);
        if (e.queue?.notBefore) {
          const notBefore = Date.parse(e.queue.notBefore);
          if (!Number.isFinite(notBefore) || this.wallClockNow() < notBefore) throw new Error(`Cannot resume task ${threadId}: queue delay has not elapsed`);
        }
        if (e.run) {
          const { ownerStatus, childStatus, subtreeStatus } = this.inspectRunProcesses(e.run);
          if (e.run.state === 'starting' || e.run.state === 'active' || e.run.state === 'stopping' || e.run.unconfirmed ||
              ownerStatus !== 'exited' || (e.run.child?.pid ? childStatus !== 'exited' : childStatus !== 'none') ||
              (e.run.child?.pid ? subtreeStatus !== 'exited' : subtreeStatus !== 'none') || !assessRunLaunchEvidence(e.run).safeToClean) {
            throw new Error(`Cannot resume task ${threadId}: previous run exit is not confirmed`);
          }
        }
        if (t.interruption?.kind === 'paused') t.interruption = undefined;
        e.run = undefined;
        e.queue = undefined;
      });
      this.runner.clearPendingCancellation(threadId);
      const cancellations = new CancellationRegistry(this.storageDir);
      cancellations.clear(threadId);
      if (cancellations.peek(threadId)) throw new Error(`Cannot resume task ${threadId}: cancellation request could not be cleared.`);
      if (!this.ownsRuns && updated.status === 'paused') {
        return this.taskStore.transitionExecution(threadId, updated.execution!.revision, (task) => {
          if (task.status !== 'paused' || task.cleanup || task.execution?.run || cancellations.peek(threadId)) {
            throw new Error('Task changed while preparing it to resume.');
          }
          task.status = 'queued';
        });
      }
      await this.startExecution(threadId, captured.phase, { allowStates: ['paused', 'queued'] });
      return this.taskStore.getTask(threadId) || updated;
    }

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
    cleanupToken?: string,
  ): Promise<DelegationThread> {
    this.taskStore.ensureUpToDate();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found: ${threadId}`);

    if (thread.execution?.version === 2) {
      const helperExitConfirmed = (run?: ExecutionRun): boolean => !run?.mergeLockHelper || this.mergeLock.confirmHelperExit({
        pid: run.mergeLockHelper.pid,
        birthId: run.mergeLockHelper.birthId,
        channelPath: run.mergeLockHelper.channelPath,
      });
      const captured = thread.execution;
      const capturedRun = captured.run ? structuredClone(captured.run) : undefined;
      if (!capturedRun) {
        return this.transitionTask(threadId, (task) => {
          const execution = task.execution;
          if (execution?.version !== 2 || execution.generation !== captured.generation ||
              execution.operationId !== captured.operationId || execution.run || task.cleanup) {
            throw new Error(`Cannot pause task ${threadId}: execution changed or cleanup is active.`);
          }
          appendInterruption(task, { kind: 'paused', reason, at: new Date().toISOString(), by });
          task.status = 'paused';
          execution.queue = undefined;
        });
      }

      const capturedNext = captured.next ? structuredClone(captured.next) : undefined;
      const cleanupMatches = (task: DelegationThread) => !task.cleanup || Boolean(
        cleanupToken && task.cleanup.token === cleanupToken && task.cleanup.action === 'delete' &&
        task.cleanup.runId === capturedRun.id && task.cleanup.operationId === captured.operationId &&
        task.cleanup.generation === captured.generation,
      );
      const marked = await this.transitionTask(threadId, (task) => {
        const execution = task.execution;
        if (execution?.version !== 2 || execution.generation !== captured.generation ||
            execution.operationId !== captured.operationId || execution.run?.id !== capturedRun.id || !cleanupMatches(task) ||
            JSON.stringify(execution.next) !== JSON.stringify(capturedNext)) {
          throw new Error(`Cannot pause task ${threadId}: run identity changed or cleanup is active.`);
        }
        execution.run.state = 'stopping';
        appendInterruption(task, { kind: 'paused', reason, at: new Date().toISOString(), by });
      }, cleanupToken);

      const active = this.activeExecutions.get(threadId);
      active?.abort();
      let localSettlement = false;
      if (active?.settled) {
        let timer: NodeJS.Timeout | undefined;
        localSettlement = await Promise.race([
          active.settled.then(() => true),
          new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(false), PROCESS_STOP_TIMEOUT_MS); timer.unref?.(); }),
        ]);
        if (timer) clearTimeout(timer);
      }

      // A controller which owns the run is the only process that can prove an
      // unbound spawn window closed. Its settled promise includes child-tree,
      // registry binding and merge-helper release acknowledgements.
      if (localSettlement) {
        const latest = this.taskStore.reloadTask(threadId);
        const run = latest?.execution?.run;
        const helperReleased = helperExitConfirmed(run);
        const evidence = this.inspectRunProcesses(run);
        const treeExited = Boolean(run &&
          (!run.child?.pid || (evidence.childStatus === 'exited' && evidence.subtreeStatus === 'exited')) &&
          (run.child?.pid || !run.spawnIntentAt || !run.unconfirmed));
        if (run?.id === capturedRun.id && !run.unconfirmed && helperReleased && treeExited) {
          return this.transitionTask(threadId, (task) => {
            const execution = task.execution;
            const current = execution?.run;
            if (execution?.version !== 2 || execution.generation !== captured.generation ||
                execution.operationId !== captured.operationId || current?.id !== capturedRun.id ||
                current.state !== 'stopping' || current.unconfirmed || !cleanupMatches(task) ||
                JSON.stringify(execution.next) !== JSON.stringify(capturedNext) ||
                !helperReleased || !helperExitConfirmed(current)) {
              throw new Error(`Cannot settle pause for ${threadId}: run or resource proof changed.`);
            }
            const freshEvidence = this.inspectRunProcesses(current);
            if (current.child?.pid && (freshEvidence.childStatus !== 'exited' || freshEvidence.subtreeStatus !== 'exited')) {
              throw new Error(`Cannot settle pause for ${threadId}: the complete child tree is not confirmed exited.`);
            }
            current.unconfirmed = false;
            execution.run = undefined;
            execution.queue = undefined;
            task.status = 'paused';
            const round = findInFlightRound(task);
            if (round) {
              round.status = 'paused';
              round.interruption = task.interruption;
            }
          }, cleanupToken, cleanupToken ? {
            runId: capturedRun.id, operationId: captured.operationId, generation: captured.generation,
          } : undefined);
        }
      } else if (!active) {
        // A different controller may own this run. We may stop only the exact
        // durably registered tree; no child means spawn/owner evidence remains
        // unknown and is deliberately retained for that owner to acknowledge.
        const current = this.taskStore.reloadTask(threadId);
        const currentRun = current?.execution?.run;
        if (currentRun?.id === capturedRun.id && currentRun.child?.pid && currentRun.child.birthId) {
          const descendants = (currentRun.subtrees || []).flatMap((tree) => tree.descendants || []);
          try {
            const job = currentRun.subtrees?.find(tree =>
              tree.root.pid === currentRun.child?.pid && tree.root.birthId === currentRun.child?.birthId)?.windowsJob;
            await stopVerifiedChildTree(currentRun.child.pid, true, undefined, currentRun.child.birthId,
              PROCESS_STOP_TIMEOUT_MS, descendants, true, job);
          } catch {}
        }
        const latest = this.taskStore.reloadTask(threadId);
        const run = latest?.execution?.run;
        if (run?.id === capturedRun.id && latest?.execution?.version === 2 &&
            latest.execution.generation === captured.generation && latest.execution.operationId === captured.operationId &&
            !run.unconfirmed && assessRunLaunchEvidence(run).safeToClean && helperExitConfirmed(run) &&
            this.inspectRunProcesses(run).ownerStatus === 'exited' &&
            (run.child?.pid
              ? this.inspectRunProcesses(run).childStatus === 'exited' && this.inspectRunProcesses(run).subtreeStatus === 'exited'
              : this.inspectRunProcesses(run).childStatus === 'none' && this.inspectRunProcesses(run).subtreeStatus === 'none')) {
          return this.transitionTask(threadId, (task) => {
            const execution = task.execution;
            const current = execution?.run;
            if (execution?.version !== 2 || execution.generation !== captured.generation ||
                execution.operationId !== captured.operationId || current?.id !== capturedRun.id ||
                current.state !== 'stopping' || current.unconfirmed || !cleanupMatches(task) ||
                JSON.stringify(execution.next) !== JSON.stringify(capturedNext) || !helperExitConfirmed(current)) {
              throw new Error(`Cannot settle pause for ${threadId}: run or resource proof changed.`);
            }
            const fresh = this.inspectRunProcesses(current);
            if (fresh.ownerStatus !== 'exited' || (current.child?.pid
                ? fresh.childStatus !== 'exited' || fresh.subtreeStatus !== 'exited'
                : fresh.childStatus !== 'none' || fresh.subtreeStatus !== 'none') ||
                !assessRunLaunchEvidence(current).safeToClean) {
              throw new Error(`Cannot settle pause for ${threadId}: durable process exit evidence is incomplete.`);
            }
            execution.run = undefined;
            execution.queue = undefined;
            task.status = 'paused';
            const round = findInFlightRound(task);
            if (round) { round.status = 'paused'; round.interruption = task.interruption; }
          }, cleanupToken, cleanupToken ? {
            runId: capturedRun.id, operationId: captured.operationId, generation: captured.generation,
          } : undefined);
        }
      }

      return this.taskStore.reloadTask(threadId) || marked;
    }

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
  private async settleReturnedNativeMaintenance(thread: DelegationThread): Promise<boolean> {
    const execution = thread.execution;
    const run = execution?.run;
    if (execution?.version !== 2 || !run || run.purpose !== 'native_maintenance' || !run.settlementPending || thread.status !== 'paused') return false;
    const noBusinessSpawn = !run.spawnIntentAt && !run.registeredAt && !run.child && !run.subtrees?.length;
    const completeTree = Boolean(
      run.spawnIntentAt && run.registeredAt && run.child?.pid && run.child.birthId &&
      Array.isArray(run.subtrees) && run.subtrees.some((tree) =>
        tree.root.pid === run.child!.pid && tree.root.birthId === run.child!.birthId) &&
      this.inspectRunProcesses(run).childStatus === 'exited' && this.inspectRunProcesses(run).subtreeStatus === 'exited'
    );
    if (!noBusinessSpawn && !completeTree) return false;
    const expectedRun = structuredClone(run);
    const expectedOperationId = execution.operationId;
    const expectedGeneration = execution.generation;
    const cleanupToken = thread.cleanup?.token;
    try {
      await this.taskStore.transitionExecution(thread.threadId, execution.revision, (current) => {
        const control = current.execution;
        const active = control?.run;
        if (current.status !== 'paused' || control?.version !== 2 || control.operationId !== expectedOperationId ||
            control.generation !== expectedGeneration || control.next || current.cleanup?.token !== cleanupToken ||
            JSON.stringify(active) !== JSON.stringify(expectedRun) || active?.purpose !== 'native_maintenance' ||
            !active.settlementPending) {
          throw new Error('Returned native maintenance settlement identity changed.');
        }
        const safeNoSpawn = !active.spawnIntentAt && !active.registeredAt && !active.child && !active.subtrees?.length;
        const freshEvidence = this.inspectRunProcesses(active);
        const safeTree = Boolean(active.spawnIntentAt && active.registeredAt && active.child?.pid && active.child.birthId &&
          Array.isArray(active.subtrees) && active.subtrees.some((tree) =>
            tree.root.pid === active.child!.pid && tree.root.birthId === active.child!.birthId) &&
          freshEvidence.childStatus === 'exited' && freshEvidence.subtreeStatus === 'exited');
        if (!safeNoSpawn && !safeTree) throw new Error('Native maintenance process tree exit is not confirmed.');
        control.run = undefined;
      }, cleanupToken, cleanupToken ? { runId: run.id, operationId: expectedOperationId, generation: expectedGeneration } : undefined);
      return true;
    } catch {
      return false;
    }
  }

  private wakeQueuedTasks(): void {
    if (!this.standingScanTimer || this.isDisposed || this.releaseWake) return;
    this.releaseWake = setImmediate(() => {
      this.releaseWake = undefined;
      if (this.drainInFlight) {
        void this.drainInFlight.then(() => this.wakeQueuedTasks(), () => this.wakeQueuedTasks());
      } else {
        void this.scanAndDrain().catch(() => {});
      }
    });
    this.releaseWake.unref?.();
  }

  private probeRunOwner(owner: ExecutionRun['owner']): Promise<'alive' | 'exited' | 'unknown'> | 'alive' | 'exited' | 'unknown' {
    if (!owner?.pid || !owner.birthId) return 'unknown';
    return owner.pid === process.pid
      ? probeProcessIdentity(owner.pid, owner.birthId)
      : probeProcessIdentityAsync(owner.pid, owner.birthId);
  }

  private readonly onDependencyChanged = (tasks: DelegationThread[], changedThreadIds?: string[]): void => {
    if (!changedThreadIds?.length) return;
    const settled = new Set(tasks.filter(task => changedThreadIds.includes(task.threadId) && !task.execution?.run &&
      (task.status === 'completed' || task.status === 'failed')).map(task => task.threadId));
    if (settled.size && tasks.some(task => task.execution?.version === 2 && task.reviewBlocked?.waitingForThreadId &&
        settled.has(task.reviewBlocked.waitingForThreadId))) this.wakeQueuedTasks();
  };

  public async scanAndDrain(): Promise<void> {
    if (!this.admissionEnabled || !this.ownsRuns || this.isDisposed) return;
    if (this.drainInFlight) return this.drainInFlight;
    const drain = this.drainLedger();
    this.drainInFlight = drain;
    try {
      await drain;
    } finally {
      this.drainInFlight = undefined;
    }
  }

  private async drainLedger(): Promise<void> {
    this.taskStore.ensureUpToDate();
    const tasks = this.taskStore.getAllTasks();
    const tasksById = new Map(tasks.map(task => [task.threadId, task]));
    const mergeCandidates: DelegationThread[] = [];
    let mergeAdmissionPending = false;
    const scheduleQueuedV2 = (task: DelegationThread, phase: ExecutionPhase) => {
      if (this.v2StartBlocker(task, phase)) return;
      if (phase === 'merge') mergeCandidates.push(task);
      else void this.startExecution(task.threadId, phase).catch(() => {});
    };

    for (const thread of tasks) {
      if (this.isDisposed) return;
      if (thread.reviewFlow !== 'dedicated') continue;
      // Production controllers are v2-only. Keep the legacy scanner branch
      // below for compatibility tests and explicitly constructed v1
      // controllers; production wiring sets requireV2Execution and therefore
      // never executes historical records.
      if (thread.execution?.version !== 2 && this.requireV2Execution) continue;
      if (thread.execution?.version === 2) {
        const phase = thread.execution.next?.phase ?? thread.execution.phase;
        if (thread.execution.run?.purpose === 'native_maintenance' && thread.execution.run.settlementPending) {
          await this.settleReturnedNativeMaintenance(thread);
          continue;
        }
        // A crashed owner's run is fed through the same v2 admission/recovery
        // classifier as an explicit start. Unknown identity remains untouched.
        if (thread.execution.run) {
          if (thread.execution.run.mergeLockHelper && thread.execution.run.state === 'stopping' && thread.execution.run.unconfirmed) {
            await this.startExecution(thread.threadId, phase);
            continue;
          }
          const ownerStatus = await this.probeRunOwner(thread.execution.run.owner);
          if (thread.execution.phase === 'merge' && thread.execution.run.state === 'starting' &&
              !thread.execution.run.unconfirmed && thread.execution.run.purpose !== 'native_maintenance' && ownerStatus === 'alive') {
            mergeAdmissionPending = true;
          }
          if (ownerStatus === 'exited') {
            if (phase === 'merge') scheduleQueuedV2(thread, phase);
            else await this.startExecution(thread.threadId, phase);
          }
          continue;
        }
        const dependencyId = reviewBlockDependencyId(thread);
        if (phase === 'review' && !thread.cleanup && !thread.execution.next &&
            (thread.status === 'reviewing' || thread.status === 'queued') && dependencyId &&
            reviewDependencyIssue(thread.threadId, dependencyId, id => tasksById.get(id))) {
          void this.failInvalidV2Dependency(thread).catch(() => {});
          continue;
        }
        // Recover a durable handoff whose queue status was not persisted by an
        // older or interrupted controller. The CAS retains its source run.
        if (thread.execution.next && thread.status !== 'queued' && !['paused', 'wait_decision', 'completed', 'failed'].includes(thread.status)) {
          if (this.v2StartBlocker(thread, phase)) continue;
          const revision = thread.execution.revision;
          try {
            const queued = await this.taskStore.transitionExecution(thread.threadId, revision, (fresh) => {
              if (fresh.execution?.version !== 2 || fresh.execution.run || !fresh.execution.next || fresh.cleanup) {
                throw new Error('The queued handoff changed.');
              }
              fresh.execution.phase = fresh.execution.next.phase;
              fresh.execution.round = fresh.execution.next.round;
              fresh.execution.purpose = fresh.execution.next.purpose;
              fresh.status = 'queued';
            });
            scheduleQueuedV2(queued, phase);
          } catch { continue; }
          continue;
        }
        if (thread.status === 'queued' || (thread.status === 'reviewing' && phase === 'review' && thread.reviewBlocked)) {
          scheduleQueuedV2(thread, phase);
        }
        continue;
      }
      if (!thread.execution) continue;
      const status = thread.status;
      const threadId = thread.threadId;

      if (status === 'queued') {
        if (thread.execution.queue?.notBefore) {
          const notBeforeTime = Date.parse(thread.execution.queue.notBefore);
          if (!Number.isFinite(notBeforeTime) || this.wallClockNow() < notBeforeTime) {
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

    if (mergeAdmissionPending || this.mergeLock.hasLocalOwner()) return;
    mergeCandidates.sort((a, b) => mergeQueueTime(a) - mergeQueueTime(b) || a.threadId.localeCompare(b.threadId));
    for (const candidate of mergeCandidates) {
      let attempted = false;
      const result = await this.startExecution(candidate.threadId, 'merge', { onMergeAdmission: () => { attempted = true; } });
      // Skip a task that changed or was blocked on its own prerequisites.
      // Once global-lock admission was attempted, wait for a release or next scan.
      if (attempted || result.started || result.reason === 'merge_lock_busy' ||
          result.reason === 'merge_lock_error' || result.reason === 'merge_lock_unconfirmed') break;
      if (result.reason === 'run_busy') {
        const current = this.taskStore.getTask(candidate.threadId)?.execution;
        const run = current?.run;
        if (current?.phase === 'merge' && run?.state === 'starting' && !run.unconfirmed &&
            run.purpose !== 'native_maintenance' && await this.probeRunOwner(run.owner) === 'alive') break;
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
    if (ExecutionController.instance === this) ExecutionController.instance = undefined;
    if (this.standingScanTimer) {
      clearInterval(this.standingScanTimer);
      this.standingScanTimer = undefined;
      this.taskStore.off('changed', this.onDependencyChanged);
    }
    if (this.releaseWake) {
      clearImmediate(this.releaseWake);
      this.releaseWake = undefined;
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
