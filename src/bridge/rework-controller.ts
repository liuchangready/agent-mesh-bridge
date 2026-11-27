import { taskArtifactBase } from './round-log';
import { assertDedicatedTask } from './types';
import {
  DelegationThread,
  DelegationRound,
  ExecutorOverride,
  RequestReworkParams,
  RequestReworkResult,
  ApproveTaskParams,
  ApproveResult,
  isValidSessionId,
  describeRoundInFlight,
  findInFlightRound,
  isReviewerBootstrapPending,
  nextRoundNumber,
  ThreadStatus,
  isIsolatedTaskWorktree,
  clearTaskFailure,
  isMergeReview,
} from './types';
import * as path from 'path';
import * as crypto from 'crypto';
import { archiveCompletedTaskLogs } from './task-summary';
import { TaskStore } from './task-store';
import { CallbackManager } from './callback-manager';
import { ModelApprovalHandler } from './model-approval';
import { TaskRetargetService, latestRetarget } from './task-retarget';
import { currentHead, snapshotTaskWorktree, gitOutput } from './git-worktree';
import { MergeCoordinator, MergeResult } from './merge-coordinator';
import { assertCanExecute } from './agent-capabilities';
import { createMergeLog, noteMergeFailure, resetMergeBudget } from './merge-recovery';
import { ExecutionController } from './execution-controller';
import { requireV2ExecutionEligibility } from './execution-gate';
import { reviewDependencyIssue } from './review-wake';

export class ReworkController {
  private taskStore: TaskStore;
  private callbackManager: CallbackManager;
  private readonly storageDir: string;
  /** Moves a task to another agent/model when rework/retry/resume asks for it. */
  private readonly retargetService: TaskRetargetService;
  private readonly mergeCoordinator: MergeCoordinator;
  private readonly executionAdmission: boolean;

  constructor(
    taskStore: TaskStore,
    callbackManager: CallbackManager,
    executionAdmission = true,
  ) {
    this.taskStore = taskStore;
    this.callbackManager = callbackManager;
    this.executionAdmission = executionAdmission;
    this.storageDir = path.dirname(taskStore.getStoragePath());
    this.mergeCoordinator = new MergeCoordinator(taskStore, this.storageDir, undefined, (thread) => {
      const reviewRound = [...(thread.rounds || [])].reverse().find((round) => round.type !== 'approval');
      if (reviewRound) void this.callbackManager.notifyReviewer(thread, reviewRound);
    });
    this.retargetService = new TaskRetargetService(taskStore, true, executionAdmission);
  }

  public getMergeCoordinator(): MergeCoordinator {
    return this.mergeCoordinator;
  }

  private assertExecutionAdmission(action: string): void {
    if (!this.executionAdmission) throw new Error(`V2_STARTUP_BLOCKED: runtime component preflight has not passed; cannot ${action}.`);
  }

  /** Answer model-switch confirmations raised by an executor switch. */
  public setModelApprovalHandler(handler: ModelApprovalHandler | undefined): void {
    this.retargetService.setModelApprovalHandler(handler);
  }

  public retargetTask(threadId: string, overrides: ExecutorOverride, reason: string, by = 'caller') {
    this.assertExecutionAdmission('retarget this task');
    const thread = this.taskStore.getTask(threadId);
    requireV2ExecutionEligibility(thread, 'retarget this task');
    return this.retargetService.retargetTask({ threadId, ...overrides, reason, by });
  }

  /**
   * Apply a rework/retry/resume executor override, if the caller asked for one.
   * A model-only request against the same agent keeps the native session; a new
   * agent always starts a fresh one (see `TaskRetargetService`).
   */
  private async applyExecutorOverride(
    threadId: string,
    overrides: ExecutorOverride,
    reason: string,
  ): Promise<void> {
    if (!this.retargetService.hasOverride(overrides)) return;
    await this.retargetService.retargetTask({
      threadId,
      targetAgent: overrides.targetAgent,
      model: overrides.model,
      modelRequestedByUser: overrides.modelRequestedByUser,
      reason,
      by: 'caller',
    });
  }

  /**
   * Dispatches a rework round to the target agent within the EXACT SAME native session.
   * STRICT RULE: never regenerate receiverSessionId — unless the caller explicitly
   * moves the task to another agent/model, in which case the session cannot
   * follow (see `TaskRetargetService`).
   */
  public async requestRework(params: RequestReworkParams): Promise<RequestReworkResult> {
    this.assertExecutionAdmission('request rework');
    let thread = this.taskStore.getTask(params.threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
    requireV2ExecutionEligibility(thread, 'request rework for this task');
    // `completed` is terminal: a stale reviewer must not resurrect an archived
    // task by submitting a late rework.
    if (thread.status === 'completed' || thread.archivedAt) {
      throw new Error(
        `Cannot request rework: thread ${params.threadId} is already approved and archived. ` +
          `Dispatch a new task if the work has to continue.`,
      );
    }
    if (thread.status === 'paused') {
      throw new Error(`Cannot request rework: task ${params.threadId} is paused; resume it first.`);
    }
    if ((thread.status === 'failed' && thread.failure?.stage === 'merge') || thread.status === 'wait_decision' || thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge')) throw new Error(`Cannot request rework while task is ${thread.status}.`);
    if (thread.worktree?.integration?.stashRecoveryPending) throw new Error(`Cannot request rework while stash metadata recovery is pending.`);
    this.assertDedicatedReviewerReady(thread, 'request rework');
    assertCanExecute(params.targetAgent || thread.targetAgent, 'receive rework');
    if (
      thread.reviewFlow === 'dedicated' && params.callerSessionId &&
      params.callerSessionId !== thread.reviewerSessionId
    ) {
      throw new Error(
        `Cannot request rework: dedicated reviewer session is fixed to ${thread.reviewerSessionId}.`,
      );
    }

    // Optional executor switch: applied before the round is created, so a
    // declined model switch aborts the rework without touching the ledger.
    let retarget = latestRetarget(thread);
    let modelNotice: string | undefined;
    if (this.retargetService.hasOverride(params)) {
      const outcome = await this.retargetService.retargetTask({
        threadId: params.threadId,
        targetAgent: params.targetAgent,
        model: params.model,
        modelRequestedByUser: params.modelRequestedByUser,
        reason: 'request_rework: executor switch',
        by: 'caller',
      });
      modelNotice = outcome.modelNotice;
      this.taskStore.loadFromDisk();
      const reloaded = this.taskStore.getTask(params.threadId);
      if (!reloaded) {
        throw new Error(`Task thread not found after retarget: ${params.threadId}`);
      }
      thread = reloaded;
      this.assertReworkStillEligible(thread, params);
      retarget = outcome.status === 'RETARGETED' ? latestRetarget(thread) : retarget;
    }

    if (!thread.receiverSessionId) {
      throw new Error(
        `Cannot request rework: thread ${params.threadId} is missing receiverSessionId.`,
      );
    }

    // Hard invariant: a task may only have ONE round in flight. Starting the
    // next round while the previous one is still running makes two agent
    // processes drive the same workspace at once.
    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot request rework: ${describeRoundInFlight(thread, inFlight)}`);
    }

    // A rework that repeats the previous instruction after a round that changed
    // no file cannot produce anything new. That is how a reviewer "waits" for a
    // sibling task, and it was turning into an endless loop of empty rounds.
    assertNotNoopRework(thread, params.feedback);

    // Monotonic numbering: never reuse a round number even if currentRound was
    // rewound by a retry.
    const nextRound = nextRoundNumber(thread);
    const nowIso = new Date().toISOString();

    const reworkRound: DelegationRound = {
      round: nextRound,
      type: 'rework',
      prompt: params.feedback,
      status: 'running',
      durationMs: 0,
      timestamp: nowIso,
    };

    // A caller with nothing new to say re-sends the original instruction to mean
    // "do it again". Record that explicitly so the ledger reads as a repeat of an
    // earlier round rather than as a second, independent task.
    const previousRound = thread.rounds[thread.rounds.length - 1];
    const normalizedFeedback = (params.feedback || '').trim();
    if (
      previousRound?.prompt &&
      normalizedFeedback.length > 0 &&
      previousRound.prompt.trim() === normalizedFeedback
    ) {
      reworkRound.repeatOfRound = previousRound.round;
      console.warn(
        `[ReworkController] Round ${nextRound} of ${thread.threadId} repeats the instruction of round ${previousRound.round} verbatim; ` +
          `use retry_task to re-run a failed round in place instead of re-sending its prompt.`,
      );
    }

    // A resumed session keeps its continuity; a task that was just moved to
    // another agent has no resumable session and starts a fresh one.
    const originalReviewRound = thread.currentRound;
    const expectedOperationId = thread.execution?.operationId;
    const expectedGeneration = thread.execution?.generation;
    const expectedRunId = thread.execution?.run?.id;
    const expectedReviewerSession = thread.reviewerSessionId;

    thread.currentRound = nextRound;
    clearTaskFailure(thread);
    thread.status = 'running';
    if (thread.worktree?.integration && isMergeReview({ ...thread, status: 'reviewing' })) {
      thread.worktree.approvedCommit = undefined;
      thread.worktree.integration = { ...thread.worktree.integration, phase: 'ready', updatedAt: nowIso };
    }
    thread.interruption = undefined;
    thread.updatedAt = nowIso;
    thread.callerSessionId = thread.reviewerSessionId;
    thread.rounds.push(reworkRound);
    thread.summary.lastPromptSnippet = (params.feedback || '').slice(0, 100);
    thread.summary.lastOutputSnippet = retarget && retarget.toAgent !== retarget.fromAgent
      ? `Reworking on ${thread.targetAgent} with a fresh session (Round ${nextRound})...`
      : `Reworking in same session (Round ${nextRound})...`;

    // Persist immediately
    let finalHasActiveRun = false;
    {
      for (let attempt = 0; attempt < 3; attempt++) {
        this.taskStore.loadFromDisk();
        const freshTask = this.taskStore.getTask(thread.threadId);
        if (!freshTask) {
          throw new Error(`Task thread not found: ${thread.threadId}`);
        }
        assertDedicatedTask(freshTask);
        requireV2ExecutionEligibility(freshTask, 'commit this rework');
        const currentRev = freshTask.execution.revision;
        try {
          await this.taskStore.transitionExecution(thread.threadId, currentRev, (t) => {
            assertDedicatedTask(t);
            requireV2ExecutionEligibility(t, 'commit this rework');
            if (expectedOperationId && t.execution.operationId !== expectedOperationId) {
              throw new Error(
                `Cannot request rework: execution operation changed concurrently (expected ${expectedOperationId}, found ${t.execution.operationId})`,
              );
            }
            if (expectedGeneration !== undefined && t.execution.generation !== expectedGeneration) {
              throw new Error(
                `Cannot request rework: execution generation changed concurrently (expected ${expectedGeneration}, found ${t.execution.generation})`,
              );
            }
            if (expectedRunId && t.execution.run?.id !== expectedRunId) {
              throw new Error(
                `Cannot request rework: execution run changed concurrently (expected ${expectedRunId}, found ${t.execution.run?.id})`,
              );
            }
            if (t.execution.next) {
              throw new Error(`Cannot request rework: run ${t.execution.next.sourceRunId || 'unknown'} already has a durable handoff.`);
            }
            if (!expectedRunId && t.execution.run) {
              throw new Error(
                `Cannot request rework: a new execution run (${t.execution.run.id}) started concurrently`,
              );
            }

            if (
              t.status === 'completed' ||
              t.status === 'paused' ||
              (t.status === 'failed' && t.failure?.stage === 'merge') ||
              t.status === 'wait_decision'
            ) {
              throw new Error(`Cannot request rework while task is ${t.status}`);
            }
            if (t.currentRound !== originalReviewRound) {
              throw new Error(
                `Cannot request rework: task round advanced concurrently (expected round ${originalReviewRound}, found ${t.currentRound})`,
              );
            }
            if (expectedReviewerSession && t.reviewerSessionId && t.reviewerSessionId !== expectedReviewerSession) {
              throw new Error(
                `Cannot request rework: reviewer session changed concurrently (expected ${expectedReviewerSession}, found ${t.reviewerSessionId})`,
              );
            }

            const currentHasActiveRun = Boolean(t.execution?.run);
            finalHasActiveRun = currentHasActiveRun;

            t.currentRound = nextRound;
            clearTaskFailure(t);
            t.status = 'running';
            t.interruption = undefined;
            t.callerSessionId = thread.callerSessionId;
            t.receiverSessionId = thread.receiverSessionId;
            t.targetAgent = thread.targetAgent;
            t.targetModel = thread.targetModel;
            t.retargets = thread.retargets;

            const existingRoundIdx = t.rounds.findIndex((r) => r.round === nextRound);
            if (existingRoundIdx >= 0) {
              t.rounds[existingRoundIdx] = reworkRound;
            } else {
              t.rounds.push(reworkRound);
            }

            t.summary = {
              ...t.summary,
              lastPromptSnippet: (params.feedback || '').slice(0, 100),
              lastOutputSnippet: retarget && retarget.toAgent !== retarget.fromAgent
                ? `Reworking on ${t.targetAgent} with a fresh session (Round ${nextRound})...`
                : `Reworking in same session (Round ${nextRound})...`,
            };

            t.execution.next = { phase: 'develop', round: nextRound, sourceRunId: expectedRunId };
          });
          break;
        } catch (err: any) {
          if (err?.message?.includes('revision mismatch') && attempt < 2) {
            continue;
          }
          throw err;
        }
      }

    }

    if (!finalHasActiveRun) {
      const controller = ExecutionController.getInstance();
      if (!controller) throw new Error('V2 execution controller is unavailable; rework was recorded but not started.');
      void controller.startExecution(thread.threadId, 'develop');
    }

    const switched = !!retarget && retarget.toAgent !== retarget.fromAgent;
    return {
      threadId: thread.threadId,
      round: nextRound,
      receiverSessionId: thread.receiverSessionId,
      targetAgent: thread.targetAgent,
      status: 'REWORK_DISPATCHED',
      retarget,
      modelNotice,
      message: switched
        ? `Rework Round ${nextRound} switched to ${thread.targetAgent}` +
          `${thread.targetModel ? ` (${thread.targetModel})` : ''} with a fresh native session ` +
          `(${thread.receiverSessionId}); the previous agent's session cannot be resumed. Conclude your turn and await callback.`
        : `Rework Round ${nextRound} dispatched to ${thread.targetAgent} in original session (${thread.receiverSessionId}). Conclude your turn and await callback.`,
    };
  }

  public async retryFailedRound(threadId: string, overrides: ExecutorOverride = {}): Promise<DelegationRound> {
    this.assertExecutionAdmission('retry this task');
    const thread = this.taskStore.getTask(threadId);
    requireV2ExecutionEligibility(thread, 'retry this task');
    if (this.retargetService.hasOverride(overrides)) {
      await this.applyExecutorOverride(threadId, overrides, 'retry_task: executor switch');
    }
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; retry was not started.');
    await controller.retryOperation(threadId);
    const updated = this.taskStore.getTask(threadId);
    const round = updated?.rounds.find((entry) => entry.round === updated.execution?.round && entry.type !== 'approval');
    if (!round) throw new Error(`Retry was accepted but round ${updated?.execution?.round ?? 'unknown'} is missing.`);
    return round;
  }
  /**
   * Resume a paused round in place. The original prompt and native session are
   * preserved; the interruption reason stays in history, and the current
   * interruption marker is cleared.
   *
   * `overrides` may resume the task on another agent/model (fresh session) — e.g.
   * pause a task whose model is rate-limited, switch, then resume.
   */
  public async resumePausedTask(threadId: string, overrides: ExecutorOverride = {}): Promise<DelegationRound> {
    this.assertExecutionAdmission('resume this task');
    const thread = this.taskStore.getTask(threadId);
    requireV2ExecutionEligibility(thread, 'resume this task');
    if (this.retargetService.hasOverride(overrides)) {
      await this.applyExecutorOverride(threadId, overrides, 'resume_task: executor switch');
    }
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; resume was not started.');
    await controller.resumeOperation(threadId);
    const updated = this.taskStore.getTask(threadId);
    const roundNumber = updated?.execution?.round;
    const round = updated?.rounds.find((entry) => entry.round === roundNumber && entry.type !== 'approval');
    if (!round) throw new Error(`Resume was accepted but round ${roundNumber ?? 'unknown'} is missing.`);
    return round;
  }
  private assertDedicatedReviewerReady(thread: DelegationThread, operation: string): void {
    assertDedicatedTask(thread);
    if (thread.reviewFlow === 'dedicated' && isReviewerBootstrapPending(thread)) {
      const phase = thread.reviewBootstrap?.phase || 'missing';
      throw new Error(
        `Cannot ${operation} for ${thread.threadId}: dedicated reviewer bootstrap is ${phase} ` +
          `or its captured reviewer session is invalid. Complete or safely resume the existing ` +
          `reviewer handoff in this thread before starting developer work.`,
      );
    }
  }

  private assertReworkStillEligible(thread: DelegationThread, params: RequestReworkParams): void {
    if ((thread.status === 'failed' && thread.failure?.stage === 'merge') || thread.status === 'wait_decision') throw new Error(`Cannot request rework while task is ${thread.status}.`);
    if (thread.worktree?.integration?.stashRecoveryPending) throw new Error('Cannot request rework while stash metadata recovery is pending.');
    if (thread.archivedAt || thread.status === 'completed') {
      throw new Error(`Cannot request rework: task ${thread.threadId} became completed or archived during retarget.`);
    }
    if (thread.status === 'paused') {
      throw new Error(`Cannot request rework: task ${thread.threadId} became paused during retarget.`);
    }
    this.assertDedicatedReviewerReady(thread, 'request rework');
    if (
      thread.reviewFlow === 'dedicated' &&
      (!isValidSessionId(thread.reviewerSessionId) ||
        thread.callerSessionId !== thread.reviewerSessionId ||
        (params.callerSessionId && params.callerSessionId !== thread.reviewerSessionId))
    ) {
      throw new Error(`Cannot request rework: dedicated reviewer session changed during retarget.`);
    }
  }

  /**
   * Park a review on a dependency instead of spending a rework round.
   *
   * The reviewer has exactly two moves — approve or rework — so "this task is
   * fine, but the batch is waiting for another task" had no expression and was
   * encoded as an empty rework round that returned in 15 seconds and burned a
   * full execution slot, over and over. Blocking keeps the task `reviewing`
   * (queue invariants unchanged), silences the stall watchdog and the wake path,
   * and wakes the reviewer as soon as the dependency reaches a terminal state.
   *
   * Unlike `pause_task` this is not a user interruption: it records no
   * `interruption`, keeps the review round intact, and needs no manual resume.
   */
  public async blockReview(params: {
    threadId: string;
    reason?: string;
    waitingForThreadId?: string;
    by?: string;
  }): Promise<{ threadId: string; status: ThreadStatus; waitingForThreadId?: string; message: string }> {
    this.assertExecutionAdmission('change review state');
    const thread = this.taskStore.getTask(params.threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
    requireV2ExecutionEligibility(thread, 'change the review state of this task');
    if (thread.status !== 'reviewing') {
      throw new Error(
        `Cannot block review: thread ${params.threadId} is "${thread.status}", not "reviewing".`,
      );
    }
    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot block review: ${describeRoundInFlight(thread, inFlight)}`);
    }

    const park = (task: DelegationThread) => {
      const nowIso = new Date().toISOString();
      task.reviewBlocked = {
        waitingForThreadId: params.waitingForThreadId,
        reason: params.reason?.slice(0, 500),
        at: nowIso,
        by: params.by || 'caller',
      };
      task.reviewWake = undefined;
      task.updatedAt = nowIso;
      task.summary.lastOutputSnippet = params.waitingForThreadId
        ? `Blocked: waiting for ${params.waitingForThreadId}`
        : 'Blocked: waiting for an external dependency';
    };

    await this.taskStore.transitionExecution(thread.threadId, thread.execution.revision, fresh => {
      const execution = fresh.execution;
      if (execution?.version !== 2 || execution.phase !== 'review' || execution.next || fresh.cleanup ||
          execution.run?.state === 'stopping' || execution.run?.unconfirmed || fresh.status !== 'reviewing') {
        throw new Error('Cannot block review: the current review changed before it could be parked.');
      }
      const issue = reviewDependencyIssue(fresh.threadId, params.waitingForThreadId, id => this.taskStore.getTask(id));
      if (issue) throw new Error(`Cannot block review: ${issue}`);
      const sameDependency = fresh.reviewBlocked?.waitingForThreadId === params.waitingForThreadId;
      const queue = execution.queue;
      const enteredAt = sameDependency && queue?.reason === 'dependency' ? queue.enteredAt : undefined;
      park(fresh);
      execution.queue = { reason: 'dependency', notBefore: queue?.notBefore,
        enteredAt: enteredAt ?? fresh.reviewBlocked!.at };
    });


    return {
      threadId: thread.threadId,
      status: thread.status,
      waitingForThreadId: params.waitingForThreadId,
      message: params.waitingForThreadId
        ? `Review parked. The task will be woken automatically when ${params.waitingForThreadId} reaches a terminal state; no rework round was created.`
        : `Review parked. Clear the block (or approve/rework) once the external dependency is resolved; no rework round was created.`,
    };
  }

  /**
   * Approves and archives the task.
   *
   * Approval is a terminal bookkeeping round, so the same one-round-in-flight
   * rule applies: a thread whose work round is still executing must not be
   * marked completed — that leaves the thread "completed" while the agent keeps
   * working, burning tokens on an orphaned round.
   */
  public async approveTask(params: ApproveTaskParams): Promise<ApproveResult> {
    this.assertExecutionAdmission('approve this task');
    const thread = this.taskStore.getTask(params.threadId);
    if (thread) assertDedicatedTask(thread);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
    requireV2ExecutionEligibility(thread, 'approve this task');
    if (thread.archivedAt) throw new Error(`Cannot approve archived task ${params.threadId}.`);
    if (thread.status === 'failed' && thread.failure?.stage === 'bootstrap') throw new Error('Cannot approve before the reviewer bootstrap handoff.');

    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot approve: ${describeRoundInFlight(thread, inFlight)}`);
    }

    // Idempotent: re-approving must not pile up duplicate approval rounds.
    if (thread.status === 'completed') {
      return {
        threadId: thread.threadId,
        status: 'COMPLETED',
        message: `Task ${thread.threadId} is already approved and archived; nothing to do.`,
      };
    }
    this.assertDedicatedReviewerReady(thread, 'approve task');
    if ((thread.status === 'failed' && thread.failure?.stage === 'merge') || thread.status === 'wait_decision' || thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge')) throw new Error(`Cannot approve while task is ${thread.status}.`);
    if (thread.worktree?.integration?.stashRecoveryPending) throw new Error('Cannot approve while stash metadata recovery is pending.');

    const nowIso = new Date().toISOString();
    let status: ApproveResult['status'] = 'COMPLETED';
    clearTaskFailure(thread);
    if (isIsolatedTaskWorktree(thread.worktree)) {
      const worktree = thread.worktree;
      let snapshot: ReturnType<typeof snapshotTaskWorktree>;
      try {
        snapshot = snapshotTaskWorktree(worktree);
        gitOutput(worktree.path, ['update-ref', `refs/mesh/candidates/${thread.threadId}`, snapshot.commit]);
        if (worktree.approvedCommit !== snapshot.commit && thread.mergeControl) resetMergeBudget(thread);
        if (worktree.approvedCommit !== snapshot.commit && worktree.mergeResolutionPath) {
          worktree.supersededMergeResolutionPaths = [...new Set([...(worktree.supersededMergeResolutionPaths || []), worktree.mergeResolutionPath])];
          worktree.mergeResolutionPath = undefined;
          worktree.mergeResolutionSubmitted = false;
          worktree.mergeResolutionSourceCommit = undefined;
        }
        worktree.approvedSnapshot = snapshot;
      } catch (error) {
        let failure = `Could not create approved snapshot: ${String(error)}`;
        const number = (thread.mergeHistory?.length || 0) + 1;
        const safeId = thread.threadId.replace(/[^A-Za-z0-9_-]/g, '_');
        const dir = path.join(taskArtifactBase(thread.threadId, this.storageDir, thread.artifactDir), 'merge_log');
        let logPath: string | undefined; let logFileNumber: number | undefined;
        try {
          ({ logPath, logFileNumber } = createMergeLog(dir, safeId, number, `${failure}\n`));
        } catch (logError) { logPath = undefined; failure += `; could not write merge log: ${String(logError)}`; }
        const updated = await this.taskStore.transitionExecution(thread.threadId, thread.execution.revision, (fresh) => {
          assertDedicatedTask(fresh);
          requireV2ExecutionEligibility(fresh, 'record this snapshot failure');
          const current = fresh.worktree;
          if (!current || fresh.execution.operationId !== thread.execution.operationId ||
              fresh.execution.generation !== thread.execution.generation ||
              fresh.execution.run?.id !== thread.execution.run?.id || fresh.execution.next ||
              fresh.currentRound !== thread.currentRound || fresh.status !== thread.status) {
            throw new Error('Cannot record snapshot failure after the current review changed.');
          }
          fresh.status = 'reviewing';
          const budget = noteMergeFailure(fresh, 'snapshot', 'SNAPSHOT_FAILED', failure);
          fresh.mergeHistory = [...(fresh.mergeHistory || []), { id: crypto.randomUUID(),
            number: (fresh.mergeHistory?.length || 0) + 1, at: nowIso, phase: 'snapshot',
            result: 'failed', error: failure, logPath, logFileNumber, ...budget }];
          current.approvedCommit = undefined;
          current.approvedSnapshot = undefined;
          current.mergePending = true;
          current.integration = { phase: 'blocked', error: failure, updatedAt: nowIso };
        });
        const reviewRound = [...updated.rounds].reverse().find((round) => round.type !== 'approval');
        if (reviewRound && updated.status === 'reviewing') {
          setTimeout(() => { void this.callbackManager.notifyReviewer(updated, reviewRound); }, 0).unref();
        }
        return { threadId: thread.threadId, status: updated.status === 'failed' ? 'MERGE_FAILED' : 'REVIEWING', message: failure };
      }
      if (snapshot.tree === gitOutput(worktree.path, ['rev-parse', `${worktree.baseCommit}^{tree}`])) {
        worktree.approvedCommit = undefined;
        worktree.integration = { phase: 'no_changes', sourceCommit: snapshot.commit, updatedAt: nowIso };
        worktree.mergePending = false;
        thread.status = 'completed';
      } else {
        worktree.approvedCommit = snapshot.commit;
        worktree.integration = { phase: 'waiting', sourceCommit: snapshot.commit, updatedAt: nowIso };
        worktree.mergePending = true;
        thread.status = 'queued';
        thread.queuePhase = 'merge';
        status = 'QUEUED';
      }
    } else {
      thread.status = 'completed';
    }
    thread.updatedAt = nowIso;

    const originalReviewRound = thread.currentRound;
    const expectedOperationId = thread.execution?.operationId;
    const expectedGeneration = thread.execution?.generation;
    const expectedRunId = thread.execution?.run?.id;
    const expectedReviewerSession = thread.reviewerSessionId;

    let approvalRound: DelegationRound | undefined;
    if (params.remarks) {
      approvalRound = {
        // Monotonic numbering keeps approval entries from colliding with a
        // round number that a retry already reused.
        round: nextRoundNumber(thread),
        type: 'approval',
        prompt: params.remarks,
        output: status === 'QUEUED' ? 'Review approved; waiting for verified local integration.' : 'Task successfully approved and verified.',
        durationMs: 0,
        status: 'completed',
        timestamp: nowIso,
      };
      thread.rounds.push(approvalRound);
      thread.currentRound = approvalRound.round;
      thread.summary.lastPromptSnippet = params.remarks.slice(0, 100);
      thread.summary.lastOutputSnippet = 'Approved';
    }

    let finalHasActiveRun = false;
    const doTransition = async () => {
      for (let attempt = 0; attempt < 3; attempt++) {
        this.taskStore.loadFromDisk();
        const freshTask = this.taskStore.getTask(thread.threadId);
        if (!freshTask) {
          throw new Error(`Task thread not found: ${thread.threadId}`);
        }
        requireV2ExecutionEligibility(freshTask, 'commit this approval');
        const currentRev = freshTask.execution.revision;
        try {
          return await this.taskStore.transitionExecution(thread.threadId, currentRev, (t) => {
            assertDedicatedTask(t);
            requireV2ExecutionEligibility(t, 'commit this approval');
            if (expectedOperationId && t.execution.operationId !== expectedOperationId) {
              throw new Error(
                `Cannot approve task: execution operation changed concurrently (expected ${expectedOperationId}, found ${t.execution.operationId})`,
              );
            }
            if (expectedGeneration !== undefined && t.execution.generation !== expectedGeneration) {
              throw new Error(
                `Cannot approve task: execution generation changed concurrently (expected ${expectedGeneration}, found ${t.execution.generation})`,
              );
            }
            if (expectedRunId && t.execution.run?.id !== expectedRunId) {
              throw new Error(
                `Cannot approve task: execution run changed concurrently (expected ${expectedRunId}, found ${t.execution.run?.id})`,
              );
            }
            if (t.execution.next) {
              throw new Error(`Cannot approve task: run ${t.execution.next.sourceRunId || 'unknown'} already has a durable handoff.`);
            }
            if (!expectedRunId && t.execution.run) {
              throw new Error(
                `Cannot approve task: a new execution run (${t.execution.run.id}) started concurrently`,
              );
            }

            if (
              t.status === 'completed' ||
              t.status === 'paused' ||
              (t.status === 'failed' && t.failure?.stage === 'merge') ||
              t.status === 'wait_decision'
            ) {
              throw new Error(`Cannot approve task while task is ${t.status}`);
            }
            if (t.currentRound !== originalReviewRound && (!approvalRound || t.currentRound !== approvalRound.round)) {
              throw new Error(
                `Cannot approve task: task round advanced concurrently (expected round ${originalReviewRound}, found ${t.currentRound})`,
              );
            }
            if (expectedReviewerSession && t.reviewerSessionId && t.reviewerSessionId !== expectedReviewerSession) {
              throw new Error(
                `Cannot approve task: reviewer session changed concurrently (expected ${expectedReviewerSession}, found ${t.reviewerSessionId})`,
              );
            }

            const currentHasActiveRun = Boolean(t.execution?.run);
            finalHasActiveRun = currentHasActiveRun;

            clearTaskFailure(t);
            t.worktree = thread.worktree;
            t.status = thread.status;
            t.queuePhase = thread.queuePhase;
            t.mergeControl = thread.mergeControl;
            t.mergeHistory = thread.mergeHistory;
            if (approvalRound) {
              const existingIdx = t.rounds.findIndex((r) => r.round === approvalRound!.round);
              if (existingIdx >= 0) {
                t.rounds[existingIdx] = approvalRound;
              } else {
                t.rounds.push(approvalRound);
              }
              t.currentRound = approvalRound.round;
              t.summary = {
                ...t.summary,
                lastPromptSnippet: (params.remarks || '').slice(0, 100),
                lastOutputSnippet: 'Approved',
              };
            }
            if (status === 'QUEUED') {
              t.execution.next = { phase: 'merge', round: t.currentRound || t.execution.round, sourceRunId: expectedRunId };
            } else if (!currentHasActiveRun) {
              t.execution.queue = undefined;
            }
          });
        } catch (err: any) {
          if (err?.message?.includes('revision mismatch') && attempt < 2) {
            continue;
          }
          throw err;
        }
      }
      throw new Error(`Failed to transition task ${thread.threadId} after retries`);
    };
    await doTransition();
    if (!finalHasActiveRun && status === 'QUEUED') {
      void ExecutionController.getInstance()?.startExecution(thread.threadId, 'merge');
    }
    if (status === 'COMPLETED') {
      try { archiveCompletedTaskLogs(thread, this.storageDir); } catch {}
    }
    return {
      threadId: thread.threadId,
      status,
      message: status === 'QUEUED'
        ? `Task ${thread.threadId} is approved at ${thread.worktree?.approvedCommit}; local integration and combined validation will run asynchronously.`
        : `Task ${thread.threadId} successfully approved and archived.`,
    };
  }

  public mergeApproved(threadId: string): Promise<MergeResult> {
    this.assertExecutionAdmission('merge this task');
    const thread = this.taskStore.getTask(threadId);
    const landedStashRecovery = thread?.status === 'completed' && thread.execution?.version === 2 &&
      Boolean(thread.worktree?.integration?.stashRecoveryPending && thread.worktree.integration.mergedCommit);
    requireV2ExecutionEligibility(landedStashRecovery ? { ...thread!, status: 'reviewing' } : thread, 'merge this task');
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; merge was not started.');
    return controller.startExecution(threadId, 'merge').then((result) => {
      if (result.started) {
        return { status: 'blocked', message: 'Merge was started under the V2 execution run; refresh the task for its result.' };
      }
      if (result.run) {
        return { status: 'blocked', message: `Merge is already owned by V2 run ${result.run.id} (${result.run.state}).` };
      }
      throw new Error(`V2 merge was not started: ${result.reason || 'unknown admission failure'}.`);
    });
  }

  public async requestMergeDecision(threadId: string, question: string, background: string, suggestion: string, conflictFiles: string[] = [], logPath?: string): Promise<{ threadId: string; status: 'wait_decision'; decisionId: string; nextAction: string; message: string }> {
    this.assertExecutionAdmission('request a merge decision');
    const current = this.taskStore.getTask(threadId);
    if (current) assertDedicatedTask(current);
    if (!current || current.status !== 'reviewing' || !question?.trim()) throw new Error('An active review and a non-empty question are required.');
    if (current.worktree?.integration?.stashRecoveryPending) throw new Error('Cannot request a merge decision while stash metadata recovery is pending.');
    requireV2ExecutionEligibility(current, 'request a merge decision for this task');
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; merge decision was not recorded.');
    const decision = await controller.requestTaskDecision(threadId, {
      question: question.trim(),
      background: (background || '').trim(),
      suggestion: (suggestion || '').trim(),
      resumePoint: isMergeReview(current) ? 'retry_merge' : 'continue_review',
      conflictFiles,
      logPath,
    });
    return {
      threadId,
      status: 'wait_decision',
      decisionId: decision.id,
      nextAction: 'END_TURN_AND_WAIT_FOR_USER',
      message: 'End this reviewer turn; the user decision will wake the same session.',
    };
  }

  public async submitMergeDecision(threadId: string, answer: string, by = 'user') {
    this.assertExecutionAdmission('submit a merge decision');
    const task = this.taskStore.getTask(threadId);
    if (task) assertDedicatedTask(task);
    requireV2ExecutionEligibility(task, 'submit a merge decision for this task');
    if (task.status !== 'wait_decision' || !task.decision?.id || !answer?.trim()) {
      throw new Error('A pending merge decision and a non-empty answer are required.');
    }
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; merge decision was not submitted.');
    const decisionId = task.decision.id;
    const updated = await controller.submitTaskDecision(threadId, decisionId, answer, by);
    return { threadId, status: updated.status, decisionId };
  }

  public async retryMerge(threadId: string) {
    this.assertExecutionAdmission('retry merge');
    const task = this.taskStore.getTask(threadId);
    if (task) assertDedicatedTask(task);
    if (!task || task.status !== 'failed' || task.failure?.stage !== 'merge') {
      throw new Error('Only a merge failure can restart its merge budget.');
    }
    requireV2ExecutionEligibility(task, 'retry merge for this task');
    const controller = ExecutionController.getInstance();
    if (!controller) throw new Error('V2 execution controller is unavailable; merge retry was not started.');
    const updated = await controller.retryOperation(threadId);
    return { threadId, status: 'queued' as const, retryGeneration: updated.execution!.generation };
  }

  public async submitMergeResolution(threadId: string): Promise<{ threadId: string; status: 'QUEUED'; message: string }> {
    this.assertExecutionAdmission('submit merge resolution');
    const thread = this.taskStore.getTask(threadId);
    if (thread) assertDedicatedTask(thread);
    requireV2ExecutionEligibility(thread, 'submit a merge resolution for this task');
    const worktree = thread?.worktree;
    if (!thread || !isMergeReview(thread) || !worktree?.approvedCommit || !worktree.mergeResolutionPath || !worktree.integration?.targetOldCommit) {
      throw new Error('No approved merge resolution workspace is awaiting submission.');
    }
    if (worktree.integration.stashRecoveryPending) throw new Error('Cannot submit a merge resolution while stash metadata recovery is pending.');
    if ((worktree.mergeResolutionSourceCommit || worktree.integration.sourceCommit) !== worktree.approvedCommit) {
      throw new Error('The resolution workspace belongs to a different approved candidate.');
    }
    const cwd = worktree.mergeResolutionPath;
    const allowedRoots = [path.resolve(this.storageDir, 'merge-worktrees')];
    if (thread.artifactDir) allowedRoots.push(path.resolve(thread.artifactDir, 'validation'));
    const resolved = path.resolve(cwd);
    const normalize = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
    if (!allowedRoots.includes(path.dirname(resolved)) || !path.basename(resolved).startsWith(`${threadId}-`) ||
        normalize(gitOutput(cwd, ['rev-parse', '--path-format=absolute', '--git-common-dir'])) !== normalize(worktree.commonDir || '')) {
      throw new Error('Invalid merge resolution workspace.');
    }
    if (currentHead(cwd) !== worktree.integration.targetOldCommit) throw new Error('Resolution workspace HEAD changed; restore the original merge state before submission.');
    if (gitOutput(cwd, ['diff', '--name-only', '--diff-filter=U'])) throw new Error('Resolve and stage all conflicts before submitting.');
    if (gitOutput(cwd, ['diff', '--name-only']) || gitOutput(cwd, ['ls-files', '--others', '--exclude-standard'])) {
      throw new Error('Stage all resolution changes before submitting.');
    }
    if (!ExecutionController.getInstance()) throw new Error('V2 execution controller is unavailable; merge resolution was not submitted.');
    const execution = thread.execution;
    await this.taskStore.transitionExecution(threadId, execution.revision, fresh => {
      assertDedicatedTask(fresh);
      const run = fresh.execution?.run;
      if (fresh.execution?.version !== 2 || fresh.execution.phase !== 'review' || fresh.execution.next ||
          !run || run.id !== execution.run?.id || run.role !== 'reviewer' ||
          !['starting', 'active'].includes(run.state) || run.unconfirmed ||
          fresh.cleanup || fresh.worktree?.integration?.stashRecoveryPending || !isMergeReview(fresh)) {
        throw new Error('The current reviewer run can no longer submit this merge resolution.');
      }
      fresh.worktree!.mergeResolutionSubmitted = true;
      fresh.status = 'queued';
      fresh.queuePhase = 'merge';
      const at = new Date().toISOString();
      fresh.worktree!.integration = { ...fresh.worktree!.integration, phase: 'waiting', updatedAt: at };
      fresh.updatedAt = at;
      fresh.execution.next = { phase: 'merge', round: fresh.execution.round, sourceRunId: run.id };
    });
    return { threadId, status: 'QUEUED', message: 'Resolution submitted; end the reviewer turn so the bridge can validate and integrate it.' };
  }

  public dispose(): void {
  }

}

/**
 * Bigram Jaccard similarity on normalized text, used to spot a rework that says
 * exactly what the previous round already said.
 *
 * Digits are folded (`第9轮` vs `第10轮` are the same instruction) and everything
 * but letters/digits is stripped, so formatting churn does not hide a repeat.
 */
export function promptSimilarity(a: string, b: string): number {
  const normalize = (value: string): string =>
    (value || '')
      .toLowerCase()
      .replace(/\d+/g, '#')
      .replace(/[^\p{L}\p{N}#]+/gu, '');
  const left = normalize(a);
  const right = normalize(b);
  if (!left || !right) return 0;
  if (left === right) return 1;

  const grams = (text: string): Set<string> => {
    const set = new Set<string>();
    for (let i = 0; i < text.length - 1; i++) set.add(text.slice(i, i + 2));
    return set;
  };
  const a2 = grams(left);
  const b2 = grams(right);
  if (a2.size === 0 || b2.size === 0) return 0;

  let shared = 0;
  for (const gram of a2) if (b2.has(gram)) shared++;
  return shared / (a2.size + b2.size - shared);
}

/** Similarity above which two rework prompts count as "the same instruction". */
export const NOOP_REWORK_SIMILARITY = 0.85;

/**
 * Refuse a rework that cannot change anything.
 *
 * A reviewer that is waiting on a sibling task ("BLOCKED, dependency not
 * delivered") used to answer with a rework round whose text repeated the last
 * one; the developer then returned "no changes" and the cycle repeated every
 * couple of minutes, each cycle burning an execution slot and a ledger round.
 * Such a wait must be expressed with `blockReview` (or `approve_task`) instead.
 */
export function assertNotNoopRework(thread: DelegationThread, feedback: string): void {
  const previous = (thread.rounds || [])[thread.rounds.length - 1];
  if (!previous || previous.type !== 'rework') return;
  // Legacy ledgers never recorded touchedFiles: absence is not evidence of a no-op.
  if (!Array.isArray(previous.touchedFiles) || previous.touchedFiles.length > 0) return;

  const similarity = promptSimilarity(feedback || '', previous.prompt || '');
  if (similarity < NOOP_REWORK_SIMILARITY) return;

  throw new Error(
    `REWORK_NOOP_LOOP: 上一轮（Round ${previous.round}）没有任何文件改动，本次返修指令与它 ` +
      `${(similarity * 100).toFixed(0)}% 相同，不可能产生新交付。请二选一：` +
      `本任务范围已通过就调用 approve_task；` +
      `被同批其他任务阻塞就调用 request_rework(threadId="${thread.threadId}", feedback="...", blocked=true, waitingForThreadId="<依赖的 threadId>") 挂起——` +
      `桥接会在依赖完成时自动唤醒，不占轮次、不烧 token。`,
  );
}
