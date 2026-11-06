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
  isUserInterruption,
  nextRoundNumber,
  shouldResumeNativeSession,
  ThreadStatus,
  isIsolatedTaskWorktree,
} from './types';
import * as fs from 'fs';
import * as path from 'path';
import { roundExecutionLogPath } from './round-log';
import { archiveCompletedTaskLogs } from './task-summary';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { TaskExecutionOwner, TaskExecutionRegistration, TaskExecutionRegistry } from './task-execution-registry';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { concurrencyPolicy } from './bridge-policy';
import { TaskRoleLock } from './task-role-lock';
import { readReviewerBootstrapToken } from './reviewer-bootstrap';
import { ModelApprovalHandler } from './model-approval';
import { TaskRetargetService, latestRetarget } from './task-retarget';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';
import { currentHead, changedFiles, commitTaskChanges } from './git-worktree';
import { MergeCoordinator, MergeResult } from './merge-coordinator';
import { assertCanExecute } from './agent-capabilities';

export class ReworkController {
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private queueDrainer: QueueDrainer;
  /** Moves a task to another agent/model when rework/retry/resume asks for it. */
  private readonly retargetService: TaskRetargetService;
  private readonly mergeCoordinator: MergeCoordinator;
  private readonly mergeWorkers = new Set<string>();
  private readonly blockedMergeRetries = new Map<string, string>();
  private mergeWatch?: NodeJS.Timeout;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    executionRegistry?: TaskExecutionRegistry,
    residentMergeOwner = false,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.storageDir = path.dirname(taskStore.getStoragePath());
    this.mergeCoordinator = new MergeCoordinator(taskStore, this.storageDir, undefined, (thread) => {
      const reviewRound = [...(thread.rounds || [])].reverse().find((round) => round.type !== 'approval');
      if (reviewRound) void this.callbackManager.notifyCaller(thread, reviewRound);
    });
    if (residentMergeOwner) {
      this.taskStore.on('changed', () => this.resumePendingMerges());
      this.mergeWatch = setInterval(() => this.resumePendingMerges(), 1_000);
      this.mergeWatch.unref();
    }
    this.executionRegistry = executionRegistry || new TaskExecutionRegistry(this.storageDir);
    this.retargetService = new TaskRetargetService(taskStore);
    this.queueDrainer = new QueueDrainer(
      this.taskStore,
      this.executionRegistry,
      () => TaskSupervisor.isSupervisorActive(),
      (target: QueuedExecution) => this.executeQueuedRound(target),
    );
    if (residentMergeOwner) {
      // Recover transactions already on disk once all controller services exist.
      this.resumePendingMerges();
    }
  }

  /** Answer model-switch confirmations raised by an executor switch. */
  public setModelApprovalHandler(handler: ModelApprovalHandler | undefined): void {
    this.retargetService.setModelApprovalHandler(handler);
  }

  public retargetTask(threadId: string, overrides: ExecutorOverride, reason: string, by = 'caller') {
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
      modelRef: overrides.modelRef,
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
    let thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
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
        modelRef: params.modelRef,
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
    const resumesNativeSession = shouldResumeNativeSession(thread, reworkRound);

    thread.currentRound = nextRound;
    thread.status = resumesNativeSession ? 'reworking' : 'running';
    thread.interruption = undefined;
    thread.updatedAt = nowIso;
    if (thread.reviewFlow === 'dedicated') {
      // Dedicated review always resumes the captured reviewer conversation.
      // MCP-supplied ids and detector guesses cannot rebind that identity.
      thread.callerSessionId = thread.reviewerSessionId;
    } else if (params.callerSessionId && isValidSessionId(params.callerSessionId)) {
      thread.callerSessionId = params.callerSessionId;
    } else if (
      (!thread.callerSessionId || !isValidSessionId(thread.callerSessionId)) &&
      this.detector
    ) {
      const detected = this.detector.detectActiveSession(
        thread.initiatorAgent,
        thread.worktree?.repositoryPath || thread.workspacePath,
        [thread.receiverSessionId],
      );
      if (detected) {
        thread.callerSessionId = detected;
        console.log(
          `[ReworkController] Auto-detected caller session for ${thread.initiatorAgent}: ${detected}`,
        );
      }
    }
    thread.rounds.push(reworkRound);
    thread.summary.lastPromptSnippet = (params.feedback || '').slice(0, 100);
    thread.summary.lastOutputSnippet = retarget && retarget.toAgent !== retarget.fromAgent
      ? `Reworking on ${thread.targetAgent} with a fresh session (Round ${nextRound})...`
      : `Reworking in same session (Round ${nextRound})...`;

    // Persist immediately
    this.taskStore.upsertTask(thread);
    // If TaskSupervisor is actively running (e.g. in VS Code extension host),
    // let it supervise execution persistently to prevent child processes from being killed when ephemeral MCP CLI exits.
    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[ReworkController] Active TaskSupervisor detected. Delegating Round ${nextRound} execution to supervisor.`,
      );
    } else if (this.isGloballyBusy(thread.threadId, thread.workspacePath)) {
      this.markQueued(thread, this.workspaceOwner(thread));
      this.queueDrainer.schedule(1_500);
    } else {
      this.executeReworkBackground(
        thread,
        nextRound,
        params.feedback,
        resumesNativeSession,
      ).catch((err) => {
        console.error(
          `[ReworkController] Error executing rework round ${nextRound} for ${thread.threadId}:`,
          err,
        );
      });
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

  /**
   * Retries the failed round in-place within the EXACT SAME native session,
   * preserving the original prompt and avoiding creating duplicate or generic rounds.
   *
   * `overrides` may move the retry to another agent/model first (fresh session).
   */
  public async retryFailedRound(
    threadId: string,
    overrides: ExecutorOverride = {},
  ): Promise<DelegationRound> {
    this.taskStore.loadFromDisk();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }
    assertCanExecute(overrides.targetAgent || thread.targetAgent, 'receive retried work');

    if (thread.archivedAt || thread.status === 'completed') {
      throw new Error(`Cannot retry task ${threadId}: completed or archived tasks are immutable.`);
    }
    const inFlightBeforeRetarget = findInFlightRound(thread);
    if (inFlightBeforeRetarget) {
      throw new Error(`Cannot retry round: ${describeRoundInFlight(thread, inFlightBeforeRetarget)}`);
    }
    this.assertDedicatedReviewerReady(thread, 'retry failed round');

    if (thread.status !== 'failed' && thread.status !== 'review_failed') {
      throw new Error(
        `Cannot retry task ${threadId} while it is ${thread.status}; retry is only for a failed task's current failed round.`,
      );
    }

    const candidateRound = latestWorkRound(thread);
    if (!candidateRound || candidateRound.status !== 'failed') {
      throw new Error(
        `No current failed execution round found for task ${threadId}. ` +
          (thread.status === 'review_failed'
            ? 'The delivered work is awaiting an explicit reviewer recovery; use wakeUpCaller to review it.'
            : 'Only the latest failed execution round can be retried.'),
      );
    }

    await this.applyExecutorOverride(threadId, overrides, 'retry_task: executor switch');
    this.taskStore.loadFromDisk();
    thread = this.taskStore.getTask(threadId);
    if (!thread) throw new Error(`Task thread not found after retarget: ${threadId}`);
    const inFlightAfterRetarget = findInFlightRound(thread);
    if (inFlightAfterRetarget) {
      throw new Error(`Cannot retry round: ${describeRoundInFlight(thread, inFlightAfterRetarget)}`);
    }
    if (thread.archivedAt || thread.status === 'completed') {
      throw new Error(`Cannot retry task ${threadId}: it became completed or archived during retarget.`);
    }
    this.assertDedicatedReviewerReady(thread, 'retry failed round');
    if (thread.status !== 'failed' && thread.status !== 'review_failed') {
      throw new Error(`Cannot retry task ${threadId}: it became ${thread.status} during retarget.`);
    }
    const refreshedRound = latestWorkRound(thread);
    if (
      !refreshedRound || refreshedRound.round !== candidateRound.round ||
      refreshedRound.status !== 'failed'
    ) {
      throw new Error(`Cannot retry task ${threadId}: latest failed round changed during retarget.`);
    }
    if (!thread.receiverSessionId) {
      throw new Error(`Cannot retry: thread ${threadId} is missing receiverSessionId.`);
    }
    const failedRound = refreshedRound;

    // Same invariant: retrying must not overlap another in-flight round.
    const isRework = shouldResumeNativeSession(thread, failedRound);
    const nowIso = new Date().toISOString();
    failedRound.status = 'running';
    failedRound.error = undefined;
    failedRound.output = undefined;
    failedRound.interruption = undefined;
    failedRound.timestamp = nowIso;

    // Never rewind the thread's notion of "latest round"; retrying an older
    // round must not make a newer one look current again.
    thread.currentRound = Math.max(thread.currentRound || 1, failedRound.round);
    thread.status = isRework ? 'reworking' : 'running';
    thread.interruption = undefined;
    thread.updatedAt = nowIso;
    thread.summary.lastPromptSnippet = (failedRound.prompt || '').slice(0, 100);
    thread.summary.lastOutputSnippet = `Retrying Round ${failedRound.round}...`;

    this.taskStore.upsertTask(thread);

    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[ReworkController] Active TaskSupervisor detected. Delegating Round ${failedRound.round} retry to supervisor.`,
      );
    } else if (this.isGloballyBusy(thread.threadId, thread.workspacePath)) {
      this.markQueued(thread, this.workspaceOwner(thread));
      this.queueDrainer.schedule(1_500);
    } else {
      this.executeReworkBackground(
        thread,
        failedRound.round,
        failedRound.prompt,
        isRework,
      ).catch((err) => {
        console.error(
          `[ReworkController] Error executing retry round ${failedRound.round} for ${thread.threadId}:`,
          err,
        );
      });
    }

    return failedRound;
  }

  /**
   * Resume a paused round in place. The original prompt and native session are
   * preserved; the interruption reason stays in history, and the current
   * interruption marker is cleared.
   *
   * `overrides` may resume the task on another agent/model (fresh session) — e.g.
   * pause a task whose model is rate-limited, switch, then resume.
   */
  public async resumePausedTask(
    threadId: string,
    overrides: ExecutorOverride = {},
  ): Promise<DelegationRound> {
    this.taskStore.loadFromDisk();
    let thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }
    assertCanExecute(overrides.targetAgent || thread.targetAgent, 'resume work');

    if (thread.archivedAt) {
      throw new Error(`Cannot resume archived task ${threadId}; its rounds and sessions were archived.`);
    }

    // A failed reviewer bootstrap has no safe resumable handoff: its token or
    // native session may have been lost, so resuming would leave a
    // task that can never run (and, before the pump guard, one that oscillated
    // `running` ↔ `queued` on every poll).
    if (thread.reviewBootstrap?.phase === 'failed') {
      throw new Error(
        `Cannot resume ${threadId}: its dedicated reviewer bootstrap failed. The original ` +
          `reviewer session/token cannot be safely inferred; restore that handoff through an ` +
          `explicit same-thread recovery before resuming. The task was left unchanged.`,
      );
    }

    const resumingBootstrap =
      thread.reviewFlow === 'dedicated' &&
      isReviewerBootstrapPending(thread) &&
      !(thread.reviewBootstrap?.phase === 'confirmed' && thread.reviewerSessionCaptured === true);
    if (!resumingBootstrap) this.assertDedicatedReviewerReady(thread, 'resume task');
    if (resumingBootstrap && this.retargetService.hasOverride(overrides)) {
      throw new Error(
        `Cannot retarget ${threadId} while its reviewer bootstrap is pending; resume the same reviewer session and token first.`,
      );
    }

    if (thread.status !== 'paused') {
      throw new Error(`Cannot resume ${threadId}: task is ${thread.status}, not paused.`);
    }
    if (thread.worktree?.approvedCommit && thread.worktree.integration?.phase === 'waiting') {
      if (this.retargetService.hasOverride(overrides)) {
        throw new Error(`Cannot change the executor while resuming approved integration for ${threadId}.`);
      }
      thread.status = 'waiting_merge';
      thread.updatedAt = new Date().toISOString();
      this.taskStore.upsertTask(thread);
      return [...thread.rounds].reverse().find((round) => round.type !== 'approval') || thread.rounds[thread.rounds.length - 1];
    }
    let pausedRound =
      [...thread.rounds].reverse().find((round) => round.status === 'paused') ||
      thread.rounds[thread.rounds.length - 1];
    if (!pausedRound || pausedRound.status !== 'paused') {
      throw new Error(`No paused round found for thread ${threadId}.`);
    }
    const resumeRoundNumber = pausedRound.round;
    const inFlightBeforeRetarget = findInFlightRound(thread);
    if (inFlightBeforeRetarget) {
      throw new Error(`Cannot resume paused task: ${describeRoundInFlight(thread, inFlightBeforeRetarget)}`);
    }

    await this.applyExecutorOverride(threadId, overrides, 'resume_task: executor switch');
    this.taskStore.loadFromDisk();
    const afterRetarget = this.taskStore.getTask(threadId);
    if (!afterRetarget) throw new Error(`Task thread not found after retarget: ${threadId}`);
    if (afterRetarget.archivedAt || afterRetarget.status !== 'paused') {
      throw new Error(`Cannot resume ${threadId}: task changed state during retarget.`);
    }
    if (afterRetarget.reviewBootstrap?.phase === 'failed') {
      throw new Error(`Cannot resume ${threadId}: its dedicated reviewer bootstrap failed during retarget.`);
    }
    if (afterRetarget.reviewFlow === 'dedicated' && !resumingBootstrap) {
      this.assertDedicatedReviewerReady(afterRetarget, 'resume task');
    }
    const afterRetargetRound =
      [...afterRetarget.rounds].reverse().find((round) => round.round === resumeRoundNumber);
    if (!afterRetargetRound || afterRetargetRound.status !== 'paused') {
      throw new Error(`Cannot resume ${threadId}: paused round changed during retarget.`);
    }
    thread = afterRetarget;
    pausedRound = afterRetargetRound;

    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot resume paused task: ${describeRoundInFlight(thread, inFlight)}`);
    }

    // A pause kills the child asynchronously. Resuming before the old round has
    // actually released its execution slot would clear its cancellation request
    // and let the old process keep running. Wait for the slot first.
    const owner = this.workspaceOwner(thread);
    if (owner && owner.threadId === threadId) {
      for (let i = 0; i < 50; i++) {
        const current = this.workspaceOwner(thread);
        if (!current || current.threadId !== threadId) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      const stillOwned = this.workspaceOwner(thread);
      if (stillOwned && stillOwned.threadId === threadId) {
        throw new Error(
          `Task ${threadId} is still stopping; wait for the pause to finish before resuming.`,
        );
      }
    }

    // The bootstrap runner may have completed while resume was waiting for its
    // execution record to clear. Always continue from its persisted session and
    // phase instead of the pre-wait task snapshot.
    this.taskStore.loadFromDisk();
    const latest = this.taskStore.getTask(threadId);
    if (!latest) throw new Error(`Task thread not found: ${threadId}`);
    if (latest.status !== 'paused') {
      throw new Error(`Cannot resume ${threadId}: task is now ${latest.status}, not paused.`);
    }
    thread = latest;
    pausedRound = thread.rounds.find((round) => round.round === resumeRoundNumber)!;
    if (!pausedRound || pausedRound.status !== 'paused') {
      throw new Error(`Cannot resume ${threadId}: paused round changed while waiting for shutdown.`);
    }

    // A stale pause/delete request must not immediately kill the resumed run.
    // The runner owns both channels — the cancellation file it polls and the
    // pause it remembers in memory — so clearing through it is the only way to
    // cover a pause that landed before any child existed and was therefore never
    // consumed by a settling round.
    this.runner.clearPendingCancellation(threadId);

    // A dedicated reviewer bootstrap is the first role in the task. Resume its
    // paused placeholder round and return it to the bootstrap scheduler; never
    // start the developer before a reviewer session has been confirmed/persisted.
    if (thread.reviewFlow === 'dedicated' && isReviewerBootstrapPending(thread)) {
      const nowIso = new Date().toISOString();
      pausedRound.status = 'running';
      pausedRound.error = undefined;
      pausedRound.interruption = undefined;
      pausedRound.timestamp = nowIso;
      thread.currentRound = Math.max(thread.currentRound || 1, pausedRound.round);
      thread.status = 'queued';
      thread.interruption = undefined;
      thread.updatedAt = nowIso;
      thread.summary.lastPromptSnippet = (pausedRound.prompt || '').slice(0, 100);
      thread.summary.lastOutputSnippet = 'Resuming the dedicated reviewer bootstrap...';
      const previousBootstrap = thread.reviewBootstrap || { phase: 'pending' as const, attempts: 0 };
      const existingToken = readReviewerBootstrapToken(threadId, this.storageDir);
      if (!existingToken) {
        const reason = previousBootstrap.phase === 'confirmed' && !thread.reviewerSessionCaptured
          ? 'The one-time reviewer token is unavailable and its native session was never captured; the original reviewer session cannot be resumed safely.'
          : 'The original reviewer bootstrap token is missing, so a replacement token or session cannot be created safely.';
        thread.status = 'failed';
        pausedRound.status = 'failed';
        pausedRound.error = reason;
        thread.reviewBootstrap = { ...previousBootstrap, phase: 'failed', lastError: reason };
        thread.updatedAt = nowIso;
        thread.summary.lastOutputSnippet = `[Reviewer bootstrap failed] ${reason}`;
        this.taskStore.upsertTask(thread);
        return pausedRound;
      }
      thread.reviewBootstrap = { ...previousBootstrap, phase: 'pending', lastError: undefined };
      this.taskStore.upsertTask(thread);
      if (thread.status === 'queued') this.queueDrainer.schedule(100);
      return pausedRound;
    }

    const isRework = shouldResumeNativeSession(thread, pausedRound);
    const nowIso = new Date().toISOString();
    pausedRound.status = 'running';
    pausedRound.error = undefined;
    pausedRound.timestamp = nowIso;
    thread.currentRound = Math.max(thread.currentRound || 1, pausedRound.round);
    thread.status = isRework ? 'reworking' : 'running';
    thread.interruption = undefined;
    thread.updatedAt = nowIso;
    thread.summary.lastPromptSnippet = (pausedRound.prompt || '').slice(0, 100);
    thread.summary.lastOutputSnippet = `Resuming paused Round ${pausedRound.round}...`;

    this.taskStore.upsertTask(thread);

    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[ReworkController] Active TaskSupervisor detected. Delegating resumed Round ${pausedRound.round} to supervisor.`,
      );
    } else if (this.isGloballyBusy(thread.threadId, thread.workspacePath)) {
      this.markQueued(thread, this.workspaceOwner(thread));
      this.queueDrainer.schedule(1_500);
    } else {
      this.executeReworkBackground(
        thread,
        pausedRound.round,
        pausedRound.prompt,
        isRework,
      ).catch((err) => {
        console.error(
          `[ReworkController] Error resuming paused round ${pausedRound.round} for ${threadId}:`,
          err,
        );
      });
    }

    return pausedRound;
  }

  private assertDedicatedReviewerReady(thread: DelegationThread, operation: string): void {
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
   * full execution slot, over and over. Blocking keeps the task `waiting_review`
   * (queue invariants unchanged), silences the stall watchdog and the wake path,
   * and wakes the reviewer as soon as the dependency reaches a terminal state.
   *
   * Unlike `pause_task` this is not a user interruption: it records no
   * `interruption`, keeps the review round intact, and needs no manual resume.
   */
  public blockReview(params: {
    threadId: string;
    reason?: string;
    waitingForThreadId?: string;
    by?: string;
  }): { threadId: string; status: ThreadStatus; waitingForThreadId?: string; message: string } {
    const thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
    if (thread.status !== 'waiting_review') {
      throw new Error(
        `Cannot block review: thread ${params.threadId} is "${thread.status}", not "waiting_review".`,
      );
    }
    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot block review: ${describeRoundInFlight(thread, inFlight)}`);
    }

    const nowIso = new Date().toISOString();
    thread.reviewBlocked = {
      waitingForThreadId: params.waitingForThreadId,
      reason: params.reason?.slice(0, 500),
      at: nowIso,
      by: params.by || 'caller',
    };
    // The review is not stalled — it is deliberately parked. Clearing the wake
    // plan keeps the watchdog and the retry timer out of the way.
    thread.reviewWake = undefined;
    thread.updatedAt = nowIso;
    thread.summary.lastOutputSnippet = params.waitingForThreadId
      ? `Blocked: waiting for ${params.waitingForThreadId}`
      : 'Blocked: waiting for an external dependency';
    this.taskStore.upsertTask(thread);

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
  public approveTask(params: ApproveTaskParams): ApproveResult {
    const thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }

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

    const nowIso = new Date().toISOString();
    let status: ApproveResult['status'] = 'COMPLETED';
    if (isIsolatedTaskWorktree(thread.worktree)) {
      const worktree = thread.worktree;
      if (!worktree.headCommit) throw new Error(`Task ${thread.threadId} has no recorded task commit; it cannot be approved.`);
      const head = currentHead(worktree.path);
      if (head !== worktree.headCommit || changedFiles(worktree.path).length) {
        throw new Error(`Task worktree changed after review began (approved candidate ${worktree.headCommit}, current HEAD ${head}); review the new state before approval.`);
      }
      if (head === worktree.baseCommit) {
        worktree.integration = { phase: 'no_changes', sourceCommit: head, updatedAt: nowIso };
        worktree.mergePending = false;
        thread.status = 'completed';
      } else {
        worktree.approvedCommit = head;
        worktree.integration = { phase: 'waiting', sourceCommit: head, updatedAt: nowIso };
        worktree.mergePending = true;
        thread.status = 'waiting_merge';
        status = 'WAITING_MERGE';
      }
    } else {
      // Legacy ledger entries retain their historical review lifecycle.
      thread.status = 'completed';
    }
    thread.updatedAt = nowIso;

    if (params.remarks) {
      const approvalRound: DelegationRound = {
        // Monotonic numbering keeps approval entries from colliding with a
        // round number that a retry already reused.
        round: nextRoundNumber(thread),
        type: 'approval',
        prompt: params.remarks,
        output: status === 'WAITING_MERGE' ? 'Review approved; waiting for verified local integration.' : 'Task successfully approved and verified.',
        durationMs: 0,
        status: 'completed',
        timestamp: nowIso,
      };
      thread.rounds.push(approvalRound);
      thread.currentRound = approvalRound.round;
      thread.summary.lastPromptSnippet = params.remarks.slice(0, 100);
      thread.summary.lastOutputSnippet = 'Approved';
    }

    this.taskStore.upsertTask(thread);
    if (status === 'COMPLETED') {
      try { archiveCompletedTaskLogs(thread, path.dirname(this.taskStore.getStoragePath())); } catch {}
    }

    return {
      threadId: thread.threadId,
      status,
      message: status === 'WAITING_MERGE'
        ? `Task ${thread.threadId} is approved at ${thread.worktree?.approvedCommit}; local integration and combined validation are running asynchronously.`
        : `Task ${thread.threadId} successfully approved and archived.`,
    };
  }

  public mergeApproved(threadId: string): Promise<MergeResult> {
    return this.mergeCoordinator.mergeApproved(threadId);
  }

  public resumePendingMerges(): void {
    this.taskStore.loadFromDisk();
    for (const thread of this.taskStore.getAllTasks()) {
      if (thread.status !== 'waiting_merge') continue;
      const blockedAt = this.blockedMergeRetries.get(thread.threadId);
      if (blockedAt && blockedAt === thread.updatedAt) continue;
      if (blockedAt) this.blockedMergeRetries.delete(thread.threadId);
      if (this.mergeWorkers.has(thread.threadId)) continue;
      this.mergeWorkers.add(thread.threadId);
      void this.mergeApproved(thread.threadId).catch((error: any) => {
        if (this.mergeCoordinator.hasLandedCandidate(thread.threadId)) {
          const latest = this.taskStore.getTask(thread.threadId);
          if (latest) this.blockedMergeRetries.set(thread.threadId, latest.updatedAt);
          console.error(`[ReworkController] Target ref is updated for ${thread.threadId}, but its durable ledger record still needs recovery: ${error?.message || error}`);
          return;
        }
        const latest = this.taskStore.getTask(thread.threadId);
        if (latest?.status !== 'waiting_merge' || !latest.worktree) return;
        latest.status = 'waiting_review';
        latest.worktree.approvedCommit = undefined;
        const failure = `Merge coordinator failed: ${error?.message || error}`;
        latest.worktree.integration = { ...latest.worktree.integration, phase: 'validation_failed', error: failure, updatedAt: new Date().toISOString() };
        latest.summary.lastOutputSnippet = failure;
        try { this.taskStore.upsertTask(latest); }
        catch (persistError: any) { console.error(`[ReworkController] Could not persist merge failure for ${thread.threadId}: ${persistError?.message || persistError}`); }
        const reviewRound = [...(latest.rounds || [])].reverse().find((round) => round.type !== 'approval');
        if (reviewRound) void this.callbackManager.notifyCaller(latest, reviewRound);
      }).finally(() => this.mergeWorkers.delete(thread.threadId));
    }
  }

  public dispose(): void {
    if (this.mergeWatch) clearInterval(this.mergeWatch);
    this.mergeWatch = undefined;
  }

  private async executeReworkBackground(
    thread: DelegationThread,
    roundNumber: number,
    feedback: string,
    isRework: boolean = true,
  ): Promise<void> {
    if (this.executionRegistry.getOwner(thread.threadId)) {
      this.queueDrainer.schedule(1_500);
      return;
    }
    this.taskStore.loadFromDisk();

    const roleLock = new TaskRoleLock(this.storageDir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) {
      this.queueDrainer.schedule(1_500);
      return;
    }

    let registration: TaskExecutionRegistration | undefined;
    this.taskStore.loadFromDisk();
    const latest = this.taskStore.getTask(thread.threadId);
    const latestRound = latest?.rounds.filter((entry) => entry.round === roundNumber).slice(-1)[0];
    if (
      !latest || latest.status === 'paused' || latest.status === 'waiting_review' ||
      latest.status === 'completed' || latest.status === 'failed' || latest.status === 'review_failed' ||
      latest.targetAgent !== thread.targetAgent || !latestRound || latestRound.status !== 'running' ||
      latestRound.prompt !== feedback || findInFlightRound(latest)?.round !== roundNumber ||
      isReviewerBootstrapPending(latest)
    ) {
      roleLock.release(roleLease);
      return;
    }
    thread = latest;


    const gatePolicy = concurrencyPolicy();
    if (gatePolicy.enabled) {
      const owners = this.executionRegistry.listOwners();
      const agentSlots = owners.filter((owner) => owner.agent === thread.targetAgent).length;
      if (owners.length >= gatePolicy.maxConcurrentTasks || agentSlots >= gatePolicy.maxConcurrentPerAgent) {
        roleLock.release(roleLease);
        this.markQueued(thread, undefined, this.describeConcurrencyBlocked(thread.targetAgent, gatePolicy));
        this.queueDrainer.schedule(1_500);
        return;
      }
    }
    try {
      registration = this.executionRegistry.begin(thread.threadId, roundNumber, thread.targetAgent, thread.workspacePath);
    } catch (error: any) {
      roleLock.release(roleLease);
      latest.status = 'failed';
      latestRound.status = 'failed';
      latestRound.error = `Could not register task execution: ${error?.message || error}`;
      latest.updatedAt = new Date().toISOString();
      this.taskStore.upsertTask(latest);
      return;
    }

    try {
      const current = thread;
      current.status = isRework ? 'reworking' : 'running';
      current.summary.lastOutputSnippet = 'Executing in background...';
      if (current.targetAgent === 'claude_code' && !isRework) {
        current.receiverSessionCaptured = true;
      }
      this.taskStore.upsertTask(current);
      thread = current;
      console.log(
        `[ReworkController] ${isRework ? 'Resuming' : 'Starting'} session ${thread.receiverSessionId} for ` +
          `${thread.targetAgent} (Round ${roundNumber})`,
      );

      let result;
      const workspaceBefore = captureWorkspaceSnapshot(thread.workspacePath);
      // Rework rounds used to run without a per-round stdio log, so the "raw
      // execution log" existed only for dispatch/supervisor rounds. Capture it
      // here too: `get_round_raw_log` must not silently return "not captured".
      const roundLogPath = roundExecutionLogPath(
        thread.threadId,
        roundNumber,
        path.dirname(this.taskStore.getStoragePath()),
        thread.targetAgent,
      );
      try {
        fs.mkdirSync(path.dirname(roundLogPath), { recursive: true });
        fs.writeFileSync(roundLogPath, '', 'utf8');
      } catch {}
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: isRework ? appendCompactionProtocol(feedback, roundNumber) : feedback,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
          model: thread.targetModel,
          modelRef: thread.targetModelRef,
          role: 'developer',
          previousTokenUsage: readStoredRoundUsage(this.taskStore, thread.threadId, roundNumber),
          onChildPid: (pid) => {
            if (registration) this.executionRegistry.attachChildPid(pid, registration);
            roleLock.attachChild(roleLease, pid);
          },
          onStdoutChunk: (chunk: string) => {
            try {
              fs.appendFileSync(roundLogPath, chunk, 'utf8');
            } catch {}
          },
          onStderrChunk: (chunk: string) => {
            try {
              fs.appendFileSync(roundLogPath, chunk, 'utf8');
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
          error: `Execution error: ${err?.message || err}`,
        };
      }

      const touchedFiles = diffWorkspaceSnapshots(
        workspaceBefore,
        captureWorkspaceSnapshot(thread.workspacePath),
      );

      this.taskStore.loadFromDisk();
      const latest = this.taskStore.getTask(thread.threadId);
      if (!latest) {
        console.warn(
          `[ReworkController] Task ${thread.threadId} was deleted while round ${roundNumber} was executing.`,
        );
        return;
      }

      // A pause may have landed after the child exited but before this handler
      // ran; the control plane's state wins over a "successful" result.
      const pausedByControl = latest.status === 'paused';
      const interruption = result.interruption || (pausedByControl ? latest.interruption : undefined);
      const isPaused = pausedByControl || interruption?.kind === 'paused';
      const responseOk = result.assessment
        ? result.assessment.success
        : result.exitCode === 0;
      let isSuccess = !interruption && !pausedByControl && responseOk;
      if (isSuccess && isIsolatedTaskWorktree(latest.worktree)) {
        try {
          latest.worktree.headCommit = commitTaskChanges(latest.worktree, `Agent work ${latest.threadId} round ${roundNumber}`);
          latest.worktree.integration = { phase: latest.worktree.headCommit === latest.worktree.baseCommit ? 'no_changes' : 'ready', sourceCommit: latest.worktree.headCommit, updatedAt: new Date().toISOString() };
        } catch (error: any) {
          result.error = `Could not create the task delivery commit: ${error?.message || error}`;
          result.exitCode = 1;
          isSuccess = false;
        }
      }
      if (!isSuccess && isIsolatedTaskWorktree(latest.worktree)) {
        try {
          const head = currentHead(latest.worktree.path);
          if (head !== latest.worktree.baseCommit || changedFiles(latest.worktree.path).length) {
            latest.worktree.headCommit = head;
            latest.worktree.integration = { phase: 'ready', sourceCommit: head, updatedAt: new Date().toISOString() };
          }
        } catch {}
      }
      const finalRoundStatus = isPaused ? 'paused' : isSuccess ? 'completed' : 'failed';
      const finalThreadStatus = isPaused ? 'paused' : isSuccess ? 'waiting_review' : 'failed';

      const nativeSessionId = result.actualSessionId || latest.receiverSessionId;
      if (result.actualSessionId && result.actualSessionId !== latest.receiverSessionId) {
        console.log(
          `[ReworkController] Syncing receiverSessionId from ${latest.receiverSessionId} to actual native ID: ${result.actualSessionId}`,
        );
        latest.receiverSessionId = result.actualSessionId;
      }
      if (result.actualSessionId) {
        latest.receiverSessionCaptured = true;
      }

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
          executorAgent: thread.targetAgent,
          executorModel: result.model || thread.targetModel,
          tokenUsage: result.tokenUsage,
        },
        finalThreadStatus,
        nativeSessionId,
        undefined,
        interruption,
      );

      if (updatedThread && this.taskStore.getTask(updatedThread.threadId)) {
        const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
        if (completedRound) {
          // Lagged compaction of the superseded round, before the handoff and
          // never allowed to block it.
          if (isSuccess) {
            compactRoundBestEffort(this.taskStore, {
              threadId: updatedThread.threadId,
              currentRoundNumber: roundNumber,
              currentOutput: result.extractedOutput || result.stdout,
            });
          }
          // Hand the task from developer to reviewer.
          roleLock.release(roleLease);
          if (!interruption && isSuccess) {
            await this.callbackManager.notifyCaller(updatedThread, completedRound);
          } else if (!isUserInterruption(interruption)) {
            // The receiver died; the caller must learn about it, otherwise the
            // task silently rots in `failed` until somebody polls the ledger.
            const notified = await this.callbackManager.notifyCallerOfFailure(
              updatedThread,
              roundNumber,
              result,
            );
            if (notified) this.taskStore.upsertTask(updatedThread);
          }
        }
      }
    } finally {
      if (registration) this.executionRegistry.end(registration);
      roleLock.release(roleLease);
      this.queueDrainer.schedule(50);
    }
  }

  private async executeQueuedRound(target: QueuedExecution): Promise<void> {
    await this.executeReworkBackground(
      target.thread,
      target.round.round,
      target.round.prompt,
      shouldResumeNativeSession(target.thread, target.round),
    );
  }

  /** A user-enabled count cap is advisory; disabled means unlimited execution. */
  private isGloballyBusy(excludeThreadId: string, _workspacePath?: string): boolean {
    const policy = concurrencyPolicy();
    if (!policy.enabled) return false;
    const owners = this.executionRegistry.listOwners().filter((owner) => owner.threadId !== excludeThreadId);
    if (owners.length >= policy.maxConcurrentTasks) return true;
    const thread = this.taskStore.getTask(excludeThreadId);
    return owners.filter((owner) => owner.agent === thread?.targetAgent).length >= policy.maxConcurrentPerAgent;
  }

  /** Current execution record for the given task, if any. */
  private workspaceOwner(thread: DelegationThread): TaskExecutionOwner | undefined {
    return this.executionRegistry.getOwner(thread.threadId);
  }

  private describeConcurrencyBlocked(targetAgent: string, policy: ReturnType<typeof concurrencyPolicy>): string {
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
    // Never rewrite another process's live execution or role state as queued.
    if (this.executionRegistry.getOwner(thread.threadId)) return;
    if (new TaskRoleLock(this.storageDir).getOwner(thread.threadId)) return;

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

    const message =
      detail ||
      (owner
        ? `Queued: waiting for task ${owner.threadId} round ${owner.round}.`
        : 'Queued: waiting for an execution slot.');
    if (fresh.status === 'queued' && fresh.summary.lastOutputSnippet === message) {
      return;
    }

    fresh.status = 'queued';
    fresh.summary.lastOutputSnippet = message;
    this.taskStore.upsertTask(fresh);
  }
}

function latestWorkRound(thread: DelegationThread): DelegationRound | undefined {
  return (thread.rounds || [])
    .filter((round) => round.type !== 'approval')
    .reduce<DelegationRound | undefined>(
      (latest, round) => !latest || round.round >= latest.round ? round : latest,
      undefined,
    );
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
