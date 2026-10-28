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
  isTaskUnfinished,
  isUserInterruption,
  nextRoundNumber,
  shouldResumeNativeSession,
  ThreadStatus,
} from './types';
import * as fs from 'fs';
import * as path from 'path';
import { roundExecutionLogPath } from './round-log';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import { findScopeConflicts, describeScopeConflict, ScopeConflict } from './file-scope';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { ExecutionGate, ExecutionOwner, normalizeWorkspaceKey } from './execution-gate';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { concurrencyPolicy } from './bridge-policy';
import { TaskRoleLock } from './task-role-lock';
import { ModelApprovalHandler } from './model-approval';
import { TaskRetargetService, latestRetarget } from './task-retarget';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';

export class ReworkController {
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private executionGate: ExecutionGate;
  private queueDrainer: QueueDrainer;
  /** Moves a task to another agent/model when rework/retry/resume asks for it. */
  private readonly retargetService: TaskRetargetService;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    executionGate?: ExecutionGate,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.executionGate = executionGate || new ExecutionGate();
    this.retargetService = new TaskRetargetService(taskStore);
    this.queueDrainer = new QueueDrainer(
      this.taskStore,
      this.executionGate,
      () => TaskSupervisor.isSupervisorActive(),
      (target: QueuedExecution) => this.executeQueuedRound(target),
    );
  }

  /** Answer model-switch confirmations raised by an executor switch. */
  public setModelApprovalHandler(handler: ModelApprovalHandler | undefined): void {
    this.retargetService.setModelApprovalHandler(handler);
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
    let thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }
    // `completed` is terminal: a stale reviewer must not resurrect an archived
    // task by submitting a late rework.
    if (thread.status === 'completed') {
      throw new Error(
        `Cannot request rework: thread ${params.threadId} is already approved and archived. ` +
          `Dispatch a new task if the work has to continue.`,
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
    if (params.callerSessionId && isValidSessionId(params.callerSessionId)) {
      thread.callerSessionId = params.callerSessionId;
    } else if (
      (!thread.callerSessionId || !isValidSessionId(thread.callerSessionId)) &&
      this.detector
    ) {
      const detected = this.detector.detectActiveSession(
        thread.initiatorAgent,
        thread.workspacePath,
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
    await this.applyExecutorOverride(threadId, overrides, 'retry_task: executor switch');
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (!thread.receiverSessionId) {
      throw new Error(`Cannot retry: thread ${threadId} is missing receiverSessionId.`);
    }

    const failedRound =
      [...thread.rounds].reverse().find((r) => r.status === 'failed') ||
      thread.rounds[thread.rounds.length - 1];

    if (!failedRound) {
      throw new Error(`No failed round found to retry for thread ${threadId}`);
    }

    // Same invariant: retrying must not overlap another in-flight round.
    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(`Cannot retry round: ${describeRoundInFlight(thread, inFlight)}`);
    }

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
    await this.applyExecutorOverride(threadId, overrides, 'resume_task: executor switch');
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    const pausedRound =
      [...thread.rounds].reverse().find((round) => round.status === 'paused') ||
      (thread.status === 'paused' ? thread.rounds[thread.rounds.length - 1] : undefined);
    if (!pausedRound) {
      throw new Error(`No paused round found for thread ${threadId}.`);
    }

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

    // A stale pause/delete request must not immediately kill the resumed run.
    // The runner owns both channels — the cancellation file it polls and the
    // pause it remembers in memory — so clearing through it is the only way to
    // cover a pause that landed before any child existed and was therefore never
    // consumed by a settling round.
    this.runner.clearPendingCancellation(threadId);

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
    thread.status = 'completed';
    thread.updatedAt = nowIso;

    if (params.remarks) {
      const approvalRound: DelegationRound = {
        // Monotonic numbering keeps approval entries from colliding with a
        // round number that a retry already reused.
        round: nextRoundNumber(thread),
        type: 'approval',
        prompt: params.remarks,
        output: 'Task successfully approved and verified.',
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

    return {
      threadId: thread.threadId,
      status: 'COMPLETED',
      message: `Task ${thread.threadId} successfully approved and archived.`,
    };
  }

  private async executeReworkBackground(
    thread: DelegationThread,
    roundNumber: number,
    feedback: string,
    isRework: boolean = true,
  ): Promise<void> {
    // Batch-level scope gate: a rework/retry round must not start on files that
    // another open task still owns, otherwise it would rewrite their work.
    this.taskStore.loadFromDisk();
    const scopeConflict = findScopeConflicts(thread, this.taskStore.getAllTasks());
    if (scopeConflict) {
      this.markQueued(thread, undefined, describeScopeConflict(scopeConflict), scopeConflict);
      this.queueDrainer.schedule(1_500);
      return;
    }

    // A reviewer decision creates the next developer round, but the developer
    // must not start until the reviewer role has handed off. This also blocks
    // a second reviewer/developer process for the same task.
    const roleLock = new TaskRoleLock();
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) {
      const owner = roleLock.getOwner(thread.threadId);
      this.markQueued(
        thread,
        undefined,
        owner?.role === 'reviewer'
          ? 'Queued: Reviewer is still active; developer starts after review handoff.'
          : `Queued: ${roleLock.describe(owner)} is active; a task may only have one role at a time.`,
      );
      this.queueDrainer.schedule(1_500);
      return;
    }

    // Cross-process execution slot: the workspace-scoped lease when concurrency
    // is enabled, the historical mesh-wide one otherwise.
    const gatePolicy = concurrencyPolicy();
    const workspaceKey = gatePolicy.enabled
      ? normalizeWorkspaceKey(thread.workspacePath)
      : undefined;
    const lease = this.executionGate.tryAcquire(
      thread.threadId,
      roundNumber,
      workspaceKey,
      thread.targetAgent,
    );
    if (!lease) {
      roleLock.release(roleLease);
      const owner = this.executionGate.getOwner(workspaceKey);
      if (owner?.threadId !== thread.threadId) {
        this.markQueued(thread, owner);
      }
      this.queueDrainer.schedule(1_500);
      return;
    }

    try {
      this.taskStore.loadFromDisk();
      const stored = this.taskStore.getTask(thread.threadId);
      if (!stored) {
        console.warn(
          `[ReworkController] Task ${thread.threadId} was deleted before round ${roundNumber} started.`,
        );
        return;
      }
      const current = stored;
      current.status = isRework ? 'reworking' : 'running';
      current.scopeBlockedBy = undefined;
      current.summary.lastOutputSnippet = 'Executing in background...';
      if (thread.targetAgent === 'claude_code' && !isRework) {
        // Claude's preassigned --session-id is the native session id.
        current.receiverSessionCaptured = true;
      }
      this.taskStore.upsertTask(current);
      thread = current;

      console.log(
        `[ReworkController] ${isRework ? 'Resuming' : 'Starting'} session ${thread.receiverSessionId} for ` +
          `${thread.targetAgent} (Round ${roundNumber})`,
      );

      let result;
      const scopeBefore = captureWorkspaceSnapshot(thread.workspacePath);
      // Rework rounds used to run without a per-round stdio log, so the "raw
      // execution log" existed only for dispatch/supervisor rounds. Capture it
      // here too: `get_round_raw_log` must not silently return "not captured".
      const roundLogPath = roundExecutionLogPath(
        thread.threadId,
        roundNumber,
        path.dirname(this.taskStore.getStoragePath()),
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
          previousTokenUsage: readStoredRoundUsage(this.taskStore, thread.threadId, roundNumber),
          // Record the real agent child so a crashed host cannot leave an orphan
          // editing the workspace while the next task starts.
          onChildPid: (pid) => this.executionGate.attachChildPid(pid, lease),
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
        scopeBefore,
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
      const isSuccess = !interruption && !pausedByControl && responseOk;
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
      this.executionGate.release(lease);
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

  /** True when another thread (or another process) is executing a round. */
  private isGloballyBusy(excludeThreadId: string, workspacePath?: string): boolean {
    const policy = concurrencyPolicy();
    const workspaceKey = policy.enabled ? normalizeWorkspaceKey(workspacePath) : undefined;
    const owner = this.executionGate.getOwner(workspaceKey);
    if (owner && owner.threadId !== excludeThreadId) return true;
    if (policy.enabled && this.executionGate.countActive() >= policy.maxConcurrentTasks) {
      return true;
    }

    return this.taskStore
      .getAllTasks()
      .some(
        (thread) =>
          thread.threadId !== excludeThreadId &&
          isTaskUnfinished(thread) &&
          // Concurrency is partitioned by workspace: a task in another workspace
          // holds a different slot and must not queue this rework round.
          (!policy.enabled || normalizeWorkspaceKey(thread.workspacePath) === workspaceKey),
      );
  }

  /** The live lease owner relevant to this task's slot (workspace-scoped when on). */
  private workspaceOwner(thread: DelegationThread): ExecutionOwner | undefined {
    const policy = concurrencyPolicy();
    return this.executionGate.getOwner(
      policy.enabled ? normalizeWorkspaceKey(thread.workspacePath) : undefined,
    );
  }

  private markQueued(
    thread: DelegationThread,
    owner?: ExecutionOwner,
    detail?: string,
    scopeConflict?: ScopeConflict,
  ): void {
    const message =
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
    if (thread.status === 'queued' && thread.summary.lastOutputSnippet === message && !blockChanged) {
      return;
    }

    thread.status = 'queued';
    thread.summary.lastOutputSnippet = message;
    this.taskStore.upsertTask(thread);
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
