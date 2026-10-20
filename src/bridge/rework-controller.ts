import {
  DelegationThread,
  DelegationRound,
  RequestReworkParams,
  RequestReworkResult,
  ApproveTaskParams,
  ApproveResult,
  isValidSessionId,
  describeRoundInFlight,
  findInFlightRound,
  isTaskUnfinished,
  nextRoundNumber,
  shouldResumeNativeSession,
} from './types';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import { ExecutionGate, ExecutionOwner } from './execution-gate';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { CancellationRegistry } from './cancellation-registry';
import { TaskRoleLock } from './task-role-lock';

export class ReworkController {
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private executionGate: ExecutionGate;
  private queueDrainer: QueueDrainer;

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
    this.queueDrainer = new QueueDrainer(
      this.taskStore,
      this.executionGate,
      () => TaskSupervisor.isSupervisorActive(),
      (target: QueuedExecution) => this.executeQueuedRound(target),
    );
  }

  /**
   * Dispatches a rework round to the target agent within the EXACT SAME native session.
   * STRICT RULE: Never regenerate receiverSessionId!
   */
  public async requestRework(params: RequestReworkParams): Promise<RequestReworkResult> {
    const thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
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

    thread.currentRound = nextRound;
    thread.status = 'reworking';
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
    thread.summary.lastOutputSnippet = `Reworking in same session (Round ${nextRound})...`;

    // Persist immediately
    this.taskStore.upsertTask(thread);

    // If TaskSupervisor is actively running (e.g. in VS Code extension host),
    // let it supervise execution persistently to prevent child processes from being killed when ephemeral MCP CLI exits.
    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[ReworkController] Active TaskSupervisor detected. Delegating Round ${nextRound} execution to supervisor.`,
      );
    } else if (this.isGloballyBusy(thread.threadId)) {
      this.markQueued(thread, this.executionGate.getOwner());
      this.queueDrainer.schedule(1_500);
    } else {
      this.executeReworkBackground(thread, nextRound, params.feedback).catch((err) => {
        console.error(
          `[ReworkController] Error executing rework round ${nextRound} for ${thread.threadId}:`,
          err,
        );
      });
    }

    return {
      threadId: thread.threadId,
      round: nextRound,
      receiverSessionId: thread.receiverSessionId,
      targetAgent: thread.targetAgent,
      status: 'REWORK_DISPATCHED',
      message: `Rework Round ${nextRound} dispatched to ${thread.targetAgent} in original session (${thread.receiverSessionId}). Conclude your turn and await callback.`,
    };
  }

  /**
   * Retries the failed round in-place within the EXACT SAME native session,
   * preserving the original prompt and avoiding creating duplicate or generic rounds.
   */
  public async retryFailedRound(threadId: string): Promise<DelegationRound> {
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
    } else if (this.isGloballyBusy(thread.threadId)) {
      this.markQueued(thread, this.executionGate.getOwner());
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
   */
  public async resumePausedTask(threadId: string): Promise<DelegationRound> {
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
    // actually released the global execution gate would clear its cancellation
    // request and let the old process keep running. Wait for the gate first.
    const owner = this.executionGate.getOwner();
    if (owner && owner.threadId === threadId) {
      for (let i = 0; i < 50; i++) {
        const current = this.executionGate.getOwner();
        if (!current || current.threadId !== threadId) break;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      const stillOwned = this.executionGate.getOwner();
      if (stillOwned && stillOwned.threadId === threadId) {
        throw new Error(
          `Task ${threadId} is still stopping; wait for the pause to finish before resuming.`,
        );
      }
    }

    // A stale pause/delete request must not immediately kill the resumed run.
    new CancellationRegistry().clear(threadId);

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
    } else if (this.isGloballyBusy(thread.threadId)) {
      this.markQueued(thread, this.executionGate.getOwner());
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

    const lease = this.executionGate.tryAcquire(thread.threadId, roundNumber);
    if (!lease) {
      roleLock.release(roleLease);
      const owner = this.executionGate.getOwner();
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
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: feedback,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
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
        },
        finalThreadStatus,
        nativeSessionId,
        undefined,
        interruption,
      );

      if (updatedThread && !interruption && this.taskStore.getTask(updatedThread.threadId)) {
        const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
        if (completedRound) {
          // Hand the task from developer to reviewer.
          roleLock.release(roleLease);
          await this.callbackManager.notifyCaller(updatedThread, completedRound);
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
  private isGloballyBusy(excludeThreadId: string): boolean {
    const owner = this.executionGate.getOwner();
    if (owner && owner.threadId !== excludeThreadId) return true;

    return this.taskStore
      .getAllTasks()
      .some((thread) => thread.threadId !== excludeThreadId && isTaskUnfinished(thread));
  }

  private markQueued(
    thread: DelegationThread,
    owner?: ExecutionOwner,
    detail?: string,
  ): void {
    const message =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
        : 'Queued: Waiting for the current task to finish; tasks execute strictly one at a time.');
    if (thread.status === 'queued' && thread.summary.lastOutputSnippet === message) return;

    thread.status = 'queued';
    thread.summary.lastOutputSnippet = message;
    this.taskStore.upsertTask(thread);
  }
}
