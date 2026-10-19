import {
  DelegationThread,
  DelegationRound,
  RequestReworkParams,
  RequestReworkResult,
  ApproveTaskParams,
  ApproveResult,
  isValidSessionId,
} from './types';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';

export class ReworkController {
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
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

    const nextRound = (thread.currentRound || 1) + 1;
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
    thread.updatedAt = nowIso;
    if (params.callerSessionId && isValidSessionId(params.callerSessionId)) {
      thread.callerSessionId = params.callerSessionId;
    } else if ((!thread.callerSessionId || !isValidSessionId(thread.callerSessionId)) && this.detector) {
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

    const nowIso = new Date().toISOString();
    failedRound.status = 'running';
    failedRound.error = undefined;
    failedRound.output = undefined;
    failedRound.timestamp = nowIso;

    thread.currentRound = failedRound.round;
    thread.status = 'reworking';
    thread.updatedAt = nowIso;
    thread.summary.lastPromptSnippet = (failedRound.prompt || '').slice(0, 100);
    thread.summary.lastOutputSnippet = `Retrying Round ${failedRound.round} in same session...`;

    this.taskStore.upsertTask(thread);

    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[ReworkController] Active TaskSupervisor detected. Delegating Round ${failedRound.round} retry to supervisor.`,
      );
    } else {
      this.executeReworkBackground(thread, failedRound.round, failedRound.prompt).catch((err) => {
        console.error(
          `[ReworkController] Error executing retry round ${failedRound.round} for ${thread.threadId}:`,
          err,
        );
      });
    }

    return failedRound;
  }

  /**
   * Approves and archives the task.
   */
  public approveTask(params: ApproveTaskParams): ApproveResult {
    const thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }

    const nowIso = new Date().toISOString();
    thread.status = 'completed';
    thread.updatedAt = nowIso;

    if (params.remarks) {
      const approvalRound: DelegationRound = {
        round: (thread.currentRound || 1) + 1,
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
  ): Promise<void> {
    console.log(
      `[ReworkController] Resuming session ${thread.receiverSessionId} for ${thread.targetAgent} (Round ${roundNumber})`,
    );

    const result = await this.runner.execute({
      agent: thread.targetAgent,
      prompt: feedback,
      isRework: true,
      receiverSessionId: thread.receiverSessionId,
      title: thread.title,
      cwd: thread.workspacePath,
    });

    const isSuccess = result.exitCode === 0;
    const finalRoundStatus = isSuccess ? 'completed' : 'failed';
    const finalThreadStatus = isSuccess ? 'waiting_review' : 'failed';

    const nativeSessionId = result.actualSessionId || thread.receiverSessionId;
    if (result.actualSessionId && result.actualSessionId !== thread.receiverSessionId) {
      console.log(
        `[ReworkController] Syncing receiverSessionId from ${thread.receiverSessionId} to actual native ID: ${result.actualSessionId}`,
      );
      thread.receiverSessionId = result.actualSessionId;
    }

    const updatedThread = this.taskStore.updateRound(
      thread.threadId,
      roundNumber,
      {
        output: result.extractedOutput || result.stdout,
        error: result.error,
        durationMs: result.durationMs,
        status: finalRoundStatus,
      },
      finalThreadStatus,
      nativeSessionId,
    );

    if (updatedThread) {
      const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
      if (completedRound) {
        await this.callbackManager.notifyCaller(updatedThread, completedRound);
      }
    }
  }
}
