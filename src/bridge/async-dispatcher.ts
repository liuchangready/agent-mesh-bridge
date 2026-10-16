import * as crypto from 'crypto';
import {
  AgentId,
  DelegationThread,
  DelegationRound,
  DispatchTaskParams,
  DispatchResult,
} from './types';
import { TaskStore } from './task-store';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';

export class AsyncDispatcher {
  private taskStore: TaskStore;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;

  constructor(
    taskStore: TaskStore,
    resolver: ActiveAgentResolver,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
  ) {
    this.taskStore = taskStore;
    this.resolver = resolver;
    this.runner = runner;
    this.callbackManager = callbackManager;
  }

  /**
   * Dispatches a new task asynchronously and non-blockingly.
   */
  public async dispatchTask(params: DispatchTaskParams): Promise<DispatchResult> {
    let targetAgent: AgentId;

    if (!params.targetAgent || params.targetAgent === 'default') {
      const resolved = await this.resolver.resolveActiveAgent();
      targetAgent = resolved.agent;
    } else {
      targetAgent = params.targetAgent;
    }

    const initiatorAgent: AgentId = params.initiatorAgent || 'antigravity';
    const now = new Date();
    const threadId = this.generateThreadId(now);
    const receiverSessionId = crypto.randomUUID();
    const nowIso = now.toISOString();

    const round1: DelegationRound = {
      round: 1,
      type: 'initial',
      prompt: params.prompt,
      status: 'running',
      durationMs: 0,
      timestamp: nowIso,
    };

    const thread: DelegationThread = {
      threadId,
      title: params.title || 'Untitled Delegation Task',
      initiatorAgent,
      targetAgent,
      status: 'running',
      currentRound: 1,
      createdAt: nowIso,
      updatedAt: nowIso,
      receiverSessionId,
      callerSessionId: params.callerSessionId,
      workspacePath: params.workspacePath || process.cwd(),
      summary: {
        lastPromptSnippet: (params.prompt || '').slice(0, 100),
        lastOutputSnippet: 'Executing in background...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    // Commit initial state to TaskStore
    this.taskStore.upsertTask(thread);

    // Launch background execution without blocking current turn
    this.executeBackgroundRound(thread, 1, params.prompt, false).catch((err) => {
      console.error(
        `[AsyncDispatcher] Unhandled error during round execution for ${threadId}:`,
        err,
      );
    });

    return {
      threadId,
      receiverSessionId,
      targetAgent,
      status: 'DISPATCHED',
      message: `Task successfully dispatched to ${targetAgent} (Thread: ${threadId}). Conclude your current turn and await callback.`,
    };
  }

  /**
   * Internal background worker executing the round and updating state on completion.
   */
  private async executeBackgroundRound(
    thread: DelegationThread,
    roundNumber: number,
    prompt: string,
    isRework: boolean,
  ): Promise<void> {
    console.log(
      `[AsyncDispatcher] Starting background execution: ${thread.threadId} (Round ${roundNumber}) on ${thread.targetAgent}`,
    );

    const result = await this.runner.execute({
      agent: thread.targetAgent,
      prompt,
      isRework,
      receiverSessionId: thread.receiverSessionId,
      title: thread.title,
      cwd: thread.workspacePath,
    });

    const isSuccess = result.exitCode === 0;
    const finalRoundStatus = isSuccess ? 'completed' : 'failed';
    const finalThreadStatus = isSuccess ? 'waiting_review' : 'failed';

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
    );

    if (updatedThread) {
      const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
      if (completedRound) {
        await this.callbackManager.notifyCaller(updatedThread, completedRound);
      }
    }
  }

  private generateThreadId(d: Date): string {
    const pad = (n: number) => String(n).padStart(2, '0');
    const ymd = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
    const hms = `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
    const rand = crypto.randomBytes(2).toString('hex');
    return `thread_${ymd}_${hms}_${rand}`;
  }
}
