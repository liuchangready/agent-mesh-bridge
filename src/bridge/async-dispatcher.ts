import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import {
  AgentId,
  DelegationThread,
  DelegationRound,
  DispatchTaskParams,
  DispatchResult,
  DispatchStatus,
  isTaskUnfinished,
  isValidSessionId,
  shouldResumeNativeSession,
} from './types';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import { findFileConflicts, validateExpectedFiles } from './file-scope';
import { ExecutionGate, ExecutionOwner } from './execution-gate';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { TaskRoleLock } from './task-role-lock';

export class AsyncDispatcher {
  private taskStore: TaskStore;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private streamSyncIntervalMs: number = 30000;
  private executionGate: ExecutionGate;
  private queueDrainer: QueueDrainer;

  constructor(
    taskStore: TaskStore,
    resolver: ActiveAgentResolver,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    executionGate?: ExecutionGate,
  ) {
    this.taskStore = taskStore;
    this.resolver = resolver;
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

    // Standalone MCP processes have no supervisor. React to ledger changes so a
    // task approval/rework immediately releases the task-level queue instead of
    // waiting for the next periodic poll.
    this.taskStore.on('changed', () => {
      this.queueDrainer.schedule(150);
    });
  }

  public setStreamSyncIntervalMs(ms: number): void {
    this.streamSyncIntervalMs = ms;
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

    const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
    const initiatorAgent: AgentId = params.initiatorAgent || callerFromEnv || 'antigravity';
    const now = new Date();
    const threadId = this.generateThreadId(now);
    const receiverSessionId = crypto.randomUUID();
    const nowIso = now.toISOString();
    const workspacePath = params.workspacePath || process.cwd();

    // expectedFiles is a lock scope, so it must be narrow: concrete code files
    // only. Folders/globs would block unrelated sibling work.
    const expectedFilesValidation = validateExpectedFiles(params.expectedFiles, workspacePath);
    if (!expectedFilesValidation.ok) {
      throw new Error(
        `Invalid expectedFiles for task "${params.title}": ${expectedFilesValidation.errors.join(' ')} ` +
          `Use concrete code file paths (e.g. ["src/service/foo.ts"]), never folders or glob patterns.`,
      );
    }

    let callerSessionId =
      params.callerSessionId && isValidSessionId(params.callerSessionId)
        ? params.callerSessionId
        : undefined;
    if (!callerSessionId && this.detector) {
      callerSessionId = this.detector.detectActiveSession(initiatorAgent, workspacePath, [
        receiverSessionId,
      ]);
      if (callerSessionId) {
        console.log(
          `[AsyncDispatcher] Auto-detected caller session for ${initiatorAgent}: ${callerSessionId}`,
        );
      }
    }

    const round1: DelegationRound = {
      round: 1,
      type: 'initial',
      prompt: params.prompt,
      status: 'running',
      durationMs: 0,
      timestamp: nowIso,
    };

    // Load active tasks to detect both file-scope conflicts and the global
    // single-flight rule. A thread with an in-flight round counts even if its
    // lifecycle status was not refreshed yet.
    this.taskStore.loadFromDisk();
    // Task-level queue: one unfinished task (including waiting_review/paused)
    // blocks every younger task, regardless of whether a round is in flight.
    const activeTasks = this.taskStore
      .getAllTasks()
      .filter((t) => t.threadId !== threadId && isTaskUnfinished(t))
      .sort(
        (a, b) =>
          new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
      );

    const tempCandidate: DelegationThread = {
      threadId,
      title: params.title || 'Untitled Delegation Task',
      initiatorAgent,
      targetAgent,
      status: 'running',
      currentRound: 1,
      createdAt: nowIso,
      updatedAt: nowIso,
      receiverSessionId,
      callerSessionId,
      workspacePath,
      expectedFiles:
        expectedFilesValidation.files.length > 0 ? expectedFilesValidation.files : undefined,
      summary: {
        lastPromptSnippet: (params.prompt || '').slice(0, 100),
        lastOutputSnippet: 'Executing in background...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    const conflict = findFileConflicts(tempCandidate, activeTasks);
    const gateOwner = this.executionGate.getOwner();
    const globalOwner = gateOwner && gateOwner.threadId !== threadId ? gateOwner : undefined;
    const blockingTask = activeTasks[0];
    const globallyBusy = !!globalOwner || !!blockingTask;
    const isQueued = conflict.hasConflict || globallyBusy;

    let queueDetail = '';
    if (conflict.hasConflict && conflict.conflictingThread) {
      queueDetail = `Queued: Waiting for task ${conflict.conflictingThread.threadId} to release the same file(s) [${(conflict.conflictingFiles || []).join(', ')}]`;
    } else if (globalOwner) {
      queueDetail = `Queued: Waiting for task ${globalOwner.threadId} (round ${globalOwner.round}) to finish; tasks execute strictly one at a time.`;
    } else if (blockingTask) {
      const phase =
        blockingTask.status === 'waiting_review'
          ? 'to complete review'
          : blockingTask.status === 'paused'
            ? 'to be resumed or deleted'
            : `to finish (${blockingTask.status})`;
      queueDetail =
        `Queued: Waiting for task ${blockingTask.threadId} ${phase} before another task runs; ` +
        `tasks execute one whole task at a time.`;
    }

    if (isQueued) {
      tempCandidate.status = 'queued';
      tempCandidate.summary.lastOutputSnippet = queueDetail;
    }

    const thread = tempCandidate;

    // Commit initial state to TaskStore
    this.taskStore.upsertTask(thread);

    // If TaskSupervisor is actively running (e.g. in VS Code extension host),
    // let it supervise execution persistently to prevent child processes from being killed when ephemeral MCP CLI exits.
    if (TaskSupervisor.isSupervisorActive()) {
      console.log(
        `[AsyncDispatcher] Active TaskSupervisor detected. Delegating Thread ${threadId} (status: ${thread.status}) execution to supervisor.`,
      );
    } else if (!isQueued) {
      this.executeBackgroundRound(thread, 1, params.prompt, false).catch((err) => {
        console.error(
          `[AsyncDispatcher] Unhandled error during round execution for ${threadId}:`,
          err,
        );
      });
    }

    if (isQueued) {
      // The current owner (or the persistent supervisor) will pump the queue;
      // this also re-checks in case no supervisor is alive.
      this.queueDrainer.schedule(1_500);
    }

    const dispatchStatus: DispatchStatus = isQueued ? 'QUEUED' : 'DISPATCHED';
    const message = isQueued
      ? `Task queued (Thread: ${threadId}). ${queueDetail} It will execute when the workspace is free.`
      : `Task successfully dispatched to ${targetAgent} (Thread: ${threadId}). Conclude your current turn and await callback.`;

    return {
      threadId,
      receiverSessionId,
      targetAgent,
      status: dispatchStatus,
      message,
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
    // Per-task role exclusivity: the developer may only run after the reviewer
    // has handed off (request_rework/approve_task), and never while another
    // reviewer/developer process owns the task.
    const roleLock = new TaskRoleLock();
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) {
      const owner = roleLock.getOwner(thread.threadId);
      this.markThreadQueued(
        thread,
        undefined,
        owner?.role === 'reviewer'
          ? 'Queued: Reviewer is still active; developer starts after review handoff.'
          : `Queued: ${roleLock.describe(owner)} is active; a task may only have one role at a time.`,
      );
      this.queueDrainer.schedule(1_500);
      return;
    }

    // The gate is the cross-process authority: a task may only start when no
    // other round (from any agent, any MCP server, any workspace) is in flight.
    const lease = this.executionGate.tryAcquire(thread.threadId, roundNumber);
    if (!lease) {
      roleLock.release(roleLease);
      const owner = this.executionGate.getOwner();
      if (owner?.threadId !== thread.threadId) {
        this.markThreadQueued(thread, owner);
      }
      this.queueDrainer.schedule(1_500);
      return;
    }

    try {
      // Re-read and refresh the thread: it may have been queued for a while and
      // another process may have updated its native session id.
      this.taskStore.loadFromDisk();
      const stored = this.taskStore.getTask(thread.threadId);
      if (!stored) {
        console.warn(
          `[AsyncDispatcher] Task ${thread.threadId} was deleted before round ${roundNumber} started.`,
        );
        return;
      }
      const current = stored;
      current.status = isRework ? 'reworking' : 'running';
      current.summary.lastOutputSnippet = 'Executing in background...';
      if (thread.targetAgent === 'claude_code' && !isRework) {
        current.receiverSessionCaptured = true;
      }
      this.taskStore.upsertTask(current);
      thread = current;

      console.log(
        `[AsyncDispatcher] Starting background execution: ${thread.threadId} (Round ${roundNumber}) on ${thread.targetAgent}`,
      );

      let streamedOutput = '';
      const startTime = Date.now();
      let streamTimer: NodeJS.Timeout | null = null;

      // Every 30 seconds, sync the live output stream and duration to taskStore
      streamTimer = setInterval(() => {
        if (!streamedOutput.trim()) return;
        const elapsedMs = Date.now() - startTime;
        const cleanSnippet = this.runner.extractCleanOutput(streamedOutput) || streamedOutput.trim();

        this.taskStore.updateRound(thread.threadId, roundNumber, {
          output: cleanSnippet,
          durationMs: elapsedMs,
          status: 'running',
        });
      }, this.streamSyncIntervalMs);

      const logDir = path.join(os.homedir(), '.agent-mesh-bridge', 'logs');
      if (!fs.existsSync(logDir)) {
        try {
          fs.mkdirSync(logDir, { recursive: true });
        } catch {}
      }
      const logFilePath = path.join(logDir, `${thread.threadId}_round_${roundNumber}.log`);
      try {
        fs.writeFileSync(logFilePath, '', 'utf8');
      } catch {}

      let result: RunResult;
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
          onStdoutChunk: (chunk: string) => {
            streamedOutput += chunk;
            try {
              fs.appendFileSync(logFilePath, chunk, 'utf8');
            } catch {}
          },
          onStderrChunk: (chunk: string) => {
            streamedOutput += chunk;
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
          error: `Execution error: ${err?.message || err}`,
        };
      } finally {
        if (streamTimer) {
          clearInterval(streamTimer);
          streamTimer = null;
        }
      }

      this.taskStore.loadFromDisk();
      const latest = this.taskStore.getTask(thread.threadId);
      if (!latest) {
        console.warn(
          `[AsyncDispatcher] Task ${thread.threadId} was deleted while round ${roundNumber} was executing.`,
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
          `[AsyncDispatcher] Syncing receiverSessionId from ${latest.receiverSessionId} to actual native ID: ${result.actualSessionId}`,
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
      // Strict turn-taking: only after this round has released the gate may the
      // next queued task start.
      this.queueDrainer.schedule(50);
    }
  }

  /** Pump one queued round; used when no persistent supervisor is alive. */
  private async executeQueuedRound(target: QueuedExecution): Promise<void> {
    await this.executeBackgroundRound(
      target.thread,
      target.round.round,
      target.round.prompt,
      shouldResumeNativeSession(target.thread, target.round),
    );
  }

  private markThreadQueued(
    thread: DelegationThread,
    owner?: ExecutionOwner,
    detail?: string,
  ): void {
    const fresh = this.taskStore.getTask(thread.threadId) || thread;
    const message =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
        : 'Queued: Waiting for the current task to finish; tasks execute strictly one at a time.');
    if (fresh.status === 'queued' && fresh.summary.lastOutputSnippet === message) return;

    fresh.status = 'queued';
    fresh.summary.lastOutputSnippet = message;
    this.taskStore.upsertTask(fresh);
  }

  private generateThreadId(d: Date): string {
    const pad = (n: number) => String(n).padStart(2, '0');
    const ymd = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
    const hms = `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
    const rand = crypto.randomBytes(2).toString('hex');
    return `thread_${ymd}_${hms}_${rand}`;
  }
}
