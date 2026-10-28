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
  DispatchWorktreeSpec,
  WorktreeInfo,
  getModelRouting,
  isTaskUnfinished,
  isValidSessionId,
  isUserInterruption,
  shouldResumeNativeSession,
} from './types';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import {
  findFileConflicts,
  validateExpectedFiles,
  findScopeConflicts,
  describeScopeConflict,
  ScopeConflict,
} from './file-scope';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { ExecutionGate, ExecutionOwner, normalizeWorkspaceKey } from './execution-gate';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { TaskRoleLock } from './task-role-lock';
import { resolveInitiatorModel } from './model-catalog';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';
import { concurrencyPolicy } from './bridge-policy';
import {
  ModelApprovalHandler,
  resolveDispatchModel,
} from './model-approval';

export class AsyncDispatcher {
  private taskStore: TaskStore;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private streamSyncIntervalMs: number = 30000;
  private executionGate: ExecutionGate;
  private queueDrainer: QueueDrainer;
  /** Answers model-switch confirmations; the file channel unless overridden. */
  private modelApprovalHandler?: ModelApprovalHandler;

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
   * Replace how a non-default receiver model is confirmed. Production uses the
   * cross-process file channel (the VS Code extension host answers it); tests
   * inject a decision directly.
   */
  public setModelApprovalHandler(handler: ModelApprovalHandler | undefined): void {
    this.modelApprovalHandler = handler;
  }

  /**
   * Dispatches a new task asynchronously and non-blockingly.
   */
  /**
   * Validate the caller's "this workspace is a temporary worktree" declaration.
   *
   * Without `mainPath` the bridge cannot tell a throwaway checkout from a real
   * workspace, and the merge gate would silently never fire — the exact failure
   * mode this parameter exists to prevent. `path` is the task's workspacePath, so
   * passing the same directory twice is a caller mistake, not a valid worktree.
   */
  private resolveWorktreeSpec(
    spec: DispatchWorktreeSpec | undefined,
    workspacePath: string,
  ): WorktreeInfo | undefined {
    if (!spec) return undefined;
    if (!spec.mainPath || !String(spec.mainPath).trim()) {
      throw new Error(
        'worktree.mainPath is required when worktree is set: name the main checkout ' +
          'this temporary worktree has to merge back into.',
      );
    }
    const worktreePath = path.resolve(workspacePath);
    const mainPath = path.resolve(spec.mainPath);
    if (worktreePath.toLowerCase() === mainPath.toLowerCase()) {
      throw new Error(
        `worktree.mainPath must differ from workspacePath (both resolve to ${worktreePath}). ` +
          'Pass the temporary worktree as workspacePath and the main checkout as worktree.mainPath.',
      );
    }
    return {
      path: worktreePath,
      mainPath,
      ...(spec.branch ? { branch: spec.branch } : {}),
      ...(spec.baseCommit ? { baseCommit: spec.baseCommit } : {}),
      mergePending: false,
    };
  }

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
    // Throws before anything is persisted when the declaration is unusable, so a
    // bad worktree spec can never create a task whose output cannot be merged.
    const worktree = this.resolveWorktreeSpec(params.worktree, workspacePath);

    // expectedFiles is a lock scope, so it must be narrow: concrete code files
    // only. Folders/globs would block unrelated sibling work.
    const expectedFilesValidation = validateExpectedFiles(params.expectedFiles, workspacePath);
    if (!expectedFilesValidation.ok) {
      throw new Error(
        `Invalid expectedFiles for task "${params.title}": ${expectedFilesValidation.errors.join(' ')} ` +
          `Use concrete code file paths (e.g. ["src/service/foo.ts"]), never folders or glob patterns.`,
      );
    }
    // A declared scope is what lets the bridge refuse to start this task on a
    // file another open task already changed. Callers that genuinely touch no
    // workspace file must say so explicitly instead of silently omitting it.
    if (
      expectedFilesValidation.files.length === 0 &&
      params.requireScope === true &&
      !params.allowNoScope
    ) {
      throw new Error(
        `dispatch_task requires expectedFiles for task "${params.title}": list the concrete code files this ` +
          `task will modify (e.g. ["engine/src/main/kotlin/Foo.kt"]) so two tasks cannot edit the same file. ` +
          `If the task touches no workspace file, pass allowNoScope: true.`,
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

    // Load active tasks to detect both file-scope conflicts and the execution
    // slot rule. A thread with an in-flight round counts even if its lifecycle
    // status was not refreshed yet.
    //
    // Concurrency is opt-in and, when enabled, partitioned by workspace: only an
    // unfinished task in the SAME workspace competes for this task's slot. With
    // concurrency off (the default) every unfinished task blocks, exactly as the
    // historical global single-flight rule requires.
    const policy = concurrencyPolicy();
    const workspaceKey = policy.enabled ? normalizeWorkspaceKey(workspacePath) : undefined;
    this.taskStore.loadFromDisk();
    // Task-level queue: one unfinished task (including waiting_review/paused)
    // blocks every younger task in its workspace, regardless of whether a round
    // is in flight.
    const allKnownTasks = this.taskStore.getAllTasks();
    const activeTasks = allKnownTasks
      .filter((t) => t.threadId !== threadId && isTaskUnfinished(t))
      .filter((t) => !policy.enabled || normalizeWorkspaceKey(t.workspacePath) === workspaceKey)
      .sort(
        (a, b) =>
          new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
      );
    // The scope gate also looks at accepted-but-unmerged tasks: their edits are
    // still in the working tree even though they no longer hold the queue.
    const scopePeers = allKnownTasks.filter((t) => t.threadId !== threadId);

    // Record both ends of the model routing: the model the sender is on (best
    // effort) and the model the receiver will execute with. Pinning the receiver
    // model here keeps rework/retry rounds on one model and makes the task list
    // show exactly what will run.
    //
    // The configured default wins unless the caller declares an explicit user
    // request AND the user confirms the switch in VS Code (see model-approval).
    const initiatorModel = resolveInitiatorModel(initiatorAgent, params.initiatorModel);
    const modelResolution = await resolveDispatchModel({
      targetAgent,
      requestedModel: params.model,
      requestedByUser: params.modelRequestedByUser === true,
      initiatorAgent,
      title: params.title,
      handler: this.modelApprovalHandler,
    });
    const targetModel = modelResolution.model;
    const modelNotice = modelResolution.notice;

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
      ...(worktree ? { worktree } : {}),
      initiatorModel: initiatorModel.model,
      initiatorModelSource: initiatorModel.source,
      targetModel,
      modelNotice,
      modelApproval: modelResolution.approval,
      expectedFiles:
        expectedFilesValidation.files.length > 0 ? expectedFilesValidation.files : undefined,
      scopeOverride: params.allowScopeOverlap ? true : undefined,
      summary: {
        lastPromptSnippet: (params.prompt || '').slice(0, 100),
        lastOutputSnippet: 'Executing in background...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    const conflict = findFileConflicts(tempCandidate, activeTasks);
    // Batch-level scope gate: refuse to schedule a task whose declared files are
    // still owned by another open task (declared or actually touched).
    const scopeConflict = findScopeConflicts(tempCandidate, scopePeers);
    // With concurrency on, an owner in ANOTHER workspace is a different slot and
    // must not queue this task; with it off, any live owner does (legacy).
    const gateOwner = this.executionGate.getOwner(workspaceKey);
    const globalOwner = gateOwner && gateOwner.threadId !== threadId ? gateOwner : undefined;
    const blockingTask = activeTasks[0];
    const activeSlots = policy.enabled ? this.executionGate.countActive() : 0;
    const globalSlotFull = policy.enabled && activeSlots >= policy.maxConcurrentTasks;
    const agentSlots = policy.enabled
      ? this.executionGate.listOwners().filter((owner) => owner.agent === targetAgent).length
      : 0;
    const agentSlotFull = policy.enabled && agentSlots >= policy.maxConcurrentPerAgent;
    const globallyBusy = !!globalOwner || !!blockingTask || globalSlotFull || agentSlotFull;
    const isQueued = conflict.hasConflict || !!scopeConflict || globallyBusy;

    let queueDetail = '';
    if (scopeConflict) {
      tempCandidate.scopeBlockedBy = {
        threadId: scopeConflict.threadId,
        title: scopeConflict.title,
        files: scopeConflict.files,
      };
      queueDetail = describeScopeConflict(scopeConflict);
    } else if (conflict.hasConflict && conflict.conflictingThread) {
      queueDetail = `Queued: Waiting for task ${conflict.conflictingThread.threadId} to release the same file(s) [${(conflict.conflictingFiles || []).join(', ')}]`;
    } else if (globalOwner) {
      queueDetail = policy.enabled
        ? `Queued: workspace ${workspaceKey} already has task ${globalOwner.threadId} (round ${globalOwner.round}) running; ` +
          `only one task per workspace may run at a time.`
        : `Queued: Waiting for task ${globalOwner.threadId} (round ${globalOwner.round}) to finish; tasks execute strictly one at a time.`;
    } else if (blockingTask) {
      const phase =
        blockingTask.status === 'waiting_review'
          ? 'to complete review'
          : blockingTask.status === 'paused'
            ? 'to be resumed or deleted'
            : `to finish (${blockingTask.status})`;
      queueDetail = policy.enabled
        ? `Queued: Waiting for task ${blockingTask.threadId} ${phase} in workspace ${workspaceKey} ` +
          `before another task runs there; only one task per workspace may run at a time.`
        : `Queued: Waiting for task ${blockingTask.threadId} ${phase} before another task runs; ` +
          `tasks execute one whole task at a time.`;
    } else if (agentSlotFull) {
      queueDetail =
        `Queued: agent ${targetAgent} already occupies ${agentSlots}/${policy.maxConcurrentPerAgent} ` +
        `of its concurrent slots; waiting for one of its running tasks to finish.`;
    } else if (globalSlotFull) {
      queueDetail =
        `Queued: global concurrency limit reached (${activeSlots}/${policy.maxConcurrentTasks}); ` +
        `waiting for a free execution slot.`;
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
    const routing = getModelRouting(thread);
    const noticeLine = modelNotice ? ` ${modelNotice}` : '';
    const modelLine = `Model routing: ${routing.text}.${noticeLine}`;
    // The caller's turn must end here. Agents that keep the turn open spend the
    // user's tokens polling for information the callback will deliver anyway, so
    // the directive is machine-readable AND leads the prose.
    const stopInstruction =
      'STOP NOW — the dispatch is asynchronous. End your turn immediately and do not call ' +
      'get_thread_details / get_recent_logs / list_recent_tasks to watch this task; the bridge ' +
      'wakes your native session when the round finishes (success and failure both wake you). ' +
      (isQueued ? 'A QUEUED result is normal: it starts on its own.' : '');
    const stopLine = `nextAction=END_TURN_AND_WAIT_FOR_CALLBACK. ${stopInstruction}`;
    const message = isQueued
      ? `${stopLine} Task queued (Thread: ${threadId}). ${modelLine} ${queueDetail} It will execute when the workspace is free.`
      : `${stopLine} Task successfully dispatched to ${targetAgent} (Thread: ${threadId}). ${modelLine} Conclude your current turn and await callback.`;

    return {
      threadId,
      receiverSessionId,
      targetAgent,
      status: dispatchStatus,
      message,
      modelRouting: routing,
      modelNotice,
      modelApproval: modelResolution.approval,
      nextAction: 'END_TURN_AND_WAIT_FOR_CALLBACK',
      stopInstruction,
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
    // Batch-level scope gate: never start a round on files another open task is
    // still holding (declared or measured). Checked before any lease is taken so
    // a blocked task cannot sit on the global slot.
    this.taskStore.loadFromDisk();
    const scopeConflict = findScopeConflicts(thread, this.taskStore.getAllTasks());
    if (scopeConflict) {
      this.markThreadQueued(
        thread,
        undefined,
        describeScopeConflict(scopeConflict),
        scopeConflict,
      );
      this.queueDrainer.schedule(1_500);
      return;
    }

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

    // The gate is the cross-process authority. With concurrency off it is the one
    // mesh-wide slot; with it on, the slot belongs to this task's workspace (and
    // the global/per-agent caps are enforced by the callers below).
    const policy = concurrencyPolicy();
    const workspaceKey = policy.enabled ? normalizeWorkspaceKey(thread.workspacePath) : undefined;
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
        this.markThreadQueued(
          thread,
          owner,
          owner
            ? policy.enabled
              ? `Queued: workspace ${workspaceKey} already has task ${owner.threadId} (round ${owner.round}) running; ` +
                `only one task per workspace may run at a time.`
              : `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
            : policy.enabled
              ? this.describeConcurrencyBlocked(thread.targetAgent)
              : undefined,
        );
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
      current.scopeBlockedBy = undefined;
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
      // Baseline right before the agent starts: the diff afterwards tells us
      // exactly which files this round changed, even if the caller declared
      // nothing or the agent edited outside its declared scope.
      const scopeBefore = captureWorkspaceSnapshot(thread.workspacePath);
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: isRework ? appendCompactionProtocol(prompt, roundNumber) : prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
          taskThreadId: thread.threadId,
          taskRound: roundNumber,
          model: thread.targetModel,
          previousTokenUsage: readStoredRoundUsage(this.taskStore, thread.threadId, roundNumber),
          onChildPid: (pid) => this.executionGate.attachChildPid(pid, lease),
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

      // Attribute the files this round changed while the tree state is still
      // fresh (a pause between here and the finalize below must not lose it).
      const touchedFiles = diffWorkspaceSnapshots(
        scopeBefore,
        captureWorkspaceSnapshot(thread.workspacePath),
      );

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
          // Lagged compaction: only after this round actually succeeded, and
          // before the role handoff. Best-effort — the compactor never throws,
          // because a failure here must not strand the reviewer wake-up.
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

  /**
   * Why no slot could be taken when no same-workspace owner explains it: either
   * the mesh-wide cap or this agent's own cap is saturated.
   */
  private describeConcurrencyBlocked(targetAgent: string): string {
    const policy = concurrencyPolicy();
    const owners = this.executionGate.listOwners();
    if (owners.length >= policy.maxConcurrentTasks) {
      return (
        `Queued: global concurrency limit reached (${owners.length}/${policy.maxConcurrentTasks}); ` +
        `waiting for a free execution slot.`
      );
    }
    const sameAgent = owners.filter((owner) => owner.agent === targetAgent).length;
    if (sameAgent >= policy.maxConcurrentPerAgent) {
      return (
        `Queued: agent ${targetAgent} already occupies ${sameAgent}/${policy.maxConcurrentPerAgent} ` +
        `of its concurrent slots; waiting for one of its running tasks to finish.`
      );
    }
    return 'Queued: waiting for a free execution slot.';
  }

  private markThreadQueued(
    thread: DelegationThread,
    owner?: ExecutionOwner,
    detail?: string,
    scopeConflict?: ScopeConflict,
  ): void {
    const fresh = this.taskStore.getTask(thread.threadId) || thread;
    const message =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish; tasks execute strictly one at a time.`
        : 'Queued: Waiting for the current task to finish; tasks execute strictly one at a time.');
    // Persist on the object that is actually written, not on the caller's copy.
    const nextBlock = scopeConflict
      ? { threadId: scopeConflict.threadId, title: scopeConflict.title, files: scopeConflict.files }
      : undefined;
    const blockChanged =
      JSON.stringify(fresh.scopeBlockedBy || null) !== JSON.stringify(nextBlock || null);
    fresh.scopeBlockedBy = nextBlock;
    if (fresh.status === 'queued' && fresh.summary.lastOutputSnippet === message && !blockChanged) {
      return;
    }

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
