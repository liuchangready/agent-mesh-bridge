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
  findInFlightRound,
  isReviewerBootstrapPending,
  isValidSessionId,
  isUserInterruption,
  shouldResumeNativeSession,
} from './types';
import { readBridgePolicy } from './bridge-policy';
import { roundExecutionLogPath } from './round-log';
import { resolveReviewFlow, writeReviewerBootstrapToken } from './reviewer-bootstrap';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import {
  validateExpectedFiles,
  findScopeConflicts,
  describeScopeConflict,
  ScopeConflict,
} from './file-scope';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { TaskExecutionOwner, TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { TaskRoleLock } from './task-role-lock';
import { resolveConfiguredModel, resolveInitiatorModel } from './model-catalog';
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
  private executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private queueDrainer: QueueDrainer;
  /** Answers model-switch confirmations; the file channel unless overridden. */
  private modelApprovalHandler?: ModelApprovalHandler;

  constructor(
    taskStore: TaskStore,
    resolver: ActiveAgentResolver,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    executionRegistry?: TaskExecutionRegistry,
  ) {
    this.taskStore = taskStore;
    this.resolver = resolver;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.storageDir = path.dirname(taskStore.getStoragePath());
    this.executionRegistry = executionRegistry || new TaskExecutionRegistry(this.storageDir);
    this.queueDrainer = new QueueDrainer(
      this.taskStore,
      this.executionRegistry,
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
    // Who is speaking for itself here. `undefined` means the caller identity is
    // genuinely unknown (legacy in-process callers); the historical
    // "antigravity" fallback below must not turn that into a self-dispatch hit.
    const knownInitiator: AgentId | undefined = params.initiatorAgent || callerFromEnv;

    // Review topology is pinned per task. Dedicated needs a long-lived host to
    // launch the reviewer bootstrap, so a standalone MCP process falls back to
    // legacy instead of creating a task nobody could dispatch.
    const reviewFlow = resolveReviewFlow(params.reviewFlow, readBridgePolicy().reviewFlow);

    // Same-agent dispatch is allowed ONLY with the dedicated reviewer: the
    // reviewer gets its own fresh conversation, so it can never collide with the
    // session that is running this very dispatch. Legacy review resumes the
    // requester's own conversation and would deadlock on the native writer lock.
    if (knownInitiator && targetAgent === knownInitiator && reviewFlow !== 'dedicated') {
      throw new Error(
        `Refusing to dispatch "${params.title || 'Untitled Delegation Task'}" from ${knownInitiator} to itself: ` +
          `self-dispatch requires the dedicated reviewer flow (the VS Code extension host must be running so a ` +
          `fresh reviewer conversation can dispatch the task). A legacy review would resume the requester's own ` +
          `session while it is still busy with this dispatch.`,
      );
    }

    const initiatorAgent: AgentId = knownInitiator || 'antigravity';
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

    this.taskStore.loadFromDisk();
    const allKnownTasks = this.taskStore.getAllTasks();
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

    // Dedicated reviewer model: the sender-side default, pinned for the whole
    // task. It must never fall back to the receiver default, or a same-agent task
    // would trivially review itself with the execution model.
    const reviewerModel =
      reviewFlow === 'dedicated'
        ? resolveConfiguredModel(initiatorAgent, undefined, 'initiator')
        : undefined;
    if (reviewFlow === 'dedicated' && initiatorAgent === targetAgent) {
      if (!reviewerModel) {
        throw new Error(
          `Refusing same-agent task "${params.title}": configure a sender-side reviewer model for ` +
            `${initiatorAgent} (agentMeshBridge.initiatorModels.*) that differs from the receiver model.`,
        );
      }
      if (!targetModel) {
        throw new Error(
          `Refusing same-agent task "${params.title}": configure a receiver-side model for ` +
            `${initiatorAgent} (agentMeshBridge.models.*) so the reviewer and the executor cannot be the same model.`,
        );
      }
      if (reviewerModel === targetModel) {
        throw new Error(
          `Refusing same-agent task "${params.title}": reviewer and receiver both run ${reviewerModel}. ` +
            `A model must not review its own output; configure a different sender-side model for ${initiatorAgent}.`,
        );
      }
    }

    const tempCandidate: DelegationThread = {
      threadId,
      title: params.title || 'Untitled Delegation Task',
      initiatorAgent,
      targetAgent,
      status: reviewFlow === 'dedicated' ? 'queued' : 'running',
      currentRound: 1,
      createdAt: nowIso,
      updatedAt: nowIso,
      receiverSessionId,
      ...(reviewFlow === 'dedicated'
        ? {
            reviewFlow,
            requesterSessionId: callerSessionId,
            requesterAgent: initiatorAgent,
            requesterModel: initiatorModel.model,
            requesterModelSource: initiatorModel.source,
            reviewerModel,
            reviewerModelSource: reviewerModel ? ('sender-configured' as const) : ('none' as const),
            reviewBootstrap: { phase: 'pending' as const, attempts: 0 },
          }
        : { callerSessionId }),
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
        lastOutputSnippet:
          reviewFlow === 'dedicated'
            ? 'Starting the dedicated reviewer conversation...'
            : 'Executing in background...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    // Batch-level scope gate: refuse to schedule a task whose declared files are
    // still owned by another open task (declared or actually touched).
    const scopeConflict = findScopeConflicts(tempCandidate, scopePeers);
    const policy = concurrencyPolicy();
    const owners = policy.enabled ? this.executionRegistry.listOwners() : [];
    const globalSlotFull = policy.enabled && owners.length >= policy.maxConcurrentTasks;
    const agentSlots = policy.enabled ? owners.filter((owner) => owner.agent === targetAgent).length : 0;
    const agentSlotFull = policy.enabled && agentSlots >= policy.maxConcurrentPerAgent;
    const capacityFull = globalSlotFull || agentSlotFull;
    const isQueued = !!scopeConflict || capacityFull;

    let queueDetail = '';
    if (scopeConflict) {
      tempCandidate.scopeBlockedBy = {
        threadId: scopeConflict.threadId,
        title: scopeConflict.title,
        files: scopeConflict.files,
      };
      queueDetail = describeScopeConflict(scopeConflict);
    } else if (agentSlotFull) {
      queueDetail =
        `Queued: agent ${targetAgent} already occupies ${agentSlots}/${policy.maxConcurrentPerAgent} ` +
        `of its concurrent slots; waiting for one of its running tasks to finish.`;
    } else if (globalSlotFull) {
      queueDetail =
        `Queued: active execution limit reached (${owners.length}/${policy.maxConcurrentTasks}); ` +
        `waiting for a free registry slot.`;
    }

    if (isQueued) {
      tempCandidate.status = 'queued';
      tempCandidate.summary.lastOutputSnippet = queueDetail;
    }

    const thread = tempCandidate;

    // Commit initial state to TaskStore
    this.taskStore.upsertTask(thread);

    // Dedicated review: the developer must NOT start yet. A fresh reviewer
    // conversation is launched by the extension-host bootstrap service; its
    // `dispatch_task` call (with the one-time token) is what releases the task.
    if (reviewFlow === 'dedicated') {
      const token = crypto.randomUUID();
      const tokenWritten = writeReviewerBootstrapToken({
        threadId,
        token,
        agent: initiatorAgent,
        createdAt: nowIso,
      });
      if (!tokenWritten) {
        thread.status = 'failed';
        thread.reviewBootstrap = { phase: 'failed', attempts: 0, lastError: 'token write failed' };
        thread.summary.lastOutputSnippet = '[Reviewer bootstrap failed] could not write the dispatch token';
        this.taskStore.upsertTask(thread);
        throw new Error(
          `Could not start the dedicated reviewer for ${threadId}: the bootstrap token could not be written.`,
        );
      }
      const routing = getModelRouting(thread);
      const reviewerLine =
        `Dedicated reviewer ${initiatorAgent}` +
        `${reviewerModel ? `(${reviewerModel})` : ''} will dispatch this task to ${targetAgent}` +
        `${targetModel ? `(${targetModel})` : ''}.`;
      const stopInstruction =
        'STOP NOW — a dedicated reviewer conversation is starting and will dispatch this task. ' +
        'End your turn immediately: you are NOT the reviewer and the bridge will not wake you for review. ' +
        'The reviewer reports the result into the task summary; use get_task_summary or the task tree.';
      return {
        threadId,
        receiverSessionId,
        targetAgent,
        status: 'REVIEWER_STARTING',
        message:
          `nextAction=END_TURN_AND_WAIT_FOR_CALLBACK. ${stopInstruction} ` +
          `Task ${threadId} created. ${reviewerLine} Model routing: ${routing.text}.`,
        modelRouting: routing,
        modelNotice,
        modelApproval: modelResolution.approval,
        nextAction: 'END_TURN_AND_WAIT_FOR_CALLBACK',
        stopInstruction,
      };
    }

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
    // A dedicated task may not execute before its reviewer conversation has
    // dispatched it. The bootstrap service owns that first turn.
    if (isReviewerBootstrapPending(thread)) {
      this.markThreadQueued(
        thread,
        undefined,
        'Queued: waiting for the dedicated reviewer conversation to dispatch this task.',
      );
      this.queueDrainer.schedule(1_500);
      return;
    }
    if (this.executionRegistry.getOwner(thread.threadId)) {
      this.queueDrainer.schedule(1_500);
      return;
    }
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
    const roleLock = new TaskRoleLock(this.storageDir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer');
    if (!roleLease) {
      this.queueDrainer.schedule(1_500);
      return;
    }

    const policy = concurrencyPolicy();
    let registration: TaskExecutionRegistration | undefined;
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
      const currentRound = current.rounds.filter((round) => round.round === roundNumber).slice(-1)[0];
      const terminal =
        current.status === 'completed' ||
        current.status === 'failed' ||
        current.status === 'review_failed' ||
        current.status === 'paused' ||
        current.status === 'waiting_review';
      if (
        terminal ||
        current.targetAgent !== thread.targetAgent ||
        !currentRound ||
        currentRound.status !== 'running' ||
        currentRound.prompt !== prompt ||
        findInFlightRound(current)?.round !== roundNumber ||
        isReviewerBootstrapPending(current)
      ) {
        return;
      }
      const freshScopeConflict = findScopeConflicts(current, this.taskStore.getAllTasks());
      if (freshScopeConflict) {
        roleLock.release(roleLease);
        this.markThreadQueued(current, undefined, describeScopeConflict(freshScopeConflict), freshScopeConflict);
        this.queueDrainer.schedule(1_500);
        return;
      }
      if (policy.enabled) {
        const owners = this.executionRegistry.listOwners();
        const agentSlots = owners.filter((owner) => owner.agent === current.targetAgent).length;
        if (owners.length >= policy.maxConcurrentTasks || agentSlots >= policy.maxConcurrentPerAgent) {
          roleLock.release(roleLease);
          this.markThreadQueued(current, undefined, this.describeConcurrencyBlocked(current.targetAgent));
          this.queueDrainer.schedule(1_500);
          return;
        }
      }
      try {
        registration = this.executionRegistry.begin(
          current.threadId,
          roundNumber,
          current.targetAgent,
          current.workspacePath,
        );
      } catch (error: any) {
        roleLock.release(roleLease);
        current.status = 'failed';
        currentRound.status = 'failed';
        currentRound.error = `Could not register task execution: ${error?.message || error}`;
        current.updatedAt = new Date().toISOString();
        this.taskStore.upsertTask(current);
        return;
      }
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

      const logFilePath = roundExecutionLogPath(
        thread.threadId,
        roundNumber,
        undefined,
        thread.targetAgent,
      );
      try {
        fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
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
          onChildPid: (pid) => {
            if (registration) this.executionRegistry.attachChildPid(pid, registration);
            roleLock.attachChild(roleLease, pid);
          },
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
      if (registration) this.executionRegistry.end(registration);
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

  /** Describe an enabled best-effort execution cap. */
  private describeConcurrencyBlocked(targetAgent: string): string {
    const policy = concurrencyPolicy();
    const owners = this.executionRegistry.listOwners();
    if (owners.length >= policy.maxConcurrentTasks) {
      return (
        `Queued: active execution limit reached (${owners.length}/${policy.maxConcurrentTasks}); ` +
        `waiting for a free execution record.`
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
    owner?: TaskExecutionOwner,
    detail?: string,
    scopeConflict?: ScopeConflict,
  ): void {
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
    const liveRole = new TaskRoleLock(this.storageDir).getOwner(thread.threadId);
    if (this.executionRegistry.getOwner(thread.threadId) || liveRole) return;
    const message =
      detail ||
      (owner
        ? `Queued: Waiting for task ${owner.threadId} (round ${owner.round}) to finish.`
        : 'Queued: Waiting for a free execution slot.');
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
