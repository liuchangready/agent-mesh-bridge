import { getTaskExecutionPath } from './types';
import { resolveTaskTemplate, appendTemplateInstructions } from './task-templates';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';
import {
  AgentId,
  assertDedicatedTask,
  DelegationThread,
  DelegationRound,
  DispatchTaskParams,
  DispatchResult,
  WorktreeInfo,
  isIsolatedTaskWorktree,
  getModelRouting,
  findInFlightRound,
  isReviewerBootstrapPending,
  isUserInterruption,
  shouldResumeNativeSession,
  ExecutionPhase,
} from './types';
import { ExecutionController } from './execution-controller';
import { roundExecutionLogPath } from './round-log';
import { ensureTaskDataDir, removeTaskDataDir } from './task-data-dir';
import { resolveReviewFlow, writeReviewerBootstrapToken } from './reviewer-bootstrap';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { TaskSupervisor } from './task-supervisor';
import { resolveGitRepository, resolveMainWorkspaceRoot, createTaskWorktree, removeTaskWorktree, changedFiles, currentHead } from './git-worktree';
import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from './git-probe';
import { TaskExecutionOwner, TaskExecutionRegistry, TaskExecutionRegistration } from './task-execution-registry';
import { QueueDrainer, QueuedExecution } from './queue-drainer';
import { TaskRoleLock } from './task-role-lock';
import { resolveConfiguredModel, resolveInitiatorModel } from './model-catalog';
import { appendCompactionProtocol, compactRoundBestEffort } from './context-compactor';
import { readStoredRoundUsage } from './token-usage-collector';
import { concurrencyPolicy } from './bridge-policy';
import { ModelApprovalHandler, resolveDispatchModel } from './model-approval';
import { assertCanExecute } from './agent-capabilities';
import { WorkspaceRegistry, WorkspaceResolver, physicalDirectory, findUniqueWindow, readWindow } from './workspace';

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
  public async dispatchTask(params: DispatchTaskParams): Promise<DispatchResult> {
    if (Object.prototype.hasOwnProperty.call(params, 'workspacePath')) throw new Error('workspacePath is not supported; use workspaceRoot or workspaceId');
    if (!params.title?.trim() || !params.prompt?.trim()) {
      throw new Error('A task title and prompt are required for a normal dispatch.');
    }
    let targetAgent: AgentId;

    if (!params.targetAgent || params.targetAgent === 'default') {
      const resolved = await this.resolver.resolveActiveAgent();
      targetAgent = resolved.agent;
    } else {
      targetAgent = params.targetAgent;
    }
    assertCanExecute(targetAgent, 'receive delegated work');

    const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
    // Who is speaking for itself here. `undefined` means the caller identity is
    // genuinely unknown (legacy in-process callers); the historical
    // "antigravity" fallback below must not turn that into a self-dispatch hit.
    const knownInitiator: AgentId | undefined = params.initiatorAgent || callerFromEnv;

    const reviewFlow = resolveReviewFlow(params.reviewFlow);
    const initiatorAgent: AgentId = knownInitiator || 'antigravity';
    assertCanExecute(initiatorAgent, 'review delegated tasks', 'initiator');
    const now = new Date();
    const threadId = this.generateThreadId(now);
    const receiverSessionId = crypto.randomUUID();
    const nowIso = now.toISOString();
    const registry = new WorkspaceRegistry(this.storageDir);
    const resolver = new WorkspaceResolver(registry);
    const requestedWorkspace = params.workspaceId ? resolver.resolve(params.workspaceId) : undefined;
    if (!params.workspaceRoot && !requestedWorkspace) throw new Error('WORKSPACE_REQUIRED');
    const repositoryPath = physicalDirectory(params.workspaceRoot || requestedWorkspace!.rootPath);
    const repository = resolveGitRepository(repositoryPath, this.storageDir, params.targetBranch);
    const mainRoot = resolveMainWorkspaceRoot(repositoryPath, repository.commonDir);
    const selected = registry.register(mainRoot || repositoryPath);
    if (requestedWorkspace && requestedWorkspace.id !== selected.id &&
        physicalDirectory(requestedWorkspace.rootPath) !== repositoryPath) {
      throw new Error('WORKSPACE_MISMATCH');
    }
    const taskTemplate = params.templateId !== undefined ? resolveTaskTemplate(selected.rootPath, params.templateId) : undefined;
    const windowContext = params.workspaceOrigin
      ? readWindow(this.storageDir, params.workspaceOrigin)
      : params.suppressWorkspaceOriginInference ? undefined : findUniqueWindow(this.storageDir, selected.rootPath);

    const round1: DelegationRound = {
      round: 1,
      type: 'initial',
      prompt: params.prompt,
      status: 'running',
      durationMs: 0,
      timestamp: nowIso,
    };

    this.taskStore.loadFromDisk();

    // Record both ends of the model routing: the model the sender is on (best
    // effort) and the model the receiver will execute with. Pinning the receiver
    // model here keeps rework/retry rounds on one model and makes the task list
    // show exactly what will run.
    //
    // The configured default wins unless the caller declares an explicit user
    // request AND the user confirms the switch in VS Code (see model-approval).
    const initiatorModel = resolveInitiatorModel(initiatorAgent, params.initiatorModel);
    const modelResolution = await resolveDispatchModel({
      targetAgent, requestedModel: params.model,
      requestedByUser: params.modelRequestedByUser === true,
      initiatorAgent, title: params.title, handler: this.modelApprovalHandler,
    });
    const targetModel = modelResolution.model;
    const modelNotice = modelResolution.notice;

    // Dedicated reviewer model: the sender-side default, pinned for the whole
    // task. It must never fall back to the receiver default, or a same-agent task
    // would trivially review itself with the execution model.
    const reviewerModel = resolveConfiguredModel(initiatorAgent, undefined, 'initiator');
    if (initiatorAgent === targetAgent) {
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
      const samePinnedModel = reviewerModel === targetModel;
      if (samePinnedModel) {
        throw new Error(
          `Refusing same-agent task "${params.title}": reviewer and receiver both run ${reviewerModel}. ` +
            `A model must not review its own output; configure a different sender-side model for ${initiatorAgent}.`,
        );
      }
    }

    this.taskStore.loadFromDisk();
    if (!this.taskStore.canAcceptTask()) {
      throw new Error('The task ledger is full and every stored task contains active, unmerged, or otherwise non-evictable work. Merge or explicitly delete retained tasks before dispatching another.');
    }
    // One bridge-side directory owns every file this task will produce. Create
    // it before the ledger row and worktree so a failed dispatch can be cleaned
    // as a unit, and persist the absolute path immediately (never re-resolve the
    // global policy for this task later).
    const taskDirs = ensureTaskDataDir(threadId, this.storageDir);
    let worktree: WorktreeInfo;
    try {
      worktree = createTaskWorktree(
        repository,
        threadId,
        this.storageDir,
        taskDirs.artifactDir,
      );
    } catch (error) {
      removeTaskDataDir(taskDirs.artifactDir, true);
      throw error;
    }
    const tempCandidate: DelegationThread = {
      taskTemplate,
      threadId,
      title: params.title || 'Untitled Delegation Task',
      initiatorAgent,
      targetAgent,
      status: 'queued',
      currentRound: 1,
      createdAt: nowIso,
      updatedAt: nowIso,
      receiverSessionId,
      reviewFlow,
      reviewerModel,
      reviewerModelSource: reviewerModel ? 'sender-configured' : 'none',
      reviewBootstrap: { phase: 'pending', attempts: 0 },
      workspace: { id: selected.id, environmentId: selected.environmentId, rootPath: selected.rootPath,
        canonicalRootKey: selected.canonicalRootKey, nameAtDispatch: selected.name,
        source: params.workspaceSource || 'explicit', executionPath: worktree.path,
        ...(windowContext ? { origin: { hostInstanceId: windowContext.hostInstanceId, generation: windowContext.generation } } : {}) },
      artifactDir: taskDirs.artifactDir,
      taskDataDir: taskDirs.artifactDir,
      ...(worktree ? { worktree } : {}),
      initiatorModel: initiatorModel.model,
      initiatorModelSource: initiatorModel.source,
      targetModel,
      modelNotice,
      modelApproval: modelResolution.approval,
      summary: {
        lastPromptSnippet: (params.prompt || '').slice(0, 100),
        lastOutputSnippet: 'Starting the dedicated reviewer conversation...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    const initialPhase: ExecutionPhase = 'review';
    const initialPurpose = 'bootstrap' as const;
    tempCandidate.execution = {
      version: 1,
      revision: 1,
      operationId: `${initialPhase}_1_${Date.now()}`,
      phase: initialPhase,
      round: 1,
      purpose: initialPurpose,
      generation: 1,
      attempt: 0,
      autoRetriesUsed: 0,
      budget: {
        limitMs: 120 * 60 * 1000,
        spentActiveMs: 0,
        maxAutoRetries: 2,
      },
      queue: undefined,
    };

    const thread = tempCandidate;

    // Commit initial state to TaskStore
    try {
      this.taskStore.upsertTask(thread);
    } catch (error) {
      // The ledger row is the durable entry point; without it the worktree and
      // task directory are orphans, so clean both before surfacing the failure.
      try { removeTaskWorktree(worktree); } catch {}
      removeTaskDataDir(taskDirs.artifactDir, true);
      throw error;
    }

    // Dedicated review: the developer must NOT start yet. A fresh reviewer
    // conversation is launched by the extension-host bootstrap service; its
    // `dispatch_task` call (with the one-time token) is what releases the task.
    const token = crypto.randomUUID();
    const tokenWritten = writeReviewerBootstrapToken({
      threadId,
      token,
      agent: initiatorAgent,
      createdAt: nowIso,
    }, this.storageDir, taskDirs.artifactDir);
    if (!tokenWritten) {
      thread.status = 'failed';
      thread.failureRevision = (thread.failureRevision || 0) + 1;
      thread.failure = { stage: 'bootstrap', reason: 'token write failed', at: nowIso, round: thread.currentRound };
      const placeholder = thread.rounds.find(r => r.round === thread.currentRound);
      if (placeholder) { placeholder.status = 'failed'; placeholder.error = 'token write failed'; }
      thread.reviewBootstrap = { phase: 'failed', attempts: 0, lastError: 'token write failed' };
      thread.summary.lastOutputSnippet = '[Reviewer bootstrap failed] could not write the dispatch token';
      this.taskStore.upsertTask(thread);
      throw new Error(
        `Could not start the dedicated reviewer for ${threadId}: the bootstrap token could not be written.`,
      );
    }
    thread.workspace.dispatchAccepted = true;
    this.taskStore.upsertTask(thread);
    if (ExecutionController.getInstance()) {
      void ExecutionController.getInstance()!.startExecution(threadId, 'review', { purpose: 'bootstrap' });
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
        `Task ${threadId} created. ${reviewerLine} Model routing: ${routing.text}.${modelNotice ? ` ${modelNotice}` : ''}`,
      modelRouting: routing,
      modelNotice,
      modelApproval: modelResolution.approval,
      workspace: thread.workspace,
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
    assertDedicatedTask(thread);
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
    this.taskStore.loadFromDisk();

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
        current.queuePhase === 'merge' ||
        current.status === 'merging' || current.status === 'wait_decision' ||
        current.status === 'completed' ||
        current.status === 'failed' ||
        current.status === 'paused' ||
        current.status === 'reviewing';
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
      if (policy.enabled) {
        const owners = this.executionRegistry.listAgentOwners();
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
          getTaskExecutionPath(current),
          'developer',
        );
      } catch (error: any) {
        roleLock.release(roleLease);
        currentRound.status = 'failed';
        currentRound.error = `Could not register task execution: ${error?.message || error}`;
        current.updatedAt = new Date().toISOString();
        enterTaskFailure(current, { stage: 'execution', reason: currentRound.error, at: current.updatedAt, round: currentRound.round });
        this.taskStore.upsertTask(current);
        return;
      }
      current.status = 'running';
      current.summary.lastOutputSnippet = 'Executing in background...';
      this.taskStore.upsertTask(current);
      thread = current;

      console.log(
        `[AsyncDispatcher] Starting background execution: ${thread.threadId} (Round ${roundNumber}) on ${thread.targetAgent}`,
      );

      const startedFailureRevision = thread.failureRevision || 0;
      const startedRoundTimestamp = thread.rounds.filter(r => r.round === roundNumber).slice(-1)[0]?.timestamp;
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
        }, undefined, undefined, undefined, undefined, { failureRevision: startedFailureRevision, roundTimestamp: startedRoundTimestamp });
      }, this.streamSyncIntervalMs);

      const logFilePath = roundExecutionLogPath(
        thread.threadId,
        roundNumber,
        undefined,
        thread.targetAgent,
        thread.artifactDir,
      );
      try {
        fs.mkdirSync(path.dirname(logFilePath), { recursive: true });
        fs.writeFileSync(logFilePath, '', 'utf8');
      } catch {}

      let result: RunResult;
      // Baseline right before the agent starts: the diff afterwards tells us
      // exactly which files this round changed, even if the caller declared
      // nothing or the agent edited files unrelated to its task.
      const workspaceBefore = captureWorkspaceSnapshot(getTaskExecutionPath(thread));
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: appendTemplateInstructions(isRework ? appendCompactionProtocol(prompt, roundNumber) : prompt, thread.taskTemplate, 'worker'),
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: getTaskExecutionPath(thread),
          taskThreadId: thread.threadId,
          artifactDir: thread.artifactDir,
          taskRound: roundNumber,
          model: thread.targetModel,
          role: 'developer',
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
        workspaceBefore,
        captureWorkspaceSnapshot(getTaskExecutionPath(thread)),
      );

      this.taskStore.loadFromDisk();
      const latest = this.taskStore.getTask(thread.threadId);
      if (!latest) {
        console.warn(
          `[AsyncDispatcher] Task ${thread.threadId} was deleted while round ${roundNumber} was executing.`,
        );
        return;
      }

      const liveRound = latest.rounds.filter(r => r.round === roundNumber).slice(-1)[0];
      if (latest.archivedAt || (latest.failureRevision || 0) !== startedFailureRevision ||
          latest.currentRound !== roundNumber || !liveRound || liveRound.timestamp !== startedRoundTimestamp ||
          roleLock.getOwner(thread.threadId)?.token !== roleLease.token) return;

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
          latest.worktree.headCommit = currentHead(latest.worktree.path);
          latest.worktree.integration = { phase: 'ready', sourceCommit: latest.worktree.headCommit, updatedAt: new Date().toISOString() };
        } catch (error: any) {
          result.error = `Could not inspect the task worktree: ${error?.message || error}`;
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
      const finalThreadStatus = isPaused ? 'paused' : isSuccess ? 'reviewing' : 'failed';

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
        {
          failureRevision: startedFailureRevision,
          roundTimestamp: startedRoundTimestamp,
          receiverSessionCaptured: Boolean(result.actualSessionId || latest.receiverSessionCaptured),
        },
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
    const owners = this.executionRegistry.listAgentOwners();
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
  ): void {
    this.taskStore.loadFromDisk();
    const fresh = this.taskStore.getTask(thread.threadId);
    const candidateRound = findInFlightRound(thread);
    const freshRound = fresh ? findInFlightRound(fresh) : undefined;
    if (
      !fresh ||
      fresh.queuePhase === 'merge' ||
      (fresh.status !== 'queued' && fresh.status !== 'running') ||
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
    if (fresh.status === 'queued' && fresh.summary.lastOutputSnippet === message) {
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
import { enterTaskFailure } from './types';
