import { resolveTaskTemplate, appendTemplateInstructions } from './task-templates';
import * as crypto from 'crypto';
import * as path from 'path';
import {
  AgentId,
  assertDedicatedTask,
  DelegationThread,
  DelegationRound,
  DispatchTaskParams,
  DispatchResult,
  WorktreeInfo,
  getModelRouting,
  ExecutionPhase,
} from './types';
import { ExecutionController } from './execution-controller';
import { ensureTaskDataDir, removeTaskDataDir } from './task-data-dir';
import { resolveReviewFlow, writeReviewerBootstrapToken } from './reviewer-bootstrap';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { resolveGitRepository, resolveMainWorkspaceRoot, createTaskWorktree, removeTaskWorktree } from './git-worktree';
import { TaskExecutionRegistry } from './task-execution-registry';
import { resolveConfiguredModel, resolveInitiatorModel } from './model-catalog';
import { ModelApprovalHandler, resolveDispatchModel } from './model-approval';
import { assertCanExecute } from './agent-capabilities';
import { WorkspaceRegistry, WorkspaceResolver, physicalDirectory, findUniqueWindow, readWindow, hasResidentExecutionHost } from './workspace';

export class AsyncDispatcher {
  private taskStore: TaskStore;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private streamSyncIntervalMs: number = 30000;
  private readonly storageDir: string;
  /** Answers model-switch confirmations; the file channel unless overridden. */
  private modelApprovalHandler?: ModelApprovalHandler;

  constructor(
    taskStore: TaskStore,
    resolver: ActiveAgentResolver,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
    _executionRegistry?: TaskExecutionRegistry,
    private readonly v2?: { admitted: boolean },
  ) {
    this.taskStore = taskStore;
    this.resolver = resolver;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;
    this.storageDir = path.dirname(taskStore.getStoragePath());
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
    if (this.v2 && !this.v2.admitted) throw new Error('V2_STARTUP_BLOCKED: runtime component preflight has not passed.');
    const submitOnly = Boolean(this.v2);
    if (submitOnly && !await hasResidentExecutionHost(this.storageDir)) {
      throw new Error('RESIDENT_EXECUTION_REQUIRED: Open the Agent Bridge extension host and wait for its runtime preflight before dispatching.');
    }
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
        ...(this.v2 ? { dispatchAccepted: false } : {}),
        ...(windowContext ? { origin: { hostInstanceId: windowContext.hostInstanceId, generation: windowContext.generation } } : {}) },
      artifactDir: taskDirs.artifactDir,
      artifactRoot: taskDirs.artifactRoot,
      taskDataDir: taskDirs.artifactDir,
      ...(worktree ? { worktree } : {}),
      initiatorModel: initiatorModel.model,
      initiatorModelSource: initiatorModel.source,
      targetModel,
      modelNotice,
      modelApproval: modelResolution.approval,
      summary: {
        lastPromptSnippet: (params.prompt || '').slice(0, 100),
        lastOutputSnippet: submitOnly ? 'Waiting for the resident Bridge to start the dedicated reviewer.' : 'Starting the dedicated reviewer conversation...',
        totalDurationMs: 0,
      },
      rounds: [round1],
    };

    const initialPhase: ExecutionPhase = 'review';
    const initialPurpose = 'bootstrap' as const;
    tempCandidate.execution = {
      version: 2,
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
      await this.taskStore.transitionExecution(threadId, thread.execution!.revision, (fresh) => {
        fresh.status = 'failed';
        fresh.failureRevision = (fresh.failureRevision || 0) + 1;
        fresh.failure = { stage: 'bootstrap', reason: 'token write failed', at: nowIso, round: fresh.currentRound };
        const placeholder = fresh.rounds.find(r => r.round === fresh.currentRound);
        if (placeholder) { placeholder.status = 'failed'; placeholder.error = 'token write failed'; }
        fresh.reviewBootstrap = { phase: 'failed', attempts: 0, lastError: 'token write failed' };
        fresh.summary.lastOutputSnippet = '[Reviewer bootstrap failed] could not write the dispatch token';
      });
      throw new Error(
        `Could not start the dedicated reviewer for ${threadId}: the bootstrap token could not be written.`,
      );
    }
    await this.taskStore.transitionExecution(threadId, this.taskStore.getTask(threadId)?.execution?.revision ?? thread.execution!.revision, (fresh) => {
      if (fresh.workspace) fresh.workspace.dispatchAccepted = true;
    });
    if (!submitOnly) void ExecutionController.getInstance()?.startExecution(threadId, 'review', { purpose: 'bootstrap' });
    const routing = getModelRouting(thread);
    const reviewerLine =
      `Dedicated reviewer ${initiatorAgent}` +
      `${reviewerModel ? `(${reviewerModel})` : ''} will dispatch this task to ${targetAgent}` +
      `${targetModel ? `(${targetModel})` : ''}.`;
    const stopInstruction = submitOnly
      ? 'End your turn now. The resident Bridge will start the dedicated reviewer and publish the result in the task summary.'
      : 'STOP NOW — a dedicated reviewer conversation is starting and will dispatch this task. ' +
      'End your turn immediately: you are NOT the reviewer and the bridge will not wake you for review. ' +
      'The reviewer reports the result into the task summary; use get_task_summary or the task tree.';
    return {
      threadId,
      receiverSessionId,
      targetAgent,
      status: submitOnly ? 'QUEUED' : 'REVIEWER_STARTING',
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

  private generateThreadId(d: Date): string {
    const pad = (n: number) => String(n).padStart(2, '0');
    const ymd = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}`;
    const hms = `${pad(d.getHours())}${pad(d.getMinutes())}${pad(d.getSeconds())}`;
    const rand = crypto.randomBytes(2).toString('hex');
    return `thread_${ymd}_${hms}_${rand}`;
  }
}
import { enterTaskFailure } from './types';
