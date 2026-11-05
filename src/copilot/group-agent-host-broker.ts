import * as path from 'path';
import * as fs from 'fs';
import * as vscode from 'vscode';
import { execFileSync } from 'child_process';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { GroupAgentHost, loadGroupAgentHost } from '../bridge/group-agent-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { DelegationThread, isValidSessionId } from '../bridge/types';
import { TaskStore } from '../bridge/task-store';
import {
  GroupAgentMailboxFailure, GroupAgentMailboxRequest, claimGroupAgentRuns, hasGroupAgentCancel, hasUnsettledGroupAgentRun,
  publishGroupAgentFailure, publishGroupAgentPid, publishGroupAgentResult,
} from '../bridge/group-agent-mailbox';
import { CopilotModelService } from './model-service';

interface BrokerOptions {
  hostInstanceId: string;
  taskStore: TaskStore;
  models: CopilotModelService;
  storageDir: string;
}

function safeError(error: unknown): string {
  return (error instanceof Error ? error.message : 'Group Agent host execution failed.')
    .replace(/\bBearer\s+[^\s]+/gi, 'Bearer [redacted]')
    .replace(/\bsk-[A-Za-z0-9_-]{20,}\b/g, '[redacted]')
    .slice(0, 4000);
}

export class GroupAgentHostBroker implements vscode.Disposable {
  private timer?: NodeJS.Timeout;
  private disposed = false;
  private readonly active = new Map<string, vscode.CancellationTokenSource>();
  private readonly cancellations: CancellationRegistry;

  constructor(private readonly options: BrokerOptions) {
    this.cancellations = new CancellationRegistry(options.storageDir);
  }

  start(intervalMs = 150): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.scan(), intervalMs);
    this.timer.unref?.();
    this.scan();
  }

  private scan(): void {
    if (this.disposed) return;
    const host = loadGroupAgentHost();
    if (!host || host.hostInstanceId !== this.options.hostInstanceId) return;
    for (const request of claimGroupAgentRuns(host)) {
      if (this.active.has(request.runId)) continue;
      const source = new vscode.CancellationTokenSource();
      this.active.set(request.runId, source);
      void this.execute(request, host, source);
    }
  }

  private validate(request: GroupAgentMailboxRequest, host: GroupAgentHost, afterHandoff = false): DelegationThread {
    const latestHost = loadGroupAgentHost();
    if (!latestHost || latestHost.hostInstanceId !== host.hostInstanceId || latestHost.generation !== request.hostGeneration)
      throw new Error('Group Agent request belongs to a stale host generation.');
    this.options.taskStore.loadFromDisk();
    const thread = this.options.taskStore.getTask(request.threadId);
    if (!thread || thread.status === 'paused' || (!afterHandoff && request.role !== 'bootstrap' && thread.currentRound !== request.round))
      throw new Error('Group Agent request is stale, paused, or deleted.');
    const worktree = thread.worktree?.path;
    if (!worktree || path.resolve(worktree) !== path.resolve(request.worktree))
      throw new Error('Group Agent request worktree does not match the task ledger.');
    const git = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'git.exe' : 'git');
    const commonDir = path.resolve(worktree, execFileSync(git, ['-C', worktree, 'rev-parse', '--git-common-dir'], {
      encoding: 'utf8', timeout: 5000, windowsHide: true, stdio: ['ignore', 'pipe', 'ignore'],
    }).trim());
    if (fs.realpathSync(commonDir) !== fs.realpathSync(host.repositoryCommonDir))
      throw new Error('Group Agent worktree does not belong to the trusted host repository.');
    const initiator = request.role !== 'developer';
    const expectedAgent = initiator ? thread.initiatorAgent : thread.targetAgent;
    const expectedModel = initiator ? thread.initiatorModelRef : thread.targetModelRef;
    const snapshot = initiator ? thread.initiatorGroupBindingSnapshot : thread.targetGroupBindingSnapshot;
    if (request.agent !== expectedAgent || request.bindingSnapshot.agentId !== request.agent ||
        !snapshot || JSON.stringify(request.bindingSnapshot) !== JSON.stringify(snapshot) ||
        JSON.stringify(request.modelRef) !== JSON.stringify(expectedModel) ||
        JSON.stringify(request.bindingSnapshot.modelRef) !== JSON.stringify(expectedModel))
      throw new Error('Group Agent request does not match the task’s pinned Agent and model snapshot.');
    const binding = this.options.models.getAgentBinding(request.agent.slice('copilot_group:'.length));
    if (!binding?.enabled || !binding.toolsAuthorized || binding.source !== snapshot.source || binding.baseUrl !== snapshot.baseUrl)
      throw new Error('The Group Agent source or endpoint changed; explicitly retarget this task to rebind it.');
    const round = thread.rounds.filter((item) => item.round === request.round).slice(-1)[0];
    if (!afterHandoff && request.role === 'developer' && (!round || round.status !== 'running' || !['running', 'reworking'].includes(thread.status) || request.sessionId !== thread.receiverSessionId))
      throw new Error('Group developer request no longer owns the active round/session.');
    if (!afterHandoff && request.role === 'reviewer' && (thread.reviewFlow !== 'dedicated' || thread.status !== 'waiting_review' || !round || round.status !== 'completed' || request.sessionId !== (thread.reviewerSessionId || thread.callerSessionId)))
      throw new Error('Group reviewer request no longer owns the active review/session.');
    if (!afterHandoff && request.role === 'bootstrap' && (thread.reviewFlow !== 'dedicated' || thread.status !== 'queued' || !['launching', 'awaiting_dispatch', 'confirmed'].includes(thread.reviewBootstrap?.phase || '')))
      throw new Error('Group bootstrap request is no longer pending.');
    if (request.role === 'bootstrap' && (request.extraEnv?.MESH_REVIEW_THREAD_ID !== request.threadId || !request.extraEnv?.MESH_REVIEW_BOOTSTRAP_TOKEN))
      throw new Error('Group bootstrap request is missing its one-time task credential.');
    const owner = new TaskRoleLock(this.options.storageDir).getOwner(request.threadId);
    if (!afterHandoff && (!owner || owner.role !== (initiator ? 'reviewer' : 'developer')))
      throw new Error('Group Agent request does not hold the task role lease.');
    if (!afterHandoff && request.role === 'reviewer' && owner?.conversationId !== request.sessionId)
      throw new Error('Group reviewer session does not match its task role lease.');
    if (!afterHandoff && hasUnsettledGroupAgentRun(request.threadId, request.sessionId, request.runId))
      throw new Error('An earlier Group Agent run for this task session has an unconfirmed outcome. Inspect its worktree before resuming.');
    if (request.role !== 'bootstrap' && !isValidSessionId(request.sessionId))
      throw new Error('Group Agent request has an invalid native session id.');
    if (this.cancellations.peek(request.threadId)) throw new Error('Group Agent task has a pending cancellation.');
    return thread;
  }

  private async execute(request: GroupAgentMailboxRequest, host: GroupAgentHost, source: vscode.CancellationTokenSource): Promise<void> {
    let pidSequence = 0;
    let cancelPoll: NodeJS.Timeout | undefined;
    const onPid = (pid?: number) => publishGroupAgentPid(request.runId, ++pidSequence, pid);
    try {
      this.validate(request, host);
      cancelPoll = setInterval(() => {
        try {
          const thread = this.options.taskStore.getTask(request.threadId);
          const currentHost = loadGroupAgentHost();
          if (!currentHost || currentHost.hostInstanceId !== request.hostInstanceId || currentHost.generation !== request.hostGeneration ||
              hasGroupAgentCancel(request.runId) || this.cancellations.peek(request.threadId) || !thread ||
              thread.status === 'paused' || (request.role !== 'bootstrap' && thread.currentRound !== request.round))
            source.cancel();
        } catch { source.cancel(); }
      }, 100);
      const result = await this.options.models.runGroupAgent(
        request.agent.slice('copilot_group:'.length), request.modelRef, request.sessionId,
        request.worktree, request.prompt, source.token, onPid, request.role,
        { threadId: request.threadId, extraEnv: request.extraEnv },
      );
      // Reviewer control tools release the reviewer lease and advance the
      // ledger before the model turn returns. Revalidate immutable ownership
      // and cancellation here, while allowing that intentional handoff.
      const latest = this.validate(request, host, true);
      if (latest.status === 'paused')
        throw new Error('Group Agent result is stale or cancelled.');
      if (source.token.isCancellationRequested || hasGroupAgentCancel(request.runId)) throw new Error('Group Agent run was cancelled before its result could be accepted.');
      onPid(undefined);
      publishGroupAgentResult(request.runId, request.hostGeneration, result);
    } catch (error) {
      const detail = error as { tokenUsage?: import('../bridge/types').TokenUsageMetrics; modelRequests?: number; interruption?: GroupAgentMailboxFailure['interruption']; toolCallsCompleted?: number };
      const cancellation = this.cancellations.peek(request.threadId);
      const latestHost = loadGroupAgentHost();
      const hostChanged = !latestHost || latestHost.hostInstanceId !== request.hostInstanceId || latestHost.generation !== request.hostGeneration;
      const kind: 'deleted' | 'paused' | 'stopped' | undefined = cancellation?.action === 'delete' ? 'deleted' : cancellation?.action === 'pause' || hostChanged ? 'paused' : source.token.isCancellationRequested ? 'stopped' : undefined;
      const interruption = detail.interruption && ['paused', 'deleted', 'timeout', 'stopped'].includes(detail.interruption.kind || '')
        ? { kind: detail.interruption.kind as 'paused' | 'deleted' | 'timeout' | 'stopped', reason: detail.interruption.reason || safeError(error), at: detail.interruption.at || new Date().toISOString(), by: detail.interruption.by }
        : kind ? { kind, reason: hostChanged ? 'The Group Agent host generation changed; the executor stopped before task handoff.' : cancellation?.reason || 'Group Agent execution was cancelled after the host confirmed it stopped.', at: cancellation?.at || new Date().toISOString(), by: hostChanged ? 'group-agent-host' : cancellation?.by || 'group-agent-host' } : undefined;
      try { onPid(undefined); } catch {}
      try {
        publishGroupAgentFailure(request.runId, request.hostGeneration, {
          message: safeError(error), modelRequests: detail.modelRequests, tokenUsage: detail.tokenUsage,
          ...(interruption ? { interruption } : {}), sessionId: request.sessionId,
          ...(detail.toolCallsCompleted !== undefined ? { toolCallsCompleted: detail.toolCallsCompleted } : {}),
        });
      } catch (publishError) { console.warn(`[GroupAgentHostBroker] Could not persist failure for run ${request.runId}:`, safeError(publishError)); }
    } finally {
      if (cancelPoll) clearInterval(cancelPoll);
      this.active.delete(request.runId);
      source.dispose();
    }
  }

  dispose(): void {
    this.disposed = true;
    if (this.timer) clearInterval(this.timer);
    for (const source of this.active.values()) source.cancel();
    this.active.clear();
  }
}
