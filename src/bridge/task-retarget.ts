/**
 * Move an existing task to another executor: another agent and/or another model.
 *
 * Dispatch pins `targetAgent` + `targetModel` once and every rework/retry round
 * reuses them. That is the right default (one task, one model, no silent drift),
 * but it left no supported way to hand a parked task to a different agent — e.g.
 * a queued task whose default model is unavailable, or a paused task the user
 * wants a cheaper/stronger agent to finish.
 *
 * This service owns that switch and its invariants:
 *
 *  - never while a round is in flight (AGENTS.md §7: one running round per task);
 *  - the configured default model still wins unless the user explicitly named
 *    another one, and a non-default model still needs the click consent from
 *    `model-approval` (AGENTS.md §18);
 *  - changing the agent drops the native session (`receiverSessionId`), because a
 *    session belongs to the CLI that created it — Claude cannot resume a Codex
 *    rollout, and DSH rejects unknown session ids. A model-only switch keeps the
 *    session so rework continuity survives;
 *  - every switch is recorded on the thread (`retargets[]`) and flows into the
 *    ledger, so the task tree shows who actually executed which round.
 */

import * as crypto from 'crypto';
import * as path from 'path';
import {
  AgentId,
  DelegationThread,
  ExecutorOverride,
  GroupModelRef,
  RetargetTaskResult,
  TaskRetargetRecord,
  describeRoundInFlight,
  findInFlightRound,
  getModelRouting,
} from './types';
import { TaskStore } from './task-store';
import {
  ModelApprovalHandler,
  resolveDispatchGroupModelRef,
  resolveDispatchModel,
} from './model-approval';
import { normalizeModelId, resolveConfiguredModel } from './model-catalog';
import { assertCanExecute, assertGroupModelReady } from './agent-capabilities';
import { isGroupAgentId, loadGroupAgentConfig } from './group-agent-registry';
import { cleanupOwnedGroupAgentSession } from './group-agent-mailbox';

function sameModelRef(left?: GroupModelRef, right?: GroupModelRef): boolean {
  return !!left && !!right && left.source === right.source && left.id === right.id &&
    (left.source === 'vscode-lm' ? right.source === 'vscode-lm' && left.vendor === right.vendor :
      right.source === 'openai-compatible' && left.origin === right.origin && left.bindingRevision === right.bindingRevision);
}

export interface RetargetTaskParams extends ExecutorOverride {
  threadId: string;
  /** Recorded in the audit trail; shown in the task detail panel. */
  reason?: string;
  by?: string;
}

export class TaskRetargetService {
  private readonly taskStore: TaskStore;
  private modelApprovalHandler?: ModelApprovalHandler;

  constructor(taskStore: TaskStore) {
    this.taskStore = taskStore;
  }

  /** Answer model-switch confirmations; defaults to the cross-process channel. */
  public setModelApprovalHandler(handler: ModelApprovalHandler | undefined): void {
    this.modelApprovalHandler = handler;
  }

  /**
   * Switch the executor of an existing task.
   *
   * @throws when the task is gone, already completed, or still has a round in
   * flight; also when the user declines the model switch.
   */
  public async retargetTask(params: RetargetTaskParams): Promise<RetargetTaskResult> {
    this.taskStore.loadFromDisk();
    let thread = this.taskStore.getTask(params.threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${params.threadId}`);
    }

    const inFlight = findInFlightRound(thread);
    if (inFlight) {
      throw new Error(
        `Cannot switch the executor of ${params.threadId}: ${describeRoundInFlight(thread, inFlight)} ` +
          `Pause the task first, switch, then resume.`,
      );
    }
    if (thread.status === 'completed') {
      throw new Error(
        `Cannot switch the executor of ${params.threadId}: the task is already completed. ` +
          `Dispatch a new task for new work.`,
      );
    }

    const initialStatus = thread.status;
    const initialCurrentRound = thread.currentRound;
    const initialTargetAgent = thread.targetAgent;
    const initialTargetModel = thread.targetModel;
    const initialTargetModelRef = thread.targetModelRef;
    const initialReceiverSessionId = thread.receiverSessionId;
    const initialReviewFlow = thread.reviewFlow;
    const initialBootstrapPhase = thread.reviewBootstrap?.phase;
    const initialReviewerSessionId = thread.reviewerSessionId;
    const initialReviewerSessionCaptured = thread.reviewerSessionCaptured;
    const initialCallerSessionId = thread.callerSessionId;
    const initialCurrentRoundEntry = (thread.rounds || [])
      .filter((round) => round.round === initialCurrentRound)
      .slice(-1)[0];

    const nextAgent: AgentId = params.targetAgent || initialTargetAgent;
    assertCanExecute(nextAgent, 'be selected as a task receiver');

    const agentChanged = nextAgent !== initialTargetAgent;
    const requested = normalizeModelId(params.model);
    if (params.modelRef && requested) throw new Error('Choose either a CLI model string or a structured Group Agent modelRef.');

    // Resolve the model BEFORE touching the thread: a declined switch must leave
    // the task exactly as it was.
    let selectedModelRef: GroupModelRef | undefined;
    let resolution: { model?: string; notice?: string; approval?: import('./types').ModelApprovalRecord };
    if (isGroupAgentId(nextAgent)) {
      const binding = loadGroupAgentConfig()?.agents.find((entry) => entry.id === nextAgent);
      if (!binding?.enabled || !binding.toolsAuthorized || !binding.receiverModel) throw new Error(`Group Agent ${nextAgent} is not ready as a receiver.`);
      if (requested) throw new Error('Group Agent retargeting requires a structured modelRef, not a CLI model string.');
      const sameRef = (left: GroupModelRef, right: GroupModelRef) => left.source === right.source && left.id === right.id &&
        (left.source === 'vscode-lm' ? right.source === 'vscode-lm' && left.vendor === right.vendor : right.source === 'openai-compatible' && left.origin === right.origin && left.bindingRevision === right.bindingRevision);
      if (params.modelRef && !sameRef(params.modelRef, binding.receiverModel) && !binding.models?.some((member) => sameRef(member, params.modelRef!)))
        throw new Error('The selected Group Agent modelRef is not a current member of this group binding.');
      const approval = await resolveDispatchGroupModelRef({
        targetAgent: nextAgent, requestedModelRef: params.modelRef, defaultModelRef: binding.receiverModel,
        requestedByUser: params.modelRequestedByUser === true, initiatorAgent: thread.initiatorAgent,
        title: thread.title, handler: this.modelApprovalHandler,
      });
      selectedModelRef = params.modelRef ? approval.modelRef : (agentChanged ? binding.receiverModel : thread.targetModelRef);
      if (!selectedModelRef) throw new Error(`Group Agent ${nextAgent} has no pinned receiver model.`);
      assertGroupModelReady(nextAgent, 'receiver', selectedModelRef);
      resolution = { model: selectedModelRef?.id, notice: approval.notice, approval: approval.approval };
    } else {
      if (params.modelRef) throw new Error('A structured Group Agent modelRef cannot be used for a CLI target.');
      resolution = await resolveDispatchModel({
        targetAgent: nextAgent, requestedModel: requested,
        requestedByUser: params.modelRequestedByUser === true,
        initiatorAgent: thread.initiatorAgent, title: thread.title,
        handler: this.modelApprovalHandler,
      });
    }

    // Model approval is an async boundary. Re-read before writing so an approve,
    // pause, retry, or other executor change during the dialog cannot be replaced
    // by this stale snapshot.
    this.taskStore.loadFromDisk();
    const fresh = this.taskStore.getTask(params.threadId);
    if (!fresh) throw new Error(`Cannot switch executor: task ${params.threadId} disappeared during model approval.`);
    const freshCurrentRoundEntry = (fresh.rounds || [])
      .filter((round) => round.round === fresh.currentRound)
      .slice(-1)[0];
    if (
      fresh.archivedAt || fresh.status === 'completed' || findInFlightRound(fresh) ||
      fresh.status !== initialStatus || fresh.currentRound !== initialCurrentRound ||
      fresh.targetAgent !== initialTargetAgent || fresh.targetModel !== initialTargetModel ||
      JSON.stringify(fresh.targetModelRef) !== JSON.stringify(initialTargetModelRef) ||
      fresh.receiverSessionId !== initialReceiverSessionId ||
      fresh.reviewFlow !== initialReviewFlow ||
      fresh.reviewBootstrap?.phase !== initialBootstrapPhase ||
      fresh.reviewerSessionId !== initialReviewerSessionId ||
      fresh.reviewerSessionCaptured !== initialReviewerSessionCaptured ||
      fresh.callerSessionId !== initialCallerSessionId ||
      freshCurrentRoundEntry?.round !== initialCurrentRoundEntry?.round ||
      freshCurrentRoundEntry?.type !== initialCurrentRoundEntry?.type ||
      freshCurrentRoundEntry?.status !== initialCurrentRoundEntry?.status ||
      freshCurrentRoundEntry?.prompt !== initialCurrentRoundEntry?.prompt
    ) {
      throw new Error(
        `Cannot switch executor of ${params.threadId}: task state changed while model approval was pending. ` +
          `No executor change was applied.`,
      );
    }
    thread = fresh;

    const previousAgent = thread.targetAgent;
    const previousModel = thread.targetModel;

    // Same-agent execution is allowed only for a dedicated-review task, and only
    // when the reviewer model differs from the executor model. A model must never
    // review its own output; a legacy task would also resume the busy session.
    if (nextAgent === thread.initiatorAgent) {
      if (thread.reviewFlow !== 'dedicated') {
        throw new Error(
          `Cannot run task ${thread.threadId} on ${nextAgent}: that is the task's initiator ` +
            `and the task uses the legacy review flow, whose callback resumes the initiator's own session. ` +
            `Pick a different executor agent.`,
        );
      }
      const reviewerRef = thread.reviewerModelRef || thread.initiatorModelRef;
      const reviewerModel = thread.reviewerModel || resolveConfiguredModel(thread.initiatorAgent, undefined, 'initiator');
      const nextModel = resolution.model;
      if ((!reviewerRef && !reviewerModel) || !nextModel) {
        throw new Error(
          `Cannot retarget ${thread.threadId} to its own agent ${nextAgent}: both the reviewer model and ` +
            `the executor model must be configured so they can be proven different.`,
        );
      }
      const samePinnedModel = reviewerRef && selectedModelRef ? sameModelRef(reviewerRef, selectedModelRef) : reviewerModel === nextModel;
      if (samePinnedModel) {
        throw new Error(
          `Cannot retarget ${thread.threadId} to ${nextAgent}(${nextModel}): reviewer and executor would ` +
            `be the same model. Configure a different sender-side model for the reviewer first.`,
        );
      }
    }

    let nextModel: string | undefined;
    if (!requested) {
      // No model named: a new agent starts on its configured default, while an
      // unchanged agent keeps whatever the task is already pinned to.
      nextModel = agentChanged ? resolution.model : previousModel;
    } else if (resolution.notice && !resolution.approval && !agentChanged) {
      // The model request was ignored (not declared as a user request): keep the
      // task's current model instead of resetting it to the configured default.
      nextModel = previousModel;
    } else {
      nextModel = resolution.model;
    }

    const previousModelRef = thread.targetModelRef;
    const modelChanged = nextModel !== previousModel || JSON.stringify(selectedModelRef) !== JSON.stringify(previousModelRef);
    const notice = requested ? resolution.notice : undefined;

    if (!agentChanged && !modelChanged) {
      return {
        threadId: thread.threadId,
        status: 'UNCHANGED',
        targetAgent: previousAgent,
        targetModel: previousModel,
        previousAgent,
        previousModel,
        sessionReset: false,
        modelNotice: notice,
        modelApproval: resolution.approval,
        modelRouting: getModelRouting(thread),
        message:
          `Task ${thread.threadId} already runs on ${previousAgent}` +
          `${previousModel ? ` (${previousModel})` : ''}; nothing to switch.` +
          (notice ? ` ${notice}` : ''),
      };
    }

    const nowIso = new Date().toISOString();
    const sessionReset = agentChanged;
    const previousReceiverSessionId = sessionReset ? thread.receiverSessionId : undefined;

    if (sessionReset) {
      // A native session cannot cross agents, so the next round starts one: a
      // fresh preallocated id (never resumed) keeps the runner contract intact.
      thread.receiverSessionId = crypto.randomUUID();
      thread.receiverSessionCaptured = false;
    }

    thread.targetAgent = nextAgent;
    thread.targetModel = nextModel;
    thread.targetModelRef = selectedModelRef;
    if (isGroupAgentId(nextAgent) && selectedModelRef) {
      const binding = loadGroupAgentConfig()?.agents.find((entry) => entry.id === nextAgent);
      if (!binding?.enabled || !binding.toolsAuthorized) throw new Error(`Group Agent ${nextAgent} changed before retarget was committed.`);
      thread.targetGroupBindingSnapshot = { agentId: nextAgent, revision: binding.revision, source: binding.source, ...(binding.baseUrl ? { baseUrl: binding.baseUrl } : {}), modelRef: selectedModelRef };
    } else delete thread.targetGroupBindingSnapshot;
    thread.modelNotice = notice;
    if (resolution.approval) thread.modelApproval = resolution.approval;
    thread.updatedAt = nowIso;

    const record: TaskRetargetRecord = {
      at: nowIso,
      by: params.by || 'caller',
      reason: (params.reason || '').trim() || undefined,
      fromAgent: previousAgent,
      toAgent: nextAgent,
      fromModel: previousModel,
      toModel: nextModel,
      fromModelRef: previousModelRef,
      toModelRef: selectedModelRef,
      previousReceiverSessionId,
      sessionReset,
    };
    thread.retargets = [...(thread.retargets || []), record];

    const direction = [
      previousAgent !== nextAgent ? `${previousAgent} -> ${nextAgent}` : undefined,
      previousModel !== nextModel
        ? `${previousModel || 'agent default'} -> ${nextModel || 'agent default'}`
        : undefined,
    ]
      .filter(Boolean)
      .join(', ');
    thread.summary.lastOutputSnippet = `Executor switched: ${direction}`;

    this.taskStore.upsertTask(thread);
    if (sessionReset && isGroupAgentId(previousAgent) && previousReceiverSessionId)
      cleanupOwnedGroupAgentSession(previousReceiverSessionId, thread.threadId, previousAgent, path.dirname(this.taskStore.getStoragePath()));

    return {
      threadId: thread.threadId,
      status: 'RETARGETED',
      targetAgent: nextAgent,
      targetModel: nextModel,
      previousAgent,
      previousModel,
      sessionReset,
      modelNotice: notice,
      modelApproval: resolution.approval,
      modelRouting: getModelRouting(thread),
      message:
        `Task ${thread.threadId} now runs on ${nextAgent}${nextModel ? ` (${nextModel})` : ''}` +
        `${direction ? ` — ${direction}` : ''}.` +
        (sessionReset
          ? ' The previous native session cannot be resumed by another agent, so the next round starts a new one.'
          : ' The native session is preserved.') +
        (notice ? ` ${notice}` : ''),
    };
  }

  /** Convenience: apply an override only when the caller actually asked for one. */
  public hasOverride(params: ExecutorOverride): boolean {
    return !!params.targetAgent || !!normalizeModelId(params.model) || !!params.modelRef;
  }
}

/** Read the latest executor switch recorded on a task, if any. */
export function latestRetarget(thread: DelegationThread): TaskRetargetRecord | undefined {
  const list = thread.retargets || [];
  return list.length > 0 ? list[list.length - 1] : undefined;
}
