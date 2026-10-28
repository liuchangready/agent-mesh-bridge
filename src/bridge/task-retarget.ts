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
import {
  AgentId,
  DelegationThread,
  ExecutorOverride,
  RetargetTaskResult,
  TaskRetargetRecord,
  describeRoundInFlight,
  findInFlightRound,
  getModelRouting,
} from './types';
import { TaskStore } from './task-store';
import {
  ModelApprovalHandler,
  resolveDispatchModel,
} from './model-approval';
import { normalizeModelId } from './model-catalog';

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
    const thread = this.taskStore.getTask(params.threadId);
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

    const previousAgent = thread.targetAgent;
    const previousModel = thread.targetModel;
    const nextAgent: AgentId = params.targetAgent || previousAgent;
    const agentChanged = nextAgent !== previousAgent;
    const requested = normalizeModelId(params.model);

    // Resolve the model BEFORE touching the thread: a declined switch must leave
    // the task exactly as it was.
    const resolution = await resolveDispatchModel({
      targetAgent: nextAgent,
      requestedModel: requested,
      requestedByUser: params.modelRequestedByUser === true,
      initiatorAgent: thread.initiatorAgent,
      title: thread.title,
      handler: this.modelApprovalHandler,
    });

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

    const modelChanged = nextModel !== previousModel;
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
    return !!params.targetAgent || !!normalizeModelId(params.model);
  }
}

/** Read the latest executor switch recorded on a task, if any. */
export function latestRetarget(thread: DelegationThread): TaskRetargetRecord | undefined {
  const list = thread.retargets || [];
  return list.length > 0 ? list[list.length - 1] : undefined;
}
