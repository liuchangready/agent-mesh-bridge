import { assertDedicatedTask } from './types';
import { getTaskExecutionPath, isReviewerWakeEligible, isMergeReview } from './types';
import type { ChildProcess } from 'child_process';
import * as path from 'path';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import {
  DelegationThread,
  DelegationRound,
  ReviewWakeState,
  ResponseAssessment,
  TaskInterruption,
  isValidSessionId,
  appendInterruption,
} from './types';
import { AgentDetector } from './agent-detector';
import type { TaskStore } from './task-store';
import { aggregateThreadTokenUsage, formatTokenCount } from './token-usage-collector';
import { applyReviewFailure, buildReviewWakeState } from './review-wake';
import { detectRateLimitSignal } from './autonomous-runner';
import { createReviewerLogWriter } from './reviewer-log';
import { assertCanExecute } from './agent-capabilities';
import { AutonomousRunner } from './autonomous-runner';
import type { NativeRunPreparationContext } from './native-session-compactor';
import { nativeSessionNamespace, SessionOwnershipStore } from './session-ownership';
import { noteMergeFailure } from './merge-recovery';
import { ExecutionController } from './execution-controller';
import {
  buildReviewerBootstrapPrompt,
  clearReviewerBootstrapToken,
  readReviewerBootstrapToken,
  recordReviewerBootstrapHostPid,
} from './reviewer-bootstrap';

export class CallbackManager extends EventEmitter {
  private detector: AgentDetector;
  private runner?: AutonomousRunner;
  private taskStore?: TaskStore;
  private readonly storageDir?: string;

  constructor(detector: AgentDetector, storageDir?: string) {
    super();
    this.detector = detector;
    this.storageDir = storageDir;
  }

  /**
   * Attach the ledger so a deferred wake can re-read the thread when it is
   * finally delivered (its status may have changed while it waited).
   */
  public setTaskStore(store: TaskStore): void {
    this.taskStore = store;
  }

  public setAutonomousRunner(runner: AutonomousRunner): void { this.runner = runner; }

  /** Rate-limit state shared with the agent nodes / status bar. */
  public getRateLimitTracker() {
    return this.detector.getRateLimitTracker();
  }

  public async notifyReviewer(thread: DelegationThread, round: DelegationRound): Promise<boolean> {
    if (thread.execution?.version !== 2 || thread.reviewFlow !== 'dedicated') return false;
    return await ExecutionController.getInstance()?.wakeReviewerV2(thread.threadId, round.round) ?? false;
  }
  private resolveReviewerSession(thread: DelegationThread): string | undefined {
    if (thread.reviewFlow !== 'dedicated' ||
        thread.reviewBootstrap?.phase !== 'confirmed' ||
        thread.reviewerSessionCaptured !== true ||
        !isValidSessionId(thread.reviewerSessionId) ||
        thread.callerSessionId !== thread.reviewerSessionId) return undefined;
    return thread.reviewerSessionId;
  }

  /** Build the report for the captured task reviewer. */
  public buildReviewWakeMessage(thread: DelegationThread, reviewRound: DelegationRound): string {
    const preview = reviewRound.output
      ? reviewRound.output.slice(0, 200).replace(/[\r\n]+/g, ' ')
      : reviewRound.error
        ? `Error: ${reviewRound.error.slice(0, 150)}`
        : 'Completed';

    const tokenLine = (() => {
      const usage = reviewRound.tokenUsage;
      if (!usage || !usage.available) return '';
      const tilde = usage.source === 'estimated' || usage.partial ? '~' : '';
      const cumulative = aggregateThreadTokenUsage(thread.rounds || []);
      const cumulativeTilde = cumulative.isPartialOrEstimated ? '~' : '';
      return (
        `\n📊 Callee Token: Round ${reviewRound.round} ${tilde}${formatTokenCount(usage.totalTokens)}` +
        ` (输入 ${formatTokenCount(usage.promptTokens)} · 输出 ${formatTokenCount(usage.outputTokens)} · 缓存 ${formatTokenCount(usage.cacheReadTokens)})` +
        ` · 任务累计 ${cumulativeTilde}${formatTokenCount(cumulative.totalTokens)}`
      );
    })();

    const promptMessage = `[Agent Mesh Bridge 自动化闭环验收指令] 任务【${thread.title}】第 ${reviewRound.round} 轮已由 ${thread.targetAgent} 执行完成。\n结果摘要: ${preview}${tokenLine}\n\n本任务已有中间文档链保存在任务账本（历轮指令 / 开发报告 / 返修意见）。get_thread_details(threadId="${thread.threadId}") 默认返回有界视图：最新轮完整，较早轮次可能是「提纯胶囊」卡片；需要逐字核对某一历史轮时用 get_round_raw_log(threadId="${thread.threadId}", round=<轮号>)，不要为此读取整段历史。必须沿用同一 threadId 与当前 callerSessionId 继续，禁止新建任务、另起会话或转派给其他 Agent。\n\n请作为任务下发方立即执行自动化检视闭环（判据只看**本任务自身范围**）：\n1. 审查本任务范围的代码与产出，并在当前工作区实际运行相关测试验证；\n2. 本任务范围内存在编译报错、测试未通过或功能缺陷：立即调用 request_rework(threadId="${thread.threadId}", feedback="具体报错与修复要求")；\n3. 本任务范围已通过、只是同批其他任务/依赖尚未交付：**不要**再发一轮 rework 空转，改用 request_rework(threadId="${thread.threadId}", feedback="阻塞说明", blocked=true, waitingForThreadId="<被依赖的 threadId>") 挂起；桥接不占用轮次，并在依赖到达 completed/failed 时自动唤醒你；\n4. 本任务范围通过即调用 approve_task(threadId="${thread.threadId}", remarks="本任务范围的验收结论与测试结果")。**不要用同批其他任务的 skip/失败否决本任务**——批次门禁属于收口阶段，由你在第 5 步统一处理；\n5. 验收通过后，由你作为调用者统一收口一个 MR：汇总该任务及同一批尚未推送的改动，只创建一个 MR，使用仓库既有远端与 gh/glab/等价 CLI 推送到远端；然后调用 mcp 工具 record_merge_request(threadId="${thread.threadId}", url="MR URL", branch="源分支", commit="提交 SHA", remote="origin", status="synced", summary="MR 摘要") 记录 MR 信息。不要每个子任务各推一次，不要新建任务/会话；开发者(receiver)不负责提交/推送。\n注意：上一轮没有任何文件改动时，本轮若再提交几乎相同的 rework 指令会被桥接拒绝（REWORK_NOOP_LOOP），只能 approve_task 或 blocked=true。\n请注意：请直接调用上述 MCP 工具完成闭环，请勿仅输出文本结论。`;

    const wakeMessage = isMergeReview(thread)
      ? `The approved task could not be integrated: ${thread.worktree?.integration?.error || 'unknown error'}. Resolution workspace: ${thread.worktree?.mergeResolutionPath || 'not available'}. Worker assessment: ${JSON.stringify(reviewRound.assessment || null)}. Interruptions: ${JSON.stringify(thread.interruptions || [])}. Recent merge decisions and recovery: ${JSON.stringify((thread.mergeHistory || []).slice(-8))}. Inspect get_thread_details, get_merge_log and get_round_raw_log for task ${thread.threadId}. Preserve the existing thread and reviewer session; use standard Git to resolve and stage changes, then call submit_merge_resolution. Request rework only if the candidate itself needs changes; request_merge_decision when a user decision is needed. Treat STREAM_GAP_EXCEEDED as a warning, defer quota failures, and investigate authentication, permission, CLI startup, environment, Git and validation failures.`
      : promptMessage;
    const answer = [...(thread.mergeHistory || [])].reverse().find(entry => entry.phase === 'decision' && entry.result === 'answered');
    const question = answer && (thread.mergeHistory || []).find(entry => entry.decisionId === answer.decisionId && entry.result === 'requested');
    const decisionMessage = answer ? `\n\nUser decision for ${question?.question || 'the pending question'}: ${answer.answer || ''}` : '';
    return (thread.taskTemplate ? `${wakeMessage}\n\n[Task template reviewer instructions]\n${thread.taskTemplate.reviewerInstructions}` : wakeMessage) + decisionMessage;
  }

  public async launchReviewerBootstrap(
    thread: DelegationThread,
    onChildPid?: (pid: number | undefined, childProcess?: ChildProcess, childBirthId?: string) => void | Promise<void>,
    executionIdentity?: NativeRunPreparationContext,
  ): Promise<{ confirmed: boolean; error?: string; transient?: boolean }> {
    assertDedicatedTask(thread);
    assertCanExecute(thread.initiatorAgent, 'bootstrap as a reviewer', 'initiator');
    const control = thread.execution;
    const identity = executionIdentity;
    if (control?.version !== 2 || !identity || identity.role !== 'reviewer' || identity.phase !== 'review' ||
        control.run?.id !== identity.runId || control.operationId !== identity.operationId ||
        control.generation !== identity.generation || control.round !== identity.round) {
      return { confirmed: false, error: 'V2 reviewer bootstrap has no matching durable run identity.' };
    }
    const store = this.taskStore;
    const root = this.storageDir || (store ? path.dirname(store.getStoragePath()) : undefined);
    const tokenRecord = root ? readReviewerBootstrapToken(thread.threadId, root, thread.artifactDir) : undefined;
    if (!tokenRecord) {
      return { confirmed: false, error: 'Reviewer bootstrap token is missing.' };
    }

    const resumeReviewerSession = thread.reviewerSessionCaptured === true && !!thread.reviewerSessionId;
    const preassigned = resumeReviewerSession ? thread.reviewerSessionId! : thread.reviewerSessionId || crypto.randomUUID();
    const prompt = buildReviewerBootstrapPrompt(thread, tokenRecord.token);
    const runner = this.runner || new AutonomousRunner(this.detector);
    const reviewerLog = createReviewerLogWriter(thread.threadId, 0, {
      agent: thread.initiatorAgent,
      sessionId: resumeReviewerSession ? preassigned : undefined,
      kind: 'bootstrap',
      note: `senderModel=${thread.reviewerModel || 'agent default'}`,
    }, { root, artifactDir: thread.artifactDir });

    const result = await runner.execute({
      agent: thread.initiatorAgent,
      prompt,
      isRework: resumeReviewerSession,
      receiverSessionId: preassigned,
      title: `Reviewer bootstrap: ${thread.title}`,
      cwd: getTaskExecutionPath(thread),
      taskThreadId: thread.threadId,
      artifactDir: thread.artifactDir,
      taskRound: 0,
      nativeSessionRole: 'reviewer',
      model: thread.reviewerModel,
      role: 'bootstrap',
      executionIdentity: identity,
      markAsReceiver: false,
      maxRetries: 0,
      extraEnv: {
        MESH_REVIEW_BOOTSTRAP: '1',
        MESH_REVIEW_BOOTSTRAP_TOKEN: tokenRecord.token,
        MESH_REVIEW_THREAD_ID: thread.threadId,
        MESH_CALLER_AGENT: thread.initiatorAgent,
      },
      onChildPid: async (pid, childProc, childBirthId) => {
        if (pid && root) recordReviewerBootstrapHostPid(thread.threadId, pid, root, thread.artifactDir);
        // The undefined notification is the runner's exit confirmation. Forward
        // it so v2 can clear the child binding and consume its durable handoff.
        await onChildPid?.(pid, childProc, childBirthId);
      },
    }).then((result) => {
      // Redact after stream assembly so a credential split across chunks is also removed.
      const redact = (text: string) => text.replaceAll(tokenRecord.token, '[redacted]');
      if (result.stdout) reviewerLog.write(redact(result.stdout));
      if (result.stderr) reviewerLog.write(`\n[stderr]\n${redact(result.stderr)}`);
      reviewerLog.writeOutcome({ exitCode: result.exitCode, error: result.error && redact(result.error) });
      return result;
    }).finally(() => reviewerLog.close());

    if (store) store.loadFromDisk();
    const fresh = store?.getTask(thread.threadId);
    if (fresh?.reviewBootstrap?.phase === 'confirmed') {
      const sessionId =
        result.actualSessionId ||
        (thread.initiatorAgent === 'claude_code' ? preassigned : undefined) ||
        (resumeReviewerSession ? preassigned : undefined);
      if (sessionId && store) {
        if (fresh.execution?.version !== 2 || fresh.execution.run?.id !== identity.runId ||
            fresh.execution.operationId !== identity.operationId || fresh.execution.generation !== identity.generation) {
          return { confirmed: false, error: 'Reviewer bootstrap run changed before session binding was persisted.' };
        }
        const assignment = fresh.nativeSessionAssignments?.reviewer;
        if (!assignment) return { confirmed: false, error: 'SESSION_OWNERSHIP_UNVERIFIED: reviewer namespace was not pinned.' };
        if (assignment.namespace !== nativeSessionNamespace(fresh.initiatorAgent)) {
          return { confirmed: false, error: 'SESSION_OWNERSHIP_UNVERIFIED: pinned reviewer namespace does not match the CLI storage root.' };
        }
        const ownershipStore = new SessionOwnershipStore(root);
        const ownershipInput = {
          agent: fresh.initiatorAgent,
          namespace: assignment.namespace,
          sessionId,
          threadId: fresh.threadId,
          role: 'reviewer',
          sessionEpoch: assignment.sessionEpoch,
        } as const;
        const ownership = fresh.reviewerSessionCaptured === true
          ? ownershipStore.verify(ownershipInput)
          : ownershipStore.claim(ownershipInput);
        if (!ownership.ok) return { confirmed: false, error: `${ownership.code}: ${ownership.reason}` };
        await store.transitionExecution(fresh.threadId, fresh.execution.revision, (current) => {
          if (current.execution?.run?.id !== fresh.execution?.run?.id || current.cleanup) {
            throw new Error('Reviewer bootstrap run changed before session binding was persisted.');
          }
          current.reviewerSessionId = sessionId;
          current.reviewerSessionCaptured = true;
          current.callerSessionId = sessionId;
        });
        if (root) clearReviewerBootstrapToken(fresh.threadId, root, fresh.artifactDir);
      }
      return { confirmed: true };
    }

    const detail = result.error || `Reviewer process exited with code ${result.exitCode} without confirming bootstrap`;
    const isTransient = runner.isTransientError(result);
    return { confirmed: false, error: detail, transient: isTransient };
  }

  public launchReviewer(
    thread: DelegationThread,
    round: DelegationRound,
    onChildPid?: (pid: number | undefined, childProcess?: ChildProcess, childBirthId?: string) => void | Promise<void>,
    executionIdentity?: NativeRunPreparationContext,
  ): Promise<{ success: boolean; error?: string; exitCode?: number; output?: string; assessment?: ResponseAssessment }> {
    assertDedicatedTask(thread);
    if (thread.execution?.version !== 2) {
      return Promise.resolve({ success: false, error: 'Historical tasks cannot start a reviewer.' , exitCode: -1 });
    }
    assertCanExecute(thread.initiatorAgent, 'resume as a reviewer', 'initiator');
    const reviewerSessionId = this.resolveReviewerSession(thread);
    if (!reviewerSessionId) {
      return Promise.resolve({ success: false, error: `No reviewer session ID found for task ${thread.threadId}` });
    }

    const initiator = thread.initiatorAgent;
    const cwd = isMergeReview(thread) && thread.worktree?.mergeResolutionPath || getTaskExecutionPath(thread);
    const senderModel = thread.reviewerModel;

    const wakeMessage = this.buildReviewWakeMessage(thread, round);

    const identity = executionIdentity;
    if (!identity || identity.role !== 'reviewer' || identity.phase !== 'review' ||
        identity.runId !== thread.execution.run?.id || identity.operationId !== thread.execution.operationId ||
        identity.generation !== thread.execution.generation || identity.round !== thread.execution.round) {
      return Promise.resolve({ success: false, error: 'V2 reviewer wake has no matching durable run identity.', exitCode: -1 });
    }
    const runner = this.runner || new AutonomousRunner(this.detector, undefined, undefined, this.taskStore);
    const reviewerLog = createReviewerLogWriter(thread.threadId, round.round, {
      agent: initiator,
      sessionId: reviewerSessionId,
      kind: 'review',
      note: `${thread.title} | receiver=${thread.targetAgent} | roundType=${round.type} | senderModel=${senderModel || 'agent default'}`,
    }, { root: this.storageDir, artifactDir: thread.artifactDir });
    return runner.execute({
      agent: initiator,
      prompt: wakeMessage,
      isRework: true,
      receiverSessionId: reviewerSessionId,
      title: `Review: ${thread.title}`,
      cwd,
      taskThreadId: thread.threadId,
      artifactDir: thread.artifactDir,
      taskRound: round.round,
      nativeSessionRole: 'reviewer',
      model: senderModel,
      role: 'reviewer',
      executionIdentity: { ...identity },
      markAsReceiver: false,
      maxRetries: 0,
      extraEnv: {
        MESH_REVIEW_SESSION_ID: reviewerSessionId,
        MESH_REVIEW_THREAD_ID: thread.threadId,
        MESH_CALLER_AGENT: initiator,
      },
      preSpawnGuard: () => {
        this.taskStore?.loadFromDisk();
        const fresh = this.taskStore?.getTask(thread.threadId);
        const current = fresh?.execution;
        if (!fresh || fresh.cleanup || fresh.status !== 'reviewing' || current?.version !== 2 ||
            current.run?.id !== identity.runId || current.operationId !== identity.operationId ||
            current.generation !== identity.generation || current.phase !== 'review' || current.round !== identity.round ||
            current.run.state === 'stopping' || current.run.unconfirmed) {
          return { ok: false, reason: 'V2 reviewer run or task state changed before spawn.' };
        }
        return { ok: true };
      },
      onStdoutChunk: (chunk) => reviewerLog.write(chunk),
      onStderrChunk: (chunk) => reviewerLog.write(chunk),
      onChildPid,
    }).then((result) => {
      reviewerLog.writeOutcome(result);
      return {
        success: result.exitCode === 0 && result.assessment?.success === true,
        exitCode: result.exitCode,
        error: result.error,
        output: result.stdout,
        assessment: result.assessment,
      };
    }).finally(() => reviewerLog.close());
  }
  public finalizeWake(
    thread: DelegationThread,
    round: DelegationRound,
    callerSessionId: string,
    outcome: { exitCode?: number; output?: string; error?: string; assessment?: ResponseAssessment; interruption?: TaskInterruption; attemptId?: string },
    wakeKind: 'review' | 'failure' = 'review',
  ): ReviewWakeState | undefined {
    const output = outcome.output || '';
    const signal = detectRateLimitSignal(output);
    const clean = !outcome.error && outcome.exitCode === 0 && !signal && (!outcome.assessment || outcome.assessment.success);

    // Failure notifications are informational; their exit status is not a
    // review verdict and must not create or clear the review retry plan.
    if (wakeKind === 'failure') return undefined;

    // Re-read the authoritative ledger: a tool call may have handed off to a
    // rework, parked the review, or approved the task while this process exits.
    let target = thread;
    if (this.taskStore) {
      try {
        this.taskStore.loadFromDisk();
        const current = this.taskStore.getTask(thread.threadId);
        // Deleted and archived tasks must never be resurrected from this
        // callback's stale in-memory copy.
        if (!current || current.archivedAt || (current.failureRevision || 0) !== (thread.failureRevision || 0)) return undefined;
        target = current;
      } catch {
        return undefined;
      }
    }

    const mergeWake = isMergeReview(thread) && isReviewerWakeEligible(thread);
    if (mergeWake && target.status === 'wait_decision' && outcome.assessment && !outcome.assessment.success) {
      const reason = outcome.assessment.reason || outcome.assessment.errorCode;
      if (outcome.interruption) appendInterruption(target, outcome.interruption);
      const budget = noteMergeFailure(target, 'reviewer', outcome.assessment.errorCode, reason);
      target.mergeHistory = [...(target.mergeHistory || []), { id: crypto.randomUUID(), number: (target.mergeHistory?.length || 0) + 1,
        at: new Date().toISOString(), phase: 'reviewer', result: 'failed', error: reason, attemptId: outcome.attemptId, assessment: outcome.assessment, interruption: outcome.interruption, ...budget }];
      target.updatedAt = new Date().toISOString(); this.taskStore?.upsertTask(target);
      return undefined;
    }
    if (mergeWake && target.status === 'queued' && target.queuePhase === 'merge' && outcome.assessment && !outcome.assessment.success && target.worktree?.integration) {
      const priorAt = Date.parse(target.worktree.integration.updatedAt || '') || 0;
      target.status = 'reviewing';
      target.queuePhase = undefined;
      target.worktree.integration = { ...target.worktree.integration, phase: 'blocked', error: `Reviewer ${outcome.assessment.errorCode}: ${outcome.assessment.reason || 'response failed'}`, updatedAt: new Date(Math.max(Date.now(), priorAt + 1)).toISOString() };
    }
    if (mergeWake && outcome.attemptId && (!isMergeReview(target) || !isReviewerWakeEligible(target))) {
      const result = target.status === 'paused' ? 'cancelled' : 'completed';
      target.mergeHistory = [...(target.mergeHistory || []), { id: crypto.randomUUID(), attemptId: outcome.attemptId,
        number: (target.mergeHistory?.length || 0) + 1, at: new Date().toISOString(), phase: 'reviewer', result,
        assessment: outcome.assessment, interruption: outcome.interruption }];
      this.taskStore?.upsertTask(target);
    }
    if (target.status === 'completed' || (isMergeReview(target) && (!mergeWake || !isReviewerWakeEligible(target)))) {
      if (target.reviewWake?.round === round.round) {
        target.reviewWake = undefined;
        target.updatedAt = new Date().toISOString();
        try {
          this.taskStore?.upsertTask(target);
        } catch {}
      }
      return undefined;
    }

    // Paused/failed/deleted/review_failed tasks no longer own a review slot.
    if (target.status !== 'reviewing' && !(mergeWake && isReviewerWakeEligible(target))) return undefined;

    const handedOff =
      (target.rounds || []).some((candidate) => candidate.round > round.round && candidate.type !== 'approval') ||
      !!target.reviewBlocked;
    if (handedOff) {
      // Clear only this round's plan. A late callback must not erase a newer
      // round's retry state.
      if (target.reviewWake?.round === round.round) {
        target.reviewWake = undefined;
        target.updatedAt = new Date().toISOString();
        try {
          this.taskStore?.upsertTask(target);
        } catch {}
      }
      return undefined;
    }

    // A callback for a prior round cannot write a retry plan for the new one.
    if (!mergeWake && target.currentRound !== round.round) return undefined;

    const failure = clean
      ? 'REVIEW_NO_DECISION: reviewer exited successfully without a recorded approve_task, request_rework, or review block'
      : outcome.error || outcome.assessment?.reason;

    const tracker = this.detector.getRateLimitTracker();
    let quota = signal ? tracker.recordExhaustion(thread.initiatorAgent, output) : undefined;
    if (!quota) {
      // The wake may fail with a generic transport error while the agent is
      // already cooling down; keep the known reset window rather than backing off
      // blindly and burning a retry before the quota is back.
      const active = tracker.getRateLimitInfo(thread.initiatorAgent);
      if (active?.isExhausted && active.resetsAt) quota = active;
    }

    const state = buildReviewWakeState({
      round: round.round,
      agent: thread.initiatorAgent,
      output,
      error: failure,
      exitCode: outcome.exitCode,
      // Attempts accumulate only within the same round: a rework starts a fresh
      // review and must get its own budget.
      previous: target.reviewWake?.round === round.round ? (mergeWake ? { ...target.reviewWake, autoRetryAttempts: 0 } : target.reviewWake) : undefined,
      quota,
    });
    target.reviewWake = state;
    target.updatedAt = new Date().toISOString();
    if (mergeWake) {
      if (outcome.interruption) appendInterruption(target, outcome.interruption);
      const reason = state.lastError;
      const code = signal ? 'RATE_LIMITED' : /auth|credential|permission|denied|eacces/i.test(reason) ? 'AUTH_OR_PERMISSION' :
        /spawn.*(?:enoent|not found)|failed to launch/i.test(outcome.error || '') ? 'CLI_START_FAILED' :
          (outcome.assessment?.errorCode || (outcome.exitCode === undefined ? 'CLI_START_FAILED' : `EXIT_${outcome.exitCode}`));
      const budget = noteMergeFailure(target, 'reviewer', code, reason);
      if ((target.status === 'failed' && target.failure?.stage === 'merge') || code === 'AUTH_OR_PERMISSION') { state.autoRetryAt = undefined; state.gaveUp = true; }
      const number = (target.mergeHistory?.length || 0) + 1;
      target.mergeHistory = [...(target.mergeHistory || []), {
        id: crypto.randomUUID(), number, at: target.updatedAt, phase: 'reviewer', result: 'failed', attemptId: outcome.attemptId, error: state.lastError, errorCode: code, exitCode: outcome.exitCode, assessment: outcome.assessment, interruption: outcome.interruption, ...budget,
      }];
      if (code === 'AUTH_OR_PERMISSION' && isMergeReview(target)) {
        const decisionId = crypto.randomUUID();
        target.mergeHistory.push({ id: decisionId, decisionId, number: number + 1, at: target.updatedAt, phase: 'decision', result: 'requested',
          decisionStage: 'merge', question: 'How should the reviewer proceed after the authentication or permission failure?', background: reason,
          suggestion: 'Restore the required credentials or permissions, then submit a decision.' });
        target.mergeControl = { ...target.mergeControl!, revision: target.mergeControl!.revision + 1, pendingDecisionId: decisionId, decisionStage: 'merge' };
        target.status = 'wait_decision';
      }
    } else if (state.gaveUp) {
      // The retry budget is spent: no reviewer will ever be woken again on its
      // own. Surface that as a terminal `review_failed` (releasing the queue
      // slot) with the concrete cause, rather than a silent `reviewing`.
      applyReviewFailure(target, {
        kind: 'retries_exhausted',
        reason: `检视唤醒自动重试已耗尽（${state.attempts} 次失败）：${state.lastError}`,
        round: round.round,
        by: 'callback',
        wake: state,
      });
    }
    try {
      this.taskStore?.upsertTask(target);
    } catch {}

    this.emit('callbackFailed', {
      thread: target,
      round,
      error: state.lastError,
      callerSessionId,
      exitCode: state.exitCode,
      quotaExhausted: state.quotaExhausted === true,
      quotaResetsAt: state.quotaResetsAt,
      quotaResetsAtFormatted: state.quotaResetsAtFormatted,
      quotaReason: state.quotaReason,
      autoRetryAt: state.autoRetryAt,
      gaveUp: state.gaveUp === true,
    });
    return state;
  }
}
