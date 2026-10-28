import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import {
  DelegationThread,
  DelegationRound,
  MESH_TASK_RECEIVER_ENV,
  MESH_TASK_ROUND_ENV,
  MESH_TASK_THREAD_ENV,
  ReviewWakeState,
  findInFlightRound,
  isValidSessionId,
} from './types';
import { AgentDetector } from './agent-detector';
import { buildDshHeadlessCommand } from './deepseek-harness';
import { TaskRoleLock, TaskRoleLease } from './task-role-lock';
import { CallerWakeQueue, WakeRequest } from './caller-wake-queue';
import type { TaskStore } from './task-store';
import { aggregateThreadTokenUsage, formatTokenCount } from './token-usage-collector';
import { buildReviewWakeState } from './review-wake';
import { detectRateLimitSignal } from './autonomous-runner';

/**
 * How many times one task may wake its caller about failures.
 *
 * Without a cap, a task that keeps dying would ping-pong between caller and
 * receiver forever: every failure wakes the caller, the caller retries, the
 * retry fails again. Three is enough to surface the problem and still let the
 * caller make one or two real attempts.
 */
const MAX_FAILURE_NOTIFICATIONS = 3;

export class CallbackManager extends EventEmitter {
  private detector: AgentDetector;
  private wakeQueue: CallerWakeQueue;
  private taskStore?: TaskStore;

  constructor(detector: AgentDetector, storageDir?: string) {
    super();
    this.detector = detector;
    this.wakeQueue = new CallerWakeQueue(storageDir);
  }

  /**
   * Attach the ledger so a deferred wake can re-read the thread when it is
   * finally delivered (its status may have changed while it waited).
   */
  public setTaskStore(store: TaskStore): void {
    this.taskStore = store;
  }

  /** Rate-limit state shared with the agent nodes / status bar. */
  public getRateLimitTracker() {
    return this.detector.getRateLimitTracker();
  }

  /** Queue behind a caller session that is already processing a wake. */
  private deferWake(callerSessionId: string, request: WakeRequest): false {
    this.wakeQueue.enqueue(callerSessionId, request);
    console.log(
      `[CallbackManager] Caller session ${callerSessionId} is busy; ` +
        `wake for ${request.threadId} (${request.kind}, round ${request.round}) queued behind it.`,
    );
    return false;
  }

  /**
   * Replay the next queued wake for a caller that just became free.
   *
   * The thread is re-read and re-validated: while it waited, the caller may
   * have approved, reworked or deleted it, and replaying a stale wake would wake
   * a reviewer for a task that no longer needs one.
   */
  public drainNextWake(callerSessionId: string): void {
    const next = this.wakeQueue.dequeue(callerSessionId);
    if (!next) return;
    if (!this.taskStore) {
      console.warn(
        `[CallbackManager] Dropping queued wake for ${next.threadId}: no TaskStore attached.`,
      );
      return;
    }
    const thread = this.taskStore.getTask(next.threadId);
    const round = thread?.rounds?.find((r) => r.round === next.round);
    if (!thread || !round) {
      console.warn(
        `[CallbackManager] Dropping queued wake for ${next.threadId} round ${next.round}: it no longer exists.`,
      );
      return;
    }
    if (next.kind === 'review') {
      if (thread.status !== 'waiting_review') {
        console.log(
          `[CallbackManager] Dropping queued review wake for ${next.threadId}: status is now "${thread.status}".`,
        );
        return;
      }
      void this.notifyCaller(thread, round);
      return;
    }
    if (thread.status !== 'failed') {
      console.log(
        `[CallbackManager] Dropping queued failure wake for ${next.threadId}: status is now "${thread.status}".`,
      );
      return;
    }
    void this.notifyCallerOfFailure(thread, next.round);
  }

  /**
   * Dispatches proactive notification callback to caller agent to resume original turn.
   */
  public async notifyCaller(thread: DelegationThread, round: DelegationRound): Promise<boolean> {
    const initiator = thread.initiatorAgent;
    const callerSessionId = this.resolveCallerSession(thread);

    if (!callerSessionId) {
      console.warn(
        `[CallbackManager] Thread ${thread.threadId} completed round ${round.round}, but no valid callerSessionId provided or detected.`,
      );
      return false;
    }

    // The reviewer is a role, not a separate task. It may only run when the
    // receiver has actually handed the report back, and never alongside a
    // developer round or another reviewer.
    const inFlight = findInFlightRound(thread);
    if (
      inFlight ||
      thread.status === 'running' ||
      thread.status === 'reworking' ||
      thread.status === 'queued'
    ) {
      console.warn(
        `[CallbackManager] Refusing to wake reviewer for ${thread.threadId}: ` +
          `receiver round ${inFlight?.round ?? '-'} is still in flight (status: ${thread.status}).`,
      );
      return false;
    }
    if (thread.status !== 'waiting_review') {
      console.warn(
        `[CallbackManager] Refusing to wake reviewer for ${thread.threadId}: ` +
          `task status is "${thread.status}", not "waiting_review".`,
      );
      return false;
    }
    if (thread.reviewBlocked) {
      console.log(
        `[CallbackManager] Thread ${thread.threadId} is parked on ` +
          `${thread.reviewBlocked.waitingForThreadId || 'an external dependency'}; not waking the reviewer.`,
      );
      return false;
    }

    const wakeRequest: WakeRequest = {
      threadId: thread.threadId,
      round: round.round,
      kind: 'review',
      enqueuedAt: Date.now(),
    };
    if (!this.wakeQueue.tryAcquire(callerSessionId)) {
      return this.deferWake(callerSessionId, wakeRequest);
    }

    const roleLock = new TaskRoleLock();
    const roleLease: TaskRoleLease | null = roleLock.tryAcquire(thread.threadId, 'reviewer');
    if (!roleLease) {
      const owner = roleLock.getOwner(thread.threadId);
      this.wakeQueue.release(callerSessionId);
      console.warn(
        `[CallbackManager] Refusing duplicate reviewer for ${thread.threadId}: ` +
          `${roleLock.describe(owner)} still owns the task role.`,
      );
      return false;
    }

    this.emit('taskCompleted', { thread, round });

    const preview = round.output
      ? round.output.slice(0, 200).replace(/[\r\n]+/g, ' ')
      : round.error
        ? `Error: ${round.error.slice(0, 150)}`
        : 'Completed';

    // Callee token line: only when something was actually measured. A missing
    // measurement must not be rendered as "0 tokens".
    const tokenLine = (() => {
      const usage = round.tokenUsage;
      if (!usage || !usage.available) return '';
      const tilde = usage.source === 'estimated' || usage.partial ? '~' : '';
      const cumulative = aggregateThreadTokenUsage(thread.rounds || []);
      const cumulativeTilde = cumulative.isPartialOrEstimated ? '~' : '';
      return (
        `\n📊 Callee Token: Round ${round.round} ${tilde}${formatTokenCount(usage.totalTokens)}` +
        ` (输入 ${formatTokenCount(usage.promptTokens)} · 输出 ${formatTokenCount(usage.outputTokens)} · 缓存 ${formatTokenCount(usage.cacheReadTokens)})` +
        ` · 任务累计 ${cumulativeTilde}${formatTokenCount(cumulative.totalTokens)}`
      );
    })();

    const promptMessage = `[Agent Mesh Bridge 自动化闭环验收指令] 任务【${thread.title}】第 ${round.round} 轮已由 ${thread.targetAgent} 执行完成。\n结果摘要: ${preview}${tokenLine}\n\n本任务已有中间文档链保存在任务账本（历轮指令 / 开发报告 / 返修意见）。get_thread_details(threadId="${thread.threadId}") 默认返回有界视图：最新轮完整，较早轮次可能是「提纯胶囊」卡片；需要逐字核对某一历史轮时用 get_round_raw_log(threadId="${thread.threadId}", round=<轮号>)，不要为此读取整段历史。必须沿用同一 threadId 与当前 callerSessionId 继续，禁止新建任务、另起会话或转派给其他 Agent。\n\n请作为任务下发方立即执行自动化检视闭环（判据只看**本任务自身范围**）：\n1. 审查本任务范围的代码与产出，并在当前工作区实际运行相关测试验证；\n2. 本任务范围内存在编译报错、测试未通过或功能缺陷：立即调用 request_rework(threadId="${thread.threadId}", feedback="具体报错与修复要求")；\n3. 本任务范围已通过、只是同批其他任务/依赖尚未交付：**不要**再发一轮 rework 空转，改用 request_rework(threadId="${thread.threadId}", feedback="阻塞说明", blocked=true, waitingForThreadId="<被依赖的 threadId>") 挂起；桥接不占用轮次，并在依赖到达 completed/failed 时自动唤醒你；\n4. 本任务范围通过即调用 approve_task(threadId="${thread.threadId}", remarks="本任务范围的验收结论与测试结果")。**不要用同批其他任务的 skip/失败否决本任务**——批次门禁属于收口阶段，由你在第 5 步统一处理；\n5. 验收通过后，由你作为调用者统一收口一个 MR：汇总该任务及同一批尚未推送的改动，只创建一个 MR，使用仓库既有远端与 gh/glab/等价 CLI 推送到远端；然后调用 mcp 工具 record_merge_request(threadId="${thread.threadId}", url="MR URL", branch="源分支", commit="提交 SHA", remote="origin", status="synced", summary="MR 摘要") 记录 MR 信息。不要每个子任务各推一次，不要新建任务/会话；开发者(receiver)不负责提交/推送。\n注意：上一轮没有任何文件改动时，本轮若再提交几乎相同的 rework 指令会被桥接拒绝（REWORK_NOOP_LOOP），只能 approve_task 或 blocked=true。\n请注意：请直接调用上述 MCP 工具完成闭环，请勿仅输出文本结论。`;

    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      roleLock.release(roleLease);
      this.wakeQueue.release(callerSessionId);
      this.drainNextWake(callerSessionId);
    };
    const launched = this.launchCaller(
      thread,
      callerSessionId,
      promptMessage,
      round,
      release,
      (childPid) => roleLock.attachChild(roleLease, childPid),
    );
    if (!launched) {
      release();
    }
    return launched;
  }

  /**
   * Wakes the caller when a round failed, so it can retry the round in place
   * instead of discovering the failure by polling the ledger and re-sending the
   * original prompt through `request_rework` (which used to produce two rounds
   * with identical instructions).
   *
   * @param roundNumber - the round that failed.
   * @param result - runner result carrying the failure reason and assessment.
   */
  public async notifyCallerOfFailure(
    thread: DelegationThread,
    roundNumber: number,
    result?: { error?: string; exitCode?: number; assessment?: { errorCode?: string; reason?: string } },
  ): Promise<boolean> {
    if (thread.status !== 'failed') {
      // Paused/stopped tasks are user decisions, not failures to triage.
      return false;
    }

    const alreadyNotified = thread.failureNotifications || [];
    if (alreadyNotified.includes(roundNumber)) {
      console.log(
        `[CallbackManager] Caller already notified about failed round ${roundNumber} of ${thread.threadId}; skipping.`,
      );
      return false;
    }
    if (alreadyNotified.length >= MAX_FAILURE_NOTIFICATIONS) {
      console.warn(
        `[CallbackManager] ${thread.threadId} already used ${alreadyNotified.length} failure notifications ` +
          `(rounds: ${alreadyNotified.join(', ')}); not waking the caller again.`,
      );
      return false;
    }

    const round = (thread.rounds || []).find((r) => r.round === roundNumber);
    if (!round) {
      console.warn(
        `[CallbackManager] Cannot notify failure: round ${roundNumber} is missing from ${thread.threadId}.`,
      );
      return false;
    }

    const callerSessionId = this.resolveCallerSession(thread);
    if (!callerSessionId) {
      console.warn(
        `[CallbackManager] Thread ${thread.threadId} failed round ${roundNumber}, but no valid callerSessionId provided or detected.`,
      );
      return false;
    }

    const reason = (result?.error || round.error || 'unknown error')
      .replace(/[\r\n]+/g, ' ')
      .slice(0, 300);
    const errorCode = result?.assessment?.errorCode || round.assessment?.errorCode || 'FAILED';
    const exitCode = result?.exitCode ?? round.assessment?.exitCode ?? -1;
    const sessionHint = thread.receiverSessionId ? `（原生会话 ${thread.receiverSessionId}）` : '（尚未捕获原生会话）';
    const repeatHint =
      round.repeatOfRound !== undefined
        ? `\n注意：本轮指令与第 ${round.repeatOfRound} 轮完全相同，账本已标记 repeatOfRound=${round.repeatOfRound}；这类重复应当用 retry_task 表达，而不是再发一次原文。`
        : '';
    const stream = round.assessment?.stream;
    const gapHint = stream
      ? ` · gap=${Math.round(stream.maxGapMs)}ms/${stream.gapThresholdMs}ms` +
        (stream.rawMaxGapMs !== undefined && stream.rawMaxGapMs > stream.maxGapMs
          ? `（原始 ${Math.round(stream.rawMaxGapMs)}ms，已扣除主机挂起 ${Math.round(stream.excludedSuspensionMs ?? 0)}ms）`
          : '')
      : '';
    const warningHint = (round.assessment?.warnings ?? [])
      .map((warning) => `\n警告(${warning.code}): ${warning.message}`)
      .join('');

    const promptMessage =
      `[Agent Mesh Bridge 任务失败通知] 任务【${thread.title}】第 ${roundNumber} 轮由 ${thread.targetAgent} 执行时异常结束。\n` +
      `失败原因: ${reason}\n` +
      `响应健康: ${errorCode} · exit=${exitCode} · chunks=${round.assessment?.stream.chunkCount ?? 0} · actualOutput=${round.assessment?.stream.hasActualOutput ?? false}${gapHint}` +
      `${warningHint}\n\n` +
      `请作为任务下发方（caller）立即处理，且必须沿用同一 threadId="${thread.threadId}"，禁止新建任务、另起会话或转派给其他 Agent：\n` +
      `1. 原样重跑这一轮：调用 retry_task(threadId="${thread.threadId}")。它复用同一轮号、同一 prompt、同一原生会话${sessionHint}，不会产生"看起来重复的第二轮"。\n` +
      `2. 接收方需要按具体问题修正：调用 request_rework(threadId="${thread.threadId}", feedback="具体报错与修复要求")。千万不要把原始需求原样贴进 feedback，那会在账本里生成一条与上一轮完全相同的轮次。\n` +
      `3. 确认无法继续：调用 delete_task(threadId="${thread.threadId}", reason="...")。\n` +
      repeatHint +
      `\n请直接调用上述 MCP 工具处理，不要仅输出文本结论。`;

    if (!this.wakeQueue.tryAcquire(callerSessionId)) {
      return this.deferWake(callerSessionId, {
        threadId: thread.threadId,
        round: roundNumber,
        kind: 'failure',
        enqueuedAt: Date.now(),
      });
    }

    console.log(
      `[CallbackManager] Waking caller ${thread.initiatorAgent} about failed round ${roundNumber} of ${thread.threadId}...`,
    );

    let released = false;
    const release = () => {
      if (released) return;
      released = true;
      this.wakeQueue.release(callerSessionId);
      this.drainNextWake(callerSessionId);
    };
    const launched = this.launchCaller(thread, callerSessionId, promptMessage, round, release);
    if (launched) {
      thread.failureNotifications = [...alreadyNotified, roundNumber];
      this.emit('taskFailed', { thread, round, error: reason });
    } else {
      release();
    }
    return launched;
  }

  /**
   * Resolves (and caches on the thread) the caller's native session id, which is
   * what makes a proactive wake-up possible at all.
   */
  private resolveCallerSession(thread: DelegationThread): string | undefined {
    let callerSessionId = thread.callerSessionId;
    const initiator = thread.initiatorAgent;

    if ((!callerSessionId || !isValidSessionId(callerSessionId)) && this.detector) {
      callerSessionId = this.detector.detectActiveSession(initiator, thread.workspacePath, [
        thread.receiverSessionId,
      ]);
      if (callerSessionId) {
        thread.callerSessionId = callerSessionId;
        console.log(
          `[CallbackManager] Auto-detected caller session for ${initiator}: ${callerSessionId}`,
        );
      }
    }

    return callerSessionId && isValidSessionId(callerSessionId) ? callerSessionId : undefined;
  }

  /**
   * Spawns the caller's CLI in its own native session, detached, with bridge
   * receiver markers stripped so the caller keeps full control-plane tools.
   *
   * @param release - called when the child fails to start or exits non-zero, so
   *   the caller can drop whatever lease it took for this wake-up.
   */
  private launchCaller(
    thread: DelegationThread,
    callerSessionId: string,
    promptMessage: string,
    round: DelegationRound,
    release: () => void,
    onChildPid?: (pid: number | undefined) => void,
  ): boolean {
    const initiator = thread.initiatorAgent;
    const cwd = thread.workspacePath || process.cwd();
    const logDir = path.join(os.homedir(), '.agent-mesh-bridge');
    try {
      fs.mkdirSync(logDir, { recursive: true });
    } catch {}
    const logPath = path.join(logDir, 'callbacks.log');
    let outFd: number | 'ignore' = 'ignore';
    try {
      outFd = fs.openSync(logPath, 'a');
      const banner = `\n--- [${new Date().toISOString()}] Waking up ${initiator} (thread: ${thread.threadId}, round: ${round.round}, session: ${callerSessionId}) ---\n`;
      fs.writeSync(outFd, banner);
    } catch (e) {
      console.warn(`[CallbackManager] Could not open callback log:`, e);
    }

    const callbackEnv: NodeJS.ProcessEnv = {
      ...process.env,
      CI: '1',
      NON_INTERACTIVE: 'true',
    };
    // The callback wakes the INITIATOR, which must retain full bridge tools
    // (review/rework/approve). Never inherit a receiver marker from whatever
    // process happened to trigger this callback.
    delete callbackEnv[MESH_TASK_RECEIVER_ENV];
    delete callbackEnv[MESH_TASK_THREAD_ENV];
    delete callbackEnv[MESH_TASK_ROUND_ENV];

    const spawnOpts: child_process.SpawnOptions = {
      cwd,
      detached: true,
      stdio: ['ignore', 'pipe', 'pipe'],
      windowsHide: true,
      env: callbackEnv,
    };

    // The wake only proves the caller process started, never that it reviewed
    // anything: a 429 / exhausted quota kills it before its first tool call.
    // Tee a bounded tail of its output (still written verbatim to
    // callbacks.log) so a failed wake can be classified and retried instead of
    // leaving the task parked in `waiting_review` forever.
    const capture = new WakeOutputCapture();
    const tee = (chunk: Buffer | string) => {
      const text = typeof chunk === 'string' ? chunk : chunk.toString('utf8');
      capture.push(text);
      if (typeof outFd !== 'number') return;
      try {
        fs.writeSync(outFd, text);
      } catch {}
    };

    const attachErrorLogging = (child: child_process.ChildProcess) => {
      // Hand the detached child to the role lock before anything else: it is the
      // process that will call approve/rework, and it must keep holding the
      // reviewer role even if this launcher dies mid-review.
      onChildPid?.(child.pid);
      let settled = false;
      const settle = (outcome: { exitCode?: number; error?: string }) => {
        if (settled) return;
        settled = true;
        release();
        try {
          const state = this.finalizeWake(thread, round, callerSessionId, {
            ...outcome,
            output: capture.text(),
          });
          if (state) {
            console.warn(
              `[CallbackManager] Wake for ${thread.threadId} failed: ${state.lastError}` +
                (state.quotaExhausted ? ' (quota/rate limit)' : '') +
                (state.autoRetryAt ? `; auto retry at ${state.autoRetryAt}` : ''),
            );
          }
        } catch (err) {
          console.error('[CallbackManager] Failed to record wake outcome:', err);
        }
      };

      child.stdout?.on('data', (chunk: Buffer) => tee(chunk));
      child.stderr?.on('data', (chunk: Buffer) => tee(chunk));

      child.on('error', (err) => {
        console.error(`[CallbackManager] Failed to launch ${initiator} callback:`, err);
        try {
          if (typeof outFd === 'number') {
            fs.writeSync(outFd, `Spawn error: ${err.message}\n`);
          }
        } catch {}
        settle({ error: err.message });
      });

      child.on('exit', (code) => {
        if (code !== 0 && code !== null) {
          const errMsg = `Caller process for ${initiator} exited with error code ${code}`;
          console.error(`[CallbackManager] ${errMsg}`);
          try {
            if (typeof outFd === 'number') {
              fs.writeSync(outFd, `\n[Exit] ${errMsg}\n`);
            }
          } catch {}
        }
        settle({ exitCode: code ?? undefined });
      });

      child.unref();
    };

    try {
      if (initiator === 'antigravity') {
        const agyPath =
          this.detector.resolveExecutable('antigravity') ||
          (process.platform === 'win32' ? 'agy.exe' : 'agy');
        // agy.exe --conversation <callerSessionId> -p "<promptMessage>" --dangerously-skip-permissions --mode accept-edits
        const child = child_process.spawn(
          agyPath,
          [
            '--conversation',
            callerSessionId,
            '-p',
            promptMessage,
            '--dangerously-skip-permissions',
            '--mode',
            'accept-edits',
          ],
          spawnOpts,
        );
        attachErrorLogging(child);
        return true;
      }

      if (initiator === 'claude_code') {
        const isWindows = process.platform === 'win32';
        const claudePath =
          this.detector.resolveExecutable('claude_code') || (isWindows ? 'claude.cmd' : 'claude');
        const isExe = claudePath.toLowerCase().endsWith('.exe');
        // The wake prompt is multi-line and `-p` is a boolean flag, so the
        // prompt travels on stdin. As an argv element it would be routed
        // through cmd.exe on Windows (npm shim), which truncates the message at
        // the first newline and the caller would wake with half the checklist.
        const claudeArgs = ['--resume', callerSessionId, '-p', '--dangerously-skip-permissions'];
        const claudeSpawnOpts: child_process.SpawnOptions = {
          ...spawnOpts,
          // stdout/stderr are pumped into the callback log by attachErrorLogging.
          stdio: ['pipe', 'pipe', 'pipe'],
        };
        const child =
          isWindows && !isExe
            ? child_process.spawn('cmd.exe', ['/c', claudePath, ...claudeArgs], claudeSpawnOpts)
            : child_process.spawn(claudePath, claudeArgs, claudeSpawnOpts);
        if (child.stdin) {
          child.stdin.on('error', () => {});
          try {
            child.stdin.write(promptMessage, 'utf8');
          } catch {}
          try {
            child.stdin.end();
          } catch {}
        }
        attachErrorLogging(child);
        return true;
      }

      if (initiator === 'codex') {
        const isWindows = process.platform === 'win32';
        const codexPath =
          this.detector.resolveExecutable('codex') || (isWindows ? 'codex.exe' : 'codex');
        // `--ask-for-approval` is a global option and must precede `exec`.
        // codex --ask-for-approval never exec resume <callerSessionId> "<promptMessage>" --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --skip-git-repo-check
        const child = child_process.spawn(
          codexPath,
          [
            '--ask-for-approval',
            'never',
            'exec',
            'resume',
            callerSessionId,
            promptMessage,
            '--dangerously-bypass-approvals-and-sandbox',
            '--dangerously-bypass-hook-trust',
            '--skip-git-repo-check',
          ],
          spawnOpts,
        );
        attachErrorLogging(child);
        return true;
      }
      if (initiator === 'deepseek_harness') {
        const launcher = this.detector.resolveExecutable('deepseek_harness');
        if (!launcher) {
          release();
          console.error(
            '[CallbackManager] DeepSeek Harness launcher not found; cannot wake caller session.',
          );
          return false;
        }
        // dsh headless --session-id <callerSessionId> "<promptMessage>"
        const built = buildDshHeadlessCommand(launcher, {
          prompt: promptMessage,
          sessionId: callerSessionId,
          json: false,
        });
        if (!built) {
          release();
          console.error(
            `[CallbackManager] Could not build a DeepSeek Harness launch command for ${launcher}.`,
          );
          return false;
        }
        const child = child_process.spawn(built.command, built.args, {
          ...spawnOpts,
          shell: built.useShell,
          env: { ...spawnOpts.env, ...(built.env || {}) },
        });
        attachErrorLogging(child);
        return true;
      }
    } catch (err) {
      release();
      console.error(`[CallbackManager] Failed to wake up caller ${initiator}:`, err);
      return false;
    }

    release();
    return false;
  }

  /**
   * Record the outcome of one reviewer wake-up attempt.
   *
   * "Success" requires the caller process to exit cleanly AND print no
   * quota/rate-limit signal: several CLIs exit 0 after printing "429" or
   * "insufficient quota", and treating that as a delivered review is exactly how
   * a task ends up parked in `waiting_review` with nobody looking at it.
   *
   * On failure the state is written to the thread so the tree/detail panel can
   * show "检视唤醒失败" with a one-click re-review, and so the scheduler knows
   * when to try again by itself (after the provider's reset time, when known).
   *
   * @returns the recorded failure, or `undefined` when the wake looked clean.
   */
  public finalizeWake(
    thread: DelegationThread,
    round: DelegationRound,
    callerSessionId: string,
    outcome: { exitCode?: number; output?: string; error?: string },
  ): ReviewWakeState | undefined {
    const output = outcome.output || '';
    const signal = detectRateLimitSignal(output);
    const clean = !outcome.error && outcome.exitCode === 0 && !signal;

    // Re-read the ledger: the review may have been decided by another reviewer
    // process while this one was dying, and a stale copy would resurrect it.
    let target = thread;
    if (this.taskStore) {
      try {
        this.taskStore.loadFromDisk();
        target = this.taskStore.getTask(thread.threadId) || thread;
      } catch {}
    }

    if (clean) {
      if (!target.reviewWake) return undefined;
      target.reviewWake = undefined;
      target.updatedAt = new Date().toISOString();
      try {
        this.taskStore?.upsertTask(target);
      } catch {}
      return undefined;
    }

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
      error: outcome.error,
      exitCode: outcome.exitCode,
      // Attempts accumulate only within the same round: a rework starts a fresh
      // review and must get its own budget.
      previous: target.reviewWake?.round === round.round ? target.reviewWake : undefined,
      quota,
    });
    target.reviewWake = state;
    target.updatedAt = new Date().toISOString();
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

/**
 * Bounded tail of a wake child's output.
 *
 * A reviewer turn can print megabytes; only the last few KB matter for
 * classifying the failure, and holding all of it would blow up the extension
 * host for long sessions.
 */
class WakeOutputCapture {
  private static readonly MAX_CHARS = 16_384;
  private buffer = '';

  public push(chunk: Buffer | string): void {
    this.buffer += chunk.toString();
    if (this.buffer.length > WakeOutputCapture.MAX_CHARS) {
      this.buffer = this.buffer.slice(-WakeOutputCapture.MAX_CHARS);
    }
  }

  public text(): string {
    return this.buffer;
  }
}

