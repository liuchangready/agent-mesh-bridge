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
  findInFlightRound,
  isValidSessionId,
} from './types';
import { AgentDetector } from './agent-detector';
import { buildDshHeadlessCommand } from './deepseek-harness';
import { TaskRoleLock, TaskRoleLease } from './task-role-lock';

export class CallbackManager extends EventEmitter {
  private detector: AgentDetector;

  constructor(detector: AgentDetector) {
    super();
    this.detector = detector;
  }

  /**
   * Dispatches proactive notification callback to caller agent to resume original turn.
   */
  public async notifyCaller(thread: DelegationThread, round: DelegationRound): Promise<boolean> {
    let callerSessionId = thread.callerSessionId;
    const initiator = thread.initiatorAgent;

    // Try auto-detecting callerSessionId if not already set or if invalid
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

    if (!callerSessionId || !isValidSessionId(callerSessionId)) {
      console.warn(
        `[CallbackManager] Thread ${thread.threadId} completed round ${round.round}, but no valid callerSessionId provided or detected (got: "${callerSessionId}").`,
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

    const roleLock = new TaskRoleLock();
    const roleLease: TaskRoleLease | null = roleLock.tryAcquire(thread.threadId, 'reviewer');
    if (!roleLease) {
      const owner = roleLock.getOwner(thread.threadId);
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

    const promptMessage = `[Agent Mesh Bridge 自动化闭环验收指令] 任务【${thread.title}】第 ${round.round} 轮已由 ${thread.targetAgent} 执行完成。\n结果摘要: ${preview}\n\n本任务已有中间文档链保存在任务账本（历轮指令 / 开发报告 / 返修意见）。如需完整内容请调用 get_thread_details(threadId="${thread.threadId}")；必须沿用同一 threadId 与当前 callerSessionId 继续，禁止新建任务、另起会话或转派给其他 Agent。\n\n请作为任务下发方立即执行自动化检视闭环：\n1. 审查代码与产出，并在当前工作区运行相关测试（如 gradlew/npm test）进行验证；\n2. 若存在编译报错、测试未通过或功能缺陷：请务必立即直接调用 mcp 工具 request_rework(threadId="${thread.threadId}", feedback="具体报错与修复要求") 提交返修；\n3. 若测试全部通过且功能确认无误：请务必立即直接调用 mcp 工具 approve_task(threadId="${thread.threadId}", remarks="验收结论与测试结果") 归档任务。\n4. 验收通过后，由你作为调用者统一收口一个 MR：汇总该任务及同一批尚未推送的改动，只创建一个 MR，使用仓库既有远端与 gh/glab/等价 CLI 推送到远端；然后调用 mcp 工具 record_merge_request(threadId="${thread.threadId}", url="MR URL", branch="源分支", commit="提交 SHA", remote="origin", status="synced", summary="MR 摘要") 记录 MR 信息。不要每个子任务各推一次，不要新建任务/会话；开发者(receiver)不负责提交/推送。\n请注意：请直接调用上述 MCP 工具完成闭环，请勿仅输出文本结论。`;

    console.log(`[CallbackManager] Waking up caller ${initiator} (session: ${callerSessionId})...`);

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
      stdio: ['ignore', outFd, outFd],
      windowsHide: true,
      env: callbackEnv,
    };

    const attachErrorLogging = (child: child_process.ChildProcess) => {
      child.on('error', (err) => {
        roleLock.release(roleLease);
        console.error(`[CallbackManager] Failed to launch ${initiator} callback:`, err);
        try {
          if (typeof outFd === 'number') {
            fs.writeSync(outFd, `Spawn error: ${err.message}\n`);
          }
        } catch {}
        this.emit('callbackFailed', { thread, round, error: err.message });
      });

      child.on('exit', (code) => {
        roleLock.release(roleLease);
        if (code !== 0 && code !== null) {
          const errMsg = `Caller process for ${initiator} exited with error code ${code}`;
          console.error(`[CallbackManager] ${errMsg}`);
          try {
            if (typeof outFd === 'number') {
              fs.writeSync(outFd, `\n[Exit] ${errMsg}\n`);
            }
          } catch {}
          this.emit('callbackFailed', { thread, round, error: errMsg });
        }
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
        const child = (isWindows && !isExe)
          ? child_process.spawn(
              'cmd.exe',
              [
                '/c',
                claudePath,
                '--resume',
                callerSessionId,
                '-p',
                promptMessage,
                '--dangerously-skip-permissions',
              ],
              spawnOpts,
            )
          : child_process.spawn(
              claudePath,
              ['--resume', callerSessionId, '-p', promptMessage, '--dangerously-skip-permissions'],
              spawnOpts,
            );
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
          roleLock.release(roleLease);
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
          roleLock.release(roleLease);
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
      roleLock.release(roleLease);
      console.error(`[CallbackManager] Failed to wake up caller ${initiator}:`, err);
      return false;
    }

    roleLock.release(roleLease);
    return false;
  }
}
