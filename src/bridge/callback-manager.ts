import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { EventEmitter } from 'events';
import { DelegationThread, DelegationRound, isValidSessionId } from './types';
import { AgentDetector } from './agent-detector';

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

    this.emit('taskCompleted', { thread, round });

    if (!callerSessionId || !isValidSessionId(callerSessionId)) {
      console.warn(
        `[CallbackManager] Thread ${thread.threadId} completed round ${round.round}, but no valid callerSessionId provided or detected (got: "${callerSessionId}").`,
      );
      return false;
    }

    const preview = round.output
      ? round.output.slice(0, 200).replace(/[\r\n]+/g, ' ')
      : round.error
        ? `Error: ${round.error.slice(0, 150)}`
        : 'Completed';

    const promptMessage = `[Agent Mesh Bridge 自动化闭环验收指令] 任务【${thread.title}】第 ${round.round} 轮已由 ${thread.targetAgent} 执行完成。\n结果摘要: ${preview}\n\n请作为任务下发方立即执行自动化检视闭环：\n1. 审查代码与产出，并在当前工作区运行相关测试（如 gradlew/npm test）进行验证；\n2. 若存在编译报错、测试未通过或功能缺陷：请务必立即直接调用 mcp 工具 request_rework(threadId="${thread.threadId}", feedback="具体报错与修复要求") 提交返修；\n3. 若测试全部通过且功能确认无误：请务必立即直接调用 mcp 工具 approve_task(threadId="${thread.threadId}", remarks="验收结论与测试结果") 归档任务。\n请注意：请直接调用上述 MCP 工具完成闭环，请勿仅输出文本结论。`;

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

    const spawnOpts: child_process.SpawnOptions = {
      cwd,
      detached: true,
      stdio: ['ignore', outFd, outFd],
      windowsHide: true,
      env: { ...process.env, CI: '1', NON_INTERACTIVE: 'true' },
    };

    const attachErrorLogging = (child: child_process.ChildProcess) => {
      child.on('error', (err) => {
        console.error(`[CallbackManager] Failed to launch ${initiator} callback:`, err);
        try {
          if (typeof outFd === 'number') {
            fs.writeSync(outFd, `Spawn error: ${err.message}\n`);
          }
        } catch {}
        this.emit('callbackFailed', { thread, round, error: err.message });
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
        // codex exec resume <callerSessionId> "<promptMessage>" --dangerously-bypass-approvals-and-sandbox
        const child = child_process.spawn(
          codexPath,
          [
            'exec',
            'resume',
            callerSessionId,
            promptMessage,
            '--dangerously-bypass-approvals-and-sandbox',
          ],
          spawnOpts,
        );
        attachErrorLogging(child);
        return true;
      }
    } catch (err) {
      console.error(`[CallbackManager] Failed to wake up caller ${initiator}:`, err);
      return false;
    }

    return false;
  }
}
