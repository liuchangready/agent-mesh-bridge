import * as child_process from 'child_process';
import { EventEmitter } from 'events';
import { DelegationThread, DelegationRound } from './types';
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

    // Try auto-detecting callerSessionId if not already set
    if (!callerSessionId && this.detector) {
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

    if (!callerSessionId) {
      console.log(
        `[CallbackManager] Thread ${thread.threadId} completed round ${round.round}, but no callerSessionId provided or detected.`,
      );
      return false;
    }

    const preview = round.output
      ? round.output.slice(0, 200).replace(/[\r\n]+/g, ' ')
      : round.error
        ? `Error: ${round.error.slice(0, 150)}`
        : 'Completed';

    const promptMessage = `[Agent Mesh Bridge 回调通知] 任务【${thread.title}】第 ${round.round} 轮已由 ${thread.targetAgent} 执行完成。\n结果摘要: ${preview}\n请使用 get_thread_details(threadId="${thread.threadId}") 审查代码与产出，并在满意时调用 approve_task，或需要调整时调用 request_rework。`;

    console.log(`[CallbackManager] Waking up caller ${initiator} (session: ${callerSessionId})...`);

    const cwd = thread.workspacePath || process.cwd();

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
          {
            cwd,
            detached: true,
            stdio: 'ignore',
            windowsHide: true,
            env: { ...process.env, CI: '1', NON_INTERACTIVE: 'true' },
          },
        );
        child.unref();
        return true;
      }

      if (initiator === 'claude_code') {
        const isWindows = process.platform === 'win32';
        const claudePath =
          this.detector.resolveExecutable('claude_code') || (isWindows ? 'claude.cmd' : 'claude');
        const child = isWindows
          ? child_process.spawn(
              'cmd.exe',
              [
                '/c',
                claudePath,
                '--resume',
                callerSessionId,
                '-p',
                promptMessage,
                '--permission-mode',
                'bypassPermissions',
              ],
              {
                cwd,
                detached: true,
                stdio: 'ignore',
                windowsHide: true,
                env: { ...process.env, CI: '1', NON_INTERACTIVE: 'true' },
              },
            )
          : child_process.spawn(
              claudePath,
              [
                '--resume',
                callerSessionId,
                '-p',
                promptMessage,
                '--permission-mode',
                'bypassPermissions',
              ],
              {
                cwd,
                detached: true,
                stdio: 'ignore',
                env: { ...process.env, CI: '1', NON_INTERACTIVE: 'true' },
              },
            );
        child.unref();
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
          {
            cwd,
            detached: true,
            stdio: 'ignore',
            windowsHide: true,
            env: { ...process.env, CI: '1', NON_INTERACTIVE: 'true' },
          },
        );
        child.unref();
        return true;
      }
    } catch (err) {
      console.error(`[CallbackManager] Failed to wake up caller ${initiator}:`, err);
      return false;
    }

    return false;
  }
}
