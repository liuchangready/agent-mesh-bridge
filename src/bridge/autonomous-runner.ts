import * as child_process from 'child_process';
import { AgentId } from './types';
import { AgentDetector } from './agent-detector';

export interface RunOptions {
  agent: AgentId;
  prompt: string;
  isRework: boolean;
  receiverSessionId: string;
  title?: string;
  cwd?: string;
  timeoutMs?: number; // Overall process timeout, default 15 min
  silenceTimeoutMs?: number; // Silence watchdog timeout, default 180s
  onStdoutChunk?: (chunk: string) => void;
  onStderrChunk?: (chunk: string) => void;
}

export interface RunResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  extractedOutput: string;
  durationMs: number;
  error?: string;
}

export class AutonomousRunner {
  private detector: AgentDetector;

  constructor(detector: AgentDetector) {
    this.detector = detector;
  }

  /**
   * Builds execution command and arguments for the target agent adhering to skip-danger standards.
   */
  public buildCommand(options: RunOptions): { command: string; args: string[]; useShell: boolean } {
    const { agent, prompt, isRework, receiverSessionId, title } = options;
    const resolvedPath = this.detector.resolveExecutable(agent);
    const isWindows = process.platform === 'win32';

    if (agent === 'codex') {
      const exec = resolvedPath || (isWindows ? 'codex.exe' : 'codex');
      if (isRework) {
        // codex exec resume <receiverSessionId> "<feedback>" --dangerously-bypass-approvals-and-sandbox --json
        return {
          command: exec,
          args: [
            'exec',
            'resume',
            receiverSessionId,
            prompt,
            '--dangerously-bypass-approvals-and-sandbox',
            '--json',
          ],
          useShell: false,
        };
      } else {
        // codex exec "<prompt>" --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --skip-git-repo-check --ask-for-approval never --json
        return {
          command: exec,
          args: [
            'exec',
            prompt,
            '--dangerously-bypass-approvals-and-sandbox',
            '--dangerously-bypass-hook-trust',
            '--skip-git-repo-check',
            '--ask-for-approval',
            'never',
            '--json',
          ],
          useShell: false,
        };
      }
    }

    if (agent === 'claude_code') {
      const exec = resolvedPath || (isWindows ? 'claude.cmd' : 'claude');
      if (isRework) {
        // claude -p "<feedback>" --resume <receiverSessionId> --permission-mode bypassPermissions --output-format json
        return {
          command: isWindows ? 'cmd.exe' : exec,
          args: isWindows
            ? [
                '/c',
                exec,
                '-p',
                prompt,
                '--resume',
                receiverSessionId,
                '--permission-mode',
                'bypassPermissions',
                '--output-format',
                'json',
              ]
            : [
                '-p',
                prompt,
                '--resume',
                receiverSessionId,
                '--permission-mode',
                'bypassPermissions',
                '--output-format',
                'json',
              ],
          useShell: false,
        };
      } else {
        // claude -p "<prompt>" --session-id <uuid> -n "<title>" --permission-mode bypassPermissions --output-format json
        const taskTitle = title || 'Task';
        return {
          command: isWindows ? 'cmd.exe' : exec,
          args: isWindows
            ? [
                '/c',
                exec,
                '-p',
                prompt,
                '--session-id',
                receiverSessionId,
                '-n',
                taskTitle,
                '--permission-mode',
                'bypassPermissions',
                '--output-format',
                'json',
              ]
            : [
                '-p',
                prompt,
                '--session-id',
                receiverSessionId,
                '-n',
                taskTitle,
                '--permission-mode',
                'bypassPermissions',
                '--output-format',
                'json',
              ],
          useShell: false,
        };
      }
    }

    if (agent === 'antigravity') {
      const exec = resolvedPath || (isWindows ? 'agy.exe' : 'agy');
      if (isRework) {
        // agy --conversation <receiverSessionId> -p "<feedback>" --dangerously-skip-permissions --mode accept-edits --output-format json
        return {
          command: exec,
          args: [
            '--conversation',
            receiverSessionId,
            '-p',
            prompt,
            '--dangerously-skip-permissions',
            '--mode',
            'accept-edits',
            '--output-format',
            'json',
          ],
          useShell: false,
        };
      } else {
        // agy -p "<prompt>" --conversation <uuid> --dangerously-skip-permissions --mode accept-edits --output-format json
        return {
          command: exec,
          args: [
            '-p',
            prompt,
            '--conversation',
            receiverSessionId,
            '--dangerously-skip-permissions',
            '--mode',
            'accept-edits',
            '--output-format',
            'json',
          ],
          useShell: false,
        };
      }
    }

    // Default generic fallback
    return {
      command: resolvedPath || agent,
      args: ['-p', prompt],
      useShell: isWindows,
    };
  }

  /**
   * Executes the agent command with non-interactive flags, watchdog timers, and closes stdin.
   */
  public execute(options: RunOptions): Promise<RunResult> {
    return new Promise((resolve) => {
      const startTime = Date.now();
      const { command, args, useShell } = this.buildCommand(options);

      const cwd = options.cwd || process.cwd();
      const timeoutMs = options.timeoutMs || 15 * 60 * 1000; // 15 mins max
      const silenceTimeoutMs = options.silenceTimeoutMs || 180 * 1000; // 180s silence watchdog

      const env: NodeJS.ProcessEnv = {
        ...process.env,
        CI: '1',
        NON_INTERACTIVE: 'true',
        FORCE_COLOR: '0',
        TERM: 'dumb',
        DEBIAN_FRONTEND: 'noninteractive',
      };

      let stdoutData = '';
      let stderrData = '';
      let isSettled = false;
      let lastActivityTime = Date.now();

      let child: child_process.ChildProcess;
      try {
        child = child_process.spawn(command, args, {
          cwd,
          env,
          shell: useShell,
          stdio: ['pipe', 'pipe', 'pipe'],
          windowsHide: true,
        });
      } catch (err: any) {
        const durationMs = Date.now() - startTime;
        return resolve({
          exitCode: -1,
          stdout: '',
          stderr: String(err),
          extractedOutput: '',
          durationMs,
          error: `Failed to spawn process (${command}): ${err.message || err}`,
        });
      }

      // CRITICAL: Close stdin immediately to prevent CLI waiting on user input
      if (child.stdin) {
        try {
          child.stdin.end();
        } catch {}
      }

      // Silence Watchdog
      const silenceInterval = setInterval(() => {
        if (isSettled) {
          clearInterval(silenceInterval);
          return;
        }
        const silentDuration = Date.now() - lastActivityTime;
        if (silentDuration > silenceTimeoutMs) {
          clearInterval(silenceInterval);
          console.warn(
            `[AutonomousRunner] Watchdog triggered: Process ${options.agent} silent for ${Math.round(silentDuration / 1000)}s. Terminating.`,
          );
          try {
            this.killProcess(child);
          } catch {}
        }
      }, 5000);

      // Overall Timeout Timer
      const maxTimer = setTimeout(() => {
        if (!isSettled) {
          console.warn(
            `[AutonomousRunner] Overall timeout (${Math.round(timeoutMs / 1000)}s) reached for ${options.agent}. Terminating.`,
          );
          try {
            this.killProcess(child);
          } catch {}
        }
      }, timeoutMs);

      child.stdout?.on('data', (data) => {
        lastActivityTime = Date.now();
        const str = data.toString();
        stdoutData += str;
        if (options.onStdoutChunk) {
          options.onStdoutChunk(str);
        }
      });

      child.stderr?.on('data', (data) => {
        lastActivityTime = Date.now();
        const str = data.toString();
        stderrData += str;
        if (options.onStderrChunk) {
          options.onStderrChunk(str);
        }
      });

      child.on('error', (err) => {
        if (isSettled) return;
        isSettled = true;
        clearInterval(silenceInterval);
        clearTimeout(maxTimer);
        const durationMs = Date.now() - startTime;
        resolve({
          exitCode: -1,
          stdout: stdoutData,
          stderr: stderrData ? `${stderrData}\n${err.message}` : err.message,
          extractedOutput: this.extractCleanOutput(stdoutData),
          durationMs,
          error: `Process execution error: ${err.message}`,
        });
      });

      child.on('close', (code) => {
        if (isSettled) return;
        isSettled = true;
        clearInterval(silenceInterval);
        clearTimeout(maxTimer);
        const durationMs = Date.now() - startTime;
        const exitCode = code ?? 0;
        const extracted = this.extractCleanOutput(stdoutData);

        let error: string | undefined = undefined;
        if (exitCode !== 0) {
          error = stderrData.trim() || `Process exited with non-zero code ${exitCode}`;
        }

        resolve({
          exitCode,
          stdout: stdoutData,
          stderr: stderrData,
          extractedOutput: extracted || stdoutData.trim(),
          durationMs,
          error,
        });
      });
    });
  }

  private killProcess(child: child_process.ChildProcess): void {
    if (!child.pid) return;
    if (process.platform === 'win32') {
      try {
        child_process.execSync(`taskkill /pid ${child.pid} /T /F`);
      } catch {
        child.kill('SIGKILL');
      }
    } else {
      child.kill('SIGKILL');
    }
  }

  /**
   * Attempts to parse JSON output if applicable, extracting human-readable message/summary.
   */
  private extractCleanOutput(raw: string): string {
    const trimmed = raw.trim();
    if (!trimmed) return '';

    // Check if JSON Lines format
    const lines = trimmed.split('\n');
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i].trim();
      if (line.startsWith('{') && line.endsWith('}')) {
        try {
          const parsed = JSON.parse(line);
          if (parsed.message) return parsed.message;
          if (parsed.content) return parsed.content;
          if (parsed.output) return parsed.output;
          if (parsed.text) return parsed.text;
          if (parsed.result)
            return typeof parsed.result === 'string'
              ? parsed.result
              : JSON.stringify(parsed.result, null, 2);
        } catch {}
      }
    }

    // Try full JSON parse
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed.message) return parsed.message;
        if (parsed.content) return parsed.content;
        if (parsed.output) return parsed.output;
        if (parsed.text) return parsed.text;
        if (parsed.result)
          return typeof parsed.result === 'string'
            ? parsed.result
            : JSON.stringify(parsed.result, null, 2);
      } catch {}
    }

    return trimmed;
  }
}
