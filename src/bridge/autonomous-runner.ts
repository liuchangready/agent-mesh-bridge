import * as child_process from 'child_process';
import {
  AgentId,
  MESH_TASK_RECEIVER_ENV,
  MESH_TASK_ROUND_ENV,
  MESH_TASK_THREAD_ENV,
  ResponseAssessment,
  StreamAssessment,
  TaskInterruption,
} from './types';
import { AgentDetector } from './agent-detector';
import { buildDshHeadlessCommand } from './deepseek-harness';
import { normalizeModelId, resolveConfiguredModel, writeDshModelOverlay } from './model-catalog';
import {
  CancellationAction,
  CancellationRegistry,
  CancellationRequest,
} from './cancellation-registry';

/** Default silence watchdog: 10 minutes (long model reasoning is not a hang). */
export const DEFAULT_SILENCE_TIMEOUT_MS = 10 * 60 * 1000;

export interface RunCommand {
  command: string;
  args: string[];
  useShell: boolean;
  /** Extra environment entries required by the specific agent launch strategy. */
  env?: Record<string, string>;
}

export interface RunOptions {
  agent: AgentId;
  prompt: string;
  isRework: boolean;
  receiverSessionId: string;
  title?: string;
  cwd?: string;
  timeoutMs?: number; // Overall process timeout, default 2 hours (120 mins)
  silenceTimeoutMs?: number; // Silence watchdog timeout, default 10 minutes
  maxRetries?: number; // Max retries for transient startup/network errors, default 3
  /** Thread being executed, so the child can be marked as a task receiver. */
  taskThreadId?: string;
  /** Round being executed (used for diagnostics and receiver guard). */
  taskRound?: number;
  /**
   * Model to run this round with. When omitted the bridge falls back to the
   * model configured for the agent (settings -> `model-config.json`), and only
   * then to whatever default the CLI itself ships with.
   */
  model?: string;
  /**
   * Mark the launched agent as a delegated-task receiver. Defaults to `true`:
   * any process this runner starts is executing work on somebody else's behalf
   * and must not delegate further.
   */
  markAsReceiver?: boolean;
  onStdoutChunk?: (chunk: string) => void;
  onStderrChunk?: (chunk: string) => void;
}

export interface RunResult {
  exitCode: number;
  stdout: string;
  stderr: string;
  extractedOutput: string;
  actualSessionId?: string;
  durationMs: number;
  error?: string;
  /** Set when the process was paused/stopped/timed out rather than finishing. */
  interruption?: TaskInterruption;
  /** Two-part response health: process error code + stream continuity. */
  assessment?: ResponseAssessment;
}

export class AutonomousRunner {
  private detector: AgentDetector;
  private defaultTimeoutMs: number;
  private cancellationRegistry: CancellationRegistry;
  private activeChildren: Map<string, child_process.ChildProcess> = new Map();
  private localCancellations: Map<string, CancellationRequest> = new Map();
  /** Where an unspecified model comes from; replaceable for tests. */
  private modelResolver: (agent: AgentId) => string | undefined = (agent) =>
    resolveConfiguredModel(agent);

  constructor(
    detector: AgentDetector,
    defaultTimeoutMs: number = 2 * 60 * 60 * 1000,
    cancellationRegistry?: CancellationRegistry,
  ) {
    this.detector = detector;
    this.defaultTimeoutMs = defaultTimeoutMs;
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry();
  }

  /**
   * Kill the child process currently executing `threadId` (same process only).
   *
   * Cross-process pause/delete is handled by `CancellationRegistry`: the owning
   * runner polls that file and calls this same path.
   */
  public cancelThread(
    threadId: string,
    action: CancellationAction,
    reason: string,
    by: string = 'user',
  ): boolean {
    const child = this.activeChildren.get(threadId);
    if (!child) return false;

    this.localCancellations.set(threadId, {
      action,
      reason: reason || 'No reason provided',
      at: new Date().toISOString(),
      by,
    });
    try {
      this.killProcess(child);
    } catch {}
    return true;
  }

  public isExecutingThread(threadId: string): boolean {
    return this.activeChildren.has(threadId);
  }

  public setDefaultTimeoutMs(ms: number): void {
    this.defaultTimeoutMs = ms;
  }

  /**
   * Override how the default model for an agent is looked up. The default
   * implementation reads the shared `model-config.json`, which is written by the
   * extension host; tests inject a stub instead.
   */
  public setModelResolver(resolver: (agent: AgentId) => string | undefined): void {
    this.modelResolver = resolver;
  }

  /**
   * The model this run will actually use: an explicit per-task choice wins,
   * otherwise the agent's configured default, otherwise the CLI's own default.
   */
  public resolveModel(agent: AgentId, requested?: string): string | undefined {
    return normalizeModelId(requested) || normalizeModelId(this.modelResolver(agent));
  }

  /**
   * Builds execution command and arguments for the target agent adhering to skip-danger standards.
   */
  public buildCommand(options: RunOptions): RunCommand {
    const { agent, prompt, isRework, receiverSessionId, title } = options;
    const resolvedPath = this.detector.resolveExecutable(agent);
    const isWindows = process.platform === 'win32';
    const model = this.resolveModel(agent, options.model);
    // Codex accepts `--model` only on some subcommands, so the model travels as
    // a global `-c model="..."` config override: it parses identically for a
    // fresh run and for `exec resume`, and cannot be mistaken for a positional.
    const codexModelArgs = model ? ['-c', `model="${model}"`] : [];
    const modelFlagArgs = model ? ['--model', model] : [];

    if (agent === 'codex') {
      const exec = resolvedPath || (isWindows ? 'codex.exe' : 'codex');
      if (isRework) {
        // `--ask-for-approval` is a GLOBAL option: it must precede `exec`, not
        // follow the prompt. Passing it after `exec` makes Codex reject the
        // whole invocation with "unexpected argument", which used to block
        // every rework/split task before the agent even started.
        // codex --ask-for-approval never exec resume <receiverSessionId> "<feedback>" --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --skip-git-repo-check --json
        return {
          command: exec,
          args: [
            ...codexModelArgs,
            '--ask-for-approval',
            'never',
            'exec',
            'resume',
            receiverSessionId,
            prompt,
            '--dangerously-bypass-approvals-and-sandbox',
            '--dangerously-bypass-hook-trust',
            '--skip-git-repo-check',
            '--json',
          ],
          useShell: false,
        };
      } else {
        // codex --ask-for-approval never exec "<prompt>" --dangerously-bypass-approvals-and-sandbox --dangerously-bypass-hook-trust --skip-git-repo-check --json
        return {
          command: exec,
          args: [
            ...codexModelArgs,
            '--ask-for-approval',
            'never',
            'exec',
            prompt,
            '--dangerously-bypass-approvals-and-sandbox',
            '--dangerously-bypass-hook-trust',
            '--skip-git-repo-check',
            '--json',
          ],
          useShell: false,
        };
      }
    }

    if (agent === 'claude_code') {
      const exec = resolvedPath || (isWindows ? 'claude.cmd' : 'claude');
      if (isRework) {
        // claude -p "<feedback>" --resume <receiverSessionId> --dangerously-skip-permissions --output-format json
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
                '--dangerously-skip-permissions',
                '--output-format',
                'json',
                ...modelFlagArgs,
              ]
            : [
                '-p',
                prompt,
                '--resume',
                receiverSessionId,
                '--dangerously-skip-permissions',
                '--output-format',
                'json',
                ...modelFlagArgs,
              ],
          useShell: false,
        };
      } else {
        // claude -p "<prompt>" --session-id <uuid> -n "<title>" --dangerously-skip-permissions --output-format json
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
                '--dangerously-skip-permissions',
                '--output-format',
                'json',
                ...modelFlagArgs,
              ]
            : [
                '-p',
                prompt,
                '--session-id',
                receiverSessionId,
                '-n',
                taskTitle,
                '--dangerously-skip-permissions',
                '--output-format',
                'json',
                ...modelFlagArgs,
              ],
          useShell: false,
        };
      }
    }

    if (agent === 'antigravity') {
      const exec = resolvedPath || (isWindows ? 'agy.exe' : 'agy');
      if (isRework) {
        // agy --conversation <receiverSessionId> -p "<feedback>" --dangerously-skip-permissions --mode accept-edits --output-format stream-json
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
            'stream-json',
            ...modelFlagArgs,
          ],
          useShell: false,
        };
      } else {
        // agy -p "<prompt>" --dangerously-skip-permissions --mode accept-edits --output-format stream-json
        // NOTE: agy does not accept --conversation for new conversations; native conversation ID is emitted in stream-json.
        return {
          command: exec,
          args: [
            '-p',
            prompt,
            '--dangerously-skip-permissions',
            '--mode',
            'accept-edits',
            '--output-format',
            'stream-json',
            ...modelFlagArgs,
          ],
          useShell: false,
        };
      }
    }

    if (agent === 'deepseek_harness') {
      // `dsh headless` answers one task and exits. `--session-id` ADOPTS an
      // existing native Session and rejects unknown ids, so a fresh task must
      // omit it: the real id arrives as the first JSON stream event and is
      // adopted by the dispatcher for later rework rounds.
      if (resolvedPath) {
        const built = buildDshHeadlessCommand(resolvedPath, {
          prompt,
          sessionId: isRework ? receiverSessionId : undefined,
          json: true,
          // DSH has no `--model`; a per-run overlay patch switches the route.
          modelOverlayPath: model ? writeDshModelOverlay(model) : undefined,
        });
        if (built) {
          return built;
        }
      }
    }

    // Default generic fallback
    return {
      command: resolvedPath || agent,
      args: ['-p', prompt, ...modelFlagArgs],
      useShell: isWindows,
    };
  }

  /**
   * Executes the agent command with retry on transient startup/network errors.
   */
  public async execute(options: RunOptions): Promise<RunResult> {
    const maxRetries = options.maxRetries ?? 3;
    let attempt = 0;
    while (true) {
      attempt++;
      const result = await this.executeOnce(options);
      if (result.exitCode === 0) {
        return result;
      }

      if (attempt <= maxRetries && this.isTransientError(result)) {
        const backoffMs = Math.min(2000 * Math.pow(2, attempt - 1), 10000);
        console.warn(
          `[AutonomousRunner] Transient startup/network error on attempt ${attempt}/${maxRetries} for ${options.agent}: "${result.error}". Retrying in ${backoffMs / 1000}s...`,
        );
        await new Promise((r) => setTimeout(r, backoffMs));
        continue;
      }

      return result;
    }
  }

  /**
   * Checks if an execution failed due to an ephemeral network or handshake error.
   */
  public isTransientError(result: RunResult): boolean {
    if (result.exitCode === 0) return false;
    const combined = `${result.stdout}\n${result.stderr}\n${result.error || ''}`.toLowerCase();
    const transientPatterns = [
      'eligibility check failed',
      'eof',
      'econnreset',
      'etimedout',
      'socket hang up',
      'network is unreachable',
      'connection reset by peer',
      'tls handshake timeout',
      'temporary failure in name resolution',
      'fetch failed',
      '502 bad gateway',
      '503 service unavailable',
      '504 gateway timeout',
    ];
    return transientPatterns.some((pattern) => combined.includes(pattern));
  }

  /**
   * Checks if an execution failed due to rate limit or quota exhaustion.
   * Uses precise token/word boundary matching to prevent false positives from
   * code line numbers (e.g. "429:"), UUIDs/hashes (e.g. "...429..."), or normal programming terms.
   */
  public isRateLimitOrQuotaError(text: string): boolean {
    if (!text) return false;
    const rateLimitRegex =
      /\b(?:error\s*429|http\s*429|status(?:\s*code)?[:\s]+429|code["':\s]+429|\[429\]|429\s+too\s+many\s+requests)\b|\b(?:rate[-_\s]*limit(?:ed)?(?:\s*(?:exceeded|reached|hit|error))?|quota[-_\s]*(?:exceeded|exhausted)|resource[-_\s]*exhausted|insufficient[-_\s]*quota|too[-_\s]*many[-_\s]*requests)\b|\b(?:model|server)[-_\s]*(?:is[-_\s]*)?overloaded\b|\b(?:retry[-_\s]*after|try[-_\s]*again[-_\s]*in|resets?[-_\s]*(?:in|at))\s+\d+/i;
    return rateLimitRegex.test(text);
  }

  /**
   * Describes why a CLI agent cannot be launched, or `undefined` when it can.
   *
   * Without this guard an unresolvable executable falls through to the generic
   * command template, which spawns a shell that treats the bare agent id as a
   * program name and reports a cryptic "not recognized as an internal or
   * external command" instead of an actionable configuration error.
   */
  public describeMissingExecutable(agent: AgentId): string | undefined {
    if (agent === 'vscode_chat') return undefined;
    if (this.detector.resolveExecutable(agent)) return undefined;

    const settingsHint: Partial<Record<AgentId, string>> = {
      antigravity: 'agentMeshBridge.antigravityPath',
      claude_code: 'agentMeshBridge.claudeCodePath',
      codex: 'agentMeshBridge.codexPath',
      deepseek_harness: 'agentMeshBridge.deepseekHarnessPath',
    };
    const hint = settingsHint[agent];
    const suffix = hint ? ` Set "${hint}" if it is installed in a non-standard location.` : '';

    return `Agent executable not found for "${agent}".${suffix}`;
  }

  /**
   * Extract the final human-readable answer without falling back to raw
   * JSON status/heartbeat events. This is what "actual output" means for the
   * response assessment.
   */
  public extractAnswerOnly(raw: string): string {
    const trimmed = (raw || '').trim();
    if (!trimmed) return '';

    const cleaned = this.extractCleanOutput(raw).trim();
    const lines = trimmed.split(/\r?\n/);
    const looksLikeJsonStream = lines.some((line) => {
      const value = line.trim();
      return value.startsWith('{') && value.endsWith('}');
    });

    // `extractCleanOutput` falls back to the raw text when it recognizes no
    // answer field. For a JSON event stream that means only status/metadata was
    // emitted, which is not an actual agent answer.
    if (looksLikeJsonStream && cleaned === trimmed) return '';
    return cleaned;
  }

  /**
   * Two-part success assessment:
   *   1. process error code must be 0;
   *   2. the stream must contain actual output and must not have stalled.
   */
  public evaluateResponse(input: {
    exitCode: number;
    stdout: string;
    stderr: string;
    chunkCount: number;
    stdoutBytes: number;
    stderrBytes: number;
    firstOutputAtMs?: number;
    lastOutputAtMs?: number;
    maxGapMs: number;
    gapThresholdMs: number;
    interruption?: TaskInterruption;
    /**
     * Streaming CLIs (Codex/DSH/Antigravity) must keep emitting events. Claude's
     * `--output-format json` emits only one final object, so for it a single
     * substantive chunk counts as continuous.
     */
    expectStreaming?: boolean;
  }): ResponseAssessment {
    const finalOutput = this.extractAnswerOnly(input.stdout);
    const hasActualOutput = finalOutput.trim().length > 0;
    const expectStreaming = input.expectStreaming !== false;
    const continuous =
      input.chunkCount > 0 && (!expectStreaming || input.maxGapMs <= input.gapThresholdMs);

    const stream: StreamAssessment = {
      chunkCount: input.chunkCount,
      stdoutBytes: input.stdoutBytes,
      stderrBytes: input.stderrBytes,
      firstOutputAtMs: input.firstOutputAtMs,
      lastOutputAtMs: input.lastOutputAtMs,
      maxGapMs: input.maxGapMs,
      gapThresholdMs: input.gapThresholdMs,
      hasActualOutput,
      continuous,
    };

    if (input.interruption) {
      return {
        success: false,
        errorCode: input.interruption.kind.toUpperCase(),
        exitCode: input.exitCode,
        stream,
        reason: input.interruption.reason,
      };
    }

    if (input.exitCode !== 0) {
      return {
        success: false,
        errorCode: 'NONZERO_EXIT',
        exitCode: input.exitCode,
        stream,
        reason:
          input.stderr.trim() ||
          `Process exited with non-zero code ${input.exitCode}`,
      };
    }

    if (!hasActualOutput) {
      return {
        success: false,
        errorCode: 'EMPTY_OUTPUT',
        exitCode: input.exitCode,
        stream,
        reason:
          'Process exited with code 0 but the stream contained no actual final answer ' +
          '(only status/metadata or no output).',
      };
    }

    if (!continuous) {
      return {
        success: false,
        errorCode: 'STREAM_STALLED',
        exitCode: input.exitCode,
        stream,
        reason:
          `Stream stalled: longest output gap ${input.maxGapMs}ms exceeded ` +
          `the ${input.gapThresholdMs}ms threshold.`,
      };
    }

    return {
      success: true,
      errorCode: 'OK',
      exitCode: input.exitCode,
      stream,
    };
  }

  /**
   * Executes a single attempt of the agent command.
   */
  public executeOnce(options: RunOptions): Promise<RunResult> {
    return new Promise((resolve) => {
      const missingExecutable = this.describeMissingExecutable(options.agent);
      if (missingExecutable) {
        console.error(`[AutonomousRunner] ${missingExecutable}`);
        resolve({
          exitCode: -1,
          stdout: '',
          stderr: missingExecutable,
          extractedOutput: '',
          durationMs: 0,
          error: missingExecutable,
        });
        return;
      }

      const startTime = Date.now();
      const { command, args, useShell, env: commandEnv } = this.buildCommand(options);

      const cwd = options.cwd || process.cwd();
      const timeoutMs = options.timeoutMs || this.defaultTimeoutMs; // 2 hours default
      // Long model reasoning / silent tool waits can easily exceed 3 minutes.
      // Keep a 10-minute default; callers may still override per run.
      const silenceTimeoutMs = options.silenceTimeoutMs || DEFAULT_SILENCE_TIMEOUT_MS;

      const env: NodeJS.ProcessEnv = {
        ...process.env,
        CI: '1',
        NON_INTERACTIVE: 'true',
        FORCE_COLOR: '0',
        TERM: 'dumb',
        DEBIAN_FRONTEND: 'noninteractive',
        ...(commandEnv || {}),
      };

      // Never let a stale marker leak in from the launching process, then mark
      // this child according to its role. The marker is inherited by the MCP
      // server the agent spawns, which uses it to refuse forwarding tools.
      delete env[MESH_TASK_RECEIVER_ENV];
      delete env[MESH_TASK_THREAD_ENV];
      delete env[MESH_TASK_ROUND_ENV];
      if (options.markAsReceiver !== false) {
        env[MESH_TASK_RECEIVER_ENV] = '1';
        if (options.taskThreadId) env[MESH_TASK_THREAD_ENV] = options.taskThreadId;
        if (options.taskRound !== undefined) env[MESH_TASK_ROUND_ENV] = String(options.taskRound);
      }

      // DSH's headless profile derives BOTH its sandbox mode and its approval
      // policy from DSH_PERMISSION_MODE. Without this it boots with
      // `workspace-write` + `approval: ask`, so a delegated DSH task stalls on a
      // permission prompt that nobody can answer. `danger-full-access` makes the
      // approval policy `never` and matches the other agents' skip-danger mode.
      if (options.agent === 'deepseek_harness') {
        env.DSH_PERMISSION_MODE = 'danger-full-access';
      }

      let stdoutData = '';
      let stderrData = '';
      let isSettled = false;
      let lastActivityTime = Date.now();
      let watchdogTriggered = false;
      let maxTimerTriggered = false;

      // Response health metrics: process error code + real/continuous stream.
      let chunkCount = 0;
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let firstOutputAtMs: number | undefined;
      let lastOutputAtMs: number | undefined;
      let maxGapMs = 0;

      const noteStreamChunk = (byteLength: number) => {
        const now = Date.now();
        const elapsed = now - startTime;
        chunkCount++;
        if (firstOutputAtMs === undefined) {
          firstOutputAtMs = elapsed;
          maxGapMs = Math.max(maxGapMs, elapsed);
        } else if (lastOutputAtMs !== undefined) {
          maxGapMs = Math.max(maxGapMs, elapsed - lastOutputAtMs);
        }
        lastOutputAtMs = elapsed;
        return byteLength;
      };

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

      // Cross-process pause/delete: a control plane running elsewhere writes a
      // cancellation file; this runner owns the child and is the only one that
      // can actually kill it.
      const taskThreadId = options.taskThreadId;
      let cancellationPoll: NodeJS.Timeout | null = null;
      let externalCancellation: CancellationRequest | undefined;

      const takeCancellation = (): CancellationRequest | undefined => {
        if (!taskThreadId) return undefined;
        const local = this.localCancellations.get(taskThreadId);
        // A pause may land in the tiny window between the child exiting and
        // this handler running; consume once more so it still wins.
        const lateExternal = this.cancellationRegistry.consume(taskThreadId);
        const cancellation = local || externalCancellation || lateExternal;
        this.localCancellations.delete(taskThreadId);
        this.activeChildren.delete(taskThreadId);
        this.cancellationRegistry.clear(taskThreadId);
        if (cancellationPoll) {
          clearInterval(cancellationPoll);
          cancellationPoll = null;
        }
        return cancellation;
      };

      if (taskThreadId) {
        this.activeChildren.set(taskThreadId, child);

        // A pause/delete may have landed between dispatch and spawn.
        const pending = this.cancellationRegistry.consume(taskThreadId);
        if (pending) {
          externalCancellation = pending;
          try {
            this.killProcess(child);
          } catch {}
        }

        cancellationPoll = setInterval(() => {
          if (isSettled || !taskThreadId) return;
          const request = this.cancellationRegistry.consume(taskThreadId);
          if (request) {
            externalCancellation = request;
            console.warn(
              `[AutonomousRunner] ${request.action} requested for ${taskThreadId}: ${request.reason}`,
            );
            try {
              this.killProcess(child);
            } catch {}
          }
        }, 1000);
        cancellationPoll.unref?.();
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
          watchdogTriggered = true;
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
          maxTimerTriggered = true;
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
        stdoutBytes += data.length;
        noteStreamChunk(data.length);
        const str = data.toString();
        stdoutData += str;
        if (options.onStdoutChunk) {
          options.onStdoutChunk(str);
        }
      });

      child.stderr?.on('data', (data) => {
        lastActivityTime = Date.now();
        stderrBytes += data.length;
        noteStreamChunk(data.length);
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
        const cancellation = takeCancellation();
        const durationMs = Date.now() - startTime;
        const interruption: TaskInterruption | undefined = cancellation
          ? {
              kind:
                cancellation.action === 'pause'
                  ? 'paused'
                  : cancellation.action === 'delete'
                    ? 'deleted'
                    : 'stopped',
              reason: cancellation.reason,
              at: cancellation.at || new Date().toISOString(),
              by: cancellation.by,
            }
          : undefined;
        resolve({
          exitCode: -1,
          stdout: stdoutData,
          stderr: stderrData ? `${stderrData}\n${err.message}` : err.message,
          extractedOutput: this.extractCleanOutput(stdoutData),
          durationMs,
          error: cancellation
            ? `Task ${cancellation.action} requested${cancellation.by ? ` by ${cancellation.by}` : ''}: ${cancellation.reason}`
            : `Process execution error: ${err.message}`,
          interruption,
        });
      });

      child.on('close', (code) => {
        if (isSettled) return;
        isSettled = true;
        clearInterval(silenceInterval);
        clearTimeout(maxTimer);
        const cancellation = takeCancellation();
        const durationMs = Date.now() - startTime;
        const extracted = this.extractCleanOutput(stdoutData);
        const actualSessionId = this.extractSessionId(stdoutData);
        let error: string | undefined = undefined;
        let interruption: TaskInterruption | undefined = undefined;

        if (cancellation) {
          const kind =
            cancellation.action === 'pause'
              ? 'paused'
              : cancellation.action === 'delete'
                ? 'deleted'
                : 'stopped';
          interruption = {
            kind,
            reason: cancellation.reason,
            at: cancellation.at || new Date().toISOString(),
            by: cancellation.by,
          };
          error =
            `Task ${cancellation.action} requested` +
            `${cancellation.by ? ` by ${cancellation.by}` : ''}: ${cancellation.reason}`;
        } else if (watchdogTriggered) {
          error = `Process terminated by watchdog: silent for more than ${Math.round(silenceTimeoutMs / 1000)}s without output`;
          interruption = {
            kind: 'watchdog',
            reason: error,
            at: new Date().toISOString(),
            by: 'watchdog',
          };
        } else if (maxTimerTriggered) {
          error = `Process terminated: exceeded maximum execution timeout of ${Math.round(timeoutMs / 1000)}s`;
          interruption = {
            kind: 'timeout',
            reason: error,
            at: new Date().toISOString(),
            by: 'timeout',
          };
        } else if (code !== 0) {
          error = stderrData.trim() || `Process exited with non-zero code ${code ?? -1}`;
        }

        const exitCode = cancellation ? -1 : (code ?? 0);

        const assessment = this.evaluateResponse({
          exitCode,
          stdout: stdoutData,
          stderr: stderrData,
          chunkCount,
          stdoutBytes,
          stderrBytes,
          firstOutputAtMs,
          lastOutputAtMs,
          maxGapMs,
          gapThresholdMs: silenceTimeoutMs,
          interruption,
          // Claude's JSON output is a single final object, not a live stream.
          expectStreaming: options.agent !== 'claude_code',
        });
        if (!assessment.success && !error) {
          error = assessment.reason;
        }

        // Process completion & Rate limit evaluation:
        // 1. If process succeeded (exitCode === 0 and no error), agent is active and healthy. Clear any stale exhaustion.
        if (!interruption && exitCode === 0 && !error) {
          if (this.detector.getRateLimitTracker().isExhausted(options.agent)) {
            this.detector.getRateLimitTracker().clearExhaustion(options.agent);
          }
        } else if (!interruption) {
          // 2. Only on process failure/error do we inspect for quota or rate limit exhaustion.
          // Prioritize stderrData and error, falling back to stdoutData only on non-zero exit code.
          const errorContext = `${stderrData}\n${error || ''}\n${stdoutData}`;
          if (this.isRateLimitOrQuotaError(errorContext)) {
            const errorSnippet = (stderrData.trim() || error || stdoutData.trim()).slice(0, 500);
            this.detector.getRateLimitTracker().recordExhaustion(options.agent, errorSnippet);
          }
        }

        resolve({
          exitCode,
          stdout: stdoutData,
          stderr: stderrData,
          extractedOutput: extracted || stdoutData.trim(),
          actualSessionId,
          durationMs,
          error,
          interruption,
          assessment,
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
   * Attempts to extract native conversation / session ID from agent output stream.
   */
  public extractSessionId(raw: string): string | undefined {
    const trimmed = raw.trim();
    if (!trimmed) return undefined;

    const lines = trimmed.split('\n');
    for (const line of lines) {
      const trimmedLine = line.trim();
      if (trimmedLine.startsWith('{') && trimmedLine.endsWith('}')) {
        try {
          const parsed = JSON.parse(trimmedLine);
          if (parsed.conversation_id) return String(parsed.conversation_id);
          if (parsed.session_id) return String(parsed.session_id);
          // Codex `exec --json` emits {"type":"thread.started","thread_id":"..."}.
          if (parsed.thread_id) return String(parsed.thread_id);
          // DeepSeek Harness emits {"type":"session","sessionId":"session-<uuid>"}
          // as its first run event.
          if (parsed.sessionId) return String(parsed.sessionId);
          if (parsed.result?.conversation_id) return String(parsed.result.conversation_id);
          if (parsed.result?.session_id) return String(parsed.result.session_id);
          if (parsed.step_update?.conversation_id)
            return String(parsed.step_update.conversation_id);
        } catch {}
      }
    }
    return undefined;
  }

  /**
   * Attempts to parse JSON / stream-json output, extracting human-readable message/summary.
   */
  public extractCleanOutput(raw: string): string {
    const trimmed = raw.trim();
    if (!trimmed) return '';

    // Check if JSON Lines format
    const lines = trimmed.split('\n');
    for (let i = lines.length - 1; i >= 0; i--) {
      const line = lines[i].trim();
      if (line.startsWith('{') && line.endsWith('}')) {
        try {
          const parsed = JSON.parse(line);
          if (parsed.event === 'result' && parsed.result) {
            if (parsed.result.response) return String(parsed.result.response);
            if (typeof parsed.result === 'string') return parsed.result;
            return JSON.stringify(parsed.result, null, 2);
          }
          // Codex `exec --json` reports the final assistant message as
          // {"type":"item.completed","item":{"type":"agent_message","text":"..."}}.
          if (parsed.item?.type === 'agent_message' && typeof parsed.item.text === 'string') {
            return parsed.item.text;
          }
          // DeepSeek Harness terminates its run-event stream with the answer.
          if (parsed.type === 'final' && typeof parsed.text === 'string') return parsed.text;
          if (parsed.response) return String(parsed.response);
          if (parsed.message) return String(parsed.message);
          if (parsed.content) return String(parsed.content);
          if (parsed.output) return String(parsed.output);
          if (parsed.text) return String(parsed.text);
          if (parsed.result) {
            if (typeof parsed.result === 'object' && parsed.result.response) {
              return String(parsed.result.response);
            }
            return typeof parsed.result === 'string'
              ? parsed.result
              : JSON.stringify(parsed.result, null, 2);
          }
        } catch {}
      }
    }

    // Try full JSON parse
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        const parsed = JSON.parse(trimmed);
        if (parsed.event === 'result' && parsed.result?.response)
          return String(parsed.result.response);
        if (parsed.response) return String(parsed.response);
        if (parsed.message) return String(parsed.message);
        if (parsed.content) return String(parsed.content);
        if (parsed.output) return String(parsed.output);
        if (parsed.text) return String(parsed.text);
        if (parsed.result) {
          if (typeof parsed.result === 'object' && parsed.result.response) {
            return String(parsed.result.response);
          }
          return typeof parsed.result === 'string'
            ? parsed.result
            : JSON.stringify(parsed.result, null, 2);
        }
      } catch {}
    }

    // Fallback: check if stream-json has text_delta in agent_response steps
    const deltas: string[] = [];
    for (const line of lines) {
      const trimmedLine = line.trim();
      if (trimmedLine.startsWith('{') && trimmedLine.endsWith('}')) {
        try {
          const parsed = JSON.parse(trimmedLine);
          if (parsed.event === 'step_update' && parsed.step_update?.text_delta) {
            deltas.push(parsed.step_update.text_delta);
          }
        } catch {}
      }
    }
    if (deltas.length > 0) {
      return deltas.join('');
    }

    return trimmed;
  }
}
