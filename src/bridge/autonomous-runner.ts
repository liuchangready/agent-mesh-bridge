import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  AgentId,
  DEFAULT_STREAM_GAP_POLICY,
  MESH_TASK_RECEIVER_ENV,
  MESH_TASK_ROUND_ENV,
  MESH_TASK_THREAD_ENV,
  ResponseAssessment,
  StreamAssessment,
  StreamGapPolicy,
  TaskInterruption,
  TokenUsageMetrics,
} from './types';
import { AgentDetector } from './agent-detector';
import { buildDshHeadlessCommand } from './deepseek-harness';
import { HostSuspensionGuard, monotonicNowMs } from './host-suspension';
import { defaultBinary, killProcessTree, needsShellWrapper } from './platform';
import { normalizeModelId, resolveConfiguredModel, writeDshModelOverlay } from './model-catalog';
import { accumulateTokenUsage, collectTokenUsage } from './token-usage-collector';
import {
  CancellationAction,
  CancellationRegistry,
  CancellationRequest,
} from './cancellation-registry';
import type { TaskStore } from './task-store';

/** Default silence watchdog: 10 minutes (long model reasoning is not a hang). */
export const DEFAULT_SILENCE_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * Network/transport level failures that are worth one more attempt with the
 * very same prompt. Kept to explicit phrases and errno spellings so normal build
 * output (a diff mentioning "connection", a test name, `eof` handling code)
 * cannot trigger a spurious replay.
 */
const TRANSPORT_ERROR_PATTERNS: string[] = [
  // DSH reports dropped transports as `dsh: TRANSPORT: Connection error.`
  'transport: connection error',
  'connection error',
  'network error',
  'network is unreachable',
  'connection reset by peer',
  'connection refused',
  'socket hang up',
  'socket closed',
  'other side closed',
  'premature close',
  'tls handshake timeout',
  'temporary failure in name resolution',
  'fetch failed',
  'econnreset',
  'econnrefused',
  'etimedout',
  'enotfound',
  'eai_again',
  'enetunreach',
  'und_err_connect_timeout',
  'und_err_socket',
];

/**
 * Structured "provider refused us" signals.
 *
 * The agent CLIs retry these internally (Claude Code: 10 attempts with 30–40s
 * backoff), so a delegated round keeps burning the remaining quota for minutes
 * after the provider already said no. The runner aborts on the first signal
 * instead, and reports RATE_LIMITED so the caller can back off or switch
 * executors.
 *
 * Only structured error envelopes and stderr text are matched: assistant output
 * can legitimately quote source files that mention rate limits.
 */
const RATE_LIMIT_SIGNALS: RegExp[] = [
  /"error_status"\s*:\s*429/i,
  /"error"\s*:\s*"rate_limit"/i,
  /"status"\s*:\s*429\b/i,
  /\bhttp\s*429\b/i,
  /\b429\s+too\s+many\s+requests\b/i,
  /\btoo\s+many\s+requests\b/i,
  /\brate[-_\s]?limit(?:ed|_error|_exceeded)?\b/i,
];

/** The first rate-limit signal in a chunk of provider output, if any. */
export function detectRateLimitSignal(text: string): string | undefined {
  if (!text) return undefined;
  for (const pattern of RATE_LIMIT_SIGNALS) {
    const match = pattern.exec(text);
    if (match) return match[0];
  }
  return undefined;
}

/**
 * Transcript envelopes carry the agent's own words: tool results quote whole
 * source files and test logs, so they routinely contain phrases like
 * `"RATE_LIMITED"` (a real application error code), `"HTTP 429"` in a test
 * assertion, or the words "rate limit" in a comment. They are never a provider
 * refusal and must not abort the run.
 */
const TRANSCRIPT_ENVELOPE_TYPES = new Set([
  'assistant',
  'user',
  'stream_event',
  'tool_use',
  'tool_result',
  'thinking',
  'text',
  'final',
  'message',
]);

/**
 * Detects a provider rate limit in one stdout line of a streaming agent CLI.
 *
 * Only structured error envelopes count: the envelope must be JSON, must not be
 * a transcript message, and must actually look like a provider error (an error
 * type/subtype, `is_error`, a 429 status, or an `error` field). Free-text
 * mentions of rate limiting are ignored on purpose — see
 * {@link TRANSCRIPT_ENVELOPE_TYPES}.
 *
 * @returns the matched signal, or `undefined` when the line is not a structured
 * provider error (including partial/non-JSON lines, which the caller buffers).
 */
export function detectStructuredRateLimitSignal(line: string): string | undefined {
  const text = line.trim();
  if (!text.startsWith('{')) return undefined;

  let envelope: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(text);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
    envelope = parsed as Record<string, unknown>;
  } catch {
    // A JSON line split across chunks: keep buffering, never guess.
    return undefined;
  }

  const type = typeof envelope.type === 'string' ? envelope.type : '';
  if (TRANSCRIPT_ENVELOPE_TYPES.has(type)) return undefined;

  const subtype = typeof envelope.subtype === 'string' ? envelope.subtype : '';
  const looksLikeProviderError =
    type === 'error' ||
    subtype === 'api_retry' ||
    subtype === 'api_error' ||
    subtype === 'rate_limit' ||
    envelope.is_error === true ||
    Number(envelope.error_status) === 429 ||
    Number(envelope.status) === 429 ||
    envelope.error !== undefined;

  if (!looksLikeProviderError) return undefined;
  return detectRateLimitSignal(text);
}

/**
 * Claude Code keeps a per-process ownership record under `~/.claude/sessions/`
 * ("pid owns sessionId") so that two processes cannot resume the same native
 * session at once. A hard kill (`taskkill /F` on a rate-limit abort, a pause or
 * a window reload) gives the CLI no chance to unregister, and every later
 * `--resume` then fails with `Session ID <id> is already in use.` even though
 * the owning process is long gone. These helpers clean such stale records up.
 */
export function claudeSessionRegistryDir(homeDir: string = os.homedir()): string {
  return path.join(homeDir, '.claude', 'sessions');
}

/** True when `pid` is still running (EPERM means it exists but is not ours). */
function isProcessAlive(pid: number): boolean {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

export interface StaleClaudeSessionLock {
  file: string;
  pid: number;
  sessionId: string;
}

/**
 * Deletes Claude Code session-ownership records whose owning process is dead.
 *
 * Records of live processes are never touched, so a genuinely concurrent run
 * keeps its lock. Select by `pid` (the child this runner launched) and/or by
 * `sessionId` (the session we are about to resume).
 */
export function clearStaleClaudeSessionLocks(
  selector: { pid?: number; sessionId?: string },
  homeDir: string = os.homedir(),
): StaleClaudeSessionLock[] {
  const dir = claudeSessionRegistryDir(homeDir);
  let files: string[];
  try {
    files = fs.readdirSync(dir).filter((name) => name.toLowerCase().endsWith('.json'));
  } catch {
    return [];
  }

  const removed: StaleClaudeSessionLock[] = [];
  for (const name of files) {
    const file = path.join(dir, name);
    let record: { pid?: unknown; sessionId?: unknown };
    try {
      record = JSON.parse(fs.readFileSync(file, 'utf8')) as { pid?: unknown; sessionId?: unknown };
    } catch {
      continue;
    }
    const pid = Number(record.pid);
    const sessionId = typeof record.sessionId === 'string' ? record.sessionId : '';
    if (selector.pid !== undefined && pid !== selector.pid) continue;
    if (selector.sessionId !== undefined && sessionId !== selector.sessionId) continue;
    if (isProcessAlive(pid)) continue;
    try {
      fs.unlinkSync(file);
      removed.push({ file, pid, sessionId });
    } catch {
      // Best effort: the retry path re-checks the record anyway.
    }
  }
  return removed;
}

/** Extracts the session id from Claude's `Session ID <id> is already in use.` */
export function parseInUseSessionId(text: string): string | undefined {
  const match = /Session ID\s+([0-9a-fA-F][0-9a-fA-F-]{7,})\s+is already in use/i.exec(text || '');
  return match ? match[1] : undefined;
}

export interface RunCommand {
  command: string;
  args: string[];
  useShell: boolean;
  /** Extra environment entries required by the specific agent launch strategy. */
  env?: Record<string, string>;
  /**
   * Prompt written to the child's stdin before it is closed.
   *
   * Some CLIs must not receive the prompt as an argument: on Windows the npm
   * shims (`claude.cmd`) are launched through cmd.exe, which treats a newline
   * inside an argument as a command separator and silently drops everything
   * after the first line.
   */
  stdinPayload?: string;
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
  /**
   * How to treat an output gap that exceeds the threshold while the process
   * still exited 0 with a real final answer. Default `warn`: keep the round
   * successful and record a `STREAM_GAP_EXCEEDED` warning. `fail` restores the
   * strict `STREAM_STALLED` verdict for that round.
   */
  streamGapPolicy?: StreamGapPolicy;
  maxRetries?: number; // Max retries for transient startup/network errors, default 3
  /** Base delay for the exponential retry backoff; tests shorten it. */
  retryBackoffMs?: number;
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
   * Token usage already recorded for this round.
   *
   * `retry_task` and `resume_task` reuse the SAME round number, so a second
   * execution must ADD to what the first one spent. The executor seeds the
   * runner with the round's stored measurement, so the returned figure is
   * always the round's running total — and stays idempotent when two
   * processes finalize the same round.
   */
  previousTokenUsage?: TokenUsageMetrics;
  /**
   * Mark the launched agent as a delegated-task receiver. Defaults to `true`:
   * any process this runner starts is executing work on somebody else's behalf
   * and must not delegate further.
   */
  markAsReceiver?: boolean;
  onStdoutChunk?: (chunk: string) => void;
  onStderrChunk?: (chunk: string) => void;
  /**
   * Reports the real agent child PID on spawn and `undefined` once it exits.
   *
   * The global execution lease records it so that a leaked child (host killed,
   * agent still running) can be detected and killed before another round starts.
   */
  onChildPid?: (pid: number | undefined) => void;
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
  /** Normalized token accounting for this attempt (see token-usage-collector). */
  tokenUsage?: TokenUsageMetrics;
  /** The model this attempt actually ran with. */
  model?: string;
}

export class AutonomousRunner {
  private detector: AgentDetector;
  private defaultTimeoutMs: number;
  private cancellationRegistry: CancellationRegistry;
  private taskStore?: TaskStore;
  private activeChildren: Map<string, child_process.ChildProcess> = new Map();
  private localCancellations: Map<string, CancellationRequest> = new Map();
  private executingThreads: Set<string> = new Set();
  /** Where an unspecified model comes from; replaceable for tests. */
  private modelResolver: (agent: AgentId) => string | undefined = (agent) =>
    resolveConfiguredModel(agent);
  /** Home directory holding the agent CLIs' state; replaceable for tests. */
  public homeDir: string = os.homedir();

  constructor(
    detector: AgentDetector,
    defaultTimeoutMs: number = 2 * 60 * 60 * 1000,
    cancellationRegistry?: CancellationRegistry,
    taskStore?: TaskStore,
  ) {
    this.detector = detector;
    this.defaultTimeoutMs = defaultTimeoutMs;
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry();
    this.taskStore = taskStore;
  }

  /**
   * Kill the child process currently executing `threadId` (same process only)
   * and record the cancellation so pending/retrying runs abort immediately.
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
    this.localCancellations.set(threadId, {
      action,
      reason: reason || 'No reason provided',
      at: new Date().toISOString(),
      by,
    });
    const child = this.activeChildren.get(threadId);
    if (child) {
      try {
        this.killProcess(child);
      } catch {}
    }
    return true;
  }

  public isExecutingThread(threadId: string): boolean {
    return this.executingThreads.has(threadId) || this.activeChildren.has(threadId);
  }

  /**
   * Inspects local cancellations, external cancellation files, and the task store
   * to determine whether `taskThreadId` has been cancelled, paused, or deleted.
   */
  public checkPendingCancellation(
    taskThreadId: string | undefined,
    initialTaskExisted?: boolean,
  ): CancellationRequest | undefined {
    if (!taskThreadId) return undefined;

    const local = this.localCancellations.get(taskThreadId);
    if (local) return local;

    const external = this.cancellationRegistry.peek(taskThreadId);
    if (external) return external;

    if (this.taskStore) {
      const stored = this.taskStore.getTask(taskThreadId);
      if (initialTaskExisted && !stored) {
        return {
          action: 'delete',
          reason: 'Task was deleted from store during execution',
          at: new Date().toISOString(),
          by: 'task_store',
        };
      }
      if (stored?.status === 'paused') {
        return {
          action: 'pause',
          reason: stored.interruption?.reason || 'Task was paused in store',
          at: stored.interruption?.at || new Date().toISOString(),
          by: stored.interruption?.by || 'task_store',
        };
      }
    }

    return undefined;
  }

  /**
   * Drops every remembered pause/delete for `taskThreadId`, in memory and on disk.
   *
   * A resume must call this: a pause that landed before any child existed is never
   * consumed by a settling round, so without this the next attempt would be
   * cancelled on sight.
   */
  public clearPendingCancellation(taskThreadId: string | undefined): void {
    if (!taskThreadId) return;
    this.localCancellations.delete(taskThreadId);
    this.cancellationRegistry.clear(taskThreadId);
  }

  /**
   * Consumes and clears any pending cancellation for `taskThreadId`.
   */
  public consumeCancellation(taskThreadId: string | undefined): CancellationRequest | undefined {
    if (!taskThreadId) return undefined;
    const local = this.localCancellations.get(taskThreadId);
    const external = this.cancellationRegistry.consume(taskThreadId);
    this.clearPendingCancellation(taskThreadId);
    return local || external;
  }

  /**
   * Synthesize a compliant RunResult when execution was cancelled before or during retries.
   */
  public buildInterruptedResult(
    cancellation: CancellationRequest,
    baseResult?: RunResult,
  ): RunResult {
    const kind =
      cancellation.action === 'pause'
        ? 'paused'
        : cancellation.action === 'delete'
          ? 'deleted'
          : 'stopped';
    const interruption: TaskInterruption = {
      kind,
      reason: cancellation.reason,
      at: cancellation.at || new Date().toISOString(),
      by: cancellation.by,
    };
    const error =
      `Task ${cancellation.action} requested` +
      `${cancellation.by ? ` by ${cancellation.by}` : ''}: ${cancellation.reason}`;

    const stream: StreamAssessment = baseResult?.assessment?.stream || {
      chunkCount: 0,
      stdoutBytes: 0,
      stderrBytes: 0,
      maxGapMs: 0,
      gapThresholdMs: DEFAULT_SILENCE_TIMEOUT_MS,
      hasActualOutput: false,
      continuous: false,
    };

    return {
      exitCode: -1,
      stdout: baseResult?.stdout || '',
      stderr: baseResult?.stderr || '',
      extractedOutput: baseResult?.extractedOutput || '',
      actualSessionId: baseResult?.actualSessionId,
      durationMs: baseResult?.durationMs || 0,
      error,
      interruption,
      // Whatever the killed process already billed is still real spend.
      tokenUsage: baseResult?.tokenUsage,
      model: baseResult?.model,
      assessment: {
        success: false,
        errorCode: kind.toUpperCase(),
        exitCode: -1,
        stream,
        reason: cancellation.reason,
      },
    };
  }

  /**
   * Sleep for `ms` milliseconds, checking periodically (every 100ms) for cancellations.
   * Returns the CancellationRequest if cancelled, or undefined if the sleep completed.
   */
  public async cancellableSleep(
    ms: number,
    taskThreadId: string | undefined,
    initialTaskExisted?: boolean,
  ): Promise<CancellationRequest | undefined> {
    const pollInterval = 100;
    const deadline = Date.now() + ms;
    while (Date.now() < deadline) {
      const cancel = this.checkPendingCancellation(taskThreadId, initialTaskExisted);
      if (cancel) return cancel;
      const waitTime = Math.min(pollInterval, deadline - Date.now());
      if (waitTime <= 0) break;
      await new Promise((r) => setTimeout(r, waitTime));
    }
    return this.checkPendingCancellation(taskThreadId, initialTaskExisted);
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
    const model = this.resolveModel(agent, options.model);
    // Codex accepts `--model` only on some subcommands, so the model travels as
    // a global `-c model="..."` config override: it parses identically for a
    // fresh run and for `exec resume`, and cannot be mistaken for a positional.
    const codexModelArgs = model ? ['-c', `model="${model}"`] : [];
    const modelFlagArgs = model ? ['--model', model] : [];

    if (agent === 'codex') {
      const exec = resolvedPath || defaultBinary('codex');
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
      const exec = resolvedPath || defaultBinary('claude');
      // Two CLI realities shape this branch:
      //   1. The npm shim is a `.cmd`, so on Windows it is launched through
      //      cmd.exe. A multi-line prompt passed as an argument is cut at the
      //      first newline (cmd.exe treats it as a command separator): the
      //      agent silently receives only the first line. `-p` is a boolean
      //      flag, so the prompt travels on stdin instead.
      //   2. `--output-format json` prints one object when the process ends, so
      //      stdout stays empty for the entire run and the 10-minute silence
      //      watchdog used to kill healthy long tasks before their first byte.
      //      `stream-json` emits `system/init` immediately and keeps streaming
      //      assistant/tool events, which keeps the watchdog fed. Print mode
      //      requires `--verbose` for that format.
      const sharedArgs = [
        '-p',
        '--output-format',
        'stream-json',
        '--verbose',
        '--dangerously-skip-permissions',
        ...modelFlagArgs,
      ];
      // claude -p --output-format stream-json --verbose --resume <session>
      // claude -p --output-format stream-json --verbose --session-id <uuid> -n "<title>"
      const sessionArgs = isRework
        ? ['--resume', receiverSessionId]
        : ['--session-id', receiverSessionId, '-n', title || 'Task'];
      const tailArgs = [...sessionArgs, ...sharedArgs];
      // Only a Windows batch shim needs cmd.exe; a POSIX launcher runs directly.
      const wrap = needsShellWrapper(exec);
      return {
        command: wrap ? 'cmd.exe' : exec,
        args: wrap ? ['/c', exec, ...tailArgs] : tailArgs,
        useShell: false,
        stdinPayload: prompt,
      };
    }

    if (agent === 'antigravity') {
      const exec = resolvedPath || defaultBinary('agy');
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

    // Default generic fallback. A shell is only needed to unwrap a Windows
    // batch shim; on POSIX the resolved executable is spawned directly.
    const fallback = resolvedPath || agent;
    return {
      command: fallback,
      args: ['-p', prompt, ...modelFlagArgs],
      useShell: needsShellWrapper(fallback),
    };
  }

  /**
   * Executes the agent command with retry on transient startup/network errors.
   *
   * A network hiccup (e.g. DSH's `TRANSPORT: Connection error`) must not burn a
   * whole task round: the same prompt is replayed with exponential backoff.
   * A run that was paused/stopped/timed out is never retried — replaying it
   * would silently resurrect a process the user asked to kill.
   */
  public async execute(options: RunOptions): Promise<RunResult> {
    const maxRetries = options.maxRetries ?? 3;
    const baseBackoffMs = options.retryBackoffMs ?? 2000;
    const taskThreadId = options.taskThreadId;
    const initialTaskExisted =
      taskThreadId && this.taskStore ? Boolean(this.taskStore.getTask(taskThreadId)) : false;

    if (taskThreadId) {
      this.executingThreads.add(taskThreadId);
    }

    try {
      let attemptOptions = options;
      let attempt = 0;
      // Every attempt is billed, so the round's token usage is the SUM of all
      // attempts, not just the last one (transient-error replays cost real
      // money even when they fail). A retried or resumed round also carries
      // the spend of its previous execution, which the executor seeds here.
      let accumulatedUsage: TokenUsageMetrics | undefined = options.previousTokenUsage
        ? { ...options.previousTokenUsage }
        : undefined;
      while (true) {
        attempt++;

        // Pre-attempt cancellation check
        const preCancel = this.checkPendingCancellation(taskThreadId, initialTaskExisted);
        if (preCancel) {
          this.consumeCancellation(taskThreadId);
          const interrupted = this.buildInterruptedResult(preCancel);
          interrupted.tokenUsage = accumulatedUsage;
          return interrupted;
        }

        const result = await this.executeOnce(attemptOptions);
        accumulatedUsage = accumulateTokenUsage(accumulatedUsage, result.tokenUsage);
        const succeeded =
          result.exitCode === 0 && (!result.assessment || result.assessment.success);
        if (succeeded) {
          result.tokenUsage = accumulatedUsage;
          return result;
        }

        if (result.interruption) {
          result.tokenUsage = accumulatedUsage;
          return result;
        }

        // A hard-killed Claude run leaves its "this pid owns this session"
        // record behind, and the next `--resume` then dies in one second with
        // "Session ID <id> is already in use." Nothing is actually running, so
        // drop the stale record and replay the same session instead of failing
        // the whole round.
        const staleSessionId =
          options.agent === 'claude_code'
            ? parseInUseSessionId(`${result.stderr}\n${result.stdout}\n${result.error || ''}`)
            : undefined;
        if (attempt <= maxRetries && staleSessionId) {
          if (!attemptOptions.isRework) {
            // The id already has a transcript: reserving it again is what the CLI
            // rejects. Resume it instead — this is the shape of a round that was
            // dispatched as "fresh" after an executor switch but whose session had
            // in fact already been opened (and captured) by the previous attempt.
            attemptOptions = {
              ...attemptOptions,
              isRework: true,
              receiverSessionId: staleSessionId,
            };
            console.warn(
              `[AutonomousRunner] ${options.agent} reported session ${staleSessionId} as already in use; ` +
                `resuming that session instead of reserving it again.`,
            );
            continue;
          }
          // A rework already resumes, so the holder must be a dead process's
          // leftover ownership record.
          const removed = clearStaleClaudeSessionLocks(
            { sessionId: staleSessionId },
            this.homeDir,
          );
          console.warn(
            `[AutonomousRunner] ${options.agent} reported session ${staleSessionId} as already in use; ` +
              `cleared ${removed.length} stale ownership record(s).`,
          );
          if (removed.length > 0) continue;
        }

        if (attempt <= maxRetries && this.isRetryableFailure(result)) {
          const backoffMs = Math.min(baseBackoffMs * Math.pow(2, attempt - 1), 10_000);
          console.warn(
            `[AutonomousRunner] Transient startup/network error on attempt ${attempt}/${maxRetries} for ${options.agent}: "${result.error || result.assessment?.reason || result.stderr.trim()}". Retrying in ${backoffMs / 1000}s...`,
          );

          // Check cancellation before sleeping
          const earlyCancel = this.checkPendingCancellation(taskThreadId, initialTaskExisted);
          if (earlyCancel) {
            this.consumeCancellation(taskThreadId);
            const interrupted = this.buildInterruptedResult(earlyCancel, result);
            interrupted.tokenUsage = accumulatedUsage;
            return interrupted;
          }

          // Cancellable sleep: checks cancellation every 100ms
          const sleepCancel = await this.cancellableSleep(backoffMs, taskThreadId, initialTaskExisted);
          if (sleepCancel) {
            this.consumeCancellation(taskThreadId);
            const interrupted = this.buildInterruptedResult(sleepCancel, result);
            interrupted.tokenUsage = accumulatedUsage;
            return interrupted;
          }

          // Check cancellation after sleeping before continuing
          const postCancel = this.checkPendingCancellation(taskThreadId, initialTaskExisted);
          if (postCancel) {
            this.consumeCancellation(taskThreadId);
            const interrupted = this.buildInterruptedResult(postCancel, result);
            interrupted.tokenUsage = accumulatedUsage;
            return interrupted;
          }

          // A dead attempt may already have opened a native session. Continuing
          // that session keeps the work the receiver already did; starting a
          // second fresh session would redo the whole task in a new context.
          if (!attemptOptions.isRework && result.actualSessionId) {
            attemptOptions = {
              ...attemptOptions,
              isRework: true,
              receiverSessionId: result.actualSessionId,
            };
            console.log(
              `[AutonomousRunner] Retry for ${options.agent} resumes native session ${result.actualSessionId} instead of starting a fresh one.`,
            );
          }
          continue;
        }

        result.tokenUsage = accumulatedUsage;
        return result;
      }
    } finally {
      if (taskThreadId) {
        this.executingThreads.delete(taskThreadId);
      }
    }
  }

  /**
   * Decides whether a failed attempt deserves another try.
   *
   * Two shapes exist in the wild:
   *   1. the CLI dies with a non-zero code after a network error;
   *   2. the CLI swallows the network error, exits 0 and emits no final answer
   *      (DSH's `dsh: TRANSPORT: Connection error.` behaves this way).
   * Both are retried; a genuine empty answer or a stalled stream without any
   * transport evidence is not.
   */
  public isRetryableFailure(result: RunResult): boolean {
    // A paused/stopped/timed-out run was killed on purpose: replaying it would
    // resurrect work the user asked to abandon.
    if (result.interruption) return false;
    // Rate limits do not clear in seconds: retrying immediately just burns the
    // remaining quota. The caller decides when (and on which executor) to retry.
    if (result.assessment?.errorCode === 'RATE_LIMITED') return false;
    if (result.exitCode !== 0) return this.isTransientError(result);
    if (!result.assessment || result.assessment.success) return false;
    return this.isTransportFailure(result);
  }

  /** True when the captured output mentions a network/transport level failure. */
  public isTransportFailure(result: RunResult): boolean {
    const combined = `${result.stdout}\n${result.stderr}\n${result.error || ''}`.toLowerCase();
    return TRANSPORT_ERROR_PATTERNS.some((pattern) => combined.includes(pattern));
  }

  /**
   * Checks if an execution failed due to an ephemeral network or handshake error.
   */
  public isTransientError(result: RunResult): boolean {
    if (result.exitCode === 0) return false;
    const combined = `${result.stdout}\n${result.stderr}\n${result.error || ''}`.toLowerCase();
    const transientPatterns = [
      ...TRANSPORT_ERROR_PATTERNS,
      'eligibility check failed',
      'eof',
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
   *
   * Stream continuity is measured on a suspension-adjusted gap: time the HOST
   * was asleep/frozen is not the agent's silence (see `HostSuspensionGuard`).
   * A remaining long gap is still not enough to throw a finished round away —
   * with exit 0 and a real final answer it is downgraded to a
   * `STREAM_GAP_EXCEEDED` warning unless the caller asks for strict `fail`.
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
    /** Longest gap the agent is accountable for (host suspension excluded). */
    maxGapMs: number;
    /** Longest wall-clock gap, before host suspension was excluded. */
    rawMaxGapMs?: number;
    excludedSuspensionMs?: number;
    suspensionCount?: number;
    gapThresholdMs: number;
    interruption?: TaskInterruption;
    /**
     * Streaming CLIs (Codex/DSH/Antigravity) must keep emitting events. Claude's
     * `--output-format json` emits only one final object, so for it a single
     * substantive chunk counts as continuous.
     */
    expectStreaming?: boolean;
    /** Default `warn` (see `DEFAULT_STREAM_GAP_POLICY`). */
    streamGapPolicy?: StreamGapPolicy;
  }): ResponseAssessment {
    const finalOutput = this.extractAnswerOnly(input.stdout);
    const hasActualOutput = finalOutput.trim().length > 0;
    const expectStreaming = input.expectStreaming !== false;
    const gapExceeded =
      input.chunkCount > 0 && expectStreaming && input.maxGapMs > input.gapThresholdMs;
    const continuous = input.chunkCount > 0 && !gapExceeded;
    const gapPolicy = input.streamGapPolicy ?? DEFAULT_STREAM_GAP_POLICY;
    const excludedSuspensionMs = Math.max(0, input.excludedSuspensionMs ?? 0);
    const rawMaxGapMs = Math.max(input.rawMaxGapMs ?? input.maxGapMs, input.maxGapMs);

    const stream: StreamAssessment = {
      chunkCount: input.chunkCount,
      stdoutBytes: input.stdoutBytes,
      stderrBytes: input.stderrBytes,
      firstOutputAtMs: input.firstOutputAtMs,
      lastOutputAtMs: input.lastOutputAtMs,
      maxGapMs: input.maxGapMs,
      rawMaxGapMs,
      excludedSuspensionMs,
      suspensionCount: input.suspensionCount ?? 0,
      gapThresholdMs: input.gapThresholdMs,
      hasActualOutput,
      continuous,
      gapExceeded,
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

    if (gapExceeded) {
      const gapDetail =
        `longest output gap ${Math.round(input.maxGapMs)}ms exceeded the ` +
        `${input.gapThresholdMs}ms threshold` +
        (excludedSuspensionMs > 0
          ? ` (raw ${Math.round(rawMaxGapMs)}ms, ` +
            `${Math.round(excludedSuspensionMs)}ms of host suspension excluded)`
          : '');

      if (gapPolicy === 'warn') {
        // The process exited 0 AND produced a real answer: the round's work is
        // there, so the long gap is a warning for the reviewer, not a reason to
        // discard the round and re-run it.
        return {
          success: true,
          errorCode: 'OK',
          exitCode: input.exitCode,
          stream,
          warnings: [
            {
              code: 'STREAM_GAP_EXCEEDED',
              message:
                `Stream gap exceeded but the round still exited 0 with a real final ` +
                `answer: ${gapDetail}.`,
            },
          ],
        };
      }

      return {
        success: false,
        errorCode: 'STREAM_STALLED',
        exitCode: input.exitCode,
        stream,
        reason: `Stream stalled: ${gapDetail}.`,
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
      // Durations are measured on the monotonic clock: a wall-clock correction
      // (NTP / manual change) must not look like a multi-minute output gap.
      const startMono = monotonicNowMs();
      // Host sleep/hibernate/Modern Standby freezes this process together with
      // the agent child. Remember those windows so they are never charged to
      // the agent as silence (see HostSuspensionGuard).
      const suspension = new HostSuspensionGuard();
      suspension.start();
      const settle = (result: RunResult) => {
        try {
          suspension.stop();
        } catch {}
        resolve(result);
      };

      const { command, args, useShell, env: commandEnv, stdinPayload } = (() => {
        try {
          return this.buildCommand(options);
        } catch (err) {
          // A synchronous build failure rejects the promise: release the
          // heartbeat so it cannot outlive this attempt.
          suspension.stop();
          throw err;
        }
      })();

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
      let lastActivityMono = startMono;
      let watchdogTriggered = false;
      let maxTimerTriggered = false;

      // Response health metrics: process error code + real/continuous stream.
      let chunkCount = 0;
      let stdoutBytes = 0;
      let stderrBytes = 0;
      let firstOutputAtMs: number | undefined;
      let lastOutputAtMs: number | undefined;
      /** Longest gap the agent is accountable for (host suspension excluded). */
      let maxGapMs = 0;
      /** Longest wall-clock gap, kept for diagnostics. */
      let rawMaxGapMs = 0;
      /** Host-suspension time that was removed from the gap measurements. */
      let excludedSuspensionMs = 0;
      let lastChunkMono: number | undefined;

      const noteStreamChunk = (byteLength: number) => {
        // Sync first: after a resume the overdue heartbeat may not have been
        // processed yet, and the window would otherwise be missed.
        suspension.sync();
        const nowMono = monotonicNowMs();
        const elapsed = Math.max(0, nowMono - startMono);
        const previousMono = lastChunkMono ?? startMono;
        const rawGap = Math.max(0, nowMono - previousMono);
        const excluded = suspension.suspendedWithin(previousMono, nowMono);
        chunkCount++;
        rawMaxGapMs = Math.max(rawMaxGapMs, rawGap);
        excludedSuspensionMs += excluded;
        maxGapMs = Math.max(maxGapMs, Math.max(0, rawGap - excluded));
        if (firstOutputAtMs === undefined) firstOutputAtMs = elapsed;
        lastOutputAtMs = elapsed;
        lastChunkMono = nowMono;
        return byteLength;
      };

      const taskThreadId = options.taskThreadId;
      const initialTaskExisted =
        taskThreadId && this.taskStore ? Boolean(this.taskStore.getTask(taskThreadId)) : false;
      const preCancel = this.checkPendingCancellation(taskThreadId, initialTaskExisted);
      if (preCancel) {
        this.consumeCancellation(taskThreadId);
        return settle(this.buildInterruptedResult(preCancel));
      }

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
        return settle({
          exitCode: -1,
          stdout: '',
          stderr: String(err),
          extractedOutput: '',
          durationMs,
          error: `Failed to spawn process (${command}): ${err.message || err}`,
        });
      }

      // CRITICAL: Close stdin so no CLI waits on interactive input. Agents that
      // take their prompt on stdin (Claude Code) get the payload written first;
      // the error handler swallows EPIPE when the child exits early.
      if (child.stdin) {
        child.stdin.on('error', () => {});
        if (stdinPayload !== undefined) {
          try {
            child.stdin.write(stdinPayload, 'utf8');
          } catch {}
        }
        try {
          child.stdin.end();
        } catch {}
      }

      // Publish the real agent PID to the execution lease: it outlives a crashed
      // host on Windows, and the next round must not start on top of it.
      if (child.pid) {
        try {
          options.onChildPid?.(child.pid);
        } catch {}
        child.once('exit', () => {
          try {
            options.onChildPid?.(undefined);
          } catch {}
        });
      }

      // Cross-process pause/delete: a control plane running elsewhere writes a
      // cancellation file; this runner owns the child and is the only one that
      // can actually kill it.
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
        const local = this.localCancellations.get(taskThreadId);
        if (pending || local) {
          externalCancellation = pending || local;
          try {
            this.killProcess(child);
          } catch {}
        }

        cancellationPoll = setInterval(() => {
          if (isSettled || !taskThreadId) return;
          const currentLocal = this.localCancellations.get(taskThreadId);
          const request = currentLocal || this.cancellationRegistry.consume(taskThreadId);
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

      // Silence Watchdog. Silence is measured on the suspension-adjusted clock:
      // a host that slept for an hour was never going to produce output, so the
      // round must not be killed the moment the machine wakes up.
      const silenceInterval = setInterval(() => {
        if (isSettled) {
          clearInterval(silenceInterval);
          return;
        }
        suspension.sync();
        const nowMono = monotonicNowMs();
        const silentDuration = Math.max(
          0,
          nowMono - lastActivityMono - suspension.suspendedWithin(lastActivityMono, nowMono),
        );
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

      // Overall timeout, counted in ACTIVE runtime: time the host spent asleep
      // does not consume the task's execution budget.
      const timeoutCheckMs = Math.min(5_000, Math.max(250, Math.floor(timeoutMs / 10)));
      const maxTimer = setInterval(() => {
        if (isSettled) {
          clearInterval(maxTimer);
          return;
        }
        suspension.sync();
        const nowMono = monotonicNowMs();
        const activeElapsedMs =
          nowMono - startMono - suspension.suspendedWithin(startMono, nowMono);
        if (activeElapsedMs >= timeoutMs) {
          clearInterval(maxTimer);
          maxTimerTriggered = true;
          console.warn(
            `[AutonomousRunner] Overall timeout (${Math.round(timeoutMs / 1000)}s of active ` +
              `runtime, host suspension excluded) reached for ${options.agent}. Terminating.`,
          );
          try {
            this.killProcess(child);
          } catch {}
        }
      }, timeoutCheckMs);

      // Fail fast on a provider rate limit: the CLI would otherwise retry for
      // minutes with 30-40s backoff while the quota is already exhausted.
      //
      // stdout is line-buffered and only STRUCTURED provider errors are matched:
      // an agent transcript quotes source files and test logs, and a task that
      // implements rate limiting prints its own "RATE_LIMITED" error code —
      // scanning raw stdout killed those rounds as if the provider had refused.
      // stderr stays free-text: that is where the CLI reports its own failures.
      let rateLimitSignal: string | undefined;
      let stdoutPartialLine = '';
      let stderrPartialLine = '';
      const MAX_PARTIAL_LINE = 1 << 20; // 1 MiB guard against a newline-less stream
      const abortOnRateLimit = (hit: string) => {
        if (rateLimitSignal || isSettled) return;
        rateLimitSignal = hit;
        console.warn(
          `[AutonomousRunner] Provider rate limit detected (${hit}) for ${options.agent}; ` +
            `aborting instead of waiting out its internal retries.`,
        );
        try {
          this.killProcess(child);
        } catch {}
      };
      const scanLines = (
        chunk: string,
        previous: string,
        onLine: (line: string) => string | undefined,
      ): string => {
        if (rateLimitSignal || isSettled) return previous;
        const lines = (previous + chunk).split(/\r?\n/);
        let rest = lines.pop() ?? '';
        if (rest.length > MAX_PARTIAL_LINE) rest = rest.slice(-MAX_PARTIAL_LINE);
        for (const line of lines) {
          if (!line.trim()) continue;
          const hit = onLine(line);
          if (hit) {
            abortOnRateLimit(hit);
            break;
          }
        }
        return rest;
      };

      child.stdout?.on('data', (data) => {
        lastActivityMono = monotonicNowMs();
        stdoutBytes += data.length;
        noteStreamChunk(data.length);
        const str = data.toString();
        stdoutData += str;
        stdoutPartialLine = scanLines(str, stdoutPartialLine, detectStructuredRateLimitSignal);
        if (options.onStdoutChunk) {
          options.onStdoutChunk(str);
        }
      });

      child.stderr?.on('data', (data) => {
        lastActivityMono = monotonicNowMs();
        stderrBytes += data.length;
        noteStreamChunk(data.length);
        const str = data.toString();
        stderrData += str;
        stderrPartialLine = scanLines(str, stderrPartialLine, detectRateLimitSignal);
        if (options.onStderrChunk) {
          options.onStderrChunk(str);
        }
      });

      child.on('error', (err) => {
        if (isSettled) return;
        isSettled = true;
        clearInterval(silenceInterval);
        clearInterval(maxTimer);
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
        settle({
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
        // Pick up a suspension window that ended right at exit time (e.g. the
        // machine was asleep when the child's last output arrived).
        suspension.sync();
        clearInterval(silenceInterval);
        clearInterval(maxTimer);
        // A provider error printed without a trailing newline still has to be
        // recognised; the process is gone, so record it instead of aborting.
        if (!rateLimitSignal && stdoutPartialLine) {
          rateLimitSignal = detectStructuredRateLimitSignal(stdoutPartialLine) ?? rateLimitSignal;
        }
        const cancellation = takeCancellation();
        const durationMs = Date.now() - startTime;
        const extracted = this.extractCleanOutput(stdoutData);
        const actualSessionId = this.extractSessionId(stdoutData);
        // A killed CLI never unregisters itself: clean its ownership record so
        // the next resume is not rejected as "already in use". Live processes
        // are never touched by the helper.
        if (options.agent === 'claude_code') {
          clearStaleClaudeSessionLocks({ pid: child.pid }, this.homeDir);
          if (actualSessionId) {
            clearStaleClaudeSessionLocks({ sessionId: actualSessionId }, this.homeDir);
          }
        }
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

        let assessment = this.evaluateResponse({
          exitCode,
          stdout: stdoutData,
          stderr: stderrData,
          chunkCount,
          stdoutBytes,
          stderrBytes,
          firstOutputAtMs,
          lastOutputAtMs,
          maxGapMs,
          rawMaxGapMs,
          excludedSuspensionMs,
          suspensionCount: suspension.suspensionCount,
          gapThresholdMs: silenceTimeoutMs,
          interruption,
          // Every agent now emits a live event stream (Claude Code runs with
          // `--output-format stream-json`), so a silent gap means a real stall.
          expectStreaming: true,
          streamGapPolicy: options.streamGapPolicy,
        });
        if (rateLimitSignal) {
          // We killed the process on purpose: report the real cause instead of
          // the exit code the kill produced.
          assessment = {
            ...assessment,
            success: false,
            errorCode: 'RATE_LIMITED',
            reason:
              `Provider rate limit hit (${rateLimitSignal}); run aborted after ` +
              `${Math.round(durationMs / 1000)}s instead of exhausting the CLI's internal retries.`,
          };
          error = assessment.reason;
        }
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

        // Token accounting: parse the buffered stdout once, at exit. Streaming
        // parsing would only add half-line JSON handling and double counting
        // risk — the whole stream is already in memory, even for a round that
        // was killed.
        const effectiveModel = this.resolveModel(options.agent, options.model);
        const tokenUsage = collectTokenUsage({
          agent: options.agent,
          stdout: stdoutData,
          finalOutput: extracted,
          effectiveModel,
          requestedModel: options.model,
          interrupted: !!interruption,
        });

        settle({
          exitCode,
          stdout: stdoutData,
          stderr: stderrData,
          extractedOutput: extracted || stdoutData.trim(),
          actualSessionId,
          durationMs,
          error,
          interruption,
          assessment,
          tokenUsage,
          model: effectiveModel,
        });
      });
    });
  }

  private killProcess(child: child_process.ChildProcess): void {
    if (!child.pid) return;
    killProcessTree(child);
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
          // Claude Code (`--output-format stream-json --verbose`): the answer is
          // the terminal `result` event. Assistant events repeat the same text
          // inside `message.content[]`, which is the only text available when a
          // run is killed before the result event arrives.
          if (
            parsed.type === 'result' &&
            typeof parsed.result === 'string' &&
            parsed.result.trim()
          ) {
            return parsed.result;
          }
          if (parsed.type === 'assistant' && Array.isArray(parsed.message?.content)) {
            const text = (parsed.message.content as any[])
              .filter((block) => block && block.type === 'text' && typeof block.text === 'string')
              .map((block) => block.text as string)
              .join('');
            if (text.trim()) return text;
          }
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
          if (typeof parsed.message === 'string') return parsed.message;
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
        if (typeof parsed.message === 'string') return parsed.message;
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
