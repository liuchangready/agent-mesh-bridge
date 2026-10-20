/**
 * Data contracts and core types for Agent Mesh Bridge.
 */

export type AgentId =
  | 'antigravity'
  | 'claude_code'
  | 'codex'
  | 'deepseek_harness'
  | 'vscode_chat';

export type TargetAgentSelection = 'default' | AgentId;

export type ThreadStatus =
  | 'queued'
  | 'running'
  | 'waiting_review'
  | 'reworking'
  | 'paused'
  | 'completed'
  | 'failed';

/**
 * Why a task/round stopped executing before it could finish normally.
 *
 * `paused` is user/agent requested and resumable; `deleted` means the ledger
 * entry was removed; `timeout`/`watchdog` are automatic process guards;
 * `stopped` covers any other explicit stop.
 */
export type InterruptionKind = 'paused' | 'stopped' | 'deleted' | 'timeout' | 'watchdog' | 'error';

export interface TaskInterruption {
  kind: InterruptionKind;
  reason: string;
  at: string; // ISO8601
  by?: string; // 'user', 'agent', 'supervisor', watchdog, ...
}

export interface StreamAssessment {
  /** Number of stdout/stderr data chunks received. */
  chunkCount: number;
  stdoutBytes: number;
  stderrBytes: number;
  firstOutputAtMs?: number;
  lastOutputAtMs?: number;
  /** Longest gap between two output chunks (or spawn -> first chunk). */
  maxGapMs: number;
  gapThresholdMs: number;
  /** A non-empty final answer was extracted from stdout. */
  hasActualOutput: boolean;
  /** Chunks arrived and no gap exceeded the threshold. */
  continuous: boolean;
}

export interface ResponseAssessment {
  /** exitCode === 0 AND actual output existed AND the stream was continuous. */
  success: boolean;
  /** Stable response status code, e.g. OK / NONZERO_EXIT / EMPTY_OUTPUT / STREAM_STALLED. */
  errorCode: string;
  exitCode: number;
  stream: StreamAssessment;
  reason?: string;
}

/**
 * Merge-request handoff recorded by the caller/reviewer after a task (or a
 * batch of tasks) has been accepted. The bridge never creates the MR itself;
 * the caller's agent does the git/gh/glab work and records the result here.
 */
export type MergeRequestStatus = 'created' | 'pushed' | 'synced' | 'failed';

export interface MergeRequestInfo {
  url?: string;
  branch?: string;
  commit?: string;
  remote?: string;
  baseBranch?: string;
  title?: string;
  status: MergeRequestStatus;
  summary?: string;
  updatedAt: string;
  by?: string;
}

/**
 * Environment markers injected into every agent process launched by the bridge.
 *
 * A delegated task's receiver is a normal agent process that also spawns this
 * bridge's MCP server (it inherits its environment). Marking the process lets
 * that server refuse to delegate further work: task receivers execute, they do
 * not fan work out to other agents.
 */
export const MESH_TASK_RECEIVER_ENV = 'MESH_TASK_RECEIVER';
export const MESH_TASK_THREAD_ENV = 'MESH_TASK_THREAD_ID';
export const MESH_TASK_ROUND_ENV = 'MESH_TASK_ROUND';

/** True when the current process belongs to a delegated task's receiver. */
export function isTaskReceiverProcess(env: NodeJS.ProcessEnv = process.env): boolean {
  return env[MESH_TASK_RECEIVER_ENV] === '1';
}

export type RoundType = 'initial' | 'rework' | 'approval';

export type RoundStatus = 'running' | 'paused' | 'completed' | 'failed';

export interface DelegationRound {
  round: number; // Round number: 1, 2, 3...
  type: RoundType;
  prompt: string; // Instruction or feedback given by caller
  output?: string; // Output returned by receiver (updated live every 30s)
  error?: string; // Error description if failed
  durationMs: number; // Duration in milliseconds (updated live every 30s)
  status: RoundStatus;
  timestamp: string; // ISO8601 timestamp
  /** Set when this round was paused/stopped/timed out before normal completion. */
  interruption?: TaskInterruption;
  /** Two-part response health: error code + stream continuity/output. */
  assessment?: ResponseAssessment;
}

export interface ThreadSummary {
  lastPromptSnippet: string; // Caller's latest prompt preview (first 100 chars)
  lastOutputSnippet: string; // Receiver's latest output preview (first 150 chars)
  totalDurationMs: number; // Cumulative duration in ms
}

export interface DelegationThread {
  threadId: string; // e.g. "thread_20261016_163000_a7f9"
  title: string; // Brief title, e.g. "Fix TCP cursor drift"
  initiatorAgent: AgentId; // Initiator (Caller A)
  targetAgent: AgentId; // Receiver (Target B)
  status: ThreadStatus; // Lifecycle status ('queued' while waiting for the one global execution slot or an exact same-file conflict)
  currentRound: number; // Current round (1: initial, 2+: rework)
  createdAt: string; // ISO8601 creation timestamp
  updatedAt: string; // ISO8601 update timestamp

  // Native session bindings for continuous rework resume
  receiverSessionId: string; // Native session ID for target agent
  /**
   * True once the receiver's real native session is known: either an agent
   * emitted its native id, or Claude's preassigned `--session-id` is the native
   * session. A paused initial round can only be resumed in-session when true.
   */
  receiverSessionCaptured?: boolean;
  callerSessionId?: string; // Caller session ID for reactive wake-up callbacks
  workspacePath: string; // Working directory

  /** Latest pause/stop/interruption, if any. Cleared when execution resumes. */
  interruption?: TaskInterruption;
  /** Full interruption history, newest last. */
  interruptions?: TaskInterruption[];

  /** MR handoff recorded by the caller after the task/batch is accepted. */
  mergeRequest?: MergeRequestInfo;

  // File scope for conflict detection and queueing
  expectedFiles?: string[]; // Concrete code files (directories/globs are rejected at dispatch)

  // Summary for fast sidebar rendering
  summary: ThreadSummary;

  // Complete round history
  rounds: DelegationRound[];
}

export interface DispatchTaskParams {
  targetAgent?: TargetAgentSelection;
  title: string;
  prompt: string;
  callerSessionId?: string;
  initiatorAgent?: AgentId;
  workspacePath?: string;
  /**
   * Concrete code file paths expected to be modified. Directories and glob
   * patterns are rejected so the lock scope stays per-file.
   */
  expectedFiles?: string[];
}

export type DispatchStatus = 'DISPATCHED' | 'QUEUED';

export interface DispatchResult {
  threadId: string;
  receiverSessionId: string;
  targetAgent: AgentId;
  status: DispatchStatus;
  message: string;
}

export interface RequestReworkParams {
  threadId: string;
  feedback: string;
  callerSessionId?: string;
}

export interface RequestReworkResult {
  threadId: string;
  round: number;
  receiverSessionId: string;
  targetAgent: AgentId;
  status: 'REWORK_DISPATCHED';
  message: string;
}

export interface ApproveTaskParams {
  threadId: string;
  remarks?: string;
}

export interface ApproveResult {
  threadId: string;
  status: 'COMPLETED';
  message: string;
}

export type AgentStatusType = 'online' | 'offline' | 'unauthenticated' | 'exhausted';

export interface RateLimitInfo {
  isExhausted: boolean;
  resetsAt?: string; // ISO8601 string
  resetsAtFormatted?: string; // e.g. "18:45:00 (in 32m)"
  remainingSeconds?: number; // Seconds until recovery
  reason?: string; // e.g. "Rate limit reached (429)"
  lastUpdated: string;
}

export interface AgentInfo {
  id: AgentId;
  name: string;
  status: AgentStatusType;
  available: boolean;
  executablePath?: string;
  version?: string;
  authDetails?: string;
  statusDetails?: string;
  rateLimit?: RateLimitInfo;
  lastChecked?: string;
}

export interface AgentMeshStatus {
  activeAgent: AgentId;
  resolvedMode: 'auto' | 'manual';
  agents: Record<AgentId, AgentInfo>;
  totalLedgerTasks: number;
}

/**
 * Validates whether a given string is a genuine session ID/UUID rather than an agent identifier or dummy value.
 */
export function isValidSessionId(sessionId?: string | null): boolean {
  if (!sessionId || typeof sessionId !== 'string') return false;
  const trimmed = sessionId.trim().toLowerCase();
  const invalidLiterals = [
    'claude',
    'claude_code',
    'antigravity',
    'agy',
    'codex',
    'deepseek_harness',
    'deepseek-harness',
    'deepseek',
    'dsh',
    'harness',
    'default',
    'null',
    'undefined',
    'none',
    'unknown',
    'vscode_chat',
  ];
  if (invalidLiterals.includes(trimmed)) {
    return false;
  }
  // Session IDs across Claude, Antigravity, and Codex are UUIDs or alphanumeric hashes of at least 8 characters
  if (trimmed.length < 8) {
    return false;
  }
  return true;
}

/**
 * Single-round-in-flight invariant.
 *
 * A delegation thread may have AT MOST ONE round in flight (`status: 'running'`)
 * at any time: the next round must never start before the previous one has been
 * finalized. Overlapping rounds used to make two agent processes drive the same
 * workspace concurrently (two Gradle builds corrupting each other's class files
 * and test results) and left duplicate round numbers in the ledger.
 */

/** Every round currently in flight, in ledger order. */
export function getInFlightRounds(thread: DelegationThread): DelegationRound[] {
  return (thread.rounds || []).filter((r) => r.status === 'running');
}

/**
 * The one round allowed to be in flight, or `undefined` when the thread is idle.
 *
 * Legacy ledgers may already hold several `running` rounds; the highest-numbered
 * one is the live round, so later rounds win deterministically.
 */
export function findInFlightRound(thread: DelegationThread): DelegationRound | undefined {
  const running = getInFlightRounds(thread);
  if (running.length === 0) return undefined;
  // `>=` keeps the most recently appended entry when numbers tie.
  return running.reduce((a, b) => ((b.round || 0) >= (a.round || 0) ? b : a));
}

/**
 * Next round number for a thread.
 *
 * Derived from the highest existing round rather than `currentRound + 1`, so a
 * retry that rewinds `currentRound` can never mint a duplicate round number.
 */
export function nextRoundNumber(thread: DelegationThread): number {
  const highest = (thread.rounds || []).reduce((max, r) => Math.max(max, r.round || 0), 0);
  return Math.max(highest, thread.currentRound || 0) + 1;
}

/**
 * Record an interruption on a thread without duplicating the same event when
 * both the control plane and the executing process report it.
 */
export function appendInterruption(
  thread: DelegationThread,
  interruption: TaskInterruption,
): void {
  thread.interruption = interruption;
  const history = thread.interruptions || [];
  const last = history[history.length - 1];
  const sameEvent =
    !!last &&
    last.kind === interruption.kind &&
    last.reason === interruption.reason &&
    Math.abs(new Date(last.at).getTime() - new Date(interruption.at).getTime()) < 15_000;
  if (!sameEvent) {
    history.push(interruption);
  }
  thread.interruptions = history;
}

/**
 * Whether a round must continue in the receiver's existing native session.
 *
 * Rework rounds always resume. A paused/retried *initial* round resumes only
 * when the native session was already captured; otherwise it must be launched
 * as a fresh initial run (Codex/DSH cannot adopt an unknown id).
 */
export function shouldResumeNativeSession(
  thread: DelegationThread,
  round: DelegationRound,
): boolean {
  if (round.type !== 'initial') return true;
  return !!thread.receiverSessionCaptured;
}

/**
 * Task-level queue occupancy.
 *
 * A task occupies the one global task slot until it reaches a terminal state:
 * `completed` (approved) or `failed`. `waiting_review` and `paused` are still
 * unfinished tasks and MUST block every other task, not just developer rounds.
 * `failed` is terminal for queue purposes so old failures cannot block the mesh
 * forever; retrying it creates an unfinished task again.
 */
export function isTaskUnfinished(thread: DelegationThread): boolean {
  return thread.status !== 'completed' && thread.status !== 'failed';
}

/** Oldest unfinished task (the queue head), or undefined when the mesh is idle. */
export function findQueueHead(tasks: DelegationThread[]): DelegationThread | undefined {
  return tasks
    .filter(isTaskUnfinished)
    .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime())[0];
}

/** Standard refusal text for every path that would start a second round. */
export function describeRoundInFlight(thread: DelegationThread, inFlight: DelegationRound): string {
  return (
    `Round ${inFlight.round} of task ${thread.threadId} is still running; ` +
    `a task may only have one round in flight at a time. ` +
    `Wait for round ${inFlight.round} to finish, or finalize it (retry/resume) before starting another round.`
  );
}
