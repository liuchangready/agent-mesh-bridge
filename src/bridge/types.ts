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

/** How the sender/receiver model recorded on a task was determined. */
export type ModelResolutionSource = 'explicit' | 'env' | 'configured' | 'none';

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

/**
 * True when a human stopped the round on purpose.
 *
 * A pause/stop/delete is a decision, not a failure: waking the caller to tell it
 * "your task failed" would be misleading. Timeouts, watchdogs and process errors
 * are system-caused and do deserve a failure wake-up.
 */
export function isUserInterruption(interruption?: TaskInterruption): boolean {
  return (
    interruption?.kind === 'paused' ||
    interruption?.kind === 'stopped' ||
    interruption?.kind === 'deleted'
  );
}

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
  /**
   * Longest gap between two output chunks (or spawn -> first chunk) that the
   * agent is actually accountable for: host suspension windows (sleep,
   * hibernate, Modern Standby, a frozen extension host) are subtracted.
   */
  maxGapMs: number;
  /**
   * Longest wall-clock gap before host-suspension time was excluded. Kept for
   * diagnostics so the ledger still shows that the clock jumped.
   */
  rawMaxGapMs?: number;
  /** Host-suspension time subtracted from the gap measurements. */
  excludedSuspensionMs?: number;
  /** How many host suspension windows were detected during the run. */
  suspensionCount?: number;
  gapThresholdMs: number;
  /** A non-empty final answer was extracted from stdout. */
  hasActualOutput: boolean;
  /** The (suspension-adjusted) gap stayed within the threshold. */
  continuous: boolean;
  /** The suspension-adjusted gap exceeded the threshold. */
  gapExceeded?: boolean;
}

/** A non-fatal problem worth surfacing to the reviewer. */
export interface AssessmentWarning {
  /** Stable machine-readable code, e.g. STREAM_GAP_EXCEEDED. */
  code: string;
  message: string;
}

/**
 * Severity of a long output gap once the process exited 0 with a real answer.
 * `warn` keeps the round successful and records an assessment warning;
 * `fail` restores the strict `STREAM_STALLED` verdict.
 */
export type StreamGapPolicy = 'warn' | 'fail';

/**
 * A gap is only proof of a stall when the round ALSO failed to deliver. If the
 * process exited 0 and produced an actual final answer, its work must not be
 * discarded because the host slept through the middle of the stream.
 */
export const DEFAULT_STREAM_GAP_POLICY: StreamGapPolicy = 'warn';

export interface ResponseAssessment {
  /** exitCode === 0 AND actual output existed AND no unattributable stall. */
  success: boolean;
  /** Stable response status code, e.g. OK / NONZERO_EXIT / EMPTY_OUTPUT / STREAM_STALLED. */
  errorCode: string;
  exitCode: number;
  stream: StreamAssessment;
  reason?: string;
  /** Non-fatal findings (e.g. a long gap that was downgraded to a warning). */
  warnings?: AssessmentWarning[];
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

/**
 * How an archived round's hot-storage summary was produced.
 *
 * `agent_protocol_envelope`: the receiver itself emitted a `<context_compaction>`
 * block while finishing a later round (semantic, preferred).
 * `deterministic_truncation`: the agent did not comply, so the bridge kept a
 * bounded head/tail excerpt plus a pointer to the full archive (always works).
 */
export type CompactionSource = 'agent_protocol_envelope' | 'deterministic_truncation';

/** Bounded summary that replaces a bulky historical round in the hot ledger. */
export interface CompactedContextSummary {
  /** 1-2 sentences describing what that round decided. */
  headline: string;
  /** Files the round actually changed (git-probe measured, not self-reported). */
  touchedFiles: string[];
  /** Problems that a later round already resolved. */
  resolvedIssues: string[];
  /** Long-lived constraints later rounds must respect. */
  longTermConstraints: string[];
  /** Absolute path of the exact pre-compaction output archive. */
  rawOutputPath?: string;
  /** Absolute path of the round's raw stdio execution log, when one exists. */
  executionLogPath?: string;
  metrics: {
    originalChars: number;
    compactedChars: number;
    /** compactedChars / originalChars, 0..1 (0 when originalChars is 0). */
    compressionRatio: number;
    compactedAt: string;
    source: CompactionSource;
  };
}

/**
 * Where a round's token numbers came from.
 *
 * `native_reported`: parsed from the receiver CLI's own usage payload.
 * `estimated`: the CLI reported nothing usable, so only the output was guessed.
 * `unavailable`: nothing at all could be measured (the numbers are 0, which
 * does NOT mean "free").
 */
export type TokenMetricSource = 'native_reported' | 'estimated' | 'unavailable';

/**
 * Where a cost figure came from.
 *
 * Only `native` is trustworthy in v1: it is the CLI's own figure (for Claude
 * that is an API-equivalent cost, not necessarily what the user paid). There is
 * no price-table estimation yet, because a stale price table is worse than no
 * number at all.
 */
export type TokenCostSource = 'native' | 'none';

/**
 * Normalized token accounting for one round.
 *
 * The four receivers report usage in incompatible shapes: Codex and Antigravity
 * report an `input` that ALREADY INCLUDES cache reads, Claude and DSH report an
 * `input` that EXCLUDES them. A single `input + output` formula therefore
 * under-counts DSH by ~182x and over-counts Antigravity by ~8%. These fields
 * remove the ambiguity:
 *
 *   promptTokens = uncachedInputTokens + cacheReadTokens + cacheWriteTokens
 *   totalTokens  = promptTokens + outputTokens
 *
 * `reasoningTokens` is a SUBSET of `outputTokens` (the CLIs count thinking
 * inside the output) and is only ever displayed, never added again.
 */
export interface TokenUsageMetrics {
  /** Prompt tokens that missed the provider's cache. */
  uncachedInputTokens: number;
  /** Prompt tokens served from the provider's cache. */
  cacheReadTokens: number;
  /** Prompt tokens written into the provider's cache. */
  cacheWriteTokens: number;
  /** Full prompt = uncached + cacheRead + cacheWrite. */
  promptTokens: number;
  /** Generated tokens (thinking included when the CLI counts it there). */
  outputTokens: number;
  /** Reasoning/thinking tokens; a subset of `outputTokens`. */
  reasoningTokens?: number;
  /** promptTokens + outputTokens (this bridge's canonical number). */
  totalTokens: number;
  /** The CLI's own total, kept only for cross-checking. */
  nativeTotalTokens?: number;
  /** The agent that executed this round. */
  agent: AgentId;
  /** Effective model (CLI-reported first, otherwise the resolved model). */
  model?: string;
  /** Raw model string as reported by the CLI (may be an alias). */
  reportedModel?: string;
  /** Model the bridge asked for (`thread.targetModel`). */
  requestedModel?: string;
  /** Native cost in USD, when the CLI reports one. */
  costUsd?: number;
  costSource?: TokenCostSource;
  source: TokenMetricSource;
  /** False when nothing could be measured at all (the zeros are not "free"). */
  available: boolean;
  /** True when the numbers are incomplete (interrupted, estimated, missing). */
  partial?: boolean;
  /** How many process attempts this metric covers. */
  attempts?: number;
  /** Human-readable explanation of what is missing and why. */
  notes?: string;
  /** ISO8601 collection timestamp. */
  collectedAt: string;
}

/**
 * Task-level rollup, DERIVED from the rounds on every read.
 *
 * It is never persisted: a stored aggregate can be rolled back by a stale
 * process writing an old copy of the task, which is exactly the failure mode
 * that once resurrected compacted output.
 */
export interface ThreadTokenUsageSummary {
  totalUncachedInputTokens: number;
  totalCacheReadTokens: number;
  totalCacheWriteTokens: number;
  totalPromptTokens: number;
  totalOutputTokens: number;
  totalTokens: number;
  /** Sum of natively reported costs, when any round had one. */
  totalCostUsd?: number;
  costSource?: TokenCostSource;
  /** totalTokens per executing agent. */
  byAgent: Partial<Record<AgentId, number>>;
  /** totalTokens per model. */
  byModel: Record<string, number>;
  /** True when any round is estimated, partial or unmeasured. */
  isPartialOrEstimated: boolean;
  /** Rounds that actually produced a measurement. */
  measuredRounds: number;
  /** Rounds in the thread. */
  totalRounds: number;
}

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
  /**
   * Files this round actually changed (workspace-relative), measured with a git
   * status probe bracketing the run. Complements the caller's declared
   * `expectedFiles`, which may be missing or incomplete.
   */
  touchedFiles?: string[];
  /**
   * Set when this round's instruction is byte-identical to an earlier round's.
   *
   * A caller that wants "run the same task again" has no retry lever in the MCP
   * surface, so it historically re-sent the original prompt through
   * `request_rework`; the ledger then showed two rounds that looked like two
   * identical tasks. Recording the origin makes the repeat explicit instead of
   * making it look like a duplicate dispatch.
   */
  repeatOfRound?: number;
  /**
   * True once a later round archived this round's bulky output.
   *
   * The lagged policy is: the current round stays 100% raw for the reviewer,
   * and only the previous round (whose exploration the current round has
   * already consumed) is condensed. The exact original text survives in
   * `rawOutputFilePath`, so compaction never destroys data.
   */
  isCompacted?: boolean;
  /** Structured capsule backing the condensed `output` card. */
  compactSummary?: CompactedContextSummary;
  /** Absolute path of the archived pre-compaction output (cold storage). */
  rawOutputFilePath?: string;
  /** UTF-8 byte length of the output before compaction, for UI/metrics. */
  outputBytes?: number;
  /**
   * The agent that actually executed this round.
   *
   * Recorded independently of the token metrics: after an executor switch the
   * task only knows its CURRENT agent, so without this the ledger cannot say
   * which round ran where. `retargets[]` only carries timestamps.
   */
  executorAgent?: AgentId;
  /** The model that actually executed this round. */
  executorModel?: string;
  /**
   * Token accounting for this round.
   *
   * Structured execution metadata: like `assessment` and `touchedFiles` it must
   * survive the lagged context compaction (the compactor may only rewrite
   * `output`).
   */
  tokenUsage?: TokenUsageMetrics;
}

export interface ThreadSummary {
  lastPromptSnippet: string; // Caller's latest prompt preview (first 100 chars)
  lastOutputSnippet: string; // Receiver's latest output preview (first 150 chars)
  totalDurationMs: number; // Cumulative duration in ms
}

/**
 * Metadata for a task that ran in a temporary git worktree.
 *
 * `mergePending` means "this task produced work that is not on the main line
 * yet": it is set once the task completes without a merge request and cleared
 * when the caller records one. Nothing is auto-merged in this phase.
 */
export interface WorktreeInfo {
  /** Absolute path of the temporary worktree the agent edited. */
  path: string;
  /** Absolute path of the main checkout the worktree belongs to. */
  mainPath: string;
  /** Branch the worktree was based on / will be merged from. */
  branch?: string;
  /** Commit the worktree was created from. */
  baseCommit?: string;
  /** True while the worktree's output has no recorded merge request. */
  mergePending?: boolean;
}

/**
 * Caller-supplied worktree declaration on `dispatch_task`.
 *
 * `path` is not repeated here: it is exactly the task's `workspacePath`, so the
 * two can never disagree.
 */
export interface DispatchWorktreeSpec {
  /** Main checkout the temporary worktree must eventually merge back into. */
  mainPath: string;
  /** Branch the worktree was created from (recorded for the merge audit). */
  branch?: string;
  /** Commit the worktree was created from (recorded for the merge audit). */
  baseCommit?: string;
}

/**
 * A failed attempt to wake the initiator (reviewer) for a finished round.
 *
 * The receiver round finishing does not imply the review happened: the wake
 * spawns the caller's CLI, and that process can die (quota, expired session,
 * provider outage) before it calls `approve_task` / `request_rework`. Without a
 * record, the task would sit in `waiting_review` forever with no trace of why.
 * This is that trace, plus the plan for the automatic retry.
 */
export interface ReviewWakeState {
  /** How the stall was detected. */
  kind?: 'wake_failed' | 'timeout';
  /** Round the reviewer still has to inspect. */
  round: number;
  /** Failed wake attempts since the last successful one. */
  attempts: number;
  lastAttemptAt: string;
  /** First line of the caller's output, or the spawn error. */
  lastError: string;
  /** Process exit code, when there was one. */
  exitCode?: number;
  /** The caller's agent (the one whose quota can be exhausted). */
  agent?: AgentId;
  /** True when the failure text looked like a 429 / quota signal. */
  quotaExhausted?: boolean;
  quotaResetsAt?: string;
  quotaResetsAtFormatted?: string;
  quotaReason?: string;
  /** When the bridge will try again on its own (ISO8601). */
  autoRetryAt?: string;
  /** Automatic attempts already spent on this failure. */
  autoRetryAttempts?: number;
  /** True once automatic retries stopped; the user must act. */
  gaveUp?: boolean;
}

/**
 * A review parked on a dependency instead of a rework round.
 *
 * `waitingForThreadId` is the sibling task whose delivery unblocks this review;
 * it may be absent when the dependency is external (a human, a remote system),
 * in which case only an explicit unblock clears it.
 */
export interface ReviewBlockInfo {
  waitingForThreadId?: string;
  /** Why the review cannot conclude yet (reviewer's own words, bounded). */
  reason?: string;
  at: string;
  by: string;
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

  /**
   * Model the sender (initiator) was running when it dispatched the task.
   *
   * The bridge cannot always introspect another agent's live session, so this is
   * best-effort: an explicit `initiatorModel`, then an agent-specific env hint,
   * then the initiator's configured default model. `initiatorModelSource` says
   * which of those produced the value.
   */
  initiatorModel?: string;
  initiatorModelSource?: ModelResolutionSource;

  /**
   * Model the receiver (target agent) runs this task with, resolved at dispatch
   * from the explicit `model` argument or the target agent's configured default.
   * Pinned on the task so every rework/retry round keeps one model instead of
   * silently drifting to the agent's current default.
   */
  targetModel?: string;

  /**
   * Why the caller's requested model was not applied (guessed model ignored, or
   * the user kept the default). Kept on the task so the decision stays auditable.
   */
  modelNotice?: string;

  /** User decision recorded when switching away from the configured default. */
  modelApproval?: ModelApprovalRecord;

  /**
   * Executor switches (agent and/or model) applied to this task after dispatch.
   * Newest last; the detail panel shows the latest one.
   */
  retargets?: TaskRetargetRecord[];

  /** Latest pause/stop/interruption, if any. Cleared when execution resumes. */
  interruption?: TaskInterruption;
  /** Full interruption history, newest last. */
  interruptions?: TaskInterruption[];
  /**
   * Round numbers whose failure already woke the caller.
   *
   * Keeps one failure from spawning two callbacks (the supervisor, the
   * dispatcher and the rework controller can all finalize the same round), and
   * caps the wake-up loop so a task that keeps failing cannot ping-pong forever.
   */
  failureNotifications?: number[];

  /**
   * Why the last reviewer wake-up failed, and when the bridge will retry.
   *
   * Cleared as soon as a wake ends cleanly (or the task leaves
   * `waiting_review`), so its presence means "the review is stalled".
   */
  reviewWake?: ReviewWakeState;

  /**
   * Review parked on another task instead of burning rework rounds.
   *
   * The reviewer sometimes cannot approve yet — not because THIS task is wrong,
   * but because a sibling task in the same batch has not been delivered (a
   * PostgreSQL suite that is still skipped, for example). The bridge only has
   * `approve_rework`, so that "waiting" used to be expressed as a rework round
   * with no code change: a full execution slot and a new ledger round every few
   * minutes, forever, until the sibling landed.
   *
   * With this flag the task stays `waiting_review` (so the queue invariants are
   * unchanged) but the watchdog and the wake path leave it alone; it is woken
   * again the moment `waitingForThreadId` reaches a terminal state.
   */
  reviewBlocked?: ReviewBlockInfo;

  /** MR handoff recorded by the caller after the task/batch is accepted. */
  mergeRequest?: MergeRequestInfo;

  /**
   * Temporary git worktree the task was executed in, when it was not run
   * directly in `workspacePath`.
   *
   * The bridge never creates or removes a worktree in this phase: it only keeps
   * the metadata and refuses to let the成果 die with the temporary directory.
   * A completed task whose worktree has no recorded merge request stays
   * `mergePending`, and `delete_task` refuses it until the caller records the MR
   * (or explicitly forces the deletion).
   */
  worktree?: WorktreeInfo;

  // File scope for conflict detection and queueing
  expectedFiles?: string[]; // Concrete code files (directories/globs are rejected at dispatch)

  /**
   * Caller explicitly allowed this task to touch files another open task already
   * changed. Without it, a scope overlap keeps the task queued.
   */
  scopeOverride?: boolean;
  /** Why this task is parked by the scope gate (cleared once it can start). */
  scopeBlockedBy?: { threadId: string; title?: string; files: string[] };

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
   * Model the receiver should run this task with. Omitted = the model configured
   * for that agent in the bridge settings.
   *
   * A non-default value only takes effect when the caller also declares that the
   * user named it (`modelRequestedByUser: true`) AND the user confirms the switch
   * in VS Code. Otherwise the configured default is used.
   */
  model?: string;
  /**
   * Caller declares the user themselves specified `model`. Defaults to false, so
   * a caller guessing a "better" model cannot silently switch away from the
   * configured default; the bridge then asks the user before switching.
   */
  modelRequestedByUser?: boolean;
  /**
   * Model the caller itself is running on, recorded for the task's
   * "sender model -> receiver model" badge. Optional; detected from the
   * environment / configured default when omitted.
   */
  initiatorModel?: string;
  /**
   * Concrete code file paths expected to be modified. Directories and glob
   * patterns are rejected so the lock scope stays per-file.
   *
   * Required by `dispatch_task` unless the caller passes `allowNoScope`: the
   * scope is what lets the bridge refuse to start a task on a file another open
   * task already changed.
   */
  expectedFiles?: string[];
  /** Caller confirms this task touches no workspace file (or refuses to declare). */
  allowNoScope?: boolean;
  /** Caller accepts running on top of files another open task already changed. */
  allowScopeOverlap?: boolean;
  /**
   * Declare that `workspacePath` points at a TEMPORARY git worktree rather than
   * the main checkout. The bridge then records the merge-back obligation: the
   * task cannot be deleted until a merge request is recorded (unless forced),
   * so work that only exists in the throwaway checkout cannot be lost.
   */
  worktree?: DispatchWorktreeSpec;
  /**
   * Reject the dispatch when `expectedFiles` is empty and `allowNoScope` is not
   * set. Enabled by the MCP layer (agent callers must declare a scope); internal
   * callers and tests keep the lenient behaviour.
   */
  requireScope?: boolean;
}

export type DispatchStatus = 'DISPATCHED' | 'QUEUED';

/** Outcome of a user confirmation for switching a receiver model. */
export type ModelApprovalStatus = 'approved' | 'use_default' | 'cancelled' | 'timeout';

/** Compact record of a model-switch confirmation, kept for audit and UI. */
export interface ModelApprovalRecord {
  id: string;
  status: ModelApprovalStatus;
  /** Model the caller asked for. */
  requestedModel: string;
  /** The agent's configured default (what runs unless approved). */
  defaultModel?: string;
  at: string;
}

/** Who executes a task: an agent plus (optionally) one of its models. */
export interface ExecutorOverride {
  targetAgent?: AgentId;
  model?: string;
  /** The user named this model (see the model-approval rule). */
  modelRequestedByUser?: boolean;
}

/** Audit record of moving a task to another agent and/or another model. */
export interface TaskRetargetRecord {
  at: string;
  by?: string;
  reason?: string;
  fromAgent: AgentId;
  toAgent: AgentId;
  fromModel?: string;
  toModel?: string;
  /** Native session dropped because a different agent cannot resume it. */
  previousReceiverSessionId?: string;
  sessionReset: boolean;
}

/** Result of switching a task's executor. */
export interface RetargetTaskResult {
  threadId: string;
  status: 'RETARGETED' | 'UNCHANGED';
  targetAgent: AgentId;
  targetModel?: string;
  previousAgent: AgentId;
  previousModel?: string;
  sessionReset: boolean;
  /** Why the requested model was not applied, when it was not. */
  modelNotice?: string;
  modelApproval?: ModelApprovalRecord;
  modelRouting?: ModelRoutingSummary;
  message: string;
}

export interface DispatchResult {
  threadId: string;
  receiverSessionId: string;
  targetAgent: AgentId;
  status: DispatchStatus;
  message: string;
  /** Sender -> receiver model routing recorded on the task. */
  modelRouting?: ModelRoutingSummary;
  /** Set when the requested model was replaced or confirmed by the user. */
  modelNotice?: string;
  modelApproval?: ModelApprovalRecord;
  /** Machine-readable directive: the caller must end its turn now. */
  nextAction?: 'END_TURN_AND_WAIT_FOR_CALLBACK';
  /** Why it must end the turn (shown to the caller). */
  stopInstruction?: string;
}

/** Compact "who sends with which model, who receives with which model" view. */
export interface ModelRoutingSummary {
  initiatorAgent: AgentId;
  initiatorModel?: string;
  initiatorModelSource: ModelResolutionSource;
  targetAgent: AgentId;
  targetModel?: string;
  /** One-line human-readable form used by messages and the task tree. */
  text: string;
}

export interface RequestReworkParams extends ExecutorOverride {
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
  /** Set when this rework also moved the task to another agent/model. */
  retarget?: TaskRetargetRecord;
  modelNotice?: string;
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
 *
 * The exception is an explicit executor switch: when the last `retarget` reset
 * the session (different agent), there is nothing to resume — even for a rework
 * round, because a native session belongs to the CLI that created it.
 */
export function shouldResumeNativeSession(
  thread: DelegationThread,
  round: DelegationRound,
): boolean {
  const retargets = thread.retargets || [];
  const latestRetarget = retargets.length > 0 ? retargets[retargets.length - 1] : undefined;
  if (latestRetarget?.sessionReset) {
    // A reset only blocks resuming until the NEW executor has opened its own
    // session. Once that first post-switch round captured the native id, every
    // later round must resume it: reserving the id again (`--session-id`) makes
    // Claude Code refuse with "Session ID <id> is already in use." and the round
    // dies in one second, forever.
    const openedAfterReset =
      thread.receiverSessionCaptured === true &&
      !!thread.receiverSessionId &&
      thread.receiverSessionId !== latestRetarget.previousReceiverSessionId;
    if (!openedAfterReset) return false;
  }
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

/**
 * The task ran in a temporary worktree whose output is not on the main line yet.
 *
 * This is the merge gate: as long as it holds, the worktree's changes exist only
 * in a throwaway directory, so the task must not be deleted as if it were noise.
 */
export function isMergePending(thread: DelegationThread): boolean {
  return !!thread?.worktree && thread.status === 'completed' && !thread.mergeRequest;
}

/**
 * Tasks waiting to be merged back into the main checkout.
 *
 * Sorted by `createdAt` ascending: the merge ORDER is deterministic, so a batch
 * of worktree tasks is merged oldest-first.
 */
export function pendingMerges(tasks: DelegationThread[]): DelegationThread[] {
  return (tasks || [])
    .filter((task) => !!task && isMergePending(task))
    .sort((a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime());
}

/** Standard refusal text for every path that would start a second round. */export function describeRoundInFlight(thread: DelegationThread, inFlight: DelegationRound): string {
  return (
    `Round ${inFlight.round} of task ${thread.threadId} is still running; ` +
    `a task may only have one round in flight at a time. ` +
    `Wait for round ${inFlight.round} to finish, or finalize it (retry/resume) before starting another round.`
  );
}

/**
 * The sender model and receiver model recorded on a task.
 *
 * A task always shows both ends: which model the caller dispatched from and
 * which model the receiver executes with. Missing values mean "the agent's own
 * default", which is why they are rendered as such instead of as an empty field.
 */
export function getModelRouting(thread: DelegationThread): ModelRoutingSummary {
  const initiatorModel = thread.initiatorModel;
  const targetModel = thread.targetModel;
  const initiatorModelSource: ModelResolutionSource = initiatorModel
    ? thread.initiatorModelSource || 'explicit'
    : 'none';
  const from = initiatorModel || 'agent default';
  const to = targetModel || 'agent default';
  return {
    initiatorAgent: thread.initiatorAgent,
    initiatorModel,
    initiatorModelSource,
    targetAgent: thread.targetAgent,
    targetModel,
    text: `sender ${thread.initiatorAgent}: ${from} -> receiver ${thread.targetAgent}: ${to}`,
  };
}

/** Localized one-liner for UI: `发送 Codex(gpt-6.1-sol) ➔ 接收 Antigravity(gemini-3.8-flash-high)`. */
export function describeModelRouting(thread: DelegationThread): string {
  const routing = getModelRouting(thread);
  return `发送 ${routing.initiatorAgent}(${routing.initiatorModel || 'Agent 默认'}) ➔ 接收 ${routing.targetAgent}(${routing.targetModel || 'Agent 默认'})`;
}
