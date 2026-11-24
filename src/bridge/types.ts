/**
 * Data contracts and core types for Agent Mesh Bridge.
 */

export type CliAgentId =
  | 'antigravity'
  | 'claude_code'
  | 'codex'
  | 'deepseek_harness';

export type AgentId = CliAgentId;

export type TargetAgentSelection = 'default' | AgentId;

/**
 * How the sender/receiver model recorded on a task was determined.
 *
 * `sender-configured` means the model came from the agent's own sender-side
 * default (`agentMeshBridge.initiatorModels.*`), as opposed to `configured`,
 * which is the receiver-side default (`agentMeshBridge.models.*`).
 */
export type ModelResolutionSource =
  | 'explicit'
  | 'env'
  | 'sender-configured'
  | 'configured'
  | 'none';

export type ThreadStatus =
  | 'queued'
  | 'running'
  | 'reviewing'
  | 'merging'
  | 'wait_decision'
  | 'paused'
  | 'completed'
  | 'failed';

export type ExecutionPhase = 'develop' | 'review' | 'merge';

export interface ProcessIdentity {
  pid: number;
  birthId?: string;
}

export interface ExecutionSubtreeEvidence {
  root: ProcessIdentity;
  descendants: ProcessIdentity[];
  windowsJob?: import('./windows-job').WindowsJobBinding;
  /** Wall-clock time of the latest cumulative observation, for audit and recovery ordering. */
  sampledAt?: string;
}

export type ExecutionVersion = 1 | 2;

export interface ExecutionRun {
  id: string;
  owner: ProcessIdentity;
  child?: ProcessIdentity;
  /** Durable identities captured under each spawned child root for crash recovery. */
  subtrees?: ExecutionSubtreeEvidence[];
  step: string;
  state: 'starting' | 'active' | 'stopping';
  startedAt: string;
  role?: 'developer' | 'reviewer';
  purpose?: 'native_maintenance';
    /** Native maintenance call returned, but its captured process tree still needs exit confirmation. */
    settlementPending?: boolean;
  spawnIntentAt?: string;
  registeredAt?: string;
  unconfirmed?: boolean;
  mergeLockHelper?: {
    pid?: number;
    birthId?: string;
    channelPath: string;
    reason: string;
    observedAt: string;
  };
}

export interface ExecutionNext {
  phase: ExecutionPhase;
  round: number;
  purpose?: 'bootstrap' | 'merge_resolution';
  sourceRunId?: string;
}

export interface ExecutionQueue {
  reason:
    | 'role'
    | 'session'
    | 'merge_lock'
    | 'merge_lock_error'
    | 'capacity'
    | 'backoff'
    | 'quota'
    | 'dependency';
  notBefore?: string;
  enteredAt?: string;
}

export interface TaskCleanupState {
  token: string;
  action: 'archive' | 'delete';
  owner: ProcessIdentity;
  startedAt: string;
  stage?: 'marking' | 'in_progress' | 'completed';
  operationId?: string;
  generation?: number;
  runId?: string;
  runExitProof?: { runId: string; operationId: string; generation: number; confirmedAt: string };
}

export interface ExecutionControl {
  version: ExecutionVersion;
  revision: number;
  operationId: string;
  phase: ExecutionPhase;
  round: number;
  purpose?: 'bootstrap' | 'merge_resolution';
  generation: number;
  attempt: number;
  autoRetriesUsed: number;
  queue?: ExecutionQueue;
  run?: ExecutionRun;
  next?: ExecutionNext;
  budget: {
    limitMs: number;
    spentActiveMs: number;
    maxAutoRetries: number;
  };
  lastOutcome?: {
    code: string;
    reason: string;
    at: string;
    evidencePath?: string;
  };
}

export interface NativeSessionAssignments {
  developer?: { namespace: string; sessionEpoch: number };
  reviewer?: { namespace: string; sessionEpoch: number };
}

export type TaskDecisionResumePoint =
  | 'continue_develop'
  | 'continue_review'
  | 'retry_merge'
  | 'continue_merge_resolution';

export interface TaskDecisionRecord {
  id: string;
  /** v2 run whose first decision this record reserves. */
  sourceRunId?: string;
  operationId: string;
  generation: number;
  phase: ExecutionPhase;
  round: number;
  question: string;
  background?: string;
  suggestion?: string;
  resumePoint: TaskDecisionResumePoint;
  createdAt: string;
  answer?: string;
  answeredAt?: string;
  answeredBy?: string;
  conflictFiles?: string[];
  logPath?: string;
}

export type NextAction =
  | { kind: 'advance'; phase?: ExecutionPhase }
  | { kind: 'retry'; notBefore: string; reason?: string }
  | { kind: 'decision'; requestId: string }
  | { kind: 'failed'; code: string; reason?: string; stage?: FailureStage }
  | { kind: 'paused' };

export type FailureStage = 'bootstrap' | 'execution' | 'review' | 'merge';
export interface TaskFailure {
  stage: FailureStage;
  reason: string;
  at: string;
  round?: number;
  by?: string;
  code?: string;
}

export function enterTaskFailure(thread: DelegationThread, failure: TaskFailure): void {
  failure = { ...failure, reason: failure.reason.trim() || 'Task failed; no reason was recorded' };
  if (thread.status === 'failed' && thread.failure && thread.failure.stage === failure.stage &&
      thread.failure.reason === failure.reason && thread.failure.round === failure.round && thread.failure.code === failure.code) return;
  thread.failureRevision = (thread.failureRevision || 0) + 1;
  thread.status = 'failed';
  thread.failure = failure;
}

export function clearTaskFailure(thread: DelegationThread): void {
  if (thread.status === 'failed') {
    thread.failureRevision = (thread.failureRevision || 0) + 1;
    thread.failure = undefined;
  }
}

/** States in which a completed developer round can be reviewed. */
export function isReviewerWakeEligible(thread: DelegationThread): boolean {
  return thread.reviewFlow === 'dedicated' && thread.status === 'reviewing';
}

export function isMergeReview(thread: DelegationThread): boolean {
  return thread.status === 'reviewing' && !!(thread.worktree?.mergePending || thread.worktree?.approvedCommit) &&
    ['blocked', 'conflict', 'validation_failed'].includes(thread.worktree.integration?.phase || '');
}

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

export interface CliResponseAssessment {
  transport?: 'cli';
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

export type ResponseAssessment = CliResponseAssessment;

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
  /** Per operation/attempt accounting; absent on historical ledger entries. */
  components?: TokenUsageComponent[];
}

export interface TokenUsageComponent {
  id: string;
  kind: 'business' | 'native_compaction';
  role: 'developer' | 'reviewer';
  agent: AgentId;
  model?: string;
  uncachedInputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  outputTokens: number;
  reasoningTokens?: number;
  nativeTotalTokens?: number;
  costUsd?: number;
  costSource?: TokenCostSource;
  source: TokenMetricSource;
  available: boolean;
  partial?: boolean;
}

export type NativeCompactionPhase =
  | 'prepared' | 'running' | 'succeeded' | 'skipped' | 'failed' | 'interrupted' | 'unknown';

export interface NativeSessionCompactionOperation {
  id: string;
  role: 'developer' | 'reviewer';
  agent: AgentId;
  sessionId: string;
  model?: string;
  reason: 'context_pressure' | 'turn_budget' | 'manual';
  phase: NativeCompactionPhase;
  revision: number;
  startedAt: string;
  finishedAt?: string;
  /** Host-suspension-adjusted active time charged to the shared execution budget. */
  activeDurationMs?: number;
  nativeTurnId?: string;
  errorCode?: string;
  error?: string;
  interruption?: TaskInterruption;
  recoveryPath: string;
  logPath: string;
}

export interface NativeSessionCompactionCursor {
  revision?: number;
  sessionId: string;
  lastOperationId?: string;
  turnsSinceCompaction: number;
  recoveryVersion?: string;
  recoveryDeliveredVersion?: string;
  recoveryVerifiedOperationId?: string;
  autoDisabledReason?: string;
  autoSuppressedOperationId?: string;
  turnCounterResetId?: string;
  recentBusinessTurnIds?: string[];
  recentNativeAutoCompactionEventIds?: string[];
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
   * status probe bracketing the run. This is observation data only.
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
  nativeSessionCompactions?: NativeSessionCompactionOperation[];
}

export interface ThreadSummary {
  lastPromptSnippet: string; // Caller's latest prompt preview (first 100 chars)
  lastOutputSnippet: string; // Receiver's latest output preview (first 150 chars)
  totalDurationMs: number; // Cumulative duration in ms
}

/**
 * Which review topology a task was created with.
 *
 * New work always uses a dedicated reviewer. Legacy and absent values
 * remain readable for history only; requester conversations are never resumed.
 */
export type ReviewFlow = 'legacy' | 'dedicated';

/** Historical requester-review tasks remain readable and safely removable. */
export function assertDedicatedTask(thread: Pick<DelegationThread, 'threadId' | 'reviewFlow'>): void {
  if (thread.reviewFlow !== 'dedicated') {
    throw new Error(`LEGACY_REVIEW_FLOW_DISABLED: Task ${thread.threadId} is historical; only reading, archival, deletion and stopping existing processes are supported.`);
  }
}

/** Lifecycle of the dedicated reviewer's bootstrap (dispatch) turn. */
export type ReviewBootstrapPhase =
  | 'pending'
  | 'launching'
  | 'awaiting_dispatch'
  | 'confirmed'
  | 'failed';

/**
 * State of the dedicated reviewer conversation before the developer may start.
 *
 * The reviewer conversation is launched by the bridge in a FRESH native session;
 * its first turn only calls `dispatch_task` with a one-time bootstrap token. The
 * developer round starts only after the token confirms, the reviewer process
 * has exited, and its native session id has been durably captured. Other tasks
 * may run concurrently when configured task limits permit.
 */
export interface ReviewerBootstrapState {
  phase: ReviewBootstrapPhase;
  /** Reviewer bootstrap attempts spent so far. */
  attempts: number;
  launchedAt?: string;
  lastError?: string;
  /**
   * Earliest time a relaunch is allowed.
   *
   * Set when a bootstrap turn ended without confirming because the host raced
   * its MCP registration (the reviewer never saw `dispatch_task`). Retrying
   * immediately tends to lose the same race, so the relaunch waits out a
   * cool-off instead of being counted as a terminal failure straight away.
   */
  retryAfter?: string;
  /** Native reviewer conversation id captured from the bootstrap turn. */
  reviewerSessionId?: string;
}

/**
 * Read-only terminal summary kept on the task, and the ONLY payload surviving
 * archival.
 *
 * It is deterministic (built from the ledger, never by an extra model turn), so
 * `get_task_summary` can answer after the rounds/logs/native sessions are gone.
 */
export interface TaskFinalSummary {
  kind: 'completed' | 'failed';
  failureStage?: FailureStage;
  failureRevision?: number;
  at: string;
  title: string;
  /** Reviewer's approval remarks (completed) or the terminal error (failed). */
  verdict?: string;
  /** Bounded human-readable digest. */
  summary: string;
  changedFiles?: string[];
  error?: string;
  rounds: number;
  mergeRequest?: MergeRequestInfo;
  reviewerAgent?: AgentId;
  reviewerModel?: string;
  executorAgent: AgentId;
  executorModel?: string;
  totalTokens?: number;
  promptTokens?: number;
  outputTokens?: number;
  tokenUsageByAgent?: Partial<Record<AgentId, number>>;
  tokenUsageByModel?: Record<string, number>;
  nativeCompactions?: { count: number; warnings: string[] };
  gitDelivery?: {
    taskBranch?: string; taskPath?: string; approvedCommit?: string;
    targetBranch?: string; targetRef?: string; targetRepositoryPath?: string; targetPath?: string; targetCommit?: string;
    mergeState?: string; blockedReason?: string;
    stashRestore?: { status: string; sha?: string; error?: string };
    validationPath?: string; validationCommit?: string; validationStatus?: string; validationResult?: string;
  };
}

/**
 * Metadata for a task that ran in a temporary git worktree.
 *
 * `deliveryTarget` pins the real local branch for executable tasks. Historical
 * `targetRef` values remain readable but cannot drive delivery.
 * An MR is separate remote-delivery metadata and never changes local delivery.
 */
export interface WorktreeInfo {
  /** Absolute path of the temporary worktree the agent edited. */
  path: string;
  /** Absolute path of the main checkout the worktree belongs to. */
  mainPath: string;
  commonDir?: string;
  repositoryPath?: string;
  targetRef?: string;
  /** Real local branch selected at dispatch; absent on legacy tasks using an internal integration ref. */
  deliveryTarget?: { branch: string; ref: string; repositoryPath: string; checkoutPath?: string; baseCommit?: string };
  /** Actual combined-validation worktree, retained as an audit fact after cleanup. */
  validation?: { path: string; commit?: string; status: 'passed' | 'failed' | 'cancelled' | 'cleaned' | 'present'; result?: 'passed' | 'failed' | 'cancelled'; updatedAt: string };
  taskBranch?: string;
  headCommit?: string;
  approvedCommit?: string;
  approvedSnapshot?: { commit: string; tree: string; sourceCommit: string };
  mergeResolutionPath?: string;
  mergeResolutionSubmitted?: boolean;
  mergeResolutionSourceCommit?: string;
  supersededMergeResolutionPaths?: string[];
  integration?: {
    phase: 'ready' | 'no_changes' | 'waiting' | 'merging' | 'conflict' | 'validation_failed' | 'blocked' | 'merged';
    sourceCommit?: string;
    targetOldCommit?: string;
    targetCommit?: string;
    targetPath?: string;
    candidateCommit?: string;
    mergedCommit?: string;
    stashRecoveryPending?: boolean;
    stashRecoveryLockHeld?: boolean;
    stashRestore?: { status: string; sha?: string; error?: string };
    error?: string;
    conflictFiles?: string[];
    updatedAt: string;
  };
  /** Branch the worktree was based on / will be merged from. */
  branch?: string;
  /** Commit the worktree was created from. */
  baseCommit?: string;
  /** True while the worktree's output has no recorded merge request. */
  mergePending?: boolean;
}

/** Only a pinned real local delivery target may drive Git execution. */
export function requireDeliveryTarget(worktree: WorktreeInfo): NonNullable<WorktreeInfo['deliveryTarget']> {
  const target = worktree.deliveryTarget;
  if (!target?.repositoryPath || !target.ref || !target.branch || target.ref !== `refs/heads/${target.branch}`) {
    throw new Error('MISSING_DELIVERY_TARGET: Historical Git delivery metadata cannot be executed.');
  }
  return target;
}

/** Metadata written by the isolated-worktree lifecycle introduced by the bridge. */
export function isIsolatedTaskWorktree(worktree?: WorktreeInfo): worktree is WorktreeInfo & { targetRef: string; taskBranch: string; baseCommit: string; repositoryPath: string; commonDir: string } {
  return !!worktree?.targetRef && !!worktree.taskBranch && !!worktree.baseCommit && !!worktree.repositoryPath && !!worktree.commonDir;
}

/**
 * A failed attempt to wake the initiator (reviewer) for a finished round.
 *
 * The receiver round finishing does not imply the review happened: the wake
 * spawns the caller's CLI, and that process can die (quota, expired session,
 * provider outage) before it calls `approve_task` / `request_rework`. Without a
 * record, the task would sit in `reviewing` forever with no trace of why.
 * This is that trace, plus the plan for the automatic retry.
 */
export interface ReviewWakeState {
  /** How the stall was detected. */
  kind?: 'wake_failed' | 'timeout' | 'pending';
  /** Round the reviewer still has to inspect. */
  round: number;
  /** Failed wake attempts since the last recorded review handoff. */
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

/**
 * Why a review ended with no verdict at all.
 *
 * `review_failed` is a TERMINAL lifecycle status: the receiver delivered, but no
 * reviewer ever produced a verdict (the wake died, the retry budget ran out, or
 * the review was parked on a dependency nothing in the mesh can observe). It is
 * kept separate from `failed` because the delivered work may be perfectly good —
 * only the review is missing — and separate from `reviewing` because no
 * reviewer is working and nothing will ever move it without human action.
 *
 * Terminal semantics (identical to `failed` for scheduling): the task releases
 * the global queue slot; re-review /
 * request_rework / retry / approve all re-open it.
 */
export interface ReviewFailure {
  /** `retries_exhausted`: the wake retry budget ran out. */
  kind: 'retries_exhausted' | 'unresolvable_park';
  /** Human-readable cause, shown in the task tree and detail panel. */
  reason: string;
  at: string;
  /** Round whose review never happened. */
  round: number;
  /** Who recorded the failure: `watchdog`, `scheduler`, `callback`. */
  by?: string;
  /** Reviewer agent that never delivered a verdict. */
  agent?: AgentId;
  /** Snapshot of the failed wake plan that preceded this status. */
  wake?: ReviewWakeState;
}

export type { TaskWorkspaceBinding } from './workspace';
import type { TaskWorkspaceBinding } from './workspace';

export function getTaskExecutionPath(thread: DelegationThread): string {
  const executionPath = thread.workspace?.executionPath;
  if (!executionPath || (thread.worktree && thread.worktree.path !== executionPath)) {
    throw new Error(`Invalid execution path binding for ${thread.threadId}`);
  }
  return executionPath;
}

export interface DelegationThread {
  failure?: TaskFailure;
  failureRevision?: number;
  execution?: ExecutionControl;
  cleanup?: TaskCleanupState;
  decision?: TaskDecisionRecord;
  decisions?: TaskDecisionRecord[];
  taskTemplate?: import('./task-templates').TaskTemplate;
  threadId: string; // e.g. "thread_20261016_163000_a7f9"
  title: string; // Brief title, e.g. "Fix TCP cursor drift"
  initiatorAgent: AgentId; // Initiator (Caller A)
  targetAgent: AgentId; // Receiver (Target B)
  status: ThreadStatus; // Lifecycle status; each task has its own isolated Git worktree.
  queuePhase?: 'development' | 'merge'; // Missing means development for existing ledgers.
  currentRound: number; // Current round (1: initial, 2+: rework)
  createdAt: string; // ISO8601 creation timestamp
  updatedAt: string; // ISO8601 update timestamp
  mergeHistory?: Array<{ id: string; number: number; at: string; phase: string; result: string; error?: string; logPath?: string;
    targetRef?: string; targetOldCommit?: string; targetCommit?: string; sourceCommit?: string; candidateCommit?: string; candidateTree?: string; conflictFiles?: string[];
    mergedCommit?: string; changedFiles?: string[]; decisionId?: string; question?: string; background?: string; suggestion?: string;
    answer?: string; decisionStage?: 'review' | 'merge'; fingerprint?: string; retryGeneration?: number; consecutiveRetries?: number; errorCode?: string; exitCode?: number; attemptId?: string; logNumber?: number; logFileNumber?: number; assessment?: ResponseAssessment; interruption?: TaskInterruption }>;
  mergeControl?: { revision: number; retryGeneration: number; failureFingerprint?: string; consecutiveRetries: number; pendingDecisionId?: string; decisionStage?: 'review' | 'merge' };

  // Native session bindings for continuous rework resume
  receiverSessionId: string; // Native session ID for target agent
  nativeSessionCompaction?: Partial<Record<'developer' | 'reviewer', NativeSessionCompactionCursor>>;
  /**
   * True once the receiver's real native session is known: either an agent
   * emitted its native id, or Claude's preassigned `--session-id` is the native
   * session. A paused initial round can only be resumed in-session when true.
   */
  receiverSessionCaptured?: boolean;
  callerSessionId?: string; // Caller session ID for reactive wake-up callbacks
  workspace: TaskWorkspaceBinding;

  /**
   * Absolute root of this task's bridge-managed files (unified task directory).
   *
   * Written once at creation and never re-resolved from the global
   * `agentMeshBridge.taskDataRoot` setting: a policy change must not make one
   * task write into two roots. Historical entries without this field do not
   * resolve logs from the retired global or per-agent directories.
   */
  artifactDir?: string;
  /** Alias retained for callers/UI that name the same directory `taskDataDir`. */
  taskDataDir?: string;

  /**
   * Review topology pinned when the task was created (see `ReviewFlow`).
   * Absent on historical entries, which cannot resume requester review.
   */
  reviewFlow?: ReviewFlow;


  /**
   * Dedicated-reviewer mode only: the model the reviewer conversation runs on
   * (sender-side default), pinned at dispatch so the reviewer never silently
   * drifts to another model.
   */
  reviewerModel?: string;
  reviewerModelSource?: ModelResolutionSource;

  /**
   * Dedicated-reviewer mode only: native id of the reviewer conversation once
   * the bootstrap turn emitted it. `callerSessionId` mirrors this value so the
   * existing wake path resumes the reviewer conversation.
   */
  reviewerSessionId?: string;
  /** Native storage identity is pinned on first v2 claim and never follows global config drift. */
  nativeSessionAssignments?: NativeSessionAssignments;
  reviewerSessionCaptured?: boolean;

  /** Dedicated-reviewer bootstrap state (undefined for legacy tasks). */
  reviewBootstrap?: ReviewerBootstrapState;

  /** Read-only terminal digest, written once the task reaches completed/failed. */
  finalSummary?: TaskFinalSummary;

  /** Set when the task was archived: rounds/logs/session ids are gone. */
  archivedAt?: string;

  /**
   * Model the sender (initiator) was running when it dispatched the task.
   *
   * The bridge cannot always introspect another agent's live session, so this is
   * best-effort: an explicit `initiatorModel`, then an agent-specific env hint,
   * then the initiator's sender-side configured default, then (legacy) its
   * receiver-side default. `initiatorModelSource` says which of those produced
   * the value.
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
   * Cleared only after a same-round verdict/handoff is recorded or a review is
   * explicitly reopened for new work; a clean process exit alone is not a verdict.
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
   * With this flag the task stays `reviewing` (so the queue invariants are
   * unchanged) but the watchdog and the wake path leave it alone; it is woken
   * again the moment `waitingForThreadId` reaches a terminal state.
   */
  reviewBlocked?: ReviewBlockInfo;

  /**
   * Why the review never produced a verdict (`status === 'review_failed'`).
   *
   * Persisted separately from `reviewWake` because the wake plan is diagnostic
   * (and may be cleared on recovery) while the failure reason is the record the
   * user needs to understand why the task stopped. Kept after a later re-review
   * or approval succeeds, as audit history.
   */
  reviewFailure?: ReviewFailure;

  /** MR handoff recorded by the caller after the task/batch is accepted. */
  mergeRequest?: MergeRequestInfo;

  /**
   * Isolated task worktree and local integration transaction. The target ref is
   * a bridge-managed batch branch; MR metadata records remote delivery only.
   */
  worktree?: WorktreeInfo;

  // Summary for fast sidebar rendering
  summary: ThreadSummary;

  // Complete round history
  rounds: DelegationRound[];
}

export interface DispatchTaskParams {
  templateId?: string;
  targetAgent?: TargetAgentSelection;
  title: string;
  prompt: string;
  /** Deprecated dispatch input, ignored: only the captured reviewer is resumed. */
  callerSessionId?: string;
  initiatorAgent?: AgentId;
  workspaceId?: string;
  workspaceRoot?: string;
  workspaceSource?: TaskWorkspaceBinding['source'];
  workspaceOrigin?: TaskWorkspaceBinding['origin'];
  /** MCP has resolved caller origin (including ambiguity); never infer it from the target. */
  suppressWorkspaceOriginInference?: boolean;
  /** Local branch to deliver into; omitted means the source checkout's branch. */
  targetBranch?: string;
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
   * Optional internal flow assertion. Explicit legacy is rejected; omitted
   * always creates a dedicated reviewer, including standalone MCP.
   */
  reviewFlow?: ReviewFlow;
}

/**
 * `REVIEWER_STARTING`: the task row exists, but the dedicated reviewer
 * conversation is still bootstrapping; the developer starts after it dispatches.
 */
export type DispatchStatus = 'DISPATCHED' | 'QUEUED' | 'REVIEWER_STARTING';

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
  workspace: TaskWorkspaceBinding;
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
  status: 'QUEUED' | 'REVIEWING' | 'MERGE_FAILED' | 'COMPLETED';
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
  agents: Partial<Record<AgentId, AgentInfo>> & Record<CliAgentId, AgentInfo>;
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
 * `completed`, `failed` or `review_failed`. `reviewing` and `paused` are
 * still unfinished tasks and MUST block every other task, not just developer
 * rounds. `failed` and `review_failed` are terminal for queue purposes so old
 * failures cannot block the mesh forever; retrying/re-reviewing either one
 * creates an unfinished task again.
 */
export function isTaskUnfinished(thread: DelegationThread): boolean {
  return (
    thread.status !== 'completed' &&
    thread.status !== 'failed'
  );
}

/**
 * The newest completed work round (never an `approval` bookkeeping round).
 *
 * This is the only round a reviewer should inspect or log against: approval
 * rounds carry no deliverable, and a stalled wake may name an older round than
 * the one that was actually delivered last.
 */
export function latestCompletedWorkRound(
  thread: DelegationThread,
): DelegationRound | undefined {
  return (thread.rounds || [])
    .filter((round) => round.type !== 'approval' && round.status === 'completed')
    .sort((a, b) => b.round - a.round)[0];
}

/**
 * A dedicated task may not execute its developer round until its reviewer
 * bootstrap has dispatched. Legacy tasks never have a bootstrap state.
 *
 * Pending/launching are not ready. A confirmed bootstrap also remains pending
 * until the reviewer session id is durably captured for the developer handoff.
 * A failed bootstrap is terminal: its one-shot token is gone and it is never
 * selected as an executable developer candidate.
 */
export function isReviewerBootstrapPending(thread: DelegationThread): boolean {
  if (!thread || thread.reviewFlow !== 'dedicated') return false;
  const phase = thread.reviewBootstrap?.phase;
  // Any state other than a captured, canonical handoff must block developer
  // admission, including terminal bootstrap failure.
  if (phase !== 'confirmed') return true;
  return !(
    thread.reviewerSessionCaptured === true &&
    isValidSessionId(thread.reviewerSessionId) &&
    isValidSessionId(thread.callerSessionId) &&
    thread.callerSessionId === thread.reviewerSessionId
  );
}

/**
 * Legacy compatibility helper. Runtime scheduling must not use a global FIFO
 * queue head: candidates are admitted independently subject to role,
 * per-thread round, bootstrap handoff, and optional concurrency limits.
 */
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
  const state = thread?.worktree?.integration?.phase;
  if (!thread?.worktree || state === 'merged' || state === 'no_changes') return false;
  return true;
}

/**
 * Tasks waiting to be merged back into the main checkout.
 *
 * Sorted by `createdAt` ascending: the merge ORDER is deterministic, so a batch
 * of worktree tasks is merged oldest-first.
 */
export function pendingMerges(tasks: DelegationThread[]): DelegationThread[] {
  return (tasks || [])
    .filter((task) => !!task && isMergePending(task) &&
      !(task.status === 'running' && !task.worktree?.integration && !task.worktree?.approvedCommit))
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
