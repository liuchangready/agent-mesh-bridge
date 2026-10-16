/**
 * Data contracts and core types for Agent Mesh Bridge.
 */

export type AgentId = 'antigravity' | 'claude_code' | 'codex' | 'vscode_chat';

export type TargetAgentSelection = 'default' | AgentId;

export type ThreadStatus = 'running' | 'waiting_review' | 'reworking' | 'completed' | 'failed';

export type RoundType = 'initial' | 'rework' | 'approval';

export type RoundStatus = 'running' | 'completed' | 'failed';

export interface DelegationRound {
  round: number; // Round number: 1, 2, 3...
  type: RoundType;
  prompt: string; // Instruction or feedback given by caller
  output?: string; // Output returned by receiver
  error?: string; // Error description if failed
  durationMs: number; // Duration in milliseconds
  status: RoundStatus;
  timestamp: string; // ISO8601 timestamp
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
  status: ThreadStatus; // Lifecycle status
  currentRound: number; // Current round (1: initial, 2+: rework)
  createdAt: string; // ISO8601 creation timestamp
  updatedAt: string; // ISO8601 update timestamp

  // Native session bindings for continuous rework resume
  receiverSessionId: string; // Native session ID for target agent
  callerSessionId?: string; // Caller session ID for reactive wake-up callbacks
  workspacePath: string; // Working directory

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
}

export interface DispatchResult {
  threadId: string;
  receiverSessionId: string;
  targetAgent: AgentId;
  status: 'DISPATCHED';
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

export interface AgentInfo {
  id: AgentId;
  name: string;
  available: boolean;
  executablePath?: string;
  version?: string;
  statusDetails?: string;
}

export interface AgentMeshStatus {
  activeAgent: AgentId;
  resolvedMode: 'auto' | 'manual';
  agents: Record<AgentId, AgentInfo>;
  totalLedgerTasks: number;
}
