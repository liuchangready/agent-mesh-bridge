/**
 * Bridge-wide policy flags that every process must agree on.
 *
 * The VS Code extension host can read `agentMeshBridge.*` settings directly, but
 * the MCP servers run in their own processes (spawned by the agents, without the
 * VS Code API), so a policy switch has to be mirrored to a file both sides read.
 * The model settings already do this via `model-config.json`; mixing an unrelated
 * switch into that snapshot would be confusing, hence this small file.
 */

import * as fs from 'fs';
import * as path from 'path';
import { bridgeHomeDir } from './round-log';
import { AgentId } from './types';

export const BRIDGE_POLICY_FILENAME = 'bridge-policy.json';

/** Rounds shorter than this are already cheap to keep verbatim. */
export const DEFAULT_COMPACTION_MIN_CHARS = 1500;

/**
 * Bounded-concurrency switches.
 *
 * The enabled flag keeps its historical opt-in meaning for the numeric limits:
 * when false there is no concurrency limit; when true the configured mesh-wide
 * and per-agent limits are applied as best-effort admission checks. Neither
 * setting creates a global or per-workspace mutex. Per-thread roles and the
 * explicit concurrency limits are the execution blockers; Git worktrees
 * isolate task files from each other.
 */
export type AgentLogDirectories = Partial<Record<AgentId | 'default', string>>;

export interface ConcurrencyPolicy {
  enabled: boolean;
  /** Mesh-wide execution slots. */
  maxConcurrentTasks: number;
  /** Slots one agent may occupy at the same time. */
  maxConcurrentPerAgent: number;
}

export interface BridgePolicy {
  /** Bridge-controlled native session maintenance; shipped disabled until certification. */
  nativeSessionCompaction?: { mode?: 'off' | 'auto' };
  /**
   * Condense the previous round's output once a later round has completed.
   * Defaults to true; only an explicit `false` disables it.
   */
  contextCompaction?: boolean;
  /** Outputs at or below this many characters stay verbatim. */
  contextCompactionMinChars?: number;
  /** Optional best-effort slot limits (disabled means unlimited). */
  concurrency?: {
    enabled?: boolean;
    maxConcurrentTasks?: number;
    maxConcurrentPerAgent?: number;
  };
  /**
   * Review topology for NEW tasks. Omitted = dedicated when the extension host
   * is alive, legacy otherwise (standalone MCP has no process able to run the
   * reviewer bootstrap).
   */
  reviewFlow?: 'legacy' | 'dedicated';
  /** Legacy log roots read for tasks created before unified task directories. */
  logDirectories?: AgentLogDirectories;
  /**
   * Root directory for NEW tasks' unified data directories.
   *
   * Each task gets `<taskDataRoot>/<threadId>/` containing its worktree, logs,
   * bootstrap credential, validation workspace and Archive. Empty keeps the
   * default `<bridge home>/tasks`; a relative value resolves against the bridge
   * home. Existing tasks keep the absolute `thread.artifactDir` recorded when
   * they were created, so changing this never forks a running task's files.
   */
  taskDataRoot?: string;
  /**
   * Legacy worktree root (pre-unified task directories), read for old callers.
   *
   * Retained when loading old policy snapshots; new tasks use `taskDataRoot`.
   */
  worktreeRoot?: string;
  updatedAt?: string;
}

/** Limits apply only when enabled; disabled means no concurrency cap. */
export const DEFAULT_CONCURRENCY_POLICY: ConcurrencyPolicy = {
  enabled: false,
  maxConcurrentTasks: 2,
  maxConcurrentPerAgent: 1,
};

export function bridgePolicyPath(root?: string): string {
  return path.join(bridgeHomeDir(root), BRIDGE_POLICY_FILENAME);
}

export function readBridgePolicy(root?: string): BridgePolicy {
  try {
    const file = bridgePolicyPath(root);
    if (!fs.existsSync(file)) return {};
    const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
    return parsed && typeof parsed === 'object' ? (parsed as BridgePolicy) : {};
  } catch {
    return {};
  }
}

/**
 * Merge a policy patch into the mirrored snapshot.
 *
 * Non-finite / negative thresholds are ignored so a bad value cannot turn every
 * round into a compaction candidate.
 */
export function saveBridgePolicy(patch: BridgePolicy, root?: string): BridgePolicy {
  const current = readBridgePolicy(root);
  const next: BridgePolicy = { ...current };
  if (patch.contextCompaction !== undefined) {
    next.contextCompaction = patch.contextCompaction !== false;
  }
  if (
    patch.contextCompactionMinChars !== undefined &&
    Number.isFinite(patch.contextCompactionMinChars) &&
    patch.contextCompactionMinChars >= 0
  ) {
    next.contextCompactionMinChars = Math.floor(patch.contextCompactionMinChars);
  }
  if (patch.nativeSessionCompaction?.mode === 'off' || patch.nativeSessionCompaction?.mode === 'auto') {
    next.nativeSessionCompaction = { mode: patch.nativeSessionCompaction.mode };
  }
  if (patch.concurrency !== undefined) {
    const currentConcurrency = next.concurrency || {};
    const merged: NonNullable<BridgePolicy['concurrency']> = { ...currentConcurrency };
    if (patch.concurrency.enabled !== undefined) {
      merged.enabled = patch.concurrency.enabled === true;
    }
    const slots = positiveSlotCount(patch.concurrency.maxConcurrentTasks);
    if (slots !== undefined) merged.maxConcurrentTasks = slots;
    const perAgent = positiveSlotCount(patch.concurrency.maxConcurrentPerAgent);
    if (perAgent !== undefined) merged.maxConcurrentPerAgent = perAgent;
    if (Object.keys(merged).length > 0) next.concurrency = merged;
  }
  if (patch.reviewFlow === 'legacy' || patch.reviewFlow === 'dedicated') {
    next.reviewFlow = patch.reviewFlow;
  }
  if (patch.logDirectories !== undefined) {
    const mergedDirs: AgentLogDirectories = { ...(next.logDirectories || {}) };
    for (const [key, value] of Object.entries(patch.logDirectories)) {
      if (typeof value !== 'string') continue;
      const trimmed = value.trim();
      if (trimmed) mergedDirs[key as AgentId | 'default'] = trimmed;
      else delete mergedDirs[key as AgentId | 'default'];
    }
    next.logDirectories = mergedDirs;
  }
  if (patch.taskDataRoot !== undefined) {
    if (typeof patch.taskDataRoot === 'string') {
      const trimmed = patch.taskDataRoot.trim();
      if (trimmed) {
        next.taskDataRoot = trimmed;
      } else {
        delete next.taskDataRoot;
      }
    }
  }
  if (patch.worktreeRoot !== undefined && typeof patch.worktreeRoot === 'string') {
    const trimmed = patch.worktreeRoot.trim();
    if (trimmed) next.worktreeRoot = trimmed;
    else delete next.worktreeRoot;
  }
  next.updatedAt = new Date().toISOString();

  try {
    const dir = path.dirname(bridgePolicyPath(root));
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const temp = `${bridgePolicyPath(root)}.tmp.${Date.now()}`;
    fs.writeFileSync(temp, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
    fs.renameSync(temp, bridgePolicyPath(root));
  } catch (err) {
    console.warn('[BridgePolicy] Failed to persist policy snapshot:', err);
  }
  return next;
}

/** Compaction is on unless the user explicitly turned it off. */
export function isContextCompactionEnabled(root?: string): boolean {
  return readBridgePolicy(root).contextCompaction !== false;
}

export function nativeSessionCompactionMode(root?: string): 'off' | 'auto' {
  return readBridgePolicy(root).nativeSessionCompaction?.mode === 'auto' ? 'auto' : 'off';
}

export function contextCompactionMinChars(root?: string): number {
  const value = readBridgePolicy(root).contextCompactionMinChars;
  return Number.isFinite(value) && (value as number) >= 0
    ? Math.floor(value as number)
    : DEFAULT_COMPACTION_MIN_CHARS;
}

/**
 * A usable slot count: non-finite/negative values are dropped (the caller keeps
 * the previous value), zero/float values are clamped to at least one slot.
 */
function positiveSlotCount(value: unknown): number | undefined {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) return undefined;
  return Math.max(1, Math.floor(value));
}

/** Effective concurrency policy, preserving the historical enabled getter semantics. */
export function concurrencyPolicy(root?: string): ConcurrencyPolicy {
  const raw = readBridgePolicy(root).concurrency;
  if (!raw || typeof raw !== 'object') return { ...DEFAULT_CONCURRENCY_POLICY };
  return {
    // Only an explicit `true` enables the configured numeric limits. With the
    // flag false, callers must not interpret the returned defaults as a cap.
    enabled: raw.enabled === true,
    maxConcurrentTasks: positiveSlotCount(raw.maxConcurrentTasks) ?? DEFAULT_CONCURRENCY_POLICY.maxConcurrentTasks,
    maxConcurrentPerAgent:
      positiveSlotCount(raw.maxConcurrentPerAgent) ?? DEFAULT_CONCURRENCY_POLICY.maxConcurrentPerAgent,
  };
}


/** Custom log directories configured per agent or globally. */
export function getAgentLogDirectories(root?: string): AgentLogDirectories {
  return readBridgePolicy(root).logDirectories || {};
}

/**
 * Root directory that holds every (legacy) task worktree.
 *
 * Falls back to `<storage>/worktrees`. New tasks ignore this switch and place
 * their worktree inside the unified task directory; the setting is retained for
 * old ledger entries and direct worktree creation callers.
 */
export function worktreeRoot(storageDir: string, root?: string): string {
  // The mirrored policy lives next to the ledger, which is `storageDir` for every
  // real caller. Reading only `root` would consult the real home directory and
  // silently ignore a root recorded for a custom storage dir.
  const policyRoot = root && root.trim() ? root : storageDir;
  const configured = readBridgePolicy(policyRoot).worktreeRoot;
  if (typeof configured === 'string' && configured.trim()) {
    const value = configured.trim();
    return path.isAbsolute(value) ? path.normalize(value) : path.resolve(policyRoot, value);
  }
  return path.join(storageDir, 'worktrees');
}
