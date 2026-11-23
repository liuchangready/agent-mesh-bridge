import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { killPidTree } from './platform';
import { DelegationThread } from './types';

/** Legacy record shape retained for reading execution.lock/leases only. */
export interface ExecutionOwner {
  token: string;
  pid: number;
  childPid?: number;
  threadId: string;
  round: number;
  acquiredAt: number;
  updatedAt: number;
  workspace?: string;
  agent?: string;
  lockPath?: string;
}

/** Legacy type retained for callers reading old ledgers; never acquired. */
export interface ExecutionLease {
  token: string;
  threadId: string;
  round: number;
  lockPath: string;
  workspace?: string;
}

function isPidAlive(pid?: number): boolean {
  if (!pid || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    return error?.code === 'EPERM';
  }
}

/** Kept for policy/path normalization callers; this no longer determines locks. */
export function normalizeWorkspaceKey(workspacePath?: string): string {
  const raw = (workspacePath || '').trim();
  if (!raw) return '';
  let resolved: string;
  try {
    resolved = path.resolve(raw);
  } catch {
    resolved = raw;
  }
  return resolved.replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase() || '/';
}

/**
 * Read-only compatibility adapter for children left by pre-registry builds.
 * It never reserves a slot, writes a lock, or blocks another task.
 */
export class LegacyExecutionRecords {
  private readonly storageDir: string;
  private readonly orphanGraceMs: number;

  constructor(storageDir?: string, options?: { orphanGraceMs?: number }) {
    this.storageDir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.orphanGraceMs = options?.orphanGraceMs ?? 120_000;
  }

  /** Kill only old-build child processes recorded for this exact thread. */
  public killOrphanChild(threadId: string, force: boolean = false): boolean {
    let killed = false;
    for (const owner of this.readLegacyOwners()) {
      if (owner.threadId !== threadId || !owner.childPid || !isPidAlive(owner.childPid)) continue;
      const age = Math.max(0, Date.now() - (owner.updatedAt || owner.acquiredAt || 0));
      if (!force && (isPidAlive(owner.pid) || age < this.orphanGraceMs)) continue;
      try {
        killPidTree(owner.childPid);
        if (!isPidAlive(owner.childPid)) killed = true;
      } catch (error: any) {
        console.warn(
          `[LegacyExecutionCleanup] Could not kill child ${owner.childPid} for ${threadId}: ${error?.message || error}`,
        );
      }
    }
    return killed;
  }

  private readLegacyOwners(): ExecutionOwner[] {
    const files = [path.join(this.storageDir, 'execution.lock')];
    const leasesDir = path.join(this.storageDir, 'leases');
    try {
      files.push(
        ...fs
          .readdirSync(leasesDir)
          .filter((name) => name.endsWith('.lock'))
          .map((name) => path.join(leasesDir, name)),
      );
    } catch {}

    const owners: ExecutionOwner[] = [];
    for (const file of files) {
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (
          parsed &&
          typeof parsed.token === 'string' &&
          typeof parsed.pid === 'number' &&
          typeof parsed.threadId === 'string' &&
          typeof parsed.round === 'number'
        ) {
          owners.push({ ...parsed, lockPath: file } as ExecutionOwner);
        }
      } catch {}
    }
    return owners;
  }
}

export type ExecutionEligibilityCode =
  | 'NO_EXECUTION'
  | 'LEGACY_VERSION_1'
  | 'UNKNOWN_VERSION'
  | 'CLEANUP_IN_PROGRESS'
  | 'ARCHIVED'
  | 'TERMINAL_STATUS'
  | 'INVALID_STRUCTURE';

export type ExecutionEligibilityResult =
  | { eligible: true; version: 2 }
  | {
      eligible: false;
      code: ExecutionEligibilityCode;
      reason: string;
    };

/** Throw a stable, explicit error at execution entry points for historical or
 * unknown records. Read, archive, delete, and stop paths must not call this. */
export function requireV2ExecutionEligibility(
  thread: DelegationThread | undefined | null,
  action: string,
): asserts thread is DelegationThread {
  const result = validateV2ExecutionEligibility(thread);
  if (!result.eligible) {
    throw new Error(`HISTORICAL_TASK_READ_ONLY: Cannot ${action}: ${result.code}: ${result.reason}`);
  }
}

/**
 * Validate execution eligibility under the v2 execution architecture.
 *
 * Rules:
 * - Tasks undergoing cleanup (cleanup marker present) cannot execute.
 * - Archived tasks cannot execute.
 * - Tasks without execution control record (legacy tasks) cannot execute under v2.
 * - Version 1 tasks cannot execute under v2.
 * - Unknown execution versions are explicitly rejected.
 * - Valid execution.version=2 tasks are accepted.
 * - Terminal states (completed) cannot execute.
 */
export function validateV2ExecutionEligibility(
  thread: DelegationThread | undefined | null,
): ExecutionEligibilityResult {
  if (!thread) {
    return {
      eligible: false,
      code: 'INVALID_STRUCTURE',
      reason: 'Task thread does not exist.',
    };
  }

  if (thread.cleanup) {
    return {
      eligible: false,
      code: 'CLEANUP_IN_PROGRESS',
      reason: `Task ${thread.threadId} is currently reserved for cleanup (${thread.cleanup.action}) by token ${thread.cleanup.token}. Execution is prohibited.`,
    };
  }

  if (thread.archivedAt) {
    return {
      eligible: false,
      code: 'ARCHIVED',
      reason: `Task ${thread.threadId} was archived at ${thread.archivedAt} and cannot execute.`,
    };
  }

  if (!thread.execution) {
    return {
      eligible: false,
      code: 'NO_EXECUTION',
      reason: `Task ${thread.threadId} has no execution control record (legacy task). Old tasks cannot execute under v2.`,
    };
  }

  const version = (thread.execution as any).version;
  if (version === 1) {
    return {
      eligible: false,
      code: 'LEGACY_VERSION_1',
      reason: `Task ${thread.threadId} is on execution version 1. Version 1 tasks cannot execute under v2.`,
    };
  }

  if (version !== 2) {
    return {
      eligible: false,
      code: 'UNKNOWN_VERSION',
      reason: `Task ${thread.threadId} has unsupported execution version ${String(version)}. Only execution.version=2 is supported for new execution.`,
    };
  }

  if (
    typeof thread.execution.revision !== 'number' ||
    !thread.execution.operationId ||
    !thread.execution.phase
  ) {
    return {
      eligible: false,
      code: 'INVALID_STRUCTURE',
      reason: `Task ${thread.threadId} has an invalid v2 execution record structure.`,
    };
  }

  if (thread.status === 'completed') {
    return {
      eligible: false,
      code: 'TERMINAL_STATUS',
      reason: `Task ${thread.threadId} is completed and cannot be executed.`,
    };
  }

  return {
    eligible: true,
    version: 2,
  };
}
