import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { killPidTree } from './platform';

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
