import * as child_process from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { killPidTree } from './platform';

/**
 * Observation record for a task execution. These files describe work that was
 * admitted by TaskRoleLock; they do not reserve a process, workspace, or slot.
 */
export interface TaskExecutionOwner {
  token: string;
  pid: number;
  childPid?: number;
  threadId: string;
  round: number;
  /**
   * Which side of the task this process runs. Only `developer` (the delegated
   * task's own receiver) participates in receiver detection: a reviewer or
   * bootstrap round is the caller and must keep its forwarding tools.
   */
  role?: TaskExecutionRole;
  acquiredAt: number;
  updatedAt: number;
  workspace?: string;
  agent?: string;
  recordPath: string;
}

export type TaskExecutionRole = 'developer' | 'reviewer' | 'bootstrap' | 'maintenance';

export interface TaskExecutionRegistration {
  token: string;
  threadId: string;
  round: number;
  recordPath: string;
}

export interface TaskExecutionRegistryOptions {
  staleAfterMs?: number;
  orphanGraceMs?: number;
}

const EXECUTIONS_DIR = 'executions';
const HEARTBEAT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 30_000;
const ORPHAN_GRACE_MS = 120_000;

function isPidAlive(pid?: number): boolean {
  if (!pid || pid <= 0) return false;
  if (pid === process.pid) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error: any) {
    return error?.code === 'EPERM';
  }
}

function safeThreadId(threadId: string): string {
  return String(threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');
}

export class TaskExecutionRegistry {
  private readonly dir: string;
  private readonly staleAfterMs: number;
  private readonly orphanGraceMs: number;
  private readonly registrations = new Map<string, TaskExecutionRegistration>();
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(storageDir?: string, options?: TaskExecutionRegistryOptions) {
    const base = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.dir = path.join(base, EXECUTIONS_DIR);
    this.staleAfterMs = options?.staleAfterMs ?? STALE_AFTER_MS;
    this.orphanGraceMs = options?.orphanGraceMs ?? ORPHAN_GRACE_MS;
    try {
      fs.mkdirSync(this.dir, { recursive: true });
    } catch {}
  }

  /** Record an execution after its task role has already been acquired. */
  public begin(
    threadId: string,
    round: number,
    agent?: string,
    workspace?: string,
    role?: TaskExecutionRole,
  ): TaskExecutionRegistration {
    const recordPath = this.recordPath(threadId);
    const now = Date.now();
    const owner: TaskExecutionOwner = {
      token: crypto.randomUUID(),
      pid: process.pid,
      threadId,
      round,
      acquiredAt: now,
      updatedAt: now,
      ...(workspace ? { workspace } : {}),
      ...(agent ? { agent } : {}),
      ...(role ? { role } : {}),
      recordPath,
    };
    const registration: TaskExecutionRegistration = {
      token: owner.token,
      threadId,
      round,
      recordPath,
    };

    // This is deliberately an ordinary write, not an atomic create. Role lock
    // admission provides exclusion; this record is only for observation/recovery.
    try {
      fs.writeFileSync(recordPath, JSON.stringify(owner), { encoding: 'utf8', mode: 0o600 });
    } catch (error: any) {
      throw new Error(
        `Could not record execution for task ${threadId}; refusing to start an untracked child: ${error?.message || error}`,
        { cause: error },
      );
    }
    this.registrations.set(registration.token, registration);
    this.startHeartbeat();
    return registration;
  }

  /** Attach or clear the spawned CLI PID for this exact task registration. */
  public attachChildPid(
    childPid: number | undefined,
    registration: TaskExecutionRegistration,
  ): void {
    const owner = this.readOwner(registration.recordPath);
    if (!owner || owner.token !== registration.token) return;
    owner.childPid = childPid && childPid > 0 ? childPid : undefined;
    owner.updatedAt = Date.now();
    this.writeOwner(owner);
  }

  /** End a registration; a stale process cannot delete a newer record. */
  public end(registration: TaskExecutionRegistration): void {
    this.registrations.delete(registration.token);
    // Only this registry's own registration files may be ended. Legacy
    // execution.lock/leases records are observation and stop-compatibility
    // inputs; do not mutate or delete them here.
    if (registration.recordPath !== this.recordPath(registration.threadId)) {
      if (this.registrations.size === 0) this.stopHeartbeat();
      return;
    }
    const owner = this.readOwner(registration.recordPath);
    if (owner?.token === registration.token) {
      try {
        fs.unlinkSync(registration.recordPath);
      } catch {}
    }
    if (this.registrations.size === 0) this.stopHeartbeat();
  }

  public getOwner(threadId: string): TaskExecutionOwner | undefined {
    const recordPath = this.recordPath(threadId);
    const owner = this.readOwner(recordPath);
    if (owner && this.isOwnerLive(owner)) return owner;
    return this.readLegacyOwners().find(
      (legacy) => legacy.threadId === threadId && this.isOwnerLive(legacy),
    );
  }

  /**
   * Find the live execution whose spawned CLI process is `hostPid`.
   *
   * Why this exists: `MESH_TASK_RECEIVER` is only visible to an MCP server that
   * inherits its environment. Codex passes ONLY the `env` table from
   * `config.toml` to the servers it spawns and DeepSeek Harness scrubs inherited
   * variables, so the receiver marker never arrives and the forwarding guard
   * silently disables itself. The bridge server is still a direct child of the
   * CLI it launched, though, so matching that pid identifies the same process
   * without any cooperation from the host.
   *
   * The record is only trusted while it is live: a stale execution file left by a
   * crashed round must never mark an unrelated new process as a receiver.
   *
   * Only `developer` records qualify. A reviewer or bootstrap round is registered
   * with the same thread and also binds its CLI pid, but that process is the
   * caller: treating it as a receiver would strip `dispatch_task` from the very
   * conversation whose job is to confirm the task.
   */
  public findByChildPid(hostPid: number | undefined): TaskExecutionOwner | undefined {
    if (!hostPid || !Number.isFinite(hostPid)) return undefined;
    const candidates: TaskExecutionOwner[] = [];
    try {
      for (const name of fs.readdirSync(this.dir)) {
        if (!name.endsWith('.json')) continue;
        const owner = this.readOwner(path.join(this.dir, name));
        if (owner) candidates.push(owner);
      }
    } catch {}
    candidates.push(...this.readLegacyOwners());

    let found: TaskExecutionOwner | undefined;
    for (const owner of candidates) {
      if (owner.childPid !== hostPid) continue;
      if (owner.role !== 'developer') continue;
      if (!this.isOwnerLive(owner)) continue;
      // A pid can be recycled; the newest live record wins, exactly like the
      // reviewer bootstrap host-pid channel.
      if (!found || owner.updatedAt > found.updatedAt) found = owner;
    }
    return found;
  }

  public listOwners(): TaskExecutionOwner[] {
    let names: string[] = [];
    try {
      names = fs.readdirSync(this.dir).filter((name) => name.endsWith('.json'));
    } catch {}
    const owners: TaskExecutionOwner[] = [];
    for (const name of names) {
      const owner = this.readOwner(path.join(this.dir, name));
      if (owner && this.isOwnerLive(owner)) {
        owners.push(owner);
      }
    }
    return owners;
  }

  /**
   * Kill only this task's recorded child. Force is used by pause/delete; normal
   * orphan recovery waits out the grace period and never affects other tasks.
   */
  public killOrphanChild(threadId: string, force: boolean = false): boolean {
    const records = [this.readOwner(this.recordPath(threadId)), ...this.readLegacyOwners()]
      .filter((owner): owner is TaskExecutionOwner => !!owner && owner.threadId === threadId);
    let killed = false;
    for (const owner of records) {
      if (!owner.childPid || !isPidAlive(owner.childPid)) continue;
      const hostAlive = isPidAlive(owner.pid);
      const heartbeatAt = owner.updatedAt || owner.acquiredAt || 0;
      const age = heartbeatAt ? Math.max(0, Date.now() - heartbeatAt) : Number.POSITIVE_INFINITY;
      if (!force && (hostAlive || age < this.orphanGraceMs)) continue;

      try {
        killPidTree(owner.childPid);
      } catch (error: any) {
        console.warn(
          `[TaskExecutionRegistry] Could not kill child ${owner.childPid} for ${threadId}: ${error?.message || error}`,
        );
        continue;
      }
      if (isPidAlive(owner.childPid)) continue;
      owner.childPid = undefined;
      owner.updatedAt = Date.now();
      // Preserve legacy lease files for audit/compatibility; new records can be
      // updated in place because their path is under `executions/`.
      if (owner.recordPath.startsWith(`${this.dir}${path.sep}`)) this.writeOwner(owner);
      killed = true;
    }
    return killed;
  }

  /** Stop local timers only; never kill a child as a side effect of disposal. */
  public dispose(): void {
    this.stopHeartbeat();
  }

  private recordPath(threadId: string): string {
    return path.join(this.dir, `${safeThreadId(threadId)}.json`);
  }

  private readOwner(recordPath: string): TaskExecutionOwner | undefined {
    try {
      const parsed = JSON.parse(fs.readFileSync(recordPath, 'utf8'));
      if (
        parsed &&
        typeof parsed.token === 'string' &&
        typeof parsed.threadId === 'string' &&
        typeof parsed.pid === 'number' &&
        typeof parsed.round === 'number'
      ) {
        return { ...parsed, recordPath } as TaskExecutionOwner;
      }
    } catch {}
    return undefined;
  }

  /** Read old execution.lock/leases records for same-thread recovery and stop. */
  private readLegacyOwners(): TaskExecutionOwner[] {
    const base = path.dirname(this.dir);
    const files = [path.join(base, 'execution.lock')];
    try {
      files.push(
        ...fs
          .readdirSync(path.join(base, 'leases'))
          .filter((name) => name.endsWith('.lock'))
          .map((name) => path.join(base, 'leases', name)),
      );
    } catch {}

    const owners: TaskExecutionOwner[] = [];
    for (const file of files) {
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
        if (
          parsed &&
          typeof parsed.token === 'string' &&
          typeof parsed.threadId === 'string' &&
          typeof parsed.pid === 'number' &&
          typeof parsed.round === 'number'
        ) {
          owners.push({ ...parsed, recordPath: file } as TaskExecutionOwner);
        }
      } catch {}
    }
    return owners;
  }

  private writeOwner(owner: TaskExecutionOwner): void {
    try {
      fs.writeFileSync(owner.recordPath, JSON.stringify(owner), { encoding: 'utf8', mode: 0o600 });
    } catch {}
  }

  private isOwnerLive(owner: TaskExecutionOwner): boolean {
    const heartbeatAt = owner.updatedAt || owner.acquiredAt || 0;
    const age = heartbeatAt ? Math.max(0, Date.now() - heartbeatAt) : Number.POSITIVE_INFINITY;
    if (heartbeatAt && age <= this.staleAfterMs) return true;
    if (isPidAlive(owner.pid)) return true;
    if (!owner.childPid || !isPidAlive(owner.childPid)) return false;
    if (age <= this.orphanGraceMs) return true;

    // Expired orphans must not disappear from observation while their child is
    // still alive: the task role lock would keep the same task blocked forever.
    // Reap only this thread's child, and keep reporting it as live if the OS has
    // not actually terminated it yet.
    this.killOrphanChild(owner.threadId, false);
    if (isPidAlive(owner.childPid)) return true;
    const current = this.readOwner(owner.recordPath);
    if (current && isPidAlive(current.pid)) return true;
    return false;
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      for (const registration of this.registrations.values()) {
        const owner = this.readOwner(registration.recordPath);
        if (!owner || owner.token !== registration.token) {
          this.registrations.delete(registration.token);
          continue;
        }
        owner.updatedAt = Date.now();
        this.writeOwner(owner);
      }
      if (this.registrations.size === 0) this.stopHeartbeat();
    }, HEARTBEAT_INTERVAL_MS);
    this.heartbeatTimer.unref?.();
  }

  private stopHeartbeat(): void {
    if (!this.heartbeatTimer) return;
    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }
}
