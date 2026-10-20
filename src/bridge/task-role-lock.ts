import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * Per-task role lock.
 *
 * A delegated task is a strict turn-taking protocol:
 *   reviewer (design/instruction) -> developer (development report) ->
 *   reviewer (review/rework report) -> ...
 *
 * At any moment only ONE role may be active for a thread. This lock prevents
 * two reviewer processes from inspecting the same task concurrently, and keeps
 * the developer from starting before the reviewer has handed off.
 */
export type TaskRole = 'reviewer' | 'developer';

export interface TaskRoleOwner {
  token: string;
  threadId: string;
  role: TaskRole;
  pid: number;
  acquiredAt: number;
  updatedAt: number;
}

export interface TaskRoleLease {
  token: string;
  threadId: string;
  role: TaskRole;
}

const ROLE_DIR = 'roles';
const HEARTBEAT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 30_000;

export class TaskRoleLock {
  private readonly dir: string;
  private activeLease: TaskRoleLease | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(storageDir?: string) {
    const base = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.dir = path.join(base, ROLE_DIR);
    try {
      fs.mkdirSync(this.dir, { recursive: true });
    } catch {}
  }

  public tryAcquire(threadId: string, role: TaskRole): TaskRoleLease | null {
    if (this.activeLease) return null;

    const file = this.filePath(threadId);
    const payload: TaskRoleOwner = {
      token: crypto.randomUUID(),
      threadId,
      role,
      pid: process.pid,
      acquiredAt: Date.now(),
      updatedAt: Date.now(),
    };

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        fs.writeFileSync(file, JSON.stringify(payload), { encoding: 'utf8', flag: 'wx' });
        const lease: TaskRoleLease = { token: payload.token, threadId, role };
        this.activeLease = lease;
        this.startHeartbeat(threadId);
        return lease;
      } catch (err: any) {
        if (err?.code !== 'EEXIST') return null;

        const owner = this.readOwner(threadId);
        if (!owner) {
          if (!this.isFileStale(file)) return null;
          this.removeStale(file, undefined);
          continue;
        }
        if (this.isOwnerLive(owner)) return null;
        this.removeStale(file, owner);
      }
    }

    return null;
  }

  public getOwner(threadId: string): TaskRoleOwner | undefined {
    const owner = this.readOwner(threadId);
    return owner && this.isOwnerLive(owner) ? owner : undefined;
  }

  public release(lease?: TaskRoleLease): void {
    const target = lease || this.activeLease;
    this.stopHeartbeat();
    this.activeLease = null;
    if (!target) return;

    const file = this.filePath(target.threadId);
    const owner = this.readOwner(target.threadId);
    if (!owner || owner.token !== target.token) return;
    try {
      fs.unlinkSync(file);
    } catch {}
  }

  /**
   * Hand off a role without knowing the lease token.
   *
   * The reviewer's MCP server calls this after `request_rework` / `approve_task`
   * succeeds: the reviewer's decision is complete, so the developer may start
   * even if the reviewer CLI process is still printing its final message.
   */
  public releaseIfRole(threadId: string, role: TaskRole): boolean {
    const owner = this.getOwner(threadId);
    if (!owner || owner.role !== role) return false;
    try {
      fs.unlinkSync(this.filePath(threadId));
      return true;
    } catch {
      return false;
    }
  }

  public describe(owner?: TaskRoleOwner): string {
    return owner ? `${owner.role} (pid ${owner.pid})` : 'another role';
  }

  public getLockFilePath(threadId: string): string {
    return this.filePath(threadId);
  }

  private filePath(threadId: string): string {
    const safe = threadId.replace(/[^A-Za-z0-9_.-]/g, '_');
    return path.join(this.dir, `${safe}.json`);
  }

  private readOwner(threadId: string): TaskRoleOwner | undefined {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.filePath(threadId), 'utf8'));
      if (
        parsed &&
        typeof parsed.token === 'string' &&
        typeof parsed.threadId === 'string' &&
        (parsed.role === 'reviewer' || parsed.role === 'developer')
      ) {
        return parsed as TaskRoleOwner;
      }
    } catch {}
    return undefined;
  }

  private isOwnerLive(owner: TaskRoleOwner): boolean {
    const heartbeatAt = owner.updatedAt || owner.acquiredAt || 0;
    if (!heartbeatAt || Date.now() - heartbeatAt > STALE_AFTER_MS) return false;
    if (!owner.pid) return false;
    if (owner.pid === process.pid) return true;
    try {
      process.kill(owner.pid, 0);
      return true;
    } catch (err: any) {
      return err?.code === 'EPERM';
    }
  }

  private isFileStale(file: string): boolean {
    try {
      return Date.now() - fs.statSync(file).mtimeMs > STALE_AFTER_MS;
    } catch {
      return false;
    }
  }

  private removeStale(file: string, owner?: TaskRoleOwner): void {
    try {
      if (!owner) {
        if (this.isFileStale(file)) fs.unlinkSync(file);
        return;
      }
      const current = this.readOwner(owner.threadId);
      if (current?.token === owner.token) {
        fs.unlinkSync(file);
      }
    } catch {}
  }

  private startHeartbeat(threadId: string): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      const lease = this.activeLease;
      if (!lease) {
        this.stopHeartbeat();
        return;
      }

      const owner = this.readOwner(threadId);
      if (!owner || owner.token !== lease.token) {
        this.activeLease = null;
        this.stopHeartbeat();
        return;
      }

      owner.updatedAt = Date.now();
      try {
        fs.writeFileSync(this.filePath(threadId), JSON.stringify(owner), 'utf8');
      } catch {}
    }, HEARTBEAT_INTERVAL_MS);
    this.heartbeatTimer.unref?.();
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }
}
