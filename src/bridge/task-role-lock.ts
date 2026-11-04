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
  /**
   * Dedicated-reviewer mode: the native reviewer conversation this role is bound
   * to. The lock file is keyed by this id, not by the thread, so two tasks can
   * never write the same reviewer conversation and a stale process is rejected
   * by session identity (not just by thread).
   */
  conversationId?: string;
  /** Process that acquired (and heartbeats) the lock. */
  pid: number;
  /**
   * The detached caller/reviewer child this role was handed to.
   *
   * The wake spawns the reviewer detached and unref'd so it outlives the bridge
   * process that launched it. That makes the launcher a bad liveness signal: when
   * the extension host restarts mid-review, the launcher's heartbeat stops while
   * the reviewer is still running, and every review decision would be refused.
   * The child pid keeps the role valid for exactly as long as the reviewer runs.
   */
  childPid?: number;
  acquiredAt: number;
  updatedAt: number;
}

/** Liveness probe that never throws: EPERM means the pid exists. */
function isPidAlive(pid?: number): boolean {
  if (!pid || pid <= 0) return false;
  if (pid === process.pid) return true;
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: any) {
    return err?.code === 'EPERM';
  }
}

export interface TaskRoleLease {
  token: string;
  threadId: string;
  role: TaskRole;
  conversationId?: string;
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

  public tryAcquire(
    threadId: string,
    role: TaskRole,
    conversationId?: string,
  ): TaskRoleLease | null {
    if (this.activeLease) return null;

    const file = this.filePath(threadId, conversationId);
    const payload: TaskRoleOwner = {
      token: crypto.randomUUID(),
      threadId,
      role,
      ...(conversationId ? { conversationId } : {}),
      pid: process.pid,
      acquiredAt: Date.now(),
      updatedAt: Date.now(),
    };

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        fs.writeFileSync(file, JSON.stringify(payload), { encoding: 'utf8', flag: 'wx' });
        const lease: TaskRoleLease = {
          token: payload.token,
          threadId,
          role,
          ...(conversationId ? { conversationId } : {}),
        };
        this.activeLease = lease;
        this.startHeartbeat(lease);
        return lease;
      } catch (err: any) {
        if (err?.code !== 'EEXIST') return null;

        const owner = this.readOwnerFile(file);
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
    const threadOwner = this.readOwner(threadId);
    if (threadOwner && this.isOwnerLive(threadOwner)) return threadOwner;
    // Dedicated review locks live under the reviewer conversation id. The
    // directory is tiny (one file per active role), so a scan is the simplest
    // cross-process index that cannot go stale.
    for (const name of this.listLockFiles()) {
      if (!name.startsWith('conv_')) continue;
      const owner = this.readOwnerFile(path.join(this.dir, name));
      if (!owner || owner.threadId !== threadId) continue;
      if (this.isOwnerLive(owner)) return owner;
    }
    return undefined;
  }

  public release(lease?: TaskRoleLease): void {
    const target = lease || this.activeLease;
    this.stopHeartbeat();
    this.activeLease = null;
    if (!target) return;

    const file = this.filePath(target.threadId, target.conversationId);
    const owner = this.readOwnerFile(file);
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
      fs.unlinkSync(this.filePath(owner.threadId, owner.conversationId));
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Hand a freshly spawned detached caller child to the role lock.
   *
   * Called by the wake path right after the reviewer process starts: from then on
   * the role is alive while EITHER the launcher heartbeats OR the child runs, so
   * a bridge restart cannot invalidate a review that is still in progress.
   */
  public attachChild(lease: TaskRoleLease | null, childPid: number | undefined): boolean {
    if (!lease || !childPid) return false;
    const file = this.filePath(lease.threadId, lease.conversationId);
    const owner = this.readOwnerFile(file);
    if (!owner || owner.token !== lease.token) return false;
    try {
      owner.childPid = childPid;
      owner.updatedAt = Date.now();
      fs.writeFileSync(file, JSON.stringify(owner), 'utf8');
      return true;
    } catch {
      return false;
    }
  }

  public describe(owner?: TaskRoleOwner): string {
    if (!owner) return 'another role';
    const conversation = owner.conversationId ? `, conversation ${owner.conversationId}` : '';
    return owner.childPid
      ? `${owner.role} (pid ${owner.pid}${conversation}, reviewer child ${owner.childPid})`
      : `${owner.role} (pid ${owner.pid}${conversation})`;
  }

  public getLockFilePath(threadId: string): string {
    return this.filePath(threadId);
  }

  private filePath(threadId: string, conversationId?: string): string {
    if (conversationId) {
      const hash = crypto.createHash('sha1').update(conversationId).digest('hex').slice(0, 24);
      return path.join(this.dir, `conv_${hash}.json`);
    }
    const safe = threadId.replace(/[^A-Za-z0-9_.-]/g, '_');
    return path.join(this.dir, `${safe}.json`);
  }

  private listLockFiles(): string[] {
    try {
      return fs.readdirSync(this.dir).filter((name) => name.endsWith('.json'));
    } catch {
      return [];
    }
  }

  private readOwner(threadId: string): TaskRoleOwner | undefined {
    return this.readOwnerFile(this.filePath(threadId));
  }

  private readOwnerFile(file: string): TaskRoleOwner | undefined {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, 'utf8'));
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
    const heartbeatFresh = !!heartbeatAt && Date.now() - heartbeatAt <= STALE_AFTER_MS;
    // A running detached reviewer outranks a silent heartbeat: the launcher may
    // have exited (VS Code restart, crashed bridge process) while the caller CLI
    // keeps reviewing. Without this the review's own approve/rework calls are
    // rejected as "stale reviewer".
    if (isPidAlive(owner.childPid)) return true;
    if (!heartbeatFresh) return false;
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
      const current = this.readOwnerFile(file);
      if (current?.token === owner.token) {
        fs.unlinkSync(file);
      }
    } catch {}
  }

  private startHeartbeat(lease: TaskRoleLease): void {
    if (this.heartbeatTimer) return;
    const file = this.filePath(lease.threadId, lease.conversationId);
    this.heartbeatTimer = setInterval(() => {
      const current = this.activeLease;
      if (!current) {
        this.stopHeartbeat();
        return;
      }

      const owner = this.readOwnerFile(file);
      if (!owner || owner.token !== current.token) {
        this.activeLease = null;
        this.stopHeartbeat();
        return;
      }

      owner.updatedAt = Date.now();
      try {
        fs.writeFileSync(file, JSON.stringify(owner), 'utf8');
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
