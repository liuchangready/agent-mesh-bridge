import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * Cross-process single-flight gate for delegated task execution.
 *
 * A delegation round spawns a real agent process that edits the workspace. Two
 * such processes must never run at the same time, even when they were dispatched
 * by different MCP server processes (each agent spawns its own bridge server).
 * The only shared authority between those processes is the filesystem, so the
 * winner is decided by an atomically-created lock file.
 *
 * The lock is refreshed every few seconds. A lock whose heartbeat stopped (or
 * whose owner PID is gone) is stale and may be reclaimed, so a crashed agent
 * cannot block the queue forever.
 */

export interface ExecutionOwner {
  token: string;
  pid: number;
  threadId: string;
  round: number;
  acquiredAt: number;
  updatedAt: number;
}

export interface ExecutionLease {
  token: string;
  threadId: string;
  round: number;
}

const LOCK_FILE_NAME = 'execution.lock';
const HEARTBEAT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 30_000;

export class ExecutionGate {
  private readonly lockFilePath: string;
  private activeLease: ExecutionLease | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(storageDir?: string) {
    const dir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch {}
    this.lockFilePath = path.join(dir, LOCK_FILE_NAME);
  }

  public getLockFilePath(): string {
    return this.lockFilePath;
  }

  /**
   * Attempt to become the one globally executing task.
   *
   * @returns a lease when acquired, `null` when another live process/round owns
   *   the gate.
   */
  public tryAcquire(threadId: string, round: number): ExecutionLease | null {
    if (this.activeLease) return null;

    const payload: ExecutionOwner = {
      token: crypto.randomUUID(),
      pid: process.pid,
      threadId,
      round,
      acquiredAt: Date.now(),
      updatedAt: Date.now(),
    };

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        // `wx` is the atomic test-and-set: exactly one creator wins.
        fs.writeFileSync(this.lockFilePath, JSON.stringify(payload), {
          encoding: 'utf8',
          flag: 'wx',
        });
        const lease: ExecutionLease = { token: payload.token, threadId, round };
        this.activeLease = lease;
        this.startHeartbeat();
        return lease;
      } catch (err: any) {
        if (err?.code !== 'EEXIST') return null;

        const owner = this.readOwner();
        if (!owner) {
          // Another process may have just created the file but not finished
          // writing it. Do not delete a fresh, still-empty lock; only reclaim a
          // corrupt one after the stale timeout.
          if (!this.isLockFileStale()) return null;
          this.removeStaleLock(undefined);
          continue;
        }
        if (this.isOwnerLive(owner)) return null;
        this.removeStaleLock(owner);
      }
    }

    return null;
  }

  /**
   * The current live owner, or `undefined` when the gate is free (including a
   * stale lock that has not been reclaimed yet).
   */
  public getOwner(): ExecutionOwner | undefined {
    const owner = this.readOwner();
    if (!owner) return undefined;
    return this.isOwnerLive(owner) ? owner : undefined;
  }

  public isBusy(): boolean {
    return this.getOwner() !== undefined;
  }

  /** Release a lease. Only the process holding that exact token may delete it. */
  public release(lease?: ExecutionLease): void {
    const target = lease || this.activeLease;
    this.stopHeartbeat();
    this.activeLease = null;
    if (!target) return;

    const owner = this.readOwner();
    if (!owner || owner.token !== target.token) return;
    try {
      fs.unlinkSync(this.lockFilePath);
    } catch {}
  }

  public describeOwner(owner?: ExecutionOwner): string {
    return owner
      ? `task ${owner.threadId} (round ${owner.round})`
      : 'another delegated task';
  }

  private readOwner(): ExecutionOwner | undefined {
    try {
      const raw = fs.readFileSync(this.lockFilePath, 'utf8');
      const parsed = JSON.parse(raw);
      if (
        parsed &&
        typeof parsed.token === 'string' &&
        typeof parsed.threadId === 'string' &&
        typeof parsed.pid === 'number'
      ) {
        return parsed as ExecutionOwner;
      }
    } catch {}
    return undefined;
  }

  private isOwnerLive(owner: ExecutionOwner): boolean {
    const heartbeatAt = owner.updatedAt || owner.acquiredAt || 0;
    if (!heartbeatAt || Date.now() - heartbeatAt > STALE_AFTER_MS) return false;
    if (!owner.pid) return false;

    // A lock written by this PID is ours or a sibling gate in this process. Its
    // freshness is the only signal available (we must not delete a live lease).
    if (owner.pid === process.pid) return true;

    try {
      process.kill(owner.pid, 0);
      return true;
    } catch (err: any) {
      // EPERM means the process exists but we may not signal it.
      return err?.code === 'EPERM';
    }
  }

  private isLockFileStale(): boolean {
    try {
      const stat = fs.statSync(this.lockFilePath);
      return Date.now() - stat.mtimeMs > STALE_AFTER_MS;
    } catch {
      return false;
    }
  }

  private removeStaleLock(owner?: ExecutionOwner): void {
    try {
      if (!owner) {
        if (this.isLockFileStale()) {
          fs.unlinkSync(this.lockFilePath);
        }
        return;
      }
      // Guard against deleting a fresh lock that replaced the stale one between
      // our read and this call.
      const current = this.readOwner();
      if (current?.token === owner.token) {
        fs.unlinkSync(this.lockFilePath);
      }
    } catch {}
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      const lease = this.activeLease;
      if (!lease) {
        this.stopHeartbeat();
        return;
      }

      const owner = this.readOwner();
      if (!owner || owner.token !== lease.token) {
        // The lock was reclaimed or deleted: stop pretending we still own it.
        this.activeLease = null;
        this.stopHeartbeat();
        return;
      }

      owner.updatedAt = Date.now();
      try {
        fs.writeFileSync(this.lockFilePath, JSON.stringify(owner), 'utf8');
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
