import * as child_process from 'child_process';
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
 *
 * The lock also records the real agent child PID. On Windows a crashed host does
 * not kill its children, so judging liveness by the host PID alone would let a
 * surviving `dsh headless` (or codex/claude) keep editing the workspace while a
 * new round starts on top of it. An orphaned child therefore keeps the gate busy
 * until it exits — or, after a grace period, is killed before takeover.
 */

export interface ExecutionOwner {
  token: string;
  pid: number;
  /**
   * Real agent process spawned by `pid`. It survives a crashed host on Windows,
   * so it must be probed independently before another round may start.
   */
  childPid?: number;
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

export interface ExecutionGateOptions {
  /** Heartbeat age after which the owner stops being trusted (default 30s). */
  staleAfterMs?: number;
  /**
   * How long an orphaned child (host dead, agent still alive) may keep running
   * before it is killed and the gate is taken over (default 2 minutes).
   */
  orphanGraceMs?: number;
}

const LOCK_FILE_NAME = 'execution.lock';
const HEARTBEAT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 30_000;
const ORPHAN_GRACE_MS = 120_000;

export class ExecutionGate {
  private readonly lockFilePath: string;
  private readonly staleAfterMs: number;
  private readonly orphanGraceMs: number;
  private activeLease: ExecutionLease | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(storageDir?: string, options?: ExecutionGateOptions) {
    const dir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch {}
    this.lockFilePath = path.join(dir, LOCK_FILE_NAME);
    this.staleAfterMs = options?.staleAfterMs ?? STALE_AFTER_MS;
    this.orphanGraceMs = options?.orphanGraceMs ?? ORPHAN_GRACE_MS;
  }

  public getLockFilePath(): string {
    return this.lockFilePath;
  }

  /**
   * Attempt to become the one globally executing task.
   *
   * @returns a lease when acquired, `null` when another live process/round owns
   *   the gate. An orphaned agent child left behind by a crashed host is killed
   *   before the gate is taken over, so it cannot edit files concurrently.
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

        const verdict = this.evaluateOwner(owner);
        if (verdict.live) return null;
        // The host is gone but its agent child is still running (or hung): kill
        // it before taking the slot, otherwise two agents would edit the same
        // workspace at once.
        if (verdict.orphanChildPid) {
          this.killProcessTree(verdict.orphanChildPid);
        }
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
    return this.evaluateOwner(owner).live ? owner : undefined;
  }

  /** Raw owner record, including one whose host died (needed for orphan cleanup). */
  public getRawOwner(): ExecutionOwner | undefined {
    return this.readOwner();
  }

  public isBusy(): boolean {
    return this.getOwner() !== undefined;
  }

  /**
   * Record (or clear) the real agent child of this lease.
   *
   * Called by every execution path right after spawn and again when the child
   * exits, so a surviving orphan can be detected and killed after a host crash.
   */
  public attachChildPid(childPid?: number): void {
    if (!this.activeLease) return;
    const owner = this.readOwner();
    if (!owner || owner.token !== this.activeLease.token) return;
    owner.childPid = childPid && childPid > 0 ? childPid : undefined;
    try {
      fs.writeFileSync(this.lockFilePath, JSON.stringify(owner), 'utf8');
    } catch {}
  }

  /**
   * Kill the agent child recorded for `threadId`.
   *
   * By default (`force = false`), this acts only on genuine orphans (host process dead)
   * so a live host can handle cancellation on its own. When `force = true` (e.g. from
   * `pauseTask` or `deleteTask`), the agent child is forcefully killed immediately
   * regardless of whether the host is alive, preventing retry loops or frozen agents
   * from holding the global execution gate.
   *
   * @returns whether an agent child was killed.
   */
  public killOrphanChild(threadId: string, force: boolean = false): boolean {
    const owner = this.readOwner();
    if (!owner || owner.threadId !== threadId) return false;
    if (!owner.childPid) return false;
    // When not forced, leave a live host to its own runner.
    if (!force && this.isPidAlive(owner.pid, true)) return false;
    if (!this.isPidAlive(owner.childPid, false)) return false;

    console.warn(
      `[ExecutionGate] Killing ${force ? 'active' : 'orphaned'} agent process ${owner.childPid} for ${threadId} ` +
        `(round ${owner.round})${force ? ' (force requested)' : `: its host process ${owner.pid} is gone.`}`,
    );
    this.killProcessTree(owner.childPid);
    owner.childPid = undefined;
    try {
      fs.writeFileSync(this.lockFilePath, JSON.stringify(owner), 'utf8');
    } catch {}
    return true;
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

  /**
   * Decides whether the recorded owner still represents work in flight.
   *
   * Priority: a fresh heartbeat wins; then a live host PID; then — for a bounded
   * grace period — a live agent child. Past that grace the child is orphaned and
   * the caller is expected to kill it before taking over.
   */
  private evaluateOwner(owner: ExecutionOwner): {
    live: boolean;
    orphanChildPid?: number;
  } {
    const heartbeatAt = owner.updatedAt || owner.acquiredAt || 0;
    const age = heartbeatAt ? Date.now() - heartbeatAt : Number.POSITIVE_INFINITY;

    if (heartbeatAt && age <= this.staleAfterMs) return { live: true };

    if (this.isPidAlive(owner.pid, true)) return { live: true };

    const childAlive = owner.childPid ? this.isPidAlive(owner.childPid, false) : false;
    if (childAlive && age <= this.orphanGraceMs) return { live: true };
    if (childAlive) return { live: false, orphanChildPid: owner.childPid };

    return { live: false };
  }

  private isPidAlive(pid: number, isHostPid: boolean): boolean {
    if (!pid || pid <= 0) return false;
    // A lock written by this PID is ours: freshness was already evaluated by the
    // caller, and we must never delete a lease we are actively holding.
    if (isHostPid && pid === process.pid) return true;

    try {
      process.kill(pid, 0);
      return true;
    } catch (err: any) {
      // EPERM means the process exists but we may not signal it.
      return err?.code === 'EPERM';
    }
  }

  private killProcessTree(pid: number): void {
    if (!pid || !this.isPidAlive(pid, false)) return;
    try {
      if (process.platform === 'win32') {
        child_process.execFileSync('taskkill', ['/pid', String(pid), '/T', '/F'], {
          stdio: 'ignore',
          windowsHide: true,
        });
        return;
      }
      process.kill(pid, 'SIGKILL');
    } catch {}
  }

  private isLockFileStale(): boolean {
    try {
      const stat = fs.statSync(this.lockFilePath);
      return Date.now() - stat.mtimeMs > this.staleAfterMs;
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
