import * as child_process from 'child_process';
import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * Cross-process execution gate for delegated task execution.
 *
 * A delegation round spawns a real agent process that edits the workspace. Two
 * such processes must never run at the same time, even when they were dispatched
 * by different MCP server processes (each agent spawns its own bridge server).
 * The only shared authority between those processes is the filesystem, so the
 * winner is decided by an atomically-created lock file.
 *
 * The gate can hold MORE THAN ONE lease, one per lock file. That is what makes
 * bounded concurrency possible: a lease is keyed by workspace, so two tasks in
 * two different workspaces can run at once while two tasks in the SAME workspace
 * still exclude each other. Calling `tryAcquire` without a workspace key keeps
 * the historical behaviour exactly: one global `execution.lock` and a single
 * mesh-wide execution slot.
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
  /** Normalized workspace key of a per-workspace lease (multi-lease mode). */
  workspace?: string;
  /** Agent executing the lease, used for per-agent concurrency limits. */
  agent?: string;
  /** Absolute path of the lock file backing this owner. */
  lockPath?: string;
}

export interface ExecutionLease {
  token: string;
  threadId: string;
  round: number;
  /**
   * Lock file this lease holds. The legacy single-flight lease points at
   * `<storageDir>/execution.lock`; a per-workspace lease points at
   * `<storageDir>/leases/<sha1(workspace).slice(0,16)>.lock`.
   */
  lockPath: string;
  /** Normalized workspace key; undefined for the legacy mesh-wide lease. */
  workspace?: string;
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
const LEASES_DIR_NAME = 'leases';
const HEARTBEAT_INTERVAL_MS = 5_000;
const STALE_AFTER_MS = 30_000;
const ORPHAN_GRACE_MS = 120_000;

/**
 * Canonical workspace identity used to pick a concurrency slot.
 *
 * Absolute + lowercased + forward slashes: the same repository reached through
 * two path spellings (`D:\repo`, `d:/repo/`, `D:\repo\sub\..`) must map to ONE
 * slot, otherwise the per-workspace exclusion would silently stop working.
 */
export function normalizeWorkspaceKey(workspacePath?: string): string {
  const raw = (workspacePath || '').trim();
  if (!raw) return '';
  let resolved: string;
  try {
    resolved = path.resolve(raw);
  } catch {
    resolved = raw;
  }
  const normalized = resolved.replace(/\\/g, '/').replace(/\/+$/, '').toLowerCase();
  // A filesystem root normalizes to an empty string; keep it distinct from
  // "no workspace given" so a root workspace still gets its own slot.
  return normalized || '/';
}

export class ExecutionGate {
  private readonly storageDir: string;
  private readonly lockFilePath: string;
  private readonly leasesDir: string;
  private readonly staleAfterMs: number;
  private readonly orphanGraceMs: number;
  /** Live leases owned by THIS process, keyed by the lock file they hold. */
  private activeLeases: Map<string, ExecutionLease> = new Map();
  private heartbeatTimer: NodeJS.Timeout | null = null;

  constructor(storageDir?: string, options?: ExecutionGateOptions) {
    const dir = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch {}
    this.storageDir = dir;
    this.lockFilePath = path.join(dir, LOCK_FILE_NAME);
    this.leasesDir = path.join(dir, LEASES_DIR_NAME);
    this.staleAfterMs = options?.staleAfterMs ?? STALE_AFTER_MS;
    this.orphanGraceMs = options?.orphanGraceMs ?? ORPHAN_GRACE_MS;
  }

  /**
   * The legacy mesh-wide lock path, or the per-workspace lock path when a
   * workspace key is given.
   */
  public getLockFilePath(workspaceKey?: string): string {
    return this.lockPathFor(workspaceKey);
  }

  /**
   * Attempt to become an executing task.
   *
   * Without `workspaceKey` this is the historical global single-flight gate: at
   * most one lease exists for the whole mesh, whoever asks second gets `null`.
   *
   * With `workspaceKey` the lease is scoped to that workspace, so another
   * workspace may acquire its own lease at the same time while a second request
   * for the SAME workspace is refused. Stale/orphan handling is evaluated per
   * lock file, independently.
   *
   * @returns a lease when acquired, `null` when another live process/round owns
   *   that slot. An orphaned agent child left behind by a crashed host is killed
   *   before the slot is taken over, so it cannot edit files concurrently.
   */
  public tryAcquire(
    threadId: string,
    round: number,
    workspaceKey?: string,
    agent?: string,
  ): ExecutionLease | null {
    const normalizedWorkspace = workspaceKey ? normalizeWorkspaceKey(workspaceKey) : undefined;
    const lockPath = normalizedWorkspace ? this.leasePathFor(normalizedWorkspace) : this.lockFilePath;

    // One lease per lock path per process: a slot is exclusive even inside the
    // same process (the supervisor may try to start two same-workspace tasks).
    if (this.activeLeases.has(lockPath)) return null;

    const payload: ExecutionOwner = {
      token: crypto.randomUUID(),
      pid: process.pid,
      threadId,
      round,
      acquiredAt: Date.now(),
      updatedAt: Date.now(),
      workspace: normalizedWorkspace,
      agent,
      lockPath,
    };

    if (lockPath !== this.lockFilePath) {
      try {
        fs.mkdirSync(this.leasesDir, { recursive: true });
      } catch {}
    }

    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        // `wx` is the atomic test-and-set: exactly one creator wins.
        fs.writeFileSync(lockPath, JSON.stringify(payload), {
          encoding: 'utf8',
          flag: 'wx',
        });
        const lease: ExecutionLease = {
          token: payload.token,
          threadId,
          round,
          lockPath,
          workspace: normalizedWorkspace,
        };
        this.activeLeases.set(lockPath, lease);
        this.startHeartbeat();
        return lease;
      } catch (err: any) {
        if (err?.code !== 'EEXIST') return null;

        const owner = this.readOwner(lockPath);
        if (!owner) {
          // Another process may have just created the file but not finished
          // writing it. Do not delete a fresh, still-empty lock; only reclaim a
          // corrupt one after the stale timeout.
          if (!this.isLockFileStale(lockPath)) return null;
          this.removeStaleLock(lockPath, undefined);
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
        this.removeStaleLock(lockPath, owner);
      }
    }

    return null;
  }

  /**
   * The live owner of one lock, or — when no workspace key is given — any live
   * owner anywhere (the historical "is the mesh busy" question).
   */
  public getOwner(workspaceKey?: string): ExecutionOwner | undefined {
    if (workspaceKey !== undefined) {
      return this.liveOwnerOf(this.lockPathFor(workspaceKey));
    }
    for (const lockPath of this.knownLockPaths()) {
      const owner = this.liveOwnerOf(lockPath);
      if (owner) return owner;
    }
    return undefined;
  }

  /** Raw owner record, including one whose host died (needed for orphan cleanup). */
  public getRawOwner(workspaceKey?: string): ExecutionOwner | undefined {
    if (workspaceKey !== undefined) {
      return this.readOwner(this.lockPathFor(workspaceKey));
    }
    for (const lockPath of this.knownLockPaths()) {
      const owner = this.readOwner(lockPath);
      if (owner) return owner;
    }
    return undefined;
  }

  public isBusy(workspaceKey?: string): boolean {
    return this.getOwner(workspaceKey) !== undefined;
  }

  /** Every live lease, whoever owns it (this process or another one). */
  public listOwners(): ExecutionOwner[] {
    const owners: ExecutionOwner[] = [];
    for (const lockPath of this.knownLockPaths()) {
      const owner = this.liveOwnerOf(lockPath);
      if (owner) owners.push(owner);
    }
    return owners;
  }

  /** How many live execution slots are taken right now. */
  public countActive(): number {
    return this.listOwners().length;
  }

  /**
   * Record (or clear) the real agent child of a lease.
   *
   * Called by every execution path right after spawn and again when the child
   * exits, so a surviving orphan can be detected and killed after a host crash.
   * The lease is preferred; without one this acts only when the process holds
   * exactly one lease, so it can never stamp its PID onto another task's lock.
   */
  public attachChildPid(childPid?: number, lease?: ExecutionLease): void {
    let lockPath: string | undefined;
    let token: string | undefined;
    if (lease) {
      lockPath = lease.lockPath || this.lockFilePath;
      token = lease.token;
    } else if (this.activeLeases.size === 1) {
      const only = Array.from(this.activeLeases.values())[0];
      lockPath = only.lockPath;
      token = only.token;
    }
    if (!lockPath || !token) return;

    const owner = this.readOwner(lockPath);
    if (!owner || owner.token !== token) return;
    owner.childPid = childPid && childPid > 0 ? childPid : undefined;
    owner.lockPath = lockPath;
    try {
      fs.writeFileSync(lockPath, JSON.stringify(owner), 'utf8');
    } catch {}
  }

  /**
   * Kill the agent child recorded for `threadId`.
   *
   * Every lease file is scanned, so a task whose lease lives under a workspace
   * slot is handled exactly like a legacy mesh-wide one.
   *
   * By default (`force = false`), this acts only on genuine orphans (host process dead)
   * so a live host can handle cancellation on its own. When `force = true` (e.g. from
   * `pauseTask` or `deleteTask`), the agent child is forcefully killed immediately
   * regardless of whether the host is alive, preventing retry loops or frozen agents
   * from holding an execution slot.
   *
   * @returns whether an agent child was killed.
   */
  public killOrphanChild(threadId: string, force: boolean = false): boolean {
    let killed = false;
    for (const lockPath of this.knownLockPaths()) {
      const owner = this.readOwner(lockPath);
      if (!owner || owner.threadId !== threadId) continue;
      if (!owner.childPid) continue;
      // When not forced, leave a live host to its own runner.
      if (!force && this.isPidAlive(owner.pid, true)) continue;
      if (!this.isPidAlive(owner.childPid, false)) continue;

      console.warn(
        `[ExecutionGate] Killing ${force ? 'active' : 'orphaned'} agent process ${owner.childPid} for ${threadId} ` +
          `(round ${owner.round})${force ? ' (force requested)' : `: its host process ${owner.pid} is gone.`}`,
      );
      this.killProcessTree(owner.childPid);
      owner.childPid = undefined;
      owner.lockPath = lockPath;
      try {
        fs.writeFileSync(lockPath, JSON.stringify(owner), 'utf8');
      } catch {}
      killed = true;
    }
    return killed;
  }

  /**
   * Release a lease.
   *
   * Without an argument, every lease this process holds is released. With one,
   * only that lease is released, and only when both its token and its lock path
   * still match (a lock another process already retook must not be deleted).
   */
  public release(lease?: ExecutionLease): void {
    const targets = lease ? [lease] : Array.from(this.activeLeases.values());
    for (const target of targets) {
      const lockPath = target.lockPath || this.lockFilePath;
      const held = this.activeLeases.get(lockPath);
      if (held && held.token !== target.token) continue;
      this.activeLeases.delete(lockPath);

      const owner = this.readOwner(lockPath);
      if (!owner || owner.token !== target.token) continue;
      try {
        fs.unlinkSync(lockPath);
      } catch {}
    }
    if (this.activeLeases.size === 0) this.stopHeartbeat();
  }

  public describeOwner(owner?: ExecutionOwner): string {
    return owner
      ? `task ${owner.threadId} (round ${owner.round})`
      : 'another delegated task';
  }

  /** `/leases/<sha1(normalizedWorkspace)[0..16]>.lock` */
  private leasePathFor(normalizedWorkspace: string): string {
    const digest = crypto
      .createHash('sha1')
      .update(normalizedWorkspace)
      .digest('hex')
      .slice(0, 16);
    return path.join(this.leasesDir, `${digest}.lock`);
  }

  /** Lock path for a workspace key, falling back to the mesh-wide lock. */
  private lockPathFor(workspaceKey?: string): string {
    const normalized = normalizeWorkspaceKey(workspaceKey);
    return normalized ? this.leasePathFor(normalized) : this.lockFilePath;
  }

  /** The legacy lock plus every per-workspace lease file present on disk. */
  private knownLockPaths(): string[] {
    const lockPaths: string[] = [this.lockFilePath];
    const seen = new Set<string>(lockPaths);
    try {
      const names = fs
        .readdirSync(this.leasesDir)
        .filter((name) => name.endsWith('.lock'))
        .sort();
      for (const name of names) {
        const lockPath = path.join(this.leasesDir, name);
        if (seen.has(lockPath)) continue;
        seen.add(lockPath);
        lockPaths.push(lockPath);
      }
    } catch {}
    // A lease this process still holds must be visible even if its file was
    // renamed or removed underneath us; the heartbeat cleans those entries up.
    for (const lockPath of this.activeLeases.keys()) {
      if (seen.has(lockPath)) continue;
      seen.add(lockPath);
      lockPaths.push(lockPath);
    }
    return lockPaths;
  }

  private liveOwnerOf(lockPath: string): ExecutionOwner | undefined {
    const owner = this.readOwner(lockPath);
    if (!owner) return undefined;
    return this.evaluateOwner(owner).live ? owner : undefined;
  }

  private readOwner(lockPath: string = this.lockFilePath): ExecutionOwner | undefined {
    try {
      const raw = fs.readFileSync(lockPath, 'utf8');
      const parsed = JSON.parse(raw);
      if (
        parsed &&
        typeof parsed.token === 'string' &&
        typeof parsed.threadId === 'string' &&
        typeof parsed.pid === 'number'
      ) {
        // Legacy locks predate the field; the file we read it from is the truth.
        if (typeof parsed.lockPath !== 'string') parsed.lockPath = lockPath;
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

  private isLockFileStale(lockPath: string): boolean {
    try {
      const stat = fs.statSync(lockPath);
      return Date.now() - stat.mtimeMs > this.staleAfterMs;
    } catch {
      return false;
    }
  }

  private removeStaleLock(lockPath: string, owner?: ExecutionOwner): void {
    try {
      if (!owner) {
        if (this.isLockFileStale(lockPath)) {
          fs.unlinkSync(lockPath);
        }
        return;
      }
      // Guard against deleting a fresh lock that replaced the stale one between
      // our read and this call.
      const current = this.readOwner(lockPath);
      if (current?.token === owner.token) {
        fs.unlinkSync(lockPath);
      }
    } catch {}
  }

  /** One timer refreshes every lease this process holds, independently. */
  private startHeartbeat(): void {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      if (this.activeLeases.size === 0) {
        this.stopHeartbeat();
        return;
      }

      for (const [lockPath, lease] of Array.from(this.activeLeases.entries())) {
        const owner = this.readOwner(lockPath);
        if (!owner || owner.token !== lease.token) {
          // The lock was reclaimed or deleted: stop pretending we still own it.
          this.activeLeases.delete(lockPath);
          continue;
        }

        owner.updatedAt = Date.now();
        owner.lockPath = lockPath;
        try {
          fs.writeFileSync(lockPath, JSON.stringify(owner), 'utf8');
        } catch {}
      }

      if (this.activeLeases.size === 0) this.stopHeartbeat();
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
