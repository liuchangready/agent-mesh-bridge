import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

/**
 * Per-caller wake serialization.
 *
 * One agent session (codex/claude/dsh) can only process one turn at a time, and
 * a wake resumes that session by id. With several tasks in flight, their
 * completions land at roughly the same moment and would otherwise spawn N
 * `--resume <same-session>` processes that fight over one native session.
 *
 * The first wake wins a lock for that caller session; the rest are queued on
 * disk (so they survive a crashed bridge process) and replayed one by one when
 * the previous wake's child exits.
 */

export type WakeKind = 'review' | 'failure';

export interface WakeRequest {
  threadId: string;
  round: number;
  kind: WakeKind;
  enqueuedAt: number;
}

interface WakeOwner {
  pid: number;
  at: number;
  threadId?: string;
}

export interface CallerWakeQueueOptions {
  /** A lock older than this (and whose pid is gone) is reclaimed. */
  staleAfterMs?: number;
}

/** A wake can legitimately run for a long review turn; 30 minutes is generous. */
const DEFAULT_STALE_MS = 30 * 60 * 1000;

export class CallerWakeQueue {
  private readonly root: string;
  private readonly staleAfterMs: number;

  constructor(storageDir?: string, options?: CallerWakeQueueOptions) {
    const base = storageDir || path.join(os.homedir(), '.agent-mesh-bridge');
    this.root = path.join(base, 'wakes');
    this.staleAfterMs = options?.staleAfterMs ?? DEFAULT_STALE_MS;
  }

  private callerDir(callerSessionId: string): string {
    const key = crypto.createHash('sha1').update(callerSessionId).digest('hex').slice(0, 16);
    return path.join(this.root, key);
  }

  private lockPath(callerSessionId: string): string {
    return path.join(this.callerDir(callerSessionId), 'active.json');
  }

  private pendingDir(callerSessionId: string): string {
    return path.join(this.callerDir(callerSessionId), 'pending');
  }

  private ensureDirs(callerSessionId: string): void {
    try {
      fs.mkdirSync(this.pendingDir(callerSessionId), { recursive: true });
    } catch {}
  }

  private readOwner(callerSessionId: string): WakeOwner | undefined {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.lockPath(callerSessionId), 'utf8'));
      if (parsed && typeof parsed.pid === 'number') return parsed as WakeOwner;
    } catch {}
    return undefined;
  }

  private isOwnerLive(owner: WakeOwner): boolean {
    const age = Date.now() - (owner.at || 0);
    if (age > this.staleAfterMs) return false;
    if (owner.pid === process.pid) return true;
    try {
      process.kill(owner.pid, 0);
      return true;
    } catch (err: any) {
      // EPERM: the process exists but we may not signal it.
      return err?.code === 'EPERM';
    }
  }

  /**
   * Take the caller's wake slot.
   *
   * @returns true when this process may launch the wake now.
   */
  public tryAcquire(callerSessionId: string): boolean {
    this.ensureDirs(callerSessionId);
    const lockPath = this.lockPath(callerSessionId);
    for (let attempt = 0; attempt < 3; attempt++) {
      const payload: WakeOwner = { pid: process.pid, at: Date.now() };
      try {
        fs.writeFileSync(lockPath, JSON.stringify(payload), { encoding: 'utf8', flag: 'wx' });
        return true;
      } catch (err: any) {
        if (err?.code !== 'EEXIST') return false;
        const owner = this.readOwner(callerSessionId);
        if (!owner) {
          // Fresh file that has not been written yet, or a corrupt one.
          try {
            if (Date.now() - fs.statSync(lockPath).mtimeMs < 5_000) return false;
            fs.unlinkSync(lockPath);
          } catch {}
          continue;
        }
        if (this.isOwnerLive(owner)) return false;
        try {
          fs.unlinkSync(lockPath);
        } catch {}
      }
    }
    return false;
  }

  /** Release the slot. Only the process that owns it may clear it. */
  public release(callerSessionId: string): void {
    const owner = this.readOwner(callerSessionId);
    if (owner && owner.pid !== process.pid) return;
    try {
      fs.unlinkSync(this.lockPath(callerSessionId));
    } catch {}
  }

  public isBusy(callerSessionId: string): boolean {
    const owner = this.readOwner(callerSessionId);
    return !!owner && this.isOwnerLive(owner);
  }

  /** Queue a wake that could not start now. */
  public enqueue(callerSessionId: string, request: WakeRequest): void {
    this.ensureDirs(callerSessionId);
    const name = `${String(request.enqueuedAt).padStart(15, '0')}-${crypto
      .randomBytes(4)
      .toString('hex')}.json`;
    try {
      fs.writeFileSync(
        path.join(this.pendingDir(callerSessionId), name),
        JSON.stringify(request, null, 2),
        'utf8',
      );
    } catch (err) {
      console.error(`[CallerWakeQueue] Failed to enqueue wake for ${callerSessionId}:`, err);
    }
  }

  /** Take the oldest queued wake, removing it from the queue. */
  public dequeue(callerSessionId: string): WakeRequest | undefined {
    let names: string[];
    try {
      names = fs
        .readdirSync(this.pendingDir(callerSessionId))
        .filter((n) => n.endsWith('.json'))
        .sort();
    } catch {
      return undefined;
    }
    for (const name of names) {
      const file = path.join(this.pendingDir(callerSessionId), name);
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as WakeRequest;
        fs.unlinkSync(file);
        if (parsed && parsed.threadId) return parsed;
      } catch {
        try {
          fs.unlinkSync(file);
        } catch {}
      }
    }
    return undefined;
  }

  public pendingCount(callerSessionId: string): number {
    try {
      return fs.readdirSync(this.pendingDir(callerSessionId)).filter((n) => n.endsWith('.json')).length;
    } catch {
      return 0;
    }
  }

  /** Drop a queued wake (e.g. the thread was approved/deleted while waiting). */
  public discard(callerSessionId: string, threadId: string): number {
    let removed = 0;
    let names: string[];
    try {
      names = fs.readdirSync(this.pendingDir(callerSessionId)).filter((n) => n.endsWith('.json'));
    } catch {
      return 0;
    }
    for (const name of names) {
      const file = path.join(this.pendingDir(callerSessionId), name);
      try {
        const parsed = JSON.parse(fs.readFileSync(file, 'utf8')) as WakeRequest;
        if (parsed?.threadId === threadId) {
          fs.unlinkSync(file);
          removed++;
        }
      } catch {}
    }
    return removed;
  }
}
