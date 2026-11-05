import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { callerSessionKey } from './caller-wake-queue';
import { AgentId } from './types';

interface NativeSessionOwner {
  token: string;
  pid: number;
  childPid?: number;
  agent: AgentId;
  sessionId: string;
  updatedAt: number;
}

export interface NativeSessionLease {
  token: string;
  path: string;
  owner: NativeSessionOwner;
}

function pidAlive(pid?: number): boolean {
  if (!pid || pid < 1) return false;
  try { process.kill(pid, 0); return true; } catch (error: any) { return error?.code === 'EPERM'; }
}

function publish(pathname: string, value: unknown, noReplace = false): boolean {
  const temp = `${pathname}.${process.pid}.${crypto.randomUUID()}.tmp`;
  let fd: number | undefined;
  try {
    fd = fs.openSync(temp, 'wx', 0o600);
    fs.writeFileSync(fd, JSON.stringify(value));
    fs.fsyncSync(fd);
    fs.closeSync(fd); fd = undefined;
    if (noReplace) fs.linkSync(temp, pathname);
    else fs.renameSync(temp, pathname);
    return true;
  } catch (error: any) {
    if (!noReplace || error?.code !== 'EEXIST') throw error;
    return false;
  } finally {
    if (fd !== undefined) try { fs.closeSync(fd); } catch {}
    try { fs.unlinkSync(temp); } catch {}
  }
}

function pendingOwnerPaths(lockPath: string): string[] {
  const dir = path.dirname(lockPath);
  const prefix = `${path.basename(lockPath)}.pending.`;
  try { return fs.readdirSync(dir).filter((name) => name.startsWith(prefix)).map((name) => path.join(dir, name)); }
  catch { return []; }
}

export class NativeSessionLock {
  private readonly dir: string;

  constructor(storageDir?: string) {
    this.dir = path.join(storageDir || path.join(os.homedir(), '.agent-mesh-bridge'), 'native-sessions');
    fs.mkdirSync(this.dir, { recursive: true });
  }

  private lockPath(agent: AgentId, sessionId: string): string {
    const key = crypto.createHash('sha256').update(`${agent}\0${callerSessionKey(sessionId)}`).digest('hex');
    return path.join(this.dir, `${key}.json`);
  }

  /** Acquire only when both the bridge owner and its child are confirmed dead. */
  acquire(agent: AgentId, sessionId: string, childPid?: number): NativeSessionLease | undefined {
    if (!sessionId?.trim()) return undefined;
    const lockPath = this.lockPath(agent, sessionId);
    const owner: NativeSessionOwner = {
      token: crypto.randomUUID(), pid: process.pid, childPid, agent,
      sessionId: callerSessionKey(sessionId), updatedAt: Date.now(),
    };
    const gatePath = `${lockPath}.reaper`;
    const gate = this.acquireGate(gatePath);
    if (!gate) return undefined;
    try {
      if (!this.ownsGate(gatePath, gate.token)) return undefined;
      let current: NativeSessionOwner | undefined;
      try { current = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as NativeSessionOwner; } catch {}
      if (!current) {
        for (const pendingPath of pendingOwnerPaths(lockPath)) {
          let pending: NativeSessionOwner | undefined;
          try { pending = JSON.parse(fs.readFileSync(pendingPath, 'utf8')) as NativeSessionOwner; } catch {}
          if (!pending?.token || !pending.pid || !pending.agent || !pending.sessionId) return undefined;
          if (pidAlive(pending.pid) || pidAlive(pending.childPid)) {
            try { fs.linkSync(pendingPath, lockPath); } catch {}
            return undefined;
          }
          try { fs.unlinkSync(pendingPath); } catch { return undefined; }
        }
        try {
          if (publish(lockPath, owner, true)) return { token: owner.token, path: lockPath, owner };
        } catch {}
        return undefined;
      }
      // An incomplete or corrupt publication is never evidence that its owner died.
      if (!current.token || !current.pid || !current.agent || !current.sessionId) return undefined;
      if (pidAlive(current.pid) || pidAlive(current.childPid)) return undefined;
      if (!this.ownsGate(gatePath, gate.token)) return undefined;
      try { fs.unlinkSync(lockPath); } catch (error: any) { if (error?.code !== 'ENOENT') return undefined; }
      try {
        if (publish(lockPath, owner, true))
        return { token: owner.token, path: lockPath, owner };
      } catch { return undefined; }
    } finally {
      this.releaseGate(gatePath, gate.token);
    }
    return undefined;
  }

  private acquireGate(gatePath: string): { token: string; pid: number } | undefined {
    const gate = { token: crypto.randomUUID(), pid: process.pid };
    try { if (publish(gatePath, gate, true)) return gate; }
    catch (error: any) { if (error?.code !== 'EEXIST') throw error; }
    let observed: { token?: string; pid?: number };
    try { observed = JSON.parse(fs.readFileSync(gatePath, 'utf8')); } catch { return undefined; }
    if (!observed.token || pidAlive(observed.pid)) return undefined;
    const quarantine = `${gatePath}.${crypto.randomUUID()}.stale`;
    try { fs.renameSync(gatePath, quarantine); } catch { return undefined; }
    let moved: { token?: string; pid?: number };
    try { moved = JSON.parse(fs.readFileSync(quarantine, 'utf8')); } catch { return undefined; }
    if (moved.token !== observed.token || pidAlive(moved.pid)) {
      try { fs.linkSync(quarantine, gatePath); } catch {}
      try { fs.unlinkSync(quarantine); } catch {}
      return undefined;
    }
    try { fs.unlinkSync(quarantine); } catch { return undefined; }
    try { return publish(gatePath, gate, true) ? gate : undefined; } catch { return undefined; }
  }

  private ownsGate(gatePath: string, token: string): boolean {
    try { return JSON.parse(fs.readFileSync(gatePath, 'utf8')).token === token; } catch { return false; }
  }

  private releaseGate(gatePath: string, token: string): void {
    if (this.ownsGate(gatePath, token)) try { fs.unlinkSync(gatePath); } catch {}
  }

  attachChild(lease: NativeSessionLease, childPid?: number): void {
    const gatePath = `${lease.path}.reaper`;
    const gate = this.acquireGate(gatePath);
    if (!gate) return;
    try {
      if (!this.ownsGate(gatePath, gate.token)) return;
      let current: NativeSessionOwner;
      try { current = JSON.parse(fs.readFileSync(lease.path, 'utf8')) as NativeSessionOwner; } catch { return; }
      if (current.token !== lease.token) return;
      current.childPid = childPid;
      current.updatedAt = Date.now();
      const pendingPath = `${lease.path}.pending.${lease.token}`;
      let fd: number | undefined;
      try {
        fd = fs.openSync(pendingPath, 'w', 0o600);
        fs.writeFileSync(fd, JSON.stringify(current));
        fs.fsyncSync(fd);
        fs.closeSync(fd); fd = undefined;
        if (!this.ownsGate(gatePath, gate.token)) return;
        try { fs.renameSync(pendingPath, lease.path); }
        catch (error: any) {
          if (error?.code !== 'EPERM' && error?.code !== 'EEXIST') throw error;
          fs.unlinkSync(lease.path);
          fs.linkSync(pendingPath, lease.path);
          fs.unlinkSync(pendingPath);
        }
      } finally {
        if (fd !== undefined) try { fs.closeSync(fd); } catch {}
        try { fs.unlinkSync(pendingPath); } catch {}
      }
    } finally { this.releaseGate(gatePath, gate.token); }
  }

  release(lease: NativeSessionLease): void {
    const gatePath = `${lease.path}.reaper`;
    const gate = this.acquireGate(gatePath);
    if (!gate) return;
    try {
      if (!this.ownsGate(gatePath, gate.token)) return;
    try {
      const current = JSON.parse(fs.readFileSync(lease.path, 'utf8')) as NativeSessionOwner;
      if (current.token === lease.token && !pidAlive(current.childPid)) fs.unlinkSync(lease.path);
    } catch {}
    } finally { this.releaseGate(gatePath, gate.token); }
  }
}
