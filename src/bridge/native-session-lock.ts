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
    try {
      fs.writeFileSync(lockPath, JSON.stringify(owner), { encoding: 'utf8', flag: 'wx', mode: 0o600 });
      return { token: owner.token, path: lockPath, owner };
    } catch (error: any) {
      if (error?.code !== 'EEXIST') throw error;
    }
    const gatePath = `${lockPath}.reaper`;
    const gate = { token: crypto.randomUUID(), pid: process.pid };
    try {
      fs.writeFileSync(gatePath, JSON.stringify(gate), { encoding: 'utf8', flag: 'wx', mode: 0o600 });
    } catch (error: any) {
      if (error?.code !== 'EEXIST') throw error;
      let currentGate: { token?: string; pid?: number } | undefined;
      try { currentGate = JSON.parse(fs.readFileSync(gatePath, 'utf8')); } catch {}
      if (!currentGate || pidAlive(currentGate.pid)) return undefined;
      try { fs.unlinkSync(gatePath); } catch { return undefined; }
      try { fs.writeFileSync(gatePath, JSON.stringify(gate), { encoding: 'utf8', flag: 'wx', mode: 0o600 }); }
      catch { return undefined; }
    }
    try {
      let current: NativeSessionOwner | undefined;
      try { current = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as NativeSessionOwner; } catch {}
      if (current && (pidAlive(current.pid) || pidAlive(current.childPid))) return undefined;
      try { fs.unlinkSync(lockPath); } catch (error: any) { if (error?.code !== 'ENOENT') return undefined; }
      try {
        fs.writeFileSync(lockPath, JSON.stringify(owner), { encoding: 'utf8', flag: 'wx', mode: 0o600 });
        return { token: owner.token, path: lockPath, owner };
      } catch { return undefined; }
    } finally {
      try {
        const currentGate = JSON.parse(fs.readFileSync(gatePath, 'utf8'));
        if (currentGate.token === gate.token) fs.unlinkSync(gatePath);
      } catch {}
    }
  }

  attachChild(lease: NativeSessionLease, childPid?: number): void {
    let current: NativeSessionOwner;
    try { current = JSON.parse(fs.readFileSync(lease.path, 'utf8')) as NativeSessionOwner; } catch { return; }
    if (current.token !== lease.token) return;
    current.childPid = childPid;
    current.updatedAt = Date.now();
    fs.writeFileSync(lease.path, JSON.stringify(current), { encoding: 'utf8', mode: 0o600 });
  }

  release(lease: NativeSessionLease): void {
    try {
      const current = JSON.parse(fs.readFileSync(lease.path, 'utf8')) as NativeSessionOwner;
      if (current.token === lease.token && !pidAlive(current.childPid)) fs.unlinkSync(lease.path);
    } catch {}
  }
}
