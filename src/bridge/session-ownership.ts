import * as crypto from 'crypto';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { callerSessionKey } from './caller-wake-queue';
import { AgentId } from './types';

export type SessionRole = 'developer' | 'reviewer';
export type SessionOwnershipResult =
  | { ok: true; record: SessionOwnershipRecord; existing: boolean }
  | { ok: false; code: 'SESSION_OWNERSHIP_CONFLICT' | 'SESSION_OWNERSHIP_UNVERIFIED'; reason: string };

export interface SessionOwnershipRecord {
  key: string;
  agent: AgentId;
  namespace: string;
  sessionId: string;
  threadId: string;
  role: SessionRole;
  sessionEpoch: number;
  createdAt: string;
  revokedAt?: string;
}

export interface SessionOwnershipOptions {
  homeDir?: string;
  bridgeDir?: string;
  env?: NodeJS.ProcessEnv;
}

/** Resolve the native storage root once; callers persist it on the task for later turns. */
export function nativeSessionNamespace(agent: AgentId, options: SessionOwnershipOptions = {}): string {
  const home = options.homeDir || os.homedir();
  const env = options.env || process.env;
  const root = agent === 'codex'
    ? env.CODEX_HOME || path.join(home, '.codex')
    : agent === 'claude_code'
      ? env.CLAUDE_CONFIG_DIR || path.join(home, '.claude')
      : agent === 'deepseek_harness'
        ? env.DSH_HOME || path.join(home, '.dsh')
        : path.join(home, '.gemini', 'antigravity-cli');
  return normalizeNativeNamespace(root);
}

/** Resolve junctions/symlinks in the existing part of a CLI storage root. */
export function normalizeNativeNamespace(value: string): string {
  let resolved = path.resolve(value);
  const tail: string[] = [];
  while (!fs.existsSync(resolved)) {
    const parent = path.dirname(resolved);
    if (parent === resolved) break;
    tail.unshift(path.basename(resolved));
    resolved = parent;
  }
  try { resolved = fs.realpathSync.native(resolved); } catch {}
  resolved = path.resolve(resolved, ...tail);
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

/** Static, non-expiring assignment. It never represents process ownership. */
export class SessionOwnershipStore {
  private readonly dir: string;
  private readonly options: SessionOwnershipOptions;

  constructor(storageDir?: string, options: SessionOwnershipOptions = {}) {
    const base = storageDir || options.bridgeDir || path.join(options.homeDir || os.homedir(), '.agent-mesh-bridge');
    this.dir = path.join(base, 'session-bindings');
    this.options = options;
  }

  public claim(input: {
    agent: AgentId;
    namespace?: string;
    sessionId: string;
    threadId: string;
    role: SessionRole;
    sessionEpoch: number;
  }): SessionOwnershipResult {
    const sessionId = callerSessionKey(input.sessionId).trim();
    if (!sessionId || !input.threadId || !Number.isSafeInteger(input.sessionEpoch) || input.sessionEpoch < 0) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Invalid session identity or epoch.' };
    }
    const namespace = normalizeNativeNamespace(input.namespace || nativeSessionNamespace(input.agent, this.options));
    const key = crypto.createHash('sha256').update(`${input.agent}\0${namespace}\0${sessionId}`).digest('hex');
    const file = path.join(this.dir, `${key}.json`);
    const revokedFile = path.join(this.dir, `${key}.revoked`);
    const wanted: SessionOwnershipRecord = {
      key, agent: input.agent, namespace, sessionId, threadId: input.threadId,
      role: input.role, sessionEpoch: input.sessionEpoch, createdAt: new Date().toISOString(),
    };
    try {
      fs.mkdirSync(this.dir, { recursive: true });
      if (fs.existsSync(revokedFile)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'This native session has a durable revocation record.' };
      }
      const temp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
      let fd: number | undefined;
      try {
        fd = fs.openSync(temp, 'wx', 0o600);
        fs.writeFileSync(fd, `${JSON.stringify(wanted)}\n`, 'utf8');
        fs.fsyncSync(fd);
        fs.closeSync(fd); fd = undefined;
        try {
          fs.linkSync(temp, file); // Atomic no-replace publication after durable content.
          try { const d = fs.openSync(this.dir, 'r'); fs.fsyncSync(d); fs.closeSync(d); } catch {}
          if (fs.existsSync(revokedFile)) return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'This native session was revoked during claim.' };
          return { ok: true, record: wanted, existing: false };
        } catch (error: any) {
          if (error?.code !== 'EEXIST') throw error;
        }
      } finally {
        if (fd !== undefined) try { fs.closeSync(fd); } catch {}
        try { fs.unlinkSync(temp); } catch {}
      }
      let current: SessionOwnershipRecord;
      try { current = JSON.parse(fs.readFileSync(file, 'utf8')); }
      catch (error: any) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: `Existing binding is unreadable: ${error?.message || error}` };
      }
      if (!this.validRecord(current, key)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Existing binding is malformed or does not match its key.' };
      }
      if (fs.existsSync(revokedFile)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'This native session has a durable revocation record.' };
      }
      if (current.threadId === wanted.threadId && current.role === wanted.role && current.sessionEpoch === wanted.sessionEpoch) {
        return { ok: true, record: current, existing: true };
      }
      return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: `Session is assigned to ${current.threadId}/${current.role}/epoch ${current.sessionEpoch}.` };
    } catch (error: any) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: error?.message || String(error) };
    }
  }

  /** Read-only check for a captured session. Missing evidence is never adopted. */
  public verify(input: {
    agent: AgentId;
    namespace?: string;
    sessionId: string;
    threadId: string;
    role: SessionRole;
    sessionEpoch: number;
  }): SessionOwnershipResult {
    const sessionId = callerSessionKey(input.sessionId).trim();
    if (!sessionId || !input.threadId || !Number.isSafeInteger(input.sessionEpoch) || input.sessionEpoch < 0) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Invalid session identity or epoch.' };
    }
    const namespace = normalizeNativeNamespace(input.namespace || nativeSessionNamespace(input.agent, this.options));
    const key = crypto.createHash('sha256').update(`${input.agent}\0${namespace}\0${sessionId}`).digest('hex');
    const file = path.join(this.dir, `${key}.json`);
    const revokedFile = path.join(this.dir, `${key}.revoked`);
    try {
      if (!fs.existsSync(file)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Captured session has no durable ownership binding.' };
      }
      const record = JSON.parse(fs.readFileSync(file, 'utf8')) as SessionOwnershipRecord;
      if (!this.validRecord(record, key)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Captured session binding is damaged or inconsistent.' };
      }
      if (fs.existsSync(revokedFile)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'This native session has a durable revocation record.' };
      }
      if (record.threadId !== input.threadId || record.role !== input.role || record.sessionEpoch !== input.sessionEpoch) {
        return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: `Session is assigned to ${record.threadId}/${record.role}/epoch ${record.sessionEpoch}.` };
      }
      return { ok: true, record, existing: true };
    } catch (error: any) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: error?.message || String(error) };
    }
  }

  public revoke(agent: AgentId, namespace: string, sessionId: string, owner: Pick<SessionOwnershipRecord, 'threadId' | 'role' | 'sessionEpoch'>): SessionOwnershipResult {
    const canonicalNamespace = normalizeNativeNamespace(namespace);
    const canonicalSession = callerSessionKey(sessionId).trim();
    const key = crypto.createHash('sha256').update(`${agent}\0${canonicalNamespace}\0${canonicalSession}`).digest('hex');
    const file = path.join(this.dir, `${key}.json`);
    const revokedFile = path.join(this.dir, `${key}.revoked`);
    const temp = `${revokedFile}.${process.pid}.${crypto.randomUUID()}.tmp`;
    try {
      const current = JSON.parse(fs.readFileSync(file, 'utf8')) as SessionOwnershipRecord;
      if (!this.validRecord(current, key)) {
        return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Cannot revoke an unverified session binding.' };
      }
      if (current.threadId !== owner.threadId || current.role !== owner.role || current.sessionEpoch !== owner.sessionEpoch) {
        return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'Session binding belongs to another task or epoch.' };
      }
      if (fs.existsSync(revokedFile)) {
        const prior = JSON.parse(fs.readFileSync(revokedFile, 'utf8')) as SessionOwnershipRecord;
        if (!this.validRecord(prior, key) || prior.threadId !== owner.threadId || prior.role !== owner.role || prior.sessionEpoch !== owner.sessionEpoch || !prior.revokedAt) {
          return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: 'Existing revocation evidence is malformed or belongs to another owner.' };
        }
        return { ok: true, record: prior, existing: true };
      }
      const revoked = { ...current, revokedAt: new Date().toISOString() };
      const fd = fs.openSync(temp, 'wx', 0o600);
      try { fs.writeFileSync(fd, `${JSON.stringify(revoked)}\n`, 'utf8'); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
      try { fs.linkSync(temp, revokedFile); }
      catch (error: any) {
        if (error?.code !== 'EEXIST') throw error;
        const prior = JSON.parse(fs.readFileSync(revokedFile, 'utf8')) as SessionOwnershipRecord;
        if (!this.validRecord(prior, key) || prior.threadId !== owner.threadId || prior.role !== owner.role || prior.sessionEpoch !== owner.sessionEpoch || !prior.revokedAt) {
          return { ok: false, code: 'SESSION_OWNERSHIP_CONFLICT', reason: 'Binding changed before revocation.' };
        }
        return { ok: true, record: prior, existing: true };
      }
      return { ok: true, record: revoked, existing: true };
    } catch (error: any) {
      return { ok: false, code: 'SESSION_OWNERSHIP_UNVERIFIED', reason: error?.message || String(error) };
    } finally { try { fs.unlinkSync(temp); } catch {} }
  }

  private validRecord(record: SessionOwnershipRecord, key: string): boolean {
    if (!record || record.key !== key || typeof record.threadId !== 'string' || !record.threadId ||
        !((record.role === 'developer' || record.role === 'reviewer') && Number.isSafeInteger(record.sessionEpoch)) ||
        typeof record.agent !== 'string' || typeof record.namespace !== 'string' || typeof record.sessionId !== 'string' ||
        typeof record.createdAt !== 'string') return false;
    if (!['codex', 'claude_code', 'deepseek_harness', 'antigravity'].includes(record.agent) ||
        !record.namespace || !record.sessionId || !Number.isSafeInteger(record.sessionEpoch) || record.sessionEpoch < 0 ||
        !Number.isFinite(Date.parse(record.createdAt)) || (record.revokedAt !== undefined && !Number.isFinite(Date.parse(record.revokedAt)))) return false;
    const canonicalNamespace = normalizeNativeNamespace(record.namespace);
    const canonicalSession = callerSessionKey(record.sessionId).trim();
    const derived = crypto.createHash('sha256').update(`${record.agent}\0${canonicalNamespace}\0${canonicalSession}`).digest('hex');
    return record.namespace === canonicalNamespace && record.sessionId === canonicalSession && derived === key;
  }
}
