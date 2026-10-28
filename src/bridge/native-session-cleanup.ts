/**
 * Native conversation cleanup for deleted tasks.
 *
 * Every delegated task creates a *receiver-side* native conversation: Claude
 * Code writes `~/.claude/projects/<workspace>/<sessionId>.jsonl`, Codex writes a
 * rollout under `~/.codex/sessions/`, Antigravity keeps a SQLite conversation DB
 * (plus a brain/annotations/implicit record), and DeepSeek Harness stores a
 * `session-<uuid>/session.v4.jsonl.zstd` directory per workspace. Deleting the
 * ledger entry used to leave all of that behind, so the conversation kept
 * showing up in the receiver's own app long after the task was gone.
 *
 * This module maps a ledger thread back to those native artifacts and removes
 * them. It is deliberately **best effort**:
 *
 *  - a session that cannot be found is not an error (the caller may have deleted
 *    it already, or the receiver never emitted a real native id);
 *  - a single failing path never aborts the remaining cleanup, it only records
 *    the reason on that entry;
 *  - the *caller's* conversation is never touched. `callerSessionId` is the
 *    user's own chat with the initiator agent and is shared by later tasks,
 *    while `receiverSessionId` (and the ids dropped by retargeting) belong to
 *    this task alone.
 *
 * The bridge only edits file-system state. App-owned SQLite indexes that might
 * also mention the conversation (`conversation_summaries.db`, Codex
 * `state_*.sqlite`, ...) are intentionally left alone: opening those while the
 * agent app is running could corrupt them, and a stale row there is harmless.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AgentId, DelegationThread, isValidSessionId } from './types';

/** A native conversation that belongs to one task. */
export interface NativeSessionRef {
  agent: AgentId;
  sessionId: string;
}

/** What cleanup managed to do for one `(agent, sessionId)` pair. */
export interface NativeSessionCleanupEntry {
  agent: AgentId;
  sessionId: string;
  /** True when at least one artifact was removed or a metadata entry pruned. */
  found: boolean;
  /** Native files/directories removed from disk. */
  removed: string[];
  /** Index/metadata files whose reference to this session was pruned in place. */
  pruned: string[];
  /** First failure for this session, if any. Cleanup never throws. */
  error?: string;
}

export interface NativeSessionCleanupResult {
  entries: NativeSessionCleanupEntry[];
  /** Number of native files/directories removed across every entry. */
  removedCount: number;
}

export interface NativeSessionCleanupOptions {
  /** Override the user home directory (tests). Defaults to `os.homedir()`. */
  homeDir?: string;
  /** Override environment lookups (`CLAUDE_CONFIG_DIR` / `CODEX_HOME` / `DSH_HOME`). */
  env?: NodeJS.ProcessEnv;
}

type JsonObject = Record<string, unknown>;

/** Deepest `sessions/<year>/<month>/<day>/<rollout>.jsonl` layout we scan. */
const MAX_CODEX_SESSION_DEPTH = 5;

/**
 * Native conversations the ledger attributes to a thread.
 *
 * The receiver's current session is the common case. A retarget that switched
 * agents records the dropped session as `previousReceiverSessionId`, and those
 * sessions still exist on the old agent's disk, so they are cleaned too.
 *
 * The initiator's session is intentionally absent — see the module comment.
 */
export function collectThreadSessionRefs(thread: DelegationThread): NativeSessionRef[] {
  const refs: NativeSessionRef[] = [];
  const seen = new Set<string>();

  const add = (agent: AgentId, sessionId?: string | null) => {
    // `vscode_chat` runs inside the editor and has no session store of its own.
    if (agent === 'vscode_chat') return;
    const safe = toSafeSessionId(sessionId);
    if (!safe) return;
    const key = `${agent}:${safe.toLowerCase()}`;
    if (seen.has(key)) return;
    seen.add(key);
    refs.push({ agent, sessionId: safe });
  };

  add(thread.targetAgent, thread.receiverSessionId);
  for (const retarget of thread.retargets || []) {
    add(retarget.fromAgent, retarget.previousReceiverSessionId);
  }

  return refs;
}

/**
 * Remove every native conversation the thread owns.
 *
 * Never throws: each session is handled independently and any failure is
 * reported on its own entry, so a missing conversation, a locked file or an
 * unreadable store cannot make `delete_task` fail.
 */
export function cleanupThreadNativeSessions(
  thread: DelegationThread,
  options: NativeSessionCleanupOptions = {},
): NativeSessionCleanupResult {
  const home = options.homeDir || os.homedir();
  const env = options.env || process.env;

  const entries = collectThreadSessionRefs(thread).map((ref) => cleanupSession(ref, home, env));

  return {
    entries,
    removedCount: entries.reduce((count, entry) => count + entry.removed.length, 0),
  };
}

/** One-line human summary used in `delete_task` results and log lines. */
export function describeNativeSessionCleanup(result?: NativeSessionCleanupResult): string {
  if (!result || result.entries.length === 0) return '';

  const cleaned = result.entries.filter((entry) => entry.found).length;
  const missing = result.entries.length - cleaned;
  const failed = result.entries.filter((entry) => !!entry.error).length;

  const parts = [`native conversations: ${cleaned} cleaned`];
  if (missing > 0) parts.push(`${missing} not found`);
  if (failed > 0) parts.push(`${failed} partly skipped`);
  return parts.join(', ');
}

function cleanupSession(
  ref: NativeSessionRef,
  home: string,
  env: NodeJS.ProcessEnv,
): NativeSessionCleanupEntry {
  const entry: NativeSessionCleanupEntry = {
    agent: ref.agent,
    sessionId: ref.sessionId,
    found: false,
    removed: [],
    pruned: [],
  };

  try {
    switch (ref.agent) {
      case 'claude_code':
        cleanupClaudeSession(home, env, ref.sessionId, entry);
        break;
      case 'codex':
        cleanupCodexSession(home, env, ref.sessionId, entry);
        break;
      case 'antigravity':
        cleanupAntigravitySession(home, ref.sessionId, entry);
        break;
      case 'deepseek_harness':
        cleanupDshSession(home, env, ref.sessionId, entry);
        break;
      default:
        // `vscode_chat` has no session store the bridge can reach.
        break;
    }
  } catch (e: any) {
    entry.error = e?.message || String(e);
  }

  entry.found = entry.removed.length > 0 || entry.pruned.length > 0;
  return entry;
}

/**
 * Claude Code layout (`$CLAUDE_CONFIG_DIR` or `~/.claude`):
 *
 *   projects/<workspace>/<sessionId>.jsonl      transcript
 *   projects/<workspace>/<sessionId>/subagents  subagent transcripts
 *   session-env/<sessionId>/                    per-session env
 *   file-history/<sessionId>/                   edit history
 *   todos/<sessionId>*.json                     legacy todo list
 */
function cleanupClaudeSession(
  home: string,
  env: NodeJS.ProcessEnv,
  sessionId: string,
  entry: NativeSessionCleanupEntry,
): void {
  const root = resolveConfigRoot(env, 'CLAUDE_CONFIG_DIR', path.join(home, '.claude'));

  for (const projectDir of listDirectories(path.join(root, 'projects'))) {
    removeArtifact(path.join(projectDir, `${sessionId}.jsonl`), entry);
    removeArtifact(path.join(projectDir, sessionId), entry);
  }

  removeArtifact(path.join(root, 'session-env', sessionId), entry);
  removeArtifact(path.join(root, 'file-history', sessionId), entry);

  const todosDir = path.join(root, 'todos');
  for (const name of listFileNames(todosDir)) {
    if (
      name === `${sessionId}.json` ||
      name.startsWith(`${sessionId}-`) ||
      name.startsWith(`${sessionId}.`)
    ) {
      removeArtifact(path.join(todosDir, name), entry);
    }
  }
}

/**
 * Codex layout (`$CODEX_HOME` or `~/.codex`):
 *
 *   sessions/<yyyy>/<mm>/<dd>/rollout-<timestamp>-<sessionId>.jsonl
 *   session_index.jsonl     one JSON record per thread
 *   thread-writer-locks/<sessionId>.lock
 */
function cleanupCodexSession(
  home: string,
  env: NodeJS.ProcessEnv,
  sessionId: string,
  entry: NativeSessionCleanupEntry,
): void {
  const root = resolveConfigRoot(env, 'CODEX_HOME', path.join(home, '.codex'));

  for (const rollout of findCodexRollouts(path.join(root, 'sessions'), sessionId)) {
    removeArtifact(rollout, entry);
  }

  removeArtifact(path.join(root, 'thread-writer-locks', `${sessionId}.lock`), entry);

  pruneJsonLines(
    path.join(root, 'session_index.jsonl'),
    (record) =>
      record.id === sessionId || record.thread_id === sessionId || record.sessionId === sessionId,
    entry,
  );
}

/**
 * Antigravity keeps two independent stores: `antigravity-cli` (behind the `agy`
 * CLI the bridge launches) and `antigravity` (the desktop app). Conversation ids
 * are UUIDs, so probing both cannot collide with an unrelated conversation.
 */
function cleanupAntigravitySession(
  home: string,
  sessionId: string,
  entry: NativeSessionCleanupEntry,
): void {
  for (const appDir of ['antigravity-cli', 'antigravity']) {
    const root = path.join(home, '.gemini', appDir);

    for (const suffix of ['.db', '.db-wal', '.db-shm', '.db-journal']) {
      removeArtifact(path.join(root, 'conversations', `${sessionId}${suffix}`), entry);
    }
    removeArtifact(path.join(root, 'brain', sessionId), entry);
    removeArtifact(path.join(root, 'annotations', `${sessionId}.pbtxt`), entry);
    removeArtifact(path.join(root, 'implicit', `${sessionId}.pb`), entry);

    pruneJsonObject(
      path.join(root, 'cache', 'conversation_metadata.json'),
      (data) => {
        const conversations = data.conversations;
        if (!conversations || typeof conversations !== 'object') return false;
        const table = conversations as JsonObject;
        if (!Object.prototype.hasOwnProperty.call(table, sessionId)) return false;
        delete table[sessionId];
        return true;
      },
      entry,
    );

    pruneJsonObject(
      path.join(root, 'cache', 'last_conversations.json'),
      (data) => {
        let changed = false;
        for (const [key, value] of Object.entries(data)) {
          if (value === sessionId) {
            delete data[key];
            changed = true;
          }
        }
        return changed;
      },
      entry,
    );
  }
}

/**
 * DeepSeek Harness layout (`$DSH_HOME` or `~/.dsh`):
 *
 *   sessions/<workspace-slug>/<sessionId>/session.v4.jsonl.zstd
 *   storages/session_projcache/sessions/<sessionId>.json
 *   storages/workspace.json    sidebar: per-workspace `sessionIds`, pinned/archived
 */
function cleanupDshSession(
  home: string,
  env: NodeJS.ProcessEnv,
  sessionId: string,
  entry: NativeSessionCleanupEntry,
): void {
  // Keep in sync with `getDshHome()` in deepseek-harness.ts.
  const dshHome = resolveConfigRoot(env, 'DSH_HOME', path.join(home, '.dsh'));
  const sessionsRoot = path.join(dshHome, 'sessions');

  for (const workspaceDir of listDirectories(sessionsRoot)) {
    removeArtifact(path.join(workspaceDir, sessionId), entry);
  }
  removeArtifact(path.join(sessionsRoot, sessionId), entry);

  removeArtifact(
    path.join(dshHome, 'storages', 'session_projcache', 'sessions', `${sessionId}.json`),
    entry,
  );

  pruneJsonObject(
    path.join(dshHome, 'storages', 'workspace.json'),
    (data) => {
      let changed = false;

      const global = data.global;
      if (global && typeof global === 'object') {
        for (const key of ['archivedSessionIds', 'pinnedSessionIds']) {
          const list = (global as JsonObject)[key];
          if (Array.isArray(list) && list.includes(sessionId)) {
            (global as JsonObject)[key] = list.filter((id) => id !== sessionId);
            changed = true;
          }
        }
      }

      const tables = data.tables;
      const workspaces =
        tables && typeof tables === 'object' ? (tables as JsonObject).workspaces : undefined;
      if (workspaces && typeof workspaces === 'object') {
        for (const workspace of Object.values(workspaces as JsonObject)) {
          if (!workspace || typeof workspace !== 'object') continue;
          const record = workspace as JsonObject;
          const ids = record.sessionIds;
          if (Array.isArray(ids) && ids.includes(sessionId)) {
            record.sessionIds = ids.filter((id) => id !== sessionId);
            changed = true;
          }
        }
      }

      return changed;
    },
    entry,
  );
}

/** Environment override wins, otherwise `<home>/<defaultName>`. */
function resolveConfigRoot(env: NodeJS.ProcessEnv, envName: string, fallback: string): string {
  const configured = (env[envName] || '').trim();
  return configured ? path.resolve(configured) : fallback;
}

/**
 * Reject anything that could escape the session store when joined into a path.
 *
 * `isValidSessionId()` already filters agent names and obvious placeholders; this
 * additionally pins the charset (letters, digits, `.`, `_`, `-`) so a forged id
 * like `../../evil` can never be interpolated into a delete target.
 */
function toSafeSessionId(sessionId?: string | null): string | undefined {
  if (!isValidSessionId(sessionId)) return undefined;
  const trimmed = (sessionId as string).trim();
  if (!/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/.test(trimmed)) return undefined;
  return trimmed;
}

function removeArtifact(target: string, entry: NativeSessionCleanupEntry): void {
  try {
    if (!fs.existsSync(target)) return;
    fs.rmSync(target, { recursive: true, force: true, maxRetries: 2 });
    entry.removed.push(target);
  } catch (e: any) {
    if (!entry.error) entry.error = `${target}: ${e?.message || e}`;
  }
}

function listDirectories(dir: string): string[] {
  try {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((item) => item.isDirectory())
      .map((item) => path.join(dir, item.name));
  } catch {
    return [];
  }
}

function listFileNames(dir: string): string[] {
  try {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((item) => item.isFile())
      .map((item) => item.name);
  } catch {
    return [];
  }
}

/** Rollout files whose basename ends with the session id as a whole token. */
function findCodexRollouts(dir: string, sessionId: string, depth = 0): string[] {
  if (depth > MAX_CODEX_SESSION_DEPTH) return [];

  let entries: fs.Dirent[];
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }

  const matches: string[] = [];
  for (const item of entries) {
    const fullPath = path.join(dir, item.name);
    if (item.isDirectory()) {
      matches.push(...findCodexRollouts(fullPath, sessionId, depth + 1));
      continue;
    }
    if (!item.isFile()) continue;

    const name = item.name.toLowerCase();
    const id = sessionId.toLowerCase();
    // <sessionId>.jsonl, rollout-...-<sessionId>.jsonl, and compressed rollouts.
    const matchesToken = (suffix: string) =>
      name === `${id}${suffix}` ||
      name.endsWith(`-${id}${suffix}`) ||
      name.endsWith(`_${id}${suffix}`);
    if (matchesToken('.jsonl') || matchesToken('.jsonl.zst')) {
      matches.push(fullPath);
    }
  }
  return matches;
}

/** Delete every JSON-object line the predicate matches, leaving the rest as-is. */
function pruneJsonLines(
  file: string,
  matches: (record: JsonObject) => boolean,
  entry: NativeSessionCleanupEntry,
): void {
  try {
    if (!fs.existsSync(file)) return;
    const raw = fs.readFileSync(file, 'utf8');
    if (!raw.trim()) return;

    let changed = false;
    const kept: string[] = [];
    for (const line of raw.split(/\r?\n/)) {
      const trimmed = line.trim();
      if (!trimmed) {
        kept.push(line);
        continue;
      }

      let record: unknown;
      try {
        record = JSON.parse(trimmed);
      } catch {
        kept.push(line);
        continue;
      }

      const object = toJsonObject(record);
      if (object && matches(object)) {
        changed = true;
        continue;
      }
      kept.push(line);
    }

    if (!changed) return;
    writeTextAtomically(file, kept.join('\n'));
    entry.pruned.push(file);
  } catch (e: any) {
    if (!entry.error) entry.error = `${file}: ${e?.message || e}`;
  }
}

/** Rewrite a JSON object file in place after `mutate` reports a change. */
function pruneJsonObject(
  file: string,
  mutate: (data: JsonObject) => boolean,
  entry: NativeSessionCleanupEntry,
): void {
  try {
    if (!fs.existsSync(file)) return;
    const raw = fs.readFileSync(file, 'utf8');
    if (!raw.trim()) return;

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw);
    } catch {
      // An app-owned file we cannot parse is left untouched rather than repaired.
      return;
    }

    const data = toJsonObject(parsed);
    if (!data || !mutate(data)) return;

    writeTextAtomically(file, `${JSON.stringify(data, null, 2)}\n`);
    entry.pruned.push(file);
  } catch (e: any) {
    if (!entry.error) entry.error = `${file}: ${e?.message || e}`;
  }
}

function toJsonObject(value: unknown): JsonObject | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonObject)
    : undefined;
}

function writeTextAtomically(file: string, content: string): void {
  const temp = `${file}.agent-mesh-${process.pid}.tmp`;
  try {
    fs.writeFileSync(temp, content, 'utf8');
    fs.renameSync(temp, file);
  } catch (e) {
    try {
      fs.rmSync(temp, { force: true });
    } catch {}
    throw e;
  }
}
