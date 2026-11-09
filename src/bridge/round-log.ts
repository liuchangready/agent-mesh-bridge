/**
 * One place that knows where a round's bytes live on disk.
 *
 * Three processes (the extension-host supervisor, the dispatcher/rework MCP
 * server, and the supervisor itself) used to build `~/.agent-mesh-bridge/logs`
 * paths by hand. Copies drift, and the "most recent log" lookup compared file
 * names as strings, so `..._round_10.log` sorted before `..._round_9.log` and
 * `get_recent_logs` returned the wrong round for any task with 10+ rounds.
 *
 * Two distinct artifacts exist per round:
 *   - execution log: the raw stdio byte stream, written while the agent runs;
 *   - output archive: the extracted final output, captured verbatim right
 *     before the ledger copy is condensed.
 */

import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

export const BRIDGE_HOME_DIRNAME = '.agent-mesh-bridge';
export const ROUND_OUTPUT_ARCHIVE_SUFFIX = '.output.md';
export const ROUND_EXECUTION_LOG_SUFFIX = '.log';

export const WORKER_LOG_DIRNAME = 'worker_log';
export const REVIEWER_LOG_DIRNAME = 'reviewer_log';
export const ARCHIVE_DIRNAME = 'Archive';
export const WORKTREE_DIRNAME = 'worktree';
export const BOOTSTRAP_DIRNAME = 'bootstrap';
export const VALIDATION_DIRNAME = 'validation';

/**
 * Paths owned by one task's unified data directory.
 *
 * A new task records the absolute base in `thread.artifactDir`; every later
 * write goes through that recorded value instead of re-reading the (mutable)
 * global `taskDataRoot` policy. Legacy tasks have no such base and keep using
 * the per-agent log directories below.
 */
export function taskWorkerLogDir(artifactDir: string): string {
  return path.join(artifactDir, WORKER_LOG_DIRNAME);
}

export function taskReviewerLogDir(artifactDir: string): string {
  return path.join(artifactDir, REVIEWER_LOG_DIRNAME);
}

export function taskArchiveDir(artifactDir: string): string {
  return path.join(artifactDir, ARCHIVE_DIRNAME);
}

export function taskWorktreeDir(artifactDir: string): string {
  return path.join(artifactDir, WORKTREE_DIRNAME);
}

export function taskBootstrapDir(artifactDir: string): string {
  return path.join(artifactDir, BOOTSTRAP_DIRNAME);
}

export function taskValidationDir(artifactDir: string): string {
  return path.join(artifactDir, VALIDATION_DIRNAME);
}

function readPolicySnapshot(root?: string): any {
  try {
    const file = path.join(bridgeHomeDir(root), 'bridge-policy.json');
    if (fs.existsSync(file)) {
      return JSON.parse(fs.readFileSync(file, 'utf8'));
    }
  } catch {}
  return {};
}

/**
 * Resolve the base log directory for an agent (or default mesh log directory).
 *
 * Checks in order:
 *  1. agent-specific directory in bridge-policy.json (e.g. logDirectories.codex);
 *  2. default directory in bridge-policy.json (logDirectories.default);
 *  3. ~/.agent-mesh-bridge/logs (or <root>/logs).
 */
export function resolveAgentLogBaseDir(
  agent?: string,
  root?: string,
  overridePolicy?: any,
): string {
  const policy = overridePolicy || readPolicySnapshot(root);
  const normalizedAgent = agent ? String(agent).replace(/-/g, '_') : undefined;
  if (
    normalizedAgent &&
    policy?.logDirectories &&
    typeof policy.logDirectories[normalizedAgent] === 'string'
  ) {
    const custom = policy.logDirectories[normalizedAgent].trim();
    if (custom) return custom;
  }
  if (policy?.logDirectories && typeof policy.logDirectories.default === 'string') {
    const customDefault = policy.logDirectories.default.trim();
    if (customDefault) return customDefault;
  }
  return bridgeLogsDir(root);
}

/** Ensure the three subdirectories (worker_log, reviewer_log, Archive) exist. */
export function ensureAgentLogDirs(agent?: string, root?: string): {
  baseDir: string;
  workerLogDir: string;
  reviewerLogDir: string;
  archiveDir: string;
} {
  const baseDir = resolveAgentLogBaseDir(agent, root);
  const workerLogDir = path.join(baseDir, WORKER_LOG_DIRNAME);
  const reviewerLogDir = path.join(baseDir, REVIEWER_LOG_DIRNAME);
  const archiveDir = path.join(baseDir, ARCHIVE_DIRNAME);
  try {
    fs.mkdirSync(workerLogDir, { recursive: true });
    fs.mkdirSync(reviewerLogDir, { recursive: true });
    fs.mkdirSync(archiveDir, { recursive: true });
  } catch {}
  return { baseDir, workerLogDir, reviewerLogDir, archiveDir };
}

export function getWorkerLogDir(agent?: string, root?: string): string {
  return ensureAgentLogDirs(agent, root).workerLogDir;
}

export function getReviewerLogDir(agent?: string, root?: string): string {
  return ensureAgentLogDirs(agent, root).reviewerLogDir;
}

export function getArchiveLogDir(agent?: string, root?: string): string {
  return ensureAgentLogDirs(agent, root).archiveDir;
}

/** Default page size for `get_round_raw_log` (never dump a 1.6MB log at once). */
export const DEFAULT_RAW_LOG_BYTES = 20_000;
/** Absolute ceiling for one `get_round_raw_log` call. */
export const MAX_RAW_LOG_BYTES = 200_000;

/** `~/.agent-mesh-bridge`, or an explicit root (tests, custom storage dirs). */
export function bridgeHomeDir(root?: string): string {
  return root && root.trim() ? root : path.join(os.homedir(), BRIDGE_HOME_DIRNAME);
}

export function bridgeLogsDir(root?: string): string {
  return path.join(bridgeHomeDir(root), 'logs');
}

/**
 * Thread ids are generated by the bridge, but MCP arguments are caller
 * controlled: strip path separators so a crafted id can never escape `logs/`.
 */
export function safeThreadIdPart(threadId: string): string {
  return String(threadId || 'unknown')
    .replace(/[^A-Za-z0-9._-]/g, '_')
    .replace(/\.{2,}/g, '_')
    .slice(0, 120);
}

function artifactBase(threadId: string, round: number): string {
  const safeRound = Number.isFinite(round) ? Math.max(1, Math.floor(round)) : 1;
  return `${safeThreadIdPart(threadId)}_round_${safeRound}`;
}

export function roundExecutionLogPath(
  threadId: string,
  round: number,
  root?: string,
  agent?: string,
  artifactDir?: string,
): string {
  const dir = artifactDir ? taskWorkerLogDir(artifactDir) : getWorkerLogDir(agent, root);
  return path.join(dir, `${artifactBase(threadId, round)}.log`);
}

export function roundOutputArchivePath(
  threadId: string,
  round: number,
  root?: string,
  agent?: string,
  artifactDir?: string,
): string {
  const dir = artifactDir ? taskWorkerLogDir(artifactDir) : getWorkerLogDir(agent, root);
  return path.join(dir, `${artifactBase(threadId, round)}${ROUND_OUTPUT_ARCHIVE_SUFFIX}`);
}

export function findExistingExecutionLog(
  threadId: string,
  round: number,
  options: { root?: string; agent?: string; artifactDir?: string } = {},
): string | undefined {
  if (options.artifactDir) {
    // A unified task directory is self-contained: one lifecycle file, one path.
    return resolveExistingArtifactPath([
      roundExecutionLogPath(threadId, round, undefined, undefined, options.artifactDir),
    ]);
  }
  const candidates: Array<string | undefined> = [
    roundExecutionLogPath(threadId, round, options.root, options.agent),
    roundExecutionLogPath(threadId, round, options.root),
    // Rounds written before a log directory was configured live here.
    path.join(
      bridgeLogsDir(options.root),
      WORKER_LOG_DIRNAME,
      `${artifactBase(threadId, round)}.log`,
    ),
    path.join(bridgeLogsDir(options.root), `${artifactBase(threadId, round)}.log`),
  ];
  return resolveExistingArtifactPath(candidates);
}

export function findExistingOutputArchive(
  threadId: string,
  round: number,
  options: { root?: string; agent?: string; artifactDir?: string } = {},
): string | undefined {
  if (options.artifactDir) {
    return resolveExistingArtifactPath([
      roundOutputArchivePath(threadId, round, undefined, undefined, options.artifactDir),
    ]);
  }
  const candidates: Array<string | undefined> = [
    roundOutputArchivePath(threadId, round, options.root, options.agent),
    roundOutputArchivePath(threadId, round, options.root),
    // Rounds written before a log directory was configured live here.
    path.join(
      bridgeLogsDir(options.root),
      WORKER_LOG_DIRNAME,
      `${artifactBase(threadId, round)}${ROUND_OUTPUT_ARCHIVE_SUFFIX}`,
    ),
    path.join(
      bridgeLogsDir(options.root),
      `${artifactBase(threadId, round)}${ROUND_OUTPUT_ARCHIVE_SUFFIX}`,
    ),
  ];
  return resolveExistingArtifactPath(candidates);
}

export interface RoundArtifact {
  round: number;
  /** `execution` = raw stdio log, `output` = pre-compaction output archive. */
  kind: 'execution' | 'output';
  path: string;
  sizeBytes: number;
  mtimeMs: number;
}

/** Every artifact recorded under worker_log/ or legacy logs/ for one task, ordered by round. */
export function listRoundArtifacts(
  threadId: string,
  root?: string,
  agent?: string,
  artifactDir?: string,
): RoundArtifact[] {
  const dirs = new Set<string>();
  if (artifactDir) {
    // Self-contained task directory: never rescan every agent's global logs.
    dirs.add(taskWorkerLogDir(artifactDir));
  } else {
    dirs.add(getWorkerLogDir(agent, root));
    dirs.add(getWorkerLogDir(undefined, root));
    // Rounds written before a log directory was configured live in the legacy
    // worker_log/ (or, older still, directly under logs/).
    dirs.add(path.join(bridgeLogsDir(root), WORKER_LOG_DIRNAME));
    dirs.add(bridgeLogsDir(root));
  }

  const prefix = `${safeThreadIdPart(threadId)}_round_`;
  const map = new Map<string, RoundArtifact>();

  for (const dir of dirs) {
    if (!fs.existsSync(dir)) continue;
    try {
      for (const name of fs.readdirSync(dir)) {
        if (!name.startsWith(prefix)) continue;
        const match = /_round_(\d+)(\.log|\.output\.md)$/.exec(name);
        if (!match) continue;
        const round = Number(match[1]);
        const kind: 'execution' | 'output' = match[2] === '.log' ? 'execution' : 'output';
        const key = `${round}:${kind}`;
        if (map.has(key)) continue;
        const full = path.join(dir, name);
        try {
          const stat = fs.statSync(full);
          map.set(key, {
            round,
            kind,
            path: full,
            sizeBytes: stat.size,
            mtimeMs: stat.mtimeMs,
          });
        } catch {}
      }
    } catch {}
  }
  return Array.from(map.values()).sort((a, b) => a.round - b.round || a.kind.localeCompare(b.kind));
}

/**
 * Execution log of the newest round, compared numerically.
 *
 * This replaces the old "sort names, take the last" lookup.
 */
export function findLatestExecutionLog(
  threadId: string,
  root?: string,
  agent?: string,
  artifactDir?: string,
): RoundArtifact | undefined {
  const logs = listRoundArtifacts(threadId, root, agent, artifactDir).filter((a) => a.kind === 'execution');
  if (logs.length === 0) return undefined;
  return logs.reduce((a, b) => (b.round > a.round ? b : a));
}

/**
 * Resolve a file that may live under the store's own directory or the default
 * home (dispatcher/rework used `~/.agent-mesh-bridge/logs`, the supervisor used
 * its own storage dir).
 */
export function resolveExistingArtifactPath(
  candidates: Array<string | undefined>,
): string | undefined {
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) return candidate;
    } catch {}
  }
  return undefined;
}

export interface PagedText {
  path: string;
  content: string;
  /** First byte included in `content`. */
  startOffset: number;
  /** Byte offset to pass as `offsetBytes` to continue reading. */
  nextOffset: number;
  totalBytes: number;
  truncated: boolean;
}

/**
 * Read a bounded window of a UTF-8 file.
 *
 * Dumping a multi-megabyte execution log into a caller's context is exactly the
 * problem compaction exists to solve, so every raw-log read is paged.
 */
export function readTextFilePaged(
  filePath: string,
  options: { offsetBytes?: number; maxBytes?: number } = {},
): PagedText {
  const maxBytes = Math.min(
    Math.max(1, options.maxBytes ?? DEFAULT_RAW_LOG_BYTES),
    MAX_RAW_LOG_BYTES,
  );
  const totalBytes = fs.statSync(filePath).size;
  const startOffset = Math.min(Math.max(0, options.offsetBytes ?? 0), totalBytes);
  const length = Math.max(0, Math.min(maxBytes, totalBytes - startOffset));

  let content = '';
  if (length > 0) {
    const buffer = Buffer.allocUnsafe(length);
    const fd = fs.openSync(filePath, 'r');
    try {
      fs.readSync(fd, buffer, 0, length, startOffset);
    } finally {
      fs.closeSync(fd);
    }
    content = buffer.toString('utf8');
    // A window can cut a multi-byte character in half; drop the replacement
    // chars at the window edges instead of corrupting the payload.
    if (startOffset > 0) content = content.replace(/^\uFFFD+/, '');
    if (startOffset + length < totalBytes) content = content.replace(/\uFFFD+$/, '');
  }

  const nextOffset = startOffset + length;
  return {
    path: filePath,
    content,
    startOffset,
    nextOffset,
    totalBytes,
    truncated: nextOffset < totalBytes || startOffset > 0,
  };
}

/** Whole-file read used only for small artifacts such as an output archive. */
export function readTextFileCapped(
  filePath: string,
  maxBytes: number = DEFAULT_RAW_LOG_BYTES,
): PagedText {
  return readTextFilePaged(filePath, { offsetBytes: 0, maxBytes });
}

export function formatBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes <= 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}
