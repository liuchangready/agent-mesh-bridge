/**
 * Deterministic terminal summaries and bridge-side archival.
 *
 * A task that reaches `completed` or `failed` gets a small, read-only digest
 * (`thread.finalSummary`). The digest is built from the ledger only — never by
 * another model turn — so it is cheap, reproducible and safe to keep after the
 * bulky rounds, logs and native-session ids are removed.
 *
 * Archival keeps the task row (id, title, status, agents/models, MR and the
 * digest) and drops everything conversational: round prompts/outputs, session
 * ids, reviewer logs and per-round execution logs. Native CLI conversations are
 * NOT deleted by archival; `delete_task` is the explicit full purge.
 */

import * as fs from 'fs';
import * as path from 'path';
import {
  DelegationThread,
  MergeRequestInfo,
  TaskFinalSummary,
  ThreadStatus,
  isMergePending,
} from './types';
import { aggregateThreadTokenUsage } from './token-usage-collector';
import * as zlib from 'zlib';
import {
  bridgeHomeDir,
  bridgeLogsDir,
  getWorkerLogDir,
  getReviewerLogDir,
  getArchiveLogDir,
} from './round-log';

/** How many newest tasks stay hot; older terminal tasks are archived. */
export const DEFAULT_ARCHIVE_KEEP_RECENT = 20;

const SUMMARY_MAX_CHARS = 1200;

export function isTerminalThreadStatus(status: ThreadStatus): boolean {
  return status === 'completed' || status === 'failed' || status === 'review_failed';
}

/** Strip ANSI/JSON noise and collapse whitespace so the digest stays readable. */
export function cleanSummaryText(text: string | undefined, maxChars = SUMMARY_MAX_CHARS): string {
  let value = String(text || '');
  // eslint-disable-next-line no-control-regex
  value = value.replace(/\u001b\[[0-9;]*m/g, '');
  value = value.replace(/\r\n?/g, '\n').replace(/[ \t]+/g, ' ').trim();
  if (value.length <= maxChars) return value;
  const head = Math.max(0, maxChars - 120);
  return `${value.slice(0, head).trim()}\n…（已截断，共 ${value.length} 字符）\n${value
    .slice(-80)
    .trim()}`;
}

function uniqueFiles(thread: DelegationThread): string[] {
  const files = new Set<string>();
  for (const round of thread.rounds || []) {
    for (const file of round.touchedFiles || []) files.add(file);
  }
  return Array.from(files).sort();
}

function approvalRemarks(thread: DelegationThread): string | undefined {
  for (let i = (thread.rounds || []).length - 1; i >= 0; i--) {
    const round = thread.rounds[i];
    if (round.type === 'approval' && round.prompt && round.prompt.trim()) {
      return round.prompt.trim();
    }
  }
  return undefined;
}

function lastWorkRound(thread: DelegationThread) {
  for (let i = (thread.rounds || []).length - 1; i >= 0; i--) {
    const round = thread.rounds[i];
    if (round.type !== 'approval') return round;
  }
  return undefined;
}

/**
 * Build the terminal digest. Pure: never mutates the thread.
 *
 * `mergeRequest` may be passed explicitly when the caller records the MR after
 * the task already completed, so the digest can be refreshed.
 */
export function buildFinalSummary(
  thread: DelegationThread,
  mergeRequest?: MergeRequestInfo,
): TaskFinalSummary {
  const terminal: TaskFinalSummary['kind'] =
    thread.status === 'completed'
      ? 'completed'
      : thread.status === 'review_failed'
        ? 'review_failed'
        : 'failed';
  const work = lastWorkRound(thread);
  const approval = approvalRemarks(thread);
  const reviewFailure = thread.reviewFailure;
  const error = work?.error || work?.interruption?.reason || reviewFailure?.reason;
  const output = work?.output;
  const tokens = aggregateThreadTokenUsage(thread.rounds || []);
  const changedFiles = uniqueFiles(thread);
  const mr = mergeRequest || thread.mergeRequest;

  const headline =
    terminal === 'completed'
      ? `审阅通过${approval ? `：${approval}` : '。'}`
      : terminal === 'review_failed'
        ? `检视失败${reviewFailure?.reason ? `：${reviewFailure.reason}` : '：未取得检视结论。'}`
        : `任务失败${error ? `：${error}` : '。'}`;
  const detail = terminal === 'completed' ? output : output || error;
  const summary = cleanSummaryText(
    [
      headline,
      detail ? `\n${detail}` : '',
      changedFiles.length > 0 ? `\n改动文件：${changedFiles.join(', ')}` : '',
      mr?.url ? `\nMR：${mr.url}` : '',
    ].join(''),
  );

  return {
    kind: terminal,
    at: new Date().toISOString(),
    title: thread.title,
    verdict: terminal === 'completed' ? approval : error,
    summary,
    ...(changedFiles.length > 0 ? { changedFiles } : {}),
    ...(terminal !== 'completed' && error ? { error } : {}),
    rounds: (thread.rounds || []).filter((r) => r.type !== 'approval').length,
    ...(mr ? { mergeRequest: mr } : {}),
    reviewerAgent: thread.initiatorAgent,
    ...(thread.reviewerModel ? { reviewerModel: thread.reviewerModel } : {}),
    executorAgent: thread.targetAgent,
    ...(thread.targetModel ? { executorModel: thread.targetModel } : {}),
    ...(tokens.measuredRounds > 0
      ? {
          totalTokens: tokens.totalTokens,
          promptTokens: tokens.totalPromptTokens,
          outputTokens: tokens.totalOutputTokens,
        }
      : {}),
  };
}

/** Fill `finalSummary` on a terminal task; returns true when it changed. */
export function ensureFinalSummary(thread: DelegationThread): boolean {
  if (!isTerminalThreadStatus(thread.status)) return false;
  if (thread.finalSummary && thread.finalSummary.kind === thread.status) return false;
  thread.finalSummary = buildFinalSummary(thread);
  return true;
}

/**
 * Archive one task: keep metadata + digest, drop everything conversational.
 * Returns true when the thread was modified.
 */
export function archiveThreadData(thread: DelegationThread, now = new Date()): boolean {
  if (thread.archivedAt || !isTerminalThreadStatus(thread.status)) return false;
  ensureFinalSummary(thread);
  const digest = thread.finalSummary;
  if (!digest) return false;
  thread.archivedAt = now.toISOString();
  thread.rounds = [];
  thread.currentRound = 0;
  thread.callerSessionId = undefined;
  thread.requesterSessionId = undefined;
  thread.receiverSessionId = '';
  thread.receiverSessionCaptured = undefined;
  thread.reviewBootstrap = undefined;
  thread.reviewWake = undefined;
  thread.reviewBlocked = undefined;
  thread.interruption = undefined;
  thread.interruptions = undefined;
  thread.failureNotifications = undefined;
  thread.summary = {
    lastPromptSnippet: digest.summary.slice(0, 100),
    lastOutputSnippet: digest.summary.slice(0, 150),
    totalDurationMs: thread.summary?.totalDurationMs || 0,
  };
  // The digest owns the conversation content now; stop duplicating it.
  thread.modelNotice = undefined;
  return true;
}

/** Directory holding the one-shot reviewer bootstrap tokens. */
export function bootstrapDir(root?: string): string {
  return path.join(bridgeHomeDir(root), 'bootstrap');
}

/**
 * Create a standard POSIX ustar tar.gz archive buffer from a list of files
 * using only Node.js standard built-ins.
 */
export function createTarGzBuffer(
  files: Array<{ name: string; content: Buffer | string; mtime?: Date }>,
): Buffer {
  const blocks: Buffer[] = [];
  for (const file of files) {
    const contentBuf = Buffer.isBuffer(file.content)
      ? file.content
      : Buffer.from(file.content, 'utf8');
    const header = Buffer.alloc(512, 0);
    const cleanName = file.name.replace(/\\/g, '/');
    const nameBuf = Buffer.from(cleanName, 'utf8');
    nameBuf.copy(header, 0, 0, Math.min(nameBuf.length, 100));
    header.write('0000644\0', 100, 8, 'ascii');
    header.write('0000000\0', 108, 8, 'ascii');
    header.write('0000000\0', 116, 8, 'ascii');
    const sizeOctal = contentBuf.length.toString(8).padStart(11, '0') + ' ';
    header.write(sizeOctal, 124, 12, 'ascii');
    const mtimeSec = Math.floor((file.mtime ? file.mtime.getTime() : Date.now()) / 1000);
    const mtimeOctal = mtimeSec.toString(8).padStart(11, '0') + ' ';
    header.write(mtimeOctal, 136, 12, 'ascii');
    header.write('        ', 148, 8, 'ascii');
    header.write('0', 156, 1, 'ascii');
    header.write('ustar\0', 257, 6, 'ascii');
    header.write('00', 263, 2, 'ascii');

    let sum = 0;
    for (let i = 0; i < 512; i++) {
      sum += header[i];
    }
    const checksumOctal = sum.toString(8).padStart(6, '0') + '\0 ';
    header.write(checksumOctal, 148, 8, 'ascii');

    blocks.push(header);
    blocks.push(contentBuf);

    const remainder = contentBuf.length % 512;
    if (remainder > 0) {
      blocks.push(Buffer.alloc(512 - remainder, 0));
    }
  }

  // End of archive marker: two 512-byte blocks of zeroes
  blocks.push(Buffer.alloc(1024, 0));

  const tarBuffer = Buffer.concat(blocks);
  return zlib.gzipSync(tarBuffer);
}

/**
 * Archive the task's logs and terminal summary into the Archive/ folder of
 * the worker and reviewer agents (as well as default).
 *
 * Produces:
 *   - <threadId>_archive.tar.gz (compressed tar containing worker_log, reviewer_log, and summary.json)
 *   - <threadId>_archive.json (summary metadata and archive manifest)
 */
export function archiveCompletedTaskLogs(
  thread: DelegationThread,
  root?: string,
): { tarGzPaths: string[]; jsonPaths: string[] } {
  ensureFinalSummary(thread);
  const safe = String(thread.threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');

  const archiveDirs = Array.from(
    new Set([
      getArchiveLogDir(thread.targetAgent, root),
      getArchiveLogDir(thread.initiatorAgent, root),
      getArchiveLogDir(undefined, root),
    ]),
  );

  for (const dir of archiveDirs) {
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch {}
  }

  const entries: Array<{ name: string; content: Buffer | string; mtime?: Date }> = [];
  const foundFilenames = new Set<string>();

  // 1. Gather worker logs
  const workerDirs = Array.from(
    new Set([
      getWorkerLogDir(thread.targetAgent, root),
      getWorkerLogDir(undefined, root),
      bridgeLogsDir(root),
    ]),
  );
  for (const dir of workerDirs) {
    if (!fs.existsSync(dir)) continue;
    try {
      for (const name of fs.readdirSync(dir)) {
        if (!name.startsWith(`${safe}_round_`)) continue;
        if (!name.endsWith('.log') && !name.endsWith('.output.md')) continue;
        if (foundFilenames.has(name)) continue;
        const full = path.join(dir, name);
        try {
          const content = fs.readFileSync(full);
          const stat = fs.statSync(full);
          entries.push({
            name: `worker_log/${name}`,
            content,
            mtime: stat.mtime,
          });
          foundFilenames.add(name);
        } catch {}
      }
    } catch {}
  }

  // 2. Gather reviewer logs
  const reviewerDirs = Array.from(
    new Set([
      getReviewerLogDir(thread.initiatorAgent, root),
      getReviewerLogDir(undefined, root),
      bridgeLogsDir(root),
    ]),
  );
  for (const dir of reviewerDirs) {
    if (!fs.existsSync(dir)) continue;
    try {
      for (const name of fs.readdirSync(dir)) {
        if (
          !name.startsWith(`${safe}_round_`) &&
          !name.startsWith(`${safe}_bootstrap.log`)
        ) {
          continue;
        }
        if (!name.endsWith('.reviewer.log') && !name.endsWith('_bootstrap.log')) continue;
        if (foundFilenames.has(name)) continue;
        const full = path.join(dir, name);
        try {
          const content = fs.readFileSync(full);
          const stat = fs.statSync(full);
          entries.push({
            name: `reviewer_log/${name}`,
            content,
            mtime: stat.mtime,
          });
          foundFilenames.add(name);
        } catch {}
      }
    } catch {}
  }

  // 3. Manifest / summary entry
  const manifest = {
    threadId: thread.threadId,
    title: thread.title,
    status: thread.status,
    initiatorAgent: thread.initiatorAgent,
    targetAgent: thread.targetAgent,
    initiatorModel: thread.initiatorModel,
    targetModel: thread.targetModel,
    reviewerModel: thread.reviewerModel,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
    archivedAt: thread.archivedAt || new Date().toISOString(),
    finalSummary: thread.finalSummary,
    mergeRequest: thread.mergeRequest,
    archivedLogFiles: entries.map((e) => e.name),
  };
  const summaryJsonStr = JSON.stringify(manifest, null, 2);
  entries.unshift({
    name: 'summary.json',
    content: summaryJsonStr,
  });

  const tarGzBuffer = createTarGzBuffer(entries);

  const tarGzPaths: string[] = [];
  const jsonPaths: string[] = [];

  for (const dir of archiveDirs) {
    try {
      const tarGzPath = path.join(dir, `${safe}_archive.tar.gz`);
      fs.writeFileSync(tarGzPath, tarGzBuffer);
      tarGzPaths.push(tarGzPath);

      const jsonPath = path.join(dir, `${safe}_archive.json`);
      fs.writeFileSync(jsonPath, summaryJsonStr, 'utf8');
      jsonPaths.push(jsonPath);
    } catch {}
  }

  return { tarGzPaths, jsonPaths };
}

/**
 * Remove bridge-side artifacts of one task (worker/reviewer logs + bootstrap token).
 * By default, keeps the archived bundle in Archive/ when task is archived.
 */
export function removeTaskArtifacts(
  threadId: string,
  root?: string,
  options: {
    keepArchive?: boolean;
    targetAgent?: string;
    initiatorAgent?: string;
  } = {},
): number {
  let removed = 0;
  const safe = String(threadId || '').replace(/[^A-Za-z0-9_.-]/g, '_');
  const keepArchive = options.keepArchive !== false;

  const targetDirs = Array.from(
    new Set([
      getWorkerLogDir(options.targetAgent, root),
      getWorkerLogDir(undefined, root),
      getReviewerLogDir(options.initiatorAgent, root),
      getReviewerLogDir(undefined, root),
      bridgeLogsDir(root),
    ]),
  );

  for (const dir of targetDirs) {
    if (!fs.existsSync(dir)) continue;
    try {
      for (const name of fs.readdirSync(dir)) {
        if (!name.startsWith(`${safe}_`)) continue;
        try {
          fs.unlinkSync(path.join(dir, name));
          removed++;
        } catch {}
      }
    } catch {}
  }

  try {
    const file = path.join(bootstrapDir(root), `${safe}.json`);
    if (fs.existsSync(file)) {
      fs.unlinkSync(file);
      removed++;
    }
  } catch {}

  if (!keepArchive) {
    const archiveDirs = Array.from(
      new Set([
        getArchiveLogDir(options.targetAgent, root),
        getArchiveLogDir(options.initiatorAgent, root),
        getArchiveLogDir(undefined, root),
      ]),
    );
    for (const dir of archiveDirs) {
      if (!fs.existsSync(dir)) continue;
      try {
        for (const name of fs.readdirSync(dir)) {
          if (!name.startsWith(`${safe}_archive.`)) continue;
          try {
            fs.unlinkSync(path.join(dir, name));
            removed++;
          } catch {}
        }
      } catch {}
    }
  }

  return removed;
}

/**
 * Terminal, not-yet-archived tasks beyond the newest `keepRecent` entries.
 *
 * Ordering is by `createdAt` (not `updatedAt`): recording an MR or a later
 * review must not make an old task look young and escape archival.
 */
export function selectTasksToArchive(
  threads: DelegationThread[],
  keepRecent: number = DEFAULT_ARCHIVE_KEEP_RECENT,
): DelegationThread[] {
  const keep = Number.isFinite(keepRecent) ? Math.max(0, Math.floor(keepRecent)) : DEFAULT_ARCHIVE_KEEP_RECENT;
  const sorted = [...(threads || [])].sort(
    (a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
  );
  // Only unarchived tasks occupy the hot window; otherwise an already archived
  // task would silently reduce the number of task rows the user can still open.
  return sorted
    .filter((thread) => !thread.archivedAt)
    .slice(keep)
    .filter(
      (thread) =>
        isTerminalThreadStatus(thread.status) &&
        // A completed worktree task whose MR is not recorded still owns the only
        // surviving diff context; archiving it would discard the evidence.
        !isMergePending(thread),
    );
}

