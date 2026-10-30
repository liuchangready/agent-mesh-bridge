/**
 * Per-thread reviewer (caller) side log.
 *
 * A delegated round only records what the RECEIVER printed: its stdio stream is
 * written to `logs/<threadId>_round_<n>.log`. The reviewer is a different
 * process — the caller's own CLI, woken in its native session by
 * `CallbackManager` — and until now its output only ever landed in the shared,
 * append-only `callbacks.log` (hundreds of megabytes, interleaved with every
 * other task). That made "what did the reviewer actually say?" unanswerable from
 * the ledger, from the detail panel and from MCP.
 *
 * This module gives one reviewer wake its own artifact:
 *
 *   logs/<threadId>_round_<n>.reviewer.log
 *
 * One file per round, appended across re-wakes of the same round (each launch
 * writes a banner), bounded so a runaway reviewer cannot fill the disk, and
 * read back through the same paged helpers as round logs.
 */

import * as fs from 'fs';
import * as path from 'path';
import {
  PagedText,
  bridgeLogsDir,
  formatBytes,
  readTextFilePaged,
  safeThreadIdPart,
} from './round-log';

/** Suffix that distinguishes a reviewer log from the receiver's round log. */
export const REVIEWER_LOG_SUFFIX = '.reviewer.log';

/**
 * Hard cap for one round's reviewer log. Reviewers can print megabytes of test
 * output; the panel only renders a tail and the full file is for `grep`, so 4 MB
 * keeps a whole round answerable without unbounded growth.
 */
export const REVIEWER_LOG_MAX_BYTES = 4 * 1024 * 1024;

/** Bytes the detail panel renders inline (the full file stays on disk). */
export const REVIEWER_LOG_TAIL_BYTES = 8 * 1024;

export function reviewerLogPath(threadId: string, round: number, root?: string): string {
  const safeRound = Number.isFinite(round) ? Math.max(1, Math.floor(round)) : 1;
  return path.join(
    bridgeLogsDir(root),
    `${safeThreadIdPart(threadId)}_round_${safeRound}${REVIEWER_LOG_SUFFIX}`,
  );
}

export interface ReviewerLogStat {
  path: string;
  exists: boolean;
  sizeBytes: number;
  mtimeMs: number;
}

export function statReviewerLog(threadId: string, round: number, root?: string): ReviewerLogStat {
  const file = reviewerLogPath(threadId, round, root);
  try {
    const stat = fs.statSync(file);
    if (!stat.isFile()) return { path: file, exists: false, sizeBytes: 0, mtimeMs: 0 };
    return { path: file, exists: true, sizeBytes: stat.size, mtimeMs: stat.mtimeMs };
  } catch {
    return { path: file, exists: false, sizeBytes: 0, mtimeMs: 0 };
  }
}

/**
 * Rounds of one thread that actually have a reviewer log, newest first.
 *
 * Used to answer "which rounds can I inspect?" without guessing round numbers.
 */
export function listReviewerLogRounds(threadId: string, root?: string): number[] {
  const dir = bridgeLogsDir(root);
  const prefix = `${safeThreadIdPart(threadId)}_round_`;
  try {
    const rounds: number[] = [];
    for (const name of fs.readdirSync(dir)) {
      if (!name.startsWith(prefix) || !name.endsWith(REVIEWER_LOG_SUFFIX)) continue;
      const match = /_round_(\d+)\.reviewer\.log$/.exec(name);
      if (match) rounds.push(Number(match[1]));
    }
    return rounds.sort((a, b) => b - a);
  } catch {
    return [];
  }
}

/**
 * Bounded tail of one round's reviewer log.
 *
 * Returns `undefined` when the reviewer never ran (or the file was removed), so
 * callers can tell "no reviewer output" from "empty reviewer output".
 */
export function readReviewerLogTail(
  threadId: string,
  round: number,
  options: { maxBytes?: number; root?: string } = {},
): PagedText | undefined {
  const file = reviewerLogPath(threadId, round, options.root);
  if (!fs.existsSync(file)) return undefined;
  const totalBytes = fs.statSync(file).size;
  const maxBytes = Math.max(1, options.maxBytes ?? REVIEWER_LOG_TAIL_BYTES);
  const startOffset = Math.max(0, totalBytes - maxBytes);
  return readTextFilePaged(file, { offsetBytes: startOffset, maxBytes });
}

export interface ReviewerLogHeader {
  agent: string;
  sessionId?: string;
  /** `review` = report handed back, `failure` = failed round notification. */
  kind?: 'review' | 'failure';
  /** Extra one-line context (round type, model, prompt preview...). */
  note?: string;
}

/**
 * Append-only stream writer for one reviewer wake.
 *
 * Every method swallows its own errors: logging must never be the reason a
 * review fails to start. Once the byte cap is hit the file gets a single
 * truncation marker and further chunks are dropped.
 */
export class ReviewerLogWriter {
  public readonly path: string;
  private fd: number | undefined;
  private written = 0;
  private closed = false;
  private capped = false;
  private readonly maxBytes: number;

  constructor(
    threadId: string,
    round: number,
    header: ReviewerLogHeader,
    options: { root?: string; maxBytes?: number } = {},
  ) {
    this.path = reviewerLogPath(threadId, round, options.root);
    this.maxBytes = Math.max(1024, options.maxBytes ?? REVIEWER_LOG_MAX_BYTES);
    try {
      fs.mkdirSync(path.dirname(this.path), { recursive: true });
      this.fd = fs.openSync(this.path, 'a');
      const meta = [
        `stream=reviewer`,
        `thread=${threadId}`,
        `round=${round}`,
        `agent=${header.agent}`,
        header.sessionId ? `session=${header.sessionId}` : '',
        header.kind ? `kind=${header.kind}` : '',
      ]
        .filter(Boolean)
        .join(' ');
      this.append(`\n--- [${new Date().toISOString()}] wake ${meta} ---\n`);
      if (header.note) {
        this.append(`note: ${header.note.replace(/[\r\n]+/g, ' ').slice(0, 300)}\n`);
      }
    } catch {
      this.fd = undefined;
    }
  }

  public get bytes(): number {
    return this.written;
  }

  public get available(): boolean {
    return this.fd !== undefined;
  }

  public write(chunk: string | Buffer): void {
    if (!chunk || this.closed) return;
    this.append(typeof chunk === 'string' ? chunk : chunk.toString('utf8'));
  }

  /** Record the process outcome verbatim so the panel shows how it ended. */
  public writeOutcome(outcome: { exitCode?: number; error?: string }): void {
    if (outcome.error) {
      this.append(`\n[spawn error] ${outcome.error}\n`);
      return;
    }
    this.append(`\n[exit] code=${outcome.exitCode ?? 'null'}\n`);
  }

  public close(): void {
    if (this.closed) return;
    this.closed = true;
    if (this.fd === undefined) return;
    try {
      fs.closeSync(this.fd);
    } catch {
      // Closing a log handle is best effort; the OS reclaims it either way.
    }
    this.fd = undefined;
  }

  private append(text: string): void {
    if (this.fd === undefined || this.capped) return;
    let payload = text;
    const remaining = this.maxBytes - this.written;
    if (Buffer.byteLength(payload, 'utf8') > remaining) {
      // Reserve room for the marker itself so the file never exceeds the cap.
      const marker = `\n[reviewer log truncated at ${formatBytes(this.maxBytes)}]\n`;
      const budget = Math.max(0, remaining - Buffer.byteLength(marker, 'utf8'));
      let head = payload.slice(0, budget);
      // Never cut in the middle of a multi-byte character.
      while (head.length > 0 && Buffer.byteLength(head, 'utf8') > budget) {
        head = head.slice(0, -1);
      }
      payload = head + marker;
      this.capped = true;
    }
    try {
      fs.writeSync(this.fd, payload);
      this.written += Buffer.byteLength(payload, 'utf8');
    } catch {
      this.capped = true;
    }
  }
}

export function createReviewerLogWriter(
  threadId: string,
  round: number,
  header: ReviewerLogHeader,
  options: { root?: string; maxBytes?: number } = {},
): ReviewerLogWriter {
  return new ReviewerLogWriter(threadId, round, header, options);
}
