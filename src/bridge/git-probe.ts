import * as child_process from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

/**
 * Workspace change probe.
 *
 * `expectedFiles` is what a caller *claims* a task will touch. This module
 * measures what a round *actually* touched, so a later task can be blocked from
 * rewriting a file an earlier task already changed — even when the caller
 * declared nothing, and even when the agent edited something outside its
 * declared scope.
 *
 * The probe compares `git status --porcelain` snapshots taken right before and
 * right after the round. A file counts as touched when it appears for the first
 * time, disappears, or its signature (status + size + mtime) changed. Using the
 * signature rather than the status alone is what catches "this file was already
 * dirty, and the round edited it further".
 */

export interface WorkspaceSnapshot {
  /** False when the workspace is missing or not a git repository. */
  available: boolean;
  head: string;
  /** Path (workspace-relative, POSIX separators) -> change signature. */
  entries: Map<string, string>;
  at: string;
  /** Repository root used for the probe (needed to attribute committed edits). */
  cwd: string;
  /** Prefix of the workspace inside the repository (`''` at the root). */
  prefix: string;
}

const GIT_TIMEOUT_MS = 15_000;
const MAX_PROBED_FILES = 2_000;

function git(cwd: string, args: string[]): string | undefined {
  try {
    return child_process.execFileSync('git', ['-C', cwd, ...args], {
      encoding: 'utf8',
      timeout: GIT_TIMEOUT_MS,
      windowsHide: true,
      maxBuffer: 32 * 1024 * 1024,
      stdio: ['ignore', 'pipe', 'ignore'],
    });
  } catch {
    return undefined;
  }
}

function signatureFor(cwd: string, relPath: string, status: string): string {
  if (status.includes('D')) return `${status}:deleted`;
  try {
    const stat = fs.statSync(path.join(cwd, relPath));
    return `${status}:${stat.size}:${Math.round(stat.mtimeMs)}`;
  } catch {
    return `${status}:missing`;
  }
}

/**
 * Parses `git status --porcelain=v1 -z` output into path -> signature.
 *
 * Renames/copies occupy two NUL-separated tokens (`new\0old`), so the parser
 * consumes the extra token instead of misreading the old path as a new entry.
 */
function parsePorcelainZ(cwd: string, raw: string, prefix: string): Map<string, string> {
  const entries = new Map<string, string>();
  const tokens = raw.split('\0');
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (!token || token.length < 4) continue;
    const status = token.slice(0, 2);
    let relPath = token.slice(3);
    if (status[0] === 'R' || status[0] === 'C') i++; // skip the original path

    if (prefix && !relPath.startsWith(prefix)) continue; // outside this workspace
    const workspaceRelative = prefix ? relPath.slice(prefix.length) : relPath;
    if (entries.size >= MAX_PROBED_FILES) break;
    entries.set(workspaceRelative, signatureFor(cwd, relPath, status));
  }
  return entries;
}

/** Captures the workspace change baseline used to attribute a round's edits. */
export function captureWorkspaceSnapshot(cwd: string): WorkspaceSnapshot {
  const at = new Date().toISOString();
  const unavailable: WorkspaceSnapshot = {
    available: false,
    head: '',
    entries: new Map(),
    at,
    cwd,
    prefix: '',
  };
  if (!cwd || !fs.existsSync(cwd)) return unavailable;

  const statusRaw = git(cwd, ['status', '--porcelain=v1', '-z']);
  if (statusRaw === undefined) return unavailable; // not a repo / git unavailable

  const head = (git(cwd, ['rev-parse', 'HEAD']) || '').trim();
  const prefix = (git(cwd, ['rev-parse', '--show-prefix']) || '').replace(/\r?\n$/, '');

  return {
    available: true,
    head,
    entries: parsePorcelainZ(cwd, statusRaw, prefix),
    at,
    cwd,
    prefix,
  };
}

/**
 * Files this round created, modified, deleted or reverted.
 *
 * Returns workspace-relative paths, sorted, capped. An unavailable baseline
 * yields an empty list rather than a noisy "everything changed" claim.
 */
export function diffWorkspaceSnapshots(
  before: WorkspaceSnapshot,
  after: WorkspaceSnapshot,
): string[] {
  if (!before.available || !after.available) return [];

  const touched = new Set<string>();
  for (const [file, signature] of after.entries) {
    const previous = before.entries.get(file);
    if (previous === undefined || previous !== signature) touched.add(file);
  }
  // A file that was dirty before and is clean now was reverted/stashed by the
  // round: that is a change to the file, and a later task must know about it.
  for (const file of before.entries.keys()) {
    if (!after.entries.has(file)) touched.add(file);
  }

  // A round that commits its own work leaves a clean tree, so the status diff
  // sees nothing. Attribute committed paths from the two HEADs instead.
  if (headChanged(before, after)) {
    const committed = git(before.cwd, [
      'diff',
      '--name-only',
      '-z',
      `${before.head}..${after.head}`,
    ]);
    for (const rawPath of committed ? committed.split('\0').filter(Boolean) : []) {
      if (before.prefix && !rawPath.startsWith(before.prefix)) continue;
      touched.add(before.prefix ? rawPath.slice(before.prefix.length) : rawPath);
    }
  }

  return [...touched].sort();
}

/** True when the two snapshots differ in HEAD (the round created commits). */
export function headChanged(before: WorkspaceSnapshot, after: WorkspaceSnapshot): boolean {
  return !!before.head && !!after.head && before.head !== after.head;
}
