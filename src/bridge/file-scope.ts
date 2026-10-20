import * as fs from 'fs';
import * as path from 'path';
import { DelegationThread } from './types';

/**
 * Narrower file-scope model: tasks declare concrete code files, never
 * directories or glob patterns. A directory/glob would lock an entire subtree
 * and serialize unrelated work, so it is rejected at dispatch time.
 */

/** Extensionless files that are still legitimate concrete code/config targets. */
const EXTENSIONLESS_CODE_FILES = new Set([
  'dockerfile',
  'makefile',
  'jenkinsfile',
  'procfile',
  'gemfile',
  'rakefile',
  'brewfile',
  'vagrantfile',
  'cmakelists.txt',
  'license',
  'notice',
  'readme',
]);

export interface ExpectedFilesValidation {
  ok: boolean;
  files: string[];
  errors: string[];
}

/**
 * Validate that every expected file is a concrete code file.
 *
 * Rules:
 *  - no trailing slash (that is a directory scope);
 *  - no glob metacharacters (`*`, `?`, `[]`, `{}`);
 *  - no `.` / `..` path segments;
 *  - if the path exists it must be a regular file, not a directory;
 *  - if the path does not exist yet, its basename must look like a file
 *    (has an extension) or be a known extensionless source file.
 */
export function validateExpectedFiles(
  files: string[] | undefined,
  workspacePath?: string,
): ExpectedFilesValidation {
  if (!files || files.length === 0) {
    return { ok: true, files: [], errors: [] };
  }

  const workspace = workspacePath || process.cwd();
  const accepted: string[] = [];
  const errors: string[] = [];
  const seen = new Set<string>();

  for (const raw of files) {
    const entry = (raw || '').trim();
    if (!entry) {
      errors.push('Empty expectedFiles entry is not allowed.');
      continue;
    }

    if (/[\\/]$/.test(entry)) {
      errors.push(
        `"${entry}" ends with a path separator; directories are not allowed. List concrete files instead.`,
      );
      continue;
    }

    // `*`/`?` are the broad wildcards that widen a lock scope. Bracketed route
    // filenames such as `pages/[id].tsx` are legitimate concrete files.
    if (/[*?]/.test(entry)) {
      errors.push(
        `"${entry}" contains a wildcard; list concrete code files instead of patterns.`,
      );
      continue;
    }

    const segments = entry.split(/[\\/]+/).filter(Boolean);
    // A leading "./" is harmless; an embedded "." or any ".." is not a
    // concrete file path.
    if (segments.some((segment, index) => segment === '..' || (segment === '.' && index > 0))) {
      errors.push(`"${entry}" contains "." or ".." path segments; use a concrete file path.`);
      continue;
    }

    const normalizedForDedupe = normalizeWorkspacePath(entry, workspacePath);
    if (seen.has(normalizedForDedupe)) continue;

    const absolute = path.isAbsolute(entry) ? entry : path.resolve(workspace, entry);
    let exists = false;
    try {
      exists = fs.existsSync(absolute);
      if (exists && fs.statSync(absolute).isDirectory()) {
        errors.push(
          `"${entry}" is a directory; expectedFiles must name concrete code files (e.g. "src/service/foo.ts").`,
        );
        continue;
      }
    } catch {
      // Fall through to the basename heuristic.
    }

    const basename = path.basename(entry);
    const hasExtension = /\.[^./\\]+$/.test(basename);
    if (!exists && !hasExtension && !EXTENSIONLESS_CODE_FILES.has(basename.toLowerCase())) {
      errors.push(
        `"${entry}" looks like a directory (no file extension); name the concrete code file instead.`,
      );
      continue;
    }

    seen.add(normalizedForDedupe);
    accepted.push(entry);
  }

  return { ok: errors.length === 0, files: accepted, errors };
}

/**
 * Normalizes a file path relative to workspace for cross-platform comparison.
 */
export function normalizeWorkspacePath(filePath: string, workspacePath?: string): string {
  if (!filePath) return '';
  let norm = filePath.trim().replace(/\\/g, '/');

  // Strip workspace prefix if present
  if (workspacePath) {
    const normWs = workspacePath.trim().replace(/\\/g, '/').replace(/\/+$/, '');
    if (norm.toLowerCase().startsWith(normWs.toLowerCase() + '/')) {
      norm = norm.slice(normWs.length + 1);
    }
  }

  // Strip leading ./
  norm = norm.replace(/^\.\//, '');

  // Lowercase on Windows / macOS for case-insensitive matching
  if (process.platform === 'win32' || process.platform === 'darwin') {
    norm = norm.toLowerCase();
  }

  return norm;
}

/**
 * Two tasks conflict only when they name the same concrete file.
 *
 * Directory-prefix and glob matching were deliberately removed: a single
 * directory/pattern scope used to lock an entire subtree and block unrelated
 * tasks. New dispatches reject those scopes; legacy ledger entries simply no
 * longer over-lock.
 */
export function doPathsConflict(pathA: string, pathB: string): boolean {
  if (!pathA || !pathB) return false;
  return pathA === pathB;
}

/**
 * Checks if two sets of file scopes conflict.
 */
export function areFileScopesConflicting(
  filesA?: string[],
  filesB?: string[],
  workspaceA?: string,
  workspaceB?: string,
): boolean {
  if (!filesA || filesA.length === 0 || !filesB || filesB.length === 0) {
    return false;
  }

  // If different workspaces are specified, they do not conflict
  if (workspaceA && workspaceB) {
    const normWsA = workspaceA.trim().replace(/\\/g, '/').toLowerCase();
    const normWsB = workspaceB.trim().replace(/\\/g, '/').toLowerCase();
    if (normWsA !== normWsB) return false;
  }

  const normalizedA = filesA.map((f) => normalizeWorkspacePath(f, workspaceA)).filter(Boolean);
  const normalizedB = filesB.map((f) => normalizeWorkspacePath(f, workspaceB)).filter(Boolean);

  for (const a of normalizedA) {
    for (const b of normalizedB) {
      if (doPathsConflict(a, b)) {
        return true;
      }
    }
  }

  return false;
}

export interface FileConflictResult {
  hasConflict: boolean;
  conflictingThread?: DelegationThread;
  conflictingFiles?: string[];
}

/**
 * Finds if a candidate thread conflicts with any active threads.
 */
export function findFileConflicts(
  candidate: DelegationThread,
  activeThreads: DelegationThread[],
): FileConflictResult {
  if (!candidate.expectedFiles || candidate.expectedFiles.length === 0) {
    return { hasConflict: false };
  }

  for (const active of activeThreads) {
    if (active.threadId === candidate.threadId) continue;
    if (!active.expectedFiles || active.expectedFiles.length === 0) continue;

    if (
      areFileScopesConflicting(
        candidate.expectedFiles,
        active.expectedFiles,
        candidate.workspacePath,
        active.workspacePath,
      )
    ) {
      // Find overlapping paths
      const overlapping: string[] = [];
      for (const fA of candidate.expectedFiles) {
        for (const fB of active.expectedFiles) {
          const normA = normalizeWorkspacePath(fA, candidate.workspacePath);
          const normB = normalizeWorkspacePath(fB, active.workspacePath);
          if (doPathsConflict(normA, normB)) {
            overlapping.push(fA);
            break;
          }
        }
      }

      return {
        hasConflict: true,
        conflictingThread: active,
        conflictingFiles: overlapping,
      };
    }
  }

  return { hasConflict: false };
}
