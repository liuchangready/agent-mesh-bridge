/**
 * Unified per-task data directory.
 *
 * A new task owns exactly one bridge-side directory:
 *
 *   <taskDataRoot>/<threadId>/
 *     worktree/       Git worktree the receiver edits
 *     worker_log/     raw stdio + pre-compaction output archives
 *     reviewer_log/   reviewer wakes + dedicated-reviewer bootstrap log
 *     Archive/        terminal tar.gz + summary JSON
 *     bootstrap/      one-shot reviewer credential (removed when consumed)
 *     validation/     temporary combined-validation worktree
 *     model-overlays/ bridge-generated one-run DSH model patch files
 *
 * The absolute path is persisted as `thread.artifactDir` / `taskDataDir` at
 * creation. Every later write must use that recorded path, never re-resolve the
 * current global setting: changing `agentMeshBridge.taskDataRoot` must not split
 * one task's files across two roots.
 *
 * Legacy ledger entries have no `artifactDir`; they keep their original worktree
 * path and per-agent `logDirectories` layout untouched.
 */

import * as fs from 'fs';
import * as path from 'path';
import { readBridgePolicy } from './bridge-policy';
import {
  ARCHIVE_DIRNAME,
  BOOTSTRAP_DIRNAME,
  MODEL_OVERLAY_DIRNAME,
  REVIEWER_LOG_DIRNAME,
  VALIDATION_DIRNAME,
  WORKER_LOG_DIRNAME,
  WORKTREE_DIRNAME,
  bridgeHomeDir,
  safeThreadIdPart,
} from './round-log';

export const TASK_DATA_ROOT_DIRNAME = 'tasks';

export interface TaskDataDirectories {
  /** Absolute `<taskDataRoot>/<threadId>` (persisted as `thread.artifactDir`). */
  artifactDir: string;
  worktreeDir: string;
  workerLogDir: string;
  reviewerLogDir: string;
  archiveDir: string;
  bootstrapDir: string;
  validationDir: string;
  modelOverlayDir: string;
}

/**
 * Resolve the configured root for NEW tasks.
 *
 * Empty / absent uses `<storageDir>/tasks` (which is `~/.agent-mesh-bridge/tasks`
 * for the real bridge home). A relative configured value resolves against the
 * bridge home/storage directory, matching the existing `worktreeRoot` /
 * `logDirectories` convention. Legacy `worktreeRoot` is intentionally NOT
 * consulted here: it remains a compatibility setting for old tasks only.
 */
export function resolveTaskDataRoot(storageDir: string, root?: string): string {
  const policyRoot = root && root.trim() ? root : storageDir;
  const configured = readBridgePolicy(policyRoot).taskDataRoot;
  if (typeof configured === 'string' && configured.trim()) {
    const value = configured.trim();
    return path.isAbsolute(value)
      ? path.normalize(value)
      : path.resolve(policyRoot, value);
  }
  return path.join(policyRoot || bridgeHomeDir(root), TASK_DATA_ROOT_DIRNAME);
}

/** Absolute task directory for a thread; the id is sanitized, never caller-controlled traversal. */
export function taskArtifactDir(
  threadId: string,
  storageDir: string,
  root?: string,
): string {
  return path.join(resolveTaskDataRoot(storageDir, root), safeThreadIdPart(threadId));
}

/**
 * Create (safely, idempotently) the standard task directory tree before a task
 * is recorded. Throwing here is deliberate: dispatch must fail without a
 * half-created ledger entry.
 */
export function ensureTaskDataDir(
  threadId: string,
  storageDir: string,
  root?: string,
): TaskDataDirectories {
  const artifactDir = taskArtifactDir(threadId, storageDir, root);
  const dirs: TaskDataDirectories = {
    artifactDir,
    worktreeDir: path.join(artifactDir, WORKTREE_DIRNAME),
    workerLogDir: path.join(artifactDir, WORKER_LOG_DIRNAME),
    reviewerLogDir: path.join(artifactDir, REVIEWER_LOG_DIRNAME),
    archiveDir: path.join(artifactDir, ARCHIVE_DIRNAME),
    bootstrapDir: path.join(artifactDir, BOOTSTRAP_DIRNAME),
    validationDir: path.join(artifactDir, VALIDATION_DIRNAME),
    modelOverlayDir: path.join(artifactDir, MODEL_OVERLAY_DIRNAME),
  };
  for (const dir of [
    artifactDir,
    dirs.workerLogDir,
    dirs.reviewerLogDir,
    dirs.archiveDir,
    dirs.bootstrapDir,
    dirs.modelOverlayDir,
  ]) {
    fs.mkdirSync(dir, { recursive: true });
  }
  return dirs;
}

/** Ensure the directory that will hold a new lifecycle file exists. */
export function ensureTaskSubdir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true });
}

/**
 * Remove a task's bridge-managed directory.
 *
 * `includeWorktree` is false for the ledger deletion step (Git must unregister
 * the worktree first) and true after `git worktree remove` has run. Best-effort:
 * a cleanup failure must not resurrect a deleted ledger entry.
 */
export function removeTaskDataDir(
  artifactDir: string | undefined,
  includeWorktree: boolean,
): boolean {
  if (!artifactDir) return false;
  const resolved = path.resolve(artifactDir);
  if (!path.isAbsolute(resolved)) return false;
  try {
    if (includeWorktree) {
      fs.rmSync(resolved, { recursive: true, force: true });
    } else {
      for (const name of [
        WORKER_LOG_DIRNAME,
        REVIEWER_LOG_DIRNAME,
        ARCHIVE_DIRNAME,
        BOOTSTRAP_DIRNAME,
        VALIDATION_DIRNAME,
        MODEL_OVERLAY_DIRNAME,
      ]) {
        fs.rmSync(path.join(resolved, name), { recursive: true, force: true });
      }
      // Remove the root only when nothing else (the worktree) remains.
      try {
        if (fs.readdirSync(resolved).length === 0) fs.rmdirSync(resolved);
      } catch {}
    }
    return true;
  } catch {
    return false;
  }
}
