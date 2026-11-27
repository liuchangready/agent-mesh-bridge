import { taskArtifactDir } from './task-data-dir';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { readBridgePolicy } from './bridge-policy';
import { WorktreeInfo } from './types';
import { physicalDirectory } from './workspace';

export interface GitRepository {
  commonDir: string;
  repositoryPath: string;
  targetRef: string;
  deliveryTarget?: { branch: string; ref: string; repositoryPath: string };
  baseCommit: string;
}

interface TaskTargetMarker { targetRef: string; deliveryTarget?: { branch: string; ref: string; repositoryPath: string; checkoutPath?: string; baseCommit?: string }; commonDir?: string; repositoryPath?: string; }

class GitSetupError extends Error {}
const GIT_LAUNCH_ERRORS = new Set(['ENOENT', 'EACCES', 'ENOEXEC']);

function gitCandidates(storageDir?: string): string[] {
  const configured = readBridgePolicy(storageDir).gitPath;
  const paths = (Array.isArray(configured) ? configured : [configured])
    .filter((value): value is string => typeof value === 'string' && path.isAbsolute(value.trim()))
    .map((value) => value.trim());
  const commands = os.platform() === 'win32' ? ['git.exe', 'git'] : ['git', 'git.exe'];
  return [...new Set([process.env.MESH_GIT_PATH, process.env.GIT_EXECUTABLE, ...paths, ...commands]
    .filter((value): value is string => !!value))];
}

function git(cwd: string, args: string[], env?: NodeJS.ProcessEnv, storageDir?: string): string {
  let result: ReturnType<typeof spawnSync> | undefined;
  let startupError: NodeJS.ErrnoException | undefined;
  for (const executable of gitCandidates(storageDir)) {
    result = spawnSync(executable, args, { cwd, encoding: 'utf8', windowsHide: true, timeout: 30_000, env });
    const error = result.error as NodeJS.ErrnoException | undefined;
    if (!error || !GIT_LAUNCH_ERRORS.has(error.code || '')) break;
    if (!startupError || startupError.code === 'ENOENT') startupError = error;
  }
  const error = result?.error as NodeJS.ErrnoException | undefined;
  if (startupError && GIT_LAUNCH_ERRORS.has(error?.code || '') && fs.existsSync(cwd)) {
    throw new GitSetupError(
      `GIT_EXECUTABLE_UNAVAILABLE: Bridge could not start Git (${startupError.message}). ` +
      `Configure the full Git executable path in VS Code's user setting git.path, or set MESH_GIT_PATH for a standalone MCP process.`,
      { cause: startupError },
    );
  }
  if (!result || result.error || result.status !== 0) {
    const detail = String(result?.stderr || '').trim() || result?.error?.message || `exit ${result?.status}`;
    if (/dubious ownership/i.test(detail) || /safe\.directory\s/.test(detail)) {
      throw new GitSetupError(`GIT_REPOSITORY_OWNERSHIP: Git refused to access ${cwd}. ${detail}`);
    }
    throw new Error(`Git command failed (${args.join(' ')}): ${detail}`);
  }
  return String(result.stdout || '').trim();
}

function safePath(value: string): string {
  let resolved = path.resolve(value).replace(/[\\/]+$/, '');
  try { resolved = fs.realpathSync(resolved); } catch {}
  return process.platform === 'win32' ? resolved.toLowerCase() : resolved;
}

export function resolveGitRepository(startPath: string, storageDir: string, requestedTargetBranch?: string): GitRepository {
  let repositoryPath = path.resolve(startPath);
  const runGit = (args: string[]) => git(repositoryPath, args, undefined, storageDir);
  let initialCommonDir: string;
  try { initialCommonDir = path.resolve(runGit(['rev-parse', '--path-format=absolute', '--git-common-dir'])); }
  catch (error) {
    if (error instanceof GitSetupError) throw error;
    throw new Error(`Workspace ${repositoryPath} is not a usable Git checkout. Open a Git repository with a committed HEAD before dispatching.`, { cause: error });
  }
  const sourceCheckout = runGit(['rev-parse', '--show-toplevel']);
  const inherited = readTaskTarget(sourceCheckout, storageDir);
  if (inherited?.repositoryPath) repositoryPath = path.resolve(inherited.repositoryPath);
  const commonDir = path.resolve(runGit(['rev-parse', '--path-format=absolute', '--git-common-dir']));
  if (safePath(initialCommonDir) !== safePath(commonDir)) throw new Error('Task worktree points to a different Git repository identity than its recorded source repository.');
  try { runGit(['rev-parse', '--verify', 'HEAD^{commit}']); }
  catch (error) {
    if (error instanceof GitSetupError) throw error;
    throw new Error(`Git checkout ${repositoryPath} has no valid committed HEAD. Create a commit before dispatching.`, { cause: error });
  }
  let targetRef: string;
  let deliveryTarget = inherited?.deliveryTarget;
  if (inherited) {
    if (!inherited.deliveryTarget) throw new Error('MISSING_DELIVERY_TARGET: Cannot dispatch from a historical task worktree.');
    targetRef = inherited.deliveryTarget.ref;
  }
  else {
    let sourceRef = '';
    try { sourceRef = runGit(['symbolic-ref', '--quiet', '--short', 'HEAD']); }
    catch (error) { if (error instanceof GitSetupError) throw error; }
    const branch = (requestedTargetBranch || sourceRef).trim().replace(/^refs\/heads\//, '');
    if (!branch || (!requestedTargetBranch && branch.startsWith('mesh/task/'))) throw new Error('Cannot choose a delivery target from a detached HEAD or mesh task branch. Check out a normal local branch or specify targetBranch.');
    try { runGit(['check-ref-format', '--branch', branch]); }
    catch (error) {
      if (error instanceof GitSetupError) throw error;
      throw new Error(`Invalid local target branch "${branch}". Specify an existing local branch in this repository.`);
    }
    const ref = `refs/heads/${branch}`;
    try { runGit(['rev-parse', '--verify', `${ref}^{commit}`]); }
    catch (error) {
      if (error instanceof GitSetupError) throw error;
      throw new Error(`Local target branch "${branch}" does not exist in this Git repository. Create or check out it before dispatching.`);
    }
    const targetRows = runGit(['worktree', 'list', '--porcelain']).split(/\r?\n\r?\n/);
    const checkoutPath = targetRows.map((row) => ({ branch: row.match(/^branch (.+)$/m)?.[1], path: row.match(/^worktree (.+)$/m)?.[1] })).find((row) => row.branch === ref)?.path;
    deliveryTarget = { branch, ref, repositoryPath, checkoutPath };
    targetRef = ref;
  }
  const baseCommit = runGit(['rev-parse', '--verify', `${targetRef}^{commit}`]);
  if (deliveryTarget) deliveryTarget.baseCommit = baseCommit;
  return { commonDir, repositoryPath, targetRef, deliveryTarget, baseCommit };
}

export function resolveTemplateWorkspaceRoot(root: string, storageDir?: string): string {
  const commonDir = gitOutput(root, ['rev-parse', '--path-format=absolute', '--git-common-dir'], storageDir);
  return resolveMainWorkspaceRoot(root, commonDir, storageDir) || physicalDirectory(root);
}

/** Identify a linked checkout's main workspace without changing its delivery target. */
export function resolveMainWorkspaceRoot(startPath: string, commonDir: string, storageDir?: string): string | undefined {
  const checkout = physicalDirectory(gitOutput(startPath, ['rev-parse', '--show-toplevel'], storageDir));
  const records = gitOutput(startPath, ['worktree', 'list', '--porcelain', '-z'], storageDir).split('\0\0');
  const entries = records.map((record) => {
    const fields = record.split('\0');
    const row = fields.find((field) => field.startsWith('worktree '));
    return row ? { path: row.slice('worktree '.length), bare: fields.includes('bare') } : undefined;
  }).filter((entry): entry is { path: string; bare: boolean } => !!entry);
  if (!entries.length || entries[0].bare) throw new Error('Cannot identify the main checkout of this Git worktree.');
  const main = physicalDirectory(entries[0].path);
  if (!entries.some((entry) => {
    try { return physicalDirectory(entry.path) === checkout; } catch { return false; }
  })) throw new Error('Source checkout is not registered in the Git worktree list.');
  if (physicalDirectory(gitOutput(main, ['rev-parse', '--path-format=absolute', '--git-common-dir'], storageDir)) !== physicalDirectory(commonDir)) {
    throw new Error('Main checkout has a different Git common directory.');
  }
  return checkout === main ? undefined : main;
}

function readTaskTarget(repositoryPath: string, storageDir: string): TaskTargetMarker | undefined {
  // A task launched from another task's worktree inherits its recorded integration target.
  try {
    const directMarker = JSON.parse(fs.readFileSync(path.join(repositoryPath, '.agent-mesh-task.json'), 'utf8'));
    if (typeof directMarker.targetRef === 'string') return directMarker;
  } catch {}
  try {
    const pointer = path.join(repositoryPath, '.git');
    if (!fs.existsSync(pointer)) return undefined;
    const content = fs.readFileSync(pointer, 'utf8');
    const gitDir = content.match(/^gitdir:\s*(.+)$/m)?.[1];
    if (!gitDir) return undefined;
    const worktreeRoot = path.resolve(path.dirname(path.resolve(repositoryPath, gitDir)), '..');
    const marker = JSON.parse(fs.readFileSync(path.join(worktreeRoot, '.agent-mesh-task.json'), 'utf8'));
    return typeof marker.targetRef === 'string' ? marker : undefined;
  } catch {
    // Ledger lookup also covers linked worktrees whose metadata has not been copied into the checkout.
    try {
      const tasksDir = path.join(storageDir, 'threads');
      for (const name of fs.readdirSync(tasksDir)) {
        const thread = JSON.parse(fs.readFileSync(path.join(tasksDir, name), 'utf8'));
        if (thread?.worktree?.path && safePath(thread.worktree.path) === safePath(repositoryPath) && thread.worktree.targetRef) {
          return { targetRef: thread.worktree.targetRef, deliveryTarget: thread.worktree.deliveryTarget, repositoryPath: thread.worktree.repositoryPath || thread.worktree.mainPath, commonDir: thread.worktree.commonDir };
        }
      }
    } catch {}
    return undefined;
  }
}

export function createTaskWorktree(repo: GitRepository, threadId: string, storageDir: string, artifactDir?: string): WorktreeInfo {
  // Unified task directory: the worktree is a child of the task's own dir, so
  // logs/Archive and the checkout can never drift to different roots.
  if (!repo.deliveryTarget) throw new Error('MISSING_DELIVERY_TARGET: A task requires a real local delivery branch.');
  const pathRoot = path.resolve(artifactDir || taskArtifactDir(threadId, storageDir));
  const worktreePath = path.join(pathRoot, 'worktree');
  const taskBranch = `mesh/task/${threadId}`;
  fs.mkdirSync(pathRoot, { recursive: true });
  if (fs.existsSync(worktreePath)) throw new Error(`Task worktree already exists: ${worktreePath}`);
  gitOutput(repo.repositoryPath, ['worktree', 'add', '-b', taskBranch, worktreePath, repo.baseCommit], storageDir);
  try {
    const exclude = path.resolve(worktreePath, gitOutput(worktreePath, ['rev-parse', '--git-path', 'info/exclude'], storageDir));
    fs.mkdirSync(path.dirname(exclude), { recursive: true });
    fs.appendFileSync(exclude, '\n/.agent-mesh-task.json\n', 'utf8');
    const marker = { threadId, targetRef: repo.targetRef, deliveryTarget: repo.deliveryTarget, commonDir: repo.commonDir, repositoryPath: repo.repositoryPath };
    fs.writeFileSync(path.join(worktreePath, '.agent-mesh-task.json'), JSON.stringify(marker), 'utf8');
  }
  catch (error) {
    try { gitOutput(repo.repositoryPath, ['worktree', 'remove', '--force', worktreePath], storageDir); } catch {}
    try { gitOutput(repo.repositoryPath, ['branch', '-D', taskBranch], storageDir); } catch {}
    throw new Error(`Could not record isolated task worktree metadata: ${String(error)}`, { cause: error });
  }
  return {
    path: worktreePath,
    mainPath: repo.repositoryPath,
    repositoryPath: repo.repositoryPath,
    commonDir: repo.commonDir,
    targetRef: repo.targetRef,
    deliveryTarget: repo.deliveryTarget,
    branch: taskBranch,
    taskBranch,
    baseCommit: repo.baseCommit,
    headCommit: repo.baseCommit,
    mergePending: false,
  };
}

export function gitOutput(cwd: string, args: string[], storageDir?: string): string { return git(cwd, args, undefined, storageDir); }

export function currentHead(cwd: string): string { return git(cwd, ['rev-parse', '--verify', 'HEAD^{commit}']); }

/** Build an immutable candidate without changing the worker index or branch. */
export function snapshotTaskWorktree(worktree: WorktreeInfo): { commit: string; tree: string; sourceCommit: string } {
  const cwd = worktree.path;
  const sourceCommit = currentHead(cwd);
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-candidate-'));
  try {
    const indexPath = path.resolve(cwd, git(cwd, ['rev-parse', '--git-path', 'index']));
    const temporaryIndex = path.join(tempDir, 'index');
    const env = { ...process.env, GIT_INDEX_FILE: temporaryIndex };
    if (fs.existsSync(indexPath)) fs.copyFileSync(indexPath, temporaryIndex);
    else git(cwd, ['read-tree', sourceCommit], env);
    git(cwd, ['add', '-A', '--', '.'], env);
    const tree = git(cwd, ['write-tree'], env);
    const commit = git(cwd, ['-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit-tree', tree, '-p', sourceCommit, '-m', 'Approved task candidate'], env);
    return { commit, tree, sourceCommit };
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

export function changedFiles(cwd: string): string[] {
  return git(cwd, ['status', '--porcelain', '-z', '--untracked-files=all'])
    .split('\0').filter(Boolean).map((entry) => entry.slice(3)).filter((name) => name !== '.agent-mesh-task.json');
}

export function commitTaskChanges(worktree: WorktreeInfo, message: string): string {
  if (changedFiles(worktree.path).length) {
    git(worktree.path, ['add', '-A']);
    git(worktree.path, ['-c', 'user.name=Agent Mesh Bridge', '-c', 'user.email=agent-mesh-bridge@localhost', 'commit', '-m', message]);
  }
  return currentHead(worktree.path);
}

export function removeTaskWorktree(worktree: WorktreeInfo): void {
  const taskId = (worktree.taskBranch || '').match(/^mesh\/task\/([A-Za-z0-9_-]+)$/)?.[1];
  if (taskId) { try { git(worktree.mainPath, ['update-ref', '-d', `refs/mesh/candidates/${taskId}`]); } catch {} }
  if (taskId) { try { git(worktree.mainPath, ['update-ref', '-d', `refs/mesh/resolutions/${taskId}`]); } catch {} }
  try { git(worktree.mainPath, ['worktree', 'remove', '--force', worktree.path]); } catch {}
  try { git(worktree.mainPath, ['branch', '-D', worktree.taskBranch || worktree.branch || '']); } catch {}
}

export function defaultWorktreeBase(storageDir: string): string {
  return path.join(storageDir, 'worktrees');
}

export function gitExecutable(storageDir?: string): string {
  return gitCandidates(storageDir).find((candidate) => !path.isAbsolute(candidate) || fs.existsSync(candidate))!;
}
