import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { WorktreeInfo } from './types';

export interface GitRepository {
  commonDir: string;
  repositoryPath: string;
  targetRef: string;
  deliveryTarget?: { branch: string; ref: string; repositoryPath: string };
  baseCommit: string;
}

interface TaskTargetMarker { targetRef: string; deliveryTarget?: { branch: string; ref: string; repositoryPath: string; checkoutPath?: string; baseCommit?: string }; commonDir?: string; repositoryPath?: string; }

function git(cwd: string, args: string[]): string {
  const candidates = [process.env.MESH_GIT_PATH, process.env.GIT_EXECUTABLE, 'git.exe', 'git'].filter(Boolean) as string[];
  let result: ReturnType<typeof spawnSync> | undefined;
  for (const executable of candidates) {
    result = spawnSync(executable, args, { cwd, encoding: 'utf8', windowsHide: true, timeout: 30_000 });
    if (!result.error || (result.error as NodeJS.ErrnoException).code !== 'ENOENT') break;
  }
  if (!result || result.error || result.status !== 0) {
    const detail = String(result?.stderr || '').trim() || result?.error?.message || `exit ${result?.status}`;
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
  let initialCommonDir: string;
  try { initialCommonDir = path.resolve(git(repositoryPath, ['rev-parse', '--path-format=absolute', '--git-common-dir'])); }
  catch (error) { throw new Error(`Workspace ${repositoryPath} is not a usable Git checkout. Open a Git repository with a committed HEAD before dispatching.`, { cause: error }); }
  const inherited = readTaskTarget(repositoryPath, storageDir);
  if (inherited?.repositoryPath) repositoryPath = path.resolve(inherited.repositoryPath);
  const commonDir = path.resolve(git(repositoryPath, ['rev-parse', '--path-format=absolute', '--git-common-dir']));
  if (safePath(initialCommonDir) !== safePath(commonDir)) throw new Error('Task worktree points to a different Git repository identity than its recorded source repository.');
  try { git(repositoryPath, ['rev-parse', '--verify', 'HEAD^{commit}']); }
  catch (error) { throw new Error(`Git checkout ${repositoryPath} has no valid committed HEAD. Create a commit before dispatching.`, { cause: error }); }
  let targetRef: string;
  let deliveryTarget = inherited?.deliveryTarget;
  if (inherited) targetRef = inherited.targetRef;
  else {
    let sourceRef = '';
    try { sourceRef = git(repositoryPath, ['symbolic-ref', '--quiet', '--short', 'HEAD']); } catch {}
    const branch = (requestedTargetBranch || sourceRef).trim().replace(/^refs\/heads\//, '');
    if (!branch || (!requestedTargetBranch && branch.startsWith('mesh/task/'))) throw new Error('Cannot choose a delivery target from a detached HEAD or mesh task branch. Check out a normal local branch or specify targetBranch.');
    try { git(repositoryPath, ['check-ref-format', '--branch', branch]); }
    catch { throw new Error(`Invalid local target branch "${branch}". Specify an existing local branch in this repository.`); }
    const ref = `refs/heads/${branch}`;
    try { git(repositoryPath, ['rev-parse', '--verify', `${ref}^{commit}`]); }
    catch { throw new Error(`Local target branch "${branch}" does not exist in this Git repository. Create or check out it before dispatching.`); }
    const targetRows = git(repositoryPath, ['worktree', 'list', '--porcelain']).split(/\r?\n\r?\n/);
    const checkoutPath = targetRows.map((row) => ({ branch: row.match(/^branch (.+)$/m)?.[1], path: row.match(/^worktree (.+)$/m)?.[1] })).find((row) => row.branch === ref)?.path;
    deliveryTarget = { branch, ref, repositoryPath, checkoutPath };
    targetRef = ref;
  }
  const baseCommit = git(repositoryPath, ['rev-parse', '--verify', `${targetRef}^{commit}`]);
  if (deliveryTarget) deliveryTarget.baseCommit = baseCommit;
  return { commonDir, repositoryPath, targetRef, deliveryTarget, baseCommit };
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

export function createTaskWorktree(repo: GitRepository, threadId: string, storageDir: string): WorktreeInfo {
  const pathRoot = path.join(storageDir, 'worktrees');
  const worktreePath = path.join(pathRoot, threadId);
  const taskBranch = `mesh/task/${threadId}`;
  fs.mkdirSync(pathRoot, { recursive: true });
  if (fs.existsSync(worktreePath)) throw new Error(`Task worktree already exists: ${worktreePath}`);
  git(repo.repositoryPath, ['worktree', 'add', '-b', taskBranch, worktreePath, repo.baseCommit]);
  try {
    const exclude = path.resolve(worktreePath, git(worktreePath, ['rev-parse', '--git-path', 'info/exclude']));
    fs.mkdirSync(path.dirname(exclude), { recursive: true });
    fs.appendFileSync(exclude, '\n/.agent-mesh-task.json\n', 'utf8');
    const marker = { threadId, targetRef: repo.targetRef, deliveryTarget: repo.deliveryTarget, commonDir: repo.commonDir, repositoryPath: repo.repositoryPath };
    fs.writeFileSync(path.join(worktreePath, '.agent-mesh-task.json'), JSON.stringify(marker), 'utf8');
  }
  catch (error) {
    try { git(repo.repositoryPath, ['worktree', 'remove', '--force', worktreePath]); } catch {}
    try { git(repo.repositoryPath, ['branch', '-D', taskBranch]); } catch {}
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

export function gitOutput(cwd: string, args: string[]): string { return git(cwd, args); }

export function currentHead(cwd: string): string { return git(cwd, ['rev-parse', '--verify', 'HEAD^{commit}']); }

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
  try { git(worktree.mainPath, ['worktree', 'remove', '--force', worktree.path]); } catch {}
  try { git(worktree.mainPath, ['branch', '-D', worktree.taskBranch || worktree.branch || '']); } catch {}
}

export function defaultWorktreeBase(storageDir: string): string {
  return path.join(storageDir, 'worktrees');
}

export function gitExecutable(): string {
  return process.env.MESH_GIT_PATH || process.env.GIT_EXECUTABLE || (os.platform() === 'win32' ? 'git.exe' : 'git');
}
