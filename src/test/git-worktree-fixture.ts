import { testWorkspace } from './workspace-fixture';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';
import { DelegationThread, WorktreeInfo } from '../bridge/types';

export const gitExe = process.env.MESH_GIT_PATH || (process.platform === 'win32' ? 'D:\\Git\\cmd\\git.exe' : 'git');
process.env.MESH_GIT_PATH = gitExe;
const isolatedHome = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-merge-home-'));
process.env.USERPROFILE = isolatedHome;
process.env.HOME = isolatedHome;
const repositoryTemplate = path.join(isolatedHome, 'repository-template');
let templateReady = false;
const stashTemplate = path.join(isolatedHome, 'stash-template');
let preparedStashes: { old: string; extra: string } | undefined;

export function git(cwd: string, ...args: string[]): string {
  const result = spawnSync(gitExe, args, { cwd, encoding: 'utf8', windowsHide: true });
  if (result.status !== 0) throw new Error(result.stderr || `git ${args.join(' ')} failed`);
  return result.stdout.trim();
}

/**
 * Compare two paths that are known to exist using their filesystem real path.
 *
 * Git returns the canonical on-disk case on Windows; path.resolve() does not
 * canonicalize existing directory case, so a TEMP env like `C:\Windows\TEMP`
 * would not compare equal to the real `C:\Windows\Temp`. realpathSync.native
 * keeps POSIX comparisons case-sensitive while normalizing Windows casing.
 */
export function assertSameExistingPath(actual: string, expected: string): void {
  assert.equal(fs.realpathSync.native(actual), fs.realpathSync.native(expected));
}

export function initRepo(root: string): void {
  if (!templateReady) {
    createRepositoryTemplate();
    templateReady = true;
  }
  // Copy ordinary files, including .git, so refs, config and objects stay independent.
  fs.cpSync(repositoryTemplate, root, { recursive: true });
}

export function initMultiStashRepo(root: string): { old: string; extra: string } {
  if (!preparedStashes) {
    initRepo(stashTemplate);
    fs.writeFileSync(path.join(stashTemplate, 'old.txt'), 'old');
    git(stashTemplate, 'stash', 'push', '-u', '-m', 'old');
    const old = git(stashTemplate, 'rev-parse', 'refs/stash');
    fs.writeFileSync(path.join(stashTemplate, 'extra.txt'), 'extra');
    git(stashTemplate, 'stash', 'push', '-u', '-m', 'extra');
    const extra = git(stashTemplate, 'rev-parse', 'refs/stash');
    git(stashTemplate, 'stash', 'drop');
    preparedStashes = { old, extra };
  }
  fs.cpSync(stashTemplate, root, { recursive: true });
  return { ...preparedStashes };
}

function createRepositoryTemplate(): void {
  const root = repositoryTemplate;
  fs.mkdirSync(root, { recursive: true });
  git(root, 'init', '-b', 'feature/caller');
  git(root, 'config', 'user.name', 'Mesh Test');
  git(root, 'config', 'user.email', 'mesh-test@example.test');
  fs.writeFileSync(path.join(root, 'shared.txt'), 'alpha\nmiddle\nbeta\n', 'utf8');
  fs.writeFileSync(path.join(root, 'package.json'), JSON.stringify({
    scripts: { compile: 'node validate.js compile', test: 'node validate.js test' },
  }), 'utf8');
  fs.writeFileSync(path.join(root, 'validate.js'), `const fs=require('fs'),path=require('path');\nconst base=process.env.MESH_VALIDATION_SIGNAL_DIR;const id=process.cwd().includes('thread_merge_A')?'thread_merge_A':process.cwd().includes('thread_merge_B')?'thread_merge_B':'';\nif(id){fs.writeFileSync(path.join(base,id+'.entered'),'yes');if(process.env.MESH_VALIDATION_HOLD_THREAD===id)while(!fs.existsSync(path.join(base,id+'.release')))Atomics.wait(new Int32Array(new SharedArrayBuffer(4)),0,0,20);}\nif(process.env.MESH_VALIDATION_FAIL==='1')process.exit(6);\nif(process.argv[2]==='test'){const text=fs.readFileSync(path.join(process.cwd(),'shared.txt'),'utf8');if(!text.includes('A')&&!text.includes('B'))process.exit(2);}\n`, 'utf8');
  git(root, 'add', '-A');
  git(root, 'commit', '-m', 'base');
}

export function thread(threadId: string, worktree: WorktreeInfo): DelegationThread {
  const now = new Date().toISOString();
  return {
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (threadId)),
    callerSessionId: ('reviewer-' + (threadId)),
    threadId, title: threadId, initiatorAgent: 'codex', targetAgent: 'claude_code',
    status: 'queued', queuePhase: 'merge', currentRound: 1, createdAt: now, updatedAt: now,
    artifactDir: path.dirname(worktree.path),
    receiverSessionId: `session-${threadId}`, workspace: testWorkspace(worktree.path),
    summary: { lastPromptSnippet: 'review', lastOutputSnippet: 'approved', totalDurationMs: 1 },
    rounds: [{ round: 1, type: 'initial', prompt: 'change one line', output: 'done', durationMs: 1, status: 'completed', timestamp: now }],
    worktree: { ...worktree, approvedCommit: worktree.headCommit, integration: { phase: 'waiting', sourceCommit: worktree.headCommit, updatedAt: now } },
  };
}

export async function waitFor(file: string, attempts = 300): Promise<void> {
  for (let n = 0; n < attempts; n++) {
    if (fs.existsSync(file)) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${file}`);
}

export async function waitForValidationStart(
  signal: string,
  merge: Promise<{ status: string; message: string }>,
  timeoutMs = 60_000,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  let settled: Error | undefined;
  void merge.then(
    result => { settled = new Error(`Merge finished before validation started (${result.status}): ${result.message}`); },
    error => { settled = new Error('Merge rejected before validation started', { cause: error }); },
  );
  while (!fs.existsSync(signal)) {
    if (settled) throw settled;
    if (performance.now() >= deadline) throw new Error(`Timed out after ${timeoutMs} ms waiting for validation start: ${signal}`);
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

export async function waitForExit(child: ReturnType<typeof spawn>): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  await new Promise<void>((resolve) => child.once('exit', () => resolve()));
}

export async function waitForProcessExit(pid: number, attempts = 300): Promise<void> {
  for (let n = 0; n < attempts; n++) {
    try { process.kill(pid, 0); } catch { return; }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for helper process ${pid} to exit`);
}
