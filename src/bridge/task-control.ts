import * as fs from 'fs';
import * as path from 'path';
import { AutonomousRunner } from './autonomous-runner';
import { CancellationRegistry } from './cancellation-registry';
import { TaskExecutionRegistry } from './task-execution-registry';
import {
  NativeSessionCleanupResult,
  cleanupThreadNativeSessions,
  collectThreadSessionRefs,
  describeNativeSessionCleanup,
} from './native-session-cleanup';
import { SessionOwnershipStore, nativeSessionNamespace } from './session-ownership';
import { TaskStore } from './task-store';
import { removeTaskArtifacts } from './task-summary';
import { removeTaskDataDir } from './task-data-dir';
import { removeTaskWorktree } from './git-worktree';
import { changedFiles, currentHead } from './git-worktree';
import { gitOutput } from './git-worktree';
import { MeshFileLock } from './merge-coordinator';
import { meshMergeLockPath } from './mesh-file-lock';
import { TaskRoleLock } from './task-role-lock';
import { ExecutionController } from './execution-controller';
import { getProcessBirthId, killPidTree } from './platform';
import { taskValidationDir } from './round-log';
import {
  DelegationRound,
  DelegationThread,
  TaskInterruption,
  appendInterruption,
  findInFlightRound,
  isMergePending,
  isMergeReview,
  isIsolatedTaskWorktree,
} from './types';

export interface TaskControlResult {
  threadId: string;
  status: 'PAUSED' | 'STOPPING' | 'DELETED';
  reason: string;
  message: string;
  /**
   * Receiver-side native conversations removed along with a deleted ledger
   * entry. Absent when the task had no resolvable native session.
   */
  nativeSessions?: NativeSessionCleanupResult;
}

/**
 * Pause/delete control plane shared by the VSCode commands and the MCP server.
 *
 * The method may run in a process that does not own the agent child. It always
 * persists the intended state first, then publishes a cancellation request that
 * the owning runner polls and acts on. The owning runner will additionally
 * finalize the round with the same interruption reason.
 */
export class TaskControlService {
  private readonly taskStore: TaskStore;
  private readonly runner?: AutonomousRunner;
  private readonly cancellationRegistry: CancellationRegistry;
  private readonly executionRegistry: TaskExecutionRegistry;
  private readonly storageDir: string;
  private readonly sessionCleanup: (thread: DelegationThread, options?: { onlyPinnedNamespace?: boolean }) => NativeSessionCleanupResult;
  private readonly mergeLock: MeshFileLock;

  constructor(
    taskStore: TaskStore,
    runner?: AutonomousRunner,
    cancellationRegistry?: CancellationRegistry,
    storageDir?: string,
    sessionCleanup?: (thread: DelegationThread, options?: { onlyPinnedNamespace?: boolean }) => NativeSessionCleanupResult,
    mergeLock?: MeshFileLock,
  ) {
    this.taskStore = taskStore;
    this.runner = runner;
    this.storageDir = storageDir || path.dirname(taskStore.getStoragePath());
    this.cancellationRegistry = cancellationRegistry || new CancellationRegistry(this.storageDir);
    this.executionRegistry = new TaskExecutionRegistry(this.storageDir);
    this.sessionCleanup = sessionCleanup || ((thread, options) => cleanupThreadNativeSessions(thread, options));
    this.mergeLock = mergeLock || new MeshFileLock(meshMergeLockPath(this.storageDir));
  }

  /**
   * Pause a queued/running task. The current round is marked
   * `paused` and keeps the original prompt so it can be resumed later.
   */
  public async pauseTask(threadId: string, reason: string, by: string = 'user'): Promise<TaskControlResult> {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (thread.status === 'completed') {
      throw new Error(`Cannot pause task ${threadId}: it is already completed.`);
    }
    if (thread.status === 'failed') {
      throw new Error(
        `Cannot pause task ${threadId}: it already failed. Use resume/retry instead.`,
      );
    }
    if (thread.status === 'reviewing' && !thread.execution && !isMergeReview(thread)) {
      throw new Error(
        `Cannot pause task ${threadId}: it is waiting for the caller's review, not executing.`,
      );
    }
    if (thread.status === 'wait_decision') {
      throw new Error(`Cannot pause task ${threadId} while it is ${thread.status}.`);
    }

    const normalizedReason = (reason || '').trim() || 'Paused by user';
    if (thread.execution?.version === 2) {
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
      const controller = ExecutionController.getInstance();
      if (!controller) {
        throw new Error(`Task ${threadId} v2 pause is pending: no execution controller can confirm the persisted run identity.`);
      }
      const settled = await controller.pauseExecution(threadId, normalizedReason, by);
      const latest = this.taskStore.reloadTask(threadId) || settled;
      const stopping = Boolean(latest.execution?.run);
      return {
        threadId,
        status: stopping ? 'STOPPING' : 'PAUSED',
        reason: normalizedReason,
        message: stopping
          ? `Task ${threadId} stop is pending confirmation. Its protected run remains recorded; resume and cleanup are blocked until the owner, complete child tree, and resources acknowledge exit.`
          : `Task ${threadId} paused. Interruption reason: ${normalizedReason}. The round is preserved and can be resumed later.`,
      };
    }
    if (thread.execution?.version === 1 && ExecutionController.getInstance()) {
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
      const settled = await ExecutionController.getInstance()!.pauseExecution(threadId, normalizedReason, by);
      return {
        threadId,
        status: settled.execution?.run ? 'STOPPING' : 'PAUSED',
        reason: normalizedReason,
        message: settled.execution?.run
          ? `Task ${threadId} stop is pending confirmation; the current process identity remains protected.`
          : `Task ${threadId} paused. Interruption reason: ${normalizedReason}. The round is preserved and can be resumed later.`,
      };
    }
    const now = new Date().toISOString();
    const interruption: TaskInterruption = {
      kind: 'paused',
      reason: normalizedReason,
      at: now,
      by,
    };

    const round = this.resolveCurrentRound(thread);
    const pausingMerge = thread.status === 'merging' || (thread.status === 'queued' && thread.queuePhase === 'merge') || isMergeReview(thread);
    if (thread.status === 'paused') {
      // Idempotent, but still publish cancellation and stop this task's child.
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
      // Registry records are observational; kill only this task's child. Its
      // compatibility reader also checks old execution.lock/leases records.
      this.executionRegistry.killOrphanChild(threadId, true);
      return {
        threadId,
        status: 'PAUSED',
        reason: thread.interruption?.reason || normalizedReason,
        message: `Task ${threadId} is already paused (${thread.interruption?.reason || normalizedReason}).`,
      };
    }

    thread.status = 'paused';
    thread.updatedAt = now;
    thread.summary.lastOutputSnippet = `Paused: ${normalizedReason}`;
    appendInterruption(thread, interruption);
    if (round && !pausingMerge) {
      round.status = 'paused';
      round.interruption = interruption;
    }

    this.taskStore.upsertTask(thread);

    // Publish the stop signal for whichever process owns the child.
    this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by);
    this.runner?.cancelThread(threadId, 'pause', normalizedReason, by);
    this.executionRegistry.killOrphanChild(threadId, true);
    if (pausingMerge) {
      const reviewerChild = new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid;
      if (reviewerChild && reviewerChild !== process.pid) { try { killPidTree(reviewerChild); } catch {} }
    }

    return {
      threadId,
      status: 'PAUSED',
      reason: normalizedReason,
      message:
        `Task ${threadId} paused. Interruption reason: ${normalizedReason}. ` +
        `The round is preserved and can be resumed later.`,
    };
  }

  /**
   * Permanently delete a task. Active children are stopped first; the ledger
   * entry is removed and a tombstone records the deletion reason.
   * Local integration, not MR metadata, decides whether unmerged task output
   * can be discarded without an explicit force.
   */
  public async deleteTask(
    threadId: string,
    reason: string,
    by: string = 'user',
    force: boolean = false,
  ): Promise<TaskControlResult> {
    this.taskStore.loadFromDisk();
    const thread = this.taskStore.getTask(threadId);
    if (!thread) {
      const resumed = await this.resumeTombstonedCleanup(threadId);
      if (resumed) return resumed;
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (thread.worktree?.integration?.stashRecoveryLockHeld) {
      throw new Error(`Cannot delete ${threadId} while stash metadata recovery is pending; retry the recovery first.`);
    }

    const hasUnmergedOutput = (candidate: DelegationThread): boolean => !!candidate.worktree && (() => {
      try {
        const worktree = candidate.worktree!;
        const head = currentHead(worktree.path);
        const dirty = changedFiles(worktree.path).length > 0;
        const phase = worktree.integration?.phase;
        if ((phase === 'merged' || phase === 'no_changes') && !dirty) {
          const expected = phase === 'merged' ? worktree.approvedCommit : worktree.baseCommit;
          if (expected && head === expected) return false;
          // A ref may have landed before its ledger write. The transaction is
          // trusted only when the target contains that exact candidate and the
          // task worktree still matches its approved source.
          const txPath = path.join(this.storageDir, 'merge-transactions', `${candidate.threadId}.json`);
          const tx = JSON.parse(fs.readFileSync(txPath, 'utf8'));
          if (!worktree.deliveryTarget) throw new Error('Historical delivery state cannot prove integration.');
          const repo = worktree.deliveryTarget.repositoryPath;
          const target = gitOutput(repo, ['rev-parse', '--verify', `${worktree.deliveryTarget.ref}^{commit}`]);
          const integrated = tx.candidateCommit && (target === tx.candidateCommit || gitOutput(repo, ['merge-base', '--is-ancestor', tx.candidateCommit, target]) === '');
          if (integrated && head === tx.sourceCommit) return false;
        }
        return dirty || (!!worktree.baseCommit && head !== worktree.baseCommit) ||
          !!worktree.approvedCommit || isMergePending(thread);
      } catch { return true; }
    })();
    const worktreeHasUnmergedOutput = hasUnmergedOutput(thread);
    if (worktreeHasUnmergedOutput && !force) {
      throw new Error(
        `Refusing to delete task ${threadId}: its approved work has not been integrated locally. ` +
          `Wait for integration or pass force: true to discard the unmerged worktree output.`,
      );
    }

    const isV2 = thread.execution?.version === 2;
    const cleanupReservation = isV2
      ? await this.taskStore.markTaskCleanup(threadId, 'delete', {
          pid: process.pid,
          birthId: getProcessBirthId(process.pid),
        }, { force })
      : undefined;

    const normalizedReason = (reason || '').trim() || 'Deleted by user';
    const active = isV2
      ? !!thread.execution?.run
      : (
      thread.status === 'running' ||
      thread.status === 'queued' ||
      thread.status === 'paused' ||
      thread.status === 'merging' ||
      !!thread.execution?.run ||
      !!findInFlightRound(thread) ||
      !!new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid ||
      this.executionRegistry.listOwners().some((owner) => owner.threadId === threadId && !!owner.childPid));

    if (active) {
      this.cancellationRegistry.request(threadId, 'delete', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'delete', normalizedReason, by);
      if (!isV2) {
        this.executionRegistry.killOrphanChild(threadId, true);
        const reviewerChild = new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid;
        if (reviewerChild) { try { killPidTree(reviewerChild); } catch {} }
      } else {
        const controller = ExecutionController.getInstance();
        if (!controller) throw new Error(`Cannot delete ${threadId}: v2 execution controller is unavailable; cleanup reservation retained.`);
        await controller.pauseExecution(threadId, `Deleting: ${normalizedReason}`, by, cleanupReservation?.token);
      }
      thread.status = 'paused';
      thread.updatedAt = new Date().toISOString();
      appendInterruption(thread, { kind: 'deleted', reason: normalizedReason, at: thread.updatedAt, by });
      thread.summary.lastOutputSnippet = `Deleting: ${normalizedReason}`;
      try {
        if (!cleanupReservation) this.taskStore.upsertTask(thread);
      }
      catch (error) { throw new Error(`Could not persist deletion intent for ${threadId}: ${(error as Error).message}`, { cause: error }); }
      if (!isV2) await this.waitForThreadChildren(threadId);
      else {
        const latest = this.taskStore.reloadTask(threadId);
        if (latest?.execution?.run) {
          throw new Error(`Cannot delete ${threadId}: v2 run ${latest.execution.run.id} has not confirmed complete process-tree and resource exit; cleanup reservation was retained for retry.`);
        }
      }
    }

    if (cleanupReservation) {
      const settled = this.taskStore.reloadTask(threadId);
      if (settled?.execution?.run) {
        throw new Error(`Cannot delete ${threadId}: v2 run ${settled.execution.run.id} has not durably settled; cleanup reservation was retained for retry.`);
      }
    }

    // Serialize final cleanup with ref updates and their durable ledger write.
    let resolutionsToRemove: string[] = [];
    if (isIsolatedTaskWorktree(thread.worktree)) {
      const release = await this.mergeLock.acquire();
      if (!release) throw new Error(`Deletion of ${threadId} was cancelled while waiting for integration to stop.`);
      try {
        this.taskStore.loadFromDisk();
        const latest = this.taskStore.getTask(threadId);
        if (latest?.worktree?.integration?.stashRecoveryLockHeld) throw new Error(`Cannot delete ${threadId} while a stash recovery lock is held; retry the recovery first.`);
        if (latest?.status === 'completed' && latest.worktree?.integration?.phase === 'merged') {
          thread.status = latest.status;
          thread.worktree = latest.worktree;
        }
        if (latest && hasUnmergedOutput(latest) && !force) {
          throw new Error(`Refusing to delete task ${threadId}: its approved work has not been integrated locally. Wait for integration or pass force: true to discard the unmerged worktree output.`);
        }
        resolutionsToRemove = [...new Set([...(latest?.worktree?.supersededMergeResolutionPaths || []), latest?.worktree?.mergeResolutionPath].filter((entry): entry is string => !!entry))];
        for (const resolution of resolutionsToRemove) {
          const resolved = path.resolve(resolution);
          const roots = [path.resolve(this.storageDir, 'merge-worktrees')];
          if (latest?.artifactDir) roots.push(path.resolve(taskValidationDir(latest.artifactDir)));
          if (!roots.includes(path.dirname(resolved)) || !path.basename(resolved).startsWith(`${threadId}-`)) {
            throw new Error(`Refusing to remove an unexpected merge resolution path: ${resolved}`);
          }
          const repo = latest!.worktree!.repositoryPath || latest!.worktree!.mainPath;
          const normalized = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
          const registered = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
            .some((line) => line.startsWith('worktree ') && normalized(line.slice(9)) === normalized(resolved));
          // Validate while the task still exists; physical cleanup follows the
          // durable tombstone so a crash can never leave an untracked task.
          void registered;
        }
        if (cleanupReservation) {
          await this.taskStore.commitTaskCleanup(threadId, cleanupReservation.token, 'delete', {
            tombstone: { reason: normalizedReason, by, forced: force },
            cleanupManifest: this.cleanupManifest(thread, cleanupReservation.token, normalizedReason, by, force),
          });
        } else {
          this.appendTombstone(thread, normalizedReason, by, undefined, force);
          this.taskStore.deleteTask(threadId);
        }
      } finally { release(); }
    } else {
      if (cleanupReservation) {
        await this.taskStore.commitTaskCleanup(threadId, cleanupReservation.token, 'delete', {
          tombstone: { reason: normalizedReason, by, forced: force },
          cleanupManifest: this.cleanupManifest(thread, cleanupReservation.token, normalizedReason, by, force),
        });
      } else {
        this.appendTombstone(thread, normalizedReason, by, undefined, force);
        this.taskStore.deleteTask(threadId);
      }
    }

    const cleanupOwner = { pid: process.pid, birthId: getProcessBirthId(process.pid) };
    if (cleanupReservation) {
      const claim = await this.taskStore.claimTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner);
      if (claim === 'busy') throw new Error(`Deletion cleanup for ${threadId} is already owned by a live cleaner.`);
      if (claim === 'complete') return { threadId, status: 'DELETED', reason: normalizedReason, message: `Task ${threadId} was already deleted and its cleanup is complete.` };
      if (claim !== 'claimed') throw new Error(`Deletion cleanup for ${threadId} has no durable tombstone manifest.`);
    }

    let nativeSessions: NativeSessionCleanupResult | undefined;
    try {
      // Physical cleanup is single-consumer after the durable tombstone claim.
      this.removeMergeResolutions(thread, resolutionsToRemove);
      nativeSessions = cleanupReservation
        ? this.cleanupVerifiedV2NativeSessions(thread)
        : this.cleanupNativeSessions(thread);
      removeTaskArtifacts(threadId, this.storageDir, {
        keepArchive: false,
        targetAgent: thread.targetAgent,
        initiatorAgent: thread.initiatorAgent,
        artifactDir: thread.artifactDir,
      });
      if (thread.worktree) removeTaskWorktree(thread.worktree);
      if (thread.artifactDir) removeTaskDataDir(thread.artifactDir, true);
      if (cleanupReservation) {
        if (!await this.taskStore.finishTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner, true, nativeSessions)) {
          throw new Error(`Could not persist completion for deletion cleanup ${threadId}/${cleanupReservation.token}.`);
        }
      } else {
        this.appendTombstone(thread, normalizedReason, by, nativeSessions, force);
      }
    } catch (error) {
      if (cleanupReservation) await this.taskStore.finishTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner, false, nativeSessions).catch(() => false);
      throw error;
    }

    const cleanupNote = describeNativeSessionCleanup(nativeSessions);
    return {
      threadId,
      status: 'DELETED',
      reason: normalizedReason,
      message:
        `Task ${threadId} deleted. Reason: ${normalizedReason}.` +
        (cleanupNote ? ` Cleaned: ${cleanupNote}.` : ''),
      nativeSessions,
    };
  }

  private async waitForThreadChildren(threadId: string): Promise<void> {
    const roles = new TaskRoleLock(this.storageDir);
    const deadline = Date.now() + 30_000;
    while (Date.now() < deadline) {
      this.executionRegistry.killOrphanChild(threadId, true);
      const executionLive = this.executionRegistry.listOwners().some((owner) => owner.threadId === threadId);
      const roleChild = roles.getOwner(threadId)?.childPid;
      let roleLive = false;
      if (roleChild) {
        try { process.kill(roleChild, 0); roleLive = true; } catch (error) { roleLive = (error as NodeJS.ErrnoException).code !== 'ESRCH'; }
      }
      if (!executionLive && !roleLive) return;
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    throw new Error(`Timed out waiting for task ${threadId} processes to exit; its worktree and ledger were retained.`);
  }

  /** Run the injected cleaner, swallowing anything it might still throw. */
  private cleanupNativeSessions(thread: DelegationThread, options?: { onlyPinnedNamespace?: boolean }): NativeSessionCleanupResult | undefined {
    try {
      const result = this.sessionCleanup(thread, options);
      return result.entries.length > 0 ? result : undefined;
    } catch (e: any) {
      console.warn('[TaskControlService] Native session cleanup failed:', e?.message || e);
      return undefined;
    }
  }

  private cleanupVerifiedV2NativeSessions(thread: DelegationThread): NativeSessionCleanupResult {
    const ownership = new SessionOwnershipStore(this.storageDir);
    const safe = { ...thread, receiverSessionId: '', reviewerSessionId: '', retargets: [] as DelegationThread['retargets'] };
    const retained: NativeSessionCleanupResult['entries'] = [];
    const verified: Array<{ agent: DelegationThread['targetAgent']; sessionId: string; role: 'developer' | 'reviewer'; namespace: string; sessionEpoch: number }> = [];
    for (const ref of collectThreadSessionRefs(thread)) {
      const reviewer = ref.agent === thread.initiatorAgent && ref.sessionId === thread.reviewerSessionId;
      const role = reviewer ? 'reviewer' : 'developer';
      const captured = reviewer ? thread.reviewerSessionCaptured === true : thread.receiverSessionCaptured === true;
      const assignment = thread.nativeSessionAssignments?.[role];
      const namespaceMatches = !!assignment?.namespace && assignment.namespace === nativeSessionNamespace(ref.agent);
      const result = captured && assignment && namespaceMatches
        ? ownership.verify({ agent: ref.agent, namespace: assignment.namespace, sessionId: ref.sessionId, threadId: thread.threadId, role, sessionEpoch: assignment.sessionEpoch })
        : { ok: false as const, code: 'SESSION_OWNERSHIP_UNVERIFIED' as const, reason: !captured ? 'Session id was not captured from the native CLI.' : !assignment ? 'Pinned native namespace/epoch is missing.' : 'Pinned namespace differs from the current CLI storage root.' };
      if (result.ok) {
        verified.push({ agent: ref.agent, sessionId: ref.sessionId, role, namespace: assignment!.namespace, sessionEpoch: assignment!.sessionEpoch });
        if (role === 'reviewer') safe.reviewerSessionId = ref.sessionId;
        else safe.receiverSessionId = ref.sessionId;
      } else {
        retained.push({ agent: ref.agent, sessionId: ref.sessionId, found: false, removed: [], pruned: [], error: `${result.code}: ${result.reason}; native assets retained.` });
      }
    }
    const cleaned = this.cleanupNativeSessions(safe, { onlyPinnedNamespace: true });
    const entries = cleaned?.entries || [];
    for (const ref of verified) {
      const outcome = entries.find((entry) => entry.agent === ref.agent && entry.sessionId === ref.sessionId);
      const blockingCleanupError = outcome?.error?.split('; ').some((message) => !message.startsWith('Shared native index retained without an app-safe update API:'));
      if (blockingCleanupError) continue;
      const revoked = ownership.revoke(ref.agent, ref.namespace, ref.sessionId, {
        threadId: thread.threadId, role: ref.role, sessionEpoch: ref.sessionEpoch,
      });
      if (!revoked.ok) {
        const entry = outcome || { agent: ref.agent, sessionId: ref.sessionId, found: false, removed: [], pruned: [] };
        entry.error = `${revoked.code}: ownership revocation failed: ${revoked.reason}`;
        if (!outcome) entries.push(entry);
      }
    }
    return { entries: [...retained, ...entries], removedCount: cleaned?.removedCount || 0 };
  }

  private removeMergeResolutions(thread: DelegationThread, resolutions: string[]): void {
    if (!resolutions.length || !thread.worktree) return;
    const repo = thread.worktree.repositoryPath || thread.worktree.mainPath;
    const normalized = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
    for (const resolution of resolutions) {
      const resolved = path.resolve(resolution);
      const registered = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
        .some((line) => line.startsWith('worktree ') && normalized(line.slice(9)) === normalized(resolved));
      if (registered) gitOutput(repo, ['worktree', 'remove', '--force', resolved]);
      if (fs.existsSync(resolved)) throw new Error(`Could not remove merge resolution workspace ${resolved}.`);
    }
  }

  private resolveCurrentRound(thread: DelegationThread): DelegationRound | undefined {
    const inFlight = findInFlightRound(thread);
    if (inFlight) return inFlight;
    return thread.rounds && thread.rounds.length > 0
      ? thread.rounds[thread.rounds.length - 1]
      : undefined;
  }

  private appendTombstone(
    thread: DelegationThread,
    reason: string,
    by: string,
    nativeSessions?: NativeSessionCleanupResult,
    forced: boolean = false,
    cleanupToken?: string,
    cleanupComplete?: boolean,
    cleanupManifest?: Record<string, unknown>,
  ): void {
    try {
      fs.mkdirSync(this.storageDir, { recursive: true });
      const record = {
        threadId: thread.threadId,
        title: thread.title,
        initiatorAgent: thread.initiatorAgent,
        targetAgent: thread.targetAgent,
        statusAtDeletion: thread.status,
        reason,
        by,
        // A forced deletion bypassed the merge gate: the audit trail must show
        // that the worktree output was discarded deliberately, not by accident.
        ...(forced ? { forced: true } : {}),
        ...(cleanupToken ? { cleanupToken } : {}),
        ...(cleanupToken && cleanupComplete ? { cleanupComplete: true } : {}),
        ...(cleanupManifest ? { cleanupManifest } : {}),
        at: new Date().toISOString(),
        nativeSessions: nativeSessions
          ? {
              removedFiles: nativeSessions.removedCount,
              sessions: nativeSessions.entries.map((entry) => ({
                agent: entry.agent,
                sessionId: entry.sessionId,
                found: entry.found,
                removed: entry.removed,
                pruned: entry.pruned,
                ...(entry.error ? { error: entry.error } : {}),
              })),
            }
          : undefined,
      };
      this.taskStore.appendTombstone(record);
    } catch (error) {
      throw new Error(`Could not persist deletion tombstone for ${thread.threadId}: ${(error as Error)?.message || error}`, { cause: error });
    }
  }

  private cleanupManifest(thread: DelegationThread, token: string, reason: string, by: string, forced: boolean): Record<string, unknown> {
    const { threadId, targetAgent, initiatorAgent, reviewFlow, receiverSessionId, receiverSessionCaptured,
      reviewerSessionId, reviewerSessionCaptured, nativeSessionAssignments, artifactDir, worktree } = thread;
    return {
      token,
      owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
      stage: 'pending',
      reason,
      by,
      forced,
      thread: { threadId, targetAgent, initiatorAgent, reviewFlow, receiverSessionId, receiverSessionCaptured,
        reviewerSessionId, reviewerSessionCaptured, nativeSessionAssignments, artifactDir, worktree },
    };
  }

  private async resumeTombstonedCleanup(threadId: string): Promise<TaskControlResult | undefined> {
    const file = path.join(this.storageDir, 'deleted_tasks.log');
    if (!fs.existsSync(file)) return undefined;
    let record: any;
    try {
      record = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line))
        .filter((entry) => entry.threadId === threadId && entry.cleanupToken).slice(-1)[0];
    } catch { return undefined; }
    const manifest = record?.cleanupManifest;
    if (manifest?.stage === 'complete') {
      return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was already deleted and its cleanup is complete.` };
    }
    if (!manifest || manifest.stage !== 'pending') return undefined;
    const cleanupOwner = { pid: process.pid, birthId: getProcessBirthId(process.pid) };
    if (!cleanupOwner.birthId) throw new Error(`Deletion cleanup for ${threadId} is blocked: cleaner identity cannot be verified.`);
    const claim = await this.taskStore.claimTombstoneCleanup(threadId, manifest.token, cleanupOwner);
    if (claim === 'busy') throw new Error(`Deletion cleanup for ${threadId} is blocked by a live cleaner or unknown owner.`);
    if (claim === 'complete') return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was already deleted and its cleanup is complete.` };
    if (claim !== 'claimed') throw new Error(`Deletion cleanup manifest for ${threadId} is missing or damaged.`);
    const thread = manifest.thread as DelegationThread;
    if (!thread || thread.threadId !== threadId) throw new Error(`Deletion cleanup manifest for ${threadId} is damaged.`);
    try {
      const nativeSessions = this.cleanupVerifiedV2NativeSessions(thread);
      const resolutions = [...new Set([...(thread.worktree?.supersededMergeResolutionPaths || []), thread.worktree?.mergeResolutionPath].filter((entry): entry is string => !!entry))];
      this.removeMergeResolutions(thread, resolutions);
      removeTaskArtifacts(threadId, this.storageDir, { keepArchive: false, targetAgent: thread.targetAgent,
        initiatorAgent: thread.initiatorAgent, artifactDir: thread.artifactDir });
      if (thread.worktree) removeTaskWorktree(thread.worktree);
      if (thread.artifactDir) removeTaskDataDir(thread.artifactDir, true);
      if (!await this.taskStore.finishTombstoneCleanup(threadId, manifest.token, cleanupOwner, true, nativeSessions)) {
        throw new Error(`Could not persist completion for deletion cleanup ${threadId}/${manifest.token}.`);
      }
      return { threadId, status: 'DELETED', reason: manifest.reason || 'Deleted by user',
        message: `Task ${threadId} deletion cleanup resumed from its durable tombstone. ${describeNativeSessionCleanup(nativeSessions)}`,
        nativeSessions };
    } catch (error) {
      await this.taskStore.finishTombstoneCleanup(threadId, manifest.token, cleanupOwner, false).catch(() => false);
      throw error;
    }
  }
}
