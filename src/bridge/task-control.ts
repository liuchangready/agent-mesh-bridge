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
import { probeProcessIdentity } from './platform';
import { assessRunLaunchEvidence } from './process-binding';
import { windowsJobFinished } from './windows-job';
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

    const v2Execution = thread.execution?.version === 2 ? thread.execution : undefined;
    const hasPendingV2Work = !!(v2Execution?.run || v2Execution?.next || v2Execution?.queue);
    if (thread.status === 'paused' && v2Execution?.run?.state === 'stopping' && !v2Execution.next && !v2Execution.queue) {
      return { threadId, status: 'STOPPING', reason: thread.interruption?.reason || reason,
        message: `Task ${threadId} is already paused for scheduling; run ${v2Execution.run.id} remains protected until exit is confirmed.` };
    }
    if (thread.status === 'paused' && !hasPendingV2Work) {
      return { threadId, status: 'PAUSED', reason: thread.interruption?.reason || reason,
        message: `Task ${threadId} is already paused.` };
    }
    // Terminal and decision states may still own a stopping run or a durable
    // handoff. Let the controller serialize those cases; reject only inert work.
    if ((thread.status === 'completed' || thread.status === 'failed' || thread.status === 'wait_decision' ||
         (thread.status === 'reviewing' && !thread.execution && !isMergeReview(thread))) &&
        !hasPendingV2Work && !thread.execution?.run) {
      throw new Error(`Cannot pause task ${threadId}: there is no active run or pending handoff to pause.`);
    }

    const normalizedReason = (reason || '').trim() || 'Paused by user';
    if (thread.execution?.version === 2) {
      const controller = ExecutionController.getInstance();
      const expectedExecution = { generation: thread.execution.generation, operationId: thread.execution.operationId };
      if (!controller) {
        let settled: DelegationThread | undefined;
        let transitionError: unknown;
        for (let attempt = 0; attempt < 3 && !settled; attempt++) {
          const latest = this.taskStore.reloadTask(threadId);
          if (!latest?.execution || latest.execution.version !== 2) throw new Error(`Task ${threadId} execution changed while pausing.`);
          if (latest.execution.generation !== expectedExecution.generation || latest.execution.operationId !== expectedExecution.operationId) {
            throw new Error(`Task ${threadId} execution generation changed while pausing; no cancellation was sent.`);
          }
          try {
            settled = await this.taskStore.transitionExecution(threadId, latest.execution.revision, (task) => {
              const execution = task.execution;
              if (execution?.version !== 2 || execution.generation !== expectedExecution.generation ||
                  execution.operationId !== expectedExecution.operationId) throw new Error('PAUSE_GENERATION_CHANGED');
              appendInterruption(task, { kind: 'paused', reason: normalizedReason, at: new Date().toISOString(), by });
              execution.next = undefined;
              execution.queue = undefined;
              if (execution.run) execution.run.state = 'stopping';
              task.status = 'paused';
              const round = this.resolveCurrentRound(task);
              if (round) { round.status = 'paused'; round.interruption = task.interruption; }
            });
          } catch (error) {
            transitionError = error;
            const message = String((error as Error)?.message || error);
            if (/revision mismatch/i.test(message) && attempt < 2) continue;
            if (/PAUSE_GENERATION_CHANGED/.test(message)) throw new Error(`Task ${threadId} execution generation changed while pausing; no cancellation was sent.`);
            break;
          }
        }
        const verified = this.taskStore.reloadTask(threadId);
        this.assertPauseCommitted(threadId, verified, expectedExecution, settled, transitionError);
        const runIdentity = verified.execution?.run ? { runId: verified.execution.run.id, ...expectedExecution } : undefined;
        this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by, runIdentity);
        this.runner?.cancelThread(threadId, 'pause', normalizedReason, by, runIdentity);
        return {
          threadId, status: verified.execution?.run ? 'STOPPING' : 'PAUSED', reason: normalizedReason,
          message: verified.execution?.run
            ? `Task ${threadId} is paused for scheduling. Stop confirmation is pending; run ${verified.execution.run.id} and its resources remain protected.`
            : `Task ${threadId} paused. Its pending handoff was canceled and the round is preserved.`,
        };
      }
      let settled: DelegationThread | undefined;
      let pauseError: unknown;
      for (let attempt = 0; attempt < 2 && !settled; attempt++) {
        try { settled = await controller.pauseExecution(threadId, normalizedReason, by, undefined, expectedExecution); }
        catch (error) {
          pauseError = error;
          const fresh = this.taskStore.reloadTask(threadId);
          if (!fresh || fresh.status === 'paused' && !fresh.execution?.next && !fresh.execution?.queue) break;
        }
      }
      const latest = this.taskStore.reloadTask(threadId);
      this.assertPauseCommitted(threadId, latest, expectedExecution, settled, pauseError);
      const runIdentity = latest.execution?.run ? { runId: latest.execution.run.id, ...expectedExecution } : undefined;
      this.cancellationRegistry.request(threadId, 'pause', normalizedReason, by, runIdentity);
      this.runner?.cancelThread(threadId, 'pause', normalizedReason, by, runIdentity);
      const stopping = Boolean(latest.execution?.run);
      return {
        threadId,
        status: stopping ? 'STOPPING' : 'PAUSED',
        reason: normalizedReason,
        message: stopping
          ? `Task ${threadId} is paused for scheduling. Stop confirmation is pending; its protected run remains recorded and resume/cleanup stay blocked.${pauseError ? ` ${String((pauseError as Error).message || pauseError)}` : ''}`
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

  private assertPauseCommitted(
    threadId: string,
    latest: DelegationThread | undefined,
    expected: { generation: number; operationId: string },
    returned: DelegationThread | undefined,
    cause?: unknown,
  ): asserts latest is DelegationThread {
    const errorText = cause instanceof Error ? cause.message : cause ? String(cause) : '';
    if (!latest) throw new Error(`Task ${threadId} disappeared or was deleted while pausing${errorText ? `: ${errorText}` : ''}.`);
    if (latest.execution?.version !== 2 || latest.execution.generation !== expected.generation ||
        latest.execution.operationId !== expected.operationId) {
      throw new Error(`Task ${threadId} execution generation changed while pausing; no cancellation was sent.${errorText ? ` ${errorText}` : ''}`);
    }
    if (latest.status !== 'paused' || latest.execution.next || latest.execution.queue) {
      throw new Error(`Task ${threadId} pause was not durably committed; persisted state is ${latest.status} with ${latest.execution.next ? 'next handoff' : latest.execution.queue ? 'queued work' : 'no pause intent'}.${errorText ? ` ${errorText}` : returned ? ` Controller returned ${returned.status} without the required persisted state.` : ''}`);
    }
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
      let resumed: TaskControlResult | undefined;
      try { resumed = await this.resumeTombstonedCleanup(threadId, force); }
      catch (error) {
        if (this.taskStore.isTaskTombstoned(threadId)) return { threadId, status: 'DELETED',
          reason: (reason || '').trim() || 'Deleted by user',
          message: `Task ${threadId} is already deleted. Physical cleanup remains pending: ${(error as Error).message}` };
        throw error;
      }
      if (resumed) return resumed;
      if (this.taskStore.isTaskTombstoned(threadId)) {
        return { threadId, status: 'DELETED', reason: (reason || '').trim() || 'Deleted by user',
          message: `Task ${threadId} is already deleted. Its tombstone is durable; remaining cleanup evidence is unavailable or incomplete.` };
      }
      throw new Error(`Task thread not found: ${threadId}`);
    }

    if (thread.execution?.version === 2) {
      return this.deleteV2Task(threadId, reason, by, force);
    }

    const normalizedReason = (reason || '').trim() || 'Deleted by user';

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
    const isV2: boolean = Boolean((thread as any).execution?.version === 2);
    const cleanupWasInProgress = !!thread.cleanup;
    const cleanupReservation = isV2
      ? await this.taskStore.markTaskCleanup(threadId, 'delete', {
          pid: process.pid,
          birthId: getProcessBirthId(process.pid),
        }, { force, allowExisting: true })
      : undefined;
    const active = isV2
      ? !!(thread.execution?.run || thread.execution?.next || thread.execution?.queue)
      : (
      thread.status === 'running' ||
      thread.status === 'queued' ||
      thread.status === 'paused' ||
      thread.status === 'merging' ||
      !!thread.execution?.run ||
      !!findInFlightRound(thread) ||
      !!new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid ||
      this.executionRegistry.listOwners().some((owner) => owner.threadId === threadId && !!owner.childPid));

    let legacyStopUnconfirmed = false;
    let legacyStopCleanupToken: string | undefined;
    let legacyStopCleanupManifest: Record<string, unknown> | undefined;
    if (active) {
      this.cancellationRegistry.request(threadId, 'delete', normalizedReason, by);
      this.runner?.cancelThread(threadId, 'delete', normalizedReason, by);
      if (!isV2) {
        this.executionRegistry.killOrphanChild(threadId, true);
        const reviewerChild = new TaskRoleLock(this.storageDir).getOwner(threadId)?.childPid;
        if (reviewerChild) { try { killPidTree(reviewerChild); } catch {} }
      } else {
        const controller = ExecutionController.getInstance();
        // Stop is best effort after the durable cleanup reservation. The
        // manifest keeps the exact run identity if this process cannot prove exit.
        if (controller) {
          try { await controller.pauseExecution(threadId, `Deleting: ${normalizedReason}`, by, cleanupReservation?.token); }
          catch (error) { console.warn(`[TaskControlService] Stop remains unconfirmed for ${threadId}: ${(error as Error).message}`); }
        }
      }
      thread.status = 'paused';
      thread.updatedAt = new Date().toISOString();
      appendInterruption(thread, { kind: 'deleted', reason: normalizedReason, at: thread.updatedAt, by });
      thread.summary.lastOutputSnippet = `Deleting: ${normalizedReason}`;
      try {
        if (!cleanupReservation) this.taskStore.upsertTask(thread);
      }
      catch (error) { throw new Error(`Could not persist deletion intent for ${threadId}: ${(error as Error).message}`, { cause: error }); }
      if (!isV2) await this.waitForThreadChildren(threadId).catch((error) => {
        legacyStopUnconfirmed = true;
        console.warn(`[TaskControlService] Stop remains unconfirmed for ${threadId}: ${(error as Error).message}`);
      });
    }

    const latest = this.taskStore.reloadTask(threadId) || thread;
    const runUnconfirmed = !!latest.execution?.run || legacyStopUnconfirmed;
    const preserveOutput = (worktreeHasUnmergedOutput && !force) || !!latest.worktree?.integration?.stashRecoveryLockHeld;
    const cleanupManifest = cleanupReservation
      ? this.cleanupManifest(latest, cleanupReservation.token, normalizedReason, by, force,
          preserveOutput || runUnconfirmed || cleanupWasInProgress)
      : undefined;
    const warnings: string[] = [];
    if (cleanupReservation) {
      await this.taskStore.commitTaskCleanup(threadId, cleanupReservation.token, 'delete', {
        tombstone: { reason: normalizedReason, by, forced: force }, cleanupManifest,
      });
      warnings.push(...this.taskStore.getCleanupCommitWarnings(cleanupReservation.token));
    } else {
      if (legacyStopUnconfirmed) {
        const token = `legacy-delete-${threadId}-${Date.now()}`;
        legacyStopCleanupToken = token;
        const registryOwners = this.executionRegistry.listOwners().filter((owner) => owner.threadId === threadId);
        const roleOwner = new TaskRoleLock(this.storageDir).getOwner(threadId);
        const identities = registryOwners.flatMap((owner) => [
          owner.birthId ? { pid: owner.pid, birthId: owner.birthId } : undefined,
          owner.childPid && owner.childBirthId ? { pid: owner.childPid, birthId: owner.childBirthId } : undefined,
        ].filter(Boolean));
        const manifest = this.cleanupManifest(latest, token, normalizedReason, by, force, true);
        manifest.legacyStopPending = true;
        manifest.legacyStopIdentities = identities;
        manifest.legacyStopIdentityUnknown = !identities.length || !!roleOwner ||
          registryOwners.some((owner) => !owner.birthId || (!!owner.childPid && !owner.childBirthId));
        legacyStopCleanupManifest = manifest;
        this.appendTombstone(latest, normalizedReason, by, undefined, force, token, false, manifest);
      } else this.appendTombstone(latest, normalizedReason, by, undefined, force);
      this.taskStore.deleteTask(threadId, undefined, { preserveAssets: legacyStopUnconfirmed });
    }

    const cleanupOwner = { pid: process.pid, birthId: getProcessBirthId(process.pid) };
    if (cleanupReservation) {
      let claim: 'claimed' | 'busy' | 'complete' | 'missing';
      try { claim = await this.taskStore.claimTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner); }
      catch (error) {
        return { threadId, status: 'DELETED', reason: normalizedReason,
          message: `Task ${threadId} deleted. Physical cleanup claim failed and can be retried: ${(error as Error).message}` };
      }
      if (claim === 'busy') return { threadId, status: 'DELETED', reason: normalizedReason, message: `Task ${threadId} deleted. Physical cleanup is already owned by another cleaner.` };
      if (claim === 'complete') return { threadId, status: 'DELETED', reason: normalizedReason, message: `Task ${threadId} was already deleted and its cleanup is complete.` };
      if (claim !== 'claimed') return { threadId, status: 'DELETED', reason: normalizedReason,
        message: `Task ${threadId} deleted. Physical cleanup evidence is incomplete; retained resources require review.` };
    }

    let nativeSessions: NativeSessionCleanupResult | undefined;
    let cleanupIncomplete = false;
    try {
      // Physical cleanup is single-consumer after the durable tombstone claim.
      if (preserveOutput) warnings.push('Unmerged task output was retained in its worktree and branch.');
      if (runUnconfirmed) warnings.push(`Run ${latest.execution?.run?.id} has not confirmed process-tree exit; its resources and native sessions were retained.`);
      if (latest.worktree?.integration?.stashRecoveryLockHeld) warnings.push('Stash recovery metadata remains pending.');
      if (cleanupWasInProgress) warnings.push('A prior archive or cleanup operation may still own task assets; physical cleanup was deferred.');
      const intentionallyRetained = preserveOutput || runUnconfirmed || cleanupWasInProgress;
      let canCleanAssets = !intentionallyRetained;
      if (canCleanAssets && latest.artifactDir && !this.isSafeArtifactDir(latest)) {
        canCleanAssets = false;
        warnings.push('Task data directory path could not be verified; task assets were retained.');
      }
      if (canCleanAssets && latest.worktree && !isIsolatedTaskWorktree(latest.worktree)) {
        canCleanAssets = false;
        warnings.push('Worktree ownership is incomplete; Git resources were retained.');
      }
      if (canCleanAssets && isIsolatedTaskWorktree(latest.worktree)) {
        if (!this.isSafeDeletionWorktree(latest)) {
          canCleanAssets = false;
          warnings.push('Git worktree path or task branch ownership could not be verified; Git resources were retained.');
        }
      }
      if (canCleanAssets && isIsolatedTaskWorktree(latest.worktree)) {
        const release = await this.mergeLock.acquire(undefined, undefined, undefined, true);
        if (!release) {
          canCleanAssets = false;
          warnings.push('The global merge lock is busy; Git worktree and branch cleanup was deferred.');
        } else {
          try {
            if (latest.worktree) removeTaskWorktree(latest.worktree);
            if (latest.worktree && fs.existsSync(latest.worktree.path)) {
              canCleanAssets = false;
              warnings.push('The Git worktree could not be removed safely and was retained.');
            }
          } finally { release(); }
        }
      }
      if (!canCleanAssets && !intentionallyRetained) cleanupIncomplete = true;
      if (canCleanAssets) nativeSessions = cleanupReservation
        ? this.cleanupVerifiedV2NativeSessions(latest)
        : this.cleanupNativeSessions(latest);
      if (!canCleanAssets) warnings.push('Task worktree, refs, logs, and task data were retained for safe recovery.');
      if (canCleanAssets) {
        removeTaskArtifacts(threadId, this.storageDir, {
          keepArchive: false,
          targetAgent: latest.targetAgent,
          initiatorAgent: latest.initiatorAgent,
          artifactDir: latest.artifactDir,
        });
        if (latest.artifactDir) {
          if (!this.isSafeArtifactDir(latest)) warnings.push('Task data directory path could not be verified and was retained.');
          else if (!removeTaskDataDir(latest.artifactDir, true, threadId) && fs.existsSync(latest.artifactDir)) {
            cleanupIncomplete = true;
            warnings.push('Task data directory cleanup was incomplete; unrecognized or nonempty child assets were retained.');
          }
        }
      }
      if (cleanupReservation) {
        if (!await this.taskStore.finishTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner, !cleanupIncomplete, nativeSessions)) {
          throw new Error(`Could not persist completion for deletion cleanup ${threadId}/${cleanupReservation.token}.`);
        }
      } else {
        this.appendTombstone(thread, normalizedReason, by, nativeSessions, force,
          legacyStopCleanupToken, false, legacyStopCleanupManifest);
      }
    } catch (error) {
      if (cleanupReservation) await this.taskStore.finishTombstoneCleanup(threadId, cleanupReservation.token, cleanupOwner, false, nativeSessions).catch(() => false);
      warnings.push(`Physical cleanup remains pending: ${(error as Error).message}`);
    }

    const cleanupNote = describeNativeSessionCleanup(nativeSessions);
    return {
      threadId,
      status: 'DELETED',
      reason: normalizedReason,
      message:
        `Task ${threadId} deleted. Reason: ${normalizedReason}.` +
        (cleanupNote ? ` Cleaned: ${cleanupNote}.` : '') +
        (warnings.length ? ` Warnings: ${[...new Set(warnings)].slice(0, 3).join(' ')}` : ''),
      nativeSessions,
    };
  }

  private async deleteV2Task(threadId: string, reason: string, by: string, force: boolean): Promise<TaskControlResult> {
    const normalizedReason = (reason || '').trim() || 'Deleted by user';
    const reservation = await this.taskStore.markTaskCleanup(threadId, 'delete', {
      pid: process.pid, birthId: getProcessBirthId(process.pid),
    }, { force, allowExisting: true });
    const fresh = reservation.thread;
    const existingCleanup = fresh.cleanup?.previousCleanup || (fresh.cleanup?.action === 'delete' ? undefined : fresh.cleanup);
    const stashPending = !!(fresh.worktree?.integration?.stashRecoveryLockHeld ||
      false);
    const retainResources = !!fresh.execution?.run || !!existingCleanup || stashPending;
    const manifest = this.cleanupManifest(fresh, reservation.token, normalizedReason, by, force, retainResources);
    manifest.previousCleanup = existingCleanup;
    manifest.stage = 'pending';

    // Tombstone and remove the task from scheduling before any process wait,
    // Git probe, or physical cleanup. The task lock is released by each store
    // call; every later operation is best effort against this immutable token.
    await this.taskStore.commitTaskCleanup(threadId, reservation.token, 'delete', {
      tombstone: { reason: normalizedReason, by, forced: force }, cleanupManifest: manifest,
    });
    const warnings = this.taskStore.getCleanupCommitWarnings(reservation.token);
    const run = fresh.execution?.run;
    if (run) {
      const runIdentity = { runId: run.id, operationId: fresh.execution!.operationId, generation: fresh.execution!.generation };
      this.cancellationRegistry.request(threadId, 'delete', normalizedReason, by, runIdentity);
      this.runner?.cancelThread(threadId, 'delete', normalizedReason, by, runIdentity);
      const controller = ExecutionController.getInstance();
      if (controller) {
        try {
          if (await controller.stopDeletedRun(threadId, run)) {
            await this.taskStore.recordTombstoneRunExitProof(threadId, reservation.token, {
              ...runIdentity, confirmedAt: new Date().toISOString(),
            });
          }
        } catch (error) {
          console.warn(`[TaskControlService] Deleted run ${run.id} remains protected: ${(error as Error).message}`);
        }
      }
    }
    let resumed: TaskControlResult | undefined;
    try { resumed = await this.resumeTombstonedCleanup(threadId, force); }
    catch (error) {
      await this.taskStore.updateTombstoneCleanupManifest(threadId, reservation.token, { retainResources: true }).catch(() => false);
      const warning = `Physical cleanup remains pending: ${(error as Error).message}`;
      return { threadId, status: 'DELETED', reason: normalizedReason,
        message: `Task ${threadId} deleted.${warnings.length ? ` Warnings: ${[...new Set([...warnings, warning])].slice(0, 3).join(' ')}` : ` Warnings: ${warning}`}` };
    }
    if (resumed && warnings.length) resumed.message += ` Warnings: ${warnings.slice(0, 3).join(' ')}`;
    return resumed || {
      threadId, status: 'DELETED', reason: normalizedReason,
      message: `Task ${threadId} deleted. ${run ? `Run ${run.id} remains protected pending exact subtree exit confirmation.` : 'Physical cleanup is pending.'}${warnings.length ? ` Warnings: ${warnings.slice(0, 3).join(' ')}` : ''}`,
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
      const roots = [
        ...(thread.artifactDir ? [path.resolve(taskValidationDir(thread.artifactDir))] : []),
        path.resolve(this.storageDir, 'merge-worktrees'),
      ];
      const root = roots.find((candidate) => normalized(path.dirname(resolved)) === normalized(candidate) && path.basename(resolved).startsWith(`${thread.threadId}-`));
      if (!path.isAbsolute(resolution) || !root || resolved === root ||
          (fs.existsSync(resolved) && fs.lstatSync(resolved).isSymbolicLink())) {
        throw new Error(`Merge resolution path is not a verified task-owned child: ${resolved}`);
      }
      if (fs.existsSync(root) && fs.existsSync(resolved) &&
          normalized(fs.realpathSync(path.dirname(resolved))) !== normalized(fs.realpathSync(root))) {
        throw new Error(`Merge resolution path escapes its task-owned directory: ${resolved}`);
      }
      const registered = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
        .some((line) => line.startsWith('worktree ') && normalized(line.slice(9)) === normalized(resolved));
      if (registered) gitOutput(repo, ['worktree', 'remove', '--force', resolved]);
      if (fs.existsSync(resolved)) throw new Error(`Could not remove merge resolution workspace ${resolved}.`);
    }
  }

  private hasUnmergedTaskOutput(thread: DelegationThread): boolean {
    const worktree = thread.worktree;
    if (!worktree) return false;
    try {
      if (!fs.existsSync(worktree.path) && worktree.integration?.phase === 'no_changes' && worktree.baseCommit &&
          worktree.taskBranch === `mesh/task/${thread.threadId}`) {
        const repo = worktree.repositoryPath || worktree.mainPath;
        const normalized = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
        const registered = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
          .some((line) => line.startsWith('worktree ') && normalized(line.slice(9)) === normalized(worktree.path));
        if (registered) return true;
        try { gitOutput(repo, ['show-ref', '--verify', `refs/heads/${worktree.taskBranch}`]); return true; }
        catch { return false; }
      }
      const head = currentHead(worktree.path);
      const dirty = changedFiles(worktree.path).length > 0;
      const phase = worktree.integration?.phase;
      if (worktree.integration?.stashRecoveryLockHeld || (worktree.integration as any)?.stashCleanupPending || (worktree.integration as any)?.recoveryRequired) return true;
      if (phase === 'no_changes' && !dirty && !!worktree.baseCommit && head === worktree.baseCommit) return false;
      if (phase === 'merged' && !dirty && worktree.approvedCommit && worktree.deliveryTarget) {
        const { repositoryPath, ref } = worktree.deliveryTarget;
        const target = gitOutput(repositoryPath, ['rev-parse', '--verify', `${ref}^{commit}`]);
        try {
          gitOutput(repositoryPath, ['merge-base', '--is-ancestor', worktree.approvedCommit, target]);
          if (head === worktree.approvedCommit) return false;
        } catch {
          const txPath = path.join(this.storageDir, 'merge-transactions', `${thread.threadId}.json`);
          const tx = JSON.parse(fs.readFileSync(txPath, 'utf8'));
          if (tx.candidateCommit === worktree.approvedCommit && tx.sourceCommit === head) {
            gitOutput(repositoryPath, ['merge-base', '--is-ancestor', tx.candidateCommit, target]);
            return false;
          }
        }
      }
      return dirty || (!!worktree.baseCommit && head !== worktree.baseCommit) || !!worktree.approvedCommit || isMergePending(thread);
    } catch { return true; }
  }

  private isVerifiedLedgerWritten(thread: DelegationThread, transaction: any): boolean {
    const worktree = thread.worktree;
    const delivery = worktree?.deliveryTarget;
    if (!worktree || !delivery || transaction?.threadId !== thread.threadId ||
        transaction.phase !== 'ledger_written' || transaction.sourceCommit !== worktree.approvedCommit ||
        !transaction.candidateCommit || !transaction.targetOldCommit ||
        worktree.integration?.phase !== 'merged' ||
        (transaction.stashCleanupPending || transaction.stashCleanupUncertain || transaction.stashRestoreError || transaction.stashRetained)) return false;
    try {
      const repo = path.resolve(delivery.repositoryPath);
      if (path.resolve(worktree.repositoryPath || worktree.mainPath) !== repo) return false;
      if (transaction.repositoryPath && path.resolve(transaction.repositoryPath) !== repo) return false;
      if (transaction.targetRef && transaction.targetRef !== delivery.ref) return false;
      if (worktree.integration.candidateCommit && worktree.integration.candidateCommit !== transaction.candidateCommit) return false;
      if (worktree.integration.mergedCommit && worktree.integration.mergedCommit !== transaction.candidateCommit && worktree.integration.mergedCommit !== worktree.approvedCommit) return false;
      const target = gitOutput(repo, ['rev-parse', '--verify', `${delivery.ref}^{commit}`]);
      gitOutput(repo, ['merge-base', '--is-ancestor', transaction.candidateCommit, target]);
      for (const checkout of new Set([delivery.checkoutPath, transaction.checkoutPath, worktree.integration.targetPath].filter((value): value is string => !!value))) {
        if (!fs.existsSync(checkout) || fs.lstatSync(checkout).isSymbolicLink()) return false;
        const top = gitOutput(checkout, ['rev-parse', '--show-toplevel']);
        if (path.resolve(top) !== path.resolve(checkout)) return false;
        const head = gitOutput(checkout, ['rev-parse', '--verify', 'HEAD^{commit}']);
        const branch = gitOutput(checkout, ['symbolic-ref', '-q', 'HEAD']);
        if (head !== target || branch !== delivery.ref || gitOutput(checkout, ['status', '--porcelain', '--untracked-files=all'])) return false;
      }
      return true;
    } catch { return false; }
  }

  /** The merge journal proves delivery of the approved candidate only. It does
   * not prove the task worktree stayed unchanged after approval. */
  private hasAdditionalTaskWorktreeOutput(thread: DelegationThread, transaction: any): boolean {
    const worktree = thread.worktree;
    if (!worktree) return false;
    const repo = worktree.repositoryPath || worktree.mainPath;
    try {
      if (!fs.existsSync(worktree.path)) {
        const listed = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
          .some((line) => line.startsWith('worktree ') && path.resolve(line.slice(9)) === path.resolve(worktree.path));
        if (listed) return true;
        const refs = gitOutput(repo, ['for-each-ref', '--format=%(refname)', `refs/heads/${worktree.taskBranch}`]).split(/\r?\n/).filter(Boolean);
        // Missing path and branch is a completed cleanup step on a prior pass.
        return refs.length !== 0;
      }
      if (changedFiles(worktree.path).length) return true;
      const head = currentHead(worktree.path);
      const branch = gitOutput(worktree.path, ['symbolic-ref', '-q', 'HEAD']);
      const branchHead = gitOutput(repo, ['rev-parse', '--verify', `refs/heads/${worktree.taskBranch}^{commit}`]);
      return branch !== `refs/heads/${worktree.taskBranch}` || branchHead !== head ||
        head !== worktree.approvedCommit || transaction.sourceCommit !== worktree.approvedCommit;
    } catch { return true; }
  }

  private isSafeDeletionWorktree(thread: DelegationThread): boolean {
    const worktree = thread.worktree;
    if (!isIsolatedTaskWorktree(worktree) || worktree.taskBranch !== `mesh/task/${thread.threadId}`) return false;
    const target = path.resolve(worktree.path);
    const allowed = [
      ...(thread.artifactDir ? [path.resolve(thread.artifactDir, 'worktree')] : []),
      path.resolve(this.storageDir, 'worktrees', thread.threadId),
    ];
    if (!allowed.includes(target)) return false;
    try {
      let targetStat: fs.Stats | undefined;
      try { targetStat = fs.lstatSync(target); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') return false; }
      if (targetStat && (!targetStat.isDirectory() || targetStat.isSymbolicLink())) return false;
      const fromArtifact = !!thread.artifactDir && target === path.resolve(thread.artifactDir, 'worktree');
      const trustedRoot = path.resolve(fromArtifact ? thread.artifactDir! : this.storageDir);
      const relative = path.relative(trustedRoot, target);
      if (!relative || relative.startsWith('..') || path.isAbsolute(relative)) return false;
      let component = trustedRoot;
      for (const part of relative.split(path.sep)) {
        component = path.join(component, part);
        try { if (fs.lstatSync(component).isSymbolicLink()) return false; }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') return false; }
      }
      const repo = worktree.repositoryPath || worktree.mainPath;
      const normalized = (value: string) => process.platform === 'win32' ? path.resolve(value).toLowerCase() : path.resolve(value);
      const registered = gitOutput(repo, ['worktree', 'list', '--porcelain']).split(/\r?\n/)
        .some((line) => line.startsWith('worktree ') && normalized(line.slice(9)) === normalized(target));
      // A prior cleanup may have removed the exact task worktree before a later
      // log/data-directory write failed. The durable tombstone carries the exact
      // task path and branch; an absent path with no registration is a completed
      // step, while an existing unregistered path remains untrusted.
      if (registered) return true;
      if (targetStat || fs.existsSync(target)) return false;
      const ref = `refs/heads/${worktree.taskBranch}`;
      const refs = gitOutput(repo, ['for-each-ref', '--format=%(refname)', ref]).split(/\r?\n/).filter(Boolean);
      if (!refs.length) return true; // The exact task branch was removed in an earlier cleanup pass.
      if (refs.length !== 1 || refs[0] !== ref) return false;
      const branchHead = gitOutput(repo, ['rev-parse', '--verify', `${ref}^{commit}`]);
      const expected = new Set([worktree.approvedCommit, worktree.headCommit, worktree.integration?.sourceCommit,
        worktree.integration?.candidateCommit, worktree.integration?.mergedCommit].filter((value): value is string => !!value));
      return expected.has(branchHead);
    } catch { return false; }
  }

  private isSafeArtifactDir(thread: DelegationThread): boolean {
    if (!thread.artifactDir) return false;
    const expected = thread.threadId.replace(/[^A-Za-z0-9_.-]/g, '_');
    const resolved = path.resolve(thread.artifactDir);
    const authorizedRoot = thread.artifactRoot
      ? path.resolve(thread.artifactRoot)
      : path.resolve(this.storageDir, 'tasks');
    if (path.basename(resolved) !== expected || path.dirname(resolved) !== authorizedRoot ||
        resolved === path.parse(resolved).root || resolved === path.resolve(this.storageDir)) return false;
    try {
      if (fs.existsSync(authorizedRoot) && fs.existsSync(resolved)) {
        if (fs.lstatSync(authorizedRoot).isSymbolicLink() || fs.lstatSync(resolved).isSymbolicLink()) return false;
        return path.resolve(fs.realpathSync(path.dirname(resolved))) === path.resolve(fs.realpathSync(authorizedRoot));
      }
      return !thread.artifactRoot && authorizedRoot === path.resolve(this.storageDir, 'tasks');
    } catch { return false; }
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

  private cleanupManifest(thread: DelegationThread, token: string, reason: string, by: string, forced: boolean, retainResources = false): Record<string, unknown> {
    const worktree = thread.worktree;
    const minimalThread = {
      threadId: thread.threadId,
      initiatorAgent: thread.initiatorAgent,
      targetAgent: thread.targetAgent,
      reviewFlow: thread.reviewFlow,
      receiverSessionId: thread.receiverSessionId,
      receiverSessionCaptured: thread.receiverSessionCaptured,
      reviewerSessionId: thread.reviewerSessionId,
      reviewerSessionCaptured: thread.reviewerSessionCaptured,
      nativeSessionAssignments: thread.nativeSessionAssignments,
      retargets: (thread.retargets || []).map(({ fromAgent, toAgent, previousReceiverSessionId }) => ({ fromAgent, toAgent, previousReceiverSessionId })),
      artifactDir: thread.artifactDir,
      artifactRoot: thread.artifactRoot,
      worktree: worktree ? {
        path: worktree.path, mainPath: worktree.mainPath, commonDir: worktree.commonDir,
        repositoryPath: worktree.repositoryPath, targetRef: worktree.targetRef,
        deliveryTarget: worktree.deliveryTarget, taskBranch: worktree.taskBranch,
        headCommit: worktree.headCommit, approvedCommit: worktree.approvedCommit,
        approvedSnapshot: worktree.approvedSnapshot, mergeResolutionPath: worktree.mergeResolutionPath,
        supersededMergeResolutionPaths: worktree.supersededMergeResolutionPaths,
        branch: worktree.branch, baseCommit: worktree.baseCommit, mergePending: worktree.mergePending,
        integration: worktree.integration ? {
          phase: worktree.integration.phase, sourceCommit: worktree.integration.sourceCommit,
          targetOldCommit: worktree.integration.targetOldCommit, targetCommit: worktree.integration.targetCommit,
          targetPath: worktree.integration.targetPath, candidateCommit: worktree.integration.candidateCommit,
          mergedCommit: worktree.integration.mergedCommit, stashRecoveryPending: worktree.integration.stashRecoveryPending,
          stashRecoveryLockHeld: worktree.integration.stashRecoveryLockHeld,
          stashRestore: worktree.integration.stashRestore ? { status: worktree.integration.stashRestore.status, sha: worktree.integration.stashRestore.sha } : undefined,
        } : undefined,
      } : undefined,
    } as Partial<DelegationThread>;
    const execution = thread.execution ? {
      version: thread.execution.version, revision: thread.execution.revision,
      operationId: thread.execution.operationId, phase: thread.execution.phase,
      round: thread.execution.round, purpose: thread.execution.purpose,
      generation: thread.execution.generation,
      run: thread.execution.run ? structuredClone(thread.execution.run) : undefined,
    } : undefined;
    return {
      token,
      owner: { pid: process.pid, birthId: getProcessBirthId(process.pid) },
      stage: 'pending',
      reason,
      by,
      forced,
      retainResources,
      execution,
      thread: minimalThread,
    };
  }

  private async resumeTombstonedCleanup(threadId: string, force = false): Promise<TaskControlResult | undefined> {
    const file = path.join(this.storageDir, 'deleted_tasks.log');
    if (!fs.existsSync(file)) return undefined;
    let record: any;
    try {
      record = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line))
        .filter((entry) => entry.threadId === threadId && entry.cleanupToken).slice(-1)[0];
    } catch { return undefined; }
    let manifest = record?.cleanupManifest;
    if (!manifest) return undefined;
    if (manifest.stage === 'complete') {
      return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was already deleted and its cleanup is complete.` };
    }
    if (manifest.stage !== 'pending' && manifest.stage !== 'running') return undefined;
    if (force && !manifest.forced) {
      if (!await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { forced: true })) {
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} remains deleted; force authorization could not be durably recorded, so retained output was not discarded.` };
      }
      manifest = { ...manifest, forced: true };
    }
    const thread = manifest.thread as DelegationThread;
    if (!thread || thread.threadId !== threadId) throw new Error(`Deletion cleanup manifest for ${threadId} is damaged.`);
    if (manifest.legacyStopPending) {
      const identities = Array.isArray(manifest.legacyStopIdentities) ? manifest.legacyStopIdentities : [];
      const exited = identities.length > 0 && identities.every((identity: any) =>
        !!identity?.pid && !!identity?.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'exited');
      if (manifest.legacyStopIdentityUnknown || !exited) {
        await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
          message: `Task ${threadId} is deleted. Historical process exit could not be proven; logs, worktree, and native requester session remain retained.` };
      }
      manifest = { ...manifest, legacyStopPending: false, retainResources: false };
      await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { legacyStopPending: false, retainResources: false });
    }
    const previousCleanup = manifest.previousCleanup as { owner?: { pid?: number; birthId?: string } } | undefined;
    if (previousCleanup) {
      const previousOwner = previousCleanup.owner;
      const ownerState = previousOwner?.pid && previousOwner.birthId
        ? probeProcessIdentity(previousOwner.pid, previousOwner.birthId) : 'unknown';
      if (ownerState !== 'exited') return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
        message: `Task ${threadId} was already deleted. Prior archive/cleaner ownership is ${ownerState}; task assets remain isolated.` };
    }
    const run = manifest.execution?.run;
    const existingProof = manifest.runExitProof;
    if (run && (!existingProof || existingProof.runId !== run.id || existingProof.operationId !== manifest.execution.operationId || existingProof.generation !== manifest.execution.generation)) {
      let confirmed = false;
      const controller = ExecutionController.getInstance();
      if (controller) confirmed = await controller.stopDeletedRun(threadId, run).catch(() => false);
      if (!confirmed && !run.unconfirmed && !run.subtrees?.some((tree: any) => tree.enumerationUnconfirmed)) {
        const launch = assessRunLaunchEvidence(run);
        const identities = [run.owner, run.child, ...(run.subtrees || []).flatMap((tree: any) => [tree.root, ...tree.descendants])]
          .filter((identity: any) => !!identity?.pid);
        const identitiesExited = identities.length > 0 && identities.every((identity: any) => !!identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'exited');
        const jobsFinished = (run.subtrees || []).every((tree: any) => !tree.windowsJob || windowsJobFinished(tree.windowsJob));
        const helperFinished = !run.mergeLockHelper || this.mergeLock.confirmHelperExit({
          pid: run.mergeLockHelper.pid, birthId: run.mergeLockHelper.birthId, channelPath: run.mergeLockHelper.channelPath,
        });
        confirmed = launch.safeToClean && identitiesExited && jobsFinished && helperFinished;
      }
      if (!confirmed) {
        await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
          message: `Task ${threadId} is deleted. Run ${run.id} has no verified owner/subtree/Job exit proof; all task resources remain isolated.` };
      }
      const proof = { runId: run.id, operationId: manifest.execution.operationId, generation: manifest.execution.generation, confirmedAt: new Date().toISOString() };
      if (!await this.taskStore.recordTombstoneRunExitProof(threadId, manifest.token, proof)) {
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} is deleted. Run exit was observed but its proof could not be durably recorded; resources remain isolated.` };
      }
      manifest = { ...manifest, runExitProof: proof, retainResources: false };
    }
    const stashPending = !!(thread.worktree?.integration?.stashRecoveryLockHeld || (thread.worktree?.integration as any)?.stashCleanupPending || (thread.worktree?.integration as any)?.recoveryRequired);
    if (stashPending) {
      await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
      return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} is deleted. Stash or merge recovery evidence remains pending and task resources were retained.` };
    }
    if ((thread.artifactDir && !this.isSafeArtifactDir(thread)) ||
        (thread.worktree && !isIsolatedTaskWorktree(thread.worktree))) {
      await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
      return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
        message: `Task ${threadId} was already deleted. Cleanup paths could not be verified; protected resources were retained.` };
    }
    const gitRelease = thread.worktree ? await this.mergeLock.acquire(undefined, undefined, undefined, true) : undefined;
    if (thread.worktree && !gitRelease) {
      await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
      return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
        message: `Task ${threadId} was already deleted. The global merge lock is busy; Git cleanup remains retryable.` };
    }
    const transactionPath = path.join(this.storageDir, 'merge-transactions', `${threadId}.json`);
    let ledgerWrittenProven = false;
    let transactionSnapshot: any;
    let cleanupOwner: { pid: number; birthId: string } | undefined;
    let cleanupClaimed = false;
    try {
      if (thread.worktree && !this.isSafeDeletionWorktree(thread)) {
        await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was deleted. Git worktree or branch ownership is unverified; resources remain retained.` };
      }
      if (fs.existsSync(transactionPath)) {
        transactionSnapshot = JSON.parse(fs.readFileSync(transactionPath, 'utf8'));
        if (transactionSnapshot.phase === 'ledger_written') ledgerWrittenProven = this.isVerifiedLedgerWritten(thread, transactionSnapshot);
        if (!ledgerWrittenProven && !['complete', 'committed'].includes(transactionSnapshot.phase)) {
          await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
          return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was deleted. Merge transaction ${transactionSnapshot.phase || 'unknown'} is preserved for recovery.` };
        }
      }
      const unmerged = !manifest.forced && (ledgerWrittenProven
        ? this.hasAdditionalTaskWorktreeOutput(thread, transactionSnapshot)
        : this.hasUnmergedTaskOutput(thread));
      if (unmerged) {
        await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user',
          message: `Task ${threadId} is deleted. Its worktree has changes, commits, or ownership evidence beyond the approved delivery; the worktree and task branch were retained.` };
      }
      const birthId = getProcessBirthId(process.pid);
      if (!birthId) throw new Error(`Deletion cleanup for ${threadId} is blocked: cleaner identity cannot be verified.`);
      cleanupOwner = { pid: process.pid, birthId };
      const claim = await this.taskStore.claimTombstoneCleanup(threadId, manifest.token, cleanupOwner);
      if (claim === 'busy') {
        await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
        return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} is already deleted. Physical cleanup remains owned by another cleaner.` };
      }
      if (claim === 'complete') return { threadId, status: 'DELETED', reason: record.reason || 'Deleted by user', message: `Task ${threadId} was already deleted and its cleanup is complete.` };
      if (claim !== 'claimed') throw new Error(`Deletion cleanup manifest for ${threadId} is missing or damaged.`);
      cleanupClaimed = true;
      const resolutions = [...new Set([...(thread.worktree?.supersededMergeResolutionPaths || []), thread.worktree?.mergeResolutionPath].filter((entry): entry is string => !!entry))];
      this.removeMergeResolutions(thread, resolutions);
      if (thread.worktree) removeTaskWorktree(thread.worktree);
      if (thread.worktree && (fs.existsSync(thread.worktree.path) || !this.isSafeDeletionWorktree(thread))) {
        throw new Error('The exact task worktree removal could not be verified.');
      }
      if (thread.worktree) {
        try { gitOutput(thread.worktree.repositoryPath || thread.worktree.mainPath, ['show-ref', '--verify', `refs/heads/${thread.worktree.taskBranch}`]);
          throw new Error('The exact task branch still exists after cleanup.'); }
        catch (error) { if ((error as Error).message === 'The exact task branch still exists after cleanup.') throw error; }
      }
    } catch (error) {
      if (cleanupClaimed && cleanupOwner) await this.taskStore.finishTombstoneCleanup(threadId, manifest.token, cleanupOwner, false).catch(() => false);
      else await this.taskStore.updateTombstoneCleanupManifest(threadId, manifest.token, { retainResources: true }).catch(() => false);
      return { threadId, status: 'DELETED', reason: manifest.reason || 'Deleted by user',
        message: `Task ${threadId} is already deleted. Physical cleanup remains pending: ${(error as Error).message}` };
    } finally {
      gitRelease?.();
    }
    let nativeSessions: NativeSessionCleanupResult | undefined;
    try {
      nativeSessions = this.cleanupVerifiedV2NativeSessions(thread);
      removeTaskArtifacts(threadId, this.storageDir, { keepArchive: false, targetAgent: thread.targetAgent,
        initiatorAgent: thread.initiatorAgent, artifactDir: thread.artifactDir });
      if (thread.artifactDir && !removeTaskDataDir(thread.artifactDir, true, threadId) && fs.existsSync(thread.artifactDir)) {
        throw new Error('Task data directory contains an unrecognized or nonempty child asset.');
      }
      if (!await this.taskStore.finishTombstoneCleanup(threadId, manifest.token, cleanupOwner!, true, nativeSessions)) {
        throw new Error(`Could not persist completion for deletion cleanup ${threadId}/${manifest.token}.`);
      }
      return { threadId, status: 'DELETED', reason: manifest.reason || 'Deleted by user',
        message: `Task ${threadId} deletion cleanup resumed from its durable tombstone. ${describeNativeSessionCleanup(nativeSessions)}`,
        nativeSessions };
    } catch (error) {
      if (cleanupOwner) await this.taskStore.finishTombstoneCleanup(threadId, manifest.token, cleanupOwner, false).catch(() => false);
      return { threadId, status: 'DELETED', reason: manifest.reason || 'Deleted by user',
        message: `Task ${threadId} is already deleted. Physical cleanup remains pending: ${(error as Error).message}` };
    }
  }
}
