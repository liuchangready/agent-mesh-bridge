import { startWindowsJob, stopWindowsJob, windowsJobFinished, windowsJobOf, type WindowsJobBinding } from './windows-job';
import * as crypto from 'crypto';
import type { ChildProcess } from 'child_process';
import {
  DelegationThread,
  ExecutionPhase,
  ExecutionRun,
  ProcessIdentity,
  TaskInterruption,
  appendInterruption,
  enterTaskFailure,
} from './types';
import { TaskStore } from './task-store';
import {
  collectDescendants,
  confirmProcessTreeExit,
  getProcessBirthId,
  getProcessBirthIds,
  getSpawnedChildIdentity,
  isProcessStrictDescendant,
  probeProcessIdentity,
  probeProcessIdentityAsync,
} from './platform';

export interface RunIdentity {
  runId: string;
  operationId: string;
  generation: number;
  phase: ExecutionPhase;
  round: number;
  role?: 'developer' | 'reviewer';
}

export type LaunchEvidenceStatus =
  | 'NO_RUN'
  | 'PROVABLY_NOT_SPAWNED'
  | 'REGISTERED'
  | 'POSSIBLY_SPAWNED_UNCONFIRMED'
  | 'STOPPING_UNCONFIRMED';

export interface LaunchEvidenceAssessment {
  status: LaunchEvidenceStatus;
  safeToClean: boolean;
  reason: string;
  run?: ExecutionRun;
}

export interface LaunchRecoveryExpectation {
  run: ExecutionRun;
  generation: number;
  operationId: string;
  phase: ExecutionPhase;
  round: number;
}

/**
 * Assesses whether a recorded execution run was never spawned, successfully registered,
 * or entered the crash window between spawn and registration.
 */
export function assessRunLaunchEvidence(
  run?: ExecutionRun,
  prober?: (pid: number, birthId?: string) => 'alive' | 'exited' | 'unknown',
): LaunchEvidenceAssessment {
  if (!run) {
    return {
      status: 'NO_RUN',
      safeToClean: true,
      reason: 'No execution run recorded on task',
    };
  }

  if (run.unconfirmed) {
    return {
      status: 'STOPPING_UNCONFIRMED',
      safeToClean: false,
      reason: 'Run has unconfirmed child exit or launch state; protection retained',
      run,
    };
  }

  const probe = prober || probeProcessIdentity;
  const ownerStatus = run.owner?.pid
    ? (run.owner.birthId ? probe(run.owner.pid, run.owner.birthId) : 'unknown')
    : 'exited';

  const childPid = run.child?.pid;
  const childBirthId = run.child?.birthId;
  const childStatus = childPid
    ? (childBirthId ? probe(childPid, childBirthId) : 'unknown')
    : 'none';

  // Stopping state: check whether child is confirmed dead
  if (run.state === 'stopping') {
    if (childPid && childBirthId) {
      if (childStatus === 'exited' && ownerStatus === 'exited' && !run.unconfirmed) {
        return {
          status: 'STOPPING_UNCONFIRMED',
          safeToClean: true,
          reason: 'Run is stopping; owner and registered child confirmed exited',
          run,
        };
      }
      return {
        status: 'STOPPING_UNCONFIRMED',
        safeToClean: false,
        reason: 'Run is stopping but child is still alive or status is unknown',
        run,
      };
    }

    // No verified child recorded on run (child is undefined or missing birthId)
    // If spawn intent was recorded, or step is spawning/executing/binding, owner exit CANNOT prove child wasn't spawned or is dead!
    if (run.spawnIntentAt || run.step === 'spawning' || run.step === 'executing' || (run.step as string) === 'binding') {
      return {
        status: 'STOPPING_UNCONFIRMED',
        safeToClean: false,
        reason: 'Run is stopping with spawn intent or execution in progress, but child identity/exit was unconfirmed',
        run,
      };
    }

    // If step was 'starting' and no spawnIntentAt was ever persisted
    if (ownerStatus === 'exited' && !run.unconfirmed) {
      return {
        status: 'PROVABLY_NOT_SPAWNED',
        safeToClean: true,
        reason: 'Run is stopping; owner exited before spawn intent was initiated',
        run,
      };
    }

    return {
      status: 'STOPPING_UNCONFIRMED',
      safeToClean: false,
      reason: 'Run is stopping with live or unknown owner and no spawn intent recorded',
      run,
    };
  }

  // Active state: child was registered or active between steps
  if (run.state === 'active') {
    if (childPid && childBirthId) {
      if (childStatus === 'exited' && ownerStatus === 'exited' && !run.unconfirmed) {
        return {
          status: 'REGISTERED',
          safeToClean: true,
          reason: 'Run was registered; owner and child confirmed exited',
          run,
        };
      }
      return {
        status: 'REGISTERED',
        safeToClean: false,
        reason: 'Run is active and child/owner is active or unconfirmed',
        run,
      };
    }
    // Child is undefined (e.g. between child steps during merge)
    if (!childPid) {
      if (ownerStatus === 'alive') {
        return {
          status: 'REGISTERED',
          safeToClean: false,
          reason: 'Run is active with live owner between child steps',
          run,
        };
      }
      return {
        status: 'POSSIBLY_SPAWNED_UNCONFIRMED',
        safeToClean: false,
        reason: 'Owner exited while run was active with no child recorded',
        run,
      };
    }
    // Child has PID but missing birthId: untrusted identity!
    return {
      status: 'POSSIBLY_SPAWNED_UNCONFIRMED',
      safeToClean: false,
      reason: 'Child registered without trusted birthId',
      run,
    };
  }

  // Starting state: before or during spawn
  if (run.state === 'starting') {
    if (run.spawnIntentAt || run.step === 'spawning' || (run.step as string) === 'binding') {
      // Spawn was attempted or initiated!
      // If owner died before registration completed, child may be running orphaned.
      return {
        status: 'POSSIBLY_SPAWNED_UNCONFIRMED',
        safeToClean: false,
        reason:
          ownerStatus === 'exited'
            ? 'Owner exited after spawn intent was recorded before child identity could be registered'
            : 'Spawn or child binding in progress; child not yet fully registered',
        run,
      };
    }

    // run.step === 'starting' (before spawn intent was marked)
    if (ownerStatus === 'exited' && !run.unconfirmed) {
      return {
        status: 'PROVABLY_NOT_SPAWNED',
        safeToClean: true,
        reason: 'Owner exited before spawn was initiated; provably not spawned',
        run,
      };
    }
    return {
      status: 'PROVABLY_NOT_SPAWNED',
      safeToClean: false,
      reason: 'Owner is still starting; spawn not yet initiated',
      run,
    };
  }

  return {
    status: 'POSSIBLY_SPAWNED_UNCONFIRMED',
    safeToClean: false,
    reason: `Unknown run state ${(run as any).state}`,
    run,
  };
}

/**
 * A recovery failure is justified only when the persisted launch intent may have
 * produced a child and the exact persisted owner identity is now proven exited.
 * In particular, `safeToClean === false` alone also describes a live starting
 * owner, which is ordinary cross-controller contention rather than a crash.
 */
export function isUnconfirmedLaunchAfterOwnerExit(
  run?: ExecutionRun,
  prober: (pid: number, birthId?: string) => 'alive' | 'exited' | 'unknown' = probeProcessIdentity,
): boolean {
  if (
    !run ||
    run.state !== 'starting' ||
    run.registeredAt ||
    run.unconfirmed ||
    !(run.spawnIntentAt || run.step === 'spawning' || (run.step as string) === 'binding') ||
    !run.owner?.pid ||
    !run.owner.birthId
  ) {
    return false;
  }
  const evidence = assessRunLaunchEvidence(run, prober);
  return evidence.status === 'POSSIBLY_SPAWNED_UNCONFIRMED' &&
    prober(run.owner.pid, run.owner.birthId) === 'exited';
}

/**
 * Records a visible recovery failure on a task where launch safety cannot be proven,
 * retaining run protection to prevent replacement runs.
 * Verifies under CAS that the run identity hasn't drifted and that the owner has provably exited.
 */
export async function recordLaunchRecoveryFailure(
  taskStore: TaskStore,
  threadId: string,
  runId: string,
  reason: string,
  expected?: LaunchRecoveryExpectation,
): Promise<boolean> {
  taskStore.ensureUpToDate();
  const current = taskStore.reloadTask(threadId);
  if (!current?.execution?.run || current.execution.run.id !== runId) {
    return false;
  }
  const currentRev = current.execution.revision ?? 0;
  try {
    await taskStore.transitionExecution(threadId, currentRev, (t) => {
      if (!t.execution?.run || t.execution.run.id !== runId) {
        throw new Error('Run ID mismatch');
      }
      const run = t.execution.run;
      if (
        run.state !== 'starting' ||
        run.registeredAt ||
        run.unconfirmed ||
        !(run.spawnIntentAt || run.step === 'spawning' || run.step === 'binding')
      ) {
        throw new Error('Run is not an unconfirmed launch');
      }
      if (expected) {
        if (
          t.execution.generation !== expected.generation ||
          t.execution.operationId !== expected.operationId ||
          t.execution.phase !== expected.phase ||
          t.execution.round !== expected.round
        ) {
          throw new Error('Execution identity changed');
        }
        if (
          run.id !== expected.run.id ||
          run.owner?.pid !== expected.run.owner?.pid ||
          run.owner?.birthId !== expected.run.owner?.birthId ||
          run.child?.pid !== expected.run.child?.pid ||
          run.child?.birthId !== expected.run.child?.birthId ||
          run.spawnIntentAt !== expected.run.spawnIntentAt ||
          run.registeredAt !== expected.run.registeredAt ||
          run.unconfirmed !== expected.run.unconfirmed ||
          run.step !== expected.run.step ||
          run.state !== expected.run.state
        ) {
          throw new Error('Launch evidence changed');
        }
      }
      // Re-verify under CAS that owner has indeed exited
      const ownerPid = t.execution.run.owner?.pid;
      const ownerBirthId = t.execution.run.owner?.birthId;
      if (ownerPid) {
        const p = ownerBirthId ? probeProcessIdentity(ownerPid, ownerBirthId) : probeProcessIdentity(ownerPid);
        if (p !== 'exited') {
          throw new Error('Owner is still alive; aborting recovery failure');
        }
      } else {
        throw new Error('Owner identity is missing; aborting recovery failure');
      }
      t.execution.run.state = 'stopping';
      t.execution.run.unconfirmed = true;
      appendInterruption(t, {
        kind: 'error',
        reason: 'PROCESS_STOP_UNCONFIRMED',
        at: new Date().toISOString(),
        by: 'system',
      });
      enterTaskFailure(t, {
        stage: 'execution',
        code: 'LAUNCH_UNCONFIRMED',
        reason,
        at: new Date().toISOString(),
        round: t.execution.round,
      });
    });
    return true;
  } catch {
    return false;
  }
}

/**
 * Verifies that candidatePid belongs to the current task run's subtree.
 * Only verified subtrees may be targeted for directional stopping.
 * The shared host (process.pid) is strictly forbidden as a target.
 */
export function verifySubtreeOwnership(
  candidatePid: number,
  childProcess?: ChildProcess,
  expectedChildPid?: number,
  expectedRootBirthId?: string,
): boolean {
  if (!candidatePid || candidatePid <= 0) return false;
  // NEVER allow the current process (host)
  if (candidatePid === process.pid) return false;

  // A closed ChildProcess retains its numeric PID but no longer establishes
  // current subtree ownership. If a birthId is available, also require that the
  // exact spawned root is still alive before walking its descendants.
  if (childProcess && childProcess.pid) {
    if (childProcess.exitCode !== null || childProcess.signalCode !== null) return false;
    const spawnBoundBirthId = getSpawnedChildIdentity(childProcess);
    if (spawnBoundBirthId && expectedRootBirthId && spawnBoundBirthId !== expectedRootBirthId) return false;
    // The exact ChildProcess handle plus the immutable identity captured by the
    // spawn adapter already proves this root. Avoid a second synchronous OS
    // birth-time query on Windows before the durable child-binding CAS.
    if (expectedRootBirthId && !spawnBoundBirthId && probeProcessIdentity(childProcess.pid, expectedRootBirthId) !== 'alive') return false;
    if (childProcess.pid === candidatePid) return true;
    return isProcessStrictDescendant(candidatePid, childProcess.pid);
  }

  // If an expected root child PID is known, candidate must be that PID or its strict descendant.
  if (expectedChildPid && expectedChildPid > 0 && expectedChildPid !== process.pid) {
    if (expectedRootBirthId && probeProcessIdentity(expectedChildPid, expectedRootBirthId) !== 'alive') return false;
    if (candidatePid === expectedChildPid) return true;
    return isProcessStrictDescendant(candidatePid, expectedChildPid);
  }

  return false;
}

export async function verifyCapturedSubtreeOwnership(
  pid: number,
  child: ChildProcess | undefined,
  birthId?: string,
): Promise<boolean> {
  if (!child?.pid || (birthId && await probeProcessIdentityAsync(child.pid, birthId) !== 'alive')) return false;
  if (!verifySubtreeOwnership(pid, child)) return false;
  return pid === child.pid || !birthId || await probeProcessIdentityAsync(child.pid, birthId) === 'alive';
}

export interface RegisterChildOptions {
  taskStore: TaskStore;
  threadId: string;
  identity: RunIdentity;
  childPid: number;
  /** Immutable identity captured when the child was spawned; never refreshed from its PID. */
  childBirthId?: string;
  /** Descendant identities captured while that exact child birthId was alive. */
  childDescendants?: ProcessIdentity[];
  childProcess?: ChildProcess;
  attachLeases?: (childPid: number) => Promise<void> | void;
  stopTimeoutMs?: number;
}

export interface RegisterChildResult {
  success: boolean;
  child?: ProcessIdentity;
  error?: string;
  unconfirmed?: boolean;
  inProgress?: boolean;
  alreadyRegistered?: boolean;
}

const CHILD_BINDING_IN_PROGRESS = Symbol('CHILD_BINDING_IN_PROGRESS');
const CHILD_BINDING_ALREADY_REGISTERED = Symbol('CHILD_BINDING_ALREADY_REGISTERED');

export async function captureChildDescendants(rootPid: number, expectedRootBirthId?: string): Promise<ProcessIdentity[]> {
  const rootBirthId = expectedRootBirthId || getProcessBirthId(rootPid);
  if (!rootBirthId || await probeProcessIdentityAsync(rootPid, rootBirthId) !== 'alive') return [];
  const pids = collectDescendants(rootPid);
  if (!pids.length) return [];
  const birthIds = await getProcessBirthIds([rootPid, ...pids]);
  const verifiedRoot = birthIds.get(rootPid) === rootBirthId;
  return pids.map(pid => ({ pid, birthId: verifiedRoot ? birthIds.get(pid) : undefined }));
}

/**
 * Directionally stops a verified child process and awaits its exit confirmation.
 * Discovers descendants before stopping root, re-checks birth identity, and never touches foreign processes or the shared host.
 */
export async function stopVerifiedChildTree(
  childPid: number,
  belongsToSubtree: boolean,
  childProcess?: ChildProcess,
  birthId?: string,
  stopTimeoutMs: number = 10_000,
  knownDescendants: ProcessIdentity[] = [],
  allowFreshSubtreeEnumeration: boolean = true,
  recordedJob?: WindowsJobBinding,
): Promise<boolean> {
  if (!belongsToSubtree || !childPid || childPid <= 0 || childPid === process.pid) {
    // Foreign PID, host PID, or unverified subtree: forbidden to kill!
    return false;
  }

  const job = recordedJob ?? windowsJobOf(childProcess);
  if (job) return stopWindowsJob(job, stopTimeoutMs);

  // Must have EITHER a verified ChildProcess handle OR a trusted birthId matching the live process
  const hasValidChildHandle = Boolean(
    childProcess &&
    childProcess.pid === childPid &&
    childProcess.exitCode === null &&
    childProcess.signalCode === null,
  );
  // A missing captured birthId is not repaired by querying whatever currently
  // owns the numeric PID; that may be a replacement process.
  const trustedBirthId = birthId?.trim() || undefined;

  if (!hasValidChildHandle && !trustedBirthId) {
    // Pure numerical PID without handle or birthId: forbidden to kill!
    return false;
  }

  // 1. The root is the only authority for discovering a fresh subtree. If its
  // birthId no longer matches, it exited or the PID was reused: never enumerate
  // children of that numeric PID, even when a new process currently owns it.
  const rootStatus = trustedBirthId
    ? await probeProcessIdentityAsync(childPid, trustedBirthId)
    : 'unknown';
  const rootWasVerifiedAlive = allowFreshSubtreeEnumeration && rootStatus === 'alive';
  let allIdentitiesKnown = Boolean(trustedBirthId) && rootStatus !== 'unknown';

  // 2. Retain only full identities already captured for this run when the root
  // is gone. Add fresh descendants only while the exact root identity is alive.
  const descendantsByIdentity = new Map<string, Required<ProcessIdentity>>();
  const addDescendant = (pid: number, birthId?: string) => {
    if (!pid || pid === childPid || pid === process.pid) return;
    // Once the root is gone, an identity-less recorded descendant cannot be
    // resolved from its numeric PID: it may now name an unrelated process.
    if (!birthId) {
      allIdentitiesKnown = false;
      return;
    }
    descendantsByIdentity.set(`${pid}:${birthId}`, { pid, birthId });
  };
  for (const item of knownDescendants) addDescendant(item.pid, item.birthId);
  if (rootWasVerifiedAlive) {
    try {
      for (const descendant of await captureChildDescendants(childPid, trustedBirthId)) {
        if (descendant.birthId || probeProcessIdentity(descendant.pid) !== 'exited') {
          addDescendant(descendant.pid, descendant.birthId);
        }
      }
    } catch {
      allIdentitiesKnown = false;
    }
  }
  const descendantsToKill = [...descendantsByIdentity.values()];

  // 3. Stop only captured descendants whose complete identity still matches.
  for (const item of descendantsToKill) {
    const status = await probeProcessIdentityAsync(item.pid, item.birthId);
    if (status === 'unknown') {
      allIdentitiesKnown = false;
      continue;
    }
    if (status === 'alive' && await probeProcessIdentityAsync(item.pid, item.birthId) === 'alive') {
      try { process.kill(item.pid, 'SIGKILL'); } catch {}
    }
  }

  // Re-check and stop the root only if the original identity is still alive.
  // On a missing/mismatched identity, a ChildProcess handle is not a reason to
  // enumerate or kill by PID; retain the unresolved result instead.
  if (rootWasVerifiedAlive && trustedBirthId) {
    const beforeRootStop = await probeProcessIdentityAsync(childPid, trustedBirthId);
    if (beforeRootStop === 'alive') {
      try {
        if (hasValidChildHandle) childProcess!.kill('SIGKILL');
        else process.kill(childPid, 'SIGKILL');
      } catch {}
    } else if (beforeRootStop === 'unknown') {
      allIdentitiesKnown = false;
    }
  }

  // 4. Confirm only the root identity and this run's captured descendants.
  // confirmProcessTreeExit is passed the snapshot and itself refuses to discover
  // a fresh subtree once the original root identity has exited.
  const deadline = Date.now() + stopTimeoutMs;
  const allTargetsToCheck: ProcessIdentity[] = [
    ...(trustedBirthId ? [{ pid: childPid, birthId: trustedBirthId }] : []),
    ...descendantsToKill,
  ];
  if (!trustedBirthId) allIdentitiesKnown = false;

  while (Date.now() < deadline) {
    let allExited = true;
    for (const identity of allTargetsToCheck) {
      if (!identity.birthId || await probeProcessIdentityAsync(identity.pid, identity.birthId) !== 'exited') {
        allExited = false;
        break;
      }
    }
    if (allExited) {
      return allIdentitiesKnown && await confirmProcessTreeExit(
        childPid,
        stopTimeoutMs,
        trustedBirthId,
        descendantsToKill,
        allowFreshSubtreeEnumeration,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }

  // Final check at deadline. A missing or unknown captured identity must never
  // be treated as proof that its process tree exited.
  for (const identity of allTargetsToCheck) {
    if (!identity.birthId || await probeProcessIdentityAsync(identity.pid, identity.birthId) !== 'exited') return false;
  }
  return allIdentitiesKnown;
}

/**
 * Transitions task run to stopping/unconfirmed failure under task write lock,
 * verifying that the authoritative task state still matches the captured identity
 * and preserving existing wait_decision/paused/completed/cleanup states.
 */
async function markLaunchFailureUnderCas(
  taskStore: TaskStore,
  threadId: string,
  identity: RunIdentity,
  reason: string,
  unconfirmed: boolean = true,
  interruptionReason?: string,
): Promise<void> {
  const effectiveInterruptionReason =
    interruptionReason ||
    (unconfirmed
      ? 'PROCESS_STOP_UNCONFIRMED'
      : (reason.split(':')[0].trim() || 'PROCESS_STOP_UNCONFIRMED'));
  for (let attempt = 0; attempt < 3; attempt++) {
    taskStore.ensureUpToDate();
    const current = taskStore.reloadTask(threadId);
    if (!current) return;
    const rev = current.execution?.revision ?? 0;
    try {
      await taskStore.transitionExecution(threadId, rev, (t) => {
        if (
          t.execution?.run?.id === identity.runId &&
          t.execution.generation === identity.generation &&
          t.execution.operationId === identity.operationId &&
          t.execution.phase === identity.phase &&
          t.execution.round === identity.round
        ) {
          t.execution.run.state = 'stopping';
          t.execution.run.unconfirmed = unconfirmed;
          appendInterruption(t, {
            kind: 'error',
            reason: effectiveInterruptionReason,
            at: new Date().toISOString(),
            by: 'system',
          });
          if (
            t.status !== 'paused' &&
            t.status !== 'wait_decision' &&
            t.status !== 'completed' &&
            t.status !== 'failed' &&
            !t.cleanup
          ) {
            enterTaskFailure(t, {
              stage: 'execution',
              code: 'LAUNCH_UNCONFIRMED',
              reason,
              at: new Date().toISOString(),
              round: t.execution.round,
            });
          }
        }
      });
      break;
    } catch {
      await new Promise((r) => setTimeout(r, 50));
    }
  }
}

/**
 * Executes confirmed launch registration and binding handshake under task short lock.
 * Phase 1: Persists pending child binding evidence (step: 'binding').
 * Phase 2: Awaits lease attachments and ACK promises.
 * Phase 3: Re-checks captured original identity and commits confirmed active registration.
 */
export async function registerRunChildV2(
  options: RegisterChildOptions,
): Promise<RegisterChildResult> {
  const { taskStore, threadId, identity, childPid } = options;
  const stopTimeoutMs = options.stopTimeoutMs ?? 10_000;

  if (!childPid || childPid <= 0) {
    return { success: false, error: 'Invalid child PID' };
  }

  const capturedBirthId = options.childBirthId?.trim() || undefined;
  const capturedDescendants = options.childDescendants || [];
  const childHandleMatches = options.childProcess?.pid === childPid;

  // Fast idempotency gate: a repeated callback for a child already durably
  // binding/registered must not reach ownership discovery, lease ACK, or cleanup.
  taskStore.ensureUpToDate();
  const snapshot = taskStore.reloadTask(threadId);
  const control = snapshot?.execution;
  const currentRun = control?.run;
  const currentChild = currentRun?.child;
  const sameExecution = !!control &&
    currentRun?.id === identity.runId &&
    control.generation === identity.generation &&
    control.operationId === identity.operationId &&
    control.phase === identity.phase &&
    control.round === identity.round;

  // Reject stale callbacks before inspecting, attaching, or stopping the
  // supplied process. A callback from an older run must not gain authority
  // over the current run's process tree, and identity errors should not depend
  // on whether a short-lived child happened to exit during OS process probing.
  if (!sameExecution) {
    if (!snapshot) return { success: false, error: 'Task not found' };
    if (!control || control.version !== 2) return { success: false, error: 'Not a v2 task' };
    if (currentRun?.id !== identity.runId) {
      return { success: false, error: `Run ID mismatch: expected ${identity.runId}, found ${currentRun?.id}` };
    }
    if (control.generation !== identity.generation) {
      return { success: false, error: `Generation mismatch: expected ${identity.generation}, found ${control.generation}` };
    }
    if (control.operationId !== identity.operationId) {
      return { success: false, error: `OperationId mismatch: expected ${identity.operationId}, found ${control.operationId}` };
    }
    if (control.phase !== identity.phase) {
      return { success: false, error: `Phase mismatch: expected ${identity.phase}, found ${control.phase}` };
    }
    if (control.round !== identity.round) {
      return { success: false, error: `Round mismatch: expected ${identity.round}, found ${control.round}` };
    }
  }

  if (sameExecution && currentRun && currentChild?.pid) {
    if (currentRun.state === 'stopping' || currentRun.unconfirmed) {
      return { success: false, inProgress: true, error: 'Child binding is stopping or unconfirmed' };
    }
    const sameCapturedChild = currentChild.pid === childPid &&
      !!capturedBirthId && currentChild.birthId === capturedBirthId;
    if (sameCapturedChild && currentRun.state === 'active' && currentRun.registeredAt) {
      return { success: true, alreadyRegistered: true, child: { pid: childPid, birthId: capturedBirthId } };
    }
    if (sameCapturedChild || currentRun.step === 'binding') {
      return { success: false, inProgress: true, error: 'Child binding already has an owner' };
    }
    const existingStatus = currentChild.birthId
      ? await probeProcessIdentityAsync(currentChild.pid, currentChild.birthId)
      : 'unknown';
    if (existingStatus === 'alive' || existingStatus === 'unknown') {
      return { success: false, inProgress: true, error: `Run already owns child ${currentChild.pid}` };
    }
  }
  const stopCapturedEvidence = async () => {
    if (!childHandleMatches && !capturedBirthId) return false;
    return stopVerifiedChildTree(
      childPid,
      true,
      options.childProcess,
      capturedBirthId,
      stopTimeoutMs,
      capturedDescendants,
      false,
    );
  };
  const failUnconfirmed = async (reason: string, error: string): Promise<RegisterChildResult> => {
    // A rejected binding may inspect/stop only the immutable spawn identity and
    // descendant snapshot. Never replace a missing or mismatched birthId with a
    // fresh identity read from the numeric PID.
    if (childHandleMatches || capturedBirthId) await stopCapturedEvidence();
    await markLaunchFailureUnderCas(taskStore, threadId, identity, reason, true);
    return { success: false, error, unconfirmed: true };
  };

  // A live exact ChildProcess handle establishes root ownership; its immutable
  // captured birthId is checked against the OS immediately below. Descendants
  // still require a full process-tree ownership walk.
  const belongsToRunnerChild = childHandleMatches
    ? options.childProcess?.exitCode === null && options.childProcess?.signalCode === null
    : verifySubtreeOwnership(childPid, options.childProcess);
  if (!belongsToRunnerChild) {
    return failUnconfirmed(
      `Process ${childPid} does not belong to a live runner child handle`,
      `CHILD_IDENTITY_UNVERIFIABLE: Process ${childPid} is not a live verified child`,
    );
  }

  if (!capturedBirthId) {
    return failUnconfirmed(
      `No spawn-time child birthId was captured for PID ${childPid}`,
      'CHILD_IDENTITY_UNVERIFIABLE: Missing spawn-time child process birthId',
    );
  }

  // Current OS identity validates the immutable spawn-time value only.
  const realBirthId = (await getProcessBirthIds([childPid])).get(childPid);
  if (realBirthId && realBirthId.trim() !== capturedBirthId) {
    return failUnconfirmed(
      `BirthId mismatch for PID ${childPid}: captured ${capturedBirthId}, current ${realBirthId}`,
      `PID_BIRTHID_MISMATCH: Child PID ${childPid} birthId mismatch`,
    );
  }
  if (await probeProcessIdentityAsync(childPid, capturedBirthId) !== 'alive') {
    return failUnconfirmed(
      `Could not verify the captured child identity ${childPid}/${capturedBirthId}`,
      `CHILD_IDENTITY_UNVERIFIABLE: Captured child ${childPid} is no longer verified alive`,
    );
  }

  const belongsToSubtree = await verifyCapturedSubtreeOwnership(
    childPid,
    options.childProcess,
    capturedBirthId,
  );
  if (!belongsToSubtree) {
    return failUnconfirmed(
      `Process ${childPid}/${capturedBirthId} failed exact subtree ownership validation`,
      `CHILD_IDENTITY_UNVERIFIABLE: Process ${childPid} is not the captured runner child`,
    );
  }

  const effectiveBirthId = capturedBirthId;

  // 3. Phase 1: Atomically persist pending binding evidence under task write lock
  let phase1Succeeded = false;
  let phase1Error: string | undefined;
  let bindingInProgressReason = '';

  for (let attempt = 0; attempt < 5; attempt++) {
    taskStore.ensureUpToDate();
    const current = taskStore.reloadTask(threadId);
    if (!current) {
      phase1Error = 'Task not found';
      break;
    }
    const currentRev = current.execution?.revision ?? 0;

    try {
      await taskStore.transitionExecution(threadId, currentRev, (t) => {
        if (!t.execution || t.execution.version !== 2) {
          throw new Error('Not a v2 task');
        }
        if (t.execution.run?.id !== identity.runId) {
          throw new Error(`Run ID mismatch: expected ${identity.runId}, found ${t.execution.run?.id}`);
        }
        if (t.execution.generation !== identity.generation) {
          throw new Error(`Generation mismatch: expected ${identity.generation}, found ${t.execution.generation}`);
        }
        if (t.execution.operationId !== identity.operationId) {
          throw new Error(`OperationId mismatch: expected ${identity.operationId}, found ${t.execution.operationId}`);
        }
        if (t.execution.phase !== identity.phase) {
          throw new Error(`Phase mismatch: expected ${identity.phase}, found ${t.execution.phase}`);
        }
        if (t.execution.round !== identity.round) {
          throw new Error(`Round mismatch: expected ${identity.round}, found ${t.execution.round}`);
        }
        // A persisted child record is the exclusive binding owner for this run.
        // Repeated callbacks must not repeat lease attachment or failure cleanup.
        const existingChild = t.execution.run.child;
        if (t.execution.run.state === 'stopping' || t.execution.run.unconfirmed) {
          if (existingChild?.pid) {
            bindingInProgressReason = `run is ${t.execution.run.state}/${t.execution.run.step}`;
            throw CHILD_BINDING_IN_PROGRESS;
          }
          throw new Error('Run is stopping or unconfirmed; cannot register child');
        }
        if (existingChild?.pid) {
          const sameCapturedIdentity = existingChild.pid === childPid &&
            existingChild.birthId === effectiveBirthId;
          if (
            sameCapturedIdentity &&
            t.execution.run.state === 'active' &&
            t.execution.run.registeredAt
          ) {
            throw CHILD_BINDING_ALREADY_REGISTERED;
          }
          const existingStatus = existingChild.birthId
            ? probeProcessIdentity(existingChild.pid, existingChild.birthId)
            : 'unknown';
          if (sameCapturedIdentity || existingStatus !== 'exited') {
            bindingInProgressReason = `prior child ${existingChild.pid}/${existingChild.birthId || 'missing birthId'} is ${existingStatus}${sameCapturedIdentity ? ' and matches this child' : ''}`;
            throw CHILD_BINDING_IN_PROGRESS;
          }
          if (identity.phase === 'merge') {
            const previous = t.execution.run.subtrees?.find(({ root }) =>
              root.pid === existingChild.pid && root.birthId === existingChild.birthId);
            const descendantStates = previous?.descendants.map(({ pid, birthId }) => ({
              pid, birthId, state: probeProcessIdentity(pid, birthId),
            })) || [];
            const blockedDescendant = descendantStates.find(({ state }) => state !== 'exited');
            if (previous?.windowsJob && !windowsJobFinished(previous.windowsJob)) {
              bindingInProgressReason = 'The prior child Windows Job has no exit receipt';
              throw CHILD_BINDING_IN_PROGRESS;
            }
            if (!previous || blockedDescendant) {
              bindingInProgressReason = !previous
                ? `prior child ${existingChild.pid}/${existingChild.birthId || 'missing birthId'} has no captured subtree`
                : `prior child ${existingChild.pid}/${existingChild.birthId || 'missing birthId'} is exited but descendant ${blockedDescendant!.pid}/${blockedDescendant!.birthId || 'missing birthId'} is ${blockedDescendant!.state}`;
              throw CHILD_BINDING_IN_PROGRESS;
            }
          }
        }
        if (t.cleanup) {
          throw new Error(`Task is reserved for cleanup (${t.cleanup.action})`);
        }
        if (
          t.status === 'paused' ||
          t.status === 'completed' ||
          t.status === 'failed' ||
          t.status === 'wait_decision'
        ) {
          throw new Error(`Task is in invalid status ${t.status}`);
        }
        const legalPhaseStatuses =
          identity.phase === 'develop'
            ? ['running', 'queued']
            : identity.phase === 'review'
              ? ['reviewing', 'queued']
              : ['merging', 'queued'];
        if (!legalPhaseStatuses.includes(t.status)) {
          throw new Error(`Task status ${t.status} does not match legal phase status for ${identity.phase}`);
        }

        // Phase 1 writes child evidence and sets step to 'binding'
        t.execution.run.child = {
          pid: childPid,
          birthId: effectiveBirthId,
        };
        const subtrees = t.execution.run.subtrees || (t.execution.run.subtrees = []);
        const rootKey = `${childPid}:${effectiveBirthId}`;
        let subtree = subtrees.find((item) => `${item.root.pid}:${item.root.birthId || 'unknown'}` === rootKey);
        if (!subtree) {
          subtree = { root: { pid: childPid, birthId: effectiveBirthId }, descendants: [] };
          subtrees.push(subtree);
        }
        const windowsJob = windowsJobOf(options.childProcess);
        if (windowsJob) subtree.windowsJob = windowsJob;
        const knownDescendants = new Map(
          subtree.descendants.map((item) => [`${item.pid}:${item.birthId || 'unknown'}`, item] as const),
        );
        for (const descendant of options.childDescendants || []) {
          knownDescendants.set(`${descendant.pid}:${descendant.birthId || 'unknown'}`, structuredClone(descendant));
        }
        subtree.descendants = [...knownDescendants.values()];
        t.execution.run.state = 'starting';
        t.execution.run.registeredAt = undefined;
        t.execution.run.step = 'binding';
      });
      phase1Succeeded = true;
      break;
    } catch (err: any) {
      if (err === CHILD_BINDING_IN_PROGRESS) {
        return {
          success: false,
          inProgress: true,
          error: `Child binding for ${childPid}/${effectiveBirthId} is already owned by this run${bindingInProgressReason ? `: ${bindingInProgressReason}` : ''}`,
        };
      }
      if (err === CHILD_BINDING_ALREADY_REGISTERED) {
        return {
          success: true,
          alreadyRegistered: true,
          child: { pid: childPid, birthId: effectiveBirthId },
        };
      }
      phase1Error = String(err?.message || err);
      if ((phase1Error.includes('revision mismatch') || phase1Error.includes('Timed out')) && attempt < 4) {
        await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
        continue;
      }
      break;
    }
  }

  if (!phase1Succeeded) {
    const exited = await stopVerifiedChildTree(
      childPid,
      belongsToSubtree,
      options.childProcess,
      effectiveBirthId,
      stopTimeoutMs,
      capturedDescendants,
    );
    await markLaunchFailureUnderCas(
      taskStore,
      threadId,
      identity,
      `REGISTRATION_CAS_FAILED: ${phase1Error}`,
      !exited,
    );
    return {
      success: false,
      error: `REGISTRATION_CAS_FAILED: ${phase1Error}`,
      unconfirmed: !exited,
    };
  }

  // 4. Phase 2: Attach required leases (session lease, merge lock, etc.) and await all ACKs
  if (options.attachLeases) {
    try {
      await options.attachLeases(childPid);
    } catch (leaseError: any) {
      const exited = await stopVerifiedChildTree(
        childPid,
        belongsToSubtree,
        options.childProcess,
        effectiveBirthId,
        stopTimeoutMs,
      );
      await markLaunchFailureUnderCas(
        taskStore,
        threadId,
        identity,
        `LEASE_ATTACHMENT_FAILED: ${leaseError?.message || leaseError}`,
        !exited,
      );
      return {
        success: false,
        error: `LEASE_ATTACHMENT_FAILED: ${leaseError?.message || leaseError}`,
        unconfirmed: !exited,
      };
    }
  }

  // 5. Phase 3: Commit confirmed registration under task write lock, re-checking captured identity
  let phase3Succeeded = false;
  let phase3Error: string | undefined;

  for (let attempt = 0; attempt < 5; attempt++) {
    taskStore.ensureUpToDate();
    const current = taskStore.reloadTask(threadId);
    if (!current) {
      phase3Error = 'Task not found';
      break;
    }
    const currentRev = current.execution?.revision ?? 0;

    try {
      await taskStore.transitionExecution(threadId, currentRev, (t) => {
        if (!t.execution || t.execution.version !== 2) {
          throw new Error('Not a v2 task');
        }
        if (t.execution.run?.id !== identity.runId) {
          throw new Error(`Run ID mismatch: expected ${identity.runId}, found ${t.execution.run?.id}`);
        }
        if (t.execution.generation !== identity.generation) {
          throw new Error(`Generation mismatch: expected ${identity.generation}, found ${t.execution.generation}`);
        }
        if (t.execution.operationId !== identity.operationId) {
          throw new Error(`OperationId mismatch: expected ${identity.operationId}, found ${t.execution.operationId}`);
        }
        if (t.execution.phase !== identity.phase) {
          throw new Error(`Phase mismatch: expected ${identity.phase}, found ${t.execution.phase}`);
        }
        if (t.execution.round !== identity.round) {
          throw new Error(`Round mismatch: expected ${identity.round}, found ${t.execution.round}`);
        }
        if (t.execution.run.state === 'stopping' || t.execution.run.unconfirmed) {
          throw new Error('Run is stopping or unconfirmed; cannot complete registration');
        }
        if (t.cleanup) {
          throw new Error(`Task is reserved for cleanup (${t.cleanup.action})`);
        }
        if (
          t.status === 'paused' ||
          t.status === 'completed' ||
          t.status === 'failed' ||
          t.status === 'wait_decision'
        ) {
          throw new Error(`Task is in invalid status ${t.status}`);
        }
        const legalPhaseStatuses =
          identity.phase === 'develop'
            ? ['running', 'queued']
            : identity.phase === 'review'
              ? ['reviewing', 'queued']
              : ['merging', 'queued'];
        if (!legalPhaseStatuses.includes(t.status)) {
          throw new Error(`Task status ${t.status} does not match legal phase status for ${identity.phase}`);
        }
        if (t.execution.run.child?.pid !== childPid) {
          throw new Error(`Child PID mismatch: expected ${childPid}, found ${t.execution.run.child?.pid}`);
        }
        if (t.execution.run.child.birthId && t.execution.run.child.birthId !== effectiveBirthId) {
          throw new Error(`Child birthId mismatch: expected ${effectiveBirthId}, found ${t.execution.run.child.birthId}`);
        }

        // Commit confirmed active registration
        t.execution.run.state = 'active';
        t.execution.run.step = 'executing';
        t.execution.run.registeredAt = new Date().toISOString();
        t.status =
          identity.phase === 'develop'
            ? 'running'
            : identity.phase === 'review'
              ? 'reviewing'
              : 'merging';
      });
      phase3Succeeded = true;
      break;
    } catch (err: any) {
      phase3Error = String(err?.message || err);
      if ((phase3Error.includes('revision mismatch') || phase3Error.includes('Timed out')) && attempt < 4) {
        await new Promise((r) => setTimeout(r, 50 * (attempt + 1)));
        continue;
      }
      break;
    }
  }

  if (!phase3Succeeded) {
    const exited = await stopVerifiedChildTree(
      childPid,
      belongsToSubtree,
      options.childProcess,
      effectiveBirthId,
      stopTimeoutMs,
      capturedDescendants,
    );
    await markLaunchFailureUnderCas(
      taskStore,
      threadId,
      identity,
      `REGISTRATION_COMMIT_FAILED: ${phase3Error}`,
      !exited,
    );
    return {
      success: false,
      error: `REGISTRATION_COMMIT_FAILED: ${phase3Error}`,
      unconfirmed: !exited,
    };
  }

  startWindowsJob(options.childProcess);
  return {
    success: true,
    child: { pid: childPid, birthId: effectiveBirthId },
  };
}
