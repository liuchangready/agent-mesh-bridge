import { DelegationThread, ExecutionControl } from './types';

export type ExecutionEligibilityCode =
  | 'NO_EXECUTION'
  | 'LEGACY_VERSION_1'
  | 'UNKNOWN_VERSION'
  | 'CLEANUP_IN_PROGRESS'
  | 'ARCHIVED'
  | 'TERMINAL_STATUS'
  | 'INVALID_STRUCTURE';

export type ExecutionEligibilityResult =
  | { eligible: true; version: 2 }
  | {
      eligible: false;
      code: ExecutionEligibilityCode;
      reason: string;
    };

/** Throw a stable, explicit error at execution entry points for historical or
 * unknown records. Read, archive, delete, and stop paths must not call this. */
export function requireV2ExecutionEligibility(
  thread: DelegationThread | undefined | null,
  action: string,
): asserts thread is DelegationThread & { execution: ExecutionControl & { version: 2 } } {
  const result = validateV2ExecutionEligibility(thread);
  if (!result.eligible) {
    throw new Error(`HISTORICAL_TASK_READ_ONLY: Cannot ${action}: ${result.code}: ${result.reason}`);
  }
}

/**
 * Validate execution eligibility under the v2 execution architecture.
 *
 * Rules:
 * - Tasks undergoing cleanup (cleanup marker present) cannot execute.
 * - Archived tasks cannot execute.
 * - Tasks without execution control record (legacy tasks) cannot execute under v2.
 * - Version 1 tasks cannot execute under v2.
 * - Unknown execution versions are explicitly rejected.
 * - Valid execution.version=2 tasks are accepted.
 * - Terminal states (completed) cannot execute.
 */
export function validateV2ExecutionEligibility(
  thread: DelegationThread | undefined | null,
): ExecutionEligibilityResult {
  if (!thread) {
    return {
      eligible: false,
      code: 'INVALID_STRUCTURE',
      reason: 'Task thread does not exist.',
    };
  }

  if (thread.cleanup) {
    return {
      eligible: false,
      code: 'CLEANUP_IN_PROGRESS',
      reason: `Task ${thread.threadId} is currently reserved for cleanup (${thread.cleanup.action}) by token ${thread.cleanup.token}. Execution is prohibited.`,
    };
  }

  if (thread.archivedAt) {
    return {
      eligible: false,
      code: 'ARCHIVED',
      reason: `Task ${thread.threadId} was archived at ${thread.archivedAt} and cannot execute.`,
    };
  }

  if (!thread.execution) {
    return {
      eligible: false,
      code: 'NO_EXECUTION',
      reason: `Task ${thread.threadId} has no execution control record (legacy task). Old tasks cannot execute under v2.`,
    };
  }

  const version = (thread.execution as any).version;
  if (version === 1) {
    return {
      eligible: false,
      code: 'LEGACY_VERSION_1',
      reason: `Task ${thread.threadId} is on execution version 1. Version 1 tasks cannot execute under v2.`,
    };
  }

  if (version !== 2) {
    return {
      eligible: false,
      code: 'UNKNOWN_VERSION',
      reason: `Task ${thread.threadId} has unsupported execution version ${String(version)}. Only execution.version=2 is supported for new execution.`,
    };
  }

  if (
    typeof thread.execution.revision !== 'number' ||
    !thread.execution.operationId ||
    !thread.execution.phase
  ) {
    return {
      eligible: false,
      code: 'INVALID_STRUCTURE',
      reason: `Task ${thread.threadId} has an invalid v2 execution record structure.`,
    };
  }

  if (thread.status === 'completed') {
    return {
      eligible: false,
      code: 'TERMINAL_STATUS',
      reason: `Task ${thread.threadId} is completed and cannot be executed.`,
    };
  }

  return {
    eligible: true,
    version: 2,
  };
}
