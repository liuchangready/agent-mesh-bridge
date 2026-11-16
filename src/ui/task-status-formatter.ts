import { DelegationThread, findInFlightRound } from '../bridge/types';

export interface TaskStatusDisplay {
  iconName: string;
  statusLabel: string;
}

/**
 * Format tree view status label and icon name for a delegation thread.
 * Differentiates active initial development vs rework using the in-flight round.
 */
export function formatTaskTreeStatus(thread: DelegationThread): TaskStatusDisplay {
  const roundText = `Round ${thread.currentRound || 1}`;
  let iconName = 'symbol-misc';
  let statusLabel = thread.status.toUpperCase();

  switch (thread.status) {
    case 'queued':
      iconName = 'clock';
      statusLabel = `Queued (排队中)`;
      break;
    case 'running': {
      iconName = 'sync~spin';
      const inFlight = findInFlightRound(thread);
      const activeRoundNum = inFlight?.round ?? thread.currentRound ?? 1;
      if (inFlight?.type === 'rework') {
        statusLabel = `Reworking (Round ${activeRoundNum})`;
      } else if (inFlight?.type === 'initial') {
        statusLabel = `Running (Round ${activeRoundNum})`;
      } else {
        statusLabel = `Running (${roundText})`;
      }
      break;
    }
    case 'waiting_merge':
      iconName = 'git-merge';
      statusLabel = 'Waiting for verified integration';
      break;
    case 'wait_decision':
      iconName = 'question';
      statusLabel = 'Waiting for user decision';
      break;
    case 'merge_failed':
      iconName = 'error';
      statusLabel = 'Merge failed; retry or delete';
      break;
    case 'waiting_review':
      iconName = 'eye';
      statusLabel = `In-Review (${roundText})`;
      break;
    case 'review_failed':
      iconName = 'warning';
      statusLabel = `Review Failed · 检视失败 (${roundText})`;
      break;
    case 'paused':
      iconName = 'debug-pause';
      statusLabel = `Paused (已暂停)`;
      break;
    case 'completed':
      iconName = 'check';
      statusLabel = `Completed (${roundText})`;
      break;
    case 'failed':
      iconName = 'error';
      statusLabel = `Failed (${roundText})`;
      break;
  }

  return { iconName, statusLabel };
}
