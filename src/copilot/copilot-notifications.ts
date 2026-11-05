import { DelegationThread } from '../bridge/types';
import { isCopilotRequester } from './copilot-projection';

const TERMINAL = new Set(['completed', 'failed', 'review_failed']);

export function claimCopilotTerminalNotification(
  task: DelegationThread,
  hostInstanceId: string,
  seen: Set<string>,
): boolean {
  if (!isCopilotRequester(task) || !TERMINAL.has(task.status) || task.requesterBinding?.hostInstanceId !== hostInstanceId) return false;
  const key = `${task.threadId}:${task.status}`;
  if (seen.has(key)) return false;
  seen.add(key);
  return true;
}
