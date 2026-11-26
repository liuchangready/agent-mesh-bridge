import { DelegationThread } from '../bridge/types';

export function renderStashRecoveryAction(thread: DelegationThread, historical: boolean): string {
  if (historical || thread.execution?.version !== 2 || !thread.worktree?.integration?.stashRecoveryPending) return '';
  return '<div class="action-bar"><button onclick="retryMerge()">重试恢复 stash 元数据</button></div>';
}
