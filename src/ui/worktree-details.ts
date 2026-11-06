import * as fs from 'fs';
import { WorktreeInfo } from '../bridge/types';

const escape = (value: string) => value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');

export function renderWorktreeDetails(worktree: WorktreeInfo): string {
  const exists = (value?: string) => value ? `${escape(value)}${fs.existsSync(value) ? '' : ' (missing)'}` : 'unknown';
  const target = worktree.deliveryTarget;
  const targetPath = worktree.integration?.targetPath || target?.checkoutPath;
  return `<div class="meta-item" style="grid-column: 1 / -1;">
    <div class="meta-label">Task development and verification Git</div>
    <div class="meta-val">Branch: ${escape(worktree.taskBranch || worktree.branch || 'unknown')}<br/>Approved commit: ${escape(worktree.approvedCommit || 'none')}<br/>Current recorded commit: ${escape(worktree.headCommit || 'unknown')}<br/>Worktree folder: ${exists(worktree.path)}</div>
    <div class="meta-label" style="margin-top:10px;">Git delivery target</div>
    <div class="meta-val">${target
      ? `Branch: ${escape(target.branch)} (${escape(target.ref)})<br/>Repository: ${escape(target.repositoryPath)}<br/>Checkout folder: ${targetPath ? exists(targetPath) : 'not checked out'}<br/>Target commit: ${escape(worktree.integration?.targetCommit || worktree.integration?.targetOldCommit || target.baseCommit || 'unknown')}<br/>Merge state: ${escape(worktree.integration?.phase || 'ready')}`
      : `Legacy target ref: ${escape(worktree.targetRef || 'unknown')}<br/>Repository: ${escape(worktree.repositoryPath || worktree.mainPath || 'unknown')}<br/>Checkout folder: unknown (legacy record has no real delivery target)<br/>Merge state: ${escape(worktree.integration?.phase || 'unknown')}`}</div>
    ${worktree.validation ? `<div class="meta-label" style="margin-top:10px;">Combined verification worktree</div><div class="meta-val">Folder: ${exists(worktree.validation.path)}<br/>Verified commit: ${escape(worktree.validation.commit || 'unknown')}<br/>State: ${escape(worktree.validation.status)}${worktree.validation.result ? ` (${escape(worktree.validation.result)})` : ''}</div>` : ''}
    ${worktree.integration?.error ? `<div class="meta-val" style="white-space:pre-wrap;color:#fca5a5;">${escape(worktree.integration.error)}</div>` : ''}
    ${worktree.integration?.conflictFiles?.length ? `<div class="meta-val">Conflicts: ${escape(worktree.integration.conflictFiles.join(', '))}</div>` : ''}
    ${worktree.integration?.phase === 'blocked' ? '<button class="secondary" style="margin-top:8px;" onclick="retryMerge()">Retry local merge</button>' : ''}
  </div>`;
}
