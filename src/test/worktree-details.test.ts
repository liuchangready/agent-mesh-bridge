import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { WorktreeInfo } from '../bridge/types';
import { renderWorktreeDetails } from '../ui/worktree-details';

test('task detail shows separate escaped task, delivery and cleaned validation locations', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-detail-repo-'));
  const taskPath = path.join(root, 'task'); const targetPath = path.join(root, 'target');
  fs.mkdirSync(taskPath); fs.mkdirSync(targetPath);
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const worktree: WorktreeInfo = {
    path: taskPath, mainPath: root, repositoryPath: root, targetRef: 'refs/heads/release<&',
    taskBranch: 'mesh/task/detail', baseCommit: 'base', headCommit: 'head', approvedCommit: 'approved',
    deliveryTarget: { branch: 'release<&', ref: 'refs/heads/release<&', repositoryPath: root, checkoutPath: targetPath },
    integration: { phase: 'merged', targetCommit: 'merged-sha', targetPath, updatedAt: new Date().toISOString() },
    validation: { path: path.join(root, 'temporary-validation'), commit: 'validated-sha', status: 'cleaned', updatedAt: new Date().toISOString() },
  };
  const html = renderWorktreeDetails(worktree);
  assert.match(html, /Task development and verification Git/);
  assert.match(html, /Git delivery target/);
  assert.match(html, /release&lt;&amp;/);
  assert.match(html, /validated-sha/);
  assert.match(html, /State: cleaned/);
  assert.match(html, /temporary-validation \(missing\)/);
  assert.doesNotMatch(html, /<repo>/);
});

test('legacy worktree detail labels the target checkout as unknown', () => {
  const html = renderWorktreeDetails({ path: 'missing-worker-path', mainPath: 'repo', targetRef: 'refs/heads/mesh/integration/old' });
  assert.match(html, /Legacy target ref/);
  assert.match(html, /Checkout folder: unknown/);
  assert.match(html, /missing-worker-path \(missing\)/);
});
