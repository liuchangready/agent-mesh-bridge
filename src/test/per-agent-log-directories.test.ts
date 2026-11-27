import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as zlib from 'zlib';
import { roundExecutionLogPath, roundOutputArchivePath, findExistingExecutionLog, findExistingOutputArchive, listRoundArtifacts } from '../bridge/round-log';
import { createReviewerLogWriter, findExistingReviewerLog, listReviewerLogRounds, reviewerLogPath } from '../bridge/reviewer-log';
import { ensureTaskDataDir } from '../bridge/task-data-dir';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import { archiveCompletedTaskLogs, removeTaskArtifacts } from '../bridge/task-summary';
import { DelegationThread } from '../bridge/types';
import { testWorkspace } from './workspace-fixture';

function fixture(t: any) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-unified-logs-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const threadId = 'thread_unified_logs';
  const dirs = ensureTaskDataDir(threadId, root);
  return { root, threadId, dirs };
}

test('recorded task roots stay pinned after a global root change', t => {
  const {root, threadId, dirs} = fixture(t);
  const execution = roundExecutionLogPath(threadId, 12, root, 'codex', dirs.artifactDir);
  const output = roundOutputArchivePath(threadId, 12, root, 'codex', dirs.artifactDir);
  fs.writeFileSync(execution, 'execution'); fs.writeFileSync(output, 'answer');
  saveBridgePolicy({ taskDataRoot: path.join(root, 'other') }, root);
  assert.equal(findExistingExecutionLog(threadId, 12, { root, artifactDir: dirs.artifactDir }), execution);
  assert.equal(findExistingOutputArchive(threadId, 12, { root, artifactDir: dirs.artifactDir }), output);
  assert.deepEqual(listRoundArtifacts(threadId, root, 'codex', dirs.artifactDir).map(a => a.round), [12,12]);
  const writer = createReviewerLogWriter(threadId, 12, {agent: 'codex'}, {root, artifactDir: dirs.artifactDir});
  writer.write('review'); writer.close();
  assert.equal(findExistingReviewerLog(threadId, 12, {root, artifactDir: dirs.artifactDir}), reviewerLogPath(threadId, 12, root, 'codex', dirs.artifactDir));
  assert.deepEqual(listReviewerLogRounds(threadId, root, 'codex', dirs.artifactDir), [12]);
});

test('old policy and global log directories are never searched', t => {
  const {root, threadId, dirs} = fixture(t);
  const old = path.join(root, 'old'); fs.mkdirSync(old);
  fs.writeFileSync(path.join(root, 'bridge-policy.json'), JSON.stringify({reviewFlow: 'legacy', logDirectories: {default: old}}));
  for (const name of [threadId+'_round_1.log', threadId+'_round_1.output.md', threadId+'_round_1.reviewer.log']) fs.writeFileSync(path.join(old,name),'old');
  assert.equal(findExistingExecutionLog(threadId,1,{root}),undefined);
  assert.equal(findExistingOutputArchive(threadId,1,{root}),undefined);
  assert.equal(findExistingReviewerLog(threadId,1,{root}),undefined);
  assert.deepEqual(listRoundArtifacts(threadId,root),[]);
  const policy = saveBridgePolicy({},root);
  assert.equal('reviewFlow' in policy, false);
  assert.equal('reviewFlow' in JSON.parse(fs.readFileSync(path.join(root, 'bridge-policy.json'), 'utf8')), false);
  assert.equal((policy as any).logDirectories,undefined);
  assert.equal(roundExecutionLogPath(threadId,1,root),path.join(dirs.workerLogDir,threadId+'_round_1.log'));
});

test('archive contains both role logs and cleanup stays within the task directory', t => {
  const {root,threadId,dirs}=fixture(t);
  fs.writeFileSync(path.join(dirs.workerLogDir,threadId+'_round_1.log'),'worker');
  fs.writeFileSync(path.join(dirs.reviewerLogDir,threadId+'_round_1.reviewer.log'),'reviewer');
  const sibling=ensureTaskDataDir('thread_sibling',root);
  const siblingLog=path.join(sibling.workerLogDir,'thread_sibling_round_1.log'); fs.writeFileSync(siblingLog,'keep');
  const thread: DelegationThread={threadId,title:'logs',status:'completed',reviewFlow:'dedicated',initiatorAgent:'codex',targetAgent:'claude_code',currentRound:1,createdAt:new Date().toISOString(),updatedAt:new Date().toISOString(),receiverSessionId:'worker-session',workspace:testWorkspace(dirs.worktreeDir),artifactDir:dirs.artifactDir,rounds:[],summary:{lastPromptSnippet:'',lastOutputSnippet:'',totalDurationMs:0}};
  const archive=archiveCompletedTaskLogs(thread,root);
  assert.equal(archive.tarGzPaths.length,1);
  const tar=zlib.gunzipSync(fs.readFileSync(archive.tarGzPaths[0])).toString();
  assert.match(tar,/worker_log/); assert.match(tar,/reviewer_log/); assert.match(tar,/summary.json/);
  removeTaskArtifacts(threadId,root,{artifactDir:dirs.artifactDir,keepArchive:true});
  assert.equal(fs.existsSync(archive.tarGzPaths[0]),true); assert.equal(fs.readFileSync(siblingLog,'utf8'),'keep');
  removeTaskArtifacts(threadId,root,{artifactDir:dirs.artifactDir,keepArchive:false});
  assert.equal(fs.existsSync(archive.tarGzPaths[0]),false);
});
