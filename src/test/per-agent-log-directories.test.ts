import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as zlib from 'zlib';

import {
  WORKER_LOG_DIRNAME,
  REVIEWER_LOG_DIRNAME,
  ARCHIVE_DIRNAME,
  ensureAgentLogDirs,
  findExistingExecutionLog,
  findExistingOutputArchive,
  getArchiveLogDir,
  getReviewerLogDir,
  getWorkerLogDir,
  listRoundArtifacts,
  resolveAgentLogBaseDir,
  roundExecutionLogPath,
  roundOutputArchivePath,
} from '../bridge/round-log';
import {
  createReviewerLogWriter,
  findExistingReviewerLog,
  readReviewerLogTail,
  reviewerBootstrapLogPath,
  reviewerLogPath,
  statReviewerLog,
} from '../bridge/reviewer-log';
import {
  archiveCompletedTaskLogs,
  removeTaskArtifacts,
} from '../bridge/task-summary';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';

function makeTempDir(prefix: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `agent-bridge-test-${prefix}-`));
}

describe('Per-agent log directories with worker_log, reviewer_log, Archive', () => {
  test('resolves per-agent custom log directories and creates the three subdirectories', () => {
    const root = makeTempDir('log-resolve');
    const codexDir = path.join(root, 'custom_codex_logs');
    const agyDir = path.join(root, 'custom_agy_logs');
    const defaultDir = path.join(root, 'custom_default_logs');

    try {
      saveBridgePolicy(
        {
          logDirectories: {
            codex: codexDir,
            antigravity: agyDir,
            default: defaultDir,
          },
        },
        root,
      );

      // 1. Agent-specific resolution
      assert.strictEqual(resolveAgentLogBaseDir('codex', root), codexDir);
      assert.strictEqual(resolveAgentLogBaseDir('antigravity', root), agyDir);

      // 2. Fallback to default
      assert.strictEqual(resolveAgentLogBaseDir('claude_code', root), defaultDir);

      // 3. Fallback when no policy set
      const emptyRoot = makeTempDir('empty-root');
      try {
        assert.strictEqual(
          resolveAgentLogBaseDir('codex', emptyRoot),
          path.join(emptyRoot, 'logs'),
        );
      } finally {
        fs.rmSync(emptyRoot, { recursive: true, force: true });
      }

      // 4. Ensure three subdirectories are automatically created
      const codexSubdirs = ensureAgentLogDirs('codex', root);
      assert.strictEqual(codexSubdirs.workerLogDir, path.join(codexDir, WORKER_LOG_DIRNAME));
      assert.strictEqual(codexSubdirs.reviewerLogDir, path.join(codexDir, REVIEWER_LOG_DIRNAME));
      assert.strictEqual(codexSubdirs.archiveDir, path.join(codexDir, ARCHIVE_DIRNAME));

      assert.ok(fs.existsSync(codexSubdirs.workerLogDir));
      assert.ok(fs.existsSync(codexSubdirs.reviewerLogDir));
      assert.ok(fs.existsSync(codexSubdirs.archiveDir));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('worker_log stores both raw execution logs and compacted output archives', () => {
    const root = makeTempDir('worker-log');
    const codexDir = path.join(root, 'codex_logs');

    try {
      saveBridgePolicy(
        {
          logDirectories: { codex: codexDir },
        },
        root,
      );

      const threadId = 'thread_worker_test';
      const execPath = roundExecutionLogPath(threadId, 1, root, 'codex');
      const outputPath = roundOutputArchivePath(threadId, 1, root, 'codex');

      assert.strictEqual(path.dirname(execPath), path.join(codexDir, 'worker_log'));
      assert.strictEqual(path.dirname(outputPath), path.join(codexDir, 'worker_log'));
      assert.ok(execPath.endsWith('.log'));
      assert.ok(outputPath.endsWith('.output.md'));

      // Write sample logs
      fs.mkdirSync(path.dirname(execPath), { recursive: true });
      fs.writeFileSync(execPath, 'worker raw execution output stream\n', 'utf8');
      fs.writeFileSync(outputPath, '# Compacted Round 1 Output\nDone.', 'utf8');

      // Verify discovery helpers
      const foundExec = findExistingExecutionLog(threadId, 1, { root, agent: 'codex' });
      const foundOutput = findExistingOutputArchive(threadId, 1, { root, agent: 'codex' });
      assert.strictEqual(foundExec, execPath);
      assert.strictEqual(foundOutput, outputPath);

      const artifacts = listRoundArtifacts(threadId, root, 'codex');
      assert.strictEqual(artifacts.length, 2);
      assert.strictEqual(artifacts[0].round, 1);
      assert.strictEqual(artifacts[0].kind, 'execution');
      assert.strictEqual(artifacts[1].round, 1);
      assert.strictEqual(artifacts[1].kind, 'output');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('reviewer_log stores round review logs and bootstrap logs', () => {
    const root = makeTempDir('reviewer-log');
    const agyDir = path.join(root, 'agy_logs');

    try {
      saveBridgePolicy(
        {
          logDirectories: { antigravity: agyDir },
        },
        root,
      );

      const threadId = 'thread_reviewer_test';
      const reviewPath = reviewerLogPath(threadId, 1, root, 'antigravity');
      const bootstrapPath = reviewerBootstrapLogPath(threadId, root, 'antigravity');

      assert.strictEqual(path.dirname(reviewPath), path.join(agyDir, 'reviewer_log'));
      assert.strictEqual(path.dirname(bootstrapPath), path.join(agyDir, 'reviewer_log'));

      // Write via ReviewerLogWriter
      const writer = createReviewerLogWriter(
        threadId,
        1,
        { agent: 'antigravity', sessionId: 'sess-test', kind: 'review', note: 'test' },
        { root, agent: 'antigravity' },
      );
      writer.write('Reviewer comment: code looks great.\n');
      writer.close();

      const stat = statReviewerLog(threadId, 1, root, 'antigravity');
      assert.strictEqual(stat.exists, true);
      assert.ok(stat.sizeBytes > 0);

      const tail = readReviewerLogTail(threadId, 1, { root, agent: 'antigravity' });
      assert.ok(tail && tail.content.includes('Reviewer comment: code looks great.'));

      // Also write bootstrap log
      fs.writeFileSync(bootstrapPath, 'dedicated reviewer bootstrap started\n', 'utf8');
      assert.ok(fs.existsSync(bootstrapPath));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('Archive compresses and packages task logs on completion, preserving archive while pruning active logs', () => {
    const root = makeTempDir('archive-test');
    const workerDir = path.join(root, 'worker_dir');
    const reviewerDir = path.join(root, 'reviewer_dir');

    try {
      saveBridgePolicy(
        {
          logDirectories: {
            codex: workerDir,
            antigravity: reviewerDir,
          },
        },
        root,
      );

      const threadId = 'thread_comp_archive_1';
      const thread: DelegationThread = {
        threadId,
        title: 'Archive Feature Test Task',
        status: 'completed',
        targetAgent: 'codex',
        initiatorAgent: 'antigravity',
        receiverSessionId: 'sess_rec_123',
        workspacePath: root,
        targetModel: 'gpt-5-pro',
        reviewerModel: 'gemini-2.5-pro',
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        currentRound: 1,
        rounds: [
          {
            round: 1,
            type: 'initial',
            prompt: 'Please build feature X',
            output: 'Feature X built completely.',
            status: 'completed',
            durationMs: 1500,
            timestamp: new Date().toISOString(),
          },
        ],
        summary: {
          lastPromptSnippet: 'Please build feature X',
          lastOutputSnippet: 'Feature X built completely.',
          totalDurationMs: 1500,
        },
      };

      // 1. Create worker logs in worker_log/
      const workerLog1 = roundExecutionLogPath(threadId, 1, root, 'codex');
      const workerArchive1 = roundOutputArchivePath(threadId, 1, root, 'codex');
      fs.mkdirSync(path.dirname(workerLog1), { recursive: true });
      fs.writeFileSync(workerLog1, 'Worker stdout logs for round 1\n', 'utf8');
      fs.writeFileSync(workerArchive1, 'Worker output archive markdown for round 1\n', 'utf8');

      // 2. Create reviewer log in reviewer_log/
      const reviewerLog1 = reviewerLogPath(threadId, 1, root, 'antigravity');
      fs.mkdirSync(path.dirname(reviewerLog1), { recursive: true });
      fs.writeFileSync(reviewerLog1, 'Reviewer review notes: Approved!\n', 'utf8');

      // 3. Trigger archival of completed task logs
      const result = archiveCompletedTaskLogs(thread, root);
      assert.ok(result.tarGzPaths.length > 0);
      assert.ok(result.jsonPaths.length > 0);

      // Verify archive was created in both worker and reviewer Archive folders
      const workerTarGz = path.join(getArchiveLogDir('codex', root), `${threadId}_archive.tar.gz`);
      const workerJson = path.join(getArchiveLogDir('codex', root), `${threadId}_archive.json`);
      const reviewerTarGz = path.join(getArchiveLogDir('antigravity', root), `${threadId}_archive.tar.gz`);

      assert.ok(fs.existsSync(workerTarGz), 'Worker archive .tar.gz exists');
      assert.ok(fs.existsSync(workerJson), 'Worker archive .json exists');
      assert.ok(fs.existsSync(reviewerTarGz), 'Reviewer archive .tar.gz exists');

      // Decompress and verify the .tar.gz content
      const gzData = fs.readFileSync(workerTarGz);
      const unzippedTar = zlib.gunzipSync(gzData);
      assert.ok(unzippedTar.length > 0);

      // USTAR check (bytes 257..262 contains 'ustar\0')
      assert.strictEqual(unzippedTar.toString('ascii', 257, 262), 'ustar');
      const tarString = unzippedTar.toString('utf8');
      assert.ok(tarString.includes('summary.json'));
      assert.ok(tarString.includes(`worker_log/${threadId}_round_1.log`));
      assert.ok(tarString.includes(`reviewer_log/${threadId}_round_1.reviewer.log`));

      // 4. Prune loose active logs with keepArchive: true (simulating archiveTask)
      const removed = removeTaskArtifacts(threadId, root, {
        keepArchive: true,
        targetAgent: 'codex',
        initiatorAgent: 'antigravity',
      });
      assert.ok(removed >= 3, `Should have removed at least 3 active files (got ${removed})`);

      // Active files are gone
      assert.strictEqual(fs.existsSync(workerLog1), false);
      assert.strictEqual(fs.existsSync(workerArchive1), false);
      assert.strictEqual(fs.existsSync(reviewerLog1), false);

      // But Archive files SURVIVE
      assert.ok(fs.existsSync(workerTarGz), 'Archive tar.gz must be kept after pruning active logs');
      assert.ok(fs.existsSync(workerJson), 'Archive json must be kept after pruning active logs');

      // 5. Permanent deletion (keepArchive: false) cleans Archive/ too
      removeTaskArtifacts(threadId, root, {
        keepArchive: false,
        targetAgent: 'codex',
        initiatorAgent: 'antigravity',
      });
      assert.strictEqual(fs.existsSync(workerTarGz), false, 'Archive tar.gz removed on permanent deletion');
      assert.strictEqual(fs.existsSync(workerJson), false, 'Archive json removed on permanent deletion');
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('maintains seamless backwards compatibility with legacy flat logs/ directory', () => {
    const root = makeTempDir('legacy-compat');
    try {
      const threadId = 'thread_legacy_99';
      const legacyLogsDir = path.join(root, 'logs');
      fs.mkdirSync(legacyLogsDir, { recursive: true });

      // Put legacy files directly under logs/
      const legacyExec = path.join(legacyLogsDir, `${threadId}_round_1.log`);
      const legacyOutput = path.join(legacyLogsDir, `${threadId}_round_1.output.md`);
      const legacyReviewer = path.join(legacyLogsDir, `${threadId}_round_1.reviewer.log`);

      fs.writeFileSync(legacyExec, 'legacy raw log\n', 'utf8');
      fs.writeFileSync(legacyOutput, 'legacy output archive\n', 'utf8');
      fs.writeFileSync(legacyReviewer, 'legacy reviewer log\n', 'utf8');

      // Helpers find legacy files when new structured path does not exist
      assert.strictEqual(findExistingExecutionLog(threadId, 1, { root }), legacyExec);
      assert.strictEqual(findExistingOutputArchive(threadId, 1, { root }), legacyOutput);
      assert.strictEqual(findExistingReviewerLog(threadId, 1, { root }), legacyReviewer);

      const artifacts = listRoundArtifacts(threadId, root);
      assert.strictEqual(artifacts.length, 2);
      assert.strictEqual(artifacts[0].kind, 'execution');
      assert.strictEqual(artifacts[1].kind, 'output');

      const stat = statReviewerLog(threadId, 1, root);
      assert.strictEqual(stat.exists, true);
      assert.ok(stat.sizeBytes > 0);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('TaskStore.archiveTask creates the compressed archive in Archive/ and retains the digest', () => {
    const root = makeTempDir('store-archive');
    const customLogDir = path.join(root, 'agent_logs');
    let store: TaskStore | undefined;

    try {
      saveBridgePolicy(
        {
          logDirectories: { default: customLogDir },
        },
        root,
      );

      store = new TaskStore(root);
      const threadId = 'thread_store_archive_task';

      const thread: DelegationThread = {
        threadId,
        title: 'Task for Store Archive',
        status: 'completed',
        targetAgent: 'codex',
        initiatorAgent: 'antigravity',
        receiverSessionId: 'sess_rec_456',
        workspacePath: root,
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
        currentRound: 1,
        rounds: [
          {
            round: 1,
            type: 'initial',
            prompt: 'Build archive',
            output: 'Archive ready',
            status: 'completed',
            durationMs: 1000,
            timestamp: new Date().toISOString(),
          },
        ],
        summary: {
          lastPromptSnippet: 'Build archive',
          lastOutputSnippet: 'Archive ready',
          totalDurationMs: 1000,
        },
      };

      store.upsertTask(thread);

      // Create worker log
      const workerLog = roundExecutionLogPath(threadId, 1, root, 'codex');
      fs.mkdirSync(path.dirname(workerLog), { recursive: true });
      fs.writeFileSync(workerLog, 'execution data\n', 'utf8');

      // Call archiveTask
      const archived = store.archiveTask(threadId);
      assert.ok(archived, 'Task should be archived');
      assert.ok(archived.archivedAt, 'archivedAt should be set');
      assert.strictEqual(archived.rounds.length, 0, 'Rounds should be stripped in ledger');
      assert.ok(archived.finalSummary, 'finalSummary should be present');

      // Verify that the Archive folder received the compressed archive
      const archiveTarGz = path.join(getArchiveLogDir('codex', root), `${threadId}_archive.tar.gz`);
      const archiveJson = path.join(getArchiveLogDir('codex', root), `${threadId}_archive.json`);

      assert.ok(fs.existsSync(archiveTarGz), 'Archive .tar.gz should be created');
      assert.ok(fs.existsSync(archiveJson), 'Archive .json should be created');

      // Active worker log should be cleaned up
      assert.strictEqual(fs.existsSync(workerLog), false, 'Active worker log should be pruned');
    } finally {
      store?.dispose();
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
