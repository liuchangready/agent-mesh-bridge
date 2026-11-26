import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CallbackManager } from '../bridge/callback-manager';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { DelegationRound } from '../bridge/types';
import { readReviewerLogTail, reviewerBootstrapLogPath, reviewerLogPath, statReviewerLog } from '../bridge/reviewer-log';
import { writeReviewerBootstrapToken } from '../bridge/reviewer-bootstrap';
import { createThread } from './unified-execution-fixture';

const completedRound: DelegationRound = {
  round: 1,
  type: 'initial',
  prompt: 'Implement the composition',
  status: 'completed',
  durationMs: 1000,
  timestamp: '2026-10-20T04:30:26.662Z',
  output: 'receiver report',
};

describe('Reviewer stream capture', () => {
  test('streams V2 reviewer output to the task log before the reviewer exits', async (t) => {
    const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'v2-review-log-'));
    t.after(() => fs.rmSync(artifactDir, { recursive: true, force: true }));
    const thread = createThread('thread_v2_live_reviewer_log', 'review');
    thread.status = 'reviewing';
    thread.artifactDir = artifactDir;
    const execution = thread.execution!;
    const identity = {
      runId: 'review-run', operationId: execution.operationId, generation: execution.generation,
      phase: 'review', round: 1, role: 'reviewer',
    } as const;
    execution.run = {
      id: identity.runId, owner: { pid: process.pid }, state: 'active', step: 'executing',
      startedAt: thread.createdAt, role: 'reviewer',
    };

    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    const manager = new CallbackManager(detector);
    manager.setAutonomousRunner(runner);
    runner.execute = async (options) => {
      options.onStdoutChunk?.('reviewer stdout\n');
      options.onStderrChunk?.('reviewer stderr\n');
      const live = readReviewerLogTail(thread.threadId, 1, { artifactDir });
      assert.match(live?.content || '', /reviewer stdout\nreviewer stderr/);
      return { exitCode: 7, stdout: '', stderr: '', extractedOutput: '', durationMs: 0 };
    };

    const outcome = await manager.launchCaller(thread, completedRound, undefined, identity);
    assert.strictEqual(outcome.exitCode, 7);
    assert.match(readReviewerLogTail(thread.threadId, 1, { artifactDir })?.content || '', /\[exit\] code=7/);
  });

  test('writes bootstrap output separately and redacts the one-time credential', async (t) => {
    const artifactDir = fs.mkdtempSync(path.join(os.tmpdir(), 'v2-bootstrap-log-'));
    t.after(() => fs.rmSync(artifactDir, { recursive: true, force: true }));
    const thread = createThread('thread_v2_bootstrap_log', 'review');
    thread.artifactDir = artifactDir;
    thread.reviewBootstrap = { phase: 'pending', attempts: 1 };
    const token = 'a7bb1630-2925-4e9e-aa7c-b560da4c06f1';
    assert.ok(writeReviewerBootstrapToken({
      threadId: thread.threadId, token, agent: thread.initiatorAgent, createdAt: thread.createdAt,
    }, artifactDir, artifactDir));

    const detector = new AgentDetector();
    const runner = new AutonomousRunner(detector);
    const manager = new CallbackManager(detector, artifactDir);
    manager.setAutonomousRunner(runner);
    runner.execute = async () => ({
      exitCode: 1, stdout: `tool arguments ${token}`, stderr: `warning ${token}`, extractedOutput: '', durationMs: 0,
    });

    assert.strictEqual((await manager.launchReviewerBootstrap(thread)).confirmed, false);
    const log = reviewerBootstrapLogPath(thread.threadId, undefined, undefined, artifactDir);
    const content = fs.readFileSync(log, 'utf8');
    assert.match(content, /tool arguments \[redacted\]/);
    assert.match(content, /warning \[redacted\]/);
    assert.match(content, /\[exit\] code=1/);
    assert.ok(!content.includes(token));
    assert.ok(!statReviewerLog(thread.threadId, 1, undefined, undefined, artifactDir).exists);
  });

});
