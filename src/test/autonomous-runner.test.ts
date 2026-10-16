import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { AgentDetector } from '../bridge/agent-detector';

describe('AutonomousRunner Tests', () => {
  const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector);

  test('should generate compliant skip-danger command for Codex initial run', () => {
    const cmdInfo = runner.buildCommand({
      agent: 'codex',
      prompt: 'Implement auth token parser',
      isRework: false,
      receiverSessionId: 'sess-uuid-001',
    });

    assert.ok(cmdInfo.command.includes('codex'));
    assert.ok(cmdInfo.args.includes('exec'));
    assert.ok(cmdInfo.args.includes('Implement auth token parser'));
    assert.ok(cmdInfo.args.includes('--dangerously-bypass-approvals-and-sandbox'));
    assert.ok(cmdInfo.args.includes('--dangerously-bypass-hook-trust'));
    assert.ok(cmdInfo.args.includes('--skip-git-repo-check'));
    assert.ok(cmdInfo.args.includes('--ask-for-approval'));
    assert.ok(cmdInfo.args.includes('never'));
    assert.ok(cmdInfo.args.includes('--json'));
  });

  test('should generate compliant rework resume command for Codex reusing exact session ID', () => {
    const sessionId = 'codex-fixed-session-999';
    const cmdInfo = runner.buildCommand({
      agent: 'codex',
      prompt: 'Fix 2 failing test assertions',
      isRework: true,
      receiverSessionId: sessionId,
    });

    assert.ok(cmdInfo.args.includes('exec'));
    assert.ok(cmdInfo.args.includes('resume'));
    assert.ok(cmdInfo.args.includes(sessionId));
    assert.ok(cmdInfo.args.includes('Fix 2 failing test assertions'));
    assert.ok(cmdInfo.args.includes('--dangerously-bypass-approvals-and-sandbox'));
    assert.ok(cmdInfo.args.includes('--json'));
  });

  test('should generate compliant skip-danger command for Claude Code initial run', () => {
    const sessionId = 'claude-session-123';
    const cmdInfo = runner.buildCommand({
      agent: 'claude_code',
      prompt: 'Refactor logger interface',
      isRework: false,
      receiverSessionId: sessionId,
      title: 'Logger Task',
    });

    assert.ok(cmdInfo.args.includes('-p'));
    assert.ok(cmdInfo.args.includes('Refactor logger interface'));
    assert.ok(cmdInfo.args.includes('--session-id'));
    assert.ok(cmdInfo.args.includes(sessionId));
    assert.ok(cmdInfo.args.includes('--permission-mode'));
    assert.ok(cmdInfo.args.includes('bypassPermissions'));
    assert.ok(cmdInfo.args.includes('--output-format'));
    assert.ok(cmdInfo.args.includes('json'));
  });

  test('should generate compliant rework resume command for Claude Code reusing exact session ID', () => {
    const sessionId = 'claude-resume-target-777';
    const cmdInfo = runner.buildCommand({
      agent: 'claude_code',
      prompt: 'Update documentation and types',
      isRework: true,
      receiverSessionId: sessionId,
    });

    assert.ok(cmdInfo.args.includes('-p'));
    assert.ok(cmdInfo.args.includes('Update documentation and types'));
    assert.ok(cmdInfo.args.includes('--resume'));
    assert.ok(cmdInfo.args.includes(sessionId));
    assert.ok(cmdInfo.args.includes('--permission-mode'));
    assert.ok(cmdInfo.args.includes('bypassPermissions'));
  });

  test('should generate compliant skip-danger command for Antigravity initial and rework', () => {
    const sessionId = 'anti-sess-456';
    const initialCmd = runner.buildCommand({
      agent: 'antigravity',
      prompt: 'Add benchmark script',
      isRework: false,
      receiverSessionId: sessionId,
    });

    assert.ok(initialCmd.args.includes('-p'));
    assert.ok(initialCmd.args.includes('Add benchmark script'));
    assert.ok(initialCmd.args.includes('--conversation'));
    assert.ok(initialCmd.args.includes(sessionId));
    assert.ok(initialCmd.args.includes('--dangerously-skip-permissions'));
    assert.ok(initialCmd.args.includes('--mode'));
    assert.ok(initialCmd.args.includes('accept-edits'));

    const reworkCmd = runner.buildCommand({
      agent: 'antigravity',
      prompt: 'Fix memory leak in benchmark',
      isRework: true,
      receiverSessionId: sessionId,
    });

    assert.ok(reworkCmd.args.includes('--conversation'));
    assert.ok(reworkCmd.args.includes(sessionId));
    assert.ok(reworkCmd.args.includes('-p'));
    assert.ok(reworkCmd.args.includes('Fix memory leak in benchmark'));
    assert.ok(reworkCmd.args.includes('--dangerously-skip-permissions'));
  });
});
