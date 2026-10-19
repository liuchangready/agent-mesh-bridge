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
    assert.ok(cmdInfo.args.includes('--dangerously-skip-permissions'));
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
    assert.ok(cmdInfo.args.includes('--dangerously-skip-permissions'));
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
    // Antigravity does not take --conversation on initial run
    assert.ok(!initialCmd.args.includes('--conversation'));
    assert.ok(initialCmd.args.includes('--dangerously-skip-permissions'));
    assert.ok(initialCmd.args.includes('--mode'));
    assert.ok(initialCmd.args.includes('accept-edits'));
    assert.ok(initialCmd.args.includes('--output-format'));
    assert.ok(initialCmd.args.includes('stream-json'));

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
    assert.ok(reworkCmd.args.includes('--output-format'));
    assert.ok(reworkCmd.args.includes('stream-json'));
  });

  test('should accurately extract session ID and output from stream-json stream', () => {
    const streamSample = [
      '{"event":"init","conversation_id":"conv-test-999","init":{"cwd":"/repo"}}',
      '{"event":"step_update","step_update":{"conversation_id":"conv-test-999","step_index":1,"state":"ACTIVE","text_delta":"Processing"}}',
      '{"event":"result","result":{"conversation_id":"conv-test-999","status":"SUCCESS","response":"Done with refactor"}}',
    ].join('\n');

    const sessionId = runner.extractSessionId(streamSample);
    assert.strictEqual(sessionId, 'conv-test-999');

    const output = runner.extractCleanOutput(streamSample);
    assert.strictEqual(output, 'Done with refactor');
  });

  test('should detect active sessions for claude_code and antigravity', () => {
    const claudeSession = detector.detectActiveSession('claude_code', 'D:\\majiang');
    assert.ok(claudeSession, 'Should detect active session for claude_code in D:\\majiang');
    assert.match(claudeSession, /^[0-9a-f-]{36}$/i, 'Detected session should be a valid UUID');

    const antiSession = detector.detectActiveSession('antigravity', undefined, [
      'non-existent-excluded-session',
    ]);
    assert.ok(antiSession, 'Should detect active session for antigravity');
  });

  test('should detect transient network and eligibility errors correctly', () => {
    const eofErr = {
      exitCode: 1,
      stdout: '',
      stderr: 'Eligibility check failed: Post "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist": EOF',
      extractedOutput: '',
      durationMs: 7500,
      error: 'Eligibility check failed: Post "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist": EOF',
    };
    assert.strictEqual(runner.isTransientError(eofErr), true);

    const normalErr = {
      exitCode: 1,
      stdout: '',
      stderr: 'Compilation error: Unresolved reference: foo',
      extractedOutput: '',
      durationMs: 12000,
      error: 'Compilation error: Unresolved reference: foo',
    };
    assert.strictEqual(runner.isTransientError(normalErr), false);

    const successRes = {
      exitCode: 0,
      stdout: 'All done',
      stderr: '',
      extractedOutput: 'All done',
      durationMs: 5000,
    };
    assert.strictEqual(runner.isTransientError(successRes), false);
  });

  test('should distinguish rate limit errors from innocent code outputs, hashes, UUIDs and line numbers', () => {
    // Innocent patterns that should NOT trigger rate limiting
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('609e3083621647429d255ca4b10c431a'),
      false,
      'Random hash containing 429 substring should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('141a642999ef43fabbff3086f2ab5333'),
      false,
      'Another hash containing 429 substring should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('bot-a95fea8c-8905-4293-b1fd-6c70bfffc304'),
      false,
      'UUID containing 4293 should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('429: val stepEvents = mutableListOf<GameEvent>()'),
      false,
      'Code line number 429: should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('2429: val ledgerConfig = CheatEconomyConfig()'),
      false,
      'Code line number 2429: should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('fun isOverloaded(): Boolean = false'),
      false,
      'Normal programming term "overloaded" should not trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('resetForNewRound() resets state across rounds'),
      false,
      '"resets state" should not trigger rate limit',
    );

    // Genuine rate limit errors that SHOULD trigger
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('Error 429: Too Many Requests. Please retry after 120 seconds.'),
      true,
      'Error 429 should trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('HTTP 429 Too Many Requests'),
      true,
      'HTTP 429 should trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('{"error": {"code": 429, "message": "Rate limit exceeded"}}'),
      true,
      'JSON error with code 429 should trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('Rate limit exceeded: please try again in 5 minutes'),
      true,
      'Rate limit exceeded message should trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('Quota exceeded for metric gemini.googleapis.com/requests'),
      true,
      'Quota exceeded should trigger rate limit',
    );
    assert.strictEqual(
      runner.isRateLimitOrQuotaError('Resource exhausted (429): model is overloaded'),
      true,
      'Resource exhausted / model is overloaded should trigger rate limit',
    );
  });
});

