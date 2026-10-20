import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  AutonomousRunner,
  DEFAULT_SILENCE_TIMEOUT_MS,
} from '../bridge/autonomous-runner';
import { AgentDetector } from '../bridge/agent-detector';
import { CancellationRegistry } from '../bridge/cancellation-registry';

describe('AutonomousRunner Tests', () => {
  const detector = new AgentDetector();
  const runner = new AutonomousRunner(detector);

  test('should use a 10-minute silence watchdog by default', () => {
    assert.strictEqual(DEFAULT_SILENCE_TIMEOUT_MS, 600_000);
  });

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
    // `--ask-for-approval` is a GLOBAL Codex option. If it lands after `exec`,
    // Codex rejects the entire invocation and every delegated task is blocked
    // before it starts.
    assert.ok(
      cmdInfo.args.indexOf('--ask-for-approval') < cmdInfo.args.indexOf('exec'),
      `--ask-for-approval must precede exec: ${cmdInfo.args.join(' ')}`,
    );
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
    assert.ok(cmdInfo.args.includes('--dangerously-bypass-hook-trust'));
    assert.ok(cmdInfo.args.includes('--skip-git-repo-check'));
    assert.ok(cmdInfo.args.includes('--json'));
    assert.ok(
      cmdInfo.args.indexOf('--ask-for-approval') < cmdInfo.args.indexOf('exec'),
      `--ask-for-approval must precede exec: ${cmdInfo.args.join(' ')}`,
    );
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

  test('should assess success by error code AND continuous actual output (Codex JSON)', () => {
    const codexStream = [
      '{"type":"thread.started","thread_id":"01a1549a-0034-7843-8af8-846a650058eb"}',
      '{"type":"turn.started"}',
      '{"type":"item.completed","item":{"id":"item_2","type":"agent_message","text":"PONG"}}',
      '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":1}}',
    ].join('\n');

    const success = runner.evaluateResponse({
      exitCode: 0,
      stdout: codexStream,
      stderr: '',
      chunkCount: 4,
      stdoutBytes: codexStream.length,
      stderrBytes: 0,
      firstOutputAtMs: 20,
      lastOutputAtMs: 900,
      maxGapMs: 400,
      gapThresholdMs: 180_000,
    });
    assert.strictEqual(success.success, true);
    assert.strictEqual(success.errorCode, 'OK');
    assert.strictEqual(success.stream.hasActualOutput, true);
    assert.strictEqual(success.stream.continuous, true);
    assert.strictEqual(
      runner.extractSessionId(codexStream),
      '01a1549a-0034-7843-8af8-846a650058eb',
    );

    const statusOnly = [
      '{"type":"thread.started","thread_id":"01a1549a-0034-7843-8af8-846a650058eb"}',
      '{"type":"turn.started"}',
      '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":0}}',
    ].join('\n');
    const empty = runner.evaluateResponse({
      exitCode: 0,
      stdout: statusOnly,
      stderr: '',
      chunkCount: 3,
      stdoutBytes: statusOnly.length,
      stderrBytes: 0,
      firstOutputAtMs: 20,
      lastOutputAtMs: 500,
      maxGapMs: 200,
      gapThresholdMs: 180_000,
    });
    assert.strictEqual(empty.success, false);
    assert.strictEqual(empty.errorCode, 'EMPTY_OUTPUT');

    const stalled = runner.evaluateResponse({
      exitCode: 0,
      stdout: 'PONG',
      stderr: '',
      chunkCount: 2,
      stdoutBytes: 4,
      stderrBytes: 0,
      firstOutputAtMs: 10,
      lastOutputAtMs: 200_000,
      maxGapMs: 199_990,
      gapThresholdMs: 180_000,
    });
    assert.strictEqual(stalled.success, false);
    assert.strictEqual(stalled.errorCode, 'STREAM_STALLED');

    const nonzero = runner.evaluateResponse({
      exitCode: 1,
      stdout: 'PONG',
      stderr: 'boom',
      chunkCount: 1,
      stdoutBytes: 4,
      stderrBytes: 4,
      firstOutputAtMs: 10,
      lastOutputAtMs: 10,
      maxGapMs: 10,
      gapThresholdMs: 180_000,
    });
    assert.strictEqual(nonzero.success, false);
    assert.strictEqual(nonzero.errorCode, 'NONZERO_EXIT');

    // Claude's `--output-format json` emits one final object after a long run.
    // A single substantive chunk is still a valid response for non-streaming CLIs.
    const singleFinal = runner.evaluateResponse({
      exitCode: 0,
      stdout: '{"type":"result","result":"DONE"}',
      stderr: '',
      chunkCount: 1,
      stdoutBytes: 32,
      stderrBytes: 0,
      firstOutputAtMs: 600_000,
      lastOutputAtMs: 600_000,
      maxGapMs: 600_000,
      gapThresholdMs: 180_000,
      expectStreaming: false,
    });
    assert.strictEqual(singleFinal.success, true);
    assert.strictEqual(singleFinal.errorCode, 'OK');
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

  test('cancelThread kills the child and reports a paused interruption', async () => {
    const runner = new AutonomousRunner(detector);
    runner.buildCommand = () => ({
      command: process.execPath,
      args: ['-e', 'setInterval(() => {}, 1000)'],
      useShell: false,
    });

    const execution = runner.execute({
      agent: 'antigravity',
      prompt: 'long running',
      isRework: false,
      receiverSessionId: 'session-cancel-local',
      taskThreadId: 'thread_cancel_local',
      taskRound: 1,
    });

    await new Promise((r) => setTimeout(r, 350));
    const canceled = runner.cancelThread('thread_cancel_local', 'pause', 'pause from test', 'test');
    assert.strictEqual(canceled, true);

    const result = await execution;
    assert.strictEqual(result.interruption?.kind, 'paused');
    assert.match(result.error || '', /pause from test/);
    assert.notStrictEqual(result.exitCode, 0);
  });

  test('a cancellation file from another process stops the child', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-cancel-'));
    const registry = new CancellationRegistry(tempDir);
    const runner = new AutonomousRunner(detector, undefined, registry);
    runner.buildCommand = () => ({
      command: process.execPath,
      args: ['-e', 'setInterval(() => {}, 1000)'],
      useShell: false,
    });

    try {
      const execution = runner.execute({
        agent: 'antigravity',
        prompt: 'long running',
        isRework: false,
        receiverSessionId: 'session-cancel-external',
        taskThreadId: 'thread_cancel_external',
        taskRound: 1,
      });

      await new Promise((r) => setTimeout(r, 300));
      registry.request('thread_cancel_external', 'pause', 'external pause', 'other-process');

      const result = await execution;
      assert.strictEqual(result.interruption?.kind, 'paused');
      assert.match(result.error || '', /external pause/);
    } finally {
      runner.cancelThread('thread_cancel_external', 'stop', 'test cleanup', 'test');
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch {}
    }
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

  test('should treat the DSH transport connection error as transient and retryable', () => {
    const transportFailure = {
      exitCode: 1,
      stdout: '',
      stderr: 'dsh: TRANSPORT: Connection error.',
      extractedOutput: '',
      durationMs: 900,
      error: 'dsh: TRANSPORT: Connection error.',
    };
    assert.strictEqual(runner.isTransportFailure(transportFailure), true);
    assert.strictEqual(runner.isTransientError(transportFailure), true);
    assert.strictEqual(runner.isRetryableFailure(transportFailure), true);

    // A genuine failure must not be replayed just because it mentions a socket.
    const realFailure = {
      exitCode: 1,
      stdout: '',
      stderr: 'error TS2304: Cannot find name "connection".',
      extractedOutput: '',
      durationMs: 12_000,
    };
    assert.strictEqual(runner.isTransportFailure(realFailure), false);
    assert.strictEqual(runner.isRetryableFailure(realFailure), false);
  });

  test('should retry a dropped transport that still exited 0 with no final answer', () => {
    const emptyStream = {
      chunkCount: 1,
      stdoutBytes: 0,
      stderrBytes: 34,
      maxGapMs: 0,
      gapThresholdMs: 600_000,
      hasActualOutput: false,
      continuous: true,
    };
    const swallowedTransport = {
      exitCode: 0,
      stdout: '',
      stderr: 'dsh: TRANSPORT: Connection error.',
      extractedOutput: '',
      durationMs: 800,
      assessment: {
        success: false,
        errorCode: 'EMPTY_OUTPUT',
        exitCode: 0,
        stream: emptyStream,
        reason: 'no final answer',
      },
    };
    assert.strictEqual(runner.isRetryableFailure(swallowedTransport as any), true);

    // Exit 0 + empty output with no transport evidence stays a hard failure.
    const quietEmpty = {
      ...swallowedTransport,
      stderr: '',
      assessment: { ...swallowedTransport.assessment, reason: 'no final answer' },
    };
    assert.strictEqual(runner.isRetryableFailure(quietEmpty as any), false);

    // A paused/stopped run must never be revived by the retry loop.
    const paused = {
      ...swallowedTransport,
      exitCode: 1,
      interruption: { kind: 'paused', reason: 'user pause', at: new Date().toISOString() },
    };
    assert.strictEqual(runner.isRetryableFailure(paused as any), false);
  });

  test('execute replays the same prompt after a transport drop and returns the good attempt', async () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-retry-'));
    const counterFile = path.join(tempDir, 'attempts.txt');
    const retryRunner = new AutonomousRunner(detector);
    retryRunner.buildCommand = () => {
      const seen = fs.existsSync(counterFile) ? Number(fs.readFileSync(counterFile, 'utf8')) : 0;
      fs.writeFileSync(counterFile, String(seen + 1));
      const script =
        seen === 0
          ? 'process.stderr.write("dsh: TRANSPORT: Connection error.\\n"); process.exit(0);'
          : 'process.stdout.write("recovered answer\\n");';
      return { command: process.execPath, args: ['-e', script], useShell: false };
    };

    try {
      const result = await retryRunner.execute({
        agent: 'deepseek_harness',
        prompt: 'same prompt',
        isRework: false,
        receiverSessionId: 'session-retry',
        maxRetries: 2,
        retryBackoffMs: 10,
      });

      assert.strictEqual(result.exitCode, 0);
      assert.strictEqual(result.assessment?.success, true);
      assert.match(result.extractedOutput, /recovered answer/);
      assert.strictEqual(fs.readFileSync(counterFile, 'utf8').trim(), '2');
    } finally {
      try {
        fs.rmSync(tempDir, { recursive: true, force: true });
      } catch {}
    }
  });

  test('execute resumes the native session opened by the failed attempt instead of starting a new one', async () => {
    const retryRunner = new AutonomousRunner(detector);
    const seen: any[] = [];
    retryRunner.executeOnce = async (options: any) => {
      seen.push(options);
      if (seen.length === 1) {
        return {
          exitCode: 1,
          stdout: '',
          stderr: 'dsh: TRANSPORT: Connection error.',
          extractedOutput: '',
          durationMs: 5,
          actualSessionId: 'session-carried-1',
          error: 'dsh: TRANSPORT: Connection error.',
        };
      }
      return {
        exitCode: 0,
        stdout: 'done',
        stderr: '',
        extractedOutput: 'done',
        durationMs: 5,
        actualSessionId: 'session-carried-1',
      };
    };

    const result = await retryRunner.execute({
      agent: 'deepseek_harness',
      prompt: 'same prompt',
      isRework: false,
      receiverSessionId: '',
      maxRetries: 1,
      retryBackoffMs: 5,
    });

    assert.strictEqual(result.exitCode, 0);
    assert.strictEqual(seen.length, 2);
    assert.strictEqual(seen[0].isRework, false);
    assert.strictEqual(
      seen[1].isRework,
      true,
      'the retry must continue the session the failed attempt already opened',
    );
    assert.strictEqual(seen[1].receiverSessionId, 'session-carried-1');
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

