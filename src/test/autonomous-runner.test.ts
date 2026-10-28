import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import {
  AutonomousRunner,
  DEFAULT_SILENCE_TIMEOUT_MS,
  clearStaleClaudeSessionLocks,
  detectRateLimitSignal,
  detectStructuredRateLimitSignal,
  parseInUseSessionId,
} from '../bridge/autonomous-runner';
import { AgentDetector } from '../bridge/agent-detector';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';

describe('AutonomousRunner Tests', () => {
  // The runner refuses to spawn an agent it cannot resolve, so a test that only
  // exercises the execution pipeline must not depend on a real Claude install.
  // `process.execPath` exists and is executable everywhere; the tests that care
  // about the spawned program override `buildCommand` anyway.
  const detector = new AgentDetector({ claude_code: process.execPath });
  const runner = new AutonomousRunner(detector);

  test('should use a 10-minute silence watchdog by default', () => {
    assert.strictEqual(DEFAULT_SILENCE_TIMEOUT_MS, 600_000);
  });

  test('detects provider rate-limit signals in structured output and stderr text', () => {
    // Claude Code stream-json retry event — the exact shape that kept a round
    // burning quota for minutes.
    assert.ok(
      detectRateLimitSignal(
        '{"type":"system","subtype":"api_retry","attempt":9,"error_status":429,"error":"rate_limit"}',
      ),
    );
    assert.ok(detectRateLimitSignal('HTTP 429 Too Many Requests'));
    assert.ok(detectRateLimitSignal('{"status":429,"message":"slow down"}'));
    assert.ok(detectRateLimitSignal('rate_limit_error: quota temporarily exhausted'));
    // Ordinary failures and code that merely mentions the words must not trip it.
    assert.strictEqual(detectRateLimitSignal('Process exited with code 1'), undefined);
    assert.strictEqual(detectRateLimitSignal('{"status":500,"error":"boom"}'), undefined);
    assert.strictEqual(detectRateLimitSignal(''), undefined);
  });

  test('ignores rate-limit words quoted from source, tool output and final answers', () => {
    // Regression: a delegated round that implements rate limiting prints its own
    // "RATE_LIMITED" error code. Scanning raw stdout treated that as a provider
    // refusal and killed the round before it could commit anything.
    const transcript = [
      '{"type":"assistant","message":{"content":[{"type":"text","text":"EnqueueError(ServerBusy, \\"connection\\", \\"RATE_LIMITED\\")"}]}}',
      '{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"retryAfterMs: 250, code RATE_LIMITED, HTTP 429 too many requests"}]}}',
      '{"type":"result","subtype":"success","is_error":false,"result":"fixed the RATE_LIMITED path"}',
      '{"type":"system","subtype":"thinking_tokens","estimated_tokens":42}',
      '{"type":"status","phase":"step_end","usage":{"inputTokens":1}}',
      '{"type":"tool_use","name":"Read","input":{"file_path":"TableSessionHandler.cs"}}',
    ];
    for (const line of transcript) {
      assert.strictEqual(
        detectStructuredRateLimitSignal(line),
        undefined,
        `transcript line must not abort the run: ${line}`,
      );
    }
    // Partial JSON (streamed in pieces) is never guessed at.
    assert.strictEqual(detectStructuredRateLimitSignal('{"type":"error","message":"429 Too'), undefined);
  });

  test('still detects structured provider errors', () => {
    assert.ok(
      detectStructuredRateLimitSignal(
        '{"type":"system","subtype":"api_retry","attempt":3,"error_status":429,"error":"rate_limit"}',
      ),
    );
    assert.ok(detectStructuredRateLimitSignal('{"type":"error","message":"429 Too Many Requests"}'));
    assert.ok(
      detectStructuredRateLimitSignal('{"type":"result","is_error":true,"result":"API Error: 429 rate_limit"}'),
    );
    // A 429 status is structured evidence even without an error type.
    assert.ok(detectStructuredRateLimitSignal('{"status":429,"message":"slow down"}'));
    assert.strictEqual(detectStructuredRateLimitSignal('{"status":500,"error":"boom"}'), undefined);
  });

  test('a transcript quoting RATE_LIMITED does not abort the run', async () => {
    const runner = new AutonomousRunner(detector);
    const transcript = [
      '{"type":"assistant","message":{"content":[{"type":"text","text":"reading TableSessionHandler.cs"}]}}',
      '{"type":"user","message":{"role":"user","content":[{"type":"tool_result","content":"EnqueueError(ProtocolErrorCode.ServerBusy, \\"connection\\", \\"RATE_LIMITED\\", retryable: true, retryAfterMs: 250);"}]}}',
      '{"type":"result","subtype":"success","is_error":false,"result":"implemented the RATE_LIMITED branch"}',
    ]
      .map((line) => `console.log(${JSON.stringify(line)});`)
      .join('');
    runner.buildCommand = () => ({
      command: process.execPath,
      args: ['-e', `${transcript}process.exit(0)`],
      useShell: false,
    });

    const started = Date.now();
    const result = await runner.executeOnce({
      agent: 'claude_code',
      prompt: 'x',
      isRework: false,
      receiverSessionId: 'sess-2',
      timeoutMs: 60_000,
      silenceTimeoutMs: 60_000,
    });
    const elapsed = Date.now() - started;

    assert.ok(elapsed < 15_000, `should not be killed as a rate limit, took ${elapsed}ms`);
    assert.strictEqual(result.exitCode, 0);
    assert.notStrictEqual(
      result.assessment?.errorCode,
      'RATE_LIMITED',
      'the transcript must not be mistaken for a provider refusal',
    );
  });

  test('clears only dead-owner Claude session records', () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-claude-reg-'));
    const dir = path.join(home, '.claude', 'sessions');
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(
      path.join(dir, '999999.json'),
      JSON.stringify({ pid: 999999, sessionId: 'dead-session' }),
    );
    fs.writeFileSync(
      path.join(dir, `${process.pid}.json`),
      JSON.stringify({ pid: process.pid, sessionId: 'live-session' }),
    );

    const removed = clearStaleClaudeSessionLocks({}, home);
    assert.strictEqual(removed.length, 1);
    assert.strictEqual(removed[0].sessionId, 'dead-session');
    assert.strictEqual(fs.existsSync(path.join(dir, '999999.json')), false);
    assert.strictEqual(
      fs.existsSync(path.join(dir, `${process.pid}.json`)),
      true,
      'a live process keeps its lock',
    );

    // A live record must never be deleted even when it is selected by session id.
    assert.strictEqual(clearStaleClaudeSessionLocks({ sessionId: 'live-session' }, home).length, 0);
    fs.rmSync(home, { recursive: true, force: true });
  });

  test('parses the session id out of the in-use error', () => {
    assert.strictEqual(
      parseInUseSessionId(
        'Error: Session ID e327178b-f398-4c25-a7c3-9f5c8f60e0e0 is already in use.',
      ),
      'e327178b-f398-4c25-a7c3-9f5c8f60e0e0',
    );
    assert.strictEqual(parseInUseSessionId('some other failure'), undefined);
  });

  test('a fresh start that hits an existing session resumes it instead of failing', async () => {
    const sessionId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
    const runner = new AutonomousRunner(detector);
    runner.homeDir = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-claude-resume-'));
    let spawns = 0;
    const isReworkCalls: boolean[] = [];
    runner.buildCommand = (opts) => {
      spawns += 1;
      isReworkCalls.push(Boolean(opts.isRework));
      if (spawns === 1) {
        return {
          command: process.execPath,
          args: [
            '-e',
            `console.error('Error: Session ID ${sessionId} is already in use.');process.exit(1)`,
          ],
          useShell: false,
        };
      }
      return {
        command: process.execPath,
        args: [
          '-e',
          "console.log(JSON.stringify({type:'result',subtype:'success',is_error:false,result:'resumed ok'}))",
        ],
        useShell: false,
      };
    };

    const result = await runner.execute({
      agent: 'claude_code',
      prompt: 'x',
      isRework: false,
      receiverSessionId: sessionId,
      timeoutMs: 60_000,
      silenceTimeoutMs: 60_000,
      retryBackoffMs: 10,
    });

    assert.deepStrictEqual(
      isReworkCalls,
      [false, true],
      'the replay must resume the existing session instead of reserving it again',
    );
    assert.strictEqual(result.exitCode, 0);
  });

  test('clears a stale Claude session lock and retries instead of failing in one second', async () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-claude-lock-'));
    const sessions = path.join(home, '.claude', 'sessions');
    fs.mkdirSync(sessions, { recursive: true });
    const sessionId = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
    const lockFile = path.join(sessions, '999999.json');
    fs.writeFileSync(lockFile, JSON.stringify({ pid: 999999, sessionId }));

    const runner = new AutonomousRunner(detector);
    runner.homeDir = home;
    let spawns = 0;
    runner.buildCommand = () => {
      spawns += 1;
      if (spawns === 1) {
        return {
          command: process.execPath,
          args: [
            '-e',
            `console.error('Error: Session ID ${sessionId} is already in use.');process.exit(1)`,
          ],
          useShell: false,
        };
      }
      return {
        command: process.execPath,
        args: [
          '-e',
          "console.log(JSON.stringify({type:'result',subtype:'success',is_error:false,result:'resumed ok',usage:{input_tokens:1,output_tokens:1}}))",
        ],
        useShell: false,
      };
    };

    const result = await runner.execute({
      agent: 'claude_code',
      prompt: 'x',
      isRework: true,
      receiverSessionId: sessionId,
      timeoutMs: 60_000,
      silenceTimeoutMs: 60_000,
      retryBackoffMs: 10,
    });

    assert.strictEqual(spawns, 2, 'the runner must replay the session after clearing the lock');
    assert.strictEqual(result.exitCode, 0);
    assert.strictEqual(fs.existsSync(lockFile), false, 'the stale record is gone');
    fs.rmSync(home, { recursive: true, force: true });
  });

  test('aborts immediately on a provider 429 instead of waiting out CLI retries', async () => {
    const runner = new AutonomousRunner(detector);
    runner.buildCommand = () => ({
      command: process.execPath,
      args: [
        '-e',
        "console.log(JSON.stringify({type:'system',subtype:'api_retry',attempt:1,max_retries:10,error_status:429,error:'rate_limit'}));setInterval(()=>{},1000)",
      ],
      useShell: false,
    });

    const started = Date.now();
    const result = await runner.executeOnce({
      agent: 'antigravity',
      prompt: 'x',
      isRework: false,
      receiverSessionId: 'sess-1',
      timeoutMs: 60_000,
      // A silent run would only be killed after 10 minutes; the 429 must win.
      silenceTimeoutMs: 60_000,
    });
    const elapsed = Date.now() - started;

    assert.ok(elapsed < 15_000, `should abort fast, took ${elapsed}ms`);
    assert.strictEqual(result.assessment?.errorCode, 'RATE_LIMITED');
    assert.strictEqual(result.assessment?.success, false);
    assert.match(result.error || '', /rate limit/i);
    assert.strictEqual(
      runner.isRetryableFailure(result),
      false,
      'a rate limit must not be replayed immediately',
    );
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
    const prompt = 'Refactor logger interface\n\nThis second line must survive too';
    const cmdInfo = runner.buildCommand({
      agent: 'claude_code',
      prompt,
      isRework: false,
      receiverSessionId: sessionId,
      title: 'Logger Task',
    });

    assert.ok(cmdInfo.args.includes('-p'));
    // The prompt travels on stdin: a multi-line argv element is truncated by
    // cmd.exe on Windows (the npm shim is a .cmd), so the agent would silently
    // lose everything after the first line.
    assert.strictEqual(cmdInfo.stdinPayload, prompt);
    assert.ok(!cmdInfo.args.includes(prompt), cmdInfo.args.join(' '));
    assert.ok(cmdInfo.args.includes('--session-id'));
    assert.ok(cmdInfo.args.includes(sessionId));
    assert.ok(cmdInfo.args.includes('-n'));
    assert.ok(cmdInfo.args.includes('Logger Task'));
    assert.ok(cmdInfo.args.includes('--dangerously-skip-permissions'));
    assert.ok(cmdInfo.args.includes('--output-format'));
    assert.ok(cmdInfo.args.includes('stream-json'));
    assert.ok(cmdInfo.args.includes('--verbose'));
  });

  test('should generate compliant rework resume command for Claude Code reusing exact session ID', () => {
    const sessionId = 'claude-resume-target-777';
    const prompt = 'Update documentation and types\n- keep the same session';
    const cmdInfo = runner.buildCommand({
      agent: 'claude_code',
      prompt,
      isRework: true,
      receiverSessionId: sessionId,
    });

    assert.ok(cmdInfo.args.includes('-p'));
    assert.strictEqual(cmdInfo.stdinPayload, prompt);
    assert.ok(!cmdInfo.args.includes(prompt), cmdInfo.args.join(' '));
    assert.ok(cmdInfo.args.includes('--resume'));
    assert.ok(cmdInfo.args.includes(sessionId));
    assert.ok(!cmdInfo.args.includes('--session-id'));
    assert.ok(cmdInfo.args.includes('--dangerously-skip-permissions'));
    assert.ok(cmdInfo.args.includes('stream-json'));
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
      streamGapPolicy: 'fail',
    });
    assert.strictEqual(stalled.success, false);
    assert.strictEqual(stalled.errorCode, 'STREAM_STALLED');
    assert.match(stalled.reason || '', /longest output gap 199990ms/);

    // Default policy: exit 0 + a real answer means the round keeps its work and
    // the long gap becomes a warning instead of a failure.
    const warned = runner.evaluateResponse({
      exitCode: 0,
      stdout: 'PONG',
      stderr: '',
      chunkCount: 2,
      stdoutBytes: 4,
      stderrBytes: 0,
      firstOutputAtMs: 10,
      lastOutputAtMs: 4_990_859,
      maxGapMs: 4_990_859,
      gapThresholdMs: 600_000,
    });
    assert.strictEqual(warned.success, true);
    assert.strictEqual(warned.errorCode, 'OK');
    assert.strictEqual(warned.stream.continuous, false);
    assert.strictEqual(warned.stream.gapExceeded, true);
    assert.strictEqual(warned.warnings?.length, 1);
    assert.strictEqual(warned.warnings?.[0].code, 'STREAM_GAP_EXCEEDED');
    assert.match(warned.warnings?.[0].message || '', /4990859ms/);

    // Host sleep is not agent silence: an 83-minute wall-clock gap whose whole
    // duration is a suspension window leaves an effective gap of ~0.
    const slept = runner.evaluateResponse({
      exitCode: 0,
      stdout: 'PONG',
      stderr: '',
      chunkCount: 2,
      stdoutBytes: 4,
      stderrBytes: 0,
      firstOutputAtMs: 10,
      lastOutputAtMs: 4_990_859,
      maxGapMs: 0,
      rawMaxGapMs: 4_990_859,
      excludedSuspensionMs: 4_990_859,
      suspensionCount: 1,
      gapThresholdMs: 600_000,
    });
    assert.strictEqual(slept.success, true);
    assert.strictEqual(slept.errorCode, 'OK');
    assert.strictEqual(slept.stream.continuous, true);
    assert.strictEqual(slept.stream.gapExceeded, false);
    assert.strictEqual(slept.stream.maxGapMs, 0);
    assert.strictEqual(slept.stream.rawMaxGapMs, 4_990_859);
    assert.strictEqual(slept.stream.excludedSuspensionMs, 4_990_859);
    assert.strictEqual(slept.stream.suspensionCount, 1);
    assert.strictEqual(slept.warnings, undefined);

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

    // Legacy non-streaming CLIs emit one final object after a long run; a
    // single substantive chunk is still a valid response when streaming is
    // explicitly not expected. Claude Code no longer takes this path.
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

  test('Claude Code stream-json keeps the silence watchdog fed and yields the final answer', () => {
    const claudeStream = [
      '{"type":"system","subtype":"init","session_id":"sess-1","tools":[]}',
      '{"type":"system","subtype":"thinking_tokens","estimated_tokens":7}',
      '{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"plan"}]}}',
      '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"PONG"}]}}',
      '{"type":"result","subtype":"success","is_error":false,"result":"PONG","usage":{"input_tokens":15,"output_tokens":3}}',
    ].join('\n');

    assert.strictEqual(runner.extractAnswerOnly(claudeStream), 'PONG');

    const ok = runner.evaluateResponse({
      exitCode: 0,
      stdout: claudeStream,
      stderr: '',
      chunkCount: 5,
      stdoutBytes: claudeStream.length,
      stderrBytes: 0,
      firstOutputAtMs: 900,
      lastOutputAtMs: 58_000,
      maxGapMs: 12_000,
      gapThresholdMs: DEFAULT_SILENCE_TIMEOUT_MS,
    });
    assert.strictEqual(ok.success, true);
    assert.strictEqual(ok.errorCode, 'OK');
    assert.strictEqual(ok.stream.chunkCount, 5);
    assert.strictEqual(ok.stream.hasActualOutput, true);
  });

  test('Claude Code stream killed before the result event still yields assistant text', () => {
    const killed = [
      '{"type":"system","subtype":"init","session_id":"sess-1"}',
      '{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"partial answer"}]}}',
    ].join('\n');

    // No result event (watchdog kill): the assistant text is the only real
    // output, and the status/metadata events must never be mistaken for it.
    assert.strictEqual(runner.extractAnswerOnly(killed), 'partial answer');
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
    // Detection reads the agents' native session stores under the user home, so
    // the fixtures live in a throwaway home instead of a pre-existing install.
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-detect-session-'));
    const workspace = path.join(home, 'workspace');
    const claudeSession = '7f3c9a1e-0b2d-4c5e-8f60-112233445566';
    const antigravitySession = 'anti-session-fixture';
    const previousHome = process.env.HOME;
    const previousUserProfile = process.env.USERPROFILE;

    try {
      fs.mkdirSync(path.join(home, '.claude', 'projects', 'workspace-project'), { recursive: true });
      fs.writeFileSync(
        path.join(home, '.claude', 'projects', 'workspace-project', `${claudeSession}.jsonl`),
        '{}\n',
      );
      const conversations = path.join(home, '.gemini', 'antigravity-cli', 'conversations');
      fs.mkdirSync(conversations, { recursive: true });
      fs.writeFileSync(path.join(conversations, `${antigravitySession}.db`), 'fixture');

      process.env.HOME = home;
      process.env.USERPROFILE = home;

      const detectedClaude = detector.detectActiveSession('claude_code', workspace);
      assert.strictEqual(detectedClaude, claudeSession);

      const detectedAnti = detector.detectActiveSession('antigravity', undefined, [
        'non-existent-excluded-session',
      ]);
      assert.strictEqual(detectedAnti, antigravitySession);
    } finally {
      if (previousHome === undefined) delete process.env.HOME;
      else process.env.HOME = previousHome;
      if (previousUserProfile === undefined) delete process.env.USERPROFILE;
      else process.env.USERPROFILE = previousUserProfile;
      fs.rmSync(home, { recursive: true, force: true });
    }
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

  test('execute records wall-clock and suspension-adjusted gap metrics separately', async () => {
    const gapRunner = new AutonomousRunner(detector);
    gapRunner.buildCommand = () => ({
      command: process.execPath,
      args: ['-e', 'process.stdout.write("done\\n")'],
      useShell: false,
    });

    const result = await gapRunner.executeOnce({
      agent: 'antigravity',
      prompt: 'x',
      isRework: false,
      receiverSessionId: 'session-gap-metrics',
    });

    assert.strictEqual(result.assessment?.success, true);
    const stream = result.assessment!.stream;
    assert.strictEqual(typeof stream.rawMaxGapMs, 'number');
    assert.ok((stream.rawMaxGapMs ?? 0) >= stream.maxGapMs);
    assert.strictEqual(stream.excludedSuspensionMs, 0);
    assert.strictEqual(stream.suspensionCount, 0);
    assert.strictEqual(stream.gapExceeded, false);
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

  test('execute aborts retry loop and does not spawn subsequent attempts when cancelled during backoff sleep', async () => {
    const retryRunner = new AutonomousRunner(detector);
    let attempts = 0;
    retryRunner.executeOnce = async () => {
      attempts++;
      return {
        exitCode: 1,
        stdout: '',
        stderr: 'dsh: TRANSPORT: Connection error.',
        extractedOutput: '',
        durationMs: 5,
        error: 'dsh: TRANSPORT: Connection error.',
      };
    };

    const execution = retryRunner.execute({
      agent: 'deepseek_harness',
      prompt: 'will be cancelled',
      isRework: false,
      receiverSessionId: '',
      taskThreadId: 'thread_cancel_sleep',
      maxRetries: 3,
      retryBackoffMs: 500,
    });

    // Wait until attempt 1 fails and enters cancellable backoff sleep
    await new Promise((r) => setTimeout(r, 60));
    retryRunner.cancelThread('thread_cancel_sleep', 'delete', 'user deleted task', 'test');

    const result = await execution;
    assert.strictEqual(result.interruption?.kind, 'deleted');
    assert.strictEqual(result.interruption?.reason, 'user deleted task');
    assert.strictEqual(attempts, 1, 'retry runner must never spawn attempt 2 after cancellation');
  });

  test('pre-spawn cancellation check prevents any process from being spawned', async () => {
    const runner = new AutonomousRunner(detector);
    let spawned = false;
    runner.buildCommand = () => {
      spawned = true;
      return { command: process.execPath, args: ['-e', 'process.exit(0)'], useShell: false };
    };

    // Pre-cancel before execute is called
    runner.cancelThread('thread_precancel', 'pause', 'pre-spawn pause', 'test');

    const result = await runner.execute({
      agent: 'antigravity',
      prompt: 'should never run',
      isRework: false,
      receiverSessionId: '',
      taskThreadId: 'thread_precancel',
      taskRound: 1,
    });

    assert.strictEqual(result.interruption?.kind, 'paused');
    assert.strictEqual(result.interruption?.reason, 'pre-spawn pause');
    assert.strictEqual(spawned, false, 'process must never be spawned when cancelled upfront');
  });

  test('a pause remembered without a child must be cleared before a resume can run', async () => {
    const resumeRunner = new AutonomousRunner(detector);
    let spawns = 0;
    resumeRunner.buildCommand = () => {
      spawns++;
      return {
        command: process.execPath,
        args: ['-e', 'console.log("resumed")'],
        useShell: false,
      };
    };

    // A pause that landed while the round had no child yet (queued / pre-spawn).
    // No settling round ever consumes it, so it stays remembered.
    resumeRunner.cancelThread('thread_remembered_pause', 'pause', 'queued pause', 'test');

    const blocked = await resumeRunner.execute({
      agent: 'antigravity',
      prompt: 'resume me',
      isRework: true,
      receiverSessionId: '',
      taskThreadId: 'thread_remembered_pause',
      taskRound: 1,
      maxRetries: 0,
    });
    assert.strictEqual(blocked.interruption?.kind, 'paused');
    assert.strictEqual(spawns, 0, 'a remembered pause must still stop the run it was raised for');

    // Resuming re-arms the same task; the resume path clears the remembered pause.
    resumeRunner.clearPendingCancellation('thread_remembered_pause');
    const resumed = await resumeRunner.execute({
      agent: 'antigravity',
      prompt: 'resume me',
      isRework: true,
      receiverSessionId: '',
      taskThreadId: 'thread_remembered_pause',
      taskRound: 1,
      maxRetries: 0,
    });

    assert.strictEqual(resumed.interruption, undefined, 'the resumed round must actually run');
    assert.strictEqual(resumed.exitCode, 0);
    assert.strictEqual(spawns, 1);
  });

  test('a runner holding the task store reports a paused or deleted task as cancelled', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-runner-store-'));
    const store = new TaskStore(dir);
    const now = new Date().toISOString();
    const storeRunner = new AutonomousRunner(
      detector,
      undefined,
      new CancellationRegistry(dir),
      store,
    );

    try {
      store.upsertTask({
        threadId: 'thread_store_1',
        title: 'store-driven cancellation',
        initiatorAgent: 'codex',
        targetAgent: 'antigravity',
        status: 'running',
        currentRound: 1,
        createdAt: now,
        updatedAt: now,
        receiverSessionId: '',
        workspacePath: process.cwd(),
        summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
        rounds: [
          {
            round: 1,
            type: 'initial',
            prompt: 'do the work',
            status: 'running',
            durationMs: 0,
            timestamp: now,
          },
        ],
      } as DelegationThread);

      assert.strictEqual(
        storeRunner.checkPendingCancellation('thread_store_1', true),
        undefined,
        'a healthy running task is not cancelled',
      );

      const paused = store.getTask('thread_store_1')!;
      paused.status = 'paused';
      paused.interruption = {
        kind: 'paused',
        reason: 'paused in store',
        at: now,
        by: 'user',
      };
      store.upsertTask(paused);

      const pauseRequest = storeRunner.checkPendingCancellation('thread_store_1', true);
      assert.strictEqual(pauseRequest?.action, 'pause');
      assert.strictEqual(pauseRequest?.reason, 'paused in store');

      store.deleteTask('thread_store_1');
      assert.strictEqual(
        storeRunner.checkPendingCancellation('thread_store_1', true)?.action,
        'delete',
        'a task deleted from the store aborts its running round',
      );
      assert.strictEqual(
        storeRunner.checkPendingCancellation('thread_store_1', false),
        undefined,
        'a task that never existed is not reported as deleted',
      );
    } finally {
      store.dispose();
      fs.rmSync(dir, { recursive: true, force: true });
    }
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

