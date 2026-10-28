import { test, describe, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  accumulateTokenUsage,
  aggregateThreadTokenUsage,
  collectTokenUsage,
  estimateOutputTokens,
  formatTokenCount,
} from '../bridge/token-usage-collector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { TaskStore } from '../bridge/task-store';
import { ContextCompactor } from '../bridge/context-compactor';
import { DelegationRound, DelegationThread, TokenUsageMetrics } from '../bridge/types';

const cleanups: Array<() => void> = [];

afterEach(() => {
  while (cleanups.length > 0) {
    try {
      cleanups.pop()!();
    } catch {}
  }
});

function makeStore(): { store: TaskStore; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-token-'));
  const store = new TaskStore(dir);
  cleanups.push(() => {
    store.dispose();
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {}
  });
  return { store, dir };
}

function makeThread(rounds: DelegationRound[]): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: `thread_token_${Math.random().toString(16).slice(2, 8)}`,
    title: 'token fixture',
    initiatorAgent: 'claude_code',
    targetAgent: 'codex',
    status: 'waiting_review',
    currentRound: rounds.length,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'session-12345678',
    workspacePath: process.cwd(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds,
  };
}

describe('token usage - per-agent normalization', () => {
  test('DSH: cached prompt tokens are NOT part of inputTokens and must be added', () => {
    // Real shape and numbers taken from a bridge execution log:
    // {"type":"status","phase":"step_end","usage":{...}}
    const stdout = [
      '{"type":"status","phase":"start","turn":1,"step":1}',
      '{"type":"status","phase":"step_end","turn":1,"step":1,"usage":{"inputTokens":2266,"outputTokens":1345,"totalTokens":657435,"cacheReadTokens":653824}}',
      '{"type":"final","text":"done"}',
    ].join('\n');

    const usage = collectTokenUsage({ agent: 'deepseek_harness', stdout });

    assert.strictEqual(usage.source, 'native_reported');
    assert.strictEqual(usage.available, true);
    assert.strictEqual(usage.uncachedInputTokens, 2_266);
    assert.strictEqual(usage.cacheReadTokens, 653_824);
    assert.strictEqual(usage.promptTokens, 656_090);
    assert.strictEqual(usage.outputTokens, 1_345);
    // The v1.0.0 formula (`input + output` = 3,611) must NOT come back.
    assert.strictEqual(usage.totalTokens, 657_435);
    assert.notStrictEqual(usage.totalTokens, usage.uncachedInputTokens + usage.outputTokens);
    assert.strictEqual(usage.nativeTotalTokens, 657_435);
  });

  test('Claude: input excludes cache, so prompt = input + cache read + cache write', () => {
    // Pretty-printed on purpose: the whole buffer must parse as one object.
    const stdout = JSON.stringify(
      {
        type: 'result',
        subtype: 'success',
        total_cost_usd: 0.0152,
        num_turns: 3,
        usage: {
          input_tokens: 8_420,
          output_tokens: 650,
          cache_read_input_tokens: 6_200,
          cache_creation_input_tokens: 1_200,
        },
      },
      null,
      2,
    );

    const usage = collectTokenUsage({ agent: 'claude_code', stdout });

    assert.strictEqual(usage.source, 'native_reported');
    assert.strictEqual(usage.uncachedInputTokens, 8_420);
    assert.strictEqual(usage.cacheReadTokens, 6_200);
    assert.strictEqual(usage.cacheWriteTokens, 1_200);
    assert.strictEqual(usage.promptTokens, 15_820);
    assert.strictEqual(usage.totalTokens, 16_470);
    assert.strictEqual(usage.costUsd, 0.0152);
    assert.strictEqual(usage.costSource, 'native');
  });

  test('Antigravity: per-step usage is summed and the conversation-total result is ignored', () => {
    const stdout = [
      '{"event":"step_update","step_update":{"usage":{"input_tokens":1000,"cache_read_tokens":200,"output_tokens":50,"thinking_tokens":20,"total_tokens":1050}}}',
      '{"event":"step_update","step_update":{"usage":{"input_tokens":1500,"cache_read_tokens":300,"output_tokens":60,"thinking_tokens":30,"total_tokens":1560}}}',
      // A conversation total (13 turns, millions of tokens). Adding this would
      // double count the whole session.
      '{"event":"result","result":{"usage":{"input_tokens":9999999,"output_tokens":999999,"total_tokens":10999998}}}',
    ].join('\n');

    const usage = collectTokenUsage({ agent: 'antigravity', stdout });

    assert.strictEqual(usage.uncachedInputTokens, 2_000); // (1000-200) + (1500-300)
    assert.strictEqual(usage.cacheReadTokens, 500);
    assert.strictEqual(usage.promptTokens, 2_500);
    assert.strictEqual(usage.outputTokens, 110);
    assert.strictEqual(usage.reasoningTokens, 50); // subset, never added again
    assert.strictEqual(usage.totalTokens, 2_610);
    assert.strictEqual(usage.nativeTotalTokens, 2_610);
  });

  test('Codex: cached and cache-write tokens are subsets of input; reasoning is not added twice', () => {
    const stdout = [
      '{"type":"thread.started","thread_id":"01a1549a-0034-7843-8af8-846a650058eb"}',
      '{"type":"turn.completed","usage":{"input_tokens":1000,"cached_input_tokens":400,"cache_write_input_tokens":100,"output_tokens":200,"reasoning_output_tokens":50,"total_tokens":1200}}',
    ].join('\n');

    const usage = collectTokenUsage({ agent: 'codex', stdout });

    assert.strictEqual(usage.uncachedInputTokens, 500);
    assert.strictEqual(usage.cacheReadTokens, 400);
    assert.strictEqual(usage.cacheWriteTokens, 100);
    assert.strictEqual(usage.promptTokens, 1_000);
    assert.strictEqual(usage.outputTokens, 200);
    assert.strictEqual(usage.reasoningTokens, 50);
    assert.strictEqual(usage.totalTokens, 1_200);
  });

  test('interrupted rounds keep native per-step usage but are flagged partial', () => {
    const stdout =
      '{"type":"status","phase":"step_end","usage":{"inputTokens":100,"outputTokens":20,"totalTokens":150,"cacheReadTokens":30}}';
    const usage = collectTokenUsage({
      agent: 'deepseek_harness',
      stdout,
      interrupted: true,
    });
    assert.strictEqual(usage.totalTokens, 150);
    assert.strictEqual(usage.partial, true);
  });
});

describe('token usage - fallbacks', () => {
  test('no native usage: estimate the output only, never the input', () => {
    const usage = collectTokenUsage({
      agent: 'codex',
      stdout: 'plain text, no usage event',
      finalOutput: 'hello world',
    });

    assert.strictEqual(usage.source, 'estimated');
    assert.strictEqual(usage.available, true);
    assert.strictEqual(usage.partial, true);
    assert.strictEqual(usage.promptTokens, 0);
    assert.strictEqual(usage.uncachedInputTokens, 0);
    assert.ok(usage.outputTokens > 0);
    assert.match(usage.notes || '', /input tokens unknown/i);
  });

  test('nothing measured is unavailable, not free', () => {
    const usage = collectTokenUsage({ agent: 'claude_code', stdout: '' });
    assert.strictEqual(usage.source, 'unavailable');
    assert.strictEqual(usage.available, false);
    assert.strictEqual(usage.totalTokens, 0);
  });

  test('output estimation is a rough mixed-script ratio', () => {
    assert.strictEqual(estimateOutputTokens(''), 0);
    const ascii = estimateOutputTokens('x'.repeat(38));
    assert.strictEqual(ascii, 10);
    const cjk = estimateOutputTokens('测'.repeat(15));
    assert.strictEqual(cjk, 10);
  });

  test('formatTokenCount keeps numbers readable', () => {
    assert.strictEqual(formatTokenCount(undefined), '—');
    assert.strictEqual(formatTokenCount(0), '0');
    assert.strictEqual(formatTokenCount(950), '950');
    assert.strictEqual(formatTokenCount(23_450), '23k');
    assert.strictEqual(formatTokenCount(1_500_000), '1.50M');
  });
});

describe('token usage - accumulation and rollup', () => {
  const usage = (total: number, prompt: number, output: number): TokenUsageMetrics => ({
    uncachedInputTokens: prompt,
    cacheReadTokens: 0,
    cacheWriteTokens: 0,
    promptTokens: prompt,
    outputTokens: output,
    totalTokens: total,
    agent: 'codex',
    source: 'native_reported',
    available: true,
    attempts: 1,
    collectedAt: new Date().toISOString(),
  });

  test('retries add up and record the attempt count', () => {
    const merged = accumulateTokenUsage(usage(100, 80, 20), usage(50, 40, 10));
    assert.ok(merged);
    assert.strictEqual(merged!.totalTokens, 150);
    assert.strictEqual(merged!.attempts, 2);
    assert.strictEqual(merged!.source, 'native_reported');
  });

  test('an estimate cannot overwrite a native measurement', () => {
    const native = usage(100, 80, 20);
    const estimate: TokenUsageMetrics = {
      ...native,
      source: 'estimated',
      promptTokens: 0,
      uncachedInputTokens: 0,
    };
    const merged = accumulateTokenUsage(native, estimate);
    assert.strictEqual(merged!.source, 'estimated'); // mixed data is conservative
    assert.strictEqual(merged!.available, true);
  });

  test('thread rollup sums rounds, splits by agent/model and flags partial data', () => {
    const rounds: DelegationRound[] = [
      {
        round: 1,
        type: 'initial',
        prompt: 'p1',
        stamp: undefined as any,
        durationMs: 1,
        status: 'completed',
        timestamp: new Date().toISOString(),
        executorAgent: 'codex',
        executorModel: 'gpt-6-sol',
        tokenUsage: usage(1_200, 1_000, 200),
      } as any,
      {
        round: 2,
        type: 'rework',
        prompt: 'p2',
        durationMs: 1,
        status: 'completed',
        timestamp: new Date().toISOString(),
        executorAgent: 'claude_code',
        executorModel: 'sonnet',
        tokenUsage: usage(800, 600, 200),
      },
      {
        round: 3,
        type: 'rework',
        prompt: 'p3',
        durationMs: 1,
        status: 'failed',
        timestamp: new Date().toISOString(),
        // no tokenUsage: unmeasured round
      },
    ];

    const summary = aggregateThreadTokenUsage(rounds);
    assert.strictEqual(summary.totalTokens, 2_000);
    assert.strictEqual(summary.totalPromptTokens, 1_600);
    assert.strictEqual(summary.byAgent.codex, 1_200);
    assert.strictEqual(summary.byAgent.claude_code, 800);
    assert.strictEqual(summary.byModel['gpt-6-sol'], 1_200);
    assert.strictEqual(summary.measuredRounds, 2);
    assert.strictEqual(summary.totalRounds, 3);
    assert.strictEqual(summary.isPartialOrEstimated, true);
  });

  test('a retried round seeds the runner with its previous spend (no overwrite)', async () => {
    const runner = new AutonomousRunner({} as any);
    const calls: number[] = [];
    (runner as any).executeOnce = async (options: any) => {
      calls.push(1);
      assert.ok(options.previousTokenUsage, 'the seed must be forwarded to the attempt');
      return {
        exitCode: 0,
        stdout: 'ok',
        stderr: '',
        extractedOutput: 'ok',
        durationMs: 1,
        assessment: { success: true, errorCode: 'OK', exitCode: 0, stream: {} },
        tokenUsage: usage(50, 40, 10),
      };
    };

    const result = await runner.execute({
      agent: 'codex',
      prompt: 'again',
      isRework: true,
      receiverSessionId: 'session-12345678',
      previousTokenUsage: usage(1_000, 800, 200),
    });

    assert.strictEqual(calls.length, 1);
    assert.strictEqual(result.tokenUsage?.totalTokens, 1_050);
    assert.strictEqual(result.tokenUsage?.attempts, 2);
  });
});

describe('token usage - durability', () => {
  const usage = (total: number): TokenUsageMetrics => ({
    uncachedInputTokens: total,
    cacheReadTokens: 0,
    cacheWriteTokens: 0,
    promptTokens: total,
    outputTokens: 0,
    totalTokens: total,
    agent: 'codex',
    source: 'native_reported',
    available: true,
    attempts: 1,
    collectedAt: new Date().toISOString(),
  });

  test('lagged context compaction keeps the token metadata', () => {
    const { store } = makeStore();
    const thread = makeThread([
      {
        round: 1,
        type: 'initial',
        prompt: 'p1',
        output: 'x'.repeat(4_000),
        durationMs: 1,
        status: 'completed',
        timestamp: new Date().toISOString(),
        executorAgent: 'codex',
        executorModel: 'gpt-6-sol',
        tokenUsage: usage(1_234),
      },
      {
        round: 2,
        type: 'rework',
        prompt: 'p2',
        output: 'current',
        durationMs: 1,
        status: 'completed',
        timestamp: new Date().toISOString(),
      },
    ]);
    store.upsertTask(thread);

    new ContextCompactor(store, { enabled: true, minCompactionChars: 100 }).compactPrecedingRound({
      threadId: thread.threadId,
      currentRoundNumber: 2,
    });

    const first = store.getTask(thread.threadId)!.rounds.find((r) => r.round === 1)!;
    assert.strictEqual(first.isCompacted, true);
    assert.strictEqual(first.tokenUsage?.totalTokens, 1_234);
    assert.strictEqual(first.executorAgent, 'codex');
    assert.strictEqual(first.executorModel, 'gpt-6-sol');
  });

  test('a stale writer cannot erase the token metadata', () => {
    const { store } = makeStore();
    const thread = makeThread([
      {
        round: 1,
        type: 'initial',
        prompt: 'p1',
        output: 'o1',
        durationMs: 1,
        status: 'completed',
        timestamp: new Date().toISOString(),
        executorAgent: 'codex',
        tokenUsage: usage(777),
      },
    ]);
    store.upsertTask(thread);

    // A process that loaded the thread before the measurement writes it back.
    const stale = JSON.parse(JSON.stringify(thread)) as DelegationThread;
    stale.rounds[0].tokenUsage = undefined;
    stale.rounds[0].executorAgent = undefined;
    store.upsertTask(stale);

    const stored = store.getTask(thread.threadId)!.rounds[0];
    assert.strictEqual(stored.tokenUsage?.totalTokens, 777);
    assert.strictEqual(stored.executorAgent, 'codex');
  });
});
