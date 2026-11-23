import { testWorkspace } from './workspace-fixture';
import { test, describe, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, spawnSync } from 'child_process';

import {
  accumulateTokenUsage,
  appendTokenUsageComponent,
  aggregateThreadTokenUsage,
  collectTokenUsage,
  estimateOutputTokens,
  formatTokenCount,
} from '../bridge/token-usage-collector';
import { AutonomousRunner } from '../bridge/autonomous-runner';
import { TaskStore } from '../bridge/task-store';
import { ContextCompactor } from '../bridge/context-compactor';
import { taskArtifactBase, taskWorkerLogDir } from '../bridge/round-log';
import { CancellationRegistry } from '../bridge/cancellation-registry';
import { NativeSessionLock } from '../bridge/native-session-lock';
import { NativeSessionCompactor, resolveCodexProviderIdentity } from '../bridge/native-session-compactor';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import { DelegationRound, DelegationThread, TokenUsageMetrics } from '../bridge/types';
import { SessionOwnershipStore, nativeSessionNamespace } from '../bridge/session-ownership';
import { getProcessBirthIdAsync, probeProcessIdentity } from '../bridge/platform';

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
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    reviewerSessionCaptured: true,
    reviewerSessionId: ('reviewer-' + (`thread_token_${Math.random().toString(16).slice(2, 8)}`)),
    callerSessionId: ('reviewer-' + (`thread_token_${Math.random().toString(16).slice(2, 8)}`)),
    threadId: `thread_token_${Math.random().toString(16).slice(2, 8)}`,
    title: 'token fixture',
    initiatorAgent: 'claude_code',
    targetAgent: 'codex',
    status: 'reviewing',
    currentRound: rounds.length,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'session-12345678',
    workspace: testWorkspace(process.cwd()),
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

  test('legacy Claude result cost is counted once', () => {
    const usage = collectTokenUsage({
      agent: 'claude_code',
      stdout: JSON.stringify({
        type: 'result', subtype: 'success', total_cost_usd: 0.0152,
        usage: { input_tokens: 8420, output_tokens: 650, cache_read_input_tokens: 6200, cache_creation_input_tokens: 1200 },
      }),
    });
    const summary = aggregateThreadTokenUsage([{
      round: 1, type: 'initial', prompt: 'p', durationMs: 1, status: 'completed',
      timestamp: new Date().toISOString(), executorAgent: 'claude_code', tokenUsage: usage,
    }]);
    assert.equal(summary.totalCostUsd, 0.0152);
  });

  test('a reviewer compaction component is counted once and grouped by its own agent/model', () => {
    const developer = usage(1_200, 1_000, 200);
    developer.agent = 'codex';
    developer.model = 'gpt-6-luna';
    const combined = appendTokenUsageComponent(developer, {
      id: 'compact-operation-1', kind: 'native_compaction', role: 'reviewer',
      agent: 'claude_code', model: 'sonnet', uncachedInputTokens: 300,
      cacheReadTokens: 200, cacheWriteTokens: 0, outputTokens: 50,
      source: 'native_reported', available: true,
    }, 'round-1-business');
    const round: DelegationRound = {
      round: 1, type: 'initial', prompt: 'p', durationMs: 1,
      status: 'completed', timestamp: new Date().toISOString(),
      executorAgent: 'codex', executorModel: 'gpt-6-luna', tokenUsage: combined,
    };
    const summary = aggregateThreadTokenUsage([round]);
    assert.equal(summary.totalTokens, 1_750);
    assert.equal(summary.byAgent.codex, 1_200);
    assert.equal(summary.byAgent.claude_code, 550);
    assert.equal(summary.byModel['gpt-6-luna'], 1_200);
    assert.equal(summary.byModel.sonnet, 550);
    assert.equal(summary.totalCostUsd, undefined);
  });

  test('business attempts after maintenance remain in the durable component rollup', () => {
    const business = { ...usage(110, 100, 10), model: 'gpt-6-luna' };
    const withMaintenance = appendTokenUsageComponent(business, {
      id: 'operation-1', kind: 'native_compaction', role: 'reviewer', agent: 'claude_code', model: 'sonnet',
      uncachedInputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 5,
      costUsd: 0.01, source: 'native_reported', available: true,
    }, 'legacy-business-round-1');
    const first = accumulateTokenUsage(withMaintenance, { ...usage(8, 7, 1), model: 'gpt-6-luna' }, 'attempt-1', 'legacy-business-round-1', 'developer')!;
    const second = accumulateTokenUsage(first, { ...usage(4, 3, 1), model: 'gpt-6-luna' }, 'attempt-2', 'legacy-business-round-1', 'developer')!;
    const round = { round: 1, type: 'rework', prompt: 'p', durationMs: 1, status: 'completed', timestamp: new Date().toISOString(), executorAgent: 'codex', executorModel: 'gpt-6-luna', tokenUsage: second } as DelegationRound;
    assert.equal(aggregateThreadTokenUsage([round]).totalTokens, 147);
    assert.equal(aggregateThreadTokenUsage([round]).byAgent.claude_code, 25);
    assert.equal(aggregateThreadTokenUsage([round]).byModel['gpt-6-luna'], 122);
    assert.equal(second.costUsd, 0.01);
    assert.deepEqual(second.components?.map((item) => item.id).sort(), ['attempt-1', 'attempt-2', 'legacy-business-round-1', 'operation-1']);
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
  test('changing the Codex configured provider invalidates a certified capability identity', () => {
    const { store, dir } = makeStore();
    const first = resolveCodexProviderIdentity('model_provider = "gateway_a"\n[model_providers.gateway_a]\nname = "Gateway A"\nbase_url = "https://gateway-a.invalid/v1"', {});
    const second = resolveCodexProviderIdentity('model_provider = "gateway_b"\n[model_providers.gateway_b]\nname = "Gateway B"\nbase_url = "https://gateway-b.invalid/v1"', {});
    assert.ok(first && second);
    const common = { agent: 'codex' as const, executablePath: 'C:/codex.exe', executableVersion: 'codex-cli 1.0', model: 'gpt-6-luna' };
    const certifiedIdentity = { ...common, ...first };
    const changedIdentity = { ...common, ...second };
    fs.writeFileSync(path.join(dir, 'native-session-capabilities.json'), JSON.stringify([{ ...certifiedIdentity, status: 'certified' }]));
    const compactor = new NativeSessionCompactor(store, {} as any);
    assert.equal((compactor as any).readCapabilityRecord(certifiedIdentity), 'certified');
    assert.equal((compactor as any).readCapabilityRecord(changedIdentity), 'needs_validation');
    assert.equal(resolveCodexProviderIdentity(undefined, {}), undefined, 'unreadable configuration is not treated as subscription default');
    const routeOne = resolveCodexProviderIdentity('model_provider = "openai"\n[model_providers.openai]\nbase_url = "https://route-one.example/v1"', {});
    const routeTwo = resolveCodexProviderIdentity('model_provider = "openai"\n[model_providers.openai]\nbase_url = "https://route-two.example/v1"', {});
    assert.ok(routeOne && routeTwo);
    assert.notEqual(routeOne.providerRoute, routeTwo.providerRoute, 'different configured endpoints cannot share a certified identity');
    assert.equal(resolveCodexProviderIdentity("model_provider = 'openai'\n[model_providers.openai]\nbase_url = 'https://route-one.example/v1'", {}), undefined);
    assert.equal(resolveCodexProviderIdentity('model_provider = "openai"\n[model_providers."openai"]\nbase_url = "https://route-one.example/v1"', {}), undefined);
    const quotedRouteOne = resolveCodexProviderIdentity('model_provider = "openai"\n["model_providers"."openai"]\nbase_url = "https://route-one.example/v1"', {});
    const quotedRouteTwo = resolveCodexProviderIdentity('model_provider = "openai"\n["model_providers"."openai"]\nbase_url = "https://route-two.example/v1"', {});
    assert.ok(!quotedRouteOne || !quotedRouteTwo || quotedRouteOne.providerRoute !== quotedRouteTwo.providerRoute);
    assert.equal(resolveCodexProviderIdentity('model_provider = "openai"\n[custom]\nbase_url = "https://unknown.example/v1"', {}), undefined);
    assert.deepEqual(resolveCodexProviderIdentity('model_provider = "openai"\n', {}), { providerId: 'openai', providerRoute: 'openai' });
    assert.equal(resolveCodexProviderIdentity('"model_provider" = "openai"\n[model_providers.openai]\nbase_url = "https://route-one.example/v1"', {}), undefined);
  });

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

  test('maintenance and multiple retry attempts survive store reload without re-billing stale snapshots', () => {
    const { store, dir } = makeStore();
    const thread = makeThread([{
      round: 1, type: 'initial', prompt: 'p', durationMs: 1, status: 'completed', timestamp: new Date().toISOString(),
      executorAgent: 'codex', executorModel: 'gpt-6-luna', tokenUsage: {
        uncachedInputTokens: 100, cacheReadTokens: 0, cacheWriteTokens: 0, promptTokens: 100, outputTokens: 10, totalTokens: 110,
        agent: 'codex', model: 'gpt-6-luna', source: 'native_reported', available: true, attempts: 1, collectedAt: new Date().toISOString(),
      },
    }]);
    store.upsertTask(thread);
    const saved = store.getTask(thread.threadId)!;
    const round = saved.rounds[0];
    round.tokenUsage = appendTokenUsageComponent(round.tokenUsage, {
      id: 'operation-1', kind: 'native_compaction', role: 'reviewer', agent: 'claude_code', model: 'sonnet',
      uncachedInputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 5,
      costUsd: 0.01, source: 'native_reported', available: true,
    }, 'legacy-business-round-1');
    round.tokenUsage = accumulateTokenUsage(round.tokenUsage, { ...usage(8), model: 'gpt-6-luna' }, 'attempt-1', 'legacy-business-round-1', 'developer');
    round.tokenUsage = accumulateTokenUsage(round.tokenUsage, { ...usage(4), model: 'gpt-6-luna' }, 'attempt-2', 'legacy-business-round-1', 'developer');
    store.upsertTask(saved);
    store.dispose();
    const reloaded = new TaskStore(dir);
    cleanups.push(() => reloaded.dispose());
    const afterReload = reloaded.getTask(thread.threadId)!;
    assert.equal(aggregateThreadTokenUsage(afterReload.rounds).totalTokens, 147);
    assert.equal(aggregateThreadTokenUsage(afterReload.rounds).totalCostUsd, 0.01);
    const stale = JSON.parse(JSON.stringify(saved)) as DelegationThread;
    stale.rounds[0].tokenUsage!.components = stale.rounds[0].tokenUsage!.components!.filter((item) => item.id !== 'attempt-2');
    reloaded.upsertTask(stale);
    assert.equal(aggregateThreadTokenUsage(reloaded.getTask(thread.threadId)!.rounds).totalTokens, 147);
  });

  test('TaskStore migrates legacy round usage once before merging a maintenance component', () => {
    const { store, dir } = makeStore();
    const business = usage(110);
    business.uncachedInputTokens = 100;
    business.promptTokens = 100;
    business.outputTokens = 10;
    const thread = makeThread([{
      round: 1, type: 'initial', prompt: 'p', output: 'done', durationMs: 1,
      status: 'completed', timestamp: new Date().toISOString(), tokenUsage: business,
    }]);
    store.upsertTask(thread);
    const task = store.getTask(thread.threadId)!;
    const round = task.rounds[0];
    round.tokenUsage = appendTokenUsageComponent(round.tokenUsage, {
      id: 'operation-1', kind: 'native_compaction', role: 'reviewer', agent: 'claude_code', model: 'sonnet',
      uncachedInputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 5,
      source: 'native_reported', available: true,
    }, 'legacy-business-round-1');
    store.upsertTask(task);

    const reopened = new TaskStore(dir);
    reopened.loadFromDisk();
    const persisted = reopened.getTask(thread.threadId)!.rounds[0].tokenUsage!;
    assert.equal(persisted.totalTokens, 135);
    assert.deepEqual(persisted.components?.map((component) => component.id).sort(), ['legacy-business-round-1', 'operation-1']);
    const stale = JSON.parse(JSON.stringify(thread)) as DelegationThread;
    stale.rounds[0].tokenUsage = appendTokenUsageComponent(business, {
      id: 'operation-1', kind: 'native_compaction', role: 'reviewer', agent: 'claude_code', model: 'sonnet',
      uncachedInputTokens: 1, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 1,
      source: 'native_reported', available: true,
    }, 'legacy-business-round-1');
    store.upsertTask(stale);
    assert.equal(store.getTask(thread.threadId)!.rounds[0].tokenUsage!.totalTokens, 135);
    reopened.dispose();
  });

  test('a stale task snapshot cannot restore the pre-retarget session cursor', () => {
    const { store } = makeStore();
    const thread = makeThread([{
      round: 1, type: 'initial', prompt: 'p', output: 'done', durationMs: 1,
      status: 'completed', timestamp: new Date().toISOString(), tokenUsage: usage(100),
      nativeSessionCompactions: [{
        id: 'old-operation', role: 'developer', agent: 'codex', sessionId: 'old-session',
        reason: 'manual', phase: 'succeeded', revision: 2, startedAt: new Date().toISOString(),
        recoveryPath: 'old.json', logPath: 'old.log',
      }],
    }]);
    thread.receiverSessionId = 'old-session';
    thread.receiverSessionCaptured = true;
    thread.nativeSessionCompaction = { developer: { revision: 2, sessionId: 'old-session', lastOperationId: 'old-operation', turnsSinceCompaction: 0 } };
    store.upsertTask(thread);
    const stale = JSON.parse(JSON.stringify(thread)) as DelegationThread;
    const current = JSON.parse(JSON.stringify(thread)) as DelegationThread;
    current.receiverSessionId = 'new-session';
    current.receiverSessionCaptured = false;
    current.retargets = [{
      fromAgent: 'codex', toAgent: 'codex', fromModel: 'gpt-6-luna', toModel: 'gpt-6-luna',
      sessionReset: true, previousReceiverSessionId: 'old-session', reason: 'test', at: new Date().toISOString(),
    }];
    current.nativeSessionCompaction = { developer: { revision: 3, sessionId: 'new-session', turnsSinceCompaction: 0 } };
    store.upsertTask(current);
    store.upsertTask(stale);
    const saved = store.getTask(thread.threadId)!;
    assert.equal(saved.receiverSessionId, 'new-session');
    assert.equal(saved.nativeSessionCompaction?.developer?.sessionId, 'new-session');
    assert.equal(saved.nativeSessionCompaction?.developer?.lastOperationId, undefined);
    assert.equal(saved.rounds[0].tokenUsage?.totalTokens, 100);
  });

  test('runner repeats pending recovery until a carried business turn succeeds and holds one session lease', async () => {
    const { store, dir } = makeStore();
    const sessionId = '01a1acc2-877d-71e3-9bf1-5e668c1f2183';
    const thread = makeThread([{
      round: 1, type: 'rework', prompt: 'full unmodified instruction', output: 'review evidence',
      durationMs: 1, status: 'completed', timestamp: new Date().toISOString(),
      nativeSessionCompactions: [{ id: 'op-recovery', role: 'developer', agent: 'codex', sessionId,
        reason: 'manual', phase: 'succeeded', revision: 2, startedAt: new Date().toISOString(),
        recoveryPath: path.join(taskWorkerLogDir(taskArtifactBase('thread_recovery', dir)), 'recovery.json'),
        logPath: path.join(taskWorkerLogDir(taskArtifactBase('thread_recovery', dir)), 'native.log') }],
    }]);
    thread.threadId = 'thread_recovery';
    thread.status = 'running';
    thread.receiverSessionId = sessionId;
    thread.receiverSessionCaptured = true;
    thread.targetModel = 'gpt-6-luna';
    thread.nativeSessionCompaction = { developer: { revision: 2, sessionId, lastOperationId: 'op-recovery',
      turnsSinceCompaction: 0, recoveryVersion: 'op-recovery' } };
    const recoveryPath = thread.rounds[0].nativeSessionCompactions![0].recoveryPath;
    fs.mkdirSync(path.dirname(recoveryPath), { recursive: true });
    fs.writeFileSync(recoveryPath, '{"constraints":"keep task identity"}', 'utf8');
    store.upsertTask(thread);
    const roleLock = new TaskRoleLock(dir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;

    const runner = new AutonomousRunner({} as any, 10_000, new CancellationRegistry(dir), store);
    const prompts: string[] = [];
    let attempt = 0;
    (runner as any).executeOnce = async (options: any) => {
      prompts.push(options.prompt);
      assert.equal(new NativeSessionLock(dir).acquire('codex', sessionId), undefined, 'business execution must hold the lease');
      attempt++;
      return attempt === 1
        ? { exitCode: 1, stdout: '', stderr: 'retryable test result', extractedOutput: '', durationMs: 1, assessment: { success: false }, actualSessionId: sessionId }
        : { exitCode: 0, stdout: 'done', stderr: '', extractedOutput: 'done', durationMs: 1,
            assessment: { success: true }, actualSessionId: sessionId };
    };
    const common = { agent: 'codex' as const, prompt: 'full unmodified instruction', isRework: true,
      receiverSessionId: sessionId, taskThreadId: thread.threadId, taskRound: 1, model: 'gpt-6-luna',
      maxRetries: 0 };
    await runner.execute(common);
    assert.match(prompts[0], /keep task identity/);
    assert.match(prompts[0], /full unmodified instruction/);
    assert.notEqual(store.getTask(thread.threadId)!.nativeSessionCompaction?.developer?.recoveryDeliveredVersion, 'op-recovery');

    await runner.execute(common);
    assert.match(prompts[1], /keep task identity/);
    const cursor = store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!;
    assert.equal(cursor.recoveryDeliveredVersion, 'op-recovery');
    assert.equal(cursor.turnsSinceCompaction, 1);
    const released = new NativeSessionLock(dir).acquire('codex', sessionId);
    assert.ok(released);
    new NativeSessionLock(dir).release(released!);
    roleLock.release(roleLease);
  });

  test('a recovered running maintenance operation becomes unknown and is never replayed', async () => {
    const { store, dir } = makeStore();
    const sessionId = '01a1acc2-877d-71e3-9bf1-5e668c1f2183';
    const recoveryPath = path.join(taskWorkerLogDir(taskArtifactBase('thread_recovered', dir)), 'crash.recovery.json');
    fs.mkdirSync(path.dirname(recoveryPath), { recursive: true });
    fs.writeFileSync(recoveryPath, '{"recover":true}', 'utf8');
    const thread = makeThread([{
      round: 1, type: 'rework', prompt: 'continue', output: 'prior work', durationMs: 1,
      status: 'completed', timestamp: new Date().toISOString(),
      nativeSessionCompactions: [{ id: 'crashed-op', role: 'developer', agent: 'codex', sessionId,
        reason: 'manual', phase: 'running', revision: 2, startedAt: new Date().toISOString(),
        recoveryPath, logPath: path.join(taskWorkerLogDir(taskArtifactBase('thread_recovered', dir)), 'crash.log') }],
    }]);
    thread.threadId = 'thread_recovered';
    thread.status = 'running'; thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.targetModel = 'gpt-6-luna';
    thread.nativeSessionCompaction = { developer: { sessionId, revision: 2, lastOperationId: 'crashed-op',
      turnsSinceCompaction: 0, recoveryVersion: 'crashed-op' } };
    store.upsertTask(thread);
    const roleLock = new TaskRoleLock(dir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;

    const prepared = await new NativeSessionCompactor(store, {} as any).prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.match(prepared.pauseReason || '', /could not be proven healthy/);
    const saved = store.getTask(thread.threadId)!;
    assert.equal(saved.rounds[0].nativeSessionCompactions?.[0].phase, 'unknown');
    assert.equal(saved.rounds[0].nativeSessionCompactions?.[0].revision, 3);
    assert.equal(saved.status, 'paused');
    assert.equal(saved.nativeSessionCompaction?.developer?.recoveryDeliveredVersion, undefined);
    const lease = new NativeSessionLock(dir).acquire('codex', sessionId);
    assert.ok(lease, 'the failed preparation releases its lease without launching a writer');
    new NativeSessionLock(dir).release(lease!);
    roleLock.release(roleLease);
  });

  test('unknown compaction uses a same-ID read-only probe and carries recovery without replay', async () => {
    const { store, dir } = makeStore();
    const sessionId = '1f7c52a3-76a4-4a9f-9b82-9de301c99874';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'complete task brief', durationMs: 1, status: 'running', timestamp: new Date().toISOString() }]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const logDir = taskWorkerLogDir(taskArtifactBase(thread.threadId, dir, thread.artifactDir));
    fs.mkdirSync(logDir, { recursive: true });
    const recoveryPath = path.join(logDir, 'thread_round_1.native_op.recovery.json');
    fs.writeFileSync(recoveryPath, '{"task":"complete task brief"}');
    thread.rounds[0].nativeSessionCompactions = [{ id: 'unknown-recovery-op', role: 'developer', agent: 'codex', sessionId, model: 'gpt-6-luna', reason: 'turn_budget', phase: 'running', revision: 2, startedAt: new Date().toISOString(), recoveryPath, logPath: path.join(logDir, 'operation.log') } as any];
    thread.nativeSessionCompaction = { developer: { sessionId, lastOperationId: 'unknown-recovery-op', recoveryVersion: 'unknown-recovery-op', turnsSinceCompaction: 4, revision: 2 } };
    store.upsertTask(thread);
    const roleLock = new TaskRoleLock(dir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'test', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    saveBridgePolicy({ nativeSessionCompaction: { mode: 'auto' } }, dir);
    let probes = 0; let compactions = 0;
    (compactor as any).codex = {
      probeResume: async (request: any) => { probes++; assert.equal(request.sessionId, sessionId); return true; },
      compact: async () => { compactions++; throw new Error('must not replay unknown compact'); },
    };
    const prepared = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.equal(probes, 1);
    assert.equal(compactions, 0);
    assert.equal(prepared.sessionId, sessionId);
    assert.equal(prepared.recoveryVersion, 'unknown-recovery-op');
    assert.match(prepared.recoveryPrefix || '', /complete task brief/);
    assert.equal(store.getTask(thread.threadId)!.status, 'running');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.lastOperationId, 'unknown-recovery-op');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.autoSuppressedOperationId, 'unknown-recovery-op');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.turnsSinceCompaction, 0);
    assert.equal(compactions, 0);
    new NativeSessionLock(dir).release(prepared.lease!);
    if (prepared.executionLease?.owned) prepared.executionLease.registry.end(prepared.executionLease.registration);
    prepared.executionLease?.registry.dispose();
    roleLock.release(roleLease);
    const secondRoleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    const second = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.equal(second.recoveryVersion, 'unknown-recovery-op', 'retries keep carrying the original pending version');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.lastOperationId, 'unknown-recovery-op');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.turnsSinceCompaction, 0);
    assert.equal(probes, 1, 'a healthy same-ID recovery probe is persisted and not repeated on the next prepare');
    assert.equal(compactions, 0, 'unknown recovery never reissues compact before a successful business turn');
    new NativeSessionLock(dir).release(second.lease!);
    if (second.executionLease?.owned) second.executionLease.registry.end(second.executionLease.registration);
    second.executionLease?.registry.dispose();
    roleLock.release(secondRoleLease);
  });

  test('auto mode also suppresses compaction after a healthy interrupted-operation probe', async () => {
    const { store, dir } = makeStore();
    const sessionId = 'c0fd2b1f-65bb-4f44-8f02-bff3c9a79fd0';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'brief', durationMs: 1, status: 'running', timestamp: new Date().toISOString() }]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const logDir = taskWorkerLogDir(taskArtifactBase(thread.threadId, dir, thread.artifactDir)); fs.mkdirSync(logDir, { recursive: true });
    const recoveryPath = path.join(logDir, 'interrupted.recovery.json'); fs.writeFileSync(recoveryPath, '{"brief":"brief"}');
    thread.rounds[0].nativeSessionCompactions = [{ id: 'interrupted-op', role: 'developer', agent: 'codex', sessionId, model: 'gpt-6-luna', reason: 'turn_budget', phase: 'interrupted', revision: 2, startedAt: new Date().toISOString(), recoveryPath, logPath: path.join(logDir, 'interrupted.log') } as any];
    thread.nativeSessionCompaction = { developer: { sessionId, lastOperationId: 'interrupted-op', recoveryVersion: 'interrupted-op', turnsSinceCompaction: 4, revision: 2 } };
    store.upsertTask(thread); saveBridgePolicy({ nativeSessionCompaction: { mode: 'auto' } }, dir);
    const roleLock = new TaskRoleLock(dir); const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'test', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let compactCalls = 0;
    (compactor as any).codex = { probeResume: async () => true, compact: async () => { compactCalls++; throw Error('must not compact'); } };
    const prepared = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.equal(compactCalls, 0);
    assert.equal(prepared.recoveryVersion, 'interrupted-op');
    assert.equal(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.autoSuppressedOperationId, 'interrupted-op');
    new NativeSessionLock(dir).release(prepared.lease!);
    if (prepared.executionLease?.owned) prepared.executionLease.registry.end(prepared.executionLease.registration);
    prepared.executionLease?.registry.dispose(); roleLock.release(roleLease);
  });

  test('a delivered unknown recovery can re-enter automatic compaction after four successful turns and reload', async () => {
    const { store, dir } = makeStore();
    const sessionId = '72c4a1cf-d335-43bb-8560-5cadd47371bc';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'full task brief', durationMs: 1, status: 'running', timestamp: new Date().toISOString() }]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const logDir = taskWorkerLogDir(taskArtifactBase(thread.threadId, dir, thread.artifactDir)); fs.mkdirSync(logDir, { recursive: true });
    const recoveryPath = path.join(logDir, 'unknown.recovery.json'); fs.writeFileSync(recoveryPath, '{"brief":"full task brief"}');
    thread.rounds[0].nativeSessionCompactions = [{ id: 'unknown-delivered-op', role: 'developer', agent: 'codex', sessionId, model: 'gpt-6-luna', reason: 'turn_budget', phase: 'unknown', revision: 3, startedAt: new Date().toISOString(), recoveryPath, logPath: path.join(logDir, 'unknown.log') } as any];
    thread.nativeSessionCompaction = { developer: { sessionId, lastOperationId: 'unknown-delivered-op', recoveryVersion: 'unknown-delivered-op', recoveryVerifiedOperationId: 'unknown-delivered-op', recoveryDeliveredVersion: 'unknown-delivered-op', autoSuppressedOperationId: 'unknown-delivered-op', turnsSinceCompaction: 0, revision: 4 } };
    store.upsertTask(thread); saveBridgePolicy({ nativeSessionCompaction: { mode: 'auto' } }, dir);
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'test', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let probes = 0; let compactions = 0;
    (compactor as any).codex = {
      probeResume: async () => { probes++; return true; },
      compact: async (request: any) => { compactions++; return { threadId: request.sessionId, nativeTurnId: 'new-native-turn', model: request.model, provider: 'test', processExitCode: 0, usage: undefined }; },
    };
    const staleBeforeDelivery = structuredClone(store.getTask(thread.threadId)!);
    for (let index = 0; index < 4; index++) compactor.recordBusinessSuccess(thread.threadId, 'developer', sessionId, `success-${index}`, 'unknown-delivered-op');
    store.upsertTask(staleBeforeDelivery);
    store.loadFromDisk();
    const afterReload = store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!;
    assert.equal(afterReload.recoveryDeliveredVersion, 'unknown-delivered-op');
    assert.equal(afterReload.turnsSinceCompaction, 4);
    const roleLock = new TaskRoleLock(dir); const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    const prepared = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.equal(probes, 0, 'a previously verified and delivered recovery is consumed without another probe');
    assert.equal(compactions, 1, 'normal auto threshold applies after recovery delivery and four new turns');
    assert.notEqual(store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.lastOperationId, 'unknown-delivered-op');
    new NativeSessionLock(dir).release(prepared.lease!);
    if (prepared.executionLease?.owned) prepared.executionLease.registry.end(prepared.executionLease.registration);
    prepared.executionLease?.registry.dispose(); roleLock.release(roleLease);
  });

  test('automatic failure probe binds its live PID and cancellation waits for probe exit', async () => {
    const { store, dir } = makeStore();
    const sessionId = '601d6973-203e-4f2a-bac6-747bd2dd4e9e';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'brief', durationMs: 1, status: 'running', timestamp: new Date().toISOString() }]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.nativeSessionCompaction = { developer: { sessionId, turnsSinceCompaction: 4, revision: 4 } };
    store.upsertTask(thread); saveBridgePolicy({ nativeSessionCompaction: { mode: 'auto' } }, dir);
    const roleLock = new TaskRoleLock(dir); const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    const detector = { resolveExecutable: () => process.execPath } as any;
    const compactor = new NativeSessionCompactor(store, detector, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'test', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let compactChild: ReturnType<typeof spawn> | undefined;
    let probeChild: ReturnType<typeof spawn> | undefined;
    let probeWasTracked = false;
    let probeWasCleared = false;
    (compactor as any).codex = {
      compact: async (request: any) => {
        compactChild = spawn(process.execPath, ['-e', 'setTimeout(()=>{},10000)'], { stdio: 'ignore', windowsHide: true });
        request.onChildPid(compactChild.pid);
        const nativeOwnerPath = fs.readdirSync(path.join(dir, 'native-sessions')).map((name) => path.join(dir, 'native-sessions', name))
          .find((file) => file.endsWith('.json') && !file.endsWith('.reaper') && JSON.parse(fs.readFileSync(file, 'utf8')).sessionId === sessionId)!;
        assert.equal(JSON.parse(fs.readFileSync(nativeOwnerPath, 'utf8')).childPid, compactChild.pid);
        const compactExited = new Promise<void>((resolve) => compactChild!.once('close', () => resolve()));
        try { compactChild.kill(); } catch {}
        await compactExited;
        request.onChildPid(undefined);
        throw new Error('ACK received, then the compact connection disconnected');
      },
      probeResume: async (request: any) => {
        probeChild = spawn(process.execPath, ['-e', 'setTimeout(()=>{},10000)'], { stdio: 'ignore', windowsHide: true });
        request.onChildPid(probeChild.pid);
        const native = new NativeSessionLock(dir).acquire('codex', sessionId);
        assert.equal(native, undefined, 'the active probe PID is persisted before another writer checks the session');
        const execution = new TaskExecutionRegistry(dir);
        assert.equal(execution.getOwner(thread.threadId)?.childPid, probeChild.pid);
        assert.equal(roleLock.getOwner(thread.threadId)?.childPid, probeChild.pid);
        probeWasTracked = true;
        new CancellationRegistry(dir).request(thread.threadId, 'pause', 'pause during recovery probe', 'test');
        const probeExited = new Promise<void>((resolve) => probeChild!.once('close', () => resolve()));
        try { probeChild.kill(); } catch {}
        await probeExited;
        request.onChildPid(undefined);
        assert.equal(execution.getOwner(thread.threadId)?.childPid, undefined);
        assert.equal(roleLock.getOwner(thread.threadId)?.childPid, undefined);
        probeWasCleared = true;
        execution.dispose();
        return false;
      },
    };
    const prepared = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.match(prepared.pauseReason || '', /could not be proven healthy/);
    assert.equal(store.getTask(thread.threadId)?.status, 'paused');
    assert.equal(probeWasTracked, true);
    assert.equal(probeWasCleared, true);
    roleLock.release(roleLease);
  });

  test('auto mode compacts at the threshold only with an injected certified capability', async () => {
    const { store, dir } = makeStore();
    const sessionId = '24b164df-c29d-4430-8dfd-7f12e0906289';
    const round: DelegationRound = { round: 1, type: 'initial', prompt: 'implement', durationMs: 1, status: 'running', timestamp: new Date().toISOString(), output: 'report' };
    const thread = makeThread([round]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.nativeSessionCompaction = { developer: { sessionId, turnsSinceCompaction: 4, revision: 4 } };
    store.upsertTask(thread);
    const roleLock = new TaskRoleLock(dir);
    const roleLease = roleLock.tryAcquire(thread.threadId, 'developer')!;
    saveBridgePolicy({ nativeSessionCompaction: { mode: 'auto' } }, dir);
    const detector = { resolveExecutable: () => process.execPath } as any;
    const compactor = new NativeSessionCompactor(store, detector, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'test', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let calls = 0;
    (compactor as any).codex = { compact: async (request: any) => {
      calls++;
      return { threadId: request.sessionId, nativeTurnId: 'native-turn-1', model: request.model, provider: 'test-provider', processExitCode: 0,
        usage: { id: request.operationId, kind: 'native_compaction', role: request.role, agent: 'codex', model: request.model, uncachedInputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 2, costUsd: 0.02, source: 'native_reported', available: true } };
    } };
    const prepared = await compactor.prepareBeforeResume(thread.threadId, 'developer', 'gpt-6-luna');
    assert.equal(calls, 1, 'certified auto path invokes native compaction before resume');
    assert.ok(prepared.lease);
    assert.equal(prepared.recoveryVersion, store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!.lastOperationId);
    assert.match(prepared.recoveryPrefix || '', /implement/);
    const stored = store.getTask(thread.threadId)!;
    assert.equal(stored.rounds[0].nativeSessionCompactions?.[0].phase, 'succeeded');
    assert.equal(stored.rounds[0].tokenUsage?.components?.some((item) => item.kind === 'native_compaction'), true);
    new NativeSessionLock(dir).release(prepared.lease!);
    if (prepared.executionLease?.owned) prepared.executionLease.registry.end(prepared.executionLease.registration);
    prepared.executionLease?.registry.dispose();
    roleLock.release(roleLease);
  });

  test('paused maintenance refuses a captured session without durable static ownership before adapter or artifacts', async () => {
    const { store, dir } = makeStore();
    const sessionId = 'dfabfca6-0e2d-4093-9e15-733390baac28';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.nativeSessionAssignments = { developer: { namespace: nativeSessionNamespace('codex'), sessionEpoch: 3 } };
    thread.execution = { version: 2, revision: 4, operationId: 'operation-maintenance', phase: 'develop', round: 1,
      generation: 7, attempt: 2, autoRetriesUsed: 1, budget: { limitMs: 120_000, spentActiveMs: 33_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    let calls = 0;
    const compactor = new NativeSessionCompactor(store, {} as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => { throw new Error('capability probe must not run without ownership evidence'); };
    (compactor as any).codex = { compact: async () => { calls++; throw new Error('adapter must not run'); } };
    await assert.rejects(compactor.compactPausedTask(thread.threadId, 'developer'), /SESSION_OWNERSHIP_UNVERIFIED/);
    assert.equal(calls, 0);
    const after = store.getTask(thread.threadId)!;
    assert.equal(after.execution!.run, undefined);
    assert.equal(after.rounds[0].nativeSessionCompactions, undefined);
    const taskDir = path.join(dir, thread.threadId);
    assert.equal(fs.existsSync(taskDir), false, 'refusal occurs before creating maintenance business artifacts');
  });

  test('paused native maintenance occupies the v2 run in the ledger and settles without resuming business', async () => {
    const { store, dir } = makeStore();
    const sessionId = '2f79e67e-31a4-4d83-a75b-9650e9b59b13';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const namespace = nativeSessionNamespace('codex');
    thread.nativeSessionAssignments = { developer: { namespace, sessionEpoch: 5 } };
    thread.execution = { version: 2, revision: 9, operationId: 'business-operation', phase: 'develop', round: 1,
      generation: 12, attempt: 3, autoRetriesUsed: 1, budget: { limitMs: 120_000, spentActiveMs: 44_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    assert.equal(new SessionOwnershipStore(dir).claim({ agent: 'codex', namespace, sessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 5 }).ok, true);
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'fixture', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let observedRun: any;
    (compactor as any).codex = { compact: async (request: any) => {
      const active = store.getTask(thread.threadId)!;
      observedRun = active.execution!.run;
      assert.equal(active.status, 'paused');
      assert.equal(active.execution!.purpose, undefined, 'business execution purpose is not rewritten');
      assert.equal(active.execution!.generation, 12);
      assert.equal(active.execution!.budget.spentActiveMs, 44_000);
      assert.equal(active.execution!.run!.purpose, 'native_maintenance');
      assert.equal(active.execution!.run!.role, 'developer');
      assert.equal(active.execution!.run!.step, 'native_maintenance');
      assert.equal(active.execution!.run!.state, 'starting');
      assert.ok(active.execution!.run!.spawnIntentAt);
      assert.equal(request.sessionId, sessionId);
      assert.ok(request.timeoutMs > 0 && request.timeoutMs <= 76_000, 'native RPC receives only the remaining task budget and maintenance cap');
      await request.onSpawnNotStarted();
      await new Promise((resolve) => setTimeout(resolve, 25));
      return { threadId: sessionId, nativeTurnId: 'fixture-turn', model: request.model, provider: 'fixture', processExitCode: 0,
        usage: { id: request.operationId, kind: 'native_compaction', role: 'developer', agent: 'codex', model: request.model, uncachedInputTokens: 10, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 2, source: 'native_reported', available: true } };
    } };
    const operation = await compactor.compactPausedTask(thread.threadId, 'developer');
    assert.equal(observedRun?.purpose, 'native_maintenance');
    assert.equal(operation.phase, 'succeeded');
    const after = store.getTask(thread.threadId)!;
    assert.equal(after.status, 'paused');
    assert.equal(after.execution!.run, undefined);
    assert.equal(after.execution!.generation, 12);
    assert.ok(after.execution!.budget.spentActiveMs > 44_000, 'active maintenance time is charged to the shared budget');
    assert.ok(after.execution!.budget.spentActiveMs <= 120_000);
    assert.equal(after.execution!.next, undefined);
    assert.equal(after.rounds[0].nativeSessionCompactions?.[0].phase, 'succeeded');
    assert.ok(after.rounds[0].nativeSessionCompactions?.[0].activeDurationMs! > 0);
    assert.equal(after.rounds[0].tokenUsage?.components?.some((component) => component.id === operation.id), true);
    assert.equal(fs.existsSync(path.join(dir, 'roles')), false, 'v2 native maintenance is admitted by its run, not a Bridge role lock');
    assert.equal(fs.existsSync(path.join(dir, 'native-sessions')), false, 'v2 native maintenance does not create a Bridge session lease');
  });

  test('paused native maintenance refuses an exhausted task budget before artifacts, capability probe, or RPC', async () => {
    const { store, dir } = makeStore();
    const sessionId = 'c4eab133-e622-4e9f-a282-202b6a31a750';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const namespace = nativeSessionNamespace('codex');
    thread.nativeSessionAssignments = { developer: { namespace, sessionEpoch: 8 } };
    thread.execution = { version: 2, revision: 1, operationId: 'budget-exhausted', phase: 'develop', round: 1,
      generation: 4, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 60_000, spentActiveMs: 60_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    assert.equal(new SessionOwnershipStore(dir).claim({ agent: 'codex', namespace, sessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 8 }).ok, true);
    let adapterCalls = 0;
    const compactor = new NativeSessionCompactor(store, {} as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => { throw new Error('capability probe must not run with exhausted budget'); };
    (compactor as any).codex = { compact: async () => { adapterCalls++; throw new Error('adapter must not run'); } };
    await assert.rejects(compactor.compactPausedTask(thread.threadId, 'developer'), /NATIVE_MAINTENANCE_BUDGET_EXHAUSTED/);
    assert.equal(adapterCalls, 0);
    const after = store.getTask(thread.threadId)!;
    assert.equal(after.execution!.run, undefined);
    assert.equal(after.rounds[0].nativeSessionCompactions, undefined);
    assert.equal(fs.existsSync(path.join(dir, thread.threadId)), false, 'exhaustion is checked before maintenance artifacts');
  });

  test('manual maintenance keeps detached descendants until exit and refuses success or forced cleanup', async () => {
    const { store, dir } = makeStore();
    const sessionId = '46fca4fd-b457-48f0-a876-3bb2d6920e1b';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const namespace = nativeSessionNamespace('codex');
    thread.nativeSessionAssignments = { developer: { namespace, sessionEpoch: 9 } };
    thread.execution = { version: 2, revision: 2, operationId: 'subtree-business-op', phase: 'develop', round: 1,
      generation: 5, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 15_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    assert.equal(new SessionOwnershipStore(dir).claim({ agent: 'codex', namespace, sessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 9 }).ok, true);
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'fixture', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    const grandchildPidFile = path.join(dir, 'grandchild.pid');
    const stopRootFile = path.join(dir, 'root.stop');
    let root: ReturnType<typeof spawn> | undefined;
    let grandchildPid: number | undefined;
    let grandchildBirthId: string | undefined;
    let adapterCalls = 0;
    let ignoredCloseError: string | undefined;
    let notifyChildExit: (() => Promise<void>) | undefined;
    const waitUntil = async (predicate: () => boolean, label: string): Promise<void> => {
      const deadline = Date.now() + 8_000;
      while (Date.now() < deadline) {
        if (predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.fail(`Timed out waiting for ${label}`);
    };
    (compactor as any).codex = { compact: async (request: any) => {
      adapterCalls++;
      notifyChildExit = async () => { await request.onChildPid(undefined); };
      const childCode = `const fs=require('fs'),cp=require('child_process');const p=cp.spawn(process.execPath,['-e',${JSON.stringify('setInterval(()=>{},1000)')}],{detached:true,stdio:'ignore'});p.unref();fs.writeFileSync(${JSON.stringify(grandchildPidFile)},String(p.pid));const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(stopRootFile)})){clearInterval(t);process.exit(0)}},10);`;
      root = spawn(process.execPath, ['-e', childCode], { stdio: 'ignore', windowsHide: true });
      const rootPid = root.pid!;
      const rootBirthId = await getProcessBirthIdAsync(rootPid);
      assert.ok(rootBirthId, 'root process has a captured birth ID');
      await request.onChildPid(rootPid, rootBirthId);
      await waitUntil(() => fs.existsSync(grandchildPidFile), 'detached grandchild creation');
      grandchildPid = Number(fs.readFileSync(grandchildPidFile, 'utf8'));
      grandchildBirthId = await getProcessBirthIdAsync(grandchildPid!);
      assert.ok(grandchildBirthId, 'grandchild process has a captured birth ID');
      await waitUntil(() => !!store.getTask(thread.threadId)?.execution?.run?.subtrees?.some((tree: any) =>
        tree.descendants.some((entry: any) => entry.pid === grandchildPid && entry.birthId === grandchildBirthId)), 'durable grandchild evidence');
      const revisionBeforeRootExit = store.getTask(thread.threadId)!.execution!.revision;
      const stopAt = Date.now();
      const rootClosed = new Promise<void>((resolve) => {
        if (root!.exitCode !== null || root!.signalCode !== null) resolve();
        else root!.once('close', () => resolve());
      });
      fs.writeFileSync(stopRootFile, 'exit');
      await rootClosed;
      await waitUntil(() => {
        const currentTask = store.getTask(thread.threadId);
        const current = currentTask?.execution?.run;
        return !!currentTask && !!current && (current.subtrees || []).some((tree: any) => tree.sampledAt && Date.parse(tree.sampledAt) >= stopAt) &&
          currentTask.execution!.revision > revisionBeforeRootExit;
      }, 'post-root-exit maintenance sample');
      const sampled = store.getTask(thread.threadId)!.execution!.run!;
      assert.equal(probeProcessIdentity(grandchildPid!, grandchildBirthId), 'alive');
      assert.ok(sampled.subtrees?.some((tree: any) => tree.root.pid === rootPid && tree.root.birthId === rootBirthId));
      assert.ok(sampled.subtrees?.some((tree: any) => tree.descendants.some((entry: any) => entry.pid === grandchildPid && entry.birthId === grandchildBirthId)), 'prior descendant survives the root-exit sample');
      try { await request.onChildPid(undefined); } catch (error: any) { ignoredCloseError = String(error?.message || error); }
      return { threadId: sessionId, nativeTurnId: 'untrusted-success', model: request.model, provider: 'fixture', processExitCode: 0 };
    } };

    try {
      const operation = await compactor.compactPausedTask(thread.threadId, 'developer');
      assert.equal(adapterCalls, 1);
      assert.match(ignoredCloseError || '', /subtree|unconfirmed/i);
      assert.equal(operation.phase, 'unknown', 'a structured adapter result cannot override a live captured descendant');
      const blocked = store.getTask(thread.threadId)!;
      assert.equal(blocked.status, 'paused');
      assert.equal(blocked.execution!.run!.state, 'stopping');
      assert.equal(blocked.execution!.run!.unconfirmed, true);
      assert.ok(blocked.execution!.run!.subtrees?.some((tree) => tree.descendants.some((entry) => entry.pid === grandchildPid && entry.birthId === grandchildBirthId)));
      const cleanup = await store.markTaskCleanup(thread.threadId, 'delete', { pid: process.pid, birthId: await getProcessBirthIdAsync(process.pid) });
      await assert.rejects(store.commitTaskCleanup(thread.threadId, cleanup.token, 'delete'), /execution run .* is still recorded/);
      await assert.rejects(compactor.compactPausedTask(thread.threadId, 'developer'), /Another developer|run, handoff, cleanup|run|cleanup/i);
      assert.equal(adapterCalls, 1, 'an unconfirmed previous subtree cannot start the next maintenance step');
      assert.equal(probeProcessIdentity(grandchildPid!, grandchildBirthId), 'alive');
    } finally {
      if (root?.pid && root.exitCode === null) { try { root.kill(); } catch {} }
      const known = store.getTask(thread.threadId)?.execution?.run?.subtrees?.flatMap((tree) => [tree.root, ...tree.descendants]) || [];
      for (const identity of known) if (identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'alive') {
        try { process.kill(identity.pid); } catch {}
      }
      if (root?.pid && root.exitCode === null) await new Promise<void>((resolve) => root!.once('close', () => resolve()));
      if (notifyChildExit && grandchildPid && grandchildBirthId) {
        const deadline = Date.now() + 8_000;
        const allKnownExited = () => known.every((identity) => !!identity.birthId && probeProcessIdentity(identity.pid, identity.birthId) === 'exited');
        while (!allKnownExited() && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        if (allKnownExited()) {
          let lateError: string | undefined;
          await notifyChildExit().catch((error: any) => { lateError = String(error?.message || error); });
          const run = store.getTask(thread.threadId)?.execution?.run;
          assert.equal(run, undefined, `durable run settles exactly once after the retained descendant exits; error=${lateError}; run=${JSON.stringify(run)}`);
        }
      }
    }
  });

  test('manual maintenance settles once only after every captured process exits', async () => {
    const { store, dir } = makeStore();
    const sessionId = 'f3ad1786-7f5d-4f10-a4bf-197f7db0e91c';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const namespace = nativeSessionNamespace('codex');
    thread.nativeSessionAssignments = { developer: { namespace, sessionEpoch: 10 } };
    thread.execution = { version: 2, revision: 2, operationId: 'all-exit-business-op', phase: 'develop', round: 1,
      generation: 6, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 10_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    assert.equal(new SessionOwnershipStore(dir).claim({ agent: 'codex', namespace, sessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 10 }).ok, true);
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'fixture', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let root: ReturnType<typeof spawn> | undefined;
    let grandchildPid: number | undefined;
    let grandchildBirthId: string | undefined;
    let adapterCalls = 0;
    const grandchildFile = path.join(dir, 'exited-grandchild.pid');
    const stopRootFile = path.join(dir, 'exited-root.stop');
    (compactor as any).codex = { compact: async (request: any) => {
      adapterCalls++;
      const childCode = `const fs=require('fs'),cp=require('child_process');const p=cp.spawn(process.execPath,['-e',${JSON.stringify('setInterval(()=>{},1000)')}],{detached:true,stdio:'ignore'});p.unref();fs.writeFileSync(${JSON.stringify(grandchildFile)},String(p.pid));const t=setInterval(()=>{if(fs.existsSync(${JSON.stringify(stopRootFile)})){clearInterval(t);process.exit(0)}},10);`;
      root = spawn(process.execPath, ['-e', childCode], { stdio: 'ignore', windowsHide: true });
      const rootPid = root.pid!;
      const rootBirthId = await getProcessBirthIdAsync(rootPid);
      assert.ok(rootBirthId);
      await request.onChildPid(rootPid, rootBirthId);
      const deadline = Date.now() + 8_000;
      while (!fs.existsSync(grandchildFile) && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 10));
      assert.ok(fs.existsSync(grandchildFile), 'detached descendant was created');
      grandchildPid = Number(fs.readFileSync(grandchildFile, 'utf8'));
      grandchildBirthId = await getProcessBirthIdAsync(grandchildPid);
      assert.ok(grandchildBirthId);
      const close = new Promise<void>((resolve) => root!.once('close', () => resolve()));
      fs.writeFileSync(stopRootFile, 'exit');
      await close;
      const childExitDeadline = Date.now() + 8_000;
      while (probeProcessIdentity(grandchildPid, grandchildBirthId) !== 'exited' && Date.now() < childExitDeadline) {
        try { process.kill(grandchildPid); } catch {}
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
      assert.equal(probeProcessIdentity(grandchildPid, grandchildBirthId), 'exited', 'captured descendant exit is verified before adapter close');
      await request.onChildPid(undefined);
      return { threadId: sessionId, nativeTurnId: 'all-exited-turn', model: request.model, provider: 'fixture', processExitCode: 0 };
    } };
    try {
      const operation = await compactor.compactPausedTask(thread.threadId, 'developer');
      assert.equal(adapterCalls, 1);
      assert.equal(operation.phase, 'succeeded', operation.error || operation.errorCode);
      const settled = store.getTask(thread.threadId)!;
      assert.equal(settled.status, 'paused');
      assert.equal(settled.execution!.run, undefined);
      assert.equal(settled.rounds[0].nativeSessionCompactions?.filter((item) => item.id === operation.id).length, 1);
      assert.equal(probeProcessIdentity(grandchildPid!, grandchildBirthId!), 'exited');
    } finally {
      if (root?.pid && root.exitCode === null) { try { root.kill(); } catch {} }
      if (grandchildPid && grandchildBirthId && probeProcessIdentity(grandchildPid, grandchildBirthId) === 'alive') { try { process.kill(grandchildPid); } catch {} }
    }
  });

  test('second control process cannot physically clean a paused task while its maintenance child is alive', async () => {
    const { store, dir } = makeStore();
    const sessionId = '82de43ee-262a-4e91-a548-021731e52130';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'maintain me', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.status = 'paused'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    const namespace = nativeSessionNamespace('codex');
    thread.nativeSessionAssignments = { developer: { namespace, sessionEpoch: 2 } };
    thread.execution = { version: 2, revision: 2, operationId: 'business-operation', phase: 'develop', round: 1,
      generation: 3, attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 9_000, maxAutoRetries: 2 } };
    store.upsertTask(thread);
    assert.equal(new SessionOwnershipStore(dir).claim({ agent: 'codex', namespace, sessionId, threadId: thread.threadId, role: 'developer', sessionEpoch: 2 }).ok, true);
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    (compactor as any).capabilityIdentity = () => ({ agent: 'codex', executablePath: process.execPath, executableVersion: 'fixture', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
    let child: ReturnType<typeof spawn> | undefined;
    let releaseChild!: () => void;
    let announceChild!: () => void;
    const childReady = new Promise<void>((resolve) => { announceChild = resolve; });
    const permitExit = new Promise<void>((resolve) => { releaseChild = resolve; });
    (compactor as any).codex = { compact: async (request: any) => {
      child = spawn(process.execPath, ['-e', 'setInterval(()=>{},10000)'], { stdio: 'ignore', windowsHide: true });
      const birthId = await getProcessBirthIdAsync(child.pid!);
      await request.onChildPid(child.pid, birthId);
      announceChild();
      await permitExit;
      const closed = new Promise<void>((resolve) => child!.once('close', () => resolve()));
      try { child!.kill(); } catch {}
      await closed;
      await request.onChildPid(undefined);
      return { threadId: sessionId, nativeTurnId: 'fixture-turn', model: request.model, provider: 'fixture', processExitCode: 0 };
    } };
    const compacting = compactor.compactPausedTask(thread.threadId, 'developer');
    try {
      await childReady;
      const script = `const {TaskStore}=require(${JSON.stringify(path.resolve('out/bridge/task-store.js'))});const {getProcessBirthId}=require(${JSON.stringify(path.resolve('out/bridge/platform.js'))});(async()=>{const s=new TaskStore(${JSON.stringify(dir)});const x=await s.markTaskCleanup(${JSON.stringify(thread.threadId)},'delete',{pid:process.pid,birthId:getProcessBirthId(process.pid)});let blocked=false;try{await s.commitTaskCleanup(${JSON.stringify(thread.threadId)},x.token,'delete')}catch(e){blocked=/execution run/.test(String(e&&e.message||e))}console.log(JSON.stringify({token:x.token,blocked,run:x.thread.execution.run&&x.thread.execution.run.id}));s.dispose()})().catch(e=>{console.error(e);process.exitCode=1})`;
      const result = spawnSync(process.execPath, ['-e', script], { cwd: process.cwd(), encoding: 'utf8', timeout: 10_000, windowsHide: true });
      assert.equal(result.status, 0, result.stderr || result.error?.message);
      const evidence = JSON.parse(result.stdout.trim());
      assert.equal(evidence.blocked, true, 'a second control process must not physically delete while the maintenance run is recorded');
      assert.equal(store.getTask(thread.threadId)!.execution!.run!.purpose, 'native_maintenance');
      releaseChild();
      const operation = await compacting;
      assert.equal(operation.phase, 'unknown', 'cleanup reservation races the adapter result and prevents a success commit');
      const stored = store.getTask(thread.threadId);
      if (stored?.execution?.run) {
        assert.equal(stored.execution.run.state, 'stopping', 'an unverified captured descendant keeps the run protected after its root closes');
        assert.equal(stored.execution.run.unconfirmed, true);
        await assert.rejects(store.commitTaskCleanup(thread.threadId, evidence.token, 'delete'), /execution run .* is still recorded/);
        assert.ok(store.getTask(thread.threadId), 'physical deletion is refused while subtree evidence is incomplete');
      } else {
        assert.deepEqual(stored?.cleanup?.runExitProof && {
          runId: stored.cleanup.runExitProof.runId,
          operationId: stored.cleanup.runExitProof.operationId,
          generation: stored.cleanup.runExitProof.generation,
        }, { runId: evidence.run, operationId: 'business-operation', generation: 3 });
        assert.equal(await store.commitTaskCleanup(thread.threadId, evidence.token, 'delete'), true);
        assert.equal(store.getTask(thread.threadId), undefined);
      }
    } finally {
      releaseChild();
      if (child?.pid && child.exitCode === null) { try { child.kill(); } catch {} }
      await compacting.catch(() => undefined);
    }
  });

  test('automatic native maintenance is a serial step on the existing v2 business run', async () => {
    const { store, dir } = makeStore();
    const sessionId = 'c12a45fe-3f57-4c5a-8950-05da31b0f866';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'continue', durationMs: 1, status: 'running', timestamp: new Date().toISOString() }]);
    thread.status = 'running'; thread.targetAgent = 'codex'; thread.targetModel = 'gpt-6-luna';
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.execution = { version: 2, revision: 2, operationId: 'live-business-op', phase: 'develop', round: 1, generation: 4,
      attempt: 1, autoRetriesUsed: 0, budget: { limitMs: 120_000, spentActiveMs: 17_000, maxAutoRetries: 2 },
      run: { id: 'live-run-id', owner: { pid: process.pid }, role: 'developer', step: 'develop', state: 'active', startedAt: new Date().toISOString() } };
    thread.nativeSessionCompaction = { developer: { sessionId, turnsSinceCompaction: 4, revision: 4 } };
    store.upsertTask(thread);
    const roles = new TaskRoleLock(dir); const roleLease = roles.tryAcquire(thread.threadId, 'developer')!;
    const native = new NativeSessionLock(dir); const nativeLease = native.acquire('codex', sessionId)!;
    const compactor = new NativeSessionCompactor(store, { resolveExecutable: () => process.execPath } as any, () => 'certified');
    let observations = 0;
    (compactor as any).codex = { compact: async (request: any) => {
      observations++;
      const active = store.getTask(thread.threadId)!;
      assert.equal(active.execution!.run!.id, 'live-run-id');
      assert.equal(active.execution!.run!.step, 'native_maintenance');
      assert.equal(active.execution!.operationId, 'live-business-op');
      assert.equal(active.execution!.generation, 4);
      return { threadId: sessionId, nativeTurnId: 'automatic-turn', model: request.model, provider: 'fixture', processExitCode: 0 };
    } };
    try {
      const result = await (compactor as any).compactAutomatically(thread, 'developer', 'codex', 'gpt-6-luna', sessionId, nativeLease,
        { sessionId, turnsSinceCompaction: 4, revision: 4 },
        { agent: 'codex', executablePath: process.execPath, executableVersion: 'fixture', providerRoute: 'subscription-default', providerId: 'openai', model: 'gpt-6-luna' });
      assert.equal(result.warning, undefined);
      assert.equal(observations, 1);
      const after = store.getTask(thread.threadId)!;
      assert.equal(after.status, 'running');
      assert.equal(after.execution!.run!.id, 'live-run-id');
      assert.equal(after.execution!.run!.step, 'develop');
      assert.equal(after.execution!.generation, 4);
      assert.equal(after.execution!.budget.spentActiveMs, 17_000);
    } finally { native.release(nativeLease); roles.release(roleLease); }
  });

  test('native automatic compaction events are consumed once and an immediate repeat disables auto maintenance', () => {
    const { store } = makeStore();
    const sessionId = '389669bb-95b0-4d1e-a243-9e23da799ed8';
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'p', durationMs: 1, status: 'completed', timestamp: new Date().toISOString() }]);
    thread.receiverSessionId = sessionId; thread.receiverSessionCaptured = true;
    thread.nativeSessionCompaction = { developer: { sessionId, turnsSinceCompaction: 0, lastOperationId: 'native-op', revision: 3 } };
    thread.rounds[0].nativeSessionCompactions = [{ id: 'native-op', role: 'developer', agent: 'codex', sessionId, reason: 'turn_budget', phase: 'succeeded', revision: 2, startedAt: new Date().toISOString(), recoveryPath: '', logPath: '' } as any];
    store.upsertTask(thread);
    const compactor = new NativeSessionCompactor(store, {} as any);
    const event = JSON.stringify({ method: 'item/completed', params: { threadId: sessionId, turnId: 'turn-1', item: { id: 'compact-item', type: 'contextCompaction' } } });
    compactor.recordNativeAutoCompactionEvents(thread.threadId, 'developer', sessionId, `${event}\n${event}`);
    compactor.recordNativeAutoCompactionEvents(thread.threadId, 'developer', sessionId, event);
    const cursor = store.getTask(thread.threadId)!.nativeSessionCompaction!.developer!;
    assert.deepEqual(cursor.recentNativeAutoCompactionEventIds, [`${sessionId}:turn-1:compact-item`]);
    assert.equal(cursor.turnsSinceCompaction, 0);
    assert.match(cursor.autoDisabledReason || '', /ineffective/);
  });

  test('missing maintenance usage is partial and cannot erase an already measured operation', () => {
    const { store } = makeStore();
    const thread = makeThread([{ round: 1, type: 'initial', prompt: 'p', durationMs: 1, status: 'completed', timestamp: new Date().toISOString(), tokenUsage: usage(100) }]);
    store.upsertTask(thread);
    const compactor = new NativeSessionCompactor(store, {} as any);
    const record = (compactor as any).recordMaintenanceUsage.bind(compactor);
    record(thread.threadId, 1, 'operation-usage', 'reviewer', 'claude_code', 'sonnet');
    let saved = store.getTask(thread.threadId)!.rounds[0].tokenUsage!;
    const unavailable = saved.components!.find((component) => component.id === 'operation-usage')!;
    assert.equal(unavailable.available, false);
    assert.equal(unavailable.partial, true);
    assert.equal(aggregateThreadTokenUsage(store.getTask(thread.threadId)!.rounds).totalTokens, 100);
    record(thread.threadId, 1, 'operation-usage', 'reviewer', 'claude_code', 'sonnet', {
      id: 'operation-usage', kind: 'native_compaction', role: 'reviewer', agent: 'claude_code', model: 'sonnet',
      uncachedInputTokens: 20, cacheReadTokens: 0, cacheWriteTokens: 0, outputTokens: 2, costUsd: 0.01, source: 'native_reported', available: true,
    });
    record(thread.threadId, 1, 'operation-usage', 'reviewer', 'claude_code', 'sonnet');
    saved = store.getTask(thread.threadId)!.rounds[0].tokenUsage!;
    assert.equal(aggregateThreadTokenUsage(store.getTask(thread.threadId)!.rounds).totalTokens, 122);
    assert.equal(saved.costUsd, 0.01);
  });
});
