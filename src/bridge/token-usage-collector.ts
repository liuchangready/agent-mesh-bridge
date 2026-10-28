/**
 * Token accounting for delegated rounds.
 *
 * Pure functions only: no process is spawned, no file is read, nothing is
 * mutated. The runner hands over the stdout it already buffered, the collector
 * extracts whatever usage the CLI reported, and the result is normalized into
 * ONE shape (see `TokenUsageMetrics`).
 *
 * The normalization matters more than the parsing. The four receivers disagree
 * on what "input tokens" means:
 *
 *   Codex / Antigravity  input INCLUDES cache reads  (cache is a subset)
 *   Claude / DSH         input EXCLUDES cache reads  (cache is counted apart)
 *
 * With a single `input + output` formula a DSH step measured at
 * 2266 + 1345 + 653824 cached tokens would be reported as 3,611 instead of
 * 657,435 — off by 182x. Every agent therefore gets its own explicit mapping
 * (see the capability matrix in docs/TOKEN_STATISTICS_DESIGN.md).
 *
 * Deliberate omissions:
 *   - Antigravity's `result` event reports a CONVERSATION TOTAL, not this
 *     round. It is dropped on the floor; summing it with the per-step usage
 *     would double count.
 *   - Input tokens are never estimated. A rework round's input is the whole
 *     history plus tool results; there is no character ratio that predicts it.
 *     Only the output is guessed, and the result is flagged as partial.
 */

import {
  AgentId,
  DelegationRound,
  ThreadTokenUsageSummary,
  TokenCostSource,
  TokenMetricSource,
  TokenUsageMetrics,
} from './types';
import type { TaskStore } from './task-store';

/**
 * The measurement already stored for a round, if any.
 *
 * `retry_task` and `resume_task` reuse the round number, so the next execution
 * of that round must add to this figure instead of starting from zero.
 */
export function readStoredRoundUsage(
  store: TaskStore | undefined,
  threadId: string | undefined,
  round: number | undefined,
): TokenUsageMetrics | undefined {
  if (!store || !threadId || round === undefined) return undefined;
  try {
    const thread = store.getTask(threadId);
    const roundEntry = (thread?.rounds || []).find((r) => r.round === round);
    return roundEntry?.tokenUsage;
  } catch {
    return undefined;
  }
}

/** Rough output-token estimate: CJK ~1.5 chars/token, other text ~3.8. */
const CJK_CHARS_PER_TOKEN = 1.5;
const OTHER_CHARS_PER_TOKEN = 3.8;

export interface TokenCollectionContext {
  agent: AgentId;
  /** The full stdout the runner buffered for this attempt. */
  stdout: string;
  /** The extracted final answer, used for the output-only estimate. */
  finalOutput?: string;
  /** Model the bridge asked for (`thread.targetModel`). */
  requestedModel?: string;
  /** Model the runner actually resolved and launched. */
  effectiveModel?: string;
  /** True when the round ended abnormally (paused / killed / timeout). */
  interrupted?: boolean;
  /** ISO8601 stamp; defaults to now. */
  collectedAt?: string;
}

/** Mutable accumulator while walking the event stream. */
interface UsageBucket {
  uncached: number;
  cacheRead: number;
  cacheWrite: number;
  output: number;
  reasoning: number;
  nativeTotal?: number;
  costUsd?: number;
  costSource?: TokenCostSource;
  reportedModel?: string;
  events: number;
  /** True when at least one event carried a usage payload. */
  sawUsage: boolean;
}

function emptyBucket(): UsageBucket {
  return {
    uncached: 0,
    cacheRead: 0,
    cacheWrite: 0,
    output: 0,
    reasoning: 0,
    events: 0,
    sawUsage: false,
  };
}

/** First finite, non-negative number among the given keys. */
function num(source: any, ...keys: string[]): number | undefined {
  if (!source || typeof source !== 'object') return undefined;
  for (const key of keys) {
    const value = source[key];
    if (typeof value === 'number' && Number.isFinite(value) && value >= 0) return value;
  }
  return undefined;
}

/** Accumulate a required counter (the incoming value may be absent). */
function addCount(target: number, value: number | undefined): number {
  return value === undefined ? target : target + value;
}

/** Accumulate an optional counter, keeping "never reported" as undefined. */
function addOptional(
  target: number | undefined,
  value: number | undefined,
): number | undefined {
  if (value === undefined) return target;
  return (target ?? 0) + value;
}

function modelOf(source: any): string | undefined {
  if (!source || typeof source !== 'object') return undefined;
  for (const key of ['model', 'model_id', 'modelId', 'model_name', 'modelName']) {
    const value = source[key];
    if (typeof value === 'string' && value.trim()) return value.trim();
  }
  return undefined;
}

/**
 * Parse the stdout into JSON events.
 *
 * Claude Code's `--output-format json` prints ONE (possibly pretty-printed)
 * JSON object, so the whole buffer is tried first; the JSONL receivers need the
 * line-by-line path.
 */
function parseEvents(stdout: string): any[] {
  const events: any[] = [];
  const trimmed = (stdout || '').trim();
  if (!trimmed) return events;

  if (trimmed.startsWith('{')) {
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed && typeof parsed === 'object') {
        if (Array.isArray(parsed)) return parsed.filter((e) => e && typeof e === 'object');
        return [parsed];
      }
    } catch {
      // Not a single JSON document after all; fall through to JSONL parsing.
    }
  }

  for (const line of trimmed.split(/\r?\n/)) {
    const candidate = line.trim();
    if (!candidate.startsWith('{')) continue;
    try {
      const parsed = JSON.parse(candidate);
      if (parsed && typeof parsed === 'object') events.push(parsed);
    } catch {
      // A partially streamed line is not an event; ignore it.
    }
  }
  return events;
}

/**
 * Codex: `{"type":"turn.completed","usage":{...}}`.
 *
 * Flat snake_case fields; `cached_input_tokens` and `cache_write_input_tokens`
 * are subsets of `input_tokens`.
 */
function collectCodex(bucket: UsageBucket, event: any): void {
  const usage = event?.usage;
  if (!usage || typeof usage !== 'object') return;
  const input = num(usage, 'input_tokens', 'inputTokens');
  const cached = num(usage, 'cached_input_tokens', 'cachedInputTokens') ?? 0;
  const cacheWrite = num(usage, 'cache_write_input_tokens', 'cacheWriteInputTokens') ?? 0;
  const output = num(usage, 'output_tokens', 'outputTokens') ?? 0;
  const reasoning = num(usage, 'reasoning_output_tokens', 'reasoningOutputTokens');
  const nativeTotal = num(usage, 'total_tokens', 'totalTokens');

  bucket.uncached += Math.max(0, (input ?? 0) - cached - cacheWrite);
  bucket.cacheRead += cached;
  bucket.cacheWrite += cacheWrite;
  bucket.output += output;
  bucket.reasoning = addCount(bucket.reasoning, reasoning);
  bucket.nativeTotal = addOptional(bucket.nativeTotal, nativeTotal);
  bucket.reportedModel = bucket.reportedModel || modelOf(event) || modelOf(usage);
  bucket.sawUsage = true;
  bucket.events += 1;
}

/**
 * Claude Code: one result object with `usage` and `total_cost_usd`.
 *
 * Cache is reported apart from `input_tokens` (Anthropic convention).
 */
function collectClaude(bucket: UsageBucket, event: any): void {
  const usage = event?.usage;
  if (!usage || typeof usage !== 'object') return;

  bucket.uncached += num(usage, 'input_tokens', 'inputTokens') ?? 0;
  bucket.cacheRead +=
    num(usage, 'cache_read_input_tokens', 'cacheReadInputTokens', 'cacheReadTokens') ?? 0;
  bucket.cacheWrite +=
    num(usage, 'cache_creation_input_tokens', 'cacheCreationInputTokens', 'cacheWriteTokens') ?? 0;
  bucket.output += num(usage, 'output_tokens', 'outputTokens') ?? 0;
  bucket.reportedModel = bucket.reportedModel || modelOf(event) || modelOf(usage);
  bucket.sawUsage = true;
  bucket.events += 1;

  const cost = num(event, 'total_cost_usd', 'totalCostUsd', 'cost_usd', 'costUSD');
  if (cost !== undefined) {
    bucket.costUsd = addOptional(bucket.costUsd, cost);
    bucket.costSource = 'native';
  }
}

/**
 * Antigravity: `{"event":"step_update","step_update":{"usage":{...}}}`.
 *
 * `input_tokens` already includes the cached part. The `result` event carries a
 * conversation total and is intentionally ignored.
 */
function collectAntigravity(bucket: UsageBucket, event: any): void {
  const usage = event?.step_update?.usage ?? event?.usage;
  if (!usage || typeof usage !== 'object') return;
  const input = num(usage, 'input_tokens', 'inputTokens') ?? 0;
  const cached = num(usage, 'cache_read_tokens', 'cacheReadTokens') ?? 0;
  const output = num(usage, 'output_tokens', 'outputTokens') ?? 0;
  const thinking = num(usage, 'thinking_tokens', 'thinkingTokens');

  bucket.uncached += Math.max(0, input - cached);
  bucket.cacheRead += cached;
  bucket.output += output;
  bucket.reasoning = addCount(bucket.reasoning, thinking);
  bucket.nativeTotal = addOptional(bucket.nativeTotal, num(usage, 'total_tokens', 'totalTokens'));
  bucket.reportedModel = bucket.reportedModel || modelOf(event) || modelOf(usage);
  bucket.sawUsage = true;
  bucket.events += 1;
}

/**
 * DeepSeek Harness: `{"type":"status","phase":"step_end","usage":{...}}`.
 *
 * camelCase fields; `inputTokens` EXCLUDES `cacheReadTokens`. The `final`
 * event carries no usage at all.
 */
function collectDsh(bucket: UsageBucket, event: any): void {
  const usage = event?.usage;
  if (!usage || typeof usage !== 'object') return;
  bucket.uncached += num(usage, 'inputTokens', 'input_tokens') ?? 0;
  bucket.cacheRead += num(usage, 'cacheReadTokens', 'cache_read_tokens') ?? 0;
  bucket.cacheWrite += num(usage, 'cacheWriteTokens', 'cache_write_tokens') ?? 0;
  bucket.output += num(usage, 'outputTokens', 'output_tokens') ?? 0;
  bucket.reasoning = addCount(bucket.reasoning, num(usage, 'reasoningTokens', 'reasoning_tokens'));
  bucket.nativeTotal = addOptional(bucket.nativeTotal, num(usage, 'totalTokens', 'total_tokens'));
  bucket.reportedModel = bucket.reportedModel || modelOf(event) || modelOf(usage);
  bucket.sawUsage = true;
  bucket.events += 1;
}

function collectEvents(agent: AgentId, events: any[]): UsageBucket {
  const bucket = emptyBucket();
  for (const event of events) {
    if (!event || typeof event !== 'object') continue;
    switch (agent) {
      case 'codex':
        if (event.type === 'turn.completed') collectCodex(bucket, event);
        break;
      case 'claude_code':
        if (event.type === 'result') collectClaude(bucket, event);
        break;
      case 'antigravity':
        if (event.event === 'step_update' || event.type === 'step_update') {
          collectAntigravity(bucket, event);
        }
        break;
      case 'deepseek_harness':
        if (event.type === 'status' && event.phase === 'step_end') {
          collectDsh(bucket, event);
        }
        break;
      default:
        break;
    }
  }

  // Claude's result can carry per-model usage only (`modelUsage`); use it when
  // the top-level `usage` is missing so a multi-model run is not lost.
  if (agent === 'claude_code' && !bucket.sawUsage) {
    for (const event of events) {
      const modelUsage = (event as any)?.modelUsage;
      if (!modelUsage || typeof modelUsage !== 'object') continue;
      for (const entry of Object.values(modelUsage as Record<string, any>)) {
        if (entry && typeof entry === 'object') collectClaude(bucket, { usage: entry, ...entry });
      }
    }
  }

  return bucket;
}

/**
 * Output-token estimate from text.
 *
 * Only the output is ever estimated: the input of a rework round is the whole
 * conversation plus tool results, which no character ratio can predict.
 */
export function estimateOutputTokens(text: string): number {
  if (!text) return 0;
  let cjk = 0;
  let other = 0;
  for (const char of text) {
    const code = char.codePointAt(0) ?? 0;
    const isCjk =
      (code >= 0x3040 && code <= 0x30ff) || // kana
      (code >= 0x3400 && code <= 0x4dbf) || // CJK ext A
      (code >= 0x4e00 && code <= 0x9fff) || // CJK
      (code >= 0xf900 && code <= 0xfaff) || // compatibility
      (code >= 0xac00 && code <= 0xd7af); // hangul
    if (isCjk) cjk += 1;
    else other += 1;
  }
  return Math.ceil(cjk / CJK_CHARS_PER_TOKEN + other / OTHER_CHARS_PER_TOKEN);
}

function buildMetrics(
  bucket: UsageBucket,
  ctx: TokenCollectionContext,
  source: TokenMetricSource,
  available: boolean,
  partial: boolean,
  notes?: string,
): TokenUsageMetrics {
  const promptTokens = bucket.uncached + bucket.cacheRead + bucket.cacheWrite;
  const totalTokens = promptTokens + bucket.output;
  return {
    uncachedInputTokens: bucket.uncached,
    cacheReadTokens: bucket.cacheRead,
    cacheWriteTokens: bucket.cacheWrite,
    promptTokens,
    outputTokens: bucket.output,
    reasoningTokens: bucket.reasoning > 0 ? bucket.reasoning : undefined,
    totalTokens,
    nativeTotalTokens: bucket.nativeTotal,
    agent: ctx.agent,
    model: bucket.reportedModel || ctx.effectiveModel || ctx.requestedModel,
    reportedModel: bucket.reportedModel,
    requestedModel: ctx.requestedModel,
    costUsd: bucket.costUsd,
    costSource: bucket.costUsd !== undefined ? bucket.costSource : 'none',
    source,
    available,
    partial: partial || undefined,
    attempts: 1,
    notes,
    collectedAt: ctx.collectedAt || new Date().toISOString(),
  };
}

/**
 * Extract and normalize the token usage of one process attempt.
 *
 * Always returns a metrics object: when nothing could be measured the result
 * is `available: false` with zeroed counters, so "0 tokens" can never be
 * mistaken for "this round was free".
 */
export function collectTokenUsage(ctx: TokenCollectionContext): TokenUsageMetrics {
  const events = parseEvents(ctx.stdout || '');
  const bucket = collectEvents(ctx.agent, events);

  if (bucket.sawUsage) {
    const notes: string[] = [];
    if (ctx.interrupted) notes.push('round ended abnormally; usage may be partial');
    if (bucket.nativeTotal !== undefined && bucket.nativeTotal !== bucket.uncached + bucket.cacheRead + bucket.cacheWrite + bucket.output) {
      notes.push(
        `CLI reported total ${bucket.nativeTotal}, normalized total ${
          bucket.uncached + bucket.cacheRead + bucket.cacheWrite + bucket.output
        }`,
      );
    }
    return buildMetrics(bucket, ctx, 'native_reported', true, !!ctx.interrupted, notes.join('; ') || undefined);
  }

  // No native usage: estimate the output only, never the input.
  const estimatedOutput = estimateOutputTokens(ctx.finalOutput || '');
  if (estimatedOutput > 0) {
    const bucket2 = emptyBucket();
    bucket2.output = estimatedOutput;
    return buildMetrics(
      bucket2,
      ctx,
      'estimated',
      true,
      true,
      'CLI reported no usage; output estimated, input tokens unknown',
    );
  }

  const empty = emptyBucket();
  return buildMetrics(
    empty,
    ctx,
    'unavailable',
    false,
    true,
    'CLI reported no usage and no output was captured',
  );
}

/**
 * Merge two measurements of the same round.
 *
 * Used for transient-error retries (each attempt is billed) and for
 * `retry_task` / `resume_task`, which reuse the SAME round number and must add
 * up instead of overwriting.
 */
export function accumulateTokenUsage(
  existing: TokenUsageMetrics | undefined,
  incoming: TokenUsageMetrics | undefined,
): TokenUsageMetrics | undefined {
  if (!existing) {
    return incoming ? { ...incoming, attempts: incoming.attempts ?? 1 } : undefined;
  }
  if (!incoming) return existing;

  const merged: TokenUsageMetrics = {
    ...existing,
    uncachedInputTokens: existing.uncachedInputTokens + incoming.uncachedInputTokens,
    cacheReadTokens: existing.cacheReadTokens + incoming.cacheReadTokens,
    cacheWriteTokens: existing.cacheWriteTokens + incoming.cacheWriteTokens,
    promptTokens: existing.promptTokens + incoming.promptTokens,
    outputTokens: existing.outputTokens + incoming.outputTokens,
    totalTokens: existing.totalTokens + incoming.totalTokens,
    attempts: (existing.attempts ?? 1) + (incoming.attempts ?? 1),
    available: existing.available || incoming.available,
    partial: existing.partial || incoming.partial ? true : undefined,
    source:
      existing.source === 'native_reported' && incoming.source === 'native_reported'
        ? 'native_reported'
        : existing.source === 'unavailable'
          ? incoming.source
          : existing.source === 'estimated'
            ? 'estimated'
            : incoming.source,
  };

  if (existing.reasoningTokens !== undefined || incoming.reasoningTokens !== undefined) {
    merged.reasoningTokens = (existing.reasoningTokens ?? 0) + (incoming.reasoningTokens ?? 0);
  }
  if (existing.nativeTotalTokens !== undefined || incoming.nativeTotalTokens !== undefined) {
    merged.nativeTotalTokens =
      (existing.nativeTotalTokens ?? 0) + (incoming.nativeTotalTokens ?? 0);
  }
  if (existing.costUsd !== undefined || incoming.costUsd !== undefined) {
    merged.costUsd = (existing.costUsd ?? 0) + (incoming.costUsd ?? 0);
    merged.costSource = 'native';
  }
  merged.reportedModel = incoming.reportedModel || existing.reportedModel;
  merged.model = incoming.model || existing.model;

  const notes = [existing.notes, incoming.notes].filter(Boolean) as string[];
  merged.notes = notes.length > 0 ? Array.from(new Set(notes)).join('; ') : undefined;

  return merged;
}

/**
 * Task-level rollup, derived from the rounds on every read.
 *
 * Never persisted: a stored aggregate can be rolled back by a stale process
 * writing an old copy of the task.
 */
export function aggregateThreadTokenUsage(rounds: DelegationRound[]): ThreadTokenUsageSummary {
  const summary: ThreadTokenUsageSummary = {
    totalUncachedInputTokens: 0,
    totalCacheReadTokens: 0,
    totalCacheWriteTokens: 0,
    totalPromptTokens: 0,
    totalOutputTokens: 0,
    totalTokens: 0,
    byAgent: {},
    byModel: {},
    isPartialOrEstimated: false,
    measuredRounds: 0,
    totalRounds: (rounds || []).length,
  };

  for (const round of rounds || []) {
    const usage = round.tokenUsage;
    if (!usage) {
      summary.isPartialOrEstimated = true;
      continue;
    }
    if (!usage.available) {
      summary.isPartialOrEstimated = true;
      continue;
    }

    summary.measuredRounds += 1;
    summary.totalUncachedInputTokens += usage.uncachedInputTokens;
    summary.totalCacheReadTokens += usage.cacheReadTokens;
    summary.totalCacheWriteTokens += usage.cacheWriteTokens;
    summary.totalPromptTokens += usage.promptTokens;
    summary.totalOutputTokens += usage.outputTokens;
    summary.totalTokens += usage.totalTokens;

    if (usage.source !== 'native_reported' || usage.partial) {
      summary.isPartialOrEstimated = true;
    }
    if (usage.costUsd !== undefined) {
      summary.totalCostUsd = (summary.totalCostUsd ?? 0) + usage.costUsd;
      summary.costSource = 'native';
    }

    const agent = round.executorAgent || usage.agent;
    if (agent) {
      summary.byAgent[agent] = (summary.byAgent[agent] ?? 0) + usage.totalTokens;
    }
    const model = round.executorModel || usage.model;
    if (model) {
      summary.byModel[model] = (summary.byModel[model] ?? 0) + usage.totalTokens;
    }
  }

  if (summary.totalCostUsd !== undefined) {
    summary.totalCostUsd = Number(summary.totalCostUsd.toFixed(6));
  }
  return summary;
}

/** Compact human-readable token count (`23.5k`). */
export function formatTokenCount(tokens: number | undefined): string {
  if (tokens === undefined || !Number.isFinite(tokens)) return '—';
  const value = Math.max(0, Math.round(tokens));
  if (value < 1000) return String(value);
  if (value < 1_000_000) return `${(value / 1000).toFixed(value < 10_000 ? 1 : 0)}k`;
  return `${(value / 1_000_000).toFixed(2)}M`;
}
