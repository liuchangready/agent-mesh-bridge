/**
 * Lagged (N-1) context compaction for the hot ledger.
 *
 * Policy, deliberately different from the original design draft:
 *
 *  1. The CURRENT round is never touched: it is the reviewer's evidence.
 *  2. Only an older, already-superseded round is condensed, and its exact text
 *     is archived first — compaction never destroys data.
 *  3. The receiver is asked to emit a structured `<context_compaction>` capsule
 *     for the previous round (semantic, preferred). If it does not, the bridge
 *     falls back to a deterministic head/tail excerpt plus a pointer. Agent
 *     compliance is an optimisation, never a correctness requirement.
 *  4. Compaction is strictly best-effort: it returns an outcome instead of
 *     throwing, because it runs between "round finalized" and "reviewer woken"
 *     and a throw there would strand the queue head forever.
 *
 * It never runs an extra agent process. Spawning `claude -p "/compact"` would
 * compact the WHOLE native session (losing the current round the next round
 * needs), cannot be targeted at N-1, and its headless support is unverified.
 */

import * as fs from 'fs';
import * as path from 'path';
import {
  CompactionSource,
  CompactedContextSummary,
  DelegationRound,
  DelegationThread,
} from './types';
import type { TaskStore } from './task-store';
import {
  contextCompactionMinChars,
  isContextCompactionEnabled,
} from './bridge-policy';
import {
  formatBytes,
  resolveExistingArtifactPath,
  roundExecutionLogPath,
  roundOutputArchivePath,
} from './round-log';

export const CONTEXT_COMPACTION_TAG = 'context_compaction';

/** Hard bound on the agent-produced capsule we are willing to trust/store. */
export const MAX_CAPSULE_BODY_CHARS = 4_000;
/** Hard bound on the condensed card kept in the hot ledger. */
export const DEFAULT_MAX_CARD_CHARS = 2_400;
const CARD_HEAD_CHARS = 900;
const CARD_TAIL_CHARS = 500;

export interface ParsedCompactionCapsule {
  headline: string;
  touchedFiles: string[];
  resolvedIssues: string[];
  longTermConstraints: string[];
  /** Raw body, kept for audit/metrics only. */
  rawBody: string;
}

export interface CompactionOutcome {
  status: 'compacted' | 'skipped' | 'failed';
  threadId: string;
  currentRound: number;
  targetRound?: number;
  reason?: string;
  source?: CompactionSource;
  originalChars?: number;
  compactedChars?: number;
  archivePath?: string;
  executionLogPath?: string;
}

export interface ContextCompactorOptions {
  /** Bridge home; defaults to the directory holding `delegation_threads.json`. */
  homeDir?: string;
  /** Override the mirrored policy (tests). */
  enabled?: boolean;
  minCompactionChars?: number;
  maxCardChars?: number;
}

/**
 * System protocol appended to a round's instruction at launch time only.
 *
 * It is appended to the *executed* prompt, not to `round.prompt`: the ledger
 * keeps exactly what the caller wrote, so `repeatOfRound` detection and the UI
 * are unaffected.
 */
export function buildCompactionProtocol(currentRound: number): string {
  const previous = currentRound - 1;
  return [
    '',
    '[系统协约：上下文自管理协议]',
    `你正在参与 Agent Mesh Bridge 的多轮协作。本轮是第 ${currentRound} 轮，第 ${previous} 轮的过程仍在你当前会话历史中。`,
    `为抑制长会话膨胀，请在完成本轮全部代码与交付之后，于最终回复的最末尾追加一个提纯块，`,
    `对【第 ${previous} 轮】做结构化提纯（过滤已解决的临时日志，只保留长效信息）：`,
    '',
    `<${CONTEXT_COMPACTION_TAG}>`,
    '- 核心决策: (1-2 句)',
    '- 涉及文件: (核心变动文件路径，逗号分隔)',
    '- 已消解问题: (上一轮遇到但本轮已解决的报错/试错)',
    '- 长效约束: (后续轮次必须遵守的接口/架构约定)',
    `</${CONTEXT_COMPACTION_TAG}>`,
    '',
    '约束：该块只是账本归档元数据，不得替代本轮交付说明；每项 1-3 句，整块不超过 600 字；',
    '若没有可提纯的内容，输出一对空标签即可。',
  ].join('\n');
}

/** Append the protocol for resume/rework rounds only (round 1 has no history). */
export function appendCompactionProtocol(prompt: string, roundNumber?: number): string {
  if (!roundNumber || roundNumber < 2) return prompt;
  return `${prompt}${buildCompactionProtocol(roundNumber)}`;
}

function normalizeKey(value: string): string {
  const key = value.trim().replace(/[*_`]/g, '').toLowerCase();
  if (key.includes('核心决策') || key === 'headline' || key === 'decision') return 'headline';
  if (key.includes('涉及文件') || key === 'files' || key === 'touchedfiles') return 'files';
  if (key.includes('已消解') || key === 'resolved' || key === 'resolvedissues') return 'resolved';
  if (key.includes('长效约束') || key.includes('约束') || key === 'constraints')
    return 'constraints';
  return '';
}

function splitList(value: string): string[] {
  return value
    .split(/[,，;；、\n]/)
    .map((item) => item.trim().replace(/^[-*]\s*/, '').replace(/[`'"]/g, ''))
    .filter((item) => item.length > 0 && item.length <= 300)
    .slice(0, 40);
}

/** Parse the body of a `<context_compaction>` block. */
export function parseCapsuleBody(body: string): ParsedCompactionCapsule | undefined {
  const rawBody = (body || '').trim().slice(0, MAX_CAPSULE_BODY_CHARS);
  if (!rawBody) return undefined;

  const buckets: Record<string, string[]> = {
    headline: [],
    files: [],
    resolved: [],
    constraints: [],
  };
  let current = '';

  for (const line of rawBody.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    const match = /^[-*•]?\s*([^:：]{1,24})\s*[:：]\s*(.*)$/.exec(trimmed);
    if (match) {
      const bucket = normalizeKey(match[1]);
      if (bucket) {
        current = bucket;
        if (match[2].trim()) buckets[bucket].push(match[2].trim());
        continue;
      }
    }
    if (current && buckets[current].length < 12) {
      buckets[current].push(trimmed);
    }
  }

  const headline =
    buckets.headline.join(' ').trim() ||
    rawBody
      .split(/\r?\n/)
      .map((line) => line.trim())
      .find((line) => line.length > 0 && !line.startsWith('<')) ||
    '';

  const capsule: ParsedCompactionCapsule = {
    headline: headline.slice(0, 600),
    touchedFiles: splitList(buckets.files.join('\n')),
    resolvedIssues: buckets.resolved.map((item) => item.slice(0, 400)).slice(0, 12),
    longTermConstraints: buckets.constraints.map((item) => item.slice(0, 400)).slice(0, 12),
    rawBody,
  };

  // A block that carries no structured key and no usable text is not a capsule.
  if (!capsule.headline && capsule.resolvedIssues.length === 0) return undefined;
  return capsule;
}

/** Extract the capsule from a round's final answer (never from the raw JSONL). */
export function extractCompactionCapsule(output: string): ParsedCompactionCapsule | undefined {
  const text = output || '';
  const closed = new RegExp(
    `<${CONTEXT_COMPACTION_TAG}>([\\s\\S]*?)</${CONTEXT_COMPACTION_TAG}>`,
    'i',
  ).exec(text);
  const unclosed = new RegExp(`<${CONTEXT_COMPACTION_TAG}>([\\s\\S]*)$`, 'i').exec(text);
  const body = closed?.[1] ?? unclosed?.[1];
  if (!body) return undefined;
  return parseCapsuleBody(body);
}

function percent(ratio: number): string {
  return `${(Math.max(0, Math.min(1, ratio)) * 100).toFixed(1)}%`;
}

function renderCapsuleCard(input: {
  round: DelegationRound;
  capsule: ParsedCompactionCapsule;
  agent: string;
  originalChars: number;
  originalBytes: number;
  archivePath: string;
  executionLogPath?: string;
  /** git-probe measured files (authoritative); empty when not a git workspace. */
  touchedFiles: string[];
  maxCardChars: number;
}): string {
  const header = [
    `> 🧠 上下文提纯胶囊 — 第 ${input.round.round} 轮（${input.agent} 自提纯 · ${input.round.type}）`,
    `> 原文 ${input.originalChars.toLocaleString()} 字符（${formatBytes(
      input.originalBytes,
    )}）· 全文已归档`,
    `> 全文归档：\`${input.archivePath}\``,
    input.executionLogPath ? `> 原始执行日志：\`${input.executionLogPath}\`` : '> 原始执行日志：未捕获',
  ];

  const sections: string[] = [];
  if (input.capsule.headline) sections.push(`**核心决策**\n${input.capsule.headline}`);
  // Prefer the git-probe list: the agent's self-report is only a fallback for
  // non-git workspaces, and is labelled as such.
  const authoritativeFiles = input.touchedFiles.filter(Boolean);
  const files = authoritativeFiles.length > 0 ? authoritativeFiles : input.capsule.touchedFiles;
  if (files.length > 0) {
    const label = authoritativeFiles.length > 0 ? '涉及文件（git 实测）' : '涉及文件（Agent 自述）';
    sections.push(`**${label}**\n${files.map((f) => `- ${f}`).join('\n')}`);
  }
  if (input.capsule.resolvedIssues.length > 0) {
    sections.push(
      `**已消解问题**\n${input.capsule.resolvedIssues.map((f) => `- ${f}`).join('\n')}`,
    );
  }
  if (input.capsule.longTermConstraints.length > 0) {
    sections.push(
      `**长效约束**\n${input.capsule.longTermConstraints.map((f) => `- ${f}`).join('\n')}`,
    );
  }

  let card = `${header.join('\n')}\n\n${sections.join('\n\n')}`.trim();
  if (card.length > input.maxCardChars) {
    card = `${card.slice(0, input.maxCardChars)}\n\n…（胶囊已按上限截断，全文见归档）`;
  }
  return card;
}

function renderDeterministicCard(input: {
  round: DelegationRound;
  original: string;
  originalChars: number;
  archivePath: string;
  executionLogPath?: string;
  maxCardChars: number;
  headChars: number;
  tailChars: number;
}): string {
  const statusNote = input.round.status === 'failed' ? '本轮失败 · ' : '';
  const header = [
    `> 🧠 历史轮次已归档 — 第 ${input.round.round} 轮（${statusNote}${input.round.type}）`,
    `> Agent 未产出提纯胶囊，本卡片为确定性截断：原文 ${input.originalChars.toLocaleString()} 字符，全文已完整归档。`,
    `> 全文归档：\`${input.archivePath}\``,
    input.executionLogPath ? `> 原始执行日志：\`${input.executionLogPath}\`` : '> 原始执行日志：未捕获',
  ];
  const head = input.original.slice(0, input.headChars).trim();
  const tail =
    input.original.length > input.headChars
      ? input.original.slice(-input.tailChars).trim()
      : '';

  let card = [
    header.join('\n'),
    '',
    '**输出开头**',
    head,
    ...(tail ? ['', '**输出结尾**', tail] : []),
  ].join('\n');

  if (card.length > input.maxCardChars) {
    card = `${card.slice(0, input.maxCardChars)}\n\n…（已按上限截断，全文见归档）`;
  }
  return card;
}

export class ContextCompactor {
  private readonly store: TaskStore;
  private readonly options: ContextCompactorOptions;
  /** In-process idempotency: the same round is never archived twice. */
  private readonly inFlight = new Set<string>();

  constructor(store: TaskStore, options: ContextCompactorOptions = {}) {
    this.store = store;
    this.options = options;
  }

  private homeDir(): string {
    return this.options.homeDir || path.dirname(this.store.getStoragePath());
  }

  private enabled(): boolean {
    if (this.options.enabled !== undefined) return this.options.enabled;
    return isContextCompactionEnabled(this.homeDir());
  }

  private minChars(): number {
    return this.options.minCompactionChars ?? contextCompactionMinChars(this.homeDir());
  }

  private maxCardChars(): number {
    return this.options.maxCardChars ?? DEFAULT_MAX_CARD_CHARS;
  }

  /**
   * Condense the newest eligible round older than `currentRoundNumber`.
   *
   * Called after a successful round has been persisted and before the developer
   * role is handed to the reviewer. Never throws.
   */
  public compactPrecedingRound(input: {
    threadId: string;
    currentRoundNumber: number;
    /** Final answer of the round that just finished (capsule source). */
    currentOutput?: string;
  }): CompactionOutcome {
    const base: CompactionOutcome = {
      status: 'skipped',
      threadId: input.threadId,
      currentRound: input.currentRoundNumber,
    };

    try {
      if (!this.enabled()) return { ...base, reason: 'compaction_disabled' };

      const thread = this.store.getTask(input.threadId);
      if (!thread) return { ...base, reason: 'thread_not_found' };

      const minChars = this.minChars();
      const target = this.findCompactionTarget(thread, input.currentRoundNumber, minChars);
      if (!target) {
        return {
          ...base,
          reason: (thread.rounds || []).some((r) => r.round < input.currentRoundNumber)
            ? 'no_eligible_round'
            : 'no_previous_round',
        };
      }

      const key = `${input.threadId}#${target.round}`;
      if (this.inFlight.has(key)) {
        return { ...base, targetRound: target.round, reason: 'already_in_progress' };
      }
      this.inFlight.add(key);

      try {
        if (target.isCompacted) {
          return { ...base, targetRound: target.round, reason: 'already_compacted' };
        }

        const original = target.output || '';
        const originalChars = original.length;
        const originalBytes = Buffer.byteLength(original, 'utf8');
        if (originalChars < minChars) {
          return { ...base, targetRound: target.round, reason: 'output_too_small' };
        }

        const home = this.homeDir();
        const archivePath = roundOutputArchivePath(input.threadId, target.round, home);
        const executionLogPath = resolveExistingArtifactPath([
          roundExecutionLogPath(input.threadId, target.round, home),
          roundExecutionLogPath(input.threadId, target.round),
        ]);

        const capsule = extractCompactionCapsule(input.currentOutput || '');
        const source: CompactionSource = capsule
          ? 'agent_protocol_envelope'
          : 'deterministic_truncation';

        const card = capsule
          ? renderCapsuleCard({
              round: target,
              capsule,
              agent: thread.targetAgent,
              originalChars,
              originalBytes,
              archivePath,
              executionLogPath,
              touchedFiles: target.touchedFiles || [],
              maxCardChars: this.maxCardChars(),
            })
          : renderDeterministicCard({
              round: target,
              original,
              originalChars,
              archivePath,
              executionLogPath,
              maxCardChars: this.maxCardChars(),
              headChars: CARD_HEAD_CHARS,
              tailChars: CARD_TAIL_CHARS,
            });

        // Never trade a readable output for a bigger card (headers cost ~400
        // chars, so small rounds could otherwise grow). No benefit, no change.
        if (card.length >= originalChars) {
          return {
            ...base,
            targetRound: target.round,
            reason: 'no_size_benefit',
            originalChars,
            compactedChars: card.length,
          };
        }

        try {
          fs.mkdirSync(path.dirname(archivePath), { recursive: true });
          writeArchiveOnce(archivePath, original);
        } catch (err: any) {
          // The archive is the safety net: if it cannot be written, do not
          // condense, and report it as an archive failure.
          return {
            ...base,
            targetRound: target.round,
            status: 'failed',
            reason: `failed to archive original output: ${String(err?.message || err)}`,
          };
        }

        const summary: CompactedContextSummary = {
          headline:
            capsule?.headline ||
            `第 ${target.round} 轮（${target.type}）输出已归档，热存储保留首尾摘录。`,
          // Reuse the git-probe measurement; never let the agent's own claim
          // become the authoritative file list.
          touchedFiles: target.touchedFiles || [],
          resolvedIssues: capsule?.resolvedIssues || [],
          longTermConstraints: capsule?.longTermConstraints || [],
          rawOutputPath: archivePath,
          executionLogPath,
          metrics: {
            originalChars,
            compactedChars: card.length,
            compressionRatio: originalChars > 0 ? card.length / originalChars : 0,
            compactedAt: new Date().toISOString(),
            source,
          },
        };

        this.store.loadFromDisk();
        const updated = this.store.updateRound(input.threadId, target.round, {
          output: card,
          isCompacted: true,
          compactSummary: summary,
          rawOutputFilePath: archivePath,
          outputBytes: originalBytes,
        });
        if (!updated) {
          return {
            ...base,
            targetRound: target.round,
            status: 'failed',
            reason: 'store_update_failed',
          };
        }

        console.log(
          `[ContextCompactor] ${input.threadId} round ${target.round}: ` +
            `${originalChars} -> ${card.length} chars (${percent(summary.metrics.compressionRatio)}), ` +
            `source=${source}`,
        );
        return {
          status: 'compacted',
          threadId: input.threadId,
          currentRound: input.currentRoundNumber,
          targetRound: target.round,
          source,
          originalChars,
          compactedChars: card.length,
          archivePath,
          executionLogPath,
        };
      } finally {
        this.inFlight.delete(key);
      }
    } catch (err: any) {
      // Must never escape: callers finalize the round and wake the reviewer
      // right after this call.
      console.warn(
        `[ContextCompactor] Compaction failed for ${input.threadId} ` +
          `(round ${input.currentRoundNumber}): ${err?.message || err}`,
      );
      return { ...base, status: 'failed', reason: String(err?.message || err) };
    }
  }

  /**
   * Highest round below `currentRoundNumber` worth condensing.
   *
   * Failed rounds are eligible too (their `error` field stays untouched); the
   * synthetic `approval` round is not, and neither is anything already
   * condensed or smaller than the threshold.
   */
  public findCompactionTarget(
    thread: DelegationThread,
    currentRoundNumber: number,
    minChars: number = this.minChars(),
  ): DelegationRound | undefined {
    const candidates = (thread.rounds || []).filter(
      (round) =>
        round.round < currentRoundNumber &&
        !round.isCompacted &&
        round.type !== 'approval' &&
        (round.status === 'completed' || round.status === 'failed') &&
        (round.output || '').trim().length >= Math.max(1, minChars),
    );
    if (candidates.length === 0) return undefined;
    const immediate = candidates.find((round) => round.round === currentRoundNumber - 1);
    if (immediate) return immediate;
    return candidates.reduce((a, b) => (b.round > a.round ? b : a));
  }
}

function writeArchiveOnce(archivePath: string, content: string): void {
  const bytes = Buffer.byteLength(content, 'utf8');
  if (fs.existsSync(archivePath) && fs.statSync(archivePath).size === bytes) return;
  fs.writeFileSync(archivePath, content, 'utf8');
}

/** Exposed for tests/UI: render a compact "x -> y chars (z%)" one-liner. */
export function describeCompaction(summary?: CompactedContextSummary): string {
  if (!summary) return '';
  return (
    `${summary.metrics.originalChars} -> ${summary.metrics.compactedChars} chars ` +
    `(${percent(summary.metrics.compressionRatio)} kept)`
  );
}

/**
 * Fire-and-forget entry point for the three finalizers.
 *
 * Kept as a free function so every execution path shares the same safety
 * wrapper: a compaction problem must never stop the reviewer handoff.
 */
export function compactRoundBestEffort(
  store: TaskStore,
  input: { threadId: string; currentRoundNumber: number; currentOutput?: string },
): CompactionOutcome {
  try {
    return new ContextCompactor(store).compactPrecedingRound(input);
  } catch (err: any) {
    console.warn(
      `[ContextCompactor] Unexpected compaction failure for ${input.threadId}: ` +
        `${err?.message || err}`,
    );
    return {
      status: 'failed',
      threadId: input.threadId,
      currentRound: input.currentRoundNumber,
      reason: String(err?.message || err),
    };
  }
}
