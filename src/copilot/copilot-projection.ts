import { DelegationRound, DelegationThread, ThreadTokenUsageSummary, TokenUsageMetrics } from '../bridge/types';

export const COPILOT_MAX_ROUNDS = 4;
const MAX_TEXT = 8_000;
const MAX_FILES = 100;
const MAX_MODELS = 20;

export function isCopilotRequester(thread: Pick<DelegationThread, 'requesterAgent' | 'requesterBinding'>): boolean {
  return thread.requesterAgent === 'vscode_chat' || thread.requesterBinding?.kind === 'vscode-copilot';
}

function limited(value: unknown, max = MAX_TEXT): { value: string; truncated: boolean; originalLength: number } {
  const text = typeof value === 'string' ? value : '';
  return { value: text.slice(0, max), truncated: text.length > max, originalLength: text.length };
}

export function projectCopilotTokenUsage(usage: ThreadTokenUsageSummary) {
  const models = Object.entries(usage.byModel || {}).sort((a, b) => b[1] - a[1]).slice(0, MAX_MODELS);
  return {
    totalUncachedInputTokens: usage.totalUncachedInputTokens,
    totalCacheReadTokens: usage.totalCacheReadTokens,
    totalCacheWriteTokens: usage.totalCacheWriteTokens,
    totalPromptTokens: usage.totalPromptTokens,
    totalOutputTokens: usage.totalOutputTokens,
    totalTokens: usage.totalTokens,
    totalCostUsd: usage.totalCostUsd,
    costSource: usage.costSource,
    byAgent: usage.byAgent,
    byModel: Object.fromEntries(models.map(([model, tokens]) => [model.slice(0, 200), tokens])),
    byModelTruncated: Object.keys(usage.byModel || {}).length > models.length,
    isPartialOrEstimated: usage.isPartialOrEstimated,
    measuredRounds: usage.measuredRounds,
    totalRounds: usage.totalRounds,
  };
}

export function projectCopilotRoundTokenUsage(usage: TokenUsageMetrics | undefined) {
  if (!usage) return null;
  return {
    uncachedInputTokens: usage.uncachedInputTokens,
    cacheReadTokens: usage.cacheReadTokens,
    cacheWriteTokens: usage.cacheWriteTokens,
    promptTokens: usage.promptTokens,
    outputTokens: usage.outputTokens,
    reasoningTokens: usage.reasoningTokens,
    totalTokens: usage.totalTokens,
    nativeTotalTokens: usage.nativeTotalTokens,
    agent: usage.agent,
    model: limited(usage.model, 200).value || undefined,
    reportedModel: limited(usage.reportedModel, 200).value || undefined,
    requestedModel: limited(usage.requestedModel, 200).value || undefined,
    costUsd: usage.costUsd,
    costSource: usage.costSource,
    source: usage.source,
    available: usage.available,
    partial: usage.partial,
    attempts: usage.attempts,
    notes: limited(usage.notes, 500).value || undefined,
    collectedAt: limited(usage.collectedAt, 100).value,
  };
}

function projectRound(round: DelegationRound) {
  const prompt = limited(round.prompt);
  const output = limited(round.output);
  const error = limited(round.error, 1_000);
  return {
    round: round.round,
    type: round.type,
    status: round.status,
    timestamp: round.timestamp,
    durationMs: round.durationMs,
    prompt: prompt.value,
    promptTruncated: prompt.truncated,
    promptOriginalLength: prompt.originalLength,
    output: output.value,
    outputTruncated: output.truncated,
    outputOriginalLength: output.originalLength,
    ...(error.value ? { error: error.value, errorTruncated: error.truncated } : {}),
    touchedFiles: (round.touchedFiles || []).slice(0, MAX_FILES).map((file) => file.slice(0, 300)),
    touchedFilesTruncated: (round.touchedFiles || []).length > MAX_FILES,
  };
}

export function projectCopilotTaskDetails(
  task: DelegationThread,
  tokenUsage: ThreadTokenUsageSummary,
) {
  const rounds = task.rounds || [];
  const visibleRounds = rounds.slice(-COPILOT_MAX_ROUNDS);
  const summary = task.summary;
  const lastPrompt = limited(summary?.lastPromptSnippet, 1_000);
  const lastOutput = limited(summary?.lastOutputSnippet, 1_000);
  const finalSummary = task.finalSummary;

  return {
    threadId: task.threadId,
    title: limited(task.title, 240).value,
    status: task.status,
    currentRound: task.currentRound,
    createdAt: task.createdAt,
    updatedAt: task.updatedAt,
    requesterAgent: 'vscode_chat' as const,
    requesterModel: null,
    requesterModelSource: 'none' as const,
    reviewerAgent: task.initiatorAgent,
    reviewerModel: task.reviewerModel || null,
    targetAgent: task.targetAgent,
    targetModel: task.targetModel || null,
    reviewBootstrapPhase: task.reviewBootstrap?.phase || null,
    notificationDelivery: 'vscode-ui' as const,
    nativeChatWakeSupported: false as const,
    summary: {
      lastPromptSnippet: lastPrompt.value,
      lastPromptTruncated: lastPrompt.truncated,
      lastOutputSnippet: lastOutput.value,
      lastOutputTruncated: lastOutput.truncated,
      totalDurationMs: summary?.totalDurationMs || 0,
    },
    rounds: visibleRounds.map(projectRound),
    omittedRoundCount: rounds.length - visibleRounds.length,
    historyTruncated: rounds.length > visibleRounds.length || visibleRounds.some((round) =>
      (round.prompt || '').length > MAX_TEXT || (round.output || '').length > MAX_TEXT,
    ),
    tokenUsage: projectCopilotTokenUsage(tokenUsage),
    archived: !!task.archivedAt,
    finalSummary: finalSummary ? {
      kind: finalSummary.kind,
      at: finalSummary.at,
      title: limited(finalSummary.title, 240).value,
      verdict: limited(finalSummary.verdict, 1_000).value,
      summary: limited(finalSummary.summary).value,
      summaryTruncated: (finalSummary.summary || '').length > MAX_TEXT,
      changedFiles: (finalSummary.changedFiles || []).slice(0, MAX_FILES).map((file) => file.slice(0, 300)),
      error: limited(finalSummary.error, 1_000).value || undefined,
      rounds: finalSummary.rounds,
      reviewerAgent: finalSummary.reviewerAgent || task.initiatorAgent,
      requesterAgent: 'vscode_chat' as const,
      executorAgent: finalSummary.executorAgent,
      mergeRequest: finalSummary.mergeRequest ? {
        url: finalSummary.mergeRequest.url,
        branch: finalSummary.mergeRequest.branch,
        commit: finalSummary.mergeRequest.commit,
        status: finalSummary.mergeRequest.status,
        summary: limited(finalSummary.mergeRequest.summary, 1_000).value,
      } : undefined,
      totalTokens: finalSummary.totalTokens,
      promptTokens: finalSummary.promptTokens,
      outputTokens: finalSummary.outputTokens,
    } : null,
    ...(rounds.some((round) =>
      (round.prompt || '').length > MAX_TEXT || (round.output || '').length > MAX_TEXT,
    ) ? { truncationHint: `Round text is capped at ${MAX_TEXT} characters; at most ${COPILOT_MAX_ROUNDS} rounds are returned.` } : {}),
  };
}

export function projectCopilotTaskSummary(task: DelegationThread, summary: any) {
  return {
    threadId: task.threadId,
    title: limited(task.title, 240).value,
    status: task.status,
    archived: !!task.archivedAt,
    ...(task.archivedAt ? { archivedAt: task.archivedAt } : {}),
    createdAt: task.createdAt,
    updatedAt: task.updatedAt,
    requesterAgent: task.requesterAgent || task.initiatorAgent,
    reviewerAgent: task.initiatorAgent,
    reviewerModel: task.reviewerModel || null,
    targetAgent: task.targetAgent,
    targetModel: task.targetModel || null,
    requesterModel: task.requesterAgent === 'vscode_chat' ? null : (task.requesterModel || task.initiatorModel || null),
    requesterModelSource: task.requesterAgent === 'vscode_chat' ? 'none' : (task.requesterModelSource || 'unknown'),
    mergeRequest: task.mergeRequest ? {
      url: task.mergeRequest.url,
      branch: task.mergeRequest.branch,
      commit: task.mergeRequest.commit,
      status: task.mergeRequest.status,
      summary: limited(task.mergeRequest.summary, 1_000).value,
    } : null,
    finalSummary: summary ? {
      kind: summary.kind,
      at: summary.at,
      title: limited(summary.title, 240).value,
      verdict: limited(summary.verdict, 1_000).value,
      summary: limited(summary.summary).value,
      summaryTruncated: (summary.summary || '').length > MAX_TEXT,
      changedFiles: (summary.changedFiles || []).slice(0, MAX_FILES).map((file: string) => file.slice(0, 300)),
      error: limited(summary.error, 1_000).value || undefined,
      rounds: summary.rounds,
      requesterAgent: task.requesterAgent || task.initiatorAgent,
      reviewerAgent: task.initiatorAgent,
      executorAgent: summary.executorAgent,
      totalTokens: summary.totalTokens,
      promptTokens: summary.promptTokens,
      outputTokens: summary.outputTokens,
    } : null,
    ...(!summary ? { hint: 'The task is still running; the read-only digest is written when it reaches a terminal state.' } : {}),
    ...(summary?.summary?.length > MAX_TEXT ? { truncationHint: `Summary text is capped at ${MAX_TEXT} characters.` } : {}),
  };
}
