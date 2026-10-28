import { test, describe, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { TaskStore } from '../bridge/task-store';
import {
  CONTEXT_COMPACTION_TAG,
  ContextCompactor,
  appendCompactionProtocol,
  buildCompactionProtocol,
  compactRoundBestEffort,
  extractCompactionCapsule,
  parseCapsuleBody,
} from '../bridge/context-compactor';
import { saveBridgePolicy } from '../bridge/bridge-policy';
import {
  findLatestExecutionLog,
  readTextFilePaged,
  roundExecutionLogPath,
  roundOutputArchivePath,
} from '../bridge/round-log';
import { DelegationRound, DelegationThread } from '../bridge/types';

const cleanups: Array<() => void> = [];

function makeStore(): { store: TaskStore; dir: string } {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-compact-'));
  const store = new TaskStore(dir);
  cleanups.push(() => {
    store.dispose();
    try {
      fs.rmSync(dir, { recursive: true, force: true });
    } catch {}
  });
  return { store, dir };
}

function makeRound(
  round: number,
  output: string,
  extra: Partial<DelegationRound> = {},
): DelegationRound {
  return {
    round,
    type: round === 1 ? 'initial' : 'rework',
    prompt: `instruction ${round}`,
    output,
    durationMs: 1,
    status: 'completed',
    timestamp: new Date().toISOString(),
    ...extra,
  };
}

function makeThread(rounds: DelegationRound[], over: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: `thread_compact_${Math.random().toString(16).slice(2, 8)}`,
    title: 'compaction fixture',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'waiting_review',
    currentRound: rounds.length,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: 'session-12345678',
    receiverSessionCaptured: true,
    workspacePath: process.cwd(),
    summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
    rounds,
    ...over,
  };
}

describe('context compaction - protocol and capsule parsing', () => {
  test('the protocol is appended only for resumed rounds and names the previous round', () => {
    assert.strictEqual(appendCompactionProtocol('fix it', 1), 'fix it');
    assert.strictEqual(appendCompactionProtocol('fix it', undefined), 'fix it');

    const withProtocol = appendCompactionProtocol('fix it', 3);
    assert.ok(withProtocol.startsWith('fix it'));
    assert.match(withProtocol, new RegExp(`<${CONTEXT_COMPACTION_TAG}>`));
    assert.match(withProtocol, /第 2 轮/);
    assert.match(withProtocol, /第 3 轮/);
    // The caller's instruction must survive verbatim at the head.
    assert.strictEqual(buildCompactionProtocol(2).includes('第 1 轮'), true);
  });

  test('parses a Chinese capsule emitted inside a normal final answer', () => {
    const output = [
      '已修复两处编译错误，测试通过。',
      '',
      '<context_compaction>',
      '- 核心决策: 统一改用 registry.clock，避免墙钟与单调钟混用',
      '- 涉及文件: SharedRoom.kt, TcpTableServer.kt',
      '- 已消解问题: 房间被提前回收；观察者断线后 isConnected 残留',
      '- 长效约束: 所有 TTL 比较必须使用单调时钟',
      '</context_compaction>',
      '',
      '请验收。',
    ].join('\n');

    const capsule = extractCompactionCapsule(output);
    assert.ok(capsule);
    assert.match(capsule!.headline, /registry\.clock/);
    assert.deepStrictEqual(capsule!.touchedFiles, ['SharedRoom.kt', 'TcpTableServer.kt']);
    assert.strictEqual(capsule!.resolvedIssues.length, 1);
    assert.match(capsule!.longTermConstraints[0], /单调时钟/);
  });

  test('tolerates an unclosed tag and empty content', () => {
    const unclosed = extractCompactionCapsule(
      'done\n<context_compaction>\n- 核心决策: only one line',
    );
    assert.ok(unclosed);
    assert.match(unclosed!.headline, /only one line/);
    assert.strictEqual(extractCompactionCapsule('no tag here'), undefined);
    assert.strictEqual(extractCompactionCapsule('<context_compaction></context_compaction>'), undefined);
  });

  test('a plain answer without a structured key still yields a headline', () => {
    const capsule = parseCapsuleBody('重构了缓存层\n补充了测试');
    assert.ok(capsule);
    assert.match(capsule!.headline, /重构了缓存层/);
    assert.deepStrictEqual(capsule!.resolvedIssues, []);
  });
});

describe('context compaction - lagged ledger rule', () => {
  test('condenses the previous round from the capsule of the current round', () => {
    const { store, dir } = makeStore();
    const original = 'E'.repeat(5_000);
    const thread = makeThread([
      makeRound(1, original, { touchedFiles: ['src/a.ts'] }),
      makeRound(2, 'done\n<context_compaction>\n- 核心决策: 改为惰性加载\n- 涉及文件: src/a.ts\n</context_compaction>'),
    ]);
    store.upsertTask(thread);

    const compactor = new ContextCompactor(store, { enabled: true, minCompactionChars: 100 });
    const outcome = compactor.compactPrecedingRound({
      threadId: thread.threadId,
      currentRoundNumber: 2,
      currentOutput: thread.rounds[1].output,
    });

    assert.strictEqual(outcome.status, 'compacted');
    assert.strictEqual(outcome.targetRound, 1);
    assert.strictEqual(outcome.source, 'agent_protocol_envelope');

    const stored = store.getTask(thread.threadId)!;
    const first = stored.rounds.find((r) => r.round === 1)!;
    const second = stored.rounds.find((r) => r.round === 2)!;

    // The current round must stay exactly as the agent produced it.
    assert.strictEqual(second.output, thread.rounds[1].output);
    assert.strictEqual(second.isCompacted, undefined);

    assert.strictEqual(first.isCompacted, true);
    assert.strictEqual(first.compactSummary?.metrics.source, 'agent_protocol_envelope');
    assert.strictEqual(first.compactSummary?.metrics.originalChars, original.length);
    assert.ok(first.compactSummary!.metrics.compressionRatio < 1);
    assert.match(first.output || '', /改为惰性加载/);
    assert.match(first.output || '', /核心决策/);
    // touchedFiles come from the git probe, never from the agent's claim.
    assert.deepStrictEqual(first.compactSummary?.touchedFiles, ['src/a.ts']);

    // The exact original is archived, byte for byte.
    const archive = roundOutputArchivePath(thread.threadId, 1, dir);
    assert.strictEqual(fs.readFileSync(archive, 'utf8'), original);
    assert.strictEqual(first.rawOutputFilePath, archive);
  });

  test('falls back to a deterministic head/tail card when the agent does not comply', () => {
    const { store } = makeStore();
    const head = 'HEAD-MARKER\n' + 'a'.repeat(2_000);
    const tail = 'b'.repeat(2_000) + '\nTAIL-MARKER';
    const thread = makeThread([
      makeRound(1, `${head}\n${tail}`),
      makeRound(2, 'no protocol block here'),
    ]);
    store.upsertTask(thread);

    const outcome = new ContextCompactor(store, {
      enabled: true,
      minCompactionChars: 100,
    }).compactPrecedingRound({ threadId: thread.threadId, currentRoundNumber: 2 });

    assert.strictEqual(outcome.status, 'compacted');
    assert.strictEqual(outcome.source, 'deterministic_truncation');
    const first = store.getTask(thread.threadId)!.rounds[0];
    assert.strictEqual(first.isCompacted, true);
    assert.match(first.output || '', /HEAD-MARKER/);
    assert.match(first.output || '', /TAIL-MARKER/);
    assert.match(first.output || '', /全文已完整归档/);
  });

  test('never replaces a small output with a bigger card', () => {
    const { store, dir } = makeStore();
    const original = 's'.repeat(1_600);
    const thread = makeThread([makeRound(1, original), makeRound(2, 'current')]);
    store.upsertTask(thread);

    const outcome = new ContextCompactor(store, {
      enabled: true,
      minCompactionChars: 100,
    }).compactPrecedingRound({ threadId: thread.threadId, currentRoundNumber: 2 });

    assert.strictEqual(outcome.status, 'skipped');
    assert.strictEqual(outcome.reason, 'no_size_benefit');
    const first = store.getTask(thread.threadId)!.rounds[0];
    assert.strictEqual(first.output, original);
    assert.strictEqual(first.isCompacted, undefined);
    // Nothing was archived for a compaction that did not happen.
    assert.strictEqual(fs.existsSync(roundOutputArchivePath(thread.threadId, 1, dir)), false);
  });

  test('a failed previous round is archived too, but its error stays verbatim', () => {
    const { store } = makeStore();
    const thread = makeThread([
      makeRound(1, 'partial output '.repeat(500), {
        status: 'failed',
        error: 'compileKotlin failed',
      }),
      makeRound(2, 'recovered'),
    ]);
    store.upsertTask(thread);

    const outcome = new ContextCompactor(store, {
      enabled: true,
      minCompactionChars: 100,
    }).compactPrecedingRound({ threadId: thread.threadId, currentRoundNumber: 2 });

    assert.strictEqual(outcome.status, 'compacted');
    const first = store.getTask(thread.threadId)!.rounds[0];
    assert.strictEqual(first.error, 'compileKotlin failed');
    assert.strictEqual(first.status, 'failed');
    assert.match(first.output || '', /本轮失败/);
  });

  test('never condenses the synthetic approval round or a short round', () => {
    const { store } = makeStore();
    const thread = makeThread([
      makeRound(1, 'Approved and verified.', { type: 'approval' }),
      makeRound(2, 'short rework output'),
    ]);
    store.upsertTask(thread);
    const compactor = new ContextCompactor(store, { enabled: true, minCompactionChars: 10 });

    // Round 1 is approval (excluded); nothing else is old enough.
    assert.strictEqual(compactor.findCompactionTarget(thread, 2, 10), undefined);

    const outcome = compactor.compactPrecedingRound({
      threadId: thread.threadId,
      currentRoundNumber: 2,
    });
    assert.strictEqual(outcome.status, 'skipped');
    assert.strictEqual(outcome.reason, 'no_eligible_round');
  });

  test('an undersized immediate round does not block an older oversized one', () => {
    const { store } = makeStore();
    const thread = makeThread([
      makeRound(1, 'x'.repeat(4_000)),
      makeRound(2, 'tiny'),
      makeRound(3, 'current'),
    ]);
    store.upsertTask(thread);

    const target = new ContextCompactor(store, {
      enabled: true,
      minCompactionChars: 100,
    }).findCompactionTarget(thread, 3, 100);
    assert.strictEqual(target?.round, 1);
  });

  test('is idempotent: a second pass does nothing', () => {
    const { store } = makeStore();
    const thread = makeThread([makeRound(1, 'z'.repeat(3_000)), makeRound(2, 'current')]);
    store.upsertTask(thread);
    const compactor = new ContextCompactor(store, { enabled: true, minCompactionChars: 100 });

    assert.strictEqual(
      compactor.compactPrecedingRound({ threadId: thread.threadId, currentRoundNumber: 2 }).status,
      'compacted',
    );
    const card = store.getTask(thread.threadId)!.rounds[0].output;

    const second = compactor.compactPrecedingRound({
      threadId: thread.threadId,
      currentRoundNumber: 2,
    });
    assert.strictEqual(second.status, 'skipped');
    assert.strictEqual(store.getTask(thread.threadId)!.rounds[0].output, card);
  });

  test('respects the mirrored opt-out policy', () => {
    const { store, dir } = makeStore();
    saveBridgePolicy({ contextCompaction: false }, dir);
    const thread = makeThread([makeRound(1, 'y'.repeat(3_000)), makeRound(2, 'current')]);
    store.upsertTask(thread);

    const outcome = compactRoundBestEffort(store, {
      threadId: thread.threadId,
      currentRoundNumber: 2,
    });
    assert.strictEqual(outcome.status, 'skipped');
    assert.strictEqual(outcome.reason, 'compaction_disabled');
    assert.strictEqual(store.getTask(thread.threadId)!.rounds[0].isCompacted, undefined);
    assert.strictEqual(fs.existsSync(roundOutputArchivePath(thread.threadId, 1, dir)), false);
  });

  test('a failing archive returns a failed outcome instead of throwing', () => {
    const { store, dir } = makeStore();
    const blocker = path.join(dir, 'not-a-directory');
    fs.writeFileSync(blocker, 'x', 'utf8');
    const thread = makeThread([makeRound(1, 'q'.repeat(3_000)), makeRound(2, 'current')]);
    store.upsertTask(thread);

    const outcome = new ContextCompactor(store, {
      enabled: true,
      minCompactionChars: 100,
      homeDir: blocker,
    }).compactPrecedingRound({ threadId: thread.threadId, currentRoundNumber: 2 });

    assert.strictEqual(outcome.status, 'failed');
    assert.match(outcome.reason || '', /archive/i);
    // Nothing was condensed, so no data was lost.
    assert.strictEqual(store.getTask(thread.threadId)!.rounds[0].isCompacted, undefined);
  });

  test('a stale writer cannot resurrect a compacted round', () => {
    const { store } = makeStore();
    const thread = makeThread([makeRound(1, 'w'.repeat(3_000)), makeRound(2, 'current')]);
    store.upsertTask(thread);
    const staleCopy = JSON.parse(JSON.stringify(store.getTask(thread.threadId)!)) as DelegationThread;

    new ContextCompactor(store, { enabled: true, minCompactionChars: 100 }).compactPrecedingRound({
      threadId: thread.threadId,
      currentRoundNumber: 2,
    });
    const compacted = store.getTask(thread.threadId)!.rounds[0];

    // A process holding a pre-compaction copy writes it back.
    store.upsertTask(staleCopy);

    const after = store.getTask(thread.threadId)!.rounds.find((r) => r.round === 1)!;
    assert.strictEqual(after.isCompacted, true);
    assert.strictEqual(after.output, compacted.output);
    assert.strictEqual(after.rawOutputFilePath, compacted.rawOutputFilePath);
  });
});

describe('context compaction - round log helpers', () => {
  test('latest execution log is chosen numerically, not lexicographically', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-roundlog-'));
    cleanups.push(() => {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    });
    const logs = path.join(dir, 'logs');
    fs.mkdirSync(logs, { recursive: true });
    const threadId = 'thread_numeric';
    for (const round of [1, 9, 10, 11]) {
      fs.writeFileSync(roundExecutionLogPath(threadId, round, dir), `round ${round}`, 'utf8');
    }

    assert.strictEqual(findLatestExecutionLog(threadId, dir)?.round, 11);
  });

  test('paged reads honor offsets and clamp the window', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-roundlog-'));
    cleanups.push(() => {
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    });
    const file = path.join(dir, 'payload.txt');
    const payload = '0123456789'.repeat(50);
    fs.writeFileSync(file, payload, 'utf8');

    const page = readTextFilePaged(file, { offsetBytes: 10, maxBytes: 20 });
    assert.strictEqual(page.content, payload.slice(10, 30));
    assert.strictEqual(page.startOffset, 10);
    assert.strictEqual(page.nextOffset, 30);
    assert.strictEqual(page.truncated, true);

    const tail = readTextFilePaged(file, { offsetBytes: payload.length - 5, maxBytes: 100 });
    assert.strictEqual(tail.content, payload.slice(-5));
    assert.strictEqual(tail.truncated, true);

    const all = readTextFilePaged(file, { maxBytes: 100_000 });
    assert.strictEqual(all.content, payload);
    assert.strictEqual(all.truncated, false);
  });
});

process.on('exit', () => {
  for (const cleanup of cleanups) {
    try {
      cleanup();
    } catch {}
  }
});

// `TaskStore` watches its ledger file, which keeps the event loop alive; drain
// the fixtures after every test so the runner can exit.
afterEach(() => {
  while (cleanups.length > 0) {
    try {
      cleanups.pop()!();
    } catch {}
  }
});
