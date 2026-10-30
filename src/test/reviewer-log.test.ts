import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  REVIEWER_LOG_SUFFIX,
  createReviewerLogWriter,
  listReviewerLogRounds,
  readReviewerLogTail,
  reviewerLogPath,
  statReviewerLog,
} from '../bridge/reviewer-log';
import { roundExecutionLogPath } from '../bridge/round-log';

function makeRoot(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-reviewer-log-'));
}

describe('Reviewer log artifact', () => {
  test('keeps reviewer logs out of the receiver round-log namespace', () => {
    const root = makeRoot();
    try {
      const reviewer = reviewerLogPath('thread_1', 3, root);
      const receiver = roundExecutionLogPath('thread_1', 3, root);

      assert.notStrictEqual(reviewer, receiver);
      assert.ok(reviewer.endsWith(`_round_3${REVIEWER_LOG_SUFFIX}`));
      // The receiver artifact is `..._round_3.log`; the reviewer name must not
      // be mistaken for it by listRoundArtifacts().
      assert.ok(!/\.log$/.test(path.basename(reviewer)) || reviewer.endsWith('.reviewer.log'));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('sanitizes caller-controlled thread ids and rounds', () => {
    const root = makeRoot();
    try {
      const file = reviewerLogPath('../evil/thread', 0, root);
      assert.strictEqual(path.dirname(file), path.join(root, 'logs'));
      assert.ok(!file.includes('..'));
      assert.ok(path.basename(file).includes('evil_thread_round_1'));
      assert.ok(path.basename(file).endsWith(REVIEWER_LOG_SUFFIX));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('writes a banner, streams chunks and reports its size', () => {
    const root = makeRoot();
    try {
      const writer = createReviewerLogWriter(
        'thread_a',
        2,
        { agent: 'codex', sessionId: 'sess-1', kind: 'review', note: 'title | receiver=claude' },
        { root },
      );
      writer.write('reviewer says hello\n');
      writer.write(Buffer.from('and more\n'));
      writer.writeOutcome({ exitCode: 0 });
      writer.close();

      const stat = statReviewerLog('thread_a', 2, root);
      assert.strictEqual(stat.exists, true);
      assert.ok(stat.sizeBytes > 0);

      const tail = readReviewerLogTail('thread_a', 2, { root, maxBytes: 4096 });
      assert.ok(tail);
      assert.match(tail!.content, /wake stream=reviewer/);
      assert.match(tail!.content, /agent=codex/);
      assert.match(tail!.content, /reviewer says hello/);
      assert.match(tail!.content, /\[exit\] code=0/);
      assert.strictEqual(tail!.truncated, false);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('appends a new banner when the same round is woken again', () => {
    const root = makeRoot();
    try {
      const first = createReviewerLogWriter('thread_b', 1, { agent: 'codex' }, { root });
      first.write('first wake\n');
      first.close();
      const second = createReviewerLogWriter('thread_b', 1, { agent: 'codex' }, { root });
      second.write('second wake\n');
      second.close();

      const tail = readReviewerLogTail('thread_b', 1, { root, maxBytes: 8192 });
      assert.ok(tail);
      const banners = tail!.content.match(/wake stream=reviewer/g) || [];
      assert.strictEqual(banners.length, 2, 'each wake attempt keeps its own banner');
      assert.match(tail!.content, /first wake/);
      assert.match(tail!.content, /second wake/);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('stops at the byte cap with an explicit truncation marker', () => {
    const root = makeRoot();
    try {
      const writer = createReviewerLogWriter(
        'thread_c',
        1,
        { agent: 'codex' },
        { root, maxBytes: 1024 },
      );
      writer.write('x'.repeat(4000));
      writer.write('this must be dropped');
      writer.close();

      const stat = statReviewerLog('thread_c', 1, root);
      assert.ok(stat.sizeBytes <= 1024, `capped size ${stat.sizeBytes} must stay under 1024`);

      const tail = readReviewerLogTail('thread_c', 1, { root, maxBytes: 4096 });
      assert.ok(tail);
      assert.match(tail!.content, /\[reviewer log truncated at/);
      assert.ok(!tail!.content.includes('this must be dropped'));
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('reads only the tail and flags truncation', () => {
    const root = makeRoot();
    try {
      const writer = createReviewerLogWriter('thread_d', 4, { agent: 'codex' }, { root });
      writer.write('HEAD-MARKER ' + 'a'.repeat(4000) + ' TAIL-MARKER');
      writer.close();

      const tail = readReviewerLogTail('thread_d', 4, { root, maxBytes: 200 });
      assert.ok(tail);
      assert.strictEqual(tail!.truncated, true);
      assert.ok(!tail!.content.includes('HEAD-MARKER'));
      assert.ok(tail!.content.includes('TAIL-MARKER'));
      assert.strictEqual(tail!.totalBytes, statReviewerLog('thread_d', 4, root).sizeBytes);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('reports missing logs instead of an empty string', () => {
    const root = makeRoot();
    try {
      assert.strictEqual(readReviewerLogTail('thread_missing', 1, { root }), undefined);
      assert.strictEqual(statReviewerLog('thread_missing', 1, root).exists, false);
      assert.deepStrictEqual(listReviewerLogRounds('thread_missing', root), []);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });

  test('lists rounds with a reviewer log, newest first, ignoring receiver logs', () => {
    const root = makeRoot();
    try {
      for (const round of [1, 3, 7]) {
        const writer = createReviewerLogWriter('thread_e', round, { agent: 'codex' }, { root });
        writer.write('x');
        writer.close();
      }
      // A receiver log for the same thread must never show up as a reviewer round.
      fs.writeFileSync(roundExecutionLogPath('thread_e', 9, root), 'receiver bytes');

      assert.deepStrictEqual(listReviewerLogRounds('thread_e', root), [7, 3, 1]);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
});
