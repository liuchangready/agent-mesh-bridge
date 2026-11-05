import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  cachedCodexResumeSupport,
  codexResumeArgument,
  extractCodexThreadId,
  isBareCodexThreadId,
  isCodexRolloutName,
  parseCodexResumeHelp,
  rememberCodexResumeSupport,
  resolveCodexResumeId,
} from '../bridge/codex-session';

const UUID = '01a17d29-d40d-74c3-831d-878dd4edc0e5';
const ROLLOUT = `rollout-2026-10-27T19-40-56-${UUID}`;

function makeCodexHome(): string {
  const home = fs.mkdtempSync(path.join(os.tmpdir(), 'codex-home-'));
  const dir = path.join(home, 'sessions', '2026', '10', '27');
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, `${ROLLOUT}.jsonl`), '{"type":"session_meta"}\n');
  return home;
}

describe('Codex session id resolution', () => {
  test('classifies the two spellings', () => {
    assert.strictEqual(isBareCodexThreadId(UUID), true);
    assert.strictEqual(isCodexRolloutName(UUID), false);
    assert.strictEqual(isCodexRolloutName(ROLLOUT), true);
    assert.strictEqual(isBareCodexThreadId(ROLLOUT), false);
  });

  test('normalises a bare UUID to the rollout name codex exec resume expects', () => {
    const home = makeCodexHome();
    try {
      assert.strictEqual(resolveCodexResumeId(UUID, home), ROLLOUT);
      // Already-correct spellings and unrelated ids are never rewritten.
      assert.strictEqual(resolveCodexResumeId(ROLLOUT, home), ROLLOUT);
      assert.strictEqual(resolveCodexResumeId('caller-session-1', home), 'caller-session-1');
      assert.strictEqual(resolveCodexResumeId('', home), '');
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

  test('leaves an unknown UUID alone so codex reports its own error', () => {
    const home = makeCodexHome();
    try {
      const missing = 'ffffffff-ffff-ffff-ffff-ffffffffffff';
      assert.strictEqual(resolveCodexResumeId(missing, home), missing);
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });

  test('extracts the canonical thread id from either spelling', () => {
    assert.strictEqual(extractCodexThreadId(ROLLOUT), UUID);
    assert.strictEqual(extractCodexThreadId(UUID), UUID);
    assert.strictEqual(extractCodexThreadId('  ' + ROLLOUT + '  '), UUID);
    assert.strictEqual(extractCodexThreadId('thread name'), 'thread name');
  });
});

describe('Codex resume capability', () => {
  test('reads the modern help: UUID ids and --no-daemon', () => {
    const support = parseCodexResumeHelp(
      'Session id (UUID) or session name. UUIDs take precedence if it parses.\n--no-daemon',
    );

    assert.deepStrictEqual(support, { noDaemon: true, uuidIds: true });
  });

  test('reads a legacy help that talks about the rollout record', () => {
    const support = parseCodexResumeHelp(
      'Resume a previous session by its rollout record (rollout-<timestamp>-<uuid>).',
    );

    assert.deepStrictEqual(support, { noDaemon: false, uuidIds: false });
  });

  test('an unreadable help defaults to the modern UUID form', () => {
    assert.deepStrictEqual(parseCodexResumeHelp(''), { noDaemon: false, uuidIds: true });
  });

  test('the argument follows the probed form and the cache is shared', () => {
    const home = makeCodexHome();
    try {
      assert.strictEqual(
        codexResumeArgument(UUID, { noDaemon: false, uuidIds: true }, home),
        UUID,
      );
      assert.strictEqual(
        codexResumeArgument(ROLLOUT, { noDaemon: false, uuidIds: true }, home),
        UUID,
      );
      assert.strictEqual(
        codexResumeArgument(UUID, { noDaemon: false, uuidIds: false }, home),
        ROLLOUT,
      );
      // Unknown support falls back to the modern form.
      assert.strictEqual(codexResumeArgument(ROLLOUT, undefined, home), UUID);

      rememberCodexResumeSupport('/fake/codex', { noDaemon: true, uuidIds: true });
      assert.deepStrictEqual(cachedCodexResumeSupport('/fake/codex'), {
        noDaemon: true,
        uuidIds: true,
      });
      assert.strictEqual(cachedCodexResumeSupport('/other/codex'), undefined);
    } finally {
      fs.rmSync(home, { recursive: true, force: true });
    }
  });
});
