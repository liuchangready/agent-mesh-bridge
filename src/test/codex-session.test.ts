import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  isBareCodexThreadId,
  isCodexRolloutName,
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
});
