import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as child_process from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import { captureWorkspaceSnapshot, diffWorkspaceSnapshots } from '../bridge/git-probe';

function git(cwd: string, args: string[]): void {
  child_process.execFileSync('git', ['-C', cwd, ...args], {
    stdio: 'ignore',
    windowsHide: true,
  });
}

describe('git-probe - attributing a round to the files it changed', () => {
  let repo: string;

  beforeEach(() => {
    repo = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-probe-'));
    git(repo, ['init']);
    git(repo, ['config', 'user.email', 'mesh@example.com']);
    git(repo, ['config', 'user.name', 'Mesh Test']);
    fs.writeFileSync(path.join(repo, 'a.kt'), 'class A\n', 'utf8');
    fs.writeFileSync(path.join(repo, 'already-dirty.kt'), 'class D\n', 'utf8');
    git(repo, ['add', '.']);
    git(repo, ['commit', '-m', 'init']);
  });

  afterEach(() => {
    try {
      fs.rmSync(repo, { recursive: true, force: true });
    } catch {}
  });

  test('reports created, modified and deleted files, and ignores untouched ones', () => {
    // Pre-existing dirt: this file is already modified before the round starts.
    fs.writeFileSync(path.join(repo, 'already-dirty.kt'), 'class D { }\n', 'utf8');

    const before = captureWorkspaceSnapshot(repo);
    assert.strictEqual(before.available, true);

    fs.writeFileSync(path.join(repo, 'a.kt'), 'class A { fun x() = 1 }\n', 'utf8');
    fs.writeFileSync(path.join(repo, 'new-file.kt'), 'class New\n', 'utf8');
    // Editing an already-dirty file again must still be attributed to the round.
    fs.writeFileSync(path.join(repo, 'already-dirty.kt'), 'class D { fun y() = 2 }\n', 'utf8');

    const touched = diffWorkspaceSnapshots(before, captureWorkspaceSnapshot(repo));
    assert.ok(touched.includes('a.kt'), `expected a.kt in ${JSON.stringify(touched)}`);
    assert.ok(touched.includes('new-file.kt'));
    assert.ok(
      touched.includes('already-dirty.kt'),
      'a file that was already dirty and edited further must still be attributed',
    );
    assert.ok(!touched.includes('unrelated.kt'));
  });

  test('reports files committed by the round itself', () => {
    const before = captureWorkspaceSnapshot(repo);

    fs.writeFileSync(path.join(repo, 'committed.kt'), 'class C\n', 'utf8');
    git(repo, ['add', '.']);
    git(repo, ['commit', '-m', 'agent commit']);

    const touched = diffWorkspaceSnapshots(before, captureWorkspaceSnapshot(repo));
    assert.ok(
      touched.includes('committed.kt'),
      `committed files must be attributed via the HEAD diff: ${JSON.stringify(touched)}`,
    );
  });

  test('reports nothing for a directory that is not a git repository', () => {
    const plain = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-plain-'));
    try {
      const snapshot = captureWorkspaceSnapshot(plain);
      assert.strictEqual(snapshot.available, false);
      assert.deepStrictEqual(diffWorkspaceSnapshots(snapshot, snapshot), []);
    } finally {
      try {
        fs.rmSync(plain, { recursive: true, force: true });
      } catch {}
    }
  });
});
