import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { git, initRepo } from './git-worktree-fixture';

test('repository fixtures keep working files, refs and config independent', (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-fixture-isolation-'));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const first = path.join(root, 'first');
  const second = path.join(root, 'second');
  const third = path.join(root, 'third');
  initRepo(first);
  initRepo(second);
  const base = git(first, 'rev-parse', 'HEAD');

  fs.writeFileSync(path.join(first, 'shared.txt'), 'changed\n');
  git(first, 'config', 'user.name', 'Changed');
  git(first, 'commit', '-am', 'change first fixture');
  assert.notEqual(git(first, 'rev-parse', 'HEAD'), base);
  initRepo(third);

  for (const repository of [second, third]) {
    assert.equal(git(repository, 'rev-parse', 'HEAD'), base);
    assert.equal(git(repository, 'branch', '--show-current'), 'feature/caller');
    assert.equal(git(repository, 'config', 'user.name'), 'Mesh Test');
    assert.equal(git(repository, 'remote'), '');
    assert.equal(fs.readFileSync(path.join(repository, 'shared.txt'), 'utf8'), 'alpha\nmiddle\nbeta\n');
    assert.equal(git(repository, 'status', '--porcelain'), '');
  }
});
