import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { listTaskTemplates, createTaskTemplate, templateFile } from '../bridge/task-templates';

test('unsafe links are rejected while a normal workspace works', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-template-closed-'));
  try {
    const root = path.join(temp, 'root'), outside = path.join(temp, 'outside');
    fs.mkdirSync(root); fs.mkdirSync(outside);
    fs.symlinkSync(outside, path.join(root, '.agent_bridge'), process.platform === 'win32' ? 'junction' : 'dir');
    assert.throws(() => listTaskTemplates(root), /Unsafe/);
    assert.throws(() => createTaskTemplate(root, 'custom', 'Custom', 'Summary'), /Unsafe/);
    assert.throws(() => templateFile(root, 'custom.md'), /Unsafe/);
    assert.deepEqual(fs.readdirSync(outside), []);
    const normal = path.join(temp, 'normal'); fs.mkdirSync(normal);
    assert.equal(listTaskTemplates(normal).templates.length, 2);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

