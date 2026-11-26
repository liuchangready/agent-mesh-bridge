import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { appendTemplateInstructions, createTaskTemplate, listTaskTemplates, resolveTaskTemplate, templateFile } from '../bridge/task-templates';

test('templates are isolated, initialized once, and snapshots survive edits and deletion', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-templates-'));
  try {
    const a = path.join(temp, 'a'), b = path.join(temp, 'b'); fs.mkdirSync(a); fs.mkdirSync(b);
    const first = listTaskTemplates(a); assert.deepEqual(first.templates.map(t => t.id).sort(), ['art', 'programming']);
    const snap = resolveTaskTemplate(a, 'programming');
    const file = templateFile(a, snap.sourceFileName); fs.writeFileSync(file, fs.readFileSync(file, 'utf8').replace(snap.workerInstructions, 'Changed worker'));
    assert.match(resolveTaskTemplate(a, 'programming').workerInstructions, /Changed worker/);
    assert.ok(appendTemplateInstructions('rework', snap, 'worker').includes(snap.workerInstructions));
    assert.ok(!appendTemplateInstructions('rework', snap, 'worker').includes(snap.reviewerInstructions));
    fs.unlinkSync(file); assert.equal(listTaskTemplates(a).templates.some(t => t.id === 'programming'), false);
    assert.equal(listTaskTemplates(b).templates.some(t => t.id === 'programming'), true);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});

test('invalid, duplicate, oversized and unsafe templates are diagnosed; creation never overwrites', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-templates-'));
  try {
    const dir = path.join(root, '.agent_bridge', 'task-templates'); listTaskTemplates(root);
    const file = createTaskTemplate(root, 'custom', 'Custom', 'For a task');
    assert.throws(() => createTaskTemplate(root, 'custom', 'Again', 'Test'));
    assert.throws(() => createTaskTemplate(root, '../escape', 'Bad', 'Test'));
    assert.throws(() => templateFile(root, '../custom.md'));
    fs.copyFileSync(file, path.join(dir, 'duplicate.md'));
    fs.writeFileSync(path.join(dir, 'broken.md'), '---\nid: broken\n---\n');
    fs.writeFileSync(path.join(dir, 'large.md'), 'x'.repeat(65537));
    const result = listTaskTemplates(root);
    assert.equal(result.templates.some(t => t.id === 'custom'), false);
    assert.equal(result.diagnostics.length, 4);
    assert.throws(() => resolveTaskTemplate(root, 'custom'));
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});

test('junctions cannot redirect template storage outside the workspace', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-templates-'));
  try {
    const root = path.join(temp, 'root'), outside = path.join(temp, 'outside'); fs.mkdirSync(root); fs.mkdirSync(outside);
    fs.symlinkSync(outside, path.join(root, '.agent_bridge'), process.platform === 'win32' ? 'junction' : 'dir');
    assert.throws(() => listTaskTemplates(root), /Unsafe/);
    assert.deepEqual(fs.readdirSync(outside), []);
  } finally { fs.rmSync(temp, { recursive: true, force: true }); }
});
test('rejects a changed ancestor before creating a template', () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-template-change-'));
  const nativeFs: typeof fs = require('fs'); const original = nativeFs.readdirSync; let changed = false;
  try {
    const root = path.join(temp, 'root'), outside = path.join(temp, 'outside'); fs.mkdirSync(root); fs.mkdirSync(outside);
    listTaskTemplates(root);
    const parent = path.join(fs.realpathSync.native(root), '.agent_bridge');
    (nativeFs as any).readdirSync = (...args: any[]) => {
      const result = (original as any)(...args);
      if (!changed && String(args[0]) === path.join(parent, 'task-templates')) {
        changed = true; fs.renameSync(parent, path.join(root, '.old-parent'));
        fs.symlinkSync(outside, parent, process.platform === 'win32' ? 'junction' : 'dir');
      }
      return result;
    };
    assert.throws(() => createTaskTemplate(root, 'new', 'New', 'Summary'), /Unsafe/);
    assert.equal(changed, true); assert.deepEqual(fs.readdirSync(outside), []);
  } finally { (nativeFs as any).readdirSync = original; fs.rmSync(temp, { recursive: true, force: true }); }
});
