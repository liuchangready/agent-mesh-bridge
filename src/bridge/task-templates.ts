import * as fs from 'fs';
import * as crypto from 'crypto';
import * as path from 'path';
import { physicalDirectory } from './workspace';

export interface TaskTemplate {
  id: string; version: string; label: string; summary: string;
  workerInstructions: string; reviewerInstructions: string; sourceFileName: string; selectedAt: string;
}
export interface TemplateDiagnostic { file: string; reason: string }
const MAX_BYTES = 64 * 1024;
const ID = /^[a-z0-9]+(?:[-_][a-z0-9]+)*$/;
const examples: Record<string, string> = {
  programming: 'Inspect the existing structure and implement the smallest complete change. Handle relevant boundaries and regressions. Report delivery and actual validation evidence.',
  art: 'Clarify the asset purpose, style, dimensions, format and transparency before creating it. Follow project naming and import references. Report the assets and actual verification; do not invent specifications or results.',
};
function text(id: string, label: string, summary: string, worker: string, reviewer: string): string {
  return `---\nid: ${id}\nversion: 1\nlabel: ${label}\nsummary: ${summary}\n---\n\n## Worker instructions\n\n${worker}\n\n## Reviewer instructions\n\n${reviewer}\n`;
}
// These checks reject observed path changes; they do not provide an atomic filesystem sandbox.
function checkDirectory(dir: string): void {
  const parent = path.dirname(dir), root = path.dirname(parent);
  for (const candidate of [root, parent, dir]) {
    const stat = fs.lstatSync(candidate);
    if (stat.isSymbolicLink() || !stat.isDirectory() || path.normalize(fs.realpathSync.native(candidate)) !== path.normalize(candidate)) throw new Error('Unsafe template directory: ' + candidate);
  }
}
function directory(root: string): string {
  const base = physicalDirectory(root), parent = path.join(base, '.agent_bridge'), dir = path.join(parent, 'task-templates');
  for (const candidate of [parent, dir]) {
    const prior = path.dirname(candidate);
    if (candidate === dir) {
      const st = fs.lstatSync(parent);
      if (st.isSymbolicLink() || !st.isDirectory() || fs.realpathSync.native(parent) !== parent) throw new Error('Unsafe template directory: ' + parent);
    }
    if (fs.realpathSync.native(prior) !== prior) throw new Error('Unsafe template directory: ' + prior);
    try { fs.mkdirSync(candidate); } catch (e: any) { if (e.code !== 'EEXIST') throw e; }
    const stat = fs.lstatSync(candidate);
    if (stat.isSymbolicLink() || !stat.isDirectory() || fs.realpathSync.native(candidate) !== candidate) throw new Error('Unsafe template directory: ' + candidate);
  }
  checkDirectory(dir); return dir;
}
function writeNew(file: string, content: string): void {
  checkDirectory(path.dirname(file));
  const temporary = path.join(path.dirname(file), '.' + crypto.randomUUID() + '.tmp');
  fs.writeFileSync(temporary, content, { flag: 'wx', encoding: 'utf8' });
  try { checkDirectory(path.dirname(file)); fs.linkSync(temporary, file); } finally { try { checkDirectory(path.dirname(file)); fs.unlinkSync(temporary); } catch {} }
}
function initialize(dir: string): void {
  checkDirectory(dir);
  const marker = path.join(dir, '.initialized');
  try { const s = fs.lstatSync(marker); if (!s.isFile() || s.isSymbolicLink()) throw new Error('Unsafe initialization marker'); return; } catch (e: any) { if (e.code !== 'ENOENT') throw e; }
  for (const [id, worker] of Object.entries(examples)) {
    const label = id === 'art' ? 'Art' : 'Programming';
    const summary = id === 'art' ? 'Create or revise project art assets.' : 'Implement and verify software changes.';
    try { writeNew(path.join(dir, `${id}.md`), text(id, label, summary, worker, 'Independently inspect the deliverables and evidence against the task requirements. Verify relevant boundaries and request concrete rework when acceptance fails.')); }
    catch (e: any) { if (e.code !== 'EEXIST') throw e; }
  }
  try { writeNew(marker, '1\n'); } catch (e: any) { if (e.code !== 'EEXIST') throw e; }
}
function parse(file: string, name: string): TaskTemplate {
  checkDirectory(path.dirname(file));
  const stat = fs.lstatSync(file);
  if (stat.isSymbolicLink() || !stat.isFile()) throw new Error('Not a regular file or is a symbolic link');
  if (stat.size > MAX_BYTES) throw new Error('File exceeds 64 KB');
  const fd = fs.openSync(file, fs.constants.O_RDONLY | (fs.constants.O_NOFOLLOW || 0));
  let raw: string;
  try {
    const actual = fs.fstatSync(fd); checkDirectory(path.dirname(file));
    const current = fs.lstatSync(file);
    if (!actual.isFile() || current.isSymbolicLink() || actual.dev !== stat.dev || actual.ino !== stat.ino || current.dev !== actual.dev || current.ino !== actual.ino || actual.size !== stat.size || actual.size > MAX_BYTES) throw new Error('Unsafe template file changed');
    raw = fs.readFileSync(fd, 'utf8').replace(/\r\n/g, '\n');
  } finally { fs.closeSync(fd); }
  const match = /^---\n([\s\S]*?)\n---\n([\s\S]*)$/.exec(raw);
  if (!match) throw new Error('Expected metadata front matter');
  const fields: Record<string, string> = {};
  for (const line of match[1].split('\n')) {
    const m = /^(id|version|label|summary):\s*(.*?)\s*$/.exec(line);
    if (!m || fields[m[1]] !== undefined) throw new Error('Invalid or duplicate metadata field');
    fields[m[1]] = m[2];
  }
  if (!ID.test(fields.id || '') || fields.id.length > 80) throw new Error('Invalid id');
  for (const key of ['version', 'label', 'summary']) if (!fields[key] || fields[key].length > (key === 'summary' ? 500 : 120)) throw new Error(`Invalid ${key}`);
  const body = /^\s*## Worker instructions\s*\n([\s\S]*?)\n## Reviewer instructions\s*\n([\s\S]*?)\s*$/.exec(match[2]);
  if (!body || !body[1].trim() || !body[2].trim() || /^## (?:Worker|Reviewer) instructions\s*$/m.test(body[1] + '\n' + body[2])) throw new Error('Both unique, nonempty role sections are required');
  return { id: fields.id, version: fields.version, label: fields.label, summary: fields.summary, workerInstructions: body[1].trim(), reviewerInstructions: body[2].trim(), sourceFileName: name, selectedAt: new Date().toISOString() };
}
export function listTaskTemplates(root: string): { templates: TaskTemplate[]; diagnostics: TemplateDiagnostic[]; files: string[] } {
  const dir = directory(root); initialize(dir);
  checkDirectory(dir);
  const files = fs.readdirSync(dir).filter(n => n.toLowerCase().endsWith('.md')).sort();
  const templates: TaskTemplate[] = [], diagnostics: TemplateDiagnostic[] = [];
  for (const file of files) { try { templates.push(parse(path.join(dir, file), file)); } catch (e: any) { diagnostics.push({ file, reason: String(e.message || e) }); } }
  const counts = new Map<string, number>(); for (const t of templates) counts.set(t.id, (counts.get(t.id) || 0) + 1);
  return { templates: templates.filter(t => { if (counts.get(t.id)! > 1) { diagnostics.push({ file: t.sourceFileName, reason: `Duplicate id: ${t.id}` }); return false; } return true; }), diagnostics, files };
}
export function resolveTaskTemplate(root: string, id: string): TaskTemplate {
  const found = listTaskTemplates(root).templates.find(t => t.id === id);
  if (!found) throw new Error(`TASK_TEMPLATE_INVALID_OR_NOT_FOUND: ${id}`);
  return found;
}
export function createTaskTemplate(root: string, id: string, label: string, summary: string): string {
  if (!ID.test(id) || id.length > 80 || !label.trim() || label.length > 120 || !summary.trim() || summary.length > 500 || /[\r\n]/.test(label + summary)) throw new Error('Invalid template metadata');
  const listing = listTaskTemplates(root);
  if (listing.templates.some(t => t.id === id)) throw new Error(`Duplicate id: ${id}`);
  const file = path.join(directory(root), `${id}.md`);
  writeNew(file, text(id, label.trim(), summary.trim(), 'Describe the work, deliverables and evidence.', 'Describe independent checks, acceptance and rework requirements.'));
  return file;
}
export function templateFile(root: string, name: string): string {
  if (path.basename(name) !== name || !name.toLowerCase().endsWith('.md')) throw new Error('Invalid template filename');
  const file = path.join(directory(root), name); const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error('Unsafe template file');
  checkDirectory(path.dirname(file));
  return file;
}
export function appendTemplateInstructions(prompt: string, template: TaskTemplate | undefined, role: 'worker' | 'reviewer'): string {
  return template ? `${prompt}\n\n[Task template ${role} instructions]\n${role === 'worker' ? template.workerInstructions : template.reviewerInstructions}` : prompt;
}
