import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawnSync } from 'child_process';
import { WorkspaceRegistry } from '../bridge/workspace';
import { registerTemplateCommands } from '../ui/template-commands';

test('registered template commands use clicked workspace rather than active workspace', async () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-template-commands-'));
  try {
    const a = path.join(temp,'a'), b = path.join(temp,'b'); fs.mkdirSync(a); fs.mkdirSync(b);
    const git = process.env.MESH_GIT_PATH || 'git';
    for (const root of [a,b]) assert.equal(spawnSync(git,['init',root],{windowsHide:true}).status,0);
    const canonicalB = fs.realpathSync.native(b);
    const registry = new WorkspaceRegistry(path.join(temp,'data')); registry.register(a); const target = registry.register(b);
    const callbacks: Record<string, Function> = {}; const opened: string[] = []; const errors: string[] = []; let input = ['custom','Custom','Summary']; let pickFile = '';
    const api: any = { commands: { registerCommand: (name: string, fn: Function) => { callbacks[name] = fn; return { dispose() {} }; } },
      window: { showInputBox: async () => input.shift(), showQuickPick: async (items: any[]) => items.find(i => i.file === pickFile), showTextDocument: async (doc: string) => opened.push(doc), showErrorMessage: (message: string) => errors.push(message) },
      workspace: { openTextDocument: async (file: string) => file } };
    registerTemplateCommands(registry, api);
    const node = { workspaceId: target.id };
    await callbacks['agent-mesh-bridge.readTaskTemplates'](node);
    await callbacks['agent-mesh-bridge.createTaskTemplate'](node);
    const created = path.join(canonicalB,'.agent_bridge','task-templates','custom.md'); assert.equal(opened[0],created); assert.ok(fs.existsSync(created));
    const invalid = path.join(canonicalB,'.agent_bridge','task-templates','invalid.md'); fs.writeFileSync(invalid,'invalid'); pickFile = 'invalid.md';
    await callbacks['agent-mesh-bridge.editTaskTemplate'](node); assert.equal(opened[1],invalid);
    assert.equal(fs.existsSync(path.join(a,'.agent_bridge')),false); assert.deepEqual(errors,[]);
  } finally { fs.rmSync(temp,{recursive:true,force:true}); }
});
