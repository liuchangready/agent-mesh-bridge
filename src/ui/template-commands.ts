import type * as vscode from 'vscode';
import { TaskTreeItem } from './task-tree-provider';
import { WorkspaceRegistry } from '../bridge/workspace';
import { resolveTemplateWorkspaceRoot } from '../bridge/git-worktree';
import { listTaskTemplates, createTaskTemplate, templateFile } from '../bridge/task-templates';

export function registerTemplateCommands(workspaceRegistry: WorkspaceRegistry, api: typeof vscode): vscode.Disposable[] {
  const templateRoot = (item?: TaskTreeItem): string => {
    const workspace = item?.workspaceId && workspaceRegistry.get(item.workspaceId);
    if (!workspace) throw new Error('Select a workspace in the Bridge task tree.');
    return resolveTemplateWorkspaceRoot(workspace.rootPath);
  };
  const templateAction = (action: 'read' | 'create' | 'edit') => async (item?: TaskTreeItem) => {
    try {
      const root = templateRoot(item);
      if (action === 'create') {
        const id = await api.window.showInputBox({ prompt: 'Template ID (lowercase letters, numbers, hyphens or underscores)' }); if (id === undefined) return;
        const label = await api.window.showInputBox({ prompt: 'Template name' }); if (label === undefined) return;
        const summary = await api.window.showInputBox({ prompt: 'Short description of when to use it' }); if (summary === undefined) return;
        const file = createTaskTemplate(root, id, label, summary);
        await api.window.showTextDocument(await api.workspace.openTextDocument(file)); return;
      }
      const result = listTaskTemplates(root);
      const choices = result.files.map(file => {
        const template = result.templates.find(t => t.sourceFileName === file);
        const issue = result.diagnostics.find(d => d.file === file);
        return { label: template?.label || file, description: template ? 'v' + template.version + ' · ' + template.summary : 'Invalid template', detail: issue?.reason, file };
      });
      const choice = await api.window.showQuickPick(choices, { placeHolder: action === 'read' ? 'Templates and validation diagnostics' : 'Select a template to edit' });
      if (choice && action === 'edit') await api.window.showTextDocument(await api.workspace.openTextDocument(templateFile(root, choice.file)));
    } catch (error: any) { void api.window.showErrorMessage('Task templates: ' + (error.message || error)); }
  };

  return [
    api.commands.registerCommand('agent-mesh-bridge.readTaskTemplates', templateAction('read')),
    api.commands.registerCommand('agent-mesh-bridge.createTaskTemplate', templateAction('create')),
    api.commands.registerCommand('agent-mesh-bridge.editTaskTemplate', templateAction('edit')),
  ];
}
