import * as fs from 'fs/promises';
import * as vscode from 'vscode';
import {
  CopilotModelGroup,
  HostModelRef,
  importGroupNames,
  newModelGroupId,
  validateGroup,
} from './model-groups';
import { CopilotModelService } from './model-service';

class ResultDocuments implements vscode.TextDocumentContentProvider {
  private values = new Map<string, string>();
  private emitter = new vscode.EventEmitter<vscode.Uri>();
  readonly onDidChange = this.emitter.event;
  put(text: string): vscode.Uri {
    const uri = vscode.Uri.parse(
      `agent-mesh-copilot:/result-${Date.now()}-${Math.random().toString(36).slice(2)}.md`,
    );
    this.values.set(uri.toString(), text);
    while (this.values.size > 20) this.values.delete(this.values.keys().next().value!);
    this.emitter.fire(uri);
    return uri;
  }
  provideTextDocumentContent(uri: vscode.Uri): string {
    return this.values.get(uri.toString()) || 'Result expired.';
  }
}

export function registerCopilotModelCommands(
  context: vscode.ExtensionContext,
  service: CopilotModelService,
): vscode.Disposable[] {
  const documents = new ResultDocuments();
  const provider = vscode.workspace.registerTextDocumentContentProvider(
    'agent-mesh-copilot',
    documents,
  );
  const canConfigure = () => {
    if (
      !vscode.workspace.isTrusted ||
      vscode.env.uiKind === vscode.UIKind.Web ||
      vscode.env.remoteName
    ) {
      void vscode.window.showWarningMessage(
        'Copilot model groups require a trusted local desktop workspace.',
      );
      return false;
    }
    return true;
  };
  const chooseGroup = async (group?: CopilotModelGroup): Promise<CopilotModelGroup | undefined> => {
    if (group) return group;
    const groups = service.groups.getAll();
    const item = await vscode.window.showQuickPick(
      groups.map((g) => ({
        label: g.name,
        description: g.source === 'vscode-lm' ? 'VS Code host models' : 'OpenAI-compatible API',
        group: g,
      })),
      { placeHolder: 'Choose a model group' },
    );
    return item?.group;
  };
  const chooseModel = async (
    group: CopilotModelGroup,
    preselect?: HostModelRef | string,
  ): Promise<HostModelRef | string | undefined> => {
    if (group.source === 'vscode-lm') {
      const refs = group.models;
      const host = service.getHostModels();
      const items = refs.map((ref) => {
        const model = host.find(
          (candidate) => candidate.id === ref.id && candidate.vendor === ref.vendor,
        );
        return {
          label: model?.name || ref.id,
          description: `${ref.vendor} · ${ref.id}${model ? '' : ' · unavailable — refresh catalog'}`,
          ref,
          disabled: !model,
          picked:
            typeof preselect === 'object' &&
            ref.id === preselect.id &&
            ref.vendor === preselect.vendor,
        };
      });
      const selected = await vscode.window.showQuickPick(items, {
        placeHolder: 'Choose the exact host model',
      });
      return selected?.ref;
    }
    const selected = await vscode.window.showQuickPick(
      group.models.map((model) => ({
        label: model.name || model.id,
        description: model.id,
        id: model.id,
      })),
      { placeHolder: 'Choose a model' },
    );
    return selected?.id;
  };
  const runCall = async (
    group: CopilotModelGroup | undefined,
    test: boolean,
    selectedModel?: HostModelRef | string,
  ) => {
    if (!canConfigure()) return;
    group = await chooseGroup(group);
    if (!group) return;
    const selection = selectedModel ?? (await chooseModel(group));
    if (!selection) return;
    const modelId = typeof selection === 'string' ? selection : selection.id;
    if (test) {
      const confirm = await vscode.window.showWarningMessage(
        `Send a short test prompt to ${modelId}? The provider may charge for this request.`,
        { modal: true },
        'Send test',
      );
      if (confirm !== 'Send test') return;
    }
    const prompt = test
      ? 'Reply with exactly: Connection test successful.'
      : await vscode.window.showInputBox({
          prompt: `Prompt for ${group.name} · ${modelId}`,
          ignoreFocusOut: true,
        });
    if (prompt === undefined || !prompt.trim()) return;
    await vscode.window.withProgress(
      {
        location: vscode.ProgressLocation.Notification,
        title: `Calling ${modelId}`,
        cancellable: true,
      },
      async (progress, token) => {
        try {
          const { text, elapsedMs } = await service.invoke(group!, selection, prompt, token);
          const uri = documents.put(
            `# ${group!.name}\n\n- Source: ${group!.source}\n- Model: ${modelId}\n- Status: completed\n- Duration: ${elapsedMs} ms\n\n---\n\n${text}`,
          );
          await vscode.window.showTextDocument(uri, { preview: false });
        } catch (error) {
          vscode.window.showErrorMessage(
            `Model call failed: ${error instanceof Error ? error.message : 'Unknown error'}`,
          );
        }
      },
    );
  };
  const refresh = async () => {
    if (!canConfigure()) return;
    try {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Refreshing VS Code model catalog',
        },
        () => service.refreshHostModels(),
      );
      vscode.window.showInformationMessage(`Found ${service.getHostModels().length} host models.`);
    } catch (error) {
      vscode.window.showWarningMessage(
        error instanceof Error ? error.message : 'Model catalog refresh failed.',
      );
    }
  };
  const addGroup = async () => {
    if (!canConfigure()) return;
    const source = await vscode.window.showQuickPick(
      [
        { label: 'VS Code host models', source: 'vscode-lm' as const },
        { label: 'OpenAI-compatible endpoint', source: 'openai-compatible' as const },
      ],
      { placeHolder: 'Choose model source' },
    );
    if (!source) return;
    const name = await vscode.window.showInputBox({
      prompt: 'Group display name',
      ignoreFocusOut: true,
    });
    if (name === undefined) return;
    if (source.source === 'vscode-lm') {
      if (!service.getHostModels().length) {
        const answer = await vscode.window.showInformationMessage(
          'No cached host models. Refresh the VS Code model catalog now?',
          'Refresh',
        );
        if (answer !== 'Refresh') return;
        await refresh();
      }
      const available = service.getHostModels();
      if (!available.length) {
        vscode.window.showWarningMessage(
          'Refresh the host model catalog and grant access before creating this group.',
        );
        return;
      }
      const members = await vscode.window.showQuickPick(
        available
          .filter((model) => !(source as any).vendor || model.vendor === (source as any).vendor)
          .map((model) => ({
            label: model.name || model.id,
            description: `${model.vendor} · ${model.id}`,
            ref: service.hostRef(model),
          })),
        { canPickMany: true, placeHolder: 'Bind exact models from the refreshed host catalog' },
      );
      if (!members?.length) return;
      await service.groups.save({
        id: newModelGroupId(),
        name,
        source: 'vscode-lm',
        models: members.map((item) => item.ref),
      });
    } else {
      const baseUrl = await vscode.window.showInputBox({
        prompt: 'OpenAI-compatible API base URL',
        placeHolder: 'https://api.example.com/v1',
        ignoreFocusOut: true,
      });
      if (baseUrl === undefined) return;
      const id = newModelGroupId();
      const candidate: CopilotModelGroup = {
        id,
        name,
        source: 'openai-compatible',
        baseUrl,
        models: [],
      };
      const rawModels = await vscode.window.showInputBox({
        prompt: 'Model IDs, comma-separated (or leave empty to add later)',
        ignoreFocusOut: true,
      });
      if (rawModels === undefined) return;
      const group = {
        ...candidate,
        models: rawModels
          .split(',')
          .map((model) => ({ id: model.trim() }))
          .filter((model) => model.id),
      };
      const key = await vscode.window.showInputBox({
        prompt: 'Optional API key; cancel to discard this group setup',
        password: true,
        value: '',
        ignoreFocusOut: true,
      });
      if (key === undefined) return;
      const validGroup = validateGroup(group) as Extract<
        CopilotModelGroup,
        { source: 'openai-compatible' }
      >;
      if (key.trim()) await service.groups.setApiKey(validGroup, key.trim());
      try {
        await service.groups.save(validGroup);
      } catch (error) {
        if (key.trim()) {
          try {
            await service.groups.deleteApiKey(validGroup);
          } catch {
            throw new Error('Group setup failed and temporary SecretStorage cleanup failed.');
          }
        }
        throw error;
      }
    }
    await service.refresh();
  };
  const configureKey = async (
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
  ): Promise<boolean> => {
    const value = await vscode.window.showInputBox({
      prompt: 'API key (stored only in VS Code SecretStorage)',
      password: true,
      value: '',
      ignoreFocusOut: true,
    });
    if (value === undefined || !value.trim()) return false;
    service.cancelGroup(group.id);
    await service.groups.setApiKey(group, value.trim());
    await service.refresh();
    return true;
  };
  const manageGroup = async (group?: CopilotModelGroup) => {
    if (!canConfigure()) return;
    group = await chooseGroup(group);
    if (!group) return;
    const action = await vscode.window.showQuickPick(
      [
        { label: 'Rename group', id: 'rename' },
        { label: 'Manage model members', id: 'members' },
        { label: 'Call model', id: 'call' },
        { label: 'Test connection', id: 'test' },
        ...(group.source === 'openai-compatible'
          ? [
              { label: 'Configure / replace API key', id: 'key' },
              { label: 'Delete API key', id: 'delete-key' },
              { label: 'Edit endpoint URL', id: 'url' },
              { label: 'Fetch model list', id: 'list' },
            ]
          : []),
        { label: 'Delete group', id: 'delete' },
      ],
      { placeHolder: group.name },
    );
    if (!action) return;
    if (action.id === 'rename') {
      const name = await vscode.window.showInputBox({
        prompt: 'New group name',
        value: group.name,
      });
      if (name === undefined) return;
      service.cancelGroup(group.id);
      await service.groups.save({ ...group, name });
    } else if (action.id === 'members') {
      if (group.source === 'vscode-lm') {
        const models = service
          .getHostModels()
          .filter((model) => !group!.source || !group!.vendor || model.vendor === group!.vendor);
        const choices = models.map((model) => ({
          label: model.name || model.id,
          description: `${model.vendor} · ${model.id}`,
          ref: service.hostRef(model),
          picked:
            group!.source === 'vscode-lm' &&
            group.models.some((m) => m.id === model.id && m.vendor === model.vendor),
        }));
        const selected = await vscode.window.showQuickPick(choices, {
          canPickMany: true,
          placeHolder: 'Select bound models; refresh the host catalog first if empty',
        });
        if (!selected) return;
        service.cancelGroup(group.id);
        await service.groups.save({ ...group, models: selected.map((item) => item.ref) });
      } else {
        const raw = await vscode.window.showInputBox({
          prompt: 'Model IDs, comma-separated',
          value: group.models.map((item) => item.id).join(','),
        });
        if (raw === undefined) return;
        service.cancelGroup(group.id);
        await service.groups.save({
          ...group,
          models: raw
            .split(',')
            .map((id) => ({ id: id.trim() }))
            .filter((item) => item.id),
        });
      }
    } else if (action.id === 'call') await runCall(group, false);
    else if (action.id === 'test') await runCall(group, true);
    else if (action.id === 'key' && group.source === 'openai-compatible') await configureKey(group);
    else if (action.id === 'delete-key' && group.source === 'openai-compatible') {
      service.cancelGroup(group.id);
      await service.groups.deleteApiKey(group);
      await service.refresh();
    } else if (action.id === 'url' && group.source === 'openai-compatible') {
      const baseUrl = await vscode.window.showInputBox({
        prompt: 'New API base URL. Changing origin deletes the old key.',
        value: group.baseUrl,
      });
      if (baseUrl === undefined) return;
      const oldOrigin = new URL(group.baseUrl).origin;
      const updated = validateGroup({ ...group, baseUrl }) as Extract<
        CopilotModelGroup,
        { source: 'openai-compatible' }
      >;
      const changedOrigin = new URL(updated.baseUrl).origin !== oldOrigin;
      let key: string | undefined;
      if (changedOrigin) {
        key = await vscode.window.showInputBox({
          prompt: 'New endpoint origin requires a new key. Leave empty to add it later.',
          password: true,
          value: '',
          ignoreFocusOut: true,
        });
        if (key === undefined) return;
        if (key.trim()) await service.groups.setApiKey(updated, key.trim());
      }
      service.cancelGroup(group.id);
      try {
        await service.groups.save(updated);
      } catch (error) {
        if (key?.trim()) {
          const persisted = service.groups
            .getAll()
            .some(
              (item) => item.id === updated.id && JSON.stringify(item) === JSON.stringify(updated),
            );
          if (!persisted) {
            try {
              await service.groups.deleteApiKey(updated);
            } catch {
              throw new Error('Endpoint update failed and temporary SecretStorage cleanup failed.');
            }
          }
        }
        throw error;
      }
    } else if (action.id === 'list' && group.source === 'openai-compatible') {
      await vscode.window.withProgress(
        {
          location: vscode.ProgressLocation.Notification,
          title: 'Fetching model list',
          cancellable: true,
        },
        async (_progress, token) => {
          try {
            const models = await service.listCompatibleModels(
              group as Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
              token,
            );
            if (!models.length) {
              vscode.window.showInformationMessage(
                'No model IDs were returned; you can enter IDs manually.',
              );
              return;
            }
            const picked = await vscode.window.showQuickPick(models, {
              canPickMany: true,
              placeHolder: 'Select model IDs to add',
            });
            if (picked) {
              const current = service.groups.getAll().find((item) => item.id === group!.id);
              if (!current || JSON.stringify(current) !== JSON.stringify(group))
                throw new Error('This model group changed; reopen it before saving model IDs.');
              service.cancelGroup(group!.id);
              await service.groups.save({
                ...group!,
                models: [
                  ...new Map(
                    [...group!.models, ...picked.map((id) => ({ id }))].map((item) => [
                      item.id,
                      item,
                    ]),
                  ).values(),
                ],
              });
            }
          } catch (error) {
            vscode.window.showWarningMessage(
              error instanceof Error ? error.message : 'Model list request failed.',
            );
          }
        },
      );
    } else if (action.id === 'delete') {
      const confirm = await vscode.window.showWarningMessage(
        `Delete ${group.name} and its saved credential?`,
        { modal: true },
        'Delete',
      );
      if (confirm === 'Delete') {
        service.cancelGroup(group.id);
        await service.groups.delete(group.id);
      }
    }
    await service.refresh();
  };
  const importNames = async () => {
    if (!canConfigure()) return;
    const picked = await vscode.window.showOpenDialog({
      canSelectMany: false,
      filters: { 'JSON / JSONC': ['json', 'jsonc'] },
      openLabel: 'Import group names',
    });
    if (!picked?.length) return;
    try {
      const stats = await fs.stat(picked[0].fsPath);
      if (stats.size > 1024 * 1024) throw new Error('File exceeds 1 MB.');
      const { parseJsonc } = await import('./model-groups');
      const names = importGroupNames(parseJsonc(await fs.readFile(picked[0].fsPath, 'utf8')));
      if (!names.length) throw new Error('No valid name/vendor entries found.');
      const existing = service.groups.getAll();
      const seenNames = new Set(existing.map((group) => group.name));
      const created: CopilotModelGroup[] = [];
      for (const entry of names) {
        const duplicate = seenNames.has(entry.name);
        const decision = await vscode.window.showQuickPick(
          [
            { label: `Create “${entry.name}”`, id: 'create' },
            ...(duplicate ? [{ label: `Skip duplicate “${entry.name}”`, id: 'skip' }] : []),
          ],
          {
            placeHolder: `Imported label only · vendor ${entry.vendor}; bind exact models separately`,
          },
        );
        if (decision?.id === 'create') {
          created.push(
            await service.groups.save({
              id: newModelGroupId(),
              name: entry.name,
              vendor: entry.vendor,
              source: 'vscode-lm',
              models: [],
            }),
          );
          seenNames.add(entry.name);
        }
      }
      vscode.window.showInformationMessage(
        `Imported ${created.length} unbound group name(s); model membership still needs explicit binding.`,
      );
    } catch (error) {
      vscode.window.showErrorMessage(
        `Could not import group names: ${error instanceof Error ? error.message : 'Invalid file'}`,
      );
    }
  };
  const guarded =
    (fn: (...args: any[]) => Promise<any>) =>
    async (...args: any[]) => {
      try {
        return await fn(...args);
      } catch (error) {
        vscode.window.showErrorMessage(
          error instanceof Error ? error.message : 'Copilot model operation failed.',
        );
      }
    };
  const disposables = [
    provider,
    vscode.commands.registerCommand('agent-mesh-bridge.refreshCopilotModels', guarded(refresh)),
    vscode.commands.registerCommand('agent-mesh-bridge.addCopilotModelGroup', guarded(addGroup)),
    vscode.commands.registerCommand(
      'agent-mesh-bridge.importCopilotModelGroups',
      guarded(importNames),
    ),
    vscode.commands.registerCommand(
      'agent-mesh-bridge.manageCopilotModelGroup',
      guarded(manageGroup),
    ),
    vscode.commands.registerCommand(
      'agent-mesh-bridge.callCopilotModel',
      guarded((group?: CopilotModelGroup, selection?: HostModelRef | string) =>
        runCall(group, false, selection),
      ),
    ),
  ];
  return disposables;
}
