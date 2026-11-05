import * as vscode from 'vscode';
import { completeCompatibleChat, fetchCompatibleModels } from './compatible-model-client';
import { CopilotModelGroup, CopilotModelGroups, HostModelRef, validateGroup } from './model-groups';

export class CopilotModelService implements vscode.Disposable {
  readonly groups: CopilotModelGroups;
  private hostModels: vscode.LanguageModelChat[] = [];
  private hostState: 'not-refreshed' | 'available' | 'empty' | 'unsupported' | 'failed' =
    'not-refreshed';
  private lastCall = 'No model call yet';
  private active = new Map<AbortController, { groupId: string; cancel: () => void }>();
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChange = this.changed.event;
  private readonly modelChange: vscode.Disposable;

  constructor(context: vscode.ExtensionContext) {
    this.groups = new CopilotModelGroups(context.globalState, context.secrets);
    this.modelChange = vscode.lm.onDidChangeChatModels(() => {
      this.hostModels = [];
      this.hostState = 'not-refreshed';
      this.changed.fire();
    });
    context.subscriptions.push(this);
    context.subscriptions.push(context.secrets.onDidChange(() => this.changed.fire()));
  }

  async refreshHostModels(): Promise<void> {
    if (typeof vscode.lm?.selectChatModels !== 'function') {
      this.hostState = 'unsupported';
      this.hostModels = [];
      this.changed.fire();
      return;
    }
    try {
      this.hostModels = await vscode.lm.selectChatModels();
      this.hostState = this.hostModels.length ? 'available' : 'empty';
    } catch {
      this.hostModels = [];
      this.hostState = 'failed';
      throw new Error('VS Code model access was denied or the provider failed.');
    } finally {
      this.changed.fire();
    }
  }

  getHostModels(): vscode.LanguageModelChat[] {
    return [...this.hostModels];
  }
  getHostState(): string {
    return this.hostState;
  }
  getLastCall(): string {
    return this.lastCall;
  }

  async getKeyStatus(
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
  ): Promise<'configured' | 'missing' | 'unavailable'> {
    try {
      return (await this.groups.getApiKey(group)) ? 'configured' : 'missing';
    } catch {
      return 'unavailable';
    }
  }

  async refresh(): Promise<void> {
    this.changed.fire();
  }

  async invoke(
    group: CopilotModelGroup,
    modelId: string,
    prompt: string,
    token: vscode.CancellationToken,
  ): Promise<{ text: string; elapsedMs: number }> {
    if (
      !vscode.workspace.isTrusted ||
      vscode.env.uiKind === vscode.UIKind.Web ||
      vscode.env.remoteName
    ) {
      throw new Error(
        'Model calls and API key management require a trusted local desktop workspace.',
      );
    }
    if (!prompt.trim() || prompt.length > 32_000)
      throw new Error('Prompt must contain 1–32,000 characters.');
    const current = this.groups.getAll().find((item) => item.id === group.id);
    if (!current) throw new Error('This model group was deleted.');
    if (JSON.stringify(validateGroup(current)) !== JSON.stringify(validateGroup(group)))
      throw new Error('This model group changed; reopen it and select the model again.');
    group = validateGroup(current);
    const started = Date.now();
    const aborter = new AbortController();
    const requestToken = new vscode.CancellationTokenSource();
    const parentSubscription = token.onCancellationRequested(() => requestToken.cancel());
    this.active.set(aborter, { groupId: group.id, cancel: () => requestToken.cancel() });
    const subscription = requestToken.token.onCancellationRequested(() => aborter.abort());
    let timedOut = false;
    let timeout: NodeJS.Timeout | undefined;
    const timeoutPromise = new Promise<never>((_resolve, reject) => {
      timeout = setTimeout(() => {
        timedOut = true;
        requestToken.cancel();
        reject(new Error('Request timed out after 120 seconds.'));
      }, 120_000);
    });
    try {
      const perform = async () => {
        if (group!.source === 'vscode-lm') {
          const ref = group!.models.find((model) => model.id === modelId);
          if (!ref) throw new Error('The selected model is no longer in this group.');
          const model = this.hostModels.find(
            (candidate) => candidate.id === ref.id && candidate.vendor === ref.vendor,
          );
          if (!model)
            throw new Error(
              'Model is unavailable; refresh the host model catalog and bind it again.',
            );
          const response = await model.sendRequest(
            [vscode.LanguageModelChatMessage.User(prompt)],
            {},
            requestToken.token,
          );
          let output = '';
          for await (const part of response.text) {
            if (requestToken.token.isCancellationRequested) throw new Error('Request cancelled.');
            output += part;
          }
          return output;
        }
        const model = group!.models.find((item) => item.id === modelId);
        if (!model) throw new Error('The selected model is no longer in this group.');
        const key = await this.groups.getApiKey(group!);
        if (!key) throw new Error('API key is missing; configure a key for this endpoint group.');
        return completeCompatibleChat({
          baseUrl: group!.baseUrl,
          model: model.id,
          prompt,
          apiKey: key,
          signal: aborter.signal,
        });
      };
      const text = await Promise.race([perform(), timeoutPromise]);
      if (requestToken.token.isCancellationRequested) throw new Error('Request cancelled.');
      if (!text.trim()) throw new Error('The model returned no text.');
      const latest = this.groups.getAll().find((item) => item.id === group!.id);
      if (!latest || JSON.stringify(latest) !== JSON.stringify(group))
        throw new Error(
          'This model group changed while the request was running; the result was discarded.',
        );
      this.lastCall = `Succeeded · ${group.name} · ${modelId}`;
      return { text, elapsedMs: Date.now() - started };
    } catch (error) {
      if (timedOut) throw new Error('Request timed out after 120 seconds.');
      this.lastCall = `Failed · ${group.name} · ${modelId}`;
      throw error;
    } finally {
      clearTimeout(timeout);
      parentSubscription.dispose();
      subscription.dispose();
      requestToken.dispose();
      this.active.delete(aborter);
      this.changed.fire();
    }
  }

  async listCompatibleModels(
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
    token: vscode.CancellationToken,
  ): Promise<string[]> {
    const current = this.groups.getAll().find((item) => item.id === group.id);
    if (!current || JSON.stringify(current) !== JSON.stringify(group))
      throw new Error('This model group changed; reopen it before fetching models.');
    const key = await this.groups.getApiKey(group);
    if (!key) throw new Error('API key is missing.');
    const aborter = new AbortController();
    this.active.set(aborter, { groupId: group.id, cancel: () => aborter.abort() });
    const subscription = token.onCancellationRequested(() => aborter.abort());
    try {
      const models = await fetchCompatibleModels({
        baseUrl: group.baseUrl,
        apiKey: key,
        signal: aborter.signal,
      });
      const latest = this.groups.getAll().find((item) => item.id === group.id);
      if (!latest || JSON.stringify(latest) !== JSON.stringify(group))
        throw new Error('This model group changed while fetching models; results were discarded.');
      return models;
    } finally {
      subscription.dispose();
      this.active.delete(aborter);
    }
  }

  hostRef(model: vscode.LanguageModelChat): HostModelRef {
    return { id: model.id, vendor: model.vendor };
  }

  cancelGroup(groupId: string): void {
    for (const [request, active] of this.active)
      if (active.groupId === groupId) {
        request.abort();
        active.cancel();
      }
  }

  dispose(): void {
    for (const [request, active] of this.active) {
      request.abort();
      active.cancel();
    }
    this.active.clear();
    this.modelChange.dispose();
    this.changed.dispose();
  }
}
