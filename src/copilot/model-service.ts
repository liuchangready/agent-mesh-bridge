import * as vscode from 'vscode';
import { completeCompatibleChat, completeCompatibleTools, fetchCompatibleModels } from './compatible-model-client';
import { CopilotModelGroup, CopilotModelGroups, HostModelRef, validateGroup } from './model-groups';
import { GroupAgentRuntime, GroupAgentRunResult, GroupAgentRuntimeError, GroupMessage } from './group-agent-runtime';
import { GroupModelRef, TokenUsageMetrics } from '../bridge/types';
import { getBridgeHome } from '../bridge/model-catalog';
import { saveGroupAgentConfig, loadGroupAgentConfig, loadGroupAgentHost, GroupAgentBinding } from '../bridge/group-agent-registry';

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
  private groupRuntime?: GroupAgentRuntime;
  private groupAgentControl?: (context: { groupId: string; sessionId: string; threadId?: string; role: 'developer' | 'reviewer' | 'bootstrap'; extraEnv?: NodeJS.ProcessEnv }, name: string, input: Record<string, unknown>) => Promise<string>;
  private readonly modelAccess: vscode.LanguageModelAccessInformation;
  private readonly globalState: vscode.Memento;

  constructor(context: vscode.ExtensionContext) {
    this.modelAccess = context.languageModelAccessInformation;
    this.globalState = context.globalState;
    this.groups = new CopilotModelGroups(context.globalState, context.secrets);
    this.modelChange = vscode.lm.onDidChangeChatModels(() => {
      this.hostModels = [];
      this.hostState = 'not-refreshed';
      this.changed.fire();
    });
    context.subscriptions.push(this);
    context.subscriptions.push(context.secrets.onDidChange(() => this.changed.fire()));
    this.publishAgentConfig();
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
    this.publishAgentConfig();
    this.changed.fire();
  }

  getGroupAgentId(groupId: string): `copilot_group:${string}` {
    return `copilot_group:${groupId}`;
  }

  getAgentBinding(groupId: string): GroupAgentBinding | undefined {
    return loadGroupAgentConfig()?.agents.find((entry) => entry.groupId === groupId);
  }

  setGroupAgentControlHandler(handler: typeof this.groupAgentControl): void {
    this.groupAgentControl = handler;
    this.publishAgentConfig();
    this.changed.fire();
  }

  async configureGroupAgent(groupId: string, settings: {
    enabled: boolean;
    toolsAuthorized: boolean;
    receiverModel?: HostModelRef | GroupModelRef | string | null;
    initiatorModel?: HostModelRef | GroupModelRef | string | null;
  }): Promise<void> {
    const group = this.groups.getAll().find((entry) => entry.id === groupId);
    if (!group) throw new Error('The model group no longer exists.');
    const previous = (this.globalState.get<Record<string, any>>('copilot.groupAgentSettings.v1', {}) || {})[groupId] || {};
    const revision = (previous.revision || 0) + 1;
    const modelRef = (selection: HostModelRef | GroupModelRef | string | null | undefined): GroupModelRef | undefined => {
      if (selection === undefined || selection === null) return undefined;
      if (group.source === 'vscode-lm') {
        const hostSelection: HostModelRef | string = typeof selection === 'string'
          ? selection
          : 'source' in selection
            ? selection.source === 'vscode-lm' ? { id: selection.id, vendor: selection.vendor } : { id: selection.id, vendor: '' }
            : selection;
        if (typeof hostSelection === 'string' || !group.models.some((model) => model.id === hostSelection.id && model.vendor === hostSelection.vendor))
          throw new Error('The selected host model is not a member of this group.');
        const model = this.hostModels.find((candidate) => candidate.id === hostSelection.id && candidate.vendor === hostSelection.vendor);
        if (!model || this.modelAccess.canSendRequest(model) !== true)
          throw new Error('The host model is unavailable or its background request permission is not already granted.');
        return { source: 'vscode-lm', vendor: hostSelection.vendor, id: hostSelection.id };
      }
      const endpointId = typeof selection === 'string' ? selection : 'source' in selection && selection.source === 'openai-compatible' ? selection.id : undefined;
      if (!endpointId || !group.models.some((model) => model.id === endpointId))
        throw new Error('The selected endpoint model is not a member of this group.');
      if (!previous.toolsAuthorized && settings.toolsAuthorized !== true)
        throw new Error('Enable the group only after explicitly authorizing task tools.');
      const origin = new URL(group.baseUrl).origin;
      if (typeof selection === 'object' && (!('source' in selection) || selection.source !== 'openai-compatible' || selection.origin !== origin))
        throw new Error('The saved endpoint model is bound to a different source or origin.');
      return { source: 'openai-compatible', id: endpointId, origin, bindingRevision: revision };
    };
    const updated = {
      ...previous,
      enabled: settings.enabled,
      toolsAuthorized: settings.toolsAuthorized,
      revision,
    };
    if (settings.receiverModel !== undefined) {
      const receiverRef = modelRef(settings.receiverModel);
      if (receiverRef) updated.receiverModel = receiverRef;
      else delete updated.receiverModel;
    }
    if (settings.initiatorModel !== undefined) {
      const initiatorRef = modelRef(settings.initiatorModel);
      if (initiatorRef) updated.initiatorModel = initiatorRef;
      else delete updated.initiatorModel;
    }
    const all = this.globalState.get<Record<string, any>>('copilot.groupAgentSettings.v1', {}) || {};
    await this.globalState.update('copilot.groupAgentSettings.v1', { ...all, [groupId]: updated });
    this.publishAgentConfig();
    this.changed.fire();
  }

  async probeToolCalling(groupId: string, selection: HostModelRef | string, token: vscode.CancellationToken): Promise<void> {
    const group = this.groups.getAll().find((entry) => entry.id === groupId);
    if (!group) throw new Error('The model group was deleted.');
    const nonce = require('crypto').randomUUID() as string;
    const tool = { name: 'bridge_capability_probe', description: 'Return the supplied nonce unchanged. Do not request any other tool.', inputSchema: { type: 'object', properties: { nonce: { type: 'string' } }, required: ['nonce'], additionalProperties: false } };
    if (group.source === 'vscode-lm') {
      if (typeof selection === 'string' || !group.models.some((ref) => ref.id === selection.id && ref.vendor === selection.vendor))
        throw new Error('Select an exact host model that belongs to this group.');
      const model = this.hostModels.find((candidate) => candidate.id === selection.id && candidate.vendor === selection.vendor);
      if (!model) throw new Error('The selected host model is not currently visible. Refresh the catalog and grant host model access first.');
      const source = new vscode.CancellationTokenSource();
      const cancel = token.onCancellationRequested(() => source.cancel());
      try {
        const response = await model.sendRequest([vscode.LanguageModelChatMessage.User(`Call bridge_capability_probe with nonce ${nonce}.`)], { tools: [tool] }, source.token);
        const calls: vscode.LanguageModelToolCallPart[] = [];
        for await (const part of response.stream) if (part instanceof vscode.LanguageModelToolCallPart) calls.push(part);
        if (calls.length !== 1 || calls[0].name !== tool.name || (calls[0].input as Record<string, unknown>)?.nonce !== nonce)
          throw new Error('The selected host model did not return the exact structured capability probe.');
      } finally { cancel.dispose(); source.dispose(); }
    } else {
      if (typeof selection !== 'string' || !group.models.some((model) => model.id === selection)) throw new Error('Select an exact endpoint model that belongs to this group.');
      const apiKey = await this.groups.getApiKey(group);
      if (!apiKey) throw new Error('Configure the endpoint API key before probing.');
      const aborter = new AbortController();
      const cancel = token.onCancellationRequested(() => aborter.abort());
      try {
        const result = await completeCompatibleTools({
          baseUrl: group.baseUrl, model: selection,
          messages: [{ role: 'user', content: `Call bridge_capability_probe with nonce ${nonce}.` }],
          tools: [tool], apiKey, signal: aborter.signal,
        });
        if (result.finishReason !== 'tool_calls' || result.toolCalls.length !== 1 || result.toolCalls[0].name !== tool.name || result.toolCalls[0].arguments.nonce !== nonce)
          throw new Error('The selected endpoint model did not return the exact structured capability probe.');
      } finally { cancel.dispose(); }
    }
  }

  async markGroupAgentRoleReady(groupId: string, role: 'receiver' | 'initiator'): Promise<void> {
    const group = this.groups.getAll().find((entry) => entry.id === groupId);
    const settings = this.globalState.get<Record<string, any>>('copilot.groupAgentSettings.v1', {}) || {};
    const saved = settings[groupId];
    const selection = role === 'receiver' ? saved?.receiverModel : saved?.initiatorModel;
    if (!group || !saved?.enabled || !saved.toolsAuthorized || !selection) throw new Error('Configure and authorize this Group Agent role before marking it ready.');
    if (role === 'initiator' && !this.groupAgentControl) throw new Error('Group reviewer control services are unavailable in this host.');
    const host = loadGroupAgentHost();
    if (!host) throw new Error('The trusted local VS Code host/workspace is offline.');
    const probeKey = JSON.stringify(selection);
    const next = { ...saved, [`${role}Probe`]: { model: probeKey, revision: saved.revision, hostGeneration: host.generation } };
    await this.globalState.update('copilot.groupAgentSettings.v1', { ...settings, [groupId]: next });
    this.publishAgentConfig();
    this.changed.fire();
  }

  private publishAgentConfig(): void {
    const settings = this.globalState.get<Record<string, any>>('copilot.groupAgentSettings.v1', {});
    const agents: GroupAgentBinding[] = this.groups.getAll().map((group) => {
      const saved = settings[group.id] || {};
      const receiverModel = saved.receiverModel;
      const initiatorModel = saved.initiatorModel;
      const revision = Number.isInteger(saved.revision) ? saved.revision : 0;
      const host = loadGroupAgentHost();
      const ready = (role: 'receiver' | 'initiator', ref: unknown) => {
        if (role === 'initiator' && !this.groupAgentControl) return false;
        const probe = saved[`${role}Probe`];
        return Boolean(ref && host && probe && probe.model === JSON.stringify(ref) && probe.hostGeneration === host.generation);
      };
      return {
        id: this.getGroupAgentId(group.id), groupId: group.id, name: group.name,
        source: group.source, ...(group.source === 'openai-compatible' ? { baseUrl: group.baseUrl } : {}),
        enabled: saved.enabled === true, toolsAuthorized: saved.toolsAuthorized === true,
        receiverReady: ready('receiver', receiverModel), initiatorReady: ready('initiator', initiatorModel),
        revision,
        models: group.source === 'vscode-lm'
          ? group.models.map((model) => ({ source: 'vscode-lm' as const, vendor: model.vendor, id: model.id }))
          : group.models.map((model) => ({ source: 'openai-compatible' as const, id: model.id, origin: new URL(group.baseUrl).origin, bindingRevision: revision })),
        ...(receiverModel ? { receiverModel } : {}),
        ...(initiatorModel ? { initiatorModel } : {}),
      };
    });
    const config = vscode.workspace.getConfiguration('agentMeshBridge');
    saveGroupAgentConfig({
      version: 1, updatedAt: Date.now(), agents,
      defaultAgent: config.get<string>('defaultAgent', 'auto'),
      defaultReviewer: config.get<string>('defaultReviewer', 'caller'),
    });
  }

  async invoke(
    group: CopilotModelGroup,
    selection: HostModelRef | string,
    prompt: string,
    token: vscode.CancellationToken,
  ): Promise<{ text: string; elapsedMs: number }> {
    if (token.isCancellationRequested) throw new Error('Request cancelled.');
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
    const modelId = typeof selection === 'string' ? selection : selection.id;
    const started = Date.now();
    const aborter = new AbortController();
    const requestToken = new vscode.CancellationTokenSource();
    const parentSubscription = token.onCancellationRequested(() => requestToken.cancel());
    this.active.set(aborter, { groupId: group.id, cancel: () => requestToken.cancel() });
    let rejectCancelled!: (error: Error) => void;
    const cancellationPromise = new Promise<never>((_resolve, reject) => {
      rejectCancelled = reject;
    });
    const subscription = requestToken.token.onCancellationRequested(() => {
      aborter.abort();
      rejectCancelled(new Error('Request cancelled.'));
    });
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
          if (typeof selection === 'string')
            throw new Error('Host model calls require the selected vendor and id.');
          const ref = group!.models.find(
            (model) => model.id === selection.id && model.vendor === selection.vendor,
          );
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
        if (typeof selection !== 'string')
          throw new Error('Endpoint model calls require a model id.');
        const model = group!.models.find((item) => item.id === selection);
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
      const text = await Promise.race([perform(), timeoutPromise, cancellationPromise]);
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

  async runGroupAgent(
    groupId: string,
    modelRef: GroupModelRef,
    sessionId: string,
    cwd: string,
    prompt: string,
    token: vscode.CancellationToken,
    onChildPid?: (pid: number | undefined) => void,
    role: 'developer' | 'reviewer' | 'bootstrap' = 'developer',
    controlContext?: { threadId?: string; extraEnv?: NodeJS.ProcessEnv },
  ): Promise<GroupAgentRunResult> {
    if (!vscode.workspace.isTrusted || vscode.env.uiKind === vscode.UIKind.Web || vscode.env.remoteName)
      throw new Error('Group Agent execution requires a trusted local desktop workspace.');
    if (!prompt.trim() || Buffer.byteLength(prompt) > 32_000) throw new Error('Task prompt is empty or exceeds 32 KB.');
    const group = this.groups.getAll().find((entry) => entry.id === groupId);
    if (!group) throw new Error('The Group Agent binding was removed.');
    const binding = loadGroupAgentConfig()?.agents.find((entry) => entry.groupId === groupId);
    const configuredModel = role === 'developer' ? binding?.receiverModel : binding?.initiatorModel;
    if (!binding?.enabled || !binding.toolsAuthorized || !configuredModel)
      throw new Error('The Group Agent role binding changed or is not authorized.');
    if (modelRef.source === 'openai-compatible' && (group.source !== 'openai-compatible' || modelRef.origin !== new URL(group.baseUrl).origin))
      throw new Error('The pinned Group Agent endpoint origin changed.');
    const runtime = new GroupAgentRuntime({
      bridgeHome: getBridgeHome(),
      onChildPid,
      ...(this.groupAgentControl ? { controlTool: (name, input) => this.groupAgentControl!({ groupId, sessionId, role, ...controlContext }, name, input) } : {}),
      request: async (messages, tools, signal) => {
        const selected = group.source === 'vscode-lm'
          ? modelRef.source === 'vscode-lm' && group.models.some((ref) => ref.id === modelRef.id && ref.vendor === modelRef.vendor)
          : modelRef.source === 'openai-compatible' && modelRef.origin === new URL(group.baseUrl).origin && group.models.some((model) => model.id === modelRef.id);
        if (!selected) throw new Error('The pinned Group Agent model no longer matches this group binding.');
        const current = this.groups.getAll().find((entry) => entry.id === groupId);
        if (!current || JSON.stringify(validateGroup(current)) !== JSON.stringify(validateGroup(group)))
          throw new Error('The Group Agent binding changed during execution.');
        if (group.source === 'openai-compatible') {
          if (modelRef.source !== 'openai-compatible') throw new Error('Endpoint group requires an endpoint model reference.');
          const apiKey = await this.groups.getApiKey(group);
          if (!apiKey) throw new Error('Group Agent API key is missing.');
          const apiMessages = messages.map((message) => ({
            role: message.role,
            content: message.content,
            ...(message.toolCallId ? { tool_call_id: message.toolCallId, name: message.toolName } : {}),
            ...(message.toolCalls ? { tool_calls: message.toolCalls.map((call) => ({ id: call.id, type: 'function' as const, function: { name: call.name, arguments: JSON.stringify(call.arguments) } })) } : {}),
          }));
          return completeCompatibleTools({
            baseUrl: group.baseUrl,
            model: modelRef.id,
            messages: apiMessages,
            tools,
            apiKey,
            signal,
          });
        }
        if (modelRef.source !== 'vscode-lm') throw new Error('Host model group requires a vendor/id reference.');
        const model = this.hostModels.find((candidate) => candidate.id === modelRef.id && candidate.vendor === modelRef.vendor);
        if (!model || this.modelAccess.canSendRequest(model) !== true)
          throw new Error('The exact VS Code host model is unavailable or has not been authorized for background requests.');
        const requestToken = new vscode.CancellationTokenSource();
        const abortRequest = () => requestToken.cancel();
        signal.addEventListener('abort', abortRequest, { once: true });
        const schemaTools: vscode.LanguageModelChatTool[] = tools.map((tool) => ({ name: tool.name, description: tool.description, inputSchema: tool.inputSchema }));
        try {
          const input = messages.map((message) => {
            if (message.role === 'tool') return vscode.LanguageModelChatMessage.User([
              new vscode.LanguageModelToolResultPart(message.toolCallId!, [new vscode.LanguageModelTextPart(message.content)]),
            ]);
            if (message.role === 'assistant') return vscode.LanguageModelChatMessage.Assistant([
              ...(message.content ? [new vscode.LanguageModelTextPart(message.content)] : []),
              ...(message.toolCalls || []).map((call) => new vscode.LanguageModelToolCallPart(call.id, call.name, call.arguments)),
            ]);
            return vscode.LanguageModelChatMessage.User(message.content);
          });
          const response = await model.sendRequest(input, { tools: schemaTools }, requestToken.token);
          let text = '';
          const toolCalls: Array<{ id: string; name: string; arguments: Record<string, unknown> }> = [];
          for await (const part of response.stream) {
            if (signal.aborted) throw new Error('Request cancelled.');
            if (part instanceof vscode.LanguageModelTextPart) text += part.value;
            else if (part instanceof vscode.LanguageModelToolCallPart) {
              if (!part.input || typeof part.input !== 'object' || Array.isArray(part.input)) throw new Error('Host model returned invalid tool arguments.');
              toolCalls.push({ id: part.callId, name: part.name, arguments: part.input as Record<string, unknown> });
            } else if (part !== undefined && part !== null) throw new Error('Host model returned an unsupported response part.');
          }
          return { text, toolCalls, finishReason: toolCalls.length ? 'tool_calls' as const : 'stop' as const };
        } finally {
          requestToken.dispose();
          signal.removeEventListener('abort', abortRequest);
        }
      },
    });
    this.groupRuntime = runtime;
    const aborter = new AbortController();
    const subscription = token.onCancellationRequested(() => aborter.abort());
    try {
      const result = await runtime.run(sessionId, cwd, prompt, aborter.signal, role);
      const collectedAt = new Date().toISOString();
      const tokenUsage: TokenUsageMetrics = {
        uncachedInputTokens: result.usage?.uncachedInputTokens ?? 0,
        cacheReadTokens: result.usage?.cacheReadTokens ?? 0,
        cacheWriteTokens: result.usage?.cacheWriteTokens ?? 0,
        promptTokens: result.usage?.promptTokens ?? 0,
        outputTokens: result.usage?.outputTokens ?? 0,
        totalTokens: result.usage?.totalTokens ?? 0,
        ...(result.usage?.nativeTotalTokens !== undefined ? { nativeTotalTokens: result.usage.nativeTotalTokens } : {}),
        agent: this.getGroupAgentId(groupId), model: modelRef.id,
        source: result.usage ? 'native_reported' : 'unavailable',
        available: Boolean(result.usage), partial: result.usageRequests < result.modelRequests,
        attempts: result.modelRequests,
        ...(!result.usage ? { notes: 'The provider did not report native usage; input and output are unknown.' } : {}),
        collectedAt,
      };
      return { ...result, tokenUsage };
    } catch (error) {
      if (error instanceof GroupAgentRuntimeError) {
        const collectedAt = new Date().toISOString();
        const tokenUsage: TokenUsageMetrics = {
          uncachedInputTokens: error.usage?.uncachedInputTokens ?? 0,
          cacheReadTokens: error.usage?.cacheReadTokens ?? 0,
          cacheWriteTokens: error.usage?.cacheWriteTokens ?? 0,
          promptTokens: error.usage?.promptTokens ?? 0,
          outputTokens: error.usage?.outputTokens ?? 0,
          totalTokens: error.usage?.totalTokens ?? 0,
          ...(error.usage?.nativeTotalTokens !== undefined ? { nativeTotalTokens: error.usage.nativeTotalTokens } : {}),
          agent: this.getGroupAgentId(groupId), model: modelRef.id,
          source: error.usage ? 'native_reported' : 'unavailable', available: Boolean(error.usage),
          partial: true, attempts: error.modelRequests,
          notes: error.usageRequests < error.modelRequests ? 'Some model requests returned no native usage.' : 'Execution ended before a final response.',
          collectedAt,
        };
        throw Object.assign(new Error(error.message), { tokenUsage, modelRequests: error.modelRequests });
      }
      throw error;
    } finally {
      subscription.dispose();
    }
  }

  readGroupAgentSession(sessionId: string, offset = 0, limit = 100): GroupMessage[] {
    if (!this.groupRuntime) this.groupRuntime = new GroupAgentRuntime({ bridgeHome: getBridgeHome(), request: async () => { throw new Error('Session history is read-only.'); } });
    return this.groupRuntime.sessions.read(sessionId, offset, limit);
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
