import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
const Module = require('module');
const http = require('node:http');
const net = require('node:net');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

test('registered commands and tree nodes preserve exact host identity; invoke settles on cancellation', async () => {
  let selected = 0;
  let vendorACalls = 0;
  let vendorBCalls = 0;
  let releasePendingSend: (() => void) | undefined;
  let releasePendingStream: (() => void) | undefined;
  let pendingSendStarted!: () => void;
  let pendingStreamStarted!: () => void;
  const sendStarted = new Promise<void>((resolve) => (pendingSendStarted = resolve));
  const streamStarted = new Promise<void>((resolve) => (pendingStreamStarted = resolve));
  let provider: any;
  let shownResult = '';
  let apiAuthorization = '';
  const uiErrors: string[] = [];
  const registeredCommands = new Map<string, (...args: any[]) => any>();
  class EventEmitter<T> {
    private listeners = new Set<() => void>();
    event = (listener: () => void) => {
      this.listeners.add(listener);
      return { dispose: () => this.listeners.delete(listener) };
    };
    fire() {
      for (const listener of this.listeners) listener();
    }
    dispose() {}
  }
  class CancellationTokenSource {
    private callbacks: Array<() => void> = [];
    token = {
      isCancellationRequested: false,
      onCancellationRequested: (callback: () => void) => {
        this.callbacks.push(callback);
        return { dispose() {} };
      },
    };
    cancel() {
      this.token.isCancellationRequested = true;
      for (const callback of this.callbacks) callback();
    }
    dispose() {}
  }
  class TreeItem {
    label: string;
    collapsibleState: number;
    constructor(label: string, collapsibleState: number) {
      this.label = label;
      this.collapsibleState = collapsibleState;
    }
  }
  const responseText = (text: string) => ({
    text: (async function* () {
      yield text;
    })(),
  });
  const models = [
    {
      id: 'opaque-1',
      vendor: 'vendor-a',
      name: 'Same',
      sendRequest: async (messages: any[]) => {
        vendorACalls++;
        const prompt = messages[0].content;
        if (prompt === 'pending-send') {
          pendingSendStarted();
          return await new Promise<any>((resolve) => {
            releasePendingSend = () => resolve(responseText('late answer'));
          });
        }
        if (prompt === 'pending-stream') {
          pendingStreamStarted();
          let pending = true;
          return {
            text: {
              [Symbol.asyncIterator]() {
                return {
                  next: () =>
                    pending
                      ? new Promise<IteratorResult<string>>((resolve) => {
                          releasePendingStream = () => {
                            pending = false;
                            resolve({ done: false, value: 'late chunk' });
                          };
                        })
                      : Promise.resolve({ done: true, value: undefined }),
                };
              },
            },
          };
        }
        assert.deepEqual(messages, [{ role: 'user', content: 'question' }]);
        return responseText('vendor a answer');
      },
    },
    {
      id: 'opaque-1',
      vendor: 'vendor-b',
      name: 'Same',
      sendRequest: async (messages: any[]) => {
        vendorBCalls++;
        assert.deepEqual(messages, [{ role: 'user', content: 'question' }]);
        return responseText('vendor b answer');
      },
    },
  ];
  const fakeVscode = {
    EventEmitter,
    CancellationTokenSource,
    TreeItem,
    TreeItemCollapsibleState: { None: 0, Collapsed: 1, Expanded: 2 },
    ThemeIcon: class { constructor(public id: string, public color?: unknown) {} },
    ThemeColor: class { constructor(public id: string) {} },
    lm: {
      onDidChangeChatModels: () => ({ dispose() {} }),
      selectChatModels: async () => {
        selected++;
        return models;
      },
    },
    workspace: {
      isTrusted: true,
      getConfiguration: () => ({ get: (_key: string, fallback?: unknown) => fallback }),
      registerTextDocumentContentProvider: (_scheme: string, contentProvider: any) => {
        provider = contentProvider;
        return { dispose() {} };
      },
    },
    commands: {
      registerCommand: (name: string, callback: (...args: any[]) => any) => {
        registeredCommands.set(name, callback);
        return { dispose() {} };
      },
    },
    env: { uiKind: 1, remoteName: undefined },
    UIKind: { Web: 2 },
    ProgressLocation: { Notification: 1 },
    Uri: { parse: (value: string) => ({ toString: () => value }) },
    LanguageModelChatMessage: { User: (content: string) => ({ role: 'user', content }) },
    window: {
      showQuickPick: async (items: any[]) =>
        items.find((item) => item.ref?.vendor === 'vendor-b') ??
        items.find((item) => item.source === 'openai-compatible') ??
        items.find((item) => item.value === 'default'),
      showInputBox: async (options: any) => {
        if (options.prompt === 'Group display name') return 'Local API';
        if (options.prompt === 'OpenAI-compatible API base URL') return baseUrl;
        if (options.prompt.startsWith('Model IDs')) return 'remote-model';
        if (options.prompt.startsWith('Optional API key')) return 'integration-secret';
        if (options.prompt.startsWith('Prompt for')) return 'question';
        return undefined;
      },
      withProgress: async (_options: any, task: (progress: any, token: any) => Promise<any>) =>
        task(
          {},
          { isCancellationRequested: false, onCancellationRequested: () => ({ dispose() {} }) },
        ),
      showTextDocument: async (uri: any) => {
        shownResult = provider.provideTextDocumentContent(uri);
      },
      showInformationMessage: async () => undefined,
      showWarningMessage: async () => undefined,
      showErrorMessage: (message: string) => {
        uiErrors.push(message);
      },
    },
  };
  const server = http.createServer((request: any, response: any) => {
    apiAuthorization = request.headers.authorization;
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
    request.on('end', () => {
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (body.tools?.some((tool: any) => tool.function?.name === 'bridge_capability_probe')) {
        const nonce = /nonce ([a-f0-9-]+)/i.exec(body.messages?.[0]?.content || '')?.[1];
        response.end(JSON.stringify({ choices: [{ finish_reason: 'tool_calls', message: { content: null, tool_calls: [{ id: 'probe-call', type: 'function', function: { name: 'bridge_capability_probe', arguments: JSON.stringify({ nonce }) } }] } }] }));
      } else response.end(JSON.stringify({ choices: [{ message: { content: 'integrated answer' } }] }));
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}/v1`;
  const originalLoad = Module._load;
  const originalBridgeHome = process.env.AGENT_MESH_BRIDGE_HOME;
  const bridgeHome = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-settings-'));
  process.env.AGENT_MESH_BRIDGE_HOME = bridgeHome;
  Module._load = function (request: string, parent: unknown, isMain: boolean) {
    return request === 'vscode' ? fakeVscode : originalLoad.call(this, request, parent, isMain);
  };
  try {
    const { CopilotModelService } = require('../copilot/model-service');
    const state = new Map<string, unknown>();
    const secrets = new Map<string, string>();
    const context = {
      globalState: {
        get: (key: string, fallback?: unknown) => state.has(key) ? state.get(key) : fallback,
        update: async (key: string, value: unknown) => {
          state.set(key, value);
        },
      },
      secrets: {
        get: async (key: string) => secrets.get(key),
        store: async (key: string, value: string) => secrets.set(key, value),
        delete: async (key: string) => secrets.delete(key),
        onDidChange: () => ({ dispose() {} }),
      },
      subscriptions: [] as any[],
    };
    const service = new CopilotModelService(context as any);
    assert.equal(selected, 0);
    await service.refreshHostModels();
    assert.equal(selected, 1);
    const group = await service.groups.save({
      id: '11111111-2222-4333-8444-555555555557',
      name: 'Host group',
      source: 'vscode-lm',
      models: [
        { id: 'opaque-1', vendor: 'vendor-a' },
        { id: 'opaque-1', vendor: 'vendor-b' },
      ],
    });
    const token = {
      isCancellationRequested: false,
      onCancellationRequested: () => ({ dispose() {} }),
    } as any;
    const result = await service.invoke(group, { id: 'opaque-1', vendor: 'vendor-a' }, 'question', token);
    assert.equal(result.text, 'vendor a answer');
    assert.equal(vendorACalls, 1);
    assert.equal(vendorBCalls, 0);
    assert.match(service.getLastCall(), /Succeeded/);
    const secondVendorResult = await service.invoke(
      group,
      { id: 'opaque-1', vendor: 'vendor-b' },
      'question',
      token,
    );
    assert.equal(secondVendorResult.text, 'vendor b answer');
    const { registerCopilotModelCommands } = require('../copilot/model-commands');
    const commandDisposables = registerCopilotModelCommands(context as any, service);
    await registeredCommands.get('agent-mesh-bridge.callCopilotModel')!(group);
    assert.equal(vendorBCalls, 2, 'QuickPick must invoke vendor-b when both vendors share an id');

    const { AgentNodesProvider } = require('../ui/agent-nodes-provider');
    const info = (id: string, name: string) => ({ id, name, status: 'online', available: true });
    const providerNodes = new AgentNodesProvider(
      {
        probeAll: async () => ({
          antigravity: info('antigravity', 'agy'),
          codex: info('codex', 'Codex'),
          claude_code: info('claude_code', 'Claude'),
          deepseek_harness: info('deepseek_harness', 'DSH'),
          vscode_chat: info('vscode_chat', 'Copilot'),
        }),
      } as any,
      { resolveActiveAgent: async () => ({ agent: 'codex' }) } as any,
      service,
    );
    const nodes = await providerNodes.getChildren();
    const copilotNode = nodes.find((node: any) => node.info?.id === 'vscode_chat');
    const hostGroupNode = copilotNode?.subItems?.find((node: any) => node.label === 'Host group');
    assert.deepEqual(hostGroupNode?.group, group, 'group tree items retain their configuration identity');
    const { pickModelAgent, pickGroupReceiverModel } = require('../ui/model-agent-picker');
    const savedPicker = fakeVscode.window.showQuickPick;
    try {
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.group?.id === group.id);
      const choice = await pickModelAgent(service, [{ label: 'Codex', agent: 'codex' }], 'initiator', 'Choose sender');
      assert.equal(choice.agent, service.getGroupAgentId(group.id));
      assert.deepEqual(choice.group.models, group.models, 'unconfigured groups remain selectable with both exact vendor identities');
      assert.match(choice.description, /未配置/);
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.agent === 'codex');
      assert.equal((await pickModelAgent(service, [{ label: 'Codex', agent: 'codex' }], 'receiver', 'Choose receiver')).agent, 'codex');
      fakeVscode.window.showQuickPick = async () => undefined;
      assert.equal(await pickModelAgent(service, [], 'receiver', 'Cancel'), undefined);
      let configuredRole: unknown;
      (fakeVscode.commands as any).executeCommand = async (command: string, selectedGroup: unknown, role: unknown) => {
        assert.equal(command, 'agent-mesh-bridge.configureCopilotGroupAgent');
        assert.equal(selectedGroup, undefined);
        configuredRole = role;
      };
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.action === 'configure-group');
      assert.equal(await pickModelAgent({ groups: { getAll: () => [] } }, [], 'initiator', 'Configure groups'), undefined);
      assert.equal(configuredRole, 'initiator', 'group setup is reachable from the shared model picker');
    } finally {
      fakeVscode.window.showQuickPick = savedPicker;
    }
    const unconfiguredSender = hostGroupNode?.subItems?.find((node: any) => node.label === '下发模型: 未配置');
    assert.ok(unconfiguredSender, 'sender configuration is visible before enabling a receiver');
    assert.deepEqual(unconfiguredSender.command.arguments, [group, 'initiator']);
    assert.ok(hostGroupNode?.subItems?.some((node: any) => node.label === '接收模型: 未配置'));
    assert.ok(copilotNode?.subItems?.some((node: any) => node.label === '配置下发者 / 检视者'));
    const vendorBNode = hostGroupNode?.subItems?.find((node: any) => node.description === 'vendor-b');
    assert.ok(vendorBNode);
    assert.deepEqual(vendorBNode.command.arguments[1], { id: 'opaque-1', vendor: 'vendor-b' });
    const treeCall = registeredCommands.get(vendorBNode.command.command);
    assert.ok(treeCall);
    await treeCall(...vendorBNode.command.arguments);
    assert.deepEqual(uiErrors, []);
    assert.equal(vendorBCalls, 3, 'tree command must call the exact vendor-b reference');

    await registeredCommands.get('agent-mesh-bridge.addCopilotModelGroup')!();
    const apiGroup = service.groups.getAll().find((item: any) => item.name === 'Local API');
    assert.ok(apiGroup);
    await registeredCommands.get('agent-mesh-bridge.callCopilotModel')!(apiGroup, 'remote-model');
    assert.equal(apiAuthorization, 'Bearer integration-secret');
    assert.match(shownResult, /integrated answer/);
    assert.match(shownResult, /remote-model/);
    assert.equal(shownResult.includes('integration-secret'), false);

    const hostRegistry = require('../bridge/group-agent-registry');
    hostRegistry.writeGroupAgentHost({ version: 1, hostInstanceId: 'sender-ui-host', generation: 1, pid: process.pid,
      workspaceUri: `file://${bridgeHome}`, repositoryCommonDir: bridgeHome, trusted: true, localDesktop: true, updatedAt: Date.now() }, bridgeHome);
    service.setGroupAgentControlHandler(async () => ({}));
    const originalQuickPick = fakeVscode.window.showQuickPick;
    const originalWarning = fakeVscode.window.showWarningMessage;
    let authorizeSender = false;
    let senderMenuVisible = false;
    (fakeVscode.window as any).showWarningMessage = async () => authorizeSender ? 'Authorize and enable' : undefined;
    fakeVscode.window.showQuickPick = async (items: any[]) => {
      if (items.some(item => item.id === 'agent-receiver')) {
        senderMenuVisible = items.some(item => item.id === 'agent-sender');
        return items.find(item => item.id === 'agent-sender');
      }
      return items.find(item => item.id === 'remote-model') ?? items.find(item => item.value === 'default');
    };
    try {
      await registeredCommands.get('agent-mesh-bridge.manageCopilotModelGroup')!(apiGroup);
      assert.equal(senderMenuVisible, true, 'sender menu is visible for a disabled group');
      assert.equal(service.getAgentBinding(apiGroup.id)?.initiatorModel, undefined, 'declining tool authorization leaves the sender unconfigured');
      authorizeSender = true;
      const initialTree = await providerNodes.getChildren();
      const apiNode = initialTree.find((node: any) => node.info?.id === 'vscode_chat')?.subItems?.find((node: any) => node.label === apiGroup.name);
      const senderNode = apiNode.subItems.find((node: any) => node.label === '下发模型: 未配置');
      await registeredCommands.get(senderNode.command.command)!(...senderNode.command.arguments);
      const senderBinding = service.getAgentBinding(apiGroup.id)!;
      assert.equal(senderBinding.enabled, true);
      assert.equal(senderBinding.toolsAuthorized, true);
      assert.equal(senderBinding.initiatorReady, true, 'the sender passes its real structured-tool probe');
      assert.equal(senderBinding.initiatorModel?.id, 'remote-model');
      assert.equal(senderBinding.receiverModel, undefined, 'sender-only setup must not require a receiver');
      assert.equal(senderBinding.receiverReady, false);
      const configuredTree = await providerNodes.getChildren();
      const configuredGroup = configuredTree.find((node: any) => node.info?.id === 'vscode_chat')?.subItems?.find((node: any) => node.label === apiGroup.name);
      assert.ok(configuredGroup.subItems.some((node: any) => node.label === '下发模型: remote-model' && node.description.includes('就绪')));
      assert.deepEqual(uiErrors, []);
    } finally {
      fakeVscode.window.showQuickPick = originalQuickPick;
      fakeVscode.window.showWarningMessage = originalWarning;
    }

    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true, receiverModel: 'remote-model' });
    const pinnedReceiver = service.getAgentBinding(apiGroup.id)!.receiverModel;
    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true, initiatorModel: 'remote-model' });
    let savedBinding = service.getAgentBinding(apiGroup.id)!;
    assert.deepEqual(savedBinding.receiverModel, pinnedReceiver, 'saving sender settings must not rewrite a pinned receiver model');
    assert.ok(savedBinding, fs.readFileSync(path.join(bridgeHome, 'group-agent-config.json'), 'utf8'));
    assert.equal(savedBinding.receiverModel?.source, 'openai-compatible');
    assert.equal(savedBinding.initiatorModel?.source, 'openai-compatible');
    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true, receiverModel: null });
    savedBinding = service.getAgentBinding(apiGroup.id)!;
    assert.equal(savedBinding.receiverModel, undefined);
    assert.equal(savedBinding.initiatorModel?.id, 'remote-model');
    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true, receiverModel: 'remote-model' });
    const pinnedSender = service.getAgentBinding(apiGroup.id)!.initiatorModel;
    await service.configureGroupAgent(apiGroup.id, { enabled: false, toolsAuthorized: true });
    assert.deepEqual(service.getAgentBinding(apiGroup.id)!.initiatorModel, pinnedSender, 'changing enabled state must preserve the other side model reference');
    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true });
    await service.configureGroupAgent(apiGroup.id, { enabled: true, toolsAuthorized: true, initiatorModel: null });
    savedBinding = service.getAgentBinding(apiGroup.id)!;
    assert.equal(savedBinding.receiverModel?.id, 'remote-model');
    assert.equal(savedBinding.initiatorModel, undefined);

    const capabilityGroup = await service.groups.save({ ...apiGroup, name: 'Capability API', models: [
      { id: 'remote-model', name: 'Default member' }, { id: 'other-member', name: 'Explicitly probed member' },
    ] });
    const host = { version: 1, hostInstanceId: 'test-host', generation: 17, pid: process.pid,
      workspaceUri: `file://${bridgeHome}`, repositoryCommonDir: bridgeHome, trusted: true, localDesktop: true, updatedAt: Date.now() };
    require('../bridge/group-agent-registry').writeGroupAgentHost(host, bridgeHome);
    await service.probeToolCalling(capabilityGroup.id, 'remote-model', token, 'receiver');
    await service.configureGroupAgent(capabilityGroup.id, { enabled: true, toolsAuthorized: true, receiverModel: 'remote-model' });
    await service.markGroupAgentRoleReady(capabilityGroup.id, 'receiver');
    let capabilityBinding = service.getAgentBinding(capabilityGroup.id)!;
    const agentId = service.getGroupAgentId(capabilityGroup.id);
    const { assertGroupModelReady } = require('../bridge/agent-capabilities');
    assert.throws(() => assertGroupModelReady(agentId, 'receiver', capabilityBinding.models!.find((ref: any) => ref.id === 'other-member')), /no current receiver structured-tool probe/);
    const oldRetargetPicker = fakeVscode.window.showQuickPick;
    try {
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.modelRef?.id === 'other-member');
      const alternate = await pickGroupReceiverModel(service, capabilityGroup);
      assert.deepEqual(alternate.modelRef, capabilityBinding.models!.find((ref: any) => ref.id === 'other-member'), 'retarget selection preserves source, origin, and binding revision');
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.description?.includes('配置默认'));
      assert.deepEqual((await pickGroupReceiverModel(service, capabilityGroup, alternate.modelRef)).modelRef, capabilityBinding.receiverModel, 'choosing default explicitly replaces a non-default model');
      fakeVscode.window.showQuickPick = async (items: any[]) => items.find(item => item.description === 'keep');
      assert.deepEqual(await pickGroupReceiverModel(service, capabilityGroup, alternate.modelRef), {}, 'keep does not send an override');
      fakeVscode.window.showQuickPick = async () => undefined;
      assert.equal(await pickGroupReceiverModel(service, capabilityGroup), undefined);
    } finally {
      fakeVscode.window.showQuickPick = oldRetargetPicker;
    }
    capabilityBinding = service.getAgentBinding(capabilityGroup.id)!;
    assert.equal(capabilityBinding.receiverModel?.id, 'remote-model', 'probing an alternate must not change the configured default');
    assertGroupModelReady(agentId, 'receiver', capabilityBinding.models!.find((ref: any) => ref.id === 'other-member'));
    const changedEndpoint = await service.groups.save({ ...capabilityGroup, baseUrl: `${baseUrl}/different-backend` });
    await service.refresh();
    capabilityBinding = service.getAgentBinding(changedEndpoint.id)!;
    assert.equal(capabilityBinding.receiverReady, false, 'changing the endpoint path invalidates old probe evidence even with the same origin');
    assert.throws(() => assertGroupModelReady(agentId, 'receiver', capabilityBinding.models!.find((ref: any) => ref.id === 'other-member')), /no current receiver structured-tool probe/);

    const pendingToken = new CancellationTokenSource();
    const pendingCall = service.invoke(group, { id: 'opaque-1', vendor: 'vendor-a' }, 'pending-send', pendingToken.token as any);
    await sendStarted;
    pendingToken.cancel();
    await assert.rejects(pendingCall, /Request cancelled/);
    releasePendingSend!();
    await new Promise((resolve) => setImmediate(resolve));
    assert.match(service.getLastCall(), /Failed/);

    const streamToken = new CancellationTokenSource();
    const pendingStream = service.invoke(group, { id: 'opaque-1', vendor: 'vendor-a' }, 'pending-stream', streamToken.token as any);
    await streamStarted;
    service.cancelGroup(group.id);
    await assert.rejects(pendingStream, /Request cancelled/);
    releasePendingStream!();
    await new Promise((resolve) => setImmediate(resolve));
    assert.match(service.getLastCall(), /Failed/);

    const alreadyCancelled = new CancellationTokenSource();
    alreadyCancelled.cancel();
    const callsBeforeCancelledInvoke = vendorACalls;
    await assert.rejects(
      service.invoke(group, { id: 'opaque-1', vendor: 'vendor-a' }, 'question', alreadyCancelled.token as any),
      /Request cancelled/,
    );
    assert.equal(vendorACalls, callsBeforeCancelledInvoke, 'an already-cancelled token must not call a provider');

    const taskStore = new (require('../bridge/task-store').TaskStore)(bridgeHome);
    service.setTaskStore(taskStore);
    const groupAgentId = service.getGroupAgentId(capabilityGroup.id);
    taskStore.upsertTask({ threadId: 'thread_group_guard_active', title: 'active Group task', initiatorAgent: 'codex', targetAgent: groupAgentId,
      status: 'running', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      receiverSessionId: 'session_group_history_1', workspacePath: bridgeHome, rounds: [],
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 } } as any);
    await assert.rejects(service.configureGroupAgent(capabilityGroup.id, { enabled: false, toolsAuthorized: true }), /active or has unmerged work/);
    await assert.rejects(service.deleteGroup(capabilityGroup.id), /active or has unmerged work/);
    taskStore.deleteTask('thread_group_guard_active');
    taskStore.upsertTask({ threadId: 'thread_group_history', title: 'history', initiatorAgent: 'codex', targetAgent: groupAgentId,
      status: 'completed', currentRound: 1, createdAt: new Date().toISOString(), updatedAt: new Date().toISOString(),
      receiverSessionId: 'session_group_history_1', workspacePath: bridgeHome, rounds: [],
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 } } as any);
    const historyRuntime = new (require('../copilot/group-agent-runtime').GroupAgentRuntime)({ bridgeHome, request: async () => ({ text: 'owned history', finishReason: 'stop', toolCalls: [] }) });
    await historyRuntime.run('session_group_history_1', bridgeHome, 'history prompt', undefined, 'developer', { agentId: groupAgentId, threadId: 'thread_group_history' });
    assert.equal(service.listGroupAgentSessions(capabilityGroup.id).length, 1);
    assert.equal(service.listGroupAgentSessions(capabilityGroup.id)[0].role, 'developer');
    assert.equal(service.readGroupAgentSession('session_group_history_1', 0, 1).length, 1, 'session history is read in bounded pages');
    await service.deleteGroup(capabilityGroup.id);
    assert.equal(service.groups.getAll().some((item: any) => item.id === capabilityGroup.id), false);
    taskStore.dispose();

    const disposeToken = new CancellationTokenSource();
    let disposeStarted!: () => void;
    const disposeStartedPromise = new Promise<void>((resolve) => (disposeStarted = resolve));
    const disposeRequestModel = models[0];
    const originalSend = disposeRequestModel.sendRequest;
    disposeRequestModel.sendRequest = async () => {
      disposeStarted();
      return await new Promise<any>(() => {});
    };
    const pendingDispose = service.invoke(group, { id: 'opaque-1', vendor: 'vendor-a' }, 'pending-dispose', disposeToken.token as any);
    await disposeStartedPromise;
    service.dispose();
    await assert.rejects(pendingDispose, /Request cancelled/);
    disposeRequestModel.sendRequest = originalSend;
    for (const disposable of commandDisposables) disposable.dispose();
  } finally {
    Module._load = originalLoad;
    if (originalBridgeHome === undefined) delete process.env.AGENT_MESH_BRIDGE_HOME;
    else process.env.AGENT_MESH_BRIDGE_HOME = originalBridgeHome;
    fs.rmSync(bridgeHome, { recursive: true, force: true });
    await new Promise<void>((resolve, reject) =>
      server.close((error: Error | undefined) => (error ? reject(error) : resolve())),
    );
  }
});
