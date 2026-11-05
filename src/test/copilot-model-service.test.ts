import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import { AddressInfo } from 'node:net';
const Module = require('module');
const http = require('node:http');
const net = require('node:net');

test('service refreshes only on request and resolves a host model by exact vendor and id', async () => {
  let selected = 0;
  let calls = 0;
  let provider: any;
  let shownResult = '';
  let apiAuthorization = '';
  const registeredCommands = new Map<string, (...args: any[]) => any>();
  const listeners = new Set<() => void>();
  class EventEmitter<T> {
    event = (listener: () => void) => {
      listeners.add(listener);
      return { dispose: () => listeners.delete(listener) };
    };
    fire() {
      for (const listener of listeners) listener();
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
  const models = [
    {
      id: 'opaque-1',
      vendor: 'vendor-a',
      name: 'Same',
      sendRequest: async (messages: any[]) => {
        calls++;
        assert.deepEqual(messages, [{ role: 'user', content: 'question' }]);
        return {
          text: (async function* () {
            yield 'exact answer';
          })(),
        };
      },
    },
    {
      id: 'opaque-1',
      vendor: 'vendor-b',
      name: 'Same',
      sendRequest: async () => {
        throw new Error('wrong vendor called');
      },
    },
  ];
  const fakeVscode = {
    EventEmitter,
    CancellationTokenSource,
    lm: {
      onDidChangeChatModels: () => ({ dispose() {} }),
      selectChatModels: async () => {
        selected++;
        return models;
      },
    },
    workspace: {
      isTrusted: true,
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
        items.find((item) => item.source === 'openai-compatible'),
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
        throw new Error(message);
      },
    },
  };
  const server = http.createServer((request: any, response: any) => {
    apiAuthorization = request.headers.authorization;
    response.end(JSON.stringify({ choices: [{ message: { content: 'integrated answer' } }] }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address() as AddressInfo;
  const baseUrl = `http://127.0.0.1:${address.port}/v1`;
  const originalLoad = Module._load;
  Module._load = function (request: string, parent: unknown, isMain: boolean) {
    return request === 'vscode' ? fakeVscode : originalLoad.call(this, request, parent, isMain);
  };
  try {
    const { CopilotModelService } = require('../copilot/model-service');
    const state = new Map<string, unknown>();
    const secrets = new Map<string, string>();
    const context = {
      globalState: {
        get: (key: string) => state.get(key),
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
      id: 'group',
      name: 'Host group',
      source: 'vscode-lm',
      models: [{ id: 'opaque-1', vendor: 'vendor-a' }],
    });
    const token = {
      isCancellationRequested: false,
      onCancellationRequested: () => ({ dispose() {} }),
    } as any;
    const result = await service.invoke(group, 'opaque-1', 'question', token);
    assert.deepEqual(result.text, 'exact answer');
    assert.equal(calls, 1);
    assert.match(service.getLastCall(), /Succeeded/);
    const { registerCopilotModelCommands } = require('../copilot/model-commands');
    const commandDisposables = registerCopilotModelCommands(context as any, service);
    await registeredCommands.get('agent-mesh-bridge.addCopilotModelGroup')!();
    const apiGroup = service.groups.getAll().find((item: any) => item.name === 'Local API');
    assert.ok(apiGroup);
    await registeredCommands.get('agent-mesh-bridge.callCopilotModel')!(apiGroup, 'remote-model');
    assert.equal(apiAuthorization, 'Bearer integration-secret');
    assert.match(shownResult, /integrated answer/);
    assert.match(shownResult, /remote-model/);
    assert.equal(shownResult.includes('integration-secret'), false);
    for (const disposable of commandDisposables) disposable.dispose();
    service.dispose();
  } finally {
    Module._load = originalLoad;
    await new Promise<void>((resolve, reject) =>
      server.close((error: Error | undefined) => (error ? reject(error) : resolve())),
    );
  }
});
