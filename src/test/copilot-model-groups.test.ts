import { test } from 'node:test';
import * as assert from 'node:assert/strict';
import {
  CopilotModelGroups,
  importGroupNames,
  parseJsonc,
  validateGroup,
} from '../copilot/model-groups';

test('JSONC import projects only name and vendor and preserves comment-like string contents', () => {
  const parsed = parseJsonc(`[
    // group list
    { "name": "My // OpenAI", "vendor": "openai", "apiKey": "must not persist", },
  ]`);
  assert.deepEqual(importGroupNames(parsed), [{ name: 'My // OpenAI', vendor: 'openai' }]);
  assert.deepEqual(
    importGroupNames(
      parseJsonc('[{"name":"X", /* comment */ "vendor":"v", // trailing comment\n}]'),
    ),
    [{ name: 'X', vendor: 'v' }],
  );
  assert.throws(() => parseJsonc('[/* unfinished'), /Unterminated/);
  assert.throws(() => importGroupNames(parseJsonc('{"x":1}')), /top-level array/);
});

test('group validation preserves exact opaque host ids and rejects unsafe endpoint URLs', () => {
  const group = validateGroup({
    id: 'stable-id',
    name: ' Group ',
    source: 'vscode-lm',
    vendor: 'openai',
    models: [{ vendor: 'openai', id: 'opaque/id=one' }],
  });
  assert.deepEqual(group, {
    id: 'stable-id',
    name: 'Group',
    source: 'vscode-lm',
    vendor: 'openai',
    models: [{ vendor: 'openai', id: 'opaque/id=one' }],
  });
  assert.throws(() =>
    validateGroup({
      id: 'bad',
      name: 'x',
      source: 'openai-compatible',
      baseUrl: 'https://user:secret@example.com/v1',
      models: [],
    }),
  );
  assert.throws(() =>
    validateGroup({
      id: 'bad',
      name: 'x',
      source: 'openai-compatible',
      baseUrl: 'http://example.com',
      models: [],
    }),
  );
});

test('SecretStorage keys are origin-bound, replacement persists only in secrets, and origin change drops the old key', async () => {
  const values = new Map<string, unknown>();
  const secrets = new Map<string, string>();
  const state = {
    get: (key: string) => values.get(key),
    update: async (key: string, value: unknown) => {
      values.set(key, value);
    },
  } as any;
  const secretStorage = {
    get: async (key: string) => secrets.get(key),
    store: async (key: string, value: string) => {
      secrets.set(key, value);
    },
    delete: async (key: string) => {
      secrets.delete(key);
    },
  } as any;
  const store = new CopilotModelGroups(state, secretStorage);
  const initial = await store.save({
    id: 'endpoint',
    name: 'Mine',
    source: 'openai-compatible',
    baseUrl: 'https://api.example/v1',
    models: [{ id: 'm1' }],
  });
  if (initial.source !== 'openai-compatible') throw new Error('narrowing failed');
  await store.setApiKey(initial, 'private-secret');
  assert.equal(await store.getApiKey(initial), 'private-secret');
  assert.equal(JSON.stringify(values).includes('private-secret'), false);
  await store.save({ ...initial, name: 'Renamed' });
  assert.equal(await store.getApiKey(initial), 'private-secret');
  const changed = await store.save({ ...initial, baseUrl: 'https://other.example/v1' });
  assert.equal(changed.source, 'openai-compatible');
  if (changed.source !== 'openai-compatible') throw new Error('narrowing failed');
  assert.equal(await store.getApiKey(changed), undefined);
  assert.equal(secrets.size, 0);
  await store.delete(changed.id);
  assert.deepEqual(store.getAll(), []);
  secretStorage.store = async (_key: string, value: string) => {
    throw new Error(`secret leaked by provider: ${value}`);
  };
  await assert.rejects(store.setApiKey(initial, 'must-not-appear'), (error: Error) => {
    assert.equal(error.message.includes('must-not-appear'), false);
    return /could not save/.test(error.message);
  });
});
