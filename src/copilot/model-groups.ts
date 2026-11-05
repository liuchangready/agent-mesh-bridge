import { createHash, randomUUID } from 'crypto';
import * as vscode from 'vscode';
import { validateCompatibleBaseUrl } from './compatible-model-client';

export type HostModelRef = { id: string; vendor: string };
export type CopilotModelGroup = {
  id: string;
  name: string;
} & (
  | { source: 'vscode-lm'; vendor?: string; models: HostModelRef[] }
  | { source: 'openai-compatible'; baseUrl: string; models: Array<{ id: string; name?: string }> }
);

const STATE_KEY = 'copilot.modelGroups.v1';
const MAX_GROUPS = 50;
const MAX_MODELS = 200;
const MAX_NAME = 100;

export function parseJsonc(text: string): unknown {
  if (Buffer.byteLength(text, 'utf8') > 1024 * 1024) throw new Error('File exceeds 1 MB.');
  let output = '';
  let quote = false;
  let escape = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quote) {
      output += char;
      if (escape) escape = false;
      else if (char === '\\') escape = true;
      else if (char === '"') quote = false;
      continue;
    }
    if (char === '"') {
      quote = true;
      output += char;
      continue;
    }
    if (char === '/' && text[i + 1] === '/') {
      i += 2;
      while (i < text.length && text[i] !== '\n' && text[i] !== '\r') i++;
      output += '\n';
      continue;
    }
    if (char === '/' && text[i + 1] === '*') {
      const end = text.indexOf('*/', i + 2);
      if (end < 0) throw new Error('Unterminated JSONC comment.');
      output += ' ';
      i = end + 1;
      continue;
    }
    if (char === ',') {
      let next = i + 1;
      while (next < text.length) {
        if (/\s/.test(text[next])) {
          next++;
          continue;
        }
        if (text.startsWith('//', next)) {
          next = text.indexOf('\n', next + 2);
          if (next < 0) {
            next = text.length;
            break;
          }
          continue;
        }
        if (text.startsWith('/*', next)) {
          const end = text.indexOf('*/', next + 2);
          if (end < 0) throw new Error('Unterminated JSONC comment.');
          next = end + 2;
          continue;
        }
        break;
      }
      if (text[next] === '}' || text[next] === ']') continue;
    }
    output += char;
  }
  try {
    return JSON.parse(output);
  } catch {
    throw new Error('File is not valid JSONC.');
  }
}

function cleanName(name: string): string {
  const result = name.trim();
  if (!result || result.length > MAX_NAME)
    throw new Error(`Group name must be 1–${MAX_NAME} characters.`);
  return result;
}

export function validateGroup(value: unknown): CopilotModelGroup {
  if (!value || typeof value !== 'object') throw new Error('Invalid model group.');
  const input = value as any;
  if (typeof input.id !== 'string' || !/^[\w-]{1,80}$/.test(input.id))
    throw new Error('Invalid group id.');
  const name = cleanName(typeof input.name === 'string' ? input.name : '');
  if (!Array.isArray(input.models) || input.models.length > MAX_MODELS)
    throw new Error(`A group can contain at most ${MAX_MODELS} models.`);
  if (input.source === 'vscode-lm') {
    const models = input.models.map((model: any) => {
      if (
        typeof model?.id !== 'string' ||
        !model.id ||
        model.id.length > 256 ||
        typeof model?.vendor !== 'string' ||
        !model.vendor ||
        model.vendor.length > 100
      )
        throw new Error('Invalid host model reference.');
      return { id: model.id, vendor: model.vendor };
    });
    return {
      id: input.id,
      name,
      source: 'vscode-lm',
      ...(typeof input.vendor === 'string' ? { vendor: input.vendor.slice(0, 100) } : {}),
      models,
    };
  }
  if (input.source === 'openai-compatible') {
    if (typeof input.baseUrl !== 'string') throw new Error('Invalid API base URL.');
    const baseUrl = validateCompatibleBaseUrl(input.baseUrl).toString().replace(/\/$/, '');
    const models = input.models.map((model: any) => {
      if (typeof model?.id !== 'string' || !model.id.trim() || model.id.length > 256)
        throw new Error('Invalid model id.');
      return {
        id: model.id.trim(),
        ...(typeof model.name === 'string' && model.name.length <= MAX_NAME
          ? { name: model.name }
          : {}),
      };
    });
    return { id: input.id, name, source: 'openai-compatible', baseUrl, models };
  }
  throw new Error('Unknown model group source.');
}

export class CopilotModelGroups {
  constructor(
    private readonly state: vscode.Memento,
    private readonly secrets: vscode.SecretStorage,
  ) {}

  getAll(): CopilotModelGroup[] {
    const raw = this.state.get<unknown>(STATE_KEY);
    if (raw === undefined) return [];
    if (
      !raw ||
      typeof raw !== 'object' ||
      (raw as any).version !== 1 ||
      !Array.isArray((raw as any).groups)
    ) {
      throw new Error(
        'Stored model groups have an unsupported format; existing data was preserved.',
      );
    }
    if ((raw as any).groups.length > MAX_GROUPS)
      throw new Error(
        'Stored model groups exceed the supported limit; existing data was preserved.',
      );
    const groups = (raw as any).groups.map(validateGroup);
    if (new Set(groups.map((group: CopilotModelGroup) => group.id)).size !== groups.length)
      throw new Error('Stored model groups contain duplicate ids; existing data was preserved.');
    return groups;
  }

  async save(value: unknown): Promise<CopilotModelGroup> {
    const group = validateGroup(value);
    const groups = this.getAll();
    const index = groups.findIndex((item) => item.id === group.id);
    if (index < 0 && groups.length >= MAX_GROUPS)
      throw new Error(`You can create at most ${MAX_GROUPS} groups.`);
    const previous = index < 0 ? undefined : groups[index];
    if (index < 0) groups.push(group);
    else groups[index] = group;
    await this.state.update(STATE_KEY, { version: 1, groups });
    if (
      index >= 0 &&
      groups[index].source === 'openai-compatible' &&
      group.source === 'openai-compatible' &&
      previous?.source === 'openai-compatible' &&
      new URL(previous.baseUrl).origin !== new URL(group.baseUrl).origin
    ) {
      try {
        await this.secrets.delete(this.secretKey(previous));
      } catch {
        throw new Error(
          'The group was updated, but the previous API key could not be removed from SecretStorage.',
        );
      }
    }
    return group;
  }

  async delete(id: string): Promise<void> {
    const groups = this.getAll();
    const group = groups.find((item) => item.id === id);
    if (!group) return;
    await this.state.update(STATE_KEY, {
      version: 1,
      groups: groups.filter((item) => item.id !== id),
    });
    if (group.source === 'openai-compatible') {
      try {
        await this.secrets.delete(this.secretKey(group));
      } catch {
        throw new Error(
          'The group was deleted, but its API key could not be removed from SecretStorage.',
        );
      }
    }
  }

  async setApiKey(
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
    value: string,
  ): Promise<void> {
    if (!value.trim() || Buffer.byteLength(value, 'utf8') > 4096 || /[\r\n\0]/.test(value))
      throw new Error('API key must be a non-empty single line under 4 KB.');
    try {
      await this.secrets.store(this.secretKey(group), value);
    } catch {
      throw new Error('VS Code SecretStorage could not save the API key.');
    }
  }

  async getApiKey(
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
  ): Promise<string | undefined> {
    try {
      return await this.secrets.get(this.secretKey(group));
    } catch {
      throw new Error('VS Code SecretStorage could not read the API key.');
    }
  }

  async deleteApiKey(
    group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>,
  ): Promise<void> {
    try {
      await this.secrets.delete(this.secretKey(group));
    } catch {
      throw new Error('VS Code SecretStorage could not delete the API key.');
    }
  }

  secretKey(group: Extract<CopilotModelGroup, { source: 'openai-compatible' }>): string {
    const origin = new URL(group.baseUrl).origin;
    const digest = createHash('sha256').update(origin).digest('hex').slice(0, 20);
    return `copilot.model-group.${group.id}.${digest}`;
  }
}

export function newModelGroupId(): string {
  return randomUUID();
}

export function importGroupNames(value: unknown): Array<{ name: string; vendor: string }> {
  if (!Array.isArray(value)) throw new Error('Expected a top-level array of model groups.');
  const names: Array<{ name: string; vendor: string }> = [];
  for (const item of value) {
    if (!item || typeof item !== 'object') continue;
    const name = typeof (item as any).name === 'string' ? (item as any).name.trim() : '';
    const vendor = typeof (item as any).vendor === 'string' ? (item as any).vendor.trim() : '';
    if (name && name.length <= MAX_NAME && vendor && vendor.length <= 100)
      names.push({ name, vendor });
  }
  return names;
}
