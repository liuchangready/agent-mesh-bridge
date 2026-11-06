import * as assert from 'node:assert/strict';
import { test } from 'node:test';
import { ActiveAgentResolver, applyConfiguredAgentDefault } from '../bridge/active-agent-resolver';
import { AgentDetector } from '../bridge/agent-detector';

test('a removed persisted default is rejected without preventing startup routing', async () => {
  const persistedSettings = { get: (_key: string, _fallback: string) => 'copilot_group:previous-user-default' };
  const migrations: Array<string | undefined> = [];
  const warnings: Array<string | undefined> = [];
  const resolver = new ActiveAgentResolver(new AgentDetector());

  const applied = applyConfiguredAgentDefault(
    resolver,
    persistedSettings.get('defaultAgent', 'auto'),
    (selected) => {
      migrations.push('auto');
      warnings.push(selected);
    },
  );

  assert.strictEqual(applied, false);
  assert.deepStrictEqual(migrations, ['auto']);
  assert.deepStrictEqual(warnings, ['copilot_group:previous-user-default']);
  assert.strictEqual(resolver.getManualOverride(), null);
  const resolved = await resolver.resolveActiveAgent();
  assert.ok(['antigravity', 'codex', 'claude_code', 'deepseek_harness'].includes(resolved.agent));
});
