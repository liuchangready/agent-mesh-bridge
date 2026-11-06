import * as assert from 'node:assert/strict';
import { test } from 'node:test';
import { assertCanExecute } from '../bridge/agent-capabilities';

test('unknown historical agent IDs are refused at execution boundaries', () => {
  assert.throws(() => assertCanExecute('retired_agent:old', 'run this task'), /no longer supported/);
});
