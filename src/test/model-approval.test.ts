import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { annotateConfiguredModel, saveModelDefaults } from '../bridge/model-catalog';
import {
  DEFAULT_MODEL_APPROVAL_TIMEOUT_MS,
  MODEL_APPROVAL_DENIED_CODE,
  MODEL_APPROVAL_TIMEOUT_CODE,
  ModelApprovalDecision,
  ModelApprovalRequest,
  getModelApprovalDir,
  listPendingModelApprovals,
  resolveDispatchModel,
  submitModelApprovalDecision,
  writeModelApprovalRequest,
} from '../bridge/model-approval';
import { ModelOption } from '../bridge/model-catalog';

const CONFIGURED = 'deepseek/deepseek-v4.1-flash';
const OTHER = 'deepseek/deepseek-v4-pro';

function makeHome(withDefault: boolean = true): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-model-approval-'));
  if (withDefault) saveModelDefaults({ deepseek_harness: CONFIGURED }, dir);
  return dir;
}

function cleanup(dir: string): void {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {}
}

function decision(status: ModelApprovalDecision['status'], extra: Partial<ModelApprovalDecision> = {}) {
  return async (request: ModelApprovalRequest): Promise<ModelApprovalDecision> => ({
    id: request.id,
    status,
    at: new Date().toISOString(),
    by: 'user',
    ...extra,
  });
}

describe('annotateConfiguredModel', () => {
  test('hoists the configured default to the front and tags it in place', () => {
    const models: ModelOption[] = [
      { id: OTHER },
      { id: 'gpt-6-sol' },
      { id: CONFIGURED, label: 'Flash' },
    ];
    const out = annotateConfiguredModel(models, CONFIGURED);

    assert.strictEqual(out[0].id, CONFIGURED);
    assert.match(out[0].hint || '', /configured default/);
    assert.strictEqual(out[0].label, 'Flash');
    // Remaining order is preserved and the original array is untouched.
    assert.deepStrictEqual(
      out.slice(1).map((m) => m.id),
      [OTHER, 'gpt-6-sol'],
    );
    assert.strictEqual(models[0].id, OTHER);
  });

  test('keeps an existing hint when tagging', () => {
    const out = annotateConfiguredModel([{ id: CONFIGURED, hint: 'hidden' }], CONFIGURED);
    assert.strictEqual(out[0].hint, 'hidden · configured default');
  });

  test('prepends a synthetic entry when the probe did not return the default', () => {
    const out = annotateConfiguredModel([{ id: OTHER }], CONFIGURED);
    assert.strictEqual(out[0].id, CONFIGURED);
    assert.match(out[0].hint || '', /not returned by the probe/);
    assert.strictEqual(out.length, 2);
  });

  test('leaves the list alone when no default is configured', () => {
    const models: ModelOption[] = [{ id: OTHER }, { id: CONFIGURED }];
    const out = annotateConfiguredModel(models, undefined);
    assert.deepStrictEqual(
      out.map((m) => m.id),
      [OTHER, CONFIGURED],
    );
    assert.strictEqual(out[0].hint, undefined);
  });
});

describe('resolveDispatchModel', () => {
  test('no requested model -> the configured default', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({ targetAgent: 'deepseek_harness', dir });
      assert.strictEqual(res.model, CONFIGURED);
      assert.strictEqual(res.notice, undefined);
      assert.strictEqual(res.approval, undefined);
    } finally {
      cleanup(dir);
    }
  });

  test('requested model equal to the default needs no approval', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: CONFIGURED,
        requestedByUser: true,
        dir,
        handler: async () => {
          throw new Error('the handler must not be called for the configured default');
        },
      });
      assert.strictEqual(res.model, CONFIGURED);
      assert.strictEqual(res.approval, undefined);
    } finally {
      cleanup(dir);
    }
  });

  test('a guessed non-default model is ignored in favour of the default', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        dir,
        handler: async () => {
          throw new Error('a guessed model must not reach the user');
        },
      });
      assert.strictEqual(res.model, CONFIGURED);
      assert.match(res.notice || '', /ignored/);
      assert.match(res.notice || '', /modelRequestedByUser: true/);
    } finally {
      cleanup(dir);
    }
  });

  test('an approved user request switches the model and is recorded', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        dir,
        requestId: 'req-approved',
        handler: decision('approved'),
      });
      assert.strictEqual(res.model, OTHER);
      assert.strictEqual(res.defaultModel, CONFIGURED);
      assert.strictEqual(res.approval?.status, 'approved');
      assert.strictEqual(res.approval?.requestedModel, OTHER);
    } finally {
      cleanup(dir);
    }
  });

  test('"use default" keeps the configured default with a notice', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        dir,
        handler: decision('use_default'),
      });
      assert.strictEqual(res.model, CONFIGURED);
      assert.strictEqual(res.approval?.status, 'use_default');
      assert.match(res.notice || '', /kept the default/);
    } finally {
      cleanup(dir);
    }
  });

  test('a cancelled confirmation fails the dispatch closed', async () => {
    const dir = makeHome();
    try {
      await assert.rejects(
        () =>
          resolveDispatchModel({
            targetAgent: 'deepseek_harness',
            requestedModel: OTHER,
            requestedByUser: true,
            dir,
            handler: decision('cancelled'),
          }),
        (err: any) => err?.code === MODEL_APPROVAL_DENIED_CODE,
      );
    } finally {
      cleanup(dir);
    }
  });

  test('an unanswered confirmation fails the dispatch closed', async () => {
    const dir = makeHome();
    try {
      await assert.rejects(
        () =>
          resolveDispatchModel({
            targetAgent: 'deepseek_harness',
            requestedModel: OTHER,
            requestedByUser: true,
            dir,
            handler: decision('timeout'),
          }),
        (err: any) => err?.code === MODEL_APPROVAL_TIMEOUT_CODE,
      );
    } finally {
      cleanup(dir);
    }
  });

  test('an unavailable confirmation channel falls back to the default', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        dir,
        handler: decision('timeout', { channelError: true, by: 'bridge' }),
      });
      assert.strictEqual(res.model, CONFIGURED);
      assert.match(res.notice || '', /could not be confirmed/);
    } finally {
      cleanup(dir);
    }
  });

  test('the approval policy can be turned off for a declared user request', async () => {
    const dir = makeHome();
    try {
      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        dir,
        requireApproval: false,
        handler: async () => {
          throw new Error('the handler must not be called with the policy off');
        },
      });
      assert.strictEqual(res.model, OTHER);
      assert.strictEqual(res.approval, undefined);
    } finally {
      cleanup(dir);
    }
  });
});

describe('model approval file channel', () => {
  test('an answered request resolves through the request/decision files', async () => {
    const dir = makeHome();
    try {
      const answering = (async () => {
        // Wait for the request file, then answer it like the extension host does.
        const deadline = Date.now() + 3000;
        let pending: ModelApprovalRequest[] = [];
        while (Date.now() < deadline) {
          pending = listPendingModelApprovals(dir);
          if (pending.length > 0) break;
          await new Promise((r) => setTimeout(r, 10));
        }
        assert.strictEqual(pending.length, 1);
        assert.strictEqual(pending[0].targetAgent, 'deepseek_harness');
        assert.strictEqual(pending[0].requestedModel, OTHER);
        assert.strictEqual(pending[0].defaultModel, CONFIGURED);
        assert.ok(new Date(pending[0].expiresAt).getTime() > Date.now());
        submitModelApprovalDecision(
          { id: pending[0].id, status: 'approved', at: new Date().toISOString(), by: 'user' },
          dir,
        );
      })();

      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        initiatorAgent: 'codex',
        title: '统一码牌千术',
        dir,
        pollMs: 10,
        timeoutMs: 5000,
      });
      await answering;

      assert.strictEqual(res.model, OTHER);
      assert.strictEqual(res.approval?.status, 'approved');
      // The channel cleans up after itself: no request/decision files remain.
      assert.deepStrictEqual(fs.readdirSync(getModelApprovalDir(dir)), []);
    } finally {
      cleanup(dir);
    }
  });

  test('an unwritable channel degrades to the configured default', async () => {
    const dir = makeHome();
    try {
      // Occupy the channel path with a file so the request cannot be published.
      fs.writeFileSync(getModelApprovalDir(dir), 'not a directory', 'utf8');

      const res = await resolveDispatchModel({
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        requestedByUser: true,
        dir,
        pollMs: 10,
        timeoutMs: 200,
      });
      assert.strictEqual(res.model, CONFIGURED);
      assert.match(res.notice || '', /channel is unavailable/);
    } finally {
      cleanup(dir);
    }
  });

  test('expired requests are never handed to the user', () => {
    const dir = makeHome();
    try {
      const stale: ModelApprovalRequest = {
        id: 'stale-1',
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        at: new Date(Date.now() - 60_000).toISOString(),
        expiresAt: new Date(Date.now() - 30_000).toISOString(),
      };
      const fresh: ModelApprovalRequest = {
        id: 'fresh-1',
        targetAgent: 'deepseek_harness',
        requestedModel: OTHER,
        at: new Date().toISOString(),
        expiresAt: new Date(Date.now() + DEFAULT_MODEL_APPROVAL_TIMEOUT_MS).toISOString(),
      };
      assert.strictEqual(writeModelApprovalRequest(stale, dir), true);
      assert.strictEqual(writeModelApprovalRequest(fresh, dir), true);

      const pending = listPendingModelApprovals(dir);
      assert.deepStrictEqual(
        pending.map((r) => r.id),
        ['fresh-1'],
      );
      // The stale request file is dropped instead of piling up unanswered.
      assert.deepStrictEqual(fs.readdirSync(getModelApprovalDir(dir)), ['fresh-1.request.json']);
    } finally {
      cleanup(dir);
    }
  });
});
