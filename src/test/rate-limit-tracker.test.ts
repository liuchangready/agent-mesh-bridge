import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { RateLimitTracker } from '../bridge/rate-limit-tracker';
import { AgentDetector } from '../bridge/agent-detector';

describe('RateLimitTracker & Agent Status Tests', () => {
  let tempDir: string;
  let tracker: RateLimitTracker;
  let detector: AgentDetector;

  beforeEach(() => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-rate-limit-test-'));
    tracker = new RateLimitTracker(tempDir);
    detector = new AgentDetector(undefined, tracker);
  });

  afterEach(() => {
    try {
      fs.rmSync(tempDir, { recursive: true, force: true });
    } catch {}
  });

  test('should record exhaustion and calculate recovery time correctly from "retry after" message', () => {
    const errorMsg = 'Error 429: Too Many Requests. Please retry after 120 seconds.';
    const recorded = tracker.recordExhaustion('claude_code', errorMsg);

    assert.strictEqual(recorded.isExhausted, true);
    assert.strictEqual(recorded.reason, '429 Rate Limit Exceeded');
    assert.ok(recorded.remainingSeconds !== undefined && recorded.remainingSeconds > 0 && recorded.remainingSeconds <= 120);
    assert.ok(recorded.resetsAtFormatted?.includes('in 2m') || recorded.resetsAtFormatted?.includes('in 1m'));

    assert.strictEqual(tracker.isExhausted('claude_code'), true);
  });

  test('should record exhaustion and calculate recovery time from "resets at" message', () => {
    const errorMsg = 'Anthropic API limit reached. Your usage limit resets at 23:59.';
    const recorded = tracker.recordExhaustion('claude_code', errorMsg);

    assert.strictEqual(recorded.isExhausted, true);
    assert.ok(recorded.resetsAt);
    assert.ok(recorded.resetsAtFormatted);
  });

  test('should clear exhaustion status manually', () => {
    tracker.recordExhaustion('codex', 'Rate limit exceeded. Retry after 60s.');
    assert.strictEqual(tracker.isExhausted('codex'), true);

    tracker.clearExhaustion('codex');
    assert.strictEqual(tracker.isExhausted('codex'), false);
    assert.strictEqual(tracker.getRateLimitInfo('codex'), undefined);
  });

  test('should reflect exhausted status in AgentDetector probeAgent', async () => {
    tracker.recordExhaustion('antigravity', 'Quota exceeded (429). Resets in 45 minutes.');

    const info = await detector.probeAgent('antigravity', true);
    assert.strictEqual(info.status, 'exhausted');
    assert.ok(info.statusDetails?.includes('配额耗尽 / 限流中'));
    assert.ok(info.rateLimit?.resetsAtFormatted);

    // Clear and re-probe
    tracker.clearExhaustion('antigravity');
    const infoRecovered = await detector.probeAgent('antigravity', true);
    assert.notStrictEqual(infoRecovered.status, 'exhausted');
  });
});
