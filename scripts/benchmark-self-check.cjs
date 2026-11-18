const { test, describe } = require('node:test');
const assert = require('node:assert');
const path = require('node:path');
const fs = require('node:fs');
const os = require('node:os');

const {
  validateLayerMapping,
  parseRunOutput,
  parseOpsLog,
  evaluateCriteria,
  computeFingerprint,
} = require('./run-benchmark.cjs');

describe('Benchmark Self-Check Tests', () => {
  test('parseRunOutput fails closed on non-zero exit code even with missing stats', () => {
    // Simulated crashed run without test reporter summary
    const crashedOutput = 'Fatal error: memory limit exceeded\nSegmentation fault';
    const res = parseRunOutput(crashedOutput, 1, null, 'Process crashed', 1234);

    assert.strictEqual(res.exitCode, 1);
    assert.strictEqual(res.status, 'fail');
    assert.ok(res.fail >= 1, 'Fail count must never be 0 when exitCode is non-zero');
    assert.strictEqual(res.error, 'Process crashed');
  });

  test('parseRunOutput correctly parses passing and failing test runs', () => {
    const passOutput = `
✔ All passed
ℹ tests 680
ℹ suites 68
ℹ pass 680
ℹ fail 0
ℹ cancelled 0
ℹ skipped 0
ℹ todo 0
ℹ duration_ms 45000
    `;
    const passRes = parseRunOutput(passOutput, 0, null, null, 45000);
    assert.strictEqual(passRes.exitCode, 0);
    assert.strictEqual(passRes.status, 'pass');
    assert.strictEqual(passRes.tests, 680);
    assert.strictEqual(passRes.pass, 680);
    assert.strictEqual(passRes.fail, 0);
    assert.strictEqual(passRes.cancelled, 0);

    const failOutput = `
✖ Scenario 22 failed
ℹ tests 680
ℹ suites 68
ℹ pass 678
ℹ fail 2
ℹ cancelled 0
ℹ skipped 0
ℹ todo 0
ℹ duration_ms 48000
    `;
    const failRes = parseRunOutput(failOutput, 1, null, null, 48000);
    assert.strictEqual(failRes.exitCode, 1);
    assert.strictEqual(failRes.status, 'fail');
    assert.strictEqual(failRes.fail, 2);

    const cancelledOutput = `
ℹ tests 10
ℹ pass 9
ℹ fail 0
ℹ cancelled 1
    `;
    const cancelRes = parseRunOutput(cancelledOutput, 0, null, null, 10000);
    assert.strictEqual(cancelRes.status, 'fail', 'Cancelled tests must result in status: fail');

    // Negative case: signal-killed process (exitCode: null, signal: 'SIGTERM')
    const signalRes = parseRunOutput('', null, 'SIGTERM', null, 5000);
    assert.strictEqual(signalRes.exitCode, null);
    assert.strictEqual(signalRes.signal, 'SIGTERM');
    assert.strictEqual(signalRes.status, 'fail');
    assert.ok(signalRes.fail >= 1);

    // Negative case: pass !== tests (tests=680, pass=678, fail=2)
    const mismatchRes = parseRunOutput('ℹ tests 680\nℹ pass 678\nℹ fail 2', 0, null, null, 40000);
    assert.strictEqual(mismatchRes.status, 'fail');
    assert.strictEqual(mismatchRes.fail, 2);

    // Negative case: empty output / missing test counts with exitCode 0
    const emptyRes = parseRunOutput('', 0, null, null, 100);
    assert.strictEqual(emptyRes.status, 'fail', 'Empty output must fail closed');
  });

  test('validateLayerMapping fails closed when unmapped test files exist', () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'layer-check-'));
    try {
      // Create valid mapped file and an unmapped file
      fs.writeFileSync(path.join(tempDir, 'active-agent-resolver.test.js'), '// test');
      fs.writeFileSync(path.join(tempDir, 'totally-untracked-secret.test.js'), '// test');

      const check = validateLayerMapping(tempDir);
      assert.strictEqual(check.valid, false);
      assert.ok(check.unmapped.includes('totally-untracked-secret.test.js'));
    } finally {
      fs.rmSync(tempDir, { recursive: true, force: true });
    }
  });

  test('parseOpsLog distinguishes CAS/write attempts vs actual successes', () => {
    const tempLog = path.join(os.tmpdir(), `ops-test-${Date.now()}.txt`);
    try {
      const data = [
        JSON.stringify({
          git: 10,
          lock_helper: 5,
          taskstore_cas_attempt: 4,
          taskstore_cas_success: 3,
          taskstore_cas_failure: 1,
          taskstore_write_attempt: 8,
          taskstore_write_success: 8,
          taskstore_write_failure: 0,
        }),
        JSON.stringify({
          git: 5,
          lock_helper: 2,
          taskstore_cas_attempt: 2,
          taskstore_cas_success: 2,
          taskstore_cas_failure: 0,
          taskstore_write_attempt: 4,
          taskstore_write_success: 4,
          taskstore_write_failure: 0,
        }),
      ].join('\n');
      fs.writeFileSync(tempLog, data, 'utf8');

      const counts = parseOpsLog(tempLog);
      assert.strictEqual(counts.git, 15);
      assert.strictEqual(counts.lock_helper, 7);
      assert.strictEqual(counts.taskstore_cas_attempt, 6);
      assert.strictEqual(counts.taskstore_cas_success, 5);
      assert.strictEqual(counts.taskstore_cas_failure, 1);
      assert.strictEqual(counts.taskstore_cas, 5); // alias
      assert.strictEqual(counts.taskstore_write_attempt, 12);
      assert.strictEqual(counts.taskstore_write_success, 12);
      assert.strictEqual(counts.taskstore_write, 12); // alias
    } finally {
      try { fs.unlinkSync(tempLog); } catch {}
    }
  });

  test('evaluateCriteria fails closed on any unfulfilled acceptance threshold', () => {
    // Base healthy mock
    const makeMockProgress = (overrides = {}) => ({
      warmup: {
        baseline: { durationMs: 100000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.warmupBaseline || {}) },
        candidate: { durationMs: 60000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.warmupCandidate || {}) },
      },
      runs: {
        A1: { durationMs: 100000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.A1 || {}) },
        B1: { durationMs: 60000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.B1 || {}) },
        A2: { durationMs: 102000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.A2 || {}) },
        B2: { durationMs: 61000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.B2 || {}) },
        A3: { durationMs: 101000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.A3 || {}) },
        B3: { durationMs: 62000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.B3 || {}) },
      },
      layers: {
        policy: { durationMs: 10000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 287, pass: 287, ...(overrides.layerPolicy || {}) },
        boundary: { durationMs: 15000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 200, pass: 200, ...(overrides.layerBoundary || {}) },
        integration: { durationMs: 25000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 193, pass: 193, ...(overrides.layerIntegration || {}) },
      },
      operations: {
        baselineRun: { durationMs: 105000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.opsBaseline || {}) },
        candidateRun: { durationMs: 65000, exitCode: 0, fail: 0, cancelled: 0, status: 'pass', tests: 680, pass: 680, ...(overrides.opsCandidate || {}) },
      },
    });

    const validMapping = { valid: true, unmapped: [] };
    const invalidMapping = { valid: false, unmapped: ['foo.test.js'] };

    // 1. Healthy case: all pass
    const ok = evaluateCriteria(makeMockProgress(), validMapping);
    assert.strictEqual(ok.recommendedForMerge, true);
    assert.strictEqual(ok.allThreePairsFaster.met, true);
    assert.strictEqual(ok.medianDropAtLeast10Percent.met, true);
    assert.strictEqual(ok.medianDropAtLeast30Seconds.met, true);
    assert.strictEqual(ok.allTestsPassWithoutFailures.met, true);

    // 2. Failure: one candidate run slower than baseline pair
    const slowPair = evaluateCriteria(makeMockProgress({ B2: { durationMs: 110000 } }), validMapping);
    assert.strictEqual(slowPair.allThreePairsFaster.met, false);
    assert.strictEqual(slowPair.recommendedForMerge, false);

    // 3. Failure: speedup < 10%
    const smallPercent = evaluateCriteria(makeMockProgress({
      B1: { durationMs: 95000 },
      B2: { durationMs: 95000 },
      B3: { durationMs: 95000 },
    }), validMapping);
    assert.strictEqual(smallPercent.medianDropAtLeast10Percent.met, false);
    assert.strictEqual(smallPercent.recommendedForMerge, false);

    // 4. Failure: any test failure in any run
    const failedRun = evaluateCriteria(makeMockProgress({
      B3: { exitCode: 1, fail: 1, status: 'fail' },
    }), validMapping);
    assert.strictEqual(failedRun.allTestsPassWithoutFailures.met, false);
    assert.strictEqual(failedRun.recommendedForMerge, false);

    // 5. Failure: unmapped files
    const unmappedFail = evaluateCriteria(makeMockProgress(), invalidMapping);
    assert.strictEqual(unmappedFail.zeroUnmappedFiles.met, false);
    assert.strictEqual(unmappedFail.recommendedForMerge, false);

    // 6. Failure: warmup run failed
    const failedWarmup = evaluateCriteria(makeMockProgress({
      warmupBaseline: { exitCode: 1, fail: 1, status: 'fail' },
    }), validMapping);
    assert.strictEqual(failedWarmup.allTestsPassWithoutFailures.met, false);
    assert.strictEqual(failedWarmup.recommendedForMerge, false);

    // 7. Failure: profiling candidate run failed (e.g. exitCode: 1 or fail: 2)
    const failedProfiling = evaluateCriteria(makeMockProgress({
      opsCandidate: { exitCode: 1, fail: 2, status: 'fail' },
    }), validMapping);
    assert.strictEqual(failedProfiling.allTestsPassWithoutFailures.met, false);
    assert.strictEqual(failedProfiling.recommendedForMerge, false);

    // 8. Failure: layer run failed
    const failedLayer = evaluateCriteria(makeMockProgress({
      layerIntegration: { exitCode: 1, fail: 1, status: 'fail' },
    }), validMapping);
    assert.strictEqual(failedLayer.allTestsPassWithoutFailures.met, false);
    assert.strictEqual(failedLayer.recommendedForMerge, false);
  });

  test('computeFingerprint generates reproducible composite hash', () => {
    const fp1 = computeFingerprint();
    const fp2 = computeFingerprint();
    assert.strictEqual(fp1.fingerprintId, fp2.fingerprintId);
    assert.ok(fp1.fingerprintId.length > 0);
  });
});
