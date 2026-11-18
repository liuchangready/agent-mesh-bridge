const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const cp = require('node:child_process');
const crypto = require('node:crypto');

const WORKTREE_ROOT = path.resolve(__dirname, '..');
const BASELINE_ROOT = path.resolve(WORKTREE_ROOT, '../baseline_worktree');
const RESULTS_DIR = path.resolve(WORKTREE_ROOT, 'benchmark-results');
const PROGRESS_FILE = path.resolve(RESULTS_DIR, 'progress.json');

fs.mkdirSync(RESULTS_DIR, { recursive: true });

function getCleanEnv(extraEnv = {}) {
  const env = { ...process.env, ...extraEnv };
  delete env.MESH_TASK_RECEIVER;
  delete env.MESH_TASK_THREAD_ID;
  delete env.MESH_TASK_ROUND;
  delete env.MESH_REVIEW_BOOTSTRAP;
  delete env.MESH_REVIEW_THREAD_ID;
  delete env.MESH_REVIEW_BOOTSTRAP_TOKEN;
  delete env.MESH_CALLER_AGENT;
  env.PATH = 'D:\\node;' + (env.PATH || '');
  return env;
}

function hashString(str) {
  return crypto.createHash('sha256').update(str).digest('hex').substring(0, 16);
}

function hashFile(filePath) {
  if (!fs.existsSync(filePath)) return 'missing';
  try {
    return hashString(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return 'error';
  }
}

function getGitCommit(dir) {
  try {
    return cp.execSync('git rev-parse HEAD', { cwd: dir, env: getCleanEnv() }).toString('utf8').trim();
  } catch {
    return 'unknown';
  }
}

function getGitDiffHash(dir) {
  try {
    const status = cp.execSync('git status --porcelain', { cwd: dir, env: getCleanEnv() }).toString('utf8');
    const diff = cp.execSync('git diff HEAD', { cwd: dir, env: getCleanEnv() }).toString('utf8');
    return hashString(status + '\n' + diff);
  } catch {
    return 'unknown';
  }
}

function computeFingerprint() {
  const baselineCommit = getGitCommit(BASELINE_ROOT);
  const candidateCommit = getGitCommit(WORKTREE_ROOT);
  const candidateDiff = getGitDiffHash(WORKTREE_ROOT);
  const probeHash = hashFile(path.resolve(WORKTREE_ROOT, 'scripts/profile-preload.cjs'));
  const testScriptHash = hashFile(path.resolve(WORKTREE_ROOT, 'scripts/test.cjs'));
  const benchmarkScriptHash = hashFile(path.resolve(WORKTREE_ROOT, 'scripts/run-benchmark.cjs'));
  const layeringConfigHash = hashFile(path.resolve(WORKTREE_ROOT, 'test-layering.json'));

  const composite = [
    baselineCommit,
    candidateCommit,
    candidateDiff,
    probeHash,
    testScriptHash,
    benchmarkScriptHash,
    layeringConfigHash,
  ].join(':');

  return {
    fingerprintId: hashString(composite),
    baselineCommit,
    candidateCommit,
    candidateDiff,
    probeHash,
    testScriptHash,
    benchmarkScriptHash,
    layeringConfigHash,
  };
}

function getSystemMetadata() {
  let gitVersion = 'unknown';
  try {
    gitVersion = cp.execSync('git --version', { env: getCleanEnv() }).toString('utf8').trim();
  } catch {}

  let psVersion = 'unknown';
  try {
    psVersion = cp.execSync('powershell -NoProfile -Command "$PSVersionTable.PSVersion.ToString()"', { env: getCleanEnv() }).toString('utf8').trim();
  } catch {}

  const cpus = os.cpus();
  return {
    timestamp: new Date().toISOString(),
    nodeVersion: process.version,
    platform: `${os.type()} ${os.release()} (${os.arch()})`,
    cpuModel: cpus.length ? cpus[0].model : 'unknown',
    cpuCores: cpus.length,
    totalMemoryGb: Number((os.totalmem() / (1024 ** 3)).toFixed(2)),
    freeMemoryGb: Number((os.freemem() / (1024 ** 3)).toFixed(2)),
    gitVersion,
    powershellVersion: psVersion,
    loadAverage: os.loadavg ? os.loadavg() : [],
    concurrencyLimit: 4,
  };
}

function validateLayerMapping(outTestDir = path.resolve(WORKTREE_ROOT, 'out/test')) {
  if (!fs.existsSync(outTestDir)) {
    return { valid: false, reason: 'out/test directory does not exist' };
  }
  const layeringFile = path.resolve(WORKTREE_ROOT, 'test-layering.json');
  let layeringConfig = null;
  if (fs.existsSync(layeringFile)) {
    try {
      layeringConfig = JSON.parse(fs.readFileSync(layeringFile, 'utf8'));
    } catch (err) {
      return { valid: false, reason: `Failed to parse test-layering.json: ${err.message}` };
    }
  }

  // Also check mapping in scripts/test.cjs
  const testCjsContent = fs.readFileSync(path.resolve(WORKTREE_ROOT, 'scripts/test.cjs'), 'utf8');
  const policyMatches = new Set(Array.from(testCjsContent.matchAll(/'([^']+\.test\.js)'/g)).map(m => m[1]));

  const discovered = fs.readdirSync(outTestDir, { recursive: true })
    .filter(f => f.endsWith('.test.js'))
    .map(f => path.basename(f));

  const unmapped = [];
  for (const file of discovered) {
    const inCjs = policyMatches.has(file);
    let inJson = false;
    if (layeringConfig?.layers) {
      inJson = Object.values(layeringConfig.layers).some(arr => Array.isArray(arr) && arr.includes(file));
    }
    if (!inCjs && !inJson) {
      unmapped.push(file);
    }
  }

  if (unmapped.length > 0) {
    return { valid: false, unmapped, reason: `Unmapped test file(s): ${unmapped.join(', ')}` };
  }
  return { valid: true, discoveredCount: discovered.length, unmapped: [] };
}

function parseSuiteTimings(output) {
  const suites = [];
  const regex = /✔\s+([^(\n\r]+)\s+\((\d+(?:\.\d+)?)(ms|s)\)/g;
  let match;
  while ((match = regex.exec(output)) !== null) {
    const name = match[1].trim();
    let durationMs = parseFloat(match[2]);
    if (match[3] === 's') durationMs *= 1000;
    suites.push({ name, durationMs });
  }
  return suites;
}

function parseRunOutput(output, exitCode, signal = null, error = null, durationMs = 0) {
  const testsMatch = output.match(/ℹ\s+tests\s+(\d+)/);
  const passMatch = output.match(/ℹ\s+pass\s+(\d+)/);
  const failMatch = output.match(/ℹ\s+fail\s+(\d+)/);
  const cancelledMatch = output.match(/ℹ\s+cancelled\s+(\d+)/);
  const skipMatch = output.match(/ℹ\s+skipped\s+(\d+)/);
  const todoMatch = output.match(/ℹ\s+todo\s+(\d+)/);

  let tests = testsMatch ? parseInt(testsMatch[1], 10) : null;
  let pass = passMatch ? parseInt(passMatch[1], 10) : null;
  let fail = failMatch ? parseInt(failMatch[1], 10) : null;
  const cancelled = cancelledMatch ? parseInt(cancelledMatch[1], 10) : 0;
  const skip = skipMatch ? parseInt(skipMatch[1], 10) : 0;
  const todo = todoMatch ? parseInt(todoMatch[1], 10) : 0;

  // Process termination checks:
  const isKilledBySignal = signal !== null && signal !== undefined;
  const isProcessError = error !== null && error !== undefined;
  const isNonZeroExit = exitCode !== null && exitCode !== undefined && exitCode !== 0;
  const isMissingExit = exitCode === null || exitCode === undefined;

  // Fail-closed enforcement:
  if (isKilledBySignal || isProcessError || isNonZeroExit || isMissingExit) {
    if (fail === null || fail === 0) {
      fail = 1;
    }
    if (tests === null) {
      tests = (pass || 0) + fail;
    }
  }

  // Exact success condition:
  // Must have exitCode === 0, NO signal, NO error, fail === 0, cancelled === 0, tests > 0, and pass === tests
  const isSuccess = (
    exitCode === 0 &&
    !isKilledBySignal &&
    !isProcessError &&
    fail === 0 &&
    cancelled === 0 &&
    tests !== null &&
    tests > 0 &&
    pass !== null &&
    pass === tests
  );

  const status = isSuccess ? 'pass' : 'fail';

  return {
    exitCode: exitCode !== undefined ? exitCode : null,
    signal: signal || null,
    error: error ? String(error) : null,
    status,
    durationMs,
    tests,
    pass,
    fail: fail ?? (isSuccess ? 0 : 1),
    cancelled,
    skip,
    todo,
    suiteTimings: parseSuiteTimings(output),
  };
}

function loadProgress() {
  const currentFingerprint = computeFingerprint();
  if (fs.existsSync(PROGRESS_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(PROGRESS_FILE, 'utf8'));
      if (data.fingerprint && data.fingerprint.fingerprintId === currentFingerprint.fingerprintId) {
        console.log(`[CACHE] Valid cache hit for fingerprint ${currentFingerprint.fingerprintId}`);
        return data;
      } else {
        console.log(`[CACHE] Fingerprint mismatch (source/diff/probe changed). Invalidating cache.`);
        console.log(`[CACHE] Old: ${data.fingerprint?.fingerprintId || 'none'} -> New: ${currentFingerprint.fingerprintId}`);
      }
    } catch {}
  }
  return {
    fingerprint: currentFingerprint,
    metadata: getSystemMetadata(),
    compile: {},
    warmup: {},
    runs: {
      A1: null, B1: null,
      B2: null, A2: null,
      A3: null, B3: null,
    },
    layers: {},
    operations: {},
  };
}

function saveProgress(progress) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2), 'utf8');
}

function runCommand(cmd, args, cwd, logFilePath, extraEnv = {}) {
  return new Promise((resolve) => {
    const startedAt = new Date().toISOString();
    console.log(`[EXEC] cd ${cwd} && ${cmd} ${args.join(' ')}`);
    const startTime = Date.now();
    const env = getCleanEnv(extraEnv);
    if (logFilePath && fs.existsSync(logFilePath)) {
      try { fs.unlinkSync(logFilePath); } catch {}
    }

    const child = cp.spawn(cmd, args, { cwd, env, windowsHide: true });
    let output = '';

    const heartbeat = setInterval(() => {
      const elapsed = Math.round((Date.now() - startTime) / 1000);
      console.log(`[PROGRESS] cd ${path.basename(cwd)} - ${args.slice(0, 2).join(' ')} Elapsed: ${elapsed}s`);
    }, 15000);

    child.stdout.on('data', (d) => {
      const s = d.toString('utf8');
      output += s;
      if (logFilePath) {
        try { fs.appendFileSync(logFilePath, s, 'utf8'); } catch {}
      }
    });

    child.stderr.on('data', (d) => {
      const s = d.toString('utf8');
      output += s;
      if (logFilePath) {
        try { fs.appendFileSync(logFilePath, s, 'utf8'); } catch {}
      }
    });

    child.on('close', (code, signal) => {
      clearInterval(heartbeat);
      const endedAt = new Date().toISOString();
      const durationMs = Date.now() - startTime;
      console.log(`[FINISHED] ${cmd} ${args[0] || ''} exit=${code} duration=${(durationMs / 1000).toFixed(2)}s`);

      const result = parseRunOutput(output, code, signal, null, durationMs);
      result.startedAt = startedAt;
      result.endedAt = endedAt;
      resolve(result);
    });

    child.on('error', (err) => {
      clearInterval(heartbeat);
      const endedAt = new Date().toISOString();
      const durationMs = Date.now() - startTime;
      console.error(`[ERROR] ${cmd} ${args[0] || ''} error=${err.message}`);

      const result = parseRunOutput(output, 1, null, err.message, durationMs);
      result.startedAt = startedAt;
      result.endedAt = endedAt;
      resolve(result);
    });
  });
}

async function compileWorktree(name, dir) {
  const outDir = path.join(dir, 'out');
  if (fs.existsSync(outDir)) {
    try { fs.rmSync(outDir, { recursive: true, force: true }); } catch {}
  }
  const logFile = path.join(RESULTS_DIR, `compile-${name}.log`);
  const res = await runCommand('node', ['./node_modules/typescript/bin/tsc', '-p', './'], dir, logFile);
  if (res.exitCode !== 0) {
    throw new Error(`Compile failed for ${name}: exit ${res.exitCode}, error=${res.error}`);
  }
  return { durationMs: res.durationMs, startedAt: res.startedAt, endedAt: res.endedAt };
}

async function executeRunWithRetry(progress, name, cmd, args, cwd, logFile, maxAttempts = 2, extraEnv = {}) {
  let lastRes = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const attemptLog = attempt === 1 ? logFile : logFile.replace(/\.log$/, `-attempt${attempt}.log`);
    const res = await runCommand(cmd, args, cwd, attemptLog, extraEnv);
    if (res.status === 'pass') {
      return res;
    }
    console.warn(`[WARN] Run ${name} attempt ${attempt} did not pass cleanly (exit=${res.exitCode}, fail=${res.fail}). Recording contaminated run.`);
    if (!progress.contaminatedRuns) progress.contaminatedRuns = [];
    progress.contaminatedRuns.push({
      runName: name,
      attempt,
      exitCode: res.exitCode,
      signal: res.signal,
      fail: res.fail,
      durationMs: res.durationMs,
      logFile: attemptLog,
      timestamp: new Date().toISOString(),
      reason: `Exit ${res.exitCode}, fail count ${res.fail}`,
    });
    saveProgress(progress);
    lastRes = res;
  }
  return lastRes;
}

function parseOpsLog(logFile) {
  const counts = {
    git: 0,
    lock_helper: 0,
    taskstore_cas: 0,
    taskstore_cas_attempt: 0,
    taskstore_cas_success: 0,
    taskstore_cas_failure: 0,
    taskstore_write: 0,
    taskstore_write_attempt: 0,
    taskstore_write_success: 0,
    taskstore_write_failure: 0,
  };
  if (!fs.existsSync(logFile)) return counts;
  const lines = fs.readFileSync(logFile, 'utf8').split('\n').filter(Boolean);
  for (const line of lines) {
    if (line.startsWith('{')) {
      try {
        const obj = JSON.parse(line);
        counts.git += obj.git || 0;
        counts.lock_helper += obj.lock_helper || 0;
        counts.taskstore_cas_attempt += obj.taskstore_cas_attempt || 0;
        counts.taskstore_cas_success += obj.taskstore_cas_success || 0;
        counts.taskstore_cas_failure += obj.taskstore_cas_failure || 0;
        counts.taskstore_write_attempt += obj.taskstore_write_attempt || 0;
        counts.taskstore_write_success += obj.taskstore_write_success || 0;
        counts.taskstore_write_failure += obj.taskstore_write_failure || 0;
      } catch {}
    } else {
      if (counts[line] !== undefined) counts[line]++;
    }
  }
  counts.taskstore_cas = counts.taskstore_cas_success;
  counts.taskstore_write = counts.taskstore_write_success;
  return counts;
}

function isRunSuccessful(r) {
  if (!r) return false;
  if (r.exitCode !== 0) return false;
  if (r.signal !== null && r.signal !== undefined) return false;
  if (r.error !== null && r.error !== undefined) return false;
  if (r.status !== 'pass') return false;
  if (typeof r.tests !== 'number' || r.tests <= 0) return false;
  if (typeof r.pass !== 'number' || r.pass !== r.tests) return false;
  if ((r.fail || 0) !== 0) return false;
  if ((r.cancelled || 0) !== 0) return false;
  return true;
}

function evaluateCriteria(progress, mappingCheck) {
  const baselines = [progress.runs?.A1?.durationMs || 0, progress.runs?.A2?.durationMs || 0, progress.runs?.A3?.durationMs || 0];
  const candidates = [progress.runs?.B1?.durationMs || 0, progress.runs?.B2?.durationMs || 0, progress.runs?.B3?.durationMs || 0];

  const median = (arr) => {
    const s = [...arr].sort((a, b) => a - b);
    return s[1];
  };

  const medianA = median(baselines);
  const medianB = median(candidates);
  const rangeA = Math.max(...baselines) - Math.min(...baselines);
  const rangeB = Math.max(...candidates) - Math.min(...candidates);
  const maxRange = Math.max(rangeA, rangeB);
  const medianDiffMs = medianA - medianB;
  const speedupPercent = medianA > 0 ? (medianDiffMs / medianA) * 100 : 0;
  const speedupSeconds = medianDiffMs / 1000;

  const pairs = [
    { pair: 1, baselineKey: 'A1', candidateKey: 'B1', baselineMs: progress.runs?.A1?.durationMs || 0, candidateMs: progress.runs?.B1?.durationMs || 0, diffMs: (progress.runs?.A1?.durationMs || 0) - (progress.runs?.B1?.durationMs || 0) },
    { pair: 2, baselineKey: 'A2', candidateKey: 'B2', baselineMs: progress.runs?.A2?.durationMs || 0, candidateMs: progress.runs?.B2?.durationMs || 0, diffMs: (progress.runs?.A2?.durationMs || 0) - (progress.runs?.B2?.durationMs || 0) },
    { pair: 3, baselineKey: 'A3', candidateKey: 'B3', baselineMs: progress.runs?.A3?.durationMs || 0, candidateMs: progress.runs?.B3?.durationMs || 0, diffMs: (progress.runs?.A3?.durationMs || 0) - (progress.runs?.B3?.durationMs || 0) },
  ];

  const allPairsFaster = pairs.every((p) => p.diffMs > 0 && p.baselineMs > 0 && p.candidateMs > 0);
  const meetsPercentThreshold = speedupPercent >= 10.0;
  const meetsSecondsThreshold = speedupSeconds >= 30.0;
  const diffExceedsMaxRange = medianDiffMs > maxRange;

  const requiredRuns = [
    { key: 'warmup.baseline', run: progress.warmup?.baseline },
    { key: 'warmup.candidate', run: progress.warmup?.candidate },
    { key: 'runs.A1', run: progress.runs?.A1 },
    { key: 'runs.B1', run: progress.runs?.B1 },
    { key: 'runs.B2', run: progress.runs?.B2 },
    { key: 'runs.A2', run: progress.runs?.A2 },
    { key: 'runs.A3', run: progress.runs?.A3 },
    { key: 'runs.B3', run: progress.runs?.B3 },
    { key: 'layers.policy', run: progress.layers?.policy },
    { key: 'layers.boundary', run: progress.layers?.boundary },
    { key: 'layers.integration', run: progress.layers?.integration },
    { key: 'operations.baselineRun', run: progress.operations?.baselineRun },
    { key: 'operations.candidateRun', run: progress.operations?.candidateRun },
  ];

  const failedRequiredRuns = requiredRuns.filter((item) => !isRunSuccessful(item.run)).map((item) => item.key);
  const allTestsPass = failedRequiredRuns.length === 0;

  const zeroUnmappedFiles = !!mappingCheck?.valid;

  const recommendedForMerge = (
    allPairsFaster &&
    meetsPercentThreshold &&
    meetsSecondsThreshold &&
    diffExceedsMaxRange &&
    allTestsPass &&
    zeroUnmappedFiles
  );

  return {
    allThreePairsFaster: { met: allPairsFaster, pairs },
    medianDropAtLeast10Percent: { met: meetsPercentThreshold, actualPercent: Number(speedupPercent.toFixed(2)) },
    medianDropAtLeast30Seconds: { met: meetsSecondsThreshold, actualSeconds: Number(speedupSeconds.toFixed(2)) },
    medianDiffExceedsMaxRange: { met: diffExceedsMaxRange, medianDiffMs, maxRangeMs: maxRange, rangeA, rangeB },
    allTestsPassWithoutFailures: { met: allTestsPass, failedRuns: failedRequiredRuns },
    zeroUnmappedFiles: { met: zeroUnmappedFiles, unmapped: mappingCheck?.unmapped || [] },
    recommendedForMerge,
    summary: {
      medianBaselineMs: medianA,
      medianCandidateMs: medianB,
      medianDiffMs,
      speedupPercent: Number(speedupPercent.toFixed(2)),
      speedupSeconds: Number(speedupSeconds.toFixed(2)),
      rangeBaselineMs: rangeA,
      rangeCandidateMs: rangeB,
    },
  };
}

async function main() {
  console.log('=== Starting Test Layering Benchmark Orchestrator ===');
  const progress = loadProgress();

  // 0. Pre-flight fail-closed check: layer mapping
  console.log('[Step 0/6] Validating Test Layering Coverage Mapping...');
  const mappingCheck = validateLayerMapping();
  if (!mappingCheck.valid) {
    throw new Error(`Layer mapping validation failed: ${mappingCheck.reason}`);
  }
  console.log(`[Step 0/6] Validated: ${mappingCheck.discoveredCount} test file(s) cleanly mapped to layers with 0 unmapped.`);

  // 1. Clean Compile baseline and candidate
  if (!progress.compile.baseline) {
    console.log('[Step 1/6] Clean Compiling Baseline (3830923)...');
    progress.compile.baseline = await compileWorktree('baseline', BASELINE_ROOT);
    saveProgress(progress);
  }
  if (!progress.compile.candidate) {
    console.log('[Step 1/6] Clean Compiling Candidate (Current Worktree)...');
    progress.compile.candidate = await compileWorktree('candidate', WORKTREE_ROOT);
    saveProgress(progress);
  }

  // 2. Warm-up runs
  if (!progress.warmup.baseline || progress.warmup.baseline.status !== 'pass') {
    console.log('[Step 2/6] Warm-up Baseline (A0)...');
    const log = path.join(RESULTS_DIR, 'warmup-baseline.log');
    const res = await executeRunWithRetry(progress, 'warmup-baseline', 'node', ['scripts/test.cjs'], BASELINE_ROOT, log);
    progress.warmup.baseline = res;
    saveProgress(progress);
    if (res.status !== 'pass') {
      throw new Error(`Warmup baseline run failed with exit ${res.exitCode}, failures: ${res.fail}`);
    }
  }
  if (!progress.warmup.candidate || progress.warmup.candidate.status !== 'pass') {
    console.log('[Step 2/6] Warm-up Candidate (B0)...');
    const log = path.join(RESULTS_DIR, 'warmup-candidate.log');
    const res = await executeRunWithRetry(progress, 'warmup-candidate', 'node', ['scripts/test.cjs'], WORKTREE_ROOT, log);
    progress.warmup.candidate = res;
    saveProgress(progress);
    if (res.status !== 'pass') {
      throw new Error(`Warmup candidate run failed with exit ${res.exitCode}, failures: ${res.fail}`);
    }
  }

  // 3. Interleaved 3 full A/B runs (A1, B1; B2, A2; A3, B3)
  const runSequence = [
    { key: 'A1', type: 'baseline', dir: BASELINE_ROOT },
    { key: 'B1', type: 'candidate', dir: WORKTREE_ROOT },
    { key: 'B2', type: 'candidate', dir: WORKTREE_ROOT },
    { key: 'A2', type: 'baseline', dir: BASELINE_ROOT },
    { key: 'A3', type: 'baseline', dir: BASELINE_ROOT },
    { key: 'B3', type: 'candidate', dir: WORKTREE_ROOT },
  ];

  console.log('[Step 3/6] Running 3 Interleaved Full Runs...');
  for (const item of runSequence) {
    if (!progress.runs[item.key] || progress.runs[item.key].status !== 'pass') {
      console.log(`[Run ${item.key}] (${item.type})...`);
      const log = path.join(RESULTS_DIR, `full-${item.key}-${item.type}.log`);
      const res = await executeRunWithRetry(progress, `full-${item.key}-${item.type}`, 'node', ['scripts/test.cjs'], item.dir, log);
      progress.runs[item.key] = res;
      saveProgress(progress);
      if (res.status !== 'pass') {
        throw new Error(`Run ${item.key} (${item.type}) failed: exit ${res.exitCode}, failures: ${res.fail}`);
      }
      console.log(`[Run ${item.key}] Finished in ${(res.durationMs / 1000).toFixed(2)}s (tests: ${res.tests}, pass: ${res.pass}, fail: ${res.fail})`);
    } else {
      console.log(`[Run ${item.key}] Already completed cleanly in ${(progress.runs[item.key].durationMs / 1000).toFixed(2)}s`);
    }
  }

  // 4. Candidate Layer-by-layer standalone runs
  console.log('[Step 4/6] Running Candidate Layer-by-Layer Standalone Tests...');
  const layers = ['policy', 'boundary', 'integration'];
  for (const layer of layers) {
    if (!progress.layers[layer] || progress.layers[layer].status !== 'pass') {
      console.log(`[Layer: ${layer}] Running...`);
      const log = path.join(RESULTS_DIR, `layer-${layer}.log`);
      const res = await executeRunWithRetry(progress, `layer-${layer}`, 'node', ['scripts/test.cjs', `--layer=${layer}`], WORKTREE_ROOT, log);
      progress.layers[layer] = res;
      saveProgress(progress);
      if (res.status !== 'pass') {
        throw new Error(`Layer run ${layer} failed: exit ${res.exitCode}, failures: ${res.fail}`);
      }
      console.log(`[Layer: ${layer}] Finished in ${(res.durationMs / 1000).toFixed(2)}s (tests: ${res.tests}, pass: ${res.pass})`);
    } else {
      console.log(`[Layer: ${layer}] Already completed cleanly in ${(progress.layers[layer].durationMs / 1000).toFixed(2)}s`);
    }
  }

  // 5. Operations profiling (sampling pass)
  console.log('[Step 5/6] Measuring Operation Counts (Git, Lock Helpers, TaskStore CAS & write attempts vs successes)...');
  const preloadScript = path.resolve(WORKTREE_ROOT, 'scripts/profile-preload.cjs');

  if (!progress.operations.baseline || !progress.operations.baselineRun || progress.operations.baselineRun.status !== 'pass') {
    const logFile = path.join(RESULTS_DIR, 'ops-baseline.txt');
    if (fs.existsSync(logFile)) fs.unlinkSync(logFile);
    console.log('[Operations] Profiling Baseline...');
    const testLog = path.join(RESULTS_DIR, 'profile-run-baseline.log');
    const res = await executeRunWithRetry(progress, 'profile-run-baseline', 'node', ['scripts/test.cjs'], BASELINE_ROOT, testLog, 2, {
      NODE_OPTIONS: `--require ${preloadScript}`,
      BENCHMARK_PROFILE_LOG: logFile,
    });
    progress.operations.baseline = parseOpsLog(logFile);
    progress.operations.baselineRun = res;
    saveProgress(progress);
    if (res.status !== 'pass') {
      throw new Error(`Baseline profiling run failed: exit ${res.exitCode}, failures: ${res.fail}`);
    }
    console.log('[Operations Baseline]', progress.operations.baseline);
  }

  if (!progress.operations.candidate || !progress.operations.candidateRun || progress.operations.candidateRun.status !== 'pass') {
    console.log('[Operations] Profiling Candidate...');
    const logFile = path.join(RESULTS_DIR, 'ops-candidate.txt');
    if (fs.existsSync(logFile)) fs.unlinkSync(logFile);
    const testLog = path.join(RESULTS_DIR, 'profile-run-candidate.log');
    const res = await executeRunWithRetry(progress, 'profile-run-candidate', 'node', ['scripts/test.cjs'], WORKTREE_ROOT, testLog, 2, {
      NODE_OPTIONS: `--require ${preloadScript}`,
      BENCHMARK_PROFILE_LOG: logFile,
    });
    progress.operations.candidate = parseOpsLog(logFile);
    progress.operations.candidateRun = res;
    saveProgress(progress);
    if (res.status !== 'pass') {
      throw new Error(`Candidate profiling run failed: exit ${res.exitCode}, failures: ${res.fail}`);
    }
    console.log('[Operations Candidate]', progress.operations.candidate);
  }

  // 6. Acceptance Criteria Evaluation & Reporting
  console.log('[Step 6/6] Computing Benchmark Statistics & Acceptance Criteria...');
  const criteria = evaluateCriteria(progress, mappingCheck);

  const benchmarkResult = {
    metadata: {
      ...progress.metadata,
      date: new Date().toISOString(),
      baselineCommit: progress.fingerprint.baselineCommit,
      candidateCommit: progress.fingerprint.candidateCommit,
      fingerprint: progress.fingerprint,
      concurrency: 4,
    },
    compilation: {
      baseline: progress.compile.baseline,
      candidate: progress.compile.candidate,
    },
    warmup: {
      baseline: progress.warmup.baseline,
      candidate: progress.warmup.candidate,
    },
    fullRuns: {
      rawRuns: progress.runs,
      summary: criteria.summary,
    },
    layers: progress.layers,
    operations: progress.operations,
    contaminatedRuns: progress.contaminatedRuns || [],
    acceptanceCriteria: criteria,
  };

  const finalJsonPath = path.resolve(WORKTREE_ROOT, 'benchmark.json');
  fs.writeFileSync(finalJsonPath, JSON.stringify(benchmarkResult, null, 2), 'utf8');
  fs.writeFileSync(path.resolve(RESULTS_DIR, 'benchmark.json'), JSON.stringify(benchmarkResult, null, 2), 'utf8');

  console.log('\n========================================');
  console.log('BENCHMARK COMPLETE');
  console.log(`Median Baseline:  ${(criteria.summary.medianBaselineMs / 1000).toFixed(2)}s`);
  console.log(`Median Candidate: ${(criteria.summary.medianCandidateMs / 1000).toFixed(2)}s`);
  console.log(`Speedup:          ${criteria.summary.speedupPercent.toFixed(2)}% (${criteria.summary.speedupSeconds.toFixed(2)}s faster)`);
  console.log(`All Pairs Faster: ${criteria.allThreePairsFaster.met ? 'YES' : 'NO'}`);
  console.log(`All Tests Pass:   ${criteria.allTestsPassWithoutFailures.met ? 'YES' : 'NO'}`);
  console.log(`Criteria Met:     ${criteria.recommendedForMerge ? 'YES (RECOMMENDED FOR MERGE)' : 'NO'}`);
  console.log(`Results saved to: ${finalJsonPath}`);
  console.log('========================================\n');
}

module.exports = {
  computeFingerprint,
  getSystemMetadata,
  validateLayerMapping,
  parseRunOutput,
  parseOpsLog,
  evaluateCriteria,
  loadProgress,
  saveProgress,
};

if (require.main === module) {
  main().catch((err) => {
    console.error('Benchmark orchestrator failed:', err);
    process.exit(1);
  });
}
