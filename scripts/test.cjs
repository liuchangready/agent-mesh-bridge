const { readdirSync, readFileSync } = require('node:fs');
const { resolve, basename } = require('node:path');
const { run } = require('node:test');
const { spec } = require('node:test/reporters');

// Sanitize ambient receiver environment variables so test subprocesses
// do not inherit receiver agent identity or trigger receiver tool restrictions.
delete process.env.MESH_TASK_RECEIVER;
delete process.env.MESH_TASK_THREAD_ID;
delete process.env.MESH_TASK_ROUND;
delete process.env.MESH_REVIEW_BOOTSTRAP;
delete process.env.MESH_REVIEW_THREAD_ID;
delete process.env.MESH_REVIEW_BOOTSTRAP_TOKEN;
delete process.env.MESH_CALLER_AGENT;

// Comprehensive 3-layer architecture mapping according to docs/TEST_LAYERING_DESIGN.md.
const LAYER_MAPPING = {
  policy: new Set([
    'active-agent-resolver.test.js',
    'agent-capabilities.test.js',
    'build-staleness.test.js',
    'caller-poll-guard.test.js',
    'caller-session-safety.test.js',
    'legacy-removal.test.js',
    'caller-wake-queue.test.js',
    'config-sync.test.js',
    'context-compactor.test.js',
    'dispatcher-rework.test.js',
    'execution-gate.test.js',
    'failed-state-unification.test.js',
    'host-suspension.test.js',
    'independent-scheduling-lifecycle.test.js',
    'merge-validation-strategy.test.js',
    'model-approval.test.js',
    'model-catalog.test.js',
    'rate-limit-tracker.test.js',
    'reviewer-bootstrap-credentials.test.js',
    'reviewer-log.test.js',
    'rework-lifecycle-fixes.test.js',
    'rework-loop-guard.test.js',
    'round-invariant.test.js',
    'task-control.test.js',
    'task-retarget.test.js',
    'task-summary.test.js',
    'task-templates-fail-closed.test.js',
    'task-templates.test.js',
    'template-commands.test.js',
    'test-runner.test.js',
    'token-usage.test.js',
    'unified-execution-policy.test.js',
    'worktree-details.test.js',
  ]),
  boundary: new Set([
    'controller-start-admission.test.js',
    'failed-round-recovery.test.js',
    'task-store-atomic-write.test.js',
    'autonomous-runner.test.js',
    'callback-failure.test.js',
    'codex-session.test.js',
    'deepseek-harness.test.js',
    'git-probe.test.js',
    'git-worktree-fixture.test.js',
    'git-worktree-locks.test.js',
    'git-worktree-recovery-resolution.test.js',
    'git-worktree-recovery-stash-2.test.js',
    'git-worktree-recovery-stash-3.test.js',
    'git-worktree-recovery-stash-4.test.js',
    'git-worktree-recovery-stash.test.js',
    'native-session-cleanup.test.js',
    'native-session-lock.test.js',
    'per-agent-log-directories.test.js',
    'reviewer-log-capture.test.js',
    'task-data-dir.test.js',
    'task-role-lock.test.js',
    'task-store.test.js',
    'unified-execution-boundary.test.js',
    'unified-state-writing.test.js',
    'unique-run-launch.test.js',
    'workspace.test.js',
    'worktree-root.test.js',
  ]),
  integration: new Set([
    'mock-agents-e2e.test.js',
    'concurrency.test.js',
    'git-worktree-merge.test.js',
    'git-worktree-recovery.test.js',
    'git-worktree-stash.test.js',
    'git-worktree-stash-part2.test.js',
    'mcp-server.test.js',
    'merge-recovery.test.js',
    'merge-resident-host.test.js',
    'review-wake.test.js',
    'reviewer-bootstrap.test.js',
    'running-reworking-merge.test.js',
    'unified-execution-integration.test.js',
    'worktree-dispatch.test.js',
  ]),
};

// Parse optional --layer=<name> CLI argument. Default is full union of all layers.
const layerArg = process.argv.find((arg) => arg.startsWith('--layer='));
const requestedLayer = layerArg ? layerArg.split('=')[1].toLowerCase() : undefined;
if (requestedLayer && !LAYER_MAPPING[requestedLayer]) {
  console.error(`Invalid layer "${requestedLayer}". Must be one of: ${Object.keys(LAYER_MAPPING).join(', ')}`);
  process.exit(1);
}

// Start longest integration and boundary files first so they do not form a late tail.
const priority = new Map([
  ['git-worktree-stash.test.js', 0],
  ['git-worktree-stash-part2.test.js', 1],
  ['git-worktree-merge.test.js', 2],
  ['unified-execution-integration.test.js', 3],
  ['git-worktree-locks.test.js', 4],
  ['concurrency.test.js', 5],
  ['mcp-server.test.js', 6],
]);

const discoveredFiles = readdirSync('out/test', { recursive: true })
  .filter((file) => file.endsWith('.test.js'));

if (!discoveredFiles.length) {
  throw new Error('No compiled tests found; run npm run compile first.');
}

// Check if running in the actual agent-mesh-bridge project repository
const isProjectRepo = (() => {
  try {
    const pkg = JSON.parse(readFileSync('package.json', 'utf8'));
    return pkg.name === 'agent-mesh-bridge';
  } catch {
    return false;
  }
})();

// Fail-closed verification: ensure every discovered test file in the project is tracked in layer mapping
if (isProjectRepo) {
  const unmappedFiles = discoveredFiles.filter(
    (f) => {
      const b = basename(f);
      return !LAYER_MAPPING.policy.has(b) && !LAYER_MAPPING.boundary.has(b) && !LAYER_MAPPING.integration.has(b);
    }
  );
  if (unmappedFiles.length > 0) {
    throw new Error(`Unmapped test file(s) found in out/test: ${unmappedFiles.join(', ')}. All tests must be categorized.`);
  }
}

const targetFiles = requestedLayer
  ? discoveredFiles.filter((file) => LAYER_MAPPING[requestedLayer].has(basename(file)))
  : discoveredFiles;

const files = targetFiles
  .sort((a, b) => {
    const pA = priority.get(basename(a)) ?? 10;
    const pB = priority.get(basename(b)) ?? 10;
    return pA - pB || a.localeCompare(b);
  })
  .map((file) => resolve('out/test', file));

if (requestedLayer) {
  console.log(`[Layer: ${requestedLayer}] Running ${files.length} test suite(s)...`);
} else {
  console.log(`[Full Suite] Running ${files.length} test suite(s) across all layers...`);
}

run({ files, concurrency: 4 })
  .on('test:fail', () => { process.exitCode = 1; })
  .compose(spec)
  .pipe(process.stdout);
