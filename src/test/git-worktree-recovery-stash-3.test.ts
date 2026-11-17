import { testMultiStashRecovery, testStashCrashRecovery } from './git-worktree-recovery-stash-fixture';

// Balanced from the 2026-11-17 four-process run: about 103 seconds.
for (const [phase, concurrent] of [
  ['stash_cleanup_started', false],
  ['stash_cleanup_prepared', true],
  ['before_stash_ref_published', true],
  ['before_stash_ref_published', false],
] as const) testMultiStashRecovery(phase, concurrent);

for (const phase of [
  'stash_applied',
]) testStashCrashRecovery(phase);
