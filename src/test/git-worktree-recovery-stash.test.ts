import { testMultiStashRecovery, testStashCrashRecovery } from './git-worktree-recovery-stash-fixture';

// Balanced from the 2026-11-17 four-process run: about 114 seconds.
for (const [phase, concurrent] of [
  ['stash_cleanup_prepared', false],
  ['stash_ref_published', true],
  ['hard_stash_cleanup_prepared', false],
  ['hard_stash_ref_published', false],
] as const) testMultiStashRecovery(phase, concurrent);

for (const phase of [
  'checkout_synced',
  'before_stash_applied',
]) testStashCrashRecovery(phase);
