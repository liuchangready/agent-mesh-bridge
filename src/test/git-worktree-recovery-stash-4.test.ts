import { testMultiStashRecovery, testStashCrashRecovery } from './git-worktree-recovery-stash-fixture';

// Balanced from the 2026-11-17 four-process run: about 113 seconds.
for (const [phase, concurrent] of [
  ['stash_reflog_published', false],
  ['before_stash_reflog_published', false],
  ['stash_ref_published', false],
  ['hard_stash_reflog_published', false],
] as const) testMultiStashRecovery(phase, concurrent);

for (const phase of [
  'ref_updated',
  'before_stash_restored',
]) testStashCrashRecovery(phase);
