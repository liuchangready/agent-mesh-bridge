import { testMultiStashRecovery, testStashCrashRecovery } from './git-worktree-recovery-stash-fixture';

// Balanced from the 2026-11-17 four-process run: about 114 seconds.
for (const [phase, concurrent] of [
  ['stash_cleanup_started', true],
  ['stash_reflog_published', true],
  ['before_stash_reflog_published', true],
] as const) testMultiStashRecovery(phase, concurrent);

for (const phase of [
  'stash_created',
  'after_reflog_deleted',
  'before_stash_restored_concurrent',
]) testStashCrashRecovery(phase);
