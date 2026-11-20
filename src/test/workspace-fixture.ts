import { TaskWorkspaceBinding } from '../bridge/workspace';
import type { DelegationThread } from '../bridge/types';

/** Protocol/worker tests model a completed bootstrap without launching another CLI. */
export function confirmTestReviewer(thread: DelegationThread): DelegationThread {
  if (thread.execution?.run) throw new Error('Cannot hand off a live test execution.');
  const reviewer = `reviewer-${thread.threadId}`;
  thread.reviewFlow = 'dedicated';
  thread.reviewBootstrap = { phase: 'confirmed', attempts: 1 };
  thread.reviewerSessionCaptured = true;
  thread.reviewerSessionId = thread.callerSessionId = reviewer;
  thread.status = 'running';
  if (thread.execution) {
    thread.execution.phase = 'develop';
    thread.execution.purpose = undefined;
  }
  return thread;
}

export function testWorkspace(executionPath: string): TaskWorkspaceBinding {
  return { id: 'ws_test', environmentId: 'test', rootPath: executionPath,
    canonicalRootKey: executionPath, nameAtDispatch: 'test', source: 'explicit', executionPath };
}
