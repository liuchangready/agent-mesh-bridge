import { TaskWorkspaceBinding } from '../bridge/workspace';

export function testWorkspace(executionPath: string): TaskWorkspaceBinding {
  return { id: 'ws_test', environmentId: 'test', rootPath: executionPath,
    canonicalRootKey: executionPath, nameAtDispatch: 'test', source: 'explicit', executionPath };
}
