import { test, describe } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { AgentDetector } from '../bridge/agent-detector';
import { CallbackManager } from '../bridge/callback-manager';
import { DelegationThread } from '../bridge/types';

const UUID = '01a17d29-d40d-74c3-831d-878dd4edc0e5';
const ROLLOUT = `rollout-2026-10-27T19-40-56-${UUID}`;
const EXEC_ID = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';
const VSCODE_ID = 'dddddddd-dddd-4ddd-8ddd-dddddddddddd';
const MISFILED_ID = 'eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee';
const WRONG_META_ID = 'ffffffff-ffff-4fff-8fff-ffffffffffff';

function writeCodexSession(
  sessionsDir: string,
  name: string,
  meta: Record<string, unknown>,
  mtime: number,
): void {
  const file = path.join(sessionsDir, '2026', '11', '05', `${name}.jsonl`);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify({ type: 'session_meta', payload: meta })}\n`);
  fs.utimesSync(file, mtime / 1000, mtime / 1000);
}

describe('Codex active session selection', () => {
  test('selects only a CLI session whose session_meta cwd matches the workspace', () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), 'codex-active-session-'));
    const workspace = path.join(home, 'work', 'repo');
    const sessionsDir = path.join(home, '.codex', 'sessions');
    const previousHome = process.env.HOME;
    const previousUserProfile = process.env.USERPROFILE;
    fs.mkdirSync(workspace, { recursive: true });
    try {
      // Newer candidates are both invalid: one belongs to another cwd, one is a guardian.
      writeCodexSession(sessionsDir, 'rollout-2026-11-05T12-00-00-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', {
        id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', cwd: path.join(home, 'elsewhere'), source: 'cli',
      }, 3000);
      writeCodexSession(sessionsDir, 'rollout-2026-11-05T12-00-01-bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', {
        id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', cwd: workspace, source: { subagent: 'guardian' },
      }, 2000);
      writeCodexSession(sessionsDir, ROLLOUT, {
        id: UUID, cwd: workspace, source: 'cli',
      }, 1000);
      writeCodexSession(sessionsDir, `rollout-2026-11-05T12-00-02-${EXEC_ID}`, {
        id: EXEC_ID, cwd: workspace, source: 'exec',
      }, 4000);
      writeCodexSession(sessionsDir, `rollout-2026-11-05T12-00-03-${VSCODE_ID}`, {
        id: VSCODE_ID, cwd: workspace, source: 'vscode',
      }, 5000);
      writeCodexSession(sessionsDir, `rollout-2026-11-05T12-00-04-${MISFILED_ID}`, {
        id: WRONG_META_ID, cwd: workspace, source: 'cli',
      }, 6000);

      process.env.HOME = home;
      process.env.USERPROFILE = home;
      const detector = new AgentDetector();
      assert.strictEqual(detector.detectActiveSession('codex', workspace), VSCODE_ID);
      assert.strictEqual(detector.detectActiveSession('codex', workspace, [VSCODE_ID]), EXEC_ID);
      assert.strictEqual(detector.detectActiveSession('codex', workspace, [VSCODE_ID, EXEC_ID]), UUID);
      if (process.platform === 'win32') {
        const windowsVariant = `${workspace.replace(/\\/g, '/').toUpperCase()}/`;
        assert.strictEqual(detector.detectActiveSession('codex', windowsVariant), VSCODE_ID);
      }
      assert.strictEqual(
        new AgentDetector().detectActiveSession('codex', path.join(home, 'wrong-workspace')),
        undefined,
      );
    } finally {
      if (previousHome === undefined) delete process.env.HOME;
      else process.env.HOME = previousHome;
      if (previousUserProfile === undefined) delete process.env.USERPROFILE;
      else process.env.USERPROFILE = previousUserProfile;
      fs.rmSync(home, { recursive: true, force: true });
    }
  });
});

describe('caller session resolution', () => {
  const reviewerSession = '05e4a9c1-73f3-4d86-b64c-b1cf7fd3a08b';

  function thread(overrides: Partial<DelegationThread> = {}): DelegationThread {
    return {
      threadId: 'thread-caller-session',
      title: 'Caller session fixture',
      initiatorAgent: 'codex',
      targetAgent: 'claude_code',
      status: 'waiting_review',
      currentRound: 1,
      createdAt: new Date(0).toISOString(),
      updatedAt: new Date(0).toISOString(),
      receiverSessionId: 'receiver-session-123',
      workspacePath: process.cwd(),
      summary: { lastPromptSnippet: '', lastOutputSnippet: '', totalDurationMs: 0 },
      rounds: [],
      ...overrides,
    };
  }

  test('dedicated reviewer requires confirmed captured matching handoff', () => {
    const detector = { detectActiveSession: () => 'wrong-fallback-session' } as unknown as AgentDetector;
    const manager = new CallbackManager(detector);
    const resolve = (item: DelegationThread) => (manager as any).resolveCallerSession(item);
    const valid = thread({
      reviewFlow: 'dedicated',
      reviewBootstrap: { phase: 'confirmed', attempts: 1 },
      reviewerSessionCaptured: true,
      reviewerSessionId: reviewerSession,
      callerSessionId: reviewerSession,
    });

    assert.strictEqual(resolve(valid), reviewerSession);
    assert.strictEqual(resolve({ ...valid, reviewBootstrap: { phase: 'failed', attempts: 1 } }), undefined);
    assert.strictEqual(resolve({ ...valid, reviewerSessionCaptured: false }), undefined);
    assert.strictEqual(resolve({ ...valid, reviewerSessionId: undefined }), undefined);
    assert.strictEqual(resolve({ ...valid, callerSessionId: 'requester-session-123' }), undefined);
  });

  test('legacy caller session retains the previous id and detection fallback behavior', () => {
    let detections = 0;
    const detector = {
      detectActiveSession: () => {
        detections += 1;
        return 'detected-legacy-session';
      },
    } as unknown as AgentDetector;
    const manager = new CallbackManager(detector);
    const resolve = (item: DelegationThread) => (manager as any).resolveCallerSession(item);

    assert.strictEqual(resolve(thread({ callerSessionId: 'legacy-session-123' })), 'legacy-session-123');
    const missing = thread({ callerSessionId: undefined });
    assert.strictEqual(resolve(missing), 'detected-legacy-session');
    assert.strictEqual(missing.callerSessionId, 'detected-legacy-session');
    assert.strictEqual(detections, 1);
  });
});
