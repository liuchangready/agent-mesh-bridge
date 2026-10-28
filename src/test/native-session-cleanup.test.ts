import { test, describe, beforeEach, afterEach } from 'node:test';
import * as assert from 'node:assert';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';

import {
  cleanupThreadNativeSessions,
  collectThreadSessionRefs,
} from '../bridge/native-session-cleanup';
import { DelegationThread, TaskRetargetRecord } from '../bridge/types';

const SESSION_CLAUDE = '11111111-2222-3333-4444-555555555555';
const SESSION_CODEX = '01a16bb5-4139-7231-910b-3bed102caa6e';
const SESSION_ANTI = 'aa9fd1da-01b7-4bd7-a8df-8aae3b0aee44';
const SESSION_DSH = 'session-941170d8-44f3-4e81-bf18-fb91786a8f86';
const SESSION_OTHER = '99999999-8888-7777-6666-555555555555';

function makeThread(overrides: Partial<DelegationThread> = {}): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: 'thread_native_1',
    title: 'Native conversation cleanup',
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'completed',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: SESSION_CLAUDE,
    workspacePath: os.tmpdir(),
    summary: { lastPromptSnippet: 'p', lastOutputSnippet: 'o', totalDurationMs: 0 },
    rounds: [],
    ...overrides,
  };
}

function writeFile(file: string, content: string = 'x'): void {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, content, 'utf8');
}

function writeSessionDir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, 'session.v4.jsonl.zstd'), 'x', 'utf8');
}

function readJson(file: string): any {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

describe('native conversation cleanup on task deletion', () => {
  let home: string;

  beforeEach(() => {
    home = fs.mkdtempSync(path.join(os.tmpdir(), 'agent-bridge-native-home-'));
  });

  afterEach(() => {
    try {
      fs.rmSync(home, { recursive: true, force: true });
    } catch {}
  });

  const cleanup = (thread: DelegationThread) =>
    cleanupThreadNativeSessions(thread, { homeDir: home, env: {} });

  test('Claude Code: removes the transcript, subagent dir and per-session state', () => {
    const project = path.join(home, '.claude', 'projects', 'D--agent-bridge');
    const transcript = path.join(project, `${SESSION_CLAUDE}.jsonl`);
    writeFile(transcript);
    writeFile(path.join(project, SESSION_CLAUDE, 'subagents', 'agent-1.jsonl'));
    writeFile(path.join(home, '.claude', 'session-env', SESSION_CLAUDE, 'env.json'));
    writeFile(path.join(home, '.claude', 'file-history', SESSION_CLAUDE, 'edit.txt'));
    writeFile(
      path.join(home, '.claude', 'todos', `${SESSION_CLAUDE}-agent-${SESSION_CLAUDE}.json`),
    );
    writeFile(path.join(project, `${SESSION_OTHER}.jsonl`));

    const result = cleanup(makeThread());

    assert.strictEqual(result.entries.length, 1);
    assert.ok(result.entries[0].found, 'the conversation must be reported as found');
    assert.strictEqual(result.entries[0].error, undefined);
    assert.strictEqual(result.removedCount, 5);
    assert.strictEqual(fs.existsSync(transcript), false);
    assert.strictEqual(fs.existsSync(path.join(project, SESSION_CLAUDE)), false);
    assert.strictEqual(
      fs.existsSync(path.join(home, '.claude', 'session-env', SESSION_CLAUDE)),
      false,
    );
    assert.strictEqual(
      fs.existsSync(path.join(home, '.claude', 'file-history', SESSION_CLAUDE)),
      false,
    );
    assert.ok(
      fs.existsSync(path.join(project, `${SESSION_OTHER}.jsonl`)),
      'unrelated conversations must never be touched',
    );
  });

  test('Codex: removes the rollout and its session_index entry', () => {
    const rollout = path.join(
      home,
      '.codex',
      'sessions',
      '2026',
      '10',
      '24',
      `rollout-2026-10-24T10-00-00-${SESSION_CODEX}.jsonl`,
    );
    writeFile(rollout);
    writeFile(path.join(home, '.codex', 'thread-writer-locks', `${SESSION_CODEX}.lock`));
    const index = path.join(home, '.codex', 'session_index.jsonl');
    const unrelated = { id: SESSION_OTHER, thread_name: 'keep me' };
    writeFile(
      index,
      [
        JSON.stringify({ id: SESSION_CODEX, thread_name: 'deleted task' }),
        JSON.stringify(unrelated),
        '',
      ].join('\n'),
    );

    const result = cleanup(makeThread({ targetAgent: 'codex', receiverSessionId: SESSION_CODEX }));

    assert.strictEqual(result.entries[0].agent, 'codex');
    assert.strictEqual(result.entries[0].error, undefined);
    assert.strictEqual(fs.existsSync(rollout), false);
    assert.strictEqual(
      fs.existsSync(path.join(home, '.codex', 'thread-writer-locks', `${SESSION_CODEX}.lock`)),
      false,
    );

    const lines = fs
      .readFileSync(index, 'utf8')
      .split('\n')
      .filter((line) => line.trim());
    assert.strictEqual(lines.length, 1);
    assert.deepStrictEqual(JSON.parse(lines[0]), unrelated);
  });

  test('Antigravity: removes the DB, brain, annotations, implicit record and cache entries', () => {
    const cliRoot = path.join(home, '.gemini', 'antigravity-cli');
    const desktopRoot = path.join(home, '.gemini', 'antigravity');

    for (const root of [cliRoot, desktopRoot]) {
      writeFile(path.join(root, 'conversations', `${SESSION_ANTI}.db`));
      writeFile(path.join(root, 'conversations', `${SESSION_ANTI}.db-wal`));
      writeFile(path.join(root, 'brain', SESSION_ANTI, 'brain.json'));
      writeFile(path.join(root, 'annotations', `${SESSION_ANTI}.pbtxt`));
      writeFile(path.join(root, 'implicit', `${SESSION_ANTI}.pb`));
      writeFile(
        path.join(root, 'cache', 'conversation_metadata.json'),
        JSON.stringify({
          conversations: { [SESSION_ANTI]: { summary: {} }, [SESSION_OTHER]: { summary: {} } },
        }),
      );
      writeFile(
        path.join(root, 'cache', 'last_conversations.json'),
        JSON.stringify({ 'D:\\agent_bridge': SESSION_ANTI, 'D:\\other': SESSION_OTHER }),
      );
    }

    const result = cleanup(
      makeThread({ targetAgent: 'antigravity', receiverSessionId: SESSION_ANTI }),
    );

    assert.strictEqual(result.entries[0].error, undefined);
    for (const root of [cliRoot, desktopRoot]) {
      assert.strictEqual(
        fs.existsSync(path.join(root, 'conversations', `${SESSION_ANTI}.db`)),
        false,
      );
      assert.strictEqual(fs.existsSync(path.join(root, 'brain', SESSION_ANTI)), false);
      assert.strictEqual(
        fs.existsSync(path.join(root, 'annotations', `${SESSION_ANTI}.pbtxt`)),
        false,
      );
      assert.strictEqual(fs.existsSync(path.join(root, 'implicit', `${SESSION_ANTI}.pb`)), false);

      const metadata = readJson(path.join(root, 'cache', 'conversation_metadata.json'));
      assert.deepStrictEqual(Object.keys(metadata.conversations), [SESSION_OTHER]);
      const last = readJson(path.join(root, 'cache', 'last_conversations.json'));
      assert.deepStrictEqual(last, { 'D:\\other': SESSION_OTHER });
    }
  });

  test('DeepSeek Harness: removes the session dir, projcache and workspace references', () => {
    const workspaceSlug = '--D-agent_bridge--';
    const sessionDir = path.join(home, '.dsh', 'sessions', workspaceSlug, SESSION_DSH);
    writeSessionDir(sessionDir);
    writeSessionDir(path.join(home, '.dsh', 'sessions', workspaceSlug, SESSION_OTHER));

    const projcache = path.join(home, '.dsh', 'storages', 'session_projcache', 'sessions');
    writeFile(path.join(projcache, `${SESSION_DSH}.json`), '{}');
    writeFile(path.join(projcache, `${SESSION_OTHER}.json`), '{}');

    const workspaceJson = path.join(home, '.dsh', 'storages', 'workspace.json');
    writeFile(
      workspaceJson,
      JSON.stringify({
        global: {
          archivedSessionIds: [SESSION_DSH, SESSION_OTHER],
          pinnedSessionIds: [SESSION_DSH],
        },
        tables: {
          workspaces: {
            w1: { path: 'D:\\agent_bridge', sessionIds: [SESSION_DSH, SESSION_OTHER] },
            w2: { path: 'D:\\other', sessionIds: [SESSION_OTHER] },
          },
        },
      }),
    );

    const result = cleanup(
      makeThread({ targetAgent: 'deepseek_harness', receiverSessionId: SESSION_DSH }),
    );

    assert.strictEqual(result.entries[0].error, undefined);
    assert.strictEqual(fs.existsSync(sessionDir), false);
    assert.strictEqual(fs.existsSync(path.join(projcache, `${SESSION_DSH}.json`)), false);
    assert.ok(fs.existsSync(path.join(home, '.dsh', 'sessions', workspaceSlug, SESSION_OTHER)));
    assert.ok(fs.existsSync(path.join(projcache, `${SESSION_OTHER}.json`)));

    const workspace = readJson(workspaceJson);
    assert.deepStrictEqual(workspace.global.archivedSessionIds, [SESSION_OTHER]);
    assert.deepStrictEqual(workspace.global.pinnedSessionIds, []);
    assert.deepStrictEqual(workspace.tables.workspaces.w1.sessionIds, [SESSION_OTHER]);
    assert.deepStrictEqual(workspace.tables.workspaces.w2.sessionIds, [SESSION_OTHER]);
  });

  test('a missing conversation is not an error', () => {
    const result = cleanup(makeThread({ receiverSessionId: SESSION_CLAUDE }));

    assert.strictEqual(result.entries.length, 1);
    assert.strictEqual(result.entries[0].found, false);
    assert.strictEqual(result.entries[0].removed.length, 0);
    assert.strictEqual(result.entries[0].error, undefined);
    assert.strictEqual(result.removedCount, 0);
  });

  test('the caller session is never deleted', () => {
    const project = path.join(home, '.claude', 'projects', 'D--agent-bridge');
    const callerTranscript = path.join(project, `${SESSION_OTHER}.jsonl`);
    writeFile(callerTranscript);

    const result = cleanup(
      makeThread({
        initiatorAgent: 'claude_code',
        targetAgent: 'claude_code',
        receiverSessionId: SESSION_CLAUDE,
        callerSessionId: SESSION_OTHER,
      }),
    );

    assert.deepStrictEqual(
      result.entries.map((entry) => entry.sessionId),
      [SESSION_CLAUDE],
      'only the receiver conversation may be cleaned',
    );
    assert.ok(fs.existsSync(callerTranscript), 'the caller chat is shared and must survive');
  });

  test('retarget history cleans the sessions of previously-used agents', () => {
    const retarget: TaskRetargetRecord = {
      at: new Date().toISOString(),
      fromAgent: 'antigravity',
      toAgent: 'codex',
      sessionReset: true,
      previousReceiverSessionId: SESSION_ANTI,
    };
    writeFile(path.join(home, '.gemini', 'antigravity-cli', 'conversations', `${SESSION_ANTI}.db`));
    writeFile(
      path.join(
        home,
        '.codex',
        'sessions',
        '2026',
        '10',
        '24',
        `rollout-2026-10-24T10-00-00-${SESSION_CODEX}.jsonl`,
      ),
    );

    const result = cleanup(
      makeThread({
        targetAgent: 'codex',
        receiverSessionId: SESSION_CODEX,
        retargets: [retarget],
      }),
    );

    assert.deepStrictEqual(
      result.entries.map((entry) => [entry.agent, entry.sessionId]),
      [
        ['codex', SESSION_CODEX],
        ['antigravity', SESSION_ANTI],
      ],
    );
    assert.strictEqual(result.removedCount, 2);
  });

  test('unsafe session ids are rejected before they can reach the file system', () => {
    const refs = collectThreadSessionRefs(
      makeThread({ receiverSessionId: '../agent-bridge-evil-marker' }),
    );
    assert.deepStrictEqual(refs, [], 'a path-traversal id must not become a cleanup target');
    assert.deepStrictEqual(cleanup(makeThread({ receiverSessionId: '../evil' })).entries, []);
  });

  test('placeholder receiver ids (never captured) are skipped silently', () => {
    const result = cleanup(makeThread({ receiverSessionId: 'sess-1' }));

    assert.deepStrictEqual(result.entries, []);
    assert.strictEqual(result.removedCount, 0);
  });

  test('vscode_chat tasks have no native conversation store', () => {
    const result = cleanup(
      makeThread({ targetAgent: 'vscode_chat', receiverSessionId: SESSION_CLAUDE }),
    );

    assert.deepStrictEqual(result.entries, []);
    assert.strictEqual(result.removedCount, 0);
  });
});
