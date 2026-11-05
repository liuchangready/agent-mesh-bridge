import { strict as assert } from 'assert';
import { test } from 'node:test';
import * as childProcess from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { GroupAgentRuntime, GroupTurnResponse } from '../copilot/group-agent-runtime';

function git(cwd: string, ...args: string[]): string {
  return childProcess.execFileSync('git', args, { cwd, encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] }).trim();
}

test('GroupAgentRuntime runs structured write and command calls in one real Git worktree and resumes its append-only session', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-agent-'));
    const repo = path.join(root, 'repo');
    const worktree = path.join(root, 'task-worktree');
    const bridgeHome = path.join(root, 'bridge-home');
    fs.mkdirSync(repo);
    git(repo, 'init', '-b', 'main');
    git(repo, 'config', 'user.name', 'Bridge Test');
    git(repo, 'config', 'user.email', 'bridge-test@example.invalid');
    fs.writeFileSync(path.join(repo, 'README.md'), 'base\n');
    git(repo, 'add', 'README.md');
    git(repo, 'commit', '-m', 'base');
    git(repo, 'worktree', 'add', '-b', 'mesh/task/test', worktree, 'HEAD');

    const replies: GroupTurnResponse[] = [
      { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'write-1', name: 'write_file', arguments: { path: 'task.txt', content: 'from group agent\n' } }] },
      { text: '', finishReason: 'tool_calls', toolCalls: [{ id: 'command-1', name: 'run_command', arguments: { command: process.platform === 'win32' ? 'Set-Content verified.txt yes' : 'printf yes > verified.txt' } }] },
      { text: 'Implemented and verified the change.', finishReason: 'stop', toolCalls: [] },
    ];
    const runtime = new GroupAgentRuntime({
      bridgeHome,
      request: async (_messages, tools, signal) => {
        assert.equal(signal.aborted, false);
        assert.ok(tools.some((tool) => tool.name === 'run_command'));
        const reply = replies.shift();
        if (!reply) throw new Error('Unexpected model request.');
        return reply;
      },
    });
    try {
      const answer = await runtime.run('session_test_123', worktree, 'Implement the task and run a check.');
      assert.equal(answer.text, 'Implemented and verified the change.');
      assert.equal(answer.toolCallsCompleted, 2);
      assert.equal(fs.readFileSync(path.join(worktree, 'task.txt'), 'utf8'), 'from group agent\n');
      assert.equal(fs.readFileSync(path.join(worktree, 'verified.txt'), 'utf8').trim(), 'yes');
      assert.equal(fs.existsSync(path.join(repo, 'task.txt')), false);
      assert.equal(git(worktree, 'status', '--porcelain'), '?? task.txt\n?? verified.txt');
      const history = runtime.sessions.read('session_test_123');
      assert.ok(history.some((event) => event.role === 'tool' && event.content.includes('Wrote task.txt')));
      assert.ok(history.some((event) => event.role === 'tool' && event.content.includes('[exit 0]')));
      assert.ok(fs.readFileSync(path.join(bridgeHome, 'group-agent-sessions', 'session_test_123.jsonl'), 'utf8').includes('"role":"intent"'));
    } finally {
      try { git(repo, 'worktree', 'remove', '--force', worktree); } catch {}
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime refuses to replay a tool call whose persisted intent has no result', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-session-'));
    let requests = 0;
    const runtime = new GroupAgentRuntime({ bridgeHome: root, request: async () => { requests++; return { text: 'unexpected', toolCalls: [], finishReason: 'stop' }; } });
    fs.mkdirSync(path.join(root, 'workspace'));
    runtime.sessions.append('session_unknown_1', { role: 'intent', callId: 'side-effect-1', name: 'run_command', arguments: { command: 'touch file' } });
    try {
      await assert.rejects(runtime.run('session_unknown_1', path.join(root, 'workspace'), 'resume'));
      assert.equal(requests, 0);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
});

test('GroupAgentRuntime waits for the real command child to close before reporting cancellation', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'mesh-group-cancel-'));
    const cwd = path.join(root, 'workspace');
    fs.mkdirSync(cwd);
    const childPids: Array<number | undefined> = [];
    const runtime = new GroupAgentRuntime({
      bridgeHome: root,
      onChildPid: (pid) => childPids.push(pid),
      request: async () => ({
        text: '', finishReason: 'tool_calls', toolCalls: [{
          id: 'long-command', name: 'run_command',
          arguments: { command: process.platform === 'win32' ? 'Start-Sleep -Seconds 30' : 'sleep 30' },
        }],
      }),
    });
    const aborter = new AbortController();
    const running = runtime.run('session_cancel_123', cwd, 'run and cancel', aborter.signal);
    const timer = setTimeout(() => aborter.abort(), 200);
    try {
      await assert.rejects(running, /cancelled/i);
      assert.ok(childPids.some((pid) => typeof pid === 'number'));
      assert.equal(childPids.at(-1), undefined);
    } finally {
      clearTimeout(timer);
      fs.rmSync(root, { recursive: true, force: true });
    }
});
