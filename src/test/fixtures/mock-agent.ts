import * as assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as crypto from 'node:crypto';
import { spawn } from 'node:child_process';
import type { ExecutionSubtreeEvidence } from '../../bridge/types';
import { MockMcpClient, MockScenario, event, fixtureRoot } from './mock-agent-protocol';

const root = fixtureRoot();
const scenario: MockScenario = JSON.parse(fs.readFileSync(path.join(root, 'mock-only.json'), 'utf8'));
const args = process.argv.slice(2);
const exec = args.indexOf('exec');
const resume = args[exec + 1] === 'resume';
const sessionId = resume ? args[exec + 2] : crypto.randomUUID();
const prompt = args[exec + (resume ? 3 : 1)];
const bootstrap = process.env.MESH_REVIEW_BOOTSTRAP === '1';
const worker = process.env.MESH_TASK_RECEIVER === '1';
const threadId = process.env.MESH_TASK_THREAD_ID || process.env.MESH_REVIEW_THREAD_ID;
const model = args.find(arg => arg.startsWith('model='));
const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
const emit = (value: unknown) => process.stdout.write(JSON.stringify(value) + '\n');
async function waitForDurableBinding(): Promise<{ runId: string; jobId?: string }> {
  const file = path.join(root, 'home', '.agent-mesh-bridge', 'threads', `${threadId}.json`);
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    try {
      const task = JSON.parse(fs.readFileSync(file, 'utf8'));
      const child = task.execution?.run?.child;
      const job = task.execution?.run?.subtrees?.find((tree: ExecutionSubtreeEvidence) =>
        tree.root.pid === process.ppid)?.windowsJob;
      if (task.execution?.run?.state === 'active' && (child?.pid === process.pid || (job && child?.pid === process.ppid)) &&
          typeof child.birthId === 'string') return { runId: task.execution.run.id, jobId: job?.id };
    } catch { /* the ledger is atomically replaced during launch binding */ }
    await delay(20);
  }
  throw new Error(`Timed out waiting for durable v2 child binding for ${threadId}`);
}
function answer(text: string): void {
  emit({ type: 'item.completed', item: { type: 'agent_message', text } });
  emit({ type: 'turn.completed', usage: { input_tokens: 20, cached_input_tokens: 5, output_tokens: 10 } });
}

async function main(): Promise<void> {
  assert.ok(Number.isInteger(scenario.rounds) && scenario.rounds >= 1 && scenario.rounds <= 5);
  assert.ok((scenario.reviewerTailMs || 0) >= 0 && (scenario.reviewerTailMs || 0) <= 2000);
  assert.ok(exec >= 0 && prompt, 'Expected the production Codex command shape');
  assert.equal(args[args.indexOf('--ask-for-approval') + 1], 'never');
  assert.ok(args.indexOf('--ask-for-approval') < exec);
  assert.ok(threadId);
  emit({ type: 'thread.started', thread_id: sessionId });
  const { runId, jobId } = await waitForDurableBinding();
  const role = bootstrap ? 'bootstrap' : worker ? 'worker' : 'reviewer';
  const round = Number(process.env.MESH_TASK_ROUND || 0);
  event(root, 'agent.start', { launchId: process.env.MESH_MOCK_LAUNCH_ID, role, round, threadId, sessionId, resume, runId, jobId, model, cwd: process.cwd() });
  if (bootstrap && scenario.bootstrapProcessChurn) {
    // A short-lived child can disappear between process enumeration and identity reads.
    for (let index = 0; index < 10; index++) {
      spawn(process.execPath, ['-e', 'process.exit(0)'], { stdio: 'ignore', windowsHide: true }).unref();
    }
    const background = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'],
      { detached: true, stdio: 'ignore', windowsHide: true });
    background.unref();
    event(root, 'bootstrap.process-churn', { backgroundPid: background.pid, launchId: process.env.MESH_MOCK_LAUNCH_ID });
  }
  const pauseStage = bootstrap ? 'bootstrap' : worker ? 'develop' : 'review';
  const credential = bootstrap ? /^(\{"reviewBootstrap":\s*\{[^\n]+\}\})$/m.exec(prompt) : undefined;
  const bootstrapToken = credential ? JSON.parse(credential[1]).reviewBootstrap.token : undefined;
  if (bootstrapToken) event(root, 'bootstrap.prompt', { threadId, sessionId, bootstrapToken, resume });
  const shouldPause = scenario.pauseAt === pauseStage && (scenario.pauseRound === undefined || scenario.pauseRound === round);
  const pauseMarker = path.join(root, `${pauseStage}-${round}.pause-marker`);
  if (shouldPause && !fs.existsSync(pauseMarker)) {
    fs.writeFileSync(pauseMarker, 'held');
    event(root, 'agent.pause-ready', { launchId: process.env.MESH_MOCK_LAUNCH_ID, role, round, threadId, sessionId, bootstrapToken });
    await new Promise<void>((resolve, reject) => {
      const poll = setInterval(() => {
        if (fs.existsSync(path.join(root, `${pauseStage}-${round}.release`))) {
          clearInterval(poll); clearTimeout(deadline); resolve();
        }
      }, 20);
      const deadline = setTimeout(() => { clearInterval(poll); reject(new Error(`Timed out at ${pauseStage} barrier`)); }, 120_000);
    });
  }
  // Give the real launcher time to attach its process identity even for faults.
  await delay(150);
  if (worker && scenario.fault === 'empty-worker') return;
  if (worker && scenario.fault === 'nonzero-worker') {
    answer('Controlled worker failure');
    process.exitCode = 7;
    return;
  }

  const mcp = new MockMcpClient(process.env, process.cwd());
  try {
    await mcp.initialize();
    if (bootstrap) {
      assert.ok(credential, 'Bootstrap credential must come from the real prompt');
      const payload = JSON.parse(credential[1]);
      if (scenario.fault === 'bad-bootstrap-token') payload.reviewBootstrap.token = 'invalid-test-token';
      await mcp.tool('dispatch_task', payload);
      event(root, 'bootstrap.confirmed', { threadId, sessionId });
      answer('Mock reviewer confirmed bootstrap');
    } else if (worker) {
      const tools = await mcp.rpc('tools/list');
      assert.ok(!tools.tools.some((tool: any) => tool.name === 'dispatch_task'));
      await assert.rejects(mcp.tool('dispatch_task', { title: 'Forbidden forwarding', prompt: 'must not dispatch' }), /receiver|forward/i);
      await assert.rejects(mcp.tool('request_rework', { threadId, feedback: 'must not review itself' }), /receiver/i);
      event(root, 'receiver.guards', { threadId, round });
      const task = await mcp.tool('get_thread_details', { threadId });
      const thread = task.thread || task;
      if (resume) assert.equal(thread.receiverSessionId, sessionId);
      fs.writeFileSync(path.join(process.cwd(), 'result.txt'), `round ${round}\n`);
      answer(`Worker completed round ${round}`);
    } else {
      assert.ok(resume, 'Reviewer must resume its bootstrap session');
      const task = await mcp.tool('get_thread_details', { threadId });
      const thread = task.thread || task;
      assert.equal(thread.reviewerSessionId, sessionId);
      assert.equal(fs.readFileSync(path.join(process.cwd(), 'result.txt'), 'utf8'), `round ${thread.currentRound}\n`);
      if (scenario.fault !== 'no-verdict') {
        const tool = thread.currentRound < scenario.rounds ? 'request_rework' : 'approve_task';
        const payload = tool === 'request_rework'
          ? { threadId, feedback: `Implement revision ${thread.currentRound + 1} of result.txt` }
          : { threadId, remarks: 'Mock reviewer verified the final file' };
        await mcp.tool(tool, payload);
        event(root, 'reviewer.verdict', { threadId, round: thread.currentRound, tool, sessionId });
      }
      answer('Mock reviewer finished its turn');
      await delay(scenario.reviewerTailMs || 0);
    }
  } finally { await mcp.close(); }
}

main().catch(error => {
  console.error(error.stack || String(error));
  event(root, 'agent.error', { threadId, error: String(error) });
  process.exitCode = 9;
});
