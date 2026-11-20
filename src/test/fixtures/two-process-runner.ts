// Worker script for two-process concurrent run test.
// Launched as a separate OS process via child_process.spawn/fork.

import * as fs from 'fs';
import * as path from 'path';
import { spawn, ChildProcess } from 'child_process';

import { TaskStore } from '../../bridge/task-store';
import { ExecutionController } from '../../bridge/execution-controller';
import { TaskExecutionRegistry } from '../../bridge/task-execution-registry';
import { TaskRoleLock } from '../../bridge/task-role-lock';
import { NativeSessionLock } from '../../bridge/native-session-lock';
import { getProcessBirthId } from '../../bridge/platform';

const args = process.argv.slice(2);
let storageDir = '';
let threadId = '';
let workerId = '';
let barrierFile = '';
let holdFile = '';
let registeredFile = '';
let spawnedFile = '';

for (const arg of args) {
  if (arg.startsWith('--storage-dir=')) storageDir = arg.split('=')[1];
  if (arg.startsWith('--thread-id=')) threadId = arg.split('=')[1];
  if (arg.startsWith('--worker-id=')) workerId = arg.split('=')[1];
  if (arg.startsWith('--barrier-file=')) barrierFile = arg.split('=')[1];
  if (arg.startsWith('--hold-file=')) holdFile = arg.split('=')[1];
  if (arg.startsWith('--registered-file=')) registeredFile = arg.split('=')[1];
  if (arg.startsWith('--spawned-file=')) spawnedFile = arg.split('=')[1];
}

if (!storageDir || !threadId) {
  console.error(JSON.stringify({ error: 'Missing required arguments' }));
  process.exit(1);
}

async function main() {
  const store = new TaskStore(storageDir);
  const registry = new TaskExecutionRegistry(path.join(storageDir, 'registry'));
  const roleLock = new TaskRoleLock(storageDir);
  const sessionLock = new NativeSessionLock(storageDir);

  let spawnedSubprocessPid: number | null = null;
  let spawnCount = 0;
  let spawnedSubprocess: ChildProcess | null = null;
  let resolveRunnerDone: (() => void) | undefined;
  const runnerDone = new Promise<void>((resolve) => { resolveRunnerDone = resolve; });

  // Mock runner that spawns a real controlled node child process if execute is reached
  const mockRunner: any = {
    clearPendingCancellation: () => undefined,
    cancelThread: () => true,
    isTransientError: () => false,
    isRateLimitOrQuotaError: () => false,
    execute: async (opts: any) => {
      spawnCount++;
      // The optional file barrier keeps the winner in starting/binding until
      // the competing control process has attempted its start.
      const child = spawn(process.execPath, [
        '-e',
        holdFile
          ? `const fs=require('fs'); const p=${JSON.stringify(holdFile)}; const t=setInterval(()=>{if(fs.existsSync(p)){clearInterval(t);process.exit(0)}},20);`
          : 'setTimeout(()=>{}, 3000)',
      ], {
        stdio: ['ignore', 'ignore', 'ignore'],
        windowsHide: true,
      });
      spawnedSubprocessPid = child.pid ?? null;
      spawnedSubprocess = child;
      if (spawnedFile) fs.writeFileSync(spawnedFile, String(child.pid), 'utf8');

      if (opts.onChildPid && child.pid) {
        const childBirthId = getProcessBirthId(child.pid);
        await opts.onChildPid(child.pid, child, childBirthId);
      }
      if (registeredFile) fs.writeFileSync(registeredFile, 'registered', 'utf8');

      await new Promise((resolve) => child.on('close', resolve));
      resolveRunnerDone?.();
      return {
        exitCode: 0,
        stdout: 'controlled_worker_output',
        stderr: '',
        extractedOutput: 'controlled_worker_output',
        durationMs: 100,
        assessment: {
          success: true,
          errorCode: 'OK',
          exitCode: 0,
          stream: {
            chunkCount: 1,
            stdoutBytes: 24,
            stderrBytes: 0,
            hasActualOutput: true,
            continuous: true,
            gapExceeded: false,
            maxGapMs: 0,
            gapThresholdMs: 60000,
          },
        },
      };
    },
  };

  const mockCallbackManager: any = {
    launchCaller: async () => ({ success: true }),
    launchReviewerBootstrap: async () => ({ confirmed: true }),
  };

  const controller = new ExecutionController(
    store,
    mockRunner,
    mockCallbackManager,
    undefined,
    roleLock,
    sessionLock,
    registry,
    { storageDir, defaultBudgetLimitMs: 60_000 },
  );

  // Synchronize on barrier file if provided
  if (barrierFile) {
    const readyFile = `${barrierFile}.${workerId}.ready`;
    fs.writeFileSync(readyFile, 'ready', 'utf8');

    // Wait until start signal is written to barrierFile
    const deadline = Date.now() + 10_000;
    while (!fs.existsSync(barrierFile) && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  // Attempt to start execution
  const res = await controller.startExecution(threadId);

  // startExecution is intentionally non-blocking; keep this control process
  // alive until its background round has settled when it owns the run.
  if (res.started) await runnerDone;

  // Clean up spawned subprocess if still running
  if (spawnedSubprocess && !(spawnedSubprocess as ChildProcess).killed) {
    try { (spawnedSubprocess as ChildProcess).kill(); } catch {}
  }

  // Print structured JSON result to stdout for parent test to read
  console.log('RESULT:' + JSON.stringify({
    workerId,
    started: res.started,
    reason: res.reason,
    runId: res.run?.id,
    spawnCount,
    childPid: spawnedSubprocessPid,
  }));

  try { controller.dispose(); } catch {}
  process.exit(0);
}

main().catch((err) => {
  console.error(JSON.stringify({ workerId, error: err.message, stack: err.stack }));
  process.exit(1);
});
