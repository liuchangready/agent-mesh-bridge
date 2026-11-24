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
import { bindSpawnedChildIdentity, getProcessBirthId } from '../../bridge/platform';

const args = process.argv.slice(2);
let storageDir = '';
let threadId = '';
let workerId = '';
let barrierFile = '';
let holdFile = '';
let registeredFile = '';
let spawnedFile = '';
let registrationGateFile = '';
let spawnedBirthFile = '';
let recoveryPhase = '';
let scanOnly = false;

function trace(event: string): void {
  if (!registeredFile) return;
  try { fs.appendFileSync(`${registeredFile}.events`, `${Date.now()} ${process.pid} ${event}\n`, 'utf8'); } catch {}
}

for (const arg of args) {
  if (arg.startsWith('--storage-dir=')) storageDir = arg.split('=')[1];
  if (arg.startsWith('--thread-id=')) threadId = arg.split('=')[1];
  if (arg.startsWith('--worker-id=')) workerId = arg.split('=')[1];
  if (arg.startsWith('--barrier-file=')) barrierFile = arg.split('=')[1];
  if (arg.startsWith('--hold-file=')) holdFile = arg.split('=')[1];
  if (arg.startsWith('--registered-file=')) registeredFile = arg.split('=')[1];
  if (arg.startsWith('--spawned-file=')) spawnedFile = arg.split('=')[1];
  if (arg.startsWith('--registration-gate-file=')) registrationGateFile = arg.split('=')[1];
  if (arg.startsWith('--spawned-birth-file=')) spawnedBirthFile = arg.split('=')[1];
  if (arg.startsWith('--phase=')) recoveryPhase = arg.split('=')[1];
  if (arg === '--scan-only') scanOnly = true;
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

  const originalTransitionExecution = store.transitionExecution.bind(store);
  (store as any).transitionExecution = async (
    transitionThreadId: string,
    revision: number,
    change: (thread: any) => void,
    cleanupToken?: string,
    runExitProof?: any,
  ) => {
    const run = store.getTask(threadId)?.execution?.run;
    const isBinding = run?.step === 'binding';
    if (isBinding) trace(`binding-transition-enter:${run.id}`);
    try {
      const result = await originalTransitionExecution(transitionThreadId, revision, change, cleanupToken, runExitProof);
      if (isBinding) trace(`binding-transition-exit:${result.execution?.run?.step || 'cleared'}`);
      return result;
    } catch (error: any) {
      if (isBinding) trace(`binding-transition-error:${error?.message || error}`);
      throw error;
    }
  };

  const originalAttachChildPid = registry.attachChildPid.bind(registry);
  (registry as any).attachChildPid = async (pid: number | undefined, registration: any) => {
    trace(`registry-attach-enter:${pid || 'clear'}`);
    try {
      const result = await originalAttachChildPid(pid, registration);
      trace(`registry-attach-exit:${pid || 'clear'}`);
      return result;
    } catch (error: any) {
      trace(`registry-attach-error:${error?.message || error}`);
      throw error;
    }
  };

  // Mock runner that spawns a real controlled node child process if execute is reached
  const mockRunner: any = {
    clearPendingCancellation: () => undefined,
    cancelThread: () => true,
    isTransientError: () => false,
    isRateLimitOrQuotaError: () => false,
    execute: async (opts: any) => {
      spawnCount++;
      trace('runner-execute-enter');
      // The optional file barrier keeps the winner in starting/binding until
      // the competing control process has attempted its start.
      const child = spawn(process.execPath, [
        '-e',
        holdFile
          ? `const fs=require('fs'); const p=${JSON.stringify(holdFile)}; const t=setInterval(()=>{if(fs.existsSync(p)){clearInterval(t);process.exit(0)}},20);setTimeout(()=>{clearInterval(t);process.exit(0)},60000);`
          : 'setTimeout(()=>{}, 3000)',
      ], {
        stdio: ['ignore', 'ignore', 'ignore'],
        windowsHide: true,
      });
      spawnedSubprocessPid = child.pid ?? null;
      spawnedSubprocess = child;
      if (spawnedFile) fs.writeFileSync(spawnedFile, String(child.pid), 'utf8');

      if (registrationGateFile) {
        fs.writeFileSync(`${registrationGateFile}.entered`, 'waiting', 'utf8');
        const deadline = Date.now() + 60_000;
        while (!fs.existsSync(registrationGateFile) && Date.now() < deadline) {
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
        if (!fs.existsSync(registrationGateFile)) throw new Error('Timed out waiting for the registration gate');
      }

      if (opts.onChildPid && child.pid) {
        trace(`child-callback-before:${child.pid}`);
        const childBirthId = getProcessBirthId(child.pid);
        bindSpawnedChildIdentity(child, childBirthId);
        if (spawnedBirthFile && childBirthId) fs.writeFileSync(spawnedBirthFile, childBirthId, 'utf8');
        await opts.onChildPid(child.pid, child, childBirthId);
        trace(`child-callback-after:${child.pid}`);
      }
      if (registeredFile) fs.writeFileSync(registeredFile, 'registered', 'utf8');
      trace('runner-child-registered');

      trace('runner-child-close-wait');
      await new Promise((resolve) => child.on('close', resolve));
      trace('runner-child-close');
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
    { storageDir, defaultBudgetLimitMs: 60_000, ...(registrationGateFile ? { streamSyncIntervalMs: 60_000 } : {}) },
  );

  if (scanOnly) {
    await controller.scanAndDrain();
    const saved = store.getTask(threadId);
    console.log('RESULT:' + JSON.stringify({
      workerId,
      scanned: true,
      runId: saved?.execution?.run?.id,
      revision: saved?.execution?.revision,
    }));
    controller.dispose();
    roleLock.dispose();
    registry.dispose();
    store.dispose();
    return;
  }

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
  const res = await controller.startExecution(threadId, recoveryPhase as any);
  trace(`controller-start-return:${res.started ? 'started' : res.reason || 'not-started'}`);

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
    controlPid: process.pid,
  }));

  try { controller.dispose(); } catch {}
  process.exit(0);
}

main().catch((err) => {
  console.error(JSON.stringify({ workerId, error: err.message, stack: err.stack }));
  process.exit(1);
});
