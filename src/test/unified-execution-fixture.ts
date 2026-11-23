import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';

import { TaskStore } from '../bridge/task-store';
import {
  ExecutionController,
  ExecutionControllerOptions,
  DEFAULT_EXECUTION_BUDGET_MS,
  AUTO_RETRY_BACKOFFS_MS,
  MAX_AUTO_RETRIES,
} from '../bridge/execution-controller';
import { TaskExecutionRegistry } from '../bridge/task-execution-registry';
import { TaskRoleLock } from '../bridge/task-role-lock';
import { NativeSessionLock } from '../bridge/native-session-lock';
import { MeshFileLock } from '../bridge/mesh-file-lock';
import { AgentDetector } from '../bridge/agent-detector';
import { AutonomousRunner, RunResult } from '../bridge/autonomous-runner';
import { CallbackManager } from '../bridge/callback-manager';
import {
  DelegationRound,
  DelegationThread,
  ExecutionControl,
  ExecutionPhase,
} from '../bridge/types';
import { testWorkspace } from './workspace-fixture';

export class FastInMemoryLock {
  private static locks = new Map<string, Promise<void>>();
  private held = false;
  private releaseFn?: () => void;
  constructor(public readonly lockPath: string) {}
  async acquire(_onWait?: () => void, _cancelled?: () => boolean, _timeoutMs?: number): Promise<(() => void) | undefined> {
    while (FastInMemoryLock.locks.has(this.lockPath)) {
      await FastInMemoryLock.locks.get(this.lockPath);
    }
    let release!: () => void;
    FastInMemoryLock.locks.set(this.lockPath, new Promise<void>((r) => { release = r; }));
    this.held = true;
    this.releaseFn = () => {
      if (this.held) {
        this.held = false;
        FastInMemoryLock.locks.delete(this.lockPath);
        release();
      }
    };
    return this.releaseFn;
  }
  dispose(): void {
    if (this.held && this.releaseFn) {
      this.releaseFn();
    }
  }
}

export function createMockRunner() {
  const executions: any[] = [];
  let nextResult: Partial<RunResult> | undefined;
  let onExecute: ((options: any) => Promise<Partial<RunResult> | void>) | undefined;

  return {
    executions,
    setNextResult: (res: Partial<RunResult>) => { nextResult = res; },
    setOnExecute: (fn: (options: any) => Promise<Partial<RunResult> | void>) => { onExecute = fn; },
    clearPendingCancellation: (_id: string) => undefined,
    cancelThread: (_id: string, _action: string, _reason: string) => true,
    isTransientError: (result: RunResult) => {
      const err = (result.error || '').toLowerCase();
      return err.includes('connection error') || err.includes('timeout') || err.includes('network');
    },
    isRateLimitOrQuotaError: (text: string) => /rate[-_\s]*limit|quota/i.test(text),
    execute: async (options: any): Promise<RunResult> => {
      executions.push(options);
      if (options.onChildPid) {
        options.onChildPid(777777);
      }
      if (onExecute) {
        const custom = await onExecute(options);
        if (custom) {
          return {
            exitCode: custom.exitCode ?? 0,
            stdout: custom.stdout ?? 'output',
            stderr: custom.stderr ?? '',
            extractedOutput: custom.extractedOutput ?? 'output',
            actualSessionId: custom.actualSessionId,
            durationMs: custom.durationMs ?? 10,
            error: custom.error,
            assessment: custom.assessment ?? {
              success: (custom.exitCode ?? 0) === 0,
              errorCode: (custom.exitCode ?? 0) === 0 ? 'OK' : 'NONZERO_EXIT',
              exitCode: custom.exitCode ?? 0,
              stream: {
                chunkCount: 1,
                stdoutBytes: 10,
                stderrBytes: 0,
                hasActualOutput: true,
                continuous: true,
                gapExceeded: false,
                maxGapMs: 0,
                gapThresholdMs: 60000,
              },
            },
          };
        }
      }
      const res = nextResult || {};
      const exitCode = res.exitCode ?? 0;
      return {
        exitCode,
        stdout: res.stdout ?? 'output',
        stderr: res.stderr ?? '',
        extractedOutput: res.extractedOutput ?? 'extracted output',
        durationMs: res.durationMs ?? 10,
        error: res.error,
        assessment: res.assessment ?? {
          success: exitCode === 0,
          errorCode: exitCode === 0 ? 'OK' : 'NONZERO_EXIT',
          exitCode,
          stream: {
            chunkCount: 1,
            stdoutBytes: 10,
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
}

export function createThread(id: string, phase: ExecutionPhase = 'develop'): DelegationThread {
  const now = new Date().toISOString();
  return {
    threadId: id,
    title: `Task ${id}`,
    initiatorAgent: 'codex',
    targetAgent: 'claude_code',
    status: 'queued',
    currentRound: 1,
    createdAt: now,
    updatedAt: now,
    receiverSessionId: `session-${id}`,
    receiverSessionCaptured: true,
    reviewerSessionId: `rev-session-${id}`,
    callerSessionId: `rev-session-${id}`,
    reviewerSessionCaptured: true,
    workspace: testWorkspace(os.tmpdir()),
    reviewFlow: 'dedicated',
    reviewBootstrap: { phase: 'confirmed', attempts: 1 },
    execution: {
      version: 1,
      revision: 1,
      operationId: `op_${id}`,
      phase,
      round: 1,
      generation: 1,
      attempt: 0,
      autoRetriesUsed: 0,
      budget: {
        limitMs: DEFAULT_EXECUTION_BUDGET_MS,
        spentActiveMs: 0,
        maxAutoRetries: MAX_AUTO_RETRIES,
      },
    },
    summary: {
      lastPromptSnippet: 'test',
      lastOutputSnippet: 'test',
      totalDurationMs: 0,
    },
    rounds: [
      {
        round: 1,
        type: 'initial',
        prompt: 'test prompt',
        status: 'running',
        durationMs: 0,
        timestamp: now,
      },
    ],
  };
}

export interface SetupFixtureOptions {
  useFastLock?: boolean;
  retryBackoffsMs?: number[];
  wallClockNow?: () => number;
  monotonicNow?: () => number;
  standingScanIntervalMs?: number;
}

export async function setupFixture(options?: SetupFixtureOptions) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'unified-exec-test-'));
  const store = new TaskStore(dir, options?.useFastLock ? { lockFactory: (f) => new FastInMemoryLock(f) } : undefined);
  const runner = createMockRunner();
  const detector = new AgentDetector();
  const callbackManager = new CallbackManager(detector, dir);
  callbackManager.setTaskStore(store);
  callbackManager.setAutonomousRunner(runner as any);
  callbackManager.launchCaller = (async () => ({ success: true, exitCode: 0 })) as any;

  const roleLock = new TaskRoleLock(dir);
  const sessionLock = new NativeSessionLock(dir);
  const registry = new TaskExecutionRegistry(dir);
  const mergeLock = new MeshFileLock(path.join(dir, 'merge.lock'));

  const ctrlOptions: ExecutionControllerOptions = {
    storageDir: dir,
    retryBackoffsMs: options?.retryBackoffsMs ?? [10_000, 30_000],
    wallClockNow: options?.wallClockNow,
    monotonicNow: options?.monotonicNow,
    standingScanIntervalMs: options?.standingScanIntervalMs ?? 0,
  };

  const controller = new ExecutionController(
    store,
    runner as any,
    callbackManager,
    undefined,
    roleLock,
    sessionLock,
    registry,
    ctrlOptions,
  );

  return {
    dir,
    store,
    runner,
    detector,
    callbackManager,
    roleLock,
    sessionLock,
    registry,
    mergeLock,
    controller,
    cleanup: async () => {
      await controller.waitForActiveExecutions(3000);
      controller.dispose();
      roleLock.dispose();
      registry.dispose();
      store.dispose();
      mergeLock.dispose();
      try {
        fs.rmSync(dir, { recursive: true, force: true });
      } catch {}
    },
  };
}
