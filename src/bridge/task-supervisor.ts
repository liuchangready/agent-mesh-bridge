import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { EventEmitter } from 'events';
import { TaskStore } from './task-store';
import { AutonomousRunner, RunResult } from './autonomous-runner';
import { CallbackManager } from './callback-manager';
import { AgentDetector } from './agent-detector';
import { DelegationThread, DelegationRound } from './types';

export class TaskSupervisor extends EventEmitter {
  private taskStore: TaskStore;
  private runner: AutonomousRunner;
  private callbackManager: CallbackManager;
  private detector?: AgentDetector;
  private activeTasks: Map<string, Promise<void>> = new Map();
  private lockFilePath: string;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private pollInterval: NodeJS.Timeout | null = null;
  private isDisposed: boolean = false;

  constructor(
    taskStore: TaskStore,
    runner: AutonomousRunner,
    callbackManager: CallbackManager,
    detector?: AgentDetector,
  ) {
    super();
    this.taskStore = taskStore;
    this.runner = runner;
    this.callbackManager = callbackManager;
    this.detector = detector;

    const storageDir = path.join(os.homedir(), '.agent-mesh-bridge');
    if (!fs.existsSync(storageDir)) {
      try {
        fs.mkdirSync(storageDir, { recursive: true });
      } catch {}
    }
    this.lockFilePath = path.join(storageDir, 'supervisor.json');

    this.startHeartbeat();
    this.startWatching();
  }

  /**
   * Checks if an external supervisor instance (e.g. VS Code extension host) is actively running.
   */
  public static isSupervisorActive(): boolean {
    const lockFile = path.join(os.homedir(), '.agent-mesh-bridge', 'supervisor.json');
    if (!fs.existsSync(lockFile)) return false;

    try {
      const data = JSON.parse(fs.readFileSync(lockFile, 'utf8'));
      if (!data || !data.updatedAt || !data.pid) return false;

      // Check if heartbeat is fresher than 20 seconds
      const isFresh = Date.now() - data.updatedAt < 20000;
      if (!isFresh) return false;

      // Check if process with that pid is alive
      try {
        process.kill(data.pid, 0);
        return true;
      } catch {
        return false;
      }
    } catch {
      return false;
    }
  }

  private startHeartbeat(): void {
    const updateLock = () => {
      if (this.isDisposed) return;
      try {
        fs.writeFileSync(
          this.lockFilePath,
          JSON.stringify({ pid: process.pid, updatedAt: Date.now() }, null, 2),
          'utf8',
        );
      } catch {}
    };

    updateLock();
    this.heartbeatInterval = setInterval(updateLock, 5000);
  }

  private startWatching(): void {
    // Listen to TaskStore changes
    this.taskStore.on('changed', () => {
      this.checkPendingTasks();
    });

    // Periodic watchdog poll (every 3s) to ensure fast autonomous reaction
    this.pollInterval = setInterval(() => {
      this.checkPendingTasks();
    }, 3000);

    // Initial check on startup
    setTimeout(() => {
      this.checkPendingTasks();
    }, 1000);
  }

  /**
   * Scans taskStore for any thread in 'running' or 'reworking' state that is not currently being executed.
   */
  public checkPendingTasks(): void {
    if (this.isDisposed) return;

    this.taskStore.loadFromDisk();
    const allTasks = this.taskStore.getAllTasks();

    for (const thread of allTasks) {
      if (this.activeTasks.has(thread.threadId)) {
        continue;
      }

      const runningRounds = (thread.rounds || []).filter(
        (r: DelegationRound) => r.status === 'running',
      );

      if (thread.status === 'running' || thread.status === 'reworking' || runningRounds.length > 0) {
        // Prefer currentRound if running, else latest running round
        const targetRound =
          thread.rounds.find(
            (r: DelegationRound) => r.round === thread.currentRound && r.status === 'running',
          ) || runningRounds[runningRounds.length - 1];

        if (targetRound && targetRound.prompt) {
          console.log(
            `[TaskSupervisor] Found unhandled pending task: ${thread.threadId} (Round ${targetRound.round}, status: ${thread.status}). Starting execution...`,
          );
          this.executeThreadRound(thread, targetRound);
        }
      }
    }
  }

  /**
   * Executes a thread round autonomously within the persistent supervisor process.
   */
  public executeThreadRound(thread: DelegationThread, round: DelegationRound): Promise<void> {
    if (this.activeTasks.has(thread.threadId)) {
      return this.activeTasks.get(thread.threadId)!;
    }

    const promise = (async () => {
      const isRework = round.type === 'rework';
      const roundNumber = round.round;

      console.log(
        `[TaskSupervisor] Executing ${thread.threadId} Round ${roundNumber} on ${thread.targetAgent} (isRework: ${isRework})...`,
      );

      this.emit('taskStarted', { thread, round });

      let result: RunResult;
      try {
        result = await this.runner.execute({
          agent: thread.targetAgent,
          prompt: round.prompt,
          isRework,
          receiverSessionId: thread.receiverSessionId,
          title: thread.title,
          cwd: thread.workspacePath,
        });
      } catch (err: any) {
        result = {
          exitCode: -1,
          stdout: '',
          stderr: String(err),
          extractedOutput: '',
          durationMs: 0,
          error: `Execution error: ${err.message || err}`,
        };
      }

      // Re-read latest task from disk to avoid overwriting newer state
      this.taskStore.loadFromDisk();
      const currentThread = this.taskStore.getTask(thread.threadId) || thread;

      const isSuccess = result.exitCode === 0;
      const finalRoundStatus = isSuccess ? 'completed' : 'failed';
      const finalThreadStatus = isSuccess ? 'waiting_review' : 'failed';
      const nativeSessionId = result.actualSessionId || currentThread.receiverSessionId;

      if (result.actualSessionId && result.actualSessionId !== currentThread.receiverSessionId) {
        console.log(
          `[TaskSupervisor] Updating receiverSessionId from ${currentThread.receiverSessionId} to ${result.actualSessionId}`,
        );
        currentThread.receiverSessionId = result.actualSessionId;
      }

      console.log(
        `[TaskSupervisor] ${thread.threadId} Round ${roundNumber} completed with exitCode ${result.exitCode}. Updating store...`,
      );

      const isLatestRound = !currentThread.currentRound || roundNumber >= currentThread.currentRound;
      const threadStatusToApply = isLatestRound ? finalThreadStatus : currentThread.status;

      const updatedThread = this.taskStore.updateRound(
        thread.threadId,
        roundNumber,
        {
          output: result.extractedOutput || result.stdout,
          error: result.error,
          durationMs: result.durationMs,
          status: finalRoundStatus,
        },
        threadStatusToApply,
        nativeSessionId,
      );

      this.activeTasks.delete(thread.threadId);

      if (updatedThread) {
        // Check if a subsequent round has already been queued while this round was executing
        const nextPendingRound = (updatedThread.rounds || []).find(
          (r: DelegationRound) => r.round > roundNumber && r.status === 'running',
        );

        if (nextPendingRound && nextPendingRound.prompt) {
          console.log(
            `[TaskSupervisor] Found subsequent pending round ${nextPendingRound.round} for ${thread.threadId}. Executing immediately...`,
          );
          this.executeThreadRound(updatedThread, nextPendingRound);
          return;
        }

        const completedRound = updatedThread.rounds.find((r) => r.round === roundNumber);
        if (completedRound) {
          this.emit('taskCompleted', { thread: updatedThread, round: completedRound, result });

          if (isSuccess && isLatestRound) {
            console.log(
              `[TaskSupervisor] Round ${roundNumber} successful and is latest. Notifying caller ${updatedThread.initiatorAgent}...`,
            );
            await this.callbackManager.notifyCaller(updatedThread, completedRound);
          }
        }
      }
    })();

    this.activeTasks.set(thread.threadId, promise);
    return promise;
  }

  public isExecuting(threadId: string): boolean {
    return this.activeTasks.has(threadId);
  }

  public dispose(): void {
    this.isDisposed = true;
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
      this.pollInterval = null;
    }
    try {
      if (fs.existsSync(this.lockFilePath)) {
        fs.unlinkSync(this.lockFilePath);
      }
    } catch {}
  }
}
