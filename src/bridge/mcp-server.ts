import * as readline from 'readline';
import * as fs from 'fs';
import * as path from 'path';
import { AsyncDispatcher } from './async-dispatcher';
import { ReworkController } from './rework-controller';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner } from './autonomous-runner';
import { TaskSupervisor } from './task-supervisor';
import { TaskControlService } from './task-control';
import { TaskRetargetService } from './task-retarget';
import { CallerPollGuard, progressSignature } from './caller-poll-guard';
import { TaskRoleLock } from './task-role-lock';
import { ModelCatalog, annotateConfiguredModel, loadModelDefaults } from './model-catalog';
import { resolveDispatchModel } from './model-approval';
import {
  AgentId,
  DelegationRound,
  MESH_TASK_THREAD_ENV,
  getModelRouting,
  isTaskReceiverProcess,
} from './types';
import {
  DEFAULT_RAW_LOG_BYTES,
  MAX_RAW_LOG_BYTES,
  readTextFilePaged,
  resolveExistingArtifactPath,
  roundExecutionLogPath,
  roundOutputArchivePath,
} from './round-log';
import { listReviewerLogRounds, reviewerLogPath } from './reviewer-log';
import { aggregateThreadTokenUsage } from './token-usage-collector';

/** Older non-compacted rounds are excerpted to this many characters. */
const DETAIL_HISTORY_OUTPUT_MAX_CHARS = 4_000;

/**
 * Tools that create new outbound work. A delegated task's receiver must never
 * use these: fanning work out again would make the mesh recursive and would
 * break the strict one-task-at-a-time guarantee.
 */
const FORWARDING_TOOLS = new Set([
  'dispatch_task',
  'invoke_agent',
  'invoke_antigravity',
  'invoke_claude_code',
  'invoke_codex',
]);

/** Control-plane tools a receiver must not apply to its own executing task. */
const CONTROL_TOOLS = new Set([
  'pause_task',
  'resume_task',
  'retry_task',
  'retarget_task',
  'allow_scope_overlap',
  'delete_task',
  'record_merge_request',
]);

export class MCPServer {
  private dispatcher: AsyncDispatcher;
  private reworkController: ReworkController;
  private taskStore: TaskStore;
  private detector: AgentDetector;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;
  private taskControl: TaskControlService;
  private modelCatalog: ModelCatalog;
  private retargetService: TaskRetargetService;
  /** Starves polling loops on tasks this process dispatched itself. */
  private pollGuard = new CallerPollGuard();

  constructor(
    dispatcher: AsyncDispatcher,
    reworkController: ReworkController,
    taskStore: TaskStore,
    detector: AgentDetector,
    resolver: ActiveAgentResolver,
    runner: AutonomousRunner,
  ) {
    this.dispatcher = dispatcher;
    this.reworkController = reworkController;
    this.taskStore = taskStore;
    this.detector = detector;
    this.resolver = resolver;
    this.runner = runner;
    this.taskControl = new TaskControlService(taskStore, runner);
    this.modelCatalog = new ModelCatalog(detector);
    this.retargetService = new TaskRetargetService(taskStore);
  }

  /** Directory holding the mirrored ledger; archives/logs live beside it. */
  private storeRoot(): string {
    try {
      return path.dirname(this.taskStore.getStoragePath());
    } catch {
      return '';
    }
  }

  private resolveRoundOutputArchive(threadId: string, round: number): string | undefined {
    return resolveExistingArtifactPath([
      roundOutputArchivePath(threadId, round, this.storeRoot()),
      roundOutputArchivePath(threadId, round),
    ]);
  }

  private resolveRoundExecutionLog(threadId: string, round: number): string | undefined {
    return resolveExistingArtifactPath([
      roundExecutionLogPath(threadId, round, this.storeRoot()),
      roundExecutionLogPath(threadId, round),
    ]);
  }

  /**
   * Bounded view of one round for `get_thread_details`.
   *
   * The latest round is returned verbatim (it is the reviewer's evidence).
   * An older round that was already condensed is returned as its card, and an
   * older round that predates compaction is excerpted with pointers instead of
   * shipping its whole body.
   */
  private projectRound(threadId: string, round: DelegationRound, isLatest: boolean) {
    if (isLatest || round.isCompacted) return round;
    const output = round.output || '';
    if (output.length <= DETAIL_HISTORY_OUTPUT_MAX_CHARS) return round;

    const archive = round.rawOutputFilePath || this.resolveRoundOutputArchive(threadId, round.round);
    const executionLog = this.resolveRoundExecutionLog(threadId, round.round);
    return {
      ...round,
      output: `${output.slice(0, DETAIL_HISTORY_OUTPUT_MAX_CHARS)}\n\n…（历史轮次已截断 ${
        output.length - DETAIL_HISTORY_OUTPUT_MAX_CHARS
      } 字符；用 get_round_raw_log 读取全文）`,
      outputTruncated: true,
      fullOutputChars: output.length,
      rawOutputPath: archive,
      executionLogPath: executionLog,
      recallHint: `get_round_raw_log(threadId="${threadId}", round=${round.round})`,
    };
  }

  /**
   * Verbatim read of one round, always bounded by `maxBytes`.
   *
   * `source: "output"` is the archived agent answer; `source: "execution"` is
   * the raw stdio log. Neither is ever dumped unbounded into a caller's context.
   */
  private readRoundRawLog(threadId: string, roundNumber: number, args: any) {
    const task = this.taskStore.getTask(threadId);
    if (!task) throw new Error(`Thread not found: ${threadId}`);
    const round = (task.rounds || []).find((entry) => entry.round === roundNumber);
    if (!round) {
      throw new Error(
        `Round ${roundNumber} not found on ${threadId}. ` +
          `Available rounds: ${(task.rounds || []).map((entry) => entry.round).join(', ') || 'none'}.`,
      );
    }

    const source = args?.source === 'execution' ? 'execution' : 'output';
    const maxBytes = Math.min(
      Math.max(1, Number(args?.maxBytes) || DEFAULT_RAW_LOG_BYTES),
      MAX_RAW_LOG_BYTES,
    );
    const offsetBytes = Math.max(0, Number(args?.offsetBytes) || 0);

    if (source === 'execution') {
      const logPath = this.resolveRoundExecutionLog(threadId, roundNumber);
      if (!logPath) {
        return {
          threadId,
          round: roundNumber,
          source,
          available: false,
          reason: 'no_execution_log',
          hint:
            'No stdio log exists for this round (it may predate per-round logging). ' +
            'Try source:"output" for the archived agent answer.',
        };
      }
      const page = readTextFilePaged(logPath, { offsetBytes, maxBytes });
      return {
        threadId,
        round: roundNumber,
        source,
        available: true,
        unit: 'bytes',
        ...page,
        returnedBytes: Buffer.byteLength(page.content, 'utf8'),
        isCompacted: round.isCompacted === true,
        hint:
          page.truncated
            ? `Paged: continue with offsetBytes=${page.nextOffset}.`
            : 'End of file.',
      };
    }

    const archivePath =
      round.rawOutputFilePath || this.resolveRoundOutputArchive(threadId, roundNumber);
    if (archivePath) {
      const page = readTextFilePaged(archivePath, { offsetBytes, maxBytes });
      return {
        threadId,
        round: roundNumber,
        source,
        available: true,
        unit: 'bytes',
        ...page,
        returnedBytes: Buffer.byteLength(page.content, 'utf8'),
        isCompacted: round.isCompacted === true,
        hint: page.truncated ? `Paged: continue with offsetBytes=${page.nextOffset}.` : 'End of file.',
      };
    }

    // Not archived yet: serve the ledger copy, but keep the same hard bound.
    const text = round.output || '';
    const start = Math.min(offsetBytes, text.length);
    const content = text.slice(start, start + maxBytes);
    const nextOffset = start + content.length;
    return {
      threadId,
      round: roundNumber,
      source,
      available: true,
      unit: 'characters',
      path: undefined,
      content,
      startOffset: start,
      nextOffset,
      totalBytes: Buffer.byteLength(text, 'utf8'),
      totalChars: text.length,
      returnedBytes: Buffer.byteLength(content, 'utf8'),
      truncated: nextOffset < text.length,
      isCompacted: round.isCompacted === true,
      hint:
        'This round is not archived yet; serving the ledger copy. ' +
        (nextOffset < text.length ? `Continue with offsetBytes=${nextOffset}.` : 'End of output.'),
    };
  }

  /**
   * Verbatim, bounded read of the reviewer side of one round.
   *
   * The reviewer is the caller's own process, so its stream is not part of the
   * round ledger. `CallbackManager` tees it into
   * `logs/<threadId>_round_<n>.reviewer.log`; this serves that artifact with
   * the same paging contract as `get_round_raw_log`.
   */
  private readReviewerLog(args: any) {
    const threadId: string = args?.threadId;
    const task = this.taskStore.getTask(threadId);
    if (!task) throw new Error(`Thread not found: ${threadId}`);

    const withLogs = [
      ...new Set([
        ...listReviewerLogRounds(threadId, this.storeRoot()),
        ...listReviewerLogRounds(threadId),
      ]),
    ].sort((a, b) => b - a);

    const requested = Number(args?.round);
    const round = Number.isFinite(requested) && requested >= 1 ? Math.floor(requested) : withLogs[0];
    if (!Number.isFinite(round) || round < 1) {
      return {
        threadId,
        available: false,
        reason: 'no_reviewer_log',
        roundsWithReviewerLog: [],
        message:
          'No reviewer log exists for this task yet: the bridge only records one while it wakes the caller ' +
          '(waiting_review), and rounds reviewed before this artifact existed have none. ' +
          'The reviewer verdict itself is still on the round ledger (approval remarks / next round feedback).',
      };
    }

    const maxBytes = Math.min(
      Math.max(1, Number(args?.maxBytes) || DEFAULT_RAW_LOG_BYTES),
      MAX_RAW_LOG_BYTES,
    );
    const offsetBytes = Math.max(0, Number(args?.offsetBytes) || 0);

    const found = [
      reviewerLogPath(threadId, round, this.storeRoot()),
      reviewerLogPath(threadId, round),
    ].find((candidate) => fs.existsSync(candidate));

    if (!found) {
      return {
        threadId,
        round,
        available: false,
        reason: 'no_reviewer_log_for_round',
        roundsWithReviewerLog: withLogs,
        message:
          `Round ${round} has no reviewer log on disk. ` +
          (withLogs.length
            ? `Rounds with a reviewer log: ${withLogs.join(', ')}.`
            : 'No round of this task has one.'),
      };
    }

    const page = readTextFilePaged(found, { offsetBytes, maxBytes });
    const latest = (task.rounds || []).reduce(
      (max, entry) => (entry.round > max ? entry.round : max),
      0,
    );
    return {
      threadId,
      round,
      available: true,
      unit: 'bytes',
      ...page,
      returnedBytes: Buffer.byteLength(page.content, 'utf8'),
      reviewerAgent: task.initiatorAgent,
      /** True while the caller is still reviewing this round. */
      live: task.status === 'waiting_review' && round === (task.currentRound || latest),
      roundsWithReviewerLog: withLogs,
      hint: page.truncated
        ? `Paged: continue with offsetBytes=${page.nextOffset}.`
        : 'End of reviewer log. The reviewer verdict lives on the ledger, not in this stream.',
    };
  }

  /**
   * Returns list of tools for MCP tools/list.
   */
  public getToolDefinitions() {
    const tools = [
      {
        name: 'dispatch_task',
        description:
          'Asynchronously delegate a new task to another agent (Codex, Claude Code, Antigravity, DeepSeek Harness) without blocking the current turn. ' +
          'After this call returns you MUST end your turn immediately and wait for the callback: the bridge wakes your native session when the round finishes, whether it succeeds or fails. ' +
          'Do NOT poll `get_thread_details` / `get_recent_logs` / `list_recent_tasks` to watch the task — that keeps the user paying for a turn that has nothing left to do, and repeated reads at unchanged progress are answered with a short `repeatPoll` stub instead of the details. ' +
          'A `QUEUED` result is normal and needs no action: the task starts on its own when the queue frees up. ' +
          'This tool is unavailable while this agent is itself executing a delegated task (receivers must not forward work).',
        inputSchema: {
          type: 'object',
          properties: {
            targetAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity', 'deepseek_harness'],
              description:
                'Target agent to execute the task. Defaults to "default" (auto-detected).',
            },
            initiatorAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity', 'deepseek_harness'],
              description:
                'Your agent identity (codex, claude_code, antigravity, deepseek_harness). Defaults to auto-detected caller.',
            },
            title: {
              type: 'string',
              description: 'Brief descriptive task title (e.g. "Fix TCP stream buffer leak")',
            },
            prompt: {
              type: 'string',
              description:
                'Detailed instructions, background, and acceptance criteria for the target agent.',
            },
            callerSessionId: {
              type: 'string',
              description:
                'Your native session UUID so the bridge can proactively wake you up upon completion. Do NOT pass agent names (e.g. "claude_code"). If omitted, the bridge automatically auto-detects your active session.',
            },
            workspacePath: {
              type: 'string',
              description: 'Workspace root path (defaults to current working directory).',
            },
            worktree: {
              type: 'object',
              description:
                'Declare that workspacePath is a TEMPORARY git worktree (created with `git worktree add`), not the shared checkout. ' +
                'The bridge then records the merge-back obligation: the task cannot be deleted until record_merge_request is called ' +
                '(delete_task force:true overrides). Omit this for the normal workspace.',
              properties: {
                mainPath: {
                  type: 'string',
                  description:
                    'Absolute path of the main checkout this worktree has to merge back into.',
                },
                branch: {
                  type: 'string',
                  description: 'Branch the worktree was created from (recorded for the merge audit).',
                },
                baseCommit: {
                  type: 'string',
                  description: 'Commit the worktree was created from (recorded for the merge audit).',
                },
              },
              required: ['mainPath'],
            },
            expectedFiles: {
              type: 'array',
              items: { type: 'string' },
              description:
                'Concrete code file paths this task will modify (e.g. ["src/service/Foo.ts", "lib/parser.ts"]). REQUIRED: the bridge refuses to start a task on a file another open task already declared or actually changed, and it cannot enforce that without your scope. Do NOT pass folders, trailing slashes, or glob patterns (rejected). Pass allowNoScope: true only if the task touches no workspace file.',
            },
            allowNoScope: {
              type: 'boolean',
              description:
                'Explicitly declare that this task modifies no workspace file, skipping the scope requirement. Omit when you pass expectedFiles.',
            },
            allowScopeOverlap: {
              type: 'boolean',
              description:
                'Accept running on top of files another open task already changed (use only when you know the two edits do not conflict, e.g. the earlier task was already committed).',
            },
            model: {
              type: 'string',
              description:
                'Receiver model, ONLY when the user explicitly named that model. Omit it in every other case: the task then runs on the model configured for that agent (list_agent_models returns it as `configuredModel`, first in the list). A value that differs from the configured default is ignored unless you also pass modelRequestedByUser: true, and even then the bridge asks the user to confirm the switch in VS Code before anything runs.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user themselves named the model in `model` (e.g. "use deepseek flash"). Defaults to false, in which case a non-default `model` is ignored and the configured default is used. With true, the bridge shows the user a confirmation dialog; approval switches the task, "keep default" falls back, and no answer within 3 minutes aborts the dispatch.',
            },
            initiatorModel: {
              type: 'string',
              description:
                'The model YOU (the caller) are currently running on, recorded as the task\'s sender model so the ledger shows "sender model -> receiver model". Omit to let the bridge infer it from the environment or your configured default.',
            },
          },
          required: ['title', 'prompt', 'expectedFiles'],
        },
      },
      {
        name: 'request_rework',
        description:
          'Request the target agent to rework or fix issues in the EXACT SAME native session with full context preserved. ' +
          'If THIS task is fine but a sibling task has not been delivered yet, pass blocked: true (with waitingForThreadId) instead of sending another rework: the task is parked without spending a round and is woken automatically when the dependency finishes. A rework whose text repeats the previous instruction after a round that changed no file is rejected as REWORK_NOOP_LOOP.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID from previous dispatch_task call.',
            },
            feedback: {
              type: 'string',
              description: 'Concrete critique, failed tests, edge-cases, or adjustments required.',
            },
            blocked: {
              type: 'boolean',
              description:
                'Park the review instead of creating a rework round: use it when the task itself passed but a dependency (another task, a human, an external system) is still missing. No round is created and no agent is started.',
            },
            waitingForThreadId: {
              type: 'string',
              description:
                'With blocked: true, the sibling thread that must reach completed/failed. The bridge wakes this review automatically when it does.',
            },
            callerSessionId: {
              type: 'string',
              description:
                'Your native session UUID for subsequent wake-up callback. Do NOT pass agent names (e.g. "claude_code"). If omitted, the bridge automatically auto-detects your active session.',
            },
            targetAgent: {
              type: 'string',
              description:
                'Optional: deliver THIS rework to a different agent. The task keeps its thread, history and file scope; the previous agent\'s native session cannot be resumed, so the round starts a fresh one.',
            },
            model: {
              type: 'string',
              description:
                'Optional: receiver model for this rework, only when the user explicitly named it. Omit to keep the task\'s pinned model (or the agent\'s configured default when you also switch targetAgent). A non-default model additionally requires modelRequestedByUser: true and the user\'s click in VS Code.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user named this model; the bridge then asks them to confirm the switch.',
            },
          },
          required: ['threadId', 'feedback'],
        },
      },
      {
        name: 'approve_task',
        description:
          'Approve the delegated task when satisfied, marking it completed and archiving the thread.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to approve.',
            },
            remarks: {
              type: 'string',
              description: 'Optional final verification notes or merge approval remarks.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'pause_task',
        description:
          'Pause a queued/running/reworking task. The current agent process is stopped, the round is preserved as paused, and the interruption reason is recorded.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to pause.',
            },
            reason: {
              type: 'string',
              description: 'Why the task is being paused (recorded as the interruption reason).',
            },
          },
          required: ['threadId', 'reason'],
        },
      },
      {
        name: 'resume_task',
        description:
          'Resume a paused task, preserving its original prompt and native session (or starting fresh when no native session was captured). Pass targetAgent/model to resume it on another agent or model instead.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to resume.',
            },
            targetAgent: {
              type: 'string',
              description:
                'Optional: resume on a different agent. The previous native session cannot follow a new agent, so the round starts a fresh one.',
            },
            model: {
              type: 'string',
              description:
                'Optional: receiver model on resume, only when the user explicitly named it. Non-default models require modelRequestedByUser: true and the user\'s confirmation.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user named this model; the bridge then asks them to confirm the switch.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'retry_task',
        description:
          'Retry a failed round in place: same round number, same prompt, same native session. Use this instead of request_rework when you simply want the receiver to run the failed instruction again — re-sending the original prompt as feedback creates a second round that looks like a duplicate task. Pass targetAgent/model to retry on another agent or model.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID whose latest failed round should be retried.',
            },
            targetAgent: {
              type: 'string',
              description:
                'Optional: retry on a different agent (fresh native session; the old session cannot be resumed by another agent).',
            },
            model: {
              type: 'string',
              description:
                'Optional: receiver model for the retry, only when the user explicitly named it. Non-default models require modelRequestedByUser: true and the user\'s confirmation.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user named this model; the bridge then asks them to confirm the switch.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'retarget_task',
        description:
          'Move an existing task to another executor: another agent and/or another model, keeping the same thread, round history and file scope. Works on queued, paused, failed and waiting_review tasks; refuses while a round is in flight (pause first) and on completed tasks. Switching the agent starts a fresh native session because the previous agent\'s session cannot be resumed. Model policy: omitted model keeps the current one (or uses the new agent\'s configured default); a non-default model requires modelRequestedByUser: true and the user\'s click consent in VS Code.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to switch.',
            },
            targetAgent: {
              type: 'string',
              enum: ['antigravity', 'codex', 'claude_code', 'deepseek_harness'],
              description: 'New receiver agent. Omit to keep the current agent and change only the model.',
            },
            model: {
              type: 'string',
              description:
                'New receiver model, only when the user explicitly named it. Omit to keep the current model (same agent) or the new agent\'s configured default.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user themselves named the model; the bridge then asks them to confirm the switch in VS Code.',
            },
            reason: {
              type: 'string',
              description: 'Why the executor is being switched (recorded on the task).',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'allow_scope_overlap',
        description:
          'Override the file-scope gate for a queued task: allow it to run even though another open task already changed some of the same files. Only use it when the overlap is genuinely safe (e.g. the earlier task was already committed or its change is unrelated).',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to unblock.',
            },
            reason: {
              type: 'string',
              description: 'Why the overlap is safe (recorded for audit).',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'delete_task',
        description:
          'Permanently delete a task. A running agent process is stopped first; the deletion reason is written to the tombstone log. ' +
          "The receiver's native conversation (Claude Code / Codex / Antigravity / DeepSeek Harness session files) is cleaned up as well — " +
          'best effort: a conversation that can no longer be found is not an error. The caller/initiator session is never deleted. ' +
          'A completed task that ran in a temporary worktree is REFUSED until its work is merged back: call record_merge_request first, or pass force: true to discard the worktree output deliberately.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to delete.',
            },
            reason: {
              type: 'string',
              description: 'Why the task is being deleted (recorded in the tombstone log).',
            },
            force: {
              type: 'boolean',
              description:
                'Delete even though the task has an unmerged worktree (its output will be lost). Omit to be refused until the merge request is recorded.',
            },
          },
          required: ['threadId', 'reason'],
        },
      },
      {
        name: 'record_merge_request',
        description:
          'Caller/reviewer only: record the merge-request handoff after a task or batch is accepted. The caller agent creates the MR and pushes to the remote; the bridge persists url/branch/commit/status for audit. The developer/receiver must not call this.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID whose changes are included in the MR.',
            },
            url: { type: 'string', description: 'Merge-request URL.' },
            branch: { type: 'string', description: 'Source branch pushed to the remote.' },
            commit: { type: 'string', description: 'Commit SHA included in the MR.' },
            remote: { type: 'string', description: 'Git remote name (default: origin).' },
            baseBranch: { type: 'string', description: 'Target/base branch.' },
            title: { type: 'string', description: 'MR title.' },
            status: {
              type: 'string',
              enum: ['created', 'pushed', 'synced', 'failed'],
              description: 'MR lifecycle status (default: synced).',
            },
            summary: { type: 'string', description: 'Short MR summary or failure reason.' },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'list_recent_tasks',
        description: 'Query recent delegation tasks from the 1,000 tasks persistent ledger.',
        inputSchema: {
          type: 'object',
          properties: {
            limit: {
              type: 'number',
              description: 'Number of recent tasks to return (default: 20, max: 1000).',
            },
            statusFilter: {
              type: 'string',
              enum: [
                'all',
                'active',
                'running',
                'waiting_review',
                'reworking',
                'paused',
                'completed',
                'failed',
              ],
              description: 'Filter by lifecycle status.',
            },
          },
        },
      },
      {
        name: 'get_thread_details',
        description:
          'Retrieve a task thread for review: all interaction rounds, prompts, outputs and diffs. ' +
          'The latest round is returned verbatim; older rounds that a later round has already superseded are condensed into "context capsule" cards with a pointer to their full archive, and any other oversized historical round is excerpted. ' +
          'Pass includeRawLogs: true only when you actually need the unbounded history and the 1,000-line execution log — it can be very large. ' +
          'For one specific round, prefer get_round_raw_log (bounded, paged). ' +
          'This is a review/inspection tool, not a progress watcher: after you dispatch a task, end your turn and wait for the callback instead of polling. ' +
          'Repeated reads of a task you dispatched at unchanged progress return a short `repeatPoll` stub with nextAction=END_TURN_AND_WAIT_FOR_CALLBACK.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'The task thread ID to fetch.',
            },
            includeRawLogs: {
              type: 'boolean',
              description:
                'Return every round verbatim plus the recent execution log. Defaults to false; keep it false unless the compacted view is genuinely insufficient.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'get_round_raw_log',
        description:
          'Read one round verbatim, bounded and paged: source "output" returns the archived final output (or the ledger copy before archiving), source "execution" returns the raw stdio log. ' +
          'Use this instead of dumping full history when you need to verify a specific earlier round. Defaults to 20,000 bytes; max 200,000 per call. ' +
          'Continue a large artifact by passing offsetBytes=nextOffset from the previous response.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: { type: 'string', description: 'Task thread ID.' },
            round: { type: 'number', description: 'Round number to read (1-based).' },
            source: {
              type: 'string',
              enum: ['output', 'execution'],
              description:
                'output = archived agent answer, execution = raw stdio log (default: output).',
            },
            offsetBytes: {
              type: 'number',
              description: 'Byte offset to start from (default 0).',
            },
            maxBytes: {
              type: 'number',
              description: `Bytes to return (default ${DEFAULT_RAW_LOG_BYTES}, max ${MAX_RAW_LOG_BYTES}).`,
            },
          },
          required: ['threadId', 'round'],
        },
      },
      {
        name: 'get_reviewer_log',
        description:
          'Read the REVIEWER (caller) side of one task round: the stdout/stderr the bridge captured while it woke the initiator agent in its native session. ' +
          'Receiver output lives in get_round_raw_log; this is the other half of a review, and it is the only place a reviewer\'s reasoning is available (the ledger stores its verdict, not its stream). ' +
          'Bounded and paged (default 20,000 bytes, max 200,000); pass round to pick a round (default: the newest round that has a reviewer log). ' +
          'A task whose reviewer never ran returns available=false instead of an empty string.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: { type: 'string', description: 'Task thread ID.' },
            round: {
              type: 'number',
              description: 'Round number to read (1-based). Omit for the newest reviewed round.',
            },
            offsetBytes: {
              type: 'number',
              description: 'Byte offset to start from (default 0).',
            },
            maxBytes: {
              type: 'number',
              description: `Bytes to return (default ${DEFAULT_RAW_LOG_BYTES}, max ${MAX_RAW_LOG_BYTES}).`,
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'get_task_token_usage',
        description:
          'Read the token accounting of a delegated task: the task-level rollup (prompt/output/cache split, per-agent and per-model distribution) and one line per round. ' +
          'Pass `round` to focus on a single round. ' +
          'These numbers are the RECEIVER (callee) side only; a round with no measurement reports source="unavailable" (0 does NOT mean free) and an estimated round reports source="estimated". ' +
          'This is not a progress watcher: it is a cheap read-only query and is never starved by the polling guard.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: { type: 'string', description: 'Task thread ID.' },
            round: {
              type: 'number',
              description: 'Optional round number (1-based). Omit for the whole task.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'get_recent_logs',
        description:
          'Get the most recent log output (default 1,000 lines) of a running or completed task thread. ' +
          'Not a progress watcher: after dispatching, end your turn and wait for the callback; repeated reads at unchanged progress return a short `repeatPoll` stub.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to inspect.',
            },
            maxLines: {
              type: 'number',
              description: 'Maximum number of recent lines to return (defaults to 1000).',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'list_agent_models',
        description:
          'List the models each agent can run, including the model currently configured as that agent\'s default. The configured default is returned as `configuredModel` and as the FIRST entry of `models`, tagged "configured default" — that is the model a task runs on unless the user explicitly names another one. Results are cached; pass forceRefresh to re-probe the CLIs and provider endpoints.',
        inputSchema: {
          type: 'object',
          properties: {
            targetAgent: {
              type: 'string',
              enum: ['all', 'antigravity', 'claude_code', 'codex', 'deepseek_harness'],
              description: 'Which agent to inspect (default: all).',
            },
            forceRefresh: {
              type: 'boolean',
              description: 'Re-run discovery instead of using the cached catalogue.',
            },
          },
        },
      },
      {
        name: 'invoke_agent',
        description:
          'Synchronously invoke an agent for lightweight queries or code snippet reviews (blocks up to 30s).',
        inputSchema: {
          type: 'object',
          properties: {
            targetAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity', 'deepseek_harness'],
              description: 'Target agent to query.',
            },
            prompt: {
              type: 'string',
              description: 'Prompt query.',
            },
            timeoutMs: {
              type: 'number',
              description: 'Timeout in milliseconds (default: 30000).',
            },
            model: {
              type: 'string',
              description:
                'Optional model for this query; only when the user explicitly named it. Omit to use the agent\'s configured default. A non-default value is ignored unless modelRequestedByUser: true is also passed, which makes VS Code ask the user to confirm the switch.',
            },
            modelRequestedByUser: {
              type: 'boolean',
              description:
                'Set true only when the user themselves named `model`. Defaults to false, in which case a non-default model is ignored and the configured default is used.',
            },
          },
          required: ['prompt'],
        },
      },
      {
        name: 'query_agent_mesh_status',
        description:
          'Query status, paths, versions and connectivity of Antigravity, Claude Code, Codex, and DeepSeek Harness in the mesh.',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
    ];

    // A receiver is executing one delegated task; hide new-work tools so the
    // model cannot even attempt recursive delegation. The runtime guard in
    // handleToolCall stays as the authoritative enforcement.
    if (isTaskReceiverProcess()) {
      return tools.filter((tool) => !FORWARDING_TOOLS.has(tool.name));
    }
    return tools;
  }

  /**
   * A review decision is only valid while the task is in its reviewer role.
   * This blocks a stale reviewer process (one that already handed off) from
   * approving or reopening the task later.
   */
  private assertActiveReviewerRole(threadId: string): void {
    const owner = new TaskRoleLock().getOwner(threadId);
    if (!owner || owner.role !== 'reviewer') {
      throw new Error(
        `[Agent Mesh Bridge] Refusing review action for thread ${threadId}: ` +
          `no active reviewer role is held. A task accepts review decisions only from its current reviewer turn; ` +
          `finish or re-wake the same task instead of acting from a stale reviewer process.`,
      );
    }
  }

  /**
   * Enforce the receiver rule at call time (defense in depth: a client may have
   * cached the tool list from before the marker was applied, or call a tool
   * directly over JSON-RPC).
   */
  private assertReceiverMayCall(name: string, args: any): void {
    if (!isTaskReceiverProcess()) return;

    const currentThreadId = process.env[MESH_TASK_THREAD_ENV];
    if (FORWARDING_TOOLS.has(name)) {
      const target = args?.targetAgent ? ` to ${args.targetAgent}` : '';
      throw new Error(
        `[Agent Mesh Bridge] Refusing "${name}"${target}: this agent is the receiver of delegated task ` +
          `${currentThreadId || '(unknown thread)'}. Task receivers must execute the current task and report ` +
          `back, not forward work to other agents.`,
      );
    }

    // Pause/resume/delete are control-plane actions. A receiver may manage a
    // different thread it initiated, but never the task it is executing.
    if (CONTROL_TOOLS.has(name)) {
      if (!currentThreadId || args?.threadId === currentThreadId) {
        throw new Error(
          `[Agent Mesh Bridge] Refusing "${name}" for thread ${args?.threadId || currentThreadId}: ` +
            `a task receiver cannot perform caller/reviewer control actions on its own executing task.`,
        );
      }
    }

    // A receiver also must not drive its own task's review loop; only the
    // initiator (caller) decides whether to rework or approve.
    if (name === 'request_rework' || name === 'approve_task') {
      if (!currentThreadId) {
        throw new Error(
          `[Agent Mesh Bridge] Refusing "${name}": this process was launched as a receiver for a one-shot ` +
            `invocation and is not the initiator of any thread.`,
        );
      }
      if (args?.threadId === currentThreadId) {
        throw new Error(
          `[Agent Mesh Bridge] Refusing "${name}" for thread ${currentThreadId}: this agent is its receiver, ` +
            `not its initiator. Finish the current round and let the caller review it.`,
        );
      }
    }
  }

  /**
   * Execute a tool call and return result object.
   */
  public async handleToolCall(name: string, args: any): Promise<any> {
    this.assertReceiverMayCall(name, args);

    switch (name) {
      case 'dispatch_task': {
        const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
        const initiatorAgent =
          args.initiatorAgent && args.initiatorAgent !== 'default'
            ? (args.initiatorAgent as AgentId)
            : callerFromEnv;

        const dispatched = await this.dispatcher.dispatchTask({
          targetAgent: args.targetAgent,
          initiatorAgent,
          title: args.title,
          prompt: args.prompt,
          callerSessionId: args.callerSessionId,
          workspacePath: args.workspacePath,
          worktree: args.worktree,
          expectedFiles: args.expectedFiles,
          allowNoScope: args.allowNoScope === true,
          allowScopeOverlap: args.allowScopeOverlap === true,
          // Agent callers must declare a scope; that is what makes the
          // file-ownership gate enforceable for tasks dispatched over MCP.
          requireScope: true,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
          initiatorModel: args.initiatorModel,
        });

        // Guard the task this process just created: repeated status reads at the
        // same progress return a status-only payload instead of feeding a poll
        // loop the caller was told to stop running.
        const created = this.taskStore.getTask(dispatched.threadId);
        if (created) {
          this.pollGuard.noteDispatch(created.threadId, progressSignature(created));
        }
        return dispatched;
      }

      case 'request_rework': {
        this.assertActiveReviewerRole(args.threadId);
        // "This task passed, but the batch still waits for X" is not a rework:
        // park it, and let the dependency's completion wake the review.
        if (args.blocked === true) {
          const parked = this.reworkController.blockReview({
            threadId: args.threadId,
            reason: args.feedback,
            waitingForThreadId:
              typeof args.waitingForThreadId === 'string' && args.waitingForThreadId.trim()
                ? args.waitingForThreadId.trim()
                : undefined,
            by: 'caller',
          });
          new TaskRoleLock().releaseIfRole(args.threadId, 'reviewer');
          this.pollGuard.forget(args.threadId);
          return parked;
        }
        const result = await this.reworkController.requestRework({
          threadId: args.threadId,
          feedback: args.feedback,
          callerSessionId: args.callerSessionId,
          targetAgent: args.targetAgent,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
        });
        // Reviewer decision submitted: hand the task role over to the
        // developer. The reviewer CLI process may still be printing its final
        // message; that is not an active review decision anymore.
        new TaskRoleLock().releaseIfRole(args.threadId, 'reviewer');
        return result;
      }

      case 'approve_task': {
        this.assertActiveReviewerRole(args.threadId);
        const result = this.reworkController.approveTask({
          threadId: args.threadId,
          remarks: args.remarks,
        });
        new TaskRoleLock().releaseIfRole(args.threadId, 'reviewer');
        // The task is closed: no polling to discourage anymore.
        this.pollGuard.forget(args.threadId);
        return result;
      }

      case 'pause_task': {
        return this.taskControl.pauseTask(args.threadId, args.reason, 'agent');
      }

      case 'resume_task': {
        return this.reworkController.resumePausedTask(args.threadId, {
          targetAgent: args.targetAgent,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
        });
      }

      case 'retarget_task': {
        const result = await this.retargetService.retargetTask({
          threadId: args.threadId,
          targetAgent: args.targetAgent,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
          reason: args.reason,
          by: (process.env.MESH_CALLER_AGENT as string) || 'caller',
        });
        return result;
      }

      case 'allow_scope_overlap': {
        const thread = this.taskStore.getTask(args.threadId);
        if (!thread) {
          throw new Error(`Task thread not found: ${args.threadId}`);
        }
        thread.scopeOverride = true;
        thread.scopeBlockedBy = undefined;
        thread.updatedAt = new Date().toISOString();
        this.taskStore.upsertTask(thread);
        return {
          threadId: args.threadId,
          status: 'SCOPE_OVERRIDE',
          message:
            `Task ${args.threadId} may now run on files owned by other open tasks` +
            `${args.reason ? ` (reason: ${args.reason})` : ''}. ` +
            `The queue advances on its own; you will be woken when the round finishes.`,
        };
      }

      case 'retry_task': {
        const retried = await this.reworkController.retryFailedRound(args.threadId, {
          targetAgent: args.targetAgent,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
        });
        const thread = this.taskStore.getTask(args.threadId);
        const routed =
          thread && (args.targetAgent || args.model)
            ? ` Now executing on ${thread.targetAgent}${thread.targetModel ? ` (${thread.targetModel})` : ''}.`
            : '';
        return {
          threadId: args.threadId,
          status: 'RETRYING',
          round: retried.round,
          targetAgent: thread?.targetAgent,
          targetModel: thread?.targetModel,
          message:
            `Retrying Round ${retried.round} in place: same prompt, same native session ` +
            `(the failed round is reused, so the ledger will not show a duplicate round).${routed} ` +
            `Conclude your turn and await the callback.`,
        };
      }

      case 'delete_task': {
        const deleted = this.taskControl.deleteTask(
          args.threadId,
          args.reason,
          'agent',
          args.force === true,
        );
        this.pollGuard.forget(args.threadId);
        return deleted;
      }

      case 'record_merge_request': {
        if (!args.url && !args.branch && !args.commit) {
          throw new Error(
            'record_merge_request requires at least one of url, branch, or commit.',
          );
        }
        const updated = this.taskStore.recordMergeRequest(args.threadId, {
          url: args.url,
          branch: args.branch,
          commit: args.commit,
          remote: args.remote || 'origin',
          baseBranch: args.baseBranch,
          title: args.title,
          status: args.status || 'synced',
          summary: args.summary,
          updatedAt: new Date().toISOString(),
          by: (process.env.MESH_CALLER_AGENT as string) || 'caller',
        });
        if (!updated) {
          throw new Error(`Task thread not found: ${args.threadId}`);
        }
        return {
          threadId: args.threadId,
          status: 'MR_RECORDED',
          mergeRequest: updated.mergeRequest,
        };
      }

      case 'list_recent_tasks': {
        const limit = typeof args.limit === 'number' ? args.limit : 20;
        const statusFilter = args.statusFilter;
        const tasks = this.taskStore.listTasks(limit, statusFilter);
        // Every task carries both ends of its model routing so a caller can see
        // which model it dispatched from and which model the receiver runs, plus
        // the derived token rollup so a list can be compared by cost.
        return tasks.map((task) => ({
          ...task,
          modelRouting: getModelRouting(task),
          tokenUsage: aggregateThreadTokenUsage(task.rounds || []),
        }));
      }

      case 'get_task_token_usage': {
        const task = this.taskStore.getTask(args.threadId);
        if (!task) {
          throw new Error(`Thread not found: ${args.threadId}`);
        }
        const rounds = task.rounds || [];
        const summary = aggregateThreadTokenUsage(rounds);

        if (args.round !== undefined && args.round !== null) {
          const roundNumber = Number(args.round);
          if (!Number.isFinite(roundNumber) || roundNumber < 1) {
            throw new Error('`round` must be a positive round number.');
          }
          const round = rounds.find((entry) => entry.round === roundNumber);
          if (!round) {
            throw new Error(
              `Round ${roundNumber} not found on ${args.threadId}. ` +
                `Available rounds: ${rounds.map((entry) => entry.round).join(', ') || 'none'}.`,
            );
          }
          return {
            threadId: task.threadId,
            title: task.title,
            round: roundNumber,
            executorAgent: round.executorAgent,
            executorModel: round.executorModel,
            status: round.status,
            durationMs: round.durationMs,
            tokenUsage: round.tokenUsage,
            taskTotal: summary,
            note:
              'Counts are the RECEIVER (callee) side only. A round without usage data was ' +
              'not measured (source=unavailable); estimated values carry source=estimated.',
          };
        }

        return {
          threadId: task.threadId,
          title: task.title,
          status: task.status,
          totalRounds: rounds.length,
          tokenUsage: summary,
          rounds: rounds.map((round) => ({
            round: round.round,
            type: round.type,
            status: round.status,
            durationMs: round.durationMs,
            executorAgent: round.executorAgent,
            executorModel: round.executorModel,
            tokenUsage: round.tokenUsage,
          })),
          note:
            'Counts are the RECEIVER (callee) side only. ' +
            'Use get_round_raw_log for the verbatim text of a round, not for its token counts.',
        };
      }

      case 'get_thread_details': {
        const task = this.taskStore.getTask(args.threadId);
        if (!task) {
          throw new Error(`Thread not found: ${args.threadId}`);
        }
        const verdict = this.pollGuard.check(task.threadId, progressSignature(task));
        if (verdict.verdict === 'repeat') {
          // Same thread, same progress, already read once in this turn.
          return CallerPollGuard.repeatPayload(task, verdict.reads);
        }

        const includeRawLogs = args.includeRawLogs === true || args.includeFullHistory === true;
        if (includeRawLogs) {
          const recentLogsInfo = TaskSupervisor.getRecentLogs(args.threadId, 1000, this.taskStore);
          return {
            ...task,
            modelRouting: getModelRouting(task),
            tokenUsage: aggregateThreadTokenUsage(task.rounds || []),
            recentLogs: recentLogsInfo.logs,
            totalLogLines: recentLogsInfo.totalLines,
          };
        }

        const rounds = task.rounds || [];
        const projected = rounds.map((round, index) =>
          this.projectRound(task.threadId, round, index === rounds.length - 1),
        );
        const latestLog = rounds.length
          ? this.resolveRoundExecutionLog(task.threadId, rounds[rounds.length - 1].round)
          : undefined;
        return {
          ...task,
          rounds: projected,
          modelRouting: getModelRouting(task),
          // Derived on read: a stored aggregate could be rolled back by a
          // stale process writing an old copy of the task.
          tokenUsage: aggregateThreadTokenUsage(rounds),
          // Do not ship 1,000 log lines by default; hand out a pointer instead.
          logReference: latestLog
            ? {
                path: latestLog,
                hint: `get_round_raw_log(threadId="${task.threadId}", round=${
                  rounds[rounds.length - 1].round
                }, source="execution")`,
              }
            : undefined,
          fullHistoryHint:
            'Older rounds are condensed or excerpted. Use get_round_raw_log for one round, ' +
            'or includeRawLogs: true for the complete history (large).',
        };
      }

      case 'get_round_raw_log': {
        const roundNumber = Number(args.round);
        if (!Number.isFinite(roundNumber) || roundNumber < 1) {
          throw new Error('`round` must be a positive round number.');
        }
        return this.readRoundRawLog(args.threadId, roundNumber, args);
      }

      case 'get_reviewer_log': {
        return this.readReviewerLog(args);
      }

      case 'get_recent_logs': {
        const maxLines = typeof args.maxLines === 'number' ? args.maxLines : 1000;
        const task = this.taskStore.getTask(args.threadId);
        if (task) {
          const verdict = this.pollGuard.check(task.threadId, progressSignature(task), 'logs');
          if (verdict.verdict === 'repeat') {
            return CallerPollGuard.repeatPayload(task, verdict.reads);
          }
        }
        return TaskSupervisor.getRecentLogs(args.threadId, maxLines, this.taskStore);
      }

      case 'list_agent_models': {
        const all = await this.modelCatalog.getAll(!!args.forceRefresh);
        const defaults = loadModelDefaults();
        const requested: AgentId[] =
          args.targetAgent && args.targetAgent !== 'all'
            ? [args.targetAgent as AgentId]
            : ['antigravity', 'codex', 'claude_code', 'deepseek_harness'];

        const agents: Record<string, any> = {};
        for (const agent of requested) {
          const catalog = all[agent];
          if (!catalog) continue;
          const configuredModel = defaults[agent] || null;
          agents[agent] = {
            // The configured default leads the payload: on a 90-entry provider
            // catalogue a caller that only skims the head would otherwise pin
            // whichever id the provider happened to list first.
            configuredModel,
            // `models` also carries it first, tagged in place.
            models: annotateConfiguredModel(catalog.models, configuredModel),
            source: catalog.source,
            updatedAt: catalog.updatedAt,
            error: catalog.error,
          };
        }
        return {
          agents,
          hint:
            'Omit `model` in dispatch_task/invoke_agent unless the user explicitly named a model: ' +
            'the task then uses configuredModel (listed first, tagged "configured default"). ' +
            'If the user did name a different model, pass it together with modelRequestedByUser: true — ' +
            'VS Code then asks the user to confirm the switch, and without that click the task runs on ' +
            'the configured default instead. Change the default with the ' +
            '"Agent Bridge: Select Model for Agent" command in VS Code.',
        };
      }

      case 'invoke_agent': {
        let agent: AgentId;
        if (!args.targetAgent || args.targetAgent === 'default') {
          const resolved = await this.resolver.resolveActiveAgent();
          agent = resolved.agent;
        } else {
          agent = args.targetAgent;
        }

        // Same model policy as dispatch_task: the agent's configured default
        // unless the caller declares an explicit user request that the user then
        // confirms in VS Code.
        const callerAgent = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
        const modelResolution = await resolveDispatchModel({
          targetAgent: agent,
          requestedModel: args.model,
          requestedByUser: args.modelRequestedByUser === true,
          initiatorAgent: callerAgent,
          title: 'invoke_agent',
        });

        const res = await this.runner.execute({
          agent,
          prompt: args.prompt,
          isRework: false,
          receiverSessionId: `sync_${Date.now()}`,
          timeoutMs: args.timeoutMs || 30000,
          model: modelResolution.model,
        });

        return {
          agent,
          model: modelResolution.model,
          modelNotice: modelResolution.notice,
          exitCode: res.exitCode,
          output: res.extractedOutput || res.stdout,
          error: res.error,
          durationMs: res.durationMs,
        };
      }

      case 'query_agent_mesh_status': {
        const [anti, claude, codex, dsh, active] = await Promise.all([
          this.detector.probeAgent('antigravity'),
          this.detector.probeAgent('claude_code'),
          this.detector.probeAgent('codex'),
          this.detector.probeAgent('deepseek_harness'),
          this.resolver.resolveActiveAgent(),
        ]);

        return {
          activeAgent: active.agent,
          resolvedMode: active.mode,
          resolveReason: active.reason,
          totalLedgerTasks: this.taskStore.count(),
          agents: {
            antigravity: anti,
            claude_code: claude,
            codex: codex,
            deepseek_harness: dsh,
          },
        };
      }

      case 'invoke_antigravity': {
        const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
        return await this.dispatcher.dispatchTask({
          targetAgent: 'antigravity',
          initiatorAgent: callerFromEnv || 'codex',
          title: args.title || 'Delegation to Antigravity',
          prompt: args.prompt,
          workspacePath: args.workspacePath,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
          initiatorModel: args.initiatorModel,
        });
      }

      case 'invoke_claude_code': {
        const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
        return await this.dispatcher.dispatchTask({
          targetAgent: 'claude_code',
          initiatorAgent: callerFromEnv || 'codex',
          title: args.title || 'Delegation to Claude Code',
          prompt: args.prompt,
          workspacePath: args.workspacePath,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
          initiatorModel: args.initiatorModel,
        });
      }

      case 'invoke_codex': {
        const callerFromEnv = (process.env.MESH_CALLER_AGENT as AgentId) || undefined;
        return await this.dispatcher.dispatchTask({
          targetAgent: 'codex',
          initiatorAgent: callerFromEnv || 'antigravity',
          title: args.title || 'Delegation to Codex',
          prompt: args.prompt,
          workspacePath: args.workspacePath,
          model: args.model,
          modelRequestedByUser: args.modelRequestedByUser === true,
          initiatorModel: args.initiatorModel,
        });
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  /**
   * Handle incoming JSON-RPC 2.0 message.
   */
  public async handleRpcMessage(msg: any): Promise<any | null> {
    if (!msg || typeof msg !== 'object') {
      return { jsonrpc: '2.0', error: { code: -32600, message: 'Invalid Request' }, id: null };
    }

    const { id, method, params } = msg;

    try {
      if (method === 'initialize') {
        return {
          jsonrpc: '2.0',
          id,
          result: {
            protocolVersion: '2024-11-05',
            capabilities: {
              tools: {
                listChanged: true,
              },
            },
            serverInfo: {
              name: 'agent-mesh-bridge',
              version: '1.0.0',
            },
          },
        };
      }

      if (method === 'notifications/initialized') {
        return null; // Notification, no response
      }

      if (method === 'ping') {
        return { jsonrpc: '2.0', id, result: {} };
      }

      if (method === 'tools/list') {
        return {
          jsonrpc: '2.0',
          id,
          result: {
            tools: this.getToolDefinitions(),
          },
        };
      }

      if (method === 'tools/call') {
        const toolName = params?.name;
        const toolArgs = params?.arguments || {};
        const result = await this.handleToolCall(toolName, toolArgs);
        return {
          jsonrpc: '2.0',
          id,
          result: {
            content: [
              {
                type: 'text',
                text: JSON.stringify(result, null, 2),
              },
            ],
          },
        };
      }

      if (id !== undefined && id !== null) {
        return {
          jsonrpc: '2.0',
          id,
          error: {
            code: -32601,
            message: `Method not found: ${method}`,
          },
        };
      }
    } catch (err: any) {
      if (id !== undefined && id !== null) {
        return {
          jsonrpc: '2.0',
          id,
          error: {
            code: -32000,
            message: err.message || 'Internal MCP server error',
          },
        };
      }
    }

    return null;
  }

  /**
   * Connect and start Stdio JSON-RPC server.
   */
  public startStdio(): void {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
      terminal: false,
    });

    rl.on('line', async (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;
      try {
        const req = JSON.parse(trimmed);
        const resp = await this.handleRpcMessage(req);
        if (resp) {
          process.stdout.write(JSON.stringify(resp) + '\n');
        }
      } catch {
        process.stdout.write(
          JSON.stringify({
            jsonrpc: '2.0',
            id: null,
            error: { code: -32700, message: 'Parse error' },
          }) + '\n',
        );
      }
    });

    process.stdin.resume();
  }
}
