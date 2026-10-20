import * as readline from 'readline';
import { AsyncDispatcher } from './async-dispatcher';
import { ReworkController } from './rework-controller';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner } from './autonomous-runner';
import { TaskSupervisor } from './task-supervisor';
import { TaskControlService } from './task-control';
import { TaskRoleLock } from './task-role-lock';
import { ModelCatalog, loadModelDefaults } from './model-catalog';
import {
  AgentId,
  MESH_TASK_THREAD_ENV,
  getModelRouting,
  isTaskReceiverProcess,
} from './types';

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
  }

  /**
   * Returns list of tools for MCP tools/list.
   */
  public getToolDefinitions() {
    const tools = [
      {
        name: 'dispatch_task',
        description:
          'Asynchronously delegate a new task to another agent (Codex, Claude Code, Antigravity, DeepSeek Harness) without blocking the current turn. Returns immediately with threadId. This tool is unavailable while this agent is itself executing a delegated task (receivers must not forward work).',
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
                'Model id the receiver should run this task with (e.g. "gpt-6-sol", "sonnet", "gemini-3.8-flash-high"). Call list_agent_models first to see what the target supports. Omit to use the model configured in the bridge settings for that agent (recommended default).',
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
          'Request the target agent to rework or fix issues in the EXACT SAME native session with full context preserved.',
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
            callerSessionId: {
              type: 'string',
              description:
                'Your native session UUID for subsequent wake-up callback. Do NOT pass agent names (e.g. "claude_code"). If omitted, the bridge automatically auto-detects your active session.',
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
          'Resume a paused task, preserving its original prompt and native session (or starting fresh when no native session was captured).',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID to resume.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'retry_task',
        description:
          'Retry a failed round in place: same round number, same prompt, same native session. Use this instead of request_rework when you simply want the receiver to run the failed instruction again — re-sending the original prompt as feedback creates a second round that looks like a duplicate task.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'Task thread ID whose latest failed round should be retried.',
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
          'Permanently delete a task. A running agent process is stopped first; the deletion reason is written to the tombstone log.',
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
          'Retrieve full details of a task thread, including all interaction rounds, prompts, outputs, and diffs.',
        inputSchema: {
          type: 'object',
          properties: {
            threadId: {
              type: 'string',
              description: 'The task thread ID to fetch.',
            },
          },
          required: ['threadId'],
        },
      },
      {
        name: 'get_recent_logs',
        description:
          'Get the most recent log output (default 1,000 lines) of a running or completed task thread in real-time.',
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
          'List the models each agent can run, including the model currently configured as that agent\'s default. Use it before dispatch_task when a task needs a specific model. Results are cached; pass forceRefresh to re-probe the CLIs and provider endpoints.',
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
                'Optional model override for this query. Omit to use the agent\'s configured default.',
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

        return await this.dispatcher.dispatchTask({
          targetAgent: args.targetAgent,
          initiatorAgent,
          title: args.title,
          prompt: args.prompt,
          callerSessionId: args.callerSessionId,
          workspacePath: args.workspacePath,
          expectedFiles: args.expectedFiles,
          allowNoScope: args.allowNoScope === true,
          allowScopeOverlap: args.allowScopeOverlap === true,
          // Agent callers must declare a scope; that is what makes the
          // file-ownership gate enforceable for tasks dispatched over MCP.
          requireScope: true,
          model: args.model,
          initiatorModel: args.initiatorModel,
        });
      }

      case 'request_rework': {
        this.assertActiveReviewerRole(args.threadId);
        const result = await this.reworkController.requestRework({
          threadId: args.threadId,
          feedback: args.feedback,
          callerSessionId: args.callerSessionId,
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
        return result;
      }

      case 'pause_task': {
        return this.taskControl.pauseTask(args.threadId, args.reason, 'agent');
      }

      case 'resume_task': {
        return this.reworkController.resumePausedTask(args.threadId);
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
        const retried = await this.reworkController.retryFailedRound(args.threadId);
        return {
          threadId: args.threadId,
          status: 'RETRYING',
          round: retried.round,
          message:
            `Retrying Round ${retried.round} in place: same prompt, same native session ` +
            `(the failed round is reused, so the ledger will not show a duplicate round). ` +
            `Conclude your turn and await the callback.`,
        };
      }

      case 'delete_task': {
        return this.taskControl.deleteTask(args.threadId, args.reason, 'agent');
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
        // which model it dispatched from and which model the receiver runs.
        return tasks.map((task) => ({ ...task, modelRouting: getModelRouting(task) }));
      }

      case 'get_thread_details': {
        const task = this.taskStore.getTask(args.threadId);
        if (!task) {
          throw new Error(`Thread not found: ${args.threadId}`);
        }
        const recentLogsInfo = TaskSupervisor.getRecentLogs(args.threadId, 1000, this.taskStore);
        return {
          ...task,
          modelRouting: getModelRouting(task),
          recentLogs: recentLogsInfo.logs,
          totalLogLines: recentLogsInfo.totalLines,
        };
      }

      case 'get_recent_logs': {
        const maxLines = typeof args.maxLines === 'number' ? args.maxLines : 1000;
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
          agents[agent] = {
            models: catalog.models,
            source: catalog.source,
            updatedAt: catalog.updatedAt,
            error: catalog.error,
            configuredModel: defaults[agent] || null,
          };
        }
        return {
          agents,
          hint:
            'Pass `model` to dispatch_task to pin one task; leave it out to use configuredModel. ' +
            'Change the default with the "Agent Bridge: Select Model for Agent" command in VS Code.',
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

        const res = await this.runner.execute({
          agent,
          prompt: args.prompt,
          isRework: false,
          receiverSessionId: `sync_${Date.now()}`,
          timeoutMs: args.timeoutMs || 30000,
          model: args.model,
        });

        return {
          agent,
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
