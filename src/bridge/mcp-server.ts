import * as readline from 'readline';
import { AsyncDispatcher } from './async-dispatcher';
import { ReworkController } from './rework-controller';
import { TaskStore } from './task-store';
import { AgentDetector } from './agent-detector';
import { ActiveAgentResolver } from './active-agent-resolver';
import { AutonomousRunner } from './autonomous-runner';
import { AgentId } from './types';

export class MCPServer {
  private dispatcher: AsyncDispatcher;
  private reworkController: ReworkController;
  private taskStore: TaskStore;
  private detector: AgentDetector;
  private resolver: ActiveAgentResolver;
  private runner: AutonomousRunner;

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
  }

  /**
   * Returns list of tools for MCP tools/list.
   */
  public getToolDefinitions() {
    return [
      {
        name: 'dispatch_task',
        description:
          'Asynchronously delegate a new task to another agent (Codex, Claude Code, Antigravity) without blocking the current turn. Returns immediately with threadId.',
        inputSchema: {
          type: 'object',
          properties: {
            targetAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity'],
              description:
                'Target agent to execute the task. Defaults to "default" (auto-detected).',
            },
            initiatorAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity'],
              description:
                'Your agent identity (codex, claude_code, antigravity). Defaults to auto-detected caller.',
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
                'Your native session ID so the bridge can proactively wake you up upon completion. If omitted, the bridge automatically auto-detects your active session.',
            },
            workspacePath: {
              type: 'string',
              description: 'Workspace root path (defaults to current working directory).',
            },
          },
          required: ['title', 'prompt'],
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
              description: 'Your native session ID for subsequent wake-up callback.',
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
        name: 'invoke_agent',
        description:
          'Synchronously invoke an agent for lightweight queries or code snippet reviews (blocks up to 30s).',
        inputSchema: {
          type: 'object',
          properties: {
            targetAgent: {
              type: 'string',
              enum: ['default', 'codex', 'claude_code', 'antigravity'],
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
          },
          required: ['prompt'],
        },
      },
      {
        name: 'query_agent_mesh_status',
        description:
          'Query status, paths, versions and connectivity of Antigravity, Claude Code, and Codex in the mesh.',
        inputSchema: {
          type: 'object',
          properties: {},
        },
      },
    ];
  }

  /**
   * Execute a tool call and return result object.
   */
  public async handleToolCall(name: string, args: any): Promise<any> {
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
        });
      }

      case 'request_rework': {
        return await this.reworkController.requestRework({
          threadId: args.threadId,
          feedback: args.feedback,
          callerSessionId: args.callerSessionId,
        });
      }

      case 'approve_task': {
        return this.reworkController.approveTask({
          threadId: args.threadId,
          remarks: args.remarks,
        });
      }

      case 'list_recent_tasks': {
        const limit = typeof args.limit === 'number' ? args.limit : 20;
        const statusFilter = args.statusFilter;
        return this.taskStore.listTasks(limit, statusFilter);
      }

      case 'get_thread_details': {
        const task = this.taskStore.getTask(args.threadId);
        if (!task) {
          throw new Error(`Thread not found: ${args.threadId}`);
        }
        return task;
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
        const [anti, claude, codex, active] = await Promise.all([
          this.detector.probeAgent('antigravity'),
          this.detector.probeAgent('claude_code'),
          this.detector.probeAgent('codex'),
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
          },
        };
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
