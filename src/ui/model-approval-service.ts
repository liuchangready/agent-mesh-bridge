/**
 * VS Code side of the model-switch consent gate.
 *
 * A caller agent reaches the bridge through its own MCP server process, which
 * cannot show UI. When that process asks to run a task on a non-default model it
 * publishes a request under `<bridge-home>/model-approvals/`; this service polls
 * that directory and turns each pending request into one modal dialog. Only a
 * click on "同意使用" switches the model — "改用默认模型" falls back, and closing
 * the dialog cancels the dispatch.
 *
 * The dialog is intentionally modal and answers a decision the user must own:
 * the caller has already told the bridge that *the user* named this model, so
 * the bridge asks that same user to confirm it.
 */

import * as vscode from 'vscode';
import {
  ModelApprovalDecision,
  ModelApprovalRequest,
  listPendingModelApprovals,
  readModelApprovalDecision,
  submitModelApprovalDecision,
} from '../bridge/model-approval';

const AGENT_LABELS: Record<string, string> = {
  antigravity: 'Google Antigravity',
  codex: 'OpenAI Codex',
  claude_code: 'Anthropic Claude Code',
  deepseek_harness: 'DeepSeek Harness',
  vscode_chat: 'VSCode Chat',
};

const APPROVE = '同意使用该模型';
const USE_DEFAULT = '改用默认模型';
const CANCEL = '取消本次派发';

export class ModelApprovalService implements vscode.Disposable {
  private timer?: ReturnType<typeof setInterval>;
  private readonly inFlight = new Set<string>();

  constructor(private readonly intervalMs: number = 1000) {}

  /** Begin polling for requests raised by the agents' MCP processes. */
  public start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => {
      void this.poll();
    }, this.intervalMs);
    void this.poll();
  }

  /** Show a dialog for every pending request that is not already on screen. */
  public async poll(): Promise<void> {
    let pending: ModelApprovalRequest[];
    try {
      pending = listPendingModelApprovals();
    } catch {
      return;
    }

    for (const request of pending) {
      if (this.inFlight.has(request.id)) continue;
      if (readModelApprovalDecision(request.id)) continue;
      this.inFlight.add(request.id);
      void this.ask(request);
    }
  }

  private async ask(request: ModelApprovalRequest): Promise<void> {
    const initiator = request.initiatorAgent
      ? AGENT_LABELS[request.initiatorAgent] || request.initiatorAgent
      : '另一个 Agent';
    const target = AGENT_LABELS[request.targetAgent] || request.targetAgent;
    const fallback = request.defaultModel || '（该 Agent 未配置默认模型）';
    const task = request.title ? `\n任务：${request.title}` : '';

    const message =
      `【模型切换确认】${initiator} 请求让 ${target} 使用非默认模型。` +
      `\n\n请求模型：${request.requestedModel}` +
      `\n当前默认：${fallback}` +
      task +
      `\n\n只有点击「${APPROVE}」才会切换；否则本次派发将使用默认模型或取消。`;

    // Every branch below assigns both fields; the dialog's own outcome decides them.
    let status: ModelApprovalDecision['status'];
    let note: string | undefined;
    try {
      const pick = await vscode.window.showWarningMessage(
        message,
        { modal: true },
        APPROVE,
        USE_DEFAULT,
        CANCEL,
      );
      if (pick === APPROVE) {
        status = 'approved';
        note = undefined;
      } else if (pick === USE_DEFAULT) {
        status = 'use_default';
        note = '用户选择保持默认模型';
      } else {
        status = 'cancelled';
        note = pick === CANCEL ? '用户取消了本次派发' : '关闭了确认对话框';
      }

      const decision: ModelApprovalDecision = {
        id: request.id,
        status,
        at: new Date().toISOString(),
        by: 'user',
        note,
      };
      submitModelApprovalDecision(decision);
    } finally {
      this.inFlight.delete(request.id);
    }
  }

  public dispose(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
    this.inFlight.clear();
  }
}
