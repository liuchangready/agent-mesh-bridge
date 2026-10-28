import * as vscode from 'vscode';
import * as path from 'path';
import { TaskStore } from '../bridge/task-store';
import { ReworkController } from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';
import { TaskSupervisor } from '../bridge/task-supervisor';
import { AgentDetector } from '../bridge/agent-detector';
import {
  resolveExistingArtifactPath,
  roundExecutionLogPath,
  roundOutputArchivePath,
} from '../bridge/round-log';
import { aggregateThreadTokenUsage, formatTokenCount } from '../bridge/token-usage-collector';

export class TaskDetailPanel {
  public static currentPanel: TaskDetailPanel | undefined;
  private readonly panel: vscode.WebviewPanel;
  private readonly extensionUri: vscode.Uri;
  private taskStore: TaskStore;
  private reworkController: ReworkController;
  private currentThreadId: string;
  private disposables: vscode.Disposable[] = [];

  private constructor(
    panel: vscode.WebviewPanel,
    extensionUri: vscode.Uri,
    taskStore: TaskStore,
    reworkController: ReworkController,
    threadId: string,
  ) {
    this.panel = panel;
    this.extensionUri = extensionUri;
    this.taskStore = taskStore;
    this.reworkController = reworkController;
    this.currentThreadId = threadId;

    this.update();

    this.panel.onDidDispose(() => this.dispose(), null, this.disposables);

    // Listen to TaskStore updates
    const onTasksChanged = () => {
      this.update();
    };
    this.taskStore.on('changed', onTasksChanged);
    this.disposables.push({
      dispose: () => {
        this.taskStore.off('changed', onTasksChanged);
      },
    });

    // Handle messages from Webview
    this.panel.webview.onDidReceiveMessage(
      async (message) => {
        switch (message.command) {
          case 'approve':
            await this.handleApprove(message.remarks);
            break;
          case 'requestRework':
            await this.handleRequestRework(message.feedback);
            break;
          case 'resumeTask':
            await this.handleResumeTask(message.feedback);
            break;
          case 'pauseTask':
            await vscode.commands.executeCommand('agent-mesh-bridge.pauseTask', this.currentThreadId);
            this.update();
            break;
          case 'deleteTask':
            await vscode.commands.executeCommand(
              'agent-mesh-bridge.deleteTask',
              this.currentThreadId,
            );
            this.update();
            break;
          case 'retryFailedRound':
            await this.handleRetryFailedRound();
            break;
          case 'wakeUpCaller':
            await vscode.commands.executeCommand(
              'agent-mesh-bridge.wakeUpCaller',
              this.currentThreadId,
            );
            this.update();
            break;
          case 'openTerminal':
            this.handleOpenTerminal();
            break;
          case 'openRoundRawLog':
            await this.handleOpenRoundRawLog(Number(message.round));
            break;
          case 'printRecentLogs':
            await vscode.commands.executeCommand(
              'agent-mesh-bridge.printRecentLogs',
              this.currentThreadId,
            );
            break;
          case 'copiedNotice':
            vscode.window.showInformationMessage('已复制最近 1,000 行日志到剪贴板。');
            break;
          case 'refresh':
            this.update();
            break;
        }
      },
      null,
      this.disposables,
    );
  }

  public static render(
    extensionUri: vscode.Uri,
    taskStore: TaskStore,
    reworkController: ReworkController,
    threadId: string,
  ) {
    if (TaskDetailPanel.currentPanel) {
      TaskDetailPanel.currentPanel.currentThreadId = threadId;
      TaskDetailPanel.currentPanel.panel.reveal(vscode.ViewColumn.One);
      TaskDetailPanel.currentPanel.update();
      return;
    }

    const panel = vscode.window.createWebviewPanel(
      'agentMeshTaskDetail',
      'Agent Task Detail',
      vscode.ViewColumn.One,
      {
        enableScripts: true,
        retainContextWhenHidden: true,
      },
    );

    TaskDetailPanel.currentPanel = new TaskDetailPanel(
      panel,
      extensionUri,
      taskStore,
      reworkController,
      threadId,
    );
  }

  private async handleApprove(remarks?: string) {
    try {
      this.reworkController.approveTask({
        threadId: this.currentThreadId,
        remarks: remarks || 'Approved via Detail Panel',
      });
      vscode.window.showInformationMessage(`Task ${this.currentThreadId} approved successfully.`);
      this.update();
    } catch (err: any) {
      vscode.window.showErrorMessage(`Approval failed: ${err.message}`);
    }
  }

  private async handleRequestRework(feedback?: string) {
    if (!feedback || !feedback.trim()) {
      const input = await vscode.window.showInputBox({
        prompt: 'Enter rework feedback for the agent:',
        placeHolder: 'Describe defects, missing test coverage, or required adjustments...',
      });
      if (!input) return;
      feedback = input;
    }

    try {
      await this.reworkController.requestRework({
        threadId: this.currentThreadId,
        feedback: feedback.trim(),
      });
      vscode.window.showInformationMessage(
        `Rework requested for ${this.currentThreadId} in same session.`,
      );
      this.update();
    } catch (err: any) {
      vscode.window.showErrorMessage(`Rework request failed: ${err.message}`);
    }
  }

  private async handleResumeTask(feedback?: string) {
    const thread = this.taskStore.getTask(this.currentThreadId);
    if (!thread) return;

    if (thread.status === 'paused' || (thread.rounds || []).some((r) => r.status === 'paused')) {
      await vscode.commands.executeCommand('agent-mesh-bridge.resumeTask', this.currentThreadId);
      this.update();
      return;
    }

    if (thread.status === 'running' || thread.status === 'reworking') {
      vscode.window.showInformationMessage(
        `任务当前正在由 ${thread.targetAgent} 自主执行中（Round ${thread.currentRound || 1}），无需重复触发。`,
      );
      return;
    }

    if (thread.status === 'waiting_review') {
      const choice = await vscode.window.showWarningMessage(
        `任务当前处于“等待检视”状态：下发方 (${thread.initiatorAgent}) 正在后台独立执行自动化测试与审查（通常耗时 30~60 秒）。确定要打断并提前下发新的返修要求吗？`,
        { modal: true },
        '确认提前下发',
        '取消等待',
      );
      if (choice !== '确认提前下发') return;
    }

    const runningRounds = (thread.rounds || []).filter((r) => r.status === 'running');
    const targetRound =
      thread.rounds.find((r) => r.round === thread.currentRound && r.status === 'running') ||
      runningRounds[runningRounds.length - 1];

    // If there is any round in running status with a valid prompt, resume execution directly
    if (targetRound && targetRound.prompt) {
      await vscode.commands.executeCommand('agent-mesh-bridge.resumeTask', this.currentThreadId);
      this.update();
      return;
    }

    if (!feedback || !feedback.trim()) {
      const lastRound = (thread.rounds || [])[thread.rounds.length - 1];
      const defaultValue =
        lastRound && lastRound.status === 'failed' && lastRound.prompt
          ? lastRound.prompt
          : '';
      const input = await vscode.window.showInputBox({
        prompt: `输入向 [${thread.targetAgent}] 下发的返修指令或补充说明：`,
        value: defaultValue,
        placeHolder: '请输入具体修改指令或测试要求...',
      });
      if (!input || !input.trim()) return;
      feedback = input;
    }

    try {
      await this.reworkController.requestRework({
        threadId: this.currentThreadId,
        feedback: feedback.trim(),
      });
      vscode.window.showInformationMessage(
        `已成功恢复任务 ${this.currentThreadId}，正在原生会话中继续执行。`,
      );
      this.update();
    } catch (err: any) {
      vscode.window.showErrorMessage(`恢复任务失败: ${err.message}`);
    }
  }

  private async handleRetryFailedRound() {
    try {
      const retried = await this.reworkController.retryFailedRound(this.currentThreadId);
      vscode.window.showInformationMessage(
        `正在重新执行任务 ${this.currentThreadId} 第 ${retried.round} 轮（保留原反馈与指令）...`,
      );
      this.update();
    } catch (err: any) {
      vscode.window.showErrorMessage(`重试失败轮次出错: ${err.message}`);
    }
  }

  private handleOpenTerminal() {
    const thread = this.taskStore.getTask(this.currentThreadId);
    if (!thread) return;

    const term = vscode.window.createTerminal(`Agent Bridge (${thread.targetAgent})`);
    term.show();

    let cmd = '';
    if (thread.targetAgent === 'codex') {
      cmd = `codex exec resume ${thread.receiverSessionId} ""`;
    } else if (thread.targetAgent === 'claude_code') {
      cmd = `claude -p "" --resume ${thread.receiverSessionId} --permission-mode bypassPermissions`;
    } else if (thread.targetAgent === 'antigravity') {
      cmd = `agy --conversation ${thread.receiverSessionId}`;
    } else if (thread.targetAgent === 'deepseek_harness') {
      // DSH resumes an existing native Session through the `tui` app.
      const launcher = new AgentDetector().resolveExecutable('deepseek_harness') || 'dsh';
      cmd = `"${launcher}" tui --resume ${thread.receiverSessionId}`;
    }

    if (cmd) {
      term.sendText(cmd, false);
      vscode.window.showInformationMessage(`Terminal command prepared: ${cmd}`);
    }
  }

  public update() {
    const thread = this.taskStore.getTask(this.currentThreadId);
    if (!thread) {
      this.panel.webview.html = `<html><body><h3 style="padding:20px;font-family:sans-serif;">Task not found: ${this.currentThreadId}</h3></body></html>`;
      return;
    }

    this.panel.title = `Task: ${thread.title.slice(0, 24)}`;
    this.panel.webview.html = this.getHtmlForWebview(thread);
  }

  /**
   * Open the archived pre-compaction output (falling back to the raw stdio log)
   * for one round. The card in the webview is a summary; this is the whole truth.
   */
  private async handleOpenRoundRawLog(roundNumber: number): Promise<void> {
    const thread = this.taskStore.getTask(this.currentThreadId);
    const round = thread?.rounds.find((entry) => entry.round === roundNumber);
    const root = path.dirname(this.taskStore.getStoragePath());
    const file = resolveExistingArtifactPath([
      round?.rawOutputFilePath,
      roundOutputArchivePath(this.currentThreadId, roundNumber, root),
      roundOutputArchivePath(this.currentThreadId, roundNumber),
      roundExecutionLogPath(this.currentThreadId, roundNumber, root),
      roundExecutionLogPath(this.currentThreadId, roundNumber),
    ]);
    if (!file) {
      vscode.window.showWarningMessage(
        `第 ${roundNumber} 轮没有可用的原始归档或执行日志（可能早于压缩功能）。`,
      );
      return;
    }
    try {
      const document = await vscode.workspace.openTextDocument(vscode.Uri.file(file));
      await vscode.window.showTextDocument(document, { preview: true });
    } catch (err) {
      vscode.window.showErrorMessage(`无法打开原始日志：${String(err)}`);
    }
  }

  private getHtmlForWebview(thread: DelegationThread): string {
    const escapeHtml = (str: string) => {
      return (str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    };

    const safeMrUrl =
      thread.mergeRequest?.url && /^https?:\/\//i.test(thread.mergeRequest.url)
        ? thread.mergeRequest.url
        : undefined;

    const statusBadgeColors: Record<string, string> = {
      queued: '#a855f7',
      running: '#f59e0b',
      reworking: '#f59e0b',
      waiting_review: '#3b82f6',
      paused: '#f97316',
      completed: '#10b981',
      failed: '#ef4444',
    };

    const badgeColor = statusBadgeColors[thread.status] || '#6b7280';
    const recentLogs = TaskSupervisor.getRecentLogs(thread.threadId, 1000, this.taskStore);
    // Derived on every render: a stored aggregate could be stale after a
    // cross-process write.
    const tokenSummary = aggregateThreadTokenUsage(thread.rounds || []);
    const tokenTilde = tokenSummary.isPartialOrEstimated ? '~' : '';

    const roundsHtml = thread.rounds
      .map((r) => {
        const durationSec = Math.round((r.durationMs || 0) / 1000);
        const isFailed = r.status === 'failed';
        const isPaused = r.status === 'paused';
        const roundBorderColor = isFailed ? '#ef4444' : isPaused ? '#f97316' : '#334155';
        const capsule = r.isCompacted ? r.compactSummary : undefined;

        return `
        <div class="round-card" style="border-left: 4px solid ${roundBorderColor};">
          <div class="round-header">
            <span class="round-badge">Round ${r.round} (${r.type.toUpperCase()})</span>
            ${
              r.repeatOfRound !== undefined
                ? `<span class="round-badge" style="background:#4c1d95;border:1px solid #7c3aed;color:#ddd6fe;margin-left:6px;" title="本轮指令与第 ${r.repeatOfRound} 轮完全相同；建议改用 retry_task 重跑失败轮">↻ 重复第 ${r.repeatOfRound} 轮指令</span>`
                : ''
            }
            <span class="round-meta">${new Date(r.timestamp).toLocaleTimeString()} · ${durationSec}s · Status: <strong>${r.status}</strong></span>
            ${
              r.tokenUsage
                ? `<span class="round-badge" style="margin-left:6px;background:#1e293b;border:1px solid #334155;color:#cbd5e1;" title="${escapeHtml(
                    `输入 ${r.tokenUsage.promptTokens} · 输出 ${r.tokenUsage.outputTokens} · 缓存 ${r.tokenUsage.cacheReadTokens} · 来源 ${r.tokenUsage.source}`,
                  )}">${
                    r.tokenUsage.available
                      ? `${r.tokenUsage.source === 'estimated' ? '~' : ''}⚡ ${formatTokenCount(r.tokenUsage.totalTokens)} tok (In ${formatTokenCount(r.tokenUsage.promptTokens)} · Out ${formatTokenCount(r.tokenUsage.outputTokens)})`
                      : '⚡ Token 未知'
                  }</span>`
                : ''
            }
          </div>

          ${
            r.assessment
              ? `
          <div class="section-title" style="color: ${r.assessment.success ? ((r.assessment.warnings?.length ?? 0) > 0 ? '#fbbf24' : '#34d399') : '#fca5a5'};">
            Response Health: ${escapeHtml(r.assessment.errorCode)} · exit=${r.assessment.exitCode}${
              (r.assessment.warnings?.length ?? 0) > 0 ? ` · ${r.assessment.warnings!.length} warning(s)` : ''
            }
          </div>
          <pre class="code-block" style="color:${r.assessment.success ? '#a7f3d0' : '#fecaca'}; max-height:120px;">chunks=${r.assessment.stream.chunkCount} (stdout ${r.assessment.stream.stdoutBytes}B / stderr ${r.assessment.stream.stderrBytes}B)
actualOutput=${r.assessment.stream.hasActualOutput} · continuous=${r.assessment.stream.continuous} · maxGap=${r.assessment.stream.maxGapMs}ms / threshold=${r.assessment.stream.gapThresholdMs}ms${
              r.assessment.stream.rawMaxGapMs !== undefined &&
              r.assessment.stream.rawMaxGapMs > r.assessment.stream.maxGapMs
                ? `\nrawGap=${r.assessment.stream.rawMaxGapMs}ms · hostSuspensionExcluded=${r.assessment.stream.excludedSuspensionMs ?? 0}ms (${r.assessment.stream.suspensionCount ?? 0} window(s))`
                : ''
            }
${r.assessment.reason ? `reason: ${escapeHtml(r.assessment.reason)}` : ''}${
              (r.assessment.warnings ?? [])
                .map((warning) => `\nwarning(${warning.code}): ${escapeHtml(warning.message)}`)
                .join('')
            }</pre>`
              : ''
          }

          <div class="prompt-section">
            <div class="section-title">Caller Instructions / Feedback:</div>
            <pre class="code-block">${escapeHtml(r.prompt)}</pre>
          </div>

          ${
            r.touchedFiles && r.touchedFiles.length > 0
              ? `
          <div class="section-title" style="color: #93c5fd;">Files Touched This Round (git probe):</div>
          <div style="display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px;">
            ${r.touchedFiles
              .map(
                (f) =>
                  `<span style="background: rgba(59, 130, 246, 0.18); border: 1px solid rgba(59, 130, 246, 0.45); padding: 2px 8px; border-radius: 4px; font-size: 12px; color: #bfdbfe;">${escapeHtml(f)}</span>`,
              )
              .join('')}
          </div>`
              : ''
          }

          ${
            r.output
              ? capsule
                ? `
          <div class="output-section" style="border:1px solid #4c1d95; border-radius:8px; padding:10px; background:rgba(76,29,149,0.12);">
            <div class="section-title" style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
              🧠 Agent 上下文提纯胶囊
              <span class="round-meta">原文 ${capsule.metrics.originalChars.toLocaleString()} 字符 → 摘要 ${capsule.metrics.compactedChars.toLocaleString()} 字符 · 保留 ${(capsule.metrics.compressionRatio * 100).toFixed(1)}% · 来源 ${escapeHtml(capsule.metrics.source)}</span>
            </div>
            <pre class="code-block output-text">${escapeHtml(r.output)}</pre>
            <div style="margin-top:8px;display:flex;gap:8px;flex-wrap:wrap;">
              <button class="secondary" style="padding:4px 10px;font-size:12px;" onclick="openRoundRawLog(${r.round})">📄 查看原始全文</button>
              ${
                capsule.executionLogPath
                  ? `<span class="round-meta" title="${escapeHtml(capsule.executionLogPath)}">执行日志: ${escapeHtml(capsule.executionLogPath)}</span>`
                  : ''
              }
            </div>
          </div>`
                : `
          <div class="output-section">
            <div class="section-title" style="display: flex; align-items: center; gap: 8px;">
              ${r.status === 'running' ? `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 6px #10b981;"></span>` : ''}
              Receiver Output ${r.status === 'running' ? '(实时流式更新中 · 每30s自动刷新):' : ':'}
            </div>
            <pre class="code-block output-text">${escapeHtml(r.output)}</pre>
          </div>`
              : ''
          }

          ${
            r.error
              ? `
          <div class="error-section">
            <div class="section-title" style="color: #ef4444;">Execution Error:</div>
            <pre class="code-block error-text">${escapeHtml(r.error)}</pre>
          </div>`
              : ''
          }

          ${
            r.interruption
              ? `
          <div class="error-section">
            <div class="section-title" style="color: #f97316;">Interruption (${escapeHtml(r.interruption.kind)}):</div>
            <pre class="code-block" style="color:#fdba74;">${escapeHtml(r.interruption.reason)}${r.interruption.by ? `\n— by ${escapeHtml(r.interruption.by)} at ${new Date(r.interruption.at).toLocaleString()}` : ''}</pre>
          </div>`
              : ''
          }
        </div>
      `;
      })
      .join('');

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Agent Task Details</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      padding: 24px;
      margin: 0;
      color: var(--vscode-editor-foreground, #cccccc);
      background-color: var(--vscode-editor-background, #1e1e1e);
      line-height: 1.5;
    }
    .header {
      border-bottom: 1px solid var(--vscode-panel-border, #333);
      padding-bottom: 16px;
      margin-bottom: 20px;
    }
    .title-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .title {
      font-size: 20px;
      font-weight: 600;
      margin: 0;
    }
    .badge {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 12px;
      font-size: 12px;
      font-weight: 600;
      color: #fff;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 12px;
      margin-top: 14px;
      font-size: 13px;
    }
    .meta-item {
      background: rgba(255, 255, 255, 0.04);
      padding: 8px 12px;
      border-radius: 6px;
    }
    .meta-label {
      opacity: 0.6;
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .meta-val {
      font-family: monospace;
      margin-top: 3px;
      word-break: break-all;
    }
    .action-bar {
      display: flex;
      gap: 10px;
      margin: 20px 0;
      flex-wrap: wrap;
    }
    button {
      background: var(--vscode-button-background, #0e639c);
      color: var(--vscode-button-foreground, #ffffff);
      border: none;
      padding: 8px 14px;
      font-size: 13px;
      border-radius: 4px;
      cursor: pointer;
      display: flex;
      align-items: center;
      gap: 6px;
      font-weight: 500;
    }
    button:hover {
      background: var(--vscode-button-hoverBackground, #1177bb);
    }
    button.secondary {
      background: var(--vscode-button-secondaryBackground, #3a3d41);
      color: var(--vscode-button-secondaryForeground, #ffffff);
    }
    button.secondary:hover {
      background: var(--vscode-button-secondaryHoverBackground, #45494e);
    }
    button.resume-btn {
      background: #059669;
      color: #ffffff;
      font-weight: 600;
    }
    button.resume-btn:hover {
      background: #10b981;
    }
    button.notify-btn {
      background: #4f46e5;
      color: #ffffff;
      font-weight: 600;
    }
    button.notify-btn:hover {
      background: #6366f1;
    }
    .round-card {
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 8px;
      padding: 16px;
      margin-bottom: 16px;
    }
    .round-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }
    .round-badge {
      font-weight: 600;
      font-size: 14px;
      color: #60a5fa;
    }
    .round-meta {
      font-size: 12px;
      opacity: 0.7;
    }
    .section-title {
      font-size: 12px;
      font-weight: 600;
      margin-top: 8px;
      margin-bottom: 4px;
      opacity: 0.8;
    }
    .code-block {
      background: rgba(0, 0, 0, 0.3);
      padding: 10px 12px;
      border-radius: 6px;
      font-family: Consolas, "Courier New", monospace;
      font-size: 12px;
      white-space: pre-wrap;
      word-break: break-all;
      max-height: 400px;
      overflow-y: auto;
      border: 1px solid rgba(255, 255, 255, 0.05);
    }
    .output-text {
      color: #93c5fd;
    }
    .error-text {
      color: #fca5a5;
    }
    .rework-box {
      margin-top: 24px;
      padding: 16px;
      background: rgba(255, 255, 255, 0.03);
      border-radius: 8px;
      border: 1px solid rgba(255, 255, 255, 0.1);
    }
    textarea {
      width: 100%;
      box-sizing: border-box;
      background: rgba(0,0,0,0.25);
      border: 1px solid rgba(255,255,255,0.15);
      color: #fff;
      padding: 10px;
      border-radius: 4px;
      font-family: inherit;
      resize: vertical;
      min-height: 80px;
      margin-bottom: 10px;
    }
  </style>
</head>
<body>
  <div class="header">
    <div class="title-row">
      <h2 class="title">[${escapeHtml(thread.initiatorAgent)} ➔ ${escapeHtml(thread.targetAgent)}] ${escapeHtml(thread.title)}</h2>
      <span class="badge" style="background: ${badgeColor};">${escapeHtml(thread.status.toUpperCase())}</span>
    </div>

    <div class="meta-grid">
      <div class="meta-item">
        <div class="meta-label">Thread ID</div>
        <div class="meta-val">${escapeHtml(thread.threadId)}</div>
      </div>
      <div class="meta-item">
        <div class="meta-label">Initiator Agent / Session</div>
        <div class="meta-val">${escapeHtml(thread.initiatorAgent)} / ${escapeHtml(thread.callerSessionId || '未绑定 (点击唤醒)')}</div>
      </div>
      <div class="meta-item">
        <div class="meta-label">Receiver Native Session</div>
        <div class="meta-val">${escapeHtml(thread.receiverSessionId)}</div>
      </div>
      <div class="meta-item">
        <div class="meta-label">Current Round</div>
        <div class="meta-val">${thread.currentRound || 1}</div>
      </div>
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Model Routing / 模型路由 (发送 ➔ 接收)</div>
        <div class="meta-val">
          <strong>发送 ${escapeHtml(thread.initiatorAgent)}</strong> [${escapeHtml(thread.initiatorModel || 'Agent 默认')}${
            thread.initiatorModelSource === 'configured' ? ' · 配置默认' : ''
          }]
          ➔
          <strong>接收 ${escapeHtml(thread.targetAgent)}</strong> [${escapeHtml(thread.targetModel || 'Agent 默认')}]
        </div>
      </div>
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Callee Token / 被调用方 Token（仅接收方）</div>
        <div class="meta-val">
          ${
            tokenSummary.measuredRounds === 0
              ? `<span style="color:#94a3b8;">未采集到 Token 数据（历史轮次早于本功能，或 CLI 未上报）</span>`
              : `🪙 累计 <strong>${tokenTilde}${formatTokenCount(tokenSummary.totalTokens)}</strong>
                 · 输入 ${formatTokenCount(tokenSummary.totalPromptTokens)}（缓存命中 ${formatTokenCount(tokenSummary.totalCacheReadTokens)}）
                 · 输出 ${formatTokenCount(tokenSummary.totalOutputTokens)}
                 · ${tokenSummary.measuredRounds}/${tokenSummary.totalRounds} 轮已计量`
          }
          ${
            tokenSummary.totalCostUsd !== undefined
              ? ` · API 等价成本 $${tokenSummary.totalCostUsd.toFixed(4)}`
              : ''
          }
          ${
            tokenSummary.isPartialOrEstimated && tokenSummary.measuredRounds > 0
              ? `<br/><span style="color:#fbbf24;">含估算或不完整数据（~ 标记）；输入 Token 无法从文本推算，缺失时不作估算。</span>`
              : ''
          }
        </div>
      </div>
      ${
        thread.modelNotice || thread.modelApproval
          ? `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Model Decision / 模型切换裁决</div>
        <div class="meta-val" style="color: #fde68a;">
          ${
            thread.modelApproval
              ? `用户裁决：<strong>${escapeHtml(thread.modelApproval.status)}</strong>（请求 ${escapeHtml(
                  thread.modelApproval.requestedModel,
                )} · 默认 ${escapeHtml(thread.modelApproval.defaultModel || 'Agent 默认')}）<br/>`
              : ''
          }
          ${thread.modelNotice ? escapeHtml(thread.modelNotice) : ''}
        </div>
      </div>`
          : ''
      }
      ${
        thread.retargets && thread.retargets.length > 0
          ? (() => {
              const last = thread.retargets[thread.retargets.length - 1];
              const rows = thread.retargets
                .map(
                  (r) =>
                    `· ${new Date(r.at).toLocaleTimeString()} ${escapeHtml(r.fromAgent)}${
                      r.fromModel ? `(${escapeHtml(r.fromModel)})` : ''
                    } ➔ ${escapeHtml(r.toAgent)}${r.toModel ? `(${escapeHtml(r.toModel)})` : ''}` +
                    `${r.sessionReset ? ' · 已换新原生会话' : ''}${r.reason ? ` · ${escapeHtml(r.reason)}` : ''}`,
                )
                .join('<br/>');
              return `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Executor Switch / 执行者切换（共 ${thread.retargets!.length} 次）</div>
        <div class="meta-val" style="color: #bfdbfe;">
          ${rows}<br/>
          <span style="color:#93c5fd;">当前执行者：${escapeHtml(last.toAgent)}${
            last.toModel ? ` (${escapeHtml(last.toModel)})` : ''
          }</span>
        </div>
      </div>`;
            })()
          : ''
      }
      <div class="meta-item">
        <div class="meta-label">Workspace</div>
        <div class="meta-val">${escapeHtml(thread.workspacePath)}</div>
      </div>
      <div class="meta-item">
        <div class="meta-label">Created At</div>
        <div class="meta-val">${new Date(thread.createdAt).toLocaleString()}</div>
      </div>
      ${
        thread.scopeBlockedBy
          ? `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label" style="color: #fbbf24;">Scope Block（文件归属互斥）</div>
        <div class="meta-val" style="color: #fde68a;">
          以下文件已被任务 <strong>${escapeHtml(thread.scopeBlockedBy.threadId)}</strong>${
            thread.scopeBlockedBy.title ? `「${escapeHtml(thread.scopeBlockedBy.title)}」` : ''
          } 声明或实际修改，本任务不会启动，直到该任务收口（approve + record_merge_request）或调用 allow_scope_overlap 放行：
          <div style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px;">
            ${thread.scopeBlockedBy.files
              .map(
                (f) =>
                  `<span style="background: rgba(251, 191, 36, 0.18); border: 1px solid rgba(251, 191, 36, 0.45); padding: 2px 8px; border-radius: 4px; font-size: 12px; color: #fde68a;">${escapeHtml(f)}</span>`,
              )
              .join('')}
          </div>
        </div>
      </div>`
          : ''
      }
      ${
        thread.expectedFiles && thread.expectedFiles.length > 0
          ? `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Expected File Scope (文件归属互斥)</div>
        <div class="meta-val" style="display: flex; flex-wrap: wrap; gap: 6px; margin-top: 6px;">
          ${thread.expectedFiles
            .map(
              (f) =>
                `<span style="background: rgba(168, 85, 247, 0.2); border: 1px solid rgba(168, 85, 247, 0.4); padding: 2px 8px; border-radius: 4px; font-size: 12px; color: #e9d5ff;">${escapeHtml(f)}</span>`,
            )
            .join('')}
        </div>
      </div>`
          : `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Expected File Scope (文件归属互斥)</div>
        <div class="meta-val" style="color: #94a3b8;">
          未声明（早期派发或 allowNoScope）。文件归属互斥对它不生效；但本轮实际改动的文件仍会由 git probe 记录，用于保护后续任务。
        </div>
      </div>`
      }
      ${
        thread.mergeRequest
          ? `
      <div class="meta-item" style="grid-column: 1 / -1;">
        <div class="meta-label">Merge Request (调用者收口)</div>
        <div class="meta-val" style="margin-top: 6px; line-height: 1.7;">
          <strong>${escapeHtml(thread.mergeRequest.status.toUpperCase())}</strong>
          ${thread.mergeRequest.url ? (safeMrUrl ? ` · <a href="${escapeHtml(safeMrUrl)}" style="color:#60a5fa;">${escapeHtml(safeMrUrl)}</a>` : ` · ${escapeHtml(thread.mergeRequest.url)}`) : ''}
          ${thread.mergeRequest.branch ? `<br/>branch: ${escapeHtml(thread.mergeRequest.branch)}` : ''}
          ${thread.mergeRequest.commit ? ` · commit: ${escapeHtml(thread.mergeRequest.commit)}` : ''}
          ${thread.mergeRequest.remote ? `<br/>remote: ${escapeHtml(thread.mergeRequest.remote)}` : ''}
          ${thread.mergeRequest.baseBranch ? ` · base: ${escapeHtml(thread.mergeRequest.baseBranch)}` : ''}
          ${thread.mergeRequest.summary ? `<br/>${escapeHtml(thread.mergeRequest.summary)}` : ''}
        </div>
      </div>`
          : ''
      }
    </div>
  </div>

  ${
    thread.status === 'completed'
      ? `
  <div style="background: rgba(16, 185, 129, 0.15); border: 1px solid #10b981; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">🎉</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #34d399;">任务已通过验收并正式归档 (Task Approved & Completed)</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">由 <strong>${escapeHtml(thread.initiatorAgent)}</strong> 独立运行全部测试套件完成验证，既有测试与回归测试全绿通过。</div>
    </div>
  </div>`
      : thread.status === 'queued'
      ? `
  <div style="background: rgba(168, 85, 247, 0.15); border: 1px solid #a855f7; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⏳</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #c084fc;">排队等待中 · 检测到文件修改范围冲突 (Task Queued)</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">${escapeHtml(thread.summary.lastOutputSnippet)}</div>
      <div style="font-size: 12px; opacity: 0.75; margin-top: 2px;">当前任务正在等待冲突任务释放文件锁定，释放后网格将自动唤醒开始执行，无需人工干预。</div>
    </div>
  </div>`
      : thread.status === 'waiting_review'
      ? thread.reviewWake && thread.reviewWake.round === (thread.currentRound || 1)
        ? `
  <div style="background: rgba(239, 68, 68, 0.15); border: 1px solid #ef4444; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⚠️</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #f87171;">${
        thread.reviewWake.kind === 'timeout'
          ? `检视卡住 · 第 ${thread.currentRound || 1} 轮等待 ${escapeHtml(thread.initiatorAgent)} 验收 (Reviewer Timeout)`
          : `检视唤醒失败 · 第 ${thread.currentRound || 1} 轮等待 ${escapeHtml(thread.initiatorAgent)} 验收 (Review Wake Failed)`
      }</div>
      <div style="font-size: 13px; opacity: 0.95; margin-top: 6px;"><strong>失败原因：</strong>${escapeHtml(thread.reviewWake.lastError)}${thread.reviewWake.exitCode !== undefined ? `（退出码 ${thread.reviewWake.exitCode}）` : ''}</div>
      ${
        thread.reviewWake.quotaExhausted
          ? `<div style="font-size: 13px; margin-top: 4px; color: #fca5a5;"><strong>配额/限流不足：</strong>${escapeHtml(thread.reviewWake.quotaReason || '429')}${thread.reviewWake.quotaResetsAtFormatted ? ` · 预计 ${escapeHtml(thread.reviewWake.quotaResetsAtFormatted)} 恢复` : ''}</div>`
          : ''
      }
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">已失败 ${thread.reviewWake.attempts} 次。${
        thread.reviewWake.gaveUp
          ? '自动重试已停止，请点击下方“🔁 重新检视下发者（唤醒失败）”手动重试。'
          : thread.reviewWake.autoRetryAt
            ? `桥接将在 ${new Date(thread.reviewWake.autoRetryAt).toLocaleString()} 自动重新唤醒（第 ${(thread.reviewWake.autoRetryAttempts || 0) + 1} 次自动尝试）。`
            : ''
      }</div>
      <div style="font-size: 12px; opacity: 0.75; margin-top: 4px;">该任务仍占据其 workspace 的队列队头，后续任务会持续排队，直到重新检视成功、验收或删除。</div>
    </div>
  </div>`
        : `
  <div style="background: rgba(59, 130, 246, 0.15); border: 1px solid #3b82f6; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">🔍</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #60a5fa;">已完成第 ${thread.currentRound || 1} 轮交付 · 等待下发者自动化跑测验收中</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">已主动唤醒 <strong>${escapeHtml(thread.initiatorAgent)}</strong>，正在后台独立执行全量构建与回归跑测（通常耗时 30~60 秒）。任务正常流转中，请等待审查结论。</div>
    </div>
  </div>`
      : thread.status === 'paused'
      ? `
  <div style="background: rgba(249, 115, 22, 0.15); border: 1px solid #f97316; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⏸️</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #fb923c;">任务已暂停 · 第 ${thread.currentRound || 1} 轮已中断</div>
      <div style="font-size: 13px; opacity: 0.95; margin-top: 4px;"><strong>中断原因：</strong>${escapeHtml(thread.interruption?.reason || '未记录')}${thread.interruption?.by ? `（by ${escapeHtml(thread.interruption.by)}）` : ''}</div>
      <div style="font-size: 12px; opacity: 0.75; margin-top: 4px;">Agent 进程已停止，当前轮次与原生会话已保留。点击下方“恢复执行”可从原轮次继续。</div>
    </div>
  </div>`
      : (thread.status === 'running' || thread.status === 'reworking')
      ? `
  <div style="background: rgba(245, 158, 11, 0.15); border: 1px solid #f59e0b; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⏳</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #fbbf24;">第 ${thread.currentRound || 1} 轮正在自主执行中 (${escapeHtml(thread.targetAgent)})</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;"><strong>${escapeHtml(thread.targetAgent)}</strong> 正在原生会话中修改代码与编写测试，后台每 30 秒流式同步一次最新输出，无需人工干预。</div>
    </div>
  </div>`
      : thread.status === 'failed'
      ? `
  <div style="background: rgba(239, 68, 68, 0.15); border: 1px solid #ef4444; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⚠️</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #f87171;">第 ${thread.currentRound || 1} 轮执行异常中断</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">${thread.interruption?.reason ? `<strong>中断原因：</strong>${escapeHtml(thread.interruption.reason)}<br/>` : ''}进程非零退出、看门狗或执行超时。可点击下方“重试失败轮次”按钮重新执行该轮次。</div>
    </div>
  </div>`
      : ''
  }

  <div class="action-bar">
    ${(thread.status === 'failed' || thread.rounds.some((r) => r.status === 'failed')) ? `<button class="retry-btn" style="background: #dc2626; color: white;" onclick="retryFailedRound()">🔄 重试失败轮次 (Retry Failed)</button>` : ''}
    ${thread.status === 'waiting_review' && thread.reviewWake && thread.reviewWake.round === (thread.currentRound || 1) ? `<button class="notify-btn" onclick="wakeUpCaller()">🔁 重新检视下发者（唤醒失败）</button>` : ''}
    ${(thread.status === 'running' || thread.status === 'reworking' || thread.status === 'queued') ? `<button style="background:#ea580c;color:white;" onclick="pauseTask()">⏸ 暂停任务 (Pause)</button>` : ''}
    ${thread.status === 'paused' ? `<button class="resume-btn" onclick="resumeTask()">▶ 恢复执行 (Resume)</button>` : ''}
    ${(thread.status === 'failed' || thread.status === 'waiting_review') ? `<button class="resume-btn" onclick="resumeTask()">▶ 下发返修要求 / 重新执行</button>` : ''}
    ${thread.status !== 'completed' ? `<button onclick="approveTask()">✓ 手动验收通过 (Approve Task)</button>` : ''}
    <button style="background:#7f1d1d;color:white;" onclick="deleteTask()">🗑 删除任务 (Delete)</button>
    <button class="secondary" onclick="printRecentLogs()">📄 打印最近1000行日志 (Print 1,000 Lines)</button>
    <button class="secondary" onclick="openTerminal()">💻 Open Terminal in Session</button>
    <button class="secondary" onclick="refresh()">⟳ Refresh</button>
  </div>

  <div class="live-logs-box" style="margin-top: 16px; margin-bottom: 24px; background: rgba(0,0,0,0.3); border: 1px solid rgba(255,255,255,0.12); border-radius: 8px; padding: 16px;">
    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom: 12px; flex-wrap:wrap; gap:8px;">
      <div style="font-weight:600; font-size:14px; display:flex; align-items:center; gap:8px;">
        ${thread.status === 'running' || thread.status === 'reworking' ? `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#10b981;box-shadow:0 0 6px #10b981;"></span>` : ''}
        <span>最近 1,000 行实时日志 (Live Logs)</span>
        <span style="font-size:12px;opacity:0.6;font-weight:normal;">(${recentLogs.returnedLines} / ${recentLogs.totalLines} 行${recentLogs.isRunning ? ' · 实时运行中' : ''})</span>
      </div>
      <div style="display:flex; gap:8px;">
        <button class="secondary" style="padding:4px 10px;font-size:12px;" onclick="printRecentLogs()">在 VS Code 输出面板打印</button>
        <button class="secondary" style="padding:4px 10px;font-size:12px;" onclick="copyLogs()">复制日志</button>
      </div>
    </div>
    <pre id="recentLogViewer" class="code-block output-text" style="max-height: 380px; overflow-y: auto; background: #0b0f19; border: 1px solid #1e293b; padding: 12px; border-radius: 6px; font-family: Consolas, monospace; font-size: 12px; line-height: 1.5; color: #e2e8f0; white-space: pre-wrap; word-break: break-all;">${escapeHtml(recentLogs.logs || '(暂无输出日志)')}</pre>
  </div>

  <div class="timeline">
    <h3>Interaction Timeline</h3>
    ${roundsHtml}
  </div>

  ${
    thread.status !== 'completed'
      ? `
  <div class="rework-box">
    <h3>Manual Rework Intervention</h3>
    <p style="font-size: 13px; opacity: 0.8;">Trigger next rework round for ${escapeHtml(thread.targetAgent)} inside the SAME native session:</p>
    <textarea id="reworkFeedback" placeholder="Enter feedback, missing coverage, or fixes required..."></textarea>
    <button onclick="submitRework()">Submit Rework Round</button>
  </div>`
      : ''
  }

  <script>
    const vscode = acquireVsCodeApi();

    function retryFailedRound() {
      vscode.postMessage({ command: 'retryFailedRound' });
    }

    function resumeTask() {
      vscode.postMessage({ command: 'resumeTask' });
    }

    function pauseTask() {
      vscode.postMessage({ command: 'pauseTask' });
    }

    function deleteTask() {
      vscode.postMessage({ command: 'deleteTask' });
    }

    function wakeUpCaller() {
      vscode.postMessage({ command: 'wakeUpCaller' });
    }

    function approveTask() {
      vscode.postMessage({ command: 'approve' });
    }

    function openTerminal() {
      vscode.postMessage({ command: 'openTerminal' });
    }

    function printRecentLogs() {
      vscode.postMessage({ command: 'printRecentLogs' });
    }

    function openRoundRawLog(round) {
      vscode.postMessage({ command: 'openRoundRawLog', round: round });
    }

    function copyLogs() {
      const el = document.getElementById('recentLogViewer');
      if (el) {
        navigator.clipboard.writeText(el.innerText).then(() => {
          vscode.postMessage({ command: 'copiedNotice' });
        });
      }
    }

    function refresh() {
      vscode.postMessage({ command: 'refresh' });
    }

    // Auto-scroll log viewer to bottom on load
    const logViewer = document.getElementById('recentLogViewer');
    if (logViewer) {
      logViewer.scrollTop = logViewer.scrollHeight;
    }

    function submitRework() {
      const text = document.getElementById('reworkFeedback').value;
      if (!text || !text.trim()) {
        alert('Please enter rework feedback');
        return;
      }
      vscode.postMessage({ command: 'requestRework', feedback: text });
      document.getElementById('reworkFeedback').value = '';
    }
  </script>
</body>
</html>`;
  }

  public dispose() {
    TaskDetailPanel.currentPanel = undefined;
    this.panel.dispose();
    while (this.disposables.length) {
      const x = this.disposables.pop();
      if (x) {
        x.dispose();
      }
    }
  }
}
