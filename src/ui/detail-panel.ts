import * as vscode from 'vscode';
import { TaskStore } from '../bridge/task-store';
import { ReworkController } from '../bridge/rework-controller';
import { DelegationThread } from '../bridge/types';

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

  private getHtmlForWebview(thread: DelegationThread): string {
    const escapeHtml = (str: string) => {
      return (str || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    };

    const statusBadgeColors: Record<string, string> = {
      running: '#f59e0b',
      reworking: '#f59e0b',
      waiting_review: '#3b82f6',
      completed: '#10b981',
      failed: '#ef4444',
    };

    const badgeColor = statusBadgeColors[thread.status] || '#6b7280';

    const roundsHtml = thread.rounds
      .map((r) => {
        const durationSec = Math.round((r.durationMs || 0) / 1000);
        const isFailed = r.status === 'failed';
        const roundBorderColor = isFailed ? '#ef4444' : '#334155';

        return `
        <div class="round-card" style="border-left: 4px solid ${roundBorderColor};">
          <div class="round-header">
            <span class="round-badge">Round ${r.round} (${r.type.toUpperCase()})</span>
            <span class="round-meta">${new Date(r.timestamp).toLocaleTimeString()} · ${durationSec}s · Status: <strong>${r.status}</strong></span>
          </div>
          
          <div class="prompt-section">
            <div class="section-title">Caller Instructions / Feedback:</div>
            <pre class="code-block">${escapeHtml(r.prompt)}</pre>
          </div>

          ${
            r.output
              ? `
          <div class="output-section">
            <div class="section-title">Receiver Output:</div>
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
      <div class="meta-item">
        <div class="meta-label">Workspace</div>
        <div class="meta-val">${escapeHtml(thread.workspacePath)}</div>
      </div>
      <div class="meta-item">
        <div class="meta-label">Created At</div>
        <div class="meta-val">${new Date(thread.createdAt).toLocaleString()}</div>
      </div>
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
      : thread.status === 'waiting_review'
      ? `
  <div style="background: rgba(59, 130, 246, 0.15); border: 1px solid #3b82f6; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">🔍</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #60a5fa;">已完成第 ${thread.currentRound || 1} 轮交付 · 等待下发者自动化跑测验收中</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">已主动唤醒 <strong>${escapeHtml(thread.initiatorAgent)}</strong>，正在后台独立执行全量构建与回归跑测（通常耗时 30~60 秒）。任务正常流转中，请等待审查结论。</div>
    </div>
  </div>`
      : (thread.status === 'running' || thread.status === 'reworking')
      ? `
  <div style="background: rgba(245, 158, 11, 0.15); border: 1px solid #f59e0b; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⏳</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #fbbf24;">第 ${thread.currentRound || 1} 轮正在自主执行中 (${escapeHtml(thread.targetAgent)})</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;"><strong>${escapeHtml(thread.targetAgent)}</strong> 正在原生会话中修改代码与编写测试，后台进程正常运转中，无需人工干预。</div>
    </div>
  </div>`
      : thread.status === 'failed'
      ? `
  <div style="background: rgba(239, 68, 68, 0.15); border: 1px solid #ef4444; border-radius: 8px; padding: 16px; margin-bottom: 20px; display: flex; align-items: center; gap: 14px;">
    <div style="font-size: 28px;">⚠️</div>
    <div>
      <div style="font-weight: 700; font-size: 16px; color: #f87171;">第 ${thread.currentRound || 1} 轮执行异常中断</div>
      <div style="font-size: 13px; opacity: 0.9; margin-top: 4px;">进程非零退出或执行超时。可点击下方“重试失败轮次”按钮重新执行该轮次。</div>
    </div>
  </div>`
      : ''
  }

  <div class="action-bar">
    ${(thread.status === 'failed' || thread.rounds.some((r) => r.status === 'failed')) ? `<button class="retry-btn" style="background: #dc2626; color: white;" onclick="retryFailedRound()">🔄 重试失败轮次 (Retry Failed)</button>` : ''}
    ${thread.status === 'waiting_review' ? `<button class="notify-btn" onclick="wakeUpCaller()">🔔 再次唤醒下发者检视</button>` : ''}
    ${(thread.status === 'failed' || thread.status === 'waiting_review') ? `<button class="resume-btn" onclick="resumeTask()">▶ 下发返修要求 / 重新执行</button>` : ''}
    ${thread.status !== 'completed' ? `<button onclick="approveTask()">✓ 手动验收通过 (Approve Task)</button>` : ''}
    <button class="secondary" onclick="openTerminal()">💻 Open Terminal in Session</button>
    <button class="secondary" onclick="refresh()">⟳ Refresh</button>
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

    function wakeUpCaller() {
      vscode.postMessage({ command: 'wakeUpCaller' });
    }

    function approveTask() {
      vscode.postMessage({ command: 'approve' });
    }

    function openTerminal() {
      vscode.postMessage({ command: 'openTerminal' });
    }

    function refresh() {
      vscode.postMessage({ command: 'refresh' });
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
