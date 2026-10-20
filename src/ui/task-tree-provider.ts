import * as vscode from 'vscode';
import { TaskStore } from '../bridge/task-store';
import { DelegationThread } from '../bridge/types';

export class TaskTreeProvider implements vscode.TreeDataProvider<TaskTreeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<TaskTreeItem | undefined | void> =
    new vscode.EventEmitter<TaskTreeItem | undefined | void>();
  readonly onDidChangeTreeData: vscode.Event<TaskTreeItem | undefined | void> =
    this._onDidChangeTreeData.event;

  private taskStore: TaskStore;
  private mode: 'active' | 'all';

  constructor(taskStore: TaskStore, mode: 'active' | 'all') {
    this.taskStore = taskStore;
    this.mode = mode;

    this.taskStore.on('changed', () => {
      this.refresh();
    });
  }

  public refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: TaskTreeItem): vscode.TreeItem {
    return element;
  }

  getChildren(element?: TaskTreeItem): Thenable<TaskTreeItem[]> {
    if (element) {
      // Sub-items: details / summary lines
      return Promise.resolve(element.subItems || []);
    }

    const tasks = this.taskStore.listTasks(1000, this.mode === 'active' ? 'active' : 'all');

    const items = tasks.map((thread) => {
      return this.createTaskTreeItem(thread);
    });

    return Promise.resolve(items);
  }

  private createTaskTreeItem(thread: DelegationThread): TaskTreeItem {
    const fromAbbr = this.formatAgentAbbr(thread.initiatorAgent);
    const toAbbr = this.formatAgentAbbr(thread.targetAgent);
    const label = `[${fromAbbr} ➔ ${toAbbr}] ${thread.title}`;

    const roundText = `Round ${thread.currentRound || 1}`;
    let iconName = 'symbol-misc';
    let statusLabel = thread.status.toUpperCase();

    switch (thread.status) {
      case 'queued':
        iconName = 'clock';
        statusLabel = `Queued (排队中)`;
        break;
      case 'running':
      case 'reworking':
        iconName = 'sync~spin';
        statusLabel =
          thread.status === 'reworking' ? `Reworking (${roundText})` : `Running (${roundText})`;
        break;
      case 'waiting_review':
        iconName = 'eye';
        statusLabel = `In-Review (${roundText})`;
        break;
      case 'paused':
        iconName = 'debug-pause';
        statusLabel = `Paused (已暂停)`;
        break;
      case 'completed':
        iconName = 'check';
        statusLabel = `Completed (${roundText})`;
        break;
      case 'failed':
        iconName = 'error';
        statusLabel = `Failed (${roundText})`;
        break;
    }

    const durationSec = Math.round((thread.summary.totalDurationMs || 0) / 1000);
    const durationText =
      durationSec > 60 ? `${Math.floor(durationSec / 60)}m${durationSec % 60}s` : `${durationSec}s`;

    const description = `${statusLabel} · ${durationText}`;

    const item = new TaskTreeItem(label, vscode.TreeItemCollapsibleState.Collapsed, thread);
    item.description = description;
    item.iconPath = new vscode.ThemeIcon(iconName);
    item.contextValue =
      thread.status === 'waiting_review'
        ? 'waiting_review'
        : thread.status === 'paused'
          ? 'paused_task'
          : 'task_item';

    // Click command
    item.command = {
      command: 'agent-mesh-bridge.openTaskDetail',
      title: 'Open Task Details',
      arguments: [thread.threadId],
    };

    // Sub items (Prompt snippet & Output snippet)
    const subItems: TaskTreeItem[] = [];

    const promptSnippet = thread.summary.lastPromptSnippet
      ? `A: "${thread.summary.lastPromptSnippet.replace(/[\r\n]+/g, ' ')}"`
      : 'No prompt snippet';
    const promptItem = new TaskTreeItem(promptSnippet, vscode.TreeItemCollapsibleState.None);
    promptItem.iconPath = new vscode.ThemeIcon('comment');
    subItems.push(promptItem);

    const outputSnippet = thread.summary.lastOutputSnippet
      ? `B: "${thread.summary.lastOutputSnippet.replace(/[\r\n]+/g, ' ')}"`
      : 'No output snippet';
    if (thread.interruption?.reason) {
      subItems.push(
        new TaskTreeItem(
          `Interrupted: ${thread.interruption.reason}`,
          vscode.TreeItemCollapsibleState.None,
        ),
      );
      const interruptionItem = subItems[subItems.length - 1];
      interruptionItem.iconPath = new vscode.ThemeIcon('stop-circle');
    }
    if (thread.mergeRequest) {
      const label = thread.mergeRequest.url
        ? `MR: ${thread.mergeRequest.url}`
        : thread.mergeRequest.commit
          ? `MR commit: ${thread.mergeRequest.commit}`
          : `MR: ${thread.mergeRequest.status}`;
      subItems.push(new TaskTreeItem(label, vscode.TreeItemCollapsibleState.None));
      const mrItem = subItems[subItems.length - 1];
      mrItem.iconPath = new vscode.ThemeIcon('git-pull-request');
    }
    const outputItem = new TaskTreeItem(outputSnippet, vscode.TreeItemCollapsibleState.None);
    outputItem.iconPath = new vscode.ThemeIcon('output');
    subItems.push(outputItem);

    item.subItems = subItems;
    return item;
  }

  private formatAgentAbbr(agent: string): string {
    switch (agent) {
      case 'antigravity':
        return 'Anti';
      case 'claude_code':
        return 'Claude';
      case 'codex':
        return 'Codex';
      case 'deepseek_harness':
        return 'DSH';
      case 'vscode_chat':
        return 'VSCode';
      default:
        return agent;
    }
  }
}

export class TaskTreeItem extends vscode.TreeItem {
  public subItems?: TaskTreeItem[];
  public thread?: DelegationThread;

  constructor(
    label: string,
    collapsibleState: vscode.TreeItemCollapsibleState,
    thread?: DelegationThread,
  ) {
    super(label, collapsibleState);
    this.thread = thread;
  }
}
