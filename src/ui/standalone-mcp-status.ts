import * as path from 'path';
import * as vscode from 'vscode';
import { compareVersions } from '../bridge/build-staleness';
import {
  availableMcpBuilds, discoverTaskMcpProcesses, McpBuild, McpInstance, McpRecord, McpStatus,
  mcpBuildIsStale, readMcpRecords, requestMcpControl, UnmanagedMcpProcess,
} from '../bridge/standalone-mcp-status';
import type { TaskStore } from '../bridge/task-store';

const COMMAND = 'agent-mesh-bridge.showStandaloneMcpStatus';

interface ManagedRow {
  record: McpRecord;
  status?: McpStatus;
  latest?: McpBuild;
  stale: boolean;
}

export function registerStandaloneMcpStatus(context: vscode.ExtensionContext, taskStore: TaskStore): void {
  const storageDir = path.dirname(taskStore.getStoragePath());
  const bar = vscode.window.createStatusBarItem('agent-mesh-bridge.standalone-mcp', vscode.StatusBarAlignment.Left, 39);
  bar.name = 'Agent Bridge Standalone MCP';
  bar.text = '$(plug) MCP';
  bar.command = COMMAND;
  bar.show();
  const output = vscode.window.createOutputChannel('Agent Bridge 独立 MCP');
  let warnedBuild: string | undefined;
  let refreshing: ReturnType<typeof inspect> | undefined;

  async function inspect() {
    const issues: string[] = [];
    const [recordsResult, processesResult] = await Promise.allSettled([
      Promise.resolve().then(() => readMcpRecords(storageDir)),
      Promise.resolve().then(() => discoverTaskMcpProcesses(taskStore.getAllTasks())),
    ]);
    const processes = processesResult.status === 'fulfilled' ? processesResult.value : [];
    const parents = new Map(processes.map(process => [process.pid, process.parentPid]));
    const records = recordsResult.status === 'fulfilled'
      ? recordsResult.value.filter(({ instance }) => parents.has(instance.pid) && parents.get(instance.pid) === instance.parentPid)
      : [];
    if (recordsResult.status === 'rejected') issues.push(`实例登记读取失败 ${recordsResult.reason}`);
    if (processesResult.status === 'rejected') issues.push('暂时无法核验 Bridge 任务的 MCP 进程');
    const managed = await Promise.all(records.map(async (record): Promise<ManagedRow> => {
      const reply = await requestMcpControl(record, { action: 'status' }).catch(() => undefined);
      const status = reply && 'status' in reply ? reply.status : undefined;
      return { record, status, stale: false };
    }));
    const builds = availableMcpBuilds(context.extensionPath, context.extension.id,
      managed.map(row => (row.status || row.record.instance).build.packageRoot));
    for (const row of managed) {
      const loaded = (row.status || row.record.instance).build;
      row.latest = builds.find(build => compareVersions(build.version, loaded.version) >= 0);
      row.stale = !!row.latest && mcpBuildIsStale(loaded, row.latest);
    }
    const pids = new Set(records.map(record => record.instance.pid));
    return { managed, unmanaged: processes.filter(process => !pids.has(process.pid)), issues, latest: builds[0] };
  }

  function refresh() {
    if (refreshing) return refreshing;
    refreshing = inspect().then(result => {
      const { managed, unmanaged, issues } = result;
      const stale = managed.filter(row => row.stale);
      const unknown = managed.filter(row => !row.status || !row.latest).length + unmanaged.length;
      const limited = managed.filter(row => row.status && !row.status.admission.ready).length;
      const attention = stale.length || unknown || limited || issues.length;
      const count = managed.length + unmanaged.length;
      bar.text = `$(${attention ? 'warning' : 'plug'}) MCP${count ? ` ${count}` : ''}`;
      bar.backgroundColor = attention ? new vscode.ThemeColor('statusBarItem.warningBackground') : undefined;
      bar.tooltip = count
        ? `Bridge 任务 MCP · ${count} 个实例 · ${stale.length} 个构建落后 · ${unknown} 个待核验 · ${limited} 个启动受限`
        : issues.join('\n') || '当前没有 Bridge 任务的 MCP 连接';
      const newest = [...new Set(stale.map(row => row.latest!.fingerprint))].sort().join(',');
      if (newest && warnedBuild !== newest) {
        warnedBuild = newest;
        void vscode.window.showWarningMessage(`检测到 ${stale.length} 个独立 MCP 实例仍在使用较旧的构建。`, '查看状态')
          .then(choice => { if (choice) void vscode.commands.executeCommand(COMMAND); });
      }
      return result;
    }).finally(() => { refreshing = undefined; });
    return refreshing;
  }

  function showDetails(instance: McpInstance, status?: McpStatus, latest?: McpBuild, reason?: string) {
    output.clear();
    output.appendLine(`独立 MCP · ${instance.caller || '调用方未知'} · PID ${instance.pid}`);
    output.appendLine([
      `父进程 PID ${instance.parentPid} · Node ${instance.node} · stdio · Profile ${instance.profile || '默认'}`,
      `连接启动 ${new Date(instance.transportStartedAt).toLocaleString()}`,
      `运行实例加载 ${new Date(instance.loadedAt).toLocaleString()}`,
      `客户端启动入口 ${instance.launcher}`,
      `实际加载入口 ${instance.build.entry}`,
      `实际加载版本 ${instance.build.version} · SHA-256 ${instance.build.fingerprint}`,
      latest ? `最新本地构建 ${latest.version} · ${latest.entry}\nSHA-256 ${latest.fingerprint}` : '未找到可核验的本地构建',
      status ? `当前处理中的请求 ${status.pending}` : '当前请求状态未知',
      instance.admission.ready ? 'v2 启动准入已通过' : 'v2 启动受限 · 历史读取与受保护清理',
      ...instance.admission.blockers.map(blocker => `${blocker.code}${blocker.threadId ? ` · ${blocker.threadId}` : ''} · ${blocker.evidence}`),
      '',
      reason || '重启检查通过。切换前会再次检查客户端活动、任务运行和新构建的启动准入。',
    ].join('\n'));
    output.show(true);
  }

  async function openManaged(row: ManagedRow) {
    const reply = await requestMcpControl(row.record, { action: 'inspect' })
      .catch(error => ({ error: `无法确认实例安全状态（${error.message}）` }));
    const status = 'status' in reply ? reply.status : undefined;
    const instance = status || row.record.instance;
    const blockers = 'error' in reply ? [reply.error] : reply.status.restartBlockers || [];
    const builds = availableMcpBuilds(context.extensionPath, context.extension.id, [instance.build.packageRoot]);
    const latest = builds.find(build => compareVersions(build.version, instance.build.version) >= 0);
    const current = builds.find(build => build.entry === instance.build.entry &&
      compareVersions(build.version, instance.build.version) >= 0);
    const reasons = blockers.length ? blockers : latest ? [] : ['未找到可核验的本地编译构建'];
    const reason = reasons.length ? `当前无法强制重启\n${reasons.join('\n')}` : undefined;
    showDetails(instance, status, latest, reason);
    const options: Array<vscode.QuickPickItem & { build?: McpBuild }> = [{
      label: '$(output) 查看详细信息',
      description: reason ? '重启暂不可用' : '重启检查通过',
      detail: reasons[0],
    }];
    if (!reason && latest) {
      options.push({ label: '$(debug-restart) 强制重启到最新本地构建', description: latest.version, detail: latest.entry, build: latest });
      if (current && current.entry !== latest.entry) options.push({
        label: '$(debug-restart) 使用当前目录的构建重启', description: current.version, detail: current.entry, build: current,
      });
    }
    const selected = await vscode.window.showQuickPick(options, { title: `独立 MCP · PID ${instance.pid}` });
    const build = selected?.build;
    if (!build) return;
    const confirmed = await vscode.window.showWarningMessage(
      `重启独立 MCP 运行实例 PID ${instance.pid}？`,
      { modal: true, detail: `将使用 ${build.version}（${build.fingerprint.slice(0, 12)}）\n${build.entry}\nAgent 的 stdio 连接会保留。启动准入和任务保护仍生效。` },
      '重启',
    );
    if (!confirmed) return;
    await vscode.window.withProgress({ location: vscode.ProgressLocation.Notification, title: '正在重启独立 MCP' }, async () => {
      const restarted = await requestMcpControl(row.record, { action: 'restart', build })
        .catch(error => ({ error: `${error.message}。请刷新状态确认实际运行构建。` }));
      if ('error' in restarted) {
        showDetails(instance, status, latest, restarted.error);
        void vscode.window.showWarningMessage(`独立 MCP 重启未确认。${restarted.error.split('\n')[0]}`);
      } else {
        showDetails(restarted.status, restarted.status, build, '运行实例已重启。');
        void vscode.window.showInformationMessage(`独立 MCP 已重启，正在运行 ${build.version}（${build.fingerprint.slice(0, 12)}）。`);
      }
      await refresh();
    });
  }

  function showUnmanaged(process: UnmanagedMcpProcess, latest?: McpBuild) {
    output.clear();
    output.appendLine(`独立 MCP · PID ${process.pid} · 父进程 PID ${process.parentPid}\n启动入口 ${path.normalize(process.launcher)}`);
    output.appendLine('无法核验已加载的构建或进行中的请求。');
    if (latest) output.appendLine(`可用的本地构建 ${latest.version} · ${latest.entry}`);
    output.appendLine('此进程没有可验证的重启控制端点。请在相应 Agent 客户端断开并重新连接 MCP，待新版实例登记后再使用这里的重启操作。');
    output.show(true);
  }

  context.subscriptions.push(bar, output, vscode.commands.registerCommand(COMMAND, async () => {
    const { managed, unmanaged, issues, latest } = await refresh();
    const items: Array<vscode.QuickPickItem & { row?: ManagedRow; process?: UnmanagedMcpProcess }> = [
      ...managed.map(row => {
        const instance = row.status || row.record.instance;
        const state = !row.status ? '控制端点未响应' : row.status.restarting ? '重启中' : !instance.admission.ready ? '启动受限' : '已连接';
        const version = row.stale ? ` · 可用 ${row.latest!.version}（${row.latest!.fingerprint.slice(0, 8)}）` : '';
        return {
          row, label: `$(${row.stale || !row.status ? 'warning' : 'plug'}) ${instance.caller || '调用方未知'} · PID ${instance.pid}`,
          description: `${state} · ${instance.build.version}（${instance.build.fingerprint.slice(0, 8)}）${version}`,
          detail: instance.build.entry,
        };
      }),
      ...unmanaged.map(process => ({
        process, label: `$(warning) 未登记的 MCP · PID ${process.pid}`,
        description: '运行版本和请求状态未知', detail: process.launcher,
      })),
    ];
    if (!items.length) {
      void vscode.window.showInformationMessage(issues.join('。') || '当前没有 Bridge 任务的 MCP 连接。');
      return;
    }
    const item = await vscode.window.showQuickPick(items, {
      title: 'Bridge 任务 MCP 状态', placeHolder: issues.join(' · ') || '选择要检查的实例', matchOnDescription: true,
    });
    if (item?.row) await openManaged(item.row);
    if (item?.process) showUnmanaged(item.process, latest);
  }));
  const timer = setInterval(() => { void refresh(); }, 30_000);
  context.subscriptions.push({ dispose: () => clearInterval(timer) });
  void refresh();
}
