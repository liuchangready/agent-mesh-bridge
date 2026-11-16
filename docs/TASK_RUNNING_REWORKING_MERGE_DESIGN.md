# 合并 `running` 与 `reworking` 状态设计

## 1. 背景

当前任务状态把“正在执行初次开发”和“正在执行返修”拆成了 `running` 与 `reworking`。但两者在调度和生命周期上的含义相同：都代表开发者正在执行一个轮次；暂停、取消、进程登记、角色锁和执行结果处理也基本相同。轮次本身已有 `round.type`（`initial` / `rework` / `approval`），可以表达开发轮次的来源。

当前区分主要让状态字符串同时承载了生命周期和轮次类别，导致调用处反复写 `running || reworking`，并在任务树、MCP 状态筛选及文案中维护重复分支。

## 2. 目标与边界

- 统一所有活动中的开发者轮次使用任务状态 `running`。
- 通过活动轮次的 `round.type` 判断是否返修，并在需要时继续显示“返修中”。
- 老账本中的 `status: "reworking"` 可正常读取、展示、恢复和更新，不要求用户手动迁移数据。
- 清理仅因这两个状态分立而存在的重复分支、状态筛选项、文案和颜色映射。
- 不改变轮次编号、原生会话续用、角色交接、审阅、暂停/恢复、合并和终态语义。

本设计不合并 `RoundStatus`，不改 `RoundType`，也不改变 `waiting_review`、`waiting_merge` 等其他任务状态。

## 3. 目标状态模型

从 `ThreadStatus` 移除 `reworking`，开发执行统一为：

```ts
export type ThreadStatus =
  | 'queued'
  | 'running'
  | 'waiting_review'
  | 'waiting_merge'
  | 'wait_decision'
  | 'merge_failed'
  | 'paused'
  | 'review_failed'
  | 'completed'
  | 'failed';
```

活动开发轮次可由 `findInFlightRound(thread)` 定位；其 `type === 'rework'` 表示返修轮，`type === 'initial'` 表示初始开发轮。`approval` 轮是记录验收的账本轮次，不用于识别开发执行类型。

| 任务状态 | 活动轮次 | 含义 |
|---|---|---|
| `running` | `initial` | 初次开发执行中 |
| `running` | `rework` | 按 Reviewer 意见返修执行中 |
| `queued` | 可能有已排队轮次 | 等待调度；不得仅凭轮次类型标成运行中 |
| `waiting_review` | 无活动开发轮次 | 开发结果已交接，等待检视 |

开发轮次成功、失败、暂停后的任务状态仍分别为 `waiting_review`、`failed`、`paused`。返修成功后仍回到 `waiting_review`；如果任务正在合并审阅闭环中，现有合并逻辑继续决定后续状态。

## 4. 状态与轮次的写入规则

所有新建或恢复的开发执行路径按以下规则写状态：

1. 开始执行 `initial` 或 `rework` 轮次时，任务状态均写 `running`。
2. 新建返修轮时保留 `round.type = 'rework'`，不得用任务状态记录返修类别。
3. `retry_task` / `resume_task` 根据被恢复轮次原有的 `round.type` 续跑；任务状态设为 `running`，不得创建新的返修轮或改写轮次类型。
4. 只有存在有效的活动开发轮次时，才能用其类型显示“初始执行中”或“返修中”。不能仅凭 `currentRound` 或历史最近一轮推断正在返修，因为 `currentRound` 可能指向 approval 轮，且 paused/queued 状态可能没有正在执行的轮次。
5. 角色互斥继续由角色锁和执行登记保证。统一后的 `running` 仍不能单独作为“当前进程一定是 developer”的证明；reviewer / developer 的准入必须保留现有会话、角色及 bootstrap 校验。

## 5. 旧账本兼容

老任务 JSON 可能持久化了 `status: "reworking"`。加载任务文件时，在进入内存模型前将其规范化为 `running`；保留所有轮次数据，尤其是 `round.type`、轮次 prompt/output、session ID、工作树和中断记录。规范化不得创建新轮次、重置会话、触发返修或改变时间戳。

使用“读时规范化”兼容历史文件：无需启动时批量改写全部账本文件；任务下一次正常持久化时自然写入 `running`。如果任务没有后续更新，磁盘上的旧字符串可以暂时保留，但任何 API/UI 返回的状态都应是规范化后的状态。

`list_recent_tasks` 的 `statusFilter` 不再把 `reworking` 暴露为独立生命周期状态。为避免既有调用方传入旧筛选值后突然查不到任务，可在筛选入口短期保留 `reworking` 兼容别名：它筛选规范化为 `running` 且活动轮次为 `rework` 的任务；该别名不得重新进入 `ThreadStatus` 或任务返回 payload。若项目决定不保留 API 兼容，则应同步说明这是 MCP 输入 schema 的破坏性变更。

## 6. 代码收敛与清理清单

### 核心状态与存储

- `src/bridge/types.ts`：从 `ThreadStatus` 删除 `reworking`；轮次类型继续作为返修语义来源。
- `src/bridge/task-store.ts`：读取线程文件时规范化旧状态；`active` 筛选中删除重复的 `reworking` 分支；增加旧 `statusFilter` 别名（若采用兼容方案）。
- `src/bridge/async-dispatcher.ts`、`src/bridge/task-supervisor.ts`、`src/bridge/rework-controller.ts`：所有开始/重试/恢复 `rework` 轮的任务状态改写为 `running`；删除只为区分初次与返修而存在的状态三元判断。结果状态和轮次 `type` 保持原样。

### 调度、准入和控制

- `src/bridge/queue-drainer.ts`、`src/bridge/task-supervisor.ts`、`src/bridge/task-store.ts`、`src/bridge/mcp-server.ts`：从候选、并发及状态筛选集合中移除 `reworking` 重复项。
- `src/bridge/task-control.ts`、`src/bridge/extension.ts`、`src/bridge/callback-manager.ts`：将“running 或 reworking”的重复活动态判断收敛到 `running`，但保留检查活动轮次、会话身份和 reviewer/developer 角色的安全条件。
- `src/bridge/autonomous-runner.ts`、`src/bridge/native-session-compactor.ts`：逐处按角色准入语义重审允许状态。不得把 `reworking` 机械替换为 `running` 后，意外允许 reviewer 在 developer 执行期间注册或写入会话。

### UI、MCP 和说明

- `src/ui/task-tree-provider.ts`：合并重复的 `running` / `reworking` 分支；有活动 `rework` 轮时显示 `Reworking (Round N)`，初始轮显示 `Running (Round N)`。没有活动轮次时按任务状态显示通用 Running 文案，不从历史轮次伪造“正在返修”。
- `src/ui/detail-panel.ts`：合并重复颜色、按钮可见性、运行状态提示和恢复入口判断；返修专属提示从活动轮次 `type` 派生。
- `src/bridge/mcp-server.ts`：从 `list_recent_tasks.statusFilter` 的正式枚举和相关工具说明中删除 `reworking`；若保留兼容别名，仅保留输入兼容，不把它描述为当前状态。
- 更新 `DESIGN.md`、`AGENTS.md`、相关流程设计和工具文案中把 `reworking` 当作独立生命周期状态的内容。保留历史事故或迁移说明时，明确它是旧账本值。

### 测试与样例

- 把测试夹具中“开发返修正在执行”的 `thread.status = 'reworking'` 改为 `running`，并确保同一线程的活动轮次有 `type: 'rework'`。
- 保留旧账本兼容用例，输入 JSON 中仍显式包含 `status: 'reworking'`，验证加载后的状态为 `running`、轮次类型和会话 ID 不变，且恢复仍使用同一原生会话。
- 更新任务树文案、活动筛选、暂停/删除、队列候选、Reviewer 交接、原生会话 compaction 准入等相关用例。

## 7. 不应做的清理

- 不删除 `RoundType = 'initial' | 'rework' | 'approval'`：它表达每个轮次的业务类型，状态合并后仍被 UI、审阅和返修流程需要。
- 不删除 `findInFlightRound()` 或轮次并发不变量：它识别正在执行的实际轮次，是状态规范化后判定返修显示和安全恢复的依据。
- 不因 `running` 已覆盖返修而放宽角色锁、bootstrap/session 校验、单任务单活跃轮次、暂停中断留痕或输出成功双条件。
- 不用最近一条历史轮次的 `type` 推导当前任务正处于返修执行中。

## 8. 验收标准

1. 新代码和 MCP 输出中的任务生命周期状态不再产生 `reworking`；`ThreadStatus` 只包含 10 种状态。
2. 新建返修、返修恢复和失败重试均写 `running`，活动轮次仍为 `type: 'rework'`，并沿用既有 `receiverSessionId`（除显式 retarget 规则外）。
3. 老账本 `reworking` 任务加载后表现为 `running`，`round.type`、session、worktree、token usage、interruption 和输出均未丢失。
4. UI 能区分“初始执行中”和“返修中”，但状态筛选、暂停/删除、候选调度和活跃统计只需识别一个开发执行状态。
5. Reviewer 不会因为统一状态而在 developer 轮次执行期间被唤醒或获得会话写租约；现有角色锁与原生会话租约测试继续通过。
6. 成功、失败、暂停、恢复、等待检视、等待合并和合并失败的状态迁移与现有行为一致。
7. 没有为此新增状态字段、配置开关、账本迁移版本或第二套状态映射。

## 9. 实施顺序

1. 先添加旧账本读时规范化，并调整测试夹具及状态派生 UI。
2. 将所有返修执行写入路径统一写 `running`，再收敛调度、控制和会话准入判断。
3. 更新 MCP schema、筛选兼容别名、设计文档和工具说明。
4. 全仓搜索 `reworking`，仅保留旧账本规范化、可选的筛选兼容别名、历史描述和兼容测试中的字面值；确认没有新的状态写入点后完成实施。

本文件是设计，不代表上述代码变更已经实施。
