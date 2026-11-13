# 文件夹级 Bridge Workspace

> 状态：实现进行中；尚未完成验收。此文记录当前实现与未完成边界。

## 身份与存储

Bridge 工作空间以本地绝对目录为根；`physicalDirectory` 验证目录并解析符号链接、junction 与目录大小写。注册表在 Bridge 存储目录下创建持久环境 ID 和 `workspaces/<id>.json`，ID 由环境 ID 和物理根目录摘要计算。完整临时文件通过独占硬链接发布，避免并发注册读到半成品或生成不同环境 ID。注册允许非 Git 目录；代码任务派发仍要求有已提交 HEAD 的 Git 仓库，创建独立任务分支与 worktree，并沿用原有目标固定、验证和 CAS 合并流程。

从 Git linked worktree（包括别名或其中的子目录）派发时，以 `git worktree list --porcelain -z` 找到并验证主 checkout，并用 Git common directory 确认仓库身份；该任务归入主 checkout 的空间，独立 clone 不合并。归组只决定空间身份：目标分支仍在原始派发 checkout 上按显式 `targetBranch` 或该 checkout 的分支固定，实际执行仍在新建的任务 worktree 内。主 checkout 自身的子目录仍可作为独立空间。

新任务保存必需的 `workspace` 绑定：`id`、`environmentId`、`rootPath`、`canonicalRootKey`、`nameAtDispatch`、`source`、可选 `origin`、`executionPath`。隔离任务的执行目录必须等于 `worktree.path`，所有任务执行与会话调用使用 `getTaskExecutionPath`；合法 worktree 移动时两者一起更新。`TaskStore.upsertTask` 拒绝缺失绑定、身份漂移及未经验证的执行目录变更。不会将旧字段迁移或双读写到新绑定。

## 窗口及查询

每个扩展宿主拥有随机实例 ID 和 generation、心跳文件和独立默认值。选择或清除默认空间的命令只影响本窗口；任务树过滤与派发默认独立。MCP 连接的调用窗口来源与任务目标分开解析：即使首次调用显式选择另一空间，也先按连接 roots 解析调用窗口，目标目录不得覆盖该来源。支持 MCP roots 的客户端可在初始化后提供根目录，只有一个根且只匹配一个存活窗口时才关联；来源存在歧义时仍可显式派发，但不记录任何窗口 origin，省略目标的调用则返回歧义。未能确定调用窗口的 MCP 派发不从目标目录猜测窗口。派发持久化并确认成功后，宿主只从该实例激活后创建、origin 匹配的任务中采纳首个默认，已有默认不被覆盖。不得根据调用者 PID 或 MCP clientInfo 推断窗口。

`dispatch_task` 使用 `workspaceRoot` 或 `workspaceId`，省略时仅使用已绑定连接上下文；未解析到空间时报 `WORKSPACE_REQUIRED`。`workspacePath` 不再接受。`list_workspaces` 列举注册表及账本派生数量；`list_recent_tasks` 可按 workspace ID 或已绑定窗口当前默认过滤，先过滤再限制数量。任务树按空间分组，可选择全部、当前或指定空间；Agent 视图显示当前窗口默认根目录。

工作空间归属只用于识别、查询与展示，不是执行锁、调度器、沙箱或授权。`samePhysicalFile` 以文件系统设备和文件标识比较现有文件（包括硬链接），但现有调度器没有文件范围冲突机制；它尚未接入任何冲突判定，也不按文件范围阻挡隔离 worktree 任务。

## 上线步骤

停止旧版派发及所有宿主，处理所有旧版未终态任务，包括 queued、running、reworking、paused、waiting_review、waiting_merge 和 bootstrap pending；备份需要保留的结果及旧账本，并在新版本启用前将全部旧任务记录移出其活动账本。新模型不迁移或兼容旧任务，不应以调用者实时账本作为测试数据。确保 Node 20+、依赖和 Git 可用，针对隔离测试主目录运行编译及完整测试。部署后重新选择窗口默认空间；旧任务数据不应作为新模型的输入。

## 尚待完成

窗口默认首次关联和上下文生命周期仍需覆盖真实多窗口及不支持 MCP roots 的客户端；路径身份在 Windows 大小写敏感目录、文件范围冲突判定、MCP current 查询和 UI 分组仍需补足端到端验收。未完成上述工作前不得视为可上线。
