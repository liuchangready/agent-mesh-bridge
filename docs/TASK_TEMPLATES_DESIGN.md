# 任务模板设计

> 状态：设计草案，尚未实现。

## 目标

让调用者用一个简短需求创建结构更稳定的任务。任务模板按工作类型提供成对的角色要求：

- **Worker 模板**：定义执行者要完成什么、应遵循什么工作方式，以及要交付哪些证据。
- **Reviewer 模板**：定义检视者如何独立验收、需要检查什么，以及何时批准或要求返修。

首批提供两种类型：**编程**和**美术**，以用户可编辑的 Markdown 文件提供。派发时由发起方 Agent 阅读文件中的模板摘要，根据用户需求自动选择；用户不需要在每次派发时手动指定模板。模板是任务指令，不是新的 Agent、模型、权限模式或执行器。

## 设计原则

1. 一份模板包含 worker 与 reviewer 两部分，必须成对选择和版本化。
2. 用户的本次任务需求、验收条件和模板要求合并成一个任务快照；创建后修改模板不影响已有任务。
3. 模板只增加任务级要求，不能覆盖桥接的系统约束、用户授权、Git/worktree、安全规则或 MCP 工具权限。
4. Worker 与 reviewer 看到各自角色的要求；Reviewer 必须独立检查实际工作结果，不能只复述 worker 自报结论。
5. 不传模板时保留现有派发行为。模板功能不得改变已有任务或旧账本兼容路径。

## 内置模板

| 模板 | Worker 关注点 | Reviewer 关注点 |
| --- | --- | --- |
| 编程 | 理解现有结构与约定；实现用户需求；避免无关改动；运行适用的验证；报告改动文件、验证结果及未解决问题。 | 检查实现是否满足需求；检查边界情况、回归风险和安全问题；核对测试或构建证据；问题按严重程度列出；只有没有阻断问题时才批准，否则给出可执行的返修要求。 |
| 美术 | 按需求制作或修改项目内的美术资产；遵循项目已有风格、尺寸、格式、命名与导入流程；保持资产可复用并避免改动无关资源；报告资产路径、规格和验证方式。 | 检查资产是否符合用途、风格、尺寸、格式、透明度/色彩空间等明确要求；检查命名、导入与引用是否正确；确认改动没有破坏既有资源；按验收条件提出具体问题或批准。 |

目录摘要应简短且可区分，例如：

- `programming`：实现或修改代码、修复缺陷、调整构建/测试逻辑。
- `art`：制作或修改视觉、美术或游戏资源文件，并检查其规格及项目集成。

模板不会凭空假定具体引擎、艺术风格或测试工具。没有在任务需求或项目资料中找到的约束，Worker 应标明未知，不应自行编造；需要用户选择的视觉方向应通过任务 brief 澄清，不由 Reviewer 擅自决定。

## 用户流程

1. 发起方 Agent 在创建任务前读取当前 workspace 的模板目录摘要。摘要说明模板适用范围及主要检查维度，不包含完整角色指令。
2. 发起方 Agent 根据用户需求选择最匹配的模板，并在 `dispatch_task` 请求中传入 `templateId`。用户填写的任务需求和验收条件仍是任务的具体依据。
3. 若没有模板适用，或需求不足以判断属于哪种工作类型，发起方传 `templateId` 缺省值，即按无模板派发；不得为了套模板而猜测或改写任务类型。
4. 桥接解析模板，生成 Worker 指令和 Reviewer 指令，并把已解析内容及模板身份快照写入任务账本。
5. Worker 在既有隔离 worktree 中执行。轮次完成后，专用 Reviewer 会话按 Reviewer 指令检查结果。
6. Reviewer 按现有控制面批准或调用返修。返修时沿用任务创建时的模板快照；Reviewer 的本轮返修意见优先约束当前修订，但不能改写原始模板记录。

MCP 提供只读 `list_task_templates` 工具，按调用绑定的 workspace 返回有效模板文件的 `id`、名称、摘要和版本。若当前 MCP 连接未绑定 workspace，工具接受显式 `workspaceId`；不得从任务执行 worktree 或任意当前目录猜测。普通发起方应在每次新任务派发前读取当前 workspace 的目录并自行判断匹配项；`dispatch_task` 的工具说明也要求先读摘要再选择。`dispatch_task.templateId` 接收选择结果，并从同一个已解析 workspace 读取完整内容；省略表示不使用模板。这个目录读取不是任务进度轮询。接收方进程隐藏并拒绝该工具，避免通过模板功能绕过接收方禁止转发的规则。

在 Bridge 工作空间视图的 workspace 项右键菜单中提供三个模板动作：

- **读取模板**：读取并校验该 workspace 的模板文件，以列表展示名称、摘要和版本；无效文件也显示文件名与错误原因。此动作不编辑文件。
- **新建模板**：询问模板 ID、名称和摘要，校验 ID 唯一后在 `.agent_bridge/task-templates/` 创建包含 Worker/Reviewer 两个章节的 Markdown 骨架，并立即在 VS Code 编辑器中打开。
- **修改模板**：列出该 workspace 已有模板及其摘要，选择后在 VS Code 编辑器中打开源文件。

三个动作始终以右键菜单对应的 workspace 为目标，不依赖当前活动编辑器或窗口默认 workspace。模板目录不存在时先创建目录和编程、美术示例（仅创建缺少的文件，不覆盖已有文件）。保存后下一次读取或派发读取即采用新内容，不要求重启扩展。派发时仍由发起方 Agent 自动选择模板；用户不在右键菜单或派发 UI 中替任务选择模板。

## 数据与接口

建议类型：

```ts
type TaskTemplateId = string;

interface TaskTemplate {
  id: TaskTemplateId;
  version: number;
  label: string;
  summary: string;
  workerInstructions: string;
  reviewerInstructions: string;
  filePath: string;
}

interface TaskTemplateSnapshot {
  id: string;
  version: number;
  label: string;
  summary: string;
  workerInstructions: string;
  reviewerInstructions: string;
  sourceFileName: string;
  selectedAt: string;
}
```

`.agent_bridge/` 是 workspace 范围的 Bridge 元数据目录，可存放该 workspace 专属的信息和设置。本功能在其中使用 `task-templates/`；既有的全局 workspace 注册表和 workspace ID 仍由当前注册机制管理，不复制或迁移。模板的具体位置为 `<workspaceRoot>/.agent_bridge/task-templates/`，每个 `.md` 文件是一份完整模板。身份以任务已绑定的规范 workspace 根目录为准；Linked worktree 派发仍使用该 workspace 根目录里的模板，而不是新建的任务 worktree 内容。文件格式由可读的元数据区和两个固定标题组成：

```md
---
id: programming
version: 1
label: 编程
summary: 实现或修改代码、修复缺陷、调整构建或测试逻辑。
---

## Worker instructions

执行者要求……

## Reviewer instructions

检视者要求……
```

`id` 在模板目录内唯一且稳定；文件名不作为身份。允许用户编辑摘要及正文，两个角色章节都必需。格式错误时该模板标为不可用，并在打开命令与日志中说明文件名和原因，不能静默使用半份模板。目录列表同时返回可用模板和校验警告，避免损坏文件无声消失。删除文件即从目录中移除；已有任务依靠账本快照继续运行。

新任务可选持久化 `thread.taskTemplate`。没有模板的旧任务该字段缺省。派发时先按现有 workspace 解析并固定模板快照，再创建任务 worktree；后续 worker/reviewer 只读账本快照，不依赖 worktree 中是否包含 `.agent_bridge/`。未知 `templateId` 应在任务创建前返回明确校验错误，不得静默退回“无模板”。

首次使用模板功能时，在当前 workspace 中创建 `.agent_bridge/task-templates/` 及编程、美术示例文件。只在目标文件不存在时创建，不覆盖已有文件；插件升级不得重置或覆盖用户编辑过的文件。用户可以复制文件并为新模板分配唯一 `id`。解析时校验文件大小、字段、标题和 ID 冲突；模板目录及文件必须解析到当前 workspace 的 `.agent_bridge/task-templates/` 内，拒绝符号链接逃逸。不要自动添加 `.gitignore` 规则；由项目决定是否将共享模板纳入版本控制。

任务账本保存解析后的模板快照，而不只保存模板 ID。快照仅保存源文件名，不保存机器绝对路径。用户修改或删除源文件后，已有任务及其返修仍使用创建时的两份角色要求。

## Prompt 组装

### Worker

现有任务 brief 继续包含标题、用户需求、验收条件和桥接生成的上下文。选择模板时追加清晰标记的 `Worker instructions` 区块。模板文字作为普通指令文本传递，不执行变量插值、命令或文件路径读取。

每次返修仍保留已有轮次和返修意见，并追加原 Worker 模板快照；模板不会因新的 reviewer 指令或模型输出而重写。

### Reviewer

专用 reviewer bootstrap 和后续 reviewer 唤醒在其 review prompt 中追加同一任务的 `Reviewer instructions` 快照。现有审查证据、worker 报告、assessment、工作树与 Git 状态仍按当前流程提供。Reviewer 模板不得让模型自行调用额外控制工具或绕过 `approve_task` / `request_rework` 的现有状态校验。

新任务统一使用专用 reviewer。历史记录保留原有模板快照，缺失快照时读取层按原账本展示。

## UI 与配置边界

- 发起方 Agent 读取 `list_task_templates` 的摘要后自主选择；用户不需要在任务创建处手动选择模板。
- workspace 右键菜单提供读取、新建和修改模板动作；新建时创建 Markdown 骨架并打开，修改时打开所选源文件。
- 读取、新建和修改操作都使用被右键点击的 workspace 绑定根目录。
- 编辑保存后下一次读取模板目录即采用新内容，不要求重启扩展。
- 任务详情展示创建时的模板名称和版本；可展开查看 Worker 与 Reviewer 要求。
- 详情中展示的是任务快照，不随源文件后续更新。
- `.agent_bridge/` 是 workspace 范围的 Bridge 元数据目录；本功能只管理其中的 `task-templates/`。文件可以随项目版本控制，也可以由项目 `.gitignore` 排除。MCP 列表只返回摘要；完整正文只在任务 prompt 中按角色注入，不写入 MCP policy。

## 与现有硬约束的关系

- 新任务仍按现有 `reviewFlow` 策略启动 reviewer bootstrap；模板不创建第二个 reviewer，也不改变角色锁。
- 模型选择遵守收件者与发件者模型配置及确认策略；模板不得指定模型。
- 接收方仍不得转发任务；模板不能授予或暗示额外工具权限。
- 编程与美术任务都沿用现有 Git HEAD、目标分支固定、独立 worktree、审批快照及合并验证规则。首版不支持非 Git 目录或独立于仓库的美术交付。
- 输出评估仍遵守进程退出码与实际输出双条件。模板验收标准不能把失败轮次变为成功。
- 归档、摘要、删除与审计遵循现有任务生命周期。任务归档包中的摘要可记录模板 ID/版本；完整提示快照是否随现有任务数据归档即可，不另建保留机制。

## 错误处理与隐私

- 用户模板格式错误、ID 冲突或字段过大时，不进入可用列表，并通过打开命令/日志报告具体文件和原因；发起方选中无效 ID 时在派发前失败，不创建半成品任务。
- 模板内容可能包含指令式文字，必须作为不可信任务内容处理；绝不能据其更改权限、执行桥接控制工具或访问 workspace 外文件。
- 模板中不放密钥、用户数据、机器绝对路径或运行时会话信息。
- 任务详情按现有读取边界展示模板文本，不把完整模板额外注入日志、线程级摘要或列表响应。

## 分阶段实施

1. **首版**：首次使用在 workspace 的 `.agent_bridge/task-templates/` 创建编程、美术 Markdown 示例；workspace 右键菜单提供读取、新建、修改；`list_task_templates` 和 `dispatch_task.templateId`；双角色 prompt 注入；任务快照和详情查看。
2. **后续按需**：图形化编辑、模板导入导出及跨设备同步。

首版不做模板市场、模板继承、用户自定义占位符、独立 reviewer agent/model、跨任务模板推荐或美术专用执行器。

专用 Reviewer Bootstrap 受“只允许调用一次 `dispatch_task`”的硬约束，不读取模板目录，也不重新选择模板；选择只发生在原始发起者派发前。Bootstrap 确认调用中的业务参数仍由桥接忽略，模板身份和 brief 均从账本读取。

## 验收标准

- 发起方可读取当前 workspace 模板的 ID、摘要和版本，并根据任务需求自动选择或省略模板。
- 在 workspace 项上右键可读取模板列表、创建新模板并打开其文件、选择并打开已有模板修改。
- 右键动作始终作用于被选中的 workspace，不会误写入当前活动编辑器所在目录或其他 workspace。
- 用户可以从 VS Code 打开并编辑 Markdown 模板；重新初始化和插件升级不会覆盖用户修改。
- 新增或删除模板会在下一次读取时生效，已创建任务仍使用原始快照。
- 两个内置模板都同时提供 worker 与 reviewer 要求。
- 选择模板的新任务将模板 ID、版本及两份原始要求快照持久化。
- Worker 只收到 worker 要求；Reviewer 的 bootstrap、复检与返修 review 收到 reviewer 要求。
- 模板修改不会改变已创建任务或其后续返修轮次所使用的快照。
- 无模板派发与既有行为一致；未知模板在任务创建前被拒绝。
- 模板不能改变模型裁决、角色切换、权限、任务状态、Git 交付目标或成功评估。
- `reviewFlow=legacy` 的历史任务与没有模板字段的旧账本继续按原路径工作。
