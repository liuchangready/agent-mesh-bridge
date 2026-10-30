# 🌉 Agent Mesh Bridge - 架构与开发指南 (AGENTS.md)

本指南为在 `D:\agent_bridge` 项目中协作开发的各 AI Agent（Google Antigravity、OpenAI Codex、Anthropic Claude Code、DeepSeek Harness）提供系统设计规范、编码准则与质量基线。

---

## 1. 项目定位与核心特性

- **项目名称**：`agent-mesh-bridge` (VSCode 插件)
- **核心目标**：在 VSCode 宿主内无缝打通 **Google Antigravity (`agy`)**、**OpenAI Codex (`codex`)**、**Anthropic Claude Code (`claude`)**、**DeepSeek Harness (`dsh`)**，构建对等多 Agent 协作网格。
- **二十一大核心设计铁律**：
  1. **默认选路原则**：无显式指定 target 时，默认自动选路至 VSCode 当前使用的前台 Agent。
  2. **全向对等互派**：四端 Agent 均可作为发件人向其他 Agent 派发任务；但**接收方不得二次转发**（见第 9 条）。
  3. **接收方原生留痕**：执行必须携带原生会话参数（`--session-id` / `--conversation` / `--session-id <dsh>`），确保在接收方原生历史可查。
  4. **异步非阻塞回调**：发起方下发任务后立即结束当前 Turn（不长轮询等待），任务完成后插件主动唤醒发起方原会话继续。
  5. **同会话多轮返修**：A 对 B 结果不满意时，调用 `request_rework` 在 B 的**同一个原生会话中继续修改**，多轮迭代直至 A 认可。
  6. **1000 条任务账本**：持久化保留最新 1,000 条任务，展示 `A -> B 标题 摘要`，支持查看完整多轮时间轴与代码 Diff。
  7. **全自动无人值守 (`skip-danger`)**：严格配置免交互与权限旁路参数，关闭标准输入，杜绝后台卡死。
  8. **全局串行执行（硬不变量）**：任意时刻整个网格**只允许一个任务轮次真实执行**。任务按 FIFO 排队；即使两个任务的 `expectedFiles` 完全不重叠，也不得并发。跨进程（扩展宿主 + 各自独立的 MCP server）通过 `~/.agent-mesh-bridge/execution.lock` 的原子租约统一裁决。
  9. **接收方禁止转发（当前策略）**：被派发的 Agent（receiver）只执行当前任务并回传结果，**不得**调用 `dispatch_task` / `invoke_agent` 把工作再转给其他 Agent，也不得驱动自己任务的 review 闭环。接收方进程由 `MESH_TASK_RECEIVER=1` 标记，MCP server 会隐藏并拒绝转发类工具。
  10. **权限旁路不可失效**：拆分/派发出去的任务必须真正无人值守。Codex 的 `--ask-for-approval` 是**全局参数，必须放在 `exec` 之前**；DSH 必须注入 `DSH_PERMISSION_MODE=danger-full-access`（否则 headless 默认 `workspace-write` + `approval: ask`，任务会卡在无人应答的权限询问上）。
  11. **可暂停、可删除、可追溯**：运行中的任务必须可以暂停（终止 Agent 进程并保留当前轮次以便恢复）、可以删除（先终止进程，再移除账本条目）；暂停、停止、看门狗、超时等非正常结束必须在 `thread.interruption` / `round.interruption` 中记录 `{ kind, reason, at, by }`，且保留 `interruptions[]` 历史。
  12. **单任务单一角色（硬不变量）**：同一 `threadId` 在任一时刻只能有一个活跃角色——`reviewer`（下发/检视）或 `developer`（开发/返修）。角色链严格为 reviewer → developer → reviewer → …；不得同时存在两个检视进程，也不得在 reviewer 未交接时启动 developer。
      - 角色锁存于 `~/.agent-mesh-bridge/roles/<threadId>.json`，带心跳与僵尸回收。
      - reviewer 在 `request_rework` / `approve_task` 成功时视为完成交接并释放角色；developer 轮次结束后、唤醒 reviewer 前释放 developer 角色。
      - 中断后**不得新建任务或另起会话**：必须沿用同一 `threadId`、同一 `receiverSessionId` / `callerSessionId`，并基于账本中的历轮指令、开发报告、返修意见（中间文档链）继续。
  13. **任务级 FIFO（端到端单任务）**：排队单位是**任务**，不是单个轮次。一个任务从进入队列到 `completed` 之前，始终占用唯一的全局任务槽；即使它已经进入 `waiting_review` 或 `paused`，其他任务也必须继续等待。
      - 占用队列的状态：`queued`、`running`、`reworking`、`waiting_review`、`paused`。
      - `completed`、`failed` 视为终止态；`failed` 不长期阻塞历史任务，重试后会重新占用队列。
      - 队头由 `findQueueHead()` 按 `createdAt` 选出：队头在 `waiting_review` / `paused` 时，`TaskSupervisor` / `QueueDrainer` 不得启动任何其他任务。
      - 新任务派发时，只要存在任何未完成任务，直接返回 `QUEUED`，消息必须说明在等哪个 `threadId` 的哪个阶段。
      - 任务被 approve / delete 后，队列自动推进到下一个任务；无需手动唤醒。
      - **检视挂起（blocked）不是新状态**：检视方用 `request_rework(..., blocked=true)` 主动挂起时，任务**保持 `waiting_review`**（不新增轮次、不释放队列槽），只把依赖与理由写进 `thread.reviewBlocked`；任务树显示 `Parked Review · 已挂起` 与等待的依赖，详情面板给出依赖、理由和「重新检视（解除挂起）」入口（`wakeUpCaller` 会先清掉 `reviewBlocked` 再唤醒）。`reviewBlocked` **无 `waitingForThreadId`**、或依赖线程**已从账本删除**属于不可恢复挂起：看门狗必须把它转成可见的终态停滞（`reviewWake.kind='timeout'` + `gaveUp`，不自动重试）并清掉死挂起，绝不静默跳过。

  14. **MR 收口由调用者完成**：任务（或同一批任务）验收通过后，由**调用者/检视方**统一合成一个 MR 并同步远端；开发者(receiver)不负责提交/推送。
      - 调用者汇总该任务及同批尚未推送的改动，只创建一个 MR，使用仓库既有远端与 `gh` / `glab` / 等价 CLI 推送。
      - 推送后必须调用 `record_merge_request(threadId, url, branch, commit, remote, status, summary)` 把 MR 信息写回账本。
      - `record_merge_request` 是调用者控制面工具；receiver 进程不得调用。
      - 每个子任务不单独推送、不单独建 MR；MR 是一个批次级收口动作。
      - 任务已 `completed` 仍允许记录 MR；MR 记录不影响任务级队列推进，但必须在调用者本轮结束前完成。

  15. **响应成功必须双条件（硬不变量）**：Agent 响应是否成功不能只看退出码，必须同时满足：
      - **错误码**：进程退出码为 `0`；非 0 记录 `NONZERO_EXIT`，暂停/超时/看门狗记录对应中断码。
      - **流质量**：stdout 中必须提取到**实际最终输出**，且输出流连续（有 chunk，且最长间隔不超过 `silenceTimeoutMs`）；没有实际输出记 `EMPTY_OUTPUT`。
      - **间隔必须扣除主机挂起**：间隔用**单调时钟**测量（免疫 NTP/改表跳变），并由 `HostSuspensionGuard` 心跳识别 `sleep`/`hibernate`/Modern Standby/宿主被冻结的窗口，从 `maxGapMs` 中扣除；被扣除的原始墙钟间隔留在 `rawMaxGapMs`，扣除量留在 `excludedSuspensionMs`、窗口数留在 `suspensionCount`。静默看门狗与总超时同样按「扣除挂起后的活跃时间」判定，机器睡多久都不算 agent 的静默、也不消耗任务预算。
      - **长间隔默认降级为 warning**：`exitCode === 0` 且提取到实际输出时，扣除挂起后仍超阈值的间隔记 `STREAM_GAP_EXCEEDED` 警告（`assessment.warnings[]`，`success` 仍为 `true`，`stream.continuous = false`、`stream.gapExceeded = true`），不得据此外抛失败、丢弃已完成的工作或重跑整轮；`STREAM_STALLED` 只在显式 `streamGapPolicy: 'fail'` 时使用。
      - `RunResult.assessment` 必须持久化到 `round.assessment`，字段包括 `success`、`errorCode`、`exitCode`、`warnings[]`、`stream.{chunkCount, stdoutBytes, stderrBytes, firstOutputAtMs, lastOutputAtMs, maxGapMs, rawMaxGapMs, excludedSuspensionMs, suspensionCount, gapThresholdMs, hasActualOutput, continuous, gapExceeded}`。
      - 任何 `exitCode === 0` 但不能提取实际输出（例如只有 JSON status/metadata）的轮次必须判定为 `failed`，不得进入 `waiting_review`，更不得唤醒 reviewer。

  17. **文件归属互斥（scope gate，硬不变量）**：全局单飞只保证「不同时改」，**不保证「不重复改」**——同一文件被前后两个任务先后覆盖，锁管不到。因此：
      - `dispatch_task` 必须声明 `expectedFiles`（具体代码文件）；确实不触碰工作区文件的，显式传 `allowNoScope: true`。
      - 每个轮次用 git probe 测量**实际改动了哪些文件**（`git status --porcelain -z` 前后快照 + HEAD 变化），写入 `round.touchedFiles`；即使调用方没申报、或 agent 改了申报范围外的文件，也能被记录并保护后续任务。
      - 任何轮次启动前必须执行 `findScopeConflicts()`：候选任务的 `expectedFiles` 与其他**未收口任务**的 `expectedFiles ∪ touchedFiles` 有交集时**不得启动**，置 `queued` 并把冲突方与文件写入 `thread.scopeBlockedBy`。
      - 「未收口」= 未完成（`queued`/`running`/`reworking`/`waiting_review`/`paused`），或已完成但**没有** `mergeRequest` 且在 `SCOPE_BLOCK_WINDOW_MS`（24h）内；`failed` 不再阻塞。
      - 放行只能通过 `allow_scope_overlap(threadId)`（或派发时 `allowScopeOverlap: true`），任务树显示 `⛔scope`、详情面板显示冲突文件。

  18. **模型默认优先、非默认必须用户点击同意（硬不变量）**：接收方模型的选择权属于用户，不属于 caller。
      - 不传 `model` 一律使用该 Agent 的**配置默认模型**（`agentMeshBridge.models.*` → `~/.agent-mesh-bridge/model-config.json`），这是唯一无需确认的路径。
      - caller **只有在用户明确点名型号时**才能传 `model`，且必须同时传 `modelRequestedByUser: true`；否则桥接**忽略**该型号、改用配置默认，并在 `DispatchResult.modelNotice` 与 `thread.modelNotice` 中说明。禁止 agent 自行「挑一个更强的模型」。
      - `modelRequestedByUser: true` 且型号 ≠ 配置默认时，桥接在 `~/.agent-mesh-bridge/model-approvals/` 发布请求，VS Code 扩展宿主（`ModelApprovalService`）弹出模态确认框：同意 → 按请求型号执行；改用默认 → 回落默认并留痕；取消/超时（默认 180s）→ **派发失败**（`MODEL_APPROVAL_DENIED` / `MODEL_APPROVAL_TIMEOUT`），不得静默切换、不得排队绕过。
      - 确认通道不可用（无扩展宿主、家目录不可写）时回落配置默认并提示 `modelNotice`，同样不得绕开确认直接使用非默认型号。
      - 裁决必须落账：`thread.modelApproval{id,status,requestedModel,defaultModel,at}`，详情面板「模型切换裁决」展示；审计追加写入 `~/.agent-mesh-bridge/model-approvals.log`。
      - `list_agent_models` 必须把 `configuredModel` 放在 payload **最前**，并把该型号在 `models` 数组内置顶并打 `configured default` 标签（`annotateConfiguredModel`），避免 caller 按 provider 顺序误选第一个条目。
      - 策略开关 `agentMeshBridge.requireModelApproval`（默认 true）随 `model-config.json` 镜像给各 MCP 进程；置 false 只表示不弹确认框，仍**不允许**未声明的型号切换。
      - **执行者改派（retarget）**：已派发任务可以换 Agent 和/或换模型，但必须走显式入口——`retarget_task`（专用工具）或 `request_rework` / `retry_task` / `resume_task` 的 `targetAgent` / `model` 参数；每次改派写入 `thread.retargets[]`（from/to Agent、from/to Model、sessionReset、previousReceiverSessionId、reason、at）。禁止静默漂移。
      - 改派前置条件：**不得**存在在飞轮次（先 `pause_task`），`completed` 任务不得改派；`queued` / `paused` / `waiting_review` / `failed` 可改派且**状态保持不变**（改派只换执行者，不偷偷推进队列或状态）。模型解析沿用同一套规则：省略 `model` 时同 Agent 保持原模型、换 Agent 用新 Agent 的配置默认。
      - **换 Agent 必须重置原生会话**：`receiverSessionId` 换成新的未捕获 id、`receiverSessionCaptured=false`，旧 id 只进 `retargets[].previousReceiverSessionId`；仅换模型（同 Agent）时保留会话。`shouldResumeNativeSession()` 必须优先尊重「最近一次改派是否重置了会话」，不得因 round.type 是 rework 就强行续聊。
      - VS Code 任务右键命令 `agent-mesh-bridge.retargetTask`（切换执行者）里用户已亲手选定 Agent/模型，该点击即视为同意，可直接以 approved 应答确认闸门。

  19. **派发即结束 Turn、禁止轮询（硬不变量）**：`dispatch_task` / `invoke_*` 返回后，发起方必须立刻结束当前 Turn，由桥接在轮次结束时唤醒其原生会话；**不得**用 `get_thread_details` / `get_recent_logs` / `list_recent_tasks` 轮询进度。
      - 派发回包必须携带机器可读的 `nextAction: 'END_TURN_AND_WAIT_FOR_CALLBACK'` 与 `stopInstruction`，且 `message` 第一句就是停止指令；`QUEUED` 必须在 `stopInstruction` 里说明「排队是正常的，会自己开始」。
      - MCP 进程用 `CallerPollGuard` 只守卫**本进程自己派发**的线程：同一线程同一进度（`progressSignature`，按 status / currentRound / rounds.length / 末轮 round+status+durationMs / 执行者 计算）首次读取给完整载荷，重复读取只返回 `repeatPoll` 短载荷（status、currentRound、lastRound、stopInstruction），**不得**抛错（用户主动查进度是合法操作）。
      - 任何真实进度变化必须重置守卫，保证回调后或复检时能拿到完整数据；`approve_task` / `delete_task` 必须 `forget` 该线程。
      - 只守卫本进程派发的任务；检视方读取他人任务、`list_recent_tasks` 概览不受影响。

  20. **上下文滞后压缩（账本卫生，不碰原生会话）（硬不变量）**：多轮返修的账本体积与 reviewer 读取量必须受控，但**不得**靠改写 Agent 原生会话实现。
      - 只压缩**严格早于当前轮**且已进入终态（`completed`/`failed`）的轮次；最新轮永远保留 100% 原始输出。`failed` 轮只压 `output`，`error`/`status`/`interruption` 保持原样；合成 `approval` 轮不参与。
      - 只有 `assessment.success === true` 才允许触发；`NONZERO_EXIT` / `EMPTY_OUTPUT` / `STREAM_STALLED` / 暂停 / 超时 / 中断一律不压。
      - 顺序不可颠倒：先写全文归档 `logs/<threadId>_round_N.output.md`，再改账本；归档失败即放弃压缩（宁可不压，不可丢数据）。卡片若不小于原文（小轮次 + 头部开销）则保留原样并返回 `no_size_benefit`，既不增大账本也不留下无用归档。
      - 压缩必须 **best-effort**：`compactPrecedingRound()` 内部吞掉所有异常并返回 `CompactionOutcome`，调用点统一 `compactRoundBestEffort()`；绝不允许压缩失败阻断 reviewer 唤醒、角色交接或队列推进。
      - Agent 协约（`<context_compaction>`）只在**执行期**追加到 `runner.execute` 的 prompt（且仅 `isRework && round >= 2`），**不得**写回 `round.prompt`；胶囊必须从 `extractAnswerOnly()` 的最终文本解析，不能对 JSONL 原始流正则。无胶囊时用确定性头尾摘录兜底。
      - 胶囊是模型产出，只作展示/归档，**不得**驱动控制流或再次拼进 prompt；`touchedFiles` 只复用 git probe 的 `round.touchedFiles`。
      - v1 **禁止**调用 `/compact` 或拉起任何额外压缩进程（会占全局串行槽、可能改工作区，且 headless 支持未证实）。如需启用，必须能力探测失败即跳过、只读权限、硬超时、默认关闭。
      - 读取层必须收敛：`get_thread_details` 默认返回有界视图（最新轮全量、已压缩轮给卡片、其他超长历史轮截断并附 `recallHint`），不默认下发千行日志；`includeRawLogs: true` 才给全量。`get_round_raw_log` 必须分页（默认 20KB，上限 200KB），`source` 区分 `output` 归档与 `execution` stdio 日志。
      - `TaskStore.upsertTask()` 合并时必须保护已压缩轮，旧的未压缩副本不得“复活”原文。
      - 开关 `agentMeshBridge.contextCompaction` 经 `bridge-policy.json` 镜像给 MCP 进程；只有显式 false 才关闭。

  21. **Token 计量必须归一化、只落轮级、不得造假（硬不变量）**：四端 CLI 的 `input` 语义不同，直接相加会得到错一个数量级的数字。
      - **归一化口径唯一**：`promptTokens = uncachedInputTokens + cacheReadTokens + cacheWriteTokens`，`totalTokens = promptTokens + outputTokens`；`reasoningTokens` 是 `outputTokens` 的**子集**，只展示、绝不重复计入。CLI 自报的 total 只能放进 `nativeTotalTokens` 做交叉校验，**不得**参与聚合。
      - **各端只认一个来源**：Codex 读 `turn.completed.usage`（`cached_input_tokens`/`cache_write_input_tokens` 是 `input_tokens` 子集）；Claude 读结果包 `usage` + `total_cost_usd`（cache 另计）；Antigravity 只读 `step_update.usage`（snake_case，`cache_read_tokens` 是 `input_tokens` 子集），**必须丢弃** `result` 的会话累计值；DSH 只读 `{"type":"status","phase":"step_end"}` 的 camelCase usage（`inputTokens` 不含 cache，`final` 不带 usage）。多源相加一律禁止。
      - **只估输出**：输入是整段历史 + 工具结果，禁止用字符数估算；无 usage 时只估输出并标 `estimated/partial`，完全没有数据时标 `unavailable/available=false`，UI 显示「未知」而不是 0。
      - **累加链**：`execute()` 内所有 attempt 累加（重试也是真实开销）；`retry_task` / `resume_task` 复用同一轮号，必须用执行器传入的 `previousTokenUsage` 做种子后累加，不得覆盖。`buildInterruptedResult()` 必须透传已采集量。
      - **只持久化 `round.tokenUsage` 与 `round.executorAgent/executorModel`**；线程级汇总一律用 `aggregateThreadTokenUsage(rounds)` 读时派生，**禁止**落盘 `thread.tokenUsage` / `byRound` / `summary.totalTokens`（旧副本回写会让聚合回退）。
      - **耐久性**：压缩只许改写 `round.output`；`TaskStore.upsertTask()` 合并时必须保留 `tokenUsage`/`executorAgent`/`executorModel`（旧副本不得抹掉），`updateRound()` 追加分支的白名单必须显式包含这些字段。
      - **成本只报原生**：只有 CLI 自报的 `costUsd`（Claude 的 `total_cost_usd` 属 API 等价成本）可展示；价格表估算、代理/未知模型一律不显示成本，只报 token。

详细架构设计见 [DESIGN.md](DESIGN.md)。

---

## 2. 技术栈与运行环境

- **运行宿主**：Visual Studio Code (`^1.90.0`)
- **语言标准**：TypeScript 5.4+ (Node.js 20+)
- **通信协议**：Model Context Protocol (MCP) JSON-RPC 2.0 over Stdio
- **可执行文件依赖探测（跨平台，平台差异统一由 `src/bridge/platform.ts` 裁决）**：
  - Antigravity: `~/.gemini/bin/agy`、`~/.local/bin/agy`、`/usr/local/bin/agy`（Windows 为 `agy.exe`）或 `PATH`
  - Claude Code: `~/.npm-global/bin/claude`、`~/.local/bin/claude`、`/usr/local/bin/claude`、`~/.claude/bin/claude`（Windows 额外探测 `%APPDATA%\npm\claude.cmd`）或 `PATH`
  - OpenAI Codex: `~/.vscode{,-server,-insiders}/extensions/openai.chatgpt-*/bin/<平台架构>/codex[.exe]`、`~/.codex/bin/codex`、`/usr/local/bin/codex` 或 `PATH`
  - DeepSeek Harness: `<安装根>/resources/runtime/cli/bin/dsh[.cmd]`（Windows 扫描盘符；POSIX 探测 `/opt`、`/usr/lib` 与 `PATH`）、`DSH_CLI_PATH` / `DSH_LAUNCHER_PATH` 环境变量，或 `agentMeshBridge.deepseekHarnessPath` 配置
  - **可执行性与 shell 包装**：POSIX 只有带可执行位（`X_OK`）的文件才算已安装；只有 Windows 批处理 shim（`.cmd` / `.bat`）才经 `cmd.exe` 启动，其余一律直接 spawn
- **本地存储路径**：`~/.agent-mesh-bridge/delegation_threads.json`（上限 1,000 条）
- **运行时协调文件**：`~/.agent-mesh-bridge/supervisor.json`（常驻 supervisor 心跳）、`~/.agent-mesh-bridge/execution.lock`（全局单飞租约）、`~/.agent-mesh-bridge/roles/<threadId>.json`（单任务 reviewer/developer 角色锁）、`~/.agent-mesh-bridge/cancellations/<threadId>.json`（跨进程暂停/删除信号）、`~/.agent-mesh-bridge/model-approvals/<id>.request.json|.decision.json`（非默认模型切换的跨进程确认握手）与 `~/.agent-mesh-bridge/model-approvals.log`（裁决审计）、`~/.agent-mesh-bridge/bridge-policy.json`（跨进程策略镜像，如 `contextCompaction`）、`~/.agent-mesh-bridge/deleted_tasks.log`（删除墓碑与原因）、`~/.agent-mesh-bridge/logs/`（每轮原始 stdio `<threadId>_round_<n>.log` 与压缩前输出归档 `<threadId>_round_<n>.output.md`）

### DeepSeek Harness (`dsh`) 接入要点

DSH 是 Electron 桌面应用，其 CLI 通过 profile 启动器暴露，与其他三端差异较大：

- **无独立会话预分配**：`dsh headless --session-id <id>` 的语义是**接管已存在的 Session**，对未知 id 直接报错。因此新任务**不得**预先指定 session id，原生 id 由首个 JSON 事件
  `{"type":"session","sessionId":"session-<uuid>"}` 回传，再由 `AsyncDispatcher` 采纳为 `receiverSessionId`，后续返修轮次才使用 `--session-id`。
- **启动方式**：优先直接拉起 `<安装根>/DeepSeek Harness.exe --expose-internals <cli.js>` 并注入 `ELECTRON_RUN_AS_NODE=1`，避免 `cmd.exe` 批处理引号问题；找不到 Electron 载荷时回退到 `cmd.exe /c dsh.cmd`。
- **权限旁路（必须）**：`headless` profile 的 `sandbox-policy.mode` 与 `user-approval.policy` 都由 `DSH_PERMISSION_MODE` 派生，默认 `workspace-write` + `approval: ask`。桥接拉起 DSH 时必须注入 `DSH_PERMISSION_MODE=danger-full-access`（`buildDshHeadlessCommand` 已统一处理），否则派发出去的任务会卡在无人应答的权限确认上。
- **MCP 注册方式不同**：DSH 没有 `mcpServers` JSON 文件。每个 `@deepseek-ai/dsh-mcp-client` 插件实例只连接**一个** MCP server，需以 YAML patch 形式写入
  `~/.dsh/profiles/<profile>/cordis.patch.yml` 的 `insert` 列表（`ConfigSyncService` 以带标记的托管块方式写入，可反复同步且不破坏用户自有 patch）。
- **凭据要求（可配置模型路由）**：`headless` profile 通过 `DEEPSEEK_API_KEY` 解析模型凭据并禁用了本地凭据服务，桌面端登录态**不足以**支撑后台无人值守运行。
  - 插件提供 `agentMeshBridge.deepseekHarness.*` 配置（`provider` / `api` / `baseUrl` / `model` / `apiKeyEnv` / `apiKey`），据此向 **仅 headless** profile 写入 provider patch 块（`desktop`/`web` 保持账号登录不变，不受影响）。
  - **密钥绝不落盘到 profile**：patch 里只声明 `apiKeyEnv`，真实密钥由扩展宿主导出到进程环境（`process.env[apiKeyEnv]`），DSH 子进程继承；`apiKey` 设置为 `machine` scope，不参与 Settings Sync。
  - 鉴权探测 `checkDshAuth()` 同时识别官方 `DEEPSEEK_API_KEY` 与所配置 provider 的 `apiKeyEnv`（经 `DSH_BRIDGE_PROVIDER_ENV` / `DSH_BRIDGE_PROVIDER_ID` 环境变量告知），因此走任意 provider 都能正确显示在线而非误报未登录。
- **原生会话位置**：`~/.dsh/sessions/--<工作区 slug>--/session-<uuid>/session.v4.jsonl.zstd`（slug 规则：非 `[A-Za-z0-9_-]` 的连续字符折叠为单个 `-`，整体再包一层 `--`）。

---

## 3. 模块分层与代码结构

```text
src/
├── extension.ts               # VSCode 扩展生命周期、组合根、命令与视图注册
├── bridge/
│   ├── types.ts               # 核心类型: DelegationThread, DelegationRound, AgentId, Status
│   ├── active-agent-resolver.ts# 默认 Agent 探测与选路算法 (宿主 -> 扩展 -> LRU -> 状态栏)
│   ├── file-scope.ts          # 具体代码文件范围校验与精确同文件冲突检测 (拒绝目录/通配符)
│   ├── execution-gate.ts      # 跨进程全局单飞租约 (execution.lock, 心跳 + 僵尸回收)
│   ├── cancellation-registry.ts# 跨进程暂停/删除信号 (cancellations/<threadId>.json)
│   ├── task-role-lock.ts      # 单任务 reviewer/developer 角色锁 (roles/<threadId>.json)
│   ├── task-control.ts        # 暂停/删除控制面 (中断原因 + 删除墓碑日志 + 原生对话清理)
│   ├── native-session-cleanup.ts# 删除任务时尽力清理接收方原生对话 (Claude/Codex/Antigravity/DSH)
│   ├── queue-drainer.ts       # 无常驻 supervisor 时的 FIFO 队列泵
│   ├── async-dispatcher.ts    # 异步非阻塞任务派发器 (生成 threadId, 全局排队, 立即返回)
│   ├── rework-controller.ts   # 同会话多轮返修控制器 (绑定 receiverSessionId)
│   ├── autonomous-runner.ts   # 跨平台 skip-danger 进程执行器 (关闭 stdin, 注入安全绕过参数)
│   ├── task-supervisor.ts     # 宿主常驻守护进程 (30s 实时流式同步, FIFO 冲突任务排队调度)
│   ├── callback-manager.ts    # 回调唤醒引擎 (Codex queue / Claude resume / Anti wake / DSH headless)
│   ├── task-store.ts          # 1000 条任务持久化、原子写入与 LRU 淘汰
│   ├── platform.ts            # 平台差异唯一出口（PATH 探测/可执行位/扩展目录/进程树终止）
│   ├── agent-detector.ts      # 本地可执行文件与健康度探测
│   ├── model-catalog.ts       # 模型发现/缓存/默认值解析 (models-cache.json, model-config.json)
│   ├── model-approval.ts      # 模型默认优先解析 + 非默认切换的用户确认通道 (model-approvals/)
│   ├── task-retarget.ts       # 执行者改派：换 Agent / 换模型 + 原生会话重置与改派留痕
│   ├── caller-poll-guard.ts   # 派发后轮询抑制：同进度重复读取只回短载荷 + 停止指令
│   ├── round-log.ts           # 每轮日志/归档路径、数值最轮取用、分页读取
│   ├── reviewer-log.ts        # 检视方（caller）侧日志：逐轮落盘、字节上限、尾部读取
│   ├── bridge-policy.ts       # 跨进程策略镜像 (bridge-policy.json: contextCompaction)
│   ├── context-compactor.ts   # 上下文滞后压缩：协议注入、胶囊解析、归档、幂等与失败隔离
│   ├── token-usage-collector.ts# Token 计量：四端解析归一化、估算、跨 attempt 累加、读时派生汇总
│   ├── deepseek-harness.ts    # DeepSeek Harness 专属接入面 (安装探测/headless 契约/原生会话/MCP patch)
│   └── mcp-server.ts          # MCP JSON-RPC 协议解析与工具处理
├── config/
│   └── config-sync.ts         # 自动向 ~/.gemini, ~/.claude, ~/.codex, ~/.dsh 注入 MCP 配置
└── ui/
    ├── task-tree-provider.ts  # 侧边栏 1000 条任务列表与状态徽章渲染
    ├── detail-panel.ts        # 任务详情与多轮时间轴 Webview (含 30s 实时流与文件范围)
    ├── model-approval-service.ts # 非默认模型切换的模态确认对话框 (轮询 model-approvals/)
    └── status-bar.ts          # 状态栏当前 Agent 切换器
```

---

## 4. 关键接口规范 (MCP Tools)

四端 Agent 均可通过标准 MCP 协议调用以下能力（`targetAgent` / `initiatorAgent` 取值为 `default`、`codex`、`claude_code`、`antigravity`、`deepseek_harness`）：

| 工具名                    | 关键入参                                                           | 语义与行为                                                                                    |
| :------------------------ | :----------------------------------------------------------------- | :-------------------------------------------------------------------------------------------- |
| `dispatch_task`           | `targetAgent`, `title`, `prompt`, `expectedFiles`（必填）, `allowNoScope?`, `allowScopeOverlap?`, `callerSessionId?`, `model?`, `modelRequestedByUser?`, `initiatorModel?` | 异步发起新任务，`expectedFiles` 必须是**具体代码文件**（禁止目录、结尾斜杠、`*`/`?` 通配符），派发前校验失败即拒绝；**缺省即拒绝**（确实不改任何工作区文件时显式传 `allowNoScope: true`）。`model?` 固定**接收方模型**：**缺省即用该 Agent 配置的默认模型**；非默认型号必须同时传 `modelRequestedByUser: true`（仅在用户点名时）并由用户在 VS Code 点击确认才生效，否则忽略该型号回落默认（见第 18 条）。`initiatorModel?` 声明**发送方模型**（缺省按 环境提示 → 发送方配置默认 推断）；两端模型都随任务落账本，任务树/详情/MCP 响应统一展示 `发送 A(x) ➔ 接收 B(y)`，返修/重试沿用同一接收方模型。只要存在任何**未完成任务**（含 `waiting_review` / `paused`）就进入任务级 FIFO 队列；接收方进程内不可用。**返回后发起方必须立即结束 Turn**（回包含 `nextAction=END_TURN_AND_WAIT_FOR_CALLBACK` 与 `stopInstruction`，见第 19 条） |
| `request_rework`          | `threadId`, `feedback`, `targetAgent?`, `model?`, `modelRequestedByUser?` | **在同一个原生会话中唤醒接收方返修**，进入第 N+1 轮；传 `targetAgent`/`model` 时先改派执行者再开新轮（换 Agent 会重置原生会话，见第 18 条） |
| `approve_task`            | `threadId`, `remarks?`                                             | 验收通过，标记为 `COMPLETED`，归档任务                                                        |
| `pause_task`              | `threadId`, `reason`                                               | 暂停运行中/排队中的任务：终止 Agent 进程、轮次标记 `paused`、记录中断原因，可随后恢复          |
| `resume_task`             | `threadId`, `targetAgent?`, `model?`, `modelRequestedByUser?`      | 恢复暂停轮次：保留原 prompt 与原生会话；未捕获原生会话时以初始轮重新执行；带 `targetAgent`/`model` 时在恢复前改派执行者 |
| `retry_task`              | `threadId`, `targetAgent?`, `model?`, `modelRequestedByUser?`      | **原地重跑失败轮**：复用同一轮号、同一 prompt、同一原生会话；不追加新轮次，因此不会在账本里出现"看起来重复的第二轮"。调用者想"把原任务再做一遍"时必须用它，**不要**把原始 prompt 原样传给 `request_rework`；带 `targetAgent`/`model` 时换执行者重跑 |
| `retarget_task`           | `threadId`, `targetAgent?`, `model?`, `modelRequestedByUser?`, `reason?` | **执行者改派**：把已有任务切到另一个 Agent 和/或该 Agent 的另一个模型，保留 threadId、轮次历史与文件范围；`queued`/`paused`/`waiting_review`/`failed` 可改派且状态不变，有在飞轮次（先 pause）或已 `completed` 则拒绝。换 Agent 必重置原生会话，旧 id 记入 `retargets[].previousReceiverSessionId`；非默认模型仍需 `modelRequestedByUser: true` + 用户点击确认。receiver 不可用 |
| `allow_scope_overlap`     | `threadId`, `reason?`                                              | **放行文件归属互斥**：让被 scope 闸门挡住的任务继续执行（仅当重叠确实安全，例如前一个任务已提交）。调用者控制面工具，receiver 不可用 |
| `delete_task`             | `threadId`, `reason`, `force?`                                     | 永久删除任务：先终止进程，再移除账本条目，原因写入 `deleted_tasks.log` 墓碑；同时**尽力清理接收方原生对话**（Claude `~/.claude/projects`、Codex `~/.codex/sessions` + `session_index.jsonl`、Antigravity `conversations/*.db` + `brain/annotations/implicit` + cache 元数据、DSH `sessions/<slug>/<sessionId>/` + projcache + `workspace.json` 引用）。找不到对话不是错误，单个文件失败只记录在墓碑 `nativeSessions[].error`，绝不回滚删除；调用方（initiator）会话永不清理 |
| `record_merge_request`    | `threadId`, `url?`, `branch?`, `commit?`, `remote?`, `baseBranch?`, `status?`, `summary?` | **调用者/检视方专用**：任务或批次验收通过后，记录 MR URL/分支/提交/远端与状态；MR 由调用者创建并推送，receiver 不可调用 |
| `list_recent_tasks`       | `limit` (最大1000), `statusFilter?`                                | 查询任务列表（包含 `A -> B 标题 摘要` 与 `queued/running` 状态），每条附**读时派生**的 `tokenUsage` 汇总 |
| `get_thread_details`      | `threadId`, `includeRawLogs?`                                      | 查询任务轮次。**默认有界**：最新轮全量、已压缩轮给提纯卡片、其他超长历史轮截断到 4,000 字符并附 `recallHint`；**不默认返回千行日志**。`includeRawLogs: true` 才返回完整历史 + 最近日志（很大，仅在确需时使用）。顶层附**读时派生**的 `tokenUsage` 汇总 |
| `get_round_raw_log`       | `threadId`, `round`, `source?` (`output`/`execution`), `offsetBytes?`, `maxBytes?` | **单轮原文分页读取**：`output` 读压缩前归档（尚未归档时回退账本副本），`execution` 读原始 stdio 日志。默认 20KB、上限 200KB，用 `nextOffset` 续读；用于核验某一历史轮，避免整段历史灌入上下文 |
| `get_reviewer_log`        | `threadId`, `round?`, `offsetBytes?`, `maxBytes?`                  | **检视方（caller）侧原文分页读取**：读 `CallbackManager` 唤醒 caller 时 tee 落盘的 `logs/<threadId>_round_<n>.reviewer.log`（默认 20KB、上限 200KB）；`round` 省略时取最新有记录的轮。没有记录返回 `available=false` + `roundsWithReviewerLog`，不返回空串。receiver 的流在 `get_round_raw_log`；reviewer 的**裁决**仍在账本（approval remarks / 下一轮 feedback），本工具只还原它的过程输出 |
| `get_task_token_usage`    | `threadId`, `round?`                                               | **被调用方 Token 只读查询**：默认返回任务级汇总（输入/输出/缓存拆分、按 Agent/模型分布、`isPartialOrEstimated`）＋每轮一行摘要；给 `round` 返回该轮明细。计量口径见铁律 21；`unavailable` 不等于 0，估算值带 `estimated`；**不受轮询守卫影响**（不是进度信号） |
| `get_recent_logs`         | `threadId`, `maxLines?` (默认1000)                                 | **获取运行中或历史任务最近 1,000 行实时日志**（流式捕获与磁盘持久化对齐）                      |
| `invoke_agent`            | `targetAgent`, `prompt`, `model?`, `modelRequestedByUser?`          | 同步阻塞调用（仅限轻量即时问询）；`model?` 与 `dispatch_task` 同一套模型策略（缺省用配置默认，非默认需 `modelRequestedByUser: true` + 用户点击确认） |
| `list_agent_models`       | `targetAgent?` (`all` 或具体 Agent), `forceRefresh?`                | **读取各 Agent 可用模型**：Antigravity 走 `agy models`、Codex 走 `codex debug models`、Claude/DSH 走各自 provider 的 `/models`；`configuredModel` 置于 payload 最前，该型号同时**置顶** `models` 数组并打 `configured default` 标签 |
| `query_agent_mesh_status` | 无                                                                 | 查询四端 Agent 在网格中的连接与版本状态                                                       |

---

## 5. 开发、构建与验证指令

```bash
# 1. 安装开发依赖
npm install

# 2. 编译 TypeScript
npm run compile

# 3. 启动监听编译
npm run watch

# 4. 运行单测
npm test

# 5. 打包为 VSCode 插件 (.vsix)
npm run package

# 6. 安装本地插件到 VS Code
code --install-extension agent-mesh-bridge-1.0.0.vsix
```

---

## 6. 协同开发与代码守则

1. **会话连续性不可破坏**：在处理返修（`rework`）时，绝对禁止为接收方生成全新的 Session ID，必须严格复用 `receiverSessionId` 进行 `--resume`。
2. **免交互防御必须完整**：任何子进程拉起逻辑必须包含 `stdin.end()` 以及各自平台的 `skip-danger` 命令行开关，严禁无参数裸跑子进程。
3. **1000 条硬上限控制**：`TaskStore` 每次保存必须执行容量截断校验，禁止无限制无限追加导致内存和磁盘暴涨。
4. **日志与错误捕获**：所有进程退出码非 0、超时以及解析异常必须记录在对应 `DelegationRound.error` 中并向上冒泡，不得静默吞并。
5. **DSH 会话语义不可臆造**：DSH 的 `--session-id` 只用于**接管已存在**的 Session。新任务严禁传该参数，必须在首轮结束后由事件流回填 `receiverSessionId`，之后返修轮次才复用该 id。
6. **DSH patch 必须可反复同步**：写入 `~/.dsh/profiles/*/cordis.patch.yml` 的内容必须包裹在托管标记内，覆盖式更新而非追加，严禁重复插入或破坏用户自有 patch 条目。
7. **单轮在飞不可重叠（硬不变量）**：同一任务在任一时刻**只允许一个 round 处于 `running`**。上一轮没跑完，绝对不允许开下一轮。
   - `requestRework()` / `retryFailedRound()` 在创建或重试轮次前必须校验无在飞轮次，命中则**直接拒绝**并给出可操作错误，不得排队或静默放行。
   - 轮号必须**单调递增**：统一使用 `nextRoundNumber(thread)`（基于已有最大轮号），严禁再用 `currentRound + 1`——`retryFailedRound` 会回退 `currentRound`，两者相加必然撞号。
   - `TaskStore.updateRound()` 命中重复轮号时更新**最后一条**（最新）记录；且在已有在飞轮次时**拒绝追加**新的 `running` 轮次。
   - `TaskSupervisor.executeThreadRound()` 必须拒绝在**另一轮在飞**时启动本轮；`normalizeRoundIntegrity()` 负责自愈历史脏数据（保留最高轮号的在飞轮，其余标记失败），避免新约束把老账本永久卡死。
8. **全局单飞不可绕过**：`AsyncDispatcher` / `ReworkController` / `TaskSupervisor` 的每一条执行路径都必须在 spawn 前 `ExecutionGate.tryAcquire()`，结束后在 `finally` 中 `release()`。
   - 一律通过 `findInFlightRound()` + `ExecutionGate` 判断可否启动；**不得**再以“文件范围不冲突”作为并发执行的依据。
   - `expectedFiles` 的锁范围必须收窄到**具体代码文件**：`validateExpectedFiles()` 拒绝目录、结尾斜杠、`*`/`?` 通配符；`doPathsConflict()` 只做精确同文件匹配，不再做目录前缀/glob 展开。
   - 非 supervisor 路径（独立 MCP server 进程）在轮次结束后必须调用 `QueueDrainer.schedule()` 泵下一轮，避免队头完成后队列永久停滞。
   - 锁文件带 5s 心跳与 30s 僵尸判定；进程崩溃后可以安全接管，但**不得**手动删除仍新鲜的锁。
   - 锁内必须记录真实 Agent 子进程 `childPid`（`onChildPid` 回填，子进程退出清空）。Windows 上父进程崩溃不杀子进程，存活判定必须同时探活宿主与子进程：宿主已死但子进程仍活着时，锁在整个孤儿宽限期（120s）内继续占用；超期后先 `taskkill /T /F` 杀掉孤儿再接管，**禁止**在孤儿仍存活时让下一个轮次开跑。
   - `pause_task` / `delete_task` 必须调用 `ExecutionGate.killOrphanChild(threadId)`，使宿主重启后仍能杀掉上一轮遗留的子进程。
9. **接收方身份必须显式标记**：`AutonomousRunner` 拉起任务进程时必须写入 `MESH_TASK_RECEIVER=1`、`MESH_TASK_THREAD_ID`、`MESH_TASK_ROUND`。
   - `CallbackManager` 唤醒发起方时**必须清除**这些变量，否则发起方会被误判为 receiver 而无法调用 `request_rework` / `approve_task`。
   - MCP server 的 `dispatch_task` / `invoke_agent` / `invoke_*` 在 receiver 进程中必须从 `tools/list` 隐藏且调用即拒绝；`request_rework` / `approve_task` 仅允许作用于“自己作为发起方”的其他线程。
10. **权限旁路参数必须按 CLI 语法落位**：Codex 的 `--ask-for-approval` 是全局参数，只能放在 `exec` 之前；任何放在子命令之后的写法都会被 CLI 直接判为 `unexpected argument`。DSH 每次拉起都必须带 `DSH_PERMISSION_MODE=danger-full-access`。
11. **暂停/删除必须可跨进程生效且可追溯**：
   - 暂停/删除先持久化目标状态，再写 `~/.agent-mesh-bridge/cancellations/<threadId>.json`；真正持有子进程的 runner 轮询该文件并终止进程树。
   - 禁止只改账本状态而不终止进程；也禁止只杀进程而不记录 `TaskInterruption`。
   - 暂停轮次必须保留原 prompt 与 `receiverSessionId`，恢复时使用 `shouldResumeNativeSession()` 决定续聊还是重新初始执行。
   - `delete_task` 先取消进程，再 `TaskStore.deleteTask()`，并向 `deleted_tasks.log` 追加包含原因的墓碑记录；随后调用 `cleanupThreadNativeSessions()` **尽力**清理接收方原生对话（Claude / Codex / Antigravity / DSH），找不到对话不算错误，单个文件失败只写入墓碑 `nativeSessions[].error`，不得回滚删除、不得阻断队列推进；`callerSessionId` 对应的调用方会话**永远不清理**。
12. **响应成功判定不可只看退出码**：
   - `AutonomousRunner.evaluateResponse()` 统一计算两段式结果：`exitCode` + `stream` 健康度。
   - `extractAnswerOnly()` 负责识别真正的最终答案；Codex `item.completed.agent_message`、DSH `final`、Antigravity `result`、Claude result 均已适配。
   - 执行器一律使用 `result.assessment.success`（缺失时才回退 `exitCode === 0`），并把 `assessment` 写入轮次。
   - Codex `exec --json` 的 `thread_id` 必须作为原生会话 ID 采纳，不能继续使用预生成 UUID。

13. **单任务角色锁必须覆盖所有入口**：
   - `CallbackManager.notifyCaller()` 必须先校验任务处于 `waiting_review`、无在飞轮次，并取得 `reviewer` 角色锁；重复唤醒直接拒绝，不得再拉起第二个检视进程。
   - `AsyncDispatcher` / `ReworkController` / `TaskSupervisor` 的 developer 轮次必须先取得 `developer` 角色锁；若 reviewer 仍持有角色则标记 `queued`，等交接后由 supervisor/queue-drainer 启动。
   - developer 轮次结束、状态落为 `waiting_review` 后，必须在唤醒 reviewer **之前**释放 developer 角色锁，避免“开发者等检视、检视等开发者”的死锁。
   - `request_rework` / `approve_task` 成功即视为 reviewer 决策完成，MCP server 必须调用 `TaskRoleLock.releaseIfRole(threadId, 'reviewer')` 完成交接；reviewer CLI 进程仍在打印最终文本不算活跃检视。
   - 中断恢复只能沿用原 `threadId`、原 `receiverSessionId` / `callerSessionId`，并以账本中历轮 prompt/output/feedback 作为中间文档继续；严禁新建任务、另起会话或把同一任务拆成新的 reviewer/developer 任务。

14. **任务级 FIFO 不可退化为轮次级**：
   - `findQueueHead()` 必须按 `createdAt` 选出最早未完成任务；`isTaskUnfinished()` 对 `waiting_review` / `paused` 返回 true。
   - 队头无在飞轮次（waiting_review/paused）时，`TaskSupervisor` / `QueueDrainer` 必须直接停止本轮调度，不得跳到后面的任务。
   - `AsyncDispatcher` 派发新任务时必须检查全部未完成任务，而不是只检查 running/reworking 或文件冲突。
   - approve / delete / retry / resume 改变队头状态后，必须通过 `TaskStore.changed` 或 supervisor 轮询自动推进队列。
   - `failed` 仅作为终止态释放队列；若要让失败任务继续占位，必须先把它重新置为未完成状态（retry/resume）。

15. **MR 收口不可绕过调用者**：
   - 开发者(receiver)只交付代码与测试报告，不提交、不推送、不建 MR。
   - 调用者/检视方在 approve 后汇总同批改动，只创建一个 MR，推送远端，并调用 `record_merge_request`.
   - `record_merge_request` 必须持久化到 `thread.mergeRequest`（url/branch/commit/remote/baseBranch/status/summary），并在任务树与详情面板展示。
   - receiver 进程调用该工具必须被拒绝；已完成任务仍允许补记 MR。

16. **模型两端必须留痕**：每条任务必须同时记录**发送方模型**与**接收方模型**。
   - `targetModel` 在派发时解析并固定（显式 `model` → 接收方配置默认），返修/重试默认沿用同一模型，不得中途静默换模型；确实需要换执行者时只能走显式改派（`retarget_task`，或 `request_rework` / `retry_task` / `resume_task` 的 `targetAgent` / `model` 覆盖参数），并写入 `thread.retargets[]` 留痕（见第 18 条）。
   - `initiatorModel` 按 `显式传参 → 环境提示（如 ANTHROPIC_MODEL / CODEX_MODEL）→ 发送方配置默认` 推断，并用 `initiatorModelSource` 标注来源，禁止把推断值伪装成用户显式选择。
   - 任务树、详情面板、`dispatch_task` 响应、`list_recent_tasks` / `get_thread_details` 必须统一使用 `getModelRouting()` 输出 `sender A: x -> receiver B: y`，不得各写一套格式。
   - 模型参数注入必须按各 CLI 语法落位：Codex 用全局 `-c model="..."`（在 `exec` 之前），Claude / Antigravity 用 `--model`，DSH 用一次性 `--patch` overlay（不得改写用户 profile patch）。

17. **文件归属互斥不可绕过**：
   - `AsyncDispatcher` / `TaskSupervisor` / `ReworkController` 在 spawn 前都必须调用 `findScopeConflicts()`，命中即 `markQueued(..., scopeConflict)`；**不得**只在 dispatch 时检查一次就放行。
   - 每个轮次必须用 `captureWorkspaceSnapshot()` / `diffWorkspaceSnapshots()` 记录 `round.touchedFiles`；非 git 工作区允许为空（`available: false`，不得因此把整棵树当成"全被改动"）。
   - `expectedFiles` 必填由 MCP 层 `requireScope: true` 触发（内部调用与测试保持宽松），**不得**删除 MCP 侧校验。
   - `scopeBlockedBy` 必须写在实际被 `upsertTask` 的对象上（`markQueued` 内部处理），否则 UI 与账本会丢失阻塞原因。
