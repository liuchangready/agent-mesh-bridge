# 📊 被调用者 Token 消耗统计机制设计规范 (Token Usage Accounting)

> **文档标识**：`docs/TOKEN_STATISTICS_DESIGN.md`  
> **版本**：1.1.0（修订自 1.0.0）  
> **设计状态**：已评审并落地（v1.1：归一化契约 + 按轮计量 + 只落轮级数据）  
> **适用范围**：Agent Mesh Bridge 核心调度器、各接收方 Agent (`codex`, `claude_code`, `antigravity`, `deepseek_harness`)、MCP 查询接口及 VS Code 交互视图

---

## 0. 修订摘要（1.0.0 → 1.1.0）

1.0.0 的方向成立：**按轮计量**、**每轮独立记录执行者与模型**、**只统计被调用方**、**元数据免疫上下文压缩**。但四端 CLI 协议事实与计量口径存在硬伤，照 1.0.0 实现会得到“看起来精确、实际偏差数十倍”的数字。本版改写如下。

| # | 1.0.0 的问题 | 1.1.0 的结论 |
| :-- | :--- | :-- |
| 1 | 统一按 `total = inputTokens + outputTokens` 计算 | **不成立**。Codex / Antigravity 的 `input` 已含缓存，Claude / DSH 的 `input` 不含缓存。按 1.0.0 的公式，DSH 实测一步会**少报约 182 倍**（3,611 vs 657,435），Claude 会少报 45%，Antigravity 若再叠加缓存会多报 7.6%。必须先归一化（§2）。 |
| 2 | 事件名/字段名凭印象写 | 与真实 CLI 不符：DSH 的 usage 在 `status/step_end`（camelCase），`final` **不带** usage（实测 42 个含 `final` 的日志 0 命中）；Codex 是扁平 `cached_input_tokens / cache_write_input_tokens / reasoning_output_tokens`；Claude 顶层是 `total_cost_usd` 而非 `cost_usd`。见 §3 能力矩阵。 |
| 3 | 未区分“单次计数”与“会话累计” | Antigravity `result.usage` 是**整段会话累计**（实测 `input_tokens=17,276,704`、`num_turns=13`），不可与逐步 usage 相加；跨轮求和也会重复计费。必须规定每端**唯一可信来源**（§3）。 |
| 4 | 三源并行（stdout / result / transcript）无去重 | 会翻倍。Antigravity 的 transcript 实测 4.1MB / 1319 条，且采样会话 id 在 `brain/<id>` 下无对应目录。v1 **砍掉 transcript 通道**，只解析 stdout 逐步 usage。 |
| 5 | 重试/恢复/中断的累加点未落到代码 | `AutonomousRunner.execute()` 只返回最后一次 attempt；`buildInterruptedResult()` 不透传。1.1.0 明确累加点与传播链（§4）。 |
| 6 | 线程级聚合落盘（`byRound` + `thread.tokenUsage` + `summary.totalTokens`） | 三重冗余且会被旧进程副本回写打回（与压缩被“复活”同类问题）。1.1.0 **只持久化轮级 `round.tokenUsage`**，线程级汇总一律读时派生（§5）。 |
| 7 | `unavailable` 用 0 表示 | 0 会被 UI 读成“免费”。1.1.0 增加 `available` / `partial` / `notes`，并把“输入不可估”显式标出（§2、§6）。 |
| 8 | `estimatedCostUsd` 混合原生与估算 | Claude 的 `total_cost_usd` 是 **API 等价成本**（订阅制下不是实付），代理/自建 provider 没有可信价格。1.1.0 拆成 `costUsd` + `costSource`，**v1 只展示原生成本**（§2、§6）。 |
| 9 | 无测试计划 | 1.1.0 补齐 fixture 与验收清单（§8）。 |

---

## 1. 目标与边界

- **任务级**：查看某任务自派发以来的**被调用方**累计 Token（输入/输出/缓存拆分、模型分布、是否含估算）。
- **轮次级**：查看**某一轮**的 Token、执行 Agent 与模型。
- **只统计被调用方（Callee）**：发起方（Caller）自身消耗不在范围内，UI 与 API 标注 “Callee”。
- **不破坏既有铁律**：不新增子进程（铁律 8）、不引入轮询（铁律 19）、压缩改写 `round.output` 时计量元数据必须存活（铁律 20）、改派后每轮归属清晰（铁律 18）。

---

## 2. 归一化契约（核心）

### 2.1 规范字段

所有四端在采集后统一到下面这组字段，**UI 与 MCP 只能使用这一组**：

| 规范字段 | 含义 |
| :--- | :--- |
| `uncachedInputTokens` | 未命中缓存的输入 token（cache miss） |
| `cacheReadTokens` | 命中缓存读取的 prompt token |
| `cacheWriteTokens` | 写入/创建缓存的 prompt token |
| `promptTokens` | **完整输入** = `uncachedInputTokens + cacheReadTokens + cacheWriteTokens` |
| `outputTokens` | 生成 token（含被 CLI 计入 output 的思考 token） |
| `reasoningTokens` | 思考 token，**是 `outputTokens` 的子集**，只作展示，**不得再计入 total** |
| `totalTokens` | `promptTokens + outputTokens`（本桥接的规范口径） |
| `nativeTotalTokens?` | CLI 自报的 total（若存在），用于交叉校验与差异告警，**不参与聚合** |

> `totalTokens` 永远按本表计算，保证四端可比。CLI 自报的 total 单独放在 `nativeTotalTokens`，出现差异时不静默改写（差异说明该 CLI 的 cache 约定与我们的假设不同，需更新能力矩阵）。

### 2.2 各端 cache 约定（能力矩阵的一部分）

| Agent | `input` 是否含缓存 | 归一化规则 |
| :--- | :--- | :--- |
| Codex | **含**（`cached_input_tokens` 是子集） | `cacheRead = cached_input_tokens`；`uncached = max(0, input_tokens - cached_input_tokens - cache_write_input_tokens)`；`cacheWrite = cache_write_input_tokens` |
| Antigravity | **含**（`cache_read_tokens` 是子集） | `cacheRead = cache_read_tokens`；`uncached = max(0, input_tokens - cache_read_tokens)`；`cacheWrite = 0`（该端不报） |
| Claude | **不含**（cache 另计） | `uncached = input_tokens`；`cacheRead = cache_read_input_tokens`；`cacheWrite = cache_creation_input_tokens` |
| DSH | **不含**（cache 另计） | `uncached = inputTokens`；`cacheRead = cacheReadTokens`；`cacheWrite = 0`（该端不报） |

### 2.3 估算的边界

- **只能估输出**。返修轮的输入是「整段历史 + 工具结果 + 系统提示」，与当前 prompt 字符数无比例关系，**禁止**用字符数估输入。
- 输出估算：CJK 约 1.5 chars/token，其余约 3.8 chars/token；估算值一律 `source='estimated'`、`partial=true`，UI 加 `~`。
- 拿不到任何原生 usage 且没有输出文本时：`source='unavailable'`，计数为 0 但 `available=false`，UI 显示「未知」而不是「0」。

---

## 3. 四端能力矩阵（唯一可信来源）

| Agent | usage 位置 | 真实字段 | 粒度 | 累计？ | 中断轮可用 | 备注 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Codex | stdout JSONL `{"type":"turn.completed","usage":{...}}` | `input_tokens` / `cached_input_tokens` / `cache_write_input_tokens` / `output_tokens` / `reasoning_output_tokens` / `total_tokens` | 每 turn | 否（单 turn 口径） | 否（只在结束时上报） | 多 turn 求和 |
| Claude | stdout 单个 JSON 结果包 | `usage.input_tokens` / `output_tokens` / `cache_read_input_tokens` / `cache_creation_input_tokens`；顶层 `total_cost_usd` / `num_turns` / `modelUsage` | 每次运行 | **待 fixture 钉死**（假设为本次运行累计） | 否 | 多模型时以 `modelUsage` 兜底 |
| Antigravity | stdout JSONL `{"event":"step_update","step_update":{"usage":{...}}}` | `input_tokens` / `cache_read_tokens` / `output_tokens` / `thinking_tokens` / `total_tokens` | 每 step | 否 | **是** | `result` 事件是**会话累计**，禁止使用 |
| DSH | stdout JSONL `{"type":"status","phase":"step_end","usage":{...}}` | `inputTokens` / `outputTokens` / `cacheReadTokens` / `totalTokens` | 每 step | 否 | **是** | `final` 事件**不带** usage；`turn_end` 也不带 |

采集规则：

1. **每端只认一个来源**，杜绝多源相加。
2. 逐步事件（Antigravity / DSH）按 step 求和；轮级事件（Codex `turn.completed` / Claude 结果）按事件求和。
3. 遇到“会话累计”型结果（Antigravity `result`）**直接丢弃**，不参与聚合。
4. 中断轮：能拿到逐步 usage 即为原生部分量（`partial=true`）；拿不到则只估输出，输入标未知。

---

## 4. 采集与累加

### 4.1 采集时机

**进程退出时一次性解析完整 stdout**。理由：runner 已把完整 stdout 留在内存（被 kill 的中断轮同样保留），流式解析既无必要又必须处理跨 chunk 半行 JSON 与重复计数。

采集器为**纯函数**模块 `src/bridge/token-usage-collector.ts`：

```ts
collectTokenUsage(agent, stdout, ctx): TokenUsageMetrics | undefined
accumulateTokenUsage(a, b): TokenUsageMetrics          // 重试 / 同轮重跑累加
estimateOutputTokens(text): number
aggregateThreadTokenUsage(rounds): ThreadTokenUsageSummary  // 读时派生
```

### 4.2 累加链（必须逐点落实）

```
execute()  ── 每个 attempt ──> executeOnce() ──> close 时 collect
    │                                             │
    │  accumulate(accumulated, attempt.tokenUsage) │
    ▼                                             ▼
result.tokenUsage = accumulated（覆盖最后一次 attempt 的值）
```

- 重试（铁律 8/10 的瞬时错误重放）：**所有 attempt 的消耗累加**，不可只留最后一次。
- `buildInterruptedResult()`：透传已有 `tokenUsage`，中断前未产生输出时用 `estimateOutputTokens` 并标 `partial`。
- `retry_task` / `resume_task`：**复用同一轮号**，token 走**累加**而不是覆盖（`accumulateTokenUsage`），否则重试一次就把上一轮的钱抹掉。
- 三个执行器（dispatcher / rework / supervisor）收轮时把 `tokenUsage` 写进 `round`；catch 兜底构造的 `RunResult` 明确不带 usage（该轮无任何采集）。

### 4.3 与压缩、改派、队列的共存

1. **铁律 20（滞后压缩）**：压缩只改写 `round.output`，`round.tokenUsage` / `executorAgent` / `executorModel` 属结构化元数据，**严禁清空或篡改**；压缩后仍能 100% 读到该轮历史消耗。
2. **铁律 18（改派）**：每轮的 `executorAgent` / `executorModel` 在**收轮时**写入（用实际执行者与实际模型），与 `tokenUsage.agent/model` 互相印证；token 缺失时执行者信息依然完整。
3. **铁律 8/19**：采集是纯解析，不拉起任何进程；查询走既有 MCP 通道，不引入轮询。

---

## 5. 存储策略

### 5.1 只持久化轮级

```ts
interface DelegationRound {
  // ...既有字段
  executorAgent?: AgentId;      // 本轮实际执行者（与 token 无关，始终写入）
  executorModel?: string;       // 本轮实际模型
  tokenUsage?: TokenUsageMetrics; // 本轮计量（缺失 = 从未采集）
}
```

- **不持久化** `thread.tokenUsage` / `byRound` / `summary.totalTokens`。
- 线程级汇总由 `aggregateThreadTokenUsage(rounds)` **读时派生**（≤ 数十轮，O(n) 可忽略），避免旧进程副本回写导致的数值回退。
- 侧边栏徽章同样从 `thread.rounds` 现算，不读缓存字段。

### 5.2 跨进程合并保护

`TaskStore.upsertTask()` 的轮次合并必须遵循：**结构化元数据取“已定义且更新”的那份**。涉及 `tokenUsage` / `assessment` / `touchedFiles` / `executorAgent` / `executorModel` / `isCompacted`。

> 背景：此前“上下文压缩”正是因为旧进程副本回写而让原文复活（见 `mergeRoundPreferringCompaction`）。计量元数据属同类耐久数据，必须同等保护。

`TaskStore.updateRound()` 的**追加分支**目前用白名单字段构造新轮，新增字段必须显式加入，否则“新建轮 + tokenUsage”会静默丢失。

### 5.3 冷热分层

- 计量数据体积极小（每轮约 250 字节），随账本热存即可，不进冷存。
- 账本体积仍由 `round.output` 主导，继续由铁律 20 的滞后压缩控制。

---

## 6. 数据契约

```ts
export type TokenMetricSource = 'native_reported' | 'estimated' | 'unavailable';

/** 成本的来源：只有原生成本可信。 */
export type TokenCostSource = 'native' | 'none';

export interface TokenUsageMetrics {
  uncachedInputTokens: number;
  cacheReadTokens: number;
  cacheWriteTokens: number;
  /** 完整输入 = uncached + cacheRead + cacheWrite */
  promptTokens: number;
  outputTokens: number;
  /** outputTokens 的子集，仅展示 */
  reasoningTokens?: number;
  /** promptTokens + outputTokens（本桥接口径） */
  totalTokens: number;
  /** CLI 自报 total，仅交叉校验 */
  nativeTotalTokens?: number;

  agent: AgentId;
  /** 实际模型（CLI 上报优先，回落桥接解析出的生效模型） */
  model?: string;
  /** CLI 上报的原始模型串（可能是别名） */
  reportedModel?: string;
  /** 桥接请求的模型（thread.targetModel） */
  requestedModel?: string;

  /** 原生成本（USD）；仅当 CLI 自报时存在 */
  costUsd?: number;
  costSource?: TokenCostSource;

  source: TokenMetricSource;
  /** false = 本轮完全没有采集到数据（计数为 0 不代表免费） */
  available: boolean;
  /** true = 数据不完整（中断、估算、缺字段） */
  partial?: boolean;
  /** 覆盖的进程 attempt 数 */
  attempts?: number;
  /** 人类可读的说明（缺什么、为什么） */
  notes?: string;
  collectedAt: string;
}

export interface ThreadTokenUsageSummary {
  totalPromptTokens: number;
  totalUncachedInputTokens: number;
  totalCacheReadTokens: number;
  totalCacheWriteTokens: number;
  totalOutputTokens: number;
  totalTokens: number;
  /** 仅原生成本之和；无原生成本时为 undefined */
  totalCostUsd?: number;
  costSource?: TokenCostSource;
  /** 按 Agent 维度的 totalTokens 分布 */
  byAgent: Partial<Record<AgentId, number>>;
  /** 按模型维度的 totalTokens 分布 */
  byModel: Record<string, number>;
  /** 存在估算/不完整/缺采集的轮次 */
  isPartialOrEstimated: boolean;
  /** 参与统计的轮次数 */
  measuredRounds: number;
  /** 总轮次数 */
  totalRounds: number;
}
```

说明：

- `byRound` 被移除：`rounds[].tokenUsage` 已是同一份数据的唯一来源，不再复制。
- `estimatedCostUsd` 被移除：原生成本与价格表估算不能混在一个字段里；v1 不做价格表估算。

---

## 7. 查询与交互

### 7.1 MCP 工具

| 工具 | 入参 | 语义 |
| :--- | :--- | :--- |
| `get_task_token_usage` | `threadId`, `round?` | 轻量只读：默认返回任务级汇总 + 每轮一行摘要；给 `round` 则返回该轮明细。**不进入 `CallerPollGuard`**（不是进度信号，载荷很小）。 |
| `get_thread_details` | `threadId`, `includeRawLogs?` | 在返回体顶层附加**派生**的 `tokenUsage` 汇总；每轮的 `tokenUsage` 随轮次一并返回。 |
| `list_recent_tasks` | `limit?`, `statusFilter?` | 每条附**派生**的 `tokenUsage` 汇总（供列表快速比较开销）。 |

### 7.2 回调唤醒

唤醒 reviewer 的消息在有计量时追加一行（无数据则不写）：

```
📊 Callee Token: Round 2 ≈ 8.2k (输入 6.8k · 输出 1.4k · 缓存 4.7k) · 任务累计 23.5k
```

估算值带 `≈`，未采集到显示「未知」，不显示 0。

### 7.3 VS Code 界面

- **详情面板顶栏**：新增「🪙 被调用方 Token」区，展示任务累计（输入/输出/缓存/成本），估算时加 `~`。
- **轮次卡片**：标题右侧显示该轮徽章 `[⚡ 8.2k tok (In 6.8k · Out 1.4k)]`；`partial` 轮加 `~`。
- **侧边栏任务树**：`description` 追加 `· 23.5k tok`（估算加 `~`，无数据显示 `· tok 未知`）。

---

## 8. 异常边界与鲁棒性

| 场景 | 行为 |
| :--- | :--- |
| 进程被暂停 / 强杀 | 逐步型 Agent（Antigravity / DSH）可从已缓冲 stdout 提取部分原生 usage（`partial=true`）；其余端只估输出，输入标未知。已产生的开销必须落账。 |
| 非 0 退出 | 同上；能解析到 usage 就用原生值。 |
| 代理剥离 usage 字段 | 走输出估算（`estimated` + `partial`），UI 加 `~`；**不估输入**。 |
| 瞬时错误重试 | 所有 attempt 累加（§4.2），`attempts` 记录次数。 |
| 同轮重跑（`retry_task` / `resume_task`） | 轮号不变，token 累加而非覆盖。 |
| 跨进程账本竞争 | 依赖既有 `execution.lock` + 合并保护（§5.2），计量只写不读回。 |
| CLI 自报 total 与规范口径不一致 | 保留 `nativeTotalTokens` 供差异排查，**不自动改写**。 |

---

## 9. 代码落点

| 文件 | 改动 |
| :--- | :-- |
| `src/bridge/types.ts` | `TokenMetricSource` / `TokenCostSource` / `TokenUsageMetrics` / `ThreadTokenUsageSummary`；`DelegationRound` 增 `executorAgent` / `executorModel` / `tokenUsage`。 |
| `src/bridge/token-usage-collector.ts`（新） | 四端解析、归一化、估算、累加、线程级派生汇总（纯函数）。 |
| `src/bridge/autonomous-runner.ts` | `RunResult` 增 `tokenUsage` / `model`；`close` 时采集；`execute()` 跨 attempt 累加；`buildInterruptedResult()` 透传。 |
| `src/bridge/task-store.ts` | 追加分支白名单补 `tokenUsage` / 执行者字段；合并保护覆盖计量元数据。 |
| `src/bridge/async-dispatcher.ts` / `rework-controller.ts` / `task-supervisor.ts` | 收轮写入 `tokenUsage` + `executorAgent` + `executorModel`。 |
| `src/bridge/mcp-server.ts` | 注册 `get_task_token_usage`；`get_thread_details` / `list_recent_tasks` 附加派生汇总。 |
| `src/ui/detail-panel.ts` / `src/ui/task-tree-provider.ts` | 顶栏、轮次徽章、树徽章。 |
| `src/bridge/callback-manager.ts` | 唤醒消息追加计量行。 |

---

## 10. 验收与测试

`src/test/token-usage.test.ts`（fixture 驱动，全部为纯函数测试，不 spawn 进程）：

1. **DSH 大缓存步**：`inputTokens=2266 / outputTokens=1345 / cacheReadTokens=653824 / totalTokens=657435` → 规范 `promptTokens=656,090`、`totalTokens=657,435`；断言 **不得**等于 `input+output`（防回归到 1.0.0 的错误公式）。
2. **Claude 含/不含缓存两例**：`input=8420 / cache_read=6200 / cache_creation=1200 / output=650` → `promptTokens=15,820`、`totalTokens=16,470`。
3. **Antigravity 逐步求和、result 累计不参与**：`step_update` 逐步累加；`result` 的会话累计值必须被丢弃。
4. **Codex 字段名**：`cached_input_tokens` / `cache_write_input_tokens` / `reasoning_output_tokens` 正确归一，`reasoning` 不重复计入 total。
5. **缺 usage → 不可估输入**：无 usage 事件时只估输出，`promptTokens=0`、`partial=true`、`available=true`；完全无输出 → `available=false`。
6. **累加**：`accumulateTokenUsage` 合并两次 attempt，`attempts=2`，各分量相加。
7. **同轮重跑不覆盖**：两次收轮累加后 total 为两者之和。
8. **聚合派生**：线程汇总等于各轮之和；含估算轮时 `isPartialOrEstimated=true`。
9. **压缩不伤元数据**：压缩后 `round.tokenUsage` 与 `executorAgent` 仍在。
10. **旧副本回写不丢计量**：旧 `upsertTask` 不得抹掉已有 `tokenUsage`。
11. **MCP**：`get_task_token_usage` 返回汇总与单轮明细；`get_thread_details` 顶层带派生汇总；工具总数 18。

---

## 11. 后续（不在 v1.1 范围）

- 价格表估算（`costSource='price_table'`）与价格版本管理。
- 按 provider/模型的成本换算（含人民币）。
- 令牌级“上下文占用”视图（区别于账单 token：缓存命中与真实占用）。
- 子 Agent（Claude Task / Codex collab）内部用量的独立归属。
