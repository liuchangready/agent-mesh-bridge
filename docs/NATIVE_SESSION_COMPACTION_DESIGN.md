# 原生会话压缩

> 版本：0.2 · 状态：实现进行中；自动维护默认关闭，未经本机路径/版本/provider 认证的适配保持 `needs_validation`。  
> 设计基线：2026-11-06 用户批准的 0.1 快照。该快照是本任务输入；原始 `D:\agent_bridge\docs\NATIVE_SESSION_COMPACTION_DESIGN.md` 未从集成 ref 带入，本次不读取或覆盖用户 checkout。  
> 前置设计：[账本上下文滞后压缩 v1](CONTEXT_COMPACTION_DESIGN.md)。本设计只增加原生会话层，不改变 v1 的账本规则。

## 1. 不变量

- 业务 thread、原生 session ID、任务 worktree、分支、审批 SHA 和本次合法模型路由保持不变。原生压缩只能通过接收 Agent 支持的控制面完成，禁止直接写 rollout、JSONL、SQLite、protobuf 或 zstd 文件，禁止把 `/compact` 当作后台 API，也不以模型摘要、截断返回值或新会话冒充压缩。
- 只维护 Bridge 能证明由当前任务角色独占的 developer 或 dedicated reviewer 会话。legacy `callerSessionId`、requester 会话与 Copilot 原生会话永不自动维护。首轮 session 尚未捕获、bootstrap pending、活跃 turn、暂停未恢复、失败通知、终态、已归档或已删除任务不自动维护。
- `contextCompaction` 胶囊仍只用于账本展示。恢复资料由可信账本、Git 元数据和本轮证据确定性生成，二者互不替代，也不改变审批结论。
- 维护和业务轮使用不同结果判据。业务仍要求零退出码及有效最终文本；维护不要求 assistant 文本，但必须具备本次 thread 的压缩 item 完成、对应 completed turn、关闭后同 ID 恢复且压缩 item 在持久历史可见的证据。RPC ACK、metadata、旧 turn 或不相关 thread 事件均不算成功。

## 2. 能力状态

状态按 agent、实际 executable 路径与版本、provider 路由和模型缓存；任何一个变化都使之前的认证失效。

| Agent | Bridge 主动压缩 | 自动维护 |
| --- | --- | --- |
| Codex | 使用私有 stdio app-server 的 `thread/compact/start`；只接受同 thread 的 `contextCompaction` `item/completed` 与对应 `turn/completed(status=completed)`；关闭后重新 resume 并读取相同 turn 的压缩 item | 默认 `off`；仅有匹配认证记录时 `auto` 才可执行。ACK 后断连、无法确认同 ID 历史或模型/provider/worktree 改变均记 `unknown`，不重放 |
| Claude Code | 沿用原生自动压缩。未验证手动后台控制接口前不接入 | 不主动维护 |
| Antigravity | 当前只确认 `--conversation` 续聊 | `unsupported` |
| DeepSeek Harness | 当前只确认已捕获 session 的接管语义 | `unsupported` |
| Copilot Chat | 不属于 CLI 原生会话接入 | `unsupported` |

Codex RPC 的空对象响应只是受理。当前 Codex 0.164.0-alpha.11.1 实测未产生旧式 `thread/compacted` 通知，而是产生完成的 `contextCompaction` item 及 completed turn；实现按这组与 thread/turn 关联的结构化证据判断，并在关闭后重新载入历史。若未来版本改变事件形状，必须重新验证，不能把 metadata 当替代物。

## 3. 时机和策略

- 设置仅为 `agentMeshBridge.nativeSessionCompaction.mode = off | auto`，默认 `off`，并独立镜像到 `bridge-policy.json`。关闭 Bridge 维护不关闭 CLI 自己的自动压缩。
- 候选触发初值：可靠的原生当前上下文占用达到 70%；没有占用指标时，自上次确认压缩后完成 4 个业务 turn。成功维护或识别到原生自动压缩后至少再完成 2 个 turn。占用未知时 UI 明示未知，不能用账本 token、历史文件大小或字符数推算。
- 最小间隔、窗口占用、维护时限 120 秒均按活跃时间计算并扣除 `HostSuspensionGuard` 检出的宿主挂起。维护没有最终文本要求，不复用业务静默失败规则。
- 每个 operation 不自动重试。只有明确拒绝且确认没有变更可作为已知失败；ACK 后断连或被中断先分类 `unknown/interrupted`，禁止重发压缩请求。

## 4. 同会话独占和生命周期

写锁顺序固定为 thread 角色锁 → `agent + callerSessionKey(nativeSessionId)` 的 session 写租约 → 执行登记。session 租约是跨进程原子文件，owner token 与子 PID 共同标识；只有 owner 与 child 都确认死亡才回收，心跳过期本身不能抢活锁。Codex bare UUID 与 rollout 文件名共用 `callerSessionKey()` 归一规则。

租约覆盖压缩进程和紧随其后的业务进程，中间不释放。普通 developer、reviewer bootstrap/wake 和维护必须走同一租约；角色交接不代表原进程已退出。等待/运行使用 `TaskExecutionRegistry` 登记 PID，并监听既有取消文件；暂停、删除、改派、归档后重新读取账本、墓碑、角色 owner、session、Agent、模型和 SHA，再决定能否继续。不得为维护新建 thread 或第二种角色。

首轮尚无真实 session ID 时不压缩。任务旧账本或其他 Agent 无法证明租约绑定时，跳过主动维护并保留既有行为。Bridge 锁不能声称约束用户手动运行的外部 CLI；如果适配不能确认原生 session 闲置或外部写入冲突，能力不能认证。

## 5. Codex 控制链

1. 私有 `codex app-server --listen stdio://`；stdio 是有界 JSON-RPC 通道例外。禁用 MCP/hook 配置；维护恢复请求只读、approval `never`，任何 server approval/tool request 均拒绝并停止。
2. `initialize` → `thread/resume`，传原 ID、既有 worktree、已解析模型、只读 sandbox；校验返回 thread ID、cwd、model 与 idle 状态。
3. `thread/compact/start`；忽略 `{}` ACK，等待本次 thread 的 compaction item 完成及同一 turn completed。没有有效证据不记成功。
4. 关闭并确认子进程退出；新 app-server 以同 ID resume，确认原 thread / cwd / model / provider 相同，且已完成压缩 turn/item 仍在持久历史。
5. 原生维护后再启动原有业务命令及业务权限。维护进程不调用 `turn/start` 生成解释性摘要，不进入 `AutonomousRunner.evaluateResponse()`。

维护不继承 `danger-full-access`。只读 sandbox 不能单独证明 MCP、动态工具和 hooks 都被关闭；如果当前 Codex 构建不能验证这些执行入口确已关闭，则 runtime 仍为 `needs_validation`，不得自动维护。维护计量只读取 `thread/tokenUsage/updated` 中匹配压缩 turn 的 `last`，绝不使用 `total` 累计值。

## 6. 恢复资料

维护前先落盘不超过 12 KiB UTF-8 的确定性资料：thread / role / Agent / model / 原生 ID；初始目标与截断提示；硬约束；固定 worktree、base/head/approved SHA；本轮完整返修意见或 reviewer 必须核验的开发报告；未完成错误/中断；分页回溯入口。超限优先缩减历史摘录，不静默截断当前指令或硬约束，无法装入则要求从账本分页读取。

压缩成功、结果未知但同 session 仍健康时，恢复资料版本保持待补入，直到一次携带该版本的业务 turn 成功完成。下一条业务 prompt 追加这份资料，原 `round.prompt` 与日志证据不改写。暂停、quota 失败或宿主重启不能清除待补入状态；重复补入同版本不重复计量。

## 7. 账本与计量

- operation 用稳定 ID 和递增 revision，字段至少含角色、Agent、session、实际模型、触发原因、阶段、关联业务 round / 原生 turn、错误/中断、恢复资料与日志路径。`prepared` 和恢复资料必须先写入；旧快照不能以低 revision 把终态覆盖成 `running`，session cursor 不能覆盖改派后的新绑定。
- Codex compaction 的 operation 状态不含第二份 usage。`round.tokenUsage.components[]` 以 attempt/operation ID 去重，区分 `business/native_compaction`、role、agent、实际 model 和归一化 usage。旧轮没有 components 时只迁移一次为 legacy business component；根级数字是组件兼容视图，线程汇总按组件读时派生，byAgent/byModel 按组件角色与模型分组。没有原生 usage 数据标为 unavailable/partial，UI 显示未知；不按字符数或压缩前后差值估成本。
- 压缩失败、暂停、损坏或无法安全恢复时保留原输出、审批 SHA 和 worktree。健康但不确定时同 ID 校验后可继续业务并带 warning；损坏时暂停任务，不能新建会话或丢弃已完成的开发结果。

## 8. 日志、分页、归档和删除

维护日志与恢复资料放在目标/检视 Agent 各自的 `worker_log/`、`reviewer_log/`；operation ID 作为文件名部分。`get_round_raw_log(source="native_compaction", operationId=...)` 按默认 20 KB、上限 200 KB 分页，并只读取相应 Agent 日志根目录下的文件。

归档前确认没有活跃维护；tar.gz 与 manifest 明确收录维护日志及恢复资料。归档包写成功后清活动散落文件与 operation rounds/cursor/session IDs，`finalSummary` 保留维护次数、告警及 token 分组。删除先停进程、释放租约、移除账本，再清本任务维护文件及归档；legacy caller 会话永不删除。

## 9. UI、配置和用户入口

任务详情分别展示 developer/reviewer 的能力、最近 operation、原因、阶段、上下文占用（或未知）、warning 和日志页读入口；账本胶囊与原生会话压缩分开展示。VS Code 命令“压缩此任务原生会话”允许选择 developer 或 captured dedicated reviewer；异步完成后刷新 UI。运行 session 不得抢锁，手动维护不自动开始业务轮，并必须保持任务原有暂停/状态。legacy caller/requester 没有此命令入口。

本设计不新增 MCP 压缩工具、不新增守护进程、不改变 Agent 模型默认值、不选择摘要模型。模式设置、能力状态和单任务 UI 是 Bridge 控制面。

## 10. 账本 v1 的版本边界

`CONTEXT_COMPACTION_DESIGN.md` v1 的“账本卫生、不碰原生会话”只描述 v1 的 `round.output` 归档与卡片压缩，不是对新增原生维护的禁令。v1 全部保证继续有效：最新业务轮与审阅证据原样保留；胶囊不驱动控制流或回填 prompt；最佳努力归档；不得改写原生会话来实现 v1。原生压缩是独立、显式版本化的 v2 行为，只走已认证原生控制面并保持 ID。

相关铁律精确调整：第 7/10 条的无人值守全权限参数仍仅用于业务开发；维护为拒绝交互、最小权限及有界 stdio RPC 例外。第 11/12/17/22 条补充同 thread、session、worktree 与角色；进程退出前持有 session 写租约。第 15 条只对业务保持最终文本双条件，维护按结构化原生证据判定。第 18 条模型路由不变。第 20 条 v1 的账本约束保留，新增独立原生层。第 21 条轮级组件归一计量。第 23 条维护资料随任务归档/删除，legacy 会话不清理。

## 11. 验收记录

Codex 0.164.0-alpha.11.1、`openai` provider、已配置的 `gpt-6-luna` 在 `%TEMP%/agent-mesh-native-session-e2e-79d4c84ef1bc41cdbff2744d0510a75e` 完成临时 thread 的真实压缩、持久历史检查及同 ID `codex exec resume`。脱敏结构化证据为目录内 `native-compaction-evidence.json` 与 `persisted-history.json`；未复制用户 auth/config。由于一次版本/路由的临时测试不能认证其他运行环境，发布默认仍为 `off`。工具/hook 屏蔽、不同模型/provider、并发宿主和完整 Bridge 自动轮次闭环仍须独立验收。

### 当前交付代码状态（2026-11-06）

已落地：Codex 私有 stdio compaction 客户端及同 ID 持久历史复查；Codex UUID/rollout 归一化 session 写租约原语；paused task 的显式手动维护入口；维护操作记录、确定性恢复文件、维护日志、按 operation 去重的轮级计量组件；bridge policy 镜像、详情状态、维护日志分页读取与归档收集；其他 Agent 能力显示为 `unsupported`，Codex 显示 `needs_validation`，自动模式默认 off。

本次交付尚未实现为完整闭环：普通 developer 的三条执行链尚未统一持有 session 租约，也未在维护后把恢复资料注入下一业务 prompt 并在成功落账后标记 delivered；dedicated reviewer 唤醒尚未增加异步 prepare；自动阈值维护未接入；宿主崩溃后的 `running/unknown` 对账恢复、暂停/删除中止与状态复查、维护文件删除清理、归档活动检查及跨进程 stale 合并专测仍缺。虽然真实 Codex 临时 session 验证已通过，但执行工具/hooks 的完整隔离还未证明，因此不得把该 adapter 标为 certified，也不得打开自动维护。此状态不满足本设计的完整验收标准。

## 12. 第 2 轮实现状态（2026-11-06）

本节更新第 1 轮状态；前述批准设计与硬约束继续有效。当前代码已将三条 developer 执行路径汇入 `AutonomousRunner.execute()` 的共享准备阶段：恢复同 session 写租约、读取待交付恢复资料、注入当前业务 prompt（不改 `round.prompt`）、成功后才确认恢复版本 delivered 并去重累计业务 turn。首轮捕获 session ID 时先绑定现存子进程 PID，再发布账本绑定。dedicated reviewer wake 在 launch 前 await prepare，并重读任务状态、session/model/worktree、取消信号和角色 token；session 租约保持到 reviewer 子进程 `close`。

已修复旧 Claude 成本重复聚合；旧轮 usage 迁移与维护费用使用同一 `legacy-business-round-N` component ID，component 合并保护较高 usage；游标只对当前账本 session 绑定有效，并按 revision 合并。stale owner 回收通过 reaper gate 串行化；真实多进程 stale-owner 竞争测试只允许一个新 owner。恢复后的 `prepared/running` operation 会落为 `unknown` 并保留恢复资料，不重放压缩。归档前获取 bound developer/dedicated-reviewer session 租约，活动写入时拒绝归档；既有分页、归档和任务前缀清理规则覆盖维护日志及恢复资料。

Codex stdio 客户端共享总活跃时间预算、取消与 HostSuspensionGuard；stderr 被消费、输入行与事件缓冲有界；终止后未确认 PID 退出时保留租约，待实际退出才释放。压缩 ACK 不作成功；仍要求本次 thread 的 contextCompaction item、对应 completed turn、关闭后同 ID 持久历史复查。

仍未达到完整验收：Codex 虽有临时测试会话的真实压缩/持久化/同 ID `exec resume` 证据（`%TEMP%/agent-mesh-native-session-e2e-79d4c84ef1bc41cdbff2744d0510a75e/native-compaction-evidence.json`、`persisted-history.json`），但当前环境尚未证明 MCP/hooks/动态工具隔离、外部 CLI 并发写安全及全 Bridge 生命周期端到端；故 Codex 仍为 `needs_validation`，自动维护默认 `off`，其他 Agent 为 `unsupported`，不得声称认证。原生自动压缩事件识别/去重、unknown 操作的独立只读可恢复性探测与不可恢复时自动暂停、专门 reviewer/三入口/取消竞态集成测试、reaper gate 自身崩溃恢复，仍需后续完成。启用 `auto` 而能力未认证时会明确跳过并告警，不发压缩 RPC。

### 第 2 轮后续安全收口

reviewer bootstrap 也显式标记为 `nativeSessionRole: reviewer` 并共用 runner session 租约；bootstrap 首次捕获时先租约绑定再发布 reviewer/caller session。`prepared/running` 崩溃恢复会记为 `unknown`，因为当前没有通过认证的只读 session 恢复探针，prepare 会保留恢复资料并将任务暂停，不重放压缩或继续业务。

仍须后续补齐：原生自动压缩事件识别及去重；Codex 同 ID 只读可恢复性探针；dedicated reviewer wake 与开发者三入口的专用集成竞态测试；CallerWakeQueue 与 session 写入租约的单一准入整合；stale reaper gate 自身崩溃后的安全恢复。以上缺口不改变 `needs_validation` / 默认 `off` 发布状态。
