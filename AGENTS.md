# 🌉 Agent Mesh Bridge - 架构与开发指南 (AGENTS.md)

本指南为在 `D:\agent_bridge` 项目中协作开发的各 AI Agent（Google Antigravity、OpenAI Codex、Anthropic Claude Code）提供系统设计规范、编码准则与质量基线。

---

## 1. 项目定位与核心特性

- **项目名称**：`agent-mesh-bridge` (VSCode 插件)
- **核心目标**：在 VSCode 宿主内无缝打通 **Google Antigravity (`agy`)**、**OpenAI Codex (`codex`)**、**Anthropic Claude Code (`claude`)**，构建对等多 Agent 协作网格。
- **七大核心设计铁律**：
  1. **默认选路原则**：无显式指定 target 时，默认自动选路至 VSCode 当前使用的前台 Agent。
  2. **全向对等互派**：三端 Agent 均可作为发件人向其他 Agent 派发任务。
  3. **接收方原生留痕**：执行必须携带原生会话参数（`--session-id` / `--conversation` / `rollout`），确保在接收方原生历史可查。
  4. **异步非阻塞回调**：发起方下发任务后立即结束当前 Turn（不长轮询等待），任务完成后插件主动唤醒发起方原会话继续。
  5. **同会话多轮返修**：A 对 B 结果不满意时，调用 `request_rework` 在 B 的**同一个原生会话中继续修改**，多轮迭代直至 A 认可。
  6. **1000 条任务账本**：持久化保留最新 1,000 条任务，展示 `A -> B 标题 摘要`，支持查看完整多轮时间轴与代码 Diff。
  7. **全自动无人值守 (`skip-danger`)**：严格配置免交互与权限旁路参数，关闭标准输入，杜绝后台卡死。

详细架构设计见 [DESIGN.md](DESIGN.md)。

---

## 2. 技术栈与运行环境

- **运行宿主**：Visual Studio Code (`^1.90.0`)
- **语言标准**：TypeScript 5.4+ (Node.js 20+)
- **通信协议**：Model Context Protocol (MCP) JSON-RPC 2.0 over Stdio
- **可执行文件依赖探测**：
  - Antigravity: `~/.gemini/bin/agy.exe` 或 `PATH`
  - Claude Code: `npm/claude.cmd` 或 `PATH`
  - OpenAI Codex: `~/.vscode/extensions/openai.chatgpt-*/bin/windows-x86_64/codex.exe` 或 `PATH`
- **本地存储路径**：`~/.agent-mesh-bridge/delegation_threads.json`（上限 1,000 条）

---

## 3. 模块分层与代码结构

```text
src/
├── extension.ts               # VSCode 扩展生命周期、组合根、命令与视图注册
├── bridge/
│   ├── types.ts               # 核心类型: DelegationThread, DelegationRound, AgentId, Status
│   ├── active-agent-resolver.ts# 默认 Agent 探测与选路算法 (宿主 -> 扩展 -> LRU -> 状态栏)
│   ├── async-dispatcher.ts    # 异步非阻塞任务派发器 (生成 threadId, 立即返回)
│   ├── rework-controller.ts   # 同会话多轮返修控制器 (绑定 receiverSessionId)
│   ├── autonomous-runner.ts   # 跨平台 skip-danger 进程执行器 (关闭 stdin, 注入安全绕过参数)
│   ├── callback-manager.ts    # 回调唤醒引擎 (Codex queue / Claude resume / Anti wake)
│   ├── task-store.ts          # 1000 条任务持久化、原子写入与 LRU 淘汰
│   ├── agent-detector.ts      # 本地可执行文件与健康度探测
│   └── mcp-server.ts          # MCP JSON-RPC 协议解析与工具处理
├── config/
│   └── config-sync.ts         # 自动向 ~/.gemini, ~/.claude, ~/.codex 注入 MCP 配置
└── ui/
    ├── task-tree-provider.ts  # 侧边栏 1000 条任务列表与状态徽章渲染
    ├── detail-panel.ts        # 任务详情与多轮时间轴 Webview
    └── status-bar.ts          # 状态栏当前 Agent 切换器
```

---

## 4. 关键接口规范 (MCP Tools)

三端 Agent 均可通过标准 MCP 协议调用以下能力：

| 工具名                    | 关键入参                                             | 语义与行为                                           |
| :------------------------ | :--------------------------------------------------- | :--------------------------------------------------- |
| `dispatch_task`           | `targetAgent`, `title`, `prompt`, `callerSessionId?` | 异步发起新任务，返回 `threadId`，发起方释放当前 Turn |
| `request_rework`          | `threadId`, `feedback`                               | **在同一个原生会话中唤醒接收方返修**，进入第 N+1 轮  |
| `approve_task`            | `threadId`, `remarks?`                               | 验收通过，标记为 `COMPLETED`，归档任务               |
| `list_recent_tasks`       | `limit` (最大1000), `statusFilter?`                  | 查询任务列表（包含 `A -> B 标题 摘要`）              |
| `get_thread_details`      | `threadId`                                           | 查询该任务全部轮次（提示词、输出、Diff、耗时）       |
| `invoke_agent`            | `targetAgent`, `prompt`                              | 同步阻塞调用（仅限轻量即时问询）                     |
| `query_agent_mesh_status` | 无                                                   | 查询三端 Agent 在网格中的连接与版本状态              |

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
