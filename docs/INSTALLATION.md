# 安装与打包

本文说明如何从源码构建 Agent Mesh Bridge 的 VS Code 扩展包（`.vsix`）、安装到本机 VS Code，并完成基础验证。

## 环境要求

### 构建与打包

- Git 工作副本，位于仓库根目录。
- Node.js 20 或更高版本，以及随 Node.js 提供的 npm。
- 网络访问，用于首次安装 `package-lock.json` 中锁定的 npm 开发依赖。
- Extension Host 冒烟测试首次运行还会下载 VS Code 测试实例；需要访问 VS Code 下载服务。
- VS Code 命令行 `code` 只在安装和验证时需要；打包本身不要求 VS Code 正在运行。

本项目将 TypeScript 编译为 ES2022/CommonJS。`@vscode/vsce`、TypeScript 和测试/代码检查工具都是开发依赖；扩展没有单独声明 npm 运行时依赖。`npm run package` 使用本地安装的 vsce，并以 `--no-dependencies` 打包。

### 安装后运行

- VS Code 版本需满足扩展清单中的 `^1.101.0`（即 1.101.0 及以上，且小于 2.0.0）。
- Antigravity、Codex、Claude Code、DeepSeek Harness 是可选的外部 Agent CLI。只使用其中某个 Agent 时，安装并配置该 Agent 即可；桥接会探测可执行文件，也可通过 `agentMeshBridge.*Path` 设置显式路径。
- 需要连接 Agent 时，还需完成对应 CLI 的认证和模型/provider 配置。DSH 的 provider 与凭据要求见 [AGENTS.md 的 DSH 接入说明](../AGENTS.md#deepseek-harness-dsh-接入要点)。

构建机的 Node.js/npm 与运行扩展时 VS Code 内置的 Node.js 是两回事：安装 VSIX 的用户无需另装 Node.js 才能启动扩展；外部 Agent CLI 按各自产品要求安装。

## 从源码构建

在仓库根目录执行：

```bash
# 使用锁文件安装可复现的开发依赖
npm ci

# 编译 TypeScript，同时编译测试文件到 out/test
npm run compile

# 运行 Node.js 内置测试套件
npm test

# 只构建 VS Code 扩展包
npm run package
```

`npm run package` 会调用 `@vscode/vsce package`。VS Code 扩展打包钩子 `vscode:prepublish` 会再次运行 `npm run compile`，因此单独打包也会先刷新编译产物。正常情况下会在仓库根目录生成 `agent-mesh-bridge-<package.json 中的 version>.vsix`。

打包内容由 `.vscodeignore` 控制：包含运行所需的扩展清单、`out` 下白名单 JavaScript、`bin` 和资源；排除源码、测试输出、源码映射、文档及开发配置。发布或分发时应使用本次命令生成的 VSIX，不要仅凭目录中遗留的旧版本包判断其对应当前源码。

## 安装到 VS Code

先确认 `npm run package` 成功。PowerShell 可从 `package.json` 读取版本号并安装刚生成的包：

```powershell
$version = (Get-Content package.json -Raw | ConvertFrom-Json).version
code --install-extension "./agent-mesh-bridge-$version.vsix" --force
```

`--force` 用于覆盖已安装的同名扩展。使用 VS Code Insiders 时将 `code` 替换为 `code-insiders`。也可以在 VS Code 的扩展视图中选择“从 VSIX 安装…”，再选取生成的文件。

安装完成后执行“Developer: Reload Window”重新加载窗口。扩展 ID 为 `agent-mesh.agent-mesh-bridge`。

## 安装验证

1. 确认扩展已注册：

   ```bash
   code --list-extensions --show-versions
   ```

   输出中应包含 `agent-mesh.agent-mesh-bridge@<版本>`。

2. 重新加载 VS Code 后，确认活动栏出现 **Agent Mesh Bridge**，并能打开 Agent Nodes 和任务视图。
3. 打开命令面板运行 **Agent Bridge: Refresh Agent Status**。若预期使用的 Agent 已安装且可认证，应能看到对应 Agent 状态；未安装的 Agent 不影响扩展本体加载。
4. 需要接收/派发任务时，运行 **Agent Bridge: Sync MCP Configuration to Agents**，再检查对应 Agent 是否已加载桥接 MCP server。

## 失败排查

- `npm ci` 失败：检查 Node.js/npm 版本、网络及 `package-lock.json` 是否与分支一致。
- 编译失败：先运行 `npm run compile` 并处理 TypeScript 错误；打包钩子也会因编译失败而终止。
- VSIX 中找不到扩展入口：检查 `out/extension.js` 是否生成，以及 `.vscodeignore` 是否仍保留 `out/extension.js` 和所需 `out` 子目录。
- CLI 安装后看不到扩展：确认安装命令使用了目标 VS Code 配置对应的 CLI（`code` 或 `code-insiders`），检查扩展 ID/版本，然后重载窗口。
- 扩展已加载但 Agent 不可用：检查该 Agent CLI 的安装、认证和路径设置；这与 VSIX 安装成功是两个独立环节。

## 当前自动化边界

仓库提供编译、Node.js 单测、VSIX 打包和本机可运行的 Extension Host 冒烟测试，但目前没有 CI 工作流自动执行这些步骤。冒烟测试也不覆盖真实 Agent CLI 认证、任务派发或 MCP 配置同步。发版前应至少完成上面的构建、测试、打包和 Extension Host 验证。

### Extension Host 冒烟测试

运行 `npm run smoke:extension-host`。该命令会先重新编译并打包当前版本的 VSIX，将它安装到临时扩展目录，然后通过 `@vscode/test-electron` 启动 VS Code Extension Host。测试确认被加载的是临时目录里的打包扩展、清单保留 `onStartupFinished` 激活事件、扩展可在 Host 中激活、版本与仓库清单一致，并且关键视图和命令已注册。它不派发任务或验证真实 Agent/MCP 交互，也不要求 Agent 凭据。

测试会在仓库 `.vscode-test/` 下使用临时 VS Code 用户数据、扩展目录和桥接数据目录，并在临时设置中关闭 `agentMeshBridge.autoSyncMcp`，避免更改开发者的 VS Code 配置或外部 Agent MCP 配置。VS Code 测试实例缓存在同一目录；该目录已被 Git 和 VSIX 忽略。首次运行需要下载 VS Code，之后可复用缓存。可通过 `VSCODE_TEST_VERSION` 指定测试版本，默认使用 stable；版本应满足 `package.json` 中的 `engines.vscode`。

这个命令和 `npm test` 是两条独立验证：前者验证 VSIX 在真实 Extension Host 中能加载，后者运行不启动 VS Code 的 Node.js 测试。Microsoft 的 [Extension Testing 指南](https://code.visualstudio.com/api/working-with-extensions/testing-extension)说明了 Extension Host 测试机制；本项目用自定义 runner 先安装 VSIX，再检查已安装扩展，避免只测开发源码目录。
