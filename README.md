# 麦麦×dsh 桥（maibot-dsh-bridge）

把 [DeepSeek Harness (dsh)](https://deepseek.com/harness/) 接入麦麦：麦麦用一句话下单，dsh 在服务器上把活干完，过程以**合并转发**回传当前聊天流，避免刷屏。

## 功能

- **命令** `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh 在后台执行。
- **工具** `dsh_run`：麦麦（Planner）在聊天中判断「这活该外包」时自主调用（需指定 `timeout_seconds`）。

任务异步执行，命令立即回执，跑完自动把结果推回群里。外部只留一句状态 + 一张转发卡片，卡片里按「工具调用 / 输出 / 最终」分节点呈现。

## 依赖

- 服务器安装 **Node.js 22+** 与 **dsh**（`npm i -g @deepseek-ai/dsh`）。
- 需要 **`DEEPSEEK_API_KEY`**（dsh 不产算力）。

## 安装

把本仓库目录放进麦麦的 `plugins/` 下（目录名建议 `maibot-dsh-bridge`），重载麦麦。

## 配置流程（三步）

1. **装引擎**：服务器装 Node 22+ 与 dsh。
2. **给密钥**：设置环境变量 `DEEPSEEK_API_KEY`（**推荐**，插件自动读取）；也可填进 `config.toml` 的 `[dsh].api_key`（不推荐，勿提交）。
3. **启用**：把 `config.toml`（由 `config.toml.example` 复制）里 `[plugin].enabled` 设为 `true`，重载麦麦。

可选：在 `[permission].allowed_users` 填入允许使用 `/dsh` 的用户 ID（逗号分隔）。**留空表示拒绝所有人**（fail-closed）；若想开放，需显式列出。`dsh_run` 工具由麦麦的 Planner 决定调用。

> 插件设计上尽量把配置压进默认值：装好引擎、给上 key、启用即用，其余参数（超时、心跳、转发节点数等）都有合理默认。

## 主要配置项

| 项 | 说明 |
|---|---|
| `[dsh].command` | dsh 可执行命令，如 `dsh` 或 `C:\nodejs\dsh.cmd` |
| `[dsh].timeout_default/min/max` | 默认 / 允许的最小、最大超时（秒） |
| `[dsh].heartbeat_seconds` | 无进展心跳：三信号（输出/进程CPU/子进程）全静超此秒数判卡死 |
| `[dsh].display_mode` | `forward`（合并转发）或 `text` |
| `[permission].allowed_users` | 有权使用 `/dsh` 的用户，留空=拒绝所有人 |

## 安全边界

- 每个聊天流一个独立工作区（`ctx.paths.data_dir/workspace/<stream>/`），群间不串。
- 强制超时 + 分层终止（先温和，宽限后 `taskkill /T` 杀整棵进程树）。
- 任务白名单前缀（`[dsh].allowed_prefixes`，留空不限制）。
- API Key 优先走环境变量，避免明文入库。

## 许可

MIT
