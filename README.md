# 麦麦×dsh 桥（maibot-dsh-bridge）

把 [DeepSeek Harness (dsh)](https://deepseek.com/harness/) 接入麦麦：麦麦用一句话下单，dsh 在服务器上把活干完——过程以**合并转发**回传，结果落成文件、回灌麦麦上下文，产出的文件**打包发回群**，还能**定时自排班**。

## 功能

- **命令** `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh 在后台执行，命令立即回执。
- **工具** `dsh_run`：麦麦（Planner）在聊天中判断「这活该外包」时自主调用。
- **工具** `schedule_task` / `schedule_list` / `schedule_update` / `schedule_cancel`：让麦麦建 / 看 / 改 / 删**定时任务**。
- **交付物**：dsh 用 `present` 声明的文件，桥自动打包发回当前群（单文件直发、多文件 zip）。

## 两轮协议：时长由 dsh 自己定

派活方（麦麦）给的秒数只作**参考建议**，真正决定「做什么、花多久」的是 dsh：

1. **规划轮**：桥先把任务交给 dsh，让它只做规划——解析意图、给出操作计划，并自报一个「最大秒数」（要求输出 JSON：`{"plan": "...", "max_seconds": N}`）。发起方的建议值会写进这一轮提示，**仅供它参考**。
2. **执行轮**：桥拿到 dsh 自报的时长，用它作为超时发起真正的执行。
3. 自报时长会被夹在 `[dsh.timeout_min, dsh.timeout_max]` 之间；规划轮本身用较短超时（最多 60s），失败则回退发起方建议值。

这样派活方定「愿望」、干活方定「现实」，既避免拍一个过紧的死超时，也逼 dsh「先想清楚怎么做」。

## 结果回传（四段）

1. **过程**：合并转发卡片，按「工具调用 / 输出 / 最终」分节点，连续输出合并、工具节点精简为「工具名 + 一行参数 + 耗时」。
2. **结果文件**：完成后把最终文本写成 `result-<时间>.md`，经 **NapCat** 上传到当前群 / 私聊。
3. **交付物**：dsh 通过 `present` 声明的文件，桥收集后打包（单文件直发、多文件 zip），一并发回群，并回一句「📦 交付物（N 个）：…」。
4. **回灌**：把结果摘要通过 `maisaka.context.append` 注入麦麦上下文（长度受 `context_inject_chars` 限制），让它「知道」dsh 查回了什么。

> ⚠️ **适配器边界**：结果文件 / 交付物上传依赖 **NapCat** 的文件接口（`adapter.napcat.file.upload_group_file` / `upload_private_file`）。**非 NapCat 适配器**（如 SnowLuma）会自动回退为转发卡片，不发文件。

## 定时任务（桥内置调度）

桥自带一个轻量调度器（轮询 10s），任务持久化在插件 data 目录的 `scheduled_tasks.json`，重启自动载入。

- **麦麦侧**：`schedule_task` 建任务，四种时间规则任选其一——`after_seconds`（N 秒后一次）/ `every_seconds`（每 N 秒重复）/ `at`（ISO 时刻）/ `cron`（5 字段，如 `0 9 * * 1-5`）。
- **dsh 侧自建**：dsh 在它的工作目录写 `schedule-requests/<名>.json`（`{"title","prompt",时间规则}`），桥在任务收尾时扫描并建任务，随后移入 `.done/`（幂等）。
  - 该约定通过 dsh 的全局 `AGENTS.md`（`$DSH_HOME/AGENTS.md`）告知模型；`schedule-requests` 目录**不计入交付物**。
- 到点后，桥把任务的 prompt 交给 dsh 执行，**走同一条链**回群；触发时若正忙会顺延 60s，避免与手动任务打架。

## 依赖

- 服务器安装 **Node.js 22+** 与 **dsh**（`npm i -g @deepseek-ai/dsh`）。
- 需要 **`DEEPSEEK_API_KEY`**（dsh 不产算力）。
- 若要让麦麦产出的插件自动上线，需配合 [插件安装审批器](https://github.com/ji-or-ji/maibot-plugin-installer)。

## 安装

把本仓库目录放进麦麦的 `plugins/` 下（目录名建议 `maibot-dsh-bridge`），重载麦麦。

## 配置流程（三步）

1. **装引擎**：服务器装 Node 22+ 与 dsh。
2. **给密钥**：设置环境变量 `DEEPSEEK_API_KEY`（**推荐**，插件自动读取）；也可填进 `config.toml` 的 `[dsh].api_key`（不推荐，勿提交）。
3. **启用**：把 `config.toml.example` 复制为 `config.toml`，将 `[plugin].enabled` 设为 `true`，重载麦麦。

可选：在 `[permission].allowed_users` 填入允许使用 `/dsh` 的用户 ID（逗号分隔）。**留空表示拒绝所有人**（fail-closed）；若想开放，需显式列出。`dsh_run` 由麦麦的 Planner 决定调用。

## 配套 dsh 插件推荐

dsh 的能力按「plugin bundle」叠进 profile（改 `$DSH_HOME/profiles/<名>/cordis.patch.yml`，用 `- insert:` 包一层；`$DSH_HOME/profiles/<名>/package.json` 的 `dsh.profile.bundles` 也可挂 bundle 型插件）。以下是本桥实测推荐的几项，**标注了是否需要额外适配**：

| 插件 | 作用 | 是否需适配 |
|---|---|---|
| `@deepseek-ai/dsh-tool-present` | 声明交付文件，让产出能交出来 | 装上即用 |
| `@deepseek-ai/dsh-workspace-changes` | 记录每轮工作区文件变更 | 装上即用 |
| `@deepseek-ai/dsh-skill-office` | Word / PPT / Excel 读写 + 结构检查 | **需**：指定 Python 解释器（3.9+）并装 `python-docx` `python-pptx` `openpyxl` |
| `@deepseek-ai/dsh-mcp-client` | 连外部 MCP 服务器、注册其工具 | **需**：为每台服务器填一条配置（`serverName` + `transport` + `url`/`headers`） |

> 注：官方 `@deepseek-ai/dsh-experimental-schedule-bundle` 的定时能力**要求 Web Session controller，headless profile 不适用**——本桥用内置调度器替代（见「定时任务」）。

**office 的 Python 从哪里指**：office skill 会遵循「用户或 `AGENTS.md` 明确指定的环境」。所以在 `$DSH_HOME/AGENTS.md` 里写明用哪个 Python 即可，**无需官方随包 payload**：

```markdown
## Office 文档工作
用 Python：`<你的 Python 3.9+ 绝对路径>`（已装 python-docx / python-pptx / openpyxl）。
```

## 主要配置项

| 项 | 说明 |
|---|---|
| `[dsh].command` | dsh 可执行命令，如 `dsh` 或 `C:\nodejs\dsh.cmd` |
| `[dsh].timeout_default` | 发起方未给秒数时的默认**建议值**（秒） |
| `[dsh].timeout_min/max` | dsh 自报时长的**夹取范围**（秒），默认 60~1800 |
| `[dsh].heartbeat_seconds` | 无进展心跳：三信号（输出/进程CPU/子进程）全静超此秒数判卡死 |
| `[dsh].context_inject_chars` | 回灌进麦麦上下文的结果摘要上限（默认 600，越小越省上下文） |
| `[dsh].deliver_max_files` | 单次任务最多打包发送的交付物数量（默认 8） |
| `[dsh].display_mode` | `forward`（合并转发）或 `text` |
| `[permission].allowed_users` | 有权使用 `/dsh` 的用户，留空=拒绝所有人 |

## 安全边界

- 每个聊天流一个独立工作区（`ctx.paths.data_dir/workspace/<stream>/`），群间不串。
- dsh 自报时长被夹在上限内 + **无进展心跳** + **分层终止**（先温和，宽限后 `taskkill /T` 杀整棵进程树）。
- 任务白名单前缀（`[dsh].allowed_prefixes`，留空不限制）。
- API Key 优先走环境变量，避免明文入库。
- 若给 dsh 挂了高权限 MCP（如可执行命令的服务器），等于放宽了边界，请自行评估。

## 许可

MIT
