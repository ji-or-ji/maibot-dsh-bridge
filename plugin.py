"""麦麦 × DeepSeek Harness (dsh) 桥插件（带任务守卫 + 精简转发回传）。

作用：把 DeepSeek Harness (dsh) 接入麦麦。
  - 命令 `/dsh [--timeout 秒] <任务>`：把任务外包给 dsh，结果回传当前聊天流。
  - 工具 `dsh_run`：让麦麦（Planner）在聊天中自主把重活交给 dsh（自主下单）。

执行模型（异步 + 守卫）：
  - 异步：命令立即回执，dsh 在后台跑，完成后主动把结果推回群里（绕开宿主 60s RPC 超时）。
  - 单并发：同一时刻只允许一个 dsh 任务。
  - 强制超时：任务必须带超时，桥校验在 [timeout_min, timeout_max] 内。
  - 进展判据（三信号）：输出 / 进程 CPU 活跃 / 子进程存在，任一有变化即续命。
  - 重复检测：输出原地复读超阈值 → 判定卡循环，终止。
  - 总硬上限：即使一直在动，也不超过声明的 timeout。
  - 分层终止：先温和 terminate，宽限期后 taskkill /T /F 杀整棵进程树。

回传形态（所有内容收进转发，外部不刷屏）：
  - dsh 以 --json 输出结构化事件，桥逐事件解析。
  - 外部：只发一句极简状态（完成 / 超时原因）。
  - 转发：一条合并转发卡片，节点精简——
      * 工具调用 = 一行「工具名 + 关键参数」（去掉 JSON 壳）
      * 连续文本输出合并为一段
      * 工具报错才保留一条
      * 最终答案一个节点
  - 这样群里只见「一句状态 + 一张卡片」，永不刷屏。
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Optional

from maibot_sdk import Command, Field, MaiBotPlugin, PluginConfigBase, Tool
from maibot_sdk.types import ToolParameterInfo, ToolParamType

try:  # 进程脉搏（可选，缺失则退化为“仅输出/子进程”判据）
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None  # type: ignore

_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
_POLL_SECONDS = 5.0


class PluginSectionConfig(PluginConfigBase):
    __ui_label__ = "插件"
    __ui_icon__ = "package"
    __ui_order__ = 0

    enabled: bool = Field(default=True, description="是否启用插件")
    config_version: str = Field(default="0.4.0", description="配置版本")


class DshConfig(PluginConfigBase):
    __ui_label__ = "dsh"
    __ui_icon__ = "terminal"
    __ui_order__ = 1

    command: str = Field(default="dsh", description="dsh 可执行命令（如 dsh，或 C:\\nodejs\\dsh.cmd）")
    extra_args: str = Field(default="--profile headless", description="固定附加参数；桥会自动追加 --json")
    workspace_dir: str = Field(default="", description="dsh 工作目录；留空则用 data/dsh-bridge/workspace")
    timeout_default: int = Field(default=300, description="未指定时使用的默认超时（秒）", ge=10, le=3600)
    timeout_min: int = Field(default=60, description="允许的最小超时（秒）", ge=10, le=3600)
    timeout_max: int = Field(default=1800, description="允许的最大超时（秒）", ge=30, le=7200)
    heartbeat_seconds: int = Field(default=180, description="无进展心跳：三信号全静超过此秒数即判卡死", ge=20, le=1800)
    repeat_threshold: int = Field(default=25, description="重复检测：同一行连续出现达到此次数即判循环", ge=5, le=500)
    grace_seconds: int = Field(default=15, description="温和终止后的宽限等待（秒），超时才强杀进程树", ge=0, le=120)
    max_output_chars: int = Field(default=8000, description="文本回退时最大字符数", ge=100, le=50000)
    max_forward_nodes: int = Field(default=40, description="合并转发最多节点数（超出则折叠中段）", ge=3, le=200)
    node_max_chars: int = Field(default=1500, description="单个转发节点最大字符数", ge=100, le=8000)
    display_mode: str = Field(default="forward", description="回传形态：forward=合并转发 / text=纯文本")
    api_key_env: str = Field(default="DEEPSEEK_API_KEY", description="存放 API Key 的环境变量名")
    api_key: str = Field(default="", description="DeepSeek API Key（明文，留空则用系统环境变量；勿分享本文件）")


class PermissionConfig(PluginConfigBase):
    __ui_label__ = "权限"
    __ui_icon__ = "shield"
    __ui_order__ = 2

    enabled: bool = Field(default=True, description="是否启用用户白名单")
    allowed_users: str = Field(default="", description="允许的用户 ID，逗号分隔；留空且启用则拒绝所有人")


class DshBridgeConfig(PluginConfigBase):
    plugin: PluginSectionConfig = Field(default_factory=PluginSectionConfig)
    dsh: DshConfig = Field(default_factory=DshConfig)
    permission: PermissionConfig = Field(default_factory=PermissionConfig)


class _RunState:
    """一次任务运行的共享状态（读线程与监控循环共用）。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events: list[dict[str, Any]] = []
        self.final_text = ""
        self.last_progress = time.time()
        self.repeat_line: Optional[str] = None
        self.repeat_count = 0
        self.repeat_triggered = False

    def push_raw(self, raw_line: str, repeat_threshold: int) -> None:
        text = raw_line.rstrip("\r\n")
        if not text.strip():
            return
        stripped = text.strip()
        evt: Optional[dict[str, Any]] = None
        if stripped.startswith("{"):
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    evt = obj
            except Exception:  # noqa: BLE001
                evt = None
        with self.lock:
            self.last_progress = time.time()
            if evt is not None:
                evt["_ts"] = time.time()
                self.events.append(evt)
                if str(evt.get("type") or "") == "final":
                    self.final_text = str(evt.get("text") or "")
                probe = str(evt.get("text") or evt.get("tool") or stripped)
            else:
                self.events.append({"type": "raw", "text": text, "_ts": time.time()})
                probe = text
            probe = probe.strip()
            if probe and probe == self.repeat_line:
                self.repeat_count += 1
            else:
                self.repeat_line = probe
                self.repeat_count = 1
            if self.repeat_count >= repeat_threshold:
                self.repeat_triggered = True

    def touch(self) -> None:
        with self.lock:
            self.last_progress = time.time()

    def snapshot(self) -> tuple[float, bool]:
        with self.lock:
            return self.last_progress, self.repeat_triggered

    def events_copy(self) -> list[dict[str, Any]]:
        with self.lock:
            return list(self.events)

    def final_copy(self) -> str:
        with self.lock:
            return self.final_text


class DshBridgePlugin(MaiBotPlugin):
    config_model = DshBridgeConfig

    async def on_load(self) -> None:
        self._running = False
        self._current_pid: Optional[int] = None
        self._cpu_prev: dict[int, float] = {}
        self.ctx.logger.info(
            "麦麦×dsh 桥插件已加载 (command=%s, timeout=%s~%s, display=%s)",
            self.config.dsh.command,
            self.config.dsh.timeout_min,
            self.config.dsh.timeout_max,
            self.config.dsh.display_mode,
        )

    async def on_unload(self) -> None:
        pid = self._current_pid
        if pid:
            await asyncio.to_thread(self._kill_tree, pid)
        self.ctx.logger.info("麦麦×dsh 桥插件已卸载")

    async def on_config_update(self, scope: str, config_data: dict, version: str) -> None:
        del scope, config_data, version

    # ── 内部辅助 ──────────────────────────────────────────────

    @staticmethod
    def _extract_user_id(kwargs: dict[str, Any]) -> str:
        user_id = (
            kwargs.get("sender_id")
            or kwargs.get("user_id")
            or kwargs.get("operator")
            or ""
        )
        if isinstance(user_id, dict):
            user_id = user_id.get("user_id") or user_id.get("id") or ""
        return str(user_id).strip()

    def _is_allowed(self, kwargs: dict[str, Any]) -> bool:
        perm = self.config.permission
        if not perm.enabled:
            return True
        user_id = self._extract_user_id(kwargs)
        allowed = {u.strip() for u in perm.allowed_users.split(",") if u.strip()}
        return bool(user_id and user_id in allowed)

    def _clamp_timeout(self, value: Optional[int]) -> int:
        conf = self.config.dsh
        lo, hi = conf.timeout_min, conf.timeout_max
        if lo > hi:
            lo, hi = hi, lo
        if value is None:
            value = conf.timeout_default
        return max(lo, min(hi, int(value)))

    def _workspace(self, stream_id: str = "") -> str:
        root = self.config.dsh.workspace_dir.strip()
        base = Path(root) if root else (Path(self.ctx.paths.data_dir) / "workspace")
        if stream_id:
            safe = re.sub(r"[^A-Za-z0-9_.-]", "_", stream_id).strip("_") or "default"
            base = base / safe
        base.mkdir(parents=True, exist_ok=True)
        return str(base)

    def _proc_active(self, pid: int) -> bool:
        if psutil is None:
            return True
        try:
            root = psutil.Process(pid)
            procs = [root, *root.children(recursive=True)]
            total = 0.0
            for q in procs:
                try:
                    times = q.cpu_times()
                    total += float(times.user) + float(times.system)
                except Exception:  # noqa: BLE001
                    continue
        except Exception:  # noqa: BLE001
            return False
        prev = self._cpu_prev.get(pid)
        self._cpu_prev[pid] = total
        if prev is None:
            return True
        return total > prev + 0.01

    @staticmethod
    def _kill_tree(pid: int) -> None:
        try:
            subprocess.run(
                ["taskkill", "/T", "/F", "/PID", str(pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=_CREATE_NO_WINDOW,
            )
        except Exception:  # noqa: BLE001
            pass

    def _terminate(self, proc: "subprocess.Popen[bytes]") -> None:
        try:
            proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        deadline = time.time() + self.config.dsh.grace_seconds
        while proc.poll() is None and time.time() < deadline:
            time.sleep(0.5)
        if proc.poll() is None:
            self._kill_tree(proc.pid)

    def _spawn(self, task: str, ws: str, env: dict[str, str]) -> "subprocess.Popen[bytes]":
        conf = self.config.dsh
        args = ["cmd.exe", "/c", conf.command, *conf.extra_args.split(), "--json", "-"]
        proc = subprocess.Popen(
            args,
            cwd=ws,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            creationflags=_CREATE_NO_WINDOW,
        )
        try:
            if proc.stdin:
                proc.stdin.write(task.encode("utf-8"))
                proc.stdin.close()
        except Exception:  # noqa: BLE001
            pass
        return proc

    async def _execute(self, task: str, stream_id: str, timeout: int) -> tuple[Optional[str], _RunState]:
        conf = self.config.dsh
        ws = self._workspace(stream_id)
        env = dict(os.environ)
        api_key = conf.api_key.strip() or os.environ.get(conf.api_key_env, "").strip()
        if api_key:
            env[conf.api_key_env] = api_key

        state = _RunState()
        try:
            proc = await asyncio.to_thread(self._spawn, task, ws, env)
        except Exception as exc:  # noqa: BLE001
            state.events.append({"type": "raw", "text": f"❌ 启动 dsh 失败：{exc}"})
            return "spawn-error", state

        self._current_pid = proc.pid
        self._cpu_prev.pop(proc.pid, None)

        def _reader(stream: Any) -> None:
            if stream is None:
                return
            try:
                for raw in iter(stream.readline, b""):
                    state.push_raw(raw.decode("utf-8", "replace"), conf.repeat_threshold)
            except Exception:  # noqa: BLE001
                pass

        t_out = threading.Thread(target=_reader, args=(proc.stdout,), daemon=True)
        t_err = threading.Thread(target=_reader, args=(proc.stderr,), daemon=True)
        t_out.start()
        t_err.start()

        start = time.time()
        reason: Optional[str] = None
        while proc.poll() is None:
            await asyncio.sleep(_POLL_SECONDS)
            if self._proc_active(proc.pid):
                state.touch()
            now = time.time()
            last_progress, repeat = state.snapshot()
            if now - start > timeout:
                reason = "timeout"
                break
            if now - last_progress > conf.heartbeat_seconds:
                reason = "stalled"
                break
            if repeat:
                reason = "loop"
                break

        if reason is not None:
            await asyncio.to_thread(self._terminate, proc)
        else:
            await asyncio.to_thread(proc.wait)

        t_out.join(timeout=5)
        t_err.join(timeout=5)
        self._current_pid = None
        self._cpu_prev.pop(proc.pid, None)
        return reason, state

    _REASON_MSG = {
        "timeout": "⏱ dsh 任务达到声明的超时时间，已终止。",
        "stalled": "🛑 dsh 无进展（无输出且进程静默）超过心跳阈值，已终止。",
        "loop": "🔁 dsh 输出疑似陷入循环（重复度过高），已终止。",
    }

    @staticmethod
    def _tool_summary(inp: Any) -> str:
        """从工具 input 里挑一行人类可读的关键参数，去掉 JSON 壳。"""
        if inp in (None, ""):
            return ""
        if not isinstance(inp, dict):
            return str(inp).strip()[:120]
        for key in ("command", "file_path", "path", "file", "name", "query", "url", "pattern", "skill"):
            val = inp.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()[:120]
        for k, v in inp.items():
            if isinstance(v, str) and v.strip():
                return f"{k}={v.strip()[:100]}"
        return ""

    # ── 回传：合并转发 ────────────────────────────────────────

    @staticmethod
    def _strip_tags(res: str) -> str:
        """去掉 dsh 结果里的 XML 壳（<path>…</path> / <content>…</content>）。"""
        cleaned = re.sub(r"</?[A-Za-z][^>]*>", "", res)
        return cleaned.strip()

    def _build_forward_nodes(self, state: _RunState) -> list[dict[str, Any]]:
        conf = self.config.dsh
        events = state.events_copy()
        nodes: list[dict[str, Any]] = []
        text_buf: list[str] = []
        seen_texts: set[str] = set()
        pending: dict[str, tuple[str, float]] = {}  # callId -> (tool, start_ts)

        def flush_text() -> None:
            if not text_buf:
                return
            joined = "\n".join(text_buf).strip()
            text_buf.clear()
            if not joined:
                return
            seen_texts.add(joined)
            if len(joined) > conf.node_max_chars:
                joined = joined[: conf.node_max_chars] + "…"
            nodes.append(
                {"user_id": "0", "nickname": "📝 输出", "segments": [{"type": "text", "content": joined}]}
            )

        for evt in events:
            t = str(evt.get("type") or "")
            if t in ("session", "status", "thinking"):
                continue
            if t == "tool_call":
                flush_text()
                tool = str(evt.get("tool") or evt.get("name") or "?")
                cid = str(evt.get("callId") or "")
                if cid:
                    pending[cid] = (tool, float(evt.get("_ts") or time.time()))
                summary = self._tool_summary(evt.get("input"))
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"🔧 {tool}",
                        "segments": [{"type": "text", "content": summary or "(调用)"}],
                    }
                )
            elif t == "tool_result":
                flush_text()
                cid = str(evt.get("callId") or "")
                tool = str(evt.get("tool") or "")
                status = str(evt.get("status") or "")
                dur = ""
                if cid in pending:
                    _tool_name, ts0 = pending.pop(cid)
                    dt = max(0.0, float(evt.get("_ts") or time.time()) - ts0)
                    dur = f" {dt:.1f}s"
                res = self._strip_tags(str(evt.get("result") or evt.get("text") or "")).strip()
                if len(res) > 300:
                    res = res[:300] + "…"
                ok = (not status) or status == "completed"
                icon = "↩️" if ok else "⚠️"
                label = tool or ("结果" if ok else "失败")
                content = res or ("完成" if ok else status or "失败")
                nodes.append(
                    {
                        "user_id": "0",
                        "nickname": f"{icon} {label}{dur}",
                        "segments": [{"type": "text", "content": content}],
                    }
                )
            elif t in ("text", "raw"):
                text_buf.append(str(evt.get("text") or ""))
            elif t == "final":
                flush_text()
                ft = str(evt.get("text") or "").strip()
                if not ft:
                    continue
                # 末尾重复：最终答案若与前面已输出的整段相同则不重复展示
                if ft in seen_texts:
                    continue
                if len(ft) > conf.node_max_chars:
                    ft = ft[: conf.node_max_chars] + "…"
                nodes.append(
                    {"user_id": "0", "nickname": "✅ 最终", "segments": [{"type": "text", "content": ft}]}
                )
        flush_text()

        limit = conf.max_forward_nodes
        if len(nodes) > limit:
            head = max(1, limit - 8)
            tail = limit - head - 1
            folded = len(nodes) - head - tail
            nodes = nodes[:head] + [
                {"user_id": "0", "nickname": "…", "segments": [{"type": "text", "content": f"（中间略去 {folded} 条）"}]}
            ] + nodes[-tail:]
        return nodes

    async def _report(self, state: _RunState, reason: Optional[str], stream_id: str) -> None:
        conf = self.config.dsh
        header = self._REASON_MSG.get(reason or "", "")
        final_text = state.final_copy().strip()
        use_forward = conf.display_mode.strip().lower() != "text"
        nodes = self._build_forward_nodes(state) if use_forward else []

        # 外部只留一句极简状态：所有内容收进转发，避免被判刷屏/重复
        if header:
            receipt = header
        elif nodes:
            receipt = "✅ dsh 任务完成（过程见转发）"
        else:
            receipt = final_text[: conf.max_output_chars] if final_text else "(dsh 无输出)"

        try:
            await self.ctx.send.text(f"dsh：{receipt}", stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 回执发送失败", exc_info=True)

        if not nodes:
            return
        try:
            await self.ctx.send.forward(nodes, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 转发发送失败", exc_info=True)
            # 兜底：转发失败时把最终答案补发出来（仅此一次，避免重复刷屏）
            if final_text:
                try:
                    await self.ctx.send.text(final_text[: conf.max_output_chars], stream_id)
                except Exception:  # noqa: BLE001
                    pass

    def _scan_plugin_dirs(self, stream_id: str = "") -> set[str]:
        """扫描工作目录，返回含 _manifest.json 的插件目录集合。"""
        ws = Path(self._workspace(stream_id))
        found: set[str] = set()
        try:
            for d in ws.iterdir():
                if d.is_dir() and (d / "_manifest.json").is_file():
                    found.add(str(d.resolve()))
        except Exception:  # noqa: BLE001
            pass
        return found

    async def _submit_new_plugins(self, before: set[str], stream_id: str) -> None:
        """将本次任务新增的插件目录提交给安装审批器。"""
        after = self._scan_plugin_dirs(stream_id)
        for path in sorted(after - before):
            try:
                res = await self.ctx.api.call(
                    "ji-or-ji.maibot-plugin-installer.submit",
                    plugin_dir=path,
                    stream_id=stream_id,
                )
                self.ctx.logger.info("已提交待审插件 %s -> %s", path, res)
            except Exception as exc:  # noqa: BLE001
                self.ctx.logger.warning("提交待审插件 %s 失败：%s", path, exc)

    async def _run_and_report(self, task: str, stream_id: str, timeout: int) -> None:
        before = self._scan_plugin_dirs(stream_id)
        try:
            reason, state = await self._execute(task, stream_id, timeout)
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.error("dsh 任务执行异常: %s", exc, exc_info=True)
            state = _RunState()
            state.events.append({"type": "raw", "text": f"❌ 执行异常：{exc}"})
            reason = "error"
        finally:
            self._running = False
        try:
            await self._report(state, reason, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 结果回传失败", exc_info=True)
        try:
            await self._submit_new_plugins(before, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("提交新插件失败", exc_info=True)

    def _reject_busy(self) -> bool:
        return self._running

    # ── 命令：/dsh ────────────────────────────────────────────

    @Command(
        "dsh",
        description="把任务外包给 DeepSeek Harness (dsh)，支持 --timeout 指定超时秒数",
        pattern=r"^/dsh\s+(?P<body>.+)$",
    )
    async def handle_dsh(self, stream_id: str = "", **kwargs: Any):
        if not self.config.plugin.enabled:
            return False, "插件未启用", False
        if not self._is_allowed(kwargs):
            await self.ctx.send.text("你没有权限使用 /dsh", stream_id)
            return False, "无权限", False

        body = ""
        matched = kwargs.get("matched_groups")
        if isinstance(matched, dict):
            body = str(matched.get("body") or "").strip()
        if not body:
            raw = str(kwargs.get("text") or "")
            m = re.match(r"^/dsh\s+(.+)$", raw, re.DOTALL)
            if m:
                body = m.group(1).strip()
        if not body:
            await self.ctx.send.text("用法：/dsh [--timeout 秒] <任务描述>", stream_id)
            return False, "缺少任务", False

        timeout: Optional[int] = None
        mt = re.match(r"^--timeout\s+(\d+)\s+(.+)$", body, re.DOTALL)
        if mt:
            timeout = int(mt.group(1))
            body = mt.group(2).strip()

        if self._reject_busy():
            await self.ctx.send.text("已有 dsh 任务在执行，请等它结束。", stream_id)
            return False, "忙碌", False

        to = self._clamp_timeout(timeout)
        self._running = True
        await self.ctx.send.text(
            f"🛠 已把任务交给 dsh（超时 {to}s）：{body[:80]}", stream_id
        )
        asyncio.create_task(self._run_and_report(body, stream_id, to))
        return True, "已提交", False

    # ── 工具：dsh_run（供 Planner 自主下单） ──────────────────

    @Tool(
        "dsh_run",
        description=(
            "把较重的任务外包给 DeepSeek Harness (dsh) 在服务器上执行（如编写插件、跑脚本、深度调研）。"
            "必须显式指定 timeout_seconds（任务预计需要的秒数）。仅当任务明显超出即时聊天能力时使用。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(
                name="task",
                param_type=ToolParamType.STRING,
                description="要 dsh 完成的任务描述",
                required=True,
            ),
            ToolParameterInfo(
                name="timeout_seconds",
                param_type=ToolParamType.INTEGER,
                description="该任务的超时秒数（必填，桥会校验在允许区间内）",
                required=True,
            ),
            ToolParameterInfo(
                name="stream_id",
                param_type=ToolParamType.STRING,
                description="结果回传的当前聊天流 ID",
                required=True,
            ),
        ],
    )
    async def _tool_dsh_run(self, **kwargs: Any) -> dict[str, Any]:
        task = str(kwargs.get("task") or "").strip()
        stream_id = str(kwargs.get("stream_id") or "").strip()
        raw_timeout = kwargs.get("timeout_seconds")

        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if not task:
            return {"success": False, "error": "缺少 task"}
        if raw_timeout is None:
            return {"success": False, "error": "必须指定 timeout_seconds"}
        if self._reject_busy():
            return {"success": False, "error": "已有 dsh 任务在执行"}

        to = self._clamp_timeout(int(raw_timeout))
        self._running = True
        await self.ctx.send.text(
            f"🛠 已把任务交给 dsh（超时 {to}s）：{task[:80]}", stream_id
        )
        asyncio.create_task(self._run_and_report(task, stream_id, to))
        return {"success": True, "accepted": True, "timeout_seconds": to}


def create_plugin() -> DshBridgePlugin:
    return DshBridgePlugin()
