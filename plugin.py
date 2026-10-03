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
import shutil
import subprocess
import tempfile
import threading
import time
import zipfile
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
    deliver_max_files: int = Field(default=8, description="单次任务最多打包发送的交付物文件数", ge=1, le=50)
    api_key_env: str = Field(default="DEEPSEEK_API_KEY", description="存放 API Key 的环境变量名")
    api_key: str = Field(default="", description="DeepSeek API Key（明文，留空则用系统环境变量；勿分享本文件）")
    context_inject_chars: int = Field(
        default=600,
        description="回灌进麦麦上下文的结果摘要最大字符数（越小越省上下文，超出部分截断）",
        ge=100,
        le=5000,
    )


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
        self._tasks: dict[str, dict[str, Any]] = {}
        self._scheduler_running = True
        self._load_tasks()
        asyncio.create_task(self._scheduler_loop())
        self.ctx.logger.info(
            "麦麦×dsh 桥插件已加载 (command=%s, timeout=%s~%s, display=%s)",
            self.config.dsh.command,
            self.config.dsh.timeout_min,
            self.config.dsh.timeout_max,
            self.config.dsh.display_mode,
        )

    async def on_unload(self) -> None:
        self._scheduler_running = False
        pid = self._current_pid
        if pid:
            await asyncio.to_thread(self._kill_tree, pid)
        self.ctx.logger.info("麦麦×dsh 桥插件已卸载")

    # ── 定时任务（桥内置调度） ────────────────────────────────

    def _tasks_file(self) -> Path:
        return Path(self.ctx.paths.data_dir) / "scheduled_tasks.json"

    def _load_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            if fp.is_file():
                data = json.loads(fp.read_text(encoding="utf-8"))
                for t in data if isinstance(data, list) else []:
                    if isinstance(t, dict) and t.get("id"):
                        self._tasks[str(t["id"])] = t
            self.ctx.logger.info("已载入定时任务 %d 个", len(self._tasks))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("读取定时任务失败", exc_info=True)

    def _save_tasks(self) -> None:
        try:
            fp = self._tasks_file()
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_text(json.dumps(list(self._tasks.values()), ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("保存定时任务失败", exc_info=True)

    @staticmethod
    def _compute_next(trigger: dict[str, Any], base: float) -> Optional[float]:
        """根据触发器推算下次触发时间戳（秒）。返回 None 表示不再触发。"""
        kind = str(trigger.get("kind") or "")
        if kind == "after":
            return base + float(trigger.get("seconds") or 0)
        if kind == "at":
            at = float(trigger.get("timestamp") or 0)
            return at if at > base + 0.5 else None
        if kind == "every":
            every = max(60.0, float(trigger.get("seconds") or 60))
            anchor = float(trigger.get("anchor") or base)
            if anchor > base:
                return anchor
            n = int((base - anchor) // every) + 1
            return anchor + n * every
        if kind == "cron":
            return DshBridgePlugin._cron_next(str(trigger.get("expression") or ""), base)
        return None

    @staticmethod
    def _cron_field_match(field: str, value: int) -> bool:
        """单个 cron 字段匹配（* / a / a-b / */n / a-b/n / 逗号列表）。"""
        for part in str(field).split(","):
            part = part.strip()
            if not part:
                continue
            step = 1
            base = part
            if "/" in part:
                base, _, s = part.partition("/")
                try:
                    step = int(s)
                except ValueError:
                    continue
                if step <= 0:
                    continue
            if base == "*":
                lo, hi = 0, 10 ** 9
            elif "-" in base:
                a, _, b = base.partition("-")
                try:
                    lo, hi = int(a), int(b)
                except ValueError:
                    continue
                if lo > hi:
                    lo, hi = hi, lo
            else:
                try:
                    v = int(base)
                except ValueError:
                    continue
                if step == 1:
                    if value == v:
                        return True
                    continue
                lo, hi = v, 10 ** 9
            if lo <= value <= hi and (value - lo) % step == 0:
                return True
        return False

    @classmethod
    def _cron_next(cls, expr: str, base: float) -> Optional[float]:
        """求 cron（5 字段，minute 精度）下一个匹配时点；限制搜索 366 天。"""
        import datetime as _dt
        parts = (expr or "").split()
        if len(parts) != 5:
            return None
        mi_f, ho_f, dom_f, mo_f, dow_f = parts
        start = _dt.datetime.fromtimestamp(base + 1).replace(second=0, microsecond=0) + _dt.timedelta(minutes=1)
        limit = start + _dt.timedelta(days=366)
        cur = start
        while cur < limit:
            dow = (cur.weekday() + 1) % 7  # cron: 周日=0
            if (
                cls._cron_field_match(mo_f, cur.month)
                and cls._cron_field_match(dom_f, cur.day)
                and cls._cron_field_match(dow_f, dow)
                and cls._cron_field_match(ho_f, cur.hour)
                and cls._cron_field_match(mi_f, cur.minute)
            ):
                return cur.timestamp()
            cur += _dt.timedelta(minutes=1)
        return None

    async def _scheduler_loop(self) -> None:
        """后台调度循环：每 20s 扫描到期任务并触发。"""
        while self._scheduler_running:
            try:
                now = time.time()
                due = [t for t in list(self._tasks.values()) if float(t.get("next_at") or 0) and float(t["next_at"]) <= now]
                for task in due:
                    try:
                        await self._fire_task(task)
                    except Exception:  # noqa: BLE001
                        self.ctx.logger.warning("触发定时任务失败", exc_info=True)
            except Exception:  # noqa: BLE001
                self.ctx.logger.warning("调度循环异常", exc_info=True)
            await asyncio.sleep(10)

    async def _fire_task(self, task: dict[str, Any]) -> None:
        stream_id = str(task.get("stream_id") or "")
        prompt = str(task.get("prompt") or "")
        ctx = task.get("ctx") or {}
        title = str(task.get("title") or "定时任务")
        if self._reject_busy():
            # 正忙：顺延 60s 后再试，避免与手动任务打架
            task["next_at"] = time.time() + 60
            self._save_tasks()
            self.ctx.logger.info("定时任务「%s」因忙碌顺延", title)
            return
        try:
            await self.ctx.send.text(f"⏰ 定时任务「{title}」触发：{prompt[:60]}", stream_id)
        except Exception:  # noqa: BLE001
            pass
        self._running = True
        try:
            await self._run_and_report(prompt, stream_id, int(task.get("timeout") or self.config.dsh.timeout_default), ctx)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("定时任务执行失败", exc_info=True)
        trigger = task.get("trigger") or {}
        kind = str(trigger.get("kind") or "")
        if kind == "every":
            task["next_at"] = self._compute_next(trigger, time.time())
        elif kind == "cron":
            task["next_at"] = self._compute_next(trigger, time.time())
        else:
            self._tasks.pop(str(task.get("id")), None)
        self._save_tasks()

    def _parse_schedule(self, after_seconds: Any, every_seconds: Any, at: Any, cron: Any = None) -> Optional[dict[str, Any]]:
        """把可选参数解析成一个触发器。"""
        now = time.time()
        if after_seconds not in (None, ""):
            return {"kind": "after", "seconds": float(after_seconds), "next_at": now + float(after_seconds)}
        if every_seconds not in (None, ""):
            sec = max(60.0, float(every_seconds))
            return {"kind": "every", "seconds": sec, "anchor": now, "next_at": now + sec}
        if cron not in (None, ""):
            expr = str(cron).strip()
            nxt = self._cron_next(expr, now)
            if nxt is None:
                return None
            return {"kind": "cron", "expression": expr, "next_at": nxt}
        if at not in (None, ""):
            ts = self._parse_iso(str(at))
            if ts is None or ts <= now:
                return None
            return {"kind": "at", "timestamp": ts, "at_text": str(at), "next_at": ts}
        return None

    def _create_task(self, title: str, prompt: str, after: Any, every: Any, at: Any, cron: Any, stream_id: str, ctx: dict[str, Any]) -> Optional[str]:
        """建一个定时任务并持久化；返回任务 id 或 None。"""
        trigger = self._parse_schedule(after, every, at, cron)
        if not trigger:
            return None
        next_at = trigger.pop("next_at")
        task_id = "st_" + time.strftime("%Y%m%d%H%M%S") + "_" + str(int(time.time() * 1000) % 1000)
        self._tasks[task_id] = {
            "id": task_id,
            "title": title,
            "prompt": prompt,
            "trigger": trigger,
            "next_at": next_at,
            "timeout": int(self.config.dsh.timeout_default),
            "stream_id": stream_id,
            "ctx": ctx or {},
            "created_at": time.time(),
        }
        self._save_tasks()
        return task_id

    def _schedule_requests_dir(self, stream_id: str) -> Path:
        return Path(self._workspace(stream_id)) / "schedule-requests"

    async def _process_schedule_requests(self, stream_id: str, ctx: dict[str, Any]) -> None:
        """扫描 dsh 写下的 schedule-requests/*.json，建任务后移到 .done（幂等）。"""
        req_dir = self._schedule_requests_dir(stream_id)
        if not req_dir.is_dir():
            return
        created: list[str] = []
        for fp in sorted(req_dir.glob("*.json")):
            try:
                req = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            if isinstance(req, dict):
                title = str(req.get("title") or "").strip()
                prompt = str(req.get("prompt") or "").strip()
                if title and prompt:
                    tid = self._create_task(
                        title,
                        prompt,
                        req.get("after_seconds"),
                        req.get("every_seconds"),
                        req.get("at"),
                        req.get("cron"),
                        stream_id,
                        ctx or {},
                    )
                    if tid:
                        created.append(f"「{title}」(id={tid})")
            # 无论是否成功都移入 .done，避免重复处理
            try:
                done_dir = req_dir / ".done"
                done_dir.mkdir(parents=True, exist_ok=True)
                fp.replace(done_dir / fp.name)
            except Exception:  # noqa: BLE001
                pass
        if created:
            self.ctx.logger.info("dsh 新建定时任务 %d 个: %s", len(created), "; ".join(created))
            try:
                await self.ctx.send.text("⏰ dsh 新建定时任务：" + "、".join(created), stream_id)
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _parse_iso(text: str) -> Optional[float]:
        """解析 ISO 时间（支持 +08:00 时区）为时间戳。"""
        try:
            s = text.strip().replace("Z", "+00:00")
            m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?\s*([+-]\d{2}:?\d{2})?$", s)
            if not m:
                return None
            y, mo, d, h, mi = (int(m.group(i)) for i in range(1, 6))
            sec = int(m.group(6) or 0)
            import datetime as _dt
            tzs = m.group(7)
            tz = _dt.timezone.utc
            if tzs:
                sign = 1 if tzs[0] == "+" else -1
                tzs2 = tzs[1:].replace(":", "")
                tz = _dt.timezone(sign * _dt.timedelta(hours=int(tzs2[:2]), minutes=int(tzs2[2:4])))
            return _dt.datetime(y, mo, d, h, mi, sec, tzinfo=tz).timestamp()
        except Exception:  # noqa: BLE001
            return None

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
            safe = re.sub(r"[^A-Za-z0-9_-]", "_", stream_id).strip("_") or "default"
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

    @staticmethod
    def _extract_ctx(kwargs: dict[str, Any]) -> dict[str, str]:
        def _g(k: str) -> str:
            v = kwargs.get(k)
            return str(v).strip() if v is not None else ""
        return {"group_id": _g("group_id"), "user_id": _g("user_id"), "platform": _g("platform")}

    def _write_result_file(self, state: _RunState) -> Optional[str]:
        """把最终输出写成 .md 文件，落在插件 data_dir。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return None
        base = Path(self.ctx.paths.data_dir) / "results"
        try:
            base.mkdir(parents=True, exist_ok=True)
            fp = base / f"result-{time.strftime('%Y%m%d-%H%M%S')}.md"
            fp.write_text(final_text, encoding="utf-8")
            return str(fp)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("写结果文件失败", exc_info=True)
            return None

    async def _send_result_file(self, path: str, ctx: dict[str, str]) -> bool:
        """经 NapCat 把结果文件发到流（群/私聊）。**NapCat 专属**；其他适配器自动跳过。"""
        group_id = (ctx or {}).get("group_id", "")
        user_id = (ctx or {}).get("user_id", "")
        name = Path(path).name
        if group_id:
            api = "adapter.napcat.file.upload_group_file"
            params = {"group_id": group_id, "file": path, "name": name}
        elif user_id:
            api = "adapter.napcat.file.upload_private_file"
            params = {"user_id": user_id, "file": path, "name": name}
        else:
            return False
        try:
            if await self.ctx.api.get(api) is None:
                return False  # 非 NapCat 适配器：没有此接口，回退卡片
            await self.ctx.api.call(api, params=params)
            return True
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("发送结果文件失败：%s", exc)
            return False

    async def _notify_bot(self, state: _RunState, stream_id: str) -> None:
        """把结果回灌进麦麦的上下文，让它知道任务已返回。"""
        final_text = state.final_copy().strip()
        if not final_text:
            return
        limit = int(self.config.dsh.context_inject_chars)
        if len(final_text) > limit:
            body = final_text[:limit].rstrip() + "…（完整结果见群内文件/转发）"
        else:
            body = final_text
        try:
            await self.ctx.maisaka.context.append(
                stream_id,
                segments=[{"type": "text", "content": f"【dsh 任务完成】\n{body}"}],
                source_kind="dsh-bridge",
            )
        except Exception as exc:  # noqa: BLE001
            self.ctx.logger.warning("回灌结果给麦麦失败：%s", exc)

    async def _report(self, state: _RunState, reason: Optional[str], stream_id: str, ctx: Optional[dict[str, Any]] = None) -> None:
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

        # 结果写文件并发送（NapCat 专属；其他适配器自动回退）+ 回灌给麦麦
        try:
            file_path = self._write_result_file(state)
            if file_path:
                if await self._send_result_file(file_path, ctx or {}):
                    await self.ctx.send.text(f"📎 结果文件：{Path(file_path).name}", stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("结果文件处理失败", exc_info=True)

        try:
            await self._notify_bot(state, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("回灌结果失败", exc_info=True)

        # 交付物：present 声明的文件 → 打包/发送（NapCat 专属，非 NapCat 自动跳过）
        try:
            await self._send_deliverables(state, stream_id, ctx or {})
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("交付物处理失败", exc_info=True)

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

    def _collect_deliverables(self, state: _RunState, ws: str) -> list[str]:
        """从 dsh 事件流里收集 present 声明的交付文件（绝对路径，仅保留存在的）。"""
        paths: list[str] = []
        seen: set[str] = set()
        for evt in state.events_copy():
            if str(evt.get("type") or "") != "tool_call":
                continue
            tool = str(evt.get("tool") or evt.get("name") or "")
            if tool not in ("present", "present_deliverables"):
                continue
            inp = evt.get("input")
            if not isinstance(inp, dict):
                continue
            files = inp.get("files")
            if not isinstance(files, list):
                continue
            for f in files:
                p = f.get("path") if isinstance(f, dict) else (f if isinstance(f, str) else None)
                if not isinstance(p, str) or not p.strip():
                    continue
                cand = Path(p)
                if not cand.is_absolute():
                    cand = Path(ws) / p
                try:
                    cand_resolved = cand.resolve()
                    rp = str(cand_resolved)
                except Exception:  # noqa: BLE001
                    continue
                # 必须落在本流工作区内（防 ../ 或宿主任意路径被外发）
                try:
                    if not cand_resolved.is_relative_to(Path(ws).resolve()):
                        continue
                except Exception:  # noqa: BLE001
                    continue
                if "schedule-requests" in rp:
                    continue  # 定时任务请求文件不算交付物
                if rp in seen or not cand.is_file():
                    continue
                seen.add(rp)
                paths.append(rp)
        return paths

    async def _send_deliverables(self, state: _RunState, stream_id: str, ctx: dict[str, str]) -> None:
        """把 present 声明的交付物打包发送到流（单文件直发，多文件打 zip）。"""
        ws = self._workspace(stream_id)
        files = self._collect_deliverables(state, ws)
        if not files:
            return
        limit = int(self.config.dsh.deliver_max_files)
        files = files[:limit]
        tmp_dir: Optional[str] = None
        try:
            if len(files) == 1:
                send_list = files
            else:
                tmp_dir = tempfile.mkdtemp(prefix="dsh-deliver-")
                zip_path = os.path.join(tmp_dir, f"deliver-{time.strftime('%Y%m%d-%H%M%S')}.zip")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
                    for f in files:
                        z.write(f, arcname=Path(f).name)
                send_list = [zip_path]
            sent = 0
            for f in send_list:
                if await self._send_result_file(f, ctx):
                    sent += 1
            if sent:
                names = "、".join(Path(f).name for f in files)
                await self.ctx.send.text(f"📦 交付物（{len(files)} 个）：{names}", stream_id)
        finally:
            if tmp_dir:
                shutil.rmtree(tmp_dir, ignore_errors=True)

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

    def _parse_plan(self, text: str) -> tuple[str, Optional[int]]:
        """从规划轮输出里提取计划与 dsh 自定的时长。"""
        plan = ""
        seconds: Optional[int] = None
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
                if isinstance(obj, dict):
                    plan = str(obj.get("plan") or "").strip()
                    raw = obj.get("max_seconds") or obj.get("seconds") or obj.get("timeout_seconds")
                    if isinstance(raw, (int, float)):
                        seconds = int(raw)
            except Exception:  # noqa: BLE001
                pass
        if not plan:
            plan = text[:500].strip()
        return plan, seconds

    async def _plan_task(self, task: str, stream_id: str, suggested: int) -> tuple[str, int]:
        """第一轮：让 dsh 解析意图、给出操作计划与自定最长时长（发起方的时长仅作参考）。"""
        prompt = (
            "你是任务规划器，只做规划、不执行。先分析下面这个任务要做什么，"
            "给出简洁的操作计划，并估计完成它所需的最大秒数。只输出一个 JSON 对象，"
            '格式：{"plan": "操作计划", "max_seconds": 整数}，不要输出其它内容。'
            f"\n（发起方建议约 {suggested} 秒，你可自行判断合理时长）\n\n任务：{task}"
        )
        plan_timeout = max(self.config.dsh.timeout_min, 60)
        try:
            _reason, state = await self._execute(prompt, stream_id, plan_timeout)
            plan, secs = self._parse_plan(state.final_copy().strip())
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("规划轮失败，回退发起方时长", exc_info=True)
            plan, secs = "", None
        if not secs:
            secs = suggested
        secs = self._clamp_timeout(secs)
        self.ctx.logger.info("dsh 规划轮：自定时长=%ss，计划=%.120s", secs, plan)
        return plan, secs

    async def _run_and_report(self, task: str, stream_id: str, timeout: int, ctx: Optional[dict[str, Any]] = None) -> None:
        before = self._scan_plugin_dirs(stream_id)
        # 第一轮：dsh 自评，定操作计划与最长时长（发起方 timeout 仅作参考）
        plan, timeout = await self._plan_task(task, stream_id, timeout)
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
            await self._report(state, reason, stream_id, ctx)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("dsh 结果回传失败", exc_info=True)
        try:
            await self._submit_new_plugins(before, stream_id)
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("提交新插件失败", exc_info=True)
        # dsh 若在 schedule-requests/ 写下了定时任务请求，建任务（幂等）
        try:
            await self._process_schedule_requests(stream_id, ctx or self._extract_ctx({}))
        except Exception:  # noqa: BLE001
            self.ctx.logger.warning("处理定时任务请求失败", exc_info=True)

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
        asyncio.create_task(self._run_and_report(body, stream_id, to, self._extract_ctx(kwargs)))
        return True, "已提交", False

    # ── 工具：dsh_run（供 Planner 自主下单） ──────────────────

    @Tool(
        "dsh_run",
        description=(
            "把较重任务外包给 dsh 在服务器执行（写插件 / 跑脚本 / 深度调研）。需给 timeout_seconds（预计秒数，仅作参考）。"
            "仅当任务明显超出即时聊天能力时使用。"
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
                description="预计所需秒数（参考值）",
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
        if not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 dsh_run"}
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
        asyncio.create_task(self._run_and_report(task, stream_id, to, self._extract_ctx(kwargs)))
        return {"success": True, "accepted": True, "timeout_seconds": to}

    # ── 工具：定时任务（供 Planner 建/看/删） ────────────────

    @Tool(
        "schedule_task",
        description=(
            "新建一个定时任务：到点后把 prompt 交给 dsh 执行、结果发回当前聊天。"
            "after_seconds / every_seconds / at 三者只填一个。"
        ),
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="任务短标题", required=True),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="到点交给 dsh 的任务描述", required=True),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="N 秒后执行一次", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="每 N 秒重复（≥60）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="指定时刻（ISO，带时区），如 2026-10-03T09:00:00+08:00", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="5 字段 cron（分 时 日 月 周），如 0 9 * * 1-5", required=False),
        ],
    )
    async def _tool_schedule_task(self, **kwargs: Any) -> dict[str, Any]:
        if not self.config.plugin.enabled:
            return {"success": False, "error": "插件未启用"}
        if not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_task"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if not title or not prompt:
            return {"success": False, "error": "title 与 prompt 不能为空"}
        stream_id = str(kwargs.get("stream_id") or "").strip()
        task_id = self._create_task(
            title,
            prompt,
            kwargs.get("after_seconds"),
            kwargs.get("every_seconds"),
            kwargs.get("at"),
            kwargs.get("cron"),
            stream_id,
            self._extract_ctx(kwargs),
        )
        if not task_id:
            return {"success": False, "error": "请提供 after_seconds / every_seconds / at / cron 之一（cron 需 5 字段且能找到匹配时点）"}
        task = self._tasks[task_id]
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(task["next_at"]))
        self.ctx.logger.info("已建定时任务 %s「%s」下一次 %s", task_id, title, when)
        try:
            await self.ctx.send.text(f"⏰ 已设定定时任务「{title}」：下次 {when}", stream_id)
        except Exception:  # noqa: BLE001
            pass
        return {"success": True, "id": task_id, "next_at": task["next_at"]}

    @Tool("schedule_list", description="列出当前所有定时任务。", visibility="visible", parameters=[])
    async def _tool_schedule_list(self, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        items = []
        for t in self._tasks.values():
            nx = t.get("next_at")
            items.append(
                {
                    "id": t.get("id"),
                    "title": t.get("title"),
                    "kind": (t.get("trigger") or {}).get("kind"),
                    "next_at": time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None,
                    "prompt": str(t.get("prompt") or "")[:80],
                }
            )
        return {"success": True, "count": len(items), "tasks": items}

    @Tool(
        "schedule_update",
        description="按 id 修改一个定时任务：可改 title / prompt / 时间规则（after_seconds / every_seconds / at / cron 之一）。",
        visibility="visible",
        parameters=[
            ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True),
            ToolParameterInfo(name="title", param_type=ToolParamType.STRING, description="新标题（可选）", required=False),
            ToolParameterInfo(name="prompt", param_type=ToolParamType.STRING, description="新任务描述（可选）", required=False),
            ToolParameterInfo(name="after_seconds", param_type=ToolParamType.INTEGER, description="改为 N 秒后一次（可选）", required=False),
            ToolParameterInfo(name="every_seconds", param_type=ToolParamType.INTEGER, description="改为每 N 秒重复（可选）", required=False),
            ToolParameterInfo(name="at", param_type=ToolParamType.STRING, description="改为指定 ISO 时刻（可选）", required=False),
            ToolParameterInfo(name="cron", param_type=ToolParamType.STRING, description="改为 5 字段 cron（可选）", required=False),
        ],
    )
    async def _tool_schedule_update(self, **kwargs: Any) -> dict[str, Any]:
        if not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_update"}
        tid = str(kwargs.get("id") or "").strip()
        task = self._tasks.get(tid)
        if not task:
            return {"success": False, "error": "未找到该任务"}
        title = str(kwargs.get("title") or "").strip()
        prompt = str(kwargs.get("prompt") or "").strip()
        if title:
            task["title"] = title
        if prompt:
            task["prompt"] = prompt
        if any(kwargs.get(k) not in (None, "") for k in ("after_seconds", "every_seconds", "at", "cron")):
            trigger = self._parse_schedule(kwargs.get("after_seconds"), kwargs.get("every_seconds"), kwargs.get("at"), kwargs.get("cron"))
            if not trigger:
                return {"success": False, "error": "时间规则无效"}
            next_at = trigger.pop("next_at")
            task["trigger"] = trigger
            task["next_at"] = next_at
        self._save_tasks()
        nx = task.get("next_at")
        when = time.strftime("%m-%d %H:%M:%S", time.localtime(nx)) if nx else None
        return {"success": True, "id": tid, "next_at": when}

    @Tool(
        "schedule_cancel",
        description="按 id 取消一个定时任务。",
        visibility="visible",
        parameters=[ToolParameterInfo(name="id", param_type=ToolParamType.STRING, description="任务 id", required=True)],
    )
    async def _tool_schedule_cancel(self, **kwargs: Any) -> dict[str, Any]:
        if not self._is_allowed(kwargs):
            return {"success": False, "error": "无权限使用 schedule_cancel"}
        tid = str(kwargs.get("id") or "").strip()
        if tid in self._tasks:
            self._tasks.pop(tid, None)
            self._save_tasks()
            return {"success": True, "removed": tid}
        return {"success": False, "error": "未找到该任务"}


def create_plugin() -> DshBridgePlugin:
    return DshBridgePlugin()
