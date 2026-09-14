from __future__ import annotations

import hashlib
import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from constants import DEFAULT_TELEGRAM_MENU_TASKS
from etk.domains.operations.jobs import JobSubmissionService, TaskBusyError
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.integrations.http_policy import HttpTransportPolicy, RetryPolicy
from etk.integrations.telegram import HttpxTelegramBotAdapter
from etk.platform import Settings
from etk.platform.tasks import RETIRED_EXTERNAL_RESOURCE_TASK_KEYS

logger = logging.getLogger(__name__)


class NativeTelegramBot:
    """仅由 Coordinator Leader 持有；Bot 轮询不依赖定时任务总开关。"""

    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        jobs: JobSubmissionService,
        state: SqlAlchemyAppSettingsRepository,
        admin_chat_ids: Callable[[], Sequence[str]],
        bot: HttpxTelegramBotAdapter | None = None,
    ) -> None:
        self._settings = settings
        self._jobs = jobs
        self._state = state
        self._admins = admin_chat_ids
        self._bot = bot or HttpxTelegramBotAdapter(
            lambda: {**settings().to_runtime_dict(), "telegram_timeout": 8},
            policy=HttpTransportPolicy(system="telegram", retry_policy=RetryPolicy(max_attempts=1)),
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._revision = ""
        self._offset: int | None = None
        self._username = ""

    def sync(self, settings: Settings) -> None:
        token = settings.telegram_bot_token.get_secret_value().strip()
        revision = hashlib.sha256(token.encode()).hexdigest() if token else ""
        if revision == self._revision and self._thread is not None and self._thread.is_alive():
            return
        self.stop()
        if not token:
            return
        self._revision = revision
        self._offset = None
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="etk-telegram-bot", daemon=True)
        self._thread.start()
        logger.info("Telegram Bot 原生指令轮询已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=30)
            if self._thread.is_alive():
                raise RuntimeError("Telegram Bot 轮询尚未停止，拒绝启动第二个消费者")
        self._thread = None
        self._revision = ""

    def _run(self) -> None:
        while not self._stop.is_set():
            delay = 2.0
            try:
                delay = self.poll_once()
            except Exception as exc:
                # 不记录异常正文，第三方 HTTP 错误可能包含 Bot Token URL。
                logger.warning("Telegram Bot 轮询失败：%s", type(exc).__name__)
                delay = 10.0
            self._stop.wait(delay)

    def poll_once(self) -> float:
        key = f"telegram_bot_cursor:{self._revision}"
        if self._offset is None:
            identity = self._bot.get_me()
            if not identity.successful:
                return max(10.0, identity.retry_after_seconds or 0)
            self._username = str((identity.payload.get("result") or {}).get("username") or "").casefold()
            # 停机期间积压的菜单点击和指令不自动执行，启动后请重新发送。
            latest = self._bot.get_updates(offset=-1, timeout=0, limit=1)
            if not latest.successful:
                return max(10.0, latest.retry_after_seconds or 0)
            previous = self._state.load(key) or {}
            updates = latest.payload.get("result") or []
            offset = max(int(previous.get("offset") or 0), max((int(u["update_id"]) + 1 for u in updates), default=0))
            if self._stop.is_set():
                return 2.0
            self._state.save(key, {"offset": offset})
            self._offset = offset
            self._bot.set_my_commands(
                [
                    {"command": "all_tasks", "description": "查看 Telegram 快捷任务"},
                    {"command": "status", "description": "查看当前任务进度"},
                ]
            )
            logger.info("Telegram Bot 已连接，历史指令已跳过，等待新指令")
            return 2.0
        result = self._bot.get_updates(offset=self._offset, timeout=0, limit=20)
        if not result.successful:
            logger.warning("Telegram Bot getUpdates 未确认：HTTP %s", result.status_code)
            return max(10.0, result.retry_after_seconds or 0)
        for update in result.payload.get("result") or []:
            if self._stop.is_set():
                break
            update_id = int(update["update_id"])
            if update_id < self._offset:
                continue
            # 在外部回复/提交前落游标：崩溃时不重放指令，任务还有 update_id 幂等键。
            self._state.save(key, {"offset": update_id + 1})
            self._offset = update_id + 1
            try:
                self.handle_update(update)
            except Exception as exc:
                logger.warning("Telegram 指令处理失败：%s，update_id=%s", type(exc).__name__, update_id)
        return 2.0

    def handle_update(self, update: Mapping[str, Any]) -> None:
        callback = update.get("callback_query") or {}
        message = callback.get("message") or update.get("message") or {}
        sender = str((callback.get("from") or message.get("from") or {}).get("id") or "")
        chat = message.get("chat") or {}
        # 只在私聊返回管理任务信息；频道 Chat ID 不构成管理员身份。
        if chat.get("type") != "private" or sender not in {str(v) for v in self._admins()}:
            return
        if self._stop.is_set():
            return
        chat_id = str(chat.get("id") or "")
        definitions = {d.key: d for d in self._jobs.registry.definitions()}
        configured = self._settings().get("tg_menu_tasks") or DEFAULT_TELEGRAM_MENU_TASKS
        if not isinstance(configured, (list, tuple)):
            configured = DEFAULT_TELEGRAM_MENU_TASKS
        allowed = list(
            dict.fromkeys(
                key
                for key in configured
                if isinstance(key, str)
                and key in definitions
                and key != "scan-organize-115"
                and key not in RETIRED_EXTERNAL_RESOURCE_TASK_KEYS
            )
        )
        task_key = ""
        if callback:
            self._bot.answer_callback_query(str(callback["id"]))
            data = str(callback.get("data") or "")
            if data.startswith("run_task_"):
                task_key = data.removeprefix("run_task_")
        else:
            command = str(message.get("text") or "").replace("\\_", "_").strip().split()
            if not command or not command[0].startswith("/"):
                return
            token = command[0][1:].casefold()
            name, _, destination = token.partition("@")
            if destination and destination != self._username:
                return
            if name in {"all_tasks", "tasks", "menu", "start", "help"}:
                buttons = [{"text": definitions[k].name, "callback_data": f"run_task_{k}"} for k in allowed]
                self._reply(
                    chat_id,
                    "Telegram 快捷任务：点击按钮提交。进度和结果可在任务中心查看。",
                    reply_markup={"inline_keyboard": [buttons[i : i + 2] for i in range(0, len(buttons), 2)]},
                )
                return
            if name == "status":
                status = self._jobs.legacy_status()
                self._reply(
                    chat_id, f"{status['current_action']}\n进度：{status['progress']}%\n{status['message']}"[:4096]
                )
                return
            task_key = next((k for k in allowed if k.replace("-", "_") == name), "")
        if task_key not in allowed:
            self._reply(chat_id, "该指令或任务未启用。请发送 /all_tasks 查看当前快捷任务。")
            return
        if self._stop.is_set():
            return
        parameters = (
            {"force_full_update": False}
            if task_key in {"role-translation", "enrich-aliases", "populate-metadata"}
            else {}
        )
        try:
            submitted = self._jobs.submit_task(
                task_key,
                parameters,
                trigger_type="manual",
                reject_when_busy=True,
                idempotency_scope=f"telegram-bot:{self._revision}",
                idempotency_key=str(update["update_id"]),
            )
        except TaskBusyError:
            self._reply(chat_id, "后台已有任务运行，请稍后重试。")
            return
        self._reply(
            chat_id,
            f"任务已提交：{definitions[task_key].name}\n任务 ID：{submitted.projection.job.id}\n请在任务中心查看结果。",
        )
        logger.info("Telegram 快捷任务已提交：%s", task_key)

    def _reply(self, chat_id: str, message: str, **kwargs: Any) -> None:
        if self._stop.is_set():
            return
        result = self._bot.send_message(chat_id, message, **kwargs)
        if not result.successful:
            logger.warning("Telegram 指令回复未确认：HTTP %s，不自动重复发送", result.status_code)
