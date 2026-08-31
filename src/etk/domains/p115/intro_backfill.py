from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome

from .directory_sync import P115TaskProgress


class IntroBackfillGateway(Protocol):
    def enqueue_active_backfill(self, limit: int = 50, *, force: bool = False) -> Mapping[str, Any]: ...


class P115IntroBackfillTaskService:
    def __init__(self, gateway: IntroBackfillGateway, *, limit: int = 50) -> None:
        self._gateway = gateway
        self._limit = max(1, min(int(limit), 200))

    def execute(self, progress: P115TaskProgress, *, force: bool = True) -> TaskOutcome:
        progress.checkpoint()
        progress.update(0, "=== 开始提交片头片尾提取任务 ===")
        result = dict(self._gateway.enqueue_active_backfill(limit=self._limit, force=force))
        details = {**result, "native_intro_detection": True, "forced_backfill": force}
        if not result.get("ok"):
            message = "片头片尾提取任务提交失败"
            progress.update(-1, message)
            return TaskOutcome.failed(message, details)
        if result.get("skipped"):
            message = "当前触发策略无需执行片头片尾查漏"
            progress.update(100, message)
            return TaskOutcome.skipped(message, details)
        count = int(result.get("count") or 0)
        message = f"片头片尾查漏完成，已创建 {count} 个持久化季任务"
        progress.update(100, message)
        return TaskOutcome.succeeded(message, details)


__all__ = ["IntroBackfillGateway", "P115IntroBackfillTaskService"]
