from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from etk.compatibility.ingestion_monitor import PersistentIngestionIngress
from etk.platform import Settings

logger = logging.getLogger(__name__)


class NativeFilesystemMonitor(FileSystemEventHandler):
    """Leader-owned filesystem producer; stability, retries and work stay in the ledger."""

    def __init__(self, ingress: PersistentIngestionIngress, *, observer_factory: Callable[[], Any] = Observer) -> None:
        self._ingress = ingress
        self._factory = observer_factory
        self._observer: Any = None
        self._roots: tuple[str, ...] = ()
        self._extensions: set[str] = set()
        self._missing: set[str] = set()
        self._enabled = False

    def sync(self, settings: Settings) -> None:
        self._extensions = {"." + str(value).lower().lstrip(".") for value in settings.monitor_extensions}
        configured = sorted(
            {os.path.abspath(str(path)) for path in settings.monitor_paths if str(path).strip()}, key=len
        )
        missing = {path for path in configured if not os.path.isdir(path)} if settings.monitor_enabled else set()
        for path in missing - self._missing:
            logger.warning("文件监控目录不可用，等待目录恢复：%s", path)
        self._missing = missing
        roots: list[str] = []
        if settings.monitor_enabled:
            for path in configured:
                if path not in missing and not any(os.path.commonpath((path, parent)) == parent for parent in roots):
                    roots.append(path)
        desired = tuple(roots)
        if desired == self._roots and (not desired or self._observer is not None and self._observer.is_alive()):
            return
        self.stop()
        if not desired:
            return
        observer = self._factory()
        try:
            for path in desired:
                observer.schedule(self, path, recursive=True)
            self._roots = desired
            self._enabled = True
            observer.start()
            self._observer = observer
        except Exception:
            self._enabled = False
            self._roots = ()
            observer.stop()
            if observer.is_alive():
                observer.join(timeout=10)
            raise
        logger.info("文件监控已启动：%s；新文件稳定后进入自动刮削队列", "、".join(desired))

    def stop(self) -> None:
        self._enabled = False
        observer, self._observer = self._observer, None
        self._roots = ()
        if observer is not None:
            observer.stop()
            observer.join(timeout=10)
            if observer.is_alive():
                self._observer = observer
                raise RuntimeError("文件监控线程未能停止，拒绝重复启动")
            logger.info("文件监控已停止")

    def on_created(self, event: Any) -> None:
        self._record("created", event, event.src_path)

    def on_moved(self, event: Any) -> None:
        self._record("moved", event, event.dest_path, related_path=event.src_path)

    def on_deleted(self, event: Any) -> None:
        self._record("deleted", event, event.src_path)

    def _record(self, kind: str, event: Any, path: str, *, related_path: str | None = None) -> None:
        if not self._enabled or event.is_directory or os.path.splitext(path)[1].lower() not in self._extensions:
            return
        try:
            result = self._ingress.record_event(kind, path, related_path=related_path, source="watchdog")
            if result:
                logger.info("文件监控收到 %s：%s；事件已持久化，等待文件稳定", kind, path)
            else:
                logger.debug("文件监控跳过 %s：%s（不符合当前监控策略）", kind, path)
        except Exception:
            logger.exception("文件监控事件入队失败：%s；可通过监控查漏任务补偿", path)
