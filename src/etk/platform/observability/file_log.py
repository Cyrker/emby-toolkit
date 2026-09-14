from __future__ import annotations

import logging
import os
from collections.abc import Mapping
from copy import copy
from pathlib import Path

from .redaction import redact_sensitive


class SharedApplicationLog(logging.Handler):
    """Linux/Mac 多进程共享一个限容日志；锁文件不随轮转替换。"""

    def __init__(self, directory: Path, *, max_bytes: int = 10 * 1024 * 1024) -> None:
        super().__init__()
        self.directory = directory
        self.max_bytes = max_bytes
        directory.mkdir(parents=True, exist_ok=True)
        self.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        import fcntl

        try:
            if isinstance(record.msg, Mapping):
                event = record.msg.get("event") or str(record.msg)
                record = copy(record)
                record.msg = str(event)
                record.args = ()
            message = (str(redact_sensitive(self.format(record))) + "\n").encode("utf-8")
            path = self.directory / "app.log"
            with (self.directory / ".app-log.lock").open("a") as lock:
                fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
                if path.exists() and path.stat().st_size + len(message) > self.max_bytes:
                    os.replace(path, self.directory / "app.log.1")
                with path.open("ab") as stream:
                    stream.write(message)
        except Exception:
            self.handleError(record)
