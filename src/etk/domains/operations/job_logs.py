from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.observability.redaction import redact_sensitive

from .logs import LocalLogStorage, LogAccessDenied, LogFileMissing, group_log_lines

JOB_LOG_PREFIX = "app.log.task-"


class PersistentJobLogStorage:
    """通过现有日志接口读取原生任务快照，兼容仍存在的旧日志文件。"""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork], local: LocalLogStorage) -> None:
        self._unit_of_work = unit_of_work
        self._local = local

    def list_names(self) -> list[str]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        "SELECT id FROM job_runs WHERE parent_job_id IS NULL ORDER BY created_at DESC, id DESC LIMIT 1"
                    )
                )
                .mappings()
                .all()
            )
        names = ["app.log"] if rows else []
        try:
            names.extend(
                name
                for name in self._local.list_names()
                if name != ".app-log.lock" and not name.startswith(JOB_LOG_PREFIX)
            )
        except FileNotFoundError:
            pass
        return list(dict.fromkeys(names))

    def read_lines(self, filename: str) -> list[str]:
        if filename != "app.log" and not filename.startswith(JOB_LOG_PREFIX):
            return self._local.read_lines(filename)
        job_id = None
        if filename != "app.log":
            try:
                job_id = UUID(filename.removeprefix(JOB_LOG_PREFIX))
            except ValueError as exc:
                raise LogAccessDenied(filename) from exc
            if filename != f"{JOB_LOG_PREFIX}{job_id}":
                raise LogAccessDenied(filename)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text("""
                    WITH RECURSIVE family(id) AS (
                        SELECT id FROM job_runs WHERE id = CAST(:job_id AS uuid)
                        OR (CAST(:job_id AS uuid) IS NULL AND id IN (
                            SELECT id FROM job_runs WHERE parent_job_id IS NULL
                            ORDER BY created_at DESC, id DESC LIMIT 100
                        ))
                        UNION
                        SELECT child.id FROM job_runs child JOIN family ON child.parent_job_id = family.id
                    )
                    SELECT job.id, job.task_key, job.status, job.progress, job.progress_message,
                           job.created_at, job.updated_at, job.finished_at, job.logs_json, job.error_summary_json,
                           job.request_payload_json->>'display_name' AS display_name
                    FROM job_runs job JOIN family ON family.id = job.id
                    ORDER BY job.created_at, job.id
                """),
                    {"job_id": job_id},
                )
                .mappings()
                .all()
            )
        lines = []
        if filename == "app.log":
            try:
                lines.extend(self._local.read_lines("app.log"))
            except FileNotFoundError:
                pass
        if not rows and not lines:
            raise LogFileMissing(filename)
        for row in rows:
            name = row.get("display_name") or row["task_key"]
            label = f"[任务 {name} / {row['id']}]"
            lines.append(_line(row["created_at"], "INFO", f"任务 {name} [{row['id']}]"))
            for entry in row["logs_json"] or []:
                if isinstance(entry, Mapping):
                    lines.append(
                        _line(entry.get("timestamp"), entry.get("level", "INFO"), f"{label} {entry.get('message', '')}")
                    )
            error = row.get("error_summary_json")
            if isinstance(error, Mapping):
                lines.append(
                    _line(
                        row.get("finished_at") or row.get("updated_at") or row["created_at"],
                        "ERROR",
                        f"任务 {name}：{redact_sensitive(error.get('message') or error.get('type') or '任务失败')}",
                    )
                )
            lines.append(
                _line(
                    row.get("finished_at") or row.get("updated_at") or row["created_at"],
                    "INFO",
                    f"任务 {name}：状态={row['status']}，进度={row['progress']}%；{row['progress_message']}",
                )
            )
        if filename == "app.log":
            lines = _chronological(lines)
        return [
            "统一应用日志：任务部分为当前保留的日志快照（最近 100 个根任务，每任务最多 200 条），不是完整进程日志。\n",
            *lines,
        ]


def _chronological(lines: list[str]) -> list[str]:
    groups: list[tuple[str, list[str]]] = []
    for line in lines:
        if re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line):
            groups.append((line[:23], [line]))
        elif groups:
            groups[-1][1].append(line)
        else:
            groups.append(("", [line]))
    return group_log_lines([line for _, group in sorted(groups, key=lambda group: group[0]) for line in group])


def _line(timestamp: Any, level: Any, message: Any) -> str:
    message = redact_sensitive(str(message))
    try:
        value = timestamp if isinstance(timestamp, datetime) else datetime.fromisoformat(str(timestamp))
        if value.tzinfo is not None:
            value = value.astimezone()
        stamp = value.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        zone = value.strftime("%z")
    except (ValueError, TypeError):
        return f"[{str(level).upper()}] {message}\n"
    return f"{stamp} - task - {str(level).upper()} - {message}" + (f" [UTC{zone}]" if zone else "") + "\n"
