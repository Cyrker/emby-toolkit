from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome

from .models import P115Result


@dataclass(frozen=True, slots=True)
class P115OrganizeRecordsPage:
    items: tuple[dict[str, Any], ...]
    total: int
    fail_reasons: tuple[dict[str, Any], ...]
    stats: dict[str, int]


class P115OrganizeRecordsRepository(Protocol):
    def list_records(
        self,
        *,
        page: int,
        per_page: int,
        search: str,
        status: str,
        cid: str,
        fail_reason: str,
    ) -> P115OrganizeRecordsPage: ...

    def delete_record(self, record_id: int) -> None: ...


class P115OrganizeRecordTmdbBackfillRepository(Protocol):
    def backfill_missing_tmdb_ids(self) -> int: ...


class P115OrganizeRecordWriter(Protocol):
    def add_missing_record(self, record: Mapping[str, Any]) -> bool: ...


class P115OrganizeRecordTmdbBackfillProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...


class P115OrganizeRecordsService:
    def __init__(self, repository: P115OrganizeRecordsRepository) -> None:
        self._repository = repository

    def list_records(self, query: Mapping[str, Any]) -> P115Result:
        try:
            page = int(query.get("page", 1))
            per_page = int(query.get("per_page", 15))
            result = self._repository.list_records(
                page=page,
                per_page=per_page,
                search=str(query.get("search") or ""),
                status=str(query.get("status") or "all"),
                cid=str(query.get("cid") or ""),
                fail_reason=str(query.get("fail_reason") or "").strip(),
            )
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        return P115Result(
            {
                "success": True,
                "items": [dict(item) for item in result.items],
                "total": result.total,
                "fail_reasons": [dict(value) for value in result.fail_reasons],
                "stats": dict(result.stats),
            },
            200,
        )

    def delete_record(self, record_id: int) -> P115Result:
        try:
            self._repository.delete_record(int(record_id))
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        return P115Result(
            {"success": True, "message": "记录及相关目录缓存已清理"},
            200,
        )


class P115OrganizeRecordTmdbBackfillService:
    """补齐整理记录身份，而不重新执行任何远端整理副作用。"""

    def __init__(self, repository: P115OrganizeRecordTmdbBackfillRepository) -> None:
        self._repository = repository

    def execute(self, progress: P115OrganizeRecordTmdbBackfillProgress) -> TaskOutcome:
        progress.update(10, "正在从媒体元数据匹配缺失 TMDb ID 的整理记录...")
        updated_count = self._repository.backfill_missing_tmdb_ids()
        message = f"整理记录 TMDb ID 补齐完成，共更新 {updated_count} 条记录。"
        progress.update(100, message)
        return TaskOutcome.succeeded(message, {"updated_count": updated_count})


def normalize_record_rows(rows: Sequence[Mapping[Any, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple({str(key): value for key, value in row.items()} for row in rows)


__all__ = [
    "P115OrganizeRecordsPage",
    "P115OrganizeRecordsRepository",
    "P115OrganizeRecordsService",
    "P115OrganizeRecordWriter",
    "P115OrganizeRecordTmdbBackfillProgress",
    "P115OrganizeRecordTmdbBackfillRepository",
    "P115OrganizeRecordTmdbBackfillService",
    "normalize_record_rows",
]
