from __future__ import annotations

import json
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome


@dataclass(frozen=True, slots=True)
class MediaEmbeddingCandidate:
    tmdb_id: str
    item_type: str
    overview: str


class MediaEmbeddingRepository(Protocol):
    def count_pending(self) -> int: ...

    def list_pending(
        self,
        *,
        after: tuple[str, str] | None,
        limit: int,
    ) -> Sequence[MediaEmbeddingCandidate]: ...

    def store(
        self,
        candidate: MediaEmbeddingCandidate,
        embedding: Sequence[float],
    ) -> bool: ...


class MediaEmbeddingProvider(Protocol):
    def embed(self, text_value: str) -> Sequence[float] | None: ...


class MediaEmbeddingProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class SqlAlchemyMediaEmbeddingRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def count_pending(self) -> int:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT COUNT(*)
                    FROM media_metadata
                    WHERE overview IS NOT NULL
                      AND BTRIM(overview) <> ''
                      AND overview_embedding IS NULL
                      AND item_type IN ('Movie', 'Series')
                      AND in_library = TRUE
                    """
                )
            )
        return int(value or 0)

    def list_pending(
        self,
        *,
        after: tuple[str, str] | None,
        limit: int,
    ) -> Sequence[MediaEmbeddingCandidate]:
        bounded_limit = max(1, min(int(limit), 200))
        after_tmdb_id, after_item_type = after or (None, None)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, overview
                        FROM media_metadata
                        WHERE overview IS NOT NULL
                          AND BTRIM(overview) <> ''
                          AND overview_embedding IS NULL
                          AND item_type IN ('Movie', 'Series')
                          AND in_library = TRUE
                          AND (
                              CAST(:after_tmdb_id AS text) IS NULL
                              OR (tmdb_id, item_type) > (:after_tmdb_id, :after_item_type)
                          )
                        ORDER BY tmdb_id, item_type
                        LIMIT :limit
                        """
                    ),
                    {
                        "after_tmdb_id": after_tmdb_id,
                        "after_item_type": after_item_type,
                        "limit": bounded_limit,
                    },
                )
                .mappings()
                .all()
            )
        return tuple(
            MediaEmbeddingCandidate(
                tmdb_id=str(row["tmdb_id"]),
                item_type=str(row["item_type"]),
                overview=str(row["overview"]),
            )
            for row in rows
        )

    def store(
        self,
        candidate: MediaEmbeddingCandidate,
        embedding: Sequence[float],
    ) -> bool:
        normalized = _embedding(embedding)
        with self._unit_of_work() as uow:
            updated = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET overview_embedding = CAST(:embedding AS jsonb),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = :item_type
                      AND overview = :overview
                      AND overview_embedding IS NULL
                      AND in_library = TRUE
                    """
                ),
                {
                    "tmdb_id": candidate.tmdb_id,
                    "item_type": candidate.item_type,
                    "overview": candidate.overview,
                    "embedding": json.dumps(normalized, separators=(",", ":")),
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                return False
            uow.commit()
        return True


class MediaEmbeddingService:
    _BATCH_SIZE = 50

    def __init__(
        self,
        repository: MediaEmbeddingRepository,
        provider: MediaEmbeddingProvider,
        *,
        enabled: Callable[[], bool],
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._enabled = enabled

    def execute(self, progress: MediaEmbeddingProgress) -> TaskOutcome:
        if not bool(self._enabled()):
            progress.update(100, "AI 向量化功能未启用，已跳过。")
            return TaskOutcome.skipped(
                "AI 向量化功能未启用，已跳过。",
                {"generated": 0, "failed": 0, "state_changed": 0},
            )

        progress.checkpoint()
        total = self._repository.count_pending()
        if total == 0:
            progress.update(100, "所有在库媒体均已拥有向量，无需处理。")
            return TaskOutcome.skipped(
                "所有在库媒体均已拥有向量，无需处理。",
                {"pending": 0, "generated": 0, "failed": 0, "state_changed": 0},
            )

        generated = 0
        failed: list[dict[str, str]] = []
        state_changed = 0
        scanned = 0
        cursor: tuple[str, str] | None = None
        while True:
            candidates = self._repository.list_pending(after=cursor, limit=self._BATCH_SIZE)
            if not candidates:
                break
            for candidate in candidates:
                progress.checkpoint()
                cursor = (candidate.tmdb_id, candidate.item_type)
                scanned += 1
                try:
                    value = self._provider.embed(candidate.overview)
                    if value is None:
                        raise RuntimeError("embedding provider returned no vector")
                    normalized = _embedding(value)
                    if self._repository.store(candidate, normalized):
                        generated += 1
                    else:
                        state_changed += 1
                except Exception as exc:
                    failed.append(
                        {
                            "tmdb_id": candidate.tmdb_id,
                            "item_type": candidate.item_type,
                            "error_type": type(exc).__name__,
                        }
                    )
                progress.update(
                    min(99, max(1, int(scanned / max(total, 1) * 100))),
                    f"正在生成向量... ({scanned}/{total})",
                )
            if len(candidates) < self._BATCH_SIZE:
                break

        details = {
            "pending": total,
            "scanned": scanned,
            "generated": generated,
            "failed": len(failed),
            "failures": failed,
            "state_changed": state_changed,
        }
        progress.update(100, f"向量生成任务结束。本次共新增 {generated} 个向量。")
        if failed:
            return TaskOutcome.partial("媒体向量生成部分完成。", details)
        return TaskOutcome.succeeded("媒体向量生成完成。", details)


def _embedding(value: Sequence[float]) -> list[float]:
    if isinstance(value, (str, bytes, bytearray)) or not value:
        raise ValueError("embedding vector must be a non-empty numeric sequence")
    if len(value) > 65_536:
        raise ValueError("embedding vector is too large")
    normalized: list[float] = []
    for item in value:
        if isinstance(item, bool):
            raise ValueError("embedding vector must contain finite numbers")
        number = float(item)
        if not math.isfinite(number):
            raise ValueError("embedding vector must contain finite numbers")
        normalized.append(number)
    return normalized


__all__ = [
    "MediaEmbeddingCandidate",
    "MediaEmbeddingProgress",
    "MediaEmbeddingProvider",
    "MediaEmbeddingRepository",
    "MediaEmbeddingService",
    "SqlAlchemyMediaEmbeddingRepository",
]
