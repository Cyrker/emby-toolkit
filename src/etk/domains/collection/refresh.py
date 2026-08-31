from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol

from sqlalchemy import bindparam, text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome


@dataclass(frozen=True, slots=True)
class NativeCollectionReference:
    emby_collection_id: str
    tmdb_collection_id: str
    name: str
    library_id: str


@dataclass(frozen=True, slots=True)
class NativeCollectionPublishResult:
    movie_count: int
    subscription_candidates: int
    applied: bool = True


class NativeCollectionRefreshProvider(Protocol):
    def list_collections(self) -> Sequence[NativeCollectionReference]: ...

    def collection_details(self, tmdb_collection_id: str) -> Mapping[str, Any]: ...


class NativeCollectionRefreshRepository(Protocol):
    def reconcile_active(
        self,
        emby_collection_ids: Sequence[str],
        *,
        observed_at: datetime,
    ) -> int: ...

    def publish(
        self,
        reference: NativeCollectionReference,
        details: Mapping[str, Any],
        *,
        auto_subscribe: bool,
        today: date,
        observed_at: datetime,
    ) -> NativeCollectionPublishResult: ...


class NativeCollectionRefreshProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class SqlAlchemyNativeCollectionRefreshRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reconcile_active(
        self,
        emby_collection_ids: Sequence[str],
        *,
        observed_at: datetime | None = None,
    ) -> int:
        identifiers = tuple(dict.fromkeys(str(value).strip() for value in emby_collection_ids if str(value).strip()))
        snapshot_at = _utc(observed_at)
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('native-collection-refresh', 0))"))
            if identifiers:
                result = uow.session.execute(
                    text(
                        """
                        UPDATE collections_info
                        SET emby_collection_id = NULL,
                            is_active = FALSE,
                            last_checked_at = NOW()
                        WHERE is_active IS TRUE
                          AND (last_checked_at IS NULL OR last_checked_at <= :observed_at)
                          AND (
                              emby_collection_id IS NULL
                              OR emby_collection_id NOT IN :emby_collection_ids
                          )
                        """
                    ).bindparams(bindparam("emby_collection_ids", expanding=True)),
                    {
                        "emby_collection_ids": identifiers,
                        "observed_at": snapshot_at,
                    },
                )
            else:
                result = uow.session.execute(
                    text(
                        """
                        UPDATE collections_info
                        SET emby_collection_id = NULL,
                            is_active = FALSE,
                            last_checked_at = NOW()
                        WHERE is_active IS TRUE
                          AND (last_checked_at IS NULL OR last_checked_at <= :observed_at)
                        """
                    ),
                    {"observed_at": snapshot_at},
                )
            changed = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
        return changed

    def publish(
        self,
        reference: NativeCollectionReference,
        details: Mapping[str, Any],
        *,
        auto_subscribe: bool,
        today: date,
        observed_at: datetime | None = None,
    ) -> NativeCollectionPublishResult:
        snapshot_at = _utc(observed_at)
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('native-collection-refresh', 0))"))
            refreshed_after_snapshot = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS(
                        SELECT 1
                        FROM collections_info
                        WHERE tmdb_collection_id = :tmdb_collection_id
                          AND last_checked_at > :observed_at
                    )
                    """
                ),
                {
                    "tmdb_collection_id": reference.tmdb_collection_id,
                    "observed_at": snapshot_at,
                },
            )
            if refreshed_after_snapshot:
                uow.commit()
                return NativeCollectionPublishResult(0, 0, applied=False)
            result = publish_native_collection_in_uow(
                uow,
                reference,
                details,
                auto_subscribe=auto_subscribe,
                today=today,
            )
            uow.commit()
        return result


def publish_native_collection_in_uow(
    uow: SqlAlchemyUnitOfWork,
    reference: NativeCollectionReference,
    details: Mapping[str, Any],
    *,
    auto_subscribe: bool,
    today: date,
) -> NativeCollectionPublishResult:
    """Publish one authoritative collection inside the caller's transaction."""

    normalized = _details(reference, details)
    parts = normalized["parts"]
    subscription_candidates = 0
    for part in parts:
        uow.session.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, original_title, release_date,
                    poster_path, backdrop_path, overview, in_library,
                    subscription_status, last_updated_at
                ) VALUES (
                    :tmdb_id, 'Movie', :title, :original_title, :release_date,
                    :poster_path, :backdrop_path, :overview, FALSE,
                    'NONE', NOW()
                )
                ON CONFLICT (tmdb_id, item_type) DO NOTHING
                """
            ),
            part,
        )
        if auto_subscribe:
            desired = "PENDING_RELEASE" if part["release_date"] > today else "WANTED"
            source = [{"type": "native_collection", "name": reference.name}]
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = :desired_status,
                        subscription_sources_json = CASE
                            WHEN COALESCE(subscription_sources_json, '[]'::jsonb)
                                 @> CAST(:source AS jsonb)
                            THEN COALESCE(subscription_sources_json, '[]'::jsonb)
                            ELSE COALESCE(subscription_sources_json, '[]'::jsonb)
                                 || CAST(:source AS jsonb)
                        END,
                        first_requested_at = COALESCE(first_requested_at, NOW()),
                        ignore_reason = NULL,
                        poster_path = COALESCE(:poster_path, poster_path),
                        backdrop_path = COALESCE(:backdrop_path, backdrop_path),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = 'Movie'
                      AND in_library IS FALSE
                      AND subscription_status NOT IN ('SUBSCRIBED', 'PAUSED')
                      AND NOT (
                          COALESCE(subscription_sources_json, '[]'::jsonb)
                          @> CAST(:source AS jsonb)
                      )
                      AND NOT (
                          subscription_status = 'IGNORED'
                          AND COALESCE(ignore_reason, '') IN ('手动忽略', '订阅超时')
                      )
                      AND (
                          :desired_status = 'WANTED'
                          OR :desired_status = 'PENDING_RELEASE'
                             AND subscription_status <> 'WANTED'
                      )
                    """
                ),
                {
                    **part,
                    "desired_status": desired,
                    "source": json.dumps(source, ensure_ascii=False, separators=(",", ":")),
                },
            )
            subscription_candidates += int(getattr(result, "rowcount", 0) or 0)
    uow.session.execute(
        text(
            """
            INSERT INTO collections_info (
                tmdb_collection_id, emby_collection_id, name, overview,
                poster_path, backdrop_path, metadata_schema_version,
                is_active, all_tmdb_ids_json, last_checked_at
            ) VALUES (
                :tmdb_collection_id, :emby_collection_id, :name, :overview,
                :poster_path, :backdrop_path, 1,
                TRUE, CAST(:all_tmdb_ids AS jsonb), NOW()
            )
            ON CONFLICT (tmdb_collection_id) DO UPDATE SET
                emby_collection_id = EXCLUDED.emby_collection_id,
                name = EXCLUDED.name,
                overview = COALESCE(EXCLUDED.overview, collections_info.overview),
                poster_path = EXCLUDED.poster_path,
                backdrop_path = COALESCE(EXCLUDED.backdrop_path, collections_info.backdrop_path),
                metadata_schema_version = 1,
                is_active = TRUE,
                all_tmdb_ids_json = EXCLUDED.all_tmdb_ids_json,
                last_checked_at = NOW()
            """
        ),
        {
            "tmdb_collection_id": reference.tmdb_collection_id,
            "emby_collection_id": reference.emby_collection_id,
            "name": reference.name or normalized["name"],
            "overview": normalized["overview"],
            "poster_path": normalized["poster_path"],
            "backdrop_path": normalized["backdrop_path"],
            "all_tmdb_ids": json.dumps(
                [part["tmdb_id"] for part in parts],
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        },
    )
    return NativeCollectionPublishResult(len(parts), subscription_candidates)


class NativeCollectionRefreshService:
    def __init__(
        self,
        provider: NativeCollectionRefreshProvider,
        repository: NativeCollectionRefreshRepository,
        *,
        auto_subscribe: Callable[[], bool],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._provider = provider
        self._repository = repository
        self._auto_subscribe = auto_subscribe
        self._clock = clock

    def execute(self, progress: NativeCollectionRefreshProgress) -> TaskOutcome:
        observed_at = self._clock()
        progress.checkpoint()
        progress.update(0, "正在连接 Emby 获取合集列表...")
        collections = tuple(self._provider.list_collections())
        deactivated = self._repository.reconcile_active(
            [collection.emby_collection_id for collection in collections],
            observed_at=observed_at,
        )
        if not collections:
            progress.update(100, "未找到需要处理的合集。")
            return TaskOutcome.succeeded(
                "未找到需要处理的合集。",
                {
                    "collections": 0,
                    "refreshed": 0,
                    "deactivated": deactivated,
                    "movies": 0,
                    "subscription_candidates": 0,
                    "failed": [],
                },
            )

        auto_subscribe = bool(self._auto_subscribe())
        refreshed = 0
        movie_count = 0
        subscription_candidates = 0
        failures: list[dict[str, str]] = []
        total = len(collections)
        for index, collection in enumerate(collections, 1):
            progress.checkpoint()
            progress.update(
                min(95, 5 + int((index - 1) / total * 90)),
                f"正在刷新合集 ({index}/{total}): {collection.name}",
            )
            try:
                details = self._provider.collection_details(collection.tmdb_collection_id)
                result = self._repository.publish(
                    collection,
                    details,
                    auto_subscribe=auto_subscribe,
                    today=observed_at.date(),
                    observed_at=observed_at,
                )
                refreshed += int(result.applied)
                movie_count += result.movie_count
                subscription_candidates += result.subscription_candidates
            except Exception as exc:
                failures.append(
                    {
                        "emby_collection_id": collection.emby_collection_id,
                        "tmdb_collection_id": collection.tmdb_collection_id,
                        "error_type": type(exc).__name__,
                    }
                )

        details = {
            "collections": total,
            "refreshed": refreshed,
            "deactivated": deactivated,
            "movies": movie_count,
            "subscription_candidates": subscription_candidates,
            "failed": failures,
        }
        progress.update(100, "原生合集扫描完成。")
        if failures:
            return TaskOutcome.partial("原生合集扫描部分完成。", details)
        return TaskOutcome.succeeded("原生合集扫描完成。", details)


def _details(reference: NativeCollectionReference, value: Mapping[str, Any]) -> dict[str, Any]:
    identity = str(value.get("id") or "").strip()
    if identity != reference.tmdb_collection_id:
        raise ValueError("TMDb collection response identity does not match the request")
    raw_parts = value.get("parts")
    if not isinstance(raw_parts, list):
        raise ValueError("TMDb collection response does not contain parts")
    parts: list[dict[str, Any]] = []
    for raw in raw_parts:
        if not isinstance(raw, Mapping) or not raw.get("poster_path") or not raw.get("release_date"):
            continue
        tmdb_id = str(raw.get("id") or "").strip()
        title = str(raw.get("title") or "").strip()
        if not tmdb_id.isdigit() or not title:
            continue
        try:
            release_date = date.fromisoformat(str(raw["release_date"]))
        except ValueError:
            continue
        parts.append(
            {
                "tmdb_id": tmdb_id,
                "title": title,
                "original_title": raw.get("original_title"),
                "release_date": release_date,
                "poster_path": raw.get("poster_path"),
                "backdrop_path": raw.get("backdrop_path"),
                "overview": raw.get("overview"),
            }
        )
    if not parts:
        raise ValueError("TMDb collection response does not contain publishable movies")
    return {
        "name": str(value.get("name") or reference.name),
        "overview": value.get("overview"),
        "poster_path": value.get("poster_path"),
        "backdrop_path": value.get("backdrop_path"),
        "parts": tuple(parts),
    }


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "NativeCollectionPublishResult",
    "NativeCollectionReference",
    "NativeCollectionRefreshProgress",
    "NativeCollectionRefreshProvider",
    "NativeCollectionRefreshRepository",
    "NativeCollectionRefreshService",
    "SqlAlchemyNativeCollectionRefreshRepository",
    "publish_native_collection_in_uow",
]
