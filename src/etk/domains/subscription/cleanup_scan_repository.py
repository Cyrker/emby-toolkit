from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentExecutionCancelled

from .cleanup_scan import (
    CleanupScanEntry,
    CleanupScanItem,
    CleanupScanPolicy,
    CleanupScanReconcileResult,
    CleanupScanSnapshot,
    cleanup_scan_setting_keys,
    effective_cleanup_scan_policy,
)


class CleanupScanConflict(RuntimeError):
    pass


class CleanupScanDrift(RuntimeError):
    pass


class CleanupScanMediaProjection(Protocol):
    def lock_cleanup_scan_source(self, uow: SqlAlchemyUnitOfWork) -> None: ...

    def cleanup_scan_source(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        user_id: str | None,
        library_ids: Sequence[str],
        rating_mapping: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> Mapping[str, Any]: ...


class SqlAlchemyCleanupScanRepository:
    """Owns the cleanup scan policy snapshot and atomic pending-index publication."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: CleanupScanMediaProjection,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection

    def load_snapshot(self, *, user_id: str | None) -> CleanupScanSnapshot:
        with self._unit_of_work() as uow:
            return self._load_snapshot(uow, user_id=user_id)

    def reconcile(
        self,
        snapshot: CleanupScanSnapshot,
        entries: Sequence[CleanupScanEntry],
        *,
        job_id: UUID | None,
        cancellation_check: Callable[[], None],
    ) -> CleanupScanReconcileResult:
        with self._unit_of_work() as uow:
            self._lock_cancellation_fence(uow, job_id)
            uow.session.execute(text("LOCK TABLE app_settings IN SHARE MODE"))
            self._media_projection.lock_cleanup_scan_source(uow)
            uow.session.execute(text("LOCK TABLE cleanup_index IN EXCLUSIVE MODE"))
            cancellation_check()
            active = bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM cleanup_execution_items
                            WHERE status IN ('queued', 'claimed', 'effect_started')
                        )
                        """
                    )
                )
            )
            if active:
                raise CleanupScanConflict("cleanup execution is active; pending index was not replaced")

            current = self._load_snapshot(uow, user_id=snapshot.user_id)
            if current.source_fingerprint != snapshot.source_fingerprint:
                raise CleanupScanDrift("cleanup scan source changed before atomic publication")

            deleted = uow.session.execute(text("DELETE FROM cleanup_index WHERE status = 'pending'"))
            deleted_pending = _rowcount(deleted)
            upserted = 0
            for entry in entries:
                result = uow.session.execute(
                    text(
                        """
                        INSERT INTO cleanup_index (
                            tmdb_id, item_type, status, versions_info_json, best_version_json
                        ) VALUES (
                            :tmdb_id, :item_type, 'pending',
                            CAST(:versions AS jsonb), CAST(:best_version AS jsonb)
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            versions_info_json = EXCLUDED.versions_info_json,
                            best_version_json = EXCLUDED.best_version_json,
                            status = 'pending',
                            last_updated_at = NOW()
                        WHERE cleanup_index.status <> 'ignored'
                        """
                    ),
                    {
                        "tmdb_id": entry.tmdb_id,
                        "item_type": entry.item_type,
                        "versions": json.dumps(list(entry.versions), ensure_ascii=False, separators=(",", ":")),
                        "best_version": json.dumps(
                            list(entry.best_version) if isinstance(entry.best_version, tuple) else entry.best_version,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    },
                )
                upserted += _rowcount(result)
            cancellation_check()
            uow.commit()
        return CleanupScanReconcileResult(deleted_pending, upserted)

    @staticmethod
    def _lock_cancellation_fence(uow: SqlAlchemyUnitOfWork, job_id: UUID | None) -> None:
        if job_id is None:
            return
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT id, parent_job_id, status
                    FROM job_runs
                    WHERE id IN (
                        SELECT id
                        FROM job_runs
                        WHERE id = :job_id
                        UNION ALL
                        SELECT parent_job_id
                        FROM job_runs
                        WHERE id = :job_id
                          AND parent_job_id IS NOT NULL
                    )
                    ORDER BY id
                    FOR UPDATE
                    """
                ),
                {"job_id": job_id},
            )
            .mappings()
            .all()
        )
        by_id = {str(row["id"]): row for row in rows}
        current = by_id.get(str(job_id))
        if current is None:
            raise CleanupScanConflict("cleanup scan Job no longer exists")
        if str(current["status"]) in {"cancel_requested", "cancelled"}:
            raise PersistentExecutionCancelled("cleanup scan cancellation was requested before publication")
        parent_id = current["parent_job_id"]
        parent = by_id.get(str(parent_id)) if parent_id is not None else None
        if parent is not None and str(parent["status"]) in {"cancel_requested", "cancelled"}:
            raise PersistentExecutionCancelled("cleanup scan parent cancellation was requested before publication")

    def _load_snapshot(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        user_id: str | None,
    ) -> CleanupScanSnapshot:
        values = self._load_settings(uow)
        policy = effective_cleanup_scan_policy(values)
        source = self._media_projection.cleanup_scan_source(
            uow,
            user_id=user_id,
            library_ids=policy.library_ids,
            rating_mapping=policy.rating_mapping,
        )
        items = _scan_items(source.get("items"))
        visible_movies = _string_tuple(source.get("visible_movie_ids"))
        visible_series = _string_tuple(source.get("visible_series_ids"))
        fingerprint = _source_fingerprint(
            user_id=user_id,
            policy=policy,
            source=source,
            items=items,
            visible_movies=visible_movies,
            visible_series=visible_series,
        )
        return CleanupScanSnapshot(
            user_id=user_id,
            policy=policy,
            visible_movie_ids=visible_movies,
            visible_series_ids=visible_series,
            items=items,
            source_fingerprint=fingerprint,
        )

    @staticmethod
    def _load_settings(uow: SqlAlchemyUnitOfWork) -> Mapping[str, Any]:
        keys = cleanup_scan_setting_keys()
        rows = (
            uow.session.execute(
                text(
                    """
                    SELECT setting_key, value_json
                    FROM app_settings
                    WHERE setting_key = ANY(CAST(:keys AS text[]))
                    """
                ),
                {"keys": list(keys)},
            )
            .mappings()
            .all()
        )
        values = {str(row["setting_key"]): row["value_json"] for row in rows}
        return {key: values.get(key) for key in keys}


def _scan_items(value: object) -> tuple[CleanupScanItem, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise TypeError("cleanup scan projection items must be an array")
    result: list[CleanupScanItem] = []
    for raw_item in value:
        if not isinstance(raw_item, Mapping):
            raise TypeError("cleanup scan projection item must be an object")
        raw_assets = raw_item.get("asset_details_json")
        if not isinstance(raw_assets, list):
            raise TypeError("cleanup scan asset_details_json must be an array")
        assets: list[Mapping[str, Any]] = []
        for asset in raw_assets:
            if not isinstance(asset, Mapping):
                raise TypeError("cleanup scan asset version must be an object")
            assets.append(dict(asset))
        result.append(
            CleanupScanItem(
                tmdb_id=str(raw_item.get("tmdb_id") or ""),
                item_type=str(raw_item.get("item_type") or ""),
                title=str(raw_item.get("title") or ""),
                original_language=str(raw_item.get("original_language") or ""),
                countries=raw_item.get("countries_json"),
                asset_details=tuple(assets),
            )
        )
    return tuple(result)


def _source_fingerprint(
    *,
    user_id: str | None,
    policy: CleanupScanPolicy,
    source: Mapping[str, Any],
    items: Sequence[CleanupScanItem],
    visible_movies: Sequence[str],
    visible_series: Sequence[str],
) -> str:
    payload = {
        "user_id": user_id,
        "user_policy": source.get("user_policy"),
        "user_exists": bool(source.get("user_exists", True)),
        "policy": {
            "rules": [dict(rule) for rule in policy.rules],
            "library_ids": list(policy.library_ids),
            "keep_one_per_resolution": policy.keep_one_per_resolution,
            "rating_mapping": {
                country: [dict(entry) for entry in entries] for country, entries in policy.rating_mapping.items()
            },
        },
        "visible_movie_ids": list(visible_movies),
        "visible_series_ids": list(visible_series),
        "items": [
            {
                "tmdb_id": item.tmdb_id,
                "item_type": item.item_type,
                "title": item.title,
                "original_language": item.original_language,
                "countries": item.countries,
                "asset_details": [dict(asset) for asset in item.asset_details],
            }
            for item in items
        ],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        raise TypeError("cleanup scan visible IDs must be an array")
    return tuple(str(item) for item in value)


def _rowcount(result: Any) -> int:
    value = getattr(result, "rowcount", 0)
    return max(int(value or 0), 0)


__all__ = [
    "CleanupScanConflict",
    "CleanupScanDrift",
    "CleanupScanMediaProjection",
    "SqlAlchemyCleanupScanRepository",
]
