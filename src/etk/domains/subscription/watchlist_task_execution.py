"""Processor-free execution for the three Watchlist maintenance tasks."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol, cast
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import PersistentExecutionControlError
from etk.platform.jobs.json_payload import canonical_json
from etk.platform.tasks import TaskOutcome

logger = logging.getLogger(__name__)

WATCHLIST_TASK_KEYS = frozenset(
    {
        "process-watchlist",
        "refresh_completed_series",
        "add-all-series-to-watchlist",
    }
)

_ASSISTANT_SUBSCRIPTIONS_KEY = "subscribes"
_ASSISTANT_TORRENTS_KEY = "torrents"
_ASSISTANT_SUPPRESSIONS_KEY = "snapshot_restore_suppressions"


class SubscriptionTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class WatchlistMetadataPort(Protocol):
    def series_details(self, tmdb_id: str) -> Mapping[str, Any]: ...

    def season_details(self, tmdb_id: str, season_number: int) -> Mapping[str, Any]: ...


class WatchlistLibraryPort(Protocol):
    def list_series(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]: ...


class WatchlistTaskRepository(Protocol):
    def list_active_series(self, tmdb_id: str | None = None) -> Sequence[Mapping[str, Any]]: ...

    def list_completed_series(self, library_ids: Sequence[str] = ()) -> Sequence[Mapping[str, Any]]: ...

    def list_all_completed_series(self) -> Sequence[Mapping[str, Any]]: ...

    def import_library_series(self, items: Sequence[Mapping[str, Any]]) -> int: ...

    def refresh_series(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        *,
        season_details: Sequence[Mapping[str, Any]] = (),
    ) -> bool: ...


class WatchlistTaskEffects(Protocol):
    """Durable follow-up effects after one local watchlist publication."""

    def publish_refresh(
        self,
        task_key: str,
        tmdb_id: str,
        candidate: Mapping[str, Any],
        details: Mapping[str, Any],
        season_details: Sequence[Mapping[str, Any]],
        parameters: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]: ...


class WatchlistMoviePilotReadPort(Protocol):
    def list_subscriptions(self) -> Any: ...

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> Any: ...


class WatchlistMoviePilotEffectPort(Protocol):
    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool = True,
    ) -> bool: ...

    def update_subscription(self, payload: Mapping[str, Any]) -> bool: ...

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool: ...

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool: ...


class WatchlistSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


class WatchlistP115PlanPort(Protocol):
    def create(self, raw_payload: object) -> Any: ...


class WatchlistP115ExecutionPort(Protocol):
    def prepare(
        self,
        plan_id: str,
        approved_intent_id: str,
        *,
        requested_by_user_id: str | None = None,
        trace_id: str | None = None,
    ) -> Any: ...


class WatchlistP115ExecutionRepositoryPort(Protocol):
    def find_by_intent(self, approved_intent_id: str) -> Any: ...


class WatchlistSharingOperationRepositoryPort(Protocol):
    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]: ...


class WatchlistIntroPort(Protocol):
    def enqueue_imported_episode_ids(
        self,
        episode_ids: Sequence[Any],
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]: ...


class NativeWatchlistTaskEffects:
    """Publish processor-free follow-ups through their existing durable owners."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: Callable[[], Settings],
        *,
        moviepilot: NativeWatchlistMoviePilotGateway | None = None,
        p115: NativeWatchlistP115ReclassificationGateway | None = None,
        sharing: NativeWatchlistSharedMetadataGateway | None = None,
        intro: WatchlistIntroPort | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._moviepilot = moviepilot
        self._p115 = p115
        self._sharing = sharing
        self._intro = intro

    def publish_refresh(
        self,
        task_key: str,
        tmdb_id: str,
        candidate: Mapping[str, Any],
        details: Mapping[str, Any],
        season_details: Sequence[Mapping[str, Any]],
        parameters: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]:
        normalized_tmdb = str(tmdb_id or "").strip()
        if not normalized_tmdb:
            raise ValueError("watchlist refresh requires a TMDb series identity")

        result: dict[str, Any] = {
            "tmdb_id": normalized_tmdb,
            "emby": self._stage_emby_refresh(normalized_tmdb, details, parameters, checkpoint),
        }
        if task_key != "refresh_completed_series":
            if self._moviepilot is not None:
                result["moviepilot"] = self._moviepilot.sync_series(
                    normalized_tmdb,
                    candidate,
                    details,
                    season_details,
                    parameters,
                    checkpoint,
                )
            if self._p115 is not None:
                result["p115"] = self._p115.stage_reclassification(
                    normalized_tmdb,
                    candidate,
                    details,
                    checkpoint,
                )
            if self._sharing is not None:
                result["sharing"] = self._sharing.stage(
                    normalized_tmdb,
                    details,
                    season_details,
                    skip_logical_share_dispatch=(
                        task_key == "add-all-series-to-watchlist"
                        or (task_key == "process-watchlist" and not bool(parameters.get("tmdb_id")))
                    ),
                )
        episode_ids = _string_values(parameters.get("new_episode_ids"))
        if (
            self._intro is not None
            and task_key == "process-watchlist"
            and bool(parameters.get("tmdb_id"))
            and episode_ids
        ):
            checkpoint()
            result["intro"] = dict(
                self._intro.enqueue_imported_episode_ids(
                    episode_ids,
                    allow_playback_followup=True,
                )
            )
        return result

    def _stage_emby_refresh(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        parameters: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]:
        from etk.domains.media.bridge_client import emby_authorization_revision
        from etk.domains.media.effects import SqlAlchemyMediaEffectRepository
        from etk.domains.media.final_refresh_effects import FINAL_METADATA_REFRESH_EFFECT

        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT emby_item_ids_json
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                        FOR SHARE
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .first()
            )
            raw_ids = rows.get("emby_item_ids_json") if rows else ()
            item_ids = tuple(
                dict.fromkeys(
                    str(value).strip()
                    for value in (
                        raw_ids if isinstance(raw_ids, Sequence) and not isinstance(raw_ids, str | bytes) else ()
                    )
                    if str(value).strip()
                )
            )
            if not item_ids:
                return {"staged": 0, "skipped": True, "reason": "no_emby_item"}
            revision = emby_authorization_revision(self._settings())
            detail_digest = hashlib.sha256(
                canonical_json(dict(details), field="watchlist refresh details").encode("utf-8")
            ).hexdigest()
            staged = 0
            ledger = SqlAlchemyMediaEffectRepository(uow.session)
            for item_id in item_ids:
                checkpoint()
                payload = {
                    "item_id": item_id,
                    "item_type": "Series",
                    "emby_authorization_revision": revision,
                    "replace_all_metadata": True,
                    "replace_all_images": False,
                    "metadata_only": False,
                    "tmdb_id": tmdb_id,
                    "new_episode_ids": list(_string_values(parameters.get("new_episode_ids"))),
                }
                effect_key = f"p7:{FINAL_METADATA_REFRESH_EFFECT}:{tmdb_id}:{item_id}:{detail_digest}:{revision}"
                reservation = ledger.reserve(
                    effect_key=effect_key,
                    effect_type=FINAL_METADATA_REFRESH_EFFECT,
                    aggregate_type="media_item",
                    aggregate_id=item_id,
                    payload=payload,
                )
                staged += int(reservation.created)
            uow.commit()
        return {"staged": staged, "items": len(item_ids), "tmdb_id": tmdb_id}


class NativeWatchlistTaskService:
    """Run watchlist tasks from SQL projections and bounded TMDb reads."""

    def __init__(
        self,
        repository: WatchlistTaskRepository,
        metadata: WatchlistMetadataPort,
        libraries: WatchlistLibraryPort,
        *,
        library_ids: Callable[[], Sequence[str]] | None = None,
        effects: WatchlistTaskEffects | None = None,
    ) -> None:
        self._repository = repository
        self._metadata = metadata
        self._libraries = libraries
        self._library_ids = library_ids or (lambda: ())
        self._effects = effects

    def execute(
        self,
        task_key: str,
        progress: SubscriptionTaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        if task_key not in WATCHLIST_TASK_KEYS:
            raise LookupError(f"unsupported watchlist task key: {task_key!r}")
        if task_key == "add-all-series-to-watchlist":
            progress.update(5, "正在读取媒体库剧集...")
            progress.checkpoint()
            library_ids = tuple(self._library_ids())
            imported = self._repository.import_library_series(self._libraries.list_series(library_ids))
            candidates = tuple(_list_completed_series(self._repository, library_ids))
        elif task_key == "refresh_completed_series":
            progress.update(5, "正在读取已完结剧集...")
            progress.checkpoint()
            imported = 0
            candidates = tuple(_list_all_completed_series(self._repository))
        else:
            progress.update(5, "正在读取追剧项目...")
            progress.checkpoint()
            imported = 0
            requested = str(parameters.get("tmdb_id") or "").strip() or None
            candidates = tuple(self._repository.list_active_series(requested))

        if not candidates:
            progress.update(100, "扫描完成：没有需要处理的剧集。")
            return TaskOutcome.skipped(
                "扫描完成：没有需要处理的剧集。",
                {"processed": 0, "failed": 0, "imported": imported},
            )

        failures: list[dict[str, str]] = []
        updated = 0
        skipped = 0
        effect_stats: list[Mapping[str, Any]] = []
        total = len(candidates)
        for index, candidate in enumerate(candidates, 1):
            progress.checkpoint()
            tmdb_id = str(candidate.get("tmdb_id") or "").strip()
            if not tmdb_id:
                failures.append({"tmdb_id": "", "reason": "missing_tmdb_id"})
                continue
            progress.update(min(95, 10 + int((index - 1) / total * 85)), f"正在刷新剧集 ({index}/{total})")
            try:
                details = self._metadata.series_details(tmdb_id)
                if not details:
                    raise RuntimeError("TMDb watchlist query returned an empty result")
                if task_key == "refresh_completed_series" and not _completed_series_changed(candidate, details):
                    skipped += 1
                    continue
                season_rows = self._load_season_details(tmdb_id, details, progress)
                refreshed = _refresh_series(self._repository, tmdb_id, details, season_rows)
                if not refreshed:
                    failures.append({"tmdb_id": tmdb_id, "reason": "projection_not_updated"})
                    continue
                updated += 1
                if self._effects is not None:
                    progress.checkpoint()
                    effect_stats.append(
                        dict(
                            self._effects.publish_refresh(
                                task_key,
                                tmdb_id,
                                candidate,
                                details,
                                season_rows,
                                parameters,
                                progress.checkpoint,
                            )
                        )
                    )
            except PersistentExecutionControlError:
                raise
            except Exception as exc:
                logger.warning("watchlist series refresh failed", extra={"tmdb_id": tmdb_id}, exc_info=True)
                failures.append({"tmdb_id": tmdb_id, "reason": type(exc).__name__})

        processed = len(candidates)
        message = "剧集追踪刷新完成。" if not failures else "剧集追踪刷新部分完成。"
        progress.update(100, message)
        details = {
            "processed": processed,
            "updated": updated,
            "failed": len(failures),
            "failed_items": failures,
            "imported": imported,
            "skipped": skipped,
        }
        if effect_stats:
            details["effects"] = effect_stats
        if failures and updated == 0:
            return TaskOutcome.failed("剧集追踪刷新失败。", details)
        return TaskOutcome.partial(message, details) if failures else TaskOutcome.succeeded(message, details)

    def _load_season_details(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        progress: SubscriptionTaskProgress,
    ) -> tuple[Mapping[str, Any], ...]:
        fetch = getattr(self._metadata, "season_details", None)
        if not callable(fetch):
            return ()
        fetch_season = cast(Callable[[str, int], Mapping[str, Any]], fetch)
        seasons = details.get("seasons")
        if not isinstance(seasons, list):
            return ()
        values: list[Mapping[str, Any]] = []
        for raw in seasons:
            if not isinstance(raw, Mapping):
                continue
            number = _positive_int(raw.get("season_number"))
            if number <= 0:
                continue
            progress.checkpoint()
            values.append(dict(fetch_season(tmdb_id, number)))
        return tuple(values)


class SqlAlchemyWatchlistTaskRepository:
    """Local watchlist projection and basic TMDb-series publication."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def list_active_series(self, tmdb_id: str | None = None) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title, watching_status, force_ended,
                               watchlist_tmdb_status, last_air_date, last_episode_to_air_json
                        FROM media_metadata
                        WHERE item_type = 'Series'
                          AND (
                              :tmdb_id IS NOT NULL
                              OR EXISTS (
                                  SELECT 1
                                  FROM media_metadata AS season
                                  WHERE season.parent_series_tmdb_id = media_metadata.tmdb_id
                                    AND season.item_type = 'Season'
                                    AND season.season_number > 0
                                    AND season.watching_status IN ('Watching', 'Paused', 'Pending')
                                    AND EXISTS (
                                        SELECT 1
                                        FROM media_metadata AS episode
                                        WHERE episode.parent_series_tmdb_id = media_metadata.tmdb_id
                                          AND episode.item_type = 'Episode'
                                          AND episode.season_number = season.season_number
                                          AND episode.in_library IS TRUE
                                          AND episode.episode_number IS NOT NULL
                                    )
                              )
                          )
                          AND (:tmdb_id IS NULL OR tmdb_id = :tmdb_id)
                        ORDER BY tmdb_id
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def list_all_completed_series(self) -> Sequence[Mapping[str, Any]]:
        """Return every non-forced completed series for the revival scan.

        The low-frequency revival task intentionally includes Ended/Canceled
        TMDb rows; only the local watchlist status and force-ended fence limit
        the scan.  The add-all task uses the narrower Returning/unknown query
        below via ``list_completed_series``.
        """
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title, watching_status, force_ended,
                               watchlist_tmdb_status, last_air_date, last_episode_to_air_json,
                               asset_details_json
                        FROM media_metadata
                        WHERE item_type = 'Series'
                          AND watching_status = 'Completed'
                          AND COALESCE(force_ended, FALSE) IS FALSE
                        ORDER BY tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def list_completed_series(self, library_ids: Sequence[str] = ()) -> Sequence[Mapping[str, Any]]:
        normalized_libraries = tuple(dict.fromkeys(str(value).strip() for value in library_ids if str(value).strip()))
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title, watching_status, force_ended,
                               watchlist_tmdb_status, last_air_date, last_episode_to_air_json,
                               asset_details_json
                        FROM media_metadata
                        WHERE item_type = 'Series'
                          AND watching_status = 'Completed'
                          AND COALESCE(force_ended, FALSE) IS FALSE
                          AND (watchlist_tmdb_status IS NULL OR watchlist_tmdb_status = 'Returning Series')
                          AND (
                              :library_filter = FALSE
                              OR EXISTS (
                                  SELECT 1
                                  FROM jsonb_array_elements(
                                      COALESCE(media_metadata.asset_details_json, '[]'::jsonb)
                                  ) AS asset(value)
                                  WHERE asset.value->>'source_library_id' = ANY(CAST(:library_ids AS text[]))
                              )
                              OR EXISTS (
                                  SELECT 1
                                  FROM media_metadata AS episode
                                  CROSS JOIN LATERAL jsonb_array_elements(
                                      COALESCE(episode.asset_details_json, '[]'::jsonb)
                                  ) AS asset(value)
                                  WHERE episode.parent_series_tmdb_id = media_metadata.tmdb_id
                                    AND episode.item_type = 'Episode'
                                    AND asset.value->>'source_library_id' = ANY(CAST(:library_ids AS text[]))
                              )
                          )
                        ORDER BY tmdb_id
                        """
                    ),
                    {"library_filter": bool(normalized_libraries), "library_ids": list(normalized_libraries)},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def import_library_series(self, items: Sequence[Mapping[str, Any]]) -> int:
        imported = 0
        imported_ids: list[str] = []
        with self._unit_of_work() as uow:
            for raw in items:
                if not isinstance(raw, Mapping) or str(raw.get("Type") or "") != "Series":
                    continue
                providers = raw.get("ProviderIds")
                tmdb_id = str(providers.get("Tmdb") or "").strip() if isinstance(providers, Mapping) else ""
                emby_id = str(raw.get("Id") or "").strip()
                if not tmdb_id or not emby_id:
                    continue
                uow.session.execute(
                    text(
                        """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, in_library, emby_item_ids_json,
                            watching_status, title, created_at, last_updated_at
                        ) VALUES (
                            :tmdb_id, 'Series', TRUE, CAST(:emby_ids AS jsonb),
                            'Completed', :title, NOW(), NOW()
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            in_library = TRUE,
                            emby_item_ids_json = CASE
                                WHEN COALESCE(media_metadata.emby_item_ids_json, '[]'::jsonb)
                                     @> CAST(:emby_ids AS jsonb)
                                    THEN COALESCE(media_metadata.emby_item_ids_json, '[]'::jsonb)
                                ELSE COALESCE(media_metadata.emby_item_ids_json, '[]'::jsonb)
                                     || CAST(:emby_ids AS jsonb)
                            END,
                            watching_status = CASE
                                WHEN media_metadata.watching_status IS NULL
                                  OR media_metadata.watching_status = 'NONE'
                                    THEN 'Completed'
                                ELSE media_metadata.watching_status
                            END,
                            force_ended = CASE
                                WHEN media_metadata.watching_status IS NULL
                                  OR media_metadata.watching_status = 'NONE'
                                    THEN FALSE
                                ELSE media_metadata.force_ended
                            END,
                            paused_until = CASE
                                WHEN media_metadata.watching_status IS NULL
                                  OR media_metadata.watching_status = 'NONE'
                                    THEN NULL
                                ELSE media_metadata.paused_until
                            END,
                            watchlist_last_checked_at = CASE
                                WHEN media_metadata.watching_status IS NULL
                                  OR media_metadata.watching_status = 'NONE'
                                    THEN NOW()
                                ELSE media_metadata.watchlist_last_checked_at
                            END,
                            title = COALESCE(EXCLUDED.title, media_metadata.title),
                            last_updated_at = NOW()
                        """
                    ),
                    {
                        "tmdb_id": tmdb_id,
                        "emby_ids": json.dumps([emby_id]),
                        "title": str(raw.get("Name") or "").strip() or None,
                    },
                )
                imported += 1
                imported_ids.append(tmdb_id)
            if imported_ids:
                # A bulk import treats existing library episodes as already
                # collected.  Reset stale episode watchlist markers first,
                # then derive each season state from its actual in-library
                # count, matching the legacy import contract without a second
                # connection/commit per series.
                uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET watching_status = 'NONE', last_updated_at = NOW()
                        WHERE item_type = 'Episode'
                          AND parent_series_tmdb_id = ANY(CAST(:series_ids AS text[]))
                        """
                    ),
                    {"series_ids": imported_ids},
                )
                uow.session.execute(
                    text(
                        """
                        WITH counts AS (
                            SELECT
                                season.parent_series_tmdb_id,
                                season.season_number,
                                COUNT(episode.tmdb_id) FILTER (WHERE episode.in_library IS TRUE) AS collected
                            FROM media_metadata AS season
                            LEFT JOIN media_metadata AS episode
                              ON episode.parent_series_tmdb_id = season.parent_series_tmdb_id
                             AND episode.item_type = 'Episode'
                             AND episode.season_number = season.season_number
                            WHERE season.item_type = 'Season'
                              AND season.parent_series_tmdb_id = ANY(CAST(:series_ids AS text[]))
                            GROUP BY season.parent_series_tmdb_id, season.season_number
                        )
                        UPDATE media_metadata AS season
                        SET watching_status = CASE
                                WHEN COALESCE(season.force_ended, FALSE) THEN 'Completed'
                                WHEN COALESCE(season.total_episodes, 0) > 0
                                 AND counts.collected >= season.total_episodes THEN 'Completed'
                                ELSE 'Watching'
                            END,
                            paused_until = NULL,
                            last_updated_at = NOW()
                        FROM counts
                        WHERE season.item_type = 'Season'
                          AND season.parent_series_tmdb_id = counts.parent_series_tmdb_id
                          AND season.season_number = counts.season_number
                        """
                    ),
                    {"series_ids": imported_ids},
                )
            uow.commit()
        return imported

    def refresh_series(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        *,
        season_details: Sequence[Mapping[str, Any]] = (),
    ) -> bool:
        normalized_id = str(tmdb_id or "").strip()
        if not normalized_id:
            return False
        seasons = details.get("seasons")
        season_rows = [value for value in seasons if isinstance(value, Mapping)] if isinstance(seasons, list) else []
        total_episodes = _positive_int(details.get("number_of_episodes"))
        if total_episodes <= 0:
            total_episodes = sum(_positive_int(value.get("episode_count")) for value in season_rows)
        status = str(details.get("status") or "").strip() or None
        next_episode = details.get("next_episode_to_air")
        last_episode = details.get("last_episode_to_air")
        release_date = _date_value(details.get("first_air_date"))
        last_air_date = _date_value(details.get("last_air_date"))
        external_ids = details.get("external_ids")
        imdb_id = str(external_ids.get("imdb_id") or "").strip() or None if isinstance(external_ids, Mapping) else None
        runtime_values = details.get("episode_run_time")
        runtime = next(
            (
                value
                for value in (
                    _positive_int(item)
                    for item in (
                        runtime_values
                        if isinstance(runtime_values, Sequence) and not isinstance(runtime_values, str | bytes)
                        else (details.get("runtime"),)
                    )
                )
                if value > 0
            ),
            0,
        )
        with self._unit_of_work() as uow:
            current = (
                uow.session.execute(
                    text(
                        """
                        SELECT watching_status, force_ended
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": normalized_id},
                )
                .mappings()
                .first()
            )
            if current is None:
                return False
            previous_seasons = {
                _positive_int(row.get("season_number")): dict(row)
                for row in (
                    uow.session.execute(
                        text(
                            """
                            SELECT season_number, total_episodes
                            FROM media_metadata
                            WHERE parent_series_tmdb_id = :tmdb_id
                              AND item_type = 'Season'
                              AND season_number > 0
                            FOR UPDATE
                            """
                        ),
                        {"tmdb_id": normalized_id},
                    )
                    .mappings()
                    .all()
                )
                if _positive_int(row.get("season_number")) > 0
            }
            existing_status = str(current.get("watching_status") or "NONE")
            force_ended = bool(current.get("force_ended"))
            watching_status = _watchlist_series_status(
                existing_status,
                force_ended=force_ended,
                tmdb_status=status,
                next_episode=next_episode,
            )
            values = {
                "tmdb_id": normalized_id,
                "title": str(details.get("name") or details.get("title") or "").strip() or None,
                "original_title": str(details.get("original_name") or "").strip() or None,
                "original_language": str(details.get("original_language") or "").strip() or None,
                "overview": str(details.get("overview") or "").strip() or None,
                "tagline": str(details.get("tagline") or "").strip() or None,
                "release_date": release_date,
                "release_year": release_date.year if release_date is not None else None,
                "last_air_date": last_air_date,
                "poster_path": _image_path(details, "poster_path", "posters"),
                "backdrop_path": _image_path(details, "backdrop_path", "backdrops"),
                "logo_path": _image_path(details, "logo_path", "logos"),
                "thumb_path": str(details.get("thumb_path") or "").strip() or None,
                "homepage": str(details.get("homepage") or "").strip() or None,
                "runtime": runtime or None,
                "rating": _positive_float(details.get("vote_average")),
                "official_ratings": _official_ratings(details),
                "genres": _json_text(_metadata_objects(details.get("genres"))),
                "directors": _json_text(_series_directors(details)),
                "companies": _json_text(_metadata_objects(details.get("production_companies"), keep_logo=True)),
                "networks": _json_text(_metadata_objects(details.get("networks"), keep_logo=True)),
                "countries": _json_text(_country_values(details)),
                "keywords": _json_text(_keyword_values(details)),
                "imdb_id": imdb_id,
                "total_episodes": total_episodes,
                "watching_status": watching_status,
                "tmdb_status": status,
                "next_episode": json.dumps(next_episode, ensure_ascii=False)
                if isinstance(next_episode, Mapping)
                else None,
                "last_episode": json.dumps(last_episode, ensure_ascii=False)
                if isinstance(last_episode, Mapping)
                else None,
                "missing_info": json.dumps({}, ensure_ascii=False),
            }
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET title = COALESCE(:title, title),
                        original_title = COALESCE(:original_title, original_title),
                        original_language = COALESCE(:original_language, original_language),
                        overview = COALESCE(:overview, overview),
                        tagline = COALESCE(:tagline, tagline),
                        release_date = COALESCE(:release_date, release_date),
                        release_year = COALESCE(:release_year, release_year),
                        last_air_date = COALESCE(:last_air_date, last_air_date),
                        poster_path = COALESCE(:poster_path, poster_path),
                        backdrop_path = COALESCE(:backdrop_path, backdrop_path),
                        logo_path = COALESCE(:logo_path, logo_path),
                        thumb_path = COALESCE(:thumb_path, thumb_path),
                        homepage = COALESCE(:homepage, homepage),
                        runtime_minutes = COALESCE(:runtime, runtime_minutes),
                        rating = COALESCE(:rating, rating),
                        official_rating_json = COALESCE(CAST(:official_ratings AS jsonb), official_rating_json),
                        genres_json = COALESCE(CAST(:genres AS jsonb), genres_json),
                        directors_json = COALESCE(CAST(:directors AS jsonb), directors_json),
                        production_companies_json = COALESCE(CAST(:companies AS jsonb), production_companies_json),
                        networks_json = COALESCE(CAST(:networks AS jsonb), networks_json),
                        countries_json = COALESCE(CAST(:countries AS jsonb), countries_json),
                        keywords_json = COALESCE(CAST(:keywords AS jsonb), keywords_json),
                        imdb_id = COALESCE(:imdb_id, imdb_id),
                        total_episodes = CASE WHEN :total_episodes > 0 THEN :total_episodes ELSE total_episodes END,
                        watching_status = :watching_status,
                        watchlist_tmdb_status = :tmdb_status,
                        watchlist_next_episode_json = CAST(:next_episode AS jsonb),
                        last_episode_to_air_json = CAST(:last_episode AS jsonb),
                        watchlist_missing_info_json = CAST(:missing_info AS jsonb),
                        watchlist_last_checked_at = NOW(),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                    """
                ),
                values,
            )
            updated = int(getattr(result, "rowcount", 0) or 0) == 1
            if not updated:
                return False

            self._publish_seasons_and_episodes(
                uow,
                normalized_id,
                season_rows,
                season_details,
                next_episode=next_episode,
            )
            if existing_status == "Completed" and not force_ended:
                self._revive_eligible_seasons(
                    uow,
                    normalized_id,
                    details,
                    season_rows,
                    previous_seasons,
                )
            uow.commit()
            return True

    @staticmethod
    def _revive_eligible_seasons(
        uow: SqlAlchemyUnitOfWork,
        series_id: str,
        details: Mapping[str, Any],
        season_rows: Sequence[Mapping[str, Any]],
        previous_seasons: Mapping[int, Mapping[str, Any]],
    ) -> None:
        """Reopen a completed series only for a newly aired/near-term season.

        This keeps the old revival window and hard-ignore fence local to the
        projection.  It deliberately does not call a provider while the
        transaction is open; a later task/effect can consume the WANTED or
        PENDING_RELEASE projection.
        """
        today = datetime.now(UTC).date()
        if _has_animation_genre(details):
            allow_count_growth = True
        else:
            allow_count_growth = False
        for season in season_rows:
            season_number = _positive_int(season.get("season_number"))
            if season_number <= 0:
                continue
            expected = max(
                _positive_int(season.get("episode_count")),
                _positive_int(
                    next(
                        (
                            value.get("episode_count")
                            for value in details.get("seasons", ())
                            if isinstance(value, Mapping) and _positive_int(value.get("season_number")) == season_number
                        ),
                        0,
                    )
                ),
            )
            previous = previous_seasons.get(season_number)
            previous_total = _positive_int(previous.get("total_episodes")) if previous else 0
            is_new = previous is None
            is_count_growth = bool(previous and expected > previous_total)
            if not is_new and (not is_count_growth or not allow_count_growth):
                continue
            air_date = _date_value(season.get("air_date"))
            if air_date is None:
                raw_detail = next(
                    (
                        value
                        for value in details.get("seasons", ())
                        if isinstance(value, Mapping) and _positive_int(value.get("season_number")) == season_number
                    ),
                    {},
                )
                air_date = _date_value(raw_detail.get("air_date")) if isinstance(raw_detail, Mapping) else None
            if air_date is None:
                continue
            days_until = (air_date - today).days
            if days_until < -30 or days_until > 7:
                continue
            desired_status = "PENDING_RELEASE" if days_until > 0 else "WANTED"
            source = json.dumps(
                {
                    "type": "revived_season",
                    "reason": "watchlist_revival",
                    "series_tmdb_id": series_id,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = CASE
                            WHEN subscription_status = 'IGNORED'
                             AND NULLIF(BTRIM(ignore_reason), '') IS NOT NULL
                                THEN subscription_status
                            WHEN subscription_status IN ('WANTED', 'SUBSCRIBED', 'REQUESTED')
                                THEN subscription_status
                            ELSE :desired_status
                        END,
                        subscription_sources_json = CASE
                            WHEN subscription_status = 'IGNORED'
                             AND NULLIF(BTRIM(ignore_reason), '') IS NOT NULL
                                THEN COALESCE(subscription_sources_json, '[]'::jsonb)
                            WHEN COALESCE(subscription_sources_json, '[]'::jsonb)
                                 @> CAST(:source AS jsonb)
                                THEN COALESCE(subscription_sources_json, '[]'::jsonb)
                            ELSE COALESCE(subscription_sources_json, '[]'::jsonb)
                                 || CAST(:source AS jsonb)
                        END,
                        watching_status = CASE
                            WHEN COALESCE(force_ended, FALSE) THEN 'Completed'
                            ELSE 'Watching'
                        END,
                        last_updated_at = NOW()
                    WHERE parent_series_tmdb_id = :series_id
                      AND item_type = 'Season'
                      AND season_number = :season_number
                    """
                ),
                {
                    "desired_status": desired_status,
                    "source": f"[{source}]",
                    "series_id": series_id,
                    "season_number": season_number,
                },
            )

    def _publish_seasons_and_episodes(
        self,
        uow: SqlAlchemyUnitOfWork,
        series_id: str,
        season_rows: Sequence[Mapping[str, Any]],
        season_details: Sequence[Mapping[str, Any]],
        *,
        next_episode: Any,
    ) -> None:
        detail_by_season = {
            _positive_int(value.get("season_number")): value
            for value in season_details
            if isinstance(value, Mapping) and _positive_int(value.get("season_number")) > 0
        }
        for season in season_rows:
            season_number = _positive_int(season.get("season_number"))
            if season_number <= 0:
                continue
            detail = detail_by_season.get(season_number, {})
            raw_episodes = detail.get("episodes") if isinstance(detail, Mapping) else None
            episodes = (
                tuple(value for value in raw_episodes if isinstance(value, Mapping))
                if isinstance(raw_episodes, list)
                else ()
            )
            season_id = str(season.get("id") or f"{series_id}:S{season_number}").strip()
            existing_season = (
                uow.session.execute(
                    text(
                        """
                        SELECT total_episodes, total_episodes_locked
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Season'
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": season_id},
                )
                .mappings()
                .first()
            )
            proposed_expected = _positive_int(season.get("episode_count"))
            if proposed_expected <= 0 and isinstance(detail, Mapping):
                proposed_expected = _positive_int(detail.get("episode_count"))
            episode_numbers = tuple(
                sorted(
                    {
                        number
                        for number in (_positive_int(value.get("episode_number")) for value in episodes)
                        if number > 0
                    }
                )
            )
            proposed_expected = max(proposed_expected, max(episode_numbers, default=0))
            locked_total = (
                _positive_int(existing_season.get("total_episodes"))
                if existing_season and bool(existing_season.get("total_episodes_locked"))
                else 0
            )
            expected = locked_total or proposed_expected
            uow.session.execute(
                text(
                    """
                    INSERT INTO media_metadata (
                        tmdb_id, item_type, parent_series_tmdb_id, season_number,
                        title, original_title, overview, release_date, poster_path, backdrop_path,
                        total_episodes, watching_status, in_library, created_at, last_updated_at
                    ) VALUES (
                        :tmdb_id, 'Season', :series_id, :season_number,
                        :title, :original_title, :overview, :release_date, :poster_path, :backdrop_path,
                        :total_episodes, 'Watching', FALSE, NOW(), NOW()
                    )
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                        parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                        season_number = EXCLUDED.season_number,
                        title = COALESCE(EXCLUDED.title, media_metadata.title),
                        original_title = COALESCE(EXCLUDED.original_title, media_metadata.original_title),
                        overview = COALESCE(EXCLUDED.overview, media_metadata.overview),
                        release_date = COALESCE(EXCLUDED.release_date, media_metadata.release_date),
                        poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
                        backdrop_path = COALESCE(EXCLUDED.backdrop_path, media_metadata.backdrop_path),
                        total_episodes = CASE
                            WHEN COALESCE(media_metadata.total_episodes_locked, FALSE)
                                THEN media_metadata.total_episodes
                            WHEN EXCLUDED.total_episodes > 0 THEN EXCLUDED.total_episodes
                            ELSE media_metadata.total_episodes
                        END,
                        last_updated_at = NOW()
                    """
                ),
                {
                    "tmdb_id": season_id,
                    "series_id": series_id,
                    "season_number": season_number,
                    "title": str(season.get("name") or detail.get("name") or "").strip() or None,
                    "original_title": str(season.get("original_name") or "").strip() or None,
                    "overview": str(season.get("overview") or detail.get("overview") or "").strip() or None,
                    "release_date": _date_value(season.get("air_date") or detail.get("air_date")),
                    "poster_path": str(season.get("poster_path") or detail.get("poster_path") or "").strip() or None,
                    "backdrop_path": str(season.get("backdrop_path") or detail.get("backdrop_path") or "").strip()
                    or None,
                    "total_episodes": expected,
                },
            )
            for episode in episodes:
                episode_number = _positive_int(episode.get("episode_number"))
                if episode_number <= 0:
                    continue
                episode_id = str(episode.get("id") or f"{season_id}:E{episode_number}").strip()
                uow.session.execute(
                    text(
                        """
                        INSERT INTO media_metadata (
                            tmdb_id, item_type, parent_series_tmdb_id, season_number, episode_number,
                            title, original_title, overview, release_date, poster_path, runtime_minutes,
                            in_library, created_at, last_updated_at
                        ) VALUES (
                            :tmdb_id, 'Episode', :series_id, :season_number, :episode_number,
                            :title, :original_title, :overview, :release_date, :poster_path, :runtime,
                            FALSE, NOW(), NOW()
                        )
                        ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                            parent_series_tmdb_id = EXCLUDED.parent_series_tmdb_id,
                            season_number = EXCLUDED.season_number,
                            episode_number = EXCLUDED.episode_number,
                            title = COALESCE(EXCLUDED.title, media_metadata.title),
                            original_title = COALESCE(EXCLUDED.original_title, media_metadata.original_title),
                            overview = COALESCE(EXCLUDED.overview, media_metadata.overview),
                            release_date = COALESCE(EXCLUDED.release_date, media_metadata.release_date),
                            poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
                            runtime_minutes = COALESCE(EXCLUDED.runtime_minutes, media_metadata.runtime_minutes),
                            last_updated_at = NOW()
                        """
                    ),
                    {
                        "tmdb_id": episode_id,
                        "series_id": series_id,
                        "season_number": season_number,
                        "episode_number": episode_number,
                        "title": str(episode.get("name") or "").strip() or None,
                        "original_title": str(episode.get("original_name") or "").strip() or None,
                        "overview": str(episode.get("overview") or "").strip() or None,
                        "release_date": _date_value(episode.get("air_date")),
                        "poster_path": (
                            str(episode.get("still_path") or episode.get("poster_path") or "").strip() or None
                        ),
                        "runtime": _positive_int(episode.get("runtime")) or None,
                    },
                )

        local_rows = (
            uow.session.execute(
                text(
                    """
                    SELECT season_number, episode_number, in_library
                    FROM media_metadata
                    WHERE parent_series_tmdb_id = :series_id AND item_type = 'Episode'
                    ORDER BY season_number, episode_number
                    """
                ),
                {"series_id": series_id},
            )
            .mappings()
            .all()
        )
        collected_numbers: dict[int, set[int]] = {}
        for row in local_rows:
            season_number = _positive_int(row.get("season_number"))
            episode_number = _positive_int(row.get("episode_number"))
            if season_number > 0 and episode_number > 0 and bool(row.get("in_library")):
                collected_numbers.setdefault(season_number, set()).add(episode_number)
        missing_seasons: list[dict[str, Any]] = []
        detail_by_season = {
            _positive_int(value.get("season_number")): value
            for value in season_details
            if isinstance(value, Mapping) and _positive_int(value.get("season_number")) > 0
        }
        for season in season_rows:
            number = _positive_int(season.get("season_number"))
            if number <= 0:
                continue
            detail = detail_by_season.get(number, {})
            raw_episodes = detail.get("episodes") if isinstance(detail, Mapping) else None
            episode_numbers = tuple(
                sorted(
                    {
                        episode_number
                        for episode_number in (
                            _positive_int(value.get("episode_number"))
                            for value in raw_episodes
                            if isinstance(value, Mapping)
                        )
                        if episode_number > 0
                    }
                )
                if isinstance(raw_episodes, list)
                else ()
            )
            expected = max(
                _positive_int(season.get("episode_count")),
                _positive_int(detail.get("episode_count")) if isinstance(detail, Mapping) else 0,
                max(episode_numbers, default=0),
            )
            if number <= 0 or expected <= 0:
                continue
            collected_set = collected_numbers.get(number, set())
            missing_numbers = list(_missing_episode_numbers(expected, collected_set))
            count = len(collected_set)
            if missing_numbers:
                missing_seasons.append(
                    {
                        "season_number": number,
                        "expected": expected,
                        "collected": count,
                        "missing": len(missing_numbers),
                    }
                )
            uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET watching_status = CASE
                            WHEN COALESCE(force_ended, FALSE) THEN 'Completed'
                            WHEN :expected > 0 AND :collected >= :expected THEN 'Completed'
                            WHEN :has_next THEN 'Pending'
                            ELSE 'Watching'
                        END,
                        watchlist_missing_info_json = CAST(:missing_info AS jsonb),
                        last_updated_at = NOW()
                    WHERE parent_series_tmdb_id = :series_id
                      AND item_type = 'Season'
                      AND season_number = :season_number
                    """
                ),
                {
                    "series_id": series_id,
                    "season_number": number,
                    "expected": expected,
                    "collected": count,
                    "has_next": isinstance(next_episode, Mapping),
                    "missing_info": json.dumps(
                        {"missing_episodes": missing_numbers} if missing_numbers else {},
                        ensure_ascii=False,
                    ),
                },
            )
        uow.session.execute(
            text(
                """
                UPDATE media_metadata
                SET watchlist_missing_info_json = CAST(:missing_info AS jsonb),
                    last_updated_at = NOW()
                WHERE tmdb_id = :series_id AND item_type = 'Series'
                """
            ),
            {
                "series_id": series_id,
                "missing_info": json.dumps({"missing_seasons": missing_seasons}, ensure_ascii=False),
            },
        )


class NativeWatchlistMoviePilotGateway:
    """Synchronize the active local season through the durable MoviePilot ledger."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: WatchlistSettingsPort,
        provider: WatchlistMoviePilotReadPort,
        effects: WatchlistMoviePilotEffectPort,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._provider = provider
        self._effects = effects
        self._clock = clock or (lambda: datetime.now(UTC))

    def sync_series(
        self,
        tmdb_id: str,
        candidate: Mapping[str, Any],
        details: Mapping[str, Any],
        season_details: Sequence[Mapping[str, Any]],
        parameters: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]:
        del candidate
        configuration = _watchlist_assistant_configuration(self._settings)
        if not configuration["enabled"]:
            return {"skipped": True, "reason": configuration["disabled_reason"]}

        local = self._local_context(tmdb_id)
        seasons = local["seasons"]
        if not seasons:
            return {"skipped": True, "reason": "no_local_progress"}
        remote_seasons = tuple(
            dict(value)
            for value in _mapping_sequence(details.get("seasons"))
            if _positive_int(value.get("season_number")) in seasons
        )
        if not remote_seasons:
            return {"skipped": True, "reason": "no_matching_remote_season"}

        checkpoint()
        listed = self._provider.list_subscriptions()
        provider_subscriptions = _moviepilot_subscriptions(listed)
        matching = tuple(
            value
            for value in provider_subscriptions
            if str(value.get("tmdbid") or value.get("tmdb_id") or "").strip() == tmdb_id
            and _positive_int(value.get("season")) > 0
        )
        by_season = {_positive_int(value.get("season")): dict(value) for value in matching}
        latest = max(_positive_int(value.get("season_number")) for value in remote_seasons)
        targets = tuple(
            value
            for value in remote_seasons
            if _positive_int(value.get("season_number")) == latest
            or _positive_int(value.get("season_number")) in by_season
        )
        episodes = _season_episodes(season_details)
        real_next_episode = _real_next_episode(episodes, seasons)
        final_status = str(local["series"].get("watching_status") or "Watching")
        stats: dict[str, Any] = {
            "provider_disposition": "empty" if not provider_subscriptions else "found",
            "targeted": len(targets),
            "created": 0,
            "updated": 0,
            "snapshots": 0,
            "full_washing": 0,
            "manual_skipped": 0,
        }
        for season_info in targets:
            checkpoint()
            season = _positive_int(season_info.get("season_number"))
            local_season = seasons[season]
            expected = _positive_int(local_season.get("total_episodes")) or _positive_int(
                season_info.get("episode_count")
            )
            local_count = _positive_int(local_season.get("local_count"))
            local_completed = expected > 0 and local_count >= expected
            signal = _watchlist_completion_signal(
                tmdb_id,
                season,
                details,
                episodes,
                configuration,
            )
            decision = _watchlist_moviepilot_decision(
                final_status="Completed" if local_completed else final_status,
                details=details,
                season=season,
                episodes=tuple(value for value in episodes if _positive_int(value.get("season_number")) == season),
                real_next_episode=real_next_episode,
                signal=signal,
                configuration=configuration,
                today=self._clock().date(),
            )
            subscription = by_season.get(season)
            created = False
            completed_full_washing = bool(
                not local_completed and signal.completed and expected > 0 and 0 < local_count < expected
            )
            if (
                subscription is None
                and season == latest
                and final_status in {"Watching", "Paused", "Pending"}
                and not local_completed
            ):
                if bool(local_season.get("virtual")):
                    continue
                payload = {
                    "name": str(local["series"].get("title") or details.get("name") or tmdb_id),
                    "tmdbid": _required_tmdb_int(tmdb_id),
                    "type": "电视剧",
                    "season": season,
                }
                _apply_moviepilot_washing(payload, configuration, force_full=completed_full_washing)
                if not bool(payload.get("best_version_full")):
                    start_episode = _positive_int(local_season.get("local_max_episode")) + 1
                    if start_episode > 1:
                        payload["start_episode"] = start_episode
                self._create(payload, tmdb_id=tmdb_id, season=season)
                stats["created"] += 1
                created = True
                checkpoint()
                subscription = _moviepilot_query_subscription(
                    self._provider.query_subscription(tmdb_id, season=season)
                ) or dict(payload)
                by_season[season] = dict(subscription)
            if subscription is None:
                continue

            subscribe_id = _positive_int(subscription.get("id") or subscription.get("subscribe_id"))
            if subscribe_id and self._recent_manual_state_change(subscribe_id):
                stats["manual_skipped"] += 1
                continue
            total = (
                _positive_int(configuration.get("pending_fake_total_episodes")) or 99
                if decision["mp_state"] == "P"
                else expected or None
            )
            if not (created and decision["mp_state"] == "R"):
                self._remember_expected_update(subscribe_id, decision["mp_state"], total)
                self._update_status(
                    tmdb_id,
                    season,
                    str(decision["mp_state"]),
                    total,
                )
                stats["updated"] += 1

            full_washing = completed_full_washing or bool(subscription.get("best_version_full") == 1)
            if bool(decision.get("snapshot")):
                full_washing = self._sync_completed_washing(
                    tmdb_id,
                    season,
                    subscription,
                    expected,
                    signal,
                    configuration,
                )
                if self._upsert_snapshot(tmdb_id, season, subscribe_id, expected, signal, subscription):
                    stats["snapshots"] += 1
            elif completed_full_washing:
                full_washing = self._ensure_full_washing(
                    tmdb_id,
                    season,
                    subscription,
                    configuration,
                )
            if full_washing:
                self._mark_full_washing(tmdb_id, season, subscribe_id, subscription)
                stats["full_washing"] += 1
        return stats

    def _local_context(self, tmdb_id: str) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            series = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, title, watching_status, force_ended
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT season.season_number, season.total_episodes,
                               season.total_episodes_locked, season.watching_status,
                               season.active_washing,
                               COUNT(DISTINCT episode.episode_number)
                                   FILTER (WHERE episode.in_library IS TRUE) AS local_count,
                               MAX(episode.episode_number)
                                   FILTER (WHERE episode.in_library IS TRUE) AS local_max_episode,
                               ARRAY_AGG(DISTINCT episode.episode_number ORDER BY episode.episode_number)
                                   FILTER (WHERE episode.in_library IS TRUE
                                           AND episode.episode_number IS NOT NULL) AS local_episode_numbers,
                               EXISTS (
                                   SELECT 1
                                   FROM shared_virtual_imports AS virtual
                                   WHERE virtual.status IN ('virtual', 'promoting')
                                     AND LOWER(virtual.item_type) IN ('series', 'season', 'episode', 'tv')
                                     AND (virtual.tmdb_id = :tmdb_id OR virtual.parent_series_tmdb_id = :tmdb_id)
                                     AND (virtual.season_number = season.season_number
                                          OR virtual.season_number IS NULL)
                               ) AS virtual
                        FROM media_metadata AS season
                        LEFT JOIN media_metadata AS episode
                          ON episode.parent_series_tmdb_id = season.parent_series_tmdb_id
                         AND episode.item_type = 'Episode'
                         AND episode.season_number = season.season_number
                        WHERE season.parent_series_tmdb_id = :tmdb_id
                          AND season.item_type = 'Season'
                          AND season.season_number > 0
                          AND EXISTS (
                              SELECT 1
                              FROM media_metadata AS local_episode
                              WHERE local_episode.parent_series_tmdb_id = :tmdb_id
                                AND local_episode.item_type = 'Episode'
                                AND local_episode.season_number = season.season_number
                                AND local_episode.in_library IS TRUE
                                AND local_episode.episode_number IS NOT NULL
                          )
                        GROUP BY season.tmdb_id, season.season_number, season.total_episodes,
                                 season.total_episodes_locked, season.watching_status,
                                 season.active_washing
                        ORDER BY season.season_number
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .all()
            )
        return {
            "series": dict(series or {}),
            "seasons": {
                _positive_int(row.get("season_number")): dict(row)
                for row in rows
                if _positive_int(row.get("season_number")) > 0
            },
        }

    def _create(self, payload: Mapping[str, Any], *, tmdb_id: str, season: int) -> None:
        from etk.processes.p7_subscription_effects import bind_moviepilot_operation_occurrence

        occurrence = _watchlist_occurrence("create", tmdb_id, season, payload)
        with bind_moviepilot_operation_occurrence(occurrence):
            confirmed = self._effects.create_subscription(
                payload,
                consume_quota=True,
                apply_local_continuation=False,
            )
        if not confirmed:
            raise RuntimeError("MoviePilot watchlist subscription create was rejected")

    def _update_status(
        self,
        tmdb_id: str,
        season: int,
        status: str,
        total: int | None,
    ) -> None:
        from etk.processes.p7_subscription_effects import bind_moviepilot_operation_occurrence

        payload = {"status": status, "total_episodes": total}
        with bind_moviepilot_operation_occurrence(_watchlist_occurrence("status", tmdb_id, season, payload)):
            confirmed = self._effects.update_subscription_status(
                tmdb_id,
                season=season,
                status=status,
                total_episodes=total,
            )
        if not confirmed:
            raise RuntimeError("MoviePilot watchlist status update was rejected")

    def _ensure_full_washing(
        self,
        tmdb_id: str,
        season: int,
        subscription: Mapping[str, Any],
        configuration: Mapping[str, Any],
    ) -> bool:
        if str(configuration.get("best_version_type") or "").strip().lower() not in {
            "tv_episode",
            "completed_full",
            "tv",
            "all",
        }:
            return False
        if _positive_int(subscription.get("best_version_full")) == 1:
            return True
        payload = dict(subscription)
        if not payload.get("id"):
            return False
        payload.update(
            {
                "tmdbid": _required_tmdb_int(tmdb_id),
                "season": season,
                "name": payload.get("name") or tmdb_id,
                "type": payload.get("type") or "电视剧",
                "best_version": 1,
                "best_version_full": 1,
                "include": "",
            }
        )
        from etk.processes.p7_subscription_effects import bind_moviepilot_operation_occurrence

        with bind_moviepilot_operation_occurrence(_watchlist_occurrence("full-washing", tmdb_id, season, payload)):
            confirmed = self._effects.update_subscription(payload)
        if not confirmed:
            raise RuntimeError("MoviePilot full-washing update was rejected")
        return True

    def _sync_completed_washing(
        self,
        tmdb_id: str,
        season: int,
        subscription: Mapping[str, Any],
        expected: int,
        signal: Any,
        configuration: Mapping[str, Any],
    ) -> bool:
        mode = str(configuration.get("best_version_type") or "").strip().lower()
        if mode not in {"tv_episode", "completed_full"}:
            return _positive_int(subscription.get("best_version_full")) == 1
        if mode == "tv_episode" and bool(configuration.get("best_version_full_consistency_check_enabled")):
            consistent = self._season_consistency(tmdb_id, season, expected)
            if consistent and not bool(configuration.get("best_version_episode_to_full")):
                subscribe_id = _positive_int(subscription.get("id") or subscription.get("subscribe_id"))
                if not subscribe_id:
                    return False
                self._mark_episode_washing_delete_pending(
                    tmdb_id,
                    season,
                    subscribe_id,
                    configuration,
                )
                from etk.processes.p7_subscription_effects import bind_moviepilot_operation_occurrence

                with bind_moviepilot_operation_occurrence(
                    _watchlist_occurrence("delete-consistent", tmdb_id, season, {"id": subscribe_id})
                ):
                    confirmed = self._effects.delete_subscription_by_id(subscribe_id)
                if not confirmed:
                    self._clear_episode_washing_delete_pending(tmdb_id, season, subscribe_id)
                    raise RuntimeError("MoviePilot consistent episode-washing delete was rejected")
                self._finalize_episode_washing_delete(
                    tmdb_id,
                    season,
                    subscribe_id,
                    subscription,
                )
                return False
        return self._ensure_full_washing(tmdb_id, season, subscription, configuration)

    def _mark_episode_washing_delete_pending(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        configuration: Mapping[str, Any],
    ) -> None:
        now = self._clock().timestamp()
        retention_days = _positive_int(configuration.get("snapshot_retention_days")) or 180
        with self._unit_of_work() as uow:
            suppressions = _locked_json_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
            for key, raw in tuple(suppressions.items()):
                value = _mapping_value(raw)
                if (_positive_float(value.get("expires_at")) or 0.0) <= now:
                    suppressions.pop(key, None)
            for key in _watchlist_suppression_keys(tmdb_id, season, subscribe_id):
                suppressions[key] = {
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "subscribe_id": subscribe_id,
                    "reason": "episode_washing_consistency_ok",
                    "created_at": now,
                    "expires_at": now + retention_days * 86_400,
                }
            _write_json_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()

    def _clear_episode_washing_delete_pending(self, tmdb_id: str, season: int, subscribe_id: int) -> None:
        with self._unit_of_work() as uow:
            suppressions = _locked_json_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
            for key in _watchlist_suppression_keys(tmdb_id, season, subscribe_id):
                suppressions.pop(key, None)
            _write_json_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()

    def _finalize_episode_washing_delete(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        subscription: Mapping[str, Any],
    ) -> None:
        now = self._clock().timestamp()
        with self._unit_of_work() as uow:
            subscriptions = _locked_json_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            item = _mapping_value(subscriptions.get(str(subscribe_id)))
            item.update(
                {
                    "subscribe_id": subscribe_id,
                    "subscribe_info": dict(subscription) or _mapping_value(item.get("subscribe_info")),
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "deleted": True,
                    "active_sources": {},
                    "snapshot_restore_suppressed": True,
                    "last_event": "episode_washing_consistency_ok",
                    "last_reason": "episode_washing_consistency_ok",
                    "updated_at": now,
                }
            )
            item.pop("full_washing", None)
            item.pop("full_washing_started_at", None)
            item.pop("washing_timeout_started_at", None)
            subscriptions[str(subscribe_id)] = item
            torrents = _locked_json_state(uow, _ASSISTANT_TORRENTS_KEY)
            for key, raw in tuple(torrents.items()):
                if _positive_int(_mapping_value(raw).get("subscribe_id")) == subscribe_id:
                    torrents.pop(key, None)
            _write_json_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            _write_json_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
            _set_watchlist_active_washing(uow, tmdb_id, season, False)
            uow.commit()

    def _season_consistency(self, tmdb_id: str, season: int, expected: int) -> bool:
        if expected <= 0:
            return False
        with self._unit_of_work() as uow:
            rows = tuple(
                uow.session.execute(
                    text(
                        """
                        SELECT asset_details_json
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = :tmdb_id
                          AND season_number = :season
                          AND item_type = 'Episode'
                          AND in_library IS TRUE
                        ORDER BY episode_number NULLS LAST, tmdb_id
                        """
                    ),
                    {"tmdb_id": tmdb_id, "season": season},
                ).scalars()
            )
        return _watchlist_assets_consistent(rows, expected)

    def _recent_manual_state_change(self, subscribe_id: int) -> bool:
        with self._unit_of_work() as uow:
            state = uow.session.scalar(
                text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = 'subscribes'")
            )
        item = state.get(str(subscribe_id)) if isinstance(state, Mapping) else None
        change = item.get("last_manual_change") if isinstance(item, Mapping) else None
        if not isinstance(change, Mapping) or "state" not in _string_values(change.get("fields")):
            return False
        updated_at = _positive_float(change.get("updated_at")) or 0.0
        return updated_at > 0 and self._clock().timestamp() - updated_at <= 3_600

    def _remember_expected_update(self, subscribe_id: int, state: str, total: int | None) -> None:
        if subscribe_id <= 0:
            return
        with self._unit_of_work() as uow:
            values = _locked_json_state(uow, "subscribes")
            item = dict(values.get(str(subscribe_id)) or {})
            item["expected_mp_update"] = {
                "fields": ["state", "total_episode"] if total else ["state"],
                "state": state,
                "total_episode": total,
                "updated_at": self._clock().timestamp(),
            }
            values[str(subscribe_id)] = item
            _write_json_state(uow, "subscribes", values)
            uow.commit()

    def _mark_full_washing(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        subscription: Mapping[str, Any],
    ) -> None:
        with self._unit_of_work() as uow:
            _set_watchlist_active_washing(uow, tmdb_id, season, True)
            if subscribe_id > 0:
                values = _locked_json_state(uow, "subscribes")
                item = dict(values.get(str(subscribe_id)) or {})
                item.update(
                    {
                        "subscribe_id": subscribe_id,
                        "subscribe_info": dict(subscription),
                        "tmdb_id": tmdb_id,
                        "season": season,
                        "full_washing": True,
                        "full_washing_started_at": _positive_float(item.get("full_washing_started_at"))
                        or self._clock().timestamp(),
                        "last_reason": "watchlist_full_washing",
                        "updated_at": self._clock().timestamp(),
                    }
                )
                values[str(subscribe_id)] = item
                _write_json_state(uow, "subscribes", values)
            uow.commit()

    def _set_active_washing(self, tmdb_id: str, season: int, enabled: bool) -> None:
        with self._unit_of_work() as uow:
            _set_watchlist_active_washing(uow, tmdb_id, season, enabled)
            uow.commit()

    def _upsert_snapshot(
        self,
        tmdb_id: str,
        season: int,
        subscribe_id: int,
        expected: int,
        signal: Any,
        subscription: Mapping[str, Any],
    ) -> bool:
        scope = {
            "season": season,
            "total": expected or _positive_int(getattr(signal, "scope_total", 0)),
            "high_risk": bool(getattr(signal, "scope_high_risk", False)),
            "signals": list(getattr(signal, "signals", ()) or ()),
        }
        scope_json = canonical_json(scope, field="watchlist snapshot scope")
        with self._unit_of_work() as uow:
            existing = uow.session.scalar(
                text(
                    """
                    SELECT id
                    FROM subscribe_assistant_snapshots
                    WHERE tmdb_id = :tmdb_id
                      AND item_type = 'Series'
                      AND season_number = :season
                      AND subscribe_id IS NOT DISTINCT FROM :subscribe_id
                      AND scope_json = CAST(:scope AS jsonb)
                    ORDER BY id DESC
                    LIMIT 1
                    FOR UPDATE
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "subscribe_id": subscribe_id or None,
                    "scope": scope_json,
                },
            )
            if existing is not None:
                uow.rollback()
                return False
            uow.session.execute(
                text(
                    """
                    INSERT INTO subscribe_assistant_snapshots (
                        tmdb_id, item_type, season_number, subscribe_id,
                        scope_total, scope_json, subscribe_json, created_at
                    ) VALUES (
                        :tmdb_id, 'Series', :season, :subscribe_id,
                        :scope_total, CAST(:scope AS jsonb), CAST(:subscribe AS jsonb), NOW()
                    )
                    """
                ),
                {
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "subscribe_id": subscribe_id or None,
                    "scope_total": scope["total"],
                    "scope": scope_json,
                    "subscribe": canonical_json(dict(subscription), field="watchlist subscription snapshot"),
                },
            )
            uow.commit()
        return True


class NativeWatchlistP115ReclassificationGateway:
    """Prepare existing native 115 organize executions on Series state changes."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        plans: WatchlistP115PlanPort,
        executions: WatchlistP115ExecutionPort,
        execution_repository: WatchlistP115ExecutionRepositoryPort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._plans = plans
        self._executions = executions
        self._execution_repository = execution_repository

    def stage_reclassification(
        self,
        tmdb_id: str,
        candidate: Mapping[str, Any],
        details: Mapping[str, Any],
        checkpoint: Callable[[], None],
    ) -> Mapping[str, Any]:
        old_status = str(candidate.get("watching_status") or "NONE")
        new_status = self._series_status(tmdb_id)
        if not _watchlist_reclassification_transition(old_status, new_status):
            return {"skipped": True, "reason": "status_unchanged", "old": old_status, "new": new_status}
        rows = self._records(tmdb_id)

        direct = sum(1 for row in rows if str(row.get("category_name") or "").strip() == "MP直出")
        candidates = tuple(
            row
            for row in rows
            if str(row.get("category_name") or "").strip() != "MP直出"
            and str(row.get("file_id") or "").strip()
            and str(row.get("parent_id") or "").strip()
        )
        if not candidates:
            return {"skipped": True, "reason": "no_candidates", "mp_direct_skipped": direct}
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for row in candidates:
            grouped.setdefault(str(row["parent_id"]), []).append(row)
        title = str(details.get("name") or candidate.get("title") or tmdb_id)
        prepared: list[dict[str, Any]] = []
        unchanged_groups = 0
        for parent_id, group in grouped.items():
            checkpoint()
            file_ids = tuple(str(row["file_id"]) for row in group)
            intent = _watchlist_p115_intent(tmdb_id, new_status, parent_id, file_ids)
            existing = self._execution_repository.find_by_intent(intent)
            if existing is not None:
                prepared.append(
                    {
                        "execution_id": str(existing.execution_id),
                        "plan_id": str(existing.plan_id),
                        "reused": True,
                    }
                )
                continue
            hints = {
                str(row["file_id"]): {
                    "title": title,
                    "media_type": "tv",
                    "tmdb_id": tmdb_id,
                    "season_number": _positive_int(row.get("season_number")) or None,
                    "episode_number": _episode_number_from_name(row.get("original_name")),
                }
                for row in group
            }
            plan_result = self._plans.create(
                {
                    "source_cid": parent_id,
                    "source_name": title,
                    "source_file_ids": list(file_ids),
                    "request_intent_id": intent,
                    "recognition_hints": hints,
                }
            )
            plan = _ready_watchlist_p115_plan(plan_result)
            if not plan:
                unchanged_groups += 1
                continue
            checkpoint()
            reservation = self._executions.prepare(
                str(plan["plan_id"]),
                intent,
                trace_id=f"watchlist:{tmdb_id}:{new_status}",
            )
            prepared.append(
                {
                    "execution_id": str(reservation.execution.execution_id),
                    "plan_id": str(reservation.execution.plan_id),
                    "job_id": str(reservation.execution.job_id),
                    "reused": not bool(reservation.created),
                }
            )
        return {
            "old": old_status,
            "new": new_status,
            "prepared": prepared,
            "unchanged_groups": unchanged_groups,
            "mp_direct_skipped": direct,
        }

    def _records(self, tmdb_id: str) -> tuple[Mapping[str, Any], ...]:
        with self._unit_of_work() as uow:
            rows = tuple(
                dict(row)
                for row in (
                    uow.session.execute(
                        text(
                            """
                            SELECT record.file_id, record.original_name, record.season_number,
                                   record.category_name, cache.parent_id
                            FROM p115_organize_records AS record
                            LEFT JOIN p115_filesystem_cache AS cache ON cache.id = record.file_id
                            WHERE record.tmdb_id = :tmdb_id
                            ORDER BY cache.parent_id, record.file_id
                            """
                        ),
                        {"tmdb_id": tmdb_id},
                    )
                    .mappings()
                    .all()
                )
            )
        return rows

    def _series_status(self, tmdb_id: str) -> str:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT watching_status FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = 'Series'"),
                {"tmdb_id": tmdb_id},
            )
        return str(value or "NONE")


class NativeWatchlistSharedMetadataGateway:
    """Reserve one no-blind-replay Shared Center display metadata mutation."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: WatchlistSettingsPort,
        operations: WatchlistSharingOperationRepositoryPort,
        principal_id: Callable[[], str],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._operations = operations
        self._principal_id = principal_id

    def stage(
        self,
        tmdb_id: str,
        details: Mapping[str, Any],
        season_details: Sequence[Mapping[str, Any]],
        *,
        skip_logical_share_dispatch: bool,
    ) -> Mapping[str, Any]:
        configuration = _mapping_value(self._settings.load("shared_resource_config"))
        if not _bool_value(configuration.get("p115_shared_resource_enabled")):
            return {"skipped": True, "reason": "shared_resource_disabled"}
        context = self._metadata_context(tmdb_id)
        items = _shared_watchlist_metadata_items(
            tmdb_id,
            details,
            season_details,
            context,
            configuration=_mapping_value(self._settings.load("watchlist_config")),
        )
        if not items:
            return {"skipped": True, "reason": "empty_items"}
        payload = {
            "skip_logical_share_dispatch": bool(skip_logical_share_dispatch),
            "items": [
                {
                    "display_meta_items_json": items,
                    "display_meta_json": items[-1],
                    "_reason": "watchlist_series_metadata_with_episode_total",
                    "skip_logical_share_dispatch": bool(skip_logical_share_dispatch),
                }
            ],
        }
        from etk.domains.sharing.operations import SharingOperationIntent, SharingOperationKind

        digest = hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()
        intent = SharingOperationIntent.create(
            kind=SharingOperationKind.DISPLAY_METADATA,
            scope_key=f"watchlist-display:{tmdb_id}",
            principal_id=self._principal_id(),
            request={
                "provider": "shared_center",
                "action": "upsert_display_metadata",
                "payload": payload,
            },
            idempotency_key=f"watchlist-display:{tmdb_id}:{digest}",
        )
        operation, replayed = self._operations.reserve_with_replay(intent)
        return {
            "operation_id": str(operation.id),
            "status": operation.status.value,
            "replayed": replayed,
            "items": len(items),
        }

    def _metadata_context(self, tmdb_id: str) -> Mapping[str, Any]:
        with self._unit_of_work() as uow:
            series = (
                uow.session.execute(
                    text(
                        """
                        SELECT title, watching_status
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
                .mappings()
                .one_or_none()
            )
            seasons = tuple(
                dict(row)
                for row in (
                    uow.session.execute(
                        text(
                            """
                            SELECT season_number, watching_status, total_episodes, total_episodes_locked
                            FROM media_metadata
                            WHERE parent_series_tmdb_id = :tmdb_id
                              AND item_type = 'Season'
                              AND season_number > 0
                            ORDER BY season_number
                            """
                        ),
                        {"tmdb_id": tmdb_id},
                    )
                    .mappings()
                    .all()
                )
            )
        return {"series": dict(series or {}), "seasons": seasons}


class HttpxWatchlistMetadataAdapter:
    """Read-only TMDb television details for watchlist refreshes."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def series_details(self, tmdb_id: str) -> Mapping[str, Any]:
        normalized = str(tmdb_id or "").strip()
        if not normalized.isdigit():
            raise ValueError("watchlist TMDb series identity is invalid")
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise RuntimeError("TMDb is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/tv/{quote(normalized, safe='')}",
                    params={
                        "api_key": api_key,
                        "language": "zh-CN",
                        "append_to_response": (
                            "credits,aggregate_credits,keywords,external_ids,content_ratings,images"
                        ),
                        "include_image_language": "zh-CN,zh-TW,zh,en,null",
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("TMDb watchlist query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb watchlist response is invalid")
        return _apply_tmdb_image_choices(payload)

    def season_details(self, tmdb_id: str, season_number: int) -> Mapping[str, Any]:
        series = str(tmdb_id or "").strip()
        season = _positive_int(season_number)
        if not series.isdigit() or season <= 0:
            raise ValueError("watchlist TMDb season identity is invalid")
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise RuntimeError("TMDb is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/tv/{quote(series, safe='')}/season/{season}",
                    params={
                        "api_key": api_key,
                        "language": "zh-CN",
                        "append_to_response": "images",
                        "include_image_language": "zh-CN,zh-TW,zh,en,null",
                    },
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("TMDb watchlist season query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb watchlist season response is invalid")
        return _apply_tmdb_image_choices(payload)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


class HttpxWatchlistLibraryAdapter:
    """Read-only Emby series inventory used by the all-series task."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        page_size: int = 200,
    ) -> None:
        if page_size < 1 or page_size > 1000:
            raise ValueError("watchlist page size must be between 1 and 1000")
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._page_size = page_size

    def list_series(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not user_id or not token:
            raise RuntimeError("watchlist task requires configured Emby authorization")
        libraries = tuple(dict.fromkeys(str(value).strip() for value in library_ids if str(value).strip()))
        parents: Sequence[str | None] = libraries or (None,)
        result: list[Mapping[str, Any]] = []
        with self._client_factory() as client:
            for parent in parents:
                start = 0
                while True:
                    params: dict[str, Any] = {
                        "Recursive": "true",
                        "IncludeItemTypes": "Series",
                        "Fields": "ProviderIds,Type,Name,Id",
                        "StartIndex": start,
                        "Limit": self._page_size,
                    }
                    if parent is not None:
                        params["ParentId"] = parent
                    response = client.get(
                        f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                        params=params,
                        headers={"Accept": "application/json", "X-Emby-Token": token},
                    )
                    response.raise_for_status()
                    payload: Any = response.json()
                    if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                        raise RuntimeError("Emby watchlist inventory is invalid")
                    items = payload["Items"]
                    result.extend(dict(item) for item in items if isinstance(item, Mapping))
                    start += len(items)
                    total = int(payload.get("TotalRecordCount") or len(items))
                    if not items or start >= total:
                        break
        return tuple(result)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


def _refresh_series(
    repository: WatchlistTaskRepository,
    tmdb_id: str,
    details: Mapping[str, Any],
    season_details: Sequence[Mapping[str, Any]],
) -> bool:
    """Call new and old-compatible repository fakes without weakening the owner."""
    try:
        return bool(repository.refresh_series(tmdb_id, details, season_details=season_details))
    except TypeError as exc:
        # Existing application fakes and downstream adapters implemented the
        # original two-argument port before season publication was added.  A
        # fallback here keeps that narrow port source-compatible; production
        # repository implementations all accept the keyword.
        if "season_details" not in str(exc):
            raise
        return bool(repository.refresh_series(tmdb_id, details))


def _list_completed_series(
    repository: WatchlistTaskRepository,
    library_ids: Sequence[str],
) -> Sequence[Mapping[str, Any]]:
    """Keep older test/adaptor implementations source-compatible while scoping production reads."""
    try:
        return repository.list_completed_series(library_ids)
    except TypeError as exc:
        if "positional" not in str(exc) and "argument" not in str(exc):
            raise
        return repository.list_completed_series()


def _list_all_completed_series(repository: WatchlistTaskRepository) -> Sequence[Mapping[str, Any]]:
    """Use the explicit revival-scan port when available.

    Older fakes only expose the add-all query.  Keeping that fallback local
    avoids widening the public protocol solely for compatibility fixtures while
    production uses the full Completed/force-ended-free scope.
    """
    method = getattr(repository, "list_all_completed_series", None)
    if callable(method):
        list_all = cast(Callable[[], Sequence[Mapping[str, Any]]], method)
        return list_all()
    return _list_completed_series(repository, ())


def _completed_series_changed(candidate: Mapping[str, Any], details: Mapping[str, Any]) -> bool:
    """Detect newly aired content before mutating a completed watchlist row."""
    if str(candidate.get("watchlist_tmdb_status") or "").strip() not in {"Ended", "Canceled"}:
        return True
    local_last = _date_value(candidate.get("last_air_date"))
    remote_last = _date_value(details.get("last_air_date"))
    if local_last is None or remote_last is None:
        return True
    if remote_last > local_last:
        return True
    local_episode = candidate.get("last_episode_to_air_json")
    remote_episode = details.get("last_episode_to_air")
    local_number = _positive_int(local_episode.get("episode_number")) if isinstance(local_episode, Mapping) else 0
    remote_number = _positive_int(remote_episode.get("episode_number")) if isinstance(remote_episode, Mapping) else 0
    local_season = _positive_int(local_episode.get("season_number")) if isinstance(local_episode, Mapping) else 0
    remote_season = _positive_int(remote_episode.get("season_number")) if isinstance(remote_episode, Mapping) else 0
    return (remote_season, remote_number) > (local_season, local_number)


def _has_animation_genre(details: Mapping[str, Any]) -> bool:
    """Return whether a TMDb series is explicitly animated."""
    values: list[Any] = []
    for key in ("genres", "genre_ids", "genres_json"):
        raw = details.get(key)
        if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
            values.extend(raw)
    for value in values:
        if isinstance(value, Mapping):
            value = value.get("name") or value.get("id")
        normalized = str(value or "").strip().casefold()
        if normalized in {"animation", "animated", "16"} or any(
            token in normalized for token in ("动画", "動漫", "anime", "アニメ")
        ):
            return True
    return False


def _watchlist_series_status(
    existing: str,
    *,
    force_ended: bool,
    tmdb_status: str | None,
    next_episode: Any,
) -> str:
    del next_episode
    if force_ended:
        return existing or "Completed"
    if tmdb_status in {"Ended", "Canceled"}:
        return "Completed"
    return "Watching"


def _mapping_value(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_sequence(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _positive_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _json_text(value: Any) -> str | None:
    if value in (None, (), [], {}):
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _metadata_objects(value: Any, *, keep_logo: bool = False) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for raw in _mapping_sequence(value):
        name = str(raw.get("name") or "").strip()
        if not name:
            continue
        item: dict[str, Any] = {"id": raw.get("id", 0), "name": name}
        if keep_logo and str(raw.get("logo_path") or "").strip():
            item["logo_path"] = str(raw["logo_path"]).strip()
        result.append(item)
    return result


def _country_values(details: Mapping[str, Any]) -> list[Any]:
    values = details.get("origin_country")
    if isinstance(values, Sequence) and not isinstance(values, str | bytes):
        return [value for value in values if str(value or "").strip()]
    countries = _mapping_sequence(details.get("production_countries"))
    return [
        str(value.get("iso_3166_1") or value.get("name") or "").strip()
        for value in countries
        if str(value.get("iso_3166_1") or value.get("name") or "").strip()
    ]


def _keyword_values(details: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = details.get("keywords")
    values = raw.get("results") if isinstance(raw, Mapping) else raw
    return _metadata_objects(values)


def _series_directors(details: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    sources: list[Mapping[str, Any]] = list(_mapping_sequence(details.get("created_by")))
    credits = details.get("credits")
    if isinstance(credits, Mapping):
        sources.extend(
            value
            for value in _mapping_sequence(credits.get("crew"))
            if str(value.get("job") or "").strip().casefold() in {"director", "series director"}
        )
    for value in sources:
        name = str(value.get("name") or "").strip()
        identity = str(value.get("id") or name).strip()
        if not name or identity in seen:
            continue
        seen.add(identity)
        result.append({"id": value.get("id", 0), "name": name})
        if len(result) >= 3:
            break
    return result


def _official_ratings(details: Mapping[str, Any]) -> str | None:
    raw = details.get("content_ratings")
    values = raw.get("results") if isinstance(raw, Mapping) else None
    ratings: dict[str, str] = {}
    for value in _mapping_sequence(values):
        country = str(value.get("iso_3166_1") or "").strip()
        rating = str(value.get("rating") or "").strip()
        if country and rating:
            ratings[country] = rating
    return _json_text(ratings)


def _image_path(details: Mapping[str, Any], field: str, collection: str) -> str | None:
    direct = str(details.get(field) or "").strip()
    if direct:
        return direct
    images = details.get("images")
    candidates = images.get(collection) if isinstance(images, Mapping) else None
    for candidate in _mapping_sequence(candidates):
        path = str(candidate.get("file_path") or "").strip()
        if path:
            return path
    return None


def _apply_tmdb_image_choices(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    for field, collection in (
        ("poster_path", "posters"),
        ("backdrop_path", "backdrops"),
        ("logo_path", "logos"),
    ):
        selected = _image_path(result, field, collection)
        if selected:
            result[field] = selected
    return result


def _watchlist_assistant_configuration(settings: WatchlistSettingsPort) -> dict[str, Any]:
    watchlist = _mapping_value(settings.load("watchlist_config"))
    moviepilot = _mapping_value(settings.load("mp_config"))
    assistant = moviepilot.get("subscribe_assistant")
    if not isinstance(assistant, Mapping):
        assistant = watchlist.get("subscribe_assistant")
    values = dict(assistant) if isinstance(assistant, Mapping) else {}
    auto_pending = _mapping_value(watchlist.get("auto_pending"))
    strategy = _mapping_value(settings.load("subscription_strategy_config"))
    sources = strategy.get("subscription_sources")
    source_enabled = not isinstance(sources, list) or "mp" in {str(value).strip().casefold() for value in sources}
    configured = bool(str(moviepilot.get("moviepilot_url") or "").strip())
    enabled = _bool_value(values.get("enabled"), True) and source_enabled and configured
    disabled_reason = (
        "assistant_disabled"
        if not _bool_value(values.get("enabled"), True)
        else "moviepilot_source_disabled"
        if not source_enabled
        else "moviepilot_not_configured"
    )
    return {
        "enabled": enabled,
        "disabled_reason": disabled_reason,
        "guard_mode": str(values.get("guard_mode") or "balanced"),
        "season_cooldown_days": _positive_int(values.get("season_cooldown_days")) or 14,
        "auto_pending_enabled": _bool_value(
            values.get("pending_enhanced_enabled", auto_pending.get("enabled")),
            True,
        ),
        "auto_pending_days": _positive_int(values.get("auto_tv_pending_days", auto_pending.get("days"))) or 7,
        "auto_pending_episodes": _positive_int(values.get("auto_tv_pending_episodes", auto_pending.get("episodes")))
        or 5,
        "pending_use_volatility": _bool_value(values.get("pending_use_volatility"), False),
        "pending_fake_total_episodes": _positive_int(
            values.get("pending_fake_total_episodes", auto_pending.get("default_total_episodes"))
        )
        or 99,
        "pause_enabled": _bool_value(values.get("pause_enhanced_enabled"), True),
        "airing_pause_days": _positive_int(values.get("airing_pause_days", watchlist.get("auto_pause"))) or 30,
        "tv_air_pause_days": _positive_int(values.get("tv_air_pause_days")) or 14,
        "best_version_type": str(values.get("best_version_type") or "tv_episode"),
        "best_version_episode_to_full": _bool_value(values.get("best_version_episode_to_full"), True),
        "best_version_full_consistency_check_enabled": _bool_value(
            values.get("best_version_full_consistency_check_enabled"),
            True,
        ),
        "snapshot_retention_days": _positive_int(values.get("snapshot_retention_days")) or 180,
    }


def _moviepilot_subscriptions(value: Any) -> tuple[Mapping[str, Any], ...]:
    raw = getattr(value, "subscriptions", value)
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        raise TypeError("MoviePilot watchlist list result is invalid")
    if any(not isinstance(item, Mapping) for item in raw):
        raise TypeError("MoviePilot watchlist subscription item is invalid")
    return tuple(dict(item) for item in raw)


def _moviepilot_query_subscription(value: Any) -> Mapping[str, Any] | None:
    if hasattr(value, "found"):
        return dict(value.subscription) if bool(value.found) and isinstance(value.subscription, Mapping) else None
    if isinstance(value, Mapping):
        raw = value.get("subscription", value)
        return dict(raw) if isinstance(raw, Mapping) and raw else None
    raise TypeError("MoviePilot watchlist query result is invalid")


def _season_episodes(season_details: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    values: list[Mapping[str, Any]] = []
    for season in season_details:
        number = _positive_int(season.get("season_number"))
        for raw in _mapping_sequence(season.get("episodes")):
            episode = dict(raw)
            if _positive_int(episode.get("season_number")) <= 0:
                episode["season_number"] = number
            values.append(episode)
    return tuple(values)


def _real_next_episode(
    episodes: Sequence[Mapping[str, Any]],
    local_seasons: Mapping[int, Mapping[str, Any]],
) -> Mapping[str, Any]:
    max_local_season = max(local_seasons, default=0)
    for episode in sorted(
        episodes,
        key=lambda value: (_positive_int(value.get("season_number")), _positive_int(value.get("episode_number"))),
    ):
        season = _positive_int(episode.get("season_number"))
        number = _positive_int(episode.get("episode_number"))
        if season <= 0 or number <= 0:
            continue
        local = local_seasons.get(season)
        if max_local_season > 0 and season < max_local_season and local is None:
            continue
        collected = {
            _positive_int(value)
            for value in ((local or {}).get("local_episode_numbers") or ())
            if _positive_int(value) > 0
        }
        if number not in collected:
            return dict(episode)
    return {}


def _watchlist_completion_signal(
    tmdb_id: str,
    season: int,
    details: Mapping[str, Any],
    episodes: Sequence[Mapping[str, Any]],
    configuration: Mapping[str, Any],
) -> Any:
    from services.subscribe_assistant.engine import evaluate_completion

    return evaluate_completion(
        tmdb_id=tmdb_id,
        season=season,
        series_details=dict(details),
        episodes=[dict(value) for value in episodes],
        season_cooldown_days=_positive_int(configuration.get("season_cooldown_days")) or 14,
        volatility_stable=True,
    )


def _watchlist_moviepilot_decision(
    *,
    final_status: str,
    details: Mapping[str, Any],
    season: int,
    episodes: Sequence[Mapping[str, Any]],
    real_next_episode: Mapping[str, Any],
    signal: Any,
    configuration: Mapping[str, Any],
    today: date,
) -> dict[str, Any]:
    from services.subscribe_assistant.engine import check_airing_gap_pause, check_pre_air_pause, should_enter_pending

    decision: dict[str, Any] = {"mp_state": "R", "snapshot": False, "reason": "订阅可运行"}
    if final_status == "Completed":
        guard = str(configuration.get("guard_mode") or "balanced").casefold()
        low_confidence = guard == "strict" or (guard == "balanced" and int(signal.scope_total or 0) <= 3)
        if guard != "off" and signal.completed and signal.confidence == "low" and low_confidence:
            return {"mp_state": "P", "snapshot": False, "reason": "低置信完结，进入完成前观察"}
        return {"mp_state": "R", "snapshot": True, "reason": "订阅目标已完成，保存完成快照"}
    if bool(configuration.get("pause_enabled")):
        paused, reason = check_pre_air_pause(
            series_details=dict(details),
            season=season,
            episodes=[dict(value) for value in episodes],
            tv_air_days=int(configuration.get("tv_air_pause_days") or 0),
            as_of=today,
        )
        if paused:
            return {"mp_state": "S", "snapshot": False, "reason": reason}
        paused, reason = check_airing_gap_pause(
            next_episode=dict(real_next_episode),
            pause_days=int(configuration.get("airing_pause_days") or 0),
            signal=signal,
            as_of=today,
        )
        if paused:
            return {"mp_state": "S", "snapshot": False, "reason": reason}
    if final_status == "Pending" or bool(configuration.get("auto_pending_enabled")):
        pending, reason = should_enter_pending(
            series_details=dict(details),
            season=season,
            episodes=[dict(value) for value in episodes],
            pending_days=int(configuration.get("auto_pending_days") or 0),
            pending_episodes=int(configuration.get("auto_pending_episodes") or 0),
            use_volatility=bool(configuration.get("pending_use_volatility")),
            signal=signal,
            as_of=today,
        )
        if final_status == "Pending" or pending:
            return {"mp_state": "P", "snapshot": False, "reason": reason or "ETK 追剧状态为待定"}
    if final_status == "Paused":
        return {"mp_state": "S", "snapshot": False, "reason": "ETK 追剧状态为暂停"}
    return decision


def _apply_moviepilot_washing(
    payload: dict[str, Any],
    configuration: Mapping[str, Any],
    *,
    force_full: bool,
) -> None:
    mode = str(configuration.get("best_version_type") or "").strip().casefold()
    if force_full or mode in {"tv", "all"}:
        payload["best_version"] = 1
        payload["best_version_full"] = 1
    elif mode == "tv_episode":
        payload["best_version"] = 1


def _watchlist_occurrence(action: str, tmdb_id: str, season: int, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_json(dict(payload)).encode("utf-8")).hexdigest()[:24]
    return f"watchlist:{tmdb_id}:S{season}:{action}:{digest}"


def _required_tmdb_int(value: Any) -> int:
    normalized = str(value or "").strip()
    if not normalized.isdigit() or int(normalized) <= 0:
        raise ValueError("watchlist MoviePilot TMDb identity must be a positive integer")
    return int(normalized)


def _locked_json_state(uow: SqlAlchemyUnitOfWork, key: str) -> dict[str, Any]:
    uow.session.execute(
        text(
            """
            INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
            VALUES (:key, '{}'::jsonb, NOW())
            ON CONFLICT (state_key) DO NOTHING
            """
        ),
        {"key": key},
    )
    value = uow.session.scalar(
        text("SELECT state_json FROM subscribe_assistant_state WHERE state_key = :key FOR UPDATE"),
        {"key": key},
    )
    return dict(value) if isinstance(value, Mapping) else {}


def _write_json_state(uow: SqlAlchemyUnitOfWork, key: str, value: Mapping[str, Any]) -> None:
    uow.session.execute(
        text(
            """
            UPDATE subscribe_assistant_state
            SET state_json = CAST(:value AS jsonb), updated_at = NOW()
            WHERE state_key = :key
            """
        ),
        {"key": key, "value": canonical_json(dict(value), field="subscribe assistant state")},
    )


def _set_watchlist_active_washing(
    uow: SqlAlchemyUnitOfWork,
    tmdb_id: str,
    season: int,
    enabled: bool,
) -> None:
    uow.session.execute(
        text(
            """
            UPDATE media_metadata
            SET active_washing = :enabled, last_updated_at = NOW()
            WHERE parent_series_tmdb_id = :tmdb_id
              AND season_number = :season
              AND item_type = 'Season'
            """
        ),
        {"enabled": bool(enabled), "tmdb_id": tmdb_id, "season": season},
    )


def _watchlist_assets_consistent(values: Sequence[Any], expected: int) -> bool:
    if len(values) < expected:
        return False
    signatures: list[tuple[str, str, str, str]] = []
    for raw in values:
        assets = _mapping_sequence(raw if isinstance(raw, list) else (raw,))
        if not assets:
            return False
        asset = assets[0]
        resolution = str(asset.get("resolution_display") or asset.get("resolution") or "").strip().casefold()
        codec = str(asset.get("codec_display") or asset.get("video_codec") or "").strip().casefold()
        group = (
            str(asset.get("release_group_raw") or asset.get("release_group") or "").split(",", 1)[0].strip().casefold()
        )
        effect = str(asset.get("effect_display") or asset.get("effect") or "").strip().casefold()
        if not resolution or not codec:
            return False
        signatures.append((resolution, codec, group, effect))
    groups = {value[2] for value in signatures if value[2]}
    effects = {value[3] for value in signatures if value[3]}
    return (
        len({value[0] for value in signatures}) == 1
        and len({value[1] for value in signatures}) == 1
        and (not groups or (len(groups) == 1 and all(value[2] for value in signatures)))
        and (not effects or (len(effects) == 1 and all(value[3] for value in signatures)))
    )


def _watchlist_reclassification_transition(old: str, new: str) -> bool:
    active = {"Watching", "Paused", "Pending"}
    return (
        (old in {"", "NONE", "None"} and new in active | {"Completed"})
        or (old in active and new == "Completed")
        or (old == "Completed" and new in active)
    )


def _watchlist_p115_intent(
    tmdb_id: str,
    status: str,
    parent_id: str,
    file_ids: Sequence[str],
) -> str:
    digest = hashlib.sha256("\0".join(sorted(file_ids)).encode("utf-8")).hexdigest()[:24]
    return f"watchlist-reclass:{tmdb_id}:{status}:{parent_id}:{digest}"


_EPISODE_PATTERN = re.compile(r"(?i)(?:^|[^A-Za-z0-9])(?:S\d{1,4}[ ._-]*)?E(?:P)?[ ._-]*(\d{1,4})(?:[^A-Za-z0-9]|$)")


def _episode_number_from_name(value: Any) -> int | None:
    match = _EPISODE_PATTERN.search(str(value or ""))
    if match is None:
        return None
    return _positive_int(match.group(1)) or None


def _ready_watchlist_p115_plan(value: Any) -> Mapping[str, Any]:
    payload = getattr(value, "payload", value)
    status_code = int(getattr(value, "status_code", 200) or 200)
    root: Mapping[str, Any] = cast(Mapping[str, Any], payload) if isinstance(payload, Mapping) else {}
    raw_data = root.get("data")
    data: Mapping[str, Any] = cast(Mapping[str, Any], raw_data) if isinstance(raw_data, Mapping) else root
    if status_code >= 400 or root.get("success", True) is not True or data.get("status") != "ready":
        errors = data.get("errors") if isinstance(data, Mapping) else None
        codes = [str(item.get("code") or "") for item in errors or () if isinstance(item, Mapping)]
        if codes and set(codes) == {"NO_ELIGIBLE_MEDIA"}:
            return {}
        raise RuntimeError(f"watchlist 115 organize plan is not ready: {codes or status_code}")
    if not str(data.get("plan_id") or "").strip():
        raise RuntimeError("watchlist 115 organize plan has no identity")
    return dict(data)


def _watchlist_suppression_keys(tmdb_id: str, season: int, subscribe_id: int) -> tuple[str, ...]:
    keys = [f"tmdb:{tmdb_id}:S{season}"] if tmdb_id and season > 0 else []
    if subscribe_id > 0:
        keys.append(f"sub:{subscribe_id}")
    return tuple(keys)


def _shared_watchlist_metadata_items(
    tmdb_id: str,
    details: Mapping[str, Any],
    season_details: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
    *,
    configuration: Mapping[str, Any],
) -> list[dict[str, Any]]:
    series = _mapping_value(context.get("series"))
    seasons = {
        _positive_int(value.get("season_number")): value
        for value in _mapping_sequence(context.get("seasons"))
        if _positive_int(value.get("season_number")) > 0
    }
    release = _date_value(details.get("first_air_date"))
    series_item = {
        "tmdb_id": tmdb_id,
        "item_type": "Series",
        "title": str(series.get("title") or details.get("name") or details.get("original_name") or f"TMDb {tmdb_id}"),
        "original_title": details.get("original_name"),
        "overview": details.get("overview"),
        "poster_path": _image_path(details, "poster_path", "posters"),
        "backdrop_path": _image_path(details, "backdrop_path", "backdrops"),
        "release_date": release.isoformat() if release else None,
        "release_year": release.year if release else None,
        "rating": _positive_float(details.get("vote_average")),
        "genres_json": _metadata_objects(details.get("genres")),
        "original_language": details.get("original_language"),
        "total_episodes": _positive_int(details.get("number_of_episodes")),
        "watching_status": series.get("watching_status"),
        "watchlist_tmdb_status": details.get("status"),
        "metadata_source": "native_watchlist_task",
    }
    items = [{key: value for key, value in series_item.items() if value not in (None, "", [], {})}]
    remote_seasons = _mapping_sequence(details.get("seasons"))
    latest = max((_positive_int(value.get("season_number")) for value in remote_seasons), default=0)
    auto_pending = _mapping_value(configuration.get("auto_pending"))
    fake_total = _positive_int(auto_pending.get("default_total_episodes")) or 99
    for remote in remote_seasons:
        season = _positive_int(remote.get("season_number"))
        if season <= 0:
            continue
        local = seasons.get(season, {})
        expected = _positive_int(local.get("total_episodes")) or _positive_int(remote.get("episode_count"))
        source = "locked" if bool(local.get("total_episodes_locked")) and expected > 0 else "tmdb"
        if str(series.get("watching_status") or "") == "Pending" and season == latest:
            expected = max(expected, fake_total)
            source = "pending_virtual"
        if expected <= 0:
            continue
        aired = _date_value(remote.get("air_date"))
        items.append(
            {
                "tmdb_id": tmdb_id,
                "item_type": "Season",
                "season_number": season,
                "title": str(remote.get("name") or ""),
                "original_title": str(remote.get("name") or ""),
                "overview": str(remote.get("overview") or ""),
                "poster_path": str(remote.get("poster_path") or ""),
                "release_date": aired.isoformat() if aired else "",
                "release_year": aired.year if aired else None,
                "expected_episode_count": expected,
                "total_episodes": expected,
                "episode_count": expected,
                "episode_count_source": source,
                "episode_count_locked": source == "locked",
                "episode_count_pending_virtual": source == "pending_virtual",
                "watching_status": str(local.get("watching_status") or ""),
                "watchlist_tmdb_status": str(details.get("status") or ""),
                "metadata_source": "native_watchlist_task",
            }
        )
    for episode in _season_episodes(season_details):
        season = _positive_int(episode.get("season_number"))
        number = _positive_int(episode.get("episode_number"))
        runtime = _positive_int(episode.get("runtime"))
        if season > 0 and number > 0 and runtime > 0:
            items.append(
                {
                    "tmdb_id": tmdb_id,
                    "item_type": "Episode",
                    "season_number": season,
                    "episode_number": number,
                    "runtime_minutes": runtime,
                    "runtime_source": "tmdb_episode_runtime",
                    "metadata_source": "native_watchlist_task",
                }
            )
    return items


def _string_values(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    values = value if isinstance(value, Sequence) and not isinstance(value, str | bytes) else (value,)
    return tuple(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        result = int(value)
    except (TypeError, ValueError):
        return 0
    return result if result > 0 else 0


def _missing_episode_numbers(expected_count: int, collected_numbers: Sequence[int] | set[int]) -> tuple[int, ...]:
    """Return the actual missing episode numbers for a season snapshot."""
    expected = _positive_int(expected_count)
    if expected <= 0:
        return ()
    collected = {number for number in (_positive_int(value) for value in collected_numbers) if number > 0}
    return tuple(number for number in range(1, expected + 1) if number not in collected)


def _date_value(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


__all__ = [
    "HttpxWatchlistLibraryAdapter",
    "HttpxWatchlistMetadataAdapter",
    "NativeWatchlistTaskEffects",
    "NativeWatchlistTaskService",
    "SqlAlchemyWatchlistTaskRepository",
    "WATCHLIST_TASK_KEYS",
]
