from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

from .intro_detection import (
    INTRO_DETECTION_ALGORITHM_VERSION,
    IntroDetectionPolicySnapshot,
    IntroEpisodeSnapshot,
    IntroFingerprintKind,
    IntroTriggerMode,
)
from .intro_detection_repository import (
    PostgresP115IntroDetectionRepository,
    episode_failure_key,
    season_failure_key,
)

_ACTIVE_WATCHLIST_STATUSES = ("Watching", "Paused", "Pending")


class IntroSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class IntroFavoriteIdentity:
    user_id: str
    item_id: str


class IntroEmbyFavoriteFallbackPort(Protocol):
    def find_favorite(
        self,
        *,
        direct_item_ids: Sequence[str],
        season_ids: Sequence[str],
    ) -> IntroFavoriteIdentity | None: ...


class HttpxEmbyIntroFavoriteGateway:
    """Processor-free authoritative read for favorites predating the local projection."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def find_favorite(
        self,
        *,
        direct_item_ids: Sequence[str],
        season_ids: Sequence[str],
    ) -> IntroFavoriteIdentity | None:
        users = self._get("/Users")
        if not isinstance(users, list):
            raise RuntimeError("Emby favorite user list returned an invalid payload")
        direct = tuple(dict.fromkeys(_nonempty_texts(direct_item_ids)))
        seasons = tuple(dict.fromkeys(_nonempty_texts(season_ids)))
        for raw_user in users:
            if not isinstance(raw_user, Mapping):
                continue
            user_id = str(raw_user.get("Id") or "").strip()
            if not user_id:
                continue
            for item_id in direct:
                item = self._get(
                    f"/Users/{quote(user_id, safe='')}/Items/{quote(item_id, safe='')}",
                    params={"Fields": "UserData"},
                    allow_not_found=True,
                )
                if isinstance(item, Mapping):
                    user_data = item.get("UserData")
                    if isinstance(user_data, Mapping) and user_data.get("IsFavorite") is True:
                        return IntroFavoriteIdentity(user_id, item_id)
            for season_id in seasons:
                result = self._get(
                    f"/Users/{quote(user_id, safe='')}/Items",
                    params={
                        "ParentId": season_id,
                        "IncludeItemTypes": "Episode",
                        "Filters": "IsFavorite",
                        "Limit": "1",
                        "Fields": "UserData",
                    },
                )
                if not isinstance(result, Mapping) or not isinstance(result.get("Items"), list):
                    raise RuntimeError("Emby favorite episode query returned an invalid payload")
                favorite = next((item for item in result["Items"] if isinstance(item, Mapping)), None)
                favorite_id = str((favorite or {}).get("Id") or "").strip()
                if favorite_id:
                    return IntroFavoriteIdentity(user_id, favorite_id)
        return None

    def _get(
        self,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        settings = self._settings()
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby favorite fallback is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params or {}),
                )
                if allow_not_found and response.status_code == 404:
                    return None
                if response.status_code != 200:
                    raise RuntimeError(f"Emby favorite read returned HTTP {response.status_code}")
                return response.json()
        except RuntimeError:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby favorite read is unavailable") from exc

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=max(1.0, float(self._settings().emby_api_timeout)), trust_env=False)


class PostgresP115IntroRuntimeSource:
    """Native P7 policy/candidate projection used by P6 intro operations."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        repository: PostgresP115IntroDetectionRepository,
        settings: IntroSettingsPort,
        favorite_fallback: IntroEmbyFavoriteFallbackPort | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._repository = repository
        self._settings = settings
        self._favorite_fallback = favorite_fallback

    def snapshot(self) -> IntroDetectionPolicySnapshot:
        try:
            watchlist = self._settings.load("watchlist_config")
            config = dict(watchlist) if isinstance(watchlist, Mapping) else {}
            mode = str(config.get("intro_detection_trigger_mode") or "").strip().lower()
            if mode not in {item.value for item in IntroTriggerMode}:
                legacy = self._settings.load("shared_resource_config")
                enabled = isinstance(legacy, Mapping) and _enabled(legacy.get("p115_etk_intro_detection_enabled"))
                mode = IntroTriggerMode.IMPORT.value if enabled else IntroTriggerMode.OFF.value
            raw_libraries = config.get("intro_detection_library_ids")
            libraries = raw_libraries if isinstance(raw_libraries, list) else []
            return IntroDetectionPolicySnapshot.create(
                trigger_mode=mode,
                credits_enabled=_enabled(config.get("intro_detection_credits_enabled")),
                library_ids=libraries,
            )
        except Exception:
            return IntroDetectionPolicySnapshot.create(
                trigger_mode=IntroTriggerMode.OFF,
                credits_enabled=False,
            )

    def season_is_favorite(self, series_tmdb_id: str, season_number: int) -> bool:
        series = _required_text(series_tmdb_id, "series_tmdb_id")
        season = _nonnegative_int(season_number, "season_number")
        all_ids, direct_ids, season_ids = self._favorite_scope_ids(series, season)
        if not all_ids:
            return False
        with self._unit_of_work() as uow:
            found = uow.session.scalar(
                text(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM emby_favorite_items
                        WHERE item_id = ANY(CAST(:item_ids AS text[]))
                    )
                    """
                ),
                {"item_ids": list(all_ids)},
            )
        if bool(found):
            return True
        if self._favorite_fallback is None:
            return False
        try:
            favorite = self._favorite_fallback.find_favorite(
                direct_item_ids=direct_ids,
                season_ids=season_ids,
            )
        except Exception:
            return False
        if favorite is None:
            return False
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_favorite_items(user_id, item_id, updated_at)
                    VALUES (:user_id, :item_id, NOW())
                    ON CONFLICT (user_id, item_id) DO UPDATE SET updated_at = EXCLUDED.updated_at
                    """
                ),
                {"user_id": favorite.user_id, "item_id": favorite.item_id},
            )
            uow.commit()
        return True

    def resolve_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
    ) -> tuple[IntroEpisodeSnapshot, ...]:
        del sha1
        payload = dict(item) if isinstance(item, Mapping) else {}
        item_id = str(payload.get("Id") or "").strip()
        if not item_id:
            return ()
        key = self._episode_key(item_id)
        if key is None:
            series_id = str(payload.get("SeriesId") or "").strip()
            season_number = _optional_int(payload.get("ParentIndexNumber"))
            episode_number = _optional_int(payload.get("IndexNumber"))
            if series_id and season_number is not None and episode_number is not None:
                key = self._episode_key_by_series(series_id, season_number, episode_number)
        return self._season(key)

    def resolve_episode_ids(
        self,
        episode_ids: Sequence[str],
        *,
        require_existing_detection: bool,
    ) -> tuple[tuple[tuple[IntroEpisodeSnapshot, ...], ...], int]:
        seasons: list[tuple[IntroEpisodeSnapshot, ...]] = []
        seen: set[tuple[str, int]] = set()
        unresolved = 0
        for item_id in _nonempty_texts(episode_ids):
            key = self._episode_key(item_id)
            if key is None:
                unresolved += 1
                continue
            if key in seen:
                continue
            seen.add(key)
            season = self._season(key)
            if not season:
                unresolved += 1
                continue
            if require_existing_detection and not self._season_has_detection(season):
                continue
            seasons.append(season)
        return tuple(seasons), unresolved

    def resolve_favorite_item(
        self,
        item: Mapping[str, Any],
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        payload = dict(item) if isinstance(item, Mapping) else {}
        item_id = str(payload.get("Id") or "").strip()
        item_type = str(payload.get("Type") or "").strip().title()
        if not item_id:
            return ()
        if item_type == "Episode":
            season = self.resolve_item(payload)
            return (season,) if season else ()
        if item_type not in {"Series", "Season"}:
            return ()
        with self._unit_of_work() as uow:
            if item_type == "Series":
                rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT DISTINCT e.parent_series_tmdb_id, e.season_number
                            FROM media_metadata AS e
                            JOIN media_metadata AS p
                              ON p.tmdb_id = e.parent_series_tmdb_id AND p.item_type = 'Series'
                            WHERE e.item_type = 'Episode' AND e.in_library = TRUE
                              AND p.emby_item_ids_json ? :item_id
                            ORDER BY e.parent_series_tmdb_id, e.season_number
                            """
                        ),
                        {"item_id": item_id},
                    )
                    .mappings()
                    .all()
                )
            else:
                rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT parent_series_tmdb_id, season_number
                            FROM media_metadata
                            WHERE item_type = 'Season' AND emby_item_ids_json ? :item_id
                            ORDER BY parent_series_tmdb_id, season_number
                            """
                        ),
                        {"item_id": item_id},
                    )
                    .mappings()
                    .all()
                )
        return tuple(
            season
            for row in rows
            if (season := self._season((str(row["parent_series_tmdb_id"]), int(row["season_number"]))))
        )

    def list_library_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
        credits_enabled: bool | None = None,
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        return self._list_backfill_seasons(
            limit=limit,
            library_ids=library_ids,
            active_watchlist=False,
            credits_enabled=credits_enabled,
        )

    def list_active_watchlist_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
        credits_enabled: bool | None = None,
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        return self._list_backfill_seasons(
            limit=limit,
            library_ids=library_ids,
            active_watchlist=True,
            credits_enabled=credits_enabled,
        )

    def list_favorite_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
        credits_enabled: bool | None = None,
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        return self.list_active_watchlist_seasons(
            limit=limit,
            library_ids=library_ids,
            credits_enabled=credits_enabled,
        )

    def _list_backfill_seasons(
        self,
        *,
        limit: int,
        library_ids: Sequence[str],
        active_watchlist: bool,
        credits_enabled: bool | None,
    ) -> tuple[tuple[IntroEpisodeSnapshot, ...], ...]:
        bounded = max(1, min(int(limit), 200))
        selected_libraries = {str(value or "").strip() for value in library_ids if str(value or "").strip()}
        include_credits = self.snapshot().credits_enabled if credits_enabled is None else bool(credits_enabled)
        page_size = min(
            max(bounded * (120 if active_watchlist else 20), 800 if active_watchlist else 500),
            8000 if active_watchlist else 4000,
        )
        seasons: list[tuple[IntroEpisodeSnapshot, ...]] = []
        seen: set[tuple[str, int]] = set()
        offset = 0
        while len(seasons) < bounded:
            rows = self._candidate_rows(
                active_watchlist=active_watchlist,
                limit=page_size,
                offset=offset,
            )
            if not rows:
                break
            offset += len(rows)
            for row in rows:
                key = (str(row.get("parent_series_tmdb_id") or "").strip(), int(row.get("season_number") or 0))
                if not key[0] or key[1] <= 0 or key in seen:
                    continue
                identity = _first_aligned_identity(row)
                if identity is None:
                    continue
                _, representative_sha1, _, library_id = identity
                if selected_libraries and library_id not in selected_libraries:
                    continue
                season = self._season(key)
                representative = next((episode for episode in season if episode.sha1 == representative_sha1), None)
                if representative is None or not self._needs_backfill(representative, include_credits):
                    continue
                seen.add(key)
                seasons.append(season)
                if len(seasons) >= bounded:
                    break
            if active_watchlist or len(rows) < page_size:
                break
        return tuple(seasons)

    def _candidate_rows(
        self,
        *,
        active_watchlist: bool,
        limit: int,
        offset: int,
    ) -> list[Mapping[str, Any]]:
        watchlist_clause = "AND p.watching_status = ANY(CAST(:active_statuses AS text[]))" if active_watchlist else ""
        offset_clause = "" if active_watchlist else "OFFSET :offset"
        order = (
            "e.last_updated_at DESC NULLS LAST" if active_watchlist else "e.last_updated_at DESC NULLS LAST, e.tmdb_id"
        )
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if active_watchlist:
            params["active_statuses"] = list(_ACTIVE_WATCHLIST_STATUSES)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        f"""
                        SELECT e.tmdb_id, e.parent_series_tmdb_id, e.season_number,
                               e.emby_item_ids_json, e.file_sha1_json,
                               e.file_pickcode_json, e.asset_details_json
                        FROM media_metadata AS e
                        JOIN media_metadata AS p
                          ON p.tmdb_id = e.parent_series_tmdb_id AND p.item_type = 'Series'
                        WHERE e.item_type = 'Episode' AND e.in_library = TRUE
                          AND e.season_number > 0
                          AND jsonb_array_length(COALESCE(e.emby_item_ids_json, '[]'::jsonb)) > 0
                          AND jsonb_array_length(COALESCE(e.file_sha1_json, '[]'::jsonb)) > 0
                          AND jsonb_array_length(COALESCE(e.file_pickcode_json, '[]'::jsonb)) > 0
                          {watchlist_clause}
                        ORDER BY {order}
                        LIMIT :limit {offset_clause}
                        """
                    ),
                    params,
                )
                .mappings()
                .all()
            )
            return [dict(row) for row in rows]

    def _needs_backfill(self, episode: IntroEpisodeSnapshot, include_credits: bool) -> bool:
        intro_start, _, credits_start = self._repository.chapter_markers(episode)
        if intro_start is None and not self._failed(episode, IntroFingerprintKind.INTRO):
            return True
        return include_credits and credits_start is None and not self._failed(episode, IntroFingerprintKind.CREDITS)

    def _failed(self, episode: IntroEpisodeSnapshot, kind: IntroFingerprintKind) -> bool:
        return self._repository.has_detection_failure(
            episode_failure_key(episode.sha1),
            kind,
            algorithm_version=INTRO_DETECTION_ALGORITHM_VERSION,
        ) or self._repository.has_detection_failure(
            season_failure_key(episode.series_tmdb_id, episode.season_number),
            kind,
            algorithm_version=INTRO_DETECTION_ALGORITHM_VERSION,
        )

    def _season_has_detection(self, season: Sequence[IntroEpisodeSnapshot]) -> bool:
        return any(any(value is not None for value in self._repository.chapter_markers(episode)) for episode in season)

    def _episode_key(self, item_id: str) -> tuple[str, int] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT parent_series_tmdb_id, season_number
                        FROM media_metadata
                        WHERE item_type = 'Episode' AND emby_item_ids_json ? :item_id
                        ORDER BY in_library DESC, last_updated_at DESC NULLS LAST, tmdb_id
                        LIMIT 1
                        """
                    ),
                    {"item_id": item_id},
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return str(row["parent_series_tmdb_id"]), int(row["season_number"])

    def _episode_key_by_series(
        self,
        series_id: str,
        season_number: int,
        episode_number: int,
    ) -> tuple[str, int] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT e.parent_series_tmdb_id, e.season_number
                        FROM media_metadata AS e
                        JOIN media_metadata AS p
                          ON p.tmdb_id = e.parent_series_tmdb_id AND p.item_type = 'Series'
                        WHERE e.item_type = 'Episode' AND p.emby_item_ids_json ? :series_id
                          AND e.season_number = :season_number AND e.episode_number = :episode_number
                        ORDER BY e.in_library DESC, e.last_updated_at DESC NULLS LAST, e.tmdb_id
                        LIMIT 1
                        """
                    ),
                    {
                        "series_id": series_id,
                        "season_number": season_number,
                        "episode_number": episode_number,
                    },
                )
                .mappings()
                .one_or_none()
            )
        if row is None:
            return None
        return str(row["parent_series_tmdb_id"]), int(row["season_number"])

    def _favorite_scope_ids(
        self,
        series_tmdb_id: str,
        season_number: int,
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT item_type, emby_item_ids_json
                        FROM media_metadata
                        WHERE (item_type = 'Series' AND tmdb_id = :series_tmdb_id)
                           OR (item_type = 'Season' AND parent_series_tmdb_id = :series_tmdb_id
                               AND season_number = :season_number)
                           OR (item_type = 'Episode' AND parent_series_tmdb_id = :series_tmdb_id
                               AND season_number = :season_number)
                        ORDER BY CASE item_type WHEN 'Series' THEN 0 WHEN 'Season' THEN 1 ELSE 2 END,
                                 episode_number NULLS FIRST, tmdb_id
                        """
                    ),
                    {"series_tmdb_id": series_tmdb_id, "season_number": season_number},
                )
                .mappings()
                .all()
            )
        all_ids: list[str] = []
        direct_ids: list[str] = []
        season_ids: list[str] = []
        for row in rows:
            values = _json_texts(row.get("emby_item_ids_json"))
            all_ids.extend(values)
            item_type = str(row.get("item_type") or "")
            if item_type in {"Series", "Season"}:
                direct_ids.extend(values)
            if item_type == "Season":
                season_ids.extend(values)
        return (
            tuple(dict.fromkeys(all_ids)),
            tuple(dict.fromkeys(direct_ids)),
            tuple(dict.fromkeys(season_ids)),
        )

    def _season(self, key: tuple[str, int] | None) -> tuple[IntroEpisodeSnapshot, ...]:
        if key is None or not key[0]:
            return ()
        return self._repository.list_season_snapshots(key[0], key[1])


def select_intro_provider_identity(credentials: Any, priority: Callable[[], object]) -> tuple[str, str]:
    snapshot = credentials.load()
    account_id = str(getattr(snapshot, "account_id", "") or "").strip()
    if not account_id.isdigit():
        raise ValueError("stable 115 account identity is unavailable")
    preferred = str(priority() or "openapi").strip().lower()
    order = ("cookie", "openapi") if preferred == "cookie" else ("openapi", "cookie")
    for backend in order:
        credential = getattr(snapshot, "cookie" if backend == "cookie" else "access_token", "")
        if str(credential or "").strip():
            return account_id, backend
    raise ValueError("stable 115 provider backend is unavailable")


def _first_aligned_identity(row: Mapping[str, Any]) -> tuple[str, str, str, str] | None:
    item_ids = _json_slot_texts(row.get("emby_item_ids_json"))
    sha1s = [_optional_sha1(value) for value in _json_values(row.get("file_sha1_json"))]
    pick_codes = _json_slot_texts(row.get("file_pickcode_json"))
    assets = _json_values(row.get("asset_details_json"))
    for index in range(min(len(item_ids), len(sha1s), len(pick_codes))):
        if not item_ids[index] or not sha1s[index] or not pick_codes[index]:
            continue
        asset = assets[index] if index < len(assets) and isinstance(assets[index], Mapping) else {}
        return item_ids[index], sha1s[index], pick_codes[index], str(asset.get("source_library_id") or "").strip()
    return None


def _json_values(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _json_texts(value: object) -> list[str]:
    return [str(item or "").strip() for item in _json_values(value) if str(item or "").strip()]


def _json_slot_texts(value: object) -> list[str]:
    return [str(item or "").strip() for item in _json_values(value)]


def _nonempty_texts(values: Sequence[object]) -> list[str]:
    return [str(value or "").strip() for value in values if str(value or "").strip()]


def _optional_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 40:
        return ""
    try:
        bytes.fromhex(normalized)
    except ValueError:
        return ""
    return normalized


def _enabled(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    parsed = int(str(value))
    if parsed < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return parsed


def _optional_int(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "HttpxEmbyIntroFavoriteGateway",
    "IntroEmbyFavoriteFallbackPort",
    "IntroFavoriteIdentity",
    "PostgresP115IntroRuntimeSource",
    "select_intro_provider_identity",
]
