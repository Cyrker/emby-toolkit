"""Processor-free execution for actor and people maintenance tasks.

The service deliberately keeps external calls behind small ports.  It owns
the iteration, readback fences and task outcomes; adapters own HTTP and SQL.
This makes native-only workers usable without importing ``tasks.actors`` or a
legacy processor while preserving the existing task keys and envelopes.
"""

from __future__ import annotations

import json
import logging
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol, cast
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome

logger = logging.getLogger(__name__)

ACTOR_TASK_KEYS = frozenset(
    {
        "actor-translation",
        "enrich-aliases",
        "actor-tracking",
        "merge-duplicate-actors",
        "purge-ghost-actors",
    }
)


class PersonNotFoundError(RuntimeError):
    """TMDb explicitly reported that a person identity no longer exists."""


class PeopleTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class PeopleCatalogPort(Protocol):
    def list_persons(self) -> Sequence[Mapping[str, Any]]: ...

    def update_person_name(self, person_id: str, name: str) -> bool: ...

    def list_media_items(self) -> Sequence[Mapping[str, Any]]: ...

    def update_media_people(self, item_id: str, people: Sequence[Mapping[str, Any]]) -> bool: ...

    def delete_person(self, person_id: str) -> bool: ...

    def person_exists(self, person_id: str) -> bool: ...


class PersonProviderPort(Protocol):
    def details(self, person_id: str) -> Mapping[str, Any]: ...

    def credits(self, person_id: str) -> Sequence[Mapping[str, Any]]: ...


class PersonTranslationPort(Protocol):
    def translate(self, values: Sequence[str]) -> Mapping[str, str]: ...


class PersonMetadataRepository(Protocol):
    def list_for_enrichment(self, *, force: bool) -> Sequence[Mapping[str, Any]]: ...

    def upsert(self, details: Mapping[str, Any], *, primary_name: str | None = None) -> bool: ...

    def list_actor_subscriptions(self, subscription_id: int | None = None) -> Sequence[Mapping[str, Any]]: ...

    def record_actor_scan(self, subscription_id: int, tmdb_ids: Sequence[str]) -> int: ...

    def reconcile_actor_scan(
        self,
        subscription: Mapping[str, Any],
        works: Sequence[Mapping[str, Any]],
        all_work_ids: Sequence[str],
    ) -> Mapping[str, int]: ...


class HttpxPeopleCatalogAdapter:
    """Small Emby People adapter with bounded pages and explicit credentials."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        page_size: int = 200,
    ) -> None:
        if page_size < 1 or page_size > 1000:
            raise ValueError("people page_size must be between 1 and 1000")
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._page_size = page_size

    def list_persons(self) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        base_url, user_id, token = _emby_identity(settings)
        result: list[Mapping[str, Any]] = []
        start = 0
        while True:
            payload = self._request(
                "GET",
                f"{base_url}/Persons",
                token,
                {
                    "UserId": user_id,
                    "Fields": "ProviderIds,Id,Name,ImageTags",
                    "StartIndex": start,
                    "Limit": self._page_size,
                },
            )
            values = payload.get("Items")
            if not isinstance(values, list):
                raise RuntimeError("Emby people query returned an invalid item list")
            result.extend(dict(value) for value in values if isinstance(value, Mapping))
            start += len(values)
            total = int(payload.get("TotalRecordCount") or len(values))
            if not values or start >= total:
                break
        return tuple(result)

    def update_person_name(self, person_id: str, name: str) -> bool:
        settings = self._settings()
        base_url, _user_id, token = _emby_identity(settings)
        normalized_id = _required_text(person_id, "person_id", 255)
        normalized_name = _required_text(name, "name", 500)
        self._request(
            "POST",
            f"{base_url}/Items/{quote(normalized_id, safe='')}",
            token,
            json_body={"Name": normalized_name},
        )
        return True

    def list_media_items(self) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        base_url, user_id, token = _emby_identity(settings)
        values: list[Mapping[str, Any]] = []
        start = 0
        while True:
            payload = self._request(
                "GET",
                f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                token,
                {
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series,Season,Episode,Video",
                    "Fields": "Id,Type,People",
                    "StartIndex": start,
                    "Limit": self._page_size,
                },
            )
            items = payload.get("Items")
            if not isinstance(items, list):
                raise RuntimeError("Emby media people query returned an invalid item list")
            values.extend(dict(item) for item in items if isinstance(item, Mapping))
            start += len(items)
            total = int(payload.get("TotalRecordCount") or len(items))
            if not items or start >= total:
                break
        return tuple(values)

    def update_media_people(self, item_id: str, people: Sequence[Mapping[str, Any]]) -> bool:
        settings = self._settings()
        base_url, _user_id, token = _emby_identity(settings)
        normalized_id = _required_text(item_id, "item_id", 255)
        self._request(
            "POST",
            f"{base_url}/Items/{quote(normalized_id, safe='')}",
            token,
            json_body={"People": [dict(value) for value in people]},
        )
        return True

    def delete_person(self, person_id: str) -> bool:
        settings = self._settings()
        base_url, _user_id, token = _emby_identity(settings)
        normalized_id = _required_text(person_id, "person_id", 255)
        self._request("DELETE", f"{base_url}/Items/{quote(normalized_id, safe='')}", token)
        return True

    def person_exists(self, person_id: str) -> bool:
        settings = self._settings()
        base_url, user_id, token = _emby_identity(settings)
        normalized_id = _required_text(person_id, "person_id", 255)
        payload = self._request(
            "GET",
            f"{base_url}/Users/{quote(user_id, safe='')}/Items",
            token,
            {"Ids": normalized_id, "IncludeItemTypes": "Person", "Limit": 1, "Fields": "Id"},
        )
        items = payload.get("Items")
        return isinstance(items, list) and any(
            isinstance(item, Mapping) and str(item.get("Id") or "").strip() == normalized_id for item in items
        )

    def _request(
        self,
        method: str,
        url: str,
        token: str,
        params: Mapping[str, Any] | None = None,
        *,
        json_body: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        try:
            with self._client_factory() as client:
                response = client.request(
                    method,
                    url,
                    params=dict(params or {}),
                    json=dict(json_body) if json_body is not None else None,
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                )
                if response.status_code == 404 and method == "DELETE":
                    return {}
                response.raise_for_status()
                if response.status_code == 204:
                    return {}
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby people operation failed") from exc
        if payload is None:
            return {}
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby people operation returned an invalid object")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout), trust_env=False)


class HttpxTmdbPersonAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def details(self, person_id: str) -> Mapping[str, Any]:
        return self._get(
            f"/person/{_tmdb_id(person_id)}",
            {"language": "zh-CN", "append_to_response": "external_ids,translations"},
        )

    def credits(self, person_id: str) -> Sequence[Mapping[str, Any]]:
        payload = self._get(f"/person/{_tmdb_id(person_id)}/combined_credits", {"language": "zh-CN"})
        values: list[Mapping[str, Any]] = []
        for key in ("cast", "crew"):
            raw = payload.get(key)
            if isinstance(raw, list):
                values.extend(dict(value) for value in raw if isinstance(value, Mapping))
        return tuple(values)

    def work_details(self, work_id: str, media_type: str) -> Mapping[str, Any]:
        normalized_id = _tmdb_id(work_id)
        kind = str(media_type or "").strip().casefold()
        if not normalized_id or kind not in {"movie", "tv", "series"}:
            raise ValueError("TMDb work identity is invalid")
        path_kind = "tv" if kind in {"tv", "series"} else "movie"
        append = "credits" if path_kind == "tv" else None
        params: dict[str, Any] = {"language": "zh-CN"}
        if append is not None:
            params["append_to_response"] = append
        return self._get(f"/{path_kind}/{normalized_id}", params)

    def _get(self, path: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise RuntimeError("TMDb is not configured")
        query = {"api_key": api_key, **dict(params)}
        try:
            with self._client_factory() as client:
                response = client.get(f"{base_url}{path}", params=query, headers={"Accept": "application/json"})
                if response.status_code == 404:
                    raise PersonNotFoundError(f"TMDb person {path.rsplit('/', 1)[-1]} was not found")
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("TMDb people query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb people query returned an invalid object")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20)


class EmptyPersonTranslation:
    def translate(self, values: Sequence[str]) -> Mapping[str, str]:
        return {str(value): str(value) for value in values if str(value)}


class SqlAlchemyPersonMetadataRepository:
    """Repository for person metadata and actor subscription scan cursors."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def list_for_enrichment(self, *, force: bool) -> Sequence[Mapping[str, Any]]:
        cutoff = datetime.now(UTC) - timedelta(days=30)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_person_id, primary_name, original_name, profile_path,
                               gender, adult, popularity, last_synced_at
                        FROM person_metadata
                        WHERE :force OR last_synced_at IS NULL OR last_synced_at < :cutoff
                        ORDER BY tmdb_person_id
                        """
                    ),
                    {"force": bool(force), "cutoff": cutoff},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def upsert(self, details: Mapping[str, Any], *, primary_name: str | None = None) -> bool:
        person_id = _tmdb_id(details.get("id"))
        if not person_id:
            return False
        aliases = details.get("also_known_as")
        alias_name = next((str(value).strip() for value in aliases or () if str(value).strip()), "")
        name = str(primary_name or details.get("name") or alias_name or person_id).strip()
        original = _person_original_name(details)
        external_ids_raw = details.get("external_ids")
        external_ids: Mapping[str, Any] = dict(external_ids_raw) if isinstance(external_ids_raw, Mapping) else {}
        imdb_id = str(external_ids.get("imdb_id") or "").strip() or None
        douban_id = str(external_ids.get("douban_id") or details.get("douban_celebrity_id") or "").strip() or None
        with self._unit_of_work() as uow:
            # Lock any existing row and resolve the unique IMDb identity before
            # writing.  TMDb occasionally moves an IMDb identity between its
            # person records; retaining one authoritative row avoids a unique
            # constraint failure during a maintenance scan.
            current = (
                uow.session.execute(
                    text(
                        "SELECT tmdb_person_id, primary_name, douban_celebrity_id "
                        "FROM person_metadata WHERE tmdb_person_id = :id FOR UPDATE"
                    ),
                    {"id": int(person_id)},
                )
                .mappings()
                .first()
            )
            conflict_id = None
            if imdb_id:
                conflict_id = uow.session.scalar(
                    text("SELECT tmdb_person_id FROM person_metadata WHERE imdb_id = :imdb_id FOR UPDATE"),
                    {"imdb_id": imdb_id},
                )
                if conflict_id is not None and int(conflict_id) != int(person_id):
                    target_id = int(conflict_id)
                    target = (
                        uow.session.execute(
                            text(
                                "SELECT tmdb_person_id, primary_name, douban_celebrity_id "
                                "FROM person_metadata WHERE tmdb_person_id = :id FOR UPDATE"
                            ),
                            {"id": target_id},
                        )
                        .mappings()
                        .first()
                    )
                    if current is not None:
                        source_douban = str(current.get("douban_celebrity_id") or "").strip() or None
                        target_douban = str((target or {}).get("douban_celebrity_id") or "").strip() or None
                        if source_douban and not target_douban and not douban_id:
                            douban_id = source_douban
                        uow.session.execute(
                            text("DELETE FROM person_metadata WHERE tmdb_person_id = :id"),
                            {"id": int(person_id)},
                        )
                    person_id = str(target_id)
                    current = dict(target or {"tmdb_person_id": target_id, "primary_name": None})
            uow.session.execute(
                text(
                    """
                    INSERT INTO person_metadata (
                        tmdb_person_id, primary_name, imdb_id, douban_celebrity_id,
                        original_name, profile_path, gender, adult, popularity,
                        last_synced_at, last_updated_at
                    ) VALUES (
                        :id, :primary_name, :imdb_id, :douban_id,
                        :original_name, :profile_path, :gender, :adult, :popularity,
                        NOW(), NOW()
                    )
                    ON CONFLICT (tmdb_person_id) DO UPDATE SET
                        primary_name = COALESCE(NULLIF(person_metadata.primary_name, ''), EXCLUDED.primary_name),
                        imdb_id = COALESCE(EXCLUDED.imdb_id, person_metadata.imdb_id),
                        douban_celebrity_id = COALESCE(
                            EXCLUDED.douban_celebrity_id, person_metadata.douban_celebrity_id
                        ),
                        original_name = COALESCE(NULLIF(EXCLUDED.original_name, ''), person_metadata.original_name),
                        profile_path = EXCLUDED.profile_path,
                        gender = EXCLUDED.gender,
                        adult = EXCLUDED.adult,
                        popularity = EXCLUDED.popularity,
                        last_synced_at = NOW(),
                        last_updated_at = NOW()
                    """
                ),
                {
                    "id": int(person_id),
                    "primary_name": str((current or {}).get("primary_name") or name),
                    "imdb_id": imdb_id,
                    "douban_id": douban_id,
                    "original_name": original,
                    "profile_path": details.get("profile_path"),
                    "gender": _integer(details.get("gender")),
                    "adult": details.get("adult") if isinstance(details.get("adult"), bool) else None,
                    "popularity": _float(details.get("popularity")),
                },
            )
            uow.commit()
        return True

    def delete_person_metadata(self, tmdb_person_id: str) -> bool:
        normalized = _tmdb_id(tmdb_person_id)
        if not normalized:
            return False
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text("DELETE FROM person_metadata WHERE tmdb_person_id = :id"),
                {"id": int(normalized)},
            )
            deleted = bool(int(getattr(result, "rowcount", 0) or 0))
            if deleted:
                uow.commit()
        return deleted

    def list_actor_subscriptions(self, subscription_id: int | None = None) -> Sequence[Mapping[str, Any]]:
        filter_sql = " AND id = :subscription_id" if subscription_id is not None else ""
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        f"""
                        SELECT id, tmdb_person_id, actor_name, status,
                               config_start_year, config_media_types,
                               config_genres_include_json, config_genres_exclude_json,
                               config_min_rating, config_main_role_only, config_min_vote_count,
                               last_scanned_tmdb_ids_json
                        FROM actor_subscriptions
                        WHERE status = 'active'
                          {filter_sql}
                        ORDER BY id
                        """
                    ),
                    {"subscription_id": subscription_id} if subscription_id is not None else {},
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def record_actor_scan(self, subscription_id: int, tmdb_ids: Sequence[str]) -> int:
        normalized = tuple(dict.fromkeys(str(value).strip() for value in tmdb_ids if str(value).strip()))
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE actor_subscriptions
                    SET last_scanned_tmdb_ids_json = CAST(:ids AS jsonb),
                        last_checked_at = NOW()
                    WHERE id = :subscription_id AND status = 'active'
                    """
                ),
                {"subscription_id": int(subscription_id), "ids": json.dumps(list(normalized))},
            )
            updated = int(getattr(result, "rowcount", 0) or 0)
            if updated:
                uow.commit()
        return updated

    def reconcile_actor_scan(
        self,
        subscription: Mapping[str, Any],
        works: Sequence[Mapping[str, Any]],
        all_work_ids: Sequence[str],
    ) -> Mapping[str, int]:
        """Apply actor-source status changes and scan cursor in one UoW."""
        from etk.domains.media.subscription_projection import (
            SqlAlchemySubscriptionMediaProjection,
            SubscriptionMediaKey,
        )

        subscription_id = int(subscription.get("id") or 0)
        if subscription_id < 1:
            raise ValueError("actor subscription identity is invalid")
        actor_name = str(subscription.get("actor_name") or "").strip()
        person_id = int(subscription.get("tmdb_person_id") or 0)
        if not actor_name or person_id < 1:
            raise ValueError("actor subscription source identity is invalid")
        source = {"type": "actor_subscription", "id": subscription_id, "name": actor_name, "person_id": person_id}
        source_filter = json.dumps([{"type": "actor_subscription", "id": subscription_id}])
        current_ids = {str(value).strip() for value in all_work_ids if str(value).strip()}
        projection = SqlAlchemySubscriptionMediaProjection()
        added = 0
        with self._unit_of_work() as uow:
            existing = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, in_library, parent_series_tmdb_id,
                               season_number, subscription_sources_json
                        FROM media_metadata
                        WHERE subscription_sources_json @> CAST(:source_filter AS jsonb)
                        FOR UPDATE
                        """
                    ),
                    {"source_filter": source_filter},
                )
                .mappings()
                .all()
            )
            active_keys: set[tuple[str, str]] = set()
            series_with_seasons: set[str] = set()
            incomplete_series: set[str] = set()
            for work in works:
                tmdb_id = _tmdb_id(work.get("id"))
                item_type = _work_item_type(work)
                if not tmdb_id or item_type not in {"Movie", "Series"}:
                    continue
                base_request = {
                    "title": work.get("title") or work.get("name") or tmdb_id,
                    "original_title": work.get("original_title") or work.get("original_name"),
                    "release_date": work.get("release_date") or work.get("first_air_date"),
                    "release_year": _year(work.get("release_date") or work.get("first_air_date")),
                    "poster_path": work.get("poster_path"),
                    "overview": work.get("overview"),
                }
                if item_type == "Series":
                    parent_key = SubscriptionMediaKey(tmdb_id, "Series")
                    projection.ensure_metadata(uow, parent_key, base_request)
                    if bool(work.get("_details_unavailable")):
                        active_keys.add((tmdb_id, "Series"))
                        incomplete_series.add(tmdb_id)
                        continue
                    seasons = _json_list(work.get("seasons"))
                    if not seasons:
                        active_keys.add((tmdb_id, "Series"))
                        status = "PENDING_RELEASE" if _future_release(base_request["release_date"]) else "WANTED"
                        changed = projection.set_status(uow, parent_key, status, base_request, source, None)
                        added += int(changed > 0)
                        continue
                    added_season = False
                    for season in seasons:
                        if not isinstance(season, Mapping):
                            continue
                        season_number = _optional_nonnegative_int(season.get("season_number"))
                        if season_number is None or season_number == 0:
                            continue
                        season_id = _tmdb_id(season.get("id")) or f"{tmdb_id}-S{season_number}"
                        season_request = {
                            "title": season.get("name") or f"{base_request['title']} 第 {season_number} 季",
                            "original_title": season.get("name") or base_request.get("original_title"),
                            "release_date": season.get("air_date") or base_request.get("release_date"),
                            "release_year": _year(season.get("air_date") or base_request.get("release_date")),
                            "poster_path": season.get("poster_path") or base_request.get("poster_path"),
                            "backdrop_path": base_request.get("backdrop_path"),
                            "overview": season.get("overview") or base_request.get("overview"),
                            "season_number": season_number,
                            "parent_series_tmdb_id": tmdb_id,
                        }
                        season_key = SubscriptionMediaKey(season_id, "Season")
                        active_keys.add((season_id, "Season"))
                        added_season = True
                        projection.ensure_metadata(uow, season_key, season_request)
                        status = "PENDING_RELEASE" if _future_release(season_request["release_date"]) else "WANTED"
                        changed = projection.set_status(uow, season_key, status, season_request, source, None)
                        added += int(changed > 0)
                    if added_season:
                        series_with_seasons.add(tmdb_id)
                    active_keys.add((tmdb_id, "Series"))
                else:
                    key = SubscriptionMediaKey(tmdb_id, item_type)
                    active_keys.add((tmdb_id, item_type))
                    status = "PENDING_RELEASE" if _future_release(base_request["release_date"]) else "WANTED"
                    changed = projection.set_status(uow, key, status, base_request, source, None)
                    added += int(changed > 0)
            removed = 0
            for row in existing:
                row_key = (str(row.get("tmdb_id") or "").strip(), str(row.get("item_type") or "").strip())
                parent_id = str(row.get("parent_series_tmdb_id") or "").strip()
                retained = row_key in active_keys or (
                    row.get("item_type") == "Season"
                    and parent_id in current_ids
                    and (parent_id not in series_with_seasons or parent_id in incomplete_series)
                )
                if retained:
                    continue
                remaining = [
                    value
                    for value in _json_list(row.get("subscription_sources_json"))
                    if not (
                        isinstance(value, Mapping)
                        and value.get("type") == "actor_subscription"
                        and str(value.get("id") or "").strip() == str(subscription_id)
                    )
                ]
                if remaining:
                    uow.session.execute(
                        text(
                            """
                            UPDATE media_metadata
                            SET subscription_sources_json = CAST(:sources AS jsonb), last_updated_at = NOW()
                            WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                            """
                        ),
                        {
                            "sources": json.dumps(remaining, ensure_ascii=False),
                            "tmdb_id": row["tmdb_id"],
                            "item_type": row["item_type"],
                        },
                    )
                elif bool(row.get("in_library")):
                    uow.session.execute(
                        text(
                            """
                            UPDATE media_metadata
                            SET subscription_status = 'NONE', subscription_sources_json = '[]'::jsonb,
                                ignore_reason = NULL, last_updated_at = NOW()
                            WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                            """
                        ),
                        {"tmdb_id": row["tmdb_id"], "item_type": row["item_type"]},
                    )
                else:
                    uow.session.execute(
                        text("DELETE FROM media_metadata WHERE tmdb_id = :tmdb_id AND item_type = :item_type"),
                        {"tmdb_id": row["tmdb_id"], "item_type": row["item_type"]},
                    )
                removed += 1
            uow.session.execute(
                text(
                    """
                    UPDATE actor_subscriptions
                    SET last_scanned_tmdb_ids_json = CAST(:ids AS jsonb), last_checked_at = NOW()
                    WHERE id = :subscription_id AND status = 'active'
                    """
                ),
                {"ids": json.dumps(sorted(current_ids)), "subscription_id": subscription_id},
            )
            uow.commit()
        return {"added": added, "removed": removed, "scanned": len(current_ids)}


class NativePeopleTaskService:
    """Execute people catalog, actor tracking and destructive person cleanup."""

    def __init__(
        self,
        catalog: PeopleCatalogPort,
        provider: PersonProviderPort,
        repository: PersonMetadataRepository,
        translation: PersonTranslationPort | None = None,
        *,
        actor_enabled: Callable[[], bool] | None = None,
    ) -> None:
        self._catalog = catalog
        self._provider = provider
        self._repository = repository
        self._translation = translation or EmptyPersonTranslation()
        self._actor_enabled = actor_enabled or (lambda: True)

    def execute(
        self,
        task_key: str,
        progress: PeopleTaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        if task_key not in ACTOR_TASK_KEYS:
            raise LookupError(f"unknown native people task: {task_key}")
        if task_key == "actor-translation":
            return self._translate(progress)
        if task_key == "enrich-aliases":
            return self._enrich(progress, force=bool(parameters.get("force_full_update", False)))
        if task_key == "actor-tracking":
            subscription_id = parameters.get("subscription_id")
            actor_name = str(parameters.get("actor_name") or "").strip() or None
            return self._track(
                progress,
                subscription_id=int(subscription_id) if subscription_id is not None else None,
                actor_name=actor_name,
            )
        if task_key == "merge-duplicate-actors":
            return self._merge(progress)
        return self._purge(progress)

    def _translate(self, progress: PeopleTaskProgress) -> TaskOutcome:
        if not bool(self._actor_enabled()):
            progress.update(100, "AI人物翻译未启用，已跳过。")
            return TaskOutcome.skipped("AI人物翻译未启用，已跳过。", {"updated": 0})
        persons = tuple(self._catalog.list_persons())
        candidates = [person for person in persons if _needs_translation(person)]
        if not candidates:
            progress.update(100, "没有需要翻译的人物。")
            return TaskOutcome.skipped("没有需要翻译的人物。", {"updated": 0, "scanned": len(persons)})
        names = tuple(
            dict.fromkeys(str(person.get("Name") or "").strip() for person in candidates if person.get("Name"))
        )
        translated = dict(self._translation.translate(names))
        updated = 0
        failed = 0
        for index, person in enumerate(candidates, 1):
            progress.checkpoint()
            person_id = str(person.get("Id") or "").strip()
            old_name = str(person.get("Name") or "").strip()
            new_name = str(translated.get(old_name) or "").strip()
            if not person_id or not new_name or new_name == old_name:
                continue
            try:
                if self._catalog.update_person_name(person_id, new_name):
                    updated += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                logger.exception("native person translation failed for %s", person_id)
            progress.update(min(95, int(index * 95 / len(candidates))), f"正在翻译人物 ({index}/{len(candidates)})")
        return _finish(progress, "人物翻译", updated, failed, len(candidates))

    def _enrich(self, progress: PeopleTaskProgress, *, force: bool) -> TaskOutcome:
        candidates = tuple(self._repository.list_for_enrichment(force=force))
        if not candidates:
            progress.update(100, "没有需要补充的人物资料。")
            return TaskOutcome.skipped("没有需要补充的人物资料。", {"updated": 0})
        updated = 0
        failed = 0
        for index, person in enumerate(candidates, 1):
            progress.checkpoint()
            try:
                person_id = str(person.get("tmdb_person_id") or "")
                details = self._provider.details(person_id)
                if self._repository.upsert(details, primary_name=str(person.get("primary_name") or "").strip() or None):
                    updated += 1
            except PersonNotFoundError:
                remove = getattr(self._repository, "delete_person_metadata", None)
                if callable(remove) and remove(str(person.get("tmdb_person_id") or "")):
                    updated += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                logger.exception("native person enrichment failed for %s", person.get("tmdb_person_id"))
            progress.update(min(95, int(index * 95 / len(candidates))), f"正在补充人物资料 ({index}/{len(candidates)})")
        return _finish(progress, "人物资料补充", updated, failed, len(candidates))

    def _track(
        self,
        progress: PeopleTaskProgress,
        *,
        subscription_id: int | None,
        actor_name: str | None,
    ) -> TaskOutcome:
        subscriptions = tuple(self._repository.list_actor_subscriptions(subscription_id))
        if actor_name:
            subscriptions = tuple(
                row for row in subscriptions if str(row.get("actor_name") or "").strip() == actor_name
            )
        if not subscriptions:
            progress.update(100, "没有需要刷新的演员订阅。")
            return TaskOutcome.skipped("没有需要刷新的演员订阅。", {"subscriptions": 0, "updated": 0})
        updated = 0
        failed = 0
        discovered = 0
        removed = 0
        for index, subscription in enumerate(subscriptions, 1):
            progress.checkpoint()
            try:
                credits = self._provider.credits(str(subscription.get("tmdb_person_id") or ""))
                works = _clean_actor_works(tuple(dict(value) for value in credits if isinstance(value, Mapping)))
                works = _enrich_actor_works(
                    self._provider,
                    works,
                    str(subscription.get("tmdb_person_id") or ""),
                )
                if not works:
                    # An empty/filtered TMDb credit list is not proof that all
                    # previously tracked sources disappeared.  Preserve the
                    # legacy no-destructive-cleanup behavior and only advance
                    # the scan cursor.
                    updated += self._repository.record_actor_scan(int(subscription["id"]), ())
                    progress.update(
                        min(95, int(index * 95 / len(subscriptions))),
                        f"正在刷新演员订阅 ({index}/{len(subscriptions)})",
                    )
                    continue
                all_ids = tuple(
                    str(value.get("id") or "").strip()
                    for value in works
                    if str(value.get("id") or "").strip().isdigit()
                )
                discovered += len(set(all_ids))
                eligible = tuple(work for work in works if _actor_work_allowed(work, subscription))
                reconcile = getattr(self._repository, "reconcile_actor_scan", None)
                if callable(reconcile):
                    result = cast(Mapping[str, Any], reconcile(subscription, eligible, all_ids))
                    updated += int(result.get("added", 0))
                    removed += int(result.get("removed", 0))
                else:
                    updated += self._repository.record_actor_scan(int(subscription["id"]), all_ids)
            except Exception:
                failed += 1
                logger.exception("native actor tracking failed for %s", subscription.get("id"))
            progress.update(
                min(95, int(index * 95 / len(subscriptions))), f"正在刷新演员订阅 ({index}/{len(subscriptions)})"
            )
        details = {
            "subscriptions": len(subscriptions),
            "updated": updated,
            "removed": removed,
            "failed": failed,
            "discovered": discovered,
        }
        progress.update(100, "演员订阅刷新完成。")
        if failed and not updated:
            return TaskOutcome.failed("演员订阅刷新失败。", details)
        if failed:
            return TaskOutcome.partial("演员订阅刷新部分完成。", details)
        return TaskOutcome.succeeded("演员订阅刷新完成。", details)

    def _merge(self, progress: PeopleTaskProgress) -> TaskOutcome:
        persons = tuple(self._catalog.list_persons())
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for person in persons:
            tmdb_id = _person_tmdb_id(person)
            if tmdb_id:
                groups[tmdb_id].append(person)
        duplicates = [group for group in groups.values() if len(group) > 1]
        if not duplicates:
            progress.update(100, "没有发现重复人物。")
            return TaskOutcome.skipped("没有发现重复人物。", {"merged": 0, "duplicates": 0})
        media = tuple(self._catalog.list_media_items())
        references: dict[str, int] = defaultdict(int)
        for item in media:
            for person in item.get("People", []):
                if isinstance(person, Mapping):
                    person_id = str(person.get("Id") or "").strip()
                    if person_id:
                        references[person_id] += 1
        merged = 0
        failed = 0
        for group in duplicates:
            keeper = min(
                group,
                key=lambda person: (-references.get(str(person.get("Id") or "").strip(), 0), _person_sort_key(person)),
            )
            keeper_id = str(keeper.get("Id") or "").strip()
            if not keeper_id:
                failed += len(group)
                continue
            duplicate_ids = {
                str(person.get("Id") or "").strip()
                for person in group
                if str(person.get("Id") or "").strip() and str(person.get("Id") or "").strip() != keeper_id
            }
            for item in media:
                item_id = str(item.get("Id") or "").strip()
                people = [dict(value) for value in item.get("People", []) if isinstance(value, Mapping)]
                if not item_id or not any(str(value.get("Id") or "").strip() in duplicate_ids for value in people):
                    continue
                progress.checkpoint()
                rewritten = []
                keeper_seen = False
                for value in people:
                    value_id = str(value.get("Id") or "").strip()
                    if value_id in duplicate_ids:
                        if keeper_seen:
                            continue
                        value["Id"] = keeper_id
                        value["Name"] = keeper.get("Name") or value.get("Name")
                        keeper_seen = True
                    elif value_id == keeper_id:
                        keeper_seen = True
                    rewritten.append(value)
                try:
                    if self._catalog.update_media_people(item_id, rewritten):
                        merged += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
                    logger.exception("native duplicate-person media update failed for %s", item_id)
            latest_media = tuple(self._catalog.list_media_items())
            still_referenced = {
                str(value.get("Id") or "").strip()
                for item in latest_media
                if isinstance(item.get("People"), list)
                for value in item["People"]
                if isinstance(value, Mapping) and str(value.get("Id") or "").strip() in duplicate_ids
            }
            for duplicate_id in duplicate_ids:
                progress.checkpoint()
                if duplicate_id in still_referenced:
                    failed += 1
                    continue
                try:
                    if self._catalog.delete_person(duplicate_id) and not self._catalog.person_exists(duplicate_id):
                        merged += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1
                    logger.exception("native duplicate-person delete failed for %s", duplicate_id)
        return _finish(progress, "重复人物合并", merged, failed, len(duplicates))

    def _purge(self, progress: PeopleTaskProgress) -> TaskOutcome:
        persons = tuple(self._catalog.list_persons())
        media = tuple(self._catalog.list_media_items())
        referenced = {
            str(value.get("Id") or "").strip()
            for item in media
            if isinstance(item.get("People"), list)
            for value in item["People"]
            if isinstance(value, Mapping) and str(value.get("Id") or "").strip()
        }
        ghosts = [person for person in persons if str(person.get("Id") or "").strip() not in referenced]
        if not ghosts:
            progress.update(100, "没有发现幽灵人物。")
            return TaskOutcome.skipped("没有发现幽灵人物。", {"deleted": 0, "scanned": len(persons)})
        deleted = 0
        failed = 0
        for person in ghosts:
            progress.checkpoint()
            person_id = str(person.get("Id") or "").strip()
            try:
                current_references = {
                    str(value.get("Id") or "").strip()
                    for item in self._catalog.list_media_items()
                    if isinstance(item.get("People"), list)
                    for value in item["People"]
                    if isinstance(value, Mapping) and str(value.get("Id") or "").strip()
                }
                if person_id in current_references:
                    failed += 1
                    continue
                if self._catalog.delete_person(person_id) and not self._catalog.person_exists(person_id):
                    deleted += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
                logger.exception("native ghost-person delete failed for %s", person_id)
        return _finish(progress, "幽灵人物清理", deleted, failed, len(ghosts))


def _finish(progress: PeopleTaskProgress, label: str, updated: int, failed: int, scanned: int) -> TaskOutcome:
    progress.update(100, f"{label}完成。")
    details = {"updated": updated, "failed": failed, "scanned": scanned}
    if failed and not updated:
        return TaskOutcome.failed(f"{label}失败。", details)
    if failed:
        return TaskOutcome.partial(f"{label}部分完成。", details)
    return TaskOutcome.succeeded(f"{label}完成。", details)


def _emby_identity(settings: Settings) -> tuple[str, str, str]:
    base_url = settings.emby_server_url.strip().rstrip("/")
    user_id = settings.emby_user_id.strip()
    token = settings.emby_api_key.get_secret_value().strip()
    if not base_url or not user_id or not token:
        raise RuntimeError("people task requires configured Emby authorization")
    return base_url, user_id, token


def _required_text(value: object, field: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ValueError(f"{field} must be a 1-{maximum} character string")
    return normalized


def _tmdb_id(value: object) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized.isdigit() else ""


def _person_tmdb_id(person: Mapping[str, Any]) -> str:
    providers = person.get("ProviderIds") or person.get("provider_ids")
    if isinstance(providers, Mapping):
        return _tmdb_id(providers.get("Tmdb") or providers.get("tmdb"))
    return _tmdb_id(person.get("tmdb_person_id"))


def _person_original_name(details: Mapping[str, Any]) -> str:
    direct = str(details.get("original_name") or "").strip()
    if direct:
        return direct
    translations = details.get("translations")
    values = translations.get("translations") if isinstance(translations, Mapping) else ()
    if isinstance(values, list):
        for entry in values:
            if not isinstance(entry, Mapping):
                continue
            language = str(entry.get("iso_639_1") or "").strip().casefold()
            data = entry.get("data")
            if language in {"en", "ja", "ko"} and isinstance(data, Mapping):
                candidate = str(data.get("name") or "").strip()
                if candidate:
                    return candidate
    return str(details.get("name") or "").strip()


def _person_sort_key(person: Mapping[str, Any]) -> tuple[int, str]:
    raw_id = str(person.get("Id") or "").strip()
    try:
        return int(raw_id), raw_id
    except ValueError:
        return 2**31 - 1, raw_id


def _work_item_type(work: Mapping[str, Any]) -> str:
    raw = str(work.get("media_type") or work.get("item_type") or "").strip().casefold()
    return {"movie": "Movie", "tv": "Series", "series": "Series"}.get(raw, "")


def _clean_actor_works(works: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    """Drop unusable credits and retain one deterministic entry per title.

    TMDb can return the same title more than once (for example through a
    localized and an original credit).  The legacy actor scan kept the most
    popular entry while preserving the first-seen title order.  Keeping that
    behavior here also makes the scan cursor independent of duplicate credit
    rows without mutating the provider payload.
    """
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for value in works:
        if not isinstance(value, Mapping):
            continue
        work = dict(value)
        if not _tmdb_id(work.get("id")):
            continue
        title = str(work.get("title") or work.get("name") or "").strip()
        release = str(work.get("release_date") or work.get("first_air_date") or "").strip()
        if not title or not release or not str(work.get("poster_path") or "").strip():
            continue
        normalized_title = "".join(
            character
            for character in unicodedata.normalize("NFKD", title).casefold()
            if not unicodedata.combining(character) and character.isalnum()
        )
        if not normalized_title:
            continue
        grouped.setdefault(normalized_title, []).append(work)

    cleaned: list[Mapping[str, Any]] = []
    for group in grouped.values():
        # ``max`` is stable for equal keys, so ties retain the first credit.
        cleaned.append(max(group, key=lambda value: _float(value.get("popularity")) or 0.0))
    return tuple(cleaned)


def _enrich_actor_works(
    provider: PersonProviderPort,
    works: Sequence[Mapping[str, Any]],
    person_id: str,
) -> tuple[Mapping[str, Any], ...]:
    """Add TV cast order and season details when the provider exposes them."""
    fetch_details = getattr(provider, "work_details", None)
    if not callable(fetch_details):
        return tuple(dict(work) for work in works)
    enriched: list[Mapping[str, Any]] = []
    for work in works:
        copied = dict(work)
        if _work_item_type(copied) != "Series":
            enriched.append(copied)
            continue
        try:
            details = fetch_details(str(copied.get("id") or ""), "tv")
        except Exception:
            logger.warning("native actor work detail lookup failed for %s", copied.get("id"), exc_info=True)
            copied["_details_unavailable"] = True
            enriched.append(copied)
            continue
        if isinstance(details, Mapping):
            cast = details.get("credits", {}).get("cast", []) if isinstance(details.get("credits"), Mapping) else []
            if isinstance(cast, list):
                for member in cast:
                    if not isinstance(member, Mapping) or _tmdb_id(member.get("id")) != _tmdb_id(person_id):
                        continue
                    copied["order"] = member.get("order", 999)
                    break
            seasons = details.get("seasons")
            if isinstance(seasons, list):
                copied["seasons"] = tuple(dict(season) for season in seasons if isinstance(season, Mapping))
        enriched.append(copied)
    return tuple(enriched)


def _actor_work_allowed(work: Mapping[str, Any], subscription: Mapping[str, Any]) -> bool:
    if bool(work.get("_details_unavailable")):
        return False
    tmdb_id = _tmdb_id(work.get("id"))
    item_type = _work_item_type(work)
    release = str(work.get("release_date") or work.get("first_air_date") or "").strip()
    title = str(work.get("title") or work.get("name") or "").strip()
    if not tmdb_id or not item_type or not release or not work.get("poster_path") or not title:
        return False
    try:
        if int(release[:4]) < int(subscription.get("config_start_year") or 0):
            return False
    except (TypeError, ValueError):
        pass
    configured_types = {
        "Series" if value.strip().casefold() in {"tv", "series"} else value.strip().capitalize()
        for value in str(subscription.get("config_media_types") or "Movie,TV").split(",")
        if value.strip()
    }
    if item_type not in configured_types:
        return False
    genres = {str(value) for value in work.get("genre_ids", []) if str(value).strip()}
    include = {str(value) for value in _json_list(subscription.get("config_genres_include_json"))}
    exclude = {str(value) for value in _json_list(subscription.get("config_genres_exclude_json"))}
    if exclude and genres.intersection(exclude):
        return False
    if include and not genres.intersection(include):
        return False
    try:
        rating = float(work.get("vote_average") or 0)
        votes = int(work.get("vote_count") or 0)
        minimum = float(subscription.get("config_min_rating") or 0)
        minimum_votes = int(subscription.get("config_min_vote_count") or 10)
    except (TypeError, ValueError):
        rating, votes, minimum, minimum_votes = 0.0, 0, 0.0, 10
    if minimum > 0 and votes >= minimum_votes and rating and rating < minimum:
        return False
    if not any("\u4e00" <= character <= "\u9fff" for character in title):
        return False
    if bool(subscription.get("config_main_role_only")):
        try:
            if int(work.get("order") or 999) >= 3:
                return False
        except (TypeError, ValueError):
            return False
    return True


def _future_release(value: object) -> bool:
    raw = str(value or "").strip()[:10]
    if not raw:
        return False
    try:
        return date.fromisoformat(raw) > date.today()
    except ValueError:
        return False


def _year(value: object) -> int | None:
    raw = str(value or "")[:4]
    return int(raw) if raw.isdigit() else None


def _optional_nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        normalized = int(cast(Any, value))
    except (TypeError, ValueError):
        return None
    return normalized if normalized >= 0 else None


def _json_list(value: object) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    return [item for item in value] if isinstance(value, list) else []


def _needs_translation(person: Mapping[str, Any]) -> bool:
    name = str(person.get("Name") or "").strip()
    return bool(name and not any("\u4e00" <= character <= "\u9fff" for character in name))


def _integer(value: object) -> int | None:
    try:
        return None if value is None or isinstance(value, bool) else int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _float(value: object) -> float | None:
    try:
        return None if value is None or isinstance(value, bool) else float(cast(Any, value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "ACTOR_TASK_KEYS",
    "HttpxPeopleCatalogAdapter",
    "HttpxTmdbPersonAdapter",
    "NativePeopleTaskService",
    "PeopleCatalogPort",
    "PeopleTaskProgress",
    "PersonMetadataRepository",
    "PersonNotFoundError",
    "PersonProviderPort",
    "PersonTranslationPort",
    "SqlAlchemyPersonMetadataRepository",
]
