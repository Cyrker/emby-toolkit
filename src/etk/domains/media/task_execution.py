"""Processor-free execution for the remaining P5 media task keys.

The legacy task functions mixed inventory reads, provider queries and local
updates in one processor object.  This module keeps the task boundary small:
the application service owns iteration/progress/outcome mapping and the
ports own provider and persistence details.  No request or subprocess is
performed by the router itself.
"""

from __future__ import annotations

import base64
import inspect
import json
import logging
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import quote

import httpx
from sqlalchemy import text

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome
from etk.policy_defaults import GENRE_TRANSLATION_PATCH

from .metadata_quality import metadata_gaps, placeholder_episode_title
from .policies import normalize_media_path
from .reprocess_metadata import ReprocessMetadataProvider, identify_root, prepare_metadata

logger = logging.getLogger(__name__)

MEDIA_TASK_KEYS = frozenset(
    {
        "populate-metadata",
        "backfill-media-metadata",
        "role-translation",
    }
)


class MediaTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...


class MediaCatalogPort(Protocol):
    def list_items(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]: ...


class MediaMetadataProvider(Protocol):
    def details(self, tmdb_id: str, media_type: str) -> Mapping[str, Any]: ...


class MediaTranslationPort(Protocol):
    def translate(self, values: Sequence[str]) -> Mapping[str, str]: ...


class MediaMetadataTaskRepository(Protocol):
    def reprocess_cache(self, item: Mapping[str, Any], root: Mapping[str, Any]) -> Mapping[str, Any] | None: ...

    def list_backfill_candidates(self) -> Sequence[Mapping[str, Any]]: ...

    def upsert_metadata(
        self,
        item: Mapping[str, Any],
        details: Mapping[str, Any],
        *,
        preserve_people: bool,
    ) -> bool: ...

    def list_role_candidates(self) -> Sequence[Mapping[str, Any]]: ...

    def update_roles(
        self,
        tmdb_id: str,
        item_type: str,
        actors: Sequence[Mapping[str, Any]],
    ) -> bool: ...

    def mark_offline(self, emby_ids: Sequence[str]) -> int: ...


class ScreenshotRepository(Protocol):
    def list_candidates(self) -> Sequence[Mapping[str, Any]]: ...

    def record_screenshot(self, item: Mapping[str, Any], *, content_hash: str) -> bool: ...


class ScreenshotCapturePort(Protocol):
    def capture(self, path: str) -> bytes | None: ...


class ScreenshotUploadPort(Protocol):
    def upload(self, item_id: str, content: bytes) -> bool: ...

    def readback(self, item_id: str) -> bool: ...


class HttpxEmbyScreenshotUploadAdapter:
    """Upload a captured JPEG and verify the resulting Primary image."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        if max_bytes < 1:
            raise ValueError("screenshot max_bytes must be positive")
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._max_bytes = max_bytes

    def upload(self, item_id: str, content: bytes) -> bool:
        normalized_id = str(item_id or "").strip()
        if not normalized_id or not content or len(content) > self._max_bytes:
            return False
        base_url, _user_id, token = _emby_identity(self._settings())
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(normalized_id, safe='')}/Images/Primary",
                    headers={"X-Emby-Token": token, "Content-Type": "image/jpeg"},
                    content=base64.b64encode(content),
                )
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby screenshot upload failed") from exc
        if response.status_code not in {200, 204}:
            if 500 <= response.status_code:
                raise RuntimeError(f"Emby screenshot upload returned HTTP {response.status_code}")
            return False
        return True

    def readback(self, item_id: str) -> bool:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            return False
        base_url, user_id, token = _emby_identity(self._settings())
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(normalized_id, safe='')}",
                    params={"Fields": "ImageTags"},
                    headers={"X-Emby-Token": token, "Accept": "application/json"},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby screenshot readback failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby screenshot readback returned an invalid item")
        tags = payload.get("ImageTags")
        if isinstance(tags, Mapping) and tags.get("Primary"):
            return True
        return bool(payload.get("PrimaryImageTag"))

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout), trust_env=False)


class HttpxMediaCatalogAdapter:
    """Read-only Emby inventory used by metadata and screenshot tasks."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        page_size: int = 200,
    ) -> None:
        if page_size < 1 or page_size > 1000:
            raise ValueError("media catalog page_size must be between 1 and 1000")
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._page_size = page_size

    def list_items(self, library_ids: Sequence[str]) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        libraries = tuple(dict.fromkeys(str(value).strip() for value in library_ids if str(value).strip()))
        if not base_url or not user_id or not token:
            raise RuntimeError("metadata task requires configured Emby authorization")
        if not libraries:
            raise ValueError("metadata task requires at least one Emby library")

        result: list[Mapping[str, Any]] = []
        for library_id in libraries:
            start = 0
            while True:
                payload = self._get(
                    base_url,
                    user_id,
                    token,
                    {
                        "ParentId": library_id,
                        "Recursive": "true",
                        "IncludeItemTypes": "Movie,Series,Season,Episode",
                        "Fields": (
                            "ProviderIds,Type,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,"
                            "OfficialRating,ProductionYear,Path,Overview,SeriesId,SeasonId,ParentIndexNumber,IndexNumber,Id,LockedFields,LockData,People,ImageTags"
                        ),
                        "StartIndex": start,
                        "Limit": self._page_size,
                    },
                )
                items = payload.get("Items")
                if not isinstance(items, list):
                    raise RuntimeError("Emby inventory returned an invalid item list")
                result.extend({**item, "_source_library_id": library_id} for item in items if isinstance(item, Mapping))
                total = int(payload.get("TotalRecordCount") or len(items))
                start += len(items)
                if not items or start >= total:
                    break
        return tuple(result)

    def item(self, item_id: str) -> Mapping[str, Any] | None:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            return None
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not user_id or not token:
            raise RuntimeError("metadata task requires configured Emby authorization")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(normalized_id, safe='')}",
                    params={
                        "Fields": (
                            "ProviderIds,Type,Name,OriginalTitle,PremiereDate,CommunityRating,Genres,Studios,"
                            "OfficialRating,ProductionYear,Path,Overview,SeriesId,SeasonId,ParentIndexNumber,IndexNumber,Id,LockedFields,LockData,People,ImageTags"
                        )
                    },
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                )
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby item query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby item query returned an invalid object")
        return dict(payload)

    def descendants(self, item_id: str) -> Sequence[Mapping[str, Any]]:
        return tuple(
            {key: value for key, value in item.items() if key != "_source_library_id"}
            for item in self.list_items((item_id,))
        )

    def _get(
        self,
        base_url: str,
        user_id: str,
        token: str,
        params: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                    params=dict(params),
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby inventory query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby inventory returned an invalid object")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout), trust_env=False)


class HttpxTmdbMetadataAdapter:
    """Bounded TMDb metadata read adapter for movie and television roots."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def details(self, tmdb_id: str, media_type: str) -> Mapping[str, Any]:
        normalized_id = str(tmdb_id or "").strip()
        kind = str(media_type or "").strip().lower()
        if not normalized_id or not normalized_id.isdigit() or kind not in {"movie", "tv"}:
            raise ValueError("TMDb metadata identity is invalid")
        return self._query(
            f"/{kind}/{normalized_id}",
            {
                "append_to_response": "credits,release_dates,external_ids,keywords"
                if kind == "movie"
                else "credits,aggregate_credits,external_ids,content_ratings,keywords",
            },
        )

    def season_details(self, tmdb_id: str, season_number: int) -> Mapping[str, Any]:
        if not str(tmdb_id).isdigit() or isinstance(season_number, bool) or season_number < 0:
            raise ValueError("TMDb season identity is invalid")
        return self._query(f"/tv/{tmdb_id}/season/{season_number}", {"append_to_response": "credits"})

    def episode_details(self, tmdb_id: str, season_number: int, episode_number: int) -> Mapping[str, Any]:
        if not str(tmdb_id).isdigit() or type(season_number) is not int or season_number < 0:
            raise ValueError("TMDb season identity is invalid")
        if type(episode_number) is not int or episode_number < 0:
            raise ValueError("TMDb episode identity is invalid")
        return self._query(f"/tv/{tmdb_id}/season/{season_number}/episode/{episode_number}", {})

    def images(self, tmdb_id: str, kind: str, *, season: int | None, episode: int | None) -> Mapping[str, Any]:
        if not str(tmdb_id).isdigit() or kind not in {"Movie", "Series", "Season", "Episode"}:
            raise ValueError("TMDb image identity is invalid")
        endpoint = f"/{'movie' if kind == 'Movie' else 'tv'}/{tmdb_id}"
        if kind in {"Season", "Episode"}:
            if type(season) is not int or season < 0:
                raise ValueError("TMDb image season is invalid")
            endpoint += f"/season/{season}"
        if kind == "Episode":
            if type(episode) is not int or episode < 0:
                raise ValueError("TMDb image episode is invalid")
            endpoint += f"/episode/{episode}"
        # 图片端点不按文字语言过滤，交由既有图片语言优先级选择。
        return self._query(endpoint + "/images", {"language": None})

    def search(self, name: str, media_type: str) -> Mapping[str, Any]:
        if media_type not in {"movie", "tv"} or not name.strip():
            raise ValueError("TMDb search identity is invalid")
        return self._query(f"/search/{media_type}", {"query": name})

    def _query(self, path: str, parameters: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise RuntimeError("TMDb is not configured")
        params = {
            "api_key": api_key,
            "language": "zh-CN",
            **parameters,
        }
        params = {key: value for key, value in params.items() if value is not None}
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    params=params,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("TMDb metadata query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb metadata returned an invalid object")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=20, trust_env=False)


class EmptyMediaTranslation:
    """Deterministic no-op translator used when AI translation is disabled."""

    def translate(self, values: Sequence[str]) -> Mapping[str, str]:
        return {str(value): str(value) for value in values if str(value)}


class SqlAlchemyMediaMetadataTaskRepository:
    """SQL owner for metadata task snapshots and compare-and-swap writes."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reprocess_cache(self, item: Mapping[str, Any], root: Mapping[str, Any]) -> Mapping[str, Any] | None:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text("""
                    SELECT * FROM media_metadata
                    WHERE item_type = :kind
                      AND emby_item_ids_json @> CAST(:ids AS jsonb)
                """),
                    {"kind": item.get("Type"), "ids": json.dumps([str(item.get("Id"))])},
                )
                .mappings()
                .all()
            )
        if len(rows) != 1:
            return None
        row = dict(rows[0])
        root_id = str((root.get("ProviderIds") or {}).get("Tmdb") or (root.get("ProviderIds") or {}).get("tmdb") or "")
        kind = item.get("Type")
        if kind in {"Movie", "Series"}:
            if str(row["tmdb_id"]) != root_id or not row.get("genres_json"):
                return None
        elif (
            str(row.get("parent_series_tmdb_id")) != root_id
            or row.get("season_number")
            != (item.get("IndexNumber") if kind == "Season" else item.get("ParentIndexNumber"))
            or (kind == "Episode" and row.get("episode_number") != item.get("IndexNumber"))
        ):
            return None
        current_id = str((item.get("ProviderIds") or {}).get("Tmdb") or "")
        if current_id and current_id != str(row["tmdb_id"]):
            return None
        if (
            (row.get("metadata_schema_version") or 0) < 1
            or not str(row.get("title") or "").strip()
            or not str(row.get("overview") or "").strip()
            or not row.get("release_year")
            or not row.get("actors_ready")
            or not any(
                normalize_media_path(asset.get("path")) == normalize_media_path(item.get("Path"))
                for asset in row.get("asset_details_json") or []
            )
        ):
            return None
        return row

    def list_backfill_candidates(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, emby_item_ids_json
                        FROM media_metadata
                        WHERE in_library IS TRUE
                          AND item_type IN ('Movie', 'Series')
                          AND (
                              metadata_schema_version < 1
                              OR NULLIF(BTRIM(title), '') IS NULL
                              OR NULLIF(BTRIM(overview), '') IS NULL
                              OR release_year IS NULL
                              OR genres_json IS NULL
                              OR genres_json = '[]'::jsonb
                          )
                        ORDER BY tmdb_id, item_type
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def upsert_metadata(
        self,
        item: Mapping[str, Any],
        details: Mapping[str, Any],
        *,
        preserve_people: bool,
    ) -> bool:
        tmdb_id = _tmdb_id(item, details)
        item_type = _root_item_type(item)
        parent_id = str(item.get("_series_tmdb_id") or "")
        season_number = item.get("_season_number")
        episode_number = item.get("_episode_number")
        if item_type in {"Season", "Episode"}:
            tmdb_id = str(details.get("id") or "")
            if not parent_id.isdigit() or season_number is None:
                return False
            if item_type == "Episode" and episode_number is None:
                return False
        if not tmdb_id.isdigit() or item_type not in {"Movie", "Series", "Season", "Episode"}:
            return False
        emby_id = str(item.get("Id") or "").strip()
        with self._unit_of_work() as uow:
            if item_type in {"Season", "Episode"}:
                conflict = uow.session.execute(
                    text("""
                    SELECT tmdb_id FROM media_metadata
                    WHERE parent_series_tmdb_id = :parent AND item_type = :kind
                      AND season_number = :season AND episode_number IS NOT DISTINCT FROM :episode
                      AND tmdb_id <> :tmdb_id LIMIT 1
                """),
                    {
                        "parent": parent_id,
                        "kind": item_type,
                        "season": season_number,
                        "episode": episode_number,
                        "tmdb_id": tmdb_id,
                    },
                ).first()
                if conflict is not None:
                    raise ValueError("现有季集记录使用不同的 TMDb 身份，请核对后再处理，未覆盖旧记录。")
            existing = (
                uow.session.execute(
                    text(
                        """
                        SELECT emby_item_ids_json, file_sha1_json, file_pickcode_json,
                               actors_json, directors_json, asset_details_json,
                               official_rating_json, poster_path, backdrop_path, logo_path, thumb_path, title, overview
                        FROM media_metadata
                        WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                        FOR UPDATE
                        """
                    ),
                    {"tmdb_id": tmdb_id, "item_type": item_type},
                )
                .mappings()
                .first()
            )
            current = dict(existing) if existing is not None else None
            emby_ids = _json_strings(current.get("emby_item_ids_json") if current else None)
            file_sha1s = _json_strings(current.get("file_sha1_json") if current else None)
            file_pickcodes = _json_strings(current.get("file_pickcode_json") if current else None)
            if emby_id and emby_id not in emby_ids:
                emby_ids.append(emby_id)
            values = _metadata_values(item, details, tmdb_id, item_type, emby_ids)
            if item.get("_preserve_cached_images") and current:
                if not str(details.get("overview") or "").strip() and current.get("overview"):
                    values["overview"] = current["overview"]
                if item_type == "Episode" and placeholder_episode_title(values.get("title")):
                    if not placeholder_episode_title(current.get("title")):
                        values["title"] = current["title"]
            previous_ratings = (current or {}).get("official_rating_json")
            values["official_rating_json"] = json.dumps(
                {
                    **(dict(previous_ratings) if isinstance(previous_ratings, Mapping) else {}),
                    **_official_ratings(details),
                },
                ensure_ascii=False,
            )
            for field in ("imdb_id", "original_language", "tagline", "runtime_minutes"):
                if values.get(field) is None:
                    values.pop(field, None)
            if item_type in {"Season", "Episode"}:
                values.update(
                    parent_series_tmdb_id=parent_id, season_number=season_number, episode_number=episode_number
                )
                if details.get("air_date"):
                    values["release_date"] = details["air_date"]
                    values["release_year"] = _year(details["air_date"])
                if item_type == "Episode":
                    values["thumb_path"] = details.get("still_path")
                    values["poster_path"] = details.get("still_path")
                    values["runtime_minutes"] = _integer(details.get("runtime"))
            if item.get("_preserve_cached_images"):
                for column in ("poster_path", "backdrop_path", "logo_path", "thumb_path"):
                    cached_image = (current or {}).get(column)
                    if cached_image:
                        values[column] = cached_image
            # Existing people data is authoritative for a metadata backfill;
            # ordinary sync may replace it only when the provider sent data.
            people = None if preserve_people else details.get("aggregate_credits") or details.get("credits")
            if people is None and existing is not None:
                people = existing.get("actors_json")
            actors = _stored_actors(people) if preserve_people else _actors_json(people)
            previous_actors = _stored_actors((current or {}).get("actors_json"))
            # 手工编辑器只存人物关联，不存 name；保留其名单、顺序和角色。
            if previous_actors and all(not actor.get("name") for actor in previous_actors):
                actors = previous_actors
            else:
                _preserve_chinese_people(actors, previous_actors)
            values["actors_json"] = json.dumps(actors, ensure_ascii=False)
            values["actors_ready"] = bool(actors) or bool(_directors_json(details))
            values["file_sha1_json"] = json.dumps(file_sha1s, ensure_ascii=False)
            values["file_pickcode_json"] = json.dumps(file_pickcodes, ensure_ascii=False)
            directors = _directors_json(details)
            _preserve_chinese_people(directors, _stored_actors((current or {}).get("directors_json")))
            values["directors_json"] = json.dumps(directors, ensure_ascii=False)
            path = normalize_media_path(item.get("Path"))
            if path:
                assets = [dict(asset) for asset in (current or {}).get("asset_details_json") or []]
                existing_asset = next(
                    (asset for asset in assets if normalize_media_path(asset.get("path")) == path), None
                )
                if existing_asset is None:
                    existing_asset = {"path": path}
                    assets.append(existing_asset)
                if emby_id:
                    existing_asset["emby_item_id"] = emby_id
                if item.get("_source_library_id"):
                    existing_asset["source_library_id"] = str(item["_source_library_id"])
                values["asset_details_json"] = json.dumps(assets, ensure_ascii=False)
            columns = ", ".join(values)
            placeholders = ", ".join(f":{column}" for column in values)
            update = ", ".join(
                f"{column} = EXCLUDED.{column}" for column in values if column not in {"tmdb_id", "item_type"}
            )
            uow.session.execute(
                text(
                    f"""
                    INSERT INTO media_metadata ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT (tmdb_id, item_type) DO UPDATE SET {update}, last_updated_at = NOW()
                    """
                ),
                values,
            )
            uow.commit()
        return True

    def list_role_candidates(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, actors_json
                        FROM media_metadata
                        WHERE in_library IS TRUE
                          AND jsonb_typeof(actors_json) = 'array'
                          AND jsonb_array_length(actors_json) > 0
                        ORDER BY tmdb_id, item_type
                        """
                    )
                )
                .mappings()
                .all()
            )
        return tuple(dict(row) for row in rows)

    def update_roles(
        self,
        tmdb_id: str,
        item_type: str,
        actors: Sequence[Mapping[str, Any]],
    ) -> bool:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET actors_json = CAST(:actors AS jsonb),
                        last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {
                    "tmdb_id": str(tmdb_id),
                    "item_type": str(item_type),
                    "actors": json.dumps([dict(actor) for actor in actors], ensure_ascii=False),
                },
            )
            updated = bool(int(getattr(result, "rowcount", 0) or 0))
            if updated:
                uow.commit()
        return updated

    def mark_offline(self, emby_ids: Sequence[str], *, library_ids: Sequence[str] = ()) -> int:
        normalized = tuple(dict.fromkeys(str(value).strip() for value in emby_ids if str(value).strip()))
        if not normalized:
            return 0
        libraries = tuple(dict.fromkeys(str(value).strip() for value in library_ids if str(value).strip()))
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata AS media
                    SET in_library = FALSE,
                        emby_item_ids_json = '[]'::jsonb,
                        asset_details_json = NULL,
                        file_sha1_json = '[]'::jsonb,
                        file_pickcode_json = '[]'::jsonb,
                        last_updated_at = NOW()
                    WHERE media.in_library IS TRUE
                      AND jsonb_typeof(media.emby_item_ids_json) = 'array'
                      AND jsonb_array_length(media.emby_item_ids_json) > 0
                      AND NOT EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements_text(media.emby_item_ids_json) AS known(item_id)
                          WHERE known.item_id = ANY(:emby_ids)
                      )
                      AND (
                          CAST(:library_ids AS text[]) = '{}'::text[]
                          OR EXISTS (
                              SELECT 1
                              FROM jsonb_array_elements(COALESCE(media.asset_details_json, '[]'::jsonb)) AS asset(value)
                              WHERE asset.value->>'source_library_id' = ANY(CAST(:library_ids AS text[]))
                          )
                      )
                    """
                ),
                {"emby_ids": list(normalized), "library_ids": list(libraries)},
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            if count:
                uow.commit()
        return count

    def is_stale(self, item: Mapping[str, Any]) -> bool:
        emby_id = str(item.get("Id") or "").strip()
        providers = item.get("ProviderIds") or item.get("provider_ids")
        tmdb_id = (
            str(providers.get("Tmdb") or providers.get("tmdb") or "").strip() if isinstance(providers, Mapping) else ""
        )
        item_type = _root_item_type(item)
        if not emby_id or not item_type:
            return True
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT last_synced_at, last_updated_at, asset_details_json
                        FROM media_metadata
                        WHERE item_type = :item_type
                          AND (
                              emby_item_ids_json @> CAST(:emby_ids AS jsonb)
                              OR (:tmdb_id <> '' AND tmdb_id = :tmdb_id)
                          )
                        ORDER BY last_updated_at DESC NULLS LAST
                        LIMIT 1
                        """
                    ),
                    {"item_type": item_type, "emby_ids": json.dumps([emby_id]), "tmdb_id": tmdb_id},
                )
                .mappings()
                .first()
            )
        if row is None:
            return True
        path = normalize_media_path(item.get("Path"))
        if path and not any(
            normalize_media_path(asset.get("path")) == path for asset in row.get("asset_details_json") or []
        ):
            return True
        modified = _parse_datetime(item.get("DateModified"))
        synced = _parse_datetime(row.get("last_synced_at"))
        if modified is None or synced is None:
            return False if synced is not None else True
        return modified > synced


class NativeMediaMetadataTaskService:
    """Execute populate, backfill and role-translation without a Processor."""

    def __init__(
        self,
        repository: MediaMetadataTaskRepository,
        catalog: MediaCatalogPort,
        metadata: MediaMetadataProvider,
        translation: MediaTranslationPort | None = None,
        *,
        settings: Callable[[], Settings] | None = None,
    ) -> None:
        self._repository = repository
        self._catalog = catalog
        self._metadata = metadata
        self._translation = translation or EmptyMediaTranslation()
        self._settings = settings

    def execute(
        self,
        task_key: str,
        progress: MediaTaskProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome:
        if task_key not in MEDIA_TASK_KEYS:
            raise LookupError(f"unknown native media task: {task_key}")
        if task_key == "role-translation":
            return self._translate_roles(progress)
        if task_key == "backfill-media-metadata":
            return self._backfill(progress, parameters)
        return self._populate(progress, parameters)

    def _populate(self, progress: MediaTaskProgress, parameters: Mapping[str, Any]) -> TaskOutcome:
        settings = self._settings() if self._settings is not None else None
        libraries = tuple(str(value) for value in (settings.libraries_to_process if settings else ()))
        if not libraries:
            return TaskOutcome.skipped("未配置媒体库，元数据同步已跳过。", {"updated": 0, "offline": 0})
        force = bool(parameters.get("force_full_update", False))
        progress.checkpoint()
        try:
            items = tuple(self._catalog.list_items(libraries))
        except Exception as exc:
            logger.warning("native metadata inventory failed: %s", exc)
            return TaskOutcome.failed("读取 Emby 媒体库失败。", {"error": type(exc).__name__})
        roots = tuple(item for item in items if _root_item_type(item) in {"Movie", "Series"})
        updated = 0
        failures = 0
        seen_emby_ids: list[str] = []
        for index, item in enumerate(roots, 1):
            progress.checkpoint()
            item_id = str(item.get("Id") or "").strip()
            if item_id:
                seen_emby_ids.append(item_id)
            tmdb_id = _tmdb_id(item, {})
            if not tmdb_id:
                continue
            if not force and not _repository_is_stale(self._repository, item):
                continue
            try:
                details = self._metadata.details(tmdb_id, "movie" if _root_item_type(item) == "Movie" else "tv")
                details = self._translate_people(item, details)
                if self._repository.upsert_metadata(item, details, preserve_people=False):
                    updated += 1
            except Exception:
                failures += 1
                logger.exception("native metadata update failed for %s", tmdb_id)
            progress.update(min(95, int(index * 95 / max(len(roots), 1))), f"正在同步媒体元数据 ({index}/{len(roots)})")
        offline = _mark_offline(self._repository, seen_emby_ids, libraries)
        details = {"updated": updated, "offline": offline, "failed": failures, "scanned": len(roots)}
        progress.update(100, "媒体元数据同步完成。")
        if failures and not updated:
            return TaskOutcome.failed("媒体元数据同步失败。", details)
        if failures:
            return TaskOutcome.partial("媒体元数据同步部分完成。", details)
        return TaskOutcome.succeeded("媒体元数据同步完成。", details)

    def reprocess_item_metadata(self, item: Mapping[str, Any], progress: MediaTaskProgress) -> TaskOutcome:
        """保留已核实的 Emby 身份与路径；普通按 TMDb ID 补齐任务没有这些信息。"""
        tmdb_id = _tmdb_id(item, {})
        item_type = _root_item_type(item)
        if not tmdb_id or item_type not in {"Movie", "Series"} or not normalize_media_path(item.get("Path")):
            return TaskOutcome.failed("媒体项缺少 TMDb ID、受支持类型或媒体路径。")
        progress.checkpoint()
        details = self._metadata.details(tmdb_id, "movie" if item_type == "Movie" else "tv")
        details = self._translate_people(item, details)
        updated = self._repository.upsert_metadata(item, details, preserve_people=False)
        if not updated:
            return TaskOutcome.failed("媒体元数据及路径关联保存失败。")
        return TaskOutcome.succeeded("媒体元数据及路径关联已保存。", {"updated": 1})

    def identify_reprocess_root(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        return identify_root(item, cast(ReprocessMetadataProvider, self._metadata))

    def cached_reprocess_metadata(self, item: Mapping[str, Any], root: Mapping[str, Any]) -> Mapping[str, Any] | None:
        row = self._repository.reprocess_cache(item, root)
        if row is None:
            return None
        if metadata_gaps(str(item.get("Type")), row.get("title"), row.get("overview")):
            return None
        expected: dict[str, Any] = {
            "Name": row["title"],
            "Overview": row["overview"],
            "_tmdb_id": str(row["tmdb_id"]),
            "ProductionYear": row["release_year"],
        }
        if item.get("Type") in {"Movie", "Series"}:
            expected["Genres"] = row["genres_json"]
        if row.get("imdb_id"):
            expected["_imdb_id"] = row["imdb_id"]
        if (row.get("official_rating_json") or {}).get("US"):
            expected["OfficialRating"] = row["official_rating_json"]["US"]
        settings = self._settings() if self._settings is not None else Settings()
        limit = settings.max_actors_to_process if settings.max_actors_to_process > 0 else 50
        if item.get("Type") == "Episode" and settings.max_episode_actors_to_process > 0:
            limit = settings.max_episode_actors_to_process
        actors = sorted(row.get("actors_json") or [], key=lambda actor: actor.get("order", 999))[: max(1, limit)]
        directors = row.get("directors_json") or []
        if settings.ai_translate_actor_role and any(
            _needs_people_translation(person.get(field))
            for person in [*actors, *directors]
            for field in ("name", "character")
        ):
            return None
        expected.update(
            _actor_count=len(actors),
            _director_count=len(directors),
            _actor_images=sum(bool(actor.get("profile_path")) for actor in actors),
            _director_images=sum(bool(actor.get("profile_path")) for actor in directors),
        )
        if settings.ai_translate_actor_role:
            expected["_people_name_translations"] = {
                str(person["original_name"]): str(person["name"])
                for person in [*actors, *directors]
                if person.get("original_name") and person.get("name") != person["original_name"]
            }
        if item.get("Type") == "Episode" and (row.get("poster_path") or row.get("thumb_path")):
            expected["_primary_image"] = True
        return expected

    def prepare_reprocess_metadata(
        self,
        item: Mapping[str, Any],
        root: Mapping[str, Any],
        cache: dict[tuple[str, int], Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        if not normalize_media_path(item.get("Path")):
            raise ValueError("媒体路径缺失。")
        # 兼容旧 ETK：整项锁（包括父级继承）不阻断插件回填，也不修改锁定状态。
        if set(item.get("LockedFields") or []) & {"Name", "Overview"}:
            raise ValueError("名称或简介字段已明确锁定，请先确认是否解锁该字段。")
        target, details = prepare_metadata(item, root, cast(ReprocessMetadataProvider, self._metadata), cache)
        details = dict(self._translate_people(target, details))
        gaps = metadata_gaps(
            str(item.get("Type")), details.get("name") or details.get("title"), details.get("overview"),
        )
        if not self._repository.upsert_metadata(
            {**target, "_preserve_cached_images": True}, details, preserve_people=False
        ):
            raise ValueError("季集元数据及路径关联保存失败。")
        expected: dict[str, Any] = {
            "Name": str(details.get("name") or details.get("title")).strip(),
            "_tmdb_id": str(details["id"]),
        }
        if details.get("overview"):
            expected["Overview"] = str(details["overview"]).strip()
        if "分集标题" in gaps:
            expected.pop("Name", None)
        expected["_metadata_gaps"] = gaps
        if details.get("_people_name_translations"):
            expected["_people_name_translations"] = details["_people_name_translations"]
        ratings = _official_ratings(details)
        if ratings.get("US"):
            expected["OfficialRating"] = ratings["US"]
        imdb = (details.get("external_ids") or {}).get("imdb_id") or details.get("imdb_id")
        if imdb:
            expected["_imdb_id"] = imdb
        settings = self._settings() if self._settings is not None else Settings()
        limit = settings.max_actors_to_process if settings.max_actors_to_process > 0 else 50
        if item.get("Type") == "Episode" and settings.max_episode_actors_to_process > 0:
            limit = settings.max_episode_actors_to_process
        actors = sorted(
            _actors_json(details.get("aggregate_credits") or details.get("credits")),
            key=lambda actor: actor.get("order", 999),
        )[: max(1, limit)]
        expected["_actor_count"] = len(actors)
        directors = _directors_json(details)
        expected["_director_count"] = len(directors)
        expected["_actor_images"] = sum(bool(actor.get("profile_path")) for actor in actors)
        expected["_director_images"] = sum(bool(director.get("profile_path")) for director in directors)
        if item.get("Type") == "Episode" and details.get("still_path"):
            expected["_primary_image"] = True
        expected["_log_messages"] = [
            f"已获取 TMDb 元数据（ID={details['id']}）；"
            + (f"资料待补齐：{'、'.join(gaps)}" if gaps else "标题、简介已就绪")
            + "；已保存到缓存",
            f"已获取演职人员：演员 {len(actors)} 人、导演 {len(directors)} 人，"
            f"可用头像 {expected['_actor_images'] + expected['_director_images']} 张",
            f"已获取图片地址：主图 {'有' if details.get('poster_path') or details.get('still_path') else '无'}，"
            f"背景图 {'有' if details.get('backdrop_path') else '无'}，"
            f"分集缩略图 {'有' if details.get('still_path') else '无'}"
            "；地址就绪不代表 Emby 已下载，后续回读确认",
        ]
        if details.get("_cast_source") == "series_fallback":
            expected["_log_messages"].append("特别篇演员资料为空，已回退整剧主演；导演等工作人员仍使用本项资料")
        for label, source in (
            ("主图", details.get("poster_path") or details.get("still_path")),
            ("背景图", details.get("backdrop_path")),
            ("分集缩略图", details.get("still_path")),
        ):
            if source:
                expected["_log_messages"].append(
                    f"{label}下载源文件：{str(source).rsplit('/', 1)[-1]}；由 Emby 按缺图策略获取"
                )
        return expected

    def _backfill(self, progress: MediaTaskProgress, parameters: Mapping[str, Any]) -> TaskOutcome:
        raw_targets = parameters.get("targets")
        if raw_targets is not None:
            candidates = tuple(dict(value) for value in raw_targets if isinstance(value, Mapping))
        else:
            candidates = tuple(dict(value) for value in self._repository.list_backfill_candidates())
        if not candidates:
            progress.update(100, "在库媒体元数据均已达到当前结构版本。")
            return TaskOutcome.skipped("在库媒体元数据均已达到当前结构版本。", {"updated": 0})
        updated = 0
        failures = 0
        for index, candidate in enumerate(candidates, 1):
            progress.checkpoint()
            tmdb_id = str(candidate.get("tmdb_id") or "").strip()
            item_type = str(candidate.get("item_type") or "").strip()
            if not tmdb_id:
                failures += 1
                continue
            raw_type = str(candidate.get("item_type") or candidate.get("media_type") or "").strip().lower()
            item_type = {"movie": "Movie", "tv": "Series", "series": "Series"}.get(raw_type, raw_type.title())
            item = {**candidate, "Id": _first_json_string(candidate.get("emby_item_ids_json")), "Type": item_type}
            try:
                details = self._metadata.details(tmdb_id, "movie" if item_type == "Movie" else "tv")
                if self._repository.upsert_metadata(item, details, preserve_people=True):
                    updated += 1
            except Exception:
                failures += 1
                logger.exception("native metadata backfill failed for %s", tmdb_id)
            progress.update(
                min(95, int(index * 95 / len(candidates))), f"正在补齐媒体元数据 ({index}/{len(candidates)})"
            )
        progress.update(100, "媒体元数据补齐完成。")
        details = {"updated": updated, "failed": failures, "scanned": len(candidates)}
        if failures and not updated:
            return TaskOutcome.failed("媒体元数据补齐失败。", details)
        if failures:
            return TaskOutcome.partial("媒体元数据补齐部分完成。", details)
        return TaskOutcome.succeeded("媒体元数据补齐完成。", details)

    def _translate_people(self, item: Mapping[str, Any], details: Mapping[str, Any]) -> Mapping[str, Any]:
        settings = self._settings() if self._settings is not None else Settings()
        if not settings.ai_translate_actor_role:
            return details
        result = deepcopy(dict(details))
        credits = result.get("aggregate_credits") or result.get("credits") or {}
        limit = settings.max_actors_to_process if settings.max_actors_to_process > 0 else 50
        if item.get("Type") == "Episode" and settings.max_episode_actors_to_process > 0:
            limit = settings.max_episode_actors_to_process
        actors = sorted(credits.get("cast") or [], key=lambda actor: int(actor.get("order", 999)))[:limit]
        directors = [
            person for person in credits.get("crew") or []
            if person.get("job") == "Director"
            or any(job.get("job") == "Director" for job in person.get("jobs") or [])
        ]
        fields = [(person, "name") for person in [*actors, *directors]]
        for actor in actors:
            fields.append((actor, "character"))
            fields.extend((role, "character") for role in actor.get("roles") or [])
        pending = [(value, key) for value, key in fields if _needs_people_translation(value.get(key))]
        if not pending:
            return details
        names = tuple(dict.fromkeys(str(value[key]).strip() for value, key in pending))
        translated = self._translation.translate(names)
        missing = [name for name in names if not translated.get(name) or _needs_people_translation(translated[name])]
        if missing:
            raise RuntimeError(f"演职人员翻译不完整：{len(missing)}/{len(names)} 项未得到中文译文，未覆盖元数据。")
        for value, key in pending:
            original = str(value[key]).strip()
            if key == "name":
                value["original_name"] = original
            value[key] = translated[original]
        result["_people_name_translations"] = {
            str(value["original_name"]): str(value["name"]) for value, key in pending if key == "name"
        }
        logger.info(
            "演职人员中文化完成：%s 个名称/角色（含缓存），演员范围 %s 人、导演 %s 人",
            len(names), len(actors), len(directors),
        )
        return result

    def _translate_roles(self, progress: MediaTaskProgress) -> TaskOutcome:
        if self._settings is not None and not bool(self._settings().ai_translate_actor_role):
            progress.update(100, "AI人物角色翻译未启用，已跳过。")
            return TaskOutcome.skipped("AI人物角色翻译未启用，已跳过。", {"updated": 0})
        candidates = tuple(self._repository.list_role_candidates())
        if not candidates:
            progress.update(100, "没有需要翻译的角色名。")
            return TaskOutcome.skipped("没有需要翻译的角色名。", {"updated": 0})
        values: list[str] = []
        for row in candidates:
            for actor in _json_objects(row.get("actors_json")):
                role = str(actor.get("character") or "").strip()
                if role:
                    values.append(role)
        translated = dict(self._translation.translate(tuple(dict.fromkeys(values))))
        updated = 0
        for row in candidates:
            progress.checkpoint()
            actors = []
            changed = False
            for actor in _json_objects(row.get("actors_json")):
                copied = dict(actor)
                role = str(copied.get("character") or "").strip()
                replacement = str(translated.get(role) or role).strip()
                if replacement and replacement != role:
                    copied["character"] = replacement
                    changed = True
                actors.append(copied)
            if changed and self._repository.update_roles(
                str(row.get("tmdb_id") or ""), str(row.get("item_type") or ""), actors
            ):
                updated += 1
        progress.update(100, "角色名翻译完成。")
        return TaskOutcome.succeeded("角色名翻译完成。", {"updated": updated, "scanned": len(candidates)})


class SqlAlchemyScreenshotRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def list_candidates(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, emby_item_ids_json, asset_details_json
                        FROM media_metadata
                        WHERE in_library IS TRUE
                          AND item_type IN ('Movie', 'Episode', 'Video')
                          AND screenshot_hash IS NULL
                        ORDER BY tmdb_id, item_type
                        """
                    )
                )
                .mappings()
                .all()
            )
        candidates: list[Mapping[str, Any]] = []
        for row in rows:
            ids = _json_strings(row.get("emby_item_ids_json"))
            assets = _json_objects(row.get("asset_details_json"))
            for index, item_id in enumerate(ids):
                asset = assets[index] if index < len(assets) else {}
                if not isinstance(asset, Mapping):
                    asset = {}
                path = str(asset.get("path") or "")
                if not path.lower().endswith(".strm"):
                    continue
                candidates.append(
                    {
                        "tmdb_id": str(row.get("tmdb_id") or ""),
                        "item_type": str(row.get("item_type") or ""),
                        "emby_item_id": item_id,
                        "path": path,
                    }
                )
        return tuple(candidates)

    def record_screenshot(self, item: Mapping[str, Any], *, content_hash: str) -> bool:
        tmdb_id = str(item.get("tmdb_id") or "").strip()
        item_type = str(item.get("item_type") or "").strip()
        if not tmdb_id or not item_type:
            return False
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET screenshot_hash = :content_hash, last_updated_at = NOW()
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {"tmdb_id": tmdb_id, "item_type": item_type, "content_hash": content_hash},
            )
            updated = bool(int(getattr(result, "rowcount", 0) or 0))
            if updated:
                uow.commit()
        return updated


class DisabledScreenshotCapture:
    def capture(self, path: str) -> bytes | None:
        del path
        return None


class FfmpegScreenshotCapture:
    """Bounded temporary-file capture; never writes beside a media file."""

    def __init__(self, *, executable: str = "ffmpeg", timeout_seconds: int = 30) -> None:
        self._executable = executable
        self._timeout = max(1, int(timeout_seconds))

    def capture(self, path: str) -> bytes | None:
        source = str(path or "").strip()
        if not source:
            return None
        with tempfile.TemporaryDirectory(prefix="etk-screenshot-") as directory:
            output = Path(directory) / "frame.jpg"
            try:
                subprocess.run(
                    [self._executable, "-y", "-ss", "00:00:05", "-i", source, "-frames:v", "1", str(output)],
                    check=True,
                    capture_output=True,
                    timeout=self._timeout,
                )
            except (OSError, subprocess.SubprocessError):
                return None
            try:
                return output.read_bytes()
            except OSError:
                return None


class NativeMediaScreenshotBackfillService:
    def __init__(
        self,
        repository: ScreenshotRepository,
        capture: ScreenshotCapturePort,
        upload: ScreenshotUploadPort,
    ) -> None:
        self._repository = repository
        self._capture = capture
        self._upload = upload

    def execute(self, progress: MediaTaskProgress) -> TaskOutcome:
        import hashlib

        progress.checkpoint()
        candidates = tuple(self._repository.list_candidates())
        if not candidates:
            progress.update(100, "没有需要补齐截图的媒体。")
            return TaskOutcome.skipped("没有需要补齐截图的媒体。", {"uploaded": 0, "failed": 0})
        uploaded = 0
        failed = 0
        for index, item in enumerate(candidates, 1):
            progress.checkpoint()
            content = self._capture.capture(str(item.get("path") or ""))
            item_id = str(item.get("emby_item_id") or "").strip()
            if not content or not item_id:
                failed += 1
                continue
            digest = hashlib.sha256(content).hexdigest()
            try:
                already_uploaded = self._upload.readback(item_id)
            except Exception:
                failed += 1
                continue
            if already_uploaded:
                if self._repository.record_screenshot(item, content_hash=digest):
                    uploaded += 1
                else:
                    failed += 1
                progress.update(
                    min(95, int(index * 95 / len(candidates))), f"正在补齐视频截图 ({index}/{len(candidates)})"
                )
                continue
            if not self._upload.upload(item_id, content) or not self._upload.readback(item_id):
                failed += 1
                continue
            if self._repository.record_screenshot(item, content_hash=digest):
                uploaded += 1
            else:
                failed += 1
            progress.update(min(95, int(index * 95 / len(candidates))), f"正在补齐视频截图 ({index}/{len(candidates)})")
        progress.update(100, "视频截图补齐完成。")
        details = {"uploaded": uploaded, "failed": failed, "scanned": len(candidates)}
        if failed and not uploaded:
            return TaskOutcome.failed("视频截图补齐失败。", details)
        if failed:
            return TaskOutcome.partial("视频截图补齐部分完成。", details)
        return TaskOutcome.succeeded("视频截图补齐完成。", details)


def _root_item_type(item: Mapping[str, Any]) -> str:
    raw = str(item.get("item_type") or item.get("Type") or "").strip().title()
    return raw if raw in {"Movie", "Series", "Season", "Episode"} else ""


def _emby_identity(settings: Settings) -> tuple[str, str, str]:
    base_url = settings.emby_server_url.strip().rstrip("/")
    user_id = settings.emby_user_id.strip()
    token = settings.emby_api_key.get_secret_value().strip()
    if not base_url or not user_id or not token:
        raise RuntimeError("media task requires configured Emby authorization")
    return base_url, user_id, token


def _tmdb_id(item: Mapping[str, Any], details: Mapping[str, Any]) -> str:
    providers = item.get("ProviderIds")
    if not isinstance(providers, Mapping):
        providers = item.get("provider_ids")
    if not isinstance(providers, Mapping):
        providers = {}
    value = providers.get("Tmdb") or providers.get("tmdb") or item.get("tmdb_id") or details.get("id")
    normalized = str(value or "").strip()
    return normalized if normalized.isdigit() else ""


def _metadata_values(
    item: Mapping[str, Any],
    details: Mapping[str, Any],
    tmdb_id: str,
    item_type: str,
    emby_ids: Sequence[str],
) -> dict[str, Any]:
    release_date = details.get("release_date") or details.get("first_air_date") or item.get("PremiereDate")
    release_year = _year(release_date) or _integer(item.get("ProductionYear"))
    genres = details.get("genres") or item.get("Genres") or []
    if isinstance(genres, list):
        genres_json = [dict(value) if isinstance(value, Mapping) else {"name": str(value)} for value in genres]
        for genre in genres_json:
            name = str(genre.get("name") or "")
            genre["name"] = GENRE_TRANSLATION_PATCH.get(name, name)
    else:
        genres_json = []
    return {
        "tmdb_id": tmdb_id,
        "item_type": item_type,
        "in_library": True,
        "emby_item_ids_json": json.dumps(list(emby_ids), ensure_ascii=False),
        "file_sha1_json": json.dumps([], ensure_ascii=False),
        "file_pickcode_json": json.dumps([], ensure_ascii=False),
        "title": str(details.get("title") or details.get("name") or item.get("Name") or tmdb_id),
        "original_title": str(
            details.get("original_title") or details.get("original_name") or item.get("OriginalTitle") or ""
        ),
        "original_language": details.get("original_language"),
        "imdb_id": (details.get("external_ids") or {}).get("imdb_id") or details.get("imdb_id"),
        "tagline": details.get("tagline"),
        "runtime_minutes": _integer(details.get("runtime") or next(iter(details.get("episode_run_time") or []), None)),
        "overview": str(details.get("overview") or item.get("Overview") or ""),
        "release_date": release_date if isinstance(release_date, (date, str)) else None,
        "release_year": release_year,
        "poster_path": details.get("poster_path"),
        "backdrop_path": details.get("backdrop_path"),
        "rating": _float(details.get("vote_average") or item.get("CommunityRating")),
        "genres_json": json.dumps(genres_json, ensure_ascii=False),
        "production_companies_json": json.dumps(details.get("production_companies") or [], ensure_ascii=False),
        "networks_json": json.dumps(details.get("networks") or [], ensure_ascii=False),
        "countries_json": json.dumps(details.get("production_countries") or [], ensure_ascii=False),
        "keywords_json": json.dumps(_keywords(details), ensure_ascii=False),
        "official_rating_json": json.dumps(_official_ratings(details), ensure_ascii=False),
        "metadata_ready": True,
        "actors_ready": False,
        "metadata_schema_version": 1,
    }


def _actors_json(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        cast = value.get("cast")
        value = cast
    if not isinstance(value, list):
        return []
    actors: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, Mapping):
            continue
        actor = {
            "tmdb_id": entry.get("id") or entry.get("tmdb_id"),
            "name": entry.get("name"),
            "character": entry.get("character")
            or " / ".join(
                dict.fromkeys(str(role["character"]) for role in entry.get("roles") or [] if role.get("character"))
            ),
            "profile_path": entry.get("profile_path"),
            "order": entry.get("order", 999),
        }
        if entry.get("original_name"):
            actor["original_name"] = entry["original_name"]
        if actor["tmdb_id"] is not None and actor["name"]:
            actors.append(actor)
    return actors


def _stored_actors(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, list):
        return []
    return [dict(entry) for entry in value if isinstance(entry, Mapping)]


def _needs_people_translation(value: object) -> bool:
    name = str(value or "").strip()
    return bool(name) and not any("\u3400" <= char <= "\u9fff" for char in name)


def _preserve_chinese_people(people: list[dict[str, Any]], previous: list[dict[str, Any]]) -> None:
    by_id = {str(person["tmdb_id"]): person for person in previous if person.get("tmdb_id") is not None}
    for person in people:
        old = by_id.get(str(person.get("tmdb_id")), {})
        for field in ("name", "character"):
            if old.get(field) and not _needs_people_translation(old[field]):
                person[field] = old[field]
                if field == "name" and old.get("original_name"):
                    person["original_name"] = old["original_name"]


def _directors_json(details: Mapping[str, Any]) -> list[dict[str, Any]]:
    credits = details.get("aggregate_credits") or details.get("credits")
    crew = credits.get("crew") if isinstance(credits, Mapping) else []
    if not isinstance(crew, list):
        return []
    return list(
        {
            str(entry.get("id") or entry["name"]): {
                "tmdb_id": entry.get("id"),
                "name": entry.get("name"),
                "profile_path": entry.get("profile_path"),
                **({"original_name": entry["original_name"]} if entry.get("original_name") else {}),
            }
            for entry in crew
            if isinstance(entry, Mapping)
            and entry.get("name")
            and (entry.get("job") == "Director" or any(job.get("job") == "Director" for job in entry.get("jobs") or []))
        }.values()
    )


def _keywords(details: Mapping[str, Any]) -> list[Any]:
    value = details.get("keywords") or []
    if isinstance(value, Mapping):
        value = value.get("keywords") or value.get("results") or []
    return value if isinstance(value, list) else []


def _official_ratings(details: Mapping[str, Any]) -> dict[str, str]:
    ratings = {
        str(row["iso_3166_1"]): str(row["rating"])
        for row in (details.get("content_ratings") or {}).get("results", [])
        if row.get("iso_3166_1") and row.get("rating")
    }
    for row in (details.get("release_dates") or {}).get("results", []):
        certification = next(
            (d.get("certification") for d in row.get("release_dates", []) if d.get("certification")), None
        )
        if row.get("iso_3166_1") and certification:
            ratings[str(row["iso_3166_1"])] = str(certification)
    return ratings


def _json_objects(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return []
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _first_json_string(value: Any) -> str:
    values = _json_strings(value)
    return values[0] if values else ""


def _year(value: Any) -> int | None:
    raw = str(value or "")[:4]
    return int(raw) if raw.isdigit() else None


def _integer(value: Any) -> int | None:
    try:
        return None if value is None or isinstance(value, bool) else int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    try:
        return None if value is None or isinstance(value, bool) else float(value)
    except (TypeError, ValueError):
        return None


def _item_changed(item: Mapping[str, Any]) -> bool:
    del item
    return True


def _repository_is_stale(repository: MediaMetadataTaskRepository, item: Mapping[str, Any]) -> bool:
    checker = getattr(repository, "is_stale", None)
    return bool(checker(item)) if callable(checker) else True


def _mark_offline(
    repository: MediaMetadataTaskRepository,
    emby_ids: Sequence[str],
    library_ids: Sequence[str],
) -> int:
    marker = cast(Any, repository.mark_offline)
    if "library_ids" in inspect.signature(marker).parameters:
        return int(marker(emby_ids, library_ids=library_ids))
    # Keep small test/fake repositories source-compatible while the native SQL
    # implementation uses the scoped form above.
    return int(marker(emby_ids))


def _parse_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


__all__ = [
    "DisabledScreenshotCapture",
    "EmptyMediaTranslation",
    "FfmpegScreenshotCapture",
    "HttpxEmbyScreenshotUploadAdapter",
    "HttpxMediaCatalogAdapter",
    "HttpxTmdbMetadataAdapter",
    "MEDIA_TASK_KEYS",
    "MediaCatalogPort",
    "MediaMetadataProvider",
    "MediaMetadataTaskRepository",
    "MediaTaskProgress",
    "NativeMediaMetadataTaskService",
    "NativeMediaScreenshotBackfillService",
    "ScreenshotCapturePort",
    "ScreenshotRepository",
    "ScreenshotUploadPort",
    "SqlAlchemyMediaMetadataTaskRepository",
    "SqlAlchemyScreenshotRepository",
]
