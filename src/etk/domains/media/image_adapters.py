from __future__ import annotations

import logging
import mimetypes
import re
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from urllib.parse import quote, urlencode, urlsplit

import httpx

from etk.platform import Settings
from etk.policy_defaults import DEFAULT_KEYWORD_MAPPING, DEFAULT_STUDIO_MAPPING

from .image_store import CACHE_TOKEN_PREFIX, MAX_IMAGE_BYTES, SqlAlchemyMediaImageStore
from .models import MediaIdentity, MediaItemType
from .policies import (
    build_image_language_parameter,
    build_image_language_priority,
    preferred_image_candidates,
    select_image_path,
    sort_image_candidates,
)
from .service import ImageCollectionNotFound, ImagePolicyError, ImagePolicyUnavailable

logger = logging.getLogger(__name__)


class _MediaImageRepository(Protocol):
    def original_language(self, identity: MediaIdentity) -> str: ...

    def replace_cached_images(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        root_images: Mapping[str, str | None] | None = None,
        season_posters: Mapping[int, str | None] | None = None,
        episode_still: str | None = None,
    ) -> int: ...

    def record_episode_screenshot(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
        content_hash: str,
    ) -> bool: ...

    def clear_episode_screenshot(
        self,
        series_tmdb_id: str,
        season_number: int,
        episode_number: int,
    ) -> bool: ...


class _EmbyImageReader(Protocol):
    def image(self, item_id: str, image_type: str) -> tuple[bytes, str] | None: ...


class StaticMediaMappingDefaults:
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]:
        return tuple(dict(value) for value in DEFAULT_KEYWORD_MAPPING)

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]:
        return tuple(dict(value) for value in DEFAULT_STUDIO_MAPPING)


class HttpxMediaImageGateway:
    """Native TMDb/image-cache adapter for the P5 metadata and image-policy surface."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        repository: _MediaImageRepository,
        store: SqlAlchemyMediaImageStore,
        emby_images: _EmbyImageReader,
        missing_collection_cleanup: Callable[[str], object],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._store = store
        self._emby_images = emby_images
        self._missing_collection_cleanup = missing_collection_cleanup
        self._client_factory = client_factory or self._default_client

    def screenshot_token(self, content_hash: str) -> str:
        return self._store.cache_token(content_hash)

    def archive_metadata(self, payload: Mapping[str, Any], base_url: str) -> Mapping[str, Any]:
        result = dict(payload)
        screenshot_hash = str(result.pop("_screenshot_hash", "") or "").strip().lower()
        raw_emby_ids = result.pop("_emby_item_ids", ())
        emby_item_ids = (
            tuple(str(value).strip() for value in raw_emby_ids if str(value).strip())
            if isinstance(raw_emby_ids, Sequence) and not isinstance(raw_emby_ids, str | bytes | bytearray)
            else ()
        )
        raw_images = result.get("images")
        if not isinstance(raw_images, Mapping):
            return result
        images = dict(raw_images)
        result["images"] = images
        original_primary = images.get("primary")
        for key in ("primary", "backdrop", "logo", "thumb"):
            value = str(images.get(key) or "").strip()
            content_hash = _cache_hash(value)
            if content_hash:
                images[key] = _cached_url(content_hash, base_url)

        if (
            self._settings().media_image_archive_enabled
            and result.get("item_type") == MediaItemType.EPISODE.value
            and not screenshot_hash
            and not original_primary
        ):
            archived = self._archive_existing_episode_image(result, emby_item_ids)
            if archived:
                screenshot_url = _cached_url(archived, base_url)
                images["primary"] = screenshot_url
                images["backdrop"] = images.get("backdrop") or screenshot_url
                images["thumb"] = images.get("thumb") or screenshot_url

        if (
            result.get("item_type") == MediaItemType.EPISODE.value
            and screenshot_hash
            and original_primary
            and not str(original_primary).startswith(CACHE_TOKEN_PREFIX)
        ):
            self._discard_episode_screenshot(result)
        return result

    def live_candidates(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_all_languages: bool,
    ) -> list[dict[str, Any]]:
        self._require_tmdb_key()
        if requested_type is MediaItemType.BOXSET:
            details = self._tmdb_json(f"/collection/{quote(identity.tmdb_id, safe='')}", {"language": "zh-CN"})
            if details is None:
                raise ImageCollectionNotFound(identity.tmdb_id)
            original_language = next(
                (
                    str(part.get("original_language") or "").strip()
                    for part in details.get("parts") or ()
                    if isinstance(part, Mapping) and part.get("original_language")
                ),
                "",
            )
        else:
            original_language = self._repository.original_language(identity)
        raw = self._tmdb_json(self._images_endpoint(requested_type, identity), {}, quiet_not_found=True)
        if raw is None:
            if requested_type is MediaItemType.EPISODE:
                raw = {"stills": []}
            elif requested_type is MediaItemType.BOXSET:
                raise ImageCollectionNotFound(identity.tmdb_id)
            else:
                raise ImagePolicyError("tmdb image lookup failed")
        priorities = build_image_language_priority(
            original_language,
            self._settings().tmdb_image_language_preference,
        )
        candidates: list[dict[str, Any]] = []

        def append(values: object, image_type: str) -> None:
            if not isinstance(values, Sequence) or isinstance(values, str | bytes | bytearray):
                return
            typed = [dict(value) for value in values if isinstance(value, Mapping)]
            selected = (
                sort_image_candidates(typed, priorities)
                if include_all_languages
                else preferred_image_candidates(
                    typed,
                    priorities,
                )
            )
            for item in selected:
                path = str(item.get("file_path") or "").strip()
                if not path:
                    continue
                source_url = f"https://image.tmdb.org/t/p/original/{path.lstrip('/')}"
                thumbnail_url = f"https://image.tmdb.org/t/p/w500/{path.lstrip('/')}"
                candidates.append(
                    {
                        "type": image_type,
                        "source_url": source_url,
                        "thumbnail_source_url": thumbnail_url,
                        "url": source_url,
                        "thumbnail_url": thumbnail_url,
                        "language": item.get("iso_639_1") if include_all_languages else None,
                        "width": item.get("width"),
                        "height": item.get("height"),
                        "community_rating": item.get("vote_average"),
                        "vote_count": item.get("vote_count"),
                    }
                )

        if requested_type in {MediaItemType.MOVIE, MediaItemType.SERIES, MediaItemType.BOXSET}:
            for image_type in ("Primary", "Disc"):
                append(raw.get("posters"), image_type)
            for image_type in ("Backdrop", "Art", "Banner", "Thumb"):
                append(raw.get("backdrops"), image_type)
            if requested_type is not MediaItemType.BOXSET:
                append(raw.get("logos"), "Logo")
        elif requested_type is MediaItemType.SEASON:
            for image_type in ("Primary", "Disc"):
                append(raw.get("posters"), image_type)
        else:
            for image_type in ("Primary", "Art", "Backdrop", "Banner", "Thumb", "Disc"):
                append(raw.get("stills"), image_type)
        return candidates

    def archive_policy_images(
        self,
        images: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]:
        values = [dict(item) for item in images]
        cached_by_source: dict[str, Mapping[str, Any] | None] = {}
        if self._settings().media_image_archive_enabled:
            for source in dict.fromkeys(str(item.get("source_url") or "").strip() for item in values):
                if not source or source.startswith(CACHE_TOKEN_PREFIX):
                    continue
                try:
                    cached_by_source[source] = self._archive_source(source)
                except (httpx.HTTPError, OSError, TypeError, ValueError) as exc:
                    logger.warning("P5 image-policy source archive failed: %s", exc)
                    cached_by_source[source] = None

        for item in values:
            source = str(item.get("source_url") or "").strip()
            token_hash = _cache_hash(source)
            if token_hash:
                item["content_hash"] = token_hash
            cached = cached_by_source.get(source)
            if cached is not None:
                item["content_hash"] = str(cached.get("content_hash") or "")
            content_hash = str(item.get("content_hash") or "").strip().lower()
            if content_hash and self._store.get(content_hash) is not None:
                item["url"] = _cached_url(content_hash, base_url)
                item["thumbnail_url"] = item["url"]
            else:
                item.pop("content_hash", None)
                item["url"] = source
                item["thumbnail_url"] = item.get("thumbnail_source_url") or source
        return values

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> None:
        self._store.discard_unreferenced_sources(sources)

    def refresh_images(self, requested_type: MediaItemType, identity: MediaIdentity) -> int:
        self._require_tmdb_key()
        if requested_type is MediaItemType.BOXSET:
            details = self._tmdb_json(f"/collection/{quote(identity.tmdb_id, safe='')}", {"language": "zh-CN"})
            if details is None:
                raise ImagePolicyError("tmdb collection image lookup failed")
            raw = self._tmdb_json(self._images_endpoint(requested_type, identity), {})
            if raw is None:
                raise ImagePolicyError("tmdb collection image lookup failed")
            original_language = next(
                (
                    str(part.get("original_language") or "").strip()
                    for part in details.get("parts") or ()
                    if isinstance(part, Mapping) and part.get("original_language")
                ),
                "",
            )
            priorities = build_image_language_priority(
                original_language,
                self._settings().tmdb_image_language_preference,
            )
            return self._repository.replace_cached_images(
                requested_type,
                identity,
                root_images={
                    "poster": select_image_path(_mapping_sequence(raw.get("posters")), priorities)
                    or _optional_text(details.get("poster_path")),
                    "backdrop": select_image_path(_mapping_sequence(raw.get("backdrops")), priorities)
                    or _optional_text(details.get("backdrop_path")),
                },
            )

        original_language = self._repository.original_language(identity)
        priorities = build_image_language_priority(
            original_language,
            self._settings().tmdb_image_language_preference,
        )
        image_languages = build_image_language_parameter(priorities)
        params = {
            "language": "zh-CN",
            "append_to_response": "images",
            "include_image_language": image_languages,
        }
        if requested_type in {MediaItemType.MOVIE, MediaItemType.SERIES}:
            endpoint = (
                f"/{'movie' if requested_type is MediaItemType.MOVIE else 'tv'}/{quote(identity.tmdb_id, safe='')}"
            )
            details = self._tmdb_json(endpoint, params)
            if details is None:
                raise ImagePolicyError("tmdb media image lookup failed")
            raw_images = _mapping(details.get("images"))
            backdrop = select_image_path(_mapping_sequence(raw_images.get("backdrops")), priorities) or _optional_text(
                details.get("backdrop_path")
            )
            root_images = {
                "poster": select_image_path(_mapping_sequence(raw_images.get("posters")), priorities)
                or _optional_text(details.get("poster_path")),
                "backdrop": backdrop,
                "logo": select_image_path(_mapping_sequence(raw_images.get("logos")), priorities),
                "thumb": backdrop,
            }
            season_posters: dict[int, str | None] | None = None
            if requested_type is MediaItemType.SERIES:
                season_posters = {}
                for season in details.get("seasons") or ():
                    if not isinstance(season, Mapping) or _optional_int(season.get("season_number")) is None:
                        continue
                    number = int(season["season_number"])
                    season_details = self._tmdb_json(
                        f"/tv/{quote(identity.tmdb_id, safe='')}/season/{number}",
                        params,
                    )
                    if season_details is None:
                        continue
                    season_images = _mapping(season_details.get("images"))
                    season_posters[number] = select_image_path(
                        _mapping_sequence(season_images.get("posters")),
                        priorities,
                    ) or _optional_text(season_details.get("poster_path"))
            return self._repository.replace_cached_images(
                requested_type,
                identity,
                root_images=root_images,
                season_posters=season_posters,
            )

        if identity.season_number is None:
            raise ValueError("season number is required")
        if requested_type is MediaItemType.SEASON:
            details = self._tmdb_json(
                f"/tv/{quote(identity.tmdb_id, safe='')}/season/{identity.season_number}",
                params,
            )
            if details is None:
                raise ImagePolicyError("tmdb season image lookup failed")
            raw_images = _mapping(details.get("images"))
            poster = select_image_path(_mapping_sequence(raw_images.get("posters")), priorities) or _optional_text(
                details.get("poster_path")
            )
            return self._repository.replace_cached_images(
                requested_type,
                identity,
                season_posters={identity.season_number: poster},
            )

        if identity.episode_number is None:
            raise ValueError("episode number is required")
        details = self._tmdb_json(
            (
                f"/tv/{quote(identity.tmdb_id, safe='')}/season/{identity.season_number}"
                f"/episode/{identity.episode_number}"
            ),
            params,
        )
        if details is None:
            raise ImagePolicyError("tmdb episode image lookup failed")
        raw_images = _mapping(details.get("images"))
        still = select_image_path(_mapping_sequence(raw_images.get("stills")), priorities) or _optional_text(
            details.get("still_path")
        )
        return self._repository.replace_cached_images(
            requested_type,
            identity,
            episode_still=still,
        )

    def schedule_missing_collection_cleanup(self, tmdb_id: str) -> None:
        self._missing_collection_cleanup(str(tmdb_id or "").strip())

    def _archive_existing_episode_image(
        self,
        payload: Mapping[str, Any],
        emby_item_ids: Sequence[str],
    ) -> str | None:
        series_tmdb_id = str(payload.get("series_tmdb_id") or "").strip()
        season_number = _optional_int(payload.get("season_number"))
        episode_number = _optional_int(payload.get("episode_number"))
        if not series_tmdb_id or season_number is None or episode_number is None:
            return None
        source = _episode_screenshot_source(series_tmdb_id, season_number, episode_number)
        for item_id in emby_item_ids:
            image = self._emby_images.image(item_id, "Primary")
            if image is None:
                continue
            content, mime_type = image
            try:
                cached = self._store.store(source, content, mime_type)
            except (OSError, ValueError):
                continue
            content_hash = str(cached["content_hash"])
            if self._repository.record_episode_screenshot(
                series_tmdb_id,
                season_number,
                episode_number,
                content_hash,
            ):
                return content_hash
            self._store.discard_unreferenced_sources((source,))
        return None

    def _discard_episode_screenshot(self, payload: Mapping[str, Any]) -> None:
        series_tmdb_id = str(payload.get("series_tmdb_id") or "").strip()
        season_number = _optional_int(payload.get("season_number"))
        episode_number = _optional_int(payload.get("episode_number"))
        if not series_tmdb_id or season_number is None or episode_number is None:
            return
        if self._repository.clear_episode_screenshot(series_tmdb_id, season_number, episode_number):
            self._store.discard_unreferenced_sources(
                (_episode_screenshot_source(series_tmdb_id, season_number, episode_number),)
            )

    def _archive_source(self, source_url: str) -> Mapping[str, Any] | None:
        cached = self._store.get_source(source_url)
        if cached is not None:
            return cached
        parsed = urlsplit(source_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            return None
        content = bytearray()
        with self._client_factory() as client:
            with client.stream(
                "GET",
                source_url,
                headers={"Accept": "image/*", "User-Agent": self._settings().user_agent or "ETK/media-image"},
            ) as response:
                response.raise_for_status()
                length = response.headers.get("Content-Length")
                if length and int(length) > MAX_IMAGE_BYTES:
                    raise ValueError("图片超过 32MB 限制")
                mime_type = str(response.headers.get("Content-Type") or "").partition(";")[0].strip().lower()
                if not mime_type.startswith("image/"):
                    mime_type = str(mimetypes.guess_type(parsed.path)[0] or "").lower()
                if not mime_type.startswith("image/"):
                    raise ValueError("远端响应不是图片")
                for chunk in response.iter_bytes(128 * 1024):
                    content.extend(chunk)
                    if len(content) > MAX_IMAGE_BYTES:
                        raise ValueError("图片超过 32MB 限制")
        if not content:
            raise ValueError("图片内容为空")
        return self._store.store(source_url, bytes(content), mime_type)

    def _images_endpoint(self, requested_type: MediaItemType, identity: MediaIdentity) -> str:
        tmdb_id = quote(identity.tmdb_id, safe="")
        if requested_type is MediaItemType.MOVIE:
            return f"/movie/{tmdb_id}/images"
        if requested_type is MediaItemType.SERIES:
            return f"/tv/{tmdb_id}/images"
        if requested_type is MediaItemType.SEASON and identity.season_number is not None:
            return f"/tv/{tmdb_id}/season/{identity.season_number}/images"
        if (
            requested_type is MediaItemType.EPISODE
            and identity.season_number is not None
            and identity.episode_number is not None
        ):
            return f"/tv/{tmdb_id}/season/{identity.season_number}/episode/{identity.episode_number}/images"
        if requested_type is MediaItemType.BOXSET:
            return f"/collection/{tmdb_id}/images"
        raise ValueError("media image hierarchy is incomplete")

    def _tmdb_json(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        *,
        quiet_not_found: bool = False,
    ) -> dict[str, Any] | None:
        settings = self._settings()
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{settings.tmdb_api_base_url.strip().rstrip('/')}{endpoint}",
                    params={"api_key": self._require_tmdb_key(), **dict(params)},
                    headers={"Accept": "application/json"},
                )
            if response.status_code == 404 and quiet_not_found:
                return None
            if response.status_code == 404:
                return None
            response.raise_for_status()
            payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise ImagePolicyError("tmdb image lookup failed") from exc
        return dict(payload) if isinstance(payload, Mapping) else None

    def _require_tmdb_key(self) -> str:
        value = self._settings().tmdb_api_key.get_secret_value().strip()
        if not value:
            raise ImagePolicyUnavailable("tmdb api key is not configured")
        return value

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(max(30.0, float(settings.emby_api_timeout)), connect=15.0),
            follow_redirects=True,
        )


def _cache_hash(value: str) -> str:
    if not value.startswith(CACHE_TOKEN_PREFIX):
        return ""
    content_hash = value[len(CACHE_TOKEN_PREFIX) :].strip().lower()
    return content_hash if re.fullmatch(r"[a-f0-9]{64}", content_hash) else ""


def _cached_url(content_hash: str, base_url: str) -> str:
    return f"{str(base_url or '').rstrip('/')}/api/image_proxy?{urlencode({'cache': content_hash})}"


def _episode_screenshot_source(series_tmdb_id: str, season_number: int, episode_number: int) -> str:
    return f"episode-screenshot://{series_tmdb_id}/S{season_number:02d}/E{episode_number:02d}"


def _mapping_sequence(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def _optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


__all__ = ["HttpxMediaImageGateway", "StaticMediaMappingDefaults"]
