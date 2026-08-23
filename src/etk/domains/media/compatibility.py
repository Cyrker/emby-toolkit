from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from etk.compatibility.flask_app import LazyCompatibilityApplication
from etk.platform import Settings

from .event_dispatch import EmbyEventOwner, classify_emby_event
from .models import MediaIdentity, MediaItemType, ServiceResult
from .service import ImageCollectionNotFound, ImagePolicyError, ImagePolicyUnavailable


class LegacyMediaMappingDefaults:
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]:
        import utils

        return tuple(dict(value) for value in utils.DEFAULT_KEYWORD_MAPPING)

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]:
        import utils

        return tuple(dict(value) for value in utils.DEFAULT_STUDIO_MAPPING)


class LegacyMediaImageGateway:
    """Temporary P5 boundary around image/network/file behavior retained until P11."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings

    def screenshot_token(self, content_hash: str) -> str:
        from handler.media_image_cache import cache_token

        return cache_token(content_hash)

    def archive_metadata(
        self,
        payload: Mapping[str, Any],
        base_url: str,
    ) -> Mapping[str, Any]:
        from handler.media_image_cache import archive_metadata_images

        return archive_metadata_images(dict(payload), base_url, archive_sources=False)

    def live_candidates(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_all_languages: bool,
    ) -> list[dict[str, Any]]:
        from handler.media_image_policy import (
            ImagePolicyCollectionNotFound,
            ImagePolicySyncError,
            get_live_image_candidates,
        )

        if not self._settings().tmdb_api_key.get_secret_value().strip():
            raise ImagePolicyUnavailable("tmdb api key is not configured")
        try:
            return get_live_image_candidates(
                requested_type.value,
                identity.tmdb_id,
                identity.media_type,
                season_number=identity.season_number,
                episode_number=identity.episode_number,
                include_all_languages=include_all_languages,
            )
        except ImagePolicyCollectionNotFound as exc:
            raise ImageCollectionNotFound(identity.tmdb_id) from exc
        except ImagePolicySyncError as exc:
            raise ImagePolicyError(str(exc)) from exc

    def archive_policy_images(
        self,
        images: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]:
        from handler.media_image_cache import archive_policy_images

        return archive_policy_images([dict(value) for value in images], base_url)

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> None:
        from handler.media_image_cache import discard_unreferenced_policy_sources

        discard_unreferenced_policy_sources(sources)

    def refresh_images(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
    ) -> int:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from database.metadata_provider_db import (
            build_image_language_parameter,
            build_image_language_priority,
            replace_cached_image_paths,
            select_image_path,
        )
        from handler import tmdb

        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        if not api_key:
            raise ImagePolicyUnavailable("tmdb api key is not configured")
        tmdb_id = identity.tmdb_id
        if requested_type is MediaItemType.BOXSET:
            from database import tmdb_collection_db

            details = tmdb.get_collection_details(int(tmdb_id), api_key, skip_fallback=True)
            if not details:
                raise ImagePolicyError("tmdb collection image lookup failed")
            return int(
                tmdb_collection_db.update_native_collection_images(
                    tmdb_id,
                    details.get("poster_path"),
                    details.get("backdrop_path"),
                )
                or 0
            )

        if identity.media_type == "movie":
            base = tmdb.get_movie_details(int(tmdb_id), api_key, append_to_response="")
        else:
            base = tmdb.get_tv_details(
                int(tmdb_id),
                api_key,
                append_to_response="",
                allow_english_fallback=False,
            )
        if not base:
            raise ImagePolicyError("tmdb media identity lookup failed")
        priorities = build_image_language_priority(
            str(base.get("original_language") or ""),
            settings.tmdb_image_language_preference,
        )
        image_languages = build_image_language_parameter(priorities)
        root_images: dict[str, str | None] | None = None
        season_posters: dict[int, str | None] | None = None
        episode_still: str | None = None
        if requested_type in {MediaItemType.MOVIE, MediaItemType.SERIES}:
            if identity.media_type == "movie":
                image_data = tmdb.get_movie_details(
                    int(tmdb_id),
                    api_key,
                    append_to_response="images",
                    include_image_language=image_languages,
                )
            else:
                image_data = tmdb.get_tv_details(
                    int(tmdb_id),
                    api_key,
                    append_to_response="images",
                    include_image_language=image_languages,
                    allow_english_fallback=False,
                )
            if not image_data:
                raise ImagePolicyError("tmdb image lookup failed")
            images = image_data.get("images") or {}
            backdrop = select_image_path(images.get("backdrops"), priorities) or image_data.get("backdrop_path")
            root_images = {
                "poster": select_image_path(images.get("posters"), priorities) or image_data.get("poster_path"),
                "backdrop": backdrop,
                "logo": select_image_path(images.get("logos"), priorities),
                "thumb": backdrop,
            }
            if requested_type is MediaItemType.SERIES:
                seasons = [value for value in image_data.get("seasons") or [] if value.get("season_number") is not None]
                season_posters = {int(value["season_number"]): None for value in seasons}

                def fetch_season(number: int) -> tuple[int, str | None]:
                    details = tmdb.get_season_details_tmdb(
                        int(tmdb_id),
                        number,
                        api_key,
                        append_to_response="images",
                        include_image_language=image_languages,
                    )
                    if not details:
                        return number, None
                    return number, (
                        select_image_path((details.get("images") or {}).get("posters"), priorities)
                        or details.get("poster_path")
                    )

                with ThreadPoolExecutor(max_workers=min(4, max(1, len(seasons)))) as executor:
                    futures = [executor.submit(fetch_season, int(value["season_number"])) for value in seasons]
                    for future in as_completed(futures):
                        number, poster = future.result()
                        season_posters[number] = poster
        elif requested_type is MediaItemType.SEASON:
            if identity.season_number is None:
                raise ValueError("season number is required")
            details = tmdb.get_season_details_tmdb(
                int(tmdb_id),
                identity.season_number,
                api_key,
                append_to_response="images",
                include_image_language=image_languages,
            )
            if not details:
                raise ImagePolicyError("tmdb season image lookup failed")
            season_posters = {
                identity.season_number: (
                    select_image_path((details.get("images") or {}).get("posters"), priorities)
                    or details.get("poster_path")
                )
            }
        else:
            if identity.season_number is None or identity.episode_number is None:
                raise ValueError("episode numbers are required")
            details = tmdb.get_episode_details_tmdb(
                int(tmdb_id),
                identity.season_number,
                identity.episode_number,
                api_key,
                append_to_response="images",
                include_image_language=image_languages,
            )
            if not details:
                raise ImagePolicyError("tmdb episode image lookup failed")
            episode_still = select_image_path((details.get("images") or {}).get("stills"), priorities) or details.get(
                "still_path"
            )
        return int(
            replace_cached_image_paths(
                tmdb_id,
                identity.media_type,
                requested_type.value,
                root_images=root_images,
                season_number=identity.season_number,
                season_posters=season_posters,
                episode_number=identity.episode_number,
                episode_still=episode_still,
            )
            or 0
        )

    def schedule_missing_collection_cleanup(self, tmdb_id: str) -> None:
        from database import tmdb_collection_db
        from routes.tmdb_collections import _schedule_missing_collection_cleanup

        row = tmdb_collection_db.get_native_collection_by_tmdb_id(tmdb_id)
        _schedule_missing_collection_cleanup(tmdb_id, row)


class _LegacyMediaEventGateway:
    """Forward one explicitly owned event family to the isolated Flask host."""

    def __init__(
        self,
        compatibility_app: LazyCompatibilityApplication,
        owner: EmbyEventOwner,
    ) -> None:
        self._compatibility_app = compatibility_app
        self._owner = owner

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        observed_owner = classify_emby_event(payload.get("Event"))
        if observed_owner is not self._owner:
            raise ValueError(
                f"event {payload.get('Event')!r} belongs to {observed_owner.value}, not {self._owner.value}"
            )
        flask_app = self._compatibility_app.materialize()
        host = str(host_url or "http://localhost/").split("//", 1)[-1].rstrip("/")
        with flask_app.test_client() as client:
            response = client.post(
                "/api/emby/events",
                json=dict(payload),
                headers={"Host": host},
                environ_overrides={"REMOTE_ADDR": str(remote_addr or "")},
            )
        body = response.get_json(silent=True)
        if not isinstance(body, Mapping):
            body = {"status": "invalid_legacy_event_response"}
        headers = {
            key: value for key, value in response.headers.items() if key.lower() in {"cache-control", "content-type"}
        }
        return ServiceResult(
            dict(body),
            response.status_code,
            headers=headers,
        )


class LegacyP5MediaEventGateway(_LegacyMediaEventGateway):
    """Compatibility implementation of the P5 metadata/image event port."""

    def __init__(self, compatibility_app: LazyCompatibilityApplication) -> None:
        super().__init__(compatibility_app, EmbyEventOwner.P5_MEDIA)


class LegacyFutureDomainEventGateway(_LegacyMediaEventGateway):
    """Compatibility implementation for the eleven event families owned after P5."""

    def __init__(self, compatibility_app: LazyCompatibilityApplication) -> None:
        super().__init__(compatibility_app, EmbyEventOwner.FUTURE_DOMAIN)


class LegacyMediaRuntimeGateway:
    """Read compatibility processor readiness without importing legacy state in the domain."""

    @staticmethod
    def processor_ready() -> bool:
        import extensions

        return extensions.media_processor_instance is not None


__all__ = [
    "LegacyFutureDomainEventGateway",
    "LegacyMediaImageGateway",
    "LegacyMediaMappingDefaults",
    "LegacyMediaRuntimeGateway",
    "LegacyP5MediaEventGateway",
]
