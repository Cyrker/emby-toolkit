from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol

from etk.domains.operations.jobs import TaskBusyError
from etk.platform import Settings

from .event_dispatch import EmbyEventDispatcher, FutureDomainEventGateway, P5MediaEventGateway
from .models import (
    ImagePolicyKey,
    MediaIdentity,
    MediaItemType,
    ServiceResult,
)
from .policies import (
    MediaPresentationPolicy,
    format_metadata_payload,
    normalize_image_rules,
    select_policy_candidates,
    stored_policy_images,
)
from .repository import MediaRepository

logger = logging.getLogger(__name__)
EMBY_EVENT_REPLAY_TTL_SECONDS = 5


class ImagePolicyError(RuntimeError):
    pass


class ImageCollectionNotFound(ImagePolicyError):
    pass


class ImagePolicyUnavailable(ImagePolicyError):
    pass


class MediaMappingDefaults(Protocol):
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]: ...

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]: ...


class MediaJobSubmitter(Protocol):
    def submit_task(
        self,
        task_key: str,
        parameters: Mapping[str, Any] | None = None,
        **options: Any,
    ) -> Any: ...

    def current(self) -> Any: ...


class MediaImageGateway(Protocol):
    def screenshot_token(self, content_hash: str) -> str: ...

    def archive_metadata(
        self,
        payload: Mapping[str, Any],
        base_url: str,
    ) -> Mapping[str, Any]: ...

    def live_candidates(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_all_languages: bool,
    ) -> list[dict[str, Any]]: ...

    def archive_policy_images(
        self,
        images: Sequence[Mapping[str, Any]],
        base_url: str,
    ) -> list[dict[str, Any]]: ...

    def discard_unreferenced_sources(self, sources: Sequence[str]) -> None: ...

    def refresh_images(
        self,
        requested_type: MediaItemType,
        identity: MediaIdentity,
    ) -> int: ...

    def schedule_missing_collection_cleanup(self, tmdb_id: str) -> None: ...


class EmptyMediaMappingDefaults:
    def keyword_mapping(self) -> Sequence[Mapping[str, Any]]:
        return ()

    def studio_mapping(self) -> Sequence[Mapping[str, Any]]:
        return ()


class MediaService:
    """P5 media boundary with explicit routing for current and future Bridge events."""

    def __init__(
        self,
        *,
        repository: MediaRepository,
        settings: Callable[[], Settings],
        jobs: MediaJobSubmitter,
        images: MediaImageGateway,
        p5_events: P5MediaEventGateway,
        future_events: FutureDomainEventGateway,
        defaults: MediaMappingDefaults | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._jobs = jobs
        self._images = images
        self._events = EmbyEventDispatcher(
            p5_media=p5_events,
            future_domain=future_events,
        )
        self._defaults = defaults or EmptyMediaMappingDefaults()

    def resolve_media_identity(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
    ) -> MediaIdentity | None:
        return self._repository.resolve_by_path(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )

    def provide_bridge_metadata(
        self,
        path: str,
        requested_type: MediaItemType,
        *,
        season_number: int | None = None,
        episode_number: int | None = None,
        base_url: str,
    ) -> ServiceResult:
        identity = self.resolve_media_identity(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ServiceResult({"error": "metadata path identity not found"}, 404)
        aggregate = self._repository.load_metadata(identity, requested_type)
        if aggregate is None:
            return ServiceResult({"error": "metadata cache not found"}, 404)
        payload = format_metadata_payload(
            aggregate,
            self._presentation_policy(requested_type),
            screenshot_token=self._images.screenshot_token,
        )
        payload = dict(self._images.archive_metadata(payload, base_url))
        return ServiceResult(payload, headers={"Cache-Control": "no-store"})

    def backfill_all(self) -> ServiceResult:
        try:
            self._jobs.submit_task(
                "backfill-media-metadata",
                trigger_type="system",
                silent=True,
                reject_when_busy=True,
            )
        except TaskBusyError:
            current = self._jobs.current()
            display_name = str(current.job.request_payload.get("display_name") or "") if current is not None else ""
            if display_name == "补齐媒体元数据":
                return ServiceResult({"ok": True, "submitted": False, "reason": "task_already_running"})
            return ServiceResult({"ok": False, "submitted": False, "reason": "etk_busy"}, 409)
        return ServiceResult({"ok": True, "submitted": True}, 202)

    def backfill_items(self, item_ids: Sequence[str]) -> ServiceResult:
        normalized = tuple(dict.fromkeys(str(value).strip() for value in item_ids if str(value).strip()))
        if not normalized:
            return ServiceResult({"ok": False, "error": "emby_item_ids is required"}, 400)
        return self._submit_backfill_targets(self._repository.find_backfill_targets_by_emby_ids(normalized))

    def backfill_library(self, library_id: str, refresh_mode: str) -> ServiceResult:
        if not str(library_id or "").strip().isdigit():
            return ServiceResult({"ok": False, "error": "emby_library_id is required"}, 400)
        if str(refresh_mode or "").strip().lower() != "missing_metadata":
            return ServiceResult({"ok": False, "error": "unsupported refresh_mode"}, 400)
        return self._submit_backfill_targets(self._repository.find_backfill_targets_by_library(str(library_id).strip()))

    def search_bridge_images(
        self,
        requested_type: MediaItemType,
        *,
        path: str,
        tmdb_id: str,
        season_number: int | None,
        episode_number: int | None,
        include_all_languages: bool,
    ) -> ServiceResult:
        identity = self._image_identity(
            requested_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ServiceResult({"error": "metadata path identity not found"}, 404)
        invalid_hierarchy = self._validate_image_hierarchy(requested_type, identity)
        if invalid_hierarchy is not None:
            return invalid_hierarchy
        try:
            candidates = self._images.live_candidates(
                requested_type,
                identity,
                include_all_languages=include_all_languages,
            )
        except ImagePolicyUnavailable as exc:
            return ServiceResult({"error": str(exc)}, 503)
        except ImagePolicyError as exc:
            return ServiceResult({"error": str(exc)}, 502)
        return ServiceResult(
            {"images": candidates},
            headers={"Cache-Control": "no-store"},
        )

    def sync_image_policy(
        self,
        requested_type: MediaItemType,
        *,
        path: str,
        tmdb_id: str,
        season_number: int | None,
        episode_number: int | None,
        rules: Sequence[Mapping[str, Any]],
        force: bool,
        base_url: str,
    ) -> ServiceResult:
        identity = self._image_identity(
            requested_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ServiceResult({"error": "metadata path identity not found"}, 404)
        invalid_hierarchy = self._validate_image_hierarchy(requested_type, identity)
        if invalid_hierarchy is not None:
            return invalid_hierarchy
        key = ImagePolicyKey.create(
            requested_type,
            identity.tmdb_id,
            season_number=identity.season_number,
            episode_number=identity.episode_number,
        )
        normalized_rules = normalize_image_rules(rules)
        existing = self._repository.get_image_policy(key)
        previous_images = tuple(existing.images if existing is not None else ())
        if existing is not None and [dict(value) for value in existing.rules] == normalized_rules and not force:
            archived = self._images.archive_policy_images(previous_images, base_url)
            stored = stored_policy_images(archived)
            if stored != [dict(value) for value in previous_images]:
                self._repository.replace_image_policy(key, normalized_rules, stored)
            return ServiceResult(
                {"ok": True, "images": archived},
                headers={"Cache-Control": "no-store"},
            )

        try:
            candidates = (
                []
                if not normalized_rules
                else self._images.live_candidates(
                    requested_type,
                    identity,
                    include_all_languages=False,
                )
            )
        except ImageCollectionNotFound:
            self._images.schedule_missing_collection_cleanup(identity.tmdb_id)
            return ServiceResult({"error": "tmdb collection no longer exists"}, 410)
        except ImagePolicyUnavailable as exc:
            return ServiceResult({"error": str(exc)}, 502)
        except ImagePolicyError as exc:
            return ServiceResult({"error": str(exc)}, 502)
        selected = select_policy_candidates(
            candidates,
            normalized_rules,
            previous_images=previous_images,
            preserve_manual=not force,
        )
        archived = self._images.archive_policy_images(selected, base_url)
        stored = stored_policy_images(archived)
        previous = self._repository.replace_image_policy(
            key,
            normalized_rules,
            stored,
        )
        current_sources = {str(value.get("source_url") or "") for value in stored if value.get("source_url")}
        removed_sources = [
            str(value.get("source_url") or "")
            for value in previous
            if value.get("source_url") and value.get("source_url") not in current_sources
        ]
        self._images.discard_unreferenced_sources(
            [source for source in removed_sources if not self._repository.source_is_referenced(source)]
        )
        return ServiceResult(
            {"ok": True, "images": archived},
            headers={"Cache-Control": "no-store"},
        )

    def refresh_bridge_images(
        self,
        requested_type: MediaItemType,
        *,
        path: str,
        tmdb_id: str,
        season_number: int | None,
        episode_number: int | None,
    ) -> ServiceResult:
        identity = self._image_identity(
            requested_type,
            path=path,
            tmdb_id=tmdb_id,
            season_number=season_number,
            episode_number=episode_number,
        )
        if identity is None:
            return ServiceResult({"ok": False, "error": "metadata path identity not found"}, 404)
        invalid_hierarchy = self._validate_image_hierarchy(requested_type, identity, include_ok=True)
        if invalid_hierarchy is not None:
            return invalid_hierarchy
        try:
            updated = self._images.refresh_images(requested_type, identity)
        except ImagePolicyUnavailable as exc:
            return ServiceResult({"ok": False, "error": str(exc)}, 503)
        except ImagePolicyError as exc:
            return ServiceResult({"ok": False, "error": str(exc)}, 502)
        except Exception as exc:
            return ServiceResult({"ok": False, "error": str(exc)}, 500)
        return ServiceResult({"ok": True, "updated": updated})

    def handle_emby_event(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        if payload.get("_etk_source") != "ETKMediaInfoBridge" or not payload.get("Event"):
            return ServiceResult({"status": "invalid_plugin_event"}, 400)
        event_key = _emby_event_key(payload)
        token = self._repository.reserve_emby_event(event_key, EMBY_EVENT_REPLAY_TTL_SECONDS)
        if token is None:
            return ServiceResult({"status": "duplicate_event_ignored"})
        try:
            result = self._events.handle(
                payload,
                host_url=host_url,
                remote_addr=remote_addr,
            )
        except BaseException:
            self._release_emby_event(event_key, token)
            raise
        if result.status_code >= 500:
            self._release_emby_event(event_key, token)
        return result

    def bind_emby_item(
        self,
        *,
        item_id: str,
        item_type: MediaItemType,
        item_path: str,
        sha1: str,
        pick_code: str = "",
        tmdb_id: str = "",
        series_id: str = "",
        season_id: str = "",
    ) -> Mapping[str, Any]:
        return self._repository.bind_emby_item(
            item_id=item_id,
            item_type=item_type,
            item_path=item_path,
            sha1=sha1,
            pick_code=pick_code,
            tmdb_id=tmdb_id,
            series_id=series_id,
            season_id=season_id,
        )

    def _submit_backfill_targets(
        self,
        targets: Sequence[MediaIdentity],
    ) -> ServiceResult:
        if not targets:
            return ServiceResult({"ok": True, "submitted": False, "reason": "metadata_complete"})
        parameters = {"targets": [{"tmdb_id": target.tmdb_id, "media_type": target.media_type} for target in targets]}
        try:
            self._jobs.submit_task(
                "backfill-media-metadata",
                parameters,
                trigger_type="system",
                silent=True,
                reject_when_busy=True,
            )
        except TaskBusyError:
            return ServiceResult({"ok": False, "submitted": False, "reason": "etk_busy"}, 409)
        return ServiceResult(
            {"ok": True, "submitted": True, "count": len(targets)},
            202,
        )

    def _image_identity(
        self,
        requested_type: MediaItemType,
        *,
        path: str,
        tmdb_id: str,
        season_number: int | None,
        episode_number: int | None,
    ) -> MediaIdentity | None:
        if requested_type is MediaItemType.BOXSET:
            normalized_id = str(tmdb_id or "").strip()
            if not normalized_id.isdigit():
                return None
            return MediaIdentity(
                normalized_id,
                "movie",
                MediaItemType.BOXSET,
            )
        return self.resolve_media_identity(
            path,
            requested_type,
            season_number=season_number,
            episode_number=episode_number,
        )

    def _presentation_policy(
        self,
        requested_type: MediaItemType,
    ) -> MediaPresentationPolicy:
        settings = self._settings()
        keyword_mapping = self._repository.load_setting("keyword_mapping")
        studio_mapping = self._repository.load_setting("studio_mapping")
        return MediaPresentationPolicy(
            keyword_to_tags=bool(settings.keyword_to_tags),
            studio_to_chinese=bool(settings.studio_to_chinese),
            actor_role_add_prefix=bool(settings.actor_role_add_prefix),
            max_actors=int(settings.max_actors_to_process),
            max_episode_actors=(
                int(settings.max_episode_actors_to_process) if requested_type is MediaItemType.EPISODE else 0
            ),
            keyword_mapping=tuple(
                dict(value)
                for value in (
                    keyword_mapping if isinstance(keyword_mapping, list) else self._defaults.keyword_mapping()
                )
                if isinstance(value, Mapping)
            ),
            studio_mapping=tuple(
                dict(value)
                for value in (studio_mapping if isinstance(studio_mapping, list) else self._defaults.studio_mapping())
                if isinstance(value, Mapping)
            ),
        )

    @staticmethod
    def _validate_image_hierarchy(
        requested_type: MediaItemType,
        identity: MediaIdentity,
        *,
        include_ok: bool = False,
    ) -> ServiceResult | None:
        error = ""
        if requested_type is MediaItemType.SEASON and identity.season_number is None:
            error = "season number is required"
        elif requested_type is MediaItemType.EPISODE and (
            identity.season_number is None or identity.episode_number is None
        ):
            error = "episode numbers are required"
        if not error:
            return None
        payload: dict[str, Any] = {"error": error}
        if include_ok:
            payload["ok"] = False
            payload = {"ok": False, "error": error}
        return ServiceResult(payload, 400)

    def _release_emby_event(self, event_key: str, token: str) -> None:
        try:
            self._repository.release_emby_event(event_key, token)
        except Exception:
            logger.exception("failed to release P5 Emby event reservation")


def _emby_event_key(payload: Mapping[str, Any]) -> str:
    explicit = str(
        payload.get("EventId")
        or payload.get("event_id")
        or payload.get("IdempotencyKey")
        or payload.get("idempotency_key")
        or ""
    ).strip()
    source = explicit or json.dumps(
        dict(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


__all__ = [
    "EmptyMediaMappingDefaults",
    "EMBY_EVENT_REPLAY_TTL_SECONDS",
    "ImageCollectionNotFound",
    "ImagePolicyError",
    "ImagePolicyUnavailable",
    "MediaImageGateway",
    "MediaJobSubmitter",
    "MediaMappingDefaults",
    "MediaService",
]
