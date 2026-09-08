from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from etk.platform.tasks import TaskOutcome, TaskOutcomeStatus


class MediaReprocessProgress(Protocol):
    def checkpoint(self) -> None: ...

    def update(self, progress: int, message: str) -> None: ...


class MediaItemLookup(Protocol):
    def item(self, item_id: str) -> Mapping[str, Any] | None: ...


class MediaMetadataReprocess(Protocol):
    def execute(
        self,
        task_key: str,
        progress: MediaReprocessProgress,
        parameters: Mapping[str, Any],
    ) -> TaskOutcome: ...


class MediaInfoReprocess(Protocol):
    def execute(
        self,
        progress: MediaReprocessProgress,
        *,
        operation_key: str,
        force_full_update: bool = False,
        emby_item_ids: Sequence[str] | None = None,
    ) -> TaskOutcome: ...


class NativeMediaItemReprocessService:
    """Reprocess one Emby item through the native MediaInfo and metadata owners."""

    def __init__(
        self,
        catalog: MediaItemLookup,
        metadata: MediaMetadataReprocess,
        mediainfo: MediaInfoReprocess,
    ) -> None:
        self._catalog = catalog
        self._metadata = metadata
        self._mediainfo = mediainfo

    def execute(
        self,
        item_id: str,
        progress: MediaReprocessProgress,
        *,
        operation_key: str,
        media_info_only: bool = False,
        restore_media_info: bool = True,
    ) -> TaskOutcome:
        normalized_id = str(item_id or "").strip()
        if not normalized_id:
            raise ValueError("media reprocess item_id must not be empty")
        progress.checkpoint()
        item = self._catalog.item(normalized_id)
        if item is None:
            return TaskOutcome.failed("Emby 中未找到待重新处理的媒体项。", {"item_id": normalized_id})

        mediainfo = (
            self._mediainfo.execute(
                progress,
                operation_key=f"{operation_key}:mediainfo",
                force_full_update=True,
                emby_item_ids=(normalized_id,),
            )
            if restore_media_info
            else TaskOutcome.skipped("媒体信息已由前置持久效果注入。", {"item_id": normalized_id})
        )
        if media_info_only:
            return _with_details(mediainfo, normalized_id, metadata=None)

        root = item
        item_type = str(item.get("Type") or "").strip()
        if item_type in {"Episode", "Season"}:
            series_id = str(item.get("SeriesId") or "").strip()
            if not series_id:
                return TaskOutcome.failed(
                    "待重新处理的剧集项目缺少 SeriesId。",
                    {"item_id": normalized_id, "mediainfo_status": mediainfo.status.value},
                )
            progress.checkpoint()
            root = self._catalog.item(series_id) or {}

        root_type = str(root.get("Type") or "").strip()
        providers = root.get("ProviderIds")
        tmdb_id = (
            str(providers.get("Tmdb") or providers.get("tmdb") or "").strip() if isinstance(providers, Mapping) else ""
        )
        media_type = "movie" if root_type == "Movie" else "tv" if root_type == "Series" else ""
        if not tmdb_id or not media_type:
            metadata = TaskOutcome.failed(
                "待重新处理的媒体项缺少 TMDb 根身份。",
                {"item_id": normalized_id, "root_type": root_type},
            )
        else:
            metadata = self._metadata.execute(
                "backfill-media-metadata",
                progress,
                {"targets": [{"tmdb_id": tmdb_id, "media_type": media_type}]},
            )
        return _combine(normalized_id, mediainfo, metadata)


class NativeMediaItemReprocessOperation:
    def __init__(self, service: NativeMediaItemReprocessService) -> None:
        self._service = service

    def execute(self, parameters: Mapping[str, Any], progress: MediaReprocessProgress) -> Mapping[str, Any]:
        allowed = {"item_id", "media_info_only", "restore_media_info"}
        extras = set(parameters) - allowed
        if extras:
            raise ValueError(f"media reprocess operation contains unsupported fields: {sorted(extras)!r}")
        item_id = str(parameters.get("item_id") or "").strip()
        for field in ("media_info_only", "restore_media_info"):
            if field in parameters and not isinstance(parameters[field], bool):
                raise TypeError(f"media reprocess operation field {field!r} must be boolean")
        outcome = self._service.execute(
            item_id,
            progress,
            operation_key=f"media.reprocess-item:{getattr(progress, 'job_id', item_id)}",
            media_info_only=bool(parameters.get("media_info_only", False)),
            restore_media_info=bool(parameters.get("restore_media_info", True)),
        )
        return {
            "status": outcome.status.value,
            "message": outcome.message,
            "details": dict(outcome.details),
        }


def _combine(item_id: str, mediainfo: TaskOutcome, metadata: TaskOutcome) -> TaskOutcome:
    details = {
        "item_id": item_id,
        "mediainfo_status": mediainfo.status.value,
        "metadata_status": metadata.status.value,
        "mediainfo": dict(mediainfo.details),
        "metadata": dict(metadata.details),
    }
    failures = sum(outcome.status is TaskOutcomeStatus.FAILED for outcome in (mediainfo, metadata))
    if failures == 2:
        return TaskOutcome.failed("媒体信息与元数据重新处理均失败。", details)
    if failures:
        return TaskOutcome.partial("媒体项重新处理部分完成。", details)
    if all(outcome.status is TaskOutcomeStatus.SKIPPED for outcome in (mediainfo, metadata)):
        return TaskOutcome.skipped("媒体项无需重新处理。", details)
    return TaskOutcome.succeeded("媒体项重新处理完成。", details)


def _with_details(outcome: TaskOutcome, item_id: str, *, metadata: TaskOutcome | None) -> TaskOutcome:
    details = {
        "item_id": item_id,
        "mediainfo_status": outcome.status.value,
        "metadata_status": metadata.status.value if metadata is not None else "not_requested",
        "mediainfo": dict(outcome.details),
    }
    return TaskOutcome(outcome.status, outcome.message, details)


__all__ = ["NativeMediaItemReprocessOperation", "NativeMediaItemReprocessService"]
