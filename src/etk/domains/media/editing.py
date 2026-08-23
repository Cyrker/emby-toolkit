from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from .models import MediaInfoEditContext, MediaInfoEpisode
from .repository import (
    MediaAssetAlignmentError,
    MediaIdentityConflictError,
    MediaIdentityNotFoundError,
    MediaInfoCacheNotFoundError,
    MediaInfoFingerprintNotFoundError,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class MediaEditingResult:
    payload: Any
    status_code: int = 200


@dataclass(frozen=True, slots=True)
class ChapterUpdate:
    mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...]
    intro_start_ticks: int | None
    intro_end_ticks: int | None
    credits_start_ticks: int | None
    clear_intro: bool
    clear_credits: bool


class MediaCommandStatus(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"
    PENDING = "pending"


@dataclass(frozen=True, slots=True)
class MediaCommandOutcome:
    status: MediaCommandStatus
    effect_id: str
    provider_result: Mapping[str, Any] | None = None
    error: str = ""
    details: Mapping[str, Any] | None = None


class MediaEditingRepository(Protocol):
    def resolve_media_info_context(self, item_id: str) -> MediaInfoEditContext: ...

    def list_series_media_info_episodes(self, series_id: str) -> tuple[MediaInfoEpisode, ...]: ...


class MediaInfoCommandGateway(Protocol):
    def apply_chapters(
        self,
        context: MediaInfoEditContext,
        update: ChapterUpdate,
    ) -> MediaCommandOutcome: ...

    def apply_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
    ) -> MediaCommandOutcome: ...


class MediaEditingService:
    """P5 read boundary for the MediaInfo editor and chapter projection."""

    def __init__(
        self,
        repository: MediaEditingRepository,
        *,
        processor_ready: Callable[[], bool],
        commands: MediaInfoCommandGateway | None = None,
    ) -> None:
        self._repository = repository
        self._processor_ready = processor_ready
        self._commands = commands

    def media_info(self, item_id: str) -> MediaEditingResult:
        context, error = self._context(item_id)
        if error is not None:
            return error
        assert context is not None
        return MediaEditingResult(
            {
                "sha1": context.sha1,
                "mediainfo": _json_mediainfo(context),
            }
        )

    def chapters(self, item_id: str) -> MediaEditingResult:
        context, error = self._context(item_id)
        if error is not None:
            return error
        assert context is not None
        return MediaEditingResult(
            {
                "sha1": context.sha1,
                **extract_marker_seconds(context.mediainfo),
            }
        )

    def series_episodes(self, series_id: str) -> MediaEditingResult:
        unavailable = self._unavailable()
        if unavailable is not None:
            return unavailable
        try:
            episodes = self._repository.list_series_media_info_episodes(series_id)
        except MediaIdentityNotFoundError:
            return MediaEditingResult({"error": "未在数据库中找到该剧集信息"}, 404)
        except MediaIdentityConflictError:
            return MediaEditingResult({"error": "剧集 ItemID 存在身份冲突"}, 409)
        except Exception:
            logger.exception("failed to list P5 MediaInfo episodes")
            return MediaEditingResult({"error": "服务器内部错误"}, 500)
        return MediaEditingResult(
            [
                {
                    "emby_id": episode.emby_id,
                    "season_number": episode.season_number,
                    "episode_number": episode.episode_number,
                    "title": episode.title,
                }
                for episode in episodes
            ]
        )

    def update_chapters(self, item_id: str, payload: Mapping[str, Any]) -> MediaEditingResult:
        context, error = self._context(item_id)
        if error is not None:
            return error
        assert context is not None
        try:
            update = prepare_chapter_update(context, payload)
        except ValueError as exc:
            status_code = 409 if str(exc) == "当前媒体信息已变化，请重新打开后再保存" else 400
            return MediaEditingResult({"error": str(exc)}, status_code)
        if self._commands is None:
            return MediaEditingResult({"error": "媒体副作用执行器未就绪"}, 503)
        try:
            outcome = self._commands.apply_chapters(context, update)
        except (MediaAssetAlignmentError, MediaIdentityConflictError):
            return MediaEditingResult({"error": "当前媒体信息已变化，请重新打开后再保存"}, 409)
        except Exception:
            logger.exception("failed to stage P5 chapter update")
            return MediaEditingResult({"error": "保存失败"}, 500)
        if outcome.status is not MediaCommandStatus.CONFIRMED:
            return _effect_failure(outcome, "缓存已保存，但写入 Emby 失败，请确认桥接插件已更新")
        details = dict(outcome.details or {})
        return MediaEditingResult(
            {
                "message": "片头片尾已更新并写入 Emby",
                "sha1": context.sha1,
                "result": dict(outcome.provider_result or {}),
                "failure_reset_count": int(details.get("failure_reset_count") or 0),
                **extract_marker_seconds(update.mediainfo),
            }
        )

    def update_media_info(self, item_id: str, payload: Mapping[str, Any]) -> MediaEditingResult:
        context, error = self._context(item_id)
        if error is not None:
            return error
        assert context is not None
        expected_sha1 = str(payload.get("sha1") or "").strip().upper()
        if not expected_sha1 or payload.get("mediainfo") is None:
            return MediaEditingResult({"error": "参数不完整"}, 400)
        if expected_sha1 != context.sha1:
            return MediaEditingResult({"error": "当前媒体信息已变化，请重新打开后再保存"}, 409)
        try:
            mediainfo = validate_mediainfo_payload(payload.get("mediainfo"))
        except ValueError as exc:
            return MediaEditingResult({"error": str(exc)}, 400)
        if self._commands is None:
            return MediaEditingResult({"error": "媒体副作用执行器未就绪"}, 503)
        try:
            outcome = self._commands.apply_mediainfo(context, mediainfo)
        except (MediaAssetAlignmentError, MediaIdentityConflictError):
            return MediaEditingResult({"error": "当前媒体信息已变化，请重新打开后再保存"}, 409)
        except Exception:
            logger.exception("failed to stage P5 MediaInfo update")
            return MediaEditingResult({"error": "保存失败"}, 500)
        if outcome.status is not MediaCommandStatus.CONFIRMED:
            return _effect_failure(outcome, "缓存已保存，但写入 Emby 失败，请确认桥接插件已安装")
        details = dict(outcome.details or {})
        return MediaEditingResult(
            {
                "message": "媒体信息已更新并写入 Emby",
                "result": dict(outcome.provider_result or {}),
                "washing_priority": details.get("washing_priority"),
                "washing_priority_error": details.get("washing_priority_error"),
            }
        )

    def _context(
        self,
        item_id: str,
    ) -> tuple[MediaInfoEditContext | None, MediaEditingResult | None]:
        unavailable = self._unavailable()
        if unavailable is not None:
            return None, unavailable
        try:
            return self._repository.resolve_media_info_context(item_id), None
        except MediaIdentityNotFoundError:
            return None, MediaEditingResult({"error": "数据库中未找到该媒体项"}, 404)
        except MediaInfoFingerprintNotFoundError:
            return None, MediaEditingResult({"error": "未找到该媒体项对应的 SHA1"}, 404)
        except MediaInfoCacheNotFoundError:
            return None, MediaEditingResult({"error": "未找到该媒体的格式化媒体信息缓存"}, 404)
        except (MediaAssetAlignmentError, MediaIdentityConflictError):
            return None, MediaEditingResult({"error": "媒体资产版本绑定冲突"}, 409)
        except Exception:
            logger.exception("failed to resolve P5 MediaInfo edit context")
            return None, MediaEditingResult({"error": "服务器内部错误"}, 500)

    def _unavailable(self) -> MediaEditingResult | None:
        return self.unavailable()

    def unavailable(self) -> MediaEditingResult | None:
        if self._processor_ready():
            return None
        return MediaEditingResult({"error": "核心处理器未就绪。"}, 503)


def extract_marker_seconds(
    mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
) -> dict[str, float | bool | None]:
    root = _mediainfo_root(mediainfo)
    chapters = root.get("Chapters") if root is not None else []
    if not isinstance(chapters, list):
        chapters = []
    result: dict[str, float | bool | None] = {
        "intro_start_seconds": None,
        "intro_end_seconds": None,
        "credits_start_seconds": None,
    }
    for chapter in chapters:
        if not isinstance(chapter, Mapping):
            continue
        marker = str(chapter.get("MarkerType") or "").strip()
        seconds = ticks_to_seconds(chapter.get("StartPositionTicks"))
        if seconds is None:
            continue
        if marker == "IntroStart":
            result["intro_start_seconds"] = seconds
        elif marker == "IntroEnd":
            result["intro_end_seconds"] = seconds
        elif marker == "CreditsStart":
            result["credits_start_seconds"] = seconds

    runtime_ticks: Any = None
    if root is not None:
        source = root.get("MediaSourceInfo")
        if not isinstance(source, Mapping):
            source = root
        runtime_ticks = source.get("RunTimeTicks") or root.get("RunTimeTicks")
    result["runtime_seconds"] = ticks_to_seconds(runtime_ticks)
    result["intro_enabled"] = result["intro_start_seconds"] is not None and result["intro_end_seconds"] is not None
    result["credits_enabled"] = result["credits_start_seconds"] is not None
    return result


def _effect_failure(outcome: MediaCommandOutcome, legacy_message: str) -> MediaEditingResult:
    payload = {
        "error": outcome.error or legacy_message,
        "effect_id": outcome.effect_id,
        "effect_status": outcome.status.value,
    }
    if outcome.status is MediaCommandStatus.INDETERMINATE:
        return MediaEditingResult(payload, 503)
    if outcome.status is MediaCommandStatus.PENDING:
        return MediaEditingResult(payload, 202)
    return MediaEditingResult(payload, 502)


def ticks_to_seconds(value: object) -> float | None:
    try:
        ticks = int(float(str(value)))
    except (TypeError, ValueError):
        return None
    return round(ticks / 10_000_000, 3) if ticks >= 0 else None


def seconds_to_ticks(value: object, label: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{label} 必须填写有效秒数")
    try:
        seconds = float(str(value))
    except (TypeError, ValueError):
        raise ValueError(f"{label} 必须填写有效秒数") from None
    if seconds < 0:
        raise ValueError(f"{label} 不能小于 0")
    return int(round(seconds * 10_000_000))


def prepare_chapter_update(
    context: MediaInfoEditContext,
    payload: Mapping[str, Any],
) -> ChapterUpdate:
    expected_sha1 = str(payload.get("sha1") or "").strip().upper()
    if expected_sha1 != context.sha1:
        raise ValueError("当前媒体信息已变化，请重新打开后再保存")
    intro_enabled = bool(payload.get("intro_enabled"))
    credits_enabled = bool(payload.get("credits_enabled"))
    intro_start_ticks: int | None = None
    intro_end_ticks: int | None = None
    credits_start_ticks: int | None = None
    if intro_enabled:
        intro_start_ticks = seconds_to_ticks(payload.get("intro_start_seconds"), "片头开始")
        intro_end_ticks = seconds_to_ticks(payload.get("intro_end_seconds"), "片头结束")
        if intro_end_ticks <= intro_start_ticks:
            raise ValueError("片头结束必须大于片头开始")
    if credits_enabled:
        credits_start_ticks = seconds_to_ticks(payload.get("credits_start_seconds"), "片尾开始")

    runtime_seconds = extract_marker_seconds(context.mediainfo).get("runtime_seconds")
    if isinstance(runtime_seconds, int | float) and runtime_seconds > 0:
        runtime_ticks = seconds_to_ticks(runtime_seconds, "视频时长")
        if intro_end_ticks is not None and intro_end_ticks >= runtime_ticks:
            raise ValueError("片头结束不能超过视频时长")
        if credits_start_ticks is not None and credits_start_ticks >= runtime_ticks:
            raise ValueError("片尾开始不能超过视频时长")

    return ChapterUpdate(
        mediainfo=merge_marker_chapters(
            context.mediainfo,
            intro_start_ticks=intro_start_ticks,
            intro_end_ticks=intro_end_ticks,
            credits_start_ticks=credits_start_ticks,
        ),
        intro_start_ticks=intro_start_ticks,
        intro_end_ticks=intro_end_ticks,
        credits_start_ticks=credits_start_ticks,
        clear_intro=not intro_enabled,
        clear_credits=not credits_enabled,
    )


def merge_marker_chapters(
    mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
    *,
    intro_start_ticks: int | None,
    intro_end_ticks: int | None,
    credits_start_ticks: int | None,
) -> Mapping[str, Any] | tuple[Mapping[str, Any], ...]:
    cloned = deepcopy(mediainfo)
    root = _mediainfo_root(cloned)
    if root is None or not isinstance(root, dict):
        raise ValueError("媒体信息格式无效")
    existing = root.get("Chapters")
    if not isinstance(existing, list):
        existing = []
    marker_types = {"IntroStart", "IntroEnd", "CreditsStart"}
    kept = [
        dict(item) if isinstance(item, Mapping) else item
        for item in existing
        if not (isinstance(item, Mapping) and str(item.get("MarkerType") or "") in marker_types)
    ]
    additions: list[dict[str, Any]] = []
    if intro_start_ticks is not None and intro_end_ticks is not None:
        additions.extend(
            [
                {
                    "StartPositionTicks": intro_start_ticks,
                    "Name": "片头",
                    "MarkerType": "IntroStart",
                    "ChapterIndex": len(kept),
                },
                {
                    "StartPositionTicks": intro_end_ticks,
                    "Name": "片头结束",
                    "MarkerType": "IntroEnd",
                    "ChapterIndex": len(kept) + 1,
                },
            ]
        )
    if credits_start_ticks is not None:
        additions.append(
            {
                "StartPositionTicks": credits_start_ticks,
                "Name": "片尾",
                "MarkerType": "CreditsStart",
                "ChapterIndex": len(kept) + len(additions),
            }
        )
    root["Chapters"] = kept + additions
    return cloned


def validate_mediainfo_payload(value: object) -> Mapping[str, Any] | tuple[Mapping[str, Any], ...]:
    if isinstance(value, Mapping):
        normalized: Mapping[str, Any] | tuple[Mapping[str, Any], ...] = dict(value)
    elif isinstance(value, list) and value and all(isinstance(item, Mapping) for item in value):
        normalized = tuple(dict(item) for item in value)
    else:
        raise ValueError("媒体信息格式无效")
    root = _mediainfo_root(normalized)
    source = root.get("MediaSourceInfo") if root is not None else None
    if not isinstance(source, Mapping) or not isinstance(source.get("MediaStreams"), list):
        raise ValueError("媒体信息缺少 MediaSourceInfo.MediaStreams")
    return normalized


def _mediainfo_root(
    mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
) -> Mapping[str, Any] | None:
    if isinstance(mediainfo, tuple):
        return mediainfo[0] if mediainfo else None
    return mediainfo


def _json_mediainfo(context: MediaInfoEditContext) -> Any:
    if isinstance(context.mediainfo, tuple):
        return [dict(value) for value in context.mediainfo]
    return dict(context.mediainfo)


__all__ = [
    "ChapterUpdate",
    "MediaCommandOutcome",
    "MediaCommandStatus",
    "MediaEditingResult",
    "MediaInfoCommandGateway",
    "MediaEditingService",
    "extract_marker_seconds",
    "merge_marker_chapters",
    "prepare_chapter_update",
    "seconds_to_ticks",
    "ticks_to_seconds",
    "validate_mediainfo_payload",
]
