from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import MediaEffectProjection
from etk.integrations import ProviderDisposition, ProviderResponse

SHARED_INTRO_EXPORT_EFFECT = "sharing.intro.export"


class IntroExportSettings(Protocol):
    def load(self, key: str) -> Any: ...


class IntroExportCenterPort(Protocol):
    def upload_intro_chapters(
        self,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        *,
        file_name: str = "",
        reason: str = "",
    ) -> ProviderResponse[dict[str, Any]]: ...


class SharedIntroExportEffectDelivery:
    effect_types = frozenset({SHARED_INTRO_EXPORT_EFFECT})

    def __init__(self, settings: IntroExportSettings, center: IntroExportCenterPort) -> None:
        self._settings = settings
        self._center = center

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type != SHARED_INTRO_EXPORT_EFFECT:
            raise ValueError(f"unsupported shared intro effect type: {effect.effect_type}")
        return {
            "operation": "shared-intro-export",
            "sha1": str(effect.payload.get("sha1") or "").strip().upper(),
            "skipped": True,
            "reason": "feature_retired",
        }


def _raise_disposition(
    disposition: ProviderDisposition,
    exc: BaseException,
    *,
    retry_after_seconds: float,
    status_code: int | None = None,
) -> None:
    error: dict[str, Any] = {"operation": "shared-intro-export"}
    if status_code is not None:
        error["status_code"] = status_code
    if disposition is ProviderDisposition.RETRYABLE:
        raise MediaEffectDeliveryRetryable(
            "Shared Center intro export is temporarily unavailable",
            error=error,
            retry_after_seconds=retry_after_seconds,
        ) from exc
    if disposition in {ProviderDisposition.REJECTED, ProviderDisposition.NOT_FOUND}:
        raise MediaEffectDeliveryRejected(
            "Shared Center rejected intro export",
            error=error,
        ) from exc
    raise MediaEffectDeliveryIndeterminate(
        "Shared Center intro export outcome is unknown",
        error=error,
    ) from exc


def _chapters(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != 2:
        raise ValueError("shared intro export requires exactly two intro chapters")
    chapters: list[dict[str, Any]] = []
    markers: set[str] = set()
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError("shared intro export chapter must be an object")
        marker = str(raw.get("MarkerType") or "").strip()
        if marker not in {"IntroStart", "IntroEnd"} or marker in markers:
            raise ValueError("shared intro export chapter markers are invalid")
        try:
            ticks = int(str(raw.get("StartPositionTicks")))
        except (TypeError, ValueError) as exc:
            raise ValueError("shared intro export chapter ticks are invalid") from exc
        if ticks < 0:
            raise ValueError("shared intro export chapter ticks are invalid")
        markers.add(marker)
        chapters.append(
            {
                "StartPositionTicks": ticks,
                "Name": str(raw.get("Name") or ("片头" if marker == "IntroStart" else "片头结束")),
                "MarkerType": marker,
                "ChapterIndex": _chapter_index(raw.get("ChapterIndex"), index),
            }
        )
    if markers != {"IntroStart", "IntroEnd"}:
        raise ValueError("shared intro export chapter markers are incomplete")
    by_marker = {chapter["MarkerType"]: chapter for chapter in chapters}
    if by_marker["IntroEnd"]["StartPositionTicks"] <= by_marker["IntroStart"]["StartPositionTicks"]:
        raise ValueError("shared intro export chapter range is invalid")
    chapters.sort(key=lambda chapter: (chapter["StartPositionTicks"], chapter["MarkerType"]))
    return chapters


def _sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 40:
        raise ValueError("shared intro export SHA1 is invalid")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("shared intro export SHA1 is invalid") from exc
    return normalized


def _enabled(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _chapter_index(value: object, fallback: int) -> int:
    if value is None:
        return fallback
    try:
        return int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError("shared intro export chapter index is invalid") from exc


__all__ = ["SHARED_INTRO_EXPORT_EFFECT", "SharedIntroExportEffectDelivery"]
