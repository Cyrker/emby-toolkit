from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, Protocol

from .media_info import (
    P115MediaInfoCacheRecord,
    P115MediaInfoCacheRepository,
    P115MediaInfoFormattedCacheWriter,
)

logger = logging.getLogger(__name__)

INTRO_MARKER_TYPES = frozenset({"IntroStart", "IntroEnd"})
_MAX_CAS_ATTEMPTS = 3
_SHA1_PATTERN = re.compile(r"[A-F0-9]{40}")
_EMBY_ITEM_ID_PATTERN = re.compile(r"\d{1,20}")


class P115IntroSyncError(RuntimeError):
    pass


class P115IntroSyncNotFound(P115IntroSyncError):
    pass


class P115IntroSyncConflict(P115IntroSyncError):
    pass


class P115IntroSyncFeature(Protocol):
    def enabled(self) -> bool: ...


class P115IntroEmbyItemVerifier(Protocol):
    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None: ...


class P115IntroSyncRepository(
    P115MediaInfoCacheRepository,
    P115MediaInfoFormattedCacheWriter,
    Protocol,
):
    pass


class P115IntroSnapshotExporter(Protocol):
    def publish_if_changed(
        self,
        *,
        sha1: str,
        chapters: Sequence[Mapping[str, Any]],
        file_name: str,
        reason: str,
        cache_revision: int,
    ) -> None: ...


def extract_intro_chapters(mediainfo: object) -> list[dict[str, Any]]:
    """Extract the legacy-compatible IntroStart/IntroEnd chapter snapshot."""
    root = mediainfo
    if isinstance(root, list) and root:
        root = root[0]
    if not isinstance(root, Mapping):
        return []
    raw_chapters = root.get("Chapters")
    if not isinstance(raw_chapters, list):
        return []

    chapters: list[dict[str, Any]] = []
    for index, item in enumerate(raw_chapters):
        if not isinstance(item, Mapping):
            continue
        marker = str(item.get("MarkerType") or "").strip()
        if marker not in INTRO_MARKER_TYPES:
            continue
        if "#SA" in str(item.get("Name") or ""):
            return []
        try:
            tick_value: Any = item.get("StartPositionTicks")
            ticks = int(float(tick_value))
        except Exception:
            continue
        if ticks < 0:
            continue
        chapter_index = item.get("ChapterIndex")
        chapters.append(
            {
                "StartPositionTicks": ticks,
                "Name": str(item.get("Name") or ("片头" if marker == "IntroStart" else "片头结束")),
                "MarkerType": marker,
                "ChapterIndex": int(chapter_index if chapter_index is not None else index),
            }
        )

    if not {"IntroStart", "IntroEnd"}.issubset(chapter["MarkerType"] for chapter in chapters):
        return []
    chapters.sort(key=lambda chapter: (int(chapter["StartPositionTicks"]), str(chapter["MarkerType"])))
    return chapters


def merge_intro_chapters(
    mediainfo: object,
    chapters: Sequence[Mapping[str, Any]],
) -> object:
    """Return a copy with legacy intro markers replaced and all other chapters retained."""
    merged = deepcopy(mediainfo)
    if not chapters:
        return merged
    if isinstance(merged, list):
        if not merged or not isinstance(merged[0], dict):
            return merged
        target = merged[0]
    elif isinstance(merged, dict):
        target = merged
    else:
        return merged
    existing = target.get("Chapters")
    if not isinstance(existing, list):
        existing = []
    kept = [
        chapter
        for chapter in existing
        if not (isinstance(chapter, Mapping) and str(chapter.get("MarkerType") or "") in INTRO_MARKER_TYPES)
    ]
    target["Chapters"] = kept + [dict(chapter) for chapter in chapters if isinstance(chapter, Mapping)]
    return merged


class P115IntroSyncService:
    """Persist verified Emby intro chapters with bounded revision-CAS retries."""

    def __init__(
        self,
        repository: P115IntroSyncRepository,
        feature: P115IntroSyncFeature,
        verifier: P115IntroEmbyItemVerifier,
        exporter: P115IntroSnapshotExporter,
        account_id: Callable[[], object],
    ) -> None:
        self._repository = repository
        self._feature = feature
        self._verifier = verifier
        self._exporter = exporter
        self._account_id = account_id

    def sync_by_pick_code(self, pick_code: object, emby_item_id: object) -> dict[str, Any]:
        normalized_pick_code = str(pick_code or "").strip()
        account_id = str(self._account_id() or "").strip()
        sha1 = self._repository.resolve_sha1_by_pick_code(
            normalized_pick_code,
            account_id=account_id,
        )
        if not sha1:
            raise P115IntroSyncNotFound("media info cache not found")
        return self._sync(sha1, emby_item_id, expected_pick_code=normalized_pick_code)

    def sync_by_sha1(self, sha1: object, emby_item_id: object) -> dict[str, Any]:
        return self._sync(sha1, emby_item_id)

    def _sync(
        self,
        sha1: object,
        emby_item_id: object,
        *,
        expected_pick_code: str = "",
    ) -> dict[str, Any]:
        if not self._feature.enabled():
            return {"ok": False, "skipped": True, "reason": "shared_intro_disabled"}

        normalized_sha1 = _normalized_sha1(sha1)
        item_id = str(emby_item_id or "").strip()
        if not normalized_sha1 or not _EMBY_ITEM_ID_PATTERN.fullmatch(item_id):
            return {"ok": False, "skipped": True, "reason": "item_identity_mismatch"}
        item = self._verifier.verified_item(
            normalized_sha1,
            item_id,
            expected_pick_code=expected_pick_code,
        )
        if not isinstance(item, Mapping):
            return {"ok": False, "skipped": True, "reason": "item_identity_mismatch"}

        chapters = extract_intro_chapters({"Chapters": item.get("Chapters") or []})
        if not chapters:
            return {"ok": False, "skipped": True, "reason": "intro_not_detected"}

        for _attempt in range(_MAX_CAS_ATTEMPTS):
            record = self._repository.find_by_sha1(normalized_sha1)
            formatted = _formatted_value(record)
            if not formatted:
                return {"ok": False, "skipped": True, "reason": "media_info_cache_not_found"}
            if extract_intro_chapters(formatted) == chapters:
                return {"ok": True, "updated": False, "chapter_count": len(chapters)}
            merged = merge_intro_chapters(formatted, chapters)
            assert record is not None
            updated = self._repository.replace_formatted_if_revision(
                normalized_sha1,
                expected_cache_revision=record.cache_revision,
                formatted=merged,
            )
            if updated is None:
                continue
            try:
                self._exporter.publish_if_changed(
                    sha1=normalized_sha1,
                    chapters=tuple(dict(chapter) for chapter in chapters),
                    file_name=str(item.get("Name") or "").strip(),
                    reason="emby_item_updated",
                    cache_revision=updated.cache_revision,
                )
            except Exception:
                logger.debug(
                    "P115 intro snapshot export failed after cache revision %s",
                    updated.cache_revision,
                    exc_info=True,
                )
            return {"ok": True, "updated": True, "chapter_count": len(chapters)}

        raise P115IntroSyncConflict("media info cache changed during intro synchronization")


def _normalized_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if _SHA1_PATTERN.fullmatch(normalized) else ""


def _formatted_value(record: P115MediaInfoCacheRecord | None) -> object | None:
    if record is None:
        return None
    formatted = record.formatted
    if isinstance(formatted, str):
        formatted = json.loads(formatted)
    return formatted


__all__ = [
    "INTRO_MARKER_TYPES",
    "P115IntroEmbyItemVerifier",
    "P115IntroSnapshotExporter",
    "P115IntroSyncConflict",
    "P115IntroSyncError",
    "P115IntroSyncFeature",
    "P115IntroSyncNotFound",
    "P115IntroSyncRepository",
    "P115IntroSyncService",
    "extract_intro_chapters",
    "merge_intro_chapters",
]
