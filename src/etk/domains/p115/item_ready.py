from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Protocol

from etk.domains.media.models import MediaItemType, ServiceResult

from .media_info import P115MediaInfoError


class P115BridgeIdentityResolver(Protocol):
    def sha1_by_pick_code(self, pick_code: object) -> str: ...


class P115BridgeEmbyItemVerifier(Protocol):
    def verified_item(
        self,
        sha1: str,
        item_id: str,
        *,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None: ...


class P115PendingBindingPort(Protocol):
    """Durable pending-binding boundary; ``None`` means no pending import owns the item."""

    def replay_pending_binding(
        self,
        *,
        emby_item_id: str,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None: ...

    def accept_pending_binding(
        self,
        item: Mapping[str, Any],
        *,
        sha1: str,
        expected_pick_code: str = "",
    ) -> Mapping[str, Any] | None: ...


class MediaItemBindingPort(Protocol):
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
    ) -> Mapping[str, Any]: ...


class P115ItemReadyService:
    """Native Bridge callback orchestration without legacy monitor or local-queue fallbacks."""

    def __init__(
        self,
        *,
        identities: P115BridgeIdentityResolver,
        emby: P115BridgeEmbyItemVerifier,
        pending_bindings: P115PendingBindingPort,
        media: MediaItemBindingPort,
    ) -> None:
        self._identities = identities
        self._emby = emby
        self._pending_bindings = pending_bindings
        self._media = media

    def accept_by_pick_code(self, pick_code: object, emby_item_id: object) -> ServiceResult:
        normalized_pick_code = str(pick_code or "").strip()
        try:
            sha1 = _canonical_sha1(self._identities.sha1_by_pick_code(normalized_pick_code))
        except (P115MediaInfoError, ValueError):
            sha1 = ""
        if not sha1:
            return ServiceResult({"error": "media info cache not found"}, 404)
        return self._accept(
            sha1,
            emby_item_id,
            expected_pick_code=normalized_pick_code,
        )

    def accept_by_sha1(self, sha1: object, emby_item_id: object) -> ServiceResult:
        return self._accept(_canonical_sha1(sha1), emby_item_id)

    def _accept(
        self,
        sha1: str,
        emby_item_id: object,
        *,
        expected_pick_code: str = "",
    ) -> ServiceResult:
        item_id = str(emby_item_id or "").strip()
        if not sha1 or not re.fullmatch(r"\d{1,20}", item_id):
            return ServiceResult({"ok": False, "error": "item identity mismatch"}, 409)
        replay = self._pending_bindings.replay_pending_binding(
            emby_item_id=item_id,
            sha1=sha1,
            expected_pick_code=expected_pick_code,
        )
        if replay is not None:
            return _binding_outcome(replay)
        item = self._emby.verified_item(
            sha1,
            item_id,
            expected_pick_code=expected_pick_code,
        )
        if not isinstance(item, Mapping) or not item:
            return ServiceResult({"ok": False, "error": "item identity mismatch"}, 409)

        verified_item = dict(item)
        verified_item_id = str(verified_item.get("Id") or "").strip()
        item_type = MediaItemType.parse(verified_item.get("Type"))
        if not verified_item_id or item_type is None or item_type not in {MediaItemType.MOVIE, MediaItemType.EPISODE}:
            return ServiceResult({"ok": False, "skipped": True, "reason": "invalid_item"}, 409)

        pending = self._pending_bindings.accept_pending_binding(
            verified_item,
            sha1=sha1,
            expected_pick_code=expected_pick_code,
        )
        if pending is not None:
            return _binding_outcome(pending)

        provider_ids = verified_item.get("ProviderIds")
        provider_ids = provider_ids if isinstance(provider_ids, Mapping) else {}
        rebound = self._media.bind_emby_item(
            item_id=verified_item_id,
            item_type=item_type,
            item_path=str(verified_item.get("Path") or ""),
            sha1=sha1,
            pick_code=expected_pick_code,
            tmdb_id=str(provider_ids.get("Tmdb") or ""),
            series_id=str(verified_item.get("SeriesId") or ""),
            season_id=str(verified_item.get("SeasonId") or ""),
        )
        if not isinstance(rebound, Mapping):
            raise TypeError("media binding port returned an invalid outcome")
        normalized = dict(rebound)
        if normalized.get("updated") or normalized.get("parent_updates"):
            return ServiceResult({"ok": True, "rebound": True, **normalized})
        reason = str(normalized.get("reason") or "").strip()
        if not reason:
            return ServiceResult({"ok": True, "skipped": True, "reason": "already_bound", **normalized})
        retryable = reason in {"asset_not_found", "database_error"}
        return ServiceResult(
            {
                "ok": False,
                "skipped": False,
                "retryable": retryable,
                **normalized,
            },
            503 if retryable else 409,
        )


def _binding_outcome(outcome: Mapping[str, Any]) -> ServiceResult:
    normalized = dict(outcome)
    if normalized.get("ok") is False:
        return ServiceResult(normalized, 503 if normalized.get("retryable") else 409)
    return ServiceResult(normalized)


def _canonical_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if re.fullmatch(r"[A-F0-9]{40}", normalized) else ""


__all__ = [
    "MediaItemBindingPort",
    "P115BridgeEmbyItemVerifier",
    "P115BridgeIdentityResolver",
    "P115ItemReadyService",
    "P115PendingBindingPort",
]
