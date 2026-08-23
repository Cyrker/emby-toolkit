from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any, Protocol

from .models import ServiceResult

P5_MEDIA_EVENT_NAMES = frozenset(
    {
        "metadata.update",
        "image.update",
    }
)

FUTURE_DOMAIN_EVENT_NAMES = frozenset(
    {
        "user.policyupdated",
        "item.markfavorite",
        "item.unmarkfavorite",
        "item.markplayed",
        "item.markunplayed",
        "item.rate",
        "playback.start",
        "playback.pause",
        "playback.stop",
        "playback.halfway",
        "collection.items.removed",
    }
)


class EmbyEventOwner(StrEnum):
    P5_MEDIA = "p5-media"
    FUTURE_DOMAIN = "future-domain"
    LEGACY_UNKNOWN = "legacy-unknown"


class P5MediaEventGateway(Protocol):
    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult: ...


class FutureDomainEventGateway(Protocol):
    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult: ...


def classify_emby_event(event_name: object) -> EmbyEventOwner:
    normalized = str(event_name or "").strip()
    if normalized in P5_MEDIA_EVENT_NAMES:
        return EmbyEventOwner.P5_MEDIA
    if normalized in FUTURE_DOMAIN_EVENT_NAMES:
        return EmbyEventOwner.FUTURE_DOMAIN
    return EmbyEventOwner.LEGACY_UNKNOWN


class EmbyEventDispatcher:
    """Route the Bridge event catalog without claiming future-domain behavior as P5."""

    def __init__(
        self,
        *,
        p5_media: P5MediaEventGateway,
        future_domain: FutureDomainEventGateway,
    ) -> None:
        self._p5_media = p5_media
        self._future_domain = future_domain

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        owner = classify_emby_event(payload.get("Event"))
        if owner is EmbyEventOwner.P5_MEDIA:
            return self._p5_media.handle(
                payload,
                host_url=host_url,
                remote_addr=remote_addr,
            )
        if owner is EmbyEventOwner.FUTURE_DOMAIN:
            return self._future_domain.handle(
                payload,
                host_url=host_url,
                remote_addr=remote_addr,
            )
        return ServiceResult({"status": "event_ignored_not_in_trigger_list"})


__all__ = [
    "EmbyEventDispatcher",
    "EmbyEventOwner",
    "FUTURE_DOMAIN_EVENT_NAMES",
    "FutureDomainEventGateway",
    "P5_MEDIA_EVENT_NAMES",
    "P5MediaEventGateway",
    "classify_emby_event",
]
