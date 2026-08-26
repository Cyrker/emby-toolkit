from __future__ import annotations

import threading
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol


class PersistentIntroDetectionGateway(Protocol):
    """P6.10 submission boundary used by still-legacy P7 trigger call sites."""

    def enqueue_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
        *,
        trigger: str = "import",
    ) -> Mapping[str, Any]: ...

    def enqueue_imported_episode_ids(
        self,
        episode_ids: Sequence[Any],
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]: ...

    def enqueue_favorite_item(self, item: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def enqueue_playback_halfway(self, item: Mapping[str, Any]) -> Mapping[str, Any]: ...

    def enqueue_active_backfill(self, limit: int = 50, *, force: bool = False) -> Mapping[str, Any]: ...


class LazyPersistentIntroDetectionGateway:
    """Install the compatibility boundary without materializing database services."""

    def __init__(self, provider: Callable[[], PersistentIntroDetectionGateway]) -> None:
        self._provider = provider

    def enqueue_item(
        self,
        item: Mapping[str, Any],
        sha1: str = "",
        *,
        trigger: str = "import",
    ) -> Mapping[str, Any]:
        return self._provider().enqueue_item(item, sha1, trigger=trigger)

    def enqueue_imported_episode_ids(
        self,
        episode_ids: Sequence[Any],
        *,
        allow_playback_followup: bool = False,
    ) -> Mapping[str, Any]:
        return self._provider().enqueue_imported_episode_ids(
            episode_ids,
            allow_playback_followup=allow_playback_followup,
        )

    def enqueue_favorite_item(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._provider().enqueue_favorite_item(item)

    def enqueue_playback_halfway(self, item: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._provider().enqueue_playback_halfway(item)

    def enqueue_active_backfill(self, limit: int = 50, *, force: bool = False) -> Mapping[str, Any]:
        return self._provider().enqueue_active_backfill(limit, force=force)


_LOCK = threading.RLock()
_GATEWAY: PersistentIntroDetectionGateway | None = None


def install_persistent_intro_detection_gateway(gateway: PersistentIntroDetectionGateway) -> None:
    if gateway is None:
        raise TypeError("persistent intro detection gateway is required")
    global _GATEWAY
    with _LOCK:
        if _GATEWAY is not None and _GATEWAY is not gateway:
            raise RuntimeError("a different persistent intro detection gateway is already installed")
        _GATEWAY = gateway


def clear_persistent_intro_detection_gateway(
    gateway: PersistentIntroDetectionGateway | None = None,
) -> None:
    global _GATEWAY
    with _LOCK:
        if gateway is None or _GATEWAY is gateway:
            _GATEWAY = None


def persistent_intro_detection_gateway() -> PersistentIntroDetectionGateway | None:
    with _LOCK:
        return _GATEWAY


__all__ = [
    "LazyPersistentIntroDetectionGateway",
    "PersistentIntroDetectionGateway",
    "clear_persistent_intro_detection_gateway",
    "install_persistent_intro_detection_gateway",
    "persistent_intro_detection_gateway",
]
