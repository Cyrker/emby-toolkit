from __future__ import annotations

import threading
from typing import Protocol
from uuid import UUID

from etk.domains.media.effects import MediaEffectProjection


class PersistentMediaEffectGateway(Protocol):
    def execute(self, effect_id: UUID) -> MediaEffectProjection: ...


_LOCK = threading.RLock()
_GATEWAY: PersistentMediaEffectGateway | None = None


def install_persistent_media_effect_gateway(gateway: PersistentMediaEffectGateway) -> None:
    global _GATEWAY
    with _LOCK:
        if _GATEWAY is not None and _GATEWAY is not gateway:
            raise RuntimeError("a persistent media effect gateway is already installed")
        _GATEWAY = gateway


def clear_persistent_media_effect_gateway(gateway: PersistentMediaEffectGateway) -> None:
    global _GATEWAY
    with _LOCK:
        if _GATEWAY is gateway:
            _GATEWAY = None


def persistent_media_effect_gateway_installed() -> bool:
    with _LOCK:
        return _GATEWAY is not None


def execute_persistent_media_effect(effect_id: UUID) -> MediaEffectProjection:
    with _LOCK:
        gateway = _GATEWAY
    if gateway is None:
        raise RuntimeError("persistent media effect gateway is not installed")
    return gateway.execute(effect_id)


__all__ = [
    "PersistentMediaEffectGateway",
    "clear_persistent_media_effect_gateway",
    "execute_persistent_media_effect",
    "install_persistent_media_effect_gateway",
    "persistent_media_effect_gateway_installed",
]
