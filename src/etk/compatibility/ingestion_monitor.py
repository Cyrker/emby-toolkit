from __future__ import annotations

import threading
from collections.abc import Mapping
from typing import Any, Protocol


class PersistentIngestionIngress(Protocol):
    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        related_path: str | None = None,
        source: str = "watchdog",
        source_event_key: str | None = None,
    ) -> object: ...


class PersistentIngestionBinding(Protocol):
    def enqueue_binding(
        self,
        path: str,
        *,
        mediainfo: Mapping[str, Any] | None = None,
    ) -> object: ...


_LOCK = threading.RLock()
_INGRESS: PersistentIngestionIngress | None = None
_BINDING: PersistentIngestionBinding | None = None


def install_persistent_ingestion_ingress(gateway: PersistentIngestionIngress) -> None:
    if gateway is None:
        raise TypeError("persistent ingestion ingress gateway is required")
    global _INGRESS
    with _LOCK:
        if _INGRESS is not None and _INGRESS is not gateway:
            raise RuntimeError("a different persistent ingestion ingress gateway is already installed")
        _INGRESS = gateway


def clear_persistent_ingestion_ingress(gateway: PersistentIngestionIngress | None = None) -> None:
    global _INGRESS
    with _LOCK:
        if gateway is None or _INGRESS is gateway:
            _INGRESS = None


def persistent_ingestion_ingress() -> PersistentIngestionIngress | None:
    with _LOCK:
        return _INGRESS


def install_persistent_ingestion_binding(gateway: PersistentIngestionBinding) -> None:
    if gateway is None:
        raise TypeError("persistent ingestion binding gateway is required")
    global _BINDING
    with _LOCK:
        if _BINDING is not None and _BINDING is not gateway:
            raise RuntimeError("a different persistent ingestion binding gateway is already installed")
        _BINDING = gateway


def clear_persistent_ingestion_binding(gateway: PersistentIngestionBinding | None = None) -> None:
    global _BINDING
    with _LOCK:
        if gateway is None or _BINDING is gateway:
            _BINDING = None


def persistent_ingestion_binding() -> PersistentIngestionBinding | None:
    with _LOCK:
        return _BINDING


__all__ = [
    "PersistentIngestionBinding",
    "PersistentIngestionIngress",
    "clear_persistent_ingestion_binding",
    "clear_persistent_ingestion_ingress",
    "install_persistent_ingestion_binding",
    "install_persistent_ingestion_ingress",
    "persistent_ingestion_binding",
    "persistent_ingestion_ingress",
]
