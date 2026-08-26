from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from typing import Protocol


class PersistentUploadMonitorIngress(Protocol):
    def enabled(self) -> bool: ...

    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        related_path: str | None = None,
        source: str = "watchdog",
        source_event_key: str | None = None,
    ) -> object: ...

    def scan_existing(self) -> int: ...

    def apply_config(self) -> object: ...


class PersistentUploadSourceCleanup(Protocol):
    def delete_sources(self, pickcodes: Sequence[str]) -> Mapping[str, object]: ...


_LOCK = threading.RLock()
_INGRESS: PersistentUploadMonitorIngress | None = None
_SOURCE_CLEANUP: PersistentUploadSourceCleanup | None = None


def install_persistent_upload_monitor_ingress(gateway: PersistentUploadMonitorIngress) -> None:
    if gateway is None:
        raise TypeError("persistent upload monitor ingress gateway is required")
    global _INGRESS
    with _LOCK:
        if _INGRESS is not None and _INGRESS is not gateway:
            raise RuntimeError("a different persistent upload monitor ingress gateway is already installed")
        _INGRESS = gateway


def clear_persistent_upload_monitor_ingress(gateway: PersistentUploadMonitorIngress | None = None) -> None:
    global _INGRESS
    with _LOCK:
        if gateway is None or _INGRESS is gateway:
            _INGRESS = None


def persistent_upload_monitor_ingress() -> PersistentUploadMonitorIngress | None:
    with _LOCK:
        return _INGRESS


def install_persistent_upload_source_cleanup(gateway: PersistentUploadSourceCleanup) -> None:
    if gateway is None:
        raise TypeError("persistent upload source cleanup gateway is required")
    global _SOURCE_CLEANUP
    with _LOCK:
        if _SOURCE_CLEANUP is not None and _SOURCE_CLEANUP is not gateway:
            raise RuntimeError("a different persistent upload source cleanup gateway is already installed")
        _SOURCE_CLEANUP = gateway


def clear_persistent_upload_source_cleanup(gateway: PersistentUploadSourceCleanup | None = None) -> None:
    global _SOURCE_CLEANUP
    with _LOCK:
        if gateway is None or _SOURCE_CLEANUP is gateway:
            _SOURCE_CLEANUP = None


def persistent_upload_source_cleanup() -> PersistentUploadSourceCleanup | None:
    with _LOCK:
        return _SOURCE_CLEANUP


__all__ = [
    "PersistentUploadMonitorIngress",
    "PersistentUploadSourceCleanup",
    "clear_persistent_upload_monitor_ingress",
    "clear_persistent_upload_source_cleanup",
    "install_persistent_upload_monitor_ingress",
    "install_persistent_upload_source_cleanup",
    "persistent_upload_monitor_ingress",
    "persistent_upload_source_cleanup",
]
