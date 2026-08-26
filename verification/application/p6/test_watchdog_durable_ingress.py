from __future__ import annotations

from typing import Any

import pytest
from watchdog.events import FileCreatedEvent, FileDeletedEvent, FileModifiedEvent, FileMovedEvent

from etk.compatibility.ingestion_monitor import (
    clear_persistent_ingestion_binding,
    clear_persistent_ingestion_ingress,
    install_persistent_ingestion_binding,
    install_persistent_ingestion_ingress,
)


@pytest.fixture(autouse=True)
def _clear_gateways():
    clear_persistent_ingestion_ingress()
    clear_persistent_ingestion_binding()
    yield
    clear_persistent_ingestion_ingress()
    clear_persistent_ingestion_binding()


class _Ingress:
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def record_event(
        self,
        event_type: str,
        path: str,
        *,
        related_path: str | None = None,
        source: str = "watchdog",
        source_event_key: str | None = None,
    ) -> None:
        self.events.append(
            {
                "event_type": event_type,
                "path": path,
                "related_path": related_path,
                "source": source,
                "source_event_key": source_event_key,
            }
        )


class _Binding:
    def __init__(self) -> None:
        self.values: list[tuple[str, dict[str, Any] | None]] = []

    def enqueue_binding(self, path: str, *, mediainfo=None) -> None:
        self.values.append((path, mediainfo))


def test_watchdog_media_handler_records_create_move_and_delete_without_modified(monkeypatch) -> None:
    import monitor_service

    monkeypatch.setattr(monitor_service.logger, "trace", lambda *_args, **_kwargs: None, raising=False)

    ingress = _Ingress()
    install_persistent_ingestion_ingress(ingress)
    handler = monitor_service.MediaFileHandler(["mkv", ".strm"])

    handler.on_created(FileCreatedEvent("/fixture/library/movie.mkv"))
    handler.on_modified(FileModifiedEvent("/fixture/library/movie.mkv"))
    handler.on_moved(FileMovedEvent("/fixture/library/old.mkv", "/fixture/library/new.mkv"))
    handler.on_deleted(FileDeletedEvent("/fixture/library/new.mkv"))

    assert ingress.events == [
        {
            "event_type": "created",
            "path": "/fixture/library/movie.mkv",
            "related_path": None,
            "source": "watchdog",
            "source_event_key": None,
        },
        {
            "event_type": "moved",
            "path": "/fixture/library/new.mkv",
            "related_path": "/fixture/library/old.mkv",
            "source": "watchdog",
            "source_event_key": None,
        },
        {
            "event_type": "deleted",
            "path": "/fixture/library/new.mkv",
            "related_path": None,
            "source": "watchdog",
            "source_event_key": None,
        },
    ]


def test_active_enqueue_uses_durable_ingress_when_installed() -> None:
    from monitor_service import enqueue_file_actively

    ingress = _Ingress()
    install_persistent_ingestion_ingress(ingress)

    enqueue_file_actively("/fixture/library/catch-up.mkv")

    assert ingress.events == [
        {
            "event_type": "created",
            "path": "/fixture/library/catch-up.mkv",
            "related_path": None,
            "source": "compatibility-active",
            "source_event_key": None,
        }
    ]


def test_emby_binding_enqueue_uses_durable_repository_gateway(tmp_path) -> None:
    from monitor_service import enqueue_emby_binding

    media_path = tmp_path / "fixture.mkv"
    media_path.write_bytes(b"fixture")
    binding = _Binding()
    install_persistent_ingestion_binding(binding)

    enqueue_emby_binding(str(media_path), mediainfo={"MediaStreams": []})

    assert binding.values == [(str(media_path), {"MediaStreams": []})]


def test_greenfield_roles_fail_closed_when_durable_gateways_are_missing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import monitor_service

    media_path = tmp_path / "fixture.mkv"
    media_path.write_bytes(b"fixture")
    monkeypatch.setenv("ETK_PROCESS_ROLE", "worker")
    with pytest.raises(RuntimeError, match="binding gateway is unavailable"):
        monitor_service.enqueue_emby_binding(str(media_path))

    monkeypatch.setenv("ETK_PROCESS_ROLE", "coordinator")
    monkeypatch.setattr(monitor_service.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    handler = monitor_service.MediaFileHandler([".mkv"])
    with pytest.raises(RuntimeError, match="ingestion ingress is unavailable"):
        handler.on_created(FileCreatedEvent(str(media_path)))

    assert monitor_service.EMBY_BIND_QUEUE == set()
    assert monitor_service.FILE_EVENT_QUEUE == set()
