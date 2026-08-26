from __future__ import annotations

from pathlib import Path
from typing import Any

from etk.platform import Settings
from etk.platform.tasks import TaskOutcomeStatus, TaskRegistry
from etk.processes.p6_ingestion_tasks import P6IngestionScanService, P6IngestionTaskRouter


class _Progress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class _Known:
    def __init__(self, values: set[str] | None = None) -> None:
        self.values = values or set()

    def known_asset_paths(self) -> set[str]:
        return set(self.values)


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


def _write(path: Path, value: bytes = b"fixture") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(value)
    return path


def test_scan_preserves_directory_representative_semantics_and_real_exclude_boundary(tmp_path: Path) -> None:
    root = tmp_path / "library"
    strm_a = _write(root / "a-series" / "episode-01.strm")
    strm_b = _write(root / "a-series" / "episode-02.strm")
    physical_a = _write(root / "b-movie" / "movie-a.mkv")
    _write(root / "b-movie" / "movie-b.mkv")
    _write(root / "excluded" / "ignored.mkv")
    boundary = _write(root / "excluded2" / "included.mkv")
    known = _write(root / "known" / "known.mkv")
    ingress = _Ingress()
    progress = _Progress()
    settings = Settings(
        monitor_enabled=True,
        monitor_paths=[str(root)],
        monitor_extensions=[".mkv", "strm"],
        monitor_exclude_dirs=[str(root / "excluded")],
        monitor_scan_batch_size=2,
        monitor_scan_max_tasks=0,
    )
    service = P6IngestionScanService(
        settings=lambda: settings,
        ingress=ingress,
        known_paths=_Known({str(known).upper()}),
    )

    outcome = service.execute(progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert [event["path"] for event in ingress.events] == [
        str(strm_a),
        str(strm_b),
        str(physical_a),
        str(boundary),
    ]
    assert all(event["event_type"] == "created" for event in ingress.events)
    assert all(event["source"] == "scan" for event in ingress.events)
    assert len({event["source_event_key"] for event in ingress.events}) == 4
    assert outcome.details["submitted"] == 4
    assert outcome.details["triggered_directories"] == 3
    assert outcome.details["skipped_known"] == 1
    assert progress.checkpoints > 4


def test_scan_reports_missing_root_instead_of_false_success(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    ingress = _Ingress()
    service = P6IngestionScanService(
        settings=lambda: Settings(monitor_enabled=True, monitor_paths=[str(missing)]),
        ingress=ingress,
        known_paths=_Known(),
    )

    outcome = service.execute(_Progress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["missing_root_count"] == 1
    assert ingress.events == []


def test_scan_monitor_public_key_is_native_and_keeps_payload_v1_empty() -> None:
    registry = TaskRegistry.from_fixture(
        Path(__file__).resolve().parents[2] / "fixtures/tasks/ordered-task-catalog.json"
    )
    message = registry.create_message("scan-monitor-folders", {}, payload_version=1)
    progress = _Progress()
    expected = P6IngestionScanService(
        settings=lambda: Settings(monitor_enabled=False),
        ingress=_Ingress(),
        known_paths=_Known(),
    )

    outcome = P6IngestionTaskRouter(expected).execute(message, progress)

    assert outcome.status is TaskOutcomeStatus.SKIPPED
    assert outcome.details["task_key"] == "scan-monitor-folders"
    assert outcome.details["compatibility_fallback"] is False
