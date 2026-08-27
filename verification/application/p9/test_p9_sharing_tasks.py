from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import pytest

from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform.tasks import ProcessorIdentity, TaskMessage, TaskOutcomeStatus, TaskRegistry
from etk.platform.tasks.catalog import P9_NATIVE_TASK_KEYS
from etk.processes.p9_sharing import P9SharingTaskRouter

REPO_ROOT = Path(__file__).resolve().parents[3]
TASK_FIXTURE = REPO_ROOT / "verification/fixtures/tasks/ordered-task-catalog.json"


class FakeProgress:
    def __init__(self) -> None:
        self.checkpoints = 0
        self.updates: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        self.checkpoints += 1

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))


class FakeSharedCenter:
    def __init__(self, response: ProviderResponse[Any] | None = None) -> None:
        self.response = response or ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"ok": True})
        self.calls: list[tuple[str, Any]] = []

    def run_maintenance(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("maintenance", dict(payload)))
        return self.response

    def register_source(self, source_kind: str, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("register_source", (source_kind, dict(payload))))
        return self.response

    def share_sync_heartbeat(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("heartbeat", dict(payload)))
        return self.response

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("lease", dict(payload)))
        return self.response

    def report_transfer(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("report", dict(payload)))
        return self.response

    def quote_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("quote", dict(payload)))
        return self.response

    def list_share_requests(self, **query: Any) -> ProviderResponse[Any]:
        self.calls.append(("list_requests", dict(query)))
        return self.response

    def create_share_request(self, payload: Mapping[str, Any]) -> ProviderResponse[Any]:
        self.calls.append(("create_request", dict(payload)))
        return self.response

    def register_device(self, *, name: str, server_id_hash: str) -> ProviderResponse[Any]:
        self.calls.append(("register_device", (name, server_id_hash)))
        return self.response

    def device_status(self) -> ProviderResponse[Any]:
        self.calls.append(("device_status", {}))
        return self.response


class FakeHDHive:
    def __init__(self, response: ProviderResponse[Any] | None = None) -> None:
        self.response = response or ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"success": True})
        self.calls: list[bool] = []

    def checkin(self, *, is_gambler: bool = False) -> ProviderResponse[Any]:
        self.calls.append(is_gambler)
        return self.response


def _message(task_key: str, payload: Mapping[str, object] | None = None) -> TaskMessage:
    return TaskMessage.create(task_key=task_key, processor=ProcessorIdentity.MEDIA, payload=payload)


def test_p9_fixture_contains_exactly_four_unique_native_media_tasks() -> None:
    fixture = json.loads(TASK_FIXTURE.read_text(encoding="utf-8"))
    p9 = [item for item in fixture["tasks"] if item["key"] in P9_NATIVE_TASK_KEYS]
    registry = TaskRegistry.from_fixture(TASK_FIXTURE)
    definitions = {item.key: item for item in registry.definitions() if item.key in P9_NATIVE_TASK_KEYS}

    assert {item["key"] for item in p9} == set(P9_NATIVE_TASK_KEYS)
    assert len(p9) == len({item["key"] for item in p9}) == 4
    assert all(item["handler_kind"] == "native" and item["processor"] == "media" for item in p9)
    assert set(definitions) == set(P9_NATIVE_TASK_KEYS)
    assert all(item.processor is ProcessorIdentity.MEDIA for item in definitions.values())


def test_p9_task_router_dispatches_all_provider_paths() -> None:
    center = FakeSharedCenter()
    hdhive = FakeHDHive()
    router = P9SharingTaskRouter(shared_center=cast(Any, center), hdhive=cast(Any, hdhive))

    checkin_progress = FakeProgress()
    checkin = router.execute(_message("hdhive-auto-checkin", {"is_gambler": True}), None, checkin_progress)
    maintenance = router.execute(_message("shared-resource-maintenance", {"limit": 5}), None, FakeProgress())
    registration = router.execute(
        _message(
            "share-all-library",
            {"sources": [{"source_kind": "movie", "tmdb_id": "101"}, {"kind": "episode", "tmdb_id": "202"}]},
        ),
        None,
        FakeProgress(),
    )
    heartbeat = router.execute(_message("shared-share-status-sync", {"source_ids": ["a"]}), None, FakeProgress())

    assert checkin.status is TaskOutcomeStatus.SUCCEEDED
    assert maintenance.status is TaskOutcomeStatus.SUCCEEDED
    assert registration.status is TaskOutcomeStatus.SUCCEEDED
    assert heartbeat.status is TaskOutcomeStatus.SUCCEEDED
    assert hdhive.calls == [True]
    assert [name for name, _payload in center.calls] == [
        "maintenance",
        "register_source",
        "register_source",
        "heartbeat",
    ]
    assert center.calls[2][1][0] == "episode"
    assert checkin_progress.checkpoints >= 1


def test_p9_router_maps_indeterminate_without_legacy_replay() -> None:
    center = FakeSharedCenter(ProviderResponse(ProviderDisposition.INDETERMINATE, 503, None))
    hdhive = FakeHDHive(ProviderResponse(ProviderDisposition.INDETERMINATE, 503, None))
    router = P9SharingTaskRouter(shared_center=cast(Any, center), hdhive=cast(Any, hdhive))

    checkin = router.execute(_message("hdhive-auto-checkin"), None, FakeProgress())
    sync = router.execute(_message("shared-share-status-sync"), None, FakeProgress())

    assert checkin.status is TaskOutcomeStatus.FAILED
    assert checkin.details["provider_status"] == "indeterminate"
    assert checkin.details["retryable"] is False
    assert sync.status is TaskOutcomeStatus.FAILED
    assert sync.details["retryable"] is False
    assert hdhive.calls == [False]
    assert [name for name, _payload in center.calls] == ["heartbeat"]


def test_p9_share_all_indeterminate_does_not_mark_mutation_retryable() -> None:
    center = FakeSharedCenter(ProviderResponse(ProviderDisposition.INDETERMINATE, 503, None))
    router = P9SharingTaskRouter(shared_center=cast(Any, center), hdhive=cast(Any, FakeHDHive()))
    outcome = router.execute(
        _message("share-all-library", {"sources": [{"source_kind": "movie", "tmdb_id": "101"}]}),
        None,
        FakeProgress(),
    )
    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["retryable"] is False


def test_p9_payload_validation_rejects_unknown_or_non_json_fields() -> None:
    router = P9SharingTaskRouter(
        shared_center=cast(Any, FakeSharedCenter()),
        hdhive=cast(Any, FakeHDHive()),
    )

    with pytest.raises(ValueError, match="unsupported parameters"):
        router.execute(_message("hdhive-auto-checkin", {"token": "secret"}), None, FakeProgress())
    with pytest.raises(TypeError, match="sources must be a list"):
        router.execute(_message("share-all-library", {"sources": {"tmdb_id": "1"}}), None, FakeProgress())
    with pytest.raises(ValueError, match="requires the media processor"):
        router.execute(
            TaskMessage.create(task_key="hdhive-auto-checkin", processor=ProcessorIdentity.WATCHLIST),
            None,
            FakeProgress(),
        )
