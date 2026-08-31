from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

import pytest

from etk.domains.p115 import (
    P115CredentialSnapshot,
    P115DirectUrl,
    P115MaintenanceIntentService,
    P115MkdirOperation,
    P115RecycleBinEmptyOperation,
    P115SpeedtestService,
    P115UnrecognizedEmptyOperation,
)
from etk.platform.jobs import PersistentEffectIndeterminate


class FakeCredentials:
    def __init__(self, snapshot: P115CredentialSnapshot) -> None:
        self.snapshot = snapshot

    def load(self) -> P115CredentialSnapshot:
        return self.snapshot

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("maintenance must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("maintenance must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return {"web": "网页端"}.get(app_type, app_type)


class FakeProgress:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    def checkpoint(self) -> None:
        self.events.append(("checkpoint", None))

    def update(self, progress: int, message: str) -> None:
        self.events.append(("update", (progress, message)))

    def record_started_effect(self, label: str) -> None:
        self.events.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.events.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.events.append(("abandoned", label))


class FakeProvider:
    def __init__(self, client: FakeClient | None) -> None:
        self.client = client
        self.opens: list[tuple[str | None, str | None]] = []

    def open(
        self,
        *,
        expected_account_id: str | None = None,
        expected_backend: str | None = None,
    ) -> FakeClient | None:
        self.opens.append((expected_account_id, expected_backend))
        return self.client


class FakeClient:
    def __init__(self, listings: Sequence[Sequence[Mapping[str, Any]]]) -> None:
        self.listings = [tuple(dict(item) for item in listing) for listing in listings]
        self.mkdir_response: Mapping[str, Any] = {"state": True, "cid": "200"}
        self.delete_response: Mapping[str, Any] = {"state": True}
        self.recycle_response: Mapping[str, Any] = {"state": True}
        self.recycle_states: list[bool | None] = []
        self.mkdir_calls: list[tuple[str, str]] = []
        self.delete_calls: list[list[str]] = []
        self.recycle_calls = 0

    def list_children(self, parent_id: str) -> tuple[dict[str, Any], ...]:
        del parent_id
        if not self.listings:
            raise AssertionError("unexpected provider readback")
        return tuple(dict(item) for item in self.listings.pop(0))

    def create_directory(self, parent_id: str, name: str) -> Mapping[str, Any]:
        self.mkdir_calls.append((parent_id, name))
        return dict(self.mkdir_response)

    def delete_files(self, file_ids: list[str]) -> Mapping[str, Any]:
        self.delete_calls.append(list(file_ids))
        return dict(self.delete_response)

    def clear_recycle_bin(self) -> Mapping[str, Any]:
        self.recycle_calls += 1
        return dict(self.recycle_response)

    def recycle_bin_empty(self) -> bool | None:
        if not self.recycle_states:
            raise AssertionError("unexpected recycle-bin readback")
        return self.recycle_states.pop(0)


class RecycleBinReadErrorClient(FakeClient):
    def recycle_bin_empty(self) -> bool | None:
        raise RuntimeError("recycle-bin read transport failed")


class RecycleBinInvalidStateClient(FakeClient):
    def recycle_bin_empty(self) -> bool | None:
        return "invalid"  # type: ignore[return-value]


class FakeFacts:
    def __init__(self) -> None:
        self.cached = frozenset({"cached-gone", "file-1"})
        self.deleted: list[tuple[tuple[str, ...], str, str]] = []
        self.sample: dict[str, Any] | None = None

    def speedtest_sample(self, *, account_id: str) -> dict[str, Any] | None:
        assert account_id == "115001"
        return dict(self.sample) if self.sample is not None else None

    def cached_unrecognized_file_ids(self, *, account_id: str, parent_id: str) -> frozenset[str]:
        assert (account_id, parent_id) == ("115001", "900")
        return self.cached

    def delete_confirmed_unrecognized_facts(
        self,
        file_ids: Sequence[str],
        *,
        account_id: str,
        parent_id: str,
    ) -> int:
        normalized = tuple(sorted(file_ids))
        self.deleted.append((normalized, account_id, parent_id))
        return len(normalized)


def _parameters(**extra: str) -> dict[str, str]:
    return {"account_id": "115001", "backend": "openapi", **extra}


def test_maintenance_intent_freezes_account_and_backend() -> None:
    credentials = FakeCredentials(P115CredentialSnapshot(account_id="115001", access_token="token", cookie="cookie"))
    service = P115MaintenanceIntentService(credentials=credentials, priority=lambda: "cookie")

    assert service.mkdir_parameters("100", "电影") == {
        "account_id": "115001",
        "backend": "cookie",
        "parent_id": "100",
        "name": "电影",
    }
    assert service.recycle_bin_parameters() == {"account_id": "115001", "backend": "openapi"}
    assert service.unrecognized_parameters("900") == {
        "account_id": "115001",
        "backend": "cookie",
        "directory_id": "900",
    }


def test_mkdir_reuses_exact_authoritative_directory_without_write() -> None:
    client = FakeClient([[{"cid": "200", "fn": "电影", "fc": "0"}]])
    progress = FakeProgress()

    result = P115MkdirOperation(FakeProvider(client)).execute(
        _parameters(parent_id="100", name="电影"),
        progress,
    )

    assert result["created"] is False
    assert result["provider_response"] == {"state": True, "cid": "200"}
    assert client.mkdir_calls == []
    assert not [event for event in progress.events if event[0] == "started"]


def test_mkdir_writes_once_and_requires_authoritative_readback() -> None:
    client = FakeClient([[], [{"cid": "200", "fn": "电影", "fc": "0"}]])
    progress = FakeProgress()

    result = P115MkdirOperation(FakeProvider(client)).execute(
        _parameters(parent_id="100", name="电影"),
        progress,
    )

    assert result["created"] is True
    assert client.mkdir_calls == [("100", "电影")]
    assert [event[0] for event in progress.events if event[0] in {"started", "committed"}] == [
        "started",
        "committed",
    ]


def test_mkdir_unknown_readback_stays_indeterminate() -> None:
    client = FakeClient([[], []])
    progress = FakeProgress()

    with pytest.raises(PersistentEffectIndeterminate):
        P115MkdirOperation(FakeProvider(client)).execute(
            _parameters(parent_id="100", name="电影"),
            progress,
        )

    assert client.mkdir_calls == [("100", "电影")]
    assert [event[0] for event in progress.events if event[0] == "committed"] == []


def test_recycle_bin_empty_confirms_absence_after_write() -> None:
    client = FakeClient([])
    client.recycle_states = [False, True]
    progress = FakeProgress()

    result = P115RecycleBinEmptyOperation(FakeProvider(client)).execute(_parameters(), progress)

    assert result["ok"] is True
    assert client.recycle_calls == 1
    assert [event[0] for event in progress.events if event[0] in {"started", "committed"}] == [
        "started",
        "committed",
    ]


@pytest.mark.parametrize(
    "client",
    [
        FakeClient([]),
        RecycleBinReadErrorClient([]),
        RecycleBinInvalidStateClient([]),
    ],
    ids=["provider-rejection", "transport-error", "invalid-response"],
)
def test_recycle_bin_preflight_unknown_state_never_clears(client: FakeClient) -> None:
    if isinstance(client, FakeClient) and not isinstance(
        client, (RecycleBinReadErrorClient, RecycleBinInvalidStateClient)
    ):
        client.recycle_states = [None]
    progress = FakeProgress()

    with pytest.raises(PersistentEffectIndeterminate):
        P115RecycleBinEmptyOperation(FakeProvider(client)).execute(_parameters(), progress)

    assert client.recycle_calls == 0
    assert not [event for event in progress.events if event[0] == "started"]


def test_unrecognized_delete_filters_directories_and_cleans_only_after_absence() -> None:
    client = FakeClient(
        [
            [
                {"fid": "file-1", "fn": "bad.mkv", "fc": "1"},
                {"cid": "dir-1", "fn": "nested", "fc": "0"},
            ],
            [{"cid": "dir-1", "fn": "nested", "fc": "0"}],
        ]
    )
    facts = FakeFacts()
    progress = FakeProgress()

    result = P115UnrecognizedEmptyOperation(provider=FakeProvider(client), facts=facts).execute(
        _parameters(directory_id="900"),
        progress,
    )

    assert client.delete_calls == [["file-1"]]
    assert result == {
        "ok": True,
        "deleted_count": 1,
        "local_deleted": 2,
        "reconciled_count": 1,
        "unrecognized_cid": "900",
    }
    assert facts.deleted == [
        (("cached-gone",), "115001", "900"),
        (("file-1",), "115001", "900"),
    ]


class FakeDirectUrls:
    def resolve(
        self,
        pick_code: str,
        *,
        expected_account_id: str,
        user_agent: str,
    ) -> P115DirectUrl | None:
        assert (pick_code, expected_account_id) == ("pick-code-123456", "115001")
        return P115DirectUrl(
            url="https://download.example.test/file",
            user_agent=user_agent,
            backend="openapi",
            method="fixture",
        )


class FakeDownloads:
    def measure(
        self,
        url: str,
        *,
        user_agent: str,
        max_bytes: int,
        max_seconds: int,
    ) -> dict[str, Any]:
        assert url == "https://download.example.test/file"
        assert user_agent
        assert (max_bytes, max_seconds) == (2 * 1024 * 1024, 5)
        return {"speed_text": "2.00 MB/s", "downloaded_bytes": max_bytes}


def test_speedtest_masks_pick_code_and_preserves_legacy_envelope() -> None:
    credentials = FakeCredentials(
        P115CredentialSnapshot(account_id="115001", access_token="token", cookie="cookie", app_type="web")
    )
    facts = FakeFacts()
    facts.sample = {
        "pick_code": "pick-code-123456",
        "sha1": "A" * 40,
        "file_name": "fixture.mkv",
    }
    service = P115SpeedtestService(
        credentials=credentials,
        facts=facts,
        direct_urls=FakeDirectUrls(),
        downloads=FakeDownloads(),
        clock=lambda: datetime(2026, 8, 31, 9, 0, 0),
    )

    result = service.run(max_mb="2", seconds="5", user_agent="ignored")

    assert result.status_code == 200
    assert isinstance(result.payload, dict)
    assert result.payload["status"] == "success"
    data = result.payload["data"]
    assert data["speed_test"]["sample"]["pick_code_masked"] == "pick****3456"
    assert "pick_code" not in data["speed_test"]["sample"]
    assert data["speed_test"]["host"] == "download.example.test"
