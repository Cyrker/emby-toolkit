from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pytest

import etk.domains.p115.directory_sync as directory_sync_module
from etk.domains.p115 import (
    P115CredentialSnapshot,
    P115DirectoryCacheEntry,
    P115DirectoryReconcileResult,
    P115DirectoryRoot,
    P115DirectorySyncService,
)
from etk.platform.jobs import PersistentExecutionCancelled
from etk.platform.tasks import TaskOutcomeStatus


class FakeSettings:
    def __init__(self, rules: object, account_id: str = "115001") -> None:
        self.rules = rules
        self.account_id = account_id
        self.calls: list[str] = []

    def load(self, key: str) -> object:
        self.calls.append(key)
        if key == "p115_sorting_rules_account_id":
            return self.account_id
        return self.rules


class FakeCredentials:
    def __init__(self, account_ids: Sequence[str] = ("115001",)) -> None:
        self.account_ids = list(account_ids)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self.account_ids) - 1)
        self.calls += 1
        return P115CredentialSnapshot(account_id=self.account_ids[index])

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory sync must not save credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("directory sync must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeClient:
    def __init__(self, responses: Mapping[tuple[str, int], object]) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, int, int]] = []

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        self.calls.append((cid, limit, offset))
        response = self.responses[(cid, offset)]
        if isinstance(response, Exception):
            raise response
        return response  # type: ignore[return-value]


class FakeGateway:
    def __init__(self, client: FakeClient | None) -> None:
        self.client = client
        self.calls: list[str] = []

    def open(self, *, expected_account_id: str) -> FakeClient | None:
        self.calls.append(expected_account_id)
        return self.client


class FakeRepository:
    def __init__(self, cleaned_by_root: Mapping[str, int] | None = None) -> None:
        self.cleaned_by_root = dict(cleaned_by_root or {})
        self.calls: list[tuple[P115DirectoryRoot, tuple[P115DirectoryCacheEntry, ...], str]] = []

    def reconcile_children(
        self,
        root: P115DirectoryRoot,
        entries: Sequence[P115DirectoryCacheEntry],
        *,
        account_id: str,
    ) -> P115DirectoryReconcileResult:
        frozen = tuple(entries)
        self.calls.append((root, frozen, account_id))
        return P115DirectoryReconcileResult(len(frozen), self.cleaned_by_root.get(root.cid, 0))


class FakeProgress:
    def __init__(self) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1


class CancellingProgress(FakeProgress):
    def checkpoint(self) -> None:
        super().checkpoint()
        if self.checkpoints == 2:
            raise PersistentExecutionCancelled("fixture cancellation")


def _service(
    *,
    rules: object,
    responses: Mapping[tuple[str, int], object],
    account_ids: Sequence[str] = ("115001",),
    cleaned_by_root: Mapping[str, int] | None = None,
) -> tuple[P115DirectorySyncService, FakeClient, FakeGateway, FakeRepository, FakeProgress]:
    client = FakeClient(responses)
    gateway = FakeGateway(client)
    repository = FakeRepository(cleaned_by_root)
    progress = FakeProgress()
    service = P115DirectorySyncService(
        settings=FakeSettings(rules),
        credentials=FakeCredentials(account_ids),
        directories=gateway,
        repository=repository,
    )
    return service, client, gateway, repository, progress


def test_directory_sync_reads_complete_pages_then_atomically_reconciles_one_root() -> None:
    first_page = [{"fid": f"dir-{index}", "fn": f"Folder {index}", "fc": 0} for index in range(1000)]
    service, client, gateway, repository, progress = _service(
        rules=[{"enabled": True, "cid": "root-a", "category_path": "媒体/电影"}],
        responses={
            ("root-a", 0): {"state": True, "data": first_page, "count": 1002},
            ("root-a", 1000): {
                "state": True,
                "count": "1002",
                "data": [
                    {"file_id": "dir-final", "file_name": "Final", "type": "0"},
                    {"fid": "video", "fn": "movie.mkv", "fc": 1},
                ],
            },
            ("root-a", 1002): {"state": True, "data": [], "count": 1002},
        },
        cleaned_by_root={"root-a": 2},
    )

    outcome = service.execute(progress)

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details == {
        "roots_total": 1,
        "roots_completed": 1,
        "roots_failed": 0,
        "cached": 1001,
        "cleaned": 2,
        "errors": [],
        "account_bound": True,
        "authoritative_cleanup_only": True,
    }
    assert gateway.calls == ["115001"]
    assert client.calls == [
        ("root-a", 1000, 0),
        ("root-a", 1000, 1000),
        ("root-a", 1000, 1002),
    ]
    root, entries, account_id = repository.calls[0]
    assert root == P115DirectoryRoot("root-a", "媒体/电影")
    assert account_id == "115001"
    assert entries[-1] == P115DirectoryCacheEntry(
        id="dir-final",
        parent_id="root-a",
        name="Final",
        local_path="媒体/电影/Final",
    )
    assert progress.updates[-1][0] == 100


def test_repeated_full_page_fails_closed_without_reconciling() -> None:
    repeated_page = [{"fid": f"dir-{index}", "fn": f"Folder {index}", "fc": 0} for index in range(1000)]
    service, client, _gateway, repository, progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={
            ("root-a", 0): {"state": True, "data": repeated_page},
            ("root-a", 1000): {"state": True, "data": repeated_page},
        },
    )

    outcome = service.execute(progress)

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert repository.calls == []
    assert client.calls == [("root-a", 1000, 0), ("root-a", 1000, 1000)]
    assert "重复返回" in outcome.details["errors"][0]["error"]


def test_changed_reported_count_fails_closed() -> None:
    full_page = [{"fid": f"dir-{index}", "fn": f"Folder {index}", "fc": 0} for index in range(1000)]
    changed_count, _client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={
            ("root-a", 0): {"state": True, "data": full_page, "count": 1001},
            ("root-a", 1000): {
                "state": True,
                "data": [{"fid": "final", "fn": "Final", "fc": 0}],
                "count": 1002,
            },
        },
    )
    outcome = changed_count.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "总数发生变化" in outcome.details["errors"][0]["error"]
    assert repository.calls == []


def test_reported_count_must_match_before_an_explicit_empty_page_is_authoritative() -> None:
    service, _client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={
            ("root-a", 0): {
                "state": True,
                "data": [{"fid": "only", "fn": "Only", "fc": 0}],
                "count": 2,
            },
            ("root-a", 1): {"state": True, "data": [], "count": 2},
        },
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "数据不完整" in outcome.details["errors"][0]["error"]
    assert repository.calls == []


def test_short_pages_continue_until_an_explicit_authoritative_empty_page() -> None:
    service, client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={
            ("root-a", 0): {
                "state": True,
                "data": [{"fid": "first", "fn": "First", "fc": 0}],
                "count": 2,
            },
            ("root-a", 1): {
                "state": True,
                "data": [{"fid": "second", "fn": "Second", "fc": 0}],
                "count": 2,
            },
            ("root-a", 2): {"state": True, "data": [], "count": 2},
        },
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert client.calls == [
        ("root-a", 1000, 0),
        ("root-a", 1000, 1),
        ("root-a", 1000, 2),
    ]
    assert [entry.id for _root, entries, _account_id in repository.calls for entry in entries] == [
        "first",
        "second",
    ]


def test_page_limit_is_a_fail_closed_scan_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(directory_sync_module, "_MAX_PAGES", 1)
    full_page = [{"fid": f"dir-{index}", "fn": f"Folder {index}", "fc": 0} for index in range(1000)]
    service, client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={
            ("root-a", 0): {"state": True, "data": full_page},
            ("root-a", 1000): {
                "state": True,
                "data": [{"fid": "overflow", "fn": "Overflow", "fc": 0}],
            },
        },
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert "超过安全上限 1 个非空页" in outcome.details["errors"][0]["error"]
    assert client.calls == [("root-a", 1000, 0), ("root-a", 1000, 1000)]
    assert repository.calls == []


def test_invalid_or_incomplete_provider_page_never_reconciles_that_root() -> None:
    service, _client, _gateway, repository, progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={("root-a", 0): {"state": True, "data": None}},
    )

    outcome = service.execute(progress)

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert repository.calls == []
    assert outcome.details["roots_completed"] == 0
    assert outcome.details["roots_failed"] == 1
    assert progress.updates[-1][0] == -1


def test_partial_sync_reconciles_only_the_authoritatively_completed_root() -> None:
    service, _client, _gateway, repository, _progress = _service(
        rules=[
            {"cid": "root-a", "name": "Movies"},
            {"cid": "root-b", "name": "Series"},
        ],
        responses={
            ("root-a", 0): {"state": True, "data": []},
            ("root-b", 0): TimeoutError("fixture timeout"),
        },
        cleaned_by_root={"root-a": 3},
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert [root.cid for root, _entries, _account_id in repository.calls] == ["root-a"]
    assert outcome.details["cleaned"] == 3
    assert outcome.details["errors"] == [{"cid": "root-b", "name": "Series", "error": "fixture timeout"}]


def test_account_change_after_listing_fails_closed_before_cache_write() -> None:
    service, _client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={("root-a", 0): {"state": True, "data": []}},
        account_ids=("115001", "115001", "115002"),
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert repository.calls == []
    assert "账号在目录同步期间发生变化" in outcome.details["errors"][0]["error"]


def test_unbound_or_different_account_rules_fail_closed_before_opening_provider() -> None:
    for rules_account_id in ("", "115002"):
        gateway = FakeGateway(None)
        repository = FakeRepository()
        outcome = P115DirectorySyncService(
            settings=FakeSettings([{"cid": "root-a"}], rules_account_id),
            credentials=FakeCredentials(("115001",)),
            directories=gateway,
            repository=repository,
        ).execute(FakeProgress())

        assert outcome.status is TaskOutcomeStatus.FAILED
        assert "未绑定当前账号" in outcome.message
        assert gateway.calls == []
        assert repository.calls == []


def test_missing_rules_or_stable_account_skips_without_opening_provider() -> None:
    no_rules = P115DirectorySyncService(
        settings=FakeSettings(None),
        credentials=FakeCredentials(),
        directories=FakeGateway(None),
        repository=FakeRepository(),
    ).execute(FakeProgress())
    no_account_gateway = FakeGateway(None)
    no_account = P115DirectorySyncService(
        settings=FakeSettings([{"cid": "root-a"}]),
        credentials=FakeCredentials(("",)),
        directories=no_account_gateway,
        repository=FakeRepository(),
    ).execute(FakeProgress())

    assert no_rules.status is TaskOutcomeStatus.SKIPPED
    assert no_account.status is TaskOutcomeStatus.SKIPPED
    assert no_account_gateway.calls == []


def test_persistent_cancellation_is_not_downgraded_to_a_root_failure() -> None:
    service, client, _gateway, repository, _progress = _service(
        rules=[{"cid": "root-a", "name": "Movies"}],
        responses={("root-a", 0): {"state": True, "data": []}},
    )

    with pytest.raises(PersistentExecutionCancelled, match="fixture cancellation"):
        service.execute(CancellingProgress())

    assert client.calls == []
    assert repository.calls == []
