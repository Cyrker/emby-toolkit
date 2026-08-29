from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import Any

import pytest

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.p115.fingerprints import (
    P115FileFingerprint,
    P115FingerprintConflict,
    P115FingerprintHints,
    P115FingerprintRepairRow,
    P115FingerprintRepairService,
)
from etk.platform.tasks import TaskOutcomeStatus

SHA_A = "A" * 40
SHA_B = "B" * 40


class FakeCredentials:
    def __init__(self, account_ids: Sequence[str] = ("115001",)) -> None:
        self.account_ids = list(account_ids)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self.account_ids) - 1)
        self.calls += 1
        return P115CredentialSnapshot(account_id=self.account_ids[index])

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("fingerprint repair must not mutate credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("fingerprint repair must not mutate credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


class FakeProgress:
    def __init__(self, *, cancel_after: int | None = None) -> None:
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0
        self.cancel_after = cancel_after

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.cancel_after is not None and self.checkpoints >= self.cancel_after:
            raise RuntimeError("cancelled-by-fixture")


class FakeHints:
    def __init__(self, values: Mapping[str, P115FingerprintHints] | None = None) -> None:
        self.values = dict(values or {})
        self.calls: list[tuple[str, str]] = []

    def inspect(self, path: str, *, local_root: str) -> P115FingerprintHints:
        self.calls.append((path, local_root))
        return self.values.get(path, P115FingerprintHints())


class FakeRemoteClient:
    backend = "openapi"

    def __init__(self, values: Mapping[str, P115FileFingerprint]) -> None:
        self.values = dict(values)
        self.calls: list[str] = []

    def lookup(self, pick_code: str) -> P115FileFingerprint | None:
        self.calls.append(pick_code)
        return self.values.get(pick_code)


class ConflictThenRemoteClient(FakeRemoteClient):
    def lookup(self, pick_code: str) -> P115FileFingerprint | None:
        self.calls.append(pick_code)
        if len(self.calls) == 1:
            raise P115FingerprintConflict("fixture provider identity conflict")
        return self.values.get(pick_code)


class FakeRemoteGateway:
    def __init__(self, client: FakeRemoteClient | None) -> None:
        self.client = client
        self.calls: list[str] = []

    def open(self, *, expected_account_id: str) -> FakeRemoteClient | None:
        self.calls.append(expected_account_id)
        return self.client


class FakeRepository:
    def __init__(
        self,
        rows: Sequence[P115FingerprintRepairRow],
        *,
        cached: Mapping[str, P115FileFingerprint] | None = None,
    ) -> None:
        self.rows = tuple(rows)
        self.cached = dict(cached or {})
        self.lookups: list[dict[str, Any]] = []
        self.commits: list[tuple[P115FingerprintRepairRow, int, P115FileFingerprint, str]] = []

    def list_repair_rows(self) -> tuple[P115FingerprintRepairRow, ...]:
        return self.rows

    def find_fingerprint(
        self,
        *,
        account_id: str,
        pick_code: str = "",
        sha1: str = "",
        file_id: str = "",
        local_paths: Sequence[str] = (),
    ) -> P115FileFingerprint | None:
        query = {
            "account_id": account_id,
            "pick_code": pick_code,
            "sha1": sha1,
            "file_id": file_id,
            "local_paths": tuple(local_paths),
        }
        self.lookups.append(query)
        for key in (pick_code, sha1, file_id, *local_paths):
            if key and key in self.cached:
                return self.cached[key]
        return None

    def commit_repair(
        self,
        row: P115FingerprintRepairRow,
        asset_index: int,
        fingerprint: P115FileFingerprint,
        *,
        account_id: str,
    ) -> bool:
        self.commits.append((row, asset_index, fingerprint, account_id))
        return True


def _row(
    *,
    path: str = "/library/Movie.strm",
    sha1s: Sequence[str] = ("",),
    pick_codes: Sequence[str] = ("",),
) -> P115FingerprintRepairRow:
    return P115FingerprintRepairRow.create(
        tmdb_id="101",
        item_type="Movie",
        title="Fixture Movie",
        assets=({"path": path, "size": 1024},),
        sha1s=sha1s,
        pick_codes=pick_codes,
        revision="1" * 64,
    )


def _fingerprint(
    *,
    sha1: str = SHA_A,
    pick_code: str = "pc-a",
    account_id: str = "115001",
) -> P115FileFingerprint:
    return P115FileFingerprint(
        file_id="9001",
        parent_id="8001",
        name="Movie.mkv",
        sha1=sha1,
        pick_code=pick_code,
        size=1024,
        local_path="Movies/Movie.mkv",
        account_id=account_id,
    )


def test_repair_uses_account_bound_cache_before_remote_and_commits_aligned_slot() -> None:
    row = _row()
    cached = _fingerprint()
    repository = FakeRepository((row,), cached={"Movies/Movie.mkv": cached})
    hints = FakeHints(
        {
            "/library/Movie.strm": P115FingerprintHints(
                local_paths=("Movies/Movie.mkv",),
            )
        }
    )
    remote = FakeRemoteClient({})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=hints,
        remote=FakeRemoteGateway(remote),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["fixed_assets"] == 1
    assert outcome.details["cache_fixed_assets"] == 1
    assert outcome.details["remote_fixed_assets"] == 0
    assert remote.calls == []
    assert len(repository.commits) == 1
    committed_row, index, committed, account_id = repository.commits[0]
    assert committed_row is row
    assert index == 0
    assert committed == cached
    assert account_id == "115001"
    assert repository.lookups[0]["account_id"] == "115001"


def test_repair_uses_hints_then_remote_last_and_rejects_account_drift() -> None:
    row = _row()
    remote_value = _fingerprint(sha1=SHA_B, pick_code="pc-b")
    repository = FakeRepository((row,))
    remote = FakeRemoteClient({"pc-b": remote_value})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(("115001", "115001", "115002")),
        repository=repository,
        hints=FakeHints(
            {
                "/library/Movie.strm": P115FingerprintHints(
                    pick_code="pc-b",
                    local_paths=("Movies/Movie.mkv",),
                )
            }
        ),
        remote=FakeRemoteGateway(remote),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["failed_assets"] == 1
    assert outcome.details["account_drift"] is True
    assert remote.calls == ["pc-b"]
    assert repository.commits == []


def test_repair_skips_virtual_strm_and_non_video_assets_without_remote_access() -> None:
    virtual = _row(path="https://fixture.invalid/api/p115/virtual-play/1/" + SHA_A + "/Movie.mkv")
    non_video = P115FingerprintRepairRow.create(
        tmdb_id="102",
        item_type="Episode",
        title="Fixture Episode",
        assets=({"path": "/library/subtitle.srt"},),
        sha1s=("",),
        pick_codes=("",),
        revision="2" * 64,
    )
    repository = FakeRepository((virtual, non_video))
    remote = FakeRemoteClient({})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=FakeHints(),
        remote=FakeRemoteGateway(remote),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["scanned_assets"] == 0
    assert outcome.details["skipped_virtual"] == 1
    assert outcome.details["skipped_non_video"] == 1
    assert repository.lookups == []
    assert repository.commits == []
    assert remote.calls == []


def test_repair_is_idempotent_when_complete_slots_match_complete_cache() -> None:
    row = _row(sha1s=(SHA_A,), pick_codes=("pc-a",))
    cached = _fingerprint()
    repository = FakeRepository((row,), cached={"pc-a": cached})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=FakeHints(),
        remote=FakeRemoteGateway(FakeRemoteClient({})),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["missing_assets"] == 0
    assert outcome.details["fixed_assets"] == 0
    assert outcome.details["verified_assets"] == 1
    assert repository.commits == []


def test_repair_seeds_provider_cache_when_complete_slot_has_only_remote_identity() -> None:
    row = _row(sha1s=(SHA_A,), pick_codes=("pc-a",))
    remote_value = _fingerprint()
    repository = FakeRepository((row,))
    remote = FakeRemoteClient({"pc-a": remote_value})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=FakeHints(),
        remote=FakeRemoteGateway(remote),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.SUCCEEDED
    assert outcome.details["fixed_assets"] == 0
    assert outcome.details["verified_assets"] == 1
    assert outcome.details["cache_seeded_assets"] == 1
    assert outcome.details["cache_updates"] == 1
    assert remote.calls == ["pc-a"]
    assert repository.commits == [(row, 0, remote_value, "115001")]


def test_repair_fails_closed_on_conflicting_cache_identity() -> None:
    row = _row(sha1s=(SHA_A,), pick_codes=("",))
    repository = FakeRepository((row,), cached={SHA_A: _fingerprint(sha1=SHA_B)})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=FakeHints(),
        remote=FakeRemoteGateway(FakeRemoteClient({})),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.FAILED
    assert outcome.details["failed_assets"] == 1
    assert repository.commits == []


def test_repair_continues_after_one_remote_identity_conflict() -> None:
    first = _row(pick_codes=("pc-a",))
    second = replace(_row(pick_codes=("pc-b",)), tmdb_id="102")
    remote_value = _fingerprint(pick_code="pc-b")
    repository = FakeRepository((first, second))
    remote = ConflictThenRemoteClient({"pc-b": remote_value})
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=repository,
        hints=FakeHints(),
        remote=FakeRemoteGateway(remote),
        local_root=lambda: "/library",
    )

    outcome = service.execute(FakeProgress())

    assert outcome.status is TaskOutcomeStatus.PARTIAL
    assert outcome.details["failed_assets"] == 1
    assert outcome.details["fixed_assets"] == 1
    assert remote.calls == ["pc-a", "pc-b"]
    assert repository.commits == [(second, 0, remote_value, "115001")]


def test_repair_row_rejects_more_fingerprint_slots_than_assets() -> None:
    with pytest.raises(ValueError, match="position-aligned"):
        replace(_row(), sha1s=(SHA_A, SHA_B))


def test_repair_propagates_cooperative_cancellation_checkpoint() -> None:
    service = P115FingerprintRepairService(
        credentials=FakeCredentials(),
        repository=FakeRepository((_row(),)),
        hints=FakeHints(),
        remote=FakeRemoteGateway(FakeRemoteClient({})),
        local_root=lambda: "/library",
    )

    with pytest.raises(RuntimeError, match="cancelled-by-fixture"):
        service.execute(FakeProgress(cancel_after=1))
