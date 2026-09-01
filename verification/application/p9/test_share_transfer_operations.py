from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.sharing.operations import SharingOperation
from etk.domains.sharing.share_transfer import (
    LogicalSeasonShareTransferOperationDelivery,
    ShareImportOutcome,
    ShareTarget,
    ShareTransferReadback,
)
from etk.domains.sharing.virtual_import import VirtualImportBundle, freeze_virtual_import_source
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import Settings

_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000271")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000272")


def _source() -> dict[str, Any]:
    return {
        "source_kind": "logical_season",
        "source_id": "logical-season-fixture",
        "item_type": "Episode",
        "title": "Fixture Series",
        "release_year": 2026,
        "parent_series_tmdb_id": "42",
        "season_number": 2,
        "files": [
            {
                "source_kind": "logical_episode",
                "source_id": "logical-episode-1",
                "sha1": "A" * 40,
                "size": 1024,
                "file_name": "Fixture.S02E01.mkv",
                "season_number": 2,
                "episode_number": 1,
            }
        ],
    }


def _operation(*, state: Mapping[str, Any] | None = None) -> SharingOperation:
    intent = freeze_virtual_import_source(_source())
    return cast(
        SharingOperation,
        SimpleNamespace(
            id=_OPERATION_ID,
            claim_token=_CLAIM_TOKEN,
            state=dict(state or {}),
            request={
                "provider": "p115",
                "action": "transfer_logical_season_share",
                "payload": {
                    "source_kind": intent.source_kind,
                    "source_id": intent.source_id,
                    "source": intent.source,
                    "source_state_hash": intent.state_hash,
                    "mode": "share",
                    "expected_channel_id": "channel-fixture",
                },
            },
        ),
    )


class Credentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(cookie="UID=fixture", app_type="web", account_id="115001")


class VirtualImports:
    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        file = dict(cast(list[Mapping[str, Any]], source["files"])[0])
        return VirtualImportBundle("logical_season", "logical-season-fixture", dict(source), (file,))


class Repository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.states: list[dict[str, Any]] = []

    def save_prewrite_state(self, operation_id: UUID, claim_token: UUID, state: Mapping[str, Any]) -> None:
        assert (operation_id, claim_token) == (_OPERATION_ID, _CLAIM_TOKEN)
        self.events.append("prewrite")
        self.states.append(dict(state))

    def save_request_state(self, operation_id: UUID, claim_token: UUID, state: Mapping[str, Any]) -> None:
        assert (operation_id, claim_token) == (_OPERATION_ID, _CLAIM_TOKEN)
        self.events.append(f"state:{state['phase']}")
        self.states.append(dict(state))


class Center:
    def __init__(self, events: list[str], *, manifest: Mapping[str, Any] | None = None) -> None:
        self.events = events
        self.manifest = (
            dict(manifest) if manifest is not None else {"item": {"files": [{"sha1": "A" * 40, "size": 1024}]}}
        )

    def logical_season_manifest(self, group_id: str) -> ProviderResponse[dict[str, Any]]:
        self.events.append("manifest")
        assert group_id == "logical-season-fixture"
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            self.manifest,
        )

    def get_logical_season_share_channel(self, group_id: str) -> ProviderResponse[dict[str, Any]]:
        self.events.append("channel")
        assert group_id == "logical-season-fixture"
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {
                "item": {
                    "channel_id": "channel-fixture",
                    "status": "valid",
                    "share_code": "provider-share-secret",
                    "receive_code": "provider-receive-secret",
                }
            },
        )


class P115:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.imports = 0

    def find_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        self.events.append("find-target")
        assert identity["folder_name"] == "Fixture Series (2026) {tmdb=42} - Season 02"
        return ShareTarget("missing")

    def create_target(self, identity: Mapping[str, Any]) -> ShareTarget:
        self.events.append("create-target")
        assert identity["base_target_cid"] == "target-fixture"
        return ShareTarget("found", "season-folder-fixture")

    def import_share(
        self,
        identity: Mapping[str, Any],
        *,
        share_code: str,
        receive_code: str,
    ) -> ShareImportOutcome:
        self.events.append("share-import")
        self.imports += 1
        assert identity["target_folder_cid"] == "season-folder-fixture"
        assert (share_code, receive_code) == ("provider-share-secret", "provider-receive-secret")
        return ShareImportOutcome("confirmed")

    def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback:
        self.events.append("readback")
        file = cast(list[Mapping[str, Any]], identity["files"])[0]
        return ShareTransferReadback(
            "confirmed",
            receipts=(
                {
                    "fid": "remote-file-1",
                    "sha1": file["sha1"],
                    "size": file["size"],
                    "target_cid": identity["target_folder_cid"],
                },
            ),
        )


def _delivery(
    events: list[str],
    repository: Repository,
    p115: P115,
    *,
    center: Center | None = None,
) -> LogicalSeasonShareTransferOperationDelivery:
    return LogicalSeasonShareTransferOperationDelivery(
        repository=cast(Any, repository),
        virtual_imports=cast(Any, VirtualImports()),
        credentials=cast(Any, Credentials()),
        settings=lambda: Settings(p115_save_path_cid="target-fixture"),
        center=cast(Any, center or Center(events)),
        p115=p115,
    )


def test_logical_season_share_uses_one_checkpoint_and_keeps_provider_codes_ephemeral() -> None:
    events: list[str] = []
    repository = Repository(events)
    p115 = P115(events)

    outcome = _delivery(events, repository, p115).request(
        _operation(),
        checkpoint=lambda: events.append("checkpoint"),
    )

    assert outcome.disposition.value == "confirmed"
    assert events.count("checkpoint") == 1
    assert events.index("checkpoint") < events.index("create-target") < events.index("share-import")
    assert p115.imports == 1
    assert outcome.result is not None
    assert outcome.result["transfer_mode"] == "share"
    assert outcome.result["share_channel_id"] == "channel-fixture"
    assert outcome.result["success_count"] == 1
    durable = json.dumps({"states": repository.states, "result": outcome.result}, ensure_ascii=False)
    assert "provider-share-secret" not in durable
    assert "provider-receive-secret" not in durable


def test_logical_season_share_recovery_only_reads_the_persisted_target() -> None:
    events: list[str] = []
    repository = Repository(events)
    p115 = P115(events)
    source = _source()
    state = {
        "schema_version": 1,
        "kind": "logical_season_share_transfer",
        "account_id": "115001",
        "backend": "cookie",
        "credential_hash": hashlib.sha256(b"cookie\0web\0UID=fixture").hexdigest(),
        "base_target_cid": "target-fixture",
        "target_folder_cid": "season-folder-fixture",
        "folder_name": "Fixture Series (2026) {tmdb=42} - Season 02",
        "source_kind": "logical_season",
        "source_id": "logical-season-fixture",
        "source_state_hash": "d" * 64,
        "channel_id": "channel-fixture",
        "files": [dict(cast(list[Mapping[str, Any]], source["files"])[0])],
        "files_manifest_hash": "e" * 64,
        "phase": "awaiting_readback",
    }

    outcome = _delivery(events, repository, p115).readback(_operation(state=state))

    assert outcome.disposition.value == "confirmed"
    assert events == ["readback"]
    assert p115.imports == 0


def test_logical_season_share_rejects_empty_manifest_before_provider_checkpoint() -> None:
    events: list[str] = []
    repository = Repository(events)
    p115 = P115(events)

    with pytest.raises(ValueError, match="manifest is empty or invalid"):
        _delivery(
            events,
            repository,
            p115,
            center=Center(events, manifest={"item": {"files": []}}),
        ).request(_operation(), checkpoint=lambda: events.append("checkpoint"))

    assert events == ["manifest"]
    assert p115.imports == 0


def test_logical_season_share_rejects_ambiguous_existing_files_without_import() -> None:
    events: list[str] = []
    repository = Repository(events)

    class AmbiguousP115(P115):
        def find_target(self, identity: Mapping[str, Any]) -> ShareTarget:
            self.events.append("find-target")
            return ShareTarget("found", "season-folder-fixture")

        def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback:
            self.events.append("readback")
            return ShareTransferReadback("ambiguous", ambiguous_count=2)

    p115 = AmbiguousP115(events)
    outcome = _delivery(events, repository, p115).request(
        _operation(),
        checkpoint=lambda: events.append("checkpoint"),
    )

    assert outcome.disposition.value == "rejected"
    assert outcome.error == {"type": "share_target_files_ambiguous"}
    assert "checkpoint" not in events
    assert p115.imports == 0


@pytest.mark.parametrize(
    ("readback", "expected_disposition", "expected_imports"),
    [
        (
            ShareTransferReadback(
                "partial",
                receipts=({"fid": "unexpected-existing-file"},),
                missing_count=1,
            ),
            "rejected",
            0,
        ),
        (ShareTransferReadback("partial", missing_count=1), "confirmed", 1),
    ],
)
def test_logical_season_share_only_imports_into_an_authoritatively_empty_target(
    readback: ShareTransferReadback,
    expected_disposition: str,
    expected_imports: int,
) -> None:
    events: list[str] = []
    repository = Repository(events)

    class ExistingP115(P115):
        def find_target(self, identity: Mapping[str, Any]) -> ShareTarget:
            self.events.append("find-target")
            return ShareTarget("found", "season-folder-fixture")

        def readback(self, identity: Mapping[str, Any]) -> ShareTransferReadback:
            self.events.append("readback")
            return readback if self.imports == 0 else super().readback(identity)

    p115 = ExistingP115(events)
    outcome = _delivery(events, repository, p115).request(
        _operation(),
        checkpoint=lambda: events.append("checkpoint"),
    )

    assert outcome.disposition.value == expected_disposition
    assert p115.imports == expected_imports
    assert ("checkpoint" in events) is (expected_imports == 1)


def test_logical_season_share_lost_import_response_recovers_without_resend() -> None:
    events: list[str] = []
    repository = Repository(events)

    class LostResponseP115(P115):
        def import_share(
            self,
            identity: Mapping[str, Any],
            *,
            share_code: str,
            receive_code: str,
        ) -> ShareImportOutcome:
            super().import_share(
                identity,
                share_code=share_code,
                receive_code=receive_code,
            )
            raise TimeoutError("fixture response lost")

    p115 = LostResponseP115(events)
    delivery = _delivery(events, repository, p115)
    with pytest.raises(TimeoutError, match="response lost"):
        delivery.request(_operation(), checkpoint=lambda: events.append("checkpoint"))

    assert repository.states[-1]["phase"] == "import_requesting"
    recovered = delivery.readback(_operation(state=repository.states[-1]))

    assert recovered.disposition.value == "confirmed"
    assert p115.imports == 1
