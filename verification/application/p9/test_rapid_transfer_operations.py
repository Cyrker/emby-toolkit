from __future__ import annotations

import hashlib
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.domains.p115 import P115CredentialSnapshot
from etk.domains.sharing.operations import SharingOperation
from etk.domains.sharing.rapid_transfer import (
    NativeP115RapidTransferGateway,
    RapidTransferOperationDelivery,
    RapidTransferReadback,
    RapidUploadOutcome,
)
from etk.domains.sharing.virtual_import import VirtualImportBundle, freeze_virtual_import_source
from etk.integrations import ProviderDisposition, ProviderResponse
from etk.platform import Settings

_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000171")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000172")


def _source() -> dict[str, Any]:
    return {
        "source_kind": "movie",
        "source_id": "source-fixture",
        "item_type": "Movie",
        "title": "Fixture",
        "sha1": "A" * 40,
        "size": 1024,
        "file_name": "Fixture.mkv",
        "preid": "B" * 40,
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
                "action": "transfer_center_source",
                "payload": {
                    "source_kind": intent.source_kind,
                    "source_id": intent.source_id,
                    "source": intent.source,
                    "source_state_hash": intent.state_hash,
                    "mode": "rapid",
                },
            },
        ),
    )


class Credentials:
    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(access_token="fixture-token", account_id="115001")


class VirtualImports:
    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        return VirtualImportBundle("movie", "source-fixture", dict(source), (dict(source),))

    def get(self, _virtual_id: int) -> None:
        return None


class Repository:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.state: dict[str, Any] = {}
        self.request_states: list[dict[str, Any]] = []

    def save_prewrite_state(self, operation_id: UUID, claim_token: UUID, state: Mapping[str, Any]) -> None:
        assert operation_id == _OPERATION_ID
        assert claim_token == _CLAIM_TOKEN
        self.events.append("save")
        self.state = dict(state)

    def save_request_state(self, operation_id: UUID, claim_token: UUID, state: Mapping[str, Any]) -> None:
        assert operation_id == _OPERATION_ID
        assert claim_token == _CLAIM_TOKEN
        self.state = dict(state)
        self.request_states.append(dict(state))


class Center:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def acquire_transfer_lease(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.events.append("lease")
        assert payload["sha1"] == "A" * 40
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"allow": True, "lease_id": "lease-1"})

    def create_rapid_sign_job(self, payload: Mapping[str, Any]) -> ProviderResponse[dict[str, Any]]:
        self.events.append("sign-create")
        assert payload["lease_id"] if "lease_id" in payload else payload["request_meta_json"]["lease_id"]
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"job_id": "sign-job-1"})

    def wait_rapid_sign_job(self, job_id: str, *, timeout: int = 75) -> ProviderResponse[dict[str, Any]]:
        self.events.append("sign-wait")
        assert (job_id, timeout) == ("sign-job-1", 75)
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"status": "done", "sign_val": "C" * 40},
        )


class P115:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.readbacks = 0

    def readback(self, identity: Mapping[str, Any]) -> RapidTransferReadback:
        self.events.append("readback")
        self.readbacks += 1
        if self.readbacks == 1:
            return RapidTransferReadback("partial", missing_count=1)
        file = identity["files"][0]
        return RapidTransferReadback(
            "confirmed",
            receipts=(
                {
                    "fid": "remote-1",
                    "pick_code": "remote-pick",
                    "sha1": file["sha1"],
                    "size": file["size"],
                    "target_cid": identity["target_cid"],
                },
            ),
        )

    def upload_init(
        self,
        identity: Mapping[str, Any],
        file: Mapping[str, Any],
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome:
        del identity, file
        if not sign_key:
            self.events.append("init")
            return RapidUploadOutcome("sign_required", sign_key="sign-key", sign_check="0-1023")
        self.events.append("finalize")
        assert (sign_key, sign_val) == ("sign-key", "C" * 40)
        return RapidUploadOutcome("confirmed")


def test_rapid_transfer_uses_one_checkpoint_and_readback_after_signed_finalize() -> None:
    events: list[str] = []
    repository = Repository(events)
    p115 = P115(events)
    delivery = RapidTransferOperationDelivery(
        repository=cast(Any, repository),
        virtual_imports=cast(Any, VirtualImports()),
        credentials=cast(Any, Credentials()),
        settings=lambda: Settings(p115_api_priority="openapi", p115_save_path_cid="target-fixture"),
        center=cast(Any, Center(events)),
        p115=p115,
    )

    outcome = delivery.request(_operation(), checkpoint=lambda: events.append("checkpoint"))

    assert outcome.disposition.value == "confirmed"
    assert events == [
        "save",
        "readback",
        "checkpoint",
        "lease",
        "init",
        "sign-create",
        "sign-wait",
        "finalize",
        "readback",
    ]
    assert repository.state["account_id"] == "115001"
    assert repository.state["backend"] == "openapi"
    assert repository.state["target_cid"] == "target-fixture"
    assert repository.state["phase"] == "confirmed"
    assert repository.request_states[0]["phase"] == "lease_pending"
    assert outcome.result is not None
    assert outcome.result["lease_id"] == "lease-1"
    assert outcome.result["success_count"] == 1


def test_rapid_transfer_recovery_performs_only_target_readback() -> None:
    events: list[str] = []
    p115 = P115(events)
    p115.readbacks = 1
    identity = {
        "schema_version": 1,
        "kind": "rapid_transfer",
        "account_id": "115001",
        "backend": "openapi",
        "credential_hash": hashlib.sha256(b"openapi\0\0fixture-token").hexdigest(),
        "target_cid": "target-fixture",
        "source_kind": "movie",
        "source_id": "source-fixture",
        "source_state_hash": "d" * 64,
        "files": [_source()],
        "virtual_id": None,
        "virtual_state_hash": "",
    }
    delivery = RapidTransferOperationDelivery(
        repository=cast(Any, Repository(events)),
        virtual_imports=cast(Any, VirtualImports()),
        credentials=cast(Any, Credentials()),
        settings=lambda: Settings(),
        center=cast(Any, Center(events)),
        p115=p115,
    )

    outcome = delivery.readback(_operation(state=identity))

    assert outcome.disposition.value == "confirmed"
    assert events == ["readback"]


class DirectoryClient:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items

    def list_page(self, *, cid: str, limit: int, offset: int) -> Mapping[str, Any]:
        assert (cid, limit) == ("target-fixture", 1000)
        return {"count": len(self.items), "data": self.items[offset : offset + limit]}


class Directory:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items

    def open(self, *, expected_account_id: str, expected_backend: str | None = None) -> DirectoryClient:
        assert (expected_account_id, expected_backend) == ("115001", "openapi")
        return DirectoryClient(self.items)


def test_native_rapid_readback_requires_unique_sha1_size_match() -> None:
    identity = {
        "account_id": "115001",
        "backend": "openapi",
        "credential_hash": hashlib.sha256(b"openapi\0\0fixture-token").hexdigest(),
        "target_cid": "target-fixture",
        "files": [_source()],
    }
    gateway = NativeP115RapidTransferGateway(
        credentials=cast(Any, Credentials()),
        priority=lambda: "openapi",
        target_cid=lambda: "target-fixture",
        directory=cast(
            Any,
            Directory(
                [
                    {"fid": "one", "sha1": "A" * 40, "size": 1024, "pick_code": "pick-one"},
                    {"fid": "wrong", "sha1": "A" * 40, "size": 2048, "pick_code": "pick-wrong"},
                ]
            ),
        ),
    )

    confirmed = gateway.readback(identity)
    assert confirmed.status == "confirmed"
    assert confirmed.receipts[0]["fid"] == "one"

    ambiguous = NativeP115RapidTransferGateway(
        credentials=cast(Any, Credentials()),
        priority=lambda: "openapi",
        target_cid=lambda: "target-fixture",
        directory=cast(
            Any,
            Directory(
                [
                    {"fid": "one", "sha1": "A" * 40, "size": 1024, "pick_code": "pick-one"},
                    {"fid": "two", "sha1": "A" * 40, "size": 1024, "pick_code": "pick-two"},
                ]
            ),
        ),
    ).readback(identity)
    assert ambiguous.status == "ambiguous"
    assert ambiguous.ambiguous_count == 1


def test_rapid_transfer_rejects_missing_preid_before_checkpoint() -> None:
    source = {**_source(), "preid": ""}

    class MissingPreidVirtual:
        def prepare(self, _source: Mapping[str, Any]) -> VirtualImportBundle:
            return VirtualImportBundle("movie", "source-fixture", source, (source,))

        def get(self, _virtual_id: int) -> None:
            return None

    delivery = RapidTransferOperationDelivery(
        repository=cast(Any, Repository([])),
        virtual_imports=cast(Any, MissingPreidVirtual()),
        credentials=cast(Any, Credentials()),
        settings=lambda: Settings(p115_save_path_cid="target-fixture"),
        center=cast(Any, Center([])),
        p115=cast(Any, P115([])),
    )
    checkpoints = 0

    def checkpoint() -> None:
        nonlocal checkpoints
        checkpoints += 1

    with pytest.raises(ValueError, match="sha1"):
        delivery.request(_operation(), checkpoint=checkpoint)

    assert checkpoints == 0


def test_rapid_transfer_status_one_is_rejected_without_plain_upload() -> None:
    events: list[str] = []

    class PlainUploadRequired(P115):
        def upload_init(
            self,
            identity: Mapping[str, Any],
            file: Mapping[str, Any],
            *,
            sign_key: str = "",
            sign_val: str = "",
        ) -> RapidUploadOutcome:
            del identity, file, sign_key, sign_val
            self.events.append("init-status-1")
            return RapidUploadOutcome("rejected", message="status=1 requires a plain upload")

    repository = Repository(events)
    delivery = RapidTransferOperationDelivery(
        repository=cast(Any, repository),
        virtual_imports=cast(Any, VirtualImports()),
        credentials=cast(Any, Credentials()),
        settings=lambda: Settings(p115_api_priority="openapi", p115_save_path_cid="target-fixture"),
        center=cast(Any, Center(events)),
        p115=PlainUploadRequired(events),
    )

    outcome = delivery.request(_operation(), checkpoint=lambda: events.append("checkpoint"))

    assert outcome.disposition.value == "rejected"
    assert events == ["save", "readback", "checkpoint", "lease", "init-status-1"]
    assert repository.state["phase"] == "upload_rejected"


def test_native_rapid_gateway_rejects_same_account_credential_rotation() -> None:
    class RotatedCredentials:
        def load(self) -> P115CredentialSnapshot:
            return P115CredentialSnapshot(access_token="rotated-token", account_id="115001")

    identity = {
        "account_id": "115001",
        "backend": "openapi",
        "credential_hash": hashlib.sha256(b"openapi\0\0fixture-token").hexdigest(),
        "target_cid": "target-fixture",
        "files": [_source()],
    }
    gateway = NativeP115RapidTransferGateway(
        credentials=cast(Any, RotatedCredentials()),
        priority=lambda: "openapi",
        target_cid=lambda: "target-fixture",
        directory=cast(Any, Directory([])),
    )

    with pytest.raises(RuntimeError, match="credential changed"):
        gateway.readback(identity)


def test_native_rapid_gateway_rejects_runtime_backend_or_target_drift() -> None:
    class DualCredentials:
        def load(self) -> P115CredentialSnapshot:
            return P115CredentialSnapshot(
                access_token="fixture-token",
                account_id="115001",
                cookie="UID=fixture",
            )

    identity = {
        "account_id": "115001",
        "backend": "openapi",
        "credential_hash": hashlib.sha256(b"openapi\0\0fixture-token").hexdigest(),
        "target_cid": "target-fixture",
        "files": [_source()],
    }
    backend = NativeP115RapidTransferGateway(
        credentials=cast(Any, DualCredentials()),
        priority=lambda: "cookie",
        target_cid=lambda: "target-fixture",
        directory=cast(Any, Directory([])),
    )
    target = NativeP115RapidTransferGateway(
        credentials=cast(Any, DualCredentials()),
        priority=lambda: "openapi",
        target_cid=lambda: "target-changed",
        directory=cast(Any, Directory([])),
    )

    with pytest.raises(RuntimeError, match="backend changed"):
        backend.readback(identity)
    with pytest.raises(RuntimeError, match="target changed"):
        target.readback(identity)
