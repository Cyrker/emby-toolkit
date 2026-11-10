from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

import etk.domains.p115.playback_native as playback_native_module
from etk.domains.p115.maintenance_transport import (
    HttpxP115BoundedDownloadGateway,
    P115MaintenanceTransportError,
    P115PinnedOperationsClient,
    P115PrimaryOperationsGateway,
)
from etk.domains.p115.media_info_adapters import P115DirectUrl
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.p115.playback import (
    P115PlaybackConfigService,
    P115PlaybackNotFoundError,
    P115PlaybackStateError,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackProviderDisposition,
    normalize_pool_config,
    play_pool_business_date,
    pool_credential_fingerprint,
)
from etk.domains.p115.playback_native import (
    NativeP115PlaybackProvider,
    NativeP115PlaybackRapidSigner,
    NativeP115PoolPlaybackClientFactory,
    P115PlaybackCredentialChanged,
    P115PlayPoolCredentialStore,
    P115PoolPlaybackClient,
    SqlAlchemyP115PlaybackSourceGateway,
)
from etk.domains.p115.playback_service import P115PlaybackService
from etk.domains.p115.playback_user_repository import SqlAlchemyP115PlaybackUserGateway
from etk.domains.sharing.rapid_transfer import RapidUploadOutcome

SOURCE = {
    "fid": "source-fid",
    "holder_account_id": "115001",
    "pick_code": "source-pick",
    "sha1": "A" * 40,
    "size": 1024,
    "preid": "B" * 40,
    "file_name": "Fixture.mkv",
}


class MutableCredentials:
    def __init__(self) -> None:
        self.snapshot = P115CredentialSnapshot(
            access_token="fixture-access-token",
            refresh_token="fixture-refresh-token",
            account_id="115001",
        )
        self.saved = 0

    def load(self) -> P115CredentialSnapshot:
        return self.snapshot

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        self.saved += 1

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        self.saved += 1

    @staticmethod
    def app_label(app_type: str) -> str:
        return app_type


class FakePinnedSdk:
    def __init__(self, after_request: Any = None) -> None:
        self.after_request = after_request
        self.copy_calls: list[tuple[list[str], str, float]] = []
        self.info_calls: list[tuple[str, float]] = []
        self.skim_calls: list[tuple[str, float]] = []
        self.request_calls: list[tuple[str, str, object, float]] = []

    def request(
        self,
        url: str,
        method: str = "GET",
        payload: object = None,
        **kwargs: Any,
    ) -> Mapping[str, Any]:
        self.request_calls.append((url, method, payload, float(kwargs["timeout"])))
        if self.after_request is not None:
            self.after_request()
        return {"state": True}

    def fs_copy(self, file_ids: list[str], pid: str, **kwargs: Any) -> Mapping[str, Any]:
        self.copy_calls.append((list(file_ids), pid, float(kwargs["timeout"])))
        if self.after_request is not None:
            self.after_request()
        return {"state": True}

    def fs_info(self, file_id: str, **kwargs: Any) -> Mapping[str, Any]:
        self.info_calls.append((file_id, float(kwargs["timeout"])))
        if self.after_request is not None:
            self.after_request()
        return {"state": True, "data": {"file_id": file_id}}

    def fs_file_skim(self, file_id: str, **kwargs: Any) -> Mapping[str, Any]:
        self.skim_calls.append((file_id, float(kwargs["timeout"])))
        if self.after_request is not None:
            self.after_request()
        return {"state": True, "data": [{"file_id": file_id}]}


def _pinned_operations(
    credentials: MutableCredentials,
    sdk: FakePinnedSdk,
    *,
    backend: str = "openapi",
) -> P115PinnedOperationsClient:
    return P115PinnedOperationsClient(
        credentials=cast(Any, credentials),
        snapshot=credentials.snapshot,
        backend=backend,
        sdk_factory=cast(Any, lambda _snapshot, _backend: sdk),
        directory=cast(Any, object()),
        timeout_seconds=9,
    )


def test_pinned_operations_use_real_sdk_copy_and_info_methods() -> None:
    credentials = MutableCredentials()
    sdk = FakePinnedSdk()
    operations = _pinned_operations(credentials, sdk)

    assert operations.copy_files(["source", "source"], "target") == {"state": True}
    assert operations.file_info("remote") == {"state": True, "data": {"file_id": "remote"}}
    assert sdk.copy_calls == [(["source"], "target", 9.0)]
    assert sdk.info_calls == [("remote", 9.0)]

    cookie_credentials = MutableCredentials()
    cookie_credentials.snapshot = P115CredentialSnapshot(
        cookie="UID=fixture",
        app_type="alipaymini",
        account_id="115001",
    )
    cookie_sdk = FakePinnedSdk()
    cookie_operations = _pinned_operations(cookie_credentials, cookie_sdk, backend="cookie")
    assert cookie_operations.file_info("cookie-remote") == {
        "state": True,
        "data": [{"file_id": "cookie-remote"}],
    }
    assert cookie_sdk.skim_calls == [("cookie-remote", 9.0)]
    assert cookie_sdk.info_calls == []


def test_pinned_cookie_share_import_uses_supported_raw_sdk_request() -> None:
    credentials = MutableCredentials()
    credentials.snapshot = P115CredentialSnapshot(
        cookie="UID=fixture",
        app_type="alipaymini",
        account_id="115001",
    )
    sdk = FakePinnedSdk()
    operations = _pinned_operations(credentials, sdk, backend="cookie")

    assert operations.import_share("share-fixture", "2468", "target-fixture") == {"state": True}
    assert sdk.request_calls == [
        (
            "https://webapi.115.com/share/receive",
            "POST",
            {
                "share_code": "share-fixture",
                "receive_code": "2468",
                "cid": "target-fixture",
            },
            9.0,
        )
    ]


def test_pinned_cookie_share_import_fences_post_request_credential_drift() -> None:
    credentials = MutableCredentials()
    credentials.snapshot = P115CredentialSnapshot(
        cookie="UID=fixture",
        app_type="alipaymini",
        account_id="115001",
    )

    def rotate() -> None:
        credentials.snapshot = P115CredentialSnapshot(
            cookie="UID=rotated",
            app_type="alipaymini",
            account_id="115001",
        )

    sdk = FakePinnedSdk(rotate)
    operations = _pinned_operations(credentials, sdk, backend="cookie")

    with pytest.raises(P115MaintenanceTransportError, match="credential changed"):
        operations.import_share("share-fixture", "2468", "target-fixture")
    assert len(sdk.request_calls) == 1


def test_primary_operations_gateway_fences_expected_provider_credential() -> None:
    credentials = MutableCredentials()
    directory_calls: list[tuple[str | None, str | None]] = []

    class Directory:
        @staticmethod
        def open(
            *,
            expected_account_id: str | None = None,
            expected_backend: str | None = None,
        ) -> object:
            directory_calls.append((expected_account_id, expected_backend))
            return object()

    gateway = P115PrimaryOperationsGateway(
        credentials=cast(Any, credentials),
        priority=lambda: "openapi",
        directory=cast(Any, Directory()),
        sdk_factory=cast(Any, lambda _snapshot, _backend: FakePinnedSdk()),
    )
    fingerprint = hashlib.sha256(b"openapi\0\0fixture-access-token").hexdigest()

    client = gateway.open(
        expected_account_id="115001",
        expected_backend="openapi",
        expected_credential_fingerprint=fingerprint,
    )

    assert client is not None
    assert client.backend == "openapi"
    assert directory_calls == [("115001", "openapi")]
    with pytest.raises(P115MaintenanceTransportError, match="credential changed"):
        gateway.open(
            expected_account_id="115001",
            expected_backend="openapi",
            expected_credential_fingerprint="0" * 64,
        )


@pytest.mark.parametrize("method", ["copy", "info"])
def test_pinned_operations_fence_credential_drift_after_provider_call(method: str) -> None:
    credentials = MutableCredentials()

    def rotate() -> None:
        credentials.snapshot = P115CredentialSnapshot(
            access_token="rotated-access-token",
            refresh_token="fixture-refresh-token",
            account_id="115001",
        )

    operations = _pinned_operations(credentials, FakePinnedSdk(rotate))

    with pytest.raises(P115MaintenanceTransportError, match="credential changed"):
        if method == "copy":
            operations.copy_files(["source"], "target")
        else:
            operations.file_info("remote")


def test_pinned_primary_credentials_forbid_rotation_inside_one_intent() -> None:
    credentials = MutableCredentials()
    pinned_type = cast(Any, playback_native_module.__dict__["_PinnedPrimaryCredentialStore"])
    store = pinned_type(
        credentials,
        account_id="115001",
        backend="openapi",
        expected_fingerprint=pool_credential_fingerprint(
            {"access_token": "fixture-access-token", "cookie": "", "app_type": ""},
            "openapi",
        ),
    )

    assert store.load().account_id == "115001"
    with pytest.raises(P115PlaybackCredentialChanged, match="new intent"):
        store.save_openapi(
            "rotated-access-token",
            "rotated-refresh-token",
            account_id="115001",
            expected_access_token="fixture-access-token",
        )
    assert credentials.saved == 0


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"state": True, "data": {"file_id": "remote"}}, True),
        ({"state": True, "data": [{"file_id": "remote"}]}, True),
        ({"state": True, "data": {}}, None),
        ({"state": True, "data": {"file_id": "other"}}, None),
        ({"state": True, "data": [{"file_id": "other"}]}, None),
        ({"state": False, "code": 404}, False),
        ({"state": False}, None),
    ],
)
def test_native_remote_existence_uses_fixed_fid_even_after_directory_move(
    response: dict[str, Any],
    expected: bool | None,
) -> None:
    class FileInfoOnlyOperations:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def file_info(self, file_id: str) -> Mapping[str, Any]:
            self.calls.append(file_id)
            return response

        def list_children(self, _parent_id: str) -> tuple[dict[str, Any], ...]:
            raise AssertionError("fixed-FID reclaim must not infer deletion from a parent listing")

    operations = FileInfoOnlyOperations()
    client_type = cast(Any, playback_native_module.__dict__["_NativeP115PoolPlaybackClient"])
    client = client_type(
        account_id="115001",
        backend="openapi",
        credential_hash="c" * 64,
        credentials=cast(Any, object()),
        operations=cast(Any, operations),
        rapid=cast(Any, object()),
        direct_urls=cast(Any, object()),
        downloads=cast(Any, object()),
        source_kind="playback",
    )

    assert client.remote_exists("remote", "original-parent") is expected
    assert operations.calls == ["remote"]


@pytest.mark.parametrize(
    ("request_patch", "error"),
    [
        ({"account_kind": ""}, P115PlaybackStateError),
        ({"account_kind": "primary"}, P115PlaybackNotFoundError),
        ({"account_kind": "pool", "credential_hash": "", "account_revision": ""}, ValueError),
    ],
)
def test_native_client_factory_rejects_missing_pinned_identity_before_provider_io(
    request_patch: dict[str, Any],
    error: type[Exception],
) -> None:
    factory = NativeP115PoolPlaybackClientFactory(cast(Any, object()))

    with pytest.raises(error):
        factory.open(_operation(request_patch=request_patch))


def test_play_pool_daily_traffic_uses_the_shanghai_business_date() -> None:
    now = datetime(2026, 8, 30, 16, 30, tzinfo=UTC)

    normalized = normalize_pool_config(
        {
            "accounts": [
                {
                    "id": "fixture-account",
                    "access_token": "fixture-token",
                    "daily_traffic_date": "2026-08-30",
                    "daily_traffic_bytes": 123,
                }
            ]
        },
        now=now,
    )

    assert play_pool_business_date(now) == "2026-08-31"
    assert normalized["accounts"][0]["daily_traffic_date"] == "2026-08-31"
    assert normalized["accounts"][0]["daily_traffic_bytes"] == 0


def test_native_primary_playback_identity_honors_cookie_priority() -> None:
    credentials = MutableCredentials()
    credentials.snapshot = P115CredentialSnapshot(
        access_token="fixture-openapi-token",
        refresh_token="fixture-refresh-token",
        cookie="UID=115001_A1_1; CID=fixture; SEID=fixture",
        app_type="mac",
        account_id="115001",
    )
    gateway = SqlAlchemyP115PlaybackSourceGateway(
        cast(Any, lambda: None),
        primary_credentials=cast(Any, credentials),
        priority=lambda: "cookie",
    )

    account = gateway.primary_account()

    assert account["account_id"] == "115001"
    assert account["provider_backend"] == "cookie"
    assert account["credential_hash"] == pool_credential_fingerprint(
        {
            "access_token": credentials.snapshot.access_token,
            "cookie": credentials.snapshot.cookie,
            "app_type": credentials.snapshot.app_type,
        },
        "cookie",
    )


def _operation(
    *,
    session: dict[str, Any] | None = None,
    kind: PlaybackOperationKind = PlaybackOperationKind.SPEEDTEST,
    mode: PlaybackMode | None = None,
    request_patch: Mapping[str, Any] | None = None,
    request_started: bool | None = None,
    include_created_remote: bool = True,
) -> PlaybackOperation:
    now = datetime.now(UTC)
    request = {
        "account_kind": "pool",
        "target_cid": "target-cid",
        "credential_hash": "c" * 64,
        "account_revision": "d" * 64,
        "file_name": SOURCE["file_name"],
        "remote_fid": "remote-fid",
        "source": SOURCE,
    }
    if kind is PlaybackOperationKind.RECLAIM and include_created_remote:
        request["created_remote"] = True
    request.update(dict(request_patch or {}))
    return PlaybackOperation(
        id=uuid4(),
        sequence=1,
        operation_key="1" * 64,
        request_hash=b"r" * 32,
        kind=kind,
        mode=mode or (PlaybackMode.MAINTENANCE if kind is PlaybackOperationKind.SPEEDTEST else PlaybackMode.POOL),
        account_id="local-pool-account-uuid",
        provider_backend="openapi",
        request=request,
        status=PlaybackOperationStatus.CLAIMED,
        state_revision=1,
        attempts=1,
        available_at=now,
        claimed_by="fixture-worker",
        claim_token=uuid4(),
        claimed_at=now,
        lease_expires_at=now,
        provider_request_started_at=(
            now if (request_started if request_started is not None else session is not None) else None
        ),
        provider_session=session,
        remote_receipt=None,
        readback=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


class FakeClient:
    def __init__(self) -> None:
        self.exists: bool | None = False
        self.rapid_calls: list[tuple[str, str]] = []
        self.resolve_calls = 0
        self.measure_calls = 0
        self.delete_calls = 0
        self.rapid_exception = False
        self.require_sign = False
        self.measure_exception = False
        self.delete_result: bool | None = True
        self.copy_result: dict[str, Any] = {"state": True}
        self.copy_calls: list[tuple[str, str]] = []

    def find_directory(self, name: str) -> str:
        del name
        return "target-cid"

    def create_directory(self, name: str) -> dict[str, Any]:
        del name
        raise AssertionError("speedtest fixture must not create a directory")

    def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
        del source, target_cid
        if not self.exists:
            return None
        return {
            "remote_fid": "remote-fid",
            "remote_pick_code": "remote-pick",
            "target_cid": "target-cid",
        }

    def rapid_upload(
        self,
        source: Mapping[str, Any],
        target_cid: str,
        *,
        sign_key: str = "",
        sign_val: str = "",
    ) -> RapidUploadOutcome:
        del source, target_cid
        self.rapid_calls.append((sign_key, sign_val))
        if self.rapid_exception:
            raise RuntimeError("https://provider.invalid/signed?token=secret")
        if self.require_sign and not sign_key:
            return RapidUploadOutcome("sign_required", sign_key="ephemeral-key", sign_check="0-3")
        self.exists = True
        return RapidUploadOutcome("confirmed")

    def copy_source(self, source_fid: str, target_cid: str) -> Mapping[str, Any]:
        self.copy_calls.append((source_fid, target_cid))
        if self.copy_result.get("state") is True:
            self.exists = True
        return dict(self.copy_result)

    def resolve_download(self, pick_code: str, user_agent: str) -> str:
        del pick_code, user_agent
        self.resolve_calls += 1
        return "https://provider.invalid/signed?token=secret"

    def measure(self, url: str, user_agent: str) -> dict[str, Any]:
        del url, user_agent
        self.measure_calls += 1
        if self.measure_exception:
            raise RuntimeError("https://provider.invalid/signed?token=secret")
        return {"bytes": 1024, "seconds": 1.0, "bps": 1024, "speed_text": "1.00 KB/s"}

    def remote_exists(self, remote_fid: str, target_cid: str) -> bool | None:
        del remote_fid, target_cid
        return self.exists

    def delete_remote(self, remote_fid: str, target_cid: str) -> bool | None:
        del remote_fid, target_cid
        self.delete_calls += 1
        if self.delete_result is True:
            self.exists = False
        return self.delete_result


class FakeFactory:
    def __init__(self, client: FakeClient) -> None:
        self.client = client

    def open(self, operation: PlaybackOperation) -> P115PoolPlaybackClient:
        del operation
        return self.client


class FakeSigner:
    def __init__(self) -> None:
        self.calls: list[tuple[dict[str, Any], str]] = []

    def sign(self, source: Mapping[str, Any], sign_check: str) -> str:
        self.calls.append((dict(source), sign_check))
        return "D" * 40

    def preid(self, source: Mapping[str, Any]) -> str:
        self.calls.append((dict(source), "preid"))
        return "B" * 40


def _provider(client: FakeClient, signer: FakeSigner | None = None) -> NativeP115PlaybackProvider:
    return NativeP115PlaybackProvider(FakeFactory(client), signer or FakeSigner())


def test_native_speedtest_signed_finalize_keeps_secrets_out_of_durable_state() -> None:
    client = FakeClient()
    client.require_sign = True
    signer = FakeSigner()
    checkpoints: list[dict[str, Any]] = []

    outcome = _provider(client, signer).execute(
        _operation(),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert [item["phase"] for item in checkpoints] == [
        "rapid_upload",
        "rapid_sign_finalize",
        "speedtest",
        "delete",
    ]
    assert client.rapid_calls == [("", ""), ("ephemeral-key", "D" * 40)]
    assert signer.calls == [(SOURCE, "0-3")]
    assert client.resolve_calls == client.measure_calls == client.delete_calls == 1
    assert "ephemeral-key" not in repr(outcome)
    assert "token=secret" not in repr(outcome)


def test_native_speedtest_checkpoints_computed_preid_before_rapid_mutation() -> None:
    class PreidSpeedtestClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.readback_preids: list[str] = []

        def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
            self.readback_preids.append(str(source.get("preid") or ""))
            return super().readback_source(source, target_cid)

    client = PreidSpeedtestClient()
    signer = FakeSigner()
    checkpoints: list[dict[str, Any]] = []
    source = {**SOURCE, "preid": ""}

    outcome = _provider(client, signer).execute(
        _operation(request_patch={"source": source}),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert signer.calls == [(source, "preid")]
    assert checkpoints[0]["phase"] == "rapid_upload"
    assert checkpoints[0]["source_preid"] == "B" * 40
    assert client.readback_preids == ["B" * 40, "B" * 40]


def test_native_speedtest_readback_uses_checkpointed_preid_without_recomputing() -> None:
    class PreidReadbackClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.readback_preids: list[str] = []

        def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
            self.readback_preids.append(str(source.get("preid") or ""))
            return super().readback_source(source, target_cid)

    client = PreidReadbackClient()
    client.exists = True
    signer = FakeSigner()

    outcome = _provider(client, signer).readback(
        _operation(
            request_patch={"source": {**SOURCE, "preid": ""}},
            session={"phase": "rapid_upload", "source_preid": "B" * 40},
        )
    )

    assert outcome.disposition is PlaybackProviderDisposition.RETRYABLE
    assert client.readback_preids == ["B" * 40]
    assert signer.calls == []


def test_native_rapid_prepare_checkpoints_once_and_projects_authoritative_identity() -> None:
    client = FakeClient()
    checkpoints: list[dict[str, Any]] = []

    outcome = _provider(client).execute(
        _operation(kind=PlaybackOperationKind.PREPARE),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert [value["phase"] for value in checkpoints] == ["rapid_upload"]
    assert client.rapid_calls == [("", "")]
    assert client.copy_calls == []
    assert outcome.receipt == {
        "target_cid": "target-cid",
        "remote_fid": "remote-fid",
        "remote_pick_code": "remote-pick",
        "file_name": "Fixture.mkv",
        "created_remote": True,
    }


def test_native_prepare_checkpoint_recovery_is_readback_only() -> None:
    client = FakeClient()

    outcome = _provider(client).execute(
        _operation(kind=PlaybackOperationKind.PREPARE, session={"phase": "rapid_upload"}),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert client.rapid_calls == []
    assert client.copy_calls == []


def test_native_prepare_checkpoint_recovery_adopts_readback_without_claiming_remote_ownership() -> None:
    client = FakeClient()
    client.exists = True
    provider = _provider(client)
    operation = _operation(
        kind=PlaybackOperationKind.PREPARE,
        session={"phase": "rapid_upload", "preflight_absent": True},
    )

    outcome = provider.readback(operation)

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt == {
        "target_cid": "target-cid",
        "remote_fid": "remote-fid",
        "remote_pick_code": "remote-pick",
        "file_name": "Fixture.mkv",
        "created_remote": False,
    }
    assert client.rapid_calls == []
    assert client.copy_calls == []


def test_native_prepare_historical_readback_never_claims_remote_ownership_without_preflight_evidence() -> None:
    client = FakeClient()
    client.exists = True

    outcome = _provider(client).readback(
        _operation(kind=PlaybackOperationKind.PREPARE, session={"phase": "rapid_upload"})
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt is not None
    assert outcome.receipt["created_remote"] is False


def test_native_prepare_recovery_uses_checkpointed_preid_without_recomputing_or_resending() -> None:
    class PreidReadbackClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.readback_preids: list[str] = []

        def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
            self.readback_preids.append(str(source.get("preid") or ""))
            return super().readback_source(source, target_cid)

    client = PreidReadbackClient()
    client.exists = True
    signer = FakeSigner()
    source = {**SOURCE, "preid": ""}

    outcome = _provider(client, signer).readback(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            request_patch={"source": source},
            session={"phase": "rapid_upload", "source_preid": "B" * 40},
        )
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt is not None and outcome.receipt["created_remote"] is False
    assert client.readback_preids == ["B" * 40]
    assert signer.calls == []


def test_native_prepare_invalid_preflight_marker_never_claims_remote_ownership() -> None:
    client = FakeClient()
    client.exists = True

    outcome = _provider(client).readback(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            session={"phase": "rapid_upload", "preflight_absent": "false"},
        )
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt is not None
    assert outcome.receipt["created_remote"] is False


def test_native_prepare_checkpoint_transport_unknown_never_resends() -> None:
    class UnknownReadbackClient(FakeClient):
        def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
            del source, target_cid
            raise RuntimeError("fixture readback transport unknown")

    client = UnknownReadbackClient()
    outcome = _provider(client).execute(
        _operation(kind=PlaybackOperationKind.PREPARE, session={"phase": "copy"}),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert client.rapid_calls == []
    assert client.copy_calls == []


def test_native_copy_prepare_uses_one_pinned_copy_then_authoritative_readback() -> None:
    client = FakeClient()
    checkpoints: list[dict[str, Any]] = []

    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            mode=PlaybackMode.COPY,
            request_patch={"account_kind": "primary", "account_revision": ""},
        ),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert [value["phase"] for value in checkpoints] == ["copy"]
    assert client.copy_calls == [("source-fid", "target-cid")]
    assert client.rapid_calls == []


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({}, PlaybackProviderDisposition.INDETERMINATE),
        ({"state": False, "code": 50001}, PlaybackProviderDisposition.REJECTED),
    ],
)
def test_native_copy_prepare_distinguishes_unknown_from_explicit_rejection(
    response: dict[str, Any],
    expected: PlaybackProviderDisposition,
) -> None:
    client = FakeClient()
    client.copy_result = response

    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            mode=PlaybackMode.COPY,
            request_patch={"account_kind": "primary", "account_revision": ""},
        ),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is expected
    assert outcome.request_sent is True
    assert client.copy_calls == [("source-fid", "target-cid")]


def test_native_copy_rejection_cannot_claim_a_matching_preexisting_file() -> None:
    class RejectedWithLateMatchClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.readback_calls = 0
            self.copy_result = {"state": False, "code": 50001}

        def readback_source(self, source: Mapping[str, Any], target_cid: str) -> dict[str, Any] | None:
            self.readback_calls += 1
            if self.readback_calls == 1:
                return None
            return super().readback_source(source, target_cid)

        def copy_source(self, source_fid: str, target_cid: str) -> Mapping[str, Any]:
            self.exists = True
            return super().copy_source(source_fid, target_cid)

    client = RejectedWithLateMatchClient()
    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            mode=PlaybackMode.COPY,
            request_patch={"account_kind": "primary", "account_revision": ""},
        ),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.REJECTED
    assert client.copy_calls == [("source-fid", "target-cid")]
    assert client.readback_calls == 1


def test_native_rapid_prepare_computes_missing_preid_before_provider_checkpoint() -> None:
    client = FakeClient()
    signer = FakeSigner()
    checkpoints: list[dict[str, Any]] = []
    source = {**SOURCE, "preid": ""}

    outcome = _provider(client, signer).execute(
        _operation(
            kind=PlaybackOperationKind.PREPARE,
            request_patch={"source": source},
        ),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert signer.calls == [(source, "preid")]
    assert checkpoints == [
        {
            "phase": "rapid_upload",
            "account_id": "local-pool-account-uuid",
            "backend": "openapi",
            "kind": "prepare",
            "preflight_absent": True,
            "source_preid": "B" * 40,
        }
    ]


def test_native_reclaim_preflight_missing_confirms_without_delete() -> None:
    client = FakeClient()
    checkpoints: list[dict[str, Any]] = []

    outcome = _provider(client).execute(
        _operation(kind=PlaybackOperationKind.RECLAIM),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.request_sent is False
    assert outcome.receipt == {"deleted": True, "remote_fid": "remote-fid"}
    assert client.delete_calls == 0
    assert checkpoints == []


def test_native_reclaim_releases_unowned_artifact_without_remote_delete() -> None:
    client = FakeClient()
    client.exists = True

    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.RECLAIM,
            request_patch={"created_remote": False},
        ),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.request_sent is False
    assert outcome.receipt == {"deleted": False, "released": True, "remote_fid": "remote-fid"}
    assert client.delete_calls == 0


@pytest.mark.parametrize(
    ("include_created_remote", "created_remote"),
    [(False, None), (True, None), (True, "false")],
)
def test_native_reclaim_never_deletes_without_explicit_boolean_ownership(
    include_created_remote: bool,
    created_remote: object,
) -> None:
    client = FakeClient()
    client.exists = True
    request_patch = {"created_remote": created_remote} if include_created_remote else {}

    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.RECLAIM,
            request_patch=request_patch,
            include_created_remote=include_created_remote,
        ),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt == {"deleted": False, "released": True, "remote_fid": "remote-fid"}
    assert client.delete_calls == 0


def test_native_unowned_reclaim_with_missing_remote_fid_rejects_before_provider_io() -> None:
    client = FakeClient()

    outcome = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.RECLAIM,
            request_patch={"remote_fid": ""},
            include_created_remote=False,
        ),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.REJECTED
    assert outcome.error is not None and outcome.error["type"] == "reclaim_fid_missing"
    assert client.delete_calls == 0


def test_native_reclaim_delete_and_checkpoint_recovery_never_resend() -> None:
    client = FakeClient()
    client.exists = True
    checkpoints: list[dict[str, Any]] = []

    confirmed = _provider(client).execute(
        _operation(kind=PlaybackOperationKind.RECLAIM),
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    assert confirmed.disposition is PlaybackProviderDisposition.CONFIRMED
    assert [value["phase"] for value in checkpoints] == ["delete"]
    assert client.delete_calls == 1

    client.exists = True
    replay = _provider(client).execute(
        _operation(
            kind=PlaybackOperationKind.RECLAIM,
            session={"phase": "delete", "remote_fid": "remote-fid"},
        ),
        checkpoint=lambda _value: None,
    )

    assert replay.disposition is PlaybackProviderDisposition.RETRYABLE
    assert client.delete_calls == 1


@pytest.mark.parametrize(
    ("exists", "expected"),
    [
        (False, PlaybackProviderDisposition.CONFIRMED),
        (True, PlaybackProviderDisposition.RETRYABLE),
        (None, PlaybackProviderDisposition.INDETERMINATE),
    ],
)
def test_native_reclaim_checkpoint_readback_is_fixed_fid_and_read_only(
    exists: bool | None,
    expected: PlaybackProviderDisposition,
) -> None:
    client = FakeClient()
    client.exists = exists

    outcome = _provider(client).readback(
        _operation(
            kind=PlaybackOperationKind.RECLAIM,
            session={"phase": "delete", "remote_fid": "remote-fid"},
        )
    )

    assert outcome.disposition is expected
    assert client.delete_calls == 0


@pytest.mark.parametrize(
    "kind",
    [
        PlaybackOperationKind.PREPARE,
        PlaybackOperationKind.ENSURE_TEMP_DIR,
        PlaybackOperationKind.SPEEDTEST,
        PlaybackOperationKind.RECLAIM,
    ],
)
@pytest.mark.parametrize("session", [None, {"phase": "invalid"}])
def test_native_provider_timestamp_without_valid_phase_never_resends_mutation(
    kind: PlaybackOperationKind,
    session: dict[str, Any] | None,
) -> None:
    client = FakeClient()
    client.exists = True
    outcome = _provider(client).execute(
        _operation(kind=kind, session=session, request_started=True),
        checkpoint=lambda _value: None,
    )

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert client.rapid_calls == []
    assert client.copy_calls == []
    assert client.delete_calls == 0


def test_native_speedtest_rapid_checkpoint_recovery_never_resends_upload() -> None:
    client = FakeClient()
    client.rapid_exception = True
    checkpoints: list[dict[str, Any]] = []
    provider = _provider(client)

    failed = provider.execute(_operation(), checkpoint=lambda value: checkpoints.append(dict(value)))

    assert failed.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert failed.session == checkpoints[-1]
    assert checkpoints[-1]["phase"] == "rapid_upload"
    assert "token=secret" not in repr(failed)
    durable = _operation(session=checkpoints[-1])

    assert provider.readback(durable).disposition is PlaybackProviderDisposition.INDETERMINATE
    assert provider.execute(durable, checkpoint=lambda _value: None).disposition is (
        PlaybackProviderDisposition.INDETERMINATE
    )
    assert len(client.rapid_calls) == 1


def test_native_speedtest_checkpoint_recovery_cleans_without_remeasuring() -> None:
    client = FakeClient()
    client.measure_exception = True
    checkpoints: list[dict[str, Any]] = []
    provider = _provider(client)

    failed = provider.execute(_operation(), checkpoint=lambda value: checkpoints.append(dict(value)))

    assert failed.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert checkpoints[-1]["phase"] == "speedtest"
    durable = _operation(session=checkpoints[-1])
    assert provider.readback(durable).disposition is PlaybackProviderDisposition.RETRYABLE

    cleanup_checkpoints: list[dict[str, Any]] = []
    cleanup = provider.execute(durable, checkpoint=lambda value: cleanup_checkpoints.append(dict(value)))

    assert cleanup.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert cleanup_checkpoints[-1]["phase"] == "delete"
    assert cleanup_checkpoints[-1]["speed_unknown"] is True
    assert client.resolve_calls == client.measure_calls == client.delete_calls == 1
    assert client.exists is False


def test_native_speedtest_delete_checkpoint_recovery_never_resends_delete() -> None:
    client = FakeClient()
    client.delete_result = None
    checkpoints: list[dict[str, Any]] = []
    provider = _provider(client)

    failed = provider.execute(_operation(), checkpoint=lambda value: checkpoints.append(dict(value)))

    assert failed.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert checkpoints[-1]["phase"] == "delete"
    durable = _operation(session=checkpoints[-1])
    recovered = provider.execute(durable, checkpoint=lambda _value: None)

    assert recovered.disposition is PlaybackProviderDisposition.RETRYABLE
    assert client.delete_calls == 1
    assert client.resolve_calls == client.measure_calls == 1


def test_native_speedtest_redacts_provider_messages_and_allowlists_speed_receipt() -> None:
    class ProviderMessageClient(FakeClient):
        def rapid_upload(
            self,
            source: Mapping[str, Any],
            target_cid: str,
            *,
            sign_key: str = "",
            sign_val: str = "",
        ) -> RapidUploadOutcome:
            del source, target_cid, sign_key, sign_val
            return RapidUploadOutcome(
                "rejected",
                message="https://provider.invalid/signed?token=secret Cookie=private",
            )

    rejected = _provider(ProviderMessageClient()).execute(_operation(), checkpoint=lambda _value: None)

    assert rejected.disposition is PlaybackProviderDisposition.REJECTED
    assert "provider.invalid" not in repr(rejected)
    assert "token=secret" not in repr(rejected)
    assert "Cookie=private" not in repr(rejected)

    class ExtraReceiptClient(FakeClient):
        def measure(self, url: str, user_agent: str) -> dict[str, Any]:
            value = super().measure(url, user_agent)
            return {
                **value,
                "url": "https://provider.invalid/signed?token=secret",
                "cookie": "private",
            }

    confirmed = _provider(ExtraReceiptClient()).execute(_operation(), checkpoint=lambda _value: None)

    assert confirmed.disposition is PlaybackProviderDisposition.CONFIRMED
    assert confirmed.receipt == {
        "bytes": 1024,
        "seconds": 1.0,
        "bps": 1024,
        "speed_text": "1.00 KB/s",
    }
    assert "provider.invalid" not in repr(confirmed)


def test_native_rapid_signer_fences_primary_credential_drift_during_range_read() -> None:
    class Credentials:
        def __init__(self) -> None:
            self.snapshot = P115CredentialSnapshot(
                access_token="fixture-access-token",
                refresh_token="fixture-refresh-token",
                account_id="115001",
            )

        def load(self) -> P115CredentialSnapshot:
            return self.snapshot

    credentials = Credentials()

    class DirectUrls:
        def resolve(self, _pick_code: str, *, expected_account_id: str, user_agent: str) -> P115DirectUrl:
            assert expected_account_id == "115001"
            return P115DirectUrl(
                "https://provider.invalid/signed",
                user_agent,
                "openapi",
                "fixture",
            )

    class Reader:
        def __init__(self, _url: str, **options: Any) -> None:
            self.cancel = options["cancel"]

        def read(self, _start: int, length: int) -> bytes:
            credentials.snapshot = P115CredentialSnapshot(
                access_token="fixture-access-token",
                refresh_token="rotated-refresh-token",
                account_id="115001",
            )
            assert self.cancel() is True
            return b"x" * length

    signer = NativeP115PlaybackRapidSigner(
        cast(Any, DirectUrls()),
        cast(Any, credentials),
        reader_factory=cast(Any, Reader),
    )

    with pytest.raises(P115PlaybackCredentialChanged, match="primary credential changed"):
        signer.sign(SOURCE, "0-3")


def test_bounded_speedtest_download_runs_the_credential_checkpoint_during_streaming() -> None:
    class Response:
        status_code = 206
        headers = {"Content-Length": "4"}

        def __enter__(self) -> Response:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def iter_bytes(self, *, chunk_size: int):
            del chunk_size
            yield b"data"

    class Client:
        def __enter__(self) -> Client:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def stream(self, method: str, url: str, *, headers: Mapping[str, str]) -> Response:
            assert method == "GET"
            assert url == "https://provider.invalid/signed"
            assert headers["Range"].startswith("bytes=0-")
            return Response()

    checks = 0

    def checkpoint() -> None:
        nonlocal checks
        checks += 1
        if checks == 4:
            raise P115PlaybackCredentialChanged("fixture credential drift")

    gateway = HttpxP115BoundedDownloadGateway(client_factory=cast(Any, Client))

    with pytest.raises(P115PlaybackCredentialChanged, match="fixture credential drift"):
        gateway.measure(
            "https://provider.invalid/signed",
            user_agent="fixture-agent",
            max_bytes=1024 * 1024,
            max_seconds=3,
            checkpoint=checkpoint,
        )
    assert checks == 4


class FakeConfig:
    def __init__(self) -> None:
        self.account_value = {
            "id": "local-pool-account-uuid",
            "access_token": "fixture-access-token",
            "refresh_token": "fixture-refresh-token",
            "cookie": "",
            "app_type": "alipaymini",
        }
        self.cas_calls: list[dict[str, str]] = []

    def account(self, account_id: str) -> dict[str, Any] | None:
        return dict(self.account_value) if account_id == self.account_value["id"] else None

    def compare_and_swap_tokens(self, account_id: str, **values: str) -> dict[str, Any] | None:
        self.cas_calls.append({"account_id": account_id, **values})
        if values["expected_access_token"] != self.account_value["access_token"]:
            return dict(self.account_value)
        self.account_value.update(
            access_token=values["access_token"],
            refresh_token=values["refresh_token"],
        )
        return dict(self.account_value)


def test_pool_credential_store_treats_local_uuid_as_opaque_and_fences_drift() -> None:
    config = FakeConfig()
    from etk.domains.p115.playback import pool_account_revision, pool_credential_fingerprint

    expected = pool_credential_fingerprint(config.account_value, "openapi")
    store = P115PlayPoolCredentialStore(
        cast(P115PlaybackConfigService, config),
        account_id="local-pool-account-uuid",
        backend="openapi",
        expected_fingerprint=expected,
        expected_account_revision=pool_account_revision(config.account_value, "openapi"),
    )

    assert store.load().account_id == "local-pool-account-uuid"
    config.account_value["owner_type"] = "user"
    with pytest.raises(P115PlaybackCredentialChanged, match="configuration changed"):
        store.load()
    config.account_value.pop("owner_type")
    config.account_value["access_token"] = "drifted-token"
    with pytest.raises(P115PlaybackCredentialChanged, match="changed after reservation"):
        store.load()

    config.account_value["access_token"] = "fixture-access-token"
    config.account_value["refresh_token"] = "drifted-refresh-token"
    with pytest.raises(P115PlaybackCredentialChanged, match="configuration changed"):
        store.load()


def test_pool_credential_store_refresh_uses_account_scoped_compare_and_swap() -> None:
    config = FakeConfig()
    from etk.domains.p115.playback import pool_account_revision, pool_credential_fingerprint

    store = P115PlayPoolCredentialStore(
        cast(P115PlaybackConfigService, config),
        account_id="local-pool-account-uuid",
        backend="openapi",
        expected_fingerprint=pool_credential_fingerprint(config.account_value, "openapi"),
        expected_account_revision=pool_account_revision(config.account_value, "openapi"),
    )

    store.save_openapi(
        "rotated-access-token",
        "rotated-refresh-token",
        account_id="local-pool-account-uuid",
        expected_access_token="fixture-access-token",
    )

    assert config.cas_calls == [
        {
            "account_id": "local-pool-account-uuid",
            "expected_access_token": "fixture-access-token",
            "access_token": "rotated-access-token",
            "refresh_token": "rotated-refresh-token",
        }
    ]


class FakePlaybackUserRepository:
    def display_names(self, user_ids: list[str]) -> dict[str, str]:
        return {user_id: f"User {user_id}" for user_id in user_ids}

    def expand_user_ids(self, user_ids: list[str]) -> tuple[str, ...]:
        return tuple(user_ids)

    def admin_chat_ids(self) -> tuple[str, ...]:
        return ("admin-chat",)


class FailingTelegram:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool]] = []

    def send_message(self, chat_id: str, text: str, *, disable_notification: bool = False) -> object:
        self.calls.append((chat_id, text, disable_notification))
        raise RuntimeError("fixture telegram failure")


def test_native_daily_speedtest_notification_is_best_effort_and_has_no_reward_text() -> None:
    repository = FakePlaybackUserRepository()
    telegram = FailingTelegram()
    gateway = SqlAlchemyP115PlaybackUserGateway(
        cast(Any, repository),
        telegram,
    )

    gateway.notify_daily_results(
        [
            {"success": True, "alias": "Fixture", "owner_name": "管理员", "speed_text": "8.00 MB/s"},
            {"success": False, "alias": "Failed", "owner_name": "管理员", "error": "测速未达标"},
        ]
    )

    assert telegram.calls == [
        (
            "admin-chat",
            "小号池每日测速结果\n通过 Fixture（管理员）：8.00 MB/s\n失败 Failed（管理员）：测速未达标",
            True,
        )
    ]


def test_native_playback_user_gateway_preserves_name_and_allowed_user_queries() -> None:
    gateway = SqlAlchemyP115PlaybackUserGateway(cast(Any, FakePlaybackUserRepository()), FailingTelegram())

    assert gateway.display_name(" fixture-user ") == "User fixture-user"
    assert gateway.display_name("") == ""
    assert gateway.expand_allowed_user_ids(["fixture-user"]) == ("fixture-user",)


class DailyConfig:
    def __init__(self) -> None:
        self.accounts = [
            {
                "id": "account-ok",
                "alias": "OK",
                "access_token": "token-ok",
                "enabled": True,
                "owner_type": "admin",
            },
            {
                "id": "account-failed",
                "alias": "Failed",
                "cookie": "cookie-failed",
                "enabled": True,
                "owner_type": "admin",
            },
        ]
        self.patches: list[tuple[str, dict[str, Any]]] = []
        self.notifications: list[list[dict[str, Any]]] = []

    def snapshot(self) -> dict[str, Any]:
        return {"enabled": True, "accounts": [dict(account) for account in self.accounts]}

    def account(self, account_id: str) -> dict[str, Any] | None:
        return next((dict(account) for account in self.accounts if account["id"] == account_id), None)

    def public_account(self, account: Mapping[str, Any]) -> dict[str, Any]:
        return {**dict(account), "owner_user_name": f"User {account.get('owner_user_id') or ''}"}

    def patch_account(self, account_id: str, patch: Mapping[str, Any]) -> dict[str, Any]:
        value = dict(patch)
        self.patches.append((account_id, value))
        account = next(account for account in self.accounts if account["id"] == account_id)
        account.update(value)
        return dict(account)

    def notify_daily_results(self, results: list[dict[str, Any]]) -> None:
        self.notifications.append([dict(item) for item in results])


class DailyProgress:
    def __init__(self) -> None:
        self.values: list[tuple[int, str]] = []

    def checkpoint(self) -> None:
        return None

    def update(self, progress: int, message: str) -> None:
        self.values.append((progress, message))


def test_native_daily_speedtest_reports_one_account_failure_as_partial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DailyConfig()
    unused = cast(Any, object())
    service = P115PlaybackService(
        config=cast(P115PlaybackConfigService, config),
        repository=unused,
        sources=unused,
        direct_links=unused,
        executor=unused,
        clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
    )

    def speedtest(account_id: str, *, intent_key: str = "") -> dict[str, Any]:
        assert intent_key == "daily:2026-08-31"
        if account_id == "account-failed":
            raise RuntimeError("fixture provider failure")
        return {"speed_text": "8.00 MB/s"}

    monkeypatch.setattr(service, "speedtest_account", speedtest)
    progress = DailyProgress()

    result = service.daily_speedtest(progress)

    assert result["status"] == "partial"
    assert result["failed_count"] == 1
    assert [item["success"] for item in result["results"]] == [True, False]
    assert config.patches[0][0] == "account-failed"
    assert config.notifications and len(config.notifications[0]) == 2
    assert progress.values[-1] == (100, "小号池每日测速完成，通过 1/2")


def test_native_daily_speedtest_idempotency_uses_the_shanghai_business_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = DailyConfig()
    config.accounts = [config.accounts[0]]
    service = P115PlaybackService(
        config=cast(P115PlaybackConfigService, config),
        repository=cast(Any, object()),
        sources=cast(Any, object()),
        direct_links=cast(Any, object()),
        executor=cast(Any, object()),
        clock=lambda: datetime(2026, 8, 30, 16, 30, tzinfo=UTC),
    )
    intent_keys: list[str] = []

    def speedtest(_account_id: str, *, intent_key: str = "") -> dict[str, Any]:
        intent_keys.append(intent_key)
        return {"speed_text": "8.00 MB/s"}

    monkeypatch.setattr(service, "speedtest_account", speedtest)

    service.daily_speedtest(DailyProgress())

    assert intent_keys == ["daily:2026-08-31"]


def test_native_daily_speedtest_reports_all_accounts_as_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    config = DailyConfig()
    unused = cast(Any, object())
    service = P115PlaybackService(
        config=cast(P115PlaybackConfigService, config),
        repository=unused,
        sources=unused,
        direct_links=unused,
        executor=unused,
        clock=lambda: datetime(2026, 8, 31, tzinfo=UTC),
    )
    monkeypatch.setattr(
        service,
        "speedtest_account",
        lambda _account_id, *, intent_key="": {"speed_text": f"8.00 MB/s {intent_key}"},
    )

    result = service.daily_speedtest(DailyProgress())

    assert result["status"] == "succeeded"
    assert result["failed_count"] == 0
    assert all(item["success"] for item in result["results"])


def test_speedtest_intent_uses_an_operation_scoped_remote_file_name(monkeypatch: pytest.MonkeyPatch) -> None:
    account = {
        "id": "local-pool-account-uuid",
        "alias": "Fixture",
        "access_token": "fixture-access-token",
        "refresh_token": "fixture-refresh-token",
        "temp_cid": "target-cid",
        "owner_type": "admin",
        "enabled": True,
    }

    class Config:
        def account(self, account_id: str) -> dict[str, Any] | None:
            return dict(account) if account_id == account["id"] else None

        def snapshot(self) -> dict[str, Any]:
            return {"auto_speedtest_threshold_mbps": 1.0}

    class Sources:
        def snapshot(self, _request: object) -> dict[str, Any]:
            return dict(SOURCE)

    service = P115PlaybackService(
        config=cast(P115PlaybackConfigService, Config()),
        repository=cast(Any, object()),
        sources=cast(Any, Sources()),
        direct_links=cast(Any, object()),
        executor=cast(Any, object()),
    )
    names: list[str] = []

    def reserve(intent: Any) -> Any:
        names.append(str(intent.request["source"]["file_name"]))
        return type(
            "ConfirmedOperation",
            (),
            {
                "status": PlaybackOperationStatus.CONFIRMED,
                "result": {"bytes": 1024, "seconds": 1.0, "bps": 1024, "speed_text": "1 KB/s"},
                "remote_receipt": None,
                "error": None,
            },
        )()

    monkeypatch.setattr(service, "_reserve_and_execute", reserve)

    service.speedtest_account(account["id"], intent_key="daily:2026-08-31")
    service.speedtest_account(account["id"], intent_key="daily:2026-08-31")
    service.speedtest_account(account["id"], intent_key="manual:2026-08-31")

    assert names[0] == names[1]
    assert names[0] != names[2]
    assert all(name.startswith("ETK-speedtest-") and name.endswith(".bin") for name in names)
