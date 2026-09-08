from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.p115 import deep_delete_adapters as deep_delete_adapters_module
from etk.domains.p115.bridge_compatibility import (
    EmbeddedP115SharedCleanupGateway,
    HttpxP115BridgeEmbyGateway,
    P115DeepDeleteJobGateway,
)
from etk.domains.p115.deep_delete import P115_DEEP_DELETE_OPERATION
from etk.domains.p115.deep_delete_adapters import (
    NativeP115DeleteReadbackGateway,
    NativeP115MediaCleanupReceiptGateway,
    NativeP115SharedCleanupGateway,
)
from etk.domains.subscription.cleanup_execution import CleanupEffectResult
from etk.platform import Settings
from etk.platform.jobs import PersistentEffectIndeterminate
from etk.processes.p6_deep_delete import NativeP115DeepDeleteOperation

SHA1 = "A" * 40


def _settings() -> Settings:
    return Settings(
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
        emby_api_timeout=3,
    )


def test_httpx_bridge_gateway_verifies_item_identity_without_exposing_token() -> None:
    requests: list[httpx.Request] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "Items": [
                    {
                        "Id": "100",
                        "Type": "Movie",
                        "Path": "/fixture/movie.strm",
                        "MediaSources": [{"Path": "/api/p115/play/pc-fixture/movie.mkv"}],
                        "Chapters": [
                            {"MarkerType": "IntroStart", "StartPositionTicks": 0},
                            {"MarkerType": "IntroEnd", "StartPositionTicks": 100},
                        ],
                    }
                ]
            },
        )

    gateway = HttpxP115BridgeEmbyGateway(
        _settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(respond)),
    )

    item = gateway.verified_item(SHA1, "100", expected_pick_code="pc-fixture")

    assert item is not None and item["Id"] == "100"
    assert len(requests) == 1
    assert requests[0].headers["X-Emby-Token"] == "fixture-token"
    assert "fixture-token" not in str(requests[0].url)
    assert "Chapters" in requests[0].url.params["Fields"].split(",")
    assert item["Chapters"][1]["MarkerType"] == "IntroEnd"


class _Jobs:
    def __init__(self) -> None:
        self.submit_call: dict[str, Any] | None = None
        self.projection = SimpleNamespace(
            job=SimpleNamespace(
                id="00000000-0000-0000-0000-000000000067",
                request_payload={"parameters": {"schema_version": 1, "snapshot": {"delete_token": "fixture-token"}}},
            )
        )

    def submit_native_operation(self, operation_key, parameters, **options):
        self.submit_call = {
            "operation_key": operation_key,
            "parameters": parameters,
            "options": options,
        }
        return SimpleNamespace(projection=self.projection)

    def get_idempotent(self, *, scope: str, key: str):
        assert (scope, key) == ("deep-delete", "fixture-token")
        return self.projection


def test_deep_delete_job_gateway_maps_internal_symbol_to_domain_process_adapter() -> None:
    jobs = _Jobs()
    gateway = P115DeepDeleteJobGateway(lambda: jobs)  # type: ignore[arg-type]
    payload = {"schema_version": 1, "snapshot": {"delete_token": "fixture-token"}}

    submitted = gateway.submit(
        P115_DEEP_DELETE_OPERATION,
        payload,
        max_attempts=1,
        timeout_seconds=900,
        idempotency_scope="deep-delete",
        idempotency_key="fixture-token",
    )
    replay = gateway.lookup(idempotency_scope="deep-delete", idempotency_key="fixture-token")

    assert submitted.job_id == "00000000-0000-0000-0000-000000000067"
    assert replay == submitted
    assert jobs.submit_call is not None
    assert jobs.submit_call["operation_key"] == P115_DEEP_DELETE_OPERATION
    assert jobs.submit_call["parameters"] == payload
    assert jobs.submit_call["options"] == {
        "display_name": "115 深度删除",
        "trigger_type": "system",
        "max_attempts": 1,
        "timeout_seconds": 900,
        "idempotency_scope": "deep-delete",
        "idempotency_key": "fixture-token",
    }


def test_embedded_shared_gateway_exposes_the_real_p5_receipt() -> None:
    receipt = {"ok": True, "removed": 2, "provider": "fixture"}

    result = EmbeddedP115SharedCleanupGateway().cleanup(
        {"delete_token": "fixture-token"},
        {"ok": True, "shared_cleanup": receipt},
        operation_id="fixture-token",
    )

    assert result == receipt


def _file_snapshot() -> dict[str, str]:
    return {
        "pick_code": "pc-fixture",
        "fid": "501",
        "parent_id": "500",
        "account_id": "115001",
        "backend": "openapi",
        "credential_fingerprint": "f" * 64,
    }


def test_native_deep_delete_uses_pinned_fid_readback_before_and_after_delete() -> None:
    calls: list[tuple[str, Any]] = []

    class Client:
        deleted = False

        def file_info(self, fid: str):
            calls.append(("info", fid))
            if self.deleted:
                return {"state": False, "code": 404, "message": "not found"}
            return {"state": True, "data": {"fid": fid}}

        def delete_files(self, fids: list[str]):
            calls.append(("delete", tuple(fids)))
            self.deleted = True
            return {"state": True}

    client = Client()

    class Provider:
        def open(self, **options: Any):
            calls.append(("open", options))
            return client

    result = NativeP115DeleteReadbackGateway(cast(Any, Provider())).delete_and_reconcile(
        ["pc-fixture"],
        [_file_snapshot()],
        enabled=True,
    )

    assert result == {
        "ok": True,
        "confirmed": True,
        "provider_accepted": True,
        "presence": {"501": False},
    }
    assert calls == [
        (
            "open",
            {
                "expected_account_id": "115001",
                "expected_backend": "openapi",
                "expected_credential_fingerprint": "f" * 64,
            },
        ),
        ("info", "501"),
        ("delete", ("501",)),
        ("info", "501"),
    ]


def test_native_deep_delete_does_not_open_p115_when_frozen_policy_is_disabled() -> None:
    provider = SimpleNamespace(
        open=lambda **_options: (_ for _ in ()).throw(AssertionError("disabled delete must not open P115"))
    )

    result = NativeP115DeleteReadbackGateway(cast(Any, provider)).delete_and_reconcile(
        ["pc-fixture"],
        [],
        enabled=False,
    )

    assert result == {"ok": True, "skipped": True, "reason": "sync_delete_disabled"}


def test_native_deep_delete_marks_failed_post_request_readback_indeterminate() -> None:
    class Client:
        reads = 0

        def file_info(self, fid: str):
            self.reads += 1
            if self.reads > 1:
                raise RuntimeError("fixture readback unavailable")
            return {"state": True, "data": {"fid": fid}}

        def delete_files(self, _fids: list[str]):
            return {"state": True}

    provider = SimpleNamespace(open=lambda **_options: Client())

    with pytest.raises(PersistentEffectIndeterminate, match="could not be read back"):
        NativeP115DeleteReadbackGateway(cast(Any, provider)).delete_and_reconcile(
            ["pc-fixture"],
            [_file_snapshot()],
            enabled=True,
        )


@pytest.mark.parametrize(
    ("pickcodes", "snapshots", "message"),
    [
        ([], [], "snapshot identity is incomplete"),
        (["pc-fixture"], [{**_file_snapshot(), "pick_code": "pc-other"}], "snapshot identity is incomplete"),
        (["pc-fixture"], [{**_file_snapshot(), "account_id": ""}], "provider identity is invalid"),
        (["pc-fixture"], [{**_file_snapshot(), "credential_fingerprint": ""}], "credential identity is invalid"),
    ],
)
def test_native_deep_delete_rejects_incomplete_frozen_identity(
    pickcodes: list[str],
    snapshots: list[dict[str, str]],
    message: str,
) -> None:
    provider = SimpleNamespace(open=lambda **_options: pytest.fail("invalid identity must not open provider"))
    with pytest.raises(RuntimeError, match=message):
        NativeP115DeleteReadbackGateway(cast(Any, provider)).delete_and_reconcile(
            pickcodes,
            snapshots,
            enabled=True,
        )


def test_native_deep_delete_rejects_missing_provider_and_invalid_fid() -> None:
    missing = SimpleNamespace(open=lambda **_options: None)
    with pytest.raises(RuntimeError, match="pinned provider is unavailable"):
        NativeP115DeleteReadbackGateway(cast(Any, missing)).delete_and_reconcile(
            ["pc-fixture"],
            [_file_snapshot()],
            enabled=True,
        )

    available = SimpleNamespace(open=lambda **_options: object())
    with pytest.raises(RuntimeError, match="FID identity is invalid"):
        NativeP115DeleteReadbackGateway(cast(Any, available)).delete_and_reconcile(
            ["pc-fixture"],
            [{**_file_snapshot(), "fid": ""}],
            enabled=True,
        )


def test_native_deep_delete_confirms_targets_already_absent() -> None:
    client = SimpleNamespace(file_info=lambda _fid: {"state": False, "code": 404})
    provider = SimpleNamespace(open=lambda **_options: client)

    result = NativeP115DeleteReadbackGateway(cast(Any, provider)).delete_and_reconcile(
        ["pc-fixture"],
        [_file_snapshot()],
        enabled=True,
    )

    assert result == {
        "ok": True,
        "confirmed": True,
        "already_absent": True,
        "presence": {"501": False},
    }


@pytest.mark.parametrize("readback", ["absent", "present", "error"])
def test_native_deep_delete_reconciles_a_failed_delete_request(readback: str) -> None:
    class Client:
        reads = 0

        def file_info(self, fid: str) -> Mapping[str, Any]:
            self.reads += 1
            if self.reads == 1 or readback == "present":
                return {"state": True, "data": {"fid": fid}}
            if readback == "error":
                raise RuntimeError("fixture readback unavailable")
            return {"state": False, "code": 404}

        @staticmethod
        def delete_files(_fids: list[str]) -> Mapping[str, Any]:
            raise RuntimeError("fixture delete failed")

    provider = SimpleNamespace(open=lambda **_options: Client())
    gateway = NativeP115DeleteReadbackGateway(cast(Any, provider))
    if readback == "absent":
        assert gateway.delete_and_reconcile(["pc-fixture"], [_file_snapshot()], enabled=True)["reconciled"] is True
    elif readback == "present":
        with pytest.raises(RuntimeError, match="targets remain present"):
            gateway.delete_and_reconcile(["pc-fixture"], [_file_snapshot()], enabled=True)
    else:
        with pytest.raises(PersistentEffectIndeterminate, match="could not be read back"):
            gateway.delete_and_reconcile(["pc-fixture"], [_file_snapshot()], enabled=True)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        ({"state": False}, "request was rejected"),
        ({"state": True}, "targets remain present after an accepted request"),
    ],
)
def test_native_deep_delete_rejects_unconfirmed_post_request_presence(
    response: Mapping[str, Any],
    message: str,
) -> None:
    class Client:
        @staticmethod
        def file_info(fid: str) -> Mapping[str, Any]:
            return {"state": True, "data": {"fid": fid}}

        @staticmethod
        def delete_files(_fids: list[str]) -> Mapping[str, Any]:
            return response

    provider = SimpleNamespace(open=lambda **_options: Client())
    with pytest.raises(RuntimeError, match=message):
        NativeP115DeleteReadbackGateway(cast(Any, provider)).delete_and_reconcile(
            ["pc-fixture"],
            [_file_snapshot()],
            enabled=True,
        )


def test_native_media_and_shared_cleanup_expose_confirmed_native_receipt() -> None:
    target = {"emby_id": "100", "item_type": "Movie", "tmdb_id": "fixture-tmdb"}
    gateway = SimpleNamespace(
        reconcile=lambda received, *, operation_id: CleanupEffectResult.confirmed(
            {
                "operation_id": operation_id,
                "removed_emby_id": received["emby_id"],
                "shared_source_count": 2,
            }
        )
    )
    media = NativeP115MediaCleanupReceiptGateway(cast(Any, gateway))
    receipt = media.cleanup(
        item_id="100",
        item_name="Fixture",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-token",
        target=target,
    )

    assert receipt["ok"] is True
    assert receipt["removed_emby_id"] == "100"
    assert NativeP115SharedCleanupGateway().cleanup(
        {"delete_token": "fixture-token"},
        receipt,
        operation_id="fixture-token",
    ) == {"ok": True, "confirmed": True, "shared_source_count": 2}


def test_native_media_cleanup_accepts_durable_shared_delegation_without_claiming_confirmation() -> None:
    target = {"emby_id": "100", "item_type": "Movie", "tmdb_id": "fixture-tmdb"}
    pending = [
        {
            "source_id": 7,
            "operation_id": "fixture-sharing-operation",
            "status": "queued",
        }
    ]
    gateway = SimpleNamespace(
        reconcile=lambda _received, *, operation_id: CleanupEffectResult.retryable(
            {"type": "cleanup_shared_source_pending"},
            {
                "operation_id": operation_id,
                "removed_emby_id": "100",
                "shared_source_count": 1,
                "pending": pending,
            },
        )
    )
    media = NativeP115MediaCleanupReceiptGateway(cast(Any, gateway))

    receipt = media.cleanup(
        item_id="100",
        item_name="Fixture",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-token",
        target=target,
    )

    assert receipt["ok"] is True
    assert receipt["removed_emby_id"] == "100"
    assert receipt["shared_cleanup"] == {
        "ok": True,
        "confirmed": False,
        "delegated": True,
        "shared_source_count": 1,
        "pending": pending,
    }
    assert (
        NativeP115SharedCleanupGateway().cleanup(
            {"delete_token": "fixture-token"},
            receipt,
            operation_id="fixture-token",
        )
        == receipt["shared_cleanup"]
    )


def test_native_media_cleanup_rejects_unproven_shared_delegation() -> None:
    gateway = SimpleNamespace(
        reconcile=lambda _received, *, operation_id: CleanupEffectResult.retryable(
            {"type": "cleanup_shared_source_pending"},
            {
                "operation_id": operation_id,
                "shared_source_count": 1,
                "pending": [{"source_id": 7, "operation_id": None, "status": "queued"}],
            },
        )
    )

    receipt = NativeP115MediaCleanupReceiptGateway(cast(Any, gateway)).cleanup(
        item_id="100",
        item_name="Fixture",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-token",
        target={"emby_id": "100", "item_type": "Movie", "tmdb_id": "fixture-tmdb"},
    )

    assert receipt["ok"] is False
    assert receipt["retryable"] is True


def test_native_media_cleanup_preserves_identity_and_indeterminate_failures() -> None:
    gateway = SimpleNamespace(
        reconcile=lambda *_args, **_kwargs: CleanupEffectResult.indeterminate(
            {"type": "fixture"},
            {"phase": "readback"},
        )
    )
    media = NativeP115MediaCleanupReceiptGateway(cast(Any, gateway))
    mismatched = media.cleanup(
        item_id="100",
        item_name="Fixture",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-token",
        target={"emby_id": "other", "item_type": "Movie"},
    )
    assert mismatched["indeterminate"] is True

    indeterminate = media.cleanup(
        item_id="100",
        item_name="Fixture",
        item_type="Movie",
        series_id="",
        local_only=False,
        operation_id="fixture-token",
        target={"emby_id": "100", "item_type": "Movie"},
    )
    assert indeterminate == {
        "ok": False,
        "indeterminate": True,
        "error": {"type": "fixture"},
        "evidence": {"phase": "readback"},
    }


def test_native_shared_cleanup_requires_a_mapping_receipt() -> None:
    assert NativeP115SharedCleanupGateway().cleanup({}, {}, operation_id="fixture") == {
        "ok": False,
        "error": "native media cleanup has no shared cleanup receipt",
    }


@pytest.mark.parametrize(
    "outcome",
    [
        CleanupEffectResult.retryable({"type": "other"}, {"pending": [{"source_id": 1}]}),
        CleanupEffectResult.retryable({"type": "cleanup_shared_source_pending"}, {"pending": []}),
        CleanupEffectResult.retryable(
            {"type": "cleanup_shared_source_pending"},
            {"pending": ["invalid"]},
        ),
        CleanupEffectResult.retryable(
            {"type": "cleanup_shared_source_pending"},
            {"pending": [{"source_id": 1, "operation_id": "fixture", "remaining_sources": 2}]},
        ),
    ],
)
def test_native_shared_cleanup_rejects_non_durable_delegation(outcome: CleanupEffectResult) -> None:
    assert deep_delete_adapters_module._durable_shared_cleanup_receipt(outcome) is None


def test_native_deep_delete_helper_parses_authoritative_file_readback() -> None:
    exists = deep_delete_adapters_module._remote_file_info_exists
    assert exists({"state": True, "data": [{"fid": "501"}]}, "501") is True
    assert exists({"state": True, "data": []}, "501") is True
    assert exists({"state": True, "data": "opaque"}, "501") is True
    assert exists({"state": True, "data": {"fid": "other"}}, "501") is False
    assert deep_delete_adapters_module._strings("invalid") == ()
    with pytest.raises(RuntimeError, match="not authoritative"):
        exists({"state": False, "code": 500, "message": "provider failure"}, "501")


def test_native_deep_delete_operation_tracks_one_nonreplayable_effect() -> None:
    events: list[tuple[Any, ...]] = []
    service = SimpleNamespace(execute=lambda parameters: {"ok": True, "payload": dict(parameters)})
    progress = SimpleNamespace(
        checkpoint=lambda: events.append(("checkpoint",)),
        record_started_effect=lambda label: events.append(("started", label)),
        record_committed_effect=lambda label: events.append(("committed", label)),
        update=lambda value, message: events.append(("update", value, message)),
    )
    operation = NativeP115DeepDeleteOperation(cast(Any, service))
    payload = {"schema_version": 1, "snapshot": {"delete_token": "fixture-token"}}

    result = operation.execute(payload, cast(Any, progress))

    assert result == {"ok": True, "payload": payload}
    assert events == [
        ("checkpoint",),
        ("started", "p115-deep-delete:fixture-token"),
        ("committed", "p115-deep-delete:fixture-token"),
        ("update", 100, "115 深度删除已完成"),
    ]
