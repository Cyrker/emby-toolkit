from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID, uuid4

import httpx
import pytest
from pydantic import SecretStr

from etk.domains.p115.maintenance_transport import P115MaintenanceTransportError
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.subscription import cleanup_adapters
from etk.domains.subscription.cleanup_adapters import (
    ConfiguredCleanupEmbyGateway,
    LegacyCleanupMediaGateway,
    LegacyCleanupP115Gateway,
    NativeCleanupP115Gateway,
    SafeLocalCleanupGateway,
    SettingsCleanupExecutionPolicy,
    SqlAlchemyCleanupMediaGateway,
)
from etk.domains.subscription.cleanup_execution import (
    CleanupEffectResult,
    CleanupExecutionItem,
    CleanupExecutionPending,
    CleanupExecutionService,
    CleanupExecutionSummary,
)
from etk.platform import Settings


class FakeStore:
    def __init__(self, *, max_attempts: int = 3) -> None:
        job_id = uuid4()
        self.item = CleanupExecutionItem(
            id=uuid4(),
            job_id=job_id,
            execution_key="a" * 64,
            cleanup_index_id=1,
            item_key="version:emby-old",
            snapshot={"target": {"emby_id": "emby-old"}},
            status="queued",
            revision=0,
            attempt_id=uuid4(),
            worker_id="unclaimed",
            claim_token=uuid4(),
            attempt_no=1,
            max_attempts=max_attempts,
            effect_started=False,
            receipt=None,
        )
        self.terminal: str | None = None
        self.error: Mapping[str, Any] | None = None

    def claim_next(
        self,
        *,
        job_id: UUID,
        execution_key: str,
        attempt_id: UUID,
        worker_id: str,
        lease_seconds: int,
    ) -> CleanupExecutionItem | None:
        del lease_seconds
        assert job_id == self.item.job_id
        assert execution_key == self.item.execution_key
        if self.terminal is not None:
            return None
        attempt_no = 1 if self.item.worker_id == "unclaimed" else self.item.attempt_no + 1
        self.item = replace(
            self.item,
            status="effect_started" if self.item.effect_started else "claimed",
            revision=self.item.revision + 1,
            attempt_id=attempt_id,
            worker_id=worker_id,
            claim_token=uuid4(),
            attempt_no=attempt_no,
        )
        return self.item

    def save_receipt(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        *,
        start_effect: bool,
        lease_seconds: int,
    ) -> CleanupExecutionItem:
        del lease_seconds
        assert item.revision == self.item.revision
        self.item = replace(
            item,
            status="effect_started" if start_effect else item.status,
            revision=item.revision + 1,
            effect_started=item.effect_started or start_effect,
            receipt=dict(receipt),
        )
        return self.item

    def mark_confirmed(self, item: CleanupExecutionItem, receipt: Mapping[str, Any]) -> None:
        assert item.revision == self.item.revision
        self.item = replace(item, receipt=dict(receipt))
        self.terminal = "confirmed"

    def mark_indeterminate(
        self,
        item: CleanupExecutionItem,
        receipt: Mapping[str, Any],
        error: Mapping[str, Any],
    ) -> None:
        assert item.revision == self.item.revision
        self.item = replace(item, receipt=dict(receipt))
        self.error = dict(error)
        self.terminal = "indeterminate"

    def release_for_retry(self, item: CleanupExecutionItem, error: Mapping[str, Any]) -> None:
        assert item.effect_started is False
        self.item = replace(item, status="queued", worker_id="unclaimed")
        self.error = dict(error)

    def cancel_unstarted_job(self, job_id: UUID, error: Mapping[str, Any]) -> int:
        assert job_id == self.item.job_id
        assert self.item.effect_started is False
        self.error = dict(error)
        self.terminal = "cancelled"
        return 1

    def abort_unstarted(self, job_id: UUID, error: Mapping[str, Any]) -> int:
        assert job_id == self.item.job_id
        self.error = dict(error)
        return 0

    def summary(self, job_id: UUID) -> CleanupExecutionSummary:
        assert job_id == self.item.job_id
        return CleanupExecutionSummary(
            confirmed=int(self.terminal == "confirmed"),
            indeterminate=int(self.terminal == "indeterminate"),
            cancelled=int(self.terminal == "cancelled"),
            active=int(self.terminal is None),
        )


class FakeProgress:
    def __init__(self, *, cancelled: bool = False, timed_out: bool = False) -> None:
        self.cancelled = cancelled
        self.timed_out = timed_out
        self.updates: list[tuple[int, str]] = []
        self.checkpoints = 0

    def update(self, progress: int, message: str) -> None:
        self.updates.append((progress, message))

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.timed_out:
            raise TimeoutError("fixture timeout")
        if self.cancelled:
            raise RuntimeError("fixture cancellation")

    def control_state(self) -> Mapping[str, Any]:
        return {"cancel_requested": self.cancelled, "timed_out": self.timed_out}


class FakeReconciler:
    def __init__(self, results: list[CleanupEffectResult] | None = None) -> None:
        self.results = list(results or [CleanupEffectResult.confirmed()])
        self.calls: list[tuple[Mapping[str, Any], str | None]] = []

    def reconcile(self, target: Mapping[str, Any], *, operation_id: str | None = None) -> CleanupEffectResult:
        self.calls.append((dict(target), operation_id))
        return self.results.pop(0)


class FakeProvider:
    def __init__(
        self,
        *,
        request: CleanupEffectResult | None = None,
        readbacks: list[CleanupEffectResult] | None = None,
    ) -> None:
        self.request_result = request or CleanupEffectResult.confirmed()
        self.readbacks = list(readbacks or [CleanupEffectResult.confirmed()])
        self.requests = 0
        self.readback_calls = 0

    def request(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        assert target["emby_id"] == "emby-old"
        self.requests += 1
        return self.request_result

    def readback(self, target: Mapping[str, Any]) -> CleanupEffectResult:
        assert target["emby_id"] == "emby-old"
        self.readback_calls += 1
        return self.readbacks.pop(0)


class FakeMediaReceiptGateway:
    def __init__(self, result: Mapping[str, Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.result = dict(result or {"ok": True, "shared_cleanup": {"ok": True, "matched": 0}})

    def cleanup(self, **values: Any) -> Mapping[str, Any]:
        self.calls.append(dict(values))
        return dict(self.result)


def _service(
    store: FakeStore,
    *,
    local: FakeReconciler | None = None,
    media: FakeReconciler | None = None,
    p115: FakeProvider | None = None,
    emby: FakeProvider | None = None,
    readback_budget: int = 3,
) -> tuple[CleanupExecutionService, FakeReconciler, FakeReconciler, FakeProvider, FakeProvider]:
    local = local or FakeReconciler()
    media = media or FakeReconciler()
    p115 = p115 or FakeProvider()
    emby = emby or FakeProvider()
    return (
        CleanupExecutionService(
            store,
            local=local,
            media=media,
            p115=p115,
            emby=emby,
            readback_budget=readback_budget,
        ),
        local,
        media,
        p115,
        emby,
    )


def _execute(service: CleanupExecutionService, store: FakeStore, progress: FakeProgress) -> Mapping[str, Any]:
    return service.execute(
        job_id=store.item.job_id,
        execution_key=store.item.execution_key,
        attempt_id=uuid4(),
        worker_id="worker-a",
        progress=progress,
    )


def test_cleanup_executes_each_frozen_phase_once_and_confirms() -> None:
    store = FakeStore()
    service, local, media, p115, emby = _service(store)

    result = _execute(service, store, FakeProgress())

    assert result["status"] == "confirmed"
    assert store.terminal == "confirmed"
    assert len(local.calls) == len(media.calls) == 1
    assert media.calls[0][1] == f"p7-cleanup:{store.item.id}:media"
    assert p115.requests == emby.requests == 1
    assert p115.readback_calls == emby.readback_calls == 0


def test_cleanup_provider_unknown_restarts_in_readback_only_mode() -> None:
    store = FakeStore()
    p115 = FakeProvider(
        request=CleanupEffectResult.retryable({"type": "transport_unknown"}),
        readbacks=[CleanupEffectResult.confirmed({"absent": True})],
    )
    service, _local, _media, _p115, emby = _service(store, p115=p115)

    with pytest.raises(CleanupExecutionPending, match="authoritative recovery"):
        _execute(service, store, FakeProgress())
    result = _execute(service, store, FakeProgress())

    assert result["status"] == "confirmed"
    assert p115.requests == 1
    assert p115.readback_calls == 1
    assert emby.requests == 1


def test_cleanup_post_effect_cancellation_continues_authoritative_readback() -> None:
    store = FakeStore()
    p115 = FakeProvider(
        request=CleanupEffectResult.retryable({"type": "timeout_unknown"}),
        readbacks=[CleanupEffectResult.confirmed({"absent": True})],
    )
    service, *_ = _service(store, p115=p115)

    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    result = _execute(service, store, FakeProgress(cancelled=True))

    assert result["status"] == "confirmed"
    assert p115.requests == 1
    assert p115.readback_calls == 1


def test_cleanup_readback_budget_exhaustion_is_indeterminate_without_resend() -> None:
    store = FakeStore(max_attempts=3)
    p115 = FakeProvider(
        request=CleanupEffectResult.retryable({"type": "timeout_unknown"}),
        readbacks=[
            CleanupEffectResult.retryable({"type": "still_present"}),
            CleanupEffectResult.retryable({"type": "still_present"}),
        ],
    )
    service, *_ = _service(store, p115=p115, readback_budget=2)

    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    result = _execute(service, store, FakeProgress())

    assert result["status"] == "indeterminate"
    assert store.terminal == "indeterminate"
    assert p115.requests == 1
    assert p115.readback_calls == 2


def test_cleanup_restart_after_last_checkpointed_readback_does_not_exceed_budget() -> None:
    store = FakeStore()
    p115 = FakeProvider(
        request=CleanupEffectResult.retryable({"type": "timeout_unknown"}),
        readbacks=[CleanupEffectResult.retryable({"type": "readback_hung"})],
    )
    service, *_ = _service(store, p115=p115, readback_budget=2)

    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    receipt = deepcopy(dict(store.item.receipt or {}))
    receipt["phases"]["p115"]["status"] = "readback"
    receipt["phases"]["p115"]["readback_attempts"] = 2
    store.item = replace(store.item, receipt=receipt, effect_started=True)

    result = _execute(service, store, FakeProgress())

    assert result["status"] == "indeterminate"
    assert p115.requests == 1
    assert p115.readback_calls == 1


def test_cleanup_cancel_before_any_effect_never_calls_adapters() -> None:
    store = FakeStore()
    service, local, media, p115, emby = _service(store)

    result = _execute(service, store, FakeProgress(cancelled=True))

    assert result["status"] == "cancelled"
    assert store.terminal == "cancelled"
    assert local.calls == media.calls == []
    assert p115.requests == emby.requests == 0


def test_cleanup_timeout_before_any_effect_releases_item_without_calling_adapters() -> None:
    store = FakeStore()
    service, local, media, p115, emby = _service(store)

    with pytest.raises(TimeoutError, match="fixture timeout"):
        _execute(service, store, FakeProgress(timed_out=True))

    assert store.terminal is None
    assert store.item.status == "queued"
    assert store.item.effect_started is False
    assert local.calls == media.calls == []
    assert p115.requests == emby.requests == 0


def test_cleanup_post_effect_timeout_continues_authoritative_readback() -> None:
    store = FakeStore()
    p115 = FakeProvider(
        request=CleanupEffectResult.retryable({"type": "timeout_unknown"}),
        readbacks=[CleanupEffectResult.confirmed({"absent": True})],
    )
    service, *_ = _service(store, p115=p115)

    with pytest.raises(CleanupExecutionPending):
        _execute(service, store, FakeProgress())
    result = _execute(service, store, FakeProgress(timed_out=True))

    assert result["status"] == "confirmed"
    assert p115.requests == 1
    assert p115.readback_calls == 1


def test_local_cleanup_freezes_identity_deletes_sidecars_and_replays_absence(tmp_path: Any) -> None:
    media_dir = tmp_path / "Movie"
    media_dir.mkdir()
    media = media_dir / "Movie.mkv"
    subtitle = media_dir / "Movie.zh.srt"
    unrelated = media_dir / "Movie Extra.srt"
    media.write_bytes(b"fixture-media")
    subtitle.write_text("fixture-subtitle", encoding="utf-8")
    unrelated.write_text("keep", encoding="utf-8")
    policy = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
        )
    )
    target = policy.freeze_target(
        {
            "emby_id": "emby-old",
            "tmdb_id": "100",
            "item_type": "Movie",
            "files": [{"path": str(media), "pick_code": "", "p115": None}],
        }
    )
    gateway = SafeLocalCleanupGateway()

    first = gateway.reconcile(target)
    replay = gateway.reconcile(target)

    assert first.status.value == replay.status.value == "confirmed"
    assert not media.exists() and not subtitle.exists()
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_local_cleanup_refuses_replaced_file_and_symlink_escape(tmp_path: Any) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"original")
    policy = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
        )
    )
    target = policy.freeze_target(
        {
            "emby_id": "emby-old",
            "tmdb_id": "100",
            "item_type": "Movie",
            "files": [{"path": str(media), "pick_code": "", "p115": None}],
        }
    )
    media.unlink()
    media.write_bytes(b"replacement")

    result = SafeLocalCleanupGateway().reconcile(target)

    assert result.status.value == "indeterminate"
    assert media.read_bytes() == b"replacement"

    outside = tmp_path.parent / f"{tmp_path.name}-outside.mkv"
    outside.write_bytes(b"outside")
    with pytest.raises(ValueError, match="outside the configured root"):
        policy.freeze_target(
            {
                "emby_id": "emby-outside",
                "files": [{"path": str(outside), "pick_code": "", "p115": None}],
            }
        )


def test_local_cleanup_atomic_quarantine_preserves_racing_replacement(tmp_path: Any, monkeypatch) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"original")
    policy = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
        )
    )
    target = policy.freeze_target(
        {
            "emby_id": "emby-old",
            "tmdb_id": "100",
            "item_type": "Movie",
            "files": [{"path": str(media), "pick_code": "", "p115": None}],
        }
    )
    real_rename = cleanup_adapters.os.rename
    raced = False

    def replace_before_quarantine(source, destination, **kwargs):
        nonlocal raced
        assert (
            cleanup_adapters.os.fstat(kwargs["src_dir_fd"]).st_dev
            == cleanup_adapters.os.fstat(kwargs["dst_dir_fd"]).st_dev
        )
        if not raced and source == media.name:
            raced = True
            media.unlink()
            media.write_bytes(b"replacement")
        return real_rename(source, destination, **kwargs)

    monkeypatch.setattr(cleanup_adapters.os, "rename", replace_before_quarantine)

    result = SafeLocalCleanupGateway().reconcile(target)

    assert raced is True
    assert result.status.value == "indeterminate"
    assert result.error == {
        "type": "local_target_identity_changed",
        "message": f"cleanup target identity changed: {media}",
    }
    assert media.read_bytes() == b"replacement"


def test_cleanup_media_receipt_preserves_shared_cleanup_compatibility() -> None:
    receipt = FakeMediaReceiptGateway()
    result = LegacyCleanupMediaGateway(cast(Any, receipt)).reconcile(
        {
            "emby_id": "emby-old",
            "title": "Fixture",
            "item_type": "Movie",
            "series_id": "",
        },
        operation_id="fixture-cleanup-operation",
    )

    assert result.status.value == "confirmed"
    assert receipt.calls[0]["local_only"] is False
    assert receipt.calls[0]["operation_id"] == "fixture-cleanup-operation"


def test_cleanup_media_receipt_never_retries_an_indeterminate_shared_effect() -> None:
    receipt = FakeMediaReceiptGateway(
        {
            "ok": False,
            "indeterminate": True,
            "error": "shared response lost",
        }
    )

    result = LegacyCleanupMediaGateway(cast(Any, receipt)).reconcile(
        {"emby_id": "emby-old", "title": "Fixture", "item_type": "Movie", "series_id": ""},
        operation_id="fixture-cleanup-operation",
    )

    assert result.status.value == "indeterminate"
    assert result.error == {
        "type": "media_cleanup_receipt_indeterminate",
        "message": "shared response lost",
    }


def test_native_cleanup_media_waits_for_same_shared_source_operation() -> None:
    class Lifecycle:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def delete_local_source(self, source_id: int, **values: Any) -> Any:
            self.calls.append({"source_id": source_id, **values})
            if len(self.calls) == 1:
                return SimpleNamespace(
                    status_code=202,
                    payload={
                        "success": True,
                        "accepted": True,
                        "status": "queued",
                        "operation_id": "fixture-sharing-operation",
                    },
                )
            return SimpleNamespace(
                status_code=200,
                payload={"success": True, "accepted": False, "status": "confirmed"},
            )

    lifecycle = Lifecycle()
    gateway = SqlAlchemyCleanupMediaGateway(cast(Any, lambda: None), cast(Any, lifecycle))
    evidence = {"operation_id": "fixture-cleanup"}

    pending = gateway._finish_shared_sources([7], "fixture-cleanup", evidence)
    confirmed = gateway._finish_shared_sources([7], "fixture-cleanup", evidence)

    assert pending.status.value == "retryable"
    assert pending.error == {"type": "cleanup_shared_source_pending"}
    assert confirmed.status.value == "confirmed"
    assert [call["idempotency_key"] for call in lifecycle.calls] == [
        "fixture-cleanup:shared-source:7",
        "fixture-cleanup:shared-source:7",
    ]


def test_cleanup_rejects_missing_p115_account_identity_before_delete(tmp_path: Any) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    policy = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
            p115_enable_sync_delete=True,
        )
    )
    identity = {
        "emby_id": "emby-old",
        "files": [
            {
                "path": str(media),
                "pick_code": "fixture-pickcode",
                "p115": {"fid": "501", "parent_id": "500", "account_id": ""},
            }
        ],
    }

    with pytest.raises(ValueError, match="account identity"):
        policy.freeze_target(identity)

    historical = {
        "p115": {
            "enabled": True,
            "nodes": [
                {
                    "fid": "501",
                    "parent_id": "500",
                    "account_id": "",
                    "pick_code": "fixture-pickcode",
                }
            ],
        }
    }
    result = LegacyCleanupP115Gateway().readback(historical)
    assert result.status.value == "indeterminate"
    assert result.error == {"type": "p115_snapshot_account_identity_missing"}


def test_cleanup_freezes_and_fences_p115_provider_credentials(tmp_path: Any) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    credentials = SimpleNamespace(
        load=lambda: P115CredentialSnapshot(
            access_token="fixture-access-token",
            refresh_token="fixture-refresh-token",
            account_id="fixture-account",
        )
    )
    target = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
            p115_enable_sync_delete=True,
            p115_api_priority="openapi",
        ),
        cast(Any, credentials),
    ).freeze_target(
        {
            "emby_id": "emby-old",
            "files": [
                {
                    "path": str(media),
                    "pick_code": "fixture-pickcode",
                    "p115": {
                        "fid": "501",
                        "parent_id": "500",
                        "account_id": "fixture-account",
                    },
                }
            ],
        }
    )

    assert target["p115"]["account_id"] == "fixture-account"
    assert target["p115"]["backend"] == "openapi"
    assert len(target["p115"]["credential_fingerprint"]) == 64
    assert "fixture-access-token" not in json.dumps(target)

    captured: dict[str, Any] = {}

    class Provider:
        @staticmethod
        def open(**values: Any) -> None:
            captured.update(values)
            raise P115MaintenanceTransportError("fixture credential drift")

    outcome = NativeCleanupP115Gateway(cast(Any, Provider())).request(target)

    assert outcome.status.value == "indeterminate"
    assert outcome.error is not None
    assert outcome.error["type"] == "p115_credential_revision_changed"
    assert captured == {
        "expected_account_id": "fixture-account",
        "expected_backend": "openapi",
        "expected_credential_fingerprint": target["p115"]["credential_fingerprint"],
    }


def test_native_cleanup_p115_deletes_exact_fid_once_and_recovers_by_listing() -> None:
    class Client:
        def __init__(self) -> None:
            self.present = {"501"}
            self.deleted: list[list[str]] = []

        def list_children(self, parent_id: str) -> tuple[Mapping[str, Any], ...]:
            assert parent_id == "500"
            return tuple({"fid": fid, "pid": parent_id} for fid in sorted(self.present))

        def delete_files(self, file_ids: list[str]) -> Mapping[str, Any]:
            self.deleted.append(list(file_ids))
            self.present.difference_update(file_ids)
            return {"state": True}

    class Provider:
        def __init__(self, client: Client) -> None:
            self.client = client
            self.accounts: list[str] = []

        def open(self, *, expected_account_id: str | None = None, expected_backend: str | None = None):
            assert expected_backend is None
            self.accounts.append(str(expected_account_id))
            return self.client

    client = Client()
    provider = Provider(client)
    gateway = NativeCleanupP115Gateway(cast(Any, provider))
    target = {
        "p115": {
            "enabled": True,
            "nodes": [
                {
                    "fid": "501",
                    "parent_id": "500",
                    "account_id": "fixture-account",
                    "pick_code": "fixture-pickcode",
                }
            ],
        }
    }

    requested = gateway.request(target)
    replay = gateway.readback(target)

    assert requested.status.value == replay.status.value == "confirmed"
    assert client.deleted == [["501"]]
    assert provider.accounts == ["fixture-account", "fixture-account"]


def test_native_cleanup_p115_fails_closed_for_unpinned_or_ambiguous_identity() -> None:
    class Provider:
        @staticmethod
        def open(**_values: Any) -> None:
            return None

    gateway = NativeCleanupP115Gateway(cast(Any, Provider()))
    unavailable = gateway.request(
        {
            "p115": {
                "enabled": True,
                "nodes": [{"fid": "501", "parent_id": "500", "account_id": "fixture-account"}],
            }
        }
    )
    ambiguous = gateway.readback(
        {
            "p115": {
                "enabled": True,
                "nodes": [
                    {"fid": "501", "parent_id": "500", "account_id": "account-a"},
                    {"fid": "502", "parent_id": "500", "account_id": "account-b"},
                ],
            }
        }
    )

    assert unavailable.status.value == "retryable"
    assert unavailable.error == {"type": "p115_delete_client_unavailable"}
    assert ambiguous.status.value == "indeterminate"
    assert ambiguous.error == {"type": "p115_snapshot_has_multiple_accounts"}


def test_native_cleanup_emby_notification_retries_with_readback_only(tmp_path: Any) -> None:
    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    settings = Settings(
        local_strm_root=str(tmp_path),
        emby_server_url="http://emby.fixture.invalid",
        emby_api_key=SecretStr("fixture-token"),
    )
    target = SettingsCleanupExecutionPolicy(lambda: settings).freeze_target(
        {
            "emby_id": "emby-old",
            "tmdb_id": "100",
            "item_type": "Movie",
            "files": [{"path": str(media), "pick_code": "", "p115": None}],
        }
    )
    calls: list[tuple[str, str]] = []
    readbacks = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal readbacks
        calls.append((request.method, request.url.path))
        assert request.headers.get("X-Emby-Token") == "fixture-token"
        if request.method == "POST":
            assert request.url.path == "/Library/Media/Updated"
            assert json.loads(request.content)["Updates"][0]["UpdateType"] == "Deleted"
            return httpx.Response(204)
        assert request.url.path == "/Items/emby-old"
        readbacks += 1
        return httpx.Response(200, json={"Id": "emby-old"}) if readbacks == 1 else httpx.Response(404)

    gateway = ConfiguredCleanupEmbyGateway(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    requested = gateway.request(target)
    recovered = gateway.readback(target)

    assert requested.status.value == "retryable"
    assert recovered.status.value == "confirmed"
    assert calls == [
        ("POST", "/Library/Media/Updated"),
        ("GET", "/Items/emby-old"),
        ("GET", "/Items/emby-old"),
    ]


def test_p115_presence_readback_falls_back_to_fid_after_parent_move() -> None:
    from handler.p115_service import _p115_remote_node_exists

    class Client:
        @staticmethod
        def fs_files(_params: Mapping[str, Any]) -> Mapping[str, Any]:
            return {"state": True, "data": []}

        @staticmethod
        def fs_get_info(node_id: str) -> Mapping[str, Any]:
            return {"state": True, "data": {"fid": node_id, "pid": "700"}}

    assert _p115_remote_node_exists(Client(), "501", "500") is True


def test_cleanup_submission_requires_local_path_and_emby_authorization(tmp_path: Any) -> None:
    missing_path = SettingsCleanupExecutionPolicy(
        lambda: Settings(
            local_strm_root=str(tmp_path),
            emby_server_url="http://emby.fixture.invalid",
            emby_api_key=SecretStr("fixture-token"),
        )
    )
    with pytest.raises(ValueError, match="frozen local path"):
        missing_path.freeze_target({"emby_id": "emby-old", "files": []})

    media = tmp_path / "Movie.mkv"
    media.write_bytes(b"fixture")
    missing_auth = SettingsCleanupExecutionPolicy(lambda: Settings(local_strm_root=str(tmp_path)))
    with pytest.raises(ValueError, match="Emby authorization"):
        missing_auth.freeze_target(
            {
                "emby_id": "emby-old",
                "files": [{"path": str(media), "pick_code": "", "p115": None}],
            }
        )
