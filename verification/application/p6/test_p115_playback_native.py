from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest

from etk.domains.p115.maintenance_transport import HttpxP115BoundedDownloadGateway
from etk.domains.p115.media_info_adapters import P115DirectUrl
from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.p115.playback import (
    P115PlaybackConfigService,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackProviderDisposition,
    normalize_pool_config,
    play_pool_business_date,
)
from etk.domains.p115.playback_native import (
    NativeP115PlaybackProvider,
    NativeP115PlaybackRapidSigner,
    P115PlaybackCredentialChanged,
    P115PlayPoolCredentialStore,
    P115PoolPlaybackClient,
)
from etk.domains.p115.playback_reward_repository import SqlAlchemyP115PlaybackUserGateway
from etk.domains.p115.playback_service import P115PlaybackService
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


def _operation(
    *,
    session: dict[str, Any] | None = None,
    kind: PlaybackOperationKind = PlaybackOperationKind.SPEEDTEST,
) -> PlaybackOperation:
    now = datetime.now(UTC)
    return PlaybackOperation(
        id=uuid4(),
        sequence=1,
        operation_key="1" * 64,
        request_hash=b"r" * 32,
        kind=kind,
        mode=PlaybackMode.MAINTENANCE,
        account_id="local-pool-account-uuid",
        provider_backend="openapi",
        request={
            "account_kind": "pool",
            "target_cid": "target-cid",
            "credential_hash": "c" * 64,
            "account_revision": "d" * 64,
            "source": SOURCE,
        },
        status=PlaybackOperationStatus.CLAIMED,
        state_revision=1,
        attempts=1,
        available_at=now,
        claimed_by="fixture-worker",
        claim_token=uuid4(),
        claimed_at=now,
        lease_expires_at=now,
        provider_request_started_at=now if session else None,
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
        self.exists = False
        self.rapid_calls: list[tuple[str, str]] = []
        self.resolve_calls = 0
        self.measure_calls = 0
        self.delete_calls = 0
        self.rapid_exception = False
        self.require_sign = False
        self.measure_exception = False
        self.delete_result: bool | None = True

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


@pytest.mark.parametrize("kind", [PlaybackOperationKind.PREPARE, PlaybackOperationKind.RECLAIM])
def test_native_daily_provider_fails_closed_without_opening_unsupported_recovery_clients(
    kind: PlaybackOperationKind,
) -> None:
    class FailIfOpenedFactory:
        def open(self, _operation: PlaybackOperation) -> P115PoolPlaybackClient:
            raise AssertionError("unsupported recovery must not open a provider client")

    provider = NativeP115PlaybackProvider(cast(Any, FailIfOpenedFactory()), FakeSigner())

    outcome = provider.execute(_operation(kind=kind), checkpoint=lambda _value: None)

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert outcome.request_sent is False
    assert outcome.error == {
        "type": "native_playback_recovery_unsupported",
        "message": f"原生 Worker 未执行 {kind.value} provider 写入，操作需保守对账",
    }


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


class FakeRewardRepository:
    def __init__(self) -> None:
        self.grants: list[dict[str, Any]] = []

    def display_names(self, user_ids: list[str]) -> dict[str, str]:
        return {user_id: f"User {user_id}" for user_id in user_ids}

    def expand_user_ids(self, user_ids: list[str]) -> tuple[str, ...]:
        return tuple(user_ids)

    def user_chat_id(self, user_id: str) -> str:
        return f"chat-{user_id}"

    def admin_chat_ids(self) -> tuple[str, ...]:
        return ("admin-chat",)

    def grant_once(self, user_id: str, **values: Any) -> dict[str, Any]:
        self.grants.append({"user_id": user_id, **values})
        return {"granted": True, "expiration_date": "2026-09-01T00:00:00+00:00"}

    def summary(self, _user_id: str) -> dict[str, Any]:
        return {"total_days": 0.8}


class FailingTelegram:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, bool]] = []

    def send_message(self, chat_id: str, text: str, *, disable_notification: bool = False) -> object:
        self.calls.append((chat_id, text, disable_notification))
        raise RuntimeError("fixture telegram failure")


def test_native_reward_keeps_local_business_date_and_telegram_failure_is_best_effort() -> None:
    repository = FakeRewardRepository()
    telegram = FailingTelegram()
    gateway = SqlAlchemyP115PlaybackUserGateway(
        cast(Any, repository),
        telegram,
        clock=lambda: datetime(2026, 8, 30, 16, 30, tzinfo=UTC),
    )

    result = gateway.grant_reward(
        {
            "id": "local-pool-account-uuid",
            "alias": "Fixture",
            "owner_type": "user",
            "owner_user_id": "fixture-user",
            "shared": True,
            "enabled": True,
            "access_token": "fixture-token",
        },
        notify_user=True,
        speed_text="8.00 MB/s",
        error_text="",
    )
    gateway.notify_daily_results(
        [{"success": True, "alias": "Fixture", "owner_name": "Fixture User", "reward_days": 0.8}]
    )

    assert result["rewarded"] is True
    assert result["reward_days"] == 0.8
    assert repository.grants == [
        {
            "user_id": "fixture-user",
            "reward_date": "2026-08-31",
            "reward_days": 0.8,
            "reward_mode": "shared",
        }
    ]
    assert telegram.calls[0][0] == "chat-fixture-user"
    assert telegram.calls[-1][0] == "admin-chat"
    assert telegram.calls[-1][2] is True


def test_native_reward_ignores_admin_owned_accounts() -> None:
    repository = FakeRewardRepository()
    gateway = SqlAlchemyP115PlaybackUserGateway(cast(Any, repository), FailingTelegram())

    result = gateway.grant_reward(
        {"owner_type": "admin", "enabled": True, "cookie": "fixture-cookie"},
        notify_user=True,
        speed_text="8.00 MB/s",
        error_text="",
    )

    assert result == {"rewarded": False, "reward_days": 0.0, "summary": {}}
    assert repository.grants == []


class DailyConfig:
    def __init__(self) -> None:
        self.accounts = [
            {
                "id": "account-ok",
                "alias": "OK",
                "access_token": "token-ok",
                "enabled": True,
                "owner_type": "user",
                "owner_user_id": "user-ok",
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

    def grant_reward(self, account: Mapping[str, Any], **_values: Any) -> dict[str, Any]:
        return {"reward_days": 0.5 if account.get("owner_type") == "user" and account.get("enabled") else 0.0}

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


def test_native_daily_reward_failure_does_not_disable_a_successful_speedtest_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RewardFailureConfig(DailyConfig):
        def grant_reward(self, _account: Mapping[str, Any], **_values: Any) -> dict[str, Any]:
            raise RuntimeError("fixture reward database unavailable")

    config = RewardFailureConfig()
    config.accounts = [config.accounts[0]]
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

    assert config.patches == []
    assert config.accounts[0]["enabled"] is True
    assert result["status"] == "partial"
    assert result["failed_count"] == 1
    assert result["results"][0]["success"] is True
    assert result["results"][0]["reward_error"] == "奖励持久化失败"


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
