from __future__ import annotations

import gzip
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest

from etk.compatibility.playback import NativePlaybackCompatibilityGateway
from etk.domains.p115.playback import (
    PLAY_POOL_CONFIG_KEY,
    P115PlaybackConfigService,
    PlaybackArtifact,
    PlaybackDirectResponse,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationIntent,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackProviderDisposition,
    PlaybackProviderOutcome,
    PlaybackUpstreamStream,
    normalize_pool_config,
    safe_playback_json,
)
from etk.domains.p115.playback_adapters import LegacyP115DirectLinkGateway, LegacyP115PlaybackProvider
from etk.domains.p115.playback_service import P115PlaybackService

_SOURCE_SHA1 = "A" * 40


class MemoryPlaybackConfigBackend:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 17, tzinfo=UTC)
        self.state = normalize_pool_config(
            {
                "enabled": True,
                "accounts": [
                    {"id": "admin-pool", "cookie": "fixture-admin-cookie", "owner_type": "admin", "enabled": True},
                    {
                        "id": "historical-contribution",
                        "cookie": "fixture-user-cookie",
                        "owner_type": "user",
                        "owner_user_id": "fixture-friend",
                        "shared": True,
                        "enabled": True,
                    },
                ],
            },
            now=self.now,
        )

    def mutate(self, setting_key: str, mutation: Any) -> Any:
        assert setting_key == PLAY_POOL_CONFIG_KEY
        next_state, result, should_write = mutation(deepcopy(self.state), self.now)
        if should_write:
            self.state = deepcopy(next_state)
        return deepcopy(result)


def _pool_config(backend: MemoryPlaybackConfigBackend, notifications: list[Any]) -> P115PlaybackConfigService:
    users = SimpleNamespace(
        display_name=lambda _user_id: "Fixture",
        expand_allowed_user_ids=lambda ids: ids,
        notify_daily_results=lambda results: notifications.append(deepcopy(results)),
    )
    return P115PlaybackConfigService(cast(Any, backend), cast(Any, users))


def test_historical_contributions_are_inactive_without_deleting_stored_credentials() -> None:
    backend = MemoryPlaybackConfigBackend()
    stored = deepcopy(backend.state)
    config = _pool_config(backend, [])

    assert [account["id"] for account in config.snapshot()["accounts"]] == ["admin-pool"]
    assert [account["id"] for account in config.public_config()["accounts"]] == ["admin-pool"]
    assert config.account("historical-contribution") is None
    selected = config.select_account(user_id="fixture-friend", own_account_only=False, active_counts={})
    assert selected is not None and selected["id"] == "admin-pool"
    assert config.select_account(user_id="fixture-friend", own_account_only=True, active_counts={}) is None
    assert backend.state == stored

    config.save_settings({"enabled": False})
    assert backend.state["accounts"] == stored["accounts"]


def test_administrator_api_cannot_recreate_or_reactivate_user_contributions() -> None:
    backend = MemoryPlaybackConfigBackend()
    stored = deepcopy(backend.state)
    config = _pool_config(backend, [])
    with pytest.raises(ValueError, match="用户贡献播放账号功能已移除"):
        config.upsert_account({"owner_type": "user", "owner_user_id": "fixture-friend", "cookie": "fixture"})
    with pytest.raises(ValueError, match="用户贡献播放账号功能已移除"):
        config.upsert_account({"enabled": True}, account_id="historical-contribution")
    assert backend.state == stored


def test_daily_speedtest_only_uses_administrator_accounts_and_has_no_reward_path(monkeypatch) -> None:
    backend = MemoryPlaybackConfigBackend()
    stored = deepcopy(backend.state)
    notifications: list[Any] = []
    config = _pool_config(backend, notifications)
    unused = cast(Any, object())
    service = P115PlaybackService(
        config=config, repository=unused, sources=unused, direct_links=unused, executor=unused,
    )
    tested: list[str] = []

    def speedtest(account_id: str, *, intent_key: str) -> dict[str, Any]:
        tested.append(account_id)
        return {"speed_text": "1 MB/s"}

    monkeypatch.setattr(service, "speedtest_account", speedtest)
    progress = SimpleNamespace(update=lambda *_args: None, checkpoint=lambda: None)
    result = service.daily_speedtest(cast(Any, progress))

    assert tested == ["admin-pool"]
    assert result["failed_count"] == 0
    assert len(result["results"]) == 1
    assert not any(key.startswith("reward") for key in result["results"][0])
    assert notifications == [result["results"]]
    assert backend.state == stored


class FakeDirectLinks:
    def __init__(self) -> None:
        self.resolve_calls: list[tuple[str, str]] = []
        self.stream_calls: list[tuple[str, str, str]] = []

    def resolve_primary(self, pick_code: str, user_agent: str) -> str:
        self.resolve_calls.append((pick_code, user_agent))
        return "https://media.fixture.invalid/signed"

    def resolve_artifact(self, artifact: PlaybackArtifact, user_agent: str) -> str:
        del artifact, user_agent
        return ""

    def stream(self, url: str, *, user_agent: str, range_header: str = "") -> PlaybackUpstreamStream:
        self.stream_calls.append((url, user_agent, range_header))
        return PlaybackUpstreamStream(206, {"content-range": "bytes 4-9/10"}, (b"fixture",), lambda: None)


def _direct_service(direct_links: FakeDirectLinks) -> P115PlaybackService:
    unused = cast(Any, object())
    return P115PlaybackService(
        config=unused,
        repository=unused,
        sources=unused,
        direct_links=direct_links,
        executor=unused,
        sleep=lambda _seconds: None,
    )


@pytest.mark.parametrize("user_agent", ["", "Emby/4.8", "Jellyfin/10", "Lavf/60", "Kodi/21"])
def test_direct_play_server_user_agents_proxy_with_fixed_ua_and_range(user_agent: str) -> None:
    direct = FakeDirectLinks()

    result = _direct_service(direct).direct_play("fixture-pick", user_agent, "bytes=4-9")

    assert isinstance(result, PlaybackDirectResponse)
    assert result.kind == "stream"
    assert result.status_code == 206
    assert len(direct.resolve_calls) == 1
    resolved_ua = direct.resolve_calls[0][1]
    assert "Chrome/124.0.0.0" in resolved_ua
    assert direct.stream_calls == [("https://media.fixture.invalid/signed", resolved_ua, "bytes=4-9")]


def test_direct_play_regular_client_redirects_without_opening_upstream_stream() -> None:
    direct = FakeDirectLinks()

    result = _direct_service(direct).direct_play("fixture-pick", "FixturePlayer/1.0", "bytes=4-9")

    assert result.kind == "redirect"
    assert result.status_code == 302
    assert result.location == "https://media.fixture.invalid/signed"
    assert direct.resolve_calls == [("fixture-pick", "FixturePlayer/1.0")]
    assert direct.stream_calls == []


def test_operation_json_rejects_signed_urls_at_any_depth() -> None:
    with pytest.raises(ValueError, match="forbidden secret field 'signed_url'"):
        PlaybackOperationIntent.create(
            semantic_key="fixture-secret-rejection",
            kind=PlaybackOperationKind.PREPARE,
            mode=PlaybackMode.COPY,
            account_id="fixture-account",
            provider_backend="openapi",
            request={"source": {"signed_url": "https://media.fixture.invalid/signed"}},
        )


@pytest.mark.parametrize(
    "field",
    [
        "operation request",
        "provider session",
        "remote receipt",
        "readback",
        "operation result",
        "operation error",
        "artifact receipt",
    ],
)
@pytest.mark.parametrize("secret_key", ["signed_url", "download_url", "access_token", "cookie"])
def test_every_ordinary_playback_json_carrier_rejects_urls_and_credentials(
    field: str,
    secret_key: str,
) -> None:
    with pytest.raises(ValueError, match="forbidden secret field"):
        safe_playback_json(
            {"nested": [{"identity": "fixture", secret_key: "fixture-secret-value"}]},
            field=field,
        )


def test_compatibility_gateway_keeps_signed_urls_out_of_prepare_results() -> None:
    requests: list[tuple[object, bool, str]] = []
    resolved: list[tuple[str, str]] = []
    prepared = SimpleNamespace(
        artifact=SimpleNamespace(
            id=uuid4(),
            remote_pick_code="fixture-native-pick",
        ),
        lease=SimpleNamespace(
            id=uuid4(),
            play_session_id="fixture-session",
            item_id="fixture-item",
            owner_user_id="fixture-user",
        ),
    )

    def prepare(
        request: object,
        *,
        force_new: bool = False,
        failed_remote_pick_code: str = "",
    ) -> object:
        requests.append((request, force_new, failed_remote_pick_code))
        return prepared

    def resolve(lease_id: str, user_agent: str) -> str:
        resolved.append((lease_id, user_agent))
        return "https://media.fixture.invalid/signed"

    service = SimpleNamespace(
        prepare_play=prepare,
        resolve_lease=resolve,
    )
    gateway = NativePlaybackCompatibilityGateway(lambda: cast(Any, service))

    pool = gateway.prepare_pool("fixture-source", play_session_id="fixture-session")
    copied = gateway.prepare_copy(
        "fixture-source",
        force_new=True,
        failed_pick_code="fixture-stale-native-pick",
    )

    assert copied == "fixture-native-pick"
    assert pool == {
        "_native_playback": True,
        "pick_code": "fixture-native-pick",
        "lease_id": str(prepared.lease.id),
        "artifact_id": str(prepared.artifact.id),
        "session": {
            "lease_id": str(prepared.lease.id),
            "play_session_id": "fixture-session",
            "item_id": "fixture-item",
            "user_id": "fixture-user",
        },
    }
    assert "url" not in repr(pool).casefold()
    assert gateway.pool_direct_url(pool, user_agent="FixturePlayer/1.0") == ("https://media.fixture.invalid/signed")
    assert resolved == [(str(prepared.lease.id), "FixturePlayer/1.0")]
    assert [
        (cast(Any, request).mode, force_new, failed_pick_code) for request, force_new, failed_pick_code in requests
    ] == [
        (PlaybackMode.POOL, False, ""),
        (PlaybackMode.COPY, True, "fixture-stale-native-pick"),
    ]


class PaginatedClient:
    def __init__(self) -> None:
        self.offsets: list[int] = []

    def fs_files_backend(self, payload: dict[str, Any], *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        offset = int(payload["offset"])
        self.offsets.append(offset)
        if offset == 0:
            return {
                "state": True,
                "count": 1001,
                "data": [
                    {
                        "fid": f"fixture-filler-{index}",
                        "pick_code": f"fixture-filler-pick-{index}",
                        "name": f"Filler-{index}.mkv",
                        "sha1": "B" * 40,
                        "size": 10,
                    }
                    for index in range(1000)
                ],
            }
        return {
            "state": True,
            "count": 1001,
            "data": [
                {
                    "fid": "fixture-target-fid",
                    "pick_code": "fixture-target-pick",
                    "name": "Target.mkv",
                    "sha1": _SOURCE_SHA1,
                    "size": 100,
                }
            ],
        }


class FixedClientProvider(LegacyP115PlaybackProvider):
    def __init__(self, client: object) -> None:
        super().__init__(cast(P115PlaybackConfigService, object()))
        self.client = client

    def _client(self, _operation: PlaybackOperation) -> Any:
        return self.client


def _operation(
    kind: PlaybackOperationKind,
    request: dict[str, Any],
    *,
    mode: PlaybackMode = PlaybackMode.COPY,
) -> PlaybackOperation:
    now = datetime.now(UTC)
    return PlaybackOperation(
        id=uuid4(),
        sequence=1,
        operation_key="1" * 64,
        request_hash=b"r" * 32,
        kind=kind,
        mode=mode,
        account_id="fixture-account",
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
        provider_request_started_at=None,
        provider_session=None,
        remote_receipt=None,
        readback=None,
        result=None,
        error=None,
        finished_at=None,
        created_at=now,
        updated_at=now,
    )


def test_prepare_readback_paginates_beyond_first_thousand_files() -> None:
    client = PaginatedClient()
    operation = _operation(
        PlaybackOperationKind.PREPARE,
        {
            "target_cid": "fixture-target-cid",
            "file_name": "Target.mkv",
            "source": {"sha1": _SOURCE_SHA1, "size": 100, "pick_code": "fixture-source-pick"},
        },
    )

    outcome = FixedClientProvider(client).readback(operation)

    assert outcome.disposition is PlaybackProviderDisposition.CONFIRMED
    assert outcome.receipt == {
        "target_cid": "fixture-target-cid",
        "remote_fid": "fixture-target-fid",
        "remote_pick_code": "fixture-target-pick",
        "file_name": "Target.mkv",
        "created_remote": False,
    }
    assert client.offsets == [0, 1000]


class MovedFileClient:
    def __init__(self) -> None:
        self.info_ids: list[str] = []
        self.deleted_ids: list[str] = []

    def fs_get_info_backend(self, remote_fid: str, *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        self.info_ids.append(remote_fid)
        return {"state": True, "data": {"fid": remote_fid, "parent_id": "moved-parent"}}

    def fs_delete_backend(self, remote_fids: list[str], *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        self.deleted_ids.extend(remote_fids)
        return {"state": True}


def test_reclaim_readback_uses_fixed_fid_and_does_not_treat_move_as_delete() -> None:
    client = MovedFileClient()
    operation = _operation(
        PlaybackOperationKind.RECLAIM,
        {"remote_fid": "fixture-moved-fid", "source": {"sha1": _SOURCE_SHA1, "size": 100}},
    )
    checkpoints: list[dict[str, Any]] = []

    outcome = FixedClientProvider(client).execute(operation, checkpoint=lambda value: checkpoints.append(dict(value)))

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert outcome.error == {
        "type": "reclaim_delete_unknown",
        "message": "播放临时文件删除后无法确认结果",
    }
    assert checkpoints == [
        {
            "account_id": "fixture-account",
            "backend": "openapi",
            "phase": "delete",
            "kind": "reclaim",
        }
    ]
    assert client.deleted_ids == ["fixture-moved-fid"]
    assert client.info_ids == ["fixture-moved-fid", "fixture-moved-fid"]


class CheckpointThenRaiseClient:
    def fs_files_backend(self, _payload: dict[str, Any], *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        return {"state": True, "count": 0, "data": []}

    def fs_copy_backend(self, _source_fids: list[str], _target_cid: str, *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        raise RuntimeError("fixture copy request failed")


def test_provider_exception_after_checkpoint_uses_the_durable_request_projection() -> None:
    operation = replace(
        _operation(
            PlaybackOperationKind.PREPARE,
            {
                "target_cid": "fixture-target-cid",
                "file_name": "Fixture.mkv",
                "source": {
                    "fid": "fixture-source-fid",
                    "sha1": _SOURCE_SHA1,
                    "size": 100,
                },
            },
        ),
        provider_session={"phase": "stale-projection"},
    )
    checkpoints: list[dict[str, Any]] = []

    outcome = FixedClientProvider(CheckpointThenRaiseClient()).execute(
        operation,
        checkpoint=lambda value: checkpoints.append(dict(value)),
    )

    expected_session = {
        "account_id": "fixture-account",
        "backend": "openapi",
        "phase": "copy",
        "kind": "prepare",
    }
    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert outcome.request_sent is True
    assert outcome.session == expected_session
    assert checkpoints == [expected_session]


class SpeedtestClient:
    def __init__(self) -> None:
        self.exists = False
        self.rapid_upload_calls = 0
        self.deleted_ids: list[str] = []

    def fs_files_backend(self, _payload: dict[str, Any], *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        items = []
        if self.exists:
            items.append(
                {
                    "fid": "fixture-speedtest-fid",
                    "pick_code": "fixture-speedtest-pick",
                    "name": "Fixture-Speedtest.mkv",
                    "sha1": _SOURCE_SHA1,
                    "size": 100,
                }
            )
        return {"state": True, "count": len(items), "data": items}

    def rapid_upload(self, _payload: dict[str, Any]) -> dict[str, Any]:
        self.rapid_upload_calls += 1
        self.exists = True
        return {"state": True}

    def fs_get_info_backend(self, remote_fid: str, *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        if self.exists:
            return {"state": True, "data": {"fid": remote_fid}}
        return {"state": False, "code": 404, "message": "not found"}

    def fs_delete_backend(self, remote_fids: list[str], *, backend: str) -> dict[str, Any]:
        assert backend == "openapi"
        self.deleted_ids.extend(remote_fids)
        self.exists = False
        return {"state": True}


class FlakySpeedtestProvider(FixedClientProvider):
    def __init__(self, client: SpeedtestClient) -> None:
        super().__init__(client)
        self.measure_calls = 0

    def _resolve_operation_url(self, _operation: PlaybackOperation, _pick_code: str, _user_agent: str) -> str:
        return "https://media.fixture.invalid/signed?token=fixture-secret"

    def _measure(self, url: str, **_options: Any) -> dict[str, Any]:
        self.measure_calls += 1
        if self.measure_calls == 1:
            raise RuntimeError(f"fixture download failed: {url}")
        return {
            "bytes": 1024,
            "seconds": 1.0,
            "bps": 1024,
            "speed_text": "1.00 KB/s",
        }


def test_speedtest_checkpoints_remote_fid_and_retries_cleanup_without_persisting_signed_url() -> None:
    client = SpeedtestClient()
    provider = FlakySpeedtestProvider(client)
    operation = _operation(
        PlaybackOperationKind.SPEEDTEST,
        {
            "account_kind": "pool",
            "target_cid": "fixture-target-cid",
            "source": {
                "sha1": _SOURCE_SHA1,
                "size": 100,
                "preid": "B" * 40,
                "pick_code": "fixture-source-pick",
                "file_name": "Fixture-Speedtest.mkv",
            },
        },
        mode=PlaybackMode.MAINTENANCE,
    )
    checkpoints: list[dict[str, Any]] = []

    failed = provider.execute(operation, checkpoint=lambda value: checkpoints.append(dict(value)))

    assert failed.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert failed.request_sent is True
    assert failed.error == {"type": "speedtest_download_failed", "message": "测速下载失败"}
    assert "signed" not in repr(failed).casefold()
    assert "fixture-secret" not in repr(failed)
    assert checkpoints[-1] == {
        "account_id": "fixture-account",
        "backend": "openapi",
        "phase": "speedtest",
        "kind": "speedtest",
        "remote_fid": "fixture-speedtest-fid",
    }

    durable = replace(
        operation,
        provider_request_started_at=datetime.now(UTC),
        provider_session=checkpoints[-1],
    )
    readback = provider.readback(durable)
    retry_checkpoints: list[dict[str, Any]] = []
    retried = provider.execute(durable, checkpoint=lambda value: retry_checkpoints.append(dict(value)))

    assert readback.disposition is PlaybackProviderDisposition.RETRYABLE
    assert readback.error == {"type": "delete_not_applied", "message": "权威回读确认播放临时文件仍存在"}
    assert retried.disposition is PlaybackProviderDisposition.CONFIRMED
    assert client.rapid_upload_calls == 1
    assert client.deleted_ids == ["fixture-speedtest-fid"]
    assert client.exists is False


class ExplodingSpeedtestProvider(FixedClientProvider):
    def _readback_prepare(self, _operation: PlaybackOperation) -> dict[str, Any] | None:
        raise RuntimeError("https://media.fixture.invalid/signed?token=fixture-secret")

    def _readback_delete_phase(self, _operation: PlaybackOperation) -> PlaybackProviderOutcome:
        raise RuntimeError("https://media.fixture.invalid/signed?token=fixture-secret")


def test_speedtest_provider_exception_uses_a_stable_safe_error() -> None:
    operation = _operation(
        PlaybackOperationKind.SPEEDTEST,
        {"target_cid": "fixture-target-cid", "source": {"sha1": _SOURCE_SHA1, "size": 100}},
        mode=PlaybackMode.MAINTENANCE,
    )

    provider = ExplodingSpeedtestProvider(object())
    outcome = provider.execute(operation, checkpoint=lambda _value: None)
    readback = provider.readback(operation)

    assert outcome.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert outcome.error == {"type": "speedtest_provider_exception", "message": "测速执行失败"}
    assert "signed" not in repr(outcome).casefold()
    assert "fixture-secret" not in repr(outcome)
    assert readback.disposition is PlaybackProviderDisposition.INDETERMINATE
    assert readback.error == {"type": "speedtest_readback_failed", "message": "测速清理回读失败"}
    assert "signed" not in repr(readback).casefold()
    assert "fixture-secret" not in repr(readback)


def test_direct_link_stream_requests_identity_and_drops_encoded_content_length() -> None:
    decoded = b"fixture-decoded-payload" * 8
    encoded = gzip.compress(decoded)
    request_headers: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        request_headers.update(request.headers)
        return httpx.Response(
            206,
            headers={
                "content-encoding": "gzip",
                "content-length": str(len(encoded)),
                "content-range": "bytes 0-23/24",
            },
            content=encoded,
            request=request,
        )

    def client_factory(**options: Any) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(handler), **options)

    gateway = LegacyP115DirectLinkGateway(
        cast(P115PlaybackConfigService, object()),
        client_factory=client_factory,
    )
    stream = gateway.stream(
        "https://media.fixture.invalid/signed",
        user_agent="FixturePlayer/1.0",
        range_header="bytes=0-23",
    )
    try:
        body = b"".join(stream.chunks)
    finally:
        stream.close()

    assert request_headers["accept-encoding"] == "identity"
    assert request_headers["range"] == "bytes=0-23"
    assert body == decoded
    assert "content-encoding" not in stream.headers
    assert "content-length" not in stream.headers
    assert stream.headers["content-range"] == "bytes 0-23/24"


class FailingStreamClient:
    def __init__(self) -> None:
        self.close_calls = 0

    def build_request(self, *_args: Any, **_kwargs: Any) -> object:
        return object()

    def send(self, _request: object, *, stream: bool) -> Any:
        assert stream is True
        raise RuntimeError("fixture upstream connect failed")

    def close(self) -> None:
        self.close_calls += 1


def test_direct_link_stream_closes_client_when_upstream_send_fails() -> None:
    client = FailingStreamClient()
    gateway = LegacyP115DirectLinkGateway(
        cast(P115PlaybackConfigService, object()),
        client_factory=lambda **_options: cast(Any, client),
    )

    with pytest.raises(RuntimeError, match="fixture upstream connect failed"):
        gateway.stream(
            "https://media.fixture.invalid/signed",
            user_agent="FixturePlayer/1.0",
        )

    assert client.close_calls == 1
