from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from etk.domains.sharing.operations import (
    SharingOperationConflictError,
    SharingOperationKind,
    SharingOperationStatus,
    sharing_operation_key,
)
from etk.domains.sharing.virtual_import import freeze_virtual_import_source
from etk.domains.subscription.auto_subscribe import AutoSourceDisposition, AutoSubscriptionInterrupted
from etk.integrations import (
    ProviderDisposition,
    ProviderResponse,
    TelegramUserBotIdempotencyConflictError,
)
from etk.platform.jobs import PersistentExecutionCancelled
from etk.processes.p7_auto_subscribe_sources import (
    AutoSubscriptionTransferOperationDelivery,
    NativeP115ResourceTransferGateway,
    PersistentOrderedAutoSubscriptionSources,
    _candidate_digest,
)


class _Progress:
    def __init__(
        self,
        *,
        fail_checkpoint: int | None = None,
        checkpoint_error: BaseException | None = None,
    ) -> None:
        self.checkpoints = 0
        self.fail_checkpoint = fail_checkpoint
        self.checkpoint_error = checkpoint_error

    def update(self, progress: int, message: str) -> None:
        del progress, message

    def checkpoint(self) -> None:
        self.checkpoints += 1
        if self.checkpoints == self.fail_checkpoint:
            if self.checkpoint_error is not None:
                raise self.checkpoint_error
            raise RuntimeError("parent execution interrupted")


class _Center:
    def __init__(self, candidate: Mapping[str, Any] | Exception) -> None:
        self.candidate = candidate

    def list_sources(self, **kwargs: Any) -> ProviderResponse[dict[str, Any]]:
        del kwargs
        if isinstance(self.candidate, Exception):
            raise self.candidate
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"items": [dict(self.candidate)]})


class _HDHive:
    def __init__(self, candidate: Mapping[str, Any] | Exception) -> None:
        self.candidate = candidate
        self.unlock_calls: list[str] = []

    def get_resources(self, *args: Any, **kwargs: Any) -> ProviderResponse[dict[str, Any]]:
        del args, kwargs
        if isinstance(self.candidate, Exception):
            raise self.candidate
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"resources": [dict(self.candidate)]})

    def unlock(self, slug: str) -> ProviderResponse[dict[str, Any]]:
        self.unlock_calls.append(slug)
        return ProviderResponse(
            ProviderDisposition.CONFIRMED,
            200,
            {"resource": {"share_code": "privateshare", "receive_code": "2468"}},
        )


class _Telegram:
    def __init__(self, candidate: Mapping[str, Any] | Exception) -> None:
        self.candidate = candidate
        self.enqueue_error: Exception | None = None
        self.wait_error: Exception | None = None
        self.enqueue_calls: list[tuple[str, dict[str, Any], str, str | None]] = []
        self.command_id = "stable-command-id"
        self.result_reference = "a" * 64
        self.result_owner_key = ""

    @property
    def raw_result(self) -> dict[str, Any]:
        if isinstance(self.candidate, Exception):
            raise self.candidate
        return {"results": [dict(self.candidate)]}

    def enqueue_once(
        self,
        command_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        idempotency_key: str,
        result_owner_key: str | None = None,
    ) -> str:
        if self.enqueue_error is not None:
            raise self.enqueue_error
        self.result_owner_key = str(result_owner_key or "")
        self.enqueue_calls.append((command_type, dict(payload or {}), idempotency_key, result_owner_key))
        return self.command_id

    def wait(self, command_id: str, **kwargs: Any) -> Mapping[str, Any]:
        del kwargs
        assert command_id == self.command_id
        if self.wait_error is not None:
            raise self.wait_error
        raw_result = self.raw_result
        return {
            "id": command_id,
            "status": "succeeded",
            "result": {
                "ok": True,
                "search_result_ref": self.result_reference,
                "search_result_hash": _candidate_digest(raw_result),
            },
        }

    def get_command(self, command_id: str) -> Mapping[str, Any] | None:
        return self.wait(command_id)

    def consume_resource_secret(self, reference: str, owner_key: str) -> Mapping[str, Any]:
        assert reference == self.result_reference
        assert owner_key == self.result_owner_key
        return self.raw_result


class _VirtualImports:
    def freeze(self, candidate: Mapping[str, Any]) -> Any:
        return SimpleNamespace(
            source_kind=str(candidate["source_kind"]),
            source_id=str(candidate["source_id"]),
            source=dict(candidate),
            state_hash="a" * 64,
        )


class _ValidatingVirtualImports:
    @staticmethod
    def freeze(candidate: Mapping[str, Any]) -> Any:
        return freeze_virtual_import_source(candidate)


class _Operations:
    def __init__(self) -> None:
        self.intents: list[Any] = []
        self.operation: Any | None = None
        self.cancelled: list[Any] = []

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        self.intents.append(intent)
        if self.operation is not None:
            if (
                self.operation.operation_key != intent.operation_key
                or self.operation.request_hash != intent.request_hash
                or self.operation.scope_key != intent.scope_key
            ):
                if self.operation.operation_key == intent.operation_key:
                    raise SharingOperationConflictError("fixture request drift")
            else:
                return self.operation, True
        self.operation = SimpleNamespace(
            id=intent.id,
            operation_key=intent.operation_key,
            request_hash=intent.request_hash,
            scope_key=intent.scope_key,
            status=SharingOperationStatus.READY,
        )
        return self.operation, False

    def read(self, operation_id: Any) -> Any:
        assert self.operation is not None and self.operation.id == operation_id
        return self.operation

    def request_cancel(self, operation_id: Any) -> Any:
        assert self.operation is not None and self.operation.id == operation_id
        self.operation.status = SharingOperationStatus.CANCELLED
        self.cancelled.append(operation_id)
        return self.operation


class _Executor:
    def __init__(self, status: SharingOperationStatus = SharingOperationStatus.CONFIRMED) -> None:
        self.calls: list[Any] = []
        self.status = status
        self.lease_seconds: list[float] = []

    def execute(self, operation_id: Any, **kwargs: Any) -> Any:
        self.calls.append(operation_id)
        self.lease_seconds.append(float(kwargs["lease_seconds"]))
        before_request = kwargs.get("before_request")
        if callable(before_request):
            before_request()
        return SimpleNamespace(operation=SimpleNamespace(status=self.status))


def _context() -> dict[str, Any]:
    return {
        "operation_occurrence": "subscribe:Movie:42",
        "tmdb_id": "42",
        "item_type": "Movie",
        "title": "Fixture Movie",
        "year": "2026",
    }


def _sources(
    *,
    center: _Center | None = None,
    hdhive: _HDHive | None = None,
    telegram: _Telegram | None = None,
    operations: _Operations | None = None,
    executor: _Executor | None = None,
    virtual_imports: Any | None = None,
    telegram_configuration: Any | None = None,
) -> tuple[PersistentOrderedAutoSubscriptionSources, _Operations, _Executor]:
    operations = operations or _Operations()
    executor = executor or _Executor()
    service = PersistentOrderedAutoSubscriptionSources(
        center=center or _Center({"tmdb_id": "42", "source_kind": "movie", "source_id": "source-1"}),  # type: ignore[arg-type]
        hdhive=hdhive or _HDHive({"tmdb_id": "42", "slug": "slug-1"}),  # type: ignore[arg-type]
        telegram=telegram
        or _Telegram(
            {
                "tmdb_id": "42",
                "message_id": 7,
                "target_link": "https://115.com/s/privateshare",
                "receive_code": "2468",
            }
        ),
        operations=operations,  # type: ignore[arg-type]
        executor=executor,  # type: ignore[arg-type]
        virtual_imports=virtual_imports or _VirtualImports(),  # type: ignore[arg-type]
        shared_configuration=lambda: {"p115_shared_virtual_import_enabled": False},
        telegram_configuration=telegram_configuration or (lambda: {}),
        worker_id="worker-1",
        lease_seconds=30,
    )
    return service, operations, executor


@pytest.mark.parametrize(
    ("source_type", "kind"),
    [
        ("shared_pool", SharingOperationKind.TRANSFER),
        ("hdhive", SharingOperationKind.TRANSFER),
        ("tg_channel", SharingOperationKind.TRANSFER),
    ],
)
def test_source_operation_identity_uses_only_parent_occurrence_and_source_type(
    source_type: str,
    kind: SharingOperationKind,
) -> None:
    sources, operations, _executor = _sources()

    result = sources.attempt(source_type, _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.CONFIRMED
    assert operations.intents[0].operation_key == sharing_operation_key(
        kind=kind,
        principal_id="worker:auto-subscribe",
        idempotency_key=f"subscribe:Movie:42:{source_type}",
    )


def test_candidate_drift_conflicts_with_original_operation_and_does_not_execute_again() -> None:
    hdhive = _HDHive({"tmdb_id": "42", "slug": "slug-1"})
    sources, operations, executor = _sources(hdhive=hdhive)

    first = sources.attempt("hdhive", _context(), _Progress())
    hdhive.candidate = {"tmdb_id": "42", "slug": "slug-2"}
    drifted = sources.attempt("hdhive", _context(), _Progress())

    assert first.disposition is AutoSourceDisposition.CONFIRMED
    assert drifted.disposition is AutoSourceDisposition.INDETERMINATE
    assert operations.intents[0].operation_key == operations.intents[1].operation_key
    assert operations.intents[0].request_hash != operations.intents[1].request_hash
    assert len(executor.calls) == 1


@pytest.mark.parametrize(
    "status",
    [
        SharingOperationStatus.READY,
        SharingOperationStatus.CLAIMED,
        SharingOperationStatus.REQUESTING,
        SharingOperationStatus.AWAITING_READBACK,
        SharingOperationStatus.RETRYABLE,
    ],
)
def test_nonterminal_sharing_operation_is_retryable_pending(status: SharingOperationStatus) -> None:
    executor = _Executor(status)
    sources, _operations, _executor = _sources(executor=executor)

    result = sources.attempt("hdhive", _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.PENDING
    assert result.error == {
        "action": "影巢",
        "error_type": f"sharing_operation_{status.value}",
        "retryable": True,
    }
    assert executor.lease_seconds == [30]


@pytest.mark.parametrize("fail_checkpoint", [2, 3])
def test_parent_interruption_cancels_reserved_operation_before_provider_request(
    fail_checkpoint: int,
) -> None:
    sources, operations, executor = _sources()

    with pytest.raises(AutoSubscriptionInterrupted, match="interrupted"):
        sources.attempt("hdhive", _context(), _Progress(fail_checkpoint=fail_checkpoint))

    operation = operations.operation
    assert operation is not None
    assert operations.cancelled == [operation.id]
    assert operation.status is SharingOperationStatus.CANCELLED
    assert len(executor.calls) == (0 if fail_checkpoint == 2 else 1)


def test_persistent_parent_interruption_before_reserve_propagates_unchanged() -> None:
    sources, operations, executor = _sources()

    with pytest.raises(PersistentExecutionCancelled):
        sources.attempt(
            "hdhive",
            _context(),
            _Progress(
                fail_checkpoint=1,
                checkpoint_error=PersistentExecutionCancelled("cancelled fixture"),
            ),
        )

    assert operations.cancelled == []
    assert executor.calls == []


def test_telegram_search_and_operation_replay_keys_are_stable_and_request_is_secret_free() -> None:
    telegram = _Telegram(
        {
            "tmdb_id": "42",
            "message_id": 7,
            "target_link": "https://115.com/s/privateshare",
            "receive_code": "2468",
        }
    )
    sources, operations, _executor = _sources(telegram=telegram)

    result = sources.attempt("tg_channel", _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.CONFIRMED
    assert telegram.enqueue_calls[0][2] == "subscribe:Movie:42:tg_channel:search"
    request = operations.intents[0].request
    assert telegram.enqueue_calls[0][3] == operations.intents[0].operation_key
    serialized = json.dumps(request, ensure_ascii=False, sort_keys=True)
    assert request["payload"]["command_id"] == telegram.command_id
    assert "privateshare" not in serialized
    assert "2468" not in serialized


@pytest.mark.parametrize(
    "rule",
    [
        "forbidden-release",
        {"pattern": "forbidden-release", "channel": "fixture_channel"},
        {"pattern": "[", "channel": "fixture_channel"},
    ],
)
def test_telegram_auto_source_applies_configured_block_rules_before_reserving_operation(rule: Any) -> None:
    telegram = _Telegram(
        {
            "tmdb_id": "42",
            "message_id": 7,
            "source_username": "fixture_channel",
            "text": "Fixture Movie 2026 forbidden-release",
            "target_link": "https://115.com/s/privateshare",
            "receive_code": "2468",
        }
    )
    service, operations, executor = _sources(
        telegram=telegram,
        telegram_configuration=lambda: {"block_keywords": [rule]},
    )

    result = service.attempt("tg_channel", _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.MISS
    assert operations.intents == []
    assert executor.calls == []


@pytest.mark.parametrize("source_type", ["shared_pool", "hdhive"])
def test_non_adapter_discovery_failures_are_retryable(source_type: str) -> None:
    sources, _operations, executor = _sources(
        center=_Center(RuntimeError("center read failed")),
        hdhive=_HDHive(RuntimeError("hdhive read failed")),
    )

    result = sources.attempt(source_type, _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.RETRYABLE
    assert executor.calls == []


def test_telegram_enqueue_wait_and_request_conflict_have_safe_dispositions() -> None:
    telegram = _Telegram({"tmdb_id": "42"})
    sources, _operations, executor = _sources(telegram=telegram)
    telegram.enqueue_error = RuntimeError("local enqueue unavailable")
    assert sources.attempt("tg_channel", _context(), _Progress()).disposition is AutoSourceDisposition.RETRYABLE

    telegram.enqueue_error = None
    telegram.wait_error = LookupError("command missing")
    assert sources.attempt("tg_channel", _context(), _Progress()).disposition is AutoSourceDisposition.RETRYABLE

    telegram.wait_error = None
    telegram.enqueue_error = TelegramUserBotIdempotencyConflictError("request changed")
    assert sources.attempt("tg_channel", _context(), _Progress()).disposition is AutoSourceDisposition.INDETERMINATE
    assert executor.calls == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("share_code", "privateshare"),
        ("shareCode", "privateshare"),
        ("receive_code", "2468"),
        ("receiveCode", "2468"),
        ("pass_code", "2468"),
        ("passCode", "2468"),
        ("extract_code", "2468"),
        ("extractCode", "2468"),
        ("code", "privateshare"),
        ("p115_share_code", "privateshare"),
        ("p115ShareCode", "privateshare"),
        ("shareUrl", "privateshare"),
        ("shareLink", "privateshare"),
        ("magnet_url", "magnet:?xt=urn:btih:" + "a" * 40),
        ("target_link", "https://115.com/s/privateshare"),
        ("metadata", '{"share_code":"privateshare"}'),
    ],
)
def test_shared_source_freeze_rejects_raw_transfer_secrets(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="敏感|URL"):
        freeze_virtual_import_source(
            {
                "source_kind": "movie",
                "source_id": "source-1",
                field: value,
            }
        )


def test_shared_center_candidate_with_legacy_receive_code_never_reaches_durable_operation() -> None:
    service, operations, executor = _sources(
        center=_Center(
            {
                "tmdb_id": "42",
                "source_kind": "movie",
                "source_id": "source-legacy-secret",
                "passCode": "2468",
            }
        ),
        virtual_imports=_ValidatingVirtualImports(),
    )

    result = service.attempt("shared_pool", _context(), _Progress())

    assert result.disposition is AutoSourceDisposition.REJECTED
    assert operations.intents == []
    assert executor.calls == []


class _TransferRepository:
    def __init__(self) -> None:
        self.states: list[dict[str, Any]] = []

    def save_prewrite_state(self, operation_id: Any, claim_token: Any, state: Mapping[str, Any]) -> None:
        del operation_id, claim_token
        self.states.append(dict(state))


class _P115:
    def __init__(self) -> None:
        self.resources: list[dict[str, Any]] = []

    def freeze_identity(self) -> Mapping[str, Any]:
        return {
            "account_id": "account-1",
            "target_cid": "target-1",
            "app_type": "web",
            "credential_hash": "c" * 64,
        }

    def transfer(
        self,
        resource: Mapping[str, Any],
        *,
        expected_identity: Mapping[str, Any],
    ) -> ProviderResponse[dict[str, Any]]:
        assert expected_identity["credential_hash"] == "c" * 64
        self.resources.append(dict(resource))
        return ProviderResponse(ProviderDisposition.CONFIRMED, 200, {"resource_kind": resource["resource_kind"]})


def test_telegram_transfer_keeps_raw_resource_in_memory_only() -> None:
    telegram = _Telegram(
        {
            "tmdb_id": "42",
            "message_id": 7,
            "target_link": "https://115.com/s/privateshare",
            "receive_code": "2468",
        }
    )
    repository = _TransferRepository()
    p115 = _P115()
    delivery = AutoSubscriptionTransferOperationDelivery(
        repository=repository,  # type: ignore[arg-type]
        hdhive=_HDHive({"tmdb_id": "42", "slug": "slug-1"}),  # type: ignore[arg-type]
        telegram=telegram,
        p115=p115,
    )
    operation = SimpleNamespace(
        id=uuid4(),
        claim_token=uuid4(),
        request={
            "provider": "auto_subscription",
            "action": "telegram_transfer",
            "payload": {
                "command_id": telegram.command_id,
                "candidate_index": 0,
                "candidate_hash": "unused",
                "search_result_ref": telegram.result_reference,
                "search_result_hash": _candidate_digest(telegram.raw_result),
            },
        },
        operation_key="b" * 64,
    )
    telegram.result_owner_key = operation.operation_key
    candidate = telegram.raw_result["results"][0]

    operation.request["payload"]["candidate_hash"] = _candidate_digest(candidate)
    checkpoints: list[bool] = []

    outcome = delivery.request(operation, checkpoint=lambda: checkpoints.append(True))  # type: ignore[arg-type]

    assert outcome.disposition.value == "confirmed"
    assert checkpoints == [True]
    assert p115.resources == [{"resource_kind": "p115_share", "share_code": "privateshare", "receive_code": "2468"}]
    persisted = json.dumps(
        {"request": operation.request, "states": repository.states, "result": outcome.result},
        ensure_ascii=False,
        sort_keys=True,
    )
    assert "privateshare" not in persisted
    assert "2468" not in persisted


def test_p115_transfer_uses_the_same_verified_credential_snapshot() -> None:
    old = SimpleNamespace(
        has_cookie=True,
        account_id="account-1",
        app_type="web",
        cookie="old-cookie",
    )
    rotated = SimpleNamespace(
        has_cookie=True,
        account_id="account-1",
        app_type="web",
        cookie="rotated-cookie",
    )

    class Credentials:
        def __init__(self) -> None:
            self.snapshots = [old, rotated]
            self.loads = 0

        def load(self) -> Any:
            value = self.snapshots[self.loads]
            self.loads += 1
            return value

    used_cookies: list[str] = []

    class Client:
        def request(self, url: str, *, method: str, data: Mapping[str, Any]) -> Mapping[str, Any]:
            del url, method, data
            return {"state": True}

        def offline_add_urls(self, payload: Mapping[str, Any]) -> Mapping[str, Any]:
            del payload
            return {"state": True}

    credentials = Credentials()
    credential_hash = hashlib.sha256(b"web\0old-cookie").hexdigest()
    gateway = NativeP115ResourceTransferGateway(
        credentials,
        lambda: SimpleNamespace(p115_save_path_cid="target-1"),  # type: ignore[arg-type,return-value]
        client_factory=lambda cookie, _app: (used_cookies.append(cookie), Client())[1],
    )

    result = gateway.transfer(
        {"resource_kind": "p115_share", "share_code": "private", "receive_code": "2468"},
        expected_identity={
            "account_id": "account-1",
            "target_cid": "target-1",
            "app_type": "web",
            "credential_hash": credential_hash,
        },
    )

    assert result.disposition is ProviderDisposition.CONFIRMED
    assert credentials.loads == 1
    assert used_cookies == ["old-cookie"]
