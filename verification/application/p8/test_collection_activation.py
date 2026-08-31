from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.domains.collection.activation import (
    CollectionActivationConflictError,
    CollectionActivationNotFoundError,
    CollectionActivationProviderUnavailableError,
    CollectionActivationReservation,
    CollectionActivationService,
)
from etk.domains.collection.operation_execution import CollectionOperationExecutor
from etk.domains.collection.operations import (
    CollectionOperation,
    CollectionOperationKind,
    CollectionOperationStatus,
    CollectionOperationStep,
)
from etk.domains.collection.router import router

_OPERATION_ID = UUID("00000000-0000-0000-0000-000000000251")
_CLAIM_TOKEN = UUID("00000000-0000-0000-0000-000000000252")
_NOW = datetime(2026, 9, 1, tzinfo=UTC)


def _operation(status: CollectionOperationStatus = CollectionOperationStatus.READY) -> CollectionOperation:
    claimed = status in {CollectionOperationStatus.CLAIMED, CollectionOperationStatus.REQUESTING}
    terminal = status.terminal
    return CollectionOperation(
        id=_OPERATION_ID,
        sequence=25,
        operation_key="a" * 64,
        request_hash=b"b" * 32,
        kind=CollectionOperationKind.ACTIVATION_REFRESH,
        scope_key="emby-collection:emby-25",
        request={
            "tmdb_collection_id": "25",
            "emby_collection_id": "emby-25",
            "name": "Fixture Collection",
        },
        status=status,
        current_step=CollectionOperationStep.RESERVED,
        state_revision=1,
        attempts=1 if claimed else 0,
        available_at=_NOW,
        claimed_by="fixture-worker" if claimed else None,
        claim_token=_CLAIM_TOKEN if claimed else None,
        claimed_at=_NOW if claimed else None,
        lease_expires_at=_NOW + timedelta(minutes=5) if claimed else None,
        step_started_at=None,
        state={},
        result={} if status is CollectionOperationStatus.CONFIRMED else None,
        error={} if status in {CollectionOperationStatus.REJECTED, CollectionOperationStatus.INDETERMINATE} else None,
        finished_at=_NOW if terminal else None,
        created_at=_NOW,
        updated_at=_NOW,
    )


class FakeActivationRepository:
    def __init__(self) -> None:
        self.mode = "success"
        self.calls: list[dict[str, str]] = []

    def reserve(self, **values: str) -> CollectionActivationReservation | None:
        self.calls.append(values)
        if self.mode == "missing":
            return None
        if self.mode == "conflict":
            raise CollectionActivationConflictError("fixture conflict")
        if self.mode == "failure":
            raise RuntimeError("fixture transaction failure")
        return CollectionActivationReservation(
            operation=_operation(),
            operation_created=True,
            changed=True,
            already_active=False,
        )


def _client(repository: FakeActivationRepository) -> TestClient:
    app = FastAPI()
    app.state.collection_activation_service = CollectionActivationService(cast(Any, repository))
    app.include_router(router)
    return TestClient(app)


def test_collection_activation_http_reserves_durable_operation_without_authentication() -> None:
    repository = FakeActivationRepository()
    with _client(repository) as client:
        response = client.post(
            "/api/emby/collections/activate",
            json={
                "tmdb_collection_id": "25",
                "emby_collection_id": "emby-25",
                "name": "Fixture Collection",
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "ok": True,
        "changed": True,
        "already_active": False,
        "operation_id": str(_OPERATION_ID),
        "operation_status": "ready",
        "operation_created": True,
    }
    assert repository.calls == [
        {
            "tmdb_collection_id": "25",
            "emby_collection_id": "emby-25",
            "name": "Fixture Collection",
        }
    ]


def test_collection_activation_http_preserves_silent_malformed_body_as_json_400() -> None:
    repository = FakeActivationRepository()
    with _client(repository) as client:
        malformed = client.post(
            "/api/emby/collections/activate",
            content=b"{",
            headers={"Content-Type": "application/json"},
        )
        wrong_content_type = client.post(
            "/api/emby/collections/activate",
            content=b'{"tmdb_collection_id":"25"}',
            headers={"Content-Type": "text/plain"},
        )
        array_body = client.post("/api/emby/collections/activate", json=[])

    assert malformed.status_code == wrong_content_type.status_code == array_body.status_code == 400
    assert (
        malformed.json()
        == wrong_content_type.json()
        == array_body.json()
        == {
            "ok": False,
            "error": "invalid collection activation request",
        }
    )
    assert repository.calls == []


def test_collection_activation_http_maps_missing_conflict_and_transaction_failure() -> None:
    repository = FakeActivationRepository()
    with _client(repository) as client:
        repository.mode = "missing"
        missing = client.post(
            "/api/emby/collections/activate",
            json={"tmdb_collection_id": "25", "emby_collection_id": "emby-25"},
        )
        repository.mode = "conflict"
        conflict = client.post(
            "/api/emby/collections/activate",
            json={"tmdb_collection_id": "25", "emby_collection_id": "emby-25"},
        )
        repository.mode = "failure"
        failed = client.post(
            "/api/emby/collections/activate",
            json={"tmdb_collection_id": "25", "emby_collection_id": "emby-25"},
        )

    assert missing.status_code == 404
    assert missing.json() == {"ok": False, "error": "collection metadata cache not found"}
    assert conflict.status_code == 409
    assert conflict.json() == {"ok": False, "error": "collection activation conflict"}
    assert failed.status_code == 500
    assert failed.json() == {"ok": False, "error": "collection activation failed"}


class FakeOperationRepository:
    def __init__(self) -> None:
        self.operation = _operation()
        self.retryable = 0
        self.rejected = 0

    def read(self, _operation_id: UUID) -> CollectionOperation:
        return self.operation

    def claim_by_id(self, _operation_id: UUID, **_values: Any) -> CollectionOperation:
        self.operation = replace(
            self.operation,
            status=CollectionOperationStatus.CLAIMED,
            attempts=self.operation.attempts + 1,
            claimed_by="fixture-worker",
            claim_token=_CLAIM_TOKEN,
            claimed_at=_NOW,
            lease_expires_at=_NOW + timedelta(minutes=5),
        )
        return self.operation

    def mark_retryable(self, _operation_id: UUID, _claim_token: UUID, **_values: Any) -> CollectionOperation:
        self.retryable += 1
        self.operation = replace(
            self.operation,
            status=CollectionOperationStatus.RETRYABLE,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
        )
        return self.operation

    def mark_rejected(self, _operation_id: UUID, _claim_token: UUID, **_values: Any) -> CollectionOperation:
        self.rejected += 1
        self.operation = replace(
            self.operation,
            status=CollectionOperationStatus.REJECTED,
            claimed_by=None,
            claim_token=None,
            claimed_at=None,
            lease_expires_at=None,
            finished_at=_NOW,
        )
        return self.operation


class FakeActivationProvider:
    def __init__(self) -> None:
        self.failure: BaseException | None = None
        self.calls: list[str] = []

    def collection_details(self, tmdb_collection_id: str) -> dict[str, Any]:
        self.calls.append(tmdb_collection_id)
        if self.failure is not None:
            raise self.failure
        return {"id": 25, "parts": [{"id": 2501}]}


class FakeActivationCompletion:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, UUID, Mapping[str, Any], str]] = []

    def complete(
        self,
        operation_id: UUID,
        claim_token: UUID,
        details: Mapping[str, Any],
        *,
        today: date,
    ) -> CollectionOperation:
        self.calls.append((operation_id, claim_token, details, today.isoformat()))
        return replace(
            _operation(CollectionOperationStatus.CONFIRMED),
            result={"movie_count": 1},
        )


def test_collection_activation_executor_reads_tmdb_then_completes_atomically() -> None:
    repository = FakeOperationRepository()
    provider = FakeActivationProvider()
    completion = FakeActivationCompletion()
    executor = CollectionOperationExecutor(
        cast(Any, repository),
        worker_id="fixture-worker",
        lease_seconds=90,
        activation_provider=provider,
        activation_completion=completion,
        clock=lambda: _NOW,
    )

    result = executor.execute(_OPERATION_ID)

    assert result is not None and result.status is CollectionOperationStatus.CONFIRMED
    assert provider.calls == ["25"]
    assert completion.calls == [
        (
            _OPERATION_ID,
            _CLAIM_TOKEN,
            {"id": 25, "parts": [{"id": 2501}]},
            "2026-09-01",
        )
    ]


def test_collection_activation_executor_retries_read_only_provider_failure() -> None:
    repository = FakeOperationRepository()
    provider = FakeActivationProvider()
    provider.failure = CollectionActivationProviderUnavailableError("fixture TMDb failure")
    executor = CollectionOperationExecutor(
        cast(Any, repository),
        worker_id="fixture-worker",
        lease_seconds=90,
        activation_provider=provider,
        activation_completion=FakeActivationCompletion(),
    )

    result = executor.execute(_OPERATION_ID)

    assert result is not None and result.status is CollectionOperationStatus.RETRYABLE
    assert repository.retryable == 1
    assert repository.rejected == 0


def test_collection_activation_executor_rejects_authoritative_tmdb_404_without_retry() -> None:
    repository = FakeOperationRepository()
    provider = FakeActivationProvider()
    provider.failure = CollectionActivationNotFoundError("fixture TMDb 404")
    executor = CollectionOperationExecutor(
        cast(Any, repository),
        worker_id="fixture-worker",
        lease_seconds=90,
        activation_provider=provider,
        activation_completion=FakeActivationCompletion(),
    )

    result = executor.execute(_OPERATION_ID)

    assert result is not None and result.status is CollectionOperationStatus.REJECTED
    assert repository.retryable == 0
    assert repository.rejected == 1
