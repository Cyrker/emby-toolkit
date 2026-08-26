from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest
from sqlalchemy import inspect, text

from etk.compatibility.persistent_runtime_state import PostgresRuntimeStateBackend
from etk.domains.p115.models import P115AuthorizationSession
from etk.domains.p115.playback import (
    PLAY_POOL_CONFIG_KEY,
    PLAYBACK_TOPIC,
    TEMP_DIR_CONFIG_KEY,
    P115PlaybackConfigService,
    P115PlaybackConflictError,
    P115PlaybackStateError,
    PlaybackArtifact,
    PlaybackArtifactIntent,
    PlaybackArtifactStatus,
    PlaybackLeaseIntent,
    PlaybackLeaseStatus,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationIntent,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackPrepareRequest,
    PlaybackProviderDisposition,
    PlaybackProviderOutcome,
    PlaybackQrStatus,
    operation_id,
)
from etk.domains.p115.playback_repository import PostgresP115PlaybackRepository
from etk.domains.p115.playback_service import P115PlaybackService
from etk.platform.db import DatabaseRuntime
from etk.processes.p6_playback import P115PlaybackOperationExecutor

_SOURCE_SHA1 = "A" * 40


@pytest.fixture(autouse=True)
def clean_playback_state(p6_runtime: DatabaseRuntime):
    def clean() -> None:
        with p6_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM p115_playback_leases"))
            connection.execute(text("DELETE FROM p115_playback_artifacts"))
            connection.execute(text("DELETE FROM p115_playback_operations"))
            connection.execute(text("DELETE FROM p115_playback_qr_sessions"))
            connection.execute(
                text("DELETE FROM outbox_events WHERE topic = :topic"),
                {"topic": PLAYBACK_TOPIC},
            )
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key IN (:pool_key, :temp_key)"),
                {"pool_key": PLAY_POOL_CONFIG_KEY, "temp_key": TEMP_DIR_CONFIG_KEY},
            )

    clean()
    yield
    clean()


class FakeUsers:
    def display_name(self, user_id: str) -> str:
        return f"Fixture {user_id}" if user_id else ""

    def expand_allowed_user_ids(self, user_ids: list[str]) -> tuple[str, ...]:
        return tuple(user_ids)

    def reward_summary(self, _user_id: str) -> dict[str, Any]:
        return {"total_days": 0.0}

    def grant_reward(
        self,
        _account: dict[str, Any],
        *,
        notify_user: bool,
        speed_text: str,
        error_text: str,
    ) -> dict[str, Any]:
        del notify_user, speed_text, error_text
        return {"reward_days": 0.0}

    def notify_daily_results(self, _results: list[dict[str, Any]]) -> None:
        return None


class FakeSources:
    def snapshot(self, _request: PlaybackPrepareRequest) -> dict[str, Any]:
        return {
            "sha1": _SOURCE_SHA1,
            "size": 1024,
            "preid": "B" * 40,
            "pick_code": "fixture-source-pick",
            "fid": "fixture-source-fid",
            "file_name": "Fixture.mkv",
        }

    def primary_account(self) -> dict[str, Any]:
        return {"account_id": "fixture-primary", "provider_backend": "openapi"}


class FakeDirectLinks:
    def resolve_primary(self, _pick_code: str, _user_agent: str) -> str:
        return ""

    def resolve_artifact(self, artifact: Any, user_agent: str) -> str:
        return f"https://media.fixture.invalid/{artifact.remote_pick_code}?ua={user_agent}"

    def stream(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("stream is not used by playback state-machine tests")


class RecordingProvider:
    def __init__(
        self,
        *,
        prepare_delay: float = 0.0,
        prepare_rejections: int = 0,
        ensure_rejections: int = 0,
    ) -> None:
        self.prepare_delay = prepare_delay
        self.prepare_rejections = prepare_rejections
        self.ensure_rejections = ensure_rejections
        self.lock = threading.Lock()
        self.prepare_writes = 0
        self.reclaim_writes = 0
        self.ensure_writes = 0

    def execute(self, operation: PlaybackOperation, *, checkpoint: Any) -> PlaybackProviderOutcome:
        checkpoint(
            {
                "account_id": operation.account_id,
                "backend": operation.provider_backend,
                "kind": operation.kind.value,
                "phase": operation.kind.value,
            }
        )
        if operation.kind is PlaybackOperationKind.PREPARE:
            with self.lock:
                self.prepare_writes += 1
                attempt = self.prepare_writes
            if self.prepare_delay:
                time.sleep(self.prepare_delay)
            if attempt <= self.prepare_rejections:
                return PlaybackProviderOutcome(
                    PlaybackProviderDisposition.REJECTED,
                    request_sent=True,
                    error={"type": "fixture_prepare_rejected", "message": "fixture prepare rejected"},
                )
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.CONFIRMED,
                request_sent=True,
                receipt={
                    "target_cid": str(operation.request.get("target_cid") or ""),
                    "remote_fid": f"fixture-remote-fid-{operation.artifact_generation}",
                    "remote_pick_code": f"fixture-remote-pick-{operation.artifact_generation}",
                    "file_name": str(operation.request.get("file_name") or "Fixture.mkv"),
                    "created_remote": True,
                },
            )
        if operation.kind is PlaybackOperationKind.RECLAIM:
            with self.lock:
                self.reclaim_writes += 1
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.CONFIRMED,
                request_sent=True,
                receipt={
                    "deleted": True,
                    "remote_fid": str(operation.request.get("remote_fid") or ""),
                },
                readback={"deleted": True},
            )
        if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
            with self.lock:
                self.ensure_writes += 1
                attempt = self.ensure_writes
            if attempt <= self.ensure_rejections:
                return PlaybackProviderOutcome(
                    PlaybackProviderDisposition.REJECTED,
                    request_sent=True,
                    error={"type": "fixture_known_rejection", "message": "fixture known rejection"},
                )
            return PlaybackProviderOutcome(
                PlaybackProviderDisposition.CONFIRMED,
                request_sent=True,
                receipt={"cid": "fixture-temp-cid", "created_remote": True},
            )
        raise AssertionError(f"unexpected playback operation: {operation.kind.value}")

    def readback(self, operation: PlaybackOperation) -> PlaybackProviderOutcome:
        del operation
        raise AssertionError("readback is not used by this provider")


def _repository(runtime: DatabaseRuntime) -> PostgresP115PlaybackRepository:
    return PostgresP115PlaybackRepository(runtime.unit_of_work)


def _config(runtime: DatabaseRuntime) -> P115PlaybackConfigService:
    raw_pool = runtime.raw_pool
    assert raw_pool is not None
    return P115PlaybackConfigService(
        PostgresRuntimeStateBackend(raw_pool.connection),
        cast(Any, FakeUsers()),
    )


def _service(
    runtime: DatabaseRuntime,
    provider: RecordingProvider,
    *,
    worker_id: str,
) -> P115PlaybackService:
    repository = _repository(runtime)
    config = _config(runtime)
    executor = P115PlaybackOperationExecutor(
        repository=repository,
        provider=provider,
        config=config,
        worker_id=worker_id,
        lease_seconds=30,
    )
    return P115PlaybackService(
        config=config,
        repository=repository,
        sources=cast(Any, FakeSources()),
        direct_links=cast(Any, FakeDirectLinks()),
        executor=executor,
    )


def _enable_pool(runtime: DatabaseRuntime) -> str:
    config = _config(runtime)
    config.save_settings({"enabled": True})
    account = config.upsert_account(
        {
            "alias": "Fixture Pool",
            "access_token": "fixture-access-token",
            "refresh_token": "fixture-refresh-token",
            "temp_cid": "fixture-temp-cid",
            "enabled": True,
        }
    )
    return str(account["id"])


def _pool_request(session: str, *, user: str, client: str) -> PlaybackPrepareRequest:
    return PlaybackPrepareRequest(
        mode=PlaybackMode.POOL,
        source_pick_code="fixture-source-pick",
        file_name="Fixture.mkv",
        item_id="fixture-item",
        play_session_id=session,
        user_id=user,
        client_key=client,
        user_agent="FixturePlayer/1.0",
        source="fixture",
    )


def _prepare_intent(
    semantic_key: str,
    artifact_key: str,
    generation: int,
) -> PlaybackOperationIntent:
    return PlaybackOperationIntent.create(
        semantic_key=semantic_key,
        kind=PlaybackOperationKind.PREPARE,
        mode=PlaybackMode.COPY,
        account_id="fixture-primary",
        provider_backend="openapi",
        artifact_key=artifact_key,
        artifact_generation=generation,
        request={
            "artifact_key": artifact_key,
            "artifact_generation": generation,
            "target_cid": "fixture-temp-cid",
            "file_name": "Fixture.mkv",
            "source": {
                "sha1": _SOURCE_SHA1,
                "size": 1024,
                "pick_code": "fixture-source-pick",
                "fid": "fixture-source-fid",
                "file_name": "Fixture.mkv",
            },
        },
    )


def _artifact_intent(
    operation: PlaybackOperation,
    *,
    artifact_key: str | None = None,
    generation: int | None = None,
    remote_suffix: str = "fixture",
) -> PlaybackArtifactIntent:
    return PlaybackArtifactIntent(
        artifact_key=artifact_key or str(operation.artifact_key or ""),
        generation=generation or int(operation.artifact_generation or 0),
        operation_id=operation.id,
        mode=operation.mode,
        account_id=operation.account_id,
        provider_backend=operation.provider_backend,
        target_cid="fixture-temp-cid",
        source_sha1=_SOURCE_SHA1,
        source_size=1024,
        source_pick_code="fixture-source-pick",
        source_fid="fixture-source-fid",
        file_name="Fixture.mkv",
        receipt={
            "target_cid": "fixture-temp-cid",
            "remote_fid": f"fixture-remote-fid-{remote_suffix}",
            "remote_pick_code": f"fixture-remote-pick-{remote_suffix}",
            "file_name": "Fixture.mkv",
            "created_remote": True,
        },
        reclaim_not_before=datetime.now(UTC) - timedelta(seconds=1),
    )


def _commit_artifact(
    repository: PostgresP115PlaybackRepository,
    *,
    semantic_key: str,
    artifact_key: str,
    generation: int,
    remote_suffix: str,
) -> PlaybackArtifact:
    operation = repository.reserve(_prepare_intent(semantic_key, artifact_key, generation)).operation
    claimed = repository.claim_by_id(operation.id, worker_id=f"fixture-{remote_suffix}", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_confirmed(
        claimed.id,
        claimed.claim_token,
        result={"artifact_key": artifact_key, "artifact_generation": generation},
        artifact=_artifact_intent(claimed, remote_suffix=remote_suffix),
    )
    artifact = repository.artifact_by_key(artifact_key)
    assert artifact is not None and artifact.generation == generation
    return artifact


def _reclaim_intent(semantic_key: str, artifact: PlaybackArtifact) -> PlaybackOperationIntent:
    return PlaybackOperationIntent.create(
        semantic_key=semantic_key,
        kind=PlaybackOperationKind.RECLAIM,
        mode=artifact.mode,
        account_id=artifact.account_id,
        provider_backend=artifact.provider_backend,
        artifact_key=artifact.artifact_key,
        artifact_generation=artifact.generation,
        request={
            "artifact_id": str(artifact.id),
            "artifact_key": artifact.artifact_key,
            "artifact_generation": artifact.generation,
            "target_cid": artifact.target_cid,
            "remote_fid": artifact.remote_fid,
            "remote_pick_code": artifact.remote_pick_code,
            "source": {
                "sha1": artifact.source_sha1,
                "size": artifact.source_size,
                "pick_code": artifact.source_pick_code,
                "fid": artifact.source_fid,
                "file_name": artifact.file_name,
            },
        },
    )


def test_playback_migration_schema_and_head_are_present(p6_runtime: DatabaseRuntime) -> None:
    inspector = inspect(p6_runtime.engine)
    assert {
        "p115_playback_operations",
        "p115_playback_artifacts",
        "p115_playback_leases",
        "p115_playback_qr_sessions",
    } <= set(inspector.get_table_names())
    with p6_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT version_num FROM etk_migrations.alembic_version")) == (
            "0012_p6_intro_detection"
        )
    assert {
        "ck_p115_playback_operation_hash",
        "ck_p115_playback_operation_claim",
        "ck_p115_playback_operation_finished",
    } <= {constraint["name"] for constraint in inspector.get_check_constraints("p115_playback_operations")}


def test_operation_reservation_uses_deterministic_id_and_rejects_identity_conflict(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    intent = PlaybackOperationIntent.create(
        semantic_key="fixture-deterministic-operation",
        kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
        mode=PlaybackMode.MAINTENANCE,
        account_id="fixture-account",
        provider_backend="openapi",
        request={"account_kind": "pool", "temp_dir_name": "ETK临时目录"},
    )

    created = repository.reserve(intent)
    replay = repository.reserve(intent)

    assert created.created is True
    assert replay.created is False
    assert created.operation.id == replay.operation.id == operation_id(intent.operation_key)
    with p6_runtime.engine.connect() as connection:
        outbox = connection.execute(
            text("SELECT aggregate_id, aggregate_type, topic FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(created.operation.id)},
        ).one()
    assert outbox == (str(created.operation.id), "p115_playback_operation", PLAYBACK_TOPIC)

    changed = PlaybackOperationIntent.create(
        semantic_key="fixture-deterministic-operation",
        kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
        mode=PlaybackMode.MAINTENANCE,
        account_id="fixture-account",
        provider_backend="openapi",
        request={"account_kind": "pool", "temp_dir_name": "Changed"},
    )
    with pytest.raises(P115PlaybackConflictError, match="different request hash"):
        repository.reserve(changed)


def test_reservation_recomputes_request_hash_and_counts_rejected_prepare_generation(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    forged = PlaybackOperationIntent.create(
        semantic_key="fixture-forged-request-hash",
        kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
        mode=PlaybackMode.MAINTENANCE,
        account_id="fixture-account",
        provider_backend="openapi",
        request={"account_kind": "pool", "temp_dir_name": "Original"},
    )

    with pytest.raises(ValueError, match="canonical playback request"):
        repository.reserve(replace(forged, request={"account_kind": "pool", "temp_dir_name": "Changed"}))

    artifact_key = "f" * 64
    operation = repository.reserve(_prepare_intent("fixture-rejected-generation", artifact_key, 1)).operation
    claimed = repository.claim_by_id(operation.id, worker_id="fixture-rejected-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_rejected(
        claimed.id,
        claimed.claim_token,
        error={"type": "fixture_known_rejection"},
    )

    assert repository.latest_artifact_generation(artifact_key) == 1
    next_operation = repository.reserve(_prepare_intent("fixture-rejected-generation-2", artifact_key, 2))
    assert next_operation.created is True
    assert next_operation.operation.artifact_generation == 2


def test_checkpoint_merges_phases_and_rejects_account_backend_or_remote_identity_drift(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    intent = PlaybackOperationIntent.create(
        semantic_key="fixture-checkpoint-identity",
        kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
        mode=PlaybackMode.MAINTENANCE,
        account_id="fixture-account",
        provider_backend="openapi",
        request={"account_kind": "pool", "temp_dir_name": "ETK临时目录"},
    )
    operation = repository.reserve(intent).operation
    claimed = repository.claim_by_id(operation.id, worker_id="fixture-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None

    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        session={"phase": "create"},
        lease_seconds=30,
    )
    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        session={"phase": "readback", "remote_fid": "fixture-remote-fid"},
        lease_seconds=30,
    )
    persisted = repository.read(claimed.id)
    assert persisted is not None
    assert persisted.provider_session == {
        "account_id": "fixture-account",
        "backend": "openapi",
        "kind": "ensure_temp_dir",
        "phase": "readback",
        "remote_fid": "fixture-remote-fid",
    }

    for drift in (
        {"account_id": "other-account"},
        {"backend": "cookie"},
        {"remote_fid": "different-remote-fid"},
    ):
        with pytest.raises(P115PlaybackConflictError, match="provider session"):
            repository.checkpoint_request(
                claimed.id,
                claimed.claim_token,
                session=drift,
                lease_seconds=30,
            )


def test_checkpoint_stable_fields_cannot_clear_or_drift_from_operation_identity(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    artifact_key = "e" * 64
    operation = repository.reserve(_prepare_intent("fixture-stable-checkpoint", artifact_key, 1)).operation
    claimed = repository.claim_by_id(operation.id, worker_id="fixture-stable-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None

    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        session={
            "artifact_key": artifact_key,
            "target_cid": "fixture-temp-cid",
            "remote_fid": "fixture-remote-fid",
        },
        lease_seconds=30,
    )
    for drift in (
        {"remote_fid": ""},
        {"remote_fid": None},
        {"artifact_key": "d" * 64},
        {"target_cid": "different-target"},
    ):
        with pytest.raises(P115PlaybackConflictError, match="provider session"):
            repository.checkpoint_request(
                claimed.id,
                claimed.claim_token,
                session=drift,
                lease_seconds=30,
            )


def test_operation_recovery_matrix_is_safe_after_claim_or_checkpoint(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)

    claimed_confirmation = repository.reserve(
        PlaybackOperationIntent.create(
            semantic_key="fixture-confirmed-without-request",
            kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
            mode=PlaybackMode.MAINTENANCE,
            account_id="fixture-account",
            provider_backend="openapi",
            request={"account_kind": "pool", "temp_dir_name": "Existing"},
        )
    ).operation
    claimed_confirmation = repository.claim_by_id(
        claimed_confirmation.id,
        worker_id="fixture-existing-worker",
        lease_seconds=30,
    )
    assert claimed_confirmation is not None and claimed_confirmation.claim_token is not None
    remote_confirmed = repository.mark_remote_confirmed(
        claimed_confirmation.id,
        claimed_confirmation.claim_token,
        {"cid": "fixture-existing-cid", "created_remote": False},
    ).operation
    assert remote_confirmed.status is PlaybackOperationStatus.REMOTE_CONFIRMED

    local_retry = repository.mark_retryable(
        remote_confirmed.id,
        claimed_confirmation.claim_token,
        error={"type": "fixture_local_projection_retry"},
        retry_after_seconds=0,
    ).operation
    assert local_retry.status is PlaybackOperationStatus.RETRYABLE
    assert local_retry.remote_receipt == {"cid": "fixture-existing-cid", "created_remote": False}

    checkpointed = repository.reserve(
        PlaybackOperationIntent.create(
            semantic_key="fixture-checkpoint-no-replay",
            kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
            mode=PlaybackMode.MAINTENANCE,
            account_id="fixture-account",
            provider_backend="openapi",
            request={"account_kind": "pool", "temp_dir_name": "Checkpointed"},
        )
    ).operation
    checkpointed = repository.claim_by_id(checkpointed.id, worker_id="fixture-request-worker", lease_seconds=30)
    assert checkpointed is not None and checkpointed.claim_token is not None
    requesting = repository.checkpoint_request(
        checkpointed.id,
        checkpointed.claim_token,
        session={"phase": "mkdir"},
        lease_seconds=30,
    ).operation
    assert requesting.status is PlaybackOperationStatus.REQUESTING
    with pytest.raises(P115PlaybackStateError, match="mark retryable"):
        repository.mark_retryable(
            requesting.id,
            checkpointed.claim_token,
            error={"type": "fixture_stale_not_sent"},
            retry_after_seconds=0,
        )

    awaiting = repository.mark_awaiting_readback(
        requesting.id,
        checkpointed.claim_token,
        session=None,
        error={"type": "fixture_response_lost"},
        retry_after_seconds=0,
    ).operation
    reclaimed = repository.claim_by_id(awaiting.id, worker_id="fixture-readback-worker", lease_seconds=30)
    assert reclaimed is not None and reclaimed.claim_token is not None
    retryable = repository.mark_retryable(
        reclaimed.id,
        reclaimed.claim_token,
        error={"type": "fixture_known_not_applied"},
        retry_after_seconds=0,
    ).operation
    assert retryable.status is PlaybackOperationStatus.RETRYABLE


def test_all_claim_mutations_reject_an_expired_lease(p6_runtime: DatabaseRuntime) -> None:
    repository = _repository(p6_runtime)

    def expired_claim(label: str) -> PlaybackOperation:
        operation = repository.reserve(
            PlaybackOperationIntent.create(
                semantic_key=f"fixture-expired-{label}",
                kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
                mode=PlaybackMode.MAINTENANCE,
                account_id="fixture-account",
                provider_backend="openapi",
                request={"account_kind": "pool", "temp_dir_name": label},
            )
        ).operation
        claimed = repository.claim_by_id(operation.id, worker_id=f"fixture-{label}", lease_seconds=30)
        assert claimed is not None and claimed.claim_token is not None
        with p6_runtime.engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE p115_playback_operations "
                    "SET claimed_at=NOW()-INTERVAL '2 seconds', lease_expires_at=NOW()-INTERVAL '1 second' "
                    "WHERE id=:operation_id"
                ),
                {"operation_id": claimed.id},
            )
        return claimed

    actions = {
        "checkpoint": lambda operation: repository.checkpoint_request(
            operation.id,
            cast(Any, operation.claim_token),
            session={"phase": "mkdir"},
            lease_seconds=30,
        ),
        "remote": lambda operation: repository.mark_remote_confirmed(
            operation.id,
            cast(Any, operation.claim_token),
            {"cid": "fixture-cid", "created_remote": False},
        ),
        "retry": lambda operation: repository.mark_retryable(
            operation.id,
            cast(Any, operation.claim_token),
            error={"type": "fixture"},
            retry_after_seconds=0,
        ),
        "awaiting": lambda operation: repository.mark_awaiting_readback(
            operation.id,
            cast(Any, operation.claim_token),
            session=None,
            error={"type": "fixture"},
            retry_after_seconds=0,
        ),
        "confirmed": lambda operation: repository.mark_confirmed(
            operation.id,
            cast(Any, operation.claim_token),
            result={"cid": "fixture-cid"},
        ),
        "rejected": lambda operation: repository.mark_rejected(
            operation.id,
            cast(Any, operation.claim_token),
            error={"type": "fixture"},
        ),
        "indeterminate": lambda operation: repository.mark_indeterminate(
            operation.id,
            cast(Any, operation.claim_token),
            error={"type": "fixture"},
        ),
    }
    for label, action in actions.items():
        operation = expired_claim(label)
        with pytest.raises(P115PlaybackStateError):
            action(operation)
        persisted = repository.read(operation.id)
        assert persisted is not None
        assert persisted.status is PlaybackOperationStatus.CLAIMED
        assert persisted.claim_token == operation.claim_token


def test_artifact_commit_binds_operation_identity_and_rejects_conflicting_generation(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    artifact_key = "a" * 64
    first = repository.reserve(_prepare_intent("fixture-artifact-owner-a", artifact_key, 1)).operation
    first = repository.claim_by_id(first.id, worker_id="fixture-artifact-owner-a", lease_seconds=30)
    assert first is not None and first.claim_token is not None

    with pytest.raises(P115PlaybackConflictError, match="generation does not match"):
        repository.mark_confirmed(
            first.id,
            first.claim_token,
            result={"artifact_key": "b" * 64, "artifact_generation": 2},
            artifact=_artifact_intent(first, artifact_key="b" * 64, generation=2),
        )

    repository.mark_confirmed(
        first.id,
        first.claim_token,
        result={"artifact_key": artifact_key, "artifact_generation": 1},
        artifact=_artifact_intent(first, remote_suffix="owner-a"),
    )

    second = repository.reserve(_prepare_intent("fixture-artifact-owner-b", artifact_key, 1)).operation
    second = repository.claim_by_id(second.id, worker_id="fixture-artifact-owner-b", lease_seconds=30)
    assert second is not None and second.claim_token is not None
    with pytest.raises(P115PlaybackConflictError, match="conflicts with another identity"):
        repository.mark_confirmed(
            second.id,
            second.claim_token,
            result={"artifact_key": artifact_key, "artifact_generation": 1},
            artifact=_artifact_intent(second, remote_suffix="owner-b"),
        )
    persisted = repository.read(second.id)
    assert persisted is not None and persisted.status is PlaybackOperationStatus.CLAIMED

    wrong_kind = repository.reserve(
        PlaybackOperationIntent.create(
            semantic_key="fixture-artifact-wrong-kind",
            kind=PlaybackOperationKind.ENSURE_TEMP_DIR,
            mode=PlaybackMode.MAINTENANCE,
            account_id="fixture-primary",
            provider_backend="openapi",
            request={"account_kind": "primary", "temp_dir_name": "Wrong kind"},
        )
    ).operation
    wrong_kind = repository.claim_by_id(wrong_kind.id, worker_id="fixture-wrong-kind", lease_seconds=30)
    assert wrong_kind is not None and wrong_kind.claim_token is not None
    with pytest.raises(P115PlaybackConflictError, match="only prepare"):
        repository.mark_confirmed(
            wrong_kind.id,
            wrong_kind.claim_token,
            result={"artifact_key": artifact_key, "artifact_generation": 1},
            artifact=replace(
                _artifact_intent(first),
                operation_id=wrong_kind.id,
                mode=wrong_kind.mode,
                account_id=wrong_kind.account_id,
                provider_backend=wrong_kind.provider_backend,
            ),
        )


def test_terminal_lease_reactivates_on_new_generation_and_active_lease_fences_reclaim(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    artifact_key = "9" * 64
    first = _commit_artifact(
        repository,
        semantic_key="fixture-lease-generation-one",
        artifact_key=artifact_key,
        generation=1,
        remote_suffix="generation-one",
    )
    lease_intent = PlaybackLeaseIntent(
        lease_key="8" * 64,
        artifact_id=first.id,
        mode=first.mode,
        owner_user_id="fixture-owner",
        item_id="fixture-item",
        play_session_id="fixture-session",
        client_key="fixture-client",
        user_agent_hash="7" * 64,
        source="fixture",
        expires_at=datetime.now(UTC) + timedelta(minutes=5),
    )
    original = repository.reserve_lease(lease_intent).lease
    released = repository.release_lease(original.id, cleanup_delay_seconds=0)
    assert released.status is PlaybackLeaseStatus.RELEASED
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE p115_playback_artifacts SET status='reclaimed', reclaimed_at=NOW(), "
                "state_revision=state_revision+1 WHERE id=:artifact_id"
            ),
            {"artifact_id": first.id},
        )

    second = _commit_artifact(
        repository,
        semantic_key="fixture-lease-generation-two",
        artifact_key=artifact_key,
        generation=2,
        remote_suffix="generation-two",
    )
    rebound = repository.reserve_lease(
        replace(
            lease_intent,
            artifact_id=second.id,
            expires_at=datetime.now(UTC) + timedelta(minutes=10),
        )
    ).lease
    assert rebound.id == original.id
    assert rebound.artifact_id == second.id
    assert rebound.status is PlaybackLeaseStatus.ACTIVE
    assert rebound.released_at is None

    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE p115_playback_leases SET expires_at=created_at, cleanup_not_before=created_at "
                "WHERE id=:lease_id"
            ),
            {"lease_id": rebound.id},
        )
    reclaim = repository.reserve(_reclaim_intent("fixture-lease-reclaim-fence", second)).operation
    assert repository.artifact(second.id).status is PlaybackArtifactStatus.RECLAIM_PENDING  # type: ignore[union-attr]

    with pytest.raises(P115PlaybackStateError, match="available artifact"):
        repository.reserve_lease(
            replace(
                lease_intent,
                artifact_id=second.id,
                expires_at=datetime.now(UTC) + timedelta(minutes=15),
            )
        )
    persisted_lease = repository.lease(rebound.id)
    assert persisted_lease is not None
    assert persisted_lease.expires_at <= datetime.now(UTC)
    assert repository.read(reclaim.id).status is PlaybackOperationStatus.READY  # type: ignore[union-attr]


def test_dead_ready_reclaim_restores_available_artifact(p6_runtime: DatabaseRuntime) -> None:
    repository = _repository(p6_runtime)
    artifact = _commit_artifact(
        repository,
        semantic_key="fixture-dead-reclaim-artifact",
        artifact_key="6" * 64,
        generation=1,
        remote_suffix="dead-reclaim",
    )
    reclaim = repository.reserve(_reclaim_intent("fixture-dead-ready-reclaim", artifact)).operation
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE outbox_events SET status='dead' WHERE aggregate_id=:operation_id"),
            {"operation_id": str(reclaim.id)},
        )

    assert repository.reject_dead_dispatches(limit=10) == 1
    rejected = repository.read(reclaim.id)
    restored = repository.artifact(artifact.id)
    assert rejected is not None and rejected.status is PlaybackOperationStatus.REJECTED
    assert restored is not None and restored.status is PlaybackArtifactStatus.AVAILABLE


def test_two_runtimes_share_one_remote_prepare_and_viewer_leases_fence_reclamation(
    p6_runtime: DatabaseRuntime,
) -> None:
    _enable_pool(p6_runtime)
    provider = RecordingProvider(prepare_delay=0.15)
    first_service = _service(p6_runtime, provider, worker_id="fixture-worker-a")
    second_service = _service(p6_runtime, provider, worker_id="fixture-worker-b")
    request = _pool_request("fixture-session-a", user="fixture-user-a", client="fixture-client-a")

    with ThreadPoolExecutor(max_workers=2) as executor:
        prepared_a, prepared_replay = tuple(
            executor.map(lambda service: service.prepare_play(request), (first_service, second_service))
        )

    assert prepared_a is not None and prepared_replay is not None
    assert prepared_a.artifact.id == prepared_replay.artifact.id
    assert prepared_a.lease.id == prepared_replay.lease.id
    assert provider.prepare_writes == 1

    prepared_b = second_service.prepare_play(
        _pool_request("fixture-session-b", user="fixture-user-b", client="fixture-client-b")
    )
    assert prepared_b is not None
    assert prepared_b.artifact.id == prepared_a.artifact.id
    assert prepared_b.lease.id != prepared_a.lease.id
    assert provider.prepare_writes == 1

    assert (
        first_service.release_for_stop(
            {
                "PlaybackInfo": {"PlaySessionId": "fixture-session-a"},
                "Item": {"Id": "fixture-item"},
                "User": {"Id": "fixture-user-a"},
            }
        )
        == 1
    )
    artifact = _repository(p6_runtime).artifact(prepared_a.artifact.id)
    assert artifact is not None and artifact.status is PlaybackArtifactStatus.AVAILABLE
    assert provider.reclaim_writes == 0

    assert (
        second_service.release_for_stop(
            {
                "PlaybackInfo": {"PlaySessionId": "fixture-session-b"},
                "Item": {"Id": "fixture-item"},
                "User": {"Id": "fixture-user-b"},
            }
        )
        == 1
    )
    reclaimed = _repository(p6_runtime).artifact(prepared_a.artifact.id)
    assert reclaimed is not None and reclaimed.status is PlaybackArtifactStatus.RECLAIMED
    assert provider.reclaim_writes == 1

    generation_two_request = _pool_request(
        "fixture-session-c",
        user="fixture-user-c",
        client="fixture-client-c",
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        prepared_generation_two, prepared_generation_two_replay = tuple(
            executor.map(
                lambda service: service.prepare_play(generation_two_request),
                (first_service, second_service),
            )
        )
    assert prepared_generation_two is not None and prepared_generation_two_replay is not None
    assert prepared_generation_two.artifact.generation == 2
    assert prepared_generation_two.artifact.id == prepared_generation_two_replay.artifact.id
    assert prepared_generation_two.artifact.id != prepared_a.artifact.id
    assert prepared_generation_two.lease.id == prepared_generation_two_replay.lease.id
    assert provider.prepare_writes == 2


def test_force_new_copy_reclaims_stale_artifact_and_rebinds_the_same_viewer_lease(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = _config(p6_runtime)
    config.save_temp_config(
        cid="fixture-temp-cid",
        cleanup_cron="0 * * * *",
        projection_key="1" * 64,
    )
    provider = RecordingProvider()
    service = _service(p6_runtime, provider, worker_id="fixture-copy-worker")
    request = PlaybackPrepareRequest(
        mode=PlaybackMode.COPY,
        source_pick_code="fixture-source-pick",
        file_name="Fixture.mkv",
        item_id="fixture-item",
        play_session_id="fixture-copy-session",
        user_id="fixture-user",
        client_key="fixture-client",
        user_agent="FixturePlayer/1.0",
        source="fixture-copy",
    )

    first = service.prepare_play(request)
    assert first is not None
    replacement = service.prepare_play(
        request,
        force_new=True,
        failed_remote_pick_code=first.artifact.remote_pick_code,
    )

    assert replacement is not None
    assert first.artifact.status is PlaybackArtifactStatus.AVAILABLE
    assert replacement.artifact.status is PlaybackArtifactStatus.AVAILABLE
    assert replacement.artifact.generation == first.artifact.generation + 1
    assert replacement.artifact.id != first.artifact.id
    assert replacement.artifact.remote_pick_code != first.artifact.remote_pick_code
    assert replacement.lease.id == first.lease.id
    assert replacement.lease.artifact_id == replacement.artifact.id
    assert provider.prepare_writes == 2
    assert provider.reclaim_writes == 1
    stale = _repository(p6_runtime).artifact(first.artifact.id)
    assert stale is not None and stale.status is PlaybackArtifactStatus.RECLAIMED

    delayed_stale_replay = service.prepare_play(
        request,
        force_new=True,
        failed_remote_pick_code=first.artifact.remote_pick_code,
    )
    assert delayed_stale_replay is not None
    assert delayed_stale_replay.artifact.id == replacement.artifact.id
    assert delayed_stale_replay.artifact.generation == replacement.artifact.generation
    assert delayed_stale_replay.lease.id == replacement.lease.id
    assert provider.prepare_writes == 2
    assert provider.reclaim_writes == 1


def test_reclaimed_artifact_reuses_the_latest_in_flight_prepare_generation(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = _config(p6_runtime)
    config.save_temp_config(cid="fixture-temp-cid", cleanup_cron="0 * * * *", projection_key="2" * 64)
    provider = RecordingProvider()
    service = _service(p6_runtime, provider, worker_id="fixture-generation-worker")
    request = PlaybackPrepareRequest(
        mode=PlaybackMode.COPY,
        source_pick_code="fixture-source-pick",
        file_name="Fixture.mkv",
        item_id="fixture-item",
        play_session_id="fixture-generation-session",
        user_id="fixture-user",
        client_key="fixture-client",
        source="fixture-copy",
    )
    first = service.prepare_play(request)
    assert first is not None
    repository = _repository(p6_runtime)
    repository.release_lease(first.lease.id, cleanup_delay_seconds=0)
    assert service.cleanup() == 1
    digest = first.artifact.artifact_key
    generation_two = repository.reserve(_prepare_intent(f"prepare:{digest}:2", digest, 2)).operation
    assert generation_two.status is PlaybackOperationStatus.READY

    prepared = service.prepare_play(request)

    assert prepared is not None
    assert prepared.artifact.generation == 2
    assert provider.prepare_writes == 2
    with p6_runtime.engine.connect() as connection:
        generations = (
            connection.execute(
                text(
                    "SELECT artifact_generation FROM p115_playback_operations "
                    "WHERE kind='prepare' AND artifact_key=:artifact_key ORDER BY artifact_generation"
                ),
                {"artifact_key": digest},
            )
            .scalars()
            .all()
        )
    assert generations == [1, 2]


def test_indeterminate_latest_prepare_generation_blocks_new_remote_mutation(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = _config(p6_runtime)
    config.save_temp_config(cid="fixture-temp-cid", cleanup_cron="0 * * * *", projection_key="3" * 64)
    provider = RecordingProvider()
    service = _service(p6_runtime, provider, worker_id="fixture-indeterminate-worker")
    request = PlaybackPrepareRequest(
        mode=PlaybackMode.COPY,
        source_pick_code="fixture-source-pick",
        file_name="Fixture.mkv",
        item_id="fixture-item",
        play_session_id="fixture-indeterminate-session",
        user_id="fixture-user",
        client_key="fixture-client",
        source="fixture-copy",
    )
    first = service.prepare_play(request)
    assert first is not None
    repository = _repository(p6_runtime)
    repository.release_lease(first.lease.id, cleanup_delay_seconds=0)
    assert service.cleanup() == 1
    digest = first.artifact.artifact_key
    generation_two = repository.reserve(_prepare_intent(f"prepare:{digest}:2", digest, 2)).operation
    claimed = repository.claim_by_id(generation_two.id, worker_id="fixture-indeterminate", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.mark_indeterminate(
        claimed.id,
        claimed.claim_token,
        error={"type": "fixture_unknown_remote_result"},
    )

    with pytest.raises(P115PlaybackStateError, match="禁止重复写入"):
        service.prepare_play(request)

    assert provider.prepare_writes == 1
    assert repository.latest_artifact_generation(digest) == 2


def test_prepare_rejection_advances_at_most_one_generation_per_request(
    p6_runtime: DatabaseRuntime,
) -> None:
    config = _config(p6_runtime)
    config.save_temp_config(cid="fixture-temp-cid", cleanup_cron="0 * * * *", projection_key="4" * 64)
    provider = RecordingProvider(prepare_rejections=100)
    service = _service(p6_runtime, provider, worker_id="fixture-rejection-worker")
    request = PlaybackPrepareRequest(
        mode=PlaybackMode.COPY,
        source_pick_code="fixture-source-pick",
        file_name="Fixture.mkv",
        item_id="fixture-item",
        play_session_id="fixture-rejection-session",
        user_id="fixture-user",
        client_key="fixture-client",
        source="fixture-copy",
    )

    with pytest.raises(P115PlaybackStateError, match="fixture prepare rejected"):
        service.prepare_play(request)
    assert provider.prepare_writes == 1

    with pytest.raises(P115PlaybackStateError, match="fixture prepare rejected"):
        service.prepare_play(request)
    assert provider.prepare_writes == 2
    with p6_runtime.engine.connect() as connection:
        statuses = connection.execute(
            text(
                "SELECT artifact_generation, status FROM p115_playback_operations "
                "WHERE kind='prepare' ORDER BY artifact_generation"
            )
        ).all()
    assert statuses == [(1, "rejected"), (2, "rejected")]


def test_known_rejected_temp_dir_can_retry_once_without_replaying_indeterminate_effect(
    p6_runtime: DatabaseRuntime,
) -> None:
    provider = RecordingProvider(ensure_rejections=1)
    service = _service(p6_runtime, provider, worker_id="fixture-worker")

    with pytest.raises(P115PlaybackStateError, match="fixture known rejection"):
        service.add_account(
            {
                "alias": "Fixture Retry",
                "access_token": "fixture-access-token",
                "refresh_token": "fixture-refresh-token",
                "_skip_auto_speedtest": True,
            }
        )

    account = _config(p6_runtime).snapshot()["accounts"][0]
    updated = service.update_account(str(account["id"]), {"alias": "Fixture Retry 2"})

    assert updated["id"] == account["id"]
    assert _config(p6_runtime).account(str(account["id"]))["temp_cid"] == "fixture-temp-cid"  # type: ignore[index]
    assert provider.ensure_writes == 2
    with p6_runtime.engine.connect() as connection:
        statuses = (
            connection.execute(
                text(
                    "SELECT status FROM p115_playback_operations WHERE kind = 'ensure_temp_dir' ORDER BY event_sequence"
                )
            )
            .scalars()
            .all()
        )
    assert statuses == ["rejected", "confirmed"]


class ReadbackAfterCrashProvider:
    def __init__(self) -> None:
        self.execute_calls = 0
        self.readback_calls = 0

    def execute(self, operation: PlaybackOperation, *, checkpoint: Any) -> PlaybackProviderOutcome:
        del operation, checkpoint
        self.execute_calls += 1
        raise AssertionError("recovered request must use readback before another mutation")

    def readback(self, operation: PlaybackOperation) -> PlaybackProviderOutcome:
        self.readback_calls += 1
        return PlaybackProviderOutcome(
            PlaybackProviderDisposition.CONFIRMED,
            request_sent=True,
            receipt={
                "target_cid": str(operation.request["target_cid"]),
                "remote_fid": "fixture-recovered-fid",
                "remote_pick_code": "fixture-recovered-pick",
                "file_name": "Fixture.mkv",
                "created_remote": True,
            },
            readback={"found": True},
        )


def test_request_checkpoint_survives_claim_expiry_and_restart_reads_back_before_mutating(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    artifact_key = "c" * 64
    intent = PlaybackOperationIntent.create(
        semantic_key="fixture-response-lost",
        kind=PlaybackOperationKind.PREPARE,
        mode=PlaybackMode.COPY,
        account_id="fixture-primary",
        provider_backend="openapi",
        artifact_key=artifact_key,
        artifact_generation=1,
        request={
            "artifact_key": artifact_key,
            "artifact_generation": 1,
            "target_cid": "fixture-temp-cid",
            "file_name": "Fixture.mkv",
            "source": {
                "sha1": _SOURCE_SHA1,
                "size": 1024,
                "pick_code": "fixture-source-pick",
                "fid": "fixture-source-fid",
            },
        },
    )
    operation = repository.reserve(intent).operation
    claimed = repository.claim_by_id(operation.id, worker_id="crashed-worker", lease_seconds=30)
    assert claimed is not None and claimed.claim_token is not None
    repository.checkpoint_request(
        claimed.id,
        claimed.claim_token,
        session={"phase": "copy", "provider_request_id": "fixture-request-id"},
        lease_seconds=30,
    )
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE p115_playback_operations "
                "SET claimed_at = NOW() - INTERVAL '2 seconds', "
                "lease_expires_at = NOW() - INTERVAL '1 second' WHERE id = :id"
            ),
            {"id": operation.id},
        )

    assert repository.expire_stale_claims(limit=10) == 1
    awaiting = repository.read(operation.id)
    assert awaiting is not None
    assert awaiting.status is PlaybackOperationStatus.AWAITING_READBACK
    assert awaiting.provider_session == {
        "account_id": "fixture-primary",
        "backend": "openapi",
        "kind": "prepare",
        "phase": "copy",
        "provider_request_id": "fixture-request-id",
    }

    provider = ReadbackAfterCrashProvider()
    recovered = P115PlaybackOperationExecutor(
        repository=repository,
        provider=provider,
        config=cast(Any, object()),
        worker_id="replacement-worker",
        lease_seconds=30,
    ).execute(operation.id)

    assert recovered is not None and recovered.status is PlaybackOperationStatus.CONFIRMED
    assert provider.execute_calls == 0
    assert provider.readback_calls == 1
    artifact = repository.artifact_by_key(artifact_key)
    assert artifact is not None and artifact.status is PlaybackArtifactStatus.AVAILABLE
    assert artifact.remote_fid == "fixture-recovered-fid"


def test_qr_sessions_are_owner_scoped_and_expiry_destroys_credentials(
    p6_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p6_runtime)
    session = repository.create_qr_session(
        owner_user_id="fixture-owner-a",
        mode="openapi",
        app_type="alipaymini",
        provider_session=P115AuthorizationSession(
            uid="fixture-provider-uid",
            created_at=1.0,
            time_value="fixture-time",
            sign="fixture-sign",
            app_type="alipaymini",
            code_verifier="fixture-code-verifier",
        ),
        ttl_seconds=600,
    )
    assert (
        repository.qr_session(
            owner_user_id="fixture-owner-b",
            provider_uid="fixture-provider-uid",
        )
        is None
    )

    succeeded = repository.update_qr_session(
        session.id,
        status=PlaybackQrStatus.SUCCEEDED,
        credentials={"access_token": "fixture-access-token", "refresh_token": "fixture-refresh-token"},
    )
    assert succeeded.credentials == {
        "access_token": "fixture-access-token",
        "refresh_token": "fixture-refresh-token",
    }
    assert succeeded.provider_session.sign == ""
    assert succeeded.provider_session.code_verifier == ""
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("UPDATE p115_playback_qr_sessions SET expires_at = NOW() - INTERVAL '1 second' WHERE id = :id"),
            {"id": session.id},
        )

    assert repository.expire_qr_sessions(limit=10) == 1
    expired = repository.qr_session(
        owner_user_id="fixture-owner-a",
        provider_uid="fixture-provider-uid",
    )

    assert expired is not None and expired.status is PlaybackQrStatus.EXPIRED
    assert expired.credentials is None
    assert expired.provider_session.code_verifier == ""
    with p6_runtime.engine.connect() as connection:
        row = connection.execute(
            text("SELECT credentials_json, provider_session_json FROM p115_playback_qr_sessions WHERE id = :id"),
            {"id": session.id},
        ).one()
    assert row.credentials_json is None
    assert dict(row.provider_session_json)["code_verifier"] == ""

    provider_expired = repository.create_qr_session(
        owner_user_id="fixture-owner-a",
        mode="openapi",
        app_type="alipaymini",
        provider_session=P115AuthorizationSession(
            uid="fixture-provider-expired",
            created_at=1.0,
            time_value="fixture-time",
            sign="fixture-sign",
            app_type="alipaymini",
            code_verifier="fixture-code-verifier",
        ),
        ttl_seconds=600,
    )
    provider_expired = repository.update_qr_session(
        provider_expired.id,
        status=PlaybackQrStatus.EXPIRED,
    )
    assert provider_expired.provider_session.sign == ""
    assert provider_expired.provider_session.code_verifier == ""
