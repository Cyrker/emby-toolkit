from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from etk.domains.p115.playback import (
    PLAYBACK_AGGREGATE_TYPE,
    PLAYBACK_TOPIC,
    PLAYBACK_TOPICS,
    P115PlaybackConfigService,
    P115PlaybackProvider,
    PlaybackArtifactIntent,
    PlaybackMode,
    PlaybackOperation,
    PlaybackOperationKind,
    PlaybackOperationStatus,
    PlaybackProviderDisposition,
    PlaybackProviderOutcome,
    human_bytes,
    safe_receipt,
    speedtest_threshold_mbps,
)
from etk.platform.jobs import OutboxClaim, PostgresJobStore, PublishBatchResult

from .celery_app import EXECUTE_P115_PLAYBACK_OPERATION_TASK, create_celery_app
from .runtime_config import TaskRuntimeConfig

P115_PLAYBACK_CLAIM_LEASE_SECONDS = 900
P115_PLAYBACK_MAX_ATTEMPTS = 4
P115_PLAYBACK_READBACK_ATTEMPTS = 3
P115_PLAYBACK_RETRY_SECONDS = 5


class CeleryP115PlaybackPublisher:
    def __init__(self, config: TaskRuntimeConfig, *, celery_app: Any | None = None) -> None:
        self._config = config
        self._app = celery_app or create_celery_app(config)

    def publish(self, claim: OutboxClaim) -> None:
        event = claim.event
        if event.topic != PLAYBACK_TOPIC or event.aggregate_type != PLAYBACK_AGGREGATE_TYPE:
            raise ValueError("P115 playback publisher received an unsupported Outbox event")
        if event.payload_version != 1:
            raise ValueError("P115 playback Outbox payload version is unsupported")
        payload = event.payload
        operation_id = _uuid(payload.get("operation_id"), "operation_id")
        if event.aggregate_id != str(operation_id):
            raise ValueError("P115 playback Outbox aggregate does not match operation_id")
        if int(payload.get("sequence") or 0) <= 0:
            raise ValueError("P115 playback Outbox sequence is invalid")
        operation_key = str(payload.get("operation_key") or "").strip()
        request_hash = str(payload.get("request_hash") or "").strip()
        if len(operation_key) != 64 or len(request_hash) != 64:
            raise ValueError("P115 playback Outbox identity is invalid")
        try:
            bytes.fromhex(operation_key)
            bytes.fromhex(request_hash)
        except ValueError as exc:
            raise ValueError("P115 playback Outbox identity is invalid") from exc
        self._app.send_task(
            EXECUTE_P115_PLAYBACK_OPERATION_TASK,
            kwargs={"operation_id": str(operation_id)},
            task_id=str(operation_id),
            queue=self._config.queue_name,
            routing_key=self._config.queue_name,
            serializer="json",
        )


@dataclass(frozen=True, slots=True)
class P115PlaybackCoordinatorResult:
    expired_claims: int
    expired_leases: int
    expired_qr_sessions: int
    rejected_dead: int
    publish: PublishBatchResult


class P115PlaybackCoordinator:
    def __init__(
        self,
        *,
        store: PostgresJobStore,
        repository: Any,
        publisher: CeleryP115PlaybackPublisher,
        config: TaskRuntimeConfig,
        publisher_id: str,
    ) -> None:
        self._store = store
        self._repository = repository
        self._publisher = publisher
        self._config = config
        self._publisher_id = str(publisher_id or "").strip()
        if not self._publisher_id:
            raise ValueError("P115 playback publisher id must not be empty")

    def tick(self) -> P115PlaybackCoordinatorResult:
        limit = self._config.outbox_batch_size
        expired_claims = self._repository.expire_stale_claims(limit=limit)
        expired_leases = len(self._repository.expire_leases(limit=limit))
        expired_qr_sessions = self._repository.expire_qr_sessions(limit=limit)
        self._store.requeue_stale_published(
            limit=limit,
            processed_timeout_seconds=max(self._config.lease_seconds * 2, 60),
            topics=PLAYBACK_TOPICS,
        )
        rejected_dead = self._repository.reject_dead_dispatches(limit=limit)
        publish = self._store.publish_outbox_batch(
            self._publisher,
            publisher_id=self._publisher_id,
            limit=limit,
            claim_timeout_seconds=self._config.outbox_claim_timeout_seconds,
            failure_backoff_seconds=2,
            topics=PLAYBACK_TOPICS,
        )
        if publish.dead:
            rejected_dead += self._repository.reject_dead_dispatches(limit=limit)
        return P115PlaybackCoordinatorResult(
            expired_claims=expired_claims,
            expired_leases=expired_leases,
            expired_qr_sessions=expired_qr_sessions,
            rejected_dead=rejected_dead,
            publish=publish,
        )


class P115PlaybackOperationExecutor:
    def __init__(
        self,
        *,
        repository: Any,
        provider: P115PlaybackProvider,
        config: P115PlaybackConfigService,
        worker_id: str,
        lease_seconds: float = P115_PLAYBACK_CLAIM_LEASE_SECONDS,
    ) -> None:
        self._repository = repository
        self._provider = provider
        self._config = config
        self._worker_id = str(worker_id or "").strip()
        self._lease_seconds = float(lease_seconds)
        if not self._worker_id or self._lease_seconds <= 0:
            raise ValueError("P115 playback Worker identity and lease must be valid")

    def execute(self, operation_id: UUID) -> PlaybackOperation | None:
        current = self._repository.read(operation_id)
        if current is None or current.status.terminal:
            return current
        claimed = self._repository.claim_by_id(
            operation_id,
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            return self._repository.read(operation_id)
        token = _claim_token(claimed)
        operation = claimed
        if operation.remote_receipt is None:
            outcome = (
                self._provider.readback(operation)
                if operation.status is PlaybackOperationStatus.AWAITING_READBACK
                else self._provider.execute(operation, checkpoint=self._checkpoint(operation, token))
            )
            operation = self._handle_provider_outcome(operation, token, outcome)
            if operation.status is not PlaybackOperationStatus.REMOTE_CONFIRMED:
                return operation
        try:
            return self._project(operation, token)
        except Exception as exc:
            error = {"type": "playback_local_projection_failed", "message": str(exc)[:500]}
            if operation.attempts < P115_PLAYBACK_MAX_ATTEMPTS:
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    error=error,
                    retry_after_seconds=P115_PLAYBACK_RETRY_SECONDS,
                ).operation
            return self._repository.mark_indeterminate(operation.id, token, error=error).operation

    def _project(self, operation: PlaybackOperation, token: UUID) -> PlaybackOperation:
        receipt = safe_receipt(operation.remote_receipt or {}, field="playback remote receipt")
        if operation.kind is PlaybackOperationKind.PREPARE:
            artifact = self._artifact_intent(operation, receipt)
            if operation.mode is PlaybackMode.POOL:
                traffic_delta = int(artifact.source_size) if bool(receipt.get("created_remote")) else 0
                self._config.apply_account_projection(
                    operation.operation_key,
                    operation.account_id,
                    patch={"temp_cid": artifact.target_cid, "last_error": ""},
                    traffic_bytes_delta=traffic_delta,
                )
            result = {
                "artifact_key": artifact.artifact_key,
                "artifact_generation": artifact.generation,
                "remote_fid": str(receipt.get("remote_fid") or ""),
                "remote_pick_code": str(receipt.get("remote_pick_code") or ""),
                "created_remote": bool(receipt.get("created_remote")),
            }
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result=result,
                artifact=artifact,
            ).operation
        if operation.kind is PlaybackOperationKind.ENSURE_TEMP_DIR:
            cid = str(receipt.get("cid") or "").strip()
            if not cid:
                raise RuntimeError("confirmed temp-dir outcome has no cid")
            account_kind = str(operation.request.get("account_kind") or "")
            if account_kind == "pool":
                self._config.apply_account_projection(
                    operation.operation_key,
                    operation.account_id,
                    patch={"temp_cid": cid, "last_error": ""},
                )
            elif account_kind == "primary":
                self._config.save_temp_config(
                    cid=cid,
                    cleanup_cron=str(operation.request.get("cleanup_cron") or "0 * * * *"),
                    projection_key=operation.operation_key,
                )
            else:
                raise RuntimeError("temp-dir operation account_kind is invalid")
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result={"cid": cid, "created_remote": bool(receipt.get("created_remote"))},
            ).operation
        if operation.kind is PlaybackOperationKind.SPEEDTEST:
            bps = max(0, int(receipt.get("bps") or 0))
            threshold = speedtest_threshold_mbps(self._config.snapshot().get("auto_speedtest_threshold_mbps"))
            threshold_bps = int(threshold * 1024 * 1024)
            enabled = bps >= threshold_bps if threshold_bps > 0 else True
            error = "" if enabled else f"测速 {human_bytes(bps)}/s 低于阈值 {human_bytes(threshold_bps)}/s，已自动停用"
            self._config.apply_account_projection(
                operation.operation_key,
                operation.account_id,
                patch={
                    "enabled": enabled,
                    "last_error": error,
                    "last_speed_bps": bps,
                    "last_speed_at": datetime.now(UTC).isoformat(),
                },
            )
            result = {
                "bytes": max(0, int(receipt.get("bytes") or 0)),
                "seconds": float(receipt.get("seconds") or 0),
                "bps": bps,
                "speed_text": str(receipt.get("speed_text") or f"{human_bytes(bps)}/s"),
            }
            return self._repository.mark_confirmed(operation.id, token, result=result).operation
        if operation.kind is PlaybackOperationKind.RECLAIM:
            if not bool(receipt.get("deleted")):
                raise RuntimeError("confirmed playback reclamation did not prove deletion")
            return self._repository.mark_confirmed(
                operation.id,
                token,
                result={
                    "deleted": True,
                    "artifact_id": str(operation.request.get("artifact_id") or ""),
                    "readback": dict(operation.readback or {}),
                },
            ).operation
        raise RuntimeError(f"unsupported playback operation kind: {operation.kind.value}")

    def _artifact_intent(
        self,
        operation: PlaybackOperation,
        receipt: Mapping[str, Any],
    ) -> PlaybackArtifactIntent:
        source = operation.request.get("source")
        if not isinstance(source, Mapping):
            raise RuntimeError("playback prepare operation source snapshot is missing")
        artifact_key = str(operation.artifact_key or operation.request.get("artifact_key") or "")
        generation = int(operation.artifact_generation or operation.request.get("artifact_generation") or 0)
        target_cid = str(receipt.get("target_cid") or operation.request.get("target_cid") or "").strip()
        remote_fid = str(receipt.get("remote_fid") or "").strip()
        remote_pick_code = str(receipt.get("remote_pick_code") or "").strip()
        if not artifact_key or generation <= 0 or not target_cid or not remote_fid or not remote_pick_code:
            raise RuntimeError("playback artifact receipt is incomplete")
        return PlaybackArtifactIntent(
            artifact_key=artifact_key,
            generation=generation,
            operation_id=operation.id,
            mode=operation.mode,
            account_id=operation.account_id,
            provider_backend=operation.provider_backend,
            target_cid=target_cid,
            source_sha1=str(source.get("sha1") or ""),
            source_size=max(0, int(source.get("size") or 0)),
            source_pick_code=str(source.get("pick_code") or ""),
            source_fid=str(source.get("fid") or ""),
            file_name=str(
                receipt.get("file_name") or operation.request.get("file_name") or source.get("file_name") or ""
            ),
            receipt=dict(receipt),
            reclaim_not_before=datetime.now(UTC),
        )

    def _checkpoint(self, operation: PlaybackOperation, token: UUID):
        def persist(session: Mapping[str, Any]) -> None:
            normalized = dict(session)
            pinned = {
                "account_id": operation.account_id,
                "backend": operation.provider_backend,
                "kind": operation.kind.value,
            }
            for field, expected in pinned.items():
                candidate = normalized.get(field)
                if candidate is not None and str(candidate) != expected:
                    raise ValueError(f"playback provider checkpoint {field} changed")
            normalized.update(pinned)
            self._repository.checkpoint_request(
                operation.id,
                token,
                session=normalized,
                lease_seconds=self._lease_seconds,
            )

        return persist

    def _handle_provider_outcome(
        self,
        operation: PlaybackOperation,
        token: UUID,
        outcome: PlaybackProviderOutcome,
    ) -> PlaybackOperation:
        if (
            outcome.request_sent
            and outcome.session is not None
            and operation.status
            in {
                PlaybackOperationStatus.CLAIMED,
                PlaybackOperationStatus.REQUESTING,
            }
        ):
            self._checkpoint(operation, token)(outcome.session)
        if outcome.disposition is PlaybackProviderDisposition.CONFIRMED:
            if outcome.receipt is None:
                raise RuntimeError("confirmed P115 playback outcome has no receipt")
            return self._repository.mark_remote_confirmed(
                operation.id,
                token,
                outcome.receipt,
                readback=outcome.readback,
            ).operation
        error = dict(outcome.error or {"type": "playback_provider_failure"})
        if outcome.disposition is PlaybackProviderDisposition.REJECTED:
            return self._repository.mark_rejected(operation.id, token, error=error).operation
        if not outcome.request_sent and outcome.disposition in {
            PlaybackProviderDisposition.RETRYABLE,
            PlaybackProviderDisposition.INDETERMINATE,
        }:
            if operation.attempts < P115_PLAYBACK_MAX_ATTEMPTS:
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    error=error,
                    retry_after_seconds=P115_PLAYBACK_RETRY_SECONDS,
                ).operation
            return self._repository.mark_rejected(
                operation.id,
                token,
                error={**error, "type": "playback_retry_budget_exhausted"},
            ).operation
        if operation.attempts < P115_PLAYBACK_READBACK_ATTEMPTS:
            return self._repository.mark_awaiting_readback(
                operation.id,
                token,
                session=outcome.session,
                error=error,
                retry_after_seconds=P115_PLAYBACK_RETRY_SECONDS,
            ).operation
        return self._repository.mark_indeterminate(operation.id, token, error=error).operation


def _claim_token(operation: PlaybackOperation) -> UUID:
    if operation.claim_token is None:
        raise RuntimeError("P115 playback operation claim token is missing")
    return operation.claim_token


def _uuid(value: object, field: str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a UUID") from exc


__all__ = [
    "CeleryP115PlaybackPublisher",
    "P115PlaybackCoordinator",
    "P115PlaybackCoordinatorResult",
    "P115PlaybackOperationExecutor",
]
