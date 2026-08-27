from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from uuid import UUID

from .effects import (
    CollectionAssetEffectsPort,
    CollectionDeletionSuppressionPort,
    CollectionRemoteEffectsPort,
    CollectionRemoteIndeterminate,
    CollectionRemoteRejected,
    RemoteCollectionState,
)
from .operation_repository import PostgresCollectionOperationRepository
from .operations import (
    CollectionOperation,
    CollectionOperationIndeterminateBlockerError,
    CollectionOperationKind,
    CollectionOperationScopeBusyError,
    CollectionOperationStateError,
    CollectionOperationStatus,
    CollectionOperationStep,
)

COLLECTION_OPERATION_READBACK_ATTEMPTS = 4
COLLECTION_OPERATION_TOTAL_ATTEMPTS = 12
COLLECTION_OPERATION_RETRY_DELAY_SECONDS = 1.0


class CollectionOperationReconciler:
    def __init__(
        self,
        repository: PostgresCollectionOperationRepository,
        remote: CollectionRemoteEffectsPort,
    ) -> None:
        self._repository = repository
        self._remote = remote

    def reconcile(
        self,
        operation_id: UUID,
        *,
        expected_state_revision: int,
        resolved_by: str,
    ) -> CollectionOperation | None:
        operation = self._repository.read(operation_id)
        if operation is None:
            return None
        if operation.status is not CollectionOperationStatus.INDETERMINATE:
            raise CollectionOperationStateError("collection operation is not indeterminate")
        if operation.state_revision != int(expected_state_revision):
            raise CollectionOperationStateError("collection operation state revision changed")
        if operation.kind not in {
            CollectionOperationKind.NATIVE_DELETE,
            CollectionOperationKind.REMOVAL_SURVIVAL_CHECK,
        }:
            raise CollectionOperationStateError("collection operation kind does not support provider reconciliation")
        collection_id = _request_text(operation, "emby_collection_id")
        remote = self._remote.read_collection(collection_id)
        expected_tmdb_id = str(operation.request.get("expected_tmdb_collection_id") or "").strip() or None
        if remote is not None and not _matches_expected_tmdb(remote, expected_tmdb_id):
            raise CollectionRemoteIndeterminate("Emby collection identity changed before reconciliation")
        observation = {
            "emby_collection_id": collection_id,
            "provider_present": remote is not None,
            "tmdb_collection_id": remote.tmdb_collection_id if remote is not None else None,
            "member_ids": list(remote.member_ids) if remote is not None else [],
        }
        return self._repository.reconcile_indeterminate(
            operation_id,
            expected_state_revision=expected_state_revision,
            provider_present=remote is not None,
            resolved_by=resolved_by,
            observation=observation,
        )


class CollectionOperationExecutor:
    def __init__(
        self,
        repository: PostgresCollectionOperationRepository,
        remote: CollectionRemoteEffectsPort,
        assets: CollectionAssetEffectsPort,
        suppression: CollectionDeletionSuppressionPort,
        *,
        worker_id: str,
        lease_seconds: float | Callable[[], float],
        readback_attempts: int = COLLECTION_OPERATION_READBACK_ATTEMPTS,
        retry_delay_seconds: float = COLLECTION_OPERATION_RETRY_DELAY_SECONDS,
    ) -> None:
        self._repository = repository
        self._remote = remote
        self._assets = assets
        self._suppression = suppression
        self._worker_id = _required_text(worker_id, "worker_id")
        self._lease_seconds = lease_seconds
        self._readback_attempts = max(1, int(readback_attempts))
        self._retry_delay_seconds = max(0.0, float(retry_delay_seconds))

    def execute(self, operation_id: UUID) -> CollectionOperation | None:
        current = self._repository.read(operation_id)
        if current is None:
            return None
        claimed = self._repository.claim_by_id(
            operation_id,
            worker_id=self._worker_id,
            lease_seconds=max(
                1.0,
                float(self._lease_seconds() if callable(self._lease_seconds) else self._lease_seconds),
            ),
        )
        if claimed is None:
            return self._repository.read(operation_id)
        token = claimed.claim_token
        if token is None:
            raise RuntimeError("claimed collection operation is missing its claim token")
        try:
            if claimed.kind is CollectionOperationKind.NATIVE_DELETE:
                return self._execute_native_delete(claimed, token)
            if claimed.kind is CollectionOperationKind.CUSTOM_DELETE:
                return self._execute_custom_delete(claimed, token)
            if claimed.kind is CollectionOperationKind.MISSING_CLEANUP:
                return self._execute_missing_cleanup(claimed, token)
            if claimed.kind is CollectionOperationKind.IMAGE_ARCHIVE:
                return self._execute_image_archive(claimed, token)
            if claimed.kind is CollectionOperationKind.REMOVAL_SURVIVAL_CHECK:
                return self._execute_survival_check(claimed, token)
            raise ValueError(f"unsupported collection operation kind: {claimed.kind.value}")
        except CollectionRemoteRejected as exc:
            if claimed.step_started_at is not None:
                return self._defer_post_checkpoint(claimed, token, exc)
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error=_error("provider_rejected", exc),
            )
        except CollectionRemoteIndeterminate as exc:
            if claimed.step_started_at is not None:
                return self._defer_post_checkpoint(claimed, token, exc)
            if claimed.attempts >= COLLECTION_OPERATION_TOTAL_ATTEMPTS:
                return self._repository.mark_rejected(
                    claimed.id,
                    token,
                    state=claimed.state,
                    error={
                        "type": "prewrite_recovery_exhausted",
                        "attempts": claimed.attempts,
                        "cause": type(exc).__name__,
                    },
                )
            return self._repository.mark_retryable(
                claimed.id,
                token,
                state=claimed.state,
                error=_error("provider_unavailable", exc),
                retry_after_seconds=self._retry_delay_seconds,
            )
        except (TypeError, ValueError) as exc:
            if claimed.step_started_at is not None:
                return self._repository.mark_indeterminate(
                    claimed.id,
                    token,
                    state=claimed.state,
                    error=_error("invalid_checkpointed_operation", exc),
                )
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error=_error("invalid_operation", exc),
            )

    def _execute_native_delete(
        self,
        operation: CollectionOperation,
        token: UUID,
    ) -> CollectionOperation:
        collection_id = _request_text(operation, "emby_collection_id")
        expected_tmdb_id = str(operation.request.get("expected_tmdb_collection_id") or "").strip() or None
        if operation.step_started_at is not None:
            if operation.current_step is CollectionOperationStep.DELETE_SHELL:
                return self._native_delete_readback(operation, token, collection_id, expected_tmdb_id)
            if operation.current_step is CollectionOperationStep.REMOVE_MEMBERS:
                try:
                    remote = self._remote.read_collection(collection_id)
                except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                    return self._defer_post_checkpoint(operation, token, exc)
                if remote is None:
                    return self._complete_native_delete(operation, token, collection_id, recovered=True)
                if not _matches_expected_tmdb(remote, expected_tmdb_id):
                    return self._defer_post_checkpoint(
                        operation,
                        token,
                        CollectionRemoteIndeterminate("Emby collection identity changed during delete recovery"),
                    )
                state = _state(operation, remove_members_recovered_by_readback=True)
                continued = self._repository.continue_claim(
                    operation.id,
                    token,
                    next_step=CollectionOperationStep.DELETE_SHELL,
                    state=state,
                )
                return self._send_native_delete(continued, token, collection_id, expected_tmdb_id)
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                state=operation.state,
                error={"type": "unsupported_provider_checkpoint", "step": operation.current_step.value},
            )

        remote = self._remote.read_collection(collection_id)
        if remote is None:
            return self._complete_native_delete(operation, token, collection_id, recovered=False)
        if not _matches_expected_tmdb(remote, expected_tmdb_id):
            raise CollectionRemoteRejected("Emby collection TMDb identity does not match the delete request")
        self._suppression.suppress(collection_id, ttl_seconds=10)
        state = {
            "emby_collection_id": collection_id,
            "expected_tmdb_collection_id": expected_tmdb_id,
            "warnings": [],
        }
        requesting = self._repository.begin_provider_step(
            operation.id,
            token,
            step=CollectionOperationStep.REMOVE_MEMBERS,
            state=state,
        )
        warnings = _warnings(requesting)
        try:
            self._remote.empty_collection(remote)
        except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
            warnings.append(_error("remove_members_failed", exc))
        continued = self._repository.continue_claim(
            operation.id,
            token,
            next_step=CollectionOperationStep.DELETE_SHELL,
            state={**dict(requesting.state), "warnings": warnings},
        )
        return self._send_native_delete(continued, token, collection_id, expected_tmdb_id)

    def _send_native_delete(
        self,
        operation: CollectionOperation,
        token: UUID,
        collection_id: str,
        expected_tmdb_id: str | None,
    ) -> CollectionOperation:
        self._suppression.suppress(collection_id, ttl_seconds=10)
        requesting = self._repository.begin_provider_step(
            operation.id,
            token,
            step=CollectionOperationStep.DELETE_SHELL,
            state=operation.state,
        )
        explicit_rejection: CollectionRemoteRejected | None = None
        try:
            self._remote.delete_collection(collection_id)
        except CollectionRemoteRejected as exc:
            explicit_rejection = exc
        except CollectionRemoteIndeterminate:
            pass
        return self._native_delete_readback(
            requesting,
            token,
            collection_id,
            expected_tmdb_id,
            explicit_rejection=explicit_rejection,
        )

    def _native_delete_readback(
        self,
        operation: CollectionOperation,
        token: UUID,
        collection_id: str,
        expected_tmdb_id: str | None,
        *,
        explicit_rejection: CollectionRemoteRejected | None = None,
    ) -> CollectionOperation:
        try:
            remote = self._remote.read_collection(collection_id)
        except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
            return self._defer_post_checkpoint(operation, token, exc)
        if remote is None:
            return self._complete_native_delete(operation, token, collection_id, recovered=True)
        if not _matches_expected_tmdb(remote, expected_tmdb_id):
            return self._defer_post_checkpoint(
                operation,
                token,
                CollectionRemoteIndeterminate("Emby collection identity changed before delete confirmation"),
            )
        if explicit_rejection is not None:
            return self._repository.mark_rejected(
                operation.id,
                token,
                state=operation.state,
                error=_error("delete_rejected", explicit_rejection),
            )
        return self._defer_post_checkpoint(
            operation,
            token,
            CollectionRemoteIndeterminate("Emby collection still exists after the delete request"),
        )

    def _complete_native_delete(
        self,
        operation: CollectionOperation,
        token: UUID,
        collection_id: str,
        *,
        recovered: bool,
    ) -> CollectionOperation:
        return self._repository.complete_native_delete(
            operation.id,
            token,
            emby_collection_id=collection_id,
            state=_state(operation, provider_absence_confirmed=True),
            result={
                "operation": operation.kind.value,
                "emby_collection_id": collection_id,
                "confirmed_absent": True,
                "recovered_by_readback": recovered,
            },
        )

    def _execute_custom_delete(
        self,
        operation: CollectionOperation,
        token: UUID,
    ) -> CollectionOperation:
        collection_id = str(operation.request.get("emby_collection_id") or "").strip() or None
        warnings = _warnings(operation)
        if collection_id and operation.step_started_at is not None:
            try:
                remote = self._remote.read_collection(collection_id)
                if remote is not None and remote.member_ids:
                    warnings.append({"type": "custom_empty_unconfirmed", "remaining_members": len(remote.member_ids)})
            except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                warnings.append(_error("custom_empty_readback_failed", exc))
            return self._finish_custom_delete(operation, token, warnings)

        if collection_id:
            remote: RemoteCollectionState | None = None
            try:
                remote = self._remote.read_collection(collection_id)
            except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                warnings.append(_error("custom_collection_read_failed", exc))
            if remote is not None:
                self._suppression.suppress(collection_id, ttl_seconds=20)
                requesting = self._repository.begin_provider_step(
                    operation.id,
                    token,
                    step=CollectionOperationStep.REMOVE_MEMBERS,
                    state={**dict(operation.state), "warnings": warnings},
                )
                try:
                    self._remote.empty_collection(remote)
                except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                    warnings.append(_error("custom_empty_failed", exc))
                try:
                    readback = self._remote.read_collection(collection_id)
                    if readback is not None and readback.member_ids:
                        warnings.append(
                            {"type": "custom_empty_unconfirmed", "remaining_members": len(readback.member_ids)}
                        )
                except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                    warnings.append(_error("custom_empty_readback_failed", exc))
                operation = requesting
        return self._finish_custom_delete(operation, token, warnings)

    def _finish_custom_delete(
        self,
        operation: CollectionOperation,
        token: UUID,
        warnings: list[dict[str, Any]],
    ) -> CollectionOperation:
        continued = self._repository.continue_claim(
            operation.id,
            token,
            next_step=CollectionOperationStep.CLEANUP_PLACEHOLDERS,
            state={**dict(operation.state), "warnings": warnings},
        )
        cleaned = {
            str(value) for value in continued.state.get("cleaned_placeholder_tmdb_ids", []) if str(value or "").strip()
        }
        for tmdb_id in _request_texts(continued, "placeholder_tmdb_ids"):
            if tmdb_id in cleaned:
                continue
            try:
                if not self._assets.cleanup_placeholder(tmdb_id):
                    raise RuntimeError("placeholder cleanup did not confirm completion")
            except Exception as exc:
                state = {
                    **dict(continued.state),
                    "warnings": warnings,
                    "cleaned_placeholder_tmdb_ids": sorted(cleaned),
                }
                if operation.attempts >= COLLECTION_OPERATION_TOTAL_ATTEMPTS:
                    return self._repository.mark_indeterminate(
                        operation.id,
                        token,
                        state=state,
                        error=_error("placeholder_cleanup_exhausted", exc, tmdb_id=tmdb_id),
                    )
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    state=state,
                    error=_error("placeholder_cleanup_failed", exc, tmdb_id=tmdb_id),
                    retry_after_seconds=self._retry_delay_seconds,
                )
            cleaned.add(tmdb_id)
            continued = self._repository.continue_claim(
                operation.id,
                token,
                next_step=CollectionOperationStep.CLEANUP_PLACEHOLDERS,
                state={
                    **dict(continued.state),
                    "warnings": warnings,
                    "cleaned_placeholder_tmdb_ids": sorted(cleaned),
                },
            )
        return self._repository.mark_confirmed(
            operation.id,
            token,
            state={**dict(continued.state), "warnings": warnings},
            result={
                "operation": operation.kind.value,
                "collection_id": int(operation.request.get("collection_id") or 0),
                "emby_collection_id": operation.request.get("emby_collection_id"),
                "local_deleted": True,
                "remote_empty_best_effort": True,
                "cleaned_placeholder_tmdb_ids": sorted(cleaned),
                "warnings": warnings,
            },
        )

    def _execute_missing_cleanup(
        self,
        operation: CollectionOperation,
        token: UUID,
    ) -> CollectionOperation:
        tmdb_id = _request_text(operation, "tmdb_collection_id")
        continued = operation
        if not operation.state.get("child_reservation_complete"):
            continued = self._repository.continue_claim(
                operation.id,
                token,
                next_step=CollectionOperationStep.DISCOVER_MISSING_TARGETS,
                state=operation.state,
            )
            warnings = _warnings(continued)
            matches: Sequence[RemoteCollectionState] = ()
            try:
                matches = self._remote.list_native_collections(tmdb_id)
            except (CollectionRemoteRejected, CollectionRemoteIndeterminate) as exc:
                warnings.append(_error("remote_collection_discovery_failed", exc))
            verified_ids: list[str] = []
            for remote in matches:
                if remote.tmdb_collection_id == tmdb_id:
                    verified_ids.append(remote.collection_id)
                else:
                    warnings.append(
                        {
                            "type": "remote_collection_identity_mismatch",
                            "emby_collection_id": remote.collection_id,
                        }
                    )
            try:
                continued = self._repository.reserve_missing_delete_children(
                    operation.id,
                    token,
                    tmdb_collection_id=tmdb_id,
                    emby_collection_ids=verified_ids,
                    state={
                        **dict(continued.state),
                        "warnings": warnings,
                        "child_reservation_complete": True,
                    },
                )
            except CollectionOperationIndeterminateBlockerError as exc:
                return self._repository.mark_indeterminate(
                    operation.id,
                    token,
                    state={**dict(continued.state), "warnings": warnings},
                    error={
                        "type": "remote_collection_indeterminate_blocker",
                        "blocker_operation_id": str(exc.blocker_id),
                    },
                )
            except CollectionOperationScopeBusyError as exc:
                if operation.attempts >= COLLECTION_OPERATION_TOTAL_ATTEMPTS:
                    return self._repository.mark_indeterminate(
                        operation.id,
                        token,
                        state={**dict(continued.state), "warnings": warnings},
                        error=_error("remote_collection_scope_busy_exhausted", exc),
                    )
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    state={**dict(continued.state), "warnings": warnings},
                    error=_error("remote_collection_scope_busy", exc),
                    retry_after_seconds=self._retry_delay_seconds,
                )
        if not continued.state.get("local_cleanup_applied"):
            continued = self._repository.checkpoint_missing_cleanup(
                operation.id,
                token,
                tmdb_collection_id=tmdb_id,
                state=continued.state,
            )
        warnings = _warnings(continued)
        raw_sources = continued.state.get("source_urls")
        source_urls = (
            [str(value).strip() for value in raw_sources if str(value or "").strip()]
            if isinstance(raw_sources, list)
            else []
        )
        try:
            discarded = self._assets.discard_unreferenced_sources(source_urls)
        except Exception as exc:
            if operation.attempts < COLLECTION_OPERATION_TOTAL_ATTEMPTS:
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    state=continued.state,
                    error=_error("missing_image_cleanup_failed", exc),
                    retry_after_seconds=self._retry_delay_seconds,
                )
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                state={**dict(continued.state), "warnings": warnings},
                error=_error("missing_image_cleanup_exhausted", exc),
            )
        child_ids = [str(value) for value in continued.state.get("child_operation_ids", [])]
        return self._repository.mark_confirmed(
            operation.id,
            token,
            state={**dict(continued.state), "warnings": warnings},
            result={
                "operation": operation.kind.value,
                "tmdb_collection_id": tmdb_id,
                "child_operation_ids": child_ids,
                "source_urls": source_urls,
                "discarded_sources": int(discarded),
                "warnings": warnings,
            },
        )

    def _execute_image_archive(
        self,
        operation: CollectionOperation,
        token: UUID,
    ) -> CollectionOperation:
        tmdb_id = _request_text(operation, "tmdb_collection_id")
        expected_digest = _request_text(operation, "metadata_digest")
        raw_images = operation.request.get("images")
        if not isinstance(raw_images, Mapping):
            raise ValueError("collection image operation images must be an object")
        images = {str(slot): _required_text(source_url, "source_url") for slot, source_url in raw_images.items()}
        receipts = _receipt_state(operation.state.get("receipts"))
        pending_slot = str(operation.state.get("pending_slot") or "").strip()
        pending_source = str(operation.state.get("pending_source") or "").strip()
        if operation.step_started_at is not None and pending_slot and pending_source:
            cached = self._safe_image_readback(pending_source)
            if cached is not None:
                receipts[pending_slot] = _image_receipt(pending_source, cached)
            elif self._image_readback_exhausted(operation, pending_source):
                receipts[pending_slot] = _fallback_receipt(pending_source, "readback_exhausted")
            else:
                return self._repository.defer_readback(
                    operation.id,
                    token,
                    state=self._increment_readback(operation, f"image:{pending_source}"),
                    error={"type": "image_archive_readback_pending", "source_url": pending_source},
                    retry_after_seconds=self._retry_delay_seconds,
                )
            operation = self._repository.continue_claim(
                operation.id,
                token,
                next_step=CollectionOperationStep.ARCHIVE_IMAGES,
                state={"receipts": receipts},
            )

        for slot, source_url in images.items():
            if slot in receipts:
                continue
            cached = self._safe_image_readback(source_url)
            if cached is not None:
                receipts[slot] = _image_receipt(source_url, cached)
                continue
            requesting = self._repository.begin_provider_step(
                operation.id,
                token,
                step=CollectionOperationStep.ARCHIVE_IMAGES,
                state={
                    "receipts": receipts,
                    "pending_slot": slot,
                    "pending_source": source_url,
                },
            )
            try:
                archived = self._assets.archive_image(source_url)
            except Exception as exc:
                archived = self._safe_image_readback(source_url)
                if archived is None:
                    return self._repository.defer_readback(
                        operation.id,
                        token,
                        state=self._increment_readback(requesting, f"image:{source_url}"),
                        error=_error("image_archive_outcome_unknown", exc, source_url=source_url),
                        retry_after_seconds=self._retry_delay_seconds,
                    )
            receipts[slot] = (
                _image_receipt(source_url, archived)
                if archived is not None
                else _fallback_receipt(source_url, "archive_unavailable")
            )
            operation = self._repository.continue_claim(
                operation.id,
                token,
                next_step=CollectionOperationStep.ARCHIVE_IMAGES,
                state={"receipts": receipts},
            )

        current_digest = self._repository.current_image_metadata_digest(tmdb_id)
        stale = current_digest != expected_digest
        warnings: list[dict[str, Any]] = []
        if stale:
            archived_sources = [
                str(receipt.get("source_url") or "") for receipt in receipts.values() if receipt.get("archived")
            ]
            try:
                self._assets.discard_unreferenced_sources(archived_sources)
            except Exception as exc:
                state = {"receipts": receipts, "warnings": warnings, "stale_snapshot": True}
                if operation.attempts >= COLLECTION_OPERATION_TOTAL_ATTEMPTS:
                    return self._repository.mark_indeterminate(
                        operation.id,
                        token,
                        state=state,
                        error=_error("stale_image_cleanup_exhausted", exc),
                    )
                return self._repository.mark_retryable(
                    operation.id,
                    token,
                    state=state,
                    error=_error("stale_image_cleanup_failed", exc),
                    retry_after_seconds=self._retry_delay_seconds,
                )
        return self._repository.mark_confirmed(
            operation.id,
            token,
            state={"receipts": receipts, "warnings": warnings},
            result={
                "operation": operation.kind.value,
                "tmdb_collection_id": tmdb_id,
                "metadata_digest": expected_digest,
                "stale_snapshot": stale,
                "receipts": receipts,
                "warnings": warnings,
            },
        )

    def _execute_survival_check(
        self,
        operation: CollectionOperation,
        token: UUID,
    ) -> CollectionOperation:
        collection_id = _request_text(operation, "emby_collection_id")
        continued = self._repository.continue_claim(
            operation.id,
            token,
            next_step=CollectionOperationStep.SURVIVAL_READBACK,
            state=operation.state,
        )
        remote = self._remote.read_collection(collection_id)
        survived = remote is not None
        return self._repository.complete_survival_check(
            operation.id,
            token,
            emby_collection_id=collection_id,
            survived=survived,
            state={**dict(continued.state), "survived": survived},
            result={
                "operation": operation.kind.value,
                "emby_collection_id": collection_id,
                "survived": survived,
            },
        )

    def _defer_post_checkpoint(
        self,
        operation: CollectionOperation,
        token: UUID,
        error: BaseException,
    ) -> CollectionOperation:
        key = f"step:{operation.current_step.value}"
        state = self._increment_readback(operation, key)
        state["last_readback_error"] = _error("authoritative_readback_pending", error)
        if int(state["readback_attempts"][key]) >= self._readback_attempts:
            return self._repository.mark_indeterminate(
                operation.id,
                token,
                state=state,
                error={
                    "type": "authoritative_readback_exhausted",
                    "attempts": int(state["readback_attempts"][key]),
                    "cause": type(error).__name__,
                },
            )
        return self._repository.defer_readback(
            operation.id,
            token,
            state=state,
            error=_error("authoritative_readback_pending", error),
            retry_after_seconds=self._retry_delay_seconds,
        )

    def _safe_image_readback(self, source_url: str) -> Mapping[str, Any] | None:
        try:
            return self._assets.read_archived_image(source_url)
        except Exception:
            return None

    @staticmethod
    def _increment_readback(operation: CollectionOperation, key: str) -> dict[str, Any]:
        state = dict(operation.state)
        raw_counts = state.get("readback_attempts")
        counts = (
            {
                str(name): int(value)
                for name, value in raw_counts.items()
                if str(name).strip() and isinstance(value, int)
            }
            if isinstance(raw_counts, Mapping)
            else {}
        )
        counts[key] = counts.get(key, 0) + 1
        state["readback_attempts"] = counts
        return state

    def _image_readback_exhausted(self, operation: CollectionOperation, source_url: str) -> bool:
        key = f"image:{source_url}"
        state = self._increment_readback(operation, key)
        return int(state["readback_attempts"][key]) >= self._readback_attempts


def _matches_expected_tmdb(remote: RemoteCollectionState, expected_tmdb_id: str | None) -> bool:
    return expected_tmdb_id is None or remote.tmdb_collection_id == expected_tmdb_id


def _request_text(operation: CollectionOperation, field: str) -> str:
    return _required_text(operation.request.get(field), field)


def _request_texts(operation: CollectionOperation, field: str) -> tuple[str, ...]:
    value = operation.request.get(field)
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(str(item).strip() for item in value if str(item or "").strip()))


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"collection operation {field} must not be empty")
    return normalized


def _state(operation: CollectionOperation, **updates: Any) -> dict[str, Any]:
    return {**dict(operation.state), **updates}


def _warnings(operation: CollectionOperation) -> list[dict[str, Any]]:
    raw = operation.state.get("warnings")
    if not isinstance(raw, list):
        return []
    return [dict(value) for value in raw if isinstance(value, Mapping)]


def _error(kind: str, error: BaseException, **details: Any) -> dict[str, Any]:
    return {
        "type": kind,
        "error_type": type(error).__name__,
        "message": str(error)[:500],
        **details,
    }


def _receipt_state(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    return {str(slot): dict(receipt) for slot, receipt in value.items() if isinstance(receipt, Mapping)}


def _image_receipt(source_url: str, value: Mapping[str, Any]) -> dict[str, Any]:
    content_hash = str(value.get("content_hash") or value.get("content_sha256") or "").strip()
    relative_path = str(value.get("relative_path") or "").strip()
    mime_type = str(value.get("mime_type") or "").strip()
    byte_size = int(value.get("byte_size") or 0)
    if len(content_hash) != 64 or not relative_path or not mime_type or byte_size <= 0:
        return _fallback_receipt(source_url, "invalid_archive_receipt")
    return {
        "source_url": source_url,
        "content_sha256": content_hash,
        "byte_size": byte_size,
        "mime_type": mime_type,
        "relative_path": relative_path,
        "archived": True,
        "fallback_source_url": None,
    }


def _fallback_receipt(source_url: str, reason: str) -> dict[str, Any]:
    return {
        "source_url": source_url,
        "content_sha256": None,
        "byte_size": None,
        "mime_type": None,
        "relative_path": None,
        "archived": False,
        "fallback_source_url": source_url,
        "reason": reason,
    }


__all__ = [
    "COLLECTION_OPERATION_READBACK_ATTEMPTS",
    "COLLECTION_OPERATION_RETRY_DELAY_SECONDS",
    "COLLECTION_OPERATION_TOTAL_ATTEMPTS",
    "CollectionOperationExecutor",
    "CollectionOperationReconciler",
]
