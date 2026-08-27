from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.exc import IntegrityError, OperationalError

from etk.platform.jobs.json_payload import canonical_json, normalize_durable_json_object
from etk.platform.jobs.types import UnitOfWork, UnitOfWorkFactory

logger = logging.getLogger(__name__)

DATABASE_MAINTENANCE_TASK_KEY = "operations.database-maintenance"
DATABASE_MAINTENANCE_TOPIC = "database.maintenance.requested"
DATABASE_MAINTENANCE_AGGREGATE_TYPE = "database-maintenance-operation"
DATABASE_MAINTENANCE_TOPICS = (DATABASE_MAINTENANCE_TOPIC,)

BACKUP_FORMAT_VERSION = 1
MAX_COMPRESSED_SIZE = 64 * 1024 * 1024
MAX_EXPANDED_SIZE = 256 * 1024 * 1024
MAX_COMPRESSION_RATIO = Decimal("20")
ARTIFACT_TTL = timedelta(hours=24)
MAX_OPERATION_ATTEMPTS = 12

SHARE_ALLOWLIST = frozenset(
    {
        "person_metadata",
        "translation_cache",
        "media_metadata",
        "p115_mediainfo_cache",
    }
)
OVERWRITE_DENYLIST = frozenset({"emby_users", "cleanup_index", "invitations"})

# These tables are control-plane state, not user-maintained data.  The explicit
# list is intentionally conservative; the prefix/suffix checks below protect
# future operation/lease/artifact tables without requiring a migration to
# remember to update this module first.
PROTECTED_TABLES = frozenset(
    {
        "job_runs",
        "job_attempts",
        "outbox_events",
        "idempotency_records",
        "media_effects",
        "collection_operations",
        "identity_operations",
        "subscription_operations",
        "subscription_quota_reservations",
        "p115_upload_operations",
        "p115_playback_operations",
        "p115_intro_detection_operations",
        "p115_intro_detection_batches",
        "p115_intro_detection_leases",
        "database_maintenance_operations",
        "database_backup_artifacts",
        "identity_avatar_artifacts",
        "identity_user_tombstones",
        "identity_projection_clock",
        "portal_subscription_requests",
        "ingestion_monitor_items",
        "ingestion_monitor_bindings",
        "p115_upload_records",
        "p115_organize_records",
        "subscribe_assistant_state",
        "subscribe_assistant_snapshots",
        "subscribe_assistant_delete_records",
    }
)
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")


class DatabaseMaintenanceOperationKind(StrEnum):
    IMPORT = "import"
    CLEAR_TABLES = "clear_tables"
    PREPARE_FOR_LIBRARY_REBUILD = "prepare_for_library_rebuild"
    CLEANUP_OFFLINE_MEDIA = "cleanup_offline_media"
    CLEAR_VECTORS = "clear_vectors"
    CORRECT_SEQUENCES = "correct_sequences"


class DatabaseMaintenanceOperationStatus(StrEnum):
    READY = "ready"
    CLAIMED = "claimed"
    REQUESTING = "requesting"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {
            DatabaseMaintenanceOperationStatus.CONFIRMED,
            DatabaseMaintenanceOperationStatus.REJECTED,
            DatabaseMaintenanceOperationStatus.INDETERMINATE,
        }


class DatabaseMaintenanceOperationStep(StrEnum):
    RESERVED = "reserved"
    VALIDATE_ARTIFACT = "validate_artifact"
    MUTATE = "mutate"
    RECONCILE = "reconcile"
    CLEANUP_ARTIFACT = "cleanup_artifact"


@dataclass(frozen=True, slots=True)
class BackupArtifact:
    id: UUID
    format_version: int
    schema_fingerprint: str
    source_filename: str
    compressed_bytes: bytes | None
    compressed_size: int
    expanded_size: int
    compression_ratio: Decimal
    compressed_sha256: bytes
    expanded_sha256: bytes
    status: str
    expires_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceOperation:
    id: UUID
    sequence: int
    operation_key: str
    request_hash: bytes
    kind: DatabaseMaintenanceOperationKind
    scope_key: str
    request: Mapping[str, Any]
    artifact_id: UUID | None
    job_id: UUID | None
    status: DatabaseMaintenanceOperationStatus
    current_step: DatabaseMaintenanceOperationStep
    state_revision: int
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    step_started_at: datetime | None
    state: Mapping[str, Any]
    result: Mapping[str, Any] | None
    error: Mapping[str, Any] | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @property
    def terminal(self) -> bool:
        return self.status.terminal


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceReservation:
    operation: DatabaseMaintenanceOperation
    created: bool


class DatabaseMaintenanceError(RuntimeError):
    pass


class DatabaseMaintenanceConflictError(DatabaseMaintenanceError):
    pass


class DatabaseMaintenanceBusyError(DatabaseMaintenanceConflictError):
    pass


class DatabaseMaintenanceValidationError(DatabaseMaintenanceError, ValueError):
    pass


class DatabaseMaintenanceArtifactExpired(DatabaseMaintenanceValidationError):
    pass


class DatabaseMaintenanceSizeLimitError(DatabaseMaintenanceValidationError):
    pass


class DatabaseMaintenanceStateError(DatabaseMaintenanceError):
    pass


@dataclass(frozen=True, slots=True)
class ExportedDatabase:
    content: bytes
    filename: str
    expanded_size: int
    compressed_size: int
    compression_ratio: Decimal


@dataclass(frozen=True, slots=True)
class DatabaseMaintenanceHttpResult:
    payload: Mapping[str, Any]
    status_code: int


def operation_key(*, kind: DatabaseMaintenanceOperationKind | str, principal_id: str, idempotency_key: str) -> str:
    normalized_kind = _kind(kind).value
    principal = _required_text(principal_id, "principal_id")
    key = _required_text(idempotency_key, "idempotency_key")
    return hashlib.sha256(f"{normalized_kind}\0{principal}\0{key}".encode()).hexdigest()


def _kind(value: DatabaseMaintenanceOperationKind | str) -> DatabaseMaintenanceOperationKind:
    try:
        return value if isinstance(value, DatabaseMaintenanceOperationKind) else DatabaseMaintenanceOperationKind(value)
    except ValueError as exc:
        raise DatabaseMaintenanceValidationError(f"unsupported database maintenance operation kind: {value!r}") from exc


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise DatabaseMaintenanceValidationError(f"{field} must not be empty")
    return normalized


def _utc(value: datetime | None = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _identifier(value: object, *, field: str = "table") -> str:
    normalized = str(value or "").strip().lower()
    if not _IDENTIFIER.fullmatch(normalized):
        raise DatabaseMaintenanceValidationError(f"{field} is not a valid table identifier")
    return normalized


def _quote_identifier(value: str) -> str:
    normalized = _identifier(value)
    return '"' + normalized.replace('"', '""') + '"'


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return {"__etk_bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _decode_json_value(value: Any) -> Any:
    if isinstance(value, list):
        return [_decode_json_value(item) for item in value]
    if isinstance(value, dict):
        if set(value) == {"__etk_bytes__"} and isinstance(value["__etk_bytes__"], str):
            try:
                return base64.b64decode(value["__etk_bytes__"], validate=True)
            except (ValueError, TypeError) as exc:
                raise DatabaseMaintenanceValidationError("backup contains invalid encoded bytes") from exc
        return {str(key): _decode_json_value(item) for key, item in value.items()}
    return value


def schema_fingerprint(session: Any) -> str:
    """Hash the normalized public table/column/type/constraint summary."""

    columns = [
        dict(row)
        for row in session.execute(
            text(
                """
                SELECT table_name, column_name, ordinal_position, data_type, udt_name,
                       is_nullable, COALESCE(column_default, '') AS column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
                """
            )
        ).mappings()
    ]
    constraints = [
        dict(row)
        for row in session.execute(
            text(
                """
                SELECT
                    tc.table_name,
                    tc.constraint_name,
                    tc.constraint_type,
                    COALESCE(kcu.column_name, '') AS column_name,
                    COALESCE(kcu.ordinal_position, 0) AS ordinal_position,
                    COALESCE(ccu.table_name, '') AS foreign_table_name,
                    COALESCE(ccu.column_name, '') AS foreign_column_name
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu
                  ON kcu.constraint_schema = tc.constraint_schema
                 AND kcu.constraint_name = tc.constraint_name
                 AND kcu.table_name = tc.table_name
                LEFT JOIN information_schema.constraint_column_usage ccu
                  ON ccu.constraint_schema = tc.constraint_schema
                 AND ccu.constraint_name = tc.constraint_name
                WHERE tc.table_schema = 'public'
                ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position NULLS FIRST
                """
            )
        ).mappings()
    ]
    payload = {"columns": columns, "constraints": constraints}
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _bounded_gunzip(compressed: bytes) -> bytes:
    if not compressed.startswith(b"\x1f\x8b"):
        return compressed
    output = bytearray()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(compressed), mode="rb") as stream:
            while True:
                chunk = stream.read(min(1024 * 1024, MAX_EXPANDED_SIZE + 1 - len(output)))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > MAX_EXPANDED_SIZE:
                    raise DatabaseMaintenanceSizeLimitError("expanded backup exceeds 256 MiB")
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        raise DatabaseMaintenanceValidationError("backup is not a valid gzip stream") from exc
    return bytes(output)


def inspect_backup_bytes(
    content: bytes, *, filename: str = "backup.json.gz"
) -> tuple[dict[str, Any], bytes, int, int, Decimal, bytes, bytes]:
    if not isinstance(content, bytes) or not content:
        raise DatabaseMaintenanceValidationError("backup content must not be empty")
    expanded = _bounded_gunzip(content)
    if len(expanded) > MAX_EXPANDED_SIZE:
        raise DatabaseMaintenanceSizeLimitError("expanded backup exceeds 256 MiB")
    try:
        decoded = json.loads(expanded.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DatabaseMaintenanceValidationError("backup JSON is invalid UTF-8 or malformed") from exc
    if not isinstance(decoded, dict):
        raise DatabaseMaintenanceValidationError("backup root must be a JSON object")
    format_version = decoded.get("format_version")
    if type(format_version) is not int or format_version < 1:
        raise DatabaseMaintenanceValidationError("backup format_version is required")
    if format_version != BACKUP_FORMAT_VERSION:
        raise DatabaseMaintenanceValidationError("backup format_version is unsupported")
    fingerprint = str(decoded.get("schema_fingerprint") or "").strip().lower()
    if not _SHA256_HEX.fullmatch(fingerprint):
        raise DatabaseMaintenanceValidationError("backup schema_fingerprint is required")
    data = decoded.get("data")
    if not isinstance(data, dict):
        raise DatabaseMaintenanceValidationError("backup data must be a JSON object")
    for table_name, rows in data.items():
        _identifier(table_name, field="backup table")
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise DatabaseMaintenanceValidationError(f"backup table {table_name!r} must contain object rows")
    compressed = bytes(content)
    compressed_size = len(compressed)
    if compressed_size > MAX_COMPRESSED_SIZE:
        raise DatabaseMaintenanceSizeLimitError("compressed backup exceeds 64 MiB")
    if compressed_size <= 0:
        raise DatabaseMaintenanceValidationError("compressed backup is empty")
    ratio = Decimal(len(expanded)) / Decimal(compressed_size)
    if ratio > MAX_COMPRESSION_RATIO:
        raise DatabaseMaintenanceSizeLimitError("backup compression ratio exceeds 20x")
    # Plain JSON uploads are normalized to the immutable bounded gzip form.
    stored = compressed if compressed.startswith(b"\x1f\x8b") else gzip.compress(expanded, mtime=0)
    if len(stored) > MAX_COMPRESSED_SIZE:
        raise DatabaseMaintenanceSizeLimitError("compressed backup exceeds 64 MiB")
    stored_ratio = Decimal(len(expanded)) / Decimal(len(stored))
    if stored_ratio > MAX_COMPRESSION_RATIO:
        raise DatabaseMaintenanceSizeLimitError("backup compression ratio exceeds 20x")
    return (
        decoded,
        stored,
        len(expanded),
        len(stored),
        stored_ratio,
        hashlib.sha256(stored).digest(),
        hashlib.sha256(expanded).digest(),
    )


def _artifact(row: RowMapping) -> BackupArtifact:
    return BackupArtifact(
        id=UUID(str(row["id"])),
        format_version=int(row["format_version"]),
        schema_fingerprint=str(row["schema_fingerprint"]),
        source_filename=str(row.get("source_filename") or ""),
        compressed_bytes=bytes(row["compressed_bytes"]) if row.get("compressed_bytes") is not None else None,
        compressed_size=int(row["compressed_size"]),
        expanded_size=int(row["expanded_size"]),
        compression_ratio=Decimal(str(row["compression_ratio"])),
        compressed_sha256=bytes(row["compressed_sha256"]),
        expanded_sha256=bytes(row["expanded_sha256"]),
        status=str(row["status"]),
        expires_at=row["expires_at"],
        created_at=row["created_at"],
    )


def _operation(row: RowMapping) -> DatabaseMaintenanceOperation:
    return DatabaseMaintenanceOperation(
        id=UUID(str(row["id"])),
        sequence=int(row["event_sequence"]),
        operation_key=str(row["operation_key"]),
        request_hash=bytes(row["request_hash"]),
        kind=DatabaseMaintenanceOperationKind(str(row["kind"])),
        scope_key=str(row["scope_key"]),
        request=dict(row["request_json"] or {}),
        artifact_id=UUID(str(row["artifact_id"])) if row.get("artifact_id") is not None else None,
        job_id=UUID(str(row["job_id"])) if row.get("job_id") is not None else None,
        status=DatabaseMaintenanceOperationStatus(str(row["status"])),
        current_step=DatabaseMaintenanceOperationStep(str(row["current_step"])),
        state_revision=int(row["state_revision"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row.get("claimed_by"),
        claim_token=UUID(str(row["claim_token"])) if row.get("claim_token") is not None else None,
        claimed_at=row.get("claimed_at"),
        lease_expires_at=row.get("lease_expires_at"),
        step_started_at=row.get("step_started_at"),
        state=dict(row.get("state_json") or {}),
        result=dict(row["result_json"]) if row.get("result_json") is not None else None,
        error=dict(row["error_json"]) if row.get("error_json") is not None else None,
        finished_at=row.get("finished_at"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class PostgresDatabaseMaintenanceRepository:
    def __init__(self, unit_of_work: UnitOfWorkFactory) -> None:
        self._unit_of_work = unit_of_work

    def create_artifact(
        self,
        content: bytes,
        *,
        source_filename: str = "backup.json.gz",
        now: datetime | None = None,
    ) -> BackupArtifact:
        decoded, stored, expanded_size, compressed_size, ratio, compressed_hash, expanded_hash = inspect_backup_bytes(
            content,
            filename=source_filename,
        )
        fingerprint = str(decoded["schema_fingerprint"]).strip().lower()
        format_version = int(decoded["format_version"])
        timestamp = _utc(now)
        artifact_id = uuid4()
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO database_backup_artifacts (
                            id, format_version, schema_fingerprint, source_filename,
                            compressed_bytes, status, compressed_size, expanded_size,
                            compression_ratio, compressed_sha256, expanded_sha256, expires_at
                        ) VALUES (
                            :id, :format_version, :schema_fingerprint, :source_filename,
                            :compressed_bytes, 'available', :compressed_size, :expanded_size,
                            :compression_ratio, :compressed_sha256, :expanded_sha256, :expires_at
                        ) ON CONFLICT (compressed_sha256) WHERE status = 'available' DO NOTHING RETURNING *
                        """
                    ),
                    {
                        "id": artifact_id,
                        "format_version": format_version,
                        "schema_fingerprint": fingerprint,
                        "source_filename": str(source_filename or "")[:255],
                        "compressed_bytes": stored,
                        "compressed_size": compressed_size,
                        "expanded_size": expanded_size,
                        "compression_ratio": ratio,
                        "compressed_sha256": compressed_hash,
                        "expanded_sha256": expanded_hash,
                        "expires_at": timestamp + ARTIFACT_TTL,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                row = (
                    uow.session.execute(
                        text(
                            "SELECT * FROM database_backup_artifacts "
                            "WHERE compressed_sha256 = :compressed_sha256 AND status = 'available' "
                            "ORDER BY created_at DESC LIMIT 1 FOR UPDATE"
                        ),
                        {"compressed_sha256": compressed_hash},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise DatabaseMaintenanceStateError("backup artifact insert conflict was not visible")
            uow.commit()
            return _artifact(row)

    def read_artifact(
        self,
        artifact_id: UUID,
        *,
        now: datetime | None = None,
        lock: bool = False,
    ) -> BackupArtifact | None:
        suffix = " FOR UPDATE" if lock else ""
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(f"SELECT * FROM database_backup_artifacts WHERE id = :id{suffix}"),
                    {"id": artifact_id},
                )
                .mappings()
                .one_or_none()
            )
            return _artifact(row) if row is not None else None

    def purge_expired_artifacts(self, *, limit: int = 100, now: datetime | None = None) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    WITH expired AS (
                        SELECT id FROM database_backup_artifacts
                        WHERE status = 'available' AND expires_at <= :now
                        ORDER BY expires_at, id
                        FOR UPDATE SKIP LOCKED
                        LIMIT :limit
                    )
                    UPDATE database_backup_artifacts artifact
                    SET status = 'expired', compressed_bytes = NULL
                    FROM expired
                    WHERE artifact.id = expired.id
                    """
                ),
                {"now": timestamp, "limit": max(1, int(limit))},
            )
            uow.commit()
            return int(getattr(result, "rowcount", 0) or 0)

    def reserve(
        self,
        *,
        kind: DatabaseMaintenanceOperationKind | str,
        principal_id: str,
        request: Mapping[str, Any],
        artifact_id: UUID | None = None,
        idempotency_key: str | None = None,
        now: datetime | None = None,
    ) -> DatabaseMaintenanceReservation:
        normalized_kind = _kind(kind)
        normalized_request = normalize_durable_json_object(dict(request), field="database maintenance request")
        if artifact_id is not None:
            normalized_request = {**normalized_request, "artifact_id": str(artifact_id)}
        request_hash = hashlib.sha256(canonical_json(normalized_request).encode()).digest()
        occurrence = str(idempotency_key or uuid4()).strip()
        key = operation_key(kind=normalized_kind, principal_id=principal_id, idempotency_key=occurrence)
        operation_id = uuid5(NAMESPACE_URL, f"etk:database-maintenance:{key}")
        timestamp = _utc(now)
        scope = "database-maintenance"
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
                {"scope": scope},
            )
            active = uow.session.execute(
                text(
                    """
                        SELECT operation_key FROM database_maintenance_operations
                        WHERE scope_key = :scope
                          AND status IN ('ready','claimed','requesting','retryable')
                        LIMIT 1
                        """
                ),
                {"scope": scope},
            ).scalar_one_or_none()
            if active is not None and str(active) != key:
                raise DatabaseMaintenanceBusyError("another database maintenance operation is active")
            row = (
                uow.session.execute(
                    text(
                        """
                        INSERT INTO database_maintenance_operations (
                            id, operation_key, request_hash, kind, scope_key,
                            request_json, artifact_id, status, current_step, available_at
                        ) VALUES (
                            :id, :operation_key, :request_hash, :kind, :scope,
                            CAST(:request_json AS jsonb), :artifact_id, 'ready', 'reserved', :available_at
                        ) ON CONFLICT (operation_key) DO NOTHING RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "operation_key": key,
                        "request_hash": request_hash,
                        "kind": normalized_kind.value,
                        "scope": scope,
                        "request_json": canonical_json(normalized_request),
                        "artifact_id": artifact_id,
                        "available_at": timestamp,
                    },
                )
                .mappings()
                .one_or_none()
            )
            created = row is not None
            if row is None:
                row = (
                    uow.session.execute(
                        text("SELECT * FROM database_maintenance_operations WHERE operation_key = :key FOR UPDATE"),
                        {"key": key},
                    )
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise DatabaseMaintenanceStateError("operation conflict was not visible")
                if bytes(row["request_hash"]) != request_hash or str(row["kind"]) != normalized_kind.value:
                    raise DatabaseMaintenanceConflictError("idempotency key was already used for another request")
            operation = _operation(row)
            if created:
                self._insert_outbox(uow, operation)
            uow.commit()
            return DatabaseMaintenanceReservation(operation, created)

    def read(self, operation_id: UUID) -> DatabaseMaintenanceOperation | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM database_maintenance_operations WHERE id = :id"),
                    {"id": operation_id},
                )
                .mappings()
                .one_or_none()
            )
            return _operation(row) if row is not None else None

    def claim_by_id(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> DatabaseMaintenanceOperation | None:
        if float(lease_seconds) <= 0:
            raise ValueError("maintenance operation lease must be positive")
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE database_maintenance_operations
                        SET status = 'claimed', attempts = attempts + 1,
                            claimed_by = :worker_id, claim_token = :claim_token,
                            claimed_at = :now, lease_expires_at = :lease_expires_at,
                            state_revision = state_revision + 1, updated_at = :now
                        WHERE id = :id AND status IN ('ready','retryable') AND available_at <= :now
                        RETURNING *
                        """
                    ),
                    {
                        "id": operation_id,
                        "worker_id": _required_text(worker_id, "worker_id"),
                        "claim_token": uuid4(),
                        "now": timestamp,
                        "lease_expires_at": timestamp + timedelta(seconds=float(lease_seconds)),
                    },
                )
                .mappings()
                .one_or_none()
            )
            uow.commit()
            return _operation(row) if row is not None else None

    def mark_confirmed(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        result: Mapping[str, Any],
        now: datetime | None = None,
    ) -> DatabaseMaintenanceOperation:
        return self._mark_terminal(operation_id, claim_token, "confirmed", state, result, None, now=now)

    def mark_rejected(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> DatabaseMaintenanceOperation:
        return self._mark_terminal(operation_id, claim_token, "rejected", state, None, error, now=now)

    def mark_indeterminate(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        now: datetime | None = None,
    ) -> DatabaseMaintenanceOperation:
        return self._mark_terminal(operation_id, claim_token, "indeterminate", state, None, error, now=now)

    def mark_retryable(
        self,
        operation_id: UUID,
        claim_token: UUID,
        *,
        state: Mapping[str, Any],
        error: Mapping[str, Any],
        retry_after_seconds: float = 1.0,
        now: datetime | None = None,
    ) -> DatabaseMaintenanceOperation:
        timestamp = _utc(now)
        normalized_state = normalize_durable_json_object(dict(state), field="database maintenance state")
        normalized_error = normalize_durable_json_object(dict(error), field="database maintenance error")
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            row = self._claimed_update(
                uow,
                operation_id,
                claim_token,
                "status = 'retryable', current_step = 'reserved', step_started_at = NULL, "
                "state_json = CAST(:state_json AS jsonb), error_json = CAST(:error_json AS jsonb), "
                "available_at = :available_at, claimed_by = NULL, claim_token = NULL, "
                "claimed_at = NULL, lease_expires_at = NULL",
                {
                    "state_json": canonical_json(normalized_state),
                    "error_json": canonical_json(normalized_error),
                    "available_at": timestamp + timedelta(seconds=max(0.0, float(retry_after_seconds))),
                },
            )
            self._requeue_outbox(uow, operation_id, timestamp + timedelta(seconds=max(0.0, float(retry_after_seconds))))
            uow.commit()
            return _operation(row)

    def expire_stale_claims(
        self, *, limit: int = 100, retry_delay_seconds: float = 1.0, now: datetime | None = None
    ) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            ids = [
                UUID(str(value))
                for value in uow.session.scalars(
                    text(
                        """
                        SELECT id FROM database_maintenance_operations
                        WHERE status IN ('claimed','requesting') AND lease_expires_at <= :now
                        ORDER BY event_sequence LIMIT :limit FOR UPDATE SKIP LOCKED
                        """
                    ),
                    {"now": timestamp, "limit": max(1, int(limit))},
                )
            ]
            for operation_id in ids:
                row = self._read_in_uow(uow, operation_id, lock=True)
                if row is None:
                    continue
                token = row["claim_token"]
                if token is None:
                    continue
                if int(row["attempts"]) >= MAX_OPERATION_ATTEMPTS:
                    self._terminal_update_in_uow(
                        uow,
                        operation_id,
                        UUID(str(token)),
                        status="indeterminate",
                        state=dict(row["state_json"] or {}),
                        result=None,
                        error={"type": "claim_expired", "attempts": int(row["attempts"])},
                        now=timestamp,
                    )
                    self._complete_outbox(uow, operation_id)
                else:
                    self._claimed_update(
                        uow,
                        operation_id,
                        UUID(str(token)),
                        "status = 'retryable', current_step = 'reserved', step_started_at = NULL, "
                        "available_at = :available_at, claimed_by = NULL, claim_token = NULL, "
                        "claimed_at = NULL, lease_expires_at = NULL, "
                        "error_json = CAST(:error_json AS jsonb)",
                        {
                            "available_at": timestamp + timedelta(seconds=max(0.0, float(retry_delay_seconds))),
                            "error_json": canonical_json({"type": "claim_expired"}),
                        },
                    )
                    self._requeue_outbox(
                        uow, operation_id, timestamp + timedelta(seconds=max(0.0, float(retry_delay_seconds)))
                    )
            uow.commit()
            return len(ids)

    def reject_dead_dispatches(self, *, limit: int = 100, now: datetime | None = None) -> int:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation.id, operation.claim_token, operation.state_json, outbox.aggregate_id
                        FROM database_maintenance_operations operation
                        JOIN outbox_events outbox
                          ON outbox.event_key = :event_key_prefix || operation.id::text
                        WHERE outbox.topic = :topic AND outbox.status = 'dead'
                          AND operation.status IN ('ready','retryable')
                        ORDER BY operation.event_sequence LIMIT :limit
                        FOR UPDATE OF operation SKIP LOCKED
                        """
                    ),
                    {
                        "event_key_prefix": f"{DATABASE_MAINTENANCE_TOPIC}:",
                        "topic": DATABASE_MAINTENANCE_TOPIC,
                        "limit": max(1, int(limit)),
                    },
                )
                .mappings()
                .all()
            )
            for row in rows:
                operation_id = UUID(str(row["id"]))
                # Dead dispatch means no mutation was attempted; reject it
                # without claiming an operation in order to preserve the
                # terminal/claim invariants.
                uow.session.execute(
                    text(
                        """
                        UPDATE database_maintenance_operations
                        SET status = 'rejected', current_step = 'reconcile',
                            state_revision = state_revision + 1,
                            state_json = CAST(:state_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb), finished_at = :now,
                            updated_at = :now
                        WHERE id = :id AND status IN ('ready','retryable')
                        """
                    ),
                    {
                        "id": operation_id,
                        "state_json": canonical_json(dict(row["state_json"] or {})),
                        "error_json": canonical_json({"type": "dispatch_dead"}),
                        "now": timestamp,
                    },
                )
            uow.commit()
            return len(rows)

    def _mark_terminal(
        self,
        operation_id: UUID,
        claim_token: UUID,
        status: str,
        state: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        *,
        now: datetime | None,
    ) -> DatabaseMaintenanceOperation:
        timestamp = _utc(now)
        with self._unit_of_work() as uow:
            self._require_live_claim(uow, operation_id, claim_token, timestamp)
            row = self._terminal_update_in_uow(
                uow,
                operation_id,
                claim_token,
                status=status,
                state=state,
                result=result,
                error=error,
                now=timestamp,
            )
            self._complete_outbox(uow, operation_id)
            uow.commit()
            return _operation(row)

    def _read_in_uow(self, uow: UnitOfWork, operation_id: UUID, *, lock: bool = False) -> RowMapping | None:
        suffix = " FOR UPDATE" if lock else ""
        return (
            uow.session.execute(
                text(f"SELECT * FROM database_maintenance_operations WHERE id = :id{suffix}"),
                {"id": operation_id},
            )
            .mappings()
            .one_or_none()
        )

    def _require_live_claim(self, uow: UnitOfWork, operation_id: UUID, claim_token: UUID, now: datetime) -> RowMapping:
        row = self._read_in_uow(uow, operation_id, lock=True)
        if row is None:
            raise DatabaseMaintenanceStateError("database maintenance operation was not found")
        if str(row["status"]) not in {"claimed", "requesting"} or UUID(str(row["claim_token"])) != claim_token:
            raise DatabaseMaintenanceStateError("database maintenance operation lost its claim")
        if row["lease_expires_at"] is None or row["lease_expires_at"] <= now:
            raise DatabaseMaintenanceStateError("database maintenance operation claim lease expired")
        return row

    @staticmethod
    def _claimed_update(
        uow: UnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        assignments: str,
        values: Mapping[str, Any],
    ) -> RowMapping:
        row = (
            uow.session.execute(
                text(
                    f"""
                    UPDATE database_maintenance_operations
                    SET {assignments}, state_revision = state_revision + 1, updated_at = NOW()
                    WHERE id = :id AND status IN ('claimed','requesting') AND claim_token = :claim_token
                    RETURNING *
                    """
                ),
                {"id": operation_id, "claim_token": claim_token, **dict(values)},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DatabaseMaintenanceStateError("database maintenance transition lost its claim")
        return row

    def _terminal_update_in_uow(
        self,
        uow: UnitOfWork,
        operation_id: UUID,
        claim_token: UUID,
        *,
        status: str,
        state: Mapping[str, Any],
        result: Mapping[str, Any] | None,
        error: Mapping[str, Any] | None,
        now: datetime,
    ) -> RowMapping:
        normalized_state = normalize_durable_json_object(dict(state), field="database maintenance state")
        normalized_result = (
            normalize_durable_json_object(dict(result), field="database maintenance result")
            if result is not None
            else None
        )
        normalized_error = (
            normalize_durable_json_object(dict(error), field="database maintenance error")
            if error is not None
            else None
        )
        return self._claimed_update(
            uow,
            operation_id,
            claim_token,
            "status = :status, current_step = 'reconcile', step_started_at = NULL, "
            "state_json = CAST(:state_json AS jsonb), result_json = CAST(:result_json AS jsonb), "
            "error_json = CAST(:error_json AS jsonb), finished_at = :now, "
            "claimed_by = NULL, claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL",
            {
                "status": status,
                "state_json": canonical_json(normalized_state),
                "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                "now": now,
            },
        )

    @staticmethod
    def _insert_outbox(uow: UnitOfWork, operation: DatabaseMaintenanceOperation) -> None:
        payload = {
            "operation_id": str(operation.id),
            "artifact_id": str(operation.artifact_id) if operation.artifact_id else None,
            "sequence": operation.sequence,
            "operation_key": operation.operation_key,
            "request_hash": operation.request_hash.hex(),
        }
        uow.session.execute(
            text(
                """
                INSERT INTO outbox_events (
                    id, event_key, topic, aggregate_type, aggregate_id,
                    payload_version, payload_json, status, available_at
                ) VALUES (
                    :id, :event_key, :topic, :aggregate_type, :aggregate_id,
                    1, CAST(:payload_json AS jsonb), 'pending', :available_at
                )
                """
            ),
            {
                "id": uuid4(),
                "event_key": f"{DATABASE_MAINTENANCE_TOPIC}:{operation.id}",
                "topic": DATABASE_MAINTENANCE_TOPIC,
                "aggregate_type": DATABASE_MAINTENANCE_AGGREGATE_TYPE,
                "aggregate_id": str(operation.id),
                "payload_json": canonical_json(payload),
                "available_at": operation.available_at,
            },
        )

    @staticmethod
    def _complete_outbox(uow: UnitOfWork, operation_id: UUID) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL, updated_at = NOW()
                WHERE event_key = :event_key AND status <> 'dead'
                """
            ),
            {"event_key": f"{DATABASE_MAINTENANCE_TOPIC}:{operation_id}"},
        )

    @staticmethod
    def _requeue_outbox(uow: UnitOfWork, operation_id: UUID, available_at: datetime) -> None:
        uow.session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'pending', available_at = :available_at,
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    published_at = NULL, processed_at = NULL, updated_at = NOW()
                WHERE event_key = :event_key AND status <> 'dead'
                """
            ),
            {"event_key": f"{DATABASE_MAINTENANCE_TOPIC}:{operation_id}", "available_at": available_at},
        )


class DatabaseMaintenanceExecutor:
    def __init__(
        self,
        repository: PostgresDatabaseMaintenanceRepository,
        *,
        unit_of_work: UnitOfWorkFactory,
        worker_id: str,
        lease_seconds: float = 300,
    ) -> None:
        self._repository = repository
        self._unit_of_work = unit_of_work
        self._worker_id = _required_text(worker_id, "worker_id")
        self._lease_seconds = max(1.0, float(lease_seconds))

    def execute(self, operation_id: UUID, *, artifact_id: UUID | None = None) -> DatabaseMaintenanceOperation | None:
        current = self._repository.read(operation_id)
        if current is None:
            return None
        if artifact_id is not None and current.artifact_id != artifact_id:
            raise DatabaseMaintenanceStateError("maintenance task artifact_id does not match operation")
        claimed = self._repository.claim_by_id(
            operation_id,
            worker_id=self._worker_id,
            lease_seconds=self._lease_seconds,
        )
        if claimed is None:
            return self._repository.read(operation_id)
        token = claimed.claim_token
        if token is None:
            raise DatabaseMaintenanceStateError("claimed maintenance operation is missing claim token")
        try:
            return self._execute_claimed(claimed, token)
        except DatabaseMaintenanceValidationError as exc:
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error={"type": type(exc).__name__, "message": str(exc)},
            )
        except DatabaseMaintenanceConflictError as exc:
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error={"type": "protected_fk_conflict", "message": str(exc)[:240]},
            )
        except IntegrityError:
            # The UoW that attempted the mutation is rolled back before this
            # branch marks the operation rejected in a fresh transaction.
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error={
                    "type": "protected_fk_conflict",
                    "message": "database mutation violated a protected foreign key",
                },
            )
        except OperationalError as exc:
            if claimed.attempts >= MAX_OPERATION_ATTEMPTS:
                return self._repository.mark_indeterminate(
                    claimed.id,
                    token,
                    state=claimed.state,
                    error={"type": "database_operation_exhausted", "message": str(exc)[:240]},
                )
            return self._repository.mark_retryable(
                claimed.id,
                token,
                state=claimed.state,
                error={"type": "database_operation_retryable", "message": str(exc)[:240]},
            )
        except Exception as exc:
            return self._repository.mark_rejected(
                claimed.id,
                token,
                state=claimed.state,
                error={"type": type(exc).__name__, "message": str(exc)[:240]},
            )

    def _execute_claimed(
        self,
        operation: DatabaseMaintenanceOperation,
        token: UUID,
    ) -> DatabaseMaintenanceOperation:
        request = dict(operation.request)
        artifact_payload: dict[str, Any] | None = None
        if operation.kind is DatabaseMaintenanceOperationKind.IMPORT:
            artifact_payload = self._load_and_validate_artifact(operation.artifact_id, operation)
        with self._unit_of_work() as uow:
            live = self._repository._require_live_claim(uow, operation.id, token, _utc())
            if artifact_payload is not None:
                # Lock and re-validate in the same UoW that performs mutation;
                # an expiry/hash change can never race the commit boundary.
                artifact = self._artifact_in_uow(uow, operation.artifact_id)
                artifact_payload = self._validate_artifact_record(artifact, operation)
            result = self._apply_mutation(uow.session, operation.kind, request, artifact_payload)
            state = {**dict(live["state_json"] or {}), "mutation_committed": True}
            row = self._repository._terminal_update_in_uow(
                uow,
                operation.id,
                token,
                status="confirmed",
                state=state,
                result=result,
                error=None,
                now=_utc(),
            )
            self._repository._complete_outbox(uow, operation.id)
            uow.commit()
            return _operation(row)

    def _load_and_validate_artifact(
        self,
        artifact_id: UUID | None,
        operation: DatabaseMaintenanceOperation,
    ) -> dict[str, Any]:
        if artifact_id is None:
            raise DatabaseMaintenanceValidationError("import operation is missing artifact_id")
        artifact = self._repository.read_artifact(artifact_id)
        return self._validate_artifact_record(artifact, operation)

    def _artifact_in_uow(self, uow: UnitOfWork, artifact_id: UUID | None) -> BackupArtifact:
        if artifact_id is None:
            raise DatabaseMaintenanceValidationError("import operation is missing artifact_id")
        row = (
            uow.session.execute(
                text("SELECT * FROM database_backup_artifacts WHERE id = :id FOR UPDATE"),
                {"id": artifact_id},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise DatabaseMaintenanceValidationError("backup artifact was not found")
        return _artifact(row)

    def _validate_artifact_record(
        self,
        artifact: BackupArtifact | None,
        operation: DatabaseMaintenanceOperation,
    ) -> dict[str, Any]:
        if artifact is None:
            raise DatabaseMaintenanceValidationError("backup artifact was not found")
        now = _utc()
        if artifact.status != "available" or artifact.compressed_bytes is None or artifact.expires_at <= now:
            raise DatabaseMaintenanceArtifactExpired("backup artifact is expired or has no retained bytes")
        if artifact.compressed_size > MAX_COMPRESSED_SIZE or artifact.expanded_size > MAX_EXPANDED_SIZE:
            raise DatabaseMaintenanceSizeLimitError("backup artifact exceeds configured size limits")
        if artifact.compression_ratio > MAX_COMPRESSION_RATIO:
            raise DatabaseMaintenanceSizeLimitError("backup artifact compression ratio exceeds 20x")
        if hashlib.sha256(artifact.compressed_bytes).digest() != artifact.compressed_sha256:
            raise DatabaseMaintenanceValidationError("backup compressed hash verification failed")
        decoded, _stored, expanded_size, compressed_size, ratio, compressed_hash, expanded_hash = inspect_backup_bytes(
            artifact.compressed_bytes,
            filename=artifact.source_filename,
        )
        if (
            expanded_size != artifact.expanded_size
            or compressed_size != artifact.compressed_size
            or ratio > MAX_COMPRESSION_RATIO
            or compressed_hash != artifact.compressed_sha256
            or expanded_hash != artifact.expanded_sha256
        ):
            raise DatabaseMaintenanceValidationError("backup artifact size/hash metadata verification failed")
        if str(decoded.get("schema_fingerprint") or "").strip().lower() != artifact.schema_fingerprint:
            raise DatabaseMaintenanceValidationError("backup artifact schema fingerprint metadata mismatch")
        if int(decoded.get("format_version") or 0) != artifact.format_version:
            raise DatabaseMaintenanceValidationError("backup artifact format metadata mismatch")
        requested_tables = operation.request.get("tables")
        if requested_tables is not None:
            _validate_table_list(requested_tables)
        return decoded

    def _apply_mutation(
        self,
        session: Any,
        kind: DatabaseMaintenanceOperationKind,
        request: Mapping[str, Any],
        backup: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        if kind is DatabaseMaintenanceOperationKind.IMPORT:
            if backup is None:
                raise DatabaseMaintenanceValidationError("import backup payload is missing")
            return _import_tables(session, backup, request)
        if kind is DatabaseMaintenanceOperationKind.CLEAR_TABLES:
            tables = _validate_mutation_tables(request.get("tables"), allow_share=False)
            _preflight_foreign_keys(session, tables)
            deleted: dict[str, int] = {}
            for table in _delete_order(session, tables):
                result = session.execute(text(f"DELETE FROM {_quote_identifier(table)}"))
                deleted[table] = int(getattr(result, "rowcount", 0) or 0)
            return {"deleted_rows": deleted, "total_deleted": sum(deleted.values())}
        if kind is DatabaseMaintenanceOperationKind.PREPARE_FOR_LIBRARY_REBUILD:
            return _prepare_for_library_rebuild(session)
        if kind is DatabaseMaintenanceOperationKind.CLEANUP_OFFLINE_MEDIA:
            return _cleanup_offline_media(session)
        if kind is DatabaseMaintenanceOperationKind.CLEAR_VECTORS:
            result = session.execute(
                text("UPDATE media_metadata SET overview_embedding = NULL WHERE overview_embedding IS NOT NULL")
            )
            return {"cleared_vectors": int(getattr(result, "rowcount", 0) or 0)}
        if kind is DatabaseMaintenanceOperationKind.CORRECT_SEQUENCES:
            return {"corrected_tables": _correct_sequences(session)}
        raise DatabaseMaintenanceValidationError(f"unsupported maintenance kind: {kind.value}")


class DatabaseMaintenanceService:
    def __init__(
        self,
        repository: PostgresDatabaseMaintenanceRepository,
        *,
        unit_of_work: UnitOfWorkFactory,
        settings: Callable[[], Any] | None = None,
        jobs: Any = None,
    ) -> None:
        self._repository = repository
        self._unit_of_work = unit_of_work
        self._settings = settings or (lambda: None)
        self._jobs = jobs

    @property
    def repository(self) -> PostgresDatabaseMaintenanceRepository:
        return self._repository

    def export(self, tables: Sequence[str]) -> ExportedDatabase:
        normalized_tables = _validate_table_list(tables)
        with self._unit_of_work() as uow:
            _ensure_existing_tables(uow.session, normalized_tables)
            exported: dict[str, list[dict[str, Any]]] = {}
            for table in normalized_tables:
                rows = uow.session.execute(text(f"SELECT * FROM {_quote_identifier(table)}")).mappings().all()
                exported[table] = [{str(key): _jsonable(value) for key, value in row.items()} for row in rows]
            fingerprint = schema_fingerprint(uow.session)
        payload = {
            "format_version": BACKUP_FORMAT_VERSION,
            "schema_fingerprint": fingerprint,
            "metadata": {
                "format_version": BACKUP_FORMAT_VERSION,
                "schema_fingerprint": fingerprint,
                "export_date": _utc().isoformat(),
                "tables": normalized_tables,
                "source_emby_server_id": self._server_id(),
            },
            "data": exported,
        }
        expanded = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=_json_default
        ).encode("utf-8")
        if len(expanded) > MAX_EXPANDED_SIZE:
            raise DatabaseMaintenanceSizeLimitError("expanded backup exceeds 256 MiB")
        compressed = gzip.compress(expanded, mtime=0)
        if len(compressed) > MAX_COMPRESSED_SIZE:
            raise DatabaseMaintenanceSizeLimitError("compressed backup exceeds 64 MiB")
        ratio = Decimal(len(expanded)) / Decimal(len(compressed))
        if ratio > MAX_COMPRESSION_RATIO:
            raise DatabaseMaintenanceSizeLimitError("backup compression ratio exceeds 20x")
        timestamp = _utc().strftime("%Y%m%d-%H%M%S")
        return ExportedDatabase(
            compressed, f"database_backup_{timestamp}.json.gz", len(expanded), len(compressed), ratio
        )

    def preview(self, content: bytes, *, filename: str = "backup.json.gz") -> DatabaseMaintenanceHttpResult:
        decoded, stored, expanded_size, compressed_size, ratio, _compressed_hash, _expanded_hash = inspect_backup_bytes(
            content,
            filename=filename,
        )
        artifact = self._repository.create_artifact(content, source_filename=filename)
        tables = sorted(str(name) for name in (decoded.get("data") or {}).keys())
        source_id = str((decoded.get("metadata") or {}).get("source_emby_server_id") or "").strip()
        current_id = self._server_id()
        import_mode = "overwrite" if source_id and current_id and source_id == current_id else "share"
        return DatabaseMaintenanceHttpResult(
            {
                "status": "success",
                "artifact_id": str(artifact.id),
                "tables": tables,
                "import_mode": import_mode,
                "format_version": int(decoded["format_version"]),
                "schema_fingerprint": str(decoded["schema_fingerprint"]),
                "expires_at": artifact.expires_at,
                "compressed_size": compressed_size,
                "expanded_size": expanded_size,
                "compression_ratio": str(ratio),
            },
            200,
        )

    def submit(
        self,
        kind: DatabaseMaintenanceOperationKind | str,
        request: Mapping[str, Any],
        *,
        principal_id: str,
        artifact_id: UUID | None = None,
        idempotency_key: str | None = None,
    ) -> DatabaseMaintenanceHttpResult:
        normalized_kind = _kind(kind)
        normalized_request = dict(request)
        if normalized_kind is DatabaseMaintenanceOperationKind.IMPORT:
            if artifact_id is None:
                raw = normalized_request.get("artifact_id")
                try:
                    artifact_id = UUID(str(raw))
                except (TypeError, ValueError) as exc:
                    raise DatabaseMaintenanceValidationError("artifact_id must be a UUID") from exc
            if self._repository.read_artifact(artifact_id) is None:
                raise DatabaseMaintenanceValidationError("backup artifact was not found")
            strategy = (
                str(normalized_request.get("import_strategy") or normalized_request.get("mode") or "share")
                .strip()
                .lower()
            )
            if strategy not in {"share", "overwrite"}:
                raise DatabaseMaintenanceValidationError("import_strategy must be share or overwrite")
            normalized_request["import_strategy"] = strategy
            normalized_request["artifact_id"] = str(artifact_id)
            if "tables" in normalized_request:
                _validate_mutation_tables(normalized_request.get("tables"), allow_share=strategy == "share")
            if strategy == "overwrite":
                tables = (
                    _validate_table_list(normalized_request.get("tables") or [])
                    if "tables" in normalized_request
                    else []
                )
                if any(table in OVERWRITE_DENYLIST for table in tables):
                    raise DatabaseMaintenanceValidationError(
                        "overwrite cannot restore emby_users, cleanup_index, or invitations"
                    )
        elif normalized_kind is DatabaseMaintenanceOperationKind.CLEAR_TABLES:
            _validate_mutation_tables(normalized_request.get("tables"), allow_share=False)
        if self._jobs is not None and bool(getattr(self._jobs, "has_active_job", lambda: False)()):
            raise DatabaseMaintenanceBusyError("another Job is active")
        reservation = self._repository.reserve(
            kind=normalized_kind,
            principal_id=principal_id,
            request=normalized_request,
            artifact_id=artifact_id,
            idempotency_key=idempotency_key,
        )
        operation = reservation.operation
        return DatabaseMaintenanceHttpResult(_operation_payload(operation), 202)

    def status(self, operation_id: UUID) -> DatabaseMaintenanceHttpResult:
        operation = self._repository.read(operation_id)
        if operation is None:
            return DatabaseMaintenanceHttpResult({"status": "error", "message": "operation not found"}, 404)
        return DatabaseMaintenanceHttpResult(_operation_payload(operation), 200)

    def cleanup_expired_artifacts(self, *, limit: int = 100) -> int:
        return self._repository.purge_expired_artifacts(limit=limit)

    def _server_id(self) -> str:
        try:
            settings = self._settings()
            if settings is None:
                value = ""
            elif hasattr(settings, "get"):
                value = settings.get("emby_server_id")
            else:
                value = getattr(settings, "emby_server_id", "")
        except Exception:
            value = ""
        return str(value or "").strip()


def _operation_payload(operation: DatabaseMaintenanceOperation) -> dict[str, Any]:
    status_url = f"/api/database/maintenance/operations/{operation.id}"
    return {
        "status": "accepted"
        if operation.status is DatabaseMaintenanceOperationStatus.READY
        else operation.status.value,
        "operation_id": str(operation.id),
        "status_url": status_url,
        "operation": {
            "operation_id": str(operation.id),
            "kind": operation.kind.value,
            "status": operation.status.value,
            "artifact_id": str(operation.artifact_id) if operation.artifact_id else None,
            "status_url": status_url,
            "created_at": operation.created_at,
            "updated_at": operation.updated_at,
            "result": dict(operation.result) if operation.result is not None else None,
            "error": dict(operation.error) if operation.error is not None else None,
        },
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return {"__etk_bytes__": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_jsonable(item) for item in value]
    return value


def _validate_table_list(value: object) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise DatabaseMaintenanceValidationError("tables must be a non-empty string array")
    tables = [_identifier(item, field="table") for item in value]
    if not tables:
        raise DatabaseMaintenanceValidationError("tables must not be empty")
    if len(set(tables)) != len(tables):
        raise DatabaseMaintenanceValidationError("tables must not contain duplicates")
    return tables


def _validate_mutation_tables(value: object, *, allow_share: bool) -> list[str]:
    tables = _validate_table_list(value)
    if any(table in PROTECTED_TABLES or _looks_control_plane(table) for table in tables):
        raise DatabaseMaintenanceValidationError("control-plane tables are protected from mutation")
    if allow_share and any(table not in SHARE_ALLOWLIST for table in tables):
        raise DatabaseMaintenanceValidationError("share import table is outside the exact allowlist")
    return tables


def _looks_control_plane(table: str) -> bool:
    return table.startswith(("job_", "outbox", "idempotency")) or table.endswith(
        ("_operations", "_artifacts", "_leases")
    )


def _ensure_existing_tables(session: Any, tables: Sequence[str]) -> None:
    rows = (
        session.execute(
            text(
                """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY(:tables)
            """
            ),
            {"tables": list(tables)},
        )
        .scalars()
        .all()
    )
    missing = sorted(set(tables) - {str(row) for row in rows})
    if missing:
        raise DatabaseMaintenanceValidationError(f"unknown database table(s): {', '.join(missing)}")


def _preflight_foreign_keys(session: Any, tables: Sequence[str]) -> None:
    rows = (
        session.execute(
            text(
                """
                SELECT child.relname AS child_table, parent.relname AS parent_table
                FROM pg_constraint constraint_row
                JOIN pg_class child ON child.oid = constraint_row.conrelid
                JOIN pg_class parent ON parent.oid = constraint_row.confrelid
                JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
                JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                WHERE constraint_row.contype = 'f'
                  AND child_ns.nspname = 'public' AND parent_ns.nspname = 'public'
                """
            )
        )
        .mappings()
        .all()
    )
    selected = set(tables)
    blocked = sorted(
        f"{row['parent_table']} (referenced by {row['child_table']})"
        for row in rows
        if str(row["parent_table"]) in selected and str(row["child_table"]) not in selected
    )
    if blocked:
        raise DatabaseMaintenanceConflictError("protected foreign-key reference blocks mutation: " + ", ".join(blocked))


def _delete_order(session: Any, tables: Sequence[str]) -> list[str]:
    # Child tables first when a request intentionally includes both sides of a
    # foreign key.  Stable lexical order keeps the operation deterministic.
    selected = set(tables)
    edges: dict[str, set[str]] = {table: set() for table in selected}
    rows = (
        session.execute(
            text(
                """
                SELECT child.relname AS child_table, parent.relname AS parent_table
                FROM pg_constraint c
                JOIN pg_class child ON child.oid = c.conrelid
                JOIN pg_class parent ON parent.oid = c.confrelid
                JOIN pg_namespace ns_child ON ns_child.oid = child.relnamespace
                JOIN pg_namespace ns_parent ON ns_parent.oid = parent.relnamespace
                WHERE c.contype = 'f' AND ns_child.nspname = 'public' AND ns_parent.nspname = 'public'
                """
            )
        )
        .mappings()
        .all()
    )
    for row in rows:
        child, parent = str(row["child_table"]), str(row["parent_table"])
        if child in selected and parent in selected:
            edges[parent].add(child)
    ordered: list[str] = []
    remaining = set(selected)
    while remaining:
        ready = sorted(table for table in remaining if not (edges[table] & remaining))
        if not ready:
            return sorted(remaining) + ordered
        ordered.extend(ready)
        remaining.difference_update(ready)
    return ordered


def _import_tables(session: Any, backup: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    strategy = str(request.get("import_strategy") or "share").strip().lower()
    tables = _validate_table_list(request.get("tables") or list((backup.get("data") or {}).keys()))
    data = backup.get("data")
    if not isinstance(data, Mapping):
        raise DatabaseMaintenanceValidationError("backup data is invalid")
    current_fingerprint = schema_fingerprint(session)
    if str(backup.get("schema_fingerprint") or "").strip().lower() != current_fingerprint:
        raise DatabaseMaintenanceValidationError("backup schema_fingerprint does not match current schema")
    if strategy == "share":
        _validate_mutation_tables(tables, allow_share=True)
    elif strategy == "overwrite":
        _validate_mutation_tables(tables, allow_share=False)
        if any(table in OVERWRITE_DENYLIST for table in tables):
            raise DatabaseMaintenanceValidationError(
                "overwrite denylist contains emby_users, cleanup_index, or invitations"
            )
    else:
        raise DatabaseMaintenanceValidationError("import_strategy must be share or overwrite")
    _ensure_existing_tables(session, tables)
    if strategy == "overwrite":
        _preflight_foreign_keys(session, tables)
    imported: dict[str, int] = {}
    # `_delete_order` already returns child tables before their referenced
    # parents.  Keep that order for overwrite deletes so PostgreSQL foreign
    # keys remain valid inside the single transaction.
    ordered_tables = _delete_order(session, tables) if strategy == "overwrite" else list(tables)
    for table in ordered_tables:
        rows = data.get(table, [])
        if not isinstance(rows, list):
            raise DatabaseMaintenanceValidationError(f"backup table {table!r} is invalid")
        prepared = _prepare_rows_for_table(session, table, rows, share=strategy == "share")
        if strategy == "overwrite":
            session.execute(text(f"DELETE FROM {_quote_identifier(table)}"))
        inserted = 0
        for row in prepared:
            if not row:
                continue
            columns = sorted(row)
            placeholders = ", ".join(f":v{i}" for i in range(len(columns)))
            params = {f"v{i}": row[column] for i, column in enumerate(columns)}
            conflict = _conflict_clause(table, share=strategy == "share")
            statement = text(
                f"INSERT INTO {_quote_identifier(table)} ({', '.join(_quote_identifier(c) for c in columns)}) "
                f"VALUES ({placeholders}) {conflict}"
            )
            result = session.execute(statement, params)
            inserted += int(getattr(result, "rowcount", 0) or 0)
        imported[table] = inserted
    # Import changes database state but must never restart the running service
    # from the worker or request thread.  The caller surfaces this explicit
    # manual-action flag in the terminal operation result.
    return {
        "import_strategy": strategy,
        "imported_rows": imported,
        "restart_required": True,
    }


def _prepare_rows_for_table(session: Any, table: str, rows: list[Any], *, share: bool) -> list[dict[str, Any]]:
    columns = {
        str(row[0])
        for row in session.execute(
            text(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = :table
                """
            ),
            {"table": table},
        ).all()
    }
    prepared: list[dict[str, Any]] = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise DatabaseMaintenanceValidationError(f"backup row for {table!r} is not an object")
        row = {str(key): _decode_json_value(value) for key, value in raw.items() if str(key) in columns}
        if share and table == "person_metadata":
            row.pop("map_id", None)
        elif share and table == "media_metadata":
            row.pop("emby_item_ids_json", None)
            row.pop("asset_details_json", None)
            row["in_library"] = False
        elif share and table == "p115_mediainfo_cache":
            raw_ffprobe = row.get("raw_ffprobe_json")
            if isinstance(raw_ffprobe, str):
                try:
                    raw_ffprobe = json.loads(raw_ffprobe)
                except json.JSONDecodeError:
                    raw_ffprobe = None
            if not isinstance(raw_ffprobe, Mapping):
                continue
            cleaned = dict(raw_ffprobe)
            fmt = cleaned.get("format")
            if isinstance(fmt, Mapping):
                cleaned["format"] = {key: value for key, value in fmt.items() if key != "filename"}
            sha1 = str(row.get("sha1") or "").strip().upper()
            if not sha1:
                continue
            row = {"sha1": sha1, "raw_ffprobe_json": cleaned}
        prepared.append(row)
    return prepared


def _conflict_clause(table: str, *, share: bool) -> str:
    if not share:
        return ""
    targets = {
        "person_metadata": "tmdb_person_id",
        "translation_cache": "original_text",
        "media_metadata": "tmdb_id, item_type",
        "p115_mediainfo_cache": "sha1",
    }
    target = targets.get(table)
    if not target:
        raise DatabaseMaintenanceValidationError(f"share import conflict target is not defined for {table!r}")
    return f"ON CONFLICT ({', '.join(_quote_identifier(part.strip()) for part in target.split(','))}) DO NOTHING"


def _prepare_for_library_rebuild(session: Any) -> dict[str, Any]:
    truncated: list[str] = []
    for table in ("user_media_data", "resubscribe_index", "cleanup_index"):
        if _table_exists(session, table):
            _preflight_foreign_keys(session, [table])
            session.execute(text(f"DELETE FROM {_quote_identifier(table)}"))
            truncated.append(table)
    updated: dict[str, int] = {}
    if _table_exists(session, "media_metadata"):
        result = session.execute(
            text(
                """
                UPDATE media_metadata
                SET in_library = FALSE, emby_item_ids_json = '[]'::jsonb,
                    date_added = NULL, last_synced_at = NULL, last_updated_at = NOW()
                WHERE in_library IS TRUE
                   OR (jsonb_typeof(emby_item_ids_json) = 'array' AND jsonb_array_length(emby_item_ids_json) > 0)
                """
            )
        )
        updated["media_metadata"] = int(getattr(result, "rowcount", 0) or 0)
    if _table_exists(session, "collections_info"):
        result = session.execute(
            text(
                """
                UPDATE collections_info
                SET emby_collection_id = NULL, is_active = FALSE, last_checked_at = NOW()
                WHERE emby_collection_id IS NOT NULL OR is_active IS TRUE
                """
            )
        )
        updated["collections_info"] = int(getattr(result, "rowcount", 0) or 0)
    if _table_exists(session, "custom_collections"):
        result = session.execute(
            text(
                """
                UPDATE custom_collections
                SET emby_collection_id = NULL, in_library_count = 0
                WHERE emby_collection_id IS NOT NULL
                """
            )
        )
        updated["custom_collections"] = int(getattr(result, "rowcount", 0) or 0)
    return {
        "truncated_tables": truncated,
        "updated_rows": updated,
        "preserved_tables": ["emby_users", "emby_users_extended", "app_settings"],
    }


def _cleanup_offline_media(session: Any) -> dict[str, int]:
    result = session.execute(
        text(
            """
            DELETE FROM media_metadata
            WHERE in_library = FALSE AND subscription_status = 'NONE'
              AND (watching_status IS NULL OR watching_status = 'NONE')
            """
        )
    )
    media_deleted = int(getattr(result, "rowcount", 0) or 0)
    result = session.execute(
        text(
            """
            DELETE FROM resubscribe_index ri
            WHERE NOT EXISTS (
                SELECT 1 FROM media_metadata mm
                WHERE mm.tmdb_id = ri.tmdb_id AND mm.item_type = ri.item_type
            )
            """
        )
    )
    resubscribe_cleaned = int(getattr(result, "rowcount", 0) or 0)
    result = session.execute(
        text(
            """
            DELETE FROM cleanup_index ci
            WHERE NOT EXISTS (
                SELECT 1 FROM media_metadata mm
                WHERE mm.tmdb_id = ci.tmdb_id AND mm.item_type = ci.item_type
            )
            """
        )
    )
    return {
        "media_metadata_deleted": media_deleted,
        "resubscribe_index_cleaned": resubscribe_cleaned,
        "cleanup_index_cleaned": int(getattr(result, "rowcount", 0) or 0),
    }


def _correct_sequences(session: Any) -> list[str]:
    rows = (
        session.execute(
            text(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND column_default LIKE 'nextval%'
                ORDER BY table_name, column_name
                """
            )
        )
        .mappings()
        .all()
    )
    corrected: list[str] = []
    for row in rows:
        table = _identifier(row["table_name"])
        column = _identifier(row["column_name"], field="column")
        session.execute(
            text(
                f"SELECT setval(pg_get_serial_sequence(:table_name, :column_name), "
                f"COALESCE((SELECT MAX({_quote_identifier(column)}) FROM {_quote_identifier(table)}), 0) + 1, false)"
            ),
            {"table_name": table, "column_name": column},
        )
        corrected.append(table)
    return corrected


def _table_exists(session: Any, table: str) -> bool:
    return bool(
        session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = :table)"
            ),
            {"table": table},
        )
    )


__all__ = [
    "ARTIFACT_TTL",
    "BACKUP_FORMAT_VERSION",
    "BackupArtifact",
    "DatabaseMaintenanceArtifactExpired",
    "DatabaseMaintenanceBusyError",
    "DatabaseMaintenanceConflictError",
    "DatabaseMaintenanceError",
    "DatabaseMaintenanceExecutor",
    "DatabaseMaintenanceHttpResult",
    "DatabaseMaintenanceOperation",
    "DatabaseMaintenanceOperationKind",
    "DatabaseMaintenanceOperationStatus",
    "DatabaseMaintenanceOperationStep",
    "DatabaseMaintenanceReservation",
    "DatabaseMaintenanceService",
    "DatabaseMaintenanceSizeLimitError",
    "DatabaseMaintenanceStateError",
    "DatabaseMaintenanceValidationError",
    "DATABASE_MAINTENANCE_AGGREGATE_TYPE",
    "DATABASE_MAINTENANCE_TASK_KEY",
    "DATABASE_MAINTENANCE_TOPIC",
    "DATABASE_MAINTENANCE_TOPICS",
    "MAX_COMPRESSED_SIZE",
    "MAX_COMPRESSION_RATIO",
    "MAX_EXPANDED_SIZE",
    "OVERWRITE_DENYLIST",
    "PROTECTED_TABLES",
    "PostgresDatabaseMaintenanceRepository",
    "SHARE_ALLOWLIST",
    "ExportedDatabase",
    "inspect_backup_bytes",
    "operation_key",
    "schema_fingerprint",
]
