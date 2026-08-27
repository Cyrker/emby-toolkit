from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import text

from etk.domains.operations.database_maintenance import (
    ARTIFACT_TTL,
    DatabaseMaintenanceBusyError,
    DatabaseMaintenanceExecutor,
    DatabaseMaintenanceOperationKind,
    DatabaseMaintenanceService,
    PostgresDatabaseMaintenanceRepository,
    schema_fingerprint,
)
from etk.platform.db import DatabaseRuntime


def _fingerprint(runtime: DatabaseRuntime) -> str:
    with runtime.unit_of_work() as uow:
        return schema_fingerprint(uow.session)


def _backup(fingerprint: str, *, tables: dict[str, list[dict[str, object]]] | None = None) -> bytes:
    payload = {
        "format_version": 1,
        "schema_fingerprint": fingerprint,
        "metadata": {"source_emby_server_id": "fixture"},
        "data": tables
        or {
            "translation_cache": [
                {
                    "original_text": "pg18 source",
                    "translated_text": "pg18 translation",
                    "engine_used": "stub",
                }
            ]
        },
    }
    return gzip.compress(json.dumps(payload, separators=(",", ":")).encode("utf-8"), mtime=0)


def _repository(runtime: DatabaseRuntime) -> PostgresDatabaseMaintenanceRepository:
    return PostgresDatabaseMaintenanceRepository(runtime.unit_of_work)


def test_artifact_is_immutable_deduplicated_and_purged_at_ttl(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    content = _backup(_fingerprint(p8_runtime))
    now = datetime(2026, 8, 27, tzinfo=UTC)

    first = repository.create_artifact(content, now=now)
    replay = repository.create_artifact(content, now=now + timedelta(minutes=1))
    assert replay.id == first.id
    assert first.expires_at == now + ARTIFACT_TTL
    assert repository.read_artifact(first.id).compressed_bytes == first.compressed_bytes

    assert repository.purge_expired_artifacts(now=first.expires_at) == 1
    expired = repository.read_artifact(first.id)
    assert expired is not None and expired.status == "expired" and expired.compressed_bytes is None

    replacement = repository.create_artifact(content, now=now + ARTIFACT_TTL + timedelta(minutes=1))
    assert replacement.id != first.id


def test_import_is_artifact_only_and_commits_mutation_with_terminal_state(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    content = _backup(_fingerprint(p8_runtime))
    artifact = repository.create_artifact(content)
    service = DatabaseMaintenanceService(
        repository,
        unit_of_work=p8_runtime.unit_of_work,
        settings=lambda: SimpleNamespace(emby_server_id="fixture"),
    )

    accepted = service.submit(
        DatabaseMaintenanceOperationKind.IMPORT,
        {"artifact_id": str(artifact.id), "import_strategy": "share"},
        principal_id="admin",
        idempotency_key="import-artifact-only",
    )
    operation_id = UUID(str(accepted.payload["operation_id"]))
    operation = repository.read(operation_id)
    assert operation is not None and operation.artifact_id == artifact.id
    assert "backup" not in operation.request and "backup_bytes" not in operation.request

    executor = DatabaseMaintenanceExecutor(
        repository,
        unit_of_work=p8_runtime.unit_of_work,
        worker_id="maintenance-pg18",
        lease_seconds=60,
    )
    terminal = executor.execute(operation_id, artifact_id=artifact.id)
    assert terminal is not None and terminal.status.value == "confirmed"
    assert terminal.result == {
        "import_strategy": "share",
        "imported_rows": {"translation_cache": 1},
        "restart_required": True,
    }
    with p8_runtime.engine.connect() as connection:
        row = connection.execute(
            text("SELECT translated_text FROM translation_cache WHERE original_text = 'pg18 source'")
        ).scalar_one()
        outbox_status = connection.execute(
            text("SELECT status FROM outbox_events WHERE aggregate_id = :operation_id"),
            {"operation_id": str(operation_id)},
        ).scalar_one()
    assert row == "pg18 translation"
    assert outbox_status == "processed"


def test_active_maintenance_scope_returns_fixed_conflict(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    service = DatabaseMaintenanceService(repository, unit_of_work=p8_runtime.unit_of_work)
    service.submit(
        DatabaseMaintenanceOperationKind.CLEAR_TABLES,
        {"tables": ["translation_cache"]},
        principal_id="admin",
        idempotency_key="first-maintenance",
    )
    with pytest.raises(DatabaseMaintenanceBusyError):
        service.submit(
            DatabaseMaintenanceOperationKind.CLEAR_TABLES,
            {"tables": ["media_metadata"]},
            principal_id="admin",
            idempotency_key="second-maintenance",
        )
