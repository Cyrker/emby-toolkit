from __future__ import annotations

import ast
from pathlib import Path

from etk.domains import ingestion

REPO_ROOT = Path(__file__).resolve().parents[3]


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_ingestion_monitor_migration_is_the_single_successor_to_p6_6() -> None:
    migration = _source("migrations/versions/0009_p6_ingestion_monitor.py")

    assert 'revision: str = "0009_p6_ingestion_monitor"' in migration
    assert 'down_revision: str | Sequence[str] | None = "0008_p6_mediainfo_cache"' in migration
    assert '"ingestion_monitor_items"' in migration
    assert '"ingestion_pending_bindings"' in migration
    assert "ingestion.monitor.requested" in migration
    assert "uq_ingestion_monitor_producer_key" in migration
    assert "uq_ingestion_monitor_unclaimed_path" in migration
    assert "uq_ingestion_monitor_claimed_path" in migration
    assert "source IN ('watchdog', 'scan', 'active')" in migration


def test_native_ingestion_domain_has_no_legacy_or_external_runtime_imports() -> None:
    imported: set[str] = set()
    for relative_path in (
        "src/etk/domains/ingestion/monitoring.py",
        "src/etk/domains/ingestion/repository.py",
    ):
        tree = ast.parse(_source(relative_path))
        imported.update(alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names)
        imported.update(str(node.module or "") for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))

    forbidden = {
        "monitor_service",
        "database.media_db",
        "handler.emby",
        "handler.p115_upload_monitor",
        "requests",
        "httpx",
        "watchdog",
    }
    assert imported.isdisjoint(forbidden)


def test_ingestion_domain_exports_root_wiring_surfaces_without_a_public_task_key() -> None:
    assert ingestion.INGESTION_MONITOR_TOPIC == "ingestion.monitor.requested"
    assert callable(ingestion.monitor_outbox_event_key)
    assert ingestion.PostgresIngestionMonitorLedger.__module__ == "etk.domains.ingestion.repository"
    assert ingestion.SqlAlchemyIngestionRepository.__module__ == "etk.domains.ingestion.repository"
    assert ingestion.MonitorPolicy.__module__ == "etk.domains.ingestion.monitoring"
