from __future__ import annotations

import hashlib
import importlib.util
import json
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
AUTHORITATIVE_SQL = REPO_ROOT / "verification" / "contracts" / "db" / "legacy-schema.sql"
AUTHORITATIVE_CATALOG = REPO_ROOT / "verification" / "contracts" / "db" / "legacy-pg-catalog.json"
MIGRATION_SQL = REPO_ROOT / "migrations" / "versions" / "p2_legacy_baseline.sql"
REVISION = REPO_ROOT / "migrations" / "versions" / "0001_p2_legacy_baseline.py"


def _revision_module():
    spec = importlib.util.spec_from_file_location("p2_baseline_revision", REVISION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_baseline_is_self_contained_and_matches_committed_dump() -> None:
    authoritative = AUTHORITATIVE_SQL.read_bytes()
    migration = MIGRATION_SQL.read_bytes()
    assert migration == authoritative

    revision_source = REVISION.read_text(encoding="utf-8")
    assert "verification/" not in revision_source
    assert "init_db" not in revision_source

    module = _revision_module()
    assert module.BASELINE_SHA256 == hashlib.sha256(migration).hexdigest()
    assert len(module.TABLES) == 39
    assert len(tuple(module._statements(module._verified_sql()))) > 100


def test_migration_ddl_covers_authoritative_tables_constraints_and_indexes() -> None:
    catalog = json.loads(AUTHORITATIVE_CATALOG.read_text(encoding="utf-8"))
    ddl = MIGRATION_SQL.read_text(encoding="utf-8-sig")
    ddl_tables = sorted(re.findall(r"^CREATE TABLE public\.([a-z0-9_]+)", ddl, flags=re.MULTILINE))
    expected_tables = sorted(item["name"] for item in catalog["tables"])

    assert ddl_tables == expected_tables
    assert len(catalog["columns"]) == 478
    assert len(catalog["constraints"]) == 60
    assert len(catalog["indexes"]) == 120


def test_compose_and_image_keep_the_pg18_migration_pack_self_contained() -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker" / "compose.refactor.yml").read_text(encoding="utf-8"))
    dockerfile = (REPO_ROOT / "Dockerfile.refactor").read_text(encoding="utf-8")

    assert compose["services"]["db"]["image"].startswith("postgres:18-alpine@sha256:")
    assert "pg18_data:/var/lib/postgresql" in compose["services"]["db"]["volumes"]
    assert compose["services"]["migrate"]["command"] == [
        "alembic",
        "-c",
        "/app/alembic.ini",
        "upgrade",
        "head",
    ]
    assert "volumes" not in compose["services"]["migrate"]
    assert "COPY alembic.ini ./alembic.ini" in dockerfile
    assert "COPY migrations/ ./migrations/" in dockerfile
