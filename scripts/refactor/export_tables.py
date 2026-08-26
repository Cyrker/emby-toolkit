from __future__ import annotations

import argparse
import ast
import re
from pathlib import Path
from typing import Any

from manifest_common import (
    REPO_ROOT,
    apply_contract_override,
    dump_yaml_compatible_json,
    parse_python,
    relative_path,
    stable_id,
)

TABLE_OWNER_RULES: list[tuple[set[str], list[str], str]] = [
    ({"processed_log", "failed_log"}, ["operations"], "P3"),
    ({"app_settings"}, ["system"], "P2"),
    ({"translation_cache", "emby_search_index", "emby_search_rebuild_state"}, ["media"], "P5"),
    (
        {
            "emby_users",
            "user_media_data",
            "emby_favorite_items",
            "user_templates",
            "invitations",
            "emby_users_extended",
        },
        ["identity", "users"],
        "P3/P8",
    ),
    ({"collections_info", "custom_collections"}, ["collection"], "P8"),
    (
        {"media_metadata", "media_image_cache", "media_image_policy_cache", "person_metadata"},
        ["media"],
        "P5",
    ),
    (
        {
            "subscribe_assistant_state",
            "subscribe_assistant_snapshots",
            "subscribe_assistant_delete_records",
            "actor_subscriptions",
            "washing_priority_groups",
            "resubscribe_rules",
            "resubscribe_index",
            "cleanup_index",
        },
        ["subscription", "people"],
        "P7",
    ),
    (
        {
            "p115_filesystem_cache",
            "p115_mediainfo_cache",
            "p115_intro_fingerprint_cache",
            "p115_intro_detection_failures",
            "p115_intro_fingerprint_failures",
            "p115_upload_records",
            "p115_organize_records",
        },
        ["p115", "ingestion"],
        "P6",
    ),
    (
        {
            "shared_credit_snapshot",
            "shared_credit_ledger_local",
            "shared_rapid_sources",
            "shared_rapid_source_files",
            "shared_virtual_imports",
            "shared_completed_season_share_channels",
        },
        ["sharing"],
        "P9",
    ),
]


CREATE_TABLE_RE = re.compile(
    r"^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*;?$",
    re.IGNORECASE | re.DOTALL,
)
CREATE_INDEX_RE = re.compile(
    r"^CREATE\s+(UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+"
    r"([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+([A-Za-z_][A-Za-z0-9_]*)\s*(.+?)\s*;?$",
    re.IGNORECASE | re.DOTALL,
)
ALTER_TABLE_RE = re.compile(
    r"^ALTER\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.*?)\s*;?$",
    re.IGNORECASE | re.DOTALL,
)


def _normalized_sql(value: str) -> str:
    return " ".join(value.split())


def _split_top_level(value: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    escaped = False
    for character in value:
        if quote:
            current.append(character)
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == quote:
                quote = None
            continue
        if character in {"'", '"'}:
            quote = character
            current.append(character)
        elif character == "(":
            depth += 1
            current.append(character)
        elif character == ")":
            depth -= 1
            current.append(character)
        elif character == "," and depth == 0:
            item = _normalized_sql("".join(current))
            if item:
                parts.append(item)
            current = []
        else:
            current.append(character)
    item = _normalized_sql("".join(current))
    if item:
        parts.append(item)
    return parts


def _table_owner(name: str) -> tuple[list[str], str]:
    for names, domains, stage in TABLE_OWNER_RULES:
        if name in names:
            return domains, stage
    raise ValueError(f"table owner mapping missing for {name}")


def export_tables() -> dict[str, Any]:
    path = REPO_ROOT / "database/connection.py"
    _, tree = parse_python(path)
    rel = relative_path(path)
    sql_literals = sorted(
        (
            (node.lineno, node.value.strip())
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and re.match(r"^\s*(CREATE|ALTER)\b", node.value, re.IGNORECASE)
        ),
        key=lambda item: item[0],
    )
    tables: list[dict[str, Any]] = []
    table_by_name: dict[str, dict[str, Any]] = {}
    for line, sql in sql_literals:
        match = CREATE_TABLE_RE.match(sql)
        if not match:
            continue
        name, body = match.groups()
        domains, stage = _table_owner(name)
        columns: list[dict[str, str]] = []
        constraints: list[str] = []
        for definition in _split_top_level(body):
            if re.match(r"^(CONSTRAINT|PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CHECK)\b", definition, re.I):
                constraints.append(definition)
                continue
            column_match = re.match(r'^"?([A-Za-z_][A-Za-z0-9_]*)"?\s+(.+)$', definition, re.S)
            if not column_match:
                constraints.append(definition)
                continue
            columns.append({"name": column_match.group(1), "definition": column_match.group(2)})
        entry = apply_contract_override(
            {
                "contract_id": stable_id("table", name),
                "name": name,
                "source": f"{rel}:{line}",
                "create_sql": _normalized_sql(sql),
                "columns_static": columns,
                "constraints_static": constraints,
                "indexes_static": [],
                "post_create_alters": [],
                "target_domains": domains,
                "target_stage": stage,
                "status": "legacy",
                "new_owner": None,
                "authoritative_schema_fixture": "verification/contracts/db/legacy-schema.sql",
                "validation_owner": "/".join(domains),
                "validation_method": "postgres-schema-contract",
            },
            "tables",
        )
        if name in table_by_name:
            raise ValueError(f"duplicate CREATE TABLE for {name}")
        table_by_name[name] = entry
        tables.append(entry)

    indexes: list[dict[str, Any]] = []
    for line, sql in sql_literals:
        match = CREATE_INDEX_RE.match(sql)
        if not match:
            continue
        unique, name, table, tail = match.groups()
        entry = {
            "name": name,
            "table": table,
            "unique": bool(unique),
            "definition": _normalized_sql(f"CREATE {'UNIQUE ' if unique else ''}INDEX {name} ON {table} {tail}"),
            "source": f"{rel}:{line}",
        }
        indexes.append(entry)
        if table in table_by_name:
            table_by_name[table]["indexes_static"].append(entry)

    alters: list[dict[str, Any]] = []
    for line, sql in sql_literals:
        match = ALTER_TABLE_RE.match(sql)
        if not match:
            continue
        table, operation = match.groups()
        entry = {
            "table": table,
            "operation": _normalized_sql(operation),
            "source": f"{rel}:{line}",
        }
        alters.append(entry)
        if table in table_by_name:
            table_by_name[table]["post_create_alters"].append(entry)

    tables.sort(key=lambda item: item["name"])
    index_names: dict[str, list[dict[str, Any]]] = {}
    for item in indexes:
        index_names.setdefault(item["name"], []).append(item)
    duplicate_indexes = {name: definitions for name, definitions in sorted(index_names.items()) if len(definitions) > 1}
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_tables.py",
        "source_scope": ["database/connection.py"],
        "metrics": {
            "table_count": len(tables),
            "static_index_statement_count": len(indexes),
            "unique_static_index_name_count": len(index_names),
            "duplicate_static_index_name_count": len(duplicate_indexes),
            "static_alter_statement_count": len(alters),
        },
        "baseline_variances": {
            "duplicate_static_indexes": duplicate_indexes,
        },
        "limitations": [
            "Static DDL is a candidate baseline, not an authoritative PostgreSQL schema dump.",
            "Dynamic schema_upgrades and deprecated table/column loops require isolated database verification.",
        ],
        "tables": tables,
        "unattached_indexes": [item for item in indexes if item["table"] not in table_by_name],
        "unattached_alters": [item for item in alters if item["table"] not in table_by_name],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export static PostgreSQL table ownership data.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/table-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_tables()
    dump_yaml_compatible_json(args.output, payload)
    print(f"tables={payload['metrics']['table_count']} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
