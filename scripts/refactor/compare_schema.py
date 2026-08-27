#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, LiteralString, cast

import psycopg
from psycopg import sql

REQUIRED_SERVER_MAJOR = 18
CATALOG_KEYS = ("tables", "columns", "constraints", "indexes")

TABLES_SQL = """
SELECT n.nspname AS schema, c.relname AS name
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p')
ORDER BY c.relname
"""

COLUMNS_SQL = """
SELECT
    c.table_name AS table,
    c.column_name AS name,
    c.ordinal_position AS ordinal,
    c.is_nullable = 'YES' AS nullable,
    c.data_type,
    c.udt_name,
    c.character_maximum_length,
    c.numeric_precision,
    c.numeric_scale,
    c.datetime_precision,
    c.column_default AS default
FROM information_schema.columns AS c
WHERE c.table_schema = 'public'
ORDER BY c.table_name, c.ordinal_position
"""

CONSTRAINTS_SQL = """
SELECT
    cls.relname AS table,
    con.conname AS name,
    CASE con.contype
        WHEN 'p' THEN 'primary_key'
        WHEN 'u' THEN 'unique'
        WHEN 'f' THEN 'foreign_key'
        WHEN 'c' THEN 'check'
    END AS type,
    pg_catalog.pg_get_constraintdef(con.oid, true) AS definition
FROM pg_catalog.pg_constraint AS con
JOIN pg_catalog.pg_class AS cls ON cls.oid = con.conrelid
JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace
WHERE nsp.nspname = 'public' AND con.contype IN ('p', 'u', 'f', 'c')
ORDER BY cls.relname, con.conname
"""

INDEXES_SQL = """
SELECT
    tbl.relname AS table,
    idx.relname AS name,
    pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS definition
FROM pg_catalog.pg_index AS i
JOIN pg_catalog.pg_class AS idx ON idx.oid = i.indexrelid
JOIN pg_catalog.pg_class AS tbl ON tbl.oid = i.indrelid
JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = tbl.relnamespace
WHERE nsp.nspname = 'public'
ORDER BY tbl.relname, idx.relname
"""


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare a PostgreSQL 18 public schema with the ETK baseline")
    parser.add_argument("baseline", type=Path, help="committed legacy-pg-catalog.json (legacy-schema.sql is accepted)")
    parser.add_argument("database_url", help="disposable PostgreSQL 18 connection URL")
    parser.add_argument("--output", type=Path, help="also write the structured JSON result to this path")
    return parser


def _catalog_path(path: Path) -> Path:
    if path.suffix.lower() == ".sql":
        candidate = path.with_name("legacy-pg-catalog.json")
        if not candidate.is_file():
            raise FileNotFoundError(f"catalog baseline next to SQL dump is missing: {candidate}")
        return candidate
    return path


def load_catalog(path: Path) -> dict[str, Any]:
    resolved = _catalog_path(path)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("catalog baseline must be a JSON object")
    for key in CATALOG_KEYS:
        if not isinstance(payload.get(key), list):
            raise ValueError(f"catalog baseline field {key!r} must be a list")
    return payload


def _conninfo(value: str) -> str:
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def collect_catalog(database_url: str) -> dict[str, Any]:
    with psycopg.connect(_conninfo(database_url)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SHOW server_version_num")
            version_num_row = cursor.fetchone()
            assert version_num_row is not None
            server_version_num = str(version_num_row[0])
            cursor.execute("SHOW server_version")
            version_row = cursor.fetchone()
            assert version_row is not None
            server_version = str(version_row[0])
            cursor.execute("SELECT current_database() AS database")
            database_row = cursor.fetchone()
            assert database_row is not None
            database = str(database_row[0])
            return {
                "tables": _fetch(cursor, TABLES_SQL),
                "columns": _fetch(cursor, COLUMNS_SQL),
                "constraints": _fetch(cursor, CONSTRAINTS_SQL),
                "indexes": _fetch(cursor, INDEXES_SQL),
                "database": database,
                "server_version": server_version,
                "server_version_num": server_version_num,
            }


def _fetch(cursor: psycopg.Cursor[Any], query: str) -> list[dict[str, Any]]:
    cursor.execute(sql.SQL(cast(LiteralString, query)))
    names = [column.name for column in cursor.description or ()]
    # Keep the catalog comparator runnable with the system Python 3.9.
    return [dict(zip(names, row)) for row in cursor.fetchall()]  # noqa: B905


def _normalized_sql(value: Any) -> Any:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text.replace("public.", "")


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _matching_outer_parenthesis(value: str) -> bool:
    if not value.startswith("(") or not value.endswith(")"):
        return False
    depth = 0
    quote: str | None = None
    index = 0
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0 and index != len(value) - 1:
                return False
        index += 1
    return depth == 0 and quote is None


def _strip_outer_parentheses(value: str) -> str:
    result = value.strip()
    while _matching_outer_parenthesis(result):
        result = result[1:-1].strip()
    return result


def _split_top_level_boolean(value: str, operator: str) -> list[str]:
    parts: list[str] = []
    start = 0
    depth = 0
    quote: str | None = None
    index = 0
    upper = value.upper()
    marker = f" {operator} "
    while index < len(value):
        char = value[index]
        if quote is not None:
            if char == quote:
                if index + 1 < len(value) and value[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if char in {"'", '"'}:
            quote = char
            index += 1
            continue
        if char == "(":
            depth += 1
            index += 1
            continue
        if char == ")":
            depth -= 1
            index += 1
            continue
        if depth == 0 and upper.startswith(marker, index):
            parts.append(value[start:index].strip())
            index += len(marker)
            start = index
            continue
        index += 1
    if parts:
        parts.append(value[start:].strip())
    return parts


def _canonical_boolean_predicate(value: str) -> str:
    """Normalize PG16/PG18 redundant grouping without erasing boolean semantics."""

    expression = _strip_outer_parentheses(value)
    for operator in ("OR", "AND"):
        parts = _split_top_level_boolean(expression, operator)
        if parts:
            rendered = ",".join(_canonical_boolean_predicate(part) for part in parts)
            return f"{operator}({rendered})"
    return str(_normalized_sql(expression))


def _normalized_index_definition(value: Any) -> Any:
    text = _normalized_sql(value)
    if text is None:
        return None
    head, separator, predicate = text.partition(" WHERE ")
    if not separator:
        return text
    return f"{head}{separator}{_canonical_boolean_predicate(predicate)}"


def normalize_catalog(catalog: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    tables = sorted(
        ({"schema": str(item.get("schema") or "public"), "name": str(item["name"])} for item in catalog["tables"]),
        key=lambda item: (item["schema"], item["name"]),
    )
    columns = sorted(
        (
            {
                "table": str(item["table"]),
                "name": str(item["name"]),
                "ordinal": int(item["ordinal"]),
                "nullable": bool(item["nullable"]),
                "data_type": str(item["data_type"]),
                "udt_name": str(item["udt_name"]),
                "character_maximum_length": _optional_int(item.get("character_maximum_length")),
                "numeric_precision": _optional_int(item.get("numeric_precision")),
                "numeric_scale": _optional_int(item.get("numeric_scale")),
                "datetime_precision": _optional_int(item.get("datetime_precision")),
                "default": _normalized_sql(item.get("default")),
            }
            for item in catalog["columns"]
        ),
        key=lambda item: (item["table"], item["ordinal"], item["name"]),
    )
    constraints = sorted(
        (
            {
                "table": str(item["table"]),
                "name": str(item["name"]),
                "type": str(item["type"]),
                "definition": _normalized_sql(item["definition"]),
            }
            for item in catalog["constraints"]
            if item.get("type") in {"primary_key", "unique", "foreign_key", "check"}
        ),
        key=lambda item: (item["table"], item["name"]),
    )
    indexes = sorted(
        (
            {
                "table": str(item["table"]),
                "name": str(item["name"]),
                "definition": _normalized_index_definition(item["definition"]),
            }
            for item in catalog["indexes"]
        ),
        key=lambda item: (item["table"], item["name"]),
    )
    return {"tables": tables, "columns": columns, "constraints": constraints, "indexes": indexes}


def _keyed(items: Iterable[Mapping[str, Any]], key_fields: Sequence[str]) -> dict[tuple[Any, ...], dict[str, Any]]:
    return {tuple(item[field] for field in key_fields): dict(item) for item in items}


def _diff_items(
    expected: Sequence[Mapping[str, Any]],
    actual: Sequence[Mapping[str, Any]],
    *,
    key_fields: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    expected_by_key = _keyed(expected, key_fields)
    actual_by_key = _keyed(actual, key_fields)
    missing = [expected_by_key[key] for key in sorted(expected_by_key.keys() - actual_by_key.keys())]
    unexpected = [actual_by_key[key] for key in sorted(actual_by_key.keys() - expected_by_key.keys())]
    changed = [
        {
            "key": {field: key[index] for index, field in enumerate(key_fields)},
            "expected": expected_by_key[key],
            "actual": actual_by_key[key],
        }
        for key in sorted(expected_by_key.keys() & actual_by_key.keys())
        if expected_by_key[key] != actual_by_key[key]
    ]
    return {"missing": missing, "unexpected": unexpected, "changed": changed}


def compare_catalogs(
    expected: Mapping[str, Any],
    actual: Mapping[str, Any],
    *,
    required_server_major: int = REQUIRED_SERVER_MAJOR,
) -> dict[str, Any]:
    normalized_expected = normalize_catalog(expected)
    normalized_actual = normalize_catalog(actual)
    differences = {
        "tables": _diff_items(
            normalized_expected["tables"], normalized_actual["tables"], key_fields=("schema", "name")
        ),
        "columns": _diff_items(
            normalized_expected["columns"], normalized_actual["columns"], key_fields=("table", "name")
        ),
        "constraints": _diff_items(
            normalized_expected["constraints"], normalized_actual["constraints"], key_fields=("table", "name")
        ),
        "indexes": _diff_items(
            normalized_expected["indexes"], normalized_actual["indexes"], key_fields=("table", "name")
        ),
    }
    version_text = str(actual.get("server_version_num") or "")
    version_ok = version_text.isdigit() and int(version_text) // 10000 == required_server_major
    schema_ok = all(
        not values[kind] for values in differences.values() for kind in ("missing", "unexpected", "changed")
    )
    return {
        "match": bool(version_ok and schema_ok),
        "required_server_major": required_server_major,
        "actual_server_version": actual.get("server_version"),
        "actual_server_version_num": version_text,
        "server_version_match": version_ok,
        "counts": {
            key: {"expected": len(normalized_expected[key]), "actual": len(normalized_actual[key])}
            for key in CATALOG_KEYS
        },
        "differences": differences,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        expected = load_catalog(args.baseline)
        actual = collect_catalog(args.database_url)
        result = compare_catalogs(expected, actual)
    except Exception as exc:
        result = {"match": False, "error": f"{type(exc).__name__}: {exc}"}
        exit_code = 2
    else:
        exit_code = 0 if result["match"] else 1

    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    print(rendered)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
