from __future__ import annotations

import json

from scripts.refactor.export_openapi import DEFAULT_OUTPUT, EXPECTED_OPERATIONS, EXPECTED_PATHS, build_openapi


def test_p4_openapi_snapshot_is_current_and_typed() -> None:
    expected = json.loads(DEFAULT_OUTPUT.read_text(encoding="utf-8"))
    actual = build_openapi()

    assert actual == expected
    assert len(actual["paths"]) == EXPECTED_PATHS
    assert sum(len(methods) for methods in actual["paths"].values()) == EXPECTED_OPERATIONS
    assert len(actual["components"]["schemas"]) >= 28
