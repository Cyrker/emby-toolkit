from __future__ import annotations

import json
from pathlib import Path

from scripts.refactor.export_openapi import build_openapi

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = REPO_ROOT / "verification/fixtures/http/p9-sharing.json"


def test_retired_p9_sharing_fixture_is_absent_from_openapi_surface() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    schema = build_openapi()
    paths = schema["paths"]
    for route in fixture["routes"]:
        assert route["path"] not in paths
