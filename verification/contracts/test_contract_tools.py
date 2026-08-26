from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts/refactor"))

from compare_contracts import compare  # noqa: E402
from export_contracts import _loopback_base_url, _percentile  # noqa: E402
from manifest_common import apply_contract_override  # noqa: E402
from scan_fixture_secrets import (  # noqa: E402
    scan_path,
    scan_payload,
)


class ContractToolTests(unittest.TestCase):
    def test_contract_diff_reports_exact_pointer(self) -> None:
        differences = compare(
            {"status": "success", "data": {"count": 1}},
            {"status": "success", "data": {"count": 2}},
        )
        self.assertEqual(
            differences,
            [{"path": "/data/count", "kind": "value", "expected": 1, "actual": 2}],
        )

    def test_contract_diff_supports_explicit_ignore(self) -> None:
        self.assertEqual(compare({"trace_id": "a"}, {"trace_id": "b"}, {"/trace_id"}), [])

    def test_fixtures_are_sanitized(self) -> None:
        self.assertEqual(scan_path(ROOT / "verification/fixtures"), [])

    def test_route_paths_are_not_mistaken_for_filesystem_paths(self) -> None:
        self.assertEqual(scan_payload({"path": "/api/health"}, "fixture"), [])
        self.assertNotEqual(scan_payload({"config_root": "/private/config"}, "fixture"), [])

    def test_runtime_capture_refuses_non_loopback_hosts(self) -> None:
        self.assertEqual(_loopback_base_url("http://127.0.0.1:5257/"), "http://127.0.0.1:5257")
        with self.assertRaises(ValueError):
            _loopback_base_url("https://example.invalid")

    def test_runtime_capture_uses_nearest_rank_percentiles(self) -> None:
        self.assertEqual(_percentile([1.0, 2.0, 3.0, 4.0], 0.50), 2.0)
        self.assertEqual(_percentile([1.0, 2.0, 3.0, 4.0], 0.95), 4.0)

    def test_ownership_overlay_preserves_lifecycle_fields(self) -> None:
        entry = {"contract_id": "task-fixture", "status": "legacy", "new_owner": None}
        overrides = {
            "schema_version": 1,
            "contracts": {
                "tasks": {
                    "task-fixture": {
                        "status": "new",
                        "new_owner": "src.etk.operations.tasks:fixture",
                    }
                }
            },
        }
        self.assertEqual(
            apply_contract_override(entry, "tasks", overrides),
            {
                "contract_id": "task-fixture",
                "status": "new",
                "new_owner": "src.etk.operations.tasks:fixture",
            },
        )


if __name__ == "__main__":
    unittest.main()
