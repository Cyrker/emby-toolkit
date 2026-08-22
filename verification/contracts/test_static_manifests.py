from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / "scripts/refactor"
sys.path.insert(0, str(TOOLS))

from check_manifests import check_manifests  # noqa: E402
from export_manifests import EXPECTED, export_all  # noqa: E402


class StaticManifestTests(unittest.TestCase):
    def test_generated_counts_and_known_variances(self) -> None:
        with tempfile.TemporaryDirectory(prefix="etk-p0-test-") as temporary:
            summary = export_all(Path(temporary))
            self.assertEqual(summary["counts"], EXPECTED)
            self.assertEqual(
                summary["baseline_variances"]["frontend_page_routes"]["source_page_route_count"],
                24,
            )
            self.assertEqual(
                summary["baseline_variances"]["task_help"]["task_keys_without_explicit_help"],
                ["play-pool-daily-speedtest"],
            )

    def test_committed_manifests_are_current(self) -> None:
        self.assertEqual(check_manifests(ROOT / "verification/contracts"), [])

    def test_method_bindings_are_unique(self) -> None:
        manifest = json.loads(
            (ROOT / "verification/contracts/route-ownership.yaml").read_text(encoding="utf-8")
        )
        identities = [(item["method"], item["path"]) for item in manifest["method_bindings"]]
        self.assertEqual(len(identities), 367)
        self.assertEqual(len(set(identities)), 367)

    def test_table_indexes_and_p115_task_stage_are_complete(self) -> None:
        table_manifest = json.loads(
            (ROOT / "verification/contracts/table-ownership.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(table_manifest["metrics"]["static_index_statement_count"], 68)
        self.assertEqual(table_manifest["metrics"]["unique_static_index_name_count"], 67)
        self.assertEqual(table_manifest["metrics"]["duplicate_static_index_name_count"], 1)

        task_manifest = json.loads(
            (ROOT / "verification/contracts/task-ownership.yaml").read_text(encoding="utf-8")
        )
        task_stages = {item["task_key"]: item["target_stage"] for item in task_manifest["tasks"]}
        self.assertEqual(task_stages["repair-p115-fingerprints"], "P6")

        config_manifest = json.loads(
            (ROOT / "verification/contracts/config-ownership.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(config_manifest["metrics"]["standalone_setting_key_count"], 52)
        self.assertEqual(config_manifest["metrics"]["unresolved_setting_call_count"], 0)

    def test_every_legacy_contract_has_a_target_stage(self) -> None:
        for name, collection in [
            ("route-ownership.yaml", "routes"),
            ("task-ownership.yaml", "tasks"),
            ("table-ownership.yaml", "tables"),
            ("frontend-route-ownership.yaml", "routes"),
            ("bridge-contract-ownership.yaml", "contracts"),
        ]:
            manifest = json.loads((ROOT / "verification/contracts" / name).read_text(encoding="utf-8"))
            missing = [item.get("contract_id") for item in manifest[collection] if not item.get("target_stage")]
            self.assertEqual(missing, [], name)

    def test_disposable_runtime_route_variances_are_classified(self) -> None:
        payload = json.loads(
            (ROOT / "verification/contracts/http/static-vs-runtime-route-diff.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(payload["counts"]["runtime_api_rules"], 345)
        self.assertEqual(payload["counts"]["runtime_proxy_rules"], 3)
        self.assertEqual(payload["counts"]["runtime_method_bindings_normalized"], 368)
        self.assertEqual(
            {
                (item["application"], item["method"], item["path"])
                for item in payload["runtime_only_normalized"]
            },
            {
                ("api", "GET", "/"),
                ("api", "GET", "/<path:path>"),
                ("api", "GET", "/api/health"),
                ("api", "GET", "/static/<path:filename>"),
                ("proxy", "GET", "/static/<path:filename>"),
            },
        )
        self.assertEqual(payload["static_only_normalized"], [])


if __name__ == "__main__":
    unittest.main()
