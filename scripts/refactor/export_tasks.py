from __future__ import annotations

import argparse
import ast
from pathlib import Path
from typing import Any

from manifest_common import (
    REPO_ROOT,
    ManifestError,
    apply_contract_override,
    dump_yaml_compatible_json,
    evaluate_static,
    parse_python,
    source_ref,
    stable_id,
)

P5_TASK_EXECUTION_OWNERS = {
    "actor-translation": "tasks.actors.task_persons_translation",
    "backfill-media-metadata": {
        "default": "tasks.media.task_backfill_media_metadata",
        "when_targets_present": "tasks.media.task_backfill_media_metadata_batch",
    },
    "populate-metadata": "tasks.media.task_populate_metadata_cache",
    "restore-nfo-and-images": "tasks.media.task_fill_missing_video_screenshots",
    "role-translation": "tasks.media.task_role_translation",
}

P6_TASK_EXECUTION_OWNERS = {
    "backfill-organize-record-tmdb-ids": ("etk.domains.p115.organize_records.P115OrganizeRecordTmdbBackfillService"),
    "full-sync-strm": "etk.domains.p115.strm_sync.P115StrmSyncService",
    "intro-fingerprint-backfill": ("etk.domains.p115.intro_detection_compatibility.P115IntroBackfillTaskService"),
    "manual-correct-organize-records": ("etk.domains.p115.organize_execution.P115OrganizeExecutionService.reconcile"),
    "monitor-115-life-events": "etk.domains.p115.strm_sync.P115StrmSyncService",
    "play-pool-daily-speedtest": "etk.domains.p115.playback_service.P115PlaybackService.daily_speedtest",
    "repair-p115-fingerprints": "etk.domains.p115.fingerprints.P115FingerprintRepairService",
    "scan-monitor-folders": "etk.processes.p6_ingestion_tasks.P6IngestionScanService",
    "scan-organize-115": "etk.domains.p115.organize_execution.P115OrganizeExecutionService.execute",
    "sync-115-directory-tree": "etk.domains.p115.directory_sync.P115DirectorySyncService",
}


P5_TASKS = {
    "populate-metadata",
    "backfill-media-metadata",
    "role-translation",
    "actor-translation",
    "restore_mediainfo",
    "restore-nfo-and-images",
}
P6_TASKS = {
    "scan-monitor-folders",
    "scan-organize-115",
    "full-sync-strm",
    "backfill-organize-record-tmdb-ids",
    "monitor-115-life-events",
    "sync-115-directory-tree",
    "manual-correct-organize-records",
    "play-pool-daily-speedtest",
    "intro-fingerprint-backfill",
    "repair-p115-fingerprints",
}
P7_TASKS = {
    "enrich-aliases",
    "process-watchlist",
    "subscribe-assistant-maintenance",
    "actor-tracking",
    "auto-subscribe",
    "refresh_completed_series",
    "add-all-series-to-watchlist",
    "resubscribe-library",
    "manual_subscribe_batch",
    "scan_old_seasons_backfill",
    "update-resubscribe-cache",
    "merge-duplicate-actors",
    "purge-ghost-actors",
    "recalculate_library_washing_priorities",
}
P7_NATIVE_EXECUTION_OWNERS = {
    "recalculate_library_washing_priorities": (
        "etk.domains.p115.washing_recalculation.P115WashingPriorityRecalculationService"
    ),
}
P7_COMPATIBILITY_EXECUTION_OWNERS = {
    "subscribe-assistant-maintenance": (
        "etk.processes.p7_subscription_tasks.LegacySubscribeAssistantMaintenanceGateway"
    ),
}
P8_TASKS = {
    "custom-collections",
    "generate-all-covers",
    "generate-custom-collection-covers",
    "sync-all-user-data",
    "generate_embeddings",
    "system-auto-update",
    "process_all_custom_collections",
    "process-single-custom-collection",
    "scan-cleanup-issues",
    "update-daily-theme",
    "refresh-collections",
    "execute-auto-tagging-rules",
    "fill-studio-images",
    "check-expired-users",
}
P9_TASKS = {
    "hdhive-auto-checkin",
    "shared-resource-maintenance",
    "share-all-library",
    "shared-share-status-sync",
}
P9_TASK_EXECUTION_OWNER = "etk.processes.p9_sharing.P9SharingTaskRouter"


def _task_owner(task_key: str) -> tuple[str, list[str]]:
    if task_key in {"task-chain-high-freq", "task-chain-low-freq"}:
        return "P4", ["operations"]
    if task_key in P5_TASKS:
        return "P5", ["media", "ingestion"]
    if task_key in P6_TASKS:
        return "P6", ["p115", "ingestion"]
    if task_key in P7_TASKS:
        return "P7", ["subscription", "people"]
    if task_key in P8_TASKS:
        return "P8", ["collection", "users", "operations"]
    if task_key in P9_TASKS:
        return "P9", ["sharing"]
    raise ManifestError(f"task owner mapping missing for {task_key}")


def _find_assignment(tree: ast.Module, name: str) -> ast.Assign:
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return node
    raise ManifestError(f"assignment {name} not found")


def _help_texts(tree: ast.Module) -> dict[str, str]:
    node = _find_assignment(tree, "TASK_HELP_TEXTS")
    value = evaluate_static(node.value)
    return {str(key): str(text) for key, text in value.items()}


def export_tasks() -> dict[str, Any]:
    path = REPO_ROOT / "tasks/core.py"
    _, tree = parse_python(path)
    registry = _find_assignment(tree, "full_registry")
    if not isinstance(registry.value, ast.Dict):
        raise ManifestError("full_registry is not a dictionary literal")
    help_texts = _help_texts(tree)
    entries: list[dict[str, Any]] = []
    # This exporter also runs under the macOS system Python, where zip(strict=...) is unavailable.
    for key_node, value_node in zip(registry.value.keys, registry.value.values):  # noqa: B905
        if key_node is None:
            continue
        task_key = str(evaluate_static(key_node))
        if not isinstance(value_node, (ast.Tuple, ast.List)) or len(value_node.elts) != 4:
            raise ManifestError(f"invalid task tuple for {task_key} at {source_ref(path, value_node.lineno)}")
        function_node, name_node, processor_node, chain_node = value_node.elts
        if not isinstance(function_node, (ast.Name, ast.Attribute)):
            raise ManifestError(f"invalid task function for {task_key}")
        function_name = function_node.id if isinstance(function_node, ast.Name) else function_node.attr
        stage, domains = _task_owner(task_key)
        display_name = str(evaluate_static(name_node))
        explicit_help = help_texts.get(task_key)
        entry = {
            "contract_id": stable_id("task", task_key),
            "task_key": task_key,
            "function": function_name,
            "display_name": display_name,
            "help": explicit_help or display_name,
            "help_source": "TASK_HELP_TEXTS" if explicit_help else "display_name_fallback",
            "processor_type": str(evaluate_static(processor_node)),
            "chain_eligible": bool(evaluate_static(chain_node)),
            "visible_in": [
                "all",
                *(["chain"] if bool(evaluate_static(chain_node)) else []),
                *(
                    ["manual"]
                    if bool(evaluate_static(chain_node))
                    and task_key not in {"task-chain-high-freq", "task-chain-low-freq"}
                    else []
                ),
            ],
            "source": source_ref(path, key_node.lineno),
            "target_domains": domains,
            "target_stage": stage,
            "status": "legacy",
            "new_owner": None,
            "retirement_approval": None,
            "validation_owner": "/".join(domains),
            "validation_method": "task-registry-contract",
        }
        if task_key == "restore_mediainfo":
            entry["ownership_layers"] = {
                "envelope_owner": "etk.processes.p5_media_tasks.P5MediaTaskRouter",
                "execution_kind": "native-use-case",
                "execution_owner": "etk.domains.p115.media_info_restore.P115MediaInfoRestoreService",
            }
        elif task_key in P5_TASK_EXECUTION_OWNERS:
            execution_owner = P5_TASK_EXECUTION_OWNERS[task_key]
            ownership_layers: dict[str, Any] = {
                "envelope_owner": "etk.processes.p5_media_tasks.P5MediaTaskRouter",
                "execution_kind": "legacy-compatibility-callable",
                "retirement_stage": "P11",
            }
            if isinstance(execution_owner, str):
                ownership_layers["execution_owner"] = execution_owner
            else:
                ownership_layers["execution_owners"] = dict(execution_owner)
            entry["ownership_layers"] = ownership_layers
        elif task_key in P6_TASK_EXECUTION_OWNERS:
            envelope_owner = (
                "etk.processes.p6_ingestion_tasks.P6IngestionTaskRouter"
                if task_key == "scan-monitor-folders"
                else "etk.processes.p6_p115_tasks.P6P115TaskRouter"
            )
            entry["ownership_layers"] = {
                "envelope_owner": envelope_owner,
                "execution_kind": "native-use-case",
                "execution_owner": P6_TASK_EXECUTION_OWNERS[task_key],
            }
        elif task_key in P7_TASKS:
            execution_owner = P7_NATIVE_EXECUTION_OWNERS.get(task_key)
            compatibility_owner = P7_COMPATIBILITY_EXECUTION_OWNERS.get(
                task_key,
                "etk.processes.p7_subscription_tasks.LegacyP7SubscriptionCompatibilityGateway",
            )
            entry["ownership_layers"] = {
                "envelope_owner": "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter",
                "execution_kind": "native-use-case" if execution_owner else "legacy-compatibility-callable",
                "execution_owner": execution_owner or compatibility_owner,
                **({} if execution_owner else {"retirement_stage": "P11"}),
            }
        elif task_key in P9_TASKS:
            entry.update(
                {
                    "status": "new",
                    "new_owner": P9_TASK_EXECUTION_OWNER,
                    "validation_owner": "sharing/task-envelope",
                    "validation_method": "ordered-task-registry-native-router-worker-outcome-and-postgres18",
                    "ownership_layers": {
                        "envelope_owner": P9_TASK_EXECUTION_OWNER,
                        "execution_kind": "typed-provider-adapter",
                        "execution_owner": P9_TASK_EXECUTION_OWNER,
                    },
                }
            )
        entries.append(
            apply_contract_override(
                entry,
                "tasks",
            )
        )
    entries.sort(key=lambda item: item["task_key"])
    fallback_help = [item["task_key"] for item in entries if item["help_source"] == "display_name_fallback"]
    registry_keys = {item["task_key"] for item in entries}
    orphan_help = sorted(set(help_texts) - registry_keys)
    return {
        "schema_version": 1,
        "generator": "scripts/refactor/export_tasks.py",
        "source_scope": ["tasks/core.py:get_task_registry"],
        "metrics": {
            "task_key_count": len(entries),
            "chain_eligible_count": sum(1 for item in entries if item["chain_eligible"]),
            "manual_visible_count": sum(1 for item in entries if "manual" in item["visible_in"]),
            "display_name_help_fallback_count": len(fallback_help),
            "orphan_help_text_count": len(orphan_help),
        },
        "baseline_variances": {
            "task_keys_without_explicit_help": fallback_help,
            "help_keys_without_registered_task": orphan_help,
            "manual_hidden_keys_are_not_chain_eligible": [
                "task-chain-high-freq",
                "task-chain-low-freq",
            ],
        },
        "tasks": entries,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export the legacy task ownership manifest.")
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/task-ownership.yaml",
    )
    args = parser.parse_args()
    payload = export_tasks()
    dump_yaml_compatible_json(args.output, payload)
    print(f"task_keys={payload['metrics']['task_key_count']} output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
