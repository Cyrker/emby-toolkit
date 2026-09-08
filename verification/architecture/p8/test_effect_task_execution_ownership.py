from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "scripts/refactor"))

from scripts.refactor.export_tasks import P8_NATIVE_EXECUTION_OWNERS  # noqa: E402

TASK_MANIFEST = REPO_ROOT / "verification/contracts/task-ownership.yaml"
USER_EXPIRY = REPO_ROOT / "src/etk/domains/identity/user_expiry.py"
DAILY_THEME = REPO_ROOT / "src/etk/domains/media/daily_theme.py"
DAILY_THEME_REPOSITORY = REPO_ROOT / "src/etk/domains/media/daily_theme_repository.py"
P8_ROUTER = REPO_ROOT / "src/etk/processes/p8_effect_tasks.py"
WORKER = REPO_ROOT / "src/etk/processes/worker.py"
LEGACY_IMPORT_ROOTS = {"database", "extensions", "handler", "task_manager", "tasks"}
P11_NATIVE_EFFECT_TASKS = {
    "custom-collections": (
        "etk.domains.collection.custom_tasks.CustomCollectionTaskService",
        REPO_ROOT / "src/etk/domains/collection/custom_tasks.py",
        '"custom-collections": ("tasks.custom_collections"',
    ),
    "execute-auto-tagging-rules": (
        "etk.domains.media.auto_tagging.AutoTaggingService",
        REPO_ROOT / "src/etk/domains/media/auto_tagging.py",
        '"execute-auto-tagging-rules": ("tasks.media"',
    ),
    "fill-studio-images": (
        "etk.domains.media.studio_images.StudioImageService",
        REPO_ROOT / "src/etk/domains/media/studio_images.py",
        '"fill-studio-images": ("tasks.media"',
    ),
    "generate-all-covers": (
        "etk.domains.collection.cover_task_execution.NativeCoverTaskService",
        REPO_ROOT / "src/etk/domains/collection/cover_task_execution.py",
        '"generate-all-covers": ("tasks.covers"',
    ),
    "generate-custom-collection-covers": (
        "etk.domains.collection.cover_task_execution.NativeCoverTaskService",
        REPO_ROOT / "src/etk/domains/collection/cover_task_execution.py",
        '"generate-custom-collection-covers": ("tasks.covers"',
    ),
    "generate_embeddings": (
        "etk.domains.media.embeddings.MediaEmbeddingService",
        REPO_ROOT / "src/etk/domains/media/embeddings.py",
        '"generate_embeddings": ("tasks.vector_tasks"',
    ),
    "refresh-collections": (
        "etk.domains.collection.refresh.NativeCollectionRefreshService",
        REPO_ROOT / "src/etk/domains/collection/refresh.py",
        '"refresh-collections": ("tasks.tmdb_collections"',
    ),
    "process-single-custom-collection": (
        "etk.domains.collection.custom_tasks.CustomCollectionTaskService",
        REPO_ROOT / "src/etk/domains/collection/custom_tasks.py",
        '"process-single-custom-collection": ("tasks.custom_collections"',
    ),
    "process_all_custom_collections": (
        "etk.domains.collection.custom_tasks.CustomCollectionTaskService",
        REPO_ROOT / "src/etk/domains/collection/custom_tasks.py",
        '"process_all_custom_collections": ("tasks.custom_collections"',
    ),
}


def _task(task_key: str) -> dict[str, object]:
    manifest = json.loads(TASK_MANIFEST.read_text(encoding="utf-8"))
    return next(entry for entry in manifest["tasks"] if entry["task_key"] == task_key)


def test_user_sync_has_native_processed_execution_ownership() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["sync-all-user-data"] == ("etk.domains.identity.user_sync.UserSyncService")
    task = _task("sync-all-user-data")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.identity.user_sync.UserSyncService",
    }
    assert "processor-free normal path" in str(task["notes"])


def test_user_expiry_has_native_durable_execution_ownership_and_no_legacy_imports() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["check-expired-users"] == (
        "etk.domains.identity.user_expiry.IdentityUserExpiryService"
    )
    task = _task("check-expired-users")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.identity.user_expiry.IdentityUserExpiryService",
    }
    assert task["validation_method"] == (
        "ordered-task-registry-native-router-worker-outcome-postgres18-redis-celery-and-import-sentinel"
    )
    assert "durable EXPIRE_USER" in str(task["notes"])

    expiry_tree = ast.parse(USER_EXPIRY.read_text(encoding="utf-8"), filename=str(USER_EXPIRY))
    imports = {alias.name for node in ast.walk(expiry_tree) if isinstance(node, ast.Import) for alias in node.names}
    imports.update(
        node.module for node in ast.walk(expiry_tree) if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports)
    router_source = P8_ROUTER.read_text(encoding="utf-8")
    assert '"check-expired-users": ("tasks.users"' not in router_source


def test_system_auto_update_has_native_skipped_execution_ownership() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["system-auto-update"] == ("etk.processes.p8_effect_tasks.P8EffectTaskRouter")
    task = _task("system-auto-update")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
    }
    assert "native SKIPPED" in str(task["notes"])


def test_daily_theme_has_native_atomic_execution_ownership_and_no_legacy_imports() -> None:
    assert P8_NATIVE_EXECUTION_OWNERS["update-daily-theme"] == ("etk.domains.media.daily_theme.DailyThemeService")
    task = _task("update-daily-theme")

    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.media.daily_theme.DailyThemeService",
    }
    assert task["validation_method"] == (
        "ordered-task-registry-native-router-worker-outcome-postgres18-redis-celery-and-import-sentinel"
    )
    for path in (DAILY_THEME, DAILY_THEME_REPOSITORY):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        imports.update(
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports)
    router_source = P8_ROUTER.read_text(encoding="utf-8")
    assert '"update-daily-theme": ("tasks.discover"' not in router_source


def test_p11_effect_tasks_have_native_execution_ownership_and_no_legacy_fallback() -> None:
    router_source = P8_ROUTER.read_text(encoding="utf-8")
    for task_key, (owner, path, forbidden_fallback) in P11_NATIVE_EFFECT_TASKS.items():
        assert P8_NATIVE_EXECUTION_OWNERS[task_key] == owner
        task = _task(task_key)
        assert task["ownership_layers"] == {
            "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
            "execution_kind": "native-use-case",
            "execution_owner": owner,
        }
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
        imports.update(
            node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module is not None
        )
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports)
        assert forbidden_fallback not in router_source


def test_worker_profiles_each_build_one_media_effect_executor() -> None:
    worker_source = WORKER.read_text(encoding="utf-8")

    assert worker_source.count("PersistentMediaEffectExecutor(") == 2
    assert "p8_media_effects_executor" not in worker_source


def test_cover_tasks_are_processor_free_in_both_worker_profiles() -> None:
    worker = __import__("etk.processes.worker", fromlist=["_NATIVE_ONLY_EXECUTABLE_TASK_KEYS"])
    worker_source = WORKER.read_text(encoding="utf-8")

    assert {"generate-all-covers", "generate-custom-collection-covers"} <= set(worker._NATIVE_ONLY_EXECUTABLE_TASK_KEYS)
    assert worker_source.count("cover_remote = HttpxCoverRemoteAdapter(settings_provider)") == 2
    assert worker_source.count("cover_tasks=_native_cover_task_service(") == 2
    assert worker_source.count("ConfiguredCoverRenderer(") == 1


def test_custom_collection_aliases_are_native_and_shared_across_worker_profiles() -> None:
    worker_source = WORKER.read_text(encoding="utf-8")

    assert {
        "custom-collections",
        "process-single-custom-collection",
        "process_all_custom_collections",
    }.issubset(
        set(
            __import__(
                "etk.processes.worker", fromlist=["_NATIVE_ONLY_EXECUTABLE_TASK_KEYS"]
            )._NATIVE_ONLY_EXECUTABLE_TASK_KEYS
        )
    )
    assert worker_source.count("custom_collection_tasks = _custom_collection_task_service(") == 2
    assert worker_source.count("custom_collection_tasks=custom_collection_tasks") == 2
    assert "LegacyCustomCollection" not in worker_source
