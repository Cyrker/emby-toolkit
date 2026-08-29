from __future__ import annotations

import ast
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
USE_CASE = REPO_ROOT / "src/etk/domains/subscription/cleanup.py"
ROUTER = REPO_ROOT / "src/etk/domains/subscription/cleanup_router.py"
REPOSITORY = REPO_ROOT / "src/etk/domains/subscription/cleanup_repository.py"
MEDIA_PROJECTION = REPO_ROOT / "src/etk/domains/media/cleanup_projection.py"
SCAN_USE_CASE = REPO_ROOT / "src/etk/domains/subscription/cleanup_scan.py"
SCAN_REPOSITORY = REPO_ROOT / "src/etk/domains/subscription/cleanup_scan_repository.py"
SCAN_ADAPTER = REPO_ROOT / "src/etk/domains/subscription/cleanup_adapters.py"
TASK_MANIFEST = REPO_ROOT / "verification/contracts/task-ownership.yaml"
FIXTURE = REPO_ROOT / "verification/fixtures/http/p7-cleanup.json"
LEGACY_IMPORT_ROOTS = {
    "config_manager",
    "constants",
    "database",
    "extensions",
    "handler",
    "routes",
    "task_manager",
    "tasks",
}


def _imports(tree: ast.AST) -> set[str]:
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            result.add(node.module)
    return result


def _uow_blocks(method: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.With]:
    blocks: list[ast.With] = []
    for node in ast.walk(method):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            expression = item.context_expr
            if (
                isinstance(expression, ast.Call)
                and isinstance(expression.func, ast.Attribute)
                and expression.func.attr == "_unit_of_work"
            ):
                blocks.append(node)
                break
    return blocks


def _uow_commit_calls(method: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "commit"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "uow"
    ]


def test_cleanup_http_and_use_case_import_only_native_ports() -> None:
    for path in (USE_CASE, ROUTER):
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imports = _imports(tree)
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports)
        assert "sqlalchemy" not in imports
        assert "cleanup_index" not in source
        assert "media_metadata" not in source

    use_case_source = USE_CASE.read_text(encoding="utf-8")
    assert 'SCAN_CLEANUP_TASK_KEY = "scan-cleanup-issues"' in use_case_source
    assert "task_scan_for_cleanup_issues" not in use_case_source
    assert "task_execute_cleanup" not in use_case_source
    assert "execute-cleanup" not in use_case_source
    assert "/api/media/batch_delete" not in use_case_source


def test_cleanup_index_sql_stays_p7_and_media_metadata_sql_stays_p5() -> None:
    repository_source = REPOSITORY.read_text(encoding="utf-8")
    projection_source = MEDIA_PROJECTION.read_text(encoding="utf-8")
    repository_tree = ast.parse(repository_source, filename=str(REPOSITORY))
    projection_tree = ast.parse(projection_source, filename=str(MEDIA_PROJECTION))

    assert "cleanup_index" in repository_source
    assert "media_metadata" not in repository_source
    assert "media_metadata" in projection_source
    assert "cleanup_index" in projection_source
    assert "sqlalchemy" in _imports(repository_tree)
    assert "sqlalchemy" in _imports(projection_tree)
    assert not any(module.startswith("etk.domains.subscription") for module in _imports(projection_tree))


def test_cleanup_mutations_have_explicit_commit_and_version_convergence_lock() -> None:
    source = REPOSITORY.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(REPOSITORY))
    repository = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "SqlAlchemyCleanupRepository"
    )
    methods = {node.name: node for node in repository.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    for method_name in {"delete_tasks", "remove_deleted_version", "_update_status"}:
        method = methods[method_name]
        blocks = _uow_blocks(method)
        commits = _uow_commit_calls(method)
        assert len(blocks) == 1, method_name
        assert len(commits) == 1, method_name
        assert commits[0] in set(ast.walk(blocks[0])), method_name

    remove_source = ast.get_source_segment(source, methods["remove_deleted_version"])
    assert remove_source is not None
    assert "FOR UPDATE" in remove_source
    assert "status = 'pending'" in remove_source
    assert "len(remaining) <= 1" in remove_source


def test_cleanup_fixture_records_external_and_p8_boundaries() -> None:
    fixture = FIXTURE.read_text(encoding="utf-8")
    assert "scan-cleanup-issues" in fixture
    assert "P5 port" in fixture
    assert "no database transaction is held across physical deletion" in fixture
    assert "/api/media/batch_delete is outside this slice" in fixture


def test_cleanup_scan_has_processor_free_execution_and_split_sql_ownership() -> None:
    for path in (SCAN_USE_CASE, SCAN_REPOSITORY):
        source = path.read_text(encoding="utf-8")
        imports = _imports(ast.parse(source, filename=str(path)))
        assert not any(module.split(".")[0] in LEGACY_IMPORT_ROOTS for module in imports)
    use_case_source = SCAN_USE_CASE.read_text(encoding="utf-8")
    repository_source = SCAN_REPOSITORY.read_text(encoding="utf-8")
    projection_source = MEDIA_PROJECTION.read_text(encoding="utf-8")
    adapter_source = SCAN_ADAPTER.read_text(encoding="utf-8")
    assert "media_metadata" not in use_case_source
    assert "cleanup_index" not in use_case_source
    assert "media_metadata" not in repository_source
    assert "cleanup_index" in repository_source
    assert "LOCK TABLE app_settings IN SHARE MODE" in repository_source
    assert "LOCK TABLE emby_users, media_metadata IN SHARE MODE" in projection_source
    assert "task_scan_for_cleanup_issues" not in use_case_source
    assert (
        "handler."
        not in adapter_source.split("class HttpxCleanupEmbyInventoryGateway", 1)[1].split(
            "class SafeLocalCleanupGateway", 1
        )[0]
    )


def test_cleanup_scan_atomic_reconcile_and_task_manifest_are_explicit() -> None:
    source = SCAN_REPOSITORY.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(SCAN_REPOSITORY))
    repository = next(
        node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "SqlAlchemyCleanupScanRepository"
    )
    reconcile = next(
        node
        for node in repository.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == "reconcile"
    )
    blocks = _uow_blocks(reconcile)
    commits = _uow_commit_calls(reconcile)
    reconcile_source = ast.get_source_segment(source, reconcile)
    assert len(blocks) == 1
    assert len(commits) == 1
    assert commits[0] in set(ast.walk(blocks[0]))
    assert reconcile_source is not None
    assert "LOCK TABLE cleanup_index IN EXCLUSIVE MODE" in reconcile_source
    assert "cleanup_execution_items" in reconcile_source
    assert "DELETE FROM cleanup_index WHERE status = 'pending'" in reconcile_source
    assert "ON CONFLICT (tmdb_id, item_type) DO UPDATE" in reconcile_source

    manifest = json.loads(TASK_MANIFEST.read_text(encoding="utf-8"))
    task = next(item for item in manifest["tasks"] if item["task_key"] == "scan-cleanup-issues")
    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p8_effect_tasks.P8EffectTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.subscription.cleanup_scan.CleanupScanService",
    }
