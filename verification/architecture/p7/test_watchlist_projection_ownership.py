from __future__ import annotations

import ast
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SUBSCRIPTION_SOURCES = (
    REPO_ROOT / "src/etk/domains/subscription/watchlist.py",
    REPO_ROOT / "src/etk/domains/subscription/watchlist_router.py",
)
MEDIA_OWNER = REPO_ROOT / "src/etk/domains/media/watchlist_projection.py"
MUTATION_METHODS = {
    "add_watchlist_item",
    "update_watchlist_status",
    "remove_watchlist_item",
    "force_end_watchlist_seasons",
    "batch_update_watchlist_status",
    "batch_remove_watchlist_items",
    "mark_old_seasons_wanted",
    "update_watchlist_total_episodes",
}
LEGACY_IMPORTS = {
    "config_manager",
    "database",
    "extensions",
    "handler",
    "routes",
    "task_manager",
}


def _imports(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


def _is_uow_context(node: ast.withitem) -> bool:
    expression = node.context_expr
    return (
        isinstance(expression, ast.Call)
        and isinstance(expression.func, ast.Attribute)
        and expression.func.attr == "_unit_of_work"
        and isinstance(expression.func.value, ast.Name)
        and expression.func.value.id == "self"
    )


def _is_uow_execute(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return False
    if node.func.attr != "execute":
        return False
    session = node.func.value
    return (
        isinstance(session, ast.Attribute)
        and session.attr == "session"
        and isinstance(session.value, ast.Name)
        and session.value.id == "uow"
    )


def _is_uow_commit(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "commit"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "uow"
    )


def test_subscription_watchlist_keeps_sql_and_legacy_owners_outside_p7() -> None:
    for source_path in SUBSCRIPTION_SOURCES:
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(source_path))
        imports = _imports(tree)
        assert "sqlalchemy" not in imports
        assert not any(module.split(".")[0] in LEGACY_IMPORTS for module in imports)
        assert "media_metadata" not in source
        assert not any(
            isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "text"
            for node in ast.walk(tree)
        )

    use_case_source = SUBSCRIPTION_SOURCES[0].read_text(encoding="utf-8")
    assert "enqueue_active_backfill" not in use_case_source
    assert "PersistentIntroDetectionGateway" not in use_case_source


def test_p5_media_owner_contains_projection_sql_without_p7_reverse_dependency() -> None:
    source = MEDIA_OWNER.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(MEDIA_OWNER))
    imports = _imports(tree)
    assert "media_metadata" in source
    assert "sqlalchemy" in imports
    assert not any(module.startswith("etk.domains.subscription") for module in imports)
    assert any(
        isinstance(node, ast.ClassDef) and node.name == "SqlAlchemyWatchlistMediaProjectionRepository"
        for node in tree.body
    )
    assert "list_missing_old_seasons" in source
    assert "series_max_season" in source


def test_old_season_backfill_task_has_native_processed_execution_ownership() -> None:
    manifest = json.loads((REPO_ROOT / "verification/contracts/task-ownership.yaml").read_text(encoding="utf-8"))
    task = next(entry for entry in manifest["tasks"] if entry["task_key"] == "scan_old_seasons_backfill")

    assert task["status"] == "new"
    assert task["new_owner"] == "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter"
    assert task["ownership_layers"] == {
        "envelope_owner": "etk.processes.p7_subscription_tasks.P7SubscriptionTaskRouter",
        "execution_kind": "native-use-case",
        "execution_owner": "etk.domains.subscription.watchlist.OldSeasonBackfillService",
    }
    assert "processor-free" in task["notes"]


def test_all_projection_mutations_use_one_explicit_unit_of_work() -> None:
    tree = ast.parse(MEDIA_OWNER.read_text(encoding="utf-8"), filename=str(MEDIA_OWNER))
    repository = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "SqlAlchemyWatchlistMediaProjectionRepository"
    )
    methods = {node.name: node for node in repository.body if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)}
    assert MUTATION_METHODS <= methods.keys()

    for method_name in MUTATION_METHODS:
        method = methods[method_name]
        uow_blocks = [
            node
            for node in ast.walk(method)
            if isinstance(node, ast.With) and any(_is_uow_context(item) for item in node.items)
        ]
        assert len(uow_blocks) == 1, method_name
        uow_nodes = set(ast.walk(uow_blocks[0]))
        execute_calls = [node for node in ast.walk(method) if _is_uow_execute(node)]
        commit_calls = [node for node in ast.walk(method) if _is_uow_commit(node)]
        if method_name == "mark_old_seasons_wanted":
            delegated_calls = [
                node
                for node in ast.walk(method)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "set_status"
            ]
            assert len(delegated_calls) == 1 and delegated_calls[0] in uow_nodes
        else:
            assert execute_calls, method_name
            assert all(node in uow_nodes for node in execute_calls), method_name
        assert len(commit_calls) == 1 and commit_calls[0] in uow_nodes, method_name

    for method_name in {"remove_watchlist_item", "batch_remove_watchlist_items"}:
        method = methods[method_name]
        execute_calls = [node for node in ast.walk(method) if _is_uow_execute(node)]
        assert len(execute_calls) == 1
        method_source = ast.get_source_segment(
            MEDIA_OWNER.read_text(encoding="utf-8"),
            method,
        )
        assert method_source is not None
        assert "item_type = 'Series'" in method_source
        assert "parent_series_tmdb_id" in method_source
        assert "subscription_sources_json = '[]'::jsonb" in method_source
