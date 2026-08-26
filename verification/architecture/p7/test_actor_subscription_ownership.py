from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
ACTOR_MODULE = "src/etk/domains/subscription/actor.py"
PROJECTION_MODULE = "src/etk/domains/media/actor_subscription_projection.py"
FORBIDDEN_LEGACY_IMPORTS = {
    "database.actor_db",
    "handler.tmdb",
    "routes.actor_subscriptions",
    "tasks.actors",
    "web_app",
}


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def _imports(source: str) -> set[str]:
    tree = ast.parse(source)
    modules = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.module is None:
            continue
        modules.add(node.module)
        modules.update(f"{node.module}.{alias.name}" for alias in node.names)
    return modules


def test_actor_application_core_uses_projection_and_cancellation_staging_ports() -> None:
    source = _source(ACTOR_MODULE)

    assert "media_metadata" not in source
    assert "class ActorMediaProjectionPort(Protocol):" in source
    assert "class ActorCancellationIntentStagePort(Protocol):" in source
    assert "class ActorSubscriptionCancellation:" in source
    assert "ActorDeletePlan" not in source
    assert "self._media_projection.remove_subscription(uow, subscription_id)" in source
    assert "self._cancellation_intent_stager.stage(" in source
    assert "self._repository.delete_subscription(subscription_id)" in source
    assert ".cancel(" not in source
    assert "for cancellation in plan.cancellations:" not in source


def test_actor_projection_owns_media_queries_and_avoids_legacy_runtime_imports() -> None:
    actor_source = _source(ACTOR_MODULE)
    projection_source = _source(PROJECTION_MODULE)

    assert "media_metadata" in projection_source
    assert "p.item_type = 'Series'" in projection_source
    assert "etk.domains.subscription" not in projection_source
    assert _imports(actor_source).isdisjoint(FORBIDDEN_LEGACY_IMPORTS)
    assert _imports(projection_source).isdisjoint(FORBIDDEN_LEGACY_IMPORTS)
