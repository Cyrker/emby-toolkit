from __future__ import annotations

import importlib
import os
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Protocol

from flask import Blueprint, Flask, send_from_directory
from starlette.middleware.wsgi import WSGIMiddleware
from starlette.types import Receive, Scope, Send

from .session_codec import configure_legacy_session

REPO_ROOT = Path(__file__).resolve().parents[3]

LEGACY_BLUEPRINTS: tuple[tuple[str, str], ...] = (
    ("routes.watchlist", "watchlist_bp"),
    ("routes.tmdb_collections", "collections_bp"),
    ("routes.custom_collections", "custom_collections_bp"),
    ("routes.actor_subscriptions", "actor_subscriptions_bp"),
    ("routes.logs", "logs_bp"),
    ("routes.database_admin", "db_admin_bp"),
    ("routes.system", "system_bp"),
    ("routes.media", "media_api_bp"),
    ("routes.media", "media_proxy_bp"),
    ("routes.actions", "actions_bp"),
    ("routes.cover_generator_config", "cover_generator_config_bp"),
    ("routes.tasks", "tasks_bp"),
    ("routes.resubscribe", "resubscribe_bp"),
    ("routes.media_cleanup", "media_cleanup_bp"),
    ("routes.user_management", "user_management_bp"),
    ("routes.webhook", "webhook_bp"),
    ("routes.unified_auth", "unified_auth_bp"),
    ("routes.user_portal", "user_portal_bp"),
    ("routes.discover", "discover_bp"),
    ("routes.p115", "p115_bp"),
    ("routes.subscription", "subscription_bp"),
    ("routes.shared_resource", "shared_resource_bp"),
)


class RuntimeSupervisorLike(Protocol):
    def save_config_and_reload(self, new_config: dict[str, Any]) -> None: ...


def _default_data_dir() -> Path:
    configured = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
    return Path(configured) if configured else REPO_ROOT / "local_data"


def _default_static_dir() -> Path:
    configured = os.environ.get("ETK_STATIC_DIR")
    if configured:
        return Path(configured)
    container_static = REPO_ROOT / "static"
    if (container_static / "index.html").is_file():
        return container_static
    return REPO_ROOT / "frontend" / "dist"


def _load_blueprints(specs: Sequence[tuple[str, str]]) -> list[Blueprint]:
    blueprints: list[Blueprint] = []
    for module_name, attribute in specs:
        module = importlib.import_module(module_name)
        blueprint = getattr(module, attribute)
        if not isinstance(blueprint, Blueprint):
            raise TypeError(f"{module_name}:{attribute} is not a Flask Blueprint")
        blueprints.append(blueprint)
    return blueprints


def create_flask_app(
    *,
    static_dir: Path | None = None,
    data_dir: Path | None = None,
    secret_key: bytes | None = None,
    supervisor: RuntimeSupervisorLike | None = None,
    blueprint_specs: Sequence[tuple[str, str]] = LEGACY_BLUEPRINTS,
) -> Flask:
    resolved_static = (static_dir or _default_static_dir()).resolve()
    app = Flask(
        "etk.compatibility",
        static_folder=str(resolved_static),
        static_url_path="/static",
    )
    configure_legacy_session(
        app,
        (data_dir or _default_data_dir()).resolve(),
        secret_key=secret_key,
    )
    app.config["ETK_COMPATIBILITY_HOST"] = True
    if supervisor is not None:
        app.extensions["etk_runtime_supervisor"] = supervisor

    for blueprint in _load_blueprints(blueprint_specs):
        app.register_blueprint(blueprint)

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def serve(path: str):
        candidate = resolved_static / path
        if path and candidate.is_file():
            return send_from_directory(resolved_static, path)
        return send_from_directory(resolved_static, "index.html")

    return app


class LazyCompatibilityApplication:
    """Delay legacy imports until a request actually reaches the fallback mount."""

    def __init__(self, factory: Callable[[], Flask]):
        self._factory = factory
        self._lock = threading.Lock()
        self._flask_app: Flask | None = None
        self._asgi_app: WSGIMiddleware | None = None

    @property
    def flask_app(self) -> Flask | None:
        return self._flask_app

    def materialize(self) -> Flask:
        if self._flask_app is None:
            with self._lock:
                if self._flask_app is None:
                    self._flask_app = self._factory()
                    self._asgi_app = WSGIMiddleware(self._flask_app)
        return self._flask_app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.materialize()
        assert self._asgi_app is not None
        await self._asgi_app(scope, receive, send)
