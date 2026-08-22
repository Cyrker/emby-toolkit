from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LegacyProcessors:
    media: Any = None
    watchlist: Any = None
    actor: Any = None


_REGISTRY_LOCK = threading.RLock()


def install_legacy_registry(
    processors: LegacyProcessors,
    *,
    emby_server_id: str | None = None,
    task_registry: dict[str, Any] | None = None,
) -> None:
    import extensions

    with _REGISTRY_LOCK:
        extensions.media_processor_instance = processors.media
        extensions.watchlist_processor_instance = processors.watchlist
        extensions.actor_subscription_processor_instance = processors.actor
        extensions.EMBY_SERVER_ID = emby_server_id
        extensions.TASK_REGISTRY = dict(task_registry or {})


def clear_legacy_registry() -> None:
    import extensions

    with _REGISTRY_LOCK:
        extensions.media_processor_instance = None
        extensions.watchlist_processor_instance = None
        extensions.actor_subscription_processor_instance = None
        extensions.EMBY_SERVER_ID = None
        extensions.TASK_REGISTRY = {}
