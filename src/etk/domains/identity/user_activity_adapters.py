from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from etk.compatibility.persistent_runtime_state import PersistentRuntimeStateStore
from etk.platform import Settings


class HttpxEmbyUserActivityAdapter:
    """Authoritative Emby reads used by the P8 user event ingress and effects."""

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def series_id_for_episode(self, user_id: str, episode_id: str) -> str | None:
        payload = self._get(
            f"/Users/{quote(_required_text(user_id, 'user_id'), safe='')}/Items/"
            f"{quote(_required_text(episode_id, 'episode_id'), safe='')}",
            params={"Fields": "Type,SeriesId"},
            allow_not_found=True,
        )
        if payload is None:
            return None
        series_id = str(payload.get("SeriesId") or "").strip()
        return series_id or None

    def get_user(self, user_id: str) -> Mapping[str, Any] | None:
        return self._get(
            f"/Users/{quote(_required_text(user_id, 'user_id'), safe='')}",
            allow_not_found=True,
        )

    def _get(
        self,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        allow_not_found: bool = False,
    ) -> dict[str, Any] | None:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby user activity adapter is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                    params=dict(params or {}),
                )
                if response.status_code == 404 and allow_not_found:
                    return None
                if response.status_code != 200:
                    raise RuntimeError(f"Emby user activity read returned HTTP {response.status_code}")
                payload: Any = response.json()
        except RuntimeError:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby user activity read is unavailable") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby user activity read returned an invalid payload")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=max(1.0, float(self._settings().emby_api_timeout)))


class PersistentPolicyPropagationGuard:
    def __init__(self, store: PersistentRuntimeStateStore) -> None:
        self._store = store

    def consume(self, user_id: str, event_key: str) -> bool:
        del event_key
        return self._store.consume_ttl_marker(
            "emby-user-policy-update",
            _required_text(user_id, "user_id"),
            10,
        )


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    return normalized


__all__ = ["HttpxEmbyUserActivityAdapter", "PersistentPolicyPropagationGuard"]
