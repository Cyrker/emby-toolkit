from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings


class HttpxSecondaryEmbyAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def configured_libraries(self) -> Sequence[Mapping[str, Any]]:
        settings = self._settings()
        user_id = settings.emby_user_id.strip()
        token = settings.emby_api_key.get_secret_value().strip()
        if not settings.emby_server_url.strip() or not user_id or not token:
            raise RuntimeError("Emby配置不完整或服务未就绪")
        payload = self._get_views(user_id, token, {})
        items = payload.get("Items")
        if not isinstance(items, list):
            raise RuntimeError("Emby library query returned an invalid payload")
        return tuple(dict(item) for item in items if isinstance(item, Mapping))

    def user_views(
        self,
        user_id: str,
        token: str,
        query: Mapping[str, str],
    ) -> Mapping[str, Any]:
        return self._get_views(user_id, token, query)

    def _get_views(
        self,
        user_id: str,
        token: str,
        query: Mapping[str, str],
    ) -> dict[str, Any]:
        base_url = self._settings().emby_server_url.strip().rstrip("/")
        if not base_url:
            raise RuntimeError("Emby配置不完整或服务未就绪")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Users/{quote(user_id, safe='')}/Views",
                    headers={
                        "Accept": "application/json",
                        "X-Emby-Token": token,
                    },
                    params=dict(query),
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby views query failed") from exc
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby views query returned an invalid payload")
        return dict(payload)

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout), trust_env=False)


__all__ = ["HttpxSecondaryEmbyAdapter"]
