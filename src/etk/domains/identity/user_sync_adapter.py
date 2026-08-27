from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings


class HttpxEmbyUserSyncAdapter:
    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def list_users(self) -> Sequence[Mapping[str, Any]]:
        payload = self._get_json("/Users")
        if not isinstance(payload, list):
            raise RuntimeError("Emby user list returned an invalid payload")
        return tuple(dict(user) for user in payload if isinstance(user, Mapping))

    def get_user(self, user_id: str) -> Mapping[str, Any] | None:
        payload = self._get_json(f"/Users/{quote(str(user_id), safe='')}", allow_not_found=True)
        if payload is None:
            return None
        if not isinstance(payload, Mapping):
            raise RuntimeError("Emby user detail returned an invalid payload")
        return dict(payload)

    def user_items(self, user_id: str) -> Sequence[Mapping[str, Any]]:
        payload = self._get_json(
            f"/Users/{quote(str(user_id), safe='')}/Items",
            params={
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Series,Episode",
                "Fields": "UserData,SeriesId",
            },
        )
        if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
            raise RuntimeError("Emby user items returned an invalid payload")
        return tuple(dict(item) for item in payload["Items"] if isinstance(item, Mapping))

    def _get_json(
        self,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        allow_not_found: bool = False,
    ) -> Any:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not token:
            raise RuntimeError("Emby is not configured")
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}{path}",
                    headers={"X-Emby-Token": token, "Accept": "application/json"},
                    params=dict(params or {}),
                )
                if allow_not_found and response.status_code == 404:
                    return None
                if response.status_code != 200:
                    raise RuntimeError("Emby user sync query was rejected")
                return response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby user sync query is unavailable") from exc

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=float(self._settings().emby_api_timeout))


__all__ = ["HttpxEmbyUserSyncAdapter"]
