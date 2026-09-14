from __future__ import annotations

import os
from collections.abc import Callable, Mapping, Sequence
from urllib.parse import quote

import httpx

_CONFIRMED_STATUS_CODES = frozenset({200, 204})
_MAX_PARENT_LEVELS = 12
_TIMEOUT_SECONDS = 10.0


class EmbyPathChangeRejected(RuntimeError):
    """The Emby endpoint deterministically rejected the configured request."""


class HttpxEmbyPathChangeGateway:
    """Single-attempt Emby path-change delivery with explicit confirmation."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._client_factory = client_factory or self._default_client

    def notify(
        self,
        paths: Sequence[str],
        *,
        base_url: str,
        api_key: str,
        update_type: str,
    ) -> bool:
        normalized_paths = tuple(str(path or "") for path in paths)
        if not normalized_paths:
            return True

        normalized_base_url = str(base_url or "").strip().rstrip("/")
        normalized_api_key = str(api_key or "").strip()
        normalized_update_type = str(update_type or "").strip().title()
        if (
            not normalized_base_url
            or not normalized_api_key
            or normalized_update_type not in {"Modified", "Deleted"}
            or any(not path.strip() for path in normalized_paths)
        ):
            raise EmbyPathChangeRejected("Emby path-change request configuration is invalid")

        with self._client_factory() as client:
            if normalized_update_type == "Deleted":
                return self._notify_deleted(
                    client,
                    normalized_paths,
                    base_url=normalized_base_url,
                    api_key=normalized_api_key,
                )
            rejection: EmbyPathChangeRejected | None = None
            indeterminate: Exception | None = None
            unconfirmed = False
            for path in normalized_paths:
                try:
                    path_confirmed = self._notify_modified(
                        client,
                        path,
                        base_url=normalized_base_url,
                        api_key=normalized_api_key,
                    )
                except EmbyPathChangeRejected as exc:
                    rejection = rejection or exc
                except Exception as exc:
                    indeterminate = indeterminate or exc
                else:
                    unconfirmed = unconfirmed or not path_confirmed
            if indeterminate is not None:
                raise indeterminate
            if unconfirmed:
                return False
            if rejection is not None:
                raise rejection
            return True

    @staticmethod
    def _notify_deleted(
        client: httpx.Client,
        paths: Sequence[str],
        *,
        base_url: str,
        api_key: str,
    ) -> bool:
        response = client.post(
            f"{base_url}/Library/Media/Updated",
            params={"api_key": api_key},
            json={"Updates": [{"Path": path, "UpdateType": "Deleted"} for path in paths]},
        )
        return _response_is_confirmed(response)

    @staticmethod
    def _notify_modified(
        client: httpx.Client,
        file_path: str,
        *,
        base_url: str,
        api_key: str,
    ) -> bool:
        current_path = os.path.dirname(file_path)
        for _ in range(_MAX_PARENT_LEVELS):
            if not current_path or current_path in {"/", "\\"}:
                break

            parent_path = os.path.dirname(current_path)
            if not os.path.isdir(current_path):
                current_path = parent_path
                continue

            response = client.get(
                f"{base_url}/Items",
                params={
                    "api_key": api_key,
                    "Recursive": "true",
                    "Path": current_path,
                    "Fields": "Id,Path,Name",
                },
            )
            if response.status_code >= 500:
                response.raise_for_status()
            if response.status_code != 200:
                _raise_if_rejected(response)
                return False

            expected_path = _normalize_media_path(current_path)
            matched_item = next(
                (
                    item
                    for item in _response_items(response)
                    if _normalize_media_path(item.get("Path")) == expected_path
                ),
                None,
            )
            if matched_item is not None:
                item_id = str(matched_item.get("Id") or "").strip()
                if not item_id:
                    return False
                refresh_response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}/Refresh",
                    params={
                        "api_key": api_key,
                        "Recursive": "true",
                        "MetadataRefreshMode": "Default",
                        "ImageRefreshMode": "Default",
                        "ReplaceAllImages": "false",
                        "ReplaceAllMetadata": "false",
                    },
                )
                return _response_is_confirmed(refresh_response)

            current_path = parent_path

        raise EmbyPathChangeRejected("Emby library has no matching parent for the modified path")

    @staticmethod
    def _default_client() -> httpx.Client:
        return httpx.Client(timeout=_TIMEOUT_SECONDS, trust_env=False)


def _response_is_confirmed(response: httpx.Response) -> bool:
    if response.status_code >= 500:
        response.raise_for_status()
    _raise_if_rejected(response)
    return response.status_code in _CONFIRMED_STATUS_CODES


def _raise_if_rejected(response: httpx.Response) -> None:
    if 300 <= response.status_code < 500:
        raise EmbyPathChangeRejected(f"Emby path-change request was rejected with HTTP {response.status_code}")


def _response_items(response: httpx.Response) -> list[Mapping[str, object]]:
    payload = response.json()
    if not isinstance(payload, Mapping):
        raise ValueError("Emby Items response must be a JSON object")
    raw_items = payload.get("Items")
    if not isinstance(raw_items, list):
        raise ValueError("Emby Items response must contain an Items array")
    if any(not isinstance(item, Mapping) for item in raw_items):
        raise ValueError("Emby Items response entries must be JSON objects")
    return list(raw_items)


def _normalize_media_path(path: object) -> str:
    return str(path or "").strip().replace("\\", "/").rstrip("/")
