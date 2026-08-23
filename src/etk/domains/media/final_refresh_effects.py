from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

import httpx

from etk.platform import Settings
from etk.platform.jobs.json_payload import canonical_json

from .bridge_client import emby_authorization_revision
from .effect_execution import MediaEffectDeliveryIndeterminate, MediaEffectDeliveryRejected
from .effects import (
    MediaEffectDbApiCursor,
    MediaEffectIntentReservation,
    MediaEffectProjection,
    reserve_media_effect_dbapi,
)

FINAL_METADATA_REFRESH_EFFECT = "emby.metadata.final_refresh"
_SETTINGS_LOCK = threading.RLock()
_SETTINGS_PROVIDER: Callable[[], Settings] | None = None


def install_final_refresh_settings_provider(provider: Callable[[], Settings]) -> None:
    global _SETTINGS_PROVIDER
    with _SETTINGS_LOCK:
        if _SETTINGS_PROVIDER is not None and _SETTINGS_PROVIDER is not provider:
            raise RuntimeError("a final refresh settings provider is already installed")
        _SETTINGS_PROVIDER = provider


def clear_final_refresh_settings_provider(provider: Callable[[], Settings]) -> None:
    global _SETTINGS_PROVIDER
    with _SETTINGS_LOCK:
        if _SETTINGS_PROVIDER is provider:
            _SETTINGS_PROVIDER = None


def stage_final_metadata_refresh(
    cursor: MediaEffectDbApiCursor,
    *,
    operation_id: str,
    item_id: str,
    item_type: str,
    replace_all_metadata: bool,
    replace_all_images: bool,
    metadata_only: bool,
    authorization_revision: str | None = None,
) -> MediaEffectIntentReservation:
    normalized_item_id = str(item_id or "").strip()
    normalized_item_type = str(item_type or "").strip().title()
    if not normalized_item_id:
        raise ValueError("final metadata refresh requires an Emby item id")
    if normalized_item_type not in {"Movie", "Series", "Season", "Episode"}:
        raise ValueError("final metadata refresh requires a supported item type")
    revision = _authorization_revision(
        authorization_revision if authorization_revision is not None else _current_authorization_revision()
    )
    payload = {
        "item_id": normalized_item_id,
        "item_type": normalized_item_type,
        "emby_authorization_revision": revision,
        "replace_all_metadata": bool(replace_all_metadata),
        "replace_all_images": bool(replace_all_images),
        "metadata_only": bool(metadata_only),
    }
    normalized_operation = str(operation_id or "").strip()
    if not normalized_operation:
        normalized_operation = hashlib.sha256(
            canonical_json(payload, field="final metadata refresh payload").encode("utf-8")
        ).hexdigest()
    effect_key = f"p5:{FINAL_METADATA_REFRESH_EFFECT}:{normalized_operation}:{normalized_item_id}:{revision}"
    return reserve_media_effect_dbapi(
        cursor,
        effect_key=effect_key,
        effect_type=FINAL_METADATA_REFRESH_EFFECT,
        aggregate_type="media_item",
        aggregate_id=normalized_item_id,
        payload=payload,
    )


class EmbyMetadataRefreshEffectDelivery:
    """Single-attempt native Emby final-refresh delivery without durable secrets."""

    effect_types = frozenset({FINAL_METADATA_REFRESH_EFFECT})

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type != FINAL_METADATA_REFRESH_EFFECT:
            raise ValueError(f"unsupported final refresh effect type: {effect.effect_type}")
        payload = effect.payload
        item_id = _required_text(payload, "item_id")
        item_type = _required_text(payload, "item_type").title()
        if item_type not in {"Movie", "Series", "Season", "Episode"}:
            raise ValueError("final metadata refresh item_type is unsupported")

        settings = self._settings()
        expected_revision = _authorization_revision(payload.get("emby_authorization_revision"))
        if emby_authorization_revision(settings) != expected_revision:
            raise MediaEffectDeliveryRejected(
                "Emby authorization changed after final refresh staging",
                error={"operation": "metadata-final-refresh", "reason": "authorization_changed"},
            )
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        user_id = str(settings.emby_user_id or "").strip()
        if not base_url or not api_key or not user_id:
            raise MediaEffectDeliveryRejected(
                "Emby metadata refresh configuration is incomplete",
                error={"operation": "metadata-final-refresh", "reason": "configuration_incomplete"},
            )
        params = {
            "api_key": api_key,
            "Recursive": str(item_type == "Series").lower(),
            "MetadataRefreshMode": "FullRefresh",
            "ImageRefreshMode": "ValidationOnly" if bool(payload.get("metadata_only")) else "FullRefresh",
            "ReplaceAllMetadata": str(bool(payload.get("replace_all_metadata"))).lower(),
            "ReplaceAllImages": str(bool(payload.get("replace_all_images"))).lower(),
        }
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(item_id, safe='')}/Refresh",
                    params=params,
                )
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise MediaEffectDeliveryIndeterminate(
                "Emby metadata refresh outcome is unknown",
                error={"operation": "metadata-final-refresh"},
            ) from exc
        if response.status_code == 204:
            return {
                "operation": "metadata-final-refresh",
                "status_code": response.status_code,
                "item_id": item_id,
            }
        if 400 <= response.status_code < 500:
            raise MediaEffectDeliveryRejected(
                f"Emby rejected metadata refresh with HTTP {response.status_code}",
                error={
                    "operation": "metadata-final-refresh",
                    "status_code": response.status_code,
                },
            )
        raise MediaEffectDeliveryIndeterminate(
            f"Emby metadata refresh returned HTTP {response.status_code}",
            error={
                "operation": "metadata-final-refresh",
                "status_code": response.status_code,
            },
        )

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(
            proxy=proxy or None,
            timeout=max(1.0, float(settings.emby_api_timeout)),
        )


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field) or "").strip()
    if not value:
        raise ValueError(f"final metadata refresh payload requires {field}")
    return value


def _authorization_revision(value: object) -> str:
    revision = str(value or "").strip().lower()
    if len(revision) != 64:
        raise ValueError("final metadata refresh authorization revision is invalid")
    try:
        bytes.fromhex(revision)
    except ValueError as exc:
        raise ValueError("final metadata refresh authorization revision is invalid") from exc
    return revision


def _current_authorization_revision() -> str:
    with _SETTINGS_LOCK:
        provider = _SETTINGS_PROVIDER
    if provider is None:
        raise RuntimeError("final refresh settings provider is not installed")
    return emby_authorization_revision(provider())


__all__ = [
    "FINAL_METADATA_REFRESH_EFFECT",
    "EmbyMetadataRefreshEffectDelivery",
    "clear_final_refresh_settings_provider",
    "install_final_refresh_settings_provider",
    "stage_final_metadata_refresh",
]
