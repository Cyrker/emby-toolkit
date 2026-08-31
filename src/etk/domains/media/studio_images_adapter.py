from __future__ import annotations

import base64
import warnings
from collections.abc import Callable, Iterable, Mapping, Sequence
from io import BytesIO
from typing import Any, cast
from urllib.parse import quote

import httpx
from PIL import Image, ImageOps

from etk.platform import Settings

from .effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
    PersistentMediaEffectExecutor,
)
from .effects import MediaEffectProjection
from .studio_images import STUDIO_IMAGE_EFFECT_TYPE

_MAX_IMAGE_BYTES = 8 * 1024 * 1024
_MAX_IMAGE_DIMENSION = 8_192
_MAX_IMAGE_PIXELS = 16_000_000
_TRANSIENT_HTTP_STATUSES = frozenset({408, 425, 429})


class _ProviderQueryError(RuntimeError):
    pass


class _ProviderQueryNotFound(_ProviderQueryError):
    pass


class _ProviderQueryRejected(_ProviderQueryError):
    pass


class _ProviderQueryRetryable(_ProviderQueryError):
    pass


class _ProviderQueryInvalid(_ProviderQueryError):
    pass


def studio_image_claim_lease_seconds(provider_timeout_seconds: float) -> float:
    timeout = max(float(provider_timeout_seconds), 1.0)
    return max(90.0, 6 * timeout + 30.0)


class HttpxStudioImageAdapter:
    effect_types = frozenset({STUDIO_IMAGE_EFFECT_TYPE})

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client

    def list_studios(self) -> Sequence[Mapping[str, Any]]:
        _settings, base_url, token, user_id = self._identity()
        results: list[dict[str, Any]] = []
        start = 0
        while True:
            payload = self._get_json(
                f"{base_url}/Users/{quote(user_id, safe='')}/Items",
                token=token,
                params={
                    "Recursive": "true",
                    "IncludeItemTypes": "Studio",
                    "Fields": "Id,Name,ImageTags,BackdropImageTags,PrimaryImageAspectRatio",
                    "StartIndex": str(start),
                    "Limit": "500",
                },
            )
            if not isinstance(payload, Mapping) or not isinstance(payload.get("Items"), list):
                raise RuntimeError("Emby studio query returned an invalid payload")
            items = [dict(item) for item in payload["Items"] if isinstance(item, Mapping)]
            results.extend(items)
            if len(items) < 500:
                break
            start += len(items)
            if start > 100_000:
                raise RuntimeError("Emby studio query exceeded the bounded result limit")
        return tuple(results)

    def resolve_logo(self, mapping: Mapping[str, Any]) -> tuple[str, str] | None:
        for kind, key in (("network", "network_ids"), ("company", "company_ids")):
            identifiers = mapping.get(key)
            if not isinstance(identifiers, list):
                continue
            for value in identifiers:
                identifier = str(value or "").strip()
                if not identifier.isdigit():
                    continue
                try:
                    payload = self._tmdb_json(f"/{kind}/{identifier}")
                except _ProviderQueryNotFound:
                    continue
                logo_path = _logo_path(payload.get("logo_path"))
                if logo_path:
                    return logo_path, f"{kind}:{identifier}"
        return None

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        item_id = str(effect.payload.get("item_id") or "").strip()
        logo_path = _logo_path(effect.payload.get("logo_path"))
        if not item_id:
            raise MediaEffectDeliveryRejected("studio-image effect item identity is invalid")
        _settings, base_url, token, user_id = self._identity()
        try:
            tags = self._image_tags(base_url, token, user_id, item_id)
        except (_ProviderQueryNotFound, _ProviderQueryRejected, _ProviderQueryInvalid) as exc:
            raise MediaEffectDeliveryRejected(str(exc)) from exc
        except _ProviderQueryRetryable as exc:
            raise MediaEffectDeliveryRetryable(str(exc), retry_after_seconds=5) from exc
        thumb_ready = bool(tags.get("Thumb"))
        if not thumb_ready:
            if logo_path is None:
                raise MediaEffectDeliveryRejected("studio-image effect has no Thumb source")
            image_bytes, content_type = self._download_logo(logo_path)
            image_bytes, content_type = _studio_thumb(image_bytes, content_type)
            upload_error: Exception | None = None
            try:
                with self._client_factory() as client:
                    response = client.post(
                        f"{base_url}/Items/{quote(item_id, safe='')}/Images/Thumb",
                        headers={"X-Emby-Token": token, "Content-Type": content_type},
                        content=base64.b64encode(image_bytes),
                    )
                    classification = _mutation_status(response.status_code)
                    if classification == "rejected":
                        raise MediaEffectDeliveryRejected("Emby rejected the Studio Thumb upload")
                    if classification == "unknown":
                        upload_error = RuntimeError(f"Emby Studio Thumb upload returned HTTP {response.status_code}")
            except MediaEffectDeliveryRejected:
                raise
            except (httpx.HTTPError, TypeError, ValueError) as exc:
                upload_error = exc
            try:
                tags = self._image_tags(base_url, token, user_id, item_id)
            except (_ProviderQueryError, RuntimeError) as exc:
                raise MediaEffectDeliveryIndeterminate("Studio Thumb upload readback is unavailable") from exc
            thumb_ready = bool(tags.get("Thumb"))
            if not thumb_ready:
                raise MediaEffectDeliveryIndeterminate(
                    "Studio Thumb upload outcome is unknown",
                    error={"response_lost": upload_error is not None},
                )
        primary_deleted = False
        if tags.get("Primary"):
            delete_error: Exception | None = None
            try:
                with self._client_factory() as client:
                    response = client.delete(
                        f"{base_url}/Items/{quote(item_id, safe='')}/Images/Primary",
                        headers={"X-Emby-Token": token, "Accept": "application/json"},
                    )
                    classification = _mutation_status(response.status_code, not_found_is_success=True)
                    if classification == "rejected":
                        raise MediaEffectDeliveryRejected("Emby rejected the Studio Primary cleanup")
                    if classification == "unknown":
                        delete_error = RuntimeError(f"Emby Studio Primary cleanup returned HTTP {response.status_code}")
            except MediaEffectDeliveryRejected:
                raise
            except (httpx.HTTPError, TypeError, ValueError) as exc:
                delete_error = exc
            try:
                tags = self._image_tags(base_url, token, user_id, item_id)
            except (_ProviderQueryError, RuntimeError) as exc:
                raise MediaEffectDeliveryIndeterminate("Studio Primary cleanup readback is unavailable") from exc
            if tags.get("Primary"):
                raise MediaEffectDeliveryIndeterminate(
                    "Studio Primary cleanup outcome is unknown",
                    error={"response_lost": delete_error is not None},
                )
            primary_deleted = True
        return {
            "item_id": item_id,
            "thumb_confirmed": True,
            "primary_deleted": primary_deleted,
            "logo_source": str(effect.payload.get("logo_source") or "existing-thumb"),
        }

    def _image_tags(self, base_url: str, token: str, user_id: str, item_id: str) -> Mapping[str, Any]:
        payload = self._get_json(
            f"{base_url}/Users/{quote(user_id, safe='')}/Items/{quote(item_id, safe='')}",
            token=token,
            params={"Fields": "ImageTags"},
        )
        if not isinstance(payload, Mapping) or str(payload.get("Id") or "").strip() != item_id:
            raise _ProviderQueryInvalid("Emby Studio readback returned an invalid item")
        tags = payload.get("ImageTags")
        image_tags = dict(tags) if isinstance(tags, Mapping) else {}
        if not image_tags.get("Thumb") and payload.get("ThumbImageTag"):
            image_tags["Thumb"] = payload["ThumbImageTag"]
        if not image_tags.get("Primary") and payload.get("PrimaryImageTag"):
            image_tags["Primary"] = payload["PrimaryImageTag"]
        if isinstance(tags, Mapping) or image_tags:
            return image_tags
        raise _ProviderQueryInvalid("Emby Studio readback did not contain image tags")

    def _download_logo(self, logo_path: str) -> tuple[bytes, str]:
        try:
            with self._client_factory() as client:
                with client.stream(
                    "GET",
                    f"https://image.tmdb.org/t/p/original{logo_path}",
                    headers={"Accept": "image/*"},
                ) as response:
                    if response.status_code == 404:
                        raise MediaEffectDeliveryRejected("TMDb Studio logo was not found")
                    if _is_transient_status(response.status_code):
                        raise MediaEffectDeliveryRetryable(
                            "TMDb Studio logo download is unavailable", retry_after_seconds=5
                        )
                    if 400 <= response.status_code < 500:
                        raise MediaEffectDeliveryRejected("TMDb rejected the Studio logo download")
                    content_length = response.headers.get("content-length")
                    if content_length and int(content_length) > _MAX_IMAGE_BYTES:
                        raise MediaEffectDeliveryRejected("TMDb Studio logo is too large")
                    chunks: list[bytes] = []
                    total = 0
                    for chunk in response.iter_bytes():
                        total += len(chunk)
                        if total > _MAX_IMAGE_BYTES:
                            raise MediaEffectDeliveryRejected("TMDb Studio logo is too large")
                        chunks.append(chunk)
                    content = b"".join(chunks)
                    content_type = str(response.headers.get("content-type") or "image/png").partition(";")[0]
        except (MediaEffectDeliveryRejected, MediaEffectDeliveryRetryable):
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise MediaEffectDeliveryRetryable(
                "TMDb Studio logo download is unavailable", retry_after_seconds=5
            ) from exc
        if not content:
            raise MediaEffectDeliveryRetryable("TMDb Studio logo download was empty", retry_after_seconds=5)
        return content, content_type

    def _tmdb_json(self, path: str) -> Mapping[str, Any]:
        settings = self._settings()
        api_key = settings.tmdb_api_key.get_secret_value().strip()
        base_url = settings.tmdb_api_base_url.strip().rstrip("/")
        if not api_key or not base_url:
            raise RuntimeError("TMDb is not configured")
        payload = self._get_json(
            f"{base_url}{path}",
            token=None,
            params={"api_key": api_key, "language": "zh-CN"},
        )
        if not isinstance(payload, Mapping):
            raise RuntimeError("TMDb Studio query returned an invalid payload")
        return dict(payload)

    def _get_json(
        self,
        url: str,
        *,
        token: str | None,
        params: Mapping[str, str] | None = None,
    ) -> Any:
        headers = {"Accept": "application/json"}
        if token:
            headers["X-Emby-Token"] = token
        try:
            with self._client_factory() as client:
                response = client.get(url, headers=headers, params=dict(params or {}))
        except httpx.HTTPError as exc:
            raise _ProviderQueryRetryable("Studio image provider query is unavailable") from exc
        if response.status_code == 404:
            raise _ProviderQueryNotFound("Studio image provider target was not found")
        if _is_transient_status(response.status_code):
            raise _ProviderQueryRetryable("Studio image provider query is unavailable")
        if 400 <= response.status_code < 500:
            raise _ProviderQueryRejected("Studio image provider rejected the query")
        try:
            return response.json()
        except (TypeError, ValueError) as exc:
            raise _ProviderQueryInvalid("Studio image provider returned invalid JSON") from exc

    def _identity(self) -> tuple[Settings, str, str, str]:
        settings = self._settings()
        base_url = settings.emby_server_url.strip().rstrip("/")
        token = settings.emby_api_key.get_secret_value().strip()
        user_id = settings.emby_user_id.strip()
        if not base_url or not token or not user_id:
            raise RuntimeError("Emby is not configured")
        return settings, base_url, token, user_id

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        timeout = max(float(settings.emby_api_timeout), 1.0)
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(timeout, connect=min(5.0, timeout)),
            trust_env=False,
        )


class PersistentStudioImageEffects:
    def __init__(self, executor: PersistentMediaEffectExecutor) -> None:
        self._executor = executor

    def reconcile(
        self,
        *,
        effect_key: str,
        item_id: str,
        logo_path: str | None,
        logo_source: str | None,
    ) -> MediaEffectProjection:
        return self._executor.reserve_and_execute(
            effect_key=effect_key,
            effect_type=STUDIO_IMAGE_EFFECT_TYPE,
            aggregate_type="emby_studio",
            aggregate_id=item_id,
            payload={
                "item_id": item_id,
                "logo_path": logo_path,
            },
        )


def _logo_path(value: object) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if not normalized.startswith("/") or ".." in normalized or "://" in normalized:
        raise ValueError("TMDb Studio logo path is invalid")
    return normalized


def _studio_thumb(image_bytes: bytes, content_type: str) -> tuple[bytes, str]:
    if content_type.casefold() in {"image/svg+xml", "image/svg"}:
        raise MediaEffectDeliveryRejected("SVG Studio logos are not accepted")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(image_bytes)) as original:
                width, height = original.size
                if (
                    width < 1
                    or height < 1
                    or width > _MAX_IMAGE_DIMENSION
                    or height > _MAX_IMAGE_DIMENSION
                    or width * height > _MAX_IMAGE_PIXELS
                ):
                    raise MediaEffectDeliveryRejected("TMDb Studio logo dimensions exceed the limit")
                source = ImageOps.exif_transpose(original).convert("RGBA")
            alpha = source.getchannel("A")
            bounding_box = alpha.getbbox()
            if bounding_box:
                source = source.crop(bounding_box)
            luminance_sum = 0.0
            weight_sum = 0.0
            pixels = cast(Iterable[tuple[int, int, int, int]], source.get_flattened_data())
            for red, green, blue, opacity in pixels:
                if opacity <= 8:
                    continue
                weight = opacity / 255.0
                luminance_sum += (0.2126 * red + 0.7152 * green + 0.0722 * blue) * weight
                weight_sum += weight
            background = (238, 238, 238, 255) if luminance_sum / max(weight_sum, 1) < 105 else (45, 45, 45, 255)
            max_width, max_height = int(1066 * 0.78), int(600 * 0.56)
            scale = min(max_width / max(source.width, 1), max_height / max(source.height, 1))
            resized = source.resize(
                (max(1, int(source.width * scale)), max(1, int(source.height * scale))),
                Image.Resampling.LANCZOS,
            )
            canvas = Image.new("RGBA", (1066, 600), background)
            canvas.alpha_composite(resized, ((1066 - resized.width) // 2, (600 - resized.height) // 2))
            output = BytesIO()
            canvas.save(output, format="PNG", optimize=True)
            return output.getvalue(), "image/png"
    except MediaEffectDeliveryRejected:
        raise
    except Exception as exc:
        raise MediaEffectDeliveryRejected("TMDb Studio logo cannot be decoded") from exc


def _is_transient_status(status_code: int) -> bool:
    return status_code in _TRANSIENT_HTTP_STATUSES or status_code >= 500 or 300 <= status_code < 400


def _mutation_status(status_code: int, *, not_found_is_success: bool = False) -> str | None:
    if 200 <= status_code < 300 or not_found_is_success and status_code == 404:
        return None
    if 400 <= status_code < 500 and status_code not in _TRANSIENT_HTTP_STATUSES:
        return "rejected"
    return "unknown"


__all__ = [
    "HttpxStudioImageAdapter",
    "PersistentStudioImageEffects",
    "studio_image_claim_lease_seconds",
]
