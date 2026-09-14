"""Processor-free cover generation tasks.

The renderer is pure and deterministic; Emby reads, image downloads and image
uploads stay behind a small remote port.  A failed upload is reconciled by a
readback and is never blindly retried in the same execution.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import random
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote
from uuid import UUID

import httpx
import yaml
from sqlalchemy import text

from etk.domains.collection.renderer import DeterministicCoverRenderer, RenderedCover
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
    PersistentMediaEffectExecutor,
)
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.tasks import TaskOutcome
from etk.policy_defaults import DEFAULT_RATING_MAPPING

logger = logging.getLogger(__name__)

COVER_TASK_KEYS = frozenset({"generate-all-covers", "generate-custom-collection-covers"})
COVER_IMAGE_EFFECT_TYPE = "emby.cover.apply"
_MAX_COVER_BYTES = 8 * 1024 * 1024
_MAX_SOURCE_BYTES = 8 * 1024 * 1024
_MAX_FONT_BYTES = 16 * 1024 * 1024
_DEFAULT_COVER_CONFIG: dict[str, Any] = {
    "enabled": False,
    "exclude_libraries": [],
    "sort_by": "Latest",
    "show_item_count": False,
    "badge_style": "badge",
    "badge_size_ratio": 0.12,
    "cover_style": "single_1",
    "multi_1_blur": False,
}


class CoverTaskProgress(Protocol):
    def update(self, progress: int, message: str) -> None: ...

    def checkpoint(self) -> None: ...

    def record_started_effect(self, label: str) -> None: ...

    def record_committed_effect(self, label: str) -> None: ...

    def record_abandoned_effect(self, label: str) -> None: ...


class CoverTargetRepository(Protocol):
    def config(self) -> Mapping[str, Any]: ...

    def list_custom_targets(self) -> Sequence[Mapping[str, Any]]: ...


class CoverRemotePort(Protocol):
    def list_libraries(self) -> Sequence[Mapping[str, Any]]: ...

    def source_images(
        self,
        target: Mapping[str, Any],
        limit: int,
        *,
        config: Mapping[str, Any],
    ) -> CoverSourceSnapshot: ...


CoverBadge = int | str | None


@dataclass(frozen=True, slots=True)
class CoverSourceSnapshot:
    images: tuple[bytes, ...]
    item_count: int | None = None


class CoverTaskEffects(Protocol):
    def read(self, effect_key: str, target_id: str) -> MediaEffectProjection | None: ...

    def execute(self, effect_id: UUID) -> MediaEffectProjection: ...

    def apply(
        self,
        *,
        effect_key: str,
        target_id: str,
        target_name: str,
        content: bytes,
        cache_key: str,
        output_root: str | None,
    ) -> MediaEffectProjection: ...


class CoverRendererPort(Protocol):
    def render(
        self,
        *,
        style: str,
        sources: Sequence[bytes],
        title_zh: str,
        title_en: str,
        item_count: CoverBadge,
        blur: bool,
        seed: int,
        config: Mapping[str, Any],
    ) -> Any: ...


class NativeCoverTaskService:
    """Generate and publish deterministic covers for libraries or collections."""

    def __init__(
        self,
        repository: CoverTargetRepository,
        remote: CoverRemotePort,
        renderer: CoverRendererPort,
        effects: CoverTaskEffects,
    ) -> None:
        self._repository = repository
        self._remote = remote
        self._renderer = renderer
        self._effects = effects

    def execute(self, task_key: str, progress: CoverTaskProgress) -> TaskOutcome:
        if task_key not in COVER_TASK_KEYS:
            raise LookupError(f"unsupported cover task key: {task_key!r}")
        config = {**_DEFAULT_COVER_CONFIG, **dict(self._repository.config())}
        if not bool(config.get("enabled")):
            progress.update(100, "封面生成器未启用，任务跳过。")
            return TaskOutcome.skipped("封面生成器未启用，任务跳过。", {"targets": 0, "skipped": True})

        if task_key == "generate-all-covers":
            targets = [dict(value) for value in self._remote.list_libraries() if isinstance(value, Mapping)]
            excluded = {str(value).strip() for value in config.get("exclude_libraries", []) if str(value).strip()}
            targets = [
                target
                for target in targets
                if str(target.get("Id") or target.get("id") or "").strip() not in excluded
                and _library_is_coverable(target)
            ]
        else:
            targets = [dict(value) for value in self._repository.list_custom_targets() if isinstance(value, Mapping)]

        if not targets:
            progress.update(100, "没有需要生成封面的目标。")
            return TaskOutcome.skipped("没有需要生成封面的目标。", {"targets": 0, "updated": 0, "failed": 0})

        updated = 0
        skipped = 0
        indeterminate = 0
        failures: list[dict[str, str]] = []
        effect_ids: list[str] = []
        total = len(targets)
        for index, target in enumerate(targets, 1):
            progress.checkpoint()
            target_id = _target_id_value(target)
            target_name = str(target.get("Name") or target.get("name") or target_id).strip()
            if not target_id:
                failures.append({"target": target_name, "reason": "missing_target_id"})
                continue
            progress.update(min(95, 10 + int((index - 1) / total * 85)), f"正在生成封面 ({index}/{total})")

            stable_effect_key = _stable_effect_key(task_key, target_id, progress)
            if stable_effect_key is not None:
                try:
                    existing = self._effects.read(stable_effect_key, target_id)
                except Exception as exc:
                    logger.warning("cover effect lookup failed", extra={"target": target_id}, exc_info=True)
                    failures.append({"target": target_name, "reason": type(exc).__name__})
                    continue
                if existing is not None:
                    progress.record_started_effect(stable_effect_key)
                    try:
                        effect = self._effects.execute(existing.id)
                    except Exception as exc:
                        progress.record_abandoned_effect(stable_effect_key)
                        logger.warning("cover effect recovery failed", extra={"target": target_id}, exc_info=True)
                        failures.append({"target": target_name, "reason": type(exc).__name__})
                        continue
                    effect_ids.append(str(effect.id))
                    result = _record_effect_result(effect, progress, stable_effect_key)
                    if result == "updated":
                        updated += 1
                    elif result == "indeterminate":
                        indeterminate += 1
                    else:
                        failures.append({"target": target_name, "reason": "effect_rejected"})
                    progress.checkpoint()
                    continue

            try:
                style = str(config.get("cover_style") or "single_1").strip()
                limit = 1 if style.startswith("single") else 9
                source_snapshot = self._remote.source_images(target, limit, config=config)
                sources = source_snapshot.images
                if not sources:
                    failures.append({"target": target_name, "reason": "no_image_source"})
                    continue
                title_zh, title_en = _titles(config, target_name)
                item_count = _item_count(target, fallback=source_snapshot.item_count)
                rendered = self._renderer.render(
                    style=style,
                    sources=sources,
                    title_zh=title_zh,
                    title_en=title_en,
                    item_count=item_count,
                    blur=bool(config.get("multi_1_blur")),
                    seed=_seed(target),
                    config=config,
                )
                content = bytes(getattr(rendered, "png", rendered))
                if not content:
                    failures.append({"target": target_name, "reason": "renderer_empty"})
                    continue
                if len(content) > _MAX_COVER_BYTES:
                    failures.append({"target": target_name, "reason": "cover_too_large"})
                    continue
                cache_key = str(getattr(rendered, "cache_key", "") or "").strip()
                if not cache_key:
                    cache_key = hashlib.sha256(content).hexdigest()
            except Exception as exc:
                logger.warning("cover target rendering failed", extra={"target": target_id}, exc_info=True)
                failures.append({"target": target_name, "reason": type(exc).__name__})
                continue

            progress.checkpoint()
            effect_key = _effect_key(task_key, target_id, cache_key, progress)
            progress.record_started_effect(effect_key)
            try:
                effect = self._effects.apply(
                    effect_key=effect_key,
                    target_id=target_id,
                    target_name=target_name,
                    content=content,
                    cache_key=cache_key,
                    output_root=_optional_text(config.get("covers_output")),
                )
            except Exception as exc:
                progress.record_abandoned_effect(effect_key)
                logger.warning("cover target delivery failed", extra={"target": target_id}, exc_info=True)
                failures.append({"target": target_name, "reason": type(exc).__name__})
                continue

            effect_ids.append(str(effect.id))
            result = _record_effect_result(effect, progress, effect_key)
            if result == "updated":
                updated += 1
            elif result == "indeterminate":
                indeterminate += 1
            else:
                failures.append({"target": target_name, "reason": "effect_rejected"})
            progress.checkpoint()

        progress.update(100, "封面生成完成。" if not failures else "封面生成部分完成。")
        details = {
            "targets": total,
            "updated": updated,
            "skipped": skipped,
            "failed": len(failures),
            "indeterminate": indeterminate,
            "effect_ids": effect_ids,
            "failed_targets": failures,
        }
        if failures or indeterminate:
            return TaskOutcome.partial("封面生成部分完成。", details)
        return TaskOutcome.succeeded("封面生成完成。", details)


class PersistentCoverTaskEffects:
    """Persist generated bytes in the media-effect ledger before Emby delivery."""

    def __init__(self, executor: PersistentMediaEffectExecutor) -> None:
        self._executor = executor

    def read(self, effect_key: str, target_id: str) -> MediaEffectProjection | None:
        existing = self._executor.read(effect_key)
        if existing is None:
            return None
        normalized_id = str(target_id or "").strip()
        if (
            existing.effect_type != COVER_IMAGE_EFFECT_TYPE
            or existing.aggregate_type != "emby_cover"
            or existing.aggregate_id != normalized_id
        ):
            raise ValueError("cover effect key is already reserved for a different target")
        return existing

    def execute(self, effect_id: UUID) -> MediaEffectProjection:
        return self._executor.execute(effect_id)

    def apply(
        self,
        *,
        effect_key: str,
        target_id: str,
        target_name: str,
        content: bytes,
        cache_key: str,
        output_root: str | None,
    ) -> MediaEffectProjection:
        normalized_id = str(target_id or "").strip()
        if not normalized_id:
            raise ValueError("cover target identity is missing")
        existing = self.read(effect_key, normalized_id)
        if existing is not None:
            return self.execute(existing.id)
        if not content or len(content) > _MAX_COVER_BYTES:
            raise ValueError("cover content is empty or too large")
        digest = hashlib.sha256(content).hexdigest()
        return self._executor.reserve_and_execute(
            effect_key=effect_key,
            effect_type=COVER_IMAGE_EFFECT_TYPE,
            aggregate_type="emby_cover",
            aggregate_id=normalized_id,
            payload={
                "target_id": normalized_id,
                "target_name": str(target_name or normalized_id).strip()[:500] or normalized_id,
                "content_b64": base64.b64encode(content).decode("ascii"),
                "content_sha256": digest,
                "byte_size": len(content),
                "cache_key": str(cache_key or "").strip()[:128],
                "output_root": _optional_text(output_root),
            },
        )


class SqlAlchemyCoverTargetRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork], settings: Callable[[], Any]) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings

    def config(self) -> Mapping[str, Any]:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = 'cover_generator_config'")
            )
        return dict(value) if isinstance(value, Mapping) else {}

    def list_custom_targets(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, name, type, item_type, in_library_count,
                               emby_collection_id, definition_json, generated_media_info_json
                        FROM custom_collections
                        WHERE status = 'active' AND NULLIF(BTRIM(emby_collection_id), '') IS NOT NULL
                        ORDER BY id
                        """
                    )
                )
                .mappings()
                .all()
            )
            targets = [dict(row) for row in rows]
            tmdb_ids = tuple(
                dict.fromkeys(
                    tmdb_id
                    for target in targets
                    for tmdb_id in _generated_tmdb_ids(target.get("generated_media_info_json"))
                )
            )
            posters: dict[str, str] = {}
            if tmdb_ids:
                poster_rows = (
                    uow.session.execute(
                        text(
                            """
                            SELECT tmdb_id, poster_path
                            FROM media_metadata
                            WHERE tmdb_id = ANY(CAST(:tmdb_ids AS text[]))
                              AND NULLIF(BTRIM(poster_path), '') IS NOT NULL
                            ORDER BY tmdb_id, item_type
                            """
                        ),
                        {"tmdb_ids": list(tmdb_ids)},
                    )
                    .mappings()
                    .all()
                )
                for row in poster_rows:
                    posters.setdefault(str(row["tmdb_id"]), str(row["poster_path"]))
        for target in targets:
            target["tmdb_poster_paths"] = [
                posters[tmdb_id]
                for tmdb_id in _generated_tmdb_ids(target.get("generated_media_info_json"))
                if tmdb_id in posters
            ]
        return tuple(targets)


class ConfiguredCoverRenderer:
    """Resolve configured fonts, then delegate to the established deterministic renderer."""

    def __init__(
        self,
        *,
        default_zh_font: Path,
        default_en_font: Path,
        default_multi_zh_font: Path | None = None,
        default_multi_en_font: Path | None = None,
        cache_root: Path,
        settings: Callable[[], Settings] | None = None,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._defaults = {
            "zh": default_zh_font,
            "en": default_en_font,
            "multi_zh": default_multi_zh_font or default_zh_font,
            "multi_en": default_multi_en_font or default_en_font,
        }
        self._cache_root = cache_root.resolve()
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._renderers: dict[tuple[str, str], DeterministicCoverRenderer] = {}

    def render(
        self,
        *,
        style: str,
        sources: Sequence[bytes],
        title_zh: str,
        title_en: str,
        item_count: CoverBadge,
        blur: bool,
        seed: int,
        config: Mapping[str, Any],
    ) -> RenderedCover:
        multi = style == "multi_1" and not bool(config.get("multi_1_use_main_font"))
        if multi:
            zh_font = self._font(
                config,
                local_key="zh_font_path_multi_1_local",
                url_key="zh_font_url_multi_1",
                default_key="multi_zh",
            )
            en_font = self._font(
                config,
                local_key="en_font_path_multi_1_local",
                url_key="en_font_url_multi_1",
                default_key="multi_en",
            )
        else:
            zh_font = self._font(config, local_key="zh_font_path_local", url_key="zh_font_url", default_key="zh")
            en_font = self._font(config, local_key="en_font_path_local", url_key="en_font_url", default_key="en")
        key = (_font_identity(zh_font), _font_identity(en_font))
        renderer = self._renderers.get(key)
        if renderer is None:
            renderer = DeterministicCoverRenderer(zh_font_path=zh_font, en_font_path=en_font)
            self._renderers[key] = renderer
        return renderer.render(
            style=style,
            sources=sources,
            title_zh=title_zh,
            title_en=title_en,
            item_count=item_count,
            blur=blur,
            seed=seed,
            config=config,
        )

    def _font(
        self,
        config: Mapping[str, Any],
        *,
        local_key: str,
        url_key: str,
        default_key: str,
    ) -> Path:
        local = _optional_text(config.get(local_key))
        if local:
            path = Path(local).expanduser().resolve()
            if path.is_file():
                return path
            logger.warning("configured cover font does not exist", extra={"font_key": local_key})
        url = _optional_text(config.get(url_key))
        if url:
            return self._download_font(url, default_key)
        default = self._defaults[default_key].resolve()
        if not default.is_file():
            raise FileNotFoundError(default)
        return default

    def _download_font(self, url: str, label: str) -> Path:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()
        destination = self._cache_root / f"{label}-{digest}.font"
        if destination.is_file() and 0 < destination.stat().st_size <= _MAX_FONT_BYTES:
            return destination
        self._cache_root.mkdir(parents=True, exist_ok=True)
        try:
            with self._client_factory() as client:
                with client.stream("GET", url, headers={"Accept": "font/*,application/octet-stream"}) as response:
                    response.raise_for_status()
                    content = _bounded_response_content(response, _MAX_FONT_BYTES)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError(f"configured cover font download failed: {label}") from exc
        if not content:
            raise ValueError(f"configured cover font is empty or too large: {label}")
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(dir=self._cache_root, prefix=".font-", delete=False) as handle:
                temporary = handle.name
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if temporary and os.path.exists(temporary):
                os.unlink(temporary)
        return destination

    def _default_client(self) -> httpx.Client:
        settings = self._settings() if self._settings is not None else None
        proxy = (
            settings.network_http_proxy_url.strip() if settings is not None and settings.network_proxy_enabled else ""
        )
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(60, connect=5),
            follow_redirects=True,
            trust_env=False,
        )


class HttpxCoverRemoteAdapter:
    """Emby inventory/image adapter used by native cover tasks."""

    effect_types = frozenset({COVER_IMAGE_EFFECT_TYPE})

    def __init__(
        self,
        settings: Callable[[], Settings],
        *,
        client_factory: Callable[[], httpx.Client] | None = None,
        external_client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        self._settings = settings
        self._client_factory = client_factory or self._default_client
        self._external_client_factory = external_client_factory or client_factory or self._default_external_client

    def list_libraries(self) -> Sequence[Mapping[str, Any]]:
        base_url, _user_id, token = _emby(self._settings())
        try:
            with self._client_factory() as client:
                response = client.get(
                    f"{base_url}/Library/VirtualFolders",
                    headers={"Accept": "application/json", "X-Emby-Token": token},
                )
                response.raise_for_status()
                payload: Any = response.json()
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby library query failed") from exc
        return tuple(dict(item) for item in payload if isinstance(item, Mapping)) if isinstance(payload, list) else ()

    def source_images(
        self,
        target: Mapping[str, Any],
        limit: int,
        *,
        config: Mapping[str, Any],
    ) -> CoverSourceSnapshot:
        target_id = _target_id(target)
        base_url, user_id, token = _emby(self._settings())
        bounded_limit = max(1, min(int(limit), 9))
        local_sources = _local_cover_sources(target, bounded_limit, config)
        if local_sources:
            try:
                with self._client_factory() as client:
                    _items, total = _query_cover_items(
                        client,
                        base_url=base_url,
                        user_id=user_id,
                        token=token,
                        target=target,
                        target_id=target_id,
                        limit=bounded_limit,
                        config=config,
                    )
            except (httpx.HTTPError, TypeError, ValueError):
                total = None
            return CoverSourceSnapshot(local_sources, total)

        try:
            if _prefer_tmdb_posters(target):
                with self._external_client_factory() as client:
                    tmdb_sources = _download_tmdb_posters(
                        client,
                        target,
                        bounded_limit,
                        config=config,
                    )
                    if tmdb_sources:
                        return CoverSourceSnapshot(tmdb_sources)

            with self._client_factory() as client:
                items, total = _query_cover_items(
                    client,
                    base_url=base_url,
                    user_id=user_id,
                    token=token,
                    target=target,
                    target_id=target_id,
                    limit=bounded_limit,
                    config=config,
                )

                values: list[bytes] = []
                for item in items:
                    if not isinstance(item, Mapping) or not _rating_allowed(item, target, config):
                        continue
                    image_path = _image_path(item, config)
                    if image_path is None:
                        continue
                    content = _download_image(
                        client,
                        f"{base_url}{image_path}",
                        headers={"Accept": "image/*", "X-Emby-Token": token},
                    )
                    if content is not None:
                        values.append(content)
                    if len(values) >= bounded_limit:
                        break
                return CoverSourceSnapshot(tuple(values), total)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise RuntimeError("Emby cover source query failed") from exc

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        target_id = str(payload.get("target_id") or "").strip()
        target_name = str(payload.get("target_name") or target_id).strip()
        encoded = payload.get("content_b64")
        expected_hash = str(payload.get("content_sha256") or "").strip().casefold()
        expected_size = payload.get("byte_size")
        if not target_id or not isinstance(encoded, str) or not expected_hash:
            raise MediaEffectDeliveryRejected("cover effect payload is invalid")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError) as exc:
            raise MediaEffectDeliveryRejected("cover effect content is invalid") from exc
        if not content or len(content) > _MAX_COVER_BYTES:
            raise MediaEffectDeliveryRejected("cover effect content exceeds the size limit")
        if isinstance(expected_size, bool) or not isinstance(expected_size, int) or expected_size != len(content):
            raise MediaEffectDeliveryRejected("cover effect content size does not match")
        if hashlib.sha256(content).hexdigest() != expected_hash:
            raise MediaEffectDeliveryRejected("cover effect content hash does not match")
        output_saved = _write_cover_output(payload.get("output_root"), target_name, content)

        settings = self._settings()
        try:
            base_url, _user_id, token = _emby(settings)
        except RuntimeError as exc:
            raise MediaEffectDeliveryRejected("Emby cover authorization is not configured") from exc
        try:
            observed = self._readback_digest(base_url, token, target_id)
        except MediaEffectDeliveryRetryable:
            raise
        if observed == expected_hash:
            return {
                "target_id": target_id,
                "content_sha256": expected_hash,
                "recovered_by_readback": True,
                "output_saved": output_saved,
            }

        response_error: Exception | None = None
        try:
            with self._client_factory() as client:
                response = client.post(
                    f"{base_url}/Items/{quote(target_id, safe='')}/Images/Primary",
                    headers={"Content-Type": "image/png", "X-Emby-Token": token},
                    content=base64.b64encode(content),
                )
                if 200 <= response.status_code < 300:
                    return {
                        "target_id": target_id,
                        "content_sha256": expected_hash,
                        "provider_response_confirmed": True,
                        "output_saved": output_saved,
                    }
                elif 400 <= response.status_code < 500 and response.status_code not in {408, 425, 429}:
                    raise MediaEffectDeliveryRejected("Emby rejected the cover upload")
                else:
                    response_error = RuntimeError(f"Emby cover upload returned HTTP {response.status_code}")
        except MediaEffectDeliveryRejected:
            raise
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            response_error = exc

        try:
            observed = self._readback_digest(base_url, token, target_id)
        except MediaEffectDeliveryRetryable as exc:
            raise MediaEffectDeliveryIndeterminate("cover upload readback is unavailable") from exc
        if observed == expected_hash:
            return {
                "target_id": target_id,
                "content_sha256": expected_hash,
                "recovered_by_readback": response_error is not None,
                "output_saved": output_saved,
            }
        raise MediaEffectDeliveryIndeterminate(
            "cover upload outcome is unknown",
            error={"response_lost": response_error is not None},
        )

    def _readback_digest(self, base_url: str, token: str, target_id: str) -> str | None:
        try:
            with self._client_factory() as client:
                with client.stream(
                    "GET",
                    f"{base_url}/Items/{quote(target_id, safe='')}/Images/Primary",
                    headers={"Accept": "image/*", "X-Emby-Token": token},
                ) as response:
                    if response.status_code == 404:
                        return None
                    if response.status_code in {408, 425, 429} or response.status_code >= 500:
                        raise MediaEffectDeliveryRetryable("Emby cover readback is unavailable", retry_after_seconds=5)
                    if 400 <= response.status_code < 500:
                        raise MediaEffectDeliveryRejected("Emby rejected the cover readback")
                    content = _bounded_response_content(response, _MAX_COVER_BYTES)
        except (httpx.HTTPError, TypeError, ValueError) as exc:
            raise MediaEffectDeliveryRetryable("Emby cover readback is unavailable", retry_after_seconds=5) from exc
        if not content:
            return None
        return hashlib.sha256(content).hexdigest()

    def _default_client(self) -> httpx.Client:
        settings = self._settings()
        timeout = max(float(settings.emby_api_timeout), 1.0)
        return httpx.Client(
            timeout=httpx.Timeout(timeout, connect=min(5.0, timeout)),
            trust_env=False,
        )

    def _default_external_client(self) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        timeout = max(float(settings.emby_api_timeout), 1.0)
        return httpx.Client(
            proxy=proxy or None,
            timeout=httpx.Timeout(timeout, connect=min(5.0, timeout)),
            trust_env=False,
        )


def _effect_key(task_key: str, target_id: str, cache_key: str, progress: CoverTaskProgress) -> str:
    execution_id = _optional_text(getattr(progress, "job_id", None)) or cache_key
    occurrence = hashlib.sha256(execution_id.encode("utf-8")).hexdigest()[:32]
    return f"p8:cover:{task_key}:{occurrence}:{target_id}"


def _stable_effect_key(task_key: str, target_id: str, progress: CoverTaskProgress) -> str | None:
    if _optional_text(getattr(progress, "job_id", None)) is None:
        return None
    return _effect_key(task_key, target_id, "", progress)


def _record_effect_result(
    effect: MediaEffectProjection,
    progress: CoverTaskProgress,
    effect_key: str,
) -> str:
    if effect.status is MediaEffectStatus.CONFIRMED:
        progress.record_committed_effect(effect_key)
        return "updated"
    if effect.status in {
        MediaEffectStatus.PENDING,
        MediaEffectStatus.CLAIMED,
        MediaEffectStatus.INDETERMINATE,
    }:
        return "indeterminate"
    progress.record_abandoned_effect(effect_key)
    return "failed"


def _item_query(
    target: Mapping[str, Any],
    target_id: str,
    limit: int,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    query: dict[str, Any] = {
        "Recursive": "true",
        "Limit": max(limit * 5, 20),
        "Fields": ("Id,ImageTags,BackdropImageTags,PrimaryImageTag,PrimaryImageItemId,DateCreated,OfficialRating"),
        "EnableTotalRecordCount": "true",
    }
    generated_ids = _generated_emby_ids(target.get("generated_media_info_json"))
    if str(target.get("type") or "") in {"list", "ai_recommendation_global"} and generated_ids:
        query["Ids"] = ",".join(generated_ids[:100])
    else:
        query["ParentId"] = target_id
    item_types = _content_types(target)
    if item_types:
        query["IncludeItemTypes"] = ",".join(item_types)
    if str(config.get("sort_by") or "Latest") == "Random":
        query["SortBy"] = "Random"
    else:
        query["SortBy"] = "DateCreated"
        query["SortOrder"] = "Descending"
    return query


def _query_cover_items(
    client: httpx.Client,
    *,
    base_url: str,
    user_id: str,
    token: str,
    target: Mapping[str, Any],
    target_id: str,
    limit: int,
    config: Mapping[str, Any],
) -> tuple[list[Any], int | None]:
    response = client.get(
        f"{base_url}/Users/{quote(user_id, safe='')}/Items",
        params=_item_query(target, target_id, limit, config),
        headers={"Accept": "application/json", "X-Emby-Token": token},
    )
    response.raise_for_status()
    payload: Any = response.json()
    items = payload.get("Items") if isinstance(payload, Mapping) else None
    if not isinstance(items, list):
        raise ValueError("Emby cover source response has no Items array")
    return items, _nonnegative_int(payload.get("TotalRecordCount"))


def _content_types(target: Mapping[str, Any]) -> tuple[str, ...]:
    definition = _json_mapping(target.get("definition_json"))
    value = definition.get("item_type") or target.get("item_type")
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, Sequence):
        values = value
    else:
        values = ()
    normalized = tuple(dict.fromkeys(str(item).strip() for item in values if str(item).strip()))
    if normalized:
        return normalized
    return {
        "movies": ("Movie",),
        "tvshows": ("Series",),
        "music": ("MusicAlbum",),
        "boxsets": ("Movie", "Series"),
        "mixed": ("Movie", "Series"),
        "audiobooks": ("AudioBook",),
    }.get(str(target.get("CollectionType") or "").casefold(), ("Movie", "Series"))


def _local_cover_sources(
    target: Mapping[str, Any],
    limit: int,
    config: Mapping[str, Any],
) -> tuple[bytes, ...]:
    configured = _optional_text(config.get("covers_input"))
    if not configured:
        return ()
    root = Path(configured).expanduser().resolve()
    name = str(target.get("Name") or target.get("name") or "").strip()
    if not name:
        return ()
    directory = (root / name).resolve()
    try:
        directory.relative_to(root)
    except ValueError:
        return ()
    if not directory.is_dir():
        return ()
    values: list[bytes] = []
    for path in sorted(directory.iterdir()):
        if path.suffix.casefold() not in {".jpg", ".jpeg", ".png"} or not path.is_file():
            continue
        try:
            if 0 < path.stat().st_size <= _MAX_SOURCE_BYTES:
                values.append(path.read_bytes())
        except OSError:
            continue
        if len(values) >= limit:
            break
    return tuple(values)


def _prefer_tmdb_posters(target: Mapping[str, Any]) -> bool:
    return (
        str(target.get("type") or "") in {"list", "ai_recommendation_global"}
        and len(_generated_emby_ids(target.get("generated_media_info_json"))) < 3
        and bool(_json_list(target.get("tmdb_poster_paths")))
    )


def _download_tmdb_posters(
    client: httpx.Client,
    target: Mapping[str, Any],
    limit: int,
    *,
    config: Mapping[str, Any],
) -> tuple[bytes, ...]:
    values: list[bytes] = []
    paths = _json_list(target.get("tmdb_poster_paths"))
    if str(config.get("sort_by") or "Latest") == "Random":
        random.shuffle(paths)
    for raw_path in paths:
        path = str(raw_path or "").strip()
        if not path.startswith("/") or "://" in path:
            continue
        content = _download_image(client, f"https://image.tmdb.org/t/p/w500{quote(path, safe='/')}")
        if content is not None:
            values.append(content)
        if len(values) >= limit:
            break
    return tuple(values)


def _download_image(
    client: httpx.Client,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
) -> bytes | None:
    try:
        with client.stream("GET", url, headers=headers) as response:
            if response.status_code != 200:
                return None
            content_type = str(response.headers.get("content-type") or "").partition(";")[0].strip()
            if content_type and not content_type.casefold().startswith("image/"):
                return None
            return _bounded_response_content(response, _MAX_SOURCE_BYTES) or None
    except (httpx.HTTPError, TypeError, ValueError):
        return None


def _bounded_response_content(response: httpx.Response, limit: int) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > limit:
                raise ValueError("response exceeds the configured size limit")
        except ValueError as exc:
            raise ValueError("response has an invalid or oversized Content-Length") from exc
    value = bytearray()
    for chunk in response.iter_bytes():
        if len(value) + len(chunk) > limit:
            raise ValueError("response exceeds the configured size limit")
        value.extend(chunk)
    return bytes(value)


def _image_path(item: Mapping[str, Any], config: Mapping[str, Any]) -> str | None:
    item_id = _optional_text(item.get("Id"))
    if not item_id:
        return None
    raw_tags = item.get("ImageTags")
    tags: Mapping[str, Any] = raw_tags if isinstance(raw_tags, Mapping) else {}
    primary_tag = _optional_text(tags.get("Primary"))
    primary_id = item_id if primary_tag else _optional_text(item.get("PrimaryImageItemId"))
    primary_tag = primary_tag or _optional_text(item.get("PrimaryImageTag"))
    primary = (
        f"/Items/{quote(primary_id, safe='')}/Images/Primary"
        + (f"?tag={quote(primary_tag, safe='')}" if primary_tag else "")
        if primary_id
        else None
    )
    backdrop_tags = item.get("BackdropImageTags")
    backdrop_tag = (
        _optional_text(backdrop_tags[0])
        if isinstance(backdrop_tags, Sequence) and not isinstance(backdrop_tags, str | bytes) and backdrop_tags
        else None
    )
    backdrop = (
        f"/Items/{quote(item_id, safe='')}/Images/Backdrop/0"
        + (f"?tag={quote(backdrop_tag, safe='')}" if backdrop_tag else "")
        if backdrop_tag
        else None
    )
    style = str(config.get("cover_style") or "single_1")
    use_primary = (
        bool(config.get("single_use_primary"))
        if style.startswith("single")
        else bool(config.get("multi_1_use_primary", True))
    )
    return (primary or backdrop) if use_primary else (backdrop or primary)


def _rating_allowed(
    item: Mapping[str, Any],
    target: Mapping[str, Any],
    config: Mapping[str, Any],
) -> bool:
    collection_type = str(target.get("type") or "")
    if collection_type in {"list", "ai_recommendation_global"}:
        return True
    definition = _json_mapping(target.get("definition_json"))
    rules = definition.get("rules")
    if isinstance(rules, list) and any(
        isinstance(rule, Mapping) and rule.get("field") == "unified_rating" for rule in rules
    ):
        return True
    name = str(target.get("Name") or target.get("name") or "")
    if any(value.casefold() in name.casefold() for value in ("R级", "限制", "成人", "Adult", "Porn", "18+")):
        return True
    raw_limit = config.get("max_safe_rating", 8)
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        limit = 8
    if limit >= 999:
        return True
    rating = str(item.get("OfficialRating") or "").strip()
    if not rating:
        return True
    country = "US"
    if "-" in rating:
        prefix, candidate = rating.split("-", 1)
        if prefix.upper() in DEFAULT_RATING_MAPPING:
            country = prefix.upper()
            rating = candidate
    values = {
        str(entry.get("code") or "").casefold(): int(entry.get("emby_value") or 0)
        for entry in DEFAULT_RATING_MAPPING.get(country, ())
    }
    return values.get(rating.casefold(), 0) <= limit


def _write_cover_output(root_value: Any, target_name: str, content: bytes) -> bool:
    configured = _optional_text(root_value)
    if not configured:
        return False
    root = Path(configured).expanduser().resolve()
    safe_name = "".join("_" if character in {"/", "\\", "\0"} else character for character in target_name).strip()
    safe_name = safe_name[:240] or "cover"
    # Keep the established covers_output filename contract; the rendered bytes remain PNG.
    destination = root / f"{safe_name}.jpg"
    temporary: str | None = None
    try:
        root.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=root, prefix=".cover-", delete=False) as handle:
            temporary = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        return True
    except OSError:
        logger.warning("unable to save configured cover output", extra={"target": target_name}, exc_info=True)
        return False
    finally:
        if temporary and os.path.exists(temporary):
            os.unlink(temporary)


def _font_identity(path: Path) -> str:
    stat = path.stat()
    return f"{path.resolve()}:{stat.st_size}:{stat.st_mtime_ns}"


def _emby(settings: Settings) -> tuple[str, str, str]:
    base_url = settings.emby_server_url.strip().rstrip("/")
    user_id = settings.emby_user_id.strip()
    token = settings.emby_api_key.get_secret_value().strip()
    if not base_url or not user_id or not token:
        raise RuntimeError("cover task requires configured Emby authorization")
    return base_url, user_id, token


def _target_id(target: Mapping[str, Any]) -> str:
    value = _target_id_value(target)
    if not value:
        raise ValueError("cover target identity is missing")
    return value


def _target_id_value(target: Mapping[str, Any]) -> str:
    return str(
        target.get("Id") or target.get("emby_collection_id") or target.get("ItemId") or target.get("id") or ""
    ).strip()


def _titles(config: Mapping[str, Any], fallback: str) -> tuple[str, str]:
    raw = str(config.get("title_config") or "")
    if not raw:
        return fallback, ""
    try:
        parsed = yaml.safe_load(raw)
    except yaml.YAMLError:
        return fallback, ""
    if isinstance(parsed, Mapping):
        titles = parsed.get(fallback)
        if isinstance(titles, Sequence) and not isinstance(titles, str | bytes):
            values = [str(value or "").strip() for value in titles]
            if values:
                return values[0] or fallback, values[1] if len(values) > 1 else ""
    return fallback, ""


def _item_count(target: Mapping[str, Any], *, fallback: int | None) -> CoverBadge:
    collection_type = str(target.get("type") or "")
    if collection_type == "list":
        raw_url = _json_mapping(target.get("definition_json")).get("url", "")
        urls = raw_url if isinstance(raw_url, list) else [raw_url]
        kinds: set[str] = set()
        for value in urls:
            url = str(value or "")
            if url.startswith("maoyan://"):
                kinds.add("猫眼")
            elif "douban.com/doulist" in url:
                kinds.add("豆列")
            elif "themoviedb.org/discover/" in url:
                kinds.add("探索")
            else:
                kinds.add("未知")
        if len(kinds) == 1 and "未知" not in kinds:
            return kinds.pop()
        return "榜单" if kinds == {"未知"} else "混合"
    if collection_type == "ai_recommendation_global":
        return "热榜"
    if collection_type == "ai_recommendation":
        return "推荐"
    value = target.get("item_count", target.get("in_library_count", fallback))
    if isinstance(value, bool):
        return fallback
    try:
        return int(value) if value is not None else fallback
    except (TypeError, ValueError):
        return fallback


def _seed(target: Mapping[str, Any]) -> int:
    identity = _target_id(target)
    return int(hashlib.sha256(identity.encode("utf-8")).hexdigest()[:8], 16)


def _json_list(value: Any) -> list[Any]:
    normalized = value
    if isinstance(normalized, str):
        try:
            normalized = json.loads(normalized)
        except (TypeError, ValueError):
            return []
    return list(normalized) if isinstance(normalized, list) else []


def _json_mapping(value: Any) -> dict[str, Any]:
    normalized = value
    if isinstance(normalized, str):
        try:
            normalized = json.loads(normalized)
        except (TypeError, ValueError):
            return {}
    return dict(normalized) if isinstance(normalized, Mapping) else {}


def _generated_tmdb_ids(value: Any) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(item.get("tmdb_id") or item.get("id") or "").strip()
            for item in _json_list(value)
            if isinstance(item, Mapping) and str(item.get("tmdb_id") or item.get("id") or "").strip()
        )
    )


def _generated_emby_ids(value: Any) -> tuple[str, ...]:
    return tuple(
        dict.fromkeys(
            str(item.get("emby_id") or "").strip()
            for item in _json_list(value)
            if isinstance(item, Mapping)
            and str(item.get("emby_id") or "").strip()
            and str(item.get("emby_id") or "").strip().casefold() != "none"
        )
    )


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return max(parsed, 0)


def _optional_text(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _library_is_coverable(target: Mapping[str, Any]) -> bool:
    collection_type = str(target.get("CollectionType") or target.get("collection_type") or "").strip().casefold()
    if collection_type in {"movies", "tvshows", "boxsets", "mixed", "music", "audiobooks"}:
        return True
    return str(target.get("Type") or target.get("type") or "").strip().casefold() == "collectionfolder"


__all__ = [
    "COVER_TASK_KEYS",
    "COVER_IMAGE_EFFECT_TYPE",
    "ConfiguredCoverRenderer",
    "CoverSourceSnapshot",
    "HttpxCoverRemoteAdapter",
    "NativeCoverTaskService",
    "PersistentCoverTaskEffects",
    "SqlAlchemyCoverTargetRepository",
]
