from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any
from uuid import UUID

from etk.platform.jobs.types import UnitOfWorkFactory

from .effect_execution import MediaEffectDeliveryRejected, PersistentMediaEffectExecutor
from .effects import MediaEffectProjection, MediaEffectStatus, PostgresMediaEffectLedger
from .image_editing import (
    ImageEditingGateway,
    ImageUpdateCommand,
    ImageUpdateOutcome,
    TmdbImageCatalog,
)

_MANUAL_IMAGE_EFFECT = "emby.image.manual_update"


class ManualImageEffectDelivery:
    effect_types = frozenset({_MANUAL_IMAGE_EFFECT})

    def __init__(self, gateway: ImageEditingGateway) -> None:
        self._gateway = gateway

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        encoded = payload.get("image_bytes_base64")
        image_bytes: bytes | None = None
        if encoded is not None:
            try:
                image_bytes = base64.b64decode(str(encoded), validate=True)
            except (ValueError, TypeError) as exc:
                raise ValueError("manual image effect contains invalid image bytes") from exc
        outcome = self._gateway.update_image(
            ImageUpdateCommand(
                item_id=str(payload.get("item_id") or ""),
                image_type=str(payload.get("image_type") or ""),
                image_url=str(payload.get("image_url") or ""),
                image_bytes=image_bytes,
                content_type=str(payload.get("content_type") or "") or None,
            )
        )
        if not outcome.success:
            raise MediaEffectDeliveryRejected(
                outcome.message or "图片替换被提供方拒绝",
                error={"operation": "manual-image-update"},
            )
        return {
            "operation": "manual-image-update",
            "success": True,
            "message": outcome.message,
        }


class PersistentMediaImageEditingGateway:
    """Keep image reads compatible while routing manual writes through the P5 ledger."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        compatibility: ImageEditingGateway,
        *,
        worker_id: str | None = None,
        lease_seconds: float | Callable[[], float] = 90,
    ) -> None:
        self._compatibility = compatibility
        self._ledger = PostgresMediaEffectLedger(uow_factory)
        self._executor = PersistentMediaEffectExecutor(
            self._ledger,
            ManualImageEffectDelivery(compatibility),
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    def processor_ready(self) -> bool:
        return self._compatibility.processor_ready()

    def tmdb_images(self, item_id: str) -> TmdbImageCatalog:
        return self._compatibility.tmdb_images(item_id)

    def update_image(self, command: ImageUpdateCommand) -> ImageUpdateOutcome:
        payload = {
            "item_id": str(command.item_id or "").strip(),
            "image_type": str(command.image_type or "").strip(),
            "image_url": str(command.image_url or ""),
            "image_bytes_base64": (
                base64.b64encode(command.image_bytes).decode("ascii") if command.image_bytes is not None else None
            ),
            "content_type": str(command.content_type or "") or None,
        }
        if not payload["item_id"] or not payload["image_type"]:
            return ImageUpdateOutcome(False, "图片替换参数不完整")
        if not payload["image_url"] and payload["image_bytes_base64"] is None:
            return ImageUpdateOutcome(False, "必须提供 image_url 或上传 file")
        reservation = self._ledger.reserve(
            effect_key=_effect_key(payload),
            effect_type=_MANUAL_IMAGE_EFFECT,
            aggregate_type="media_item",
            aggregate_id=str(payload["item_id"]),
            payload=payload,
        )
        return self.execute(reservation.effect.id)

    def execute(self, effect_id: UUID) -> ImageUpdateOutcome:
        effect = self._executor.execute(effect_id)
        if effect.status is MediaEffectStatus.CONFIRMED:
            result = effect.result or {}
            return ImageUpdateOutcome(
                True,
                str(result.get("message") or "图片替换成功"),
                effect_id=str(effect.id),
                effect_status=effect.status.value,
            )
        error = str((effect.error or {}).get("message") or "图片替换失败")
        return ImageUpdateOutcome(
            False,
            error,
            effect_id=str(effect.id),
            effect_status=effect.status.value,
        )


def _effect_key(payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"p5:{_MANUAL_IMAGE_EFFECT}:{payload['item_id']}:{payload['image_type']}:{digest}"


__all__ = ["ManualImageEffectDelivery", "PersistentMediaImageEditingGateway"]
