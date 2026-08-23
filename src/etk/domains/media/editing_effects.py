from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.platform.jobs.types import UnitOfWorkFactory

from .bridge_client import (
    BridgeAuthorizationChanged,
    BridgeWriteIndeterminate,
    BridgeWriteRejected,
    BridgeWriteResult,
)
from .editing import (
    ChapterUpdate,
    MediaCommandOutcome,
    MediaCommandStatus,
)
from .effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    PersistentMediaEffectExecutor,
)
from .effects import (
    MediaEffectNotFoundError,
    MediaEffectProjection,
    MediaEffectStatus,
    PostgresMediaEffectLedger,
    SqlAlchemyMediaEffectRepository,
)
from .models import MediaInfoEditContext
from .repository import MediaAssetAlignmentError, MediaIdentityConflictError


class MediaInfoBridgeGateway(Protocol):
    def authorization_revision(self) -> str: ...

    def apply_mediainfo(
        self,
        item_id: str,
        raw: Any,
        *,
        drop_conflicting_external_streams: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult: ...

    def update_chapters(
        self,
        item_id: str,
        *,
        intro_start_ticks: int | None = None,
        intro_end_ticks: int | None = None,
        credits_start_ticks: int | None = None,
        clear_intro: bool = False,
        clear_credits: bool = False,
        expected_authorization_revision: str | None = None,
    ) -> BridgeWriteResult: ...


class BridgeMediaEffectDelivery:
    effect_types = frozenset({"emby.mediainfo.apply", "emby.chapters.update"})

    def __init__(self, bridge: MediaInfoBridgeGateway) -> None:
        self._bridge = bridge

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        item_id = str(payload.get("item_id") or "")
        expected_revision = _authorization_revision(payload.get("emby_authorization_revision"))
        if _authorization_revision(self._bridge.authorization_revision()) != expected_revision:
            raise MediaEffectDeliveryRejected(
                "Emby authorization changed after media effect staging",
                error={"operation": effect.effect_type, "reason": "authorization_changed"},
            )
        try:
            if effect.effect_type == "emby.mediainfo.apply":
                result = self._bridge.apply_mediainfo(
                    item_id,
                    payload.get("mediainfo"),
                    drop_conflicting_external_streams=bool(payload.get("drop_conflicting_external_streams")),
                    expected_authorization_revision=expected_revision,
                )
            elif effect.effect_type == "emby.chapters.update":
                result = self._bridge.update_chapters(
                    item_id,
                    intro_start_ticks=_optional_int(payload.get("intro_start_ticks")),
                    intro_end_ticks=_optional_int(payload.get("intro_end_ticks")),
                    credits_start_ticks=_optional_int(payload.get("credits_start_ticks")),
                    clear_intro=bool(payload.get("clear_intro")),
                    clear_credits=bool(payload.get("clear_credits")),
                    expected_authorization_revision=expected_revision,
                )
            else:
                raise ValueError(f"unsupported Bridge media effect type: {effect.effect_type}")
        except BridgeAuthorizationChanged as exc:
            raise MediaEffectDeliveryRejected(
                str(exc),
                error={"operation": exc.operation, "reason": "authorization_changed"},
            ) from exc
        except BridgeWriteRejected as exc:
            raise MediaEffectDeliveryRejected(
                str(exc),
                error={"operation": exc.operation, "status_code": exc.status_code},
            ) from exc
        except BridgeWriteIndeterminate as exc:
            raise MediaEffectDeliveryIndeterminate(
                str(exc),
                error={"operation": exc.operation},
            ) from exc
        return {
            "operation": result.operation,
            "status_code": result.status_code,
            "payload": dict(result.payload),
        }


class PersistentMediaInfoCommandGateway:
    """Atomically persist MediaInfo desired state and a durable Bridge effect intent."""

    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        bridge: MediaInfoBridgeGateway,
        *,
        worker_id: str | None = None,
        lease_seconds: float | Callable[[], float] = 90,
    ) -> None:
        self._uow_factory = uow_factory
        self._bridge = bridge
        self._ledger = PostgresMediaEffectLedger(uow_factory)
        self._executor = PersistentMediaEffectExecutor(
            self._ledger,
            BridgeMediaEffectDelivery(bridge),
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    def apply_chapters(
        self,
        context: MediaInfoEditContext,
        update: ChapterUpdate,
    ) -> MediaCommandOutcome:
        payload = {
            "item_id": context.item_id,
            "sha1": context.sha1,
            "emby_authorization_revision": _authorization_revision(self._bridge.authorization_revision()),
            "mediainfo": _json_mediainfo(update.mediainfo),
            "intro_start_ticks": update.intro_start_ticks,
            "intro_end_ticks": update.intro_end_ticks,
            "credits_start_ticks": update.credits_start_ticks,
            "clear_intro": update.clear_intro,
            "clear_credits": update.clear_credits,
        }
        effect = self._stage(
            context,
            effect_type="emby.chapters.update",
            payload=payload,
            mediainfo=payload["mediainfo"],
        )
        return self.execute(effect.id)

    def apply_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
    ) -> MediaCommandOutcome:
        payload = {
            "item_id": context.item_id,
            "sha1": context.sha1,
            "emby_authorization_revision": _authorization_revision(self._bridge.authorization_revision()),
            "mediainfo": _json_mediainfo(mediainfo),
            "drop_conflicting_external_streams": False,
        }
        effect = self._stage(
            context,
            effect_type="emby.mediainfo.apply",
            payload=payload,
            mediainfo=payload["mediainfo"],
        )
        return self.execute(effect.id)

    def execute(self, effect_id: UUID) -> MediaCommandOutcome:
        try:
            current = self._executor.execute(effect_id)
        except MediaEffectNotFoundError:
            return MediaCommandOutcome(MediaCommandStatus.REJECTED, str(effect_id), error="媒体副作用不存在")
        return _command_outcome(current) or MediaCommandOutcome(
            MediaCommandStatus.PENDING,
            str(effect_id),
            error="媒体副作用正在由其他执行器处理",
        )

    def _stage(
        self,
        context: MediaInfoEditContext,
        *,
        effect_type: str,
        payload: Mapping[str, Any],
        mediainfo: object,
    ) -> MediaEffectProjection:
        effect_key = _effect_key(effect_type, context, payload)
        with self._uow_factory() as uow:
            _lock_and_validate_binding(uow.session, context)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_mediainfo_cache
                    SET mediainfo_json = CAST(:mediainfo AS jsonb)
                    WHERE upper(sha1) = :sha1
                    """
                ),
                {
                    "mediainfo": json.dumps(mediainfo, ensure_ascii=False, separators=(",", ":")),
                    "sha1": context.sha1,
                },
            )
            if int(getattr(updated, "rowcount", 0) or 0) != 1:
                raise MediaIdentityConflictError("MediaInfo cache changed before the command was staged")
            reservation = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                effect_key=effect_key,
                effect_type=effect_type,
                aggregate_type="media_asset",
                aggregate_id=f"{context.item_id}:{context.sha1}",
                payload=payload,
            )
            uow.commit()
        return reservation.effect


def _lock_and_validate_binding(session: Any, context: MediaInfoEditContext) -> None:
    rows = (
        session.execute(
            text(
                """
                SELECT asset_details_json, emby_item_ids_json, file_sha1_json, file_pickcode_json
                FROM media_metadata
                WHERE emby_item_ids_json @> CAST(:item_ids AS jsonb)
                FOR UPDATE
                """
            ),
            {"item_ids": json.dumps([context.item_id])},
        )
        .mappings()
        .all()
    )
    if len(rows) != 1:
        raise MediaIdentityConflictError("Emby ItemID binding changed before the command was staged")
    row = rows[0]
    arrays = (
        _json_list(row.get("asset_details_json")),
        [str(value or "").strip() for value in _json_list(row.get("emby_item_ids_json"))],
        [str(value or "").strip().upper() for value in _json_list(row.get("file_sha1_json"))],
        _json_list(row.get("file_pickcode_json")),
    )
    if len({len(values) for values in arrays}) != 1:
        raise MediaAssetAlignmentError("media asset version arrays are not position-aligned")
    assets, emby_ids, sha1s, _pick_codes = arrays
    indices = [index for index, item_id in enumerate(emby_ids) if item_id == context.item_id]
    if len(indices) != 1 or sha1s[indices[0]] != context.sha1 or not isinstance(assets[indices[0]], Mapping):
        raise MediaIdentityConflictError("Emby ItemID and SHA1 binding changed before the command was staged")


def _effect_key(
    effect_type: str,
    context: MediaInfoEditContext,
    payload: Mapping[str, Any],
) -> str:
    digest = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    return f"p5:{effect_type}:{context.item_id}:{context.sha1}:{digest}"


def _command_outcome(effect: MediaEffectProjection) -> MediaCommandOutcome | None:
    if effect.status is MediaEffectStatus.CONFIRMED:
        result = dict(effect.result or {})
        provider_payload = result.get("payload")
        return MediaCommandOutcome(
            MediaCommandStatus.CONFIRMED,
            str(effect.id),
            provider_result=dict(provider_payload) if isinstance(provider_payload, Mapping) else {},
        )
    if effect.status is MediaEffectStatus.REJECTED:
        return MediaCommandOutcome(
            MediaCommandStatus.REJECTED,
            str(effect.id),
            error=str((effect.error or {}).get("message") or "Bridge 拒绝写入"),
        )
    if effect.status is MediaEffectStatus.INDETERMINATE:
        return MediaCommandOutcome(
            MediaCommandStatus.INDETERMINATE,
            str(effect.id),
            error=str((effect.error or {}).get("message") or "Bridge 写入结果未知，禁止自动重放"),
        )
    return None


def _json_mediainfo(
    value: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
) -> Mapping[str, Any] | list[Mapping[str, Any]]:
    if isinstance(value, tuple):
        return [dict(item) for item in value]
    return dict(value)


def _json_list(value: object) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _optional_int(value: object) -> int | None:
    return int(str(value)) if value is not None else None


def _authorization_revision(value: object) -> str:
    revision = str(value or "").strip().lower()
    if len(revision) != 64:
        raise ValueError("Emby authorization revision is invalid")
    try:
        bytes.fromhex(revision)
    except ValueError as exc:
        raise ValueError("Emby authorization revision is invalid") from exc
    return revision


__all__ = [
    "BridgeMediaEffectDelivery",
    "MediaInfoBridgeGateway",
    "PersistentMediaInfoCommandGateway",
]
