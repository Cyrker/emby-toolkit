from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy import text

from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
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
        executor: PersistentMediaEffectExecutor | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._bridge = bridge
        self._ledger = PostgresMediaEffectLedger(uow_factory)
        self._executor = executor or PersistentMediaEffectExecutor(
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
        *,
        operation_key: str | None = None,
        expected_cache_revision: int | None = None,
    ) -> MediaCommandOutcome:
        payload = {
            "item_id": context.item_id,
            "sha1": context.sha1,
            "emby_authorization_revision": _authorization_revision(self._bridge.authorization_revision()),
            "mediainfo": _json_mediainfo(mediainfo),
            "drop_conflicting_external_streams": False,
        }
        if operation_key is not None:
            payload["media_operation_key"] = _operation_key(operation_key)
        effect = self._stage(
            context,
            effect_type="emby.mediainfo.apply",
            payload=payload,
            mediainfo=payload["mediainfo"],
            expected_cache_revision=expected_cache_revision,
        )
        return self.execute(effect.id)

    def apply_generated_mediainfo(
        self,
        context: MediaInfoEditContext,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
        raw: Mapping[str, Any],
        *,
        probe_item_id: str,
        probe_claim_token: str,
        account_id: str,
        provider_backend: str,
        formatter_revision: str,
        probe_revision: str,
        probe_result: Mapping[str, Any] | None = None,
        raw_schema_version: int = 1,
    ) -> MediaCommandOutcome:
        effect = self._stage_generated(
            context,
            mediainfo=mediainfo,
            raw=raw,
            probe_item_id=probe_item_id,
            probe_claim_token=probe_claim_token,
            account_id=account_id,
            provider_backend=provider_backend,
            formatter_revision=formatter_revision,
            probe_revision=probe_revision,
            probe_result=probe_result,
            raw_schema_version=raw_schema_version,
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
        expected_cache_revision: int | None = None,
    ) -> MediaEffectProjection:
        if expected_cache_revision is not None and expected_cache_revision < 0:
            raise ValueError("expected MediaInfo cache revision must be non-negative")
        effect_key = _effect_key(effect_type, context, payload)
        with self._uow_factory() as uow:
            _lock_and_validate_binding(uow.session, context)
            updated = uow.session.execute(
                text(
                    """
                    UPDATE p115_mediainfo_cache
                    SET mediainfo_json = CAST(:mediainfo AS jsonb),
                        cache_revision = cache_revision + 1,
                        updated_at = NOW()
                    WHERE upper(sha1) = :sha1
                      AND (
                          CAST(:expected_cache_revision AS bigint) IS NULL
                          OR cache_revision = CAST(:expected_cache_revision AS bigint)
                      )
                    """
                ),
                {
                    "mediainfo": json.dumps(mediainfo, ensure_ascii=False, separators=(",", ":")),
                    "sha1": context.sha1,
                    "expected_cache_revision": expected_cache_revision,
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

    def _stage_generated(
        self,
        context: MediaInfoEditContext,
        *,
        mediainfo: Mapping[str, Any] | tuple[Mapping[str, Any], ...],
        raw: Mapping[str, Any],
        probe_item_id: str,
        probe_claim_token: str,
        account_id: str,
        provider_backend: str,
        formatter_revision: str,
        probe_revision: str,
        probe_result: Mapping[str, Any] | None,
        raw_schema_version: int,
    ) -> MediaEffectProjection:
        normalized_sha1 = _canonical_sha1(context.sha1)
        normalized_account = str(account_id or "").strip()
        normalized_backend = str(provider_backend or "").strip().lower()
        normalized_formatter_revision = _cache_tag(formatter_revision, "formatter revision")
        normalized_probe_revision = _cache_tag(probe_revision, "probe revision")
        if not normalized_account.isdigit():
            raise ValueError("generated MediaInfo requires a stable numeric 115 account ID")
        if normalized_backend not in {"openapi", "cookie", "raw"}:
            raise ValueError("generated MediaInfo provider backend is invalid")
        if isinstance(raw_schema_version, bool) or int(raw_schema_version) < 1:
            raise ValueError("generated MediaInfo raw schema version must be positive")
        item_id = UUID(str(probe_item_id))
        claim_token = UUID(str(probe_claim_token))
        formatted_json = _json_mediainfo(mediainfo)
        raw_json = dict(raw)
        authorization_revision = _authorization_revision(self._bridge.authorization_revision())

        with self._uow_factory() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {"namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE, "lock_key": P115_AUTH_LOCK_KEY},
            )
            persisted_account = str(
                uow.session.scalar(
                    text(
                        """
                        SELECT value_json ->> 'account_id'
                        FROM app_settings
                        WHERE setting_key = 'p115_auth_tokens'
                        """
                    )
                )
                or ""
            ).strip()
            if persisted_account != normalized_account:
                raise MediaIdentityConflictError("115 account changed before generated MediaInfo was staged")
            _lock_and_validate_binding(uow.session, context)
            work = (
                uow.session.execute(
                    text(
                        """
                        SELECT operation_key, source, account_id, sha1, expected_cache_revision
                        FROM p115_media_probe_items
                        WHERE id = :item_id
                          AND status = 'probing'
                          AND claim_token = :claim_token
                        FOR UPDATE
                        """
                    ),
                    {"item_id": item_id, "claim_token": claim_token},
                )
                .mappings()
                .one_or_none()
            )
            if work is None:
                raise MediaIdentityConflictError("generated MediaInfo probe claim changed before staging")
            if str(work["account_id"]) != normalized_account or _canonical_sha1(work["sha1"]) != normalized_sha1:
                raise MediaIdentityConflictError("generated MediaInfo probe identity changed before staging")
            operation_key = _operation_key(work["operation_key"])
            expected_revision = _optional_nonnegative_int(work.get("expected_cache_revision"))
            current_revision = uow.session.scalar(
                text("SELECT cache_revision FROM p115_mediainfo_cache WHERE sha1 = :sha1 FOR UPDATE"),
                {"sha1": normalized_sha1},
            )
            if current_revision is None:
                if expected_revision is not None:
                    raise MediaIdentityConflictError("MediaInfo cache disappeared before generated state was staged")
                next_revision = 1
                uow.session.execute(
                    text(
                        """
                        INSERT INTO p115_mediainfo_cache (
                            sha1, mediainfo_json, raw_ffprobe_json, cache_revision,
                            raw_schema_version, formatter_revision, probe_revision
                        ) VALUES (
                            :sha1, CAST(:mediainfo AS jsonb), CAST(:raw AS jsonb), :cache_revision,
                            :raw_schema_version, :formatter_revision, :probe_revision
                        )
                        """
                    ),
                    {
                        "sha1": normalized_sha1,
                        "mediainfo": _json_text(formatted_json),
                        "raw": _json_text(raw_json),
                        "cache_revision": next_revision,
                        "raw_schema_version": int(raw_schema_version),
                        "formatter_revision": normalized_formatter_revision,
                        "probe_revision": normalized_probe_revision,
                    },
                )
            else:
                actual_revision = int(current_revision)
                if expected_revision is None or actual_revision != expected_revision:
                    raise MediaIdentityConflictError(
                        "MediaInfo cache revision changed before generated state was staged"
                    )
                next_revision = actual_revision + 1
                updated = uow.session.execute(
                    text(
                        """
                        UPDATE p115_mediainfo_cache
                        SET mediainfo_json = CAST(:mediainfo AS jsonb),
                            raw_ffprobe_json = CAST(:raw AS jsonb),
                            cache_revision = :next_revision,
                            raw_schema_version = :raw_schema_version,
                            formatter_revision = :formatter_revision,
                            probe_revision = :probe_revision,
                            updated_at = NOW()
                        WHERE sha1 = :sha1 AND cache_revision = :expected_revision
                        """
                    ),
                    {
                        "sha1": normalized_sha1,
                        "mediainfo": _json_text(formatted_json),
                        "raw": _json_text(raw_json),
                        "next_revision": next_revision,
                        "expected_revision": expected_revision,
                        "raw_schema_version": int(raw_schema_version),
                        "formatter_revision": normalized_formatter_revision,
                        "probe_revision": normalized_probe_revision,
                    },
                )
                if int(getattr(updated, "rowcount", 0) or 0) != 1:
                    raise MediaIdentityConflictError("MediaInfo cache revision changed during generated staging")

            payload = {
                "item_id": context.item_id,
                "sha1": normalized_sha1,
                "emby_authorization_revision": authorization_revision,
                "mediainfo": formatted_json,
                "drop_conflicting_external_streams": False,
                "media_operation_key": operation_key,
            }
            effect_key = _effect_key("emby.mediainfo.apply", context, payload)
            reservation = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                effect_key=effect_key,
                effect_type="emby.mediainfo.apply",
                aggregate_type="media_asset",
                aggregate_id=f"{context.item_id}:{normalized_sha1}",
                payload=payload,
            )
            result = {
                **dict(probe_result or {}),
                "effect_id": str(reservation.effect.id),
                "cache_revision": next_revision,
                "source": str(work["source"]),
            }
            confirmed = uow.session.execute(
                text(
                    """
                    UPDATE p115_media_probe_items
                    SET status = 'confirmed',
                        provider_backend = :provider_backend,
                        result_json = CAST(:result AS jsonb),
                        last_error_json = NULL,
                        claim_token = NULL,
                        lease_expires_at = NULL,
                        updated_at = NOW(),
                        finished_at = NOW()
                    WHERE id = :item_id
                      AND status = 'probing'
                      AND claim_token = :claim_token
                    """
                ),
                {
                    "provider_backend": normalized_backend,
                    "result": _json_text(result),
                    "item_id": item_id,
                    "claim_token": claim_token,
                },
            )
            if int(getattr(confirmed, "rowcount", 0) or 0) != 1:
                raise MediaIdentityConflictError("generated MediaInfo probe claim changed during staging")
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


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _operation_key(value: object) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 255:
        raise ValueError("media operation key must be 1-255 characters")
    return normalized


def _canonical_sha1(value: object) -> str:
    normalized = str(value or "").strip().upper()
    if len(normalized) != 40:
        raise ValueError("MediaInfo SHA1 must be 40 hexadecimal characters")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError("MediaInfo SHA1 must be 40 hexadecimal characters") from exc
    return normalized


def _cache_tag(value: object, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 64:
        raise ValueError(f"generated MediaInfo {label} is invalid")
    if any(not (character.isalnum() or character in "._:-") for character in normalized):
        raise ValueError(f"generated MediaInfo {label} is invalid")
    return normalized


def _optional_nonnegative_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("expected MediaInfo cache revision must be non-negative")
    parsed = int(str(value))
    if parsed < 0:
        raise ValueError("expected MediaInfo cache revision must be non-negative")
    return parsed


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
