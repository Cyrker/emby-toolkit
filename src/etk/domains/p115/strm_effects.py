from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol
from uuid import UUID

from sqlalchemy.orm import Session

from etk.domains.media.bridge_client import emby_authorization_revision
from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
)
from etk.domains.media.effects import (
    MediaEffectProjection,
    MediaEffectStatus,
    ReserveMediaEffectResult,
    SqlAlchemyMediaEffectRepository,
)
from etk.platform import Settings
from etk.platform.jobs.json_payload import canonical_json

from .emby_path_gateway import EmbyPathChangeRejected

EMBY_PATHS_CHANGED_EFFECT = "emby.library.paths_changed"
_PATH_BATCH_SIZE = 500


class EmbyPathChangeGateway(Protocol):
    def notify(
        self,
        paths: Sequence[str],
        *,
        base_url: str,
        api_key: str,
        update_type: str,
    ) -> bool: ...


def stage_emby_path_change_effects(
    session: Session,
    *,
    account_id: str,
    operation_revision: str,
    authorization_revision: str,
    modified_paths: Sequence[str],
    deleted_paths: Sequence[str],
    plan_revision: str,
    recovery_intent: bool = False,
) -> tuple[UUID, ...]:
    account = str(account_id or "").strip()
    if not account:
        raise ValueError("Emby path-change effect requires account_id")
    operation = _hex_revision(operation_revision, "operation_revision")
    authorization = _hex_revision(authorization_revision, "authorization_revision")
    plan = _hex_revision(plan_revision, "plan_revision")
    repository = SqlAlchemyMediaEffectRepository(session)
    effect_ids: list[UUID] = []
    for update_type, paths in (("Modified", modified_paths), ("Deleted", deleted_paths)):
        normalized = sorted({str(path or "").strip() for path in paths if str(path or "").strip()})
        batch_size = 1 if update_type == "Modified" else _PATH_BATCH_SIZE
        for index in range(0, len(normalized), batch_size):
            batch = normalized[index : index + batch_size]
            payload: dict[str, Any] = {
                "paths": batch,
                "update_type": update_type,
                "emby_authorization_revision": authorization,
                "operation_revision": operation,
            }
            if recovery_intent:
                payload["recovery_revision"] = plan
            reservation = _reserve_path_change_effect(repository, account=account, payload=payload)
            if (
                not reservation.created
                and not recovery_intent
                and reservation.effect.status is MediaEffectStatus.REJECTED
                and isinstance(reservation.effect.error, Mapping)
                and reservation.effect.error.get("reason") == "authorization_changed"
            ):
                payload["recovery_revision"] = plan
                reservation = _reserve_path_change_effect(repository, account=account, payload=payload)
            effect_ids.append(reservation.effect.id)
    return tuple(effect_ids)


def _reserve_path_change_effect(
    repository: SqlAlchemyMediaEffectRepository,
    *,
    account: str,
    payload: Mapping[str, Any],
) -> ReserveMediaEffectResult:
    digest = hashlib.sha256(canonical_json(payload, field="Emby path-change payload").encode()).hexdigest()
    return repository.reserve(
        effect_key=f"p6:{EMBY_PATHS_CHANGED_EFFECT}:{account}:{digest}",
        effect_type=EMBY_PATHS_CHANGED_EFFECT,
        aggregate_type="p115_strm_sync",
        aggregate_id=account,
        payload=payload,
    )


class EmbyPathChangeEffectDelivery:
    effect_types = frozenset({EMBY_PATHS_CHANGED_EFFECT})

    def __init__(
        self,
        settings: Callable[[], Settings],
        gateway: EmbyPathChangeGateway,
    ) -> None:
        self._settings = settings
        self._gateway = gateway

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        if effect.effect_type != EMBY_PATHS_CHANGED_EFFECT:
            raise ValueError(f"unsupported Emby path-change effect type: {effect.effect_type}")
        payload = effect.payload
        raw_paths = payload.get("paths")
        if not isinstance(raw_paths, list) or not raw_paths or len(raw_paths) > _PATH_BATCH_SIZE:
            raise ValueError("Emby path-change effect requires a bounded non-empty paths array")
        paths = tuple(str(path or "").strip() for path in raw_paths)
        if any(not path for path in paths) or len(set(paths)) != len(paths):
            raise ValueError("Emby path-change effect paths are empty or duplicated")
        update_type = str(payload.get("update_type") or "").strip().title()
        if update_type not in {"Modified", "Deleted"}:
            raise ValueError("Emby path-change effect update_type is unsupported")

        settings = self._settings()
        expected_revision = _hex_revision(
            payload.get("emby_authorization_revision"),
            "emby_authorization_revision",
        )
        if emby_authorization_revision(settings) != expected_revision:
            raise MediaEffectDeliveryRejected(
                "Emby authorization changed after path notification was staged",
                error={"operation": "library-paths-changed", "reason": "authorization_changed"},
            )
        base_url = str(settings.emby_server_url or "").strip().rstrip("/")
        api_key = settings.emby_api_key.get_secret_value().strip()
        if not base_url or not api_key:
            raise MediaEffectDeliveryRejected(
                "Emby path notification configuration is incomplete",
                error={"operation": "library-paths-changed", "reason": "configuration_incomplete"},
            )
        try:
            delivered = self._gateway.notify(
                paths,
                base_url=base_url,
                api_key=api_key,
                update_type=update_type,
            )
        except EmbyPathChangeRejected as exc:
            raise MediaEffectDeliveryRejected(
                "Emby path notification was rejected",
                error={"operation": "library-paths-changed", "update_type": update_type},
            ) from exc
        except Exception as exc:
            raise MediaEffectDeliveryIndeterminate(
                "Emby path notification outcome is unknown",
                error={"operation": "library-paths-changed", "update_type": update_type},
            ) from exc
        if not delivered:
            raise MediaEffectDeliveryIndeterminate(
                "Emby path notification did not confirm delivery",
                error={"operation": "library-paths-changed", "update_type": update_type},
            )
        return {
            "operation": "library-paths-changed",
            "path_count": len(paths),
            "update_type": update_type,
        }


def _hex_revision(value: object, field: str) -> str:
    revision = str(value or "").strip().lower()
    if len(revision) != 64:
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    try:
        bytes.fromhex(revision)
    except ValueError as exc:
        raise ValueError(f"{field} must be a SHA-256 hex digest") from exc
    return revision


__all__ = [
    "EMBY_PATHS_CHANGED_EFFECT",
    "EmbyPathChangeEffectDelivery",
    "EmbyPathChangeGateway",
    "stage_emby_path_change_effects",
]
