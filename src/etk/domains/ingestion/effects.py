from __future__ import annotations

import hashlib
from typing import Literal

from sqlalchemy.orm import Session

from etk.domains.media.effects import ReserveMediaEffectResult, SqlAlchemyMediaEffectRepository
from etk.platform.jobs.json_payload import canonical_json

from .monitoring import MonitorEventType, MonitorItemProjection

EMBY_PATHS_CHANGED_EFFECT = "emby.library.paths_changed"


def stage_ingestion_path_notification(
    session: Session,
    item: MonitorItemProjection,
    *,
    authorization_revision: str,
) -> ReserveMediaEffectResult:
    authorization = _hex_revision(authorization_revision, "authorization_revision")
    update_type: Literal["Modified", "Deleted"] = (
        "Deleted" if item.event_type is MonitorEventType.DELETED else "Modified"
    )
    payload = {
        "paths": [item.path],
        "update_type": update_type,
        "emby_authorization_revision": authorization,
        "operation_revision": item.request_hash.hex(),
    }
    digest = hashlib.sha256(canonical_json(payload, field="ingestion path notification").encode()).hexdigest()
    return SqlAlchemyMediaEffectRepository(session).reserve(
        effect_key=f"p6:ingestion:{EMBY_PATHS_CHANGED_EFFECT}:{item.id}:{digest}",
        effect_type=EMBY_PATHS_CHANGED_EFFECT,
        aggregate_type="ingestion_monitor",
        aggregate_id=str(item.id),
        payload=payload,
    )


def _hex_revision(value: object, field: str) -> str:
    revision = str(value or "").strip().lower()
    if len(revision) != 64:
        raise ValueError(f"{field} must be a SHA-256 hex digest")
    try:
        bytes.fromhex(revision)
    except ValueError as exc:
        raise ValueError(f"{field} must be a SHA-256 hex digest") from exc
    return revision


__all__ = ["EMBY_PATHS_CHANGED_EFFECT", "stage_ingestion_path_notification"]
