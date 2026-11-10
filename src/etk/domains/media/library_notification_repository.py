from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork

from .effect_execution import MediaEffectDeliveryRetryable
from .effects import MediaEffectProjection, SqlAlchemyMediaEffectRepository

LIBRARY_BATCH_EFFECT = "telegram.library.batch"
_QUIET_SECONDS = 30


def library_notification_member_key(operation_key: str, item_id: str) -> str:
    return hashlib.sha256(f"{operation_key}:{item_id}".encode()).hexdigest()


class SqlAlchemyLibraryNotificationRepository:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def media(self, item_id: str) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("""
                SELECT overview,poster_path,backdrop_path,asset_details_json
                FROM media_metadata
                WHERE emby_item_ids_json @> CAST(:ids AS jsonb)
                   OR asset_details_json @> CAST(:assets AS jsonb)
                ORDER BY CASE item_type WHEN 'Episode' THEN 0 WHEN 'Movie' THEN 1 ELSE 2 END,
                         in_library DESC, date_added DESC NULLS LAST
                LIMIT 1
            """),
                    {"ids": json.dumps([item_id]), "assets": json.dumps([{"emby_item_id": item_id}])},
                )
                .mappings()
                .first()
            )
        return dict(row) if row else {}

    def stage(self, member: Mapping[str, Any], *, operation_key: str) -> MediaEffectProjection:
        group = str(member["group_key"])
        member_key = library_notification_member_key(operation_key, str(member["item_id"]))
        lock = int.from_bytes(hashlib.sha256(f"library-notice:{group}".encode()).digest()[:8], signed=True)
        with self._unit_of_work() as uow:
            uow.session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
            previous = uow.session.execute(
                text("""
                SELECT id FROM media_library_notification_batches WHERE members_json ? :key LIMIT 1
            """),
                {"key": member_key},
            ).scalar()
            if previous is not None:
                return self._effect(uow, previous)
            batch = (
                uow.session.execute(
                    text("""
                SELECT id,members_json FROM media_library_notification_batches
                WHERE group_key=:group AND sealed_at IS NULL FOR UPDATE
            """),
                    {"group": group},
                )
                .mappings()
                .first()
            )
            now = datetime.now(UTC)
            deadline = now + timedelta(seconds=_QUIET_SECONDS)
            if batch is None:
                batch_id = uuid4()
                members = {member_key: dict(member)}
                uow.session.execute(
                    text("""
                    INSERT INTO media_library_notification_batches
                        (id,group_key,group_path,members_json,quiet_until)
                    VALUES (:id,:group,:path,CAST(:members AS jsonb),:deadline)
                """),
                    {
                        "id": batch_id,
                        "group": group,
                        "path": member.get("group_path") or "",
                        "members": json.dumps(members, ensure_ascii=False),
                        "deadline": deadline,
                    },
                )
            else:
                batch_id = batch["id"]
                members = {**batch["members_json"], member_key: dict(member)}
                uow.session.execute(
                    text("""
                        UPDATE media_library_notification_batches
                        SET members_json=CAST(:members AS jsonb),quiet_until=:deadline
                    WHERE id=:id
                """),
                    {"id": batch_id, "members": json.dumps(members, ensure_ascii=False), "deadline": deadline},
                )
            effect = self._effect(uow, batch_id, deadline)
            uow.commit()
        return effect

    @staticmethod
    def _effect(uow: SqlAlchemyUnitOfWork, batch_id: UUID, deadline: datetime | None = None) -> MediaEffectProjection:
        return (
            SqlAlchemyMediaEffectRepository(uow.session)
            .reserve(
                effect_key=f"telegram:library-batch:{batch_id}",
                effect_type=LIBRARY_BATCH_EFFECT,
                aggregate_type="library_notification_batch",
                aggregate_id=str(batch_id),
                payload={"batch_id": str(batch_id)},
                available_at=deadline,
            )
            .effect
        )

    def snapshot(self, batch_id: str) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("""
                SELECT * FROM media_library_notification_batches WHERE id=:id FOR UPDATE
            """),
                    {"id": UUID(batch_id)},
                )
                .mappings()
                .one()
            )
            if row["sealed_at"] is None:
                remaining = (row["quiet_until"] - datetime.now(UTC)).total_seconds()
                if remaining > 0 or self._has_pending_work(uow, row["group_path"]):
                    raise MediaEffectDeliveryRetryable(
                        "等待同批媒体处理完成后合并通知", retry_after_seconds=max(remaining, _QUIET_SECONDS)
                    )
                # 冻结后不再加入新成员；发送不确定时仍由 effect 账本阻止重复发送。
                uow.session.execute(
                    text("""
                    UPDATE media_library_notification_batches SET sealed_at=NOW() WHERE id=:id
                """),
                    {"id": row["id"]},
                )
                uow.commit()
            return list(row["members_json"].values())

    def mark_retry_success(self, member_key: str, retry_count: int) -> dict[str, Any]:
        key = str(member_key or "").strip()
        if not key:
            raise ValueError("library notification member key must not be empty")
        if type(retry_count) is not int or not 1 <= retry_count <= 3:
            raise ValueError("metadata retry count must be between 1 and 3")
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("""
                    SELECT id,members_json FROM media_library_notification_batches
                    WHERE members_json ? :key FOR UPDATE
                """),
                    {"key": key},
                )
                .mappings()
                .first()
            )
            if row is None:
                return {"updated": False, "members": [], "messages": []}
            members = {str(name): dict(member) for name, member in row["members_json"].items()}
            member = members[key]
            previous_count = member.get("retry_success_count")
            if type(previous_count) is int and previous_count > 0:
                retry_count = min(retry_count, previous_count)
            member["succeeded"] = True
            member["retry_success_count"] = retry_count
            members[key] = member
            uow.session.execute(
                text("""
                    UPDATE media_library_notification_batches
                    SET members_json=CAST(:members AS jsonb)
                    WHERE id=:id
                """),
                {
                    "id": row["id"],
                    "members": json.dumps(members, ensure_ascii=False),
                },
            )
            message_effect = (
                uow.session.execute(
                    text("""
                    SELECT status,result_json FROM media_effects WHERE effect_key=:effect_key
                """),
                    {"effect_key": f"telegram:library-batch:{row['id']}"},
                )
                .mappings()
                .one_or_none()
            )
            uow.commit()
        result = message_effect["result_json"] if message_effect and message_effect["status"] == "confirmed" else {}
        return {
            "updated": True,
            "batch_id": str(row["id"]),
            "members": list(members.values()),
            "messages": list((result or {}).get("messages") or []),
        }

    @staticmethod
    def _has_pending_work(uow: SqlAlchemyUnitOfWork, path: str) -> bool:
        if not path:
            return False
        return bool(
            uow.session.execute(
                text("""
            SELECT EXISTS (
                SELECT 1 FROM ingestion_monitor_items
                WHERE (path=:path OR starts_with(path,:prefix))
                  AND status IN ('pending','stability_wait','ready','claimed','retryable')
                UNION ALL
                SELECT 1 FROM job_runs j
                WHERE j.status IN ('queued','running','retry_wait','cancel_requested')
                  AND j.task_key='native-operation:media.reprocess-item'
                  AND j.request_payload_json->'parameters'->>'item_id' IN (
                      SELECT emby_item_id FROM ingestion_pending_bindings
                      WHERE path=:path OR starts_with(path,:prefix)
                  )
            )
        """),
                {"path": path, "prefix": path.rstrip("/") + "/"},
            ).scalar()
        )
