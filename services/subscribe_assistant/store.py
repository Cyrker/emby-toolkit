from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from database.connection import get_db_connection


class SnapshotClaimLostError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SnapshotClaim:
    snapshot_id: int
    state_revision: int
    job_id: UUID
    attempt_id: UUID
    worker_id: str
    claim_token: UUID
    lease_expires_at: datetime
    snapshot: dict[str, Any]


def read_state(key: str) -> dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT state_json FROM subscribe_assistant_state WHERE state_key = %s",
                (key,),
            )
            row = cursor.fetchone()
    if not row:
        return {}
    value = row.get("state_json") if isinstance(row, dict) else row[0]
    return value if isinstance(value, dict) else {}


def write_state(key: str, value: dict[str, Any]) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (%s, %s::jsonb, NOW())
                ON CONFLICT (state_key) DO UPDATE SET
                    state_json = EXCLUDED.state_json,
                    updated_at = NOW()
                """,
                (key, json.dumps(value or {}, ensure_ascii=False)),
            )
        conn.commit()


def update_state(key: str, updater) -> dict[str, Any]:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (%s, '{}'::jsonb, NOW())
                ON CONFLICT (state_key) DO NOTHING
                """,
                (key,),
            )
            cursor.execute(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = %s
                FOR UPDATE
                """,
                (key,),
            )
            row = cursor.fetchone()
            value = row.get("state_json") if isinstance(row, dict) else row[0]
            current = value if isinstance(value, dict) else {}
            updated = updater(dict(current)) or {}
            if not isinstance(updated, dict):
                raise TypeError("subscribe assistant state updater must return an object")
            cursor.execute(
                """
                UPDATE subscribe_assistant_state
                SET state_json = %s::jsonb, updated_at = NOW()
                WHERE state_key = %s
                """,
                (json.dumps(updated, ensure_ascii=False), key),
            )
        conn.commit()
    return updated


def upsert_snapshot(
    *,
    tmdb_id: str,
    item_type: str = "Series",
    season_number: int | None = None,
    subscribe_id: int | None = None,
    scope_total: int = 0,
    scope: dict[str, Any] | None = None,
    subscribe: dict[str, Any] | None = None,
) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_snapshots (
                    tmdb_id, item_type, season_number, subscribe_id,
                    scope_total, scope_json, subscribe_json, created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, NOW())
                """,
                (
                    str(tmdb_id),
                    item_type,
                    season_number,
                    subscribe_id,
                    int(scope_total or 0),
                    json.dumps(scope or {}, ensure_ascii=False),
                    json.dumps(subscribe or {}, ensure_ascii=False),
                ),
            )
        conn.commit()


def claim_next_snapshot(
    hours: int,
    *,
    job_id: UUID,
    attempt_id: UUID,
    worker_id: str,
    lease_seconds: float,
) -> SnapshotClaim | None:
    normalized_worker = str(worker_id or "").strip()
    if not normalized_worker:
        raise ValueError("subscribe assistant snapshot worker_id must not be empty")
    normalized_lease = max(float(lease_seconds), 1.0)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            _lock_current_snapshot_attempt(cursor, job_id, attempt_id, normalized_worker)
            cursor.execute(
                """
                SELECT snapshot.*
                FROM subscribe_assistant_snapshots AS snapshot
                WHERE (snapshot.last_checked_at IS NULL
                       OR last_checked_at <= NOW() - (%s * INTERVAL '1 hour'))
                  AND (
                      snapshot.claim_token IS NULL
                      OR snapshot.lease_expires_at <= NOW()
                      OR (
                          snapshot.job_id = %s
                          AND snapshot.attempt_id <> %s
                          AND EXISTS (
                              SELECT 1
                              FROM job_attempts AS previous_attempt
                              WHERE previous_attempt.id = snapshot.attempt_id
                                AND previous_attempt.job_id = snapshot.job_id
                                AND previous_attempt.status IN (
                                    'succeeded', 'failed', 'timed_out', 'lost', 'cancelled'
                                )
                          )
                      )
                  )
                ORDER BY COALESCE(snapshot.last_checked_at, snapshot.created_at) ASC, snapshot.id ASC
                FOR UPDATE OF snapshot SKIP LOCKED
                LIMIT 1
                """,
                (max(1, int(hours or 1)), str(job_id), str(attempt_id)),
            )
            candidate = cursor.fetchone()
            if candidate is None:
                conn.commit()
                return None
            candidate = dict(candidate)
            claim_token = uuid4()
            cursor.execute(
                """
                UPDATE subscribe_assistant_snapshots
                SET job_id = %s,
                    attempt_id = %s,
                    worker_id = %s,
                    claim_token = %s,
                    claimed_at = NOW(),
                    lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                    state_revision = state_revision + 1
                WHERE id = %s
                  AND state_revision = %s
                RETURNING *
                """,
                (
                    str(job_id),
                    str(attempt_id),
                    normalized_worker,
                    str(claim_token),
                    normalized_lease,
                    int(candidate["id"]),
                    int(candidate.get("state_revision") or 0),
                ),
            )
            row = cursor.fetchone()
            if row is None:
                raise SnapshotClaimLostError("subscribe assistant snapshot claim update was lost")
        conn.commit()
    return _snapshot_claim(dict(row))


def renew_snapshot_claim(claim: SnapshotClaim, *, lease_seconds: float) -> SnapshotClaim:
    normalized_lease = max(float(lease_seconds), 1.0)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            _lock_current_snapshot_attempt(cursor, claim.job_id, claim.attempt_id, claim.worker_id)
            cursor.execute(
                """
                UPDATE subscribe_assistant_snapshots
                SET lease_expires_at = NOW() + (%s * INTERVAL '1 second'),
                    state_revision = state_revision + 1
                WHERE id = %s
                  AND claim_token = %s
                  AND job_id = %s
                  AND attempt_id = %s
                  AND worker_id = %s
                  AND state_revision = %s
                  AND lease_expires_at > NOW()
                RETURNING *
                """,
                (
                    normalized_lease,
                    claim.snapshot_id,
                    str(claim.claim_token),
                    str(claim.job_id),
                    str(claim.attempt_id),
                    claim.worker_id,
                    claim.state_revision,
                ),
            )
            row = cursor.fetchone()
            if row is None:
                raise SnapshotClaimLostError("subscribe assistant snapshot claim is no longer current")
        conn.commit()
    return _snapshot_claim(dict(row))


def mark_snapshot_checked(claim: SnapshotClaim) -> None:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            _lock_current_snapshot_attempt(cursor, claim.job_id, claim.attempt_id, claim.worker_id)
            cursor.execute(
                """
                UPDATE subscribe_assistant_snapshots
                SET last_checked_at = NOW(),
                    job_id = NULL,
                    attempt_id = NULL,
                    worker_id = NULL,
                    claim_token = NULL,
                    claimed_at = NULL,
                    lease_expires_at = NULL,
                    state_revision = state_revision + 1
                WHERE id = %s
                  AND claim_token = %s
                  AND job_id = %s
                  AND attempt_id = %s
                  AND worker_id = %s
                  AND state_revision = %s
                  AND lease_expires_at > NOW()
                """,
                (
                    claim.snapshot_id,
                    str(claim.claim_token),
                    str(claim.job_id),
                    str(claim.attempt_id),
                    claim.worker_id,
                    claim.state_revision,
                ),
            )
            if cursor.rowcount != 1:
                raise SnapshotClaimLostError("subscribe assistant snapshot claim is no longer current")
        conn.commit()


def _lock_current_snapshot_attempt(cursor, job_id: UUID, attempt_id: UUID, worker_id: str) -> None:
    cursor.execute(
        """
        SELECT id, status, current_attempt
        FROM job_runs
        WHERE id = %s
        FOR UPDATE
        """,
        (str(job_id),),
    )
    job = cursor.fetchone()
    cursor.execute(
        """
        SELECT id, job_id, attempt_no, status, worker_id,
               lease_expires_at > NOW() AS lease_active
        FROM job_attempts
        WHERE id = %s
        FOR UPDATE
        """,
        (str(attempt_id),),
    )
    attempt = cursor.fetchone()
    if not isinstance(job, dict) or not isinstance(attempt, dict):
        raise SnapshotClaimLostError("subscribe assistant snapshot attempt is no longer current")
    owned = (
        str(attempt.get("job_id")) == str(job_id)
        and int(attempt.get("attempt_no") or 0) == int(job.get("current_attempt") or 0)
        and str(job.get("status") or "") in {"running", "cancel_requested"}
        and str(attempt.get("status") or "") == "running"
        and str(attempt.get("worker_id") or "") == worker_id
        and attempt.get("lease_active") is True
    )
    if not owned:
        raise SnapshotClaimLostError("subscribe assistant snapshot attempt is no longer current")


def _snapshot_claim(row: dict[str, Any]) -> SnapshotClaim:
    lease_expires_at = row.get("lease_expires_at")
    if not isinstance(lease_expires_at, datetime):
        raise SnapshotClaimLostError("subscribe assistant snapshot lease is invalid")
    return SnapshotClaim(
        snapshot_id=int(row["id"]),
        state_revision=int(row.get("state_revision") or 0),
        job_id=UUID(str(row.get("job_id"))),
        attempt_id=UUID(str(row.get("attempt_id"))),
        worker_id=str(row.get("worker_id") or ""),
        claim_token=UUID(str(row.get("claim_token"))),
        lease_expires_at=lease_expires_at,
        snapshot=row,
    )


def get_latest_snapshot(
    *,
    tmdb_id: str,
    season_number: int | None = None,
    subscribe_id: int | None = None,
) -> dict[str, Any] | None:
    conditions = ["tmdb_id = %s"]
    params: list[object] = [str(tmdb_id)]
    if season_number is not None:
        conditions.append("season_number = %s")
        params.append(int(season_number))
    if subscribe_id is not None:
        conditions.append("subscribe_id = %s")
        params.append(int(subscribe_id))

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT *
                FROM subscribe_assistant_snapshots
                WHERE {" AND ".join(conditions)}
                ORDER BY created_at DESC
                LIMIT 1
                """,
                tuple(params),
            )
            row = cursor.fetchone()
            return dict(row) if row else None


def cleanup_snapshots(retention_days: int) -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                DELETE FROM subscribe_assistant_snapshots
                WHERE created_at < NOW() - (%s || ' days')::interval
                  AND (claim_token IS NULL OR lease_expires_at <= NOW())
                """,
                (max(1, int(retention_days or 1)),),
            )
            count = cursor.rowcount
        conn.commit()
    return count


def record_deleted_resource(
    fingerprint: str,
    *,
    tmdb_id: str | None = None,
    season_number: int | None = None,
    episodes: list[int] | None = None,
    reason: str = "",
    retention_hours: int = 24,
) -> None:
    expires_at = datetime.now(UTC) + timedelta(hours=max(1, int(retention_hours or 1)))
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO subscribe_assistant_delete_records (
                    fingerprint, tmdb_id, season_number, episode_json, reason, created_at, expires_at
                )
                VALUES (%s, %s, %s, %s::jsonb, %s, NOW(), %s)
                ON CONFLICT (fingerprint) DO UPDATE SET
                    reason = EXCLUDED.reason,
                    expires_at = EXCLUDED.expires_at,
                    created_at = NOW()
                """,
                (
                    fingerprint,
                    tmdb_id,
                    season_number,
                    json.dumps(episodes or [], ensure_ascii=False),
                    reason,
                    expires_at,
                ),
            )
        conn.commit()


def has_deleted_resource(fingerprint: str) -> bool:
    if not fingerprint:
        return False
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM subscribe_assistant_delete_records
                WHERE fingerprint = %s
                  AND (expires_at IS NULL OR expires_at > NOW())
                LIMIT 1
                """,
                (fingerprint,),
            )
            return cursor.fetchone() is not None


def cleanup_delete_records() -> int:
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM subscribe_assistant_delete_records WHERE expires_at <= NOW()")
            count = cursor.rowcount
        conn.commit()
    return count
