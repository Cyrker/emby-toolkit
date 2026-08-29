from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork


class SubscriptionQuotaStatus(StrEnum):
    RESERVED = "reserved"
    COMMITTED = "committed"
    RELEASED = "released"


class SubscriptionQuotaExhaustedError(RuntimeError):
    pass


class SubscriptionQuotaStateError(RuntimeError):
    pass


class SubscriptionRuntimeStateConflictError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SubscriptionQuotaReservation:
    id: UUID
    reservation_key: str
    operation_id: UUID | None
    quota_date: date
    units: int
    cap_snapshot: int
    status: SubscriptionQuotaStatus
    state_revision: int
    expires_at: datetime
    committed_at: datetime | None
    released_at: datetime | None


@dataclass(frozen=True, slots=True)
class SubscriptionQuotaAvailability:
    quota_date: date
    daily_cap: int
    reserved_units: int
    committed_units: int

    @property
    def available_units(self) -> int:
        return max(0, self.daily_cap - self.reserved_units - self.committed_units)


@dataclass(frozen=True, slots=True)
class SubscriptionRuntimeState:
    key: str
    value: Mapping[str, Any]
    schema_version: int
    revision: int
    created_at: datetime
    updated_at: datetime


class PostgresSubscriptionRuntimeRepository:
    """Transactional P7 runtime state and quota reservation boundary."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def reserve_quota(
        self,
        idempotency_key: str,
        *,
        quota_date: date,
        daily_cap: int,
        units: int = 1,
        ttl_seconds: float = 3600,
        operation_id: UUID | None = None,
        now: datetime | None = None,
    ) -> SubscriptionQuotaReservation:
        key = _quota_key(idempotency_key, quota_date)
        cap = _non_negative_int(daily_cap, "daily_cap")
        requested_units = _positive_int(units, "units")
        ttl = _positive_seconds(ttl_seconds, "ttl_seconds")
        current_time = _utc(now)
        expires_at = current_time + timedelta(seconds=ttl)

        with self._unit_of_work() as uow:
            result = _reserve_quota_in_uow(
                uow,
                reservation_key=key,
                quota_date=quota_date,
                daily_cap=cap,
                units=requested_units,
                expires_at=expires_at,
                operation_id=operation_id,
                now=current_time,
            )
            uow.commit()
        return result

    def commit_quota(
        self,
        reservation_id: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionQuotaReservation:
        return self._transition_quota(reservation_id, SubscriptionQuotaStatus.COMMITTED, now=_utc(now))

    def release_quota(
        self,
        reservation_id: UUID,
        *,
        now: datetime | None = None,
    ) -> SubscriptionQuotaReservation:
        return self._transition_quota(reservation_id, SubscriptionQuotaStatus.RELEASED, now=_utc(now))

    def quota_availability(
        self,
        quota_date: date,
        *,
        daily_cap: int,
        now: datetime | None = None,
    ) -> SubscriptionQuotaAvailability:
        cap = _non_negative_int(daily_cap, "daily_cap")
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            _lock_quota_date(uow, quota_date)
            self._release_expired_for_date(uow, quota_date, current_time)
            availability = self._availability_in_uow(uow, quota_date, cap)
            self._sync_legacy_projection(
                uow,
                quota_date,
                cap,
                availability.reserved_units,
                availability.committed_units,
            )
            uow.commit()
        return availability

    def quota_snapshot(
        self,
        quota_date: date,
        *,
        daily_cap: int,
    ) -> SubscriptionQuotaAvailability:
        """Read quota usage without expiring reservations or updating the legacy projection."""
        cap = _non_negative_int(daily_cap, "daily_cap")
        with self._unit_of_work() as uow:
            return self._availability_in_uow(uow, quota_date, cap)

    def expire_quota_reservations(
        self,
        *,
        limit: int = 100,
        now: datetime | None = None,
    ) -> int:
        bounded_limit = max(1, min(int(limit), 1000))
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            dates = tuple(
                uow.session.execute(
                    text(
                        """
                        SELECT DISTINCT quota_date
                        FROM subscription_quota_reservations
                        WHERE status = 'reserved' AND expires_at <= :now
                        ORDER BY quota_date
                        LIMIT :limit
                        """
                    ),
                    {"now": current_time, "limit": bounded_limit},
                ).scalars()
            )
            released = 0
            for quota_day in dates:
                _lock_quota_date(uow, quota_day)
                released += self._release_expired_for_date(uow, quota_day, current_time)
                cap = self._projected_cap_for_date(
                    uow,
                    quota_day,
                    fallback=self._cap_for_date(uow, quota_day),
                )
                availability = self._availability_in_uow(uow, quota_day, cap)
                self._sync_legacy_projection(
                    uow,
                    quota_day,
                    cap,
                    availability.reserved_units,
                    availability.committed_units,
                )
            uow.commit()
        return released

    def read_runtime_state(self, key: str) -> SubscriptionRuntimeState | None:
        state_key = _state_key(key)
        row: Mapping[Any, Any] | None = None
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM subscription_runtime_states WHERE state_key = :state_key"),
                    {"state_key": state_key},
                )
                .mappings()
                .first()
            )
        return _runtime_state(row) if row is not None else None

    def create_runtime_state(
        self,
        key: str,
        value: Mapping[str, Any],
        *,
        schema_version: int = 1,
        now: datetime | None = None,
    ) -> SubscriptionRuntimeState:
        state_key = _state_key(key)
        state_json = _json_object(value)
        version = _positive_int(schema_version, "schema_version")
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                    INSERT INTO subscription_runtime_states (
                        state_key,
                        state_json,
                        schema_version,
                        state_revision,
                        created_at,
                        updated_at
                    ) VALUES (
                        :state_key,
                        CAST(:state_json AS jsonb),
                        :schema_version,
                        0,
                        :now,
                        :now
                    )
                    ON CONFLICT (state_key) DO NOTHING
                    RETURNING *
                    """
                    ),
                    {
                        "state_key": state_key,
                        "state_json": state_json,
                        "schema_version": version,
                        "now": current_time,
                    },
                )
                .mappings()
                .first()
            )
            if row is None:
                row = (
                    uow.session.execute(
                        text(
                            """
                        SELECT *
                        FROM subscription_runtime_states
                        WHERE state_key = :state_key
                        FOR UPDATE
                        """
                        ),
                        {"state_key": state_key},
                    )
                    .mappings()
                    .one()
                )
            projection = _runtime_state(row)
            uow.commit()
        return projection

    def compare_and_swap_runtime_state(
        self,
        key: str,
        *,
        expected_revision: int,
        value: Mapping[str, Any],
        schema_version: int,
        now: datetime | None = None,
    ) -> SubscriptionRuntimeState:
        state_key = _state_key(key)
        revision = _non_negative_int(expected_revision, "expected_revision")
        version = _positive_int(schema_version, "schema_version")
        state_json = _json_object(value)
        current_time = _utc(now)
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                    UPDATE subscription_runtime_states
                    SET state_json = CAST(:state_json AS jsonb),
                        schema_version = :schema_version,
                        state_revision = state_revision + 1,
                        updated_at = :now
                    WHERE state_key = :state_key
                      AND state_revision = :expected_revision
                    RETURNING *
                    """
                    ),
                    {
                        "state_key": state_key,
                        "state_json": state_json,
                        "schema_version": version,
                        "expected_revision": revision,
                        "now": current_time,
                    },
                )
                .mappings()
                .first()
            )
            if row is None:
                raise SubscriptionRuntimeStateConflictError(
                    f"subscription runtime state revision conflict: key={state_key} expected={revision}"
                )
            projection = _runtime_state(row)
            uow.commit()
        return projection

    def _transition_quota(
        self,
        reservation_id: UUID,
        target: SubscriptionQuotaStatus,
        *,
        now: datetime,
    ) -> SubscriptionQuotaReservation:
        with self._unit_of_work() as uow:
            quota_day = uow.session.scalar(
                text("SELECT quota_date FROM subscription_quota_reservations WHERE id = :id"),
                {"id": reservation_id},
            )
            if quota_day is None:
                raise SubscriptionQuotaStateError("quota reservation was not found")
            _lock_quota_date(uow, quota_day)
            row = (
                uow.session.execute(
                    text(
                        """
                    SELECT *
                    FROM subscription_quota_reservations
                    WHERE id = :id
                    FOR UPDATE
                    """
                    ),
                    {"id": reservation_id},
                )
                .mappings()
                .one()
            )
            current = _quota_reservation(row)
            if current.status is target:
                result = current
            elif current.status is not SubscriptionQuotaStatus.RESERVED:
                raise SubscriptionQuotaStateError(
                    f"quota reservation cannot transition from {current.status.value} to {target.value}"
                )
            else:
                timestamp_column = "committed_at" if target is SubscriptionQuotaStatus.COMMITTED else "released_at"
                updated = (
                    uow.session.execute(
                        text(
                            f"""
                        UPDATE subscription_quota_reservations
                        SET status = :status,
                            state_revision = state_revision + 1,
                            {timestamp_column} = :now,
                            updated_at = :now
                        WHERE id = :id AND status = 'reserved'
                        RETURNING *
                        """
                        ),
                        {"id": reservation_id, "status": target.value, "now": now},
                    )
                    .mappings()
                    .one()
                )
                cap = self._projected_cap_for_date(
                    uow,
                    quota_day,
                    fallback=current.cap_snapshot,
                )
                availability = self._availability_in_uow(uow, quota_day, cap)
                self._sync_legacy_projection(
                    uow,
                    quota_day,
                    cap,
                    availability.reserved_units,
                    availability.committed_units,
                )
                result = _quota_reservation(updated)
            uow.commit()
        return result

    @staticmethod
    def _release_expired_for_date(
        uow: SqlAlchemyUnitOfWork,
        quota_date: date,
        now: datetime,
    ) -> int:
        rows = uow.session.execute(
            text(
                """
                UPDATE subscription_quota_reservations AS quota
                SET status = 'released',
                    state_revision = state_revision + 1,
                    released_at = :now,
                    updated_at = :now
                WHERE quota.quota_date = :quota_date
                  AND quota.status = 'reserved'
                  AND quota.expires_at <= :now
                  AND (
                      quota.operation_id IS NULL
                      OR EXISTS (
                          SELECT 1
                          FROM subscription_operations AS operation
                          WHERE operation.id = quota.operation_id
                            AND operation.status IN ('confirmed','rejected','indeterminate','cancelled')
                      )
                  )
                RETURNING quota.id
                """
            ),
            {"quota_date": quota_date, "now": now},
        ).all()
        return len(rows)

    @staticmethod
    def _availability_in_uow(
        uow: SqlAlchemyUnitOfWork,
        quota_date: date,
        daily_cap: int,
    ) -> SubscriptionQuotaAvailability:
        row = (
            uow.session.execute(
                text(
                    """
                SELECT
                    COALESCE(SUM(units) FILTER (WHERE status = 'reserved'), 0)::integer AS reserved_units,
                    COALESCE(SUM(units) FILTER (WHERE status = 'committed'), 0)::integer AS committed_units
                FROM subscription_quota_reservations
                WHERE quota_date = :quota_date
                """
                ),
                {"quota_date": quota_date},
            )
            .mappings()
            .one()
        )
        return SubscriptionQuotaAvailability(
            quota_date,
            daily_cap,
            int(row["reserved_units"]),
            int(row["committed_units"]),
        )

    @staticmethod
    def _cap_for_date(uow: SqlAlchemyUnitOfWork, quota_date: date) -> int:
        value = uow.session.scalar(
            text(
                """
                SELECT cap_snapshot
                FROM subscription_quota_reservations
                WHERE quota_date = :quota_date
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            ),
            {"quota_date": quota_date},
        )
        return int(value or 0)

    @staticmethod
    def _projected_cap_for_date(
        uow: SqlAlchemyUnitOfWork,
        quota_date: date,
        *,
        fallback: int,
    ) -> int:
        row = (
            uow.session.execute(
                text(
                    """
                SELECT
                    value_json ->> 'last_reset_date' AS quota_date,
                    value_json ->> 'max_quota_on_reset' AS daily_cap
                FROM app_settings
                WHERE setting_key = 'subscription_quota_state'
                FOR UPDATE
                """
                )
            )
            .mappings()
            .first()
        )
        if row is None or str(row["quota_date"] or "") != quota_date.isoformat():
            return max(0, int(fallback))
        try:
            return max(0, int(row["daily_cap"]))
        except (TypeError, ValueError):
            return max(0, int(fallback))

    @staticmethod
    def _sync_legacy_projection(
        uow: SqlAlchemyUnitOfWork,
        quota_date: date,
        daily_cap: int,
        reserved_units: int,
        committed_units: int,
    ) -> None:
        remaining = max(0, daily_cap - reserved_units - committed_units)
        projection = json.dumps(
            {
                "current_quota": remaining,
                "last_reset_date": quota_date.isoformat(),
                "max_quota_on_reset": daily_cap,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        uow.session.execute(
            text(
                """
                INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                VALUES ('subscription_quota_state', CAST(:value AS jsonb), NOW())
                ON CONFLICT (setting_key) DO UPDATE SET
                    value_json = EXCLUDED.value_json,
                    last_updated_at = NOW()
                """
            ),
            {"value": projection},
        )


def _reserve_quota_in_uow(
    uow: SqlAlchemyUnitOfWork,
    *,
    reservation_key: str,
    quota_date: date,
    daily_cap: int,
    units: int,
    expires_at: datetime,
    operation_id: UUID | None,
    now: datetime,
) -> SubscriptionQuotaReservation:
    _lock_quota_date(uow, quota_date)
    PostgresSubscriptionRuntimeRepository._release_expired_for_date(uow, quota_date, now)
    existing = (
        uow.session.execute(
            text(
                """
                SELECT *
                FROM subscription_quota_reservations
                WHERE reservation_key = :reservation_key
                FOR UPDATE
                """
            ),
            {"reservation_key": reservation_key},
        )
        .mappings()
        .first()
    )
    if existing is not None:
        projection = _quota_reservation(existing)
        if operation_id is not None and projection.operation_id not in {None, operation_id}:
            raise SubscriptionQuotaStateError("quota reservation is bound to another operation")
        if projection.quota_date != quota_date or projection.units != units:
            raise SubscriptionQuotaStateError("quota reservation idempotency identity conflict")
        availability = PostgresSubscriptionRuntimeRepository._availability_in_uow(
            uow,
            quota_date,
            daily_cap,
        )
        PostgresSubscriptionRuntimeRepository._sync_legacy_projection(
            uow,
            quota_date,
            daily_cap,
            availability.reserved_units,
            availability.committed_units,
        )
        return projection

    availability = PostgresSubscriptionRuntimeRepository._availability_in_uow(
        uow,
        quota_date,
        daily_cap,
    )
    if units > availability.available_units:
        raise SubscriptionQuotaExhaustedError(
            f"subscription quota exhausted: requested={units} available={availability.available_units}"
        )
    reservation_id = uuid5(NAMESPACE_URL, f"etk:subscription-quota:{reservation_key}")
    row = (
        uow.session.execute(
            text(
                """
                INSERT INTO subscription_quota_reservations (
                    id,
                    reservation_key,
                    operation_id,
                    quota_date,
                    units,
                    cap_snapshot,
                    status,
                    state_revision,
                    expires_at,
                    created_at,
                    updated_at
                ) VALUES (
                    :id,
                    :reservation_key,
                    :operation_id,
                    :quota_date,
                    :units,
                    :cap_snapshot,
                    'reserved',
                    0,
                    :expires_at,
                    :now,
                    :now
                )
                RETURNING *
                """
            ),
            {
                "id": reservation_id,
                "reservation_key": reservation_key,
                "operation_id": operation_id,
                "quota_date": quota_date,
                "units": units,
                "cap_snapshot": daily_cap,
                "expires_at": expires_at,
                "now": now,
            },
        )
        .mappings()
        .one()
    )
    PostgresSubscriptionRuntimeRepository._sync_legacy_projection(
        uow,
        quota_date,
        daily_cap,
        availability.reserved_units + units,
        availability.committed_units,
    )
    return _quota_reservation(row)


def _lock_quota_date(uow: SqlAlchemyUnitOfWork, quota_date: date) -> None:
    uow.session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"etk:subscription-quota:{quota_date.isoformat()}"},
    )


def _quota_key(idempotency_key: str, quota_date: date) -> str:
    normalized = str(idempotency_key or "").strip()
    if not normalized:
        raise ValueError("idempotency_key must not be empty")
    return hashlib.sha256(f"{quota_date.isoformat()}\0{normalized}".encode()).hexdigest()


def _state_key(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError("state key must not be empty")
    return normalized


def _json_object(value: Mapping[str, Any]) -> str:
    if not isinstance(value, Mapping):
        raise TypeError("runtime state must be a mapping")
    return json.dumps(
        dict(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _positive_int(value: int, name: str) -> int:
    normalized = int(value)
    if normalized < 1:
        raise ValueError(f"{name} must be positive")
    return normalized


def _non_negative_int(value: int, name: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{name} must not be negative")
    return normalized


def _positive_seconds(value: float, name: str) -> float:
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{name} must be positive and finite")
    return normalized


def _utc(value: datetime | None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return current.astimezone(UTC)


def _quota_reservation(row: Mapping[Any, Any]) -> SubscriptionQuotaReservation:
    return SubscriptionQuotaReservation(
        id=row["id"],
        reservation_key=str(row["reservation_key"]),
        operation_id=row["operation_id"],
        quota_date=row["quota_date"],
        units=int(row["units"]),
        cap_snapshot=int(row["cap_snapshot"]),
        status=SubscriptionQuotaStatus(str(row["status"])),
        state_revision=int(row["state_revision"]),
        expires_at=row["expires_at"],
        committed_at=row["committed_at"],
        released_at=row["released_at"],
    )


def _runtime_state(row: Mapping[Any, Any]) -> SubscriptionRuntimeState:
    return SubscriptionRuntimeState(
        key=str(row["state_key"]),
        value=dict(row["state_json"]),
        schema_version=int(row["schema_version"]),
        revision=int(row["state_revision"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


__all__ = [
    "PostgresSubscriptionRuntimeRepository",
    "SubscriptionQuotaAvailability",
    "SubscriptionQuotaExhaustedError",
    "SubscriptionQuotaReservation",
    "SubscriptionQuotaStateError",
    "SubscriptionQuotaStatus",
    "SubscriptionRuntimeState",
    "SubscriptionRuntimeStateConflictError",
]
