from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.orm import Session

from etk.platform.jobs.json_payload import canonical_json, canonical_json_hash, normalize_json_object
from etk.platform.jobs.types import JsonObject, UnitOfWorkFactory

MEDIA_EFFECT_TOPIC = "media.effect.requested"


class MediaEffectStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RETRYABLE = "retryable"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"

    @property
    def terminal(self) -> bool:
        return self in {
            MediaEffectStatus.CONFIRMED,
            MediaEffectStatus.REJECTED,
            MediaEffectStatus.INDETERMINATE,
        }


class MediaEffectError(RuntimeError):
    pass


class MediaEffectConflictError(MediaEffectError):
    pass


class MediaEffectNotFoundError(MediaEffectError):
    pass


class MediaEffectStateError(MediaEffectError):
    pass


@dataclass(frozen=True, slots=True)
class MediaEffectProjection:
    id: UUID
    effect_key: str
    effect_type: str
    aggregate_type: str
    aggregate_id: str
    payload_version: int
    payload: JsonObject
    request_hash: bytes
    status: MediaEffectStatus
    attempts: int
    available_at: datetime
    claimed_by: str | None
    claim_token: UUID | None
    claimed_at: datetime | None
    lease_expires_at: datetime | None
    last_attempt_at: datetime | None
    result: JsonObject | None
    error: JsonObject | None
    finished_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReserveMediaEffectResult:
    effect: MediaEffectProjection
    created: bool


@dataclass(frozen=True, slots=True)
class MediaEffectIntentReservation:
    effect_id: UUID
    created: bool


@dataclass(frozen=True, slots=True)
class MediaEffectTransition:
    effect: MediaEffectProjection
    changed: bool


class SqlAlchemyMediaEffectRepository:
    """Session-bound effect primitive for atomic media write + intent + Outbox UoWs."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def reserve(
        self,
        *,
        effect_key: str,
        effect_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
        available_at: datetime | None = None,
        max_publish_attempts: int = 20,
    ) -> ReserveMediaEffectResult:
        values = _reservation_values(
            effect_key=effect_key,
            effect_type=effect_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
            available_at=available_at,
            max_publish_attempts=max_publish_attempts,
        )
        row = (
            self.session.execute(
                text(
                    """
                    INSERT INTO media_effects (
                        id, effect_key, effect_type, aggregate_type, aggregate_id,
                        payload_version, payload_json, request_hash, status, available_at
                    ) VALUES (
                        :effect_id, :effect_key, :effect_type, :aggregate_type, :aggregate_id,
                        1, CAST(:payload_json AS jsonb), :request_hash, 'pending',
                        COALESCE(:available_at, NOW())
                    )
                    ON CONFLICT (effect_key) DO NOTHING
                    RETURNING *
                    """
                ),
                values,
            )
            .mappings()
            .one_or_none()
        )
        created = row is not None
        if row is None:
            row = (
                self.session.execute(
                    text("SELECT * FROM media_effects WHERE effect_key = :effect_key"),
                    {"effect_key": values["effect_key"]},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise MediaEffectStateError("effect reservation conflicted but no existing effect was visible")
            if bytes(row["request_hash"]) != values["request_hash"]:
                raise MediaEffectConflictError("effect_key was already reserved with a different request")
        else:
            outbox_payload = {
                "effect_id": str(values["effect_id"]),
                "effect_key": values["effect_key"],
                "effect_type": values["effect_type"],
                "aggregate_type": values["aggregate_type"],
                "aggregate_id": values["aggregate_id"],
                "request_hash": values["request_hash"].hex(),
            }
            self.session.execute(
                text(
                    """
                    INSERT INTO outbox_events (
                        id, event_key, topic, aggregate_type, aggregate_id,
                        payload_version, payload_json, status, available_at,
                        max_publish_attempts
                    ) VALUES (
                        :event_id, :event_key, :topic, 'media_effect', :effect_id,
                        1, CAST(:payload_json AS jsonb), 'pending',
                        COALESCE(:available_at, NOW()), :max_publish_attempts
                    )
                    """
                ),
                {
                    "event_id": values["event_id"],
                    "event_key": values["event_key"],
                    "topic": MEDIA_EFFECT_TOPIC,
                    "effect_id": str(values["effect_id"]),
                    "payload_json": canonical_json(outbox_payload, field="media effect outbox payload"),
                    "available_at": values["available_at"],
                    "max_publish_attempts": values["max_publish_attempts"],
                },
            )
        return ReserveMediaEffectResult(effect=_projection(row), created=created)

    def record_confirmed(
        self,
        *,
        effect_key: str,
        effect_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
        result: Mapping[str, Any],
    ) -> ReserveMediaEffectResult:
        """Record a local effect receipt that completed in the surrounding UoW."""
        values = _reservation_values(
            effect_key=effect_key,
            effect_type=effect_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
            available_at=None,
            max_publish_attempts=1,
        )
        normalized_result = normalize_json_object(dict(result), field="media effect result")
        row = (
            self.session.execute(
                text(
                    """
                    INSERT INTO media_effects (
                        id, effect_key, effect_type, aggregate_type, aggregate_id,
                        payload_version, payload_json, request_hash, status,
                        attempts, available_at, last_attempt_at,
                        result_json, finished_at
                    ) VALUES (
                        :effect_id, :effect_key, :effect_type, :aggregate_type, :aggregate_id,
                        1, CAST(:payload_json AS jsonb), :request_hash, 'confirmed',
                        1, NOW(), NOW(), CAST(:result_json AS jsonb), NOW()
                    )
                    ON CONFLICT (effect_key) DO NOTHING
                    RETURNING *
                    """
                ),
                {
                    **values,
                    "result_json": canonical_json(normalized_result, field="media effect result"),
                },
            )
            .mappings()
            .one_or_none()
        )
        created = row is not None
        if row is None:
            row = (
                self.session.execute(
                    text("SELECT * FROM media_effects WHERE effect_key = :effect_key"),
                    {"effect_key": values["effect_key"]},
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                raise MediaEffectStateError("local receipt conflicted but was not visible")
            if bytes(row["request_hash"]) != values["request_hash"]:
                raise MediaEffectConflictError("effect_key was already reserved with a different request")
            if MediaEffectStatus(row["status"]) is not MediaEffectStatus.CONFIRMED:
                raise MediaEffectStateError("local receipt exists without a confirmed result")
        return ReserveMediaEffectResult(effect=_projection(row), created=created)


class MediaEffectDbApiCursor(Protocol):
    def execute(self, query: str, parameters: Mapping[str, Any]) -> Any: ...

    def fetchone(self) -> Any: ...


def reserve_media_effect_dbapi(
    cursor: MediaEffectDbApiCursor,
    *,
    effect_key: str,
    effect_type: str,
    aggregate_type: str,
    aggregate_id: str,
    payload: Mapping[str, Any],
    available_at: datetime | None = None,
    max_publish_attempts: int = 20,
) -> MediaEffectIntentReservation:
    """Reserve an effect on an existing DB-API transaction.

    Legacy P5 metadata persistence still owns the surrounding psycopg transaction.
    This primitive keeps the new durable intent in that same commit until P11 removes
    the compatibility write path.
    """

    values = _reservation_values(
        effect_key=effect_key,
        effect_type=effect_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        payload=payload,
        available_at=available_at,
        max_publish_attempts=max_publish_attempts,
    )
    parameters = {
        **values,
        "effect_id": str(values["effect_id"]),
        "event_id": str(values["event_id"]),
    }
    cursor.execute(
        """
        INSERT INTO media_effects (
            id, effect_key, effect_type, aggregate_type, aggregate_id,
            payload_version, payload_json, request_hash, status, available_at
        ) VALUES (
            CAST(%(effect_id)s AS uuid), %(effect_key)s, %(effect_type)s,
            %(aggregate_type)s, %(aggregate_id)s, 1,
            CAST(%(payload_json)s AS jsonb), %(request_hash)s, 'pending',
            COALESCE(%(available_at)s, NOW())
        )
        ON CONFLICT (effect_key) DO NOTHING
        RETURNING id, request_hash
        """,
        parameters,
    )
    row = cursor.fetchone()
    created = row is not None
    if row is None:
        cursor.execute(
            "SELECT id, request_hash FROM media_effects WHERE effect_key = %(effect_key)s",
            {"effect_key": values["effect_key"]},
        )
        row = cursor.fetchone()
        if row is None:
            raise MediaEffectStateError("effect reservation conflicted but no existing effect was visible")
        if bytes(_dbapi_value(row, "request_hash", 1)) != values["request_hash"]:
            raise MediaEffectConflictError("effect_key was already reserved with a different request")
    else:
        outbox_payload = {
            "effect_id": str(values["effect_id"]),
            "effect_key": values["effect_key"],
            "effect_type": values["effect_type"],
            "aggregate_type": values["aggregate_type"],
            "aggregate_id": values["aggregate_id"],
            "request_hash": values["request_hash"].hex(),
        }
        cursor.execute(
            """
            INSERT INTO outbox_events (
                id, event_key, topic, aggregate_type, aggregate_id,
                payload_version, payload_json, status, available_at,
                max_publish_attempts
            ) VALUES (
                CAST(%(event_id)s AS uuid), %(event_key)s, %(topic)s,
                'media_effect', %(effect_id)s, 1,
                CAST(%(outbox_payload_json)s AS jsonb), 'pending',
                COALESCE(%(available_at)s, NOW()), %(max_publish_attempts)s
            )
            """,
            {
                **parameters,
                "topic": MEDIA_EFFECT_TOPIC,
                "outbox_payload_json": canonical_json(
                    outbox_payload,
                    field="media effect outbox payload",
                ),
            },
        )
    return MediaEffectIntentReservation(
        effect_id=UUID(str(_dbapi_value(row, "id", 0))),
        created=created,
    )


def media_effect_event_key(effect_key: str) -> str:
    normalized = _nonempty(effect_key, "effect_key", maximum=512)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return f"{MEDIA_EFFECT_TOPIC}:{digest}"


def media_effect_request_hash(
    *,
    effect_type: str,
    aggregate_type: str,
    aggregate_id: str,
    payload: Mapping[str, Any],
) -> bytes:
    normalized_payload = normalize_json_object(dict(payload), field="media effect payload")
    envelope = {
        "payload_version": 1,
        "effect_type": _nonempty(effect_type, "effect_type"),
        "aggregate_type": _nonempty(aggregate_type, "aggregate_type"),
        "aggregate_id": _nonempty(aggregate_id, "aggregate_id"),
        "payload": normalized_payload,
    }
    return canonical_json_hash(envelope, field="media effect request")


class PostgresMediaEffectLedger:
    """Durable P5 effect intent and outcome ledger backed by one PostgreSQL UoW."""

    def __init__(self, uow_factory: UnitOfWorkFactory):
        self._uow_factory = uow_factory

    def reserve(
        self,
        *,
        effect_key: str,
        effect_type: str,
        aggregate_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
        available_at: datetime | None = None,
        max_publish_attempts: int = 20,
    ) -> ReserveMediaEffectResult:
        _nonempty(effect_key, "effect_key", maximum=512)
        _nonempty(effect_type, "effect_type")
        _nonempty(aggregate_type, "aggregate_type")
        _nonempty(aggregate_id, "aggregate_id")
        if max_publish_attempts < 1:
            raise ValueError("max_publish_attempts must be positive")
        normalize_json_object(dict(payload), field="media effect payload")
        with self._uow_factory() as uow:
            result = SqlAlchemyMediaEffectRepository(uow.session).reserve(
                effect_key=effect_key,
                effect_type=effect_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                payload=payload,
                available_at=available_at,
                max_publish_attempts=max_publish_attempts,
            )
            uow.commit()
            return result

    def read(self, effect_key: str) -> MediaEffectProjection | None:
        key = _nonempty(effect_key, "effect_key", maximum=512)
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM media_effects WHERE effect_key = :effect_key"),
                    {"effect_key": key},
                )
                .mappings()
                .one_or_none()
            )
            return _projection(row) if row is not None else None

    def read_by_id(self, effect_id: UUID) -> MediaEffectProjection | None:
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM media_effects WHERE id = :effect_id"),
                    {"effect_id": effect_id},
                )
                .mappings()
                .one_or_none()
            )
            return _projection(row) if row is not None else None

    def claim_by_id(
        self,
        effect_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> MediaEffectProjection | None:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        claim_token = uuid4()
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE media_effects
                        SET status = 'claimed', claimed_by = :worker_id,
                            claim_token = :claim_token, claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            attempts = attempts + 1, last_attempt_at = NOW(), updated_at = NOW()
                        WHERE id = :effect_id
                          AND status IN ('pending', 'retryable')
                          AND available_at <= NOW()
                        RETURNING *
                        """
                    ),
                    {
                        "effect_id": effect_id,
                        "worker_id": worker,
                        "claim_token": claim_token,
                        "lease_seconds": lease_seconds,
                    },
                )
                .mappings()
                .one_or_none()
            )
            uow.commit()
        return _projection(row) if row is not None else None

    def claim(
        self,
        *,
        worker_id: str,
        lease_seconds: float,
        limit: int = 1,
    ) -> tuple[MediaEffectProjection, ...]:
        worker = _nonempty(worker_id, "worker_id")
        _positive(lease_seconds, "lease_seconds")
        if limit < 1:
            raise ValueError("limit must be positive")
        claim_token = uuid4()
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH candidates AS (
                            SELECT id
                            FROM media_effects
                            WHERE status IN ('pending', 'retryable')
                              AND available_at <= NOW()
                            ORDER BY available_at, created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE media_effects AS effect
                        SET status = 'claimed', claimed_by = :worker_id,
                            claim_token = :claim_token, claimed_at = NOW(),
                            lease_expires_at = NOW() + make_interval(secs => :lease_seconds),
                            attempts = attempts + 1, last_attempt_at = NOW(), updated_at = NOW()
                        FROM candidates
                        WHERE effect.id = candidates.id
                        RETURNING effect.*
                        """
                    ),
                    {
                        "worker_id": worker,
                        "claim_token": claim_token,
                        "lease_seconds": lease_seconds,
                        "limit": limit,
                    },
                )
                .mappings()
                .all()
            )
            projections = tuple(_projection(row) for row in rows)
            uow.commit()
            return projections

    def expire_stale_claims(self, *, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be positive")
        error = canonical_json(
            {
                "type": "claim_lease_expired",
                "message": "执行器租约过期，外部结果未知，禁止自动重放",
            },
            field="media effect stale claim error",
        )
        with self._uow_factory() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        WITH candidates AS (
                            SELECT id
                            FROM media_effects
                            WHERE status = 'claimed'
                              AND lease_expires_at <= NOW()
                            ORDER BY lease_expires_at, created_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE media_effects AS effect
                        SET status = 'indeterminate',
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = NOW(), updated_at = NOW()
                        FROM candidates
                        WHERE effect.id = candidates.id
                          AND effect.status = 'claimed'
                          AND effect.lease_expires_at <= NOW()
                        RETURNING effect.*
                        """
                    ),
                    {"limit": limit, "error_json": error},
                )
                .mappings()
                .all()
            )
            projections = tuple(_projection(row) for row in rows)
            for projection in projections:
                _update_effect_outbox(uow.session, projection)
            uow.commit()
            return len(projections)

    def requeue_stale_published(
        self,
        *,
        processed_timeout_seconds: float,
        limit: int = 100,
    ) -> int:
        _positive(processed_timeout_seconds, "processed_timeout_seconds")
        if limit < 1:
            raise ValueError("limit must be positive")
        with self._uow_factory() as uow:
            rows = uow.session.execute(
                text(
                    """
                    WITH candidates AS (
                        SELECT event.id
                        FROM outbox_events AS event
                        JOIN media_effects AS effect
                          ON effect.id::text = event.aggregate_id
                        WHERE event.topic = :topic
                          AND event.aggregate_type = 'media_effect'
                          AND event.status = 'published'
                          AND event.processed_at IS NULL
                          AND event.published_at < NOW() - make_interval(secs => :timeout_seconds)
                          AND effect.status IN ('pending', 'retryable')
                        ORDER BY event.published_at, event.created_at
                        LIMIT :limit
                        FOR UPDATE OF event SKIP LOCKED
                    )
                    UPDATE outbox_events AS event
                    SET status = 'pending', published_at = NULL,
                        claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                        updated_at = NOW()
                    FROM candidates
                    WHERE event.id = candidates.id
                    RETURNING event.id
                    """
                ),
                {
                    "topic": MEDIA_EFFECT_TOPIC,
                    "timeout_seconds": processed_timeout_seconds,
                    "limit": limit,
                },
            ).all()
            uow.commit()
            return len(rows)

    def reject_dead_dispatches(self, *, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be positive")
        error = canonical_json(
            {
                "type": "effect_dispatch_exhausted",
                "message": "媒体副作用消息投递失败，未调用外部提供方",
            },
            field="media effect dead dispatch error",
        )
        with self._uow_factory() as uow:
            rows = uow.session.execute(
                text(
                    """
                        WITH candidates AS (
                            SELECT effect.id
                            FROM media_effects AS effect
                            JOIN outbox_events AS event
                              ON event.aggregate_id = effect.id::text
                            WHERE event.topic = :topic
                              AND event.aggregate_type = 'media_effect'
                              AND event.status = 'dead'
                              AND effect.status IN ('pending', 'retryable')
                            ORDER BY event.updated_at, event.created_at
                            LIMIT :limit
                            FOR UPDATE OF effect SKIP LOCKED
                        )
                        UPDATE media_effects AS effect
                        SET status = 'rejected', attempts = GREATEST(attempts, 1),
                            last_attempt_at = COALESCE(last_attempt_at, NOW()),
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = NOW(), updated_at = NOW()
                        FROM candidates
                        WHERE effect.id = candidates.id
                          AND effect.status IN ('pending', 'retryable')
                        RETURNING effect.id
                        """
                ),
                {"topic": MEDIA_EFFECT_TOPIC, "limit": limit, "error_json": error},
            ).all()
            uow.commit()
            return len(rows)

    def mark_confirmed(
        self,
        effect_key: str,
        claim_token: UUID,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> MediaEffectTransition:
        return self._transition(
            effect_key,
            claim_token,
            MediaEffectStatus.CONFIRMED,
            result=result,
        )

    def mark_rejected(
        self,
        effect_key: str,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> MediaEffectTransition:
        return self._transition(
            effect_key,
            claim_token,
            MediaEffectStatus.REJECTED,
            error=error,
        )

    def mark_indeterminate(
        self,
        effect_key: str,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
    ) -> MediaEffectTransition:
        return self._transition(
            effect_key,
            claim_token,
            MediaEffectStatus.INDETERMINATE,
            error=error,
        )

    def mark_retryable(
        self,
        effect_key: str,
        claim_token: UUID,
        *,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> MediaEffectTransition:
        if retry_after_seconds < 0:
            raise ValueError("retry_after_seconds must not be negative")
        return self._transition(
            effect_key,
            claim_token,
            MediaEffectStatus.RETRYABLE,
            error=error,
            retry_after_seconds=retry_after_seconds,
        )

    def _transition(
        self,
        effect_key: str,
        claim_token: UUID,
        status: MediaEffectStatus,
        *,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        retry_after_seconds: float = 0,
    ) -> MediaEffectTransition:
        key = _nonempty(effect_key, "effect_key", maximum=512)
        normalized_result = (
            normalize_json_object(dict(result), field="media effect result") if result is not None else None
        )
        normalized_error = normalize_json_object(dict(error), field="media effect error") if error is not None else None
        terminal = status.terminal
        with self._uow_factory() as uow:
            row = (
                uow.session.execute(
                    text(
                        """
                        UPDATE media_effects
                        SET status = :status,
                            available_at = CASE
                                WHEN :status = 'retryable'
                                THEN NOW() + make_interval(secs => :retry_after_seconds)
                                ELSE available_at
                            END,
                            claimed_by = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_expires_at = NULL,
                            result_json = CAST(:result_json AS jsonb),
                            error_json = CAST(:error_json AS jsonb),
                            finished_at = CASE WHEN :terminal THEN NOW() ELSE NULL END,
                            updated_at = NOW()
                        WHERE effect_key = :effect_key
                          AND status = 'claimed'
                          AND claim_token = :claim_token
                        RETURNING *
                        """
                    ),
                    {
                        "effect_key": key,
                        "claim_token": claim_token,
                        "status": status.value,
                        "retry_after_seconds": retry_after_seconds,
                        "result_json": canonical_json(normalized_result) if normalized_result is not None else None,
                        "error_json": canonical_json(normalized_error) if normalized_error is not None else None,
                        "terminal": terminal,
                    },
                )
                .mappings()
                .one_or_none()
            )
            if row is not None:
                projection = _projection(row)
                _update_effect_outbox(uow.session, projection)
                uow.commit()
                return MediaEffectTransition(effect=projection, changed=True)

            current = (
                uow.session.execute(
                    text("SELECT * FROM media_effects WHERE effect_key = :effect_key"),
                    {"effect_key": key},
                )
                .mappings()
                .one_or_none()
            )
            if current is None:
                raise MediaEffectNotFoundError(f"media effect {key!r} was not found")
            projection = _projection(current)
            if not status.terminal or projection.status is not status:
                raise MediaEffectStateError(
                    f"media effect {key!r} is {projection.status.value}, not a claim owned by this token"
                )
            _assert_duplicate_payload(projection, normalized_result, normalized_error)
            uow.commit()
            return MediaEffectTransition(effect=projection, changed=False)


def _reservation_values(
    *,
    effect_key: str,
    effect_type: str,
    aggregate_type: str,
    aggregate_id: str,
    payload: Mapping[str, Any],
    available_at: datetime | None,
    max_publish_attempts: int,
) -> dict[str, Any]:
    key = _nonempty(effect_key, "effect_key", maximum=512)
    kind = _nonempty(effect_type, "effect_type")
    aggregate_kind = _nonempty(aggregate_type, "aggregate_type")
    aggregate_key = _nonempty(aggregate_id, "aggregate_id")
    if max_publish_attempts < 1:
        raise ValueError("max_publish_attempts must be positive")
    normalized_payload = normalize_json_object(dict(payload), field="media effect payload")
    request_hash = media_effect_request_hash(
        effect_type=kind,
        aggregate_type=aggregate_kind,
        aggregate_id=aggregate_key,
        payload=normalized_payload,
    )
    effect_id = uuid4()
    return {
        "effect_id": effect_id,
        "event_id": uuid4(),
        "event_key": media_effect_event_key(key),
        "effect_key": key,
        "effect_type": kind,
        "aggregate_type": aggregate_kind,
        "aggregate_id": aggregate_key,
        "payload_json": canonical_json(normalized_payload),
        "request_hash": request_hash,
        "available_at": available_at,
        "max_publish_attempts": max_publish_attempts,
    }


def _update_effect_outbox(session: Session, effect: MediaEffectProjection) -> None:
    event_key = media_effect_event_key(effect.effect_key)
    if effect.status.terminal:
        session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'processed', processed_at = COALESCE(processed_at, NOW()),
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key
                  AND topic = :topic
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {"event_key": event_key, "topic": MEDIA_EFFECT_TOPIC},
        )
    elif effect.status is MediaEffectStatus.RETRYABLE:
        session.execute(
            text(
                """
                UPDATE outbox_events
                SET status = 'pending', available_at = :available_at,
                    published_at = NULL, processed_at = NULL,
                    claimed_by = NULL, claim_token = NULL, claimed_at = NULL,
                    updated_at = NOW()
                WHERE event_key = :event_key
                  AND topic = :topic
                  AND status IN ('pending', 'publishing', 'published')
                """
            ),
            {
                "event_key": event_key,
                "topic": MEDIA_EFFECT_TOPIC,
                "available_at": effect.available_at,
            },
        )


def _projection(row: RowMapping) -> MediaEffectProjection:
    return MediaEffectProjection(
        id=UUID(str(row["id"])),
        effect_key=str(row["effect_key"]),
        effect_type=str(row["effect_type"]),
        aggregate_type=str(row["aggregate_type"]),
        aggregate_id=str(row["aggregate_id"]),
        payload_version=int(row["payload_version"]),
        payload=dict(row["payload_json"] or {}),
        request_hash=bytes(row["request_hash"]),
        status=MediaEffectStatus(row["status"]),
        attempts=int(row["attempts"]),
        available_at=row["available_at"],
        claimed_by=row["claimed_by"],
        claim_token=_uuid(row["claim_token"]),
        claimed_at=row["claimed_at"],
        lease_expires_at=row["lease_expires_at"],
        last_attempt_at=row["last_attempt_at"],
        result=dict(row["result_json"]) if row["result_json"] is not None else None,
        error=dict(row["error_json"]) if row["error_json"] is not None else None,
        finished_at=row["finished_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _dbapi_value(row: Any, key: str, index: int) -> Any:
    if isinstance(row, Mapping):
        return row[key]
    return row[index]


def _uuid(value: object) -> UUID | None:
    return UUID(str(value)) if value is not None else None


def _assert_duplicate_payload(
    projection: MediaEffectProjection,
    result: JsonObject | None,
    error: JsonObject | None,
) -> None:
    if result is not None and projection.result != result:
        raise MediaEffectStateError("terminal effect result does not match the persisted result")
    if error is not None and projection.error != error:
        raise MediaEffectStateError("terminal effect error does not match the persisted error")


def _nonempty(value: str, field: str, *, maximum: int | None = None) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field} must not be empty")
    if maximum is not None and len(normalized) > maximum:
        raise ValueError(f"{field} must not exceed {maximum} characters")
    return normalized


def _positive(value: float, field: str) -> None:
    if value <= 0:
        raise ValueError(f"{field} must be positive")


__all__ = [
    "MEDIA_EFFECT_TOPIC",
    "MediaEffectConflictError",
    "MediaEffectError",
    "MediaEffectIntentReservation",
    "MediaEffectNotFoundError",
    "MediaEffectProjection",
    "MediaEffectStateError",
    "MediaEffectStatus",
    "MediaEffectTransition",
    "PostgresMediaEffectLedger",
    "ReserveMediaEffectResult",
    "SqlAlchemyMediaEffectRepository",
    "media_effect_event_key",
    "media_effect_request_hash",
    "reserve_media_effect_dbapi",
]
