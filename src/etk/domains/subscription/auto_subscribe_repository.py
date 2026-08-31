from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import text

from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.platform.db import SqlAlchemyUnitOfWork

from .auto_subscribe import (
    AutoSourceDisposition,
    AutoSubscriptionClaim,
    AutoSubscriptionClaimDisposition,
    AutoSubscriptionExecution,
)


class SqlAlchemyAutoSubscriptionRepository:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media: SqlAlchemySubscriptionMediaProjection,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media = media

    def promote_pending_to_wanted(self) -> int:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE media_metadata
                    SET subscription_status = 'WANTED',
                        last_synced_at = NOW(),
                        last_updated_at = NOW()
                    WHERE subscription_status = 'PENDING_RELEASE'
                      AND release_date <= NOW()
                    """
                )
            )
            count = int(getattr(result, "rowcount", 0) or 0)
            uow.commit()
            return count

    def movies_to_revive(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, subscription_status,
                               paused_until, last_subscribed_at, first_requested_at
                        FROM media_metadata
                        WHERE item_type = 'Movie'
                          AND subscription_status = 'PAUSED'
                          AND paused_until <= NOW()
                        ORDER BY paused_until, tmdb_id
                        """
                    )
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    def movies_to_pause(self, *, search_window_days: int) -> Sequence[Mapping[str, Any]]:
        days = max(int(search_window_days), 1)
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT tmdb_id, item_type, title, subscription_status,
                               paused_until, last_subscribed_at, first_requested_at
                        FROM media_metadata
                        WHERE item_type = 'Movie'
                          AND subscription_status = 'SUBSCRIBED'
                          AND in_library = FALSE
                          AND last_subscribed_at < NOW() - make_interval(days => :days)
                        ORDER BY last_subscribed_at, tmdb_id
                        """
                    ),
                    {"days": days},
                )
                .mappings()
                .all()
            )
            return tuple(dict(row) for row in rows)

    def worklist(self) -> Sequence[Mapping[str, Any]]:
        with self._unit_of_work() as uow:
            rows = uow.session.execute(text(_WORKLIST_SQL)).mappings().all()
            return tuple(dict(row) for row in rows)

    def series_title(self, tmdb_id: str) -> str | None:
        if not str(tmdb_id or "").strip():
            return None
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT title
                    FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = 'Series'
                    LIMIT 1
                    """
                ),
                {"tmdb_id": str(tmdb_id)},
            )
            return str(value) if value else None

    def claim(
        self,
        item: Mapping[str, Any],
        *,
        phase: str,
        execution: AutoSubscriptionExecution,
    ) -> AutoSubscriptionClaim:
        normalized_phase = _required_text(phase, "phase")
        occurrence = auto_subscription_occurrence(item, phase=normalized_phase)
        state_key = f"auto-subscribe:item:v1:{occurrence}"
        now = datetime.now(UTC)
        token = str(uuid4())
        lease_expires_at = now + timedelta(seconds=max(float(execution.lease_seconds), 1.0))
        with self._unit_of_work() as uow:
            initial = {
                "type": "auto-subscribe-item",
                "occurrence": occurrence,
                "phase": normalized_phase,
                "status": "ready",
                "projection_applied": False,
            }
            uow.session.execute(
                text(
                    """
                    INSERT INTO subscription_runtime_states(
                        state_key, state_json, schema_version, state_revision, created_at, updated_at
                    ) VALUES (
                        :state_key, CAST(:state_json AS jsonb), 1, 0, :now, :now
                    ) ON CONFLICT(state_key) DO NOTHING
                    """
                ),
                {"state_key": state_key, "state_json": _json(initial), "now": now},
            )
            row = (
                uow.session.execute(
                    text(
                        """
                        SELECT state_json, state_revision
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
            state = _state(row, occurrence=occurrence, phase=normalized_phase)
            status = str(state.get("status") or "ready")
            if status == "confirmed" and state.get("projection_applied") is True:
                uow.commit()
                return AutoSubscriptionClaim(
                    state_key,
                    occurrence,
                    normalized_phase,
                    AutoSubscriptionClaimDisposition.REPLAYED,
                    action_type=str(state.get("action_type") or ""),
                )
            if status == "indeterminate":
                error = state.get("error")
                uow.commit()
                return AutoSubscriptionClaim(
                    state_key,
                    occurrence,
                    normalized_phase,
                    AutoSubscriptionClaimDisposition.INDETERMINATE,
                    action_type=str(state.get("action_type") or ""),
                    error=dict(error) if isinstance(error, Mapping) else None,
                )

            lease = _optional_datetime(state.get("lease_expires_at"))
            active = status in {"claimed", "requesting"} and lease is not None and lease > now
            if active:
                uow.commit()
                return AutoSubscriptionClaim(
                    state_key,
                    occurrence,
                    normalized_phase,
                    AutoSubscriptionClaimDisposition.BUSY,
                )
            if status == "requesting" and not bool(state.get("replay_safe")):
                error = {
                    "action": str(state.get("source_type") or "auto-subscribe"),
                    "error_type": "expired_non_replay_safe_source_claim",
                    "indeterminate": True,
                }
                state.update(
                    status="indeterminate",
                    error=error,
                    finished_at=now.isoformat(),
                    claimed_by=None,
                    claim_token=None,
                    lease_expires_at=None,
                )
                self._write_state(uow, state_key, state, now=now)
                uow.commit()
                return AutoSubscriptionClaim(
                    state_key,
                    occurrence,
                    normalized_phase,
                    AutoSubscriptionClaimDisposition.INDETERMINATE,
                    error=error,
                )

            state.update(
                status="claimed",
                claimed_by=execution.worker_id,
                claim_token=token,
                job_id=execution.job_id,
                attempt_id=execution.attempt_id,
                claimed_at=now.isoformat(),
                lease_expires_at=lease_expires_at.isoformat(),
                source_type=None,
                replay_safe=None,
                error=None,
            )
            self._write_state(uow, state_key, state, now=now)
            uow.commit()
        return AutoSubscriptionClaim(
            state_key,
            occurrence,
            normalized_phase,
            AutoSubscriptionClaimDisposition.ACQUIRED,
            claim_token=token,
            job_id=execution.job_id,
            attempt_id=execution.attempt_id,
            worker_id=execution.worker_id,
        )

    def begin_source(
        self,
        claim: AutoSubscriptionClaim,
        *,
        source_type: str,
        replay_safe: bool,
        execution: AutoSubscriptionExecution,
    ) -> None:
        source = _required_text(source_type, "source_type")
        if (claim.job_id, claim.attempt_id, claim.worker_id) != (
            execution.job_id,
            execution.attempt_id,
            execution.worker_id,
        ):
            raise RuntimeError("auto-subscribe item claim execution identity changed")
        now = datetime.now(UTC)
        lease_expires_at = now + timedelta(seconds=max(float(execution.lease_seconds), 1.0))
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"claimed"})
            state.update(
                status="requesting",
                source_type=source,
                replay_safe=bool(replay_safe),
                source_started_at=now.isoformat(),
                lease_expires_at=lease_expires_at.isoformat(),
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    def finish_source(
        self,
        claim: AutoSubscriptionClaim,
        *,
        disposition: AutoSourceDisposition,
    ) -> None:
        now = datetime.now(UTC)
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"requesting"})
            state.update(
                status="claimed",
                last_source_type=state.get("source_type"),
                last_source_disposition=disposition.value,
                source_type=None,
                replay_safe=None,
                source_started_at=None,
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    def release(self, claim: AutoSubscriptionClaim) -> None:
        now = datetime.now(UTC)
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"claimed"})
            state.update(
                status="ready",
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                lease_expires_at=None,
                source_type=None,
                replay_safe=None,
                source_started_at=None,
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    def mark_indeterminate(
        self,
        claim: AutoSubscriptionClaim,
        *,
        source_type: str,
        error: Mapping[str, Any],
    ) -> None:
        now = datetime.now(UTC)
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"claimed", "requesting"})
            state.update(
                status="indeterminate",
                source_type=_required_text(source_type, "source_type"),
                error=dict(error),
                finished_at=now.isoformat(),
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                lease_expires_at=None,
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    def complete_subscription(
        self,
        claim: AutoSubscriptionClaim,
        item: Mapping[str, Any],
        *,
        action_type: str,
        mark_subscribed: bool,
    ) -> None:
        now = datetime.now(UTC)
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"requesting"})
            if mark_subscribed:
                key = SubscriptionMediaKey.from_mapping(item)
                updated = self._media.set_status(uow, key, "SUBSCRIBED", item, None, None)
                if updated != 1:
                    raise RuntimeError("auto-subscribe local subscription projection was not applied")
            state.update(
                status="confirmed",
                action_type=_required_text(action_type, "action_type"),
                projection_applied=True,
                finished_at=now.isoformat(),
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                lease_expires_at=None,
                source_type=None,
                replay_safe=None,
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    def complete_movie_maintenance(
        self,
        claim: AutoSubscriptionClaim,
        *,
        tmdb_id: str,
        action: str,
        pause_days: int | None = None,
    ) -> None:
        normalized_action = _required_text(action, "action")
        if normalized_action not in {"revive", "pause"}:
            raise ValueError("auto-subscribe movie maintenance action must be revive or pause")
        now = datetime.now(UTC)
        with self._unit_of_work() as uow:
            state = self._claimed_state(uow, claim, allowed={"requesting"})
            if normalized_action == "revive":
                result = uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET subscription_status = 'SUBSCRIBED',
                            last_subscribed_at = NOW(),
                            paused_until = NULL,
                            last_updated_at = NOW()
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
            else:
                days = max(int(pause_days or 1), 1)
                result = uow.session.execute(
                    text(
                        """
                        UPDATE media_metadata
                        SET subscription_status = 'PAUSED',
                            paused_until = NOW() + make_interval(days => :days)
                                + (random() * INTERVAL '2 days'),
                            last_updated_at = NOW()
                        WHERE tmdb_id = :tmdb_id AND item_type = 'Movie'
                        """
                    ),
                    {"tmdb_id": tmdb_id, "days": days},
                )
            if int(getattr(result, "rowcount", 0) or 0) != 1:
                raise RuntimeError("auto-subscribe local movie maintenance projection was not applied")
            state.update(
                status="confirmed",
                action_type=normalized_action,
                projection_applied=True,
                finished_at=now.isoformat(),
                claimed_by=None,
                claim_token=None,
                claimed_at=None,
                lease_expires_at=None,
                source_type=None,
                replay_safe=None,
            )
            self._write_state(uow, claim.state_key, state, now=now)
            uow.commit()

    @staticmethod
    def _write_state(
        uow: SqlAlchemyUnitOfWork,
        state_key: str,
        state: Mapping[str, Any],
        *,
        now: datetime,
    ) -> None:
        uow.session.execute(
            text(
                """
                UPDATE subscription_runtime_states
                SET state_json = CAST(:state_json AS jsonb),
                    state_revision = state_revision + 1,
                    updated_at = :now
                WHERE state_key = :state_key
                """
            ),
            {"state_key": state_key, "state_json": _json(state), "now": now},
        )

    @staticmethod
    def _claimed_state(
        uow: SqlAlchemyUnitOfWork,
        claim: AutoSubscriptionClaim,
        *,
        allowed: set[str],
    ) -> dict[str, Any]:
        if not claim.acquired or not claim.claim_token:
            raise RuntimeError("auto-subscribe operation requires an acquired item claim")
        row = (
            uow.session.execute(
                text(
                    """
                    SELECT state_json
                    FROM subscription_runtime_states
                    WHERE state_key = :state_key
                    FOR UPDATE
                    """
                ),
                {"state_key": claim.state_key},
            )
            .mappings()
            .one_or_none()
        )
        if row is None:
            raise RuntimeError("auto-subscribe item claim disappeared")
        state = _state(row, occurrence=claim.occurrence, phase=claim.phase)
        lease_expires_at = _optional_datetime(state.get("lease_expires_at"))
        if (
            str(state.get("claim_token") or "") != claim.claim_token
            or str(state.get("job_id") or "") != claim.job_id
            or str(state.get("attempt_id") or "") != claim.attempt_id
            or str(state.get("claimed_by") or "") != claim.worker_id
            or lease_expires_at is None
            or lease_expires_at <= datetime.now(UTC)
            or str(state.get("status") or "") not in allowed
        ):
            raise RuntimeError("auto-subscribe item claim is no longer valid")
        return state


class SqlAlchemyAutoSubscriptionNotificationDirectory:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def user_chat_id(self, user_id: str) -> str | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT telegram_chat_id
                    FROM emby_users_extended
                    WHERE emby_user_id = :user_id
                    """
                ),
                {"user_id": str(user_id)},
            )
            return str(value) if value else None

    def admin_chat_ids(self) -> Sequence[str]:
        with self._unit_of_work() as uow:
            values = uow.session.scalars(
                text(
                    """
                    SELECT extended.telegram_chat_id
                    FROM emby_users AS base
                    JOIN emby_users_extended AS extended
                      ON base.id = extended.emby_user_id
                    WHERE base.is_administrator = TRUE
                      AND extended.telegram_chat_id IS NOT NULL
                      AND extended.telegram_chat_id <> ''
                    ORDER BY extended.telegram_chat_id
                    """
                )
            ).all()
            return tuple(str(value) for value in values)


_WORKLIST_SQL = """
WITH target_media AS (
    SELECT
        tmdb_id,
        item_type,
        title,
        release_date,
        release_year,
        poster_path,
        backdrop_path,
        overview,
        parent_series_tmdb_id,
        season_number,
        NULL::integer AS episode_number,
        subscription_status,
        watching_status,
        subscription_sources_json,
        in_library,
        first_requested_at,
        last_subscribed_at
    FROM media_metadata
    WHERE (item_type = 'Movie' AND subscription_status IN ('WANTED', 'SUBSCRIBED'))
       OR (item_type = 'Season' AND subscription_status IN ('WANTED', 'SUBSCRIBED'))
)
SELECT
    target.tmdb_id,
    target.item_type,
    target.title,
    target.release_date,
    target.release_year,
    target.poster_path,
    target.backdrop_path,
    target.overview,
    target.parent_series_tmdb_id,
    target.season_number,
    target.episode_number,
    target.subscription_status,
    target.watching_status,
    target.subscription_sources_json,
    target.first_requested_at,
    target.last_subscribed_at,
    target.in_library,
    COALESCE(
        jsonb_agg(episode.episode_number ORDER BY episode.episode_number)
            FILTER (WHERE episode.episode_number IS NOT NULL),
        '[]'::jsonb
    ) AS missing_episode_numbers,
    COUNT(episode.episode_number)::integer AS missing_episode_count
FROM target_media AS target
LEFT JOIN media_metadata AS episode
  ON target.item_type = 'Season'
 AND episode.item_type = 'Episode'
 AND episode.parent_series_tmdb_id = COALESCE(target.parent_series_tmdb_id, target.tmdb_id)
 AND COALESCE(episode.season_number, -1) = COALESCE(target.season_number, -1)
 AND COALESCE(episode.in_library, FALSE) = FALSE
 AND episode.episode_number IS NOT NULL
 AND (episode.release_date IS NULL OR episode.release_date <= CURRENT_DATE)
GROUP BY
    target.tmdb_id, target.item_type, target.title, target.release_date, target.release_year,
    target.poster_path, target.backdrop_path, target.overview, target.parent_series_tmdb_id,
    target.season_number, target.episode_number, target.subscription_status,
    target.watching_status, target.subscription_sources_json, target.first_requested_at,
    target.last_subscribed_at, target.in_library
HAVING target.subscription_status = 'WANTED'
    OR (target.item_type = 'Movie' AND COALESCE(target.in_library, FALSE) = FALSE)
    OR (
        target.item_type = 'Season'
        AND (COUNT(episode.episode_number) > 0 OR COALESCE(target.in_library, FALSE) = FALSE)
    )
ORDER BY
    CASE
        WHEN target.subscription_status = 'WANTED' THEN 0
        WHEN target.item_type = 'Season' AND target.subscription_status = 'SUBSCRIBED' THEN 0
        ELSE 1
    END,
    COALESCE(target.first_requested_at, target.last_subscribed_at, NOW()) ASC
"""


def auto_subscription_occurrence(item: Mapping[str, Any], *, phase: str) -> str:
    document = {
        "phase": _required_text(phase, "phase"),
        "tmdb_id": _required_text(item.get("tmdb_id"), "tmdb_id"),
        "item_type": str(item.get("item_type") or "Movie").strip() or "Movie",
        "subscription_status": str(item.get("subscription_status") or "").strip().upper(),
        "first_requested_at": _stable_value(item.get("first_requested_at")),
        "last_subscribed_at": _stable_value(item.get("last_subscribed_at")),
        "paused_until": _stable_value(item.get("paused_until")),
        "parent_series_tmdb_id": str(item.get("parent_series_tmdb_id") or "").strip(),
        "season_number": item.get("season_number"),
        "missing_episode_numbers": _positive_int_values(item.get("missing_episode_numbers")),
    }
    return hashlib.sha256(_json(document).encode("utf-8")).hexdigest()


def _state(
    row: Mapping[Any, Any],
    *,
    occurrence: str,
    phase: str,
) -> dict[str, Any]:
    value = row.get("state_json")
    if not isinstance(value, Mapping):
        raise RuntimeError("auto-subscribe item state is not an object")
    state = dict(value)
    if state.get("type") != "auto-subscribe-item":
        raise RuntimeError("auto-subscribe item state type changed")
    if str(state.get("occurrence") or "") != occurrence or str(state.get("phase") or "") != phase:
        raise RuntimeError("auto-subscribe item state identity changed")
    return state


def _stable_value(value: object) -> object:
    if isinstance(value, datetime):
        current = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return current.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def _optional_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _positive_int_values(value: object) -> list[int]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except ValueError:
            decoded = [part.strip() for part in value.replace("，", ",").split(",")]
        value = decoded
    values = value if isinstance(value, Sequence) and not isinstance(value, str | bytes) else [value]
    result: set[int] = set()
    for item in values:
        if isinstance(item, bool):
            continue
        try:
            parsed = int(cast(Any, item))
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            result.add(parsed)
    return sorted(result)


def _required_text(value: object, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"auto-subscribe {field} must not be empty")
    return normalized


def _json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), ensure_ascii=False, separators=(",", ":"), sort_keys=True)


__all__ = [
    "SqlAlchemyAutoSubscriptionNotificationDirectory",
    "SqlAlchemyAutoSubscriptionRepository",
    "auto_subscription_occurrence",
]
