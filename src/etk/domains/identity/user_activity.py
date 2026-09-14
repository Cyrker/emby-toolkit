from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import text

from etk.domains.media.effects import (
    MediaEffectConflictError,
    SqlAlchemyMediaEffectRepository,
)
from etk.domains.media.models import ServiceResult
from etk.platform.db import SqlAlchemyUnitOfWork

_ACTIVITY_EVENTS = frozenset(
    {
        "item.markfavorite",
        "item.unmarkfavorite",
        "item.markplayed",
        "item.markunplayed",
        "item.rate",
        "playback.start",
        "playback.pause",
        "playback.stop",
        "playback.halfway",
    }
)
_EXACT_FAVORITE_TYPES = frozenset({"Series", "Season", "Episode"})


@dataclass(frozen=True, slots=True)
class UserActivityMutation:
    user_id: str
    source_item_id: str
    source_item_type: str
    aggregate_item_id: str | None
    exact_favorite: bool | None
    is_favorite: bool | None
    played: bool | None
    playback_position_ticks: int | None
    last_played_date: datetime | None
    received_at: datetime


@dataclass(frozen=True, slots=True)
class UserEventEffectIntent:
    kind: str
    aggregate_id: str
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class UserEventApplication:
    result: ServiceResult
    created: bool


class UserMediaTargetResolver(Protocol):
    def series_id_for_episode(self, user_id: str, episode_id: str) -> str | None: ...


class PolicyPropagationGuard(Protocol):
    def consume(self, user_id: str, event_key: str) -> bool: ...


class UserActivityRepository(Protocol):
    def apply(
        self,
        *,
        event_key: str,
        event_name: str,
        payload_digest: str,
        aggregate_id: str,
        result: ServiceResult,
        mutation: UserActivityMutation | None,
        intents: Sequence[UserEventEffectIntent],
    ) -> UserEventApplication: ...


class SqlAlchemyUserActivityRepository:
    """Atomically apply a user projection and reserve its durable downstream intents."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def apply(
        self,
        *,
        event_key: str,
        event_name: str,
        payload_digest: str,
        aggregate_id: str,
        result: ServiceResult,
        mutation: UserActivityMutation | None,
        intents: Sequence[UserEventEffectIntent],
    ) -> UserEventApplication:
        receipt_payload = {
            "event_name": event_name,
            "payload_sha256": payload_digest,
        }
        result_payload = {
            "status_code": result.status_code,
            "payload": dict(result.payload),
        }
        with self._unit_of_work() as uow:
            effects = SqlAlchemyMediaEffectRepository(uow.session)
            receipt = effects.record_confirmed(
                effect_key=_receipt_key(event_key),
                effect_type="emby.event.user.applied",
                aggregate_type="emby_user_event",
                aggregate_id=aggregate_id,
                payload=receipt_payload,
                result=result_payload,
            )
            if not receipt.created:
                existing = receipt.effect.result or {}
                existing_payload = existing.get("payload")
                replay = ServiceResult(
                    dict(existing_payload)
                    if isinstance(existing_payload, Mapping)
                    else {"status": "user_event_applied"},
                    _status_code(existing.get("status_code"), default=result.status_code),
                )
                uow.commit()
                return UserEventApplication(replay, False)

            if mutation is not None:
                self._apply_mutation(uow, mutation)
            for intent in intents:
                effects.reserve(
                    effect_key=f"{_receipt_key(event_key)}:{intent.kind}",
                    effect_type=f"emby.event.user.{intent.kind}",
                    aggregate_type="emby_user_event_effect",
                    aggregate_id=intent.aggregate_id,
                    payload={
                        "source_event_key": event_key,
                        **dict(intent.payload),
                    },
                )
            uow.commit()
        return UserEventApplication(result, True)

    @staticmethod
    def _apply_mutation(uow: SqlAlchemyUnitOfWork, mutation: UserActivityMutation) -> None:
        if mutation.exact_favorite is True:
            uow.session.execute(
                text(
                    """
                    INSERT INTO emby_favorite_items (user_id, item_id, updated_at)
                    VALUES (:user_id, :item_id, :updated_at)
                    ON CONFLICT (user_id, item_id) DO UPDATE SET
                        updated_at = EXCLUDED.updated_at
                    """
                ),
                {
                    "user_id": mutation.user_id,
                    "item_id": mutation.source_item_id,
                    "updated_at": mutation.received_at,
                },
            )
        elif mutation.exact_favorite is False:
            uow.session.execute(
                text(
                    """
                    DELETE FROM emby_favorite_items
                    WHERE user_id = :user_id AND item_id = :item_id
                    """
                ),
                {"user_id": mutation.user_id, "item_id": mutation.source_item_id},
            )

        if mutation.aggregate_item_id is None or not any(
            value is not None
            for value in (
                mutation.is_favorite,
                mutation.played,
                mutation.playback_position_ticks,
                mutation.last_played_date,
            )
        ):
            return
        uow.session.execute(
            text(
                """
                INSERT INTO user_media_data (
                    user_id, item_id, is_favorite, played,
                    playback_position_ticks, last_played_date, last_updated_at
                ) VALUES (
                    :user_id, :item_id, COALESCE(:is_favorite, FALSE),
                    COALESCE(:played, FALSE), COALESCE(:position, 0),
                    :last_played_date, :received_at
                )
                ON CONFLICT (user_id, item_id) DO UPDATE SET
                    is_favorite = CASE
                        WHEN :has_is_favorite THEN EXCLUDED.is_favorite
                        ELSE user_media_data.is_favorite
                    END,
                    played = CASE
                        WHEN :has_played THEN EXCLUDED.played
                        ELSE user_media_data.played
                    END,
                    playback_position_ticks = CASE
                        WHEN :has_position THEN EXCLUDED.playback_position_ticks
                        ELSE user_media_data.playback_position_ticks
                    END,
                    last_played_date = CASE
                        WHEN :has_last_played_date THEN EXCLUDED.last_played_date
                        ELSE user_media_data.last_played_date
                    END,
                    last_updated_at = EXCLUDED.last_updated_at
                """
            ),
            {
                "user_id": mutation.user_id,
                "item_id": mutation.aggregate_item_id,
                "is_favorite": mutation.is_favorite,
                "played": mutation.played,
                "position": mutation.playback_position_ticks,
                "last_played_date": mutation.last_played_date,
                "received_at": mutation.received_at,
                "has_is_favorite": mutation.is_favorite is not None,
                "has_played": mutation.played is not None,
                "has_position": mutation.playback_position_ticks is not None,
                "has_last_played_date": mutation.last_played_date is not None,
            },
        )


class P8UserEventGateway:
    def __init__(
        self,
        *,
        repository: UserActivityRepository,
        resolver: UserMediaTargetResolver,
        policy_guard: PolicyPropagationGuard,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._resolver = resolver
        self._policy_guard = policy_guard
        self._clock = clock

    def handle(
        self,
        payload: Mapping[str, Any],
        *,
        host_url: str,
        remote_addr: str,
    ) -> ServiceResult:
        del host_url
        event_name = str(payload.get("Event") or "").strip()
        event_key = _event_key(payload)
        payload_digest = _payload_digest(payload)
        try:
            if event_name == "user.policyupdated":
                return self._policy_event(payload, event_key=event_key, payload_digest=payload_digest)
            if event_name in _ACTIVITY_EVENTS:
                return self._activity_event(
                    payload,
                    event_name=event_name,
                    event_key=event_key,
                    payload_digest=payload_digest,
                    remote_addr=remote_addr,
                )
            return ServiceResult({"status": "event_ignored_not_in_trigger_list"})
        except MediaEffectConflictError:
            return ServiceResult({"status": "event_id_conflict"}, 409)
        except Exception:
            return ServiceResult({"status": "error_updating_user_data"}, 500)

    def _policy_event(
        self,
        payload: Mapping[str, Any],
        *,
        event_key: str,
        payload_digest: str,
    ) -> ServiceResult:
        user = _mapping(payload.get("User"))
        user_id = str(user.get("Id") or "").strip()
        if not user_id:
            return ServiceResult({"status": "event_ignored_no_user_id"})
        suppressed = self._policy_guard.consume(user_id, event_key)
        intents = [
            UserEventEffectIntent(
                "refresh-user-policy",
                user_id,
                {"user_id": user_id},
            )
        ]
        if not suppressed:
            intents.append(
                UserEventEffectIntent(
                    "sync-template-policy",
                    user_id,
                    {"updated_user_id": user_id},
                )
            )
        result = ServiceResult(
            {"status": ("event_ignored_system_triggered" if suppressed else "auto_sync_task_submitted")},
            200 if suppressed else 202,
        )
        return self._repository.apply(
            event_key=event_key,
            event_name="user.policyupdated",
            payload_digest=payload_digest,
            aggregate_id=user_id,
            result=result,
            mutation=None,
            intents=intents,
        ).result

    def _activity_event(
        self,
        payload: Mapping[str, Any],
        *,
        event_name: str,
        event_key: str,
        payload_digest: str,
        remote_addr: str,
    ) -> ServiceResult:
        user = _mapping(payload.get("User"))
        item = _mapping(payload.get("Item"))
        user_id = str(user.get("Id") or "").strip()
        item_id = str(item.get("Id") or "").strip()
        item_type = str(item.get("Type") or "").strip()
        if not user_id or not item_id or not item_type:
            return ServiceResult({"status": "event_ignored_missing_data"})

        playback = _mapping(payload.get("PlaybackInfo"))
        if event_name == "playback.halfway":
            if item_type != "Episode":
                return ServiceResult({"status": "event_ignored_non_episode_halfway"})
            position, runtime = _halfway_numbers(payload, item, playback)
            if runtime > 0 and position / runtime < 0.5:
                return ServiceResult({"status": "event_ignored_before_halfway"})

        user_data = _mapping(payload.get("UserData")) or _mapping(item.get("UserData"))
        exact_favorite = _exact_favorite(event_name, item_type, user_data)
        aggregate_item_id = self._aggregate_item_id(item_type, user_id, item_id)
        now = self._clock().astimezone(UTC)
        is_favorite = bool(user_data.get("IsFavorite")) if "IsFavorite" in user_data else None
        played = bool(user_data.get("Played")) if "Played" in user_data else None
        position: int | None = None
        last_played: datetime | None = None
        if played is True:
            position = 0
            last_played = now
        if event_name in {"playback.start", "playback.pause", "playback.stop", "playback.halfway"}:
            if "PositionTicks" in playback:
                position = _integer_or_zero(playback.get("PositionTicks"))
            if playback or event_name == "playback.halfway":
                last_played = now
        if event_name == "playback.stop" and playback:
            played = playback.get("PlayedToCompletion") is True
            if played:
                position = 0

        mutation = UserActivityMutation(
            user_id=user_id,
            source_item_id=item_id,
            source_item_type=item_type,
            aggregate_item_id=aggregate_item_id,
            exact_favorite=exact_favorite,
            is_favorite=is_favorite,
            played=played,
            playback_position_ticks=position,
            last_played_date=last_played,
            received_at=now,
        )
        intents = _activity_intents(
            event_name,
            event_key=event_key,
            user_id=user_id,
            item_id=item_id,
            item_type=item_type,
            exact_favorite=exact_favorite,
            playback=playback,
            snapshot=_delivery_snapshot(payload, event_name=event_name, remote_addr=remote_addr),
        )
        has_aggregate_update = aggregate_item_id is not None and any(
            value is not None for value in (is_favorite, played, position, last_played)
        )
        if has_aggregate_update:
            result = ServiceResult({"status": "user_data_updated"})
        elif exact_favorite is not None:
            result = ServiceResult({"status": "exact_favorite_updated"})
        elif intents:
            result = ServiceResult({"status": "user_event_effects_submitted"}, 202)
        else:
            return ServiceResult({"status": "event_ignored_no_updatable_data"})
        return self._repository.apply(
            event_key=event_key,
            event_name=event_name,
            payload_digest=payload_digest,
            aggregate_id=f"{user_id}:{item_id}",
            result=result,
            mutation=mutation,
            intents=intents,
        ).result

    def _aggregate_item_id(self, item_type: str, user_id: str, item_id: str) -> str | None:
        if item_type in {"Movie", "Series"}:
            return item_id
        if item_type == "Episode":
            return self._resolver.series_id_for_episode(user_id, item_id)
        return None


def _activity_intents(
    event_name: str,
    *,
    event_key: str,
    user_id: str,
    item_id: str,
    item_type: str,
    exact_favorite: bool | None,
    playback: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> list[UserEventEffectIntent]:
    base = {
        "event_name": event_name,
        "event_key": event_key,
        "user_id": user_id,
        "item_id": item_id,
        "item_type": item_type,
        "event_snapshot": dict(snapshot),
    }
    intents: list[UserEventEffectIntent] = []
    if exact_favorite is True:
        intents.append(UserEventEffectIntent("favorite-intro-detection", item_id, base))
    if event_name == "playback.halfway":
        intents.append(
            UserEventEffectIntent(
                "halfway-intro-detection",
                item_id,
                {**base, "position_ticks": _integer_or_zero(playback.get("PositionTicks"))},
            )
        )
    if event_name == "playback.stop":
        for kind in (
            "playback-cleanup",
            "proxy-concurrency-release",
            "telegram-playback-notification",
        ):
            intents.append(UserEventEffectIntent(kind, item_id, base))
    elif event_name in {"playback.start", "playback.pause"}:
        intents.append(UserEventEffectIntent("telegram-playback-notification", item_id, base))
    return intents


def _delivery_snapshot(
    payload: Mapping[str, Any],
    *,
    event_name: str,
    remote_addr: str,
) -> dict[str, Any]:
    user = dict(_mapping(payload.get("User")))
    item = dict(_mapping(payload.get("Item")))
    user_data = dict(_mapping(payload.get("UserData")) or _mapping(item.get("UserData")))
    playback = dict(_mapping(payload.get("PlaybackInfo")))
    session = dict(_mapping(payload.get("Session")))
    user_id = str(user.get("Id") or payload.get("UserId") or "").strip()
    item_id = str(item.get("Id") or payload.get("ItemId") or "").strip()
    play_session_id = str(playback.get("PlaySessionId") or payload.get("PlaySessionId") or "").strip()
    device_id = str(session.get("DeviceId") or payload.get("DeviceId") or "").strip()
    return {
        "Event": event_name,
        "UserId": user_id,
        "ItemId": item_id,
        "PlaySessionId": play_session_id,
        "DeviceId": device_id,
        "User": user,
        "Item": item,
        "UserData": user_data,
        "PlaybackInfo": playback,
        "Session": session,
        "remote_addr": str(remote_addr or ""),
    }


def _exact_favorite(
    event_name: str,
    item_type: str,
    user_data: Mapping[str, Any],
) -> bool | None:
    if item_type not in _EXACT_FAVORITE_TYPES:
        return None
    if event_name == "item.markfavorite":
        return True
    if event_name == "item.unmarkfavorite":
        return False
    if "IsFavorite" in user_data:
        return bool(user_data.get("IsFavorite"))
    return None


def _halfway_numbers(
    payload: Mapping[str, Any],
    item: Mapping[str, Any],
    playback: Mapping[str, Any],
) -> tuple[int, int]:
    try:
        position = int(
            playback.get("PositionTicks") or playback.get("PlaybackPositionTicks") or payload.get("PositionTicks") or 0
        )
        runtime = int(playback.get("RunTimeTicks") or item.get("RunTimeTicks") or 0)
        return position, runtime
    except (TypeError, ValueError):
        return 0, 0


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _integer_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _payload_digest(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _event_key(payload: Mapping[str, Any]) -> str:
    explicit = str(
        payload.get("EventId")
        or payload.get("event_id")
        or payload.get("IdempotencyKey")
        or payload.get("idempotency_key")
        or ""
    ).strip()
    return hashlib.sha256((explicit or _payload_digest(payload)).encode("utf-8")).hexdigest()


def _receipt_key(event_key: str) -> str:
    return f"p8:user-event:{event_key}"


def _status_code(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "P8UserEventGateway",
    "PolicyPropagationGuard",
    "SqlAlchemyUserActivityRepository",
    "UserActivityMutation",
    "UserActivityRepository",
    "UserEventApplication",
    "UserEventEffectIntent",
    "UserMediaTargetResolver",
]
