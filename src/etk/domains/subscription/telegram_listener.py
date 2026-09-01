from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs import normalize_json_object

from .provider_control import ProviderControlService


class TelegramSubscriptionInterestPort(Protocol):
    def wants(self, candidate: Mapping[str, Any]) -> bool: ...


class TelegramListenerCommandPort(Protocol):
    def enqueue_listener_action_once(
        self,
        payload: Mapping[str, Any],
        *,
        secret_payload: Mapping[str, Any],
        reference: str,
        owner_key: str,
        ttl_seconds: float,
        idempotency_key: str,
    ) -> str: ...


class DurableTelegramRealtimeIngress:
    """Atomically externalize one candidate and enqueue only its opaque identity."""

    def __init__(
        self,
        commands: TelegramListenerCommandPort,
        *,
        ttl_seconds: float = 7 * 24 * 60 * 60,
    ) -> None:
        self._commands = commands
        self._ttl_seconds = max(60.0, float(ttl_seconds))

    def stage(
        self,
        candidate: Mapping[str, Any],
        *,
        occurrence: str,
        is_brainless: bool,
        is_keyword_matched: bool,
        is_subscribe: bool,
        is_hdhive_push: bool,
    ) -> bool:
        normalized_candidate = normalize_json_object(dict(candidate), field="Telegram listener candidate secret")
        if not normalized_candidate:
            raise ValueError("Telegram listener candidate cannot be empty")
        normalized_occurrence = str(occurrence or "").strip()
        if not re.fullmatch(r"telegram-listener:[0-9a-f]{64}", normalized_occurrence):
            raise ValueError("Telegram listener occurrence is invalid")
        idempotency_key = f"telegram-listener-action:{normalized_occurrence}"
        owner_key = hashlib.sha256(idempotency_key.encode()).hexdigest()
        candidate_hash = _json_hash(normalized_candidate)
        reference = hashlib.sha256(f"{idempotency_key}\0{candidate_hash}".encode()).hexdigest()
        command_id = self._commands.enqueue_listener_action_once(
            {
                "candidate_ref": reference,
                "candidate_hash": candidate_hash,
                "occurrence": normalized_occurrence,
                "is_brainless": bool(is_brainless),
                "is_keyword_matched": bool(is_keyword_matched),
                "is_subscribe": bool(is_subscribe),
                "is_hdhive_push": bool(is_hdhive_push),
            },
            secret_payload={"candidate": normalized_candidate},
            reference=reference,
            owner_key=owner_key,
            ttl_seconds=self._ttl_seconds,
            idempotency_key=idempotency_key,
        )
        return bool(command_id)


class SqlAlchemyTelegramSubscriptionInterest:
    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def wants(self, candidate: Mapping[str, Any]) -> bool:
        tmdb_id = str(candidate.get("tmdb_id") or "").strip()
        if not tmdb_id:
            return False
        media_type = str(candidate.get("media_type") or candidate.get("item_type") or "movie").casefold()
        if media_type in {"episode", "season", "series"}:
            media_type = "tv"
        season_number = _optional_int(candidate.get("season_number"))
        episode_number = _optional_int(candidate.get("episode_number"))
        with self._unit_of_work() as uow:
            if media_type == "tv" and episode_number is not None:
                existing = bool(
                    uow.session.scalar(
                        text(
                            """
                            SELECT EXISTS(
                                SELECT 1
                                FROM media_metadata
                                WHERE item_type = 'Episode'
                                  AND parent_series_tmdb_id = :tmdb_id
                                  AND COALESCE(season_number, -1) = COALESCE(:season_number, -1)
                                  AND episode_number = :episode_number
                                  AND COALESCE(in_library, FALSE) IS TRUE
                            )
                            """
                        ),
                        {
                            "tmdb_id": tmdb_id,
                            "season_number": season_number,
                            "episode_number": episode_number,
                        },
                    )
                )
                if existing:
                    uow.commit()
                    return False
            wanted = bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT EXISTS(
                            SELECT 1
                            FROM media_metadata
                            WHERE (
                                item_type = 'Movie'
                                AND tmdb_id = :tmdb_id
                                AND subscription_status IN ('WANTED','SUBSCRIBED','PENDING_RELEASE','PAUSED')
                            ) OR (
                                item_type = 'Season'
                                AND (tmdb_id = :tmdb_id OR parent_series_tmdb_id = :tmdb_id)
                                AND subscription_status IN ('WANTED','SUBSCRIBED','PENDING_RELEASE','PAUSED')
                            ) OR (
                                item_type = 'Series'
                                AND tmdb_id = :tmdb_id
                                AND (
                                    watching_status IN ('Watching','Paused','Pending')
                                    OR COALESCE(waiting_for_completed_pack, FALSE) IS TRUE
                                )
                            )
                        )
                        """
                    ),
                    {"tmdb_id": tmdb_id},
                )
            )
            uow.commit()
            return wanted


class NativeTelegramRealtimeActionGateway:
    """Apply listener policy, then reserve one credential-free durable operation."""

    def __init__(
        self,
        provider_control: ProviderControlService,
        interests: TelegramSubscriptionInterestPort,
    ) -> None:
        self._provider_control = provider_control
        self._interests = interests

    def stage(
        self,
        candidate: Mapping[str, Any],
        *,
        occurrence: str,
        is_brainless: bool,
        is_keyword_matched: bool,
        is_subscribe: bool,
        is_hdhive_push: bool,
    ) -> bool:
        forced = bool(is_brainless or is_keyword_matched or is_hdhive_push)
        if not forced and (not is_subscribe or not self._interests.wants(candidate)):
            return False
        target_link = str(candidate.get("target_link") or "").strip()
        hdhive_match = re.search(
            r"(?i)hdhive\.com/resource/(?:115|magnet|ed2k|bt)?/?([a-f0-9-]{32,36})",
            target_link,
        )
        if hdhive_match is not None:
            result = self._provider_control.hdhive_download(
                {
                    "slug": hdhive_match.group(1),
                    "tmdb_id": candidate.get("tmdb_id"),
                    "media_type": candidate.get("media_type") or candidate.get("item_type"),
                    "title": candidate.get("title") or candidate.get("name"),
                },
                principal_id="telegram-listener",
                idempotency_key=occurrence,
            )
        else:
            result = self._provider_control.cloud_download(
                {"source_type": "channel", "resource": dict(candidate)},
                principal_id="telegram-listener",
                idempotency_key=occurrence,
            )
        if result.status_code >= 500:
            raise RuntimeError("Telegram listener durable operation submission is unavailable")
        return bool(result.payload.get("success") or result.payload.get("accepted"))


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _json_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        normalize_json_object(dict(value), field="Telegram listener candidate hash"),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "DurableTelegramRealtimeIngress",
    "NativeTelegramRealtimeActionGateway",
    "SqlAlchemyTelegramSubscriptionInterest",
    "TelegramSubscriptionInterestPort",
]
