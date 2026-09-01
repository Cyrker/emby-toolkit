from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Protocol
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import text

from etk.domains.media.daily_theme import DAILY_THEME_REPLENISHMENT_EFFECT_TYPE
from etk.domains.media.daily_theme_repository import remove_recommendation_in_uow
from etk.domains.media.effects import SqlAlchemyMediaEffectRepository
from etk.platform.db import SqlAlchemyUnitOfWork

from .operation_repository import PostgresSubscriptionOperationRepository
from .operations import (
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionQuotaRequirement,
)
from .runtime_repository import SubscriptionQuotaExhaustedError

PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER = "subscription.portal"


class PortalTmdbPort(Protocol):
    def movie(self, tmdb_id: int) -> Mapping[str, Any]: ...

    def tv(self, tmdb_id: int) -> Mapping[str, Any]: ...


class PortalSettingsPort(Protocol):
    def load(self, key: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class PortalSubscriptionHttpResult:
    payload: dict[str, Any]
    status_code: int = 200


@dataclass(frozen=True, slots=True)
class PortalMediaRecord:
    tmdb_id: str
    item_type: str
    title: str
    original_title: str | None
    release_date: str | None
    poster_path: str | None
    overview: str | None
    parent_series_tmdb_id: str | None = None
    season_number: int | None = None


@dataclass(frozen=True, slots=True)
class PortalProviderIntent:
    tmdb_id: str
    title: str
    item_type: str
    season_number: int | None = None


@dataclass(frozen=True, slots=True)
class PortalSubmissionResult:
    operation_ids: tuple[UUID, ...]
    response: Mapping[str, Any] | None = None
    replayed: bool = False
    recommendation_revision: int | None = None
    recommendation_pool_size: int | None = None


class PortalSubscriptionConflictError(RuntimeError):
    pass


class PortalSubscriptionRepository:
    """Local request state, quota, provider operations, and Outbox share one UoW."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        settings: PortalSettingsPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._unit_of_work = unit_of_work
        self._settings = settings
        self._clock = clock

    def can_subscribe_without_review(self, user_id: str) -> bool:
        with self._unit_of_work() as uow:
            return bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT COALESCE(templates.allow_unrestricted_subscriptions, FALSE)
                        FROM emby_users_extended AS extended
                        LEFT JOIN user_templates AS templates ON extended.template_id = templates.id
                        WHERE extended.emby_user_id = :user_id
                        """
                    ),
                    {"user_id": user_id},
                )
            )

    def moviepilot_config(self) -> dict[str, Any]:
        value = self._settings.load("mp_config")
        return dict(value) if isinstance(value, Mapping) else {}

    def current_status(self, tmdb_id: str, item_type: str) -> str | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT subscription_status FROM media_metadata
                    WHERE tmdb_id = :tmdb_id AND item_type = :item_type
                    """
                ),
                {"tmdb_id": tmdb_id, "item_type": item_type},
            )
            return str(value) if value is not None else None

    def submit(
        self,
        *,
        user_id: str,
        records: Sequence[PortalMediaRecord],
        target_status: str,
        provider_intents: Sequence[PortalProviderIntent],
        consume_quota: bool,
        recommendation_tmdb_id: str | None,
        request_item_type: str,
        request_tmdb_id: str,
        request_season_number: int | None,
        idempotency_key: str | None = None,
        response_payload: Mapping[str, Any] | None = None,
    ) -> PortalSubmissionResult:
        status = str(target_status or "").strip().upper()
        if status not in {"REQUESTED", "WANTED", "PENDING_RELEASE"}:
            raise ValueError("portal subscription target status is invalid")
        source = [{"type": "user_request", "user_id": user_id}]
        source_json = json.dumps(source, ensure_ascii=False, separators=(",", ":"))
        operation_ids: list[UUID] = []
        mp_config = self.moviepilot_config()
        daily_cap = _daily_cap(mp_config)
        quota_date = self._clock().astimezone(ZoneInfo("Asia/Shanghai")).date()
        normalized_idempotency = str(idempotency_key or "").strip() or None
        request_key_hash = (
            hashlib.sha256(f"{user_id}\0{normalized_idempotency}".encode()).hexdigest()
            if normalized_idempotency is not None
            else None
        )
        request_fingerprint = _portal_request_fingerprint(
            user_id=user_id,
            item_type=request_item_type,
            tmdb_id=request_tmdb_id,
            season_number=request_season_number,
        )
        accepted_response = dict(response_payload or {})
        scoped_intents = {_provider_scope(intent): intent for intent in provider_intents}
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:request_identity, 0))"),
                {
                    "request_identity": (
                        f"portal:key:{request_key_hash}"
                        if request_key_hash is not None
                        else f"portal:intent:{request_fingerprint}"
                    )
                },
            )
            if request_key_hash is not None:
                receipt = (
                    uow.session.execute(
                        text(
                            """
                        SELECT request_fingerprint, operation_ids, response_json
                        FROM portal_subscription_requests
                        WHERE request_key_hash = :request_key_hash
                        FOR UPDATE
                        """
                        ),
                        {"request_key_hash": request_key_hash},
                    )
                    .mappings()
                    .first()
                )
                if receipt is not None:
                    if str(receipt["request_fingerprint"]) != request_fingerprint:
                        raise PortalSubscriptionConflictError(
                            "portal subscription idempotency key was used for a different request"
                        )
                    uow.commit()
                    return PortalSubmissionResult(
                        tuple(UUID(str(operation_id)) for operation_id in receipt["operation_ids"]),
                        response=dict(receipt["response_json"]),
                        replayed=True,
                    )
            for scope_key in sorted(scoped_intents):
                uow.session.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:scope_key, 0))"),
                    {"scope_key": scope_key},
                )
            for record in records:
                _upsert_record(
                    uow,
                    record,
                    status=status,
                    source_json=source_json,
                )
            occurrence = request_key_hash or str(uuid4())
            for scope_key, provider_intent in scoped_intents.items():
                latest = (
                    uow.session.execute(
                        text(
                            """
                        SELECT id, status, request_json
                        FROM subscription_operations
                        WHERE scope_key = :scope_key
                        ORDER BY event_sequence DESC
                        LIMIT 1
                        FOR UPDATE
                        """
                        ),
                        {"scope_key": scope_key},
                    )
                    .mappings()
                    .first()
                )
                if latest is not None:
                    latest_status = str(latest["status"])
                    if latest_status == "indeterminate":
                        raise PortalSubscriptionConflictError(
                            "portal subscription has an indeterminate provider operation"
                        )
                    if latest_status in {
                        "ready",
                        "claimed",
                        "requesting",
                        "awaiting_readback",
                        "remote_confirmed",
                        "retryable",
                    }:
                        operation_ids.append(UUID(str(latest["id"])))
                        continue
                intent = _provider_operation(
                    provider_intent,
                    user_id=user_id,
                    quota=(
                        SubscriptionQuotaRequirement(
                            quota_date=quota_date,
                            daily_cap=daily_cap,
                            units=1,
                            ttl_seconds=3600,
                        )
                        if consume_quota
                        else None
                    ),
                    now=self._clock(),
                    occurrence=occurrence,
                    request_fingerprint=request_fingerprint,
                    request_key_hash=request_key_hash,
                    idempotency_key=(
                        f"portal:{request_key_hash}:{scope_key}" if request_key_hash is not None else None
                    ),
                )
                reservation = PostgresSubscriptionOperationRepository.reserve_in_uow(uow, intent)
                operation_ids.append(reservation.operation.id)
            if request_key_hash is not None:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO portal_subscription_requests (
                            id, request_key_hash, request_fingerprint, user_id,
                            response_json, operation_ids
                        ) VALUES (
                            :id, :request_key_hash, :request_fingerprint,
                            :user_id, CAST(:response_json AS jsonb),
                            CAST(:operation_ids AS uuid[])
                        )
                        """
                    ),
                    {
                        "id": uuid4(),
                        "request_key_hash": request_key_hash,
                        "request_fingerprint": request_fingerprint,
                        "user_id": user_id,
                        "response_json": json.dumps(
                            accepted_response,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                        "operation_ids": operation_ids,
                    },
                )
            recommendation_mutation = (
                remove_recommendation_in_uow(
                    uow,
                    tmdb_id=recommendation_tmdb_id,
                    mutation_id=f"portal:{occurrence}:remove:{recommendation_tmdb_id}",
                    occurred_at=self._clock().astimezone(UTC).isoformat(),
                )
                if recommendation_tmdb_id is not None
                else None
            )
            if recommendation_mutation is not None and recommendation_mutation.pool_size < 5:
                SqlAlchemyMediaEffectRepository(uow.session).reserve(
                    effect_key=f"daily-theme:replenishment:publication:{recommendation_mutation.revision}",
                    effect_type=DAILY_THEME_REPLENISHMENT_EFFECT_TYPE,
                    aggregate_type="daily_theme_publication",
                    aggregate_id=str(recommendation_mutation.revision),
                    payload={"publication_revision": recommendation_mutation.revision},
                )
            uow.commit()
        return PortalSubmissionResult(
            tuple(operation_ids),
            response=accepted_response,
            recommendation_revision=(recommendation_mutation.revision if recommendation_mutation else None),
            recommendation_pool_size=(recommendation_mutation.pool_size if recommendation_mutation else None),
        )


class PortalSubscriptionService:
    def __init__(
        self,
        repository: PortalSubscriptionRepository,
        tmdb: PortalTmdbPort,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._tmdb = tmdb
        self._clock = clock

    def submit(
        self,
        *,
        user_id: str,
        username: str,
        is_admin: bool,
        payload: Any,
        idempotency_key: str | None = None,
    ) -> PortalSubscriptionHttpResult:
        del username
        if not isinstance(payload, Mapping):
            return PortalSubscriptionHttpResult({"status": "error", "message": "订阅参数无效"}, 400)
        item_type = str(payload.get("item_type") or "").strip()
        if item_type not in {"Movie", "Series", "Season"}:
            return PortalSubscriptionHttpResult({"status": "error", "message": "订阅参数无效"}, 400)
        try:
            tmdb_id = _integer(payload.get("tmdb_id"))
        except (TypeError, ValueError):
            return PortalSubscriptionHttpResult({"status": "error", "message": "订阅参数无效"}, 400)
        if tmdb_id < 1:
            return PortalSubscriptionHttpResult({"status": "error", "message": "订阅参数无效"}, 400)
        try:
            details = dict(self._tmdb.movie(tmdb_id) if item_type == "Movie" else self._tmdb.tv(tmdb_id))
        except Exception:
            return PortalSubscriptionHttpResult(
                {"status": "error", "message": "无法获取媒体详情，请稍后再试。"},
                500,
            )
        if not details:
            return PortalSubscriptionHttpResult(
                {"status": "error", "message": "无法获取媒体详情，请稍后再试。"},
                500,
            )
        try:
            records, provider_intents, primary = _records(
                item_type,
                tmdb_id,
                details,
                season_number=payload.get("season_number"),
                season_tmdb_id=payload.get("season_tmdb_id"),
                fallback_name=str(payload.get("item_name") or ""),
            )
        except ValueError as exc:
            return PortalSubscriptionHttpResult({"status": "error", "message": str(exc)}, 400)

        mp_config = self._repository.moviepilot_config()
        mp_configured = bool(str(mp_config.get("moviepilot_url") or "").strip())
        unrestricted = bool(is_admin or self._repository.can_subscribe_without_review(user_id))
        if not unrestricted and mp_configured:
            existing = self._repository.current_status(primary.tmdb_id, primary.item_type)
            if existing and existing != "NONE":
                message = "该项目正在等待审核。" if existing == "REQUESTED" else "该项目已在订阅队列中。"
                response_status = "pending" if existing == "REQUESTED" else existing
            else:
                response_status = "pending"
                message = "“想看”请求已提交，请等待管理员审核。"
            target_status = "REQUESTED"
            provider_intents = ()
        elif item_type != "Series" and _future_release(primary.release_date, self._clock().date()):
            target_status = "PENDING_RELEASE"
            provider_intents = ()
            response_status = "pending"
            message = "该项目尚未发行，已为您加入待发行监控队列。"
        else:
            target_status = "WANTED"
            if not mp_configured:
                provider_intents = ()
                response_status = "WANTED"
                message = "已标记为想看/待订阅。MoviePilot 未配置，已跳过自动订阅。"
            else:
                response_status = "approved"
                message = "订阅请求已提交，系统将自动处理！"
        try:
            normalized_idempotency = str(idempotency_key or "").strip() or None
            submission = self._repository.submit(
                user_id=user_id,
                records=records,
                target_status=target_status,
                provider_intents=provider_intents,
                consume_quota=bool(provider_intents),
                recommendation_tmdb_id=str(tmdb_id) if item_type == "Movie" else None,
                request_item_type=item_type,
                request_tmdb_id=str(tmdb_id),
                request_season_number=primary.season_number if item_type == "Season" else None,
                idempotency_key=normalized_idempotency,
                response_payload={"status": response_status, "message": message},
            )
        except PortalSubscriptionConflictError:
            return PortalSubscriptionHttpResult(
                {"status": "error", "message": "订阅请求与已有进行中或待确认操作冲突"},
                409,
            )
        except SubscriptionQuotaExhaustedError:
            return PortalSubscriptionHttpResult(
                {"status": "error", "message": "今日订阅额度已用尽"},
                409,
            )
        except Exception:
            return PortalSubscriptionHttpResult({"status": "error", "message": "提交订阅请求失败"}, 500)
        return PortalSubscriptionHttpResult(
            dict(submission.response or {"status": response_status, "message": message})
        )


def _records(
    item_type: str,
    tmdb_id: int,
    details: Mapping[str, Any],
    *,
    season_number: object,
    season_tmdb_id: object,
    fallback_name: str,
) -> tuple[tuple[PortalMediaRecord, ...], tuple[PortalProviderIntent, ...], PortalMediaRecord]:
    tmdb_text = str(tmdb_id)
    title = str(details.get("title") or details.get("name") or fallback_name or "").strip()
    original_title = _optional_text(details.get("original_title") or details.get("original_name"))
    release_date = _optional_text(details.get("release_date") or details.get("first_air_date"))
    poster_path = _optional_text(details.get("poster_path"))
    overview = _optional_text(details.get("overview"))
    if item_type == "Movie":
        record = PortalMediaRecord(
            tmdb_text,
            "Movie",
            title,
            original_title,
            release_date,
            poster_path,
            overview,
        )
        return (record,), (PortalProviderIntent(tmdb_text, title, "Movie"),), record

    parent = PortalMediaRecord(
        tmdb_text,
        "Series",
        title,
        original_title,
        release_date,
        poster_path,
        overview,
    )
    seasons = [season for season in details.get("seasons", []) if isinstance(season, Mapping)]
    if item_type == "Season":
        try:
            number = _integer(season_number)
        except (TypeError, ValueError) as exc:
            raise ValueError("季号无效") from exc
        if number < 1:
            raise ValueError("季号无效")
        season = next(
            (value for value in seasons if _optional_integer(value.get("season_number")) == number),
            None,
        )
        if season is None:
            raise ValueError("未找到可订阅的季。")
        local_id = str(season.get("id") or f"{tmdb_id}_S{number}")
        supplied_season_id = str(season_tmdb_id or "").strip()
        if supplied_season_id and supplied_season_id != local_id:
            raise ValueError("季信息不匹配")
        record = PortalMediaRecord(
            local_id,
            "Season",
            f"{title} 第 {number} 季",
            None,
            _optional_text(season.get("air_date")),
            _optional_text(season.get("poster_path")) or poster_path,
            _optional_text(season.get("overview")),
            parent_series_tmdb_id=tmdb_text,
            season_number=number,
        )
        return (
            (
                parent,
                record,
            ),
            (PortalProviderIntent(tmdb_text, title, "Season", number),),
            record,
        )

    valid_seasons: list[PortalMediaRecord] = []
    provider: list[PortalProviderIntent] = []
    for season in seasons:
        try:
            number = _integer(season.get("season_number"))
        except (TypeError, ValueError):
            continue
        if number < 1:
            continue
        valid_seasons.append(
            PortalMediaRecord(
                str(season.get("id") or f"{tmdb_id}_S{number}"),
                "Season",
                f"{title} 第 {number} 季",
                None,
                _optional_text(season.get("air_date")),
                _optional_text(season.get("poster_path")) or poster_path,
                _optional_text(season.get("overview")),
                parent_series_tmdb_id=tmdb_text,
                season_number=number,
            )
        )
        provider.append(PortalProviderIntent(tmdb_text, title, "Season", number))
    if not valid_seasons:
        raise ValueError("未找到可订阅的季。")
    return (parent, *valid_seasons), tuple(provider), parent


def _upsert_record(
    uow: SqlAlchemyUnitOfWork,
    record: PortalMediaRecord,
    *,
    status: str,
    source_json: str,
) -> None:
    record_status = "NONE" if record.item_type == "Series" and status == "PENDING_RELEASE" else status
    uow.session.execute(
        text(
            """
            INSERT INTO media_metadata (
                tmdb_id, item_type, title, original_title, release_date,
                poster_path, overview, parent_series_tmdb_id, season_number,
                subscription_status, subscription_sources_json,
                first_requested_at, last_updated_at
            ) VALUES (
                :tmdb_id, :item_type, :title, :original_title, CAST(:release_date AS date),
                :poster_path, :overview, :parent_series_tmdb_id, :season_number,
                :status, CAST(:source_json AS jsonb), NOW(), NOW()
            )
            ON CONFLICT (tmdb_id, item_type) DO UPDATE SET
                title = COALESCE(EXCLUDED.title, media_metadata.title),
                original_title = COALESCE(EXCLUDED.original_title, media_metadata.original_title),
                release_date = COALESCE(EXCLUDED.release_date, media_metadata.release_date),
                poster_path = COALESCE(EXCLUDED.poster_path, media_metadata.poster_path),
                overview = COALESCE(EXCLUDED.overview, media_metadata.overview),
                parent_series_tmdb_id = COALESCE(
                    EXCLUDED.parent_series_tmdb_id, media_metadata.parent_series_tmdb_id
                ),
                season_number = COALESCE(EXCLUDED.season_number, media_metadata.season_number),
                subscription_status = CASE
                    WHEN media_metadata.in_library THEN media_metadata.subscription_status
                    WHEN EXCLUDED.subscription_status = 'NONE'
                        THEN media_metadata.subscription_status
                    WHEN media_metadata.subscription_status = 'IGNORED'
                        AND COALESCE(media_metadata.ignore_reason, '') IN ('手动忽略', '订阅超时')
                        THEN media_metadata.subscription_status
                    WHEN media_metadata.subscription_status IN ('SUBSCRIBED', 'PAUSED')
                        THEN media_metadata.subscription_status
                    WHEN EXCLUDED.subscription_status = 'PENDING_RELEASE'
                        AND media_metadata.subscription_status = 'WANTED'
                        THEN media_metadata.subscription_status
                    WHEN EXCLUDED.subscription_status = 'REQUESTED'
                        AND media_metadata.subscription_status NOT IN ('NONE', 'REQUESTED')
                        THEN media_metadata.subscription_status
                    ELSE EXCLUDED.subscription_status
                END,
                subscription_sources_json = CASE
                    WHEN media_metadata.subscription_status = 'IGNORED'
                        AND COALESCE(media_metadata.ignore_reason, '') IN ('手动忽略', '订阅超时')
                        THEN media_metadata.subscription_sources_json
                    WHEN media_metadata.subscription_sources_json @> CAST(:source_json AS jsonb)
                        THEN media_metadata.subscription_sources_json
                    ELSE media_metadata.subscription_sources_json || CAST(:source_json AS jsonb)
                END,
                first_requested_at = COALESCE(media_metadata.first_requested_at, NOW()),
                last_updated_at = NOW()
            """
        ),
        {
            "tmdb_id": record.tmdb_id,
            "item_type": record.item_type,
            "title": record.title,
            "original_title": record.original_title,
            "release_date": record.release_date,
            "poster_path": record.poster_path,
            "overview": record.overview,
            "parent_series_tmdb_id": record.parent_series_tmdb_id,
            "season_number": record.season_number,
            "status": record_status,
            "source_json": source_json,
        },
    )


def _provider_operation(
    intent: PortalProviderIntent,
    *,
    user_id: str,
    quota: SubscriptionQuotaRequirement | None,
    now: datetime,
    occurrence: str | None,
    request_fingerprint: str,
    request_key_hash: str | None,
    idempotency_key: str | None,
) -> SubscriptionOperationIntent:
    payload: dict[str, Any] = {
        "name": intent.title,
        "tmdbid": int(intent.tmdb_id),
        "type": "电影" if intent.item_type == "Movie" else "电视剧",
    }
    if intent.season_number is not None:
        payload["season"] = intent.season_number
    request: dict[str, Any] = {
        "action": "create",
        "tmdb_id": intent.tmdb_id,
        "payload": payload,
        "expected": {},
        "preflight_readback": True,
        "requested_by_user_id": user_id,
        "portal_request_fingerprint": request_fingerprint,
        "local_continuation": {
            "type": "mark_media_subscribed",
            "tmdb_id": intent.tmdb_id,
            "media_type": "Movie" if intent.item_type == "Movie" else "Season",
            "season": intent.season_number,
        },
    }
    if request_key_hash is not None:
        request["portal_request_key_hash"] = request_key_hash
    if occurrence is not None:
        request["request_id"] = occurrence
    if intent.season_number is not None:
        request["season"] = intent.season_number
    return SubscriptionOperationIntent.create(
        kind=SubscriptionOperationKind.CREATE,
        scope_key=_provider_scope(intent),
        request=request,
        available_at=now,
        quota=quota,
        idempotency_key=idempotency_key,
    )


def _provider_scope(intent: PortalProviderIntent) -> str:
    return (
        f"portal:provider:create:{intent.tmdb_id}:"
        f"season:{intent.season_number if intent.season_number is not None else 'all'}"
    )


def _portal_request_fingerprint(
    *,
    user_id: str,
    item_type: str,
    tmdb_id: str,
    season_number: int | None,
) -> str:
    stable = {
        "user_id": user_id,
        "item_type": item_type,
        "tmdb_id": tmdb_id,
        "season_number": season_number,
    }
    return hashlib.sha256(
        json.dumps(stable, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _daily_cap(config: Mapping[str, Any]) -> int:
    value = config.get("resubscribe_daily_cap", 200)
    if isinstance(value, bool):
        return 200
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 200


def _future_release(value: str | None, today: date) -> bool:
    if not value:
        return False
    try:
        return date.fromisoformat(value[:10]) > today
    except ValueError:
        return False


def _optional_text(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _integer(value: object) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer")
    return int(str(value))


def _optional_integer(value: object) -> int | None:
    try:
        return _integer(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "PORTAL_SUBSCRIPTION_USE_CASE_PROVIDER",
    "PortalMediaRecord",
    "PortalProviderIntent",
    "PortalSettingsPort",
    "PortalSubmissionResult",
    "PortalSubscriptionHttpResult",
    "PortalSubscriptionRepository",
    "PortalSubscriptionService",
    "PortalTmdbPort",
]
