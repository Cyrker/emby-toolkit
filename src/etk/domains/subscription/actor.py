from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, cast

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.platform.db import SqlAlchemyUnitOfWork

ACTOR_DEFAULT_CONFIG_KEY = "actor_subscriptions_default_config"

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ActorSubscriptionResult:
    payload: Any
    status_code: int = 200
    media_type: str = "application/json"


@dataclass(frozen=True, slots=True)
class ActorRefreshIntent:
    subscription_id: int
    actor_name: str


@dataclass(frozen=True, slots=True)
class ActorSubscriptionCancellation:
    actor_subscription_id: int
    tmdb_id: str
    item_type: str
    season_number: int | None


class DuplicateActorSubscriptionError(RuntimeError):
    pass


class ActorSubscriptionRepository(Protocol):
    def list_subscriptions(self) -> list[dict[str, Any]]: ...

    def get_subscription(self, subscription_id: int) -> dict[str, Any] | None: ...

    def create_subscription(
        self,
        *,
        tmdb_person_id: object,
        actor_name: object,
        profile_path: object,
        config: Mapping[str, Any],
    ) -> int: ...

    def update_subscription(self, subscription_id: int, data: Mapping[str, Any]) -> bool: ...

    def delete_subscription(self, subscription_id: int) -> None: ...


class ActorMediaProjectionPort(Protocol):
    def tracked_media(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> list[dict[str, Any]]: ...

    def reset_changed_configuration(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> None: ...

    def remove_subscription(
        self,
        uow: SqlAlchemyUnitOfWork,
        subscription_id: int,
    ) -> tuple[Mapping[str, Any], ...]: ...


class ActorCancellationIntentStagePort(Protocol):
    def stage(
        self,
        uow: SqlAlchemyUnitOfWork,
        intent: ActorSubscriptionCancellation,
    ) -> None: ...


class ActorDefaultConfigSettingsPort(Protocol):
    """Narrow view of the system standalone settings repository."""

    def load(self, key: str) -> Any: ...

    def save(self, key: str, value: Any) -> None: ...


class ActorSearchPort(Protocol):
    def configured(self) -> bool: ...

    def search(self, query: str) -> Sequence[Mapping[str, Any]] | None: ...


class ActorRefreshIntentPort(Protocol):
    def submit(self, intent: ActorRefreshIntent) -> bool: ...


class ActorRuntimePort(Protocol):
    def processor_ready(self) -> bool: ...

    def detail_context(self) -> Mapping[str, Any]: ...


class SqlAlchemyActorSubscriptionRepository:
    """SQLAlchemy UoW owner for the legacy ``actor_subscriptions`` table."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        media_projection: ActorMediaProjectionPort,
        cancellation_intent_stager: ActorCancellationIntentStagePort,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._media_projection = media_projection
        self._cancellation_intent_stager = cancellation_intent_stager

    def list_subscriptions(self) -> list[dict[str, Any]]:
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT id, tmdb_person_id, actor_name, profile_path, status, last_checked_at
                        FROM actor_subscriptions
                        ORDER BY added_at DESC
                        """
                    )
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in rows]

    def get_subscription(self, subscription_id: int) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM actor_subscriptions WHERE id = :subscription_id"),
                    {"subscription_id": subscription_id},
                )
                .mappings()
                .first()
            )
            if row is None:
                return None
            subscription = dict(cast(Mapping[str, Any], row))
            media_rows = self._media_projection.tracked_media(uow, subscription_id)

        return {
            "id": subscription["id"],
            "tmdb_person_id": subscription["tmdb_person_id"],
            "actor_name": subscription["actor_name"],
            "profile_path": subscription["profile_path"],
            "status": subscription["status"],
            "last_checked_at": subscription["last_checked_at"],
            "added_at": subscription["added_at"],
            "config": _config_from_row(subscription),
            "tracked_media": [_tracked_media(media_row) for media_row in media_rows],
        }

    def create_subscription(
        self,
        *,
        tmdb_person_id: object,
        actor_name: object,
        profile_path: object,
        config: Mapping[str, Any],
    ) -> int:
        start_year = config.get("start_year", 1900)
        raw_media_types = config.get("media_types", ["Movie", "TV"])
        media_types = ",".join(raw_media_types) if isinstance(raw_media_types, list) else str(raw_media_types)
        parameters = {
            "tmdb_person_id": tmdb_person_id,
            "actor_name": actor_name,
            "profile_path": profile_path,
            "status": "active",
            "start_year": start_year,
            "media_types": media_types,
            "genres_include": _safe_json_dumps(config.get("genres_include_json", [])),
            "genres_exclude": _safe_json_dumps(config.get("genres_exclude_json", [])),
            "min_rating": config.get("min_rating", 6.0),
            "main_role_only": config.get("main_role_only", False),
            "min_vote_count": config.get("min_vote_count", 10),
        }
        try:
            with self._unit_of_work() as uow:
                raw_subscription_id = uow.session.scalar(
                    text(
                        """
                        INSERT INTO actor_subscriptions (
                            tmdb_person_id, actor_name, profile_path, status,
                            config_start_year, config_media_types,
                            config_genres_include_json, config_genres_exclude_json,
                            config_min_rating, config_main_role_only, config_min_vote_count
                        )
                        VALUES (
                            :tmdb_person_id, :actor_name, :profile_path, :status,
                            :start_year, :media_types,
                            CAST(:genres_include AS jsonb), CAST(:genres_exclude AS jsonb),
                            :min_rating, :main_role_only, :min_vote_count
                        )
                        RETURNING id
                        """
                    ),
                    parameters,
                )
                if raw_subscription_id is None:
                    raise RuntimeError("database did not return the new actor subscription id")
                subscription_id = int(raw_subscription_id)
                uow.commit()
        except IntegrityError as error:
            raise DuplicateActorSubscriptionError from error
        return subscription_id

    def update_subscription(self, subscription_id: int, data: Mapping[str, Any]) -> bool:
        with self._unit_of_work() as uow:
            row = (
                uow.session.execute(
                    text("SELECT * FROM actor_subscriptions WHERE id = :subscription_id"),
                    {"subscription_id": subscription_id},
                )
                .mappings()
                .first()
            )
            if row is None:
                return False
            current = dict(row)
            current_config = _config_from_row(current)
            incoming_config = data.get("config", data)
            if not isinstance(incoming_config, Mapping):
                raise TypeError("actor subscription config must be an object")
            final_config = {**current_config, **dict(incoming_config)}
            media_types = ",".join(final_config.get("media_types", []))
            uow.session.execute(
                text(
                    """
                    UPDATE actor_subscriptions
                    SET status = :status,
                        config_start_year = :start_year,
                        config_media_types = :media_types,
                        config_genres_include_json = CAST(:genres_include AS jsonb),
                        config_genres_exclude_json = CAST(:genres_exclude AS jsonb),
                        config_min_rating = :min_rating,
                        config_main_role_only = :main_role_only,
                        config_min_vote_count = :min_vote_count
                    WHERE id = :subscription_id
                    """
                ),
                {
                    "subscription_id": subscription_id,
                    "status": data.get("status", current["status"]),
                    "start_year": final_config["start_year"],
                    "media_types": media_types,
                    "genres_include": json.dumps(final_config["genres_include_json"], ensure_ascii=False),
                    "genres_exclude": json.dumps(final_config["genres_exclude_json"], ensure_ascii=False),
                    "min_rating": final_config["min_rating"],
                    "main_role_only": final_config["main_role_only"],
                    "min_vote_count": final_config["min_vote_count"],
                },
            )
            if final_config != current_config:
                uow.session.execute(
                    text(
                        """
                        UPDATE actor_subscriptions
                        SET last_scanned_tmdb_ids_json = NULL
                        WHERE id = :subscription_id
                        """
                    ),
                    {"subscription_id": subscription_id},
                )
                self._media_projection.reset_changed_configuration(uow, subscription_id)
            uow.commit()
        return True

    def delete_subscription(self, subscription_id: int) -> None:
        with self._unit_of_work() as uow:
            subscription = (
                uow.session.execute(
                    text(
                        """
                        SELECT id
                        FROM actor_subscriptions
                        WHERE id = :subscription_id
                        """
                    ),
                    {"subscription_id": subscription_id},
                )
                .mappings()
                .first()
            )
            if subscription is None:
                return

            cancellations = self._media_projection.remove_subscription(uow, subscription_id)
            for cancellation in cancellations:
                self._cancellation_intent_stager.stage(
                    uow,
                    ActorSubscriptionCancellation(
                        actor_subscription_id=subscription_id,
                        tmdb_id=str(cancellation["tmdb_id"]),
                        item_type=str(cancellation["item_type"]),
                        season_number=_optional_season_number(cancellation.get("season_number")),
                    ),
                )

            uow.session.execute(
                text("DELETE FROM actor_subscriptions WHERE id = :subscription_id"),
                {"subscription_id": subscription_id},
            )
            uow.commit()


class ActorSubscriptionService:
    def __init__(
        self,
        *,
        repository: ActorSubscriptionRepository,
        default_settings: ActorDefaultConfigSettingsPort,
        search: ActorSearchPort,
        refresh_intents: ActorRefreshIntentPort,
        runtime: ActorRuntimePort,
    ) -> None:
        self._repository = repository
        self._default_settings = default_settings
        self._search = search
        self._refresh_intents = refresh_intents
        self._runtime = runtime

    def search(self, raw_query: object) -> ActorSubscriptionResult:
        if not self._runtime.processor_ready():
            return ActorSubscriptionResult({"error": "核心处理器未就绪。"}, 503)
        query = str(raw_query or "").strip()
        if not query:
            return ActorSubscriptionResult({"error": "必须提供搜索关键词 'name'"}, 400)
        if not self._search.configured():
            return ActorSubscriptionResult({"error": "服务器未配置TMDb API Key"}, 503)
        try:
            search_results = self._search.search(query)
            if search_results is None:
                return ActorSubscriptionResult({"error": "从TMDb搜索演员时发生错误"}, 500)
            formatted = []
            for person in search_results:
                if person.get("profile_path") and person.get("known_for"):
                    formatted.append(
                        {
                            "id": person.get("id"),
                            "name": person.get("name"),
                            "profile_path": person.get("profile_path"),
                            "known_for_department": person.get("known_for_department"),
                            "known_for": ", ".join(
                                item.get("title", item.get("name", "")) for item in person.get("known_for", [])
                            ),
                        }
                    )
            return ActorSubscriptionResult(formatted)
        except Exception:
            logger.exception("Actor subscription search failed")
            return ActorSubscriptionResult({"error": "搜索演员时发生未知的服务器错误"}, 500)

    def get_default_config(self) -> ActorSubscriptionResult:
        try:
            stored = self._default_settings.load(ACTOR_DEFAULT_CONFIG_KEY) or {}
            if not isinstance(stored, Mapping):
                raise TypeError("stored actor default config must be an object")
            return ActorSubscriptionResult(_default_config_response(stored))
        except Exception:
            logger.exception("Reading the default actor subscription config failed")
            return ActorSubscriptionResult({"error": "获取默认配置时发生服务器内部错误"}, 500)

    def save_default_config(self, value: Any) -> ActorSubscriptionResult:
        try:
            self._default_settings.save(ACTOR_DEFAULT_CONFIG_KEY, deepcopy(value))
            return ActorSubscriptionResult({"message": "默认配置已成功保存！"})
        except Exception:
            logger.exception("Saving the default actor subscription config failed")
            return ActorSubscriptionResult({"error": "保存默认配置时发生服务器内部错误"}, 500)

    def list_subscriptions(self) -> ActorSubscriptionResult:
        try:
            return ActorSubscriptionResult(self._repository.list_subscriptions())
        except Exception:
            logger.exception("Listing actor subscriptions failed")
            return ActorSubscriptionResult({"error": "获取订阅列表时发生服务器内部错误"}, 500)

    def create_subscription(self, data: Mapping[str, Any]) -> ActorSubscriptionResult:
        tmdb_person_id = data.get("tmdb_person_id")
        actor_name = data.get("actor_name")
        if not tmdb_person_id or not actor_name:
            return ActorSubscriptionResult({"error": "请求无效: 缺少 tmdb_person_id 或 actor_name"}, 400)
        config: Any = data.get("config")
        try:
            if not config:
                config = self._default_settings.load(ACTOR_DEFAULT_CONFIG_KEY) or {}
        except Exception:
            logger.exception("Reading actor defaults before subscription creation failed")
            return legacy_http_error_result(500)
        try:
            if not isinstance(config, Mapping):
                raise TypeError("actor subscription config must be an object")
            subscription_id = self._repository.create_subscription(
                tmdb_person_id=tmdb_person_id,
                actor_name=actor_name,
                profile_path=data.get("profile_path"),
                config=config,
            )
            return ActorSubscriptionResult(
                {"message": f"演员 {actor_name} 已成功订阅！", "id": subscription_id},
                201,
            )
        except DuplicateActorSubscriptionError:
            return ActorSubscriptionResult({"error": "该演员已经被订阅过了"}, 409)
        except Exception:
            logger.exception("Creating an actor subscription failed")
            return ActorSubscriptionResult({"error": "添加订阅时发生服务器内部错误"}, 500)

    def get_subscription(self, subscription_id: int) -> ActorSubscriptionResult:
        try:
            details = self._repository.get_subscription(subscription_id)
            if details is None:
                return ActorSubscriptionResult({"error": "未找到指定的订阅"}, 404)
            payload = deepcopy(details)
            payload.update(deepcopy(dict(self._runtime.detail_context())))
            return ActorSubscriptionResult(payload)
        except Exception:
            logger.exception("Reading actor subscription %s failed", subscription_id)
            return ActorSubscriptionResult({"error": "获取订阅详情时发生服务器内部错误"}, 500)

    def update_subscription(self, subscription_id: int, data: Mapping[str, Any]) -> ActorSubscriptionResult:
        try:
            if self._repository.update_subscription(subscription_id, data):
                return ActorSubscriptionResult({"message": "订阅已成功更新！"})
            return ActorSubscriptionResult({"error": "未找到指定的订阅"}, 404)
        except Exception:
            logger.exception("Updating actor subscription %s failed", subscription_id)
            return ActorSubscriptionResult({"error": "更新订阅时发生服务器内部错误"}, 500)

    def delete_subscription(self, subscription_id: int) -> ActorSubscriptionResult:
        try:
            self._repository.delete_subscription(subscription_id)
            return ActorSubscriptionResult({"message": "订阅已成功删除。"})
        except Exception:
            logger.exception("Deleting actor subscription %s failed", subscription_id)
            return ActorSubscriptionResult({"error": "删除订阅时发生服务器内部错误"}, 500)

    def refresh_subscription(self, subscription_id: int) -> ActorSubscriptionResult:
        try:
            details = self._repository.get_subscription(subscription_id)
        except Exception:
            logger.exception("Reading actor subscription %s before refresh failed", subscription_id)
            details = {}
        if details is None:
            return ActorSubscriptionResult({"error": f"未找到 ID 为 {subscription_id} 的订阅"}, 404)
        actor_name = str((details or {}).get("actor_name") or f"订阅ID {subscription_id}")
        try:
            # A symbolic intent is submitted; this boundary never executes a legacy callable.
            self._refresh_intents.submit(ActorRefreshIntent(subscription_id, actor_name))
        except Exception:
            logger.exception("Submitting actor subscription refresh %s failed", subscription_id)
            return legacy_http_error_result(500)
        return ActorSubscriptionResult({"message": f"刷新演员 {actor_name} 作品的任务已提交！"}, 202)


def legacy_http_error_result(status_code: int) -> ActorSubscriptionResult:
    if status_code != 500:
        raise ValueError("unsupported legacy actor subscription HTML error")
    return ActorSubscriptionResult(
        "<!doctype html>\n"
        "<html lang=en>\n"
        "<title>500 Internal Server Error</title>\n"
        "<h1>Internal Server Error</h1>\n"
        "<p>The server encountered an internal error and was unable to complete your request. "
        "Either the server is overloaded or there is an error in the application.</p>\n",
        500,
        "text/html",
    )


def _default_config_response(config: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "start_year": config.get("start_year"),
        "media_types": deepcopy(config.get("media_types", [])),
        "genres_include_json": deepcopy(config.get("genres_include_json", [])),
        "genres_exclude_json": deepcopy(config.get("genres_exclude_json", [])),
        "min_rating": config.get("min_rating", 0.0),
        "main_role_only": config.get("main_role_only", False),
        "min_vote_count": config.get("min_vote_count", 10),
    }


def _config_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "start_year": row.get("config_start_year"),
        "media_types": [item.strip() for item in str(row.get("config_media_types") or "").split(",") if item.strip()],
        "genres_include_json": deepcopy(row.get("config_genres_include_json") or []),
        "genres_exclude_json": deepcopy(row.get("config_genres_exclude_json") or []),
        "min_rating": float(row.get("config_min_rating", 0.0)),
        "main_role_only": row.get("config_main_role_only", False),
        "min_vote_count": row.get("config_min_vote_count", 10),
    }


def _tracked_media(media: dict[str, Any]) -> dict[str, Any]:
    if media.get("media_type") == "Season" and media.get("parent_title") and media.get("season_number") is not None:
        media["title"] = f"{media['parent_title']} 第 {media['season_number']} 季"
    if media.get("in_library"):
        status = "IN_LIBRARY"
    elif media.get("status") in {"SUBSCRIBED", "WANTED", "IGNORED"}:
        status = media["status"]
    else:
        release_date = media.get("release_date")
        status = (
            "PENDING_RELEASE"
            if release_date and release_date.strftime("%Y-%m-%d") > datetime.now().strftime("%Y-%m-%d")
            else "MISSING"
        )
    media["status"] = status
    emby_ids = media.get("emby_item_ids_json") or []
    media["emby_item_id"] = emby_ids[0] if emby_ids else None
    return media


def _safe_json_dumps(value: object) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            pass
    return json.dumps(value, ensure_ascii=False)


def _optional_season_number(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("actor cancellation season_number must be a non-negative integer or null")
    return value


__all__ = [
    "ACTOR_DEFAULT_CONFIG_KEY",
    "ActorCancellationIntentStagePort",
    "ActorDefaultConfigSettingsPort",
    "ActorMediaProjectionPort",
    "ActorRefreshIntent",
    "ActorRefreshIntentPort",
    "ActorRuntimePort",
    "ActorSearchPort",
    "ActorSubscriptionCancellation",
    "ActorSubscriptionRepository",
    "ActorSubscriptionResult",
    "ActorSubscriptionService",
    "DuplicateActorSubscriptionError",
    "SqlAlchemyActorSubscriptionRepository",
]
