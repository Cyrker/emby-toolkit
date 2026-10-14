from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, NoReturn, Protocol

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from etk.domains.media.effect_execution import (
    MediaEffectDeliveryIndeterminate,
    MediaEffectDeliveryRejected,
    MediaEffectDeliveryRetryable,
)
from etk.domains.media.effects import (
    MediaEffectConflictError,
    MediaEffectProjection,
    PostgresMediaEffectLedger,
)
from etk.domains.media.models import ServiceResult
from etk.domains.media.subscription_projection import (
    SqlAlchemySubscriptionMediaProjection,
    SubscriptionMediaKey,
)
from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json

from .moviepilot import MoviePilotActiveDownload
from .moviepilot_transfer import (
    MoviePilotCredentialStore,
    MoviePilotTransferPreparationError,
    moviepilot_credential_context,
)
from .operation_repository import PostgresSubscriptionOperationRepository
from .operations import (
    SubscriptionOperation,
    SubscriptionOperationIntent,
    SubscriptionOperationKind,
    SubscriptionOperationStatus,
)

MOVIEPILOT_ASSISTANT_EVENT_TYPE = "moviepilot.webhook.assistant"
logger = logging.getLogger(__name__)
MOVIEPILOT_TRANSFER_EVENT_TYPE = "moviepilot.webhook.transfer"
MOVIEPILOT_WEBHOOK_EFFECT_TYPES = frozenset({MOVIEPILOT_ASSISTANT_EVENT_TYPE, MOVIEPILOT_TRANSFER_EVENT_TYPE})

_ASSISTANT_EVENTS = frozenset(
    {
        "download.added",
        "subscribe.added",
        "subscribe.modified",
        "subscribe.deleted",
        "subscribe.complete",
    }
)
_TRANSFER_EVENTS = frozenset({"transfer.complete", "transfer.subtitle.complete"})
_ASSISTANT_SUBSCRIPTIONS_KEY = "subscribes"
_ASSISTANT_TORRENTS_KEY = "torrents"
_ASSISTANT_WEBHOOK_RECEIPTS_KEY = "moviepilot_webhook_receipts"
_ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY = "snapshot_restore_suppressions"
_SOURCE_DOWNLOAD_PENDING = "download_pending"
_SOURCE_MANUAL_MP = "manual_mp"
_EXPECTED_UPDATE_TTL_SECONDS = 300
_MAX_RECEIPTS = 2_000


class MoviePilotAssistantReadbackUnavailable(RuntimeError):
    pass


class MoviePilotWebhookSettings(Protocol):
    def load(self, key: str) -> Any: ...


class MoviePilotTransferProcessor(Protocol):
    def process(
        self,
        *,
        event_key: str,
        event_type: str,
        file: Mapping[str, Any],
        passthrough: bool,
    ) -> Mapping[str, Any]: ...


class MoviePilotAssistantEventRepository(Protocol):
    def full_washing_started_at(self, *, payload: Mapping[str, Any]) -> float: ...

    def apply(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...

    def prepare_full_washing_timeout(
        self,
        *,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        suppression_days: int,
    ) -> bool: ...

    def finalize_full_washing_timeout(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        suppression_days: int,
    ) -> Mapping[str, Any]: ...

    def finalize_full_washing_failure(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        phase: str,
        operation_id: str,
        operation_status: SubscriptionOperationStatus,
    ) -> Mapping[str, Any]: ...


class MoviePilotWebhookService:
    """Normalize MoviePilot events and reserve one durable Worker-owned effect."""

    def __init__(
        self,
        ledger: PostgresMediaEffectLedger,
        settings: MoviePilotWebhookSettings,
        runtime_settings: Callable[[], Settings],
        credentials: MoviePilotCredentialStore,
    ) -> None:
        self._ledger = ledger
        self._settings = settings
        self._runtime_settings = runtime_settings
        self._credentials = credentials

    def handle(self, payload: Mapping[str, Any]) -> ServiceResult:
        normalized = dict(payload)
        if normalized.get("Event"):
            return ServiceResult({"status": "emby_webhook_removed"})

        event_type = str(normalized.get("type") or "").strip()
        data = _mapping(normalized.get("data"))
        media = _mapping(data.get("mediainfo"))
        logger.info(
            "收到 MoviePilot 通知：事件=%s，媒体=%s",
            event_type or "未提供事件类型",
            media.get("title") or data.get("title") or "未提供标题",
        )
        if event_type in _ASSISTANT_EVENTS:
            if not _assistant_enabled(self._settings):
                logger.info("MoviePilot 通知已跳过：订阅助手未启用，事件=%s", event_type)
                return ServiceResult({"status": "subscribe_assistant_ignored"})
            if not _assistant_event_is_actionable(event_type, normalized):
                logger.info("MoviePilot 通知已跳过：缺少可处理的订阅或下载身份，事件=%s", event_type)
                return ServiceResult({"status": "subscribe_assistant_ignored"})
            try:
                self._reserve(
                    event_type=event_type,
                    effect_type=MOVIEPILOT_ASSISTANT_EVENT_TYPE,
                    payload=_assistant_effect_payload(event_type, normalized),
                    aggregate_id=_assistant_aggregate_id(event_type, normalized),
                    identity_payload=normalized,
                )
            except MediaEffectConflictError:
                return ServiceResult({"status": "event_id_conflict"}, 409)
            return ServiceResult({"status": "subscribe_assistant_processed"})

        if event_type in _TRANSFER_EVENTS:
            transfer = _transfer_file(normalized)
            if transfer is None:
                logger.info("MoviePilot 整理通知未进入 115 整理队列：缺少所需文件数据；本地媒体由文件监控处理")
                return ServiceResult({"status": "ignored_missing_data"})
            if transfer["file_type"] != "file":
                return ServiceResult({"status": "ignored_not_file"})
            runtime = self._runtime_settings()
            passthrough = bool(runtime.p115_mp_classify)
            binding = moviepilot_credential_context(
                self._credentials.load(),
                str(runtime.p115_api_priority),
            )
            effect_payload = {
                "event_type": event_type,
                "file": {**transfer, **binding},
                "passthrough": passthrough,
            }
            try:
                self._reserve(
                    event_type=event_type,
                    effect_type=MOVIEPILOT_TRANSFER_EVENT_TYPE,
                    payload=effect_payload,
                    aggregate_id=f"{transfer['tmdb_id']}:{transfer['file_id']}",
                    identity_payload=normalized,
                )
            except MediaEffectConflictError:
                return ServiceResult({"status": "event_id_conflict"}, 409)
            return ServiceResult(
                {"status": ("processing_single_file_passthrough" if passthrough else "processing_single_file")}
            )

        logger.info("MoviePilot 通知已记录但不触发任务：事件 %s 不在当前触发列表", event_type)
        return ServiceResult({"status": "event_ignored_not_in_trigger_list"})

    def _reserve(
        self,
        *,
        event_type: str,
        effect_type: str,
        payload: Mapping[str, Any],
        aggregate_id: str,
        identity_payload: Mapping[str, Any] | None = None,
    ) -> MediaEffectProjection:
        identity = identity_payload or payload
        event_key = _event_key(event_type, identity)
        effect_payload = {
            "event_key": event_key,
            "event_type": event_type,
            "payload_digest": _payload_digest(identity),
            "payload": dict(payload),
        }
        effect = self._ledger.reserve(
            effect_key=f"p11:moviepilot-webhook:{event_key}",
            effect_type=effect_type,
            aggregate_type="moviepilot_webhook",
            aggregate_id=aggregate_id,
            payload=effect_payload,
        ).effect
        logger.info(
            "MoviePilot 通知已持久化，等待后台处理：事件=%s，记录=%s",
            event_type,
            effect.id,
        )
        return effect


class SqlAlchemyMoviePilotAssistantEventRepository:
    """Apply one assistant event and its replay receipt in one PostgreSQL transaction."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        projection: SqlAlchemySubscriptionMediaProjection | None = None,
        full_washing_guard: Callable[[], bool] | None = None,
        active_downloads: Callable[[], Sequence[MoviePilotActiveDownload]] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._projection = projection or SqlAlchemySubscriptionMediaProjection()
        self._full_washing_guard = full_washing_guard or (lambda: True)
        self._active_downloads = active_downloads

    def full_washing_started_at(self, *, payload: Mapping[str, Any]) -> float:
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id is None:
            return 0
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            current = _mapping(subscriptions.get(str(subscribe_id)))
            if not current.get("full_washing"):
                return 0
            current_tmdb_id = str(current.get("tmdb_id") or "").strip()
            current_season = _positive_int(current.get("season"))
            if not tmdb_id or season is None or current_tmdb_id != tmdb_id or current_season != season:
                return 0
            return _state_timestamp(current.get("full_washing_started_at"), 0)

    def apply(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if event_type not in _ASSISTANT_EVENTS:
            raise ValueError(f"unsupported MoviePilot assistant event: {event_type}")
        data = payload.get("data")
        event_data = dict(data) if isinstance(data, Mapping) else {}
        replayed = self._receipt_result(event_key, payload_digest)
        if replayed is not None:
            return replayed
        terminal = self._completion_terminal_result(event_type, event_data)
        if terminal is not None:
            return terminal
        full_washing_guard_enabled = bool(self._full_washing_guard()) if event_type == "subscribe.complete" else True
        active_downloads: tuple[MoviePilotActiveDownload, ...] | None = None
        active_state_digest: str | None = None
        if (
            event_type == "subscribe.complete"
            and full_washing_guard_enabled
            and _full_washing_requested(event_data)
            and self._active_downloads is not None
        ):
            try:
                active_state_digest = self._download_state_digest(event_data)
                active_downloads = tuple(self._active_downloads())
                if any(not isinstance(value, MoviePilotActiveDownload) for value in active_downloads):
                    raise TypeError("MoviePilot active download readback returned an invalid item")
            except Exception as exc:
                raise MoviePilotAssistantReadbackUnavailable("MoviePilot active download readback failed") from exc
        with self._unit_of_work() as uow:
            receipts = self._locked_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY)
            existing = receipts.get(event_key)
            if isinstance(existing, Mapping):
                if str(existing.get("payload_digest") or "") != payload_digest:
                    raise ValueError("MoviePilot event identity was reused with a different payload")
                result = existing.get("result")
                return dict(result) if isinstance(result, Mapping) else {"handled": True, "replayed": True}

            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            subscribe_id = _positive_int(event_data.get("subscribe_id") or _subscription_info(event_data).get("id"))
            if (
                active_state_digest is not None
                and subscribe_id is not None
                and _assistant_download_state_digest(subscriptions, torrents, subscribe_id) != active_state_digest
            ):
                raise MoviePilotAssistantReadbackUnavailable(
                    "MoviePilot assistant state changed during active-download readback"
                )
            suppressions = self._locked_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY)
            result = self._apply_event(
                uow,
                event_type=event_type,
                data=event_data,
                subscriptions=subscriptions,
                torrents=torrents,
                suppressions=suppressions,
                full_washing_guard_enabled=full_washing_guard_enabled,
                active_downloads=active_downloads,
            )
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
            self._write_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY, suppressions)
            if not result.get("completion_deferred"):
                receipts[event_key] = {
                    "payload_digest": payload_digest,
                    "event_type": event_type,
                    "result": dict(result),
                    "processed_at": datetime.now(UTC).isoformat(),
                }
                if len(receipts) > _MAX_RECEIPTS:
                    ordered = sorted(
                        receipts,
                        key=lambda key: str(
                            receipts[key].get("processed_at") if isinstance(receipts[key], Mapping) else ""
                        ),
                    )
                    for stale_key in ordered[: len(receipts) - _MAX_RECEIPTS]:
                        receipts.pop(stale_key, None)
            self._write_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY, receipts)
            uow.commit()
        return result

    def prepare_full_washing_timeout(
        self,
        *,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        suppression_days: int,
    ) -> bool:
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        if subscribe_id is None or not tmdb_id or season is None:
            raise ValueError("full-washing timeout requires subscription, TMDb, and season identity")
        now = datetime.now(UTC).timestamp()
        cycle = _full_washing_cycle_token(cycle_started_at)
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            current = dict(_mapping(subscriptions.get(str(subscribe_id))))
            current_cycle = _current_full_washing_cycle(current)
            if (
                current_cycle != cycle
                or str(current.get("tmdb_id") or "").strip() != tmdb_id
                or _positive_int(current.get("season")) != season
            ):
                return False
            current["washing_timeout_started_at"] = _state_timestamp(
                current.get("washing_timeout_started_at"),
                now,
            )
            current["washing_timeout_cycle"] = cycle
            subscriptions[str(subscribe_id)] = current
            suppressions = self._locked_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY)
            _record_snapshot_suppressions(
                suppressions,
                tmdb_id=tmdb_id,
                season=season,
                subscribe_id=subscribe_id,
                reason="full_washing_timeout_pending",
                now=now,
                retention_days=suppression_days,
            )
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()
        return True

    def finalize_full_washing_timeout(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        suppression_days: int,
    ) -> Mapping[str, Any]:
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        if subscribe_id is None or not tmdb_id or season is None:
            raise ValueError("full-washing timeout requires subscription, TMDb, and season identity")
        now = datetime.now(UTC).timestamp()
        cycle = _full_washing_cycle_token(cycle_started_at)
        with self._unit_of_work() as uow:
            receipts = self._locked_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY)
            existing = receipts.get(event_key)
            if isinstance(existing, Mapping):
                if str(existing.get("payload_digest") or "") != payload_digest:
                    raise ValueError("MoviePilot event identity was reused with a different payload")
                result = existing.get("result")
                return dict(result) if isinstance(result, Mapping) else {"handled": True, "replayed": True}
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            suppressions = self._locked_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY)
            current = dict(_mapping(subscriptions.get(str(subscribe_id))))
            if _current_full_washing_cycle(current) != cycle or current.get("washing_timeout_cycle") != cycle:
                result = {
                    "handled": True,
                    "subscribe_id": subscribe_id,
                    "completion_superseded": True,
                    "expected_cycle": cycle,
                    "current_cycle": _current_full_washing_cycle(current),
                }
                receipts[event_key] = {
                    "payload_digest": payload_digest,
                    "event_type": event_type,
                    "result": result,
                    "processed_at": datetime.now(UTC).isoformat(),
                }
                self._write_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY, receipts)
                uow.commit()
                return result
            _remember_subscription(
                current,
                subscribe_id=subscribe_id,
                info=info,
                reason="full_washing_timeout",
                now=now,
            )
            current["deleted"] = True
            current["active_sources"] = {}
            current["snapshot_restore_suppressed"] = True
            current.pop("full_washing", None)
            current.pop("full_washing_started_at", None)
            current.pop("washing_timeout_started_at", None)
            current.pop("washing_timeout_cycle", None)
            subscriptions[str(subscribe_id)] = current
            removed_hashes = _remove_torrents(torrents, subscribe_id)
            _record_snapshot_suppressions(
                suppressions,
                tmdb_id=tmdb_id,
                season=season,
                subscribe_id=subscribe_id,
                reason="full_washing_timeout_confirmed",
                now=now,
                retention_days=suppression_days,
            )
            _set_season_active_washing(uow, tmdb_id, season, False)
            result = {
                "handled": True,
                "subscribe_id": subscribe_id,
                "snapshot_created": False,
                "torrents_removed": len(removed_hashes),
                "completion_timed_out": True,
            }
            receipts[event_key] = {
                "payload_digest": payload_digest,
                "event_type": event_type,
                "result": result,
                "processed_at": datetime.now(UTC).isoformat(),
            }
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
            self._write_state(uow, _ASSISTANT_SNAPSHOT_SUPPRESSIONS_KEY, suppressions)
            self._write_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY, receipts)
            uow.commit()
        return result

    def finalize_full_washing_failure(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        phase: str,
        operation_id: str,
        operation_status: SubscriptionOperationStatus,
    ) -> Mapping[str, Any]:
        if operation_status not in {
            SubscriptionOperationStatus.REJECTED,
            SubscriptionOperationStatus.CANCELLED,
            SubscriptionOperationStatus.INDETERMINATE,
        }:
            raise ValueError("full-washing failure requires a terminal unsuccessful operation")
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        if subscribe_id is None or not tmdb_id or season is None:
            raise ValueError("full-washing failure requires subscription, TMDb, and season identity")
        cycle = _full_washing_cycle_token(cycle_started_at)
        with self._unit_of_work() as uow:
            receipts = self._locked_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY)
            existing = receipts.get(event_key)
            if isinstance(existing, Mapping):
                if str(existing.get("payload_digest") or "") != payload_digest:
                    raise ValueError("MoviePilot event identity was reused with a different payload")
                result = existing.get("result")
                return dict(result) if isinstance(result, Mapping) else {"handled": True, "replayed": True}
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            current = dict(_mapping(subscriptions.get(str(subscribe_id))))
            if _current_full_washing_cycle(current) != cycle:
                result = {
                    "handled": True,
                    "subscribe_id": subscribe_id,
                    "completion_superseded": True,
                    "expected_cycle": cycle,
                    "current_cycle": _current_full_washing_cycle(current),
                }
                receipts[event_key] = {
                    "payload_digest": payload_digest,
                    "event_type": event_type,
                    "result": result,
                    "processed_at": datetime.now(UTC).isoformat(),
                }
                self._write_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY, receipts)
                uow.commit()
                return result
            pending_download = _has_torrent_for_subscription(torrents, subscribe_id)
            sources = dict(_mapping(current.get("active_sources")))
            if pending_download:
                sources.setdefault(_SOURCE_DOWNLOAD_PENDING, "下载状态待人工核对")
            else:
                sources.pop(_SOURCE_DOWNLOAD_PENDING, None)
            current["active_sources"] = sources
            current.pop("full_washing", None)
            current.pop("full_washing_started_at", None)
            current.pop("washing_timeout_started_at", None)
            current.pop("washing_timeout_cycle", None)
            current["full_washing_failure"] = {
                "completion_digest": _completion_fact_digest(subscribe_id, info, media),
                "phase": phase,
                "operation_id": operation_id,
                "operation_status": operation_status.value,
                "download_pending": pending_download,
                "failed_at": datetime.now(UTC).timestamp(),
            }
            current["updated_at"] = datetime.now(UTC).timestamp()
            subscriptions[str(subscribe_id)] = current
            _set_season_active_washing(uow, tmdb_id, season, pending_download)
            result = {
                "handled": True,
                "subscribe_id": subscribe_id,
                "completion_failed": True,
                "phase": phase,
                "operation_id": operation_id,
                "operation_status": operation_status.value,
                "download_pending": pending_download,
            }
            receipts[event_key] = {
                "payload_digest": payload_digest,
                "event_type": event_type,
                "result": result,
                "processed_at": datetime.now(UTC).isoformat(),
            }
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_WEBHOOK_RECEIPTS_KEY, receipts)
            uow.commit()
        return result

    def _apply_event(
        self,
        uow: SqlAlchemyUnitOfWork,
        *,
        event_type: str,
        data: Mapping[str, Any],
        subscriptions: dict[str, Any],
        torrents: dict[str, Any],
        suppressions: dict[str, Any],
        full_washing_guard_enabled: bool,
        active_downloads: Sequence[MoviePilotActiveDownload] | None,
    ) -> Mapping[str, Any]:
        now = datetime.now(UTC).timestamp()
        _prune_suppressions(suppressions, now)
        if event_type == "download.added":
            torrent_hash = str(data.get("hash") or "").strip().lower()
            source = _source_mapping(data.get("source"))
            subscribe_id = _positive_int(source.get("id") or data.get("subscribe_id"))
            if not torrent_hash or subscribe_id is None:
                return {"handled": False, "reason": "missing_download_identity"}
            context = _mapping(data.get("context"))
            meta = _mapping(context.get("meta_info"))
            media = _mapping(context.get("media_info"))
            torrent = _mapping(context.get("torrent_info"))
            season = _positive_int(source.get("season") or meta.get("begin_season") or media.get("season"))
            episodes_value = data.get("episodes") or meta.get("episode_list") or []
            if not isinstance(episodes_value, list):
                episodes_value = [episodes_value]
            torrents[torrent_hash] = {
                "hash": torrent_hash,
                "subscribe_id": subscribe_id,
                "baseline_progress": _nonnegative_float(data.get("progress")),
                "baseline_at": now,
                "retry_count": 0,
                "time": now,
                "tmdb_id": str(source.get("tmdbid") or media.get("tmdb_id") or ""),
                "season": season,
                "episodes": [value for value in (_positive_int(item) for item in episodes_value) if value],
                "title": str(
                    torrent.get("title") or meta.get("title") or media.get("title") or source.get("name") or ""
                ),
                "page_url": str(torrent.get("page_url") or ""),
                "enclosure": str(torrent.get("enclosure") or ""),
                "site_name": str(torrent.get("site_name") or ""),
                "size": torrent.get("size"),
                "username": str(data.get("username") or ""),
                "source": "moviepilot_webhook",
            }
            current = dict(_mapping(subscriptions.get(str(subscribe_id))))
            remembered_info = dict(source) or {
                "id": subscribe_id,
                "tmdbid": str(media.get("tmdb_id") or ""),
                "season": season,
                "name": str(torrent.get("title") or media.get("title") or ""),
            }
            _remember_subscription(
                current,
                subscribe_id=subscribe_id,
                info=remembered_info,
                reason="download.added",
                now=now,
            )
            sources = dict(_mapping(current.get("active_sources")))
            sources[_SOURCE_DOWNLOAD_PENDING] = "下载已发起，等待整理入库"
            current["active_sources"] = sources
            current["download_started_at"] = now
            current.pop("no_download_action_at", None)
            current.pop("last_completion_digest", None)
            current.pop("full_washing_failure", None)
            subscriptions[str(subscribe_id)] = current
            tmdb_id = str(remembered_info.get("tmdbid") or remembered_info.get("tmdb_id") or "").strip()
            if tmdb_id and season is not None:
                _set_season_active_washing(uow, tmdb_id, season, True)
            return {"handled": True, "subscribe_id": subscribe_id, "torrent_hash": torrent_hash}

        info = _subscription_info(data)
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id is None:
            return {"handled": False, "reason": "missing_subscription_identity"}
        key = str(subscribe_id)
        current = dict(_mapping(subscriptions.get(key)))
        if event_type == "subscribe.deleted":
            _remember_subscription(
                current,
                subscribe_id=subscribe_id,
                info=info,
                reason=event_type,
                now=now,
            )
            tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or current.get("tmdb_id") or "").strip()
            season = _positive_int(info.get("season") or current.get("season"))
            suppressed = any(
                suppression_key in suppressions
                for suppression_key in _snapshot_suppression_keys(tmdb_id, season, subscribe_id)
            )
            current["deleted"] = True
            current["active_sources"] = {}
            current["snapshot_repair_pending"] = not suppressed
            current["snapshot_restore_suppressed"] = suppressed
            subscriptions[key] = current
            removed_hashes = _remove_torrents(torrents, subscribe_id)
            return {
                "handled": True,
                "subscribe_id": subscribe_id,
                "torrents_removed": len(removed_hashes),
                "snapshot_repair_pending": not suppressed,
            }

        reason = event_type
        projection_changed = 0
        if event_type == "subscribe.added":
            for suppression_key in _snapshot_suppression_keys(
                str(info.get("tmdbid") or info.get("tmdb_id") or ""),
                _positive_int(info.get("season")),
                subscribe_id,
            ):
                suppressions.pop(suppression_key, None)
            current.pop("deleted", None)
            current.pop("snapshot_repair_pending", None)
            current.pop("snapshot_restore_suppressed", None)
            current.pop("last_completion_digest", None)
            current.pop("full_washing_failure", None)
            projection_changed = self._sync_projection(uow, info, subscribe_id)
        elif event_type == "subscribe.modified":
            fields = list(data.get("fields") or []) if isinstance(data.get("fields"), list) else []
            old_info = _mapping(data.get("old_subscribe_info"))
            if _consume_expected_update(current, info, fields, now):
                reason = "subscribe.modified.expected"
            elif str(old_info.get("state") or "").upper() == "N" and str(info.get("state") or "").upper() == "R":
                reason = "subscribe.modified.initial_search"
                current.pop("last_completion_digest", None)
                current.pop("full_washing_failure", None)
                projection_changed = self._sync_projection(uow, info, subscribe_id)
            else:
                current["last_manual_change"] = {
                    "scene": str(data.get("scene") or ""),
                    "fields": fields,
                    "old_state": old_info.get("state"),
                    "new_state": info.get("state"),
                    "updated_at": now,
                }
                if fields:
                    sources = dict(_mapping(current.get("active_sources")))
                    sources[_SOURCE_MANUAL_MP] = f"MP 手动修改：{','.join(str(value) for value in fields)}"
                    current["active_sources"] = sources
                projection_changed = self._sync_projection(uow, info, subscribe_id)

        _remember_subscription(
            current,
            subscribe_id=subscribe_id,
            info=info,
            reason=reason,
            now=now,
        )
        subscriptions[key] = current

        if event_type == "subscribe.complete":
            media = _mapping(data.get("mediainfo"))
            tmdb_id = str(info.get("tmdbid") or media.get("tmdb_id") or "").strip()
            season = _positive_int(info.get("season") or media.get("season"))
            total = _nonnegative_int(info.get("total_episode") or media.get("number_of_episodes"))
            completion_digest = _completion_fact_digest(subscribe_id, info, media)
            if current.get("last_completion_digest") == completion_digest:
                return {
                    "handled": True,
                    "subscribe_id": subscribe_id,
                    "snapshot_created": False,
                    "torrents_removed": 0,
                    "completion_replayed": True,
                }
            previous_failure = _mapping(current.get("full_washing_failure"))
            if previous_failure.get("completion_digest") == completion_digest:
                return {
                    "handled": True,
                    "subscribe_id": subscribe_id,
                    "completion_failed": True,
                    "phase": str(previous_failure.get("phase") or "reconciliation"),
                    "operation_id": str(previous_failure.get("operation_id") or ""),
                    "operation_status": str(previous_failure.get("operation_status") or ""),
                }
            if current.get("washing_timeout_cycle"):
                return {
                    "handled": True,
                    "subscribe_id": subscribe_id,
                    "snapshot_created": False,
                    "torrents_removed": 0,
                    "completion_deferred": True,
                    "reconciliation": "timeout_pending",
                    "full_washing_started_at": _state_timestamp(current.get("full_washing_started_at"), now),
                }
            sources = dict(_mapping(current.get("active_sources")))
            if full_washing_guard_enabled and season is not None and _positive_int(info.get("best_version_full")) == 1:
                if active_downloads is not None:
                    _sync_active_torrents(
                        torrents,
                        subscribe_id,
                        tmdb_id,
                        season,
                        active_downloads,
                    )
                current["full_washing"] = True
                current["full_washing_started_at"] = _state_timestamp(
                    current.get("full_washing_started_at"),
                    now,
                )
                pending_download = _has_torrent_for_subscription(torrents, subscribe_id)
                consistent = not pending_download and _season_consistency_ok(
                    uow,
                    tmdb_id=tmdb_id,
                    season=season,
                    expected_count=total,
                )
                if consistent:
                    current.pop("full_washing", None)
                    current.pop("full_washing_started_at", None)
                    current.pop("washing_timeout_started_at", None)
                    current.pop("washing_timeout_cycle", None)
                    for suppression_key in _snapshot_suppression_keys(
                        tmdb_id,
                        season,
                        subscribe_id,
                    ):
                        suppressions.pop(suppression_key, None)
                    _set_season_active_washing(uow, tmdb_id, season, False)
                else:
                    reconciliation = "pending_download" if pending_download else "consistency_failed"
                    current["last_event"] = (
                        "full_washing_downloading" if pending_download else "consistency_failed_complete"
                    )
                    current["updated_at"] = now
                    subscriptions[key] = current
                    _set_season_active_washing(uow, tmdb_id, season, True)
                    return {
                        "handled": True,
                        "subscribe_id": subscribe_id,
                        "snapshot_created": False,
                        "torrents_removed": 0,
                        "completion_deferred": True,
                        "reconciliation": reconciliation,
                        "full_washing_started_at": current["full_washing_started_at"],
                    }
            snapshot_created = bool(tmdb_id) and current.get("last_completion_digest") != completion_digest
            if snapshot_created:
                uow.session.execute(
                    text(
                        """
                        INSERT INTO subscribe_assistant_snapshots (
                            tmdb_id, item_type, season_number, subscribe_id,
                            scope_total, scope_json, subscribe_json, created_at
                        ) VALUES (
                            :tmdb_id, :item_type, :season_number, :subscribe_id,
                            :scope_total, CAST(:scope_json AS jsonb),
                            CAST(:subscribe_json AS jsonb), NOW()
                        )
                        """
                    ),
                    {
                        "tmdb_id": tmdb_id,
                        "item_type": (
                            "Series" if str(info.get("type") or media.get("type") or "") == "电视剧" else "Movie"
                        ),
                        "season_number": season,
                        "subscribe_id": subscribe_id,
                        "scope_total": total,
                        "scope_json": canonical_json(
                            {
                                "season": season,
                                "total": total,
                                "source": "subscribe.complete",
                                "completed_at": now,
                            }
                        ),
                        "subscribe_json": canonical_json(dict(info)),
                    },
                )
                current["last_completion_digest"] = completion_digest
            current.pop("full_washing_failure", None)
            sources.pop(_SOURCE_DOWNLOAD_PENDING, None)
            current["active_sources"] = sources
            current["last_reason"] = "订阅已完成"
            current.pop("full_washing", None)
            current.pop("full_washing_started_at", None)
            current.pop("washing_timeout_started_at", None)
            current.pop("washing_timeout_cycle", None)
            current["updated_at"] = now
            subscriptions[key] = current
            removed_hashes = _remove_torrents(torrents, subscribe_id)
            _set_season_active_washing(uow, tmdb_id, season, False)
            return {
                "handled": True,
                "subscribe_id": subscribe_id,
                "snapshot_created": snapshot_created,
                "torrents_removed": len(removed_hashes),
            }
        return {
            "handled": True,
            "subscribe_id": subscribe_id,
            "projection_changed": projection_changed,
            "reason": reason,
        }

    def _sync_projection(
        self,
        uow: SqlAlchemyUnitOfWork,
        info: Mapping[str, Any],
        subscribe_id: int,
    ) -> int:
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or "").strip()
        if not tmdb_id:
            return 0
        media_type = str(info.get("type") or info.get("media_type") or "").strip().casefold()
        season = _positive_int(info.get("season"))
        if "电影" in media_type or media_type in {"movie", "movies"}:
            key = SubscriptionMediaKey(tmdb_id, "Movie")
        elif season is not None and ("电视" in media_type or "剧" in media_type or media_type in {"tv", "series"}):
            key = self._projection.lock_season_key(
                uow,
                parent_series_tmdb_id=tmdb_id,
                season_number=season,
            )
            if key is None:
                return 0
        else:
            return 0
        return self._projection.set_status(
            uow,
            key,
            "SUBSCRIBED",
            {
                "title": info.get("name") or info.get("title"),
                "release_year": _positive_int(info.get("year")),
                "season_number": season,
                "parent_series_tmdb_id": tmdb_id if key.item_type == "Season" else None,
            },
            {
                "type": "moviepilot",
                "subscribe_id": subscribe_id,
            },
            None,
        )

    @staticmethod
    def _locked_state(uow: SqlAlchemyUnitOfWork, key: str) -> dict[str, Any]:
        uow.session.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (:state_key, '{}'::jsonb, NOW())
                ON CONFLICT (state_key) DO NOTHING
                """
            ),
            {"state_key": key},
        )
        value = uow.session.scalar(
            text(
                """
                SELECT state_json
                FROM subscribe_assistant_state
                WHERE state_key = :state_key
                FOR UPDATE
                """
            ),
            {"state_key": key},
        )
        return dict(value) if isinstance(value, Mapping) else {}

    def _receipt_result(self, event_key: str, payload_digest: str) -> dict[str, Any] | None:
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT state_json
                    FROM subscribe_assistant_state
                    WHERE state_key = :state_key
                    """
                ),
                {"state_key": _ASSISTANT_WEBHOOK_RECEIPTS_KEY},
            )
        receipts = value if isinstance(value, Mapping) else {}
        existing = receipts.get(event_key)
        if not isinstance(existing, Mapping):
            return None
        if str(existing.get("payload_digest") or "") != payload_digest:
            raise ValueError("MoviePilot event identity was reused with a different payload")
        result = existing.get("result")
        return dict(result) if isinstance(result, Mapping) else {"handled": True, "replayed": True}

    def _completion_terminal_result(
        self,
        event_type: str,
        data: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        if event_type != "subscribe.complete":
            return None
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id is None:
            return None
        completion_digest = _completion_fact_digest(subscribe_id, info, media)
        with self._unit_of_work() as uow:
            value = uow.session.scalar(
                text(
                    """
                    SELECT state_json
                    FROM subscribe_assistant_state
                    WHERE state_key = :state_key
                    """
                ),
                {"state_key": _ASSISTANT_SUBSCRIPTIONS_KEY},
            )
        subscriptions = value if isinstance(value, Mapping) else {}
        current = _mapping(subscriptions.get(str(subscribe_id)))
        if current.get("last_completion_digest") == completion_digest:
            return {
                "handled": True,
                "subscribe_id": subscribe_id,
                "snapshot_created": False,
                "torrents_removed": 0,
                "completion_replayed": True,
            }
        failure = _mapping(current.get("full_washing_failure"))
        if failure.get("completion_digest") != completion_digest:
            return None
        return {
            "handled": True,
            "subscribe_id": subscribe_id,
            "completion_failed": True,
            "phase": str(failure.get("phase") or "reconciliation"),
            "operation_id": str(failure.get("operation_id") or ""),
            "operation_status": str(failure.get("operation_status") or ""),
        }

    def _download_state_digest(self, data: Mapping[str, Any]) -> str:
        info = _subscription_info(data)
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        if subscribe_id is None:
            return ""
        with self._unit_of_work() as uow:
            rows = (
                uow.session.execute(
                    text(
                        """
                        SELECT state_key, state_json
                        FROM subscribe_assistant_state
                        WHERE state_key IN (:subscriptions_key, :torrents_key)
                        """
                    ),
                    {
                        "subscriptions_key": _ASSISTANT_SUBSCRIPTIONS_KEY,
                        "torrents_key": _ASSISTANT_TORRENTS_KEY,
                    },
                )
                .mappings()
                .all()
            )
        values = {str(row["state_key"]): row["state_json"] for row in rows}
        subscriptions = values.get(_ASSISTANT_SUBSCRIPTIONS_KEY)
        torrents = values.get(_ASSISTANT_TORRENTS_KEY)
        return _assistant_download_state_digest(
            subscriptions if isinstance(subscriptions, Mapping) else {},
            torrents if isinstance(torrents, Mapping) else {},
            subscribe_id,
        )

    @staticmethod
    def _write_state(uow: SqlAlchemyUnitOfWork, key: str, value: Mapping[str, Any]) -> None:
        uow.session.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:state_json AS jsonb), updated_at = NOW()
                WHERE state_key = :state_key
                """
            ),
            {"state_key": key, "state_json": canonical_json(dict(value))},
        )


def _remember_subscription(
    current: dict[str, Any],
    *,
    subscribe_id: int,
    info: Mapping[str, Any],
    reason: str,
    now: float,
) -> None:
    _normalize_subscription_timestamps(current)
    if not _state_timestamp(current.get("created_at"), 0):
        current["created_at"] = now
    detached_info = dict(info)
    if detached_info:
        current["subscribe_info"] = detached_info
    else:
        current.setdefault("subscribe_info", {})
    current["subscribe_id"] = subscribe_id
    current["tmdb_id"] = str(
        detached_info.get("tmdbid") or detached_info.get("tmdb_id") or current.get("tmdb_id") or ""
    )
    current["season"] = _positive_int(detached_info.get("season") or current.get("season"))
    if detached_info.get("state") not in (None, ""):
        current["mp_state"] = detached_info.get("state")
    current["last_event"] = reason
    current["last_reason"] = reason
    current["updated_at"] = now


def _consume_expected_update(
    current: dict[str, Any],
    info: Mapping[str, Any],
    fields: list[Any],
    now: float,
) -> bool:
    expected = current.get("expected_mp_update")
    if not isinstance(expected, Mapping):
        return False
    updated_at = _state_timestamp(expected.get("updated_at"), 0)
    if updated_at <= 0 or now - updated_at > _EXPECTED_UPDATE_TTL_SECONDS:
        return False
    expected_fields = {str(value) for value in expected.get("fields") or []}
    changed_fields = {str(value) for value in fields}
    if changed_fields and not changed_fields.issubset(expected_fields):
        return False
    expected_state = expected.get("state")
    if expected_state and info.get("state") != expected_state:
        return False
    expected_total = _positive_int(expected.get("total_episode"))
    if expected_total is not None and _positive_int(info.get("total_episode")) not in {
        None,
        expected_total,
    }:
        return False
    current.pop("expected_mp_update", None)
    return True


def _snapshot_suppression_keys(
    tmdb_id: str,
    season: int | None,
    subscribe_id: int,
) -> tuple[str, ...]:
    result: list[str] = []
    normalized_tmdb_id = str(tmdb_id or "").strip()
    if normalized_tmdb_id and season is not None:
        result.append(f"tmdb:{normalized_tmdb_id}:S{season}")
    if subscribe_id > 0:
        result.append(f"sub:{subscribe_id}")
    return tuple(result)


def _prune_suppressions(suppressions: dict[str, Any], now: float) -> None:
    for key, value in tuple(suppressions.items()):
        expires_at = _state_timestamp(_mapping(value).get("expires_at"), 0)
        if expires_at <= now:
            suppressions.pop(key, None)


def _record_snapshot_suppressions(
    suppressions: dict[str, Any],
    *,
    tmdb_id: str,
    season: int,
    subscribe_id: int,
    reason: str,
    now: float,
    retention_days: int,
) -> None:
    _prune_suppressions(suppressions, now)
    expires_at = now + max(int(retention_days), 1) * 86_400
    for key in _snapshot_suppression_keys(tmdb_id, season, subscribe_id):
        suppressions[key] = {
            "tmdb_id": str(tmdb_id),
            "season": season,
            "subscribe_id": subscribe_id,
            "reason": str(reason),
            "created_at": now,
            "expires_at": expires_at,
        }


def _remove_torrents(torrents: dict[str, Any], subscribe_id: int) -> tuple[str, ...]:
    removed = tuple(
        torrent_hash
        for torrent_hash, value in torrents.items()
        if _positive_int(_mapping(value).get("subscribe_id")) == subscribe_id
    )
    for torrent_hash in removed:
        torrents.pop(torrent_hash, None)
    return removed


def _has_torrent_for_subscription(torrents: Mapping[str, Any], subscribe_id: int) -> bool:
    return any(_positive_int(_mapping(value).get("subscribe_id")) == subscribe_id for value in torrents.values())


def _normalize_subscription_timestamps(current: dict[str, Any]) -> None:
    for field in (
        "created_at",
        "download_started_at",
        "full_washing_started_at",
        "washing_timeout_started_at",
    ):
        if field not in current:
            continue
        normalized = _state_timestamp(current.get(field), 0)
        if normalized > 0:
            current[field] = normalized
    for field in ("last_manual_change", "expected_mp_update"):
        nested = current.get(field)
        if not isinstance(nested, Mapping) or "updated_at" not in nested:
            continue
        normalized = _state_timestamp(nested.get("updated_at"), 0)
        if normalized > 0:
            current[field] = {**dict(nested), "updated_at": normalized}


def _full_washing_requested(data: Mapping[str, Any]) -> bool:
    info = _subscription_info(data)
    media = _mapping(data.get("mediainfo"))
    return (
        _positive_int(info.get("season") or media.get("season")) is not None
        and _positive_int(info.get("best_version_full")) == 1
    )


def _sync_active_torrents(
    torrents: dict[str, Any],
    subscribe_id: int,
    tmdb_id: str,
    season: int,
    active_downloads: Sequence[MoviePilotActiveDownload],
) -> tuple[str, ...]:
    active_hashes = frozenset(value.task_hash.casefold() for value in active_downloads)
    removed = tuple(
        torrent_hash
        for torrent_hash, value in torrents.items()
        if _positive_int(_mapping(value).get("subscribe_id")) == subscribe_id
        and str(torrent_hash).casefold() not in active_hashes
    )
    for torrent_hash in removed:
        torrents.pop(torrent_hash, None)
    now = datetime.now(UTC).timestamp()
    for download in active_downloads:
        identity_matches = download.subscribe_id == subscribe_id or (
            download.tmdb_id == str(tmdb_id) and download.season == season
        )
        if not identity_matches or download.task_hash in torrents:
            continue
        torrents[download.task_hash] = {
            "hash": download.task_hash,
            "subscribe_id": subscribe_id,
            "baseline_progress": 0.0,
            "baseline_at": now,
            "retry_count": 0,
            "time": now,
            "tmdb_id": str(tmdb_id),
            "season": season,
            "episodes": [],
            "title": "",
            "site_name": "",
            "size": None,
            "username": "",
            "source": "moviepilot_active_readback",
        }
    return removed


def _assistant_download_state_digest(
    subscriptions: Mapping[str, Any],
    torrents: Mapping[str, Any],
    subscribe_id: int,
) -> str:
    relevant_torrents = {
        str(torrent_hash): dict(_mapping(value))
        for torrent_hash, value in torrents.items()
        if _positive_int(_mapping(value).get("subscribe_id")) == subscribe_id
    }
    payload = canonical_json(
        {
            "subscription": dict(_mapping(subscriptions.get(str(subscribe_id)))),
            "torrents": relevant_torrents,
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _season_consistency_ok(
    uow: SqlAlchemyUnitOfWork,
    *,
    tmdb_id: str,
    season: int,
    expected_count: int,
) -> bool:
    if not str(tmdb_id or "").strip() or season < 1 or expected_count < 1:
        return False
    rows = (
        uow.session.execute(
            text(
                """
                SELECT asset_details_json
                FROM media_metadata
                WHERE parent_series_tmdb_id = :tmdb_id
                  AND season_number = :season
                  AND item_type = 'Episode'
                  AND in_library IS TRUE
                ORDER BY episode_number NULLS LAST, tmdb_id
                FOR SHARE
                """
            ),
            {"tmdb_id": str(tmdb_id), "season": season},
        )
        .mappings()
        .all()
    )
    if len(rows) < expected_count:
        return False
    signatures: list[tuple[str, str, str, str]] = []
    for row in rows:
        assets = _season_assets(row.get("asset_details_json"))
        if not assets:
            return False
        asset = assets[0]
        resolution = _known_signature(asset.get("resolution_display") or asset.get("resolution"))
        codec = _known_signature(asset.get("codec_display") or asset.get("video_codec"))
        if not resolution or not codec:
            return False
        signatures.append(
            (
                resolution,
                codec,
                _known_signature(_season_release_group(asset)),
                _known_signature(_season_effect(asset)),
            )
        )
    resolutions = {value[0] for value in signatures}
    codecs = {value[1] for value in signatures}
    groups = {value[2] for value in signatures if value[2]}
    effects = {value[3] for value in signatures if value[3]}
    return (
        bool(signatures)
        and len(resolutions) == 1
        and len(codecs) == 1
        and (not groups or (len(groups) == 1 and all(value[2] for value in signatures)))
        and (not effects or (len(effects) == 1 and all(value[3] for value in signatures)))
    )


def _season_assets(value: Any) -> tuple[Mapping[str, Any], ...]:
    normalized = value
    if isinstance(value, str):
        try:
            normalized = json.loads(value)
        except ValueError:
            return ()
    if isinstance(normalized, Mapping):
        for key in ("items", "files", "assets", "asset_details"):
            nested = normalized.get(key)
            if isinstance(nested, list):
                normalized = nested
                break
        else:
            normalized = [normalized]
    if not isinstance(normalized, list):
        return ()
    return tuple(
        dict(item)
        for item in normalized
        if isinstance(item, Mapping)
        and any(
            item.get(key) not in (None, "", [], {})
            for key in (
                "resolution_display",
                "codec_display",
                "effect_display",
                "frame_rate",
                "audio_display",
                "subtitle_display",
                "size_bytes",
                "video_codec",
                "width",
                "height",
                "release_group_raw",
            )
        )
    )


def _known_signature(value: Any) -> str:
    normalized = str(value or "").strip()
    return "" if not normalized or normalized.casefold() == "unknown" or normalized == "未知" else normalized


def _season_release_group(asset: Mapping[str, Any]) -> str:
    value = asset.get("release_group_raw")
    if isinstance(value, str):
        return next((part.strip() for part in value.split(",") if part.strip()), "")
    if isinstance(value, Sequence):
        return next((str(part).strip() for part in value if str(part).strip()), "")
    return ""


def _season_effect(asset: Mapping[str, Any]) -> str:
    values = tuple(
        str(asset.get(key))
        for key in (
            "effect_display",
            "effect",
            "video_effect",
            "hdr_type",
            "dynamic_range",
            "ExtendedVideoSubType",
            "ExtendedVideoSubTypeDescription",
            "ExtendedVideoType",
            "VideoRange",
            "Profile",
            "DisplayTitle",
            "Title",
            "video_title",
        )
        if asset.get(key) not in (None, "", [], {})
    )
    text_value = " | ".join(values)
    compact = re.sub(r"[\s._-]+", "", text_value.casefold())
    lowered = text_value.casefold()
    for profile in (8, 7, 5):
        if any(
            token in compact
            for token in (
                f"dovip{profile}",
                f"doviprofile{profile}",
                f"dolbyvisionprofile{profile}",
                f"dvprofile{profile}",
            )
        ) or re.search(rf"\b(?:p|profile)\s*{profile}(?:\.1)?\b", lowered):
            return f"DoVi_P{profile}"
    if any(token in compact for token in ("dovi", "dolbyvision", "dvhe", "dvh1")):
        return "DoVi"
    if "hdr10+" in lowered or "hdr10plus" in compact:
        return "HDR10+"
    if "hlg" in compact:
        return "HLG"
    if "hdr" in compact or "smpte2084" in compact:
        return "HDR"
    if "sdr" in compact:
        return "SDR"
    return ""


def _set_season_active_washing(
    uow: SqlAlchemyUnitOfWork,
    tmdb_id: str,
    season: int | None,
    enabled: bool,
) -> None:
    if not str(tmdb_id or "").strip() or season is None:
        return
    uow.session.execute(
        text(
            """
            UPDATE media_metadata
            SET active_washing = :enabled,
                last_updated_at = NOW()
            WHERE parent_series_tmdb_id = :tmdb_id
              AND season_number = :season
              AND item_type = 'Season'
            """
        ),
        {"enabled": bool(enabled), "tmdb_id": str(tmdb_id), "season": season},
    )


def _state_timestamp(value: Any, default: float) -> float:
    if isinstance(value, bool) or value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except (TypeError, ValueError):
            return default


def _nonnegative_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        return max(float(value or 0), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _full_washing_cycle_token(value: float) -> str:
    normalized = float(value)
    if normalized <= 0:
        raise ValueError("full-washing cycle start time must be positive")
    return str(int(normalized * 1_000_000))


def _current_full_washing_cycle(current: Mapping[str, Any]) -> str:
    started_at = _state_timestamp(current.get("full_washing_started_at"), 0)
    if not current.get("full_washing") or started_at <= 0:
        return ""
    return _full_washing_cycle_token(started_at)


def _completion_fact_digest(
    subscribe_id: int,
    info: Mapping[str, Any],
    media: Mapping[str, Any],
) -> str:
    payload = canonical_json(
        {
            "subscribe_id": subscribe_id,
            "subscribe_info": dict(info),
            "mediainfo": dict(media),
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class MoviePilotFullWashingOperations:
    """Stage replay-safe provider restart operations for an inconsistent completed season."""

    def __init__(self, repository: PostgresSubscriptionOperationRepository) -> None:
        self._repository = repository

    def ensure_restart(
        self,
        *,
        payload: Mapping[str, Any],
        cycle_started_at: float,
    ) -> tuple[str, SubscriptionOperation]:
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        if subscribe_id is None or not tmdb_id or season is None:
            raise ValueError("full-washing restart requires subscription, TMDb, and season identity")
        total = _nonnegative_int(info.get("total_episode") or media.get("number_of_episodes"))
        provider_payload = _selected_mapping(
            info,
            {
                "id",
                "name",
                "title",
                "type",
                "tmdbid",
                "tmdb_id",
                "imdbid",
                "imdb_id",
                "season",
                "year",
                "state",
                "total_episode",
                "best_version",
                "best_version_full",
            },
        )
        provider_payload.update(
            {
                "id": subscribe_id,
                "tmdbid": int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                "type": provider_payload.get("type") or "电视剧",
                "season": season,
                "state": "R",
                "best_version": 1,
                "best_version_full": 1,
            }
        )
        if total > 0:
            provider_payload["total_episode"] = total
        cycle = _full_washing_cycle_token(cycle_started_at)
        scope = f"moviepilot-full-washing:{tmdb_id}:season:{season}:cycle:{cycle}"
        update = self._repository.reserve(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.UPDATE,
                scope_key=f"{scope}:resume",
                request={
                    "action": "update",
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "payload": provider_payload,
                    "expected": {
                        "state": "R",
                        "best_version": 1,
                        "best_version_full": 1,
                    },
                    "preflight_readback": True,
                    "origin": {"type": "moviepilot_webhook", "cycle_started_at": cycle},
                },
                idempotency_key=f"{scope}:resume",
            )
        ).operation
        if update.status is not SubscriptionOperationStatus.CONFIRMED:
            return "resume", update
        search = self._repository.reserve(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.RECONCILE,
                scope_key=f"{scope}:search",
                request={
                    "action": "search",
                    "subscription_id": str(subscribe_id),
                    "payload": {},
                    "expected": {},
                    "origin": {"type": "moviepilot_webhook", "cycle_started_at": cycle},
                },
                idempotency_key=f"{scope}:search",
            )
        ).operation
        return "search", search

    def ensure_timeout(
        self,
        *,
        payload: Mapping[str, Any],
        cycle_started_at: float,
    ) -> tuple[str, SubscriptionOperation]:
        data = _mapping(payload.get("data"))
        info = _subscription_info(data)
        media = _mapping(data.get("mediainfo"))
        subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
        tmdb_id = str(info.get("tmdbid") or info.get("tmdb_id") or media.get("tmdb_id") or "").strip()
        season = _positive_int(info.get("season") or media.get("season"))
        if subscribe_id is None or not tmdb_id or season is None:
            raise ValueError("full-washing timeout requires subscription, TMDb, and season identity")
        cycle = _full_washing_cycle_token(cycle_started_at)
        scope = f"moviepilot-full-washing:{tmdb_id}:season:{season}:cycle:{cycle}"
        operation = self._repository.reserve(
            SubscriptionOperationIntent.create(
                kind=SubscriptionOperationKind.DELETE,
                scope_key=f"{scope}:timeout-delete",
                request={
                    "action": "delete",
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "expected": {"id": subscribe_id},
                    "preflight_readback": True,
                    "origin": {"type": "moviepilot_webhook", "cycle_started_at": cycle},
                },
                idempotency_key=f"{scope}:timeout-delete",
            )
        ).operation
        return "timeout-delete", operation


class MoviePilotWebhookEffectDelivery:
    effect_types = MOVIEPILOT_WEBHOOK_EFFECT_TYPES

    def __init__(
        self,
        assistant: MoviePilotAssistantEventRepository,
        transfer: MoviePilotTransferProcessor,
        full_washing: MoviePilotFullWashingOperations | None = None,
        full_washing_timeout_hours: Callable[[], int] | None = None,
        snapshot_retention_days: Callable[[], int] | None = None,
    ) -> None:
        self._assistant = assistant
        self._transfer = transfer
        self._full_washing = full_washing
        self._full_washing_timeout_hours = full_washing_timeout_hours or (lambda: 72)
        self._snapshot_retention_days = snapshot_retention_days or (lambda: 180)

    def deliver(self, effect: MediaEffectProjection) -> Mapping[str, Any]:
        payload = effect.payload
        event_key = _required_text(payload.get("event_key"), "event_key")
        event_type = _required_text(payload.get("event_type"), "event_type")
        payload_digest = _digest(payload.get("payload_digest"), "payload_digest")
        event_payload = payload.get("payload")
        if not isinstance(event_payload, Mapping):
            raise ValueError("MoviePilot webhook effect payload is invalid")
        if effect.effect_type == MOVIEPILOT_ASSISTANT_EVENT_TYPE:
            try:
                return self._deliver_assistant(
                    event_key=event_key,
                    event_type=event_type,
                    payload_digest=payload_digest,
                    payload=event_payload,
                )
            except MoviePilotAssistantReadbackUnavailable as exc:
                raise MediaEffectDeliveryRetryable(
                    "MoviePilot full-washing download readback is unavailable",
                    error={"event_key": event_key, "event_type": event_type},
                    retry_after_seconds=30,
                ) from exc
            except SQLAlchemyError as exc:
                raise MediaEffectDeliveryRetryable(
                    "MoviePilot assistant persistence is temporarily unavailable",
                    error={"event_key": event_key, "event_type": event_type},
                    retry_after_seconds=10,
                ) from exc
        if effect.effect_type != MOVIEPILOT_TRANSFER_EVENT_TYPE:
            raise ValueError(f"unsupported MoviePilot webhook effect type: {effect.effect_type}")
        file = event_payload.get("file")
        if not isinstance(file, Mapping):
            raise ValueError("MoviePilot transfer effect file is invalid")
        try:
            return self._transfer.process(
                event_key=event_key,
                event_type=event_type,
                file=file,
                passthrough=bool(event_payload.get("passthrough")),
            )
        except ValueError:
            raise
        except MediaEffectDeliveryIndeterminate:
            raise
        except Exception as exc:
            failure = (
                exc if isinstance(exc, MoviePilotTransferPreparationError)
                else MoviePilotTransferPreparationError("preparation", exc)
            )
            details = {
                "event_key": event_key,
                "event_type": event_type,
                **failure.details,
            }
            logger.warning("MoviePilot 处理失败：事件=%s，尝试=%s，详情=%s", event_type, effect.attempts, details)
            if effect.attempts >= 5:
                raise MediaEffectDeliveryIndeterminate(
                    "MoviePilot transfer preparation retry budget was exhausted",
                    error=details,
                ) from exc
            raise MediaEffectDeliveryRetryable(
                "MoviePilot transfer could not be prepared before provider mutation",
                error=details,
                retry_after_seconds=10,
            ) from exc

    def _deliver_assistant(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        timeout_hours = max(int(self._full_washing_timeout_hours()), 0)
        started_at = 0.0
        if event_type == "subscribe.complete" and timeout_hours > 0:
            started_at = self._assistant.full_washing_started_at(payload=payload)
            if self._full_washing_timed_out(started_at, timeout_hours):
                return self._deliver_full_washing_timeout(
                    event_key=event_key,
                    event_type=event_type,
                    payload_digest=payload_digest,
                    payload=payload,
                    cycle_started_at=started_at,
                )
        result = self._assistant.apply(
            event_key=event_key,
            event_type=event_type,
            payload_digest=payload_digest,
            payload=payload,
        )
        if result.get("completion_failed"):
            self._raise_recorded_failure(result, event_key=event_key, event_type=event_type)
        if not result.get("completion_deferred"):
            return result
        reconciliation = str(result.get("reconciliation") or "")
        started_at = _state_timestamp(result.get("full_washing_started_at"), 0)
        if self._full_washing_timed_out(started_at, timeout_hours):
            return self._deliver_full_washing_timeout(
                event_key=event_key,
                event_type=event_type,
                payload_digest=payload_digest,
                payload=payload,
                cycle_started_at=started_at,
            )
        if reconciliation == "pending_download":
            raise MediaEffectDeliveryRetryable(
                "MoviePilot full-washing download is still active",
                error={"event_key": event_key, "event_type": event_type},
                retry_after_seconds=60,
            )
        if reconciliation != "consistency_failed" or self._full_washing is None:
            raise MediaEffectDeliveryIndeterminate(
                "MoviePilot full-washing completion has no native reconciliation owner",
                error={"event_key": event_key, "event_type": event_type},
            )
        phase, operation = self._full_washing.ensure_restart(
            payload=payload,
            cycle_started_at=started_at,
        )
        if operation.status.terminal and operation.status is not SubscriptionOperationStatus.CONFIRMED:
            self._finalize_terminal_operation(
                operation,
                event_key=event_key,
                event_type=event_type,
                payload_digest=payload_digest,
                payload=payload,
                cycle_started_at=started_at,
                phase=phase,
            )
        raise MediaEffectDeliveryRetryable(
            (
                "MoviePilot full-washing restart was confirmed; waiting for consistent local media"
                if operation.status is SubscriptionOperationStatus.CONFIRMED
                else "MoviePilot full-washing restart is pending"
            ),
            error={
                "event_key": event_key,
                "phase": phase,
                "operation_id": str(operation.id),
                "operation_status": operation.status.value,
            },
            retry_after_seconds=(60 if operation.status is SubscriptionOperationStatus.CONFIRMED else 10),
        )

    def _deliver_full_washing_timeout(
        self,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
    ) -> Mapping[str, Any]:
        if self._full_washing is None:
            raise MediaEffectDeliveryIndeterminate(
                "MoviePilot full-washing timeout has no native reconciliation owner",
                error={"event_key": event_key, "event_type": event_type},
            )
        retention_days = max(int(self._snapshot_retention_days()), 1)
        prepared = self._assistant.prepare_full_washing_timeout(
            payload=payload,
            cycle_started_at=cycle_started_at,
            suppression_days=retention_days,
        )
        if not prepared:
            raise MediaEffectDeliveryRetryable(
                "MoviePilot full-washing state changed before timeout reservation",
                error={"event_key": event_key, "event_type": event_type},
                retry_after_seconds=1,
            )
        phase, operation = self._full_washing.ensure_timeout(
            payload=payload,
            cycle_started_at=cycle_started_at,
        )
        if operation.status is SubscriptionOperationStatus.CONFIRMED:
            result = self._assistant.finalize_full_washing_timeout(
                event_key=event_key,
                event_type=event_type,
                payload_digest=payload_digest,
                payload=payload,
                cycle_started_at=cycle_started_at,
                suppression_days=retention_days,
            )
            self._raise_if_superseded(result, event_key=event_key, event_type=event_type)
            return result
        if operation.status.terminal:
            self._finalize_terminal_operation(
                operation,
                event_key=event_key,
                event_type=event_type,
                payload_digest=payload_digest,
                payload=payload,
                cycle_started_at=cycle_started_at,
                phase=phase,
            )
        raise MediaEffectDeliveryRetryable(
            "MoviePilot full-washing timeout reconciliation is pending",
            error={
                "event_key": event_key,
                "phase": phase,
                "operation_id": str(operation.id),
                "operation_status": operation.status.value,
            },
            retry_after_seconds=10,
        )

    def _finalize_terminal_operation(
        self,
        operation: SubscriptionOperation,
        *,
        event_key: str,
        event_type: str,
        payload_digest: str,
        payload: Mapping[str, Any],
        cycle_started_at: float,
        phase: str,
    ) -> NoReturn:
        result = self._assistant.finalize_full_washing_failure(
            event_key=event_key,
            event_type=event_type,
            payload_digest=payload_digest,
            payload=payload,
            cycle_started_at=cycle_started_at,
            phase=phase,
            operation_id=str(operation.id),
            operation_status=operation.status,
        )
        self._raise_if_superseded(result, event_key=event_key, event_type=event_type)
        self._raise_recorded_failure(result, event_key=event_key, event_type=event_type)

    @staticmethod
    def _raise_if_superseded(
        result: Mapping[str, Any],
        *,
        event_key: str,
        event_type: str,
    ) -> None:
        if result.get("completion_superseded"):
            raise MediaEffectDeliveryIndeterminate(
                "MoviePilot full-washing cycle was superseded before local finalization",
                error={
                    "event_key": event_key,
                    "event_type": event_type,
                    "expected_cycle": result.get("expected_cycle"),
                    "current_cycle": result.get("current_cycle"),
                },
            )

    @staticmethod
    def _raise_recorded_failure(
        result: Mapping[str, Any],
        *,
        event_key: str,
        event_type: str,
    ) -> NoReturn:
        phase = str(result.get("phase") or "reconciliation")
        operation_id = str(result.get("operation_id") or "")
        try:
            status = SubscriptionOperationStatus(str(result.get("operation_status") or ""))
        except ValueError as exc:
            raise ValueError("MoviePilot full-washing failure receipt has an invalid operation status") from exc
        error = {
            "event_key": event_key,
            "event_type": event_type,
            "phase": phase,
            "operation_id": operation_id,
            "operation_status": status.value,
        }
        if status is SubscriptionOperationStatus.INDETERMINATE:
            raise MediaEffectDeliveryIndeterminate(
                f"MoviePilot full-washing {phase} outcome is indeterminate",
                error=error,
            )
        if status in {
            SubscriptionOperationStatus.REJECTED,
            SubscriptionOperationStatus.CANCELLED,
        }:
            raise MediaEffectDeliveryRejected(
                f"MoviePilot full-washing {phase} was {status.value}",
                error=error,
            )
        raise ValueError("MoviePilot full-washing failure receipt is not terminal")

    @staticmethod
    def _full_washing_timed_out(started_at: float, timeout_hours: int) -> bool:
        elapsed_seconds = datetime.now(UTC).timestamp() - started_at
        return timeout_hours > 0 and started_at > 0 and elapsed_seconds >= timeout_hours * 3600


def _assistant_effect_payload(event_type: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    data = _mapping(payload.get("data"))
    if event_type == "download.added":
        context = _mapping(data.get("context"))
        return {
            "type": event_type,
            "data": {
                "hash": _bounded_text(data.get("hash"), 255),
                "subscribe_id": _positive_int(data.get("subscribe_id")),
                "source": _selected_mapping(
                    _source_mapping(data.get("source")),
                    {
                        "id",
                        "name",
                        "title",
                        "type",
                        "tmdbid",
                        "tmdb_id",
                        "season",
                        "state",
                        "total_episode",
                        "best_version",
                        "best_version_full",
                    },
                ),
                "episodes": [
                    value
                    for value in (_positive_int(item) for item in _list_value(data.get("episodes"))[:1000])
                    if value is not None
                ],
                "progress": _nonnegative_float(data.get("progress")),
                "context": {
                    "meta_info": _selected_mapping(
                        _mapping(context.get("meta_info")),
                        {"begin_season", "episode_list", "title"},
                    ),
                    "media_info": _selected_mapping(
                        _mapping(context.get("media_info")),
                        {"tmdb_id", "season", "title", "type"},
                    ),
                    "torrent_info": _selected_mapping(
                        _mapping(context.get("torrent_info")),
                        {"title", "site_name", "size"},
                    ),
                },
            },
        }

    info_fields = {
        "id",
        "name",
        "title",
        "type",
        "media_type",
        "tmdbid",
        "tmdb_id",
        "imdbid",
        "imdb_id",
        "season",
        "year",
        "state",
        "total_episode",
        "best_version",
        "best_version_full",
    }
    return {
        "type": event_type,
        "data": {
            "subscribe_id": _positive_int(data.get("subscribe_id")),
            "subscribe_info": _selected_mapping(_mapping(data.get("subscribe_info")), info_fields),
            "old_subscribe_info": _selected_mapping(
                _mapping(data.get("old_subscribe_info")),
                info_fields,
            ),
            "mediainfo": _selected_mapping(
                _mapping(data.get("mediainfo")),
                {
                    "title",
                    "type",
                    "tmdb_id",
                    "imdb_id",
                    "season",
                    "year",
                    "number_of_episodes",
                },
            ),
            "fields": [_bounded_text(value, 128) for value in _list_value(data.get("fields"))[:100]],
            "scene": _bounded_text(data.get("scene"), 128),
        },
    }


def _selected_mapping(value: Mapping[str, Any], fields: set[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in fields:
        item = value.get(key)
        if item in (None, "", [], {}):
            continue
        if isinstance(item, (str, int, float, bool)):
            result[key] = _bounded_text(item, 1000) if isinstance(item, str) else item
        elif isinstance(item, list):
            result[key] = [
                _bounded_text(entry, 1000) if isinstance(entry, str) else entry
                for entry in item[:1000]
                if isinstance(entry, (str, int, float, bool))
            ]
    return result


def _list_value(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else ([value] if value not in (None, "") else [])


def _bounded_text(value: Any, maximum: int) -> str:
    return str(value or "").strip()[:maximum]


def _assistant_enabled(settings: MoviePilotWebhookSettings) -> bool:
    mp_value = settings.load("mp_config")
    watchlist_value = settings.load("watchlist_config")
    mp = dict(mp_value) if isinstance(mp_value, Mapping) else {}
    watchlist = dict(watchlist_value) if isinstance(watchlist_value, Mapping) else {}
    assistant_value = mp.get("subscribe_assistant")
    if not isinstance(assistant_value, Mapping):
        assistant_value = watchlist.get("subscribe_assistant")
    assistant = dict(assistant_value) if isinstance(assistant_value, Mapping) else {}
    sources_value = settings.load("subscription_strategy_config")
    sources_config = dict(sources_value) if isinstance(sources_value, Mapping) else {}
    sources = sources_config.get("subscription_sources")
    source_enabled = not isinstance(sources, list) or "mp" in sources
    return bool(assistant.get("enabled")) and bool(str(mp.get("moviepilot_url") or "").strip()) and source_enabled


def _assistant_event_is_actionable(event_type: str, payload: Mapping[str, Any]) -> bool:
    data = _mapping(payload.get("data"))
    if event_type == "download.added":
        source = _source_mapping(data.get("source"))
        return (
            bool(str(data.get("hash") or "").strip())
            and _positive_int(source.get("id") or data.get("subscribe_id")) is not None
        )
    info = _subscription_info(data)
    subscribe_id = _positive_int(data.get("subscribe_id") or info.get("id"))
    if subscribe_id is None:
        return False
    if event_type == "subscribe.complete":
        media = _mapping(data.get("mediainfo"))
        return bool(str(info.get("tmdbid") or media.get("tmdb_id") or "").strip())
    return True


def _assistant_aggregate_id(event_type: str, payload: Mapping[str, Any]) -> str:
    data = _mapping(payload.get("data"))
    if event_type == "download.added":
        return f"download:{str(data.get('hash') or '').strip().lower()}"
    info = _subscription_info(data)
    return f"subscription:{_positive_int(data.get('subscribe_id') or info.get('id')) or 'unknown'}"


def _transfer_file(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    data = _mapping(payload.get("data"))
    transfer = _mapping(data.get("transferinfo"))
    media = _mapping(data.get("mediainfo"))
    meta = _mapping(data.get("meta"))
    target = _mapping(transfer.get("target_item"))
    target_dir = _mapping(transfer.get("target_diritem"))
    file_id = str(target.get("fileid") or target.get("file_id") or target.get("fid") or "").strip()
    tmdb_id = str(media.get("tmdb_id") or "").strip()
    if not file_id or not tmdb_id:
        return None
    media_type = "tv" if str(media.get("type") or "").strip() == "电视剧" else "movie"
    return {
        "file_id": file_id,
        "file_name": str(target.get("name") or target.get("file_name") or "").strip(),
        "file_type": str(target.get("type") or "").strip(),
        "pick_code": str(target.get("pickcode") or target.get("pick_code") or target.get("pc") or "").strip(),
        "parent_id": str(target_dir.get("fileid") or target_dir.get("cid") or "").strip(),
        "tmdb_id": tmdb_id,
        "media_type": media_type,
        "title": str(media.get("title") or "").strip(),
        "season_number": _positive_int(meta.get("begin_season")),
        "episode_number": _positive_int(meta.get("begin_episode")),
        "path": str(target.get("path") or "").strip(),
    }


def _event_key(event_type: str, payload: Mapping[str, Any]) -> str:
    explicit = str(
        payload.get("EventId")
        or payload.get("event_id")
        or payload.get("IdempotencyKey")
        or payload.get("idempotency_key")
        or ""
    ).strip()
    identity = f"{event_type}\0{explicit}" if explicit else canonical_json(dict(payload), field="MoviePilot event")
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _payload_digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(dict(payload), field="MoviePilot payload").encode("utf-8")).hexdigest()


def _subscription_info(data: Mapping[str, Any]) -> dict[str, Any]:
    info = data.get("subscribe_info")
    if isinstance(info, Mapping) and info:
        return dict(info)
    media = _mapping(data.get("mediainfo"))
    if not media:
        return {}
    subscribe_id = _positive_int(data.get("subscribe_id"))
    return {
        "id": subscribe_id,
        "name": media.get("title"),
        "type": media.get("type"),
        "tmdbid": media.get("tmdb_id"),
        "imdbid": media.get("imdb_id"),
        "season": media.get("season"),
        "year": media.get("year"),
    }


def _source_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        normalized = value.strip()
        if normalized.casefold().startswith("subscribe|"):
            normalized = normalized.split("|", 1)[1].strip()
        try:
            parsed = json.loads(normalized)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


def _required_text(value: Any, field: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"MoviePilot webhook effect requires {field}")
    return normalized


def _digest(value: Any, field: str) -> str:
    normalized = _required_text(value, field).lower()
    if len(normalized) != 64:
        raise ValueError(f"MoviePilot webhook effect {field} is invalid")
    try:
        bytes.fromhex(normalized)
    except ValueError as exc:
        raise ValueError(f"MoviePilot webhook effect {field} is invalid") from exc
    return normalized


__all__ = [
    "MOVIEPILOT_ASSISTANT_EVENT_TYPE",
    "MOVIEPILOT_TRANSFER_EVENT_TYPE",
    "MOVIEPILOT_WEBHOOK_EFFECT_TYPES",
    "MoviePilotAssistantReadbackUnavailable",
    "MoviePilotAssistantEventRepository",
    "MoviePilotFullWashingOperations",
    "MoviePilotTransferProcessor",
    "MoviePilotWebhookEffectDelivery",
    "MoviePilotWebhookService",
    "MoviePilotWebhookSettings",
    "SqlAlchemyMoviePilotAssistantEventRepository",
]
