"""Processor-free execution for the Subscribe Assistant maintenance task."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from uuid import UUID, uuid4

from sqlalchemy import text

from etk.platform.db import SqlAlchemyUnitOfWork
from etk.platform.jobs.json_payload import canonical_json

from .assistant import ASSISTANT_MAINTENANCE_PHASES, AssistantMaintenanceSession

_ASSISTANT_SUBSCRIPTIONS_KEY = "subscribes"
_ASSISTANT_TORRENTS_KEY = "torrents"
_ASSISTANT_SUPPRESSIONS_KEY = "snapshot_restore_suppressions"
_SOURCE_DOWNLOAD_PENDING = "download_pending"


class AssistantMaintenanceReadPort(Protocol):
    def list_subscriptions(self) -> Any: ...

    def active_downloads(self) -> Sequence[Any]: ...

    def list_transfer_history(
        self,
        *,
        status: bool | None = None,
        limit: int = 100,
        page: int = 1,
    ) -> Sequence[Mapping[str, Any]]: ...

    def query_subscription(self, tmdb_id: int | str, *, season: int | None = None) -> Any: ...


class AssistantMaintenanceEffectPort(Protocol):
    def create_subscription(
        self,
        payload: Mapping[str, Any],
        *,
        consume_quota: bool,
        apply_local_continuation: bool,
    ) -> bool: ...

    def update_subscription(self, payload: Mapping[str, Any]) -> bool: ...

    def delete_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
        expected_subscription_id: int | str | None = None,
    ) -> bool: ...

    def delete_subscription_by_id(self, subscription_id: int | str) -> bool: ...

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        *,
        season: int | None,
        status: str,
        total_episodes: int | None,
    ) -> bool: ...

    def search_subscription(self, subscription_id: int | str) -> bool: ...

    def delete_download_tasks(self, hashes: Sequence[str], *, operation_occurrence: str | None = None) -> bool: ...

    def delete_transfer_history(
        self,
        record: Mapping[str, Any],
        *,
        operation_occurrence: str | None = None,
    ) -> bool: ...

    def redo_transfer_history(self, history_ids: Sequence[int]) -> bool: ...


class AssistantMaintenanceEffectIndeterminate(RuntimeError):
    """A provider write was started but did not reach an authoritative outcome."""


class NativeSubscribeAssistantMaintenanceGateway:
    """Processor-free assistant maintenance over typed reads and effects."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        provider: AssistantMaintenanceReadPort | None = None,
        effects: AssistantMaintenanceEffectPort | None = None,
        configuration: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._provider = provider
        self._effects = effects
        self._configuration = configuration or (lambda: {})

    def begin(
        self,
        processor: object | None,
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
    ) -> AssistantMaintenanceSession:
        del processor
        return _NativeAssistantSession(
            self._unit_of_work,
            job_id=job_id,
            attempt_id=attempt_id,
            worker_id=str(worker_id),
            provider=self._provider,
            effects=self._effects,
            configuration=self._configuration,
        )


class _NativeAssistantSession:
    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        job_id: UUID,
        attempt_id: UUID,
        worker_id: str,
        provider: AssistantMaintenanceReadPort | None,
        effects: AssistantMaintenanceEffectPort | None,
        configuration: Callable[[], Mapping[str, Any]],
    ) -> None:
        self._unit_of_work = unit_of_work
        self._job_id = job_id
        self._attempt_id = attempt_id
        self._worker_id = worker_id
        self._provider = provider
        self._effects = effects
        self._configuration = configuration

    def phases(self) -> Sequence[str]:
        configuration = self._configuration()
        phases: list[str] = list(ASSISTANT_MAINTENANCE_PHASES[:6])
        if bool(configuration.get("download_monitor_enabled")):
            phases.append("download_check")
        if bool(configuration.get("verify_enabled", True)):
            phases.append("snapshot_verify")
        return tuple(phases)

    @staticmethod
    def _locked_state(uow: SqlAlchemyUnitOfWork, state_key: str) -> dict[str, Any]:
        """Load one assistant JSON state row under the current transaction lock."""
        uow.session.execute(
            text(
                """
                INSERT INTO subscribe_assistant_state (state_key, state_json, updated_at)
                VALUES (:state_key, '{}'::jsonb, NOW())
                ON CONFLICT (state_key) DO NOTHING
                """
            ),
            {"state_key": state_key},
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
            {"state_key": state_key},
        )
        return dict(value) if isinstance(value, Mapping) else {}

    @staticmethod
    def _write_state(uow: SqlAlchemyUnitOfWork, state_key: str, value: Mapping[str, Any]) -> None:
        uow.session.execute(
            text(
                """
                UPDATE subscribe_assistant_state
                SET state_json = CAST(:value AS jsonb), updated_at = NOW()
                WHERE state_key = :state_key
                """
            ),
            {
                "state_key": state_key,
                "value": json.dumps(dict(value), ensure_ascii=False, separators=(",", ":")),
            },
        )

    def run_phase(self, phase: str, checkpoint: Callable[[], None]) -> int:
        if phase not in ASSISTANT_MAINTENANCE_PHASES:
            raise LookupError(f"unknown subscribe assistant maintenance phase: {phase}")
        checkpoint()
        if phase == "delete_records_cleanup":
            with self._unit_of_work() as uow:
                result = uow.session.execute(
                    text(
                        "DELETE FROM subscribe_assistant_delete_records "
                        "WHERE expires_at IS NOT NULL AND expires_at <= NOW()"
                    )
                )
                count = int(getattr(result, "rowcount", 0) or 0)
                uow.commit()
            checkpoint()
            return count
        if phase == "snapshot_cleanup":
            retention = _assistant_int(self._configuration().get("snapshot_retention_days"), 180)
            retention = max(1, min(retention, 3650))
            with self._unit_of_work() as uow:
                result = uow.session.execute(
                    text(
                        "DELETE FROM subscribe_assistant_snapshots "
                        "WHERE created_at < NOW() - make_interval(days => :days) "
                        "AND (claim_token IS NULL OR lease_expires_at <= NOW())"
                    ),
                    {"days": retention},
                )
                count = int(getattr(result, "rowcount", 0) or 0)
                uow.commit()
            checkpoint()
            return count
        if phase == "washing_timeout":
            count = self._washing_timeout(checkpoint)
        elif phase == "history_reconcile":
            count = self._history_reconcile(checkpoint)
        elif phase == "moviepilot_subscription_sync":
            count = self._subscription_sync(checkpoint)
        elif phase == "no_download_check":
            count = self._no_download_check(checkpoint)
        elif phase == "download_check":
            count = self._download_check(checkpoint)
        else:
            count = self._snapshot_verify(checkpoint)
        checkpoint()
        return count

    def _washing_timeout(self, checkpoint: Callable[[], None]) -> int:
        timeout_hours = max(
            0,
            min(_assistant_int(self._configuration().get("full_washing_timeout_hours"), 72), 24 * 365),
        )
        # A missing provider is not an authoritative empty readback.  Keep
        # the durable subscription state untouched so a later retry can still
        # reconcile the outstanding subscription instead of falsely closing
        # the timeout.
        if timeout_hours <= 0 or self._provider is None:
            return 0
        now = datetime.now(UTC).timestamp()
        timeout_seconds = timeout_hours * 3600
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
        candidates: list[tuple[str, dict[str, Any], float]] = []
        for key, raw in subscriptions.items():
            if not isinstance(raw, Mapping):
                continue
            task = dict(raw)
            if not _assistant_bool(task.get("full_washing")):
                continue
            started_at = _assistant_float(task.get("full_washing_started_at") or task.get("washing_timeout_started_at"))
            if started_at <= 0 or now - started_at < timeout_seconds:
                continue
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _positive_int(task.get("season"))
            if not tmdb_id or season <= 0:
                continue
            candidates.append((str(key), task, started_at))
            if len(candidates) >= 100:
                break
        changed = 0
        for state_key, task, started_at in candidates:
            checkpoint()
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _positive_int(task.get("season"))
            subscribe_id = _positive_int(task.get("subscribe_id"))
            info = _mapping_value(task.get("subscribe_info"))
            expected_total = _positive_int(
                info.get("total_episode")
                or info.get("total")
                or info.get("total_episodes")
                or task.get("total_episode")
            )
            if expected_total > 0:
                with self._unit_of_work() as uow:
                    consistent = _assistant_season_consistency_ok(
                        uow,
                        tmdb_id=tmdb_id,
                        season=season,
                        expected_count=expected_total,
                    )
                    uow.rollback()
                if consistent:
                    with self._unit_of_work() as uow:
                        current = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
                        latest = current.get(state_key)
                        if not isinstance(latest, Mapping):
                            continue
                        latest_task = dict(latest)
                        latest_started = _assistant_float(
                            latest_task.get("full_washing_started_at") or latest_task.get("washing_timeout_started_at")
                        )
                        if not _assistant_bool(latest_task.get("full_washing")) or latest_started != started_at:
                            continue
                        latest_task.pop("full_washing", None)
                        latest_task.pop("full_washing_started_at", None)
                        latest_task.pop("washing_timeout_started_at", None)
                        latest_task["last_reason"] = "full_washing_consistency_ok"
                        latest_task["full_washing_timeout_checked_at"] = now
                        latest_task["updated_at"] = now
                        current[state_key] = latest_task
                        self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, current)
                        _set_assistant_season_active_washing(uow, tmdb_id=tmdb_id, season=season, enabled=False)
                        uow.commit()
                    changed += 1
                    continue
            found = self._provider.query_subscription(
                int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                season=season,
            )
            if found is None:
                # A missing result is an unavailable readback, not proof that
                # the subscription disappeared.  Leave state untouched.
                continue
            found_state = _assistant_query_found(found)
            found_subscription = _assistant_query_subscription(found)
            subscribe_id = subscribe_id or _positive_int(
                found_subscription.get("id") or found_subscription.get("subscribe_id")
            )
            delete_confirmed = False
            if found_state and (subscribe_id <= 0 or self._effects is None):
                # We cannot safely close a still-present provider subscription
                # without a durable delete owner and an identity.
                continue
            if found_state and subscribe_id > 0 and self._effects is not None:
                if not self._prepare_washing_timeout(
                    state_key=state_key,
                    started_at=started_at,
                    subscribe_id=subscribe_id,
                    tmdb_id=tmdb_id,
                    season=season,
                    now=now,
                ):
                    continue
                delete_confirmed = _assistant_write_confirmed(
                    self._effect_call(
                        self._effects.delete_subscription,
                        int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                        season=season,
                        expected_subscription_id=subscribe_id,
                        occurrence=f"assistant:washing-timeout:{subscribe_id}:{started_at:.6f}",
                        checkpoint=checkpoint,
                    )
                )
            if found_state and not delete_confirmed:
                # Rejected writes leave the timeout state active for a later
                # attempt; an indeterminate write raises and is never replayed.
                self._clear_washing_timeout_pending(
                    state_key=state_key,
                    started_at=started_at,
                    subscribe_id=subscribe_id,
                    tmdb_id=tmdb_id,
                    season=season,
                )
                continue
            with self._unit_of_work() as uow:
                current = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
                latest = current.get(state_key)
                if not isinstance(latest, Mapping):
                    continue
                latest_task = dict(latest)
                latest_started = _assistant_float(
                    latest_task.get("full_washing_started_at") or latest_task.get("washing_timeout_started_at")
                )
                if (
                    not _assistant_bool(latest_task.get("full_washing"))
                    or latest_started != started_at
                    or str(latest_task.get("tmdb_id") or "").strip() != tmdb_id
                    or _positive_int(latest_task.get("season")) != season
                    or (found_state and str(latest_task.get("washing_timeout_cycle") or "") != f"{started_at:.6f}")
                ):
                    # A webhook or another maintenance attempt superseded this
                    # cycle while the provider readback was in flight.
                    continue
                torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
                for torrent_key, torrent in tuple(torrents.items()):
                    if isinstance(torrent, Mapping) and _positive_int(torrent.get("subscribe_id")) == subscribe_id:
                        torrents.pop(torrent_key, None)
                suppressions = self._locked_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
                for suppression_key in _assistant_suppression_keys(tmdb_id, season, subscribe_id):
                    suppressions[suppression_key] = {
                        "tmdb_id": tmdb_id,
                        "season": season,
                        "subscribe_id": subscribe_id,
                        "reason": "full_washing_timeout",
                        "created_at": now,
                        "expires_at": now
                        + max(_assistant_int(self._configuration().get("snapshot_retention_days"), 180), 1) * 86_400,
                    }
                latest_task["deleted"] = True
                latest_task["active_sources"] = {}
                latest_task["snapshot_restore_suppressed"] = True
                latest_task["last_reason"] = "full_washing_timeout"
                latest_task["full_washing_timeout_checked_at"] = now
                latest_task["full_washing_timeout_readback"] = "found" if found_state else "empty"
                latest_task["full_washing_timeout_delete_confirmed"] = delete_confirmed
                latest_task.pop("full_washing", None)
                latest_task.pop("full_washing_started_at", None)
                latest_task.pop("washing_timeout_started_at", None)
                latest_task.pop("washing_timeout_cycle", None)
                latest_task["updated_at"] = now
                current[state_key] = latest_task
                self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, current)
                self._write_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
                self._write_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
                _set_assistant_season_active_washing(uow, tmdb_id=tmdb_id, season=season, enabled=False)
                uow.commit()
            changed += 1
        return changed

    def _prepare_washing_timeout(
        self,
        *,
        state_key: str,
        started_at: float,
        subscribe_id: int,
        tmdb_id: str,
        season: int,
        now: float,
    ) -> bool:
        cycle = f"{started_at:.6f}"
        retention_days = max(_assistant_int(self._configuration().get("snapshot_retention_days"), 180), 1)
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            latest = subscriptions.get(state_key)
            if not isinstance(latest, Mapping):
                uow.rollback()
                return False
            task = dict(latest)
            latest_started = _assistant_float(
                task.get("full_washing_started_at") or task.get("washing_timeout_started_at")
            )
            if (
                not _assistant_bool(task.get("full_washing"))
                or latest_started != started_at
                or str(task.get("tmdb_id") or "").strip() != tmdb_id
                or _positive_int(task.get("season")) != season
            ):
                uow.rollback()
                return False
            task["washing_timeout_started_at"] = _assistant_float(task.get("washing_timeout_started_at"), now) or now
            task["washing_timeout_cycle"] = cycle
            subscriptions[state_key] = task
            suppressions = self._locked_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
            for key in _assistant_suppression_keys(tmdb_id, season, subscribe_id):
                suppressions[key] = {
                    "tmdb_id": tmdb_id,
                    "season": season,
                    "subscribe_id": subscribe_id,
                    "reason": "full_washing_timeout_pending",
                    "cycle": cycle,
                    "created_at": now,
                    "expires_at": now + retention_days * 86_400,
                }
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()
        return True

    def _clear_washing_timeout_pending(
        self,
        *,
        state_key: str,
        started_at: float,
        subscribe_id: int,
        tmdb_id: str,
        season: int,
    ) -> None:
        cycle = f"{started_at:.6f}"
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            latest = subscriptions.get(state_key)
            if isinstance(latest, Mapping):
                task = dict(latest)
                if str(task.get("washing_timeout_cycle") or "") == cycle:
                    task.pop("washing_timeout_started_at", None)
                    task.pop("washing_timeout_cycle", None)
                    subscriptions[state_key] = task
            suppressions = self._locked_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
            for key in _assistant_suppression_keys(tmdb_id, season, subscribe_id):
                value = _mapping_value(suppressions.get(key))
                if value.get("reason") == "full_washing_timeout_pending" and value.get("cycle") == cycle:
                    suppressions.pop(key, None)
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            self._write_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()

    def _history_reconcile(self, checkpoint: Callable[[], None]) -> int:
        if (
            self._provider is None
            or self._effects is None
            or not callable(getattr(self._provider, "list_transfer_history", None))
        ):
            return 0
        failed_records = self._transfer_history_pages(status=False, page_limit=10, checkpoint=checkpoint)
        recent_records = self._transfer_history_pages(status=None, page_limit=5, checkpoint=checkpoint)
        records_list: list[Mapping[str, Any]] = []
        seen_records: set[str] = set()
        for record in (*failed_records, *recent_records):
            if not isinstance(record, Mapping):
                continue
            identity = _assistant_history_identity(record)
            if identity in seen_records:
                continue
            seen_records.add(identity)
            records_list.append(dict(record))
            if len(records_list) >= 100:
                break
        records = tuple(records_list)
        now = datetime.now(UTC).timestamp()
        retry_after = 6 * 60 * 60
        with self._unit_of_work() as uow:
            state = self._locked_state(uow, "history_reconcile")
            last_retry_raw = state.get("last_retry")
            last_retry = dict(last_retry_raw) if isinstance(last_retry_raw, Mapping) else {}
            candidates: list[tuple[int, str, int | None]] = []
            for record in records:
                checkpoint()
                identifier = _positive_int(record.get("id"))
                if identifier <= 0:
                    continue
                fingerprint = f"id:{identifier}"
                if now - _assistant_float(last_retry.get(fingerprint)) < retry_after:
                    continue
                is_failed = _assistant_history_failed(record)
                pick_code = _history_text(record, "pickcode", "pick_code", "pc", "PickCode")
                file_id = _history_text(record, "fileid", "file_id", "fid", "FileId")
                stale_record_id: int | None = None
                if pick_code or file_id:
                    local = (
                        uow.session.execute(
                            text(
                                """
                                SELECT id, status
                                FROM p115_organize_records
                                WHERE (:pick_code <> '' AND pick_code = :pick_code)
                                   OR (:file_id <> '' AND file_id = :file_id)
                                ORDER BY processed_at DESC NULLS LAST, id DESC
                                LIMIT 1
                                FOR UPDATE
                                """
                            ),
                            {"pick_code": pick_code, "file_id": file_id},
                        )
                        .mappings()
                        .first()
                    )
                    if local is not None and str(local.get("status") or "").strip().casefold() != "success":
                        stale_record_id = _positive_int(local.get("id")) or None
                        is_failed = True
                if is_failed:
                    candidates.append((identifier, fingerprint, stale_record_id))
                if len(candidates) >= 20:
                    break
            if not candidates:
                uow.rollback()
                return 0
            ids = tuple(item[0] for item in candidates)
            fingerprints = tuple(item[1] for item in candidates)
            stale_record_ids = tuple(item[2] for item in candidates if item[2] is not None)
            uow.rollback()

        occurrence = "assistant:history-reconcile:" + hashlib.sha256(",".join(map(str, ids)).encode()).hexdigest()[:24]
        confirmed = _assistant_write_confirmed(
            self._effect_call(
                self._effects.redo_transfer_history,
                ids,
                occurrence=occurrence,
                checkpoint=checkpoint,
            )
        )
        if not confirmed:
            return 0
        with self._unit_of_work() as uow:
            state = self._locked_state(uow, "history_reconcile")
            last_retry_raw = state.get("last_retry")
            last_retry = dict(last_retry_raw) if isinstance(last_retry_raw, Mapping) else {}
            last_retry.update({fingerprint: now for fingerprint in fingerprints})
            if stale_record_ids:
                uow.session.execute(
                    text("DELETE FROM p115_organize_records WHERE id = ANY(CAST(:ids AS integer[]))"),
                    {"ids": list(stale_record_ids)},
                )
            state["last_retry"] = last_retry
            state["last_run_at"] = now
            state["last_reconciled"] = len(ids)
            state["last_local_records_cleaned"] = len(stale_record_ids)
            self._write_state(uow, "history_reconcile", state)
            uow.commit()
        return len(ids)

    def _transfer_history_pages(
        self,
        *,
        status: bool | None,
        page_limit: int,
        checkpoint: Callable[[], None],
    ) -> tuple[Mapping[str, Any], ...]:
        if self._provider is None:
            return ()
        page_size = 100
        records: list[Mapping[str, Any]] = []
        for page in range(1, page_limit + 1):
            checkpoint()
            values = tuple(
                self._provider.list_transfer_history(
                    status=status,
                    limit=page_size,
                    page=page,
                )
            )
            records.extend(value for value in values if isinstance(value, Mapping))
            if len(values) < page_size:
                break
        return tuple(records)

    def _subscription_sync(self, checkpoint: Callable[[], None]) -> int:
        if self._provider is None:
            return 0
        result = self._provider.list_subscriptions()
        values = getattr(result, "subscriptions", result)
        if not isinstance(values, Sequence) or isinstance(values, str | bytes):
            return 0
        now = datetime.now(UTC).timestamp()
        changed = 0
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            from etk.domains.media.subscription_projection import SqlAlchemySubscriptionMediaProjection

            projection = SqlAlchemySubscriptionMediaProjection()
            for value in values[:100]:
                if not isinstance(value, Mapping):
                    continue
                checkpoint()
                subscribe_id = _positive_int(value.get("id"))
                tmdb_id = str(value.get("tmdbid") or value.get("tmdb_id") or "").strip()
                season = _positive_int(value.get("season")) or None
                if subscribe_id <= 0 or not tmdb_id:
                    continue
                projected = _assistant_subscription_projection(value)
                if not projected:
                    continue
                state_key = str(subscribe_id)
                current = dict(_mapping_value(subscriptions.get(state_key)))
                merged = _merge_assistant_subscription(current, value, subscribe_id, now)
                if merged != current:
                    changed += 1
                subscriptions[state_key] = merged
                row = uow.session.execute(
                    text(
                        """
                        UPDATE subscribe_assistant_snapshots
                        SET subscribe_json = COALESCE(subscribe_json, '{}'::jsonb)
                            || CAST(:value AS jsonb),
                            last_checked_at = NOW()
                        WHERE subscribe_id = :subscribe_id
                          AND tmdb_id = :tmdb_id
                          AND (:season IS NULL OR season_number = :season)
                        """
                    ),
                    {
                        "value": json.dumps(projected, ensure_ascii=False),
                        "subscribe_id": subscribe_id,
                        "tmdb_id": tmdb_id,
                        "season": season,
                    },
                )
                changed += int(getattr(row, "rowcount", 0) or 0)
                changed += _sync_assistant_subscription_projection(uow, projection, value, subscribe_id)
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            uow.commit()
        return changed

    def _no_download_check(self, checkpoint: Callable[[], None]) -> int:
        wait_days = max(
            0,
            min(_assistant_int(self._configuration().get("tv_no_download_days"), 0), 3650),
        )
        actions = _assistant_strings(self._configuration().get("no_download_actions"))
        if wait_days <= 0 or not actions or self._effects is None:
            return 0
        now = datetime.now(UTC).timestamp()
        with self._unit_of_work() as uow:
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
        candidates: list[tuple[str, dict[str, Any]]] = []
        for key, raw in subscriptions.items():
            if not isinstance(raw, Mapping):
                continue
            task = dict(raw)
            if _assistant_bool(task.get("deleted")) or _assistant_bool(task.get("full_washing")):
                continue
            subscribe_id = _positive_int(task.get("subscribe_id") or key)
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _positive_int(task.get("season"))
            info = _mapping_value(task.get("subscribe_info"))
            state = str(task.get("mp_state") or info.get("state") or "").strip().upper()
            started_at = _assistant_float(task.get("created_at") or task.get("updated_at"))
            if (
                subscribe_id <= 0
                or not tmdb_id
                or season <= 0
                or state not in {"", "R"}
                or _assistant_float(task.get("download_started_at")) > 0
                or started_at <= 0
                or now - started_at < wait_days * 86_400
            ):
                continue
            last_action = _assistant_float(task.get("no_download_action_at"))
            if last_action > 0 and now - last_action < 86_400:
                continue
            candidates.append((str(key), task))
            if len(candidates) >= 100:
                break
        changed = 0
        for state_key, task in candidates:
            checkpoint()
            subscribe_id = _positive_int(task.get("subscribe_id") or state_key)
            tmdb_id = str(task.get("tmdb_id") or "").strip()
            season = _positive_int(task.get("season")) or None
            if subscribe_id <= 0 or not tmdb_id:
                continue
            action_cycle = _assistant_float(task.get("no_download_action_at"))
            occurrence_prefix = f"assistant:no-download:{subscribe_id}:{action_cycle:.6f}"
            acted = False
            if "search" in actions:
                acted = (
                    _assistant_write_confirmed(
                        self._effect_call(
                            self._effects.search_subscription,
                            subscribe_id,
                            occurrence=f"{occurrence_prefix}:search",
                            checkpoint=checkpoint,
                        )
                    )
                    or acted
                )
            target = "P" if "pending" in actions else ("S" if "pause" in actions else None)
            if target:
                acted = (
                    _assistant_write_confirmed(
                        self._effect_call(
                            self._effects.update_subscription_status,
                            int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                            season=season,
                            status=target,
                            total_episodes=_positive_int(task.get("total_episode")) or None,
                            occurrence=f"{occurrence_prefix}:status:{target}",
                            checkpoint=checkpoint,
                        )
                    )
                    or acted
                )
            if acted:
                with self._unit_of_work() as uow:
                    current = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
                    latest = current.get(state_key)
                    if not isinstance(latest, Mapping):
                        continue
                    latest_task = dict(latest)
                    # Do not overwrite a webhook that registered a download
                    # while the provider effect was in flight.
                    if (
                        _assistant_bool(latest_task.get("deleted"))
                        or _assistant_bool(latest_task.get("full_washing"))
                        or _assistant_float(latest_task.get("download_started_at")) > 0
                        or _assistant_float(latest_task.get("no_download_action_at")) != action_cycle
                    ):
                        continue
                    sources = dict(_mapping_value(latest_task.get("active_sources")))
                    sources["no_download"] = f"{wait_days} days without a download"
                    latest_task["active_sources"] = sources
                    latest_task["no_download_action_at"] = now
                    latest_task["last_reason"] = f"{wait_days} days without a download"
                    latest_task["updated_at"] = now
                    applied_state = str(target or "R")
                    if target:
                        latest_task["mp_state"] = applied_state
                        info = dict(_mapping_value(latest_task.get("subscribe_info")))
                        info["state"] = applied_state
                        latest_task["subscribe_info"] = info
                        latest_task["expected_mp_update"] = {
                            "fields": ["state"],
                            "state": applied_state,
                            "total_episode": None,
                            "updated_at": now,
                        }
                    current[state_key] = latest_task
                    self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, current)
                    uow.commit()
                changed += 1
        return changed

    def _download_check(self, checkpoint: Callable[[], None]) -> int:
        if self._provider is None:
            return 0
        monitor = getattr(self._provider, "monitor_downloads", None)
        authoritative = False
        if callable(monitor):
            snapshot = monitor()
            raw_live = getattr(snapshot, "downloads", None)
            authoritative = bool(getattr(snapshot, "authoritative", False))
        else:
            raw_live = self._provider.active_downloads()
        if raw_live is None:
            return 0
        live = tuple(raw_live)
        if not live:
            # Preserve monitor state on an empty response.  MoviePilot and
            # downloader upgrades have both produced transient empty lists.
            return 0
        live_entries: dict[str, dict[str, Any]] = {}
        for value in live:
            entry = _assistant_download_entry(value)
            normalized_hash = str(entry.get("hash") or "").strip().casefold()
            if not normalized_hash:
                continue
            live_entries.setdefault(normalized_hash, entry)
        live_hashes = set(live_entries)
        if not live_hashes:
            return 0

        with self._unit_of_work() as uow:
            monitored = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            uow.rollback()
        needs_subscription_match = any(
            task_hash not in monitored
            and _assistant_bool(entry.get("moviepilot_active"))
            and _positive_int(entry.get("subscribe_id")) <= 0
            and bool(str(entry.get("tmdb_id") or "").strip())
            for task_hash, entry in live_entries.items()
        )
        provider_subscriptions: tuple[Mapping[str, Any], ...] = ()
        if needs_subscription_match:
            listed = self._provider.list_subscriptions()
            values = getattr(listed, "subscriptions", listed)
            if not isinstance(values, Sequence) or isinstance(values, str | bytes):
                raise RuntimeError("MoviePilot subscription list is unavailable for download reconciliation")
            provider_subscriptions = tuple(value for value in values if isinstance(value, Mapping))

        changed = 0
        now = datetime.now(UTC).timestamp()
        for task_hash, entry in live_entries.items():
            if task_hash in monitored or not _assistant_bool(entry.get("moviepilot_active")):
                continue
            subscribe_id = _positive_int(entry.get("subscribe_id"))
            if subscribe_id <= 0:
                matches = _assistant_matching_subscriptions(entry, provider_subscriptions)
                if len(matches) != 1:
                    continue
                subscribe_id = _positive_int(matches[0].get("id") or matches[0].get("subscribe_id"))
            if subscribe_id <= 0:
                continue
            registered = {
                "hash": task_hash,
                "subscribe_id": subscribe_id,
                "baseline_progress": _assistant_float(entry.get("progress")),
                "baseline_at": _assistant_float(entry.get("added_at"), now) or now,
                "retry_count": 0,
                "time": _assistant_float(entry.get("added_at"), now) or now,
                "tmdb_id": str(entry.get("tmdb_id") or ""),
                "season": _positive_int(entry.get("season")) or None,
                "episodes": [_positive_int(entry.get("episode"))] if _positive_int(entry.get("episode")) else [],
                "title": str(entry.get("title") or "")[:500],
                "source": "moviepilot_active_reconcile",
            }
            with self._unit_of_work() as uow:
                current = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
                if task_hash in current:
                    uow.rollback()
                    continue
                subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
                current[task_hash] = registered
                subscription = dict(_mapping_value(subscriptions.get(str(subscribe_id))))
                subscription["subscribe_id"] = subscribe_id
                subscription["tmdb_id"] = registered["tmdb_id"] or subscription.get("tmdb_id") or ""
                subscription["season"] = registered["season"] or subscription.get("season")
                sources = dict(_mapping_value(subscription.get("active_sources")))
                sources[_SOURCE_DOWNLOAD_PENDING] = "下载已发起，等待整理入库"
                subscription["active_sources"] = sources
                subscription["download_started_at"] = now
                subscription["updated_at"] = now
                subscriptions[str(subscribe_id)] = subscription
                self._write_state(uow, _ASSISTANT_TORRENTS_KEY, current)
                self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
                uow.commit()
            changed += 1

        with self._unit_of_work() as uow:
            monitored = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            uow.rollback()
        configuration = self._configuration()
        manual_delete = _assistant_config_bool(configuration, "manual_delete_listen", True)
        tracker_listen = _assistant_config_bool(configuration, "tracker_response_listen", True)
        exclude_tags = _assistant_strings(configuration.get("delete_exclude_tags") or ("H&R",))
        tracker_keywords = _assistant_strings(
            configuration.get("tracker_keywords") or ("torrent not registered with this tracker", "torrent banned")
        )
        timeout_seconds = max(_assistant_int(configuration.get("download_timeout_minutes"), 120), 1) * 60
        progress_threshold = max(_assistant_float(configuration.get("download_progress_threshold"), 10), 0)
        retry_limit = max(_assistant_int(configuration.get("download_retry_limit"), 3), 1)
        now = datetime.now(UTC).timestamp()
        for task_hash, raw_task in tuple(monitored.items()):
            if not isinstance(raw_task, Mapping):
                continue
            checkpoint()
            task = dict(raw_task)
            entry = live_entries.get(str(task_hash).casefold())
            if entry is None:
                if manual_delete and authoritative and self._remove_download_state(task_hash, task, "下载任务已不存在"):
                    changed += 1
                continue
            tags = _assistant_strings(entry.get("tags"))
            if exclude_tags and any(
                configured == actual or configured in actual for configured in exclude_tags for actual in tags
            ):
                if self._remove_download_state(task_hash, task, "下载任务命中排除标签"):
                    changed += 1
                continue
            tracker_text = str(entry.get("tracker_text") or "").casefold()
            tracker_keyword = next(
                (keyword for keyword in tracker_keywords if tracker_listen and keyword in tracker_text),
                "",
            )
            if tracker_keyword:
                reason = f"Tracker响应命中：{tracker_keyword}"
                if not self._delete_monitored_download(task_hash, task, reason, checkpoint):
                    raise RuntimeError("durable MoviePilot tracker-response cleanup was rejected")
                if self._remove_download_state(task_hash, task, reason):
                    changed += 1
                continue
            progress = _assistant_float(entry.get("progress"))
            state = str(entry.get("state") or "").strip().casefold()
            if progress >= 100 or state in {"已完成", "completed", "complete"}:
                if self._remove_download_state(task_hash, task, "下载已完成"):
                    changed += 1
                continue
            baseline = _assistant_float(task.get("baseline_progress"))
            baseline_at = _assistant_float(task.get("baseline_at") or task.get("time"), now)
            if now - baseline_at < timeout_seconds:
                continue
            if progress - baseline >= progress_threshold:
                self._update_download_progress(task_hash, task, progress=progress, retry_count=0, updated_at=now)
                continue
            retry_count = _assistant_int(task.get("retry_count")) + 1
            if retry_count >= retry_limit:
                self._update_download_progress(
                    task_hash,
                    task,
                    progress=baseline,
                    retry_count=retry_count,
                    updated_at=now,
                )
                continue
            if not self._delete_monitored_download(task_hash, task, "下载超时停滞", checkpoint):
                self._update_download_progress(
                    task_hash,
                    task,
                    progress=baseline,
                    retry_count=retry_count,
                    updated_at=now,
                )
                raise RuntimeError("durable MoviePilot stalled-download cleanup was rejected")
            if self._remove_download_state(task_hash, task, "下载超时停滞"):
                changed += 1
        return changed

    def _delete_monitored_download(
        self,
        task_hash: str,
        task: Mapping[str, Any],
        reason: str,
        checkpoint: Callable[[], None],
    ) -> bool:
        if self._effects is None:
            return False
        fingerprint = _assistant_delete_fingerprint(task)
        if _assistant_config_bool(self._configuration(), "skip_deletion", True):
            with self._unit_of_work() as uow:
                recently_deleted = bool(
                    uow.session.scalar(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM subscribe_assistant_delete_records "
                            "WHERE fingerprint = :fingerprint "
                            "AND (expires_at IS NULL OR expires_at > NOW()))"
                        ),
                        {"fingerprint": fingerprint},
                    )
                )
                uow.rollback()
            if recently_deleted:
                return True
        cycle = _assistant_download_cycle(task)
        prefix = f"assistant:download:{str(task_hash).casefold()}:{cycle}"
        confirmed = _assistant_write_confirmed(
            self._effect_call(
                self._effects.delete_download_tasks,
                (task_hash,),
                operation_occurrence=f"{prefix}:delete",
                occurrence=f"{prefix}:delete",
                checkpoint=checkpoint,
            )
        )
        if not confirmed:
            return False
        subscribe_id = _positive_int(task.get("subscribe_id"))
        if subscribe_id > 0 and _assistant_config_bool(self._configuration(), "auto_search_when_delete", True):
            searched = _assistant_write_confirmed(
                self._effect_call(
                    self._effects.search_subscription,
                    subscribe_id,
                    occurrence=f"{prefix}:search",
                    checkpoint=checkpoint,
                )
            )
            if searched and self._provider is not None:
                repeated = any(
                    str(_assistant_download_entry(value).get("hash") or "").casefold() == str(task_hash).casefold()
                    for value in self._provider.active_downloads()
                )
                if repeated and not _assistant_write_confirmed(
                    self._effect_call(
                        self._effects.delete_download_tasks,
                        (task_hash,),
                        operation_occurrence=f"{prefix}:post-search-delete",
                        occurrence=f"{prefix}:post-search-delete",
                        checkpoint=checkpoint,
                    )
                ):
                    return False
        retention_hours = max(_assistant_int(self._configuration().get("delete_record_retention_hours"), 24), 1)
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    INSERT INTO subscribe_assistant_delete_records (
                        fingerprint, tmdb_id, season_number, episode_json, reason, created_at, expires_at
                    ) VALUES (
                        :fingerprint, :tmdb_id, :season, CAST(:episodes AS jsonb), :reason,
                        NOW(), NOW() + make_interval(hours => :hours)
                    )
                    ON CONFLICT (fingerprint) DO UPDATE SET
                        reason = EXCLUDED.reason,
                        expires_at = EXCLUDED.expires_at,
                        created_at = NOW()
                    """
                ),
                {
                    "fingerprint": fingerprint,
                    "tmdb_id": str(task.get("tmdb_id") or "") or None,
                    "season": _positive_int(task.get("season")) or None,
                    "episodes": json.dumps(list(_assistant_int_values(task.get("episodes")))),
                    "reason": reason,
                    "hours": retention_hours,
                },
            )
            uow.commit()
        return True

    def _remove_download_state(self, task_hash: str, task: Mapping[str, Any], reason: str) -> bool:
        cycle = _assistant_download_cycle(task)
        subscribe_id = _positive_int(task.get("subscribe_id"))
        with self._unit_of_work() as uow:
            torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            latest = torrents.get(task_hash)
            if not isinstance(latest, Mapping) or _assistant_download_cycle(latest) != cycle:
                uow.rollback()
                return False
            torrents.pop(task_hash, None)
            subscriptions = self._locked_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY)
            if subscribe_id > 0:
                subscription = dict(_mapping_value(subscriptions.get(str(subscribe_id))))
                sources = dict(_mapping_value(subscription.get("active_sources")))
                sources.pop(_SOURCE_DOWNLOAD_PENDING, None)
                subscription["active_sources"] = sources
                subscription["last_reason"] = reason
                subscription["updated_at"] = datetime.now(UTC).timestamp()
                subscriptions[str(subscribe_id)] = subscription
            self._write_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
            self._write_state(uow, _ASSISTANT_SUBSCRIPTIONS_KEY, subscriptions)
            uow.commit()
        return True

    def _update_download_progress(
        self,
        task_hash: str,
        task: Mapping[str, Any],
        *,
        progress: float,
        retry_count: int,
        updated_at: float,
    ) -> bool:
        cycle = _assistant_download_cycle(task)
        with self._unit_of_work() as uow:
            torrents = self._locked_state(uow, _ASSISTANT_TORRENTS_KEY)
            latest = torrents.get(task_hash)
            if not isinstance(latest, Mapping) or _assistant_download_cycle(latest) != cycle:
                uow.rollback()
                return False
            updated = dict(latest)
            updated["baseline_progress"] = progress
            updated["baseline_at"] = updated_at
            updated["retry_count"] = retry_count
            torrents[task_hash] = updated
            self._write_state(uow, _ASSISTANT_TORRENTS_KEY, torrents)
            uow.commit()
        return True

    def _snapshot_verify(self, checkpoint: Callable[[], None]) -> int:
        if self._provider is None:
            return 0
        verify_interval = max(_assistant_int(self._configuration().get("verify_interval_hours"), 12), 1)
        checked = 0
        for _ in range(100):
            checkpoint()
            with self._unit_of_work() as uow:
                row = (
                    uow.session.execute(
                        text(
                            """
                            SELECT id, tmdb_id, season_number, subscribe_id, scope_total, subscribe_json
                            FROM subscribe_assistant_snapshots
                            WHERE (last_checked_at IS NULL OR last_checked_at < NOW() - make_interval(hours => :hours))
                              AND (claim_token IS NULL OR lease_expires_at <= NOW())
                            ORDER BY id
                            LIMIT 1
                            FOR UPDATE SKIP LOCKED
                            """
                        ),
                        {"hours": verify_interval},
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    return checked
                token = uuid4()
                uow.session.execute(
                    text(
                        """
                        UPDATE subscribe_assistant_snapshots
                        SET job_id = :job_id, attempt_id = :attempt_id, worker_id = :worker_id,
                            claim_token = :claim_token, claimed_at = NOW(),
                            lease_expires_at = NOW() + INTERVAL '5 minutes'
                        WHERE id = :id
                        """
                    ),
                    {
                        "id": row["id"],
                        "job_id": self._job_id,
                        "attempt_id": self._attempt_id,
                        "worker_id": self._worker_id,
                        "claim_token": token,
                    },
                )
                uow.commit()
            snapshot_id = int(row["id"])
            readback_ok = False
            verify_error: str | None = None
            repaired = False
            skipped: str | None = None

            def before_effect(snapshot_id: int = snapshot_id, token: UUID = token) -> None:
                checkpoint()
                self._renew_snapshot_claim(snapshot_id, token)

            try:
                before_effect()
                tmdb_id = str(row.get("tmdb_id") or "").strip()
                season = _positive_int(row.get("season_number"))
                subscribe_id = _positive_int(row.get("subscribe_id"))
                if not tmdb_id or season <= 0:
                    skipped = "invalid_identity"
                elif self._snapshot_suppressed(tmdb_id, season, subscribe_id):
                    skipped = "restore_suppressed"
                elif self._snapshot_total_locked(tmdb_id, season):
                    skipped = "season_total_locked"
                else:
                    result = self._provider.query_subscription(
                        int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                        season=season,
                    )
                    if result is None:
                        raise RuntimeError("MoviePilot subscription readback is unavailable")
                    if _assistant_query_found(result):
                        readback_ok = True
                        subscription = _assistant_query_subscription(result)
                        expected_total = _positive_int(row.get("scope_total"))
                        observed_total = _positive_int(
                            subscription.get("total_episode")
                            or subscription.get("total_episodes")
                            or subscription.get("total")
                        )
                        if self._effects is not None and expected_total > 0 and 0 < observed_total < expected_total:
                            repaired = _assistant_write_confirmed(
                                self._effect_call(
                                    self._effects.update_subscription_status,
                                    int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
                                    season=season,
                                    status=str(subscription.get("state") or "R").strip().upper() or "R",
                                    total_episodes=expected_total,
                                    occurrence=f"assistant:snapshot:{snapshot_id}:total:{expected_total}",
                                    checkpoint=before_effect,
                                )
                            )
                        provider_subscribe_id = _positive_int(
                            subscription.get("id") or subscription.get("subscribe_id") or subscribe_id
                        )
                        if (
                            self._effects is not None
                            and _assistant_config_bool(self._configuration(), "auto_search_when_delete", True)
                            and provider_subscribe_id > 0
                        ):
                            repaired = (
                                _assistant_write_confirmed(
                                    self._effect_call(
                                        self._effects.search_subscription,
                                        provider_subscribe_id,
                                        occurrence=f"assistant:snapshot:{snapshot_id}:search:{provider_subscribe_id}",
                                        checkpoint=before_effect,
                                    )
                                )
                                or repaired
                            )
                    else:
                        verify_error = "MoviePilot subscription snapshot is not present"
                        if self._effects is not None:
                            payload = _assistant_repair_payload(cast(Mapping[str, Any], row))
                            if payload is not None:
                                repaired = _assistant_write_confirmed(
                                    self._effect_call(
                                        self._effects.create_subscription,
                                        payload,
                                        consume_quota=False,
                                        apply_local_continuation=False,
                                        occurrence=f"assistant:snapshot:{snapshot_id}:create",
                                        checkpoint=before_effect,
                                    )
                                )
                                readback_ok = repaired
                before_effect()
                self._finish_snapshot_claim(
                    snapshot_id,
                    token,
                    {
                        "last_verify_ok": readback_ok,
                        "last_verify_repaired": repaired,
                        "last_verify_error": None if readback_ok or skipped else verify_error,
                        "last_verify_skipped": skipped,
                    },
                )
            except BaseException as exc:
                self._release_snapshot_claim(snapshot_id, token, error=type(exc).__name__)
                raise
            checked += 1
        return checked

    def _renew_snapshot_claim(self, snapshot_id: int, token: UUID) -> None:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE subscribe_assistant_snapshots
                    SET lease_expires_at = NOW() + INTERVAL '5 minutes'
                    WHERE id = :id AND claim_token = :claim_token
                      AND job_id = :job_id AND attempt_id = :attempt_id
                      AND worker_id = :worker_id AND lease_expires_at > NOW()
                    """
                ),
                {
                    "id": snapshot_id,
                    "claim_token": token,
                    "job_id": self._job_id,
                    "attempt_id": self._attempt_id,
                    "worker_id": self._worker_id,
                },
            )
            if int(getattr(result, "rowcount", 0) or 0) != 1:
                uow.rollback()
                raise RuntimeError("subscribe assistant snapshot claim was lost")
            uow.commit()

    def _finish_snapshot_claim(
        self,
        snapshot_id: int,
        token: UUID,
        patch: Mapping[str, Any],
    ) -> None:
        with self._unit_of_work() as uow:
            result = uow.session.execute(
                text(
                    """
                    UPDATE subscribe_assistant_snapshots
                    SET last_checked_at = NOW(),
                        subscribe_json = COALESCE(subscribe_json, '{}'::jsonb) || CAST(:verify_patch AS jsonb),
                        job_id = NULL, attempt_id = NULL, worker_id = NULL,
                        claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL
                    WHERE id = :id AND claim_token = :claim_token
                      AND job_id = :job_id AND attempt_id = :attempt_id
                      AND worker_id = :worker_id AND lease_expires_at > NOW()
                    """
                ),
                {
                    "id": snapshot_id,
                    "claim_token": token,
                    "job_id": self._job_id,
                    "attempt_id": self._attempt_id,
                    "worker_id": self._worker_id,
                    "verify_patch": json.dumps(dict(patch), ensure_ascii=False),
                },
            )
            if int(getattr(result, "rowcount", 0) or 0) != 1:
                uow.rollback()
                raise RuntimeError("subscribe assistant snapshot claim was lost")
            uow.commit()

    def _release_snapshot_claim(self, snapshot_id: int, token: UUID, *, error: str) -> None:
        with self._unit_of_work() as uow:
            uow.session.execute(
                text(
                    """
                    UPDATE subscribe_assistant_snapshots
                    SET subscribe_json = COALESCE(subscribe_json, '{}'::jsonb) || CAST(:verify_patch AS jsonb),
                        job_id = NULL, attempt_id = NULL, worker_id = NULL,
                        claim_token = NULL, claimed_at = NULL, lease_expires_at = NULL
                    WHERE id = :id AND claim_token = :claim_token
                    """
                ),
                {
                    "id": snapshot_id,
                    "claim_token": token,
                    "verify_patch": json.dumps({"last_verify_error": error}, ensure_ascii=False),
                },
            )
            uow.commit()

    def _snapshot_suppressed(self, tmdb_id: str, season: int, subscribe_id: int) -> bool:
        now = datetime.now(UTC).timestamp()
        keys = set(_assistant_suppression_keys(tmdb_id, season, subscribe_id))
        with self._unit_of_work() as uow:
            suppressions = self._locked_state(uow, _ASSISTANT_SUPPRESSIONS_KEY)
            for key, value in tuple(suppressions.items()):
                if _assistant_float(_mapping_value(value).get("expires_at")) <= now:
                    suppressions.pop(key, None)
            suppressed = any(key in suppressions for key in keys)
            self._write_state(uow, _ASSISTANT_SUPPRESSIONS_KEY, suppressions)
            uow.commit()
        return suppressed

    def _snapshot_total_locked(self, tmdb_id: str, season: int) -> bool:
        with self._unit_of_work() as uow:
            locked = bool(
                uow.session.scalar(
                    text(
                        """
                        SELECT COALESCE(total_episodes_locked, FALSE)
                        FROM media_metadata
                        WHERE parent_series_tmdb_id = :tmdb_id
                          AND season_number = :season
                          AND item_type = 'Season'
                        ORDER BY last_updated_at DESC NULLS LAST, tmdb_id
                        LIMIT 1
                        """
                    ),
                    {"tmdb_id": tmdb_id, "season": season},
                )
            )
            uow.rollback()
        return locked

    def _effect_call(
        self,
        method: Callable[..., Any],
        *args: Any,
        checkpoint: Callable[[], None],
        occurrence: str | None = None,
        **kwargs: Any,
    ) -> Any:
        from etk.processes.p7_subscription_effects import (
            bind_moviepilot_execution,
            bind_moviepilot_operation_occurrence,
        )

        with bind_moviepilot_execution(
            job_id=str(self._job_id),
            attempt_id=str(self._attempt_id),
            worker_id=self._worker_id,
            task_key="subscribe-assistant-maintenance",
            checkpoint=checkpoint,
        ):
            if occurrence is None:
                return method(*args, **kwargs)
            with bind_moviepilot_operation_occurrence(occurrence):
                return method(*args, **kwargs)


def _assistant_download_entry(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        raw = value
        task_hash = raw.get("task_hash") or raw.get("hash") or raw.get("hashString")
        return {
            "hash": str(task_hash or "").strip().casefold(),
            "subscribe_id": raw.get("subscribe_id") or raw.get("subscribeid"),
            "tmdb_id": raw.get("tmdb_id") or raw.get("tmdbid"),
            "season": raw.get("season"),
            "episode": raw.get("episode"),
            "progress": raw.get("progress"),
            "state": raw.get("state") or raw.get("status"),
            "tags": raw.get("tags") or raw.get("labels") or raw.get("category") or (),
            "tracker_text": raw.get("tracker_text") or raw.get("tracker") or raw.get("message") or "",
            "added_at": raw.get("added_at") or raw.get("added_on") or raw.get("time"),
            "title": raw.get("title") or raw.get("name") or "",
            "moviepilot_active": raw.get("moviepilot_active", True),
        }
    return {
        "hash": str(getattr(value, "task_hash", "") or "").strip().casefold(),
        "subscribe_id": getattr(value, "subscribe_id", None),
        "tmdb_id": getattr(value, "tmdb_id", None),
        "season": getattr(value, "season", None),
        "episode": getattr(value, "episode", None),
        "progress": getattr(value, "progress", None),
        "state": getattr(value, "state", ""),
        "tags": getattr(value, "tags", ()),
        "tracker_text": getattr(value, "tracker_text", ""),
        "added_at": getattr(value, "added_at", None),
        "title": getattr(value, "title", ""),
        "moviepilot_active": getattr(value, "moviepilot_active", True),
    }


def _assistant_matching_subscriptions(
    entry: Mapping[str, Any],
    subscriptions: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    tmdb_id = str(entry.get("tmdb_id") or "").strip()
    season = _positive_int(entry.get("season"))
    if not tmdb_id:
        return ()
    return tuple(
        value
        for value in subscriptions
        if str(value.get("tmdbid") or value.get("tmdb_id") or "").strip() == tmdb_id
        and (season <= 0 or _positive_int(value.get("season")) == season)
        and _positive_int(value.get("id") or value.get("subscribe_id")) > 0
    )


def _assistant_config_bool(configuration: Mapping[str, Any], key: str, default: bool) -> bool:
    return default if key not in configuration else _assistant_bool(configuration.get(key))


def _assistant_suppression_keys(tmdb_id: str, season: int, subscribe_id: int) -> tuple[str, ...]:
    keys = [f"tmdb:{tmdb_id}:S{season}"] if tmdb_id and season > 0 else []
    if subscribe_id > 0:
        keys.append(f"sub:{subscribe_id}")
    return tuple(keys)


def _assistant_int_values(value: Any) -> tuple[int, ...]:
    values = value if isinstance(value, Sequence) and not isinstance(value, str | bytes) else (value,)
    return tuple(number for number in (_positive_int(item) for item in values) if number > 0)


def _assistant_delete_fingerprint(task: Mapping[str, Any]) -> str:
    material = "|".join(
        (
            str(task.get("tmdb_id") or ""),
            str(task.get("season") or ""),
            ",".join(map(str, _assistant_int_values(task.get("episodes")))),
            str(task.get("title") or ""),
            str(task.get("enclosure") or ""),
            str(task.get("page_url") or ""),
        )
    )
    return hashlib.sha1(material.encode("utf-8", errors="ignore")).hexdigest()


def _assistant_download_cycle(task: Mapping[str, Any]) -> str:
    started_at = _assistant_float(task.get("time") or task.get("created_at"))
    if started_at > 0:
        return f"{started_at:.6f}"
    material = canonical_json(
        {
            "subscribe_id": task.get("subscribe_id"),
            "tmdb_id": task.get("tmdb_id"),
            "season": task.get("season"),
            "episodes": list(_assistant_int_values(task.get("episodes"))),
            "title": task.get("title"),
        },
        field="assistant download cycle",
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _assistant_season_consistency_ok(
    uow: SqlAlchemyUnitOfWork,
    *,
    tmdb_id: str,
    season: int,
    expected_count: int,
) -> bool:
    if not tmdb_id or season <= 0 or expected_count <= 0:
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
            {"tmdb_id": tmdb_id, "season": season},
        )
        .mappings()
        .all()
    )
    if len(rows) < expected_count:
        return False
    signatures: list[tuple[str, str, str, str]] = []
    for row in rows:
        assets = _asset_objects(row.get("asset_details_json"))
        if not assets:
            return False
        asset = assets[0]
        resolution = _known_signature(asset.get("resolution_display") or asset.get("resolution"))
        codec = _known_signature(asset.get("codec_display") or asset.get("video_codec"))
        if not resolution or not codec:
            return False
        group_value = asset.get("release_group_raw") or asset.get("release_group")
        if isinstance(group_value, Sequence) and not isinstance(group_value, str | bytes):
            group = next((str(value).strip() for value in group_value if str(value).strip()), "")
        else:
            group = str(group_value or "").split(",", 1)[0].strip()
        signatures.append(
            (
                resolution,
                codec,
                _known_signature(group),
                _known_signature(asset.get("effect_display") or asset.get("effect")),
            )
        )
    groups = {value[2] for value in signatures if value[2]}
    effects = {value[3] for value in signatures if value[3]}
    return (
        bool(signatures)
        and len({value[0] for value in signatures}) == 1
        and len({value[1] for value in signatures}) == 1
        and (not groups or (len(groups) == 1 and all(value[2] for value in signatures)))
        and (not effects or (len(effects) == 1 and all(value[3] for value in signatures)))
    )


def _set_assistant_season_active_washing(
    uow: SqlAlchemyUnitOfWork,
    *,
    tmdb_id: str,
    season: int,
    enabled: bool,
) -> None:
    uow.session.execute(
        text(
            """
            UPDATE media_metadata
            SET active_washing = :enabled, last_updated_at = NOW()
            WHERE parent_series_tmdb_id = :tmdb_id
              AND season_number = :season
              AND item_type = 'Season'
            """
        ),
        {"enabled": bool(enabled), "tmdb_id": tmdb_id, "season": season},
    )


def _assistant_int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _assistant_float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _assistant_strings(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset({value.strip().casefold()}) if value.strip() else frozenset()
    if not isinstance(value, Sequence):
        return frozenset()
    return frozenset(str(item).strip().casefold() for item in value if str(item).strip())


def _assistant_query_found(value: Any) -> bool:
    """Normalize the typed MoviePilot query result without trusting truthiness."""
    if isinstance(value, Mapping):
        if "found" in value:
            return value.get("found") is True
        if "disposition" in value:
            return str(value.get("disposition") or "").strip().casefold() == "found"
        return bool(value.get("subscription") or value.get("id"))
    found = getattr(value, "found", None)
    if isinstance(found, bool):
        return found
    disposition = getattr(value, "disposition", None)
    if disposition is not None:
        return str(getattr(disposition, "value", disposition)).strip().casefold() == "found"
    return False


def _assistant_query_subscription(value: Any) -> Mapping[str, Any]:
    """Return the bounded subscription payload from a typed or mapping result."""
    if isinstance(value, Mapping):
        raw = value.get("subscription")
        return dict(raw) if isinstance(raw, Mapping) else {}
    raw = getattr(value, "subscription", None)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _merge_assistant_subscription(
    current: Mapping[str, Any],
    provider_value: Mapping[str, Any],
    subscribe_id: int,
    now: float,
) -> dict[str, Any]:
    """Merge a bounded provider snapshot without erasing webhook-owned state."""
    merged = dict(current)
    projected = _assistant_subscription_projection(provider_value)
    info = dict(_mapping_value(merged.get("subscribe_info")))
    info.update(projected)
    merged["subscribe_info"] = info
    merged["subscribe_id"] = subscribe_id
    tmdb_id = str(provider_value.get("tmdbid") or provider_value.get("tmdb_id") or merged.get("tmdb_id") or "")
    season = _positive_int(provider_value.get("season") or merged.get("season"))
    if tmdb_id:
        merged["tmdb_id"] = tmdb_id
    if season > 0:
        merged["season"] = season
    state = str(provider_value.get("state") or provider_value.get("status") or "").strip().upper()
    if state:
        merged["mp_state"] = state
    merged.pop("deleted", None)
    merged.pop("snapshot_restore_suppressed", None)
    merged["last_event"] = "maintenance.subscription_sync"
    merged["last_reason"] = "maintenance.subscription_sync"
    merged["updated_at"] = now
    return merged


def _sync_assistant_subscription_projection(
    uow: SqlAlchemyUnitOfWork,
    projection: Any,
    provider_value: Mapping[str, Any],
    subscribe_id: int,
) -> int:
    """Publish a MoviePilot subscription into the native media projection."""
    tmdb_id = str(provider_value.get("tmdbid") or provider_value.get("tmdb_id") or "").strip()
    if not tmdb_id:
        return 0
    media_type = str(provider_value.get("type") or provider_value.get("media_type") or "").strip().casefold()
    season = _positive_int(provider_value.get("season"))
    if "电影" in media_type or media_type in {"movie", "movies"}:
        from etk.domains.media.subscription_projection import SubscriptionMediaKey

        media_key = SubscriptionMediaKey(tmdb_id, "Movie")
    elif season > 0 and ("电视" in media_type or "剧" in media_type or media_type in {"tv", "series"}):
        media_key = projection.lock_season_key(
            uow,
            parent_series_tmdb_id=tmdb_id,
            season_number=season,
        )
        if media_key is None:
            return 0
    else:
        return 0
    current = projection.lock_item(uow, media_key)
    if str((current or {}).get("subscription_status") or "").strip().upper() in {
        "PAUSED",
        "IGNORED",
        "PENDING_RELEASE",
    }:
        return 0
    request = {
        "title": provider_value.get("name") or provider_value.get("title"),
        "release_year": _positive_int(provider_value.get("year")),
        "season_number": season or None,
        "parent_series_tmdb_id": tmdb_id if getattr(media_key, "item_type", "") == "Season" else None,
    }
    return int(
        projection.set_status(
            uow,
            media_key,
            "SUBSCRIBED",
            request,
            {"type": "moviepilot", "subscribe_id": subscribe_id},
            None,
        )
    )


def _assistant_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on", "启用", "开启"}
    return bool(value)


def _assistant_repair_payload(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Build a small provider create payload from a frozen snapshot only."""
    tmdb_id = str(row.get("tmdb_id") or "").strip()
    season = _positive_int(row.get("season_number"))
    if not tmdb_id or season <= 0:
        return None
    raw = row.get("subscribe_json")
    info = dict(raw) if isinstance(raw, Mapping) else {}
    payload: dict[str, Any] = {
        "tmdbid": int(tmdb_id) if tmdb_id.isdigit() else tmdb_id,
        "season": season,
        "type": str(info.get("type") or "电视剧"),
        "name": str(info.get("name") or info.get("title") or tmdb_id).strip()[:500],
        "state": "R",
    }
    for key in ("best_version", "best_version_full", "total_episode"):
        value = _positive_int(info.get(key))
        if value > 0:
            payload[key] = value
    return payload


def _assistant_write_confirmed(value: Any) -> bool:
    """Normalize typed provider outcomes without relying on object truthiness."""
    if isinstance(value, bool):
        return value
    outcome = getattr(value, "outcome", None)
    if outcome is not None:
        normalized = str(getattr(outcome, "value", outcome)).strip().casefold()
        if normalized == "confirmed":
            return True
        if normalized == "rejected":
            return False
        if normalized == "indeterminate":
            raise AssistantMaintenanceEffectIndeterminate("MoviePilot maintenance effect outcome is indeterminate")
    if isinstance(value, Mapping):
        normalized = str(value.get("outcome") or value.get("status") or "").strip().casefold()
        if normalized in {"confirmed", "succeeded", "success"} or value.get("confirmed") is True:
            return True
        if normalized in {"rejected", "failed", "failure", "cancelled", "canceled"}:
            return False
        if normalized == "indeterminate":
            raise AssistantMaintenanceEffectIndeterminate("MoviePilot maintenance effect outcome is indeterminate")
    return bool(value)


_ASSISTANT_SUBSCRIPTION_FIELDS = frozenset(
    {
        "id",
        "subscribe_id",
        "tmdbid",
        "tmdb_id",
        "season",
        "type",
        "media_type",
        "name",
        "title",
        "year",
        "state",
        "status",
        "total_episode",
        "total_episodes",
        "best_version",
        "best_version_full",
        "category",
    }
)


def _assistant_subscription_projection(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep provider subscription sync data bounded and free of credentials/URLs."""
    projected: dict[str, Any] = {}
    for key in _ASSISTANT_SUBSCRIPTION_FIELDS:
        raw = value.get(key)
        if raw in (None, "", [], {}):
            continue
        if isinstance(raw, (str, int, float, bool)):
            projected[key] = str(raw).strip()[:500] if isinstance(raw, str) else raw
        elif isinstance(raw, list):
            projected[key] = [
                (str(item).strip()[:500] if isinstance(item, str) else item)
                for item in raw[:100]
                if isinstance(item, (str, int, float, bool))
            ]
    return projected


def _assistant_history_identity(value: Mapping[str, Any]) -> str:
    """Return a stable bounded identity for duplicate transfer-history pages."""
    identifier = _positive_int(value.get("id"))
    if identifier > 0:
        return f"id:{identifier}"
    parts = tuple(
        str(value.get(key) or "").strip().casefold()
        for key in ("pickcode", "pick_code", "fileid", "file_id", "fid", "title", "name")
    )
    material = "\0".join(parts).strip("\0")
    if not material:
        return "empty"
    return "digest:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def _history_text(value: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        raw = value.get(key)
        normalized = str(raw or "").strip()
        if normalized:
            return normalized
    return ""


def _assistant_history_failed(value: Mapping[str, Any]) -> bool:
    status = value.get("status")
    if status is False:
        return True
    normalized = str(status or "").strip().casefold()
    if normalized in {"false", "failed", "fail", "error", "0"}:
        return True
    return bool(_history_text(value, "errmsg", "error", "message"))


def _asset_objects(value: Any) -> tuple[Mapping[str, Any], ...]:
    normalized = value
    if isinstance(normalized, str):
        try:
            normalized = json.loads(normalized)
        except (TypeError, ValueError):
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
    return tuple(dict(item) for item in normalized if isinstance(item, Mapping))


def _known_signature(value: Any) -> str:
    normalized = str(value or "").strip()
    return "" if not normalized or normalized.casefold() in {"unknown", "未知", "无"} else normalized.casefold()


def _positive_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        result = int(value)
    except (TypeError, ValueError):
        return 0
    return result if result > 0 else 0
