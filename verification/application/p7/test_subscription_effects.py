from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

import task_manager
from database import settings_db as legacy_settings_db
from etk.domains.subscription.operations import SubscriptionOperation, SubscriptionOperationStatus
from etk.platform.tasks import TaskOutcomeStatus
from etk.processes.p7_subscription_effects import (
    DurableMoviePilotEffectIndeterminateError,
    PersistentMoviePilotEffectGateway,
    _scope,
    bind_moviepilot_operation_occurrence,
)
from etk.processes.p7_subscription_tasks import _explicit_outcome
from handler import moviepilot
from tasks import subscriptions


def _gateway() -> PersistentMoviePilotEffectGateway:
    return PersistentMoviePilotEffectGateway(
        cast(Any, object()),
        cast(Any, object()),
        cast(Any, object()),
    )


@pytest.fixture(autouse=True)
def _legacy_trace_logger(monkeypatch) -> None:
    monkeypatch.setattr(subscriptions.logger, "trace", lambda *_args, **_kwargs: None, raising=False)


def test_quota_consuming_moviepilot_fallback_fails_before_provider_write(monkeypatch) -> None:
    monkeypatch.setattr(moviepilot, "_persistent_effect_gateway", None)
    monkeypatch.setattr(
        moviepilot.requests,
        "post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("provider write attempted")),
    )


def test_direct_subscription_quota_decrement_fails_without_database_access(monkeypatch) -> None:
    monkeypatch.setattr(
        legacy_settings_db,
        "get_db_connection",
        lambda: (_ for _ in ()).throw(AssertionError("database connection attempted")),
    )

    assert legacy_settings_db.decrement_subscription_quota() is False

    assert (
        moviepilot.subscribe_with_custom_payload(
            {"tmdbid": 101, "type": "电影", "name": "Fixture"},
            consume_quota=True,
        )
        is False
    )


def test_rejected_effect_returns_false_without_reporting_job_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        task_manager,
        "update_status_from_thread",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("gateway reported Job failure")),
    )
    gateway = _gateway()
    operation = cast(
        SubscriptionOperation,
        SimpleNamespace(
            status=SubscriptionOperationStatus.REJECTED,
            scope_key="fixture:rejected",
        ),
    )

    assert gateway._terminal_result(operation) is False


def test_occurrence_distinguishes_legitimate_repeated_delete_but_is_stable_across_retry() -> None:
    gateway = _gateway()
    identity = {
        "task_key": "subscribe-assistant-maintenance",
        "job_id": "00000000-0000-0000-0000-000000000701",
        "attempt_id": "00000000-0000-0000-0000-000000000702",
        "worker_id": "fixture-worker",
    }
    intents: list[Any] = []
    state = cast(Any, gateway)
    state._identity = lambda: dict(identity)
    state._execute = lambda intent: intents.append(intent) is None or True

    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:initial") is True
    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:post-search") is True
    assert gateway.delete_download_tasks(["ABC"], operation_occurrence="download-monitor:abc:initial") is True

    assert intents[0].scope_key != intents[1].scope_key
    assert intents[0].scope_key == intents[2].scope_key
    assert [intent.request["operation_occurrence"] for intent in intents] == [
        "download-monitor:abc:initial",
        "download-monitor:abc:post-search",
        "download-monitor:abc:initial",
    ]


def test_missing_occurrence_preserves_existing_scope_and_request_shape() -> None:
    gateway = _gateway()
    identity = {
        "task_key": "process-watchlist",
        "job_id": "00000000-0000-0000-0000-000000000703",
        "attempt_id": "00000000-0000-0000-0000-000000000704",
        "worker_id": "fixture-worker",
    }
    intents: list[Any] = []
    state = cast(Any, gateway)
    state._identity = lambda: dict(identity)
    state._execute = lambda intent: intents.append(intent) is None or True

    assert gateway.delete_download_tasks(["ABC"]) is True

    assert intents[0].scope_key == _scope(identity, "delete-downloads", "downloads", None, ["abc"])
    assert "operation_occurrence" not in intents[0].request


def test_auto_occurrence_is_stable_across_jobs_and_removes_job_id_from_request() -> None:
    gateway = _gateway()
    intents: list[Any] = []
    state = cast(Any, gateway)
    identity = {
        "task_key": "auto-subscribe",
        "job_id": "00000000-0000-0000-0000-000000000711",
        "attempt_id": "00000000-0000-0000-0000-000000000712",
        "worker_id": "fixture-worker",
    }
    state._identity = lambda: dict(identity)
    state._execute = lambda intent: intents.append(intent) is None or True

    with bind_moviepilot_operation_occurrence("auto:item:movie:101:generation-1"):
        assert gateway.create_subscription(
            {"name": "Fixture", "tmdbid": 101, "type": "电影"},
            consume_quota=False,
            apply_local_continuation=False,
        )
    identity["job_id"] = "00000000-0000-0000-0000-000000000713"
    with bind_moviepilot_operation_occurrence("auto:item:movie:101:generation-1"):
        assert gateway.create_subscription(
            {"name": "Fixture", "tmdbid": 101, "type": "电影"},
            consume_quota=False,
            apply_local_continuation=False,
        )

    assert intents[0].id == intents[1].id
    assert intents[0].scope_key == intents[1].scope_key
    assert intents[0].request_hash == intents[1].request_hash
    assert intents[0].request["origin"] == {
        "task_key": "auto-subscribe",
        "operation_occurrence": "auto:item:movie:101:generation-1",
    }


def test_transfer_history_secret_field_is_rejected_before_durable_reservation() -> None:
    gateway = _gateway()
    state = cast(Any, gateway)
    state._identity = lambda: {
        "task_key": "process-watchlist",
        "job_id": "00000000-0000-0000-0000-000000000705",
        "attempt_id": "00000000-0000-0000-0000-000000000706",
        "worker_id": "fixture-worker",
    }
    state._execute = lambda _intent: (_ for _ in ()).throw(AssertionError("invalid intent reached execution"))

    with pytest.raises(ValueError, match="secret field"):
        gateway.delete_transfer_history(
            {"id": 7, "authorization": "fixture-secret"},
            operation_occurrence="episode-residue:101:2:history:7",
        )


def test_transfer_history_occurrence_identity_ignores_mutable_record_fields() -> None:
    gateway = _gateway()
    intents: list[Any] = []
    state = cast(Any, gateway)
    state._identity = lambda: {
        "task_key": "process-watchlist",
        "job_id": "00000000-0000-0000-0000-000000000707",
        "attempt_id": "00000000-0000-0000-0000-000000000708",
        "worker_id": "fixture-worker",
    }
    state._execute = lambda intent: intents.append(intent) is None or True

    occurrence = "episode-residue:101:2:history:7"
    assert gateway.delete_transfer_history(
        {"id": 7, "name": "first"},
        operation_occurrence=occurrence,
    )
    assert gateway.delete_transfer_history(
        {"id": 7, "name": "changed"},
        operation_occurrence=occurrence,
    )

    assert intents[0].id == intents[1].id
    assert intents[0].scope_key == intents[1].scope_key
    assert intents[0].request_hash != intents[1].request_hash


def test_manual_subscription_rejection_returns_failed_task_outcome(monkeypatch) -> None:
    monkeypatch.setattr(subscriptions.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(subscriptions.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(subscriptions, "is_movie_subscribable", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(subscriptions.task_manager, "update_status_from_thread", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(subscriptions.task_manager, "has_persistent_execution", lambda: True)

    result = subscriptions.task_manual_subscribe_batch(
        object(),
        [{"tmdb_id": "101", "item_type": "Movie", "title": "Fixture"}],
    )

    assert result["status"] == "failed"
    assert result["retryable"] is False
    assert result["provider_failures"] == [{"action": "create", "tmdb_id": "101", "item_type": "Movie", "season": None}]
    assert _explicit_outcome("manual_subscribe_batch", result).status is TaskOutcomeStatus.FAILED


def test_full_series_indeterminate_effect_is_not_swallowed(monkeypatch) -> None:
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", lambda *_args: {})
    monkeypatch.setattr(
        subscriptions.tmdb,
        "get_tv_details",
        lambda *_args, **_kwargs: {"name": "Fixture", "seasons": []},
    )
    monkeypatch.setattr(subscriptions.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DurableMoviePilotEffectIndeterminateError("fixture unknown effect")
        ),
    )

    with pytest.raises(DurableMoviePilotEffectIndeterminateError, match="fixture unknown effect"):
        subscriptions._subscribe_full_series_with_logic(101, "Fixture", {}, "fixture-key")


def test_auto_subscription_stage_one_failure_survives_empty_wanted_list(monkeypatch) -> None:
    def setting(key: str):
        if key == "mp_config":
            return {"movie_search_window_days": 1, "movie_pause_days": 7}
        return {}

    monkeypatch.setattr(subscriptions.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", setting)
    monkeypatch.setattr(
        subscriptions.request_db,
        "get_movies_to_revive",
        lambda: [{"tmdb_id": "101", "title": "Fixture"}],
    )
    monkeypatch.setattr(subscriptions.request_db, "get_movies_to_pause", lambda **_kwargs: [])
    monkeypatch.setattr(subscriptions.media_db, "promote_pending_to_wanted", lambda: 0)
    monkeypatch.setattr(subscriptions.media_db, "get_all_wanted_media", lambda: [])
    monkeypatch.setattr(subscriptions.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: False,
    )
    monkeypatch.setattr(subscriptions.task_manager, "update_status_from_thread", lambda *_args, **_kwargs: None)

    result = subscriptions.task_auto_subscribe(SimpleNamespace(is_stop_requested=lambda: False))

    assert result["status"] == "failed"
    assert result["provider_failures"] == [{"action": "revive", "tmdb_id": "101", "item_type": "Movie", "season": None}]
    assert _explicit_outcome("auto-subscribe", result).status is TaskOutcomeStatus.FAILED


def test_auto_pending_state_rejection_prevents_local_success(monkeypatch) -> None:
    def setting(key: str):
        if key == "subscription_strategy_config":
            return {"subscription_sources": ["mp"]}
        if key == "mp_config":
            return {"movie_search_window_days": 0, "movie_pause_days": 0}
        return {}

    wanted = {
        "tmdb_id": "season-101-2",
        "parent_series_tmdb_id": "101",
        "item_type": "Season",
        "title": "Fixture S02",
        "season_number": 2,
        "subscription_status": "WANTED",
        "subscription_sources_json": [],
    }
    monkeypatch.setattr(subscriptions.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", setting)
    monkeypatch.setattr(subscriptions.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(subscriptions.media_db, "promote_pending_to_wanted", lambda: 0)
    monkeypatch.setattr(subscriptions.media_db, "get_all_wanted_media", lambda: [wanted])
    monkeypatch.setattr(subscriptions.media_db, "get_series_title_by_tmdb_id", lambda *_args: "Fixture")
    monkeypatch.setattr(subscriptions, "should_mark_as_pending", lambda *_args: (True, 99))
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(subscriptions.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(
        subscriptions.request_db,
        "set_media_status_subscribed",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("local state marked subscribed")),
    )
    monkeypatch.setattr(subscriptions.user_db, "get_admin_telegram_chat_ids", lambda: [])
    monkeypatch.setattr(subscriptions.task_manager, "update_status_from_thread", lambda *_args, **_kwargs: None)

    result = subscriptions.task_auto_subscribe(SimpleNamespace(is_stop_requested=lambda: False))

    assert result["status"] == "failed"
    assert result["provider_failures"] == [
        {"action": "update_status", "tmdb_id": "101", "item_type": "Season", "season": 2}
    ]


@pytest.mark.parametrize("item_type", ["Movie", "Season"])
def test_auto_moviepilot_failure_is_discarded_after_later_source_succeeds(
    monkeypatch,
    item_type: str,
) -> None:
    def setting(key: str):
        if key == "subscription_strategy_config":
            return {"subscription_sources": ["mp", "shared_pool"]}
        if key == "mp_config":
            return {"movie_search_window_days": 0, "movie_pause_days": 0}
        return {}

    wanted = {
        "tmdb_id": "101" if item_type == "Movie" else "season-101-2",
        "item_type": item_type,
        "title": "Fixture",
        "subscription_status": "WANTED",
        "subscription_sources_json": [],
    }
    if item_type == "Season":
        wanted.update({"parent_series_tmdb_id": "101", "season_number": 2})
    updates: list[tuple[object, str]] = []
    monkeypatch.setattr(subscriptions.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", setting)
    monkeypatch.setattr(subscriptions.settings_db, "get_subscription_quota", lambda: 1)
    monkeypatch.setattr(subscriptions.media_db, "promote_pending_to_wanted", lambda: 0)
    monkeypatch.setattr(subscriptions.media_db, "get_all_wanted_media", lambda: [wanted])
    monkeypatch.setattr(subscriptions.media_db, "get_series_title_by_tmdb_id", lambda *_args: "Fixture")
    monkeypatch.setattr(subscriptions, "is_movie_subscribable", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(subscriptions, "should_mark_as_pending", lambda *_args: (True, 99))
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: item_type == "Season",
    )
    monkeypatch.setattr(subscriptions.moviepilot, "update_subscription_status", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(subscriptions, "batch_probe_shared_resources", None)
    monkeypatch.setattr(subscriptions, "try_consume_preprobed_shared_resource", None)
    monkeypatch.setattr(subscriptions, "try_consume_shared_resource", lambda **_kwargs: {})
    monkeypatch.setattr(
        subscriptions,
        "_try_consume_shared_resource_no_gap",
        lambda *_args, **_kwargs: {
            "success": True,
            "action_type": "共享资源秒传",
            "mode": "rapid",
            "count": 1,
        },
    )
    monkeypatch.setattr(
        subscriptions.request_db,
        "set_media_status_subscribed",
        lambda tmdb_ids, item_type: updates.append((tmdb_ids, item_type)),
    )
    monkeypatch.setattr(subscriptions.user_db, "get_admin_telegram_chat_ids", lambda: [])
    monkeypatch.setattr(subscriptions.task_manager, "update_status_from_thread", lambda *_args, **_kwargs: None)

    result = subscriptions.task_auto_subscribe(SimpleNamespace(is_stop_requested=lambda: False))

    assert result["status"] == "succeeded"
    assert result["provider_failures"] == []
    assert result["processed"] == 1
    assert len(updates) == 1


def test_auto_full_series_preserves_failed_season_as_partial(monkeypatch) -> None:
    def setting(key: str):
        if key == "subscription_strategy_config":
            return {"subscription_sources": ["mp"]}
        if key == "mp_config":
            return {"movie_search_window_days": 0, "movie_pause_days": 0}
        return {}

    wanted = {
        "tmdb_id": "101",
        "item_type": "Series",
        "title": "Fixture",
        "subscription_status": "WANTED",
        "subscription_sources_json": [],
    }
    provider_results = iter((True, False))
    local_updates: list[object] = []
    monkeypatch.setattr(subscriptions.config_manager, "APP_CONFIG", {})
    monkeypatch.setattr(subscriptions.settings_db, "get_setting", setting)
    monkeypatch.setattr(subscriptions.settings_db, "get_subscription_quota", lambda: 2)
    monkeypatch.setattr(subscriptions.media_db, "promote_pending_to_wanted", lambda: 0)
    monkeypatch.setattr(subscriptions.media_db, "get_all_wanted_media", lambda: [wanted])
    monkeypatch.setattr(subscriptions.media_db, "get_series_title_by_tmdb_id", lambda *_args: "Fixture")
    monkeypatch.setattr(
        subscriptions.tmdb,
        "get_tv_details",
        lambda *_args, **_kwargs: {
            "name": "Fixture",
            "seasons": [
                {"id": 1001, "season_number": 1, "air_date": "2020-01-01", "name": "S1"},
                {"id": 1002, "season_number": 2, "air_date": "2021-01-01", "name": "S2"},
            ],
        },
    )
    monkeypatch.setattr(subscriptions, "should_mark_as_pending", lambda *_args: (False, None))
    monkeypatch.setattr(subscriptions, "check_series_completion", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(subscriptions.watchlist_db, "add_item_to_watchlist", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        subscriptions.moviepilot,
        "subscribe_with_custom_payload",
        lambda *_args, **_kwargs: next(provider_results),
    )
    monkeypatch.setattr(
        subscriptions.request_db,
        "set_media_status_subscribed",
        lambda **kwargs: local_updates.append(kwargs),
    )
    monkeypatch.setattr(subscriptions.user_db, "get_admin_telegram_chat_ids", lambda: [])
    monkeypatch.setattr(subscriptions.task_manager, "update_status_from_thread", lambda *_args, **_kwargs: None)

    result = subscriptions.task_auto_subscribe(SimpleNamespace(is_stop_requested=lambda: False))

    assert result["status"] == "partial"
    assert result["processed"] == 1
    assert result["provider_failures"] == [{"action": "create", "tmdb_id": "101", "item_type": "Season", "season": 2}]
    assert len(local_updates) == 1
