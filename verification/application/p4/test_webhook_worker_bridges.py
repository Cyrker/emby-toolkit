from __future__ import annotations

from types import SimpleNamespace

import pytest
from flask import Flask


def _episode_ref():
    from handler.intro_detection_service import EpisodeRef

    return EpisodeRef(
        item_id="episode-1",
        series_id="series-1",
        season_id="season-1",
        series_tmdb_id="100",
        title="Episode 1",
        series_title="Series",
        season_number=1,
        episode_number=1,
        sha1="A" * 40,
        pick_code="pick-1",
    )


def test_intro_backfill_api_submits_json_safe_operation_without_starting_daemon(monkeypatch) -> None:
    from handler import intro_detection_service
    from routes import webhook

    submitted = {}
    monkeypatch.setattr(webhook, "persistent_background_gateway_installed", lambda: True, raising=False)
    monkeypatch.setattr(
        intro_detection_service,
        "enqueue_active_backfill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not run intro backfill")),
    )

    def submit(operation, **options):
        submitted.update({"operation": operation, **options})
        return SimpleNamespace(job_id="intro-job-1")

    monkeypatch.setattr(webhook, "submit_persistent_background", submit)
    app = Flask(__name__)
    app.config["ETK_COMPATIBILITY_HOST"] = True

    with app.test_request_context("/api/emby/intro-detection/backfill", method="POST"):
        response, status = webhook.trigger_emby_intro_detection_backfill()

    assert status == 202
    assert response.get_json() == {"ok": True, "submitted": True, "job_id": "intro-job-1"}
    assert submitted["operation"] is intro_detection_service.run_active_backfill_persistent
    assert submitted["args"] == (50,)
    assert submitted["kwargs"] == {"force": False}


def test_intro_backfill_api_submit_failure_does_not_fallback_to_daemon(monkeypatch) -> None:
    from handler import intro_detection_service
    from routes import webhook

    monkeypatch.setattr(webhook, "persistent_background_gateway_installed", lambda: True)
    monkeypatch.setattr(
        webhook,
        "submit_persistent_background",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("gateway unavailable")),
    )
    monkeypatch.setattr(
        intro_detection_service,
        "enqueue_active_backfill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("API must not fall back to daemon")),
    )
    app = Flask(__name__)
    app.config["ETK_COMPATIBILITY_HOST"] = True

    with app.test_request_context("/api/emby/intro-detection/backfill", method="POST"):
        response, status = webhook.trigger_emby_intro_detection_backfill()

    assert status == 503
    assert response.get_json() == {
        "ok": False,
        "submitted": False,
        "reason": "persistent_submit_failed",
    }


def test_intro_persistent_backfill_processes_each_season_synchronously(monkeypatch) -> None:
    import task_manager
    from handler import intro_detection_service

    processed = []
    monkeypatch.setattr(task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        intro_detection_service,
        "_start_worker",
        lambda: (_ for _ in ()).throw(AssertionError("persistent execution must not start a daemon")),
    )
    monkeypatch.setattr(
        intro_detection_service, "get_trigger_mode", lambda: intro_detection_service.INTRO_TRIGGER_MODE_IMPORT
    )
    monkeypatch.setattr(intro_detection_service, "_intro_detection_library_ids", lambda: [])
    monkeypatch.setattr(intro_detection_service, "_load_library_episode_refs", lambda **_kwargs: [_episode_ref()])
    monkeypatch.setattr(intro_detection_service, "_claim_recent", lambda _key: True)
    monkeypatch.setattr(intro_detection_service, "_process_job", lambda payload: processed.append(payload))
    monkeypatch.setattr(
        intro_detection_service._queue,
        "put_nowait",
        lambda _payload: (_ for _ in ()).throw(AssertionError("persistent execution must not use module queue")),
    )

    result = intro_detection_service.enqueue_active_backfill(limit=10, force=True)

    assert result["ok"] is True
    assert result["count"] == 1
    assert [payload["episode"]["item_id"] for payload in processed] == ["episode-1"]


def test_intro_persistent_backfill_and_catalog_task_propagate_failure(monkeypatch) -> None:
    import task_manager
    from handler import intro_detection_service
    from tasks import watchlist

    monkeypatch.setattr(task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        intro_detection_service, "get_trigger_mode", lambda: intro_detection_service.INTRO_TRIGGER_MODE_IMPORT
    )
    monkeypatch.setattr(intro_detection_service, "_intro_detection_library_ids", lambda: [])
    monkeypatch.setattr(intro_detection_service, "_load_library_episode_refs", lambda **_kwargs: [_episode_ref()])
    monkeypatch.setattr(intro_detection_service, "_claim_recent", lambda _key: True)
    monkeypatch.setattr(
        intro_detection_service,
        "_process_job",
        lambda _payload: (_ for _ in ()).throw(RuntimeError("intro processing failed")),
    )

    with pytest.raises(RuntimeError, match="intro processing failed"):
        intro_detection_service.enqueue_active_backfill(limit=10, force=True)

    monkeypatch.setattr(
        intro_detection_service,
        "enqueue_active_backfill",
        lambda **_kwargs: {"ok": False, "error": "intro catalog failed"},
    )
    monkeypatch.setattr(watchlist.task_manager, "update_status_from_thread", lambda *_args: None)

    with pytest.raises(RuntimeError, match="intro catalog failed"):
        watchlist.task_intro_fingerprint_backfill(object())


@pytest.mark.parametrize(
    "batch_result",
    [
        {"ok": False, "created": 0, "failed": 1, "raw_batch_result": {"ok": True, "errors": []}},
        {"ok": True, "created": 2, "failed": 0, "raw_batch_result": {"ok": False, "errors": ["raw failed"]}},
    ],
)
def test_shared_auto_share_persistent_execution_propagates_center_and_raw_failures(
    monkeypatch,
    batch_result,
) -> None:
    from routes import webhook
    from tasks import shared_resource_tasks

    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(webhook, "_shared_resource_auto_share_enabled", lambda: True)
    monkeypatch.setattr(
        webhook,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent execution must not spawn")),
    )
    monkeypatch.setattr(
        shared_resource_tasks,
        "trigger_shared_rapid_register_batch_for_library_items",
        lambda *_args, **_kwargs: batch_result,
    )

    with pytest.raises(RuntimeError, match="共享源登记"):
        webhook._run_shared_auto_share_batch_detached(
            "fixture share",
            [{"emby_item_id": "one"}, {"emby_item_id": "two"}],
        )


def test_shared_auto_share_parent_propagates_failure_but_legacy_keeps_async_fallback(monkeypatch) -> None:
    from routes import webhook

    original = webhook._run_shared_auto_share_batch_detached
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        webhook,
        "_run_shared_auto_share_batch_detached",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("shared registration failed")),
    )
    with pytest.raises(RuntimeError, match="shared registration failed"):
        webhook._submit_shared_auto_share_after_library_ready(
            {"Name": "Movie"},
            "item-1",
            "Movie",
            "100",
        )

    scheduled = []
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: False)
    monkeypatch.setattr(webhook, "_shared_resource_auto_share_enabled", lambda: True)
    monkeypatch.setattr(webhook, "spawn", lambda function: scheduled.append(function) or object())
    monkeypatch.setattr(
        webhook,
        "_run_shared_auto_share_batch_detached",
        original,
    )

    webhook._run_shared_auto_share_batch_detached("legacy share", [{"emby_item_id": "item-1"}])

    assert len(scheduled) == 1


def test_watchlist_refresh_persistent_execution_submits_inline_and_checks_acceptance(monkeypatch) -> None:
    from routes import webhook

    submitted = []
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(webhook.watchlist_db, "get_watching_tmdb_ids", lambda: {"100"})
    monkeypatch.setattr(
        webhook,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent execution must not spawn")),
    )
    monkeypatch.setattr(
        webhook.task_manager,
        "submit_task",
        lambda *args, **kwargs: submitted.append((args, kwargs)) or True,
    )

    assert (
        webhook._schedule_watchlist_refresh_after_media(
            item_name="Series",
            tmdb_id="100",
            precise_new_episode_ids=["episode-1"],
            triggering_episode_ids=["episode-1"],
        )
        is True
    )
    assert submitted[0][0][0] is webhook.task_process_watchlist
    assert submitted[0][1]["tmdb_id"] == "100"

    monkeypatch.setattr(webhook.task_manager, "submit_task", lambda *_args, **_kwargs: False)
    with pytest.raises(RuntimeError, match="watchlist Job was not accepted"):
        webhook._schedule_watchlist_refresh_after_media(
            item_name="Series",
            tmdb_id="100",
            precise_new_episode_ids=["episode-1"],
            triggering_episode_ids=["episode-1"],
        )


def test_watchlist_refresh_legacy_mode_keeps_async_fallback(monkeypatch) -> None:
    from routes import webhook

    scheduled = []
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: False)
    monkeypatch.setattr(webhook, "spawn", lambda function: scheduled.append(function) or object())
    monkeypatch.setattr(
        webhook.task_manager,
        "submit_task",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("legacy submit runs inside spawned callback")),
    )

    webhook._schedule_watchlist_refresh_after_media(
        item_name="Series",
        tmdb_id="100",
        precise_new_episode_ids=["episode-1"],
        triggering_episode_ids=["episode-1"],
    )

    assert len(scheduled) == 1


def test_watchlist_task_top_level_failure_propagates_only_in_persistent_execution(monkeypatch) -> None:
    from tasks import watchlist

    processor = SimpleNamespace(
        run_regular_processing_task_concurrent=lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("watchlist processing failed")
        )
    )
    progress_updates = []
    monkeypatch.setattr(
        watchlist.task_manager,
        "update_status_from_thread",
        lambda progress, message: progress_updates.append((progress, message)),
    )
    monkeypatch.setattr(watchlist.task_manager, "has_persistent_execution", lambda: True)

    with pytest.raises(RuntimeError, match="watchlist processing failed"):
        watchlist.task_process_watchlist(processor, tmdb_id="100")

    monkeypatch.setattr(watchlist.task_manager, "has_persistent_execution", lambda: False)
    watchlist.task_process_watchlist(processor, tmdb_id="100")

    assert progress_updates == [
        (-1, "启动任务时发生错误: watchlist processing failed"),
        (-1, "启动任务时发生错误: watchlist processing failed"),
    ]


def test_active_series_notification_is_synchronous_and_failure_propagates_in_persistent_execution(monkeypatch) -> None:
    from routes import webhook

    details = {"Id": "series-1", "Name": "Series", "ProviderIds": {"Tmdb": "100"}}
    sent = []
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: True)
    monkeypatch.setattr(
        webhook,
        "spawn_later",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent execution must not use timer")),
    )
    monkeypatch.setattr(
        webhook.telegram,
        "send_media_notification",
        lambda **kwargs: sent.append(kwargs),
    )

    webhook._enqueue_active_series_notification(details, "update", ["episode-1"])

    assert sent == [
        {
            "item_details": details,
            "notification_type": "update",
            "new_episode_ids": ["episode-1"],
        }
    ]

    monkeypatch.setattr(
        webhook.telegram,
        "send_media_notification",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("telegram failed")),
    )
    with pytest.raises(RuntimeError, match="telegram failed"):
        webhook._send_media_notification_after_processing(
            item_details=details,
            item_type="Series",
            item_id="series-1",
            precise_new_episode_ids=["episode-1"],
            is_new_item=False,
            aggregate_notification=True,
        )


def test_active_series_notification_legacy_mode_keeps_process_local_debounce(monkeypatch) -> None:
    from routes import webhook

    details = {"Id": "series-legacy", "Name": "Legacy", "ProviderIds": {"Tmdb": "101"}}
    scheduled = []
    webhook.ACTIVE_SERIES_NOTIFICATION_QUEUE.clear()
    webhook.ACTIVE_SERIES_NOTIFICATION_TIMERS.clear()
    monkeypatch.setattr(webhook.task_manager, "has_persistent_execution", lambda: False)
    monkeypatch.setattr(
        webhook.telegram,
        "send_media_notification",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("legacy notification must wait for debounce")),
    )
    monkeypatch.setattr(
        webhook,
        "spawn_later",
        lambda delay, function, key: scheduled.append((delay, function, key)) or SimpleNamespace(kill=lambda: None),
    )

    webhook._enqueue_active_series_notification(details, "new", ["episode-1"])

    assert scheduled == [
        (
            webhook.ACTIVE_SERIES_NOTIFICATION_DEBOUNCE_TIME,
            webhook._flush_active_series_notification,
            "series-legacy",
        )
    ]
    webhook.ACTIVE_SERIES_NOTIFICATION_QUEUE.clear()
    webhook.ACTIVE_SERIES_NOTIFICATION_TIMERS.clear()


def test_media_notification_delivery_retries_only_failed_recipients(monkeypatch) -> None:
    from handler import telegram

    states = {}
    token_counter = iter(("token-1", "token-2", "token-3"))

    def claim(_namespace, key, *, lease_seconds):
        assert lease_seconds == telegram._TELEGRAM_MEDIA_DELIVERY_LEASE_SECONDS
        if states.get(key) == "delivered":
            return None
        token = next(token_counter)
        states[key] = token
        return SimpleNamespace(owner_token=token)

    def complete(_namespace, key, token, *, result):
        assert states.get(key) == token
        assert result == {"transport": "message"}
        states[key] = "delivered"
        return True

    def release(_namespace, key, token):
        assert states.get(key) == token
        states.pop(key)
        return True

    monkeypatch.setattr(telegram.persistent_runtime_state, "claim_delivery", claim)
    monkeypatch.setattr(telegram.persistent_runtime_state, "complete_delivery", complete)
    monkeypatch.setattr(telegram.persistent_runtime_state, "release_delivery", release)
    sends = []
    accepted = iter((True, False, True))
    monkeypatch.setattr(
        telegram,
        "send_telegram_message",
        lambda chat_id, _caption: sends.append(chat_id) or next(accepted),
    )
    targets = [
        {"chat_id": "channel", "role": "channel", "caption": "caption"},
        {"chat_id": "subscriber", "role": "subscriber", "caption": "personal"},
    ]

    with pytest.raises(RuntimeError, match="1 recipient"):
        telegram._deliver_media_notification_targets(targets, photo_url=None, event_key="event-1")
    retried = telegram._deliver_media_notification_targets(targets, photo_url=None, event_key="event-1")

    assert sends == ["channel", "subscriber", "subscriber"]
    assert retried == {
        "ok": True,
        "event_key": "event-1",
        "deliveries": [
            {"recipient": "channel", "role": "channel", "ok": True, "status": "deduplicated"},
            {"recipient": "subscriber", "role": "subscriber", "ok": True, "status": "sent"},
        ],
    }


def test_media_notification_event_key_is_stable_per_job_and_payload(monkeypatch) -> None:
    import task_manager
    from handler import telegram

    monkeypatch.setattr(task_manager, "current_persistent_execution_job_id", lambda: "job-1")
    details = {"Id": "series-1", "ProviderIds": {"Tmdb": "100"}}

    first = telegram._persistent_media_notification_event_key(details, "update", ["episode-2", "episode-1"])
    repeated = telegram._persistent_media_notification_event_key(details, "update", ["episode-1", "episode-2"])
    changed = telegram._persistent_media_notification_event_key(details, "wash", ["episode-1", "episode-2"])

    assert first == repeated
    assert first != changed
    assert len(first or "") == 64
