from __future__ import annotations

from typing import Any, cast

import pytest

import config_manager  # noqa: F401  # Initialize legacy database modules in production order.
from services.subscribe_assistant import manager as assistant_manager
from services.subscribe_assistant.config import AssistantConfig
from services.subscribe_assistant.engine import CompletionSignal


def _manager() -> assistant_manager.SubscribeAssistantManager:
    value = assistant_manager.SubscribeAssistantManager.__new__(assistant_manager.SubscribeAssistantManager)
    value.cfg = AssistantConfig(
        subscription_cleanup_history_type="current",
        subscription_cleanup_history_scenes=["completed"],
        best_version_type="completed_full",
        best_version_full_consistency_check_enabled=False,
        best_version_episode_to_full=False,
    )
    value.app_config = {}
    cast(Any, value)._series_title = lambda _tmdb_id, _info=None: "Fixture Series"
    return value


def test_persistent_subscription_cleanup_runs_inline_and_propagates_failure(monkeypatch) -> None:
    manager = _manager()
    monkeypatch.setattr(assistant_manager, "_has_persistent_execution", lambda: True)
    monkeypatch.setattr(assistant_manager, "_cleanup_mp_episode_residue_persistent", lambda *_args: False)
    monkeypatch.setattr(
        assistant_manager,
        "spawn",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent cleanup must not detach")),
    )

    with pytest.raises(RuntimeError, match="MoviePilot episode residue cleanup failed"):
        manager._trigger_subscription_cleanup_on_complete(
            "100",
            1,
            {"best_version_full": 1, "name": "Fixture Series"},
        )


def test_existing_full_washing_state_retries_persistent_cleanup(monkeypatch) -> None:
    manager = _manager()
    cleanup_calls = []
    cast(Any, manager)._set_season_active_washing = lambda *_args, **_kwargs: None
    cast(Any, manager)._mark_full_washing_started = lambda *_args, **_kwargs: None
    cast(Any, manager)._trigger_subscription_cleanup_on_complete = lambda *args: cleanup_calls.append(args)

    manager._sync_completed_full_washing(
        tmdb_id="100",
        series_name="Fixture Series",
        season=1,
        subscribe={"id": 7, "best_version_full": 1, "name": "Fixture Series"},
        season_info={"episode_count": 12},
        signal=CompletionSignal(scope_total=12),
    )

    assert cleanup_calls == [("100", 1, {"id": 7, "best_version_full": 1, "name": "Fixture Series"})]


def test_legacy_subscription_cleanup_keeps_the_compatibility_dispatcher(monkeypatch) -> None:
    manager = _manager()
    calls = []
    monkeypatch.setattr(assistant_manager, "_has_persistent_execution", lambda: False)
    monkeypatch.setattr(assistant_manager, "spawn", lambda *args: calls.append(args))

    manager._trigger_subscription_cleanup_on_complete(
        "100",
        1,
        {"best_version_full": 1, "name": "Fixture Series"},
    )

    assert calls[0][0] is assistant_manager._cleanup_mp_episode_residue_persistent
