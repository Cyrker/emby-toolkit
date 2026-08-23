from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

core_processor = import_module("core_processor")


@pytest.mark.parametrize("refresh_failure", ["false", "exception"])
def test_metadata_persisted_but_refresh_failed_is_not_marked_processed(
    monkeypatch: pytest.MonkeyPatch,
    refresh_failure: str,
) -> None:
    events: list[str] = []

    class Connection:
        def cursor(self) -> object:
            return object()

        def commit(self) -> None:
            events.append("commit")

    @contextmanager
    def connection() -> Any:
        yield Connection()

    processor = cast(Any, object.__new__(core_processor.MediaProcessor))
    processor.config = {}
    processor.emby_url = "http://emby.fixture.invalid"
    processor.emby_api_key = "fixture-token"
    processor.emby_user_id = "fixture-user"
    processor.processed_items_cache = {}
    processor._is_etk_metadata_item = lambda _item: True
    processor._reconstruct_full_data_from_db = lambda _tmdb_id, _item_type: (
        {"title": "Fixture Movie", "overview": "Persisted before refresh"},
        [{"id": 1, "name": "Fixture Actor", "order": 0}],
    )
    processor._upsert_media_metadata = lambda **_kwargs: events.append("metadata.persisted")
    processor.restore_cached_mediainfo_for_emby_items = lambda *_args, **_kwargs: events.append("mediainfo.restored")
    processor._mark_item_as_processed = lambda *_args, **_kwargs: events.append("processed.marked")
    processor.log_db_manager = SimpleNamespace(
        remove_from_failed_log=lambda *_args, **_kwargs: events.append("failed.removed"),
        save_to_failed_log=lambda *_args, **_kwargs: events.append("failed.recorded"),
    )

    monkeypatch.setattr(core_processor, "get_central_db_connection", connection)
    monkeypatch.setattr(core_processor.logger, "trace", lambda *_args, **_kwargs: None, raising=False)

    def fail_refresh(**_kwargs: object) -> bool:
        events.append("refresh.started")
        if refresh_failure == "exception":
            raise RuntimeError("fixture refresh outcome unknown")
        return False

    monkeypatch.setattr(core_processor.emby, "refresh_emby_item_metadata", fail_refresh)

    result = processor._process_item_core_logic(
        {
            "Id": "movie-1",
            "Name": "Fixture Movie",
            "Type": "Movie",
            "ProviderIds": {"Tmdb": "1001"},
        }
    )

    assert result is False
    assert events == [
        "metadata.persisted",
        "commit",
        "refresh.started",
        "failed.recorded",
    ]
    assert processor.processed_items_cache == {}


def test_confirmed_durable_refresh_is_staged_before_commit_and_then_marked_processed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import task_manager
    from etk.compatibility.media_effects import (
        clear_persistent_media_effect_gateway,
        install_persistent_media_effect_gateway,
    )
    from etk.domains.media import final_refresh_effects
    from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus

    effect_id = UUID("00000000-0000-0000-0000-000000000551")
    events: list[str] = []

    class Connection:
        def cursor(self) -> object:
            return object()

        def commit(self) -> None:
            events.append("commit")

    @contextmanager
    def connection() -> Any:
        yield Connection()

    class Gateway:
        def execute(self, effect_id: UUID) -> MediaEffectProjection:
            assert effect_id == UUID("00000000-0000-0000-0000-000000000551")
            events.append("refresh.confirmed")
            return cast(MediaEffectProjection, SimpleNamespace(status=MediaEffectStatus.CONFIRMED))

    def stage(_cursor: object, **values: object) -> Any:
        assert values["operation_id"] == "job-551"
        assert values["item_id"] == "movie-1"
        events.append("refresh.staged")
        return SimpleNamespace(effect_id=effect_id)

    processor = cast(Any, object.__new__(core_processor.MediaProcessor))
    processor.config = {}
    processor.emby_url = "http://emby.fixture.invalid"
    processor.emby_api_key = "fixture-token"
    processor.emby_user_id = "fixture-user"
    processor.processed_items_cache = {}
    processor._is_etk_metadata_item = lambda _item: True
    processor._reconstruct_full_data_from_db = lambda _tmdb_id, _item_type: (
        {"title": "Fixture Movie", "overview": "Durable refresh"},
        [{"id": 1, "name": "Fixture Actor", "order": 0}],
    )
    processor._upsert_media_metadata = lambda **_kwargs: events.append("metadata.persisted")
    processor.restore_cached_mediainfo_for_emby_items = lambda *_args, **_kwargs: events.append("mediainfo.restored")
    processor._mark_item_as_processed = lambda *_args, **_kwargs: events.append("processed.marked")
    processor.log_db_manager = SimpleNamespace(
        remove_from_failed_log=lambda *_args, **_kwargs: events.append("failed.removed"),
        save_to_failed_log=lambda *_args, **_kwargs: events.append("failed.recorded"),
    )
    monkeypatch.setattr(core_processor, "get_central_db_connection", connection)
    monkeypatch.setattr(core_processor.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    monkeypatch.setattr(task_manager, "current_persistent_execution_job_id", lambda: "job-551")
    monkeypatch.setattr(final_refresh_effects, "stage_final_metadata_refresh", stage)

    def forbidden_direct_refresh(**_kwargs: object) -> bool:
        raise AssertionError("durable refresh must not use direct Emby write")

    monkeypatch.setattr(
        core_processor.emby,
        "refresh_emby_item_metadata",
        forbidden_direct_refresh,
    )
    gateway = Gateway()
    install_persistent_media_effect_gateway(gateway)
    try:
        result = processor._process_item_core_logic(
            {
                "Id": "movie-1",
                "Name": "Fixture Movie",
                "Type": "Movie",
                "ProviderIds": {"Tmdb": "1001"},
            }
        )
    finally:
        clear_persistent_media_effect_gateway(gateway)

    assert result is True
    assert events == [
        "metadata.persisted",
        "refresh.staged",
        "commit",
        "refresh.confirmed",
        "mediainfo.restored",
        "processed.marked",
        "failed.removed",
        "commit",
    ]
