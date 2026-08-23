from __future__ import annotations

import os
from contextlib import contextmanager
from importlib import import_module
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import pytest

from etk.compatibility.media_effects import (
    clear_persistent_media_effect_gateway,
    install_persistent_media_effect_gateway,
)
from etk.domains.media import final_refresh_effects
from etk.domains.media.effects import MediaEffectProjection, MediaEffectStatus

import_module("config_manager")
core_processor = import_module("core_processor")
extensions = import_module("extensions")
metadata_provider_db = import_module("database.metadata_provider_db")
monitor_service = import_module("monitor_service")
task_manager = import_module("task_manager")
webhook = import_module("routes.webhook")


@pytest.mark.parametrize("source_kind", ["strm", "physical"])
def test_offline_ingestion_sample_reaches_confirmed_final_refresh_once(
    monkeypatch: pytest.MonkeyPatch,
    source_kind: str,
) -> None:
    suffix = ".strm" if source_kind == "strm" else ".mkv"
    path = f"/fixture/{source_kind}/movie{suffix}"
    item_id = f"movie-{source_kind}"
    effect_id = UUID("00000000-0000-0000-0000-000000000561")
    events: list[str] = []

    class Connection:
        def cursor(self) -> object:
            return object()

        def commit(self) -> None:
            events.append("metadata.commit")

    @contextmanager
    def connection() -> Any:
        yield Connection()

    class Gateway:
        def execute(self, effect_id: UUID) -> MediaEffectProjection:
            assert effect_id == UUID("00000000-0000-0000-0000-000000000561")
            events.append("final-refresh.confirmed")
            return cast(MediaEffectProjection, SimpleNamespace(status=MediaEffectStatus.CONFIRMED))

    processor = cast(Any, object.__new__(core_processor.MediaProcessor))
    processor.config = {"etk_server_url": "http://etk.fixture.invalid"}
    processor.emby_url = "http://emby.fixture.invalid"
    processor.emby_api_key = "fixture-token"
    processor.emby_user_id = "fixture-user"
    processor.tmdb_api_key = "fixture-tmdb-key"
    processor.ai_translator = None
    processor.processed_items_cache = {}
    processor._extract_season_episode_from_path = lambda _path: (None, None)
    processor._is_etk_metadata_item = lambda _item: True
    processor._reconstruct_full_data_from_db = lambda _tmdb_id, _item_type: (
        {"title": f"Fixture {source_kind}", "overview": "Offline Stub metadata"},
        [{"id": 1, "name": "Fixture Actor", "order": 0}],
    )
    processor._upsert_media_metadata = lambda **_kwargs: events.append("metadata.persisted")
    processor.restore_cached_mediainfo_for_emby_items = lambda *_args, **_kwargs: events.append("mediainfo.restored")
    processor._mark_item_as_processed = lambda *_args, **_kwargs: events.append("processed.marked")
    processor.log_db_manager = SimpleNamespace(
        remove_from_failed_log=lambda *_args, **_kwargs: events.append("failed.removed"),
        save_to_failed_log=lambda *_args, **_kwargs: events.append("failed.recorded"),
    )

    def identify(_path: str, _processor: object) -> tuple[str, str, str]:
        events.append("identity.resolved")
        return "1001", "movie", f"Fixture {source_kind}"

    def persist_snapshot(*_args: object, **_kwargs: object) -> bool:
        events.append("metadata.snapshot-persisted")
        return True

    def register_path(*_args: object, **_kwargs: object) -> bool:
        events.append("physical-path.registered")
        return True

    def generate_mediainfo(_path: str) -> dict[str, object]:
        events.append("physical-mediainfo.probed")
        return {"MediaSourceInfo": {"MediaStreams": [{"Type": "Video"}]}}

    def enqueue(_path: str, mediainfo: object = None) -> None:
        events.append("binding.enqueued")
        if source_kind == "physical":
            assert mediainfo is not None

    def stage(_cursor: object, **values: object) -> Any:
        assert values["operation_id"] == f"job-{source_kind}"
        assert values["item_id"] == item_id
        events.append("final-refresh.staged")
        return SimpleNamespace(effect_id=effect_id)

    def dispatch(items: list[dict[str, object]]) -> None:
        assert [item["Id"] for item in items] == [item_id]
        events.append("itemid.bound")
        assert processor._process_item_core_logic(items[0]) is True

    def forbidden_direct_refresh(**_kwargs: object) -> bool:
        raise AssertionError("Greenfield Worker flow must use the durable final-refresh effect")

    monkeypatch.setattr(extensions, "media_processor_instance", processor)
    monkeypatch.setattr(monitor_service, "_identify_physical_media", identify)
    monkeypatch.setattr(monitor_service, "_ensure_metadata_snapshot", persist_snapshot)
    monkeypatch.setattr(monitor_service, "_generate_local_mediainfo", generate_mediainfo)
    monkeypatch.setattr(monitor_service, "enqueue_emby_binding", enqueue)
    monkeypatch.setattr(metadata_provider_db, "register_metadata_asset_path", register_path)
    monkeypatch.setattr(
        monitor_service.emby,
        "configure_etk_plugin_origin",
        lambda *_args, **_kwargs: events.append("plugin-origin.configured") or True,
    )
    monkeypatch.setattr(webhook, "dispatch_active_emby_items", dispatch)
    monkeypatch.setattr(core_processor, "get_central_db_connection", connection)
    monkeypatch.setattr(core_processor.logger, "trace", lambda *_args, **_kwargs: None, raising=False)
    monkeypatch.setattr(task_manager, "current_persistent_execution_job_id", lambda: f"job-{source_kind}")
    monkeypatch.setattr(final_refresh_effects, "stage_final_metadata_refresh", stage)
    monkeypatch.setattr(core_processor.emby, "refresh_emby_item_metadata", forbidden_direct_refresh)

    with monitor_service.EMBY_BIND_CONDITION:
        monitor_service.EMBY_BIND_QUEUE.clear()
        monitor_service.EMBY_BIND_PENDING_PATHS.clear()
        monitor_service.EMBY_BIND_PLUGIN_COMPLETED.clear()
        monitor_service.EMBY_BIND_RETRY_COUNTS.clear()
        monitor_service.EMBY_BIND_MEDIAINFO.clear()
        monitor_service.EMBY_BIND_PENDING_PATHS.add(path)
    with monitor_service.MEDIA_PREP_LOCK:
        monitor_service.MEDIA_PREP_INFLIGHT.add(os.path.normpath(path))
        monitor_service.MEDIA_PREP_RECENT.clear()

    gateway = Gateway()
    install_persistent_media_effect_gateway(gateway)
    try:
        if source_kind == "physical":
            assert monitor_service.prepare_physical_files_for_binding(processor, [path]) == [path]
        else:
            monitor_service._prepare_strm_for_binding(processor, path)

        item = {
            "Id": item_id,
            "Name": f"Fixture {source_kind}",
            "Type": "Movie",
            "Path": path,
            "ProviderIds": {"Tmdb": "1001"},
        }
        first = monitor_service.accept_plugin_emby_binding(item, sha1="A" * 40)
        with monitor_service.EMBY_BIND_CONDITION:
            monitor_service.EMBY_BIND_PENDING_PATHS.add(path)
        replay = monitor_service.accept_plugin_emby_binding(item, sha1="A" * 40)
    finally:
        clear_persistent_media_effect_gateway(gateway)
        with monitor_service.EMBY_BIND_CONDITION:
            monitor_service.EMBY_BIND_QUEUE.clear()
            monitor_service.EMBY_BIND_PENDING_PATHS.clear()
            monitor_service.EMBY_BIND_PLUGIN_COMPLETED.clear()
            monitor_service.EMBY_BIND_RETRY_COUNTS.clear()
            monitor_service.EMBY_BIND_MEDIAINFO.clear()
        with monitor_service.MEDIA_PREP_LOCK:
            monitor_service.MEDIA_PREP_INFLIGHT.clear()
            monitor_service.MEDIA_PREP_RECENT.clear()

    assert first == {"ok": True, "accepted": True, "item_id": item_id}
    assert replay == {"ok": True, "skipped": True, "reason": "already_completed"}
    assert events.count("identity.resolved") == 1
    assert events.count("metadata.snapshot-persisted") == 1
    assert events.count("itemid.bound") == 1
    assert events.count("metadata.persisted") == 1
    assert events.count("final-refresh.staged") == 1
    assert events.count("final-refresh.confirmed") == 1
    assert events.count("processed.marked") == 1
    assert events.index("metadata.persisted") < events.index("final-refresh.staged")
    assert events.index("final-refresh.staged") < events.index("metadata.commit")
    assert events.index("metadata.commit") < events.index("final-refresh.confirmed")
    assert events.index("final-refresh.confirmed") < events.index("processed.marked")
    if source_kind == "physical":
        assert events.count("plugin-origin.configured") == 1
        assert events.count("physical-path.registered") == 1
        assert events.count("physical-mediainfo.probed") == 1
    else:
        assert "physical-path.registered" not in events
        assert "physical-mediainfo.probed" not in events
