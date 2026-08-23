from __future__ import annotations

from importlib import import_module

import pytest

import_module("config_manager")
media_db = import_module("database.media_db")
p115_routes = import_module("routes.p115")
monitor_service = import_module("monitor_service")


def test_destructive_legacy_consumer_rejects_unaligned_asset_arrays() -> None:
    row = {
        "asset_details_json": [{"path": "/one"}, {"path": "/two"}],
        "emby_item_ids_json": ["one", "two"],
        "file_sha1_json": ["sha-one"],
        "file_pickcode_json": ["pc-one", "pc-two"],
    }

    with pytest.raises(media_db.UnalignedAssetVersionsError, match="not position-aligned"):
        media_db._aligned_asset_arrays(row)


def test_deep_delete_job_fails_when_local_cleanup_is_not_confirmed(monkeypatch) -> None:
    maintenance_db = import_module("database.maintenance_db")
    monkeypatch.setattr(
        maintenance_db,
        "cleanup_deleted_media_item",
        lambda **kwargs: {"ok": False, "error": "fixture cleanup failed"},
    )
    snapshot = {
        "item_id": "item-1",
        "item_name": "Fixture",
        "item_type": "Movie",
        "series_id": "",
        "pickcodes": [],
        "local_only": True,
        "virtual_id": "virtual-1",
    }

    with pytest.raises(RuntimeError, match="fixture cleanup failed"):
        p115_routes._process_deep_delete_snapshot(snapshot)


def test_deep_delete_job_reports_success_only_after_confirmed_cleanup(monkeypatch) -> None:
    maintenance_db = import_module("database.maintenance_db")
    monkeypatch.setattr(
        maintenance_db,
        "cleanup_deleted_media_item",
        lambda **kwargs: {"ok": True, "partial_version_cleanup": True},
    )
    snapshot = {
        "item_id": "item-1",
        "item_name": "Fixture",
        "item_type": "Movie",
        "series_id": "",
        "pickcodes": [],
        "local_only": True,
        "virtual_id": "virtual-1",
    }

    assert p115_routes._process_deep_delete_snapshot(snapshot) == {
        "ok": True,
        "local_only": True,
        "item_id": "item-1",
    }


@pytest.mark.parametrize(
    ("repository_result", "expected"),
    [
        (
            {"updated": False, "reason": "asset_not_found"},
            {"ok": False, "skipped": False, "retryable": True, "updated": False, "reason": "asset_not_found"},
        ),
        (
            {"updated": False, "reason": "ambiguous_asset"},
            {"ok": False, "skipped": False, "retryable": False, "updated": False, "reason": "ambiguous_asset"},
        ),
        (
            {"updated": False, "tmdb_id": "100"},
            {"ok": True, "skipped": True, "reason": "already_bound", "updated": False, "tmdb_id": "100"},
        ),
    ],
)
def test_item_rebind_callback_exposes_retryable_failure_semantics(
    monkeypatch,
    repository_result,
    expected,
) -> None:
    extensions = import_module("extensions")
    task_manager = import_module("task_manager")
    webhook = import_module("routes.webhook")

    def forbidden_side_effect(*_args, **_kwargs):
        raise AssertionError("ItemID rebind must not submit ingest, TMDb, translation, or processing work")

    monkeypatch.setattr(extensions, "media_processor_instance", object())
    monkeypatch.setattr(media_db, "rebind_emby_item_by_fingerprint", lambda **kwargs: repository_result)
    monkeypatch.setattr(task_manager, "submit_task", forbidden_side_effect)
    monkeypatch.setattr(webhook, "dispatch_active_emby_items", forbidden_side_effect)
    with monitor_service.EMBY_BIND_CONDITION:
        monitor_service.EMBY_BIND_PENDING_PATHS.clear()

    result = monitor_service.accept_plugin_emby_binding(
        {"Id": "item-1", "Type": "Movie", "Path": "/library/movie.mkv", "ProviderIds": {}},
        sha1="A" * 40,
    )

    assert result == expected
