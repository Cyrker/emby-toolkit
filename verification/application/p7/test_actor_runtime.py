from __future__ import annotations

from typing import Any

import config_manager  # noqa: F401
from tasks import actors


class _Processor:
    def __init__(self, *, libraries: list[str] | None = None) -> None:
        self.config: dict[str, Any] = {"libraries_to_process": libraries or ["library-a"]}
        self.emby_url = "http://emby.fixture.invalid"
        self.emby_api_key = "fixture"
        self.emby_user_id = "fixture-user"

    def is_stop_requested(self) -> bool:
        return False

    def get_stop_event(self) -> object:
        return object()


def _install_task_hooks(monkeypatch) -> list[str]:
    effects: list[str] = []
    monkeypatch.setattr(actors.time, "sleep", lambda *_args: None)
    monkeypatch.setattr(actors.task_manager, "update_status_from_thread", lambda *_args: None)
    monkeypatch.setattr(actors.task_manager, "persistent_execution_checkpoint", lambda: effects.append("checkpoint"))
    monkeypatch.setattr(
        actors.task_manager,
        "record_persistent_effect_started",
        lambda label: effects.append(f"started:{label}"),
    )
    monkeypatch.setattr(
        actors.task_manager,
        "record_persistent_effect_committed",
        lambda label: effects.append(f"committed:{label}"),
    )
    monkeypatch.setattr(
        actors.task_manager,
        "record_persistent_effect_abandoned",
        lambda label: effects.append(f"abandoned:{label}"),
    )
    return effects


def test_purge_scans_all_item_types_and_rechecks_each_candidate_before_delete(monkeypatch) -> None:
    effects = _install_task_hooks(monkeypatch)
    media_calls: list[str | None] = []
    readbacks = [
        [{"Id": "episode-1", "Type": "Episode", "People": [{"Id": "episode-only"}]}],
        [{"Id": "video-1", "Type": "Video", "People": [{"Id": "became-referenced"}]}],
        [],
    ]

    def get_items(*_args, **kwargs):
        assert kwargs["require_complete"] is True
        assert kwargs["library_ids"] == ["movies", "tv", "mixed", "folder"]
        media_calls.append(kwargs.get("media_type_filter"))
        return readbacks.pop(0)

    monkeypatch.setattr(
        actors.emby,
        "get_emby_libraries",
        lambda *_args: [
            {"Id": "movies", "CollectionType": "movies"},
            {"Id": "tv", "CollectionType": "tvshows"},
            {"Id": "mixed", "CollectionType": "mixed"},
            {"Id": "folder", "CollectionType": "", "Type": "CollectionFolder"},
            {"Id": "ignored", "CollectionType": "music"},
        ],
    )
    monkeypatch.setattr(actors.emby, "get_emby_library_items", get_items)
    monkeypatch.setattr(
        actors.emby,
        "get_all_persons_from_emby",
        lambda **_kwargs: iter(
            [
                [
                    {"Id": "episode-only", "Name": "Episode"},
                    {"Id": "became-referenced", "Name": "Concurrent"},
                    {"Id": "ghost", "Name": "Ghost"},
                ]
            ]
        ),
    )
    deleted: list[str] = []
    monkeypatch.setattr(
        actors.emby,
        "delete_person_custom_api",
        lambda **kwargs: deleted.append(kwargs["person_id"]) is None or True,
    )

    result = actors.task_purge_ghost_actors(_Processor())

    assert media_calls == [None, None, None]
    assert deleted == ["ghost"]
    assert result["status"] == "succeeded"
    assert result["skipped_referenced"] == ["became-referenced"]
    assert effects == [
        "checkpoint",
        "started:actor-purge-delete:ghost",
        "committed:actor-purge-delete:ghost",
    ]


def test_actor_reference_scan_fails_closed_when_any_library_read_is_partial(monkeypatch) -> None:
    class Response:
        @staticmethod
        def raise_for_status() -> None:
            return None

        @staticmethod
        def json() -> dict[str, Any]:
            return {"Items": [{"Id": "movie-a", "People": [{"Id": "person-a"}]}]}

    responses: list[object] = [Response(), RuntimeError("library-b unavailable")]

    def get(*_args, **_kwargs):
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(actors.emby.emby_client, "get", get)

    result = actors.emby.get_emby_library_items(
        base_url="http://emby.fixture.invalid",
        api_key="fixture",
        user_id="fixture-user",
        library_ids=["library-a", "library-b"],
        media_type_filter=None,
        fields="People",
        require_complete=True,
    )

    assert result is None


def test_purge_unknown_delete_is_structured_non_retryable_and_restart_converges(monkeypatch) -> None:
    _install_task_hooks(monkeypatch)
    monkeypatch.setattr(
        actors.emby,
        "get_emby_libraries",
        lambda *_args: [{"Id": "movies", "CollectionType": "movies"}],
    )
    monkeypatch.setattr(actors.emby, "get_emby_library_items", lambda *_args, **_kwargs: [])
    person_batches = [iter([[{"Id": "ghost", "Name": "Ghost"}]]), iter([[]])]
    monkeypatch.setattr(actors.emby, "get_all_persons_from_emby", lambda **_kwargs: person_batches.pop(0))
    delete_calls: list[str] = []
    monkeypatch.setattr(
        actors.emby,
        "delete_person_custom_api",
        lambda **kwargs: delete_calls.append(kwargs["person_id"]) is None and False,
    )

    first = actors.task_purge_ghost_actors(_Processor())
    second = actors.task_purge_ghost_actors(_Processor())

    assert first["status"] == "failed"
    assert first["retryable"] is False
    assert first["failures"] == [{"type": "person_delete_unconfirmed", "person_id": "ghost"}]
    assert second["status"] == "succeeded"
    assert delete_calls == ["ghost"]


def test_merge_reports_media_update_failure_and_checkpoints_inner_loop(monkeypatch) -> None:
    effects = _install_task_hooks(monkeypatch)
    monkeypatch.setattr(
        actors.emby,
        "get_emby_libraries",
        lambda *_args: [{"Id": "library-a", "CollectionType": "tvshows"}],
    )
    monkeypatch.setattr(
        actors.emby,
        "get_emby_library_items",
        lambda *_args, **kwargs: (
            [
                {
                    "Id": "episode-1",
                    "Type": "Episode",
                    "People": [{"Id": "1"}, {"Id": "2"}],
                }
            ]
            if kwargs.get("media_type_filter") is None
            else None
        ),
    )
    monkeypatch.setattr(
        actors.emby,
        "get_all_persons_from_emby",
        lambda **_kwargs: iter(
            [
                [
                    {"Id": "1", "Name": "Keeper", "ProviderIds": {"Tmdb": "9"}},
                    {"Id": "2", "Name": "Deletee", "ProviderIds": {"Tmdb": "9"}},
                ]
            ]
        ),
    )
    monkeypatch.setattr(
        actors.emby,
        "get_emby_item_details",
        lambda *_args: {
            "Name": "Episode",
            "People": [{"Id": "1", "Name": "Keeper"}, {"Id": "2", "Name": "Deletee"}],
        },
    )
    monkeypatch.setattr(actors.emby, "update_emby_item_details", lambda **_kwargs: False)
    monkeypatch.setattr(
        actors.emby,
        "delete_person_custom_api",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("delete must be skipped")),
    )

    result = actors.task_merge_duplicate_actors(_Processor())

    assert result["status"] == "partial"
    assert result["failed_objects"] == [{"type": "media_update_failed", "media_id": "episode-1"}]
    assert effects == [
        "checkpoint",
        "started:actor-merge-media:episode-1",
        "abandoned:actor-merge-media:episode-1",
    ]


def test_merge_rechecks_references_before_person_delete(monkeypatch) -> None:
    effects = _install_task_hooks(monkeypatch)
    monkeypatch.setattr(
        actors.emby,
        "get_emby_libraries",
        lambda *_args: [
            {"Id": "library-a", "CollectionType": "tvshows"},
            {"Id": "library-global", "CollectionType": "homevideos"},
        ],
    )
    readbacks = [
        [{"Id": "episode-1", "People": [{"Id": "1"}, {"Id": "2"}]}],
        [{"Id": "episode-new", "People": [{"Id": "2"}]}],
    ]
    monkeypatch.setattr(
        actors.emby,
        "get_emby_library_items",
        lambda *_args, **kwargs: (
            readbacks.pop(0)
            if kwargs.get("media_type_filter") is None and kwargs.get("require_complete") is True
            else None
        ),
    )
    monkeypatch.setattr(
        actors.emby,
        "get_all_persons_from_emby",
        lambda **_kwargs: iter(
            [
                [
                    {"Id": "1", "Name": "Keeper", "ProviderIds": {"Tmdb": "9"}},
                    {"Id": "2", "Name": "Deletee", "ProviderIds": {"Tmdb": "9"}},
                ]
            ]
        ),
    )
    monkeypatch.setattr(
        actors.emby,
        "get_emby_item_details",
        lambda *_args: {
            "Name": "Episode",
            "People": [{"Id": "1", "Name": "Keeper"}, {"Id": "2", "Name": "Deletee"}],
        },
    )
    monkeypatch.setattr(actors.emby, "update_emby_item_details", lambda **_kwargs: True)
    monkeypatch.setattr(
        actors.emby,
        "delete_person_custom_api",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("referenced Person must not be deleted")),
    )

    result = actors.task_merge_duplicate_actors(_Processor())

    assert result["status"] == "partial"
    assert result["deleted_people"] == 0
    assert result["failed_objects"] == [{"type": "person_still_referenced", "person_id": "2"}]
    assert effects == [
        "checkpoint",
        "started:actor-merge-media:episode-1",
        "committed:actor-merge-media:episode-1",
    ]
