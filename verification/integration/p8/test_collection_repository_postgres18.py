from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from etk.domains.collection.repository import (
    CollectionNameConflictError,
    SqlAlchemyCollectionRepository,
)
from etk.domains.identity.collection_projection import SqlAlchemyCollectionUserProjection
from etk.domains.media.collection_projection import SqlAlchemyCollectionMediaProjection
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


def _repository(runtime: DatabaseRuntime) -> SqlAlchemyCollectionRepository:
    return SqlAlchemyCollectionRepository(runtime.unit_of_work)


def _user_projection(runtime: DatabaseRuntime) -> SqlAlchemyCollectionUserProjection:
    return SqlAlchemyCollectionUserProjection(runtime.unit_of_work)


def _media_projection(runtime: DatabaseRuntime) -> SqlAlchemyCollectionMediaProjection:
    return SqlAlchemyCollectionMediaProjection(runtime.unit_of_work)


def test_postgres18_collection_crud_order_and_unique_rollback(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    created = repository.create_custom(
        name="Fixture Collection",
        collection_type="filter",
        definition={"media_type": "Movie"},
        allowed_user_ids=["user-a"],
    )

    assert created["id"] == 1
    assert created["definition_json"] == {"media_type": "Movie"}
    assert created["allowed_user_ids"] == ["user-a"]
    with pytest.raises(CollectionNameConflictError):
        repository.create_custom(
            name="Fixture Collection",
            collection_type="list",
            definition={"source": "fixture"},
            allowed_user_ids=None,
        )

    second = repository.create_custom(
        name="Second Collection",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=None,
    )
    with pytest.raises(CollectionNameConflictError):
        repository.update_custom(
            int(second["id"]),
            name="Fixture Collection",
            collection_type="list",
            definition={"source": "conflict"},
            status="active",
            allowed_user_ids=[],
        )
    assert repository.get_custom(int(second["id"]))["name"] == "Second Collection"  # type: ignore[index]
    repository.update_custom_order([second["id"], created["id"]])
    assert [value["id"] for value in repository.list_custom()] == [second["id"], created["id"]]

    updated = repository.update_custom(
        int(created["id"]),
        name="Updated Collection",
        collection_type="filter",
        definition={"media_type": "Series"},
        status="disabled",
        allowed_user_ids=["user-b"],
    )
    assert updated is not None
    assert (updated["name"], updated["status"], updated["allowed_user_ids"]) == (
        "Updated Collection",
        "disabled",
        ["user-b"],
    )
    assert repository.delete_custom(int(second["id"])) is not None
    assert repository.get_custom(int(second["id"])) is None


def test_custom_order_reverse_concurrency_is_deadlock_free_and_invalid_input_rolls_back(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    ids = [
        int(
            repository.create_custom(
                name=f"Order {index}",
                collection_type="list",
                definition={"index": index},
                allowed_user_ids=None,
            )["id"]
        )
        for index in range(3)
    ]
    barrier = Barrier(2)

    def update(order: list[int]) -> None:
        barrier.wait(timeout=5)
        _repository(p8_runtime).update_custom_order(order)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(update, ids),
            executor.submit(update, list(reversed(ids))),
        ]
        for future in futures:
            future.result(timeout=10)

    observed = [int(row["id"]) for row in repository.list_custom()]
    assert observed in (ids, list(reversed(ids)))
    with pytest.raises(ValueError):
        repository.update_custom_order([ids[1], "not-an-id", ids[0]])
    assert [int(row["id"]) for row in repository.list_custom()] == observed


def test_allowed_user_ids_preserve_null_and_empty_array(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    null_value = repository.create_custom(
        name="Null Users",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=None,
    )
    empty_value = repository.create_custom(
        name="Empty Users",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=[],
    )

    assert null_value["allowed_user_ids"] is None
    assert empty_value["allowed_user_ids"] == []


def test_template_expansion_and_collection_user_options_use_one_snapshot(p8_runtime: DatabaseRuntime) -> None:
    projection = _user_projection(p8_runtime)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO emby_users (id, name, is_administrator)
                VALUES
                    ('source', 'Source User', FALSE),
                    ('child', 'Child User', FALSE),
                    ('plain', 'Plain User', FALSE)
                """
            )
        )
        template_id = connection.scalar(
            text(
                """
                INSERT INTO user_templates (
                    name, emby_policy_json, source_emby_user_id
                ) VALUES (
                    'Fixture Template', '{}'::jsonb, 'source'
                )
                RETURNING id
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended (emby_user_id, template_id)
                VALUES ('child', :template_id)
                """
            ),
            {"template_id": template_id},
        )

    assert projection.expand_template_users(["source"]) == ["child", "source"]
    assert projection.collection_user_options() == [
        {"label": "Plain User", "value": "plain", "is_template_source": False},
        {"label": "Source User", "value": "source", "is_template_source": True},
    ]


def test_custom_status_projects_movie_and_season_item_ids_from_jsonb(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    collection = repository.create_custom(
        name="Status Collection",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET generated_media_info_json = CAST(:items AS jsonb)
                WHERE id = :collection_id
                """
            ),
            {
                "collection_id": collection["id"],
                "items": """
                    [
                      {"tmdb_id":"11","media_type":"Movie","title":"Fixture Movie"},
                      {"tmdb_id":"22","media_type":"Series","season":2,"title":"Fixture Series"},
                      {"tmdb_id":"33","media_type":"Movie","title":"Missing Movie"}
                    ]
                """,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    title, release_date
                ) VALUES (
                    '11', 'Movie', TRUE, '["emby-movie"]'::jsonb,
                    'Fixture Movie', :release_date
                )
                """
            ),
            {"release_date": date(2026, 8, 1)},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, in_library, emby_item_ids_json,
                    parent_series_tmdb_id, season_number, title
                ) VALUES (
                    '22-season-2', 'Season', FALSE, '["emby-season"]'::jsonb,
                    '22', 2, 'Fixture Series Season 2'
                )
                """
            )
        )

    refreshed = repository.get_custom(int(collection["id"]))
    assert refreshed is not None
    status = _media_projection(p8_runtime).custom_status(refreshed)

    assert status["missing_count"] == 2
    assert status["health_status"] == "has_missing"
    assert [(item["emby_id"], item["status"]) for item in status["media_items"]] == [
        (None, "in_library"),
        (None, "missing"),
        (None, "missing"),
    ]


def test_native_collection_upsert_activation_and_deactivation(p8_runtime: DatabaseRuntime) -> None:
    repository = _repository(p8_runtime)
    inactive = repository.upsert_native(
        {
            "id": 101,
            "name": "Fixture Saga",
            "overview": "Fixture overview",
            "poster_path": "/poster.jpg",
            "backdrop_path": "/backdrop.jpg",
            "parts": [{"id": 11}, {"id": 12}],
        }
    )
    assert inactive["is_active"] is False
    assert repository.list_native() == []

    active = repository.upsert_native(
        {
            "id": 101,
            "name": "Fixture Saga",
            "overview": "Updated overview",
            "poster_path": "/poster-2.jpg",
            "parts": [{"id": 11}],
        },
        existing_emby_id="emby-boxset",
    )
    assert active["is_active"] is True
    assert active["all_tmdb_ids_json"] == ["11"]
    assert len(repository.list_native()) == 1
    assert repository.deactivate_native("emby-boxset") is True
    assert repository.list_native() == []
    assert repository.get_native("101")["name"] == "Fixture Saga"  # type: ignore[index]


def test_native_collection_unique_emby_activation_race_rolls_back_loser(p8_runtime: DatabaseRuntime) -> None:
    barrier = Barrier(2)

    def activate(tmdb_id: int) -> dict[str, object]:
        barrier.wait(timeout=5)
        return _repository(p8_runtime).upsert_native(
            {
                "id": tmdb_id,
                "name": f"Race {tmdb_id}",
                "parts": [{"id": tmdb_id * 10}],
            },
            existing_emby_id="shared-emby-boxset",
        )

    outcomes: list[dict[str, object] | BaseException] = []
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(activate, 201), executor.submit(activate, 202)]
        for future in futures:
            try:
                outcomes.append(future.result(timeout=10))
            except BaseException as exc:
                outcomes.append(exc)

    assert sum(isinstance(value, dict) for value in outcomes) == 1
    assert sum(isinstance(value, IntegrityError) for value in outcomes) == 1
    active = _repository(p8_runtime).list_native()
    assert len(active) == 1
    assert active[0]["emby_collection_id"] == "shared-emby-boxset"


def test_all_p8_collection_settings_round_trip_as_jsonb(p8_runtime: DatabaseRuntime) -> None:
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    values = {
        "auto_tagging_rules": [{"name": "fixture"}],
        "country_mapping": [{"label": "中国大陆", "value": "CN"}],
        "cover_generator_config": {"enabled": True, "cover_style": "single_1"},
        "junk_file_patterns": ["sample"],
        "keyword_mapping": [{"label": "科幻", "ids": [1]}],
        "language_mapping": [{"label": "国语", "value": "zh"}],
        "native_collections_config": {"auto_sub_enabled": False},
        "rating_mapping": {"US": [{"code": "G", "label": "全年龄"}]},
        "rating_priority": ["ORIGIN", "US"],
        "recommendation_pool": [{"tmdb_id": 1}],
        "recommendation_pool_page": 3,
        "recommendation_theme_index": 2,
        "release_group_mapping": [{"label": "Fixture", "aliases": ["FIX"]}],
        "studio_mapping": [{"label": "Fixture Studio", "company_ids": [1]}],
        "unified_ratings": [{"label": "全年龄"}],
    }

    settings.save_many(values)

    assert {key: settings.load(key) for key in values} == values


def test_settings_mapping_merge_is_atomic_under_concurrency(p8_runtime: DatabaseRuntime) -> None:
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    settings.save(
        "native_collections_config",
        {"preserved": True, "auto_complete_enabled": True},
    )
    barrier = Barrier(2)

    def merge(key: str, value: object) -> None:
        barrier.wait(timeout=5)
        SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work).merge_mapping(
            "native_collections_config",
            {key: value},
            remove_keys=("auto_complete_enabled",),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(merge, "auto_sub_enabled", False),
            executor.submit(merge, "batch_size", 25),
        ]
        for future in futures:
            future.result(timeout=10)

    assert settings.load("native_collections_config") == {
        "preserved": True,
        "auto_sub_enabled": False,
        "batch_size": 25,
    }
