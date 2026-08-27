from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import date
from threading import Barrier

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import IntegrityError

from etk.domains.collection.repository import (
    CollectionCorrectionSnapshot,
    CollectionMatchConflictError,
    CollectionNameConflictError,
    SqlAlchemyCollectionRepository,
)
from etk.domains.identity.collection_projection import SqlAlchemyCollectionUserProjection
from etk.domains.media.collection_projection import SqlAlchemyCollectionMediaProjection
from etk.domains.media.subscription_projection import SubscriptionMediaKey
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


def test_custom_match_updates_movie_metadata_status_source_and_json_in_one_uow(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    collection = repository.create_custom(
        name="Correction Fixture",
        collection_type="list",
        definition={"source": "fixture", "corrections": {"preserved": {"tmdb_id": "1"}}},
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
                      {"tmdb_id":"101","title":"Wrong Match","media_type":"Movie"}
                    ]
                """,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, subscription_status, subscription_sources_json
                ) VALUES ('101', 'Movie', 'Old Movie', 'WANTED', '[]'::jsonb)
                """
            )
        )

    candidate = repository.inspect_custom_match(
        int(collection["id"]),
        old_tmdb_id="101",
        old_title=None,
    )
    assert candidate is not None
    corrected = repository.apply_custom_match(
        candidate,
        CollectionCorrectionSnapshot(
            collection_tmdb_id="202",
            season_number=None,
            target_key=SubscriptionMediaKey("202", "Movie"),
            target_record={
                "title": "Correct Movie",
                "original_title": "Correct Movie Original",
                "release_date": "2020-01-01",
                "release_year": 2020,
                "poster_path": "/movie.jpg",
                "backdrop_path": "/backdrop.jpg",
                "overview": "Correct overview",
                "season_number": None,
                "parent_series_tmdb_id": None,
            },
            target_status="WANTED",
            corrected_item={
                "tmdb_id": "202",
                "title": "Correct Movie",
                "media_type": "Movie",
                "status": "subscribed",
            },
        ),
    )
    assert corrected == {
        "tmdb_id": "202",
        "title": "Correct Movie",
        "media_type": "Movie",
        "status": "subscribed",
    }
    row = repository.get_custom(int(collection["id"]))
    assert row is not None
    assert row["generated_media_info_json"][0]["tmdb_id"] == "202"
    assert row["definition_json"]["corrections"] == {
        "101": {"tmdb_id": "202"},
        "preserved": {"tmdb_id": "1"},
    }
    with p8_runtime.engine.connect() as connection:
        media = (
            connection.execute(
                text(
                    """
                SELECT tmdb_id, item_type, title, original_title, release_date,
                       subscription_status, subscription_sources_json, ignore_reason
                FROM media_metadata
                WHERE (tmdb_id, item_type) IN (('101','Movie'),('202','Movie'))
                ORDER BY tmdb_id
                """
                )
            )
            .mappings()
            .all()
        )
    assert media[0]["subscription_status"] == "IGNORED"
    assert media[0]["ignore_reason"] == "修正为 202"
    assert media[1]["title"] == "Correct Movie"
    assert media[1]["original_title"] == "Correct Movie Original"
    assert media[1]["release_date"] == date(2020, 1, 1)
    assert media[1]["subscription_status"] == "WANTED"
    assert media[1]["subscription_sources_json"] == [
        {"type": "collection_correction", "id": int(collection["id"]), "name": "Correction Fixture"}
    ]


def test_custom_match_season_uses_provider_season_id_and_ignores_old_authoritative_season(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    collection = repository.create_custom(
        name="Season Correction",
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
                "items": '[{"tmdb_id":"100","title":"Old Season","media_type":"Series","season":1}]',
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, parent_series_tmdb_id,
                    season_number, subscription_status
                ) VALUES (
                    '10001', 'Season', 'Old Season', '100', 1, 'WANTED'
                )
                """
            )
        )

    candidate = repository.inspect_custom_match(
        int(collection["id"]),
        old_tmdb_id="100",
        old_title=None,
    )
    assert candidate is not None and candidate.season_number == 1
    corrected = repository.apply_custom_match(
        candidate,
        CollectionCorrectionSnapshot(
            collection_tmdb_id="200",
            season_number=2,
            target_key=SubscriptionMediaKey("20002", "Season"),
            target_record={
                "title": "New Season",
                "original_title": "New Season",
                "release_date": "2099-01-01",
                "release_year": 2099,
                "poster_path": "/season.jpg",
                "backdrop_path": "/series-backdrop.jpg",
                "overview": "Season overview",
                "season_number": 2,
                "parent_series_tmdb_id": "200",
            },
            target_status="PENDING_RELEASE",
            corrected_item={
                "tmdb_id": "200",
                "title": "New Series - 第 2 季",
                "media_type": "Series",
                "season": 2,
                "status": "unreleased",
            },
            parent_key=SubscriptionMediaKey("200", "Series"),
            parent_record={
                "title": "New Series",
                "original_title": "New Series Original",
                "release_date": "2020-01-01",
                "release_year": 2020,
                "poster_path": "/series.jpg",
                "backdrop_path": "/series-backdrop.jpg",
                "overview": "Series overview",
                "season_number": None,
                "parent_series_tmdb_id": None,
            },
        ),
    )

    assert corrected["tmdb_id"] == "200"
    row = repository.get_custom(int(collection["id"]))
    assert row is not None
    assert row["generated_media_info_json"][0]["tmdb_id"] == "200"
    assert row["generated_media_info_json"][0]["season"] == 2
    with p8_runtime.engine.connect() as connection:
        values = {
            (str(item["tmdb_id"]), str(item["item_type"])): dict(item)
            for item in connection.execute(
                text(
                    """
                    SELECT tmdb_id, item_type, title, parent_series_tmdb_id,
                           season_number, subscription_status, subscription_sources_json, ignore_reason
                    FROM media_metadata
                    WHERE tmdb_id IN ('10001','200','20002')
                    """
                )
            ).mappings()
        }
    assert values[("10001", "Season")]["subscription_status"] == "IGNORED"
    assert values[("10001", "Season")]["ignore_reason"] == "修正为 200"
    assert values[("200", "Series")]["subscription_status"] == "NONE"
    assert values[("20002", "Season")]["parent_series_tmdb_id"] == "200"
    assert values[("20002", "Season")]["season_number"] == 2
    assert values[("20002", "Season")]["subscription_status"] == "PENDING_RELEASE"
    assert values[("20002", "Season")]["subscription_sources_json"] == [
        {"type": "collection_correction", "id": int(collection["id"]), "name": "Season Correction"}
    ]


def test_custom_match_sql_failure_rolls_back_media_and_both_json_documents(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    collection = repository.create_custom(
        name="Correction Rollback",
        collection_type="list",
        definition={"source": "fixture"},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET generated_media_info_json =
                    '[{"tmdb_id":"101","title":"Fixture","media_type":"Movie"}]'::jsonb
                WHERE id = :collection_id
                """
            ),
            {"collection_id": collection["id"]},
        )
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (tmdb_id, item_type, title, subscription_status)
                VALUES ('101', 'Movie', 'Old', 'WANTED')
                """
            )
        )
    before = repository.get_custom(int(collection["id"]))
    candidate = repository.inspect_custom_match(
        int(collection["id"]),
        old_tmdb_id="101",
        old_title=None,
    )
    assert candidate is not None
    snapshot = CollectionCorrectionSnapshot(
        collection_tmdb_id="202",
        season_number=None,
        target_key=SubscriptionMediaKey("202", "Movie"),
        target_record={"title": "New", "release_date": "2020-01-01"},
        target_status="WANTED",
        corrected_item={"tmdb_id": "202"},
    )

    def fail_update(_connection, _cursor, statement, _parameters, _context, _executemany):
        if "SET definition_json = CAST" in statement:
            raise RuntimeError("injected update failure")

    event.listen(p8_runtime.engine, "before_cursor_execute", fail_update)
    try:
        with pytest.raises(RuntimeError, match="injected update failure"):
            repository.apply_custom_match(candidate, snapshot)
    finally:
        event.remove(p8_runtime.engine, "before_cursor_execute", fail_update)

    after = repository.get_custom(int(collection["id"]))
    assert after is not None and before is not None
    assert after["definition_json"] == before["definition_json"]
    assert after["generated_media_info_json"] == before["generated_media_info_json"]
    with p8_runtime.engine.connect() as connection:
        statuses = connection.execute(
            text(
                """
                SELECT tmdb_id, subscription_status
                FROM media_metadata
                WHERE tmdb_id IN ('101','202')
                ORDER BY tmdb_id
                """
            )
        ).all()
    assert statuses == [("101", "WANTED")]


def test_custom_match_rejects_stale_collection_digest_without_media_mutation(
    p8_runtime: DatabaseRuntime,
) -> None:
    repository = _repository(p8_runtime)
    collection = repository.create_custom(
        name="Correction Drift",
        collection_type="list",
        definition={},
        allowed_user_ids=None,
    )
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET generated_media_info_json =
                    '[{"tmdb_id":"101","title":"Fixture","media_type":"Movie"}]'::jsonb
                WHERE id = :collection_id
                """
            ),
            {"collection_id": collection["id"]},
        )
    candidate = repository.inspect_custom_match(
        int(collection["id"]),
        old_tmdb_id="101",
        old_title=None,
    )
    assert candidate is not None
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                UPDATE custom_collections
                SET definition_json = CAST(:definition AS jsonb)
                WHERE id = :collection_id
                """
            ),
            {"collection_id": collection["id"], "definition": '{"changed":true}'},
        )

    with pytest.raises(CollectionMatchConflictError):
        repository.apply_custom_match(
            candidate,
            CollectionCorrectionSnapshot(
                collection_tmdb_id="202",
                season_number=None,
                target_key=SubscriptionMediaKey("202", "Movie"),
                target_record={"title": "New"},
                target_status="WANTED",
                corrected_item={"tmdb_id": "202"},
            ),
        )
    with p8_runtime.engine.connect() as connection:
        assert connection.scalar(text("SELECT COUNT(*) FROM media_metadata WHERE tmdb_id = '202'")) == 0


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
