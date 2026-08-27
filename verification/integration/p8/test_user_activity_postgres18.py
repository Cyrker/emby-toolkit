from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime

from sqlalchemy import text

from etk.domains.identity.user_activity import P8UserEventGateway, SqlAlchemyUserActivityRepository
from etk.platform.db import DatabaseRuntime

NOW = datetime(2026, 8, 27, 9, 0, tzinfo=UTC)


class FixtureResolver:
    @staticmethod
    def series_id_for_episode(user_id: str, episode_id: str) -> str | None:
        del user_id
        return "series-1" if episode_id == "episode-1" else None


class FixtureGuard:
    @staticmethod
    def consume(user_id: str, event_key: str) -> bool:
        del user_id, event_key
        return False


def _gateway(runtime: DatabaseRuntime) -> P8UserEventGateway:
    return P8UserEventGateway(
        repository=SqlAlchemyUserActivityRepository(runtime.unit_of_work),
        resolver=FixtureResolver(),
        policy_guard=FixtureGuard(),
        clock=lambda: NOW,
    )


def _event(
    event_id: str,
    name: str,
    *,
    item_type: str = "Series",
    item_id: str = "series-1",
    user_data: dict[str, object] | None = None,
    playback: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "_etk_source": "ETKMediaInfoBridge",
        "Event": name,
        "EventId": event_id,
        "User": {"Id": "user-1"},
        "Item": {"Id": item_id, "Type": item_type, "RunTimeTicks": 1000},
        "UserData": user_data or {},
        "PlaybackInfo": playback or {},
    }


def _handle(gateway: P8UserEventGateway, payload: dict[str, object]):
    return gateway.handle(payload, host_url="https://fixture.invalid", remote_addr="127.0.0.1")


def test_activity_projection_receipt_and_effect_intent_are_atomic_and_durable(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = _gateway(p8_runtime)
    payload = _event(
        "favorite-event",
        "item.markfavorite",
        user_data={"IsFavorite": True},
    )
    first = _handle(gateway, payload)
    with p8_runtime.engine.connect() as connection:
        favorite = connection.execute(
            text(
                """
                SELECT user_id, item_id, updated_at
                FROM emby_favorite_items
                WHERE user_id = 'user-1' AND item_id = 'series-1'
                """
            )
        ).one()
        aggregate = connection.execute(
            text(
                """
                SELECT is_favorite, played, playback_position_ticks,
                       play_count, last_updated_at
                FROM user_media_data
                WHERE user_id = 'user-1' AND item_id = 'series-1'
                """
            )
        ).one()
        effects = connection.execute(
            text(
                """
                SELECT effect_type, status
                FROM media_effects
                ORDER BY effect_type
                """
            )
        ).all()
        outbox = connection.execute(
            text(
                """
                SELECT topic, status
                FROM outbox_events
                ORDER BY created_at
                """
            )
        ).all()

    replay = _handle(gateway, payload)
    with p8_runtime.engine.connect() as connection:
        replay_favorite = connection.execute(
            text(
                """
                SELECT updated_at FROM emby_favorite_items
                WHERE user_id = 'user-1' AND item_id = 'series-1'
                """
            )
        ).one()
        assert connection.scalar(text("SELECT COUNT(*) FROM media_effects")) == 2
        assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 1

    assert first.payload == replay.payload == {"status": "user_data_updated"}
    assert favorite.updated_at == replay_favorite.updated_at == NOW
    assert aggregate == (True, False, 0, 0, NOW)
    assert effects == [
        ("emby.event.user.applied", "confirmed"),
        ("emby.event.user.favorite-intro-detection", "pending"),
    ]
    assert outbox == [("media.effect.requested", "pending")]

    conflict_payload = {**payload, "UserData": {"IsFavorite": False}}
    conflict = _handle(gateway, conflict_payload)
    assert conflict.status_code == 409
    assert conflict.payload == {"status": "event_id_conflict"}


def test_episode_aggregate_and_partial_updates_preserve_unmentioned_fields(
    p8_runtime: DatabaseRuntime,
) -> None:
    gateway = _gateway(p8_runtime)
    _handle(
        gateway,
        _event(
            "episode-favorite",
            "item.rate",
            item_type="Episode",
            item_id="episode-1",
            user_data={"IsFavorite": True, "Played": True},
        ),
    )
    _handle(
        gateway,
        _event(
            "episode-unplayed",
            "item.markunplayed",
            item_type="Episode",
            item_id="episode-1",
            user_data={"Played": False},
        ),
    )
    _handle(
        gateway,
        _event(
            "episode-position",
            "playback.pause",
            item_type="Episode",
            item_id="episode-1",
            playback={"PositionTicks": 321},
        ),
    )

    with p8_runtime.engine.connect() as connection:
        exact = connection.scalar(
            text(
                """
                SELECT COUNT(*) FROM emby_favorite_items
                WHERE user_id = 'user-1' AND item_id = 'episode-1'
                """
            )
        )
        aggregate = connection.execute(
            text(
                """
                SELECT is_favorite, played, playback_position_ticks,
                       play_count, last_played_date
                FROM user_media_data
                WHERE user_id = 'user-1' AND item_id = 'series-1'
                """
            )
        ).one()
    assert exact == 1
    assert aggregate == (True, False, 321, 0, NOW)


def test_concurrent_duplicate_and_nonconflicting_partial_events_converge(
    p8_runtime: DatabaseRuntime,
) -> None:
    duplicate = _event(
        "concurrent-duplicate",
        "item.markfavorite",
        user_data={"IsFavorite": True},
    )
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(lambda _: _handle(_gateway(p8_runtime), duplicate), range(20)))
    assert {result.status_code for result in results} == {200}
    assert {result.payload["status"] for result in results} == {"user_data_updated"}
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(text("SELECT COUNT(*) FROM media_effects WHERE effect_type = 'emby.event.user.applied'"))
            == 1
        )
        assert (
            connection.scalar(
                text(
                    """
                SELECT COUNT(*) FROM media_effects
                WHERE effect_type = 'emby.event.user.favorite-intro-detection'
                """
                )
            )
            == 1
        )

    favorite_patch = _event(
        "concurrent-favorite-patch",
        "item.rate",
        user_data={"IsFavorite": False},
    )
    playback_patch = _event(
        "concurrent-playback-patch",
        "playback.pause",
        playback={"PositionTicks": 777},
    )
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(_handle, _gateway(p8_runtime), favorite_patch),
            executor.submit(_handle, _gateway(p8_runtime), playback_patch),
        ]
        for future in futures:
            assert future.result(timeout=10).status_code == 200
    with p8_runtime.engine.connect() as connection:
        assert connection.execute(
            text(
                """
                SELECT is_favorite, playback_position_ticks
                FROM user_media_data
                WHERE user_id = 'user-1' AND item_id = 'series-1'
                """
            )
        ).one() == (False, 777)


def test_outbox_failure_rolls_back_receipt_projection_and_exact_favorite(
    p8_runtime: DatabaseRuntime,
) -> None:
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE FUNCTION p8_reject_user_event_outbox() RETURNS trigger
                LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.payload_json ->> 'effect_type'
                       = 'emby.event.user.favorite-intro-detection' THEN
                        RAISE EXCEPTION 'fixture outbox rejected';
                    END IF;
                    RETURN NEW;
                END;
                $$
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TRIGGER p8_reject_user_event_outbox
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION p8_reject_user_event_outbox()
                """
            )
        )
    try:
        failed = _handle(
            _gateway(p8_runtime),
            _event(
                "rollback-favorite",
                "item.markfavorite",
                user_data={"IsFavorite": True},
            ),
        )
        assert failed.status_code == 500
        with p8_runtime.engine.connect() as connection:
            assert connection.scalar(text("SELECT COUNT(*) FROM emby_favorite_items")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM user_media_data")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM media_effects")) == 0
            assert connection.scalar(text("SELECT COUNT(*) FROM outbox_events")) == 0
    finally:
        with p8_runtime.engine.begin() as connection:
            connection.execute(text("DROP TRIGGER IF EXISTS p8_reject_user_event_outbox ON outbox_events"))
            connection.execute(text("DROP FUNCTION IF EXISTS p8_reject_user_event_outbox()"))
