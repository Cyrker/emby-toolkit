from __future__ import annotations

import json
import threading
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from sqlalchemy import text

from etk.domains.p115.models import P115CredentialSnapshot
from etk.domains.p115.playback import (
    P115PlaybackNotFoundError,
    P115PlaybackStateError,
    PlaybackMode,
    PlaybackPrepareRequest,
)
from etk.domains.p115.playback_native import (
    SqlAlchemyP115PlaybackConfigBackend,
    SqlAlchemyP115PlaybackSourceGateway,
)
from etk.domains.p115.playback_reward_repository import SqlAlchemyP115PlaybackRewardRepository
from etk.platform.db import DatabaseRuntime

SHA1 = "A" * 40
PREID = "B" * 40
PICK_CODE = "fixture-speedtest-pick"
INTERACTIVE_SHA1 = "C" * 40
INTERACTIVE_PREID = "D" * 40
INTERACTIVE_PICK_CODE = "fixture-interactive-pick"
USER_IDS = ("native-reward-shared", "native-reward-private")
CONFIG_BACKEND_KEY = "etk:test:p115-playback-config-backend"


@pytest.fixture(autouse=True)
def clean_native_playback_facts(p6_runtime: DatabaseRuntime):
    def clean() -> None:
        with p6_runtime.engine.begin() as connection:
            connection.execute(text("DELETE FROM app_settings WHERE setting_key LIKE 'etk:p4:play-pool-reward:v1:%'"))
            connection.execute(
                text("DELETE FROM app_settings WHERE setting_key = :setting_key"),
                {"setting_key": CONFIG_BACKEND_KEY},
            )
            connection.execute(
                text("DELETE FROM emby_users WHERE id = ANY(CAST(:ids AS text[]))"),
                {"ids": list(USER_IDS)},
            )
            connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id LIKE 'native-speedtest-%'"))
            connection.execute(text("DELETE FROM p115_filesystem_cache WHERE id LIKE 'native-playback-%'"))
            connection.execute(
                text("DELETE FROM p115_mediainfo_cache WHERE UPPER(sha1) = ANY(CAST(:sha1s AS text[]))"),
                {"sha1s": [SHA1, INTERACTIVE_SHA1]},
            )
            connection.execute(
                text(
                    "DELETE FROM media_metadata "
                    "WHERE tmdb_id LIKE 'native-speedtest%' OR tmdb_id LIKE 'native-playback%'"
                )
            )
            connection.execute(text("DELETE FROM shared_virtual_imports WHERE source_id LIKE 'native-playback-%'"))

    clean()
    yield
    clean()


def _request() -> PlaybackPrepareRequest:
    return PlaybackPrepareRequest(mode=PlaybackMode.MAINTENANCE, source="speedtest")


class PrimaryCredentials:
    def __init__(self, account_id: str = "115001") -> None:
        self.account_id = account_id

    def load(self) -> P115CredentialSnapshot:
        return P115CredentialSnapshot(access_token="fixture-token", account_id=self.account_id)


def _insert_cache(
    runtime: DatabaseRuntime,
    *,
    fid: str,
    account_id: str = "115001",
    pick_code: str = PICK_CODE,
    sha1: str = SHA1,
    size: int = 1024,
    name: str = "Fixture.mkv",
) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_filesystem_cache(
                    id, parent_id, name, sha1, pick_code, size, account_id, entry_kind
                ) VALUES (
                    :id, :parent_id, :name, :sha1, :pick_code, :size, :account_id, 'file'
                )
                """
            ),
            {
                "id": fid,
                "parent_id": f"parent-{fid}",
                "name": name,
                "sha1": sha1,
                "pick_code": pick_code,
                "size": size,
                "account_id": account_id,
            },
        )


def _insert_preid(runtime: DatabaseRuntime, *, sha1: str = SHA1, preid: str = PREID) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO p115_mediainfo_cache(sha1, raw_ffprobe_json)
                VALUES (:sha1, CAST(:raw AS jsonb))
                """
            ),
            {"sha1": sha1, "raw": json.dumps({"_etk": {"preid": preid}})},
        )


def _insert_metadata(runtime: DatabaseRuntime) -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, in_library, file_pickcode_json,
                    file_sha1_json, asset_details_json
                ) VALUES (
                    'native-speedtest', 'Movie', TRUE, CAST(:pick_codes AS jsonb),
                    CAST(:sha1s AS jsonb), CAST(:assets AS jsonb)
                )
                """
            ),
            {
                "pick_codes": json.dumps([PICK_CODE]),
                "sha1s": json.dumps([SHA1]),
                "assets": json.dumps([{"file_name": "Metadata-Name.mkv"}]),
            },
        )


def _insert_interactive_metadata(runtime: DatabaseRuntime, *, item_id: str = "native-playback-item") -> None:
    with runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata(
                    tmdb_id, item_type, title, in_library, emby_item_ids_json,
                    file_pickcode_json, file_sha1_json, asset_details_json
                ) VALUES (
                    'native-playback-movie', 'Movie', 'Interactive Fixture', TRUE,
                    CAST(:emby_ids AS jsonb), CAST(:pick_codes AS jsonb),
                    CAST(:sha1s AS jsonb), CAST(:assets AS jsonb)
                )
                """
            ),
            {
                "emby_ids": json.dumps([item_id]),
                "pick_codes": json.dumps([INTERACTIVE_PICK_CODE]),
                "sha1s": json.dumps([INTERACTIVE_SHA1]),
                "assets": json.dumps(
                    [
                        {
                            "emby_item_id": item_id,
                            "file_name": "Interactive.mkv",
                            "size": 2048,
                        }
                    ]
                ),
            },
        )


def _insert_virtual(runtime: DatabaseRuntime) -> int:
    with runtime.engine.begin() as connection:
        return int(
            connection.scalar(
                text(
                    """
                    INSERT INTO shared_virtual_imports(
                        source_kind, source_id, title, file_count, total_size, status, files_json
                    ) VALUES (
                        'shared_center', 'native-playback-virtual', 'Virtual Fixture', 1, 2048,
                        'virtual', CAST(:files AS jsonb)
                    ) RETURNING id
                    """
                ),
                {
                    "files": json.dumps(
                        [
                            {
                                "sha1": INTERACTIVE_SHA1,
                                "size": 2048,
                                "pick_code": INTERACTIVE_PICK_CODE,
                                "preid": INTERACTIVE_PREID,
                                "file_name": "Virtual.mkv",
                                "source_id": "virtual-file-1",
                            }
                        ]
                    )
                },
            )
        )


def test_native_speedtest_source_freezes_complete_metadata_holder_snapshot(p6_runtime: DatabaseRuntime) -> None:
    _insert_metadata(p6_runtime)
    _insert_cache(p6_runtime, fid="native-speedtest-holder", name="Holder-Name.mkv")
    _insert_preid(p6_runtime)

    source = SqlAlchemyP115PlaybackSourceGateway(p6_runtime.unit_of_work).snapshot(_request())

    assert source == {
        "fid": "native-speedtest-holder",
        "holder_account_id": "115001",
        "pick_code": PICK_CODE,
        "sha1": SHA1,
        "size": 1024,
        "preid": PREID,
        "file_name": "Holder-Name.mkv",
    }


def test_native_speedtest_source_uses_complete_cache_fallback_without_pool_uuid_binding(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, fid="native-speedtest-fallback")
    _insert_preid(p6_runtime)

    source = SqlAlchemyP115PlaybackSourceGateway(p6_runtime.unit_of_work).snapshot(_request())

    assert source["fid"] == "native-speedtest-fallback"
    assert source["holder_account_id"] == "115001"
    assert source["preid"] == PREID


def test_native_speedtest_source_uses_only_the_primary_holder_even_when_metadata_pickcode_differs(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_metadata(p6_runtime)
    _insert_cache(
        p6_runtime,
        fid="native-speedtest-secondary-holder",
        account_id="115002",
        pick_code=PICK_CODE,
    )
    _insert_cache(
        p6_runtime,
        fid="native-speedtest-primary-holder",
        account_id="115001",
        pick_code="primary-holder-pick",
    )
    _insert_preid(p6_runtime)

    source = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    ).snapshot(_request())

    assert source["fid"] == "native-speedtest-primary-holder"
    assert source["holder_account_id"] == "115001"
    assert source["pick_code"] == "primary-holder-pick"


def test_native_speedtest_fallback_rejects_duplicate_primary_holder_identity(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(p6_runtime, fid="native-speedtest-duplicate-a", account_id="115001")
    _insert_cache(p6_runtime, fid="native-speedtest-duplicate-b", account_id="115001")
    _insert_preid(p6_runtime)

    gateway = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    )

    with pytest.raises(P115PlaybackStateError, match="歧义"):
        gateway.snapshot(_request())


def test_native_speedtest_source_fails_closed_for_missing_and_ambiguous_facts(
    p6_runtime: DatabaseRuntime,
) -> None:
    gateway = SqlAlchemyP115PlaybackSourceGateway(p6_runtime.unit_of_work)
    with pytest.raises(P115PlaybackNotFoundError, match="PickCode"):
        gateway.snapshot(_request())

    _insert_metadata(p6_runtime)
    _insert_preid(p6_runtime)
    _insert_cache(p6_runtime, fid="native-speedtest-ambiguous-a", account_id="115001")
    _insert_cache(p6_runtime, fid="native-speedtest-ambiguous-b", account_id="115002")

    with pytest.raises(P115PlaybackStateError, match="歧义"):
        gateway.snapshot(_request())


def test_native_pool_source_resolves_emby_slot_and_primary_holder_without_cached_preid(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_interactive_metadata(p6_runtime)
    _insert_cache(
        p6_runtime,
        fid="native-playback-pool",
        account_id="115001",
        pick_code=INTERACTIVE_PICK_CODE,
        sha1=INTERACTIVE_SHA1,
        size=2048,
        name="Interactive.mkv",
    )
    gateway = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    )

    source = gateway.snapshot(
        PlaybackPrepareRequest(
            mode=PlaybackMode.POOL,
            item_id="native-playback-item",
            source_pick_code=INTERACTIVE_PICK_CODE,
            file_name="Interactive.mkv",
        )
    )

    assert source == {
        "fid": "native-playback-pool",
        "holder_account_id": "115001",
        "pick_code": INTERACTIVE_PICK_CODE,
        "sha1": INTERACTIVE_SHA1,
        "size": 2048,
        "preid": "",
        "file_name": "Interactive.mkv",
    }


def test_native_copy_source_uses_primary_cache_and_cached_preid(p6_runtime: DatabaseRuntime) -> None:
    _insert_cache(
        p6_runtime,
        fid="native-playback-copy",
        account_id="115001",
        pick_code=INTERACTIVE_PICK_CODE,
        sha1=INTERACTIVE_SHA1,
        size=2048,
        name="Copy.mkv",
    )
    _insert_preid(p6_runtime, sha1=INTERACTIVE_SHA1, preid=INTERACTIVE_PREID)
    gateway = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    )

    source = gateway.snapshot(
        PlaybackPrepareRequest(
            mode=PlaybackMode.COPY,
            source_pick_code=INTERACTIVE_PICK_CODE,
            file_name="Copy.mkv",
        )
    )

    assert source["fid"] == "native-playback-copy"
    assert source["holder_account_id"] == "115001"
    assert source["preid"] == INTERACTIVE_PREID


def test_native_virtual_source_uses_durable_import_projection_and_primary_holder(
    p6_runtime: DatabaseRuntime,
) -> None:
    virtual_id = _insert_virtual(p6_runtime)
    gateway = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    )

    source = gateway.snapshot(
        PlaybackPrepareRequest(
            mode=PlaybackMode.VIRTUAL,
            virtual_id=virtual_id,
            sha1=INTERACTIVE_SHA1,
            file_name="Virtual.mkv",
        )
    )

    assert source["virtual_id"] == virtual_id
    assert source["holder_account_id"] == "115001"
    assert source["source_kind"] == "shared_center"
    assert source["source_id"] == "native-playback-virtual"
    assert source["pick_code"] == INTERACTIVE_PICK_CODE
    assert source["preid"] == INTERACTIVE_PREID


def test_native_interactive_source_rejects_non_primary_and_ambiguous_cache_rows(
    p6_runtime: DatabaseRuntime,
) -> None:
    _insert_cache(
        p6_runtime,
        fid="native-playback-secondary",
        account_id="115002",
        pick_code=INTERACTIVE_PICK_CODE,
        sha1=INTERACTIVE_SHA1,
        size=2048,
    )
    gateway = SqlAlchemyP115PlaybackSourceGateway(
        p6_runtime.unit_of_work,
        primary_credentials=cast(Any, PrimaryCredentials()),
    )
    request = PlaybackPrepareRequest(mode=PlaybackMode.COPY, source_pick_code=INTERACTIVE_PICK_CODE)
    with pytest.raises(P115PlaybackNotFoundError, match="主号"):
        gateway.snapshot(request)

    _insert_cache(
        p6_runtime,
        fid="native-playback-primary-a",
        account_id="115001",
        pick_code=INTERACTIVE_PICK_CODE,
        sha1=INTERACTIVE_SHA1,
        size=2048,
        name="A.mkv",
    )
    _insert_cache(
        p6_runtime,
        fid="native-playback-primary-b",
        account_id="115001",
        pick_code=INTERACTIVE_PICK_CODE,
        sha1=INTERACTIVE_SHA1,
        size=2048,
        name="B.mkv",
    )
    with pytest.raises(P115PlaybackStateError, match="歧义"):
        gateway.snapshot(request)


@pytest.mark.parametrize(
    ("user_id", "days", "mode"),
    [
        ("native-reward-shared", 0.8, "shared"),
        ("native-reward-private", 0.5, "private"),
    ],
)
def test_native_reward_receipt_is_atomic_idempotent_and_preserves_reward_values(
    p6_runtime: DatabaseRuntime,
    user_id: str,
    days: float,
    mode: str,
) -> None:
    with p6_runtime.engine.begin() as connection:
        connection.execute(
            text("INSERT INTO emby_users(id, name) VALUES (:id, :name)"),
            {"id": user_id, "name": user_id},
        )
        connection.execute(
            text(
                """
                INSERT INTO emby_users_extended(emby_user_id, expiration_date)
                VALUES (:id, NOW() + INTERVAL '10 days')
                """
            ),
            {"id": user_id},
        )
        before = connection.scalar(
            text("SELECT expiration_date FROM emby_users_extended WHERE emby_user_id = :id"),
            {"id": user_id},
        )

    repository = SqlAlchemyP115PlaybackRewardRepository(p6_runtime.unit_of_work)
    barrier = threading.Barrier(2)
    results: list[dict[str, object]] = []
    errors: list[BaseException] = []

    def grant() -> None:
        try:
            barrier.wait(timeout=2)
            results.append(
                repository.grant_once(
                    user_id,
                    reward_date="2026-08-31",
                    reward_days=days,
                    reward_mode=mode,
                )
            )
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=grant, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert not errors
    assert sorted(bool(result["granted"]) for result in results) == [False, True]
    with p6_runtime.engine.connect() as connection:
        after = connection.scalar(
            text("SELECT expiration_date FROM emby_users_extended WHERE emby_user_id = :id"),
            {"id": user_id},
        )
    assert isinstance(before, datetime) and isinstance(after, datetime)
    assert (after.astimezone(UTC) - before.astimezone(UTC)).total_seconds() == pytest.approx(days * 86400)
    assert repository.summary(user_id) == {
        "total_days": days,
        "last_reward_days": days,
        "last_reward_date": "2026-08-31",
        "last_reward_mode": mode,
        "last_expiration_date": after.astimezone(UTC).isoformat(),
    }


def test_native_reward_rejects_a_nonexistent_calendar_date(p6_runtime: DatabaseRuntime) -> None:
    repository = SqlAlchemyP115PlaybackRewardRepository(p6_runtime.unit_of_work)

    with pytest.raises(ValueError, match="real calendar date"):
        repository.grant_once(
            "native-reward-shared",
            reward_date="2026-02-31",
            reward_days=0.8,
            reward_mode="shared",
        )


def test_native_playback_config_backend_serializes_concurrent_mutations(p6_runtime: DatabaseRuntime) -> None:
    backend = SqlAlchemyP115PlaybackConfigBackend(p6_runtime.unit_of_work)
    barrier = threading.Barrier(2)
    results: list[int] = []

    def increment() -> None:
        barrier.wait(timeout=2)

        def mutate(current: dict[str, object] | None, _now: datetime):
            value = int(cast(Any, (current or {}).get("value")) or 0) + 1
            return {"value": value}, value, True

        results.append(int(backend.mutate(CONFIG_BACKEND_KEY, mutate)))

    threads = [threading.Thread(target=increment, daemon=True) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)

    assert sorted(results) == [1, 2]
    with p6_runtime.engine.connect() as connection:
        value = connection.scalar(
            text("SELECT value_json FROM app_settings WHERE setting_key = :setting_key"),
            {"setting_key": CONFIG_BACKEND_KEY},
        )
    assert value == {"value": 2}
