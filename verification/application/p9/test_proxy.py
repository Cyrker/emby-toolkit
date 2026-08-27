from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from etk.processes.proxy import InMemoryPlaybackLeaseStore, InMemoryVirtualLibrary, VirtualItem, create_proxy_app


def _catalog() -> InMemoryVirtualLibrary:
    catalog = InMemoryVirtualLibrary(server_id="fixture-server")
    catalog.add_library("42", "Fixture Library", allowed_user_ids=("user-1",), item_types=("Movie",))
    catalog.add_item(
        VirtualItem(
            id="movie-1",
            name="Zeta",
            item_type="Movie",
            direct_url="https://upstream.fixture/movie-1",
            metadata={"DateCreated": "2026-01-02"},
        ),
        library_id="42",
    )
    catalog.add_item(
        VirtualItem(
            id="movie-2",
            name="Alpha",
            item_type="Movie",
            direct_url="https://upstream.fixture/movie-2",
            metadata={"DateCreated": "2026-01-01"},
        ),
        library_id="42",
    )
    return catalog


def test_proxy_license_health_and_virtual_library_contract() -> None:
    app = create_proxy_app(catalog=_catalog())

    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        assert client.get("/Users/user-1/Views").status_code == 200
        views = client.get("/Users/user-1/Views").json()
        assert views["TotalRecordCount"] == 1
        first = views["Items"][0]
        assert first["Id"] == views["Items"][0]["Id"]
        assert first["Guid"] == views["Items"][0]["Guid"]
        assert first["Etag"] == views["Items"][0]["Etag"]
        assert client.get("/Users/other/Views").json() == {"Items": [], "TotalRecordCount": 0}

        virtual_id = first["Id"]
        page = client.get(
            "/Users/user-1/Items",
            params={"ParentId": virtual_id, "SortBy": "SortName", "SortOrder": "Ascending", "Limit": 1},
        )
        assert page.json()["TotalRecordCount"] == 2
        assert [item["Name"] for item in page.json()["Items"]] == ["Alpha"]


def test_proxy_playback_lease_replay_limit_progress_stop_and_non_etk_fallback() -> None:
    leases = InMemoryPlaybackLeaseStore()
    app = create_proxy_app(catalog=_catalog(), leases=leases, max_streams=1)

    with TestClient(app) as client:
        redirect = client.get(
            "/Videos/movie-1/stream",
            params={"UserId": "user-1", "PlaySessionId": "session-1"},
            follow_redirects=False,
        )
        assert redirect.status_code == 302
        assert redirect.headers["location"] == "https://upstream.fixture/movie-1"
        assert redirect.headers["access-control-allow-origin"] == "*"
        assert len(leases.active_for_user("user-1")) == 1

        replay = client.post(
            "/Sessions/playing",
            json={"UserId": "user-1", "ItemId": "movie-1", "PlaySessionId": "session-1"},
        )
        assert replay.status_code == 200
        assert len(leases.active_for_user("user-1")) == 1

        limited = client.get(
            "/Videos/movie-2/stream",
            params={"UserId": "user-1", "PlaySessionId": "session-2"},
            follow_redirects=False,
        )
        assert limited.status_code == 429

        progress = client.post(
            "/Sessions/playing/progress",
            json={"UserId": "user-1", "ItemId": "movie-1", "PlaySessionId": "session-1"},
        )
        assert progress.status_code == 200

        stopped = client.post(
            "/Sessions/playing/stopped",
            json={"UserId": "user-1", "ItemId": "movie-1", "PlaySessionId": "session-1"},
        )
        assert stopped.status_code == 200
        assert stopped.json()["released"] == 1
        assert leases.active_for_user("user-1") == ()

        available_after_stop = client.get(
            "/Videos/movie-2/stream",
            params={"UserId": "user-1", "PlaySessionId": "session-2"},
            follow_redirects=False,
        )
        assert available_after_stop.status_code == 302
        assert client.get("/Videos/not-etk/stream", params={"UserId": "user-1"}).status_code == 409


def test_proxy_progress_refreshes_device_bound_lease_without_session_id() -> None:
    leases = InMemoryPlaybackLeaseStore()
    app = create_proxy_app(catalog=_catalog(), leases=leases)
    with TestClient(app) as client:
        started = client.get(
            "/Videos/movie-1/stream",
            params={"UserId": "user-1", "DeviceId": "device-1"},
            headers={"X-Emby-Client": "Fixture Client"},
            follow_redirects=False,
        )
        assert started.status_code == 302
        progress = client.post(
            "/Sessions/playing/progress",
            json={"UserId": "user-1", "ItemId": "movie-1", "DeviceId": "device-1", "Client": "Fixture Client"},
        )
        assert progress.status_code == 200


def test_proxy_expired_lease_reclamation_and_inactive_license() -> None:
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    clock = [now]
    leases = InMemoryPlaybackLeaseStore(clock=lambda: clock[0])
    lease = leases.acquire("user-1", "movie-1", play_session_id="session-1", ttl_seconds=1)
    assert lease is not None
    clock[0] = now + timedelta(seconds=2)
    assert leases.reclaim_expired() == 1

    app = create_proxy_app(catalog=_catalog(), license_active=False)
    with TestClient(app) as client:
        assert client.get("/api/health").status_code == 200
        denied = client.get("/Users/user-1/Views")
        assert denied.status_code == 403
        assert denied.headers["content-type"].startswith("application/json")


def test_proxy_websocket_fake_echo_and_inactive_rejection() -> None:
    app = create_proxy_app(catalog=_catalog())
    with TestClient(app) as client:
        with client.websocket_connect("/embywebsocket") as websocket:
            websocket.send_text("fixture-message")
            assert websocket.receive_text() == "fixture-message"

    inactive = create_proxy_app(catalog=_catalog(), license_active=False)
    with TestClient(inactive) as client:
        try:
            with client.websocket_connect("/embywebsocket"):
                raise AssertionError("inactive proxy websocket unexpectedly connected")
        except WebSocketDisconnect as exc:
            assert exc.code in {403, 4403}
