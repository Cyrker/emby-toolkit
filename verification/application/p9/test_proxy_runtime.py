from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
from fastapi.testclient import TestClient
from starlette.responses import Response

from etk.platform import Settings
from etk.processes.proxy import ProxyCatalogError, create_proxy_app
from etk.processes.proxy_runtime import (
    EmbyCatalogClient,
    HttpxEmbyProxyUpstream,
    SqlAlchemyProxyCatalog,
    _emby_target,
)


class FakeCollections:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self.rows = [dict(row) for row in rows]

    def list_active_for_execution(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.rows if row.get("status") == "active"]

    def get_custom(self, collection_id: int) -> dict[str, Any] | None:
        return next((dict(row) for row in self.rows if row.get("id") == collection_id), None)


class FakeLibraryQuery:
    def query_filter(self, *_: Any, **__: Any) -> Any:
        raise AssertionError("filter query is not expected")

    def query_personal_recommendation(self, *_: Any, **__: Any) -> Any:
        raise AssertionError("recommendation query is not expected")


class FakeEmbyCatalog:
    def __init__(self) -> None:
        self.tokens: list[str] = []
        self.playback_payload: Mapping[str, Any] = {"MediaSources": []}

    def user_views(self, user_id: str, access_token: str) -> list[dict[str, Any]]:
        assert user_id == "user-1"
        self.tokens.append(access_token)
        return [
            {"Id": "native-1", "Name": "Movies", "CollectionType": "movies", "ServerId": "server-1"},
            {"Id": "native-2", "Name": "Shows", "CollectionType": "tvshows", "ServerId": "server-1"},
        ]

    def user_items(self, *_: Any, **__: Any) -> list[dict[str, Any]]:
        return []

    def user(self, user_id: str, access_token: str) -> Mapping[str, Any]:
        assert user_id == "user-1"
        self.tokens.append(access_token)
        if not access_token:
            raise ProxyCatalogError("token required", status_code=401)
        return {"Id": "other-user" if access_token == "other-token" else "user-1"}

    def playback_info(self, *_: Any, **__: Any) -> Mapping[str, Any]:
        return self.playback_payload


class FakeIdentity:
    def server_id(self) -> str:
        return "server-1"


def _production_catalog(
    settings: Settings,
    emby: FakeEmbyCatalog,
    p115_direct_url: Callable[[str, str], str] | None = None,
) -> SqlAlchemyProxyCatalog:
    rows = [
        {
            "id": 42,
            "name": "Virtual Movies",
            "type": "manual",
            "status": "active",
            "emby_collection_id": "boxset-42",
            "allowed_user_ids": [],
            "definition_json": {"item_type": "Movie"},
            "generated_media_info_json": [],
        }
    ]
    return SqlAlchemyProxyCatalog(
        lambda: None,  # type: ignore[arg-type,return-value]
        lambda: settings,
        FakeCollections(rows),  # type: ignore[arg-type]
        FakeLibraryQuery(),  # type: ignore[arg-type]
        emby,  # type: ignore[arg-type]
        FakeIdentity(),  # type: ignore[arg-type]
        p115_direct_url,
    )


def test_production_catalog_merges_selected_native_views_and_uses_caller_token() -> None:
    settings = Settings(
        proxy_merge_native_libraries=True,
        proxy_native_view_selection=["native-2"],
        proxy_native_view_order="after",
    )
    emby = FakeEmbyCatalog()
    app = create_proxy_app(catalog=_production_catalog(settings, emby), server_id="server-1")

    with TestClient(app) as client:
        response = client.get(
            "/emby/Users/user-1/Views",
            headers={"X-Emby-Authorization": 'MediaBrowser Client="Emby Web", Token="caller-token"'},
        )

    assert response.status_code == 200
    assert [(item["Id"], item["Name"]) for item in response.json()["Items"]] == [
        ("-900042", "Virtual Movies"),
        ("native-2", "Shows"),
    ]
    assert emby.tokens == ["caller-token"]


def test_official_ios_receives_unmodified_native_views_without_virtual_entries() -> None:
    settings = Settings(
        proxy_merge_native_libraries=True,
        proxy_native_view_selection=["native-2"],
    )
    emby = FakeEmbyCatalog()
    app = create_proxy_app(catalog=_production_catalog(settings, emby), server_id="server-1")

    with TestClient(app) as client:
        response = client.get(
            "/emby/Users/user-1/Views",
            headers={"X-Emby-Client": "Emby for iOS", "X-Emby-Token": "ios-token"},
        )

    assert [item["Id"] for item in response.json()["Items"]] == ["native-1", "native-2"]
    assert emby.tokens == ["ios-token"]


def test_virtual_library_details_require_a_matching_emby_user_token() -> None:
    settings = Settings()
    emby = FakeEmbyCatalog()
    app = create_proxy_app(catalog=_production_catalog(settings, emby), server_id="server-1")

    with TestClient(app) as client:
        missing = client.get("/emby/Users/user-1/Items/-900042")
        mismatch = client.get(
            "/emby/Users/user-1/Items/-900042",
            headers={"X-Emby-Token": "other-token"},
        )
        allowed = client.get(
            "/emby/Users/user-1/Items/-900042",
            headers={"X-Emby-Token": "caller-token"},
        )

    assert missing.status_code == 401
    assert mismatch.status_code == 403
    assert allowed.status_code == 200
    assert allowed.json()["Id"] == "-900042"


def test_emby_catalog_client_uses_token_without_environment_proxy() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"Items": [{"Id": "native-1"}]})

    transport = httpx.MockTransport(handler)
    settings = Settings(emby_server_url="http://emby.internal:8096", emby_api_timeout=15)
    client = EmbyCatalogClient(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=transport, trust_env=False),
    )

    assert client.user_views("user/1", "real-token") == [{"Id": "native-1"}]
    assert seen[0].url == "http://emby.internal:8096/Users/user%2F1/Views"
    assert seen[0].headers["X-Emby-Token"] == "real-token"


def test_p115_stream_resolves_to_direct_url_and_never_falls_back_to_emby() -> None:
    settings = Settings()
    emby = FakeEmbyCatalog()
    emby.playback_payload = {
        "MediaSources": [
            {"Path": "http://etk.internal:5257/api/p115/play/pick-code/Movie.mkv"},
        ]
    }
    calls: list[tuple[str, str]] = []

    def resolve(pick_code: str, user_agent: str) -> str:
        calls.append((pick_code, user_agent))
        return "https://115.example.test/direct"

    app = create_proxy_app(
        catalog=_production_catalog(settings, emby, resolve),
        server_id="server-1",
    )
    with TestClient(app) as client:
        response = client.get(
            "/Videos/item-1/stream",
            params={"UserId": "user-1", "PlaySessionId": "play-1", "api_key": "caller-token"},
            headers={"User-Agent": "Emby Web"},
            follow_redirects=False,
        )

    assert response.status_code == 302
    assert response.headers["Location"] == "https://115.example.test/direct"
    assert calls == [("pick-code", "Emby Web")]


def test_p115_resolution_failure_is_not_silently_sent_to_emby() -> None:
    settings = Settings()
    emby = FakeEmbyCatalog()
    emby.playback_payload = {"MediaSources": [{"Path": "http://etk/api/p115/play/pick-code"}]}
    app = create_proxy_app(
        catalog=_production_catalog(settings, emby, lambda *_: ""),
        server_id="server-1",
    )

    with TestClient(app) as client:
        response = client.get(
            "/Videos/item-1/stream",
            params={"UserId": "user-1", "api_key": "caller-token"},
            follow_redirects=False,
        )

    assert response.status_code == 503
    assert "did not return a URL" in response.text


def test_transcoding_is_blocked_before_the_emby_fallback() -> None:
    class Upstream:
        def __init__(self) -> None:
            self.paths: list[str] = []

        async def request(self, _request: Any, path: str) -> Response:
            self.paths.append(path)
            return Response(status_code=204)

        async def websocket(self, *_: Any) -> None:
            raise AssertionError("websocket is not expected")

    blocked_upstream = Upstream()
    allowed_upstream = Upstream()
    with TestClient(create_proxy_app(upstream=blocked_upstream, allow_transcoding=False)) as client:
        playlist = client.get("/Videos/item-1/master.m3u8")
        stream = client.get("/Videos/item-1/stream", params={"TranscodeReasons": "VideoCodecNotSupported"})
    with TestClient(create_proxy_app(upstream=allowed_upstream, allow_transcoding=True)) as client:
        allowed = client.get("/Videos/item-1/master.m3u8")

    assert playlist.status_code == 409
    assert stream.status_code == 409
    assert blocked_upstream.paths == []
    assert allowed.status_code == 204
    assert allowed_upstream.paths == ["/Videos/item-1/master.m3u8"]


def test_custom_view_latest_is_handled_and_virtual_playback_info_is_not_item_detail() -> None:
    class Upstream:
        async def request(self, _request: Any, _path: str) -> Response:
            return Response(status_code=204)

        async def websocket(self, *_: Any) -> None:
            raise AssertionError("websocket is not expected")

    settings = Settings()
    emby = FakeEmbyCatalog()
    app = create_proxy_app(
        catalog=_production_catalog(settings, emby),
        upstream=Upstream(),
        server_id="server-1",
    )

    with TestClient(app) as client:
        latest = client.get(
            "/emby/Items/Latest",
            params={"customViewId": "-900042", "UserId": "user-1", "api_key": "caller-token"},
        )
        playback_info = client.get(
            "/emby/Users/user-1/Items/-900042/PlaybackInfo",
            headers={"X-Emby-Token": "caller-token"},
        )

    assert latest.status_code == 200
    assert latest.json() == []
    assert playback_info.status_code == 204


def test_http_upstream_preserves_query_body_range_and_repeated_cookies() -> None:
    seen: list[httpx.Request] = []

    class AsyncBytes(httpx.AsyncByteStream):
        async def __aiter__(self):  # type: ignore[no-untyped-def]
            yield b"data"

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        assert await request.aread() == b"request-body"
        return httpx.Response(
            206,
            headers=[
                ("Content-Type", "application/octet-stream"),
                ("Content-Range", "bytes 0-3/10"),
                ("Set-Cookie", "first=1; Path=/"),
                ("Set-Cookie", "second=2; Path=/"),
            ],
            stream=AsyncBytes(),
        )

    async_client = httpx.AsyncClient(transport=httpx.MockTransport(handler), trust_env=False)
    settings = Settings(emby_server_url="http://emby.internal:8096")
    upstream = HttpxEmbyProxyUpstream(lambda: settings, client=async_client)
    app = create_proxy_app(upstream=upstream)

    with TestClient(app) as client:
        response = client.post(
            "/emby/Items/item-1?value=one&value=two",
            content=b"request-body",
            headers={"Range": "bytes=0-3", "X-Emby-Token": "caller-token"},
        )

    asyncio.run(async_client.aclose())
    assert response.status_code == 206
    assert response.content == b"data"
    assert response.headers["Content-Range"] == "bytes 0-3/10"
    assert response.headers.get_list("Set-Cookie") == ["first=1; Path=/", "second=2; Path=/"]
    assert seen[0].url == "http://emby.internal:8096/emby/Items/item-1?value=one&value=two"
    assert seen[0].headers["Range"] == "bytes=0-3"
    assert seen[0].headers["X-Emby-Token"] == "caller-token"
    assert seen[0].headers["Host"] == "emby.internal:8096"


def test_emby_target_preserves_or_prefixes_configured_base_path() -> None:
    assert _emby_target("http://emby:8096", "/") == "http://emby:8096/"
    assert _emby_target("http://emby:8096/emby", "/Users/1/Views") == (
        "http://emby:8096/emby/Users/1/Views"
    )
    assert _emby_target("http://emby:8096/emby", "/emby/Users/1/Views") == (
        "http://emby:8096/emby/Users/1/Views"
    )
