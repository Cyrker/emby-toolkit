from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from etk.domains.p115.emby_path_gateway import (
    EmbyPathChangeRejected,
    HttpxEmbyPathChangeGateway,
)


def _factory(
    handler: Callable[[httpx.Request], httpx.Response],
) -> Callable[[], httpx.Client]:
    transport = httpx.MockTransport(handler)

    def create() -> httpx.Client:
        return httpx.Client(transport=transport)

    return create


def _gateway(
    handler: Callable[[httpx.Request], httpx.Response],
) -> HttpxEmbyPathChangeGateway:
    return HttpxEmbyPathChangeGateway(client_factory=_factory(handler))


def _create_file(path: Path) -> Path:
    path.parent.mkdir(parents=True)
    path.write_text("fixture", encoding="utf-8")
    return path


def test_deleted_posts_exact_items_contract_and_accepts_204() -> None:
    requests: list[httpx.Request] = []
    paths = ("/media/one.strm", "/media/two.strm")

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.path == "/Library/Media/Updated"
        assert dict(request.url.params) == {"api_key": "fixture-token"}
        assert json.loads(request.content) == {
            "Updates": [
                {"Path": "/media/one.strm", "UpdateType": "Deleted"},
                {"Path": "/media/two.strm", "UpdateType": "Deleted"},
            ]
        }
        return httpx.Response(204)

    delivered = _gateway(handler).notify(
        paths,
        base_url="http://emby.fixture.invalid/",
        api_key="fixture-token",
        update_type="Deleted",
    )

    assert delivered is True
    assert len(requests) == 1


def test_deleted_4xx_is_not_confirmed() -> None:
    gateway = _gateway(lambda _request: httpx.Response(409))

    with pytest.raises(EmbyPathChangeRejected, match="HTTP 409"):
        gateway.notify(
            ("/media/deleted.strm",),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Deleted",
        )


def test_deleted_5xx_raises_status_error() -> None:
    gateway = _gateway(lambda _request: httpx.Response(503))

    with pytest.raises(httpx.HTTPStatusError) as raised:
        gateway.notify(
            ("/media/deleted.strm",),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Deleted",
        )

    assert raised.value.response.status_code == 503


def test_deleted_transport_error_propagates_unchanged() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("fixture unavailable", request=request)

    gateway = _gateway(handler)

    with pytest.raises(httpx.ConnectError, match="fixture unavailable"):
        gateway.notify(
            ("/media/deleted.strm",),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Deleted",
        )


@pytest.mark.parametrize("refresh_status", [200, 204])
def test_modified_refreshes_nearest_exact_parent(
    tmp_path: Path,
    refresh_status: int,
) -> None:
    nearest_parent = tmp_path / "library" / "show"
    media_path = _create_file(nearest_parent / "season" / "episode.strm")
    requests: list[httpx.Request] = []
    emby_parent_path = f"  {str(nearest_parent).replace('/', chr(92))}///  "

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            assert request.url.path == "/Items"
            query = dict(request.url.params)
            assert query == {
                "api_key": "fixture-token",
                "Recursive": "true",
                "Path": query["Path"],
                "Fields": "Id,Path,Name",
            }
            if query["Path"] == str(media_path.parent):
                return httpx.Response(
                    200,
                    json={"Items": [{"Id": "similar", "Path": f"{media_path.parent}-other"}]},
                )
            assert query["Path"] == str(nearest_parent)
            return httpx.Response(
                200,
                json={"Items": [{"Id": "folder/id ?", "Path": emby_parent_path}]},
            )

        assert request.method == "POST"
        assert request.url.raw_path.split(b"?", 1)[0] == b"/Items/folder%2Fid%20%3F/Refresh"
        assert dict(request.url.params) == {
            "api_key": "fixture-token",
            "Recursive": "true",
            "MetadataRefreshMode": "Default",
            "ImageRefreshMode": "Default",
            "ReplaceAllImages": "false",
            "ReplaceAllMetadata": "false",
        }
        return httpx.Response(refresh_status)

    delivered = _gateway(handler).notify(
        (str(media_path),),
        base_url="http://emby.fixture.invalid",
        api_key="fixture-token",
        update_type="modified",
    )

    assert delivered is True
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/Items"),
        ("GET", "/Items"),
        ("POST", "/Items/folder/id ?/Refresh"),
    ]


def test_modified_skips_missing_local_directory_before_querying_parent(tmp_path: Path) -> None:
    existing_parent = tmp_path / "library"
    existing_parent.mkdir()
    media_path = existing_parent / "missing" / "episode.strm"
    queried_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            queried_paths.append(request.url.params["Path"])
            return httpx.Response(
                200,
                json={"Items": [{"Id": "library-1", "Path": str(existing_parent)}]},
            )
        return httpx.Response(204)

    delivered = _gateway(handler).notify(
        (str(media_path),),
        base_url="http://emby.fixture.invalid",
        api_key="fixture-token",
        update_type="Modified",
    )

    assert delivered is True
    assert queried_paths == [str(existing_parent)]


def test_modified_refresh_4xx_is_not_confirmed(tmp_path: Path) -> None:
    media_path = _create_file(tmp_path / "library" / "movie.strm")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(
                200,
                json={"Items": [{"Id": "library-1", "Path": str(media_path.parent)}]},
            )
        return httpx.Response(409)

    with pytest.raises(EmbyPathChangeRejected, match="HTTP 409"):
        _gateway(handler).notify(
            (str(media_path),),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Modified",
        )


@pytest.mark.parametrize("failure_stage", ["lookup", "refresh"])
def test_modified_5xx_raises_status_error(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    media_path = _create_file(tmp_path / "library" / "movie.strm")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            if failure_stage == "lookup":
                return httpx.Response(503)
            return httpx.Response(
                200,
                json={"Items": [{"Id": "library-1", "Path": str(media_path.parent)}]},
            )
        return httpx.Response(502)

    with pytest.raises(httpx.HTTPStatusError) as raised:
        _gateway(handler).notify(
            (str(media_path),),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Modified",
        )

    assert raised.value.response.status_code == (503 if failure_stage == "lookup" else 502)


@pytest.mark.parametrize("payload", [[], {"Items": {}}])
def test_modified_malformed_items_response_raises(
    tmp_path: Path,
    payload: Any,
) -> None:
    media_path = _create_file(tmp_path / "library" / "movie.strm")
    gateway = _gateway(lambda _request: httpx.Response(200, json=payload))

    with pytest.raises(ValueError):
        gateway.notify(
            (str(media_path),),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Modified",
        )


def test_modified_stops_after_twelve_unmatched_parent_lookups(tmp_path: Path) -> None:
    current = tmp_path
    for index in range(13):
        current /= f"level-{index}"
    media_path = _create_file(current / "movie.strm")
    queried_paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        queried_paths.append(request.url.params["Path"])
        return httpx.Response(200, json={"Items": []})

    with pytest.raises(EmbyPathChangeRejected, match="no matching parent"):
        _gateway(handler).notify(
            (str(media_path),),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Modified",
        )
    assert len(queried_paths) == 12
    assert queried_paths[0] == str(media_path.parent)


def test_modified_requires_every_path_to_be_confirmed(tmp_path: Path) -> None:
    first_path = _create_file(tmp_path / "first" / "one.strm")
    second_path = _create_file(tmp_path / "second" / "two.strm")
    third_path = _create_file(tmp_path / "third" / "three.strm")
    requests: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append((request.method, request.url.params.get("Path", "")))
        if request.method == "POST":
            return httpx.Response(204)
        requested_path = request.url.params["Path"]
        if requested_path in {str(first_path.parent), str(third_path.parent)}:
            return httpx.Response(
                200,
                json={"Items": [{"Id": Path(requested_path).name, "Path": requested_path}]},
            )
        return httpx.Response(404)

    with pytest.raises(EmbyPathChangeRejected, match="HTTP 404"):
        _gateway(handler).notify(
            (str(first_path), str(second_path), str(third_path)),
            base_url="http://emby.fixture.invalid",
            api_key="fixture-token",
            update_type="Modified",
        )
    assert requests == [
        ("GET", str(first_path.parent)),
        ("POST", ""),
        ("GET", str(second_path.parent)),
        ("GET", str(third_path.parent)),
        ("POST", ""),
    ]


def test_modified_unconfirmed_result_takes_precedence_over_a_mixed_4xx_rejection(
    tmp_path: Path,
) -> None:
    unconfirmed_path = _create_file(tmp_path / "unconfirmed" / "one.strm")
    rejected_path = _create_file(tmp_path / "rejected" / "two.strm")
    queried: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_path = request.url.params["Path"]
        queried.append(requested_path)
        if requested_path == str(unconfirmed_path.parent):
            return httpx.Response(204)
        return httpx.Response(404)

    delivered = _gateway(handler).notify(
        (str(unconfirmed_path), str(rejected_path)),
        base_url="http://emby.fixture.invalid",
        api_key="fixture-token",
        update_type="Modified",
    )

    assert delivered is False
    assert queried == [str(unconfirmed_path.parent), str(rejected_path.parent)]


def test_empty_paths_and_invalid_arguments_do_not_open_client() -> None:
    def create_client() -> httpx.Client:
        raise AssertionError("client must not be opened")

    gateway = HttpxEmbyPathChangeGateway(client_factory=create_client)

    assert gateway.notify((), base_url="", api_key="", update_type="Created") is True
    invalid = [
        {"paths": ("/one.strm",), "base_url": "", "api_key": "token", "update_type": "Modified"},
        {"paths": ("/one.strm",), "base_url": "http://emby", "api_key": "", "update_type": "Modified"},
        {"paths": ("/one.strm",), "base_url": "http://emby", "api_key": "token", "update_type": "Created"},
        {"paths": ("",), "base_url": "http://emby", "api_key": "token", "update_type": "Modified"},
    ]
    for values in invalid:
        with pytest.raises(EmbyPathChangeRejected, match="configuration is invalid"):
            gateway.notify(**values)  # type: ignore[arg-type]
