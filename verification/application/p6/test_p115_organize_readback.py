from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import httpx
import pytest

from etk.domains.p115 import (
    HttpxP115OrganizeReadbackGateway,
    P115CredentialSnapshot,
    P115DirectoryCredentialChanged,
    P115OrganizeReadbackMutationForbidden,
)


class FakeCredentials:
    def __init__(self, snapshots: Sequence[P115CredentialSnapshot]) -> None:
        self._snapshots = list(snapshots)
        self.calls = 0

    def load(self) -> P115CredentialSnapshot:
        index = min(self.calls, len(self._snapshots) - 1)
        self.calls += 1
        return self._snapshots[index]

    def save_openapi(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("organize readback must not refresh credentials")

    def save_cookie(self, *_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("organize readback must not save credentials")

    def app_label(self, app_type: str) -> str:
        return app_type


def _snapshot(
    *,
    account_id: str = "115001",
    access_token: str = "fixture-openapi-token",
    cookie: str = "UID=115001_fixture; CID=fixture; KID=fixture; SEID=fixture",
) -> P115CredentialSnapshot:
    return P115CredentialSnapshot(
        access_token=access_token,
        cookie=cookie,
        app_type="web",
        account_id=account_id,
    )


def _client_factory(transport: httpx.BaseTransport) -> Callable[..., httpx.Client]:
    return lambda **kwargs: httpx.Client(transport=transport, **kwargs)


def test_readback_uses_only_the_approved_backend_and_normalizes_all_children() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        offset = int(request.url.params.get("offset") or 0)
        data = (
            [
                {"file_id": "dir-1", "file_name": "Movie", "type": 0},
                {
                    "file_id": "file-1",
                    "file_name": "Movie.mkv",
                    "type": 1,
                    "sha1": "a" * 40,
                    "pick_code": "pc-file-1",
                    "size": 1024,
                },
            ]
            if offset == 0
            else []
        )
        return httpx.Response(200, json={"state": True, "count": 2, "data": data})

    gateway = HttpxP115OrganizeReadbackGateway(
        credentials=FakeCredentials([_snapshot()]),
        priority=lambda: "cookie",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001", expected_backend="openapi")
    assert client is not None

    entries = client.list_children("target")

    assert client.backend == "openapi"
    assert [(entry.id, entry.parent_id, entry.name, entry.entry_kind) for entry in entries] == [
        ("dir-1", "target", "Movie", "directory"),
        ("file-1", "target", "Movie.mkv", "file"),
    ]
    assert entries[1].sha1 == "A" * 40
    assert entries[1].pick_code == "pc-file-1"
    assert entries[1].size == 1024
    assert entries[1].raw_hash
    assert [request.url.params["offset"] for request in requests] == ["0", "2"]
    assert all(request.url.host == "proapi.115.com" for request in requests)
    assert all(request.headers["authorization"] == "Bearer fixture-openapi-token" for request in requests)
    assert all("cookie" not in request.headers for request in requests)

    with pytest.raises(P115OrganizeReadbackMutationForbidden):
        client.ensure_directory("target", "Forbidden")
    with pytest.raises(P115OrganizeReadbackMutationForbidden):
        client.move("file-1", "target-2")
    with pytest.raises(P115OrganizeReadbackMutationForbidden):
        client.rename("file-1", "Forbidden.mkv")


def test_readback_never_falls_back_from_the_approved_backend() -> None:
    hosts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        hosts.append(str(request.url.host))
        if request.url.host == "proapi.115.com":
            return httpx.Response(503, json={"state": False, "error": "fixture unavailable"})
        return httpx.Response(200, json={"state": True, "data": []})

    gateway = HttpxP115OrganizeReadbackGateway(
        credentials=FakeCredentials([_snapshot()]),
        priority=lambda: "cookie",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001", expected_backend="openapi")
    assert client is not None

    with pytest.raises(httpx.HTTPStatusError):
        client.list_children("target")

    assert hosts == ["proapi.115.com"]


def test_readback_requires_the_exact_backend_credential_without_network_access() -> None:
    gateway = HttpxP115OrganizeReadbackGateway(
        credentials=FakeCredentials([_snapshot(access_token="")]),
        priority=lambda: "cookie",
        client_factory=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("network client constructed")),
    )

    assert gateway.open(expected_account_id="115001", expected_backend="openapi") is None
    assert gateway.open(expected_account_id="115001", expected_backend=None) is None
    assert gateway.open(expected_account_id="115001", expected_backend="invalid") is None


def test_credential_drift_after_readback_fails_closed() -> None:
    requests: list[httpx.Request] = []
    gateway = HttpxP115OrganizeReadbackGateway(
        credentials=FakeCredentials(
            [
                _snapshot(),
                _snapshot(),
                _snapshot(access_token="replacement-token"),
            ]
        ),
        priority=lambda: "openapi",
        client_factory=_client_factory(
            httpx.MockTransport(
                lambda request: (
                    requests.append(request) or httpx.Response(200, json={"state": True, "count": 0, "data": []})
                )
            )
        ),
    )
    client = gateway.open(expected_account_id="115001", expected_backend="openapi")
    assert client is not None

    with pytest.raises(P115DirectoryCredentialChanged, match="凭据"):
        client.list_children("target")

    assert len(requests) == 1


@pytest.mark.parametrize(
    ("responses", "error"),
    [
        (
            {
                0: {"state": True, "count": 2, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]},
                1: {"state": True, "count": 2, "data": []},
            },
            "分页不完整",
        ),
        (
            {
                0: {"state": True, "count": 1, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]},
                1: {"state": True, "count": 2, "data": []},
            },
            "分页总数发生变化",
        ),
        (
            {
                0: {"state": True, "count": 2, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]},
                1: {"state": True, "count": 2, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]},
            },
            "重复返回相同页面",
        ),
        (
            {
                0: {"state": True, "count": 2, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]},
                1: {"state": True, "count": 2, "data": [{"file_id": "dir-1", "file_name": "Two", "type": 0}]},
            },
            "重复返回项目 ID",
        ),
        (
            {0: {"state": True, "count": 1, "data": [{"file_id": "dir-without-name", "type": 0}]}},
            "缺少 ID 或名称",
        ),
        (
            {0: {"state": True, "count": 0, "data": [{"file_id": "dir-1", "file_name": "One", "type": 0}]}},
            "数量超过 provider count",
        ),
    ],
)
def test_readback_rejects_incomplete_or_ambiguous_authoritative_pages(
    responses: dict[int, dict[str, Any]],
    error: str,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset") or 0)
        return httpx.Response(200, json=responses.get(offset, {"state": True, "count": 0, "data": []}))

    gateway = HttpxP115OrganizeReadbackGateway(
        credentials=FakeCredentials([_snapshot()]),
        priority=lambda: "openapi",
        client_factory=_client_factory(httpx.MockTransport(handler)),
    )
    client = gateway.open(expected_account_id="115001", expected_backend="openapi")
    assert client is not None

    with pytest.raises(RuntimeError, match=error):
        client.list_children("target")
