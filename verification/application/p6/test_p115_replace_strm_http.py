from __future__ import annotations

import json
import re
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115 import AtomicP115LocalStrmLibraryGateway, P115StrmReplaceService
from etk.domains.p115.router import router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = REPO_ROOT / "verification/fixtures/http/p6-p115-replace-strm.json"
FIXTURE: Mapping[str, Any] = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
CASES: tuple[Mapping[str, Any], ...] = tuple(FIXTURE["cases"])


class FakePermissions:
    def is_admin(self, user_id: str) -> bool:
        return user_id == "fixture-admin"


class RecordingCoordinator:
    def __init__(self) -> None:
        self.scopes: list[str] = []

    @contextmanager
    def hold(self, scope: str) -> Iterator[None]:
        self.scopes.append(scope)
        yield


class PartialFailureLibrary:
    def __init__(
        self,
        delegate: Any,
        root: Path,
        failure: Mapping[str, Any],
        partial_writes: list[str],
    ) -> None:
        self._delegate = delegate
        self._root = root
        self._failure = failure
        self._partial_writes = partial_writes

    def replace_strm(self, *, mode: str, search: str, replacement: str) -> tuple[int, int]:
        relative_path = str(self._failure["after_path"])
        target = self._root.joinpath(*relative_path.split("/"))
        content = target.read_text(encoding="utf-8").strip()
        if mode == "plain":
            new_content = content.replace(search, replacement)
        elif mode == "regex":
            new_content = re.sub(search, replacement, content)
        else:
            new_content = content
        self._delegate.write_strm(relative_path, new_content)
        self._partial_writes.append(relative_path)
        raise OSError(str(self._failure["message"]))


class RecordingLibraryGateway:
    def __init__(self, root: Path, spec: Mapping[str, Any]) -> None:
        self._root = root
        self._spec = spec
        self.open_calls: list[str] = []
        self.partial_writes: list[str] = []

    def open(self, root: str) -> Any | None:
        self.open_calls.append(root)
        if not bool(self._spec.get("available", True)):
            return None
        delegate = AtomicP115LocalStrmLibraryGateway().open(str(self._root))
        assert delegate is not None
        failure = self._spec.get("failure")
        if isinstance(failure, Mapping):
            return PartialFailureLibrary(delegate, self._root, failure, self.partial_writes)
        return delegate


def _build_client(
    case: Mapping[str, Any],
    tmp_path: Path,
) -> tuple[TestClient, RecordingLibraryGateway, RecordingCoordinator, Path, AppContainer]:
    root = tmp_path / str(case["case_id"])
    root.mkdir()
    library_spec = case["library"]
    assert isinstance(library_spec, Mapping)
    files = library_spec.get("files") or {}
    assert isinstance(files, Mapping)
    for relative_path, content in files.items():
        target = root.joinpath(*str(relative_path).split("/"))
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(str(content), encoding="utf-8")

    gateway = RecordingLibraryGateway(root, library_spec)
    coordinator = RecordingCoordinator()
    container = AppContainer(
        Settings.model_validate(
            {
                "auth_enabled": bool(case.get("auth_enabled", False)),
                "local_strm_root": str(root),
            }
        ),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    service = P115StrmReplaceService(
        settings=lambda: container.settings,
        libraries=gateway,
        coordinator=coordinator,
    )
    app = FastAPI()
    app.state.container = container
    app.state.p115_strm_replace_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-replace-strm-http-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app), gateway, coordinator, root, container


def _request(client: TestClient, request: Mapping[str, Any]) -> Any:
    method = str(request["method"])
    path = str(request["path"])
    if "json" in request:
        return client.request(method, path, json=request["json"])
    headers = {"content-type": str(request.get("content_type") or "application/json")}
    return client.request(method, path, content=str(request.get("raw_body") or ""), headers=headers)


def _strm_files(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8") for path in sorted(root.rglob("*.strm"))
    }


def _assert_response(response: Any, expected: Mapping[str, Any]) -> None:
    assert response.status_code != 422
    assert response.status_code == expected["status"]
    if "json" in expected:
        assert response.json() == expected["json"]
        return
    if "json_subset" in expected:
        body = response.json()
        assert all(body.get(key) == value for key, value in expected["json_subset"].items())
        assert str(body.get("message") or "").startswith(str(expected["message_prefix"]))
        return
    assert response.headers["content-type"].startswith(str(expected["content_type_prefix"]))
    assert str(expected["body_contains"]) in response.text


def test_fixture_defines_the_p65_replace_strm_contract_and_intentional_variance() -> None:
    assert FIXTURE["schema_version"] == 1
    assert FIXTURE["captured_live"] is False
    assert FIXTURE["route"] == {"method": "POST", "path": "/api/p115/replace_strm"}
    assert {case["case_id"] for case in CASES} == {
        "admin-denial-precedes-malformed-json",
        "plain-replacement",
        "regex-replacement",
        "malformed-json",
        "missing-json-body",
        "non-object-json",
        "bad-regex",
        "unknown-mode-legacy-compatible-no-op",
        "empty-mode-legacy-compatible-no-op",
        "null-mode-legacy-compatible-no-op",
        "uppercase-mode-legacy-compatible-no-op",
        "null-replacement-legacy-compatible-no-op",
        "local-root-unavailable",
        "partial-file-failure-is-native-fail-closed",
    }
    notes = " ".join(str(note) for note in FIXTURE["compatibility_notes"])
    assert "legacy HTTP 200 success/no-op" in notes
    assert "never coerced into destructive empty-string replacement" in notes
    assert "Native fails closed with HTTP 500" in notes


@pytest.mark.parametrize("case", CASES, ids=[str(case["case_id"]) for case in CASES])
def test_native_replace_strm_http_contract(
    case: Mapping[str, Any],
    tmp_path: Path,
) -> None:
    client, gateway, coordinator, root, container = _build_client(case, tmp_path)
    request = case["request"]
    expected = case["expected"]
    assert isinstance(request, Mapping)
    assert isinstance(expected, Mapping)
    try:
        with client:
            response = _request(client, request)
    finally:
        container.close()

    _assert_response(response, expected)
    assert _strm_files(root) == expected["files"]
    assert len(gateway.open_calls) == expected["library_open_calls"]
    assert coordinator.scopes == expected["lock_scopes"]
    assert gateway.partial_writes == expected.get("partial_writes", [])
