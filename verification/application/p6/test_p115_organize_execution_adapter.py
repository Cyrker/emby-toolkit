from __future__ import annotations

from typing import Any

import pytest

from etk.domains.p115.adapters import (
    LegacyP115OrganizeMutationGateway,
    _LegacyP115OrganizeMutationClient,
)
from handler import p115_service


class FixedBackendClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def fs_files_backend(self, payload: dict[str, Any], backend: str):
        self.calls.append(("fs_files_backend", (dict(payload), backend)))
        if int(payload["offset"]) == 0:
            return {
                "state": True,
                "count": 2,
                "data": [
                    {"fid": "dir-1", "fn": "Movie", "fc": 0},
                    {
                        "fid": "file-1",
                        "fn": "Movie.mkv",
                        "fc": 1,
                        "sha": "A" * 40,
                        "pc": "pc-file-1",
                        "fs": 1024,
                    },
                ],
                "_etk_api_label_lower": backend,
                "_etk_authoritative_list_payload": True,
            }
        return {
            "state": True,
            "count": 2,
            "data": [],
            "_etk_api_label_lower": backend,
            "_etk_authoritative_list_payload": True,
        }

    def fs_mkdir_backend(self, name: str, parent_id: str, backend: str):
        self.calls.append(("fs_mkdir_backend", (name, parent_id, backend)))
        return {"state": True, "data": {"file_id": "dir-1"}}

    def fs_move_backend(self, file_ids: list[str], parent_id: str, backend: str):
        self.calls.append(("fs_move_backend", (file_ids, parent_id, backend)))
        return {"state": True}

    def fs_rename_backend(self, pair: tuple[str, str], backend: str):
        self.calls.append(("fs_rename_backend", (pair, backend)))
        return {"state": True}

    def p115_organize_backends(self) -> tuple[str, ...]:
        return ("cookie", "openapi")

    def fs_mkdir(self, *_args: Any):
        raise AssertionError("combined fs_mkdir must never be used")

    def fs_move(self, *_args: Any):
        raise AssertionError("combined fs_move must never be used")

    def fs_rename(self, *_args: Any):
        raise AssertionError("combined fs_rename must never be used")


def test_fixed_backend_adapter_uses_one_backend_for_full_readback_and_mutations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    raw = FixedBackendClient()
    client = _LegacyP115OrganizeMutationClient(raw, "115001", "openapi")

    entries = client.list_children("target")
    assert [(entry.id, entry.entry_kind) for entry in entries] == [
        ("dir-1", "directory"),
        ("file-1", "file"),
    ]
    assert client.ensure_directory("target", "Movie")["state"] is True
    assert client.move("file-1", "dir-1")["state"] is True
    assert client.rename("file-1", "Renamed.mkv")["state"] is True
    assert [name for name, _ in raw.calls] == [
        "fs_files_backend",
        "fs_files_backend",
        "fs_mkdir_backend",
        "fs_move_backend",
        "fs_rename_backend",
    ]
    assert all(
        args[-1] == "openapi"
        for name, args in raw.calls
        if name in {"fs_mkdir_backend", "fs_move_backend", "fs_rename_backend"}
    )


def test_gateway_pins_explicit_backend_instead_of_falling_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = FixedBackendClient()
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")
    monkeypatch.setattr(
        p115_service.P115Service,
        "get_client",
        lambda *, expected_account_id=None: raw if expected_account_id == "115001" else None,
    )
    gateway = LegacyP115OrganizeMutationGateway()

    selected = gateway.open(expected_account_id="115001")
    pinned = gateway.open(expected_account_id="115001", expected_backend="openapi")
    unavailable = gateway.open(expected_account_id="115001", expected_backend="missing")

    assert selected is not None and selected.backend == "cookie"
    assert pinned is not None and pinned.backend == "openapi"
    assert unavailable is None


def test_account_drift_before_or_after_a_fixed_backend_call_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_account = ["115001"]
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: current_account[0])
    raw = FixedBackendClient()
    client = _LegacyP115OrganizeMutationClient(raw, "115001", "openapi")

    current_account[0] = "115002"
    with pytest.raises(RuntimeError, match="账号在整理执行期间发生变化"):
        client.move("file-1", "target")
    assert raw.calls == []

    current_account[0] = "115001"

    def drift_after_response(file_ids: list[str], parent_id: str, backend: str):
        raw.calls.append(("fs_move_backend", (file_ids, parent_id, backend)))
        current_account[0] = "115002"
        return {"state": True}

    raw.fs_move_backend = drift_after_response  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="请求返回后发生变化"):
        client.move("file-1", "target")


def test_incomplete_or_backend_drifting_readback_is_never_authoritative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(p115_service, "get_115_account_id", lambda: "115001")

    class Incomplete(FixedBackendClient):
        def fs_files_backend(self, payload: dict[str, Any], backend: str):
            return {
                "state": True,
                "count": 2,
                "data": [] if int(payload["offset"]) else [{"fid": "dir-1", "fn": "Movie", "fc": 0}],
                "_etk_api_label_lower": "cookie",
                "_etk_authoritative_list_payload": True,
            }

    client = _LegacyP115OrganizeMutationClient(Incomplete(), "115001", "openapi")
    with pytest.raises(RuntimeError, match="固定 mutation 后端不一致"):
        client.list_children("target")
