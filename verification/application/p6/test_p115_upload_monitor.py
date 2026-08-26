from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.p115.router import router
from etk.domains.p115.upload_monitor import (
    P115UploadMonitorConfigService,
    P115UploadMonitorIngressService,
    UploadedSource,
    UploadFileFingerprint,
    UploadMonitorConfigRevisionChanged,
    UploadMonitorControl,
    UploadReservation,
    matching_upload_mapping,
    normalize_upload_monitor_config,
    upload_monitor_config_revision,
)
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE = json.loads((REPO_ROOT / "verification/fixtures/http/p6-p115-upload-monitor.json").read_text(encoding="utf-8"))
NOW = datetime(2026, 8, 26, 1, 2, 3, tzinfo=UTC)


class FakePermissions:
    def is_admin(self, _user_id: str) -> bool:
        return True


class FakeRepository:
    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self.config = config
        self.saved: list[tuple[dict[str, Any], str]] = []
        self.control_value: UploadMonitorControl | None = None
        self.operations: dict[str, Any] = {}
        self.sources: dict[tuple[str, str], UploadedSource] = {}

    def load_config(self):
        return self.config

    def ensure_config(self, config, revision):
        if self.control_value is None:
            return self.save_config(config, revision)
        return self.control_value

    def save_config(self, config, revision):
        copied = dict(config)
        self.config = copied
        self.saved.append((copied, revision))
        generation = (self.control_value.generation + 1) if self.control_value is not None else 1
        self.control_value = UploadMonitorControl(revision, None, generation, NOW, None)
        return self.control_value

    def control(self):
        return self.control_value

    def apply_config(self, revision):
        generation = self.control_value.generation if self.control_value is not None else 0
        self.control_value = UploadMonitorControl(revision, revision, generation, NOW, NOW)
        return self.control_value

    def reserve(self, intent):
        existing = self.operations.get(intent.operation_key)
        if existing is not None:
            return UploadReservation(existing, created=False)
        self.operations[intent.operation_key] = intent
        return UploadReservation(cast(Any, intent), created=True)

    def read(self, operation_id: UUID):
        return next(
            (operation for operation in self.operations.values() if operation.id == operation_id),
            None,
        )

    def source_by_path(self, mapping_id: str, path_key: str):
        return self.sources.get((mapping_id, path_key))

    def status(self):
        return {"pending": 0, "uploading": 0, "failed": 0, "completed": 0, "recent": []}


def _client(repository: FakeRepository, *, default_extensions: tuple[str, ...] = (".mkv",)) -> TestClient:
    container = AppContainer(
        Settings(auth_enabled=False),
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    service = P115UploadMonitorConfigService(repository, lambda: default_extensions)
    app = FastAPI()
    app.state.container = container
    app.state.p115_upload_monitor_service = service
    codec = LegacySessionCodec.from_secret_key(b"p6-upload-monitor-session")
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(router)
    return TestClient(app)


def test_http_fixture_declares_native_upload_monitor_contract() -> None:
    assert {(entry["method"], entry["path"], entry["auth"]) for entry in FIXTURE["routes"]} == {
        ("GET", "/api/p115/upload_monitor/config", "admin"),
        ("POST", "/api/p115/upload_monitor/config", "admin"),
    }
    app = FastAPI()
    app.include_router(router)
    route = app.openapi()["paths"]["/api/p115/upload_monitor/config"]
    assert set(route) >= {"get", "post"}
    assert {int(value) for value in route["post"]["responses"]} == {200, 400, 403, 500}


def test_get_default_upload_monitor_config_matches_fixture() -> None:
    case = next(value for value in FIXTURE["cases"] if value["case_id"] == "get-default")
    repository = FakeRepository()
    with _client(repository) as client:
        response = client.get(case["request"]["path"])
    assert response.status_code == case["expected"]["status"]
    assert response.json() == case["expected"]["body"]
    assert repository.saved == []


def test_post_persists_revision_without_claiming_cross_process_restart(tmp_path: Path) -> None:
    case = next(value for value in FIXTURE["cases"] if value["case_id"] == "save-valid")
    body = json.loads(json.dumps(case["request"]["body"]).replace("$LOCAL_DIR", str(tmp_path)))
    repository = FakeRepository()
    with _client(repository) as client:
        response = client.post(case["request"]["path"], json=body)

    assert response.status_code == case["expected"]["status"]
    payload = response.json()
    assert payload["message"] == case["expected"]["message"]
    assert payload["monitor_restarted"] is case["expected"]["monitor_restarted"]
    assert payload["data"]["mappings"][0]["extensions"] == [".mkv"]
    assert repository.saved[0][0] == payload["data"]
    assert repository.control_value is not None and repository.control_value.applied is False


def test_post_preserves_exact_validation_messages(tmp_path: Path) -> None:
    empty = next(value for value in FIXTURE["cases"] if value["case_id"] == "save-enabled-empty")
    with _client(FakeRepository()) as client:
        response = client.post(empty["request"]["path"], json=empty["request"]["body"])
        invalid = client.post(
            empty["request"]["path"],
            json={
                "enabled": True,
                "mappings": [{"local_dir": str(tmp_path / "missing"), "target_cid": "9001"}],
            },
        )
        too_many = client.post(
            empty["request"]["path"],
            json={"enabled": True, "mappings": [{} for _ in range(51)]},
        )
    assert response.status_code == empty["expected"]["status"]
    assert response.json() == empty["expected"]["body"]
    assert invalid.status_code == 400
    assert invalid.json()["message"] == "第 1 组本地目录不存在或不可访问"
    assert too_many.status_code == 400
    assert too_many.json()["message"] == "上传监控目录最多配置 50 组"


def test_normalization_preserves_extension_fallback_empty_override_and_longest_root(tmp_path: Path) -> None:
    nested = tmp_path / "nested"
    nested.mkdir()
    config = normalize_upload_monitor_config(
        {
            "enabled": "true",
            "mappings": [
                {"local_dir": str(tmp_path), "target_cid": "1"},
                {
                    "local_dir": str(nested),
                    "target_cid": "2",
                    "extensions": [],
                    "mode": "invalid",
                },
            ],
        },
        default_extensions=("mkv", ".strm"),
    )
    assert config["enabled"] is True
    assert config["mappings"][0]["extensions"] == [".mkv"]
    assert config["mappings"][1]["extensions"] == []
    assert config["mappings"][1]["mode"] == "keep"
    matching = matching_upload_mapping(nested / "Movie.mkv", config["mappings"])
    assert matching is not None and matching["target_cid"] == "2"


@pytest.mark.parametrize(
    ("mapping_mode", "source_mode"),
    [("delete", "keep"), ("keep", "delete")],
)
def test_delete_ingress_requires_current_mapping_and_completed_source_to_be_keep_mode(
    tmp_path: Path,
    mapping_mode: str,
    source_mode: str,
) -> None:
    path = tmp_path / "Movie.mkv"
    path.write_bytes(b"fixture")
    raw_config = {
        "enabled": True,
        "mappings": [
            {
                "enabled": True,
                "local_dir": str(tmp_path),
                "target_cid": "9001",
                "mode": mapping_mode,
                "extensions": [".mkv"],
            }
        ],
    }
    normalized = normalize_upload_monitor_config(raw_config, default_extensions=(".mkv",))
    revision = upload_monitor_config_revision(normalized)
    mapping_id = str(normalized["mappings"][0]["id"])
    path_key = os.path.normcase(os.path.abspath(path))
    state = path.stat()
    source = UploadedSource(
        record_key=f"{mapping_id}:{path_key}",
        operation_id=UUID("00000000-0000-0000-0000-000000000820"),
        mapping_id=mapping_id,
        config_revision=revision,
        local_path=str(path),
        path_key=path_key,
        relative_path=path.name,
        target_cid="9001",
        mode=source_mode,
        fingerprint=UploadFileFingerprint(state.st_size, state.st_mtime_ns, state.st_dev, state.st_ino),
        remote_receipt={
            "fid": "500",
            "pick_code": "pick-mode-fence",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
        derived_paths=(),
        derived_ownership={},
        completed_at=NOW,
        operation_sequence=1,
    )
    repository = FakeRepository(raw_config)
    repository.sources[(mapping_id, path_key)] = source
    ingress = P115UploadMonitorIngressService(
        config=P115UploadMonitorConfigService(repository, lambda: (".mkv",)),
        repository=repository,
        clock=lambda: NOW,
    )

    assert ingress.record_event("deleted", str(path)) == ()
    assert repository.operations == {}


def test_ingress_deduplicates_upserts_rejects_symlinks_and_correlates_moves(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    source_root.mkdir()
    source_path = source_root / "Old.mkv"
    destination = source_root / "New.mkv"
    source_path.write_bytes(b"old")
    destination.write_bytes(b"new")
    repository = FakeRepository(
        {
            "enabled": True,
            "mappings": [
                {
                    "enabled": True,
                    "local_dir": str(source_root),
                    "target_cid": "9001",
                    "target_name": "Fixture",
                    "mode": "keep",
                    "extensions": [".mkv"],
                }
            ],
        }
    )
    config = P115UploadMonitorConfigService(repository, lambda: (".mkv",))
    _snapshot, revision = config.snapshot()
    repository.control_value = UploadMonitorControl(revision, revision, 1, NOW, NOW)
    ingress = P115UploadMonitorIngressService(config=config, repository=repository, clock=lambda: NOW)

    first = ingress.record_event("created", str(destination), source_event_key="fixture-event")
    replay = ingress.record_event("created", str(destination), source_event_key="fixture-event")
    assert first[0].created is True
    assert replay[0].created is False
    assert first[0].operation.id == replay[0].operation.id

    mapping_id = config.get()["mappings"][0]["id"]
    path_key = os.path.normcase(os.path.abspath(source_path))
    fingerprint = UploadFileFingerprint.from_mapping(
        {
            "size": source_path.stat().st_size,
            "mtime_ns": source_path.stat().st_mtime_ns,
            "device": source_path.stat().st_dev,
            "inode": source_path.stat().st_ino,
        }
    )
    completed = UploadedSource(
        record_key=f"{mapping_id}:{path_key}",
        operation_id=UUID("00000000-0000-0000-0000-000000000801"),
        mapping_id=mapping_id,
        config_revision=revision,
        local_path=str(source_path),
        path_key=path_key,
        relative_path="Old.mkv",
        target_cid="9001",
        mode="keep",
        fingerprint=fingerprint,
        remote_receipt={"fid": "1", "pick_code": "pc", "parent_id": "9001"},
        derived_paths=(),
        derived_ownership={},
        completed_at=NOW,
    )
    repository.sources[(mapping_id, path_key)] = completed
    moved = ingress.record_event("moved", str(destination), related_path=str(source_path))
    assert len(moved) == 2
    assert moved[0].operation.correlation_id == moved[1].operation.correlation_id

    outside = tmp_path / "outside.mkv"
    outside.write_bytes(b"outside")
    link = source_root / "Link.mkv"
    link.symlink_to(outside)
    assert ingress.record_event("created", str(link)) == ()


def test_ingress_reloads_config_and_completed_source_after_one_reservation_race(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    path = root / "Movie.mkv"
    path.write_bytes(b"fixture")
    initial_raw = {
        "enabled": True,
        "mappings": [
            {
                "enabled": True,
                "local_dir": str(root),
                "target_cid": "9001",
                "target_name": "Initial",
                "mode": "keep",
                "extensions": [".mkv"],
            }
        ],
    }
    revised_raw = {
        "enabled": True,
        "mappings": [
            {
                "enabled": True,
                "local_dir": str(root),
                "target_cid": "9001",
                "target_name": "Revised",
                "mode": "keep",
                "extensions": [".mkv"],
            }
        ],
    }
    initial = normalize_upload_monitor_config(initial_raw, default_extensions=(".mkv",))
    revised = normalize_upload_monitor_config(revised_raw, default_extensions=(".mkv",))
    initial_revision = upload_monitor_config_revision(initial)
    revised_revision = upload_monitor_config_revision(revised)
    mapping_id = str(initial["mappings"][0]["id"])
    assert revised["mappings"][0]["id"] == mapping_id
    path_key = os.path.normcase(os.path.abspath(path))
    fingerprint = UploadFileFingerprint(
        size=path.stat().st_size,
        mtime_ns=path.stat().st_mtime_ns,
        device=path.stat().st_dev,
        inode=path.stat().st_ino,
    )
    source_a = UploadedSource(
        record_key=f"{mapping_id}:{path_key}",
        operation_id=UUID("00000000-0000-0000-0000-000000000821"),
        mapping_id=mapping_id,
        config_revision=initial_revision,
        local_path=str(path),
        path_key=path_key,
        relative_path=path.name,
        target_cid="9001",
        mode="keep",
        fingerprint=fingerprint,
        remote_receipt={
            "fid": "501",
            "pick_code": "pick-a",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
        derived_paths=(),
        derived_ownership={},
        completed_at=NOW,
        operation_sequence=1,
    )
    source_b = replace(
        source_a,
        operation_id=UUID("00000000-0000-0000-0000-000000000822"),
        config_revision=revised_revision,
        remote_receipt={
            "fid": "502",
            "pick_code": "pick-b",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
        operation_sequence=2,
    )

    class StaleOnceRepository(FakeRepository):
        reserve_calls = 0

        def reserve(self, intent):
            self.reserve_calls += 1
            if self.reserve_calls == 1:
                self.config = revised_raw
                self.sources[(mapping_id, path_key)] = source_b
                raise UploadMonitorConfigRevisionChanged("fixture revision race")
            return super().reserve(intent)

    repository = StaleOnceRepository(initial_raw)
    repository.sources[(mapping_id, path_key)] = source_a
    ingress = P115UploadMonitorIngressService(
        config=P115UploadMonitorConfigService(repository, lambda: (".mkv",)),
        repository=repository,
        clock=lambda: NOW,
    )

    reservations = ingress.record_event("deleted", str(path))

    assert repository.reserve_calls == 2
    assert len(reservations) == 1
    intent = reservations[0].operation
    assert intent.config_revision == revised_revision
    assert intent.mapping["target_name"] == "Revised"
    assert intent.completed_source == source_b.snapshot()


def test_ingress_reservation_revision_retry_is_bounded_to_one_retry(tmp_path: Path) -> None:
    root = tmp_path / "source"
    root.mkdir()
    path = root / "Movie.mkv"
    path.write_bytes(b"fixture")
    config = normalize_upload_monitor_config(
        {
            "enabled": True,
            "mappings": [
                {
                    "enabled": True,
                    "local_dir": str(root),
                    "target_cid": "9001",
                    "mode": "keep",
                    "extensions": [".mkv"],
                }
            ],
        },
        default_extensions=(".mkv",),
    )
    mapping_id = str(config["mappings"][0]["id"])
    path_key = os.path.normcase(os.path.abspath(path))
    state = path.stat()
    source = UploadedSource(
        record_key=f"{mapping_id}:{path_key}",
        operation_id=UUID("00000000-0000-0000-0000-000000000823"),
        mapping_id=mapping_id,
        config_revision=upload_monitor_config_revision(config),
        local_path=str(path),
        path_key=path_key,
        relative_path=path.name,
        target_cid="9001",
        mode="keep",
        fingerprint=UploadFileFingerprint(state.st_size, state.st_mtime_ns, state.st_dev, state.st_ino),
        remote_receipt={
            "fid": "503",
            "pick_code": "pick-c",
            "parent_id": "9001",
            "backend": "openapi",
            "account_id": "115001",
        },
        derived_paths=(),
        derived_ownership={},
        completed_at=NOW,
        operation_sequence=3,
    )

    class AlwaysStaleRepository(FakeRepository):
        reserve_calls = 0

        def reserve(self, intent):
            del intent
            self.reserve_calls += 1
            raise UploadMonitorConfigRevisionChanged("fixture repeated revision race")

    repository = AlwaysStaleRepository(config)
    repository.sources[(mapping_id, path_key)] = source
    ingress = P115UploadMonitorIngressService(
        config=P115UploadMonitorConfigService(repository, lambda: (".mkv",)),
        repository=repository,
        clock=lambda: NOW,
    )

    with pytest.raises(UploadMonitorConfigRevisionChanged):
        ingress.record_event("deleted", str(path))

    assert repository.reserve_calls == 2
