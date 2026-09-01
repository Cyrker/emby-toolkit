from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest

from etk.domains.p115 import AtomicP115LocalStrmLibraryGateway
from etk.domains.sharing.operations import SharingOperation, SharingOperationStatus
from etk.domains.sharing.service import SharingService
from etk.domains.sharing.virtual_import import (
    AtomicVirtualImportProjector,
    HttpxEmbyVirtualItemGateway,
    VirtualImportBundle,
    VirtualImportCreation,
    VirtualImportOperationDelivery,
    VirtualImportSourceIntent,
    freeze_virtual_import_source,
)
from etk.platform import Settings


def _source(*, source_id: str = "source-fixture", file_name: str = "Fixture.mkv") -> dict[str, Any]:
    return {
        "source_kind": "movie",
        "source_id": source_id,
        "item_type": "Movie",
        "title": "Fixture",
        "tmdb_id": "123",
        "sha1": "A" * 40,
        "size": 1024,
        "file_name": file_name,
    }


def _bundle(*, files: tuple[dict[str, Any], ...] | None = None) -> VirtualImportBundle:
    source = _source()
    values = files or (source,)
    return VirtualImportBundle("movie", "source-fixture", source, values)


def _settings(root: Path, **values: Any) -> Settings:
    defaults: dict[str, Any] = {
        "local_strm_root": str(root),
        "etk_server_url": "https://etk.example.test",
        "emby_server_url": "https://emby.example.test",
        "emby_api_key": "fixture-token",
        "emby_user_id": "fixture-user",
        "emby_api_timeout": 5,
    }
    defaults.update(values)
    return Settings(**defaults)


class RecordingOperations:
    def __init__(self) -> None:
        self.intent: Any = None

    def reserve_with_replay(self, intent: Any) -> tuple[Any, bool]:
        self.intent = intent
        return (
            SimpleNamespace(
                id=intent.id,
                status=SharingOperationStatus.READY,
                attempts=0,
                result=None,
                error=None,
            ),
            False,
        )


class StagingVirtualImports:
    def __init__(self) -> None:
        self.prepare_calls = 0

    def freeze(self, source: Mapping[str, Any]) -> VirtualImportSourceIntent:
        return freeze_virtual_import_source(source)

    def prepare(self, _source: Mapping[str, Any]) -> VirtualImportBundle:
        self.prepare_calls += 1
        raise AssertionError("request thread must not resolve center files")


class EnabledConfiguration:
    def load(self) -> dict[str, Any]:
        return {"p115_shared_virtual_import_enabled": True}


def test_center_import_request_only_freezes_identity_and_reserves_operation() -> None:
    operations = RecordingOperations()
    virtual = StagingVirtualImports()
    service = SharingService(
        cast(Any, object()),
        cast(Any, object()),
        cast(Any, operations),
        configuration=cast(Any, EnabledConfiguration()),
        virtual_imports=cast(Any, virtual),
    )

    result = service.import_center_source(
        {"source": _source()},
        principal_id="admin-fixture",
        idempotency_key="virtual-create-fixture",
    )

    assert result.status_code == 202
    assert virtual.prepare_calls == 0
    assert operations.intent.request["provider"] == "virtual_import"
    assert operations.intent.request["action"] == "create"
    assert operations.intent.request["payload"]["source_id"] == "source-fixture"
    assert "readback" not in operations.intent.request


def test_center_share_import_stages_explicit_share_action_without_persisting_channel_codes() -> None:
    operations = RecordingOperations()
    virtual = StagingVirtualImports()
    service = SharingService(
        cast(Any, object()),
        cast(Any, object()),
        cast(Any, operations),
        configuration=cast(Any, EnabledConfiguration()),
        virtual_imports=cast(Any, virtual),
    )
    source = {
        "source_kind": "logical_season",
        "source_id": "logical-season-fixture",
        "title": "Fixture Series",
        "share_channel": {
            "channel_id": "channel-fixture",
            "status": "valid",
            "share_code": "provider-share-secret",
            "receive_code": "provider-receive-secret",
        },
    }

    result = service.import_center_source(
        {"source": source, "mode": "share", "force_real": True},
        principal_id="admin-fixture",
        idempotency_key="share-create-fixture",
    )

    assert result.status_code == 202
    assert operations.intent.request["action"] == "transfer_logical_season_share"
    assert operations.intent.request["payload"]["mode"] == "share"
    assert operations.intent.request["payload"]["expected_channel_id"] == "channel-fixture"
    durable = str(operations.intent.request)
    assert "provider-share-secret" not in durable
    assert "provider-receive-secret" not in durable


def test_center_share_import_binds_nested_logical_group_channel_identity() -> None:
    operations = RecordingOperations()
    virtual = StagingVirtualImports()
    service = SharingService(
        cast(Any, object()),
        cast(Any, object()),
        cast(Any, operations),
        configuration=cast(Any, EnabledConfiguration()),
        virtual_imports=cast(Any, virtual),
    )
    source = {
        "source_kind": "logical_season",
        "source_id": "logical-season-nested",
        "logical_group": {
            "share_channel": {
                "channel_id": "channel-nested",
                "share_code": "provider-share-secret",
                "receive_code": "provider-receive-secret",
            }
        },
    }

    result = service.import_center_source(
        {"source": source, "mode": "share", "force_real": True},
        principal_id="admin-fixture",
        idempotency_key="share-create-nested",
    )

    assert result.status_code == 202
    assert operations.intent.request["payload"]["expected_channel_id"] == "channel-nested"
    durable = str(operations.intent.request)
    assert "provider-share-secret" not in durable
    assert "provider-receive-secret" not in durable


def test_virtual_projector_rolls_back_created_files_and_preserves_unknown_target(tmp_path: Path) -> None:
    first = _source(file_name="First.mkv")
    second = {**_source(source_id="source-second", file_name="Second.mkv"), "sha1": "B" * 40}
    projector = AtomicVirtualImportProjector(
        settings=lambda: _settings(tmp_path),
        libraries=AtomicP115LocalStrmLibraryGateway(),
    )
    unknown = tmp_path / "电影/Fixture {tmdb=123}/Second.strm"
    unknown.parent.mkdir(parents=True)
    unknown.write_text("user-owned", encoding="utf-8")

    with pytest.raises(RuntimeError, match="拒绝覆盖"):
        projector.project(17, _bundle(files=(first, second)))

    assert not (tmp_path / "电影/Fixture {tmdb=123}/First.strm").exists()
    assert unknown.read_text(encoding="utf-8") == "user-owned"


def test_virtual_projector_cleanup_refuses_digest_drift(tmp_path: Path) -> None:
    projector = AtomicVirtualImportProjector(
        settings=lambda: _settings(tmp_path),
        libraries=AtomicP115LocalStrmLibraryGateway(),
    )
    projection = projector.project(17, _bundle())
    target = Path(projection.paths[0])
    target.write_text("changed-by-user", encoding="utf-8")

    with pytest.raises(RuntimeError, match="ownership"):
        projector.cleanup({"files_json": list(projection.files)})

    assert target.read_text(encoding="utf-8") == "changed-by-user"


class DeliveryService:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.rows: tuple[dict[str, Any], ...] = ()

    def freeze(self, source: Mapping[str, Any]) -> VirtualImportSourceIntent:
        return freeze_virtual_import_source(source)

    def prepare(self, source: Mapping[str, Any]) -> VirtualImportBundle:
        self.events.append("prepare")
        intent = self.freeze(source)
        return VirtualImportBundle(intent.source_kind, intent.source_id, intent.source, (dict(intent.source),))

    def create_bundle(self, bundle: VirtualImportBundle) -> VirtualImportCreation:
        self.events.append("create")
        item = {"id": 17, "files_json": list(bundle.files)}
        self.rows = (item,)
        return VirtualImportCreation(item, True)

    def active_by_source(self, _source_kind: str, _source_id: str) -> tuple[dict[str, Any], ...]:
        return self.rows


class NoopEmby:
    def exact_item_ids(self, paths: Any) -> tuple[str, ...]:
        del paths
        return ()

    def delete_items(self, item_ids: Any) -> None:
        del item_ids
        return None


def test_virtual_create_delivery_resolves_before_checkpoint_and_projects_after() -> None:
    service = DeliveryService()
    intent = freeze_virtual_import_source(_source())
    operation = cast(
        SharingOperation,
        SimpleNamespace(
            request={
                "provider": "virtual_import",
                "action": "create",
                "payload": {
                    "source_kind": intent.source_kind,
                    "source_id": intent.source_id,
                    "source": intent.source,
                    "source_state_hash": intent.state_hash,
                },
            }
        ),
    )
    delivery = VirtualImportOperationDelivery(cast(Any, service), NoopEmby())

    outcome = delivery.request(operation, checkpoint=lambda: service.events.append("checkpoint"))

    assert outcome.disposition.value == "confirmed"
    assert service.events == ["prepare", "checkpoint", "create"]
    assert outcome.result == {
        "authoritative_readback": True,
        "created": True,
        "file_count": 1,
        "virtual_id": 17,
    }


def test_emby_virtual_gateway_matches_only_exact_paths_and_rejects_ambiguity(tmp_path: Path) -> None:
    target = str(tmp_path / "Fixture.strm")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "Items": [
                    {"Id": "wrong", "Path": f"{target}.backup"},
                    {"Id": "exact", "Path": target},
                ]
            },
        )

    gateway = HttpxEmbyVirtualItemGateway(
        lambda: _settings(tmp_path),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert gateway.exact_item_ids([target]) == ("exact",)
    assert requests[0].url.params["Path"] == target

    def ambiguous(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"Items": [{"Id": "a", "Path": target}, {"Id": "b", "Path": target}]})

    ambiguous_gateway = HttpxEmbyVirtualItemGateway(
        lambda: _settings(tmp_path),
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(ambiguous)),
    )
    with pytest.raises(RuntimeError, match="多个相同路径"):
        ambiguous_gateway.exact_item_ids([target])


def test_freeze_virtual_source_rejects_sensitive_values() -> None:
    with pytest.raises(ValueError, match="凭据形态"):
        freeze_virtual_import_source({**_source(), "private_url": "https://tracker.invalid/file?token=secret"})
