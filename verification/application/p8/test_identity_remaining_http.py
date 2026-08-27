from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.identity.effect_adapters import (
    HttpxEmbyIdentityEffectsAdapter,
    InProcessRegistrationSecretPort,
)
from etk.domains.identity.effect_execution import (
    IdentityOperationExecution,
    IdentityUserMutationCoordinator,
    RegistrationSagaCoordinator,
)
from etk.domains.identity.effect_ports import IdentityEmbyMutationPort
from etk.domains.identity.operations import (
    IdentityOperation,
    IdentityOperationKind,
    IdentityOperationReservation,
    IdentityOperationStatus,
    IdentityOperationStep,
)
from etk.domains.identity.users_effects import IdentityEffectsRepository, IdentityEffectsService
from etk.domains.identity.users_router import effects_router
from etk.platform import AppContainer, Settings
from etk.platform.web import CookieSessionMiddleware

SECRET_KEY = b"p8-identity-effects-fixture-key"
NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
OPERATION_ID = UUID("00000000-0000-0000-0000-000000000088")


class FakePermissions:
    def __init__(self) -> None:
        self.admins: set[str] = set()

    def is_admin(self, user_id: str) -> bool:
        return user_id in self.admins


class FakeRepository:
    def __init__(self) -> None:
        self.created: list[dict[str, Any]] = []
        self.managed_users = {"managed-user"}
        self.pending_registration_usernames: set[str] = set()

    def create_template(self, **values: Any) -> int:
        self.created.append(dict(values))
        return 42

    def managed_user_exists(self, user_id: str) -> bool:
        return user_id in self.managed_users

    def adopt_remote_user(self, user: Mapping[str, Any]) -> bool:
        username = str(user.get("Name") or "").casefold()
        if username in self.pending_registration_usernames:
            return False
        self.managed_users.add(str(user["Id"]))
        return True

    @staticmethod
    def template_effect(template_id: int) -> dict[str, Any] | None:
        if template_id != 2:
            return None
        return {
            "id": 2,
            "name": "Fixture Target",
            "emby_policy_json": {"CanDownload": True},
            "emby_configuration_json": {"AudioLanguagePreference": "eng"},
        }

    @staticmethod
    def managed_user_projection() -> tuple[set[str], dict[str, dict[str, Any]]]:
        return (
            {"source-user"},
            {
                "managed-user": {
                    "status": "active",
                    "template_id": 2,
                    "template_name": "Fixture Target",
                    "expiration_date": NOW,
                    "max_concurrent_streams": 3,
                }
            },
        )

    @staticmethod
    def profile_image_tag(user_id: str) -> str | None:
        assert user_id == "managed-user"
        return "old-tag"


class FakeEmby:
    def __init__(self) -> None:
        self.users: dict[str, dict[str, Any]] = {
            "source-user": {
                "Id": "source-user",
                "Name": "Template Source",
                "Policy": {"CanDownload": False},
                "Configuration": {"AudioLanguagePreference": "jpn"},
            },
            "managed-user": {
                "Id": "managed-user",
                "Name": "Managed User",
                "Policy": {"CanDownload": False, "IsDisabled": False},
                "Configuration": {"AudioLanguagePreference": "jpn"},
            },
            "external-user": {
                "Id": "external-user",
                "Name": "External User",
                "Policy": {"CanDownload": False, "IsDisabled": False},
                "Configuration": {},
            },
            "registration-remote-only": {
                "Id": "registration-remote-only",
                "Name": "Registration Pending",
                "Policy": {"CanDownload": False, "IsDisabled": False},
                "Configuration": {},
            },
        }

    def list_users(self) -> tuple[dict[str, Any], ...]:
        return tuple(dict(value) for value in self.users.values())

    def get_user(self, user_id: str) -> dict[str, Any] | None:
        value = self.users.get(user_id)
        return dict(value) if value is not None else None


class FakeMutations:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.operation = _operation(IdentityOperationStatus.READY)
        self.idempotent: IdentityOperation | None = None
        self.execution_status = IdentityOperationStatus.CONFIRMED
        self.raise_after_checkpoint = False

    def read(self, operation_id: UUID) -> IdentityOperation | None:
        return self.operation if operation_id == self.operation.id else None

    def find_idempotent(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        idempotency_key: str,
    ) -> IdentityOperation | None:
        self.calls.append(
            (
                "find_idempotent",
                {"kind": kind, "user_id": user_id, "idempotency_key": idempotency_key},
            )
        )
        return self.idempotent

    def reserve(
        self,
        *,
        kind: IdentityOperationKind,
        user_id: str,
        request: Mapping[str, Any],
        idempotency_key: str | None = None,
        outbox_delay_seconds: float = 0,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        del now
        self.calls.append(
            (
                "reserve",
                {
                    "kind": kind,
                    "user_id": user_id,
                    "request": dict(request),
                    "idempotency_key": idempotency_key,
                    "outbox_delay_seconds": outbox_delay_seconds,
                },
            )
        )
        self.operation = replace(
            self.operation,
            kind=kind,
            request={"user_id": user_id, **dict(request)},
        )
        return IdentityOperationReservation(self.operation, created=True)

    def finalize_request(
        self,
        operation_id: UUID,
        request: Mapping[str, Any],
        *,
        now: datetime | None = None,
    ) -> IdentityOperation:
        del now
        assert operation_id == self.operation.id
        self.calls.append(("finalize_request", {"operation_id": operation_id, "request": dict(request)}))
        self.operation = replace(
            self.operation,
            request={"user_id": "managed-user", **dict(request)},
        )
        return self.operation

    def reject_ready(
        self,
        operation_id: UUID,
        *,
        error_type: str,
        now: datetime | None = None,
    ) -> IdentityOperation:
        del now
        assert operation_id == self.operation.id
        self.calls.append(("reject_ready", {"operation_id": operation_id, "error_type": error_type}))
        self.operation = replace(
            self.operation,
            status=IdentityOperationStatus.REJECTED,
            error={"type": error_type},
            finished_at=NOW,
        )
        return self.operation

    def reserve_avatar(
        self,
        *,
        user_id: str,
        content: bytes,
        content_type: str,
        previous_tag: str | None,
        idempotency_key: str | None = None,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        del now
        self.calls.append(
            (
                "reserve_avatar",
                {
                    "user_id": user_id,
                    "content": content,
                    "content_type": content_type,
                    "previous_tag": previous_tag,
                    "idempotency_key": idempotency_key,
                },
            )
        )
        return IdentityOperationReservation(self.operation, created=True)

    def execute(
        self,
        operation_id: UUID,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution:
        del now
        self.calls.append(
            (
                "execute",
                {
                    "operation_id": operation_id,
                    "worker_id": worker_id,
                    "lease_seconds": lease_seconds,
                },
            )
        )
        operation = _operation(
            self.execution_status,
            result={"new_tag": "new-tag"} if self.execution_status is IdentityOperationStatus.CONFIRMED else None,
            request=self.operation.request,
        )
        self.operation = operation
        if self.raise_after_checkpoint:
            raise RuntimeError("fixture transport failed after durable checkpoint")
        return IdentityOperationExecution(operation, claimed=True)


def _operation(
    status: IdentityOperationStatus,
    *,
    result: Mapping[str, Any] | None = None,
    request: Mapping[str, Any] | None = None,
) -> IdentityOperation:
    return IdentityOperation(
        id=OPERATION_ID,
        sequence=8,
        operation_key="a" * 64,
        request_hash=b"b" * 32,
        kind=IdentityOperationKind.SET_STATUS,
        scope_key="user:managed-user",
        request=dict(request or {"user_id": "managed-user"}),
        status=status,
        current_step=IdentityOperationStep.RESERVED,
        state_revision=0,
        attempts=0,
        available_at=NOW,
        claimed_by=None,
        claim_token=None,
        claimed_at=None,
        lease_expires_at=None,
        cancel_requested_at=None,
        step_started_at=None,
        state={},
        result=dict(result) if result is not None else None,
        error=None,
        finished_at=NOW if status.terminal else None,
        created_at=NOW,
        updated_at=NOW,
    )


def _build_app() -> tuple[FastAPI, FakePermissions, FakeRepository, FakeMutations]:
    permissions = FakePermissions()
    repository = FakeRepository()
    emby = FakeEmby()
    mutations = FakeMutations()
    settings = Settings.model_validate({"auth_enabled": True})
    service = IdentityEffectsService(
        registration=cast(RegistrationSagaCoordinator, object()),
        mutations=cast(IdentityUserMutationCoordinator, mutations),
        secrets=InProcessRegistrationSecretPort(),
        repository=cast(IdentityEffectsRepository, repository),
        emby=cast(IdentityEmbyMutationPort, emby),
        settings=lambda: settings,
        worker_id="fixture-http",
    )
    container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: permissions},
    )
    app = FastAPI()
    app.state.container = container
    app.state.identity_effects_service = service
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app, permissions, repository, mutations


def _set_user(client: TestClient, user_id: str) -> None:
    codec = LegacySessionCodec.from_secret_key(SECRET_KEY)
    client.cookies.set("session", codec.dumps({"emby_user_id": user_id}))


def test_create_template_and_managed_user_query_preserve_contracts() -> None:
    app, permissions, repository, _mutations = _build_app()
    permissions.admins.add("admin")
    with TestClient(app) as client:
        _set_user(client, "admin")
        created = client.post(
            "/api/admin/user_templates",
            json={
                "name": "Fixture",
                "source_emby_user_id": "source-user",
                "include_configuration": True,
            },
        )
        users = client.get("/api/admin/users")

    assert created.status_code == 201
    assert created.json() == {"status": "ok", "message": "模板创建成功", "id": 42}
    assert repository.created[0]["policy"] == {"CanDownload": False}
    assert repository.created[0]["configuration"] == {"AudioLanguagePreference": "jpn"}
    assert users.status_code == 200
    assert [user["Id"] for user in users.json()] == [
        "managed-user",
        "external-user",
        "registration-remote-only",
    ]
    assert users.json()[0]["template_name"] == "Fixture Target"


def test_admin_mutations_use_durable_coordinator_and_do_not_run_when_unauthorized() -> None:
    app, permissions, _repository, mutations = _build_app()
    with TestClient(app) as client:
        _set_user(client, "user")
        denied = client.post("/api/admin/users/managed-user/status", json={"disable": True})
        permissions.admins.add("user")
        assigned = client.post(
            "/api/admin/users/managed-user/template",
            json={"template_id": 2},
            headers={"Idempotency-Key": "fixture-template-request"},
        )
        status = client.post(
            "/api/admin/users/managed-user/status",
            json={"disable": True},
        )
        deleted = client.delete("/api/admin/users/managed-user")

    assert denied.status_code == 403
    assert assigned.status_code == status.status_code == deleted.status_code == 200
    reserve_calls = [values for name, values in mutations.calls if name == "reserve"]
    assert [call["kind"] for call in reserve_calls] == [
        IdentityOperationKind.ASSIGN_TEMPLATE,
        IdentityOperationKind.SET_STATUS,
        IdentityOperationKind.DELETE_USER,
    ]
    assert reserve_calls[0]["idempotency_key"] == "fixture-template-request"


def test_admin_mutation_rejects_remote_user_until_registration_local_commit() -> None:
    app, permissions, repository, mutations = _build_app()
    permissions.admins.add("fixture-admin")
    repository.pending_registration_usernames.add("registration pending")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.post(
            "/api/admin/users/registration-remote-only/status",
            json={"disable": True},
        )

    assert response.status_code == 409
    assert response.json() == {
        "status": "error",
        "message": "该用户的注册仍在进行中或状态尚未确认",
    }
    assert mutations.calls == []


def test_remote_only_list_user_is_adopted_before_durable_admin_mutation() -> None:
    app, permissions, repository, mutations = _build_app()
    permissions.admins.add("fixture-admin")
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.post(
            "/api/admin/users/external-user/status",
            json={"disable": True},
        )

    assert response.status_code == 200
    assert "external-user" in repository.managed_users
    assert any(name == "reserve" for name, _values in mutations.calls)


@pytest.mark.parametrize("missing_field", ["Policy", "Configuration"])
def test_assign_template_rejects_before_write_when_prior_snapshot_is_incomplete(
    missing_field: str,
) -> None:
    app, permissions, _repository, mutations = _build_app()
    permissions.admins.add("fixture-admin")
    emby = cast(Any, app.state.identity_effects_service)._emby
    emby.users["managed-user"].pop(missing_field)
    with TestClient(app) as client:
        _set_user(client, "fixture-admin")
        response = client.post(
            "/api/admin/users/managed-user/template",
            json={"template_id": 2},
        )

    assert response.status_code == 500
    assert any(name == "reject_ready" for name, _values in mutations.calls)
    assert not any(name == "execute" for name, _values in mutations.calls)


def test_nonterminal_mutation_returns_traceable_503_and_authorized_actor_can_query_status() -> None:
    app, permissions, _repository, mutations = _build_app()
    mutations.execution_status = IdentityOperationStatus.AWAITING_READBACK
    permissions.admins.add("managed-user")
    with TestClient(app) as client:
        _set_user(client, "managed-user")
        submitted = client.post(
            "/api/admin/users/managed-user/status",
            json={"disable": True},
        )
        status_url = submitted.json()["operation"]["status_url"]
        observed = client.get(status_url)
        _set_user(client, "different-user")
        forbidden = client.get(status_url)

    assert submitted.status_code == 503
    assert submitted.json()["status"] == "pending"
    assert submitted.json()["operation"]["operation_status"] == "awaiting_readback"
    assert observed.status_code == 200
    assert observed.json()["operation"] == {
        "id": str(OPERATION_ID),
        "kind": "set_status",
        "operation_status": "awaiting_readback",
        "terminal": False,
        "error_type": None,
    }
    assert forbidden.status_code == 403


def test_execute_exception_after_checkpoint_rereads_ledger_and_returns_operation_reference() -> None:
    app, permissions, _repository, mutations = _build_app()
    permissions.admins.add("managed-user")
    mutations.execution_status = IdentityOperationStatus.AWAITING_READBACK
    mutations.raise_after_checkpoint = True
    with TestClient(app) as client:
        _set_user(client, "managed-user")
        response = client.post(
            "/api/admin/users/managed-user/status",
            json={"disable": True},
        )

    assert response.status_code == 503
    assert response.json()["operation"]["id"] == str(OPERATION_ID)
    assert response.json()["operation"]["operation_status"] == "awaiting_readback"
    assert response.headers["location"] == response.json()["operation"]["status_url"]


@pytest.mark.parametrize(
    "user_payload,configuration_payload",
    [([], None), ({"Id": "fixture-user"}, [])],
)
def test_emby_user_readback_rejects_non_object_200_payloads(
    user_payload: Any,
    configuration_payload: Any,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/Configuration"):
            return httpx.Response(200, json=configuration_payload)
        return httpx.Response(200, json=user_payload)

    settings = Settings.model_validate(
        {
            "emby_server_url": "https://emby.fixture.invalid",
            "emby_api_key": "fixture-api-key",
        }
    )
    adapter = HttpxEmbyIdentityEffectsAdapter(
        lambda: settings,
        client_factory=lambda: httpx.Client(transport=httpx.MockTransport(handler)),
    )

    with pytest.raises(RuntimeError, match="invalid payload"):
        adapter.get_user("fixture-user")


def test_avatar_multipart_is_bounded_and_staged_before_execution() -> None:
    app, _permissions, _repository, mutations = _build_app()
    with TestClient(app) as client:
        _set_user(client, "managed-user")
        uploaded = client.post(
            "/api/portal/upload-avatar",
            files={"avatar": ("avatar.png", b"fixture-png", "image/png")},
        )
        unsupported = client.post(
            "/api/portal/upload-avatar",
            files={"avatar": ("avatar.gif", b"fixture-gif", "image/gif")},
        )

    assert uploaded.status_code == 200
    assert uploaded.json() == {
        "status": "ok",
        "message": "头像上传成功",
        "new_tag": "new-tag",
    }
    assert unsupported.status_code == 400
    avatar_calls = [values for name, values in mutations.calls if name == "reserve_avatar"]
    assert avatar_calls == [
        {
            "user_id": "managed-user",
            "content": b"fixture-png",
            "content_type": "image/png",
            "previous_tag": "old-tag",
            "idempotency_key": None,
        }
    ]


def test_idempotent_template_replay_does_not_refresh_remote_snapshot() -> None:
    app, permissions, _repository, mutations = _build_app()
    permissions.admins.add("admin")
    mutations.idempotent = _operation(
        IdentityOperationStatus.CONFIRMED,
        result={"template_id": 2},
        request={"user_id": "managed-user", "template_id": 2},
    )
    with TestClient(app) as client:
        _set_user(client, "admin")
        replay = client.post(
            "/api/admin/users/managed-user/template",
            json={"template_id": 2},
            headers={"Idempotency-Key": "fixture-template-replay"},
        )
        conflict = client.post(
            "/api/admin/users/managed-user/template",
            json={"template_id": 3},
            headers={"Idempotency-Key": "fixture-template-replay"},
        )

    assert replay.status_code == 200
    assert replay.json()["status"] == "ok"
    assert conflict.status_code == 409
    assert conflict.json()["message"] == "Idempotency-Key 已用于不同请求"
    assert [name for name, _values in mutations.calls] == [
        "find_idempotent",
        "execute",
        "find_idempotent",
    ]
