from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast
from uuid import uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from etk.compatibility.session_codec import LegacySessionCodec
from etk.domains.identity.effect_adapters import InProcessRegistrationSecretPort
from etk.domains.identity.effect_execution import (
    IdentityOperationExecution,
    RegistrationSagaCoordinator,
)
from etk.domains.identity.operations import (
    IdentityOperation,
    IdentityOperationIntent,
    IdentityOperationKind,
    IdentityOperationReservation,
    IdentityOperationStatus,
    IdentityOperationStep,
)
from etk.domains.identity.users_effects import IdentityEffectsService
from etk.domains.identity.users_router import effects_router
from etk.platform import AppContainer, Settings
from etk.platform.jobs import InvalidJsonPayloadError
from etk.platform.web import CookieSessionMiddleware

NOW = datetime(2026, 8, 27, 12, 0, tzinfo=UTC)
PASSWORD = "fixture-http-registration-password"
TOKEN = "fixture-http-registration-token"
SESSION_KEY = b"p8-registration-status-session-key"


def _operation() -> IdentityOperation:
    intent = IdentityOperationIntent.create(
        kind=IdentityOperationKind.REGISTER,
        scope_key="invitation:1",
        request={
            "invitation_id": 1,
            "username": "fixture-user",
            "template_id": 2,
            "template_name": "Fixture",
            "template_description": "Fixture description",
            "template_policy": {},
            "template_configuration": None,
            "expiration_date": None,
        },
        available_at=NOW,
    )
    return IdentityOperation(
        id=intent.id,
        sequence=1,
        operation_key=intent.operation_key,
        request_hash=intent.request_hash,
        kind=intent.kind,
        scope_key=intent.scope_key,
        request=intent.request,
        status=IdentityOperationStatus.READY,
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
        result=None,
        error=None,
        finished_at=None,
        created_at=NOW,
        updated_at=NOW,
    )


class FakeRegistration:
    def __init__(
        self,
        *,
        fail: bool = False,
        execution_status: IdentityOperationStatus = IdentityOperationStatus.CONFIRMED,
    ) -> None:
        self.operation = _operation()
        self.fail = fail
        self.execution_status = execution_status
        self.seen_lease: Any = None
        self.tokens: list[str] = []

    def read(self, operation_id: Any) -> IdentityOperation | None:
        return self.operation if operation_id == self.operation.id else None

    @staticmethod
    def registration_token_matches(operation_id: Any, invitation_token: str) -> bool:
        return operation_id == _operation().id and invitation_token == TOKEN

    def reserve(
        self,
        *,
        invitation_token: str,
        username: str,
        now: datetime | None = None,
    ) -> IdentityOperationReservation:
        del username, now
        self.tokens.append(invitation_token)
        return IdentityOperationReservation(self.operation, created=True)

    def execute_with_secret(
        self,
        operation_id: Any,
        secret: Any,
        *,
        worker_id: str,
        lease_seconds: float,
        now: datetime | None = None,
    ) -> IdentityOperationExecution:
        del operation_id, worker_id, lease_seconds, now
        self.seen_lease = secret
        assert repr(secret) == "RegistrationSecretLease('**********')"
        assert secret.reveal() == PASSWORD
        if self.fail:
            raise RuntimeError(f"provider failed with {PASSWORD} and {TOKEN}")
        completed = replace(
            self.operation,
            status=self.execution_status,
            result=(
                {
                    "username": "fixture-user",
                    "expiration_info": "永久有效",
                    "template_description": "Fixture description",
                }
                if self.execution_status is IdentityOperationStatus.CONFIRMED
                else None
            ),
            finished_at=NOW if self.execution_status.terminal else None,
        )
        self.operation = completed
        return IdentityOperationExecution(completed, claimed=True)


class FakePermissions:
    @staticmethod
    def is_admin(user_id: str) -> bool:
        del user_id
        return False


def _app(registration: FakeRegistration) -> FastAPI:
    settings = Settings.model_validate(
        {
            "auth_enabled": True,
            "emby_server_url": "https://emby.fixture.invalid",
            "emby_public_url": "https://watch.fixture.invalid",
        }
    )
    service = IdentityEffectsService(
        registration=cast(RegistrationSagaCoordinator, registration),
        mutations=cast(Any, object()),
        secrets=InProcessRegistrationSecretPort(),
        repository=cast(Any, object()),
        emby=cast(Any, object()),
        settings=lambda: settings,
        worker_id="fixture-http",
    )
    app = FastAPI()
    app.state.container = AppContainer(
        settings,
        repository_providers={"identity": lambda _: FakePermissions()},
    )
    app.state.identity_effects_service = service
    codec = LegacySessionCodec.from_secret_key(SESSION_KEY)
    app.add_middleware(CookieSessionMiddleware, codec_provider=lambda: codec)
    app.include_router(effects_router)
    return app


def test_registration_http_keeps_legacy_success_contract_and_closes_secret_lease() -> None:
    registration = FakeRegistration()
    with TestClient(_app(registration)) as client:
        response = client.post(
            "/api/register/invite",
            json={"username": "fixture-user", "password": PASSWORD, "token": TOKEN},
        )

    assert response.status_code == 201
    assert response.json() == {
        "status": "ok",
        "message": "注册成功！",
        "data": {
            "username": "fixture-user",
            "expiration_info": "永久有效",
            "redirect_url": "https://watch.fixture.invalid",
            "template_description": "Fixture description",
        },
    }
    assert registration.tokens == [TOKEN]
    assert PASSWORD not in repr(response.json())
    assert TOKEN not in repr(response.json())
    with pytest.raises(RuntimeError, match="closed"):
        registration.seen_lease.reveal()


def test_registration_failure_never_echoes_secret_or_invitation_token() -> None:
    with TestClient(_app(FakeRegistration(fail=True))) as client:
        response = client.post(
            "/api/register/invite",
            json={"username": "fixture-user", "password": PASSWORD, "token": TOKEN},
        )

    assert response.status_code == 503
    body = repr(response.json())
    assert PASSWORD not in body
    assert TOKEN not in body
    assert response.json()["message"] == "注册暂未完成，请使用相同邀请码重新提交以继续"
    assert response.json()["operation"]["requires_resubmission"] is True
    assert response.headers["retry-after"] == "2"


def test_nonterminal_registration_returns_traceable_503_and_invitation_can_query_status() -> None:
    registration = FakeRegistration(execution_status=IdentityOperationStatus.AWAITING_READBACK)
    with TestClient(_app(registration)) as client:
        submitted = client.post(
            "/api/register/invite",
            json={"username": "fixture-user", "password": PASSWORD, "token": TOKEN},
        )
        status_url = submitted.json()["operation"]["status_url"]
        unauthenticated = client.get(status_url)
        observed = client.get(status_url, headers={"X-Invitation-Token": TOKEN})

    assert submitted.status_code == 503
    assert submitted.json()["operation"]["operation_status"] == "awaiting_readback"
    assert unauthenticated.status_code == 401
    assert observed.status_code == 200
    assert observed.json()["operation"]["terminal"] is False


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "token",
        "secret_ref",
        "nested_api_key",
        "accessToken",
        "providerApiKey",
        "refresh-token",
    ],
)
def test_identity_operation_intent_rejects_secret_shaped_fields(key: str) -> None:
    payload: dict[str, Any]
    if key == "nested_api_key":
        payload = {"safe": {"provider_api_key": "fixture"}}
    else:
        payload = {key: "fixture"}
    with pytest.raises(InvalidJsonPayloadError):
        IdentityOperationIntent.create(
            kind=IdentityOperationKind.REGISTER,
            scope_key=f"fixture:{uuid4()}",
            request=payload,
            available_at=NOW,
        )
