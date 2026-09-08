from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from etk.platform.config import Settings


class IdentityRepository(Protocol):
    def projection_revision(self) -> int: ...

    def upsert_user(self, user: Mapping[str, Any], *, expected_revision: int) -> bool: ...

    def registration_pending(self, username: str) -> bool: ...

    def is_admin(self, user_id: str) -> bool: ...

    def is_active(self, user_id: str) -> bool: ...

    def subscription_permission(self, user_id: str) -> bool: ...

    def display_names(self, user_ids: Sequence[str]) -> Mapping[str, str]: ...


class EmbyIdentityGateway(Protocol):
    def authenticate(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        device_id: str,
    ) -> Mapping[str, Any] | None: ...

    def get_user(self, base_url: str, access_token: str, user_id: str) -> Mapping[str, Any] | None: ...

    def test_connection(self, base_url: str, access_token: str) -> Mapping[str, Any]: ...

    def install_plugin(
        self,
        authorization: Mapping[str, Any],
        settings: Settings,
        request_id: str,
    ) -> Mapping[str, Any]: ...

    def configure_libraries(
        self,
        authorization: Mapping[str, Any],
        plugin_install: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class ConfigurationWriter(Protocol):
    def save(self, updates: Mapping[str, Any]) -> Settings: ...


@dataclass(frozen=True, slots=True)
class PostAuthorizationSubmission:
    job_id: str
    status: str
    reused: bool
    resumed: bool
    result: Mapping[str, Any] | None = None
    error: Mapping[str, Any] | None = None
    error_type: str | None = None


class PostAuthorizationSubmitter(Protocol):
    def submit(self, settings: Settings) -> PostAuthorizationSubmission: ...
