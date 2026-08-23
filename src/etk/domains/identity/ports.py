from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from etk.platform.config import Settings


class IdentityRepository(Protocol):
    def upsert_user(self, user: Mapping[str, Any]) -> None: ...

    def is_admin(self, user_id: str) -> bool: ...

    def subscription_permission(self, user_id: str) -> bool: ...


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

    def install_plugin(self, authorization: Mapping[str, Any], settings: Settings) -> Mapping[str, Any]: ...

    def configure_libraries(
        self,
        authorization: Mapping[str, Any],
        plugin_install: Mapping[str, Any],
    ) -> Mapping[str, Any]: ...


class ConfigurationWriter(Protocol):
    def save(self, updates: Mapping[str, Any]) -> Settings: ...
