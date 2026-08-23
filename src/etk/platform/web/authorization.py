from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from .session import SessionState


class UserPermissionReader(Protocol):
    def is_admin(self, user_id: str) -> bool: ...


@dataclass(frozen=True, slots=True)
class AuthorizationDecision:
    allowed: bool
    status_code: int = 200
    message: str = ""
    user_id: str | None = None


class PermissionPolicy:
    """Preserve legacy setup/login/admin decisions without trusting cookie role flags."""

    def __init__(self, permissions: UserPermissionReader, *, auth_enabled: bool) -> None:
        self._permissions = permissions
        self._auth_enabled = auth_enabled

    def require_admin(self, session: SessionState) -> AuthorizationDecision:
        user_id = _session_user_id(session)
        if not self._auth_enabled:
            return AuthorizationDecision(True, user_id=user_id)
        if user_id and self._permissions.is_admin(user_id):
            return AuthorizationDecision(True, user_id=user_id)
        return AuthorizationDecision(
            False,
            status_code=403,
            message="需要 Emby 管理员权限才能执行此操作",
            user_id=user_id,
        )

    def require_config_access(
        self,
        session: SessionState,
        *,
        configured: bool,
    ) -> AuthorizationDecision:
        user_id = _session_user_id(session)
        if not configured or bool(session.get("is_setup_mode")):
            return AuthorizationDecision(True, user_id=user_id)
        if user_id and self._permissions.is_admin(user_id):
            return AuthorizationDecision(True, user_id=user_id)
        return AuthorizationDecision(False, status_code=403, user_id=user_id)

    @staticmethod
    def require_emby_login(session: SessionState) -> AuthorizationDecision:
        user_id = _session_user_id(session)
        if user_id:
            return AuthorizationDecision(True, user_id=user_id)
        return AuthorizationDecision(
            False,
            status_code=401,
            message="需要 Emby 用户登录才能访问此资源",
        )

    @staticmethod
    def require_any_login(session: SessionState) -> AuthorizationDecision:
        user_id = _session_user_id(session)
        if user_id:
            return AuthorizationDecision(True, user_id=user_id)
        return AuthorizationDecision(False, status_code=401, message="需要登录才能访问此资源")


def _session_user_id(session: SessionState) -> str | None:
    value = str(session.get("emby_user_id") or "").strip()
    return value or None
