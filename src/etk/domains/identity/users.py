from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from etk.platform import Settings

from .users_repository import UserTemplateNameConflictError


@dataclass(frozen=True, slots=True)
class UsersHttpResult:
    payload: Any
    status_code: int = 200


class UsersRepository(Protocol):
    def list_templates(self) -> list[dict[str, Any]]: ...

    def update_template(
        self,
        template_id: int,
        *,
        name: str,
        description: str | None,
        default_expiration_days: int | None,
        allow_unrestricted_subscriptions: bool,
        personal_play_account_only: bool,
        max_concurrent_streams: int,
    ) -> bool: ...

    def delete_template(self, template_id: int) -> bool: ...

    def create_invitations(
        self,
        *,
        template_id: int,
        expiration_days: int | None,
        link_expires_in_days: int,
        count: int,
    ) -> list[str]: ...

    def list_invitations(self) -> list[dict[str, Any]]: ...

    def delete_invitation(self, invitation_id: int) -> bool: ...

    def invitation_status(self, token: str) -> Mapping[str, Any] | None: ...

    def set_expiration(self, user_id: str, expiration_date: str | None) -> None: ...

    def account_info(self, user_id: str) -> dict[str, Any] | None: ...

    def set_telegram_chat_id(self, user_id: str, chat_id: str | None) -> None: ...


class UsersMediaProjection(Protocol):
    def request_history(
        self,
        user_id: str,
        *,
        page: int,
        page_size: int,
        status: str,
    ) -> tuple[list[dict[str, Any]], int]: ...

    def request_stats(self, user_id: str) -> dict[str, int]: ...


class UsersService:
    def __init__(
        self,
        *,
        repository: UsersRepository,
        media: UsersMediaProjection,
        settings: Callable[[], Settings],
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._repository = repository
        self._media = media
        self._settings = settings
        self._clock = clock

    def list_templates(self) -> UsersHttpResult:
        try:
            return UsersHttpResult(self._repository.list_templates())
        except Exception:
            return UsersHttpResult({"status": "error", "message": "获取模板列表失败"}, 500)

    def update_template(self, template_id: int, payload: Any) -> UsersHttpResult:
        if not isinstance(payload, Mapping):
            return UsersHttpResult({"status": "error", "message": "模板名称不能为空"}, 400)
        name = str(payload.get("name") or "").strip()
        if not name:
            return UsersHttpResult({"status": "error", "message": "模板名称不能为空"}, 400)
        try:
            updated = self._repository.update_template(
                template_id,
                name=name,
                description=_optional_text(payload.get("description")),
                default_expiration_days=_optional_int(payload.get("default_expiration_days")),
                allow_unrestricted_subscriptions=bool(payload.get("allow_unrestricted_subscriptions", False)),
                personal_play_account_only=bool(payload.get("personal_play_account_only", False)),
                max_concurrent_streams=max(0, int(payload.get("max_concurrent_streams") or 0)),
            )
        except UserTemplateNameConflictError:
            return UsersHttpResult({"status": "error", "message": "模板名称已被占用"}, 409)
        except (TypeError, ValueError):
            return UsersHttpResult({"status": "error", "message": "模板参数无效"}, 400)
        except Exception:
            return UsersHttpResult({"status": "error", "message": "模板更新失败"}, 500)
        if not updated:
            return UsersHttpResult({"status": "error", "message": "模板不存在"}, 404)
        return UsersHttpResult({"status": "ok", "message": "模板更新成功"})

    def delete_template(self, template_id: int) -> UsersHttpResult:
        try:
            deleted = self._repository.delete_template(template_id)
        except Exception:
            return UsersHttpResult({"status": "error", "message": "删除模板失败"}, 500)
        if not deleted:
            return UsersHttpResult({"status": "error", "message": "未找到该模板"}, 404)
        return UsersHttpResult({"status": "ok", "message": "模板已删除"})

    def create_invitations(self, payload: Any, host_url: str) -> UsersHttpResult:
        if not isinstance(payload, Mapping) or not payload.get("template_id"):
            return UsersHttpResult({"status": "error", "message": "必须选择一个模板"}, 400)
        try:
            count = int(payload.get("count", 1))
            if count < 1:
                return UsersHttpResult({"status": "error", "message": "生成数量必须大于 0"}, 400)
            tokens = self._repository.create_invitations(
                template_id=int(payload["template_id"]),
                expiration_days=_optional_int(payload.get("expiration_days")),
                link_expires_in_days=int(payload.get("link_expires_in_days", 7)),
                count=count,
            )
        except LookupError as exc:
            return UsersHttpResult({"status": "error", "message": str(exc)}, 400)
        except (TypeError, ValueError):
            return UsersHttpResult({"status": "error", "message": "邀请参数无效"}, 400)
        except Exception:
            return UsersHttpResult({"status": "error", "message": "创建邀请码失败"}, 500)
        configured = str(self._settings().get("app_base_url") or "").rstrip("/")
        base_url = configured or host_url.rstrip("/")
        links = [f"{base_url}/register/invite/{token}" for token in tokens]
        response: dict[str, Any] = {"status": "ok", "invite_links": links, "count": len(links)}
        if len(links) == 1:
            response["invite_link"] = links[0]
        return UsersHttpResult(response, 201)

    def list_invitations(self) -> UsersHttpResult:
        try:
            return UsersHttpResult(self._repository.list_invitations())
        except Exception:
            return UsersHttpResult({"status": "error", "message": "获取邀请码列表失败"}, 500)

    def delete_invitation(self, invitation_id: int) -> UsersHttpResult:
        try:
            deleted = self._repository.delete_invitation(invitation_id)
        except Exception:
            return UsersHttpResult({"status": "error", "message": "删除邀请码失败"}, 500)
        if not deleted:
            return UsersHttpResult({"status": "error", "message": "未找到该邀请码"}, 404)
        return UsersHttpResult({"status": "ok", "message": "邀请码已删除"})

    def validate_invitation(self, token: str) -> UsersHttpResult:
        try:
            invitation = self._repository.invitation_status(token)
        except Exception:
            return UsersHttpResult({"valid": False, "reason": "服务器内部错误"}, 500)
        if invitation is None:
            return UsersHttpResult({"valid": False, "reason": "邀请链接不存在"}, 404)
        if invitation.get("status") != "active":
            return UsersHttpResult({"valid": False, "reason": f"邀请链接已{invitation.get('status')}"}, 410)
        expires_at = invitation.get("expires_at")
        if isinstance(expires_at, datetime) and expires_at < self._clock():
            return UsersHttpResult({"valid": False, "reason": "邀请链接已过期"}, 410)
        return UsersHttpResult({"valid": True})

    def set_expiration(self, user_id: str, payload: Any) -> UsersHttpResult:
        if not isinstance(payload, Mapping):
            return UsersHttpResult({"status": "error", "message": "更新有效期失败"}, 400)
        try:
            self._repository.set_expiration(user_id, _optional_text(payload.get("expiration_date")))
            return UsersHttpResult({"status": "ok", "message": "用户有效期已更新"})
        except Exception:
            return UsersHttpResult({"status": "error", "message": "更新有效期失败"}, 500)

    def account_info(self, user_id: str) -> UsersHttpResult:
        try:
            account = self._repository.account_info(user_id) or {}
            account["telegram_channel_id"] = self._settings().telegram_channel_id
            return UsersHttpResult(account)
        except Exception:
            return UsersHttpResult({"status": "error", "message": "获取账户信息失败"}, 500)

    def subscription_history(
        self,
        user_id: str,
        *,
        page: int,
        page_size: int,
        status: str,
    ) -> UsersHttpResult:
        try:
            items, total = self._media.request_history(
                user_id,
                page=page,
                page_size=page_size,
                status=status,
            )
            return UsersHttpResult(
                {
                    "items": items,
                    "total_records": total,
                    "page": page,
                    "page_size": page_size,
                }
            )
        except Exception:
            return UsersHttpResult({"status": "error", "message": "获取订阅历史失败"}, 500)

    def save_telegram_chat_id(self, user_id: str, payload: Any) -> UsersHttpResult:
        if not isinstance(payload, Mapping):
            return UsersHttpResult({"status": "error", "message": "保存失败，请联系管理员"}, 400)
        chat_id = str(payload.get("chat_id") or "").strip() or None
        try:
            self._repository.set_telegram_chat_id(user_id, chat_id)
            return UsersHttpResult({"status": "ok", "message": "Telegram Chat ID 保存成功！"})
        except Exception:
            return UsersHttpResult({"status": "error", "message": "保存失败，请联系管理员"}, 500)

    def subscription_stats(self, user_id: str) -> UsersHttpResult:
        try:
            return UsersHttpResult(self._media.request_stats(user_id))
        except Exception:
            return UsersHttpResult({"total": 0, "completed": 0, "processing": 0, "pending": 0, "failed": 0})


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


__all__ = ["UsersHttpResult", "UsersMediaProjection", "UsersRepository", "UsersService"]
