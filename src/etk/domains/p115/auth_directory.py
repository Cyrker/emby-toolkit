from __future__ import annotations

import base64
import binascii
import json
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from etk.platform import Settings

from .models import (
    P115AuthorizationPoll,
    P115AuthorizationSession,
    P115CredentialSnapshot,
    P115Result,
)

_SESSION_TTL_SECONDS = 600
OPENAPI_AUTH_ERROR_CODES = frozenset({"40140123", "40140124", "40140125", "40140126"})


class P115AuthorizationGateway(Protocol):
    def create_openapi_qrcode(self, client_id: str) -> tuple[dict[str, Any], P115AuthorizationSession]: ...

    def poll_openapi_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll: ...

    def create_cookie_qrcode(self, app_type: str) -> tuple[dict[str, Any], P115AuthorizationSession]: ...

    def poll_cookie_qrcode(self, session: P115AuthorizationSession) -> P115AuthorizationPoll: ...

    def check_openapi(self, access_token: str) -> Mapping[str, Any]: ...

    def refresh_openapi(self, refresh_token: str) -> Mapping[str, Any]: ...

    def check_cookie(self, cookie: str, app_type: str) -> Mapping[str, Any]: ...


class P115CredentialStore(Protocol):
    def load(self) -> P115CredentialSnapshot: ...

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None: ...

    def save_cookie(self, cookie: str, app_type: str) -> None: ...

    def app_label(self, app_type: str) -> str: ...


class P115DirectoryGateway(Protocol):
    def list_entries(
        self,
        *,
        cid: int,
        search: str,
        expected_account_id: str,
    ) -> Mapping[str, Any] | None: ...


class P115AuthorizationSessionStore:
    """Process-local QR state with explicit expiry and synchronized replacement."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._values: dict[str, P115AuthorizationSession] = {}

    def put(self, flow: str, session: P115AuthorizationSession) -> None:
        with self._lock:
            self._values[flow] = session

    def get(self, flow: str) -> P115AuthorizationSession | None:
        with self._lock:
            value = self._values.get(flow)
            if value is None:
                return None
            if self._clock() - value.created_at > _SESSION_TTL_SECONDS:
                self._values.pop(flow, None)
                return None
            return value

    def discard(self, flow: str, session: P115AuthorizationSession) -> None:
        with self._lock:
            if self._values.get(flow) == session:
                self._values.pop(flow, None)


class P115AuthDirectoryService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        authorization: P115AuthorizationGateway,
        credentials: P115CredentialStore,
        directories: P115DirectoryGateway,
        sessions: P115AuthorizationSessionStore | None = None,
    ) -> None:
        self._settings = settings
        self._authorization = authorization
        self._credentials = credentials
        self._directories = directories
        self._sessions = sessions or P115AuthorizationSessionStore()
        self._status_lock = threading.Lock()

    def create_openapi_qrcode(self) -> P115Result:
        client_id = str(self._settings().p115_app_id or "").strip()
        if not client_id:
            return P115Result(
                {"success": False, "message": "未配置自定义 AppID，请先在设置中保存"},
                400,
            )
        try:
            data, session = self._authorization.create_openapi_qrcode(client_id)
        except Exception:
            return P115Result({"success": False, "message": "获取二维码失败"}, 500)
        self._sessions.put("openapi", session)
        return P115Result(
            {
                "success": True,
                "data": {"qrcode": data.get("qrcode"), "uid": data.get("uid")},
            }
        )

    def poll_openapi_qrcode(self) -> P115Result:
        session = self._sessions.get("openapi")
        if session is None:
            return P115Result({"success": True, "status": "waiting", "message": "等待扫码..."})
        try:
            outcome = self._authorization.poll_openapi_qrcode(session)
        except Exception as exc:
            return P115Result(
                {"success": False, "status": "error", "message": str(exc)},
                500,
            )
        if outcome.status == "success":
            if outcome.access_token and outcome.refresh_token:
                account_id = _account_id(outcome.user_info)
                try:
                    self._credentials.save_openapi(
                        outcome.access_token,
                        outcome.refresh_token,
                        account_id=account_id,
                    )
                except Exception as exc:
                    return P115Result(
                        {
                            "success": False,
                            "status": "error",
                            "message": f"授权信息无法绑定到稳定的 115 账号: {exc}",
                        },
                        500,
                    )
            self._sessions.discard("openapi", session)
            return P115Result({"success": True, "status": "success", "message": "授权成功！"})
        if outcome.status == "expired":
            self._sessions.discard("openapi", session)
            return P115Result({"success": False, "status": "expired", "message": "二维码已过期，请重新获取"})
        if outcome.status == "waiting":
            return P115Result({"success": True, "status": "waiting", "message": "等待扫码..."})
        return P115Result(
            {
                "success": False,
                "status": "error",
                "message": outcome.message or "检查状态失败",
            },
            500,
        )

    def create_cookie_qrcode(self, app_type: str) -> P115Result:
        normalized_app = str(app_type or "alipaymini").strip() or "alipaymini"
        try:
            data, session = self._authorization.create_cookie_qrcode(normalized_app)
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        self._sessions.put("cookie", session)
        return P115Result({"success": True, "data": {"qrcode": data.get("qrcode")}})

    def poll_cookie_qrcode(self, app_type: str) -> P115Result:
        session = self._sessions.get("cookie")
        if session is None:
            return P115Result({"success": False, "status": "expired", "message": "请先获取二维码"})
        requested_app = str(app_type or "").strip()
        if requested_app and requested_app != session.app_type:
            return P115Result({"success": False, "status": "expired", "message": "请先获取二维码"})
        try:
            outcome = self._authorization.poll_cookie_qrcode(session)
        except Exception as exc:
            return P115Result(
                {"success": False, "status": "error", "message": str(exc)},
                500,
            )
        if outcome.status == "success":
            try:
                self._credentials.save_cookie(outcome.cookie, outcome.app_type)
            except Exception as exc:
                return P115Result(
                    {"success": False, "status": "error", "message": str(exc)},
                    500,
                )
            self._sessions.discard("cookie", session)
            return P115Result({"success": True, "status": "success", "message": "Cookie 获取成功！"})
        if outcome.status == "expired":
            self._sessions.discard("cookie", session)
            return P115Result({"success": False, "status": "expired", "message": "二维码已过期"})
        if outcome.status == "waiting":
            return P115Result(
                {
                    "success": True,
                    "status": "waiting",
                    "message": outcome.message or "等待扫码...",
                }
            )
        return P115Result(
            {
                "success": False,
                "status": "error",
                "message": outcome.message or "登录失败",
            },
            500 if outcome.status == "error" else 200,
        )

    def save_manual_cookie(self, cookie: str, app_type: str) -> P115Result:
        try:
            self._credentials.save_cookie(
                str(cookie or "").strip(),
                str(app_type or "web").strip() or "web",
            )
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        return P115Result({"success": True, "message": "Cookie 已保存"})

    def save_callback_authorization(self, encoded: str) -> P115Result:
        value = str(encoded or "").strip()
        if not value:
            return P115Result("授权失败：缺少 Token 数据", 400, "text/html")
        try:
            decoded = base64.b64decode(value)
            token_data = json.loads(decoded.decode("utf-8"))
            if not isinstance(token_data, Mapping):
                raise ValueError("授权数据必须是对象")
            access_token = str(token_data.get("access_token") or "").strip()
            refresh_token = str(token_data.get("refresh_token") or "").strip()
            claimed_account_id = _account_id(token_data)
            if not access_token or not refresh_token:
                return P115Result("无效的授权码格式：缺少 token", 400, "text/html")
            identity_response = self._authorization.check_openapi(access_token)
            identity_data = identity_response.get("data")
            verified_account_id = (
                _account_id(identity_data)
                if identity_response.get("state") and isinstance(identity_data, Mapping)
                else ""
            )
            if not verified_account_id:
                raise ValueError("无法通过 115 OpenAPI Token 确认稳定账号身份")
            if claimed_account_id and claimed_account_id != verified_account_id:
                raise ValueError("授权数据中的账号身份与 Token 不一致")
            self._credentials.save_openapi(
                access_token,
                refresh_token,
                account_id=verified_account_id,
            )
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            return P115Result(f"解析授权码失败: {exc}", 400, "text/html")
        except Exception as exc:
            return P115Result(f"解析授权码失败: {exc}", 400, "text/html")
        return P115Result(_authorization_success_html(), media_type="text/html")

    def status(self) -> P115Result:
        try:
            credentials = self._credentials.load()
            app_type = str(credentials.app_type or "web").strip().lower() or "web"
            app_label = self._credentials.app_label(app_type)
            data: dict[str, Any] = {
                "has_token": credentials.has_openapi,
                "has_cookie": credentials.has_cookie,
                "cookie_valid": None,
                "cookie_app_type": app_type if credentials.has_cookie else None,
                "cookie_app_label": app_label if credentials.has_cookie else None,
                "valid": False,
                "msg": "",
                "user_info": None,
            }
            if credentials.has_openapi:
                try:
                    with self._status_lock:
                        credentials, response = self._check_status_openapi()
                    if response.get("state"):
                        raw_user_info = response.get("data")
                        user_info = (
                            {str(key): value for key, value in raw_user_info.items()}
                            if isinstance(raw_user_info, Mapping)
                            else {}
                        )
                        data.update(valid=True, msg="Token 有效 (OpenAPI)", user_info=user_info)
                        account_id = _account_id(user_info)
                        if account_id:
                            self._credentials.save_openapi(
                                None,
                                None,
                                account_id=account_id,
                                expected_access_token=credentials.access_token,
                            )
                        if credentials.has_cookie:
                            try:
                                cookie_response = self._authorization.check_cookie(credentials.cookie, app_type)
                                if cookie_response.get("state"):
                                    data["cookie_valid"] = True
                                    data["msg"] = f"Token + Cookie 均有效（{app_label}）"
                                else:
                                    data["cookie_valid"] = False
                                    data["msg"] = f"Token 有效，但 Cookie 已失效（{app_label}）"
                            except Exception:
                                data["msg"] = "Token 有效，Cookie 状态未知"
                        return P115Result({"status": "success", "data": data})
                    data["msg"] = "OpenAPI 授权无效或续期失败，请重新授权；无法获取容量。"
                except Exception:
                    data["msg"] = "OpenAPI 状态查询或续期失败，无法获取容量，请稍后重试或重新授权。"
            if credentials.has_cookie and data["user_info"] is None:
                try:
                    response = self._authorization.check_cookie(credentials.cookie, app_type)
                    if response.get("state"):
                        data.update(
                            valid=not credentials.has_openapi,
                            cookie_valid=True,
                            msg=(data["msg"] + " Cookie 播放授权仍有效。")
                            if credentials.has_openapi
                            else "仅配置 Cookie (播放专用)",
                            user_info={"user_name": "Cookie用户(正常)"},
                        )
                        return P115Result({"status": "success", "data": data})
                    data["msg"] = "Cookie 已失效或被风控拦截"
                except Exception as exc:
                    data["msg"] = f"Cookie 检查失败: {exc}"
            if not credentials.has_openapi and not credentials.has_cookie:
                data["msg"] = "未配置任何凭证"
            return P115Result({"status": "success", "data": data})
        except Exception as exc:
            return P115Result({"status": "error", "message": str(exc)}, 500)

    def _check_status_openapi(self) -> tuple[P115CredentialSnapshot, Mapping[str, Any]]:
        return check_openapi_credentials(self._credentials, self._authorization)

    def list_directories(self, *, cid_value: object, search: str) -> P115Result:
        try:
            cid = int(str(cid_value or 0))
        except (TypeError, ValueError):
            cid = 0
        credentials = self._credentials.load()
        try:
            response = self._directories.list_entries(
                cid=cid,
                search=str(search or "").strip(),
                expected_account_id=credentials.account_id,
            )
        except Exception as exc:
            return P115Result({"success": False, "message": str(exc)}, 500)
        if response is None:
            return P115Result(
                {"status": "error", "message": "无法初始化 115 客户端，请检查凭证"},
                500,
            )
        if not response.get("state"):
            return P115Result(
                {
                    "success": False,
                    "message": response.get("error_msg", response.get("message", "获取失败")),
                },
                500,
            )
        directories = []
        for raw in response.get("data") or []:
            if not isinstance(raw, Mapping) or not _is_directory(raw):
                continue
            directory_id = raw.get("fid") or raw.get("file_id") or raw.get("id") or raw.get("cid")
            if directory_id is None:
                continue
            directories.append(
                {
                    "id": str(directory_id),
                    "name": raw.get("fn") or raw.get("file_name") or raw.get("n") or raw.get("name"),
                    "parent_id": raw.get("pid") or raw.get("parent_id") or str(cid),
                }
            )
        current_name = "根目录"
        path = response.get("path")
        if cid and isinstance(path, list) and path and isinstance(path[-1], Mapping):
            current = path[-1]
            current_name = (
                current.get("file_name") or current.get("fn") or current.get("n") or current.get("name") or "未知目录"
            )
        return P115Result(
            {
                "success": True,
                "data": directories,
                "current": {"id": str(cid), "name": current_name},
            }
        )


def check_openapi_credentials(
    store: P115CredentialStore, authorization: P115AuthorizationGateway
) -> tuple[P115CredentialSnapshot, Mapping[str, Any]]:
    """状态页和后台任务共用：仅明确授权失效才续期一次，保存时比较旧 Token。"""
    credentials = store.load()
    response = authorization.check_openapi(credentials.access_token)
    if (
        response.get("state")
        or str(response.get("code")) not in OPENAPI_AUTH_ERROR_CODES
        or not credentials.refresh_token
        or not credentials.account_id.isdigit()
    ):
        return credentials, response
    # 续期不可盲重试；先重读并在保存时比较旧 Token，避免覆盖并发重新授权。
    latest = store.load()
    if latest != credentials:
        return latest, authorization.check_openapi(latest.access_token)
    renewed = authorization.refresh_openapi(credentials.refresh_token)
    tokens = renewed.get("data")
    if not renewed.get("state") or not isinstance(tokens, Mapping):
        return credentials, {"state": False, "code": renewed.get("code"),
                             "message": renewed.get("message") or renewed.get("msg") or "115 Token 续期失败"}
    access = str(tokens.get("access_token") or "").strip()
    refresh = str(tokens.get("refresh_token") or "").strip()
    if not access or not refresh:
        return credentials, {"state": False, "message": "115 Token 续期响应缺少必要字段"}
    verified = authorization.check_openapi(access)
    info = verified.get("data")
    account = _account_id(info) if isinstance(info, Mapping) else ""
    if not verified.get("state") or not account or account != credentials.account_id:
        raise ValueError("续期结果无法确认原账号身份")
    store.save_openapi(
        access, refresh, account_id=account, expected_access_token=credentials.access_token
    )
    latest = store.load()
    if latest.access_token != access:
        return latest, authorization.check_openapi(latest.access_token)
    return latest, verified


def _account_id(user_info: Mapping[str, Any]) -> str:
    value = str(user_info.get("user_id") or "").strip()
    return value if value.isdigit() else ""


def _is_directory(item: Mapping[str, Any]) -> bool:
    item_type = item.get("fc")
    if item_type is None:
        item_type = item.get("file_category")
    if item_type is None:
        icon = str(item.get("ico") or item.get("icon") or "").lower()
        if icon in {"folder", "dir", "directory"} or str(item.get("is_dir")).lower() in {"1", "true"}:
            item_type = "0"
    return str(item_type) == "0"


def _authorization_success_html() -> str:
    return (
        "\n            <!DOCTYPE html>\n"
        '            <html lang="zh-CN">\n'
        "            <head>\n"
        '                <meta charset="UTF-8">\n'
        '                <meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        "                <title>授权成功</title>\n"
        "                <style>\n"
        "                    body { font-family: system-ui, sans-serif; background: #f0f2f5; "
        "display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }\n"
        "                    .card { background: white; padding: 40px; border-radius: 12px; "
        "box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; }\n"
        "                    h2 { color: #18a058; margin-top: 0; }\n"
        "                    p { color: #666; font-size: 14px; }\n"
        "                </style>\n"
        "            </head>\n"
        "            <body>\n"
        '                <div class="card">\n'
        "                    <h2>➜ 授权成功！</h2>\n"
        "                    <p>Token 已自动保存到 embytool 系统。</p>\n"
        '                    <p style="color: #999; font-size: 12px;">本窗口将在 3 秒后自动关闭...</p>\n'
        "                </div>\n"
        "                <script>\n"
        "                    setTimeout(function() { window.close(); }, 3000);\n"
        "                </script>\n"
        "            </body>\n"
        "            </html>\n"
        "            "
    )


__all__ = [
    "P115AuthDirectoryService",
    "P115AuthorizationGateway",
    "P115AuthorizationSessionStore",
    "P115CredentialStore",
    "P115DirectoryGateway",
]
