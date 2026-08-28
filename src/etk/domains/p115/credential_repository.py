from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping
from typing import Any

from sqlalchemy import text

from etk.platform.config.locking import DYNAMIC_CONFIG_LOCK_NAMESPACE, P115_AUTH_LOCK_KEY
from etk.platform.db import SqlAlchemyUnitOfWork

from .models import P115CredentialSnapshot

P115_AUTH_TOKENS_SETTING = "p115_auth_tokens"

_APP_LABELS = {
    "web": "网页版",
    "tv": "安卓电视端",
    "alipaymini": "支付宝小程序",
    "wechatmini": "微信小程序",
}
_COOKIE_ACCOUNT_PATTERN = re.compile(r"(?:^|;\s*)UID=([^;]+)", re.IGNORECASE)


class SqlAlchemyP115CredentialRepository:
    """Own the account-bound 115 credential bundle in one PostgreSQL row."""

    def __init__(
        self,
        unit_of_work: Callable[[], SqlAlchemyUnitOfWork],
        *,
        on_change: Callable[[], None] | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._on_change = on_change

    def load(self) -> P115CredentialSnapshot:
        value = self.load_record()
        return P115CredentialSnapshot(
            access_token=str(value.get("access_token") or "").strip(),
            refresh_token=str(value.get("refresh_token") or "").strip(),
            cookie=str(value.get("cookie") or "").strip(),
            app_type=str(value.get("app_type") or "web").strip() or "web",
            account_id=_resolved_account_id(value) or "",
        )

    def load_record(self) -> dict[str, Any]:
        with self._unit_of_work() as uow:
            raw = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :key"),
                {"key": P115_AUTH_TOKENS_SETTING},
            )
        return _credential_record(raw)

    def save_openapi(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        account_id: str,
        expected_access_token: str | None = None,
    ) -> None:
        self._save(
            access_token=access_token,
            refresh_token=refresh_token,
            account_id=account_id,
            credential_type="openapi",
            expected_access_token=expected_access_token,
        )

    def save_cookie(self, cookie: str, app_type: str) -> None:
        self._save(
            cookie=cookie,
            app_type=app_type,
            credential_type="cookie",
        )

    def save_bundle(
        self,
        access_token: str | None,
        refresh_token: str | None,
        *,
        cookie: str | None = None,
        app_type: str | None = None,
        account_id: str | None = None,
        credential_type: str | None = None,
        expected_access_token: str | None = None,
        expected_refresh_token: str | None = None,
    ) -> bool:
        return self._save(
            access_token=access_token,
            refresh_token=refresh_token,
            cookie=cookie,
            app_type=app_type,
            account_id=account_id,
            credential_type=credential_type,
            expected_access_token=expected_access_token,
            expected_refresh_token=expected_refresh_token,
        )

    def app_label(self, app_type: str) -> str:
        normalized = str(app_type or "web").strip().lower() or "web"
        return _APP_LABELS.get(normalized, str(app_type or "web"))

    def _save(
        self,
        *,
        access_token: str | None = None,
        refresh_token: str | None = None,
        cookie: str | None = None,
        app_type: str | None = None,
        account_id: str | None = None,
        credential_type: str | None = None,
        expected_access_token: str | None = None,
        expected_refresh_token: str | None = None,
    ) -> bool:
        source = _credential_type(
            credential_type,
            access_token=access_token,
            refresh_token=refresh_token,
            cookie=cookie,
            account_id=account_id,
        )
        with self._unit_of_work() as uow:
            uow.session.execute(
                text("SELECT pg_advisory_xact_lock(:namespace, :lock_key)"),
                {
                    "namespace": DYNAMIC_CONFIG_LOCK_NAMESPACE,
                    "lock_key": P115_AUTH_LOCK_KEY,
                },
            )
            uow.session.execute(
                text(
                    """
                    INSERT INTO app_settings (setting_key, value_json, last_updated_at)
                    VALUES (:key, '{}'::jsonb, NOW())
                    ON CONFLICT (setting_key) DO NOTHING
                    """
                ),
                {"key": P115_AUTH_TOKENS_SETTING},
            )
            raw = uow.session.scalar(
                text("SELECT value_json FROM app_settings WHERE setting_key = :key FOR UPDATE"),
                {"key": P115_AUTH_TOKENS_SETTING},
            )
            existing = _credential_record(raw)
            if expected_access_token is not None and existing.get("access_token") != expected_access_token:
                raise RuntimeError("115 OpenAPI 凭据已发生变化，拒绝覆盖较新的授权")
            if expected_refresh_token is not None and existing.get("refresh_token") != expected_refresh_token:
                raise RuntimeError("115 OpenAPI 刷新凭据已发生变化，拒绝覆盖较新的授权")

            payload = _updated_credentials(
                existing,
                source=source,
                access_token=access_token,
                refresh_token=refresh_token,
                cookie=cookie,
                app_type=app_type,
                account_id=account_id,
            )
            changed = payload != existing
            if changed:
                uow.session.execute(
                    text(
                        """
                        UPDATE app_settings
                        SET value_json = CAST(:value AS jsonb), last_updated_at = NOW()
                        WHERE setting_key = :key
                        """
                    ),
                    {
                        "key": P115_AUTH_TOKENS_SETTING,
                        "value": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
                    },
                )
            uow.commit()
        if changed and self._on_change is not None:
            self._on_change()
        return changed


def _updated_credentials(
    existing: Mapping[str, Any],
    *,
    source: str,
    access_token: str | None,
    refresh_token: str | None,
    cookie: str | None,
    app_type: str | None,
    account_id: str | None,
) -> dict[str, Any]:
    existing_account_id = _account_id(existing.get("account_id"))
    existing_cookie = str(existing.get("cookie") or "").strip() or None
    existing_cookie_account_id = _account_id(existing.get("cookie_account_id")) or (
        _cookie_account_id(existing_cookie) if existing_cookie else None
    )
    existing_openapi_account_id = _account_id(existing.get("openapi_account_id"))
    if (
        not existing_openapi_account_id
        and (existing.get("access_token") or existing.get("refresh_token"))
        and not existing_cookie
    ):
        existing_openapi_account_id = existing_account_id

    new_access_token = access_token if access_token is not None else existing.get("access_token")
    new_refresh_token = refresh_token if refresh_token is not None else existing.get("refresh_token")
    new_cookie = cookie if cookie is not None else existing_cookie
    new_cookie = str(new_cookie or "").strip() or None
    new_app_type = app_type if app_type is not None else existing.get("app_type", "web")
    new_openapi_account_id = existing_openapi_account_id
    new_cookie_account_id = existing_cookie_account_id
    resolved_account_id = _account_id(account_id)

    if source in {"cookie", "both"}:
        parsed_cookie_account_id = _cookie_account_id(new_cookie)
        if resolved_account_id and parsed_cookie_account_id and resolved_account_id != parsed_cookie_account_id:
            raise ValueError("115 Cookie 与提供的账号身份不一致")
        new_cookie_account_id = parsed_cookie_account_id or resolved_account_id

    openapi_credentials_supplied = source in {"openapi", "both"} and (
        access_token is not None or refresh_token is not None
    )
    if source in {"openapi", "both"}:
        new_openapi_account_id = (
            resolved_account_id if openapi_credentials_supplied else resolved_account_id or existing_openapi_account_id
        )

    if source in {"cookie", "both"} and new_cookie and not new_cookie_account_id:
        raise ValueError("无法从 115 Cookie 确认稳定账号身份")
    if (
        source in {"openapi", "both"}
        and openapi_credentials_supplied
        and (new_access_token or new_refresh_token)
        and not new_openapi_account_id
    ):
        raise ValueError("无法确认 115 OpenAPI Token 的稳定账号身份")

    if source == "openapi" and new_cookie:
        if not new_openapi_account_id or new_cookie_account_id != new_openapi_account_id:
            new_cookie = None
            new_cookie_account_id = None
            new_app_type = "web"
    elif source == "cookie" and new_cookie and (new_access_token or new_refresh_token):
        if not new_cookie_account_id or new_openapi_account_id != new_cookie_account_id:
            new_access_token = None
            new_refresh_token = None
            new_openapi_account_id = None

    configured_account_ids: list[str | None] = []
    if new_access_token or new_refresh_token:
        configured_account_ids.append(new_openapi_account_id)
    if new_cookie:
        configured_account_ids.append(new_cookie_account_id)
    configured_ids = {value for value in configured_account_ids if value}
    if len(configured_ids) > 1:
        raise ValueError("115 Token 与 Cookie 属于不同账号")
    unified_account_id = next(iter(configured_ids)) if configured_account_ids and all(configured_account_ids) else None
    if not configured_account_ids:
        unified_account_id = resolved_account_id or existing_account_id

    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "cookie": new_cookie,
        "app_type": new_app_type,
        "account_id": unified_account_id,
        "openapi_account_id": new_openapi_account_id if (new_access_token or new_refresh_token) else None,
        "cookie_account_id": new_cookie_account_id if new_cookie else None,
    }


def _resolved_account_id(value: Mapping[str, Any]) -> str | None:
    metadata_account_id = _account_id(value.get("account_id"))
    has_openapi = bool(value.get("access_token") or value.get("refresh_token"))
    has_cookie = bool(str(value.get("cookie") or "").strip())
    openapi_account_id = _account_id(value.get("openapi_account_id"))
    cookie_account_id = _account_id(value.get("cookie_account_id")) or _cookie_account_id(value.get("cookie"))

    if has_openapi and not openapi_account_id and not has_cookie:
        openapi_account_id = metadata_account_id
    if has_cookie and not cookie_account_id:
        return None
    if has_openapi and has_cookie and not openapi_account_id:
        return None

    configured_ids: list[str | None] = []
    if has_openapi:
        configured_ids.append(openapi_account_id)
    if has_cookie:
        configured_ids.append(cookie_account_id)
    if any(not account_id for account_id in configured_ids):
        return None
    unique_ids = {account_id for account_id in configured_ids if account_id}
    if len(unique_ids) > 1:
        return None
    resolved = next(iter(unique_ids), metadata_account_id or cookie_account_id)
    if metadata_account_id and resolved and metadata_account_id != resolved:
        return None
    return resolved


def _credential_type(
    value: str | None,
    *,
    access_token: str | None,
    refresh_token: str | None,
    cookie: str | None,
    account_id: str | None,
) -> str:
    source = str(value or "").strip().lower()
    if not source:
        if access_token is not None or refresh_token is not None:
            source = "openapi"
        elif cookie is not None:
            source = "cookie"
        elif account_id is not None:
            source = "openapi"
    if source not in {"", "openapi", "cookie", "both"}:
        raise ValueError(f"未知 115 凭据类型: {value}")
    return source


def _credential_record(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    raise TypeError("p115_auth_tokens must contain a JSON object")


def _cookie_account_id(cookie: object) -> str | None:
    match = _COOKIE_ACCOUNT_PATTERN.search(str(cookie or ""))
    if match is None:
        return None
    return _account_id(match.group(1).partition("_")[0])


def _account_id(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized if normalized.isdigit() else None


__all__ = ["P115_AUTH_TOKENS_SETTING", "SqlAlchemyP115CredentialRepository"]
