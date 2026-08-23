from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

from etk.platform import Settings

from .configuration import MASKED_SECRET
from .models import SystemResult

logger = logging.getLogger(__name__)

PRO_PRICING_FALLBACK_TIERS = [
    {
        "key": "month",
        "tier": "M",
        "label": "月付",
        "amount": "8.00",
        "amount_cents": 800,
        "currency": "CNY",
        "note": "",
    },
    {
        "key": "year",
        "tier": "Y",
        "label": "年付",
        "amount": "68.00",
        "amount_cents": 6800,
        "currency": "CNY",
        "note": "",
    },
    {
        "key": "lifetime",
        "tier": "L",
        "label": "终身",
        "amount": "188.00",
        "amount_cents": 18800,
        "currency": "CNY",
        "note": "含部署",
    },
]


@dataclass(frozen=True, slots=True)
class ExternalResult:
    payload: Any
    status_code: int = 200


class IntegrationSettingsRepository(Protocol):
    def save_many(self, values: Mapping[str, Any]) -> None: ...


class SystemIntegrationGateway(Protocol):
    def test_ai(self, config: Mapping[str, Any]) -> ExternalResult: ...

    def list_ai_models(self, config: Mapping[str, Any]) -> ExternalResult: ...

    def test_proxy(self, proxy_url: str, tmdb_api_key: str) -> ExternalResult: ...

    def test_telegram(self, token: str, chat_id: str) -> ExternalResult: ...

    def about_info(self) -> tuple[str, list[dict[str, Any]]]: ...

    def pricing(self) -> Mapping[str, Any] | None: ...

    def server_id(self) -> str | None: ...

    def pro_action(self, action: str, license_key: str, server_id: str) -> Mapping[str, Any]: ...

    def update_runtime_license(self, expire_time: str) -> None: ...


class SystemIntegrationService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        repository: IntegrationSettingsRepository,
        gateway: SystemIntegrationGateway,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._gateway = gateway

    def test_ai(self, incoming: Mapping[str, Any]) -> SystemResult:
        config = self._restore_masked(incoming)
        if not config:
            return SystemResult({"success": False, "message": "缺少配置数据"}, 400)
        try:
            result = self._gateway.test_ai(config)
            return SystemResult(result.payload, result.status_code)
        except Exception as exc:
            logger.error("AI connection test failed: %s", exc)
            return SystemResult({"success": False, "message": f"测试失败: {exc}"}, 500)

    def list_ai_models(self, incoming: Mapping[str, Any]) -> SystemResult:
        config = self._restore_masked(incoming)
        if not str(config.get("ai_api_key") or "").strip():
            return SystemResult({"success": False, "message": "请先填写 API Key"}, 400)
        try:
            result = self._gateway.list_ai_models(config)
            return SystemResult(result.payload, result.status_code)
        except Exception as exc:
            logger.exception("Listing AI models failed")
            return SystemResult({"success": False, "message": f"刷新模型列表失败: {exc}"}, 500)

    def test_proxy(self, incoming: Mapping[str, Any]) -> SystemResult:
        proxy_url = str(incoming.get("url") or "").strip()
        if not proxy_url:
            return SystemResult({"success": False, "message": "错误：未提供代理 URL。"}, 400)
        tmdb_api_key = self._settings().tmdb_api_key.get_secret_value().strip()
        if not tmdb_api_key:
            return SystemResult(
                {"success": False, "message": "测试失败：请先在通用设置配置 TMDB API Key。"},
                400,
            )
        try:
            result = self._gateway.test_proxy(proxy_url, tmdb_api_key)
            return SystemResult(result.payload, result.status_code)
        except Exception as exc:
            return SystemResult({"success": False, "message": f"发生未知错误: {exc}"}, 500)

    def test_telegram(self, incoming: Mapping[str, Any]) -> SystemResult:
        token = incoming.get("token")
        if token == MASKED_SECRET:
            token = self._settings().telegram_bot_token.get_secret_value()
        token = str(token or "").strip()
        chat_id = str(incoming.get("chat_id") or "").strip()
        if not token or not chat_id:
            return SystemResult({"success": False, "message": "缺少 Token 或 Chat ID"}, 400)
        try:
            result = self._gateway.test_telegram(token, chat_id)
            return SystemResult(result.payload, result.status_code)
        except Exception as exc:
            logger.error("Telegram connection test failed: %s", exc)
            return SystemResult({"success": False, "message": f"发生未知错误: {exc}"}, 500)

    def about_info(self) -> SystemResult:
        try:
            version, releases = self._gateway.about_info()
            return SystemResult({"current_version": version, "releases": releases})
        except Exception:
            logger.exception("Reading release information failed")
            return SystemResult({"error": "获取版本信息时发生服务器内部错误"}, 500)

    def pricing(self) -> SystemResult:
        try:
            normalized = _normalize_pro_pricing(self._gateway.pricing())
        except Exception as exc:
            logger.warning("Pro pricing gateway failed: %s", exc)
            normalized = None
        return SystemResult(normalized or _fallback_pro_pricing("云端价格暂不可用，已显示默认价格"))

    def activate(self, incoming: Mapping[str, Any]) -> SystemResult:
        return self._pro_action(
            "activate", incoming, empty_message="请输入激活码", failure_message="激活码无效或已被使用"
        )

    def transfer(self, incoming: Mapping[str, Any]) -> SystemResult:
        return self._pro_action(
            "transfer", incoming, empty_message="请输入曾使用过的激活码", failure_message="换绑失败"
        )

    def _pro_action(
        self,
        action: str,
        incoming: Mapping[str, Any],
        *,
        empty_message: str,
        failure_message: str,
    ) -> SystemResult:
        license_key = str(incoming.get("license_key") or "").strip()
        if not license_key:
            return SystemResult({"success": False, "message": empty_message}, 400)
        server_id = self._gateway.server_id()
        if not server_id:
            return SystemResult(
                {"success": False, "message": "无法获取本机 Server ID，请确保 Emby 连接正常"},
                500,
            )
        try:
            result = self._gateway.pro_action(action, license_key, server_id)
            if not result.get("success") or not result.get("is_pro"):
                return SystemResult(
                    {"success": False, "message": result.get("msg", failure_message)},
                    400,
                )
            expire_time = str(result.get("expire_time") or "2099-12-31T23:59:59Z")
            self._repository.save_many({"pro_license_key": license_key, "pro_expire_time": expire_time})
            self._gateway.update_runtime_license(expire_time)
            default_message = "激活成功！" if action == "activate" else "换绑成功！"
            return SystemResult({"success": True, "message": result.get("msg", default_message)})
        except Exception:
            logger.exception("Pro %s request failed", action)
            return SystemResult({"success": False, "message": "连接验证服务器失败，请检查网络"}, 500)

    def _restore_masked(self, incoming: Mapping[str, Any]) -> dict[str, Any]:
        current = self._settings().to_runtime_dict()
        result = dict(incoming)
        for key in Settings.secret_keys:
            if result.get(key) == MASKED_SECRET:
                result[key] = current.get(key, "")
        return result


def _fallback_pro_pricing(message: str) -> dict[str, Any]:
    return {
        "success": True,
        "source": "fallback",
        "currency": "CNY",
        "tiers": PRO_PRICING_FALLBACK_TIERS,
        "fallback": True,
        "message": message,
    }


def _normalize_pro_pricing(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("tiers"), list):
        return None
    normalized: list[dict[str, Any]] = []
    allowed_keys = {"month", "quarter", "year", "lifetime"}
    allowed_tiers = {"M", "Q", "Y", "L"}
    for raw in payload["tiers"]:
        if not isinstance(raw, Mapping):
            continue
        key = str(raw.get("key") or "").strip()
        tier = str(raw.get("tier") or "").strip().upper()
        if key not in allowed_keys or tier not in allowed_tiers:
            continue
        try:
            amount_cents = int(cast(Any, raw.get("amount_cents")))
        except (TypeError, ValueError):
            continue
        if amount_cents <= 0:
            continue
        normalized.append(
            {
                "key": key,
                "tier": tier,
                "label": str(raw.get("label") or key).strip(),
                "amount": f"{amount_cents / 100:.2f}",
                "amount_cents": amount_cents,
                "currency": "CNY",
                "note": str(raw.get("note") or "").strip(),
            }
        )
    if not normalized:
        return None
    order = {"month": 0, "quarter": 1, "year": 2, "lifetime": 3}
    normalized.sort(key=lambda item: order.get(item["key"], 99))
    return {
        "success": True,
        "source": "worker",
        "currency": "CNY",
        "tiers": normalized,
        "fallback": False,
    }
