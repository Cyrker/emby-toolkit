from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from etk.platform import Settings

from .configuration import MASKED_SECRET
from .models import SystemResult

logger = logging.getLogger(__name__)

@dataclass(frozen=True, slots=True)
class ExternalResult:
    payload: Any
    status_code: int = 200


class SystemIntegrationGateway(Protocol):
    def test_ai(self, config: Mapping[str, Any]) -> ExternalResult: ...

    def list_ai_models(self, config: Mapping[str, Any]) -> ExternalResult: ...

    def test_proxy(self, proxy_url: str, tmdb_api_key: str) -> ExternalResult: ...

    def test_telegram(self, token: str, chat_id: str) -> ExternalResult: ...

    def about_info(self) -> tuple[str, list[dict[str, Any]]]: ...


class SystemIntegrationService:
    def __init__(
        self,
        *,
        settings: Callable[[], Settings],
        gateway: SystemIntegrationGateway,
    ) -> None:
        self._settings = settings
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

    def _restore_masked(self, incoming: Mapping[str, Any]) -> dict[str, Any]:
        current = self._settings().to_runtime_dict()
        result = dict(incoming)
        for key in Settings.secret_keys:
            if result.get(key) == MASKED_SECRET:
                result[key] = current.get(key, "")
        return result
