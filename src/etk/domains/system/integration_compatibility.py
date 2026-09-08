from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

import httpx

from etk.platform import Settings

from .integrations import ExternalResult

_PRO_AUTH_WORKER_URL = "https://auth.55565576.xyz"


class _SettingsReader(Protocol):
    def load(self, key: str) -> Any: ...


class HttpxSystemIntegrationGateway:
    def __init__(self, settings: Callable[[], Settings], repository: _SettingsReader) -> None:
        self._settings = settings
        self._repository = repository

    def test_ai(self, config: Mapping[str, Any]) -> ExternalResult:
        from ai_translator import AITranslator

        test_text = "Bald Qiang"
        result = AITranslator(dict(config)).translate(test_text)
        if result and result != test_text:
            return ExternalResult({"success": True, "message": f"连接成功！测试翻译结果: '{test_text}' ➜ '{result}'"})
        if result == test_text:
            return ExternalResult({"success": True, "message": "连接成功，但 AI 返回了原词（可能模型认为无需翻译）。"})
        return ExternalResult({"success": False, "message": "AI 未返回有效结果。"}, 500)

    def list_ai_models(self, config: Mapping[str, Any]) -> ExternalResult:
        provider = str(config.get("ai_provider") or "openai").strip()
        api_key = str(config.get("ai_api_key") or "").strip()
        base_url = str(config.get("ai_base_url") or "").strip()
        headers = {"Accept": "application/json"}
        params: dict[str, str] | None = None
        if provider == "gemini":
            base = base_url.rstrip("/") if base_url else "https://generativelanguage.googleapis.com/v1beta"
            url = base if base.endswith("/models") else f"{base}/models"
            params = {"key": api_key}
        else:
            url = f"{_normalize_openai_base_url(base_url, provider)}/models"
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            with self._client(timeout=20) as client:
                response = client.get(url, headers=headers, params=params)
            if response.status_code in {401, 403}:
                return ExternalResult(
                    {"success": False, "message": "认证失败，请检查 API Key 是否正确。"},
                    response.status_code,
                )
            response.raise_for_status()
            try:
                payload = response.json()
            except ValueError:
                return ExternalResult(
                    {"success": False, "message": "模型接口未返回 JSON 数据，请检查 API Base URL 是否正确。"},
                    500,
                )
            models = _extract_model_ids(payload, provider)
            return ExternalResult({"success": True, "models": models, "count": len(models)})
        except httpx.ConnectTimeout:
            return ExternalResult({"success": False, "message": "连接模型接口超时，请检查网络或代理配置。"}, 500)
        except httpx.ProxyError as exc:
            return ExternalResult({"success": False, "message": f"代理连接失败: {exc}"}, 500)
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text[:300]
            return ExternalResult(
                {"success": False, "message": f"模型接口返回 HTTP {exc.response.status_code}。{detail}"},
                500,
            )
        except httpx.HTTPError as exc:
            return ExternalResult({"success": False, "message": f"请求模型接口失败: {exc}"}, 500)

    def test_proxy(self, proxy_url: str, tmdb_api_key: str) -> ExternalResult:
        try:
            with httpx.Client(proxy=proxy_url, timeout=10) as client:
                response = client.get(
                    "https://api.themoviedb.org/3/configuration",
                    params={"api_key": tmdb_api_key},
                )
            if response.status_code == 401:
                return ExternalResult(
                    {"success": False, "message": "代理连接成功，但 TMDB API Key 无效或错误。"},
                    401,
                )
            response.raise_for_status()
            return ExternalResult({"success": True, "message": "代理和 API Key 均测试成功！"})
        except httpx.ConnectTimeout:
            return ExternalResult({"success": False, "message": "连接代理服务器超时，请检查地址和端口。"}, 500)
        except httpx.ProxyError as exc:
            return ExternalResult({"success": False, "message": f"代理错误: {exc}"}, 500)
        except httpx.HTTPStatusError as exc:
            return ExternalResult(
                {
                    "success": False,
                    "message": f"HTTP 错误: 代理连接成功，但 TMDB 返回了 {exc.response.status_code} 状态码。",
                },
                500,
            )
        except httpx.HTTPError as exc:
            return ExternalResult({"success": False, "message": f"网络请求失败: {exc}"}, 500)

    def test_telegram(self, token: str, chat_id: str) -> ExternalResult:
        final_chat_id = chat_id.strip()
        if final_chat_id.startswith("https://t.me/"):
            username = final_chat_id.rsplit("/", 1)[-1]
            if username:
                final_chat_id = f"@{username}"
        payload = {
            "chat_id": final_chat_id,
            "text": (
                "🔔 *Emby Toolkit 测试消息*\n\n这是一条测试消息，证明您的机器人配置正确。"
                f"\n⏱ 时间: `{time.strftime('%Y-%m-%d %H:%M:%S')}`"
            ),
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": True,
        }
        try:
            with self._client(timeout=15) as client:
                response = client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    json=payload,
                )
            if response.status_code == 200:
                return ExternalResult({"success": True, "message": "测试消息发送成功！请检查您的 Telegram。"})
            return ExternalResult(
                {"success": False, "message": f"发送失败 (HTTP {response.status_code}): {response.text}"},
                500,
            )
        except httpx.ProxyError:
            return ExternalResult({"success": False, "message": "代理连接失败，请检查网络代理设置。"}, 500)
        except httpx.ConnectTimeout:
            return ExternalResult({"success": False, "message": "连接 Telegram 服务器超时。"}, 500)

    def about_info(self) -> tuple[str, list[dict[str, Any]]]:
        import constants

        headers = {
            "Accept": "application/vnd.github.v3+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        token = self._settings().github_token.get_secret_value().strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            with self._client(timeout=20) as client:
                response = client.get(
                    f"https://api.github.com/repos/{constants.GITHUB_REPO_OWNER}/{constants.GITHUB_REPO_NAME}/releases",
                    headers=headers,
                )
            response.raise_for_status()
            payload = response.json()
            releases = [
                {
                    "version": item.get("tag_name"),
                    "published_at": item.get("published_at"),
                    "changelog": item.get("body"),
                    "url": item.get("html_url"),
                }
                for item in payload
                if isinstance(item, Mapping)
            ]
        except (httpx.HTTPError, ValueError, TypeError):
            releases = []
        return constants.APP_VERSION, releases

    def pricing(self) -> Mapping[str, Any] | None:
        url = os.getenv("ETK_PRO_AUTH_WORKER_URL", _PRO_AUTH_WORKER_URL).strip() or _PRO_AUTH_WORKER_URL
        with self._client(timeout=5) as client:
            response = client.post(url, json={"action": "pricing"})
        if not response.is_success:
            return None
        payload = response.json()
        return payload if isinstance(payload, Mapping) else None

    def server_id(self) -> str | None:
        value = str(self._repository.load("emby_server_id_cache") or "").strip()
        return value or None

    def pro_action(self, action: str, license_key: str, server_id: str) -> Mapping[str, Any]:
        with self._client(timeout=10) as client:
            response = client.post(
                _PRO_AUTH_WORKER_URL,
                json={"action": action, "license_key": license_key, "server_id": server_id},
            )
        payload = response.json()
        return payload if isinstance(payload, Mapping) else {}

    def _client(self, *, timeout: float) -> httpx.Client:
        settings = self._settings()
        proxy = settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""
        return httpx.Client(proxy=proxy or None, timeout=timeout)


def _normalize_openai_base_url(base_url: str, provider: str) -> str:
    normalized = base_url.strip().rstrip("/")
    if not normalized:
        normalized = "https://open.bigmodel.cn/api/paas/v4" if provider == "zhipuai" else "https://api.openai.com/v1"
    for suffix in ("/chat/completions", "/completions", "/responses", "/models"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
    return normalized.rstrip("/")


def _extract_model_ids(payload: Any, provider: str) -> list[str]:
    raw_models: list[Any] = []
    if isinstance(payload, Mapping):
        for key in ("data", "models", "items"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                raw_models = candidate
                break
    elif isinstance(payload, list):
        raw_models = payload
    result: list[str] = []
    seen: set[str] = set()
    for item in raw_models:
        model_id: Any = None
        if isinstance(item, str):
            model_id = item
        elif isinstance(item, Mapping):
            model_id = item.get("id") or item.get("name") or item.get("model")
            if provider == "gemini":
                methods = item.get("supportedGenerationMethods") or item.get("supported_generation_methods")
                if isinstance(methods, list) and "generateContent" not in methods:
                    continue
        normalized = str(model_id or "").strip()
        if provider == "gemini" and normalized.startswith("models/"):
            normalized = normalized.split("/", 1)[1]
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result
