"""Small provider adapter shared by processor-free translation tasks."""

from __future__ import annotations

import json
from collections.abc import Sequence
from threading import Lock
from typing import Any

from google import genai
from google.genai import types
from openai import OpenAI
from zhipuai import ZhipuAI

from etk.platform import Settings


class ConfiguredBatchTranslation:
    """Translate bounded batches without importing the legacy ``AITranslator``."""

    def __init__(self, settings: Any, *, batch_size: int = 50) -> None:
        if batch_size < 1 or batch_size > 100:
            raise ValueError("translation batch_size must be between 1 and 100")
        self._settings = settings
        self._batch_size = batch_size
        self._lock = Lock()
        self._identity: tuple[str, str, str] | None = None
        self._client: Any = None

    def translate(self, values: Sequence[str]) -> dict[str, str]:
        unique = tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
        if not unique:
            return {}
        settings = self._settings()
        if not isinstance(settings, Settings):
            raise TypeError("translation settings provider returned an invalid object")
        api_key = settings.ai_api_key.get_secret_value().strip()
        if not api_key:
            raise RuntimeError("AI translation provider is not configured")
        results: dict[str, str] = {}
        for offset in range(0, len(unique), self._batch_size):
            chunk = unique[offset : offset + self._batch_size]
            results.update(self._translate_chunk(settings, api_key, chunk))
        return results

    def _translate_chunk(self, settings: Settings, api_key: str, values: tuple[str, ...]) -> dict[str, str]:
        prompt = (
            "将下列影视人物或角色名称翻译成简体中文。保留专有名词的准确性，"
            "不得改变 JSON key。仅返回 JSON 对象，格式为 "
            '{"translations":{"原文":"译文"}}。\n'
            f"翻译模式：{settings.ai_translation_mode}\n"
            f"待翻译名称：{json.dumps(list(values), ensure_ascii=False)}"
        )
        provider = str(settings.ai_provider or "openai").strip().casefold()
        if provider == "openai":
            client = self._cached(
                (provider, api_key, settings.ai_base_url.strip()),
                lambda: OpenAI(
                    api_key=api_key,
                    base_url=settings.ai_base_url.strip() or None,
                    timeout=60.0,
                    max_retries=0,
                ),
            )
            response = client.chat.completions.create(
                model=settings.ai_model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
        elif provider == "zhipuai":
            client = self._cached(
                (provider, api_key, ""),
                lambda: ZhipuAI(api_key=api_key, timeout=60.0, max_retries=0),
            )
            response = client.chat.completions.create(
                model=settings.ai_model_name,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content
        elif provider == "gemini":
            client = self._cached(
                (provider, api_key, ""),
                lambda: genai.Client(api_key=api_key, http_options=types.HttpOptions(timeout=60_000)),
            )
            response = client.models.generate_content(
                model=settings.ai_model_name,
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json"),
            )
            content = response.text
        else:
            raise RuntimeError("AI translation provider is unsupported")
        return _normalize_result(content, values)

    def _cached(self, identity: tuple[str, str, str], factory: Any) -> Any:
        with self._lock:
            if self._client is None or self._identity != identity:
                self._client = factory()
                self._identity = identity
            return self._client


def _normalize_result(content: object, values: tuple[str, ...]) -> dict[str, str]:
    raw = str(content or "").strip()
    if raw.startswith("```"):
        raw = raw.removeprefix("```").removeprefix("json").removesuffix("```").strip()
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("AI translation response is not valid JSON") from exc
    if isinstance(payload, dict) and isinstance(payload.get("translations"), dict):
        payload = payload["translations"]
    if not isinstance(payload, dict):
        raise RuntimeError("AI translation response must be an object")
    allowed = set(values)
    result: dict[str, str] = {}
    for key, value in payload.items():
        normalized_key = str(key)
        normalized_value = str(value or "").strip()
        if normalized_key in allowed and normalized_value:
            result[normalized_key] = normalized_value
    return result


__all__ = ["ConfiguredBatchTranslation"]
