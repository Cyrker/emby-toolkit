"""Small provider adapter shared by processor-free translation tasks."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping, Sequence
from threading import Lock
from typing import Any, Protocol

from google import genai
from google.genai import types
from openai import OpenAI
from sqlalchemy import text
from zhipuai import ZhipuAI

from etk.platform import Settings
from etk.platform.db import SqlAlchemyUnitOfWork

logger = logging.getLogger(__name__)


class TranslationCache(Protocol):
    def load_many(self, values: Sequence[str]) -> Mapping[str, str]: ...

    def save_many(self, values: Mapping[str, str], *, engine_used: str) -> int: ...


class SqlAlchemyTranslationCache:
    """Own the established translation cache without importing legacy database modules."""

    def __init__(self, unit_of_work: Callable[[], SqlAlchemyUnitOfWork]) -> None:
        self._unit_of_work = unit_of_work

    def load_many(self, values: Sequence[str]) -> dict[str, str]:
        requested = tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
        if not requested:
            return {}
        with self._unit_of_work() as uow:
            rows = uow.session.execute(
                text(
                    """
                    SELECT original_text, translated_text
                    FROM translation_cache
                    WHERE original_text = ANY(CAST(:values AS text[]))
                    """
                ),
                {"values": list(requested)},
            ).mappings()
            cached: dict[str, str] = {}
            invalid: list[str] = []
            for row in rows:
                original = str(row.get("original_text") or "").strip()
                translated = str(row.get("translated_text") or "").strip()
                if original and _contains_chinese(translated):
                    cached[original] = translated
                elif original:
                    invalid.append(original)
            if invalid:
                uow.session.execute(
                    text("DELETE FROM translation_cache WHERE original_text = ANY(CAST(:values AS text[]))"),
                    {"values": invalid},
                )
                uow.commit()
            return cached

    def save_many(self, values: Mapping[str, str], *, engine_used: str) -> int:
        prepared = {
            str(original).strip(): str(translated).strip()
            for original, translated in values.items()
            if str(original).strip() and _contains_chinese(str(translated).strip())
        }
        if not prepared:
            return 0
        engine = str(engine_used or "").strip()
        with self._unit_of_work() as uow:
            for original, translated in prepared.items():
                uow.session.execute(
                    text(
                        """
                        INSERT INTO translation_cache (
                            original_text, translated_text, engine_used, last_updated_at
                        ) VALUES (
                            :original_text, :translated_text, :engine_used, NOW()
                        )
                        ON CONFLICT (original_text) DO UPDATE SET
                            translated_text = EXCLUDED.translated_text,
                            engine_used = EXCLUDED.engine_used,
                            last_updated_at = NOW()
                        """
                    ),
                    {
                        "original_text": original,
                        "translated_text": translated,
                        "engine_used": engine,
                    },
                )
            uow.commit()
        return len(prepared)


class ConfiguredBatchTranslation:
    """Translate bounded batches without importing the legacy ``AITranslator``."""

    def __init__(
        self,
        settings: Any,
        cache: TranslationCache | None = None,
        *,
        batch_size: int = 50,
    ) -> None:
        if batch_size < 1 or batch_size > 100:
            raise ValueError("translation batch_size must be between 1 and 100")
        self._settings = settings
        self._cache = cache
        self._batch_size = batch_size
        self._lock = Lock()
        self._identity: tuple[str, str, str] | None = None
        self._client: Any = None

    def translate(self, values: Sequence[str]) -> dict[str, str]:
        unique = tuple(dict.fromkeys(str(value).strip() for value in values if str(value).strip()))
        if not unique:
            return {}
        results = self._load_cached(unique)
        missing = tuple(value for value in unique if value not in results)
        if not missing:
            return results
        settings = self._settings()
        if not isinstance(settings, Settings):
            raise TypeError("translation settings provider returned an invalid object")
        api_key = settings.ai_api_key.get_secret_value().strip()
        if not api_key:
            raise RuntimeError("AI translation provider is not configured")
        translated: dict[str, str] = {}
        for offset in range(0, len(missing), self._batch_size):
            chunk = missing[offset : offset + self._batch_size]
            chunk_result = self._translate_chunk(settings, api_key, chunk)
            self._save_cached(
                chunk_result,
                engine_used=f"{str(settings.ai_provider or 'openai').strip().casefold()}:{settings.ai_model_name}",
            )
            translated.update(chunk_result)
        results.update(translated)
        return results

    def _load_cached(self, values: Sequence[str]) -> dict[str, str]:
        if self._cache is None:
            return {}
        try:
            return {
                str(key): str(value) for key, value in self._cache.load_many(values).items() if str(key) and str(value)
            }
        except Exception as exc:
            logger.warning("Unable to read translation cache (%s)", type(exc).__name__)
            return {}

    def _save_cached(self, values: Mapping[str, str], *, engine_used: str) -> None:
        if self._cache is None or not values:
            return
        try:
            self._cache.save_many(values, engine_used=engine_used)
        except Exception as exc:
            logger.warning("Unable to persist translation cache (%s)", type(exc).__name__)

    def _translate_chunk(self, settings: Settings, api_key: str, values: tuple[str, ...]) -> dict[str, str]:
        prompt = (
            "将下列影视人物或角色名称翻译成简体中文。保留专有名词的准确性，"
            "每个输入都必须给出含中文的译文，人名无通用译名时音译，不得遗漏或原样返回英文。"
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


def _contains_chinese(value: str) -> bool:
    return any(
        "\u3400" <= character <= "\u4dbf" or "\u4e00" <= character <= "\u9fff" or "\uf900" <= character <= "\ufaff"
        for character in value
    )


__all__ = ["ConfiguredBatchTranslation", "SqlAlchemyTranslationCache", "TranslationCache"]
