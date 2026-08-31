from __future__ import annotations

from collections.abc import Callable, Sequence
from threading import Lock
from typing import Any

from google import genai
from google.genai import types
from openai import OpenAI
from zhipuai import ZhipuAI

from etk.platform import Settings


class ConfiguredMediaEmbeddingProvider:
    """Bounded provider adapter for the three embedding backends supported by ETK."""

    def __init__(self, settings: Callable[[], Settings]) -> None:
        self._settings = settings
        self._client_lock = Lock()
        self._client_identity: tuple[str, str, str] | None = None
        self._client: Any = None

    def embed(self, text_value: str) -> Sequence[float] | None:
        text = str(text_value or "").strip()
        if not text:
            return None
        settings = self._settings()
        provider = str(settings.ai_provider or "openai").strip().casefold()
        api_key = settings.ai_api_key.get_secret_value().strip()
        if not api_key:
            raise RuntimeError("AI embedding provider is not configured")
        if provider == "openai":
            return self._openai(settings, api_key, text)
        if provider == "zhipuai":
            return self._zhipu(settings, api_key, text)
        if provider == "gemini":
            return self._gemini(settings, api_key, text)
        raise RuntimeError("AI embedding provider is unsupported")

    @staticmethod
    def _embedding_model(settings: Settings, fallback: str) -> str:
        configured = str(getattr(settings, "ai_embedding_model", "") or "").strip()
        return configured or fallback

    def _openai(self, settings: Settings, api_key: str, text: str) -> Sequence[float]:
        fallback = "BAAI/bge-m3" if "siliconflow" in settings.ai_base_url.casefold() else "text-embedding-3-small"
        base_url = settings.ai_base_url.strip()
        client = self._cached_client(
            ("openai", api_key, base_url),
            lambda: OpenAI(
                api_key=api_key,
                base_url=base_url or None,
                timeout=60.0,
                max_retries=0,
            ),
        )
        response = client.embeddings.create(
            input=text,
            model=self._embedding_model(settings, fallback),
        )
        return tuple(float(item) for item in response.data[0].embedding)

    def _zhipu(self, settings: Settings, api_key: str, text: str) -> Sequence[float]:
        client: Any = self._cached_client(
            ("zhipuai", api_key, ""),
            lambda: ZhipuAI(api_key=api_key, timeout=60.0, max_retries=0),
        )
        response = client.embeddings.create(
            model=self._embedding_model(settings, "embedding-2"),
            input=text,
        )
        return tuple(float(item) for item in response.data[0].embedding)

    def _gemini(self, settings: Settings, api_key: str, text: str) -> Sequence[float]:
        client = self._cached_client(
            ("gemini", api_key, ""),
            lambda: genai.Client(
                api_key=api_key,
                http_options=types.HttpOptions(timeout=60_000),
            ),
        )
        response = client.models.embed_content(
            model=self._embedding_model(settings, "text-embedding-004"),
            contents=text,
            config=types.EmbedContentConfig(title="Movie Overview"),
        )
        if not response.embeddings:
            raise RuntimeError("Gemini embedding response did not contain a vector")
        values = response.embeddings[0].values
        if not values:
            raise RuntimeError("Gemini embedding response did not contain vector values")
        return tuple(float(item) for item in values)

    def _cached_client(self, identity: tuple[str, str, str], factory: Callable[[], Any]) -> Any:
        with self._client_lock:
            if self._client is None or self._client_identity != identity:
                self._client = factory()
                self._client_identity = identity
            return self._client


__all__ = ["ConfiguredMediaEmbeddingProvider"]
