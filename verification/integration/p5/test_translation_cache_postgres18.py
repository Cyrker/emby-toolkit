from __future__ import annotations

from sqlalchemy import text

from etk.domains.translation import SqlAlchemyTranslationCache
from etk.platform.db import DatabaseRuntime, EngineConfig

_PREFIX = "etk-p11-translation-cache-"


def test_native_translation_cache_round_trip_and_invalid_row_cleanup(
    migrated_p5_postgres_url: str,
) -> None:
    runtime = DatabaseRuntime.create(
        EngineConfig(
            migrated_p5_postgres_url,
            application_name="etk-p11-translation-cache",
        )
    )
    runtime.start()
    try:
        with runtime.engine.begin() as connection:
            assert str(connection.scalar(text("SHOW server_version_num")) or "").startswith("18")
            connection.execute(
                text("DELETE FROM translation_cache WHERE original_text LIKE :prefix"),
                {"prefix": f"{_PREFIX}%"},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO translation_cache(original_text, translated_text, engine_used)
                    VALUES (:original, 'latin-only', 'legacy-fixture')
                    """
                ),
                {"original": f"{_PREFIX}invalid"},
            )

        cache = SqlAlchemyTranslationCache(runtime.unit_of_work)
        assert (
            cache.save_many(
                {
                    f"{_PREFIX}valid": "有效翻译",
                    f"{_PREFIX}ignored": "latin-only",
                },
                engine_used="openai:fixture",
            )
            == 1
        )
        assert cache.load_many(
            [
                f"{_PREFIX}valid",
                f"{_PREFIX}invalid",
                f"{_PREFIX}missing",
            ]
        ) == {f"{_PREFIX}valid": "有效翻译"}

        assert (
            cache.save_many(
                {f"{_PREFIX}valid": "更新后的翻译"},
                engine_used="gemini:fixture",
            )
            == 1
        )
        with runtime.engine.connect() as connection:
            rows = connection.execute(
                text(
                    """
                    SELECT original_text, translated_text, engine_used
                    FROM translation_cache
                    WHERE original_text LIKE :prefix
                    ORDER BY original_text
                    """
                ),
                {"prefix": f"{_PREFIX}%"},
            ).mappings()
            assert [dict(row) for row in rows] == [
                {
                    "original_text": f"{_PREFIX}valid",
                    "translated_text": "更新后的翻译",
                    "engine_used": "gemini:fixture",
                }
            ]
    finally:
        try:
            with runtime.engine.begin() as connection:
                connection.execute(
                    text("DELETE FROM translation_cache WHERE original_text LIKE :prefix"),
                    {"prefix": f"{_PREFIX}%"},
                )
        finally:
            runtime.shutdown()
