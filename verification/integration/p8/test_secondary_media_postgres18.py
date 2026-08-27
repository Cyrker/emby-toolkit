from __future__ import annotations

from sqlalchemy import text

from etk.domains.media.secondary import SecondaryMediaService
from etk.domains.media.user_projection import SqlAlchemyUserMediaProjection
from etk.domains.system.repository import SqlAlchemyAppSettingsRepository
from etk.platform.db import DatabaseRuntime


def test_studio_projection_and_auto_tagging_rules_round_trip(p8_runtime: DatabaseRuntime) -> None:
    projection = SqlAlchemyUserMediaProjection(p8_runtime.unit_of_work)
    settings = SqlAlchemyAppSettingsRepository(p8_runtime.unit_of_work)
    service = SecondaryMediaService(media=projection, settings=settings)
    with p8_runtime.engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO media_metadata (
                    tmdb_id, item_type, title, in_library,
                    production_companies_json, networks_json
                ) VALUES
                    (
                        'studio-1', 'Movie', 'Fixture One', TRUE,
                        '["Alpha Studio", {"name":"Netflix"}]'::jsonb,
                        '[{"name":"Network Alpha"}, "Zeta Network"]'::jsonb
                    ),
                    (
                        'studio-2', 'Series', 'Fixture Two', TRUE,
                        '[{"name":"阿尔法工作室"}, {"missing":"ignored"}]'::jsonb,
                        'null'::jsonb
                    ),
                    (
                        'studio-hidden', 'Movie', 'Not In Library', FALSE,
                        '["Alpha Hidden"]'::jsonb, '[]'::jsonb
                    )
                """
            )
        )

    assert service.search_studios("Net").payload == ["Netflix", "Network Alpha", "Zeta Network"]
    assert service.search_studios("Alpha").payload == ["Alpha Studio", "Network Alpha"]
    assert service.search_studios("阿尔法").payload == ["阿尔法工作室"]
    assert service.search_studios("%").payload == []

    rules = [
        {
            "name": "Fixture Rule",
            "tags": ["已整理", "4K"],
            "library_ids": ["library-a"],
        }
    ]
    assert service.save_auto_tagging_rules(rules).status_code == 200
    assert service.auto_tagging_rules().payload == rules
    with p8_runtime.engine.connect() as connection:
        assert (
            connection.scalar(
                text(
                    """
                SELECT value_json
                FROM app_settings
                WHERE setting_key = 'auto_tagging_rules'
                """
                )
            )
            == rules
        )
