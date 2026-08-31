from __future__ import annotations

from typing import Any

import pytest

from etk.domains.p115.rename_moviepilot import (
    P115MoviePilotRenameExportOperation,
    P115RenameMoviePilotService,
    convert_etk_rename_template_to_mp,
)


class FakeSettings:
    def __init__(self, revisions: list[str] | None = None) -> None:
        self.revisions = revisions or ["revision-1"]

    def revision(self, key: str) -> str:
        assert key == "mp_config"
        if len(self.revisions) > 1:
            return self.revisions.pop(0)
        return self.revisions[0]


class FakeGateway:
    def __init__(self, values: dict[str, Any]) -> None:
        self.values = dict(values)
        self.writes: list[tuple[str, Any]] = []

    def get_system_setting(self, key: str) -> Any:
        return self.values[key]

    def set_system_setting(self, key: str, value: Any) -> object:
        self.writes.append((key, value))
        self.values[key] = value
        return object()


class FakeProgress:
    def __init__(self) -> None:
        self.effects: list[tuple[str, str]] = []

    def checkpoint(self) -> None:
        pass

    def update(self, _progress: int, _message: str) -> None:
        pass

    def record_started_effect(self, label: str) -> None:
        self.effects.append(("started", label))

    def record_committed_effect(self, label: str) -> None:
        self.effects.append(("committed", label))

    def record_abandoned_effect(self, label: str) -> None:
        self.effects.append(("abandoned", label))


def test_moviepilot_import_preserves_legacy_envelope_and_aliases() -> None:
    gateway = FakeGateway(
        {
            "MOVIE_RENAME_FORMAT": "{{ title }}/{{ en_name }}{{ fileExt }}",
            "TV_RENAME_FORMAT": "{{ title }}/Season {{ season }}/{{ videoCodec }}{{ fileExt }}",
        }
    )
    result = P115RenameMoviePilotService(gateway=gateway, settings=FakeSettings()).import_templates()

    assert result.status_code == 200
    assert isinstance(result.payload, dict)
    assert result.payload["data"]["movie_file_template"] == "{{ title_en }}{{ fileExt }}"
    assert result.payload["data"]["tv_file_template"] == "{{ codec }}{{ fileExt }}"


def test_moviepilot_export_builds_templates_and_freezes_config_revision() -> None:
    service = P115RenameMoviePilotService(gateway=FakeGateway({}), settings=FakeSettings())

    result = service.export_parameters(
        {
            "main_dir_template": "{{title_zh}}",
            "season_dir_template": "Season {{season_no}}",
            "movie_file_template": "{{title_zh}}",
            "tv_file_template": "{{title_zh}} {{s_e}}",
        }
    )

    assert result.status_code == 200
    assert isinstance(result.payload, dict)
    assert result.payload["data"] == {
        "movie_template": "{{title}}/{{title}}{{fileExt}}",
        "tv_template": "{{title}}/Season {{ season|string }}.zfill(2)/{{title}} {{season_episode}}{{fileExt}}",
        "configuration_revision": "revision-1",
    }


def test_moviepilot_export_rejects_etk_only_variable() -> None:
    converted, unsupported = convert_etk_rename_template_to_mp("{{ unknown_etk_value }}")

    assert converted == "{{ unknown_etk_value }}"
    assert unsupported == ["unknown_etk_value"]


def test_moviepilot_export_does_not_treat_filter_string_literals_as_variables() -> None:
    converted, unsupported = convert_etk_rename_template_to_mp("{{ title|replace('a', 'b') }}")

    assert converted == "{{ title|replace('a', 'b') }}"
    assert unsupported == []


def test_moviepilot_export_does_not_rewrite_filter_string_literals() -> None:
    converted, unsupported = convert_etk_rename_template_to_mp("{{ title_zh|replace('title_zh', 'x') }}")

    assert converted == "{{ title|replace('title_zh', 'x') }}"
    assert unsupported == []


def test_moviepilot_export_operation_writes_only_drifted_values() -> None:
    gateway = FakeGateway(
        {
            "MOVIE_RENAME_FORMAT": "movie-old",
            "TV_RENAME_FORMAT": "tv-new",
        }
    )
    progress = FakeProgress()
    operation = P115MoviePilotRenameExportOperation(gateway=gateway, settings=FakeSettings())

    result = operation.execute(
        {
            "movie_template": "movie-new",
            "tv_template": "tv-new",
            "configuration_revision": "revision-1",
        },
        progress,
    )

    assert result == {"ok": True, "mp_movie_template": "movie-new", "mp_tv_template": "tv-new"}
    assert gateway.writes == [("MOVIE_RENAME_FORMAT", "movie-new")]
    assert progress.effects == [
        ("started", "moviepilot-system-setting:MOVIE_RENAME_FORMAT"),
        ("committed", "moviepilot-system-setting:MOVIE_RENAME_FORMAT"),
    ]


def test_moviepilot_export_operation_stops_on_configuration_drift() -> None:
    gateway = FakeGateway(
        {
            "MOVIE_RENAME_FORMAT": "movie-old",
            "TV_RENAME_FORMAT": "tv-old",
        }
    )
    operation = P115MoviePilotRenameExportOperation(
        gateway=gateway,
        settings=FakeSettings(["revision-1", "revision-2"]),
    )

    with pytest.raises(RuntimeError, match="发生变化"):
        operation.execute(
            {
                "movie_template": "movie-new",
                "tv_template": "tv-new",
                "configuration_revision": "revision-1",
            },
            FakeProgress(),
        )

    assert gateway.writes == []
