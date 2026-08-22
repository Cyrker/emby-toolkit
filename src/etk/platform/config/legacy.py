from __future__ import annotations

import configparser
import logging
import os
import tempfile
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Protocol

from pydantic import SecretStr

from .settings import BOOTSTRAP_KEYS, Settings

logger = logging.getLogger(__name__)

_BOOTSTRAP_LAYOUT: dict[str, tuple[str, str]] = {
    "db_host": ("Database", "string"),
    "db_port": ("Database", "int"),
    "db_user": ("Database", "string"),
    "db_password": ("Database", "string"),
    "db_name": ("Database", "string"),
    "auth_enabled": ("Authentication", "boolean"),
    "username": ("Authentication", "string"),
}

_ENVIRONMENT_ALIASES: dict[str, str] = {
    "DB_HOST": "db_host",
    "DB_PORT": "db_port",
    "DB_USER": "db_user",
    "DB_PASSWORD": "db_password",
    "DB_NAME": "db_name",
    "TMDB_API_BASE_URL": "tmdb_api_base_url",
}


class _CasePreservingConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


class LegacyDynamicConfigStore(Protocol):
    """Port for the legacy ``app_settings.dynamic_app_config`` record."""

    def load_dynamic_config(self) -> Mapping[str, Any] | None: ...

    def save_dynamic_config(self, values: Mapping[str, Any]) -> None: ...

    def merge_dynamic_config(self, updates: Mapping[str, Any]) -> Mapping[str, Any]: ...


class LegacyConfigAdapter:
    """Load and save legacy configuration without importing legacy modules."""

    def __init__(
        self,
        config_path: str | Path | None,
        *,
        environ: Mapping[str, str] | None = None,
        dynamic_store: LegacyDynamicConfigStore | None = None,
    ) -> None:
        self.config_path = Path(config_path) if config_path is not None else None
        self._environ = environ if environ is not None else os.environ
        self._dynamic_store = dynamic_store

    def load(self, legacy_mapping: Mapping[str, Any] | None = None) -> Settings:
        defaults = Settings()
        first_run = self.config_path is None or not self.config_path.exists()
        parser = self._read_parser()
        ini_document = {section: dict(parser.items(section, raw=True)) for section in parser.sections()}

        bootstrap = {key: defaults.to_runtime_dict()[key] for key in BOOTSTRAP_KEYS}
        if first_run:
            bootstrap["auth_enabled"] = True
        for key, (section, value_type) in _BOOTSTRAP_LAYOUT.items():
            raw = self._find_option(parser, section, key)
            if raw is not None:
                bootstrap[key] = self._parse_ini_value(section, key, raw, value_type)

        for environment, key in _ENVIRONMENT_ALIASES.items():
            raw = self._environ.get(environment)
            if not raw:
                continue
            if key == "db_port":
                try:
                    bootstrap[key] = int(raw)
                except ValueError:
                    logger.error("Ignoring invalid integer environment variable %s", environment)
            else:
                bootstrap[key] = raw

        dynamic_mapping = self._load_dynamic_mapping(legacy_mapping)
        dynamic_keys = set(Settings.dynamic_keys())
        known_keys = set(Settings.model_fields)
        dynamic = {key: defaults.to_runtime_dict()[key] for key in Settings.dynamic_keys()}
        dynamic.update({key: deepcopy(value) for key, value in dynamic_mapping.items() if key in dynamic_keys})
        dynamic_extras = {
            str(key): deepcopy(value) for key, value in dynamic_mapping.items() if str(key) not in known_keys
        }

        values = bootstrap
        values.update(dynamic)
        values.update(dynamic_extras)
        settings = Settings.model_validate(values)
        settings._set_legacy_metadata(
            first_run=first_run,
            ini=ini_document,
        )
        return settings

    def dump_dynamic(self, settings: Settings) -> dict[str, Any]:
        runtime = settings.to_runtime_dict()
        values = {key: deepcopy(runtime[key]) for key in Settings.dynamic_keys()}
        values.update({key: deepcopy(value) for key, value in runtime.items() if key not in Settings.model_fields})
        return values

    def save(self, settings: Settings) -> dict[str, Any]:
        """Persist the legacy dynamic mapping; bootstrap INI writes stay explicit."""

        dynamic = self.dump_dynamic(settings)
        if self._dynamic_store is not None:
            self._dynamic_store.save_dynamic_config(deepcopy(dynamic))
        return dynamic

    def merge_and_save(self, settings: Settings, updates: Mapping[str, Any]) -> Settings:
        dynamic_updates = {key: value for key, value in updates.items() if key not in BOOTSTRAP_KEYS}
        updated = settings.with_updates(dynamic_updates)
        if self._dynamic_store is None:
            return updated
        runtime = updated.to_runtime_dict()
        serialized_updates = {key: deepcopy(runtime[key]) for key in dynamic_updates}
        merged = self._dynamic_store.merge_dynamic_config(serialized_updates)
        return settings.with_updates(merged)

    def write_config_ini(self, settings: Settings) -> None:
        if self.config_path is None:
            raise RuntimeError("config path is not configured")
        parser = self._read_parser(strict=True)
        if not parser.sections():
            parser.read_dict(settings.legacy_ini)
        runtime = settings.to_runtime_dict()
        for key, (section, value_type) in _BOOTSTRAP_LAYOUT.items():
            if not parser.has_section(section):
                parser.add_section(section)
            for existing in tuple(parser.options(section)):
                if existing.casefold() == key.casefold():
                    parser.remove_option(section, existing)
            parser.set(section, key, self._serialize_ini_value(runtime[key], value_type))

        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w",
                encoding="utf-8",
                dir=self.config_path.parent,
                prefix=f".{self.config_path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                parser.write(handle)
                handle.flush()
                os.fsync(handle.fileno())
                temporary_path = Path(handle.name)
            temporary_path.replace(self.config_path)
        finally:
            if temporary_path is not None and temporary_path.exists():
                temporary_path.unlink()

    def _load_dynamic_mapping(self, explicit: Mapping[str, Any] | None) -> dict[str, Any]:
        if explicit is not None:
            return deepcopy(dict(explicit))
        if self._dynamic_store is None:
            return {}
        try:
            return deepcopy(dict(self._dynamic_store.load_dynamic_config() or {}))
        except Exception as exc:
            logger.warning(
                "Unable to load legacy dynamic configuration; using defaults (%s)",
                type(exc).__name__,
            )
            return {}

    def _read_parser(self, *, strict: bool = False) -> configparser.ConfigParser:
        parser = self._new_parser()
        if self.config_path is not None and self.config_path.exists():
            try:
                with self.config_path.open(encoding="utf-8") as handle:
                    parser.read_file(handle)
            except (configparser.Error, OSError, UnicodeError) as exc:
                if strict:
                    raise RuntimeError("Unable to parse existing legacy config") from None
                logger.error(
                    "Unable to parse legacy config; using defaults (%s)",
                    type(exc).__name__,
                )
                return self._new_parser()
        return parser

    @staticmethod
    def _new_parser() -> configparser.ConfigParser:
        return _CasePreservingConfigParser(interpolation=None)

    @staticmethod
    def _find_option(parser: configparser.ConfigParser, section: str, key: str) -> str | None:
        if not parser.has_section(section):
            return None
        for option, value in parser.items(section, raw=True):
            if option.casefold() == key.casefold():
                return value
        return None

    @staticmethod
    def _parse_ini_value(section: str, key: str, raw: str, value_type: str) -> Any:
        try:
            if value_type == "boolean":
                normalized = raw.strip().casefold()
                states = configparser.ConfigParser.BOOLEAN_STATES
                if normalized not in states:
                    raise ValueError(f"not a boolean: {raw!r}")
                return states[normalized]
            if value_type == "int":
                return int(raw)
            return raw
        except ValueError as exc:
            raise ValueError(f"Invalid legacy config value [{section}] {key}") from exc

    @staticmethod
    def _serialize_ini_value(value: Any, value_type: str) -> str:
        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        if value_type == "boolean":
            return "true" if bool(value) else "false"
        if isinstance(value, (list, tuple)):
            return ",".join(str(item) for item in value)
        return str(value)
