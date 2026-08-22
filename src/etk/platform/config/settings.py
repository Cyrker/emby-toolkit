from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any, ClassVar

from pydantic import Field, PrivateAttr, SecretStr
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict

from ..observability.redaction import redact_sensitive

BOOTSTRAP_KEYS = (
    "auth_enabled",
    "db_host",
    "db_name",
    "db_password",
    "db_port",
    "db_user",
    "username",
)


class Settings(BaseSettings):
    """Validated, side-effect-free application configuration snapshot."""

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_prefix="",
        extra="allow",
        frozen=True,
        hide_input_in_errors=True,
        validate_default=True,
    )

    auth_enabled: bool = False
    db_host: str = "localhost"
    db_name: str = "emby_toolkit"
    db_password: SecretStr = SecretStr("your_password")
    db_port: int = 5432
    db_user: str = "postgres"
    username: str = "admin"

    accept_language: str = "zh-CN,zh;q=0.9,en;q=0.8"
    actor_main_role_only: bool = False
    actor_role_add_prefix: bool = False
    ai_api_key: SecretStr = SecretStr("")
    ai_base_url: str = "https://api.siliconflow.cn/v1"
    ai_joke_fallback: bool = False
    ai_model_name: str = "Qwen/Qwen2.5-72B-Instruct"
    ai_provider: str = "openai"
    ai_recognition: bool = False
    ai_translate_actor_role: bool = False
    ai_translate_episode_overview: bool = False
    ai_translate_overview: bool = False
    ai_translate_title: bool = False
    ai_translation_mode: str = "fast"
    ai_vector: bool = False
    api_douban_default_cooldown_seconds: float = 1.0
    delay_between_items_sec: float = 0.5
    douban_cookie: SecretStr = SecretStr("")
    douban_enable_online_api: bool = True
    emby_api_key: SecretStr = SecretStr("")
    emby_api_timeout: int = 60
    emby_auth_mode: str = ""
    emby_public_url: str = ""
    emby_server_url: str = ""
    emby_user_id: str = ""
    etk_server_url: str = "http://192.168.1.X:5257"
    etk_server_url_manual: str = ""
    extract_thumb: bool = False
    github_token: SecretStr = SecretStr("")
    keyword_to_tags: bool = False
    libraries_to_process: list[str] = Field(default_factory=list)
    local_strm_root: str = "/mnt/media"
    log_rotation_backup_count: int = 10
    log_rotation_size_mb: int = 5
    max_actors_to_process: int = 50
    max_episode_actors_to_process: int = 0
    media_image_archive_enabled: bool = False
    media_image_archive_path: str = ""
    monitor_enabled: bool = False
    monitor_exclude_dirs: list[str] = Field(default_factory=list)
    monitor_exclude_refresh_delay: int = 0
    monitor_extensions: list[str] = Field(
        default_factory=lambda: [".mp4", ".mkv", ".avi", ".mov", ".iso", ".ts", ".strm"]
    )
    monitor_paths: list[str] = Field(default_factory=list)
    monitor_scan_batch_size: int = 2
    monitor_scan_max_tasks: int = 10
    monitor_sha1_pc_search: bool = True
    network_http_proxy_url: str = ""
    network_proxy_enabled: bool = False
    p115_api_priority: str = "openapi"
    p115_app_id: str = ""
    p115_copy_play_enabled: bool = False
    p115_download_subs: bool = True
    p115_enable_sync_delete: bool = False
    p115_extensions: list[str] = Field(
        default_factory=lambda: ["mp4", "mkv", "avi", "ts", "iso", "mov", "m2ts", "srt", "ass", "ssa"]
    )
    p115_local_cleanup: bool = False
    p115_max_workers: int = 3
    p115_media_root_cid: str = "0"
    p115_media_root_name: str = "媒体库"
    p115_mediainfo_assisted_recognition: bool = False
    p115_min_video_size: int = 10
    p115_mp_classify: bool = False
    p115_request_interval: float = 1.0
    p115_save_path_cid: str = "0"
    p115_save_path_name: str = "根目录"
    p115_unrecognized_cid: str = "0"
    p115_unrecognized_name: str = "未识别"
    proxy_allow_transcoding: bool = False
    proxy_discovery_url_manual: str = ""
    proxy_enabled: bool = False
    proxy_lock_discovery_address: bool = False
    proxy_merge_native_libraries: bool = True
    proxy_native_view_order: str = "before"
    proxy_native_view_selection: list[str] = Field(default_factory=list)
    proxy_port: int = 8097
    proxy_show_missing_placeholders: bool = False
    remove_actors_without_avatars: bool = True
    studio_to_chinese: bool = False
    system_update_helper_image: str = "hbq0405/emby-toolkit:latest"
    system_update_strategy: str = "docker_helper"
    task_chain_cron: str = "0 2 * * *"
    task_chain_enabled: bool = False
    task_chain_low_freq_cron: str = "0 5 * * 0"
    task_chain_low_freq_enabled: bool = False
    task_chain_low_freq_max_runtime_minutes: int = 0
    task_chain_low_freq_sequence: list[str] = Field(default_factory=list)
    task_chain_max_runtime_minutes: int = 0
    task_chain_sequence: list[str] = Field(default_factory=list)
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_channel_id: str = ""
    telegram_notify_types: list[str] = Field(
        default_factory=lambda: ["library_new", "transfer_success", "recognize_fail", "intercept_notify"]
    )
    tg_menu_tasks: list[str] = Field(
        default_factory=lambda: [
            "task-chain-high-freq",
            "task-chain-low-freq",
            "scan-organize-115",
            "populate-metadata",
            "process-watchlist",
            "scan-cleanup-issues",
            "system-auto-update",
        ]
    )
    tmdb_api_base_url: str = "https://api.themoviedb.org/3"
    tmdb_api_key: SecretStr = SecretStr("")
    tmdb_image_language_preference: str = "zh"
    tmdb_include_adult: bool = False
    user_agent: str = "Mozilla/5.0 ..."

    _first_run: bool = PrivateAttr(default=False)
    _legacy_ini: dict[str, dict[str, str]] = PrivateAttr(default_factory=dict)

    secret_keys: ClassVar[frozenset[str]] = frozenset(
        {
            "ai_api_key",
            "db_password",
            "douban_cookie",
            "emby_api_key",
            "github_token",
            "telegram_bot_token",
            "tmdb_api_key",
        }
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        del settings_cls, env_settings, dotenv_settings, file_secret_settings
        return (init_settings,)

    @property
    def is_first_run(self) -> bool:
        return self._first_run

    @property
    def legacy_ini(self) -> dict[str, dict[str, str]]:
        return deepcopy(self._legacy_ini)

    @classmethod
    def dynamic_keys(cls) -> tuple[str, ...]:
        return tuple(key for key in cls.model_fields if key not in BOOTSTRAP_KEYS)

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_runtime_dict().get(key, default)

    def to_runtime_dict(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for key in self.__class__.model_fields:
            values[key] = _reveal_secret(getattr(self, key))
        for key, value in (self.__pydantic_extra__ or {}).items():
            values[key] = _reveal_secret(value)
        return values

    def redacted_dict(self) -> dict[str, Any]:
        return redact_sensitive(self.to_runtime_dict())

    def with_updates(self, updates: Mapping[str, Any]) -> Settings:
        values = self.to_runtime_dict()
        values.update(deepcopy(dict(updates)))
        updated = self.__class__.model_validate(values)
        updated._set_legacy_metadata(
            first_run=self._first_run,
            ini=self._legacy_ini,
        )
        return updated

    def __repr__(self) -> str:
        return f"Settings({self.redacted_dict()!r})"

    __str__ = __repr__

    def _set_legacy_metadata(
        self,
        *,
        first_run: bool,
        ini: Mapping[str, Mapping[str, str]],
    ) -> None:
        object.__setattr__(self, "_first_run", bool(first_run))
        object.__setattr__(
            self,
            "_legacy_ini",
            {section: dict(values) for section, values in ini.items()},
        )


def _reveal_secret(value: Any) -> Any:
    if isinstance(value, SecretStr):
        return value.get_secret_value()
    if isinstance(value, list):
        return [_reveal_secret(item) for item in value]
    if isinstance(value, tuple):
        return [_reveal_secret(item) for item in value]
    if isinstance(value, Mapping):
        return {str(key): _reveal_secret(item) for key, item in value.items()}
    return deepcopy(value)
