from __future__ import annotations

import importlib
import logging
import re
import shutil
from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

from etk.compatibility.legacy_registry import (
    LegacyProcessors,
    clear_legacy_registry,
    install_legacy_registry,
)

from .runtime_supervisor import RuntimeReloadError, RuntimeRole

logger = logging.getLogger(__name__)


class LegacyRuntimeAdapter:
    """Explicit lifecycle adapter for the temporary plaintext legacy host."""

    def __init__(self) -> None:
        self._config: dict[str, Any] = {}
        self._processors = LegacyProcessors()
        self._monitor: Any = None
        self._role: RuntimeRole | None = None
        self._background = False
        self._started = False

    @property
    def config(self) -> Mapping[str, Any]:
        return self._config

    def start(self, *, role: RuntimeRole, background: bool) -> None:
        if self._started:
            return

        from etk.compatibility.background import resume_background_tasks

        resume_background_tasks()
        importlib.import_module("logger_setup")  # Installs legacy TRACE before config loading.
        import config_manager
        from database import connection

        config_manager.load_config()
        self._config = config_manager.APP_CONFIG
        self._role = role
        self._configure_file_logging()
        connection.init_db()
        self._ensure_cover_generator_fonts()
        self._replace_processors()

        self._background = bool(background and role is RuntimeRole.COORDINATOR)
        if self._background:
            self._start_background_services()
        self._started = True

    def save_config_and_reload(self, new_config: Mapping[str, Any]) -> None:
        import config_manager
        import task_manager

        if task_manager.is_task_running():
            raise RuntimeReloadError(
                "configuration reload is unavailable while a task is running",
                compensated=True,
            )
        with config_manager.dynamic_config_write_lock():
            try:
                previous_persisted, previous_memory = config_manager.capture_dynamic_config_state()
            except Exception as exc:
                raise RuntimeReloadError(
                    "configuration reload could not capture the previous state",
                    compensated=True,
                ) from exc

            persistence_started = False
            runtime_reload_started = False
            try:
                persistence_started = True
                config_manager.save_config(dict(new_config))
                self._config = config_manager.APP_CONFIG
                runtime_reload_started = True
                self._reload_runtime_components()
            except Exception as reload_error:
                rollback_errors: list[Exception] = []
                if persistence_started:
                    try:
                        config_manager.restore_dynamic_config_state(
                            previous_persisted,
                            previous_memory,
                            expected_updates=dict(new_config),
                        )
                    except Exception as exc:
                        rollback_errors.append(exc)
                        logger.exception("Failed to restore the previous persisted configuration")
                    finally:
                        self._config = config_manager.APP_CONFIG
                if runtime_reload_started:
                    try:
                        self._reload_runtime_components()
                    except Exception as exc:
                        rollback_errors.append(exc)
                        logger.exception("Failed to restore the previous compatibility runtime")
                if rollback_errors:
                    failures = ExceptionGroup(
                        "configuration reload and rollback failed",
                        [reload_error, *rollback_errors],
                    )
                    raise RuntimeReloadError(
                        "configuration reload failed and rollback was incomplete",
                        compensated=False,
                    ) from failures
                raise RuntimeReloadError(
                    "configuration reload failed; the previous state was restored",
                    compensated=True,
                ) from reload_error

    def apply_persisted_config_snapshot(self, new_config: Mapping[str, Any]) -> None:
        """Apply an already committed database snapshot without writing it again."""

        import config_manager
        import task_manager

        if task_manager.is_task_running():
            raise RuntimeReloadError(
                "configuration reload is unavailable while a task is running",
                compensated=True,
            )
        previous_memory = deepcopy(config_manager.APP_CONFIG)
        runtime_reload_started = False
        try:
            config_manager.APP_CONFIG.update(deepcopy(dict(new_config)))
            self._config = config_manager.APP_CONFIG
            runtime_reload_started = True
            self._reload_runtime_components()
        except Exception as reload_error:
            config_manager.APP_CONFIG.clear()
            config_manager.APP_CONFIG.update(previous_memory)
            self._config = config_manager.APP_CONFIG
            rollback_errors: list[Exception] = []
            if runtime_reload_started:
                try:
                    self._reload_runtime_components()
                except Exception as exc:
                    rollback_errors.append(exc)
                    logger.exception("Failed to restore the previous compatibility runtime snapshot")
            if rollback_errors:
                raise RuntimeReloadError(
                    "persisted configuration reload failed and rollback was incomplete",
                    compensated=False,
                ) from ExceptionGroup(
                    "persisted configuration reload and rollback failed",
                    [reload_error, *rollback_errors],
                )
            raise RuntimeReloadError(
                "persisted configuration reload failed; the previous snapshot was restored",
                compensated=True,
            ) from reload_error

    def _reload_runtime_components(self) -> None:
        self._replace_processors()
        if not self._background:
            return

        from tasks.p115 import LifeEventMonitorDaemon

        self._restart_monitor()
        LifeEventMonitorDaemon.start_or_update()
        self._refresh_shared_device_event_listener(reason="config_reload")
        self._refresh_telegram_services()
        self._refresh_vector_cache_service()

    def stop(self) -> None:
        errors: list[BaseException] = []

        for processor in self._processor_values():
            self._capture(errors, processor.signal_stop)

        if self._background:
            self._stop_background_services(errors)

        from etk.compatibility.background import stop_background_tasks

        self._capture_result(errors, "compatibility background dispatcher", stop_background_tasks)
        self._raise_shutdown_errors(errors)

        self._close_processors(self._processors, errors)
        self._raise_shutdown_errors(errors)
        self._processors = LegacyProcessors()
        self._capture(errors, clear_legacy_registry)
        self._capture_result(errors, "database pool", self._close_database_pool)
        self._raise_shutdown_errors(errors)

        self._monitor = None
        self._role = None
        self._background = False
        self._started = False
        self._config = {}

    @staticmethod
    def _capture(errors: list[BaseException], function, *args, **kwargs) -> None:
        try:
            function(*args, **kwargs)
        except BaseException as exc:
            errors.append(exc)
            logger.exception("Compatibility runtime cleanup step failed")

    @classmethod
    def _capture_result(cls, errors: list[BaseException], name: str, function, *args, **kwargs) -> None:
        try:
            result = function(*args, **kwargs)
            if result is False:
                raise RuntimeError(f"{name} did not stop before its timeout")
        except BaseException as exc:
            errors.append(exc)
            logger.exception("Compatibility runtime cleanup step failed: %s", name)

    @staticmethod
    def _raise_shutdown_errors(errors: list[BaseException]) -> None:
        if errors:
            raise RuntimeError(f"legacy runtime shutdown had {len(errors)} error(s)") from errors[0]

    def _processor_values(self) -> tuple[Any, ...]:
        return tuple(
            processor
            for processor in (
                self._processors.media,
                self._processors.watchlist,
                self._processors.actor,
            )
            if processor is not None
        )

    @staticmethod
    def _close_processors(processors: LegacyProcessors, errors: list[BaseException] | None = None) -> None:
        captured = errors if errors is not None else []
        seen: set[int] = set()
        for processor in (processors.media, processors.watchlist, processors.actor):
            if processor is None or id(processor) in seen:
                continue
            seen.add(id(processor))
            close = getattr(processor, "close", None)
            if not callable(close):
                continue
            try:
                close()
            except BaseException as exc:
                captured.append(exc)
                logger.exception("Failed to close compatibility processor")
        if errors is None and captured:
            raise RuntimeError(f"failed to close {len(captured)} compatibility processor(s)") from captured[0]

    def _configure_file_logging(self) -> None:
        import config_manager
        import constants
        from logger_setup import add_file_handler

        config_manager.LOG_DIRECTORY = str(Path(config_manager.PERSISTENT_DATA_PATH) / "logs")
        try:
            log_size = int(
                self._config.get(
                    constants.CONFIG_OPTION_LOG_ROTATION_SIZE_MB,
                    constants.DEFAULT_LOG_ROTATION_SIZE_MB,
                )
            )
            log_backups = int(
                self._config.get(
                    constants.CONFIG_OPTION_LOG_ROTATION_BACKUPS,
                    constants.DEFAULT_LOG_ROTATION_BACKUPS,
                )
            )
        except (TypeError, ValueError):
            log_size = constants.DEFAULT_LOG_ROTATION_SIZE_MB
            log_backups = constants.DEFAULT_LOG_ROTATION_BACKUPS
        add_file_handler(
            log_directory=config_manager.LOG_DIRECTORY,
            log_size_mb=log_size,
            log_backups=log_backups,
        )

    @staticmethod
    def _ensure_cover_generator_fonts() -> None:
        import config_manager

        destination = Path(config_manager.PERSISTENT_DATA_PATH) / "cover_generator" / "fonts"
        source = Path(__file__).resolve().parents[3] / "fonts"
        destination.mkdir(parents=True, exist_ok=True)
        for name in ("en_font.ttf", "en_font_multi_1.otf", "zh_font.ttf", "zh_font_multi_1.ttf"):
            target = destination / name
            candidate = source / name
            if not target.is_file() and candidate.is_file():
                shutil.copy2(candidate, target)

    def _replace_processors(self) -> None:
        new_processors, server_id, task_registry = self._build_processors()
        old_processors = self._processors
        try:
            self._close_processors(old_processors)
            install_legacy_registry(
                new_processors,
                emby_server_id=server_id,
                task_registry=task_registry,
            )
        except Exception:
            self._close_processors(new_processors)
            raise
        self._processors = new_processors

    def _build_processors(self) -> tuple[LegacyProcessors, str | None, dict[str, Any]]:
        import constants
        from actor_subscription_processor import ActorSubscriptionProcessor
        from ai_translator import AITranslator
        from core_processor import MediaProcessor
        from handler.douban import DoubanApi
        from watchlist_processor import WatchlistProcessor

        current_config = dict(self._config)
        shared_ai_translator = None
        ai_enabled = any(
            current_config.get(key, False)
            for key in (
                constants.CONFIG_OPTION_AI_TRANSLATE_ACTOR_ROLE,
                constants.CONFIG_OPTION_AI_TRANSLATE_TITLE,
                constants.CONFIG_OPTION_AI_TRANSLATE_OVERVIEW,
                constants.CONFIG_OPTION_AI_TRANSLATE_EPISODE_OVERVIEW,
                constants.CONFIG_OPTION_AI_VECTOR,
            )
        )
        if ai_enabled:
            try:
                shared_ai_translator = AITranslator(current_config)
            except Exception:
                logger.exception("AITranslator compatibility initialization failed")

        shared_douban_api = None
        if getattr(constants, "DOUBAN_API_AVAILABLE", False):
            try:
                shared_douban_api = DoubanApi(
                    cooldown_seconds=current_config.get(
                        constants.CONFIG_OPTION_DOUBAN_DEFAULT_COOLDOWN,
                        2.0,
                    ),
                    user_cookie=current_config.get(constants.CONFIG_OPTION_DOUBAN_COOKIE, ""),
                )
            except Exception:
                logger.exception("Douban compatibility initialization failed")

        server_id = self._resolve_server_id_and_license()
        media = self._construct(
            "MediaProcessor",
            MediaProcessor,
            config=current_config,
            ai_translator=shared_ai_translator,
            douban_api=shared_douban_api,
        )
        watchlist = self._construct(
            "WatchlistProcessor",
            WatchlistProcessor,
            config=current_config,
            ai_translator=shared_ai_translator,
            douban_api=shared_douban_api,
        )
        actor = self._construct(
            "ActorSubscriptionProcessor",
            ActorSubscriptionProcessor,
            config=current_config,
        )
        task_registry: dict[str, Any] = {}
        if self._role is RuntimeRole.WORKER:
            from tasks.core import get_task_registry

            task_registry = dict(get_task_registry())
        return (
            LegacyProcessors(media=media, watchlist=watchlist, actor=actor),
            server_id,
            task_registry,
        )

    @staticmethod
    def _construct(name: str, constructor, **kwargs):
        try:
            return constructor(**kwargs)
        except Exception:
            logger.exception("%s compatibility initialization failed", name)
            return None

    def _resolve_server_id_and_license(self) -> str | None:
        import config_manager
        from database import settings_db
        from handler.emby import get_emby_server_info

        emby_url = self._config.get("emby_server_url")
        emby_key = self._config.get("emby_api_key")
        if not emby_url or not emby_key:
            config_manager.APP_CONFIG["is_pro_active"] = False
            config_manager.APP_CONFIG["pro_tier"] = ""
            config_manager.APP_CONFIG["pro_expire_time"] = ""
            return None

        cached_id = settings_db.get_setting("emby_server_id_cache")
        timeout = 5 if cached_id else 20
        server_info = get_emby_server_info(emby_url, emby_key, timeout=timeout)
        server_id = str((server_info or {}).get("Id") or cached_id or "").strip() or None
        if server_info and server_info.get("Id"):
            try:
                settings_db.save_setting("emby_server_id_cache", cast(Any, server_id))
            except Exception:
                logger.warning("Unable to cache Emby server ID", exc_info=True)

        config_manager.APP_CONFIG["is_pro_active"] = False
        config_manager.APP_CONFIG["pro_tier"] = ""
        config_manager.APP_CONFIG["pro_expire_time"] = ""
        if server_id:
            self._refresh_license_status(server_id)
        return server_id

    def _refresh_license_status(self, server_id: str) -> None:
        import requests

        import config_manager

        try:
            response = requests.post(
                "https://auth.55565576.xyz",
                json={"action": "check", "server_id": server_id},
                timeout=10,
            ).json()
            if response.get("success") and response.get("is_pro"):
                tier = (
                    self._normalize_pro_tier(
                        response.get("tier")
                        or response.get("pro_tier")
                        or response.get("level")
                        or response.get("card_type")
                    )
                    or self._current_license_tier_hint()
                )
                config_manager.APP_CONFIG["is_pro_active"] = True
                config_manager.APP_CONFIG["pro_tier"] = tier
                config_manager.APP_CONFIG["pro_expire_time"] = response.get("expire_time", "")
        except Exception:
            logger.error("Pro compatibility status check failed; using free mode", exc_info=True)

    @staticmethod
    def _normalize_pro_tier(value: Any = "") -> str:
        text = str(value or "").strip().upper()
        aliases = {
            "M": "M",
            "MONTH": "M",
            "MONTHLY": "M",
            "月卡": "M",
            "Y": "Y",
            "YEAR": "Y",
            "YEARLY": "Y",
            "年卡": "Y",
            "L": "L",
            "LIFETIME": "L",
            "FOREVER": "L",
            "永久": "L",
            "终身": "L",
        }
        return aliases.get(text, "")

    def _current_license_tier_hint(self) -> str:
        import config_manager
        from database import settings_db

        for key in ("pro_license_key", "license_key"):
            try:
                value = str(settings_db.get_setting(key) or "").strip().upper()
            except Exception:
                continue
            match = re.search(r"ETK[-_]?([MYL])(?:[-_]|$)", value)
            if match:
                return match.group(1)
            normalized = self._normalize_pro_tier(value)
            if normalized:
                return normalized
        return self._normalize_pro_tier((config_manager.APP_CONFIG or {}).get("pro_license_key"))

    def _start_background_services(self) -> None:
        from tasks.p115 import LifeEventMonitorDaemon

        self._refresh_shared_device_event_listener(reason="startup")
        LifeEventMonitorDaemon.start_or_update()
        self._restart_monitor()
        self._refresh_telegram_services()
        self._refresh_vector_cache_service()

    def _stop_background_services(self, errors: list[BaseException]) -> None:
        from handler import telegram
        from handler.custom_collection import RecommendationEngine
        from handler.intro_detection_service import stop_intro_detection_worker
        from handler.tg_userbot import TGUserBotManager, stop_tg_task_consumer
        from tasks.shared_resource_tasks import stop_shared_device_event_listener

        if self._monitor is not None:
            self._capture_result(errors, "monitor service", self._monitor.stop)
        self._capture_result(errors, "shared-device listener", stop_shared_device_event_listener)
        self._capture_result(errors, "Telegram task consumer", stop_tg_task_consumer)
        self._capture(errors, TGUserBotManager.get_instance().stop)
        self._capture_result(errors, "Telegram bot", telegram.stop_telegram_bot)
        self._capture_result(errors, "vector-cache refresh", RecommendationEngine.stop_auto_refresh_loop)
        self._capture_result(errors, "intro-detection worker", stop_intro_detection_worker)

    def _restart_monitor(self) -> None:
        from monitor_service import MonitorService

        if self._monitor is not None:
            if self._monitor.stop() is False:
                raise RuntimeError("previous monitor service did not stop before its timeout")
            self._monitor = None
        if self._processors.media is not None:
            self._monitor = MonitorService(self._config, self._processors.media)
            self._monitor.start()

    def _refresh_shared_device_event_listener(self, *, reason: str) -> None:
        from database import settings_db
        from tasks.shared_resource_tasks import (
            ensure_shared_device_event_listener,
            stop_shared_device_event_listener,
        )

        try:
            config = settings_db.get_shared_resource_config() or {}
            enabled = self._boolish(config.get("p115_shared_resource_enabled", False))
            if enabled:
                if ensure_shared_device_event_listener() is False:
                    raise RuntimeError("shared listener is enabled but could not start")
            elif stop_shared_device_event_listener() is False:
                raise RuntimeError("shared listener did not stop before its timeout")
        except Exception as exc:
            logger.warning("Shared listener refresh failed (%s)", reason, exc_info=True)
            raise RuntimeError(f"shared listener refresh failed: {reason}") from exc

    def _refresh_telegram_services(self) -> None:
        from handler import telegram
        from handler.tg_userbot import (
            TGUserBotManager,
            start_tg_task_consumer,
        )

        start_tg_task_consumer()
        manager = TGUserBotManager.get_instance()
        if self._config.get("is_pro_active", False):
            telegram.start_telegram_bot()
            manager.start()
        else:
            manager.stop()
            if telegram.stop_telegram_bot() is False:
                raise RuntimeError("Telegram bot did not stop before its timeout")

    def _refresh_vector_cache_service(self) -> None:
        import constants
        from handler.custom_collection import RecommendationEngine

        if self._config.get(constants.CONFIG_OPTION_AI_VECTOR, False):
            RecommendationEngine.start_auto_refresh_loop()
        elif RecommendationEngine.stop_auto_refresh_loop() is False:
            raise RuntimeError("vector-cache refresh did not stop before its timeout")

    @staticmethod
    def _boolish(value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "y", "on", "enabled", "启用", "开启", "是"}:
            return True
        if text in {"0", "false", "no", "n", "off", "disabled", "停用", "关闭", "否"}:
            return False
        return default

    @staticmethod
    def _close_database_pool() -> bool:
        from database import connection

        return connection.close_pool()
