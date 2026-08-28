from __future__ import annotations

import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TEMPLATE_DIR = REPO_ROOT / "templates" / "nginx"
DEFAULT_OUTPUT = Path("/etc/nginx/conf.d/default.conf")
DISABLED_MARKER = "# Proxy disabled in config.ini\n"


class NginxConfigError(RuntimeError):
    pass


def load_legacy_config() -> dict[str, Any]:
    from etk.platform.config import LegacyConfigAdapter

    configured = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
    config_path = Path(configured) / "config.ini" if configured else None
    return LegacyConfigAdapter(config_path, environ=os.environ).load().to_runtime_dict()


def generate_nginx_config(
    config: Mapping[str, Any],
    *,
    output: Path = DEFAULT_OUTPUT,
    template_dir: Path = DEFAULT_TEMPLATE_DIR,
    proxy_upstream: str = "127.0.0.1:7758",
) -> Path:
    content = render_nginx_config(
        config,
        template_dir=template_dir,
        proxy_upstream=proxy_upstream,
    )
    _atomic_write(output, content)
    return output


def render_nginx_config(
    config: Mapping[str, Any],
    *,
    template_dir: Path = DEFAULT_TEMPLATE_DIR,
    proxy_upstream: str = "127.0.0.1:7758",
) -> str:
    from jinja2 import Environment, FileSystemLoader, StrictUndefined

    import constants

    if not _boolish(config.get(constants.CONFIG_OPTION_PROXY_ENABLED, False)):
        return DISABLED_MARKER

    emby_url = str(config.get(constants.CONFIG_OPTION_EMBY_SERVER_URL) or "").strip()
    emby_upstream = emby_url.removeprefix("http://").removeprefix("https://").rstrip("/")
    if not emby_upstream or any(char.isspace() for char in emby_upstream):
        raise NginxConfigError("proxy is enabled but emby_server_url is missing or invalid")

    normalized_proxy_upstream = str(proxy_upstream or "").strip()
    if not normalized_proxy_upstream or any(char.isspace() for char in normalized_proxy_upstream):
        raise NginxConfigError("proxy upstream is missing or invalid")

    try:
        listen_port = int(config.get(constants.CONFIG_OPTION_PROXY_PORT, 8097))
    except (TypeError, ValueError) as exc:
        raise NginxConfigError("proxy_port must be an integer") from exc
    if not 1 <= listen_port <= 65535:
        raise NginxConfigError("proxy_port must be between 1 and 65535")

    try:
        template = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=False,
            undefined=StrictUndefined,
        ).get_template("emby_proxy.conf.template")
        return template.render(
            EMBY_UPSTREAM=emby_upstream,
            PROXY_UPSTREAM=normalized_proxy_upstream,
            NGINX_LISTEN_PORT=listen_port,
            NGINX_MAX_BODY_SIZE="128m",
        )
    except NginxConfigError:
        raise
    except Exception as exc:
        raise NginxConfigError(f"failed to render Nginx config: {exc}") from exc


def _atomic_write(path: Path, content: str) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        dir=destination.parent,
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _boolish(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "enabled", "启用", "开启", "是"}
    return bool(value)
