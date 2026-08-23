from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, cast


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be between 1 and 65535")
    return port


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etk", description="Emby Toolkit process entrypoint")
    commands = parser.add_subparsers(dest="command", required=True)

    api = commands.add_parser("api", help="run the FastAPI application host")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=_port, default=5257)
    api.add_argument("--log-level", default="info")
    api.add_argument("--log-format", choices=("console", "json"), default=None)
    api.set_defaults(handler=_run_api)

    proxy = commands.add_parser("proxy", help="run the gevent HTTP/WebSocket proxy host")
    proxy.add_argument("--host", default="0.0.0.0")
    proxy.add_argument("--port", type=_port, default=7758)
    proxy.set_defaults(handler=_run_proxy)

    nginx = commands.add_parser("generate-nginx-config", help="render the Nginx proxy config")
    nginx.add_argument("--output", type=Path, default=Path("/etc/nginx/conf.d/default.conf"))
    nginx.add_argument("--template-dir", type=Path)
    nginx.add_argument("--proxy-upstream", default="127.0.0.1:7758")
    nginx.set_defaults(handler=_run_generate_nginx_config)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args) or 0)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"etk: error: {exc}", file=sys.stderr)
        return 1


def _run_api(args: argparse.Namespace) -> int:
    import os

    import uvicorn

    from etk.platform.observability import configure_logging

    log_format = args.log_format or os.environ.get("ETK_LOG_FORMAT", "console").casefold()
    if log_format not in {"console", "json"}:
        raise ValueError("ETK_LOG_FORMAT must be 'console' or 'json'")
    configure_logging(json_output=log_format == "json", level=args.log_level)
    uvicorn.run(
        "etk.bootstrap.app_factory:create_app",
        factory=True,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        log_config=None,
    )
    return 0


def _run_proxy(args: argparse.Namespace) -> int:
    # Proxy compatibility requires patching before importing requests/socket-using legacy modules.
    from gevent import monkey

    monkey.patch_all()

    import signal

    from gevent import signal_handler, wait
    from gevent.event import Event
    from gevent.pywsgi import WSGIServer
    from geventwebsocket.handler import WebSocketHandler

    import constants

    from .runtime_supervisor import RuntimeRole, get_runtime_supervisor

    runtime = get_runtime_supervisor()
    runtime.start(role=RuntimeRole.PROXY, background=False)
    try:
        config = runtime.config
        if not bool(config.get(constants.CONFIG_OPTION_PROXY_ENABLED, False)):
            print("Proxy is disabled; no listener was started.")
            return 0
        if not bool(config.get("is_pro_active", False)):
            print("Proxy requires an active Pro license; no listener was started.")
            return 0

        from reverse_proxy import proxy_app

        server = WSGIServer(
            (args.host, args.port),
            proxy_app,
            handler_class=WebSocketHandler,
        )
        received_signal: int | None = None
        shutdown_requested = Event()

        def stop_for_signal(signum: int) -> None:
            nonlocal received_signal
            if received_signal is None:
                received_signal = signum
            shutdown_requested.set()

        signal_watchers = (
            signal_handler(signal.SIGTERM, stop_for_signal, signal.SIGTERM),
            signal_handler(signal.SIGINT, stop_for_signal, signal.SIGINT),
        )
        try:
            server.start()
            # Stopping inside the signal callback can race with serve_forever's socket setup.
            stop_event = cast(Any, server)._stop_event
            wait((shutdown_requested, stop_event), count=1)
        finally:
            for watcher in signal_watchers:
                watcher.cancel()
            server.stop(timeout=5)
        return 128 + received_signal if received_signal is not None else 0
    finally:
        runtime.stop()


def _run_generate_nginx_config(args: argparse.Namespace) -> int:
    from .nginx import DEFAULT_TEMPLATE_DIR, generate_nginx_config, load_legacy_config

    output = generate_nginx_config(
        load_legacy_config(),
        output=args.output,
        template_dir=args.template_dir or DEFAULT_TEMPLATE_DIR,
        proxy_upstream=args.proxy_upstream,
    )
    print(f"Nginx config generated: {output}")
    return 0
