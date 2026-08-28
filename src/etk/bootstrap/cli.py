from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any


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

    worker = commands.add_parser("worker", help="run the Celery persistent task Worker")
    worker.add_argument("--log-level", default="INFO")
    worker.add_argument("--concurrency", type=int)
    worker.add_argument("--hostname", default="etk-worker@%h")
    worker.set_defaults(handler=_run_worker)

    coordinator = commands.add_parser("coordinator", help="run the scheduler and Outbox Coordinator")
    coordinator.add_argument(
        "--health-file",
        type=Path,
        default=Path("/tmp/etk-coordinator-health.json"),
    )
    coordinator.set_defaults(handler=_run_coordinator)

    control_plane = commands.add_parser("control-plane", help="run the restricted Docker control plane")
    control_plane.add_argument("--host", default="0.0.0.0")
    control_plane.add_argument("--port", type=_port, default=5260)
    control_plane.add_argument("--log-level", default="info")
    control_plane.set_defaults(handler=_run_control_plane)

    worker_health = commands.add_parser("worker-health", help="probe a live Celery Worker")
    worker_health.add_argument("--timeout", type=float, default=2.0)
    worker_health.set_defaults(handler=_run_worker_health)

    coordinator_health = commands.add_parser("coordinator-health", help="probe Coordinator heartbeat")
    coordinator_health.add_argument(
        "--health-file",
        type=Path,
        default=Path("/tmp/etk-coordinator-health.json"),
    )
    coordinator_health.add_argument("--max-age", type=float, default=10.0)
    coordinator_health.set_defaults(handler=_run_coordinator_health)

    proxy = commands.add_parser("proxy", help="run the native ASGI HTTP/WebSocket proxy host")
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

    os.environ["ETK_PROCESS_ROLE"] = "api"
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
    import os

    import uvicorn

    from etk.platform.config import LegacyConfigAdapter
    from etk.processes.proxy import create_proxy_app

    configured = os.environ.get("APP_DATA_DIR") or os.environ.get("CONFIG_DIR")
    config_path = Path(configured) / "config.ini" if configured else None
    settings = LegacyConfigAdapter(config_path, environ=os.environ).load()
    app = create_proxy_app(
        license_active=bool(settings.proxy_enabled and settings.get("is_pro_active", False)),
        allow_transcoding=settings.proxy_allow_transcoding,
        static_dir=os.environ.get("ETK_STATIC_DIR"),
    )
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", log_config=None)
    return 0


def _run_worker(args: argparse.Namespace) -> int:
    import os

    from etk.processes.celery_app import create_celery_app
    from etk.processes.runtime_config import TaskRuntimeConfig

    os.environ["ETK_PROCESS_ROLE"] = "worker"
    config = TaskRuntimeConfig.from_environ()
    concurrency = args.concurrency or config.worker_concurrency
    if concurrency != 1:
        raise ValueError("legacy.serial Worker concurrency must be exactly 1")
    app = create_celery_app(config)
    return int(
        app.worker_main(
            [
                "worker",
                "--loglevel",
                str(args.log_level).upper(),
                "--concurrency",
                str(concurrency),
                "--hostname",
                str(args.hostname),
                "--queues",
                config.queue_name,
                "--pool",
                "solo",
            ]
        )
        or 0
    )


def _run_control_plane(args: argparse.Namespace) -> int:
    import os

    os.environ["ETK_PROCESS_ROLE"] = "control-plane"
    import uvicorn

    from etk.platform.observability import configure_logging

    configure_logging(json_output=True, level=args.log_level)
    uvicorn.run(
        "etk.processes.control_plane:create_control_plane_app",
        factory=True,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        log_config=None,
    )
    return 0


def _run_coordinator(args: argparse.Namespace) -> int:
    import signal

    from etk.processes.coordinator import CoordinatorRuntime

    runtime = CoordinatorRuntime(health_file=args.health_file)
    received_signal: int | None = None

    def request_stop(signum: int, _frame: Any) -> None:
        nonlocal received_signal
        received_signal = received_signal or signum
        runtime.request_stop()

    previous = {signum: signal.signal(signum, request_stop) for signum in (signal.SIGTERM, signal.SIGINT)}
    try:
        runtime.run()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return 128 + received_signal if received_signal is not None else 0


def _run_worker_health(args: argparse.Namespace) -> int:
    from etk.processes.celery_app import create_celery_app

    if args.timeout <= 0:
        raise ValueError("worker health timeout must be positive")
    responses = create_celery_app().control.ping(timeout=args.timeout)
    return 0 if responses else 1


def _run_coordinator_health(args: argparse.Namespace) -> int:
    from etk.processes.coordinator import coordinator_health

    if args.max_age <= 0:
        raise ValueError("Coordinator health max age must be positive")
    return 0 if coordinator_health(args.health_file, max_age_seconds=args.max_age) else 1


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
