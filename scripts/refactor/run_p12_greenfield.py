#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, LiteralString

import httpx
import psycopg
from psycopg import sql

PROJECT_PATTERN = re.compile(r"etk-p12-[a-z0-9][a-z0-9-]{2,48}\Z")
EXPECTED_SERVICES = frozenset({"api", "control-plane", "coordinator", "db", "migrate", "redis", "worker"})
APP_SERVICES = frozenset({"api", "control-plane", "coordinator", "migrate", "worker"})
EXPECTED_VOLUME_KEYS = frozenset({"app_config", "control_plane_runtime", "pg18_data", "redis7_data"})
EXPECTED_NETWORK_KEYS = frozenset({"etk-greenfield"})
EXPECTED_SERVICE_VOLUME_MOUNTS = {
    "api": frozenset({("app_config", "/config", False)}),
    "control-plane": frozenset(
        {
            ("app_config", "/config", False),
            ("control_plane_runtime", "/run/etk-control-plane", False),
        }
    ),
    "coordinator": frozenset({("app_config", "/config", False)}),
    "db": frozenset({("pg18_data", "/var/lib/postgresql", False)}),
    "migrate": frozenset({("app_config", "/config", False)}),
    "redis": frozenset({("redis7_data", "/data", False)}),
    "worker": frozenset(
        {
            ("app_config", "/config", False),
            ("control_plane_runtime", "/run/etk-control-plane", True),
        }
    ),
}
EXPECTED_ALEMBIC_HEAD = "0027_p11_watchlist_display"
EXPECTED_PUBLIC_TABLE_COUNT = 78
GREENFIELD_ADMIN_ID = "p12-fixture-admin"
TERMINAL_JOB_STATUSES = frozenset({"succeeded", "failed", "timed_out", "cancelled"})
FORBIDDEN_EXTERNAL_ENV_MARKERS = (
    "EMBY_SERVER_URL",
    "EMBY_API_KEY",
    "TMDB_API_KEY",
    "P115_COOKIE",
    "MOVIEPILOT",
    "TELEGRAM",
    "HDHIVE",
    "SHARED_CENTER",
)


class GreenfieldHarnessError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class HarnessConfig:
    repo: Path
    revision: str
    allow_root: Path
    evidence_root: Path
    source_root: Path
    project: str
    api_port: int
    pg_port: int
    fake_docker_socket: Path
    compose_file: Path
    override_file: Path
    image_id: str

    @property
    def database_url(self) -> str:
        return f"postgresql://etk:etk-greenfield-only@127.0.0.1:{self.pg_port}/etk"


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=False).relative_to(root.resolve(strict=False))
    except ValueError:
        return False
    return True


def _validated_new_root(value: str, *, label: str, required_prefix: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise GreenfieldHarnessError(f"{label} must be an absolute path")
    resolved = path.resolve(strict=False)
    if resolved == Path("/") or len(resolved.parts) < 3:
        raise GreenfieldHarnessError(f"{label} is too broad")
    if not resolved.name.startswith(required_prefix):
        raise GreenfieldHarnessError(f"{label} must use the {required_prefix!r} disposable prefix")
    if resolved.exists():
        raise GreenfieldHarnessError(f"{label} already exists; refusing to reuse state")
    if not resolved.parent.is_dir():
        raise GreenfieldHarnessError(f"{label} parent directory does not exist")
    return resolved


def _validated_project(value: str) -> str:
    project = str(value or "").strip().lower()
    if PROJECT_PATTERN.fullmatch(project) is None:
        raise GreenfieldHarnessError("Compose project must use an isolated etk-p12-* name")
    if any(marker in project for marker in ("prod", "production", "mainline")):
        raise GreenfieldHarnessError("Compose project resembles a non-disposable environment")
    return project


def _validated_port(value: int, *, label: str) -> int:
    port = int(value)
    if port < 1024 or port > 65535:
        raise GreenfieldHarnessError(f"{label} must be an unprivileged TCP port")
    return port


def _assert_port_available(port: int) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as exc:
            raise GreenfieldHarnessError(f"loopback port {port} is already in use") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run(
    args: list[str],
    *,
    cwd: Path,
    output: Path | None = None,
    environment: dict[str, str] | None = None,
    timeout: float = 900,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        args,
        cwd=cwd,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout,
        check=False,
    )
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(completed.stdout, encoding="utf-8")
    if check and completed.returncode != 0:
        suffix = f"; see {output}" if output is not None else ""
        raise GreenfieldHarnessError(f"command failed with exit {completed.returncode}: {args[0]}{suffix}")
    return completed


def _docker_json(args: list[str], *, cwd: Path) -> Any:
    completed = _run(args, cwd=cwd)
    try:
        return json.loads(completed.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise GreenfieldHarnessError(
            f"Docker returned invalid JSON for {args[1] if len(args) > 1 else args[0]}"
        ) from exc


def _snapshot_repository(repo: Path, revision: str, source_root: Path) -> str:
    top_level = Path(_run(["git", "rev-parse", "--show-toplevel"], cwd=repo).stdout.strip()).resolve(strict=True)
    if top_level != repo:
        raise GreenfieldHarnessError(f"repository root mismatch: expected {repo}, got {top_level}")
    resolved_revision = _run(
        ["git", "rev-parse", "--verify", f"{revision}^{{commit}}"],
        cwd=repo,
    ).stdout.strip()
    source_root.mkdir(parents=True)
    process = subprocess.Popen(
        ["git", "archive", "--format=tar", resolved_revision],
        cwd=repo,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as archive:
            archive.extractall(source_root, filter="data")
    finally:
        process.stdout.close()
    stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr is not None else ""
    return_code = process.wait(timeout=120)
    if return_code != 0:
        raise GreenfieldHarnessError(f"git archive failed: {stderr.strip()}")
    return resolved_revision


def _compose_environment() -> dict[str, str]:
    environment = dict(os.environ)
    environment.update(
        {
            "ETK_COORDINATOR_SCHEDULES_ENABLED": "false",
            "ETK_CONTROL_PLANE_EMBY_ALLOWED_SERVICES": "",
            "ETK_INTERNAL_NETWORK": "false",
        }
    )
    return environment


def _runtime_override(
    *,
    image_ref: str,
    api_port: int,
    pg_port: int,
    fake_docker_socket: Path,
) -> str:
    image = json.dumps(image_ref)
    socket_path = json.dumps(str(fake_docker_socket))
    services: list[str] = []
    for service in sorted(EXPECTED_SERVICES):
        lines = [f"  {service}:", "    labels:", "      com.etk.environment: greenfield"]
        if service in APP_SERVICES:
            lines.append(f"    image: {image}")
        if service == "api":
            lines.extend(["    ports: !override", f'      - "127.0.0.1:{api_port}:5257"'])
        elif service == "db":
            lines.extend(["    ports: !override", f'      - "127.0.0.1:{pg_port}:5432"'])
        elif service == "migrate":
            lines.extend(
                [
                    "    volumes: !override",
                    "      - type: volume",
                    "        source: app_config",
                    "        target: /config",
                ]
            )
        elif service == "control-plane":
            lines.extend(
                [
                    "    volumes: !override",
                    "      - type: volume",
                    "        source: app_config",
                    "        target: /config",
                    "      - type: volume",
                    "        source: control_plane_runtime",
                    "        target: /run/etk-control-plane",
                    "      - type: bind",
                    f"        source: {socket_path}",
                    "        target: /var/run/docker.sock",
                    "        read_only: true",
                ]
            )
        services.append("\n".join(lines))
    return (
        "services:\n"
        + "\n".join(services)
        + "\nnetworks:\n"
        + "  etk-greenfield:\n"
        + "    labels:\n"
        + "      com.etk.environment: greenfield\n"
    )


def _failure_override() -> str:
    return 'services:\n  migrate:\n    command: ["sh", "-c", "echo P12_INJECTED_MIGRATION_FAILURE >&2; exit 42"]\n'


def _compose_args(config: HarnessConfig, *args: str, extra_file: Path | None = None) -> list[str]:
    command = [
        "docker",
        "compose",
        "--project-name",
        config.project,
        "--file",
        str(config.compose_file),
        "--file",
        str(config.override_file),
    ]
    if extra_file is not None:
        command.extend(["--file", str(extra_file)])
    command.extend(args)
    return command


def _redacted_environment(environment: dict[str, Any]) -> dict[str, Any]:
    output: dict[str, Any] = {}
    for key, value in sorted(environment.items()):
        upper = key.upper()
        if any(marker in upper for marker in ("PASSWORD", "TOKEN", "SECRET", "API_KEY", "COOKIE")):
            output[key] = "<redacted>" if str(value or "") else ""
        elif key == "ETK_DATABASE_URL":
            output[key] = "postgresql+psycopg://etk:<redacted>@db:5432/etk"
        else:
            output[key] = value
    return output


def _validate_resolved_compose(
    payload: dict[str, Any],
    *,
    allow_root: Path,
    fake_docker_socket: Path,
    project: str,
    image_ref: str,
    api_port: int,
    pg_port: int,
) -> dict[str, Any]:
    if payload.get("name") != project:
        raise GreenfieldHarnessError("resolved Compose project name does not match the disposable project")
    services = payload.get("services")
    if not isinstance(services, dict) or set(services) != EXPECTED_SERVICES:
        raise GreenfieldHarnessError("resolved Compose services do not match the Greenfield topology")
    expected_ports = {"api": (api_port, 5257), "db": (pg_port, 5432)}
    bind_mounts: list[dict[str, Any]] = []
    volume_mounts: dict[str, list[dict[str, Any]]] = {}
    for service_name, service in services.items():
        if not isinstance(service, dict):
            raise GreenfieldHarnessError(f"resolved service {service_name} is invalid")
        labels = service.get("labels") or {}
        if labels.get("com.etk.environment") != "greenfield":
            raise GreenfieldHarnessError(f"service {service_name} lacks the Greenfield ownership label")
        if service_name in APP_SERVICES and service.get("image") != image_ref:
            raise GreenfieldHarnessError(f"service {service_name} is not pinned to the built image ID")
        if (
            service_name in {"db", "redis"}
            and re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", str(service.get("image") or "")) is None
        ):
            raise GreenfieldHarnessError(f"service {service_name} image is not digest-pinned")
        environment = service.get("environment") or {}
        if not isinstance(environment, dict):
            raise GreenfieldHarnessError(f"service {service_name} environment is invalid")
        for key, value in environment.items():
            if (
                any(marker in str(key).upper() for marker in FORBIDDEN_EXTERNAL_ENV_MARKERS)
                and str(value or "").strip()
            ):
                raise GreenfieldHarnessError(f"service {service_name} contains external configuration {key}")
        observed_volume_mounts: set[tuple[str, str, bool]] = set()
        observed_bind_mounts: list[dict[str, Any]] = []
        for mount in service.get("volumes") or []:
            if not isinstance(mount, dict):
                raise GreenfieldHarnessError(f"service {service_name} contains an invalid mount")
            mount_type = str(mount.get("type") or "")
            target = str(mount.get("target") or "")
            read_only = bool(mount.get("read_only", False))
            if mount_type == "bind":
                source = Path(str(mount.get("source") or ""))
                if not source.is_absolute() or not _is_within(source, allow_root):
                    raise GreenfieldHarnessError(f"service {service_name} bind mount escapes the allow-root")
                observed_bind_mounts.append(
                    {
                        "service": service_name,
                        "source": str(source.resolve(strict=False)),
                        "target": target,
                        "read_only": read_only,
                    }
                )
            elif mount_type == "volume":
                observed_volume_mounts.add((str(mount.get("source") or ""), target, read_only))
            else:
                raise GreenfieldHarnessError(f"service {service_name} contains unsupported mount type {mount_type!r}")
        if observed_volume_mounts != EXPECTED_SERVICE_VOLUME_MOUNTS[service_name]:
            raise GreenfieldHarnessError(f"service {service_name} volume mounts do not match the disposable topology")
        expected_bind_mounts: list[dict[str, Any]] = []
        if service_name == "control-plane":
            expected_bind_mounts = [
                {
                    "service": "control-plane",
                    "source": str(fake_docker_socket.resolve(strict=False)),
                    "target": "/var/run/docker.sock",
                    "read_only": True,
                }
            ]
        if observed_bind_mounts != expected_bind_mounts:
            raise GreenfieldHarnessError(f"service {service_name} bind mounts do not match the disabled control plane")
        bind_mounts.extend(observed_bind_mounts)
        volume_mounts[service_name] = [
            {"source": source, "target": target, "read_only": read_only}
            for source, target, read_only in sorted(observed_volume_mounts)
        ]
        service_networks = service.get("networks") or {}
        if isinstance(service_networks, dict):
            service_network_keys = set(service_networks)
        elif isinstance(service_networks, list):
            service_network_keys = set(service_networks)
        else:
            raise GreenfieldHarnessError(f"service {service_name} networks are invalid")
        if service_network_keys != EXPECTED_NETWORK_KEYS:
            raise GreenfieldHarnessError(f"service {service_name} is not isolated to the Greenfield network")
        ports = service.get("ports") or []
        if service_name in expected_ports:
            if len(ports) != 1:
                raise GreenfieldHarnessError(f"service {service_name} must publish exactly one loopback port")
            port = ports[0]
            published, target = expected_ports[service_name]
            if (
                str(port.get("host_ip") or "") != "127.0.0.1"
                or int(port.get("published") or 0) != published
                or int(port.get("target") or 0) != target
            ):
                raise GreenfieldHarnessError(f"service {service_name} port is not loopback-isolated")
        elif ports:
            raise GreenfieldHarnessError(f"service {service_name} unexpectedly publishes a port")
    coordinator_environment = services["coordinator"].get("environment") or {}
    if str(coordinator_environment.get("ETK_COORDINATOR_SCHEDULES_ENABLED") or "").lower() != "false":
        raise GreenfieldHarnessError("Coordinator schedules are not disabled")
    volumes = payload.get("volumes")
    if not isinstance(volumes, dict) or set(volumes) != EXPECTED_VOLUME_KEYS:
        raise GreenfieldHarnessError("resolved Compose volumes do not match the disposable topology")
    volume_names: list[str] = []
    for key, volume in volumes.items():
        if volume.get("external"):
            raise GreenfieldHarnessError(f"volume {key} is external")
        if (volume.get("labels") or {}).get("com.etk.environment") != "greenfield":
            raise GreenfieldHarnessError(f"volume {key} lacks the Greenfield ownership label")
        name = str(volume.get("name") or "")
        if name != f"{project}_{key}":
            raise GreenfieldHarnessError(f"volume {key} resolved to an unexpected name")
        volume_names.append(name)
    networks = payload.get("networks")
    if not isinstance(networks, dict) or set(networks) != EXPECTED_NETWORK_KEYS:
        raise GreenfieldHarnessError("resolved Compose networks do not match the disposable topology")
    network = networks["etk-greenfield"]
    if network.get("external") or network.get("name") != f"{project}_etk-greenfield" or network.get("internal") is True:
        raise GreenfieldHarnessError("resolved Compose network identity is unsafe")
    if (network.get("labels") or {}).get("com.etk.environment") != "greenfield":
        raise GreenfieldHarnessError("resolved Compose network lacks the Greenfield ownership label")
    return {
        "project": project,
        "services": sorted(services),
        "bind_mounts": bind_mounts,
        "volume_mounts": volume_mounts,
        "ports": {name: {"published": values[0], "target": values[1]} for name, values in expected_ports.items()},
        "volume_names": sorted(volume_names),
        "network_name": network["name"],
        "images": {name: service.get("image") for name, service in sorted(services.items())},
        "environments": {
            name: _redacted_environment(service.get("environment") or {}) for name, service in sorted(services.items())
        },
    }


def _resolved_compose(config: HarnessConfig, output: Path, *, extra_file: Path | None = None) -> dict[str, Any]:
    completed = _run(
        _compose_args(config, "config", "--format", "json", extra_file=extra_file),
        cwd=config.source_root,
        environment=_compose_environment(),
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise GreenfieldHarnessError("docker compose config returned invalid JSON") from exc
    summary = _validate_resolved_compose(
        payload,
        allow_root=config.allow_root,
        fake_docker_socket=config.fake_docker_socket,
        project=config.project,
        image_ref=config.image_id,
        api_port=config.api_port,
        pg_port=config.pg_port,
    )
    _write_json(output, payload)
    _write_json(output.with_name(output.stem + "-summary.json"), summary)
    return payload


def _listed_project_resources(project: str, *, cwd: Path) -> dict[str, list[str]]:
    container_rows = _run(["docker", "ps", "-a", "--format", "{{.ID}}\t{{.Names}}"], cwd=cwd).stdout.splitlines()
    volume_names = _run(["docker", "volume", "ls", "--format", "{{.Name}}"], cwd=cwd).stdout.splitlines()
    network_names = _run(["docker", "network", "ls", "--format", "{{.Name}}"], cwd=cwd).stdout.splitlines()
    labeled_containers = _run(
        [
            "docker",
            "ps",
            "-a",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.ID}}",
        ],
        cwd=cwd,
    ).stdout.splitlines()
    labeled_volumes = _run(
        [
            "docker",
            "volume",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Name}}",
        ],
        cwd=cwd,
    ).stdout.splitlines()
    labeled_networks = _run(
        [
            "docker",
            "network",
            "ls",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--format",
            "{{.Name}}",
        ],
        cwd=cwd,
    ).stdout.splitlines()
    container_prefixes = (f"{project}-", f"{project}_")
    return {
        "containers": sorted(
            {
                *labeled_containers,
                *(
                    row.split("\t", 1)[0]
                    for row in container_rows
                    if row.split("\t", 1)[-1].startswith(container_prefixes)
                ),
            }
        ),
        "volumes": sorted({*labeled_volumes, *(name for name in volume_names if name.startswith(f"{project}_"))}),
        "networks": sorted({*labeled_networks, *(name for name in network_names if name.startswith(f"{project}_"))}),
    }


def _assert_no_project_resources(config: HarnessConfig) -> dict[str, list[str]]:
    resources = _listed_project_resources(config.project, cwd=config.source_root)
    if any(resources.values()):
        raise GreenfieldHarnessError(f"Compose project {config.project} already owns Docker resources")
    return resources


def _runtime_bind_source_matches(source: Path, expected: Path) -> bool:
    normalized_source = source.resolve(strict=False)
    normalized_expected = expected.resolve(strict=False)
    docker_desktop_expected = Path("/host_mnt") / normalized_expected.relative_to("/")
    return normalized_source in {normalized_expected, docker_desktop_expected}


def _runtime_ownership(config: HarnessConfig, *, require_complete: bool) -> dict[str, Any]:
    resources = _listed_project_resources(config.project, cwd=config.source_root)
    container_inspect = (
        _docker_json(["docker", "inspect", *resources["containers"]], cwd=config.source_root)
        if resources["containers"]
        else []
    )
    volume_inspect = (
        _docker_json(["docker", "volume", "inspect", *resources["volumes"]], cwd=config.source_root)
        if resources["volumes"]
        else []
    )
    network_inspect = (
        _docker_json(["docker", "network", "inspect", *resources["networks"]], cwd=config.source_root)
        if resources["networks"]
        else []
    )
    services: set[str] = set()
    container_summary = []
    for item in container_inspect:
        labels = item.get("Config", {}).get("Labels") or {}
        service = str(labels.get("com.docker.compose.service") or "")
        if (
            labels.get("com.docker.compose.project") != config.project
            or labels.get("com.etk.environment") != "greenfield"
        ):
            raise GreenfieldHarnessError("container ownership labels do not match the disposable project")
        if service not in EXPECTED_SERVICES:
            raise GreenfieldHarnessError(f"unexpected Compose service container: {service}")
        services.add(service)
        mounts = []
        observed_volume_mounts: set[tuple[str, str, bool]] = set()
        observed_bind_mounts: list[tuple[str, str, bool]] = []
        for mount in item.get("Mounts") or []:
            mount_type = str(mount.get("Type") or "")
            source = str(mount.get("Source") or "")
            destination = str(mount.get("Destination") or "")
            read_only = not bool(mount.get("RW", False))
            if mount_type == "bind":
                if not _runtime_bind_source_matches(Path(source), config.fake_docker_socket):
                    raise GreenfieldHarnessError(f"runtime bind mount does not match the disabled socket: {source}")
                observed_bind_mounts.append(
                    (str(config.fake_docker_socket.resolve(strict=False)), destination, read_only)
                )
                mount_identity = source
            elif mount_type == "volume":
                volume_name = str(mount.get("Name") or "")
                if volume_name not in {f"{config.project}_{key}" for key in EXPECTED_VOLUME_KEYS}:
                    raise GreenfieldHarnessError(
                        f"runtime volume is not owned by the disposable project: {volume_name}"
                    )
                volume_key = volume_name.removeprefix(f"{config.project}_")
                observed_volume_mounts.add((volume_key, destination, read_only))
                mount_identity = volume_name
            else:
                raise GreenfieldHarnessError(
                    f"runtime service {service} contains unsupported mount type {mount_type!r}"
                )
            mounts.append(
                {
                    "type": mount_type,
                    "source": mount_identity,
                    "destination": destination,
                    "read_only": read_only,
                }
            )
        if observed_volume_mounts != EXPECTED_SERVICE_VOLUME_MOUNTS[service]:
            raise GreenfieldHarnessError(f"runtime service {service} volume mounts are unsafe")
        expected_bind_mounts: list[tuple[str, str, bool]] = []
        if service == "control-plane":
            expected_bind_mounts = [
                (
                    str(config.fake_docker_socket.resolve(strict=False)),
                    "/var/run/docker.sock",
                    True,
                )
            ]
        if observed_bind_mounts != expected_bind_mounts:
            raise GreenfieldHarnessError(f"runtime service {service} bind mounts are unsafe")
        ports = item.get("HostConfig", {}).get("PortBindings") or {}
        published_ports: list[tuple[str, str, int]] = []
        for bindings in ports.values():
            for binding in bindings or []:
                if binding.get("HostIp") != "127.0.0.1":
                    raise GreenfieldHarnessError("runtime port is not bound to loopback")
        for container_port, bindings in ports.items():
            for binding in bindings or []:
                published_ports.append(
                    (container_port, str(binding.get("HostIp") or ""), int(binding.get("HostPort") or 0))
                )
        expected_published_ports: list[tuple[str, str, int]] = []
        if service == "api":
            expected_published_ports = [("5257/tcp", "127.0.0.1", config.api_port)]
        elif service == "db":
            expected_published_ports = [("5432/tcp", "127.0.0.1", config.pg_port)]
        if sorted(published_ports) != expected_published_ports:
            raise GreenfieldHarnessError(f"runtime service {service} published ports are unsafe")
        runtime_networks = set((item.get("NetworkSettings", {}).get("Networks") or {}).keys())
        if runtime_networks != {f"{config.project}_etk-greenfield"}:
            raise GreenfieldHarnessError(f"runtime service {service} networks are unsafe")
        container_summary.append(
            {
                "id": item.get("Id"),
                "name": str(item.get("Name") or "").lstrip("/"),
                "service": service,
                "image": item.get("Image"),
                "state": item.get("State"),
                "mounts": mounts,
                "ports": ports,
                "labels": {
                    "com.docker.compose.project": labels.get("com.docker.compose.project"),
                    "com.docker.compose.service": service,
                    "com.etk.environment": labels.get("com.etk.environment"),
                },
            }
        )
        if service in APP_SERVICES and item.get("Image") != config.image_id:
            raise GreenfieldHarnessError(f"service {service} is not running the pinned image ID")
    volume_summary = []
    for item in volume_inspect:
        labels = item.get("Labels") or {}
        name = str(item.get("Name") or "")
        if (
            labels.get("com.docker.compose.project") != config.project
            or labels.get("com.etk.environment") != "greenfield"
            or name not in {f"{config.project}_{key}" for key in EXPECTED_VOLUME_KEYS}
        ):
            raise GreenfieldHarnessError(f"volume ownership is unsafe: {name}")
        volume_summary.append({"name": name, "created_at": item.get("CreatedAt"), "labels": labels})
    network_summary = []
    for item in network_inspect:
        labels = item.get("Labels") or {}
        name = str(item.get("Name") or "")
        if (
            labels.get("com.docker.compose.project") != config.project
            or labels.get("com.etk.environment") != "greenfield"
            or name != f"{config.project}_etk-greenfield"
        ):
            raise GreenfieldHarnessError(f"network ownership is unsafe: {name}")
        network_summary.append(
            {
                "name": name,
                "id": item.get("Id"),
                "internal": item.get("Internal"),
                "labels": labels,
            }
        )
    if require_complete:
        if services != EXPECTED_SERVICES:
            raise GreenfieldHarnessError("not all expected service containers exist")
        if set(resources["volumes"]) != {f"{config.project}_{key}" for key in EXPECTED_VOLUME_KEYS}:
            raise GreenfieldHarnessError("not all expected disposable volumes exist")
        if set(resources["networks"]) != {f"{config.project}_etk-greenfield"}:
            raise GreenfieldHarnessError("the disposable network identity is incomplete")
    return {
        "project": config.project,
        "containers": sorted(container_summary, key=lambda item: item["service"]),
        "volumes": sorted(volume_summary, key=lambda item: item["name"]),
        "networks": network_summary,
    }


def _compose_logs(config: HarnessConfig, output: Path, *, extra_file: Path | None = None) -> None:
    _run(
        _compose_args(config, "logs", "--no-color", "--timestamps", extra_file=extra_file),
        cwd=config.source_root,
        environment=_compose_environment(),
        output=output,
        check=False,
    )


def _destroy(config: HarnessConfig, evidence: Path, *, extra_file: Path | None = None) -> None:
    ownership = _runtime_ownership(config, require_complete=False)
    if not ownership["containers"] and not ownership["volumes"] and not ownership["networks"]:
        _write_json(evidence / "after-destroy.json", _assert_no_project_resources(config))
        return
    _write_json(evidence / "ownership-before-destroy.json", ownership)
    _compose_logs(config, evidence / "compose.log", extra_file=extra_file)
    _run(
        _compose_args(
            config,
            "down",
            "--volumes",
            "--remove-orphans",
            "--timeout",
            "30",
            extra_file=extra_file,
        ),
        cwd=config.source_root,
        environment=_compose_environment(),
        output=evidence / "destroy.log",
        timeout=300,
    )
    _write_json(evidence / "after-destroy.json", _assert_no_project_resources(config))


def _database_snapshot(config: HarnessConfig, *, require_empty: bool) -> dict[str, Any]:
    with psycopg.connect(config.database_url, connect_timeout=10) as connection:

        def scalar(query: LiteralString) -> Any:
            row = connection.execute(sql.SQL(query)).fetchone()
            if row is None:
                raise GreenfieldHarnessError("database scalar query returned no row")
            return row[0]

        tables = [
            str(row[0])
            for row in connection.execute(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' ORDER BY tablename"
            ).fetchall()
        ]
        job_statuses = {
            str(status): int(count)
            for status, count in connection.execute("SELECT status, COUNT(*) FROM job_runs GROUP BY status").fetchall()
        }
        outbox_statuses = {
            str(status): int(count)
            for status, count in connection.execute(
                "SELECT status, COUNT(*) FROM outbox_events GROUP BY status"
            ).fetchall()
        }
        snapshot = {
            "server_version": str(scalar("SHOW server_version")),
            "server_version_num": str(scalar("SHOW server_version_num")),
            "alembic_revision": str(scalar("SELECT version_num FROM etk_migrations.alembic_version")),
            "public_table_count": len(tables),
            "public_tables": tables,
            "job_count": int(scalar("SELECT COUNT(*) FROM job_runs")),
            "job_statuses": job_statuses,
            "active_job_count": int(
                scalar(
                    "SELECT COUNT(*) FROM job_runs WHERE status IN ('queued','running','retry_wait','cancel_requested')"
                )
            ),
            "idempotency_count": int(scalar("SELECT COUNT(*) FROM idempotency_records")),
            "outbox_count": int(scalar("SELECT COUNT(*) FROM outbox_events")),
            "outbox_statuses": outbox_statuses,
            "duplicate_outbox_event_keys": int(
                scalar(
                    "SELECT COUNT(*) FROM ("
                    "SELECT event_key FROM outbox_events GROUP BY event_key HAVING COUNT(*) > 1"
                    ") AS duplicates"
                )
            ),
            "duplicate_media_identities": int(
                scalar(
                    "SELECT COUNT(*) FROM ("
                    "SELECT tmdb_id, item_type FROM media_metadata "
                    "GROUP BY tmdb_id, item_type HAVING COUNT(*) > 1"
                    ") AS duplicates"
                )
            ),
            "invalid_quota_values": int(
                scalar("SELECT COUNT(*) FROM subscription_quota_reservations WHERE units <= 0 OR cap_snapshot < 0")
            ),
            "negative_credit_accounts": int(scalar("SELECT COUNT(*) FROM sharing_credit_accounts WHERE balance < 0")),
            "negative_credit_entries": int(
                scalar("SELECT COUNT(*) FROM sharing_credit_ledger WHERE balance_after < 0")
            ),
            "fixture_admin_count": int(scalar("SELECT COUNT(*) FROM emby_users WHERE id = 'p12-fixture-admin'")),
        }
    if not snapshot["server_version_num"].startswith("18"):
        raise GreenfieldHarnessError("Greenfield PostgreSQL is not version 18")
    if snapshot["alembic_revision"] != EXPECTED_ALEMBIC_HEAD:
        raise GreenfieldHarnessError("Greenfield Alembic revision does not match head")
    if snapshot["public_table_count"] != EXPECTED_PUBLIC_TABLE_COUNT:
        raise GreenfieldHarnessError("Greenfield table count does not match the complete migration-owned catalog")
    for key in (
        "active_job_count",
        "duplicate_outbox_event_keys",
        "duplicate_media_identities",
        "invalid_quota_values",
        "negative_credit_accounts",
        "negative_credit_entries",
        "fixture_admin_count",
    ):
        if snapshot[key] != 0:
            raise GreenfieldHarnessError(f"database invariant failed: {key}")
    if require_empty and any(snapshot[key] != 0 for key in ("job_count", "idempotency_count", "outbox_count")):
        raise GreenfieldHarnessError("new Greenfield state is not empty before the simulated canary")
    return snapshot


def _schema_tests(config: HarnessConfig, output: Path) -> None:
    environment = dict(os.environ)
    environment.update(
        {
            "ETK_RUN_POSTGRES_INTEGRATION": "1",
            "ETK_TEST_DATABASE_URL": config.database_url,
            "ETK_DATABASE_URL": config.database_url,
        }
    )
    _run(
        [
            "uv",
            "run",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "verification/integration/platform/test_postgres18_platform.py::test_pg18_alembic_schema_matches_legacy_catalog",
            "verification/integration/p3/test_database_read_postgres18.py::test_pg18_empty_migrated_schema_executes_all_database_read_queries",
        ],
        cwd=config.repo,
        environment=environment,
        output=output,
        timeout=300,
    )


def _install_admin_fixture(config: HarnessConfig) -> None:
    with psycopg.connect(config.database_url, connect_timeout=10) as connection:
        connection.execute(
            sql.SQL(
                "INSERT INTO emby_users (id, name, is_administrator, policy_json) "
                "VALUES (%s, 'P12 Fixture Admin', TRUE, '{}'::jsonb)"
            ),
            (GREENFIELD_ADMIN_ID,),
        )
        connection.commit()


def _remove_admin_fixture(config: HarnessConfig) -> None:
    with psycopg.connect(config.database_url, connect_timeout=10) as connection:
        connection.execute(sql.SQL("DELETE FROM emby_users WHERE id = %s"), (GREENFIELD_ADMIN_ID,))
        connection.commit()


def _admin_session_cookie(config: HarnessConfig) -> str:
    code = (
        "from pathlib import Path; "
        "from etk.platform.web.session_codec import NativeSessionCodec, load_or_create_secret_key; "
        "print(NativeSessionCodec(load_or_create_secret_key(Path('/config'))).dumps("
        f"{{'emby_user_id': {GREENFIELD_ADMIN_ID!r}}}))"
    )
    completed = _run(
        _compose_args(config, "exec", "--no-TTY", "api", "python", "-c", code),
        cwd=config.source_root,
        environment=_compose_environment(),
    )
    cookie = completed.stdout.strip()
    if not cookie or any(character.isspace() for character in cookie):
        raise GreenfieldHarnessError("failed to create the disposable administrator session")
    return cookie


def _api_smoke(config: HarnessConfig, *, session_cookie: str) -> dict[str, Any]:
    base_url = f"http://127.0.0.1:{config.api_port}"
    expected_openapi = json.loads(
        (config.source_root / "verification/contracts/http/p4-openapi.json").read_text(encoding="utf-8")
    )
    with httpx.Client(
        base_url=base_url,
        timeout=15,
        trust_env=False,
        cookies={"session": session_cookie},
    ) as client:
        health = client.get("/api/health")
        auth = client.get("/api/auth/check_status")
        tasks = client.get("/api/tasks/available", params={"context": "all"})
        database_tables = client.get("/api/database/tables")
        core_stats = client.get("/api/database/stats/core")
        configuration = client.get("/api/config")
        review_items = client.get("/api/review_items")
        subscriptions = client.get("/api/subscriptions/all")
        actor_subscriptions = client.get("/api/actor-subscriptions")
        sharing_summary = client.get("/api/shared/resources/summary")
        organize_records = client.get("/api/p115/records")
        openapi = client.get("/openapi.json")
    for name, response in {
        "health": health,
        "auth": auth,
        "tasks": tasks,
        "database_tables": database_tables,
        "core_stats": core_stats,
        "configuration": configuration,
        "review_items": review_items,
        "subscriptions": subscriptions,
        "actor_subscriptions": actor_subscriptions,
        "sharing_summary": sharing_summary,
        "organize_records": organize_records,
        "openapi": openapi,
    }.items():
        if response.status_code != 200:
            raise GreenfieldHarnessError(f"API smoke {name} returned HTTP {response.status_code}")
    health_payload = health.json()
    auth_payload = auth.json()
    task_payload = tasks.json()
    table_payload = database_tables.json()
    configuration_payload = configuration.json()
    review_payload = review_items.json()
    subscription_payload = subscriptions.json()
    actor_subscription_payload = actor_subscriptions.json()
    sharing_payload = sharing_summary.json()
    organize_payload = organize_records.json()
    openapi_payload = openapi.json()
    if health_payload != {"status": "ok"}:
        raise GreenfieldHarnessError("API health payload is invalid")
    if auth_payload.get("status") != "setup_required":
        raise GreenfieldHarnessError("fresh config does not report setup_required")
    if not isinstance(task_payload, list) or len(task_payload) != 50:
        raise GreenfieldHarnessError("task catalog does not contain the frozen 50 keys")
    required_keys = {
        "subscribe-assistant-maintenance",
        "add-all-series-to-watchlist",
        "process-watchlist",
        "refresh_completed_series",
        "resubscribe-library",
        "generate-all-covers",
        "generate-custom-collection-covers",
    }
    if not required_keys.issubset({str(item.get("key") or "") for item in task_payload if isinstance(item, dict)}):
        raise GreenfieldHarnessError("final P11 task owners are missing from the live catalog")
    if not isinstance(table_payload, list) or len(table_payload) != EXPECTED_PUBLIC_TABLE_COUNT:
        raise GreenfieldHarnessError("database table API does not expose the complete Greenfield catalog")
    if not isinstance(configuration_payload, dict):
        raise GreenfieldHarnessError("configuration smoke did not return an object")
    if review_payload.get("items") != [] or review_payload.get("total_items") != 0:
        raise GreenfieldHarnessError("review smoke did not return an authoritative empty result")
    if subscription_payload != [] or actor_subscription_payload != []:
        raise GreenfieldHarnessError("subscription smoke did not return authoritative empty lists")
    if (
        sharing_payload.get("success") is not True
        or sharing_payload.get("data", {}).get("shares", {}).get("total") != 0
        or sharing_payload.get("data", {}).get("credit", {}).get("credit") != 0
    ):
        raise GreenfieldHarnessError("sharing smoke did not return an authoritative empty projection")
    if (
        organize_payload.get("success") is not True
        or organize_payload.get("items") != []
        or organize_payload.get("total") != 0
    ):
        raise GreenfieldHarnessError("115 record smoke did not return an authoritative empty projection")
    if openapi_payload != expected_openapi:
        raise GreenfieldHarnessError("running API OpenAPI differs from the frozen contract")
    openapi_sha256 = hashlib.sha256(
        json.dumps(openapi_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "health": health_payload,
        "auth": auth_payload,
        "task_count": len(task_payload),
        "required_task_keys": sorted(required_keys),
        "database_table_count": len(table_payload),
        "core_stats": core_stats.json(),
        "read_only_smoke": {
            "configuration_key_count": len(configuration_payload),
            "review_item_count": len(review_payload["items"]),
            "subscription_count": len(subscription_payload),
            "actor_subscription_count": len(actor_subscription_payload),
            "sharing_count": sharing_payload["data"]["shares"]["total"],
            "organize_record_count": len(organize_payload["items"]),
        },
        "openapi_sha256": openapi_sha256,
    }


def _job_projection(client: httpx.Client, job_id: str) -> dict[str, Any]:
    response = client.get(f"/api/jobs/{job_id}")
    if response.status_code != 200 or not isinstance(response.json(), dict):
        raise GreenfieldHarnessError(f"Job {job_id} could not be read back")
    return dict(response.json())


def _simulated_canary(
    config: HarnessConfig,
    name: str,
    evidence: Path,
    *,
    session_cookie: str,
) -> dict[str, Any]:
    _runtime_ownership(config, require_complete=True)
    _run(
        _compose_args(config, "pause", "worker"),
        cwd=config.source_root,
        environment=_compose_environment(),
        output=evidence / "worker-pause.log",
    )
    base_url = f"http://127.0.0.1:{config.api_port}"
    idempotency_key = f"{config.project}-{name}-directory-sync"
    initial_projection: dict[str, Any] | None = None
    final_projection: dict[str, Any] | None = None
    try:
        with httpx.Client(
            base_url=base_url,
            timeout=15,
            trust_env=False,
            cookies={"session": session_cookie},
        ) as client:
            submitted = client.post(
                "/api/tasks/run",
                json={"task_name": "sync-115-directory-tree"},
                headers={"Idempotency-Key": idempotency_key},
            )
            if submitted.status_code != 202:
                raise GreenfieldHarnessError(f"simulated canary submission returned HTTP {submitted.status_code}")
            job_id = str(submitted.headers.get("x-etk-job-id") or "")
            if not job_id:
                raise GreenfieldHarnessError("simulated canary response has no durable Job ID")
            initial_projection = _job_projection(client, job_id)
    finally:
        _run(
            _compose_args(config, "unpause", "worker"),
            cwd=config.source_root,
            environment=_compose_environment(),
            output=evidence / "worker-unpause.log",
        )
    deadline = time.monotonic() + 120
    with httpx.Client(
        base_url=base_url,
        timeout=15,
        trust_env=False,
        cookies={"session": session_cookie},
    ) as client:
        while time.monotonic() < deadline:
            final_projection = _job_projection(client, job_id)
            if str(final_projection.get("status") or "") in TERMINAL_JOB_STATUSES:
                break
            time.sleep(0.5)
        if final_projection is None or final_projection.get("status") != "succeeded":
            raise GreenfieldHarnessError("simulated canary did not reach a successful terminal state")
        replay = client.post(
            "/api/tasks/run",
            json={"task_name": "sync-115-directory-tree"},
            headers={"Idempotency-Key": idempotency_key},
        )
        if replay.status_code != 202 or replay.headers.get("x-etk-job-id") != job_id:
            raise GreenfieldHarnessError("simulated canary idempotent replay created a different Job")
    result = {
        "task_key": "sync-115-directory-tree",
        "idempotency_key": idempotency_key,
        "job_id": job_id,
        "worker_paused_projection": initial_projection,
        "terminal_projection": final_projection,
        "replay_job_id": replay.headers.get("x-etk-job-id"),
    }
    _write_json(evidence / "simulated-canary.json", result)
    return result


def _config_file_inventory(config: HarnessConfig, output: Path) -> None:
    _run(
        _compose_args(
            config,
            "exec",
            "--no-TTY",
            "api",
            "sh",
            "-c",
            "find /config -mindepth 1 -maxdepth 3 -printf '%y %p\\n' | sort",
        ),
        cwd=config.source_root,
        environment=_compose_environment(),
        output=output,
    )


def _successful_round(config: HarnessConfig, name: str, evidence: Path) -> dict[str, Any]:
    evidence.mkdir(parents=True)
    _write_json(evidence / "before-create.json", _assert_no_project_resources(config))
    started = False
    try:
        _assert_port_available(config.api_port)
        _assert_port_available(config.pg_port)
        _run(
            _compose_args(
                config,
                "up",
                "--detach",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "300",
            ),
            cwd=config.source_root,
            environment=_compose_environment(),
            output=evidence / "create.log",
            timeout=600,
        )
        started = True
        ownership = _runtime_ownership(config, require_complete=True)
        _write_json(evidence / "ownership-running.json", ownership)
        before = _database_snapshot(config, require_empty=True)
        _write_json(evidence / "database-before-canary.json", before)
        _schema_tests(config, evidence / "schema-and-query-tests.log")
        _install_admin_fixture(config)
        try:
            session_cookie = _admin_session_cookie(config)
            _write_json(
                evidence / "admin-fixture.json",
                {"user_id": GREENFIELD_ADMIN_ID, "session_cookie_persisted": False},
            )
            api = _api_smoke(config, session_cookie=session_cookie)
            _write_json(evidence / "api-smoke.json", api)
            canary = _simulated_canary(
                config,
                name,
                evidence,
                session_cookie=session_cookie,
            )
        finally:
            _remove_admin_fixture(config)
        after = _database_snapshot(config, require_empty=False)
        if after["job_count"] != 1 or after["idempotency_count"] != 1:
            raise GreenfieldHarnessError("simulated canary did not preserve one idempotent durable Job")
        _write_json(evidence / "database-after-canary.json", after)
        _config_file_inventory(config, evidence / "config-file-inventory.txt")
        result = {
            "name": name,
            "status": "passed",
            "image_id": config.image_id,
            "postgresql": before["server_version"],
            "server_version_num": before["server_version_num"],
            "alembic_revision": before["alembic_revision"],
            "public_table_count": before["public_table_count"],
            "task_count": api["task_count"],
            "canary_job_id": canary["job_id"],
        }
        _write_json(evidence / "result.json", result)
        return result
    finally:
        resources = _listed_project_resources(config.project, cwd=config.source_root)
        if started or any(resources.values()):
            _destroy(config, evidence)


def _wait_for_migrate_exit(config: HarnessConfig, failure_file: Path) -> dict[str, Any]:
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        completed = _run(
            _compose_args(config, "ps", "--all", "--quiet", "migrate", extra_file=failure_file),
            cwd=config.source_root,
            environment=_compose_environment(),
            check=False,
        )
        container_id = completed.stdout.strip()
        if container_id:
            item = _docker_json(["docker", "inspect", container_id], cwd=config.source_root)[0]
            state = item.get("State") or {}
            if not state.get("Running"):
                return {"container_id": container_id, "state": state}
        time.sleep(0.5)
    raise GreenfieldHarnessError("injected migration container did not reach an exited state")


def _failure_round(config: HarnessConfig, evidence: Path) -> dict[str, Any]:
    evidence.mkdir(parents=True)
    failure_file = config.allow_root / "failure.override.yml"
    failure_file.write_text(_failure_override(), encoding="utf-8")
    _resolved_compose(config, evidence / "resolved-compose-failure.json", extra_file=failure_file)
    _write_json(evidence / "before-create.json", _assert_no_project_resources(config))
    try:
        injected = _run(
            _compose_args(
                config,
                "up",
                "--detach",
                "--no-build",
                "--pull",
                "never",
                "--wait",
                "--wait-timeout",
                "180",
                extra_file=failure_file,
            ),
            cwd=config.source_root,
            environment=_compose_environment(),
            output=evidence / "injected-create.log",
            timeout=300,
            check=False,
        )
        if injected.returncode == 0:
            raise GreenfieldHarnessError("failure injection unexpectedly completed Compose startup")
        migration = _wait_for_migrate_exit(config, failure_file)
        if int(migration["state"].get("ExitCode") or 0) != 42:
            raise GreenfieldHarnessError("failure injection did not produce the expected exit code 42")
        ownership = _runtime_ownership(config, require_complete=False)
        _write_json(evidence / "ownership-after-injected-failure.json", ownership)
        result = {
            "status": "expected-failure-observed",
            "failure": "migrate command exited 42 before API/Worker activation",
            "migrate": migration,
        }
        _write_json(evidence / "result.json", result)
        return result
    finally:
        resources = _listed_project_resources(config.project, cwd=config.source_root)
        if any(resources.values()):
            _destroy(config, evidence, extra_file=failure_file)


def _validated_image_ref(value: str) -> str:
    reference = str(value or "").strip().lower()
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", reference):
        raise GreenfieldHarnessError("application image must be an explicit immutable sha256 ID")
    return reference


def _inspect_local_image(reference: str, *, cwd: Path) -> dict[str, Any]:
    payload = _docker_json(["docker", "image", "inspect", reference], cwd=cwd)
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        raise GreenfieldHarnessError("application image inspect result is invalid")
    inspect = dict(payload[0])
    if inspect.get("Id") != reference:
        raise GreenfieldHarnessError("local application image ID does not match the requested digest")
    return inspect


def _summary(config: HarnessConfig, rounds: list[dict[str, Any]], failure: dict[str, Any]) -> str:
    lines = [
        "# P12 disposable Greenfield evidence",
        "",
        f"- Source revision: `{config.revision}`",
        f"- Compose project: `{config.project}`",
        f"- Allow-root: `{config.allow_root}`",
        f"- Immutable application image: `{config.image_id}`",
        "- Published ports: loopback only; the Compose network is project-isolated, Coordinator schedules are "
        "disabled, and no external credentials are present.",
        "- Destructive scope: only resources whose resolved names and runtime labels match this Compose project.",
        "",
        "| Evidence group | PostgreSQL | Alembic | Tables | Task catalog | Simulated canary |",
        "|---|---|---|---:|---:|---|",
    ]
    for item in rounds:
        lines.append(
            f"| {item['name']} | {item['server_version_num']} | {item['alembic_revision']} | "
            f"{item['public_table_count']} | {item['task_count']} | `{item['canary_job_id']}` succeeded and replayed |"
        )
    lines.extend(
        [
            "",
            f"Failure drill: {failure['failure']}; ownership was revalidated, all project containers/network/volumes "
            "were removed, and the final recovery row above rebuilt from empty state.",
            "",
            "Each successful round includes the resolved Compose, runtime ownership, PG18/schema query tests, "
            "OpenAPI equality, empty-state API checks, paused-Worker recovery, idempotent Job replay, database "
            "invariants, config filename inventory, logs, and post-destroy zero-resource evidence.",
            "",
            "Live external-system acceptance was not run and remains `HUMAN_PENDING`.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> Path:
    repo = Path(args.repo).resolve(strict=True)
    allow_root = _validated_new_root(args.allow_root, label="allow-root", required_prefix="etk-p12-greenfield-")
    evidence_root = _validated_new_root(
        args.evidence_root,
        label="evidence-root",
        required_prefix="p12-greenfield-",
    )
    project = _validated_project(args.project)
    image_id = _validated_image_ref(args.image_ref)
    image_inspect = _inspect_local_image(image_id, cwd=repo)
    api_port = _validated_port(args.api_port, label="API port")
    pg_port = _validated_port(args.pg_port, label="PostgreSQL port")
    if api_port == pg_port:
        raise GreenfieldHarnessError("API and PostgreSQL ports must be distinct")
    _assert_port_available(api_port)
    _assert_port_available(pg_port)
    existing_resources = _listed_project_resources(project, cwd=repo)
    if any(existing_resources.values()):
        raise GreenfieldHarnessError(f"Compose project {project} already owns Docker resources")
    allow_root.mkdir()
    evidence_root.mkdir()
    source_root = allow_root / "source"
    revision = _snapshot_repository(repo, args.revision, source_root)
    fake_socket = allow_root / "disabled-docker.sock"
    fake_socket.write_bytes(b"")
    override_file = allow_root / "runtime.override.yml"
    override_file.write_text(
        _runtime_override(
            image_ref=image_id,
            api_port=api_port,
            pg_port=pg_port,
            fake_docker_socket=fake_socket,
        ),
        encoding="utf-8",
    )
    config = HarnessConfig(
        repo=repo,
        revision=revision,
        allow_root=allow_root,
        evidence_root=evidence_root,
        source_root=source_root,
        project=project,
        api_port=api_port,
        pg_port=pg_port,
        fake_docker_socket=fake_socket,
        compose_file=source_root / "docker/compose.refactor.yml",
        override_file=override_file,
        image_id=image_id,
    )
    _write_json(evidence_root / "image-inspect.json", image_inspect)
    _resolved_compose(config, evidence_root / "resolved-compose.json")
    _write_json(
        evidence_root / "configuration-summary.json",
        {
            "source_revision": revision,
            "project": project,
            "allow_root": str(allow_root),
            "image_id": image_id,
            "api_endpoint": f"http://127.0.0.1:{api_port}",
            "postgres_endpoint": f"127.0.0.1:{pg_port}",
            "scheduler_enabled": False,
            "runtime_network_internal": False,
            "external_credentials_present": False,
            "external_writes_enabled": False,
            "docker_socket_source": str(fake_socket),
        },
    )
    _assert_no_project_resources(config)
    round_one = _successful_round(config, "round-1-empty-create", evidence_root / "round-1-empty-create")
    round_two = _successful_round(config, "round-2-empty-rebuild", evidence_root / "round-2-empty-rebuild")
    failure = _failure_round(config, evidence_root / "failure-rebuild" / "injected-failure")
    recovery = _successful_round(
        config,
        "round-3-failure-recovery",
        evidence_root / "failure-rebuild" / "clean-recovery",
    )
    final_resources = _assert_no_project_resources(config)
    manifest = {
        "status": "passed",
        "source_revision": revision,
        "project": project,
        "image_id": image_id,
        "rounds": [round_one, round_two, recovery],
        "failure_drill": failure,
        "final_resources": final_resources,
        "live_acceptance": "HUMAN_PENDING",
    }
    _write_json(evidence_root / "manifest.json", manifest)
    (evidence_root / "summary.md").write_text(
        _summary(config, [round_one, round_two, recovery], failure),
        encoding="utf-8",
    )
    return evidence_root


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the guarded P12 disposable Greenfield rebuild drill")
    parser.add_argument("--repo", default=str(Path.cwd()), help="absolute repository root")
    parser.add_argument("--revision", default="HEAD", help="committed source revision to archive")
    parser.add_argument("--image-ref", required=True, help="existing immutable sha256 application image ID")
    parser.add_argument("--allow-root", required=True, help="new absolute etk-p12-greenfield-* directory")
    parser.add_argument("--evidence-root", required=True, help="new absolute p12-greenfield-* evidence directory")
    parser.add_argument("--project", required=True, help="isolated etk-p12-* Compose project")
    parser.add_argument("--api-port", required=True, type=int, help="unused loopback API port")
    parser.add_argument("--pg-port", required=True, type=int, help="unused loopback PostgreSQL port")
    return parser


def main() -> int:
    args = _parser().parse_args()
    evidence_root: Path | None = None
    try:
        evidence_root = run(args)
    except (GreenfieldHarnessError, OSError, subprocess.SubprocessError, psycopg.Error, httpx.HTTPError) as exc:
        raw_evidence = Path(args.evidence_root)
        if raw_evidence.is_absolute() and raw_evidence.is_dir():
            _write_json(raw_evidence / "failure.json", {"status": "failed", "error": str(exc)})
        print(f"p12_greenfield=failed error={exc}", file=sys.stderr)
        return 1
    print(f"p12_greenfield=passed evidence={evidence_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
