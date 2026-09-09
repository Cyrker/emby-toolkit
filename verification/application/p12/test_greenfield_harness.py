from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts.refactor.run_p12_greenfield import (
    APP_SERVICES,
    EXPECTED_NETWORK_KEYS,
    EXPECTED_SERVICE_VOLUME_MOUNTS,
    EXPECTED_SERVICES,
    EXPECTED_VOLUME_KEYS,
    GreenfieldHarnessError,
    _runtime_bind_source_matches,
    _validate_resolved_compose,
    _validated_image_ref,
    _validated_new_root,
    _validated_project,
)

PROJECT = "etk-p12-guard-test"
IMAGE_ID = f"sha256:{'1' * 64}"
INFRA_IMAGE = f"fixture@sha256:{'2' * 64}"
API_PORT = 25257
PG_PORT = 25432


def _resolved_payload(tmp_path: Path) -> tuple[dict[str, Any], Path, Path]:
    allow_root = tmp_path / "etk-p12-greenfield-guard-test"
    fake_socket = allow_root / "disabled-docker.sock"
    services: dict[str, dict[str, Any]] = {}
    for service_name in sorted(EXPECTED_SERVICES):
        service: dict[str, Any] = {
            "environment": {},
            "labels": {"com.etk.environment": "greenfield"},
            "networks": {"etk-greenfield": None},
            "volumes": [
                {
                    "type": "volume",
                    "source": source,
                    "target": target,
                    "read_only": read_only,
                }
                for source, target, read_only in sorted(EXPECTED_SERVICE_VOLUME_MOUNTS[service_name])
            ],
        }
        if service_name in APP_SERVICES:
            service["image"] = IMAGE_ID
        elif service_name in {"db", "redis"}:
            service["image"] = INFRA_IMAGE
        if service_name == "api":
            service["ports"] = [{"host_ip": "127.0.0.1", "published": API_PORT, "target": 5257}]
        elif service_name == "db":
            service["ports"] = [{"host_ip": "127.0.0.1", "published": PG_PORT, "target": 5432}]
        if service_name == "control-plane":
            service["volumes"].append(
                {
                    "type": "bind",
                    "source": str(fake_socket),
                    "target": "/var/run/docker.sock",
                    "read_only": True,
                }
            )
        if service_name == "coordinator":
            service["environment"]["ETK_COORDINATOR_SCHEDULES_ENABLED"] = "false"
        services[service_name] = service

    payload = {
        "name": PROJECT,
        "services": services,
        "volumes": {
            key: {
                "external": False,
                "labels": {"com.etk.environment": "greenfield"},
                "name": f"{PROJECT}_{key}",
            }
            for key in EXPECTED_VOLUME_KEYS
        },
        "networks": {
            "etk-greenfield": {
                "external": False,
                "internal": False,
                "labels": {"com.etk.environment": "greenfield"},
                "name": f"{PROJECT}_etk-greenfield",
            }
        },
    }
    return payload, allow_root, fake_socket


def _validate(payload: dict[str, Any], allow_root: Path, fake_socket: Path) -> dict[str, Any]:
    return _validate_resolved_compose(
        payload,
        allow_root=allow_root,
        fake_docker_socket=fake_socket,
        project=PROJECT,
        image_ref=IMAGE_ID,
        api_port=API_PORT,
        pg_port=PG_PORT,
    )


def test_new_root_rejects_relative_too_broad_and_existing_paths(tmp_path: Path) -> None:
    with pytest.raises(GreenfieldHarnessError, match="absolute path"):
        _validated_new_root(
            "etk-p12-greenfield-relative",
            label="allow-root",
            required_prefix="etk-p12-greenfield-",
        )

    with pytest.raises(GreenfieldHarnessError, match="too broad"):
        _validated_new_root(
            "/etk-p12-greenfield-root",
            label="allow-root",
            required_prefix="etk-p12-greenfield-",
        )

    existing = tmp_path / "etk-p12-greenfield-existing"
    existing.mkdir()
    with pytest.raises(GreenfieldHarnessError, match="already exists"):
        _validated_new_root(
            str(existing),
            label="allow-root",
            required_prefix="etk-p12-greenfield-",
        )


@pytest.mark.parametrize("reference", ["etk-refactor:latest", "sha256:1234", ""])
def test_image_ref_rejects_mutable_or_invalid_references(reference: str) -> None:
    with pytest.raises(GreenfieldHarnessError, match="immutable sha256"):
        _validated_image_ref(reference)


def test_image_ref_accepts_exact_digest() -> None:
    assert _validated_image_ref(IMAGE_ID) == IMAGE_ID


def test_runtime_bind_source_accepts_native_and_docker_desktop_paths(tmp_path: Path) -> None:
    expected = tmp_path / "etk-p12-greenfield-bind" / "disabled-docker.sock"
    docker_desktop_source = Path("/host_mnt") / expected.resolve().relative_to("/")

    assert _runtime_bind_source_matches(expected, expected)
    assert _runtime_bind_source_matches(docker_desktop_source, expected)
    assert not _runtime_bind_source_matches(tmp_path / "outside.sock", expected)


@pytest.mark.parametrize("project", ["etk-main", "etk-p11-guard-test", "etk-p12-production"])
def test_project_rejects_non_disposable_names(project: str) -> None:
    with pytest.raises(GreenfieldHarnessError):
        _validated_project(project)


def test_resolved_compose_rejects_external_bind_mount(tmp_path: Path) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)
    payload["services"]["control-plane"]["volumes"][-1]["source"] = str(tmp_path / "outside.sock")

    with pytest.raises(GreenfieldHarnessError, match="escapes the allow-root"):
        _validate(payload, allow_root, fake_socket)


def test_resolved_compose_rejects_non_loopback_port(tmp_path: Path) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)
    payload["services"]["api"]["ports"][0]["host_ip"] = "0.0.0.0"

    with pytest.raises(GreenfieldHarnessError, match="not loopback-isolated"):
        _validate(payload, allow_root, fake_socket)


def test_resolved_compose_rejects_internal_network_that_suppresses_host_probes(tmp_path: Path) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)
    payload["networks"]["etk-greenfield"]["internal"] = True

    with pytest.raises(GreenfieldHarnessError, match="network identity is unsafe"):
        _validate(payload, allow_root, fake_socket)


@pytest.mark.parametrize(
    ("resource", "mutation", "message"),
    [
        ("volume", "external", "volume app_config is external"),
        ("volume", "unmarked", "volume app_config lacks the Greenfield ownership label"),
        ("network", "external", "network identity is unsafe"),
        ("network", "unmarked", "network lacks the Greenfield ownership label"),
    ],
)
def test_resolved_compose_rejects_external_or_unmarked_resources(
    tmp_path: Path,
    resource: str,
    mutation: str,
    message: str,
) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)
    target = (
        payload["volumes"]["app_config"]
        if resource == "volume"
        else payload["networks"][next(iter(EXPECTED_NETWORK_KEYS))]
    )
    if mutation == "external":
        target["external"] = True
    else:
        target["labels"] = {}

    with pytest.raises(GreenfieldHarnessError, match=message):
        _validate(payload, allow_root, fake_socket)


def test_resolved_compose_rejects_unexpected_volume_mount(tmp_path: Path) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)
    payload["services"]["migrate"]["volumes"] = []

    with pytest.raises(GreenfieldHarnessError, match="volume mounts do not match"):
        _validate(payload, allow_root, fake_socket)


def test_valid_resolved_compose_is_accepted(tmp_path: Path) -> None:
    payload, allow_root, fake_socket = _resolved_payload(tmp_path)

    summary = _validate(payload, allow_root, fake_socket)

    assert summary["project"] == PROJECT
    assert summary["services"] == sorted(EXPECTED_SERVICES)
    assert summary["network_name"] == f"{PROJECT}_etk-greenfield"
    assert summary["volume_names"] == sorted(f"{PROJECT}_{key}" for key in EXPECTED_VOLUME_KEYS)
    assert summary["bind_mounts"] == [
        {
            "service": "control-plane",
            "source": str(fake_socket),
            "target": "/var/run/docker.sock",
            "read_only": True,
        }
    ]
