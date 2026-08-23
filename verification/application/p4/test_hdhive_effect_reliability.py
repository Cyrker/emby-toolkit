from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest
import requests

import task_manager
from etk.compatibility import tg_resource_effects
from etk.compatibility import tg_resource_operations as operations
from etk.compatibility.tg_resource_effects import (
    TelegramResourceEffectBindingStore,
    TelegramResourceEffectStore,
    TelegramResourceSecretExpiredError,
    TelegramResourceSecretStore,
)
from etk.platform.jobs import normalize_json_object
from handler import hdhive_client
from tasks import hdhive, subscriptions


class MemoryEffectBackend:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 23, tzinfo=UTC)
        self.states: dict[str, dict[str, Any]] = {}

    def mutate(self, setting_key, mutation):
        current = deepcopy(self.states.get(setting_key))
        next_state, result, should_write = mutation(current, self.now)
        if should_write:
            if next_state is None:
                self.states.pop(setting_key, None)
            else:
                self.states[setting_key] = normalize_json_object(next_state, field="effect state")
        return deepcopy(result)


class CaptureGateway:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def submit(
        self,
        task_function,
        task_name,
        processor_type,
        args,
        kwargs,
        *,
        silent,
        max_attempts=1,
        idempotency_key=None,
        idempotency_scope=None,
    ) -> bool:
        self.calls.append(
            {
                "task_function": task_function,
                "task_name": task_name,
                "processor_type": processor_type,
                "args": args,
                "kwargs": kwargs,
                "silent": silent,
                "max_attempts": max_attempts,
                "idempotency_key": idempotency_key,
                "idempotency_scope": idempotency_scope,
            }
        )
        return True

    @staticmethod
    def is_busy() -> bool:
        return False

    @staticmethod
    def status() -> dict[str, Any]:
        return {}


def _install_stores(monkeypatch):
    backend = MemoryEffectBackend()
    monkeypatch.setattr(tg_resource_effects, "_DEFAULT_STORE", TelegramResourceEffectStore(backend))
    monkeypatch.setattr(
        tg_resource_effects,
        "_DEFAULT_BINDING_STORE",
        TelegramResourceEffectBindingStore(backend),
    )
    monkeypatch.setattr(
        tg_resource_effects,
        "_DEFAULT_SECRET_STORE",
        TelegramResourceSecretStore(backend),
    )
    return backend


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self.text = "fixture"

    def json(self) -> dict[str, Any]:
        return {"success": False, "message": "fixture"}


@pytest.mark.parametrize("status_code", [408, 429, 500, 503])
def test_unlock_http_indeterminate_response_posts_once(monkeypatch, status_code: int) -> None:
    client = object.__new__(hdhive_client.HDHiveClient)
    client.limit_window = 60
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr(client, "_check_unlock_rate_limit", lambda: None)
    monkeypatch.setattr(
        client,
        "_request",
        lambda method, path, *_args, **_kwargs: calls.append((method, path)) or FakeResponse(status_code),
    )

    with pytest.raises(hdhive_client.HDHiveUnlockIndeterminateError):
        client.unlock_resource("fixture", max_retries=9)

    assert calls == [("POST", "/api/hdhive/unlock")]


@pytest.mark.parametrize(
    "error",
    [
        requests.exceptions.Timeout("timeout"),
        requests.exceptions.ConnectionError("connection"),
        requests.exceptions.SSLError("ssl"),
    ],
)
def test_unlock_transport_indeterminate_response_posts_once(monkeypatch, error: Exception) -> None:
    client = object.__new__(hdhive_client.HDHiveClient)
    client.limit_window = 60
    calls = 0
    monkeypatch.setattr(client, "_check_unlock_rate_limit", lambda: None)

    def request(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        raise error

    monkeypatch.setattr(client, "_request", request)
    with pytest.raises(hdhive_client.HDHiveUnlockIndeterminateError):
        client.unlock_resource("fixture", max_retries=9)

    assert calls == 1


def test_unlock_receipt_survives_downstream_failure_and_retry_destroys_secret(monkeypatch) -> None:
    _install_stores(monkeypatch)

    class FakeHDHiveClient:
        unlock_calls = 0
        ping_calls = 0
        instance_id = "relay-instance-1"

        @classmethod
        def ping(cls) -> bool:
            cls.ping_calls += 1
            return True

        def unlock_resource(self, _slug):
            self.__class__.unlock_calls += 1
            return {"url": "https://115.com/s/abc123", "access_code": "a1b2"}

    monkeypatch.setattr(hdhive, "HDHiveClient", FakeHDHiveClient)
    monkeypatch.setattr(hdhive_client, "HDHiveClient", FakeHDHiveClient)
    downstream_calls: list[dict[str, str]] = []

    def downstream(**kwargs):
        downstream_calls.append(dict(kwargs))
        if len(downstream_calls) == 1:
            raise RuntimeError("downstream failed")
        return {"message": "done", "organize_submitted": True}

    monkeypatch.setattr(operations, "execute_hdhive_p115_effect", downstream)

    with pytest.raises(RuntimeError, match="downstream failed"):
        hdhive.task_download_from_hdhive(slug="fixture-slug", operation_id="delivery-1")
    assert FakeHDHiveClient.unlock_calls == 1
    assert FakeHDHiveClient.ping_calls == 1

    result = hdhive.task_download_from_hdhive(slug="fixture-slug", operation_id="delivery-1")

    assert result["ok"] is True
    assert FakeHDHiveClient.unlock_calls == 1
    assert FakeHDHiveClient.ping_calls == 1
    assert len(downstream_calls) == 2
    secret_ref = downstream_calls[0]["unlock_secret_ref"]
    owner_key = downstream_calls[0]["unlock_owner_key"]
    with pytest.raises(TelegramResourceSecretExpiredError):
        tg_resource_effects.consume_secret(secret_ref, owner_key)


def test_indeterminate_unlock_attempt_is_never_posted_again(monkeypatch) -> None:
    _install_stores(monkeypatch)

    class FakeHDHiveClient:
        unlock_calls = 0
        instance_id = "relay-instance-1"

        @staticmethod
        def ping() -> bool:
            return True

        def unlock_resource(self, _slug):
            self.__class__.unlock_calls += 1
            raise hdhive_client.HDHiveUnlockIndeterminateError("indeterminate")

    monkeypatch.setattr(hdhive_client, "HDHiveClient", FakeHDHiveClient)

    with pytest.raises(operations.HDHiveUnlockPendingError):
        operations.acquire_hdhive_unlock_receipt("fixture-slug", "delivery-1")
    with pytest.raises(operations.HDHiveUnlockPendingError):
        operations.acquire_hdhive_unlock_receipt("fixture-slug", "delivery-1")

    assert FakeHDHiveClient.unlock_calls == 1


def test_web_hdhive_delivery_is_deduplicated_and_resource_payload_is_scrubbed(monkeypatch) -> None:
    gateway = CaptureGateway()
    monkeypatch.setattr(subscriptions.time, "time", lambda: 1_800_000_000.0)
    task_manager.install_persistent_submission_gateway(gateway)
    try:
        kwargs = {
            "slug": "fixture-slug",
            "tmdb_id": "100",
            "media_type": "movie",
            "title": "Fixture",
            "hdhive_resource": {
                "slug": "fixture-slug",
                "title": "Fixture",
                "url": "magnet:?xt=urn:btih:PRIVATE",
                "password": "private",
            },
        }
        assert subscriptions.submit_hdhive_download_job(**kwargs)
        assert subscriptions.submit_hdhive_download_job(**kwargs)
    finally:
        task_manager.clear_persistent_submission_gateway(gateway)

    assert len(gateway.calls) == 2
    assert gateway.calls[0]["idempotency_key"] == gateway.calls[1]["idempotency_key"]
    assert gateway.calls[0]["idempotency_scope"] == "hdhive-download-delivery:v2"
    assert gateway.calls[0]["max_attempts"] == 3
    serialized = json.dumps(gateway.calls[0]["kwargs"], ensure_ascii=False, sort_keys=True)
    assert "magnet:?" not in serialized
    assert "password" not in serialized
