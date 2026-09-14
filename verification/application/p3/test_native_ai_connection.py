from __future__ import annotations

import builtins
import json

import httpx
import pytest

from etk.domains.system.integration_compatibility import HttpxSystemIntegrationGateway
from etk.platform import Settings


@pytest.mark.parametrize("provider", ["openai", "zhipuai", "gemini"])
def test_ai_connection_uses_native_http_without_legacy_imports(monkeypatch, provider):
    original_import = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name in {"database", "ai_translator"}:
            raise AssertionError("AI test must not load legacy database or translator")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    calls = []

    def respond(request):
        calls.append(request)
        body = json.loads(request.content)
        if provider == "gemini":
            assert request.headers["x-goog-api-key"] == "fixture-key"
            assert request.url.path.endswith("/models/model-a:generateContent")
            assert body["contents"][0]["parts"][0]["text"]
            return httpx.Response(200, json={"candidates": [{"content": {"parts": [{"text": "光头强"}]}}]})
        assert request.url.path == "/v1/chat/completions"
        assert request.headers["Authorization"] == "Bearer fixture-key"
        assert body["model"] == "model-a"
        return httpx.Response(200, json={"choices": [{"message": {"content": "光头强"}}]})

    gateway = HttpxSystemIntegrationGateway(lambda: Settings())
    monkeypatch.setattr(gateway, "_ai_client", lambda *_: httpx.Client(transport=httpx.MockTransport(respond)))
    result = gateway.test_ai(
        {
            "ai_provider": provider,
            "ai_api_key": "fixture-key",
            "ai_model_name": "model-a",
            "ai_base_url": "https://ai.fixture.invalid/v1",
        }
    )
    assert result.status_code == 200 and result.payload["success"]
    assert "光头强" in result.payload["message"] and len(calls) == 1


@pytest.mark.parametrize(
    "status,body",
    [(401, {}), (429, {"error": "SECRET"}), (500, {"error": "SECRET"}), (200, {}), (200, {"choices": []})],
)
def test_ai_errors_are_actionable_and_do_not_echo_provider_secrets(monkeypatch, status, body):
    gateway = HttpxSystemIntegrationGateway(lambda: Settings())
    monkeypatch.setattr(
        gateway,
        "_ai_client",
        lambda *_: httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(status, json=body))),
    )
    result = gateway.test_ai({"ai_api_key": "fixture-key", "ai_model_name": "model-a"})
    assert not result.payload["success"] and result.status_code >= 400
    assert "SECRET" not in result.payload["message"]


@pytest.mark.parametrize(
    "host,proxied",
    [
        ("api.openai.com", True),
        ("generativelanguage.googleapis.com", True),
        ("192.168.1.2", False),
        ("localhost", False),
        ("api.siliconflow.cn", False),
        ("ai.fixture.invalid", False),
    ],
)
def test_ai_proxy_is_not_applied_to_lan_domestic_or_custom_services(monkeypatch, host, proxied):
    calls = []
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: calls.append(kwargs))
    gateway = HttpxSystemIntegrationGateway(
        lambda: Settings(network_proxy_enabled=True, network_http_proxy_url="http://proxy.fixture.invalid:8080")
    )
    gateway._ai_client(f"https://{host}/v1", {})
    assert bool(calls[0]["proxy"]) is proxied
    assert calls[0]["trust_env"] is False


def test_ai_timeout_and_empty_configuration_do_not_report_success(monkeypatch):
    gateway = HttpxSystemIntegrationGateway(lambda: Settings())
    assert gateway.test_ai({}).status_code == 400

    def timeout(_):
        raise httpx.ReadTimeout("fixture timeout")

    monkeypatch.setattr(gateway, "_ai_client", lambda *_: httpx.Client(transport=httpx.MockTransport(timeout)))
    result = gateway.test_ai({"ai_api_key": "fixture-key", "ai_model_name": "model-a"})
    assert result.status_code == 504 and not result.payload["success"]
