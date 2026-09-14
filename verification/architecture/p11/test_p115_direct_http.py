from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
P115_ROOT = REPO_ROOT / "src/etk/domains/p115"


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8-sig")


def test_p115_api_and_direct_media_clients_ignore_proxy_configuration_and_environment() -> None:
    direct_client_calls = {
        "src/etk/domains/p115/adapters.py": (
            "self._client_factory(timeout=timeout, follow_redirects=True, trust_env=False)",
        ),
        "src/etk/domains/p115/directory_transport.py": ("trust_env=False,",),
        "src/etk/domains/p115/fingerprint_transport.py": ("trust_env=False,",),
        "src/etk/domains/p115/organize_transport.py": ("trust_env=False,",),
        "src/etk/domains/p115/strm_adapters.py": (
            "self._client_factory(timeout=timeout, follow_redirects=True, trust_env=False)",
        ),
        "src/etk/domains/p115/playback_adapters.py": (
            "self._http_client_factory(timeout=10, follow_redirects=True, trust_env=False)",
            "self._http_client_factory(timeout=timeout, follow_redirects=True, trust_env=False)",
            "self._client_factory(timeout=10, follow_redirects=True, trust_env=False)",
        ),
        "src/etk/domains/p115/playback_native.py": (
            "self._http_client_factory(timeout=30, follow_redirects=True, trust_env=False)",
            "self._client_factory(timeout=self._timeout_seconds, follow_redirects=False, trust_env=False)",
        ),
        "src/etk/domains/p115/media_info_adapters.py": ("trust_env=False,",),
        "src/etk/domains/p115/maintenance_transport.py": ("trust_env=False,",),
    }

    for relative_path, expected_calls in direct_client_calls.items():
        source = _source(relative_path)
        for expected_call in expected_calls:
            assert expected_call in source, relative_path

    proxy_configuration_users = {
        path.relative_to(REPO_ROOT).as_posix()
        for path in P115_ROOT.rglob("*.py")
        if "network_http_proxy_url" in path.read_text(encoding="utf-8-sig")
    }
    assert proxy_configuration_users == {
        "src/etk/domains/p115/washing_recalculation_adapters.py",
    }

    for path in P115_ROOT.rglob("*.py"):
        if path.name == "washing_recalculation_adapters.py":
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8-sig"))):
            if not isinstance(node, ast.Call) or ast.unparse(node.func) != "httpx.Client":
                continue
            assert any(
                keyword.arg == "trust_env" and isinstance(keyword.value, ast.Constant) and keyword.value.value is False
                for keyword in node.keywords
            ), path


def test_p115_emby_clients_are_direct_but_tmdb_runtime_lookup_keeps_its_proxy() -> None:
    for relative_path in (
        "src/etk/domains/p115/bridge_compatibility.py",
        "src/etk/domains/p115/emby_path_gateway.py",
        "src/etk/domains/p115/intro_runtime.py",
        "src/etk/domains/p115/media_info_restore.py",
        "src/etk/domains/p115/quick_deploy.py",
    ):
        source = _source(relative_path)
        assert "trust_env=False" in source, relative_path
        assert "network_http_proxy_url" not in source, relative_path

    tmdb_runtime = _source("src/etk/domains/p115/washing_recalculation_adapters.py")
    assert 'settings.network_http_proxy_url.strip() if settings.network_proxy_enabled else ""' in tmdb_runtime
    assert "httpx.Client(proxy=proxy or None, timeout=20)" in tmdb_runtime

    rapid_transfer = _source("src/etk/domains/sharing/rapid_transfer.py")
    assert "httpx.Client(timeout=max(1.0, timeout_seconds), trust_env=False)" in rapid_transfer


def test_legacy_p115_requests_facade_disables_environment_and_explicit_proxies() -> None:
    service = _source("handler/p115_service.py")
    assert "class _P115DirectRequestsSession(_requests.Session):" in service
    assert "self.trust_env = False" in service
    assert 'kwargs.pop("proxies", None)' in service
    assert "P115_DIRECT_REQUESTS = _P115DirectRequestsFacade()" in service

    for relative_path in (
        "routes/p115.py",
        "handler/p115_play_pool.py",
        "handler/p115_play_pool_client.py",
        "handler/p115_iso_analyzer.py",
        "tasks/p115.py",
    ):
        source = _source(relative_path)
        assert "requests = P115_DIRECT_REQUESTS" in source, relative_path
        assert "import requests" not in source, relative_path


def test_legacy_p115_requests_facade_uses_direct_sessions_for_all_http_verbs(monkeypatch) -> None:
    from handler import p115_service

    calls: list[tuple[str, dict[object, object]]] = []

    def fake_request(_session, method, _url, **kwargs):
        calls.append((method, kwargs))
        return p115_service._requests.Response()

    monkeypatch.setattr(p115_service._requests.Session, "request", fake_request)

    session = p115_service.P115_DIRECT_REQUESTS.Session()
    assert session.trust_env is False
    session.request("GET", "https://example.invalid", proxies={"https": "http://proxy.invalid"})

    for method in ("get", "post", "put", "request"):
        call = getattr(p115_service.P115_DIRECT_REQUESTS, method)
        if method == "request":
            call("DELETE", "https://example.invalid", proxies={"https": "http://proxy.invalid"})
        else:
            call("https://example.invalid", proxies={"https": "http://proxy.invalid"})

    assert [method for method, _ in calls] == ["GET", "GET", "POST", "PUT", "DELETE"]
    assert all("proxies" not in kwargs for _, kwargs in calls)
