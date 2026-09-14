from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _source(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8-sig")


def _callee(call: ast.Call) -> str:
    return ast.unparse(call.func)


def _function_calls(relative_path: str, function_name: str) -> set[str]:
    tree = ast.parse(_source(relative_path))
    function = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name
    )
    return {_callee(node) for node in ast.walk(function) if isinstance(node, ast.Call)}


def test_legacy_lan_emby_requests_use_the_proxy_independent_client() -> None:
    reverse_proxy = _source("reverse_proxy.py")
    reverse_calls = {_callee(node) for node in ast.walk(ast.parse(reverse_proxy)) if isinstance(node, ast.Call)}

    assert "requests." not in reverse_proxy
    assert {"emby.emby_client.get", "emby.emby_client.request"}.issubset(reverse_calls)
    assert "emby.emby_client.get" in _function_calls("tasks/media.py", "task_populate_metadata_cache")
    assert "emby.emby_client.get" in _function_calls("routes/user_portal.py", "get_dashboard_stats")
    assert "request_get = self._request_get or emby_client.get" in _source(
        "src/etk/domains/media/image_proxy_compatibility.py"
    )
    assert "self.session.trust_env = False" in _source("handler/emby.py")


def test_legacy_external_requests_keep_their_existing_configured_proxy_paths() -> None:
    assert "requests.get(image_url, timeout=20, proxies=proxies, headers=headers)" in _source("tasks/media.py")
    assert "proxies=config_manager.get_proxies_for_requests()," in _source("handler/poster_generator.py")
    assert "requests.get(api_url, timeout=20, proxies=proxies)" in _source("routes/user_portal.py")
    assert "proxies=config_manager.get_proxies_for_requests()" in _source("routes/webhook.py")
    assert "kwargs['proxies'] = proxies" in _source("watchlist_processor.py")
    assert "requests.get(models_url, headers=headers, params=params, timeout=20, proxies=proxies)" in _source(
        "routes/system.py"
    )
