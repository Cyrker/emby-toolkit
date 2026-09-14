from __future__ import annotations

import inspect
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import websocket

import constants
from etk.bootstrap.cli import _run_proxy
from etk.bootstrap.nginx import (
    DISABLED_MARKER,
    NginxConfigError,
    generate_nginx_config,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
PROXY_BASELINE = REPO_ROOT / "verification" / "contracts" / "http" / "legacy-proxy-runtime-url-map.json"


def test_nginx_disabled_enabled_and_last_good_atomicity(tmp_path: Path) -> None:
    output = tmp_path / "default.conf"
    disabled = {constants.CONFIG_OPTION_PROXY_ENABLED: False}
    generate_nginx_config(disabled, output=output)
    assert output.read_text(encoding="utf-8") == DISABLED_MARKER

    enabled = {
        constants.CONFIG_OPTION_PROXY_ENABLED: True,
        constants.CONFIG_OPTION_EMBY_SERVER_URL: "http://emby.example.invalid:8096",
        constants.CONFIG_OPTION_PROXY_PORT: 18097,
    }
    generate_nginx_config(enabled, output=output, proxy_upstream="proxy:7758")
    rendered = output.read_text(encoding="utf-8")
    assert "server emby.example.invalid:8096;" in rendered
    assert "server proxy:7758;" in rendered
    assert "listen 0.0.0.0:18097;" in rendered

    with pytest.raises(NginxConfigError, match="emby_server_url"):
        generate_nginx_config(
            {
                constants.CONFIG_OPTION_PROXY_ENABLED: True,
                constants.CONFIG_OPTION_EMBY_SERVER_URL: "",
            },
            output=output,
        )
    assert output.read_text(encoding="utf-8") == rendered


def test_proxy_cli_uses_native_asgi_runtime_without_legacy_imports() -> None:
    source = inspect.getsource(_run_proxy)
    assert "gevent" not in source
    assert "reverse_proxy" not in source
    assert "is_pro_active" not in source
    assert "license_active" not in source
    assert "create_production_proxy_app" in source
    assert "uvicorn.run" in source


def test_proxy_route_map_and_inactive_license_http_contract(tmp_path: Path) -> None:
    script = """
import json
import threading

before = {thread.ident for thread in threading.enumerate()}
import config_manager
config_manager.APP_CONFIG.clear()
config_manager.APP_CONFIG['is_pro_active'] = False
import reverse_proxy
rules = []
for rule in reverse_proxy.proxy_app.url_map.iter_rules():
    rules.append({
        'path': rule.rule,
        'methods_contract': sorted(set(rule.methods) - {'HEAD', 'OPTIONS'}),
    })
response = reverse_proxy.proxy_app.test_client().get('/Users')
after = {thread.ident for thread in threading.enumerate()}
print('RESULT=' + json.dumps({
    'rules': sorted(rules, key=lambda item: item['path']),
    'status': response.status_code,
    'body': response.get_json(),
    'new_threads': sorted(str(value) for value in after - before),
}))
"""
    environment = os.environ.copy()
    environment.update(APP_DATA_DIR=str(tmp_path / "data"), PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    result_line = next(line for line in reversed(result.stdout.splitlines()) if line.startswith("RESULT="))
    payload = json.loads(result_line.removeprefix("RESULT="))
    baseline = json.loads(PROXY_BASELINE.read_text(encoding="utf-8"))
    expected_rules = sorted(
        (
            {
                "path": rule["path"],
                "methods_contract": rule["methods_contract"],
            }
            for rule in baseline["rules"]
        ),
        key=lambda item: item["path"],
    )
    assert payload["rules"] == expected_rules
    assert payload["status"] == 403
    assert payload["body"] == {"error": "Pro subscription expired or not activated. 302 Proxy service is disabled."}
    assert payload["new_threads"] == []


def test_gevent_websocket_handler_exposes_wsgi_websocket(tmp_path: Path) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    child_code = """
from gevent import monkey
monkey.patch_all()
import sys
from gevent.pywsgi import WSGIServer
from geventwebsocket.handler import WebSocketHandler

def app(environ, start_response):
    client = environ.get('wsgi.websocket')
    if client is None:
        start_response('500 Internal Server Error', [('Content-Type', 'text/plain')])
        return [b'missing wsgi.websocket']
    value = client.receive()
    if value is not None:
        client.send('echo:' + value)
    client.receive()
    return []

WSGIServer(('127.0.0.1', int(sys.argv[1])), app, handler_class=WebSocketHandler).serve_forever()
"""
    environment = os.environ.copy()
    environment.update(APP_DATA_DIR=str(tmp_path / "data"), PYTHONDONTWRITEBYTECODE="1")
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", child_code, str(port)],
        cwd=REPO_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    connection = None
    deadline = time.monotonic() + 8
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(f"WebSocket stub exited early: {stdout}\n{stderr}")
            try:
                connection = websocket.create_connection(f"ws://127.0.0.1:{port}/ws", timeout=1)
                break
            except OSError:
                time.sleep(0.05)
        assert connection is not None, "WebSocket stub did not become ready"
        connection.send("fixture")
        assert connection.recv() == "echo:fixture"
    finally:
        if connection is not None:
            connection.close()
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def test_proxy_sigterm_stops_listener_and_runtime(tmp_path: Path) -> None:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]

    child_code = """
import sys
from etk.bootstrap.cli import main
raise SystemExit(main(['proxy', '--host', '127.0.0.1', '--port', sys.argv[1]]))
"""
    environment = os.environ.copy()
    environment.update(PYTHONDONTWRITEBYTECODE="1", ETK_ENABLE_LEGACY_COMPATIBILITY="0")
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", child_code, str(port)],
        cwd=REPO_ROOT,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(f"Proxy exited before SIGTERM: {stdout}\n{stderr}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            raise AssertionError("Proxy listener did not become ready")

        process.send_signal(signal.SIGTERM)
        assert process.wait(timeout=8) in {0, -signal.SIGTERM, 128 + signal.SIGTERM}
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
