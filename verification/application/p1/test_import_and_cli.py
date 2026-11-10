from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]


def _environment(tmp_path: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        APP_DATA_DIR=str(tmp_path / "data"),
        HOME=str(tmp_path / "home"),
        PYTHONDONTWRITEBYTECODE="1",
    )
    return environment


def test_plaintext_imports_are_side_effect_free(tmp_path: Path) -> None:
    script = """
import json
import pathlib
import sys
import threading

data_dir = pathlib.Path(__import__('os').environ['APP_DATA_DIR'])
home_dir = pathlib.Path(__import__('os').environ['HOME'])
before = {thread.ident for thread in threading.enumerate()}
import main
import etk.bootstrap.app_factory
after = {thread.ident for thread in threading.enumerate()}
forbidden = [
    name for name in (
        'web_app', 'pyarmor_runtime_000000', 'config_manager', 'extensions',
        'reverse_proxy', 'handler.tg_userbot', 'scheduler_manager', 'task_manager',
    )
    if name in sys.modules
]
print(json.dumps({
    'forbidden': forbidden,
    'new_threads': sorted(str(value) for value in after - before),
    'data_exists': data_dir.exists(),
    'home_exists': home_dir.exists(),
}))
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        env=_environment(tmp_path),
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload == {
        "forbidden": [],
        "new_threads": [],
        "data_exists": False,
        "home_exists": False,
    }


def test_cli_help_is_lazy_and_complete(tmp_path: Path) -> None:
    environment = _environment(tmp_path)
    commands = (
        ["--help"],
        ["api", "--help"],
        ["proxy", "--help"],
        ["generate-nginx-config", "--help"],
    )
    for arguments in commands:
        result = subprocess.run(
            [sys.executable, "main.py", *arguments],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        assert "usage: embytool" in result.stdout

    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "home").exists()


def test_cli_rejects_missing_unknown_and_invalid_roles(tmp_path: Path) -> None:
    environment = _environment(tmp_path)
    cases = (
        [],
        ["unknown-command"],
        ["api", "--port", "not-a-number"],
        ["api", "--port", "70000"],
    )
    for arguments in cases:
        result = subprocess.run(
            [sys.executable, "main.py", *arguments],
            cwd=REPO_ROOT,
            env=environment,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 2
        assert "error:" in result.stderr


def test_refactor_image_definition_uses_only_plaintext_entrypoint() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile.refactor").read_text(encoding="utf-8")
    entrypoint = (REPO_ROOT / "docker" / "entrypoint.refactor.sh").read_text(encoding="utf-8")

    assert "web_app.py" not in dockerfile
    assert "pyarmor_runtime" not in dockerfile
    assert "main.py" in dockerfile
    assert "entrypoint.refactor.sh" in dockerfile
    assert 'exec "$@"' in entrypoint
    assert "web_app.py" not in entrypoint
