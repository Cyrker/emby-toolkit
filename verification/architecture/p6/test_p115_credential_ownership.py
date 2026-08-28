from __future__ import annotations

import ast
import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
CREDENTIAL_REPOSITORY = REPO_ROOT / "src/etk/domains/p115/credential_repository.py"
COMPATIBILITY_GATEWAY = REPO_ROOT / "src/etk/compatibility/p115_credentials.py"
APP_FACTORY = REPO_ROOT / "src/etk/bootstrap/app_factory.py"
WORKER = REPO_ROOT / "src/etk/processes/worker.py"
LEGACY_PROVIDER = REPO_ROOT / "handler/p115_service.py"
CONFIG_OWNERSHIP = REPO_ROOT / "verification/contracts/config-ownership.yaml"


def test_native_credential_repository_has_no_legacy_runtime_imports() -> None:
    tree = ast.parse(CREDENTIAL_REPOSITORY.read_text(encoding="utf-8"))
    imported = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names} | {
        str(node.module or "") for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
    }

    assert all(
        not name.startswith(("config_manager", "database", "extensions", "handler", "tasks")) for name in imported
    )
    source = CREDENTIAL_REPOSITORY.read_text(encoding="utf-8")
    assert "pg_advisory_xact_lock" in source
    assert "DYNAMIC_CONFIG_LOCK_NAMESPACE" in source
    assert "P115_AUTH_LOCK_KEY" in source


def test_native_app_and_worker_do_not_construct_legacy_credential_store() -> None:
    for source in (APP_FACTORY, WORKER):
        tree = ast.parse(source.read_text(encoding="utf-8"))
        names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
        assert "LegacyP115CredentialStore" not in names, source
        assert "SqlAlchemyP115CredentialRepository" in names, source
        assert "install_persistent_p115_credential_gateway" in names, source
        assert "invalidate_legacy_p115_credential_clients" in names, source


def test_active_legacy_provider_reads_and_refresh_writes_use_installed_gateway() -> None:
    tree = ast.parse(LEGACY_PROVIDER.read_text(encoding="utf-8"))
    functions = {node.name: node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}

    for name in ("get_115_tokens", "get_115_account_id", "save_115_tokens", "refresh_115_token"):
        called_names = {node.id for node in ast.walk(functions[name]) if isinstance(node, ast.Name)}
        assert "_persistent_p115_credential_gateway" in called_names
    assert any(
        isinstance(node, ast.Attribute) and node.attr == "save_bundle"
        for node in ast.walk(functions["save_115_tokens"])
    )

    compatibility_imports = {
        str(node.module or "")
        for node in ast.walk(ast.parse(COMPATIBILITY_GATEWAY.read_text(encoding="utf-8")))
        if isinstance(node, ast.ImportFrom)
    }
    assert all(not name.startswith(("database", "tasks")) for name in compatibility_imports)


def test_p115_auth_tokens_has_native_repository_ownership() -> None:
    manifest = json.loads(CONFIG_OWNERSHIP.read_text(encoding="utf-8"))
    entry = next(item for item in manifest["standalone_app_settings"] if item["key"] == "p115_auth_tokens")

    assert entry["status"] == "new"
    assert entry["new_owner"] == "etk.domains.p115.credential_repository.SqlAlchemyP115CredentialRepository"
    assert entry["validation_owner"] == "p115/credentials"
