from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

SECRET_KEYS = {
    "authorization",
    "cookie",
    "x-emby-token",
    "password",
    "db_password",
    "api_key",
    "secret",
    "token",
}
ALLOWED_SECRET_VALUES = {"", "********", "<redacted>", "fixture-token", "your_password"}
ALLOWED_URL_HOSTS = {"example.invalid", "localhost", "127.0.0.1"}
TOKEN_PATTERNS = [
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
    re.compile(r"\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\bBearer\s+(?!<redacted>)[A-Za-z0-9._~+/=-]{16,}\b", re.IGNORECASE),
]
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)


def _allowed_secret(value: str) -> bool:
    return value in ALLOWED_SECRET_VALUES or value.startswith("fixture-") or bool(re.fullmatch(r"[AB]{40,64}", value))


def scan_payload(payload: Any, source: str) -> list[str]:
    findings: list[str] = []

    def visit(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}/{key}"
                if str(key).casefold() in SECRET_KEYS and isinstance(child, str) and not _allowed_secret(child):
                    findings.append(f"{source}:{child_path}: non-placeholder secret-like value")
                visit(child, child_path)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{path}/{index}")
        elif isinstance(value, str):
            for url in URL_RE.findall(value):
                host = (urlparse(url).hostname or "").casefold()
                if host not in ALLOWED_URL_HOSTS and not host.endswith(".invalid"):
                    findings.append(f"{source}:{path}: non-fixture URL host {host}")
            if EMAIL_RE.search(value) and not value.casefold().endswith("@example.invalid"):
                findings.append(f"{source}:{path}: email-like personal data")
            field_name = path.rsplit("/", 1)[-1]
            is_route_path = field_name in {"path", "rule", "declared_path"}
            if value.startswith("/") and not is_route_path and not value.startswith("/fixture/"):
                findings.append(f"{source}:{path}: non-fixture absolute path")

    visit(payload, "")
    return findings


def scan_path(root: Path) -> list[str]:
    findings: list[str] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in {".json", ".yaml", ".yml"}:
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in TOKEN_PATTERNS:
            if pattern.search(text):
                findings.append(f"{path}: token/private-key pattern {pattern.pattern}")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            findings.append(f"{path}: fixture is not deterministic JSON/YAML-subset: {exc}")
            continue
        findings.extend(scan_payload(payload, str(path)))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Reject secrets and personal data in sanitized fixtures.")
    parser.add_argument("path", nargs="?", type=Path, default=Path("verification/fixtures"))
    args = parser.parse_args()
    findings = scan_path(args.path)
    if findings:
        print("\n".join(findings))
        return 1
    print(f"fixture_secret_scan=clean path={args.path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
