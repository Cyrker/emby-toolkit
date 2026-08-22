from __future__ import annotations

import argparse
import ipaddress
import json
import math
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from manifest_common import REPO_ROOT, dump_json


def _loopback_base_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("base URL must be an absolute HTTP(S) URL")
    try:
        address = ipaddress.ip_address(parsed.hostname)
    except ValueError as exc:
        raise ValueError("P0 runtime capture accepts literal loopback hosts only") from exc
    if not address.is_loopback:
        raise ValueError("P0 runtime capture refuses non-loopback targets")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("base URL must not include a path, query, or fragment")
    return value.rstrip("/")


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _request(url: str, timeout: float) -> tuple[int, str, bytes, float]:
    request = urllib.request.Request(
        url,
        method="GET",
        headers={"Accept": "application/json", "User-Agent": "etk-p0-contract-exporter"},
    )
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read(65537)
            return response.status, response.headers.get_content_type(), body, time.perf_counter() - started
    except urllib.error.HTTPError as exc:
        body = exc.read(65537)
        return exc.code, exc.headers.get_content_type(), body, time.perf_counter() - started


def capture_health(base_url: str, samples: int, timeout: float) -> dict[str, Any]:
    url = f"{_loopback_base_url(base_url)}/api/health"
    timings: list[float] = []
    statuses: list[int] = []
    content_types: list[str] = []
    response_json: Any = None
    for _ in range(samples):
        status, content_type, body, elapsed = _request(url, timeout)
        if len(body) > 65536:
            raise ValueError("health response exceeds the 64 KiB capture limit")
        statuses.append(status)
        content_types.append(content_type)
        timings.append(elapsed * 1000)
        if content_type == "application/json":
            response_json = json.loads(body.decode("utf-8"))
    return {
        "request": {"method": "GET", "path": "/api/health"},
        "response": {
            "statuses": sorted(set(statuses)),
            "content_types": sorted(set(content_types)),
            "json": response_json,
        },
        "latency_ms": {
            "samples": len(timings),
            "min": round(min(timings), 3),
            "p50": round(_percentile(timings, 0.50), 3),
            "p95": round(_percentile(timings, 0.95), 3),
            "max": round(max(timings), 3),
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Capture a sanitized health/performance contract from an isolated loopback reference."
    )
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO_ROOT / "verification/contracts/http/legacy-local-health.json",
    )
    args = parser.parse_args()
    if not 1 <= args.samples <= 1000:
        parser.error("--samples must be between 1 and 1000")
    payload = {
        "schema_version": 1,
        "environment": "disposable-local-legacy-reference",
        "external_credentials_used": False,
        "health": capture_health(args.base_url, args.samples, args.timeout),
    }
    dump_json(args.output, payload)
    print(
        f"health_samples={args.samples} status={payload['health']['response']['statuses']} "
        f"p95_ms={payload['health']['latency_ms']['p95']} output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
