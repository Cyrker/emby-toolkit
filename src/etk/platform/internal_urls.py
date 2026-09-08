from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit


def validate_internal_service_url(value: str) -> str:
    url = str(value or "").strip().rstrip("/")
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("地址必须以 http:// 或 https:// 开头")
    try:
        host_ip = ipaddress.ip_address(parsed.hostname)
    except ValueError as exc:
        raise ValueError("地址必须使用内网 IP，不能使用域名") from exc
    if not host_ip.is_private or host_ip.is_loopback or host_ip.is_link_local or host_ip.is_unspecified:
        raise ValueError("地址不是可用的内网 IP")
    return url


def derive_internal_service_url(source_url: str, port: int) -> str:
    source = validate_internal_service_url(source_url)
    parsed = urlsplit(source)
    service_port = int(port)
    if not 1 <= service_port <= 65535:
        raise ValueError("反向代理端口无效")
    assert parsed.hostname is not None
    host = parsed.hostname
    if ":" in host:
        host = f"[{host}]"
    return f"http://{host}:{service_port}"


__all__ = ["derive_internal_service_url", "validate_internal_service_url"]
