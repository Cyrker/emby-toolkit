from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx
from p115pickcode import to_id
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .auth_directory import P115CredentialStore
from .directory_transport import p115_cookie_user_agent
from .fingerprints import (
    P115FileFingerprint,
    P115FingerprintAccountChanged,
    P115FingerprintConflict,
    P115FingerprintRemoteClient,
)

_OPENAPI_INFO_URL = "https://proapi.115.com/open/folder/get_info"
_COOKIE_INFO_URL = "https://webapi.115.com/files/file"
_NOT_FOUND_CODES = frozenset(
    {
        404,
        20018,
        31001,
        31003,
        31004,
        50003,
        50015,
        70005,
        70008,
        90008,
        430004,
        800001,
    }
)


class P115FingerprintTransportError(RuntimeError):
    pass


class HttpxP115FingerprintRemoteGateway:
    """Open one deterministic, account- and credential-bound file-info reader."""

    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        priority: Callable[[], str],
        client_factory: Callable[..., httpx.Client] = httpx.Client,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._credentials = credentials
        self._priority = priority
        self._client_factory = client_factory
        self._timeout_seconds = max(1.0, float(timeout_seconds))

    def open(self, *, expected_account_id: str) -> P115FingerprintRemoteClient | None:
        account_id = str(expected_account_id or "").strip()
        if not account_id.isdigit():
            return None
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != account_id:
            raise P115FingerprintAccountChanged("115 account changed before fingerprint lookup")

        priority = str(self._priority() or "openapi").strip().lower()
        backends = ("cookie", "openapi") if priority == "cookie" else ("openapi", "cookie")
        for backend in backends:
            credential = snapshot.access_token if backend == "openapi" else snapshot.cookie
            if not credential:
                continue
            headers = (
                {
                    "Authorization": f"Bearer {credential}",
                    "User-Agent": "Emby-toolkit/1.0 (OpenAPI)",
                }
                if backend == "openapi"
                else {
                    "Cookie": credential,
                    "User-Agent": p115_cookie_user_agent(snapshot.app_type),
                }
            )
            return _HttpxP115FingerprintRemoteClient(
                credentials=self._credentials,
                account_id=account_id,
                backend=backend,
                headers=headers,
                credential_value=credential,
                credential_app_type="" if backend == "openapi" else snapshot.app_type,
                fallback_openapi_credential=(snapshot.access_token if backend == "cookie" else ""),
                client_factory=self._client_factory,
                timeout_seconds=self._timeout_seconds,
            )
        return None


class _HttpxP115FingerprintRemoteClient:
    def __init__(
        self,
        *,
        credentials: P115CredentialStore,
        account_id: str,
        backend: str,
        headers: Mapping[str, str],
        credential_value: str,
        credential_app_type: str,
        fallback_openapi_credential: str,
        client_factory: Callable[..., httpx.Client],
        timeout_seconds: float,
    ) -> None:
        self._credentials = credentials
        self._account_id = account_id
        self.backend = backend
        self._headers = dict(headers)
        self._credential_value = credential_value
        self._credential_app_type = credential_app_type
        self._fallback_openapi_credential = fallback_openapi_credential
        self._client_factory = client_factory
        self._timeout_seconds = timeout_seconds

    def lookup(self, pick_code: str) -> P115FileFingerprint | None:
        normalized_pick_code = str(pick_code or "").strip()
        if not normalized_pick_code:
            return None
        file_id = _file_id_from_pick_code(normalized_pick_code)
        payload = self._request_info(
            backend=self.backend,
            file_id=file_id,
            headers=self._headers,
            credential_value=self._credential_value,
            credential_app_type=self._credential_app_type,
        )
        if payload is None:
            return None
        data = _file_info_data(payload)
        if data is None:
            return None
        if self.backend == "cookie" and not _parent_id(data, file_id=file_id) and self._fallback_openapi_credential:
            fallback_payload = self._request_info(
                backend="openapi",
                file_id=file_id,
                headers={
                    "Authorization": f"Bearer {self._fallback_openapi_credential}",
                    "User-Agent": "Emby-toolkit/1.0 (OpenAPI)",
                },
                credential_value=self._fallback_openapi_credential,
                credential_app_type="",
            )
            if fallback_payload is None:
                raise P115FingerprintConflict("115 OpenAPI parent fallback did not find the Cookie file")
            fallback_data = _file_info_data(fallback_payload)
            if fallback_data is None:
                raise P115FingerprintConflict("115 OpenAPI parent fallback did not return file information")
            data = _merge_provider_info(data, fallback_data)
        return _fingerprint_from_info(
            data,
            requested_file_id=file_id,
            requested_pick_code=normalized_pick_code,
            account_id=self._account_id,
        )

    def _request_info(
        self,
        *,
        backend: str,
        file_id: str,
        headers: Mapping[str, str],
        credential_value: str,
        credential_app_type: str,
    ) -> Mapping[str, Any] | None:
        url = _OPENAPI_INFO_URL if backend == "openapi" else _COOKIE_INFO_URL

        def invoke() -> Mapping[str, Any] | None:
            self._assert_credentials(
                backend=backend,
                credential_value=credential_value,
                credential_app_type=credential_app_type,
            )
            try:
                with self._client_factory(
                    timeout=self._timeout_seconds,
                    follow_redirects=True,
                    trust_env=False,
                ) as client:
                    response = client.get(
                        url,
                        params={"file_id": file_id},
                        headers=headers,
                    )
                    if response.status_code == 404:
                        return None
                    if response.status_code >= 400:
                        raise P115FingerprintTransportError(
                            f"115 fingerprint {backend} returned HTTP {response.status_code}"
                        )
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        raise P115FingerprintTransportError(f"115 fingerprint {backend} returned invalid JSON") from exc
            finally:
                self._assert_credentials(
                    backend=backend,
                    credential_value=credential_value,
                    credential_app_type=credential_app_type,
                )
            if not isinstance(payload, Mapping):
                raise P115FingerprintTransportError(f"115 fingerprint {backend} returned an invalid response object")
            return payload

        retrying = Retrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.1, max=1.0),
            retry=retry_if_exception_type(httpx.TransportError),
            reraise=True,
        )
        try:
            return retrying(invoke)
        except httpx.TransportError as exc:
            raise P115FingerprintTransportError(f"115 fingerprint {backend} transport failed") from exc

    def _assert_credentials(
        self,
        *,
        backend: str,
        credential_value: str,
        credential_app_type: str,
    ) -> None:
        snapshot = self._credentials.load()
        if str(snapshot.account_id or "").strip() != self._account_id:
            raise P115FingerprintAccountChanged("115 account changed during fingerprint lookup")
        credential = snapshot.access_token if backend == "openapi" else snapshot.cookie
        app_type = "" if backend == "openapi" else snapshot.app_type
        if credential != credential_value or app_type != credential_app_type:
            raise P115FingerprintAccountChanged("115 credential changed during fingerprint lookup")


def _file_id_from_pick_code(pick_code: str) -> str:
    try:
        file_id = int(to_id(pick_code))
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("115 PickCode is invalid") from exc
    if file_id <= 0:
        raise ValueError("115 PickCode did not resolve to a file ID")
    return str(file_id)


def _file_info_data(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    if not _provider_succeeded(payload):
        code = _provider_error_code(payload)
        if code in _NOT_FOUND_CODES:
            return None
        suffix = f" with provider code {code}" if code is not None else ""
        raise P115FingerprintTransportError(f"115 file-info failed{suffix}")

    value: object = payload.get("data")
    if isinstance(value, Mapping):
        for key in ("file_info", "info"):
            nested = value.get(key)
            if isinstance(nested, (Mapping, list)):
                value = nested
                break
    if value is None:
        value = payload.get("file_info") or payload.get("info")
    if value is None and any(key in payload for key in ("fid", "file_id", "id", "pc", "pick_code")):
        value = payload
    if isinstance(value, list):
        if not value:
            return None
        if len(value) != 1:
            raise P115FingerprintTransportError("115 file-info returned multiple objects")
        value = value[0]
    if not isinstance(value, Mapping):
        raise P115FingerprintTransportError("115 file-info returned an invalid object")
    if not value:
        return None
    normalized = dict(value)
    for key in ("path", "paths", "breadcrumb"):
        if key not in normalized and key in payload:
            normalized[key] = payload[key]
    return normalized


def _merge_provider_info(
    primary: Mapping[str, Any],
    fallback: Mapping[str, Any],
) -> Mapping[str, Any]:
    for keys, label, normalize in (
        (("fid", "file_id", "id"), "file ID", _text),
        (("pc", "pick_code", "pickcode"), "PickCode", _text),
        (("sha1", "sha", "file_sha1"), "SHA1", _upper_text),
        (("fn", "n", "file_name", "name"), "file name", _text),
    ):
        left = _one_alias(primary, keys, label, normalize=normalize, required=False)
        right = _one_alias(fallback, keys, label, normalize=normalize, required=False)
        if left and right and left != right:
            raise P115FingerprintConflict(f"115 Cookie and OpenAPI returned conflicting {label}")
    left_size = _one_nonnegative_size(
        primary,
        ("size_byte", "fs", "size", "file_size"),
        required=False,
    )
    right_size = _one_nonnegative_size(
        fallback,
        ("size_byte", "fs", "size", "file_size"),
        required=False,
    )
    if left_size is not None and right_size is not None and left_size != right_size:
        raise P115FingerprintConflict("115 Cookie and OpenAPI returned conflicting file size")

    merged = dict(primary)
    for key, value in fallback.items():
        if key not in merged or merged[key] in (None, "", [], {}):
            merged[key] = value
    return merged


def _parent_id(data: Mapping[str, Any], *, file_id: str) -> str:
    direct_parent_id = _one_alias(
        data,
        ("parent_id", "pid", "cid"),
        "parent ID",
        required=False,
    )
    path_parent_ids: set[str] = set()
    for key in ("path", "paths", "breadcrumb"):
        nodes = data.get(key)
        if nodes is None:
            continue
        if not isinstance(nodes, list):
            raise P115FingerprintConflict("115 file-info path evidence is invalid")
        for node in reversed(nodes):
            if not isinstance(node, Mapping):
                raise P115FingerprintConflict("115 file-info path node is invalid")
            node_id = _one_alias(
                node,
                ("fid", "file_id", "id", "cid"),
                "path node ID",
                required=False,
            )
            category = _one_alias(
                node,
                ("fc", "file_category"),
                "path node category",
                required=False,
            )
            if node_id and node_id != file_id and category in {"", "0"}:
                path_parent_ids.add(node_id)
                break
    if len(path_parent_ids) > 1:
        raise P115FingerprintConflict("115 file-info path parents are inconsistent")
    path_parent_id = next(iter(path_parent_ids), "")
    if direct_parent_id and path_parent_id and direct_parent_id != path_parent_id:
        raise P115FingerprintConflict("115 file-info parent identity is inconsistent")
    return direct_parent_id or path_parent_id


def _fingerprint_from_info(
    data: Mapping[str, Any],
    *,
    requested_file_id: str,
    requested_pick_code: str,
    account_id: str,
) -> P115FileFingerprint:
    file_id = _one_alias(data, ("fid", "file_id", "id"), "file ID")
    pick_code = _one_alias(data, ("pc", "pick_code", "pickcode"), "PickCode")
    sha1 = _one_alias(
        data,
        ("sha1", "sha", "file_sha1"),
        "SHA1",
        normalize=lambda value: str(value).strip().upper(),
    )
    if file_id != requested_file_id or pick_code != requested_pick_code:
        raise P115FingerprintConflict("115 file-info identity did not match the requested PickCode")

    parent_id = _parent_id(data, file_id=file_id)
    if not file_id.isdigit() or not parent_id.isdigit():
        raise P115FingerprintConflict("115 file-info requires numeric file and parent IDs")
    if parent_id == file_id:
        raise P115FingerprintConflict("115 file-info cannot use the file as its own parent")

    category = _one_alias(
        data,
        ("fc", "file_category"),
        "file category",
        required=False,
    )
    if category == "0":
        raise P115FingerprintConflict("115 file-info returned a directory for a video fingerprint")

    name = _one_alias(data, ("fn", "n", "file_name", "name"), "file name")
    size = _one_nonnegative_size(data, ("size_byte", "fs", "size", "file_size"))
    assert size is not None
    try:
        return P115FileFingerprint(
            file_id=file_id,
            parent_id=parent_id,
            name=name,
            sha1=sha1,
            pick_code=pick_code,
            size=size,
            account_id=account_id,
        )
    except ValueError as exc:
        raise P115FingerprintConflict("115 file-info fingerprint fields are invalid") from exc


def _one_alias(
    data: Mapping[str, Any],
    keys: Sequence[str],
    label: str,
    *,
    normalize: Callable[[object], str] = lambda value: str(value).strip(),
    required: bool = True,
) -> str:
    values = {normalize(data[key]) for key in keys if data.get(key) not in (None, "")}
    if len(values) > 1:
        raise P115FingerprintConflict(f"115 file-info has conflicting {label} aliases")
    value = next(iter(values), "")
    if required and not value:
        raise P115FingerprintConflict(f"115 file-info is missing {label}")
    return value


def _one_nonnegative_size(
    data: Mapping[str, Any],
    keys: Sequence[str],
    *,
    required: bool = True,
) -> int | None:
    values: set[int] = set()
    for key in keys:
        raw = data.get(key)
        if raw in (None, ""):
            continue
        if isinstance(raw, bool):
            raise P115FingerprintConflict("115 file-info size is invalid")
        try:
            value = int(str(raw))
        except (TypeError, ValueError) as exc:
            raise P115FingerprintConflict("115 file-info size is invalid") from exc
        if value < 0:
            raise P115FingerprintConflict("115 file-info size is negative")
        values.add(value)
    if len(values) > 1:
        raise P115FingerprintConflict("115 file-info has conflicting size aliases")
    value = next(iter(values), None)
    if required and value is None:
        raise P115FingerprintConflict("115 file-info is missing file size")
    return value


def _text(value: object) -> str:
    return str(value).strip()


def _upper_text(value: object) -> str:
    return str(value).strip().upper()


def _provider_error_code(payload: Mapping[str, Any]) -> int | None:
    for key in ("code", "errno", "errNo"):
        raw = payload.get(key)
        if raw in (None, "") or isinstance(raw, bool):
            continue
        try:
            return int(str(raw))
        except (TypeError, ValueError):
            return None
    return None


def _provider_succeeded(payload: Mapping[str, Any]) -> bool:
    state = payload.get("state")
    if state is True or state in (1, "1") or str(state).lower() == "true":
        return True
    if "state" in payload and state not in (None, ""):
        return False
    if payload.get("errno") in (0, "0") and not payload.get("error"):
        return True
    return payload.get("code") in (0, 200, "0", "200") and not (
        payload.get("error") or payload.get("error_msg") or payload.get("message")
    )


__all__ = [
    "HttpxP115FingerprintRemoteGateway",
    "P115FingerprintTransportError",
]
