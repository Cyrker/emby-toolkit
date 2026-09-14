from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import Any, NoReturn
from urllib.parse import quote, urlparse, urlunparse

import httpx

_CREATE_STATUS_CODES = frozenset({200, 201, 204})
_WRITE_STATUS_CODES = frozenset({200, 204})
_STATUS_STATES = frozenset({"P", "R", "S"})
_DEFAULT_TIMEOUT_SECONDS = 20.0
_MISSING = object()


class MoviePilotProviderErrorKind(StrEnum):
    AUTHENTICATION = "authentication"
    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    RATE_LIMITED = "rate_limited"
    SERVER = "server"
    TIMEOUT = "timeout"
    TRANSPORT = "transport"
    INVALID_RESPONSE = "invalid_response"
    REJECTED = "rejected"


class MoviePilotQueryDisposition(StrEnum):
    FOUND = "found"
    EMPTY = "empty"


class MoviePilotWriteOutcome(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    INDETERMINATE = "indeterminate"


class MoviePilotProviderError(RuntimeError):
    def __init__(
        self,
        kind: MoviePilotProviderErrorKind,
        operation: str,
        message: str,
        *,
        status_code: int | None = None,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.operation = operation
        self.status_code = status_code
        self.outcome = outcome


@dataclass(frozen=True, slots=True)
class MoviePilotListResult:
    subscriptions: tuple[Mapping[str, Any], ...]

    @property
    def disposition(self) -> MoviePilotQueryDisposition:
        return MoviePilotQueryDisposition.EMPTY if self.empty else MoviePilotQueryDisposition.FOUND

    @property
    def empty(self) -> bool:
        return not self.subscriptions


@dataclass(frozen=True, slots=True)
class MoviePilotActiveDownload:
    task_hash: str
    subscribe_id: int | None
    tmdb_id: str | None
    season: int | None
    progress: float | None = None
    state: str = ""
    tags: tuple[str, ...] = ()
    tracker_text: str = ""
    added_at: int | None = None
    title: str = ""
    episode: int | None = None
    moviepilot_active: bool = True


@dataclass(frozen=True, slots=True)
class MoviePilotDownloadSnapshot:
    downloads: tuple[MoviePilotActiveDownload, ...]
    authoritative: bool


@dataclass(frozen=True, slots=True)
class MoviePilotQueryResult:
    disposition: MoviePilotQueryDisposition
    subscription: Mapping[str, Any] | None

    @property
    def found(self) -> bool:
        return self.disposition is MoviePilotQueryDisposition.FOUND

    @property
    def empty(self) -> bool:
        return self.disposition is MoviePilotQueryDisposition.EMPTY


@dataclass(frozen=True, slots=True)
class MoviePilotWriteResult:
    operation: str
    status_code: int
    confirmed: bool = True
    idempotent: bool = False

    @property
    def outcome(self) -> MoviePilotWriteOutcome:
        return MoviePilotWriteOutcome.CONFIRMED


class HttpxMoviePilotGateway:
    """Typed MoviePilot subscription HTTP boundary.

    MoviePilot exposes no idempotency-key contract for these writes, so this
    adapter never invents an idempotency header. Safe write replay belongs to
    the caller's durable effect ledger. The only in-adapter replay is one
    authenticated retry after a provider 401 response.
    """

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        *,
        timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS,
        client_factory: Callable[[], httpx.Client] | None = None,
    ) -> None:
        normalized_base_url = str(base_url or "").strip().rstrip("/")
        normalized_username = str(username or "")
        normalized_password = str(password or "")
        normalized_timeout = float(timeout_seconds)
        if not normalized_base_url or not normalized_username or not normalized_password:
            raise ValueError("MoviePilot base URL, username, and password are required")
        if normalized_timeout <= 0:
            raise ValueError("MoviePilot timeout must be positive")

        self._base_url = normalized_base_url
        self._username = normalized_username
        self._password = normalized_password
        self._timeout_seconds = normalized_timeout
        self._client_factory = client_factory or self._default_client
        self._token_lock = Lock()
        self._cached_access_token: str | None = None

    def list_subscriptions(self) -> MoviePilotListResult:
        operation = "list-subscriptions"
        response = self._authorized_request(operation, "GET", "/api/v1/subscribe/")
        self._expect_status(response, operation, {200})
        decoded = self._decode_json(response, operation)
        if not isinstance(decoded, list):
            self._raise_invalid_response(operation, response, "subscription list is not an array")

        subscriptions: list[Mapping[str, Any]] = []
        for index, subscription in enumerate(decoded):
            if not isinstance(subscription, Mapping):
                self._raise_invalid_response(
                    operation,
                    response,
                    f"subscription list item {index} is not an object",
                )
            subscriptions.append(dict(subscription))
        return MoviePilotListResult(tuple(subscriptions))

    def list_transfer_history(
        self,
        *,
        status: bool | None = None,
        limit: int = 100,
        page: int = 1,
    ) -> tuple[Mapping[str, Any], ...]:
        """Read a bounded transfer-history page for maintenance reconciliation."""
        operation = "list-transfer-history"
        if isinstance(limit, bool) or int(limit) < 1 or int(limit) > 500:
            raise ValueError("MoviePilot transfer history limit must be between 1 and 500")
        if isinstance(page, bool) or int(page) < 1:
            raise ValueError("MoviePilot transfer history page must be positive")
        params: dict[str, Any] = {"page": int(page), "count": int(limit)}
        if status is not None:
            params["status"] = "true" if status else "false"
        response = self._authorized_request(operation, "GET", "/api/v1/history/transfer", params=params)
        self._expect_status(response, operation, {200})
        decoded = self._decode_json(response, operation)
        values: Any = decoded
        if isinstance(decoded, Mapping):
            for key in ("data", "items", "results", "records"):
                if isinstance(decoded.get(key), list):
                    values = decoded[key]
                    break
        if not isinstance(values, list):
            self._raise_invalid_response(operation, response, "transfer history is not an array")
        return tuple(dict(value) for value in values if isinstance(value, Mapping))

    def active_downloads(self) -> tuple[MoviePilotActiveDownload, ...]:
        operation = "list-active-downloads"
        response = self._authorized_request(operation, "GET", "/api/v1/download/")
        self._expect_status(response, operation, {200})
        decoded = self._decode_json(response, operation)
        tasks = _download_task_list(decoded)
        if tasks is None:
            self._raise_invalid_response(operation, response, "download list shape is unsupported")
        downloads: list[MoviePilotActiveDownload] = []
        for index, task in enumerate(tasks):
            if not isinstance(task, Mapping):
                self._raise_invalid_response(
                    operation,
                    response,
                    f"download list item {index} is not an object",
                )
            task_hash = _download_hash(task)
            if not task_hash:
                self._raise_invalid_response(
                    operation,
                    response,
                    f"download list item {index} has no torrent hash",
                )
            media = task.get("media")
            media_info = media if isinstance(media, Mapping) else {}
            source = task.get("source")
            source_info = source if isinstance(source, Mapping) else {}
            subscribe_id = _first_positive_int(
                task.get("subscribe_id"),
                task.get("subscribeid"),
                media_info.get("subscribe_id"),
                source_info.get("id"),
            )
            tmdb_id = _first_identifier(
                task.get("tmdbid"),
                task.get("tmdb_id"),
                media_info.get("tmdbid"),
                media_info.get("tmdb_id"),
                source_info.get("tmdbid"),
                source_info.get("tmdb_id"),
            )
            season = _first_positive_int(task.get("season"), media_info.get("season"), source_info.get("season"))
            if subscribe_id is None and not tmdb_id:
                self._raise_invalid_response(
                    operation,
                    response,
                    f"download list item {index} has no subscription or media identity",
                )
            if all(value.task_hash != task_hash for value in downloads):
                downloads.append(
                    _download_projection(
                        task,
                        task_hash=task_hash,
                        subscribe_id=subscribe_id,
                        tmdb_id=tmdb_id,
                        season=season,
                        moviepilot_active=True,
                    )
                )
        return tuple(downloads)

    def monitor_downloads(self) -> MoviePilotDownloadSnapshot:
        """Merge MoviePilot-active downloads with an all-torrent downloader snapshot.

        The MoviePilot download endpoint omits completed/seeding torrents.  Stale
        monitor rows may therefore be removed only when every configured downloader
        was read successfully.  A partial snapshot is still useful for progress and
        registration, but is explicitly non-authoritative for absence decisions.
        """
        active = self.active_downloads()
        try:
            raw_configs = self.get_system_setting("Downloaders")
        except MoviePilotProviderError:
            return MoviePilotDownloadSnapshot(active, False)
        if not isinstance(raw_configs, list):
            return MoviePilotDownloadSnapshot(active, False)

        complete = True
        configured = 0
        all_downloads: list[MoviePilotActiveDownload] = []
        for raw in raw_configs:
            if not isinstance(raw, Mapping) or not _downloader_enabled(raw):
                continue
            kind = str(raw.get("type") or "").strip().casefold()
            if kind in {"qbittorrent", "qb", "qbit"}:
                configured += 1
                values = self._qbittorrent_downloads(raw)
            elif kind in {"transmission", "tr"}:
                configured += 1
                values = self._transmission_downloads(raw)
            else:
                complete = False
                continue
            if values is None:
                complete = False
                continue
            all_downloads.extend(values)

        if configured == 0:
            complete = False
        return MoviePilotDownloadSnapshot(_merge_download_projections(all_downloads, active), complete)

    def _qbittorrent_downloads(
        self,
        configuration: Mapping[str, Any],
    ) -> tuple[MoviePilotActiveDownload, ...] | None:
        base_url = _downloader_base_url(configuration)
        if not base_url:
            return None
        username = str(_downloader_value(configuration, "username", "user") or "")
        password = str(_downloader_value(configuration, "password", "passwd", "pass") or "")
        api_key = str(_downloader_value(configuration, "apikey", "api_key", "apiKey", "token") or "").strip()
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Origin": base_url,
            "Referer": f"{base_url}/",
        }
        if api_key and api_key not in {"******", "********", "**********"}:
            headers["Authorization"] = f"Bearer {api_key}"
        try:
            with self._client_factory() as client:
                if "Authorization" not in headers and (username or password):
                    login = client.post(
                        f"{base_url}/api/v2/auth/login",
                        data={"username": username, "password": password},
                        headers=headers,
                        timeout=self._timeout_seconds,
                    )
                    if login.status_code not in {200, 204} or "fails" in login.text.casefold():
                        return None
                response = client.get(
                    f"{base_url}/api/v2/torrents/info",
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        try:
            values = response.json()
        except ValueError:
            return None
        if not isinstance(values, list):
            return None
        return tuple(
            _download_projection(value, moviepilot_active=False)
            for value in values
            if isinstance(value, Mapping) and _download_hash(value)
        )

    def _transmission_downloads(
        self,
        configuration: Mapping[str, Any],
    ) -> tuple[MoviePilotActiveDownload, ...] | None:
        base_url = _downloader_base_url(configuration)
        if not base_url:
            return None
        rpc_url = base_url if base_url.endswith("/transmission/rpc") else f"{base_url}/transmission/rpc"
        username = str(_downloader_value(configuration, "username", "user") or "")
        password = str(_downloader_value(configuration, "password", "passwd", "pass") or "")
        auth = httpx.BasicAuth(username, password) if username or password else None
        payload = {
            "method": "torrent-get",
            "arguments": {
                "fields": [
                    "id",
                    "hashString",
                    "name",
                    "percentDone",
                    "status",
                    "labels",
                    "addedDate",
                ]
            },
        }
        try:
            with self._client_factory() as client:
                response = (
                    client.post(
                        rpc_url,
                        json=payload,
                        auth=auth,
                        timeout=self._timeout_seconds,
                    )
                    if auth is not None
                    else client.post(
                        rpc_url,
                        json=payload,
                        timeout=self._timeout_seconds,
                    )
                )
                if response.status_code == 409:
                    session_id = response.headers.get("X-Transmission-Session-Id")
                    if not session_id:
                        return None
                    response = (
                        client.post(
                            rpc_url,
                            json=payload,
                            headers={"X-Transmission-Session-Id": session_id},
                            auth=auth,
                            timeout=self._timeout_seconds,
                        )
                        if auth is not None
                        else client.post(
                            rpc_url,
                            json=payload,
                            headers={"X-Transmission-Session-Id": session_id},
                            timeout=self._timeout_seconds,
                        )
                    )
        except httpx.HTTPError:
            return None
        if response.status_code != 200:
            return None
        try:
            decoded = response.json()
        except ValueError:
            return None
        arguments = decoded.get("arguments") if isinstance(decoded, Mapping) else None
        values = arguments.get("torrents") if isinstance(arguments, Mapping) else None
        if not isinstance(values, list):
            return None
        return tuple(
            _download_projection(value, moviepilot_active=False)
            for value in values
            if isinstance(value, Mapping) and _download_hash(value)
        )

    def query_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotQueryResult:
        operation = "query-subscription"
        response = self._authorized_request(
            operation,
            "GET",
            self._media_identity_path(tmdb_id),
            params=self._season_params(season),
        )
        if response.status_code == 404:
            return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)
        self._expect_status(response, operation, {200})
        subscription = self._decode_mapping(response, operation)
        if not subscription:
            return MoviePilotQueryResult(MoviePilotQueryDisposition.EMPTY, None)
        self._subscription_id(subscription, operation, response)
        return MoviePilotQueryResult(MoviePilotQueryDisposition.FOUND, subscription)

    def create_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        operation = "create-subscription"
        response = self._authorized_request(
            operation,
            "POST",
            "/api/v1/subscribe/",
            json_body=self._payload(payload),
            write=True,
        )
        return self._confirmed_write(response, operation, _CREATE_STATUS_CODES)

    def update_subscription(self, payload: Mapping[str, Any]) -> MoviePilotWriteResult:
        operation = "update-subscription"
        response = self._authorized_request(
            operation,
            "PUT",
            "/api/v1/subscribe/",
            json_body=self._payload(payload),
            write=True,
        )
        return self._confirmed_write(response, operation, _WRITE_STATUS_CODES)

    def update_subscription_status(
        self,
        tmdb_id: int | str,
        status: str,
        *,
        season: int | None = None,
        total_episodes: int | None = None,
    ) -> MoviePilotWriteResult:
        operation = "update-subscription-status"
        normalized_status = str(status or "").strip().upper()
        if normalized_status not in _STATUS_STATES:
            raise ValueError("MoviePilot subscription status must be one of P, R, or S")
        normalized_total = self._optional_non_negative_integer(total_episodes, "total_episodes")

        lookup_response = self._authorized_request(
            operation,
            "GET",
            self._media_identity_path(tmdb_id),
            params=self._season_params(season),
        )
        if lookup_response.status_code == 404:
            self._raise_not_found(operation, lookup_response, write=True)
        self._expect_status(lookup_response, operation, {200}, write=True)
        subscription = self._decode_mapping(
            lookup_response,
            operation,
            outcome=MoviePilotWriteOutcome.REJECTED,
        )
        if not subscription:
            self._raise_not_found(operation, lookup_response, write=True)
        subscription_id = self._subscription_id(subscription, operation, lookup_response)

        status_response = self._authorized_request(
            operation,
            "PUT",
            f"/api/v1/subscribe/status/{quote(subscription_id, safe='')}",
            params={"state": normalized_status},
            write=True,
        )
        self._expect_status(status_response, operation, _WRITE_STATUS_CODES, write=True)
        if normalized_total is None:
            return MoviePilotWriteResult(operation, status_response.status_code)

        detail_response = self._authorized_request(
            operation,
            "GET",
            f"/api/v1/subscribe/{quote(subscription_id, safe='')}",
            write=True,
        )
        if detail_response.status_code == 404:
            self._raise_not_found(
                operation,
                detail_response,
                outcome=MoviePilotWriteOutcome.INDETERMINATE,
            )
        self._expect_status(
            detail_response,
            operation,
            {200},
            write=True,
            outcome=MoviePilotWriteOutcome.INDETERMINATE,
        )
        detail = self._decode_mapping(
            detail_response,
            operation,
            outcome=MoviePilotWriteOutcome.INDETERMINATE,
        )
        old_total = self._response_integer(detail, "total_episode", operation, detail_response, default=0)
        if old_total == normalized_total:
            return MoviePilotWriteResult(operation, status_response.status_code)

        updated_detail = dict(detail)
        updated_detail["total_episode"] = normalized_total
        if old_total > normalized_total:
            old_lack = self._response_integer(detail, "lack_episode", operation, detail_response, default=0)
            updated_detail["lack_episode"] = max(0, old_lack - (old_total - normalized_total))

        update_response = self._authorized_request(
            operation,
            "PUT",
            "/api/v1/subscribe/",
            json_body=updated_detail,
            write=True,
        )
        self._expect_status(
            update_response,
            operation,
            _WRITE_STATUS_CODES,
            write=True,
            outcome=MoviePilotWriteOutcome.INDETERMINATE,
        )
        return MoviePilotWriteResult(operation, update_response.status_code)

    def delete_subscription(
        self,
        tmdb_id: int | str,
        *,
        season: int | None = None,
    ) -> MoviePilotWriteResult:
        operation = "delete-subscription"
        response = self._authorized_request(
            operation,
            "DELETE",
            self._media_identity_path(tmdb_id),
            params=self._season_params(season),
            write=True,
        )
        if response.status_code == 404:
            return MoviePilotWriteResult(operation, 404, idempotent=True)
        return self._confirmed_write(response, operation, _WRITE_STATUS_CODES)

    def delete_subscription_by_id(self, subscription_id: int | str) -> MoviePilotWriteResult:
        operation = "delete-subscription-by-id"
        normalized_id = self._identifier(subscription_id, "MoviePilot subscription id")
        response = self._authorized_request(
            operation,
            "DELETE",
            f"/api/v1/subscribe/{quote(normalized_id, safe='')}",
            write=True,
        )
        if response.status_code == 404:
            return MoviePilotWriteResult(operation, 404, idempotent=True)
        return self._confirmed_write(response, operation, _WRITE_STATUS_CODES)

    def search_subscription(self, subscription_id: int | str) -> MoviePilotWriteResult:
        operation = "search-subscription"
        normalized_id = self._identifier(subscription_id, "MoviePilot subscription id")
        response = self._authorized_request(
            operation,
            "GET",
            f"/api/v1/subscribe/search/{quote(normalized_id, safe='')}",
            write=True,
        )
        return self._confirmed_write(response, operation, frozenset({200}))

    def delete_download_tasks(self, hashes: Sequence[str]) -> MoviePilotWriteResult:
        operation = "delete-download-tasks"
        normalized = tuple(dict.fromkeys(str(value or "").strip() for value in hashes if str(value or "").strip()))
        if not normalized:
            raise ValueError("MoviePilot download hashes must not be empty")
        last_status = 404
        all_absent = True
        for task_hash in normalized:
            response = self._authorized_request(
                operation,
                "DELETE",
                f"/api/v1/download/{quote(task_hash, safe='')}",
                write=True,
            )
            if response.status_code == 404:
                continue
            self._expect_status(response, operation, _WRITE_STATUS_CODES, write=True)
            last_status = response.status_code
            all_absent = False
        return MoviePilotWriteResult(operation, last_status, idempotent=all_absent)

    def delete_transfer_history(self, record: Mapping[str, Any]) -> MoviePilotWriteResult:
        operation = "delete-transfer-history"
        if not record:
            raise ValueError("MoviePilot transfer history record must not be empty")
        response = self._authorized_request(
            operation,
            "DELETE",
            "/api/v1/history/transfer",
            params={"deletesrc": "false", "deletedest": "false"},
            json_body=dict(record),
            write=True,
        )
        if response.status_code == 404:
            return MoviePilotWriteResult(operation, 404, idempotent=True)
        return self._confirmed_write(response, operation, frozenset({200, 204}))

    def redo_transfer_history(self, history_ids: Sequence[int]) -> MoviePilotWriteResult:
        operation = "redo-transfer-history"
        normalized = tuple(dict.fromkeys(int(value) for value in history_ids if int(value) > 0))
        if not normalized:
            raise ValueError("MoviePilot transfer history ids must not be empty")
        response = self._authorized_request(
            operation,
            "POST",
            "/api/v1/history/transfer/ai-redo",
            json_body={"history_ids": list(normalized)},
            write=True,
        )
        return self._confirmed_write(response, operation, frozenset({200, 201, 202}))

    def get_system_setting(self, key: str) -> Any:
        return self._read_system_setting(key)

    def set_system_setting(self, key: str, value: Any) -> MoviePilotWriteResult:
        normalized_key = self._system_setting_key(key)
        operation = f"set-system-setting:{normalized_key}"
        response = self._authorized_request(
            operation,
            "POST",
            f"/api/v1/system/setting/{quote(normalized_key, safe='')}",
            json_body=value,
            write=True,
        )
        self._expect_status(response, operation, frozenset({200, 201, 204}), write=True)
        confirmed = self._read_system_setting(
            normalized_key,
            operation=operation,
            outcome=MoviePilotWriteOutcome.INDETERMINATE,
        )
        if confirmed != value:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.INVALID_RESPONSE,
                operation,
                f"MoviePilot {operation} readback did not match the requested value",
                status_code=response.status_code,
                outcome=MoviePilotWriteOutcome.INDETERMINATE,
            )
        return MoviePilotWriteResult(operation, response.status_code)

    def _read_system_setting(
        self,
        key: str,
        *,
        operation: str | None = None,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> Any:
        normalized_key = self._system_setting_key(key)
        resolved_operation = operation or f"get-system-setting:{normalized_key}"
        response = self._authorized_request(
            resolved_operation,
            "GET",
            f"/api/v1/system/setting/{quote(normalized_key, safe='')}",
            write=outcome is not None,
        )
        self._expect_status(
            response,
            resolved_operation,
            {200},
            write=outcome is not None,
            outcome=outcome,
        )
        try:
            decoded = response.json()
        except ValueError as exc:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.INVALID_RESPONSE,
                resolved_operation,
                f"MoviePilot {resolved_operation} returned invalid JSON",
                status_code=response.status_code,
                outcome=outcome,
            ) from exc
        value = self._system_setting_value(decoded)
        if value is _MISSING:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.INVALID_RESPONSE,
                resolved_operation,
                f"MoviePilot {resolved_operation} returned no setting value",
                status_code=response.status_code,
                outcome=outcome,
            )
        return value

    def _authorized_request(
        self,
        operation: str,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int] | None = None,
        json_body: Any = None,
        write: bool = False,
    ) -> httpx.Response:
        access_token = self._access_token()
        response = self._send(
            operation,
            method,
            path,
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
            json_body=json_body,
            write=write,
        )
        if response.status_code != 401:
            return response

        self._discard_access_token(access_token)
        try:
            refreshed_token = self._access_token()
        except MoviePilotProviderError as exc:
            if write and exc.kind is MoviePilotProviderErrorKind.AUTHENTICATION:
                raise MoviePilotProviderError(
                    exc.kind,
                    operation,
                    str(exc),
                    status_code=exc.status_code,
                    outcome=MoviePilotWriteOutcome.REJECTED,
                ) from exc
            raise
        response = self._send(
            operation,
            method,
            path,
            headers={"Authorization": f"Bearer {refreshed_token}"},
            params=params,
            json_body=json_body,
            write=write,
        )
        if response.status_code == 401:
            self._discard_access_token(refreshed_token)
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.AUTHENTICATION,
                operation,
                "MoviePilot rejected the refreshed access token with HTTP 401",
                status_code=401,
                outcome=MoviePilotWriteOutcome.REJECTED if write else None,
            )
        return response

    def _access_token(self) -> str:
        with self._token_lock:
            if self._cached_access_token is not None:
                return self._cached_access_token
            access_token = self._login()
            self._cached_access_token = access_token
            return access_token

    def _discard_access_token(self, rejected_token: str) -> None:
        with self._token_lock:
            if self._cached_access_token == rejected_token:
                self._cached_access_token = None

    def _login(self) -> str:
        operation = "authenticate"
        response = self._send(
            operation,
            "POST",
            "/api/v1/login/access-token",
            data={"username": self._username, "password": self._password},
        )
        self._expect_status(response, operation, {200})
        decoded = self._decode_mapping(response, operation)
        access_token = decoded.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            self._raise_invalid_response(operation, response, "access_token is missing or invalid")
        return access_token.strip()

    def _send(
        self,
        operation: str,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str | int] | None = None,
        data: Mapping[str, str] | None = None,
        json_body: Any = None,
        write: bool = False,
    ) -> httpx.Response:
        try:
            with self._client_factory() as client:
                return client.request(
                    method,
                    f"{self._base_url}{path}",
                    headers=dict(headers or {}),
                    params=dict(params or {}),
                    data=dict(data) if data is not None else None,
                    json=json_body,
                    timeout=self._timeout_seconds,
                )
        except httpx.TimeoutException as exc:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.TIMEOUT,
                operation,
                f"MoviePilot {operation} request timed out",
                outcome=MoviePilotWriteOutcome.INDETERMINATE if write else None,
            ) from exc
        except httpx.TransportError as exc:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.TRANSPORT,
                operation,
                f"MoviePilot {operation} transport failed",
                outcome=MoviePilotWriteOutcome.INDETERMINATE if write else None,
            ) from exc

    def _confirmed_write(
        self,
        response: httpx.Response,
        operation: str,
        accepted_status_codes: frozenset[int],
    ) -> MoviePilotWriteResult:
        self._expect_status(response, operation, accepted_status_codes, write=True)
        return MoviePilotWriteResult(operation, response.status_code)

    def _expect_status(
        self,
        response: httpx.Response,
        operation: str,
        accepted_status_codes: set[int] | frozenset[int],
        *,
        write: bool = False,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> None:
        if response.status_code in accepted_status_codes:
            return
        self._raise_status_error(response, operation, write=write, outcome=outcome)

    def _raise_status_error(
        self,
        response: httpx.Response,
        operation: str,
        *,
        write: bool,
        outcome: MoviePilotWriteOutcome | None,
    ) -> NoReturn:
        status_code = response.status_code
        if status_code == 401:
            kind = MoviePilotProviderErrorKind.AUTHENTICATION
        elif status_code == 404:
            kind = MoviePilotProviderErrorKind.NOT_FOUND
        elif status_code == 409:
            kind = MoviePilotProviderErrorKind.CONFLICT
        elif status_code == 429:
            kind = MoviePilotProviderErrorKind.RATE_LIMITED
        elif status_code >= 500:
            kind = MoviePilotProviderErrorKind.SERVER
        elif 200 <= status_code < 300:
            kind = MoviePilotProviderErrorKind.INVALID_RESPONSE
        else:
            kind = MoviePilotProviderErrorKind.REJECTED

        resolved_outcome = outcome
        if resolved_outcome is None and write:
            if kind in {MoviePilotProviderErrorKind.SERVER, MoviePilotProviderErrorKind.INVALID_RESPONSE}:
                resolved_outcome = MoviePilotWriteOutcome.INDETERMINATE
            else:
                resolved_outcome = MoviePilotWriteOutcome.REJECTED
        raise MoviePilotProviderError(
            kind,
            operation,
            f"MoviePilot {operation} returned HTTP {status_code}",
            status_code=status_code,
            outcome=resolved_outcome,
        )

    def _raise_not_found(
        self,
        operation: str,
        response: httpx.Response,
        *,
        write: bool = False,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> NoReturn:
        raise MoviePilotProviderError(
            MoviePilotProviderErrorKind.NOT_FOUND,
            operation,
            f"MoviePilot {operation} found no matching subscription",
            status_code=response.status_code,
            outcome=outcome or (MoviePilotWriteOutcome.REJECTED if write else None),
        )

    def _decode_json(self, response: httpx.Response, operation: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.INVALID_RESPONSE,
                operation,
                f"MoviePilot {operation} returned invalid JSON",
                status_code=response.status_code,
            ) from exc

    def _decode_mapping(
        self,
        response: httpx.Response,
        operation: str,
        *,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> dict[str, Any]:
        try:
            decoded = response.json()
        except ValueError as exc:
            raise MoviePilotProviderError(
                MoviePilotProviderErrorKind.INVALID_RESPONSE,
                operation,
                f"MoviePilot {operation} returned invalid JSON",
                status_code=response.status_code,
                outcome=outcome,
            ) from exc
        if not isinstance(decoded, Mapping):
            self._raise_invalid_response(
                operation,
                response,
                "response is not an object",
                outcome=outcome,
            )
        return dict(decoded)

    def _raise_invalid_response(
        self,
        operation: str,
        response: httpx.Response,
        reason: str,
        *,
        outcome: MoviePilotWriteOutcome | None = None,
    ) -> NoReturn:
        raise MoviePilotProviderError(
            MoviePilotProviderErrorKind.INVALID_RESPONSE,
            operation,
            f"MoviePilot {operation} returned an invalid response: {reason}",
            status_code=response.status_code,
            outcome=outcome,
        )

    def _subscription_id(
        self,
        subscription: Mapping[str, Any],
        operation: str,
        response: httpx.Response,
    ) -> str:
        try:
            return self._identifier(subscription.get("id"), "MoviePilot subscription id")
        except ValueError as exc:
            self._raise_invalid_response(operation, response, str(exc))

    def _response_integer(
        self,
        payload: Mapping[str, Any],
        field: str,
        operation: str,
        response: httpx.Response,
        *,
        default: int,
    ) -> int:
        value = payload.get(field, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            self._raise_invalid_response(
                operation,
                response,
                f"{field} is not a non-negative integer",
                outcome=MoviePilotWriteOutcome.INDETERMINATE,
            )
        return value

    def _media_identity_path(self, tmdb_id: int | str) -> str:
        normalized_tmdb_id = self._identifier(tmdb_id, "TMDb id")
        media_identity = quote(f"tmdb:{normalized_tmdb_id}", safe=":")
        return f"/api/v1/subscribe/media/{media_identity}"

    @staticmethod
    def _system_setting_key(value: Any) -> str:
        normalized = str(value or "").strip()
        if not normalized or not all(character.isalnum() or character == "_" for character in normalized):
            raise ValueError("MoviePilot system setting key is invalid")
        return normalized

    @staticmethod
    def _system_setting_value(value: Any) -> Any:
        if not isinstance(value, Mapping):
            return _MISSING
        data = value.get("data", _MISSING)
        if isinstance(data, Mapping):
            for key in ("value", "data", "setting"):
                if key in data:
                    return data[key]
        if data is not _MISSING:
            return data
        for key in ("value", "setting"):
            if key in value:
                return value[key]
        return _MISSING

    def _season_params(self, season: int | None) -> Mapping[str, int] | None:
        normalized_season = self._optional_non_negative_integer(season, "season")
        return None if normalized_season is None else {"season": normalized_season}

    @staticmethod
    def _payload(payload: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(payload, Mapping):
            raise ValueError("MoviePilot subscription payload must be an object")
        return dict(payload)

    @staticmethod
    def _identifier(value: Any, label: str) -> str:
        if value is None or isinstance(value, bool):
            raise ValueError(f"{label} is required")
        normalized = str(value).strip()
        if not normalized:
            raise ValueError(f"{label} is required")
        return normalized

    @staticmethod
    def _optional_non_negative_integer(value: int | None, label: str) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{label} must be a non-negative integer")
        return value

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=self._timeout_seconds, trust_env=False)


def _download_task_list(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return value
    if not isinstance(value, Mapping):
        return None
    if not value:
        return None
    for key in ("data", "list", "items", "results", "records"):
        nested = value.get(key)
        if isinstance(nested, list):
            return nested
        if isinstance(nested, Mapping):
            tasks = _download_task_list(nested)
            if tasks is not None:
                return tasks
    return None


def _download_hash(value: Mapping[str, Any]) -> str:
    for key in (
        "hash",
        "hashString",
        "hash_string",
        "info_hash",
        "infoHash",
        "download_hash",
        "downloadHash",
        "torrent_hash",
        "torrentHash",
    ):
        raw = value.get(key)
        if not isinstance(raw, str):
            continue
        candidate = raw.strip().casefold()
        if candidate:
            return candidate
    return ""


def _download_projection(
    value: Mapping[str, Any],
    *,
    task_hash: str | None = None,
    subscribe_id: int | None = None,
    tmdb_id: str | None = None,
    season: int | None = None,
    moviepilot_active: bool,
) -> MoviePilotActiveDownload:
    media = value.get("media")
    media_info = media if isinstance(media, Mapping) else {}
    source = value.get("source")
    source_info = source if isinstance(source, Mapping) else {}
    normalized_hash = str(task_hash or _download_hash(value)).strip().casefold()
    normalized_subscribe_id = subscribe_id or _first_positive_int(
        value.get("subscribe_id"),
        value.get("subscribeid"),
        media_info.get("subscribe_id"),
        source_info.get("id"),
    )
    normalized_tmdb_id = tmdb_id or _first_identifier(
        value.get("tmdbid"),
        value.get("tmdb_id"),
        media_info.get("tmdbid"),
        media_info.get("tmdb_id"),
        source_info.get("tmdbid"),
        source_info.get("tmdb_id"),
    )
    normalized_season = season or _first_positive_int(
        value.get("season"),
        media_info.get("season"),
        source_info.get("season"),
    )
    return MoviePilotActiveDownload(
        task_hash=normalized_hash,
        subscribe_id=normalized_subscribe_id,
        tmdb_id=normalized_tmdb_id,
        season=normalized_season,
        progress=_download_progress(value),
        state=_first_scalar_text(value, "state", "status", "status_text"),
        tags=_nested_text_values(value, {"tag", "tags", "label", "labels", "category", "categories"}),
        tracker_text="\n".join(
            _nested_text_values(
                value,
                {
                    "tracker",
                    "trackers",
                    "tracker_status",
                    "trackerstatus",
                    "message",
                    "msg",
                    "error",
                    "errmsg",
                    "reason",
                },
            )
        )[:4000],
        added_at=_first_positive_int(value.get("added_on"), value.get("addedDate"), value.get("time")),
        title=_first_scalar_text(value, "name", "title", "torrent_name", "torrentName")[:500],
        episode=_first_positive_int(value.get("episode"), media_info.get("episode")),
        moviepilot_active=moviepilot_active,
    )


def _merge_download_projections(
    complete: Sequence[MoviePilotActiveDownload],
    active: Sequence[MoviePilotActiveDownload],
) -> tuple[MoviePilotActiveDownload, ...]:
    merged: dict[str, MoviePilotActiveDownload] = {}
    for value in (*complete, *active):
        existing = merged.get(value.task_hash)
        if existing is None:
            merged[value.task_hash] = value
            continue
        tracker_parts = tuple(dict.fromkeys(part for part in (existing.tracker_text, value.tracker_text) if part))
        merged[value.task_hash] = MoviePilotActiveDownload(
            task_hash=value.task_hash,
            subscribe_id=value.subscribe_id or existing.subscribe_id,
            tmdb_id=value.tmdb_id or existing.tmdb_id,
            season=value.season or existing.season,
            progress=value.progress if value.progress is not None else existing.progress,
            state=value.state or existing.state,
            tags=tuple(dict.fromkeys((*existing.tags, *value.tags))),
            tracker_text="\n".join(tracker_parts)[:4000],
            added_at=value.added_at or existing.added_at,
            title=value.title or existing.title,
            episode=value.episode or existing.episode,
            moviepilot_active=existing.moviepilot_active or value.moviepilot_active,
        )
    return tuple(merged.values())


def _download_progress(value: Mapping[str, Any]) -> float | None:
    for key in ("progress", "percent", "completed", "percentDone"):
        raw = value.get(key)
        if raw is None or isinstance(raw, bool):
            continue
        try:
            progress = float(raw)
        except (TypeError, ValueError):
            continue
        return progress * 100 if 0 <= progress <= 1 else progress
    return None


def _first_scalar_text(value: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        raw = value.get(key)
        if isinstance(raw, (str, int, float)) and not isinstance(raw, bool):
            normalized = str(raw).strip()
            if normalized:
                return normalized
    return ""


def _nested_text_values(
    value: Any,
    keys: set[str],
    *,
    depth: int = 0,
    capture: bool = False,
) -> tuple[str, ...]:
    if depth > 4:
        return ()
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if str(key).casefold() in keys:
                found.extend(_nested_text_values(nested, keys, depth=depth + 1, capture=True))
            elif isinstance(nested, (Mapping, list, tuple)):
                found.extend(_nested_text_values(nested, keys, depth=depth + 1, capture=False))
    elif isinstance(value, (list, tuple)):
        for nested in value[:100]:
            found.extend(_nested_text_values(nested, keys, depth=depth + 1, capture=capture))
    elif capture and isinstance(value, (str, int, float)) and not isinstance(value, bool):
        normalized = str(value).strip()
        if normalized:
            found.append(normalized[:500])
    return tuple(dict.fromkeys(found[:100]))


def _downloader_enabled(value: Mapping[str, Any]) -> bool:
    raw = value.get("enabled", True)
    if isinstance(raw, str):
        return raw.strip().casefold() not in {"0", "false", "no", "off", "禁用", "关闭"}
    return bool(raw)


def _downloader_value(value: Mapping[str, Any], *keys: str) -> Any:
    nested = value.get("config")
    configuration = nested if isinstance(nested, Mapping) else value
    for key in keys:
        raw = configuration.get(key)
        if raw not in (None, ""):
            return raw
        raw = value.get(key)
        if raw not in (None, ""):
            return raw
    return None


def _downloader_base_url(value: Mapping[str, Any]) -> str:
    raw_host = str(_downloader_value(value, "host", "url", "server", "address", "domain") or "").strip()
    if not raw_host:
        return ""
    ssl_value = _downloader_value(value, "ssl", "https")
    use_tls = (
        ssl_value.strip().casefold() in {"1", "true", "yes", "on"} if isinstance(ssl_value, str) else bool(ssl_value)
    )
    candidate = raw_host if "://" in raw_host else f"{'https' if use_tls else 'http'}://{raw_host}"
    try:
        parsed = urlparse(candidate)
        if not parsed.hostname:
            return ""
        port = _first_positive_int(_downloader_value(value, "port"))
        netloc = parsed.netloc
        if port is not None and parsed.port is None:
            host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
            netloc = f"{host}:{port}"
        return urlunparse((parsed.scheme, netloc, parsed.path.rstrip("/"), "", "", "")).rstrip("/")
    except ValueError:
        return ""


def _optional_positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    if isinstance(value, str) and not value.strip().isdigit():
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _first_positive_int(*values: Any) -> int | None:
    for value in values:
        parsed = _optional_positive_int(value)
        if parsed is not None:
            return parsed
    return None


def _first_identifier(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (int, str)):
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


__all__ = [
    "HttpxMoviePilotGateway",
    "MoviePilotActiveDownload",
    "MoviePilotDownloadSnapshot",
    "MoviePilotListResult",
    "MoviePilotProviderError",
    "MoviePilotProviderErrorKind",
    "MoviePilotQueryDisposition",
    "MoviePilotQueryResult",
    "MoviePilotWriteOutcome",
    "MoviePilotWriteResult",
]
