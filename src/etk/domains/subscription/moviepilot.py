from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from threading import Lock
from typing import Any, NoReturn
from urllib.parse import quote

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
        return httpx.Client(timeout=self._timeout_seconds)


__all__ = [
    "HttpxMoviePilotGateway",
    "MoviePilotListResult",
    "MoviePilotProviderError",
    "MoviePilotProviderErrorKind",
    "MoviePilotQueryDisposition",
    "MoviePilotQueryResult",
    "MoviePilotWriteOutcome",
    "MoviePilotWriteResult",
]
