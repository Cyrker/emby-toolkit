from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from pydantic import SecretStr


class ProviderWriteOutcome(StrEnum):
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ProviderWriteResult:
    outcome: ProviderWriteOutcome
    status_code: int | None = None


class RegistrationSecretLease:
    __slots__ = ("_value", "_closed")

    def __init__(self, secret: SecretStr) -> None:
        self._value = bytearray(secret.get_secret_value().encode("utf-8"))
        self._closed = False

    def reveal(self) -> str:
        if self._closed:
            raise RuntimeError("registration secret lease is closed")
        return self._value.decode("utf-8")

    def close(self) -> None:
        if self._closed:
            return
        for index in range(len(self._value)):
            self._value[index] = 0
        self._closed = True

    def __enter__(self) -> RegistrationSecretLease:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "RegistrationSecretLease('**********')"


class RegistrationSecretPort(Protocol):
    def lease(self, secret: SecretStr) -> RegistrationSecretLease: ...


class IdentityEmbyUserQueryPort(Protocol):
    def list_users(self) -> Sequence[Mapping[str, object]]: ...

    def get_user(self, user_id: str) -> Mapping[str, object] | None: ...

    def find_user_by_name(self, username: str) -> Mapping[str, object] | None: ...

    def verify_password(self, username: str, password: str) -> bool | None: ...


class IdentityEmbyUserCommandPort(Protocol):
    def create_user(self, username: str) -> tuple[ProviderWriteResult, str | None]: ...

    def set_password(self, user_id: str, password: str) -> ProviderWriteResult: ...

    def set_policy(self, user_id: str, policy: Mapping[str, object]) -> ProviderWriteResult: ...

    def set_configuration(
        self,
        user_id: str,
        configuration: Mapping[str, object],
    ) -> ProviderWriteResult: ...

    def delete_user(self, user_id: str) -> ProviderWriteResult: ...


class IdentityEmbyRegistrationPort(IdentityEmbyUserQueryPort, IdentityEmbyUserCommandPort, Protocol):
    pass


class IdentityEmbyMutationPort(IdentityEmbyRegistrationPort, Protocol):
    def avatar_sha256(self, user_id: str) -> bytes | None: ...

    def upload_avatar(
        self,
        user_id: str,
        content: bytes,
        content_type: str,
    ) -> ProviderWriteResult: ...


__all__ = [
    "IdentityEmbyRegistrationPort",
    "IdentityEmbyMutationPort",
    "IdentityEmbyUserCommandPort",
    "IdentityEmbyUserQueryPort",
    "ProviderWriteOutcome",
    "ProviderWriteResult",
    "RegistrationSecretLease",
    "RegistrationSecretPort",
]
