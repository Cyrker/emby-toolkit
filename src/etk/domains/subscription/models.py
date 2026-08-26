from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class SeriesWatchState(StrEnum):
    NONE = "NONE"
    WATCHING = "Watching"
    PAUSED = "Paused"
    PENDING = "Pending"
    COMPLETED = "Completed"

    @classmethod
    def parse(cls, value: object) -> SeriesWatchState:
        if isinstance(value, cls):
            return value
        normalized = "" if value is None else str(value).strip()
        try:
            return cls(normalized)
        except ValueError as error:
            raise ValueError(f"unknown series watch state: {value!r}") from error


class SeasonWatchState(StrEnum):
    NONE = "NONE"
    WATCHING = "Watching"
    COMPLETED = "Completed"

    @classmethod
    def parse(cls, value: object) -> SeasonWatchState:
        if isinstance(value, cls):
            return value
        normalized = "" if value is None else str(value).strip()
        try:
            return cls(normalized)
        except ValueError as error:
            raise ValueError(f"unknown season watch state: {value!r}") from error


class MoviePilotState(StrEnum):
    NEW = "N"
    RUNNING = "R"
    PAUSED = "S"
    PENDING = "P"


class SeriesTransitionReason(StrEnum):
    FORCE_ENDED = "force_ended"
    TMDB_TERMINAL = "tmdb_terminal"
    TMDB_ACTIVE = "tmdb_active"
    REVIVAL = "revival"
    REVIVAL_REQUIRED = "revival_required"


class SeasonTransitionReason(StrEnum):
    FORCE_ENDED = "force_ended"
    LOCAL_COMPLETE = "local_complete"
    LOCAL_INCOMPLETE = "local_incomplete"


class MoviePilotDecisionReason(StrEnum):
    NEW_SUBSCRIPTION = "new_subscription"
    PAUSED = "paused"
    PENDING = "pending"
    COMPLETED_GUARD = "completed_guard"
    RUNNABLE = "runnable"


@dataclass(frozen=True, slots=True)
class SeriesTransitionResult:
    previous_state: SeriesWatchState
    target_state: SeriesWatchState
    current_state: SeriesWatchState
    reason: SeriesTransitionReason
    allowed: bool = True
    stable_terminal: bool = False

    @property
    def changed(self) -> bool:
        return self.previous_state is not self.current_state

    @property
    def newly_completed(self) -> bool:
        return (
            self.previous_state is not SeriesWatchState.COMPLETED and self.current_state is SeriesWatchState.COMPLETED
        )

    @property
    def revived(self) -> bool:
        return self.previous_state is SeriesWatchState.COMPLETED and self.current_state is SeriesWatchState.WATCHING


@dataclass(frozen=True, slots=True)
class SeasonTransitionResult:
    previous_state: SeasonWatchState
    current_state: SeasonWatchState
    reason: SeasonTransitionReason
    stable_terminal: bool = False

    @property
    def changed(self) -> bool:
        return self.previous_state is not self.current_state

    @property
    def newly_completed(self) -> bool:
        return (
            self.previous_state is not SeasonWatchState.COMPLETED and self.current_state is SeasonWatchState.COMPLETED
        )


@dataclass(frozen=True, slots=True)
class MoviePilotStateDecision:
    series_state: SeriesWatchState
    state: MoviePilotState
    reason: MoviePilotDecisionReason


class SeriesStatePolicy:
    _TMDB_TERMINAL_STATUSES = frozenset({"ended", "canceled"})

    @classmethod
    def transition(
        cls,
        current_state: SeriesWatchState | str,
        *,
        tmdb_status: object,
        force_ended: bool = False,
        revival_signal: bool = False,
    ) -> SeriesTransitionResult:
        previous = SeriesWatchState.parse(current_state)
        tmdb_terminal = str(tmdb_status or "").strip().casefold() in cls._TMDB_TERMINAL_STATUSES
        target = SeriesWatchState.COMPLETED if force_ended or tmdb_terminal else SeriesWatchState.WATCHING

        if force_ended:
            return SeriesTransitionResult(
                previous_state=previous,
                target_state=target,
                current_state=SeriesWatchState.COMPLETED,
                reason=SeriesTransitionReason.FORCE_ENDED,
                stable_terminal=True,
            )
        if tmdb_terminal:
            return SeriesTransitionResult(
                previous_state=previous,
                target_state=target,
                current_state=SeriesWatchState.COMPLETED,
                reason=SeriesTransitionReason.TMDB_TERMINAL,
            )
        if previous is SeriesWatchState.COMPLETED and not revival_signal:
            return SeriesTransitionResult(
                previous_state=previous,
                target_state=target,
                current_state=SeriesWatchState.COMPLETED,
                reason=SeriesTransitionReason.REVIVAL_REQUIRED,
                allowed=False,
            )
        if previous is SeriesWatchState.COMPLETED:
            return SeriesTransitionResult(
                previous_state=previous,
                target_state=target,
                current_state=SeriesWatchState.WATCHING,
                reason=SeriesTransitionReason.REVIVAL,
            )
        return SeriesTransitionResult(
            previous_state=previous,
            target_state=target,
            current_state=SeriesWatchState.WATCHING,
            reason=SeriesTransitionReason.TMDB_ACTIVE,
        )


class SeasonStatePolicy:
    @staticmethod
    def transition(
        current_state: SeasonWatchState | str,
        *,
        local_count: int,
        total_episodes: int,
        force_ended: bool = False,
    ) -> SeasonTransitionResult:
        previous = SeasonWatchState.parse(current_state)
        local = _non_negative_count(local_count, field="local_count")
        total = _non_negative_count(total_episodes, field="total_episodes")

        if force_ended:
            return SeasonTransitionResult(
                previous_state=previous,
                current_state=SeasonWatchState.COMPLETED,
                reason=SeasonTransitionReason.FORCE_ENDED,
                stable_terminal=True,
            )
        if total > 0 and local >= total:
            return SeasonTransitionResult(
                previous_state=previous,
                current_state=SeasonWatchState.COMPLETED,
                reason=SeasonTransitionReason.LOCAL_COMPLETE,
            )
        return SeasonTransitionResult(
            previous_state=previous,
            current_state=SeasonWatchState.WATCHING,
            reason=SeasonTransitionReason.LOCAL_INCOMPLETE,
        )


class MoviePilotStatePolicy:
    @staticmethod
    def decide(
        series_state: SeriesWatchState | str,
        *,
        completion_guard_pending: bool = False,
        new_subscription: bool = False,
    ) -> MoviePilotStateDecision:
        state = SeriesWatchState.parse(series_state)
        if state is SeriesWatchState.PAUSED:
            return MoviePilotStateDecision(state, MoviePilotState.PAUSED, MoviePilotDecisionReason.PAUSED)
        if state is SeriesWatchState.PENDING:
            return MoviePilotStateDecision(state, MoviePilotState.PENDING, MoviePilotDecisionReason.PENDING)
        if state is SeriesWatchState.COMPLETED and completion_guard_pending:
            return MoviePilotStateDecision(
                state,
                MoviePilotState.PENDING,
                MoviePilotDecisionReason.COMPLETED_GUARD,
            )
        if state is SeriesWatchState.WATCHING and new_subscription:
            return MoviePilotStateDecision(
                state,
                MoviePilotState.NEW,
                MoviePilotDecisionReason.NEW_SUBSCRIPTION,
            )
        return MoviePilotStateDecision(state, MoviePilotState.RUNNING, MoviePilotDecisionReason.RUNNABLE)


class SubscriptionSourceType(StrEnum):
    ACTOR_SUBSCRIPTION = "actor_subscription"
    MOVIEPILOT = "moviepilot"
    REVIVED_SEASON = "revived_season"
    SCAN_OLD_SEASONS_BACKFILL = "scan_old_seasons_backfill"
    RESUBSCRIBE = "resubscribe"
    GAP_SCAN = "gap_scan"


@dataclass(frozen=True, slots=True, init=False)
class SubscriptionSource:
    source_type: SubscriptionSourceType
    _metadata_json: str

    def __init__(
        self,
        source_type: SubscriptionSourceType | str,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        try:
            parsed_type = (
                source_type
                if isinstance(source_type, SubscriptionSourceType)
                else SubscriptionSourceType(str(source_type).strip())
            )
        except ValueError as error:
            raise ValueError(f"unknown subscription source type: {source_type!r}") from error

        normalized = _normalize_json({} if metadata is None else metadata)
        if not isinstance(normalized, dict):
            raise ValueError("subscription source metadata must be a JSON object")
        if "type" in normalized:
            raise ValueError("subscription source metadata cannot contain 'type'")

        object.__setattr__(self, "source_type", parsed_type)
        object.__setattr__(self, "_metadata_json", _canonical_json(normalized))

    @classmethod
    def from_raw(cls, raw: object) -> SubscriptionSource | None:
        if isinstance(raw, cls):
            return raw
        if not isinstance(raw, Mapping):
            return None
        metadata = {key: value for key, value in raw.items() if key != "type"}
        try:
            return cls(raw.get("type", ""), metadata)
        except (TypeError, ValueError):
            return None

    @property
    def metadata(self) -> dict[str, Any]:
        value = json.loads(self._metadata_json)
        if not isinstance(value, dict):
            raise AssertionError("canonical subscription source metadata must be an object")
        return dict(value)

    @property
    def canonical_key(self) -> str:
        return _canonical_json(self.to_json_value())

    def to_json_value(self) -> dict[str, Any]:
        return {"type": self.source_type.value, **self.metadata}


@dataclass(frozen=True, slots=True)
class SubscriptionSourceDiff:
    added: tuple[SubscriptionSource, ...]
    removed: tuple[SubscriptionSource, ...]
    unchanged: tuple[SubscriptionSource, ...]

    @property
    def changed(self) -> bool:
        return bool(self.added or self.removed)


@dataclass(frozen=True, slots=True, init=False)
class SubscriptionSources:
    items: tuple[SubscriptionSource, ...]

    def __init__(self, sources: Iterable[SubscriptionSource] = ()) -> None:
        by_key: dict[str, SubscriptionSource] = {}
        for source in sources:
            if not isinstance(source, SubscriptionSource):
                raise TypeError("subscription sources must contain SubscriptionSource values")
            by_key[source.canonical_key] = source
        object.__setattr__(self, "items", tuple(by_key[key] for key in sorted(by_key)))

    @classmethod
    def from_raw(cls, raw: object) -> SubscriptionSources:
        if isinstance(raw, cls):
            return raw
        if not isinstance(raw, (list, tuple)):
            return cls()
        return cls(source for item in raw if (source := SubscriptionSource.from_raw(item)) is not None)

    def __iter__(self) -> Iterator[SubscriptionSource]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def merge(self, other: SubscriptionSources) -> SubscriptionSources:
        return SubscriptionSources((*self.items, *other.items))

    def diff(self, desired: SubscriptionSources) -> SubscriptionSourceDiff:
        current_by_key = {source.canonical_key: source for source in self.items}
        desired_by_key = {source.canonical_key: source for source in desired.items}
        current_keys = set(current_by_key)
        desired_keys = set(desired_by_key)
        return SubscriptionSourceDiff(
            added=tuple(desired_by_key[key] for key in sorted(desired_keys - current_keys)),
            removed=tuple(current_by_key[key] for key in sorted(current_keys - desired_keys)),
            unchanged=tuple(current_by_key[key] for key in sorted(current_keys & desired_keys)),
        )

    def to_json_value(self) -> list[dict[str, Any]]:
        return [source.to_json_value() for source in self.items]

    def canonical_json(self) -> str:
        return _canonical_json(self.to_json_value())


def _non_negative_count(value: int, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value


def _normalize_json(value: object) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("subscription source metadata must contain finite JSON numbers")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("subscription source metadata keys must be strings")
            normalized[key] = _normalize_json(item)
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json(item) for item in value]
    raise ValueError(f"unsupported subscription source metadata value: {type(value).__name__}")


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)


__all__ = [
    "MoviePilotDecisionReason",
    "MoviePilotState",
    "MoviePilotStateDecision",
    "MoviePilotStatePolicy",
    "SeasonStatePolicy",
    "SeasonTransitionReason",
    "SeasonTransitionResult",
    "SeasonWatchState",
    "SeriesStatePolicy",
    "SeriesTransitionReason",
    "SeriesTransitionResult",
    "SeriesWatchState",
    "SubscriptionSource",
    "SubscriptionSourceDiff",
    "SubscriptionSourceType",
    "SubscriptionSources",
]
