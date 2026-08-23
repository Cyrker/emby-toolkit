from __future__ import annotations


class PersistentExecutionControlError(RuntimeError):
    """Base class for a cooperative persistent-execution stop at a safe point."""


class PersistentExecutionCancelled(PersistentExecutionControlError):
    pass


class PersistentExecutionTimedOut(PersistentExecutionControlError, TimeoutError):
    pass


class PersistentExecutionInterruptedAfterEffect(PersistentExecutionControlError):
    """A stop was observed after a confirmed effect, so cancellation is not a truthful terminal state."""


class PersistentEffectIndeterminate(PersistentExecutionControlError):
    """An external provider may have accepted an effect that cannot yet be reconciled."""


__all__ = [
    "PersistentEffectIndeterminate",
    "PersistentExecutionCancelled",
    "PersistentExecutionControlError",
    "PersistentExecutionInterruptedAfterEffect",
    "PersistentExecutionTimedOut",
]
