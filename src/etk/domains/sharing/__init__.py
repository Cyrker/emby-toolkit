"""P9 sharing domain: durable contribution ledger and external-effects seams."""

from .execution import (
    SharingDeliveryDisposition,
    SharingDeliveryOutcome,
    SharingOperationDeliveryPort,
    SharingOperationExecution,
    SharingOperationExecutor,
)
from .intro_effects import SHARED_INTRO_EXPORT_EFFECT, SharedIntroExportEffectDelivery
from .ledger import (
    ContributionLedger,
    ContributionLedgerEntry,
    ContributionLedgerError,
    InMemoryContributionLedger,
    InsufficientContributionError,
    LedgerIdempotencyConflictError,
    PostgresContributionLedger,
)
from .operations import (
    SHARING_OPERATION_AGGREGATE_TYPE,
    SHARING_OPERATION_TOPIC,
    SharingOperation,
    SharingOperationIntent,
    SharingOperationKind,
    SharingOperationStatus,
)
from .repository import DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS, PostgresSharingOperationRepository
from .router import router
from .service import SharingLedgerService, SharingService

__all__ = [
    "ContributionLedger",
    "ContributionLedgerEntry",
    "ContributionLedgerError",
    "InMemoryContributionLedger",
    "InsufficientContributionError",
    "LedgerIdempotencyConflictError",
    "PostgresContributionLedger",
    "PostgresSharingOperationRepository",
    "SHARED_INTRO_EXPORT_EFFECT",
    "SHARING_OPERATION_AGGREGATE_TYPE",
    "SHARING_OPERATION_TOPIC",
    "DEFAULT_SHARING_OPERATION_MAX_ATTEMPTS",
    "SharingLedgerService",
    "SharingDeliveryDisposition",
    "SharingDeliveryOutcome",
    "SharingOperationDeliveryPort",
    "SharingOperationExecution",
    "SharingOperationExecutor",
    "SharedIntroExportEffectDelivery",
    "SharingService",
    "SharingOperation",
    "SharingOperationIntent",
    "SharingOperationKind",
    "SharingOperationStatus",
    "router",
]
