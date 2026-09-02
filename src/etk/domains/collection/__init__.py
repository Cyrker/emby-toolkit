"""P8 collection, custom collection, and cover ownership."""

from .activation import CollectionActivationService, PostgresCollectionActivationRepository
from .custom_tasks import (
    CUSTOM_COLLECTION_BATCH_TASK_KEYS,
    CUSTOM_COLLECTION_SOURCE_TYPES,
    CustomCollectionCandidate,
    CustomCollectionRefreshPlan,
    CustomCollectionRefreshResult,
    CustomCollectionTaskService,
    ResolvedCustomCollectionItem,
)
from .local_gateway import P8LocalCollectionGateway
from .renderer import (
    CoverPreviewRenderer,
    DeterministicCoverRenderer,
    PillowCoverPreviewRenderer,
    RenderedCover,
)
from .repository import SqlAlchemyCollectionRepository
from .service import CollectionService

__all__ = [
    "CollectionActivationService",
    "CUSTOM_COLLECTION_BATCH_TASK_KEYS",
    "CUSTOM_COLLECTION_SOURCE_TYPES",
    "CustomCollectionCandidate",
    "CustomCollectionRefreshPlan",
    "CustomCollectionRefreshResult",
    "CustomCollectionTaskService",
    "CollectionService",
    "CoverPreviewRenderer",
    "DeterministicCoverRenderer",
    "P8LocalCollectionGateway",
    "PillowCoverPreviewRenderer",
    "PostgresCollectionActivationRepository",
    "RenderedCover",
    "ResolvedCustomCollectionItem",
    "SqlAlchemyCollectionRepository",
]
