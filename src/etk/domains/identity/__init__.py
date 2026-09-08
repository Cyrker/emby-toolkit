"""Identity, setup authorization, and compatible session contracts."""

from .adapters import HttpxEmbyIdentityGateway, RuntimeConfigurationWriter
from .collection_projection import SqlAlchemyCollectionUserProjection
from .post_authorization import PersistentPostAuthorizationSubmitter
from .repository import SqlAlchemyIdentityRepository
from .router import router
from .service import IdentityService, RecoveryTokenStore

__all__ = [
    "IdentityService",
    "HttpxEmbyIdentityGateway",
    "PersistentPostAuthorizationSubmitter",
    "RecoveryTokenStore",
    "RuntimeConfigurationWriter",
    "SqlAlchemyCollectionUserProjection",
    "SqlAlchemyIdentityRepository",
    "router",
]
