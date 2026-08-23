"""Identity, setup authorization, and compatible session contracts."""

from .compatibility import LegacyEmbyIdentityGateway, RuntimeConfigurationWriter
from .post_authorization import PersistentPostAuthorizationSubmitter
from .repository import SqlAlchemyIdentityRepository
from .router import router
from .service import IdentityService, RecoveryTokenStore

__all__ = [
    "IdentityService",
    "LegacyEmbyIdentityGateway",
    "PersistentPostAuthorizationSubmitter",
    "RecoveryTokenStore",
    "RuntimeConfigurationWriter",
    "SqlAlchemyIdentityRepository",
    "router",
]
