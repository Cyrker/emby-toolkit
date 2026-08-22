"""Shared platform services for the plaintext ETK mainline."""

from .config import LegacyConfigAdapter, PostgresLegacyConfigStore, Settings
from .container import AppContainer

__all__ = ["AppContainer", "LegacyConfigAdapter", "PostgresLegacyConfigStore", "Settings"]
