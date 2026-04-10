"""
Repositories package - Data access layer for PostgreSQL.

This package replaces the legacy `database.py` module with:
- PostgreSQL support via SQLAlchemy
- Repository pattern per domain (datasources, configs, pipelines)
- Thread-safe connection management
- UUID native type support
"""
from repositories.connection import (
    get_engine,
    dispose_engine,
    get_connection,
    get_transaction,
    test_connection,
)

__all__ = [
    "get_engine",
    "dispose_engine",
    "get_connection",
    "get_transaction",
    "test_connection",
]
