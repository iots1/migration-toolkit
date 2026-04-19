"""
Dialect registry - Central registry for database dialects.

Provides dynamic dialect registration and retrieval.
"""
from typing import Dict
from dialects.base import BaseDialect

# Internal dialect storage
_dialects: Dict[str, BaseDialect] = {}


def register(dialect: BaseDialect) -> None:
    """
    Register a database dialect.

    Args:
        dialect: Dialect instance to register

    Example:
        >>> from dialects.mysql import MySQLDialect
        >>> register(MySQLDialect())
    """
    _dialects[dialect.name] = dialect


def get(name: str) -> BaseDialect:
    """
    Get a registered dialect by name.

    Args:
        name: Dialect name (e.g., 'MySQL', 'PostgreSQL', 'Microsoft SQL Server')

    Returns:
        BaseDialect: Registered dialect instance

    Raises:
        ValueError: If dialect not found

    Example:
        >>> dialect = get("MySQL")
        >>> url = dialect.build_url("localhost", "3306", ...)
    """
    if name not in _dialects:
        available = ", ".join(available_types())
        raise ValueError(
            f"Unknown database type: {name}. "
            f"Available types: {available}"
        )
    return _dialects[name]


def available_types() -> list[str]:
    """
    Get list of available dialect types.

    Returns:
        list[str]: List of dialect names

    Example:
        >>> available_types()
        ['MySQL', 'PostgreSQL', 'Microsoft SQL Server']
    """
    return list(_dialects.keys())


# Auto-register built-in dialects
def _register_builtin_dialects() -> None:
    """Register all built-in dialects on module import."""
    from dialects.mysql import MySQLDialect
    from dialects.postgresql import PostgreSQLDialect
    from dialects.mssql import MSSQLDialect

    register(MySQLDialect())
    register(PostgreSQLDialect())
    register(MSSQLDialect())


# Register built-in dialects on import
_register_builtin_dialects()
