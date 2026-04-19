"""
Dialects package - Database-specific implementations (OCP).

This package provides pluggable database dialects following the
Open/Closed Principle (OCP).

Each dialect implements DatabaseDialect protocol and can be
registered dynamically.

Usage:
    >>> from dialects.registry import get
    >>> dialect = get("MySQL")
    >>> url = dialect.build_url("localhost", "3306", "mydb", "user", "pass")
"""
from dialects.registry import (
    register,
    get,
    available_types,
)

__all__ = [
    "register",
    "get",
    "available_types",
]
