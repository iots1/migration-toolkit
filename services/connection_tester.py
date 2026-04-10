"""
connection_tester.py — Database connection testing service.

Extracted from db_connector.py in Phase 9 (SRP - Single Responsibility Principle).
Provides functionality to test database connections before using them.
"""
from __future__ import annotations

from services.connection_pool import _connection_pool


def test_db_connection(db_type: str, host: str, port: str, db_name: str, user: str, password: str) -> tuple[bool, str]:
    """
    Test connection to external database sources.

    Args:
        db_type: Database type ("MySQL", "PostgreSQL", "Microsoft SQL Server")
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        cursor.close()
        return True, f"Successfully connected to {db_type}!"
    except Exception as e:
        return False, str(e)
