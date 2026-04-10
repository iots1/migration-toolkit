"""
PostgreSQL connection management with thread-safe context managers.

This module provides:
- Engine singleton (thread-safe)
- Connection context manager (per-thread connections)
- Transaction context manager (auto-commit/rollback)

Usage:
    # Simple connection
    with get_connection() as conn:
        result = conn.execute(text("SELECT * FROM datasources"))

    # Transaction (auto-commit)
    with get_transaction() as conn:
        conn.execute(text("INSERT INTO datasources ..."))
        # Commits on success, rolls back on exception
"""
from contextlib import contextmanager
from typing import Generator, Any
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.pool import QueuePool
from config import get_database_url

_engine: Engine | None = None


def get_engine() -> Engine:
    """
    Get or create SQLAlchemy Engine singleton.

    Engine is thread-safe and uses connection pooling.
    Call this function instead of creating engines directly.

    Returns:
        Engine: SQLAlchemy engine with connection pooling

    Example:
        >>> engine = get_engine()
        >>> with engine.connect() as conn:
        ...     df = pd.read_sql("SELECT * FROM datasources", conn)
    """
    global _engine
    if _engine is None:
        url = get_database_url()
        _engine = create_engine(
            url,
            poolclass=QueuePool,
            pool_pre_ping=True,      # Detect stale connections
            pool_size=5,             # Connection pool size
            max_overflow=10,         # Additional connections when pool is full
            echo=False,              # Set True for SQL query logging
        )
    return _engine


def dispose_engine() -> None:
    """
    Dispose the engine and close all connections.

    Use this for cleanup or when changing database connection.
    Next call to get_engine() will create a new engine.

    Example:
        >>> dispose_engine()
        >>> # Next get_engine() will create new connection
    """
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None


@contextmanager
def get_connection() -> Generator[Any, None, None]:
    """
    Thread-safe connection context manager.

    Each call gets its own connection from the pool.
    Automatically closes connection after use.

    Usage:
        >>> with get_connection() as conn:
        ...     result = conn.execute(text("SELECT * FROM datasources"))
        ...     data = result.fetchall()

    Yields:
        Connection: SQLAlchemy connection object

    Note:
        - Thread-safe: each thread gets its own connection
        - Autocommit: OFF (must call commit() explicitly)
        - For auto-commit transactions, use get_transaction() instead
    """
    engine = get_engine()
    with engine.connect() as conn:
        yield conn
    # Connection automatically returned to pool after with block


@contextmanager
def get_transaction() -> Generator[Any, None, None]:
    """
    Thread-safe transaction context manager with auto-commit.

    Each call gets its own connection and transaction.
    Commits on success, rolls back on exception.

    Usage:
        >>> with get_transaction() as conn:
        ...     conn.execute(text("INSERT INTO datasources (name) VALUES (...)"))
        ...     conn.execute(text("UPDATE configs SET ..."))
        ...     # Auto-commits if no exception

    Yields:
        Connection: SQLAlchemy connection in transaction context

    Note:
        - Thread-safe: each thread gets its own transaction
        - Auto-commit: commits on success, rolls back on exception
        - Use this for all write operations (INSERT, UPDATE, DELETE)
    """
    engine = get_engine()
    with engine.begin() as conn:
        yield conn
    # Transaction auto-commits if no exception, auto-rolls back on exception


def test_connection() -> tuple[bool, str]:
    """
    Test database connection.

    Returns:
        tuple[bool, str]: (success: bool, message: str)

    Example:
        >>> ok, msg = test_connection()
        >>> if ok:
        ...     print("Connected!")
        ... else:
        ...     print(f"Error: {msg}")
    """
    try:
        with get_connection() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        return True, "✅ Database connection successful"
    except Exception as e:
        return False, f"❌ Database connection failed: {str(e)}"
