"""
db_connector.py — SQLAlchemy engine factory.

Phase 9 Refactoring: This file now contains ONLY the SQLAlchemy engine creation logic.
All other responsibilities have been extracted to:
    - services/connection_pool.py — Raw DBAPI connection pool
    - services/schema_inspector.py — Schema inspection & sampling
    - services/connection_tester.py — Connection testing

This module is used by the Migration Engine for Pandas read_sql/to_sql operations.
It creates SQLAlchemy engines with proper URL handling and connection pooling.
"""
from __future__ import annotations
from typing import Optional

from sqlalchemy import Engine, URL
from sqlalchemy import create_engine


def create_sqlalchemy_engine(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    charset: str | None = None,
    **engine_kwargs
) -> Optional[Engine]:
    """
    Creates a SQLAlchemy Engine using URL object construction.

    This prevents errors when passwords contain special characters (@, :, /).

    Args:
        db_type: Database type ("MySQL", "PostgreSQL", "Microsoft SQL Server")
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        charset: Optional charset override.
                 For Thai legacy databases, use 'tis620' or 'latin1'.
                 Default: 'utf8mb4' for MySQL, 'utf8' for others.
        **engine_kwargs: Additional keyword arguments passed to create_engine()

    Returns:
        SQLAlchemy Engine instance or None if creation fails

    Raises:
        ValueError: If db_type is not supported
        Exception: If engine creation fails (exception is re-raised)

    Examples:
        >>> engine = create_sqlalchemy_engine(
        ...     "MySQL", "localhost", "3306", "mydb", "user", "pass"
        ... )
        >>> df = pd.read_sql("SELECT * FROM table", engine)

        >>> # Thai legacy database with TIS-620 encoding
        >>> engine = create_sqlalchemy_engine(
        ...     "MySQL", "localhost", "3306", "mydb", "user", "pass",
        ...     charset="tis620"
        ... )
    """
    try:
        # Convert port to int if exists
        port_int = int(port) if port and str(port).strip() else None

        if db_type == "MySQL":
            # Requires: pip install pymysql
            mysql_charset = charset if charset else "utf8mb4"
            connection_url = URL.create(
                "mysql+pymysql",
                username=user,
                password=password,
                host=host,
                port=port_int or 3306,
                database=db_name,
                query={
                    "charset": mysql_charset,
                    "binary_prefix": "true"  # Helps with binary/blob data
                }
            )

        elif db_type == "PostgreSQL":
            # Requires: pip install psycopg2-binary
            pg_encoding = charset if charset else "utf8"
            connection_url = URL.create(
                "postgresql+psycopg2",
                username=user,
                password=password,
                host=host,
                port=port_int or 5432,
                database=db_name,
                query={"client_encoding": pg_encoding}
            )

        elif db_type == "Microsoft SQL Server":
            # Requires: pip install pymssql
            mssql_charset = charset if charset else "utf8"
            connection_url = URL.create(
                "mssql+pymssql",
                username=user,
                password=password,
                host=host,
                port=port_int or 1433,
                database=db_name,
                query={"charset": mssql_charset}
            )

        else:
            raise ValueError(f"Unsupported DB Type for Engine: {db_type}")

        # Create Engine with pool settings
        # pool_recycle: Recycle connections older than 30 minutes (before DB firewall timeout)
        # This is safe for long-running migrations (hours) because we don't close active connections
        if "pool_recycle" not in engine_kwargs:
            engine_kwargs["pool_recycle"] = 1800  # 30 minutes
        if "pool_pre_ping" not in engine_kwargs:
            engine_kwargs["pool_pre_ping"] = False  # Trust pool_recycle instead of SELECT 1

        engine = create_engine(connection_url, **engine_kwargs)
        return engine

    except Exception as e:
        print(f"Error creating engine: {e}")
        raise e


# Re-export functions from split modules for backward compatibility
# This ensures existing code continues to work during transition
from services.connection_pool import close_connection, close_all_connections
from services.connection_tester import test_db_connection
from services.schema_inspector import (
    get_tables_from_datasource,
    get_columns_from_table,
    get_foreign_keys,
    get_table_sample_data,
    get_column_sample_values,
)
