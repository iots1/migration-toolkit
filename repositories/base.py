"""
PostgreSQL DDL schema and database initialization.

This module defines all table schemas for PostgreSQL 18+ with:
- UUID native types (using gen_random_uuid())
- TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
- Proper foreign key constraints with CASCADE
- Audit fields (created_at, updated_at)
"""
from sqlalchemy import text
from repositories.connection import get_engine

# DDL statements for all tables
TABLES_DDL = [
    # datasources table - stores database connection configurations
    """CREATE TABLE IF NOT EXISTS datasources (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) UNIQUE NOT NULL,
        db_type VARCHAR(50) NOT NULL,
        host VARCHAR(255),
        port VARCHAR(10),
        dbname VARCHAR(255),
        username VARCHAR(255),
        password VARCHAR(255),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )""",

    # configs table - stores mapping configurations
    """CREATE TABLE IF NOT EXISTS configs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        config_name VARCHAR(255) UNIQUE NOT NULL,
        table_name VARCHAR(255),
        json_data TEXT,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )""",

    # config_histories table - version history for configs
    """CREATE TABLE IF NOT EXISTS config_histories (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        config_id UUID NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        version INTEGER NOT NULL,
        json_data TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(config_id, version)
    )""",

    # pipelines table - stores pipeline definitions
    """CREATE TABLE IF NOT EXISTS pipelines (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) UNIQUE NOT NULL,
        description TEXT DEFAULT '',
        json_data TEXT,
        source_datasource_id INTEGER REFERENCES datasources(id) ON DELETE SET NULL,
        target_datasource_id INTEGER REFERENCES datasources(id) ON DELETE SET NULL,
        error_strategy VARCHAR(50) DEFAULT 'fail_fast',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )""",

    # pipeline_runs table - stores pipeline execution runs
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        status VARCHAR(50) DEFAULT 'pending',
        started_at TIMESTAMP WITH TIME ZONE,
        completed_at TIMESTAMP WITH TIME ZONE,
        steps_json TEXT,
        error_message TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )""",
]


def init_db() -> None:
    """
    Initialize PostgreSQL database with all required tables.

    This function:
    1. Enables pgcrypto extension for UUID generation
    2. Creates all tables if they don't exist
    3. Is idempotent - safe to run multiple times

    Usage:
        >>> from repositories.base import init_db
        >>> init_db()  # Creates all tables
    """
    engine = get_engine()

    with engine.begin() as conn:
        # Enable pgcrypto extension for gen_random_uuid()
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"pgcrypto\""))

        # Create all tables
        for ddl in TABLES_DDL:
            conn.execute(text(ddl))


def drop_all_tables() -> None:
    """
    Drop all tables. Use with caution - this deletes all data!

    Order matters due to foreign key constraints.
    """
    engine = get_engine()

    with engine.begin() as conn:
        # Drop in reverse order of dependencies
        tables = [
            "pipeline_runs",
            "pipelines",
            "config_histories",
            "configs",
            "datasources",
        ]
        for table in tables:
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE"))


def get_table_info() -> list[dict]:
    """
    Get information about all tables in the database.

    Returns:
        list[dict]: List of table info dictionaries

    Example:
        >>> info = get_table_info()
        >>> for table in info:
        ...     print(f"{table['table_name']}: {table['row_count']} rows")
    """
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT
                table_name,
                (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """))

        return [
            {
                "table_name": row[0],
                "column_count": row[1],
            }
            for row in result.fetchall()
        ]
