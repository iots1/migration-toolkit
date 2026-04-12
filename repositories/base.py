"""
PostgreSQL DDL schema and database initialization.

This module defines all table schemas for PostgreSQL 18+ with:
- UUID native types (using gen_random_uuid())
- TIMESTAMP WITH TIME ZONE for timezone-aware timestamps
- Proper foreign key constraints with CASCADE
- Audit fields (created_at, updated_at)
- Soft-delete fields (is_deleted, deleted_at, deleted_by, deleted_reason)
"""

from sqlalchemy import text
from repositories.connection import get_engine

TABLES_DDL = [
    """CREATE TABLE IF NOT EXISTS datasources (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) UNIQUE NOT NULL,
        db_type VARCHAR(50) NOT NULL,
        host VARCHAR(255),
        port VARCHAR(10),
        dbname VARCHAR(255),
        username VARCHAR(255),
        password VARCHAR(255),
        charset VARCHAR(50),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS configs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        config_name VARCHAR(255) UNIQUE NOT NULL,
        table_name VARCHAR(255),
        json_data TEXT,
        datasource_source_id UUID REFERENCES datasources(id) ON DELETE SET NULL,
        datasource_target_id UUID REFERENCES datasources(id) ON DELETE SET NULL,
        config_type VARCHAR(20) DEFAULT 'std',
        script TEXT,
        generate_sql TEXT,
        condition TEXT,
        lookup TEXT,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS config_histories (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        config_id UUID NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        version INTEGER NOT NULL,
        json_data TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        UNIQUE(config_id, version)
    )""",
    """CREATE TABLE IF NOT EXISTS pipelines (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name VARCHAR(255) UNIQUE NOT NULL,
        description TEXT DEFAULT '',
        json_data TEXT,
        error_strategy VARCHAR(50) DEFAULT 'fail_fast',
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_nodes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        config_id UUID NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        position_x INTEGER DEFAULT 0,
        position_y INTEGER DEFAULT 0,
        order_sort INTEGER DEFAULT 0,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_edges (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        source_config_uuid UUID NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        target_config_uuid UUID NOT NULL REFERENCES configs(id) ON DELETE CASCADE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS pipeline_runs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
        status VARCHAR(50) DEFAULT 'pending',
        started_at TIMESTAMP WITH TIME ZONE,
        completed_at TIMESTAMP WITH TIME ZONE,
        steps_json TEXT,
        error_message TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        created_by UUID,
        updated_by UUID,
        is_deleted BOOLEAN NOT NULL DEFAULT false,
        deleted_at TIMESTAMP WITH TIME ZONE,
        deleted_by UUID,
        deleted_reason TEXT
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
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS "pgcrypto"'))

        for ddl in TABLES_DDL:
            conn.execute(text(ddl))


def drop_all_tables() -> None:
    """
    Drop all tables. Use with caution - this deletes all data!

    Order matters due to foreign key constraints.
    """
    engine = get_engine()

    with engine.begin() as conn:
        tables = [
            "pipeline_runs",
            "pipeline_edges",
            "pipeline_nodes",
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
        ...     print(f"{table['table_name']}: {table['column_count']} rows")
    """
    engine = get_engine()

    with engine.connect() as conn:
        result = conn.execute(
            text("""
            SELECT
                table_name,
                (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_schema = 'public' AND table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        )

        return [
            {
                "table_name": row[0],
                "column_count": row[1],
            }
            for row in result.fetchall()
        ]
