"""
schema_inspector.py — Database schema inspection service.

Extracted from db_connector.py in Phase 9 (SRP - Single Responsibility Principle).
Provides functionality to inspect database schemas, tables, columns, foreign keys, and sample data.

All functions use dialect-specific SQL queries via the connection pool.
This module should be used for read-only schema inspection operations.
"""
from __future__ import annotations
import re

from services.connection_pool import _connection_pool


def _safe_id(name: str) -> str:
    """
    Validate DB identifier (table/schema/column) to prevent SQL injection.

    Allows alphanumeric, underscore, hyphen, dot, and spaces (for MSSQL schemas).
    Raises ValueError on suspicious input.

    Args:
        name: The identifier to validate

    Returns:
        The validated identifier string

    Raises:
        ValueError: If the identifier contains unsafe characters
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError(f"Invalid identifier: {name!r}")
    if not re.match(r'^[a-zA-Z0-9_\-\. ]+$', name):
        raise ValueError(f"Unsafe identifier rejected: {name}")
    return name


def get_tables_from_datasource(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    schema: str | None = None
) -> tuple[bool, list[str] | str]:
    """
    Retrieves list of tables from a datasource.

    Args:
        db_type: Database type ("MySQL", "PostgreSQL", "Microsoft SQL Server")
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        schema: Optional schema name (defaults: MySQL=None, PostgreSQL='public', MSSQL='dbo')

    Returns:
        Tuple of (success: bool, tables: list[str] | error_message: str)
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        if db_type == "MySQL":
            cursor.execute("SHOW TABLES")
        elif db_type == "Microsoft SQL Server":
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema_filter}' "
                f"ORDER BY TABLE_NAME"
            )
        elif db_type == "PostgreSQL":
            schema_filter = _safe_id(schema) if schema else 'public'
            cursor.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema_filter}' ORDER BY table_name"
            )
        else:
            return False, f"Unknown Database Type: {db_type}"

        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return True, tables
    except Exception as e:
        return False, str(e)


def get_columns_from_table(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    table_name: str,
    schema: str | None = None
) -> tuple[bool, list[dict] | str]:
    """
    Retrieves column information from a specific table, including nullable status and default values.

    Args:
        db_type: Database type
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        table_name: Table name to inspect
        schema: Optional schema name

    Returns:
        Tuple of (success: bool, columns: list[dict] | error_message: str)
        Each column dict has keys: name, type, is_nullable, column_default
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        safe_table = _safe_id(table_name)

        if db_type == "MySQL":
            cursor.execute(f"DESCRIBE `{safe_table}`")
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[4] if len(row) > 4 else None
                }
                for row in cursor.fetchall()
            ]
        elif db_type == "Microsoft SQL Server":
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT "
                f"FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{safe_table}' AND TABLE_SCHEMA = '{schema_filter}' "
                f"ORDER BY ORDINAL_POSITION"
            )
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[3]
                }
                for row in cursor.fetchall()
            ]
        elif db_type == "PostgreSQL":
            schema_filter = _safe_id(schema) if schema else 'public'
            cursor.execute(
                f"SELECT column_name, data_type, is_nullable, column_default "
                f"FROM information_schema.columns "
                f"WHERE table_name = '{safe_table}' AND table_schema = '{schema_filter}' "
                f"ORDER BY ordinal_position"
            )
            columns = [
                {
                    "name": row[0],
                    "type": row[1],
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[3]
                }
                for row in cursor.fetchall()
            ]
        else:
            cursor.close()
            return False, f"Unknown Database Type: {db_type}"

        cursor.close()
        return True, columns
    except Exception as e:
        return False, str(e)


def get_foreign_keys(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    schema: str | None = None
) -> tuple[bool, list[dict] | str]:
    """
    Retrieves foreign key relationships from a datasource.

    Args:
        db_type: Database type
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        schema: Optional schema name

    Returns:
        Tuple of (success: bool, relationships: list[dict] | error_message: str)
        Each relationship dict has keys: table, col, ref_table, ref_col
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)
        relationships = []

        if db_type == "MySQL":
            safe_db = _safe_id(db_name)
            query = f"""
                SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE REFERENCED_TABLE_SCHEMA = '{safe_db}'
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0],
                    "col": row[1],
                    "ref_table": row[2],
                    "ref_col": row[3]
                })

        elif db_type == "PostgreSQL":
            schema_filter = _safe_id(schema) if schema else 'public'
            query = f"""
                SELECT
                    tc.table_name, kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = '{schema_filter}'
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0],
                    "col": row[1],
                    "ref_table": row[2],
                    "ref_col": row[3]
                })

        elif db_type == "Microsoft SQL Server":
            query = """
                SELECT
                    tp.name, cp.name, tr.name, cr.name
                FROM
                    sys.foreign_keys fk
                INNER JOIN
                    sys.tables tp ON fk.parent_object_id = tp.object_id
                INNER JOIN
                    sys.tables tr ON fk.referenced_object_id = tr.object_id
                INNER JOIN
                    sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                INNER JOIN
                    sys.columns cp ON fkc.parent_column_id = cp.column_id AND fkc.parent_object_id = cp.object_id
                INNER JOIN
                    sys.columns cr ON fkc.referenced_column_id = cr.column_id AND fkc.referenced_object_id = cr.object_id
            """
            cursor.execute(query)
            for row in cursor.fetchall():
                relationships.append({
                    "table": row[0],
                    "col": row[1],
                    "ref_table": row[2],
                    "ref_col": row[3]
                })

        cursor.close()
        return True, relationships
    except Exception as e:
        return False, str(e)


def get_table_sample_data(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    table_name: str,
    limit: int = 50,
    schema: str | None = None
) -> tuple[bool, str | tuple, list]:
    """
    Retrieves a sample of data from a table.

    Args:
        db_type: Database type
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        table_name: Table name to sample
        limit: Maximum number of rows to return (default: 50)
        schema: Optional schema name

    Returns:
        Tuple of (success: bool, error_message: str | (rows, columns): tuple)
        On success: (True, (rows, columns))
        On failure: (False, error_message, [])
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        safe_table = _safe_id(table_name)
        safe_schema = _safe_id(schema) if schema else None
        limit = int(limit)  # ensure numeric

        table_ref = safe_table
        if safe_schema:
            if db_type == "Microsoft SQL Server":
                table_ref = f"[{safe_schema}].[{safe_table}]"
            elif db_type == "PostgreSQL":
                table_ref = f'"{safe_schema}"."{safe_table}"'

        query = ""
        if db_type == "Microsoft SQL Server":
            query = f"SELECT TOP {limit} * FROM {table_ref}"
        else:
            query = f"SELECT * FROM {table_ref} LIMIT {limit}"

        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        cursor.close()
        return True, rows, columns
    except Exception as e:
        return False, str(e), []


def get_column_sample_values(
    db_type: str,
    host: str,
    port: str,
    db_name: str,
    user: str,
    password: str,
    table_name: str,
    column_name: str,
    limit: int = 20,
    schema: str | None = None
) -> tuple[bool, list | str]:
    """
    Retrieves distinct sample values from a specific column.

    Args:
        db_type: Database type
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        table_name: Table name
        column_name: Column name to sample
        limit: Maximum number of distinct values to return (default: 20)
        schema: Optional schema name

    Returns:
        Tuple of (success: bool, values: list | error_message: str)
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password)

        safe_table = _safe_id(table_name)
        safe_col = _safe_id(column_name)
        safe_schema = _safe_id(schema) if schema else None
        limit = int(limit)  # ensure numeric

        table_ref = safe_table
        if safe_schema:
            if db_type == "Microsoft SQL Server":
                table_ref = f"[{safe_schema}].[{safe_table}]"
            elif db_type == "PostgreSQL":
                table_ref = f'"{safe_schema}"."{safe_table}"'

        if db_type == "MySQL":
            query = (
                f"SELECT DISTINCT `{safe_col}` FROM {table_ref} "
                f"WHERE `{safe_col}` IS NOT NULL AND CAST(`{safe_col}` AS CHAR) <> '' "
                f"LIMIT {limit}"
            )
        elif db_type == "PostgreSQL":
            query = (
                f'SELECT DISTINCT "{safe_col}" FROM {table_ref} '
                f'WHERE "{safe_col}" IS NOT NULL AND CAST("{safe_col}" AS TEXT) <> \'\' '
                f'LIMIT {limit}'
            )
        elif db_type == "Microsoft SQL Server":
            query = (
                f"SELECT DISTINCT TOP {limit} [{safe_col}] FROM {table_ref} "
                f"WHERE [{safe_col}] IS NOT NULL AND CAST([{safe_col}] AS NVARCHAR(MAX)) <> ''"
            )
        else:
            return False, f"Unknown Database Type: {db_type}"

        cursor.execute(query)
        values = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return True, values
    except Exception as e:
        return False, str(e)
