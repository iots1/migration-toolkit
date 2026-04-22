"""
schema_inspector.py — Database schema inspection service.

Extracted from db_connector.py in Phase 9 (SRP - Single Responsibility Principle).
Provides functionality to inspect database schemas, tables, columns, foreign keys, and sample data.

All functions use dialect-specific SQL queries via the connection pool.
This module should be used for read-only schema inspection operations.
"""
from __future__ import annotations
import re

from models.db_type import DbType
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
    schema: str | None = None,
    charset: str | None = None,
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
        charset: Optional charset override (e.g. "tis620" for Thai legacy MySQL DBs)

    Returns:
        Tuple of (success: bool, tables: list[str] | error_message: str)
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password, charset)

        if db_type == DbType.MYSQL:
            cursor.execute("SHOW TABLES")
        elif db_type == DbType.MSSQL:
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
                f"WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '{schema_filter}' "
                f"ORDER BY TABLE_NAME"
            )
        elif db_type == DbType.POSTGRESQL:
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
    schema: str | None = None,
    charset: str | None = None,
) -> tuple[bool, list[dict] | str]:
    """
    Retrieves detailed column information from a specific table.

    Args:
        db_type: Database type
        host: Database server host
        port: Database server port
        db_name: Database name
        user: Database user
        password: Database password
        table_name: Table name to inspect
        schema: Optional schema name
        charset: Optional charset override (e.g. "tis620" for Thai legacy MySQL DBs)

    Returns:
        Tuple of (success: bool, columns: list[dict] | error_message: str)
        Each column dict has keys: name, type, is_nullable, column_default, is_primary,
                                length, precision, scale, comment, constraints, indexes
    """
    try:
        _, cursor = _connection_pool.get_connection(db_type, host, port, db_name, user, password, charset)
        safe_table = _safe_id(table_name)
        schema_filter = _safe_id(schema) if schema else ('public' if db_type == DbType.POSTGRESQL else None)

        # Fetch primary keys
        primary_keys = set()
        if db_type == DbType.MYSQL:
            cursor.execute(f"SHOW KEYS FROM `{safe_table}` WHERE Key_name = 'PRIMARY'")
            primary_keys = {row[4] for row in cursor.fetchall()}
        elif db_type == DbType.POSTGRESQL:
            cursor.execute(
                f"SELECT a.attname FROM pg_index i "
                f"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
                f"JOIN pg_class c ON c.oid = i.indrelid "
                f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE i.indisprimary AND c.relname = '{safe_table}' AND n.nspname = '{schema_filter}'"
            )
            primary_keys = {row[0] for row in cursor.fetchall()}
        elif db_type == DbType.MSSQL:
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                f"WHERE TABLE_NAME = '{safe_table}' AND TABLE_SCHEMA = '{schema_filter}' "
                f"AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1"
            )
            primary_keys = {row[0] for row in cursor.fetchall()}

        # Fetch indexes
        indexes_by_column = {}
        if db_type == DbType.MYSQL:
            cursor.execute(f"SHOW INDEX FROM `{safe_table}`")
            for row in cursor.fetchall():
                col_name = row[4]
                index_name = row[2]
                is_unique = row[1] == 0
                if col_name not in indexes_by_column:
                    indexes_by_column[col_name] = []
                indexes_by_column[col_name].append({
                    "name": index_name,
                    "unique": is_unique
                })
        elif db_type == DbType.POSTGRESQL:
            cursor.execute(
                f"SELECT a.attname, i.relname, ix.indisunique, ix.indisprimary "
                f"FROM pg_index ix "
                f"JOIN pg_class t ON t.oid = ix.indrelid "
                f"JOIN pg_class i ON i.oid = ix.indexrelid "
                f"JOIN pg_namespace n ON n.oid = t.relnamespace "
                f"JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) "
                f"WHERE t.relname = '{safe_table}' AND n.nspname = '{schema_filter}'"
            )
            for row in cursor.fetchall():
                col_name, index_name, is_unique, is_primary = row
                if col_name not in indexes_by_column:
                    indexes_by_column[col_name] = []
                indexes_by_column[col_name].append({
                    "name": index_name,
                    "unique": is_unique,
                    "primary": is_primary
                })
        elif db_type == DbType.MSSQL:
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT c.name, i.name, i.is_unique, i.is_primary_key "
                f"FROM sys.indexes i "
                f"JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id "
                f"JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id "
                f"JOIN sys.tables t ON i.object_id = t.object_id "
                f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
                f"WHERE t.name = '{safe_table}' AND s.name = '{schema_filter}'"
            )
            for row in cursor.fetchall():
                col_name, index_name, is_unique, is_primary = row
                if col_name not in indexes_by_column:
                    indexes_by_column[col_name] = []
                indexes_by_column[col_name].append({
                    "name": index_name,
                    "unique": bool(is_unique),
                    "primary": bool(is_primary)
                })

        # Fetch constraints (NOT NULL, UNIQUE, CHECK, etc.)
        constraints_by_column = {}
        if db_type == DbType.POSTGRESQL:
            cursor.execute(
                f"SELECT a.attname, con.conname, con.contype "
                f"FROM pg_constraint con "
                f"JOIN pg_class c ON c.oid = con.conrelid "
                f"JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey) "
                f"WHERE c.relname = '{safe_table}' AND n.nspname = '{schema_filter}'"
            )
            for row in cursor.fetchall():
                col_name, constr_name, constr_type = row
                if col_name not in constraints_by_column:
                    constraints_by_column[col_name] = []
                constraints_by_column[col_name].append({
                    "name": constr_name,
                    "type": {'c': 'CHECK', 'f': 'FOREIGN KEY', 'p': 'PRIMARY KEY', 'u': 'UNIQUE', 'x': 'EXCLUSION'}.get(constr_type, 'UNKNOWN')
                })
        elif db_type == DbType.MYSQL:
            cursor.execute(
                f"SELECT k.COLUMN_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE "
                f"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                f"JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                f"WHERE tc.TABLE_NAME = '{safe_table}' AND tc.TABLE_SCHEMA = '{db_name}'"
            )
            for row in cursor.fetchall():
                col_name, constr_name, constr_type = row
                if col_name not in constraints_by_column:
                    constraints_by_column[col_name] = []
                constraints_by_column[col_name].append({
                    "name": constr_name,
                    "type": constr_type
                })
        elif db_type == DbType.MSSQL:
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT ccu.COLUMN_NAME, tc.CONSTRAINT_NAME, tc.CONSTRAINT_TYPE "
                f"FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc "
                f"JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu "
                f"  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME "
                f"  AND tc.TABLE_NAME = ccu.TABLE_NAME "
                f"  AND tc.TABLE_SCHEMA = ccu.TABLE_SCHEMA "
                f"WHERE tc.TABLE_NAME = '{safe_table}' AND tc.TABLE_SCHEMA = '{schema_filter}'"
            )
            for row in cursor.fetchall():
                col_name, constr_name, constr_type = row
                if col_name not in constraints_by_column:
                    constraints_by_column[col_name] = []
                constraints_by_column[col_name].append({
                    "name": constr_name,
                    "type": constr_type
                })

        # Fetch column details
        if db_type == DbType.MYSQL:
            cursor.execute(
                f"SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, "
                f"CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT "
                f"FROM INFORMATION_SCHEMA.COLUMNS "
                f"WHERE TABLE_NAME = '{safe_table}' AND TABLE_SCHEMA = '{db_name}' "
                f"ORDER BY ORDINAL_POSITION"
            )
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                columns.append({
                    "id": f"{safe_table}.{col_name}",  # Add unique ID for JSON API
                    "name": col_name,
                    "type": row[1],
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[3],
                    "is_primary": col_name in primary_keys,
                    "length": row[4],
                    "precision": row[5],
                    "scale": row[6],
                    "comment": row[7],
                    "constraints": constraints_by_column.get(col_name, []),
                    "indexes": indexes_by_column.get(col_name, [])
                })
        elif db_type == DbType.MSSQL:
            schema_filter = _safe_id(schema) if schema else 'dbo'
            cursor.execute(
                f"SELECT c.COLUMN_NAME, c.DATA_TYPE, c.IS_NULLABLE, c.COLUMN_DEFAULT, "
                f"c.CHARACTER_MAXIMUM_LENGTH, c.NUMERIC_PRECISION, c.NUMERIC_SCALE, "
                f"ep.value, c.DOMAIN_SCHEMA "
                f"FROM INFORMATION_SCHEMA.COLUMNS c "
                f"LEFT JOIN sys.extended_properties ep ON ep.major_id = OBJECT_ID(TABLE_SCHEMA + '.' + TABLE_NAME) "
                f"AND ep.minor_id = c.ORDINAL_POSITION AND ep.name = 'MS_Description' "
                f"WHERE c.TABLE_NAME = '{safe_table}' AND c.TABLE_SCHEMA = '{schema_filter}' "
                f"ORDER BY c.ORDINAL_POSITION"
            )
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                max_length = row[4]
                # Build full type string with length
                full_type = data_type
                if max_length and max_length != -1 and data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                    full_type = f"{data_type}({max_length})"
                elif row[5] and row[6] and data_type in ('decimal', 'numeric'):
                    full_type = f"{data_type}({row[5]},{row[6]})"

                columns.append({
                    "id": f"{safe_table}.{col_name}",  # Add unique ID for JSON API
                    "name": col_name,
                    "type": full_type,
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[3],
                    "is_primary": col_name in primary_keys,
                    "length": max_length if max_length and max_length != -1 else None,
                    "precision": row[5],
                    "scale": row[6],
                    "comment": row[7],
                    "constraints": constraints_by_column.get(col_name, []),
                    "indexes": indexes_by_column.get(col_name, [])
                })
        elif db_type == DbType.POSTGRESQL:
            cursor.execute(
                f"SELECT c.column_name, c.data_type, c.is_nullable, c.column_default, "
                f"c.character_maximum_length, c.numeric_precision, c.numeric_scale, "
                f"pgd.description "
                f"FROM information_schema.columns c "
                f"LEFT JOIN pg_catalog.pg_description pgd ON pgd.objoid = "
                f"(SELECT cls.oid FROM pg_class cls JOIN pg_namespace ns ON ns.oid = cls.relnamespace "
                f"WHERE ns.nspname = '{schema_filter}' AND cls.relname = '{safe_table}') "
                f"AND pgd.objsubid = c.ordinal_position "
                f"WHERE c.table_name = '{safe_table}' AND c.table_schema = '{schema_filter}' "
                f"ORDER BY c.ordinal_position"
            )
            columns = []
            for row in cursor.fetchall():
                col_name = row[0]
                data_type = row[1]
                max_len = row[4]
                precision = row[5]
                scale = row[6]

                # Build full type string with length/precision
                full_type = data_type
                if max_len and data_type in ('character varying', 'varchar', 'character', 'char', 'bpchar'):
                    full_type = f"{data_type}({max_len})"
                elif precision and scale and data_type in ('numeric', 'decimal'):
                    full_type = f"{data_type}({precision},{scale})"

                columns.append({
                    "id": f"{safe_table}.{col_name}",  # Add unique ID for JSON API
                    "name": col_name,
                    "type": full_type,
                    "is_nullable": row[2].upper() == "YES",
                    "column_default": row[3],
                    "is_primary": col_name in primary_keys,
                    "length": max_len,
                    "precision": precision,
                    "scale": scale,
                    "comment": row[7],
                    "constraints": constraints_by_column.get(col_name, []),
                    "indexes": indexes_by_column.get(col_name, [])
                })
        else:
            cursor.close()
            return False, f"Unknown Database Type: {db_type}"

        cursor.close()
        return True, columns
    except Exception as e:
        import traceback
        error_details = f"{str(e)}\n{traceback.format_exc()}"
        return False, error_details


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

        if db_type == DbType.MYSQL:
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

        elif db_type == DbType.POSTGRESQL:
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

        elif db_type == DbType.MSSQL:
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
            if db_type == DbType.MSSQL:
                table_ref = f"[{safe_schema}].[{safe_table}]"
            elif db_type == DbType.POSTGRESQL:
                table_ref = f'"{safe_schema}"."{safe_table}"'

        query = ""
        if db_type == DbType.MSSQL:
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
            if db_type == DbType.MSSQL:
                table_ref = f"[{safe_schema}].[{safe_table}]"
            elif db_type == DbType.POSTGRESQL:
                table_ref = f'"{safe_schema}"."{safe_table}"'

        if db_type == DbType.MYSQL:
            query = (
                f"SELECT DISTINCT `{safe_col}` FROM {table_ref} "
                f"WHERE `{safe_col}` IS NOT NULL AND CAST(`{safe_col}` AS CHAR) <> '' "
                f"LIMIT {limit}"
            )
        elif db_type == DbType.POSTGRESQL:
            query = (
                f'SELECT DISTINCT "{safe_col}" FROM {table_ref} '
                f'WHERE "{safe_col}" IS NOT NULL AND CAST("{safe_col}" AS TEXT) <> \'\' '
                f'LIMIT {limit}'
            )
        elif db_type == DbType.MSSQL:
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
