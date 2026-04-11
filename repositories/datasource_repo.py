"""
Datasource repository - CRUD operations for datasources table.

This module handles all database operations for datasource entities.
Thread-safe: each function gets its own connection/transaction.
"""

from __future__ import annotations

import uuid
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from repositories.connection import get_transaction
from models.datasource import DatasourceRecord


def get_all() -> pd.DataFrame:
    """
    Get all datasources as a pandas DataFrame.

    Returns:
        pd.DataFrame: All datasources with columns:
            [id, name, db_type, host, port, dbname, username]
    """
    with get_transaction() as conn:
        df = pd.read_sql(
            "SELECT id, name, db_type, host, port, dbname, username FROM datasources WHERE is_deleted = false ORDER BY name",
            conn,
        )
        import numpy as np

        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = np.array(df[col].fillna("").tolist(), dtype=object)
        return df


def get_all_list() -> list[dict]:
    """Get all datasources as a list of dicts (includes password for internal use)."""
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT id, name, db_type, host, port, dbname, username, password FROM datasources WHERE is_deleted = false ORDER BY name"
            )
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]


def get_by_id(ds_id) -> dict | None:
    """
    Get datasource by ID.

    Args:
        ds_id: Datasource ID (UUID or string)

    Returns:
        dict | None: Datasource data or None if not found

    Example:
        >>> ds = get_by_id(uuid.UUID("..."))
        >>> if ds:
        ...     print(f"Host: {ds['host']}")
    """
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT id, name, db_type, host, port, dbname, username, password FROM datasources WHERE id = :id AND is_deleted = false"
            ),
            {"id": ds_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))


def get_by_name(name: str) -> dict | None:
    """
    Get datasource by name.

    Args:
        name: Datasource name (unique)

    Returns:
        dict | None: Datasource data or None if not found

    Example:
        >>> ds = get_by_name("MySQL DB")
        >>> if ds:
        ...     print(f"Host: {ds['host']}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT id, name, db_type, host, port, dbname, username, password FROM datasources WHERE name = :name AND is_deleted = false"
            ),
            {"name": name},
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))


def save(record: DatasourceRecord) -> tuple[bool, str]:
    """Insert a new datasource. Pass a DatasourceRecord — no flat kwargs."""
    col_params = {
        "name": record.name,
        "db_type": record.db_type,
        "host": record.host,
        "port": record.port,
        "dbname": record.dbname,
        "username": record.username,
        "password": record.password,
    }
    try:
        with get_transaction() as conn:
            conn.execute(
                text("""
                    INSERT INTO datasources (name, db_type, host, port, dbname, username, password)
                    VALUES (:name, :db_type, :host, :port, :dbname, :username, :password)
                """),
                col_params,
            )
        return True, f"✅ บันทึก '{record.name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{record.name}' มีอยู่แล้ว"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def update(ds_id, record: DatasourceRecord) -> tuple[bool, str]:
    """Update an existing datasource by ID. Pass a DatasourceRecord — no flat kwargs."""
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
    col_params = {
        "name": record.name,
        "db_type": record.db_type,
        "host": record.host,
        "port": record.port,
        "dbname": record.dbname,
        "username": record.username,
        "password": record.password,
    }
    try:
        with get_transaction() as conn:
            conn.execute(
                text("""
                    UPDATE datasources SET
                        name = :name,
                        db_type = :db_type,
                        host = :host,
                        port = :port,
                        dbname = :dbname,
                        username = :username,
                        password = :password,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """),
                {"id": ds_id, **col_params},
            )
        return True, f"✅ อัปเดต '{record.name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{record.name}' มีอยู่แล้ว"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def delete(ds_id) -> None:
    """
    Soft-delete a datasource by ID (sets is_deleted = true).

    Args:
        ds_id: Datasource ID to delete (UUID or string)

    Note:
        This sets is_deleted = true instead of hard-deleting.
        Cascade behavior is handled by foreign key constraints.

    Example:
        >>> delete(uuid.UUID("..."))  # Soft-deletes datasource
    """
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
    with get_transaction() as conn:
        conn.execute(
            text(
                "UPDATE datasources SET is_deleted = true, deleted_at = CURRENT_TIMESTAMP WHERE id = :id"
            ),
            {"id": ds_id},
        )
