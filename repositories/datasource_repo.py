"""
Datasource repository - CRUD operations for datasources table.

This module handles all database operations for datasource entities.
Thread-safe: each function gets its own connection/transaction.
"""
from __future__ import annotations  # Enable modern type hints

import uuid
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from repositories.connection import get_transaction


def get_all() -> pd.DataFrame:
    """
    Get all datasources as a pandas DataFrame.

    Returns:
        pd.DataFrame: All datasources with columns:
            [id, name, db_type, host, dbname, username]

    Example:
        >>> df = get_all()
        >>> print(df)
           id     name  db_type        host      dbname username
           0   1  MySQL DB    MySQL  localhost   testdb      root
    """
    with get_transaction() as conn:
        df = pd.read_sql(
            "SELECT id, name, db_type, host, dbname, username FROM datasources ORDER BY id",
            conn
        )
        # Force string columns to numpy object dtype (prevents PyArrow LargeUtf8 errors)
        import numpy as np
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = np.array(df[col].fillna("").tolist(), dtype=object)
        return df


def get_by_id(ds_id: int) -> dict | None:
    """
    Get datasource by ID.

    Args:
        ds_id: Datasource ID (integer primary key)

    Returns:
        dict | None: Datasource data or None if not found

    Example:
        >>> ds = get_by_id(1)
        >>> if ds:
        ...     print(f"Host: {ds['host']}")
    """
    # Convert numpy.int64 to int for psycopg2
    ds_id = int(ds_id)
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM datasources WHERE id = :id"),
            {"id": ds_id}
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
            text("SELECT * FROM datasources WHERE name = :name"),
            {"name": name}
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        return dict(zip(columns, row))


def save(
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str
) -> tuple[bool, str]:
    """
    Save a new datasource.

    Args:
        name: Datasource name (must be unique)
        db_type: Database type (MySQL, PostgreSQL, Microsoft SQL Server)
        host: Database host
        port: Database port
        dbname: Database name
        username: Database username
        password: Database password

    Returns:
        tuple[bool, str]: (success, message)

    Example:
        >>> ok, msg = save("My DB", "MySQL", "localhost", "3306",
        ...                "testdb", "root", "password")
        >>> if ok:
        ...     print("Saved!")
    """
    try:
        with get_transaction() as conn:
            conn.execute(text("""
                INSERT INTO datasources (name, db_type, host, port, dbname, username, password)
                VALUES (:name, :db_type, :host, :port, :dbname, :username, :password)
            """), {
                "name": name,
                "db_type": db_type,
                "host": host,
                "port": port,
                "dbname": dbname,
                "username": username,
                "password": password
            })
        return True, f"✅ บันทึก '{name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{name}' มีอยู่แล้ว"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def update(
    ds_id: int,
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str
) -> tuple[bool, str]:
    """
    Update an existing datasource.

    Args:
        ds_id: Datasource ID to update
        name: New datasource name (must be unique)
        db_type: Database type
        host: Database host
        port: Database port
        dbname: Database name
        username: Database username
        password: Database password

    Returns:
        tuple[bool, str]: (success, message)

    Example:
        >>> ok, msg = update(1, "Updated Name", "PostgreSQL", ...)
        >>> if ok:
        ...     print("Updated!")
    """
    # Convert numpy.int64 to int for psycopg2
    ds_id = int(ds_id)
    try:
        with get_transaction() as conn:
            conn.execute(text("""
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
            """), {
                "id": ds_id,
                "name": name,
                "db_type": db_type,
                "host": host,
                "port": port,
                "dbname": dbname,
                "username": username,
                "password": password
            })
        return True, f"✅ อัปเดต '{name}' สำเร็จ"
    except IntegrityError:
        return False, f"❌ ชื่อ '{name}' มีอยู่แล้ว"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def delete(ds_id: int) -> None:
    """
    Delete a datasource by ID.

    Args:
        ds_id: Datasource ID to delete

    Note:
        This will also cascade delete any pipelines that reference
        this datasource as source or target.

    Example:
        >>> delete(1)  # Deletes datasource with ID 1
    """
    with get_transaction() as conn:
        conn.execute(text("DELETE FROM datasources WHERE id = :id"), {"id": ds_id})
