"""
Datasource repository - CRUD operations for datasources table.

Thread-safe: each function gets its own connection/transaction.
"""
from __future__ import annotations

import uuid

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from repositories.connection import get_transaction
from repositories.utils import row_to_dict, rows_to_dicts
from models.datasource import DatasourceRecord

_COLUMNS = "id, name, db_type, host, port, dbname, username, password, charset"


def get_all() -> pd.DataFrame:
    """Get all datasources as a pandas DataFrame (used by Streamlit views)."""
    with get_transaction() as conn:
        df = pd.read_sql(
            "SELECT id, name, db_type, host, port, dbname, username, charset"
            " FROM datasources WHERE is_deleted = false ORDER BY name",
            conn,
        )
        for col in df.columns:
            if pd.api.types.is_string_dtype(df[col]):
                df[col] = np.array(df[col].fillna("").tolist(), dtype=object)
        return df


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM datasources WHERE is_deleted = false AND deleted_at IS NULL")
        )
        return result.scalar()


def get_all_list() -> list[dict]:
    """Get all datasources as a list of dicts (includes password for internal use)."""
    with get_transaction() as conn:
        result = conn.execute(
            text(
                f"SELECT {_COLUMNS} FROM datasources WHERE is_deleted = false ORDER BY name"
            )
        )
        return rows_to_dicts(result)


def get_by_id(ds_id: str | uuid.UUID) -> dict | None:
    """Get datasource by ID."""
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
    with get_transaction() as conn:
        result = conn.execute(
            text(
                f"SELECT {_COLUMNS} FROM datasources WHERE id = :id AND is_deleted = false"
            ),
            {"id": ds_id},
        )
        return row_to_dict(result)


def get_by_name(name: str) -> dict | None:
    """Get datasource by name (used by Streamlit views)."""
    with get_transaction() as conn:
        result = conn.execute(
            text(
                f"SELECT {_COLUMNS} FROM datasources WHERE name = :name AND is_deleted = false"
            ),
            {"name": name},
        )
        return row_to_dict(result)


def save(record: DatasourceRecord) -> uuid.UUID:
    """Insert a new datasource. Returns the generated UUID.

    Raises IntegrityError on duplicate name (handled by BaseService.execute_db_operation).
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                INSERT INTO datasources (name, db_type, host, port, dbname, username, password, charset)
                VALUES (:name, :db_type, :host, :port, :dbname, :username, :password, :charset)
                RETURNING id
            """),
            {
                "name": record.name,
                "db_type": record.db_type,
                "host": record.host,
                "port": record.port,
                "dbname": record.dbname,
                "username": record.username,
                "password": record.password,
                "charset": record.charset,
            },
        )
        return result.scalar()


def update(ds_id: str | uuid.UUID, record: DatasourceRecord) -> tuple[bool, str]:
    """Update an existing datasource by ID."""
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
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
                        charset = :charset,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id
                """),
                {
                    "id": ds_id,
                    "name": record.name,
                    "db_type": record.db_type,
                    "host": record.host,
                    "port": record.port,
                    "dbname": record.dbname,
                    "username": record.username,
                    "password": record.password,
                    "charset": record.charset,
                },
            )
        return True, f"Datasource '{record.name}' updated successfully"
    except IntegrityError:
        return False, f"Datasource '{record.name}' already exists"
    except Exception:
        return False, f"Failed to update datasource '{record.name}'"


def delete(ds_id: str | uuid.UUID) -> None:
    """Soft-delete a datasource by ID."""
    if isinstance(ds_id, str):
        ds_id = uuid.UUID(ds_id)
    with get_transaction() as conn:
        conn.execute(
            text(
                "UPDATE datasources SET is_deleted = true, deleted_at = CURRENT_TIMESTAMP WHERE id = :id"
            ),
            {"id": ds_id},
        )
