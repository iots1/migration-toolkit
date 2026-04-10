"""
Pipeline repository - CRUD operations for pipelines table.

This module handles pipeline definitions with:
- Upsert using ON CONFLICT (PostgreSQL feature)
- UUID primary keys
- Foreign key references to datasources
"""
from __future__ import annotations  # Enable modern type hints

import uuid
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from repositories.connection import get_transaction


def save(
    name: str,
    description: str,
    json_data: str,
    source_ds_id: int | None,
    target_ds_id: int | None,
    error_strategy: str = "fail_fast"
) -> tuple[bool, str]:
    """
    Save or update a pipeline configuration.

    Uses PostgreSQL's ON CONFLICT for upsert semantics.

    Args:
        name: Pipeline name (must be unique)
        description: Pipeline description
        json_data: Pipeline configuration as JSON string
        source_ds_id: Source datasource ID (or None)
        target_ds_id: Target datasource ID (or None)
        error_strategy: Error handling strategy ("fail_fast", "continue", "stop_on_error")

    Returns:
        tuple[bool, str]: (success, message)

    Example:
        >>> ok, msg = save(
        ...     "ETL Pipeline",
        ...     "Daily ETL from MySQL to PostgreSQL",
        ...     '{"steps": [...]}',
        ...     source_ds_id=1,
        ...     target_ds_id=2,
        ...     error_strategy="fail_fast"
        ... )
    """
    try:
        with get_transaction() as conn:
            conn.execute(text("""
                INSERT INTO pipelines (
                    name, description, json_data,
                    source_datasource_id, target_datasource_id, error_strategy, updated_at
                )
                VALUES (:name, :description, :json_data,
                        :src_ds, :tgt_ds, :strategy, CURRENT_TIMESTAMP)
                ON CONFLICT (name) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    json_data = EXCLUDED.json_data,
                    source_datasource_id = EXCLUDED.source_datasource_id,
                    target_datasource_id = EXCLUDED.target_datasource_id,
                    error_strategy = EXCLUDED.error_strategy,
                    updated_at = CURRENT_TIMESTAMP
            """), {
                "name": name,
                "description": description,
                "json_data": json_data,
                "src_ds": source_ds_id,
                "tgt_ds": target_ds_id,
                "strategy": error_strategy
            })
        return True, f"✅ Pipeline '{name}' บันทึกสำเร็จ"
    except IntegrityError:
        return False, f"❌ Pipeline '{name}' มีอยู่แล้ว"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def get_list() -> pd.DataFrame:
    """
    Get all pipelines as a pandas DataFrame.

    Returns:
        pd.DataFrame: All pipelines with columns:
            [id, name, description, source_datasource_id, target_datasource_id,
             error_strategy, created_at, updated_at]

    Example:
        >>> df = get_list()
        >>> print(df)
                                      id          name  ...
        0  123e4567-e89b-12d3-a456-426614174000  ETL Pipeline  ...
    """
    with get_transaction() as conn:
        return pd.read_sql(
            """SELECT id, name, description, source_datasource_id, target_datasource_id,
                      error_strategy, created_at, updated_at
               FROM pipelines
               ORDER BY updated_at DESC""",
            conn
        )


def get_by_name(name: str) -> dict | None:
    """
    Get pipeline by name.

    Args:
        name: Pipeline name

    Returns:
        dict | None: Pipeline data or None if not found
            {
                "id": <uuid>,
                "name": <str>,
                "description": <str>,
                "json_data": <str>,
                "source_datasource_id": <int|None>,
                "target_datasource_id": <int|None>,
                "error_strategy": <str>,
                "created_at": <timestamp>,
                "updated_at": <timestamp>
            }

    Example:
        >>> pipeline = get_by_name("ETL Pipeline")
        >>> if pipeline:
        ...     print(f"Strategy: {pipeline['error_strategy']}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM pipelines WHERE name = :name"),
            {"name": name}
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        data = dict(zip(columns, row))
        # Convert UUID to string
        data["id"] = str(data["id"])
        return data


def delete(name: str) -> tuple[bool, str]:
    """
    Delete a pipeline and all its runs.

    Args:
        name: Pipeline name to delete

    Returns:
        tuple[bool, str]: (success, message)

    Note:
        This will cascade delete all pipeline_runs for this pipeline.

    Example:
        >>> ok, msg = delete("Old Pipeline")
        >>> if ok:
        ...     print("Deleted!")
    """
    try:
        with get_transaction() as conn:
            result = conn.execute(
                text("DELETE FROM pipelines WHERE name = :name"),
                {"name": name}
            )
            if result.rowcount == 0:
                return False, f"❌ ไม่พบ pipeline '{name}'"
        return True, f"✅ ลบ pipeline '{name}' สำเร็จ"
    except Exception as e:
        return False, f"❌ เกิดข้อผิดพลาด: {str(e)}"


def get_with_datasource_names(name: str) -> dict | None:
    """
    Get pipeline with datasource names joined in.

    Args:
        name: Pipeline name

    Returns:
        dict | None: Pipeline data with datasource names or None

    Example:
        >>> pipeline = get_with_datasource_names("ETL Pipeline")
        >>> if pipeline:
        ...     print(f"Source: {pipeline.get('source_datasource_name')}")
    """
    with get_transaction() as conn:
        result = conn.execute(text("""
            SELECT
                p.id, p.name, p.description, p.json_data,
                p.source_datasource_id, s1.name as source_datasource_name,
                p.target_datasource_id, s2.name as target_datasource_name,
                p.error_strategy, p.created_at, p.updated_at
            FROM pipelines p
            LEFT JOIN datasources s1 ON p.source_datasource_id = s1.id
            LEFT JOIN datasources s2 ON p.target_datasource_id = s2.id
            WHERE p.name = :name
        """), {"name": name})
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        data = dict(zip(columns, row))
        data["id"] = str(data["id"])
        return data
