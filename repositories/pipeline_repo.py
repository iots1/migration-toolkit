"""
Pipeline repository - CRUD operations for pipelines table.

This module handles pipeline definitions with:
- Upsert using ON CONFLICT (PostgreSQL feature)
- UUID primary keys
"""

from __future__ import annotations  # Enable modern type hints

import uuid
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from repositories.connection import get_transaction
from models.pipeline_config import PipelineRecord


def save(record: PipelineRecord) -> tuple[bool, str]:
    """Upsert a pipeline configuration. Pass a PipelineRecord — no flat kwargs."""
    import json as _json

    json_str = record.json_data
    if isinstance(json_str, dict):
        json_str = _json.dumps(json_str, ensure_ascii=False)

    col_params = {
        "name": record.name,
        "description": record.description,
        "json_data": json_str,
        "error_strategy": record.error_strategy,
    }
    try:
        with get_transaction() as conn:
            conn.execute(
                text("""
                    INSERT INTO pipelines (
                        name, description, json_data,
                        error_strategy, updated_at
                    )
                    VALUES (:name, :description, :json_data,
                            :error_strategy,
                            CURRENT_TIMESTAMP)
                    ON CONFLICT (name) DO UPDATE SET
                        description = EXCLUDED.description,
                        json_data = EXCLUDED.json_data,
                        error_strategy = EXCLUDED.error_strategy,
                        updated_at = CURRENT_TIMESTAMP
                """),
                col_params,
            )
        return True, f"Pipeline '{record.name}' saved successfully"
    except IntegrityError:
        return False, f"Pipeline '{record.name}' already exists"
    except Exception as e:
        return False, f"Failed to save pipeline '{record.name}'"


def get_list() -> pd.DataFrame:
    """
    Get all pipelines as a pandas DataFrame.
    """
    with get_transaction() as conn:
        return pd.read_sql(
            """SELECT id, name, description,
                      error_strategy, created_at, updated_at
               FROM pipelines
               WHERE is_deleted = false
               ORDER BY updated_at DESC""",
            conn,
        )


def get_all_list() -> list[dict]:
    """Get all pipelines as a list of dicts."""
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text(
                """SELECT id::text AS id, name, description, json_data,
                      error_strategy, created_at, created_by, updated_at, updated_by,
                      is_deleted, deleted_at, deleted_by, deleted_reason
               FROM pipelines
               WHERE is_deleted = false
               ORDER BY updated_at DESC"""
            )
        )
        rows = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            raw = data.get("json_data")
            try:
                data["json_data"] = (
                    _json.loads(raw) if isinstance(raw, str) else (raw or {})
                )
            except (_json.JSONDecodeError, TypeError):
                data["json_data"] = {}
            rows.append(data)
        return rows


def get_by_id(pipeline_id: str) -> dict | None:
    """Get pipeline by UUID — returns clean DB row with json_data parsed to dict."""
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id::text AS id, name, description, json_data,
                   error_strategy, created_at, created_by, updated_at, updated_by,
                   is_deleted, deleted_at, deleted_by, deleted_reason
            FROM pipelines WHERE id = :id
            """),
            {"id": pipeline_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        data = dict(zip(result.keys(), row))
        raw = data.get("json_data")
        try:
            data["json_data"] = (
                _json.loads(raw) if isinstance(raw, str) else (raw or {})
            )
        except (_json.JSONDecodeError, TypeError):
            data["json_data"] = {}
        return data


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
            text("SELECT * FROM pipelines WHERE name = :name"), {"name": name}
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
                text("DELETE FROM pipelines WHERE name = :name"), {"name": name}
            )
            if result.rowcount == 0:
                return False, f"Pipeline '{name}' not found"
        return True, f"Pipeline '{name}' deleted successfully"
    except Exception as e:
        return False, f"Failed to delete pipeline '{name}'"
