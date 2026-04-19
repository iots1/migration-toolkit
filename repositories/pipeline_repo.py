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
                        is_deleted = false,
                        deleted_at = NULL,
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


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text(
                "SELECT COUNT(*) FROM pipelines WHERE is_deleted = false AND deleted_at IS NULL"
            )
        )
        return result.scalar()


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
    """Get pipeline by UUID with nodes (JOIN configs) and edges (JOIN configs).

    Returns pipeline row with two extra properties:
        nodes: [{id, pipeline_id, config_id, config_name, position_x, position_y, order_sort}, ...]
        edges: [{id, pipeline_id, source_config_uuid, target_config_uuid,
                 source_config_name, target_config_name}, ...]
    """
    import json as _json

    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id::text AS id, name, description, json_data,
                   error_strategy, created_at, created_by, updated_at, updated_by,
                   is_deleted, deleted_at, deleted_by, deleted_reason
            FROM pipelines WHERE id = :id AND is_deleted = false
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

        nodes_result = conn.execute(
            text("""
            SELECT pn.id::text AS id,
                   pn.pipeline_id::text AS pipeline_id,
                   pn.config_id::text AS config_id,
                   c.config_name,
                   pn.position_x, pn.position_y, pn.order_sort
            FROM pipeline_nodes pn
            JOIN configs c ON c.id = pn.config_id
            WHERE pn.pipeline_id = :id
              AND pn.is_deleted = false
            ORDER BY pn.order_sort ASC
            """),
            {"id": pipeline_id},
        )
        data["nodes"] = [
            dict(zip(nodes_result.keys(), r)) for r in nodes_result.fetchall()
        ]

        edges_result = conn.execute(
            text("""
            SELECT pe.id::text AS id,
                   pe.pipeline_id::text AS pipeline_id,
                   pe.source_config_uuid::text AS source_config_uuid,
                   pe.target_config_uuid::text AS target_config_uuid,
                   src_c.config_name AS source_config_name,
                   tgt_c.config_name AS target_config_name
            FROM pipeline_edges pe
            JOIN configs src_c ON src_c.id = pe.source_config_uuid
            JOIN configs tgt_c ON tgt_c.id = pe.target_config_uuid
            WHERE pe.pipeline_id = :id
              AND pe.is_deleted = false
            """),
            {"id": pipeline_id},
        )
        data["edges"] = [
            dict(zip(edges_result.keys(), r)) for r in edges_result.fetchall()
        ]

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


def delete(pipeline_id: str) -> tuple[bool, str]:
    """
    Soft-delete a pipeline by ID (sets is_deleted = true).

    Note:
        Soft delete keeps the row but marks it invisible. Hard cascade
        (nodes/edges/jobs/runs) is NOT triggered here — those rows remain
        until the pipeline is hard-deleted or cleaned up separately.
        Use delete_hard() if you need cascade removal.

    Example:
        >>> ok, msg = delete("848ab041-448a-47f7-bd19-c98b26a290cb")
        >>> if ok:
        ...     print("Deleted!")
    """
    try:
        with get_transaction() as conn:
            result = conn.execute(
                text(
                    "UPDATE pipelines SET is_deleted = true, deleted_at = CURRENT_TIMESTAMP"
                    " WHERE id = :id AND is_deleted = false"
                ),
                {"id": pipeline_id},
            )
            if result.rowcount == 0:
                return False, f"Pipeline '{pipeline_id}' not found"
        return True, f"Pipeline '{pipeline_id}' deleted successfully"
    except Exception:
        return False, f"Failed to delete pipeline '{pipeline_id}'"
