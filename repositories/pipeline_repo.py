"""
Pipeline repository - CRUD operations for pipelines table.
"""
from __future__ import annotations

import json
import uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from repositories.connection import get_transaction
from repositories.utils import row_to_dict, rows_to_dicts, parse_json_field
from models.pipeline_config import PipelineRecord


def save(record: PipelineRecord) -> uuid.UUID:
    """Insert a new pipeline. Returns the generated UUID.

    Raises IntegrityError on duplicate name (handled by BaseService.execute_db_operation).
    """
    json_str = record.json_data
    if isinstance(json_str, dict):
        json_str = json.dumps(json_str, ensure_ascii=False)

    with get_transaction() as conn:
        result = conn.execute(
            text("""
                INSERT INTO pipelines (name, description, json_data, error_strategy, updated_at)
                VALUES (:name, :description, :json_data, :error_strategy, CURRENT_TIMESTAMP)
                RETURNING id
            """),
            {
                "name": record.name,
                "description": record.description,
                "json_data": json_str,
                "error_strategy": record.error_strategy,
            },
        )
        return result.scalar()


def update_by_id(pipeline_id: str, record: PipelineRecord) -> tuple[bool, str]:
    """Update a pipeline by UUID. Allows renaming because it matches by id, not name."""
    json_str = record.json_data
    if isinstance(json_str, dict):
        json_str = json.dumps(json_str, ensure_ascii=False)

    try:
        with get_transaction() as conn:
            result = conn.execute(
                text("""
                    UPDATE pipelines SET
                        name = :name,
                        description = :description,
                        json_data = :json_data,
                        error_strategy = :error_strategy,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :id AND is_deleted = false
                """),
                {
                    "id": pipeline_id,
                    "name": record.name,
                    "description": record.description,
                    "json_data": json_str,
                    "error_strategy": record.error_strategy,
                },
            )
            if result.rowcount == 0:
                return False, f"Pipeline '{pipeline_id}' not found"
        return True, f"Pipeline '{pipeline_id}' updated successfully"
    except IntegrityError:
        return False, f"Pipeline name '{record.name}' already exists"
    except Exception as e:
        return False, f"Failed to update pipeline '{pipeline_id}': {e}"


def get_list() -> pd.DataFrame:
    """Get all pipelines as a pandas DataFrame (used by Streamlit views)."""
    with get_transaction() as conn:
        return pd.read_sql(
            "SELECT id, name, description, error_strategy, created_at, updated_at"
            " FROM pipelines WHERE is_deleted = false ORDER BY updated_at DESC",
            conn,
        )


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM pipelines WHERE is_deleted = false AND deleted_at IS NULL")
        )
        return result.scalar()


def get_all_list() -> list[dict]:
    """Get all pipelines as a list of dicts."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id::text AS id, name, description, json_data,
                       error_strategy, created_at, created_by, updated_at, updated_by,
                       is_deleted, deleted_at, deleted_by, deleted_reason
                FROM pipelines
                WHERE is_deleted = false
                ORDER BY updated_at DESC
            """)
        )
        rows = rows_to_dicts(result)
        for row in rows:
            parse_json_field(row)
        return rows


def get_by_id(pipeline_id: str) -> dict | None:
    """Get pipeline by UUID with nodes (JOIN configs) and edges (JOIN configs)."""
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
        data = row_to_dict(result)
        if data is None:
            return None
        parse_json_field(data)

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
        data["nodes"] = rows_to_dicts(nodes_result)

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
        data["edges"] = rows_to_dicts(edges_result)

        return data


def get_by_name(name: str) -> dict | None:
    """Get pipeline by name (used by deprecated database.py facade)."""
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM pipelines WHERE name = :name"), {"name": name}
        )
        data = row_to_dict(result)
        if data is None:
            return None
        data["id"] = str(data["id"])
        return data


def delete(pipeline_id: str) -> tuple[bool, str]:
    """Soft-delete a pipeline by ID."""
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
