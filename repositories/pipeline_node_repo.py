"""Pipeline node repository — CRUD for pipeline_nodes table."""
from __future__ import annotations

import uuid

from sqlalchemy import text

from repositories.connection import get_transaction
from repositories.utils import rows_to_dicts
from models.pipeline_config import PipelineNodeRecord

_COLUMNS_JOIN = """
    pn.id::text AS id,
    pn.pipeline_id::text AS pipeline_id,
    pn.config_id::text AS config_id,
    c.config_name,
    c.table_name,
    c.json_data,
    c.config_type,
    pn.position_x, pn.position_y, pn.order_sort
"""


def bulk_insert(records: list[PipelineNodeRecord]) -> None:
    if not records:
        return
    with get_transaction() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_nodes (pipeline_id, config_id, position_x, position_y, order_sort)
                VALUES (:pipeline_id, :config_id, :position_x, :position_y, :order_sort)
            """),
            [
                {
                    "pipeline_id": rec.pipeline_id,
                    "config_id": rec.config_id,
                    "position_x": rec.position_x,
                    "position_y": rec.position_y,
                    "order_sort": rec.order_sort,
                }
                for rec in records
            ],
        )


def get_by_pipeline(pipeline_id: uuid.UUID) -> list[dict]:
    with get_transaction() as conn:
        result = conn.execute(
            text(f"""
                SELECT {_COLUMNS_JOIN}
                FROM pipeline_nodes pn
                JOIN configs c ON c.id = pn.config_id
                WHERE pn.pipeline_id = :pipeline_id
                  AND pn.is_deleted = false
                ORDER BY pn.order_sort ASC
            """),
            {"pipeline_id": pipeline_id},
        )
        return rows_to_dicts(result)


def get_pipelines_using_config(config_id: str) -> list[dict]:
    """Return distinct pipelines that reference config_id in nodes or edges."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT DISTINCT p.id::text AS pipeline_id,
                       COALESCE(NULLIF(p.name, ''), 'Unnamed Pipeline') AS pipeline_name
                FROM pipeline_nodes pn
                JOIN pipelines p ON pn.pipeline_id = p.id
                WHERE pn.config_id = :config_id
                  AND pn.is_deleted = false
                  AND p.is_deleted = false
                UNION
                SELECT DISTINCT p.id::text AS pipeline_id,
                       COALESCE(NULLIF(p.name, ''), 'Unnamed Pipeline') AS pipeline_name
                FROM pipeline_edges pe
                JOIN pipelines p ON pe.pipeline_id = p.id
                WHERE (pe.source_config_uuid = :config_id OR pe.target_config_uuid = :config_id)
                  AND p.is_deleted = false
                ORDER BY pipeline_name
            """),
            {"config_id": config_id},
        )
        return rows_to_dicts(result)


def get_nodes_by_pipeline_ids(pipeline_ids: list[str]) -> dict[str, list[dict]]:
    """Batch-fetch nodes for multiple pipelines. Returns {pipeline_id: [nodes]}."""
    if not pipeline_ids:
        return {}
    with get_transaction() as conn:
        result = conn.execute(
            text(f"""
                SELECT {_COLUMNS_JOIN}
                FROM pipeline_nodes pn
                JOIN configs c ON c.id = pn.config_id
                WHERE pn.pipeline_id::text = ANY(:ids)
                  AND pn.is_deleted = false
                ORDER BY pn.order_sort ASC
            """),
            {"ids": pipeline_ids},
        )
        grouped: dict[str, list[dict]] = {}
        for row in rows_to_dicts(result):
            grouped.setdefault(row["pipeline_id"], []).append(row)
        return grouped


def delete_by_pipeline(pipeline_id: uuid.UUID) -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("DELETE FROM pipeline_nodes WHERE pipeline_id = :pipeline_id"),
            {"pipeline_id": pipeline_id},
        )
        return result.rowcount
