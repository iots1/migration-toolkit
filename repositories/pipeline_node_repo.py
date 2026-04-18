"""Pipeline node repository — CRUD for pipeline_nodes table."""

from __future__ import annotations

import uuid
from sqlalchemy import text
from repositories.connection import get_transaction
from models.pipeline_config import PipelineNodeRecord


def bulk_insert(records: list[PipelineNodeRecord]) -> None:
    for rec in records:
        with get_transaction() as conn:
            conn.execute(
                text("""
                    INSERT INTO pipeline_nodes (
                        pipeline_id, config_id, position_x, position_y, order_sort
                    ) VALUES (
                        :pipeline_id, :config_id, :position_x, :position_y, :order_sort
                    )
                """),
                {
                    "pipeline_id": rec.pipeline_id,
                    "config_id": rec.config_id,
                    "position_x": rec.position_x,
                    "position_y": rec.position_y,
                    "order_sort": rec.order_sort,
                },
            )


def get_by_pipeline(pipeline_id: uuid.UUID) -> list[dict]:
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id::text AS id,
                       pipeline_id::text AS pipeline_id,
                       config_id::text AS config_id,
                       position_x, position_y, order_sort
                FROM pipeline_nodes
                WHERE pipeline_id = :pipeline_id
                ORDER BY order_sort ASC
            """),
            {"pipeline_id": pipeline_id},
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]


def get_pipelines_using_config(config_id: str) -> list[dict]:
    """
    Get all pipelines that use a specific config (in nodes or edges).

    Args:
        config_id: Config UUID to check

    Returns:
        list[dict]: List of pipelines with usage details:
            [{
                "pipeline_id": "uuid",
                "pipeline_name": "name",
                "usage_type": "node" | "source_edge" | "target_edge"
            }]
    """
    with get_transaction() as conn:
        # Check pipeline_nodes
        result = conn.execute(
            text(
                """SELECT DISTINCT p.id::text,
                   COALESCE(NULLIF(p.name, ''), 'Unnamed Pipeline') as name
                   FROM pipeline_nodes pn
                   JOIN pipelines p ON pn.pipeline_id = p.id
                   WHERE pn.config_id = :config_id
                     AND pn.is_deleted = false
                     AND p.is_deleted = false
                   ORDER BY name"""
            ),
            {"config_id": config_id},
        )
        node_pipelines = [
            {"pipeline_id": row[0], "pipeline_name": row[1], "usage_type": "node"}
            for row in result.fetchall()
        ]

        # Check pipeline_edges (source_config_uuid)
        result = conn.execute(
            text(
                """SELECT DISTINCT p.id::text,
                   COALESCE(NULLIF(p.name, ''), 'Unnamed Pipeline') as name
                   FROM pipeline_edges pe
                   JOIN pipelines p ON pe.pipeline_id = p.id
                   WHERE pe.source_config_uuid = :config_id
                     AND p.is_deleted = false
                   ORDER BY name"""
            ),
            {"config_id": config_id},
        )
        source_edge_pipelines = [
            {"pipeline_id": row[0], "pipeline_name": row[1], "usage_type": "source_edge"}
            for row in result.fetchall()
        ]

        # Check pipeline_edges (target_config_uuid)
        result = conn.execute(
            text(
                """SELECT DISTINCT p.id::text,
                   COALESCE(NULLIF(p.name, ''), 'Unnamed Pipeline') as name
                   FROM pipeline_edges pe
                   JOIN pipelines p ON pe.pipeline_id = p.id
                   WHERE pe.target_config_uuid = :config_id
                     AND p.is_deleted = false
                   ORDER BY name"""
            ),
            {"config_id": config_id},
        )
        target_edge_pipelines = [
            {"pipeline_id": row[0], "pipeline_name": row[1], "usage_type": "target_edge"}
            for row in result.fetchall()
        ]

        # Combine all results
        return node_pipelines + source_edge_pipelines + target_edge_pipelines


def delete_by_pipeline(pipeline_id: uuid.UUID) -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("DELETE FROM pipeline_nodes WHERE pipeline_id = :pipeline_id"),
            {"pipeline_id": pipeline_id},
        )
        return result.rowcount
