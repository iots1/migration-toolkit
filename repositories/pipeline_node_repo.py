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


def delete_by_pipeline(pipeline_id: uuid.UUID) -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("DELETE FROM pipeline_nodes WHERE pipeline_id = :pipeline_id"),
            {"pipeline_id": pipeline_id},
        )
        return result.rowcount
