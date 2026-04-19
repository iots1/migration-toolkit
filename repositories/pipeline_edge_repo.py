"""Pipeline edge repository — CRUD for pipeline_edges table."""

from __future__ import annotations

import uuid
from sqlalchemy import text
from repositories.connection import get_transaction
from models.pipeline_config import PipelineEdgeRecord


def bulk_insert(records: list[PipelineEdgeRecord]) -> None:
    if not records:
        return
    with get_transaction() as conn:
        conn.execute(
            text("""
                INSERT INTO pipeline_edges (
                    pipeline_id, source_config_uuid, target_config_uuid
                ) VALUES (
                    :pipeline_id, :source_config_uuid, :target_config_uuid
                )
            """),
            [
                {
                    "pipeline_id": rec.pipeline_id,
                    "source_config_uuid": rec.source_config_uuid,
                    "target_config_uuid": rec.target_config_uuid,
                }
                for rec in records
            ],
        )


def get_by_pipeline(pipeline_id: uuid.UUID) -> list[dict]:
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id::text AS id,
                       pipeline_id::text AS pipeline_id,
                       source_config_uuid::text AS source_config_uuid,
                       target_config_uuid::text AS target_config_uuid
                FROM pipeline_edges
                WHERE pipeline_id = :pipeline_id
            """),
            {"pipeline_id": pipeline_id},
        )
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]


def get_edges_by_pipeline_ids(pipeline_ids: list[str]) -> dict[str, list[dict]]:
    """Batch-fetch edges for multiple pipelines. Returns {pipeline_id: [edges]}."""
    if not pipeline_ids:
        return {}
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id::text AS id,
                       pipeline_id::text AS pipeline_id,
                       source_config_uuid::text AS source_config_uuid,
                       target_config_uuid::text AS target_config_uuid
                FROM pipeline_edges
                WHERE pipeline_id::text = ANY(:ids)
            """),
            {"ids": pipeline_ids},
        )
        grouped: dict[str, list[dict]] = {}
        for row in result.fetchall():
            d = dict(zip(result.keys(), row))
            grouped.setdefault(d["pipeline_id"], []).append(d)
        return grouped


def delete_by_pipeline(pipeline_id: uuid.UUID) -> int:
    with get_transaction() as conn:
        result = conn.execute(
            text("DELETE FROM pipeline_edges WHERE pipeline_id = :pipeline_id"),
            {"pipeline_id": pipeline_id},
        )
        return result.rowcount
