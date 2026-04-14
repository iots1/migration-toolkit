"""
Job repository — CRUD operations for the jobs table.

Each job represents one triggered pipeline execution request.
pipeline_runs.job_id → jobs.id (FK).
"""

from __future__ import annotations

import uuid
from sqlalchemy import text
from repositories.connection import get_transaction
from models.job import JobRecord, JobUpdateRecord


def save(record: JobRecord) -> uuid.UUID:
    """
    Insert a new job. Returns generated UUID.

    Pass a JobRecord with:
    - pipeline_id: The pipeline being executed
    - status: Initial status (usually 'running')
    - total_config: Number of configs in the pipeline
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                INSERT INTO jobs (pipeline_id, status, total_config)
                VALUES (:pipeline_id, :status, :total_config)
                RETURNING id
            """),
            {
                "pipeline_id": record.pipeline_id,
                "status": record.status,
                "total_config": record.total_config,
            },
        )
        return result.scalar()


def update(job_id: uuid.UUID, patch: JobUpdateRecord) -> None:
    """
    Patch job (status, error_message, total_config).

    Sets completed_at on terminal status (completed, failed, partial).
    Only updates fields present in the patch record.
    """
    with get_transaction() as conn:
        conn.execute(
            text("""
                UPDATE jobs
                SET status = :status,
                    error_message = COALESCE(:error_message, error_message),
                    total_config = COALESCE(:total_config, total_config),
                    completed_at = CASE WHEN :status IN ('completed', 'failed', 'partial')
                        THEN CURRENT_TIMESTAMP ELSE completed_at END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {
                "id": job_id,
                "status": patch.status,
                "error_message": patch.error_message,
                "total_config": patch.total_config,
            },
        )


def get_by_id(job_id: uuid.UUID) -> dict | None:
    """Get a job by its UUID."""
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        data = dict(zip(result.keys(), row))
        data["id"] = str(data["id"])
        data["pipeline_id"] = str(data["pipeline_id"])
        return data


def get_by_pipeline(pipeline_id: uuid.UUID, limit: int = 50) -> list[dict]:
    """Get recent jobs for a pipeline, newest first. Includes total_config."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                SELECT id, pipeline_id, status, completed_at,
                       error_message, total_config, created_at
                FROM jobs
                WHERE pipeline_id = :pipeline_id
                ORDER BY created_at DESC
                LIMIT :limit
            """),
            {"pipeline_id": pipeline_id, "limit": limit},
        )
        rows = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            data["id"] = str(data["id"])
            data["pipeline_id"] = str(data["pipeline_id"])
            rows.append(data)
        return rows
