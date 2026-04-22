"""Job repository — CRUD operations for the jobs table."""
from __future__ import annotations

import uuid

from sqlalchemy import text

from repositories.connection import get_transaction
from repositories.utils import row_to_dict, rows_to_dicts
from models.job import JobRecord, JobUpdateRecord

_COLUMNS = """
    id::text AS id, pipeline_id::text AS pipeline_id,
    status, completed_at, error_message, total_config,
    last_heartbeat, summary, created_at
"""


def save(record: JobRecord) -> uuid.UUID:
    """Insert a new job. Returns the generated UUID."""
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
    """Patch job status, error_message, total_config, summary, last_heartbeat."""
    import json as _json

    summary_json = _json.dumps(patch.summary) if patch.summary else None

    with get_transaction() as conn:
        conn.execute(
            text("""
                UPDATE jobs
                SET status = :status,
                    error_message = COALESCE(:error_message, error_message),
                    total_config = COALESCE(:total_config, total_config),
                    summary = COALESCE(CAST(:summary AS jsonb), summary),
                    last_heartbeat = COALESCE(CAST(:last_heartbeat AS timestamptz), last_heartbeat),
                    completed_at = CASE WHEN :status IN ('completed', 'failed', 'partial', 'interrupted')
                        THEN CURRENT_TIMESTAMP ELSE completed_at END,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {
                "id": job_id,
                "status": patch.status,
                "error_message": patch.error_message,
                "total_config": patch.total_config,
                "summary": summary_json,
                "last_heartbeat": patch.last_heartbeat,
            },
        )


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM jobs"))
        return result.scalar()


def get_all(limit: int = 50, offset: int = 0) -> list[dict]:
    """Get all jobs, newest first."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM jobs ORDER BY created_at DESC LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset},
        )
        return rows_to_dicts(result)


def get_by_id(job_id: uuid.UUID) -> dict | None:
    """Get a job by its UUID."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        return row_to_dict(result)


def get_by_pipeline(pipeline_id: uuid.UUID, limit: int = 50) -> list[dict]:
    """Get recent jobs for a pipeline, newest first."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM jobs WHERE pipeline_id = :pipeline_id ORDER BY created_at DESC LIMIT :limit"),
            {"pipeline_id": pipeline_id, "limit": limit},
        )
        return rows_to_dicts(result)
