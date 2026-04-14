"""
Pipeline Run repository - Thread-safe operations for pipeline_runs table.

This module handles pipeline execution tracking with:
- Thread-safe writes (each operation gets its own transaction)
- UUID primary keys
- Status tracking (pending, running, completed, failed, partial)
- Automatic timestamp management
"""

from __future__ import annotations  # Enable modern type hints

import uuid
import pandas as pd
from sqlalchemy import text
from repositories.connection import get_transaction
from models.pipeline_config import PipelineRunRecord, PipelineRunUpdateRecord


def save(record: PipelineRunRecord) -> uuid.UUID:
    """
    Insert a new batch record into pipeline_runs.

    Each batch execution creates one record with flat columns.
    Example: config 'import_patients', batch 0, inserted 1000 rows, cumulative 1000.

    Pass a PipelineRunRecord with:
    - pipeline_id: Pipeline UUID
    - config_name: Config name (e.g., 'import_patients')
    - batch_round: Batch number (0=first, 1=second, ...)
    - rows_in_batch: Rows inserted in this batch
    - rows_cumulative: Total rows from batch 0 to this batch
    - batch_size: Row batch size
    - total_records_in_config: Total records in this config
    - status: 'success' or 'failed'
    - job_id: Optional link to jobs table
    - error_message: Error text if failed
    - transformation_warnings: Warnings as JSON string or semicolon-delimited

    Returns: Generated UUID for this batch record
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
                INSERT INTO pipeline_runs
                (pipeline_id, job_id, config_name, batch_round,
                 rows_in_batch, rows_cumulative, batch_size, total_records_in_config,
                 status, error_message, transformation_warnings)
                VALUES (:pipeline_id, :job_id, :config_name, :batch_round,
                        :rows_in_batch, :rows_cumulative, :batch_size, :total_records_in_config,
                        :status, :error_message, :transformation_warnings)
                RETURNING id
            """),
            {
                "pipeline_id": record.pipeline_id,
                "job_id": record.job_id,
                "config_name": record.config_name,
                "batch_round": record.batch_round,
                "rows_in_batch": record.rows_in_batch,
                "rows_cumulative": record.rows_cumulative,
                "batch_size": record.batch_size,
                "total_records_in_config": record.total_records_in_config,
                "status": record.status,
                "error_message": record.error_message,
                "transformation_warnings": record.transformation_warnings,
            },
        )
        return result.scalar()


def update(run_id: uuid.UUID, patch: PipelineRunUpdateRecord) -> None:
    """
    Patch batch record (status and/or error_message).

    Since each batch is a separate INSERT record, updates are minimal.
    Mainly used to change status or add error details after insert.
    """
    with get_transaction() as conn:
        conn.execute(
            text("""
                UPDATE pipeline_runs
                SET status = :status,
                    error_message = COALESCE(:error_message, error_message),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {
                "id": run_id,
                "status": patch.status,
                "error_message": patch.error_message,
            },
        )


def get_all(limit: int = 1000, offset: int = 0) -> list[dict]:
    """Get all batch records, newest first."""
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, job_id, config_name, batch_round,
                   rows_in_batch, rows_cumulative, batch_size, total_records_in_config,
                   status, error_message, transformation_warnings,
                   created_at, created_by, updated_at, updated_by,
                   is_deleted, deleted_at, deleted_by, deleted_reason
            FROM pipeline_runs
            ORDER BY created_at DESC
            LIMIT :limit OFFSET :offset
        """),
            {"limit": limit, "offset": offset},
        )
        runs = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            data["id"] = str(data["id"])
            data["pipeline_id"] = str(data["pipeline_id"])
            if data.get("job_id"):
                data["job_id"] = str(data["job_id"])
            runs.append(data)
        return runs


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs"))
        return result.scalar()


def get_list(pipeline_id: uuid.UUID) -> pd.DataFrame:
    """
    Get all batch records for a pipeline.

    Args:
        pipeline_id: Pipeline UUID

    Returns:
        pd.DataFrame: Batch records with columns:
            [id, pipeline_id, job_id, config_name, batch_round, rows_in_batch,
             rows_cumulative, batch_size, total_records_in_config, status,
             error_message, created_at]
        Ordered by created_at descending (newest first)

    Example:
        >>> import uuid
        >>> pipeline_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> batch_records = get_list(pipeline_id)
        >>> print(f"{len(batch_records)} batches executed")
    """
    with get_transaction() as conn:
        return pd.read_sql(
            text("""
            SELECT id, pipeline_id, job_id, config_name, batch_round, rows_in_batch,
                   rows_cumulative, batch_size, total_records_in_config, status,
                   error_message, created_at
            FROM pipeline_runs
            WHERE pipeline_id = :pipeline_id
            ORDER BY created_at DESC
        """),
            conn,
            params={"pipeline_id": pipeline_id},
        )


def get_latest(pipeline_id: uuid.UUID) -> dict | None:
    """
    Get the latest batch record for a pipeline.

    Args:
        pipeline_id: Pipeline UUID

    Returns:
        dict | None: Latest batch record or None if no records exist
            {
                "id": <uuid>,
                "pipeline_id": <uuid>,
                "job_id": <uuid|None>,
                "config_name": <str>,
                "batch_round": <int>,
                "rows_in_batch": <int>,
                "rows_cumulative": <int>,
                "batch_size": <int>,
                "total_records_in_config": <int>,
                "status": <str>,
                "error_message": <str|None>,
                "transformation_warnings": <str|None>,
                "created_at": <timestamp>
            }

    Example:
        >>> import uuid
        >>> pipeline_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> latest = get_latest(pipeline_id)
        >>> if latest:
        ...     print(f"Latest batch: {latest['config_name']} batch {latest['batch_round']}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, job_id, config_name, batch_round, rows_in_batch,
                   rows_cumulative, batch_size, total_records_in_config, status,
                   error_message, transformation_warnings, created_at
            FROM pipeline_runs
            WHERE pipeline_id = :pipeline_id
            ORDER BY created_at DESC
            LIMIT 1
        """),
            {"pipeline_id": pipeline_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        data = dict(zip(columns, row))
        # Convert UUIDs to strings
        data["id"] = str(data["id"])
        data["pipeline_id"] = str(data["pipeline_id"])
        if data.get("job_id"):
            data["job_id"] = str(data["job_id"])
        return data


def get_by_id(run_id: uuid.UUID) -> dict | None:
    """
    Get a specific pipeline run by ID.

    Args:
        run_id: Run UUID

    Returns:
        dict | None: Run data or None if not found

    Example:
        >>> import uuid
        >>> run_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> run = get_by_id(run_id)
        >>> if run:
        ...     print(f"Status: {run['status']}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT * FROM pipeline_runs WHERE id = :id"), {"id": run_id}
        )
        row = result.fetchone()
        if row is None:
            return None
        columns = result.keys()
        data = dict(zip(columns, row))
        data["id"] = str(data["id"])
        data["pipeline_id"] = str(data["pipeline_id"])
        return data


def get_by_job(job_id: uuid.UUID) -> list[dict]:
    """
    Get all batch records for a specific job.

    Args:
        job_id: Job UUID

    Returns:
        list[dict]: All batch records for this job, ordered by created_at

    Example:
        >>> import uuid
        >>> job_id = uuid.UUID("...")
        >>> batches = get_by_job(job_id)
        >>> print(f"Job executed {len(batches)} batches")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, job_id, config_name, batch_round, rows_in_batch,
                   rows_cumulative, batch_size, total_records_in_config, status,
                   error_message, transformation_warnings, created_at
            FROM pipeline_runs
            WHERE job_id = :job_id
            ORDER BY created_at ASC
        """),
            {"job_id": job_id},
        )
        batches = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            data["id"] = str(data["id"])
            data["pipeline_id"] = str(data["pipeline_id"])
            if data.get("job_id"):
                data["job_id"] = str(data["job_id"])
            batches.append(data)
        return batches
