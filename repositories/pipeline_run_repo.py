"""
Pipeline Run repository - Thread-safe operations for pipeline_runs table.
"""
from __future__ import annotations

import uuid

import pandas as pd
from sqlalchemy import text

from repositories.connection import get_transaction
from repositories.utils import row_to_dict, rows_to_dicts
from models.pipeline_config import PipelineRunRecord, PipelineRunUpdateRecord

_COLUMNS = """
    id::text AS id, pipeline_id::text AS pipeline_id,
    CASE WHEN job_id IS NOT NULL THEN job_id::text ELSE NULL END AS job_id,
    config_name, batch_round, rows_in_batch, rows_cumulative, batch_size,
    total_records_in_config, status, error_message, transformation_warnings,
    created_at, created_by, updated_at, updated_by,
    is_deleted, deleted_at, deleted_by, deleted_reason
"""


def save(record: PipelineRunRecord) -> uuid.UUID:
    """Insert a new batch record. Returns the generated UUID."""
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
    """Patch batch record (status and/or error_message)."""
    with get_transaction() as conn:
        conn.execute(
            text("""
                UPDATE pipeline_runs
                SET status = :status,
                    error_message = COALESCE(:error_message, error_message),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """),
            {"id": run_id, "status": patch.status, "error_message": patch.error_message},
        )


def get_all(limit: int = 1000, offset: int = 0) -> list[dict]:
    """Get all batch records, newest first."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs ORDER BY created_at DESC LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset},
        )
        return rows_to_dicts(result)


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs"))
        return result.scalar()


def get_list(pipeline_id: uuid.UUID) -> pd.DataFrame:
    """Get all batch records for a pipeline as a DataFrame (used by Streamlit views)."""
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
    """Get the latest batch record for a pipeline."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs WHERE pipeline_id = :pipeline_id ORDER BY created_at DESC LIMIT 1"),
            {"pipeline_id": pipeline_id},
        )
        return row_to_dict(result)


def get_by_id(run_id: uuid.UUID) -> dict | None:
    """Get a specific pipeline run by ID."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs WHERE id = :id"),
            {"id": run_id},
        )
        return row_to_dict(result)


def get_by_job(job_id: uuid.UUID) -> list[dict]:
    """Get all batch records for a specific job, ordered by created_at."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs WHERE job_id = :job_id ORDER BY created_at ASC"),
            {"job_id": job_id},
        )
        return rows_to_dicts(result)


def get_running_runs() -> list[dict]:
    """Get all currently running pipeline runs."""
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs WHERE status = 'running' ORDER BY created_at DESC")
        )
        return rows_to_dicts(result)


def get_page(limit: int = 25, offset: int = 0, job_id: uuid.UUID | None = None) -> list[dict]:
    """Get a page of batch records with optional job_id filter."""
    if job_id:
        with get_transaction() as conn:
            result = conn.execute(
                text(f"SELECT {_COLUMNS} FROM pipeline_runs WHERE job_id = :job_id ORDER BY created_at DESC LIMIT :limit OFFSET :offset"),
                {"job_id": job_id, "limit": limit, "offset": offset},
            )
            return rows_to_dicts(result)
    with get_transaction() as conn:
        result = conn.execute(
            text(f"SELECT {_COLUMNS} FROM pipeline_runs ORDER BY created_at DESC LIMIT :limit OFFSET :offset"),
            {"limit": limit, "offset": offset},
        )
        return rows_to_dicts(result)


def count_by_job(job_id: uuid.UUID) -> int:
    """Count pipeline_runs for a specific job."""
    with get_transaction() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM pipeline_runs WHERE job_id = :job_id"),
            {"job_id": job_id},
        )
        return result.scalar()


def save_batch(records: list[PipelineRunRecord]) -> list[uuid.UUID]:
    """Bulk insert multiple batch records in a single transaction."""
    if not records:
        return []
    ids = []
    with get_transaction() as conn:
        for record in records:
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
            ids.append(result.scalar())
    return ids
