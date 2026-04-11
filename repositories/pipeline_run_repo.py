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


def save(pipeline_id: uuid.UUID, status: str, steps_json: str) -> uuid.UUID:
    """
    Save a new pipeline run. Thread-safe.

    Each call creates a new run record with a generated UUID.

    Args:
        pipeline_id: Pipeline UUID (references pipelines.id)
        status: Initial status ("pending", "running", etc.)
        steps_json: Pipeline steps as JSON string

    Returns:
        uuid.UUID: Generated run ID

    Example:
        >>> import uuid
        >>> pipeline_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> run_id = save(pipeline_id, "pending", '{"steps": []}')
        >>> print(f"Created run: {run_id}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            INSERT INTO pipeline_runs (pipeline_id, status, started_at, steps_json)
            VALUES (:pipeline_id, :status, CURRENT_TIMESTAMP, :steps_json)
            RETURNING id
        """),
            {"pipeline_id": pipeline_id, "status": status, "steps_json": steps_json},
        )
        return result.scalar()


def update(
    run_id: uuid.UUID,
    status: str,
    steps_json: str | None = None,
    error_message: str | None = None,
) -> None:
    """
    Update pipeline run status. Thread-safe.

    Automatically sets completed_at when status is terminal
    (completed, failed, partial).

    Args:
        run_id: Run UUID to update
        status: New status
        steps_json: Updated steps JSON (optional)
        error_message: Error message if failed (optional)

    Example:
        >>> import uuid
        >>> run_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> update(run_id, "running")
        >>> update(run_id, "completed", steps_json='{"steps": [...]}')
        >>> update(run_id, "failed", error_message="Connection timeout")
    """
    with get_transaction() as conn:
        conn.execute(
            text("""
            UPDATE pipeline_runs
            SET status = :status,
                steps_json = COALESCE(:steps_json, steps_json),
                error_message = COALESCE(:error_message, error_message),
                completed_at = CASE WHEN :status IN ('completed','failed','partial')
                    THEN CURRENT_TIMESTAMP ELSE completed_at END
            WHERE id = :id
        """),
            {
                "id": run_id,
                "status": status,
                "steps_json": steps_json,
                "error_message": error_message,
            },
        )


def get_all(limit: int = 1000, offset: int = 0) -> list[dict]:
    import json as _json
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, status, started_at, completed_at, steps_json,
                   error_message, created_at, created_by, updated_at, updated_by,
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
            raw = data.get("steps_json")
            try:
                data["steps_json"] = _json.loads(raw) if isinstance(raw, str) else (raw or {})
            except (_json.JSONDecodeError, TypeError):
                data["steps_json"] = {}
            runs.append(data)
        return runs


def count_all() -> int:
    with get_transaction() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs"))
        return result.scalar()


def get_list(pipeline_id: uuid.UUID) -> pd.DataFrame:
    """
    Get all runs for a pipeline.

    Args:
        pipeline_id: Pipeline UUID

    Returns:
        pd.DataFrame: Pipeline runs with columns:
            [id, pipeline_id, status, started_at, completed_at, error_message, created_at]
        Ordered by started_at descending (newest first)

    Example:
        >>> import uuid
        >>> pipeline_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> runs = get_list(pipeline_id)
        >>> print(runs)
    """
    with get_transaction() as conn:
        return pd.read_sql(
            text("""
            SELECT id, pipeline_id, status, started_at, completed_at, error_message, created_at
            FROM pipeline_runs
            WHERE pipeline_id = :pipeline_id
            ORDER BY started_at DESC
        """),
            conn,
            params={"pipeline_id": pipeline_id},
        )


def get_latest(pipeline_id: uuid.UUID) -> dict | None:
    """
    Get the latest run for a pipeline.

    Args:
        pipeline_id: Pipeline UUID

    Returns:
        dict | None: Latest run data or None if no runs exist
            {
                "id": <uuid>,
                "pipeline_id": <uuid>,
                "status": <str>,
                "started_at": <timestamp>,
                "completed_at": <timestamp|None>,
                "steps_json": <str>,
                "error_message": <str|None>,
                "created_at": <timestamp>
            }

    Example:
        >>> import uuid
        >>> pipeline_id = uuid.UUID("123e4567-e89b-12d3-a456-426614174000")
        >>> latest = get_latest(pipeline_id)
        >>> if latest:
        ...     print(f"Latest status: {latest['status']}")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, status, started_at, completed_at, steps_json, error_message, created_at
            FROM pipeline_runs
            WHERE pipeline_id = :pipeline_id
            ORDER BY started_at DESC
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


def get_running_runs() -> list[dict]:
    """
    Get all currently running pipeline runs.

    Returns:
        list[dict]: List of running runs

    Example:
        >>> running = get_running_runs()
        >>> print(f"Currently running: {len(running)} pipelines")
    """
    with get_transaction() as conn:
        result = conn.execute(
            text("""
            SELECT id, pipeline_id, status, started_at
            FROM pipeline_runs
            WHERE status = 'running'
            ORDER BY started_at ASC
        """)
        )
        runs = []
        for row in result.fetchall():
            data = dict(zip(result.keys(), row))
            data["id"] = str(data["id"])
            data["pipeline_id"] = str(data["pipeline_id"])
            runs.append(data)
        return runs
