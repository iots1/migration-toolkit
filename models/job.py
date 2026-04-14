"""
Job domain models.

JobRecord:        write model for INSERT into jobs table.
JobUpdateRecord:  write model for patching job status / error.
"""

from __future__ import annotations
from dataclasses import dataclass
import uuid


@dataclass
class JobRecord:
    """
    Write model for INSERT into jobs.

    Single source of truth for writable columns.
    total_config: Number of configs in the pipeline being executed.
    """

    pipeline_id: uuid.UUID
    status: str = "running"
    total_config: int = 0


@dataclass
class JobUpdateRecord:
    """Write model for patching jobs (status, error_message, total_config)."""

    status: str
    error_message: str | None = None
    total_config: int | None = None
