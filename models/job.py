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
    """Write model for INSERT into jobs. Single source of truth for writable columns."""

    pipeline_id: uuid.UUID
    status: str = "running"


@dataclass
class JobUpdateRecord:
    """Write model for patching jobs status / error_message."""

    status: str
    error_message: str | None = None
