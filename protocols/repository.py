"""
Repository protocols - Abstract interfaces for data access.

These protocols define the contract that all repositories must follow.
Controllers depend on these protocols, not concrete implementations.

Benefits:
- Testable: Can mock repositories for unit tests
- Flexible: Can swap implementations (PostgreSQL → MySQL)
- Type-safe: Static type checking with mypy
"""

from typing import Protocol, runtime_checkable
import pandas as pd
import uuid
from models.migration_config import ConfigRecord
from models.datasource import DatasourceRecord
from models.pipeline_config import PipelineRecord, PipelineRunRecord, PipelineRunUpdateRecord
from models.job import JobRecord, JobUpdateRecord


@runtime_checkable
class DatasourceRepository(Protocol):
    """Protocol for datasource CRUD operations."""

    def get_all(self) -> pd.DataFrame:
        """Get all datasources."""
        ...

    def get_by_id(self, id) -> dict | None:
        """Get datasource by ID."""
        ...

    def get_by_name(self, name: str) -> dict | None:
        """Get datasource by name."""
        ...

    def save(self, record: DatasourceRecord) -> tuple[bool, str]:
        """Insert new datasource. Pass DatasourceRecord."""
        ...

    def update(self, id, record: DatasourceRecord) -> tuple[bool, str]:
        """Update existing datasource by ID. Pass DatasourceRecord."""
        ...

    def delete(self, id) -> None:
        """Delete datasource."""
        ...


@runtime_checkable
class ConfigRepository(Protocol):
    """Protocol for config CRUD with versioning."""

    def save(self, record: ConfigRecord) -> tuple[bool, str]:
        """Save or update config with versioning. Pass ConfigRecord."""
        ...

    def get_list(self) -> pd.DataFrame:
        """Get all configs."""
        ...

    def get_content(self, config_name: str) -> dict | None:
        """Get config content by name."""
        ...

    def delete(self, config_name: str) -> tuple[bool, str]:
        """Delete config and history."""
        ...

    def get_history(self, config_name: str) -> pd.DataFrame:
        """Get version history."""
        ...

    def get_version(self, config_name: str, version: int) -> dict | None:
        """Get specific version."""
        ...

    def compare_versions(self, config_name: str, v1: int, v2: int) -> dict | None:
        """Compare two versions."""
        ...


@runtime_checkable
class PipelineRepository(Protocol):
    """Protocol for pipeline CRUD operations."""

    def save(self, record: PipelineRecord) -> tuple[bool, str]:
        """Upsert pipeline. Pass PipelineRecord."""
        ...

    def get_list(self) -> pd.DataFrame:
        """Get all pipelines."""
        ...

    def get_by_name(self, name: str) -> dict | None:
        """Get pipeline by name."""
        ...

    def delete(self, name: str) -> tuple[bool, str]:
        """Delete pipeline."""
        ...


@runtime_checkable
class PipelineRunRepository(Protocol):
    """Protocol for pipeline run tracking."""

    def save(self, record: PipelineRunRecord) -> uuid.UUID:
        """Insert new pipeline run. Pass PipelineRunRecord. Returns generated UUID."""
        ...

    def update(self, run_id: uuid.UUID, patch: PipelineRunUpdateRecord) -> None:
        """Patch pipeline run status/steps/error. Pass PipelineRunUpdateRecord."""
        ...

    def get_list(self, pipeline_id: uuid.UUID) -> pd.DataFrame:
        """Get all runs for a pipeline."""
        ...

    def get_latest(self, pipeline_id: uuid.UUID) -> dict | None:
        """Get latest run for a pipeline."""
        ...


@runtime_checkable
class JobRepository(Protocol):
    """Protocol for job CRUD operations."""

    def save(self, record: JobRecord) -> uuid.UUID:
        """Insert a new job. Returns generated UUID."""
        ...

    def update(self, job_id: uuid.UUID, patch: JobUpdateRecord) -> None:
        """Patch job status / error_message."""
        ...

    def get_by_id(self, job_id: uuid.UUID) -> dict | None:
        """Get job by UUID."""
        ...
