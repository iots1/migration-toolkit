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


@runtime_checkable
class DatasourceRepository(Protocol):
    """Protocol for datasource CRUD operations."""

    def get_all(self) -> pd.DataFrame:
        """Get all datasources."""
        ...

    def get_by_id(self, id: int) -> dict | None:
        """Get datasource by ID."""
        ...

    def get_by_name(self, name: str) -> dict | None:
        """Get datasource by name."""
        ...

    def save(
        self,
        name: str,
        db_type: str,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str
    ) -> tuple[bool, str]:
        """Save new datasource."""
        ...

    def update(
        self,
        id: int,
        name: str,
        db_type: str,
        host: str,
        port: str,
        dbname: str,
        username: str,
        password: str
    ) -> tuple[bool, str]:
        """Update existing datasource."""
        ...

    def delete(self, id: int) -> None:
        """Delete datasource."""
        ...


@runtime_checkable
class ConfigRepository(Protocol):
    """Protocol for config CRUD with versioning."""

    def save(self, config_name: str, table_name: str, json_data: str) -> tuple[bool, str]:
        """Save or update config with versioning."""
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

    def save(
        self,
        name: str,
        description: str,
        json_data: str,
        source_ds_id: int | None,
        target_ds_id: int | None,
        error_strategy: str
    ) -> tuple[bool, str]:
        """Save or update pipeline."""
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

    def save(self, pipeline_id: uuid.UUID, status: str, steps_json: str) -> uuid.UUID:
        """Save new pipeline run."""
        ...

    def update(
        self,
        run_id: uuid.UUID,
        status: str,
        steps_json: str | None = None,
        error_message: str | None = None
    ) -> None:
        """Update pipeline run status."""
        ...

    def get_list(self, pipeline_id: uuid.UUID) -> pd.DataFrame:
        """Get all runs for a pipeline."""
        ...

    def get_latest(self, pipeline_id: uuid.UUID) -> dict | None:
        """Get latest run for a pipeline."""
        ...
