"""
Database Module — Re-export Facade (Transition Layer).

This file provides backward compatibility by re-exporting functions from the new
PostgreSQL repositories. This facade will be removed once all callers are migrated
to use the repositories directly.

TODO: Remove this file after all imports are updated to use repositories directly.
"""

from __future__ import annotations  # Enable modern type hints

import pandas as pd
import uuid
from typing import Any

from models.datasource import DatasourceRecord
from models.pipeline_config import (
    PipelineRecord,
    PipelineRunRecord,
    PipelineRunUpdateRecord,
)
from models.migration_config import ConfigRecord

# Import the new PostgreSQL repositories
from repositories.datasource_repo import (
    get_all as _get_datasources,
    get_by_id as _get_ds_by_id,
    get_by_name as _get_ds_by_name,
    save as _save_ds,
    update as _update_ds,
    delete as _delete_ds,
)
from repositories.config_repo import (
    save as _save_config,
    get_list as _get_configs_list,
    get_content as _get_config_content,
    delete as _delete_config,
    get_history as _get_config_history,
    get_version as _get_config_version,
    compare_versions as _compare_config_versions,
)
from repositories.pipeline_repo import (
    save as _save_pipeline,
    get_list as _get_pipelines_list,
    get_by_name as _get_pipeline_by_name,
    delete as _delete_pipeline,
)
from repositories.pipeline_run_repo import (
    save as _save_pipeline_run,
    update as _update_pipeline_run,
    get_list as _get_pipeline_runs_list,
    get_latest as _get_latest_pipeline_run,
)
from repositories.base import init_db


# ---------------------------------------------------------------------------
# Datasource Functions (re-exported from repositories/datasource_repo.py)
# ---------------------------------------------------------------------------


def get_datasources() -> pd.DataFrame:
    """Get all datasources as DataFrame."""
    return _get_datasources()


def get_datasource_by_id(ds_id) -> dict | None:
    """Get datasource by ID."""
    return _get_ds_by_id(ds_id)


def get_datasource_by_name(name: str) -> dict | None:
    """Get datasource by name."""
    return _get_ds_by_name(name)


def save_datasource(
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str,
    charset: str | None = None,
) -> tuple[bool, str]:
    """Save a new datasource. Deprecated: use datasource_repo.save(DatasourceRecord)."""
    return _save_ds(
        DatasourceRecord(
            name=name,
            db_type=db_type,
            host=host,
            port=port,
            dbname=dbname,
            username=username,
            password=password,
            charset=charset,
        )
    )


def update_datasource(
    ds_id,
    name: str,
    db_type: str,
    host: str,
    port: str,
    dbname: str,
    username: str,
    password: str,
    charset: str | None = None,
) -> tuple[bool, str]:
    """Update an existing datasource. Deprecated: use datasource_repo.update(id, DatasourceRecord)."""
    return _update_ds(
        ds_id,
        DatasourceRecord(
            name=name,
            db_type=db_type,
            host=host,
            port=port,
            dbname=dbname,
            username=username,
            password=password,
            charset=charset,
        ),
    )


def delete_datasource(ds_id) -> None:
    """Delete a datasource."""
    _delete_ds(ds_id)


# ---------------------------------------------------------------------------
# Config Functions (re-exported from repositories/config_repo.py)
# ---------------------------------------------------------------------------


def get_configs_list() -> pd.DataFrame:
    """Get all configs as DataFrame."""
    return _get_configs_list()


def get_config_content(config_name: str) -> dict | None:
    """Get config content by name."""
    return _get_config_content(config_name)


def save_config_to_db(
    config_name: str,
    table_name: str,
    json_data: str,
    datasource_source_id=None,
    datasource_target_id=None,
    config_type="std",
    script=None,
    generate_sql=None,
    condition=None,
    lookup=None,
) -> tuple[bool, str]:
    """Save or update a config to the database.

    Deprecated: build a ConfigRecord and call config_repo.save(record) directly.
    """
    from models.migration_config import ConfigRecord

    record = ConfigRecord(
        config_name=config_name,
        table_name=table_name,
        json_data=json_data,
        datasource_source_id=datasource_source_id,
        datasource_target_id=datasource_target_id,
        config_type=config_type,
        script=script,
        generate_sql=generate_sql,
        condition=condition,
        lookup=lookup,
    )
    return _save_config(record)


def delete_config(config_name: str) -> tuple[bool, str]:
    """Delete a config."""
    return _delete_config(config_name)


def get_config_history(config_name: str) -> pd.DataFrame:
    """Get version history for a config."""
    return _get_config_history(config_name)


def get_config_version(config_name: str, version: int) -> dict | None:
    """Get specific version of a config."""
    return _get_config_version(config_name, version)


def compare_config_versions(config_name: str, v1: int, v2: int) -> dict | None:
    """Compare two versions of a config."""
    return _compare_config_versions(config_name, v1, v2)


# ---------------------------------------------------------------------------
# Pipeline Functions (re-exported from repositories/pipeline_repo.py)
# ---------------------------------------------------------------------------


def get_pipelines() -> pd.DataFrame:
    """Get all pipelines as DataFrame."""
    return _get_pipelines_list()


def get_pipeline_by_name(name: str) -> dict | None:
    """Get pipeline by name."""
    return _get_pipeline_by_name(name)


def save_pipeline(
    name: str,
    description: str,
    json_data: str,
    source_ds_id: int,
    target_ds_id: int,
    error_strategy: str,
) -> tuple[bool, str]:
    """Save a new pipeline. Deprecated: use pipeline_repo.save(PipelineRecord)."""
    return _save_pipeline(
        PipelineRecord(
            name=name,
            description=description,
            json_data=json_data,
            error_strategy=error_strategy,
        )
    )


def delete_pipeline(name: str) -> tuple[bool, str]:
    """Delete a pipeline."""
    return _delete_pipeline(name)


# ---------------------------------------------------------------------------
# Pipeline Run Functions (re-exported from repositories/pipeline_run_repo.py)
# ---------------------------------------------------------------------------


def get_pipeline_runs(pipeline_id: str) -> pd.DataFrame:
    """Get runs for a pipeline."""
    return _get_pipeline_runs_list(pipeline_id)


def save_pipeline_run(pipeline_id: str, status: str, steps_json: str) -> str:
    """Save a new pipeline run. Deprecated: use pipeline_run_repo.save(PipelineRunRecord)."""
    return _save_pipeline_run(
        PipelineRunRecord(
            pipeline_id=uuid.UUID(pipeline_id)
            if isinstance(pipeline_id, str)
            else pipeline_id,
            status=status,
            steps_json=steps_json,
        )
    )


def update_pipeline_run(
    run_id: str,
    status: str,
    steps_json: str | None = None,
    error_message: str | None = None,
) -> None:
    """Update pipeline run status. Deprecated: use pipeline_run_repo.update(run_id, PipelineRunUpdateRecord)."""
    _update_pipeline_run(
        uuid.UUID(run_id) if isinstance(run_id, str) else run_id,
        PipelineRunUpdateRecord(
            status=status,
            steps_json=steps_json,
            error_message=error_message,
        ),
    )


def get_latest_run(pipeline_id: str) -> dict | None:
    """Get latest run for a pipeline."""
    return _get_latest_pipeline_run(pipeline_id)


# ---------------------------------------------------------------------------
# Database Initialization
# ---------------------------------------------------------------------------


def init_db() -> None:
    """Initialize database schema."""
    from repositories.base import init_db as _init_db

    _init_db()


# ---------------------------------------------------------------------------
# Legacy Compatibility Functions (deprecated)
# ---------------------------------------------------------------------------

# These functions may have been used in old code but are now deprecated
# They are kept here for backward compatibility during migration


def get_connection():
    """Deprecated: Use repositories.connection.get_engine() instead."""
    raise NotImplementedError(
        "get_connection() is deprecated. "
        "Use repositories.connection.get_engine() for SQLAlchemy connections."
    )


def ensure_config_histories_table():
    """Deprecated: Table schemas are now managed by repositories/base.py."""
    pass  # No-op, schema is created by init_db()
