"""Pydantic schemas for configs API.

Field list mirrors ConfigRecord in models/migration_config.py.
When a new column is added to ConfigRecord, add the field here too.
"""
from __future__ import annotations

from pydantic import BaseModel, Field


class CreateConfigSchema(BaseModel):
    """Create config request."""

    config_name: str = Field(..., min_length=1)
    table_name: str = Field(default="")
    json_data: str | dict = Field(default="{}")
    datasource_source_id: str | None = None
    datasource_target_id: str | None = None
    config_type: str = Field(default="std")
    script: str | None = None
    generate_sql: str | None = None
    condition: str | None = None
    lookup: str | None = None
    pk_columns: str | None = None


class UpdateConfigSchema(BaseModel):
    """Update config request (all fields optional — patch semantics)."""

    config_name: str | None = None
    table_name: str | None = None
    json_data: str | dict | None = None
    datasource_source_id: str | None = None
    datasource_target_id: str | None = None
    config_type: str | None = None
    script: str | None = None
    generate_sql: str | None = None
    condition: str | None = None
    lookup: str | None = None
    pk_columns: str | None = None


class ConfigSchema(BaseModel):
    """Config response."""

    id: str
    config_name: str
    table_name: str
    updated_at: str
    datasource_source_id: str | None
    datasource_target_id: str | None
    datasource_source_name: str | None
    datasource_source_db_type: str | None
    datasource_source_dbname: str | None
    datasource_target_name: str | None
    datasource_target_db_type: str | None
    datasource_target_dbname: str | None
