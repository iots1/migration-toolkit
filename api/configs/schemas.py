"""Pydantic schemas for configs."""
from __future__ import annotations

from pydantic import BaseModel, Field


class CreateConfigSchema(BaseModel):
    """Create config request."""

    config_name: str = Field(..., min_length=1)
    table_name: str = Field(default="")
    json_data: str | dict = Field(default="{}")


class UpdateConfigSchema(BaseModel):
    """Update config request."""

    config_name: str | None = None
    table_name: str | None = None
    json_data: str | dict | None = None


class ConfigSchema(BaseModel):
    """Config response."""

    id: str
    config_name: str
    table_name: str
    updated_at: str
