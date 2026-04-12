"""Pydantic schemas for pipelines."""

from __future__ import annotations

import uuid
from pydantic import BaseModel, Field


class CreatePipelineSchema(BaseModel):
    """Create pipeline request."""

    name: str = Field(..., min_length=1)
    description: str = Field(default="")
    json_data: str | dict = Field(default="{}")
    error_strategy: str = Field(default="fail_fast")


class UpdatePipelineSchema(BaseModel):
    """Update pipeline request."""

    name: str | None = None
    description: str | None = None
    json_data: str | dict | None = None
    error_strategy: str | None = None


class PipelineSchema(BaseModel):
    """Pipeline response."""

    id: str
    name: str
    description: str
    error_strategy: str
    created_at: str
    updated_at: str
