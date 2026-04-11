"""Pydantic schemas for pipeline runs."""
from __future__ import annotations

from pydantic import BaseModel, Field


class CreatePipelineRunSchema(BaseModel):
    """Create pipeline run request."""

    pipeline_id: str = Field(..., min_length=1)
    status: str = Field(default="pending")
    steps_json: str | dict = Field(default="{}")


class UpdatePipelineRunSchema(BaseModel):
    """Update pipeline run request."""

    status: str | None = None
    steps_json: str | dict | None = None
    error_message: str | None = None


class PipelineRunSchema(BaseModel):
    """Pipeline run response."""

    id: str
    pipeline_id: str
    status: str
    started_at: str | None
    completed_at: str | None
    error_message: str | None
    created_at: str
