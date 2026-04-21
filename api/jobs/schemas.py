"""Pydantic schemas for jobs API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CreateJobSchema(BaseModel):
    pipeline_id: str = Field(..., description="UUID of the pipeline to execute")
    resume: bool = Field(
        False,
        description="Resume from last checkpoint instead of starting fresh. "
        "Stale 'running' jobs for this pipeline are marked failed automatically.",
    )


class UpdateJobSchema(BaseModel):
    """Jobs cannot be updated via API — this schema is a placeholder for BaseController."""


class JobCreatedResponse(BaseModel):
    job_id: str
    run_id: str
    pipeline_id: str
    status: str = "running"
