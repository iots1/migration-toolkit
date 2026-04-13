"""Pydantic schemas for jobs API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CreateJobSchema(BaseModel):
    pipeline_id: str = Field(..., description="UUID of the pipeline to execute")


class JobCreatedResponse(BaseModel):
    job_id: str
    run_id: str
    pipeline_id: str
    status: str = "running"
