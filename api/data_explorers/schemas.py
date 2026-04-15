"""Pydantic schemas for Data Explorers API."""

from __future__ import annotations

import uuid
from pydantic import BaseModel, Field


class ExecuteQueryRequest(BaseModel):
    cmd: str = Field(
        ..., min_length=1, max_length=10000, description="SQL query to execute"
    )
    datasource_id: uuid.UUID = Field(..., description="UUID of the target datasource")
