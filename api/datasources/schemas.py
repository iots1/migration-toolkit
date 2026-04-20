"""Pydantic schemas for datasources."""

from __future__ import annotations

import uuid
from pydantic import BaseModel, Field


class CreateDatasourceSchema(BaseModel):
    """Create datasource request."""

    name: str = Field(..., min_length=1)
    db_type: str = Field(..., min_length=1)
    host: str = Field(default="", min_length=0)
    port: str = Field(default="", min_length=0)
    dbname: str = Field(default="", min_length=0)
    username: str = Field(default="", min_length=0)
    password: str = Field(default="", min_length=0)
    charset: str | None = None


class UpdateDatasourceSchema(BaseModel):
    """Update datasource request."""

    name: str | None = None
    db_type: str | None = None
    host: str | None = None
    port: str | None = None
    dbname: str | None = None
    username: str | None = None
    password: str | None = None
    charset: str | None = None


class DatasourceSchema(BaseModel):
    """Datasource response."""

    id: uuid.UUID
    name: str
    db_type: str
    host: str
    port: str
    dbname: str
    username: str
    charset: str | None = None
