"""Pydantic schemas for pipelines."""

from __future__ import annotations

from pydantic import BaseModel, Field


class PipelineNodeSchema(BaseModel):
    config_id: str
    position_x: int = 0
    position_y: int = 0
    order_sort: int = 0


class PipelineEdgeSchema(BaseModel):
    source_config_uuid: str
    target_config_uuid: str


class CreatePipelineSchema(BaseModel):
    name: str = Field(default="")
    description: str = Field(default="")
    nodes: list[PipelineNodeSchema] | None = None
    edges: list[PipelineEdgeSchema] | None = None


class UpdatePipelineSchema(BaseModel):
    name: str | None = None
    description: str | None = None
    nodes: list[PipelineNodeSchema] | None = None
    edges: list[PipelineEdgeSchema] | None = None


class PipelineSchema(BaseModel):
    id: str
    name: str
    description: str
    created_at: str
    updated_at: str
    nodes: list[dict] = Field(default_factory=list)
    edges: list[dict] = Field(default_factory=list)
