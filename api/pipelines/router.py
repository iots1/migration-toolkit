"""Pipelines API router."""

from __future__ import annotations

from api.base.controller import BaseController
from api.pipelines.service import PipelinesService
from api.pipelines.schemas import CreatePipelineSchema, UpdatePipelineSchema


def get_pipelines_router():
    """Get the pipelines router."""
    service = PipelinesService()
    controller = BaseController(
        prefix="pipelines",
        service=service,
        create_schema=CreatePipelineSchema,
        update_schema=UpdatePipelineSchema,
        tags=["Pipelines"],
        id_param="name",
    )
    return controller.router
