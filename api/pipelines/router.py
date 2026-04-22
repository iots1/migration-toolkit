"""Pipelines API router."""

from __future__ import annotations

from fastapi import Request

from api.base.controller import BaseController
from api.base import json_api
from api.pipelines.service import PipelinesService
from api.pipelines.schemas import CreatePipelineSchema, UpdatePipelineSchema


class PipelinesController(BaseController):

    def _register_pre_routes(self) -> None:
        svc = self.service

        def list_jobs(pipeline_id: str, request: Request):
            rows = svc.find_jobs(pipeline_id)
            return json_api.create_collection_response(
                "jobs", rows, str(request.url.path)
            )

        def duplicate_pipeline(id: str, request: Request):
            duplicated = svc.duplicate(id)
            return json_api.create_created_response(
                "pipelines", duplicated, str(request.url.path)
            )

        self.router.get("/{pipeline_id}/jobs")(list_jobs)
        self.router.post("/{id}/duplicate")(duplicate_pipeline)


def get_pipelines_router():
    """Get the pipelines router."""
    service = PipelinesService()
    controller = PipelinesController(
        prefix="pipelines",
        service=service,
        create_schema=CreatePipelineSchema,
        update_schema=UpdatePipelineSchema,
        tags=["Pipelines"],
        id_param="id",
    )
    return controller.router
