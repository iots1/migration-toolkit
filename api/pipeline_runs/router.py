"""Pipeline runs API router — uses PipelineRunsController(BaseController)."""

from __future__ import annotations

from fastapi import Request

from api.base.controller import BaseController
from api.base import json_api
from api.pipeline_runs.service import PipelineRunsService
from api.pipeline_runs.schemas import CreatePipelineRunSchema, UpdatePipelineRunSchema


class PipelineRunsController(BaseController):
    """Extends BaseController with a GET /running route.

    ``/running`` must be registered *before* ``/{id}`` so FastAPI matches the
    literal segment first — that is handled automatically by the
    ``_register_pre_routes`` hook in BaseController.
    """

    def _register_pre_routes(self) -> None:
        svc = self.service

        @self.router.get("/running", tags=["Pipeline Runs"])
        def get_running_runs(request: Request):
            """List all pipeline runs that are currently in 'running' status."""
            runs = svc.get_running_runs()
            return json_api.create_collection_response(
                "pipeline-runs", runs, str(request.url.path)
            )


def get_pipeline_runs_router():
    service = PipelineRunsService()
    controller = PipelineRunsController(
        prefix="pipeline-runs",
        service=service,
        create_schema=CreatePipelineRunSchema,
        update_schema=UpdatePipelineRunSchema,
        tags=["Pipeline Runs"],
    )
    return controller.router
