"""
Jobs API router — uses JobsController(BaseController).

Standard endpoints (via BaseController):
    GET    /api/v1/jobs          — list jobs (paginated)
    GET    /api/v1/jobs/{id}     — get job by UUID
    POST   /api/v1/jobs          — trigger pipeline (202 Accepted)
    PUT    /api/v1/jobs/{id}     — 405 (not supported)
    DELETE /api/v1/jobs/{id}     — 405 (not supported)

Socket.IO events emitted during execution:
    "job:batch"     — { run_id, job_id, pipeline_id, step, batch_num, rows_processed }
    "job:error"     — { run_id, job_id, pipeline_id, step, batch_num, error_message }
    "job:completed" — { run_id, job_id, pipeline_id, status, total_rows }
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from api.base.controller import BaseController
from api.base import json_api
from api.jobs.schemas import CreateJobSchema, UpdateJobSchema, JobCreatedResponse
from api.jobs.service import JobsService
from api.socket_manager import emit_from_thread


class JobsController(BaseController):
    """Extends BaseController for the async job-trigger pattern.

    Differences from standard CRUD:
    - POST returns 202 Accepted (async) instead of 201 Created.
    - POST response is ``JobCreatedResponse`` instead of JSON:API format.
    - PUT and DELETE are blocked (service raises 405).
    """

    create_status_code = 202

    def _make_create_response(self, resource_type: str, data: dict, url: str):
        return JobCreatedResponse(**data)

    def _register_pre_routes(self) -> None:
        svc = self.service

        def list_pipeline_runs(job_id: str, request: Request):
            rows = svc.find_pipeline_runs(job_id)
            return json_api.create_collection_response(
                "pipeline_runs", rows, str(request.url.path)
            )

        self.router.get("/{job_id}/pipeline-runs")(list_pipeline_runs)


def get_jobs_router():
    service = JobsService(emit_fn=emit_from_thread)
    controller = JobsController(
        prefix="jobs",
        service=service,
        create_schema=CreateJobSchema,
        update_schema=UpdateJobSchema,
        tags=["Jobs"],
    )
    return controller.router
