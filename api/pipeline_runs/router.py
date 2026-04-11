"""Pipeline runs API router."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from api.base import json_api
from api.base.query_params import QueryParams
from api.pipeline_runs.service import PipelineRunsService
from api.pipeline_runs.schemas import (
    CreatePipelineRunSchema,
    UpdatePipelineRunSchema,
)


def get_pipeline_runs_router():
    """Get the pipeline_runs router."""
    service = PipelineRunsService()
    router = APIRouter(prefix="/api/v1/pipeline-runs", tags=["Pipeline Runs"])

    @router.get("/running")
    def get_running_runs(request: Request):
        runs = service.get_running_runs()
        return json_api.create_collection_response(
            "pipeline-runs",
            runs,
            str(request.url.path),
        )

    @router.get("/")
    def find_all(request: Request, params: QueryParams = Depends()):
        result = service.find_all(params)
        pagination = service._build_pagination_meta(result["total"], params)
        return json_api.create_paginated_response(
            service.resource_type,
            result["data"],
            pagination,
            str(request.url.path),
        )

    @router.get("/{id}")
    def find_by_id(id: str, request: Request):
        item = service.find_by_id(id)
        return json_api.create_success_response(service.resource_type, item, str(request.url.path))

    @router.post("/", status_code=201)
    def create(data: CreatePipelineRunSchema, request: Request):
        created = service.create(data.model_dump())
        return json_api.create_created_response(
            service.resource_type,
            created,
            str(request.url.path),
        )

    @router.put("/{id}")
    def update(id: str, data: UpdatePipelineRunSchema, request: Request):
        updated = service.update(id, data.model_dump(exclude_unset=True))
        return json_api.create_success_response(service.resource_type, updated, str(request.url.path))

    return router
