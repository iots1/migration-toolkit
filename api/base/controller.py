"""
Base controller class with generic CRUD route registration.

Mirrors NestJS BaseControllerOperations — registers GET, POST, PUT, DELETE
routes automatically on an APIRouter.
"""

from fastapi import APIRouter, Depends, Request
from api.base.service import BaseService
from api.base.query_params import QueryParams, get_query_params
from api.base import json_api


def _make_create_endpoint(schema, svc, router):
    """Factory that creates a POST endpoint with correct type annotation."""

    def create(data: schema, request: Request):
        created = svc.create(data.model_dump())
        return json_api.create_created_response(
            svc.resource_type,
            created,
            str(request.url.path),
        )

    router.post("/", status_code=201)(create)
    router.post("")(create)


def _make_update_endpoint(schema, svc, router, id_param: str = "id"):
    """Factory that creates a PUT endpoint with correct type annotation."""
    code = f"""
def update({id_param}: str, data: schema, request: Request):
    updated = svc.update({id_param}, data.model_dump(exclude_unset=True))
    return json_api.create_success_response(
        svc.resource_type, updated, str(request.url.path)
    )
"""
    namespace = {"svc": svc, "schema": schema, "Request": Request, "json_api": json_api}
    exec(code, namespace)
    update = namespace["update"]
    router.put(f"/{{{id_param}}}")(update)


def _make_delete_endpoint(svc, router, id_param: str = "id"):
    """Factory that creates a DELETE endpoint."""
    code = f"""
def delete({id_param}: str):
    svc.delete({id_param})
    return json_api.create_no_content_response()
"""
    namespace = {"svc": svc, "json_api": json_api}
    exec(code, namespace)
    delete = namespace["delete"]
    router.delete(f"/{{{id_param}}}", status_code=204)(delete)


class BaseController:
    """Generic CRUD controller that registers routes on an APIRouter."""

    def __init__(
        self,
        prefix: str,
        service: BaseService,
        create_schema,
        update_schema,
        tags: list[str],
        id_param: str = "id",
    ):
        self.router = APIRouter(
            prefix=f"/api/v1/{prefix}", tags=tags, redirect_slashes=False
        )
        self.service = service
        self._prefix = prefix
        self._register_routes(create_schema, update_schema, id_param)

    def _register_routes(self, create_schema, update_schema, id_param: str = "id"):
        """Register standard CRUD endpoints (both with and without trailing slash)."""
        router = self.router
        svc = self.service

        def find_all(request: Request, params: QueryParams = Depends(get_query_params)):
            result = svc.find_all(params)
            pagination = svc._build_pagination_meta(
                result["total"], params, result.get("total_records")
            )
            return json_api.create_paginated_response(
                svc.resource_type,
                result["data"],
                pagination,
                str(request.url.path),
            )

        code = f"""
def find_by_id({id_param}: str, request: Request):
    item = svc.find_by_id({id_param})
    return json_api.create_success_response(
        svc.resource_type, item, str(request.url.path)
    )
"""
        namespace = {"svc": svc, "Request": Request, "json_api": json_api}
        exec(code, namespace)
        find_by_id = namespace["find_by_id"]

        router.get("/")(find_all)
        router.get("")(find_all)
        router.get(f"/{{{id_param}}}")(find_by_id)

        _make_create_endpoint(create_schema, svc, router)
        _make_update_endpoint(update_schema, svc, router, id_param)
        _make_delete_endpoint(svc, router, id_param)
