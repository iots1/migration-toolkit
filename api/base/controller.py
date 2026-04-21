"""
Base controller class with generic CRUD route registration.

Subclass BaseController to get standard REST endpoints automatically:
    GET    /prefix          — list (paginated)
    GET    /prefix/{id}     — get by id
    POST   /prefix          — create
    PUT    /prefix/{id}     — update
    DELETE /prefix/{id}     — delete

Extension hooks (override in subclasses):
    _register_pre_routes()    — add literal routes that must precede /{id}
    _make_create_response()   — customize POST response format / status
    create_status_code        — int, default 201
"""

from fastapi import APIRouter, Depends, Request
from api.base.service import BaseService
from api.base.query_params import QueryParams, get_query_params
from api.base import json_api


def _make_update_endpoint(schema, svc, router, id_param: str = "id"):
    code = f"""
def update({id_param}: str, data: schema, request: Request):
    updated = svc.update({id_param}, data.model_dump(exclude_unset=True))
    return json_api.create_success_response(svc.resource_type, updated, str(request.url.path))
"""
    ns = {"svc": svc, "schema": schema, "Request": Request, "json_api": json_api}
    exec(code, ns)
    router.put(f"/{{{id_param}}}")(ns["update"])


def _make_delete_endpoint(svc, router, id_param: str = "id"):
    code = f"""
def delete({id_param}: str):
    svc.delete({id_param})
    return json_api.create_no_content_response()
"""
    ns = {"svc": svc, "json_api": json_api}
    exec(code, ns)
    router.delete(f"/{{{id_param}}}", status_code=204)(ns["delete"])


class BaseController:
    """Generic CRUD controller. Subclass and override hooks to customise behaviour."""

    #: HTTP status returned by the POST (create) endpoint. Override to 202 for async.
    create_status_code: int = 201

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

    # ------------------------------------------------------------------
    # Extension hooks
    # ------------------------------------------------------------------

    def _register_pre_routes(self) -> None:
        """Override to register *literal* routes that must appear before ``/{id}``.

        Called after the GET-list routes and before ``GET /{id}`` is registered,
        so literal paths (e.g. ``/running``, ``/stats``) take precedence over the
        parameterised route.
        """

    def _make_create_response(self, resource_type: str, data: dict, url: str):
        """Override to customise the POST response format.

        Default: standard JSON:API 201 Created response.
        """
        return json_api.create_created_response(resource_type, data, url)

    # ------------------------------------------------------------------
    # Route registration
    # ------------------------------------------------------------------

    def _register_create_endpoint(self, schema, svc, router) -> None:
        """Register POST (create) endpoints — extracted so subclasses can override.

        Uses exec() so ``data: schema`` is evaluated eagerly with the actual schema
        class in scope, bypassing PEP-563 lazy annotation issues.
        """
        ctrl = self
        status_code = self.create_status_code
        code = """
def create(data: schema, request: Request):
    created = svc.create(data.model_dump())
    return ctrl._make_create_response(svc.resource_type, created, str(request.url.path))
"""
        ns = {"svc": svc, "schema": schema, "Request": Request, "ctrl": ctrl}
        exec(code, ns)
        router.post("/", status_code=status_code)(ns["create"])
        router.post("", status_code=status_code)(ns["create"])

    def _register_routes(self, create_schema, update_schema, id_param: str = "id") -> None:
        router = self.router
        svc = self.service

        # ── GET list (both "/" and "" to avoid 307 redirect) ──────────
        def find_all(request: Request, params: QueryParams = Depends(get_query_params)):
            result = svc.find_all(params)
            pagination = svc._build_pagination_meta(
                result["total"], params, result.get("total_records")
            )
            return json_api.create_paginated_response(
                svc.resource_type, result["data"], pagination, str(request.url.path)
            )

        router.get("/")(find_all)
        router.get("")(find_all)

        # ── Pre-routes hook ────────────────────────────────────────────
        # Must run BEFORE GET /{id} so literal paths aren't swallowed by
        # the parameterised route.
        self._register_pre_routes()

        # ── GET /{id} ──────────────────────────────────────────────────
        code = f"""
def find_by_id({id_param}: str, request: Request):
    item = svc.find_by_id({id_param})
    return json_api.create_success_response(svc.resource_type, item, str(request.url.path))
"""
        ns = {"svc": svc, "Request": Request, "json_api": json_api}
        exec(code, ns)
        router.get(f"/{{{id_param}}}")(ns["find_by_id"])

        # ── POST / PUT / DELETE ────────────────────────────────────────
        self._register_create_endpoint(create_schema, svc, router)
        _make_update_endpoint(update_schema, svc, router, id_param)
        _make_delete_endpoint(svc, router, id_param)
