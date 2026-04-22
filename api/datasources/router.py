"""Datasources API router."""

from __future__ import annotations

from fastapi import APIRouter

from api.base.controller import BaseController
from api.base.json_api import create_collection_response
from api.datasources.service import DatasourcesService
from api.datasources.schemas import (
    CreateDatasourceSchema,
    UpdateDatasourceSchema,
)


def get_datasources_router():
    """Get the datasources router."""
    service = DatasourcesService()
    controller = BaseController(
        prefix="datasources",
        service=service,
        create_schema=CreateDatasourceSchema,
        update_schema=UpdateDatasourceSchema,
        tags=["Datasources"],
    )

    router: APIRouter = controller.router

    @router.get("/{datasource_id}/tables", tags=["Datasources"])
    def list_tables(datasource_id: str):
        data = service.get_tables(datasource_id)
        return create_collection_response(
            resource_type="datasource_tables",
            data=data,
            base_url=f"/api/v1/datasources/{datasource_id}/tables",
        )

    @router.get("/{datasource_id}/tables/{table_name}/columns", tags=["Datasources"])
    def list_columns(datasource_id: str, table_name: str):
        data = service.get_columns(datasource_id, table_name)
        return create_collection_response(
            resource_type="datasource_columns",
            data=data,
            base_url=f"/api/v1/datasources/{datasource_id}/tables/{table_name}/columns",
        )

    return router
