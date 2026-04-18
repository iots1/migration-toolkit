"""Datasources API router."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from api.base.controller import BaseController
from api.base.json_api import create_collection_response, create_success_response
from api.datasources.service import DatasourcesService
from api.datasources.schemas import (
    CreateDatasourceSchema,
    UpdateDatasourceSchema,
)
from repositories.datasource_repo import get_by_id as get_datasource
from services.schema_inspector import (
    get_tables_from_datasource,
    get_columns_from_table,
    _safe_id,
)


def _resolve_datasource(datasource_id: str) -> dict:
    ds = get_datasource(datasource_id)
    if not ds:
        raise HTTPException(
            status_code=404, detail=f"Datasource '{datasource_id}' not found"
        )
    return ds


def _datasource_kwargs(ds: dict) -> dict:
    return {
        "db_type": ds["db_type"],
        "host": ds["host"],
        "port": ds["port"],
        "db_name": ds["dbname"],
        "user": ds["username"],
        "password": ds["password"],
    }


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
        ds = _resolve_datasource(datasource_id)
        kw = _datasource_kwargs(ds)
        ok, result = get_tables_from_datasource(**kw)
        if not ok:
            raise HTTPException(status_code=500, detail=result)

        data = [{"name": t} for t in result]
        return create_collection_response(
            resource_type="datasource_tables",
            data=data,
            base_url=f"/api/v1/datasources/{datasource_id}/tables",
        )

    @router.get("/{datasource_id}/tables/{table_name}/columns", tags=["Datasources"])
    def list_columns(datasource_id: str, table_name: str):
        """
        Get detailed column information for a table.

        Returns columns with the following properties:
        - name: Column name
        - type: Full data type (including length/precision)
        - is_nullable: Whether the column accepts NULL values
        - column_default: Default value expression
        - is_primary: Whether the column is a primary key
        - length: Character/byte maximum length (if applicable)
        - precision: Numeric precision (if applicable)
        - scale: Numeric scale (if applicable)
        - comment: Column comment/description
        - constraints: Array of constraints (name, type)
        - indexes: Array of indexes (name, unique, primary)
        """
        ds = _resolve_datasource(datasource_id)
        _safe_id(table_name)
        kw = _datasource_kwargs(ds)
        ok, result = get_columns_from_table(**kw, table_name=table_name)
        if not ok:
            raise HTTPException(status_code=500, detail=result)

        return create_collection_response(
            resource_type="datasource_columns",
            data=result,
            base_url=f"/api/v1/datasources/{datasource_id}/tables/{table_name}/columns",
        )

    return router
