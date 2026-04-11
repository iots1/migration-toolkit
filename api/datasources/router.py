"""Datasources API router."""
from __future__ import annotations

from api.base.controller import BaseController
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
    return controller.router
