"""Configs API router with history endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Request
from api.base.controller import BaseController
from api.base import json_api
from api.configs.service import ConfigsService
from api.configs.schemas import CreateConfigSchema, UpdateConfigSchema


def get_configs_router():
    """Get the configs router with history endpoints."""
    service = ConfigsService()
    controller = BaseController(
        prefix="configs",
        service=service,
        create_schema=CreateConfigSchema,
        update_schema=UpdateConfigSchema,
        tags=["Configs"],
        id_param="id",
    )

    # Add extra history endpoints
    @controller.router.get("/{id}/histories")
    def get_history(id: str, request: Request):
        existing = service.find_by_id(id)
        history = service.get_history(existing["config_name"])
        return json_api.create_collection_response(
            "config-versions",
            history,
            str(request.url.path),
        )

    @controller.router.get("/{id}/histories/{version}")
    def get_version(id: str, version: int, request: Request):
        existing = service.find_by_id(id)
        version_data = service.get_version(existing["config_name"], version)
        return json_api.create_success_response(
            "config-version",
            version_data,
            str(request.url.path),
        )

    return controller.router
