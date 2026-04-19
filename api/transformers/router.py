"""Transformers API router — GET /api/v1/transformers"""

from __future__ import annotations

from fastapi import APIRouter

from api.base.json_api import create_collection_response
from data_transformers import get_transformer_options

router = APIRouter(prefix="/api/v1/transformers", tags=["Transformers"])


@router.get("", status_code=200)
def list_transformers():
    return create_collection_response(
        resource_type="transformers",
        data=get_transformer_options(),
        base_url="/api/v1/transformers",
    )


def get_transformers_router() -> APIRouter:
    return router
