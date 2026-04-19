"""Validators API router — GET /api/v1/validators"""

from __future__ import annotations

from fastapi import APIRouter

from api.base.json_api import create_collection_response
from validators import get_validator_options

router = APIRouter(prefix="/api/v1/validators", tags=["Validators"])


@router.get("", status_code=200)
def list_validators():
    return create_collection_response(
        resource_type="validators",
        data=get_validator_options(),
        base_url="/api/v1/validators",
    )


def get_validators_router() -> APIRouter:
    return router
