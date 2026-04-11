"""
JSON:API response builder functions.

Mirrors NestJS json-api.util.ts for consistent response formatting.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any


def _build_resource_object(resource_type: str, data: Any) -> dict:
    """Build a JSON:API resource object with id separate from attributes."""
    if isinstance(data, dict):
        data_copy = data.copy()
    else:
        return {"type": resource_type, "attributes": data}

    # Extract id from data
    item_id = data_copy.pop("id", None)
    if item_id is None:
        item_id = str(hash(frozenset(data.items())))

    resource = {
        "type": resource_type,
        "id": str(item_id),
        "attributes": data_copy,
    }

    return resource


def create_success_response(resource_type: str, data: dict, base_url: str = "") -> dict:
    """Single resource response (GET /{id}, PUT, POST 201)."""
    resource = _build_resource_object(resource_type, data)

    return {
        "data": resource,
        "links": {"self": f"{base_url}/{resource['id']}" if base_url else ""},
        "meta": {"timestamp": datetime.utcnow().isoformat()},
        "status": {"code": 200000, "message": "Request Succeeded"},
    }


def create_collection_response(resource_type: str, data: list[dict], base_url: str = "") -> dict:
    """Collection response without pagination (GET /)."""
    resources = [_build_resource_object(resource_type, item) for item in data]

    return {
        "data": resources,
        "links": {"self": base_url or ""},
        "meta": {"timestamp": datetime.utcnow().isoformat()},
        "status": {"code": 200000, "message": "Request Succeeded"},
    }


def create_paginated_response(
    resource_type: str,
    data: list[dict],
    pagination: dict,
    base_url: str = "",
) -> dict:
    """Collection response with pagination metadata and links."""
    resources = [_build_resource_object(resource_type, item) for item in data]
    page = pagination.get("page", 1)
    page_size = pagination.get("page_size", 10)
    total_pages = pagination.get("total_pages", 1)

    # Build pagination links
    links = {"self": f"{base_url}?page={page}&limit={page_size}" if base_url else ""}

    if data:
        links["first"] = f"{base_url}?page=1&limit={page_size}" if base_url else ""
        links["last"] = f"{base_url}?page={total_pages}&limit={page_size}" if base_url else ""

    if page > 1:
        links["prev"] = f"{base_url}?page={page - 1}&limit={page_size}" if base_url else ""

    if page < total_pages:
        links["next"] = f"{base_url}?page={page + 1}&limit={page_size}" if base_url else ""

    return {
        "data": resources,
        "links": links,
        "meta": {
            "timestamp": datetime.utcnow().isoformat(),
            "pagination": pagination,
        },
        "status": {"code": 200000, "message": "Request Succeeded"},
    }


def create_created_response(resource_type: str, data: dict, base_url: str = "") -> dict:
    """Created response (POST 201)."""
    resource = _build_resource_object(resource_type, data)
    location = f"{base_url}/{resource['id']}" if base_url else f"/{resource['id']}"

    return {
        "data": resource,
        "links": {"self": location},
        "meta": {"timestamp": datetime.utcnow().isoformat()},
        "status": {"code": 201000, "message": "Created successfully"},
    }


def create_no_content_response() -> dict:
    """No content response (DELETE 204)."""
    return {
        "data": None,
        "meta": {"timestamp": datetime.utcnow().isoformat()},
    }
