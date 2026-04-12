"""
Query parameters schema — mirrors NestJS QueryParamsDTO.

Supports pagination, sorting, filtering (filter + s), field selection,
timezone handling, exclude_ids, ignore_limit, get_count_only.
"""

from __future__ import annotations

from fastapi import Query
from pydantic import BaseModel


class QueryParams(BaseModel):
    """Standard query parameters for all list endpoints."""

    page: int = 1
    offset: int | None = None
    limit: int = 10
    sort: str | None = None
    s: str | None = None
    filter: list[str] = []
    or_: list[str] = []
    fields: str | None = None
    timezone: str = "Asia/Bangkok"
    exclude_ids: str | None = None
    ignore_limit: bool = False
    get_count_only: bool = False

    model_config = {"populate_by_name": True}


def get_query_params(
    page: int = Query(default=1, ge=1, description="Current page number"),
    offset: int | None = Query(
        default=None, ge=0, description="Raw offset (overrides page)"
    ),
    limit: int = Query(default=10, ge=1, le=1000, description="Records per page"),
    sort: str | None = Query(
        default=None, description="Sort: field:direction[,field2:direction2]"
    ),
    s: str | None = Query(
        default=None, description='JSON search: {"field": {"operator": value}}'
    ),
    filter: list[str] = Query(
        default=[], description="Filter: field||$operator||value"
    ),
    or_: list[str] = Query(
        default=[], alias="or", description="OR filter: field||$operator||value"
    ),
    fields: str | None = Query(
        default=None, description="Comma-separated field names to return"
    ),
    timezone: str = Query(default="Asia/Bangkok", description="IANA timezone"),
    exclude_ids: str | None = Query(
        default=None, description="Comma-separated IDs to exclude"
    ),
    ignore_limit: bool = Query(
        default=False, description="Return all records without pagination"
    ),
    get_count_only: bool = Query(default=False, description="Return only total count"),
) -> QueryParams:
    return QueryParams(
        page=page,
        offset=offset,
        limit=limit,
        sort=sort,
        s=s,
        filter=filter,
        or_=or_,
        fields=fields,
        timezone=timezone,
        exclude_ids=exclude_ids,
        ignore_limit=ignore_limit,
        get_count_only=get_count_only,
    )
