"""
Query parameters schema — mirrors NestJS QueryParamsDTO.

Supports pagination, sorting, filtering (filter + s), field selection,
timezone handling, exclude_ids, ignore_limit, get_count_only.
"""
from __future__ import annotations

from fastapi import Query
from pydantic import BaseModel, Field


class QueryParams(BaseModel):
    """Standard query parameters for all list endpoints."""

    # Pagination
    page: int = Field(default=1, ge=1, description="Current page number")
    offset: int | None = Field(default=None, ge=0, description="Raw offset (overrides page)")
    limit: int = Field(default=10, ge=1, le=1000, description="Records per page")

    # Sorting: "field:asc" or "field1:asc,field2:desc"
    sort: str | None = Field(default=None, description='Sort: field:direction[,field2:direction2]')

    # Search: JSON string e.g. '{"status":"active","age":{">":25}}'
    s: str | None = Field(default=None, description='JSON search: {"field": {"operator": value}}')

    # Filtering: "field||$operator||value"
    filter: list[str] = Field(default=[], description='Filter: field||$operator||value')

    # OR conditions (same format as filter)
    or_: list[str] = Field(default=[], alias="or", description='OR filter: field||$operator||value')

    # Sparse fieldsets: "field1,field2"
    fields: str | None = Field(default=None, description="Comma-separated field names to return")

    # Timezone for datetime comparison
    timezone: str = Field(default="Asia/Bangkok", description="IANA timezone")

    # Exclude specific IDs: "id1,id2,id3"
    exclude_ids: str | None = Field(default=None, description="Comma-separated IDs to exclude")

    # Flags
    ignore_limit: bool = Field(default=False, description="Return all records without pagination")
    get_count_only: bool = Field(default=False, description="Return only total count")

    model_config = {"populate_by_name": True}
