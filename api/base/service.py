"""
Abstract base service class with CRUD operations and error handling.

Mirrors NestJS BaseServiceOperations with execute_db_operation wrapper
that catches PostgreSQL errors and maps them to HTTP exceptions.
"""

from __future__ import annotations

import math
import traceback as _tb
import uuid
from abc import ABC, abstractmethod
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from api.base.query_params import QueryParams
from api.base.sql_query_builder import SqlQueryBuilder


class BaseService(ABC):
    """Abstract base service for CRUD operations.

    Uses Template Method pattern for find_all():
        - Subclasses implement _count_all() and _list_all()
        - Optional _post_process_page() hook for post-pagination transforms
    """

    resource_type: str = ""
    allowed_fields: list[str] = []

    @abstractmethod
    def _count_all(self) -> int:
        """Return total record count from the database."""

    @abstractmethod
    def _list_all(self) -> list[dict]:
        """Return all records as list of dicts from the database."""

    def _post_process_page(self, page_data: list[dict]) -> list[dict]:
        """Hook: transform paginated data before returning. Override in subclasses."""
        return page_data

    def find_all(self, params: QueryParams) -> dict:
        """List all records with in-memory filtering, sorting, and pagination."""
        total_records = self.execute_db_operation(self._count_all)
        data = self.execute_db_operation(self._list_all)
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)
        page_data = self._post_process_page(page_data)

        return {
            "data": page_data,
            "total": total,
            "total_records": total_records,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    @abstractmethod
    def find_by_id(self, id: str | int) -> dict:
        """Return single record dict or raise HTTPException 404."""

    @abstractmethod
    def create(self, data) -> dict:
        """Return created record dict."""

    @abstractmethod
    def update(self, id: str | int, data) -> dict:
        """Return updated record dict."""

    @abstractmethod
    def delete(self, id: str | int) -> None:
        """Raise HTTPException 404 if not found."""

    def execute_db_operation(self, operation, *, operation_name: str = ""):
        try:
            return operation()
        except IntegrityError as e:
            pg_code = getattr(e.orig, "pgcode", None)

            error_map = {
                "23505": (
                    409,
                    f"A {self.resource_type} record with this identifier already exists.",
                ),
                "23503": (400, "Referenced record does not exist or has been deleted."),
                "23502": (400, "A required field is missing from the request."),
                "22P02": (400, "Invalid format: expected a valid UUID or identifier."),
            }

            if pg_code in error_map:
                status, msg = error_map[pg_code]
                raise HTTPException(status_code=status, detail=msg)

            raise HTTPException(
                status_code=500,
                detail=f"Database operation failed. Please try again.",
            )
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"{type(e).__name__}: {e}\n\n{_tb.format_exc()}",
            )

    def _apply_query_params(self, data: list[dict], params: QueryParams) -> list[dict]:
        return SqlQueryBuilder.build(params).apply(data)

    def _paginate(
        self, data: list[dict], params: QueryParams
    ) -> tuple[list[dict], int, int]:
        return SqlQueryBuilder.paginate(data, params)

    # --- Shared helpers ---

    def _sanitize_response(self, record: dict) -> dict:
        """Strip fields not in allowed_fields from a single record."""
        if not self.allowed_fields:
            return record
        return {k: v for k, v in record.items() if k in self.allowed_fields}

    def _sanitize_list(self, records: list[dict]) -> list[dict]:
        """Strip fields not in allowed_fields from a list of records."""
        if not self.allowed_fields:
            return records
        return [self._sanitize_response(r) for r in records]

    def _df_to_list(self, df) -> list[dict]:
        """Convert pandas DataFrame to list of dicts, handle UUID columns."""
        if hasattr(df, "to_dict"):
            return df.to_dict("records")
        return []

    def _assert_found(self, result, id: str | int) -> None:
        """Raise 404 if result is None."""
        if result is None:
            raise HTTPException(status_code=404, detail=f"Not found: {id}")

    def _assert_success(self, ok: bool, message: str) -> None:
        """Convert repo (bool, str) tuple to HTTPException if failed."""
        if not ok:
            raise HTTPException(status_code=400, detail=message)

    def _build_pagination_meta(
        self, total: int, params: QueryParams, total_records: int | None = None
    ) -> dict:
        page_size = params.limit
        total_pages = max(1, math.ceil(total / page_size))
        return {
            "page": params.page,
            "page_size": page_size,
            "total": total,
            "total_records": total_records if total_records is not None else total,
            "total_pages": total_pages,
        }

    def _parse_uuid(self, value: str, field: str = "id") -> uuid.UUID:
        """Parse and validate a UUID string. Raises HTTPException 400 if invalid."""
        try:
            return uuid.UUID(value)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {value}")

    def _merge_fields(
        self,
        data: dict,
        existing: dict,
        fields: list[str],
        defaults: dict | None = None,
    ) -> dict:
        """Merge request data with existing record. Request fields take precedence."""
        df = defaults or {}
        return {
            f: data.get(f) if f in data else existing.get(f, df.get(f)) for f in fields
        }
