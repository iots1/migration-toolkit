"""
Abstract base service class with CRUD operations and error handling.

Mirrors NestJS BaseServiceOperations with execute_db_operation wrapper
that catches PostgreSQL errors and maps them to HTTP exceptions.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from api.base.query_params import QueryParams


class BaseService(ABC):
    """Abstract base service for CRUD operations."""

    resource_type: str = ""
    allowed_fields: list[str] = []

    @abstractmethod
    def find_all(self, params: QueryParams) -> dict:
        """Return {data: list, total: int, page: int, page_size: int, total_pages: int}."""
        pass

    @abstractmethod
    def find_by_id(self, id: str | int) -> dict:
        """Return single record dict or raise HTTPException 404."""
        pass

    @abstractmethod
    def create(self, data) -> dict:
        """Return created record dict."""
        pass

    @abstractmethod
    def update(self, id: str | int, data) -> dict:
        """Return updated record dict."""
        pass

    @abstractmethod
    def delete(self, id: str | int) -> None:
        """Raise HTTPException 404 if not found."""
        pass

    def execute_db_operation(self, operation, *, operation_name: str = ""):
        """
        Wrap DB calls to catch PostgreSQL-specific errors.

        Maps PostgreSQL error codes to HTTP exceptions:
        - 23505: Unique violation → 409 Conflict
        - 23503: FK violation → 400 Bad Request
        - 23502: Not-null violation → 400 Bad Request
        - 22P02: Invalid UUID → 400 Bad Request
        - Other: → 500 Internal Server Error
        """
        try:
            return operation()
        except IntegrityError as e:
            pg_code = getattr(e.orig, "pgcode", None)
            detail = str(e.orig) if e.orig else str(e)

            error_map = {
                "23505": (409, f"Duplicate record in {self.resource_type}. {detail}"),
                "23503": (400, f"Invalid reference to another record. {detail}"),
                "23502": (400, f"A required field was left empty. {detail}"),
                "22P02": (400, f"Invalid format for a field. {detail}"),
            }

            if pg_code in error_map:
                status, msg = error_map[pg_code]
                raise HTTPException(status_code=status, detail=msg)

            ctx = f" in {operation_name}" if operation_name else ""
            raise HTTPException(
                status_code=500, detail=f"Database error{ctx}: {detail}"
            )
        except HTTPException:
            raise
        except Exception as e:
            ctx = f" in {operation_name}" if operation_name else ""
            raise HTTPException(
                status_code=500, detail=f"Unexpected error{ctx}: {str(e)}"
            )

    def _apply_query_params(self, data: list[dict], params: QueryParams) -> list[dict]:
        """Apply filtering, sorting, and field selection from QueryParams."""
        if params.fields:
            field_set = {f.strip() for f in params.fields.split(",")}
            data = [{k: v for k, v in item.items() if k in field_set} for item in data]

        if params.filter:
            data = self._apply_filters(data, params.filter)

        if params.or_:
            or_matches = self._apply_filters(data, params.or_)
            data = [item for item in data if item in or_matches]

        if params.s:
            data = self._apply_search(data, params.s)

        if params.sort:
            data = self._apply_sort(data, params.sort)

        if params.exclude_ids:
            exclude_set = {eid.strip() for eid in params.exclude_ids.split(",")}
            data = [item for item in data if str(item.get("id", "")) not in exclude_set]

        return data

    def _apply_filters(self, data: list[dict], filters: list[str]) -> list[dict]:
        """Apply filter conditions in format 'field||$operator||value'."""
        import re

        pattern = re.compile(r"^(\w+)\|\|\$(\w+)\|\|(.+)$")
        result = list(data)

        for f in filters:
            match = pattern.match(f)
            if not match:
                continue
            field, operator, value = match.group(1), match.group(2), match.group(3)

            filtered = []
            for item in result:
                item_val = item.get(field)
                if item_val is None:
                    continue

                if operator == "eq" and str(item_val) == value:
                    filtered.append(item)
                elif operator == "ne" and str(item_val) != value:
                    filtered.append(item)
                elif operator == "contains" and value.lower() in str(item_val).lower():
                    filtered.append(item)
                elif operator == "gt" and str(item_val) > value:
                    filtered.append(item)
                elif operator == "gte" and str(item_val) >= value:
                    filtered.append(item)
                elif operator == "lt" and str(item_val) < value:
                    filtered.append(item)
                elif operator == "lte" and str(item_val) <= value:
                    filtered.append(item)
                else:
                    filtered.append(item)

            result = filtered

        return result

    def _apply_search(self, data: list[dict], search_json: str) -> list[dict]:
        """Apply search conditions from JSON string."""
        import json

        try:
            conditions = json.loads(search_json)
            if not isinstance(conditions, dict):
                return data
        except (json.JSONDecodeError, TypeError):
            return data

        result = []
        for item in data:
            match = True
            for field, condition in conditions.items():
                item_val = item.get(field)
                if item_val is None:
                    match = False
                    break
                if isinstance(condition, dict):
                    for op, val in condition.items():
                        if op == ">" and not (str(item_val) > str(val)):
                            match = False
                        elif op == ">=" and not (str(item_val) >= str(val)):
                            match = False
                        elif op == "<" and not (str(item_val) < str(val)):
                            match = False
                        elif op == "<=" and not (str(item_val) <= str(val)):
                            match = False
                        elif op == "$eq" and str(item_val) != str(val):
                            match = False
                        elif op == "$ne" and str(item_val) == str(val):
                            match = False
                        elif (
                            op == "$contains"
                            and str(val).lower() not in str(item_val).lower()
                        ):
                            match = False
                        if not match:
                            break
                elif str(condition).lower() not in str(item_val).lower():
                    match = False
                if not match:
                    break
            if match:
                result.append(item)

        return result

    def _apply_sort(self, data: list[dict], sort_str: str) -> list[dict]:
        """Apply sorting from 'field:direction[,field2:direction2]' format."""
        sort_fields = []
        for part in sort_str.split(","):
            part = part.strip()
            if ":" in part:
                field, direction = part.rsplit(":", 1)
            else:
                field, direction = part, "asc"
            sort_fields.append((field.strip(), direction.strip().lower() == "asc"))

        def sort_key(item):
            keys = []
            for field, ascending in sort_fields:
                val = item.get(field, "")
                if val is None:
                    val = ""
                keys.append((str(val), ascending))
            return keys

        return sorted(data, key=sort_key)

    def _paginate(
        self, data: list[dict], params: QueryParams
    ) -> tuple[list[dict], int, int]:
        """Apply pagination and return (page_data, total, total_pages)."""
        total = len(data)
        if params.ignore_limit:
            return data, total, 1

        limit = params.limit
        page = params.page
        total_pages = max(1, -(-total // limit)) if total > 0 else 1
        offset = params.offset if params.offset is not None else (page - 1) * limit

        page_data = data[offset : offset + limit]
        return page_data, total, total_pages

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

    def _build_pagination_meta(self, total: int, params: QueryParams) -> dict:
        """Build pagination metadata."""
        page_size = params.limit
        total_pages = max(1, math.ceil(total / page_size))
        return {
            "page": params.page,
            "page_size": page_size,
            "total": total,
            "total_records": total,
            "total_pages": total_pages,
        }
