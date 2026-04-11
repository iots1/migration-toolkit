"""Pipelines service — handles CRUD for pipelines table."""

from __future__ import annotations

import json
from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import pipeline_repo


class PipelinesService(BaseService):
    """Service for pipelines CRUD operations."""

    resource_type = "pipelines"
    allowed_fields = [
        "id",
        "name",
        "description",
        "source_datasource_id",
        "target_datasource_id",
        "error_strategy",
        "created_at",
        "updated_at",
    ]

    def find_all(self, params: QueryParams) -> dict:
        """List all pipelines with pagination."""
        data = self.execute_db_operation(lambda: pipeline_repo.get_all_list())
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)

        return {
            "data": page_data,
            "total": total,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str) -> dict:
        """Get pipeline by name (id is treated as pipeline_name)."""
        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def create(self, data: dict) -> dict:
        """Create new pipeline."""
        name = data.get("name", "")
        description = data.get("description", "")
        json_data = data.get("json_data", "{}")

        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        ok, msg = self.execute_db_operation(
            lambda: pipeline_repo.save(
                name=name,
                description=description,
                json_data=json_data,
                source_ds_id=data.get("source_datasource_id"),
                target_ds_id=data.get("target_datasource_id"),
                error_strategy=data.get("error_strategy", "fail_fast"),
            )
        )
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(name))
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update pipeline (upsert)."""
        existing = self.find_by_id(id)

        name = data.get("name") or existing.get("name", "")
        description = data.get("description") or existing.get("description", "")
        json_data = data.get("json_data") or existing.get("json_data", "{}")

        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        ok, msg = self.execute_db_operation(
            lambda: pipeline_repo.save(
                name=name,
                description=description,
                json_data=json_data,
                source_ds_id=data.get("source_datasource_id")
                or existing.get("source_datasource_id"),
                target_ds_id=data.get("target_datasource_id")
                or existing.get("target_datasource_id"),
                error_strategy=data.get("error_strategy")
                or existing.get("error_strategy", "fail_fast"),
            )
        )
        self._assert_success(ok, msg)

        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(name))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """Delete pipeline."""
        self.find_by_id(id)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.delete(id))
        self._assert_success(ok, msg)
