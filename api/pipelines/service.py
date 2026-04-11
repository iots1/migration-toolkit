"""Pipelines service — handles CRUD for pipelines table."""

from __future__ import annotations

import json
from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import pipeline_repo
from models.pipeline_config import PipelineRecord


class PipelinesService(BaseService):
    """Service for pipelines CRUD operations."""

    resource_type = "pipelines"
    allowed_fields = [
        "id",
        "name",
        "description",
        "json_data",
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
        record = self._to_record(data)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.save(record))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(record.name))
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update pipeline (patch — missing fields fall back to existing values)."""
        existing = self.find_by_id(id)
        record = self._to_record(data, existing=existing, name_override=id)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.save(record))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(lambda: pipeline_repo.get_by_name(record.name))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """Delete pipeline."""
        self.find_by_id(id)
        ok, msg = self.execute_db_operation(lambda: pipeline_repo.delete(id))
        self._assert_success(ok, msg)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_record(
        self,
        data: dict,
        *,
        existing: dict | None = None,
        name_override: str | None = None,
    ) -> PipelineRecord:
        """Build a PipelineRecord from request data, falling back to existing values."""
        ex = existing or {}

        json_data = data.get("json_data") or ex.get("json_data", "{}")
        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        return PipelineRecord(
            name=name_override or data.get("name", ""),
            description=data.get("description") or ex.get("description", ""),
            json_data=json_data,
            source_datasource_id=(
                data.get("source_datasource_id") or ex.get("source_datasource_id")
            ),
            target_datasource_id=(
                data.get("target_datasource_id") or ex.get("target_datasource_id")
            ),
            error_strategy=(
                data.get("error_strategy") or ex.get("error_strategy", "fail_fast")
            ),
        )
