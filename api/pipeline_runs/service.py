"""Pipeline runs service — handles CRUD for pipeline_runs table."""

from __future__ import annotations

import json
import uuid
from fastapi import HTTPException
from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import pipeline_run_repo
from models.pipeline_config import PipelineRunRecord, PipelineRunUpdateRecord


class PipelineRunsService(BaseService):
    """Service for pipeline_runs CRUD operations."""

    resource_type = "pipeline-runs"
    allowed_fields = [
        "id",
        "pipeline_id",
        "job_id",
        "config_name",
        "batch_round",
        "rows_in_batch",
        "rows_cumulative",
        "batch_size",
        "total_records_in_config",
        "status",
        "error_message",
        "transformation_warnings",
        "created_at",
        "created_by",
        "updated_at",
        "updated_by",
        "is_deleted",
        "deleted_at",
        "deleted_by",
        "deleted_reason",
    ]

    def find_all(self, params: QueryParams) -> dict:
        """List all pipeline runs with pagination."""
        total_records = self.execute_db_operation(lambda: pipeline_run_repo.count_all())
        data = self.execute_db_operation(
            lambda: pipeline_run_repo.get_all(limit=params.limit, offset=0)
        )
        data = self._apply_query_params(data, params)
        data = self._sanitize_list(data)
        page_data, total, total_pages = self._paginate(data, params)

        return {
            "data": page_data,
            "total": total,
            "total_records": total_records,
            "page": params.page,
            "page_size": params.limit,
            "total_pages": total_pages,
        }

    def find_by_id(self, id: str) -> dict:
        """Get pipeline run by ID."""
        try:
            run_id = uuid.UUID(id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {id}")
        result = self.execute_db_operation(lambda: pipeline_run_repo.get_by_id(run_id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def create(self, data: dict) -> dict:
        """Create new pipeline run."""
        try:
            pipeline_id = uuid.UUID(data.get("pipeline_id", ""))
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {data.get('pipeline_id')}")

        steps_json = data.get("steps_json", "{}")
        if isinstance(steps_json, dict):
            steps_json = json.dumps(steps_json, ensure_ascii=False)

        raw_job_id = data.get("job_id")
        try:
            job_id = uuid.UUID(raw_job_id) if raw_job_id else None
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {raw_job_id}")

        record = PipelineRunRecord(
            pipeline_id=pipeline_id,
            job_id=job_id,
            status=data.get("status", "pending"),
            steps_json=steps_json,
        )
        run_id = self.execute_db_operation(lambda: pipeline_run_repo.save(record))
        result = self.execute_db_operation(lambda: pipeline_run_repo.get_by_id(run_id))
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update pipeline run status/steps/error."""
        try:
            run_id = uuid.UUID(id)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid UUID: {id}")

        steps_json = data.get("steps_json")
        if isinstance(steps_json, dict):
            steps_json = json.dumps(steps_json, ensure_ascii=False)

        patch = PipelineRunUpdateRecord(
            status=data.get("status", "pending"),
            steps_json=steps_json,
            error_message=data.get("error_message"),
        )
        self.execute_db_operation(lambda: pipeline_run_repo.update(run_id, patch))
        result = self.execute_db_operation(lambda: pipeline_run_repo.get_by_id(run_id))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """Pipeline runs cannot be deleted."""
        raise HTTPException(status_code=405, detail="Pipeline runs cannot be deleted")

    def get_running_runs(self) -> list[dict]:
        """Get all currently running pipeline runs."""
        return self.execute_db_operation(lambda: pipeline_run_repo.get_running_runs())
