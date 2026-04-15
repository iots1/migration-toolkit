"""Configs service — handles CRUD for configs table."""

from __future__ import annotations

import json
from api.base.service import BaseService
from api.base.query_params import QueryParams
from repositories import config_repo
from models.migration_config import ConfigRecord


class ConfigsService(BaseService):
    """Service for configs CRUD operations."""

    resource_type = "configs"
    allowed_fields = [
        "id",
        "config_name",
        "table_name",
        "json_data",
        "datasource_source_id",
        "datasource_target_id",
        "config_type",
        "script",
        "generate_sql",
        "condition",
        "lookup",
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
        """List all configs with pagination."""
        total_records = self.execute_db_operation(lambda: config_repo.count_all())
        data = self.execute_db_operation(lambda: config_repo.get_all_list())
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
        """Get config by UUID."""
        result = self.execute_db_operation(lambda: config_repo.get_by_id_raw(id))
        self._assert_found(result, id)
        return self._sanitize_response(result)

    def create(self, data: dict) -> dict:
        """Create new config."""
        record = self._to_record(data)
        ok, msg = self.execute_db_operation(lambda: config_repo.save(record))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(
            lambda: config_repo.get_content(record.config_name)
        )
        return self._sanitize_response(result)

    def update(self, id: str, data: dict) -> dict:
        """Update config (patch — existing values are preserved for missing fields)."""
        existing = self.find_by_id(id)
        record = self._to_record(
            data,
            existing=existing,
        )
        ok, msg = self.execute_db_operation(lambda: config_repo.save(record, id))
        self._assert_success(ok, msg)
        result = self.execute_db_operation(lambda: config_repo.get_by_id_raw(id))
        return self._sanitize_response(result)

    def delete(self, id: str) -> None:
        """Delete config."""
        existing = self.find_by_id(id)
        ok, msg = self.execute_db_operation(
            lambda: config_repo.delete(existing["config_name"])
        )
        self._assert_success(ok, msg)

    def get_history(self, config_name: str) -> list[dict]:
        """Get config version history."""
        df = self.execute_db_operation(lambda: config_repo.get_history(config_name))
        return self._df_to_list(df)

    def get_version(self, config_name: str, version: int) -> dict:
        """Get specific config version."""
        result = self.execute_db_operation(
            lambda: config_repo.get_version(config_name, version)
        )
        self._assert_found(result, f"{config_name}:v{version}")
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _to_record(
        self,
        data: dict,
        *,
        existing: dict | None = None,
        config_name_override: str | None = None,
    ) -> ConfigRecord:
        """Build a ConfigRecord from request data, falling back to existing values."""
        ex = existing or {}

        json_data = data.get("json_data") or ex.get("json_data", "{}")
        if isinstance(json_data, dict):
            json_data = json.dumps(json_data, ensure_ascii=False)

        return ConfigRecord(
            config_name=config_name_override or data.get("config_name", ""),
            table_name=data.get("table_name") or ex.get("table_name", ""),
            json_data=json_data,
            datasource_source_id=data.get("datasource_source_id")
            or ex.get("datasource_source_id"),
            datasource_target_id=data.get("datasource_target_id")
            or ex.get("datasource_target_id"),
            config_type=data.get("config_type") or ex.get("config_type", "std"),
            script=data.get("script") or ex.get("script"),
            generate_sql=data.get("generate_sql") or ex.get("generate_sql"),
            condition=data.get("condition") or ex.get("condition"),
            lookup=data.get("lookup") or ex.get("lookup"),
        )
